import discord
from discord.ext import commands, tasks
import google.generativeai as genai
import json
import asyncio       
import re
import traceback
import datetime         
from typing import Optional, Dict, Any, List, Union, Callable
import logging
import sqlite3
from pathlib import Path
import io
import base64
from collections import defaultdict
import textwrap
import ast
import aiohttp
import inspect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AdvancedTaskManager:
    """Enhanced task manager with comprehensive error handling and persistence"""
    
    def __init__(self, storage=None, bot=None):
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.task_results: Dict[str, Any] = {}
        self.task_metadata: Dict[str, Dict[str, Any]] = {}
        self.task_counter = 0
        self.scheduled_tasks: Dict[str, Dict] = {}
        self.paused_tasks: Dict[str, asyncio.Event] = {}
        self.task_speeds: Dict[str, float] = {}
        self.task_shared_data: Dict[str, Dict[str, Any]] = {}
        self.max_tasks = 100
        self.storage = storage
        self.bot = bot
        self.shutdown_flag = False
        self._cleanup_lock = asyncio.Lock()
        
        # Start reminder checker if storage exists
        if self.storage:
            self._reminder_loop = self.check_reminders.start()
            
        logger.info("TaskManager initialized")

    def has_task(self, task_id: str) -> bool:
        """Check if a task exists (either active or finished)."""
        return task_id in self.active_tasks or task_id in self.task_results
    
    async def stop_all_tasks(self) -> int:
        """Stop all running tasks safely"""
        async with self._cleanup_lock:
            try:
                self.shutdown_flag = True
                stopped_count = 0
                
                # Stop reminder loop
                if hasattr(self, '_reminder_loop'):
                    self._reminder_loop.cancel()

                # Cancel all active tasks
                for task_id, task in list(self.active_tasks.items()):
                    try:
                        if not task.done():
                            task.cancel()
                            stopped_count += 1
                            logger.info(f"Cancelled task: {task_id}")
                    except Exception as e:
                        logger.error(f"Error cancelling task {task_id}: {e}")
                
                # Wait for tasks to finish cancelling (with timeout)
                if self.active_tasks:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*self.active_tasks.values(), return_exceptions=True),
                            timeout=5.0
                        )
                    except asyncio.TimeoutError:
                        logger.warning("Some tasks didn't cancel within timeout")
                
                # Clear all task data
                self.active_tasks.clear()
                self.paused_tasks.clear()
                self.task_speeds.clear()
                self.task_shared_data.clear()
                self.scheduled_tasks.clear()
                
                # Persist task state
                if self.storage:
                    try:
                        self.storage.store('task_manager_state', {
                            'active_tasks': [],
                            'shutdown_at': datetime.datetime.utcnow().isoformat()
                        })
                    except Exception as e:
                        logger.error(f"Failed to persist shutdown state: {e}")
                
                logger.info(f"✅ Stopped {stopped_count} tasks")
                return stopped_count
                
            except Exception as e:
                logger.error(f"Error in stop_all_tasks: {e}")
                return 0
            finally:
                self.shutdown_flag = False

    def create_task(self, coro, task_name: str | None = None, metadata: Dict | None = None, **kwargs) -> str:
        """Create a background task with comprehensive error handling"""
        try:
            if self.shutdown_flag:
                logger.warning("Task creation blocked - shutdown in progress")
                raise RuntimeError("Task manager is shutting down")
            
            if len(self.active_tasks) >= self.max_tasks:
                logger.warning(f"Task limit reached ({self.max_tasks})")
                raise RuntimeError(f"Maximum task limit ({self.max_tasks}) reached")
            
            if task_name is None:
                self.task_counter += 1
                task_name = f"task_{self.task_counter}"
            
            task_name = str(task_name)[:100]
            
            async def safe_coro():
                try:
                    # Check shutdown flag periodically
                    if self.shutdown_flag:
                        logger.info(f"Task {task_name} aborted due to shutdown")
                        return "Shutdown"
                    
                    # Validate if coro is actually awaitable
                    if inspect.iscoroutine(coro) or inspect.isawaitable(coro):
                        try:
                            return await coro
                        except TypeError as e:
                            # Catch "object str can't be used in await"
                            if "str" in str(e) and "await" in str(e):
                                logger.warning(f"Task {task_name} tried to await a string. Returning string directly.")
                                return str(coro)
                            raise
                            
                    elif callable(coro):
                        # If it's a synchronous function, run it in a thread
                        return await asyncio.to_thread(coro)
                    else:
                        # If it's a raw value, just return it
                        return coro
                        
                except asyncio.CancelledError:
                    logger.info(f"Task {task_name} cancelled")
                    raise
                except Exception as e:
                    logger.error(f"Task {task_name} error: {e}")
                    raise
            
            task = asyncio.create_task(safe_coro())
            self.active_tasks[task_name] = task
            
            self.task_metadata[task_name] = metadata or {}
            self.task_metadata[task_name].update({
                'created_at': datetime.datetime.utcnow(),
                'status': 'running',
                'pid': id(task)
            })
            
            self.paused_tasks[task_name] = asyncio.Event()
            self.paused_tasks[task_name].set()
            self.task_speeds[task_name] = 1.0
            self.task_shared_data[task_name] = {}
            
            # Persist task creation
            if self.storage:
                try:
                    active_list = list(self.active_tasks.keys())
                    self.storage.store('active_task_ids', active_list)
                except Exception as e:
                    logger.error(f"Failed to persist task creation: {e}")
            
            def cleanup_task(task_id):
                def callback(task):
                    try:
                        if task_id in self.active_tasks:
                            del self.active_tasks[task_id]
                        
                        try:
                            result = task.result()
                            self.task_results[task_id] = result
                            if task_id in self.task_metadata:
                                self.task_metadata[task_id]['status'] = 'completed'
                                self.task_metadata[task_id]['completed_at'] = datetime.datetime.utcnow()
                        except asyncio.CancelledError:
                            self.task_results[task_id] = "Cancelled"
                            if task_id in self.task_metadata:
                                self.task_metadata[task_id]['status'] = 'cancelled'
                        except Exception as e:
                            error_msg = f"Error: {str(e)[:200]}"
                            self.task_results[task_id] = error_msg
                            if task_id in self.task_metadata:
                                self.task_metadata[task_id]['status'] = 'failed'
                                self.task_metadata[task_id]['error'] = error_msg
                        
                        # Persist completion
                        if self.storage:
                            try:
                                active_list = list(self.active_tasks.keys())
                                self.storage.store('active_task_ids', active_list)
                                
                                # Store task history
                                self.storage.store(f'task_history_{task_id}', {
                                    'status': self.task_metadata.get(task_id, {}).get('status', 'unknown'),
                                    'completed_at': datetime.datetime.utcnow().isoformat()
                                }, ttl=86400)
                            except Exception as e:
                                logger.error(f"Failed to persist task completion: {e}")
                                
                    except Exception as e:
                        logger.error(f"Cleanup error for {task_id}: {e}")
                
                return callback
            
            task.add_done_callback(cleanup_task(task_name))
            logger.info(f"Created task: {task_name}")
            return task_name
            
        except Exception as e:
            logger.error(f"Failed to create task: {e}")
            raise
    
    def schedule_task(self, coro_func: Callable, task_name: str, delay_seconds: int, metadata: Dict | None = None):
        """Schedule a task with validation"""
        try:
            if self.shutdown_flag:
                logger.warning("Task scheduling blocked - shutdown in progress")
                return None
            
            delay_seconds = max(0, min(86400, int(delay_seconds)))
            
            async def delayed_task():
                await asyncio.sleep(delay_seconds)
                if self.shutdown_flag:
                    return "Shutdown"
                
                if inspect.iscoroutine(coro_func):
                    return await coro_func
                elif callable(coro_func):
                    result = coro_func()
                    if inspect.isawaitable(result):
                        return await result
                    return result
                return coro_func

            task_id = self.create_task(delayed_task(), task_name, metadata)
            self.scheduled_tasks[task_name] = {
                'task_id': task_id,
                'scheduled_for': datetime.datetime.utcnow() + datetime.timedelta(seconds=delay_seconds),
                'delay': delay_seconds
            }
            logger.info(f"Scheduled task {task_name} for {delay_seconds}s")
            return task_id
        except Exception as e:
            logger.error(f"Failed to schedule task: {e}")
            return None

    def add_reminder(self, user_id: int, channel_id: int, message: str, delay_seconds: int):
        """Add a reminder that persists via storage"""
        try:
            if not self.storage:
                return False, "Storage not initialized"
            
            remind_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay_seconds)
            self.storage.add_db_reminder(user_id, channel_id, message, remind_at)
            
            return True, f"Reminder set for <t:{int(remind_at.timestamp())}:R>"
            
        except Exception as e:
            logger.error(f"Error adding reminder: {e}")
            return False, str(e)

    @tasks.loop(seconds=30)
    async def check_reminders(self):
        """Background loop to check database for due reminders"""
        if not self.storage or not self.bot:
            return

        try:
            due_reminders = self.storage.get_due_reminders()
            for row in due_reminders:
                try:
                    rowid = row[0]
                    user_id = row[1]
                    channel_id = row[2]
                    message = row[3]
                    
                    channel = self.bot.get_channel(channel_id)
                    if channel:
                        user = self.bot.get_user(user_id)
                        mention = user.mention if user else f"User {user_id}"
                        
                        embed = discord.Embed(
                            title="⏰ Reminder",
                            description=message,
                            color=discord.Color.gold(),
                            timestamp=datetime.datetime.utcnow()
                        )
                        await channel.send(content=f"Hey {mention}!", embed=embed)
                        logger.info(f"Sent reminder to {user_id}")
                    
                    self.storage.delete_db_reminder(rowid)
                    
                except Exception as e:
                    logger.error(f"Failed to send/process reminder row: {e}")
                    try:
                        if 'rowid' in locals():
                            self.storage.delete_db_reminder(rowid)
                    except:
                        pass
                    
        except Exception as e:
            logger.error(f"Error in reminder loop: {e}")

    def get_task_info(self, task_id: str) -> Dict:
        """Get task info with safe fallbacks"""
        try:
            info = {'task_id': task_id}
            
            if task_id in self.active_tasks:
                try:
                    task = self.active_tasks[task_id]
                    info['status'] = 'running'
                    info['done'] = str(task.done())
                    info['cancelled'] = str(task.cancelled())
                    info['paused'] = str(not self.paused_tasks.get(task_id, asyncio.Event()).is_set())
                    info['speed'] = str(self.task_speeds.get(task_id, 1.0))
                    
                    if task.done():
                        try:
                            result = task.result()
                            info['result'] = str(result)[:500]
                            info['status'] = 'completed'
                        except asyncio.CancelledError:
                            info['status'] = 'cancelled'
                        except Exception as e:
                            info['status'] = 'failed'
                            info['error'] = str(e)[:200]
                    
                except Exception as e:
                    logger.error(f"Error getting task info: {e}")
                    info['status'] = 'error'
            elif task_id in self.task_results:
                info['status'] = 'finished'
                result = self.task_results[task_id]
                info['result'] = str(result)[:500]
            else:
                info['status'] = 'not_found'
            
            if task_id in self.task_metadata:
                try:
                    meta = self.task_metadata[task_id].copy()
                    for key, value in meta.items():
                        if isinstance(value, datetime.datetime):
                            meta[key] = value.isoformat()
                    info.update(meta)
                except Exception as e:
                    logger.error(f"Error reading metadata: {e}")
            
            return info
        except Exception as e:
            logger.error(f"Critical error in get_task_info: {e}")
            return {'task_id': task_id, 'status': 'error', 'error': str(e)}
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel task with cleanup"""
        try:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                if not task.done():
                    task.cancel()
                
                if task_id in self.task_metadata:
                    self.task_metadata[task_id]['status'] = 'cancelled'
                    self.task_metadata[task_id]['cancelled_at'] = datetime.datetime.utcnow()
                
                if self.storage:
                    try:
                        self.storage.store(f'task_history_{task_id}', {
                            'status': 'cancelled',
                            'cancelled_at': datetime.datetime.utcnow().isoformat()
                        }, ttl=86400)
                    except Exception as e:
                        logger.error(f"Failed to persist cancellation: {e}")
                
                logger.info(f"Cancelled task: {task_id}")
                return True
        except Exception as e:
            logger.error(f"Error cancelling task: {e}")
        return False
    
    def pause_task(self, task_id: str) -> bool:
        """Pause task safely"""
        try:
            if task_id in self.active_tasks and task_id in self.paused_tasks:
                self.paused_tasks[task_id].clear()
                if task_id in self.task_metadata:
                    self.task_metadata[task_id]['status'] = 'paused'
                logger.info(f"Paused task: {task_id}")
                return True
        except Exception as e:
            logger.error(f"Error pausing task: {e}")
        return False
    
    def resume_task(self, task_id: str) -> bool:
        """Resume task safely"""
        try:
            if task_id in self.active_tasks and task_id in self.paused_tasks:
                self.paused_tasks[task_id].set()
                if task_id in self.task_metadata:
                    self.task_metadata[task_id]['status'] = 'running'
                logger.info(f"Resumed task: {task_id}")
                return True
        except Exception as e:
            logger.error(f"Error resuming task: {e}")
        return False
    
    def is_paused(self, task_id: str) -> bool:
        """Check pause state safely"""
        try:
            if task_id in self.paused_tasks:
                return not self.paused_tasks[task_id].is_set()
        except Exception as e:
            logger.error(f"Error checking pause state: {e}")
        return False
    
    async def wait_if_paused(self, task_id: str):
        """Wait if paused with timeout protection"""
        try:
            if self.shutdown_flag:
                return
            
            if task_id in self.paused_tasks:
                try:
                    await asyncio.wait_for(
                        self.paused_tasks[task_id].wait(),
                        timeout=3600
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Task {task_id} pause timeout, auto-resuming")
                    self.paused_tasks[task_id].set()
        except Exception as e:
            logger.error(f"Error in wait_if_paused: {e}")
    
    def set_task_speed(self, task_id: str, speed: float) -> bool:
        """Set speed with validation"""
        try:
            speed = float(speed)
            speed = max(0.1, min(10.0, speed))
            
            if task_id in self.active_tasks or task_id in self.task_results:
                self.task_speeds[task_id] = speed
                logger.info(f"Set speed for {task_id}: {speed}x")
                return True
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid speed value: {e}")
        return False
    
    def get_task_speed(self, task_id: str) -> float:
        try:
            return self.task_speeds.get(task_id, 1.0)
        except Exception:
            return 1.0
    
    def set_task_data(self, task_id: str, key: str, value: Any) -> bool:
        """Set task data with size limits"""
        try:
            key = str(key)[:100]
            
            if task_id not in self.task_shared_data:
                self.task_shared_data[task_id] = {}
            
            if len(self.task_shared_data[task_id]) >= 100:
                logger.warning(f"Task data limit reached for {task_id}")
                return False
            
            self.task_shared_data[task_id][key] = value
            return True
        except Exception as e:
            logger.error(f"Error setting task data: {e}")
            return False
    
    def get_task_data(self, task_id: str, key: str, default=None):
        try:
            if task_id in self.task_shared_data:
                return self.task_shared_data[task_id].get(key, default)
        except Exception as e:
            logger.error(f"Error getting task data: {e}")
        return default
    
    def get_all_task_data(self, task_id: str) -> Dict[str, Any]:
        try:
            return self.task_shared_data.get(task_id, {}).copy()
        except Exception as e:
            logger.error(f"Error getting all task data: {e}")
            return {}
    
    def get_all_tasks(self) -> Dict[str, Dict]:
        """Get all tasks with error handling"""
        try:
            all_task_ids = list(set(
                list(self.active_tasks.keys()) + 
                list(self.task_results.keys())
            ))
            return {tid: self.get_task_info(tid) for tid in all_task_ids}
        except Exception as e:
            logger.error(f"Error getting all tasks: {e}")
            return {}
    
    def cleanup_old_tasks(self, max_age_hours: int = 24):
        """Clean up old completed tasks"""
        try:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=max_age_hours)
            removed = 0
            
            for task_id in list(self.task_results.keys()):
                try:
                    if task_id in self.task_metadata:
                        completed_at = self.task_metadata[task_id].get('completed_at')
                        if completed_at and isinstance(completed_at, datetime.datetime):
                            if completed_at < cutoff:
                                del self.task_results[task_id]
                                del self.task_metadata[task_id]
                                removed += 1
                except Exception as e:
                    logger.error(f"Error cleaning up task {task_id}: {e}")
            
            if removed > 0:
                logger.info(f"Cleaned up {removed} old tasks")
            
            return removed
        except Exception as e:
            logger.error(f"Error in cleanup_old_tasks: {e}")
            return 0

        
class EnhancedStorage:
    """Storage with comprehensive error handling"""
    
    def __init__(self, db_path: str = "bot_data.db"):
        try:
            self.db_path = Path(db_path)
            self.cache: Dict[str, Any] = {}
            self.cache_size_limit = 1000
            self.init_database()
            logger.info(f"Storage initialized: {self.db_path}")
        except Exception as e:
            logger.error(f"Storage init failed: {e}")
            raise

    def __iter__(self):
        return iter([])

    def __repr__(self):
        return f"<EnhancedStorage db={self.db_path} cache={len(self.cache)}>"
    
    def init_database(self):
        """Initialize database with error recovery"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                cursor = conn.cursor()
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS cog_data (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        data_type TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP,
                        tags TEXT
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_preferences (
                        user_id INTEGER,
                        pref_key TEXT,
                        pref_value TEXT,
                        PRIMARY KEY (user_id, pref_key)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS guild_settings (
                        guild_id INTEGER,
                        setting_key TEXT,
                        setting_value TEXT,
                        PRIMARY KEY (guild_id, setting_key)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS task_history (
                        task_id TEXT PRIMARY KEY,
                        task_type TEXT,
                        created_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        status TEXT,
                        result TEXT
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS reminders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        channel_id INTEGER,
                        message TEXT,
                        remind_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_expires ON cog_data(expires_at)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags ON cog_data(tags)')
                
                conn.commit()
                logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database init error: {e}")
            raise
    
    def store(self, key: str, value: Any, ttl: int | None = None, tags: List[str] | None = None):
        """Store data with validation"""
        try:
            key = str(key)[:255]
            
            if len(self.cache) >= self.cache_size_limit:
                self.cache.pop(next(iter(self.cache)), None)
            
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                
                if isinstance(value, (dict, list)):
                    stored_value = json.dumps(value)
                    data_type = "json"
                else:
                    stored_value = str(value)[:10000]
                    data_type = type(value).__name__
                
                expires_at = None
                if ttl:
                    ttl = max(1, min(31536000, int(ttl)))
                    expires_at = (datetime.datetime.utcnow() + datetime.timedelta(seconds=ttl)).isoformat()
                
                tags_str = json.dumps(tags[:20]) if tags else None
                
                cursor.execute('''
                    INSERT OR REPLACE INTO cog_data 
                    (key, value, data_type, updated_at, expires_at, tags)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
                ''', (key, stored_value, data_type, expires_at, tags_str))
                conn.commit()
            
            self.cache[key] = value
            
        except Exception as e:
            logger.error(f"Store error for key '{key}': {e}")
            raise
        
    def set(self, key: str, value: Any, ttl: Optional[int] = None, tags: Optional[List[str]] = None):
        return self.store(key, value, ttl, tags)
    
    def get(self, key: str, default=None):
        """Get data with safe deserialization"""
        try:
            key = str(key)[:255]
            
            if key in self.cache:
                return self.cache[key]
            
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT value, data_type, expires_at 
                    FROM cog_data WHERE key = ?
                ''', (key,))
                result = cursor.fetchone()
                
                if not result:
                    return default
                
                value, data_type, expires_at = result
                
                if expires_at:
                    try:
                        if datetime.datetime.fromisoformat(expires_at) < datetime.datetime.utcnow():
                            self.delete(key)
                            return default
                    except Exception as e:
                        logger.error(f"Expiry check error: {e}")
                
                try:
                    if data_type == "json":
                        parsed = json.loads(value)
                    elif data_type == "int":
                        parsed = int(value)
                    elif data_type == "float":
                        parsed = float(value)
                    elif data_type == "bool":
                        parsed = value.lower() == "true"
                    else:
                        parsed = value
                    
                    self.cache[key] = parsed
                    return parsed
                except Exception as e:
                    logger.error(f"Deserialization error: {e}")
                    return default
                
        except Exception as e:
            logger.error(f"Get error for key '{key}': {e}")
            return default
    
    def delete(self, key: str):
        try:
            key = str(key)[:255]
            
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM cog_data WHERE key = ?', (key,))
                conn.commit()
            
            self.cache.pop(key, None)
        except Exception as e:
            logger.error(f"Delete error for key '{key}': {e}")
    
    def find_by_tags(self, tags: List[str]) -> List[str]:
        try:
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT key, tags FROM cog_data WHERE tags IS NOT NULL')
                results = cursor.fetchall()
                
                matching_keys = []
                for key, tags_str in results:
                    try:
                        stored_tags = json.loads(tags_str)
                        if any(tag in stored_tags for tag in tags):
                            matching_keys.append(key)
                    except Exception:
                        continue
                
                return matching_keys
        except Exception as e:
            logger.error(f"Find by tags error: {e}")
            return []
    
    def set_user_pref(self, user_id: int, key: str, value: Any):
        try:
            user_id = int(user_id)
            key = str(key)[:100]
            
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO user_preferences (user_id, pref_key, pref_value)
                    VALUES (?, ?, ?)
                ''', (user_id, key, json.dumps(value)))
                conn.commit()
        except Exception as e:
            logger.error(f"Set user pref error: {e}")
    
    def get_user_pref(self, user_id: int, key: str, default=None):
        try:
            user_id = int(user_id)
            key = str(key)[:100]
            
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT pref_value FROM user_preferences 
                    WHERE user_id = ? AND pref_key = ?
                ''', (user_id, key))
                result = cursor.fetchone()
                return json.loads(result[0]) if result else default
        except Exception as e:
            logger.error(f"Get user pref error: {e}")
            return default

    def add_db_reminder(self, user_id, channel_id, message, remind_at):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO reminders (user_id, channel_id, message, remind_at)
                VALUES (?, ?, ?, ?)
            ''', (user_id, channel_id, message, remind_at))
            conn.commit()

    def get_due_reminders(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT id, user_id, channel_id, message, remind_at
                FROM reminders
                WHERE remind_at <= ?
            ''', (datetime.datetime.utcnow(),))
            return cursor.fetchall()
            
    def delete_db_reminder(self, rowid):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM reminders WHERE id = ?', (rowid,))
            conn.commit()


class WebHelper:
    """Ultra-safe WebHelper with consistent response types"""

    def __init__(self):
        self.cache: Dict[str, Any] = {}
        self.cache_ttl: Dict[str, datetime.datetime] = {}
        self.max_cache_size = 100
        self.default_timeout = 10
        self.session: Optional[aiohttp.ClientSession] = None
        logger.info("WebHelper initialized")

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return True

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("WebHelper session closed")

    def _cache_key(self, url: str, method: str = "GET") -> str:
        return f"{method}:{url[:512]}"

    def _get_cached(self, key: str):
        ttl = self.cache_ttl.get(key)
        if ttl and ttl > datetime.datetime.utcnow():
            return self.cache.get(key)
        return None

    def _set_cached(self, key: str, val: Any, ttl: int = 300):
        self.cache[key] = val
        self.cache_ttl[key] = datetime.datetime.utcnow() + datetime.timedelta(seconds=ttl)
        if len(self.cache) > self.max_cache_size:
            oldest = sorted(self.cache_ttl.items(), key=lambda x: x[1])[:len(self.cache) - self.max_cache_size]
            for k, _ in oldest:
                self.cache.pop(k, None)
                self.cache_ttl.pop(k, None)

    async def get(self, url: str, headers: Dict | None = None, timeout: int | None = None,
                  use_cache: bool = True, cache_ttl: int = 300) -> Optional[Dict[str, Any]]:
        if not url or not isinstance(url, str):
            return None

        cache_key = self._cache_key(url, "GET")
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached:
                return cached

        if not await self._ensure_session():
            return None

        assert self.session is not None
        
        timeout_obj = aiohttp.ClientTimeout(total=timeout or self.default_timeout)
        headers = headers or {"User-Agent": "Mozilla/5.0"}

        try:
            async with self.session.get(url, headers=headers, timeout=timeout_obj) as resp:
                data = {
                    "status": resp.status,
                    "headers": dict(resp.headers),
                    "text": await resp.text()
                }
                if resp.status == 200 and use_cache:
                    self._set_cached(cache_key, data, cache_ttl)
                return data
        except Exception as e:
            logger.error(f"GET error for {url[:100]}: {e}")
            return None

    async def fetch_json(self, url: str, headers: Dict | None = None, timeout: int | None = None,
                         use_cache: bool = True, cache_ttl: int = 300) -> Optional[Dict]:
        if not url or not isinstance(url, str):
            return None

        cache_key = self._cache_key(url, "JSON")
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached:
                return cached

        if not await self._ensure_session():
            return None

        assert self.session is not None

        headers = headers or {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }

        timeout_obj = aiohttp.ClientTimeout(total=timeout or self.default_timeout)
        try:
            async with self.session.get(url, headers=headers, timeout=timeout_obj) as resp:
                if resp.status != 200:
                    logger.warning(f"fetch_json failed ({resp.status}) {url[:100]}")
                    return None

                try:
                    data = await resp.json(content_type=None)
                    if use_cache:
                        self._set_cached(cache_key, data, cache_ttl)
                    return data
                except Exception:
                    text = await resp.text()
                    logger.warning(f"Non-JSON response: {text[:200]}")
                    return None
        except Exception as e:
            logger.error(f"fetch_json error: {e}")
            return None

    async def post(self, url: str, data: Dict | None = None, json_data: Dict | None = None,
                   headers: Dict | None = None, timeout: int | None = None) -> Optional[Dict]:
        if not url:
            return None

        if not await self._ensure_session():
            return None

        assert self.session is not None

        headers = headers or {"User-Agent": "Mozilla/5.0"}
        timeout_obj = aiohttp.ClientTimeout(total=timeout or self.default_timeout)

        try:
            async with self.session.post(url, data=data, json=json_data, headers=headers, timeout=timeout_obj) as resp:
                result = {"status": resp.status, "headers": dict(resp.headers)}
                try:
                    result["json"] = await resp.json(content_type=None)
                except Exception:
                    result["text"] = await resp.text()
                return result
        except Exception as e:
            logger.error(f"POST error {url[:100]}: {e}")
            return None

    async def download(self, url: str, timeout: int = 15, max_size: int = 10_000_000) -> Optional[bytes]:
        if not url:
            return None

        if not await self._ensure_session():
            return None

        assert self.session is not None

        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        try:
            async with self.session.get(url, timeout=timeout_obj) as resp:
                if resp.status != 200:
                    logger.warning(f"Download failed {url[:100]} ({resp.status})")
                    return None
                content = await resp.read()
                if len(content) > max_size:
                    logger.warning(f"File too large ({len(content)} bytes)")
                    return None
                return content
        except Exception as e:
            logger.error(f"Download error {url[:100]}: {e}")
            return None

    def clear_cache(self):
        self.cache.clear()
        self.cache_ttl.clear()

    def get_cache_stats(self):
        return {"entries": len(self.cache), "limit": self.max_cache_size}


class CogEditor:
    """Safe cog file editing for bot owner"""
    
    @staticmethod
    def list_cogs(cogs_dir: str = "cogs") -> List[str]:
        try:
            cogs_path = Path(cogs_dir)
            if not cogs_path.exists():
                return []
            
            cog_files = []
            for file in cogs_path.glob("*.py"):
                if file.name != "__init__.py":
                    cog_files.append(file.name)
            
            return sorted(cog_files)
        except Exception as e:
            logger.error(f"List cogs error: {e}")
            return []
    
    @staticmethod
    def list_backups(cog_name: str, cogs_dir: str = "cogs") -> List[str]:
        try:
            if not cog_name.endswith('.py'):
                cog_name += '.py'
            
            cogs_path = Path(cogs_dir)
            if not cogs_path.exists():
                return []
            
            backup_pattern = f"{cog_name}.backup_*"
            backups = []
            for file in cogs_path.glob(backup_pattern):
                backups.append(file.name)
            
            return sorted(backups, reverse=True)
        except Exception as e:
            logger.error(f"List backups error: {e}")
            return []
    
    @staticmethod
    def read_cog(cog_name: str, cogs_dir: str = "cogs") -> Optional[str]:
        try:
            cog_name = str(cog_name)[:100]
            if not cog_name.endswith('.py'):
                cog_name += '.py'
            
            cog_name = re.sub(r'[<>:"/\\|?*]', '_', cog_name)
            
            cog_path = Path(cogs_dir) / cog_name
            if not cog_path.exists() or not cog_path.is_file():
                return None
            
            with open(cog_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            return content
        except Exception as e:
            logger.error(f"Read cog error: {e}")
            return None
    
    @staticmethod
    def write_cog(cog_name: str, content: str, cogs_dir: str = "cogs", backup: bool = True) -> tuple[bool, str]:
        try:
            cog_name = str(cog_name)[:100]
            if not cog_name.endswith('.py'):
                cog_name += '.py'
            
            cog_name = re.sub(r'[<>:"/\\|?*]', '_', cog_name)
            
            cogs_path = Path(cogs_dir)
            cogs_path.mkdir(exist_ok=True)
            
            cog_path = cogs_path / cog_name
            
            backup_created = False
            backup_path = None
            if cog_path.exists():
                backup_name = f"{cog_name}.backup_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                backup_path = cogs_path / backup_name
                try:
                    with open(cog_path, 'r', encoding='utf-8') as f:
                        backup_content = f.read()
                    with open(backup_path, 'w', encoding='utf-8') as f:
                        f.write(backup_content)
                    backup_created = True
                    logger.info(f"✅ Created backup: {backup_name}")
                except Exception as e:
                    logger.error(f"❌ BACKUP FAILED: {e}")
                    return False, f"Backup failed: {e}. Write operation aborted for safety."
            
            with open(cog_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            result_msg = str(cog_path)
            if backup_created and backup_path:
                result_msg += f"\n💾 Backup: {backup_path.name}"
            
            return True, result_msg
        except Exception as e:
            logger.error(f"Write cog error: {e}")
            return False, str(e)
    
    @staticmethod
    def restore_backup(cog_name: str, backup_name: str, cogs_dir: str = "cogs") -> tuple[bool, str]:
        try:
            if not cog_name.endswith('.py'):
                cog_name += '.py'
            
            cogs_path = Path(cogs_dir)
            backup_path = cogs_path / backup_name
            cog_path = cogs_path / cog_name
            
            if not backup_path.exists():
                return False, f"Backup not found: {backup_name}"
            
            if cog_path.exists():
                safety_backup = f"{cog_name}.before_restore_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                safety_path = cogs_path / safety_backup
                with open(cog_path, 'r', encoding='utf-8') as f:
                    current_content = f.read()
                with open(safety_path, 'w', encoding='utf-8') as f:
                    f.write(current_content)
                logger.info(f"Created safety backup: {safety_backup}")
            
            with open(backup_path, 'r', encoding='utf-8') as f:
                backup_content = f.read()
            
            try:
                ast.parse(backup_content)
            except SyntaxError as e:
                return False, f"Backup has invalid syntax: {e}"
            
            with open(cog_path, 'w', encoding='utf-8') as f:
                f.write(backup_content)
            
            return True, f"Restored from {backup_name}"
        except Exception as e:
            logger.error(f"Restore backup error: {e}")
            return False, str(e)
    
    @staticmethod
    def validate_cog_syntax(content: str) -> tuple[bool, Optional[str]]:
        try:
            ast.parse(content)
            return True, None
        except SyntaxError as e:
            return False, f"Syntax error at line {e.lineno}: {e.msg}"
        except Exception as e:
            return False, str(e)
    
    @staticmethod
    def diff_preview(cog_name: str, new_content: str, cogs_dir: str = "cogs") -> Optional[str]:
        try:
            old_content = CogEditor.read_cog(cog_name, cogs_dir)
            if not old_content:
                return "New file (no previous version)"
            
            old_lines = old_content.split('\n')
            new_lines = new_content.split('\n')
            
            changes = []
            changes.append(f"📊 Lines: {len(old_lines)} → {len(new_lines)} (Δ {len(new_lines) - len(old_lines):+d})")
            
            changed = 0
            for i, (old, new) in enumerate(zip(old_lines, new_lines)):
                if old != new:
                    changed += 1
            
            changes.append(f"✏️  Changed lines: {changed}")
            
            return '\n'.join(changes)
        except Exception as e:
            logger.error(f"Diff preview error: {e}")
            return None

        
class FileHelper:
    """File operations with error handling"""
    
    @staticmethod
    def create_text_file(content: str, filename: str = "output.txt") -> Optional[discord.File]:
        try:
            if not content:
                content = ""
            
            content = str(content)[:10_000_000]
            filename = str(filename)[:100]
            
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            if not filename.endswith('.txt'):
                filename += '.txt'
            
            buffer = io.BytesIO(content.encode('utf-8', errors='replace'))
            return discord.File(buffer, filename=filename)
        except Exception as e:
            logger.error(f"Create text file error: {e}")
            return None
    
    @staticmethod
    def create_json_file(data: Dict, filename: str = "data.json") -> Optional[discord.File]:
        try:
            if not isinstance(data, (dict, list)):
                return None
            
            filename = str(filename)[:100]
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            if not filename.endswith('.json'):
                filename += '.json'
            
            json_str = json.dumps(data, indent=2, ensure_ascii=False)
            buffer = io.BytesIO(json_str.encode('utf-8', errors='replace'))
            return discord.File(buffer, filename=filename)
        except Exception as e:
            logger.error(f"Create JSON file error: {e}")
            return None
    
    @staticmethod
    def create_csv_file(data: List[List], filename: str = "data.csv") -> Optional[discord.File]:
        try:
            if not data or not isinstance(data, list):
                return None
            
            import csv
            
            filename = str(filename)[:100]
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            if not filename.endswith('.csv'):
                filename += '.csv'
            
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            
            for row in data[:10000]:
                if isinstance(row, (list, tuple)):
                    writer.writerow(row[:100])
            
            bytes_buffer = io.BytesIO(buffer.getvalue().encode('utf-8', errors='replace'))
            return discord.File(bytes_buffer, filename=filename)
        except Exception as e:
            logger.error(f"Create CSV file error: {e}")
            return None
    
    @staticmethod
    async def read_attachment(attachment: discord.Attachment) -> Optional[bytes]:
        try:
            if not attachment:
                return None
            
            if attachment.size > 25_000_000:
                logger.warning(f"Attachment too large: {attachment.size}")
                return None
            
            return await attachment.read()
        except Exception as e:
            logger.error(f"Read attachment error: {e}")
            return None

class PermissionChecker:
    """Safe permission validation - OWNER ONLY MODE"""
    
    @classmethod
    def can_use_ai_commands(cls, message: discord.Message, bot_owner_id: int | None, bot_user) -> bool:
        """
        Check if user can use AI commands.
        STRICT MODE: Only the bot owner can use commands, and they MUST mention the bot.
        """
        try:
            if not message or not message.author or not bot_user:
                return False
            
            if message.author.bot:
                return False
            
            # 1. MANDATORY: Bot must be mentioned (or be a DM)
            # We treat DMs as implicit mentions.
            is_dm = isinstance(message.channel, discord.DMChannel)
            if not is_dm and bot_user not in message.mentions:
                # If it's a reply to the bot, we can optionally count that, 
                # but "mentioned" usually means explicit mention.
                # Let's check strictly for mention as requested.
                return False
            
            # 2. MANDATORY: User must be the bot owner
            # If we don't know the owner ID yet, we default to fail for safety
            if bot_owner_id is None:
                logger.warning("Bot owner ID is unknown! Permission denied.")
                return False

            if message.author.id != bot_owner_id:
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Permission check error: {e}")
            return False


class CodeExtractor:
    """Safe code extraction"""
    
    @staticmethod
    def extract_code_blocks(text: str) -> list[str]:
        try:
            if not text or not isinstance(text, str):
                return []
            
            text = str(text)[:100000]
            
            patterns = [
                r'```python\n(.+?)\n```',
                r'```\n(.+?)\n```',
                r'```(.+?)```',
            ]
            
            for pattern in patterns:
                try:
                    matches = re.findall(pattern, text, re.DOTALL)
                    if matches:
                        return [match.strip()[:50000] for match in matches]
                except Exception as e:
                    logger.error(f"Regex error: {e}")
                    continue
            
            return []
        except Exception as e:
            logger.error(f"Code extraction error: {e}")
            return []

class AdvancedPromptBuilder:
    """Safe prompt building"""
    
    SYSTEM_PROMPT = """You are an expert Discord.py developer. Generate ONLY valid, executable Python code.

⚠️ CRITICAL INDENTATION RULES:
- Start ALL code at column 0 (no leading spaces before first line)
- Use EXACTLY 4 spaces per indentation level
- NEVER use tabs
- NEVER mix spaces and tabs
- Every line after a colon (:) must be indented exactly 4 more spaces
- except/elif/else must be at same level as their try/if

⚠️ CRITICAL CHARACTER RULES:
- NEVER use emoji or non-ASCII characters in code (only in strings sent to Discord)
- Use plain ASCII for all variable names, comments, and code structure
- Emoji are OK inside string literals like: await channel.send("✅ Done!")
- But NOT in code like: result = ✅  # This breaks!

⚠️ CRITICAL ASYNC RULES:
- Use 'await' for coroutines, NOT 'async with'
- ONLY use 'async with' for actual async context managers (like aiohttp sessions, locks)
- Wrong: async with channel.send("hi")  
- Right: await channel.send("hi")  
- Wrong: async with bot.fetch_user(123)  
- Right: user = await bot.fetch_user(123)  

⚠️ WEB HELPER RULES (CRITICAL):
- The 'web' object methods (get, post, fetch_json) return DATA, not context managers.
- NEVER use 'async with web.get(...)'.
- CORRECT: response = await web.get("url")
- CORRECT: data = await web.fetch_json("url")
- 'web.get' returns a dict with keys: 'status', 'text', 'headers'
- 'web.fetch_json' returns a dict (parsed JSON) or None

⚠️ CRITICAL ATTRIBUTE-SAFETY RULES:
- ALWAYS verify object attributes/methods before using them (`hasattr(obj, "name")`).
- Prefer safe fallback access: `value = getattr(obj, "name", None)`.
- For async iterables/generators, iterate with `async for item in stream:` instead of attribute access.
- NEVER assume an object has `.send`, `.edit`, `.reply`, `.history`, `.content`, etc without checking first.
- If required attribute is missing, send a helpful message and return early.

CONTEXT:
User: {user_name} (ID: {user_id})
Server: {guild_info}
Channel: {channel_info}
Request: "{content}"

AVAILABLE VARIABLES:
message, bot, channel, guild, author, task_manager, storage, web, files, discord, asyncio, datetime, json, logger

EXAMPLE TEMPLATE (COPY THIS STRUCTURE EXACTLY):
```python
try:
    if not guild:
        await channel.send("This needs a server")
        return
    
    # Your code here
    result = await guild.create_text_channel("test")
    await channel.send(f"✅ Created {{result.mention}}")
    
except discord.Forbidden:
    await channel.send("❌ Missing permissions")
except Exception as e:
    await channel.send(f"❌ Error: {{e}}")
    logger.error(f"Error: {{e}}")
```

RULES:
1. Start code at column 0
2. Wrap everything in try/except
3. Check if guild exists for server commands
4. Check permissions before operations
5. Use f-strings for formatting
6. Log errors with logger.error()
7. Send user-friendly error messages

Now generate code for: "{content}"
"""
    
    @classmethod
    def build(cls, message: discord.Message, content: str) -> str:
        try:
            guild_info = "DM"
            if message.guild:
                try:
                    guild_info = f"{message.guild.name} (ID: {message.guild.id})"
                except Exception:
                    guild_info = f"Server (ID: {message.guild.id})"
            
            channel_info = "DM"
            if isinstance(message.channel, discord.DMChannel):
                channel_info = "DM"
            elif isinstance(message.channel, discord.TextChannel): 
                channel_info = message.channel.name
            else:
                channel_info = "Channel" 
            
            user_name = "Unknown"
            if message.author:
                try:
                    user_name = str(message.author.name)[:100]
                except Exception:
                    user_name = "User"
            
            user_id = 0
            if message.author:
                try:
                    user_id = message.author.id
                except Exception:
                    user_id = 0
            
            content_safe = ""
            try:
                content_safe = str(content)[:2000]
            except Exception:
                content_safe = "Unknown request"
            
            return cls.SYSTEM_PROMPT.format(
                user_name=user_name,
                user_id=user_id,
                guild_info=guild_info,
                channel_info=channel_info,
                content=content_safe
            )
        except Exception as e:
            logger.error(f"Prompt build error: {e}")
            logger.error(traceback.format_exc())
            try:
                return cls.SYSTEM_PROMPT.format(
                    user_name="Unknown",
                    user_id=0,
                    guild_info="Unknown",
                    channel_info="Unknown",
                    content="Help request"
                )
            except Exception as e2:
                logger.error(f"Fallback prompt build error: {e2}")
                return "You are a Discord bot assistant. Generate safe Python code to help the user."

class AdvancedCodeExecutor:
    """Ultra-safe code execution with maximum Discord.py capabilities"""
    
    def __init__(self, timeout: int = 30):
        self.timeout = max(5, min(300, timeout))
        self.max_output_size = 10000
        logger.info(f"CodeExecutor initialized with {self.timeout}s timeout")
    
    def _get_safe_builtins(self) -> dict:
        """Comprehensive builtins for maximum functionality"""
        try:
            return {
                # Core Python builtins
                '__import__': __import__,
                '__build_class__': __build_class__,
                '__name__': '__main__',
                'print': print,
                'len': len,
                'str': str,
                'int': int,
                'float': float,
                'bool': bool,
                'list': list,
                'dict': dict,
                'tuple': tuple,
                'set': set,
                'frozenset': frozenset,
                'bytes': bytes,
                'bytearray': bytearray,
                'range': range,
                'slice': slice,
                'enumerate': enumerate,
                'zip': zip,
                'map': map,
                'filter': filter,
                'reversed': reversed,
                'sorted': sorted,
                
                # Math & Numbers
                'sum': sum,
                'max': max,
                'min': min,
                'abs': abs,
                'round': round,
                'pow': pow,
                'divmod': divmod,
                'hex': hex,
                'oct': oct,
                'bin': bin,
                'ord': ord,
                'chr': chr,
                
                # Type checking
                'type': type,
                'isinstance': isinstance,
                'issubclass': issubclass,
                'hasattr': hasattr,
                'getattr': getattr,
                'setattr': setattr,
                'delattr': delattr,
                'callable': callable,
                
                # Introspection
                'dir': dir,
                'vars': vars,
                'id': id,
                'hash': hash,
                'repr': repr,
                'format': format,
                
                # Iteration
                'iter': iter,
                'next': next,
                'any': any,
                'all': all,
                
                # Object creation
                'object': object,
                'property': property,
                'staticmethod': staticmethod,
                'classmethod': classmethod,
                'super': super,

                # Namespace / execution helpers
                'globals': globals,
                'locals': locals,
                'exec': exec,
                'eval': eval,
                'compile': compile,
                'open': open,
                'input': input,
                
                # Exceptions - All major types
                'Exception': Exception,
                'BaseException': BaseException,
                'SystemExit': SystemExit,
                'KeyboardInterrupt': KeyboardInterrupt,
                'StopIteration': StopIteration,
                'StopAsyncIteration': StopAsyncIteration,
                'ValueError': ValueError,
                'TypeError': TypeError,
                'KeyError': KeyError,
                'IndexError': IndexError,
                'AttributeError': AttributeError,
                'RuntimeError': RuntimeError,
                'TimeoutError': TimeoutError,
                'OSError': OSError,
                'IOError': IOError,
                'NameError': NameError,
                'ZeroDivisionError': ZeroDivisionError,
                'ImportError': ImportError,
                'ModuleNotFoundError': ModuleNotFoundError,
                'NotImplementedError': NotImplementedError,
                'MemoryError': MemoryError,
                'RecursionError': RecursionError,
                'OverflowError': OverflowError,
                'SyntaxError': SyntaxError,
                'IndentationError': IndentationError,
                'AssertionError': AssertionError,
                'UnicodeError': UnicodeError,
                'PermissionError': PermissionError,
                'FileNotFoundError': FileNotFoundError,
                
                # Constants
                'True': True,
                'False': False,
                'None': None,
                'Ellipsis': Ellipsis,
                'NotImplemented': NotImplemented,
            }
        except Exception as e:
            logger.error(f"Builtins error: {e}")
            return {}
    
    def _create_namespace(self, message, bot, cog, task_manager, storage, web, files) -> dict:
        """Create comprehensive namespace with full Discord.py access"""
        try:
            safe_builtins = self._get_safe_builtins()
            
            # Import standard library modules
            import os
            import sys
            import platform
            import subprocess
            import shutil
            import inspect
            import glob
            import random
            import math
            import time
            import collections
            import itertools
            import functools
            import statistics
            import re
            import json
            import base64
            import hashlib
            import uuid
            import copy
            
            # Import Discord utilities (with error handling)
            discord_utils = None
            discord_errors = None
            discord_enums = None
            
            try:
                from discord import utils as discord_utils
            except Exception:
                logger.warning("Could not import discord.utils")
            
            try:
                from discord import errors as discord_errors
            except Exception:
                logger.warning("Could not import discord.errors")
            
            try:
                from discord import enums as discord_enums
            except Exception:
                logger.warning("Could not import discord.enums")
            
            # Core namespace with all Discord objects
            namespace = {
                # Discord Context - Full access
                'message': message,
                'bot': bot,
                'channel': message.channel,
                'guild': message.guild,
                'author': message.author,
                'ctx': message,  # Alias for command-style access
                
                # Discord.py Core
                'discord': discord,
                'commands': commands,
            }
            
            # Safely add Discord models
            discord_models = {
                'User': 'User',
                'Member': 'Member',
                'Guild': 'Guild',
                'TextChannel': 'TextChannel',
                'VoiceChannel': 'VoiceChannel',
                'CategoryChannel': 'CategoryChannel',
                'Thread': 'Thread',
                'Role': 'Role',
                'Emoji': 'Emoji',
                'PartialEmoji': 'PartialEmoji',
                'Message': 'Message',
                'Reaction': 'Reaction',
                'Permissions': 'Permissions',
                'PermissionOverwrite': 'PermissionOverwrite',
                'Invite': 'Invite',
                'Webhook': 'Webhook',
                'Integration': 'Integration',
                'VoiceState': 'VoiceState',
                'Embed': 'Embed',
                'File': 'File',
                'Colour': 'Colour',
                'Color': 'Color',
                'ButtonStyle': 'ButtonStyle',
                'SelectOption': 'SelectOption',
                'Status': 'Status',
                'ActivityType': 'ActivityType',
                'ChannelType': 'ChannelType',
                'MessageType': 'MessageType',
                'VerificationLevel': 'VerificationLevel',
                'AuditLogAction': 'AuditLogAction',
            }
            
            for key, attr_name in discord_models.items():
                try:
                    namespace[key] = getattr(discord, attr_name)
                except AttributeError:
                    logger.debug(f"Discord attribute not found: {attr_name}")
            
            # Add Discord utilities if available
            if discord_utils:
                namespace['utils'] = discord_utils
                try:
                    namespace['get'] = discord_utils.get
                    namespace['find'] = discord_utils.find
                    namespace['escape_markdown'] = discord_utils.escape_markdown
                    namespace['escape_mentions'] = discord_utils.escape_mentions
                except AttributeError:
                    pass
            
            # Add Discord errors if available
            if discord_errors:
                namespace['errors'] = discord_errors
            
            # Always add core Discord exceptions
            discord_exceptions = {
                'DiscordException': 'DiscordException',
                'HTTPException': 'HTTPException',
                'Forbidden': 'Forbidden',
                'NotFound': 'NotFound',
                'InvalidData': 'InvalidData',
                'LoginFailure': 'LoginFailure',
                'ConnectionClosed': 'ConnectionClosed',
            }
            
            for key, exc_name in discord_exceptions.items():
                try:
                    namespace[key] = getattr(discord, exc_name)
                except AttributeError:
                    logger.debug(f"Discord exception not found: {exc_name}")
            
            # Async tools
            namespace.update({
                'asyncio': asyncio,
                'Task': asyncio.Task,
                'Queue': asyncio.Queue,
                'Event': asyncio.Event,
                'Lock': asyncio.Lock,
                'Semaphore': asyncio.Semaphore,
                'sleep': asyncio.sleep,
                'wait_for': asyncio.wait_for,
                'gather': asyncio.gather,
                'create_task': asyncio.create_task,
            })
            
            # DateTime
            namespace.update({
                'datetime': datetime,
                'timedelta': datetime.timedelta,
            })
            
            # Custom Tools
            namespace.update({
                'task_manager': task_manager,
                'storage': storage,
                'web': web,
                'files': files,
                'cog': cog,
            })
            
            # System tools
            namespace.update({
                'os': os,
                'sys': sys,
                'subprocess': subprocess,
                'shutil': shutil,
                'platform': platform,
                'glob': glob,
            })
            
            # Standard library
            namespace.update({
                'random': random,
                'math': math,
                'time': time,
                'collections': collections,
                'itertools': itertools,
                'functools': functools,
                'statistics': statistics,
                're': re,
                'json': json,
                'base64': base64,
                'hashlib': hashlib,
                'uuid': uuid,
                'copy': copy,
                'inspect': inspect,
            })
            
            # Logging
            namespace.update({
                'traceback': traceback,
                'logger': logger,
            })
            
            # Helper functions
            namespace['print_to_channel'] = self._create_print_helper(message.channel)
            
            # Add builtins
            namespace.update(safe_builtins)
            namespace['__builtins__'] = safe_builtins
            
            return namespace
            
        except Exception as e:
            logger.error(f"Namespace creation error: {e}")
            logger.error(traceback.format_exc())
            # Return minimal safe namespace
            return {
                '__builtins__': self._get_safe_builtins(),
                'message': message,
                'bot': bot,
                'channel': message.channel,
                'guild': message.guild,
                'author': message.author,
                'discord': discord,
                'asyncio': asyncio,
                'datetime': datetime,
                'logger': logger,
            }
    
    def _create_print_helper(self, channel):
        """Create a helper function to send messages from print statements"""
        async def print_to_channel(*args, **kwargs):
            try:
                message = ' '.join(str(arg) for arg in args)
                if message:
                    await channel.send(message[:2000])
            except Exception as e:
                logger.error(f"Print helper error: {e}")
        return print_to_channel
    
    def _fix_indentation_aggressively(self, code: str) -> str:
        """Standardize indentation using textwrap"""
        try:
            # First, try standard dedent which fixes most AI output issues
            dedented = textwrap.dedent(code)
            
            # Check if it parses
            try:
                ast.parse(dedented)
                return dedented
            except SyntaxError:
                pass
            
            # If standard dedent fails, it might be because the first line has no indent
            # but subsequent lines do. Let's try to detect the common indent.
            lines = code.split('\n')
            if not lines:
                return code
                
            # Filter out empty lines for analysis
            non_empty_lines = [l for l in lines if l.strip()]
            if not non_empty_lines:
                return code
                
            # If it parses as-is, return it
            try:
                ast.parse(code)
                return code
            except SyntaxError:
                pass

            return dedented # Fallback to dedented version as it's usually best
            
        except Exception as e:
            logger.error(f"Indentation fix error: {e}")
            return code


    def _normalize_code(self, code: str) -> str:
        """Normalize code by removing problematic characters and fixing common issues"""
        try:
            # Remove any non-ASCII characters that might cause issues
            # But keep newlines and basic punctuation
            normalized = ''
            for char in code:
                # Allow ASCII printable, newlines, tabs
                if ord(char) < 128 or char in '\n\t':
                    normalized += char
                else:
                    # Replace with space for safety
                    normalized += ' '
            
            # Remove any excessive blank lines
            lines = normalized.split('\n')
            cleaned_lines = []
            prev_blank = False
            
            for line in lines:
                is_blank = not line.strip()
                if is_blank:
                    if not prev_blank:  # Allow one blank line
                        cleaned_lines.append('')
                    prev_blank = True
                else:
                    cleaned_lines.append(line)
                    prev_blank = False
            
            return '\n'.join(cleaned_lines)
            
        except Exception as e:
            logger.error(f"Code normalization error: {e}")
            return code
    
    def _wrap_code(self, code: str) -> str:
        """Wrap code in async function with robust indentation"""
        try:
            if not code or not isinstance(code, str):
                return "async def __exec():\n    pass\n\n__task = asyncio.create_task(__exec())"
            
            # Normalize and fix indentation of the payload
            code = self._normalize_code(code)
            code = self._fix_indentation_aggressively(code)
            
            if not code.strip():
                return "async def __exec():\n    pass\n\n__task = asyncio.create_task(__exec())"
            
            # Construct the wrapper using explicit string concatenation to avoid f-string multiline pitfalls
            wrapper_lines = []
            wrapper_lines.append("async def __exec():")
            wrapper_lines.append("    try:")
            
            # Indent user code by 8 spaces (2 levels: function + try block)
            # Use textwrap.indent for reliability
            indented_code = textwrap.indent(code, '        ')
            wrapper_lines.append(indented_code)
            
            # Add exception handlers (at indent level 4)
            handlers = """
    except TypeError as e:
        error_msg = str(e)
        if "async context manager" in error_msg or "asynchronous context manager" in error_msg:
            await channel.send("ERROR: Tried to use 'async with' on something that isn't an async context manager. Use 'await' instead.")
        else:
            await channel.send(f"Type error: {str(e)[:500]}")
        logger.error(f"TypeError: {e}")
    except discord.Forbidden as e:
        await channel.send(f"Permission denied: {e}")
        logger.error(f"Forbidden: {e}")
    except discord.HTTPException as e:
        if hasattr(e, 'status') and e.status == 429:
            await channel.send("Rate limited! Please wait a moment.")
        else:
            await channel.send(f"Discord API error: {e}")
        logger.error(f"HTTPException: {e}")
    except discord.NotFound as e:
        await channel.send(f"Resource not found: {e}")
        logger.error(f"NotFound: {e}")
    except asyncio.TimeoutError:
        await channel.send("Operation timed out")
        logger.error("Timeout in generated code")
    except Exception as e:
        await channel.send(f"Error: {str(e)[:500]}")
        logger.error(f"Execution error: {e}")
        logger.error(traceback.format_exc())"""
            
            wrapper_lines.append(handlers)
            
            # Add task creation (at root level)
            wrapper_lines.append("")
            wrapper_lines.append("__task = asyncio.create_task(__exec())")
            
            wrapped = "\n".join(wrapper_lines)
            
            return wrapped
            
        except Exception as e:
            logger.error(f"Code wrap error: {e}")
            return "async def __exec():\n    pass\n\n__task = asyncio.create_task(__exec())"
    
    async def execute(self, message, code, bot, cog, task_manager, storage, web, files) -> tuple[bool, Optional[str]]:
        """Execute code with comprehensive error handling and rate limit support"""
        try:
            if not code or not isinstance(code, str):
                return False, "Invalid code provided"
            
            # Check if task manager is shutting down
            if hasattr(task_manager, 'shutdown_flag') and task_manager.shutdown_flag:
                return False, "Task manager is shutting down, execution cancelled"
            
            # Create namespace
            namespace = self._create_namespace(message, bot, cog, task_manager, storage, web, files)
            if not namespace or '__builtins__' not in namespace:
                return False, "Failed to create execution namespace"
            
            # Wrap code
            wrapped = self._wrap_code(code)
            
            # Execute with comprehensive error handling
            try:
                # Compile and execute
                exec(wrapped, namespace)
                
                if '__task' not in namespace:
                    return False, "Failed to create execution task"
                
                exec_task = namespace['__task']
                
                # Wait for completion with timeout
                try:
                    await asyncio.wait_for(exec_task, timeout=self.timeout)
                    return True, None
                    
                except asyncio.CancelledError:
                    logger.info("Execution cancelled")
                    return False, "Execution cancelled"
                    
                except asyncio.TimeoutError:
                    error_msg = f"⏱️ Timeout after {self.timeout}s - use task_manager.create_task() for long operations"
                    logger.warning(error_msg)
                    return False, error_msg
                
            except discord.Forbidden as e:
                error_msg = f"🔒 Missing Discord permissions: {e}"
                logger.warning(error_msg)
                return False, error_msg
            
            except discord.HTTPException as e:
                if hasattr(e, 'status') and e.status == 429:
                    error_msg = "⏱️ Rate limited by Discord API - please wait"
                    logger.warning(f"Rate limited: {e}")
                else:
                    error_msg = f"🌐 Discord API error: {e}"
                    logger.error(error_msg)
                return False, error_msg
            
            except discord.NotFound as e:
                error_msg = f"❓ Discord resource not found: {e}"
                logger.warning(error_msg)
                return False, error_msg
            
            except discord.InvalidData as e:
                error_msg = f"📛 Invalid data sent to Discord: {e}"
                logger.error(error_msg)
                return False, error_msg
            
            except SyntaxError as e:
                error_msg = f"📝 Syntax error on line {e.lineno}: {e.msg}"
                logger.error(f"Syntax error: {e}")
                return False, error_msg
            
            except NameError as e:
                error_msg = f"🔤 Name error: {e}"
                logger.error(error_msg)
                return False, error_msg
            
            except AttributeError as e:
                error_msg = f"🔍 Attribute error: {e}"
                logger.error(error_msg)
                return False, error_msg
            
            except TypeError as e:
                error_msg = f"🔧 Type error: {e}"
                logger.error(error_msg)
                return False, error_msg
            
            except ValueError as e:
                error_msg = f"💢 Value error: {e}"
                logger.error(error_msg)
                return False, error_msg
            
            except KeyError as e:
                error_msg = f"🔑 Key error: {e}"
                logger.error(error_msg)
                return False, error_msg
            
            except IndexError as e:
                error_msg = f"📊 Index error: {e}"
                logger.error(error_msg)
                return False, error_msg
            
            except ZeroDivisionError as e:
                error_msg = f"➗ Division by zero: {e}"
                logger.error(error_msg)
                return False, error_msg
            
            except MemoryError:
                error_msg = "🧠 Out of memory - operation too large"
                logger.error("Memory error in execution")
                return False, error_msg
            
            except RecursionError:
                error_msg = "🔄 Recursion limit exceeded"
                logger.error("Recursion error in execution")
                return False, error_msg
            
            except Exception as e:
                error_msg = f"❌ Execution error: {str(e)[:500]}"
                logger.error(f"Execution error:\n{traceback.format_exc()}")
                return False, error_msg
                
        except Exception as e:
            error_msg = f"💥 Critical error: {str(e)[:500]}"
            logger.error(f"Critical execution error:\n{traceback.format_exc()}")
            return False, error_msg
    
    def validate_code(self, code: str) -> tuple[bool, Optional[str]]:
        """Validate code syntax before execution"""
        try:
            if not code:
                return False, "Empty code"
            
            # Try to parse
            try:
                ast.parse(code)
                return True, None
            except SyntaxError as e:
                return False, f"Syntax error on line {e.lineno}: {e.msg}"
            
        except Exception as e:
            return False, f"Validation error: {e}"
    
    def get_stats(self) -> dict:
        """Get executor statistics"""
        return {
            'timeout': self.timeout,
            'max_output_size': self.max_output_size,
        }
        
class ComplexAICog(commands.Cog):
    """AI cog with ultra-robust error handling"""
    
    def __init__(self, bot):
        try:
            self.bot = bot
            self.model = None
            self.initialization_failed = False
            
            try:
                api_key = self.bot.config_manager.get_gemini_api_key()
                if not api_key:
                    raise ValueError("No Gemini API key found")
                
                genai.configure(api_key=api_key)
                # CHANGED: Use a stable model name to prevent 404s
                self.model = genai.GenerativeModel('gemini-2.5-flash-lite')
                logger.info("✅ Gemini initialized successfully")
                
            except Exception as e:
                logger.error(f"❌ Gemini initialization failed: {e}")
                self.initialization_failed = True
            
            try:
                self.storage = EnhancedStorage()
                self.task_manager = AdvancedTaskManager(storage=self.storage, bot=self.bot)
                self.web = WebHelper()
                self.files = FileHelper()
                self.permission_checker = PermissionChecker()
                self.code_extractor = CodeExtractor()
                self.prompt_builder = AdvancedPromptBuilder()
                self.code_executor = AdvancedCodeExecutor(timeout=120)
                
                logger.info("✅ All components initialized")
                
            except Exception as e:
                logger.error(f"❌ Component initialization failed: {e}")
                raise
            
            self.ai_timeout = 25.0
            self.generation_config = genai.types.GenerationConfig(
                temperature=0.2,
                max_output_tokens=8192
            )
            
            logger.info("🤖 Complex AI Cog initialized")
            
        except Exception as e:
            logger.error(f"💥 Cog initialization failed: {e}")
            raise
    
    @property
    def owner_id(self):
        """Helper to get owner ID reliably"""
        if self.bot.owner_id:
            return self.bot.owner_id
        if self.bot.owner_ids:
            return list(self.bot.owner_ids)[0]
        return None

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Handle messages with comprehensive error handling"""
        try:
            if not self.model or self.initialization_failed:
                return
            
            if not message or not message.author:
                return
            
            # Fetch owner ID if missing (needed for permission check)
            owner_id = self.owner_id
            if owner_id is None and self.bot.is_ready():
                 try:
                     app_info = await self.bot.application_info()
                     self.bot.owner_id = app_info.owner.id
                     owner_id = self.bot.owner_id
                 except Exception as e:
                     logger.error(f"Could not fetch owner ID: {e}")
            
            if not self.permission_checker.can_use_ai_commands(
                message, owner_id, self.bot.user
            ):
                return
            
            await self.process_request(message)
            
        except Exception as e:
            logger.error(f"Message handling error: {e}")
            logger.error(traceback.format_exc())
    
    async def cog_unload(self):
        try:
            logger.info("🔄 Unloading AI cog, stopping all tasks...")
            
            if hasattr(self, 'task_manager'):
                stopped = await self.task_manager.stop_all_tasks()
                logger.info(f"✅ Stopped {stopped} task(s)")
            
            if hasattr(self, 'web'):
                await self.web.close()
            
            logger.info("✅ AI cog unloaded successfully")
            
        except Exception as e:
            logger.error(f"Error during cog unload: {e}")
            logger.error(traceback.format_exc())
    
    async def process_request(self, message: discord.Message):
        try:
            content = self._clean_content(message)
            if not content:
                # Ignore empty messages (e.g. just image with no text)
                return

            if self._is_command_inventory_request(content):
                await self._send_command_inventory(message)
                return
            
            # Show typing indicator while processing
            async with message.channel.typing():
                ai_response = await self._get_ai_response(message, content)
            
            if ai_response:
                await self._handle_ai_response(message, ai_response)
            else:
                try:
                    await message.add_reaction("❌")
                except Exception:
                    pass
                
        except Exception as e:
            logger.error(f"Request processing error: {e}")
            try:
                await message.add_reaction("⚠️")
            except Exception:
                pass
    
    def _clean_content(self, message: discord.Message) -> str:
        try:
            if not message or not message.content:
                return ""
            
            content = str(message.content)
            
            if self.bot.user:
                content = content.replace(f'<@{self.bot.user.id}>', '')
                content = content.replace(f'<@!{self.bot.user.id}>', '')
            
            return content.strip()[:2000]
            
        except Exception as e:
            logger.error(f"Content cleaning error: {e}")
            return ""

    def _is_command_inventory_request(self, content: str) -> bool:
        """Detect requests asking for command-to-cog mapping."""
        try:
            normalized = (content or "").lower()
            if not normalized:
                return False

            key_phrases = [
                "which command",
                "which cog",
                "what cog",
                "command list",
                "list commands",
                "show commands",
                "commands in",
                "cog commands",
                "command mapping",
                "what commands",
            ]
            return any(phrase in normalized for phrase in key_phrases)
        except Exception as e:
            logger.error(f"Inventory request detection error: {e}")
            return False

    def _build_command_inventory_lines(self) -> List[str]:
        """Build a readable list of all commands grouped by cog."""
        lines: List[str] = []
        try:
            if not self.bot:
                return ["Bot instance unavailable."]

            grouped: Dict[str, List[str]] = defaultdict(list)
            for cmd in self.bot.commands:
                cog_name = cmd.cog_name or "No Cog"
                aliases = f" (aliases: {', '.join(cmd.aliases)})" if getattr(cmd, 'aliases', None) else ""
                help_text = f" - {cmd.help.strip()}" if getattr(cmd, 'help', None) else ""
                grouped[cog_name].append(f"!{cmd.name}{aliases}{help_text}")

            if not grouped:
                return ["No registered commands were found."]

            for cog_name in sorted(grouped.keys(), key=lambda x: x.lower()):
                lines.append(f"[{cog_name}]")
                for cmd_line in sorted(grouped[cog_name], key=lambda x: x.lower()):
                    lines.append(f"- {cmd_line}")
                lines.append("")

            return lines
        except Exception as e:
            logger.error(f"Failed building command inventory: {e}")
            return [f"Error building command inventory: {str(e)[:200]}"]

    async def _send_command_inventory(self, message: discord.Message):
        """Send command-to-cog mapping in Discord-safe chunks."""
        try:
            lines = self._build_command_inventory_lines()
            body = "\n".join(lines).strip()
            if not body:
                body = "No command inventory available."

            chunks: List[str] = []
            current = ""
            for line in body.splitlines():
                candidate = f"{current}\n{line}".strip() if current else line
                if len(candidate) <= 1900:
                    current = candidate
                    continue

                if current:
                    chunks.append(current)
                current = line

            if current:
                chunks.append(current)

            header = "📚 Command map (command -> cog):"
            if chunks:
                await message.reply(f"{header}\n```\n{chunks[0]}\n```")
                for extra_chunk in chunks[1:]:
                    await message.channel.send(f"```\n{extra_chunk}\n```")
            else:
                await message.reply(f"{header}\nNo commands found.")

        except Exception as e:
            logger.error(f"Send command inventory error: {e}")
            try:
                await message.reply(f"❌ Failed to build command inventory: {str(e)[:200]}")
            except Exception:
                pass

    async def _get_ai_response(self, message: discord.Message, content: str) -> Optional[str]:
        try:
            if not self.model:
                return None
            
            prompt = self.prompt_builder.build(message, content)
            if not prompt:
                return None
            
            try:
                response = await asyncio.wait_for(
                    self.model.generate_content_async(
                        prompt,
                        generation_config=self.generation_config
                    ),
                    timeout=self.ai_timeout
                )
                
                if response and hasattr(response, 'text') and response.text:
                    return response.text[:50000]
                
                return None
                
            except asyncio.TimeoutError:
                logger.warning(f"AI timeout for user {message.author.id}")
                return None
                
            except Exception as e:
                logger.error(f"AI generation error: {e}")
                return None
                
        except Exception as e:
            logger.error(f"AI response error: {e}")
            return None
    
    async def _handle_ai_response(self, message: discord.Message, ai_response: str):
        """Handle AI response with enhanced logging"""
        try:
            code_blocks = self.code_extractor.extract_code_blocks(ai_response)
            
            if not code_blocks:
                try:
                    response_text = ai_response.strip()[:2000]
                    if response_text:
                        await message.reply(response_text)
                    else:
                        await message.add_reaction("❓")
                except discord.Forbidden:
                    logger.warning("Cannot send message - forbidden")
                except discord.HTTPException as e:
                    logger.error(f"Failed to send response: {e}")
                except Exception as e:
                    logger.error(f"Reply error: {e}")
                return
            
            # Log the extracted code for debugging
            extracted_code = code_blocks[0]
            logger.info(f"Extracted code for user {message.author.id} ({len(extracted_code)} chars)")
            logger.debug(f"Code preview: {extracted_code[:200]}...")
            
            # Check for obvious problems before execution
            if not extracted_code.strip():
                logger.warning("Empty code block extracted")
                await message.add_reaction("⚠️")
                return
            
            # Execute the code
            await self._execute_code(message, extracted_code)
            
        except Exception as e:
            logger.error(f"Response handling error: {e}")
            logger.error(traceback.format_exc())
            try:
                await message.add_reaction("⚠️")
            except Exception:
                pass


    async def _execute_code(self, message: discord.Message, code: str):
        """Execute code with enhanced error handling and logging"""
        try:
            logger.info(f"Executing code for user {message.author.id}")
            
            # Log first few lines for debugging (without sensitive data)
            code_preview = '\n'.join(code.split('\n')[:5])
            logger.debug(f"Code preview:\n{code_preview}")
            
            success, error = await self.code_executor.execute(
                message, code, self.bot, self,
                self.task_manager, self.storage, self.web, self.files
            )
            
            if success:
                logger.info(f"✅ Code executed successfully for user {message.author.id}")
                try:
                    await message.add_reaction("✅")
                except Exception:
                    pass
            else:
                logger.error(f"❌ Code execution failed for user {message.author.id}")
                if error:
                    logger.error(f"Error details: {error[:500]}")
                
                try:
                    await message.add_reaction("❌")
                except Exception:
                    pass
                
                # Send error message to user if it's actionable
                if error and any(keyword in error.lower() for keyword in 
                            ['syntax', 'indentation', 'async with', 'await']):
                    try:
                        await message.channel.send(f"⚠️ {error[:500]}")
                    except Exception:
                        pass
                
        except Exception as e:
            logger.error(f"Code execution wrapper error: {e}")
            logger.error(traceback.format_exc())
            try:
                await message.add_reaction("💥")
            except Exception:
                pass
    
    # --- Commands ---
    
    @commands.command(name="tasks")
    @commands.is_owner()
    async def list_tasks(self, ctx):
        """List all tasks"""
        try:
            all_tasks = self.task_manager.get_all_tasks()
            
            if not all_tasks:
                await ctx.send("📋 No active tasks")
                return
            
            embed = discord.Embed(title="Background Tasks", color=discord.Color.blue())
            
            for task_id, info in list(all_tasks.items())[:25]:
                status_emoji = {
                    'running': '🔄', 'completed': '✅', 'finished': '✅',
                    'failed': '❌', 'cancelled': '⛔', 'paused': '⏸️'
                }.get(info.get('status', 'unknown'), '❓')
                
                value = f"Status: {status_emoji} {info.get('status', 'unknown')}"
                if 'created_at' in info:
                    try:
                        value += f"\nCreated: {info['created_at'][:19]}"
                    except Exception:
                        pass
                
                embed.add_field(
                    name=str(task_id)[:256],
                    value=value[:1024],
                    inline=True
                )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"List tasks error: {e}")
            await ctx.send("❌ Error listing tasks")
    
    @commands.command(name="stop_all_tasks")
    @commands.is_owner()
    async def stop_all_tasks_cmd(self, ctx):
        """Stop all running tasks (owner only)"""
        try:
            msg = await ctx.send("🛑 Stopping all tasks...")
            stopped = await self.task_manager.stop_all_tasks()
            await msg.edit(content=f"✅ Stopped {stopped} task(s)")
        except Exception as e:
            logger.error(f"Stop all tasks error: {e}")
            await ctx.send(f"❌ Error stopping tasks: {str(e)[:200]}")
    
    @commands.command(name="cleanup_tasks")
    @commands.is_owner()
    async def cleanup_tasks_cmd(self, ctx, hours: int = 24):
        """Clean up old completed tasks (owner only)"""
        try:
            hours = max(1, min(168, hours))
            removed = self.task_manager.cleanup_old_tasks(hours)
            await ctx.send(f"🧹 Cleaned up {removed} task(s) older than {hours} hours")
        except Exception as e:
            logger.error(f"Cleanup tasks error: {e}")
            await ctx.send(f"❌ Error cleaning up tasks: {str(e)[:200]}")
    
    @commands.command(name="reload")
    @commands.is_owner()
    async def reload_cog(self, ctx):
        """Reload the AI cog (owner only)"""
        try:
            msg = await ctx.send("🔄 Reloading AI cog...")
            
            try:
                await self.bot.unload_extension('cogs.gemini_cog')
                logger.info("✅ Unloaded gemini_cog")
                await asyncio.sleep(0.5)
                await self.bot.load_extension('cogs.gemini_cog')
                logger.info("✅ Loaded gemini_cog")
                await msg.edit(content="✅ AI cog reloaded successfully!")
            except commands.ExtensionNotLoaded:
                await self.bot.load_extension('cogs.gemini_cog')
                await msg.edit(content="✅ AI cog loaded successfully!")
                logger.info("✅ Loaded gemini_cog (was not loaded)")
            except commands.ExtensionNotFound:
                await msg.edit(content="❌ Error: gemini_cog.py not found in cogs folder")
                logger.error("gemini_cog.py not found")
            except commands.ExtensionFailed as e:
                await msg.edit(content=f"❌ Failed to load cog: {str(e)[:100]}")
                logger.error(f"Extension failed: {e}")
                logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(f"Reload command error: {e}")
            logger.error(traceback.format_exc())
            try:
                await ctx.send(f"❌ Error reloading cog: {str(e)[:200]}")
            except Exception:
                pass

async def setup(bot):
    """Load cog with error handling"""
    try:
        if not bot:
            logger.error("Invalid bot object")
            return
        
        if not hasattr(bot, 'config_manager'):
            logger.error("Bot missing config_manager")
            return
        
        try:
            api_key = bot.config_manager.get_gemini_api_key()
            if not api_key:
                logger.error("No Gemini API key found")
                return
        except Exception as e:
            logger.error(f"Error getting API key: {e}")
            return
        
        await bot.add_cog(ComplexAICog(bot))
        logger.info("✅ Complex AI Cog loaded successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to load cog: {e}")
        logger.error(traceback.format_exc())
