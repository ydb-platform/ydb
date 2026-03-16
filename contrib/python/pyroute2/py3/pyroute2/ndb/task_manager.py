import asyncio
import logging
import threading
import time
import traceback

from pyroute2 import config

from . import schema
from .events import (
    DBMExitException,
    InvalidateHandlerException,
    RescheduleException,
    ShutdownException,
    State,
)
from .messages import cmsg_event, cmsg_failed

log = logging.getLogger(__name__)


class TaskAdapter:
    def __init__(self):
        self.event = asyncio.Event()
        self.state = State()


class Task:
    def __init__(self, coro, target_state, obj):
        self.exception = None
        self.coro = coro
        self.target_state = target_state
        self.obj = obj if obj is not None else TaskAdapter()
        self.restart()

    def restart(self):
        self.task = asyncio.create_task(self.coro())
        self.obj.task_id = id(self.task)

    def commit(self):
        self.exception = self.task.exception()

    @property
    def event(self):
        return self.obj.event

    @property
    def state(self):
        return self.obj.state

    @property
    def task_id(self):
        return id(self.task)


class TaskManager:
    def __init__(self, ndb):
        self.ndb = ndb
        self.log = ndb.log
        self.event_map = {}
        self.task_map = {}
        self.event_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.reload_event = asyncio.Event()
        self.thread = None
        self.ctime = self.gctime = time.time()

    def register_handler(self, event, handler):
        if event not in self.event_map:
            self.event_map[event] = []
        self.event_map[event].append(handler)

    def unregister_handler(self, event, handler):
        self.event_map[event].remove(handler)

    async def handler_default(self, sources, target, event):
        if isinstance(getattr(event, 'payload', None), Exception):
            raise event.payload
        log.debug('unsupported event ignored: %s' % type(event))

    async def handler_event(self, sources, target, event):
        event.payload.set()

    async def handler_failed(self, sources, target, event):
        self.ndb.schema.mark(target, 1)

    def create_task(self, coro, state='running', obj=None):
        task = Task(coro, state, obj)
        self.task_map[task.task_id] = task
        self.reload_event.set()
        return task

    def restart_task(self, task):
        task.restart()
        self.task_map[task.task_id] = task
        self.reload_event.set()
        return task

    async def stop(self):
        await self.stop_event.wait()

    async def reload(self):
        await self.reload_event.wait()

    async def task_watch(self):
        while True:
            tasks = list([x.task for x in self.task_map.values()])
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            self.log.debug(f'task done {done}')
            if self.stop_event.is_set():
                return
            for t in done:
                task = self.task_map.pop(id(t))
                task.commit()
                if task.target_state == 'running':
                    self.restart_task(task)
            self.reload_event.clear()

    async def receiver(self):
        while True:
            event = await self.event_queue.get()
            reschedule = []
            handlers = self.event_map.get(
                event.__class__, [self.handler_default]
            )

            for handler in tuple(handlers):
                try:
                    target = event['header']['target']
                    # self.log.debug(f'await {handler} for {event}')
                    await handler(self.ndb.sources.asyncore, target, event)
                except RescheduleException:
                    if 'rcounter' not in event['header']:
                        event['header']['rcounter'] = 0
                    if event['header']['rcounter'] < 3:
                        event['header']['rcounter'] += 1
                        self.log.debug('reschedule %s' % (event,))
                        reschedule.append(event)
                    else:
                        self.log.error('drop %s' % (event,))
                except InvalidateHandlerException:
                    try:
                        handlers.remove(handler)
                    except Exception:
                        self.log.error(
                            'could not invalidate '
                            'event handler:\n%s' % traceback.format_exc()
                        )
                except ShutdownException:
                    return
                except DBMExitException:
                    return
                except Exception:
                    self.log.error(
                        'could not load event:\n%s\n%s'
                        % (event, traceback.format_exc())
                    )
            if time.time() - self.gctime > config.gc_timeout:
                self.gctime = time.time()

    def setup(self):
        self.thread = id(threading.current_thread())
        event_map = {
            cmsg_event: [self.handler_event],
            cmsg_failed: [self.handler_failed],
        }
        self.event_map = event_map
        self.ndb.schema = schema.DBSchema(
            self.ndb.config, self.event_map, self.log.channel('schema')
        )
        for event, handlers in self.ndb.schema.event_map.items():
            for handler in handlers:
                self.register_handler(event, handler)

    async def run(self):
        self.event_loop = asyncio.get_event_loop()
        self.ndb.event_loop = self.event_loop
        self.create_task(self.receiver)
        self.create_task(self.stop)
        self.create_task(self.reload)
        self.ndb._dbm_ready.set()
        await self.task_watch()

    def cleanup(self):
        self.ndb.schema.close()
        self.ndb._dbm_shutdown.set()

    def main(self):
        try:
            self.setup()
        except Exception as e:
            self.ndb._dbm_error = e
            self.ndb._dbm_ready.set()
            return
        asyncio.run(self.run())
        self.cleanup()
