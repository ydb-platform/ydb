import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from ydb.tools.mnc.agent.schemas.task import TaskResult, TaskSchema, TaskStatsSchema


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskBasic(ABC):
    def __init__(self, task_id: Optional[str] = None):
        self.task_id = task_id or str(uuid.uuid4())
        self.status = TaskStatus.PENDING
        self.created_at = time.time()
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self.result: TaskResult = TaskResult(success=False, message="", data=None)
        self.error: Optional[str] = None
        self.delay: Optional[float] = None

    @abstractmethod
    async def do(self) -> Any:
        raise NotImplementedError

    def to_schema(self) -> TaskSchema:
        return TaskSchema(
            id=self.task_id,
            type=self.__class__.__name__,
            status=self.status.value,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            result=self.result,
            error=self.error,
            delay=self.delay,
        )


@dataclass
class TaskWrapper:
    task: TaskBasic
    delay: Optional[float] = None
    future: Optional[asyncio.Future] = None


class TaskService:
    def __init__(self, max_inflight: int = 4):
        self.tasks: Dict[str, TaskBasic] = {}
        self.task_queue = asyncio.Queue()
        self.running = False
        self.max_inflight = max_inflight
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.logger = logging.getLogger(__name__)
        self.worker_tasks: list[asyncio.Task] = []

    async def start(self):
        if self.running:
            return
        self.running = True
        self.semaphore = asyncio.Semaphore(self.max_inflight)
        for _ in range(self.max_inflight):
            self.worker_tasks.append(asyncio.create_task(self._worker_task()))
        self.logger.info("Task service started")

    async def stop(self):
        if not self.running:
            return
        self.running = False
        for worker_task in self.worker_tasks:
            if not worker_task.done():
                worker_task.cancel()
        if self.worker_tasks:
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.worker_tasks.clear()
        self.logger.info("Task service stopped")

    async def _worker_task(self):
        while self.running:
            try:
                task_wrapper = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                async with self.semaphore:
                    await self._execute_task(task_wrapper)
                self.task_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                self.logger.error("Worker task error: %s", exc)

    async def _execute_task(self, task_wrapper: TaskWrapper):
        task = task_wrapper.task
        future = task_wrapper.future

        try:
            if task.status == TaskStatus.CANCELLED:
                task.completed_at = time.time()
                task.result = TaskResult(success=False, message="Task was cancelled", data=None)
                if future and not future.done():
                    future.set_result(task)
                return
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()
            result = await task.do()
            task.status = TaskStatus.COMPLETED
            task.completed_at = time.time()
            task.result = result
            if future and not future.done():
                future.set_result(task)
        except Exception as exc:  # noqa: BLE001
            task.status = TaskStatus.FAILED
            task.completed_at = time.time()
            task.error = str(exc)
            if future and not future.done():
                future.set_exception(exc)
            self.logger.error("Task %s failed: %s", task.task_id, exc)

    async def add_task(self, task: TaskBasic, delay: Optional[float] = None) -> asyncio.Future:
        task.delay = delay
        future = asyncio.get_running_loop().create_future()
        task_wrapper = TaskWrapper(task=task, delay=delay, future=future)
        self.tasks[task.task_id] = task
        if delay and delay > 0:
            asyncio.create_task(self._schedule_delayed_task(task_wrapper, delay))
        else:
            await self.task_queue.put(task_wrapper)
        self.logger.info("Task %s added to queue", task.task_id)
        return future

    async def _schedule_delayed_task(self, task_wrapper: TaskWrapper, delay: float):
        await asyncio.sleep(delay)
        await self.task_queue.put(task_wrapper)

    def get_task(self, task_id: str) -> Optional[TaskBasic]:
        return self.tasks.get(task_id)

    def get_all_tasks(self) -> Dict[str, TaskBasic]:
        return self.tasks.copy()

    def cancel_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task and task.status == TaskStatus.PENDING:
            task.status = TaskStatus.CANCELLED
            self.logger.info("Task %s cancelled", task_id)
            return True
        return False

    def get_task_stats(self) -> TaskStatsSchema:
        total = len(self.tasks)
        pending = sum(1 for t in self.tasks.values() if t.status == TaskStatus.PENDING)
        running = sum(1 for t in self.tasks.values() if t.status == TaskStatus.RUNNING)
        completed = sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED)
        failed = sum(1 for t in self.tasks.values() if t.status == TaskStatus.FAILED)
        cancelled = sum(1 for t in self.tasks.values() if t.status == TaskStatus.CANCELLED)
        current_inflight = self.max_inflight - self.semaphore._value if self.semaphore else 0
        return TaskStatsSchema(
            total=total,
            pending=pending,
            running=running,
            completed=completed,
            failed=failed,
            cancelled=cancelled,
            queue_size=self.task_queue.qsize(),
            max_inflight=self.max_inflight,
            current_inflight=current_inflight,
        )


task_service = TaskService()
