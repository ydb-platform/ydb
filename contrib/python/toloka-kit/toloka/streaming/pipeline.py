__all__ = [
    'Pipeline',
]

import asyncio
from enum import Enum

import itertools
import logging
import signal

from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Container, ContextManager, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import attr.setters

from .observer import BaseObserver
from .storage import BaseStorage
from ..util.async_utils import ComplexException
from ..util._managing_headers import add_headers

logger = logging.getLogger(__name__)


_OrderedSet = dict.fromkeys


@attr.s(eq=False, hash=False)
class _Worker:
    """_Worker object that run observer's __call__() and should_resume() methods.
    Keep track of the observer's should_resume state.

    Attributes:
        name: Unique key to be identified by.
        observer: BaseObserver object to run.
        should_resume: Current observer's should_resume state.
    """
    name: str = attr.ib()
    observer: BaseObserver = attr.ib()
    should_resume: bool = attr.ib(default=False)

    @add_headers('streaming')
    async def __call__(self) -> None:
        if getattr(self.observer, '_enabled', True):
            await self.observer()
            self.should_resume = await self.observer.should_resume()
        else:
            self.should_resume = False

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other) -> bool:
        if not isinstance(other, _Worker):
            return False
        return self.name == other.name

    @classmethod
    def _from_observer(cls, observer: BaseObserver) -> '_Worker':
        return cls(str(observer.get_unique_key()), observer)

    @staticmethod
    def _no_one_should_resume(workers: Iterable['_Worker']) -> bool:
        return all(not worker.should_resume for worker in workers)

    def _is_deleted(self) -> bool:
        return getattr(self.observer, '_deleted', False)


class IterationMode(Enum):
    """
    Possible values:
        * `ALL_COMPLETED` – start next iteration only when all current tasks are done.
        * `FIRST_COMPLETED` – start next iteration as soon as any single task is done.
    """
    ALL_COMPLETED = asyncio.ALL_COMPLETED
    FIRST_COMPLETED = asyncio.FIRST_COMPLETED


@attr.s
class Pipeline:
    """An entry point for toloka streaming pipelines.
    Allow you to register multiple observers and call them periodically
    while at least one of them may resume.

    Attributes:
        period: Period of observers calls. By default, 60 seconds.
        storage: Optional storage object to save pipeline's state.
            Allow to recover from previous state in case of failure.
        iteration_mode: When to start new iteration. Default is `FIRST_COMPLETED`

    Examples:
        Get assignments from segmentation pool and send them for verification to another pool.

        >>> def handle_submitted(events: List[AssignmentEvent]) -> None:
        >>>     verification_tasks = [create_verification_task(item.assignment) for item in events]
        >>>     toloka_client.create_tasks(verification_tasks, open_pool=True)
        >>>
        >>> def handle_accepted(events: List[AssignmentEvent]) -> None:
        >>>     do_some_aggregation([item.assignment for item in events])
        >>>
        >>> async_toloka_client = AsyncTolokaClient.from_sync_client(toloka_client)
        >>>
        >>> observer_123 = AssignmentsObserver(async_toloka_client, pool_id='123')
        >>> observer_123.on_submitted(handle_submitted)
        >>>
        >>> observer_456 = AssignmentsObserver(async_toloka_client, pool_id='456')
        >>> observer_456.on_accepted(handle_accepted)
        >>>
        >>> pipeline = Pipeline()
        >>> pipeline.register(observer_123)
        >>> pipeline.register(observer_456)
        >>> await pipeline.run()
        ...

        One-liners version.

        >>> pipeline = Pipeline()
        >>> pipeline.register(AssignmentsObserver(toloka_client, pool_id='123')).on_submitted(handle_submitted)
        >>> pipeline.register(AssignmentsObserver(toloka_client, pool_id='456')).on_accepted(handle_accepted)
        >>> await pipeline.run()
        ...

        With external storage.

        >>> from toloka.streaming import S3Storage, ZooKeeperLocker
        >>> locker = ZooKeeperLocker(...)
        >>> storage = S3Storage(locker=locker, ...)
        >>> pipeline = Pipeline(storage=storage)
        >>> await pipeline.run()  # Save state after each iteration. Try to load saved at start.
        ...
    """

    MIN_SLEEP_SECONDS = 10  # Allow lock to be taken in concurrent cases.

    period: timedelta = attr.ib(default=timedelta(seconds=60))
    storage: Optional[BaseStorage] = attr.ib(default=None)
    iteration_mode: IterationMode = attr.ib(default=IterationMode.FIRST_COMPLETED)
    name: Optional[str] = attr.ib(default=None, kw_only=True)
    _observers: Dict[Tuple, BaseObserver] = attr.ib(factory=dict, init=False)
    _got_sigint: bool = attr.ib(default=False, init=False)

    @contextmanager
    def _lock(self, key: str) -> ContextManager[Any]:
        if self.storage:
            with self.storage.lock(key) as lock:
                yield lock
                return
        yield None

    def _storage_load(self, pipeline_key: str, workers: Iterable[_Worker]) -> None:
        if self.storage:
            logger.info('Loading state from storage: %s', type(self.storage).__name__)
            observer_by_key = {worker.name: worker.observer for worker in workers}
            state_by_key = self.storage.load(pipeline_key, observer_by_key.keys())
            if state_by_key:
                logger.info('Found saved states count: %d / %d', len(state_by_key), len(observer_by_key))
                for key, injection in state_by_key.items():
                    observer_by_key[key].inject(injection)
            else:
                logger.info('No saved states found')

    def _storage_save(self, pipeline_key: str, workers: Iterable[_Worker]) -> None:
        if self.storage:
            logger.info('Save state to: %s', type(self.storage).__name__)
            observer_by_key = {worker.name: worker.observer for worker in workers}
            self.storage.save(pipeline_key, observer_by_key)
            logger.info('Saved count: %d', len(observer_by_key))

    def _storage_cleanup(self, pipeline_key: str, workers: Iterable[_Worker], lock: Any) -> None:
        if self.storage:
            try:
                keys = {worker.name for worker in workers}
                logger.info('Cleanup storage %s with keys count: %d', type(self.storage).__name__, len(keys))
                self.storage.cleanup(pipeline_key, keys, lock)
            except Exception:
                logger.exception('Got an exception while running cleanup at: %s', type(self.storage).__name__)

    def _get_unique_key(self) -> Tuple:
        return (self.__class__.__name__, self.name or '', tuple(sorted(
            observer.get_unique_key()
            for observer in self._observers.values()
        )))

    def register(self, observer: BaseObserver) -> BaseObserver:
        """Register given observer.

        Args:
            observer: Observer object.

        Returns:
            The same observer object. It's usable to write one-liners.

        Examples:
            Register observer.

            >>> observer = SomeObserver(pool_id='123')
            >>> observer.do_some_preparations(...)
            >>> toloka_loop.register(observer)
            ...

            One-line version.

            >>> toloka_loop.register(SomeObserver(pool_id='123')).do_some_preparations(...)
            ...
        """
        observer_key = observer.get_unique_key()
        if observer_key in self._observers:
            raise ValueError(f'Failed to register observer to pipeline: observer with key {observer_key} is already '
                             f'registered')
        self._observers[observer_key] = observer
        return observer

    def observers_iter(self) -> Iterator[BaseObserver]:
        """Iterate over registered observers.

        Returns:
            An iterator over all registered observers except deleted ones.
            Might contain observers scheduled for deletion and not deleted yet.
        """
        return iter(self._observers.values())

    def _sigint_handler(self, loop: asyncio.AbstractEventLoop, waiting: Container[_Worker]) -> None:
        """Is being called in case of KeyboardInterrupt during workers run."""
        logger.error('Gracefully shutdown...')
        loop.remove_signal_handler(signal.SIGINT)
        self._got_sigint = True
        if not waiting:
            logger.warning('No workers to wait')
            raise KeyboardInterrupt

    def _process_done_tasks(
        self,
        pipeline_key: str,
        done: Set[asyncio.Task],
        waiting: Dict[_Worker, asyncio.Task],
        pending: Dict[_Worker, datetime],
    ) -> None:
        """Take done tasks and modify `waiting` and `pending`.
        """

        logger.info('Done count: %d', len(done))
        workers_to_dump = []
        errored = []
        for task in done:
            del waiting[task.worker]
            if task.exception():
                errored.append(task)
            else:
                workers_to_dump.append(task.worker)
                pending[task.worker] = task.start_time + self.period
        self._storage_save(pipeline_key, workers_to_dump)
        if errored:
            asyncio.get_event_loop().remove_signal_handler(signal.SIGINT)
            for task in errored:
                logger.error('Got error in: %s', task)
            raise ComplexException([task.exception() for task in errored])

    @attr.s
    class RunState:
        """State of a single Pipeline run.

        Attributes:
            workers: all known workers
            waiting: currently running workers
            pending: currently not running workers
        """
        pipeline_key: str = attr.ib()
        workers: Dict[_Worker, None] = attr.ib(on_setattr=attr.setters.frozen, factory=lambda: {})
        waiting: Dict[_Worker, asyncio.Task] = attr.ib(on_setattr=attr.setters.frozen, factory=lambda: {})
        pending: Dict[_Worker, datetime] = attr.ib(on_setattr=attr.setters.frozen, factory=lambda: {})

        def update_observers(self, pipeline: 'Pipeline'):
            known_observers_keys = {worker.observer.get_unique_key() for worker in self.workers.keys()}
            new_observers = [
                observer
                for observer in pipeline.observers_iter()
                if observer.get_unique_key() not in known_observers_keys
            ]
            if len(new_observers) > 0:
                logger.info(f'New observers found in quantity: {len(new_observers)}')
            new_workers = _OrderedSet(_Worker._from_observer(observer) for observer in new_observers)
            self.workers.update(new_workers)
            self.pending.update({worker: datetime.min for worker in new_workers})

    def _create_state(self):
        state = Pipeline.RunState(pipeline_key=str(self._get_unique_key()))
        state.update_observers(self)
        with self._lock(state.pipeline_key):
            # Get checkpoint from the storage.
            self._storage_load(state.pipeline_key, state.workers)
        return state

    async def run_manually(self) -> AsyncGenerator['Pipeline.RunState', None]:
        if not self._observers:
            raise ValueError('No observers registered')

        state = await self._initialize_run()

        # Check mode means that all workers should_run methods returned False. Setting check_mode to True has following
        # effects:
        #   * All pending tasks will be started regardless of their scheduled time;
        #   * ALL_COMPLETED iteration mode will be forced;
        #   * If all workers should_run will return False pipeline will be finished.
        check_mode = False

        for iteration in itertools.count(1):
            logger.info('Iteration %d', iteration)

            with self._lock(state.pipeline_key) as lock:
                iteration_start = datetime.now()

                still_pending = {}
                to_start: List[_Worker] = []
                to_remove: List[_Worker] = []

                for worker, time_to_start in state.pending.items():
                    if worker._is_deleted():
                        to_remove.append(worker)
                    elif time_to_start <= iteration_start or check_mode:
                        assert worker not in state.waiting
                        to_start.append(worker)
                    else:
                        still_pending[worker] = time_to_start
                state.pending.clear()
                state.pending.update(still_pending)

                if to_remove:
                    logger.info('Found observers to remove count: %d', len(to_remove))
                    for worker in to_remove:
                        self._observers.pop(worker.observer.get_unique_key())
                        state.workers.pop(worker)

                if not self._got_sigint:
                    logger.info('Observers to run count: %d', len(to_start))
                    for worker in to_start:
                        task = asyncio.get_event_loop().create_task(worker())
                        task.worker = worker
                        task.start_time = iteration_start
                        state.waiting[worker] = task
                else:
                    logger.warning('Not starting new workers due to SIGINT received')

                return_when = self.iteration_mode.value
                if check_mode or self._got_sigint:
                    return_when = asyncio.ALL_COMPLETED
                    logger.info('Waiting until all tasks are completed')

                if state.waiting:  # It may be empty due to SIGINT.
                    done, _ = await asyncio.wait(state.waiting.values(), return_when=return_when)
                    self._process_done_tasks(state.pipeline_key, done, state.waiting, state.pending)
                elif self._got_sigint:  # reraise KeyboardInterrupt when all tasks are finished
                    raise KeyboardInterrupt
                else:
                    logger.info('No workers to process. Finish')
                    return

                if _Worker._no_one_should_resume(state.workers):  # But some of them may still work.
                    if check_mode:
                        self._storage_cleanup(state.pipeline_key, state.workers, lock)
                        logger.info('Finish')
                        asyncio.get_event_loop().remove_signal_handler(signal.SIGINT)
                        return

                    logger.info('No one should resume yet. Waiting for remaining ones...')
                    if state.waiting:
                        done, _ = await asyncio.wait(state.waiting.values(), return_when=asyncio.ALL_COMPLETED)
                    else:
                        done = set()
                    self._process_done_tasks(state.pipeline_key, done, state.waiting, state.pending)
                    if self._got_sigint:
                        raise KeyboardInterrupt
                    if _Worker._no_one_should_resume(state.workers):  # If stop condition at all workers, run in check_mode.
                        check_mode = True
                else:
                    check_mode = False

            state.update_observers(self)

            if self._got_sigint:
                logger.warning('Execute next iteration immediately due to SIGINT handling')
            elif check_mode:
                logger.warning('Execute next iteration immediately due to check mode handling')
            else:
                yield state

    async def _initialize_run(self):
        state = self._create_state()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self._sigint_handler, loop, state.waiting)
        self._got_sigint = False
        return state

    async def run(self) -> None:
        async for state in self.run_manually():
            start_soon = max(state.pending.values(), default=None)
            sleep_time = (start_soon - datetime.now()).total_seconds()
            sleep_time = max(sleep_time, self.MIN_SLEEP_SECONDS)
            logger.info('Sleeping for %f seconds', sleep_time)
            await asyncio.sleep(sleep_time)
