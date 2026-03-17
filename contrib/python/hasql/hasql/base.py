import asyncio
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import chain
from types import MappingProxyType
from typing import (Any, AsyncContextManager, DefaultDict, Dict, List,
                    Optional, Sequence, Set, Union)

from .metrics import CalculateMetrics, DriverMetrics, Metrics
from .utils import Dsn, Stopwatch, split_dsn

logger = logging.getLogger(__name__)

DEFAULT_REFRESH_DELAY: int = 1
DEFAULT_REFRESH_TIMEOUT: int = 30
DEFAULT_ACQUIRE_TIMEOUT: float = 1.0
DEFAULT_MASTER_AS_REPLICA_WEIGHT: float = 0.
DEFAULT_STOPWATCH_WINDOW_SIZE: int = 128


class AbstractBalancerPolicy(ABC):
    def __init__(self, pool_manager: "BasePoolManager"):
        raise NotImplementedError

    @abstractmethod
    async def get_pool(
        self,
        read_only: bool,
        fallback_master: bool = False,
        master_as_replica_weight: Optional[float] = None,
    ) -> Any:
        raise NotImplementedError


class PoolAcquireContext(AsyncContextManager):

    def __init__(
        self,
        pool_manager: "BasePoolManager",
        read_only: bool,
        fallback_master: Optional[bool],
        master_as_replica_weight: Optional[float],
        timeout: float,
        metrics: CalculateMetrics,
        **kwargs,
    ):
        self.pool_manager = pool_manager
        self.read_only = read_only
        self.fallback_master = fallback_master
        self.master_as_replica_weight = master_as_replica_weight
        self.timeout = timeout
        self.kwargs = kwargs
        self.pool = None
        self.context = None
        self.metrics = metrics

    async def acquire_from_pool_connection(self):
        async def execute():
            with self.metrics.with_get_pool():
                self.pool = await self.pool_manager.balancer.get_pool(
                    read_only=self.read_only,
                    fallback_master=self.fallback_master,
                    master_as_replica_weight=self.master_as_replica_weight,
                )

            with self.metrics.with_acquire(self.pool_manager.host(self.pool)):
                return await self.pool_manager.acquire_from_pool(
                    self.pool, **self.kwargs,
                )

        self.conn = await asyncio.wait_for(execute(), timeout=self.timeout)
        self.metrics.add_connection(self.pool_manager.host(self.pool))
        self.pool_manager.register_connection(self.conn, self.pool)
        return self.conn

    async def __aenter__(self):
        async def go():
            with self.metrics.with_get_pool():
                self.pool = await self.pool_manager.balancer.get_pool(
                    read_only=self.read_only,
                    fallback_master=self.fallback_master,
                    master_as_replica_weight=self.master_as_replica_weight,
                )
            with self.metrics.with_acquire(self.pool_manager.host(self.pool)):
                self.context = self.pool_manager.acquire_from_pool(
                    self.pool,
                    **self.kwargs,
                )
            return await self.context.__aenter__()

        self.conn = await asyncio.wait_for(go(), timeout=self.timeout)
        self.metrics.add_connection(self.pool_manager.host(self.pool))
        return self.conn

    async def __aexit__(self, *exc):
        self.metrics.remove_connection(self.pool_manager.host(self.pool))
        await self.context.__aexit__(*exc)
        del self.conn

    def __await__(self):
        return self.acquire_from_pool_connection().__await__()


class BasePoolManager(ABC):
    _dsn_ready_event: DefaultDict[Dsn, asyncio.Event]
    _dsn_check_cond: DefaultDict[Dsn, asyncio.Condition]
    _master_pool_set: Set[Any]
    _replica_pool_set: Set[Any]
    _unmanaged_connections: Dict[Any, Any]

    def __init__(
        self,
        dsn: str,
        acquire_timeout: Union[float, int] = DEFAULT_ACQUIRE_TIMEOUT,
        refresh_delay: Union[float, int] = DEFAULT_REFRESH_DELAY,
        refresh_timeout: Union[float, int] = DEFAULT_REFRESH_TIMEOUT,
        fallback_master: bool = False,
        master_as_replica_weight: float = DEFAULT_MASTER_AS_REPLICA_WEIGHT,
        balancer_policy: type = AbstractBalancerPolicy,
        stopwatch_window_size: int = DEFAULT_STOPWATCH_WINDOW_SIZE,
        pool_factory_kwargs: Optional[dict] = None,
    ):
        if not issubclass(balancer_policy, AbstractBalancerPolicy):
            raise ValueError(
                "balancer_policy must be a class BaseBalancerPolicy heir",
            )

        if balancer_policy is AbstractBalancerPolicy:
            # Avoid circular import
            from .balancer_policy.greedy import GreedyBalancerPolicy
            balancer_policy = GreedyBalancerPolicy

        if pool_factory_kwargs is None:
            pool_factory_kwargs = {}
        self._pool_factory_kwargs = MappingProxyType(
            self._prepare_pool_factory_kwargs(pool_factory_kwargs),
        )
        self._dsn: List[Dsn] = split_dsn(dsn)
        self._dsn_ready_event = defaultdict(asyncio.Event)
        self._dsn_check_cond = defaultdict(asyncio.Condition)
        self._pools = [None] * len(self._dsn)
        self._acquire_timeout = acquire_timeout
        self._refresh_delay = refresh_delay
        self._refresh_timeout = refresh_timeout
        self._fallback_master = fallback_master
        self._master_as_replica_weight = master_as_replica_weight
        self._balancer = balancer_policy(self)
        self._master_pool_set = set()
        self._replica_pool_set = set()
        self._master_cond = asyncio.Condition()
        self._replica_cond = asyncio.Condition()
        self._unmanaged_connections = {}
        self._stopwatch = Stopwatch(window_size=stopwatch_window_size)
        self._refresh_role_tasks = [
            asyncio.create_task(self._check_pool_task(index))
            for index in range(len(self._dsn))
        ]
        self._closing = False
        self._closed = False
        self._metrics = CalculateMetrics()

    @property
    def dsn(self) -> List[Dsn]:
        return self._dsn

    @property
    def refresh_delay(self):
        return self._refresh_delay

    @property
    def refresh_timeout(self):
        return self._refresh_timeout

    @property
    def pool_factory_kwargs(self):
        return self._pool_factory_kwargs

    @property
    def master_pool_count(self):
        return len(self._master_pool_set)

    @property
    def replica_pool_count(self):
        return len(self._replica_pool_set)

    @property
    def available_pool_count(self):
        return self.master_pool_count + self.replica_pool_count

    @property
    def balancer(self) -> AbstractBalancerPolicy:
        return self._balancer

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def pools(self) -> Sequence[Any]:
        return tuple(self._pools)

    @abstractmethod
    def get_pool_freesize(self, pool):
        pass

    @abstractmethod
    def acquire_from_pool(self, pool, **kwargs):
        pass

    @abstractmethod
    async def release_to_pool(self, connection, pool, **kwargs):
        pass

    @abstractmethod
    async def _is_master(self, connection):
        pass

    @abstractmethod
    async def _pool_factory(self, dsn: Dsn):
        pass

    @abstractmethod
    async def _close(self, pool):
        pass

    @abstractmethod
    async def _terminate(self, pool) -> None:
        pass

    @abstractmethod
    def is_connection_closed(self, connection):
        pass

    @abstractmethod
    def host(self, pool: Any):
        pass

    @abstractmethod
    def _driver_metrics(self) -> Sequence[DriverMetrics]:
        pass

    def metrics(self) -> Metrics:
        return Metrics(
            drivers=self._driver_metrics(),
            hasql=self._metrics.metrics(),
        )

    def acquire(
        self,
        read_only: bool = False,
        fallback_master: Optional[bool] = None,
        master_as_replica_weight: Optional[float] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ):
        if fallback_master is None:
            fallback_master = self._fallback_master

        if not read_only and master_as_replica_weight is not None:
            raise ValueError(
                "Field master_as_replica_weight is used only when "
                "read_only is True",
            )
        if (
            master_as_replica_weight is not None and
            not (0. <= master_as_replica_weight <= 1)
        ):
            raise ValueError(
                "Field master_as_replica_weight must belong "
                "to the segment [0; 1]",
            )

        if read_only:
            if master_as_replica_weight is None:
                master_as_replica_weight = self._master_as_replica_weight

        if timeout is None:
            timeout = self._acquire_timeout

        ctx = PoolAcquireContext(
            pool_manager=self,
            read_only=read_only,
            fallback_master=fallback_master,
            master_as_replica_weight=master_as_replica_weight,
            timeout=timeout,
            metrics=self._metrics,
            **kwargs,
        )

        return ctx

    def acquire_master(
        self, timeout: Optional[float] = None, **kwargs,
    ):
        return self.acquire(read_only=False, timeout=timeout, **kwargs)

    def acquire_replica(
        self,
        fallback_master: Optional[bool] = None,
        master_as_replica_weight: Optional[float] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ):
        return self.acquire(
            read_only=True,
            fallback_master=fallback_master,
            master_as_replica_weight=master_as_replica_weight,
            timeout=timeout,
            **kwargs,
        )

    async def release(self, connection, **kwargs):
        if connection not in self._unmanaged_connections:
            raise ValueError(
                "Pool.release() received invalid connection: "
                f"{connection!r} is not a member of this pool",
            )

        pool = self._unmanaged_connections.pop(connection)
        self._metrics.remove_connection(self.host(pool))
        await self.release_to_pool(connection, pool, **kwargs)

    async def close(self):
        self._closing = True
        await self._clear()
        await asyncio.gather(
            *[self._close(pool) for pool in self._pools if pool is not None],
            return_exceptions=True,
        )
        self._closing = False
        self._closed = True

    async def terminate(self):
        self._closing = True
        await self._clear()
        for pool in self._pools:
            if pool is None:
                continue
            await self._terminate(pool)
        self._closing = False
        self._closed = True

    async def wait_next_pool_check(self, timeout: int = 10):
        tasks = [self._wait_checking_pool(dsn) for dsn in self._dsn]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)

    async def _wait_checking_pool(self, dsn: Dsn):
        async with self._dsn_check_cond[dsn]:
            for _ in range(2):
                await self._dsn_check_cond[dsn].wait()

    async def ready(
        self,
        masters_count: Optional[int] = None,
        replicas_count: Optional[int] = None,
        timeout: int = 10,
    ):

        if (
            (masters_count is not None and replicas_count is None) or
            (masters_count is None and replicas_count is not None)
        ):
            raise ValueError(
                "Arguments master_count and replicas_count "
                "should both be either None or not None",
            )

        if masters_count is not None and masters_count < 0:
            raise ValueError("masters_count shouldn't be negative")
        if replicas_count is not None and replicas_count < 0:
            raise ValueError("replicas_count shouldn't be negative")

        if masters_count is None and replicas_count is None:
            await asyncio.wait_for(self.wait_all_ready(), timeout=timeout)
            return

        assert isinstance(masters_count, int)
        assert isinstance(replicas_count, int)

        await asyncio.wait_for(
            asyncio.gather(
                self.wait_masters_ready(masters_count),
                self.wait_replicas_ready(replicas_count),
            ), timeout=timeout,
        )

    async def wait_all_ready(self):
        for dsn in self._dsn:
            await self._dsn_ready_event[dsn].wait()

    async def wait_masters_ready(self, masters_count: int):
        def predicate():
            return self.master_pool_count >= masters_count

        async with self._master_cond:
            await self._master_cond.wait_for(predicate)

    async def wait_replicas_ready(self, replicas_count: int):
        def predicate():
            return self.replica_pool_count >= replicas_count

        async with self._replica_cond:
            await self._replica_cond.wait_for(predicate)

    async def get_master_pools(self) -> List:
        if not self._master_pool_set:
            async with self._master_cond:
                await self._master_cond.wait()
        return list(self._master_pool_set)

    async def get_replica_pools(self, fallback_master: bool = False) -> List:
        if not self._replica_pool_set:
            if fallback_master:
                return await self.get_master_pools()
            async with self._replica_cond:
                await self._replica_cond.wait()
        return list(self._replica_pool_set)

    def pool_is_master(self, pool) -> bool:
        return pool in self._master_pool_set

    def pool_is_replica(self, pool) -> bool:
        return pool in self._replica_pool_set

    def register_connection(self, connection, pool):
        self._unmanaged_connections[connection] = pool

    def get_last_response_time(self, pool) -> Optional[float]:
        return self._stopwatch.get_time(pool)

    def _prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        return kwargs

    async def _clear(self):
        self._balancer = None
        if self._refresh_role_tasks is not None:
            for refresh_role_task in self._refresh_role_tasks:
                refresh_role_task.cancel()

            await asyncio.gather(
                *self._refresh_role_tasks,
                return_exceptions=True,
            )

            self._refresh_role_tasks = None

        release_tasks = []
        for connection in self._unmanaged_connections:
            release_tasks.append(self.release(connection))

        await asyncio.gather(*release_tasks, return_exceptions=True)

        self._unmanaged_connections.clear()
        self._master_pool_set.clear()
        self._replica_pool_set.clear()

    async def _check_pool_task(self, index: int):
        logger.debug("Starting pool task")
        dsn = self._dsn[index]
        censored_dsn = str(dsn.with_(password="******"))
        pool = await self._wait_creating_pool(dsn)
        self._pools[index] = pool

        logger.debug("Setting dsn=%r event", censored_dsn)
        sys_connection = None
        while not self._closing:
            try:
                # Не использовать async with self.acquire_from_pool(pool)
                # из-за большого таймаута
                logger.debug(
                    "Acquiring connection for checking dsn=%r", censored_dsn,
                )
                sys_connection = await asyncio.wait_for(
                    self.acquire_from_pool(pool), timeout=self._refresh_timeout,
                )

                logger.debug("Checking dsn=%r", censored_dsn)
                await self._periodic_pool_check(pool, dsn, sys_connection)
            except asyncio.TimeoutError:
                logger.warning(
                    "Creating system connection failed for dsn=%r",
                    censored_dsn,
                )
            except asyncio.CancelledError as cancelled_error:
                if self._closing:
                    raise cancelled_error from None
                logger.warning(
                    "Cancelled error for dsn=%r",
                    censored_dsn,
                    exc_info=True,
                )
                self._remove_pool_from_master_set(pool, dsn)
                self._remove_pool_from_replica_set(pool, dsn)
            except Exception:
                logger.warning(
                    "Database is not available with exception for dsn=%r",
                    censored_dsn,
                    exc_info=True,
                )
                self._remove_pool_from_master_set(pool, dsn)
                self._remove_pool_from_replica_set(pool, dsn)
            finally:
                if sys_connection is not None:
                    try:
                        await self.release_to_pool(sys_connection, pool)
                    except (Exception, asyncio.CancelledError):
                        logger.warning(
                            "Release connection to pool with "
                            "exception for dsn=%r",
                            censored_dsn,
                            exc_info=True,
                        )
                    except asyncio.CancelledError as cancelled_error:
                        if self._closing:
                            raise cancelled_error from None
                        logger.warning(
                            "Release connection to pool with "
                            "Cancelled error for dsn=%r",
                            censored_dsn,
                            exc_info=True,
                        )
                    sys_connection = None
                await self._notify_about_pool_has_checked(dsn)

            await asyncio.sleep(self._refresh_delay)

    async def _wait_creating_pool(self, dsn: Dsn):
        while not self._closing:
            try:
                return await asyncio.wait_for(
                    self._pool_factory(dsn),
                    timeout=self._refresh_timeout,
                )
            except Exception:
                logger.warning(
                    "Creating pool failed with exception for dsn=%s",
                    dsn.with_(password="******"),
                    exc_info=True,
                )
                await asyncio.sleep(self._refresh_delay)

    async def _periodic_pool_check(self, pool, dsn: Dsn, sys_connection):
        while not self._closing:
            try:
                await asyncio.wait_for(
                    self._refresh_pool_role(pool, dsn, sys_connection),
                    timeout=self._refresh_timeout,
                )
                await self._notify_about_pool_has_checked(dsn)
            except asyncio.TimeoutError:
                logger.warning(
                    "Periodic pool check failed for dsn=%s",
                    dsn.with_(password="******"),
                )
                self._remove_pool_from_master_set(pool, dsn)
                self._remove_pool_from_replica_set(pool, dsn)
                await self._notify_about_pool_has_checked(dsn)

            await asyncio.sleep(self._refresh_delay)

    async def _notify_about_pool_has_checked(self, dsn: Dsn):
        async with self._dsn_check_cond[dsn]:
            self._dsn_check_cond[dsn].notify_all()

    async def _add_pool_to_master_set(self, pool, dsn: Dsn):
        if pool in self._master_pool_set:
            return
        self._master_pool_set.add(pool)
        logger.debug(
            "Pool %s has been added to master set",
            dsn.with_(password="******"),
        )
        async with self._master_cond:
            self._master_cond.notify_all()

    async def _add_pool_to_replica_set(self, pool, dsn: Dsn):
        if pool in self._replica_pool_set:
            return
        self._replica_pool_set.add(pool)
        logger.debug(
            "Pool %s has been added to replica set",
            dsn.with_(password="******"),
        )
        async with self._replica_cond:
            self._replica_cond.notify_all()

    def _remove_pool_from_master_set(self, pool, dsn: Dsn):
        if pool in self._master_pool_set:
            self._master_pool_set.remove(pool)
            logger.debug(
                "Pool %s has been removed from master set",
                dsn.with_(password="******"),
            )

    def _remove_pool_from_replica_set(self, pool, dsn: Dsn):
        if pool in self._replica_pool_set:
            self._replica_pool_set.remove(pool)
            logger.debug(
                "Pool %s has been removed from replica set",
                dsn.with_(password="******"),
            )

    async def _refresh_pool_role(self, pool, dsn: Dsn, sys_connection):
        with self._stopwatch(pool):
            is_master = await self._is_master(sys_connection)
        if is_master:
            await self._add_pool_to_master_set(pool, dsn)
            self._remove_pool_from_replica_set(pool, dsn)
        else:
            await self._add_pool_to_replica_set(pool, dsn)
            self._remove_pool_from_master_set(pool, dsn)
        self._dsn_ready_event[dsn].set()

    def __iter__(self):
        return chain(iter(self._master_pool_set), iter(self._replica_pool_set))

    async def __aenter__(self):
        await self.ready()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ("BasePoolManager", "AbstractBalancerPolicy")
