import asyncio
import functools
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from operator import attrgetter
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import Sentinel
from redis.exceptions import RedisError, WatchError

from .constants import default_queue_name, expires_extra_ms, job_key_prefix, result_key_prefix
from .jobs import Deserializer, Job, JobDef, JobResult, Serializer, deserialize_job, serialize_job
from .utils import timestamp_ms, to_ms, to_unix_ms

logger = logging.getLogger('arq.connections')


@dataclass
class RedisSettings:
    """
    No-Op class used to hold redis connection redis_settings.

    Used by :func:`arq.connections.create_pool` and :class:`arq.worker.Worker`.
    """

    host: Union[str, list[tuple[str, int]]] = 'localhost'
    port: int = 6379
    unix_socket_path: Optional[str] = None
    database: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    ssl: bool = False
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_cert_reqs: str = 'required'
    ssl_ca_certs: Optional[str] = None
    ssl_ca_data: Optional[str] = None
    ssl_check_hostname: bool = False
    conn_timeout: int = 1
    conn_retries: int = 5
    conn_retry_delay: int = 1
    max_connections: Optional[int] = None

    sentinel: bool = False
    sentinel_master: str = 'mymaster'

    retry_on_timeout: bool = False
    retry_on_error: Optional[list[type[Exception]]] = None
    retry: Optional[Retry] = None

    @classmethod
    def from_dsn(cls, dsn: str) -> 'RedisSettings':
        conf = urlparse(dsn)
        if conf.scheme not in {'redis', 'rediss', 'unix'}:
            raise RuntimeError('invalid DSN scheme')
        query_db = parse_qs(conf.query).get('db')
        if query_db:
            # e.g. redis://localhost:6379?db=1
            database = int(query_db[0])
        elif conf.scheme != 'unix':
            database = int(conf.path.lstrip('/')) if conf.path else 0
        else:
            database = 0
        return RedisSettings(
            host=conf.hostname or 'localhost',
            port=conf.port or 6379,
            ssl=conf.scheme == 'rediss',
            username=conf.username,
            password=conf.password,
            database=database,
            unix_socket_path=conf.path if conf.scheme == 'unix' else None,
        )

    def __repr__(self) -> str:
        return 'RedisSettings({})'.format(', '.join(f'{k}={v!r}' for k, v in self.__dict__.items()))


if TYPE_CHECKING:
    BaseRedis = Redis[bytes]
else:
    BaseRedis = Redis


class ArqRedis(BaseRedis):
    """
    Thin subclass of ``redis.asyncio.Redis`` which adds :func:`arq.connections.enqueue_job`.

    :param redis_settings: an instance of ``arq.connections.RedisSettings``.
    :param job_serializer: a function that serializes Python objects to bytes, defaults to pickle.dumps
    :param job_deserializer: a function that deserializes bytes into Python objects, defaults to pickle.loads
    :param default_queue_name: the default queue name to use, defaults to ``arq.queue``.
    :param expires_extra_ms: the default length of time from when a job is expected to start
     after which the job expires, defaults to 1 day in ms.
    :param kwargs: keyword arguments directly passed to ``redis.asyncio.Redis``.
    """

    def __init__(
        self,
        pool_or_conn: Optional[ConnectionPool] = None,
        job_serializer: Optional[Serializer] = None,
        job_deserializer: Optional[Deserializer] = None,
        default_queue_name: str = default_queue_name,
        expires_extra_ms: int = expires_extra_ms,
        **kwargs: Any,
    ) -> None:
        self.job_serializer = job_serializer
        self.job_deserializer = job_deserializer
        self.default_queue_name = default_queue_name
        if pool_or_conn:
            kwargs['connection_pool'] = pool_or_conn
        self.expires_extra_ms = expires_extra_ms
        super().__init__(**kwargs)

    async def enqueue_job(
        self,
        function: str,
        *args: Any,
        _job_id: Optional[str] = None,
        _queue_name: Optional[str] = None,
        _defer_until: Optional[datetime] = None,
        _defer_by: Union[None, int, float, timedelta] = None,
        _expires: Union[None, int, float, timedelta] = None,
        _job_try: Optional[int] = None,
        **kwargs: Any,
    ) -> Optional[Job]:
        """
        Enqueue a job.

        :param function: Name of the function to call
        :param args: args to pass to the function
        :param _job_id: ID of the job, can be used to enforce job uniqueness
        :param _queue_name: queue of the job, can be used to create job in different queue
        :param _defer_until: datetime at which to run the job
        :param _defer_by: duration to wait before running the job
        :param _expires: do not start or retry a job after this duration;
            defaults to 24 hours plus deferring time, if any
        :param _job_try: useful when re-enqueueing jobs within a job
        :param kwargs: any keyword arguments to pass to the function
        :return: :class:`arq.jobs.Job` instance or ``None`` if a job with this ID already exists
        """
        if _queue_name is None:
            _queue_name = self.default_queue_name
        job_id = _job_id or uuid4().hex
        job_key = job_key_prefix + job_id
        if _defer_until and _defer_by:
            raise RuntimeError("use either 'defer_until' or 'defer_by' or neither, not both")

        defer_by_ms = to_ms(_defer_by)
        expires_ms = to_ms(_expires)

        async with self.pipeline(transaction=True) as pipe:
            await pipe.watch(job_key)
            if await pipe.exists(job_key, result_key_prefix + job_id):
                await pipe.reset()
                return None

            enqueue_time_ms = timestamp_ms()
            if _defer_until is not None:
                score = to_unix_ms(_defer_until)
            elif defer_by_ms:
                score = enqueue_time_ms + defer_by_ms
            else:
                score = enqueue_time_ms

            expires_ms = expires_ms or score - enqueue_time_ms + self.expires_extra_ms

            job = serialize_job(function, args, kwargs, _job_try, enqueue_time_ms, serializer=self.job_serializer)
            pipe.multi()
            pipe.psetex(job_key, expires_ms, job)
            pipe.zadd(_queue_name, {job_id: score})
            try:
                await pipe.execute()
            except WatchError:
                # job got enqueued since we checked 'job_exists'
                return None
        return Job(job_id, redis=self, _queue_name=_queue_name, _deserializer=self.job_deserializer)

    async def _get_job_result(self, key: bytes) -> JobResult:
        job_id = key[len(result_key_prefix) :].decode()
        job = Job(job_id, self, _deserializer=self.job_deserializer)
        r = await job.result_info()
        if r is None:
            raise KeyError(f'job "{key.decode()}" not found')
        r.job_id = job_id
        return r

    async def all_job_results(self) -> list[JobResult]:
        """
        Get results for all jobs in redis.
        """
        keys = await self.keys(result_key_prefix + '*')
        results = await asyncio.gather(*[self._get_job_result(k) for k in keys])
        return sorted(results, key=attrgetter('enqueue_time'))

    async def _get_job_def(self, job_id: bytes, score: int) -> JobDef:
        key = job_key_prefix + job_id.decode()
        v = await self.get(key)
        if v is None:
            raise RuntimeError(f'job "{key}" not found')
        jd = deserialize_job(v, deserializer=self.job_deserializer)
        jd.score = score
        jd.job_id = job_id.decode()
        return jd

    async def queued_jobs(self, *, queue_name: Optional[str] = None) -> list[JobDef]:
        """
        Get information about queued, mostly useful when testing.
        """
        if queue_name is None:
            queue_name = self.default_queue_name
        jobs = await self.zrange(queue_name, withscores=True, start=0, end=-1)
        return await asyncio.gather(*[self._get_job_def(job_id, int(score)) for job_id, score in jobs])


async def create_pool(
    settings_: Optional[RedisSettings] = None,
    *,
    retry: int = 0,
    job_serializer: Optional[Serializer] = None,
    job_deserializer: Optional[Deserializer] = None,
    default_queue_name: str = default_queue_name,
    expires_extra_ms: int = expires_extra_ms,
) -> ArqRedis:
    """
    Create a new redis pool, retrying up to ``conn_retries`` times if the connection fails.

    Returns a :class:`arq.connections.ArqRedis` instance, thus allowing job enqueuing.
    """
    settings: RedisSettings = RedisSettings() if settings_ is None else settings_

    if isinstance(settings.host, str) and settings.sentinel:
        raise RuntimeError("str provided for 'host' but 'sentinel' is true; list of sentinels expected")

    if settings.sentinel:

        def pool_factory(*args: Any, **kwargs: Any) -> ArqRedis:
            client = Sentinel(  # type: ignore[misc]
                *args,
                sentinels=settings.host,
                ssl=settings.ssl,
                **kwargs,
            )
            redis = client.master_for(settings.sentinel_master, redis_class=ArqRedis)
            return cast(ArqRedis, redis)

    else:
        pool_factory = functools.partial(
            ArqRedis,
            host=settings.host,
            port=settings.port,
            unix_socket_path=settings.unix_socket_path,
            socket_connect_timeout=settings.conn_timeout,
            ssl=settings.ssl,
            ssl_keyfile=settings.ssl_keyfile,
            ssl_certfile=settings.ssl_certfile,
            ssl_cert_reqs=settings.ssl_cert_reqs,
            ssl_ca_certs=settings.ssl_ca_certs,
            ssl_ca_data=settings.ssl_ca_data,
            ssl_check_hostname=settings.ssl_check_hostname,
            retry=settings.retry,
            retry_on_timeout=settings.retry_on_timeout,
            retry_on_error=settings.retry_on_error,
            max_connections=settings.max_connections,
        )

    while True:
        try:
            pool = pool_factory(
                db=settings.database, username=settings.username, password=settings.password, encoding='utf8'
            )
            pool.job_serializer = job_serializer
            pool.job_deserializer = job_deserializer
            pool.default_queue_name = default_queue_name
            pool.expires_extra_ms = expires_extra_ms
            await pool.ping()

        except (ConnectionError, OSError, RedisError, asyncio.TimeoutError) as e:
            if retry < settings.conn_retries:
                logger.warning(
                    'redis connection error %s:%s %s %s, %s retries remaining...',
                    settings.host,
                    settings.port,
                    e.__class__.__name__,
                    e,
                    settings.conn_retries - retry,
                )
                await asyncio.sleep(settings.conn_retry_delay)
                retry = retry + 1
            else:
                raise
        else:
            if retry > 0:
                logger.info('redis connection successful')
            return pool


async def log_redis_info(redis: 'Redis[bytes]', log_func: Callable[[str], Any]) -> None:
    async with redis.pipeline(transaction=False) as pipe:
        pipe.info(section='Server')
        pipe.info(section='Memory')
        pipe.info(section='Clients')
        pipe.dbsize()
        info_server, info_memory, info_clients, key_count = await pipe.execute()

    redis_version = info_server.get('redis_version', '?')
    mem_usage = info_memory.get('used_memory_human', '?')
    clients_connected = info_clients.get('connected_clients', '?')

    log_func(
        f'redis_version={redis_version} mem_usage={mem_usage} clients_connected={clients_connected} db_keys={key_count}'
    )
