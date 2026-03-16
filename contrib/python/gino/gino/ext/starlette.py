import asyncio
import logging

from gino.api import Gino as _Gino
from gino.api import GinoExecutor as _Executor
from gino.engine import GinoConnection as _Connection
from gino.engine import GinoEngine as _Engine
from gino.strategies import GinoStrategy
from sqlalchemy.engine.url import make_url, URL
from starlette import status
from starlette.exceptions import HTTPException
from starlette.types import Receive, Scope, Send

logger = logging.getLogger("gino.ext.starlette")


class StarletteModelMixin:
    @classmethod
    async def get_or_404(cls, *args, **kwargs):
        # noinspection PyUnresolvedReferences
        rv = await cls.get(*args, **kwargs)
        if rv is None:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                "{} is not found".format(cls.__name__),
            )
        return rv


# noinspection PyClassHasNoInit
class GinoExecutor(_Executor):
    async def first_or_404(self, *args, **kwargs):
        rv = await self.first(*args, **kwargs)
        if rv is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No such data")
        return rv


# noinspection PyClassHasNoInit
class GinoConnection(_Connection):
    async def first_or_404(self, *args, **kwargs):
        rv = await self.first(*args, **kwargs)
        if rv is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No such data")
        return rv


# noinspection PyClassHasNoInit
class GinoEngine(_Engine):
    connection_cls = GinoConnection

    async def first_or_404(self, *args, **kwargs):
        rv = await self.first(*args, **kwargs)
        if rv is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No such data")
        return rv


class StarletteStrategy(GinoStrategy):
    name = "starlette"
    engine_cls = GinoEngine


StarletteStrategy()


class _Middleware:
    def __init__(self, app, db):
        self.app = app
        self.db = db
        self._conn_for_req = db.config["use_connection_for_request"]

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        if scope["type"] == "http" and self._conn_for_req:
            scope["connection"] = await self.db.acquire(lazy=True)
            try:
                await self.app(scope, receive, send)
            finally:
                conn = scope.pop("connection", None)
                if conn is not None:
                    await conn.release()
            return

        await self.app(scope, receive, send)


class Gino(_Gino):
    """Support Starlette server.

    The common usage looks like this::

        from starlette.applications import Starlette
        from gino.ext.starlette import Gino

        app = Starlette()
        db = Gino(app, **kwargs)

    GINO adds a middleware to the Starlette app to setup and cleanup database
    according to the configurations that passed in the ``kwargs`` parameter.

    The config includes:

    * ``driver`` - the database driver, default is ``asyncpg``.
    * ``host`` - database server host, default is ``localhost``.
    * ``port`` - database server port, default is ``5432``.
    * ``user`` - database server user, default is ``postgres``.
    * ``password`` - database server password, default is empty.
    * ``database`` - database name, default is ``postgres``.
    * ``dsn`` - a SQLAlchemy database URL to create the engine, its existence
      will replace all previous connect arguments.
    * ``pool_min_size`` - the initial number of connections of the db pool.
    * ``pool_max_size`` - the maximum number of connections in the db pool.
    * ``echo`` - enable SQLAlchemy echo mode.
    * ``ssl`` - SSL context passed to ``asyncpg.connect``, default is ``None``.
    * ``use_connection_for_request`` - flag to set up lazy connection for
      requests.
    * ``retry_limit`` - the number of retries to connect to the database on
      start up, default is 1.
    * ``retry_interval`` - seconds to wait between retries, default is 1.
    * ``kwargs`` - other parameters passed to the specified dialects,
      like ``asyncpg``. Unrecognized parameters will cause exceptions.

    If ``use_connection_for_request`` is set to be True, then a lazy connection
    is available at ``request['connection']``. By default, a database
    connection is borrowed on the first query, shared in the same execution
    context, and returned to the pool on response. If you need to release the
    connection early in the middle to do some long-running tasks, you can
    simply do this::

        await request['connection'].release(permanent=False)

    """

    model_base_classes = _Gino.model_base_classes + (StarletteModelMixin,)
    query_executor = GinoExecutor

    def __init__(self, app=None, *args, **kwargs):
        self.config = dict()
        if "dsn" in kwargs:
            self.config["dsn"] = make_url(kwargs.pop("dsn"))
        else:
            self.config["dsn"] = URL(
                drivername=kwargs.pop("driver", "asyncpg"),
                host=kwargs.pop("host", "localhost"),
                port=kwargs.pop("port", 5432),
                username=kwargs.pop("user", "postgres"),
                password=kwargs.pop("password", ""),
                database=kwargs.pop("database", "postgres"),
            )
        self.config["retry_limit"] = kwargs.pop("retry_limit", 1)
        self.config["retry_interval"] = kwargs.pop("retry_interval", 1)
        self.config["echo"] = kwargs.pop("echo", False)
        self.config["min_size"] = kwargs.pop("pool_min_size", 5)
        self.config["max_size"] = kwargs.pop("pool_max_size", 10)
        self.config["ssl"] = kwargs.pop("ssl", None)
        self.config["use_connection_for_request"] = kwargs.pop(
            "use_connection_for_request", True
        )
        self.config["kwargs"] = kwargs.pop("kwargs", dict())

        super().__init__(*args, **kwargs)
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        @app.on_event("startup")
        async def startup():
            config = self.config
            try:
                logger.info("Connecting to the database: %r", config["dsn"])
            except TypeError:
                log_str = config["dsn"].drivername + "://"
                if config["dsn"].host is not None:
                    log_str += str(config["dsn"].host)
                if config["dsn"].port is not None:
                    log_str += ":" + str(config["dsn"].port)
                if config["dsn"].database is not None:
                    log_str += "/" + config["dsn"].database
                logger.info("Connecting to the database: %r", log_str)
            retries = 0
            while True:
                retries += 1
                # noinspection PyBroadException
                try:
                    await self.set_bind(
                        config["dsn"],
                        echo=config["echo"],
                        min_size=config["min_size"],
                        max_size=config["max_size"],
                        ssl=config["ssl"],
                        **config["kwargs"],
                    )
                    break
                except Exception:
                    if retries < config["retry_limit"]:
                        logger.info("Waiting for the database to start...")
                        await asyncio.sleep(config["retry_interval"])
                    else:
                        logger.error(
                            "Cannot connect to the database; max retries reached."
                        )
                        raise
            msg = "Database connection pool created: "
            logger.info(
                msg + repr(self.bind),
                extra={"color_message": msg + self.bind.repr(color=True)},
            )

        @app.on_event("shutdown")
        async def shutdown():
            msg = "Closing database connection: "
            logger.info(
                msg + repr(self.bind),
                extra={"color_message": msg + self.bind.repr(color=True)},
            )
            _bind = self.pop_bind()
            await _bind.close()
            msg = "Closed database connection: "
            logger.info(
                msg + repr(_bind),
                extra={"color_message": msg + _bind.repr(color=True)},
            )

        app.add_middleware(_Middleware, db=self)

    async def first_or_404(self, *args, **kwargs):
        rv = await self.first(*args, **kwargs)
        if rv is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No such data")
        return rv

    async def set_bind(self, bind, loop=None, **kwargs):
        kwargs.setdefault("strategy", "starlette")
        return await super().set_bind(bind, loop=loop, **kwargs)
