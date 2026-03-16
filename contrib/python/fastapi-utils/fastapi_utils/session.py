from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.orm import Session


class FastAPISessionMaker:
    """
    A convenience class for managing a (cached) sqlalchemy ORM engine and sessionmaker.

    Intended for use creating ORM sessions injected into endpoint functions by FastAPI.
    """

    def __init__(self, database_uri: str):
        """
        `database_uri` should be any sqlalchemy-compatible database URI.

        In particular, `sqlalchemy.create_engine(database_uri)` should work to create an engine.

        Typically, this would look like:

            "<scheme>://<user>:<password>@<host>:<port>/<database>"

        A concrete example looks like "postgresql://db_user:password@db:5432/app"
        """
        self.database_uri = database_uri

        self._cached_engine: sa.engine.Engine | None = None
        self._cached_sessionmaker: sa.orm.sessionmaker | None = None

    @property
    def cached_engine(self) -> sa.engine.Engine:
        """
        Returns a lazily-cached sqlalchemy engine for the instance's database_uri.
        """
        engine = self._cached_engine
        if engine is None:
            engine = self.get_new_engine()
            self._cached_engine = engine
        return engine

    @property
    def cached_sessionmaker(self) -> sa.orm.sessionmaker:
        """
        Returns a lazily-cached sqlalchemy sessionmaker using the instance's (lazily-cached) engine.
        """
        sessionmaker = self._cached_sessionmaker
        if sessionmaker is None:
            sessionmaker = self.get_new_sessionmaker(self.cached_engine)
            self._cached_sessionmaker = sessionmaker
        return sessionmaker

    def get_new_engine(self) -> sa.engine.Engine:
        """
        Returns a new sqlalchemy engine using the instance's database_uri.
        """
        return get_engine(self.database_uri)

    def get_new_sessionmaker(self, engine: sa.engine.Engine | None) -> sa.orm.sessionmaker:
        """
        Returns a new sessionmaker for the provided sqlalchemy engine. If no engine is provided, the
        instance's (lazily-cached) engine is used.
        """
        engine = engine or self.cached_engine
        return get_sessionmaker_for_engine(engine)

    def get_db(self) -> Iterator[Session]:
        """
        A generator function that yields a sqlalchemy orm session and cleans up the session once resumed after yielding.

        Can be used directly as a context-manager FastAPI dependency, or yielded from inside a separate dependency.
        """
        yield from _get_db(self.cached_sessionmaker)

    @contextmanager
    def context_session(self) -> Iterator[Session]:
        """
        A context-manager wrapped version of the `get_db` method.

        This makes it possible to get a context-managed orm session for the relevant database_uri without
        needing to rely on FastAPI's dependency injection.

        Usage looks like:

            session_maker = FastAPISessionMaker(database_uri)
            with session_maker.context_session() as session:
                session.query(...)
                ...
        """
        yield from self.get_db()

    def reset_cache(self) -> None:
        """
        Resets the engine and sessionmaker caches.

        After calling this method, the next time you try to use the cached engine or sessionmaker,
        new ones will be created.
        """
        self._cached_engine = None
        self._cached_sessionmaker = None


def get_engine(uri: str) -> sa.engine.Engine:
    """
    Returns a sqlalchemy engine with pool_pre_ping enabled.

    This function may be updated over time to reflect recommended engine configuration for use with FastAPI.
    """
    return sa.create_engine(uri, pool_pre_ping=True)


def get_sessionmaker_for_engine(engine: sa.engine.Engine) -> sa.orm.sessionmaker:
    """
    Returns a sqlalchemy sessionmaker for the provided engine with recommended configuration settings.

    This function may be updated over time to reflect recommended sessionmaker configuration for use with FastAPI.
    """
    return sa.orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def context_session(engine: sa.engine.Engine) -> Iterator[Session]:
    """
    This contextmanager yields a managed session for the provided engine.

    Usage is similar to `FastAPISessionMaker.context_session`, except that you have to provide the engine to use.

    A new sessionmaker is created for each call, so the FastAPISessionMaker.context_session
    method may be preferable in performance-sensitive contexts.
    """
    sessionmaker = get_sessionmaker_for_engine(engine)
    yield from _get_db(sessionmaker)


def _get_db(sessionmaker: sa.orm.sessionmaker) -> Iterator[Session]:
    """
    A generator function that yields an ORM session using the provided sessionmaker, and cleans it up when resumed.
    """
    session = sessionmaker()
    try:
        yield session
        session.commit()
    except Exception as exc:
        session.rollback()
        raise exc
    finally:
        session.close()
