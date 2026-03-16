from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession

from .query import Query


def make_session(engine, is_async=False):
    session_class = Session
    if is_async:
        session_class = AsyncSession

    factory = sessionmaker(bind=engine, class_=session_class)

    return factory(query_cls=Query)
