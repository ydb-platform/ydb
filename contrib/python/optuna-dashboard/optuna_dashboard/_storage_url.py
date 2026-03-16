from __future__ import annotations

import os.path
from pathlib import Path
import re
from typing import TYPE_CHECKING

from optuna.storages import BaseStorage
from optuna.storages import RDBStorage


if TYPE_CHECKING:
    from optuna.storages import JournalStorage


# The code to declare `rfc1738_pattern` variable is quoted from the source code
# of SQLAlchemy project (MIT License). See LICENSE file for detials.
# https://github.com/zzzeek/sqlalchemy/blob/c6554ac52/lib/sqlalchemy/engine/url.py#L234-L292
rfc1738_pattern = re.compile(
    r"""
        (?P<name>[\w\+]+)://
        (?:
            (?P<username>[^:/]*)
            (?::(?P<password>.*))?
        @)?
        (?:
            (?:
                \[(?P<ipv6host>[^/]+)\] |
                (?P<ipv4host>[^/:]+)
            )?
            (?::(?P<port>[^/]*))?
        )?
        (?:/(?P<database>.*))?
        """,
    re.X,
)


def get_storage(storage: str | BaseStorage, storage_class: str | None = None) -> BaseStorage:
    if isinstance(storage, BaseStorage):
        return storage

    if storage_class:
        if storage_class == "RDBStorage":
            return get_rdb_storage(storage)
        if storage_class == "JournalRedisStorage":
            return get_journal_redis_storage(storage)
        if storage_class == "JournalFileStorage":
            return get_journal_file_storage(storage)
        raise ValueError("Unexpected storage_class")

    return guess_storage_from_url(storage)


def _has_sqlite_header(storage_url: str) -> bool:
    storage_path = Path(storage_url)
    SQLITE_HEADER = (
        b"SQLite format 3\x00"  # see https://github.com/optuna/optuna-dashboard/pull/800
    )
    with storage_path.open(mode="rb") as f:
        header = f.read(len(SQLITE_HEADER))
    return header == SQLITE_HEADER


def guess_storage_from_url(storage_url: str) -> BaseStorage:
    if storage_url.startswith("redis"):
        return get_journal_redis_storage(storage_url)

    if os.path.isfile(storage_url):
        if _has_sqlite_header(storage_url):
            raise ValueError(
                f"Please specify 'sqlite:///{storage_url}' to use SQLite3 (RDBStorage)"
            )
        return get_journal_file_storage(storage_url)

    if rfc1738_pattern.match(storage_url) is not None:
        return get_rdb_storage(storage_url)

    raise ValueError("Failed to guess storage class from storage_url")


def get_rdb_storage(storage_url: str) -> RDBStorage:
    return RDBStorage(storage_url, skip_compatibility_check=True, skip_table_creation=True)


def get_journal_file_storage(file_path: str) -> JournalStorage:
    from optuna.storages import JournalStorage

    try:
        from optuna.storages.journal import JournalFileBackend
    except ImportError:
        from optuna.storages import JournalFileStorage as JournalFileBackend
    try:
        from optuna.storages.journal import JournalFileOpenLock
    except ImportError:
        from optuna.storages import JournalFileOpenLock

    storage: JournalStorage
    if os.name == "nt":
        lock_obj = JournalFileOpenLock(file_path)
        storage = JournalStorage(JournalFileBackend(file_path=file_path, lock_obj=lock_obj))
    else:
        storage = JournalStorage(JournalFileBackend(file_path=file_path))
    return storage


def get_journal_redis_storage(redis_url: str) -> JournalStorage:
    from optuna.storages import JournalRedisStorage
    from optuna.storages import JournalStorage

    return JournalStorage(JournalRedisStorage(redis_url))
