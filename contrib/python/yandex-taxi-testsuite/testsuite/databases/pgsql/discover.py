import collections
import dataclasses
import hashlib
import itertools
import logging
import pathlib
from collections.abc import Iterable
from typing import DefaultDict

from . import exceptions, utils

logger = logging.getLogger(__name__)

SINGLE_SHARD = -1
DB_NAME_MAX = 31


@dataclasses.dataclass(frozen=True)
class ShardName:
    db_name: str
    shard: int


@dataclasses.dataclass
class ShardFiles:
    name: ShardName
    files: list[pathlib.Path] | None = None
    pg_migrations: list[pathlib.Path] | None = None


@dataclasses.dataclass
class ShardFileInfo:
    files: list[pathlib.Path]
    pg_migrations: list[pathlib.Path]

    def extend(self, other: ShardFiles) -> None:
        if other.files:
            self.files.extend(other.files)
        if other.pg_migrations:
            self.pg_migrations.extend(other.pg_migrations)


ShardPathesDict = dict[int, ShardFileInfo]


@dataclasses.dataclass(frozen=True)
class PgShard:
    shard_id: int
    pretty_name: str
    dbname: str
    files: list[pathlib.Path]
    migrations: list[pathlib.Path]

    def get_schema_hash(self) -> str:
        return utils.get_files_hash(
            itertools.chain(self.files, self.migrations),
        )


@dataclasses.dataclass(frozen=True)
class PgShardedDatabase:
    service_name: str | None
    dbname: str
    shards: list[PgShard]


def find_schemas(
    service_name: str | None,
    schema_dirs: list[pathlib.Path],
) -> dict[str, PgShardedDatabase]:
    """Read database schemas from directories ``schema_dirs``. ::

     |- schema_path/
       |- database1.sql
       |- database2.sql

    :param service_name: service name used as prefix for database name if not
           empty, e.g. "servicename_dbname".
    :param schema_dirs: list of pathes to scan for schemas
    :returns: :py:class:`Dict[str, PgShardedDatabase]` where key is
              database name as stored in :py:attr:`PgShard.dbname`
    """
    result: dict[str, PgShardedDatabase] = {}
    for path in schema_dirs:
        if not path.is_dir():
            continue
        schemas = _find_databases_schemas(service_name, path)
        for dbname in schemas.keys() & result.keys():
            raise exceptions.PostgresqlError(
                f'Database {dbname} is declared twice',
            )
        result.update(schemas)
    return result


def _find_databases_schemas(
    service_name: str | None,
    schema_path: pathlib.Path,
) -> dict[str, PgShardedDatabase]:
    logger.debug('Looking up for PostgreSQL schemas at %s', schema_path)
    shard_files_map = _build_shard_files_map(schema_path)
    result = {}
    for dbname, shards in shard_files_map.items():
        _raise_if_invalid_shards(dbname, shards)
        pg_shards = []
        for shard_id, shard_files in sorted(shards.items()):
            pg_shards.append(
                _create_pgshard(
                    dbname,
                    service_name=service_name,
                    shard_id=shard_id,
                    files=sorted(shard_files.files),
                    migrations=sorted(shard_files.pg_migrations),
                ),
            )
        result[dbname] = PgShardedDatabase(
            service_name=service_name,
            dbname=dbname,
            shards=pg_shards,
        )
    return result


def _build_shard_files_map(
    root_path: pathlib.Path,
) -> DefaultDict[str, ShardPathesDict]:
    result: DefaultDict[str, ShardPathesDict]
    result = collections.defaultdict(
        lambda: collections.defaultdict(lambda: ShardFileInfo([], [])),
    )
    for shard in _find_shard_files(root_path):
        result[shard.name.db_name][shard.name.shard].extend(shard)
    return result


def _find_shard_files(schema_path: pathlib.Path) -> Iterable[ShardFiles]:
    for entry in schema_path.iterdir():
        shard_files = _get_shard_schema_files(entry)
        if shard_files is not None:
            yield shard_files


def _get_shard_schema_files(path: pathlib.Path) -> ShardFiles | None:
    shard_name = _parse_shard_name(path.stem)
    if path.is_file():
        if path.suffix == '.sql':
            return ShardFiles(shard_name, files=[path])
    elif path.is_dir():
        if path.joinpath('migrations').is_dir():
            return ShardFiles(shard_name, pg_migrations=[path])
        return ShardFiles(shard_name, files=utils.scan_sql_directory(path))
    return None


def _raise_if_invalid_shards(dbname: str, shards: ShardPathesDict) -> None:
    if SINGLE_SHARD in shards:
        if len(shards) != 1:
            raise exceptions.PostgresqlError(
                'Postgresql database %s has single shard configuration '
                'while defined as multishard' % (dbname,),
            )
    else:
        if set(shards.keys()) != set(range(len(shards))):
            raise exceptions.PostgresqlError(
                'Postgresql database %s is missing fixtures '
                'for some shards' % (dbname,),
            )


def _create_pgshard(
    dbname: str,
    service_name: str | None = None,
    shard_id: int = SINGLE_SHARD,
    files: list[pathlib.Path] | None = None,
    migrations: list[pathlib.Path] | None = None,
) -> PgShard:
    if files is None:
        files = []
    if migrations is None:
        migrations = []
    if shard_id == SINGLE_SHARD:
        actual_shard_id = 0
        pretty_name = dbname
    else:
        actual_shard_id = shard_id
        pretty_name = '%s@%d' % (dbname, shard_id)

    sharded_dbname = _database_name(service_name, dbname, shard_id)
    return PgShard(
        shard_id=actual_shard_id,
        pretty_name=pretty_name,
        dbname=sharded_dbname,
        files=files,
        migrations=migrations,
    )


_names_used = {}


def _database_name(service_name: str | None, dbname: str, shard_id: int):
    dbkey = (service_name, dbname)
    suffix = ''
    if shard_id != SINGLE_SHARD:
        suffix = f'_{shard_id}'
    prefix = ''
    if service_name is not None:
        prefix = f'{service_name}_'
    name = _normalize_name(prefix + dbname)
    dbname = _normalize_name(name + suffix)
    if len(dbname) > DB_NAME_MAX:
        dbname = _shortened(name, suffix)
    if dbname not in _names_used:
        _names_used[dbname] = dbkey
    elif _names_used[dbname] != dbkey:
        raise exceptions.NameCannotBeShortend(
            f'Database name conflict for {dbkey} and {_names_used[dbname]}'
        )
    return dbname


def _shortened(name: str, suffix: str):
    short_name = ''.join([part[:1] for part in name.split('_')])
    name_hash = hashlib.sha1(name.encode('utf-8')).hexdigest()
    hash_len = DB_NAME_MAX - len(short_name) - len(suffix) - 1
    name_hash = name_hash[:hash_len]
    dbname = f'{short_name}_{name_hash}{suffix}'
    if len(dbname) > DB_NAME_MAX:
        raise exceptions.NameCannotBeShortend(
            f'Dbname cannot be shortened {name}{suffix}'
        )
    return dbname


def _parse_shard_name(name) -> ShardName:
    parts = name.rsplit('@', 1)
    if len(parts) == 2:
        try:
            shard_id = int(parts[1])
        except (ValueError, TypeError):
            pass
        else:
            return ShardName(parts[0], shard_id)
    return ShardName(name, SINGLE_SHARD)


def _normalize_name(name):
    return name.replace('.', '_').replace('-', '_')
