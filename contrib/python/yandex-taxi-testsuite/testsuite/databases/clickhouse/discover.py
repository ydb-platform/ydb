import collections
import pathlib
import typing

from . import classes, utils


def find_schemas(
    schema_dirs: list[pathlib.Path],
    dbprefix: str = 'testsuite-',
) -> dict[str, classes.DatabaseConfig]:
    result = {}
    for path in schema_dirs:
        if not path.is_dir():
            continue
        for dbname, migrations in _scan_path(path).items():
            full_db_name: str = dbprefix + dbname
            result[dbname] = classes.DatabaseConfig(
                dbname=full_db_name,
                migrations=migrations,
            )

    return result


def _scan_path(
    schema_path: pathlib.Path,
) -> typing.DefaultDict[str, list[pathlib.Path]]:
    result = collections.defaultdict(list)
    for entry in schema_path.iterdir():
        if entry.suffix == '.sql' and entry.is_file():
            result[entry.stem].append(entry)
        elif entry.is_dir():
            result[entry.stem].extend(utils.scan_sql_directory(entry))

    return result
