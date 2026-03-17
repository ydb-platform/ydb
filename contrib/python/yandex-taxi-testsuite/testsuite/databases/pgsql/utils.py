import hashlib
import pathlib
import typing
import urllib.parse


def scan_sql_directory(root: pathlib.Path) -> list[pathlib.Path]:
    return [
        path
        for path in sorted(root.iterdir())
        if path.is_file() and path.suffix == '.sql'
    ]


def connstr_replace_dbname(connstr: str, dbname: str) -> str:
    """Replace dbname in existing connection string."""
    if connstr.endswith(' dbname='):
        return connstr + dbname
    if connstr.startswith('postgresql://'):
        url = urllib.parse.urlparse(connstr)
        url = url._replace(path=dbname)  # pylint: disable=protected-access
        return url.geturl()
    raise RuntimeError(
        f'Unsupported PostgreSQL connection string format {connstr!r}',
    )


def get_files_hash(paths: typing.Iterable[pathlib.Path]) -> str:
    result = hashlib.md5()
    for path in paths:
        if path.is_dir():
            files = sorted(
                child for child in path.rglob('*') if child.is_file()
            )
        elif path.is_file():
            files = [path]
        else:
            continue

        for file_path in files:
            result.update(bytes(str(file_path) + '\n', 'utf8'))
            with file_path.open('rb') as file:
                content = file.read()
            result.update(b'%d\n' % len(content))
            result.update(content)
    return result.hexdigest()
