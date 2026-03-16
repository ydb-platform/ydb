import pathlib
import typing


def load_queries_directory(root: pathlib.Path) -> typing.Iterable[str]:
    for path in scan_sql_directory(root):
        yield path.read_text()


def scan_sql_directory(root: pathlib.Path) -> list[pathlib.Path]:
    return [
        path
        for path in sorted(root.iterdir())
        if path.is_file() and path.suffix == '.sql'
    ]
