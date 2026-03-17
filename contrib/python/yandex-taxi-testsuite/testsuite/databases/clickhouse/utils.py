import pathlib


def scan_sql_directory(root: pathlib.Path) -> list[pathlib.Path]:
    return [
        entry
        for entry in sorted(root.iterdir())
        if entry.is_file() and entry.suffix == '.sql'
    ]
