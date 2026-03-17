from ._ryaml import InvalidYamlError, loads, loads_all, dumps

from typing import IO, AnyStr, Any
import io


def _read_file(fp: IO[AnyStr]) -> str:
    data = fp.read()
    if isinstance(data, str):
        return data
    elif isinstance(data, bytes):
        return data.decode("utf8")
    else:
        return bytes(data).decode('utf8')


def load(fp: IO[AnyStr]) -> Any:
    if not isinstance(fp, io.IOBase):
        raise TypeError("fp must be a file-like object")
    return loads(_read_file(fp))


def load_all(fp: IO[AnyStr]) -> list[Any]:
    if not isinstance(fp, io.IOBase):
        raise TypeError("fp must be a file-like object")
    return loads_all(_read_file(fp))


def dump(fp: IO[AnyStr], obj: Any) -> None:
    yaml = dumps(obj)
    if isinstance(fp, io.TextIOBase):
        fp.write(yaml) # type: ignore
    else:
        fp.write(yaml.encode('utf8')) # type: ignore
