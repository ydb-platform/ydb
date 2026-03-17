# aiocsv

Asynchronous CSV reading and writing.


## Installation

`pip install aiocsv`. Python 3.8+ is required.

This module contains an extension written in C. Pre-build binaries
may not be available for your configuration. You might need a C compiler
and Python headers to install aiocsv.


## Usage

AsyncReader & AsyncDictReader accept any object that has a `read(size: int)` coroutine,
which should return a string.

AsyncWriter & AsyncDictWriter accept any object that has a `write(b: str)` coroutine.

Reading is implemented using a custom CSV parser, which should behave exactly like the CPython parser.

Writing is implemented using the synchronous csv.writer and csv.DictWriter objects -
the serializers write data to a StringIO, and that buffer is then rewritten to the underlying
asynchronous file.


## Example

Example usage with [aiofiles](https://pypi.org/project/aiofiles/).

```python
import asyncio
import csv

import aiofiles
from aiocsv import AsyncReader, AsyncDictReader, AsyncWriter, AsyncDictWriter

async def main():
    # simple reading
    async with aiofiles.open("some_file.csv", mode="r", encoding="utf-8", newline="") as afp:
        async for row in AsyncReader(afp):
            print(row)  # row is a list

    # dict reading, tab-separated
    async with aiofiles.open("some_other_file.tsv", mode="r", encoding="utf-8", newline="") as afp:
        async for row in AsyncDictReader(afp, delimiter="\t"):
            print(row)  # row is a dict

    # simple writing, "unix"-dialect
    async with aiofiles.open("new_file.csv", mode="w", encoding="utf-8", newline="") as afp:
        writer = AsyncWriter(afp, dialect="unix")
        await writer.writerow(["name", "age"])
        await writer.writerows([
            ["John", 26], ["Sasha", 42], ["Hana", 37]
        ])

    # dict writing, all quoted, "NULL" for missing fields
    async with aiofiles.open("new_file2.csv", mode="w", encoding="utf-8", newline="") as afp:
        writer = AsyncDictWriter(afp, ["name", "age"], restval="NULL", quoting=csv.QUOTE_ALL)
        await writer.writeheader()
        await writer.writerow({"name": "John", "age": 26})
        await writer.writerows([
            {"name": "Sasha", "age": 42},
            {"name": "Hana"}
        ])

asyncio.run(main())
```

## Differences with `csv`

`aiocsv` strives to be a drop-in replacement for Python's builtin [csv module](https://docs.python.org/3/library/csv.html).
However, there are a few notable differences, due to technical limitations or CPython bugs:

- Readers accept objects with async `read` methods, instead of an AsyncIterable over lines
    from a file.
- `AsyncDictReader.fieldnames` can be `None` - use `await AsyncDictReader.get_fieldnames()` instead.
- Changes to `csv.field_size_limit` are not picked up by existing Reader instances.
    The field size limit is cached on Reader instantiation to avoid expensive function calls
    on each character of the input.
- `QUOTE_NOTNULL` and `QUOTE_STRINGS` work on readers even in 3.12. aiocsv does not replicate
    [CPython bug #113732](https://github.com/python/cpython/issues/113732).

Other, minor, differences include:
- `AsyncReader.line_num`, `AsyncDictReader.line_num` and `AsyncDictReader.dialect` are not settable,
- `AsyncDictReader.reader` is of `AsyncReader` type,
- `AsyncDictWriter.writer` is of `AsyncWriter` type,
- `AsyncDictWriter` provides an extra, read-only `dialect` property.


## Reference


### aiocsv.AsyncReader

```
AsyncReader(
    asyncfile: aiocsv.protocols.WithAsyncRead,
    dialect: str | csv.Dialect | Type[csv.Dialect] = "excel",
    **csv_dialect_kwargs: Unpack[aiocsv.protocols.CsvDialectKwargs],
)
```

An object that iterates over records in the given asynchronous CSV file.
Additional keyword arguments are understood as dialect parameters.

Iterating over this object returns parsed CSV rows (`List[str]`).

*Methods*:
- `__aiter__(self) -> self`
- `async __anext__(self) -> List[str]`

*Read-only properties*:
- `dialect` (`aiocsv.protocols.DialectLike`): The dialect used when parsing
- `line_num` (`int`): The number of lines read from the source file. This coincides with a 1-based index
    of the line number of the last line of the recently parsed record.


### aiocsv.AsyncDictReader

```
AsyncDictReader(
    asyncfile: aiocsv.protocols.WithAsyncRead,
    fieldnames: Optional[Sequence[str]] = None,
    restkey: Optional[str] = None,
    restval: Optional[str] = None,
    dialect: str | csv.Dialect | Type[csv.Dialect] = "excel",
    **csv_dialect_kwargs: Unpack[aiocsv.protocols.CsvDialectKwargs],
)
```

An object that iterates over records in the given asynchronous CSV file.
All arguments work exactly the same was as in csv.DictReader.

Iterating over this object returns parsed CSV rows (`Dict[str, str]`).

*Methods*:
- `__aiter__(self) -> self`
- `async __anext__(self) -> Dict[str, str]`
- `async get_fieldnames(self) -> List[str]`


*Properties*:
- `fieldnames` (`List[str] | None`): field names used when converting rows to dictionaries  
    **⚠️** Unlike csv.DictReader, this property can't read the fieldnames if they are missing -
    it's not possible to `await` on the header row in a property getter.
    **Use `await reader.get_fieldnames()`**.
    ```py
    reader = csv.DictReader(some_file)
    reader.fieldnames  # ["cells", "from", "the", "header"]

    areader = aiofiles.AsyncDictReader(same_file_but_async)
    areader.fieldnames   # ⚠️ None
    await areader.get_fieldnames()  # ["cells", "from", "the", "header"]
    ```
- `restkey` (`str | None`): If a row has more cells then the header, all remaining cells are stored under
    this key in the returned dictionary. Defaults to `None`.
- `restval` (`str | None`): If a row has less cells then the header, then missing keys will use this
    value. Defaults to `None`.
- `reader`: Underlying `aiofiles.AsyncReader` instance

*Read-only properties*:
- `dialect` (`aiocsv.protocols.DialectLike`): Link to `self.reader.dialect` - the current csv.Dialect
- `line_num` (`int`): The number of lines read from the source file. This coincides with a 1-based index
    of the line number of the last line of the recently parsed record.


### aiocsv.AsyncWriter

```
AsyncWriter(
    asyncfile: aiocsv.protocols.WithAsyncWrite,
    dialect: str | csv.Dialect | Type[csv.Dialect] = "excel",
    **csv_dialect_kwargs: Unpack[aiocsv.protocols.CsvDialectKwargs],
)
```

An object that writes csv rows to the given asynchronous file.
In this object "row" is a sequence of values.

Additional keyword arguments are passed to the underlying csv.writer instance.

*Methods*:
- `async writerow(self, row: Iterable[Any]) -> None`:
    Writes one row to the specified file.
- `async writerows(self, rows: Iterable[Iterable[Any]]) -> None`:
    Writes multiple rows to the specified file.

*Readonly properties*:
- `dialect` (`aiocsv.protocols.DialectLike`): Link to underlying's csv.writer's `dialect` attribute


### aiocsv.AsyncDictWriter

```
AsyncDictWriter(
    asyncfile: aiocsv.protocols.WithAsyncWrite,
    fieldnames: Sequence[str],
    restval: Any = "",
    extrasaction: Literal["raise", "ignore"] = "raise",
    dialect: str | csv.Dialect | Type[csv.Dialect] = "excel",
    **csv_dialect_kwargs: Unpack[aiocsv.protocols.CsvDialectKwargs],
)
```

An object that writes csv rows to the given asynchronous file.
In this object "row" is a mapping from fieldnames to values.

Additional keyword arguments are passed to the underlying csv.DictWriter instance.

*Methods*:
- `async writeheader(self) -> None`: Writes header row to the specified file.
- `async writerow(self, row: Mapping[str, Any]) -> None`:
    Writes one row to the specified file.
- `async writerows(self, rows: Iterable[Mapping[str, Any]]) -> None`:
    Writes multiple rows to the specified file.

*Properties*:
- `fieldnames` (`Sequence[str]`): Sequence of keys to identify the order of values when writing rows
    to the underlying file
- `restval` (`Any`): Placeholder value used when a key from fieldnames is missing in a row,
    defaults to `""`
- `extrasaction` (`Literal["raise", "ignore"]`): Action to take when there are keys in a row, which are not present in
    fieldnames, defaults to `"raise"` which causes ValueError to be raised on extra keys,
    may be also set to `"ignore"` to ignore any extra keys
- `writer`: Link to the underlying `AsyncWriter`

*Readonly properties*:
- `dialect` (`aiocsv.protocols.DialectLike`): Link to underlying's csv.reader's `dialect` attribute


### aiocsv.protocols.WithAsyncRead
A `typing.Protocol` describing an asynchronous file, which can be read.


### aiocsv.protocols.WithAsyncWrite
A `typing.Protocol` describing an asynchronous file, which can be written to.

### aiocsv.protocols.DialectLike
Type of an instantiated `dialect` property. Thank CPython for an incredible mess of
having unrelated and disjoint `csv.Dialect` and `_csv.Dialect` classes.

### aiocsv.protocols.CsvDialectArg
Type of the `dialect` argument, as used in the `csv` module.


### aiocsv.protocols.CsvDialectKwargs
Keyword arguments used by `csv` module to override the dialect settings during reader/writer
instantiation.

## Development

Contributions are welcome, however please open an issue beforehand. `aiocsv` is meant as
a replacement for the built-in `csv`, any features not present in the latter will be rejected.

### Building from source

To create a wheel (and a source tarball), run `python -m build`.

For local development, use a [virtual environment](https://docs.python.org/3/library/venv.html).
`pip install --editable .` will build the C extension and make it available for the current
venv. This is required for running the tests. However, [due to the mess of Python packaging](https://docs.python.org/3/library/venv.html)
this will force an optimized build without debugging symbols. If you need to debug the C part
of aiocsv and build the library with e.g. debugging symbols, the only sane way is to
run `python setup.py build --debug` and manually copy the shared object/DLL from `build/lib*/aiocsv`
to `aiocsv`.

### Tests

This project uses [pytest](https://docs.pytest.org/en/latest/contents.html) with
[pytest-asyncio](https://pypi.org/project/pytest-asyncio/) for testing. Run `pytest`
after installing the library in the manner explained above.

### Linting & other tools

This library uses [black](https://pypi.org/project/black/) and [isort](https://pypi.org/project/isort/)
for formatting and [pyright](https://github.com/microsoft/pyright) in strict mode for type checking.

For the C part of library, please use [clang-format](https://clang.llvm.org/docs/ClangFormat.html)
for formatting and [clang-tidy](https://clang.llvm.org/extra/clang-tidy/) linting,
however this are not yet integrated in the CI.

### Installing required tools

`pip install -r requirements.dev.txt` will pull all of the development tools mentioned above,
however this might not be necessary depending on your setup. For example, if you use VS Code
with the Python extension, pyright is already bundled and doesn't need to be installed again. 

### Recommended VS Code settings

Use [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python),
[Pylance](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance)
(should be installed automatically alongside Python extension),
[black](https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter) and
[isort](https://marketplace.visualstudio.com/items?itemName=ms-python.isort) Python extensions.

You will need to install all dev dependencies from `requirements.dev.txt`, except for `pyright`.
Recommended `.vscode/settings.json`:

```json
{
    "C_Cpp.codeAnalysis.clangTidy.enabled": true,
    "python.testing.pytestArgs": [
        "."
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true,
    "[python]": {
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.organizeImports": "always"
        }
    },
    "[c]": {
        "editor.formatOnSave": true
    }
}
```

For the C part of the library, [C/C++ extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools) is sufficient.
Ensure that your system has Python headers installed. Usually a separate package like python3-dev
needs to be installed, consult with your system repositories on that. `.vscode/c_cpp_properties.json`
needs to manually include Python headers under `includePath`. On my particular system this
config file looks like this:

```json
{
    "configurations": [
        {
            "name": "Linux",
            "includePath": [
                "${workspaceFolder}/**",
                "/usr/include/python3.11"
            ],
            "defines": [],
            "compilerPath": "/usr/bin/clang",
            "cStandard": "c17",
            "cppStandard": "c++17",
            "intelliSenseMode": "linux-clang-x64"
        }
    ],
    "version": 4
}
```
