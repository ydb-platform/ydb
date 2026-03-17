__copyright__ = """
Copyright (C) 2022- University of Illinois Board of Trustees
"""

__license__ = """
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

from collections.abc import Mapping, Sequence
from typing import Callable, final

from typing_extensions import override

__all__: Sequence[str] = [
    "AstLoad",
    "AstModule",
    "Dialect",
    "DialectTypes",
    "Error",
    "EvalSeverity",
    "FileLoader",
    "FrozenModule",
    "Globals",
    "Interface",
    "LibraryExtension",
    "Lint",
    "Module",
    "OpaquePythonObject",
    "ResolvedFileSpan",
    "ResolvedPos",
    "ResolvedSpan",
    "StarlarkError",
    "eval",
    "parse",
]

@final
class ResolvedPos:
    line: int
    column: int

@final
class ResolvedSpan:
    begin: ResolvedPos
    end: ResolvedPos

@final
class ResolvedFileSpan:
    file: str
    span: ResolvedSpan

    @override
    def __str__(self) -> str: ...

class StarlarkError(Exception): ...

@final
class EvalSeverity:
    Error: EvalSeverity
    Warning: EvalSeverity
    Advice: EvalSeverity
    Disabled: EvalSeverity

    @override
    def __str__(self) -> str: ...
    @override
    def __eq__(self, other: object) -> bool: ...

@final
class Lint:
    resolved_location: ResolvedFileSpan
    short_name: str
    severity: EvalSeverity
    problem: str
    original: str

@final
class Error:
    @property
    def span(self) -> ResolvedFileSpan | None: ...
    @override
    def __str__(self) -> str: ...

@final
class DialectTypes:
    DISABLE: DialectTypes
    PARSE_ONLY: DialectTypes
    ENABLE: DialectTypes

@final
class Dialect:
    enable_def: bool
    enable_lambda: bool
    enable_load: bool
    enable_keyword_only_arguments: bool
    enable_types: DialectTypes
    enable_load_reexport: bool
    enable_top_level_stmt: bool
    enable_f_strings: bool

    @staticmethod
    def standard() -> Dialect: ...
    @staticmethod
    def extended() -> Dialect: ...

@final
class Interface:
    pass

@final
class AstLoad:
    module_id: str
    symbols: Mapping[str, str]

@final
class AstModule:
    def lint(self) -> Sequence[Lint]: ...
    def loads(self) -> Sequence[AstLoad]: ...
    def typecheck(self,
                globals: Globals,
                loads: dict[str, Interface],
            ) -> tuple[list[Error], Interface, None]:
        ...

@final
class LibraryExtension:
    StructType: LibraryExtension
    RecordType: LibraryExtension
    EnumType: LibraryExtension
    Map: LibraryExtension
    Filter: LibraryExtension
    Partial: LibraryExtension
    Debug: LibraryExtension
    Print: LibraryExtension
    Pprint: LibraryExtension
    Breakpoint: LibraryExtension
    Json: LibraryExtension
    Typing: LibraryExtension
    Internal: LibraryExtension
    CallStack: LibraryExtension
    RustDecimal: LibraryExtension

@final
class OpaquePythonObject:
    def __new__(cls, obj: object) -> OpaquePythonObject: ...

@final
class Globals:
    @staticmethod
    def standard() -> Globals: ...
    @staticmethod
    def extended_by(extensions: list[LibraryExtension]) -> Globals: ...

@final
class FrozenModule:
    def call(self, name: str, *args: object, **kwargs: object) -> object: ...

@final
class Module:
    def __getitem__(self, key: str, /) -> object: ...
    def __setitem__(self, key: str, value: object, /) -> None: ...
    def add_callable(self, name: str, callable: Callable[..., object]) -> None: ...
    def freeze(self) -> FrozenModule: ...

@final
class FileLoader:
    def __new__(cls, load_func: Callable[[str], FrozenModule]) -> FileLoader: ...

def parse(filename: str, content: str, dialect: Dialect | None = None) -> AstModule: ...
def eval(
    module: Module,
    ast: AstModule,
    globals: Globals,
    file_loader: FileLoader | None = None,
) -> object: ...
