from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
from itertools import starmap
from typing import TYPE_CHECKING, Optional

from datamodel_code_generator.util import BaseModel

if TYPE_CHECKING:
    from collections.abc import Iterable


class Import(BaseModel):
    from_: Optional[str] = None  # noqa: UP045
    import_: str
    alias: Optional[str] = None  # noqa: UP045
    reference_path: Optional[str] = None  # noqa: UP045

    @classmethod
    @lru_cache
    def from_full_path(cls, class_path: str) -> Import:
        split_class_path: list[str] = class_path.split(".")
        return Import(from_=".".join(split_class_path[:-1]) or None, import_=split_class_path[-1])


class Imports(defaultdict[Optional[str], set[str]]):
    def __str__(self) -> str:
        return self.dump()

    def __init__(self, use_exact: bool = False) -> None:  # noqa: FBT001, FBT002
        super().__init__(set)
        self.alias: defaultdict[str | None, dict[str, str]] = defaultdict(dict)
        self.counter: dict[tuple[str | None, str], int] = defaultdict(int)
        self.reference_paths: dict[str, Import] = {}
        self.use_exact: bool = use_exact

    def _set_alias(self, from_: str | None, imports: set[str]) -> list[str]:
        return [
            f"{i} as {self.alias[from_][i]}" if i in self.alias[from_] and i != self.alias[from_][i] else i
            for i in sorted(imports)
        ]

    def create_line(self, from_: str | None, imports: set[str]) -> str:
        if from_:
            return f"from {from_} import {', '.join(self._set_alias(from_, imports))}"
        return "\n".join(f"import {i}" for i in self._set_alias(from_, imports))

    def dump(self) -> str:
        return "\n".join(starmap(self.create_line, self.items()))

    def append(self, imports: Import | Iterable[Import] | None) -> None:
        if imports:
            if isinstance(imports, Import):
                imports = [imports]
            for import_ in imports:
                if import_.reference_path:
                    self.reference_paths[import_.reference_path] = import_
                if "." in import_.import_:
                    self[None].add(import_.import_)
                    self.counter[None, import_.import_] += 1
                else:
                    self[import_.from_].add(import_.import_)
                    self.counter[import_.from_, import_.import_] += 1
                    if import_.alias:
                        self.alias[import_.from_][import_.import_] = import_.alias

    def remove(self, imports: Import | Iterable[Import]) -> None:
        if isinstance(imports, Import):  # pragma: no cover
            imports = [imports]
        for import_ in imports:
            if "." in import_.import_:  # pragma: no cover
                self.counter[None, import_.import_] -= 1
                if self.counter[None, import_.import_] == 0:  # pragma: no cover
                    self[None].remove(import_.import_)
                    if not self[None]:
                        del self[None]
            else:
                self.counter[import_.from_, import_.import_] -= 1  # pragma: no cover
                if self.counter[import_.from_, import_.import_] == 0:  # pragma: no cover
                    self[import_.from_].remove(import_.import_)
                    if not self[import_.from_]:
                        del self[import_.from_]
                    if import_.alias:  # pragma: no cover
                        del self.alias[import_.from_][import_.import_]
                        if not self.alias[import_.from_]:
                            del self.alias[import_.from_]

    def remove_referenced_imports(self, reference_path: str) -> None:
        if reference_path in self.reference_paths:
            self.remove(self.reference_paths[reference_path])


IMPORT_ANNOTATED = Import.from_full_path("typing.Annotated")
IMPORT_ANY = Import.from_full_path("typing.Any")
IMPORT_LIST = Import.from_full_path("typing.List")
IMPORT_SET = Import.from_full_path("typing.Set")
IMPORT_UNION = Import.from_full_path("typing.Union")
IMPORT_OPTIONAL = Import.from_full_path("typing.Optional")
IMPORT_LITERAL = Import.from_full_path("typing.Literal")
IMPORT_TYPE_ALIAS = Import.from_full_path("typing.TypeAlias")
IMPORT_SEQUENCE = Import.from_full_path("typing.Sequence")
IMPORT_FROZEN_SET = Import.from_full_path("typing.FrozenSet")
IMPORT_MAPPING = Import.from_full_path("typing.Mapping")
IMPORT_ABC_SEQUENCE = Import.from_full_path("collections.abc.Sequence")
IMPORT_ABC_SET = Import.from_full_path("collections.abc.Set")
IMPORT_ABC_MAPPING = Import.from_full_path("collections.abc.Mapping")
IMPORT_ENUM = Import.from_full_path("enum.Enum")
IMPORT_ANNOTATIONS = Import.from_full_path("__future__.annotations")
IMPORT_DICT = Import.from_full_path("typing.Dict")
IMPORT_DECIMAL = Import.from_full_path("decimal.Decimal")
IMPORT_DATE = Import.from_full_path("datetime.date")
IMPORT_DATETIME = Import.from_full_path("datetime.datetime")
IMPORT_TIMEDELTA = Import.from_full_path("datetime.timedelta")
IMPORT_PATH = Import.from_full_path("pathlib.Path")
IMPORT_TIME = Import.from_full_path("datetime.time")
IMPORT_UUID = Import.from_full_path("uuid.UUID")
IMPORT_PENDULUM_DATE = Import.from_full_path("pendulum.Date")
IMPORT_PENDULUM_DATETIME = Import.from_full_path("pendulum.DateTime")
IMPORT_PENDULUM_DURATION = Import.from_full_path("pendulum.Duration")
IMPORT_PENDULUM_TIME = Import.from_full_path("pendulum.Time")
