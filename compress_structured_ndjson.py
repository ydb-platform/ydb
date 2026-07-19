#!/usr/bin/env python3
"""
Чтение NDJSON в ``FlattenInternalized``, запись и чтение текстового дампа этого представления.

**Режимы CLI**
  - ``INPUT.ndjson --internalized-out X.dump`` — ``read_ndjson`` + запись дампа во внешний файл.
  - ``--read-internalized-only X.dump`` — только чтение дампа (позиционный ``INPUT`` не указывать).

**NDJSON** (``read_ndjson``): каждая строка — JSON-объект; вложенные объекты и **массивы** раскрываются
по пути. Элемент пути — ``PathElement`` с дискриминатором ``PathElement.Kind``: ключ подобъекта,
индекс элемента массива (в нотации ``[n]``) или индекс фрагмента строки (``{n}``). В дампе каждая строка
каталога путей — текстовая нотация этапов через ``::``, внутри этапа: ключи через ``.``, индекс массива
в ``[]``, фрагмент строки в ``{}`` (пример: ``a.b.c[4].d{0}``). В ключах экранируются ``\\``, ``.``, ``[]{}:``
и управляющие символы.
Строковый атрибут пытаются распарсить как вложенный JSON object/array. Если ``json.loads`` не удался,
ищется суффикс по шаблону ``_TRUNCATION_TOTAL_BYTES_RE``: запятая, затем текст **без запятых** до
``(truncated, total <число> bytes)`` (часто после запятой идёт ``...`` и пояснение об усечении). Совпадение
отрезают; к остатку дописывают закрытие кавычек и ``{}``/``[]`` (см. ``_close_truncated_json_document``),
при необходимости убирают запятую перед последней ``}``/``]``, снова вызывают ``json.loads``.
Если и после этого разбор не удался — вложенный JSON не извлекается (``None``).
Если в строке есть встроенный идентификатор — **UUID** (``8-4-4-4-12`` hex с дефисами),
**десятичные цифры с косой чертой** (например ``16348081190184/``), **``6`` цифр — дефис — ``7`` цифр**
(например ``260509-5018556``) или **32 hex** подряд (MD5 и т.п.),
строка режется по таким фрагментам; части обходятся отдельно (в пути — ``PathElement`` с ``Kind.STRING_FRAGMENT``, индексы ``0``, ``1``, …).

В ``FlattenInternalized``: ``path_pool``, ``value_pool``, ``rows``.

**Дамп** (запись/чтение UTF-8 файла): заголовок JSON (``path_entries``, ``value_entries``, ``rows``),
строки путей (нотация ``a.b[0]::{...}``, см. выше), строки значений ``[kind, value]`` (``kind``: одно из ``null``, ``boolean``, ``number``, ``string``, ``array``, ``object``), строки таблиц.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Iterator
import zstandard as zstd
from collections import Counter


# Прогресс в stderr: не чаще, чем раз в _PROGRESS_MIN_INTERVAL_SEC и/или каждые _PROGRESS_ROW_INTERVAL строк
_PROGRESS_ROW_INTERVAL = 1000_000
_PROGRESS_MIN_INTERVAL_SEC = 5.0
_JSON_SEP = (",", ":")

# Встроенные в строку идентификаторы: UUID; цифры/; 6 цифр - 7 цифр; 32 hex (порядок: «32 цифр/» раньше сырого 32-hex).
_STRING_EMBEDDED_ID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r"|\d+/|\d{6}-\d{7}|[0-9a-fA-F]{32}",
    re.IGNORECASE,
)


def _split_string_by_embedded_ids(s: str) -> list[str] | None:
    """
    Разбить строку по непересекающимся вхождениям UUID, ``цифры/``, ``dddddd-ddddddd`` или 32-символьного hex.
    Возвращает список из двух и более непустых частей или ``None``, если резать нечего.
    """
    parts: list[str] = []
    last = 0
    for m in _STRING_EMBEDDED_ID_RE.finditer(s):
        if m.start() > last:
            head = s[last : m.start()]
            if head:
                parts.append(head)
        parts.append(m.group(0))
        last = m.end()
    if last < len(s):
        tail = s[last:]
        if tail:
            parts.append(tail)
    if len(parts) <= 1:
        return None
    return parts

# Маркер усечённого лога: запятая + фрагмент без запятых + «(truncated, total <n> bytes)».
_TRUNCATION_TOTAL_BYTES_RE = re.compile(
    r",[^,]*\(truncated,\s*total\s+\d+\s+bytes\)",
    re.IGNORECASE,
)


def _loads_json_object_or_array(raw: str) -> dict[str, Any] | list[Any] | None:
    try:
        v = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError, RecursionError):
        return None
    if isinstance(v, dict) or isinstance(v, list):
        return v
    return None

def _close_truncated_json_document(s: str) -> str:
    """
    Дописать к обрезанному тексту минимальные ``"``, ``}``, ``]``, чтобы незакрытые строки и скобки
    ``{}``/``[]`` стали синтаксически завершёнными (эвристика, без полного JSON-парсера).
    """
    stack: list[str] = []
    in_string = False
    escape = False
    for c in s:
        if escape:
            escape = False
            continue
        if in_string:
            if c == "\\":
                escape = True
            elif c == '"':
                in_string = False
            continue
        if c == '"':
            in_string = True
            continue
        if c == "{":
            stack.append("}")
            continue
        if c == "[":
            stack.append("]")
            continue
        if c == "}" and stack and stack[-1] == "}":
            stack.pop()
            continue
        if c == "]" and stack and stack[-1] == "]":
            stack.pop()
            continue
    out = s
    if in_string:
        out += '"'
    out += "".join(reversed(stack))
    return out


def _try_parse_nested_json_document(s: str) -> dict[str, Any] | list[Any] | None:
    """
    Успешный ``json.loads(s)`` → вернуть ``dict``/``list``. Иначе поиск суффикса ``_TRUNCATION_TOTAL_BYTES_RE``;
    при совпадении — отрезать суффикс, дописать закрытие JSON (``_close_truncated_json_document``) и снова
    ``json.loads``. При любой неудаче — ``None``.
    """
    parsed = _loads_json_object_or_array(s)
    if parsed is not None:
        return parsed

    m = _TRUNCATION_TOTAL_BYTES_RE.search(s)
    if m is None:
        return None

    cut = s[: m.start()].rstrip()
    if not cut:
        return None

    closed = _close_truncated_json_document(cut)
    parsed = _loads_json_object_or_array(closed)
    if parsed is not None:
        return parsed
    return None


def _timing_log(label: str, seconds: float) -> None:
    print(f"[ndjson-compress] время {label}: {seconds:.3f} s", file=sys.stderr, flush=True)


def _progress_log(msg: str) -> None:
    print(f"[ndjson-compress] {msg}", file=sys.stderr, flush=True)


class _ProgressTicker:
    """Равномерные сообщения по числу шагов и/или по времени (monotonic)."""

    def __init__(
        self,
        label: str,
        *,
        enabled: bool,
        row_interval: int = _PROGRESS_ROW_INTERVAL,
        min_interval_sec: float = _PROGRESS_MIN_INTERVAL_SEC,
    ) -> None:
        self.label = label
        self.enabled = enabled
        self.row_interval = max(1, row_interval)
        self.min_interval_sec = min_interval_sec
        self._n = 0
        self._last_report = time.monotonic()

    def tick(self) -> None:
        if not self.enabled:
            return
        self._n += 1
        now = time.monotonic()
        if self._n % self.row_interval == 0 or now - self._last_report >= self.min_interval_sec:
            _progress_log(f"{self.label}: {self._n:,}")
            self._last_report = now

    def done(self, extra: str = "") -> None:
        if not self.enabled or self._n == 0:
            return
        _progress_log(f"{self.label}: готово, {self._n:,}{extra}")


def iter_ndjson_objects(path: Path) -> Iterator[dict[str, Any]]:
    """Построчно читать UTF-8 файл; непустые строки — json.loads, только объекты dict."""
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            o = json.loads(s)
            if not isinstance(o, dict):
                raise ValueError("каждая непустая строка должна быть JSON-объектом")
            yield o


@dataclass(frozen=True)
class ValueAsString:
    """Значение ячейки: ``kind`` — тип JSON; ``value`` — для ``string`` сырое содержимое, иначе ``json.dumps`` значения."""

    class JsonValueKind(str, Enum):
        """Дискриминатор типа значения по модели JSON (RFC 8259)."""

        NULL = "null"
        BOOLEAN = "boolean"
        NUMBER = "number"
        STRING = "string"
        ARRAY = "array"
        OBJECT = "object"


    kind: JsonValueKind
    value: str

    @staticmethod
    def _get_kind(value: Any) -> JsonValueKind:
        if value is None:
            return ValueAsString.JsonValueKind.NULL
        if isinstance(value, bool):
            return ValueAsString.JsonValueKind.BOOLEAN
        if isinstance(value, (int, float)):
            return ValueAsString.JsonValueKind.NUMBER
        if isinstance(value, str):
            return ValueAsString.JsonValueKind.STRING
        if isinstance(value, list):
            return ValueAsString.JsonValueKind.ARRAY
        if isinstance(value, dict):
            return ValueAsString.JsonValueKind.OBJECT
        raise TypeError(f"значение не представимо как JSON: {type(value)!r}")

    def __init__(self, value: Any) -> None:
        kind = ValueAsString._get_kind(value)
        if kind is ValueAsString.JsonValueKind.STRING:
            payload = value
        else:
            payload = json.dumps(value, ensure_ascii=False, separators=_JSON_SEP)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "value", payload)

    def to_string(self) -> str:
        return json.dumps(
            [self.kind.value, self.value],
            ensure_ascii=False,
            separators=_JSON_SEP,
        )

    def __str__(self) -> str:
        return self.to_string()

    @classmethod
    def from_string(cls, line: str) -> ValueAsString:
        j = json.loads(line.strip())
        if not isinstance(j, list) or len(j) != 2:
            raise ValueError(f"ожидался JSON-массив из двух элементов, получено {j!r}")
        if not isinstance(j[1], str):
            raise ValueError(f"второй элемент должен быть str, получено {type(j[1])}")
        if not isinstance(j[0], str):
            raise ValueError(
                f"первый элемент — строка-дискриминатор типа JSON, получено {type(j[0])}"
            )
        try:
            kind = ValueAsString.JsonValueKind(j[0])
        except ValueError as e:
            raise ValueError(f"неизвестный kind значения: {j[0]!r}") from e
        o = object.__new__(cls)
        object.__setattr__(o, "kind", kind)
        object.__setattr__(o, "value", j[1])
        return o


@dataclass
class ValueInternPool:
    entries: list[ValueAsString] = field(default_factory=list)

    def print_stat(self) -> None:
        kinds = Counter([str(e.kind) for e in self.entries])
        for k, v in kinds.items():
            print(f"{str(k)}: {v}")


@dataclass(frozen=True)
class PathElement:
    """Один сегмент пути: ключ объекта, индекс массива или индекс фрагмента строки."""

    class Kind(str, Enum):
        """Тип элемента пути."""

        OBJECT_KEY = "object_key"
        ARRAY_INDEX = "array_index"
        STRING_FRAGMENT = "string_fragment"

    kind: Kind
    value: str | int

    def __post_init__(self) -> None:
        if self.kind is PathElement.Kind.OBJECT_KEY:
            if not isinstance(self.value, str):
                raise TypeError(
                    f"PathElement.Kind.OBJECT_KEY требует str, получено {type(self.value)}"
                )
        elif self.kind in (
            PathElement.Kind.ARRAY_INDEX,
            PathElement.Kind.STRING_FRAGMENT,
        ):
            if type(self.value) is not int or isinstance(self.value, bool):
                raise TypeError(
                    f"{self.kind!r} требует int, получено {type(self.value)}"
                )

    @staticmethod
    def object_key(key: str) -> PathElement:
        return PathElement(PathElement.Kind.OBJECT_KEY, key)

    @staticmethod
    def array_index(i: int) -> PathElement:
        return PathElement(PathElement.Kind.ARRAY_INDEX, i)

    @staticmethod
    def string_fragment(i: int) -> PathElement:
        return PathElement(PathElement.Kind.STRING_FRAGMENT, i)


@dataclass(frozen=True)
class PathInternEntry:
    """Этапы пути; внутри этапа — элементы ``PathElement``. Сериализация и разбор нотации пути."""

    stages: tuple[tuple[PathElement, ...], ...]

    _KEY_ESCAPABLE: ClassVar[frozenset[str]] = frozenset("\\.[]{}:\n\r\t")

    @staticmethod
    def _escape_path_key(k: str) -> str:
        out: list[str] = []
        for c in k:
            if c in PathInternEntry._KEY_ESCAPABLE:
                if c == "\n":
                    out.append("\\n")
                elif c == "\r":
                    out.append("\\r")
                elif c == "\t":
                    out.append("\\t")
                else:
                    out.append("\\" + c)
            elif ord(c) < 32:
                out.append(f"\\x{ord(c):02x}")
            else:
                out.append(c)
        return "".join(out)

    @staticmethod
    def _read_path_key(st: str, i: int) -> tuple[str, int]:
        buf: list[str] = []
        n = len(st)
        while i < n:
            c = st[i]
            if c == "\\":
                i += 1
                if i >= n:
                    raise ValueError("обрыв escape в ключе пути")
                esc = st[i]
                if esc == "n":
                    buf.append("\n")
                elif esc == "r":
                    buf.append("\r")
                elif esc == "t":
                    buf.append("\t")
                elif esc == "x" and i + 2 < n:
                    hx = st[i + 1 : i + 3]
                    if len(hx) == 2 and all(
                        x in "0123456789abcdefABCDEF" for x in hx
                    ):
                        buf.append(chr(int(hx, 16)))
                        i += 3
                        continue
                    buf.append(esc)
                else:
                    buf.append(esc)
                i += 1
                continue
            if c in ".[{:":
                if c == ":" and i + 1 < n and st[i + 1] == ":":
                    raise ValueError(
                        "неэкранированный разделитель этапов :: внутри этапа пути"
                    )
                if c == ":":
                    raise ValueError(
                        "неэкранированный ':' в ключе пути (используйте \\:)"
                    )
                break
            buf.append(c)
            i += 1
        return "".join(buf), i

    @staticmethod
    def _path_stage_to_notation(stage: tuple[PathElement, ...]) -> str:
        parts: list[str] = []
        for idx, el in enumerate(stage):
            if el.kind is PathElement.Kind.OBJECT_KEY:
                if idx > 0:
                    prev = stage[idx - 1]
                    if prev.kind is PathElement.Kind.OBJECT_KEY:
                        parts.append(".")
                    elif prev.kind in (
                        PathElement.Kind.ARRAY_INDEX,
                        PathElement.Kind.STRING_FRAGMENT,
                    ):
                        parts.append(".")
                parts.append(PathInternEntry._escape_path_key(str(el.value)))
            elif el.kind is PathElement.Kind.ARRAY_INDEX:
                parts.append(f"[{el.value}]")
            elif el.kind is PathElement.Kind.STRING_FRAGMENT:
                parts.append(f"{{{el.value}}}")
            else:
                raise TypeError(el.kind)
        return "".join(parts)

    @staticmethod
    def _split_path_stages(line: str) -> list[str]:
        """Разбить строку пути по ``::``, не считая пары двоеточий внутри экранированных ключей."""
        parts: list[str] = []
        start = 0
        n = len(line)
        i = 0
        while i < n - 1:
            if line[i] == ":" and line[i + 1] == ":":
                nb = 0
                j = i - 1
                while j >= 0 and line[j] == "\\":
                    nb += 1
                    j -= 1
                if nb % 2 == 0:
                    parts.append(line[start:i])
                    i += 2
                    start = i
                    continue
            i += 1
        parts.append(line[start:])
        return parts

    @staticmethod
    def _parse_path_stage(st: str) -> tuple[PathElement, ...]:
        elements: list[PathElement] = []
        i = 0
        n = len(st)
        while i < n:
            if st[i] == ".":
                i += 1
                continue
            if st[i] == "[":
                j = i + 1
                if j < n and st[j] == "-":
                    j += 1
                while j < n and st[j].isdigit():
                    j += 1
                if j >= n or st[j] != "]":
                    raise ValueError(
                        f"ожидалось …[число] в пути, позиция {i}: {st[i : i + 20]!r}"
                    )
                num = int(st[i + 1 : j])
                elements.append(PathElement.array_index(num))
                i = j + 1
                continue
            if st[i] == "{":
                j = i + 1
                while j < n and st[j].isdigit():
                    j += 1
                if j >= n or st[j] != "}":
                    raise ValueError(
                        f"ожидалось …{{число}} в пути, позиция {i}: {st[i : i + 20]!r}"
                    )
                num = int(st[i + 1 : j])
                elements.append(PathElement.string_fragment(num))
                i = j + 1
                continue
            key, i = PathInternEntry._read_path_key(st, i)
            if not key and not elements and i >= n:
                break
            if not key:
                raise ValueError(f"пустой ключ в пути: {st!r}")
            elements.append(PathElement.object_key(key))
        return tuple(elements)

    def to_string(self) -> str:
        return "::".join(
            PathInternEntry._path_stage_to_notation(st) for st in self.stages
        )

    @classmethod
    def from_path_notation(cls, line: str) -> PathInternEntry:
        """Разобрать строку каталога путей из дампа (текстовая нотация ``::``, ``.``, ``[]``, ``{}``)."""
        s = line.strip()
        if not s:
            return cls(((),))
        stages_raw = PathInternEntry._split_path_stages(s)
        stages = tuple(PathInternEntry._parse_path_stage(t) for t in stages_raw)
        return cls(stages)


@dataclass
class PathInternPool:
    """Упорядоченный список ``PathInternEntry`` (индекс в каталоге путей)."""

    entries: list[PathInternEntry] = field(default_factory=list)


@dataclass
class FlattenInternalized:

    path_pool: PathInternPool
    value_pool: ValueInternPool
    rows: list[dict[int, int]]

    def print_stat(self) -> None:
        print("FlattenInternalized stat:")
        print(f"\tPaths: {len(self.path_pool.entries)}")
        print(f"\tValues: {len(self.value_pool.entries)}")
        self.value_pool.print_stat()
        print(f"\tRows: {len(self.rows)}")


def read_ndjson(path: Path, *, progress: bool = False) -> FlattenInternalized:
    rows: list[dict[int, int]] = []
    path_pool = PathInternPool()
    value_pool = ValueInternPool()
    value_entry_index: dict[ValueAsString, int] = {}
    path_index: dict[tuple[tuple[PathElement, ...], ...], int] = {}
    ticker = _ProgressTicker("чтение NDJSON", enabled=progress)

    for obj in iter_ndjson_objects(path):
        out: dict[int, int] = {}

        def intern_path(stages_key: tuple[tuple[PathElement, ...], ...]) -> int:
            i = path_index.get(stages_key)
            if i is None:
                i = len(path_pool.entries)
                path_pool.entries.append(PathInternEntry(stages_key))
                path_index[stages_key] = i
            return i

        def intern_value(v: Any) -> int:
            s = ValueAsString(v)
            i = value_entry_index.get(s)
            if i is None:
                i = len(value_pool.entries)
                value_pool.entries.append(s)
                value_entry_index[s] = i
            return i

        def walk(
            v: Any,
            doc_segs: list[PathElement],
            stage_prefix: tuple[tuple[PathElement, ...], ...],
        ) -> None:
            if isinstance(v, dict):
                if not v:
                    if doc_segs:
                        pk = stage_prefix + (tuple(doc_segs),)
                        pid = intern_path(pk)
                        out[pid] = intern_value(v)
                    elif stage_prefix:
                        pk = stage_prefix + ((),)
                        pid = intern_path(pk)
                        out[pid] = intern_value(v)
                    return
                for kk, vv in v.items():
                    walk(vv, doc_segs + [PathElement.object_key(kk)], stage_prefix)
            elif isinstance(v, list):
                if not v:
                    if doc_segs:
                        pk = stage_prefix + (tuple(doc_segs),)
                        pid = intern_path(pk)
                        out[pid] = intern_value(v)
                    elif stage_prefix:
                        pk = stage_prefix + ((),)
                        pid = intern_path(pk)
                        out[pid] = intern_value(v)
                    return
                for i, vv in enumerate(v):
                    walk(vv, doc_segs + [PathElement.array_index(i)], stage_prefix)
            else:
                if isinstance(v, str):
                    nested = _try_parse_nested_json_document(v)
                    if nested is not None:
                        walk(nested, [], stage_prefix + (tuple(doc_segs),))
                        return
                    parts = _split_string_by_embedded_ids(v)
                    if parts is not None:
                        for fi, part in enumerate(parts):
                            walk(
                                part,
                                doc_segs + [PathElement.string_fragment(fi)],
                                stage_prefix,
                            )
                        return
                pk = stage_prefix + (tuple(doc_segs),)
                pid = intern_path(pk)
                out[pid] = intern_value(v)

        walk(obj, [], ())
        rows.append(dict(out))
        ticker.tick()
    ticker.done(" строк")
    return FlattenInternalized(
        path_pool=path_pool,
        value_pool=value_pool,
        rows=rows,
    )


def store_internal_representation(
    internalized: FlattenInternalized, path: Path, *, progress: bool = False
) -> None:
    """Записать ``FlattenInternalized`` в UTF-8 файл (заголовок + каталоги + строки)."""
    with path.open("w", encoding="utf-8", newline="\n") as f:
        paths = internalized.path_pool
        values = internalized.value_pool
        rows = internalized.rows
        header: dict[str, Any] = {
            "path_entries": len(paths.entries),
            "value_entries": len(values.entries),
            "rows": len(rows),
        }
        f.write(json.dumps(header, ensure_ascii=False, separators=_JSON_SEP) + "\n")
        pt = _ProgressTicker("дамп FlattenInternalized: каталог путей", enabled=progress)
        for pe in paths.entries:
            f.write(pe.to_string() + "\n")
            pt.tick()
        pt.done(" путей")
        vt = _ProgressTicker("дамп FlattenInternalized: каталог значений", enabled=progress)
        for e in values.entries:
            f.write(e.to_string() + "\n")
            vt.tick()
        vt.done(" значений")
        rt = _ProgressTicker("дамп FlattenInternalized: строки таблицы", enabled=progress)
        for row in rows:
            idx_row = {str(pid): vi for pid, vi in row.items()}
            f.write(json.dumps(idx_row, ensure_ascii=False, separators=_JSON_SEP) + "\n")
            rt.tick()
        rt.done(" строк")
    if progress:
        _progress_log(f"дамп FlattenInternalized: запись завершена → {path}")


def load_internal_representation(path: Path, *, progress: bool = False) -> FlattenInternalized:
    """Прочитать ``FlattenInternalized`` из файла, записанного ``store_internal_representation``."""
    if progress:
        _progress_log(f"дамп FlattenInternalized: чтение {path}")
    with path.open("r", encoding="utf-8") as f:
        hdr = json.loads(f.readline())
        n_path = int(hdr["path_entries"])
        n_val = int(hdr["value_entries"])
        n_rows = int(hdr["rows"])

        path_entries: list[PathInternEntry] = []
        path_pool = PathInternPool()
        pt = _ProgressTicker("дамп FlattenInternalized чтение: пути", enabled=progress)
        for _ in range(n_path):
            line = f.readline()
            if not line:
                raise ValueError("неожиданный EOF в секции путей")
            path_entries.append(PathInternEntry.from_path_notation(line))
            pt.tick()
        pt.done(" путей")
        path_pool.entries = path_entries

        entries: list[ValueAsString] = []
        vt = _ProgressTicker("дамп FlattenInternalized чтение: значения", enabled=progress)
        for _ in range(n_val):
            line = f.readline()
            if not line:
                raise ValueError("неожиданный EOF в секции значений")
            e = ValueAsString.from_string(line)
            entries.append(e)
            vt.tick()
        vt.done(" значений")

        pool = ValueInternPool()
        pool.entries = entries

        rows: list[dict[int, int]] = []
        rt = _ProgressTicker("дамп FlattenInternalized чтение: строки", enabled=progress)
        for _ in range(n_rows):
            line = f.readline()
            if not line:
                raise ValueError("неожиданный EOF в секции строк")
            d = json.loads(line)
            row = {int(k): int(v) for k, v in d.items()}
            rows.append(row)
            rt.tick()
        rt.done(" строк")

    if progress:
        _progress_log("дамп FlattenInternalized: загружен в память")
    return FlattenInternalized(
        path_pool=path_pool, value_pool=pool, rows=rows
    )


def compress_and_print_stat(name: str, raw: str | bytes, level: int, print_above: int) -> bytes:
    """UTF-8 + zstd; печать размеров в stdout. ``raw`` — строка или уже закодированные байты."""
    raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
    compressed = zstd.ZstdCompressor(level=level).compress(raw_bytes)

    def _pretty_byte_size(n: int) -> str:
        if n >= 1024 * 1024:
            return f"{n / (1024.0 * 1024):.2f} MiB"
        if n >= 1024:
            return f"{n / 1024.0:.2f} KiB"
        return f"{n} B"

    raw_n, comp_n = len(raw_bytes), len(compressed)
    ratio_x = (raw_n / comp_n) if comp_n > 0 else float("inf")
    pct = (100.0 * comp_n / raw_n) if raw_n > 0 else 0.0
    if comp_n > print_above:
        print(
            f"[ndjson-compress] zstd-{level} {name}: raw {raw_n:,} B ({_pretty_byte_size(raw_n)}) → "
            f"{comp_n:,} B ({_pretty_byte_size(comp_n)}); "
            f"×{ratio_x:.2f} сжатие, compressed {pct:.1f}% от raw"
        )



def compress_internalized(state: FlattenInternalized, level: int) -> None:
    compress_and_print_stat("paths", str(state.path_pool), level, 0)
    entries = [str(e) for e in state.value_pool.entries]
    # entries = [str(e)[::-1] for e in state.value_pool.entries]
    # entries.sort()  # опционально: фиксированный порядок для сравнимости zstd между прогонами
    values = "\n".join(entries)
    compress_and_print_stat("values", values, level, 0)
    # with open("values.dmp", "wb") as f:
    #     f.write(values.encode("utf-8"))
    compress_and_print_stat("rows", str(state.rows), level, 0)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--internalized-out",
        type=Path,
        metavar="FILE",
        default=None,
        help="read_ndjson + запись дампа FlattenInternalized в этот файл",
    )
    ap.add_argument(
        "--read-internalized-only",
        type=Path,
        metavar="FILE",
        default=None,
        help="только загрузить дамп FlattenInternalized из файла; позиционный input не указывать",
    )
    ap.add_argument(
        "--time-steps",
        action="store_true",
        help="печатать в stderr длительность шагов ([ndjson-compress] время …)",
    )
    ap.add_argument(
        "--no-progress",
        action="store_true",
        help="не печатать в stderr промежуточные сообщения [ndjson-compress] о ходе работы",
    )
    ap.add_argument("input", type=Path, nargs="?", default=None, help="входной NDJSON")
    args = ap.parse_args()

    pg = not args.no_progress
    tm = args.time_steps

    if args.read_internalized_only is not None:
        if args.input is not None:
            ap.error("с --read-internalized-only не указывайте позиционный input")
        if args.internalized_out is not None:
            ap.error("с --read-internalized-only не используйте --internalized-out")
        t0 = time.perf_counter()
        dump_path = args.read_internalized_only
        internal_state = load_internal_representation(dump_path, progress=pg)
        internal_state.print_stat()
        dt_read = time.perf_counter() - t0
        _timing_log("read_flatten_internalized_dump", dt_read)
        t_step = time.perf_counter()
        compress_internalized(internal_state, 6)
        _timing_log("compress_internalized", time.perf_counter() - t_step)
        if tm:
            print(
                f"[ndjson-compress] всего (сумма измеренных шагов): {dt_read:.3f} s",
                file=sys.stderr,
            )
        return

    if args.input is None:
        ap.error("нужен входной NDJSON (позиционный аргумент)")
    if args.internalized_out is None:
        ap.error("укажите --internalized-out для записи дампа FlattenInternalized")

    times: dict[str, float] = {}

    if pg:
        inp_sz = args.input.stat().st_size
        _progress_log(f"старт: {args.input} ({inp_sz:,} B)")

    t0 = time.perf_counter()
    flat = read_ndjson(args.input, progress=pg)
    flat.print_stat()
    times["read_ndjson"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    store_internal_representation(flat, args.internalized_out, progress=pg)
    times["write_flatten_internalized_dump"] = time.perf_counter() - t0

    print(f"FlattenInternalized → дамп: {args.internalized_out}", file=sys.stderr)
    before = args.input.stat().st_size
    after = args.internalized_out.stat().st_size
    label = "исходный NDJSON → дамп FlattenInternalized"
    extra = ""
    if before > 0:
        pct = 100.0 * after / before
        extra = f" ({pct:.2f}% от «до»"
        if after < before:
            extra += f", сжатие ×{before / after:.2f}"
        elif after > before:
            extra += f", после больше исходного в {after / before:.2f}×"
        extra += ")"
    print(
        f"[ndjson-compress] размеры {label}: {before:,} B → {after:,} B{extra}",
        file=sys.stderr,
        flush=True,
    )

    if tm:
        print("[ndjson-compress] --- тайминги шагов ---", file=sys.stderr)
        for label, sec in times.items():
            _timing_log(label, sec)
        print(
            f"[ndjson-compress] всего (сумма измеренных шагов): {sum(times.values()):.3f} s",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
