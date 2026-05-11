#!/usr/bin/env python3
"""
Чтение NDJSON в ``FlattenInternalized``, запись и чтение текстового дампа этого представления.

**Режимы CLI**
  - ``INPUT.ndjson --internalized-out X.dump`` — ``read_ndjson`` + запись дампа во внешний файл.
  - ``--read-internalized-only X.dump`` — только чтение дампа (позиционный ``INPUT`` не указывать); опционально ``--split-by-subcolumns`` — после сжатия дампа выполнить разбиение по подстолбцам.

**NDJSON** (``read_ndjson``): каждая строка — JSON-объект; вложенные объекты и **массивы** раскрываются
по пути (индексы массива в пути — строки ``"0"``, ``"1"``, …). В дампе путь задаётся этапами: плоский
``["a","b"]`` или ``[["payload"],["x"]]`` для JSON-строки в ``payload`` и поля ``x`` внутри.
Строковый атрибут пытаются распарсить как вложенный JSON object/array. Если ``json.loads`` не удался,
ищется суффикс по шаблону ``_TRUNCATION_TOTAL_BYTES_RE``: запятая, затем текст **без запятых** до
``(truncated, total <число> bytes)`` (часто после запятой идёт ``...`` и пояснение об усечении). Совпадение
отрезают; к остатку дописывают закрытие кавычек и ``{}``/``[]`` (см. ``_close_truncated_json_document``),
при необходимости убирают запятую перед последней ``}``/``]``, снова вызывают ``json.loads``.
Если и после этого разбор не удался — вложенный JSON не извлекается (``None``).

В ``FlattenInternalized``: ``path_pool``, ``value_pool``, ``rows``.

**Дамп** (запись/чтение UTF-8 файла): заголовок JSON (``path_entries``, ``value_entries``, ``rows``),
строки путей (см. выше), строки значений ``[kind, value]`` (``kind``: одно из ``null``, ``boolean``, ``number``, ``string``, ``array``, ``object``), строки таблиц.
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
from typing import Any, Iterator
import zstandard as zstd
from collections import Counter


# Прогресс в stderr: не чаще, чем раз в _PROGRESS_MIN_INTERVAL_SEC и/или каждые _PROGRESS_ROW_INTERVAL строк
_PROGRESS_ROW_INTERVAL = 1000_000
_PROGRESS_MIN_INTERVAL_SEC = 5.0
_JSON_SEP = (",", ":")

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
class PathInternEntry:
    """Этапы пути; внутри этапа — ключи объекта и/или индексы массива (строками ``0``, ``1``, …)."""

    stages: tuple[tuple[str, ...], ...]

    def to_string(self) -> str:
        return "::".join(".".join(st) if st else "" for st in self.stages)

    @classmethod
    def from_json_line(cls, line: str) -> PathInternEntry:
        """Разобрать одну строку дампа (JSON-массив пути) в ``PathInternEntry``."""
        raw = json.loads(line)
        if not isinstance(raw, list) or not raw:
            raise ValueError(f"ожидался непустой JSON-массив пути, получено {raw!r}")
        if isinstance(raw[0], str):
            if not all(isinstance(x, str) for x in raw):
                raise ValueError(
                    f"ожидался массив строк-сегментов пути, получено {raw!r}"
                )
            return cls((tuple(raw),))
        if isinstance(raw[0], list):
            stages_acc: list[tuple[str, ...]] = []
            for part in raw:
                if not isinstance(part, list) or not all(
                    isinstance(x, str) for x in part
                ):
                    raise ValueError(
                        f"ожидался массив массивов строк-сегментов пути, получено {raw!r}"
                    )
                stages_acc.append(tuple(part))
            return cls(tuple(stages_acc))
        raise ValueError(
            f"строка пути: ожидались строки или вложенные массивы строк, получено {raw!r}"
        )


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
    path_index: dict[tuple[tuple[str, ...], ...], int] = {}
    ticker = _ProgressTicker("чтение NDJSON", enabled=progress)

    for obj in iter_ndjson_objects(path):
        out: dict[int, int] = {}

        def intern_path(stages_key: tuple[tuple[str, ...], ...]) -> int:
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

        def walk(v: Any, doc_segs: list[str], stage_prefix: tuple[tuple[str, ...], ...]) -> None:
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
                    walk(vv, doc_segs + [kk], stage_prefix)
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
                    walk(vv, doc_segs + [str(i)], stage_prefix)
            else:
                if isinstance(v, str):
                    nested = _try_parse_nested_json_document(v)
                    if nested is not None:
                        walk(nested, [], stage_prefix + (tuple(doc_segs),))
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
            st = pe.stages
            if len(st) == 1:
                line_payload: list[Any] = list(st[0])
            else:
                line_payload = [list(t) for t in st]
            f.write(
                json.dumps(line_payload, ensure_ascii=False, separators=_JSON_SEP) + "\n"
            )
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
            path_entries.append(PathInternEntry.from_json_line(line))
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


def compress_and_print_stat(name: str, raw: str | bytes, level: int) -> bytes:
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
    if comp_n > 1024 * 1024:
        print(
            f"[ndjson-compress] zstd-{level} {name}: raw {raw_n:,} B ({_pretty_byte_size(raw_n)}) → "
            f"{comp_n:,} B ({_pretty_byte_size(comp_n)}); "
            f"×{ratio_x:.2f} сжатие, compressed {pct:.1f}% от raw"
        )



def compress_internalized(state: FlattenInternalized, level: int) -> None:
    compress_and_print_stat("paths", str(state.path_pool), level)
    values = str(state.value_pool.entries)
    compress_and_print_stat("values", values, level)
    # with open("values.dmp", "wb") as f:
    #     f.write(values.encode("utf-8"))
    compress_and_print_stat("rows", str(state.rows), level)


@dataclass
class SplitBySubcolumns:

    path_pool: PathInternPool
    cross_path_value_pool: ValueInternPool
    value_pools: list[ValueInternPool]
    rows: list[dict[int, int]]

    def print_stat(self) -> None:
        print(
            "SplitBySubcolumns stat:",
            f"cross_path_value_pool entries: {len(self.cross_path_value_pool.entries)}",
        )
        per_path: list[tuple[int, int, int, str]] = []
        for path_id in range(len(self.path_pool.entries)):
            pool = self.value_pools[path_id]
            path_label = self.path_pool.entries[path_id].to_string()
            lines: list[str] = []
            for row in self.rows:
                if path_id in row:
                    lines.append(pool.entries[row[path_id]].to_string())
                else:
                    lines.append("null")
            col = "\n".join(lines) + ("\n" if lines else "")
            raw_b = len(col.encode("utf-8"))
            per_path.append((raw_b, path_id, len(pool.entries), path_label))
        # per_path.sort(key=lambda x: x[0], reverse=True)
        # print("paths by column UTF-8 size (descending):")
        # for raw_b, path_id, n_distinct, segs in per_path:
        #     label = ".".join(segs)
        #     print(f"\tpath_id={path_id}\tsize={raw_b:,} B\tdistinct={n_distinct}\t{label}")


def flatten_to_split_by_subcolumns(flat: FlattenInternalized) -> SplitBySubcolumns:
    """Переложить глобальный ``value_pool`` в отдельные пулы по путям; общие для нескольких путей значения вынести в ``cross_path_value_pool`` и убрать из пер-путевых пулов. Все пулы значений упорядочены по возрастанию ``ValueAsString.to_string()``; ``rows`` — по возрастанию лексикографического кортежа этих строк по ``path_id`` (пропуск столбца — как ``ValueAsString(None).to_string()``)."""
    n_paths = len(flat.path_pool.entries)
    value_pools = [ValueInternPool() for _ in range(n_paths)]
    per_path_intern: list[dict[ValueAsString, int]] = [dict() for _ in range(n_paths)]
    value_to_path_ids: dict[ValueAsString, set[int]] = {}

    def intern_at_path(path_id: int, vs: ValueAsString) -> int:
        d = per_path_intern[path_id]
        k = d.get(vs)
        if k is None:
            pool = value_pools[path_id]
            k = len(pool.entries)
            pool.entries.append(vs)
            d[vs] = k
        return k

    new_rows: list[dict[int, int]] = []
    for row in flat.rows:
        new_row: dict[int, int] = {}
        for path_id, global_vid in row.items():
            vs = flat.value_pool.entries[global_vid]
            value_to_path_ids.setdefault(vs, set()).add(path_id)
            new_row[path_id] = intern_at_path(path_id, vs)
        new_rows.append(new_row)

    cross_path_values = {
        vs: frozenset(pids)
        for vs, pids in value_to_path_ids.items()
        if len(pids) > 1
    }
    cross_path_value_pool = ValueInternPool()
    cross_path_value_pool.entries = sorted(
        cross_path_values.keys(),
        key=ValueAsString.to_string,
    )

    cross_set = frozenset(cross_path_value_pool.entries)
    old_snapshots = [list(p.entries) for p in value_pools]

    for path_id in range(n_paths):
        value_pools[path_id].entries = [
            vs for vs in old_snapshots[path_id] if vs not in cross_set
        ]

    per_path_old_to_new: list[dict[int, int]] = []
    for path_id in range(n_paths):
        m: dict[int, int] = {}
        ni = 0
        for oi, vs in enumerate(old_snapshots[path_id]):
            if vs not in cross_set:
                m[oi] = ni
                ni += 1
        per_path_old_to_new.append(m)

    stripped_rows: list[dict[int, int]] = []
    for row in new_rows:
        sr: dict[int, int] = {}
        for path_id, old_vid in row.items():
            vs = old_snapshots[path_id][old_vid]
            if vs in cross_set:
                continue
            sr[path_id] = per_path_old_to_new[path_id][old_vid]
        stripped_rows.append(sr)

    for path_id in range(n_paths):
        before = list(value_pools[path_id].entries)
        sorted_entries = sorted(before, key=ValueAsString.to_string)
        value_pools[path_id].entries = sorted_entries
        pos = {vs: i for i, vs in enumerate(sorted_entries)}
        old_to_new = [pos[vs] for vs in before]
        for row in stripped_rows:
            if path_id in row:
                row[path_id] = old_to_new[row[path_id]]

    _missing_cell_sort = ValueAsString(None).to_string()

    def _split_row_sort_key(row: dict[int, int]) -> tuple[str, ...]:
        return tuple(
            ValueAsString.to_string(value_pools[path_id].entries[row[path_id]])
            if path_id in row
            else _missing_cell_sort
            for path_id in range(n_paths)
        )

    stripped_rows = sorted(stripped_rows, key=_split_row_sort_key)

    return SplitBySubcolumns(
        path_pool=flat.path_pool,
        cross_path_value_pool=cross_path_value_pool,
        value_pools=value_pools,
        rows=stripped_rows,
    )

def compress_split_by_subcolumns(state: SplitBySubcolumns, level: int) -> None:
    compress_and_print_stat("paths", str(state.path_pool), level)
    compress_and_print_stat(
        "cross_path_value_pool",
        str(state.cross_path_value_pool.entries),
        level,
    )
    for path_id, vp in enumerate(state.value_pools):
        pe = state.path_pool.entries[path_id]
        path_label = pe.to_string()
        name = f"value_pool[{path_id}]"
        if path_label:
            name = f"{name} ({path_label})"
        compress_and_print_stat(name, str(vp.entries), level)
    compress_and_print_stat("rows", str(state.rows), level)


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
        "--split-by-subcolumns",
        action="store_true",
        help="с --read-internalized-only: после compress_internalized выполнить flatten_to_split_by_subcolumns и compress_split_by_subcolumns",
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

    if args.split_by_subcolumns and args.read_internalized_only is None:
        ap.error("--split-by-subcolumns только вместе с --read-internalized-only FILE")

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
        if args.split_by_subcolumns:
            t_step = time.perf_counter()
            by_subcolumns = flatten_to_split_by_subcolumns(internal_state)
            by_subcolumns.print_stat()
            _timing_log("flatten_to_split_by_subcolumns", time.perf_counter() - t_step)
            t_step = time.perf_counter()
            compress_split_by_subcolumns(by_subcolumns, 6)
            _timing_log("compress_split_by_subcolumns", time.perf_counter() - t_step)
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
