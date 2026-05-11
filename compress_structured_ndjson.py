#!/usr/bin/env python3
"""
Чтение NDJSON в ``FlattenInternalized``, запись и чтение текстового дампа этого представления.

**Режимы CLI**
  - ``INPUT.ndjson --internalized-out X.dump`` — ``read_ndjson`` + запись дампа во внешний файл.
  - ``--read-internalized-only X.dump`` — только чтение дампа (позиционный ``INPUT`` не указывать).

**NDJSON** (``read_ndjson``): каждая строка — JSON-объект; вложенные объекты раскрываются
по пути (массивы не обходятся). В ``FlattenInternalized``: ``path_pool``, ``value_pool``, ``rows_flat``.

**Дамп** (запись/чтение UTF-8 файла): заголовок JSON (``path_entries``, ``value_entries``, ``rows``),
строки путей (массив сегментов), строки значений ``[is_string, value]``, строки таблиц.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

# Прогресс в stderr: не чаще, чем раз в _PROGRESS_MIN_INTERVAL_SEC и/или каждые _PROGRESS_ROW_INTERVAL строк
_PROGRESS_ROW_INTERVAL = 1000_000
_PROGRESS_MIN_INTERVAL_SEC = 5.0
_JSON_SEP = (",", ":")


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
    """
        Обычно в атрибутах хранятся строки, но могут быть и другие типы данных.
        Может быть None, double, init, {}, []
        Также можно ограничивать глубину парсинга и в атрбуте может лежать список или объект
        Поэтому выделяем строки, как особый случай, а всё остальное храним как текстовое представление json'a
    """
    value: str
    is_string: bool

    def __init__(self, value: Any) -> None:
        if isinstance(value, str):
            object.__setattr__(self, "is_string", True)
            object.__setattr__(self, "value", value)
        else:
            object.__setattr__(self, "is_string", False)
            object.__setattr__(
                self,
                "value",
                json.dumps(value, ensure_ascii=False, separators=_JSON_SEP),
            )

    def to_string(self) -> str:
        return json.dumps(
            [self.is_string, self.value],
            ensure_ascii=False,
            separators=_JSON_SEP,
        )

    @classmethod
    def from_string(cls, line: str) -> ValueAsString:
        j = json.loads(line.strip())
        if not isinstance(j, list) or len(j) != 2:
            raise ValueError(f"ожидался JSON-массив из двух элементов, получено {j!r}")
        if not isinstance(j[0], bool):
            raise ValueError(f"первый элемент должен быть bool, получено {type(j[0])}")
        if not isinstance(j[1], str):
            raise ValueError(f"второй элемент должен быть str, получено {type(j[1])}")
        o = object.__new__(cls)
        object.__setattr__(o, "is_string", j[0])
        object.__setattr__(o, "value", j[1])
        return o

@dataclass
class ValueInternPool:
    entries: list[ValueAsString] = field(default_factory=list)


@dataclass(frozen=True)
class PathInternEntry:
    """Путь к полю: последовательность ключей объекта JSON (без индексов массива)."""

    segments: tuple[str, ...]


@dataclass
class PathInternPool:
    """Упорядоченный список ``PathInternEntry`` (индекс в каталоге путей)."""

    entries: list[PathInternEntry] = field(default_factory=list)


@dataclass
class FlattenInternalized:

    path_pool: PathInternPool
    value_pool: ValueInternPool
    rows_flat: list[dict[int, int]]

    def print_stat(self) -> None:
        print("FlattenInternalized stat:")
        print(f"\tPaths: {len(self.path_pool.entries)}")
        print(f"\tValues: {len(self.value_pool.entries)}")
        print(f"\tRows: {len(self.rows_flat)}")


def read_ndjson(path: Path, *, progress: bool = False) -> FlattenInternalized:
    rows_flat: list[dict[int, int]] = []
    path_pool = PathInternPool()
    value_pool = ValueInternPool()
    value_entry_index: dict[ValueAsString, int] = {}
    path_index: dict[tuple[str, ...], int] = {}
    ticker = _ProgressTicker("чтение NDJSON", enabled=progress)
    for obj in iter_ndjson_objects(path):
        out: dict[int, int] = {}

        def intern_path(segs: list[str]) -> int:
            t = tuple(segs)
            i = path_index.get(t)
            if i is None:
                i = len(path_pool.entries)
                path_pool.entries.append(PathInternEntry(t))
                path_index[t] = i
            return i

        def intern_value(v: Any) -> int:
            s = ValueAsString(v)
            i = value_entry_index.get(s)
            if i is None:
                i = len(value_pool.entries)
                value_pool.entries.append(s)
                value_entry_index[s] = i
            return i

        def walk(v: Any, segs: list[str]) -> None:
            if isinstance(v, dict):
                if not v:
                    if segs:
                        pid = intern_path(segs)
                        out[pid] = intern_value(v)
                    return
                for kk, vv in v.items():
                    walk(vv, segs + [kk])
            elif isinstance(v, list):
                pid = intern_path(segs)
                out[pid] = intern_value(v)
            else:
                pid = intern_path(segs)
                out[pid] = intern_value(v)

        walk(obj, [])
        rows_flat.append(dict(out))
        ticker.tick()
    ticker.done(" строк")
    return FlattenInternalized(
        path_pool=path_pool,
        value_pool=value_pool,
        rows_flat=rows_flat,
    )


def store_internal_representation(
    internalized: FlattenInternalized, path: Path, *, progress: bool = False
) -> None:
    """Записать ``FlattenInternalized`` в UTF-8 файл (заголовок + каталоги + строки)."""
    with path.open("w", encoding="utf-8", newline="\n") as f:
        pp = internalized.path_pool
        pool = internalized.value_pool
        rows = internalized.rows_flat
        header: dict[str, Any] = {
            "path_entries": len(pp.entries),
            "value_entries": len(pool.entries),
            "rows": len(rows),
        }
        f.write(json.dumps(header, ensure_ascii=False, separators=_JSON_SEP) + "\n")
        pt = _ProgressTicker("дамп FlattenInternalized: каталог путей", enabled=progress)
        for pe in pp.entries:
            f.write(
                json.dumps(list(pe.segments), ensure_ascii=False, separators=_JSON_SEP) + "\n"
            )
            pt.tick()
        pt.done(" путей")
        vt = _ProgressTicker("дамп FlattenInternalized: каталог значений", enabled=progress)
        for e in pool.entries:
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
            segs = json.loads(line)
            if not isinstance(segs, list) or not all(isinstance(x, str) for x in segs):
                raise ValueError(
                    f"ожидался JSON-массив строк-сегментов пути, получено {segs!r}"
                )
            t = tuple(segs)
            path_entries.append(PathInternEntry(t))
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

        rows_flat: list[dict[int, int]] = []
        rt = _ProgressTicker("дамп FlattenInternalized чтение: строки", enabled=progress)
        for _ in range(n_rows):
            line = f.readline()
            if not line:
                raise ValueError("неожиданный EOF в секции строк")
            d = json.loads(line)
            row = {int(k): int(v) for k, v in d.items()}
            rows_flat.append(row)
            rt.tick()
        rt.done(" строк")

    if progress:
        _progress_log("дамп FlattenInternalized: загружен в память")
    return FlattenInternalized(
        path_pool=path_pool, value_pool=pool, rows_flat=rows_flat
    )


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
