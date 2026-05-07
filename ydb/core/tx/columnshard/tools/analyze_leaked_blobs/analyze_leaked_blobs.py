#!/usr/bin/env python3
import argparse
import bisect
from collections import defaultdict
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

ISO_TS_RE = re.compile(r"(?P<iso>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)")
BLOB_ID_RE = re.compile(r"\[([0-9:]+)\]")
EXPECTED_CHANNELS_PER_TABLET = 66


def next_month(dt: date) -> date:
    if dt.month == 12:
        return date(year=dt.year + 1, month=1, day=1)
    return date(year=dt.year, month=dt.month + 1, day=1)

def date_from_ms(ms: int) -> date:
    dt = datetime.fromtimestamp(ms / 1000, timezone.utc)
    return date(year=dt.year, month=dt.month, day=dt.day)

def month_from_ms(ms: int) -> date:
    return date_from_ms(ms).replace(day=1)

def parse_iso_z(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def extract_iso_dt(line: str) -> datetime | None:
    match = ISO_TS_RE.search(line)
    if not match:
        return None
    try:
        return parse_iso_z(match.group("iso"))
    except ValueError:
        return None


def extract_int_field_optional(line: str, key: str) -> int | None:
    match = re.search(rf"{re.escape(key)}=(\d+);", line)
    if not match:
        return None
    return int(match.group(1))

def extract_int_field(line: str, key: str) -> int:
    res = extract_int_field_optional(line, key)
    if res is None:
        raise ValueError(f"required field {key} not found in line: {line}")
    return res

def extract_text_field_optional(line: str, key: str) -> str | None:
    match = re.search(rf"{re.escape(key)}=([^;]+);", line)
    if not match:
        return None
    return match.group(1)

def extract_text_field(line: str, key: str) -> str:
    res = extract_text_field_optional(line, key)
    if res is None:
        raise ValueError(f"required field {key} not found in line: {line}")
    return res


@dataclass(frozen=True, slots=True)
class BlobId:
    # YDB blob id format in logs:
    # [tablet_id:generation:step:channel:blob_size:cookie:part_id]
    tablet_id: int
    generation: int
    step: int
    channel: int
    blob_size: int
    cookie: int
    part_id: int

    @classmethod
    def parse(cls, text: str) -> "BlobId":
        parts = text.split(":")
        assert len(parts) == 7, f"expected 7 parts, got {len(parts)}: {text}"
        return cls(
            tablet_id=int(parts[0]),
            generation=int(parts[1]),
            step=int(parts[2]),
            channel=int(parts[3]),
            # In log format like [tablet:gen:step:channel:0:size:part],
            # the size is the second last element.
            cookie=int(parts[4]),
            blob_size=int(parts[5]),
            part_id=int(parts[6]),
        )


@dataclass
class HistoryRecord:
    channel: int
    from_generation: int
    group_id: int
    timestamp_ms: int


@dataclass
class TabletStats:
    missing: bool = True
    cs_blob_ids_count: int = 0
    cs_blob_ids_size: int = 0
    bs_total_blobs_count: int = 0
    bs_total_blobs_size: int = 0
    bs_do_not_keep_count: int = 0
    bs_keep_count: int = 0
    leaked_blobs_count: int = 0
    leaked_blobs_size: int = 0
    # calculated
    leaked_small_blobs_count: int = 0
    leaked_small_blobs_size: int = 0
    leaked_big_blobs_count: int = 0
    leaked_big_blobs_size: int = 0

    def merge(self, other: "TabletStats") -> None:
        self.cs_blob_ids_count += other.cs_blob_ids_count
        self.cs_blob_ids_size += other.cs_blob_ids_size
        self.bs_total_blobs_count += other.bs_total_blobs_count
        self.bs_total_blobs_size += other.bs_total_blobs_size
        self.bs_do_not_keep_count += other.bs_do_not_keep_count
        self.bs_keep_count += other.bs_keep_count
        self.leaked_blobs_count += other.leaked_blobs_count
        self.leaked_blobs_size += other.leaked_blobs_size

        self.leaked_small_blobs_count += other.leaked_small_blobs_count
        self.leaked_small_blobs_size += other.leaked_small_blobs_size
        self.leaked_big_blobs_count += other.leaked_big_blobs_count
        self.leaked_big_blobs_size += other.leaked_big_blobs_size




@dataclass
class Interval:
    tablet_id: int
    channel: int
    group_id: int
    from_generation: int
    to_generation: int # exclusive
    beginning_ms: int
    end_ms: int  # exclusive
    current_interval: bool
    blobs: list[BlobId] = field(default_factory=list)

    def has_blobs(self) -> bool:
        return len(self.blobs) > 0

    def get_blob_generation_range(self) -> tuple[int, int]:
        first = self.blobs[0].generation
        last = first
        for blob in self.blobs:
            if blob.generation < first:
                first = blob.generation
            if blob.generation > last:
                last = blob.generation
        return first, last


@dataclass
class Tablet:
    tablet_id: int
    no_localdb_blobs: bool
    now_ms: int = -1
    current_generation: int = -1
    stats: TabletStats = field(default_factory=TabletStats)
    # Hive history
    hive_chunks_count: int | None = None
    hive_chunks_parsed: set[int] = field(default_factory=set)
    hive_records_count: int | None = None
    hive_records_parsed: int = 0
    hive_duplicate_chunks_count: int = 0
    history_by_channel: list[list[HistoryRecord]] = field(default_factory=lambda: [[] for _ in range(EXPECTED_CHANNELS_PER_TABLET)])
    # Leaked blobs
    chunks_count: int = 0
    chunks_parsed: set[int] = field(default_factory=set)
    duplicate_chunks_count: int = 0
    leaked_blob_ids: set[BlobId] = field(default_factory=set)
    # Intervals
    # intervals[channel] -> list[Interval]
    intervals: list[list[Interval]] = field(default_factory=lambda: [[] for _ in range(EXPECTED_CHANNELS_PER_TABLET)])

    def validate(self) -> list[str]:
        issues: list[str] = []
        issues.extend(self._validate_blobs())
        issues.extend(self._validate_hive_history())
        return issues
    
    def finalize(self) -> None:
        if self.no_localdb_blobs:
            self.history_by_channel[0].clear()
            self.history_by_channel[1].clear()
        for _, history in enumerate(self.history_by_channel):
            history.sort(key=lambda x: x.timestamp_ms)

    def build_intervals(self) -> None:
        for channel, history in enumerate(self.history_by_channel):
            for idx, record in enumerate(history):
                end_ms = self.now_ms
                end_generation = self.current_generation + 1
                current_interval = True
                if idx + 1 < len(history):
                    end_ms = history[idx + 1].timestamp_ms
                    end_generation = history[idx + 1].from_generation
                    current_interval = False
                interval = Interval(
                    self.tablet_id,
                    channel,
                    record.group_id,
                    record.from_generation,
                    end_generation,
                    record.timestamp_ms,
                    end_ms,
                    current_interval,
                )
                self.intervals[channel].append(interval)
        
        for blobId in self.leaked_blob_ids:
            idx = bisect.bisect_right(self.intervals[blobId.channel], blobId.generation, key=lambda x: x.from_generation) - 1
            assert idx >= 0
            interval = self.intervals[blobId.channel][idx]
            assert interval.from_generation <= blobId.generation < interval.to_generation
            interval.blobs.append(blobId)
        
        # there may be many blob ids, we do not duplicate them in memory
        self.leaked_blob_ids.clear()


    def _validate_blobs(self) -> list[str]:
        if self.stats.missing:
            return [f"tablet {self.tablet_id}: leaked blobs stats are missing"]
        if self.chunks_count != len(self.chunks_parsed):
            return [f"tablet {self.tablet_id}: leaked blobs chunks_count mismatch parsed={len(self.chunks_parsed)} expected={self.chunks_count}"]
        if self.stats.leaked_blobs_count != len(self.leaked_blob_ids):
            return [f"tablet {self.tablet_id}: leaked blobs count mismatch parsed={len(self.leaked_blob_ids)} expected={self.stats.leaked_blobs_count}"]
        if self.stats.leaked_blobs_count != self.stats.leaked_small_blobs_count + self.stats.leaked_big_blobs_count:
            return [f"tablet {self.tablet_id}: leaked blobs count mismatch small={self.stats.leaked_small_blobs_count} big={self.stats.leaked_big_blobs_count} total={self.stats.leaked_blobs_count}"]
        if self.stats.leaked_blobs_size != self.stats.leaked_small_blobs_size + self.stats.leaked_big_blobs_size:
            return [f"tablet {self.tablet_id}: leaked blobs size mismatch small={self.stats.leaked_small_blobs_size} big={self.stats.leaked_big_blobs_size} total={self.stats.leaked_blobs_size}"]
        if self.no_localdb_blobs:
            for blob_id in self.leaked_blob_ids:
                if blob_id.channel <= 1:
                    return [f"tablet {self.tablet_id}: blob id {blob_id} has channel <= 1, so it is from localdb"]
        return []
    
    def _validate_hive_history(self) -> list[str]:
        if self.hive_chunks_count is None:
            return [f"tablet {self.tablet_id}: hive chunks count is missing"]
        if self.hive_chunks_count != len(self.hive_chunks_parsed):
            return [f"tablet {self.tablet_id}: hive chunks count mismatch parsed={len(self.hive_chunks_parsed)} expected={self.hive_chunks_count}"]
        if self.hive_records_count is None:
            return [f"tablet {self.tablet_id}: hive records count is missing"]
        if self.hive_records_count != self.hive_records_parsed:
            return [f"tablet {self.tablet_id}: hive records count mismatch parsed={self.hive_records_parsed} expected={self.hive_records_count}"]
        
        start_from_channel = 2 if self.no_localdb_blobs else 0
        for channel in range(start_from_channel, EXPECTED_CHANNELS_PER_TABLET):
            history = self.history_by_channel[channel]
            if not history:
                return [f"tablet {self.tablet_id}: channel {channel} has no history"]
            if history[0].from_generation != 0:
                return [f"tablet {self.tablet_id}: channel {channel} has no history from generation 0"]
            is_sorted_by_generation = all(
                history[i].from_generation <= history[i + 1].from_generation 
                for i in range(len(history) - 1)
            )
            if not is_sorted_by_generation:
                return [f"tablet {self.tablet_id}: channel {channel} history is sorted by timestamp, but not sorted by generation"]
        
        return []


    def add_hive_history_chunk(self, records_count: int, chunks_total: int, chunk_idx: int, records: list[HistoryRecord]) -> None:
        assert self.hive_records_count is None or self.hive_records_count == records_count
        self.hive_records_count = records_count

        assert self.hive_chunks_count is None or self.hive_chunks_count == chunks_total
        self.hive_chunks_count = chunks_total

        if chunk_idx in self.hive_chunks_parsed:
            self.hive_duplicate_chunks_count += 1
            return

        self.hive_chunks_parsed.add(chunk_idx)
        self.hive_records_parsed += len(records)
        for rec in records:
            self.history_by_channel[rec.channel].append(rec)

    def set_hive_snapshot(self, now_ms: int, current_generation: int) -> None:
        assert self.now_ms == -1 or self.now_ms == now_ms
        self.now_ms = now_ms
        assert self.current_generation == -1 or self.current_generation == current_generation
        self.current_generation = current_generation

    def add_blobs(self, chunks_total: int, chunk_idx: int, blob_ids: list[BlobId], small_blob_threshold: int):
        assert self.chunks_count == 0 or self.chunks_count == chunks_total
        self.chunks_count = chunks_total

        # Keep parsing and count duplicates for diagnostics.
        if chunk_idx in self.chunks_parsed:
            self.duplicate_chunks_count += 1
            return
        self.chunks_parsed.add(chunk_idx)
        for blob_id in blob_ids:
            self.leaked_blob_ids.add(blob_id)
        for blob_id in blob_ids:
            if blob_id.blob_size <= small_blob_threshold:
                self.stats.leaked_small_blobs_count += 1
                self.stats.leaked_small_blobs_size += blob_id.blob_size
            else:
                self.stats.leaked_big_blobs_count += 1
                self.stats.leaked_big_blobs_size += blob_id.blob_size


@dataclass
class Table:
    shard_count: int
    tablets: dict[int, Tablet] = field(default_factory=dict)

    def validate(self) -> list[str]:
        # Completeness checks.
        issues: list[str] = []
        if len(self.tablets) != self.shard_count:
            issues.append(f"tablets count mismatch parsed={len(self.tablets)} expected={self.shard_count}")
            return issues
        for tablet in self.tablets.values():
            issues.extend(tablet.validate())
        return issues
    
    def finalize(self) -> None:
        for tablet in self.tablets.values():
            tablet.finalize()

    def build_intervals(self) -> list[Interval]:
        for tablet in self.tablets.values():
            tablet.build_intervals()
        
        all_intervals: list[Interval] = []
        for tablet in self.tablets.values():
            intervals_by_channel = tablet.intervals
            for intervals in intervals_by_channel:
                all_intervals.extend(intervals)
        return all_intervals

    def get_stats(self) -> TabletStats:
        stats = TabletStats(missing=False)
        for tablet in self.tablets.values():
            stats.merge(tablet.stats)
        return stats


@dataclass
class AgeChartSegment:
    begin_ms: int
    end_ms: int
    blobs_count: int


def human_size(size_bytes: int) -> str:
    if size_bytes < 0:
        return "n/a"
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]
    size = float(size_bytes)
    unit_idx = 0
    while size >= 1024.0 and unit_idx < len(units) - 1:
        size /= 1024.0
        unit_idx += 1
    return f"{size:.2f}{units[unit_idx]}"


def human_count(value: int) -> str:
    return f"{value:,}".replace(",", " ")


def ms_to_iso_utc(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze leaked blobs logs")
    parser.add_argument("--log_file", required=True, help="Path to filtered.log")
    parser.add_argument("--output_folder", default=None, help="Folder for plots")
    parser.add_argument("--from", dest="from_ts", default=None, help="Ignore lines before this ISO timestamp")
    parser.add_argument("--to", dest="to_ts", default=None, help="Ignore lines after this ISO timestamp")
    parser.add_argument(
        "--small_blob_threshold",
        dest="small_blob_threshold",
        type=int,
        default=259968,
        help="Blob size threshold in bytes for small blobs (default: 259968)",
    )
    parser.add_argument("--shard_count", type=int, default=64, help="Expected tablet count (default: 64)")
    parser.add_argument(
        "--require_complete_log",
        action="store_true",
        help="Fail when data is incomplete",
    )
    parser.add_argument(
        "--no_localdb_blobs",
        action="store_true",
        help="Logs do not contain blobs from localdb, channels <= 1",
    )
    return parser.parse_args()


def is_in_time_range(line_dt: datetime | None, from_ts: datetime | None, to_ts: datetime | None) -> bool:
    if line_dt is None:
        return False
    if from_ts is not None and line_dt < from_ts:
        return False
    if to_ts is not None and line_dt > to_ts:
        return False
    return True


class Parser:
    def __init__(self, args: argparse.Namespace):
        self.Args = args
        self.LinesTotal = 0
        self.LinesSkippedByTime = 0
        self.LogPath = Path(self.Args.log_file).expanduser()
        if self.Args.output_folder:
            self.OutputFolder = Path(self.Args.output_folder).expanduser()
        else:
            self.OutputFolder = Path.cwd() / "result"
        self.OutputFolder.mkdir(parents=True, exist_ok=True)
        if not self.LogPath.exists():
            raise SystemExit(f"log_file does not exist: {self.LogPath}")
        if self.Args.shard_count <= 0:
            raise SystemExit("shard_count must be > 0")
        if self.Args.small_blob_threshold < 0:
            raise SystemExit("small_blob_threshold must be >= 0")
        self.From = parse_iso_z(self.Args.from_ts) if self.Args.from_ts else None
        self.To = parse_iso_z(self.Args.to_ts) if self.Args.to_ts else None
        if self.From and self.To and self.From > self.To:
            raise SystemExit("--from must be <= --to")

    def parse(self) -> Table:
        tablets: dict[int, Tablet] = {}
        with self.LogPath.open("r", encoding="utf-8", errors="replace") as inp:
            for raw_line in inp:
                self.LinesTotal += 1
                line = raw_line.rstrip("\n")
                line_dt = extract_iso_dt(line)
                if not is_in_time_range(line_dt, self.From, self.To):
                    self.LinesSkippedByTime += 1
                    continue

                event = extract_text_field_optional(line, "event")
                if event is None:
                    continue

                tablet_id = extract_int_field_optional(line, "tablet_id")
                if tablet_id is None:
                    continue

                tablet = tablets.setdefault(tablet_id, Tablet(tablet_id=tablet_id, no_localdb_blobs=self.Args.no_localdb_blobs))
                if event == "hive_channel_history":
                    self._parse_hive_channel_history_event(line, tablet)
                    continue
                if event == "found_leaked_blobs_stats":
                    self._parse_found_leaked_blobs_stats_event(line, tablet)
                    continue
                if event == "found_leaked_blob_ids_chunk":
                    self._parse_found_leaked_blob_ids_chunk_event(line, tablet)
                    continue

        return Table(shard_count=self.Args.shard_count, tablets=tablets)

    def _parse_hive_channel_history_event(self, line: str, tablet: Tablet) -> None:
        status = extract_text_field(line, "status")
        if status != "ok":
            return
        now_ms_text: str = extract_text_field(line, "now_ms")
        current_generation_text: str = extract_text_field(line, "current_generation")
        tablet.set_hive_snapshot(
            int(now_ms_text),
            int(current_generation_text),
        )
        records_count = extract_int_field(line, "records_count")
        chunks_total = extract_int_field(line, "chunks_total")
        chunk_idx = extract_int_field(line, "chunk_idx")
        chunk_pos = line.find("history_chunk=")
        if chunk_idx is None or chunks_total is None or chunk_pos < 0:
            return
        chunk_payload = line[chunk_pos + len("history_chunk="):].strip()
        if chunk_payload.endswith(";"):
            chunk_payload = chunk_payload[:-1]
        # history_payload format:
        # [{channel=0,from_generation=0,group_id=...,timestamp_ms=...};{...};...]
        records: list[HistoryRecord] = []
        for rec_text in re.findall(r"\{([^}]*)\}", chunk_payload):
            field_map: dict[str, str] = {}
            for item in rec_text.split(","):
                if "=" not in item:
                    continue
                k, v = item.split("=", 1)
                field_map[k.strip()] = v.strip()

            if not {"channel", "from_generation", "group_id", "timestamp_ms"}.issubset(field_map.keys()):
                continue

            rec = HistoryRecord(
                channel=int(field_map["channel"]),
                from_generation=int(field_map["from_generation"]),
                group_id=int(field_map["group_id"]),
                timestamp_ms=int(field_map["timestamp_ms"]),
            )
            records.append(rec)
        tablet.add_hive_history_chunk(records_count, chunks_total, chunk_idx, records)

    def _parse_found_leaked_blobs_stats_event(self, line: str, tablet: Tablet) -> None:
        stats = tablet.stats
        stats.missing = False
        stats.leaked_blobs_count = extract_int_field(line, "leaked_blobs_count")
        stats.leaked_blobs_size = extract_int_field(line, "leaked_blobs_size")
        stats.cs_blob_ids_count = extract_int_field(line, "cs_blob_ids_count")
        stats.cs_blob_ids_size = extract_int_field(line, "cs_blob_ids_size")
        stats.bs_total_blobs_count = extract_int_field(line, "bs_total_blobs_count")
        stats.bs_total_blobs_size = extract_int_field(line, "bs_total_blobs_size")
        stats.bs_do_not_keep_count = extract_int_field(line, "bs_do_not_keep_count")
        stats.bs_keep_count = extract_int_field(line, "bs_keep_count")

    def _parse_found_leaked_blob_ids_chunk_event(self, line: str, tablet: Tablet) -> None:
        chunks_total = extract_int_field(line, "chunks_total")
        chunk_idx = extract_int_field(line, "chunk_idx")
        if chunk_idx is None or chunks_total is None:
            return
        blob_ids_pos = line.find("blob_ids=")
        if blob_ids_pos < 0:
            return
        blob_ids_payload = line[blob_ids_pos + len("blob_ids="):]
        blob_ids_raw = BLOB_ID_RE.findall(blob_ids_payload)
        blob_ids = [BlobId.parse(raw) for raw in blob_ids_raw]
        tablet.add_blobs(chunks_total, chunk_idx, blob_ids, self.Args.small_blob_threshold)


@dataclass
class HistoryStats:
    internals_count: int = 0
    intervals_with_blobs_count: int = 0
    blobs_in_intervals: int = 0
    blobs_in_current_intervals: int = 0
    current_intervals_count: int = 0
    current_intervals_with_blobs_count: int = 0
    oldest_current_interval: int = 0
    youngest_current_interval: int = 0
    closed_intervals_count: int = 0
    closed_intervals_with_blobs_count: int = 0
    blobs_in_closed_intervals: int = 0


@dataclass
class StatsPrinter:

    def print_stats(self, parser: Parser, stats: TabletStats, history_stats: HistoryStats, issues: list[str]) -> None:
        # Output.
        print("=== Input ===")
        print(f"log_file: {parser.LogPath}")
        print(f"output_folder: {parser.OutputFolder}")
        print(f"from: {parser.From}")
        print(f"to: {parser.To}")
        print(f"no_localdb_blobs: {parser.Args.no_localdb_blobs}")
        print(f"small_blob_threshold: {human_count(parser.Args.small_blob_threshold)}")
        print(f"shard_count: {human_count(parser.Args.shard_count)}")
        print(f"require_complete_log: {parser.Args.require_complete_log}")
        print()
        print("=== Processed lines ===")
        print(f"lines_total: {human_count(parser.LinesTotal)}")
        print(f"lines_skipped_by_time: {human_count(parser.LinesSkippedByTime)}")
        print()
        print("=== Blobs Stats ===")
        print(f"leaked_blobs_count: {human_count(stats.leaked_blobs_count)}")
        print(f"leaked_blobs_size: {human_size(stats.leaked_blobs_size)}")
        print(f"leaked_small_blobs_count: {human_count(stats.leaked_small_blobs_count)}")
        print(f"leaked_small_blobs_size: {human_size(stats.leaked_small_blobs_size)}")
        print(f"leaked_big_blobs_count: {human_count(stats.leaked_big_blobs_count)}")
        print(f"leaked_big_blobs_size: {human_size(stats.leaked_big_blobs_size)}")
        print(f"cs_blobs_count: {human_count(stats.cs_blob_ids_count)}")
        print(f"cs_blobs_size: {human_size(stats.cs_blob_ids_size)}")
        print(f"bs_blobs_count: {human_count(stats.bs_total_blobs_count)}")
        print(f"bs_total_blobs_size: {human_size(stats.bs_total_blobs_size)}")
        print(f"bs_do_not_keep_count: {human_count(stats.bs_do_not_keep_count)}")
        print(f"bs_keep_count: {human_count(stats.bs_keep_count)}")
        print()

        self.print_issues(issues)

        print("=== Intervals Stats ===")
        print(f"internals_count: {human_count(history_stats.internals_count)}")
        print(f"intervals_with_blobs_count: {human_count(history_stats.intervals_with_blobs_count)}")
        print(f"blobs_in_intervals: {human_count(history_stats.blobs_in_intervals)}")
        print(f"blobs_in_current_intervals: {human_count(history_stats.blobs_in_current_intervals)}")
        print(f"current_intervals_count: {human_count(history_stats.current_intervals_count)}")
        print(f"current_intervals_with_blobs_count: {human_count(history_stats.current_intervals_with_blobs_count)}")
        print(f"oldest_current_interval: {ms_to_iso_utc(history_stats.oldest_current_interval)}")
        print(f"youngest_current_interval: {ms_to_iso_utc(history_stats.youngest_current_interval)}")
        print(f"closed_intervals_count: {human_count(history_stats.closed_intervals_count)}")
        print(f"closed_intervals_with_blobs_count: {human_count(history_stats.closed_intervals_with_blobs_count)}")
        print(f"blobs_in_closed_intervals: {human_count(history_stats.blobs_in_closed_intervals)}")
    
    def print_issues(self, issues: list[str]) -> None:
        if issues:
            print("=== Issues ===")
            for issue in issues:
                print(f"- {issue}")
            print()


@dataclass
class DataPoint:
    month: date
    blobs_count: int
    interval_count: int


@dataclass
class HistoryAnalyzer:
    intervals: list[Interval]
    folder_for_plots: Path
    stats: HistoryStats = field(default_factory=HistoryStats)

    def __post_init__(self) -> None:
        intervals_with_blobs_count = 0
        blobs_in_intervals = 0
        current_intervals_count = 0
        current_intervals_with_blobs_count = 0
        blobs_in_current_intervals = 0
        oldest_current_interval = 0
        youngest_current_interval = 0
        closed_intervals_count = 0
        closed_intervals_with_blobs_count = 0
        blobs_in_closed_intervals = 0

        for i in self.intervals:
            blobs_in_intervals += len(i.blobs)
            if i.has_blobs():
                intervals_with_blobs_count += 1
            if i.current_interval:
                current_intervals_count += 1
                blobs_in_current_intervals += len(i.blobs)
                if i.has_blobs():
                    current_intervals_with_blobs_count += 1
                if not oldest_current_interval or i.beginning_ms < oldest_current_interval:
                    oldest_current_interval = i.beginning_ms
                if not youngest_current_interval or i.beginning_ms > youngest_current_interval:
                    youngest_current_interval = i.beginning_ms
            # closed interval
            else:
                closed_intervals_count += 1
                if i.has_blobs():
                    closed_intervals_with_blobs_count += 1
                    blobs_in_closed_intervals += len(i.blobs)

        self.stats.internals_count = len(self.intervals)
        self.stats.intervals_with_blobs_count = intervals_with_blobs_count
        self.stats.blobs_in_intervals = blobs_in_intervals
        self.stats.current_intervals_count = current_intervals_count
        self.stats.current_intervals_with_blobs_count = current_intervals_with_blobs_count
        self.stats.blobs_in_current_intervals = blobs_in_current_intervals
        self.stats.oldest_current_interval = oldest_current_interval
        self.stats.youngest_current_interval = youngest_current_interval
        self.stats.closed_intervals_count = closed_intervals_count
        self.stats.closed_intervals_with_blobs_count = closed_intervals_with_blobs_count
        self.stats.blobs_in_closed_intervals = blobs_in_closed_intervals

    def draw_what_makes_sense(self) -> None:
        # when current intervals started?
        self._draw_current_interval_beginning_distribution()
        # is there a correlation between the interval generation range and its blobs generation range?
        self._draw_blobs_range_over_interval_range(current_only=False)
        self._draw_blobs_range_over_interval_range(current_only=True)
        # how wide are the intervals? (in generations)
        self._draw_intervals_by_generation_range(current_only=False)
        self._draw_intervals_by_generation_range(current_only=True)
        # how far do blobs appear from the start/end of the interval? (in generations)
        self._draw_blobs_count_by_generation_gap(current_only=True)
        self._draw_blobs_count_by_generation_gap(current_only=False)
        # many some particular groups or channels are leaking blobs?
        self._draw_blobs_count_by_group()
        self._draw_blobs_count_by_channel()
        # how many generations pass in a day?
        self._draw_generation_rate()
    
    def _draw_blobs_range_over_interval_range(self, current_only: bool) -> None:
        blobs_over_interval_ratios: list[float] = []
        for i in self.intervals:
            if current_only and not i.current_interval or not i.has_blobs():
                continue
            interval_generation_range = i.to_generation - i.from_generation
            assert interval_generation_range > 0
            first, last = i.get_blob_generation_range()
            blobs_generation_range = last - first + 1
            assert blobs_generation_range > 0
            ratio = blobs_generation_range / interval_generation_range
            blobs_over_interval_ratios.append(ratio)
        if not blobs_over_interval_ratios:
            return

        import matplotlib.pyplot as plt  # type: ignore[reportMissingImports]

        fig, ax = plt.subplots(figsize=(13, 6))
        bins_count = min(50, max(10, int(len(blobs_over_interval_ratios) ** 0.5)))
        ax.hist(
            blobs_over_interval_ratios,
            bins=bins_count,
            color="tab:red",
            alpha=0.8,
            label="intervals with blobs",
        )
        ax.set_xlabel("Blobs generation range / interval generation range")
        ax.set_ylabel("Intervals count")
        ax.set_title(
            "Distribution of Blobs/Interval Generation Range"
            + (" (Current Intervals)" if current_only else " (All Intervals)")
        )
        ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
        ax.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(
            self.folder_for_plots
            / (
                "current_blobs_range_over_interval_range.png"
                if current_only
                else "blobs_range_over_interval_range.png"
            ),
            dpi=140,
        )
        plt.close(fig)

    def _draw_blobs_count_by_group(self) -> None:
        group_to_blobs_count: dict[int, int] = defaultdict(int)
        for i in self.intervals:
            if i.has_blobs():
                group_to_blobs_count[i.group_id] += len(i.blobs)
        if not group_to_blobs_count:
            return
        sorted_groups = sorted(group_to_blobs_count.items(), key=lambda x: x[0])
        x_groups = list(range(1, len(sorted_groups) + 1))
        x_group_labels = [str(group) for group, _ in sorted_groups]
        y_blobs_count = [blobs_count for _, blobs_count in sorted_groups]

        import matplotlib.pyplot as plt  # type: ignore[reportMissingImports]

        fig, ax = plt.subplots(figsize=(13, 6))
        ax.bar(
            x_groups,
            y_blobs_count,
            color="tab:blue",
            alpha=0.8,
            label="leaked blobs count",
        )
        ax.set_xlabel("Group index")
        ax.set_ylabel("Leaked blobs count")
        ax.set_title("Leaked Blobs by Group")
        ax.set_xticks(x_groups, x_group_labels, rotation=90)
        ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
        ax.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(
            self.folder_for_plots / "blobs_count_by_group.png",
            dpi=140,
        )
        plt.close(fig)

    def _draw_blobs_count_by_channel(self) -> None:
        channel_to_blobs_count: list[int] = [0] * EXPECTED_CHANNELS_PER_TABLET
        for i in self.intervals:
            if i.has_blobs():
                channel_to_blobs_count[i.channel] += len(i.blobs)
        if not channel_to_blobs_count:
            return
        
        x_channels = list(range(EXPECTED_CHANNELS_PER_TABLET))
        y_blobs_count = channel_to_blobs_count

        import matplotlib.pyplot as plt  # type: ignore[reportMissingImports]

        fig, ax = plt.subplots(figsize=(13, 6))
        ax.bar(
            x_channels,
            y_blobs_count,
            color="tab:cyan",
            alpha=0.8,
            label="leaked blobs count",
        )
        ax.set_xlabel("Channel")
        ax.set_ylabel("Leaked blobs count")
        ax.set_title("Leaked Blobs by Channel")
        ax.set_xticks(x_channels)
        ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
        ax.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(
            self.folder_for_plots / "blobs_count_by_channel.png",
            dpi=140,
        )
        plt.close(fig)


    def _draw_intervals_by_generation_range(self, current_only: bool) -> None:
        generation_range_to_intervals_count: dict[int, int] = defaultdict(int)
        for i in self.intervals:
            if current_only and not i.current_interval:
                continue
            generation_range = i.to_generation - i.from_generation
            assert generation_range > 0
            generation_range_to_intervals_count[generation_range] += 1
        if not generation_range_to_intervals_count:
            return
        sorted_generation_ranges = sorted(generation_range_to_intervals_count.items(), key=lambda x: x[0])
        x_generation_ranges = [generation_range for generation_range, _ in sorted_generation_ranges]
        y_intervals_running: list[int] = []
        intervals_running = 0
        for _, count in sorted_generation_ranges:
            intervals_running += count
            y_intervals_running.append(intervals_running)

        import matplotlib.pyplot as plt  # type: ignore[reportMissingImports]

        fig, ax = plt.subplots(figsize=(13, 6))
        ax.plot(
            x_generation_ranges,
            y_intervals_running,
            color="tab:orange",
            linewidth=2,
            marker="o",
            markersize=3,
            label="running intervals count by interval generation range",
        )
        # Long-tail ranges hide the head on linear scale.
        # Log X keeps tiny ranges visible while preserving full domain.
        ax.set_xscale("log")
        ax.set_xlabel("Generation range (to_generation - from_generation), log scale")
        ax.set_ylabel("Intervals count")
        ax.set_title(
            "Intervals by Generation Range"
            + (" (Current Intervals)" if current_only else " (All Intervals)")
        )
        ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.5)
        ax.legend(loc="upper left")
        fig.tight_layout()
        fig.savefig(
            self.folder_for_plots
            / (
                "current_intervals_by_generation_range.png"
                if current_only
                else "intervals_by_generation_range.png"
            ),
            dpi=140,
        )
        plt.close(fig)

    def _draw_generation_rate(self) -> None:
        gen_steps_per_day: list[float] = []
        for i in self.intervals:
            time_span_days: float = (i.end_ms - i.beginning_ms) / (24 * 60 * 60 * 1000)
            if not i.has_blobs() or time_span_days < 1:
                continue
            generation_span = i.to_generation - i.from_generation
            assert generation_span > 0
            generation_steps_per_day: float = generation_span / time_span_days
            gen_steps_per_day.append(generation_steps_per_day)
        
        if not gen_steps_per_day:
            return
        gen_steps_per_day.sort()
        avg_rate = sum(gen_steps_per_day) / len(gen_steps_per_day)

        # Easy to tweak for experiments.
        BUCKETS_COUNT = min(10, len(gen_steps_per_day))

        bucket_count = min(BUCKETS_COUNT, len(gen_steps_per_day))
        bucket_avg_rates: list[float] = []
        bucket_intervals_counts: list[int] = []
        bucket_labels: list[str] = []
        n = len(gen_steps_per_day)
        for idx in range(bucket_count):
            begin = idx * n // bucket_count
            end = (idx + 1) * n // bucket_count
            bucket = gen_steps_per_day[begin:end]
            assert bucket

            bucket_min = bucket[0]
            bucket_max = bucket[-1]
            bucket_avg = sum(bucket) / len(bucket)

            bucket_avg_rates.append(bucket_avg)
            bucket_intervals_counts.append(len(bucket))
            bucket_labels.append(f"{bucket_min:.2f}..{bucket_max:.2f}")

        import matplotlib.pyplot as plt  # type: ignore[reportMissingImports]

        fig, ax = plt.subplots(figsize=(14, 6))
        x = list(range(1, bucket_count + 1))
        ax.plot(
            x,
            bucket_intervals_counts,
            color="tab:purple",
            linewidth=2,
            marker="o",
            markersize=4,
            label="intervals count",
        )
        ax.set_xlabel("Quantile bucket")
        ax.set_ylabel("Intervals count")
        ax.set_title(f"Generation Steps per Day (Quantile Buckets), mean rate: {avg_rate:.2f}")
        ax.set_xticks(x, bucket_labels, rotation=45, ha="right")
        ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
        ax.legend(loc="upper left")
        fig.tight_layout()
        fig.savefig(
            self.folder_for_plots / "generation_rate.png",
            dpi=140,
        )
        plt.close(fig)

    def _draw_blobs_count_by_generation_gap(self, current_only: bool) -> None:
        # (generation gap from the start of the interval, blobs_count)
        gap_from_start_to_blobs_count: dict[int, int] = defaultdict(int)
        gap_from_end_to_blobs_count: dict[int, int] = defaultdict(int)
        for i in self.intervals:
            if current_only and not i.current_interval:
                continue
            for blob in i.blobs:
                gen_gap_from_start = blob.generation - i.from_generation
                gen_gap_from_end = i.to_generation - blob.generation - 1
                assert gen_gap_from_start >= 0 and gen_gap_from_end >= 0
                gap_from_start_to_blobs_count[gen_gap_from_start] += 1
                gap_from_end_to_blobs_count[gen_gap_from_end] += 1
        
        if not gap_from_start_to_blobs_count:
            return
        
        sorted_start_gaps = sorted(gap_from_start_to_blobs_count.items(), key=lambda x: x[0])
        sorted_end_gaps = sorted(gap_from_end_to_blobs_count.items(), key=lambda x: x[0])
        x_start_gaps = [gap for gap, _ in sorted_start_gaps]
        x_end_gaps = [gap for gap, _ in sorted_end_gaps]
        blobs_count_running = 0
        y_start_blobs_count_running = []
        for _, blobs_count in sorted_start_gaps:
            blobs_count_running += blobs_count
            y_start_blobs_count_running.append(blobs_count_running)
        blobs_count_running = 0
        y_end_blobs_count_running = []
        for _, blobs_count in sorted_end_gaps:
            blobs_count_running += blobs_count
            y_end_blobs_count_running.append(blobs_count_running)

        import matplotlib.pyplot as plt  # type: ignore[reportMissingImports]

        fig, ax = plt.subplots(figsize=(13, 6))
        ax.plot(
            x_start_gaps,
            y_start_blobs_count_running,
            color="tab:green",
            linewidth=2,
            marker="o",
            markersize=3,
            label="running blobs count from start",
        )
        ax.plot(
            x_end_gaps,
            y_end_blobs_count_running,
            color="tab:blue",
            linewidth=2,
            marker="o",
            markersize=3,
            label="running blobs count from end",
        )
        ax.set_xlabel("Distance to start/end (generation) of interval")
        ax.set_ylabel("Running blobs count")
        ax.set_title(f"Running Blobs Count by Distance to Start/End of Interval{' (only current intervals)' if current_only else ''}")
        ax.set_xscale("log")
        ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.5)
        ax.legend(loc="upper left")
        fig.tight_layout()
        fig.savefig(
            self.folder_for_plots / f"blobs_count_by_generation_gap_{'current' if current_only else 'all'}.png",
            dpi=140,
        )
        plt.close(fig)

    def _draw_current_interval_beginning_distribution(self) -> None:
        # (beginning_ms, blobs_count)
        current_intervals: list[tuple[int, int]] = []
        for i in self.intervals:
            if i.current_interval:
                current_intervals.append((i.beginning_ms, len(i.blobs)))
        if not current_intervals:
            return

        current_intervals.sort(key=lambda x: x[0])
        oldest = month_from_ms(current_intervals[0][0])
        youngest = month_from_ms(current_intervals[-1][0])
        
        months: list[DataPoint] = []
        cur_month = oldest
        last_month = youngest
        while cur_month <= last_month:
            months.append(DataPoint(month=cur_month, blobs_count=0, interval_count=0))
            cur_month = next_month(cur_month)

        idx = 0
        for beginning_ms, blobs_count in current_intervals:
            month = month_from_ms(beginning_ms)
            while months[idx].month < month:
                idx += 1
                assert idx < len(months)
            months[idx].blobs_count += blobs_count
            months[idx].interval_count += 1

        import matplotlib.dates as mdates  # type: ignore[reportMissingImports]
        import matplotlib.pyplot as plt  # type: ignore[reportMissingImports]

        x = [m.month for m in months]
        interval_counts = [m.interval_count for m in months]
        blobs_counts = [m.blobs_count for m in months]

        fig, ax_left = plt.subplots(figsize=(13, 6))
        ax_right = ax_left.twinx()

        ax_left.bar(x, interval_counts, width=25, color="tab:blue", alpha=0.6, label="intervals count") # type: ignore[reportArgumentType]
        ax_left.set_xlabel("Month")
        ax_left.set_ylabel("intervals count", color="tab:blue")
        ax_left.tick_params(axis="y", labelcolor="tab:blue")

        ax_right.plot(x, blobs_counts, color="tab:red", linewidth=2, marker="o", markersize=3, label="blobs count") # type: ignore[reportArgumentType]
        ax_right.set_ylabel("blobs count", color="tab:red")
        ax_right.tick_params(axis="y", labelcolor="tab:red")

        ax_left.set_title("Current Intervals Beginning by Month")
        ax_left.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        fig.autofmt_xdate()
        ax_left.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
        fig.tight_layout()
        fig.savefig(
            self.folder_for_plots / "current_interval_beginning_distribution.png",
            dpi=140
        )
        plt.close(fig)


def main() -> None:
    args = parse_args()
    parser = Parser(args)
    printer = StatsPrinter()
    table = parser.parse()

    table.finalize()
    issues = table.validate()
    if args.require_complete_log and issues:
        printer.print_issues(issues)
        print("ERROR: log is incomplete and --require_complete_log is enabled", file=sys.stderr)
        raise SystemExit(1)

    intervals = table.build_intervals()
    history_analyzer = HistoryAnalyzer(intervals, folder_for_plots=parser.OutputFolder)
    printer.print_stats(parser, table.get_stats(), history_analyzer.stats, issues)

    history_analyzer.draw_what_makes_sense()


if __name__ == "__main__":
    main()
