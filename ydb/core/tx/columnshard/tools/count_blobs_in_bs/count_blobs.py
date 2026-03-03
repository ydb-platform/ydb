import argparse
import re
from pathlib import Path
import sys
import time
from dataclasses import dataclass, field
from collections.abc import Iterable
from enum import Enum, auto

LOGO_BLOB_ID_RE = re.compile(r"\[(\d+):(\d+):(\d+):(\d+):(\d+):(\d+):(\d+)\]")
LOCAL_VECTOR_RE = re.compile(r"\blocal:\s+([01](?:\s+[01])*)\b")

# BlobStorage huge/inplaced thresholds by media (runtime values, bytes).
MIN_HUGE_RECORD_IN_BYTES_BY_MEDIA = {
    "nvme": 64 * 1024 + 1,
    "ssd": 64 * 1024 + 1,
    "hdd": 512 * 1024 + 1,
    "calculated_by_alex_rutkovsky": 259968 // 4 + 1
}
DEFAULT_BS_MEDIA = "calculated_by_alex_rutkovsky"


class Erasure(Enum):
    BLOCK_4_2 = auto()
    MIRROR_3_DC = auto()


ERASURE_BY_NAME = {
    "block-4-2": Erasure.BLOCK_4_2,
    "mirror-3-dc": Erasure.MIRROR_3_DC,
}
DEFAULT_ERASURE_NAME = "block-4-2"


DEFAULT_ERASURE = Erasure.BLOCK_4_2
DEFAULT_BLOB_HEADER_SIZE_BYTES = 8


@dataclass
class Blob:
    total: int = 0
    with_local: int = 0
    local_union: int = 0
    not_keep: int = 0
    inplaced: bool = False

    def keep(self) -> bool:
        return self.not_keep == 0
    
    def keep_and_local(self) -> bool:
        return self.not_keep == 0 and self.with_local > 0

    def keep_and_full_local(self, erasure: Erasure) -> bool:
        match erasure:
            case Erasure.BLOCK_4_2:
                return self.not_keep == 0 and self.local_union == 0b111111
            case Erasure.MIRROR_3_DC:
                return self.not_keep == 0 and self.local_union == 0b111
        raise ValueError(f"Unknown erasure: {erasure}")

    def add(self, record: "LBRecord") -> None:
        self.total += 1
        if record.local:
            self.with_local += 1
        self.local_union |= record.local
        if record.not_keep:
            self.not_keep += 1
        self.inplaced = record.inplaced


@dataclass
class Channel:
    erasure: Erasure
    _blobs: dict[str, Blob] = field(default_factory=dict)
    total: int = 0
    with_local: int = 0
    fresh: int = 0
    not_keep: int = 0
    # like the NumItemsInplaced/Huge counter: with local and not fresh
    num_items_inplaced: int = 0
    num_items_huge: int = 0
    inplaced_size: int = 0
    huge_size: int = 0

    def blobs(self) -> Iterable[Blob]:
        return self._blobs.values()

    def blobs_keep(self) -> Iterable[Blob]:
        for blob in self._blobs.values():
            if blob.keep():
                yield blob
    
    def blobs_keep_and_local(self) -> Iterable[Blob]:
        for blob in self._blobs.values():
            if blob.keep_and_local():
                yield blob
    
    def blobs_keep_and_full_local(self) -> Iterable[Blob]:
        for blob in self._blobs.values():
            if blob.keep_and_full_local(self.erasure):
                yield blob

    def add(self, record: "LBRecord") -> None:
        self.total += 1
        if record.local:
            self.with_local += 1
        if record.fresh:
            self.fresh += 1
        if record.not_keep:
            self.not_keep += 1
        if record.local and not record.fresh and record.inplaced:
            self.num_items_inplaced += 1
            self.inplaced_size += record.part_size
        if record.local and not record.fresh and not record.inplaced:
            self.num_items_huge += 1
            self.huge_size += record.part_size
        
        blob = self._blobs.get(record.blob_id)
        if blob is None:
            blob = Blob()
            self._blobs[record.blob_id] = blob
        blob.add(record)

    def print_blob_group(
        self,
        logger: "Logger",
        blobs_name: str,
        blobs_enumerator: Iterable[Blob],
        total_channel_blobs: int,
        total_blobs: int,
        total_channel_records: int,
        total_records: int,
    ) -> None:
        emit = logger.emit
        right = logger.shift_right
        left = logger.shift_left

        blobs = 0
        small_blobs = 0
        huge_blobs = 0
        records = 0
        small_records = 0
        huge_records = 0
        for blob in blobs_enumerator:
            blobs += 1
            records += blob.total
            if blob.inplaced:
                small_blobs += 1
                small_records += blob.total
            else:
                huge_blobs += 1
                huge_records += blob.total

        emit(f"{blobs_name} blobs:", str(blobs), pct(blobs, total_channel_blobs), pct(blobs, total_blobs))
        right()
        emit("records:", str(records), pct(records, total_channel_records), pct(records, total_records))
        emit("records per blob:", f"{(records / blobs):.1f}" if blobs else "0.0")
        emit("small blobs:", str(small_blobs), pct(small_blobs, blobs), pct(small_blobs, total_blobs))
        right()
        emit("records:", str(small_records), pct(small_records, records), pct(small_records, total_records))
        emit("records per blob:", f"{(small_records / small_blobs):.1f}" if small_blobs else "0.0")
        left()
        emit("huge blobs:", str(huge_blobs), pct(huge_blobs, blobs), pct(huge_blobs, total_blobs))
        right()
        emit("records:", str(huge_records), pct(huge_records, records), pct(huge_records, total_records))
        emit("records per blob:", f"{(huge_records / huge_blobs):.1f}" if huge_blobs else "0.0")
        left()
        left()

    def print(
        self,
        logger: "Logger",
        label: str,
        total_blobs: int,
        total_logoblobs_records: int,
        total_records: int,
    ) -> None:
        emit = logger.emit
        right = logger.shift_right
        left = logger.shift_left

        if self.total == 0:
            emit(f"{label}:", "0")
            return

        emit(f"{label}:", str(self.total), pct(self.total, total_logoblobs_records), pct(self.total, total_records))
        right()
        emit("not keep:", str(self.not_keep), pct(self.not_keep, self.total), pct(self.not_keep, total_records))
        emit("with local:", str(self.with_local), pct(self.with_local, self.total), pct(self.with_local, total_records))
        emit("fresh:", str(self.fresh), pct(self.fresh, self.total), pct(self.fresh, total_records))
        emit("inplaced:", str(self.num_items_inplaced), pct(self.num_items_inplaced, self.total), pct(self.num_items_inplaced, total_records))
        emit("huge:", str(self.num_items_huge), pct(self.num_items_huge, self.total), pct(self.num_items_huge, total_records))
        emit("inplaced size:", human_size(self.inplaced_size))
        emit("huge size:", human_size(self.huge_size))

        total_channel_blobs = len(self._blobs)
        self.print_blob_group(logger, "all", self.blobs(), total_channel_blobs, total_blobs, self.total, total_records)
        self.print_blob_group(logger, "keep", self.blobs_keep(), total_channel_blobs, total_blobs, self.total, total_records)
        self.print_blob_group(logger, "keep and local", self.blobs_keep_and_local(), total_channel_blobs, total_blobs, self.total, total_records)
        self.print_blob_group(logger, "keep and full local", self.blobs_keep_and_full_local(), total_channel_blobs, total_blobs, self.total, total_records)
        left()

@dataclass
class LBRecord:
    not_keep: bool
    local: int
    inplaced: bool
    blob_size: int
    part_size: int
    blob_id: str
    channel: int
    fresh: bool

    @classmethod
    def from_logoblob_record(
        cls,
        logoblob_record: str,
        erasure: Erasure,
        media: str = DEFAULT_BS_MEDIA,
    ) -> "LBRecord":
        table_parts = logoblob_record.split(maxsplit=1)
        table_type = table_parts[0] if table_parts else ""
        if not table_type:
            raise ValueError("failed to parse table_type from logoblob record")

        match = LOGO_BLOB_ID_RE.search(logoblob_record)
        if not match:
            raise ValueError("failed to parse TLogoBlobID from logoblob record")
        blob_id = match.group(0)
        _tablet_id, _generation, _step, channel_raw, _cookie, blob_size_raw, _part_id = match.groups()
        channel = int(channel_raw)
        blob_size = int(blob_size_raw)
        part_size = max_part_size(blob_size, erasure)
        inplaced = is_small(
            blob_size,
            media=media,
            erasure=erasure,
        )

        local = 0
        local_match = LOCAL_VECTOR_RE.search(logoblob_record)
        if local_match:
            for idx, bit in enumerate(local_match.group(1).split()):
                if bit == "1":
                    local |= 1 << idx

        not_keep = "DoNotKeep" in logoblob_record
        fresh = table_type in ("FCur", "FDreg", "FOld")

        return cls(
            not_keep=not_keep,
            local=local,
            inplaced=inplaced,
            blob_size=blob_size,
            part_size=part_size,
            blob_id=blob_id,
            channel=channel,
            fresh=fresh,
        )


@dataclass
class Report:
    erasure: Erasure = DEFAULT_ERASURE
    media: str = DEFAULT_BS_MEDIA
    channels: list[Channel] = field(default_factory=list)
    blocks_total: int = 0
    barriers_total: int = 0
    logoblobs_total: int = 0
    non_data_lines_skipped: int = 0

    def __post_init__(self) -> None:
        if not self.channels:
            self.channels = [Channel(self.erasure), Channel(self.erasure), Channel(self.erasure)]

    def add(self, line: str) -> None:
        parts = line.split(maxsplit=3)
        if len(parts) < 4:
            self.non_data_lines_skipped += 1
            return
        db_kind = parts[2]

        # From blobsan:
        # A = LOGOBLOBS, B = BLOCKS, C = BARRIERS
        if db_kind == "B":
            self.blocks_total += 1
            return
        if db_kind == "C":
            self.barriers_total += 1
            return
        if db_kind != "A":
            self.non_data_lines_skipped += 1
            return

        # A - LOGOBLOBS records
        logoblob_record = parts[3]
        try:
            record = LBRecord.from_logoblob_record(
                logoblob_record,
                erasure=self.erasure,
                media=self.media,
            )
        except ValueError:
            self.non_data_lines_skipped += 1
            return
        self.logoblobs_total += 1
        self.channels[min(record.channel, 2)].add(record)

    def print(self) -> None:
        if self.non_data_lines_skipped:
            print(f"note: skipped non-data lines: {self.non_data_lines_skipped}", file=sys.stderr)

        total_records = self.blocks_total + self.barriers_total + self.logoblobs_total
        with_local = sum(ch.with_local for ch in self.channels)
        fresh = sum(ch.fresh for ch in self.channels)
        not_keep = sum(ch.not_keep for ch in self.channels)
        num_items_inplaced = sum(ch.num_items_inplaced for ch in self.channels)
        num_items_huge = sum(ch.num_items_huge for ch in self.channels)
        inplaced_size = sum(ch.inplaced_size for ch in self.channels)
        huge_size = sum(ch.huge_size for ch in self.channels)

        log = Logger()
        emit = log.emit
        right = log.shift_right
        left = log.shift_left

        emit("total records:", str(total_records))
        right()
        emit("blocks:", str(self.blocks_total), pct(self.blocks_total, total_records))
        emit("barriers:", str(self.barriers_total), pct(self.barriers_total, total_records))
        emit("logoblobs:", str(self.logoblobs_total), pct(self.logoblobs_total, total_records))
        right()
        emit("with local:", str(with_local), pct(with_local, self.logoblobs_total), pct(with_local, total_records))
        emit("fresh:", str(fresh), pct(fresh, self.logoblobs_total), pct(fresh, total_records))
        emit("not keep:", str(not_keep), pct(not_keep, self.logoblobs_total), pct(not_keep, total_records))
        emit("inplaced:", str(num_items_inplaced), pct(num_items_inplaced, self.logoblobs_total), pct(num_items_inplaced, total_records))
        emit("huge:", str(num_items_huge), pct(num_items_huge, self.logoblobs_total), pct(num_items_huge, total_records))
        emit("inplaced size:", human_size(inplaced_size), pct(inplaced_size, inplaced_size + huge_size))
        emit("huge size:", human_size(huge_size), pct(huge_size, inplaced_size + huge_size))

        total_blobs = sum(len(ch._blobs) for ch in self.channels)
        for idx, channel_key in enumerate(("channel 0", "channel 1", "other channels")):
            self.channels[idx].print(
                logger=log,
                label=channel_key,
                total_blobs=total_blobs,
                total_logoblobs_records=self.logoblobs_total,
                total_records=total_records,
            )
        left()
        left()
        log.flush()


def pct(part: int, whole: int) -> str:
    if whole == 0:
        return "0.0%"
    return f"{(part * 100.0) / whole:.1f}%"


def human_size(size_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size_bytes)
    unit_idx = 0
    while unit_idx + 1 < len(units) and value >= 1024.0:
        value /= 1024.0
        unit_idx += 1
    return f"{value:.1f} {units[unit_idx]}"


def max_part_size(blob_size: int, erasure: Erasure) -> int:
    match erasure:
        case Erasure.BLOCK_4_2:
            # Approximate one 4:2 part payload size from full logical blob size.
            return (blob_size + 3) // 4
        case Erasure.MIRROR_3_DC:
            # For mirror3dc, MaxPartSize is full blob size.
            return blob_size
        case _:
            raise ValueError(f"Unknown erasure: {erasure}")


def is_small(
    blob_size: int,
    media: str = DEFAULT_BS_MEDIA,
    erasure: Erasure = DEFAULT_ERASURE,
    header_size_bytes: int = DEFAULT_BLOB_HEADER_SIZE_BYTES,
) -> bool:
    threshold = MIN_HUGE_RECORD_IN_BYTES_BY_MEDIA[media]
    return max_part_size(blob_size, erasure) + header_size_bytes < threshold


def format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


class Progress:
    def __init__(self, total_bytes: int) -> None:
        self._total_bytes = total_bytes
        self._processed_bytes = 0
        self._start_ts = time.monotonic()
        self._last_progress_ts = self._start_ts

    def add_processed_bytes(self, size: int) -> None:
        self._processed_bytes += size

    def print_progress(self) -> None:
        now = time.monotonic()
        if now - self._last_progress_ts < 1.0:
            return

        approx_percent = (
            self._processed_bytes * 100.0 / self._total_bytes
            if self._total_bytes
            else 100.0
        )
        elapsed = max(now - self._start_ts, 1e-9)
        bytes_per_sec = self._processed_bytes / elapsed
        remaining_bytes = max(self._total_bytes - self._processed_bytes, 0)
        eta_seconds = (remaining_bytes / bytes_per_sec) if bytes_per_sec > 0 else 0.0
        print(
            f"\rprogress: {approx_percent:.2f}% eta: {format_duration(eta_seconds)}",
            end="",
            file=sys.stderr,
            flush=True,
        )
        self._last_progress_ts = now

    def print_completed(self) -> None:
        print("\rprogress: 100.00% eta: 00:00", file=sys.stderr)


class Logger:
    def __init__(self) -> None:
        self._current_gap = 0
        self._lines: list[tuple[int, str, str, tuple[str, ...]]] = []

    def __enter__(self) -> "Logger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.flush()

    def shift_right(self) -> None:
        self._current_gap += 2

    def shift_left(self) -> None:
        self._current_gap = max(0, self._current_gap - 2)

    def emit(self, label: str, value: str, *extras: str) -> None:
        self._lines.append((self._current_gap, label, value, tuple(extras)))

    def flush(self) -> None:
        widths: dict[int, int] = {}
        for gap, label, _value, _extras in self._lines:
            widths[gap] = max(widths.get(gap, 0), len(label))

        for gap, label, value, extras in self._lines:
            tail = " ".join(x for x in extras if x)
            print(f"{' ' * gap}{label:<{widths[gap]}}  {value}{(' ' + tail) if tail else ''}")
        self._lines.clear()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calculates blob counts in a snapshot retrieved by dstool from a blobstorage, and parsed by blobsan")
    parser.add_argument("input_path", nargs="?", default="my-snapshot.txt", help="Path to a snapshot file. The file must be retrieved by dstool and parsed by blobsan.")
    parser.add_argument("--disk-type", choices=MIN_HUGE_RECORD_IN_BYTES_BY_MEDIA.keys(), default=DEFAULT_BS_MEDIA)
    parser.add_argument("--erasure", choices=ERASURE_BY_NAME.keys(), default=DEFAULT_ERASURE_NAME)
    args = parser.parse_args()
    args.erasure = ERASURE_BY_NAME[args.erasure]
    return args


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_path)
    total_bytes = input_path.stat().st_size

    report = Report(
        erasure=args.erasure,
        media=args.disk_type,
    )
    progress = Progress(total_bytes)

    with input_path.open("rb") as f:
        for _line_no, raw_line in enumerate(f, start=1):
            progress.add_processed_bytes(len(raw_line))
            progress.print_progress()

            line = raw_line.decode("utf-8", errors="replace")
            report.add(line)

    progress.print_completed()
    report.print()


if __name__ == "__main__":
    main()
