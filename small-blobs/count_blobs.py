import argparse
import re
from pathlib import Path
import sys
import time


LOGO_BLOB_ID_RE = re.compile(r"\[(\d+):(\d+):(\d+):(\d+):(\d+):(\d+):(\d+)\]")
DATA_PREFIX_RE = re.compile(r"^\d+\s+\d+\s+[ABC]\b")
LOCAL_ALL_ZERO_MARKER = "local: 0 0 0 0 0 0 "

# BlobStorage huge/inplaced thresholds by media (runtime values, bytes).
MIN_HUGE_BLOB_IN_BYTES_BY_MEDIA = {
    "nvme": 64 * 1024 + 1,
    "ssd": 64 * 1024 + 1,
    "hdd": 512 * 1024 + 1,
}
DEFAULT_BS_MEDIA = "ssd"
ERASURE_BLOCK_4_2 = "block-4-2"
ERASURE_MIRROR_3_DC = "mirror-3-dc"
DEFAULT_ERASURE = ERASURE_BLOCK_4_2
DEFAULT_BLOB_HEADER_SIZE_BYTES = 8


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


def max_part_size(blob_size: int, erasure: str) -> int:
    if erasure == ERASURE_BLOCK_4_2:
        # Approximate one 4:2 part payload size from full logical blob size.
        return (blob_size + 3) // 4
    if erasure == ERASURE_MIRROR_3_DC:
        # For mirror3dc, MaxPartSize is full blob size.
        return blob_size
    raise ValueError(f"Unknown erasure: {erasure}")


def estimate_blob_size(blob_size: int, erasure: str) -> int:
    if erasure == ERASURE_BLOCK_4_2:
        # Lower-bound physical payload estimate for one logical blob in 4:2:
        # 6 parts, each roughly ceil(blob_size / 4).
        return 6 * max_part_size(blob_size, erasure)
    if erasure == ERASURE_MIRROR_3_DC:
        # Lower-bound physical payload estimate for one logical blob in mirror3dc:
        # 3 full copies.
        return 3 * blob_size
    raise ValueError(f"Unknown erasure: {erasure}")


def min_records_per_blob(erasure: str) -> int:
    if erasure == ERASURE_BLOCK_4_2:
        return 6
    if erasure == ERASURE_MIRROR_3_DC:
        return 3
    raise ValueError(f"Unknown erasure: {erasure}")


def is_small(
    blob_size: int,
    media: str = DEFAULT_BS_MEDIA,
    erasure: str = DEFAULT_ERASURE,
    header_size_bytes: int = DEFAULT_BLOB_HEADER_SIZE_BYTES,
    min_huge_blob_in_bytes: int | None = None,
) -> bool:
    threshold = min_huge_blob_in_bytes
    if threshold is None:
        threshold = MIN_HUGE_BLOB_IN_BYTES_BY_MEDIA[media]
    return max_part_size(blob_size, erasure) + header_size_bytes < threshold


def fail(line_no: int, line: str, reason: str) -> None:
    print(f"ERROR: line {line_no}: {reason}", file=sys.stderr)
    print(f"ROW: {line.rstrip()}", file=sys.stderr)
    raise ValueError(f"Unexpected input at line {line_no}")


def format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def new_channel_stats() -> dict[str, int | set[str]]:
    return {
        "records": 0,
        "with_local_not_fresh_records": 0,
        "estimated_size": 0,
        "not_keep": 0,
        "size": 0,
        "small_estimated_size": 0,
        "small_size": 0,
        "small_records": 0,
        "blobs": set(),
        "small_blobs": set(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse blobsan text output and calculate counters")
    parser.add_argument("input_path", nargs="?", default="group-dump.txt", help="Path to blobsan text dump")
    parser.add_argument("--disk-type", choices=["ssd", "nvme", "hdd"], default=DEFAULT_BS_MEDIA)
    parser.add_argument("--erasure", choices=[ERASURE_BLOCK_4_2, ERASURE_MIRROR_3_DC], default=DEFAULT_ERASURE)
    parser.add_argument(
        "--min-huge-blob-in-bytes",
        type=int,
        default=None,
        help="Override huge blob threshold in bytes (if omitted, threshold is derived from --disk-type)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_path)
    total_bytes = input_path.stat().st_size

    blocks_total = 0
    barriers_total = 0
    logoblobs_total = 0
    logoblobs_with_local_total = 0
    logoblobs_fresh_total = 0
    logoblobs_with_local_not_fresh_total = 0

    channels = {
        "channel 0": new_channel_stats(),
        "channel 1": new_channel_stats(),
        "other channels": new_channel_stats(),
    }
    non_data_lines_skipped = 0

    processed_bytes = 0
    start_ts = time.monotonic()
    last_progress_ts = time.monotonic()

    with input_path.open("rb") as f:
        for line_no, raw_line in enumerate(f, start=1):
            processed_bytes += len(raw_line)
            now = time.monotonic()
            if now - last_progress_ts >= 1.0:
                approx_percent = (processed_bytes * 100.0 / total_bytes) if total_bytes else 100.0
                elapsed = max(now - start_ts, 1e-9)
                bytes_per_sec = processed_bytes / elapsed
                remaining_bytes = max(total_bytes - processed_bytes, 0)
                eta_seconds = (remaining_bytes / bytes_per_sec) if bytes_per_sec > 0 else 0.0
                print(
                    f"\rprogress: {approx_percent:.2f}% eta: {format_duration(eta_seconds)}",
                    end="",
                    file=sys.stderr,
                    flush=True,
                )
                last_progress_ts = now

            line = raw_line.decode("utf-8", errors="replace")
            if not line.strip():
                continue
            if not DATA_PREFIX_RE.match(line):
                # blobsan may emit non-record lines (for example, summary/diagnostic output)
                # into the same file when stderr is merged with stdout.
                non_data_lines_skipped += 1
                continue
            parts = line.split(maxsplit=3)
            if len(parts) < 4:
                fail(line_no, line, "expected at least 4 prefix fields")
            db_kind = parts[2]
            table_type = parts[3].split(maxsplit=1)[0]

            # From blobsan:
            # A = LOGOBLOBS, B = BLOCKS, C = BARRIERS
            if db_kind == "B":
                blocks_total += 1
                continue
            if db_kind == "C":
                barriers_total += 1
                continue
            if db_kind != "A":
                fail(line_no, line, f"unknown db kind '{db_kind}'")

            # LOGOBLOBS records.
            if "Key#" not in line or "Ingress#" not in line:
                fail(line_no, line, "LOGOBLOBS row missing 'Key#' or 'Ingress#'")
            match = LOGO_BLOB_ID_RE.search(line)
            if not match:
                fail(line_no, line, "failed to parse TLogoBlobID from row")
            logoblobs_total += 1
            _tablet_id, _generation, _step, channel, _cookie, blob_size_raw, _part_id = match.groups()
            channel_id = int(channel)
            blob_size = int(blob_size_raw)
            estimated_blob_size = estimate_blob_size(blob_size, args.erasure)
            part_size = max_part_size(blob_size, args.erasure)
            is_deleted = "DoNotKeep" in line
            blob_id = match.group(0)
            channel_key = "channel 0" if channel_id == 0 else "channel 1" if channel_id == 1 else "other channels"
            st = channels[channel_key]
            st["records"] += 1
            is_fresh = table_type in ("FCur", "FDreg", "FOld")
            if is_fresh:
                logoblobs_fresh_total += 1
            has_local = LOCAL_ALL_ZERO_MARKER not in line
            if has_local:
                logoblobs_with_local_total += 1
                if blob_id not in st["blobs"]:
                    st["blobs"].add(blob_id)
                    st["estimated_size"] += estimated_blob_size
                if is_small(
                    blob_size,
                    media=args.disk_type,
                    erasure=args.erasure,
                    min_huge_blob_in_bytes=args.min_huge_blob_in_bytes,
                ) and blob_id not in st["small_blobs"]:
                    st["small_blobs"].add(blob_id)
                    st["small_estimated_size"] += estimated_blob_size
            has_local_not_fresh = has_local and not is_fresh
            if not has_local_not_fresh:
                continue

            logoblobs_with_local_not_fresh_total += 1
            st["with_local_not_fresh_records"] += 1
            st["size"] += part_size
            if is_small(
                blob_size,
                media=args.disk_type,
                erasure=args.erasure,
                min_huge_blob_in_bytes=args.min_huge_blob_in_bytes,
            ):
                st["small_records"] += 1
                st["small_size"] += part_size
            if is_deleted:
                st["not_keep"] += 1

    print("\rprogress: 100.00% eta: 00:00", file=sys.stderr)
    if non_data_lines_skipped:
        print(f"note: skipped non-data lines: {non_data_lines_skipped}", file=sys.stderr)

    total_records = blocks_total + barriers_total + logoblobs_total
    logoblobs_estimated_size_total = sum(st.get("estimated_size", 0) for st in channels.values())
    logoblobs_calculated_size_total = sum(int(st.get("size", 0)) for st in channels.values())

    level_widths = {
        0: len("total records:"),
        2: len("logoblobs:"),
        4: len("with local not fresh:"),
        6: len("calculated size:"),
        8: len("records per blob:"),
        10: len("records per blob:"),
    }

    def emit(indent: int, label: str, value: str, *extras: str) -> None:
        width = level_widths.get(indent, len(label))
        tail = " ".join(x for x in extras if x)
        print(f"{' ' * indent}{label:<{width}}  {value}{(' ' + tail) if tail else ''}")

    emit(0, "total records:", str(total_records))
    emit(2, "blocks:", str(blocks_total), pct(blocks_total, total_records))
    emit(2, "barriers:", str(barriers_total), pct(barriers_total, total_records))
    emit(2, "logoblobs:", str(logoblobs_total), pct(logoblobs_total, total_records))
    emit(4, "with local:", str(logoblobs_with_local_total), pct(logoblobs_with_local_total, logoblobs_total), pct(logoblobs_with_local_total, total_records))
    emit(4, "fresh:", str(logoblobs_fresh_total), pct(logoblobs_fresh_total, logoblobs_total), pct(logoblobs_fresh_total, total_records))
    emit(4, "with local not fresh:", str(logoblobs_with_local_not_fresh_total), pct(logoblobs_with_local_not_fresh_total, logoblobs_total), pct(logoblobs_with_local_not_fresh_total, total_records))
    emit(4, "estimated size:", human_size(logoblobs_estimated_size_total))
    emit(4, "calculated size:", human_size(logoblobs_calculated_size_total))

    for channel_key in ("channel 0", "channel 1", "other channels"):
        st = channels[channel_key]
        records = int(st["with_local_not_fresh_records"])
        if records == 0:
            emit(4, f"{channel_key}:", "0")
            continue
        estimated_size = int(st["estimated_size"])
        not_keep = int(st["not_keep"])
        size = int(st["size"])
        small_estimated_size = int(st["small_estimated_size"])
        small_size = int(st["small_size"])
        small_records = int(st["small_records"])
        blobs = len(st["blobs"])
        unique_blobs = blobs
        small_blobs = len(st["small_blobs"])
        estimated_small_records = small_blobs * min_records_per_blob(args.erasure)
        records_per_blob = (records / unique_blobs) if unique_blobs else 0.0
        small_records_per_blob = (small_records / small_blobs) if small_blobs else 0.0

        emit(4, f"{channel_key}:", str(records), pct(records, logoblobs_with_local_not_fresh_total), pct(records, total_records))
        emit(6, "not keep:", str(not_keep), pct(not_keep, records), pct(not_keep, total_records))
        emit(6, "unique blobs:", str(unique_blobs))
        emit(8, "estimated size:", human_size(estimated_size), pct(estimated_size, logoblobs_estimated_size_total), pct(estimated_size, logoblobs_estimated_size_total))
        emit(8, "calculated size:", human_size(size), pct(size, logoblobs_calculated_size_total), pct(size, logoblobs_calculated_size_total))
        emit(8, "records per blob:", f"{records_per_blob:.1f}")
        emit(8, "small blobs:", str(small_blobs), pct(small_blobs, blobs))
        emit(10, "estimated size:", human_size(small_estimated_size), pct(small_estimated_size, estimated_size), pct(small_estimated_size, logoblobs_estimated_size_total))
        emit(10, "calculated size:", human_size(small_size), pct(small_size, size), pct(small_size, logoblobs_calculated_size_total))
        emit(10, "estimated:", str(estimated_small_records))
        emit(10, "actual:", str(small_records), pct(small_records, records), pct(small_records, total_records))
        emit(10, "records per blob:", f"{small_records_per_blob:.1f}")


if __name__ == "__main__":
    main()
