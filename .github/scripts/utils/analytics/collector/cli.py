"""CLI for start/end/track/send/flush/enrich."""

from __future__ import annotations

import argparse
import sys
from typing import Any, Callable, Dict, Optional

from .schema import KIND_UNITS
from .values import parse_labels


def _properties_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}
    label_items = list(getattr(args, "label", None) or [])
    properties.update(parse_labels(label_items))
    run_id = getattr(args, "run_id", None)
    if run_id not in (None, ""):
        properties["run_id"] = run_id
    error = getattr(args, "error", None)
    if error not in (None, ""):
        properties["error"] = error
    return properties


def resolve_track_name(args: argparse.Namespace) -> str:
    for candidate in (getattr(args, "name", None), getattr(args, "positional_name", None)):
        text = str(candidate).strip() if candidate is not None else ""
        if text:
            return text
    return ""


def resolve_track_value(args: argparse.Namespace) -> Optional[float]:
    if args.value is not None:
        return args.value
    duration_ms = getattr(args, "duration_ms", None)
    if duration_ms is not None:
        return duration_ms
    return None


def resolve_track_kind(args: argparse.Namespace) -> Optional[str]:
    if args.kind:
        return args.kind
    if getattr(args, "duration_ms", None) is not None:
        return "duration"
    return None


def run_cli(
    args: argparse.Namespace,
    *,
    start_fn: Optional[Callable[..., Any]] = None,
    end_fn: Optional[Callable[..., Any]] = None,
    track_fn: Optional[Callable[..., Any]] = None,
    send_fn: Optional[Callable[..., Any]] = None,
    flush_fn: Optional[Callable[..., Any]] = None,
    enrich_fn: Optional[Callable[..., Any]] = None,
    default_file: Optional[str] = None,
    extra_kwargs_fn: Optional[Callable[[argparse.Namespace], Dict[str, Any]]] = None,
) -> int:
    """Dispatch start/end/track/send/flush/enrich. Wrappers pass their own fns."""
    if start_fn is None or end_fn is None or track_fn is None or send_fn is None or flush_fn is None or enrich_fn is None:
        from . import flush as flush_mod
        from . import spans as spans_mod

        start_fn = start_fn or spans_mod.start
        end_fn = end_fn or spans_mod.end
        track_fn = track_fn or spans_mod.track
        send_fn = send_fn or spans_mod.send
        flush_fn = flush_fn or flush_mod.flush_file
        enrich_fn = enrich_fn or spans_mod.enrich
    file = getattr(args, "file", None) or default_file
    extra = extra_kwargs_fn(args) if extra_kwargs_fn else {}
    if args.command == "flush":
        flush_fn(file, table_path=getattr(args, "table_path", None))
        return 0
    props = _properties_from_args(args)
    name = resolve_track_name(args)
    if args.command == "start":
        if not name:
            print("Warning: start requires a span name", file=sys.stderr)
            return 1
        start_fn(
            name,
            props,
            file=file,
            kind=resolve_track_kind(args) or "duration",
            source=args.source,
            started_at=args.started_at,
            started_epoch=args.started_epoch,
            conclusion=args.conclusion,
            **extra,
        )
        return 0
    if args.command == "end":
        end_fn(
            name or None,
            props,
            file=file,
            conclusion=args.conclusion,
            source=args.source,
            value=resolve_track_value(args),
            unit=args.unit,
            finished_epoch=args.finished_epoch,
            **extra,
        )
        return 0
    if args.command == "enrich":
        if not name:
            print("Warning: enrich requires an event name", file=sys.stderr)
            return 1
        enrich_fn(name, props, file=file, **extra)
        return 0
    if args.command == "track":
        if not name:
            print("Warning: track requires an event name (--name / positional)", file=sys.stderr)
            return 1
        track_fn(
            name,
            props,
            file=file,
            kind=resolve_track_kind(args),
            source=args.source,
            value=resolve_track_value(args),
            unit=args.unit,
            started_at=args.started_at,
            started_epoch=args.started_epoch,
            finished_epoch=args.finished_epoch,
            conclusion=args.conclusion,
            **extra,
        )
        return 0
    if args.command == "send":
        send_fn(
            name or None,
            props,
            file=file,
            conclusion=args.conclusion,
            source=args.source,
            kind=resolve_track_kind(args),
            value=resolve_track_value(args),
            unit=args.unit,
            started_at=args.started_at,
            started_epoch=args.started_epoch,
            finished_epoch=args.finished_epoch,
            table_path=getattr(args, "table_path", None),
            **extra,
        )
        return 0
    return 0


def add_track_cli_args(parser: argparse.ArgumentParser, *, kind_default: Optional[str] = None) -> None:
    parser.add_argument("positional_name", nargs="?", default=None, help="Event/metric name")
    parser.add_argument("--name", default=None, help="Event/metric name")
    parser.add_argument("--kind", default=kind_default, choices=sorted(KIND_UNITS))
    parser.add_argument("--source", default=None, help="Producer id, e.g. nightly_build")
    parser.add_argument("--value", type=float, default=None)
    parser.add_argument("--duration-ms", type=float, default=None, help="Duration shortcut (kind=duration)")
    parser.add_argument("--unit", default=None)
    parser.add_argument("--started-at", default=None, help="ISO-8601 timestamp")
    parser.add_argument("--started-epoch", default=None, help="epoch seconds or ms")
    parser.add_argument("--finished-epoch", default=None, help="epoch seconds or ms")
    parser.add_argument("--conclusion", default=None)
    parser.add_argument("--error", default=None, help="Short error/status reason (labels.error)")
    parser.add_argument("--label", action="append", default=[], help="key=value")
    parser.add_argument("--file", default=None, help="JSONL path")
    parser.add_argument("--run-id", default=None, help="Run id (or $ANALYTICS_RUN_ID)")


def add_enrich_cli_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("positional_name", nargs="?", default=None, help="Event name")
    parser.add_argument("--name", default=None, help="Event name")
    parser.add_argument("--label", action="append", default=[], help="key=value")
    parser.add_argument("--error", default=None, help="Short error/status reason (labels.error)")
    parser.add_argument("--file", default=None, help="JSONL path")


def build_parser(
    description: str = "Analytics: start/end/track + batch send",
    *,
    track_extra: Optional[Callable[[argparse.ArgumentParser], None]] = None,
    enrich_extra: Optional[Callable[[argparse.ArgumentParser], None]] = None,
) -> tuple[argparse.ArgumentParser, argparse._SubParsersAction]:
    parser = argparse.ArgumentParser(description=description)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_track(name: str, help_text: str, *, kind_default: Optional[str] = None) -> argparse.ArgumentParser:
        command = sub.add_parser(name, help=help_text)
        add_track_cli_args(command, kind_default=kind_default)
        if track_extra:
            track_extra(command)
        return command

    add_track("start", "Open a span (auto start time)", kind_default="duration")
    add_track("end", "Close open span(s); duration is computed")
    add_track("track", "Queue a completed event (no open span)")
    enrich_p = sub.add_parser("enrich", help="Add labels to last unsent record; duration stays")
    add_enrich_cli_args(enrich_p)
    if enrich_extra:
        enrich_extra(enrich_p)
    send_p = add_track("send", "End leftover spans and export the batch")
    send_p.add_argument("--table-path", default=None)
    flush_p = sub.add_parser("flush", help="Export completed events only")
    flush_p.add_argument("--file", default=None, help="JSONL path")
    flush_p.add_argument("--table-path", default=None)
    return parser, sub


def parse_args(argv=None) -> argparse.Namespace:
    parser, _sub = build_parser()
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        return run_cli(parse_args(argv))
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail the caller
        print(f"Warning: analytics failed: {exc}", file=sys.stderr)
        return 0
