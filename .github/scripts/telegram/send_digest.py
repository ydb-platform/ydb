#!/usr/bin/env python3
"""
Send batched Telegram digests for new muted-test GitHub issues.

How it works
------------
Issues are placed into ``digest_queue`` at the moment they are created
(by create_new_muted_ya.py).  This script reads unsent rows
(sent_at IS NULL), sends per-team Telegram messages, then marks rows
as sent by writing sent_at = NOW().

There are no timing assumptions, no cursors, no historical-data floods.
The queue is the single source of truth for "what still needs to be sent".

Reads profiles from .github/config/telegram_notification_config.json
and runs only those whose schedule_utc_hours contains the current UTC hour
(unless --force is passed).

Usage:
  python send_digest.py [--config PATH] [--dry-run] [--force] [--profile ID]
"""

import argparse
import json
import os
import re
import sys
import ydb
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from github_issue_utils import make_profile_id

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "analytics"))
from ydb_wrapper import YDBWrapper

sys.path.insert(0, os.path.dirname(__file__))
from parse_and_send_team_issues import (
    load_team_channels,
    send_team_messages,
    get_all_team_data,
)


_SAFE_PROFILE_RE = re.compile(r'^[a-zA-Z0-9._-]+$')


def _validate_profile_id(profile_id: str) -> str:
    if not _SAFE_PROFILE_RE.match(profile_id):
        raise ValueError(f"Invalid profile_id: {profile_id!r}")
    return profile_id


# ── YDB helpers ───────────────────────────────────────────────────────────────


def _fetch_closed_unsent(w: YDBWrapper, profile_id: str) -> list:
    """Return unsent queue rows whose issues have been closed (should be silently marked sent)."""
    _validate_profile_id(profile_id)
    queue_path = w.get_table_path("digest_queue")
    issues_path = w.get_table_path("issues")
    return w.execute_scan_query(
        f"""
        SELECT
            q.github_issue_number AS github_issue_number,
            q.github_issue_url AS github_issue_url,
            q.github_issue_title AS github_issue_title,
            q.owner_team AS owner_team,
            q.branch AS branch,
            q.build_type AS build_type,
            q.enqueued_at AS enqueued_at
        FROM `{queue_path}` AS q
        INNER JOIN `{issues_path}` AS i
            ON q.github_issue_number = i.issue_number
        WHERE q.profile_id = '{profile_id}'
          AND q.sent_at IS NULL
          AND i.state = 'CLOSED'
        """,
        query_name="digest_fetch_closed_unsent",
    )


def _mark_closed_as_sent(w: YDBWrapper, profile_id: str) -> None:
    """Silently mark closed issues as sent so they never appear in digest."""
    closed = _fetch_closed_unsent(w, profile_id)
    if closed:
        now = datetime.now(tz=timezone.utc)
        _mark_sent(w, profile_id, closed, now)
        print(f"Marked {len(closed)} closed issue(s) as sent (skipped from digest)")


def _fetch_unsent(w: YDBWrapper, profile_id: str) -> list:
    """Return unsent digest_queue rows, excluding issues that have been closed since enqueue."""
    _validate_profile_id(profile_id)
    queue_path = w.get_table_path("digest_queue")
    issues_path = w.get_table_path("issues")
    return w.execute_scan_query(
        f"""
        SELECT
            q.github_issue_number AS github_issue_number,
            q.github_issue_url AS github_issue_url,
            q.github_issue_title AS github_issue_title,
            q.owner_team AS owner_team,
            q.branch AS branch,
            q.build_type AS build_type,
            q.enqueued_at AS enqueued_at
        FROM `{queue_path}` AS q
        LEFT JOIN `{issues_path}` AS i
            ON q.github_issue_number = i.issue_number
        WHERE q.profile_id = '{profile_id}'
          AND q.sent_at IS NULL
          AND (i.state IS NULL OR i.state != 'CLOSED')
        ORDER BY q.owner_team, q.github_issue_number
        """,
        query_name="digest_fetch_unsent",
    )


def _mark_sent(w: YDBWrapper, profile_id: str, unsent_rows: list, sent_at: datetime) -> None:
    """Mark rows as sent via UPSERT (re-writes the full row with sent_at populated).

    All original columns are preserved — bulk_upsert replaces the whole row
    so we must provide every column to avoid nulling out data.
    """
    if not unsent_rows:
        return

    table_path = w.get_table_path("digest_queue")

    rows = []
    for r in unsent_rows:
        rows.append({
            'profile_id':          profile_id,
            'github_issue_number': int(r["github_issue_number"]),
            'github_issue_url':    r.get("github_issue_url") or "",
            'github_issue_title':  r.get("github_issue_title") or "",
            'owner_team':          r.get("owner_team") or "",
            'branch':              r.get("branch") or "",
            'build_type':          r.get("build_type") or "",
            'enqueued_at':         r.get("enqueued_at"),
            'sent_at':             sent_at,
        })

    column_types = (
        ydb.BulkUpsertColumns()
        .add_column('profile_id',          ydb.PrimitiveType.Utf8)
        .add_column('github_issue_number', ydb.PrimitiveType.Uint64)
        .add_column('github_issue_url',    ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('github_issue_title',  ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('owner_team',          ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('branch',              ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('build_type',          ydb.OptionalType(ydb.PrimitiveType.Utf8))
        .add_column('enqueued_at',         ydb.PrimitiveType.Timestamp)
        .add_column('sent_at',             ydb.OptionalType(ydb.PrimitiveType.Timestamp))
    )

    w.bulk_upsert(table_path, rows, column_types)


# ── Digest logic ──────────────────────────────────────────────────────────────

def _group_by_team(rows: list) -> dict:
    """Return {team_name: [{url, title}, ...]}."""
    teams: dict = {}
    for row in rows:
        team = (row.get("owner_team") or "Unknown").strip() or "Unknown"
        teams.setdefault(team, []).append(
            {
                "url":   row.get("github_issue_url") or "",
                "title": row.get("github_issue_title") or "(no title)",
            }
        )
    return teams


def run_digest(
    profile: dict,
    team_channels: dict,
    bot_token: str,
    dry_run: bool = False,
) -> bool:
    """Process one notification profile. Returns True on success."""
    profile_id  = make_profile_id(profile["branch"], profile["build_type"])
    include_plots = profile.get("include_plots", False)

    print(f"\n{'=' * 60}")
    print(f"Profile : {profile_id}")
    print(f"Branch  : {profile['branch']}   build_type: {profile['build_type']}")
    print(f"{'=' * 60}")

    with YDBWrapper() as w:
        if not w.check_credentials():
            return False

        _mark_closed_as_sent(w, profile_id)

        unsent = _fetch_unsent(w, profile_id)
        print(f"Unsent issues in queue: {len(unsent)}")

        if not unsent:
            print("Nothing to send.")
            return True

        teams = _group_by_team(unsent)
        print(f"Teams: {sorted(teams)}")

        if dry_run:
            print("[DRY RUN] Would send:")
            for team, issues in sorted(teams.items()):
                print(f"  {team}: {len(issues)} issue(s)")
            return True

        muted_stats   = None
        all_team_data = None
        try:
            all_team_data = get_all_team_data()
            if all_team_data:
                muted_stats = {t: d["stats"] for t, d in all_team_data.items()}
        except Exception as exc:
            print(f"Warning: could not fetch muted stats: {exc}")

        send_team_messages(
            teams=teams,
            bot_token=bot_token,
            team_channels=team_channels,
            muted_stats=muted_stats,
            include_plots=include_plots,
            ydb_config={"use_yesterday": False} if include_plots else None,
            all_team_data=all_team_data,
        )

        now = datetime.now(tz=timezone.utc)
        _mark_sent(w, profile_id, unsent, now)
        print(f"Marked {len(unsent)} issue(s) as sent.")
        return True


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send batched Telegram digest for new muted-test issues"
    )
    parser.add_argument(
        "--config",
        default=".github/config/telegram_notification_config.json",
        help="Path to notification config JSON",
    )
    parser.add_argument("--bot-token", help="Telegram bot token")
    parser.add_argument(
        "--team-channels",
        help="JSON string or file path with team channel config",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print without sending")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run all profiles regardless of schedule",
    )
    parser.add_argument("--profile", help="Run only this profile ID")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)

    config = json.loads(config_path.read_text())
    profiles: list = config.get("profiles", [])

    current_hour_utc = datetime.now(tz=timezone.utc).hour

    bot_token = args.bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
    if not bot_token and not args.dry_run:
        print("Error: --bot-token or TELEGRAM_BOT_TOKEN env var is required")
        sys.exit(1)

    team_channels_src = args.team_channels or os.environ.get("TG_TEAM_CHANNELS")
    team_channels = load_team_channels(team_channels_src) if team_channels_src else None
    if not team_channels:
        print("Error: --team-channels or TG_TEAM_CHANNELS env var is required")
        sys.exit(1)

    active = [
        p
        for p in profiles
        if (not args.profile or make_profile_id(p["branch"], p["build_type"]) == args.profile)
        and (args.force or current_hour_utc in p.get("schedule_utc_hours", []))
    ]

    if not active:
        print(f"No profiles active for UTC hour {current_hour_utc} — nothing to do")
        sys.exit(0)

    print(f"Active profiles: {[make_profile_id(p['branch'], p['build_type']) for p in active]}")

    failed = False
    for profile in active:
        channels_var = profile.get("channels_var", "TG_TEAM_CHANNELS")
        profile_channels_src = os.environ.get(channels_var, team_channels_src)
        profile_channels = load_team_channels(profile_channels_src) or team_channels

        ok = run_digest(profile, profile_channels, bot_token, dry_run=args.dry_run)
        if not ok:
            failed = True

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
