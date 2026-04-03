#!/usr/bin/env python3
"""
Send batched Telegram digests for new muted-test GitHub issues.

How it works
------------
Issues are placed into `digest_queue` at the moment they are created
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
import sys
from datetime import datetime, timezone
from pathlib import Path

import ydb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "analytics"))
from ydb_wrapper import YDBWrapper

sys.path.insert(0, os.path.dirname(__file__))
from parse_and_send_team_issues import (
    load_team_channels,
    send_team_messages,
    get_all_team_data,
)


# ── YDB helpers ───────────────────────────────────────────────────────────────


def _fetch_unsent(w: YDBWrapper, profile_id: str) -> list:
    """Return all digest_queue rows for this profile where sent_at IS NULL."""
    table_path = w.get_table_path("digest_queue")
    return w.execute_scan_query(
        f"""
        SELECT
            github_issue_number,
            github_issue_url,
            github_issue_title,
            owner_team
        FROM `{table_path}`
        WHERE profile_id = '{profile_id}'
          AND sent_at IS NULL
        ORDER BY owner_team, github_issue_number
        """,
        query_name="digest_fetch_unsent",
    )


def _mark_sent(w: YDBWrapper, profile_id: str, issue_numbers: list, sent_at: datetime) -> None:
    """Set sent_at on the given rows."""
    if not issue_numbers:
        return

    table_path = w.get_table_path("digest_queue")
    sent_at_us = int(sent_at.timestamp() * 1_000_000)
    in_list = ", ".join(f"{n}ul" for n in issue_numbers)

    w.execute_dml(
        f"""
        UPDATE `{table_path}`
        SET sent_at = CAST({sent_at_us}ul AS Timestamp)
        WHERE profile_id = '{profile_id}'
          AND github_issue_number IN ({in_list})
        """,
        query_name="digest_mark_sent",
    )


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
    profile_id  = profile["id"]
    branch      = profile["branch"]
    build_type  = profile["build_type"]
    include_plots = profile.get("include_plots", False)

    print(f"\n{'=' * 60}")
    print(f"Profile : {profile_id}")
    print(f"Branch  : {branch}   build_type: {build_type}")
    print(f"{'=' * 60}")

    with YDBWrapper(use_local_config=False) as w:
        if not w.check_credentials():
            return False

        # Step 1: fetch what needs to be sent
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

        # Step 2: fetch muted stats (best-effort)
        muted_stats   = None
        all_team_data = None
        try:
            all_team_data = get_all_team_data()
            if all_team_data:
                muted_stats = {t: d["stats"] for t, d in all_team_data.items()}
        except Exception as exc:
            print(f"Warning: could not fetch muted stats: {exc}")

        # Step 3: send
        send_team_messages(
            teams=teams,
            bot_token=bot_token,
            team_channels=team_channels,
            muted_stats=muted_stats,
            include_plots=include_plots,
            ydb_config={} if include_plots else None,
            all_team_data=all_team_data,
        )

        # Step 4: mark as sent
        sent_numbers = [int(r["github_issue_number"]) for r in unsent]
        _mark_sent(w, profile_id, sent_numbers, datetime.now(tz=timezone.utc))
        print(f"Marked {len(sent_numbers)} issue(s) as sent.")
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
        if (not args.profile or p["id"] == args.profile)
        and (args.force or current_hour_utc in p.get("schedule_utc_hours", []))
    ]

    if not active:
        print(f"No profiles active for UTC hour {current_hour_utc} — nothing to do")
        sys.exit(0)

    print(f"Active profiles: {[p['id'] for p in active]}")

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
