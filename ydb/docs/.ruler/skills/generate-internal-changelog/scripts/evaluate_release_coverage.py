#!/usr/bin/env python3
"""Validate Tracker-to-RU/EN release-note coverage for one release line."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys
from typing import Any


TICKET_RE = re.compile(r"^YDBFEATURES-[0-9]+$")
LINK_RE = re.compile(r"\[[^]]+\]\(([^)]+)\)")
TRACKER_FIELDS = (
    "actualCodeReadyBranch",
    "actualDefaultInclusionBranch",
)
EXCLUSION_REASON_CODES = {
    "not-in-target",
    "bug-only",
    "already-released",
}


def _normalize_block(value: str) -> str:
    lines = [line.rstrip() for line in value.strip().splitlines()]
    return "\n".join(lines)


def _extract_release_bullets(
    text: str, release_line: str, locale: str, errors: list[str]
) -> list[str]:
    slug = release_line.replace(".", "-")
    if locale == "en":
        release_heading = rf"^## Version {re.escape(release_line)} \{{#{slug}\}}\s*$"
        functionality_heading = r"^### Functionality\s*$"
        functionality_title = "Functionality"
    else:
        release_heading = rf"^## Версия {re.escape(release_line)} \{{#{slug}\}}\s*$"
        functionality_heading = r"^### Функциональность\s*$"
        functionality_title = "Функциональность"

    release_matches = list(re.finditer(release_heading, text, re.MULTILINE))
    if not release_matches:
        raise ValueError(f"{locale.upper()} release heading is missing")
    if len(release_matches) != 1:
        raise ValueError(
            f"{locale.upper()} release heading appears {len(release_matches)} times"
        )
    release_match = release_matches[0]

    next_release = re.search(r"^##\s+", text[release_match.end() :], re.MULTILINE)
    section_end = (
        release_match.end() + next_release.start() if next_release else len(text)
    )
    section = text[release_match.end() : section_end]

    if re.search(r"^### (?:Version|Версия) [0-9]+\.[0-9]+\.[0-9]+", section, re.MULTILINE):
        raise ValueError(f"{locale.upper()} release section contains a patch-version subsection")
    bug_heading = r"^### (?:Bug Fixes|Исправления ошибок)\s*$"
    if re.search(bug_heading, section, re.MULTILINE):
        raise ValueError(f"{locale.upper()} release section contains a bug-fix subsection")

    subsections = re.findall(r"^(#{3,})\s+(.+?)\s*$", section, re.MULTILINE)
    if subsections != [("###", functionality_title)]:
        errors.append(f"{locale.upper()} release section has unexpected subsections")

    functionality_match = re.search(functionality_heading, section, re.MULTILINE)
    if not functionality_match:
        raise ValueError(f"{locale.upper()} functionality heading is missing")

    before_functionality = section[: functionality_match.start()]
    if re.search(r"^(?:[*+-]|[0-9]+[.)])\s+", before_functionality, re.MULTILINE):
        errors.append(
            f"{locale.upper()} release section has bullets before {functionality_title}"
        )

    body = section[functionality_match.end() :]
    bullets: list[str] = []
    current: list[str] | None = None
    reported_non_asterisk = False
    for line in body.splitlines():
        if re.match(r"^#+\s+", line):
            if current is not None:
                bullets.append(_normalize_block("\n".join(current)))
                current = None
            continue
        bullet_match = re.match(r"^([*+-]|[0-9]+[.)])\s+(.*)$", line)
        if bullet_match:
            if current is not None:
                bullets.append(_normalize_block("\n".join(current)))
            if bullet_match.group(1) != "*" and not reported_non_asterisk:
                errors.append(
                    f"{locale.upper()} release section uses a non-asterisk bullet"
                )
                reported_non_asterisk = True
            current = [bullet_match.group(2)]
        elif current is not None:
            current.append(line)
    if current is not None:
        bullets.append(_normalize_block("\n".join(current)))
    return bullets


def _check_links(note: dict[str, Any], release_line: str, errors: list[str]) -> None:
    ticket = note.get("ticket_key", "<missing>")
    locale_links: dict[str, list[str]] = {}
    for locale in ("en", "ru"):
        text = note.get(locale, "")
        links = LINK_RE.findall(text) if isinstance(text, str) else []
        locale_links[locale] = links
        if not links:
            errors.append(f"{ticket}: {locale.upper()} note has no public link")
        for link in links:
            if "st.yandex-team.ru" in link or "tracker.yandex" in link:
                errors.append(f"{ticket}: {locale.upper()} note exposes an internal link")
            is_external = re.match(r"^[a-z][a-z0-9+.-]*://", link, re.IGNORECASE)
            is_docs_link = not is_external or "ydb.tech/docs/" in link
            if is_docs_link and f"?version=v{release_line}" not in link:
                errors.append(
                    f"{ticket}: {locale.upper()} note has an unversioned documentation link"
                )
    if locale_links["en"] != locale_links["ru"]:
        errors.append(f"{ticket}: EN and RU link targets differ")


def _derive_filter_keys(
    field_key: str,
    field_export: dict[str, Any],
    expected_value: str,
    errors: list[str],
) -> list[str]:
    field_id = field_export.get("field_id", "")
    if not isinstance(field_id, str) or not field_id.endswith(f"--{field_key}"):
        errors.append(f"Tracker field_id must resolve the {field_key} local field")

    pages = field_export.get("pages")
    if not isinstance(pages, list) or not pages:
        errors.append(f"Tracker {field_key} export must contain at least one page")
        return []

    page_numbers = [page.get("page") for page in pages if isinstance(page, dict)]
    page_counts = {
        page.get("pages_count") for page in pages if isinstance(page, dict)
    }
    total_counts = {
        page.get("total_issues_count") for page in pages if isinstance(page, dict)
    }
    if len(page_counts) != 1 or not all(isinstance(value, int) for value in page_counts):
        errors.append(f"Tracker {field_key} pages disagree on pages_count")
        pages_count = 0
    else:
        pages_count = next(iter(page_counts))
    if page_numbers != list(range(1, pages_count + 1)):
        errors.append(f"Tracker {field_key} pagination is incomplete")
    if len(total_counts) != 1 or not all(isinstance(value, int) for value in total_counts):
        errors.append(f"Tracker {field_key} pages disagree on total_issues_count")
        total_count = -1
    else:
        total_count = next(iter(total_counts))

    issues: list[dict[str, Any]] = []
    for page in pages:
        if not isinstance(page, dict) or not isinstance(page.get("issues"), list):
            errors.append(f"Tracker {field_key} page has no issues list")
            continue
        issues.extend(issue for issue in page["issues"] if isinstance(issue, dict))
    if len(issues) != total_count:
        errors.append(
            f"Tracker {field_key} export issue count {len(issues)} "
            f"does not match live total {total_count}"
        )

    keys: list[str] = []
    for issue in issues:
        key = issue.get("key", "<missing>")
        if issue.get("type") != "feature":
            errors.append(f"{key}: Tracker type is not feature")
            continue
        if not isinstance(key, str) or not TICKET_RE.fullmatch(key):
            errors.append("Tracker export contains an invalid ticket key")
            continue
        if issue.get(field_key) != expected_value:
            errors.append(f"{key}: {field_key} does not match {expected_value}")
            continue
        keys.append(key)
    if len(set(keys)) != len(keys):
        errors.append(f"Tracker {field_key} export contains duplicate ticket keys")
    return keys


def _derive_tracker_keys(
    tracker_export: dict[str, Any], expected_value: str, errors: list[str]
) -> list[str]:
    if tracker_export.get("queue") != "YDBFEATURES":
        errors.append("Tracker queue must be YDBFEATURES")
    if tracker_export.get("issue_type") != "feature":
        errors.append("Tracker issue_type must be feature")
    if tracker_export.get("value") != expected_value:
        errors.append(f"Tracker value must be {expected_value}")

    fields = tracker_export.get("fields")
    if not isinstance(fields, dict):
        errors.append("Tracker export must contain both release fields")
        return []

    keys: list[str] = []
    for field_key in TRACKER_FIELDS:
        field_export = fields.get(field_key)
        if not isinstance(field_export, dict):
            errors.append(f"Tracker export is missing {field_key}")
            continue
        keys.extend(
            _derive_filter_keys(field_key, field_export, expected_value, errors)
        )
    return sorted(set(keys))


def evaluate(
    manifest: dict[str, Any],
    tracker_export: dict[str, Any],
    en_text: str,
    ru_text: str,
) -> list[str]:
    errors: list[str] = []
    release_input = manifest.get("release_input", "")
    version_match = re.fullmatch(r"v?([0-9]+)\.([0-9]+)\.([0-9]+)", release_input)
    if not version_match:
        return ["release_input must have three numeric components"]

    expected_line = f"{version_match.group(1)}.{version_match.group(2)}"
    expected_tracker_value = (
        f"stable-{version_match.group(1)}-{version_match.group(2)}-"
        f"{version_match.group(3)}"
    )
    release_line = manifest.get("release_line")
    if release_line != expected_line:
        errors.append(f"release_line must be {expected_line}")

    tracker_keys = _derive_tracker_keys(tracker_export, expected_tracker_value, errors)
    notes = manifest.get("notes")
    if not isinstance(notes, list):
        errors.append("notes must be a list")
        notes = []

    note_keys: list[str] = []
    for index, note in enumerate(notes, start=1):
        if not isinstance(note, dict):
            errors.append(f"note {index} must be an object")
            continue
        ticket = note.get("ticket_key")
        if not isinstance(ticket, str) or not TICKET_RE.fullmatch(ticket):
            errors.append(f"note {index} has an invalid ticket_key")
        else:
            note_keys.append(ticket)
        for locale in ("en", "ru"):
            value = note.get(locale)
            if not isinstance(value, str) or not value.strip():
                errors.append(f"{ticket or f'note {index}'}: {locale.upper()} text is empty")
        _check_links(note, expected_line, errors)

    duplicates = sorted(key for key, count in Counter(note_keys).items() if count > 1)
    if duplicates:
        errors.append(f"duplicate note keys: {', '.join(duplicates)}")

    exclusions = manifest.get("exclusions", [])
    if not isinstance(exclusions, list):
        errors.append("exclusions must be a list")
        exclusions = []
    exclusion_keys: list[str] = []
    for index, exclusion in enumerate(exclusions, start=1):
        if not isinstance(exclusion, dict):
            errors.append(f"exclusion {index} must be an object")
            continue
        ticket = exclusion.get("ticket_key")
        if not isinstance(ticket, str) or not TICKET_RE.fullmatch(ticket):
            errors.append(f"exclusion {index} has an invalid ticket_key")
        else:
            exclusion_keys.append(ticket)
        reason_code = exclusion.get("reason_code")
        if reason_code not in EXCLUSION_REASON_CODES:
            errors.append(f"{ticket or f'exclusion {index}'}: invalid exclusion reason_code")
        reason = exclusion.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            errors.append(f"{ticket or f'exclusion {index}'}: exclusion reason is empty")

    duplicate_exclusions = sorted(
        key for key, count in Counter(exclusion_keys).items() if count > 1
    )
    if duplicate_exclusions:
        errors.append(f"duplicate exclusion keys: {', '.join(duplicate_exclusions)}")

    overlap = sorted(set(note_keys) & set(exclusion_keys))
    if overlap:
        errors.append(f"ticket keys are both noted and excluded: {', '.join(overlap)}")

    accounted_keys = set(note_keys) | set(exclusion_keys)
    missing = sorted(set(tracker_keys) - accounted_keys)
    extra = sorted(accounted_keys - set(tracker_keys))
    if missing:
        errors.append(f"unaccounted ticket keys: {', '.join(missing)}")
    if extra:
        errors.append(f"extra ticket keys: {', '.join(extra)}")

    if release_line == expected_line:
        try:
            actual_en = _extract_release_bullets(en_text, release_line, "en", errors)
            expected_en = [
                _normalize_block(note.get("en", ""))
                for note in notes
                if isinstance(note, dict)
            ]
            if actual_en != expected_en:
                errors.append("EN bullets do not exactly match manifest order/content")
        except ValueError as error:
            errors.append(str(error))
        try:
            actual_ru = _extract_release_bullets(ru_text, release_line, "ru", errors)
            expected_ru = [
                _normalize_block(note.get("ru", ""))
                for note in notes
                if isinstance(note, dict)
            ]
            if actual_ru != expected_ru:
                errors.append("RU bullets do not exactly match manifest order/content")
        except ValueError as error:
            errors.append(str(error))

    return errors


def _coverage_counts(
    manifest: dict[str, Any], tracker_export: dict[str, Any]
) -> dict[str, int]:
    version_match = re.fullmatch(
        r"v?([0-9]+)\.([0-9]+)\.([0-9]+)", manifest["release_input"]
    )
    assert version_match is not None
    expected_tracker_value = (
        f"stable-{version_match.group(1)}-{version_match.group(2)}-"
        f"{version_match.group(3)}"
    )
    tracker_keys = _derive_tracker_keys(tracker_export, expected_tracker_value, [])
    return {
        "tracker_count": len(set(tracker_keys)),
        "note_count": len(manifest.get("notes", [])),
        "exclusion_count": len(manifest.get("exclusions", [])),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--tracker-export", required=True, type=Path)
    parser.add_argument("--en", required=True, type=Path)
    parser.add_argument("--ru", required=True, type=Path)
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    tracker_export = json.loads(args.tracker_export.read_text(encoding="utf-8"))
    errors = evaluate(
        manifest,
        tracker_export,
        args.en.read_text(encoding="utf-8"),
        args.ru.read_text(encoding="utf-8"),
    )
    if errors:
        print(json.dumps({"status": "fail", "errors": errors}, ensure_ascii=False, indent=2))
        return 1
    result = {"status": "pass", **_coverage_counts(manifest, tracker_export)}
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
