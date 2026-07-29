"""Upload duty artifacts to Yandex Object Storage (workload-log).

Uses AWS_KEY_ID / AWS_KEY_VALUE from env (via ``dutyctl init-token``).
Requires ``boto3`` (same stack as CI Object Storage clients).

Layout (immutable per publish):

  s3://workload-log/perfomance_tests_status/duty_artifacts/{run_id}/{utc_stamp}/…
  https://storage.yandexcloud.net/workload-log/perfomance_tests_status/duty_artifacts/{run_id}/{utc_stamp}/…
"""

from __future__ import annotations

import json
import mimetypes
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_BUCKET = "workload-log"
DEFAULT_ENDPOINT = "https://storage.yandexcloud.net"
DEFAULT_REGION = "ru-central1"
DEFAULT_PREFIX_ROOT = "perfomance_tests_status/duty_artifacts"

# Local filenames → S3 object names (unchanged) + human labels for GitHub.
DEFAULT_FILES = ("analysis.md", "result.json", "problems.json")
FILE_LABELS = {
    "analysis.md": "полный отчёт",
    "result.json": "result",
    "problems.json": "problems",
}


class S3UploadError(RuntimeError):
    """S3 PutObject / config failure."""


def _endpoint() -> str:
    ep = (os.environ.get("AWS_ENDPOINT") or DEFAULT_ENDPOINT).rstrip("/")
    if not ep.startswith("http"):
        ep = "https://" + ep
    return ep


def _creds() -> tuple[str, str]:
    key = os.environ.get("AWS_KEY_ID") or ""
    secret = os.environ.get("AWS_KEY_VALUE") or ""
    if not key or not secret:
        raise S3UploadError(
            "AWS_KEY_ID / AWS_KEY_VALUE missing — run: "
            'eval "$(python3 dutyctl.py init-token --shell)"'
        )
    return key, secret


def _ensure_boto3() -> None:
    """Import boto3; fall back to duty_agent/.cache/venv-s3 site-packages."""
    try:
        import boto3  # noqa: F401
        return
    except ImportError:
        pass
    venv_lib = _ROOT / ".cache" / "venv-s3" / "lib"
    if venv_lib.is_dir():
        for site in sorted(venv_lib.glob("python*/site-packages")):
            sp = str(site)
            if sp not in sys.path:
                sys.path.insert(0, sp)
            try:
                import boto3  # noqa: F401
                return
            except ImportError:
                continue
    raise S3UploadError(
        "boto3 is required for upload-report — "
        "python3 -m pip install boto3  "
        "or: python3 -m venv .cache/venv-s3 && .cache/venv-s3/bin/pip install boto3"
    )


def _boto3_client(*, endpoint: str | None = None, region: str = DEFAULT_REGION) -> Any:
    _ensure_boto3()
    import boto3
    from botocore.client import Config

    access_key, secret_key = _creds()
    return boto3.client(
        "s3",
        endpoint_url=(endpoint or _endpoint()).rstrip("/"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )


def put_object(
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
    acl: str | None = "public-read",
    endpoint: str | None = None,
    region: str = DEFAULT_REGION,
) -> str:
    """Put object via boto3; return path-style public HTTPS URL."""
    client = _boto3_client(endpoint=endpoint, region=region)
    ep = (endpoint or _endpoint()).rstrip("/")
    extra: dict[str, Any] = {"ContentType": content_type}
    if acl:
        extra["ACL"] = acl
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, **extra)
    except Exception as e:
        err = str(e)
        if acl and ("AccessDenied" in err or "InvalidArgument" in err or "NotImplemented" in err):
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
            )
        else:
            raise S3UploadError(f"PutObject failed s3://{bucket}/{key}: {e}") from e
    return f"{ep}/{bucket}/{key}"


def content_type_for(path: Path) -> str:
    if path.suffix == ".md":
        return "text/markdown; charset=utf-8"
    if path.suffix == ".json":
        return "application/json"
    guessed, _ = mimetypes.guess_type(path.name)
    return guessed or "application/octet-stream"


def public_url(bucket: str, key: str, endpoint: str | None = None) -> str:
    return f"{(endpoint or _endpoint()).rstrip('/')}/{bucket}/{key}"


def new_publish_stamp() -> str:
    """UTC stamp for immutable publish directory (second resolution)."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def upload_duty_report(
    out_dir: Path,
    *,
    bucket: str = DEFAULT_BUCKET,
    prefix_root: str = DEFAULT_PREFIX_ROOT,
    files: tuple[str, ...] = DEFAULT_FILES,
    run_id: str | None = None,
    stamp: str | None = None,
) -> dict[str, Any]:
    """Upload analysis.md (+ companions); write ``s3_report.json``.

    Keys are immutable: ``{prefix_root}/{run_id}/{stamp}/{file}``.
    Re-running upload creates a new stamp directory (no overwrite).
    """
    out_dir = Path(out_dir)
    run_id = run_id or out_dir.name
    stamp = stamp or new_publish_stamp()
    prefix = f"{prefix_root.strip('/')}/{run_id}/{stamp}"
    uploaded: list[dict[str, str]] = []
    for name in files:
        path = out_dir / name
        if not path.is_file():
            continue
        key = f"{prefix}/{name}"
        url = put_object(
            bucket=bucket,
            key=key,
            body=path.read_bytes(),
            content_type=content_type_for(path),
        )
        uploaded.append(
            {
                "file": name,
                "label": FILE_LABELS.get(name, name),
                "key": key,
                "url": url,
            }
        )

    if not uploaded:
        raise S3UploadError(f"no files to upload in {out_dir} (wanted {files})")

    analysis_url = next(
        (u["url"] for u in uploaded if u["file"] == "analysis.md"),
        uploaded[0]["url"],
    )
    meta: dict[str, Any] = {
        "bucket": bucket,
        "prefix": prefix,
        "run_id": run_id,
        "stamp": stamp,
        "files": uploaded,
        "analysis_url": analysis_url,
        "links_md": format_duty_report_links(uploaded),
        "faktura_row": format_duty_report_faktura_row(uploaded),
    }
    (out_dir / "s3_report.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    return meta


def format_duty_report_links(files: list[dict[str, str]]) -> str:
    """Human markdown links: ``[полный отчёт](url) · [result](url) · [problems](url)``."""
    order = {"analysis.md": 0, "result.json": 1, "problems.json": 2}
    items = sorted(
        (f for f in files if f.get("file") and f.get("url")),
        key=lambda f: (order.get(str(f["file"]), 99), str(f["file"])),
    )
    parts: list[str] = []
    for f in items:
        label = str(f.get("label") or FILE_LABELS.get(str(f["file"]), f["file"]))
        parts.append(f"[{label}]({f['url']})")
    return " · ".join(parts)


def format_duty_report_faktura_row(files: list[dict[str, str]]) -> str:
    """One Фактура table row with human links."""
    return f"| Duty report | {format_duty_report_links(files)} |"


def duty_report_run_id_in_body(body: str) -> str | None:
    """Run id from existing Фактура Duty report S3 URL, if any."""
    m = re.search(
        r"^\|\s*Duty report\s*\|.*?duty_artifacts/([^/]+)/",
        body or "",
        re.M,
    )
    return m.group(1) if m else None


def upsert_duty_report_in_body(
    body: str,
    files: list[dict[str, str]],
    *,
    replace_existing: bool = True,
    run_id: str | None = None,
) -> str:
    """Insert or replace ``| Duty report | … |`` in issue/Materials body.

    If ``replace_existing`` is False and the body already has a Duty report for a
    *different* ``run_id``, leave the primary Фактура alone (sightings belong in
    comments / ``--sighting-from``, not by overwriting the opening report).
    """
    row = format_duty_report_faktura_row(files)
    existing_run = duty_report_run_id_in_body(body)
    if (
        not replace_existing
        and existing_run
        and run_id
        and existing_run != run_id
    ):
        return body

    replaced, n = re.subn(
        r"^\|\s*Duty report\s*\|.*\|\s*$",
        row,
        body,
        count=1,
        flags=re.M,
    )
    if n:
        return replaced

    lines = body.splitlines(keepends=True)
    out: list[str] = []
    inserted = False
    for ln in lines:
        out.append(ln)
        if inserted:
            continue
        if re.match(r"^\|\s*Allure\s*\|", ln):
            nl = "\n" if ln.endswith("\n") else ""
            out.append(row + (nl or "\n"))
            inserted = True
            continue
        if re.match(r"^\|\s*Failed\s*\|", ln):
            out.pop()
            nl = "\n" if ln.endswith("\n") else ""
            out.append(row + (nl or "\n"))
            out.append(ln)
            inserted = True
    if not inserted:
        out.append(("\n" if out and not str(out[-1]).endswith("\n") else "") + row + "\n")
    return "".join(out)


def detect_issue_number(out_dir: Path) -> int | None:
    """Find GitHub issue # from analysis.md / problems.json / result.json."""
    out_dir = Path(out_dir)
    # problems.json / result.json structured fields first
    for name in ("problems.json", "result.json"):
        p = out_dir / name
        if not p.is_file():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        items = []
        if isinstance(data, dict):
            items = list(data.get("items") or data.get("problems", {}).get("items") or [])
            for key in ("related_issue", "issue", "ticket"):
                n = _coerce_issue(data.get(key))
                if n:
                    return n
        for it in items:
            if not isinstance(it, dict):
                continue
            for key in ("related_issue", "issue", "ticket"):
                n = _coerce_issue(it.get(key))
                if n:
                    return n

    analysis = out_dir / "analysis.md"
    if not analysis.is_file():
        return None
    text = analysis.read_text(encoding="utf-8")
    # Prefer explicit ticket lines / github issue URLs
    patterns = (
        r"https://github\.com/ydb-platform/ydb/issues/(\d+)",
        r"(?i)\*\*Тикет:\*\*[^\n#]*#(\d+)",
        r"(?i)Тикет:\s*\[?#(\d+)",
        r"(?i)Связанный issue:\s*\[?#(\d+)",
        r"\[#(\d+)\]\(https://github\.com/ydb-platform/ydb/issues/\1\)",
    )
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return int(m.group(1))
    return None


def _coerce_issue(val: Any) -> int | None:
    if val is None:
        return None
    if isinstance(val, int) and val > 0:
        return val
    if isinstance(val, str):
        m = re.search(r"(\d+)", val)
        if m:
            n = int(m.group(1))
            if n > 0:
                return n
    if isinstance(val, dict):
        for key in ("number", "id", "url"):
            n = _coerce_issue(val.get(key))
            if n:
                return n
    return None


def has_duty_report_row(body: str) -> bool:
    return bool(re.search(r"^\|\s*Duty report\s*\|", body, re.M))


def has_human_duty_report_links(body: str) -> bool:
    """True if Фактура Duty report uses human labels (not bare harness filenames)."""
    m = re.search(r"^\|\s*Duty report\s*\|\s*(.+?)\s*\|\s*$", body, re.M)
    if not m:
        return False
    cell = m.group(1)
    return bool(re.search(r"\[полный отчёт\]\([^)]+\)", cell))
