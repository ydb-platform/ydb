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

# PTS root (parent of duty_agent) for shared duty_decisions helpers.
_PTS = Path(__file__).resolve().parents[2]
if str(_PTS) not in sys.path:
    sys.path.insert(0, str(_PTS))

from common.duty_decisions import (  # noqa: E402
    DECISIONS_PREFIX,
    INDEX_KEY,
    by_focus_key,
    empty_index,
    focus_key,
    merge_decision_into_index,
    normalize_index,
)

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


def get_object_bytes(
    *,
    bucket: str,
    key: str,
    endpoint: str | None = None,
    region: str = DEFAULT_REGION,
) -> bytes | None:
    """GET object body, or None if missing."""
    client = _boto3_client(endpoint=endpoint, region=region)
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        err = str(e)
        if "NoSuchKey" in err or "404" in err or "Not Found" in err:
            return None
        # botocore ClientError code
        code = getattr(e, "response", None)
        if isinstance(code, dict):
            err_code = (code.get("Error") or {}).get("Code")
            if err_code in ("NoSuchKey", "404", "NotFound"):
                return None
        raise S3UploadError(f"GetObject failed s3://{bucket}/{key}: {e}") from e
    body = resp.get("Body")
    if body is None:
        return None
    return body.read()


def get_object_json(
    *,
    bucket: str,
    key: str,
    endpoint: str | None = None,
    region: str = DEFAULT_REGION,
) -> Any | None:
    raw = get_object_bytes(bucket=bucket, key=key, endpoint=endpoint, region=region)
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise S3UploadError(f"invalid JSON at s3://{bucket}/{key}: {e}") from e


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
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            problems = data.get("problems")
            if isinstance(problems, list):
                items = list(problems)
            elif isinstance(problems, dict):
                items = list(problems.get("items") or [])
            else:
                items = list(data.get("items") or [])
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


def _read_json_file(path: Path) -> Any | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def resolution_from_out_dir(out_dir: Path) -> str | None:
    """Prefer result.json.resolution, else **Решение:** in analysis.md."""
    out_dir = Path(out_dir)
    result = _read_json_file(out_dir / "result.json")
    if isinstance(result, dict):
        res = str(result.get("resolution") or "").strip().lower()
        if res:
            return res
    analysis = out_dir / "analysis.md"
    if analysis.is_file():
        try:
            text = analysis.read_text(encoding="utf-8")
        except OSError:
            return None
        m = re.search(
            r"\*\*(?:Решение|Resolution):\*\*\s*`?([a-z_]+)`?",
            text,
            re.I,
        )
        if m:
            return m.group(1).lower()
    return None


def _focus_context_from_out_dir(out_dir: Path) -> dict[str, str]:
    """kind/branch/db/suite/label from context.json or result.context."""
    out_dir = Path(out_dir)
    kind = branch = db = suite = label = ""
    ctx = _read_json_file(out_dir / "context.json")
    if isinstance(ctx, dict):
        sel = ctx.get("selection") or {}
        fr = sel.get("focus_run") or {}
        kind = str((ctx.get("report") or {}).get("kind") or "")
        branch = str(sel.get("branch") or "")
        db = str(sel.get("db") or "")
        suite = str(sel.get("suite") or "")
        label = str(fr.get("label") or fr.get("day") or fr.get("sha") or "")
    if not all([kind, branch, db, suite, label]):
        result = _read_json_file(out_dir / "result.json")
        if isinstance(result, dict):
            stub = result.get("context") or {}
            if isinstance(stub, dict):
                kind = kind or str(stub.get("kind") or "")
                branch = branch or str(stub.get("branch") or "")
                db = db or str(stub.get("db") or "")
                suite = suite or str(stub.get("suite") or "")
                label = label or str(stub.get("focus_label") or "")
    return {
        "kind": kind.strip().lower(),
        "branch": branch.strip(),
        "db": db.strip(),
        "suite": suite.strip(),
        "label": label.strip(),
    }


def _summary_from_out_dir(out_dir: Path, *, max_len: int = 180) -> str:
    result = _read_json_file(Path(out_dir) / "result.json")
    if isinstance(result, dict):
        s = str(result.get("summary") or "").strip()
        if s:
            return s[:max_len]
    analysis = Path(out_dir) / "analysis.md"
    if analysis.is_file():
        try:
            text = analysis.read_text(encoding="utf-8")
        except OSError:
            return ""
        for pat in (
            r"\*\*Проблема:\*\*\s*(.+)",
            r"\*\*Итог:\*\*\s*(.+)",
            r"\*\*Summary:\*\*\s*(.+)",
        ):
            m = re.search(pat, text, re.I)
            if m:
                return m.group(1).strip()[:max_len]
    return ""


def _add_wait_next_query(name: str, out: list[str], seen: set[str]) -> None:
    q = str(name or "").strip()
    if not q or q in seen:
        return
    seen.add(q)
    out.append(q)


def _queries_for_wait_next(out_dir: Path) -> list[str]:
    """Query names under wait_next_wave for per-query dashboard badges.

    Collects ``test`` / ``query`` / ``sample`` / ``queries`` from wait_next
    problems. If none look like ``QueryNN`` (suite wipe / only
    ``Infrastructure error``), falls back to pack
    ``focus_run.uncovered_queries`` so the UI can attach ``wait next`` under
    nodata/fail rows (see ``waitNextAppliesToQuery`` in olap/template.html).
    """
    out_dir = Path(out_dir)
    problems = _read_json_file(out_dir / "problems.json")
    if isinstance(problems, dict):
        items = problems.get("items") or []
    elif isinstance(problems, list):
        items = problems
    else:
        items = []
    out: list[str] = []
    seen: set[str] = set()
    any_wait = False
    for it in items:
        if not isinstance(it, dict):
            continue
        if str(it.get("resolution") or "") != "wait_next_wave":
            continue
        any_wait = True
        _add_wait_next_query(str(it.get("test") or it.get("query") or ""), out, seen)
        for key in ("sample", "queries"):
            vals = it.get(key)
            if not isinstance(vals, list):
                continue
            for raw in vals:
                _add_wait_next_query(str(raw or ""), out, seen)
    has_query_nn = any(re.search(r"(?i)\bQuery\d+\b", q) for q in out)
    if any_wait and not has_query_nn:
        ctx = _read_json_file(out_dir / "context.json")
        if isinstance(ctx, dict):
            fr = ((ctx.get("selection") or {}).get("focus_run") or {})
            for raw in fr.get("uncovered_queries") or []:
                _add_wait_next_query(str(raw or ""), out, seen)
    return out


def build_wait_next_wave_decision(
    out_dir: Path,
    meta: dict[str, Any],
) -> dict[str, Any] | None:
    """Build decision record for dashboard badge, or None if focus incomplete."""
    focus = _focus_context_from_out_dir(out_dir)
    if not all(focus.get(k) for k in ("kind", "branch", "db", "suite", "label")):
        return None
    files = list(meta.get("files") or [])
    analysis_url = str(meta.get("analysis_url") or "")
    result_url = next(
        (str(f.get("url") or "") for f in files if f.get("file") == "result.json"),
        "",
    )
    problems_url = next(
        (str(f.get("url") or "") for f in files if f.get("file") == "problems.json"),
        "",
    )
    fk = focus_key(**focus)
    updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "schema": "perf-duty-decision/v1",
        "focus_key": fk,
        "resolution": "wait_next_wave",
        "kind": focus["kind"],
        "branch": focus["branch"],
        "db": focus["db"],
        "suite": focus["suite"],
        "label": focus["label"],
        "run_id": str(meta.get("run_id") or Path(out_dir).name),
        "stamp": str(meta.get("stamp") or ""),
        "analysis_url": analysis_url,
        "result_url": result_url,
        "problems_url": problems_url,
        "queries": _queries_for_wait_next(out_dir),
        "summary": _summary_from_out_dir(out_dir),
        "updated_at": updated,
        "pointer_key": by_focus_key(**focus),
    }


def publish_wait_next_wave_decision(
    out_dir: Path,
    meta: dict[str, Any],
    *,
    bucket: str = DEFAULT_BUCKET,
) -> dict[str, Any]:
    """Write by_focus pointer + merge into public decisions index.

    Raises ``S3UploadError`` on failure. Returns the decision record.
    """
    decision = build_wait_next_wave_decision(out_dir, meta)
    if not decision:
        raise S3UploadError(
            "wait_next_wave decision needs kind/branch/db/suite/label "
            "in context.json (selection) or result.json context stub"
        )
    if not decision.get("analysis_url"):
        raise S3UploadError("wait_next_wave decision missing analysis_url from upload meta")

    pointer_key = str(decision["pointer_key"])
    body = (json.dumps(decision, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    pointer_url = put_object(
        bucket=bucket,
        key=pointer_key,
        body=body,
        content_type="application/json",
    )

    existing = get_object_json(bucket=bucket, key=INDEX_KEY)
    index = merge_decision_into_index(
        normalize_index(existing) if existing is not None else empty_index(),
        decision,
        updated_at=str(decision.get("updated_at") or ""),
    )
    index_body = (json.dumps(index, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    index_url = put_object(
        bucket=bucket,
        key=INDEX_KEY,
        body=index_body,
        content_type="application/json",
    )
    decision = {
        **decision,
        "pointer_url": pointer_url,
        "index_url": index_url,
        "decisions_prefix": DECISIONS_PREFIX,
    }
    out_path = Path(out_dir) / "duty_decision.json"
    out_path.write_text(json.dumps(decision, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return decision


def maybe_publish_wait_next_wave_decision(
    out_dir: Path,
    meta: dict[str, Any],
    *,
    bucket: str = DEFAULT_BUCKET,
) -> dict[str, Any] | None:
    """If resolution is wait_next_wave, publish decision; else None."""
    if resolution_from_out_dir(out_dir) != "wait_next_wave":
        return None
    return publish_wait_next_wave_decision(out_dir, meta, bucket=bucket)
