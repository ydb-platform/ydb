"""Lint analysis.md — structure + quality gate (logs, mechanism, since, bisect)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .run_dir import read_json

REQUIRED_HEADINGS = (
    "## Заключение",
    "## Проблемы",
    "## Что дальше",
    "## Материалы для issue",
)

RESOLUTIONS = (
    "update_known",
    "open_ticket",
    "wait_next_wave",
    "investigate_further",
    "no_action",
)


def _analysis_types(out_dir: Path | None) -> list[str]:
    if not out_dir or not (out_dir / "detect_type.json").is_file():
        return []
    det = read_json(out_dir / "detect_type.json")
    return list(det.get("analysis_types") or [])


def validate_analysis_md(
    text: str,
    *,
    out_dir: Path | None = None,
) -> dict[str, Any]:
    """Return {ok, errors[], warnings[]}."""
    errors: list[str] = []
    warnings: list[str] = []
    body = text or ""
    bl = body.lower()
    types = _analysis_types(out_dir)
    olap_fail = "olap_fail" in types

    if not body.strip():
        return {"ok": False, "errors": ["analysis.md is empty"], "warnings": []}

    for h in REQUIRED_HEADINGS:
        if h not in body:
            errors.append(f"missing heading `{h}`")

    # Заключение required fields
    if not re.search(r"\*\*Итог:\*\*", body) and not re.search(r"\*\*Summary:\*\*", body, re.I):
        errors.append("Заключение must include **Итог:** (or **Summary:**)")
    res_m = re.search(
        r"\*\*(?:Решение|Resolution):\*\*\s*`?([a-z_]+)`?",
        body,
        re.I,
    )
    if not res_m:
        errors.append("Заключение must include **Решение:** with a resolution token")
    else:
        token = res_m.group(1).lower()
        if token not in RESOLUTIONS:
            errors.append(f"unknown resolution `{token}`; want one of {', '.join(RESOLUTIONS)}")

    if not re.search(r"\*\*(?:Виновник|Culprit):\*\*", body, re.I):
        errors.append("Заключение must include **Виновник:** / **Culprit:** (use unknown if none)")
    if not re.search(r"\*\*(?:Уверенность|Confidence):\*\*", body, re.I):
        errors.append("Заключение must include **Уверенность:** / **Confidence:**")

    # Mechanism + since-when (all kinds)
    if not re.search(r"\*\*(?:Механика|Mechanism):\*\*", body, re.I):
        errors.append(
            "Заключение must include **Механика:** — system behavior that led to the failure "
            "(not only an error fingerprint)"
        )
    if not re.search(r"\*\*(?:Давность|Since):\*\*", body, re.I):
        errors.append(
            "Заключение must include **Давность:** / **Since:** "
            "(first-fail / priors / last_touch / issue age)"
        )

    # OLAP: must discuss execution + cluster logs
    if olap_fail:
        has_stderr = bool(
            re.search(r"kikimr__stderr|stderr empty|нет stderr|stderr пуст", bl)
        )
        has_logs = bool(
            re.search(
                r"kikimr__logs|cluster log|лог(?:и)? кластера|logs empty|нет logs|логи пуст",
                bl,
            )
        )
        if not has_stderr:
            errors.append(
                "olap_fail: mention kikimr__stderr dig (or explicitly 'stderr empty') — "
                "execution/crash logs must not be ignored"
            )
        if not has_logs:
            errors.append(
                "olap_fail: mention kikimr__logs / cluster logs dig "
                "(or explicitly 'logs empty') — cluster logs must not be ignored"
            )
        if not re.search(r"код\s*\(|\*\*код|\bsha\b|focus.?sha|read\.cpp|ydb/core/", bl):
            errors.append(
                "olap_fail: tie root cause to code at tested sha "
                "(path/symbol under Код (sha …) or similar)"
            )

        # Artifacts quality gate
        if out_dir:
            if not (out_dir / "focus.json").is_file():
                errors.append("olap_fail: missing focus.json — run `dutyctl prepare`")
            else:
                focus = read_json(out_dir / "focus.json")
                cases = (focus.get("allure") or {}).get("cases") or []
                fatal = focus.get("fatal") or {}
                fetched_any = False
                for c in cases:
                    aa = c.get("attach_analysis") or {}
                    if aa.get("attachments_fetched"):
                        fetched_any = True
                        break
                if cases and not fetched_any and not fatal.get("signals") and focus.get("fetched"):
                    warnings.append(
                        "focus.json has failed cases but no attachments_fetched — "
                        "confirm prepare fetched kikimr__stderr/logs"
                    )
            if not (out_dir / "code_bisect.json").is_file():
                if not re.search(r"bisect skipped|без bisect|bisect не", bl):
                    errors.append(
                        "olap_fail: missing code_bisect.json — run `dutyctl bisect` "
                        "(or explain 'bisect skipped' in the report)"
                    )
            if not (out_dir / "priors.json").is_file():
                warnings.append("missing priors.json — давность may be weak; run prepare")

    # Surface 2005 without fatal
    if re.search(r"code:\s*2005|cluster unavailable|connection with node", bl):
        has_fatal = bool(
            re.search(r"verify failed|afl_verify|sigabrt|received signal|oom|segfault", bl)
        )
        stderr_note = bool(
            re.search(r"stderr empty|no crash|нет fatal|без fatal|kikimr__stderr", bl)
        )
        if olap_fail and not has_fatal and not stderr_note:
            errors.append(
                "olap_fail: do not stop at 2005/node-lost without VERIFY/abort evidence "
                "or an explicit note that stderr has no fatal / is empty"
            )

    # Confident culprit without proof markers
    cul_m = re.search(
        r"\*\*(?:Виновник|Culprit):\*\*\s*(.+)",
        body,
        re.I,
    )
    if cul_m:
        cul = cul_m.group(1).strip().lower()
        if cul and not cul.startswith("unknown") and "не найден" not in cul:
            if not re.search(r"доказательств|evidence|pr files|bisect|∩|intersect", bl):
                errors.append(
                    "Culprit named but no доказательство/evidence/bisect/PR files — "
                    "evidence bar not met (use unknown or add proof)"
                )

    # Bisect unchanged vs blaming focus PR
    if out_dir and (out_dir / "code_bisect.json").is_file():
        bis = read_json(out_dir / "code_bisect.json")
        if bis.get("introduced_in_window") is False:
            if re.search(r"focus.?wave pr|pr of focus", bl) and re.search(
                r"виновник:\s*(?!unknown)", bl
            ):
                # soft: only if they claim a specific PR as owner without "candidate"
                if re.search(r"виновник:\s*[^u\n]*#\d+", bl) and "candidate" not in bl and "кандидат" not in bl:
                    if bis.get("introduced_in_window") is False:
                        warnings.append(
                            "bisect: crash path unchanged in window — "
                            "do not present focus-wave PR as root introducer"
                        )

    lines = body.splitlines()
    if len(lines) > 120:
        warnings.append(f"analysis.md is long ({len(lines)} lines); prefer clarity ≤~80")

    if out_dir and (out_dir / "problems.json").is_file():
        probs = read_json(out_dir / "problems.json")
        items = probs if isinstance(probs, list) else list((probs or {}).get("items") or [])
        if items and "### P" not in body and "### p" not in body.lower():
            warnings.append("problems.json has items but analysis.md has no ### P1-style sections")

    # Hypothesis verification required in problems
    if "### P" in body or "### p" in body.lower():
        if not re.search(r"гипотеза проверена|hypothesis", bl):
            errors.append("Each problem should state Гипотеза проверена: yes|no|partial")

    # Human report language: no agent-internal English micro-phrases
    jargon = [
        (r"\bsticky\b", "sticky — убери"),
        (r"prev[-\s]?green", "prev-green — пиши прогон с label/датой"),
        (r"\bpriors?\b", "priors — пиши «прогон YYYY-MM-DD_sha»"),
        (r"focus[-\s]?wave|фокусн\w*\s+волн|PR фокус", "«фокус/волна» — пиши «разбираемый прогон» / «PR в том же commit»"),
        (r"(?i)\bfocus\b|фокусн|/\s*\*?\*?фокус|\|\s*\*\*фокус|\(фокус\)", "«фокус» — пиши «разбираемый прогон» + label"),
        (r"same[-\s]?wave", "same-wave — «тот же прогон»"),
        (r"\btwin\b", "twin — «связанный тикет»"),
        (r"\bsurface\b", "surface — «сообщение 2005»"),
        (r"statusMessage[-\s]?only", "statusMessage-only — «только текст ошибки Allure»"),
        (r"\bseed\b", "seed — не в отчёт"),
        (r"reclassif", "reclassified — убери ложную проблему"),
        (r"detect_type|metrics_delta", "баги harness не в analysis.md"),
        (r"(?i)\bRC\b|root cause", "RC/root cause — «корневая причина»"),
        (r"last\s*touch|касание\s+path|Crash path|crash-path", "last touch/Crash path — «место падения» / «когда файл меняли последний раз»"),
        (r"метрик[ауи]\s*«?зелён", "не «метрика зелёный» — объясни: в UI зелёный по метрике, по Allure с падениями"),
        (r"^\|?\s*раньше\s*\|", "«раньше» в таблице — поставь дату/label прогона"),
    ]
    for pat, hint in jargon:
        if re.search(pat, body, re.I):
            errors.append(f"jargon in report: {hint}")

    # Issue materials: sandbox report URL required
    if "## Материалы для issue" in body or "## Materials" in body:
        if not re.search(r"proxy\.sandbox\.yandex-team\.ru/\d+", body):
            errors.append(
                "Материалы для issue: нужен хотя бы один URL отчёта "
                "proxy.sandbox.yandex-team.ru/<id>/…"
            )
        if not re.search(r"github\.com/ydb-platform/ydb/(commit|blob|issues|pull)/", body):
            errors.append(
                "Материалы для issue: нужны ссылки на commit/blob/issue/PR на github.com/ydb-platform/ydb"
            )

    # Bare #123 / unlinked issue|PR — require markdown links
    no_links = re.sub(r"\[[^\]]*\]\([^)]+\)", "", body)
    bare_nums = re.findall(r"(?:^|[^/\w])#(\d+)\b", no_links)
    if bare_nums:
        errors.append(
            "issue/PR must be markdown links, not bare #"
            + ", #".join([""] + bare_nums[:6])
            + " — e.g. [#29944](https://github.com/ydb-platform/ydb/issues/29944)"
        )
    # Short sha in backticks without a nearby github commit/blob link
    sha_hits = re.findall(r"`([0-9a-f]{7,40})`", body, re.I)
    if sha_hits and "github.com/ydb-platform/ydb/commit/" not in body:
        warnings.append(
            "commit sha mentioned but no github.com/.../commit/ link — "
            "link shas: [`abc1234`](https://github.com/ydb-platform/ydb/commit/abc1234)"
        )

    return {"ok": not errors, "errors": errors, "warnings": warnings}
