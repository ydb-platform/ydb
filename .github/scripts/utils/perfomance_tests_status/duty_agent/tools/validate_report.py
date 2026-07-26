"""Lint analysis.md — structure + quality gate (logs, mechanism, since, bisect)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .run_dir import read_json
from .trace import TRACE_MARK_END, TRACE_MARK_START

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


def _issue_body_section(body: str, heading: str) -> str:
    """Text under #### {heading} until the next #### / ### heading."""
    m = re.search(
        rf"^####\s+{re.escape(heading)}\s*\n(.*?)(?=^####\s|^###\s|\Z)",
        body,
        re.I | re.M | re.S,
    )
    return m.group(1) if m else ""


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
    # Action-tree <details> may list artifact names (priors/focus) — not report jargon.
    body_for_jargon = body
    if TRACE_MARK_START in body_for_jargon and TRACE_MARK_END in body_for_jargon:
        body_for_jargon = (
            body_for_jargon.split(TRACE_MARK_START, 1)[0]
            + body_for_jargon.split(TRACE_MARK_END, 1)[1]
        )
    bl = body.lower()
    bl_jargon = body_for_jargon.lower()
    types = _analysis_types(out_dir)
    olap_fail = "olap_fail" in types
    olap_nodata = "olap_nodata" in types
    olap_slow = "olap_slow" in types
    # Also force nodata gate from context / detect query_counts (legacy packs).
    if out_dir and not olap_nodata:
        for name in ("detect_type.json", "context.json"):
            p = out_dir / name
            if not p.is_file():
                continue
            blob = read_json(p)
            qc = blob.get("query_counts") or (blob.get("suite_now") or {}).get("query_counts") or {}
            n_nd = qc.get("nodata") if isinstance(qc, dict) else None
            if n_nd is None:
                n_nd = (blob.get("suite_now") or {}).get("n_nodata")
            try:
                if n_nd is not None and int(n_nd) > 0:
                    olap_nodata = True
                    break
            except (TypeError, ValueError):
                pass
            if any(
                isinstance(q, dict) and str(q.get("kind") or "") in ("nodata", "missing")
                for q in (blob.get("queries") or [])
            ):
                olap_nodata = True
                break
            seed = blob.get("problems_seed") or []
            if any(str(s.get("analysis_type") or "") == "olap_nodata" for s in seed if isinstance(s, dict)):
                olap_nodata = True
                break

    if not body.strip():
        return {"ok": False, "errors": ["analysis.md is empty"], "warnings": []}

    for h in REQUIRED_HEADINGS:
        if h not in body:
            errors.append(f"missing heading `{h}`")

    # Action tree under <details> (inject-trace / validate refreshes it)
    if "duty-action-tree:start" not in body and "<details>" not in body.lower():
        if out_dir and (out_dir / "action_tree.json").is_file():
            warnings.append(
                "action_tree.json exists but analysis.md has no <details> cut — "
                "run `dutyctl inject-trace` (validate usually injects it)"
            )
        else:
            warnings.append(
                "no action-tree <details> in analysis.md — "
                "run `dutyctl inject-trace` so the dig path is under the cut"
            )

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

    # Slow / duration growth: must dig plans × iterations + baseline plan (not ydb_pct only).
    if olap_slow:
        has_plan = bool(
            re.search(
                r"\bplan\b|final plan|plan_dig|plan table|explain|"
                r"gracejoin|lookupjoin|fullscan|итерац",
                bl,
            )
        )
        has_iter = bool(
            re.search(
                r"iteration|итерац|across iterations|между итерац|plan_changed",
                bl,
            )
        )
        has_baseline_plan = bool(
            re.search(
                r"baseline|baseline_focus|базов\w*\s+план|план\s+на\s+сосед|"
                r"нормальн\w+\s+продолжит|plan_same|plan_regressed|plan_compare|"
                r"разъехал|совпал.*план|план.*совпал|"
                r"plan skipped|без plan|plans empty|план(?:а|ов)?\s+нет|"
                r"baseline skipped|без baseline|baseline не",
                bl,
            )
        )
        if not has_plan:
            errors.append(
                "olap_slow: discuss query plan dig (Final plan / Explain / plan_dig) — "
                "do not conclude from ydb_pct alone"
            )
        if not has_iter:
            errors.append(
                "olap_slow: compare plans/durations across iterations "
                "(or note single-iteration / plans empty)"
            )
        if not has_baseline_plan:
            errors.append(
                "olap_slow: compare plan to baseline_focus / dig-runs baseline_candidate "
                "(good historical run with Report) "
                "or explicitly note baseline skipped / unavailable"
            )
        if out_dir and (out_dir / "baseline_focus.json").is_file():
            bf = read_json(out_dir / "baseline_focus.json")
            if bf.get("fetched") and bf.get("plan_compare") and not re.search(
                r"plan_compare|plan_same|plan_regressed|baseline_focus|совпал|разъехал",
                bl,
            ):
                warnings.append(
                    "olap_slow: baseline_focus.json has plan_compare — mention verdict in analysis.md"
                )
        if out_dir and (out_dir / "focus.json").is_file():
            focus = read_json(out_dir / "focus.json")
            cases = (focus.get("allure") or {}).get("cases") or []
            slow_cases = [
                c
                for c in cases
                if c.get("want_plans")
                or c.get("role") == "slow"
                or (c.get("attach_analysis") or {}).get("plan_dig")
            ]
            if focus.get("fetched") and (focus.get("slow_query_names") or slow_cases):
                any_plan = any(
                    (c.get("attach_analysis") or {}).get("plan_dig") for c in cases
                )
                if not any_plan and not re.search(
                    r"plan skipped|plans empty|план(?:а|ов)?\s+нет|без plan", bl
                ):
                    warnings.append(
                        "olap_slow: focus has slow query names but no plan_dig — "
                        "re-run prepare (non-offline) or note plans empty"
                    )

    # Nodata: must discuss gap + report-first branch (lag vs real missing).
    if olap_nodata:
        if not re.search(
            r"no\s*data|nodata|successcount|success\s*count|покрыт|выгрузк|query_counts|n_nodata",
            bl,
        ):
            errors.append(
                "olap_nodata: analysis must discuss no-data / incomplete SuccessCount / "
                "coverage gap (do not treat suite as clean ok)"
            )
        if not re.search(r"allure|отч[её]т|sandbox\.yandex|proxy\.sandbox", bl):
            errors.append(
                "olap_nodata: first step is Allure/report check for the missing queries — "
                "mention the report outcome in analysis.md"
            )
        lag_branch = bool(
            re.search(
                r"не\s*доехал|ещ[её]\s*не\s*доехал|доехал|лаг\s*выгруз|в\s*базу|"
                r"в\s*отч[её]те\s*(вс[её]\s*)?(ok|ок|passed|успеш)|"
                r"отч[её]т\w*\s*(ok|ок|passed|зелён)",
                bl,
            )
        )
        report_gap_branch = bool(
            re.search(
                r"в\s*отч[её]те\s*(тоже\s*)?(нет|nodata|missing|fail|дыр)|"
                r"отч[её]т\w*\s*тоже\s*(нет|дыр|пуст)|"
                r"нет\s*в\s*allure|в\s*allure\s*(нет|fail|skipped)",
                bl,
            )
        )
        if not lag_branch and not report_gap_branch:
            errors.append(
                "olap_nodata: state the branch after report check — "
                "«в отчёте ok → в базу ещё не доехали» OR "
                "«в отчёте тоже нет → логи / журналы на кластере»"
            )
        if out_dir and (out_dir / "problems.json").is_file():
            probs = read_json(out_dir / "problems.json")
            items = probs if isinstance(probs, list) else list((probs or {}).get("items") or [])
            if items and not any(
                "nodata" in str(it.get("analysis_type") or "").lower()
                or "nodata" in str(it.get("title") or "").lower()
                or "no data" in str(it.get("title") or "").lower()
                for it in items
                if isinstance(it, dict)
            ):
                errors.append(
                    "olap_nodata: problems.json must include a nodata/no-data problem "
                    "(not only fail/slow seeds)"
                )

    # TPC-C / OLAP: must dig mart history (pack suite_history alone is not enough)
    tpcc = any(str(t).startswith("tpcc_") for t in types)
    olap_needs_dig = any(t in ("olap_slow", "olap_fail", "olap_nodata") for t in types)
    if (tpcc or olap_needs_dig) and out_dir:
        if not (out_dir / "dig_runs.json").is_file():
            if not re.search(r"dig-runs skipped|dig.runs не|без dig-runs", bl):
                kind = "tpcc" if tpcc else "olap"
                errors.append(
                    f"{kind}: missing dig_runs.json — run `dutyctl dig-runs` "
                    "(neighbors + ~35d via ydb_client; widen --days-before if edged) "
                    "or explain 'dig-runs skipped' with reason"
                )
    if (tpcc or olap_slow) and out_dir:
        if not (out_dir / "dig_prs.json").is_file():
            if not re.search(r"dig-prs skipped|dig.prs не|без dig-prs", bl):
                kind = "tpcc" if tpcc else "olap_slow"
                errors.append(
                    f"{kind}: missing dig_prs.json — run `dutyctl dig-prs` on the "
                    f"{'latency' if tpcc else 'ydb'} jump window "
                    "(or explain 'dig-prs skipped')"
                )
    if olap_slow and out_dir:
        if not (out_dir / "code_bisect.json").is_file():
            if not re.search(r"bisect skipped|без bisect|bisect не", bl):
                errors.append(
                    "olap_slow: missing code_bisect.json — run `dutyctl bisect` "
                    "on a path suggested by plan_compare/hot PR "
                    "(or explain 'bisect skipped')"
                )
        if not re.search(
            r"гипотеза проверена|hypothesis|plan_regressed|plan_same|"
            r"кандидат|dig_prs|hot pr|в окне",
            bl,
        ):
            warnings.append(
                "olap_slow: after plans, state hypothesis loop / dig-prs filtering "
                "(what could affect duration in the jump window)"
            )

    # Surface 2005 without fatal
    if re.search(r"code:\s*2005|cluster unavailable|connection with node", bl):
        has_fatal = bool(
            re.search(
                r"verify failed|afl_verify|sigabrt|sigsegv|received signal|"
                r"oom|segfault|coredump|backtrace",
                bl,
            )
        )
        stderr_note = bool(
            re.search(r"stderr empty|no crash|нет fatal|без fatal|kikimr__stderr", bl)
        )
        if olap_fail and not has_fatal and not stderr_note:
            errors.append(
                "olap_fail: do not stop at 2005/node-lost without VERIFY/abort evidence "
                "or an explicit note that stderr has no fatal / is empty"
            )

    # Segfault and/or explicit coredump URL → must dig dump (VERIFY/SIGABRT in stderr is enough without URL)
    need_coredump_dig = False
    if out_dir and (out_dir / "focus.json").is_file():
        focus = read_json(out_dir / "focus.json")
        fatal = focus.get("fatal") or {}
        sigs = {str(s).lower() for s in (fatal.get("signals") or [])}
        if "segfault" in sigs or fatal.get("coredump_urls"):
            need_coredump_dig = True
        for c in (focus.get("allure") or {}).get("cases") or []:
            aa = c.get("attach_analysis") or {}
            if "segfault" in {str(s).lower() for s in (aa.get("signals") or [])}:
                need_coredump_dig = True
            if (aa.get("host_dig") or {}).get("coredump_urls"):
                need_coredump_dig = True
    if olap_fail and need_coredump_dig:
        has_core_dig = bool(
            re.search(
                r"coredump|cores\.yandex|/place/coredumps|backtrace_kikimr|"
                r"traceback_fingerprint|url_v3|journalctl|unified_agent|"
                r"coredump skipped|без coredump|coredump не|host dig skipped",
                bl,
            )
        )
        if not has_core_dig:
            errors.append(
                "olap_fail: focus has SIGSEGV/segfault or coredump URL — "
                "dig coredumps.yandex-team.ru / /place/coredumps (or journalctl/"
                "unified_agent), or explicitly note coredump skipped"
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
    # Title+Body paste block makes reports longer; soft cap excludes that expectation
    if len(lines) > 160:
        warnings.append(f"analysis.md is long ({len(lines)} lines); prefer clarity ≤~120")

    if out_dir and (out_dir / "problems.json").is_file():
        probs = read_json(out_dir / "problems.json")
        items = probs if isinstance(probs, list) else list((probs or {}).get("items") or [])
        if items and "### P" not in body and "### p" not in body.lower():
            warnings.append("problems.json has items but analysis.md has no ### P1-style sections")

    # Hypothesis verification required in problems
    if "### P" in body or "### p" in body.lower():
        if not re.search(r"гипотеза проверена|hypothesis", bl):
            errors.append("Each problem should state Гипотеза проверена: yes|no|partial")

    # Заключение: avoid one mega-bullet (hard to scan / hard to paste into issue)
    for label in ("Итог", "Давность", "Виновник", "Summary", "Since", "Culprit"):
        m = re.search(rf"\*\*{label}:\*\*\s*(.+)", body)
        if m and len(m.group(1).strip()) > 420:
            warnings.append(
                f"Заключение **{label}:** too long (>420 chars) — "
                "split into short sentences; keep sha/PR history out of Итог"
            )

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
        if re.search(pat, body_for_jargon, re.I):
            errors.append(f"jargon in report: {hint}")

    # Issue materials: sandbox report URL (OLAP) or explicit metrics-only / DataLens (TPC-C)
    tpcc_only = bool(types) and all(t.startswith("tpcc_") for t in types)
    resolution = (res_m.group(1).lower() if res_m else "")
    needs_paste = resolution in ("open_ticket", "update_known")
    if "## Материалы для issue" in body or "## Materials" in body:
        has_sandbox = bool(re.search(r"proxy\.sandbox\.yandex-team\.ru/\d+", body))
        has_tpcc_alt = bool(
            re.search(
                r"datalens\.yandex|"
                r"sandbox/allure\s*(report\s*)?отсутств|"
                r"нет\s*\(TPC-C|"
                r"TPC-C metrics-only|"
                r"report:\s*null",
                body,
                re.I,
            )
        )
        if not has_sandbox and not (tpcc_only and has_tpcc_alt):
            errors.append(
                "Материалы для issue: нужен URL proxy.sandbox.yandex-team.ru/<id>/… "
                "(для TPC-C — DataLens и/или явно «Sandbox/Allure нет»)"
            )
        if not re.search(r"github\.com/ydb-platform/ydb/(commit|blob|issues|pull)/", body):
            errors.append(
                "Материалы для issue: нужны ссылки на commit/blob/issue/PR на github.com/ydb-platform/ydb"
            )
        # Copy-paste gate: Title + Body ready for GitHub
        if needs_paste:
            has_title = bool(
                re.search(r"^###\s+(Title|Заголовок)\s*$", body, re.I | re.M)
            )
            has_body = bool(re.search(r"^###\s+(Body|Тело)\s*$", body, re.I | re.M))
            if not has_title or not has_body:
                errors.append(
                    f"{resolution}: Материалы для issue must have ### Title and ### Body "
                    "(ready to paste into GitHub; see REPORT_TEMPLATE.md)"
                )
            else:
                title_m = re.search(
                    r"^###\s+(?:Title|Заголовок)\s*\n+(?:```[^\n]*\n)?([^\n`]+)",
                    body,
                    re.I | re.M,
                )
                if title_m and len(title_m.group(1).strip()) < 12:
                    errors.append(
                        f"{resolution}: ### Title must be a real issue title (≥12 chars), not a stub"
                    )
                # Body paste must lead with Фактура so the next agent can gh-search the issue
                if not re.search(r"^####\s+Фактура\s*$", body, re.I | re.M):
                    errors.append(
                        f"{resolution}: ### Body must include #### Фактура "
                        "(branch, Version/CI, suite@db, Allure URL, fingerprint, Search keys) "
                        "— see REPORT_TEMPLATE.md"
                    )
                else:
                    facts = _issue_body_section(body, "Фактура")
                    fl = facts.lower()
                    if not re.search(r"\bbranch\b|ветк", fl):
                        errors.append(
                            f"{resolution}: Фактура must include Branch / ветка запуска"
                        )
                    if not re.search(
                        r"ci\s*version|trunk\.r\d+|version\s*\||\bmain\.[0-9a-f]{7,}\b",
                        fl,
                    ):
                        errors.append(
                            f"{resolution}: Фактура must include Version / CI version "
                            "(e.g. main.<sha>, trunk.r…)"
                        )
                    if not re.search(r"proxy\.sandbox\.yandex-team\.ru/\d+", facts):
                        if not (tpcc_only and re.search(r"datalens|sandbox/allure|report:\s*null", fl)):
                            errors.append(
                                f"{resolution}: Фактура must include Allure/Sandbox report URL"
                            )
                    if not re.search(r"suite|db\b|cluster|db\s*/", fl):
                        errors.append(
                            f"{resolution}: Фактура must include Suite and DB/cluster"
                        )
                    if not re.search(
                        r"search\s*keys|ключ\w*\s*поиска|"
                        r"fline\s*=|"
                        r"\w+\.cpp:\d+|"
                        r"afl_verify|verification\s*=",
                        fl,
                    ):
                        errors.append(
                            f"{resolution}: Фактура must include Fingerprint / Search keys "
                            "(stable tokens for `gh search issues`, e.g. file.cpp:117)"
                        )
                if not re.search(r"^####\s+Кратко\s*$", body, re.I | re.M):
                    warnings.append(
                        f"{resolution}: ### Body should include #### Кратко "
                        "(1–3 sentences for the issue lead)"
                    )
                if not re.search(
                    r"^####\s+(Важно|Что важно)",
                    body,
                    re.I | re.M,
                ):
                    warnings.append(
                        f"{resolution}: ### Body should include #### Важно (3–5 bullets)"
                    )

    # Bare #123 / unlinked issue|PR — require markdown links
    # (skip fenced code: Title paste often has "#29944" as plain GitHub title text)
    no_fences = re.sub(r"```.*?```", "", body, flags=re.S)
    no_links = re.sub(r"\[[^\]]*\]\([^)]+\)", "", no_fences)
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
