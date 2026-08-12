"""Lint analysis.md — structure + quality gate (logs, mechanism, since, bisect)."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

_PTS = Path(__file__).resolve().parents[2]
if str(_PTS) not in sys.path:
    sys.path.insert(0, str(_PTS))

from common.duty_issues import parse_match_block  # noqa: E402

from .run_dir import read_json
from .s3_upload import detect_issue_number, has_human_duty_report_links
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


def _fenced_blocks(text: str) -> list[str]:
    # GitHub accepts ``` and ~~~; agents sometimes use either.
    return re.findall(
        r"(?:```|~~~)(?:[^\n]*)\n(.*?)(?:```|~~~)",
        text or "",
        flags=re.S,
    )


# GFM accepts "|--|--|" (2+ dashes); do not require ---.
_GFM_TABLE_SEP = re.compile(
    r"^\|[ \t]*:?-{2,}:?[ \t]*\|[ \t]*:?-{2,}:?[ \t]*\|",
    re.M,
)
_PIPE_DATA_ROW = re.compile(
    r"^\|(?!\s*:?-+:?\s*\|)[^|\n]+\|[^|\n]+\|\s*$",
    re.M,
)


def _section_needs_gfm_table_header(section: str) -> bool:
    """True when section has pipe rows but no GFM separator (GitHub won't render)."""
    if not section or not _PIPE_DATA_ROW.search(section):
        return False
    return not _GFM_TABLE_SEP.search(section)


def _nodata_query_names(out_dir: Path | None) -> set[str]:
    """Query names that must land in issue affected after abort/cut-off nodata.

    Prefer the full Now gap list from ``ticket_coverage.uncovered_queries`` /
    ``selection.focus_run.uncovered_queries`` (includes suite tails and
    ``Infrastructure error``). Also fold pack ``queries`` kind=nodata and
    detect/problems seeds — older packs truncated nodata to 24 in ``queries[]``.
    """
    if out_dir is None:
        return set()
    names: set[str] = set()

    def _add(q: Any) -> None:
        if isinstance(q, str) and q.strip():
            names.add(q.strip())
        elif isinstance(q, dict):
            for k in ("test", "query", "name"):
                v = q.get(k)
                if isinstance(v, str) and v.strip():
                    names.add(v.strip())
                    return

    ctx_path = out_dir / "context.json"
    if ctx_path.is_file():
        ctx = read_json(ctx_path)
        for q in ctx.get("queries") or []:
            if not isinstance(q, dict):
                continue
            kind = str(q.get("kind") or "").lower()
            if kind in ("nodata", "missing", "no_data"):
                _add(q)
        # Full suite gaps (may be longer than truncated queries[] samples).
        tc = ctx.get("ticket_coverage") or {}
        if isinstance(tc, dict):
            for q in tc.get("uncovered_queries") or []:
                _add(q)
        sel = ctx.get("selection") or {}
        fr = (sel.get("focus_run") or {}) if isinstance(sel, dict) else {}
        if isinstance(fr, dict):
            for q in fr.get("uncovered_queries") or []:
                _add(q)

    det_path = out_dir / "detect_type.json"
    if det_path.is_file():
        det = read_json(det_path)
        for seed in det.get("problems_seed") or []:
            if not isinstance(seed, dict):
                continue
            at = str(seed.get("analysis_type") or "").lower()
            title = str(seed.get("title") or "").lower()
            if "nodata" in at or "nodata" in title or "no data" in title:
                _add(seed)
                for q in seed.get("queries") or seed.get("tests") or []:
                    _add(q)
        for q in det.get("nodata_queries") or []:
            _add(q)
        tc = det.get("ticket_coverage") or {}
        if isinstance(tc, dict):
            for q in tc.get("uncovered_queries") or []:
                _add(q)

    probs_path = out_dir / "problems.json"
    if probs_path.is_file():
        probs = read_json(probs_path)
        items = probs if isinstance(probs, list) else list((probs or {}).get("items") or [])
        for it in items:
            if not isinstance(it, dict):
                continue
            at = str(it.get("analysis_type") or "").lower()
            title = str(it.get("title") or "").lower()
            if "nodata" in at or "nodata" in title or "no data" in title:
                _add(it)
                for q in it.get("queries") or it.get("tests") or []:
                    _add(q)
    return names


def _match_affected_queries(match: dict[str, Any] | None) -> set[str]:
    out: set[str] = set()
    if not match:
        return out
    for a in match.get("affected") or []:
        if not isinstance(a, dict):
            continue
        for q in a.get("queries") or []:
            if isinstance(q, str) and q.strip():
                out.add(q.strip())
    return out


_UNCHANGED_PATH_BOILERPLATE = re.compile(
    r"(?:path|файл|fline|crash\s*path|место\s*падени)\s*"
    r"(?:из\s*трейс\w*\s*)?(?:не\s*менял|не\s*изменял|unchanged)|"
    r"(?:не\s*менял(?:ся|ись)?|не\s*изменял(?:ся|ись)?)\s*"
    r"(?:в\s*окне|в\s*pr[_\s-]?window|между)|"
    r"bisect\s*(?:path\s*)?unchanged|"
    r"introduced_in_window\s*[:=]\s*false",
    re.I,
)


def _focus_has_crash_signals(out_dir: Path | None) -> bool:
    if out_dir is None or not (out_dir / "focus.json").is_file():
        return False
    focus = read_json(out_dir / "focus.json")
    sigs: set[str] = set()
    for s in (focus.get("fatal") or {}).get("signals") or []:
        sigs.add(str(s).lower())
    for c in (focus.get("allure") or {}).get("cases") or []:
        for s in ((c.get("attach_analysis") or {}).get("signals") or []):
            sigs.add(str(s).lower())
    crashish = {
        "segfault",
        "abort",
        "verify",
        "sigsegv",
        "sigabrt",
        "asan",
        "gwp-asan",
        "tsan",
        "ubsan",
    }
    return bool(sigs & crashish)


def _check_rca_sections(body: str) -> list[str]:
    """Require one winning hypothesis + causes + how-to-fix; reject unchanged-path boilerplate."""
    errs: list[str] = []
    has_hyps = bool(
        re.search(
            r"гипотез\w*\s*происхожден|##\s*гипотез|origin\s*hypothes|"
            r"\*\*H[123]\*\*|H1\s*\(|H1:",
            body,
            re.I,
        )
    )
    # Competing open/confirmed H1..H3 in the final report — keep only the winner (RCA.md).
    active_hs = re.findall(
        r"\*\*H([123])\*\*\s*\((?:открыта|подтверждена|наиболее\s*вероятн\w*)\)",
        body,
        re.I,
    )
    if len(set(active_hs)) > 1:
        errs.append(
            "RCA: leave only one most-probable hypothesis in analysis "
            "(discard competing H1/H2/H3) — see RCA.md"
        )
    has_blob = bool(
        re.search(
            r"github\.com/ydb-platform/ydb/blob/[0-9a-f]{7,40}/",
            body,
            re.I,
        )
    )
    # blob OR path under ydb/ with commit link nearby is ok for origin code
    has_code_link = has_blob or bool(
        re.search(
            r"ydb/(?:core|library)/[^\s\)]+"
            r".{0,120}github\.com/ydb-platform/ydb/commit/",
            body,
            re.I | re.S,
        )
        or re.search(
            r"github\.com/ydb-platform/ydb/commit/[0-9a-f]{7,40}"
            r".{0,200}ydb/(?:core|library)/",
            body,
            re.I | re.S,
        )
    )
    has_status = bool(
        re.search(r"\*\*Проблема:\*\*", body)
        and re.search(r"\*\*Из[‑-]за чего:\*\*", body)
        and re.search(r"\*\*Чинить:\*\*", body)
    )
    if not has_status:
        errs.append(
            "RCA: missing human status «Проблема / Из‑за чего / Чинить» — see RCA.md"
        )
    if not has_hyps:
        errs.append(
            "RCA: missing «Гипотезы происхождения» (one winning H + origin code) — see RCA.md"
        )
    elif not has_code_link:
        errs.append(
            "RCA: hypothesis must link origin code at tested sha "
            "(github …/blob/<sha>/… or path + commit link) — not detection frame alone"
        )
    has_issue_search = bool(
        re.search(
            r"(?:поиск|искал|search).{0,80}(?:issue|тикет|gh\s+search|known-issues)|"
            r"Issues\s*\(поиск\)|"
            r"уч[её]л\w*\s+(?:issue|тикет)|"
            r"соседн\w+\s+issue",
            body,
            re.I | re.S,
        )
    )
    if has_hyps and not has_issue_search:
        errs.append(
            "RCA: note GitHub issue search beyond Linked/related "
            "(symbols / path / fingerprint / suite) — see RCA.md"
        )

    has_causes = bool(
        re.search(
            r"##\s*причин|\*\*причин|#\s*причин|"
            r"suspect\s*pr|кандидат\w*\s*pr|цепочк\w*\s*изменен",
            body,
            re.I,
        )
    )
    if not has_causes:
        errs.append(
            "RCA: missing «Причины» (suspect PR / change sequence / what exposed the defect) "
            "— see RCA.md"
        )
    else:
        # Causes section must not be only "path unchanged" boilerplate.
        causes_m = re.search(
            r"(?:##\s*Причины|\*\*Причины\*\*|Причины\s*:)(.*?)(?=^##\s|\Z)",
            body,
            re.I | re.M | re.S,
        )
        causes_text = causes_m.group(1) if causes_m else ""
        # Also scan a short window after the heading word if section parse failed
        if not causes_text.strip():
            causes_text = body
        only_boilerplate = bool(_UNCHANGED_PATH_BOILERPLATE.search(causes_text))
        has_substance = bool(
            re.search(
                r"github\.com/ydb-platform/ydb/(?:pull|commit)/|"
                r"\[\s*#\d+\s*\]\s*\(|"
                r"прояви|давн\w*\s*дефект|ownership|writer|raw\s*ptr|"
                r"гонка|data\s*race|переполн|use-after-free|UAF|"
                r"snapshot|shared_ptr|lifetime|FillTask|producer",
                causes_text,
                re.I,
            )
        )
        if only_boilerplate and not has_substance:
            errs.append(
                "RCA: «Причины» must not be only «path/file unchanged in window» — "
                "dig writers/producers wider than the detection stack (RCA.md)"
            )

    has_fix = bool(
        re.search(
            r"##\s*как\s*починить|как\s*починить|suggested\s*fix|"
            r"направлен\w*\s*фикс|чтобы\s*починить|fix-direction|"
            r"решающ\w*\s*эксперимент",
            body,
            re.I,
        )
    )
    if not has_fix:
        errs.append(
            "RCA: missing «Как починить» (fix direction or decisive experiment) — see RCA.md"
        )
    return errs


def _check_issue_crash_paste(details: str, *, resolution: str) -> list[str]:
    """Quality gate for #### Детали ошибки in Materials (open_ticket / update_known).

    Forbid truncated backtraces («#7 … #16») and «filter URL в descriptionHtml»
    placeholders; require a real coredumps.yandex-team.ru link when the paste
    quotes SIGSEGV / Received signal / Backtrace.
    """
    errs: list[str] = []
    if not details:
        return errs
    dl = details.lower()
    has_crash_quote = bool(
        re.search(
            r"Received signal\s*\d+|SIGSEGV|SIGABRT|Backtrace\s*:",
            details,
            re.I,
        )
    )
    has_core_url = bool(
        re.search(r"coredumps\.yandex-team\.ru/v3/cores", details, re.I)
    )
    if re.search(
        r"descriptionhtml|filter\s*url\s*в|coredump:\s*filter\b|"
        r"filter url в description",
        dl,
    ):
        errs.append(
            f"{resolution}: #### Детали ошибки — paste a real "
            "`https://coredumps.yandex-team.ru/v3/cores…` URL from "
            "`focus.fatal.coredump_urls` / case `host_dig`, not "
            "«filter URL в descriptionHtml»"
        )
    if has_crash_quote and not has_core_url:
        if not re.search(
            r"coredump skipped|без coredump|coredump не|нет coredump|"
            r"uuid нет|coredump url отсутствует",
            dl,
        ):
            errs.append(
                f"{resolution}: #### Детали ошибки quotes signal/Backtrace — "
                "include clickable `coredumps.yandex-team.ru/v3/cores…` "
                "(from focus.fatal / attach_analysis.host_dig), or explicitly "
                "«coredump skipped» with why"
            )

    for fence in _fenced_blocks(details):
        if not re.search(r"Backtrace\s*:|Received signal\s*\d+", fence, re.I):
            continue
        # Ellipsis-only line or "#N … #M" cut inside the stack paste
        if re.search(r"^\s*[.……]{1,3}\s*$", fence, re.M) or re.search(
            r"#\d+[^\n]*\n\s*[.……]{1,3}\s*\n\s*#\d+",
            fence,
        ):
            errs.append(
                f"{resolution}: #### Детали ошибки — do not truncate Backtrace "
                "with «…» / «...»; paste full stack from kikimr__stderr "
                "(#0 … last frame)"
            )
        frames = [int(n) for n in re.findall(r"^#(\d+)\b", fence, re.M)]
        if len(frames) < 8:
            errs.append(
                f"{resolution}: #### Детали ошибки — Backtrace too short "
                f"({len(frames)} frames); paste full kikimr__stderr stack "
                "(#0 … last), not 3–4 key frames"
            )
        elif frames:
            span = max(frames) - min(frames) + 1
            # e.g. #5,#6,#7,#16 → span 12, only 4 frames present
            if span >= 10 and len(frames) < span * 0.6:
                errs.append(
                    f"{resolution}: #### Детали ошибки — Backtrace has gaps "
                    f"(frames {min(frames)}…{max(frames)} but only "
                    f"{len(frames)} lines); paste contiguous #0…#N from stderr"
                )
    return errs


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

    # Заключение: human status (Проблема / Из‑за чего / Чинить) replaces Итог+Механика.
    has_human_status = bool(
        re.search(r"\*\*Проблема:\*\*", body)
        and re.search(r"\*\*Из[‑-]за чего:\*\*", body)
        and re.search(r"\*\*Чинить:\*\*", body)
    )
    if not has_human_status:
        if not re.search(r"\*\*Итог:\*\*", body) and not re.search(
            r"\*\*Summary:\*\*", body, re.I
        ):
            errors.append(
                "Заключение must include **Проблема / Из‑за чего / Чинить** "
                "(or legacy **Итог:**) — see RCA.md"
            )
        if not re.search(r"\*\*(?:Механика|Mechanism):\*\*", body, re.I):
            errors.append(
                "Заключение must include **Проблема / Из‑за чего / Чинить** "
                "(or legacy **Механика:**) — see RCA.md"
            )
    resolution_token: str | None = None
    res_m = re.search(
        r"\*\*(?:Решение|Resolution):\*\*\s*`?([a-z_]+)`?",
        body,
        re.I,
    )
    if not res_m:
        errors.append("Заключение must include **Решение:** with a resolution token")
    else:
        resolution_token = res_m.group(1).lower()
        if resolution_token not in RESOLUTIONS:
            errors.append(
                f"unknown resolution `{resolution_token}`; want one of {', '.join(RESOLUTIONS)}"
            )

    if not re.search(r"\*\*(?:Виновник|Culprit):\*\*", body, re.I):
        errors.append("Заключение must include **Виновник:** / **Culprit:** (use unknown if none)")
    if not re.search(r"\*\*(?:Уверенность|Confidence):\*\*", body, re.I):
        errors.append("Заключение must include **Уверенность:** / **Confidence:**")

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
        errors.extend(_check_rca_sections(body))

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

            # no_action forbidden for IC/disconnect cascade without process abort
            if resolution_token == "no_action" and (out_dir / "focus.json").is_file():
                focus = read_json(out_dir / "focus.json")
                fatal = focus.get("fatal") or {}
                sigs = {str(s).lower() for s in (fatal.get("signals") or [])}
                for c in (focus.get("allure") or {}).get("cases") or []:
                    for s in ((c.get("attach_analysis") or {}).get("signals") or []):
                        sigs.add(str(s).lower())
                abortish = bool(
                    sigs
                    & {
                        "segfault",
                        "abort",
                        "verify",
                        "sigsegv",
                        "sigabrt",
                        "asan",
                        "gwp-asan",
                    }
                ) or bool(
                    re.search(
                        r"VERIFY failed|AFL_VERIFY|Received signal\s+[116]|GWP-ASan|"
                        r"Mismatched-size-class",
                        body,
                    )
                )
                cascade_only = bool(
                    sigs & {"disconnect", "unavailable", "restart", "node_down"}
                ) or bool(
                    re.search(
                        r"DeadPeer|connection closed by peer|YDBE-02001|"
                        r"detected disconnected node|INTERCONNECT_",
                        body,
                    )
                )
                stderr_empty = bool(
                    re.search(r"stderr empty|stderr пуст|kikimr__stderr[^\n]{0,40}пуст", bl)
                )
                if cascade_only and not abortish and stderr_empty:
                    errors.append(
                        "no_action forbidden: only IC/disconnect cascade with empty stderr — "
                        "каскад ≠ корневая причина; use wait_next_wave "
                        "(see AGENTS.md «Disconnect / IC cascade»)"
                    )

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

    # Compare heatmap: must dig compare.run, not only focus_run / now.
    compare_active = False
    compare_label = ""
    compare_sha = ""
    compare_fail_names: list[str] = []
    if out_dir:
        for name in ("detect_type.json", "context.json"):
            p = out_dir / name
            if not p.is_file():
                continue
            blob = read_json(p)
            if name == "detect_type.json":
                compare_active = bool(blob.get("compare_active"))
                compare_label = str(blob.get("compare_label") or "")
                compare_sha = str(blob.get("compare_sha") or "")
            cmp = blob.get("compare") if isinstance(blob.get("compare"), dict) else {}
            if cmp.get("active") or cmp.get("wave_id"):
                compare_active = True
            if not compare_label:
                compare_label = str(
                    cmp.get("label") or (cmp.get("run") or {}).get("label") or ""
                )
            if not compare_sha:
                compare_sha = str((cmp.get("run") or {}).get("sha") or "")
            for q in cmp.get("queries") or []:
                if isinstance(q, dict) and str(q.get("kind") or "") in ("fail", "both"):
                    t = str(q.get("test") or "").strip()
                    if t and t not in compare_fail_names:
                        compare_fail_names.append(t)
            for seed in blob.get("problems_seed") or []:
                if not isinstance(seed, dict):
                    continue
                if str(seed.get("source") or "") == "compare.run" and str(
                    seed.get("analysis_type") or ""
                ) == "olap_fail":
                    t = str(seed.get("test") or "").strip()
                    if t and t not in compare_fail_names:
                        compare_fail_names.append(t)
    if compare_active:
        if not re.search(
            r"прогон\s+сравнения|compare\.run|compare_focus|heatmap\s+cmp|"
            r"compare\s+active|cmp\s+прогон",
            bl,
        ):
            errors.append(
                "compare.active: analysis must dig compare.run (прогон сравнения) "
                "— not only selection.focus_run / now. Mention «прогон сравнения» "
                "+ label/sha and Allure outcome."
            )
        if compare_label and compare_label not in body and (
            not compare_sha or compare_sha[:7] not in body
        ):
            errors.append(
                f"compare.active: mention compare label `{compare_label}` "
                f"or sha `{compare_sha[:7] if compare_sha else '?'}` in analysis.md"
            )
        if compare_fail_names and not any(n.lower() in bl for n in compare_fail_names):
            errors.append(
                "compare.active: discuss failing compare query(ies) "
                + ", ".join(compare_fail_names[:6])
            )
        if compare_fail_names and not re.search(
            r"kikimr__stderr|segfault|sigsegv|sigabrt|received signal|"
            r"coredump|blob_cache|verify|в\s*отч[её]те",
            bl,
        ):
            errors.append(
                "compare.active with fail(s): dig compare Allure logs "
                "(kikimr__stderr / signal / coredump) — do not stop at pack fail_rate"
            )
        if out_dir and not (out_dir / "compare_focus.json").is_file():
            if not re.search(r"compare_focus skipped|compare\.run без report", bl):
                errors.append(
                    "compare.active: missing compare_focus.json — re-run "
                    "`dutyctl prepare` (fetches compare.run Allure) or explain skip"
                )

    # Nodata: must discuss gap + report-first branch (lag vs real missing).
    lag_branch = False
    report_gap_branch = False
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
    # Crash-like TPC-C (fatal in focus) — same RCA gate as olap_fail (no sanitizer hardcode).
    if tpcc and not olap_fail and _focus_has_crash_signals(out_dir):
        errors.extend(_check_rca_sections(body))
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

    # «Что дальше» = for the human on duty — not agent playbook (gh search belongs in Materials only).
    next_sec = ""
    if re.search(r"^##\s+Что дальше\s*$", body_for_jargon, re.M):
        next_sec = re.split(r"^##\s+Что дальше\s*$", body_for_jargon, maxsplit=1, flags=re.M)[-1]
        next_sec = re.split(r"^##\s+\S", next_sec, maxsplit=1, flags=re.M)[0]
    if next_sec and re.search(
        r"перед\s+заведением|скопировать\s+\*\*Title\*\*|gh\s+search\s+issues|"
        r"dutyctl\s+\w+|создать\s+issue:\s*скопир",
        next_sec,
        re.I,
    ):
        errors.append(
            "jargon in report: чеклист агента в «Что дальше» — "
            "пиши шаги для дежурного (тикет / coredump)"
        )
    # Antipattern: "это не #N / не путать / не смешивать с #M"
    # Use \b before «не» so we don't match the «не» inside «окне».
    if re.search(
        r"(?<![А-Яа-яA-Za-z])не\s+смешивать\s+с|"
        r"(?<![А-Яа-яA-Za-z])не\s+путать\s+с|"
        r"(?<![А-Яа-яA-Za-z])это\s+\*{0,2}не\*{0,2}\s+\[?#\d|"
        r"(?<![А-Яа-яA-Za-z])не\s+тот\s+же\s+баг,\s+что\s+\[?#\d|"
        r"(?<![А-Яа-яA-Za-z])не\s+\[?#\d{3,}\].{0,60}(?<![А-Яа-яA-Za-z])не\s+\[?#\d{3,}",
        body_for_jargon,
        re.I,
    ):
        errors.append(
            "antipattern: do not write «это не #N» / «не путать с…» / «не смешивать с…» "
            "(Title fingerprint + match keys already separate bugs; "
            "link only the real related issue or write «нет»)"
        )

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
                # Body: short Фактура first; human sections; keys only in perf-duty-match
                if not re.search(r"^####\s+Фактура\s*$", body, re.I | re.M):
                    errors.append(
                        f"{resolution}: ### Body must include #### Фактура "
                        "(short table: suite/db, branch·version, run, Allure, failed) "
                        "— see REPORT_TEMPLATE.md"
                    )
                else:
                    facts = _issue_body_section(body, "Фактура")
                    fl = facts.lower()
                    if _section_needs_gfm_table_header(facts):
                        errors.append(
                            f"{resolution}: Фактура table needs GFM header "
                            "`| | |` + `|--|--|` before data rows "
                            "(without separator GitHub does not render a table) "
                            "— see REPORT_TEMPLATE.md"
                        )
                    if not re.search(r"\bbranch\b|ветк", fl):
                        errors.append(
                            f"{resolution}: Фактура must include Branch / ветка запуска"
                        )
                    if not re.search(
                        r"ci\s*version|trunk\.r\d+|version\s*\||\bmain\.[0-9a-f]{7,}\b|"
                        r"github\.com/ydb-platform/ydb/commit/[0-9a-f]{7,}",
                        fl,
                    ):
                        errors.append(
                            f"{resolution}: Фактура must include Version / commit sha "
                            "(e.g. main.<sha> or commit link)"
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
                    if re.search(r"search\s*keys|ключ\w*\s*поиска|gh\s+search", fl):
                        warnings.append(
                            f"{resolution}: keep Search keys / gh search out of Фактура "
                            "(put tokens in <!-- perf-duty-match --> keys:)"
                        )
                if re.search(r"^####\s+Отчёты\s*\(соседи\)", body, re.I | re.M):
                    warnings.append(
                        f"{resolution}: omit #### Отчёты (соседи) from issue Body "
                        "(see REPORT_TEMPLATE.md)"
                    )
                if not re.search(r"^####\s+Что сломалось\s*$", body, re.I | re.M):
                    errors.append(
                        f"{resolution}: ### Body must include #### Что сломалось"
                    )
                if not re.search(r"^####\s+К чему приводит\s*$", body, re.I | re.M):
                    errors.append(
                        f"{resolution}: ### Body must include #### К чему приводит"
                    )
                if not re.search(
                    r"^####\s+Из[‑-]за чего\s*$", body, re.I | re.M
                ):
                    errors.append(
                        f"{resolution}: ### Body must include #### Из‑за чего "
                        "(root cause in plain Russian) — see REPORT_TEMPLATE.md"
                    )
                if not re.search(r"^####\s+Чинить\s*$", body, re.I | re.M):
                    errors.append(
                        f"{resolution}: ### Body must include #### Чинить "
                        "(where/how to fix) — see REPORT_TEMPLATE.md"
                    )
                if not re.search(r"^####\s+Детали ошибки\s*$", body, re.I | re.M):
                    warnings.append(
                        f"{resolution}: ### Body should include #### Детали ошибки "
                        "(VERIFY/signal quote, not under details)"
                    )
                else:
                    details = _issue_body_section(body, "Детали ошибки")
                    errors.extend(
                        _check_issue_crash_paste(details, resolution=resolution)
                    )
                if not re.search(r"^####\s+Код\s*$", body, re.I | re.M):
                    warnings.append(
                        f"{resolution}: ### Body should include #### Код "
                        "(detection path + related issue links)"
                    )
                else:
                    code_sec = _issue_body_section(body, "Код")
                    if _section_needs_gfm_table_header(code_sec):
                        errors.append(
                            f"{resolution}: #### Код table needs GFM header "
                            "`| | |` + `|--|--|` before data rows "
                            "(without separator GitHub does not render a table) "
                            "— see REPORT_TEMPLATE.md"
                        )
                # Machine block for dashboard / cross-suite match
                match = parse_match_block(body)
                if not match:
                    errors.append(
                        f"{resolution}: ### Body must include <!-- perf-duty-match --> "
                        "with keys: and affected: (suite/db/queries) — see REPORT_TEMPLATE.md"
                    )
                else:
                    if not match.get("keys"):
                        errors.append(
                            f"{resolution}: perf-duty-match must list keys: "
                            "(stable error tokens, not only suite name)"
                        )
                    if not match.get("affected"):
                        errors.append(
                            f"{resolution}: perf-duty-match must list affected: "
                            "with at least one suite (+ db/queries)"
                        )
                    else:
                        bad_aff = [
                            a
                            for a in match["affected"]
                            if not a.get("suite")
                        ]
                        if bad_aff:
                            errors.append(
                                f"{resolution}: perf-duty-match affected entries need suite:"
                            )
                        # Fail + real report-gap nodata: nodata tail must be in affected
                        # (abort/cut-off consequence → same issue, else Now stays uncovered).
                        if olap_fail and olap_nodata and report_gap_branch:
                            nd_qs = _nodata_query_names(out_dir)
                            aff_qs = _match_affected_queries(match)
                            missing = sorted(nd_qs - aff_qs)
                            if nd_qs and missing:
                                errors.append(
                                    f"{resolution}: nodata after abort/cut-off must be in "
                                    "perf-duty-match affected (same issue) — missing "
                                    + ", ".join(missing[:12])
                                    + (
                                        f" (+{len(missing) - 12} more)"
                                        if len(missing) > 12
                                        else ""
                                    )
                                    + "; use ticket_coverage.uncovered_queries "
                                    "(not only queries[] sample) — "
                                    "annotate-issue --queries … --no-comment"
                                )

    # After the human created/pointed an issue: require S3 publish + Duty report in Фактура.
    if needs_paste and out_dir is not None:
        ticket_n = detect_issue_number(out_dir)
        s3_meta = out_dir / "s3_report.json"
        if ticket_n:
            if not s3_meta.is_file():
                errors.append(
                    f"{resolution}: issue #{ticket_n} mentioned but s3_report.json missing — "
                    "run `dutyctl upload-report -o $OUT` "
                    "(uploads + upserts Duty report into issue body)"
                )
            elif not has_human_duty_report_links(body):
                errors.append(
                    f"{resolution}: Фактура must include Duty report "
                    "with «[полный отчёт](…)» — re-run `dutyctl upload-report -o $OUT`"
                )
        elif not s3_meta.is_file():
            warnings.append(
                f"{resolution}: after creating the GitHub issue, run "
                "`dutyctl upload-report -o $OUT --issue N` "
                "(or put Тикет: [#N](url) in analysis and re-run without --issue)"
            )

    # open_ticket + known_issues.related_closed → must link those issues (not silent reopen).
    if resolution == "open_ticket" and out_dir is not None:
        ki_path = out_dir / "known_issues.json"
        if ki_path.is_file():
            try:
                ki = read_json(ki_path)
            except Exception:  # noqa: BLE001
                ki = {}
            related = list((ki or {}).get("related_closed") or [])
            missing_closed: list[str] = []
            for iss in related:
                num = iss.get("number") if isinstance(iss, dict) else None
                if not num:
                    continue
                n = int(num)
                linked = bool(
                    re.search(
                        rf"\[#{n}\]\(https://github\.com/ydb-platform/ydb/issues/{n}\)",
                        body,
                    )
                )
                if not linked:
                    missing_closed.append(f"#{n}")
            if missing_closed:
                errors.append(
                    "open_ticket: known_issues.related_closed must be linked in analysis "
                    "(Фактура «Related closed» / Чинить «заодно») — missing "
                    + ", ".join(missing_closed[:8])
                    + "; new ticket OK, but do not omit same-fingerprint closed issues"
                )
        else:
            warnings.append(
                "open_ticket: no known_issues.json — run "
                "`dutyctl known-issues --keys … -o $OUT` so related_closed can be linked"
            )

    # wait_next_wave: report must be on S3 so the dashboard can show a wait-next badge.
    if resolution_token == "wait_next_wave" and out_dir is not None:
        if not (out_dir / "s3_report.json").is_file():
            errors.append(
                "wait_next_wave: s3_report.json missing — "
                "run `dutyctl upload-report -o $OUT --no-issue` "
                "(publishes report + duty_decision for the dashboard badge)"
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
