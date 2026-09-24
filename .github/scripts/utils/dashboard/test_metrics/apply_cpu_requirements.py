"""Insert or update REQUIREMENTS(cpu:N) in ya.make content."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

try:
    from .ya_make_requirements import eval_ya_make_condition, normalize_suite_path, requirements_span
except ImportError:
    from ya_make_requirements import eval_ya_make_condition, normalize_suite_path, requirements_span

RE_REQ_LINE = re.compile(r"^(\s*REQUIREMENTS\s*\()(.*?)(\)\s*)$", re.DOTALL)
RE_REQ_OPEN = re.compile(r"^\s*REQUIREMENTS\s*\(", re.IGNORECASE)
RE_CPU = re.compile(r"\bcpu\s*:\s*([^\s)]+(?:\([^)]*\))?)", re.IGNORECASE)
RE_IF = re.compile(r"^\s*IF\s*\((.*)\)\s*$")
RE_ELSEIF = re.compile(r"^\s*ELSEIF\s*\((.*)\)\s*$")
RE_ELSE = re.compile(r"^\s*ELSE\s*\(\s*\)\s*$")
RE_ENDIF = re.compile(r"^\s*ENDIF\s*\(\s*\)\s*$")


def normalize_cpu_req(value: object) -> str:
    s = str(value).strip()
    if s.lower() == "all":
        return "all"
    try:
        return str(int(float(s)))
    except Exception:
        return s or "1"


def update_requirements_text(text: str, cpu: str) -> str:
    m = RE_REQ_LINE.match(text.rstrip("\n"))
    if not m:
        return text
    prefix, body, suffix = m.group(1), m.group(2), m.group(3)
    if RE_CPU.search(body):
        body = RE_CPU.sub("cpu:" + str(cpu), body, count=1)
    elif "\n" in body:
        indent = "        "
        for raw in body.splitlines():
            stripped = raw.strip()
            if stripped:
                indent = raw[: len(raw) - len(stripped)]
                break
        trimmed = body.rstrip()
        body = trimmed + "\n" + indent + "cpu:" + str(cpu) + "\n"
    else:
        body = (body.strip() + " " if body.strip() else "") + "cpu:" + str(cpu)
    return prefix + body + suffix


def _cond_mentions_sanitizer(cond: str) -> bool:
    return bool(re.search(r"\bSANITIZER_TYPE\b", cond or ""))


def _cond_mentions_valgrind_or_sanitizer(cond: str) -> bool:
    c = cond or ""
    return bool(re.search(r"\bWITH_VALGRIND\b", c)) or bool(re.search(r"\bSANITIZER_TYPE\b", c))


def _find_top_insert_index(lines: list[str]) -> Optional[int]:
    for i, raw in enumerate(lines):
        line = raw.strip()
        m = RE_IF.match(line)
        if m and _cond_mentions_valgrind_or_sanitizer(m.group(1)):
            return i
    return None


def _find_with_valgrind_else_span(lines: list[str]) -> Optional[tuple[int, int]]:
    depth = 0
    first_valgrind_idx: Optional[int] = None
    for i, raw in enumerate(lines):
        line = raw.strip()
        m_if = RE_IF.match(line)
        if m_if:
            depth += 1
            if depth == 1 and re.search(r"\bWITH_VALGRIND\b", m_if.group(1) or ""):
                first_valgrind_idx = i
            continue
        if RE_ELSE.match(line):
            if depth == 1 and first_valgrind_idx is not None:
                return (first_valgrind_idx, i)
            continue
        if RE_ENDIF.match(line):
            depth = max(0, depth - 1)
    return None


def _find_requirements_line(lines: list[str], sanitizer: Optional[str]) -> Optional[int]:
    # parent_active, taken, current_active, seen_else, parent_scope, current_sanitizer_scope
    stack: list[tuple[bool, bool, bool, bool, bool, bool]] = []
    current_active = True
    current_sanitizer_scope = False
    seen_sanitizer_or_valgrind_if = False
    for i, raw in enumerate(lines):
        line = raw.strip()
        m_if = RE_IF.match(line)
        m_elseif = RE_ELSEIF.match(line) if not m_if else None
        if m_if:
            cond = m_if.group(1)
            parent_active = current_active
            parent_scope = current_sanitizer_scope
            if_res = eval_ya_make_condition(cond, sanitizer)
            current_active = parent_active and if_res
            current_sanitizer_scope = parent_scope or _cond_mentions_sanitizer(cond)
            if _cond_mentions_valgrind_or_sanitizer(cond):
                seen_sanitizer_or_valgrind_if = True
            stack.append((parent_active, if_res, current_active, False, parent_scope, current_sanitizer_scope))
            continue
        if m_elseif:
            cond = m_elseif.group(1)
            if not stack:
                continue
            parent_active, if_res, _prev_cur, seen_else, parent_scope, _prev_scope = stack.pop()
            if seen_else:
                stack.append((parent_active, if_res, _prev_cur, seen_else, parent_scope, _prev_scope))
                continue
            elseif_res = eval_ya_make_condition(cond, sanitizer)
            current_active = parent_active and (not if_res) and elseif_res
            current_sanitizer_scope = parent_scope or _cond_mentions_sanitizer(cond)
            if _cond_mentions_valgrind_or_sanitizer(cond):
                seen_sanitizer_or_valgrind_if = True
            stack.append((parent_active, if_res or elseif_res, current_active, False, parent_scope, current_sanitizer_scope))
            continue
        if RE_ELSE.match(line):
            if not stack:
                continue
            parent_active, if_res, _prev_cur, seen_else, parent_scope, _prev_scope = stack.pop()
            if seen_else:
                stack.append((parent_active, if_res, _prev_cur, seen_else, parent_scope, _prev_scope))
                continue
            current_active = parent_active and (not if_res)
            current_sanitizer_scope = parent_scope
            stack.append((parent_active, if_res, current_active, True, parent_scope, current_sanitizer_scope))
            continue
        if RE_ENDIF.match(line):
            if stack:
                stack.pop()
            current_active = stack[-1][2] if stack else True
            current_sanitizer_scope = stack[-1][5] if stack else False
            continue
        if current_active and RE_REQ_OPEN.match(raw):
            if not seen_sanitizer_or_valgrind_if or (not sanitizer or current_sanitizer_scope):
                return i
    return None


def _find_sanitizer_block_insert_index(lines: list[str], sanitizer: Optional[str]) -> Optional[int]:
    if not sanitizer or not str(sanitizer).strip():
        return None
    stack: list[tuple[int, bool, bool, bool, bool]] = []
    current_active = True
    for i, raw in enumerate(lines):
        line = raw.strip()
        m_if = RE_IF.match(line)
        m_elseif = RE_ELSEIF.match(line) if not m_if else None
        if m_if:
            cond = m_if.group(1)
            parent_active = current_active
            if_res = eval_ya_make_condition(cond, sanitizer)
            current_active = parent_active and if_res
            has_sanitizer = _cond_mentions_sanitizer(cond)
            stack.append((i, parent_active, if_res, False, has_sanitizer))
            if has_sanitizer and current_active:
                return i + 1
            continue
        if m_elseif:
            cond = m_elseif.group(1)
            if not stack:
                continue
            start, parent_active, if_res, seen_else, has_sanitizer = stack.pop()
            if seen_else:
                stack.append((start, parent_active, if_res, seen_else, has_sanitizer))
                continue
            elseif_res = eval_ya_make_condition(cond, sanitizer)
            current_active = parent_active and (not if_res) and elseif_res
            has_sanitizer = _cond_mentions_sanitizer(cond)
            stack.append((i, parent_active, if_res or elseif_res, False, has_sanitizer))
            if has_sanitizer and current_active:
                return i + 1
            continue
        if RE_ELSE.match(line) and stack:
            start, parent_active, if_res, seen_else, has_sanitizer = stack.pop()
            if seen_else:
                stack.append((start, parent_active, if_res, seen_else, has_sanitizer))
                continue
            current_active = parent_active and (not if_res)
            stack.append((start, parent_active, if_res, True, has_sanitizer))
            if has_sanitizer and current_active:
                return i + 1
            continue
        if RE_ENDIF.match(line):
            if stack:
                stack.pop()
            current_active = (
                (stack[-1][1] and ((not stack[-1][2]) if stack[-1][3] else stack[-1][2]))
                if stack
                else True
            )
            continue
    return None


def _find_default_insert_index(lines: list[str]) -> int:
    stack: list[tuple[bool, bool, bool, bool]] = []
    current_active = True
    last_active_size_idx: Optional[int] = None
    end_idx: Optional[int] = None
    for i, raw in enumerate(lines):
        line = raw.strip()
        if line == "END()" and end_idx is None:
            end_idx = i
        m_if = RE_IF.match(line)
        m_elseif = RE_ELSEIF.match(line) if not m_if else None
        if m_if:
            cond = m_if.group(1)
            parent_active = current_active
            if_res = eval_ya_make_condition(cond, None)
            current_active = parent_active and if_res
            stack.append((parent_active, if_res, current_active, False))
            continue
        if m_elseif:
            cond = m_elseif.group(1)
            if not stack:
                continue
            parent_active, if_res, _prev_cur, seen_else = stack.pop()
            if seen_else:
                stack.append((parent_active, if_res, _prev_cur, seen_else))
                continue
            elseif_res = eval_ya_make_condition(cond, None)
            current_active = parent_active and (not if_res) and elseif_res
            stack.append((parent_active, if_res or elseif_res, current_active, False))
            continue
        if RE_ELSE.match(line):
            if not stack:
                continue
            parent_active, if_res, _prev_cur, seen_else = stack.pop()
            if seen_else:
                stack.append((parent_active, if_res, _prev_cur, seen_else))
                continue
            current_active = parent_active and (not if_res)
            stack.append((parent_active, if_res, current_active, True))
            continue
        if RE_ENDIF.match(line):
            if stack:
                stack.pop()
            current_active = stack[-1][2] if stack else True
            continue
        if current_active and line.startswith("SIZE("):
            last_active_size_idx = i
    if last_active_size_idx is not None:
        return last_active_size_idx + 1
    if end_idx is not None:
        return end_idx
    return len(lines)


def _line_indent(raw: str) -> str:
    return raw[: len(raw) - len(raw.lstrip())]


def _choose_insert_indent(lines: list[str], insert_idx: int) -> str:
    candidates: list[str] = []
    if 0 <= insert_idx - 1 < len(lines):
        candidates.append(lines[insert_idx - 1])
    if 0 <= insert_idx < len(lines):
        candidates.append(lines[insert_idx])
    for raw in candidates:
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped in {"ELSE()", "ENDIF()", "END()"}:
            continue
        if RE_IF.match(stripped) or RE_ELSE.match(stripped) or RE_ENDIF.match(stripped):
            continue
        if RE_ELSEIF.match(stripped):
            continue
        return _line_indent(raw)
    for raw in candidates:
        if raw.strip():
            return _line_indent(raw)
    return ""


def _has_module_block(lines: list[str]) -> bool:
    return any(raw.strip() == "END()" for raw in lines)


def apply_cpu_requirements_to_content(
    content: str, cpu: str, sanitizer: Optional[str] = None
) -> tuple[str, str]:
    """Return (new_content, status): updated / no change / skip (no module block)."""
    cpu = normalize_cpu_req(cpu)
    lines = content.splitlines()
    changed = False
    req_idx = _find_requirements_line(lines, sanitizer)

    if req_idx is not None:
        span = requirements_span(lines, req_idx)
        if span is None:
            new_line = update_requirements_text(lines[req_idx], cpu)
            if new_line != lines[req_idx]:
                lines[req_idx] = new_line
                changed = True
        else:
            start, end = span
            old_block = "\n".join(lines[start : end + 1])
            new_block = update_requirements_text(old_block, cpu)
            if new_block != old_block:
                lines[start : end + 1] = new_block.splitlines()
                changed = True
    else:
        if not _has_module_block(lines):
            return content, "skip (no module block)"
        if sanitizer and str(sanitizer).strip():
            insert_inside = _find_sanitizer_block_insert_index(lines, sanitizer)
            if insert_inside is not None:
                base_indent = _line_indent(lines[insert_inside - 1]) if insert_inside > 0 else ""
                insert_line = base_indent + "    REQUIREMENTS(cpu:" + cpu + ")"
                lines.insert(insert_inside, insert_line)
                changed = True
            else:
                valgrind_span = _find_with_valgrind_else_span(lines)
                if valgrind_span is not None:
                    if_idx, else_idx = valgrind_span
                    block_indent = _line_indent(lines[if_idx + 1]) if if_idx + 1 < len(lines) else "    "
                    else_indent = _line_indent(lines[else_idx])
                    lines.insert(else_idx, block_indent + "REQUIREMENTS(cpu:" + cpu + ")")
                    lines.insert(else_idx + 1, else_indent + "ELSEIF(SANITIZER_TYPE)")
                    lines.insert(else_idx + 2, (else_indent + "    ") + "REQUIREMENTS(cpu:" + cpu + ")")
                    changed = True
                else:
                    insert_idx = _find_default_insert_index(lines)
                    if insert_idx < 0 or insert_idx > len(lines):
                        insert_idx = len(lines)
                    indent = _choose_insert_indent(lines, insert_idx)
                    lines.insert(insert_idx, indent + "IF (SANITIZER_TYPE)")
                    lines.insert(insert_idx + 1, indent + "    REQUIREMENTS(cpu:" + cpu + ")")
                    lines.insert(insert_idx + 2, indent + "ENDIF()")
                    changed = True
        else:
            insert_idx = _find_top_insert_index(lines)
            if insert_idx is None:
                insert_idx = _find_default_insert_index(lines)
            if insert_idx < 0 or insert_idx > len(lines):
                insert_idx = len(lines)
            indent = _choose_insert_indent(lines, insert_idx)
            insert_line = indent + "REQUIREMENTS(cpu:" + cpu + ")"
            lines.insert(insert_idx, insert_line)
            changed = True

    if not changed:
        return content, "no change"
    return "\n".join(lines) + "\n", "updated"


def apply_one(
    repo_root: Path, suite_path: str, cpu: str, dry_run: bool, sanitizer: Optional[str]
) -> tuple[str, str]:
    """Write CPU requirement into suite/ya.make. Returns (suite_path, status)."""
    suite = normalize_suite_path(suite_path)
    ya_make = repo_root / suite / "ya.make"
    if not ya_make.exists():
        return suite, "missing ya.make"
    text = ya_make.read_text(encoding="utf-8", errors="replace")
    new_content, status = apply_cpu_requirements_to_content(text, cpu, sanitizer)
    if status == "skip (no module block)" or status == "no change":
        return suite, status
    if status == "updated":
        if not dry_run:
            ya_make.write_text(new_content, encoding="utf-8")
        return suite, "updated" if not dry_run else "would update"
    return suite, status
