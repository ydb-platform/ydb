"""Read REQUIREMENTS / SIZE / SPLIT_FACTOR from ya.make, including SANITIZER_TYPE branches."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any, Optional, Sequence

RE_REQ_OPEN = re.compile(r"^\s*REQUIREMENTS\s*\(", re.IGNORECASE)
RE_REQ_RAM = re.compile(r"\bram\s*:\s*(\d+)\b", re.IGNORECASE)
RE_REQ_CPU = re.compile(r"\bcpu\s*:\s*(\w+)\b", re.IGNORECASE)
RE_SIZE = re.compile(r"^\s*SIZE\s*\(\s*(\w+)\s*\)\s*$")
RE_SPLIT_FACTOR = re.compile(r"^\s*SPLIT_FACTOR\s*\(\s*(\d+)\s*\)\s*$")
RE_FORK_TEST_FILES = re.compile(r"^\s*FORK_TEST_FILES\s*\(\s*\)\s*$")
RE_TEST_SRCS_OPEN = re.compile(r"^\s*TEST_SRCS\s*\(\s*$")
RE_IF = re.compile(r"^\s*IF\s*\((.*)\)\s*$")
RE_ELSEIF = re.compile(r"^\s*ELSEIF\s*\((.*)\)\s*$")
RE_ELSE = re.compile(r"^\s*ELSE\s*\(\s*\)\s*$")
RE_ENDIF = re.compile(r"^\s*ENDIF\s*\(\s*\)\s*$")
RE_SAN_EQ = re.compile(r'SANITIZER_TYPE\s*==\s*"([^"]+)"')
RE_SAN_NE = re.compile(r'SANITIZER_TYPE\s*!=\s*"([^"]+)"')

# Strip /partN for normalized suite path (same as in generate_chunk_trace)
PART_SUFFIX_RE = re.compile(r"/part\d+$")


def normalize_suite_path(path: str) -> str:
    """Merge partitioned suites like .../part7 into one suite path."""
    return PART_SUFFIX_RE.sub("", path)


def _paren_delta(text: str) -> int:
    return text.count("(") - text.count(")")


def requirements_span(lines: Sequence[str], start: int) -> Optional[tuple[int, int]]:
    """Inclusive (start, end) of a REQUIREMENTS(...) block that may span lines."""
    if start < 0 or start >= len(lines) or not RE_REQ_OPEN.match(lines[start]):
        return None
    depth = _paren_delta(lines[start])
    end = start
    while depth > 0 and end + 1 < len(lines):
        end += 1
        depth += _paren_delta(lines[end])
    return start, end


def requirements_body(lines: Sequence[str], start: int, end: int) -> str:
    text = "\n".join(lines[start : end + 1])
    open_idx = text.find("(")
    close_idx = text.rfind(")")
    if open_idx < 0 or close_idx <= open_idx:
        return ""
    return text[open_idx + 1 : close_idx]


def parse_requirements_body(body: str) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    m_ram = RE_REQ_RAM.search(body)
    m_cpu = RE_REQ_CPU.search(body)
    if m_ram:
        attrs["ram_gb"] = int(m_ram.group(1))
    if m_cpu:
        cpu_val = m_cpu.group(1).strip().lower()
        if cpu_val != "all":
            try:
                attrs["cpu_cores"] = int(cpu_val)
            except ValueError:
                pass
    return attrs


def _has_sanitizer(sanitizer: Optional[str]) -> bool:
    return bool(sanitizer and str(sanitizer).strip() and str(sanitizer).strip().lower() not in ("none", "off", "false", "0"))


def eval_ya_make_condition(cond: str, sanitizer: Optional[str] = None) -> bool:
    has_san = _has_sanitizer(sanitizer)
    san = (sanitizer or "").strip().lower()
    expr = cond.strip()
    expr = RE_SAN_EQ.sub(lambda m: "True" if san == m.group(1).strip().lower() else "False", expr)
    expr = RE_SAN_NE.sub(lambda m: "True" if san != m.group(1).strip().lower() else "False", expr)
    expr = re.sub(r"\bSANITIZER_TYPE\b", "True" if has_san else "False", expr)
    expr = re.sub(r"\bWITH_VALGRIND\b", "False", expr)
    expr = re.sub(r"\bOS_WINDOWS\b", "False", expr)
    expr = re.sub(r"\bOS_LINUX\b", "True", expr)
    expr = re.sub(r"\bOS_DARWIN\b", "False", expr)
    expr = re.sub(r"\bOR\b", "or", expr)
    expr = re.sub(r"\bAND\b", "and", expr)
    expr = re.sub(r"\bNOT\b", "not", expr)
    try:
        return _safe_eval_bool(expr)
    except Exception:
        return has_san if "SANITIZER_TYPE" in cond else False


def _safe_eval_bool(expr: str) -> bool:
    tree = ast.parse(expr, mode="eval")
    return _eval_ast_bool(tree.body)


def _eval_ast_bool(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant):
        return bool(node.value)
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(_eval_ast_bool(v) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(_eval_ast_bool(v) for v in node.values)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return not _eval_ast_bool(node.operand)
    raise ValueError(f"Unsupported AST node: {type(node)}")


def _parse_active_attrs(text: str, sanitizer: Optional[str]) -> dict[str, Any]:
    """
    Evaluate ya.make with IF/ELSE/ENDIF for the selected sanitizer and collect
    REQUIREMENTS + SIZE from active lines.
    When FORK_TEST_FILES() is present, effective split = TEST_SRCS file count × SPLIT_FACTOR(N).
    """
    attrs: dict[str, Any] = {}
    # Stack frames: (parent_active, if_result, current_active, seen_else)
    stack: list[tuple[bool, bool, bool, bool]] = []
    current_active = True
    has_fork_test_files = False
    test_srcs_count = 0
    inside_test_srcs = False
    test_srcs_active = False
    count_this_test_srcs = 0
    raw_lines = text.splitlines()
    i = 0
    while i < len(raw_lines):
        raw = raw_lines[i]
        line = raw.strip()
        if not line:
            i += 1
            continue

        if inside_test_srcs:
            if line == ")":
                inside_test_srcs = False
                if test_srcs_active:
                    test_srcs_count += count_this_test_srcs
                count_this_test_srcs = 0
            elif line and not line.startswith(")"):
                if test_srcs_active:
                    count_this_test_srcs += 1
            i += 1
            continue

        if RE_TEST_SRCS_OPEN.match(line):
            inside_test_srcs = True
            test_srcs_active = current_active
            count_this_test_srcs = 0
            i += 1
            continue
        # TEST_SRCS( with content on same line: TEST_SRCS(\n or TEST_SRCS( file.py )
        if re.match(r"^\s*TEST_SRCS\s*\(\s*\S", line):
            inside_test_srcs = True
            test_srcs_active = current_active
            count_this_test_srcs = 0
            rest = re.sub(r"^\s*TEST_SRCS\s*\(\s*", "", line)
            rest = rest.rstrip()
            if rest.endswith(")"):
                rest = rest[:-1].rstrip()
                inside_test_srcs = False
            if rest and test_srcs_active:
                count_this_test_srcs += 1
            if not inside_test_srcs:
                test_srcs_count += count_this_test_srcs
                count_this_test_srcs = 0
            i += 1
            continue

        m_if = RE_IF.match(line)
        if m_if:
            parent_active = current_active
            if_res = eval_ya_make_condition(m_if.group(1), sanitizer)
            current_active = parent_active and if_res
            stack.append((parent_active, if_res, current_active, False))
            i += 1
            continue

        m_elseif = RE_ELSEIF.match(line)
        if m_elseif:
            if stack:
                parent_active, if_res, _cur, seen_else = stack.pop()
                if seen_else:
                    stack.append((parent_active, if_res, _cur, seen_else))
                else:
                    elseif_res = eval_ya_make_condition(m_elseif.group(1), sanitizer)
                    current_active = parent_active and (not if_res) and elseif_res
                    stack.append((parent_active, if_res or elseif_res, current_active, False))
            i += 1
            continue

        if RE_ELSE.match(line):
            if stack:
                parent_active, if_res, _cur, seen_else = stack.pop()
                if seen_else:
                    stack.append((parent_active, if_res, _cur, seen_else))
                else:
                    current_active = parent_active and (not if_res)
                    stack.append((parent_active, if_res, current_active, True))
            i += 1
            continue

        if RE_ENDIF.match(line):
            if stack:
                stack.pop()
                current_active = stack[-1][2] if stack else True
            i += 1
            continue

        if not current_active:
            i += 1
            continue

        if RE_FORK_TEST_FILES.match(line):
            has_fork_test_files = True
            i += 1
            continue

        span = requirements_span(raw_lines, i)
        if span is not None:
            _start, end = span
            attrs.update(parse_requirements_body(requirements_body(raw_lines, _start, end)))
            i = end + 1
            continue

        m_size = RE_SIZE.match(line)
        if m_size:
            attrs["size"] = m_size.group(1).upper()
            i += 1
            continue

        m_split = RE_SPLIT_FACTOR.match(line)
        if m_split:
            attrs["split_factor"] = int(m_split.group(1))
            i += 1
            continue

        i += 1

    if has_fork_test_files and test_srcs_count > 0:
        attrs["test_srcs_count"] = test_srcs_count
        if attrs.get("split_factor") is not None:
            raw = int(attrs["split_factor"])
            effective = test_srcs_count * raw
            attrs["effective_split_factor"] = effective
            attrs["split_factor_tooltip"] = (
                f"Effective split {effective} = {test_srcs_count} TEST_SRCS files × SPLIT_FACTOR({raw}). "
                f"ya.make contains SPLIT_FACTOR({raw}), not {effective}."
            )
    elif attrs.get("split_factor") is not None:
        raw = int(attrs["split_factor"])
        attrs["split_factor_tooltip"] = f"SPLIT_FACTOR({raw}) from ya.make (no FORK_TEST_FILES)."

    return attrs


def get_requirements_for_suite(repo_root: Path, suite_path: str, sanitizer: Optional[str] = None) -> Optional[dict[str, Any]]:
    """Active REQUIREMENTS/SIZE/SPLIT_FACTOR for suite_path/ya.make, or None."""
    suite_path = normalize_suite_path(suite_path)
    ya_make = repo_root / suite_path / "ya.make"
    if not ya_make.exists():
        return None
    try:
        text = ya_make.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    attrs = _parse_active_attrs(text, sanitizer)
    return attrs or None


def build_requirements_cache(repo_root: Path, suite_paths: list[str], sanitizer: Optional[str] = None) -> dict[str, dict[str, Any]]:
    """suite_path -> attrs from the active ya.make branch."""
    cache: dict[str, dict[str, Any]] = {}
    for suite_path in suite_paths:
        norm = normalize_suite_path(suite_path)
        if norm in cache:
            continue
        req = get_requirements_for_suite(repo_root, norm, sanitizer=sanitizer)
        if req is not None:
            cache[norm] = req
    return cache
