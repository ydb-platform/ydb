import argparse
import json
import os
import re
import subprocess
from functools import lru_cache
from typing import Optional, Dict, List, Set, Tuple
from collections import defaultdict

# External keys (right-oriented matching by path segments)
EXTERNAL_PREFIXES = frozenset(
    [
        "boost/",
        "Eigen/",
        "unsupported/Eigen/",
        "tensorflow/",
        "third_party/tensorflow/",
        "contrib/",
        "opencv/",
        "opencv2/",
        "range/",
    ]
)

PRIVATE_PATTERNS = frozenset(["detail/", "impl/", "internal/", "src/"])

INCLUDE_PATTERN = re.compile(r'#\s*include\s*[<"]([^">]+)[">]')
COMMENT_PATTERN = re.compile(r"//.*")
ERROR_PATTERNS = re.compile(r"(error:|failed)", re.IGNORECASE)

CORRECT_INCLUDES_RE = re.compile(r"has correct #includes/fwd-decls")

DEFAULT_VERBOSE_LEVEL = "3"

SECTION_SUFFIX_ADD = " should add these lines:"
SECTION_SUFFIX_REMOVE = " should remove these lines:"
SECTION_PREFIX_FULL = "The full include-list for "
EXTERNAL_TERMINALS = frozenset(p.rstrip("/").split("/")[-1] for p in EXTERNAL_PREFIXES)

# ROS header roots (for path normalization only)
ROS_HEADER_PATHS = [
    "sdg/sdc/ros/",
    "sdg/sdc/third_party/ros/",
]

# ------------------------------ utilities ------------------------------


def should_output_verbose_to_stderr(args) -> bool:
    return args.verbose and args.verbose != DEFAULT_VERBOSE_LEVEL


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--testing-src", required=True)
    parser.add_argument("--iwyu-bin", required=True)
    parser.add_argument("--iwyu-json", required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--build-root", required=True)
    parser.add_argument("--mapping_file", default="")
    parser.add_argument("--default-mapping-file", default="")
    parser.add_argument("--verbose", default="")
    return parser.parse_known_args()


def generate_outputs(path: str) -> None:
    open(os.path.splitext(path)[0] + ".o", "w").close()
    open(path, "w").close()


def filter_cmd(cmd):
    """Drop all args up to and including /retry_cc.py."""
    skip = True
    for x in cmd:
        if not skip:
            yield x
        elif "/retry_cc.py" in x:
            skip = False


LINKER_FILTER_RE = re.compile(r'(/python(?:3)?$|/clang(?:\+\+)?$|/\.ya/tools/)')


def filter_linker_flags(cmd):
    """Remove runner/tool entries from command line."""

    @lru_cache(maxsize=256)
    def drop(a: str) -> bool:
        return bool(LINKER_FILTER_RE.search(a))

    return [a for a in cmd if not drop(a)]


def is_generated(testing_src: str, build_root: str) -> bool:
    return testing_src.startswith(build_root)


@lru_cache(maxsize=2048)
def parse_include_line(line: str) -> Optional[str]:
    """Return include path without quotes/angles if line is an #include."""
    s = COMMENT_PATTERN.sub("", line.lstrip("- ").strip())
    m = INCLUDE_PATTERN.match(s)
    return m.group(1) if m else None


def _basename(s: str) -> str:
    i = s.rfind("/")
    return s if i < 0 else s[i + 1 :]


ParsedLine = Tuple[str, Optional[str], Optional[str]]  # (original, include, basename)


def _preparse_lines(lines: List[str]) -> List[ParsedLine]:
    """Single pass parse for a block: cache include and basename."""
    out: List[ParsedLine] = []
    for line in lines:
        inc = parse_include_line(line)
        base = _basename(inc) if inc else None
        out.append((line, inc, base))
    return out


# ------------------------------ verbose analyzer ------------------------------


def _parse_warning(line: str) -> Optional[Tuple[str, int, int, str, str]]:
    pat = r'^(.+?):(\d+):(\d+): warning: (.+?) is defined in [<"]([^">]+)[">], which isn\'t directly'
    m = re.match(pat, line)
    if not m:
        return None
    try:
        fpath = m.group(1).strip()
        lno = int(m.group(2))
        col = int(m.group(3))
        token = m.group(4).strip()
        header = m.group(5)
        simple_token = token.split('<')[0].split('::')[-1] if '::' in token else token
        return fpath, lno, col, simple_token, header
    except (ValueError, IndexError):
        return None


def parse_verbose_headers_to_skip(verbose_text: str) -> Dict[str, Set[str]]:
    """
    Headers to ignore when all their warnings are auto-related.
    """
    if not verbose_text:
        return {}
    file_header_warnings: Dict[str, Dict[str, List[Tuple[int, int, str]]]] = defaultdict(lambda: defaultdict(list))
    for raw in verbose_text.splitlines():
        p = _parse_warning(raw.strip())
        if not p:
            continue
        fpath, lno, col, simple_token, header = p
        file_header_warnings[fpath][header].append((lno, col - 1, simple_token))

    result: Dict[str, Set[str]] = {}
    for fpath, header_warnings in file_header_warnings.items():
        headers_to_skip: Set[str] = set()
        try:
            import linecache

            for header, warnings in header_warnings.items():
                all_auto = True
                for line_num, col0, token in warnings:
                    line = linecache.getline(fpath, line_num).rstrip('\n\r')
                    if not line:
                        continue
                    if col0 < len(line):
                        if col0 + len(token) <= len(line) and line[col0 : col0 + len(token)] == token:
                            all_auto = False
                            break
                        prefix = line[:col0]
                        if not re.search(r'\b(auto|decltype\s*\(\s*auto\s*\))\b', prefix):
                            all_auto = False
                            break
                if all_auto and warnings:
                    headers_to_skip.add(header)
            linecache.clearcache()
        except OSError:
            pass
        if headers_to_skip:
            result[fpath] = headers_to_skip
    return result


# ------------------------------ external key logic ------------------------------


@lru_cache(maxsize=8192)
def _is_external(path: Optional[str]) -> bool:
    if not path:
        return False
    return any(seg in EXTERNAL_TERMINALS for seg in path.split("/"))


@lru_cache(maxsize=8192)
def get_rightmost_key(path: str) -> Optional[str]:
    if not path:
        return None
    for seg in reversed(path.split("/")):
        if seg in EXTERNAL_TERMINALS:
            return seg
    return None


# ------------------------------ IWYU output parsing ------------------------------


def separate_verbose_from_suggestions(iwyu_full_output: str) -> Tuple[str, dict, Set[str]]:
    """Split IWYU stderr into (verbose, structured blocks, global removes)."""
    IN_NONE, IN_ADD, IN_REMOVE, IN_FULL = 0, 1, 2, 3
    WARNING_MARKER = ": warning: "
    NOTE_MARKER = ": note: "
    state = IN_NONE
    verbose = []
    blocks: Dict[str, Dict[str, Dict[str, object]]] = {}
    global_removes: Set[str] = set()
    cur_file, cur_kind = None, None

    def ensure(f: str) -> dict:
        if f not in blocks:
            blocks[f] = {
                "add": {"hdr": None, "lines": []},
                "remove": {"hdr": None, "lines": []},
                "full": {"hdr": None, "lines": []},
            }
        return blocks[f]

    for s in iwyu_full_output.splitlines():
        if WARNING_MARKER in s:
            verbose.append(s)
            continue
        if NOTE_MARKER in s or s.strip() == "---":
            continue

        if s.endswith(SECTION_SUFFIX_ADD):
            cur_file = s[: -len(SECTION_SUFFIX_ADD)]
            cur_kind = "add"
            ensure(cur_file)["add"]["hdr"] = s
            state = IN_ADD
            continue
        elif s.endswith(SECTION_SUFFIX_REMOVE):
            cur_file = s[: -len(SECTION_SUFFIX_REMOVE)]
            cur_kind = "remove"
            ensure(cur_file)["remove"]["hdr"] = s
            state = IN_REMOVE
            continue
        elif s.startswith(SECTION_PREFIX_FULL):
            cur_file = s[len(SECTION_PREFIX_FULL) : -1]
            cur_kind = "full"
            ensure(cur_file)["full"]["hdr"] = s
            state = IN_FULL
            continue

        if state != IN_NONE or CORRECT_INCLUDES_RE.search(s):
            if cur_file and cur_kind:
                ensure(cur_file)[cur_kind]["lines"].append(s)
                # Collect global removes during parsing
                if cur_kind == "remove":
                    inc = parse_include_line(s)
                    if inc:
                        global_removes.add(inc)

    return "\n".join(verbose), blocks, global_removes


# ------------------------------ section processors ------------------------------


def process_add_section(
    add_parsed: List[ParsedLine],
    skip_full: Set[str],
    skip_base: Set[str],
    valid_1to1_keys: Set[str],
    conflict_keys: Set[str],
) -> List[str]:
    """Keep: non-external (unless auto-skipped) + external only for valid 1↔1 keys."""
    kept: List[str] = []
    for line, inc, base in add_parsed:
        if not inc:
            kept.append(line)
            continue
        if inc in skip_full or (base and base in skip_base):
            continue
        if _is_external(inc):
            k = get_rightmost_key(inc)
            if not k or k in conflict_keys or k not in valid_1to1_keys:
                continue
        kept.append(line)
    return kept


def process_remove_section(
    rem_parsed: List[ParsedLine],
    valid_1to1_keys: Set[str],
    conflict_keys: Set[str],
) -> Tuple[List[str], List[str]]:
    """
    Returns:
      - kept REMOVE lines (only non-external or external with valid 1↔1 key)
      - raw '#include ...' lines prepared to paste into FULL for conflict keys (N↔M).
    """
    kept: List[str] = []
    to_full: List[str] = []
    for line, inc, _ in rem_parsed:
        if not inc:
            continue
        if _is_external(inc):
            k = get_rightmost_key(inc)
            if not k:
                continue
            if k in conflict_keys:
                clean = line.lstrip("-").strip()
                if clean.startswith("#include"):
                    to_full.append(clean)
                continue
            if k in valid_1to1_keys:
                kept.append(line)
        else:
            kept.append(line)
    return kept, to_full


def process_full_section(
    full_parsed: List[ParsedLine],
    removed_includes_1to1: Set[str],
    conflict_keys: Set[str],
    rem_for_full: List[str],
    skip_full: Set[str],
    skip_base: Set[str],
    add_conflict_includes: Set[str],
) -> List[str]:
    """
    Process FULL section:
      - 1↔1:
          Drop only headers actually present in removed_includes_1to1
          (either full path or basename).
      - N↔M (conflict keys):
          Instead of dropping ALL headers with that key (too aggressive),
          drop only those that were suggested in ADD under the same key
          (tracked in add_conflict_includes/add_conflict_basenames).
          This prevents unrelated boost/opencv headers from disappearing.
      - Auto-skip:
          Drop headers explicitly flagged by skip_full/skip_base.
      - Append rem_for_full:
          After filtering, append cleaned headers collected from REMOVE,
          deduplicated by include path and respecting auto-skip.
    """
    removed_basenames = {_basename(h) for h in removed_includes_1to1}
    kept: List[str] = []
    seen: Set[str] = set()

    # Pass 1: filter existing FULL
    for line, inc, base in full_parsed:
        txt = line.strip()
        if not txt or CORRECT_INCLUDES_RE.search(txt):
            continue
        if not inc:
            kept.append(line)
            continue
        if inc in skip_full or (base and base in skip_base):
            continue
        if inc in add_conflict_includes:
            continue
        if inc in removed_includes_1to1 or (base and base in removed_basenames):
            continue
        kept.append(line)
        seen.add(inc)

    # Pass 2: append rem_for_full
    for line in rem_for_full:
        inc = parse_include_line(line)
        base = _basename(inc) if inc else None
        if not inc:
            continue
        if inc in seen:
            continue
        if inc in skip_full or (base and base in skip_base):
            continue
        kept.append(line)
        seen.add(inc)

    return kept


# ---------- Parsing / precompute helpers ----------


def preparse_all(structured_blocks: dict) -> Dict[str, Dict[str, List[ParsedLine]]]:
    """Preparse add/remove/full once for all files."""
    parsed: Dict[str, Dict[str, List[ParsedLine]]] = {}
    for file_name, sect in structured_blocks.items():
        parsed[file_name] = {
            "add": _preparse_lines(sect["add"]["lines"]),
            "remove": _preparse_lines(sect["remove"]["lines"]),
            "full": _preparse_lines(sect["full"]["lines"]),
        }
    return parsed


def auto_skip_sets(file_name: str, headers_to_skip: Dict[str, Set[str]]) -> Tuple[Set[str], Set[str]]:
    """Build full-path and basename skip sets derived from the 'auto' analysis."""
    skip_full = set(headers_to_skip.get(file_name, ()))
    skip_base = {_basename(x) for x in skip_full}
    return skip_full, skip_base


def count_key_stats(
    add_parsed: List[ParsedLine], rem_parsed: List[ParsedLine]
) -> Tuple[Dict[str, int], Dict[str, int], Set[str], Set[str]]:
    """
    Return (add_keys, rem_keys, valid_1to1_keys, conflict_keys) by right-oriented external key.
    """
    add_keys: Dict[str, int] = defaultdict(int)
    rem_keys: Dict[str, int] = defaultdict(int)

    for _, inc, _ in add_parsed:
        if inc and _is_external(inc):
            k = get_rightmost_key(inc)
            if k:
                add_keys[k] += 1

    for _, inc, _ in rem_parsed:
        if inc and _is_external(inc):
            k = get_rightmost_key(inc)
            if k:
                rem_keys[k] += 1

    keys = set(add_keys) | set(rem_keys)

    # Check for potential 1->1 keys and exclude those with private includes in ADD
    potential_1to1 = {k for k in keys if add_keys.get(k, 0) == 1 and rem_keys.get(k, 0) == 1}
    valid_1to1: Set[str] = set()

    for k in potential_1to1:
        # Check if ADD section for this key contains private patterns
        has_private_in_add = any(
            inc
            and _is_external(inc)
            and get_rightmost_key(inc) == k
            and any(pattern in inc for pattern in PRIVATE_PATTERNS)
            for _, inc, _ in add_parsed
        )
        if not has_private_in_add:
            valid_1to1.add(k)

    conflict_keys = keys - valid_1to1
    return add_keys, rem_keys, valid_1to1, conflict_keys


def removed_set_from_rem_kept(rem_kept: List[str]) -> Set[str]:
    """Collect includes kept in REMOVE (used to filter 1↔1 from FULL)."""
    return {inc for _, inc, _ in _preparse_lines(rem_kept) if inc}


def emit_sections(
    add_hdr: Optional[str],
    add_body: List[str],
    rem_hdr: Optional[str],
    rem_body: List[str],
    full_hdr: Optional[str],
    full_body: List[str],
) -> List[str]:
    """Format three sections back to IWYU-like text chunk."""
    out: List[str] = []
    if add_hdr:
        out.append(add_hdr)
        out.extend(add_body)
        out.append("")
    if rem_hdr:
        out.append(rem_hdr)
        out.extend(rem_body)
        out.append("")
    if full_hdr:
        out.append(full_hdr)
        out.extend(full_body)
        out.append("")
    return out


# ---------- Migration / OK-case post-step ----------


def postprocess_ok_and_migration(
    file_name: str,
    add_hdr: Optional[str],
    rem_hdr: Optional[str],
    full_hdr: Optional[str],
    add_kept: List[str],
    rem_kept: List[str],
    full_kept: List[str],
    add_parsed_original: List[ParsedLine],
    full_parsed_original: List[ParsedLine],
    global_removes: Set[str],
    skip_full: Set[str],
    skip_base: Set[str],
) -> Tuple[bool, List[str], List[str]]:
    remove_empty_after = not rem_kept
    if not remove_empty_after:
        return (False, add_kept, full_kept)
    if not add_kept:
        return (True, add_kept, full_kept)
    migrated: List[str] = []
    for line, inc, base in _preparse_lines(add_kept):
        if inc and (inc in global_removes) and not (inc in skip_full or (base and base in skip_base)):
            migrated.append(line)

    if not migrated:
        return (True, add_kept, full_kept)
    add_kept = migrated
    if full_hdr and full_parsed_original:
        migrated_paths = {parse_include_line(line) for line in migrated}
        orig_add_paths = {inc for _, inc, _ in add_parsed_original if inc}
        non_migrated = orig_add_paths - migrated_paths

        filtered_full: List[str] = []
        for line, inc, base in full_parsed_original:
            if not inc:
                # keep comments/blank between sections out (filter later in emit)
                continue
            if inc in non_migrated:
                continue
            if inc in skip_full or (base and base in skip_base):
                continue
            if CORRECT_INCLUDES_RE.search(line) or not line.strip():
                continue
            filtered_full.append(line)
        full_kept = filtered_full

    return (False, add_kept, full_kept)


# ------------------------------ builder ------------------------------


def build_clean_output(structured_blocks: dict, headers_to_skip: Dict[str, Set[str]], global_removes: Set[str]) -> str:
    """
    Build a cleaned IWYU suggestion text from parsed sections with external-key
    semantics, auto-related filtering, and cross-file migration handling.
    Inputs
    -------
    structured_blocks : dict
        Parsed IWYU output grouped per file and section, see `separate_verbose_from_suggestions`:
        {
          "<file>": {
            "add":    {"hdr": str|None, "lines": [str, ...]},
            "remove": {"hdr": str|None, "lines": [str, ...]},
            "full":   {"hdr": str|None, "lines": [str, ...]},
          },
          ...
        }

    headers_to_skip : Dict[str, Set[str]]
        Per-file map of headers that should be ignored entirely if their usage
        warnings are purely auto-related (derived from verbose analysis).
        Keys: file path; Values: set of header paths to skip.
        For convenience we also skip by basename in addition to full path.

    global_removes : Set[str]
        The union of all headers mentioned in REMOVE sections across all files.
        Used to detect a "migration" scenario where a header is being moved
        from one file to another (ADD in one file, REMOVE in another).

    Core Concepts
    -------------
    * External keys:
      We classify third-party headers by a right-oriented key (e.g. "boost", "opencv2"),
      computed from the rightmost path segment that matches a known external prefix.
      This lets us reason about 1↔1 vs N↔M replacement scenarios at library level.

    * 1↔1 vs conflict keys:
      For each file and section we count external includes by key:
        - valid_1to1_keys: keys where ADD count == 1 and REMOVE count == 1
        - conflict_keys  : all other keys appearing in ADD or REMOVE (N↔M, 1↔N, N↔1)
      We only allow external replacements for valid_1to1_keys; conflict_keys
      are filtered out from ADD/REMOVE and handled via FULL.

    * Auto-only warnings:
      Headers marked in `headers_to_skip[file]` (and their basenames) are dropped
      from ADD and FULL, because their usage was attributed exclusively to `auto`
      or `decltype(auto)` in the verbose analysis.

    Processing Steps (per file)
    ---------------------------
    1) Precompute:
       - Parse once per section (`_preparse_lines`).
       - Count external keys in ADD and REMOVE.
       - Split keys into `valid_1to1_keys` and `conflict_keys`.
       - Build skip sets: exact headers and basenames from `headers_to_skip`.

    2) ADD:
       - Keep non-external includes unless auto-skipped.
       - For external includes:
           * Drop if key ∈ conflict_keys (ignore N↔M).
           * Keep only if key ∈ valid_1to1_keys and not auto-skipped.

    3) REMOVE:
       - Keep non-external includes (they are explicit signals).
       - For external includes:
           * If key ∈ conflict_keys: do not show in REMOVE; capture a cleaned
             `#include ...` line into `rem_for_full` to be injected into FULL later.
           * If key ∈ valid_1to1_keys: keep in REMOVE.

    4) FULL:
       - Drop lines matching auto-skip (full path or basename).
       - For conflict_keys: drop all FULL includes with those keys; then append
         `rem_for_full` (deduplicated by include path).
       - For valid 1↔1: drop only the headers actually present in REMOVE
         (match by full path or basename).
       - Keep all other lines.

    5) OK-case:
       - If REMOVE became empty after filtering AND there is nothing to inject
         into FULL (`rem_for_full` is empty), and there is no migration (see next),
         skip emitting this file entirely (treated as OK).

    6) Migration handling (cross-file include movement):
       - If this file's REMOVE is empty after filtering but ADD exists:
           * Compute `migrated_includes` = includes in ADD that appear in `global_removes`
             (i.e., they are being removed from some other file).
           * If `migrated_includes` is non-empty:
               - Keep ONLY these migrated includes in ADD (others are dropped).
               - In FULL, remove those non-migrated includes that originated in ADD
                 (while still respecting auto-skip).
    Output
    ------
    A single text blob that mirrors IWYU's multi-file, multi-section layout:
      <file> should add these lines:
      ...
      <file> should remove these lines:
      ...
      The full include-list for <file>:
      ...
      ---
    """
    out_chunks: List[str] = []

    parsed = preparse_all(structured_blocks)

    for file_name, sect in structured_blocks.items():
        add_hdr, rem_hdr, full_hdr = sect["add"]["hdr"], sect["remove"]["hdr"], sect["full"]["hdr"]
        add_par = parsed[file_name]["add"] if add_hdr else []
        rem_par = parsed[file_name]["remove"] if rem_hdr else []
        full_par = parsed[file_name]["full"] if full_hdr else []

        skip_full, skip_base = auto_skip_sets(file_name, headers_to_skip)

        _, _, valid_1to1, conflict_keys = count_key_stats(add_par, rem_par)
        add_conflict_includes: Set[str] = set()
        for _, inc, _ in add_par:
            if not inc or not _is_external(inc):
                continue
            k = get_rightmost_key(inc)
            if k and k in conflict_keys:
                add_conflict_includes.add(inc)

        add_kept = (
            process_add_section(
                add_parsed=add_par,
                skip_full=skip_full,
                skip_base=skip_base,
                valid_1to1_keys=valid_1to1,
                conflict_keys=conflict_keys,
            )
            if add_hdr
            else []
        )

        rem_kept, rem_for_full = (
            process_remove_section(
                rem_parsed=rem_par,
                valid_1to1_keys=valid_1to1,
                conflict_keys=conflict_keys,
            )
            if rem_hdr
            else ([], [])
        )

        removed_includes_1to1 = removed_set_from_rem_kept(rem_kept)

        full_kept = (
            process_full_section(
                full_parsed=full_par,
                removed_includes_1to1=removed_includes_1to1,
                conflict_keys=conflict_keys,
                rem_for_full=rem_for_full,
                skip_full=skip_full,
                skip_base=skip_base,
                add_conflict_includes=add_conflict_includes,
            )
            if full_hdr
            else []
        )

        # Post-step: OK / Migration
        skip_file, add_kept, full_kept = postprocess_ok_and_migration(
            file_name=file_name,
            add_hdr=add_hdr,
            rem_hdr=rem_hdr,
            full_hdr=full_hdr,
            add_kept=add_kept,
            rem_kept=rem_kept,
            full_kept=full_kept,
            add_parsed_original=add_par,
            full_parsed_original=full_par,
            global_removes=global_removes,
            skip_full=skip_full,
            skip_base=skip_base,
        )

        if skip_file:
            continue

        if not any(x.strip() for x in (add_kept + rem_kept + full_kept)):
            continue

        # Emit
        chunk = emit_sections(add_hdr, add_kept, rem_hdr, rem_kept, full_hdr, full_kept)
        if chunk and any(c.strip() for c in chunk):
            out_chunks.append("\n".join(chunk).rstrip())

    return "\n---\n\n".join(out_chunks)


# ------------------------------ iwyu runner + normalization ------------------------------


def run_iwyu_command(iwyu_bin: str, filtered_clang_cmd: List[str], verbose: str, mapping_file: str) -> subprocess.Popen:
    cmd = ["env", "-u", "LD_LIBRARY_PATH", iwyu_bin]
    cmd.extend(filtered_clang_cmd)
    for arg in (
        f"--mapping_file={mapping_file}",
        f"--verbose={verbose}",
        "--cxx17ns",
        "--no_fwd_decls",
        "--error",
        "--no_default_mappings",
    ):
        cmd.extend(["-Xiwyu", arg])
    return subprocess.Popen(stdout=subprocess.PIPE, stderr=subprocess.PIPE, args=cmd)


@lru_cache(maxsize=4)
def _norm_prefix(source_root: str) -> str:
    root = os.path.normpath(source_root)
    return (root + os.sep) if not root.endswith(os.sep) else root


def _normalize_paths(text: str, source_root: str) -> str:
    return text.replace(_norm_prefix(source_root), "$(SOURCE_ROOT)/")


def extract_include_paths(clang_cmd: List[str]) -> List[str]:
    paths = []
    for arg in clang_cmd:
        if arg.startswith("-I"):
            paths.append(arg[2:])
    return paths


def filter_ros_include_paths(include_paths: List[str], source_root: str) -> List[str]:
    ros_paths = []
    for path in include_paths:
        abs_path = path if os.path.isabs(path) else os.path.join(source_root, path)
        for ros_base in ROS_HEADER_PATHS:
            ros_full_path = os.path.join(source_root, ros_base)
            if abs_path.startswith(ros_full_path) or ros_full_path in abs_path:
                ros_paths.append(abs_path)
                break
    return ros_paths


def _iter_headers(root: str):
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        stack.append(e.path)
                    elif e.is_file(follow_symlinks=False) and e.name.endswith(('.h', '.hpp')):
                        yield e.path
        except (FileNotFoundError, PermissionError, NotADirectoryError):
            pass


@lru_cache(maxsize=1024)
def _find_header_once(basename: str, search_root: str) -> Optional[str]:
    for path in _iter_headers(search_root):
        if os.path.basename(path) == basename:
            return path
    return None


@lru_cache(maxsize=1024)
def normalize_sdg_sdc_header_path(header_path: str) -> str:
    return header_path.removeprefix("sdg/sdc/")


def find_header_in_ros_paths(
    header_basename: str, ros_paths: List[str], source_root: str, build_root: str
) -> Optional[str]:
    # Try build mirrors first
    for ros_base in ROS_HEADER_PATHS:
        root = os.path.join(build_root, ros_base)
        candidate = _find_header_once(header_basename, root)
        if candidate:
            try:
                rel = os.path.relpath(candidate, build_root)
                return normalize_sdg_sdc_header_path(rel)
            except ValueError:
                return normalize_sdg_sdc_header_path(candidate)

    return None


def normalize_header_suggestions(
    suggestions_text: str, include_paths: List[str], source_root: str, build_root: str
) -> str:
    """Resolve bare names to full ROS paths; deduplicate within a section (optimized for single TU)."""
    lines = suggestions_text.split("\n")
    out: List[str] = []
    ros_paths: Optional[List[str]] = None
    # Cache for basename
    resolve_cache: Dict[str, str] = {}  # basename -> full path (or "" if not found)
    seen_in_section: Set[str] = set()
    in_include_section = False

    for line in lines:
        stripped = line.strip()

        # Section headers
        if (
            stripped.endswith(SECTION_SUFFIX_ADD)
            or stripped.endswith(SECTION_SUFFIX_REMOVE)
            or stripped.startswith(SECTION_PREFIX_FULL)
        ):
            seen_in_section.clear()
            in_include_section = True
            out.append(line)
            continue

        # Section boundaries
        if stripped == "---" or (not stripped and in_include_section):
            in_include_section = False
            out.append(line)
            continue

        # Fast guard: skip regex if line doesn't start with "#include"
        lstr = line.lstrip(" -")
        if not lstr.startswith("#include"):
            out.append(line)
            continue

        inc = parse_include_line(line)
        if not inc:
            out.append(line)
            continue

        normalized = inc
        new_line = line

        # Resolve only bare includes
        if "/" not in inc and not inc.startswith("<"):
            if ros_paths is None:
                ros_paths = filter_ros_include_paths(include_paths, source_root)

            full_path = resolve_cache.get(inc)
            if full_path is None:
                fp = find_header_in_ros_paths(inc, ros_paths, source_root, build_root)
                full_path = fp if fp else ""
                resolve_cache[inc] = full_path

            if full_path:
                normalized = full_path
                if lstr.startswith('#include "'):
                    new_line = line.replace(f'"{inc}"', f'"{full_path}"', 1)
                elif lstr.startswith('#include <'):
                    new_line = line.replace(f'<{inc}>', f'"{full_path}"', 1)

        # Deduplication within a section
        if in_include_section:
            if normalized in seen_in_section:
                continue
            seen_in_section.add(normalized)

        out.append(new_line)

    return "\n".join(out)


def make_result(src, code, stderr, stdout):
    return {"file": src, "exit_code": code, "stderr": stderr, "stdout": stdout}


def run_iwyu_with_verbose(args, filtered_clang_cmd, mapping_file):
    proc = run_iwyu_command(args.iwyu_bin, filtered_clang_cmd, args.verbose, mapping_file)
    _, iwyu_verbose_stderr = proc.communicate()
    return _normalize_paths(iwyu_verbose_stderr.decode("utf-8", errors="replace"), args.source_root)


# ------------------------------ main ------------------------------


def main() -> None:
    args, clang_cmd = parse_args()

    if "/retry_cc.py" in str(clang_cmd):
        clang_cmd = list(filter_cmd(clang_cmd))

    generate_outputs(args.iwyu_json)
    if is_generated(args.testing_src, args.build_root):
        return

    filtered_clang_cmd = filter_linker_flags(clang_cmd[1:])
    mapping_file = args.mapping_file or args.default_mapping_file
    testing_src = os.path.relpath(args.testing_src, args.source_root)

    process = run_iwyu_command(args.iwyu_bin, filtered_clang_cmd, DEFAULT_VERBOSE_LEVEL, mapping_file)
    iwyu_raw_stdout, iwyu_raw_stderr = process.communicate()
    stderr = iwyu_raw_stderr.decode("utf-8", errors="replace")
    iwyu_raw_stdout = iwyu_raw_stdout.decode("utf-8", errors="replace")

    # If IWYU printed to stdout, surface as failure
    if iwyu_raw_stdout:
        result = make_result(testing_src, 1, iwyu_raw_stdout, iwyu_raw_stdout)
        with open(args.iwyu_json, "w") as fh:
            json.dump(result, fh, indent=2)
        return

    verbose_text, structured_blocks, global_removes = separate_verbose_from_suggestions(stderr)
    iwyu_ok = bool(structured_blocks)

    if iwyu_ok:
        headers_to_skip = parse_verbose_headers_to_skip(verbose_text)
        iwyu_clean_output = build_clean_output(structured_blocks, headers_to_skip, global_removes)

        include_paths = extract_include_paths(filtered_clang_cmd)
        iwyu_clean_output = normalize_header_suggestions(
            iwyu_clean_output, include_paths, args.source_root, args.build_root
        )
        iwyu_clean_output = _normalize_paths(iwyu_clean_output, args.source_root)
        iwyu_stderr = _normalize_paths(stderr, args.source_root)

        if should_output_verbose_to_stderr(args):
            iwyu_stderr = run_iwyu_with_verbose(args, filtered_clang_cmd, mapping_file)

        exit_code = 0 if not iwyu_clean_output.strip() else 1
        result = make_result(testing_src, exit_code, iwyu_stderr, iwyu_clean_output)
    else:
        err_norm = _normalize_paths(stderr, args.source_root)
        if should_output_verbose_to_stderr(args):
            err_norm = run_iwyu_with_verbose(args, filtered_clang_cmd, mapping_file)
        if ERROR_PATTERNS.search(stderr):
            result = make_result(testing_src, 1, err_norm, err_norm)
        else:
            result = make_result(testing_src, 0, err_norm, "")

    with open(args.iwyu_json, "w") as fh:
        json.dump(result, fh, indent=2)


if __name__ == "__main__":
    main()
