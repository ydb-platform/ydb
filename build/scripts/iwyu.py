import argparse
import json
import os
import re
import subprocess
from functools import lru_cache
from typing import Optional, Dict, List, Set, Tuple

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

PRIVATE_MARKERS = ("src/", "internal/", "detail/", "impl/", "private/")
PRIVATE_MARKERS_RE = re.compile("|".join(map(re.escape, PRIVATE_MARKERS)))

INCLUDE_PATTERN = re.compile(r"#\s*include\s*[<\"]([^\">]+)[\">]")
COMMENT_PATTERN = re.compile(r"//.*")
ERROR_PATTERNS = re.compile(r"(error:|failed)", re.IGNORECASE)
VERBOSE_DEFINED_RE = re.compile(
    r"^(?P<file>/.+?):(?P<line>\d+):\d+:\s+warning:\s+(?P<symbol>.+?)\s+is "
    r"defined in\s+[<\"](?P<header>[^\">]+)[\">]"
)
ADD_HDR_RE = re.compile(r"^(?P<file>\S.*) should add these lines:$")
REM_HDR_RE = re.compile(r"^(?P<file>\S.*) should remove these lines:$")
FULL_HDR_RE = re.compile(r"^The full include-list for (?P<file>\S.*):$")
FORWARD_DECL_RE = re.compile(
    r"^\s*(?:"
    r"namespace\s+[A-Za-z_]\w*(?:::[A-Za-z_]\w*)*\s*\{.*\}\s*;?"
    r"|(?:class|struct|enum(?:\s+class|\s+struct)?)\s+[A-Za-z_]\w*(?:\s*;|[^;]*;)"
    r")\s*$"
)
DEFAULT_VERBOSE_LEVEL = "3"


def parse_args():
    """Parse command line arguments for IWYU wrapper."""
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
    """Create empty .o and .json output files."""
    open(os.path.splitext(path)[0] + ".o", "w").close()
    open(path, "w").close()


def filter_cmd(cmd):
    """Filter clang command args, skip until /retry_cc.py."""
    skip = True
    for x in cmd:
        if not skip:
            yield x
        elif "/retry_cc.py" in x:
            skip = False


def filter_linker_flags(cmd):
    """Drop unwanted tool/compiler flags."""

    def drop(a: str) -> bool:
        return (
            ("/python" in a and (a.endswith("/python") or "/python3" in a))
            or (("/clang" in a) and (a.endswith("/clang++") or a.endswith("/clang")))
            or ("/.ya/tools/" in a)
        )

    return [a for a in cmd if not drop(a)]


def is_generated(testing_src: str, build_root: str) -> bool:
    """Return True if file is under build_root."""
    return testing_src.startswith(build_root)


@lru_cache(maxsize=2048)
def parse_include_line(line: str) -> Optional[str]:
    """Extract header path from a #include line."""
    s = COMMENT_PATTERN.sub("", line.lstrip("- ").strip())
    match = INCLUDE_PATTERN.match(s)
    return match.group(1) if match else None


@lru_cache(maxsize=1024)
def get_matching_key(path: str) -> Optional[str]:
    """Return external prefix key for a header path, or the path itself."""
    if not path:
        return None
    for pfx in EXTERNAL_PREFIXES:
        if path.startswith(pfx):
            return pfx.rstrip("/")
    return None if "/" not in path else path


def _basename(s: str) -> str:
    """Basename of path."""
    i = s.rfind("/")
    return s if i < 0 else s[i + 1 :]


def _in_headerset(inc: str, full_set: Set[str], base_set: Set[str]) -> bool:
    """Check membership by full path or basename."""
    return (inc in full_set) or (_basename(inc) in base_set)


def _symbol_token(sym: str) -> str:
    """Last identifier token from symbol (no templates)."""
    sym = re.sub(r"<[^>]*>", "", sym)
    last = sym.split("::")[-1]
    m = re.match(r"(operator)\b", last) or re.search(r"([A-Za-z_]\w*)$", last)
    return m.group(1) if m else last


def _line_contains_token(src_line: str, token: str) -> bool:
    """Check if token appears in the given source line."""
    return re.search(r"\b" + re.escape(token) + r"\b", src_line) is not None


def parse_verbose_headers_to_skip(verbose_text: str) -> Dict[str, Set[str]]:
    """Map {file -> headers to skip} via verbose symbol location heuristics."""
    files_cache: Dict[str, List[str]] = {}
    used: Dict[str, Dict[str, bool]] = {}

    for line in verbose_text.splitlines():
        match = VERBOSE_DEFINED_RE.match(line.strip())
        if not match:
            continue

        fpath = match.group("file")
        lno = int(match.group("line"))
        token = _symbol_token(match.group("symbol"))
        header = match.group("header")

        if fpath not in files_cache:
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as fh:
                    files_cache[fpath] = fh.readlines()
            except Exception:
                files_cache[fpath] = []

        src = files_cache[fpath]
        src_line = src[lno - 1] if 1 <= lno <= len(src) else ""
        direct = _line_contains_token(src_line, token)

        per_file = used.setdefault(fpath, {})
        per_file[header] = per_file.get(header, False) or direct

    res: Dict[str, Set[str]] = {}
    for fpath, mp in used.items():
        skip = {h for h, direct in mp.items() if not direct}
        if skip:
            res[fpath] = skip
    return res


def _is_forward_decl_line(s: str) -> bool:
    """Detect forward declarations & namespace decls."""
    t = COMMENT_PATTERN.sub("", s).strip()
    return t.startswith("namespace ") or bool(FORWARD_DECL_RE.match(t))


def separate_verbose_from_suggestions(iwyu_full_output: str) -> Tuple[str, str]:
    """Split IWYU stderr into (verbose_warnings, raw_suggestions)."""
    lines = iwyu_full_output.splitlines()
    verbose: List[str] = []
    sugg: List[str] = []

    IN_NONE, IN_ADD, IN_REMOVE, IN_FULL = 0, 1, 2, 3
    state = IN_NONE

    def state_of(s: str) -> int:
        if " should add these lines:" in s:
            return IN_ADD
        if " should remove these lines:" in s:
            return IN_REMOVE
        if s.startswith("The full include-list for "):
            return IN_FULL
        return IN_NONE

    for s in lines:
        st = state_of(s)
        if st:
            state = st
            sugg.append(s)
            continue

        if VERBOSE_DEFINED_RE.match(s.strip()):
            verbose.append(s)
            continue

        if state == IN_NONE:
            continue

        t = s.strip()
        if not t:
            sugg.append(s)
            continue

        if state == IN_REMOVE and t.startswith("-"):
            sugg.append(s)
            continue

        if INCLUDE_PATTERN.match(t) or INCLUDE_PATTERN.match(t.lstrip("- ").strip()):
            sugg.append(s)
            continue

        if state in (IN_ADD, IN_FULL) and _is_forward_decl_line(s):
            sugg.append(s)
            continue

        if state == IN_FULL and t.startswith("("):
            sugg.append(s)
            continue

    return "\n".join(verbose), "\n".join(sugg)


def _stream_blocks_by_file(text: str) -> dict:
    """Group suggestion lines by file into add/remove/full sections."""
    per_file, cur_file, cur_kind = {}, None, None

    def ensure(f: str) -> dict:
        if f not in per_file:
            per_file[f] = {
                "add": {"hdr": None, "lines": []},
                "remove": {"hdr": None, "lines": []},
                "full": {"hdr": None, "lines": []},
            }
        return per_file[f]

    for s in text.splitlines():
        m = ADD_HDR_RE.match(s)
        if m:
            cur_file = m.group("file")
            cur_kind = "add"
            ensure(cur_file)["add"]["hdr"] = s
            continue

        m = REM_HDR_RE.match(s)
        if m:
            cur_file = m.group("file")
            cur_kind = "remove"
            ensure(cur_file)["remove"]["hdr"] = s
            continue

        m = FULL_HDR_RE.match(s)
        if m:
            cur_file = m.group("file")
            cur_kind = "full"
            ensure(cur_file)["full"]["hdr"] = s
            continue

        if cur_file and cur_kind:
            ensure(cur_file)[cur_kind]["lines"].append(s)

    return per_file


def _is_private_like(inc: str) -> bool:
    """Heuristic: external include looks private (src/internal/detail/impl/private)."""
    if not inc or not get_matching_key(inc) or "/" not in inc:
        return False
    rest = inc.split("/", 1)[1]
    return bool(PRIVATE_MARKERS_RE.search(rest))


def build_clean_output(raw_suggestions: str, headers_to_skip: Dict[str, Set[str]]) -> str:
    """Build cleaned IWYU suggestions with verbose and private-header filtering."""
    blocks = _stream_blocks_by_file(raw_suggestions)
    out: List[str] = []

    for file_name, sect in blocks.items():
        skip_full = set(headers_to_skip.get(file_name, set()))
        skip_base = {_basename(x) for x in skip_full}

        add_hdr, rem_hdr, full_hdr = (
            sect["add"]["hdr"],
            sect["remove"]["hdr"],
            sect["full"]["hdr"],
        )
        add_lines, rem_lines, full_lines = (
            sect["add"]["lines"],
            sect["remove"]["lines"],
            sect["full"]["lines"],
        )

        # ADD: drop verbose-false-positives and private-like includes
        add_kept: List[str] = []
        removed_verbose: Set[str] = set()
        private_keys: Set[str] = set()

        if add_hdr:
            for line in add_lines:
                inc = parse_include_line(line)
                if not inc:
                    add_kept.append(line)
                    continue
                if _in_headerset(inc, skip_full, skip_base):
                    removed_verbose.add(inc)
                    continue
                if _is_private_like(inc):
                    key = get_matching_key(inc)
                    if key:
                        private_keys.add(key)
                    continue
                add_kept.append(line)

        # REMOVE: keep as-is, except when replacing facade with private from same key
        rem_kept: List[str] = []
        facades_for_full: List[str] = []

        if rem_hdr:
            for line in rem_lines:
                inc = parse_include_line(line)
                if not inc:
                    rem_kept.append(line)
                    continue
                key = get_matching_key(inc)
                if key in private_keys and not _is_private_like(inc):
                    clean = COMMENT_PATTERN.sub("", line.lstrip("-").strip()).strip()
                    if clean:
                        facades_for_full.append(clean)
                    continue
                rem_kept.append(line)

        # If remove is empty after our filtering — file is OK, suppress everything
        if rem_hdr and not any(line.strip() for line in rem_kept):
            continue

        removed_incs = [parse_include_line(line) for line in rem_kept if parse_include_line(line)]
        removed_full = set(removed_incs)
        removed_base = {_basename(x) for x in removed_incs}

        # FULL: drop verbose-false-positives, removed ones, and private-like; restore facades
        full_kept: List[str] = []
        if full_hdr:
            for line in full_lines:
                t = line.strip()
                if not t:
                    continue
                if t.startswith("(") and "has correct #includes/fwd-decls" in t:
                    continue

                inc = parse_include_line(line)
                if inc:
                    if inc in removed_verbose:
                        continue
                    if _in_headerset(inc, skip_full, skip_base):
                        continue
                    if _in_headerset(inc, removed_full, removed_base):
                        continue
                    if _is_private_like(inc):
                        continue

                full_kept.append(line)

            existing = {parse_include_line(line) for line in full_kept if parse_include_line(line)}
            for clean in facades_for_full:
                inc = parse_include_line(clean)
                if inc and inc not in existing:
                    full_kept.append(clean)
                    existing.add(inc)

        # Assemble sections for this file
        file_out: List[str] = []
        if add_hdr is not None:
            file_out.append(add_hdr)
            file_out.extend(add_kept)
            file_out.append("")
        if rem_hdr is not None:
            file_out.append(rem_hdr)
            file_out.extend(rem_kept)
            file_out.append("")
        if full_hdr is not None:
            file_out.append(full_hdr)
            file_out.extend(full_kept)
            file_out.append("")

        if file_out and any(x.strip() for x in file_out):
            out.append("\n".join(file_out).rstrip())

    return "\n---\n\n".join(out)


def run_iwyu_command(iwyu_bin: str, filtered_clang_cmd: List[str], verbose: str, mapping_file: str) -> subprocess.Popen:
    """Run IWYU process and return handle."""
    cmd = ["env", "-u", "LD_LIBRARY_PATH", iwyu_bin]
    cmd.extend(filtered_clang_cmd)
    for arg in (
        f"--mapping_file={mapping_file}",
        f"--verbose={verbose}",
        "--cxx17ns",
        "--no_fwd_decls",
    ):
        cmd.extend(["-Xiwyu", arg])
    return subprocess.Popen(stdout=subprocess.PIPE, stderr=subprocess.PIPE, args=cmd)


@lru_cache(maxsize=4)
def _norm_prefix(source_root: str) -> str:
    """Normalize source_root to always have trailing slash."""
    root = os.path.normpath(source_root)
    return (root + os.sep) if not root.endswith(os.sep) else root


def _normalize_paths(text: str, source_root: str) -> str:
    """Replace absolute source_root prefix with $(SOURCE_ROOT)/."""
    return re.sub(re.escape(_norm_prefix(source_root)), "$(SOURCE_ROOT)/", text)


def main() -> None:
    """Run IWYU once with verbose=3, filter output, emit JSON."""
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

    verbose_text, raw_suggestions = separate_verbose_from_suggestions(stderr)

    # No IWYU sections: either pure compile error (fail) or silence (ok)
    if not raw_suggestions.strip():
        err_norm = _normalize_paths(stderr, args.source_root)
        if ERROR_PATTERNS.search(stderr):
            result = {
                "file": testing_src,
                "exit_code": 1,
                "stderr": err_norm,
                "stdout": err_norm,
                "iwyu_raw_stdout": iwyu_raw_stdout.decode(errors="replace"),
                "iwyu_raw_stderr": iwyu_raw_stderr.decode(errors="replace"),
            }
        else:
            result = {
                "file": testing_src,
                "exit_code": 0,
                "stderr": err_norm,
                "stdout": "",
            }
        with open(args.iwyu_json, "w") as fh:
            json.dump(result, fh, indent=2)
        return

    # IWYU sections present — parse/clean normally
    headers_to_skip = parse_verbose_headers_to_skip(verbose_text)
    iwyu_clean_output = build_clean_output(raw_suggestions, headers_to_skip)
    iwyu_clean_output = _normalize_paths(iwyu_clean_output, args.source_root)

    iwyu_stderr = _normalize_paths(stderr, args.source_root)

    if args.verbose and args.verbose != DEFAULT_VERBOSE_LEVEL:
        process = run_iwyu_command(args.iwyu_bin, filtered_clang_cmd, args.verbose, mapping_file)
        _, iwyu_verbose_stderr = process.communicate()
        iwyu_stderr = _normalize_paths(iwyu_verbose_stderr.decode("utf-8", errors="replace"), args.source_root)

    result = {
        "file": testing_src,
        "exit_code": 0 if not iwyu_clean_output.strip() else 1,
        "stderr": iwyu_stderr,
        "stdout": iwyu_clean_output,
        "iwyu_raw_stdout": iwyu_raw_stdout.decode(errors="replace"),
        "iwyu_raw_stderr": iwyu_raw_stderr.decode(errors="replace"),
    }
    with open(args.iwyu_json, "w") as fh:
        json.dump(result, fh, indent=2)


if __name__ == "__main__":
    main()
