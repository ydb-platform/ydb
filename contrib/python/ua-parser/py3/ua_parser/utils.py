import platform
import re
from typing import Match, Optional

IS_GRAAL: bool = platform.python_implementation() == "GraalVM"


def get(m: Match[str], idx: int) -> Optional[str]:
    return (m[idx] or None) if 0 < idx <= m.re.groups else None


def replacer(repl: str, m: Match[str]) -> Optional[str]:
    """The replacement rules are frustratingly subtle and innimical to
    standard python fallback semantics:

    - if there is a non-null replacement pattern, then it must be used with
      match groups as template parameters (at indices 1+)
      - the result is stripped
      - if it is an empty string, then it's replaced by a null
    - otherwise fallback to a (possibly optional) match group
    - or null (device brand has no fallback)

    Replacement rules only apply to OS and Device matchers, the UA
    matcher has bespoke replacement semantics for the family (just
    $1), and no replacement for the other fields, either there is a
    static replacement or it falls back to the corresponding
    (optional) match group.

    """
    if not repl:
        return None

    return re.sub(r"\$(\d)", lambda n: get(m, int(n[1])) or "", repl).strip() or None


REPETITION_PATTERN = re.compile(r"\{(0|1)\s*,\s*\d{3,}\}")
CLASS_PATTERN = re.compile(
    r"""
\[[^]]*\\(d|w)[^]]*\]
|
\\(d|w)
""",
    re.VERBOSE,
)


def class_replacer(m: re.Match[str]) -> str:
    d, w = ("0-9", "A-Za-z0-9_") if m[1] else ("[0-9]", "[A-Za-z0-9_]")
    return m[0].replace(r"\d", d).replace(r"\w", w)


def fa_simplifier(pattern: str) -> str:
    """uap-core makes significant use of large bounded repetitions, to
    mitigate catastrophic backtracking.

    However this explodes the number of states (and thus graph size)
    for finite automaton engines, which significantly increases their
    memory use, and for those which use JITs it can exceed the JIT
    threshold and force fallback to a slower engine (seems to be the
    case for graal's TRegex).
    """
    pattern = REPETITION_PATTERN.sub(lambda m: "*" if m[1] == "0" else "+", pattern)
    return CLASS_PATTERN.sub(class_replacer, pattern)
