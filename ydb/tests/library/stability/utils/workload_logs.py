"""Keep workload stdout/stderr on the node instead of in the test process.

``yatest.common.execute`` reads the whole captured stream back into a Python
string when the process exits. Stability runs used to store that string on
every iteration until the test finished, so a long workload on many nodes
exhausted the test machine RAM.

The wrapper below redirects the workload to files on the host that runs it.
The SSH session prints only a short trailer: sizes, a nonempty flag, and
(for a failed run) a base64 tail. A clean success deletes the files.
"""
from __future__ import annotations

import base64
import re
import shlex
import uuid
from typing import NamedTuple, Optional


# Last bytes of a failed run kept in the test process and in the Allure JSON.
TAIL_BYTES = 8192

WORKLOAD_LOG_DIR = "/tmp/stability_workload_logs"

_LOG_ID_RE = re.compile(r"[A-Za-z0-9._-]+")

# Same lines RemoteExecutor._filter_ssh_warnings drops. They must not fail a
# run whose real stderr is empty. The YDB CLI version notice is written by the
# workload itself, not by ssh.
_IGNORED_STDERR_PREFIXES = (
    "Warning: Permanently added",
    "(!) New version of YDB CL",
)

_SCRIPT = r"""set +e
mkdir -p __LOG_DIR__
out=__STDOUT__
err=__STDERR__
( __COMMAND__ ) >"$out" 2>"$err"
rc=$?
echo "STABILITY_RC=$rc"
echo "STABILITY_STDOUT_PATH=$out"
echo "STABILITY_STDERR_PATH=$err"
stdout_bytes=$(stat -c %s "$out" 2>/dev/null || echo -1)
stderr_bytes=$(stat -c %s "$err" 2>/dev/null || echo -1)
echo "STABILITY_STDOUT_BYTES=$stdout_bytes"
echo "STABILITY_STDERR_BYTES=$stderr_bytes"
nonempty=0
if [ "$stderr_bytes" -lt 0 ]; then
nonempty=1
elif [ "$stderr_bytes" -gt 0 ] && grep -a -E -v __GREP_ARGS__ "$err" | grep -q .; then
nonempty=1
fi
echo "STABILITY_STDERR_NONEMPTY=$nonempty"
if [ "$rc" -ne 0 ] || [ "$nonempty" -eq 1 ]; then
echo STABILITY_LOGS_KEPT=1
stdout_tail_b64=$(tail -c __TAIL__ "$out" | base64 | tr -d '\n')
stderr_tail_b64=$(tail -c __TAIL__ "$err" | base64 | tr -d '\n')
echo "STABILITY_STDOUT_TAIL_B64=$stdout_tail_b64"
echo "STABILITY_STDERR_TAIL_B64=$stderr_tail_b64"
else
echo STABILITY_LOGS_KEPT=0
rm -f "$out" "$err"
fi
exit $rc
"""


class WorkloadLogInfo(NamedTuple):
    """Parsed trailer of a redirected workload. Missing fields stay empty."""

    parsed: bool
    exit_code: Optional[int]
    stdout_path: Optional[str]
    stderr_path: Optional[str]
    stdout_bytes: Optional[int]
    stderr_bytes: Optional[int]
    stderr_nonempty: bool
    logs_kept: bool
    stdout_tail: str
    stderr_tail: str


def _ignored_grep_args() -> str:
    """grep -E arguments that drop blank lines and known harmless stderr prefixes."""
    parts = ["-e", shlex.quote(r"^[[:space:]]*$")]
    for prefix in _IGNORED_STDERR_PREFIXES:
        pattern = "^" + re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", prefix)
        parts.extend(["-e", shlex.quote(pattern)])
    return " ".join(parts)


def make_log_id(run_name: str) -> str:
    """Filesystem-safe id. The random suffix avoids collisions across iterations."""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", run_name).strip("._")[:80]
    if not safe:
        safe = "workload"
    return f"{safe}_{uuid.uuid4().hex[:8]}"


def build_redirected_workload_command(
    command: str,
    log_id: str,
    log_dir: str = WORKLOAD_LOG_DIR,
) -> tuple[str, str, str]:
    """Wrap ``command`` so its output stays in files on the executing host.

    Returns the shell script, the stdout path, and the stderr path.
    """
    if not _LOG_ID_RE.fullmatch(log_id):
        raise ValueError(f"unsafe workload log id: {log_id!r}")
    if not log_dir or not log_dir.startswith("/"):
        raise ValueError(f"workload log dir must be absolute: {log_dir!r}")

    stdout_path = f"{log_dir.rstrip('/')}/{log_id}.out"
    stderr_path = f"{log_dir.rstrip('/')}/{log_id}.err"
    script = (
        _SCRIPT
        .replace("__LOG_DIR__", shlex.quote(log_dir))
        .replace("__STDOUT__", shlex.quote(stdout_path))
        .replace("__STDERR__", shlex.quote(stderr_path))
        .replace("__TAIL__", str(TAIL_BYTES))
        .replace("__GREP_ARGS__", _ignored_grep_args())
        .replace("__COMMAND__", command)
    )
    return script, stdout_path, stderr_path


def _decode_tail(value: str) -> str:
    if not value:
        return ""
    # Reject a trailer that somehow grew past one tail. 4/3 expansion + padding.
    max_b64 = ((TAIL_BYTES + 2) // 3) * 4 + 8
    try:
        raw = base64.b64decode(value[:max_b64])
    except Exception:
        return ""
    if len(raw) > TAIL_BYTES:
        raw = raw[-TAIL_BYTES:]
    return raw.decode("utf-8", errors="replace")


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_workload_trailer(stdout: str) -> WorkloadLogInfo:
    """Parse the wrapper trailer. Unrelated stdout lines are ignored."""
    fields: dict[str, str] = {}
    for line in (stdout or "").splitlines():
        if not line.startswith("STABILITY_"):
            continue
        key, sep, value = line.partition("=")
        if sep:
            fields[key] = value

    parsed = "STABILITY_RC" in fields
    logs_kept = fields.get("STABILITY_LOGS_KEPT") == "1"
    return WorkloadLogInfo(
        parsed=parsed,
        exit_code=_parse_int(fields.get("STABILITY_RC")),
        stdout_path=fields.get("STABILITY_STDOUT_PATH") or None,
        stderr_path=fields.get("STABILITY_STDERR_PATH") or None,
        stdout_bytes=_parse_int(fields.get("STABILITY_STDOUT_BYTES")),
        stderr_bytes=_parse_int(fields.get("STABILITY_STDERR_BYTES")),
        stderr_nonempty=fields.get("STABILITY_STDERR_NONEMPTY") == "1",
        logs_kept=logs_kept,
        stdout_tail=_decode_tail(fields.get("STABILITY_STDOUT_TAIL_B64", "")),
        stderr_tail=_decode_tail(fields.get("STABILITY_STDERR_TAIL_B64", "")),
    )
