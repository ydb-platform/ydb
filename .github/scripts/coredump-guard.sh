#!/bin/bash
# Pipe helper for kernel.core_pattern.
# Saves at most MAX coredumps under /coredumps; further dumps are discarded.
# Filename layout matches ya recover path for pipe patterns: /coredumps/%e.%p.%s
#
# Args from core_pattern: %p %s %e
set -u

PID="${1:-unknown}"
SIG="${2:-unknown}"
# Kernel %e is already truncated to 15 chars; keep the same for ya's recover mask.
EXE="$(/usr/bin/basename "${3:-unknown}" | /usr/bin/cut -c1-15)"

DIR=/coredumps
MAX_FILE="${DIR}/.max"
COUNT_FILE="${DIR}/.count"
LOCK_FILE="${DIR}/.lock"
LOG_FILE="${DIR}/.log"

MAX=10
if [[ -f "$MAX_FILE" ]]; then
  MAX="$(/bin/cat "$MAX_FILE" 2>/dev/null || echo 10)"
fi

/bin/mkdir -p "$DIR"

decision="$(
  (
    /usr/bin/flock -x 200
    count="$(/bin/cat "$COUNT_FILE" 2>/dev/null || echo 0)"
    if [[ "$count" -ge "$MAX" ]]; then
      echo "$((count + 1))" >"$COUNT_FILE"
      echo reject
    else
      echo "$((count + 1))" >"$COUNT_FILE"
      echo accept
    fi
  ) 200>"$LOCK_FILE"
)"

if [[ "$decision" == "accept" ]]; then
  # Match yandex_pattern used by library.python.cores when core_pattern is a pipe.
  outfile="${DIR}/${EXE}.${PID}.${SIG}"
  /bin/cat >"$outfile"
  echo "$(/bin/date -Is) saved ${outfile}" >>"$LOG_FILE"
else
  /bin/cat >/dev/null
  echo "$(/bin/date -Is) ignored pid=${PID} sig=${SIG} exe=${EXE} (limit ${MAX})" >>"$LOG_FILE"
fi
