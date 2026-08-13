#!/bin/bash
# Raise RLIMIT_CORE as root, drop to the runner user via setpriv, then keep
# soft core unlimited (incl. via LD_PRELOAD) and exec ya make.
set -euo pipefail

UID_TO="$1"
GID_TO="$2"
FORCE_SO="$3"
shift 3

ulimit -c unlimited
exec setpriv --reuid="$UID_TO" --regid="$GID_TO" --init-groups -- \
  env "LD_PRELOAD=${FORCE_SO}${LD_PRELOAD:+:$LD_PRELOAD}" \
  bash -c '
    set -euo pipefail
    ulimit -c unlimited
    python3 -c "import resource; inf=resource.RLIM_INFINITY; resource.setrlimit(resource.RLIMIT_CORE,(inf,inf)); s,h=resource.getrlimit(resource.RLIMIT_CORE); print(\"ya-make rlimit_core=\", (s,h)); assert s == inf or s > 1, (s,h)"
    exec "$@"
  ' bash "$@"
