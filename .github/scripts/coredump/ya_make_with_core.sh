#!/bin/bash
# systemd-run --scope does not inherit shell ulimit, and sudo -u resets RLIMIT_CORE.
# Raise core unlimited as root, drop to the CI user via setpriv (keeps groups),
# raise again as that user, then exec ya make.
set -euo pipefail

UID_TO="$1"
GID_TO="$2"
shift 2

ulimit -c unlimited
exec setpriv --reuid="$UID_TO" --regid="$GID_TO" --init-groups -- \
  bash -c 'ulimit -c unlimited; exec "$@"' bash "$@"
