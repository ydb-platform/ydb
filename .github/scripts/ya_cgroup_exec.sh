#!/bin/bash
set -euo pipefail
target_uid="$1"
target_gid="$2"
shift 2
cg_rel="$(sed -n 's/^0:://p' /proc/self/cgroup)"
dir="/sys/fs/cgroup${cg_rel}"
mkdir -p "$dir/ya-runtime"
for _ in 1 2 3 4 5 6 7 8; do
  mapfile -t pids < "$dir/cgroup.procs"
  if [ "${#pids[@]}" -eq 0 ]; then
    break
  fi
  for pid in "${pids[@]}"; do
    [ -n "$pid" ] || continue
    echo "$pid" > "$dir/ya-runtime/cgroup.procs" 2>/dev/null || true
  done
done
if ! grep -qw cpu "$dir/cgroup.subtree_control"; then
  echo '+cpu' > "$dir/cgroup.subtree_control"
fi
# Directory ownership is not enough. cgroup v2 only allows a migration when the
# writer can also write cgroup.procs of the common ancestor, and those files
# stay root-owned after chown of the directory itself.
for node in \
  "$dir" "$dir/ya-runtime" \
  "$dir/cgroup.procs" "$dir/cgroup.subtree_control" "$dir/cgroup.threads" \
  "$dir/ya-runtime/cgroup.procs" "$dir/ya-runtime/cgroup.threads"
do
  if [ -e "$node" ]; then
    chown "$target_uid:$target_gid" "$node"
  fi
done
export YA_CPU_CGROUP_ROOT="$dir"
echo "YA_CPU_CGROUP_ROOT=$dir"
exec setpriv --reuid="$target_uid" --regid="$target_gid" --init-groups -- "$@"
