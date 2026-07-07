#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 1 ] || {
  echo "usage: $0 OUT_DIR" >&2
  exit 2
}

out="$1"
mkdir -p "$out"

copy_file() {
  local src="$1"
  local rel="${src#/}"

  [ -f "$src" ] && [ -r "$src" ] || return 0

  mkdir -p "$out/$(dirname "$rel")"
  cat "$src" > "$out/$rel" 2>/dev/null || true
}

# Global CPU state
for f in \
  /sys/devices/system/cpu/online \
  /sys/devices/system/cpu/offline \
  /sys/devices/system/cpu/present \
  /sys/devices/system/cpu/possible \
  /sys/devices/system/cpu/kernel_max \
  /sys/devices/system/cpu/isolated \
  /sys/devices/system/cpu/nohz_full
do
  copy_file "$f"
done

# Per-CPU topology
for f in /sys/devices/system/cpu/cpu[0-9]*/topology/*; do
  copy_file "$f"
done

# Per-CPU capacity, useful on heterogeneous systems
for f in /sys/devices/system/cpu/cpu[0-9]*/cpu_capacity; do
  copy_file "$f"
done

# L3 / LLC cache-sharing info only
for idx in /sys/devices/system/cpu/cpu[0-9]*/cache/index*; do
  [ -d "$idx" ] || continue
  [ "$(cat "$idx/level" 2>/dev/null || true)" = "3" ] || continue

  for f in level type size id shared_cpu_list shared_cpu_map; do
    copy_file "$idx/$f"
  done
done

# NUMA node CPU info
for f in \
  /sys/devices/system/node/node*/cpulist \
  /sys/devices/system/node/node*/cpumap \
  /sys/devices/system/node/node*/distance
do
  copy_file "$f"
done

# Filtered lscpu snapshot
if command -v lscpu >/dev/null; then
  lscpu | grep -Ev '^(Flags|Bugs|Vulnerability|Mitigation)' \
    > "$out/lscpu.txt" || true
fi
