#!/bin/bash

use_custom_ya_bin() {
  YA_BIN="./ya"
  if [ -n "${CUSTOM_YA_BIN_S3_URI:-}" ]; then
    local custom_dir="${RUNNER_TEMP}/custom-ya"
    rm -rf "$custom_dir"
    mkdir -p "$custom_dir"
    s3cmd get --force "${CUSTOM_YA_BIN_S3_URI}" "$custom_dir/ya-bin.tar.gz"
    tar -xzf "$custom_dir/ya-bin.tar.gz" -C "$custom_dir"
    YA_BIN="$(find "$custom_dir" -type f -name ya-bin | head -1)"
    if [ -z "$YA_BIN" ]; then
      echo "ya-bin not found in ${CUSTOM_YA_BIN_S3_URI}"
      exit 1
    fi
    chmod +x "$YA_BIN"
    export YA_SOURCE_ROOT="${YA_SOURCE_ROOT:-$GITHUB_WORKSPACE}"
    echo "Using custom ya-bin $YA_BIN from ${CUSTOM_YA_BIN_S3_URI}"
  fi
}

install_cpu_quota_wrapper() {
  DELEGATE_CPU=()
  if [ -n "${CUSTOM_YA_BIN_S3_URI:-}" ]; then
    DROP_PRIVS=(bash .github/scripts/ya_cgroup_exec.sh "$(id -u)" "$(id -g)")
    DELEGATE_CPU=(-p Delegate=yes)
  else
    DROP_PRIVS=(setpriv --reuid="$(id -u)" --regid="$(id -g)" --init-groups --)
  fi
  if [ "${COLLECT_COREDUMPS}" = "true" ] || [ -n "${CUSTOM_YA_BIN_S3_URI:-}" ]; then
    YA_MAKE_RUN_CMD=(
      prlimit --core=unlimited:unlimited
      "${DROP_PRIVS[@]}"
      "${YA_MAKE_CMD[@]}"
    )
  else
    YA_MAKE_RUN_CMD=(
      sudo -n -E -u "$(id -un)" -- "${YA_MAKE_CMD[@]}"
    )
  fi
}
