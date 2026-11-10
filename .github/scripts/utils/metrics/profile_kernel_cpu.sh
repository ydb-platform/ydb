# Sourced by the diagnostic PR action; keeps its run block below the Actions limit.
case "$1" in
  start)
    KERNEL_PROFILE_PID=""
    if [ "$RETRY" = 1 ]; then
      KERNEL_PROFILE_DIR="$CURRENT_PUBLIC_DIR/kernel-profile"
      mkdir -p "$KERNEL_PROFILE_DIR"
      # Diagnostic PR only: sample remaining kernel CPU without a utilization threshold.
      (
        if ! perf --version; then
          sudo -n timeout 180 apt-get update
          sudo -n timeout 180 apt-get install -y "linux-tools-$(uname -r)" || \
            sudo -n timeout 180 apt-get install -y linux-tools-generic
        fi
        perf_bin=$(command -v perf)
        if ! "$perf_bin" --version; then
          perf_bin=$(find /usr/lib/linux-tools -type f -name perf | sort -V | tail -1)
        fi
        "$perf_bin" --version
        exec python3 -u .github/scripts/utils/metrics/profile_kernel_cpu.py \
          "$KERNEL_PROFILE_DIR" "$RESOURCES_JSONL" "$perf_bin"
      ) > "$KERNEL_PROFILE_DIR/collector.log" 2>&1 &
      KERNEL_PROFILE_PID=$!
    fi
    ;;
  stop)
    if [ -n "$KERNEL_PROFILE_PID" ]; then
      touch "$KERNEL_PROFILE_DIR/stop"
      wait "$KERNEL_PROFILE_PID" || true
    fi
    ;;
esac
