# Sourced by the sequencing experiment; uses the current attempt paths.
start_ta_profile() {
TA_PROFILE_PID=""
if [ "$RETRY" = 1 ]; then
  TA_PROFILE_STOP="$TMP_DIR/ta-profile.stop"
  rm -f "$TA_PROFILE_STOP"
  mkdir -p "$CURRENT_PUBLIC_DIR/ta_profile"
  export TA_TIMINGS_FILE="$CURRENT_PUBLIC_DIR/ta_profile/operations.jsonl"
  export Y_PYTHON_ENTRY_POINT=:main
  # Execute our entry wrapper with ya's embedded Python, then its normal main.
  for i in "${!YA_MAKE_RUN_CMD[@]}"; do
    if [ "${YA_MAKE_RUN_CMD[$i]}" = ./ya ]; then
      YA_MAKE_RUN_CMD=("${YA_MAKE_RUN_CMD[@]:0:$((i+1))}"
        "$PWD/.github/scripts/tests/ta_timed_entry.py"
        "${YA_MAKE_RUN_CMD[@]:$((i+1))}")
      break
    fi
  done
  sudo -n -E python3 .github/scripts/tests/profile_ta.py \
    "$CURRENT_PUBLIC_DIR/ya_evlog.jsonl" "$CURRENT_PUBLIC_DIR/ta_profile" \
    "$TMP_DIR/ta-profile-private" "$TA_PROFILE_STOP" \
    > "$CURRENT_PUBLIC_DIR/ta_profile/collector.log" 2>&1 &
  TA_PROFILE_PID=$!
fi
}

stop_ta_profile() {
unset Y_PYTHON_ENTRY_POINT TA_TIMINGS_FILE
if [ -n "$TA_PROFILE_PID" ]; then
  touch "$TA_PROFILE_STOP"
  wait "$TA_PROFILE_PID" || echo "TA profiler failed; see ta_profile/collector.log"
  sudo chown -R "$(id -u):$(id -g)" "$CURRENT_PUBLIC_DIR/ta_profile" "$TMP_DIR/ta-profile-private"
fi
}
