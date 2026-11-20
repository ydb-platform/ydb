# Sourced by the sequencing experiment; uses the current attempt paths.
start_ta_profile() {
TA_PROFILE_PID=""
if [ "$RETRY" = 1 ]; then
  TA_PROFILE_STOP="$TMP_DIR/ta-profile.stop"
  rm -f "$TA_PROFILE_STOP"
  mkdir -p "$CURRENT_PUBLIC_DIR/ta_profile"
  sudo -n -E python3 .github/scripts/tests/profile_ta.py \
    "$CURRENT_PUBLIC_DIR/ya_evlog.jsonl" "$CURRENT_PUBLIC_DIR/ta_profile" \
    "$TMP_DIR/ta-profile-private" "$TA_PROFILE_STOP" \
    > "$CURRENT_PUBLIC_DIR/ta_profile/collector.log" 2>&1 &
  TA_PROFILE_PID=$!
fi
}

stop_ta_profile() {
if [ -n "$TA_PROFILE_PID" ]; then
  touch "$TA_PROFILE_STOP"
  wait "$TA_PROFILE_PID" || echo "TA profiler failed; see ta_profile/collector.log"
  sudo chown -R "$(id -u):$(id -g)" "$CURRENT_PUBLIC_DIR/ta_profile" "$TMP_DIR/ta-profile-private"
fi
}
