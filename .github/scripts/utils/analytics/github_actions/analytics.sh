# Sourced by test_ya steps. Must not fail the caller.
ANALYTICS_PY="${ANALYTICS_PY:-.github/scripts/utils/analytics/github_actions/ci_metrics.py}"

analytics() {
  python3 "$ANALYTICS_PY" "$@" || true
}

# Split a ya evlog into ya_build / ya_tests / ya_cache_* and attach them to parent.
analytics_evlog() {
  local evlog="$1"
  local parent="$2"
  local attempt="${3:-}"
  if [ ! -f "$evlog" ]; then
    return 0
  fi
  local args=(
    .github/scripts/utils/analytics/github_actions/ya_evlog_phases.py
    --evlog "$evlog"
    --parent "$parent"
  )
  if [ -n "$attempt" ]; then
    args+=(--ya-attempt "$attempt")
  fi
  python3 "${args[@]}" || true
}

# start → command → end --rc. Returns the command's exit code.
analytics_run() {
  local name="$1"
  shift
  analytics start --name "$name" --source ya_phase
  set +e
  "$@"
  local rc=$?
  set -e
  analytics end --name "$name" --rc "$rc"
  return "$rc"
}
