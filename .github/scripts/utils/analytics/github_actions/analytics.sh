# Sourced by test_ya steps. Must not fail the caller.
ANALYTICS_PY="${ANALYTICS_PY:-.github/scripts/utils/analytics/github_actions/ci_metrics.py}"

analytics() {
  python3 "$ANALYTICS_PY" "$@" || true
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
