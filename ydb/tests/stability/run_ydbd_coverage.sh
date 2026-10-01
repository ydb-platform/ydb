#!/usr/bin/env bash
# Instrumented ydbd -> ydbd_slice install -> stability test -> collect clang coverage.
#
#   ./ydb/tests/stability/run_ydbd_coverage.sh build
#   ./ydb/tests/stability/run_ydbd_coverage.sh deploy
#   ./ydb/tests/stability/run_ydbd_coverage.sh test
#   ./ydb/tests/stability/run_ydbd_coverage.sh collect
#   ./ydb/tests/stability/run_ydbd_coverage.sh report
#
# Verified against this tree:
#   ya make --clang-coverage instruments PROGRAM sources (-fprofile-instr-generate
#   -fcoverage-mapping) even without -t. --coverage-prefix-filter=ydb +
#   -DCOVERAGE_TARGET_REGEXP='^ydb(/|$)' instruments ydb/** and skips contrib.
# On hosts, static nodes run as user kikimr, slots as kikimr_slot; the binary
# is copied to /Berkanavt/kikimr/bin/kikimr. Coverage dir must be world-writable
# and not under /Berkanavt/kikimr (slot user may not traverse it).

set -euo pipefail

YDB_ROOT="${YDB_ROOT:-/home/pefavel/ydbwork/ydb}"
ARCADIA_ROOT="${ARCADIA_ROOT:-/home/pefavel/ydbwork/arcadia}"
WORK_DIR="${WORK_DIR:-/home/pefavel/ydbwork/ydbd-coverage-run}"
CLUSTER_YAML="${CLUSTER_YAML:-$ARCADIA_ROOT/kikimr/ci/stability/resources/ydb_klg_stability_testing/cluster.yaml}"
if [[ ! -f "$CLUSTER_YAML" && -f "$WORK_DIR/cluster.yaml" ]]; then
  CLUSTER_YAML="$WORK_DIR/cluster.yaml"
fi
# World-writable on-disk dir (not /tmp: may be tmpfs; not /Berkanavt/kikimr: slot user).
REMOTE_COV_DIR="${REMOTE_COV_DIR:-/Berkanavt/ydbd-coverage}"
# %p = pid. Do not use %c: continuous mode needs __llvm_profile_counter_bias,
# which --clang-coverage does not emit. Dump happens on process exit.
REMOTE_PROF_TEMPLATE="${REMOTE_PROF_TEMPLATE:-$REMOTE_COV_DIR/ydbd-%p.profraw}"
SSH_USER="${SSH_USER:-$USER}"
BUILD_TYPE="${BUILD_TYPE:-release}"
COVERAGE_TARGET_REGEXP="${COVERAGE_TARGET_REGEXP:-^ydb(/|$)}"
YA_JOBS="${YA_JOBS:-32}"
LINK_THREADS="${LINK_THREADS:-8}"
YDBD_BIN="${YDBD_BIN:-$WORK_DIR/ydbd}"
YA_OUT="${YA_OUT:-$WORK_DIR/ya-out}"
SLICE_BIN="${SLICE_BIN:-$YDB_ROOT/ydb/tools/ydbd_slice/bin/ydbd_slice}"
YDB_ENDPOINT="${YDB_ENDPOINT:-grpc://ydb-qa-01-klg-026.ydb.yandex.net:2135}"
YDB_DATABASE="${YDB_DATABASE:-/Root/db1}"
WORKLOAD_DURATION="${WORKLOAD_DURATION:-7200}"
# pytest/ya timeout must cover cluster wait + workload + diagnostics.
TEST_TIMEOUT="${TEST_TIMEOUT:-10800}"
COVERAGE_HTML="${COVERAGE_HTML:-0}"
SSH_OPTS=( -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 )

mkdir -p "$WORK_DIR"
LOG="$WORK_DIR/run.log"

log() { printf '[%s] %s\n' "$(date '+%F %T')" "$*" | tee -a "$LOG"; }
die() { log "ERROR: $*"; exit 1; }

ya() {
  (cd "$YDB_ROOT" && ./ya "$@")
}

hosts_from_cluster() {
  python3 - "$CLUSTER_YAML" <<'PY'
import sys
path = sys.argv[1]
try:
    import yaml
except ImportError:
    yaml = None
if yaml is not None:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    for host in cfg.get("hosts") or []:
        name = host.get("name")
        if name:
            print(name)
    raise SystemExit(0)
in_hosts = False
with open(path) as f:
    for line in f:
        if line.startswith("hosts:"):
            in_hosts = True
            continue
        if in_hosts and line and not line.startswith(" ") and not line.startswith("#"):
            break
        if in_hosts and line.lstrip().startswith("- name:"):
            print(line.split(":", 1)[1].strip().strip("'\""))
PY
}

ssh_host() {
  local host="$1"; shift
  ssh "${SSH_OPTS[@]}" -l "$SSH_USER" "$host" "$@"
}

assert_instrumented() {
  local bin="$1"
  [[ -x "$bin" ]] || die "not an executable: $bin"
  # 10+GiB coverage binaries can have huge/odd ELF metadata; readelf -S may fail
  # even when LLVM coverage sections are present. Prefer -WS, then a byte scan.
  if command -v readelf >/dev/null; then
    if readelf -WS "$bin" 2>/dev/null | grep -qE '__llvm_covmap|__llvm_prf'; then
      log "Coverage sections present in $bin"
      return 0
    fi
  fi
  if python3 - "$bin" <<'PY'
import sys
needles = (b"__llvm_covmap", b"__llvm_prf_cnts", b"__llvm_prf_data")
path = sys.argv[1]
with open(path, "rb") as f:
    blob = f.read(256 * 1024 * 1024)
sys.exit(0 if any(n in blob for n in needles) else 1)
PY
  then
    log "Coverage markers present in $bin (byte scan)"
    return 0
  fi
  die "$bin has no __llvm_covmap/__llvm_prf sections — it is not a clang-coverage build"
}

cmd_build() {
  [[ -x "$YDB_ROOT/ya" ]] || die "ya not found at $YDB_ROOT/ya"
  mkdir -p "$WORK_DIR" "$YA_OUT"
  local ya_log="$WORK_DIR/ya-make.log"
  log "Building instrumented ydbd: --build=$BUILD_TYPE --clang-coverage --pic --coverage-prefix-filter=ydb"
  log "COVERAGE_TARGET_REGEXP=$COVERAGE_TARGET_REGEXP  jobs=$YA_JOBS link-threads=$LINK_THREADS"
  log "CFLAGS/LDFLAGS=-mcmodel=large (coverage binary exceeds 2GiB R_X86_64_PC32 even with --pic)"
  log "ya make log: $ya_log"
  local ya_rc=0
  # --pic is not enough: coverage still produces R_X86_64_PC32 out of range (~3GiB).
  # -mcmodel=large switches those relocs to 64-bit.
  ya make -T \
    --build="$BUILD_TYPE" \
    --clang-coverage \
    --pic \
    --coverage-prefix-filter=ydb \
    -DCOVERAGE_TARGET_REGEXP="$COVERAGE_TARGET_REGEXP" \
    -DCFLAGS='-mcmodel=large' \
    -DLDFLAGS='-mcmodel=large' \
    --link-threads="$LINK_THREADS" \
    -j"$YA_JOBS" \
    -o "$YA_OUT" \
    ydb/apps/ydbd >"$ya_log" 2>&1 || ya_rc=$?
  tail -n 30 "$ya_log" | tee -a "$LOG"
  [[ "$ya_rc" -eq 0 ]] || die "ya make failed with exit $ya_rc; see $ya_log"
  local built="$YA_OUT/ydb/apps/ydbd/ydbd"
  [[ -e "$built" ]] || die "ydbd was not produced at $built"
  cp -L "$built" "$YDBD_BIN"
  chmod +x "$YDBD_BIN"
  log "Copied $built -> $YDBD_BIN ($(du -h "$YDBD_BIN" | awk '{print $1}'))"
  assert_instrumented "$YDBD_BIN"
  if [[ ! -e "$SLICE_BIN" ]]; then
    log "Building ydbd_slice"
    ya make -T ydb/tools/ydbd_slice
  fi
  log "Build done. Next (when you are ready): $0 deploy"
}

ensure_binary() {
  [[ -x "$YDBD_BIN" ]] || die "Instrumented ydbd not found: $YDBD_BIN (run: $0 build)"
  assert_instrumented "$YDBD_BIN"
}

hosts_list() {
  local hosts
  hosts="$(hosts_from_cluster)"
  [[ -n "$hosts" ]] || die "No hosts parsed from $CLUSTER_YAML"
  printf '%s\n' "$hosts"
}

remote_prepare_coverage_env() {
  local host="$1"
  log "Set LLVM_PROFILE_FILE + TimeoutStopSec=300 on $host (kikimr.cfg + slot env.txt)"
  ssh_host "$host" "sudo bash -s" <<EOF
set -euo pipefail
mkdir -p '$REMOTE_COV_DIR'
chmod 1777 '$REMOTE_COV_DIR'
# Static unit sources kikimr.cfg, not kikimr.env. Slots use env.txt.
export_line='export LLVM_PROFILE_FILE=$REMOTE_PROF_TEMPLATE'
env_line='LLVM_PROFILE_FILE=$REMOTE_PROF_TEMPLATE'
cfg=/Berkanavt/kikimr/cfg/kikimr.cfg
if [[ -f "\$cfg" ]]; then
  if grep -q '^export LLVM_PROFILE_FILE=' "\$cfg"; then
    sed -i "s|^export LLVM_PROFILE_FILE=.*|\$export_line|" "\$cfg"
  else
    printf '\\n%s\\n' "\$export_line" >> "\$cfg"
  fi
fi
upsert() {
  local envf="\$1"
  mkdir -p "\$(dirname "\$envf")"
  touch "\$envf"
  if grep -q '^LLVM_PROFILE_FILE=' "\$envf" 2>/dev/null; then
    sed -i "s|^LLVM_PROFILE_FILE=.*|\$env_line|" "\$envf"
  else
    printf '%s\\n' "\$env_line" >> "\$envf"
  fi
}
upsert /Berkanavt/kikimr/cfg/kikimr.env
for envf in /Berkanavt/kikimr_*/env.txt; do
  [[ -e "\$envf" ]] || continue
  upsert "\$envf"
done
mkdir -p /etc/systemd/system/kikimr.service.d /etc/systemd/system/kikimr-multi@.service.d
printf '%s\\n' '[Service]' 'TimeoutStopSec=300' > /etc/systemd/system/kikimr.service.d/coverage-stop.conf
printf '%s\\n' '[Service]' 'TimeoutStopSec=300' > /etc/systemd/system/kikimr-multi@.service.d/coverage-stop.conf
systemctl daemon-reload
systemctl try-restart kikimr.service || true
systemctl list-units --type=service --state=running --no-legend 'kikimr-multi@*' | awk '{print \$1}' | while read -r u; do
  [[ -n "\$u" ]] || continue
  systemctl restart "\$u" || true
done
EOF
}

cmd_deploy() {
  ensure_binary
  [[ -f "$CLUSTER_YAML" ]] || die "cluster yaml not found: $CLUSTER_YAML"
  [[ -x "$SLICE_BIN" ]] || die "ydbd_slice not found: $SLICE_BIN (ya make ydb/tools/ydbd_slice)"
  log "Installing $YDBD_BIN as /Berkanavt/kikimr/bin/kikimr"
  (cd "$YDB_ROOT" && "$SLICE_BIN" install "$CLUSTER_YAML" --binary "$YDBD_BIN" -y)
  local host
  while IFS= read -r host; do
    remote_prepare_coverage_env "$host"
  done < <(hosts_list)
  log "Deploy done. Next: $0 test"
}

cmd_test() {
  [[ -x "$YDB_ROOT/ya" ]] || die "ya not found at $YDB_ROOT/ya"
  local allure_dir="${ALLURE_DIR:-$WORK_DIR/allure_tmp}"
  mkdir -p "$allure_dir"
  log "Running stability test (nemesis_false) duration=${WORKLOAD_DURATION}s timeout=${TEST_TIMEOUT}s endpoint=$YDB_ENDPOINT"
  log "Allure: $allure_dir"
  (
    cd "$YDB_ROOT"
    ./ya make -ttt \
      --pytest-args=--timeout="$TEST_TIMEOUT" \
      --test-size-timeout="large=$TEST_TIMEOUT" \
      --test-param "workload_duration=$WORKLOAD_DURATION" \
      ydb/tests/stability/tests \
      --test-tag ya:manual \
      '--test-filter=*TestWorkloadParallel::test_all_workloads_parallel*nemesis_false*' \
      --test-param event_process_mode=send \
      --test-param cluster_log=all \
      --test-param cluster_path="$CLUSTER_YAML" \
      --test-param "ydb-db=$YDB_DATABASE" \
      --test-param "ydb-endpoint=$YDB_ENDPOINT" \
      --test-param save_san_logs_in_html=true \
      --test-param ignore_stderr_content=true \
      --allure "$allure_dir" \
      --test-param nemesis-static-location="$YDB_ROOT/ydb/tests/stability/nemesis/static" \
      --test-param stress-utils-to-run=SimpleQueue,Cdc,Statistics,Log,Kv,Mixed,Tpcc,Viewer
  )
  log "Test finished. Next: $0 collect"
}

remote_stop_ydbd() {
  local host="$1"
  log "Graceful stop kikimr on $host (systemctl stop waits for dump; TimeoutStopSec=300)"
  ssh_host "$host" "sudo bash -s" <<'EOF'
set +e
systemctl stop kikimr.service
systemctl list-units --type=service --no-legend 'kikimr-multi@*' | awk '{print $1}' | while read -r u; do
  [[ -n "$u" ]] || continue
  systemctl stop "$u"
done
# process name on hosts is kikimr, not ydbd
pkill -TERM -f '/Berkanavt/kikimr/bin/kikimr' || true
sleep 5
EOF
}

cmd_collect() {
  ensure_binary
  local dumps="$WORK_DIR/profraw"
  rm -rf "$dumps"
  mkdir -p "$dumps"
  local host
  while IFS= read -r host; do
    remote_stop_ydbd "$host"
    mkdir -p "$dumps/$host"
    log "Pull .profraw from $host:$REMOTE_COV_DIR"
    local tarfile="$dumps/$host.tgz"
    if ssh_host "$host" "sudo bash -s" <<EOF >"$tarfile"
set -euo pipefail
cd '$REMOTE_COV_DIR' 2>/dev/null || exit 0
shopt -s nullglob
files=()
for f in *.profraw *.profraw.old; do
  [[ -f "\$f" && -s "\$f" ]] || continue
  files+=( "\$f" )
done
(( \${#files[@]} > 0 )) || exit 0
tar -cf - "\${files[@]}"
EOF
    then
      if [[ -s "$tarfile" ]]; then
        tar -xf "$tarfile" -C "$dumps/$host"
      else
        log "WARN: no .profraw on $host"
      fi
      rm -f "$tarfile"
    else
      log "WARN: failed to pull coverage from $host"
      rm -f "$tarfile"
    fi
  done < <(hosts_list)

  local count
  count="$(find "$dumps" -type f \( -name '*.profraw' -o -name '*.profraw.old' \) | wc -l | tr -d ' ')"
  log "Collected $count profraw files under $dumps"
  [[ "$count" != 0 ]] || die "No .profraw collected. Check LLVM_PROFILE_FILE on hosts and that the installed binary is $YDBD_BIN."
  cmd_report
}

cmd_report() {
  ensure_binary
  local dumps="$WORK_DIR/profraw"
  local merged="$WORK_DIR/default.profdata"
  local html="$WORK_DIR/coverage.report"
  local list
  mapfile -t list < <(find "$dumps" -type f \( -name '*.profraw' -o -name '*.profraw.old' \) | sort)
  (( ${#list[@]} > 0 )) || die "No profraw files in $dumps"
  log "Merging ${#list[@]} profraw files"
  ya tool llvm-profdata merge -sparse "${list[@]}" -o "$merged"
  if [[ "$COVERAGE_HTML" == "1" ]]; then
    rm -rf "$html"
    log "Writing HTML report to $html"
    ya tool llvm-cov show "$YDBD_BIN" \
      -instr-profile="$merged" \
      -format=html \
      -output-dir="$html" \
      -Xdemangler=c++filt \
      -Xdemangler=-n \
      -ignore-filename-regex='(^|/)(contrib|third_party)/'
    log "Report: $html/index.html"
  else
    log "Skipping HTML (set COVERAGE_HTML=1 to generate $html)"
  fi
  ya tool llvm-cov report "$YDBD_BIN" \
    -instr-profile="$merged" \
    -ignore-filename-regex='(^|/)(contrib|third_party)/' \
    | tee "$WORK_DIR/coverage.summary.txt"
}

usage() {
  cat <<EOF
Usage: $0 {build|deploy|test|collect|report}

Paths:
  YDB_ROOT=$YDB_ROOT
  CLUSTER_YAML=$CLUSTER_YAML
  WORK_DIR=$WORK_DIR
  YDBD_BIN=$YDBD_BIN
  REMOTE_COV_DIR=$REMOTE_COV_DIR
  LLVM_PROFILE_FILE template: $REMOTE_PROF_TEMPLATE
EOF
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    build) cmd_build ;;
    deploy) cmd_deploy ;;
    test) cmd_test ;;
    collect) cmd_collect ;;
    report) cmd_report ;;
    -h|--help|help) usage ;;
    "") usage; exit 1 ;;
    *) die "unknown command: $cmd" ;;
  esac
}

main "${1:-}"
