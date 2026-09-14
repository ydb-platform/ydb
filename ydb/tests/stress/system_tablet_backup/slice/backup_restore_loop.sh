#!/bin/bash
#
# Repeatable restore exercise for YDB cluster system tablets.
# Default: enter RECOVERY with DDL running, then copy and restore the backup.
# The older stale/truncated experiment below is available with --stale-restore.
#
# One iteration in --stale-restore mode:
#   1. make sure the cluster is healthy and the DDL workload is running
#   2. copy the tablet's current backup aside      -> this becomes the stale one
#   3. let the workload run on, so the cluster moves ahead of the copy
#   4. truncate the copy's changelog tail          -> records that never arrived
#   5. run the consistency checker (no ledger)     -> report-before.txt
#   6. run doctor                                  -> repaired copy + doctor.txt
#   7. put the tablet in RECOVERY, restart the node
#   8. dry-run the restore, then restore for real
#   9. leave RECOVERY, restart, wait for healthcheck GOOD
#  10. run the checker again                       -> report-after.txt
#  11. reconcile the workload's table list and prove it still works
#
# Staleness and truncation vary per iteration so repeated runs hit different
# cases.  Everything is kept under --outdir/iter-NN for review.
#
# Run it ON the cluster host (it needs /Berkanavt and the local ydb CLI).

set -uo pipefail

# ---------------------------------------------------------------- settings

ITERATIONS=1
RECOVERY_FIRST=1
RECOVERY_DOCTOR=0
START_ITERATION=1
TABLET=root_hive
OUTDIR=~/restore-runs
USE_DOCTOR=1
ALLOW_UNHEALTHY_START=0
ALLOW_DEGRADED=0
FAST=0
MON=http://localhost:8765
GRPC=grpc://localhost:2135
DOMAIN=/Root
CACHE=/Berkanavt/kikimr/cache
STAGE=/Berkanavt/kikimr/restore
CHECKER=${CHECKER:-$HOME/consistency}
WORKLOAD=~/slice_workload.sh
WORKLOAD_LOG=~/workload.log
WORKLOAD_LIVE=~/workload_live.txt
MAX_BACKUP_AGE=3600

usage() {
    cat <<EOF
usage: $0 [options]

  -n N            iterations (default $ITERATIONS)
  --start N       first iteration number (default $START_ITERATION)
  -t TABLET       scheme_shard | root_hive | hive | bscontroller (default $TABLET)
  -o DIR          artifacts directory (default $OUTDIR)
  --max-backup-age SECONDS
                  reject older snapshots; 0 disables (default $MAX_BACKUP_AGE)
  --stale-restore use the older copy-before-recovery, truncate + doctor experiment
  --doctor        repair the Hive copy after RECOVERY, before restoring it
  --no-doctor     disable doctor
  --allow-unhealthy-start
                  proceed when repairing damage from an earlier restore
  --allow-degraded accept GOOD or DEGRADED health (default: GOOD only)
  --fast          10s staleness and shorter observation waits
  -h              this help
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -n) ITERATIONS=$2; shift 2 ;;
        --start) START_ITERATION=$2; shift 2 ;;
        -t) TABLET=$2; shift 2 ;;
        -o) OUTDIR=$2; shift 2 ;;
        --max-backup-age) MAX_BACKUP_AGE=$2; shift 2 ;;
        --stale-restore) RECOVERY_FIRST=0; shift ;;
        --doctor) USE_DOCTOR=1; RECOVERY_DOCTOR=1; shift ;;
        --no-doctor) USE_DOCTOR=0; RECOVERY_DOCTOR=0; shift ;;
        --allow-unhealthy-start) ALLOW_UNHEALTHY_START=1; shift ;;
        --allow-degraded) ALLOW_DEGRADED=1; shift ;;
        --fast) FAST=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage; exit 2 ;;
    esac
done

case "$TABLET" in
    scheme_shard) TABLET_ID=72057594046678944; BOOT_TYPE=FLAT_SCHEMESHARD ;;
    hive|root_hive)
                  TABLET=root_hive
                  BACKUP_TABLET=hive
                  TABLET_ID=72057594037968897
                  BOOT_TYPE=FLAT_HIVE
                  ;;
    bscontroller) TABLET_ID=72057594037932033; BOOT_TYPE=FLAT_BS_CONTROLLER ;;
    *) echo "unsupported tablet: $TABLET" >&2; exit 2 ;;
esac
BACKUP_TABLET=${BACKUP_TABLET:-$TABLET}

mkdir -p "$OUTDIR"

# ---------------------------------------------------------------- helpers

log()  { echo "$(date -u +%H:%M:%S) | $*"; }
fail() { echo "$(date -u +%H:%M:%S) | FAILED: $*" >&2; return 1; }

healthcheck() {
    ydb -e "$GRPC" -d "$DOMAIN" monitoring healthcheck 2>&1 | sed -n '/^Healthcheck status:/p'
}

wait_healthy() {
    local deadline=$((SECONDS + ${1:-180}))
    while [ $SECONDS -lt $deadline ]; do
        case "$(healthcheck)" in
            *GOOD*) return 0 ;;
            *DEGRADED*) [ "$ALLOW_DEGRADED" = 1 ] && return 0 ;;
        esac
        sleep 5
    done
    return 1
}

tablet_page() {
    curl -fsS --max-time 30 "$MON/tablets/app?TabletID=$TABLET_ID"
}

restart_tablet() {
    curl -fsS --max-time 30 "$MON/tablets?RestartTabletID=$TABLET_ID" -o /dev/null || return 1
    sleep 5
}

# Wait until the tablet's app page reports a terminal restore status.
wait_restore() {
    local deadline=$((SECONDS + ${1:-600}))
    while [ $SECONDS -lt $deadline ]; do
        local page
        page=$(tablet_page)
        case "$page" in
            *alert-success*) echo success; return 0 ;;
            *alert-danger*)  echo "error: $(echo "$page" | grep -o 'Restore from[^<]*' | head -1)"; return 1 ;;
            *alert-warning*) echo "warning: $(echo "$page" | grep -o 'Restore from[^<]*' | head -1)"; return 0 ;;
        esac
        sleep 5
    done
    echo "timeout"
    return 1
}

# The config's metadata.version returned by `fetch` is already the value the
# next `replace` must carry, so the file is edited and pushed back unchanged.
set_boot_type() {
    local mode=$1          # RECOVERY | NORMAL
    local cfg=/tmp/cfg-$$.yaml
    wait_config > "$cfg" || return 1

    python3 - "$cfg" "$BOOT_TYPE" "$mode" <<'PY'
import sys
path, boot_type, mode = sys.argv[1], sys.argv[2], sys.argv[3]
out, marker, selected, matches = [], "- type: %s" % boot_type, False, 0
for line in open(path).read().split("\n"):
    if line.lstrip().startswith("- type:"):
        selected = line.strip() == marker
        matches += int(selected)
    if selected and line.strip().startswith("boot_type:"):
        continue
    out.append(line)
    if mode == "RECOVERY" and line.strip() == marker:
        out.append(" " * (len(line) - len(line.lstrip()) + 2) + "boot_type: RECOVERY")
assert matches == 1, "expected exactly one bootstrap entry for " + boot_type
open(path, "w").write("\n".join(out))
PY
    [ "$?" -eq 0 ] || { rm -f "$cfg"; return 1; }

    ydb -y -e "$GRPC" -d "$DOMAIN" admin cluster config replace -f "$cfg" 2>&1 | head -2
    local rc=$?
    rm -f "$cfg"
    return "$rc"
}

restart_node() {
    sudo systemctl restart kikimr || return 1
    local deadline=$((SECONDS + 180))
    while [ $SECONDS -lt $deadline ]; do
        [ "$(systemctl is-active kikimr)" = active ] && return 0
        sleep 5
    done
    return 1
}

in_recovery() {
    curl -fsS --max-time 30 "$MON/tablets?TabletID=$TABLET_ID" | grep -qi recovery
}

wait_recovery() {
    local deadline=$((SECONDS + ${1:-60}))
    while [ $SECONDS -lt $deadline ]; do
        in_recovery && return 0
        sleep 2
    done
    return 1
}

start_workload() {
    pgrep -f "$(basename "$WORKLOAD")" >/dev/null && return 0
    log "starting workload"
    nohup "$WORKLOAD" >/dev/null 2>&1 &
    sleep 10
}

workload_creates() { grep -c 'create ok' "$WORKLOAD_LOG" 2>/dev/null || echo 0; }

# A backup_* directory appears before its initial snapshot is complete.  Such a
# directory may already have a changelog, but the executor cannot restore it.
latest_complete_backup() {
    local dir latest=""
    for dir in "$CACHE/$BACKUP_TABLET/$TABLET_ID"/backup_*; do
        [ -d "$dir/snapshot" ] || continue
        latest=$dir
    done
    [ -n "$latest" ] && printf '%s\n' "$latest"
}

check_backup_age() {
    local backup=$1 age
    [ "$MAX_BACKUP_AGE" -eq 0 ] && return 0
    age=$(( $(date +%s) - $(stat -c %Y "$backup/snapshot") ))
    if [ "$age" -gt "$MAX_BACKUP_AGE" ]; then
        fail "latest complete $TABLET backup is ${age}s old (limit ${MAX_BACKUP_AGE}s)"
        return 1
    fi
}

# Drop names the SchemeShard no longer has: after a stale restore the workload's
# own list is ahead of the cluster, and every drop would fail forever.
reconcile_workload() {
    local have=/tmp/ss-has-$$.txt
    ydb -e "$GRPC" -d "$DOMAIN" scheme ls 2>/dev/null | tr ' ' '\n' | grep '^w_' | sort > "$have"
    if [ -s "$WORKLOAD_LIVE" ]; then
        comm -12 "$have" <(sort "$WORKLOAD_LIVE") > "$WORKLOAD_LIVE.new"
        mv "$WORKLOAD_LIVE.new" "$WORKLOAD_LIVE"
    fi
    rm -f "$have"
}

run_checker() {
    local root=$1 out=$2 extra=${3:-}
    ( cd "$(dirname "$CHECKER")" && \
      python3 -m "$(basename "$CHECKER")" --backup-root "$root" --mon-endpoint "$MON" \
          --json "$out.json" $extra ) \
      > "$out" 2>&1
    echo $?
}

# The text report caps each check at 20 findings, so two very different runs can
# print identical counts.  The JSON keeps the real totals in the summary
# finding's details, which is what makes iterations comparable.
digest() {
    local json=$1
    [ -f "$json" ] || { echo "n/a"; return; }
    python3 - "$json" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
parts = []
for check in report["checks"]:
    findings = [f for f in check["findings"] if f["severity"] != "INFO"]
    if not findings:
        continue
    # A capped check ends in a summary finding carrying the true total.
    total = max((f["details"].get("total", 0) for f in findings), default=0)
    count = max(total, len(findings))
    worst = "C" if any(f["severity"] == "CRITICAL" for f in findings) else \
            "E" if any(f["severity"] == "ERROR" for f in findings) else "W"
    parts.append("%s:%s%d" % (check["id"], worst, count))
print(" ".join(parts) if parts else "clean")
PY
}

# ---------------------------------------------------------------- iteration

run_iteration() {
    local n=$1
    local dir="$OUTDIR/iter-$(printf %02d "$n")"
    rm -rf "$dir"; mkdir -p "$dir/backups"

    # Vary the two knobs that decide which failure classes show up.
    local staleness=$(( 30 + (n % 3) * 60 ))          # 30 / 90 / 150 s
    local truncate_kb=$(( (n % 4) * 16 ))             # 0 / 16 / 32 / 48 KiB
    [ "$FAST" = 1 ] && staleness=10

    log "=== iteration $n: tablet=$TABLET staleness=${staleness}s truncate=${truncate_kb}KiB doctor=$USE_DOCTOR"
    {
        echo "tablet=$TABLET tablet_id=$TABLET_ID"
        echo "staleness_seconds=$staleness truncate_kb=$truncate_kb use_doctor=$USE_DOCTOR"
    } > "$dir/params.txt"

    if [ "$ALLOW_UNHEALTHY_START" = 1 ]; then
        log "cluster is not GOOD; continuing because --allow-unhealthy-start was given"
    else
        wait_healthy 240 || { fail "cluster not GOOD before the iteration"; return 1; }
    fi
    start_workload
    local creates_before; creates_before=$(workload_creates)

    # ---- capture the stale copy
    local src="" name="" attempt
    for attempt in 1 2 3 4 5; do
        src=$(latest_complete_backup)
        [ -n "$src" ] || break
        check_backup_age "$src" || return 1
        name=$(basename "${src%/}")
        rm -rf "$dir/stale.tmp"
        if sudo cp -a "${src%/}" "$dir/stale.tmp" 2>/dev/null; then
            mv "$dir/stale.tmp" "$dir/stale"
            break
        fi
        sleep 1                         # backup rotated between discovery and cp
    done
    [ -d "$dir/stale/snapshot" ] || {
        fail "could not atomically capture a complete $TABLET backup"
        return 1
    }
    log "stale copy from $name"
    sudo chown -R "$USER" "$dir/stale" || return 1

    # ---- let the cluster move ahead
    sleep "$staleness"

    # ---- simulate complete records that never reached the backup.  Cutting at
    # an arbitrary byte makes the backup malformed instead of merely stale.
    if [ "$truncate_kb" -gt 0 ] && [ -f "$dir/stale/changelog.json" ]; then
        local sizes
        sizes=$(python3 - "$dir/stale/changelog.json" "$truncate_kb" <<'PY'
import hashlib
import sys

path, truncate_kb = sys.argv[1], int(sys.argv[2])
with open(path, "rb") as stream:
    data = stream.read()
old_size = len(data)
target = max(0, old_size - truncate_kb * 1024)
cut = data.rfind(b"\n", 0, target + 1)
body = data[:cut + 1] if cut >= 0 else b""
with open(path, "wb") as stream:
    stream.write(body)
with open(path + ".sha256", "w") as stream:
    stream.write(hashlib.sha256(body).hexdigest())
print(old_size, len(body))
PY
        )
        log "truncated changelog $sizes"
    fi

    # ---- assemble a backup root the checker can read: stale target + live rest
    mkdir -p "$dir/backups/$BACKUP_TABLET/$TABLET_ID"
    cp -a "$dir/stale" "$dir/backups/$BACKUP_TABLET/$TABLET_ID/$name"
    local other
    for other in hive scheme_shard bscontroller; do
        [ "$other" = "$BACKUP_TABLET" ] && continue
        sudo cp -a "$CACHE/$other" "$dir/backups/" 2>/dev/null
    done
    sudo chown -R "$USER" "$dir/backups"

    # ---- check before
    local rc_before; rc_before=$(run_checker "$dir/backups" "$dir/report-before.txt")
    log "checker before restore: exit=$rc_before ($(tail -1 "$dir/report-before.txt"))"

    # ---- doctor
    local restore_src="$dir/backups/$BACKUP_TABLET/$TABLET_ID/$name"
    if [ "$USE_DOCTOR" = 1 ]; then
        if ! ( cd "$(dirname "$CHECKER")" && \
          python3 -m "$(basename "$CHECKER")" --backup-root "$dir/backups" \
              --mon-endpoint "$MON" --doctor --doctor-out "$dir/repaired" ) \
          > "$dir/doctor.txt" 2>&1; then
            fail "doctor failed: $(tail -1 "$dir/doctor.txt")"
            return 1
        fi
        if [ -d "$dir/repaired/$BACKUP_TABLET/$TABLET_ID/$name" ]; then
            restore_src="$dir/repaired/$BACKUP_TABLET/$TABLET_ID/$name"
            # Edits render as "  <tablet>/<Table> [key]: col old -> new".
            local edits; edits=$(grep -cE '^ +[a-z_]+/[A-Za-z]+ \[' "$dir/doctor.txt" 2>/dev/null || echo 0)
            log "doctor: $edits edit(s), using repaired copy"
        else
            log "doctor: nothing to repair, restoring the copy as is"
        fi
    fi

    # The backup writer may be stuck on snapshot.tmp, so checking cache after
    # restore can keep selecting an unrelated old snapshot.  Verify the exact
    # repaired input as a stable measure of what doctor achieved.
    local repaired_root="$dir/backups"
    [ -d "$dir/repaired" ] && repaired_root="$dir/repaired"
    local rc_repaired
    rc_repaired=$(run_checker "$repaired_root" "$dir/report-repaired.txt")
    log "checker repaired input: exit=$rc_repaired ($(tail -1 "$dir/report-repaired.txt"))"

    # ---- stage where the tablet process can read it
    sudo rm -rf "$STAGE/$name" || return 1
    sudo mkdir -p "$STAGE" || return 1
    sudo cp -a "$restore_src" "$STAGE/$name" || {
        fail "could not stage restore source $restore_src"
        return 1
    }
    sudo chown -R kikimr "$STAGE" || return 1

    # ---- into RECOVERY
    log "switching $BOOT_TYPE to RECOVERY"
    set_boot_type RECOVERY > "$dir/config-recovery.txt" 2>&1 || {
        fail "could not switch $BOOT_TYPE to recovery mode"
        return 1
    }
    restart_node || { fail "node did not come back"; return 1; }
    wait_recovery || { fail "tablet is not in recovery mode"; return 1; }

    # ---- dry run, then the real thing
    local encoded; encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$STAGE/$name")
    restart_tablet
    curl -s -X POST "$MON/tablets/app?TabletID=$TABLET_ID&dryRun=1&restoreBackup=$encoded" -o /dev/null
    local dry; dry=$(wait_restore 600)
    log "dry run: $dry"
    echo "$dry" > "$dir/dryrun.txt"

    local restore_status="skipped (dry run failed)"
    if [ "${dry%%:*}" != "error" ] && [ "$dry" != timeout ]; then
        restart_tablet
        curl -s -X POST "$MON/tablets/app?TabletID=$TABLET_ID&restoreBackup=$encoded" -o /dev/null
        restore_status=$(wait_restore 900)
        log "restore: $restore_status"
    fi
    echo "$restore_status" > "$dir/restore.txt"

    # ---- back to normal
    log "switching $BOOT_TYPE back to normal"
    set_boot_type NORMAL > "$dir/config-normal.txt" 2>&1 || {
        fail "could not switch $BOOT_TYPE back to normal mode"
        return 1
    }
    restart_node || { fail "node did not come back after recovery"; return 1; }
    wait_healthy 300 || { fail "cluster not GOOD after restore"; return 1; }

    case "$dry" in
        success|warning:*) ;;
        *) fail "restore dry run did not succeed: $dry"; return 1 ;;
    esac
    case "$restore_status" in
        success|warning:*) ;;
        *) fail "restore did not succeed: $restore_status"; return 1 ;;
    esac

    # ---- check after, against freshly written backups
    [ "$FAST" = 1 ] && sleep 5 || sleep 30
    rm -rf "$dir/backups-after"; mkdir -p "$dir/backups-after"
    for other in hive scheme_shard bscontroller; do
        sudo cp -a "$CACHE/$other" "$dir/backups-after/" 2>/dev/null
    done
    sudo chown -R "$USER" "$dir/backups-after"
    local rc_after; rc_after=$(run_checker "$dir/backups-after" "$dir/report-after.txt")
    log "checker after restore: exit=$rc_after ($(tail -1 "$dir/report-after.txt"))"

    # ---- the cluster has to keep serving the workload
    reconcile_workload
    [ "$FAST" = 1 ] && sleep 10 || sleep 45
    local creates_after; creates_after=$(workload_creates)
    local progressed=$(( creates_after - creates_before ))
    log "workload created $progressed table(s) across the iteration"

    {
        echo "dry_run=$dry"
        echo "restore=$restore_status"
        echo "checker_before=$(tail -1 "$dir/report-before.txt")"
        echo "checker_after=$(tail -1 "$dir/report-after.txt")"
        echo "findings_before=$(digest "$dir/report-before.txt.json")"
        echo "findings_repaired=$(digest "$dir/report-repaired.txt.json")"
        echo "findings_after=$(digest "$dir/report-after.txt.json")"
        echo "workload_creates=$progressed"
        echo "healthcheck=$(healthcheck)"
    } > "$dir/summary.txt"

    [ "$progressed" -gt 0 ] || { fail "workload made no progress after the restore"; return 1; }
    return 0
}

# Recovery-first implementation shares the existing config/restore helpers.
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/recovery_first.sh"

# ---------------------------------------------------------------- main

if [ "$RECOVERY_FIRST" = 1 ]; then
    run_recovery_first_series
    exit $?
fi

log "artifacts: $OUTDIR"
passed=0; failed=0
for i in $(seq "$START_ITERATION" $((START_ITERATION + ITERATIONS - 1))); do
    if run_iteration "$i"; then
        passed=$((passed + 1))
    else
        failed=$((failed + 1))
        log "iteration $i FAILED, continuing"
    fi
done

echo
echo "======================================================================================"
printf "%-8s %-6s %-6s %-9s %-6s %s\n" ITER STALE TRUNC RESTORE WORKL "FINDINGS  before -> after"
for d in "$OUTDIR"/iter-*/; do
    [ -f "$d/summary.txt" ] || continue
    printf "%-8s %-6s %-6s %-9s %-6s %s -> %s\n" \
        "$(basename "$d")" \
        "$(grep -o 'staleness_seconds=[0-9]*' "$d/params.txt" | cut -d= -f2)s" \
        "$(grep -o 'truncate_kb=[0-9]*' "$d/params.txt" | cut -d= -f2)K" \
        "$(grep '^restore=' "$d/summary.txt" | cut -d= -f2- | cut -c1-8)" \
        "$(grep '^workload_creates=' "$d/summary.txt" | cut -d= -f2-)" \
        "$(grep '^findings_before=' "$d/summary.txt" | cut -d= -f2-)" \
        "$(grep '^findings_after=' "$d/summary.txt" | cut -d= -f2-)"
done
echo "======================================================================================"
echo "$passed iteration(s) ok, $failed failed"
echo
echo "FINDINGS read as <check>:<worst severity><real count>, e.g. I6:C37 = I6 critical, 37"
echo "occurrences.  A clean cycle clears every C and leaves only the referential E findings,"
echo "which need reconciliation in the running cluster and cannot be fixed in a backup file."
[ "$failed" -eq 0 ]
