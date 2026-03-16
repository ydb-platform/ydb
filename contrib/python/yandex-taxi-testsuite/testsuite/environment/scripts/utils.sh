#!/bin/sh

PATH=$PATH:/usr/sbin:/sbin
START_STOP_DAEMON=$(which start-stop-daemon 2>/dev/null)

die() {
    echo "$@" >&2
    exit 1
}

if [ "x$TESTSUITE_DEBUG" != "x" ]; then
    set -x
fi

if [ "x$WORKER_SUFFIX" != "x" ]; then
    WORKER_SUFFIX_PATH=/$WORKER_SUFFIX
else
    WORKER_SUFFIX_PATH=""
fi

ulimit_files() {
    local current=$(ulimit -n)
    local limit

    if [ "$current" != "unlimited" -a "$current" -lt "16384" ]; then
        for limit in 4096 8192 16384; do
            if ! ulimit -n $limit 2> /dev/null; then
                break
            fi
        done
    fi
    echo "ulimit -n is set to $(ulimit -n)"
}

stop_daemon() {
    local binary=$1
    local pidfile=$2

    if [ "x$START_STOP_DAEMON" != "x" ]; then
        $START_STOP_DAEMON --stop --retry TERM/5/KILL/3 \
                           -p $pidfile $binary 2> /dev/null >&2
    else
        # MacOS X workaround
        for i in `seq 10`; do
            if [ -f "$pidfile" ]; then
                if [ $i = "1" ]; then
                    kill -TERM $(cat "$pidfile")
                else
                    kill -TERM $(cat "$pidfile") 2> /dev/null >&2
                fi
            else
                break
            fi
            sleep 0.1
        done

        if [ -f "$pidfile" ]; then
            kill -KILL $(cat "$pidfile") 2> /dev/null >&2
            rm -f "$pidfile"
        fi
    fi
}

script_main() {
    case "$1" in
        start)
            start
            ;;
        stop)
            stop || true
            ;;
        *)
            echo "Usage: $0 <start|stop>"
    esac
}

# Returns an absolute path to a file in /tmp subdirectory
# The parent directory is created for the returned path
get_pidfile() {
    local service=$(basename "$0")
    local path=/tmp/taxi-testsuite-$USER/run/$service/${WORKER_SUFFIX}/$1.pid
    mkdir -p "$(dirname "$path")"
    echo $path
}

dump_log() {
    local logfile="$1"
    if [ -f "$logfile" ]; then
        echo ">>> Dumping $logfile:"
        cat "$logfile"
        echo ">>> EOF ($logfile)"
    else
        echo ">>> Log file not found at $logfile"
    fi
}

dump_log_stderr() {
    dump_log "$@" >&2
}

find_binary() {
    which "$1"
}

choose_binaries() {
  which "$1" || which "$2"
}

find_binary_or_die() {
    local binary_name="$1"
    local binary=$(find_binary $binary_name)
    [ -z "$binary" ] && die "No $binary_name binary found"
    echo $binary
}

choose_binaries_or_die() {
  local first_binary_name="$1"
  local second_binary_name="$2"
  local binary=$(choose_binaries $first_binary_name $second_binary_name)
  [ -z "$binary" ] && die "No $first_binary_name or $second_binary_name binary found"
  echo $binary
}
