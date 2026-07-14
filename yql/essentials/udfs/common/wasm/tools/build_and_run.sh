#!/usr/bin/env bash
# End-to-end build and run for Wasm UDF examples via the env-driven registry.
#
# Usage:
#   yql/essentials/udfs/common/wasm/tools/build_and_run.sh [options] [<wasm-udf-project-dir>...]
#
# Options:
#   -q, --query FILE       Path to SQL query (default: examples/base64/query.sql).
#   -e, --expected FILE    Path to expected-results file for diff (default: examples/base64/expected.txt).
#   -k, --keep-registry    Don't delete the temporary registry after the run.
#   --with-sdk             Cross-build the shared SDK (libc/libcxx/util) and copy
#                          it into the registry as sdk.so. Required for UDFs that
#                          dynamically link against the SDK (e.g. base64, anything
#                          pulling in libcxx). Disabled by default; without it the
#                          UDFs are loaded into a minimal-runtime compartment.
#   --rebuild-sdk          Force SDK rebuild (implies --with-sdk).
#   --build-type TYPE      ya make --build=TYPE for emscripten targets (default: profile).
#   --fpcast-emu           Post-process each built UDF .so (and SDK .so when --with-sdk)
#                          with `wasm-opt --fpcast-emu`, i.e. the Binaryen pass that
#                          emscripten's tools/link.py runs when EMULATE_FUNCTION_POINTER_CASTS=1.
#                          Requires WASM_OPT to point at a Binaryen wasm-opt binary (>=116).
#                          NOTE: --fpcast-emu only normalizes indirect calls; it does NOT
#                          fix direct wasm<->wasm import ABI mismatches (e.g. i32 vs i64).
#   --precompiled          For PROGRAM(.wasm) UDF targets: after emscripten build, run
#                          `wavm compile --format=precompiled-wasm` and deploy the resulting
#                          precompiled.wasm into the registry instead of a .so. Requires
#                          WAVM_BIN (default: ~/WAVM/build/bin/wavm).
#   -h, --help             Show this help.
#
# Positional arguments are zero or more directories containing a wasm UDF
# project (must have ya.make with DLL() + function_descriptor.yson). Each
# directory may be absolute or arcadia-relative. If none are given, the script
# defaults to examples/base64/module.
#
# Each module ends up in its own subdir of the env registry, named after the
# project basename (e.g. base64 -> Base64::, digest -> Digest::, snowball_stemmer
# -> SnowballStemmer::).
#
# Behaviour:
#   1. (Optional, --with-sdk) Cross-build SDK alone under emscripten-wasm64.
#   2. Cross-build every given UDF module under emscripten-wasm64 (.so or .wasm).
#   2.5. (Optional, --precompiled) Compile each built .wasm with wavm into
#        precompiled.wasm (AOT object code embedded in the module).
#   3. Native-build libwasm_udf.so (so kqprun can load the registry impl).
#   4. Assemble a temporary env-registry directory in YT subdir-layout (sdk.so
#      copied in only when --with-sdk was requested; UDF artifact is precompiled.wasm
#      when --precompiled was requested, otherwise the built .so).
#   5. Run the query through kqprun with YQL_WASM_UDF_REGISTRY_PATH set and
#      optionally diff results.txt against expected.txt.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WASM_UDF_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_EXAMPLE_DIR="$WASM_UDF_DIR/examples/base64"

QUERY_FILE=""
EXPECTED_FILE=""
KEEP_REGISTRY=0
WITH_SDK=0
REBUILD_SDK=0
FPCAST_EMU=0
PRECOMPILED=0
BUILD_TYPE="${YQL_WASM_BUILD_TYPE:-profile}"
declare -a USER_MODULES=()
# WASM_STANDARD_BUILD_DEFINE="-DUSER_CFLAGS=-DLIBGEOBASE_STANDARD_BUILD"
WASM_STANDARD_BUILD_DEFINE=""

usage() {
    sed -n '2,/^set -euo/p' "$0" | sed -n '1,/^$/p' | sed 's/^# \?//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        -q|--query)        QUERY_FILE="$2"; shift 2;;
        -e|--expected)     EXPECTED_FILE="$2"; shift 2;;
        -k|--keep-registry) KEEP_REGISTRY=1; shift;;
        --with-sdk)         WITH_SDK=1; shift;;
        --rebuild-sdk)      REBUILD_SDK=1; WITH_SDK=1; shift;;
        --build-type)       BUILD_TYPE="$2"; shift 2;;
        --fpcast-emu)       FPCAST_EMU=1; shift;;
        --precompiled)      PRECOMPILED=1; shift;;
        -h|--help)         usage; exit 0;;
        --) shift; while [ $# -gt 0 ]; do USER_MODULES+=("$1"); shift; done;;
        -*) echo "Unknown option: $1" >&2; exit 2;;
        *)  USER_MODULES+=("$1"); shift;;
    esac
done

QUERY_FILE="${QUERY_FILE:-$DEFAULT_EXAMPLE_DIR/query.sql}"
EXPECTED_FILE="${EXPECTED_FILE:-$DEFAULT_EXAMPLE_DIR/expected.txt}"
QUERY_FILE="$(cd "$(dirname "$QUERY_FILE")" && pwd)/$(basename "$QUERY_FILE")"
[ -f "$QUERY_FILE" ] || { echo "Query file not found: $QUERY_FILE" >&2; exit 1; }

# Locate arcadia root (directory containing ./ya).
ARCADIA_ROOT="$SCRIPT_DIR"
while [ "$ARCADIA_ROOT" != "/" ] && [ ! -x "$ARCADIA_ROOT/ya" ]; do
    ARCADIA_ROOT="$(dirname "$ARCADIA_ROOT")"
done
[ -x "$ARCADIA_ROOT/ya" ] || {
    echo "Cannot find arcadia root (no ./ya in any parent of $SCRIPT_DIR)" >&2
    exit 1
}

YA="$ARCADIA_ROOT/ya"
SDK_SRC_DIR="$WASM_UDF_DIR/sdk"

# Default to a single base64 module if user didn't pass any.
if [ ${#USER_MODULES[@]} -eq 0 ]; then
    USER_MODULES=("$WASM_UDF_DIR/examples/base64/module")
fi

# Resolve module paths to absolute, validate, and remember registry names.
declare -a MODULE_DIRS=()
declare -a MODULE_NAMES=()
declare -a DESCRIPTOR_FILES=()
for raw in "${USER_MODULES[@]}"; do
    if [ -d "$raw" ]; then
        abs="$(cd "$raw" && pwd)"
    elif [ -d "$ARCADIA_ROOT/$raw" ]; then
        abs="$(cd "$ARCADIA_ROOT/$raw" && pwd)"
    else
        echo "Module dir not found: $raw" >&2
        exit 1
    fi
    if [ ! -f "$abs/ya.make" ]; then
        echo "$abs is not a wasm UDF project (missing ya.make)" >&2
        exit 1
    fi

    descriptor=""
    if [ -f "$abs/function_descriptor.yson" ]; then
        descriptor="$abs/function_descriptor.yson"
    elif [ -f "$abs/../function_descriptor.yson" ]; then
        descriptor="$(cd "$abs/.." && pwd)/function_descriptor.yson"
    fi
    if [ -z "$descriptor" ]; then
        echo "$abs is not a wasm UDF project (missing function_descriptor.yson in dir or parent)" >&2
        exit 1
    fi

    registry_name="$(basename "$abs")"
    if [ "$registry_name" = "wasm" ]; then
        registry_name="$(basename "$(dirname "$abs")")"
    fi

    MODULE_DIRS+=("$abs")
    MODULE_NAMES+=("$registry_name")
    DESCRIPTOR_FILES+=("$descriptor")
done

# kqprun lives in the ydb tree (separate repo). Allow KQPRUN_BIN override.
if [ -z "${KQPRUN_BIN:-}" ]; then
    for candidate in \
        "$HOME/ydbwork/ydb/ydb/tests/tools/kqprun/kqprun" \
        "$ARCADIA_ROOT/ydb/tests/tools/kqprun/kqprun"
    do
        if [ -x "$candidate" ]; then
            KQPRUN_BIN="$candidate"
            break
        fi
    done
fi
[ -n "${KQPRUN_BIN:-}" ] && [ -x "$KQPRUN_BIN" ] || {
    echo "kqprun binary not found. Build it (ya make ydb/tests/tools/kqprun) and set KQPRUN_BIN." >&2
    exit 1
}
KQPRUN_CONFIG_DIR="$(dirname "$KQPRUN_BIN")"

sdk_so_path() {
    ls "$SDK_SRC_DIR"/lib*.so 2>/dev/null | head -1 || true
}

print_sdk_artifact() {
    local so
    so="$(sdk_so_path)"
    if [ -n "$so" ] && [ -e "$so" ]; then
        local real
        real="$(readlink -f "$so")"
        echo "  SDK artifact: $so"
        echo "  SDK resolved: $real ($(stat -c%s "$real") bytes, mtime $(stat -c%y "$real"))"
    else
        echo "  SDK artifact: (missing)"
    fi
}

# Apply the Binaryen pass that emscripten's tools/link.py runs when
# EMULATE_FUNCTION_POINTER_CASTS=1. The pass rewrites the __indirect_function_table
# so all indirectly-callable functions share a "polymorphic" signature. NOTE: it
# does NOT change direct wasm<->wasm import declarations, so it cannot fix
# i32-vs-i64 ABI mismatches on direct imports (e.g. wasm64 libcxx <-> wasm64 libc
# frexpl/ldexpl). For SIDE_MODULE outputs Binaryen sometimes refuses validation
# of the input (because the SIDE_MODULE itself relies on imports the validator
# cannot reconcile); we pass --no-validation to mimic emscripten's pipeline,
# which also defers validation until everything is linked at run time.
apply_fpcast_emu() {
    local so="$1"
    local label="$2"
    [ -n "${WASM_OPT:-}" ] || {
        echo "WASM_OPT is not set; cannot run --fpcast-emu" >&2
        exit 1
    }
    [ -x "$WASM_OPT" ] || {
        echo "WASM_OPT=$WASM_OPT is not executable" >&2
        exit 1
    }
    local resolved size_before size_after
    resolved="$(readlink -f "$so")"
    size_before="$(stat -c%s "$resolved")"
    local tmp="${resolved}.fpcast.tmp"
    "$WASM_OPT" \
        --no-validation \
        --enable-reference-types \
        --enable-bulk-memory \
        --enable-multivalue \
        --debug \
        --enable-exception-handling \
        "$resolved" -o "$tmp"
    mv "$tmp" "$resolved"
    size_after="$(stat -c%s "$resolved")"
    echo "  fpcast-emu: $label $resolved (${size_before} -> ${size_after} bytes)"
}

module_so_path() {
    ls "$1"/lib*.so 2>/dev/null | head -1 || true
}

module_wasm_path() {
    local dir="$1"
    local wasm
    for wasm in "$dir"/*.wasm; do
        [ -e "$wasm" ] || continue
        [ "$(basename "$wasm")" = "precompiled.wasm" ] && continue
        echo "$wasm"
        return 0
    done
}

resolve_wavm_bin() {
    if [ -z "${WAVM_BIN:-}" ]; then
        WAVM_BIN="$HOME/WAVM/build/bin/wavm"
    fi
    [ -x "$WAVM_BIN" ] || {
        echo "wavm binary not found at $WAVM_BIN. Build WAVM or set WAVM_BIN." >&2
        exit 1
    }
}

compile_precompiled_wasm() {
    local input_wasm="$1"
    local output_dir="$2"
    local label="$3"
    local output="${output_dir}/precompiled.wasm"
    resolve_wavm_bin
    local resolved size_before size_after
    resolved="$(readlink -f "$input_wasm")"
    size_before="$(stat -c%s "$resolved")"
    "$WAVM_BIN" compile \
        --format=precompiled-wasm \
        --enable table64 \
        --enable memory64 \
        --enable exception-handling \
        --enable atomics \
        "$resolved" "$output"
    size_after="$(stat -c%s "$output")"
    echo "  wavm compile: $label $output (${size_before} -> ${size_after} bytes)"
}

WASM_MAKE_FLAGS=(
    --target-platform=clang18-emscripten-wasm64
    "--build=${BUILD_TYPE}"
    "-DUSE_VANILLA_PROTOC=yes"
    "-DPROTOBUF_LITE=yes"
    "-DPROTO_LAYOUT=lite"
    "-DUSE_EMPTY_CONDITIONAL_ACTION_PROTO=yes"
    "-DLIBRARY_CPP_PROTOBUF_DEFINE_NPROTOBUF_NAMESPACE=yes"
)
if [ "$WITH_SDK" -eq 1 ]; then
    echo "=== [1/5] Cross-build SDK (clang18-emscripten-wasm64, --build=${BUILD_TYPE}) ==="
    if [ ! -d "$SDK_SRC_DIR" ]; then
        echo "SDK source directory not found: $SDK_SRC_DIR" >&2
        echo "Either remove --with-sdk or check out yql/essentials/udfs/common/wasm/sdk." >&2
        exit 1
    fi
    print_sdk_artifact
    SDK_MAKE_FLAGS=("${WASM_MAKE_FLAGS[@]}")
    if [ "$REBUILD_SDK" -eq 1 ]; then
        SDK_MAKE_FLAGS+=(--rebuild)
        echo "  (--rebuild-sdk: forcing SDK rebuild)"
    fi
    "$YA" make "${SDK_MAKE_FLAGS[@]}" "$SDK_SRC_DIR"
    print_sdk_artifact
    if [ "$FPCAST_EMU" -eq 1 ]; then
        sdk_so_for_pass="$(sdk_so_path)"
        [ -n "$sdk_so_for_pass" ] || {
            echo "SDK .so missing after build; cannot apply --fpcast-emu" >&2
            exit 1
        }
        apply_fpcast_emu "$sdk_so_for_pass" "SDK"
    fi
else
    echo "=== [1/5] SDK build skipped (use --with-sdk to enable) ==="
fi

echo "=== [2/5] Cross-build ${#MODULE_DIRS[@]} UDF module(s) (clang18-emscripten-wasm64, --build=${BUILD_TYPE}) ==="
echo "  modules:"
for i in "${!MODULE_DIRS[@]}"; do
    echo "    [$i] ${MODULE_NAMES[$i]} <- ${MODULE_DIRS[$i]}"
done
"$YA" make "${WASM_MAKE_FLAGS[@]}" "--rebuild" "${MODULE_DIRS[@]}"

if [ "$FPCAST_EMU" -eq 1 ] && [ "$PRECOMPILED" -eq 0 ]; then
    echo "=== [2.5/5] Post-process UDF modules with wasm-opt --fpcast-emu ==="
    for i in "${!MODULE_DIRS[@]}"; do
        module_so="$(module_so_path "${MODULE_DIRS[$i]}")"
        [ -n "$module_so" ] || {
            echo "No .so in ${MODULE_DIRS[$i]} after build" >&2
            exit 1
        }
        apply_fpcast_emu "$module_so" "${MODULE_NAMES[$i]}"
    done
fi

if [ "$PRECOMPILED" -eq 1 ]; then
    echo "=== [2.5/5] Compile UDF .wasm modules into precompiled.wasm (wavm AOT) ==="
    # for i in "${!MODULE_DIRS[@]}"; do
    #     module_wasm="$(module_wasm_path "${MODULE_DIRS[$i]}")"
    #     [ -n "$module_wasm" ] || {
    #         echo "No .wasm in ${MODULE_DIRS[$i]} after build; --precompiled requires PROGRAM(.wasm) targets" >&2
    #         exit 1
    #     }
    #     compile_precompiled_wasm "$module_wasm" "${MODULE_DIRS[$i]}" "${MODULE_NAMES[$i]}"
    # done
fi

echo "=== [3/5] Native-build wasm UDF library ==="
"$YA" make "$WASM_UDF_DIR"

SDK_SO=""
if [ "$WITH_SDK" -eq 1 ]; then
    SDK_SO="$(sdk_so_path)"
    [ -n "$SDK_SO" ] || { echo "SDK .so missing in $SDK_SRC_DIR" >&2; exit 1; }
fi
WASM_UDF_SO="$WASM_UDF_DIR/libwasm_udf.so"
[ -e "$WASM_UDF_SO" ] || { echo "libwasm_udf.so missing in $WASM_UDF_DIR" >&2; exit 1; }
if [ -n "$SDK_SO" ]; then
    echo "  SDK_SO      = $SDK_SO ($(stat -c%s "$(readlink -f "$SDK_SO")") bytes)"
else
    echo "  SDK_SO      = (none, minimal-runtime compartment)"
fi
echo "  WASM_UDF_SO = $WASM_UDF_SO"
echo "  KQPRUN_BIN  = $KQPRUN_BIN"

echo "=== [4/5] Assemble temporary env-registry ==="
REGISTRY="$(mktemp -d -t yql-wasm-registry-XXXXXX)"
if [ "$KEEP_REGISTRY" -eq 0 ]; then
    trap 'rm -rf "$REGISTRY"' EXIT
else
    echo "  (keep-registry mode: directory will survive the script)"
fi

if [ -n "$SDK_SO" ]; then
    cp -L "$SDK_SO" "$REGISTRY/sdk.so"
fi
for i in "${!MODULE_DIRS[@]}"; do
    module_dir="${MODULE_DIRS[$i]}"
    module_name="${MODULE_NAMES[$i]}"
    descriptor="${DESCRIPTOR_FILES[$i]}"
    subdir="$REGISTRY/$module_name"
    mkdir "$subdir"
    if [ "$PRECOMPILED" -eq 1 ]; then
        precompiled_file="$module_dir/precompiled.wasm"
        [ -f "$precompiled_file" ] || {
            echo "precompiled.wasm missing in $module_dir after wavm compile" >&2
            exit 1
        }
        cp -L "$precompiled_file" "$subdir/precompiled.wasm"
    else
        so_file="$(module_so_path "$module_dir")"
        [ -n "$so_file" ] || { echo "No .so in $module_dir after build" >&2; exit 1; }
        cp -L "$so_file" "$subdir/$(basename "$so_file")"
    fi
    cp "$descriptor" "$subdir/function_descriptor.yson"
done

echo "  registry layout:"
( cd "$REGISTRY" && find . -type f -printf '    %p (%s bytes)\n' | sort )

echo "=== [5/5] Run query through kqprun ==="
RESULTS="$(dirname "$QUERY_FILE")/results.txt"
rm -f "$RESULTS"
# kqprun reads ./configuration/app_config.conf relative to CWD by default,
# so run it from its install directory.
pushd "$KQPRUN_CONFIG_DIR" >/dev/null
set +e
YQL_WASM_UDF_REGISTRY_PATH="$REGISTRY" "$KQPRUN_BIN" \
    -p "$QUERY_FILE" \
    -u "$WASM_UDF_SO" \
    --result-file "$RESULTS"
KQPRUN_EXIT=$?
set -e
popd >/dev/null

# kqprun currently throws a benign std::system_error at shutdown after the
# result file is already written; check via file content rather than exit code.
if [ ! -s "$RESULTS" ]; then
    echo "kqprun exited with code $KQPRUN_EXIT and produced no results" >&2
    exit 1
fi

echo
echo "=== Result ($(wc -c < "$RESULTS") bytes) ==="
cat "$RESULTS"
echo

if [ -f "$EXPECTED_FILE" ]; then
    if diff -u "$EXPECTED_FILE" "$RESULTS"; then
        echo "OK: matches $EXPECTED_FILE"
    else
        echo "MISMATCH: $RESULTS differs from $EXPECTED_FILE" >&2
        exit 1
    fi
fi

if [ "$KEEP_REGISTRY" -ne 0 ]; then
    echo "Registry kept at: $REGISTRY"
fi
