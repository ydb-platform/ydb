BUILD_ONLY_IF(OS_EMSCRIPTEN)

EXPORT_ALL_DYNAMIC_SYMBOLS()

# We want to be able to link several arbitrary .so files using some WebAssembly VM.
# Some of these .so files will depend on the SDK .so, while the SDK itself will define the necessary symbols.
#
# Build with Arcadia/YT-compatible flags (see ydb/services/udf_store/wasm/ya_make
# and arcadia_compat.flags), otherwise opensource ya.conf may emit Tag section ID 13
# for __cpp_exception and WAVM will reject the module.
LDFLAGS(-Wl,--allow-undefined)
LD_PLUGIN(ydb/tests/functional/udf_store/sdk/ld_plugin.py)
