BUILD_ONLY_IF(OS_EMSCRIPTEN)

EXPORT_ALL_DYNAMIC_SYMBOLS()

# We want to be able to link several arbitrary .so files using some WebAssembly VM.
# Some of these .so files will depend on the SDK .so, while the SDK itself will define the necessary symbols.
LDFLAGS(-Wl,--allow-undefined)
LD_PLUGIN(ydb/services/udf_store/wasm/sdk/ld_plugin.py)
