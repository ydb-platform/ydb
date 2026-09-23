BUILD_ONLY_IF(OS_EMSCRIPTEN)

EXPORT_ALL_DYNAMIC_SYMBOLS()

LDFLAGS(-Wl,--allow-undefined)
LD_PLUGIN(ydb/udfs/wasm/common/ld_plugin.py)
