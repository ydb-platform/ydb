BUILD_ONLY_IF(OS_EMSCRIPTEN)

EXPORT_ALL_DYNAMIC_SYMBOLS()

LDFLAGS(-Wl,--allow-undefined)
LD_PLUGIN(ydb/tests/functional/udf_store/examples/sdk/ld_plugin.py)
