#include <ydb/services/udf_store/wasm/abi/bridge.h>
#include <ydb/services/udf_store/wasm/abi/bridge_abi.h>
#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <stdint.h>

using namespace NYdb::NUdfStore::NAbi;

//! Example bridge UDF: Lookup(Dict<String,Int64>, String) -> Optional<Int64>
//! Demonstrates handle reuse: the same dict handle across rows can be cached
//! by the guest (see CachedDictHandle below).
namespace {

TBridgeHandle CachedDictHandle = 0;
int64_t CachedHits = 0;

} // namespace

extern "C" {

__attribute__((visibility("default"))) void dict_lookup(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t dictH,
    uint64_t keyH)
{
    TBridgeDict dict(dictH, /*owned*/ false);
    TBridgeString key(keyH, /*owned*/ false);

    if (CachedDictHandle == 0) {
        CachedDictHandle = dictH;
        BridgeRef(CachedDictHandle);
    } else if (CachedDictHandle == dictH) {
        ++CachedHits;
    }

    auto payload = dict.Lookup(key);
    if (!payload) {
        *result = MakeNull().Release();
        return;
    }

    const int64_t value = BridgeGetInt64(payload.Get());
    payload.Reset();
    *result = MakeOptional(MakeInt64(value)).Release();
}

__attribute__((visibility("default"))) int64_t dict_lookup_cache_hits(void) {
    return CachedHits;
}

} // extern "C"
