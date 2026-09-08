#include <ydb/services/udf_store/wasm/abi/bridge.h>
#include <ydb/services/udf_store/wasm/abi/bridge_abi.h>
#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/generic/string.h>

using namespace NYdb::NUdfStore::NAbi;

extern "C" {

//! Md5::md5(data: String) -> String
//! Bridge CC: BridgeEnsureString pins the input once per distinct value.
__attribute__((visibility("default"))) void md5(
    TExpressionContext* /*ctx*/,
    uint64_t* result,
    uint64_t arg0)
{
    if (BridgeIsNull(arg0)) {
        *result = MakeNull().Release();
        return;
    }

    const uint64_t offset = BridgeEnsureString(arg0);
    const int64_t length = BridgeGetStringLen(arg0);
    const TStringBuf input(
        reinterpret_cast<const char*>(static_cast<uintptr_t>(offset)),
        static_cast<size_t>(length));
    const TString hash = MD5::Calc(input);
    *result = MakeString(hash.data(), static_cast<int64_t>(hash.size())).Release();
}

} // extern "C"
