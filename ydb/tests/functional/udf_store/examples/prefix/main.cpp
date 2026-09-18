#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>
#include <ydb/services/udf_store/wasm/abi/bridge.h>
#include <ydb/services/udf_store/wasm/object_framework/object_framework.h>

#include <stdlib.h>
#include <string.h>

using namespace NYdb::NUdfStore::NAbi;

namespace {

struct TPrefix {
    char* Data = nullptr;
    size_t Len = 0;
};

void PrefixInit(void* self, const void* blob, size_t blobLen) {
    auto* prefix = static_cast<TPrefix*>(self);
    if (blobLen == 0) {
        prefix->Data = nullptr;
        prefix->Len = 0;
        return;
    }
    prefix->Data = static_cast<char*>(malloc(blobLen));
    if (!prefix->Data) {
        ThrowException("PrefixInit: malloc failed");
    }
    memcpy(prefix->Data, blob, blobLen);
    prefix->Len = blobLen;
}

void PrefixDestroy(void* self) {
    auto* prefix = static_cast<TPrefix*>(self);
    free(prefix->Data);
    prefix->Data = nullptr;
    prefix->Len = 0;
}

const TObjectType PrefixType = {
    "Prefix",
    sizeof(TPrefix),
    &PrefixInit,
    &PrefixDestroy,
};

} // namespace

extern "C" {

__attribute__((visibility("default"))) void prefix_create(
    TExpressionContext* /*context*/,
    uint64_t* result,
    uint64_t config)
{
    const char* blob = reinterpret_cast<const char*>(BridgeEnsureString(config));
    const size_t blobLen = static_cast<size_t>(BridgeGetStringLen(config));

    const TObjectHandle handle = ObjectFrameworkCreate(&PrefixType, blob, blobLen);
    if (handle == 0) {
        ThrowException("prefix_create failed");
    }
    *result = MakeUint64(handle).Release();
}

__attribute__((visibility("default"))) void prefix_apply(
    TExpressionContext* context,
    uint64_t* result,
    uint64_t handleArg,
    uint64_t inputArg)
{
    const uint64_t handle = BridgeGetUint64(handleArg);
    auto* prefix = static_cast<TPrefix*>(ObjectFrameworkGet(handle, &PrefixType));
    if (!prefix) {
        ThrowException("prefix_apply: unknown handle");
    }

    if (BridgeIsNull(inputArg)) {
        *result = MakeNull().Release();
        return;
    }
    const size_t inputLen = static_cast<size_t>(BridgeGetStringLen(inputArg));
    const char* input = reinterpret_cast<const char*>(BridgeEnsureString(inputArg));
    const size_t total = prefix->Len + inputLen;
    char* output = AllocateBytes(context, total);
    if (prefix->Len) { memcpy(output, prefix->Data, prefix->Len); }
    if (inputLen) { memcpy(output + prefix->Len, input, inputLen); }
    *result = MakeString(output, total).Release();

}

__attribute__((visibility("default"))) void prefix_destroy(
    TExpressionContext* /*context*/,
    uint64_t* result,
    uint64_t handleArg)
{
    ObjectFrameworkDestroy(BridgeGetUint64(handleArg));
    *result = MakeNull().Release();
}

} // extern "C"
