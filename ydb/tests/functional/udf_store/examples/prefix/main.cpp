#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>
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

uint64_t AsHandle(const TUnversionedValue* value) {
    if (!value || value->Type == EValueType::Null) {
        return 0;
    }
    if (value->Type == EValueType::Uint64) {
        return value->Data.Uint64;
    }
    if (value->Type == EValueType::Int64) {
        return static_cast<uint64_t>(value->Data.Int64);
    }
    ThrowException("expected int64/uint64 handle");
    return 0;
}

} // namespace

extern "C" {

__attribute__((visibility("default"))) void prefix_create(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* config)
{
    const char* blob = nullptr;
    size_t blobLen = 0;
    if (config && config->Type == EValueType::String) {
        blob = config->Data.String;
        blobLen = config->Length;
    } else if (config && config->Type != EValueType::Null) {
        ThrowException("prefix_create: expected string config");
    }

    const TObjectHandle handle = ObjectFrameworkCreate(&PrefixType, blob, blobLen);
    if (handle == 0) {
        ThrowException("prefix_create failed");
    }
    result->Type = EValueType::Uint64;
    result->Data.Uint64 = handle;
}

__attribute__((visibility("default"))) void prefix_apply(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* handleArg,
    TUnversionedValue* inputArg)
{
    const uint64_t handle = AsHandle(handleArg);
    auto* prefix = static_cast<TPrefix*>(ObjectFrameworkGet(handle, &PrefixType));
    if (!prefix) {
        ThrowException("prefix_apply: unknown handle");
    }

    if (!inputArg || inputArg->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }
    if (inputArg->Type != EValueType::String) {
        ThrowException("prefix_apply: expected string input");
    }

    const size_t total = prefix->Len + inputArg->Length;
    result->Type = EValueType::String;
    result->Length = static_cast<uint32_t>(total);
    result->Data.String = AllocateBytes(context, total);
    if (prefix->Len > 0) {
        memcpy(result->Data.String, prefix->Data, prefix->Len);
    }
    if (inputArg->Length > 0) {
        memcpy(result->Data.String + prefix->Len, inputArg->Data.String, inputArg->Length);
    }
}

__attribute__((visibility("default"))) void prefix_destroy(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* handleArg)
{
    ObjectFrameworkDestroy(AsHandle(handleArg));
    result->Type = EValueType::Null;
}

} // extern "C"
