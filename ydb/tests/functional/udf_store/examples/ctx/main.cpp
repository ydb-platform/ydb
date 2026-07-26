#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

__attribute__((import_module("ctx_lib"), import_name("ctx_lib_create")))
extern "C" unsigned long long ctx_lib_create(void);

__attribute__((import_module("ctx_lib"), import_name("ctx_lib_destroy")))
extern "C" void ctx_lib_destroy(unsigned long long handle);

__attribute__((import_module("ctx_lib"), import_name("ctx_lib_format")))
extern "C" int ctx_lib_format(unsigned long long handle, char* buf, int bufLen);

extern "C" {

__attribute__((visibility("default"))) void ctx_create(
    TExpressionContext* /*context*/,
    TUnversionedValue* result)
{
    const unsigned long long handle = ctx_lib_create();
    if (handle == 0) {
        ThrowException("ctx_create failed");
    }
    result->Type = EValueType::Uint64;
    result->Data.Uint64 = handle;
}

__attribute__((visibility("default"))) void ctx_destroy(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* handleArg)
{
    unsigned long long handle = 0;
    if (handleArg && handleArg->Type == EValueType::Uint64) {
        handle = handleArg->Data.Uint64;
    } else if (handleArg && handleArg->Type == EValueType::Int64) {
        handle = static_cast<unsigned long long>(handleArg->Data.Int64);
    }
    ctx_lib_destroy(handle);
    result->Type = EValueType::Null;
}

__attribute__((visibility("default"))) void ctx_snapshot(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* handleArg)
{
    unsigned long long handle = 0;
    if (handleArg && handleArg->Type == EValueType::Uint64) {
        handle = handleArg->Data.Uint64;
    } else if (handleArg && handleArg->Type == EValueType::Int64) {
        handle = static_cast<unsigned long long>(handleArg->Data.Int64);
    } else {
        ThrowException("ctx_snapshot: expected uint64 handle");
    }

    char tmp[64];
    const int n = ctx_lib_format(handle, tmp, (int)sizeof(tmp));
    if (n <= 0) {
        ThrowException("ctx_snapshot: format failed");
    }
    result->Type = EValueType::String;
    result->Length = static_cast<uint32_t>(n);
    result->Data.String = AllocateBytes(context, result->Length);
    for (int i = 0; i < n; ++i) {
        result->Data.String[i] = tmp[i];
    }
}

} // extern "C"
