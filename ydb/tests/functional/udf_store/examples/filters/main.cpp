#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

__attribute__((import_module("ctx_lib"), import_name("ctx_lib_inc_a")))
extern "C" void ctx_lib_inc_a(unsigned long long handle);

__attribute__((import_module("ctx_lib"), import_name("ctx_lib_inc_b")))
extern "C" void ctx_lib_inc_b(unsigned long long handle);

namespace {

unsigned long long AsHandle(const TUnversionedValue* value) {
    if (!value || value->Type == EValueType::Null) {
        ThrowException("expected ctx handle");
    }
    if (value->Type == EValueType::Uint64) {
        return value->Data.Uint64;
    }
    if (value->Type == EValueType::Int64) {
        return static_cast<unsigned long long>(value->Data.Int64);
    }
    ThrowException("expected uint64 ctx handle");
    return 0;
}

} // namespace

extern "C" {

__attribute__((visibility("default"))) void filter_a(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* handleArg,
    TUnversionedValue* inputArg)
{
    ctx_lib_inc_a(AsHandle(handleArg));
    if (!inputArg || inputArg->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }
    *result = *inputArg;
}

__attribute__((visibility("default"))) void filter_b(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* handleArg,
    TUnversionedValue* inputArg)
{
    ctx_lib_inc_b(AsHandle(handleArg));
    if (!inputArg || inputArg->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }
    *result = *inputArg;
}

} // extern "C"
