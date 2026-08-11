#include "wavm_private_imports.h"

#include <ydb/library/wasm/api/type_builder.h>

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/compact_containers/compact_vector.h>
#include <library/cpp/yt/error/error.h>

using NYT::TCompactVector;

namespace NYdb::NWasm {

using namespace WAVM;

////////////////////////////////////////////////////////////////////////////////

IR::ValueType ConvertToRuntimeType(EWebAssemblyValueType type)
{
    switch (type) {
        case EWebAssemblyValueType::UintPtr:
            return IR::ValueType::i64;
        case EWebAssemblyValueType::Int64:
            return IR::ValueType::i64;
        case EWebAssemblyValueType::Int32:
            return IR::ValueType::i32;
        case EWebAssemblyValueType::Float64:
            return IR::ValueType::f64;
        case EWebAssemblyValueType::Float32:
            return IR::ValueType::f32;
        case EWebAssemblyValueType::Void:
            THROW_ERROR_EXCEPTION("Type \"void\" is not a correct Wavm value type");
        default:
            YT_ABORT();
    }
}

IR::TypeTuple ConvertToRuntimeTypeTuple(EWebAssemblyValueType type)
{
    if (type == EWebAssemblyValueType::Void) {
        return IR::TypeTuple();
    }
    return IR::TypeTuple(ConvertToRuntimeType(type));
}

IR::TypeTuple ConvertToRuntimeTypeTuple(TRange<EWebAssemblyValueType> types)
{
    TCompactVector<IR::ValueType, 8> wavmTypes;
    for (auto type : types) {
        wavmTypes.push_back(ConvertToRuntimeType(type));
    }
    return IR::TypeTuple(wavmTypes.data(), wavmTypes.size());
}

TWebAssemblyRuntimeType GetTypeId(
    bool intrinsic,
    EWebAssemblyValueType returnType,
    TRange<EWebAssemblyValueType> argumentTypes)
{
    auto wavmReturnType = ConvertToRuntimeTypeTuple(returnType);
    auto wavmArgumentTypes = ConvertToRuntimeTypeTuple(argumentTypes);
    const auto wavmCallingConvention = intrinsic ? IR::CallingConvention::intrinsic : IR::CallingConvention::wasm;
    auto functionType = IR::FunctionType(wavmReturnType, wavmArgumentTypes, wavmCallingConvention);
    return TWebAssemblyRuntimeType{std::bit_cast<void*>(functionType.getEncoding().impl)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
