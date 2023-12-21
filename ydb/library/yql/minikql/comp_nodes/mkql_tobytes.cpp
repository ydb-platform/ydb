#include "mkql_tobytes.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/utils/swap_bytes.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsOptional, typename Type>
class TToBytesPrimitiveTypeWrapper : public TDecoratorCodegeneratorNode<TToBytesPrimitiveTypeWrapper<IsOptional, Type>> {
using TBaseComputation = TDecoratorCodegeneratorNode<TToBytesPrimitiveTypeWrapper<IsOptional, Type>>;
public:
    TToBytesPrimitiveTypeWrapper(IComputationNode* data)
        : TBaseComputation(data)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        if (IsOptional && !value)
            return NUdf::TUnboxedValuePod();

        const auto& v = value.Get<Type>();
        return NUdf::TUnboxedValuePod::Embedded(NUdf::TStringRef(reinterpret_cast<const char*>(&v), sizeof(v)));
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* value, BasicBlock*& block) const {
        const uint64_t one[] = {0ULL, sizeof(Type) << 48ULL};
        const auto size = ConstantInt::get(value->getType(), APInt(128, 2, one));
        const uint64_t two[] = {0xFFFFFFFFFFFFFFFFULL, 0xFF00FFFFFFFFFFFFULL};
        const auto mask = ConstantInt::get(value->getType(), APInt(128, 2, two));
        const auto result = BinaryOperator::CreateOr(BinaryOperator::CreateAnd(value, mask, "and", block), size, "or", block);
        if constexpr (IsOptional)
            return SelectInst::Create(IsExists(value, block), result, GetEmpty(ctx.Codegen.GetContext()), "select", block);
        return result;
    }
#endif
};

template<bool IsOptional, typename Type>
class TToBytesTzTypeWrapper : public TDecoratorComputationNode<TToBytesTzTypeWrapper<IsOptional, Type>> {
using TBaseComputation = TDecoratorComputationNode<TToBytesTzTypeWrapper<IsOptional, Type>>;
public:
    TToBytesTzTypeWrapper(IComputationNode* data)
        : TBaseComputation(data)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        if (IsOptional && !value)
            return NUdf::TUnboxedValuePod();

        const auto v = NYql::SwapBytes(value.Get<Type>());
        const auto tzId = NYql::SwapBytes(value.GetTimezoneId());
        char buf[sizeof(Type) + sizeof(ui16)];
        std::memcpy(buf, &v, sizeof(v));
        std::memcpy(buf + sizeof(Type), &tzId, sizeof(tzId));
        return NUdf::TUnboxedValuePod::Embedded(NUdf::TStringRef(buf, sizeof(buf)));
    }
};

class TToBytesWrapper : public TDecoratorCodegeneratorNode<TToBytesWrapper> {
using TBaseComputation = TDecoratorCodegeneratorNode<TToBytesWrapper>;
public:
    TToBytesWrapper(IComputationNode* optional)
        : TBaseComputation(optional)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const { return value; }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext&, Value* value, BasicBlock*&) const { return value; }
#endif
};

}

IComputationNode* WrapToBytes(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    const auto data = LocateNode(ctx.NodeLocator, callable, 0);
    switch(dataType->GetSchemeType()) {
#define MAKE_PRIMITIVE_TYPE_BYTES(type, layout) \
        case NUdf::TDataType<type>::Id: \
            if (isOptional) \
                return new TToBytesPrimitiveTypeWrapper<true, layout>(data); \
            else \
                return new TToBytesPrimitiveTypeWrapper<false, layout>(data);

        KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_BYTES)
#undef MAKE_PRIMITIVE_TYPE_BYTES
#define MAKE_TZ_TYPE_BYTES(type, layout) \
        case NUdf::TDataType<type>::Id: \
            if (isOptional) \
                return new TToBytesTzTypeWrapper<true, layout>(data); \
            else \
                return new TToBytesTzTypeWrapper<false, layout>(data);

        MAKE_TZ_TYPE_BYTES(NUdf::TTzDate, ui16);
        MAKE_TZ_TYPE_BYTES(NUdf::TTzDatetime, ui32);
        MAKE_TZ_TYPE_BYTES(NUdf::TTzTimestamp, ui64);
#undef MAKE_TZ_TYPE_BYTES
        default:
            break;
    }

    return new TToBytesWrapper(data);
}

}
}
