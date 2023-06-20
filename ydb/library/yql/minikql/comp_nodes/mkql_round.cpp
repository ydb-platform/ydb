#include "mkql_round.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/presort.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/utils/utf8.h>

#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql::NUdf;

namespace {

template<typename From, typename To>
class TRoundIntegralWrapper : public TMutableComputationNode<TRoundIntegralWrapper<From, To>> {
    using TSelf = TRoundIntegralWrapper<From, To>;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TRoundIntegralWrapper(TComputationMutables& mutables, IComputationNode* source, bool down)
        : TBaseComputation(mutables)
        , Source(source)
        , Down(down)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto value = Source->GetValue(ctx).Get<From>();
        constexpr auto toMin = std::numeric_limits<To>::min();
        constexpr auto toMax = std::numeric_limits<To>::max();

        if constexpr (std::is_signed<From>::value && std::is_unsigned<To>::value) {
            if (value < 0) {
                return Down ? TUnboxedValuePod() : TUnboxedValuePod(toMin);
            }

            if (static_cast<std::make_unsigned_t<From>>(value) > toMax) {
                return Down ? TUnboxedValuePod(toMax) : TUnboxedValuePod();
            }

            return TUnboxedValuePod(static_cast<To>(value));
        }

        if constexpr (std::is_unsigned<From>::value && std::is_signed<To>::value) {
            if (value > static_cast<std::make_unsigned_t<To>>(toMax)) {
                return Down ? TUnboxedValuePod(toMax) : TUnboxedValuePod();
            }

            return TUnboxedValuePod(static_cast<To>(value));
        }

        if (value < toMin) {
            return Down ? TUnboxedValuePod() : TUnboxedValuePod(toMin);
        }

        if (value > toMax) {
            return Down ? TUnboxedValuePod(toMax) : TUnboxedValuePod();
        }

        return TUnboxedValuePod(static_cast<To>(value));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source);
    }

    IComputationNode* const Source;
    const bool Down;
};

class TRoundDateTypeWrapper : public TMutableComputationNode<TRoundDateTypeWrapper> {
    using TSelf = TRoundDateTypeWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TRoundDateTypeWrapper(TComputationMutables& mutables, IComputationNode* source, bool down, EDataSlot from, EDataSlot to)
        : TBaseComputation(mutables)
        , Source(source)
        , Down(down)
        , From(from)
        , To(to)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        constexpr ui64 usInDay = 86400000000ull;
        constexpr ui32 usInSec = 1000000u;

        ui64 us;
        if (From == EDataSlot::Timestamp) {
            us = Source->GetValue(ctx).Get<NUdf::TDataType<TTimestamp>::TLayout>();
        } else {
            Y_ENSURE(From == EDataSlot::Datetime);
            us = Source->GetValue(ctx).Get<NUdf::TDataType<TDatetime>::TLayout>();
            us *= usInSec;
        }

        TUnboxedValuePod result;
        if (To == EDataSlot::Date) {
            NUdf::TDataType<TTimestamp>::TLayout rounded = (us + (Down ? 0 : (usInDay - 1u))) / usInDay;
            if (rounded >= MAX_DATE) {
                return {};
            }
            result = TUnboxedValuePod(rounded);
        } else {
            Y_ENSURE(To == EDataSlot::Datetime);
            NUdf::TDataType<TDatetime>::TLayout rounded = (us + (Down ? 0 : (usInSec - 1u))) / usInSec;
            if (rounded >= MAX_DATETIME) {
                return {};
            }
            result = TUnboxedValuePod(rounded);
        }

        return result;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source);
    }

    IComputationNode* const Source;
    const bool Down;
    const EDataSlot From;
    const EDataSlot To;
};

class TRoundStringWrapper : public TMutableComputationNode<TRoundStringWrapper> {
    using TSelf = TRoundStringWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TRoundStringWrapper(TComputationMutables& mutables, IComputationNode* source, bool down)
        : TBaseComputation(mutables)
        , Source(source)
        , Down(down)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValue input = Source->GetValue(ctx);
        auto output = NYql::RoundToNearestValidUtf8(input.AsStringRef(), Down);
        if (!output) {
            return {};
        }
        return MakeString(*output);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source);
    }

    IComputationNode* const Source;
    const bool Down;
};

template<typename From>
IComputationNode* FromIntegral(TComputationMutables& mutables, IComputationNode* source, bool down, EDataSlot target) {
    switch (target) {
        case EDataSlot::Int8:    return new TRoundIntegralWrapper<From, i8>(mutables, source, down);
        case EDataSlot::Uint8:   return new TRoundIntegralWrapper<From, ui8>(mutables, source, down);
        case EDataSlot::Int16:   return new TRoundIntegralWrapper<From, i16>(mutables, source, down);
        case EDataSlot::Uint16:  return new TRoundIntegralWrapper<From, ui16>(mutables, source, down);
        case EDataSlot::Int32:   return new TRoundIntegralWrapper<From, i32>(mutables, source, down);
        case EDataSlot::Uint32:  return new TRoundIntegralWrapper<From, ui32>(mutables, source, down);
        case EDataSlot::Int64:   return new TRoundIntegralWrapper<From, i64>(mutables, source, down);
        case EDataSlot::Uint64:  return new TRoundIntegralWrapper<From, ui64>(mutables, source, down);
        default: Y_ENSURE(false, "Unsupported integral rounding");
    }
    return nullptr;
}


} // namespace

IComputationNode* WrapRound(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expecting exactly one argument");

    auto type = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(type->IsData(), "Expecting data as argument");

    auto returnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(returnType->IsOptional(), "Expecting optional as return type");

    auto targetType = static_cast<TOptionalType*>(returnType)->GetItemType();
    MKQL_ENSURE(targetType->IsData(), "Expecting Data as target type");

    auto from = GetDataSlot(static_cast<TDataType*>(type)->GetSchemeType());
    auto to = GetDataSlot(static_cast<TDataType*>(targetType)->GetSchemeType());

    bool down = callable.GetType()->GetName() == "RoundDown";
    auto source = LocateNode(ctx.NodeLocator, callable, 0);

    switch (from) {
        case EDataSlot::Int8:   return FromIntegral<i8>(ctx.Mutables, source, down, to);
        case EDataSlot::Uint8:  return FromIntegral<ui8>(ctx.Mutables, source, down, to);
        case EDataSlot::Int16:  return FromIntegral<i16>(ctx.Mutables, source, down, to);
        case EDataSlot::Uint16: return FromIntegral<ui16>(ctx.Mutables, source, down, to);
        case EDataSlot::Int32:  return FromIntegral<i32>(ctx.Mutables, source, down, to);
        case EDataSlot::Uint32: return FromIntegral<ui32>(ctx.Mutables, source, down, to);
        case EDataSlot::Int64:  return FromIntegral<i64>(ctx.Mutables, source, down, to);
        case EDataSlot::Uint64: return FromIntegral<ui64>(ctx.Mutables, source, down, to);
        case EDataSlot::Datetime:
        case EDataSlot::Timestamp:
            Y_ENSURE(GetDataTypeInfo(to).Features & DateType);
            return new TRoundDateTypeWrapper(ctx.Mutables, source, down, from, to);
        case EDataSlot::String:
            Y_ENSURE(to == EDataSlot::Utf8);
            return new TRoundStringWrapper(ctx.Mutables, source, down);
        default:
            Y_ENSURE(false,
                "Unsupported rounding from " << GetDataTypeInfo(from).Name << " to " << GetDataTypeInfo(to).Name);
    }
    return nullptr;
}

}
}
