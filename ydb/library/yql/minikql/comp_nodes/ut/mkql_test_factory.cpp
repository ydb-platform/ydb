#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>


namespace NKikimr {
namespace NMiniKQL {

namespace {


ui64 g_Yield = std::numeric_limits<ui64>::max();
ui64 g_TestStreamData[] = {0, 0, 1, 0, 0, 0, 1, 2, 3};
ui64 g_TestYieldStreamData[] = {0, 1, 2, g_Yield, 0, g_Yield, 1, 2, 0, 1, 2, 0, g_Yield, 1, 2};

class TTestStreamWrapper: public TMutableComputationNode<TTestStreamWrapper> {
    typedef TMutableComputationNode<TTestStreamWrapper> TBaseComputation;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, ui64 count)
            : TBase(memInfo)
            , Count(count)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Index == Count) {
                return NUdf::EFetchStatus::Finish;
            }

            result = NUdf::TUnboxedValuePod(g_TestStreamData[Index++]);
            return NUdf::EFetchStatus::Ok;
        }

    private:
        ui64 Index = 0;
        const ui64 Count;
    };

    TTestStreamWrapper(TComputationMutables& mutables, ui64 count)
        : TBaseComputation(mutables)
        , Count(Min<ui64>(count, Y_ARRAY_SIZE(g_TestStreamData)))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(Count);
    }

private:
    void RegisterDependencies() const final {}

private:
    const ui64 Count;
};

class TTestYieldStreamWrapper: public TMutableComputationNode<TTestYieldStreamWrapper> {
    typedef TMutableComputationNode<TTestYieldStreamWrapper> TBaseComputation;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx)
            : TBase(memInfo)
            , CompCtx(compCtx) {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Index == Y_ARRAY_SIZE(g_TestYieldStreamData)) {
                return NUdf::EFetchStatus::Finish;
            }

            const auto value = g_TestYieldStreamData[Index];
            if (value == g_Yield) {
                ++Index;
                return NUdf::EFetchStatus::Yield;
            }

            NUdf::TUnboxedValue* items = nullptr;
            result = CompCtx.HolderFactory.CreateDirectArrayHolder(2, items);
            items[0] = NUdf::TUnboxedValuePod(value);
            items[1] = MakeString(ToString(Index));

            ++Index;
            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& CompCtx;
        ui64 Index = 0;
    };

    TTestYieldStreamWrapper(TComputationMutables& mutables)
        : TBaseComputation(mutables) {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx);
    }

private:
    void RegisterDependencies() const final {}
};

IComputationNode* WrapTestStream(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args");
    const ui64 count = AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().Get<ui64>();
    return new TTestStreamWrapper(ctx.Mutables, count);
}


IComputationNode* WrapTestYieldStream(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(!callable.GetInputsCount(), "Expected no args");
    return new TTestYieldStreamWrapper(ctx.Mutables);
}

}

TComputationNodeFactory GetTestFactory(TComputationNodeFactory customFactory) {
    return [customFactory](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestList") {
            return new TExternalComputationNode(ctx.Mutables);
        }

        if (callable.GetType()->GetName() == "TestStream") {
            return WrapTestStream(callable, ctx);
        }

        if (callable.GetType()->GetName() == "TestYieldStream") {
            return WrapTestYieldStream(callable, ctx);
        }

        if (customFactory) {
            auto ret = customFactory(callable, ctx);
            if (ret) {
                return ret;
            }
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}

}
}
