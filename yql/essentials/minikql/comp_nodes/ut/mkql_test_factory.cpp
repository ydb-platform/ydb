#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL {

namespace {

ui64 g_Yield = std::numeric_limits<ui64>::max();
auto g_TestStreamData = std::to_array<ui64>({0, 0, 1, 0, 0, 0, 1, 2, 3});
auto g_TestYieldStreamData = std::to_array<ui64>({0, 1, 2, g_Yield, 0, g_Yield, 1, 2, 0, 1, 2, 0, g_Yield, 1, 2});

class TTestStreamWrapper: public TMutableComputationNode<TTestStreamWrapper> {
    typedef TMutableComputationNode<TTestStreamWrapper> TBaseComputation;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, ui64 count)
            : TBase(memInfo)
            , Count_(count)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Index_ == Count_) {
                return NUdf::EFetchStatus::Finish;
            }

            result = NUdf::TUnboxedValuePod(g_TestStreamData[Index_++]);
            return NUdf::EFetchStatus::Ok;
        }

    private:
        ui64 Index_ = 0;
        const ui64 Count_;
    };

    TTestStreamWrapper(TComputationMutables& mutables, ui64 count)
        : TBaseComputation(mutables)
        , Count_(Min<ui64>(count, g_TestStreamData.size()))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(Count_);
    }

private:
    void RegisterDependencies() const final {
    }

private:
    const ui64 Count_;
};

class TTestYieldStreamWrapper: public TMutableComputationNode<TTestYieldStreamWrapper> {
    typedef TMutableComputationNode<TTestYieldStreamWrapper> TBaseComputation;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx)
            : TBase(memInfo)
            , CompCtx_(compCtx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Index_ == g_TestYieldStreamData.size()) {
                return NUdf::EFetchStatus::Finish;
            }

            const auto value = g_TestYieldStreamData[Index_];
            if (value == g_Yield) {
                ++Index_;
                return NUdf::EFetchStatus::Yield;
            }

            NUdf::TUnboxedValue* items = nullptr;
            result = CompCtx_.HolderFactory.CreateDirectArrayHolder(2, items);
            items[0] = NUdf::TUnboxedValuePod(value);
            items[1] = MakeString(ToString(Index_));

            ++Index_;
            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& CompCtx_;
        ui64 Index_ = 0;
    };

    explicit TTestYieldStreamWrapper(TComputationMutables& mutables)
        : TBaseComputation(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx);
    }

private:
    void RegisterDependencies() const final {
    }
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

} // namespace

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

} // namespace NKikimr::NMiniKQL
