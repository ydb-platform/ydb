#include "kqp_stream_enumerate.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

// Builds the (rank, item) pair value. Rank is 1-based.
inline NUdf::TUnboxedValue MakePair(TComputationContext& ctx, ui64 rank, NUdf::TUnboxedValue&& item) {
    NUdf::TUnboxedValue* items = nullptr;
    auto result = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
    items[0] = NUdf::TUnboxedValuePod(rank);
    items[1] = std::move(item);
    return result;
}

// ---- Flow ----
class TFlowWrapper: public TStatefulFlowComputationNode<TFlowWrapper> {
    using TBaseComputation = TStatefulFlowComputationNode<TFlowWrapper>;
public:
    TFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded)
        , Flow(flow)
    {
    }

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = NUdf::TUnboxedValuePod::Zero();
        }
        auto item = Flow->GetValue(ctx);
        if (item.IsSpecial()) {
            return item;
        }
        const ui64 rank = state.Get<ui64>() + 1;
        state = NUdf::TUnboxedValuePod(rank);
        return MakePair(ctx, rank, std::move(item));
    }

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow);
    }

    IComputationNode* const Flow;
};

// ---- Stream ----
class TStreamWrapper: public TMutableComputationNode<TStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TStreamWrapper>;
public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;
        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue&& input)
            : TBase(memInfo)
            , Ctx(ctx)
            , Input(std::move(input))
            , Counter(0)
        {
        }
    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            NUdf::TUnboxedValue item;
            const auto status = Input.Fetch(item);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }
            result = MakePair(Ctx, ++Counter, std::move(item));
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& Ctx;
        const NUdf::TUnboxedValue Input;
        ui64 Counter;
    };

    TStreamWrapper(TComputationMutables& mutables, IComputationNode* input)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Input(input)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Input->GetValue(ctx));
    }

    void RegisterDependencies() const final {
        DependsOn(Input);
    }

private:
    IComputationNode* const Input;
};

// ---- List ----
class TListWrapper: public TMutableComputationNode<TListWrapper> {
    using TBaseComputation = TMutableComputationNode<TListWrapper>;
public:
    class TListValue: public TCustomListValue {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue&& iter)
                : TComputationValue<TIterator>(memInfo)
                , Ctx(ctx)
                , Iter(std::move(iter))
                , Counter(0)
            {
            }
        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                NUdf::TUnboxedValue item;
                if (!Iter.Next(item)) {
                    return false;
                }
                value = MakePair(Ctx, ++Counter, std::move(item));
                return true;
            }

            TComputationContext& Ctx;
            const NUdf::TUnboxedValue Iter;
            ui64 Counter;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue&& list)
            : TCustomListValue(memInfo)
            , Ctx(ctx)
            , List(std::move(list))
        {
        }
    private:
        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx.HolderFactory.Create<TIterator>(Ctx, List.GetListIterator());
        }

        TComputationContext& Ctx;
        const NUdf::TUnboxedValue List;
    };

    TListWrapper(TComputationMutables& mutables, IComputationNode* input)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Input(input)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TListValue>(ctx, Input->GetValue(ctx));
    }

    void RegisterDependencies() const final {
        DependsOn(Input);
    }

private:
    IComputationNode* const Input;
};

} // namespace

IComputationNode* WrapKqpStreamEnumerate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "KqpStreamEnumerate expects exactly 1 argument");
    const auto type = callable.GetInput(0).GetStaticType();
    const auto input = LocateNode(ctx.NodeLocator, callable, 0);
    if (type->IsFlow()) {
        return new TFlowWrapper(ctx.Mutables, GetValueRepresentation(type), input);
    } else if (type->IsStream()) {
        return new TStreamWrapper(ctx.Mutables, input);
    } else if (type->IsList()) {
        return new TListWrapper(ctx.Mutables, input);
    }
    THROW yexception() << "KqpStreamEnumerate: expected flow, stream or list input.";
}

} // namespace NMiniKQL
} // namespace NKikimr
