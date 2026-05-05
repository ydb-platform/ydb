#include "mkql_enumerate.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TEnumerateWrapper: public TMutableCodegeneratorNode<TEnumerateWrapper> {
    typedef TMutableCodegeneratorNode<TEnumerateWrapper> TBaseComputation;

public:
    using TSelf = TEnumerateWrapper;

    class TValue: public TCustomListValue {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(
                TMemoryUsageInfo* memInfo,
                NUdf::TUnboxedValue&& inner,
                ui64 start, ui64 step,
                TComputationContext& ctx, const TSelf* self)
                : TComputationValue(memInfo)
                , Inner(std::move(inner))
                , Step(step)
                , Counter(start - step)
                , Ctx(ctx)
                , Self(self)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                NUdf::TUnboxedValue item;
                if (Inner.Next(item)) {
                    Counter += Step;
                    NUdf::TUnboxedValue* items = nullptr;
                    value = Self->ResPair.NewArray(Ctx, 2, items);
                    items[0] = NUdf::TUnboxedValuePod(Counter);
                    items[1] = std::move(item);
                    return true;
                }

                return false;
            }

            bool Skip() override {
                if (Inner.Skip()) {
                    Counter += Step;
                    return true;
                }

                return false;
            }

            const NUdf::TUnboxedValue Inner;
            const ui64 Step;
            ui64 Counter;
            TComputationContext& Ctx;
            const TSelf* const Self;
        };

        TValue(
            TMemoryUsageInfo* memInfo,
            const NUdf::TUnboxedValue& list,
            ui64 start, ui64 step,
            TComputationContext& ctx,
            const TSelf* self)
            : TCustomListValue(memInfo)
            , List(list)
            , Start(start)
            , Step(step)
            , Ctx(ctx)
            , Self(self)
        {
        }

    private:
        ui64 GetListLength() const override {
            if (!Length_) {
                Length_ = List.GetListLength();
            }

            return *Length_;
        }

        bool HasListItems() const override {
            if (!HasItems_) {
                HasItems_ = List.HasListItems();
            }

            return *HasItems_;
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx.HolderFactory.Create<TIterator>(List.GetListIterator(), Start, Step, Ctx, Self);
        }

        const NUdf::TUnboxedValue List;
        const ui64 Start;
        const ui64 Step;
        TComputationContext& Ctx;
        const TSelf* const Self;
    };

    TEnumerateWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* start, IComputationNode* step)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , List(list)
        , Start(start)
        , Step(step)
        , ResPair(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return WrapList(ctx, List->GetValue(ctx).Release(), Start->GetValue(ctx).Get<ui64>(), Step->GetValue(ctx).Get<ui64>());
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto list = GetNodeValue(List, ctx, block);
        const auto startv = GetNodeValue(Start, ctx, block);
        const auto stepv = GetNodeValue(Step, ctx, block);

        const auto start = GetterFor<ui64>(startv, context, block);
        const auto step = GetterFor<ui64>(stepv, context, block);

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);

        return EmitFunctionCall<&TEnumerateWrapper::WrapList>(list->getType(), {self, ctx.Ctx, list, start, step}, ctx, block);
    }
#endif
private:
    NUdf::TUnboxedValuePod WrapList(TComputationContext& ctx, NUdf::TUnboxedValuePod list, ui64 start, ui64 step) const {
        return ctx.HolderFactory.Create<TValue>(list, start, step, ctx, this);
    }

    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(Start);
        DependsOn(Step);
    }

    IComputationNode* const List;
    IComputationNode* const Start;
    IComputationNode* const Step;

    const TContainerCacheOnContext ResPair;
};

} // namespace

IComputationNode* WrapEnumerate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");
    AS_TYPE(TListType, callable.GetInput(0));
    MKQL_ENSURE(AS_TYPE(TDataType, callable.GetInput(1))->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected Uint64");
    MKQL_ENSURE(AS_TYPE(TDataType, callable.GetInput(2))->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected Uint64");

    return new TEnumerateWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0),
                                 LocateNode(ctx.NodeLocator, callable, 1), LocateNode(ctx.NodeLocator, callable, 2));
}

} // namespace NMiniKQL
} // namespace NKikimr
