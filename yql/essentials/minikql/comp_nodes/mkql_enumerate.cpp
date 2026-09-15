#include "mkql_enumerate.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <utility>

namespace NKikimr::NMiniKQL {

namespace {

class TEnumerateWrapper: public TMutableCodegeneratorNode<TEnumerateWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TEnumerateWrapper>;

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
                , Inner_(std::move(inner))
                , Step_(step)
                , Counter_(start - step)
                , Ctx_(ctx)
                , Self_(self)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                NUdf::TUnboxedValue item;
                if (Inner_.Next(item)) {
                    Counter_ += Step_;
                    NUdf::TUnboxedValue* items = nullptr;
                    value = Self_->ResPair_.NewArray(Ctx_, 2, items);
                    items[0] = NUdf::TUnboxedValuePod(Counter_);
                    items[1] = std::move(item);
                    return true;
                }

                return false;
            }

            bool Skip() override {
                if (Inner_.Skip()) {
                    Counter_ += Step_;
                    return true;
                }

                return false;
            }

            const NUdf::TUnboxedValue Inner_;
            const ui64 Step_;
            ui64 Counter_;
            TComputationContext& Ctx_;
            const TSelf* const Self_;
        };

        TValue(
            TMemoryUsageInfo* memInfo,
            NUdf::TUnboxedValue list,
            ui64 start, ui64 step,
            TComputationContext& ctx,
            const TSelf* self)
            : TCustomListValue(memInfo)
            , List_(std::move(list))
            , Start_(start)
            , Step_(step)
            , Ctx_(ctx)
            , Self_(self)
        {
        }

    private:
        ui64 GetListLength() const override {
            if (!Length_) {
                Length_ = List_.GetListLength();
            }

            return *Length_;
        }

        bool HasListItems() const override {
            if (!HasItems_) {
                HasItems_ = List_.HasListItems();
            }

            return *HasItems_;
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx_.HolderFactory.Create<TIterator>(List_.GetListIterator(), Start_, Step_, Ctx_, Self_);
        }

        const NUdf::TUnboxedValue List_;
        const ui64 Start_;
        const ui64 Step_;
        TComputationContext& Ctx_;
        const TSelf* const Self_;
    };

    TEnumerateWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* start, IComputationNode* step)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , List_(list)
        , Start_(start)
        , Step_(step)
        , ResPair_(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return WrapList(ctx, List_->GetValue(ctx).Release(), Start_->GetValue(ctx).Get<ui64>(), Step_->GetValue(ctx).Get<ui64>());
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto list = GetNodeValue(List_, ctx, block);
        const auto startv = GetNodeValue(Start_, ctx, block);
        const auto stepv = GetNodeValue(Step_, ctx, block);

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
        DependsOn(List_);
        DependsOn(Start_);
        DependsOn(Step_);
    }

    IComputationNode* const List_;
    IComputationNode* const Start_;
    IComputationNode* const Step_;

    const TContainerCacheOnContext ResPair_;
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

} // namespace NKikimr::NMiniKQL
