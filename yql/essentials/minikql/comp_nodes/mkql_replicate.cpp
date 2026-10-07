#include "mkql_replicate.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <utility>

namespace NKikimr::NMiniKQL {

namespace {

class TReplicateWrapper: public TMutableComputationNode<TReplicateWrapper> {
    using TBaseComputation = TMutableComputationNode<TReplicateWrapper>;

public:
    class TValue: public TCustomListValue {
    public:
        template <EDictItems Mode>
        class TIterator: public TComputationValue<TIterator<Mode>> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue item, ui64 count)
                : TComputationValue<TIterator<Mode>>(memInfo)
                , Item_(std::move(item))
                , Current_(0)
                , End_(count)
            {
            }

        private:
            bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
                if (Current_ < End_) {
                    switch (Mode) {
                        case EDictItems::Payloads:
                            this->ThrowNotSupported(__func__);
                            break;
                        case EDictItems::Keys:
                            this->ThrowNotSupported(__func__);
                            break;
                        case EDictItems::Both:
                            key = NUdf::TUnboxedValuePod(ui64(Current_));
                            payload = Item_;
                            break;
                    }

                    ++Current_;
                    return true;
                }

                return false;
            }

            bool Next(NUdf::TUnboxedValue& value) override {
                if (Current_ < End_) {
                    switch (Mode) {
                        case EDictItems::Payloads:
                            value = Item_;
                            break;
                        case EDictItems::Keys:
                            value = NUdf::TUnboxedValuePod(ui64(Current_));
                            break;
                        case EDictItems::Both:
                            this->ThrowNotSupported(__func__);
                            break;
                    }

                    ++Current_;
                    return true;
                }

                return false;
            }

            bool Skip() override {
                if (Current_ < End_) {
                    ++Current_;
                    return true;
                }

                return false;
            }

            const NUdf::TUnboxedValue Item_;
            ui64 Current_;
            const ui64 End_;
        };

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, NUdf::TUnboxedValue item, ui64 count)
            : TCustomListValue(memInfo)
            , Ctx_(ctx)
            , Item_(std::move(item))
            , Count_(count)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx_.HolderFactory.Create<TIterator<EDictItems::Payloads>>(Item_, Count_);
        }

        bool HasFastListLength() const override {
            return true;
        }

        ui64 GetListLength() const override {
            return Count_;
        }

        ui64 GetEstimatedListLength() const override {
            return Count_;
        }

        bool HasListItems() const override {
            return Count_ > 0;
        }

        NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override {
            Y_UNUSED(builder);
            return const_cast<TValue*>(this);
        }

        NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
            Y_UNUSED(builder);
            if (count == 0) {
                return const_cast<TValue*>(this);
            }

            if (count >= Count_) {
                return Ctx_.HolderFactory.GetEmptyContainerLazy().AsBoxed();
            }

            return Ctx_.HolderFactory.Create<TValue>(Ctx_, Item_, Count_ - count).AsBoxed();
        }

        NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
            Y_UNUSED(builder);
            if (count == 0) {
                return Ctx_.HolderFactory.GetEmptyContainerLazy().AsBoxed();
            }

            if (count >= Count_) {
                return const_cast<TValue*>(this);
            }

            return Ctx_.HolderFactory.Create<TValue>(Ctx_, Item_, count).AsBoxed();
        }

        NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
            Y_UNUSED(builder);
            return const_cast<TValue*>(this);
        }

        ui64 GetDictLength() const override {
            return Count_;
        }

        bool HasDictItems() const override {
            return Count_ > 0;
        }

        bool Contains(const NUdf::TUnboxedValuePod& key) const override {
            return key.Get<ui64>() < Count_;
        }

        NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
            if (key.Get<ui64>() < Count_) {
                return Item_.MakeOptional();
            }

            return {};
        }

        NUdf::TUnboxedValue GetDictIterator() const override {
            return Ctx_.HolderFactory.Create<TIterator<EDictItems::Both>>(Item_, Count_);
        }

        NUdf::TUnboxedValue GetKeysIterator() const override {
            return Ctx_.HolderFactory.Create<TIterator<EDictItems::Keys>>(Item_, Count_);
        }

        NUdf::TUnboxedValue GetPayloadsIterator() const override {
            return GetListIterator();
        }

        bool IsSortedDict() const override {
            return true;
        }

        TComputationContext& Ctx_;
        const NUdf::TUnboxedValue Item_;
        const ui64 Count_;
    };

    TReplicateWrapper(TComputationMutables& mutables, IComputationNode* item, IComputationNode* count,
                      NUdf::TSourcePosition pos)
        : TBaseComputation(mutables)
        , Item_(item)
        , Count_(count)
        , Pos_(pos)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto count = Count_->GetValue(ctx).Get<ui64>();
        const ui64 MAX_VALUE = 1ULL << 32;
        if (count >= MAX_VALUE) {
            TStringBuilder res;
            res << Pos_ << " Second argument in ListReplicate = " << count << " exceeds maximum value = " << MAX_VALUE;
            UdfTerminate(res.data());
        }

        if (!count) {
            return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        return ctx.HolderFactory.Create<TValue>(ctx, Item_->GetValue(ctx), count);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Item_);
        DependsOn(Count_);
    }

    IComputationNode* const Item_;
    IComputationNode* const Count_;
    const NUdf::TSourcePosition Pos_;
};

} // namespace

IComputationNode* WrapReplicate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");

    const auto countType = AS_TYPE(TDataType, callable.GetInput(1));
    MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    const TStringBuf file = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef();
    const ui32 row = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>();
    const ui32 column = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<ui32>();
    const NUdf::TSourcePosition pos = NUdf::TSourcePosition(row, column, file);

    return new TReplicateWrapper(ctx.Mutables, list, count, pos);
}

} // namespace NKikimr::NMiniKQL
