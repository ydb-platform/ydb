#include "mkql_replicate.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TReplicateWrapper : public TMutableComputationNode<TReplicateWrapper> {
    typedef TMutableComputationNode<TReplicateWrapper> TBaseComputation;
public:
    class TValue : public TCustomListValue {
    public:
        template <EDictItems Mode>
        class TIterator : public TComputationValue<TIterator<Mode>> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, const NUdf::TUnboxedValue& item, ui64 count)
                : TComputationValue<TIterator<Mode>>(memInfo)
                , Item(item)
                , Current(0)
                , End(count)
            {}

        private:
            bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
                if (Current < End) {
                    switch (Mode) {
                    case EDictItems::Payloads:
                        this->ThrowNotSupported(__func__);
                        break;
                    case EDictItems::Keys:
                        this->ThrowNotSupported(__func__);
                        break;
                    case EDictItems::Both:
                        key = NUdf::TUnboxedValuePod(ui64(Current));
                        payload = Item;
                        break;
                    }

                    ++Current;
                    return true;
                }

                return false;
            }

            bool Next(NUdf::TUnboxedValue& value) override {
                if (Current < End) {
                    switch (Mode) {
                    case EDictItems::Payloads:
                        value = Item;
                        break;
                    case EDictItems::Keys:
                        value = NUdf::TUnboxedValuePod(ui64(Current));
                        break;
                    case EDictItems::Both:
                        this->ThrowNotSupported(__func__);
                        break;
                    }

                    ++Current;
                    return true;
                }

                return false;
            }

            bool Skip() override {
                if (Current < End) {
                    ++Current;
                    return true;
                }

                return false;
            }

            const NUdf::TUnboxedValue Item;
            ui64 Current;
            const ui64 End;
        };

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const NUdf::TUnboxedValue& item, ui64 count)
            : TCustomListValue(memInfo)
            , Ctx(ctx)
            , Item(item)
            , Count(count)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const override {
            return Ctx.HolderFactory.Create<TIterator<EDictItems::Payloads>>(Item, Count);
        }

        bool HasFastListLength() const override {
            return true;
        }

        ui64 GetListLength() const override {
            return Count;
        }

        ui64 GetEstimatedListLength() const override {
            return Count;
        }

        bool HasListItems() const override {
            return Count > 0;
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

            if (count >= Count) {
                return Ctx.HolderFactory.GetEmptyContainerLazy().AsBoxed();
            }

            return Ctx.HolderFactory.Create<TValue>(Ctx, Item, Count - count).AsBoxed();
        }

        NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
            Y_UNUSED(builder);
            if (count == 0) {
                return Ctx.HolderFactory.GetEmptyContainerLazy().AsBoxed();
            }

            if (count >= Count) {
                return const_cast<TValue*>(this);
            }

            return Ctx.HolderFactory.Create<TValue>(Ctx, Item, count).AsBoxed();
        }

        NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
            Y_UNUSED(builder);
            return const_cast<TValue*>(this);
        }

        ui64 GetDictLength() const override {
            return Count;
        }

        bool HasDictItems() const override {
            return Count > 0;
        }

        bool Contains(const NUdf::TUnboxedValuePod& key) const override {
            return key.Get<ui64>() < Count;
        }

        NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
            if (key.Get<ui64>() < Count) {
                return Item.MakeOptional();
            }

            return {};
        }

        NUdf::TUnboxedValue GetDictIterator() const override {
            return Ctx.HolderFactory.Create<TIterator<EDictItems::Both>>(Item, Count);
        }

        NUdf::TUnboxedValue GetKeysIterator() const override {
            return Ctx.HolderFactory.Create<TIterator<EDictItems::Keys>>(Item, Count);
        }

        NUdf::TUnboxedValue GetPayloadsIterator() const override {
            return GetListIterator();
        }

        bool IsSortedDict() const override {
            return true;
        }

        TComputationContext& Ctx;
        const NUdf::TUnboxedValue Item;
        const ui64 Count;
    };

    TReplicateWrapper(TComputationMutables& mutables, IComputationNode* item, IComputationNode* count,
        NUdf::TSourcePosition pos)
        : TBaseComputation(mutables)
        , Item(item)
        , Count(count)
        , Pos(pos)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto count = Count->GetValue(ctx).Get<ui64>();
        const ui64 MAX_VALUE = 1ull << 32;
        if (count >= MAX_VALUE) {
            TStringBuilder res;
            res << Pos << " Second argument in ListReplicate = " << count << " exceeds maximum value = " << MAX_VALUE;
            UdfTerminate(res.data());
        }

        if (!count) {
            return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        return ctx.HolderFactory.Create<TValue>(ctx, Item->GetValue(ctx), count);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Item);
        DependsOn(Count);
    }

    IComputationNode* const Item;
    IComputationNode* const Count;
    const NUdf::TSourcePosition Pos;
};

}

IComputationNode* WrapReplicate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2 || callable.GetInputsCount() == 5, "Expected 2 or 5 args");

    const auto countType = AS_TYPE(TDataType, callable.GetInput(1));
    MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    NUdf::TSourcePosition pos;
    if (callable.GetInputsCount() == 5) {
        const TStringBuf file = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef();
        const ui32 row = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>();
        const ui32 column = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<ui32>();
        pos = NUdf::TSourcePosition(row, column, file);
    }

    return new TReplicateWrapper(ctx.Mutables, list, count, pos);
}

}
}
