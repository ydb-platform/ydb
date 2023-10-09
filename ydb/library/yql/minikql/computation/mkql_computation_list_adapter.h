#pragma once
#include <ydb/library/yql/minikql/defs.h>
#include "mkql_computation_node_impl.h"
#include <ydb/library/yql/minikql/mkql_alloc.h>

namespace NKikimr {
namespace NMiniKQL {

template <typename TVectorType>
class TVectorListAdapter : public TComputationValue<TVectorListAdapter<TVectorType>> {
public:
    typedef typename TVectorType::value_type TItem;
    typedef TVectorListAdapter<TVectorType> TSelf;
    typedef std::function<NUdf::TUnboxedValue(const TItem&)> TItemFactory;
    typedef TComputationValue<TVectorListAdapter<TVectorType>> TBase;

    class TIterator: public TTemporaryComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, const TVectorType& list, TItemFactory itemFactory, ui64 start, ui64 finish, bool reversed)
            : TTemporaryComputationValue<TIterator>(memInfo)
            , List(list)
            , ItemFactory(itemFactory)
            , Start(start)
            , Finish(finish)
            , Reversed(reversed)
        {
            Index = Reversed ? Finish : (Start - 1);
        }

    private:
        bool Next(NUdf::TUnboxedValue& value) override {
            if (!Skip())
                return false;
            value = ItemFactory(List[Index]);
            return true;
        }

        bool Skip() override {
            if (!Reversed) {
                if (Index + 1 >= Finish)
                    return false;
                ++Index;
            } else {
                if (Index < Start + 1)
                    return false;
                --Index;
            }

            return true;
        }

        const TVectorType& List;
        const TItemFactory ItemFactory;
        const ui64 Start;
        const ui64 Finish;
        const bool Reversed;
        ui64 Index;
    };

    class TDictIterator : public TTemporaryComputationValue<TDictIterator> {
    public:
        TDictIterator(TMemoryUsageInfo* memInfo, THolder<TIterator>&& iter)
            : TTemporaryComputationValue<TDictIterator>(memInfo)
            , Iter(std::move(iter))
            , Index(Max<ui64>())
        {}

    private:
        bool Next(NUdf::TUnboxedValue& key) override {
            if (NUdf::TBoxedValueAccessor::Skip(*Iter)) {
                key = NUdf::TUnboxedValuePod(++Index);
                return true;
            }

            return false;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (NUdf::TBoxedValueAccessor::Next(*Iter, payload)) {
                key = NUdf::TUnboxedValuePod(++Index);
                return true;
            }

            return false;
        }

        bool Skip() override {
            if (NUdf::TBoxedValueAccessor::Skip(*Iter)) {
                ++Index;
                return true;
            }

            return false;
        }

        THolder<TIterator> Iter;
        ui64 Index;
    };

    TVectorListAdapter(
            TMemoryUsageInfo* memInfo,
            const TVectorType& list,
            TItemFactory itemFactory,
            ui64 start, ui64 finish,
            bool reversed)
        : TBase(memInfo)
        , List(list)
        , ItemFactory(itemFactory)
        , Start(start)
        , Finish(finish)
        , Reversed(reversed)
    {
        Y_ABORT_UNLESS(Start <= Finish && Finish <= List.size());
    }

private:
    bool HasFastListLength() const override {
        return true;
    }

    ui64 GetListLength() const override {
        return Finish - Start;
    }

    ui64 GetEstimatedListLength() const override {
        return Finish - Start;
    }

    bool HasListItems() const override {
        return Finish != Start;
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return new TSelf(this->GetMemInfo(), List, ItemFactory, Start, Finish, !Reversed);
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        const ui64 newStart = Min(Start + count, Finish);
        if (newStart == Start) {
            return const_cast<TVectorListAdapter*>(this);
        }

        return new TSelf(this->GetMemInfo(), List, ItemFactory, newStart, Finish, Reversed);
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        const ui64 newFinish = Min(Start + count, Finish);
        if (newFinish == Finish) {
            return const_cast<TVectorListAdapter*>(this);
        }

        return new TSelf(this->GetMemInfo(), List, ItemFactory, Start, newFinish, Reversed);
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return const_cast<TVectorListAdapter*>(this);
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(this->GetMemInfo(), List, ItemFactory, Start, Finish, Reversed));
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        const ui64 index = key.Get<ui64>();
        return (index < GetListLength());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        const ui64 index = key.Get<ui64>();
        if (index >= GetListLength()) {
            return NUdf::TUnboxedValuePod();
        }

        const ui64 realIndex = Reversed ? (Finish - 1 - index) : (Start + index);
        return ItemFactory(List[realIndex]).Release().MakeOptional();
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(new TDictIterator(this->GetMemInfo(), MakeHolder<TIterator>(this->GetMemInfo(), List, ItemFactory, Start, Finish, Reversed)));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(new TDictIterator(this->GetMemInfo(), MakeHolder<TIterator>(this->GetMemInfo(), List, ItemFactory, Start, Finish, Reversed)));
    }

    ui64 GetDictLength() const override {
        return GetListLength();
    }

    bool HasDictItems() const override {
        return Finish != Start;
    }

    bool IsSortedDict() const override {
        return true;
    }

private:
    const TVectorType& List;
    const TItemFactory ItemFactory;
    const ui64 Start;
    const ui64 Finish;
    const bool Reversed;
};

template <typename TVectorType>
class TOwningVectorListAdapter : private TVectorType, public TVectorListAdapter<TVectorType> {
public:
    using TAdapterBase = TVectorListAdapter<TVectorType>;

    TOwningVectorListAdapter(
            TMemoryUsageInfo* memInfo,
            TVectorType&& list,
            typename TAdapterBase::TItemFactory itemFactory,
            ui64 start, ui64 finish,
            bool reversed)
        : TVectorType(std::move(list))
        , TAdapterBase(memInfo, *this, itemFactory, start, finish, reversed) {}

    TOwningVectorListAdapter(
            TMemoryUsageInfo* memInfo,
            const TVectorType& list,
            typename TAdapterBase::TItemFactory itemFactory,
            ui64 start, ui64 finish,
            bool reversed)
        : TVectorType(list)
        , TAdapterBase(memInfo, *this, itemFactory, start, finish, reversed) {}
};

template<typename TVectorType>
NUdf::TUnboxedValue CreateOwningVectorListAdapter(
    TVectorType&& list,
    typename TVectorListAdapter<std::remove_reference_t<TVectorType>>::TItemFactory itemFactory,
    ui64 start, ui64 finish,
    bool reversed,
    TMemoryUsageInfo& memInfo)
{
    return NUdf::TUnboxedValuePod(new TOwningVectorListAdapter<std::remove_reference_t<TVectorType>>(
        &memInfo, std::forward<TVectorType>(list), itemFactory, start, finish, reversed));
}

}
}
