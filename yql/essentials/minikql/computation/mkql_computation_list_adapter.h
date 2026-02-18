#pragma once
#include <yql/essentials/minikql/defs.h>
#include "mkql_computation_node_impl.h"
#include <yql/essentials/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {

template <typename TVectorType>
class TVectorListAdapter: public TComputationValue<TVectorListAdapter<TVectorType>> {
public:
    typedef typename TVectorType::value_type TItem;
    typedef TVectorListAdapter<TVectorType> TSelf;
    typedef std::function<NUdf::TUnboxedValue(const TItem&)> TItemFactory;
    typedef TComputationValue<TVectorListAdapter<TVectorType>> TBase;

    class TIterator: public TTemporaryComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, const TVectorType& list,
                  TItemFactory itemFactory, ui64 start, ui64 finish, bool reversed)
            : TTemporaryComputationValue<TIterator>(memInfo)
            , List_(list)
            , ItemFactory_(itemFactory)
            , Start_(start)
            , Finish_(finish)
            , Reversed_(reversed)
        {
            Index_ = Reversed_ ? Finish_ : (Start_ - 1);
        }

    private:
        bool Next(NUdf::TUnboxedValue& value) override {
            if (!Skip()) {
                return false;
            }
            value = ItemFactory_(List_[Index_]);
            return true;
        }

        bool Skip() override {
            if (!Reversed_) {
                if (Index_ + 1 >= Finish_) {
                    return false;
                }
                ++Index_;
            } else {
                if (Index_ < Start_ + 1) {
                    return false;
                }
                --Index_;
            }

            return true;
        }

        const TVectorType& List_;
        const TItemFactory ItemFactory_;
        const ui64 Start_;
        const ui64 Finish_;
        const bool Reversed_;
        ui64 Index_;
    };

    class TDictIterator: public TTemporaryComputationValue<TDictIterator> {
    public:
        TDictIterator(TMemoryUsageInfo* memInfo, THolder<TIterator>&& iter)
            : TTemporaryComputationValue<TDictIterator>(memInfo)
            , Iter_(std::move(iter))
            , Index_(Max<ui64>())
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& key) override {
            if (NUdf::TBoxedValueAccessor::Skip(*Iter_)) {
                key = NUdf::TUnboxedValuePod(++Index_);
                return true;
            }

            return false;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (NUdf::TBoxedValueAccessor::Next(*Iter_, payload)) {
                key = NUdf::TUnboxedValuePod(++Index_);
                return true;
            }

            return false;
        }

        bool Skip() override {
            if (NUdf::TBoxedValueAccessor::Skip(*Iter_)) {
                ++Index_;
                return true;
            }

            return false;
        }

        THolder<TIterator> Iter_;
        ui64 Index_;
    };

    TVectorListAdapter(
        TMemoryUsageInfo* memInfo,
        const TVectorType& list,
        TItemFactory itemFactory,
        ui64 start, ui64 finish,
        bool reversed)
        : TBase(memInfo)
        , List_(list)
        , ItemFactory_(itemFactory)
        , Start_(start)
        , Finish_(finish)
        , Reversed_(reversed)
    {
        Y_ABORT_UNLESS(Start_ <= Finish_ && Finish_ <= List_.size());
    }

private:
    bool HasFastListLength() const override {
        return true;
    }

    ui64 GetListLength() const override {
        return Finish_ - Start_;
    }

    ui64 GetEstimatedListLength() const override {
        return Finish_ - Start_;
    }

    bool HasListItems() const override {
        return Finish_ != Start_;
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return new TSelf(this->GetMemInfo(), List_, ItemFactory_, Start_, Finish_, !Reversed_);
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        const ui64 newStart = Min(Start_ + count, Finish_);
        if (newStart == Start_) {
            return const_cast<TVectorListAdapter*>(this);
        }

        return new TSelf(this->GetMemInfo(), List_, ItemFactory_, newStart, Finish_, Reversed_);
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        const ui64 newFinish = Min(Start_ + count, Finish_);
        if (newFinish == Finish_) {
            return const_cast<TVectorListAdapter*>(this);
        }

        return new TSelf(this->GetMemInfo(), List_, ItemFactory_, Start_, newFinish, Reversed_);
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return const_cast<TVectorListAdapter*>(this);
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(this->GetMemInfo(), List_, ItemFactory_, Start_, Finish_, Reversed_));
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

        const ui64 realIndex = Reversed_ ? (Finish_ - 1 - index) : (Start_ + index);
        return ItemFactory_(List_[realIndex]).Release().MakeOptional();
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(new TDictIterator(
            this->GetMemInfo(),
            MakeHolder<TIterator>(this->GetMemInfo(), List_, ItemFactory_, Start_, Finish_, Reversed_)));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(new TDictIterator(
            this->GetMemInfo(),
            MakeHolder<TIterator>(this->GetMemInfo(), List_, ItemFactory_, Start_, Finish_, Reversed_)));
    }

    ui64 GetDictLength() const override {
        return GetListLength();
    }

    bool HasDictItems() const override {
        return Finish_ != Start_;
    }

    bool IsSortedDict() const override {
        return true;
    }

private:
    const TVectorType& List_;
    const TItemFactory ItemFactory_;
    const ui64 Start_;
    const ui64 Finish_;
    const bool Reversed_;
};

template <typename TVectorType>
class TOwningVectorListAdapter: private TVectorType, public TVectorListAdapter<TVectorType> {
public:
    using TAdapterBase = TVectorListAdapter<TVectorType>;

    TOwningVectorListAdapter(
        TMemoryUsageInfo* memInfo,
        TVectorType&& list,
        typename TAdapterBase::TItemFactory itemFactory,
        ui64 start, ui64 finish,
        bool reversed)
        : TVectorType(std::move(list))
        , TAdapterBase(memInfo, *this, itemFactory, start, finish, reversed)
    {
    }

    TOwningVectorListAdapter(
        TMemoryUsageInfo* memInfo,
        const TVectorType& list,
        typename TAdapterBase::TItemFactory itemFactory,
        ui64 start, ui64 finish,
        bool reversed)
        : TVectorType(list)
        , TAdapterBase(memInfo, *this, itemFactory, start, finish, reversed)
    {
    }
};

template <typename TVectorType>
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

} // namespace NKikimr::NMiniKQL
