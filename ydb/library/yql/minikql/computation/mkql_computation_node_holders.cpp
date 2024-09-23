#include "mkql_computation_node_holders.h"
#include "mkql_computation_node_pack.h"
#include "mkql_custom_list.h"
#include "mkql_value_builder.h"
#include "presort.h"

#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/singleton.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TValueDataHolder: public TComputationValue<TValueDataHolder> {
public:
    TValueDataHolder(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value)
        : TComputationValue(memInfo)
        , Value(std::move(value))
    {}

private:
    const NUdf::TUnboxedValue Value;
};

class TDirectListHolder: public TComputationValue<TDirectListHolder> {
public:
    class TIterator: public TComputationValue<TIterator> {
    public:
        TIterator(const TDirectListHolder* parent)
            : TComputationValue(parent->GetMemInfo())
            , Parent(const_cast<TDirectListHolder*>(parent))
            , Iterator(parent->Items)
            , AtStart(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart) {
                AtStart = false;
            } else {
                if (Iterator.AtEnd()) {
                    return false;
                }

                Iterator.Next();
            }

            return !Iterator.AtEnd();
        }

        bool Next(NUdf::TUnboxedValue& value) override {
            if (!Skip())
                return false;
            value = Iterator.Current();
            return true;
        }

        const NUdf::TRefCountedPtr<TDirectListHolder> Parent;
        TDefaultListRepresentation::TIterator Iterator;
        bool AtStart;
    };

    class TDictIterator: public TComputationValue<TDictIterator> {
    public:
        TDictIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& iter)
            : TComputationValue(memInfo)
            , Iter(std::move(iter))
            , Index(Max<ui64>())
        {}

    private:
        bool Next(NUdf::TUnboxedValue& key) override {
            if (Iter.Skip()) {
                key = NUdf::TUnboxedValuePod(++Index);
                return true;
            }

            return false;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (Iter.Next(payload)) {
                key = NUdf::TUnboxedValuePod(++Index);
                return true;
            }

            return false;
        }

        bool Skip() override {
            if (Iter.Skip()) {
                ++Index;
                return true;
            }

            return false;
        }

        const NUdf::TUnboxedValue Iter;
        ui64 Index;
    };

    TDirectListHolder(TMemoryUsageInfo* memInfo, TDefaultListRepresentation&& items)
        : TComputationValue(memInfo)
        , Items(std::move(items))
    {}

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        const ui64 index = key.Get<ui64>();
        return (index < GetListLength());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        const ui64 index = key.Get<ui64>();
        if (index >= GetListLength()) {
            return NUdf::TUnboxedValuePod();
        }

        return Items.GetItemByIndex(index).Release().MakeOptional();
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(new TDictIterator(GetMemInfo(), GetListIterator()));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(new TDictIterator(GetMemInfo(), GetListIterator()));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    bool HasFastListLength() const override {
        return true;
    }

    ui64 GetListLength() const override {
        return Items.GetLength();
    }

    ui64 GetEstimatedListLength() const override {
        return Items.GetLength();
    }

    bool HasListItems() const override {
        return Items.GetLength() != 0;
    }

    const NUdf::TOpaqueListRepresentation* GetListRepresentation() const override {
        return reinterpret_cast<const NUdf::TOpaqueListRepresentation*>(&Items);
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override {
        switch (Items.GetLength()) {
            case 0U: return builder.NewEmptyList().Release().AsBoxed();
            case 1U: return const_cast<TDirectListHolder*>(this);
            default: break;
        }

        TDefaultListRepresentation result;
        for (auto it = Items.GetReverseIterator(); !it.AtEnd(); it.Next()) {
            result = result.Append(NUdf::TUnboxedValue(it.Current()));
        }

        return new TDirectListHolder(GetMemInfo(), std::move(result));
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        if (count == 0)
            return const_cast<TDirectListHolder*>(this);

        if (count >= Items.GetLength())
            return builder.NewEmptyList().Release().AsBoxed();

        auto result = Items.SkipFromBegin(static_cast<size_t>(count));
        return new TDirectListHolder(GetMemInfo(), std::move(result));
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        if (count == 0)
            return builder.NewEmptyList().Release().AsBoxed();

        if (count >= Items.GetLength())
            return const_cast<TDirectListHolder*>(this);

        auto result = Items.SkipFromEnd(static_cast<size_t>(Items.GetLength() - count));
        return new TDirectListHolder(GetMemInfo(), std::move(result));
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return const_cast<TDirectListHolder*>(this);
    }

    ui64 GetDictLength() const override {
        return GetListLength();
    }

    bool HasDictItems() const override {
        return Items.GetLength() != 0;
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const final {
        return Items.GetItemByIndex(index);
    }

    const NUdf::TUnboxedValue* GetElements() const final {
        return Items.GetItems();
    }

    bool IsSortedDict() const override {
        return true;
    }

    TDefaultListRepresentation Items;
};

template <class TBaseVector>
class TVectorHolderBase: public TComputationValue<TVectorHolderBase<TBaseVector>>, public TBaseVector {
private:
    using TBaseValue = TComputationValue<TVectorHolderBase<TBaseVector>>;
public:
    TVectorHolderBase(TMemoryUsageInfo* memInfo)
        : TBaseValue(memInfo)
    {

    }
    TVectorHolderBase(TMemoryUsageInfo* memInfo, TBaseVector&& vector)
        : TBaseValue(memInfo)
        , TBaseVector(std::move(vector)) {
    }

    ~TVectorHolderBase() {
    }

private:
    class TValuesIterator: public TTemporaryComputationValue<TValuesIterator> {
    private:
        using TBase = TTemporaryComputationValue<TValuesIterator>;
    public:
        TValuesIterator(const TVectorHolderBase* parent)
            : TBase(parent->GetMemInfo())
            , Size(parent->size())
            , Parent(const_cast<TVectorHolderBase*>(parent)) {
        }

    private:
        bool Skip() final {
            return ++Current < Size;
        }

        bool Next(NUdf::TUnboxedValue& value) final {
            if (Size <= Current) {
                return false;
            }
            value = (*Parent)[Current];
            ++Current;
            return true;
        }

        const size_t Size;
        ui64 Current = 0;
        const NUdf::TRefCountedPtr<TVectorHolderBase> Parent;
    };

    class TDictIterator: public TTemporaryComputationValue<TDictIterator> {
    private:
        using TBase = TTemporaryComputationValue<TDictIterator>;
    public:
        TDictIterator(const TVectorHolderBase* parent)
            : TBase(parent->GetMemInfo())
            , Size(parent->size())
            , Parent(const_cast<TVectorHolderBase*>(parent)) {
        }
    private:
        bool Skip() final {
            return ++Current < Size;
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (Current == Size) {
                return false;
            }
            key = NUdf::TUnboxedValuePod(Current);
            ++Current;
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (Current == Size) {
                return false;
            }
            key = NUdf::TUnboxedValuePod(Current);
            payload = (*Parent)[Current];
            ++Current;
            return true;
        }

        const size_t Size;
        ui64 Current = 0;
        const NUdf::TRefCountedPtr<TVectorHolderBase> Parent;
    };

    bool HasListItems() const final {
        return TBaseVector::size();
    }

    bool HasDictItems() const final {
        return TBaseVector::size();
    }

    bool HasFastListLength() const final {
        return true;
    }

    ui64 GetListLength() const final {
        return TBaseVector::size();
    }

    ui64 GetDictLength() const final {
        return TBaseVector::size();
    }

    ui64 GetEstimatedListLength() const final {
        return TBaseVector::size();
    }

    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TUnboxedValuePod(new TValuesIterator(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const final {
        return NUdf::TUnboxedValuePod(new TDictIterator(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        return NUdf::TUnboxedValuePod(new TValuesIterator(this));
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        return NUdf::TUnboxedValuePod(new TDictIterator(this));
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder&) const final {
        if (1U >= TBaseVector::size()) {
            return const_cast<TVectorHolderBase*>(this);
        }

        TBaseVector copy(TBaseVector::rbegin(), TBaseVector::rend());
        return new TVectorHolderBase(TBaseValue::GetMemInfo(), std::move(copy));
    }

    void Push(const NUdf::TUnboxedValuePod& value) final {
        TBaseVector::emplace_back(value);
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        if (!count)
            return const_cast<TVectorHolderBase*>(this);

        if (count >= TBaseVector::size())
            return builder.NewEmptyList().Release().AsBoxed();

        TBaseVector copy(TBaseVector::begin() + count, TBaseVector::end());
        return new TVectorHolderBase(TBaseValue::GetMemInfo(), std::move(copy));
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        if (!count)
            return builder.NewEmptyList().Release().AsBoxed();

        if (count >= TBaseVector::size())
            return const_cast<TVectorHolderBase*>(this);

        TBaseVector copy(TBaseVector::begin(), TBaseVector::begin() + count);
        return new TVectorHolderBase(TBaseValue::GetMemInfo(), std::move(copy));
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder&) const final {
        return const_cast<TVectorHolderBase*>(this);
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        return key.Get<ui64>() < TBaseVector::size();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        const auto index = key.Get<ui64>();
        return index < TBaseVector::size() ? TBaseVector::at(index).MakeOptional() : NUdf::TUnboxedValuePod();
    }

    const NUdf::TUnboxedValue* GetElements() const final {
        return TBaseVector::data();
    }

    bool IsSortedDict() const override {
        return true;
    }
};

class TVectorHolder: public TVectorHolderBase<TUnboxedValueVector> {
private:
    using TBase = TVectorHolderBase<TUnboxedValueVector>;
public:
    using TBase::TBase;
};

class TTemporaryVectorHolder: public TVectorHolderBase<TTemporaryUnboxedValueVector> {
private:
    using TBase = TVectorHolderBase<TTemporaryUnboxedValueVector>;
public:
    using TBase::TBase;
};

class TEmptyContainerHolder: public TComputationValue<TEmptyContainerHolder> {
public:
    TEmptyContainerHolder(TMemoryUsageInfo* memInfo)
        : TComputationValue(memInfo), None()
    {}

private:
    bool Contains(const NUdf::TUnboxedValuePod&) const override {
        return false;
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod&) const override {
        return None;
    }

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue&) override {
        return NUdf::EFetchStatus::Finish;
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return NUdf::TUnboxedValuePod(const_cast<TEmptyContainerHolder*>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(const_cast<TEmptyContainerHolder*>(this));
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(const_cast<TEmptyContainerHolder*>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return NUdf::TUnboxedValuePod(const_cast<TEmptyContainerHolder*>(this));
    }

    bool Skip() final {
        return false;
    }

    bool Next(NUdf::TUnboxedValue&) final {
        return false;
    }

    bool NextPair(NUdf::TUnboxedValue&, NUdf::TUnboxedValue&) final {
        return false;
    }

    const NUdf::TOpaqueListRepresentation* GetListRepresentation() const override {
        return reinterpret_cast<const NUdf::TOpaqueListRepresentation*>(&List);
    }

    bool HasFastListLength() const override {
        return true;
    }

    ui64 GetListLength() const override {
        return 0;
    }

    ui64 GetEstimatedListLength() const override {
        return 0;
    }

    bool HasListItems() const override {
        return false;
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return const_cast<TEmptyContainerHolder*>(this);
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        Y_UNUSED(count);
        return const_cast<TEmptyContainerHolder*>(this);
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        Y_UNUSED(builder);
        Y_UNUSED(count);
        return const_cast<TEmptyContainerHolder*>(this);
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override {
        Y_UNUSED(builder);
        return const_cast<TEmptyContainerHolder*>(this);
    }

    ui64 GetDictLength() const override {
        return 0;
    }

    bool HasDictItems() const override {
        return false;
    }

    bool IsSortedDict() const override {
        return true;
    }

    const NUdf::TUnboxedValue* GetElements() const override {
        return &None;
    }

    const NUdf::TUnboxedValue None;
    const TDefaultListRepresentation List;
};

class TSortedSetHolder: public TComputationValue<TSortedSetHolder> {
public:
    typedef TUnboxedValueVector TItems;

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        TIterator(const TSortedSetHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent(const_cast<TSortedSetHolder*>(parent))
            , Iterator(Parent->Items.begin())
            , AtStart(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart) {
                AtStart = false;
            } else {
                if (Iterator == Parent->Items.end())
                    return false;

                ++Iterator;
            }

           return Iterator != Parent->Items.end();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip())
                return false;
            if (NoSwap) {
                key = *Iterator;
                if (Parent->Packer) {
                    key = Parent->Packer->Decode(key.AsStringRef(), false, Parent->HolderFactory);
                }
            } else {
                key = NUdf::TUnboxedValuePod::Void();
            }
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
           if (!Next(key))
               return false;
           if (NoSwap) {
               payload = NUdf::TUnboxedValuePod::Void();
           } else {
                payload = *Iterator;
                if (Parent->Packer) {
                    payload = Parent->Packer->Decode(payload.AsStringRef(), false, Parent->HolderFactory);
                }
           }
           return true;
        }

        const NUdf::TRefCountedPtr<TSortedSetHolder> Parent;
        TItems::const_iterator Iterator;
        bool AtStart;
    };

    TSortedSetHolder(
            TMemoryUsageInfo* memInfo,
            TSortedSetFiller filler,
            const TKeyTypes& types,
            bool isTuple,
            EDictSortMode mode,
            bool eagerFill,
            TType* encodedType,
            const NUdf::ICompare* compare,
            const NUdf::IEquate* equate,
            const THolderFactory& holderFactory)
        : TComputationValue(memInfo)
        , Filler(filler)
        , Types(types)
        , IsTuple(isTuple)
        , Mode(mode)
        , Compare(compare)
        , Equate(equate)
        , IsBuilt(false)
        , HolderFactory(holderFactory)
    {
        if (encodedType) {
            Packer.emplace(encodedType);
        }

        if (eagerFill)
            LazyBuildDict();
    }

    ~TSortedSetHolder() {
        MKQL_MEM_RETURN(GetMemInfo(), &Items, Items.capacity() * sizeof(TItems::value_type));
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer) {
            encodedKey = MakeString(Packer->Encode(key, false));
        }

        return BinarySearch(Items.begin(), Items.end(), NUdf::TUnboxedValuePod(Packer ? encodedKey : key),
            TValueLess(Types, IsTuple, Compare));
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer) {
            encodedKey = MakeString(Packer->Encode(key, false));
        }

        const auto it = LowerBound(Items.begin(), Items.end(), NUdf::TUnboxedValuePod(Packer ? encodedKey : key), TValueLess(Types, IsTuple, Compare));
        if (it == Items.end() || !TValueEqual(Types, IsTuple, Equate)(*it, NUdf::TUnboxedValuePod(Packer ? encodedKey : key)))
            return NUdf::TUnboxedValuePod();

        return it->MakeOptional();
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<false>(this));
    }

    ui64 GetDictLength() const override {
        LazyBuildDict();
        return Items.size();
    }

    bool HasDictItems() const override {
        LazyBuildDict();
        return !Items.empty();
    }

    void LazyBuildDict() const {
        if (IsBuilt)
            return;

        Filler(Items);
        Filler = TSortedSetFiller();

        switch (Mode) {
        case EDictSortMode::RequiresSorting:
            StableSort(Items.begin(), Items.end(), TValueLess(Types, IsTuple, Compare));
            Items.erase(Unique(Items.begin(), Items.end(), TValueEqual(Types, IsTuple, Equate)), Items.end());
            break;
        case EDictSortMode::SortedUniqueAscending:
            break;
        case EDictSortMode::SortedUniqueDescening:
            Reverse(Items.begin(), Items.end());
            break;
        default:
            Y_ABORT();
        }

        Y_DEBUG_ABORT_UNLESS(IsSortedUnique());
        IsBuilt = true;

        if (!Items.empty()) {
            MKQL_MEM_TAKE(GetMemInfo(), &Items, Items.capacity() * sizeof(TItems::value_type));
        }
    }

    bool IsSortedUnique() const {
        TValueLess less(Types, IsTuple, Compare);
        for (size_t i = 1, e = Items.size(); i < e; ++i) {
            if (!less(Items[i - 1], Items[i]))
                return false;
        }

        return true;
    }

    bool IsSortedDict() const override {
        return true;
    }

private:
    mutable TSortedSetFiller Filler;
    const TKeyTypes Types;
    const bool IsTuple;
    const EDictSortMode Mode;
    const NUdf::ICompare* Compare;
    const NUdf::IEquate* Equate;
    mutable bool IsBuilt;
    const THolderFactory& HolderFactory;
    mutable TItems Items;
    mutable std::optional<TGenericPresortEncoder> Packer;
};

class TSortedDictHolder: public TComputationValue<TSortedDictHolder> {
public:
    typedef TKeyPayloadPairVector TItems;

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        TIterator(const TSortedDictHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent(const_cast<TSortedDictHolder*>(parent))
            , Iterator(Parent->Items.begin())
            , AtStart(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart) {
                AtStart = false;
            } else {
                if (Iterator == Parent->Items.end())
                    return false;

                ++Iterator;
            }

           return Iterator != Parent->Items.end();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip())
                return false;
            if (NoSwap) {
                key = Iterator->first;
                if (Parent->Packer) {
                    key = Parent->Packer->Decode(key.AsStringRef(), false, Parent->HolderFactory);
                }
            } else {
                key = Iterator->second;
            }
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
           if (!Next(key))
               return false;
           if (NoSwap) {
               payload = Iterator->second;
           } else {
                payload = Iterator->first;
                if (Parent->Packer) {
                    payload = Parent->Packer->Decode(payload.AsStringRef(), false, Parent->HolderFactory);
                }
           }
           return true;
        }

        const NUdf::TRefCountedPtr<TSortedDictHolder> Parent;
        TItems::const_iterator Iterator;
        bool AtStart;
    };

    TSortedDictHolder(
            TMemoryUsageInfo* memInfo,
            TSortedDictFiller filler,
            const TKeyTypes& types,
            bool isTuple,
            EDictSortMode mode,
            bool eagerFill,
            TType* encodedType,
            const NUdf::ICompare* compare,
            const NUdf::IEquate* equate,
            const THolderFactory& holderFactory)
        : TComputationValue(memInfo)
        , Filler(filler)
        , Types(types)
        , IsTuple(isTuple)
        , Mode(mode)
        , Compare(compare)
        , Equate(equate)
        , IsBuilt(false)
        , HolderFactory(holderFactory)
    {
        if (encodedType) {
            Packer.emplace(encodedType);
        }

        if (eagerFill)
            LazyBuildDict();
    }

    ~TSortedDictHolder() {
        MKQL_MEM_RETURN(GetMemInfo(), &Items, Items.capacity() * sizeof(TItems::value_type));
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer) {
            encodedKey = MakeString(Packer->Encode(key, false));
        }

        return BinarySearch(Items.begin(), Items.end(), TItems::value_type(NUdf::TUnboxedValuePod(Packer ? encodedKey : key), NUdf::TUnboxedValuePod()),
            TKeyPayloadPairLess(Types, IsTuple, Compare));
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer) {
            encodedKey = MakeString(Packer->Encode(key, false));
        }

        const auto it = LowerBound(Items.begin(), Items.end(), TItems::value_type(NUdf::TUnboxedValuePod(Packer ? encodedKey : key), NUdf::TUnboxedValue()), TKeyPayloadPairLess(Types, IsTuple, Compare));
        if (it == Items.end() || !TKeyPayloadPairEqual(Types, IsTuple, Equate)({it->first, it->second}, TKeyPayloadPair(NUdf::TUnboxedValuePod(Packer ? encodedKey : key), {})))
            return NUdf::TUnboxedValuePod();

        return it->second.MakeOptional();
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<false>(this));
    }

    ui64 GetDictLength() const override {
        LazyBuildDict();
        return Items.size();
    }

    bool HasDictItems() const override {
        LazyBuildDict();
        return !Items.empty();
    }

    void LazyBuildDict() const {
        if (IsBuilt)
            return;

        Filler(Items);
        Filler = TSortedDictFiller();

        switch (Mode) {
        case EDictSortMode::RequiresSorting:
            StableSort(Items.begin(), Items.end(), TKeyPayloadPairLess(Types, IsTuple, Compare));
            Items.erase(Unique(Items.begin(), Items.end(), TKeyPayloadPairEqual(Types, IsTuple, Equate)), Items.end());
            break;
        case EDictSortMode::SortedUniqueAscending:
            break;
        case EDictSortMode::SortedUniqueDescening:
            Reverse(Items.begin(), Items.end());
            break;
        default:
            Y_ABORT();
        }

        Y_DEBUG_ABORT_UNLESS(IsSortedUnique());
        IsBuilt = true;

        if (!Items.empty()) {
            MKQL_MEM_TAKE(GetMemInfo(), &Items, Items.capacity() * sizeof(TItems::value_type));
        }
    }

    bool IsSortedUnique() const {
        TKeyPayloadPairLess less(Types, IsTuple, Compare);
        for (size_t i = 1, e = Items.size(); i < e; ++i) {
            if (!less(Items[i - 1], Items[i]))
                return false;
        }

        return true;
    }

    bool IsSortedDict() const override {
        return true;
    }

private:
    mutable TSortedDictFiller Filler;
    const TKeyTypes Types;
    const bool IsTuple;
    const EDictSortMode Mode;
    const NUdf::ICompare* Compare;
    const NUdf::IEquate* Equate;
    mutable bool IsBuilt;
    const THolderFactory& HolderFactory;
    mutable TItems Items;
    mutable std::optional<TGenericPresortEncoder> Packer;
};

class THashedSetHolder : public TComputationValue<THashedSetHolder> {
public:
    class TIterator : public TComputationValue<TIterator> {
    public:
        TIterator(const THashedSetHolder* parent)
            : TComputationValue(parent->GetMemInfo())
            , Parent(const_cast<THashedSetHolder*>(parent))
            , Iterator(Parent->Set.begin())
            , End(Parent->Set.end())
            , AtStart(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart) {
                AtStart = false;
            }
            else {
                if (Iterator == End) {
                    return false;
                }

                ++Iterator;
            }

            return Iterator != End;
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip())
                return false;
            key = *Iterator;
            if (Parent->Packer) {
                key = Parent->Packer->Unpack(key.AsStringRef(), Parent->HolderFactory);
            }

            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key))
                return false;
            payload = NUdf::TUnboxedValuePod::Void();
            return true;
        }

    private:
        const NUdf::TRefCountedPtr<THashedSetHolder> Parent;
        TValuesDictHashSet::const_iterator Iterator;
        TValuesDictHashSet::const_iterator End;
        bool AtStart;
    };

    THashedSetHolder(TMemoryUsageInfo* memInfo, THashedSetFiller filler,
        const TKeyTypes& types, bool isTuple, bool eagerFill, TType* encodedType,
        const NUdf::IHash* hash, const NUdf::IEquate* equate, const THolderFactory& holderFactory)
        : TComputationValue(memInfo)
        , Filler(filler)
        , Types(types)
        , Set(0, TValueHasher(Types, isTuple, hash), TValueEqual(Types, isTuple, equate))
        , IsBuilt(false)
        , HolderFactory(holderFactory)
    {
        if (encodedType) {
            Packer.emplace(true, encodedType);
        }

        if (eagerFill)
            LazyBuildDict();
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer) {
            encodedKey = MakeString(Packer->Pack(key));
        }

        return Set.find(NUdf::TUnboxedValuePod(Packer ? encodedKey : key)) != Set.cend();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer) {
            encodedKey = MakeString(Packer->Pack(key));
        }

        const auto it = Set.find(NUdf::TUnboxedValuePod(Packer ? encodedKey : key));
        if (it == Set.cend())
            return NUdf::TUnboxedValuePod();
        return NUdf::TUnboxedValuePod::Void();
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    ui64 GetDictLength() const override {
        LazyBuildDict();
        return Set.size();
    }

    bool HasDictItems() const override {
        LazyBuildDict();
        return !Set.empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

private:
    void LazyBuildDict() const {
        if (IsBuilt)
            return;

        Filler(Set);
        Filler = THashedSetFiller();

        IsBuilt = true;
    }

private:
    mutable THashedSetFiller Filler;
    const TKeyTypes Types;
    mutable TValuesDictHashSet Set;
    mutable bool IsBuilt;
    const THolderFactory& HolderFactory;
    mutable std::optional<TValuePacker> Packer;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedSetHolder : public TComputationValue<THashedSingleFixedSetHolder<T, OptionalKey>> {
public:
    using TSetType = TValuesDictHashSingleFixedSet<T>;

    class TIterator : public TComputationValue<TIterator> {
    public:
        enum class EState {
            AtStart,
            AtNull,
            Iterator
        };
        TIterator(const THashedSingleFixedSetHolder* parent)
            : TComputationValue<TIterator>(parent->GetMemInfo())
            , Parent(const_cast<THashedSingleFixedSetHolder*>(parent))
            , Iterator(Parent->Set.begin())
            , End(Parent->Set.end())
            , State(EState::AtStart)
        {
        }

    private:
        bool Skip() final {
            switch (State) {
            case EState::AtStart:
                State = OptionalKey && Parent->HasNull ? EState::AtNull : EState::Iterator;
                break;
            case EState::AtNull:
                State = EState::Iterator;
                break;
            case EState::Iterator:
                if (Iterator == End)
                    return false;
                ++Iterator;
                break;
            }

            return EState::AtNull == State || Iterator != End;
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip())
                return false;
            key = EState::AtNull == State ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(*Iterator);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key))
                return false;
            payload = NUdf::TUnboxedValuePod::Void();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedSetHolder> Parent;
        typename TSetType::const_iterator Iterator;
        typename TSetType::const_iterator End;
        EState State;
    };

    THashedSingleFixedSetHolder(TMemoryUsageInfo* memInfo, TSetType&& set, bool hasNull)
        : TComputationValue<THashedSingleFixedSetHolder>(memInfo)
        , Set(std::move(set))
        , HasNull(hasNull)
    {
        MKQL_ENSURE(OptionalKey || !HasNull, "Null value is not allowed for non-optional key type");
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return HasNull;
            }
        }
        return Set.find(key.Get<T>()) != Set.cend();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        if (Contains(key))
            return NUdf::TUnboxedValuePod::Void();
        return NUdf::TUnboxedValuePod();
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    ui64 GetDictLength() const final {
        return Set.size() + ui64(OptionalKey && HasNull);
    }

    bool HasDictItems() const final {
        return !Set.empty() || (OptionalKey && HasNull);
    }

    bool IsSortedDict() const final {
        return false;
    }

    const TSetType Set;
    const bool HasNull;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactSetHolder : public TComputationValue<THashedSingleFixedCompactSetHolder<T, OptionalKey>> {
public:
    using TSetType = TValuesDictHashSingleFixedCompactSet<T>;

    class TIterator : public TComputationValue<TIterator> {
    public:
        enum class EState {
            AtStart,
            AtNull,
            Iterator
        };
        TIterator(const THashedSingleFixedCompactSetHolder* parent)
            : TComputationValue<TIterator>(parent->GetMemInfo())
            , Parent(const_cast<THashedSingleFixedCompactSetHolder*>(parent))
            , Iterator(Parent->Set.Iterate())
            , State(EState::AtStart)
        {
        }

    private:
        bool Skip() final {
            switch (State) {
            case EState::AtStart:
                State = OptionalKey && Parent->HasNull ? EState::AtNull : EState::Iterator;
                break;
            case EState::AtNull:
                State = EState::Iterator;
                break;
            case EState::Iterator:
                if (!Iterator.Ok())
                    return false;
                ++Iterator;
                break;
            }

            return EState::AtNull == State || Iterator.Ok();
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip())
                return false;
            key = EState::AtNull == State ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(*Iterator);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key))
                return false;
            payload = NUdf::TUnboxedValuePod::Void();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedCompactSetHolder> Parent;
        typename TSetType::TIterator Iterator;
        EState State;
    };

    THashedSingleFixedCompactSetHolder(TMemoryUsageInfo* memInfo, TSetType&& set, bool hasNull)
        : TComputationValue<THashedSingleFixedCompactSetHolder>(memInfo)
        , Set(std::move(set))
        , HasNull(hasNull)
    {
        MKQL_ENSURE(OptionalKey || !HasNull, "Null value is not allowed for non-optional key type");
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return HasNull;
            }
        }
        return Set.Has(key.Get<T>());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        if (Contains(key))
            return NUdf::TUnboxedValuePod::Void();
        return NUdf::TUnboxedValuePod();
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    ui64 GetDictLength() const final {
        return Set.Size() + ui64(OptionalKey && HasNull);
    }

    bool HasDictItems() const final {
        return !Set.Empty() || (OptionalKey && HasNull);
    }

    bool IsSortedDict() const final {
        return false;
    }

    const TSetType Set;
    const bool HasNull;
};

class THashedCompactSetHolder : public TComputationValue<THashedCompactSetHolder> {
public:
    using TSetType = TValuesDictHashCompactSet;

    class TIterator : public TComputationValue<TIterator> {
    public:
        TIterator(const THashedCompactSetHolder* parent)
            : TComputationValue(parent->GetMemInfo())
            , Parent(const_cast<THashedCompactSetHolder*>(parent))
            , Iterator(Parent->Set.Iterate())
            , AtStart(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart) {
                AtStart = false;
            }
            else {
                if (!Iterator.Ok())
                    return false;
                ++Iterator;
            }

            return Iterator.Ok();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip())
                return false;
            key = Parent->KeyPacker.Unpack(GetSmallValue(*Iterator), Parent->Ctx->HolderFactory);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key))
                return false;
            payload = NUdf::TUnboxedValuePod::Void();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedCompactSetHolder> Parent;
        typename TSetType::TIterator Iterator;
        bool AtStart;
    };

    THashedCompactSetHolder(TMemoryUsageInfo* memInfo, TSetType&& set, TPagedArena&& pool, TType* keyType, TComputationContext* ctx)
        : TComputationValue(memInfo)
        , Pool(std::move(pool))
        , Set(std::move(set))
        , KeyPacker(true, keyType)
        , Ctx(ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        return Set.Has(smallValue);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        if (Set.Has(smallValue))
            return NUdf::TUnboxedValuePod::Void();
        return NUdf::TUnboxedValuePod();
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(this));
    }

    ui64 GetDictLength() const override {
        return Set.Size();
    }

    bool HasDictItems() const override {
        return !Set.Empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

private:
    TPagedArena Pool;
    const TSetType Set;
    mutable TValuePacker KeyPacker;
    TComputationContext* Ctx;
};

class THashedCompactMapHolder : public TComputationValue<THashedCompactMapHolder> {
public:
    using TMapType = TValuesDictHashCompactMap;

    template <bool NoSwap>
    class TIterator : public TComputationValue<TIterator<NoSwap>> {
    public:
        TIterator(const THashedCompactMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent(const_cast<THashedCompactMapHolder*>(parent))
            , Iterator(Parent->Map.Iterate())
            , AtStart(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart) {
                AtStart = false;
            }
            else {
                if (!Iterator.Ok())
                    return false;
                ++Iterator;
            }

            return Iterator.Ok();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip())
                return false;
            key = NoSwap ?
                Parent->KeyPacker.Unpack(GetSmallValue(Iterator.Get().first), Parent->Ctx->HolderFactory):
                Parent->PayloadPacker.Unpack(GetSmallValue(Iterator.Get().second), Parent->Ctx->HolderFactory);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key))
                return false;
            payload = NoSwap ?
                Parent->PayloadPacker.Unpack(GetSmallValue(Iterator.Get().second), Parent->Ctx->HolderFactory):
                Parent->KeyPacker.Unpack(GetSmallValue(Iterator.Get().first), Parent->Ctx->HolderFactory);
            return true;
        }

        const NUdf::TRefCountedPtr<THashedCompactMapHolder> Parent;
        typename TMapType::TIterator Iterator;
        bool AtStart;
    };

    THashedCompactMapHolder(TMemoryUsageInfo* memInfo, TMapType&& map, TPagedArena&& pool,
        TType* keyType, TType* payloadType, TComputationContext* ctx)
        : TComputationValue(memInfo)
        , Pool(std::move(pool))
        , Map(std::move(map))
        , KeyPacker(true, keyType)
        , PayloadPacker(false, payloadType)
        , Ctx(ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        return Map.Has(smallValue);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        auto it = Map.Find(smallValue);
        if (!it.Ok())
            return NUdf::TUnboxedValuePod();
        return PayloadPacker.Unpack(GetSmallValue(it.Get().second), Ctx->HolderFactory).Release().MakeOptional();
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<false>(this));
    }

    ui64 GetDictLength() const override {
        return Map.Size();
    }

    bool HasDictItems() const override {
        return !Map.Empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

    TPagedArena Pool;
    const TMapType Map;
    mutable TValuePacker KeyPacker;
    mutable TValuePacker PayloadPacker;
    TComputationContext* Ctx;
};

class THashedCompactMultiMapHolder : public TComputationValue<THashedCompactMultiMapHolder> {
public:
    using TMapType = TValuesDictHashCompactMultiMap;
    using TMapIterator = typename TMapType::TIterator;

    class TPayloadList: public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(const THashedCompactMultiMapHolder* parent, TMapIterator from)
                : TComputationValue(parent->GetMemInfo())
                , Parent(const_cast<THashedCompactMultiMapHolder*>(parent))
                , Iterator(from)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                if (!Iterator.Ok()) {
                    return false;
                }

                value = Parent->PayloadPacker.Unpack(GetSmallValue(Iterator.GetValue()), Parent->CompCtx.HolderFactory);
                ++Iterator;
                return true;
            }

            bool Skip() override {
                if (!Iterator.Ok()) {
                    return false;
                }

                ++Iterator;
                return true;
            }

            const NUdf::TRefCountedPtr<THashedCompactMultiMapHolder> Parent;
            TMapIterator Iterator;
        };

        TPayloadList(TMemoryUsageInfo* memInfo, const THashedCompactMultiMapHolder* parent, TMapIterator from)
            : TCustomListValue(memInfo)
            , Parent(const_cast<THashedCompactMultiMapHolder*>(parent))
            , From(from)
        {
            Y_ASSERT(From.Ok());
        }

    private:
        bool HasFastListLength() const override {
            return true;
        }

        ui64 GetListLength() const override {
            if (!Length) {
                Length = Parent->Map.Count(From.GetKey());
            }

            return *Length;
        }

        bool HasListItems() const override {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator(Parent.Get(), From));
        }

        const NUdf::TRefCountedPtr<THashedCompactMultiMapHolder> Parent;
        TMapIterator From;
    };

    template <bool NoSwap>
    class TIterator : public TComputationValue<TIterator<NoSwap>> {
    public:
        TIterator(const THashedCompactMultiMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent(const_cast<THashedCompactMultiMapHolder*>(parent))
            , Iterator(parent->Map.Iterate())
        {
        }

    private:
        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Iterator.Ok()) {
                return false;
            }

            if (NoSwap) {
                key = Parent->KeyPacker.Unpack(GetSmallValue(Iterator.GetKey()), Parent->CompCtx.HolderFactory);
                payload = Parent->CompCtx.HolderFactory.Create<TPayloadList>(Parent.Get(), Iterator.MakeCurrentKeyIter());
            } else {
                payload = Parent->KeyPacker.Unpack(GetSmallValue(Iterator.GetKey()), Parent->CompCtx.HolderFactory);
                key = Parent->CompCtx.HolderFactory.Create<TPayloadList>(Parent.Get(), Iterator.MakeCurrentKeyIter());
            }
            Iterator.NextKey();
            return true;
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Iterator.Ok()) {
                return false;
            }

            key = NoSwap ?
                Parent->KeyPacker.Unpack(GetSmallValue(Iterator.GetKey()), Parent->CompCtx.HolderFactory):
                NUdf::TUnboxedValue(Parent->CompCtx.HolderFactory.Create<TPayloadList>(Parent.Get(), Iterator.MakeCurrentKeyIter()));
            Iterator.NextKey();
            return true;
        }

        bool Skip() override {
            if (!Iterator.Ok()) {
                return false;
            }

            Iterator.NextKey();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedCompactMultiMapHolder> Parent;
        TMapIterator Iterator;
    };

    THashedCompactMultiMapHolder(TMemoryUsageInfo* memInfo, TMapType&& map, TPagedArena&& pool,
        TType* keyType, TType* payloadType, TComputationContext* ctx)
        : TComputationValue(memInfo)
        , Pool(std::move(pool))
        , Map(std::move(map))
        , KeyPacker(true, keyType)
        , PayloadPacker(false, payloadType)
        , CompCtx(*ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        return Map.Has(smallValue);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        auto it = Map.Find(smallValue);
        if (!it.Ok())
            return NUdf::TUnboxedValuePod();

        return CompCtx.HolderFactory.Create<TPayloadList>(this, it);
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<false>(this));
    }

    ui64 GetDictLength() const override {
        return Map.UniqSize();
    }

    bool HasDictItems() const override {
        return !Map.Empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

    TPagedArena Pool;
    const TMapType Map;
    mutable TValuePacker KeyPacker;
    mutable TValuePacker PayloadPacker;
    TComputationContext& CompCtx;
};

class THashedDictHolder: public TComputationValue<THashedDictHolder> {
public:
    template <bool NoSwap>
    class TIterator: public TTemporaryComputationValue<TIterator<NoSwap>> {
    public:
        TIterator(const THashedDictHolder* parent)
            : TTemporaryComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent(const_cast<THashedDictHolder*>(parent))
            , Iterator(Parent->Map.begin())
            , End(Parent->Map.end())
            , AtStart(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart) {
                AtStart = false;
            } else {
                if (Iterator == End)
                    return false;
                ++Iterator;
            }

            return Iterator != End;
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip())
                return false;
            if (NoSwap) {
                key = Iterator->first;
                if (Parent->Packer) {
                    key = Parent->Packer->Unpack(key.AsStringRef(), Parent->HolderFactory);
                }
            } else {
                key = Iterator->second;
            }

            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key))
                return false;
            if (NoSwap) {
                payload = Iterator->second;
            } else {
                payload = Iterator->first;
                if (Parent->Packer) {
                    payload = Parent->Packer->Unpack(payload.AsStringRef(), Parent->HolderFactory);
                }
            }
            return true;
        }

        const NUdf::TRefCountedPtr<THashedDictHolder> Parent;
        TValuesDictHashMap::const_iterator Iterator;
        TValuesDictHashMap::const_iterator End;
        bool AtStart;
    };

    THashedDictHolder(TMemoryUsageInfo* memInfo, THashedDictFiller filler,
        const TKeyTypes& types, bool isTuple, bool eagerFill, TType* encodedType,
        const NUdf::IHash* hash, const NUdf::IEquate* equate, const THolderFactory& holderFactory)
        : TComputationValue(memInfo)
        , Filler(filler)
        , Types(types)
        , Map(0, TValueHasher(Types, isTuple, hash), TValueEqual(Types, isTuple, equate))
        , IsBuilt(false)
        , HolderFactory(holderFactory)
    {
        if (encodedType) {
            Packer.emplace(true, encodedType);
        }

        if (eagerFill)
            LazyBuildDict();
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer) {
            encodedKey = MakeString(Packer->Pack(key));
        }

        return Map.find(NUdf::TUnboxedValuePod(Packer ? encodedKey : key)) != Map.cend();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer) {
            encodedKey = MakeString(Packer->Pack(key));
        }

        const auto it = Map.find(NUdf::TUnboxedValuePod(Packer ? encodedKey : key));
        if (it == Map.cend())
            return NUdf::TUnboxedValuePod();
        return it->second.MakeOptional();
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        LazyBuildDict();
        return NUdf::TUnboxedValuePod(new TIterator<false>(this));
    }

    ui64 GetDictLength() const override {
        LazyBuildDict();
        return Map.size();
    }

    bool HasDictItems() const override {
        LazyBuildDict();
        return !Map.empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

private:
    void LazyBuildDict() const {
        if (IsBuilt)
            return;

        Filler(Map);
        Filler = THashedDictFiller();
        IsBuilt = true;
    }

private:
    mutable THashedDictFiller Filler;
    const TKeyTypes Types;
    mutable TValuesDictHashMap Map;
    mutable bool IsBuilt;
    const THolderFactory& HolderFactory;
    std::optional<TValuePacker> Packer;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedMapHolder : public TComputationValue<THashedSingleFixedMapHolder<T, OptionalKey>> {
public:
    using TMapType = TValuesDictHashSingleFixedMap<T>;

    template <bool NoSwap>
    class TIterator : public TComputationValue<TIterator<NoSwap>> {
    public:
        enum class EState {
            AtStart,
            AtNull,
            Iterator
        };
        TIterator(const THashedSingleFixedMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent(const_cast<THashedSingleFixedMapHolder*>(parent))
            , Iterator(Parent->Map.begin())
            , End(Parent->Map.end())
            , State(EState::AtStart)
        {
        }

    private:
        bool Skip() final {
            switch (State) {
            case EState::AtStart:
                State = OptionalKey && Parent->NullPayload.has_value() ? EState::AtNull : EState::Iterator;
                break;
            case EState::AtNull:
                State = EState::Iterator;
                break;
            case EState::Iterator:
                if (Iterator == End) {
                    return false;
                }
                ++Iterator;
                break;
            }

            return EState::AtNull == State || Iterator != End;
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip())
                return false;
            key = NoSwap
                ? (EState::AtNull == State ? NUdf::TUnboxedValue() : NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(Iterator->first)))
                : (EState::AtNull == State ? *Parent->NullPayload : Iterator->second);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key))
                return false;
            payload = NoSwap
                ? (EState::AtNull == State ? *Parent->NullPayload : Iterator->second)
                : (EState::AtNull == State ? NUdf::TUnboxedValue() : NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(Iterator->first)));
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedMapHolder> Parent;
        typename TMapType::const_iterator Iterator;
        typename TMapType::const_iterator End;
        EState State;
    };

    THashedSingleFixedMapHolder(TMemoryUsageInfo* memInfo, TValuesDictHashSingleFixedMap<T>&& map, std::optional<NUdf::TUnboxedValue>&& nullPayload)
        : TComputationValue<THashedSingleFixedMapHolder>(memInfo)
        , Map(std::move(map))
        , NullPayload(std::move(nullPayload))
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayload.has_value();
            }
        }
        return Map.find(key.Get<T>()) != Map.end();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayload.has_value() ? NullPayload->MakeOptional() : NUdf::TUnboxedValuePod();
            }
        }
        const auto it = Map.find(key.Get<T>());
        if (it == Map.end())
            return NUdf::TUnboxedValuePod();
        return it->second.MakeOptional();
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator<false>(this));
    }

    ui64 GetDictLength() const final {
        return Map.size() + ui64(OptionalKey && NullPayload.has_value());
    }

    bool HasDictItems() const final {
        return !Map.empty() || (OptionalKey && NullPayload.has_value());
    }

    bool IsSortedDict() const final {
        return false;
    }

    const TMapType Map;
    const std::optional<NUdf::TUnboxedValue> NullPayload;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMapHolder : public TComputationValue<THashedSingleFixedCompactMapHolder<T, OptionalKey>> {
public:
    using TMapType = TValuesDictHashSingleFixedCompactMap<T>;

    template <bool NoSwap>
    class TIterator : public TComputationValue<TIterator<NoSwap>> {
    public:
        enum class EState {
            AtStart,
            AtNull,
            Iterator
        };
        TIterator(const THashedSingleFixedCompactMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent(const_cast<THashedSingleFixedCompactMapHolder*>(parent))
            , Iterator(Parent->Map.Iterate())
            , State(EState::AtStart)
        {
        }

    private:
        bool Skip() final {
            switch (State) {
            case EState::AtStart:
                State = OptionalKey && Parent->NullPayload.has_value() ? EState::AtNull : EState::Iterator;
                break;
            case EState::AtNull:
                State = EState::Iterator;
                break;
            case EState::Iterator:
                if (Iterator.Ok())
                    ++Iterator;
                break;
            }

            return EState::AtNull == State || Iterator.Ok();
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip())
                return false;

            key = NoSwap
                ? (EState::AtNull == State
                    ? NUdf::TUnboxedValue()
                    : NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(Iterator.Get().first))
                  )
                : (EState::AtNull == State
                    ? Parent->PayloadPacker.Unpack(GetSmallValue(*Parent->NullPayload), Parent->Ctx->HolderFactory)
                    : Parent->PayloadPacker.Unpack(GetSmallValue(Iterator.Get().second), Parent->Ctx->HolderFactory)
                  );
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key))
                return false;
            payload =  NoSwap
                ? (EState::AtNull == State
                    ? Parent->PayloadPacker.Unpack(GetSmallValue(*Parent->NullPayload), Parent->Ctx->HolderFactory)
                    : Parent->PayloadPacker.Unpack(GetSmallValue(Iterator.Get().second), Parent->Ctx->HolderFactory)
                  )
                : (EState::AtNull == State
                    ? NUdf::TUnboxedValue()
                    : NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(Iterator.Get().first))
                  );
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedCompactMapHolder> Parent;
        typename TMapType::TIterator Iterator;
        EState State;
    };

    THashedSingleFixedCompactMapHolder(TMemoryUsageInfo* memInfo, TMapType&& map, std::optional<ui64>&& nullPayload, TPagedArena&& pool,
        TType* payloadType, TComputationContext* ctx)
        : TComputationValue<THashedSingleFixedCompactMapHolder>(memInfo)
        , Pool(std::move(pool))
        , Map(std::move(map))
        , NullPayload(std::move(nullPayload))
        , PayloadPacker(false, payloadType)
        , Ctx(ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayload.has_value();
            }
        }
        return Map.Has(key.Get<T>());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayload.has_value()
                    ? PayloadPacker.Unpack(GetSmallValue(*NullPayload), Ctx->HolderFactory).Release().MakeOptional()
                    : NUdf::TUnboxedValuePod();
            }
        }
        auto it = Map.Find(key.Get<T>());
        if (!it.Ok())
            return NUdf::TUnboxedValuePod();
        return PayloadPacker.Unpack(GetSmallValue(it.Get().second), Ctx->HolderFactory).Release().MakeOptional();
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        return NUdf::TUnboxedValuePod(new TIterator<false>(this));
    }

    ui64 GetDictLength() const final {
        return Map.Size() + ui64(OptionalKey && NullPayload.has_value());
    }

    bool HasDictItems() const final {
        return !Map.Empty() || (OptionalKey && NullPayload.has_value());
    }

    bool IsSortedDict() const final {
        return false;
    }

private:
    TPagedArena Pool;
    const TMapType Map;
    const std::optional<ui64> NullPayload;
    mutable TValuePacker PayloadPacker;
    TComputationContext* Ctx;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMultiMapHolder : public TComputationValue<THashedSingleFixedCompactMultiMapHolder<T, OptionalKey>> {
public:
    using TMapType = TValuesDictHashSingleFixedCompactMultiMap<T>;
    using TMapIterator = typename TMapType::TIterator;

    class TPayloadList: public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(const THashedSingleFixedCompactMultiMapHolder* parent, TMapIterator from)
                : TComputationValue<TIterator>(parent->GetMemInfo())
                , Parent(const_cast<THashedSingleFixedCompactMultiMapHolder*>(parent))
                , Iterator(from)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (!Iterator.Ok()) {
                    return false;
                }

                value = Parent->PayloadPacker.Unpack(GetSmallValue(Iterator.GetValue()), Parent->Ctx->HolderFactory);
                ++Iterator;
                return true;
            }

            bool Skip() final {
                if (!Iterator.Ok()) {
                    return false;
                }

                ++Iterator;
                return true;
            }

            const NUdf::TRefCountedPtr<THashedSingleFixedCompactMultiMapHolder> Parent;
            TMapIterator Iterator;
        };

        TPayloadList(TMemoryUsageInfo* memInfo, const THashedSingleFixedCompactMultiMapHolder* parent, TMapIterator from)
            : TCustomListValue(memInfo)
            , Parent(const_cast<THashedSingleFixedCompactMultiMapHolder*>(parent))
            , From(from)
        {
            Y_ASSERT(From.Ok());
        }

        bool HasFastListLength() const final {
            return true;
        }

        ui64 GetListLength() const final {
            if (!Length) {
                Length = Parent->Map.Count(From.GetKey());
            }

            return *Length;
        }

        bool HasListItems() const final {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const final {
            return NUdf::TUnboxedValuePod(new TIterator(Parent.Get(), From));
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedCompactMultiMapHolder> Parent;
        TMapIterator From;
    };

    class TNullPayloadList: public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(const THashedSingleFixedCompactMultiMapHolder* parent)
                : TComputationValue<TIterator>(parent->GetMemInfo())
                , Parent(const_cast<THashedSingleFixedCompactMultiMapHolder*>(parent))
                , Iterator(Parent->NullPayloads.cbegin())
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (Iterator == Parent->NullPayloads.cend()) {
                    return false;
                }

                value = Parent->PayloadPacker.Unpack(GetSmallValue(*Iterator), Parent->Ctx->HolderFactory);
                ++Iterator;
                return true;
            }

            bool Skip() final {
                if (Iterator == Parent->NullPayloads.cend()) {
                    return false;
                }

                ++Iterator;
                return true;
            }

            const NUdf::TRefCountedPtr<THashedSingleFixedCompactMultiMapHolder> Parent;
            typename std::vector<ui64>::const_iterator Iterator;
        };

        TNullPayloadList(TMemoryUsageInfo* memInfo, const THashedSingleFixedCompactMultiMapHolder* parent)
            : TCustomListValue(memInfo)
            , Parent(const_cast<THashedSingleFixedCompactMultiMapHolder*>(parent))
        {
        }

        bool HasFastListLength() const final {
            return true;
        }

        ui64 GetListLength() const final {
            if (!Length) {
                Length = Parent->NullPayloads.size();
            }

            return *Length;
        }

        bool HasListItems() const final {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const final {
            return NUdf::TUnboxedValuePod(new TIterator(Parent.Get()));
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedCompactMultiMapHolder> Parent;
    };

    template <bool NoSwap>
    class TIterator : public TComputationValue<TIterator<NoSwap>> {
    public:
        TIterator(const THashedSingleFixedCompactMultiMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent(const_cast<THashedSingleFixedCompactMultiMapHolder*>(parent))
            , Iterator(parent->Map.Iterate())
            , AtNull(OptionalKey && !parent->NullPayloads.empty())
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& key) override {
            if (AtNull) {
                AtNull = false;
                key = NoSwap
                    ? NUdf::TUnboxedValuePod()
                    : Parent->Ctx->HolderFactory.template Create<TNullPayloadList>(Parent.Get());
                return true;
            }
            if (!Iterator.Ok()) {
                return false;
            }

            key = NoSwap ?
                NUdf::TUnboxedValuePod(Iterator.GetKey()):
                Parent->Ctx->HolderFactory.template Create<TPayloadList>(Parent.Get(), Iterator.MakeCurrentKeyIter());
            Iterator.NextKey();
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (AtNull) {
                AtNull = false;
                if (NoSwap) {
                    key = NUdf::TUnboxedValuePod();
                    payload = Parent->Ctx->HolderFactory.template Create<TNullPayloadList>(Parent.Get());
                } else {
                    payload = NUdf::TUnboxedValuePod();
                    key = Parent->Ctx->HolderFactory.template Create<TNullPayloadList>(Parent.Get());
                }
                return true;
            }
            if (!Iterator.Ok()) {
                return false;
            }

            if (NoSwap) {
                key = NUdf::TUnboxedValuePod(Iterator.GetKey());
                payload = Parent->Ctx->HolderFactory.template Create<TPayloadList>(Parent.Get(), Iterator.MakeCurrentKeyIter());
            } else {
                payload = NUdf::TUnboxedValuePod(Iterator.GetKey());
                key = Parent->Ctx->HolderFactory.template Create<TPayloadList>(Parent.Get(), Iterator.MakeCurrentKeyIter());
            }
            Iterator.NextKey();
            return true;
        }

        bool Skip() override {
            if (AtNull) {
                AtNull = false;
                return true;
            }
            if (!Iterator.Ok()) {
                return false;
            }

            Iterator.NextKey();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedCompactMultiMapHolder> Parent;
        TMapIterator Iterator;
        bool AtNull;
    };

    THashedSingleFixedCompactMultiMapHolder(TMemoryUsageInfo* memInfo, TMapType&& map, std::vector<ui64>&& nullPayloads, TPagedArena&& pool,
        TType* payloadType, TComputationContext* ctx)
        : TComputationValue<THashedSingleFixedCompactMultiMapHolder>(memInfo)
        , Pool(std::move(pool))
        , Map(std::move(map))
        , NullPayloads(std::move(nullPayloads))
        , PayloadPacker(false, payloadType)
        , Ctx(ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        if constexpr (OptionalKey) {
            if (!key) {
                return !NullPayloads.empty();
            }
        }
        return Map.Has(key.Get<T>());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayloads.empty()
                    ? NUdf::TUnboxedValuePod()
                    : Ctx->HolderFactory.Create<TNullPayloadList>(this);
            }
        }
        const auto it = Map.Find(key.Get<T>());
        if (!it.Ok())
            return NUdf::TUnboxedValuePod();
        return Ctx->HolderFactory.Create<TPayloadList>(this, it);
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<true>(this));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator<false>(this));
    }

    ui64 GetDictLength() const override {
        return Map.UniqSize() + ui64(OptionalKey && !NullPayloads.empty());
    }

    bool HasDictItems() const override {
        return !Map.Empty() || (OptionalKey && !NullPayloads.empty());
    }

    bool IsSortedDict() const override {
        return false;
    }

private:
    TPagedArena Pool;
    const TMapType Map;
    const std::vector<ui64> NullPayloads;
    mutable TValuePacker PayloadPacker;
    TComputationContext* Ctx;
};

class TVariantHolder : public TComputationValue<TVariantHolder> {
public:
    TVariantHolder(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& item, ui32 index)
        : TComputationValue(memInfo)
        , Item(std::move(item))
        , Index(index)
    {
    }

private:
    NUdf::TUnboxedValue GetVariantItem() const override {
        return Item;
    }

    ui32 GetVariantIndex() const override {
        return Index;
    }

    const NUdf::TUnboxedValue Item;
    const ui32 Index;
};

class TListIteratorHolder : public TComputationValue<TListIteratorHolder> {
public:
    TListIteratorHolder(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& list)
        : TComputationValue(memInfo)
        , List(std::move(list))
        , Iter(List.GetListIterator())
    {}

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        return Iter.Next(result) ? NUdf::EFetchStatus::Ok : NUdf::EFetchStatus::Finish;
    }

    const NUdf::TUnboxedValue List;
    const NUdf::TUnboxedValue Iter;
};

class TLimitedList: public TComputationValue<TLimitedList> {
public:
    class TIterator: public TComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& iter, TMaybe<ui64> skip, TMaybe<ui64> take)
            : TComputationValue(memInfo)
            , Iter(std::move(iter))
            , Skip_(skip)
            , Take_(take)
            , Index(Max<ui64>())
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& value) override {
            if (!Iter) {
                return false;
            }

            if (Skip_) {
                while ((Index + 1) < Skip_.GetRef()) {
                    if (!Iter.Skip()) {
                        Iter = NUdf::TUnboxedValue();
                        return false;
                    }

                    ++Index;
                }
            }

            if (Take_ && ((Index + 1) - Skip_.GetOrElse(0)) >= Take_.GetRef()) {
                Iter = NUdf::TUnboxedValue();
                return false;
            }

            if (!Iter.Next(value)) {
                Iter = NUdf::TUnboxedValue();
                return false;
            }

            ++Index;
            return true;
        }

        bool Skip() override {
            if (!Iter) {
                return false;
            }

            if (Skip_) {
                while ((Index + 1) < Skip_.GetRef()) {
                    if (!Iter.Skip()) {
                        Iter = NUdf::TUnboxedValue();
                        return false;
                    }

                    ++Index;
                }
            }

            if (Take_ && ((Index + 1) - Skip_.GetOrElse(0)) >= Take_.GetRef()) {
                Iter = NUdf::TUnboxedValue();
                return false;
            }

            if (!Iter.Skip()) {
                Iter = NUdf::TUnboxedValue();
                return false;
            }

            ++Index;
            return true;
        }

        NUdf::TUnboxedValue Iter;
        const TMaybe<ui64> Skip_;
        const TMaybe<ui64> Take_;
        ui64 Index;
    };

    TLimitedList(TMemoryUsageInfo* memInfo, NUdf::TRefCountedPtr<NUdf::IBoxedValue> parent, TMaybe<ui64> skip, TMaybe<ui64> take)
        : TComputationValue(memInfo)
        , Parent(parent)
        , Skip(skip)
        , Take(take)
    {
    }

private:
    bool HasFastListLength() const override {
        return Length.Defined();
    }

    ui64 GetListLength() const override {
        if (!Length) {
            ui64 length = NUdf::TBoxedValueAccessor::GetListLength(*Parent);
            if (Skip) {
                if (Skip.GetRef() >= length) {
                    length = 0;
                } else {
                    length -= Skip.GetRef();
                }
            }

            if (Take) {
                length = Min(length, Take.GetRef());
            }

            Length = length;
        }

        return Length.GetRef();
    }

    ui64 GetEstimatedListLength() const override {
        return GetListLength();
    }

    bool HasListItems() const override {
        if (HasItems) {
            return *HasItems;
        }

        if (Length) {
            HasItems = (*Length != 0);
            return *HasItems;
        }

        HasItems = GetListIterator().Skip();
        return *HasItems;
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), NUdf::TBoxedValueAccessor::GetListIterator(*Parent), Skip, Take));
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        if (!count) {
            return const_cast<TLimitedList*>(this);
        }

        if (Length) {
            if (count >= Length.GetRef()) {
                return builder.NewEmptyList().Release().AsBoxed();
            }
        }

        ui64 prevSkip = Skip.GetOrElse(0);
        if (count > Max<ui64>() - prevSkip) {
            return builder.NewEmptyList().Release().AsBoxed();
        }

        const ui64 newSkip = prevSkip + count;
        TMaybe<ui64> newTake = Take;
        if (newTake) {
            if (count >= newTake.GetRef()) {
                return builder.NewEmptyList().Release().AsBoxed();
            }

            newTake = newTake.GetRef() - count;
        }

        return new TLimitedList(GetMemInfo(), Parent, newSkip, newTake);
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        if (!count) {
            return builder.NewEmptyList().Release().AsBoxed();
        }

        if (Length) {
            if (count >= Length.GetRef()) {
                return const_cast<TLimitedList*>(this);
            }
        }

        TMaybe<ui64> newTake = Take;
        if (newTake) {
            newTake = Min(count, newTake.GetRef());
        } else {
            newTake = count;
        }

        return new TLimitedList(GetMemInfo(), Parent, Skip, newTake);
    }

    NUdf::TRefCountedPtr<NUdf::IBoxedValue> Parent;
    TMaybe<ui64> Skip;
    TMaybe<ui64> Take;
    mutable TMaybe<ui64> Length;
    mutable TMaybe<bool> HasItems;
};

class TLazyListDecorator : public TComputationValue<TLazyListDecorator> {
public:
    TLazyListDecorator(TMemoryUsageInfo* memInfo, NUdf::IBoxedValuePtr&& list)
        : TComputationValue(memInfo), List(std::move(list))
    {}

private:
    bool HasListItems() const final {
        return NUdf::TBoxedValueAccessor::HasListItems(*List);
    }

    bool HasDictItems() const final {
        return NUdf::TBoxedValueAccessor::HasDictItems(*List);
    }

    bool HasFastListLength() const final {
        return NUdf::TBoxedValueAccessor::HasFastListLength(*List);
    }

    ui64 GetListLength() const final {
        return NUdf::TBoxedValueAccessor::GetListLength(*List);
    }

    ui64 GetDictLength() const final {
        return NUdf::TBoxedValueAccessor::GetDictLength(*List);
    }

    ui64 GetEstimatedListLength() const final {
        return NUdf::TBoxedValueAccessor::GetEstimatedListLength(*List);
    }

    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TBoxedValueAccessor::GetListIterator(*List);
    }

    NUdf::TUnboxedValue GetDictIterator() const final {
        return NUdf::TBoxedValueAccessor::GetDictIterator(*List);
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        return NUdf::TBoxedValueAccessor::GetPayloadsIterator(*List);
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        return NUdf::TBoxedValueAccessor::GetKeysIterator(*List);
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const final {
        return NUdf::TBoxedValueAccessor::ReverseListImpl(*List, builder);
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        return NUdf::TBoxedValueAccessor::SkipListImpl(*List, builder, count);
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        return NUdf::TBoxedValueAccessor::TakeListImpl(*List, builder, count);
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const final {
        return NUdf::TBoxedValueAccessor::ToIndexDictImpl(*List, builder);
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        return NUdf::TBoxedValueAccessor::Contains(*List, key);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        return NUdf::TBoxedValueAccessor::Lookup(*List, key);
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const final {
        return NUdf::TBoxedValueAccessor::GetElement(*List, index);
    }

    const NUdf::TUnboxedValue* GetElements() const final {
        return nullptr;
    }

    bool IsSortedDict() const final {
        return NUdf::TBoxedValueAccessor::IsSortedDict(*List);
    }

    const NUdf::IBoxedValuePtr List;
};

} // namespace

///////////////////////////////////////////////////////////////////////////////
// TDictValueBuilder
///////////////////////////////////////////////////////////////////////////////
class TDictValueBuilder: public NUdf::IDictValueBuilder
{
public:
    TDictValueBuilder(
            const THolderFactory& holderFactory,
            const TKeyTypes& types,
            bool isTuple,
            ui32 dictFlags,
            TType* encodeType,
            const NUdf::IHash* hash,
            const NUdf::IEquate* equate,
            const NUdf::ICompare* compare)
        : HolderFactory_(holderFactory)
        , Types_(types)
        , IsTuple_(isTuple)
        , DictFlags_(dictFlags)
        , EncodeType_(encodeType)
        , Hash_(hash)
        , Equate_(equate)
        , Compare_(compare)
    {
        Items_.reserve(10);
    }

    NUdf::IDictValueBuilder& Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& value) override
    {
        Items_.emplace_back(std::move(key), std::move(value));
        return *this;
    }

    NUdf::TUnboxedValue Build() override {
        if (Items_.empty())
            return HolderFactory_.GetEmptyContainerLazy();

        if (DictFlags_ & NUdf::TDictFlags::Hashed) {
            auto prepareFn = (DictFlags_ & NUdf::TDictFlags::Multi)
                    ? &TDictValueBuilder::PrepareMultiHasedDict
                    : &TDictValueBuilder::PrepareHasedDict;
            THashedDictFiller filler(std::bind(prepareFn, this, std::placeholders::_1));

            return HolderFactory_.CreateDirectHashedDictHolder(
                        filler, Types_, IsTuple_, true, EncodeType_, Hash_, Equate_);
        }
        else {
            auto prepareFn = (DictFlags_ & NUdf::TDictFlags::Multi)
                    ? &TDictValueBuilder::PrepareMultiSortedDict
                    : &TDictValueBuilder::PrepareSortedDict;
            TSortedDictFiller filler(std::bind(prepareFn, this, std::placeholders::_1));

            EDictSortMode mode = (DictFlags_ & NUdf::TDictFlags::Multi)
                    ? EDictSortMode::SortedUniqueAscending
                    : EDictSortMode::RequiresSorting;

            return HolderFactory_.CreateDirectSortedDictHolder(filler, Types_, IsTuple_, mode, true,
                EncodeType_, Compare_, Equate_);
        }
    }

private:
    void PrepareMultiHasedDict(TValuesDictHashMap& map) {
        TKeyPayloadPairVector localValues;
        localValues.swap(Items_);
        map.clear();
        std::optional<TValuePacker> packer;
        if (EncodeType_) {
            packer.emplace(true, EncodeType_);
        }

        for (auto& value : localValues) {
            auto key = value.first;
            if (packer) {
                key = MakeString(packer->Pack(key));
            }

            auto it = map.find(key);
            if (it == map.end()) {
                TDefaultListRepresentation emptyList;
                auto newList = HolderFactory_.CreateDirectListHolder(
                    emptyList.Append(std::move(value.second)));
                map.emplace(std::move(key), std::move(newList));
            } else {
                auto prevList = GetDefaultListRepresentation(it->second);
                auto newList = HolderFactory_.CreateDirectListHolder(
                    prevList->Append(std::move(value.second)));
                it->second = std::move(newList);
            }
        }
    }

    void PrepareHasedDict(TValuesDictHashMap& map) {
        TKeyPayloadPairVector localValues;
        localValues.swap(Items_);
        map.clear();
        std::optional<TValuePacker> packer;
        if (EncodeType_) {
            packer.emplace(true, EncodeType_);
        }

        for (auto& value : localValues) {
            auto key = value.first;
            if (packer) {
                key = MakeString(packer->Pack(key));
            }

            map.emplace(std::move(key), std::move(value.second));
        }
    }

    void PrepareMultiSortedDict(TKeyPayloadPairVector& values) {
        TKeyPayloadPairVector localValues;
        localValues.swap(Items_);
        std::optional<TGenericPresortEncoder> packer;
        if (EncodeType_) {
            packer.emplace(EncodeType_);
            for (auto& x : localValues) {
                x.first = MakeString(packer->Encode(x.first, false));
            }
        }

        StableSort(localValues.begin(), localValues.end(), TKeyPayloadPairLess(Types_, IsTuple_, Compare_));

        TKeyPayloadPairVector groups;
        groups.reserve(localValues.size());
        if (!localValues.empty()) {
            TDefaultListRepresentation currentList(std::move(localValues.begin()->second));
            auto lastKey = std::move(localValues.begin()->first);
            TValueEqual eqPredicate(Types_, IsTuple_, Equate_);
            for (auto it = localValues.begin() + 1; it != localValues.end(); ++it) {
                if (eqPredicate(lastKey, it->first)) {
                    currentList = currentList.Append(std::move(it->second));
                } else {
                    auto payload = HolderFactory_.CreateDirectListHolder(std::move(currentList));
                    groups.emplace_back(std::move(lastKey), std::move(payload));
                    currentList = TDefaultListRepresentation(std::move(it->second));
                    lastKey = std::move(it->first);
                }
            }

            auto payload = HolderFactory_.CreateDirectListHolder(std::move(currentList));
            groups.emplace_back(std::move(lastKey), std::move(payload));
        }

        values.swap(groups);
    }

    void PrepareSortedDict(TKeyPayloadPairVector& values) {
        Items_.swap(values);
        std::optional<TGenericPresortEncoder> packer;
        if (EncodeType_) {
            packer.emplace(EncodeType_);
            for (auto& x : values) {
                x.first = MakeString(packer->Encode(x.first, false));
            }
        }
    }

private:
    const THolderFactory& HolderFactory_;
    const TKeyTypes Types_;
    const bool IsTuple_;
    const ui32 DictFlags_;
    TType* const EncodeType_;
    const NUdf::IHash* Hash_;
    const NUdf::IEquate* Equate_;
    const NUdf::ICompare* Compare_;
    TKeyPayloadPairVector Items_;
};

//////////////////////////////////////////////////////////////////////////////
// THolderFactory
//////////////////////////////////////////////////////////////////////////////
THolderFactory::THolderFactory(
    TAllocState& allocState,
    TMemoryUsageInfo& memInfo,
    const IFunctionRegistry* functionRegistry)
    : CurrentAllocState(&allocState)
    , MemInfo(memInfo)
    , FunctionRegistry(functionRegistry)
{
}

THolderFactory::~THolderFactory() {
    if (EmptyContainer) {
        CurrentAllocState->UnlockObject(*EmptyContainer);
    }
}

NUdf::TUnboxedValuePod THolderFactory::GetEmptyContainerLazy() const {
    if (!EmptyContainer) {
        EmptyContainer.ConstructInPlace(
            NUdf::TUnboxedValuePod(AllocateOn<TEmptyContainerHolder>(CurrentAllocState, &MemInfo)));
        CurrentAllocState->LockObject(*EmptyContainer);
    }
    return *EmptyContainer;
}

NUdf::TUnboxedValuePod THolderFactory::CreateTypeHolder(TType* type) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TTypeHolder>(CurrentAllocState, &MemInfo, type));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectListHolder(TDefaultListRepresentation&& items) const{
    if (!items.GetLength())
        return GetEmptyContainerLazy();

    return NUdf::TUnboxedValuePod(AllocateOn<TDirectListHolder>(CurrentAllocState, &MemInfo, std::move(items)));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectArrayHolder(ui64 size, NUdf::TUnboxedValue*& itemsPtr) const {
    if (!size) {
        itemsPtr = nullptr;
        return GetEmptyContainerLazy();
    }

    const auto buffer = MKQLAllocFastWithSize(
        sizeof(TDirectArrayHolderInplace) + size * sizeof(NUdf::TUnboxedValue), CurrentAllocState, EMemorySubPool::Default);
    const auto h = ::new(buffer) TDirectArrayHolderInplace(&MemInfo, size);

    auto res = NUdf::TUnboxedValuePod(h);
    itemsPtr = h->GetPtr();
    return res;
}

NUdf::TUnboxedValuePod THolderFactory::CreateArrowBlock(arrow::Datum&& datum) const {
    return Create<TArrowBlock>(std::move(datum));
}

NUdf::TUnboxedValuePod THolderFactory::VectorAsArray(TUnboxedValueVector& values) const {
    if (values.empty())
        return GetEmptyContainerLazy();

    NUdf::TUnboxedValue* itemsPtr = nullptr;
    auto tuple = CreateDirectArrayHolder(values.size(), itemsPtr);
    for (auto& value : values) {
        *itemsPtr++ = std::move(value);
    }

    return tuple;
}

NUdf::TUnboxedValuePod THolderFactory::NewVectorHolder() const {
    return NUdf::TUnboxedValuePod(new TVectorHolder(&MemInfo));
}

NUdf::TUnboxedValuePod THolderFactory::NewTemporaryVectorHolder() const {
    return NUdf::TUnboxedValuePod(new TTemporaryVectorHolder(&MemInfo));
}

const NUdf::IHash* THolderFactory::GetHash(const TType& type, bool useIHash) const {
    return useIHash ? HashRegistry.FindOrEmplace(type) : nullptr;
}

const NUdf::IEquate* THolderFactory::GetEquate(const TType& type, bool useIHash) const {
    return useIHash ? EquateRegistry.FindOrEmplace(type) : nullptr;
}

const NUdf::ICompare* THolderFactory::GetCompare(const TType& type, bool useIHash) const {
    return useIHash ? CompareRegistry.FindOrEmplace(type) : nullptr;
}

NUdf::TUnboxedValuePod THolderFactory::VectorAsVectorHolder(TUnboxedValueVector&& list) const {
    return NUdf::TUnboxedValuePod(new TVectorHolder(&MemInfo, std::move(list)));
}

NUdf::TUnboxedValuePod THolderFactory::CloneArray(const NUdf::TUnboxedValuePod list, NUdf::TUnboxedValue*& items) const {
    if (const auto size = list.GetListLength()) {
        const auto ptr = list.GetElements();
        if (ptr && list.UniqueBoxed()) {
            items = const_cast<NUdf::TUnboxedValue*>(ptr);
            return list;
        } else {
            const auto array = CreateDirectArrayHolder(size, items);
            if (ptr) {
                std::copy(ptr, ptr + size, items);
            } else if (const auto& it = list.GetListIterator()) {
                for (auto out = items; it.Next(*out++);)
                    continue;
            }
            list.DeleteUnreferenced();
            return array;
        }
    } else {
        items = nullptr;
        return GetEmptyContainerLazy();
    }
}

NUdf::TUnboxedValuePod THolderFactory::Cloned(const NUdf::TUnboxedValuePod& it) const
{
    TDefaultListRepresentation result;
    for (NUdf::TUnboxedValue item; it.Next(item);) {
        result = result.Append(std::move(item));
    }

    return CreateDirectListHolder(std::move(result));
}

NUdf::TUnboxedValuePod THolderFactory::Reversed(const NUdf::TUnboxedValuePod& it) const
{
    TDefaultListRepresentation result;
    for (NUdf::TUnboxedValue item; it.Next(item);) {
        result = result.Prepend(std::move(item));
    }

    return CreateDirectListHolder(std::move(result));
}

NUdf::TUnboxedValuePod THolderFactory::CreateLimitedList(
        NUdf::IBoxedValuePtr&& parent,
        TMaybe<ui64> skip, TMaybe<ui64> take,
        TMaybe<ui64> knownLength) const
{
    if (take && !take.GetRef()) {
        return GetEmptyContainerLazy();
    }

    if (skip && !skip.GetRef()) {
        skip = TMaybe<ui64>();
    }

    if (knownLength && skip) {
        if (skip.GetRef() >= knownLength.GetRef()) {
            return GetEmptyContainerLazy();
        }
    }

    if (knownLength && take) {
        if (take.GetRef() >= knownLength.GetRef() - skip.GetOrElse(0)) {
            take = TMaybe<ui64>();
        }
    }

    if (!skip && !take) {
        return NUdf::TUnboxedValuePod(std::move(parent));
    }

    return NUdf::TUnboxedValuePod(AllocateOn<TLimitedList>(CurrentAllocState, &MemInfo, std::move(parent), skip, take));
}

NUdf::TUnboxedValuePod THolderFactory::ReverseList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list) const
{
    auto boxed = list.AsBoxed();
    if (auto res = NUdf::TBoxedValueAccessor::ReverseListImpl(*boxed, *builder)) {
        return NUdf::TUnboxedValuePod(std::move(boxed = std::move(res)));
    }

    return Reversed(list.GetListIterator());
}

NUdf::TUnboxedValuePod THolderFactory::SkipList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list, ui64 count) const
{
    auto boxed = list.AsBoxed();
    if (auto res = NUdf::TBoxedValueAccessor::SkipListImpl(*boxed, *builder, count)) {
        return NUdf::TUnboxedValuePod(std::move(boxed = std::move(res)));
    }

    TMaybe<ui64> knownLength;
    if (list.HasFastListLength()) {
        knownLength = list.GetListLength();
    }

    return CreateLimitedList(std::move(boxed), count, TMaybe<ui64>(), knownLength);
}

NUdf::TUnboxedValuePod THolderFactory::TakeList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list, ui64 count) const
{
    auto boxed = list.AsBoxed();
    if (auto res = NUdf::TBoxedValueAccessor::TakeListImpl(*boxed, *builder, count)) {
        return NUdf::TUnboxedValuePod(std::move(boxed = std::move(res)));
    }

    TMaybe<ui64> knownLength;
    if (list.HasFastListLength()) {
        knownLength = list.GetListLength();
    }

    return CreateLimitedList(std::move(boxed), TMaybe<ui64>(), count, knownLength);
}

NUdf::TUnboxedValuePod THolderFactory::ToIndexDict(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list) const
{
    auto boxed = list.AsBoxed();
    if (auto res = NUdf::TBoxedValueAccessor::ToIndexDictImpl(*boxed, *builder)) {
        return NUdf::TUnboxedValuePod(std::move(boxed = std::move(res)));
    }

    return Cloned(list.GetListIterator());
}

template<bool IsStream>
NUdf::TUnboxedValuePod THolderFactory::Collect(NUdf::TUnboxedValuePod list) const {
    const auto boxed = list.AsBoxed(); // Only for release on exit.
    if (!IsStream && list.HasFastListLength()) {
        auto size = list.GetListLength();
        NUdf::TUnboxedValue* items = nullptr;
        const auto result = CreateDirectArrayHolder(size, items);

        TThresher<IsStream>::DoForEachItem(list,
            [&items] (NUdf::TUnboxedValue&& item) {
                *items++ = std::move(item);
            }
        );
        return result;
    } else {
        TDefaultListRepresentation res;

        TThresher<IsStream>::DoForEachItem(list,
            [&res] (NUdf::TUnboxedValue&& item) {
                res = res.Append(std::move(item));
            }
        );

        return CreateDirectListHolder(std::move(res));
    }
}

template NUdf::TUnboxedValuePod THolderFactory::Collect<true>(NUdf::TUnboxedValuePod list) const;
template NUdf::TUnboxedValuePod THolderFactory::Collect<false>(NUdf::TUnboxedValuePod list) const;

NUdf::TUnboxedValuePod THolderFactory::LazyList(NUdf::TUnboxedValuePod list) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TLazyListDecorator>(CurrentAllocState, &MemInfo, list.AsBoxed()));;
}

NUdf::TUnboxedValuePod THolderFactory::Append(NUdf::TUnboxedValuePod list, NUdf::TUnboxedValuePod last) const {
    const auto boxed = list.AsBoxed();
    TDefaultListRepresentation resList;
    if (const auto leftRepr = reinterpret_cast<const TDefaultListRepresentation*>(NUdf::TBoxedValueAccessor::GetListRepresentation(*boxed))) {
        resList = std::move(*leftRepr);
    } else {
        TThresher<false>::DoForEachItem(list,
            [&resList] (NUdf::TUnboxedValue&& item) {
                resList = resList.Append(std::move(item));
            }
        );
    }

    resList = resList.Append(std::move(last));
    return CreateDirectListHolder(std::move(resList));
}

NUdf::TUnboxedValuePod THolderFactory::Prepend(NUdf::TUnboxedValuePod first, NUdf::TUnboxedValuePod list) const {
    const auto boxed = list.AsBoxed();
    TDefaultListRepresentation resList;
    if (const auto rightRepr = reinterpret_cast<const TDefaultListRepresentation*>(NUdf::TBoxedValueAccessor::GetListRepresentation(*boxed))) {
        resList = *rightRepr;
    } else {
        TThresher<false>::DoForEachItem(list,
            [&resList] (NUdf::TUnboxedValue&& item) {
                resList = resList.Append(std::move(item));
            }
        );
    }

    resList = resList.Prepend(std::move(first));
    return CreateDirectListHolder(std::move(resList));
}

NUdf::TUnboxedValuePod THolderFactory::ExtendStream(NUdf::TUnboxedValue* data, ui64 size) const {
    if (!data || !size) {
        return GetEmptyContainerLazy();
    }

    TUnboxedValueVector values(size);
    std::move(data, data + size, values.begin());
    return Create<TExtendStreamValue>(std::move(values));
}

template<>
NUdf::TUnboxedValuePod THolderFactory::ExtendList<true>(NUdf::TUnboxedValue* data, ui64 size) const {
    if (!data || !size) {
        return GetEmptyContainerLazy();
    }

    TUnboxedValueVector values;
    values.reserve(size);
    std::transform(data, data + size, std::back_inserter(values), [this](NUdf::TUnboxedValue& stream){ return Create<TForwardListValue>(std::move(stream)); });
    return Create<TExtendListValue>(std::move(values));
}

template<>
NUdf::TUnboxedValuePod THolderFactory::ExtendList<false>(NUdf::TUnboxedValue* data, ui64 size) const {
    if (!data || !size) {
        return GetEmptyContainerLazy();
    }

    using TElementsAndSize = std::tuple<const NUdf::TUnboxedValuePod*, ui64, ui64>;
    TSmallVec<TElementsAndSize, TMKQLAllocator<TElementsAndSize>> elements;
    elements.reserve(size);

    for (ui64 i = 0ULL; i < size; ++i) {
        if (const auto ptr = data[i].GetElements()) {
            if (const auto length = data[i].GetListLength()) {
                elements.emplace_back(ptr, length, i);
            }
        } else {
            TUnboxedValueVector values(size);
            std::move(data, data + size, values.begin());
            return Create<TExtendListValue>(std::move(values));
        }
    }

    const auto total = std::accumulate(elements.cbegin(), elements.cend(), 0ULL, [](ui64 s, TElementsAndSize i) { return s + std::get<1U>(i); });

    if (!total) {
        std::fill_n(data, size, NUdf::TUnboxedValue());
        return GetEmptyContainerLazy();
    }

    if (1U == elements.size()) {
        const auto result = data[std::get<2U>(elements.front())].Release();
        std::fill_n(data, size, NUdf::TUnboxedValue());
        return result;
    }

    auto it = elements.cbegin();
    if (const auto first = GetDefaultListRepresentation(data[std::get<2U>(*it++)])) {
        TDefaultListRepresentation list(*first);
        while (elements.cend() != it) {
            const auto& e = *it++;
            if (const auto repr = GetDefaultListRepresentation(data[std::get<2U>(e)])) {
                list = list.Extend(*repr);
            } else {
                std::for_each(std::get<0U>(e), std::get<0U>(e) + std::get<1U>(e),
                    [&](NUdf::TUnboxedValue item) {
                        list = list.Append(std::move(item));
                    }
                );
            }
        }
        std::fill_n(data, size, NUdf::TUnboxedValue());
        return CreateDirectListHolder(std::move(list));
    } else {
        NUdf::TUnboxedValue *items = nullptr;
        const auto result = CreateDirectArrayHolder(total, items);
        for (const auto& i : elements) {
            std::copy_n(std::get<0U>(i), std::get<1U>(i), items);
            items += std::get<1U>(i);
        }
        std::fill_n(data, size, NUdf::TUnboxedValue());
        return result;
    }
}

NUdf::TUnboxedValuePod THolderFactory::CreateVariantHolder(NUdf::TUnboxedValuePod item, ui32 index) const {
    if (item.TryMakeVariant(index))
        return item;

    return CreateBoxedVariantHolder(std::move(item), index);
}

NUdf::TUnboxedValuePod THolderFactory::CreateBoxedVariantHolder(NUdf::TUnboxedValuePod item, ui32 index) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TVariantHolder>(CurrentAllocState, &MemInfo, std::move(item), index));
}

NUdf::TUnboxedValuePod THolderFactory::CreateIteratorOverList(NUdf::TUnboxedValuePod list) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TListIteratorHolder>(CurrentAllocState, &MemInfo, list));
}

NUdf::TUnboxedValuePod THolderFactory::CreateForwardList(NUdf::TUnboxedValuePod stream) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TForwardListValue>(CurrentAllocState, &MemInfo, stream));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectSortedSetHolder(
        TSortedSetFiller filler,
        const TKeyTypes& types,
        bool isTuple,
        EDictSortMode mode,
        bool eagerFill,
        TType* encodedType,
        const NUdf::ICompare* compare,
        const NUdf::IEquate* equate) const
{
    return NUdf::TUnboxedValuePod(AllocateOn<TSortedSetHolder>(CurrentAllocState, &MemInfo,
        filler, types, isTuple, mode, eagerFill, encodedType, compare, equate, *this));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectSortedDictHolder(
        TSortedDictFiller filler,
        const TKeyTypes& types,
        bool isTuple,
        EDictSortMode mode,
        bool eagerFill,
        TType* encodedType,
        const NUdf::ICompare* compare,
        const NUdf::IEquate* equate) const
{
    return NUdf::TUnboxedValuePod(AllocateOn<TSortedDictHolder>(CurrentAllocState, &MemInfo,
        filler, types, isTuple, mode, eagerFill, encodedType, compare, equate, *this));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedDictHolder(
        THashedDictFiller filler,
        const TKeyTypes& types,
        bool isTuple,
        bool eagerFill,
        TType* encodedType,
        const NUdf::IHash* hash,
        const NUdf::IEquate* equate) const
{
    return NUdf::TUnboxedValuePod(AllocateOn<THashedDictHolder>(CurrentAllocState, &MemInfo,
        filler, types, isTuple, eagerFill, encodedType, hash, equate, *this));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSetHolder(
    THashedSetFiller filler,
    const TKeyTypes& types,
    bool isTuple,
    bool eagerFill,
    TType* encodedType,
    const NUdf::IHash* hash,
    const NUdf::IEquate* equate) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSetHolder>(CurrentAllocState, &MemInfo,
        filler, types, isTuple, eagerFill, encodedType, hash, equate, *this));
}

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedSetHolder(
    TValuesDictHashSingleFixedSet<T>&& set, bool hasNull) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedSetHolder<T, OptionalKey>>(CurrentAllocState, &MemInfo, std::move(set), hasNull));
}

#define DEFINE_HASHED_SINGLE_FIXED_SET_OPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedSetHolder<xType, true> \
    (TValuesDictHashSingleFixedSet<xType>&& set, bool hasNull) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_SET_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_SET_OPT

#define DEFINE_HASHED_SINGLE_FIXED_SET_NONOPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedSetHolder<xType, false> \
    (TValuesDictHashSingleFixedSet<xType>&& set, bool hasNull) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_SET_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_SET_NONOPT

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactSetHolder(
    TValuesDictHashSingleFixedCompactSet<T>&& set, bool hasNull) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedCompactSetHolder<T, OptionalKey>>(CurrentAllocState, &MemInfo, std::move(set), hasNull));
}

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_OPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactSetHolder<xType, true> \
    (TValuesDictHashSingleFixedCompactSet<xType>&& set, bool hasNull) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_OPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_NONOPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactSetHolder<xType, false> \
    (TValuesDictHashSingleFixedCompactSet<xType>&& set, bool hasNull) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_NONOPT

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedMapHolder(
    TValuesDictHashSingleFixedMap<T>&& map, std::optional<NUdf::TUnboxedValue>&& nullPayload) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedMapHolder<T, OptionalKey>>(CurrentAllocState, &MemInfo, std::move(map), std::move(nullPayload)));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedCompactSetHolder(
    TValuesDictHashCompactSet&& set, TPagedArena&& pool, TType* keyType, TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedCompactSetHolder>(CurrentAllocState, &MemInfo, std::move(set), std::move(pool), keyType, ctx));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedCompactMapHolder(
    TValuesDictHashCompactMap&& map, TPagedArena&& pool, TType* keyType, TType* payloadType,
    TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedCompactMapHolder>(CurrentAllocState, &MemInfo, std::move(map), std::move(pool), keyType, payloadType, ctx));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedCompactMultiMapHolder(
    TValuesDictHashCompactMultiMap&& map, TPagedArena&& pool, TType* keyType, TType* payloadType,
    TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedCompactMultiMapHolder>(CurrentAllocState, &MemInfo, std::move(map), std::move(pool), keyType, payloadType, ctx));
}

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMapHolder(
    TValuesDictHashSingleFixedCompactMap<T>&& map, std::optional<ui64>&& nullPayload, TPagedArena&& pool, TType* payloadType,
    TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedCompactMapHolder<T, OptionalKey>>(CurrentAllocState, &MemInfo,
        std::move(map), std::move(nullPayload), std::move(pool), payloadType, ctx));
}

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMultiMapHolder(
    TValuesDictHashSingleFixedCompactMultiMap<T>&& map, std::vector<ui64>&& nullPayloads, TPagedArena&& pool, TType* payloadType,
    TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedCompactMultiMapHolder<T, OptionalKey>>(CurrentAllocState, &MemInfo,
        std::move(map), std::move(nullPayloads), std::move(pool), payloadType, ctx));
}

NUdf::IDictValueBuilder::TPtr THolderFactory::NewDict(
        const NUdf::TType* dictType,
        ui32 flags) const
{
    TType* type = const_cast<TType*>(static_cast<const TType*>(dictType));
    TType* keyType = AS_TYPE(TDictType, type)->GetKeyType();
    TKeyTypes types;
    bool encoded;
    bool isTuple;
    bool useIHash;
    GetDictionaryKeyTypes(keyType, types, isTuple, encoded, useIHash);
    return new TDictValueBuilder(*this, types, isTuple, flags, encoded ? keyType : nullptr,
        GetHash(*keyType, useIHash), GetEquate(*keyType, useIHash),
        GetCompare(*keyType, useIHash));
}

#define DEFINE_HASHED_SINGLE_FIXED_MAP_OPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedMapHolder<xType, true> \
    (TValuesDictHashSingleFixedMap<xType>&& map, std::optional<NUdf::TUnboxedValue>&& nullPayload) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_MAP_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_MAP_OPT

#define DEFINE_HASHED_SINGLE_FIXED_MAP_NONOPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedMapHolder<xType, false> \
    (TValuesDictHashSingleFixedMap<xType>&& map, std::optional<NUdf::TUnboxedValue>&& nullPayload) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_MAP_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_MAP_NONOPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_OPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMapHolder<xType, true> \
    (TValuesDictHashSingleFixedCompactMap<xType>&& map, std::optional<ui64>&& nullPayload, TPagedArena&& pool, TType* payloadType, \
    TComputationContext* ctx) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_OPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_NONOPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMapHolder<xType, false> \
    (TValuesDictHashSingleFixedCompactMap<xType>&& map, std::optional<ui64>&& nullPayload, TPagedArena&& pool, TType* payloadType, \
    TComputationContext* ctx) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_NONOPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_OPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMultiMapHolder<xType, true> \
    (TValuesDictHashSingleFixedCompactMultiMap<xType>&& map, std::vector<ui64>&& nullPayloads, TPagedArena&& pool, TType* payloadType, \
    TComputationContext* ctx) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_OPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_NONOPT(xType) \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMultiMapHolder<xType, false> \
    (TValuesDictHashSingleFixedCompactMultiMap<xType>&& map, std::vector<ui64>&& nullPayloads, TPagedArena&& pool, TType* payloadType, \
    TComputationContext* ctx) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_NONOPT

void GetDictionaryKeyTypes(const TType* keyType, TKeyTypes& types, bool& isTuple, bool& encoded, bool& useIHash, bool expandTuple) {
    isTuple = false;
    encoded = false;
    useIHash = false;
    types.clear();
    if (!keyType->IsPresortSupported()) {
        useIHash = true;
        return;
    }

    const bool isOptional = keyType->IsOptional();
    if (isOptional) {
        keyType = AS_TYPE(TOptionalType, keyType)->GetItemType();
    }

    if (expandTuple && keyType->IsTuple()) {
        auto tuple = AS_TYPE(TTupleType, keyType);
        for (ui32 i = 0; i < tuple->GetElementsCount(); ++i) {
            bool isOptional;
            auto unpacked = UnpackOptional(tuple->GetElementType(i), isOptional);
            if (!unpacked->IsData()) {
                encoded = true;
                break;
            }

            types.emplace_back(*AS_TYPE(TDataType, unpacked)->GetDataSlot(), isOptional);
        }

        if (!encoded) {
            isTuple = true;
        }
    } else if (keyType->IsData()) {
        types.emplace_back(*AS_TYPE(TDataType, keyType)->GetDataSlot(), isOptional);
    } else {
        encoded = true;
    }

    if (encoded) {
        types.clear();
        types.emplace_back(NUdf::EDataSlot::String, false);
        return;
    }
}

TPlainContainerCache::TPlainContainerCache() {
    Clear();
}

void TPlainContainerCache::Clear() {
    Cached.fill(NUdf::TUnboxedValue());
    CachedItems.fill(nullptr);
}

NUdf::TUnboxedValuePod TPlainContainerCache::NewArray(const THolderFactory& factory, ui64 size, NUdf::TUnboxedValue*& items) {
    if (!CachedItems[CacheIndex] || !Cached[CacheIndex].UniqueBoxed()) {
        CacheIndex ^= 1U;
        if (!CachedItems[CacheIndex] || !Cached[CacheIndex].UniqueBoxed()) {
            Cached[CacheIndex] = factory.CreateDirectArrayHolder(size, CachedItems[CacheIndex]);
            items = CachedItems[CacheIndex];
            return static_cast<const NUdf::TUnboxedValuePod&>(Cached[CacheIndex]);
        }
    }

    items = CachedItems[CacheIndex];
    std::fill_n(items, size, NUdf::TUnboxedValue());
    return static_cast<const NUdf::TUnboxedValuePod&>(Cached[CacheIndex]);
}

} // namespace NMiniKQL
} // namespace NKikimr
