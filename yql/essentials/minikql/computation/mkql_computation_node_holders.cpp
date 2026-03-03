#include "mkql_computation_node_holders.h"
#include "mkql_computation_node_pack.h"
#include "mkql_custom_list.h"
#include "mkql_value_builder.h"
#include "presort.h"

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/utils/unaligned_read.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/singleton.h>

namespace NKikimr::NMiniKQL {

namespace {

class TValueDataHolder: public TComputationValue<TValueDataHolder> {
public:
    TValueDataHolder(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& value)
        : TComputationValue(memInfo)
        , Value_(std::move(value))
    {
    }

private:
    const NUdf::TUnboxedValue Value_;
};

class TDirectListHolder: public TComputationValue<TDirectListHolder> {
public:
    class TIterator: public TComputationValue<TIterator> {
    public:
        explicit TIterator(const TDirectListHolder* parent)
            : TComputationValue(parent->GetMemInfo())
            , Parent_(const_cast<TDirectListHolder*>(parent))
            , Iterator_(parent->Items_)
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (Iterator_.AtEnd()) {
                    return false;
                }

                Iterator_.Next();
            }

            return !Iterator_.AtEnd();
        }

        bool Next(NUdf::TUnboxedValue& value) override {
            if (!Skip()) {
                return false;
            }
            value = Iterator_.Current();
            return true;
        }

        const NUdf::TRefCountedPtr<TDirectListHolder> Parent_;
        TDefaultListRepresentation::TIterator Iterator_;
        bool AtStart_;
    };

    class TDictIterator: public TComputationValue<TDictIterator> {
    public:
        TDictIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& iter)
            : TComputationValue(memInfo)
            , Iter_(std::move(iter))
            , Index_(Max<ui64>())
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& key) override {
            if (Iter_.Skip()) {
                key = NUdf::TUnboxedValuePod(++Index_);
                return true;
            }

            return false;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (Iter_.Next(payload)) {
                key = NUdf::TUnboxedValuePod(++Index_);
                return true;
            }

            return false;
        }

        bool Skip() override {
            if (Iter_.Skip()) {
                ++Index_;
                return true;
            }

            return false;
        }

        const NUdf::TUnboxedValue Iter_;
        ui64 Index_;
    };

    TDirectListHolder(TMemoryUsageInfo* memInfo, TDefaultListRepresentation&& items)
        : TComputationValue(memInfo)
        , Items_(std::move(items))
    {
    }

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

        return Items_.GetItemByIndex(index).Release().MakeOptional();
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
        return Items_.GetLength();
    }

    ui64 GetEstimatedListLength() const override {
        return Items_.GetLength();
    }

    bool HasListItems() const override {
        return Items_.GetLength() != 0;
    }

    const NUdf::TOpaqueListRepresentation* GetListRepresentation() const override {
        return reinterpret_cast<const NUdf::TOpaqueListRepresentation*>(&Items_);
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const override {
        switch (Items_.GetLength()) {
            case 0U:
                return builder.NewEmptyList().Release().AsBoxed();
            case 1U:
                return const_cast<TDirectListHolder*>(this);
            default:
                break;
        }

        TDefaultListRepresentation result;
        for (auto it = Items_.GetReverseIterator(); !it.AtEnd(); it.Next()) {
            result = result.Append(NUdf::TUnboxedValue(it.Current()));
        }

        return new TDirectListHolder(GetMemInfo(), std::move(result));
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        if (count == 0) {
            return const_cast<TDirectListHolder*>(this);
        }

        if (count >= Items_.GetLength()) {
            return builder.NewEmptyList().Release().AsBoxed();
        }

        auto result = Items_.SkipFromBegin(static_cast<size_t>(count));
        return new TDirectListHolder(GetMemInfo(), std::move(result));
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        if (count == 0) {
            return builder.NewEmptyList().Release().AsBoxed();
        }

        if (count >= Items_.GetLength()) {
            return const_cast<TDirectListHolder*>(this);
        }

        auto result = Items_.SkipFromEnd(static_cast<size_t>(Items_.GetLength() - count));
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
        return Items_.GetLength() != 0;
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const final {
        return Items_.GetItemByIndex(index);
    }

    const NUdf::TUnboxedValue* GetElements() const final {
        return Items_.GetItems();
    }

    bool IsSortedDict() const override {
        return true;
    }

    TDefaultListRepresentation Items_;
};

template <class TBaseVector>
class TVectorHolderBase: public TComputationValue<TVectorHolderBase<TBaseVector>>, public TBaseVector {
private:
    using TBaseValue = TComputationValue<TVectorHolderBase<TBaseVector>>;

public:
    explicit TVectorHolderBase(TMemoryUsageInfo* memInfo)
        : TBaseValue(memInfo)
    {
    }
    TVectorHolderBase(TMemoryUsageInfo* memInfo, TBaseVector&& vector)
        : TBaseValue(memInfo)
        , TBaseVector(std::move(vector))
    {
    }

    ~TVectorHolderBase() override {
    }

private:
    class TValuesIterator: public TTemporaryComputationValue<TValuesIterator> {
    private:
        using TBase = TTemporaryComputationValue<TValuesIterator>;

    public:
        explicit TValuesIterator(const TVectorHolderBase* parent)
            : TBase(parent->GetMemInfo())
            , Size_(parent->size())
            , Parent_(const_cast<TVectorHolderBase*>(parent))
        {
        }

    private:
        bool Skip() final {
            return ++Current_ < Size_;
        }

        bool Next(NUdf::TUnboxedValue& value) final {
            if (Size_ <= Current_) {
                return false;
            }
            value = (*Parent_)[Current_];
            ++Current_;
            return true;
        }

        const size_t Size_;
        ui64 Current_ = 0;
        const NUdf::TRefCountedPtr<TVectorHolderBase> Parent_;
    };

    class TDictIterator: public TTemporaryComputationValue<TDictIterator> {
    private:
        using TBase = TTemporaryComputationValue<TDictIterator>;

    public:
        explicit TDictIterator(const TVectorHolderBase* parent)
            : TBase(parent->GetMemInfo())
            , Size_(parent->size())
            , Parent_(const_cast<TVectorHolderBase*>(parent))
        {
        }

    private:
        bool Skip() final {
            return ++Current_ < Size_;
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (Current_ == Size_) {
                return false;
            }
            key = NUdf::TUnboxedValuePod(Current_);
            ++Current_;
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (Current_ == Size_) {
                return false;
            }
            key = NUdf::TUnboxedValuePod(Current_);
            payload = (*Parent_)[Current_];
            ++Current_;
            return true;
        }

        const size_t Size_;
        ui64 Current_ = 0;
        const NUdf::TRefCountedPtr<TVectorHolderBase> Parent_;
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
        if (!count) {
            return const_cast<TVectorHolderBase*>(this);
        }

        if (count >= TBaseVector::size()) {
            return builder.NewEmptyList().Release().AsBoxed();
        }

        TBaseVector copy(TBaseVector::begin() + count, TBaseVector::end());
        return new TVectorHolderBase(TBaseValue::GetMemInfo(), std::move(copy));
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        if (!count) {
            return builder.NewEmptyList().Release().AsBoxed();
        }

        if (count >= TBaseVector::size()) {
            return const_cast<TVectorHolderBase*>(this);
        }

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
    explicit TEmptyContainerHolder(TMemoryUsageInfo* memInfo)
        : TComputationValue(memInfo)
        , None_()
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod&) const override {
        return false;
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod&) const override {
        return None_;
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
        return reinterpret_cast<const NUdf::TOpaqueListRepresentation*>(&List_);
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
        return &None_;
    }

    const NUdf::TUnboxedValue None_;
    const TDefaultListRepresentation List_;
};

class TSortedSetHolder: public TComputationValue<TSortedSetHolder> {
public:
    typedef TUnboxedValueVector TItems;

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        explicit TIterator(const TSortedSetHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent_(const_cast<TSortedSetHolder*>(parent))
            , Iterator_(Parent_->Items_.begin())
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (Iterator_ == Parent_->Items_.end()) {
                    return false;
                }

                ++Iterator_;
            }

            return Iterator_ != Parent_->Items_.end();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip()) {
                return false;
            }
            if (NoSwap) {
                key = *Iterator_;
                if (Parent_->Packer_) {
                    key = Parent_->Packer_->Decode(key.AsStringRef(), false, Parent_->HolderFactory_);
                }
            } else {
                key = NUdf::TUnboxedValuePod::Void();
            }
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key)) {
                return false;
            }
            if (NoSwap) {
                payload = NUdf::TUnboxedValuePod::Void();
            } else {
                payload = *Iterator_;
                if (Parent_->Packer_) {
                    payload = Parent_->Packer_->Decode(payload.AsStringRef(), false, Parent_->HolderFactory_);
                }
            }
            return true;
        }

        const NUdf::TRefCountedPtr<TSortedSetHolder> Parent_;
        TItems::const_iterator Iterator_;
        bool AtStart_;
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
        , Filler_(filler)
        , Types_(types)
        , IsTuple_(isTuple)
        , Mode_(mode)
        , Compare_(compare)
        , Equate_(equate)
        , IsBuilt_(false)
        , HolderFactory_(holderFactory)
    {
        if (encodedType) {
            Packer_.emplace(encodedType);
        }

        if (eagerFill) {
            LazyBuildDict();
        }
    }

    ~TSortedSetHolder() override {
        MKQL_MEM_RETURN(GetMemInfo(), &Items_, Items_.capacity() * sizeof(TItems::value_type));
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer_) {
            encodedKey = MakeString(Packer_->Encode(key, false));
        }

        return BinarySearch(Items_.begin(), Items_.end(), NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key),
                            TValueLess(Types_, IsTuple_, Compare_));
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer_) {
            encodedKey = MakeString(Packer_->Encode(key, false));
        }

        const auto it = LowerBound(Items_.begin(), Items_.end(),
                                   NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key),
                                   TValueLess(Types_, IsTuple_, Compare_));
        if (it == Items_.end() || !TValueEqual(Types_, IsTuple_, Equate_)(
                                      *it, NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key))) {
            return NUdf::TUnboxedValuePod();
        }

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
        return Items_.size();
    }

    bool HasDictItems() const override {
        LazyBuildDict();
        return !Items_.empty();
    }

    void LazyBuildDict() const {
        if (IsBuilt_) {
            return;
        }

        Filler_(Items_);
        Filler_ = TSortedSetFiller();

        switch (Mode_) {
            case EDictSortMode::RequiresSorting:
                StableSort(Items_.begin(), Items_.end(),
                           TValueLess(Types_, IsTuple_, Compare_));
                Items_.erase(Unique(Items_.begin(), Items_.end(),
                                    TValueEqual(Types_, IsTuple_, Equate_)), Items_.end());
                break;
            case EDictSortMode::SortedUniqueAscending:
                break;
            case EDictSortMode::SortedUniqueDescening:
                Reverse(Items_.begin(), Items_.end());
                break;
            default:
                Y_ABORT();
        }

        Y_DEBUG_ABORT_UNLESS(IsSortedUnique());
        IsBuilt_ = true;

        if (!Items_.empty()) {
            MKQL_MEM_TAKE(GetMemInfo(), &Items_, Items_.capacity() * sizeof(TItems::value_type));
        }
    }

    bool IsSortedUnique() const {
        TValueLess less(Types_, IsTuple_, Compare_);
        for (size_t i = 1, e = Items_.size(); i < e; ++i) {
            if (!less(Items_[i - 1], Items_[i])) {
                return false;
            }
        }

        return true;
    }

    bool IsSortedDict() const override {
        return true;
    }

private:
    mutable TSortedSetFiller Filler_;
    const TKeyTypes Types_;
    const bool IsTuple_;
    const EDictSortMode Mode_;
    const NUdf::ICompare* Compare_;
    const NUdf::IEquate* Equate_;
    mutable bool IsBuilt_;
    const THolderFactory& HolderFactory_;
    mutable TItems Items_;
    mutable std::optional<TGenericPresortEncoder> Packer_;
};

class TSortedDictHolder: public TComputationValue<TSortedDictHolder> {
public:
    typedef TKeyPayloadPairVector TItems;

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        explicit TIterator(const TSortedDictHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent_(const_cast<TSortedDictHolder*>(parent))
            , Iterator_(Parent_->Items_.begin())
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (Iterator_ == Parent_->Items_.end()) {
                    return false;
                }

                ++Iterator_;
            }

            return Iterator_ != Parent_->Items_.end();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip()) {
                return false;
            }
            if (NoSwap) {
                key = Iterator_->first;
                if (Parent_->Packer_) {
                    key = Parent_->Packer_->Decode(key.AsStringRef(), false, Parent_->HolderFactory_);
                }
            } else {
                key = Iterator_->second;
            }
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key)) {
                return false;
            }
            if (NoSwap) {
                payload = Iterator_->second;
            } else {
                payload = Iterator_->first;
                if (Parent_->Packer_) {
                    payload = Parent_->Packer_->Decode(payload.AsStringRef(), false, Parent_->HolderFactory_);
                }
            }
            return true;
        }

        const NUdf::TRefCountedPtr<TSortedDictHolder> Parent_;
        TItems::const_iterator Iterator_;
        bool AtStart_;
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
        , Filler_(filler)
        , Types_(types)
        , IsTuple_(isTuple)
        , Mode_(mode)
        , Compare_(compare)
        , Equate_(equate)
        , IsBuilt_(false)
        , HolderFactory_(holderFactory)
    {
        if (encodedType) {
            Packer_.emplace(encodedType);
        }

        if (eagerFill) {
            LazyBuildDict();
        }
    }

    ~TSortedDictHolder() override {
        MKQL_MEM_RETURN(GetMemInfo(), &Items_, Items_.capacity() * sizeof(TItems::value_type));
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer_) {
            encodedKey = MakeString(Packer_->Encode(key, false));
        }

        return BinarySearch(Items_.begin(), Items_.end(),
                            TItems::value_type(
                                NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key),
                                NUdf::TUnboxedValuePod()),
                            TKeyPayloadPairLess(Types_, IsTuple_, Compare_));
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer_) {
            encodedKey = MakeString(Packer_->Encode(key, false));
        }

        const auto it = LowerBound(Items_.begin(), Items_.end(),
                                   TItems::value_type(
                                       NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key),
                                       NUdf::TUnboxedValue()), TKeyPayloadPairLess(Types_, IsTuple_, Compare_));
        if (it == Items_.end() || !TKeyPayloadPairEqual(Types_, IsTuple_, Equate_)(
                                      {it->first, it->second},
                                      TKeyPayloadPair(NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key), {}))) {
            return NUdf::TUnboxedValuePod();
        }

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
        return Items_.size();
    }

    bool HasDictItems() const override {
        LazyBuildDict();
        return !Items_.empty();
    }

    void LazyBuildDict() const {
        if (IsBuilt_) {
            return;
        }

        Filler_(Items_);
        Filler_ = TSortedDictFiller();

        switch (Mode_) {
            case EDictSortMode::RequiresSorting:
                StableSort(Items_.begin(), Items_.end(), TKeyPayloadPairLess(Types_, IsTuple_, Compare_));
                Items_.erase(Unique(Items_.begin(), Items_.end(), TKeyPayloadPairEqual(Types_, IsTuple_, Equate_)), Items_.end());
                break;
            case EDictSortMode::SortedUniqueAscending:
                break;
            case EDictSortMode::SortedUniqueDescening:
                Reverse(Items_.begin(), Items_.end());
                break;
            default:
                Y_ABORT();
        }

        Y_DEBUG_ABORT_UNLESS(IsSortedUnique());
        IsBuilt_ = true;

        if (!Items_.empty()) {
            MKQL_MEM_TAKE(GetMemInfo(), &Items_, Items_.capacity() * sizeof(TItems::value_type));
        }
    }

    bool IsSortedUnique() const {
        TKeyPayloadPairLess less(Types_, IsTuple_, Compare_);
        for (size_t i = 1, e = Items_.size(); i < e; ++i) {
            if (!less(Items_[i - 1], Items_[i])) {
                return false;
            }
        }

        return true;
    }

    bool IsSortedDict() const override {
        return true;
    }

private:
    mutable TSortedDictFiller Filler_;
    const TKeyTypes Types_;
    const bool IsTuple_;
    const EDictSortMode Mode_;
    const NUdf::ICompare* Compare_;
    const NUdf::IEquate* Equate_;
    mutable bool IsBuilt_;
    const THolderFactory& HolderFactory_;
    mutable TItems Items_;
    mutable std::optional<TGenericPresortEncoder> Packer_;
};

class THashedSetHolder: public TComputationValue<THashedSetHolder> {
public:
    class TIterator: public TComputationValue<TIterator> {
    public:
        explicit TIterator(const THashedSetHolder* parent)
            : TComputationValue(parent->GetMemInfo())
            , Parent_(const_cast<THashedSetHolder*>(parent))
            , Iterator_(Parent_->Set_.begin())
            , End_(Parent_->Set_.end())
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (Iterator_ == End_) {
                    return false;
                }

                ++Iterator_;
            }

            return Iterator_ != End_;
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip()) {
                return false;
            }
            key = *Iterator_;
            if (Parent_->Packer_) {
                key = Parent_->Packer_->Unpack(key.AsStringRef(), Parent_->HolderFactory_);
            }

            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key)) {
                return false;
            }
            payload = NUdf::TUnboxedValuePod::Void();
            return true;
        }

    private:
        const NUdf::TRefCountedPtr<THashedSetHolder> Parent_;
        TValuesDictHashSet::const_iterator Iterator_;
        TValuesDictHashSet::const_iterator End_;
        bool AtStart_;
    };

    THashedSetHolder(TMemoryUsageInfo* memInfo, THashedSetFiller filler,
                     const TKeyTypes& types, bool isTuple, bool eagerFill, TType* encodedType,
                     const NUdf::IHash* hash, const NUdf::IEquate* equate, const THolderFactory& holderFactory)
        : TComputationValue(memInfo)
        , Filler_(filler)
        , Types_(types)
        , Set_(0, TValueHasher(Types_, isTuple, hash), TValueEqual(Types_, isTuple, equate))
        , IsBuilt_(false)
        , HolderFactory_(holderFactory)
    {
        if (encodedType) {
            Packer_.emplace(true, encodedType);
        }

        if (eagerFill) {
            LazyBuildDict();
        }
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer_) {
            encodedKey = MakeString(Packer_->Pack(key));
        }

        return Set_.find(NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key)) != Set_.cend();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer_) {
            encodedKey = MakeString(Packer_->Pack(key));
        }

        const auto it = Set_.find(NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key));
        if (it == Set_.cend()) {
            return NUdf::TUnboxedValuePod();
        }
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
        return Set_.size();
    }

    bool HasDictItems() const override {
        LazyBuildDict();
        return !Set_.empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

private:
    void LazyBuildDict() const {
        if (IsBuilt_) {
            return;
        }

        Filler_(Set_);
        Filler_ = THashedSetFiller();

        IsBuilt_ = true;
    }

private:
    mutable THashedSetFiller Filler_;
    const TKeyTypes Types_;
    mutable TValuesDictHashSet Set_;
    mutable bool IsBuilt_;
    const THolderFactory& HolderFactory_;
    mutable std::optional<TValuePacker> Packer_;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedSetHolder: public TComputationValue<THashedSingleFixedSetHolder<T, OptionalKey>> {
public:
    using TSetType = TValuesDictHashSingleFixedSet<T>;

    class TIterator: public TComputationValue<TIterator> {
    public:
        enum class EState {
            AtStart,
            AtNull,
            Iterator
        };
        explicit TIterator(const THashedSingleFixedSetHolder* parent)
            : TComputationValue<TIterator>(parent->GetMemInfo())
            , Parent_(const_cast<THashedSingleFixedSetHolder*>(parent))
            , Iterator_(Parent_->Set_.begin())
            , End_(Parent_->Set_.end())
            , State_(EState::AtStart)
        {
        }

    private:
        bool Skip() final {
            switch (State_) {
                case EState::AtStart:
                    State_ = OptionalKey && Parent_->HasNull_ ? EState::AtNull : EState::Iterator;
                    break;
                case EState::AtNull:
                    State_ = EState::Iterator;
                    break;
                case EState::Iterator:
                    if (Iterator_ == End_) {
                        return false;
                    }
                    ++Iterator_;
                    break;
            }

            return EState::AtNull == State_ || Iterator_ != End_;
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip()) {
                return false;
            }
            key = EState::AtNull == State_ ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(*Iterator_);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key)) {
                return false;
            }
            payload = NUdf::TUnboxedValuePod::Void();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedSetHolder> Parent_;
        typename TSetType::const_iterator Iterator_;
        typename TSetType::const_iterator End_;
        EState State_;
    };

    THashedSingleFixedSetHolder(TMemoryUsageInfo* memInfo, TSetType&& set, bool hasNull)
        : TComputationValue<THashedSingleFixedSetHolder>(memInfo)
        , Set_(std::move(set))
        , HasNull_(hasNull)
    {
        MKQL_ENSURE(OptionalKey || !HasNull_, "Null value is not allowed for non-optional key type");
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return HasNull_;
            }
        }
        return Set_.find(key.Get<T>()) != Set_.cend();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        if (Contains(key)) {
            return NUdf::TUnboxedValuePod::Void();
        }
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
        return Set_.size() + ui64(OptionalKey && HasNull_);
    }

    bool HasDictItems() const final {
        return !Set_.empty() || (OptionalKey && HasNull_);
    }

    bool IsSortedDict() const final {
        return false;
    }

    const TSetType Set_;
    const bool HasNull_;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactSetHolder: public TComputationValue<THashedSingleFixedCompactSetHolder<T, OptionalKey>> {
public:
    using TSetType = TValuesDictHashSingleFixedCompactSet<T>;

    class TIterator: public TComputationValue<TIterator> {
    public:
        enum class EState {
            AtStart,
            AtNull,
            Iterator
        };
        explicit TIterator(const THashedSingleFixedCompactSetHolder* parent)
            : TComputationValue<TIterator>(parent->GetMemInfo())
            , Parent_(const_cast<THashedSingleFixedCompactSetHolder*>(parent))
            , Iterator_(Parent_->Set_.Iterate())
            , State_(EState::AtStart)
        {
        }

    private:
        bool Skip() final {
            switch (State_) {
                case EState::AtStart:
                    State_ = OptionalKey && Parent_->HasNull_ ? EState::AtNull : EState::Iterator;
                    break;
                case EState::AtNull:
                    State_ = EState::Iterator;
                    break;
                case EState::Iterator:
                    if (!Iterator_.Ok()) {
                        return false;
                    }
                    ++Iterator_;
                    break;
            }

            return EState::AtNull == State_ || Iterator_.Ok();
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip()) {
                return false;
            }
            key = EState::AtNull == State_ ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(*Iterator_);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key)) {
                return false;
            }
            payload = NUdf::TUnboxedValuePod::Void();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedCompactSetHolder> Parent_;
        typename TSetType::TIterator Iterator_;
        EState State_;
    };

    THashedSingleFixedCompactSetHolder(TMemoryUsageInfo* memInfo, TSetType&& set, bool hasNull)
        : TComputationValue<THashedSingleFixedCompactSetHolder>(memInfo)
        , Set_(std::move(set))
        , HasNull_(hasNull)
    {
        MKQL_ENSURE(OptionalKey || !HasNull_, "Null value is not allowed for non-optional key type");
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return HasNull_;
            }
        }
        return Set_.Has(key.Get<T>());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        if (Contains(key)) {
            return NUdf::TUnboxedValuePod::Void();
        }
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
        return Set_.Size() + ui64(OptionalKey && HasNull_);
    }

    bool HasDictItems() const final {
        return !Set_.Empty() || (OptionalKey && HasNull_);
    }

    bool IsSortedDict() const final {
        return false;
    }

    const TSetType Set_;
    const bool HasNull_;
};

class THashedCompactSetHolder: public TComputationValue<THashedCompactSetHolder> {
public:
    using TSetType = TValuesDictHashCompactSet;

    class TIterator: public TComputationValue<TIterator> {
    public:
        explicit TIterator(const THashedCompactSetHolder* parent)
            : TComputationValue(parent->GetMemInfo())
            , Parent_(const_cast<THashedCompactSetHolder*>(parent))
            , Iterator_(Parent_->Set_.Iterate())
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (!Iterator_.Ok()) {
                    return false;
                }
                ++Iterator_;
            }

            return Iterator_.Ok();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip()) {
                return false;
            }
            key = Parent_->KeyPacker_.Unpack(GetSmallValue(*Iterator_), Parent_->Ctx_->HolderFactory);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key)) {
                return false;
            }
            payload = NUdf::TUnboxedValuePod::Void();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedCompactSetHolder> Parent_;
        typename TSetType::TIterator Iterator_;
        bool AtStart_;
    };

    THashedCompactSetHolder(TMemoryUsageInfo* memInfo, TSetType&& set, TPagedArena&& pool, TType* keyType, TComputationContext* ctx)
        : TComputationValue(memInfo)
        , Pool_(std::move(pool))
        , Set_(std::move(set))
        , KeyPacker_(true, keyType)
        , Ctx_(ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker_.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        return Set_.Has(smallValue);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker_.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        if (Set_.Has(smallValue)) {
            return NUdf::TUnboxedValuePod::Void();
        }
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
        return Set_.Size();
    }

    bool HasDictItems() const override {
        return !Set_.Empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

private:
    TPagedArena Pool_;
    const TSetType Set_;
    mutable TValuePacker KeyPacker_;
    TComputationContext* Ctx_;
};

class THashedCompactMapHolder: public TComputationValue<THashedCompactMapHolder> {
public:
    using TMapType = TValuesDictHashCompactMap;

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        explicit TIterator(const THashedCompactMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent_(const_cast<THashedCompactMapHolder*>(parent))
            , Iterator_(Parent_->Map_.Iterate())
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (!Iterator_.Ok()) {
                    return false;
                }
                ++Iterator_;
            }

            return Iterator_.Ok();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip()) {
                return false;
            }
            key = NoSwap
                      ? Parent_->KeyPacker_.Unpack(GetSmallValue(Iterator_.Get().first), Parent_->Ctx_->HolderFactory)
                      : Parent_->PayloadPacker_.Unpack(GetSmallValue(Iterator_.Get().second), Parent_->Ctx_->HolderFactory);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key)) {
                return false;
            }
            payload = NoSwap
                          ? Parent_->PayloadPacker_.Unpack(GetSmallValue(Iterator_.Get().second), Parent_->Ctx_->HolderFactory)
                          : Parent_->KeyPacker_.Unpack(GetSmallValue(Iterator_.Get().first), Parent_->Ctx_->HolderFactory);
            return true;
        }

        const NUdf::TRefCountedPtr<THashedCompactMapHolder> Parent_;
        typename TMapType::TIterator Iterator_;
        bool AtStart_;
    };

    THashedCompactMapHolder(TMemoryUsageInfo* memInfo, TMapType&& map, TPagedArena&& pool,
                            TType* keyType, TType* payloadType, TComputationContext* ctx)
        : TComputationValue(memInfo)
        , Pool_(std::move(pool))
        , Map_(std::move(map))
        , KeyPacker_(true, keyType)
        , PayloadPacker_(false, payloadType)
        , Ctx_(ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker_.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        return Map_.Has(smallValue);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker_.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        auto it = Map_.Find(smallValue);
        if (!it.Ok()) {
            return NUdf::TUnboxedValuePod();
        }
        return PayloadPacker_.Unpack(GetSmallValue(it.Get().second), Ctx_->HolderFactory).Release().MakeOptional();
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
        return Map_.Size();
    }

    bool HasDictItems() const override {
        return !Map_.Empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

    TPagedArena Pool_;
    const TMapType Map_;
    mutable TValuePacker KeyPacker_;
    mutable TValuePacker PayloadPacker_;
    TComputationContext* Ctx_;
};

class THashedCompactMultiMapHolder: public TComputationValue<THashedCompactMultiMapHolder> {
public:
    using TMapType = TValuesDictHashCompactMultiMap;
    using TMapIterator = typename TMapType::TIterator;

    class TPayloadList: public TCustomListValue {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(const THashedCompactMultiMapHolder* parent, TMapIterator from)
                : TComputationValue(parent->GetMemInfo())
                , Parent_(const_cast<THashedCompactMultiMapHolder*>(parent))
                , Iterator_(from)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                if (!Iterator_.Ok()) {
                    return false;
                }

                value = Parent_->PayloadPacker_.Unpack(GetSmallValue(Iterator_.GetValue()), Parent_->CompCtx_.HolderFactory);
                ++Iterator_;
                return true;
            }

            bool Skip() override {
                if (!Iterator_.Ok()) {
                    return false;
                }

                ++Iterator_;
                return true;
            }

            const NUdf::TRefCountedPtr<THashedCompactMultiMapHolder> Parent_;
            TMapIterator Iterator_;
        };

        TPayloadList(TMemoryUsageInfo* memInfo, const THashedCompactMultiMapHolder* parent, TMapIterator from)
            : TCustomListValue(memInfo)
            , Parent_(const_cast<THashedCompactMultiMapHolder*>(parent))
            , From_(from)
        {
            Y_ASSERT(From_.Ok());
        }

    private:
        bool HasFastListLength() const override {
            return true;
        }

        ui64 GetListLength() const override {
            if (!Length_) {
                Length_ = Parent_->Map_.Count(From_.GetKey());
            }

            return *Length_;
        }

        bool HasListItems() const override {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator(Parent_.Get(), From_));
        }

        const NUdf::TRefCountedPtr<THashedCompactMultiMapHolder> Parent_;
        TMapIterator From_;
    };

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        explicit TIterator(const THashedCompactMultiMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent_(const_cast<THashedCompactMultiMapHolder*>(parent))
            , Iterator_(parent->Map_.Iterate())
        {
        }

    private:
        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Iterator_.Ok()) {
                return false;
            }

            if (NoSwap) {
                key = Parent_->KeyPacker_.Unpack(GetSmallValue(Iterator_.GetKey()), Parent_->CompCtx_.HolderFactory);
                payload = Parent_->CompCtx_.HolderFactory.Create<TPayloadList>(Parent_.Get(), Iterator_.MakeCurrentKeyIter());
            } else {
                payload = Parent_->KeyPacker_.Unpack(GetSmallValue(Iterator_.GetKey()), Parent_->CompCtx_.HolderFactory);
                key = Parent_->CompCtx_.HolderFactory.Create<TPayloadList>(Parent_.Get(), Iterator_.MakeCurrentKeyIter());
            }
            Iterator_.NextKey();
            return true;
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Iterator_.Ok()) {
                return false;
            }

            key = NoSwap ? Parent_->KeyPacker_.Unpack(GetSmallValue(Iterator_.GetKey()), Parent_->CompCtx_.HolderFactory) : NUdf::TUnboxedValue(Parent_->CompCtx_.HolderFactory.Create<TPayloadList>(Parent_.Get(), Iterator_.MakeCurrentKeyIter()));
            Iterator_.NextKey();
            return true;
        }

        bool Skip() override {
            if (!Iterator_.Ok()) {
                return false;
            }

            Iterator_.NextKey();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedCompactMultiMapHolder> Parent_;
        TMapIterator Iterator_;
    };

    THashedCompactMultiMapHolder(TMemoryUsageInfo* memInfo, TMapType&& map, TPagedArena&& pool,
                                 TType* keyType, TType* payloadType, TComputationContext* ctx)
        : TComputationValue(memInfo)
        , Pool_(std::move(pool))
        , Map_(std::move(map))
        , KeyPacker_(true, keyType)
        , PayloadPacker_(false, payloadType)
        , CompCtx_(*ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker_.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        return Map_.Has(smallValue);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        auto serializedKey = KeyPacker_.Pack(NUdf::TUnboxedValuePod(key));
        ui64 smallValue = AsSmallValue(serializedKey);
        auto it = Map_.Find(smallValue);
        if (!it.Ok()) {
            return NUdf::TUnboxedValuePod();
        }

        return CompCtx_.HolderFactory.Create<TPayloadList>(this, it);
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
        return Map_.UniqSize();
    }

    bool HasDictItems() const override {
        return !Map_.Empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

    TPagedArena Pool_;
    const TMapType Map_;
    mutable TValuePacker KeyPacker_;
    mutable TValuePacker PayloadPacker_;
    TComputationContext& CompCtx_;
};

class THashedDictHolder: public TComputationValue<THashedDictHolder> {
public:
    template <bool NoSwap>
    class TIterator: public TTemporaryComputationValue<TIterator<NoSwap>> {
    public:
        explicit TIterator(const THashedDictHolder* parent)
            : TTemporaryComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent_(const_cast<THashedDictHolder*>(parent))
            , Iterator_(Parent_->Map_.begin())
            , End_(Parent_->Map_.end())
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (Iterator_ == End_) {
                    return false;
                }
                ++Iterator_;
            }

            return Iterator_ != End_;
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip()) {
                return false;
            }
            if (NoSwap) {
                key = Iterator_->first;
                if (Parent_->Packer_) {
                    key = Parent_->Packer_->Unpack(key.AsStringRef(), Parent_->HolderFactory_);
                }
            } else {
                key = Iterator_->second;
            }

            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key)) {
                return false;
            }
            if (NoSwap) {
                payload = Iterator_->second;
            } else {
                payload = Iterator_->first;
                if (Parent_->Packer_) {
                    payload = Parent_->Packer_->Unpack(payload.AsStringRef(), Parent_->HolderFactory_);
                }
            }
            return true;
        }

        const NUdf::TRefCountedPtr<THashedDictHolder> Parent_;
        TValuesDictHashMap::const_iterator Iterator_;
        TValuesDictHashMap::const_iterator End_;
        bool AtStart_;
    };

    THashedDictHolder(TMemoryUsageInfo* memInfo, THashedDictFiller filler,
                      const TKeyTypes& types, bool isTuple, bool eagerFill, TType* encodedType,
                      const NUdf::IHash* hash, const NUdf::IEquate* equate, const THolderFactory& holderFactory)
        : TComputationValue(memInfo)
        , Filler_(filler)
        , Types_(types)
        , Map_(0, TValueHasher(Types_, isTuple, hash), TValueEqual(Types_, isTuple, equate))
        , IsBuilt_(false)
        , HolderFactory_(holderFactory)
    {
        if (encodedType) {
            Packer_.emplace(true, encodedType);
        }

        if (eagerFill) {
            LazyBuildDict();
        }
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer_) {
            encodedKey = MakeString(Packer_->Pack(key));
        }

        return Map_.find(NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key)) != Map_.cend();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        LazyBuildDict();
        NUdf::TUnboxedValue encodedKey;
        if (Packer_) {
            encodedKey = MakeString(Packer_->Pack(key));
        }

        const auto it = Map_.find(NUdf::TUnboxedValuePod(Packer_ ? encodedKey : key));
        if (it == Map_.cend()) {
            return NUdf::TUnboxedValuePod();
        }
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
        return Map_.size();
    }

    bool HasDictItems() const override {
        LazyBuildDict();
        return !Map_.empty();
    }

    bool IsSortedDict() const override {
        return false;
    }

private:
    void LazyBuildDict() const {
        if (IsBuilt_) {
            return;
        }

        Filler_(Map_);
        Filler_ = THashedDictFiller();
        IsBuilt_ = true;
    }

private:
    mutable THashedDictFiller Filler_;
    const TKeyTypes Types_;
    mutable TValuesDictHashMap Map_;
    mutable bool IsBuilt_;
    const THolderFactory& HolderFactory_;
    std::optional<TValuePacker> Packer_;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedMapHolder: public TComputationValue<THashedSingleFixedMapHolder<T, OptionalKey>> {
public:
    using TMapType = TValuesDictHashSingleFixedMap<T>;

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        enum class EState {
            AtStart,
            AtNull,
            Iterator
        };
        explicit TIterator(const THashedSingleFixedMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent_(const_cast<THashedSingleFixedMapHolder*>(parent))
            , Iterator_(Parent_->Map_.begin())
            , End_(Parent_->Map_.end())
            , State_(EState::AtStart)
        {
        }

    private:
        bool Skip() final {
            switch (State_) {
                case EState::AtStart:
                    State_ = OptionalKey && Parent_->NullPayload_.has_value() ? EState::AtNull : EState::Iterator;
                    break;
                case EState::AtNull:
                    State_ = EState::Iterator;
                    break;
                case EState::Iterator:
                    if (Iterator_ == End_) {
                        return false;
                    }
                    ++Iterator_;
                    break;
            }

            return EState::AtNull == State_ || Iterator_ != End_;
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip()) {
                return false;
            }
            key = NoSwap
                      ? (EState::AtNull == State_
                             ? NUdf::TUnboxedValue()
                             : NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(Iterator_->first)))
                      : (EState::AtNull == State_
                             ? *Parent_->NullPayload_
                             : Iterator_->second);
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key)) {
                return false;
            }
            payload = NoSwap
                          ? (EState::AtNull == State_
                                 ? *Parent_->NullPayload_
                                 : Iterator_->second)
                          : (EState::AtNull == State_
                                 ? NUdf::TUnboxedValue()
                                 : NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(Iterator_->first)));
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedMapHolder> Parent_;
        typename TMapType::const_iterator Iterator_;
        typename TMapType::const_iterator End_;
        EState State_;
    };

    THashedSingleFixedMapHolder(TMemoryUsageInfo* memInfo, TValuesDictHashSingleFixedMap<T>&& map,
                                std::optional<NUdf::TUnboxedValue>&& nullPayload)
        : TComputationValue<THashedSingleFixedMapHolder>(memInfo)
        , Map_(std::move(map))
        , NullPayload_(std::move(nullPayload))
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayload_.has_value();
            }
        }
        return Map_.find(key.Get<T>()) != Map_.end();
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayload_.has_value() ? NullPayload_->MakeOptional() : NUdf::TUnboxedValuePod();
            }
        }
        const auto it = Map_.find(key.Get<T>());
        if (it == Map_.end()) {
            return NUdf::TUnboxedValuePod();
        }
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
        return Map_.size() + ui64(OptionalKey && NullPayload_.has_value());
    }

    bool HasDictItems() const final {
        return !Map_.empty() || (OptionalKey && NullPayload_.has_value());
    }

    bool IsSortedDict() const final {
        return false;
    }

    const TMapType Map_;
    const std::optional<NUdf::TUnboxedValue> NullPayload_;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMapHolder: public TComputationValue<THashedSingleFixedCompactMapHolder<T, OptionalKey>> {
public:
    using TMapType = TValuesDictHashSingleFixedCompactMap<T>;

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        enum class EState {
            AtStart,
            AtNull,
            Iterator
        };
        explicit TIterator(const THashedSingleFixedCompactMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent_(const_cast<THashedSingleFixedCompactMapHolder*>(parent))
            , Iterator_(Parent_->Map_.Iterate())
            , State_(EState::AtStart)
        {
        }

    private:
        bool Skip() final {
            switch (State_) {
                case EState::AtStart:
                    State_ = OptionalKey && Parent_->NullPayload_.has_value() ? EState::AtNull : EState::Iterator;
                    break;
                case EState::AtNull:
                    State_ = EState::Iterator;
                    break;
                case EState::Iterator:
                    if (Iterator_.Ok()) {
                        ++Iterator_;
                    }
                    break;
            }

            return EState::AtNull == State_ || Iterator_.Ok();
        }

        void FromUnpack(NUdf::TUnboxedValue& out) {
            if (EState::AtNull == State_) {
                out = Parent_->PayloadPacker_.Unpack(GetSmallValue(*Parent_->NullPayload_), Parent_->Ctx_->HolderFactory);
            } else {
                const auto& iteratorValue = Iterator_.Get();
                const auto& aligned = NYql::TypedReadUnaligned(&iteratorValue.second);
                out = Parent_->PayloadPacker_.Unpack(GetSmallValue(aligned), Parent_->Ctx_->HolderFactory);
            }
        }

        void FromIterator(NUdf::TUnboxedValue& out) {
            if (EState::AtNull == State_) {
                out = NUdf::TUnboxedValue();
            } else {
                out = NUdf::TUnboxedValue(NUdf::TUnboxedValuePod(Iterator_.Get().first));
            }
        }

        bool Next(NUdf::TUnboxedValue& key) final {
            if (!Skip()) {
                return false;
            }

            if constexpr (NoSwap) {
                FromIterator(key);
            } else {
                FromUnpack(key);
            }
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
            if (!Next(key)) {
                return false;
            }
            if constexpr (NoSwap) {
                FromUnpack(payload);
            } else {
                FromIterator(payload);
            }
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedCompactMapHolder> Parent_;
        typename TMapType::TIterator Iterator_;
        EState State_;
    };

    THashedSingleFixedCompactMapHolder(TMemoryUsageInfo* memInfo, TMapType&& map,
                                       std::optional<ui64>&& nullPayload, TPagedArena&& pool,
                                       TType* payloadType, TComputationContext* ctx)
        : TComputationValue<THashedSingleFixedCompactMapHolder>(memInfo)
        , Pool_(std::move(pool))
        , Map_(std::move(map))
        , NullPayload_(std::move(nullPayload))
        , PayloadPacker_(false, payloadType)
        , Ctx_(ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayload_.has_value();
            }
        }
        return Map_.Has(key.Get<T>());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayload_.has_value()
                           ? PayloadPacker_.Unpack(GetSmallValue(*NullPayload_), Ctx_->HolderFactory).Release().MakeOptional()
                           : NUdf::TUnboxedValuePod();
            }
        }
        auto it = Map_.Find(key.Get<T>());
        if (!it.Ok()) {
            return NUdf::TUnboxedValuePod();
        }
        const auto& iteratorValue = it.Get();
        const auto& aligned = NYql::TypedReadUnaligned(&iteratorValue.second);
        return PayloadPacker_.Unpack(GetSmallValue(aligned), Ctx_->HolderFactory).Release().MakeOptional();
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
        return Map_.Size() + ui64(OptionalKey && NullPayload_.has_value());
    }

    bool HasDictItems() const final {
        return !Map_.Empty() || (OptionalKey && NullPayload_.has_value());
    }

    bool IsSortedDict() const final {
        return false;
    }

private:
    TPagedArena Pool_;
    const TMapType Map_;
    const std::optional<ui64> NullPayload_;
    mutable TValuePacker PayloadPacker_;
    TComputationContext* Ctx_;
};

template <typename T, bool OptionalKey>
class THashedSingleFixedCompactMultiMapHolder: public TComputationValue<THashedSingleFixedCompactMultiMapHolder<T, OptionalKey>> {
public:
    using TMapType = TValuesDictHashSingleFixedCompactMultiMap<T>;
    using TMapIterator = typename TMapType::TIterator;

    class TPayloadList: public TCustomListValue {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(const THashedSingleFixedCompactMultiMapHolder* parent, TMapIterator from)
                : TComputationValue<TIterator>(parent->GetMemInfo())
                , Parent_(const_cast<THashedSingleFixedCompactMultiMapHolder*>(parent))
                , Iterator_(from)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (!Iterator_.Ok()) {
                    return false;
                }

                value = Parent_->PayloadPacker_.Unpack(GetSmallValue(Iterator_.GetValue()), Parent_->Ctx_->HolderFactory);
                ++Iterator_;
                return true;
            }

            bool Skip() final {
                if (!Iterator_.Ok()) {
                    return false;
                }

                ++Iterator_;
                return true;
            }

            const NUdf::TRefCountedPtr<THashedSingleFixedCompactMultiMapHolder> Parent_;
            TMapIterator Iterator_;
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
            if (!Length_) {
                Length_ = Parent->Map_.Count(From.GetKey());
            }

            return *Length_;
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
        class TIterator: public TComputationValue<TIterator> {
        public:
            explicit TIterator(const THashedSingleFixedCompactMultiMapHolder* parent)
                : TComputationValue<TIterator>(parent->GetMemInfo())
                , Parent_(const_cast<THashedSingleFixedCompactMultiMapHolder*>(parent))
                , Iterator_(Parent_->NullPayloads_.cbegin())
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (Iterator_ == Parent_->NullPayloads_.cend()) {
                    return false;
                }

                value = Parent_->PayloadPacker_.Unpack(GetSmallValue(*Iterator_), Parent_->Ctx_->HolderFactory);
                ++Iterator_;
                return true;
            }

            bool Skip() final {
                if (Iterator_ == Parent_->NullPayloads_.cend()) {
                    return false;
                }

                ++Iterator_;
                return true;
            }

            const NUdf::TRefCountedPtr<THashedSingleFixedCompactMultiMapHolder> Parent_;
            typename std::vector<ui64>::const_iterator Iterator_;
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
            if (!Length_) {
                Length_ = Parent->NullPayloads_.size();
            }

            return *Length_;
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
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
    public:
        explicit TIterator(const THashedSingleFixedCompactMultiMapHolder* parent)
            : TComputationValue<TIterator<NoSwap>>(parent->GetMemInfo())
            , Parent_(const_cast<THashedSingleFixedCompactMultiMapHolder*>(parent))
            , Iterator_(parent->Map_.Iterate())
            , AtNull_(OptionalKey && !parent->NullPayloads_.empty())
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& key) override {
            if (AtNull_) {
                AtNull_ = false;
                key = NoSwap
                          ? NUdf::TUnboxedValuePod()
                          : Parent_->Ctx_->HolderFactory.template Create<TNullPayloadList>(Parent_.Get());
                return true;
            }
            if (!Iterator_.Ok()) {
                return false;
            }

            key = NoSwap
                      ? NUdf::TUnboxedValuePod(Iterator_.GetKey())
                      : Parent_->Ctx_->HolderFactory.template Create<TPayloadList>(Parent_.Get(), Iterator_.MakeCurrentKeyIter());
            Iterator_.NextKey();
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (AtNull_) {
                AtNull_ = false;
                if (NoSwap) {
                    key = NUdf::TUnboxedValuePod();
                    payload = Parent_->Ctx_->HolderFactory.template Create<TNullPayloadList>(Parent_.Get());
                } else {
                    payload = NUdf::TUnboxedValuePod();
                    key = Parent_->Ctx_->HolderFactory.template Create<TNullPayloadList>(Parent_.Get());
                }
                return true;
            }
            if (!Iterator_.Ok()) {
                return false;
            }

            if (NoSwap) {
                key = NUdf::TUnboxedValuePod(Iterator_.GetKey());
                payload = Parent_->Ctx_->HolderFactory.template Create<TPayloadList>(Parent_.Get(), Iterator_.MakeCurrentKeyIter());
            } else {
                payload = NUdf::TUnboxedValuePod(Iterator_.GetKey());
                key = Parent_->Ctx_->HolderFactory.template Create<TPayloadList>(Parent_.Get(), Iterator_.MakeCurrentKeyIter());
            }
            Iterator_.NextKey();
            return true;
        }

        bool Skip() override {
            if (AtNull_) {
                AtNull_ = false;
                return true;
            }
            if (!Iterator_.Ok()) {
                return false;
            }

            Iterator_.NextKey();
            return true;
        }

        const NUdf::TRefCountedPtr<THashedSingleFixedCompactMultiMapHolder> Parent_;
        TMapIterator Iterator_;
        bool AtNull_;
    };

    THashedSingleFixedCompactMultiMapHolder(TMemoryUsageInfo* memInfo, TMapType&& map,
                                            std::vector<ui64>&& nullPayloads, TPagedArena&& pool,
                                            TType* payloadType, TComputationContext* ctx)
        : TComputationValue<THashedSingleFixedCompactMultiMapHolder>(memInfo)
        , Pool_(std::move(pool))
        , Map_(std::move(map))
        , NullPayloads_(std::move(nullPayloads))
        , PayloadPacker_(false, payloadType)
        , Ctx_(ctx)
    {
    }

private:
    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        if constexpr (OptionalKey) {
            if (!key) {
                return !NullPayloads_.empty();
            }
        }
        return Map_.Has(key.Get<T>());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        if constexpr (OptionalKey) {
            if (!key) {
                return NullPayloads_.empty()
                           ? NUdf::TUnboxedValuePod()
                           : Ctx_->HolderFactory.Create<TNullPayloadList>(this);
            }
        }
        const auto it = Map_.Find(key.Get<T>());
        if (!it.Ok()) {
            return NUdf::TUnboxedValuePod();
        }
        return Ctx_->HolderFactory.Create<TPayloadList>(this, it);
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
        return Map_.UniqSize() + ui64(OptionalKey && !NullPayloads_.empty());
    }

    bool HasDictItems() const override {
        return !Map_.Empty() || (OptionalKey && !NullPayloads_.empty());
    }

    bool IsSortedDict() const override {
        return false;
    }

private:
    TPagedArena Pool_;
    const TMapType Map_;
    const std::vector<ui64> NullPayloads_;
    mutable TValuePacker PayloadPacker_;
    TComputationContext* Ctx_;
};

class TVariantHolder: public TComputationValue<TVariantHolder> {
public:
    TVariantHolder(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& item, ui32 index)
        : TComputationValue(memInfo)
        , Item_(std::move(item))
        , Index_(index)
    {
    }

private:
    NUdf::TUnboxedValue GetVariantItem() const override {
        return Item_;
    }

    ui32 GetVariantIndex() const override {
        return Index_;
    }

    const NUdf::TUnboxedValue Item_;
    const ui32 Index_;
};

class TListIteratorHolder: public TComputationValue<TListIteratorHolder> {
public:
    TListIteratorHolder(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& list)
        : TComputationValue(memInfo)
        , List_(std::move(list))
        , Iter_(List_.GetListIterator())
    {
    }

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        return Iter_.Next(result) ? NUdf::EFetchStatus::Ok : NUdf::EFetchStatus::Finish;
    }

    const NUdf::TUnboxedValue List_;
    const NUdf::TUnboxedValue Iter_;
};

class TLimitedList: public TComputationValue<TLimitedList> {
public:
    class TIterator: public TComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& iter, TMaybe<ui64> skip, TMaybe<ui64> take)
            : TComputationValue(memInfo)
            , Iter_(std::move(iter))
            , Skip_(skip)
            , Take_(take)
            , Index_(Max<ui64>())
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& value) override {
            if (!Iter_) {
                return false;
            }

            if (Skip_) {
                while ((Index_ + 1) < Skip_.GetRef()) {
                    if (!Iter_.Skip()) {
                        Iter_ = NUdf::TUnboxedValue();
                        return false;
                    }

                    ++Index_;
                }
            }

            if (Take_ && ((Index_ + 1) - Skip_.GetOrElse(0)) >= Take_.GetRef()) {
                Iter_ = NUdf::TUnboxedValue();
                return false;
            }

            if (!Iter_.Next(value)) {
                Iter_ = NUdf::TUnboxedValue();
                return false;
            }

            ++Index_;
            return true;
        }

        bool Skip() override {
            if (!Iter_) {
                return false;
            }

            if (Skip_) {
                while ((Index_ + 1) < Skip_.GetRef()) {
                    if (!Iter_.Skip()) {
                        Iter_ = NUdf::TUnboxedValue();
                        return false;
                    }

                    ++Index_;
                }
            }

            if (Take_ && ((Index_ + 1) - Skip_.GetOrElse(0)) >= Take_.GetRef()) {
                Iter_ = NUdf::TUnboxedValue();
                return false;
            }

            if (!Iter_.Skip()) {
                Iter_ = NUdf::TUnboxedValue();
                return false;
            }

            ++Index_;
            return true;
        }

        NUdf::TUnboxedValue Iter_;
        const TMaybe<ui64> Skip_;
        const TMaybe<ui64> Take_;
        ui64 Index_;
    };

    TLimitedList(TMemoryUsageInfo* memInfo, NUdf::TRefCountedPtr<NUdf::IBoxedValue> parent, TMaybe<ui64> skip, TMaybe<ui64> take)
        : TComputationValue(memInfo)
        , Parent_(parent)
        , Skip_(skip)
        , Take_(take)
    {
    }

private:
    bool HasFastListLength() const override {
        return Length_.Defined();
    }

    ui64 GetListLength() const override {
        if (!Length_) {
            ui64 length = NUdf::TBoxedValueAccessor::GetListLength(*Parent_);
            if (Skip_) {
                if (Skip_.GetRef() >= length) {
                    length = 0;
                } else {
                    length -= Skip_.GetRef();
                }
            }

            if (Take_) {
                length = Min(length, Take_.GetRef());
            }

            Length_ = length;
        }

        return Length_.GetRef();
    }

    ui64 GetEstimatedListLength() const override {
        return GetListLength();
    }

    bool HasListItems() const override {
        if (HasItems_) {
            return *HasItems_;
        }

        if (Length_) {
            HasItems_ = (*Length_ != 0);
            return *HasItems_;
        }

        HasItems_ = GetListIterator().Skip();
        return *HasItems_;
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), NUdf::TBoxedValueAccessor::GetListIterator(*Parent_), Skip_, Take_));
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        if (!count) {
            return const_cast<TLimitedList*>(this);
        }

        if (Length_) {
            if (count >= Length_.GetRef()) {
                return builder.NewEmptyList().Release().AsBoxed();
            }
        }

        ui64 prevSkip = Skip_.GetOrElse(0);
        if (count > Max<ui64>() - prevSkip) {
            return builder.NewEmptyList().Release().AsBoxed();
        }

        const ui64 newSkip = prevSkip + count;
        TMaybe<ui64> newTake = Take_;
        if (newTake) {
            if (count >= newTake.GetRef()) {
                return builder.NewEmptyList().Release().AsBoxed();
            }

            newTake = newTake.GetRef() - count;
        }

        return new TLimitedList(GetMemInfo(), Parent_, newSkip, newTake);
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const override {
        if (!count) {
            return builder.NewEmptyList().Release().AsBoxed();
        }

        if (Length_) {
            if (count >= Length_.GetRef()) {
                return const_cast<TLimitedList*>(this);
            }
        }

        TMaybe<ui64> newTake = Take_;
        if (newTake) {
            newTake = Min(count, newTake.GetRef());
        } else {
            newTake = count;
        }

        return new TLimitedList(GetMemInfo(), Parent_, Skip_, newTake);
    }

    NUdf::TRefCountedPtr<NUdf::IBoxedValue> Parent_;
    TMaybe<ui64> Skip_;
    TMaybe<ui64> Take_;
    mutable TMaybe<ui64> Length_;
    mutable TMaybe<bool> HasItems_;
};

class TLazyListDecorator: public TComputationValue<TLazyListDecorator> {
public:
    TLazyListDecorator(TMemoryUsageInfo* memInfo, NUdf::IBoxedValuePtr&& list)
        : TComputationValue(memInfo)
        , List_(std::move(list))
    {
    }

private:
    bool HasListItems() const final {
        return NUdf::TBoxedValueAccessor::HasListItems(*List_);
    }

    bool HasDictItems() const final {
        return NUdf::TBoxedValueAccessor::HasDictItems(*List_);
    }

    bool HasFastListLength() const final {
        return NUdf::TBoxedValueAccessor::HasFastListLength(*List_);
    }

    ui64 GetListLength() const final {
        return NUdf::TBoxedValueAccessor::GetListLength(*List_);
    }

    ui64 GetDictLength() const final {
        return NUdf::TBoxedValueAccessor::GetDictLength(*List_);
    }

    ui64 GetEstimatedListLength() const final {
        return NUdf::TBoxedValueAccessor::GetEstimatedListLength(*List_);
    }

    NUdf::TUnboxedValue GetListIterator() const final {
        return NUdf::TBoxedValueAccessor::GetListIterator(*List_);
    }

    NUdf::TUnboxedValue GetDictIterator() const final {
        return NUdf::TBoxedValueAccessor::GetDictIterator(*List_);
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const final {
        return NUdf::TBoxedValueAccessor::GetPayloadsIterator(*List_);
    }

    NUdf::TUnboxedValue GetKeysIterator() const final {
        return NUdf::TBoxedValueAccessor::GetKeysIterator(*List_);
    }

    NUdf::IBoxedValuePtr ReverseListImpl(const NUdf::IValueBuilder& builder) const final {
        return NUdf::TBoxedValueAccessor::ReverseListImpl(*List_, builder);
    }

    NUdf::IBoxedValuePtr SkipListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        return NUdf::TBoxedValueAccessor::SkipListImpl(*List_, builder, count);
    }

    NUdf::IBoxedValuePtr TakeListImpl(const NUdf::IValueBuilder& builder, ui64 count) const final {
        return NUdf::TBoxedValueAccessor::TakeListImpl(*List_, builder, count);
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const final {
        return NUdf::TBoxedValueAccessor::ToIndexDictImpl(*List_, builder);
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const final {
        return NUdf::TBoxedValueAccessor::Contains(*List_, key);
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const final {
        return NUdf::TBoxedValueAccessor::Lookup(*List_, key);
    }

    NUdf::TUnboxedValue GetElement(ui32 index) const final {
        return NUdf::TBoxedValueAccessor::GetElement(*List_, index);
    }

    const NUdf::TUnboxedValue* GetElements() const final {
        return nullptr;
    }

    bool IsSortedDict() const final {
        return NUdf::TBoxedValueAccessor::IsSortedDict(*List_);
    }

    const NUdf::IBoxedValuePtr List_;
};

} // namespace

///////////////////////////////////////////////////////////////////////////////
// TDictValueBuilder
///////////////////////////////////////////////////////////////////////////////
class TDictValueBuilder: public NUdf::IDictValueBuilder {
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

    NUdf::IDictValueBuilder& Add(NUdf::TUnboxedValue&& key, NUdf::TUnboxedValue&& value) override {
        Items_.emplace_back(std::move(key), std::move(value));
        return *this;
    }

    NUdf::TUnboxedValue Build() override {
        if (Items_.empty()) {
            return HolderFactory_.GetEmptyContainerLazy();
        }

        if (DictFlags_ & NUdf::TDictFlags::Hashed) {
            auto prepareFn = (DictFlags_ & NUdf::TDictFlags::Multi)
                                 ? &TDictValueBuilder::PrepareMultiHasedDict
                                 : &TDictValueBuilder::PrepareHasedDict;
            THashedDictFiller filler(std::bind(prepareFn, this, std::placeholders::_1));

            return HolderFactory_.CreateDirectHashedDictHolder(
                filler, Types_, IsTuple_, true, EncodeType_, Hash_, Equate_);
        } else {
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

///////////////////////////////////////////////////////////////////////////////
// TListValueBuilder
///////////////////////////////////////////////////////////////////////////////
class TListValueBuilder: public NUdf::IListValueBuilder {
public:
    explicit TListValueBuilder(const THolderFactory& HolderFactory)
        : HolderFactory_(HolderFactory)
    {
    }

    // Destroys (moves out from) the element
    IListValueBuilder& Add(NUdf::TUnboxedValue&& element) final {
        List_.emplace_back(element);
        return *this;
    }

    // Destroys (moves out from) the elements
    IListValueBuilder& AddMany(const NUdf::TUnboxedValue* elements, size_t count) final {
        std::copy_n(std::make_move_iterator(elements), count, std::back_inserter(List_));
        return *this;
    }

    NUdf::TUnboxedValue Build() final {
        if (List_.empty()) {
            return HolderFactory_.GetEmptyContainerLazy();
        }

        return HolderFactory_.VectorAsVectorHolder(std::move(List_));
    }

private:
    const NMiniKQL::THolderFactory& HolderFactory_;
    TUnboxedValueVector List_;
};

//////////////////////////////////////////////////////////////////////////////
// THolderFactory
//////////////////////////////////////////////////////////////////////////////
THolderFactory::THolderFactory(
    TAllocState& allocState,
    TMemoryUsageInfo& memInfo,
    const IFunctionRegistry* functionRegistry)
    : CurrentAllocState_(&allocState)
    , MemInfo_(memInfo)
    , FunctionRegistry_(functionRegistry)
{
}

THolderFactory::~THolderFactory() {
    if (EmptyContainer_) {
        CurrentAllocState_->UnlockObject(*EmptyContainer_);
    }
}

NUdf::TUnboxedValuePod THolderFactory::GetEmptyContainerLazy() const {
    if (!EmptyContainer_) {
        EmptyContainer_.ConstructInPlace(
            NUdf::TUnboxedValuePod(AllocateOn<TEmptyContainerHolder>(CurrentAllocState_, &MemInfo_)));
        CurrentAllocState_->LockObject(*EmptyContainer_);
    }
    return *EmptyContainer_;
}

NUdf::TUnboxedValuePod THolderFactory::CreateTypeHolder(TType* type) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TTypeHolder>(CurrentAllocState_, &MemInfo_, type));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectListHolder(TDefaultListRepresentation&& items) const {
    if (!items.GetLength()) {
        return GetEmptyContainerLazy();
    }

    return NUdf::TUnboxedValuePod(AllocateOn<TDirectListHolder>(CurrentAllocState_, &MemInfo_, std::move(items)));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectArrayHolder(ui64 size, NUdf::TUnboxedValue*& itemsPtr) const {
    if (!size) {
        itemsPtr = nullptr;
        return GetEmptyContainerLazy();
    }

    const auto buffer = MKQLAllocFastWithSize(
        sizeof(TDirectArrayHolderInplace) + size * sizeof(NUdf::TUnboxedValue),
        CurrentAllocState_, EMemorySubPool::Default);
    const auto h = ::new (buffer) TDirectArrayHolderInplace(&MemInfo_, size);

    auto res = NUdf::TUnboxedValuePod(h);
    itemsPtr = h->GetPtr();
    return res;
}

NUdf::TUnboxedValuePod THolderFactory::CreateArrowBlock(arrow::Datum&& datum) const {
    return Create<TArrowBlock>(std::move(datum));
}

NUdf::TUnboxedValuePod THolderFactory::VectorAsArray(TUnboxedValueVector& values) const {
    if (values.empty()) {
        return GetEmptyContainerLazy();
    }

    NUdf::TUnboxedValue* itemsPtr = nullptr;
    auto tuple = CreateDirectArrayHolder(values.size(), itemsPtr);
    for (auto& value : values) {
        *itemsPtr++ = std::move(value);
    }

    return tuple;
}

NUdf::TUnboxedValuePod THolderFactory::NewVectorHolder() const {
    return NUdf::TUnboxedValuePod(new TVectorHolder(&MemInfo_));
}

NUdf::TUnboxedValuePod THolderFactory::NewTemporaryVectorHolder() const {
    return NUdf::TUnboxedValuePod(new TTemporaryVectorHolder(&MemInfo_));
}

const NUdf::IHash* THolderFactory::GetHash(TType& type, bool useIHash) const {
    return useIHash ? HashRegistry_.FindOrEmplace(type) : nullptr;
}

const NUdf::IEquate* THolderFactory::GetEquate(TType& type, bool useIHash) const {
    return useIHash ? EquateRegistry_.FindOrEmplace(type) : nullptr;
}

const NUdf::ICompare* THolderFactory::GetCompare(TType& type, bool useIHash) const {
    return useIHash ? CompareRegistry_.FindOrEmplace(type) : nullptr;
}

NUdf::TUnboxedValuePod THolderFactory::VectorAsVectorHolder(TUnboxedValueVector&& list) const {
    return NUdf::TUnboxedValuePod(new TVectorHolder(&MemInfo_, std::move(list)));
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
                for (auto out = items; it.Next(*out++);) {
                    continue;
                }
            }
            list.DeleteUnreferenced();
            return array;
        }
    } else {
        items = nullptr;
        return GetEmptyContainerLazy();
    }
}

NUdf::TUnboxedValuePod THolderFactory::Cloned(const NUdf::TUnboxedValuePod& it) const {
    TDefaultListRepresentation result;
    for (NUdf::TUnboxedValue item; it.Next(item);) {
        result = result.Append(std::move(item));
    }

    return CreateDirectListHolder(std::move(result));
}

NUdf::TUnboxedValuePod THolderFactory::Reversed(const NUdf::TUnboxedValuePod& it) const {
    TDefaultListRepresentation result;
    for (NUdf::TUnboxedValue item; it.Next(item);) {
        result = result.Prepend(std::move(item));
    }

    return CreateDirectListHolder(std::move(result));
}

NUdf::TUnboxedValuePod THolderFactory::CreateLimitedList(
    NUdf::IBoxedValuePtr&& parent,
    TMaybe<ui64> skip, TMaybe<ui64> take,
    TMaybe<ui64> knownLength) const {
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

    return NUdf::TUnboxedValuePod(AllocateOn<TLimitedList>(CurrentAllocState_, &MemInfo_, std::move(parent), skip, take));
}

NUdf::TUnboxedValuePod THolderFactory::ReverseList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list) const {
    auto boxed = list.AsBoxed();
    if (auto res = NUdf::TBoxedValueAccessor::ReverseListImpl(*boxed, *builder)) {
        return NUdf::TUnboxedValuePod(std::move(boxed = std::move(res)));
    }

    return Reversed(list.GetListIterator());
}

NUdf::TUnboxedValuePod THolderFactory::SkipList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list, ui64 count) const {
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

NUdf::TUnboxedValuePod THolderFactory::TakeList(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list, ui64 count) const {
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

NUdf::TUnboxedValuePod THolderFactory::ToIndexDict(const NUdf::IValueBuilder* builder, const NUdf::TUnboxedValuePod list) const {
    auto boxed = list.AsBoxed();
    if (auto res = NUdf::TBoxedValueAccessor::ToIndexDictImpl(*boxed, *builder)) {
        return NUdf::TUnboxedValuePod(std::move(boxed = std::move(res)));
    }

    return Cloned(list.GetListIterator());
}

template <bool IsStream>
NUdf::TUnboxedValuePod THolderFactory::Collect(NUdf::TUnboxedValuePod list) const {
    const auto boxed = list.AsBoxed(); // Only for release on exit.
    if (!IsStream && list.HasFastListLength()) {
        auto size = list.GetListLength();
        NUdf::TUnboxedValue* items = nullptr;
        const auto result = CreateDirectArrayHolder(size, items);

        TThresher<IsStream>::DoForEachItem(list,
                                           [&items](NUdf::TUnboxedValue&& item) {
                                               *items++ = std::move(item);
                                           });
        return result;
    } else {
        TDefaultListRepresentation res;

        TThresher<IsStream>::DoForEachItem(list,
                                           [&res](NUdf::TUnboxedValue&& item) {
                                               res = res.Append(std::move(item));
                                           });

        return CreateDirectListHolder(std::move(res));
    }
}

template NUdf::TUnboxedValuePod THolderFactory::Collect<true>(NUdf::TUnboxedValuePod list) const;
template NUdf::TUnboxedValuePod THolderFactory::Collect<false>(NUdf::TUnboxedValuePod list) const;

NUdf::TUnboxedValuePod THolderFactory::LazyList(NUdf::TUnboxedValuePod list) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TLazyListDecorator>(CurrentAllocState_, &MemInfo_, list.AsBoxed()));
    ;
}

NUdf::TUnboxedValuePod THolderFactory::Append(NUdf::TUnboxedValuePod list, NUdf::TUnboxedValuePod last) const {
    const auto boxed = list.AsBoxed();
    TDefaultListRepresentation resList;
    if (const auto leftRepr = reinterpret_cast<const TDefaultListRepresentation*>(
            NUdf::TBoxedValueAccessor::GetListRepresentation(*boxed))) {
        resList = std::move(*leftRepr);
    } else {
        TThresher<false>::DoForEachItem(list,
                                        [&resList](NUdf::TUnboxedValue&& item) {
                                            resList = resList.Append(std::move(item));
                                        });
    }

    resList = resList.Append(std::move(last));
    return CreateDirectListHolder(std::move(resList));
}

NUdf::TUnboxedValuePod THolderFactory::Prepend(NUdf::TUnboxedValuePod first, NUdf::TUnboxedValuePod list) const {
    const auto boxed = list.AsBoxed();
    TDefaultListRepresentation resList;
    if (const auto rightRepr = reinterpret_cast<const TDefaultListRepresentation*>(
            NUdf::TBoxedValueAccessor::GetListRepresentation(*boxed))) {
        resList = *rightRepr;
    } else {
        TThresher<false>::DoForEachItem(list,
                                        [&resList](NUdf::TUnboxedValue&& item) {
                                            resList = resList.Append(std::move(item));
                                        });
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

template <>
NUdf::TUnboxedValuePod THolderFactory::ExtendList<true>(NUdf::TUnboxedValue* data, ui64 size) const {
    if (!data || !size) {
        return GetEmptyContainerLazy();
    }

    TUnboxedValueVector values;
    values.reserve(size);
    std::transform(data, data + size, std::back_inserter(values),
                   [this](NUdf::TUnboxedValue& stream) { return Create<TForwardListValue>(std::move(stream)); });
    return Create<TExtendListValue>(std::move(values));
}

template <>
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

    const auto total = std::accumulate(elements.cbegin(), elements.cend(), 0ULL,
                                       [](ui64 s, TElementsAndSize i) { return s + std::get<1U>(i); });

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
                              });
            }
        }
        std::fill_n(data, size, NUdf::TUnboxedValue());
        return CreateDirectListHolder(std::move(list));
    } else {
        NUdf::TUnboxedValue* items = nullptr;
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
    if (item.TryMakeVariant(index)) {
        return item;
    }

    return CreateBoxedVariantHolder(std::move(item), index);
}

NUdf::TUnboxedValuePod THolderFactory::CreateBoxedVariantHolder(NUdf::TUnboxedValuePod item, ui32 index) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TVariantHolder>(CurrentAllocState_, &MemInfo_, std::move(item), index));
}

NUdf::TUnboxedValuePod THolderFactory::CreateIteratorOverList(NUdf::TUnboxedValuePod list) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TListIteratorHolder>(CurrentAllocState_, &MemInfo_, list));
}

NUdf::TUnboxedValuePod THolderFactory::CreateForwardList(NUdf::TUnboxedValuePod stream) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TForwardListValue>(CurrentAllocState_, &MemInfo_, stream));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectSortedSetHolder(
    TSortedSetFiller filler,
    const TKeyTypes& types,
    bool isTuple,
    EDictSortMode mode,
    bool eagerFill,
    TType* encodedType,
    const NUdf::ICompare* compare,
    const NUdf::IEquate* equate) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TSortedSetHolder>(
        CurrentAllocState_, &MemInfo_, filler, types, isTuple, mode, eagerFill, encodedType, compare, equate, *this));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectSortedDictHolder(
    TSortedDictFiller filler,
    const TKeyTypes& types,
    bool isTuple,
    EDictSortMode mode,
    bool eagerFill,
    TType* encodedType,
    const NUdf::ICompare* compare,
    const NUdf::IEquate* equate) const {
    return NUdf::TUnboxedValuePod(AllocateOn<TSortedDictHolder>(
        CurrentAllocState_, &MemInfo_, filler, types, isTuple, mode, eagerFill, encodedType, compare, equate, *this));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedDictHolder(
    THashedDictFiller filler,
    const TKeyTypes& types,
    bool isTuple,
    bool eagerFill,
    TType* encodedType,
    const NUdf::IHash* hash,
    const NUdf::IEquate* equate) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedDictHolder>(
        CurrentAllocState_, &MemInfo_, filler, types, isTuple, eagerFill, encodedType, hash, equate, *this));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSetHolder(
    THashedSetFiller filler,
    const TKeyTypes& types,
    bool isTuple,
    bool eagerFill,
    TType* encodedType,
    const NUdf::IHash* hash,
    const NUdf::IEquate* equate) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSetHolder>(
        CurrentAllocState_, &MemInfo_, filler, types, isTuple, eagerFill, encodedType, hash, equate, *this));
}

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedSetHolder(
    TValuesDictHashSingleFixedSet<T>&& set, bool hasNull) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedSetHolder<T, OptionalKey>>(
        CurrentAllocState_, &MemInfo_, std::move(set), hasNull));
}

#define DEFINE_HASHED_SINGLE_FIXED_SET_OPT(xType)                                                        \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedSetHolder<xType, true>( \
        TValuesDictHashSingleFixedSet<xType> && set, bool hasNull) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_SET_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_SET_OPT

#define DEFINE_HASHED_SINGLE_FIXED_SET_NONOPT(xType)                                                      \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedSetHolder<xType, false>( \
        TValuesDictHashSingleFixedSet<xType> && set, bool hasNull) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_SET_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_SET_NONOPT

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactSetHolder(
    TValuesDictHashSingleFixedCompactSet<T>&& set, bool hasNull) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedCompactSetHolder<T, OptionalKey>>(
        CurrentAllocState_, &MemInfo_, std::move(set), hasNull));
}

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_OPT(xType)                                                       \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactSetHolder<xType, true>( \
        TValuesDictHashSingleFixedCompactSet<xType> && set, bool hasNull) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_OPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_NONOPT(xType)                                                     \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactSetHolder<xType, false>( \
        TValuesDictHashSingleFixedCompactSet<xType> && set, bool hasNull) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_SET_NONOPT

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedMapHolder(
    TValuesDictHashSingleFixedMap<T>&& map, std::optional<NUdf::TUnboxedValue>&& nullPayload) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedMapHolder<T, OptionalKey>>(
        CurrentAllocState_, &MemInfo_, std::move(map), std::move(nullPayload)));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedCompactSetHolder(
    TValuesDictHashCompactSet&& set, TPagedArena&& pool, TType* keyType, TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedCompactSetHolder>(
        CurrentAllocState_, &MemInfo_, std::move(set), std::move(pool), keyType, ctx));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedCompactMapHolder(
    TValuesDictHashCompactMap&& map, TPagedArena&& pool, TType* keyType, TType* payloadType,
    TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedCompactMapHolder>(
        CurrentAllocState_, &MemInfo_, std::move(map), std::move(pool), keyType, payloadType, ctx));
}

NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedCompactMultiMapHolder(
    TValuesDictHashCompactMultiMap&& map, TPagedArena&& pool, TType* keyType, TType* payloadType,
    TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedCompactMultiMapHolder>(
        CurrentAllocState_, &MemInfo_, std::move(map), std::move(pool), keyType, payloadType, ctx));
}

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMapHolder(
    TValuesDictHashSingleFixedCompactMap<T>&& map, std::optional<ui64>&& nullPayload, TPagedArena&& pool, TType* payloadType,
    TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedCompactMapHolder<T, OptionalKey>>(
        CurrentAllocState_, &MemInfo_, std::move(map), std::move(nullPayload), std::move(pool), payloadType, ctx));
}

template <typename T, bool OptionalKey>
NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMultiMapHolder(
    TValuesDictHashSingleFixedCompactMultiMap<T>&& map, std::vector<ui64>&& nullPayloads, TPagedArena&& pool, TType* payloadType,
    TComputationContext* ctx) const {
    return NUdf::TUnboxedValuePod(AllocateOn<THashedSingleFixedCompactMultiMapHolder<T, OptionalKey>>(
        CurrentAllocState_, &MemInfo_, std::move(map), std::move(nullPayloads), std::move(pool), payloadType, ctx));
}

NUdf::IDictValueBuilder::TPtr THolderFactory::NewDict(
    const NUdf::TType* dictType,
    ui32 flags) const {
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

NUdf::IListValueBuilder::TPtr THolderFactory::NewList() const {
    return new TListValueBuilder(*this);
}

#define DEFINE_HASHED_SINGLE_FIXED_MAP_OPT(xType)                                                        \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedMapHolder<xType, true>( \
        TValuesDictHashSingleFixedMap<xType> && map, std::optional<NUdf::TUnboxedValue> && nullPayload) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_MAP_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_MAP_OPT

#define DEFINE_HASHED_SINGLE_FIXED_MAP_NONOPT(xType)                                                      \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedMapHolder<xType, false>( \
        TValuesDictHashSingleFixedMap<xType> && map, std::optional<NUdf::TUnboxedValue> && nullPayload) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_MAP_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_MAP_NONOPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_OPT(xType)                                                            \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMapHolder<xType, true>(      \
        TValuesDictHashSingleFixedCompactMap<xType> && map, std::optional<ui64> && nullPayload, TPagedArena && pool, \
        TType * payloadType, TComputationContext * ctx) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_OPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_NONOPT(xType)                                                         \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMapHolder<xType, false>(     \
        TValuesDictHashSingleFixedCompactMap<xType> && map, std::optional<ui64> && nullPayload, TPagedArena && pool, \
        TType * payloadType, TComputationContext * ctx) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_NONOPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_MAP_NONOPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_OPT(xType)                                                          \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMultiMapHolder<xType, true>(     \
        TValuesDictHashSingleFixedCompactMultiMap<xType> && map, std::vector<ui64> && nullPayloads, TPagedArena && pool, \
        TType * payloadType, TComputationContext * ctx) const;

KNOWN_PRIMITIVE_VALUE_TYPES(DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_OPT)
#undef DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_OPT

#define DEFINE_HASHED_SINGLE_FIXED_COMPACT_MULTI_MAP_NONOPT(xType)                                                       \
    template NUdf::TUnboxedValuePod THolderFactory::CreateDirectHashedSingleFixedCompactMultiMapHolder<xType, false>(    \
        TValuesDictHashSingleFixedCompactMultiMap<xType> && map, std::vector<ui64> && nullPayloads, TPagedArena && pool, \
        TType * payloadType, TComputationContext * ctx) const;

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
    Cached_.fill(NUdf::TUnboxedValue());
    CachedItems_.fill(nullptr);
}

NUdf::TUnboxedValuePod TPlainContainerCache::NewArray(const THolderFactory& factory, ui64 size, NUdf::TUnboxedValue*& items) {
    if (!CachedItems_[CacheIndex_] || !Cached_[CacheIndex_].UniqueBoxed()) {
        CacheIndex_ ^= 1U;
        if (!CachedItems_[CacheIndex_] || !Cached_[CacheIndex_].UniqueBoxed()) {
            Cached_[CacheIndex_] = factory.CreateDirectArrayHolder(size, CachedItems_[CacheIndex_]);
            items = CachedItems_[CacheIndex_];
            return static_cast<const NUdf::TUnboxedValuePod&>(Cached_[CacheIndex_]);
        }
    }

    items = CachedItems_[CacheIndex_];
    std::fill_n(items, size, NUdf::TUnboxedValue());
    return static_cast<const NUdf::TUnboxedValuePod&>(Cached_[CacheIndex_]);
}

} // namespace NKikimr::NMiniKQL
