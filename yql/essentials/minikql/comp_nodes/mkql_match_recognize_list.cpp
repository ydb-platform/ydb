#include "mkql_match_recognize_list.h"

#include <utility>

namespace NKikimr::NMiniKQL::NMatchRecognize {

namespace {

class TIterator: public TTemporaryComputationValue<TIterator> {
public:
    TIterator(TMemoryUsageInfo* memUsage, const TSparseList& parent)
        : TTemporaryComputationValue<TIterator>(memUsage)
        , Parent_(parent)
        , Current_(Parent_.Begin())
    {
    }

private:
    bool Skip() final {
        return ++Current_ != Parent_.End();
    }

    bool Next(NUdf::TUnboxedValue& value) final {
        if (!Skip()) {
            return false;
        }
        value = Current_->second.Value;
        return true;
    }

    bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
        if (!Next(payload)) {
            return false;
        }
        key = NUdf::TUnboxedValuePod(Current_->first);
        return true;
    }

    const TSparseList& Parent_;
    TSparseList::iterator Current_;
};

class TKeysIterator: public TTemporaryComputationValue<TKeysIterator> {
public:
    TKeysIterator(TMemoryUsageInfo* memUsage, const TSparseList& parent)
        : TTemporaryComputationValue<TKeysIterator>(memUsage)
        , Parent_(parent)
        , Current_(Parent_.Begin())
    {
    }

private:
    bool Skip() final {
        return ++Current_ != Parent_.End();
    }

    bool Next(NUdf::TUnboxedValue& key) final {
        if (!Skip()) {
            return false;
        }
        key = NUdf::TUnboxedValuePod(Current_->first);
        return true;
    }

    const TSparseList& Parent_;
    TSparseList::iterator Current_;
};

} // anonymous namespace

TListValue::TListValue(TMemoryUsageInfo* memUsage, TSparseList list)
    : TComputationValue<TListValue>(memUsage)
    , List_(std::move(list))
{
}

bool TListValue::HasFastListLength() const {
    return true;
}

ui64 TListValue::GetListLength() const {
    return GetDictLength();
}

ui64 TListValue::GetEstimatedListLength() const {
    return GetListLength();
}

NUdf::TUnboxedValue TListValue::GetListIterator() const {
    return GetPayloadsIterator();
}

bool TListValue::HasListItems() const {
    return HasDictItems();
}

NUdf::IBoxedValuePtr TListValue::ToIndexDictImpl(const NUdf::IValueBuilder& builder) const {
    Y_UNUSED(builder);
    return const_cast<TListValue*>(this);
}

ui64 TListValue::GetDictLength() const {
    return List_.Size();
}

NUdf::TUnboxedValue TListValue::GetDictIterator() const {
    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), List_));
}

NUdf::TUnboxedValue TListValue::GetKeysIterator() const {
    return NUdf::TUnboxedValuePod(new TKeysIterator(GetMemInfo(), List_));
}

NUdf::TUnboxedValue TListValue::GetPayloadsIterator() const {
    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), List_));
}

bool TListValue::Contains(const NUdf::TUnboxedValuePod& key) const {
    return List_.Contains(key.Get<ui64>());
}

NUdf::TUnboxedValue TListValue::Lookup(const NUdf::TUnboxedValuePod& key) const {
    return List_.Get(key.Get<ui64>());
}

bool TListValue::HasDictItems() const {
    return !List_.Empty();
}

} // namespace NKikimr::NMiniKQL::NMatchRecognize
