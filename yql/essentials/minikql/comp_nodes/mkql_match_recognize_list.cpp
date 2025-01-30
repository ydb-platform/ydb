#include "mkql_match_recognize_list.h"

namespace NKikimr::NMiniKQL::NMatchRecognize {

namespace {

class TIterator : public TTemporaryComputationValue<TIterator> {
public:
    TIterator(TMemoryUsageInfo* memUsage, const TSparseList& parent)
        : TTemporaryComputationValue<TIterator>(memUsage)
        , Parent(parent)
        , Current(Parent.Begin())
    {}

private:
    bool Skip() final {
        return ++Current != Parent.End();
    }

    bool Next(NUdf::TUnboxedValue& value) final {
        if (!Skip()) {
            return false;
        }
        value = Current->second.Value;
        return true;
    }

    bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) final {
        if (!Next(payload)) {
            return false;
        }
        key = NUdf::TUnboxedValuePod(Current->first);
        return true;
    }

    const TSparseList& Parent;
    TSparseList::iterator Current;
};

class TKeysIterator : public TTemporaryComputationValue<TKeysIterator> {
public:
    TKeysIterator(TMemoryUsageInfo* memUsage, const TSparseList& parent)
        : TTemporaryComputationValue<TKeysIterator>(memUsage)
        , Parent(parent)
        , Current(Parent.Begin())
    {}
private:
    bool Skip() final {
        return ++Current != Parent.End();
    }

    bool Next(NUdf::TUnboxedValue& key) final {
        if (!Skip()) {
            return false;
        }
        key = NUdf::TUnboxedValuePod(Current->first);
        return true;
    }

    const TSparseList& Parent;
    TSparseList::iterator Current;
};

} // anonymous namespace

TListValue::TListValue(TMemoryUsageInfo* memUsage, const TSparseList& list)
    : TComputationValue<TListValue>(memUsage)
    , List(list)
{}

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
    return List.Size();
}

NUdf::TUnboxedValue TListValue::GetDictIterator() const {
    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), List));
}

NUdf::TUnboxedValue TListValue::GetKeysIterator() const {
    return NUdf::TUnboxedValuePod(new TKeysIterator(GetMemInfo(), List));
}

NUdf::TUnboxedValue TListValue::GetPayloadsIterator() const {
    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), List));
}

bool TListValue::Contains(const NUdf::TUnboxedValuePod& key) const {
    return List.Contains(key.Get<ui64>());
}

NUdf::TUnboxedValue TListValue::Lookup(const NUdf::TUnboxedValuePod& key) const {
    return List.Get(key.Get<ui64>());
}

bool TListValue::HasDictItems() const {
    return !List.Empty();
}

} // namespace NKikimr::NMiniKQL::NMatchRecognize
