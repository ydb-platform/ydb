#include "topfreq.h"
#include <cmath>
#include <algorithm>

using namespace NKikimr;
using namespace NUdf;

template <typename THash, typename TEquals>
TTopFreqBase<THash, TEquals>::TTopFreqBase(THash hash, TEquals equals)
    : Indices_(0, hash, equals)
{}

template <typename THash, typename TEquals>
void TTopFreqBase<THash, TEquals>::Init(const TUnboxedValuePod& value, const ui32 minSize, const ui32 maxSize) {
    MinSize_ = minSize;
    MaxSize_ = maxSize;

    Freqs_.reserve(MaxSize_ + 1);
    Indices_.reserve(MaxSize_ + 1);

    AddValue(value);
}

template <typename THash, typename TEquals>
void TTopFreqBase<THash, TEquals>::Merge(const TTopFreqBase& topFreq1, const TTopFreqBase& topFreq2) {
    MinSize_ = std::max(topFreq1.MinSize_, topFreq2.MinSize_);
    MaxSize_ = std::max(topFreq1.MaxSize_, topFreq2.MaxSize_);

    Freqs_.reserve(std::max(MaxSize_ + 1, ui32(topFreq1.Freqs_.size() + topFreq2.Freqs_.size())));
    Indices_.reserve(MaxSize_ + 1);

    Add(topFreq1);
    Add(topFreq2);
}

template <typename THash, typename TEquals>
void TTopFreqBase<THash, TEquals>::Deserialize(const TUnboxedValuePod& serialized) {
    MinSize_ = serialized.GetElement(0).Get<ui32>();
    MaxSize_ = serialized.GetElement(1).Get<ui32>();

    Freqs_.reserve(MaxSize_ + 1);
    Indices_.reserve(MaxSize_ + 1);

    const auto listIter = serialized.GetElement(2).GetListIterator();
    for (TUnboxedValue current; listIter.Next(current);) {
        Update(current.GetElement(1), current.GetElement(0).Get<ui64>());
    }
}

template <typename THash, typename TEquals>
TUnboxedValue TTopFreqBase<THash, TEquals>::Convert(const IValueBuilder* valueBuilder) const {
    TUnboxedValue* values = nullptr;
    const auto list = valueBuilder->NewArray(Freqs_.size(), values);
    for (const auto& item : Freqs_) {
        TUnboxedValue* items = nullptr;
        *values++ = valueBuilder->NewArray(2U, items);
        items[0] = TUnboxedValuePod(item.second);
        items[1] = item.first;
    }
    return list;
}

template <typename THash, typename TEquals>
void TTopFreqBase<THash, TEquals>::Add(const TTopFreqBase& otherModeCalc) {
    for (auto& it : otherModeCalc.Freqs_) {
        Update(it.first, it.second);
    }

    TryCompress();
}

template <typename THash, typename TEquals>
TUnboxedValue TTopFreqBase<THash, TEquals>::Get(const IValueBuilder* builder, ui32 resultSize) {
    resultSize = std::min(resultSize, ui32(Freqs_.size()));
    Compress(resultSize, true);
    return Convert(builder);
}

template <typename THash, typename TEquals>
void TTopFreqBase<THash, TEquals>::AddValue(const TUnboxedValuePod& value) {
    Update(value, 1);
    TryCompress();
}

template <typename THash, typename TEquals>
void TTopFreqBase<THash, TEquals>::Update(const TUnboxedValuePod& value, ui64 freq) {
    Freqs_.emplace_back(TUnboxedValuePod(value), freq);
    auto mapInsertResult = Indices_.emplace(TUnboxedValuePod(value), Freqs_.size() - 1);

    if (!mapInsertResult.second) {
        Freqs_[mapInsertResult.first->second].second += freq;
        Freqs_.pop_back();
    }
}

template <typename THash, typename TEquals>
void TTopFreqBase<THash, TEquals>::TryCompress() {
    auto freqSize = Freqs_.size();
    if (freqSize > MaxSize_) {
        Compress(MinSize_);
    }
}

template <typename THash, typename TEquals>
void TTopFreqBase<THash, TEquals>::Compress(ui32 newSize, bool sort) {
    auto compare = [](const TVectorElement& v1, const TVectorElement& v2) {
        return v1.second > v2.second;
    };

    if (sort) {
        std::sort(Freqs_.begin(), Freqs_.end(), compare);
    } else {
        std::nth_element(Freqs_.begin(), Freqs_.begin() + newSize - 1, Freqs_.end(), compare);
    }

    Indices_.clear();
    Freqs_.resize(newSize);

    for (ui32 i = 0; i < newSize; i++) {
        Indices_[Freqs_[i].first] = i;
    }
}

template <typename THash, typename TEquals>
TUnboxedValue TTopFreqBase<THash, TEquals>::Serialize(const IValueBuilder* builder) {
    if (ui32(Freqs_.size()) > MinSize_) {
        Compress(MinSize_);
    }

    TUnboxedValue* items = nullptr;
    auto tuple = builder->NewArray(3U, items);
    items[0] = TUnboxedValuePod(MinSize_);
    items[1] = TUnboxedValuePod(MaxSize_);
    items[2] = Convert(builder);
    return tuple;
}

template <EDataSlot Slot>
TTopFreqData<Slot>::TTopFreqData(const TUnboxedValuePod& value, const ui32 minSize, const ui32 maxSize)
    : TBase(TUnboxedValueHash<Slot>(), TUnboxedValueEquals<Slot>())
{
    TBase::Init(value, minSize, maxSize);
}

template <EDataSlot Slot>
TTopFreqData<Slot>::TTopFreqData(const TTopFreqData& topFreq1, const TTopFreqData& topFreq2)
    : TBase(TUnboxedValueHash<Slot>(), TUnboxedValueEquals<Slot>())
{
    TBase::Merge(topFreq1, topFreq2);
}

template <EDataSlot Slot>
TTopFreqData<Slot>::TTopFreqData(const TUnboxedValuePod& serialized)
    : TBase(TUnboxedValueHash<Slot>(), TUnboxedValueEquals<Slot>())
{
    TBase::Deserialize(serialized);
}

template <EDataSlot Slot>
TUnboxedValue TTopFreqData<Slot>::Serialize(const IValueBuilder* builder) {
    return TBase::Serialize(builder);
}

template <EDataSlot Slot>
TUnboxedValue TTopFreqData<Slot>::Get(const IValueBuilder* builder, ui32 resultSize) {
    return TBase::Get(builder, resultSize);
}

template <EDataSlot Slot>
void TTopFreqData<Slot>::AddValue(const TUnboxedValuePod& value) {
    TBase::AddValue(value);
}

#define INSTANCE_FOR(slot, ...) \
    template class TTopFreqData<EDataSlot::slot>;

UDF_TYPE_ID_MAP(INSTANCE_FOR)

#undef INSTANCE_FOR

TTopFreqGeneric::TTopFreqGeneric(const TUnboxedValuePod& value, const ui32 minSize, const ui32 maxSize,
    IHash::TPtr hash, IEquate::TPtr equate)
    : TBase(TGenericHash{hash}, TGenericEquals{equate})
{
    TBase::Init(value, minSize, maxSize);
}

TTopFreqGeneric::TTopFreqGeneric(const TTopFreqGeneric& topFreq1, const TTopFreqGeneric& topFreq2,
    IHash::TPtr hash, IEquate::TPtr equate)
    : TBase(TGenericHash{hash}, TGenericEquals{equate})
{
    TBase::Merge(topFreq1, topFreq2);
}

TTopFreqGeneric::TTopFreqGeneric(const TUnboxedValuePod& serialized,
    IHash::TPtr hash, IEquate::TPtr equate)
    : TBase(TGenericHash{hash}, TGenericEquals{equate})
{
    TBase::Deserialize(serialized);
}

TUnboxedValue TTopFreqGeneric::Serialize(const IValueBuilder* builder) {
    return TBase::Serialize(builder);
}

TUnboxedValue TTopFreqGeneric::Get(const IValueBuilder* builder, ui32 resultSize) {
    return TBase::Get(builder, resultSize);
}

void TTopFreqGeneric::AddValue(const TUnboxedValuePod& value) {
    TBase::AddValue(value);
}

