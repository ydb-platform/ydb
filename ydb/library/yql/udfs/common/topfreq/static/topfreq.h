#pragma once

#include <ydb/library/yql/public/udf/udf_allocator.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_type_ops.h>

#include <unordered_map>

template <typename THash, typename TEquals>
class TTopFreqBase {
protected:
    using TUnboxedValuePod = NKikimr::NUdf::TUnboxedValuePod;
    using TUnboxedValue = NKikimr::NUdf::TUnboxedValue;
    using IValueBuilder = NKikimr::NUdf::IValueBuilder;

    using TVectorElement = std::pair<TUnboxedValue, ui64>;
    using TVectorType = std::vector<TVectorElement, NKikimr::NUdf::TStdAllocatorForUdf<TVectorElement>>;

    TVectorType Freqs_;
    std::unordered_map<TUnboxedValue, ui32, THash, TEquals, NKikimr::NUdf::TStdAllocatorForUdf<std::pair<const TUnboxedValue, ui32>>> Indices_;
    ui32 MinSize_ = 0;
    ui32 MaxSize_ = 0;

    void Add(const TTopFreqBase& otherCalc);
    void Update(const TUnboxedValuePod& key, const ui64 value);
    void TryCompress();
    void Compress(ui32 newSize, bool sort = false);
    TUnboxedValue Convert(const IValueBuilder* valueBuilder) const;

protected:
    TTopFreqBase(THash hash, TEquals equals);

    void Init(const TUnboxedValuePod& value, const ui32 minSize, const ui32 maxSize);
    void Merge(const TTopFreqBase& TopFreq1, const TTopFreqBase& TopFreq2);
    void Deserialize(const TUnboxedValuePod& serialized);

    TUnboxedValue Serialize(const IValueBuilder* builder);
    TUnboxedValue Get(const IValueBuilder* builder, ui32 resultSize);
    void AddValue(const TUnboxedValuePod& value);
};

template <NKikimr::NUdf::EDataSlot Slot>
class TTopFreqData
    : public TTopFreqBase<
        NKikimr::NUdf::TUnboxedValueHash<Slot>,
        NKikimr::NUdf::TUnboxedValueEquals<Slot>>
{
public:
    using TBase = TTopFreqBase<
        NKikimr::NUdf::TUnboxedValueHash<Slot>,
        NKikimr::NUdf::TUnboxedValueEquals<Slot>>;

    TTopFreqData(const NKikimr::NUdf::TUnboxedValuePod& value, const ui32 minSize, const ui32 maxSize);
    TTopFreqData(const TTopFreqData& topFreq1, const TTopFreqData& topFreq2);
    TTopFreqData(const NKikimr::NUdf::TUnboxedValuePod& serialized);

    NKikimr::NUdf::TUnboxedValue Serialize(const NKikimr::NUdf::IValueBuilder* builder);
    NKikimr::NUdf::TUnboxedValue Get(const NKikimr::NUdf::IValueBuilder* builder, ui32 resultSize);
    void AddValue(const NKikimr::NUdf::TUnboxedValuePod& value);
};

struct TGenericHash {
    NKikimr::NUdf::IHash::TPtr Hash;

    std::size_t operator()(const NKikimr::NUdf::TUnboxedValuePod& value) const {
        return Hash->Hash(value);
    }
};

struct TGenericEquals {
    NKikimr::NUdf::IEquate::TPtr Equate;

    bool operator()(
        const NKikimr::NUdf::TUnboxedValuePod& left,
        const NKikimr::NUdf::TUnboxedValuePod& right) const
    {
        return Equate->Equals(left, right);
    }
};

class TTopFreqGeneric
    : public TTopFreqBase<TGenericHash, TGenericEquals>
{
public:
    using TBase = TTopFreqBase<TGenericHash, TGenericEquals>;

    TTopFreqGeneric(const NKikimr::NUdf::TUnboxedValuePod& value, const ui32 minSize, const ui32 maxSize,
        NKikimr::NUdf::IHash::TPtr hash, NKikimr::NUdf::IEquate::TPtr equate);
    TTopFreqGeneric(const TTopFreqGeneric& topFreq1, const TTopFreqGeneric& topFreq2,
        NKikimr::NUdf::IHash::TPtr hash, NKikimr::NUdf::IEquate::TPtr equate);
    TTopFreqGeneric(const NKikimr::NUdf::TUnboxedValuePod& serialized,
        NKikimr::NUdf::IHash::TPtr hash, NKikimr::NUdf::IEquate::TPtr equate);

    NKikimr::NUdf::TUnboxedValue Serialize(const NKikimr::NUdf::IValueBuilder* builder);
    NKikimr::NUdf::TUnboxedValue Get(const NKikimr::NUdf::IValueBuilder* builder, ui32 resultSize);
    void AddValue(const NKikimr::NUdf::TUnboxedValuePod& value);
};
