#pragma once
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NOlap {
    struct TIndexInfo;
    struct TGranuleRecord;
    struct TPredicate;
}

namespace NKikimr::NOlap::NCosts {


class TMarkRangeFeatures {
private:
    bool IntervalSkipped = false;
    bool MarkIncluded = true;
public:
    TMarkRangeFeatures() = default;

    TString ToString() const;

    TMarkRangeFeatures& SetIntervalSkipped(const bool value) {
        IntervalSkipped = value;
        return *this;
    }

    bool GetIntervalSkipped() const {
        return IntervalSkipped;
    }

    TMarkRangeFeatures& SetMarkIncluded(const bool value) {
        MarkIncluded = value;
        return *this;
    }

    bool GetMarkIncluded() const {
        return MarkIncluded;
    }

    NKikimrKqp::TEvRemoteCostData::TIntervalMeta SerializeToProto() const {
        NKikimrKqp::TEvRemoteCostData::TIntervalMeta result;
        result.SetIntervalSkipped(IntervalSkipped);
        result.SetMarkIncluded(MarkIncluded);
        return result;
    }

    bool DeserializeFromProto(const NKikimrKqp::TEvRemoteCostData::TIntervalMeta& proto) {
        SetIntervalSkipped(proto.GetIntervalSkipped());
        SetMarkIncluded(proto.GetMarkIncluded());
        return true;
    }
};

class TKeyRanges {
private:
    friend class TKeyRangesBuilder;
    NArrow::TRecordBatchReader BatchReader;
    std::vector<TMarkRangeFeatures> RangeMarkFeatures;
    bool LeftBorderOpened = false;
public:
    using TMark = NArrow::TRecordBatchReader::TRecordIterator;

    const TMarkRangeFeatures& GetMarkFeatures(const ui32 index) const {
        Y_VERIFY(index < RangeMarkFeatures.size());
        return RangeMarkFeatures[index];
    }

    TString ToString() const;
    ui32 GetMarksCount() const {
        return BatchReader.GetRowsCount();
    }
    size_t GetColumnsCount() const {
        return BatchReader.GetColumnsCount();
    }
    size_t ColumnsCount() const {
        return BatchReader.GetColumnsCount();
    }
    const NArrow::TRecordBatchReader& GetBatch() const {
        return BatchReader;
    }
    bool IsLeftBorderOpened() const {
        return LeftBorderOpened;
    }

    NKikimrKqp::TEvRemoteCostData::TCostInfo SerializeToProto() const;
    bool DeserializeFromProto(const NKikimrKqp::TEvRemoteCostData::TCostInfo& proto);

};

class TKeyRangesBuilder {
private:
    bool LeftBorderOpened = false;
    NArrow::TRecordBatchConstructor Constructor;
    TVector<TMarkRangeFeatures> Features;

    bool AddMarkFromPredicate(const std::shared_ptr<NOlap::TPredicate>& p);
    void AddMarkFromGranule(const TGranuleRecord& record);

public:
    TKeyRangesBuilder(const TIndexInfo& indexInfo);
    void Reserve(const ui32 num) {
        Constructor.Reserve(num);
    }
    void FillRangeMarks(const std::shared_ptr<NOlap::TPredicate>& left, const TVector<TGranuleRecord>& granuleRecords,
        const std::shared_ptr<NOlap::TPredicate>& right);

    TKeyRanges Build();
};
}
