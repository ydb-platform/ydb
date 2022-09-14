#pragma once
#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NOlap {
    struct TIndexInfo;
    struct TGranuleRecord;
    struct TPredicate;
}

namespace NKikimr::NOlap::NCosts {


class TKeyMark {
private:
    TVector<std::shared_ptr<arrow::Scalar>> Values;
public:
    TKeyMark() = default;

    TString ToString() const;

    TVector<std::shared_ptr<arrow::Scalar>>::const_iterator begin() const {
        return Values.begin();
    }
    TVector<std::shared_ptr<arrow::Scalar>>::const_iterator end() const {
        return Values.end();
    }
    TKeyMark& AddValue(std::shared_ptr<arrow::Scalar> value) {
        Values.emplace_back(value);
        return *this;
    }
    ui32 ColumnsCount() const {
        return Values.size();
    }
    bool operator<(const TKeyMark& item) const;
};

class TRangeMark {
private:
    TKeyMark Mark;
    bool IntervalSkipped = false;
    bool MarkIncluded = true;
public:
    TRangeMark() = default;
    TRangeMark(TKeyMark&& mark)
        : Mark(std::move(mark))
    {

    }

    TString ToString() const;

    const TKeyMark& GetMark() const {
        return Mark;
    }

    TKeyMark& MutableMark() {
        return Mark;
    }

    bool operator<(const TRangeMark& item) const;

    TRangeMark& SetIntervalSkipped(const bool value) {
        IntervalSkipped = value;
        return *this;
    }

    bool GetIntervalSkipped() const {
        return IntervalSkipped;
    }

    TRangeMark& SetMarkIncluded(const bool value) {
        MarkIncluded = value;
        return *this;
    }

    bool GetMarkIncluded() const {
        return MarkIncluded;
    }

    NKikimrKqp::TEvRemoteCostData::TIntervalMeta SerializeMetaToProto() const {
        NKikimrKqp::TEvRemoteCostData::TIntervalMeta result;
        result.SetIntervalSkipped(IntervalSkipped);
        result.SetMarkIncluded(MarkIncluded);
        return result;
    }

    bool DeserializeMetaFromProto(const NKikimrKqp::TEvRemoteCostData::TIntervalMeta& proto) {
        SetIntervalSkipped(proto.GetIntervalSkipped());
        SetMarkIncluded(proto.GetMarkIncluded());
        return true;
    }
};

class TKeyRanges {
private:
    std::shared_ptr<arrow::Schema> KeyColumns;
    std::vector<TRangeMark> RangeMarks;
    bool LeftBorderOpened = false;
public:
    TKeyRanges();

    TKeyRanges& Reserve(const ui32 num) {
        RangeMarks.reserve(num);
        return *this;
    }

    TString ToString() const;

    size_t ColumnsCount() const {
        return KeyColumns ? KeyColumns->num_fields() : 0;
    }
    const arrow::Schema& GetKeyColumns() const {
        return *KeyColumns;
    }
    const std::vector<TRangeMark>& GetRangeMarks() const {
        return RangeMarks;
    }
    std::vector<TRangeMark>& MutableRangeMarks() {
        return RangeMarks;
    }
    TKeyRanges& InitColumns(std::shared_ptr<arrow::Schema> schema) {
        KeyColumns = schema;
        return *this;
    }
    TKeyRanges& SetLeftBorderOpened(const bool value) {
        LeftBorderOpened = value;
        return *this;
    }
    bool IsLeftBorderOpened() const {
        return LeftBorderOpened;
    }
    bool AddRangeIfGrow(TRangeMark&& range);
    bool AddRangeIfNotLess(TRangeMark&& range);

    NKikimrKqp::TEvRemoteCostData::TCostInfo SerializeToProto() const;
    bool DeserializeFromProto(const NKikimrKqp::TEvRemoteCostData::TCostInfo& proto);

};

class TCostsOperator {
private:
    const TVector<TGranuleRecord>& GranuleRecords;
    const TIndexInfo& IndexInfo;
    TRangeMark BuildMarkFromPredicate(const std::shared_ptr<NOlap::TPredicate>& p) const;
    TKeyMark BuildMarkFromGranule(const TGranuleRecord& record) const;
    static ui64 ExtractKey(const TString& key);
public:
    TCostsOperator(const TVector<TGranuleRecord>& granuleRecords, const TIndexInfo& indexInfo)
        : GranuleRecords(granuleRecords)
        , IndexInfo(indexInfo)
    {

    }
    void FillRangeMarks(TKeyRanges& result, const std::shared_ptr<NOlap::TPredicate>& left, const std::shared_ptr<NOlap::TPredicate>& right) const;
};
}
