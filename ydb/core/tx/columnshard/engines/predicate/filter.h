#pragma once
#include "range.h"
#include <deque>

namespace NKikimr::NOlap {

class TPKRangesFilter {
private:
    bool FakeRanges = true;
    std::deque<TPKRangeFilter> SortedRanges;
    bool ReverseFlag = false;

public:
    TPKRangesFilter(const bool reverse);

    [[nodiscard]] TConclusionStatus Add(
        std::shared_ptr<NOlap::TPredicate> f, std::shared_ptr<NOlap::TPredicate> t, const std::shared_ptr<arrow::Schema>& pkSchema);
    std::shared_ptr<arrow::RecordBatch> SerializeToRecordBatch(const std::shared_ptr<arrow::Schema>& pkSchema) const;
    TString SerializeToString(const std::shared_ptr<arrow::Schema>& pkSchema) const;

    bool IsEmpty() const {
        return SortedRanges.empty() || FakeRanges;
    }

    bool IsReverse() const {
        return ReverseFlag;
    }

    const TPKRangeFilter& Front() const {
        Y_ABORT_UNLESS(Size());
        return SortedRanges.front();
    }

    size_t Size() const {
        return SortedRanges.size();
    }

    std::deque<TPKRangeFilter>::const_iterator begin() const {
        return SortedRanges.begin();
    }

    std::deque<TPKRangeFilter>::const_iterator end() const {
        return SortedRanges.end();
    }

    bool IsPortionInUsage(const TPortionInfo& info) const;
    TPKRangeFilter::EUsageClass IsPortionInPartialUsage(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& end) const;
    bool CheckPoint(const NArrow::TReplaceKey& point) const;

    NArrow::TColumnFilter BuildFilter(const arrow::Datum& data) const;

    std::set<std::string> GetColumnNames() const {
        std::set<std::string> result;
        for (auto&& i : SortedRanges) {
            for (auto&& c : i.GetColumnNames()) {
                result.emplace(c);
            }
        }
        return result;
    }

    TString DebugString() const;

    std::set<ui32> GetColumnIds(const TIndexInfo& indexInfo) const;

    static std::shared_ptr<TPKRangesFilter> BuildFromRecordBatchLines(const std::shared_ptr<arrow::RecordBatch>& batch, const bool reverse);

    static std::shared_ptr<TPKRangesFilter> BuildFromRecordBatchFull(
        const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& pkSchema, const bool reverse);
    static std::shared_ptr<TPKRangesFilter> BuildFromString(
        const TString& data, const std::shared_ptr<arrow::Schema>& pkSchema, const bool reverse);

    template <class TProto>
    static TConclusion<TPKRangesFilter> BuildFromProto(const TProto& proto, const bool reverse, const std::vector<TNameTypeInfo>& ydbPk) {
        TPKRangesFilter result(reverse);
        for (auto& protoRange : proto.GetRanges()) {
            TSerializedTableRange range(protoRange);
            auto fromPredicate = std::make_shared<TPredicate>();
            auto toPredicate = std::make_shared<TPredicate>();
            TSerializedTableRange serializedRange(protoRange);
            std::tie(*fromPredicate, *toPredicate) = TPredicate::DeserializePredicatesRange(serializedRange, ydbPk);
            auto status = result.Add(fromPredicate, toPredicate, NArrow::TStatusValidator::GetValid(NArrow::MakeArrowSchema(ydbPk)));
            if (status.IsFail()) {
                return status;
            }
        }
        return result;
    }
};

}
