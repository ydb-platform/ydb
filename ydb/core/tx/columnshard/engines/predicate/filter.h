#pragma once
#include "range.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <deque>

namespace NKikimr::NOlap {

class TPKRangesFilter {
private:
    bool FakeRanges = true;
    std::deque<TPKRangeFilter> SortedRanges;

public:
    TPKRangesFilter();

    std::optional<ui32> GetFilteredCountLimit(const std::shared_ptr<arrow::Schema>& pkSchema);

    [[nodiscard]] TConclusionStatus Add(
        std::shared_ptr<NOlap::TPredicate> f, std::shared_ptr<NOlap::TPredicate> t, const std::shared_ptr<arrow::Schema>& pkSchema);
    std::shared_ptr<arrow::RecordBatch> SerializeToRecordBatch(const std::shared_ptr<arrow::Schema>& pkSchema) const;
    TString SerializeToString(const std::shared_ptr<arrow::Schema>& pkSchema) const;

    bool IsEmpty() const {
        return SortedRanges.empty() || FakeRanges;
    }

    const TPKRangeFilter& Front() const;

    size_t Size() const {
        return SortedRanges.size();
    }

    std::deque<TPKRangeFilter>::const_iterator begin() const {
        return SortedRanges.begin();
    }

    std::deque<TPKRangeFilter>::const_iterator end() const {
        return SortedRanges.end();
    }

    bool IsUsed(const TPortionInfo& info) const {
        return IsUsed(info.IndexKeyStart(), info.IndexKeyEnd());
    }

    bool IsUsed(const NArrow::TSimpleRow& start, const NArrow::TSimpleRow& end) const {
        return GetUsageClass(start, end) != TPKRangeFilter::EUsageClass::NoUsage;
    }
    TPKRangeFilter::EUsageClass GetUsageClass(const NArrow::TSimpleRow& start, const NArrow::TSimpleRow& end) const;
    bool CheckPoint(const NArrow::TSimpleRow& point) const;

    NArrow::TColumnFilter BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& data) const;

    std::set<std::string> GetColumnNames() const;

    TString DebugString() const;

    std::set<ui32> GetColumnIds(const TIndexInfo& indexInfo) const;

    static std::shared_ptr<TPKRangesFilter> BuildFromRecordBatchLines(const std::shared_ptr<arrow::RecordBatch>& batch);

    static std::shared_ptr<TPKRangesFilter> BuildFromRecordBatchFull(
        const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& pkSchema);

    static std::shared_ptr<TPKRangesFilter> BuildFromString(const TString& data, const std::shared_ptr<arrow::Schema>& pkSchema);

    static TConclusion<TPKRangesFilter> BuildFromProto(
        const NKikimrTxDataShard::TEvKqpScan& proto, const std::vector<TNameTypeInfo>& ydbPk, const std::shared_ptr<arrow::Schema>& arrPk);
};

class ICursorEntity {
private:
    virtual ui64 DoGetEntityId() const = 0;
    virtual ui64 DoGetEntityRecordsCount() const = 0;

public:
    virtual ~ICursorEntity() = default;

    ui64 GetEntityId() const {
        return DoGetEntityId();
    }
    ui64 GetEntityRecordsCount() const {
        return DoGetEntityRecordsCount();
    }
};

class IScanCursor {
private:
    YDB_ACCESSOR_DEF(std::optional<ui64>, TabletId);

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const = 0;
    virtual bool DoCheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const = 0;
    virtual bool DoCheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) = 0;
    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const = 0;

public:
    virtual bool IsInitialized() const = 0;

    virtual ~IScanCursor() = default;

    const std::shared_ptr<NArrow::TSimpleRow>& GetPKCursor() const {
        return DoGetPKCursor();
    }

    bool CheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const;

    bool CheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const;

    TConclusionStatus DeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor &proto);

    NKikimrKqp::TEvKqpScanCursor SerializeToProto() const;
};

class TSimpleScanCursor: public IScanCursor {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TSimpleRow>, PrimaryKey);
    YDB_READONLY(ui64, SourceId, 0);
    YDB_READONLY(ui32, RecordIndex, 0);

    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const override;

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const override {
        return PrimaryKey;
    }

    virtual bool IsInitialized() const override {
        return !!SourceId;
    }

    virtual bool DoCheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) override;

    virtual bool DoCheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const override;

  public:
    TSimpleScanCursor() = default;

    TSimpleScanCursor(const std::shared_ptr<NArrow::TSimpleRow> &pk, const ui64 portionId, const ui32 recordIndex);
};

class TNotSortedSimpleScanCursor: public TSimpleScanCursor {
private:
    YDB_READONLY(ui64, SourceId, 0);
    YDB_READONLY(ui32, RecordIndex, 0);

    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor &proto) const override;

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const override {
        return Default<std::shared_ptr<NArrow::TSimpleRow>>();
    }

    virtual bool IsInitialized() const override {
        return !!SourceId;
    }

    virtual bool DoCheckEntityIsBorder(const ICursorEntity &entity, bool &usage) const override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor &proto) override;

    virtual bool DoCheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const override;

  public:
    TNotSortedSimpleScanCursor() = default;

    TNotSortedSimpleScanCursor(const ui64 portionId, const ui32 recordIndex)
        : SourceId(portionId)
        , RecordIndex(recordIndex) {
    }
};

class TPlainScanCursor: public IScanCursor {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TSimpleRow>, PrimaryKey);

    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const override {
        *proto.MutableColumnShardPlain() = {};
    }

    virtual bool IsInitialized() const override {
        return !!PrimaryKey;
    }

    virtual const std::shared_ptr<NArrow::TSimpleRow> & DoGetPKCursor() const override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& /*proto*/) override {
        return TConclusionStatus::Success();
    }

    virtual bool DoCheckEntityIsBorder(const ICursorEntity& /*entity*/, bool& usage) const override {
        usage = true;
        return true;
    }

    virtual bool DoCheckSourceIntervalUsage(const ui64 /*sourceId*/, const ui32 /*indexStart*/, const ui32 /*recordsCount*/) const override {
        return true;
    }

public:
    TPlainScanCursor() = default;

    TPlainScanCursor(const std::shared_ptr<NArrow::TSimpleRow>& pk);
};

}   // namespace NKikimr::NOlap
