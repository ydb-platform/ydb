#pragma once
#include "range.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/library/formats/arrow/size_calcer.h>

#include <deque>

namespace NKikimr::NOlap {

class TPKRangesFilter {
    friend class TRangesBuilder;

    class TMemoryTrackingGuard: TNonCopyable {
    private:
        YDB_READONLY_DEF(ui64, Bytes);

    public:
        TMemoryTrackingGuard(const ui64 mem)
            : Bytes(mem)
        {
            TotalFiltersMemorySize += mem;
        }
        ~TMemoryTrackingGuard() {
            TotalFiltersMemorySize.Sub(Bytes);
        }
    };

private:
    bool FakeRanges = true;
    std::deque<TPKRangeFilter> SortedRanges;
    std::shared_ptr<arrow::RecordBatch> Data;
    TMemoryTrackingGuard MemoryGuard;

    [[nodiscard]] TConclusionStatus Add(std::optional<NOlap::TPredicate> f, std::optional<NOlap::TPredicate> t);
    TPKRangesFilter()
        : MemoryGuard(0)
    {
    }
    TPKRangesFilter(const std::shared_ptr<arrow::RecordBatch>& data);

    static inline TPositiveControlInteger TotalFiltersMemorySize;

public:
    TPKRangesFilter(TPKRangesFilter&& other)
        : FakeRanges(other.FakeRanges)
        , SortedRanges(std::move(other.SortedRanges))
        , Data(other.Data)
        , MemoryGuard(other.MemoryGuard.GetBytes())
    {
    }

    std::optional<ui32> GetFilteredCountLimit(const std::shared_ptr<arrow::Schema>& pkSchema) {
        if (SortedRanges.empty()) {
            return std::nullopt;
        }
        ui32 result = 0;
        for (auto&& i : SortedRanges) {
            if (i.IsPointRange(pkSchema)) {
                ++result;
            } else {
                return std::nullopt;
            }
        }
        return result;
    }

    std::shared_ptr<arrow::RecordBatch> SerializeToRecordBatch(const std::shared_ptr<arrow::Schema>& pkSchema) const;
    TString SerializeToString(const std::shared_ptr<arrow::Schema>& pkSchema) const;

    bool IsEmpty() const {
        return SortedRanges.empty() || FakeRanges;
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

    bool IsUsed(const TPortionInfo& info) const {
        return IsUsed(info.IndexKeyStart().BuildSortablePosition(), info.IndexKeyEnd().BuildSortablePosition());
    }

    bool IsUsed(const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& end) const {
        return GetUsageClass(start, end) != TPKRangeFilter::EUsageClass::NoUsage;
    }
    TPKRangeFilter::EUsageClass GetUsageClass(
        const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& end) const;
    bool CheckPoint(const NArrow::NMerger::TSortableBatchPosition& point) const;

    NArrow::TColumnFilter BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& data) const;

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

    static std::shared_ptr<TPKRangesFilter> BuildFromRecordBatchLines(const std::shared_ptr<arrow::RecordBatch>& batch);

    static std::shared_ptr<TPKRangesFilter> BuildFromRecordBatchFull(
        const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& pkSchema);
    static std::shared_ptr<TPKRangesFilter> BuildFromString(const TString& data, const std::shared_ptr<arrow::Schema>& pkSchema);
    static TPKRangesFilter BuildEmpty() {
        return TPKRangesFilter();
    }

    static TConclusion<TPKRangesFilter> BuildFromProto(
        const NKikimrTxDataShard::TEvKqpScan& proto, const std::vector<TNameTypeInfo>& ydbPk, const std::shared_ptr<arrow::Schema>& arrPk);

    size_t GetMemorySize() const {
        return NArrow::GetBatchMemorySize(Data);
    }
    static size_t GetFiltersTotalMemorySize() {
        return TotalFiltersMemorySize.Val();
    }
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

    bool CheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const {
        AFL_VERIFY(IsInitialized());
        return DoCheckSourceIntervalUsage(sourceId, indexStart, recordsCount);
    }

    bool CheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const {
        AFL_VERIFY(IsInitialized());
        return DoCheckEntityIsBorder(entity, usage);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) {
        if (proto.HasTabletId()) {
            TabletId = proto.GetTabletId();
        }
        return DoDeserializeFromProto(proto);
    }

    NKikimrKqp::TEvKqpScanCursor SerializeToProto() const {
        NKikimrKqp::TEvKqpScanCursor result;
        if (TabletId) {
            result.SetTabletId(*TabletId);
        }
        DoSerializeToProto(result);
        return result;
    }
};

class TSimpleScanCursor: public IScanCursor {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TSimpleRow>, PrimaryKey);
    YDB_READONLY(ui64, SourceId, 0);
    YDB_READONLY(ui32, RecordIndex, 0);

    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const override {
        proto.MutableColumnShardSimple()->SetSourceId(SourceId);
        proto.MutableColumnShardSimple()->SetStartRecordIndex(RecordIndex);
    }

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const override {
        return PrimaryKey;
    }

    virtual bool IsInitialized() const override {
        return !!SourceId;
    }

    virtual bool DoCheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const override {
        if (SourceId != entity.GetEntityId()) {
            return false;
        }
        if (!entity.GetEntityRecordsCount()) {
            usage = false;
        } else {
            AFL_VERIFY(RecordIndex <= entity.GetEntityRecordsCount());
            usage = RecordIndex < entity.GetEntityRecordsCount();
        }
        return true;
    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) override {
        if (!proto.HasColumnShardSimple()) {
            return TConclusionStatus::Fail("absent sorted cursor data");
        }
        if (!proto.GetColumnShardSimple().HasSourceId()) {
            return TConclusionStatus::Fail("incorrect source id for cursor initialization");
        }
        SourceId = proto.GetColumnShardSimple().GetSourceId();
        if (!proto.GetColumnShardSimple().HasStartRecordIndex()) {
            return TConclusionStatus::Fail("incorrect record index for cursor initialization");
        }
        RecordIndex = proto.GetColumnShardSimple().GetStartRecordIndex();
        return TConclusionStatus::Success();
    }

    virtual bool DoCheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const override {
        AFL_VERIFY(sourceId == SourceId);
        if (indexStart >= RecordIndex) {
            return true;
        }
        AFL_VERIFY(indexStart + recordsCount <= RecordIndex);
        return false;
    }

public:
    TSimpleScanCursor() = default;

    TSimpleScanCursor(const std::shared_ptr<NArrow::TSimpleRow>& pk, const ui64 portionId, const ui32 recordIndex)
        : PrimaryKey(pk)
        , SourceId(portionId)
        , RecordIndex(recordIndex)
    {
    }
};

class TNotSortedSimpleScanCursor: public TSimpleScanCursor {
private:
    YDB_READONLY(ui64, SourceId, 0);
    YDB_READONLY(ui32, RecordIndex, 0);

    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const override {
        auto& data = *proto.MutableColumnShardNotSortedSimple();
        data.SetSourceId(SourceId);
        data.SetStartRecordIndex(RecordIndex);
    }

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const override {
        return Default<std::shared_ptr<NArrow::TSimpleRow>>();
    }

    virtual bool IsInitialized() const override {
        return !!SourceId;
    }

    virtual bool DoCheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const override {
        if (SourceId != entity.GetEntityId()) {
            return false;
        }
        if (!entity.GetEntityRecordsCount()) {
            usage = false;
        } else {
            AFL_VERIFY(RecordIndex <= entity.GetEntityRecordsCount())("index", RecordIndex)("count", entity.GetEntityRecordsCount());
            usage = RecordIndex < entity.GetEntityRecordsCount();
        }
        return true;
    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) override {
        if (!proto.HasColumnShardNotSortedSimple()) {
            return TConclusionStatus::Fail("absent unsorted cursor data");
        }
        auto& data = proto.GetColumnShardNotSortedSimple();
        if (!data.HasSourceId()) {
            return TConclusionStatus::Fail("incorrect source id for cursor initialization");
        }
        SourceId = data.GetSourceId();
        if (!data.HasStartRecordIndex()) {
            return TConclusionStatus::Fail("incorrect record index for cursor initialization");
        }
        RecordIndex = data.GetStartRecordIndex();
        return TConclusionStatus::Success();
    }

    virtual bool DoCheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const override {
        AFL_VERIFY(sourceId == SourceId);
        if (indexStart >= RecordIndex) {
            return true;
        }
        AFL_VERIFY(indexStart + recordsCount <= RecordIndex);
        return false;
    }

public:
    TNotSortedSimpleScanCursor() = default;

    TNotSortedSimpleScanCursor(const ui64 portionId, const ui32 recordIndex)
        : SourceId(portionId)
        , RecordIndex(recordIndex)
    {
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

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const override {
        AFL_VERIFY(!!PrimaryKey);
        return PrimaryKey;
    }

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

    TPlainScanCursor(const std::shared_ptr<NArrow::TSimpleRow>& pk)
        : PrimaryKey(pk)
    {
        AFL_VERIFY(PrimaryKey);
    }
};

class TRangesBuilder {
    class TPredicateInfo {
    private:
        YDB_READONLY_DEF(NKernels::EOperation, Operation);
        YDB_READONLY_DEF(ui32, NumColumns);
        YDB_READONLY_DEF(ui32, RowIndex);

    public:
        TPredicateInfo(const NKernels::EOperation operation, const ui32 numColumns, const ui32 rowIndex)
            : Operation(operation)
            , NumColumns(numColumns)
            , RowIndex(rowIndex)
        {
        }

        std::optional<TPredicate> BuildPredicate(
            const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<arrow::RecordBatch>& batch) const {
            if (!NumColumns) {
                return std::nullopt;
            }
            auto columns = schema->field_names();
            columns.resize(NumColumns);
            return TPredicate(Operation, NArrow::NMerger::TSortableBatchPosition(batch, RowIndex, columns, {}, false));
        }
    };

private:
    const std::vector<NScheme::TTypeInfo> YdbPK;
    const std::shared_ptr<arrow::Schema> ArrPK;
    NArrow::TArrowBatchBuilder BatchBuilder;
    std::vector<std::pair<TPredicateInfo, TPredicateInfo>> RangesInfo;

private:
    static std::vector<NScheme::TTypeInfo> ExtractTypes(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
        std::vector<NScheme::TTypeInfo> types;
        types.reserve(columns.size());
        for (auto& [name, type] : columns) {
            types.push_back(type);
        }
        return types;
    }

    static TConclusion<TCell> MakeDefaultCell(const NScheme::TTypeInfo typeInfo);

public:
    TRangesBuilder(const std::vector<TNameTypeInfo>& ydbPk, const std::shared_ptr<arrow::Schema>& arrPk)
        : YdbPK(ExtractTypes(ydbPk))
        , ArrPK(arrPk)
    {
        AFL_VERIFY((i64)ydbPk.size() == arrPk->num_fields());
        NArrow::TStatusValidator::Validate(BatchBuilder.Start(ydbPk, arrPk));
    }

    void AddRange(TSerializedTableRange&&);
    TConclusion<TPKRangesFilter> Finish();
};

}   // namespace NKikimr::NOlap
