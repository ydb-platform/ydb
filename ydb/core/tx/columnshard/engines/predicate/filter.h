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
    virtual ui64 DoGetSourceId() const = 0;
    virtual ui64 DoGetSourceRecordsCount() const = 0;

public:
    virtual ~ICursorEntity() = default;

    ui64 GetSourceId() const {
        return DoGetSourceId();
    }

    ui64 GetEntityId() const {
        return DoGetEntityId();
    }

    ui64 GetSourceRecordsCount() const {
        return DoGetSourceRecordsCount();
    }
};

// Which oneof a cursor travels in. Nodes older than the SourcesSorting field recover the sources
// order from the tag alone, so it has to keep matching the order the position was taken in.
enum class ECursorTag {
    PkOrderedSources,
    SourceIdOrderedSources,
};

inline ECursorTag LegacyCursorTag(const NKikimrKqp::TEvKqpScanCursor::ESourcesSorting sorting) {
    return sorting == NKikimrKqp::TEvKqpScanCursor::SOURCE_ID_ASC ? ECursorTag::SourceIdOrderedSources : ECursorTag::PkOrderedSources;
}

inline TString CursorImplementationName(const NKikimrKqp::TEvKqpScanCursor::ImplementationCase impl) {
    if (const auto* field = NKikimrKqp::TEvKqpScanCursor::descriptor()->FindFieldByNumber((int)impl)) {
        return field->name();
    }
    return "not_set";
}

// Nothing for a message that carries no position at all: no sources order can contradict it.
inline std::optional<ECursorTag> LegacyCursorTagFromProto(const NKikimrKqp::TEvKqpScanCursor::ImplementationCase impl) {
    switch (impl) {
        case NKikimrKqp::TEvKqpScanCursor::kColumnShardSimple:
        case NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardSimple:
            return ECursorTag::PkOrderedSources;
        case NKikimrKqp::TEvKqpScanCursor::kColumnShardNotSortedSimple:
        case NKikimrKqp::TEvKqpScanCursor::kDeprecatedColumnShardNotSortedSimple:
            return ECursorTag::SourceIdOrderedSources;
        case NKikimrKqp::TEvKqpScanCursor::kColumnShardPlain:
        case NKikimrKqp::TEvKqpScanCursor::IMPLEMENTATION_NOT_SET:
            return std::nullopt;
    }
}

class IScanCursor {
private:
    YDB_ACCESSOR_DEF(std::optional<ui64>, TabletId);
    // The sources order this cursor's position was taken in. See the proto field.
    YDB_ACCESSOR_DEF(std::optional<NKikimrKqp::TEvKqpScanCursor::ESourcesSorting>, SourcesSorting);

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const = 0;
    virtual bool DoCheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const = 0;
    virtual bool DoCheckSourceIntervalUsage(const ui32 sourceIdx, const ui32 indexStart, const ui32 recordsCount) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) = 0;
    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const = 0;

protected:
    ECursorTag GetLegacyTag() const {
        AFL_VERIFY(SourcesSorting);
        return LegacyCursorTag(*SourcesSorting);
    }

    static bool CheckRecordIndexIsBorder(const ICursorEntity& entity, const ui32 recordIndex, bool& usage) {
        if (!entity.GetSourceRecordsCount()) {
            usage = false;
        } else {
            AFL_VERIFY(recordIndex <= entity.GetSourceRecordsCount())("index", recordIndex)("count", entity.GetSourceRecordsCount());
            usage = recordIndex < entity.GetSourceRecordsCount();
        }
        return true;
    }

public:
    virtual bool IsInitialized() const = 0;

    virtual ~IScanCursor() = default;

    const std::shared_ptr<NArrow::TSimpleRow>& GetPKCursor() const {
        return DoGetPKCursor();
    }

    bool CheckSourceIntervalUsage(const ui32 sourceIdx, const ui32 indexStart, const ui32 recordsCount) const {
        AFL_VERIFY(IsInitialized());
        return DoCheckSourceIntervalUsage(sourceIdx, indexStart, recordsCount);
    }

    bool CheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const {
        AFL_VERIFY(IsInitialized());
        return DoCheckEntityIsBorder(entity, usage);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) {
        if (proto.HasTabletId()) {
            TabletId = proto.GetTabletId();
        }
        if (proto.HasSourcesSorting()) {
            SourcesSorting = proto.GetSourcesSorting();
        }
        return DoDeserializeFromProto(proto);
    }

    NKikimrKqp::TEvKqpScanCursor SerializeToProto() const {
        NKikimrKqp::TEvKqpScanCursor result;
        if (TabletId) {
            result.SetTabletId(*TabletId);
        }
        if (SourcesSorting) {
            result.SetSourcesSorting(*SourcesSorting);
        }
        DoSerializeToProto(result);
        return result;
    }
};

// A position in the sources array: which source, and how far into it. The index means nothing without
// the sources order the cursor carries.
class TSourceIndexScanCursor: public IScanCursor {
private:
    std::optional<ui32> SourceIdx;
    ui32 RecordIndex = 0;
    std::optional<ui64> PortionId;
    std::shared_ptr<NArrow::TSimpleRow> PrimaryKey;

    template <class TProto>
    void FillProto(TProto& data) const {
        data.SetSourceIdx(*SourceIdx);
        data.SetStartRecordIndex(RecordIndex);
        if (PortionId) {
            data.SetOptionalPortionId(*PortionId);
        }
    }

    template <class TProto>
    TConclusionStatus ParseProto(const TProto& data) {
        if (!data.HasSourceIdx()) {
            return TConclusionStatus::Fail("incorrect source index for cursor initialization");
        }
        if (!data.HasStartRecordIndex()) {
            return TConclusionStatus::Fail("incorrect record index for cursor initialization");
        }
        SourceIdx = data.GetSourceIdx();
        RecordIndex = data.GetStartRecordIndex();
        if (data.HasOptionalPortionId()) {
            PortionId = data.GetOptionalPortionId();
        }
        return TConclusionStatus::Success();
    }

    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const override {
        AFL_VERIFY(SourceIdx);
        switch (GetLegacyTag()) {
            case ECursorTag::PkOrderedSources:
                return FillProto(*proto.MutableColumnShardSimple());
            case ECursorTag::SourceIdOrderedSources:
                return FillProto(*proto.MutableColumnShardNotSortedSimple());
        }
    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) override {
        if (proto.HasColumnShardSimple()) {
            return ParseProto(proto.GetColumnShardSimple());
        }
        if (proto.HasColumnShardNotSortedSimple()) {
            return ParseProto(proto.GetColumnShardNotSortedSimple());
        }
        return TConclusionStatus::Fail("absent source index cursor data");
    }

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const override {
        return PrimaryKey;
    }

    virtual bool IsInitialized() const override {
        return !!SourceIdx;
    }

    virtual bool DoCheckSourceIntervalUsage(const ui32 sourceIdx, const ui32 indexStart, const ui32 recordsCount) const override {
        AFL_VERIFY(SourceIdx);
        AFL_VERIFY(sourceIdx == *SourceIdx);
        if (indexStart >= RecordIndex) {
            return true;
        }
        AFL_VERIFY(indexStart + recordsCount <= RecordIndex);
        return false;
    }

    virtual bool DoCheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const override {
        AFL_VERIFY(SourceIdx);
        if (*SourceIdx != entity.GetEntityId()) {
            return false;
        }
        // Identity before position: a slot number only names this source while the sources set is the one
        // the cursor was taken on, and comparing a record index against another source explains nothing.
        AFL_VERIFY(!PortionId || *PortionId == entity.GetSourceId())("source_idx", *SourceIdx)("cursor_portion_id", *PortionId)(
                                               "found_source_id", entity.GetSourceId());
        return CheckRecordIndexIsBorder(entity, RecordIndex, usage);
    }

public:
    TSourceIndexScanCursor() = default;

    TSourceIndexScanCursor(const NKikimrKqp::TEvKqpScanCursor::ESourcesSorting sourcesSorting, const std::shared_ptr<NArrow::TSimpleRow>& pk,
        const ui32 sourceIdx, const ui32 recordIndex, const std::optional<ui64>& portionId)
        : SourceIdx(sourceIdx)
        , RecordIndex(recordIndex)
        , PortionId(portionId)
        , PrimaryKey(pk)
    {
        SetSourcesSorting(sourcesSorting);
    }
};

// A position given by the source's own id instead of its place in the sources array. Nothing produces
// this shape any more; it stays to keep reading a cursor made before source indexes were used.
class TSourceIdScanCursor: public IScanCursor {
private:
    std::optional<ui64> SourceId;
    ui32 RecordIndex = 0;

    template <class TProto>
    void FillProto(TProto& data) const {
        data.SetSourceId(*SourceId);
        data.SetStartRecordIndex(RecordIndex);
    }

    template <class TProto>
    TConclusionStatus ParseProto(const TProto& data) {
        if (!data.HasSourceId()) {
            return TConclusionStatus::Fail("incorrect source id for cursor initialization");
        }
        if (!data.HasStartRecordIndex()) {
            return TConclusionStatus::Fail("incorrect record index for cursor initialization");
        }
        SourceId = data.GetSourceId();
        RecordIndex = data.GetStartRecordIndex();
        return TConclusionStatus::Success();
    }

    virtual void DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const override {
        AFL_VERIFY(SourceId);
        switch (GetLegacyTag()) {
            case ECursorTag::PkOrderedSources:
                return FillProto(*proto.MutableDeprecatedColumnShardSimple());
            case ECursorTag::SourceIdOrderedSources:
                return FillProto(*proto.MutableDeprecatedColumnShardNotSortedSimple());
        }
    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) override {
        if (proto.HasDeprecatedColumnShardSimple()) {
            return ParseProto(proto.GetDeprecatedColumnShardSimple());
        }
        if (proto.HasDeprecatedColumnShardNotSortedSimple()) {
            return ParseProto(proto.GetDeprecatedColumnShardNotSortedSimple());
        }
        return TConclusionStatus::Fail("absent source id cursor data");
    }

    virtual const std::shared_ptr<NArrow::TSimpleRow>& DoGetPKCursor() const override {
        return Default<std::shared_ptr<NArrow::TSimpleRow>>();
    }

    virtual bool IsInitialized() const override {
        return !!SourceId;
    }

    virtual bool DoCheckSourceIntervalUsage(const ui32 /*sourceIdx*/, const ui32 indexStart, const ui32 recordsCount) const override {
        AFL_VERIFY(SourceId);
        if (indexStart >= RecordIndex) {
            return true;
        }
        AFL_VERIFY(indexStart + recordsCount <= RecordIndex);
        return false;
    }

    virtual bool DoCheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const override {
        AFL_VERIFY(SourceId);
        if (*SourceId != entity.GetSourceId()) {
            return false;
        }
        return CheckRecordIndexIsBorder(entity, RecordIndex, usage);
    }

public:
    TSourceIdScanCursor() = default;
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

    virtual bool DoCheckSourceIntervalUsage(const ui32 /*sourceIdx*/, const ui32 /*indexStart*/, const ui32 /*recordsCount*/) const override {
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
            AFL_VERIFY(columns.size() >= NumColumns)("schema", columns.size())("predicate", NumColumns);
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
