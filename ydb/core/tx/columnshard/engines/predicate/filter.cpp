#include "filter.h"

#include <ydb/core/formats/arrow/serializer/native.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NOlap {

std::optional<ui32> TPKRangesFilter::GetFilteredCountLimit(const std::shared_ptr<arrow::Schema>& pkSchema) {
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

NKikimr::NArrow::TColumnFilter TPKRangesFilter::BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& data) const {
    if (SortedRanges.empty()) {
        return NArrow::TColumnFilter::BuildAllowFilter();
    }

    auto result = NArrow::TColumnFilter::BuildDenyFilter();
    for (const auto& range : SortedRanges) {
        result = result.Or(range.BuildFilter(data));
    }
    return result;
}

TConclusionStatus TPKRangesFilter::Add(
    std::shared_ptr<NOlap::TPredicate> f, std::shared_ptr<NOlap::TPredicate> t, const std::shared_ptr<arrow::Schema>& pkSchema) {
    if ((!f || f->Empty()) && (!t || t->Empty())) {
        return TConclusionStatus::Success();
    }
    auto fromContainerConclusion = TPredicateContainer::BuildPredicateFrom(f, pkSchema);
    if (fromContainerConclusion.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "incorrect from container")(
            "from", fromContainerConclusion.GetErrorMessage());
        return fromContainerConclusion;
    }
    auto toContainerConclusion = TPredicateContainer::BuildPredicateTo(t, pkSchema);
    if (toContainerConclusion.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "incorrect to container")(
            "from", toContainerConclusion.GetErrorMessage());
        return toContainerConclusion;
    }
    if (SortedRanges.size() && !FakeRanges) {
        if (fromContainerConclusion->CrossRanges(SortedRanges.back().GetPredicateTo())) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not sorted sequence");
            return TConclusionStatus::Fail("not sorted sequence");
        }
    }
    auto pkRangeFilterConclusion = TPKRangeFilter::Build(fromContainerConclusion.DetachResult(), toContainerConclusion.DetachResult());
    if (pkRangeFilterConclusion.IsFail()) {
        return pkRangeFilterConclusion;
    }
    if (FakeRanges) {
        FakeRanges = false;
        SortedRanges.clear();
    }
    SortedRanges.emplace_back(pkRangeFilterConclusion.DetachResult());
    return TConclusionStatus::Success();
}

std::set<std::string> TPKRangesFilter::GetColumnNames() const {
    std::set<std::string> result;
    for (auto&& i : SortedRanges) {
        for (auto&& c : i.GetColumnNames()) {
            result.emplace(c);
        }
    }
    return result;
}


TString TPKRangesFilter::DebugString() const {
    if (SortedRanges.empty()) {
        return "no_ranges";
    } else {
        TStringBuilder sb;
        for (auto&& i : SortedRanges) {
            sb << " range{" << i.DebugString() << "}";
        }
        return sb;
    }
}

std::set<ui32> TPKRangesFilter::GetColumnIds(const TIndexInfo& indexInfo) const {
    std::set<ui32> result;
    for (auto&& i : SortedRanges) {
        for (auto&& c : i.GetColumnIds(indexInfo)) {
            result.emplace(c);
        }
    }
    return result;
}

bool TPKRangesFilter::CheckPoint(const NArrow::TSimpleRow& point) const {
    for (auto&& i : SortedRanges) {
        if (i.CheckPoint(point)) {
            return true;
        }
    }
    return SortedRanges.empty();
}

TPKRangeFilter::EUsageClass TPKRangesFilter::GetUsageClass(const NArrow::TSimpleRow& start, const NArrow::TSimpleRow& end) const {
    if (SortedRanges.empty()) {
        return TPKRangeFilter::EUsageClass::FullUsage;
    }

    const TPredicateContainer startPredicate = TPredicateContainer::BuildPredicateFrom(
        std::make_shared<TPredicate>(NKernels::EOperation::GreaterEqual, start.ToBatch()), start.GetSchema())
                                                   .DetachResult();
    const auto rangesBegin = std::lower_bound(
        SortedRanges.begin(), SortedRanges.end(), startPredicate, [](const TPKRangeFilter& range, const TPredicateContainer& predicate) {
            return !range.GetPredicateTo().CrossRanges(predicate);
        });

    if (rangesBegin == SortedRanges.end()) {
        return TPKRangeFilter::EUsageClass::NoUsage;
    }
    return rangesBegin->GetUsageClass(start, end);
}

TPKRangesFilter::TPKRangesFilter() {
    auto range = TPKRangeFilter::Build(TPredicateContainer::BuildNullPredicateFrom(), TPredicateContainer::BuildNullPredicateTo());
    Y_ABORT_UNLESS(range.IsSuccess());
    SortedRanges.emplace_back(range.DetachResult());
}

std::shared_ptr<arrow::RecordBatch> TPKRangesFilter::SerializeToRecordBatch(const std::shared_ptr<arrow::Schema>& pkSchema) const {
    auto fullSchema = NArrow::TStatusValidator::GetValid(
        pkSchema->AddField(pkSchema->num_fields(), std::make_shared<arrow::Field>(".ydb_operation_type", arrow::uint32())));
    auto builders = NArrow::MakeBuilders(fullSchema, SortedRanges.size() * 2);
    for (auto&& i : SortedRanges) {
        i.GetPredicateFrom().GetReplaceKey()->AddToBuilders(builders).Validate();
        for (ui32 idx = i.GetPredicateFrom().GetReplaceKey()->GetColumnsCount(); idx < (ui32)pkSchema->num_fields(); ++idx) {
            NArrow::TStatusValidator::Validate(builders[idx]->AppendNull());
        }
        NArrow::Append<arrow::UInt32Type>(*builders[pkSchema->num_fields()], (ui32)i.GetPredicateFrom().GetCompareType());

        i.GetPredicateTo().GetReplaceKey()->AddToBuilders(builders).Validate();
        for (ui32 idx = i.GetPredicateTo().GetReplaceKey()->GetColumnsCount(); idx < (ui32)pkSchema->num_fields(); ++idx) {
            NArrow::TStatusValidator::Validate(builders[idx]->AppendNull());
        }
        NArrow::Append<arrow::UInt32Type>(*builders[pkSchema->num_fields()], (ui32)i.GetPredicateTo().GetCompareType());
    }
    return arrow::RecordBatch::Make(fullSchema, SortedRanges.size() * 2, NArrow::Finish(std::move(builders)));
}

std::shared_ptr<NKikimr::NOlap::TPKRangesFilter> TPKRangesFilter::BuildFromRecordBatchLines(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::shared_ptr<TPKRangesFilter> result = std::make_shared<TPKRangesFilter>();
    for (ui32 i = 0; i < batch->num_rows(); ++i) {
        auto batchRow = batch->Slice(i, 1);
        auto pFrom = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::GreaterEqual, batchRow);
        auto pTo = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::LessEqual, batchRow);
        result->Add(pFrom, pTo, batch->schema()).Validate();
    }
    return result;
}

std::shared_ptr<NKikimr::NOlap::TPKRangesFilter> TPKRangesFilter::BuildFromRecordBatchFull(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& pkSchema) {
    std::shared_ptr<TPKRangesFilter> result = std::make_shared<TPKRangesFilter>();
    auto pkBatch = NArrow::TColumnOperator().Adapt(batch, pkSchema).DetachResult();
    auto c = batch->GetColumnByName(".ydb_operation_type");
    AFL_VERIFY(c);
    AFL_VERIFY(c->type_id() == arrow::Type::UINT32);
    auto cUi32 = static_pointer_cast<arrow::UInt32Array>(c);
    for (ui32 i = 0; i < batch->num_rows();) {
        std::shared_ptr<NOlap::TPredicate> pFrom;
        std::shared_ptr<NOlap::TPredicate> pTo;
        {
            auto batchRow = TPredicate::CutNulls(batch->Slice(i, 1));
            NKernels::EOperation op = (NKernels::EOperation)cUi32->Value(i);
            if (op == NKernels::EOperation::GreaterEqual || op == NKernels::EOperation::Greater) {
                pFrom = std::make_shared<NOlap::TPredicate>(op, batchRow);
            } else if (op == NKernels::EOperation::Equal) {
                pFrom = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::GreaterEqual, batchRow);
            } else {
                AFL_VERIFY(false);
            }
            if (op != NKernels::EOperation::Equal) {
                ++i;
            }
        }
        {
            auto batchRow = TPredicate::CutNulls(batch->Slice(i, 1));
            NKernels::EOperation op = (NKernels::EOperation)cUi32->Value(i);
            if (op == NKernels::EOperation::LessEqual || op == NKernels::EOperation::Less) {
                pTo = std::make_shared<NOlap::TPredicate>(op, batchRow);
            } else if (op == NKernels::EOperation::Equal) {
                pTo = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::LessEqual, batchRow);
            } else {
                AFL_VERIFY(false);
            }
        }
        result->Add(pFrom, pTo, pkSchema).Validate();
    }
    return result;
}

std::shared_ptr<NKikimr::NOlap::TPKRangesFilter> TPKRangesFilter::BuildFromString(
    const TString& data, const std::shared_ptr<arrow::Schema>& pkSchema) {
    auto batch = NArrow::TStatusValidator::GetValid(NArrow::NSerialization::TNativeSerializer().Deserialize(data));
    return BuildFromRecordBatchFull(batch, pkSchema);
}

TString TPKRangesFilter::SerializeToString(const std::shared_ptr<arrow::Schema>& pkSchema) const {
    return NArrow::NSerialization::TNativeSerializer().SerializeFull(SerializeToRecordBatch(pkSchema));
}

const TPKRangeFilter& TPKRangesFilter::Front() const {
    Y_ABORT_UNLESS(Size());
    return SortedRanges.front();
}

TConclusion<TPKRangesFilter> TPKRangesFilter::BuildFromProto(
    const NKikimrTxDataShard::TEvKqpScan& proto, const std::vector<TNameTypeInfo>& ydbPk, const std::shared_ptr<arrow::Schema>& arrPk) {
    
    TPKRangesFilter result;
    for (auto& protoRange : proto.GetRanges()) {
        auto fromPredicate = std::make_shared<TPredicate>();
        auto toPredicate = std::make_shared<TPredicate>();
        std::tie(*fromPredicate, *toPredicate) = TPredicate::DeserializePredicatesRange(TSerializedTableRange{ protoRange }, ydbPk, arrPk);
        auto status = result.Add(fromPredicate, toPredicate, arrPk);
        if (status.IsFail()) {
            return status;
        }
    }
    return result;
}


bool IScanCursor::CheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const {
    AFL_VERIFY(IsInitialized());
    return DoCheckSourceIntervalUsage(sourceId, indexStart, recordsCount);
}

bool IScanCursor::CheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const {
    AFL_VERIFY(IsInitialized());
    return DoCheckEntityIsBorder(entity, usage);
}

TConclusionStatus IScanCursor::DeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor &proto) {
  if (proto.HasTabletId()) {
    TabletId = proto.GetTabletId();
  }
  return DoDeserializeFromProto(proto);
}

NKikimrKqp::TEvKqpScanCursor IScanCursor::SerializeToProto() const {
  NKikimrKqp::TEvKqpScanCursor result;
  if (TabletId) {
    result.SetTabletId(*TabletId);
  }
  DoSerializeToProto(result);
  return result;
}

void TSimpleScanCursor::DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor& proto) const {
    proto.MutableColumnShardSimple()->SetSourceId(SourceId);
    proto.MutableColumnShardSimple()->SetStartRecordIndex(RecordIndex);
}

bool TSimpleScanCursor::DoCheckEntityIsBorder(const ICursorEntity& entity, bool& usage) const {
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

TConclusionStatus TSimpleScanCursor::DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor& proto) {
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

bool TSimpleScanCursor::DoCheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const {
  AFL_VERIFY(sourceId == SourceId);
  if (indexStart >= RecordIndex) {
    return true;
  }
  AFL_VERIFY(indexStart + recordsCount <= RecordIndex);
  return false;
}

TSimpleScanCursor::TSimpleScanCursor(const std::shared_ptr<NArrow::TSimpleRow> &pk, const ui64 portionId, const ui32 recordIndex)
    : PrimaryKey(pk), SourceId(portionId), RecordIndex(recordIndex) {
}


void TNotSortedSimpleScanCursor::DoSerializeToProto(NKikimrKqp::TEvKqpScanCursor &proto) const {
  auto &data = *proto.MutableColumnShardNotSortedSimple();
  data.SetSourceId(SourceId);
  data.SetStartRecordIndex(RecordIndex);
}

bool TNotSortedSimpleScanCursor::DoCheckEntityIsBorder(const ICursorEntity &entity, bool &usage) const {
  if (SourceId != entity.GetEntityId()) {
    return false;
  }
  if (!entity.GetEntityRecordsCount()) {
    usage = false;
  } else {
    AFL_VERIFY(RecordIndex <= entity.GetEntityRecordsCount())
    ("index", RecordIndex)("count", entity.GetEntityRecordsCount());
    usage = RecordIndex < entity.GetEntityRecordsCount();
  }
  return true;
}

TConclusionStatus TNotSortedSimpleScanCursor::DoDeserializeFromProto(const NKikimrKqp::TEvKqpScanCursor &proto) {
  if (!proto.HasColumnShardNotSortedSimple()) {
    return TConclusionStatus::Fail("absent unsorted cursor data");
  }
  auto &data = proto.GetColumnShardNotSortedSimple();
  if (!data.HasSourceId()) {
    return TConclusionStatus::Fail(
        "incorrect source id for cursor initialization");
  }
  SourceId = data.GetSourceId();
  if (!data.HasStartRecordIndex()) {
    return TConclusionStatus::Fail(
        "incorrect record index for cursor initialization");
  }
  RecordIndex = data.GetStartRecordIndex();
  return TConclusionStatus::Success();
}

bool TNotSortedSimpleScanCursor::DoCheckSourceIntervalUsage(const ui64 sourceId, const ui32 indexStart, const ui32 recordsCount) const {
  AFL_VERIFY(sourceId == SourceId);
  if (indexStart >= RecordIndex) {
    return true;
  }
  AFL_VERIFY(indexStart + recordsCount <= RecordIndex);
  return false;
}


const std::shared_ptr<NArrow::TSimpleRow> & TPlainScanCursor::DoGetPKCursor() const {
  AFL_VERIFY(!!PrimaryKey);
  return PrimaryKey;
}

TPlainScanCursor::TPlainScanCursor(const std::shared_ptr<NArrow::TSimpleRow>& pk)
    : PrimaryKey(pk) {
    AFL_VERIFY(PrimaryKey);
}

} // namespace NKikimr::NOlap
