#include "filter.h"

#include <ydb/core/formats/arrow/serializer/native.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

NKikimr::NArrow::TColumnFilter TPKRangesFilter::BuildFilter(const arrow::Datum& data) const {
    if (SortedRanges.empty()) {
        return NArrow::TColumnFilter::BuildAllowFilter();
    }
    NArrow::TColumnFilter result = SortedRanges.front().BuildFilter(data);
    for (ui32 i = 1; i < SortedRanges.size(); ++i) {
        result = result.Or(SortedRanges[i].BuildFilter(data));
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
        if (ReverseFlag) {
            if (fromContainerConclusion->CrossRanges(SortedRanges.front().GetPredicateTo())) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not sorted sequence");
                return TConclusionStatus::Fail("not sorted sequence");
            }
        } else {
            if (fromContainerConclusion->CrossRanges(SortedRanges.back().GetPredicateTo())) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not sorted sequence");
                return TConclusionStatus::Fail("not sorted sequence");
            }
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
    if (ReverseFlag) {
        SortedRanges.emplace_front(pkRangeFilterConclusion.DetachResult());
    } else {
        SortedRanges.emplace_back(pkRangeFilterConclusion.DetachResult());
    }
    return TConclusionStatus::Success();
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

bool TPKRangesFilter::IsPortionInUsage(const TPortionInfo& info) const {
    for (auto&& i : SortedRanges) {
        if (i.IsPortionInUsage(info)) {
            return true;
        }
    }
    return SortedRanges.empty();
}

bool TPKRangesFilter::CheckPoint(const NArrow::TReplaceKey& point) const {
    for (auto&& i : SortedRanges) {
        if (i.CheckPoint(point)) {
            return true;
        }
    }
    return SortedRanges.empty();
}

TPKRangeFilter::EUsageClass TPKRangesFilter::IsPortionInPartialUsage(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& end) const {
    for (auto&& i : SortedRanges) {
        switch (i.IsPortionInPartialUsage(start, end)) {
            case TPKRangeFilter::EUsageClass::FullUsage:
                return TPKRangeFilter::EUsageClass::FullUsage;
            case TPKRangeFilter::EUsageClass::PartialUsage:
                return TPKRangeFilter::EUsageClass::PartialUsage;
            case TPKRangeFilter::EUsageClass::DontUsage:
                break;
        }
    }
    return TPKRangeFilter::EUsageClass::DontUsage;
}

TPKRangesFilter::TPKRangesFilter(const bool reverse)
    : ReverseFlag(reverse) {
    auto range = TPKRangeFilter::Build(TPredicateContainer::BuildNullPredicateFrom(), TPredicateContainer::BuildNullPredicateTo());
    Y_ABORT_UNLESS(range);
    SortedRanges.emplace_back(*range);
}

std::shared_ptr<arrow::RecordBatch> TPKRangesFilter::SerializeToRecordBatch(const std::shared_ptr<arrow::Schema>& pkSchema) const {
    auto fullSchema = NArrow::TStatusValidator::GetValid(
        pkSchema->AddField(pkSchema->num_fields(), std::make_shared<arrow::Field>(".ydb_operation_type", arrow::uint32())));
    auto builders = NArrow::MakeBuilders(fullSchema, SortedRanges.size() * 2);
    for (auto&& i : SortedRanges) {
        for (ui32 idx = 0; idx < (ui32)pkSchema->num_fields(); ++idx) {
            if (idx < i.GetPredicateFrom().GetReplaceKey()->Size()) {
                AFL_VERIFY(NArrow::Append(
                    *builders[idx], i.GetPredicateFrom().GetReplaceKey()->Column(idx), i.GetPredicateFrom().GetReplaceKey()->GetPosition()));
            } else {
                NArrow::TStatusValidator::Validate(builders[idx]->AppendNull());
            }
        }
        NArrow::Append<arrow::UInt32Type>(*builders[pkSchema->num_fields()], (ui32)i.GetPredicateFrom().GetCompareType());

        for (ui32 idx = 0; idx < (ui32)pkSchema->num_fields(); ++idx) {
            if (idx < i.GetPredicateTo().GetReplaceKey()->Size()) {
                AFL_VERIFY(NArrow::Append(
                    *builders[idx], i.GetPredicateTo().GetReplaceKey()->Column(idx), i.GetPredicateTo().GetReplaceKey()->GetPosition()));
            } else {
                NArrow::TStatusValidator::Validate(builders[idx]->AppendNull());
            }
        }
        NArrow::Append<arrow::UInt32Type>(*builders[pkSchema->num_fields()], (ui32)i.GetPredicateTo().GetCompareType());
    }
    return arrow::RecordBatch::Make(fullSchema, SortedRanges.size() * 2, NArrow::Finish(std::move(builders)));
}

std::shared_ptr<NKikimr::NOlap::TPKRangesFilter> TPKRangesFilter::BuildFromRecordBatchLines(
    const std::shared_ptr<arrow::RecordBatch>& batch, const bool reverse) {
    std::shared_ptr<TPKRangesFilter> result = std::make_shared<TPKRangesFilter>(reverse);
    for (ui32 i = 0; i < batch->num_rows(); ++i) {
        auto batchRow = batch->Slice(i, 1);
        auto pFrom = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::GreaterEqual, batchRow);
        auto pTo = std::make_shared<NOlap::TPredicate>(NKernels::EOperation::LessEqual, batchRow);
        result->Add(pFrom, pTo, batch->schema()).Validate();
    }
    return result;
}

std::shared_ptr<NKikimr::NOlap::TPKRangesFilter> TPKRangesFilter::BuildFromRecordBatchFull(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& pkSchema, const bool reverse) {
    std::shared_ptr<TPKRangesFilter> result = std::make_shared<TPKRangesFilter>(reverse);
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
    const TString& data, const std::shared_ptr<arrow::Schema>& pkSchema, const bool reverse) {
    auto batch = NArrow::TStatusValidator::GetValid(NArrow::NSerialization::TNativeSerializer().Deserialize(data));
    return BuildFromRecordBatchFull(batch, pkSchema, reverse);
}

TString TPKRangesFilter::SerializeToString(const std::shared_ptr<arrow::Schema>& pkSchema) const {
    return NArrow::NSerialization::TNativeSerializer().SerializeFull(SerializeToRecordBatch(pkSchema));
}

}   // namespace NKikimr::NOlap
