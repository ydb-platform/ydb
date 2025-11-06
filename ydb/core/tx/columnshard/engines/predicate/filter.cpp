#include "filter.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/serializer/native.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NOlap {

NKikimr::NArrow::TColumnFilter TPKRangesFilter::BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& data) const {
    if (IsEmpty()) {
        return NArrow::TColumnFilter::BuildAllowFilter();
    }

    auto result = NArrow::TColumnFilter::BuildDenyFilter();
    NArrow::NMerger::TRWSortableBatchPosition iterator(data, 0, false);
    bool reachedEnd = false;
    for (const auto& range : SortedRanges) {
        const ui64 initialIdx = iterator.GetPosition();
        const auto findBegin = range.GetPredicateFrom().FindFirstIncluded(iterator);
        const ui64 beginIdx = findBegin ? findBegin->GetPosition() : iterator.GetRecordsCount();

        result.Add(false, beginIdx - initialIdx);
        reachedEnd = !iterator.InitPosition(beginIdx);
        if (reachedEnd) {
            AFL_VERIFY((i64)beginIdx == iterator.GetRecordsCount());
            break;
        }

        const auto findEnd = range.GetPredicateTo().FindFirstExcluded(iterator);
        const ui64 endIdx = findEnd ? findEnd->GetPosition() : iterator.GetRecordsCount();

        result.Add(true, endIdx - beginIdx);
        reachedEnd = !iterator.InitPosition(endIdx);
        if (reachedEnd) {
            AFL_VERIFY((i64)endIdx == iterator.GetRecordsCount());
            break;
        }
    }
    if (!reachedEnd) {
        result.Add(false, iterator.GetRecordsCount() - iterator.GetPosition());
    }
    return result;
}

TConclusionStatus TPKRangesFilter::Add(std::optional<NOlap::TPredicate> f, std::optional<NOlap::TPredicate> t) {
    if ((!f) && (!t)) {
        return TConclusionStatus::Success();
    }
    auto fromContainerConclusion = TPredicateContainer::BuildPredicateFrom(std::move(f));
    if (fromContainerConclusion.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "incorrect from container")(
            "from", fromContainerConclusion.GetErrorMessage());
        return fromContainerConclusion;
    }
    auto toContainerConclusion = TPredicateContainer::BuildPredicateTo(std::move(t));
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

bool TPKRangesFilter::CheckPoint(const NArrow::NMerger::TSortableBatchPosition& point) const {
    for (auto&& i : SortedRanges) {
        if (i.CheckPoint(point)) {
            return true;
        }
    }
    return SortedRanges.empty();
}

TPKRangeFilter::EUsageClass TPKRangesFilter::GetUsageClass(
    const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& end) const {
    if (SortedRanges.empty()) {
        return TPKRangeFilter::EUsageClass::FullUsage;
    }

    const TPredicateContainer startPredicate =
        TPredicateContainer::BuildPredicateFrom(TPredicate(NKernels::EOperation::GreaterEqual, start)).DetachResult();
    const auto rangesBegin = std::lower_bound(
        SortedRanges.begin(), SortedRanges.end(), startPredicate, [](const TPKRangeFilter& range, const TPredicateContainer& predicate) {
            return !range.GetPredicateTo().CrossRanges(predicate);
        });

    if (rangesBegin == SortedRanges.end()) {
        return TPKRangeFilter::EUsageClass::NoUsage;
    }
    return rangesBegin->GetUsageClass(start, end);
}

TPKRangesFilter::TPKRangesFilter(const std::shared_ptr<arrow::RecordBatch>& data)
    : Data(data)
    , MemoryGuard(GetMemorySize())
{
    auto range = TPKRangeFilter::Build(TPredicateContainer::BuildNullPredicateFrom(), TPredicateContainer::BuildNullPredicateTo());
    Y_ABORT_UNLESS(range.IsSuccess());
    SortedRanges.emplace_back(range.DetachResult());
}

std::shared_ptr<arrow::RecordBatch> TPKRangesFilter::SerializeToRecordBatch(const std::shared_ptr<arrow::Schema>& pkSchema) const {
    auto fullSchema = NArrow::TStatusValidator::GetValid(
        pkSchema->AddField(pkSchema->num_fields(), std::make_shared<arrow::Field>(".ydb_operation_type", arrow::uint32())));
    auto builders = NArrow::MakeBuilders(fullSchema, SortedRanges.size() * 2);
    for (auto&& i : SortedRanges) {
        i.GetPredicateFrom().AppendPointTo(builders);
        for (ui32 idx = i.GetPredicateFrom().NumColumns(); idx < (ui32)pkSchema->num_fields(); ++idx) {
            NArrow::TStatusValidator::Validate(builders[idx]->AppendNull());
        }
        NArrow::Append<arrow::UInt32Type>(*builders[pkSchema->num_fields()], (ui32)i.GetPredicateFrom().GetCompareType());

        i.GetPredicateTo().AppendPointTo(builders);
        for (ui32 idx = i.GetPredicateTo().NumColumns(); idx < (ui32)pkSchema->num_fields(); ++idx) {
            NArrow::TStatusValidator::Validate(builders[idx]->AppendNull());
        }
        NArrow::Append<arrow::UInt32Type>(*builders[pkSchema->num_fields()], (ui32)i.GetPredicateTo().GetCompareType());
    }
    return arrow::RecordBatch::Make(fullSchema, SortedRanges.size() * 2, NArrow::Finish(std::move(builders)));
}

std::shared_ptr<NKikimr::NOlap::TPKRangesFilter> TPKRangesFilter::BuildFromRecordBatchLines(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::shared_ptr<TPKRangesFilter> result = std::make_shared<TPKRangesFilter>(TPKRangesFilter(batch));
    for (ui32 i = 0; i < batch->num_rows(); ++i) {
        NArrow::NMerger::TSortableBatchPosition batchRow(batch, i, false);
        result->Add(TPredicate(NKernels::EOperation::GreaterEqual, batchRow), TPredicate(NKernels::EOperation::LessEqual, batchRow)).Validate();
    }
    return result;
}

std::shared_ptr<NKikimr::NOlap::TPKRangesFilter> TPKRangesFilter::BuildFromRecordBatchFull(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& pkSchema) {
    std::shared_ptr<TPKRangesFilter> result = std::make_shared<TPKRangesFilter>(TPKRangesFilter(batch));
    auto pkBatch = NArrow::TColumnOperator().Adapt(batch, pkSchema).DetachResult();
    auto c = batch->GetColumnByName(".ydb_operation_type");
    AFL_VERIFY(c);
    AFL_VERIFY(c->type_id() == arrow::Type::UINT32);
    auto cUi32 = static_pointer_cast<arrow::UInt32Array>(c);
    for (ui32 i = 0; i < batch->num_rows();) {
        NOlap::TPredicate pFrom = [&]() {
            auto batchRow = TPredicate::CutNulls(batch, i, pkSchema);
            NKernels::EOperation op = (NKernels::EOperation)cUi32->Value(i);
            if (op != NKernels::EOperation::Equal) {
                ++i;
            }
            if (op == NKernels::EOperation::GreaterEqual || op == NKernels::EOperation::Greater) {
                return TPredicate(op, batchRow);
            } else if (op == NKernels::EOperation::Equal) {
                return TPredicate(NKernels::EOperation::GreaterEqual, batchRow);
            } else {
                AFL_VERIFY(false);
            }
        }();
        NOlap::TPredicate pTo = [&]()
        {
            auto batchRow = TPredicate::CutNulls(batch, i, pkSchema);
            NKernels::EOperation op = (NKernels::EOperation)cUi32->Value(i);
            if (op == NKernels::EOperation::LessEqual || op == NKernels::EOperation::Less) {
                return TPredicate(op, batchRow);
            } else if (op == NKernels::EOperation::Equal) {
                return TPredicate(NKernels::EOperation::LessEqual, batchRow);
            } else {
                AFL_VERIFY(false);
            }
        }();
        result->Add(std::move(pFrom), std::move(pTo)).Validate();
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

TConclusion<TPKRangesFilter> TPKRangesFilter::BuildFromProto(
    const NKikimrTxDataShard::TEvKqpScan& proto, const std::vector<TNameTypeInfo>& ydbPk, const std::shared_ptr<arrow::Schema>& arrPk) {
    TRangesBuilder builder(ydbPk, arrPk);
    for (const auto& range : proto.GetRanges()) {
        builder.AddRange(TSerializedTableRange(range));
    }
    return builder.Finish();
}

void TRangesBuilder::AddRange(TSerializedTableRange&& range) {
    auto addRow = [this](TConstArrayRef<TCell> cells) -> ui32 {
        std::vector<TCell> cellsWithDefaults;
        ui32 nonNullCount = 0;
        const size_t size = YdbPK.size();
        Y_ASSERT(size <= (size_t)ArrPK->num_fields());
        cellsWithDefaults.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            if (i < cells.size() && !cells[i].IsNull()) {
                cellsWithDefaults.push_back(cells[i]);
                AFL_VERIFY(nonNullCount == i)("non_null", nonNullCount)("i", i);
                ++nonNullCount;
            } else {
                TConclusion<TCell> defaultCell = MakeDefaultCell(YdbPK[i]);
                AFL_VERIFY(!!defaultCell);
                cellsWithDefaults.push_back(defaultCell.DetachResult());
            }
        }
        AFL_VERIFY(cellsWithDefaults.size() == YdbPK.size());
        BatchBuilder.AddRow(NKikimr::TDbTupleRef(), NKikimr::TDbTupleRef(YdbPK.data(), cellsWithDefaults.data(), cellsWithDefaults.size()));

        return nonNullCount;
    };

    const ui32 leftPrefix = addRow(range.From.GetCells());
    const ui32 rightPrefix = addRow(range.To.GetCells());
    AFL_VERIFY(BatchBuilder.Rows());
    AFL_VERIFY(leftPrefix <= YdbPK.size());
    AFL_VERIFY(rightPrefix <= YdbPK.size());

    RangesInfo.emplace_back(TPredicateInfo(range.FromInclusive ? NKernels::EOperation::GreaterEqual : NKernels::EOperation::Greater, leftPrefix,
                                BatchBuilder.Rows() - 2),
        TPredicateInfo(range.ToInclusive ? NKernels::EOperation::LessEqual : NKernels::EOperation::Less, rightPrefix, BatchBuilder.Rows() - 1));
}

TConclusion<TPKRangesFilter> TRangesBuilder::Finish() {
    if (RangesInfo.empty()) {
        return TPKRangesFilter::BuildEmpty();
    }

    auto batch = BatchBuilder.FlushBatch(false, false);
    TPKRangesFilter result(batch);
    auto fieldNames = ArrPK->field_names();
    for (const auto& range : RangesInfo) {
        auto status = result.Add(range.first.BuildPredicate(ArrPK, batch), range.second.BuildPredicate(ArrPK, batch));
        if (status.IsFail()) {
            return status;
        }
    }
    return result;
}

namespace {
template <typename T>
TCell MakeDefaultCellByPrimitiveType() {
    return TCell::Make<T>(T());
}
}   // namespace

TConclusion<TCell> TRangesBuilder::MakeDefaultCell(const NScheme::TTypeInfo typeInfo) {
    switch (typeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Bool:
            return MakeDefaultCellByPrimitiveType<bool>();
        case NScheme::NTypeIds::Int8:
            return MakeDefaultCellByPrimitiveType<i8>();
        case NScheme::NTypeIds::Uint8:
            return MakeDefaultCellByPrimitiveType<ui8>();
        case NScheme::NTypeIds::Int16:
            return MakeDefaultCellByPrimitiveType<i16>();
        case NScheme::NTypeIds::Date:
        case NScheme::NTypeIds::Uint16:
            return MakeDefaultCellByPrimitiveType<ui16>();
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Date32:
            return MakeDefaultCellByPrimitiveType<i32>();
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Uint32:
            return MakeDefaultCellByPrimitiveType<ui32>();
        case NScheme::NTypeIds::Int64:
            return MakeDefaultCellByPrimitiveType<i64>();
        case NScheme::NTypeIds::Uint64:
            return MakeDefaultCellByPrimitiveType<ui64>();
        case NScheme::NTypeIds::Float:
            return MakeDefaultCellByPrimitiveType<float>();
        case NScheme::NTypeIds::Double:
            return MakeDefaultCellByPrimitiveType<double>();
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::Json:
            return TCell(Default<TString>().data(), Default<TString>().size());
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
        case NScheme::NTypeIds::Yson:
        case NScheme::NTypeIds::DyNumber:
        case NScheme::NTypeIds::JsonDocument:
            return TCell(Default<TString>().data(), Default<TString>().size());
        case NScheme::NTypeIds::Timestamp:
            return MakeDefaultCellByPrimitiveType<ui64>();
        case NScheme::NTypeIds::Interval:
            return MakeDefaultCellByPrimitiveType<i64>();
        case NScheme::NTypeIds::Decimal:
            return MakeDefaultCellByPrimitiveType<std::pair<ui64, i64>>();
        case NScheme::NTypeIds::Uuid:
            return MakeDefaultCellByPrimitiveType<ui16>();

        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            return MakeDefaultCellByPrimitiveType<i64>();

        case NScheme::NTypeIds::PairUi64Ui64:
        case NScheme::NTypeIds::ActorId:
        case NScheme::NTypeIds::StepOrderId:
            break;   // Deprecated types

        case NScheme::NTypeIds::Pg:
            switch (NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc())) {
                case INT2OID:
                    return MakeDefaultCellByPrimitiveType<i16>();
                case INT4OID:
                    return MakeDefaultCellByPrimitiveType<i32>();
                case INT8OID:
                    return MakeDefaultCellByPrimitiveType<i64>();
                case FLOAT4OID:
                    return MakeDefaultCellByPrimitiveType<float>();
                case FLOAT8OID:
                    return MakeDefaultCellByPrimitiveType<double>();
                case BYTEAOID:
                    return TCell(Default<TString>().data(), Default<TString>().size());
                case TEXTOID:
                    return TCell(Default<TString>().data(), Default<TString>().size());
                default:
                    break;
            }
            break;
    }
    return TConclusionStatus::Fail(TStringBuilder() << "Unsupported type: " << NScheme::TypeName(typeInfo.GetTypeId()));
}

}   // namespace NKikimr::NOlap
