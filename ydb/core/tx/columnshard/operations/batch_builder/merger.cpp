#include "merger.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap {

NKikimr::TConclusionStatus IMerger::Finish() {
    while (!IncomingFinished) {
        auto result = OnIncomingOnly(IncomingPosition);
        if (result.IsFail()) {
            return result;
        }
        IncomingFinished = !IncomingPosition.NextPosition(1);
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus IMerger::AddExistsDataOrdered(const std::shared_ptr<arrow::Table>& data) {
    AFL_VERIFY(data);
    NArrow::NMerger::TRWSortableBatchPosition existsPosition(data, 0, Schema->GetPKColumnNames(),
        Schema->GetIndexInfo().GetColumnSTLNames(Schema->GetIndexInfo().GetColumnIds(false)), false);
    bool exsistFinished = !existsPosition.InitPosition(0);
    while (!IncomingFinished && !exsistFinished) {
        auto cmpResult = IncomingPosition.Compare(existsPosition);
        if (cmpResult == std::partial_ordering::equivalent) {
            auto result = OnEqualKeys(existsPosition, IncomingPosition);
            if (result.IsFail()) {
                return result;
            }
            exsistFinished = !existsPosition.NextPosition(1);
            IncomingFinished = !IncomingPosition.NextPosition(1);
        } else if (cmpResult == std::partial_ordering::less) {
            auto result = OnIncomingOnly(IncomingPosition);
            if (result.IsFail()) {
                return result;
            }
            IncomingFinished = !IncomingPosition.NextPosition(1);
        } else {
            AFL_VERIFY(false);
        }
    }
    AFL_VERIFY(exsistFinished);
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TUpdateMerger::OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) {
    auto rGuard = Builder.StartRecord();
    AFL_VERIFY(Schema->GetIndexInfo().GetColumnIds(false).size() == exists.GetData().GetColumns().size())
        ("index", Schema->GetIndexInfo().GetColumnIds(false).size())("exists", exists.GetData().GetColumns().size());
    for (i32 columnIdx = 0; columnIdx < Schema->GetIndexInfo().ArrowSchema()->num_fields(); ++columnIdx) {
        const std::optional<ui32>& incomingColumnIdx = IncomingColumnRemap[columnIdx];
        if (incomingColumnIdx && HasIncomingDataFlags[*incomingColumnIdx]->GetView(incoming.GetPosition())) {
            const ui32 idxChunk = incoming.GetData().GetPositionInChunk(*incomingColumnIdx, incoming.GetPosition());
            rGuard.Add(*incoming.GetData().GetPositionAddress(*incomingColumnIdx).GetArray(), idxChunk);
        } else {
            const ui32 idxChunk = exists.GetData().GetPositionInChunk(columnIdx, exists.GetPosition());
            rGuard.Add(*exists.GetData().GetPositionAddress(columnIdx).GetArray(), idxChunk);
        }
    }
    return TConclusionStatus::Success();
}

TUpdateMerger::TUpdateMerger(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema,
    const TString& insertDenyReason, const std::optional<NArrow::NMerger::TSortableBatchPosition>& defaultExists /*= {}*/)
    : TBase(incoming, actualSchema)
    , Builder(actualSchema->GetIndexInfo().ArrowSchema()->fields())
    , DefaultExists(defaultExists)
    , InsertDenyReason(insertDenyReason)
{
    for (auto&& i : actualSchema->GetIndexInfo().ArrowSchema()->field_names()) {
        auto fIdx = IncomingData->schema()->GetFieldIndex(i);
        if (fIdx == -1) {
            IncomingColumnRemap.emplace_back();
        } else {
            auto fExistsIdx = IncomingData->schema()->GetFieldIndex("$$EXISTS::" + i);
            std::shared_ptr<arrow::Array> flagsArray;
            if (fExistsIdx != -1) {
                AFL_VERIFY(IncomingData->column(fExistsIdx)->type_id() == arrow::Type::BOOL);
                flagsArray = IncomingData->column(fExistsIdx);
            } else {
                flagsArray = NArrow::TThreadSimpleArraysCache::GetConst(arrow::TypeTraits<arrow::BooleanType>::type_singleton(),
                    std::make_shared<arrow::BooleanScalar>(true), IncomingData->num_rows());
            }
            HasIncomingDataFlags.emplace_back(static_pointer_cast<arrow::BooleanArray>(flagsArray));
            IncomingColumnRemap.emplace_back(fIdx);
        }
    }
}

}
