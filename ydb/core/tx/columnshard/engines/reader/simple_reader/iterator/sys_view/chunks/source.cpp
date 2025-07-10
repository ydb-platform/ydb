#include "source.h"

#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {
namespace {
class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    NReader::NCommon::TFetchingScriptCursor Step;
    std::shared_ptr<NReader::NSimple::IDataSource> Source;
    const NColumnShard::TCounterGuard Guard;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Source->GetContext()->GetCommonContext()->GetAbortionFlag();
    }

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("facc"));
        if (result.HasErrors()) {
            Source->GetContext()->GetCommonContext()->AbortWithError("has errors on portion accessors restore");
            return;
        }
        AFL_VERIFY(result.GetPortions().size() == 1)("count", result.GetPortions().size());
        Source->MutableStageData().SetPortionAccessor(std::move(result.ExtractPortionsVector().front()));
        Source->InitUsedRawBytes();
        AFL_VERIFY(Step.Next());
        const auto& commonContext = *Source->GetContext()->GetCommonContext();
        auto task = std::make_shared<NReader::NCommon::TStepAction>(Source, std::move(Step), commonContext.GetScanActorId(), false);
        NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, commonContext.GetConveyorProcessId());
    }

public:
    TPortionAccessorFetchingSubscriber(
        const NReader::NCommon::TFetchingScriptCursor& step, const std::shared_ptr<NReader::NSimple::IDataSource>& source)
        : Step(step)
        , Source(source)
        , Guard(Source->GetContext()->GetCommonContext()->GetCounters().GetFetcherAcessorsGuard()) {
    }
};

}   // namespace

bool TSourceData::DoStartFetchingAccessor(
    const std::shared_ptr<IDataSource>& sourcePtr, const NReader::NCommon::TFetchingScriptCursor& step) {
    AFL_VERIFY(!GetStageData().HasPortionAccessor());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    std::shared_ptr<TDataAccessorsRequest> request =
        std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
    request->AddPortion(GetPortion());
    request->SetColumnIds(GetContext()->GetAllUsageColumns()->GetColumnIds());
    request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(step, sourcePtr));
    GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
    return true;
}

std::shared_ptr<arrow::Array> TSourceData::BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const {
    if (columnId == 1) {
        return NArrow::TStatusValidator::GetValid(
            arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetUnifiedPathId().GetSchemeShardLocalPathId().GetRawValue()), recordsCount));
    }
    if (columnId == 2) {
        return NArrow::TStatusValidator::GetValid(
            arrow::MakeArrayFromScalar(arrow::StringScalar(::ToString(GetPortion()->GetProduced())), recordsCount));
    }
    if (columnId == 3) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetTabletId()), recordsCount));
    }
    if (columnId == 4) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetMeta().GetRecordsCount());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetRecordsCount());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 5) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetMeta().GetRawBytes());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetRawBytes());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 6) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetPortion()->GetPortionId()), recordsCount));
    }
    if (columnId == 7) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetChunkIdx());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetChunkIdx());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 8) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            const auto colName = Schema->GetIndexInfo().GetColumnFieldVerified(i.GetEntityId())->name();
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(colName.data(), colName.size()));
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            const auto idxName = Schema->GetIndexInfo().GetIndexVerified(i.GetEntityId())->GetIndexName();
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(idxName.data(), idxName.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 9) {
        auto builder = NArrow::MakeBuilder(arrow::uint32());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt32Type>(*builder, i.GetEntityId());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt32Type>(*builder, i.GetEntityId());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 10) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            const TString blobIdStr = GetStageData().GetPortionAccessor().GetBlobId(i.BlobRange.GetBlobIdxVerified()).ToStringNew();
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(blobIdStr.data(), blobIdStr.size()));
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            if (auto range = i.GetBlobRangeOptional()) {
                const TString blobIdStr = GetStageData().GetPortionAccessor().GetBlobId(range->GetBlobIdxVerified()).ToStringNew();
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(blobIdStr.data(), blobIdStr.size()));
            } else {
                const TString blobIdStr = "__INPLACE";
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(blobIdStr.data(), blobIdStr.size()));
            }
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 11) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetBlobRange().GetOffset());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            if (auto range = i.GetBlobRangeOptional()) {
                NArrow::Append<arrow::UInt64Type>(*builder, range->GetOffset());
            } else {
                NArrow::Append<arrow::UInt64Type>(*builder, 0);
            }
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 12) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetBlobRange().GetSize());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetDataSize());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 13) {
        if (GetPortion()->IsRemovedFor(GetContext()->GetCommonContext()->GetReadMetadata()->GetRequestSnapshot())) {
            return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(0), recordsCount));
        } else {
            return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(1), recordsCount));
        }
    }
    if (columnId == 14) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            const TString tierName = Schema->GetIndexInfo().GetEntityStorageId(i.GetEntityId(), Portion->GetMeta().GetTierName());
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(tierName.data(), tierName.size()));
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            const TString tierName = Schema->GetIndexInfo().GetEntityStorageId(i.GetEntityId(), Portion->GetMeta().GetTierName());
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(tierName.data(), tierName.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == 15) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            Y_UNUSED(i);
            const TString type = "COL";
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(type.data(), type.size()));
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            Y_UNUSED(i);
            const TString type = "IDX";
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(type.data(), type.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    AFL_VERIFY(false)("column_id", columnId);
    return nullptr;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
