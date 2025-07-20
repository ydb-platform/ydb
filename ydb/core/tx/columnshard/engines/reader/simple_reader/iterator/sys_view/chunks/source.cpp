#include "source.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {
namespace {
class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    NReader::NCommon::TFetchingScriptCursor Step;
    std::shared_ptr<NCommon::IDataSource> Source;
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
        AFL_VERIFY(Step.Next());
        const auto& commonContext = *Source->GetContext()->GetCommonContext();
        auto task = std::make_shared<NReader::NCommon::TStepAction>(std::move(Source), std::move(Step), commonContext.GetScanActorId(), false);
        NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, commonContext.GetConveyorProcessId());
    }

public:
    TPortionAccessorFetchingSubscriber(const NReader::NCommon::TFetchingScriptCursor& step, const std::shared_ptr<NCommon::IDataSource>& source)
        : Step(step)
        , Source(source)
        , Guard(Source->GetContext()->GetCommonContext()->GetCounters().GetFetcherAcessorsGuard()) {
    }
};

}   // namespace

bool TSourceData::DoStartFetchingAccessor(
    const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const NReader::NCommon::TFetchingScriptCursor& step) {
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
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::PathId::ColumnId) {
        return NArrow::TStatusValidator::GetValid(
            arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetUnifiedPathId().GetSchemeShardLocalPathId().GetRawValue()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::Kind::ColumnId) {
        return NArrow::TStatusValidator::GetValid(
            arrow::MakeArrayFromScalar(arrow::StringScalar(::ToString(GetPortion()->GetProduced())), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::TabletId::ColumnId) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetTabletId()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::Rows::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetMeta().GetRecordsCount());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetRecordsCount());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::RawBytes::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetMeta().GetRawBytes());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetRawBytes());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::PortionId::ColumnId) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetPortion()->GetPortionId()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkIdx::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetChunkIdx());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetChunkIdx());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::EntityName::ColumnId) {
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
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::InternalEntityId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint32());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt32Type>(*builder, i.GetEntityId());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt32Type>(*builder, i.GetEntityId());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::BlobId::ColumnId) {
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
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::BlobRangeOffset::ColumnId) {
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
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::BlobRangeSize::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetBlobRange().GetSize());
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetDataSize());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::Activity::ColumnId) {
        if (Portion->HasRemoveSnapshot()) {
            return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(0), recordsCount));
        } else {
            return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(1), recordsCount));
        }
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::TierName::ColumnId) {
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
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::EntityType::ColumnId) {
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
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        const auto& records = GetStageData().GetPortionAccessor().GetRecordsVerified();
        for (auto it = records.begin(); it != records.end();) {
            auto accessor = OriginalData ? OriginalData->ExtractAccessorOptional(it->GetEntityId()) : nullptr;
            const ui32 entityId = it->GetEntityId();
            if (!accessor) {
                while (it != records.end() && it->GetEntityId() == entityId) {
                    NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view());
                    ++it;
                }
            } else {
                const auto addChunkInfo = [&builder](const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& chunk) {
                    AFL_VERIFY(chunk->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray);
                    const NArrow::NAccessor::TSubColumnsPartialArray* arr =
                        static_cast<const NArrow::NAccessor::TSubColumnsPartialArray*>(chunk.get());
                    const TString data = arr->GetHeader().DebugJson().GetStringRobust();
                    NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(data.data(), data.size()));
                };

                AFL_VERIFY(it->GetChunkIdx() == 0);
                if (accessor->GetType() == NArrow::NAccessor::IChunkedArray::EType::CompositeChunkedArray) {
                    const NArrow::NAccessor::TCompositeChunkedArray* composite =
                        static_cast<const NArrow::NAccessor::TCompositeChunkedArray*>(accessor.get());
                    for (auto&& i : composite->GetChunks()) {
                        AFL_VERIFY(it != records.end());
                        AFL_VERIFY(it->GetChunkIdx() < composite->GetChunks().size());
                        AFL_VERIFY(it->GetEntityId() == entityId);
                        addChunkInfo(i);
                        ++it;
                    }

                } else {
                    AFL_VERIFY(it->GetChunkIdx() == 0);
                    addChunkInfo(accessor);
                    ++it;
                }
                AFL_VERIFY(it == records.end() || it->GetEntityId() != entityId)("it", it->GetEntityId())("from", entityId);
            }
        }
        for (auto&& i : GetStageData().GetPortionAccessor().GetIndexesVerified()) {
            Y_UNUSED(i);
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    AFL_VERIFY(false)("column_id", columnId);
    return nullptr;
}

TConclusion<bool> TSourceData::DoStartFetchImpl(
    const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NCommon::IKernelFetchLogic>>& fetchersExt) {
    AFL_VERIFY(fetchersExt.size());
    if (!OriginalData) {
        OriginalData = std::make_shared<NArrow::NAccessor::TAccessorsCollection>();
    }

    TReadActionsCollection readActions;
    auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
    NCommon::TFetchingResultContext contextFetch(*OriginalData, *GetStageData().GetIndexes(), source, nullptr);
    for (auto&& i : fetchersExt) {
        i->Start(readActions, contextFetch);
    }
    if (readActions.IsEmpty()) {
        for (auto&& i : fetchersExt) {
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            i->OnDataReceived(readActions, blobs);
            MutableStageData().AddFetcher(i);
            AFL_VERIFY(readActions.IsEmpty());
        }
        return false;
    }
    THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>> fetchers;
    for (auto&& i : fetchersExt) {
        AFL_VERIFY(fetchers.emplace(i->GetEntityId(), i).second);
    }
    NActors::TActivationContext::AsActorContext().Register(
        new NOlap::NBlobOperations::NRead::TActor(std::make_shared<NCommon::TColumnsFetcherTask>(
            std::move(readActions), fetchers, source, GetExecutionContext().GetCursorStep(), "fetcher", "")));
    return true;
}

TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> TSourceData::DoStartFetchData(
    const NArrow::NSSA::TProcessorContext& /*context*/, const NArrow::NSSA::IDataSource::TDataAddress& addr) {
    if (addr.GetColumnId() == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId) {
        THashSet<ui32> entityIds;
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            if (!entityIds.emplace(i.GetEntityId()).second) {
                continue;
            }
            if (Schema->GetColumnLoaderVerified(i.GetEntityId())->GetAccessorConstructor()->GetType() ==
                NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
                return std::make_shared<NCommon::TSubColumnsFetchLogic>(i.GetEntityId(), Schema,
                    GetContext()->GetCommonContext()->GetStoragesManager(),
                    GetStageData().GetPortionAccessor().GetPortionInfo().GetRecordsCount(), std::vector<TString>());
            }
        }
    }
    return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
}

void TSourceData::DoAssembleAccessor(const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) {
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId) {
        auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
        for (auto&& i : GetStageData().GetPortionAccessor().GetRecordsVerified()) {
            if (auto fetcher = MutableStageData().ExtractFetcherOptional(i.GetEntityId())) {
                AFL_VERIFY(OriginalData);
                NCommon::TFetchingResultContext fetchContext(*OriginalData, *GetStageData().GetIndexes(), source, nullptr);
                fetcher->OnDataCollected(fetchContext);
            }
        }
    }
    TBase::DoAssembleAccessor(context, columnId, subColumnName);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
