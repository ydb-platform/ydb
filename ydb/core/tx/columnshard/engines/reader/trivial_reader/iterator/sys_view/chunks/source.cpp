#include "source.h"

#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessor_callback.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/fetcher.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/min_max/meta.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <library/cpp/json/writer/json.h>

namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NChunks {

namespace {

class TChunkDetailsFetchLogic: public NCommon::IKernelFetchLogic {
    using TBase = NCommon::IKernelFetchLogic;
    std::vector<std::shared_ptr<NCommon::IKernelFetchLogic>> SubFetchers;

    virtual void DoStart(TReadActionsCollection& nextRead, NCommon::TFetchingResultContext& context) override {
        for (auto& f : SubFetchers) {
            f->Start(nextRead, context);
        }
    }

    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override {
        for (auto& f : SubFetchers) {
            f->OnDataReceived(nextRead, blobs);
        }
    }

    virtual void DoOnDataCollected(NCommon::TFetchingResultContext& context) override {
        for (auto& f : SubFetchers) {
            f->OnDataCollected(context);
        }
    }

public:
    TChunkDetailsFetchLogic(const ui32 entityId, const std::shared_ptr<IStoragesManager>& storagesManager)
        : TBase(entityId, storagesManager)
    {
    }

    void Add(std::shared_ptr<NCommon::IKernelFetchLogic> fetcher) {
        SubFetchers.push_back(std::move(fetcher));
    }

    bool IsEmpty() const {
        return SubFetchers.empty();
    }
};

}   // namespace

bool TSourceData::DoStartFetchingAccessor(
    const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const NReader::NCommon::TFetchingScriptCursor& step) {
    AFL_VERIFY(!HasPortionAccessor());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    std::shared_ptr<TDataAccessorsRequest> request =
        std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
    request->AddPortion(GetPortion());
    request->SetColumnIds(GetContext()->GetAllUsageColumns()->GetColumnIds());
    request->RegisterSubscriber(std::make_shared<NCommon::TPortionAccessorFetchingSubscriber>(step, sourcePtr));
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
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetMeta().GetRecordsCount());
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetRecordsCount());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::RawBytes::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetMeta().GetRawBytes());
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetRawBytes());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::PortionId::ColumnId) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetPortion()->GetPortionId()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkIdx::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetChunkIdx());
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetChunkIdx());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::EntityName::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            const auto colName = Schema->GetIndexInfo().GetColumnFieldVerified(i.GetEntityId())->name();
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(colName.data(), colName.size()));
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            const auto idxName = Schema->GetIndexInfo().GetIndexVerified(i.GetEntityId())->GetIndexName();
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(idxName.data(), idxName.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::InternalEntityId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint32());
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt32Type>(*builder, i.GetEntityId());
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            NArrow::Append<arrow::UInt32Type>(*builder, i.GetEntityId());
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::BlobId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            const TString blobIdStr = GetPortionAccessor().GetBlobId(i.BlobRange.GetBlobIdxVerified()).ToStringNew();
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(blobIdStr.data(), blobIdStr.size()));
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            if (auto range = i.GetBlobRangeOptional()) {
                const TString blobIdStr = GetPortionAccessor().GetBlobId(range->GetBlobIdxVerified()).ToStringNew();
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
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetBlobRange().GetOffset());
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
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
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            NArrow::Append<arrow::UInt64Type>(*builder, i.GetBlobRange().GetSize());
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
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
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            const TString tierName = Portion->GetEntityStorageId(i.GetEntityId(), Schema->GetIndexInfo());
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(tierName.data(), tierName.size()));
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            const TString tierName = Portion->GetEntityStorageId(i.GetEntityId(), Schema->GetIndexInfo());
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(tierName.data(), tierName.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::EntityType::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            Y_UNUSED(i);
            const TString type = "COL";
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(type.data(), type.size()));
        }
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            Y_UNUSED(i);
            const TString type = "IDX";
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(type.data(), type.size()));
        }
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        const auto& records = GetPortionAccessor().GetRecordsVerified();
        for (auto it = records.begin(); it != records.end();) {
            auto accessor = OriginalData ? OriginalData->ExtractAccessorOptional(it->GetEntityId()) : nullptr;
            const ui32 entityId = it->GetEntityId();
            if (!accessor) {
                while (it != records.end() && it->GetEntityId() == entityId) {
                    TString data;
                    if (it->GetMeta().HasAdditionalAccessorData()) {
                        data = it->GetMeta().GetAdditionalAccessorData()->DebugJson().GetStringRobust();
                    }
                    NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(data.data(), data.size()));
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
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            TString data;
            if (auto* stringData = i.GetBlobDataOptional()) {
                const auto indexMeta = Schema->GetIndexInfo().GetIndexVerified(i.GetEntityId());
                if (indexMeta->GetClassName() == NIndexes::NMinMax::TIndexMeta::GetClassNameStatic()) {
                    const auto json = indexMeta->SerializeDataToJson(*stringData, Schema->GetIndexInfo());
                    if (json.Has("data")) {
                        NJsonWriter::TBuf buf;
                        buf.BeginObject();
                        buf.WriteKey("min").WriteString(json["data"]["min"].GetStringRobust());
                        buf.WriteKey("max").WriteString(json["data"]["max"].GetStringRobust());
                        buf.EndObject();
                        data = buf.Str();
                    }
                }
            } else {
                const auto indexMeta = Schema->GetIndexInfo().GetIndexVerified(i.GetEntityId());
                if (indexMeta->GetClassName() == NIndexes::NMinMax::TIndexMeta::GetClassNameStatic()) {
                    if (const auto* indexData = GetStageData().GetIndexes()->GetIndexDataOptional(i.GetEntityId())) {
                        if (const auto* blobData = indexData->GetChunkDataOptional(i.GetChunkIdx(), std::nullopt)) {
                            const auto json = indexMeta->SerializeDataToJson(*blobData, Schema->GetIndexInfo());
                            if (json.Has("data")) {
                                NJsonWriter::TBuf buf;
                                buf.BeginObject();
                                buf.WriteKey("min").WriteString(json["data"]["min"].GetStringRobust());
                                buf.WriteKey("max").WriteString(json["data"]["max"].GetStringRobust());
                                buf.EndObject();
                                data = buf.Str();
                            }
                        }
                    }
                }
            }
            NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(data.data(), data.size()));
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
        auto composite = std::make_shared<TChunkDetailsFetchLogic>(
            NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId, GetContext()->GetCommonContext()->GetStoragesManager());

        THashSet<ui32> entityIds;
        for (auto&& i : GetPortionAccessor().GetRecordsVerified()) {
            if (!entityIds.emplace(i.GetEntityId()).second) {
                continue;
            }
            if (Schema->GetColumnLoaderVerified(i.GetEntityId())->GetAccessorConstructor()->GetType() ==
                NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
                composite->Add(std::make_shared<NCommon::TSubColumnsFetchLogic>(i.GetEntityId(), Schema,
                    GetContext()->GetCommonContext()->GetStoragesManager(), GetPortionAccessor().GetPortionInfo().GetRecordsCount(),
                    std::vector<TString>()));
                break;
            }
        }

        THashSet<ui32> indexIds;
        for (auto&& i : GetPortionAccessor().GetIndexesVerified()) {
            const auto* blobRangeLink = i.GetBlobRangeOptional();
            if (!blobRangeLink) {
                continue;
            }
            if (!indexIds.emplace(i.GetEntityId()).second) {
                continue;
            }
            const auto indexMeta = Schema->GetIndexInfo().GetIndexVerified(i.GetEntityId());
            if (indexMeta->GetClassName() != NIndexes::NMinMax::TIndexMeta::GetClassNameStatic()) {
                continue;
            }
            THashSet<NIndexes::NRequest::TOriginalDataAddress> dummyAddr;
            dummyAddr.emplace(NIndexes::NRequest::TOriginalDataAddress(i.GetEntityId(), ""));
            composite->Add(std::make_shared<NIndexes::TIndexFetcherLogic>(
                dummyAddr, indexMeta.GetObjectPtr(), GetContext()->GetCommonContext()->GetStoragesManager()));
        }

        if (!composite->IsEmpty()) {
            return composite;
        }
    }
    return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
}

void TSourceData::DoAssembleAccessor(const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) {
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId) {
        auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
        if (auto fetcher = MutableStageData().ExtractFetcherOptional(NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId)) {
            AFL_VERIFY(OriginalData);
            NCommon::TFetchingResultContext fetchContext(*OriginalData, *GetStageData().GetIndexes(), source, nullptr);
            fetcher->OnDataCollected(fetchContext);
        }
    }
    TBase::DoAssembleAccessor(context, columnId, subColumnName);
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NChunks
