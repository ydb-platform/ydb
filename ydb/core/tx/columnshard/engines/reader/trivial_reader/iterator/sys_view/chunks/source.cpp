#include "source.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessor_callback.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/fetcher.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/min_max/meta.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <library/cpp/json/writer/json.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

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

    virtual TConclusionStatus DoOnDataCollected(NCommon::TFetchingResultContext& context) override {
        for (auto& f : SubFetchers) {
            auto conclusion = f->OnDataCollected(context);
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        return TConclusionStatus::Success();
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
    YDB_LOG_DEBUG("",
        {"event", step.GetName()},
        {"fetchingInfo", step.DebugString()});

    std::shared_ptr<TDataAccessorsRequest> request =
        std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
    request->AddPortion(GetPortion());
    request->SetColumnIds(GetContext()->GetAllUsageColumns()->GetColumnIds());
    request->RegisterSubscriber(std::make_shared<NCommon::TPortionAccessorFetchingSubscriber>(step, sourcePtr));
    GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
    return true;
}

const NCommon::TPKSortPermutation& TSourceData::GetChunksPKOrder() const {
    if (ChunksPKOrder) {
        return *ChunksPKOrder;
    }
    ChunksPKOrder.emplace();
    // the permutation is only consumed by the flag-on limit-pushdown path: the limit sync point walks each
    // source assuming PK order to drop rows early. A flag-off sorted scan goes through TSortedFullScanCollection
    // and KQP re-sorts on top, so within-source order is irrelevant there; skip the work.
    if (!GetContext()->GetReadMetadata()->IsSorted() || !HasAppData() ||
        !AppDataVerified().FeatureFlags.GetEnableSysViewOrderByLimitPushdown()) {
        return *ChunksPKOrder;
    }
    const auto& records = GetPortionAccessor().GetRecordsVerified();
    const auto& indexes = GetPortionAccessor().GetIndexesVerified();
    std::vector<std::pair<TChunkAddress, ui64>> positions;
    positions.reserve(records.size() + indexes.size());
    for (auto&& record : records) {
        positions.emplace_back(record.GetAddress(), positions.size());
    }
    for (auto&& index : indexes) {
        positions.emplace_back(index.GetAddress(), positions.size());
    }
    // fast path: records-then-indexes are already in PK order (no interleaved entity ids) => empty permutation, iterate as stored
    if (!std::is_sorted(positions.begin(), positions.end())) {
        std::sort(positions.begin(), positions.end());
        ChunksPKOrder->reserve(positions.size());
        for (auto&& position : positions) {
            ChunksPKOrder->emplace_back(position.second);
        }
    }
    return *ChunksPKOrder;
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
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                NArrow::Append<arrow::UInt64Type>(*builder, record.GetMeta().GetRecordsCount());
            },
            [&](const TIndexChunk& index) {
                NArrow::Append<arrow::UInt64Type>(*builder, index.GetRecordsCount());
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::RawBytes::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                NArrow::Append<arrow::UInt64Type>(*builder, record.GetMeta().GetRawBytes());
            },
            [&](const TIndexChunk& index) {
                NArrow::Append<arrow::UInt64Type>(*builder, index.GetRawBytes());
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::PortionId::ColumnId) {
        return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(GetPortion()->GetPortionId()), recordsCount));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkIdx::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                NArrow::Append<arrow::UInt64Type>(*builder, record.GetChunkIdx());
            },
            [&](const TIndexChunk& index) {
                NArrow::Append<arrow::UInt64Type>(*builder, index.GetChunkIdx());
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::EntityName::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                const auto colName = Schema->GetIndexInfo().GetColumnFieldVerified(record.GetEntityId())->name();
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(colName.data(), colName.size()));
            },
            [&](const TIndexChunk& index) {
                const auto idxName = Schema->GetIndexInfo().GetIndexVerified(index.GetEntityId())->GetIndexName();
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(idxName.data(), idxName.size()));
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::InternalEntityId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint32());
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                NArrow::Append<arrow::UInt32Type>(*builder, record.GetEntityId());
            },
            [&](const TIndexChunk& index) {
                NArrow::Append<arrow::UInt32Type>(*builder, index.GetEntityId());
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::BlobId::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                const TString blobIdStr = GetPortionAccessor().GetBlobId(record.BlobRange.GetBlobIdxVerified()).ToStringNew();
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(blobIdStr.data(), blobIdStr.size()));
            },
            [&](const TIndexChunk& index) {
                if (auto range = index.GetBlobRangeOptional()) {
                    const TString blobIdStr = GetPortionAccessor().GetBlobId(range->GetBlobIdxVerified()).ToStringNew();
                    NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(blobIdStr.data(), blobIdStr.size()));
                } else {
                    const TString blobIdStr = "__INPLACE";
                    NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(blobIdStr.data(), blobIdStr.size()));
                }
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::BlobRangeOffset::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                NArrow::Append<arrow::UInt64Type>(*builder, record.GetBlobRange().GetOffset());
            },
            [&](const TIndexChunk& index) {
                if (auto range = index.GetBlobRangeOptional()) {
                    NArrow::Append<arrow::UInt64Type>(*builder, range->GetOffset());
                } else {
                    NArrow::Append<arrow::UInt64Type>(*builder, 0);
                }
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::BlobRangeSize::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::uint64());
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                NArrow::Append<arrow::UInt64Type>(*builder, record.GetBlobRange().GetSize());
            },
            [&](const TIndexChunk& index) {
                NArrow::Append<arrow::UInt64Type>(*builder, index.GetDataSize());
            });
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
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                const TString tierName = Portion->GetEntityStorageId(record.GetEntityId(), Schema->GetIndexInfo());
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(tierName.data(), tierName.size()));
            },
            [&](const TIndexChunk& index) {
                const TString tierName = Portion->GetEntityStorageId(index.GetEntityId(), Schema->GetIndexInfo());
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(tierName.data(), tierName.size()));
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::EntityType::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        ForEachChunkInPKOrder(
            [&](const TColumnRecord&) {
                const TString type = "COL";
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(type.data(), type.size()));
            },
            [&](const TIndexChunk&) {
                const TString type = "IDX";
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(type.data(), type.size()));
            });
        return NArrow::FinishBuilder(std::move(builder));
    }
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId) {
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        // an entity's records are consecutive in PK order (TChunkAddress = (EntityId, ChunkIdx)), so the
        // accessor is extracted once per entity and only one is held at a time
        std::optional<ui32> currentEntityId;
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> currentAccessor;
        const auto recordDetail = [&](const TColumnRecord& record) -> TString {
            if (!currentEntityId || *currentEntityId != record.GetEntityId()) {
                currentEntityId = record.GetEntityId();
                currentAccessor = OriginalData ? OriginalData->ExtractAccessorOptional(record.GetEntityId()) : nullptr;
            }
            if (!currentAccessor) {
                return record.GetMeta().HasAdditionalAccessorData() ? record.GetMeta().GetAdditionalAccessorData()->DebugJson().GetStringRobust()
                                                                    : TString();
            }
            const NArrow::NAccessor::IChunkedArray* chunk = currentAccessor.get();
            if (currentAccessor->GetType() == NArrow::NAccessor::IChunkedArray::EType::CompositeChunkedArray) {
                const auto* composite = static_cast<const NArrow::NAccessor::TCompositeChunkedArray*>(currentAccessor.get());
                AFL_VERIFY(record.GetChunkIdx() < composite->GetChunks().size());
                chunk = composite->GetChunks()[record.GetChunkIdx()].get();
            } else {
                AFL_VERIFY(record.GetChunkIdx() == 0);
            }
            AFL_VERIFY(chunk->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray);
            return static_cast<const NArrow::NAccessor::TSubColumnsPartialArray*>(chunk)->GetHeader().DebugJson().GetStringRobust();
        };
        const auto indexDetail = [&](const TIndexChunk& index) -> TString {
            const auto indexMeta = Schema->GetIndexInfo().GetIndexVerified(index.GetEntityId());
            if (indexMeta->GetClassName() != NIndexes::NMinMax::TIndexMeta::GetClassNameStatic()) {
                return TString();
            }
            const TString* stringData = index.GetBlobDataOptional();
            NJson::TJsonValue json;
            if (stringData) {
                json = indexMeta->SerializeDataToJson(*stringData, Schema->GetIndexInfo());
            } else if (const auto* indexData = GetStageData().GetIndexes()->GetIndexDataOptional(index.GetEntityId())) {
                if (const auto* blobData = indexData->GetChunkDataOptional(index.GetChunkIdx(), std::nullopt)) {
                    json = indexMeta->SerializeDataToJson(*blobData, Schema->GetIndexInfo());
                }
            }
            if (!json.Has("data")) {
                return TString();
            }
            NJsonWriter::TBuf buf;
            buf.BeginObject();
            buf.WriteKey("min").WriteString(json["data"]["min"].GetStringRobust());
            buf.WriteKey("max").WriteString(json["data"]["max"].GetStringRobust());
            buf.EndObject();
            return buf.Str();
        };
        ForEachChunkInPKOrder(
            [&](const TColumnRecord& record) {
                const TString data = recordDetail(record);
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(data.data(), data.size()));
            },
            [&](const TIndexChunk& index) {
                const TString data = indexDetail(index);
                NArrow::Append<arrow::StringType>(*builder, arrow::util::string_view(data.data(), data.size()));
            });
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

TConclusionStatus TSourceData::DoAssembleAccessor(
    const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) {
    if (columnId == NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId) {
        auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
        if (auto fetcher = MutableStageData().ExtractFetcherOptional(NKikimr::NSysView::Schema::PrimaryIndexStats::ChunkDetails::ColumnId)) {
            AFL_VERIFY(OriginalData);
            NCommon::TFetchingResultContext fetchContext(*OriginalData, *GetStageData().GetIndexes(), source, nullptr);
            auto conclusion = fetcher->OnDataCollected(fetchContext);
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
    }
    return TBase::DoAssembleAccessor(context, columnId, subColumnName);
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NChunks
