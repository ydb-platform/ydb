#include "columnshard.h"
#include "columnshard_impl.h"

#include "data_accessor/cache_policy/policy.h"
#include "ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <yql/essentials/core/minsketch/count_min_sketch.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(NStat::TEvStatistics::TEvAnalyzeTable::TPtr& ev, const TActorContext&) {
    auto& requestRecord = ev->Get()->Record;
    // TODO Start a potentially long analysis process.
    // ...

    // Return the response when the analysis is completed
    auto response = std::make_unique<NStat::TEvStatistics::TEvAnalyzeTableResponse>();
    auto& responseRecord = response->Record;
    responseRecord.SetOperationId(requestRecord.GetOperationId());
    responseRecord.MutablePathId()->CopyFrom(requestRecord.GetTable().GetPathId());
    responseRecord.SetShardTabletId(TabletID());
    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

class TResultAccumulator {
private:
    TMutex Mutex;
    THashMap<ui32, std::unique_ptr<TCountMinSketch>> Calculated;
    TAtomicCounter ResultsCount = 0;
    TAtomicCounter WaitingCount = 0;
    const NActors::TActorId RequestSenderActorId;
    bool Started = false;
    const ui64 Cookie;
    std::unique_ptr<NStat::TEvStatistics::TEvStatisticsResponse> Response;
    bool Replied = false;

    void OnResultReady() {
        AFL_VERIFY(!Replied);
        Replied = true;
        auto& respRecord = Response->Record;
        respRecord.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS);

        for (auto&& [columnTag, sketch] : Calculated) {
            if (!sketch) {
                continue;
            }

            auto* column = respRecord.AddColumns();
            column->SetTag(columnTag);
            auto* statistic = column->AddStatistics();
            statistic->SetType(NStat::COUNT_MIN_SKETCH);
            statistic->SetData(TString(sketch->AsStringBuf()));
        }

        NActors::TActivationContext::Send(RequestSenderActorId, std::move(Response), 0, Cookie);
    }

public:
    TResultAccumulator(const std::set<ui32>& tags, const NActors::TActorId& requestSenderActorId, const ui64 cookie,
        std::unique_ptr<NStat::TEvStatistics::TEvStatisticsResponse>&& response)
        : RequestSenderActorId(requestSenderActorId)
        , Cookie(cookie)
        , Response(std::move(response)) {
        for (auto&& i : tags) {
            AFL_VERIFY(Calculated.emplace(i, nullptr).second);
        }
    }

    void AddResult(THashMap<ui32, std::unique_ptr<TCountMinSketch>>&& sketch) {
        {
            TGuard<TMutex> g(Mutex);
            for (auto&& i : sketch) {
                auto it = Calculated.find(i.first);
                AFL_VERIFY(it != Calculated.end());
                if (!it->second) {
                    it->second = std::move(i.second);
                } else {
                    *it->second += *i.second;
                }
            }
        }
        const i64 count = ResultsCount.Inc();
        if (count == WaitingCount.Val()) {
            OnResultReady();
        } else {
            AFL_VERIFY(count < WaitingCount.Val());
        }
    }

    void AddWaitingTask() {
        AFL_VERIFY(!Started);
        WaitingCount.Inc();
    }

    void Start() {
        AFL_VERIFY(!Started);
        Started = true;
        if (WaitingCount.Val() == ResultsCount.Val()) {
            OnResultReady();
        }
    }
};

class TColumnPortionsAccumulator {
private:
    const std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    const std::set<ui32> ColumnTagsRequested;
    std::vector<NOlap::TPortionInfo::TConstPtr> Portions;
    const ui32 PortionsCountLimit = 10000;
    std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager> DataAccessors;
    std::shared_ptr<TResultAccumulator> Result;
    const std::shared_ptr<NOlap::TVersionedIndex> VersionedIndex;

public:
    TColumnPortionsAccumulator(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager,
        const std::shared_ptr<TResultAccumulator>& result, const ui32 portionsCountLimit, const std::set<ui32>& originalColumnTags,
        const std::shared_ptr<NOlap::TVersionedIndex>& vIndex,
        const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager)
        : StoragesManager(storagesManager)
        , ColumnTagsRequested(originalColumnTags)
        , PortionsCountLimit(portionsCountLimit)
        , DataAccessors(dataAccessorsManager)
        , Result(result)
        , VersionedIndex(vIndex) {
    }

    class TIndexReadTask: public NOlap::NBlobOperations::NRead::ITask {
    private:
        using TBase = NOlap::NBlobOperations::NRead::ITask;
        const std::shared_ptr<TResultAccumulator> Result;
        THashMap<ui32, THashMap<TString, THashSet<NOlap::TBlobRange>>> RangesByColumn;
        THashMap<ui32, std::unique_ptr<TCountMinSketch>> SketchesByColumns;

    protected:
        virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) override {
            NOlap::NBlobOperations::NRead::TCompositeReadBlobs blobsData = ExtractBlobsData();
            for (auto&& [columnId, data] : RangesByColumn) {
                for (auto&& [storageId, blobs] : data) {
                    for (auto&& b : blobs) {
                        const TString blob = blobsData.ExtractVerified(storageId, b);
                        auto sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::FromString(blob.data(), blob.size()));
                        auto it = SketchesByColumns.find(columnId);
                        AFL_VERIFY(it != SketchesByColumns.end());
                        *it->second += *sketch;
                    }
                }
            }
            Result->AddResult(std::move(SketchesByColumns));
        }

        virtual bool DoOnError(
            const TString& storageId, const NOlap::TBlobRange& range, const NOlap::IBlobsReadingAction::TErrorStatus& status) override {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "DoOnError")("storage_id", storageId)("blob_id", range)(
                "status", status.GetErrorMessage())("status_code", status.GetStatus());
            AFL_VERIFY(status.GetStatus() != NKikimrProto::EReplyStatus::NODATA)("blob_id", range)("status", status.GetStatus())(
                "error", status.GetErrorMessage())("type", "STATISTICS");
            return false;
        }

    public:
        TIndexReadTask(const std::shared_ptr<TResultAccumulator>& result,
            std::vector<std::shared_ptr<NOlap::IBlobsReadingAction>>&& readingActions,
            THashMap<ui32, THashMap<TString, THashSet<NOlap::TBlobRange>>>&& rangesByColumn,
            THashMap<ui32, std::unique_ptr<TCountMinSketch>>&& readySketches)
            : TBase(std::move(readingActions), "STATISTICS", "STATISTICS")
            , Result(result)
            , RangesByColumn(std::move(rangesByColumn))
            , SketchesByColumns(std::move(readySketches)) {
            AFL_VERIFY(!!Result);
            AFL_VERIFY(RangesByColumn.size());
        }
    };

    class TMetadataSubscriber: public NOlap::IDataAccessorRequestsSubscriber {
    private:
        const std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
        const std::shared_ptr<TResultAccumulator> Result;
        std::shared_ptr<NOlap::TVersionedIndex> VersionedIndex;
        const std::set<ui32> ColumnTagsRequested;
        virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
            return Default<std::shared_ptr<const TAtomicCounter>>();
        }

        virtual void DoOnRequestsFinished(NOlap::TDataAccessorsResult&& result) override {
            THashMap<ui32, std::unique_ptr<TCountMinSketch>> sketchesByColumns;
            for (auto id : ColumnTagsRequested) {
                sketchesByColumns.emplace(id, TCountMinSketch::Create());
            }

            THashMap<ui32, THashMap<TString, THashSet<NOlap::TBlobRange>>> rangesByColumn;
            THashMap<ui32, ui32> indexIdToColumnId;

            for (const auto& [id, portionInfo] : result.GetPortions()) {
                std::shared_ptr<NOlap::ISnapshotSchema> portionSchema = portionInfo.GetPortionInfo().GetSchema(*VersionedIndex);
                for (const ui32 columnId : ColumnTagsRequested) {
                    auto indexMeta = portionSchema->GetIndexInfo().GetIndexMetaCountMinSketch({ columnId });

                    if (!indexMeta) {
                        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "Missing countMinSketch index for columnId " + ToString(columnId));
                        continue;
                    }
                    AFL_VERIFY(indexMeta->GetColumnIds().size() == 1);
                    indexIdToColumnId.emplace(indexMeta->GetIndexId(), columnId);
                    if (!indexMeta->IsInplaceData()) {
                        portionInfo.FillBlobRangesByStorage(rangesByColumn, portionSchema->GetIndexInfo(), { indexMeta->GetIndexId() });
                    } else {
                        const std::vector<TString> data = portionInfo.GetIndexInplaceDataOptional(indexMeta->GetIndexId());

                        for (const auto& sketchAsString : data) {
                            auto sketch =
                                std::unique_ptr<TCountMinSketch>(TCountMinSketch::FromString(sketchAsString.data(), sketchAsString.size()));
                            *sketchesByColumns[columnId] += *sketch;
                        }
                    }
                }
            }
            if (rangesByColumn.size()) {
                NOlap::TBlobsAction blobsAction(StoragesManager, NOlap::NBlobOperations::EConsumer::STATISTICS);
                THashMap<ui32, THashMap<TString, THashSet<NOlap::TBlobRange>>> rangesByColumnLocal;
                for (auto&& i : rangesByColumn) {
                    for (auto&& [storageId, ranges] : i.second) {
                        auto reader = blobsAction.GetReading(storageId);
                        for (auto&& i : ranges) {
                            reader->AddRange(i);
                        }
                    }
                    auto it = indexIdToColumnId.find(i.first);
                    AFL_VERIFY(it != indexIdToColumnId.end());
                    rangesByColumnLocal.emplace(it->second, std::move(i.second));
                }
                TActorContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(std::make_shared<TIndexReadTask>(
                    Result, blobsAction.GetReadingActions(), std::move(rangesByColumnLocal), std::move(sketchesByColumns))));
            } else {
                Result->AddResult(std::move(sketchesByColumns));
            }
        }

    public:
        TMetadataSubscriber(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager, const std::shared_ptr<TResultAccumulator>& result,
            const std::shared_ptr<NOlap::TVersionedIndex>& vIndex, const std::set<ui32>& tags)
            : StoragesManager(storagesManager)
            , Result(result)
            , VersionedIndex(vIndex)
            , ColumnTagsRequested(tags) {
        }
    };

    void Flush() {
        if (!Portions.size()) {
            return;
        }
        Result->AddWaitingTask();
        std::shared_ptr<NOlap::TDataAccessorsRequest> request =
            std::make_shared<NOlap::TDataAccessorsRequest>(NOlap::NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::STATISTICS_FLUSH);
        for (auto&& i : Portions) {
            request->AddPortion(i);
        }
        request->RegisterSubscriber(std::make_shared<TMetadataSubscriber>(StoragesManager, Result, VersionedIndex, ColumnTagsRequested));
        Portions.clear();
        DataAccessors->AskData(request);
    }

    void AddTask(const NOlap::TPortionInfo::TConstPtr& portion) {
        Portions.emplace_back(portion);
        if (Portions.size() >= PortionsCountLimit) {
            Flush();
        }
    }
};

void TColumnShard::Handle(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext&) {
    const auto& record = ev->Get()->Record;

    auto response = std::make_unique<NStat::TEvStatistics::TEvStatisticsResponse>();
    auto& respRecord = response->Record;
    respRecord.SetShardTabletId(TabletID());

    if (record.TypesSize() > 0 && (record.TypesSize() > 1 || record.GetTypes(0) != NKikimrStat::TYPE_COUNT_MIN_SKETCH)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "Unsupported statistic type in statistics request");

        respRecord.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_ERROR);

        Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    AFL_VERIFY(HasIndex());
    const auto& schemeShardLocalPathId = TSchemeShardLocalPathId::FromProto(record.GetTable().GetPathId());
    const auto& internalPathId = TablesManager.ResolveInternalPathId(schemeShardLocalPathId);
    AFL_VERIFY(internalPathId);
    auto index = GetIndexAs<NOlap::TColumnEngineForLogs>();
    auto spg = index.GetGranuleOptional(*internalPathId);
    AFL_VERIFY(spg);

    std::set<ui32> columnTagsRequested;
    for (ui32 tag : record.GetTable().GetColumnTags()) {
        columnTagsRequested.insert(tag);
    }
    if (columnTagsRequested.empty()) {
        auto schema = index.GetVersionedIndex().GetLastSchema();
        auto allColumnIds = schema->GetIndexInfo().GetColumnIds(false);
        columnTagsRequested = std::set<ui32>(allColumnIds.begin(), allColumnIds.end());
    }

    NOlap::TDataAccessorsRequest request(NOlap::NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::STATISTICS);
    std::shared_ptr<TResultAccumulator> resultAccumulator =
        std::make_shared<TResultAccumulator>(columnTagsRequested, ev->Sender, ev->Cookie, std::move(response));
    auto versionedIndex = std::make_shared<NOlap::TVersionedIndex>(index.GetVersionedIndex());
    TColumnPortionsAccumulator portionsPack(
        StoragesManager, resultAccumulator, 1000, columnTagsRequested, versionedIndex, DataAccessorsManager.GetObjectPtrVerified());

    for (const auto& [_, portionInfo] : spg->GetPortions()) {
        if (!portionInfo->IsVisible(GetMaxReadVersion())) {
            continue;
        }
        portionsPack.AddTask(portionInfo);
    }
    portionsPack.Flush();
    resultAccumulator->Start();
}

}   // namespace NKikimr::NColumnShard
