#include "columnshard.h"
#include "columnshard_impl.h"
#include "ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <yql/essentials/core/minsketch/count_min_sketch.h>


namespace NKikimr::NColumnShard {

class TAnalyzeSubscriber: public NSubscriber::ISubscriber {
private:
    const TActorId OriginalSender;
    const ui64 OriginalCookie;
    const TActorContext& Context;
    std::unique_ptr<NStat::TEvStatistics::TEvAnalyzeTableResponse> ResponseDraft;

public:
    virtual std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return { NSubscriber::EEventType::AppendCompleted };
    }

    virtual bool DoOnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev, TColumnShard& shard) override {
        AFL_VERIFY(ev->GetType() == NSubscriber::EEventType::AppendCompleted);

        auto& responseRecord = ResponseDraft->Record;

        AFL_VERIFY(shard.AnalyzeOperationsInFlight.erase(responseRecord.GetOperationId()));
        responseRecord.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        return Context.Send(OriginalSender, ResponseDraft.release(), 0, OriginalCookie);
    }

    virtual bool IsFinished() const override {
        return !ResponseDraft;
    }

    TAnalyzeSubscriber(const TActorId& sender,
                       const ui64 cookie,
                       const TActorContext& ctx,
                       std::unique_ptr<NStat::TEvStatistics::TEvAnalyzeTableResponse>&& response)
        : OriginalSender(sender)
        , OriginalCookie(cookie)
        , Context(ctx)
        , ResponseDraft(std::move(response))
    {
    }
};


void TColumnShard::Handle(NStat::TEvStatistics::TEvAnalyzeTable::TPtr& ev, const TActorContext& ctx) {
    auto& requestRecord = ev->Get()->Record;

    const TString operationId = requestRecord.GetOperationId();

    auto response = std::make_unique<NStat::TEvStatistics::TEvAnalyzeTableResponse>();
    auto& responseRecord = response->Record;
    responseRecord.SetOperationId(operationId);
    responseRecord.MutablePathId()->CopyFrom(requestRecord.GetTable().GetPathId());
    responseRecord.SetShardTabletId(TabletID());

    AFL_VERIFY(HasIndex());
    auto index = GetIndexAs<NOlap::TColumnEngineForLogs>();
    auto spg = index.GetGranuleOptional(requestRecord.GetTable().GetPathId().GetLocalId());
    AFL_VERIFY(spg);

    auto actualSchema = index.GetVersionedIndex().GetLastCriticalSchema();
    if (!actualSchema) {  // never was actualized
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("warning", "Table was never actualized, it could mean no statistics enabled");

        responseRecord.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;

    }
    ui64 actualVersion = actualSchema->GetVersion();
    ui32 totalVisiblePortions = 0;
    ui32 actualVisiblePortions = 0;

    for (const auto& [_, portionInfo] : spg->GetPortions()) {
        if (portionInfo->IsVisible(GetMaxReadVersion())) {
            std::shared_ptr<NOlap::ISnapshotSchema> portionSchema = portionInfo->GetSchema(index.GetVersionedIndex());
            totalVisiblePortions++;
            if (portionSchema->GetVersion() >= actualVersion) {
                actualVisiblePortions++;
            }
        }
    }

    if (totalVisiblePortions > 0 && actualVisiblePortions == totalVisiblePortions) {
        responseRecord.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        Send(ev->Sender, response.release(), 0, ev->Cookie);
    } else {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "wait_analyze");

        if (AnalyzeOperationsInFlight.find(operationId) != AnalyzeOperationsInFlight.end()) {
            // retried request, may contain new sender, so resubscribe
            Subscribers->UnregisterSubscriber(AnalyzeOperationsInFlight[operationId]);
        }

        auto subscriber = std::make_shared<TAnalyzeSubscriber>(ev->Sender, ev->Cookie, ctx, std::move(response));
        AnalyzeOperationsInFlight[operationId] = subscriber;
        Subscribers->RegisterSubscriber(std::move(subscriber));
    }
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
        , Response(std::move(response))
    {
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
    const std::set<ui32> ColumnTagsRequested;
    std::vector<NOlap::TPortionInfo::TConstPtr> Portions;
    const ui32 PortionsCountLimit = 10000;
    std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager> DataAccessors;
    std::shared_ptr<TResultAccumulator> Result;
    const std::shared_ptr<NOlap::TVersionedIndex> VersionedIndex;

public:
    TColumnPortionsAccumulator(const std::shared_ptr<TResultAccumulator>& result, const ui32 portionsCountLimit,
        const std::set<ui32>& originalColumnTags, const std::shared_ptr<NOlap::TVersionedIndex>& vIndex,
        const std::shared_ptr<NOlap::NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager)
        : ColumnTagsRequested(originalColumnTags)
        , PortionsCountLimit(portionsCountLimit)
        , DataAccessors(dataAccessorsManager)
        , Result(result)
        , VersionedIndex(vIndex)
    {
    }

    class TMetadataSubscriber: public NOlap::IDataAccessorRequestsSubscriber {
    private:
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

            for (const auto& [id, portionInfo] : result.GetPortions()) {
                std::shared_ptr<NOlap::ISnapshotSchema> portionSchema = portionInfo.GetPortionInfo().GetSchema(*VersionedIndex);
                for (const ui32 columnId : ColumnTagsRequested) {
                    auto indexMeta = portionSchema->GetIndexInfo().GetIndexMetaCountMinSketch({ columnId });

                    if (!indexMeta) {
                        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "Missing countMinSketch index for columnId " + ToString(columnId));
                        continue;
                    }
                    AFL_VERIFY(indexMeta->GetColumnIds().size() == 1);

                    const std::vector<TString> data = portionInfo.GetIndexInplaceDataVerified(indexMeta->GetIndexId());

                    for (const auto& sketchAsString : data) {
                        auto sketch =
                            std::unique_ptr<TCountMinSketch>(TCountMinSketch::FromString(sketchAsString.data(), sketchAsString.size()));
                        *sketchesByColumns[columnId] += *sketch;
                    }
                }
            }
            Result->AddResult(std::move(sketchesByColumns));
        }

    public:
        TMetadataSubscriber(
            const std::shared_ptr<TResultAccumulator>& result, const std::shared_ptr<NOlap::TVersionedIndex>& vIndex, const std::set<ui32>& tags)
            : Result(result)
            , VersionedIndex(vIndex)
            , ColumnTagsRequested(tags)
        {

        }
    };

    void Flush() {
        if (!Portions.size()) {
            return;
        }
        Result->AddWaitingTask();
        std::shared_ptr<NOlap::TDataAccessorsRequest> request = std::make_shared<NOlap::TDataAccessorsRequest>("STATISTICS_FLUSH");
        for (auto&& i : Portions) {
            request->AddPortion(i);
        }
        request->RegisterSubscriber(std::make_shared<TMetadataSubscriber>(Result, VersionedIndex, ColumnTagsRequested));
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
    auto index = GetIndexAs<NOlap::TColumnEngineForLogs>();
    auto spg = index.GetGranuleOptional(record.GetTable().GetPathId().GetLocalId());
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

    NOlap::TDataAccessorsRequest request("STATISTICS");
    std::shared_ptr<TResultAccumulator> resultAccumulator =
        std::make_shared<TResultAccumulator>(columnTagsRequested, ev->Sender, ev->Cookie, std::move(response));
    auto versionedIndex = std::make_shared<NOlap::TVersionedIndex>(index.GetVersionedIndex());
    TColumnPortionsAccumulator portionsPack(resultAccumulator, 1000, columnTagsRequested, versionedIndex, DataAccessorsManager.GetObjectPtrVerified());

    for (const auto& [_, portionInfo] : spg->GetPortions()) {
        if (!portionInfo->IsVisible(GetMaxReadVersion())) {
            continue;
        }
        portionsPack.AddTask(portionInfo);
    }
    portionsPack.Flush();
    resultAccumulator->Start();
}

}
