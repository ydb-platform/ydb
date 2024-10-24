#include "columnshard.h"
#include "columnshard_impl.h"
#include "ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <ydb/library/minsketch/count_min_sketch.h>


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

    std::map<ui32, std::unique_ptr<TCountMinSketch>> sketchesByColumns;
    for (auto id : columnTagsRequested) {
        sketchesByColumns.emplace(id, TCountMinSketch::Create());
    }

    for (const auto& [_, portionInfo] : spg->GetPortions()) {
        if (portionInfo->IsVisible(GetMaxReadVersion())) {
            std::shared_ptr<NOlap::ISnapshotSchema> portionSchema = portionInfo->GetSchema(index.GetVersionedIndex());
            for (ui32 columnId : columnTagsRequested) {
                auto indexMeta = portionSchema->GetIndexInfo().GetIndexMetaCountMinSketch({columnId});

                if (!indexMeta) {
                    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "Missing countMinSketch index for columnId " + ToString(columnId));
                    continue;
                }
                AFL_VERIFY(indexMeta->GetColumnIds().size() == 1);

                const std::vector<TString> data = portionInfo->GetIndexInplaceDataVerified(indexMeta->GetIndexId());

                for (const auto& sketchAsString : data) {
                    auto sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::FromString(sketchAsString.data(), sketchAsString.size()));
                    *sketchesByColumns[columnId] += *sketch;
                }
            }
        }
    }

    respRecord.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS);

    for (ui32 columnTag : columnTagsRequested) {
        auto* column = respRecord.AddColumns();
        column->SetTag(columnTag);

        auto* statistic = column->AddStatistics();
        statistic->SetType(NStat::COUNT_MIN_SKETCH);
        statistic->SetData(TString(sketchesByColumns[columnTag]->AsStringBuf()));
    }

    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

}
