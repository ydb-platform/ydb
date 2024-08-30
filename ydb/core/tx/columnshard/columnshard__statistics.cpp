#include "columnshard.h"
#include "columnshard_impl.h"
#include "ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <ydb/library/minsketch/count_min_sketch.h>


namespace NKikimr::NColumnShard {

void TColumnShard::Handle(NStat::TEvStatistics::TEvAnalyzeTable::TPtr& ev, const TActorContext&) {
    auto& requestRecord = ev->Get()->Record;

    auto response = std::make_unique<NStat::TEvStatistics::TEvAnalyzeTableResponse>();
    auto& responseRecord = response->Record;
    responseRecord.SetOperationId(requestRecord.GetOperationId());
    responseRecord.MutablePathId()->CopyFrom(requestRecord.GetTable().GetPathId());
    responseRecord.SetShardTabletId(TabletID());

    if (requestRecord.TypesSize() > 0 && (requestRecord.TypesSize() > 1 || requestRecord.GetTypes(0) != NKikimrStat::TYPE_COUNT_MIN_SKETCH)) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "Unsupported statistic type in analyze request");

        Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    // TODO Start a potentially long analysis process.
    // ...



    Send(ev->Sender, response.release(), 0, ev->Cookie);
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

    auto actualSchema = index.GetVersionedIndex().GetLastCriticalSchema();
    AFL_VERIFY(actualSchema);
    // ui32 totalVisiblePortions = 0;
    // ui32 actualVisiblePortions = 0;

    for (const auto& [_, portionInfo] : spg->GetPortions()) {
        if (portionInfo->IsVisible(GetMaxReadVersion())) {
            std::shared_ptr<NOlap::ISnapshotSchema> portionSchema = portionInfo->GetSchema(index.GetVersionedIndex());
            // totalVisiblePortions++;
            // if (portionSchema->GetVersion() >= actualSchema->GetVersion()) {
            //     actualVisiblePortions++;
            // }
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

    // double freshness = totalVisiblePortions > 0 ? (double)actualVisiblePortions / (double)totalVisiblePortions : 0.;

    // Cerr << ">>> freshness is " << freshness << Endl;

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
