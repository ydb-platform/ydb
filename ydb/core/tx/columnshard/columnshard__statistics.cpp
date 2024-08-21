#include "columnshard.h"
#include "columnshard_impl.h"
#include "ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <ydb/library/minsketch/count_min_sketch.h>


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

void TColumnShard::Handle(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext&) {
    AFL_VERIFY(HasIndex());
    auto index = GetIndexAs<NOlap::TColumnEngineForLogs>();
    auto spg = index.GetGranuleOptional(ev->Get()->Record.GetTable().GetPathId().GetLocalId());
    AFL_VERIFY(spg);

    std::set<ui32> columnTagsRequested;
    for (ui32 tag : ev->Get()->Record.GetTable().GetColumnTags()) {
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

    for (const auto& [indexKey, keyPortions] : spg->GetPortionsIndex().GetPoints()) {
        for (auto&& [_, portionInfo] : keyPortions.GetStart()) {
            std::shared_ptr<NOlap::ISnapshotSchema> portionSchema = portionInfo->GetSchema(index.GetVersionedIndex());
            for (ui32 columnId : columnTagsRequested) {
                auto indexMeta = portionSchema->GetIndexInfo().GetIndexCountMinSketch({columnId});

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

    auto response = std::make_unique<NStat::TEvStatistics::TEvStatisticsResponse>();
    auto& record = response->Record;
    record.SetShardTabletId(TabletID());

    record.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS);

    for (ui32 columnTag : columnTagsRequested) {
        auto* column = record.AddColumns();
        column->SetTag(columnTag);

        auto* statistic = column->AddStatistics();
        statistic->SetType(NStat::COUNT_MIN_SKETCH);
        statistic->SetData(TString(sketchesByColumns[columnTag]->AsStringBuf()));
    }

    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

}
