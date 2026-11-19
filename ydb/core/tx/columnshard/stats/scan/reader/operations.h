#pragma once

#include <ydb/core/protos/columnshard_scan_stats.pb.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace arrow {
class Schema;
}

namespace NKikimr::NOlap::NStats::NScan {

NColumnShardStats::TOperation MakeStepStatisticsDefinition(NColumnShardStats::TOperation::DetailsCase kind, TStringBuf key);
NColumnShardStats::TOperation MakeMetastepStatisticsDefinition(NColumnShardStats::TOperation::DetailsCase kind);

NColumnShardStats::TOperation MakePreparation(const TString& tablePath);
NColumnShardStats::TOperation MakePrepareRead();
NColumnShardStats::TOperation MakePrepareReadMetadata();
NColumnShardStats::TOperation MakeReaderInit();
NColumnShardStats::TOperation MakeSelectPortions();
NColumnShardStats::TOperation MakeParseProgram();
NColumnShardStats::TOperation MakeReader();
NColumnShardStats::TOperation MakeOutput();
NColumnShardStats::TOperation MakeSend();
NColumnShardStats::TOperation MakeAck();
NColumnShardStats::TOperation MakeStorageRead();
NColumnShardStats::TOperation MakeReadySources();
NColumnShardStats::TOperation MakeReadyIntervals();
NColumnShardStats::TOperation MakeConflictDetection();
NColumnShardStats::TOperation MakeSourceOutput();
NColumnShardStats::TOperation MakeSourcesAggregation();
NColumnShardStats::TOperation MakeSourceResult();
NColumnShardStats::TOperation MakePagePlanning();
NColumnShardStats::TOperation MakeDuplicateFilter();
NColumnShardStats::TOperation MakeDuplicateMerge(const arrow::Schema& schema, bool hasIntervals);

}   // namespace NKikimr::NOlap::NStats::NScan
