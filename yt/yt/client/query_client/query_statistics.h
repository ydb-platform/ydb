#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueryStatistics
{
    i64 RowsRead = 0;
    i64 DataWeightRead = 0;
    i64 RowsWritten = 0;
    TDuration SyncTime;
    TDuration AsyncTime;
    TDuration ExecuteTime;
    TDuration ReadTime;
    TDuration WriteTime;
    TDuration CodegenTime;
    TDuration WaitOnReadyEventTime;
    bool IncompleteInput = false;
    bool IncompleteOutput = false;
    size_t MemoryUsage = 0;

    std::vector<TQueryStatistics> InnerStatistics;

    void AddInnerStatistics(TQueryStatistics statistics);
};

void ToProto(NProto::TQueryStatistics* serialized, const TQueryStatistics& original);
void FromProto(TQueryStatistics* original, const NProto::TQueryStatistics& serialized);

void FormatValue(TStringBuilderBase* builder, const TQueryStatistics& stat, TStringBuf spec);

void Serialize(const TQueryStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
