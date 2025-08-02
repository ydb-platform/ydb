#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TAggregate
{
    DEFINE_BYVAL_RW_PROPERTY(T, Total);
    DEFINE_BYVAL_RW_PROPERTY(T, Max);
    DEFINE_BYREF_RW_PROPERTY(std::string, ArgmaxNode);

public:
    TAggregate<T>() = default;

    void operator=(T value);

    void Merge(const TAggregate<T>& other);
};

////////////////////////////////////////////////////////////////////////////////

struct TExecutionStatistics
{
    i64 RowsRead = {};
    i64 DataWeightRead = {};
    i64 RowsWritten = {};
    i64 MemoryUsage = {};
    i64 GroupedRowCount = {};

    TDuration SyncTime;
    TDuration AsyncTime;
    TDuration ExecuteTime;
    TDuration ReadTime;
    TDuration WriteTime;
    TDuration CodegenTime;
    TDuration WaitOnReadyEventTime;

    bool IncompleteInput = false;
    bool IncompleteOutput = false;
};

struct TQueryStatistics
{
    TAggregate<i64> RowsRead;
    TAggregate<i64> DataWeightRead;
    TAggregate<i64> RowsWritten;
    TAggregate<i64> MemoryUsage;
    TAggregate<i64> GroupedRowCount;

    TAggregate<TDuration> SyncTime;
    TAggregate<TDuration> AsyncTime;
    TAggregate<TDuration> ExecuteTime;
    TAggregate<TDuration> ReadTime;
    TAggregate<TDuration> WriteTime;
    TAggregate<TDuration> CodegenTime;
    TAggregate<TDuration> WaitOnReadyEventTime;

    bool IncompleteInput = false;
    bool IncompleteOutput = false;
    i64 QueryCount = 1;

    std::vector<TQueryStatistics> InnerStatistics;

    void AddInnerStatistics(TQueryStatistics statistics);

    void Merge(const TQueryStatistics& statistics);

    static TQueryStatistics FromExecutionStatistics(const TExecutionStatistics& statistics);
};


void ToProto(NProto::TQueryStatistics* serialized, const TQueryStatistics& original);
void FromProto(TQueryStatistics* original, const NProto::TQueryStatistics& serialized);

void FormatValue(TStringBuilderBase* builder, const TQueryStatistics& stat, TStringBuf spec);

void Serialize(const TQueryStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
