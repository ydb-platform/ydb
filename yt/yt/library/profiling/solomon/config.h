#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TShardConfig
    : public NYTree::TYsonStruct
{
    std::vector<std::string> Filter;

    std::optional<TDuration> GridStep;

    REGISTER_YSON_STRUCT(TShardConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShardConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSolomonExporterConfig
    : public NYTree::TYsonStruct
{
    TDuration GridStep;

    TDuration LingerTimeout;

    int WindowSize;

    int ThreadPoolSize;
    int EncodingThreadPoolSize;
    TDuration ThreadPoolPollingPeriod;
    TDuration EncodingThreadPoolPollingPeriod;

    bool ConvertCountersToRateForSolomon;
    bool RenameConvertedCounters;
    bool ConvertCountersToDeltaGauge;
    bool EnableHistogramCompat;

    bool ExportSummary;
    bool ExportSummaryAsMax;
    bool ExportSummaryAsAvg;

    bool MarkAggregates;

    bool StripSensorsNamePrefix;

    bool EnableCoreProfilingCompatibility;

    bool EnableSelfProfiling;

    bool ReportBuildInfo;

    bool ReportKernelVersion;

    bool ReportRestart;

    TDuration ResponseCacheTtl;

    TDuration ReadDelay;

    std::optional<std::string> Host;

    THashMap<std::string, std::string> InstanceTags;

    THashMap<std::string, TShardConfigPtr> Shards;

    TDuration UpdateSensorServiceTreePeriod;

    int ProducerCollectionBatchSize;

    ELabelSanitizationPolicy LabelSanitizationPolicy;

    TShardConfigPtr MatchShard(const std::string& sensorName);

    ESummaryPolicy GetSummaryPolicy() const;

    REGISTER_YSON_STRUCT(TSolomonExporterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSolomonExporterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
