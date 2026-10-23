#include "config.h"

#include <ydb/core/nbs/cloud/storage/core/compat/protos/trace.pb.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

// clang-format off
#define BLOCKSTORE_DIAGNOSTICS_CONFIG(xxx)                                                               \
    xxx(HostNameScheme,                  NProto::EHostNameScheme, NProto::EHostNameScheme::HOSTNAME_RAW )\
    xxx(BastionNameSuffix,               TString,         ""                                            )\
    xxx(ViewerHostName,                  TString,         ""                                            )\
    xxx(KikimrMonPort,                   ui32,            8765                                          )\
    xxx(NbsMonPort,                      ui32,            8766                                          )\
                                                                                                         \
    xxx(SamplingRate,                    ui32,            0                                             )\
    xxx(SlowRequestSamplingRate,         ui32,            0                                             )\
    xxx(TracesUnifiedAgentEndpoint,      TString,         ""                                            )\
    xxx(TracesSyslogIdentifier,          TString,         ""                                            )\
                                                                                                         \
    xxx(ProfileLogTimeThreshold,         TDuration,       TDuration::Seconds(15)                        )\
    xxx(UseAsyncLogger,                  bool,            false                                         )\
    xxx(UnsafeLWTrace,                   bool,            false                                         )\
    xxx(LWTraceDebugInitializationQuery, TString,         ""                                            )\
    xxx(SsdPerfSettings,                TVolumePerfSettings,  {}                                        )\
    xxx(HddPerfSettings,                TVolumePerfSettings,  {}                                        )\
    xxx(NonreplPerfSettings,            TVolumePerfSettings,  {}                                        )\
    xxx(HddNonreplPerfSettings,         TVolumePerfSettings,  {}                                        )\
    xxx(Mirror2PerfSettings,            TVolumePerfSettings,  {}                                        )\
    xxx(Mirror3PerfSettings,            TVolumePerfSettings,  {}                                        )\
    xxx(LocalSSDPerfSettings,           TVolumePerfSettings,  {}                                        )\
    xxx(LocalHDDPerfSettings,           TVolumePerfSettings,  {}                                        )\
    xxx(ExpectedIoParallelism,          ui32,                 32                                        )\
    xxx(CloudIdsWithStrictSLA,          TVector<TString>,     {}                                        )\
    xxx(LWTraceShuttleCount,            ui32,                 2000                                      )\
    xxx(MonitoringUrlData,              TMonitoringUrlData,   {}                                        )\
                                                                                                         \
    xxx(CpuWaitFilename,            TString, "/sys/fs/cgroup/cpu/system.slice/nbs.service/cpuacct.wait" )\
                                                                                                         \
    xxx(PostponeTimePredictorInterval,       TDuration,       TDuration::Seconds(5)                     )\
    xxx(PostponeTimePredictorMaxTime,        TDuration,       TDuration::Seconds(20)                    )\
    xxx(PostponeTimePredictorPercentage,     double,          0.5                                       )\
    xxx(SSDDowntimeThreshold,                TDuration,       TDuration::Seconds(5)                     )\
    xxx(HDDDowntimeThreshold,                TDuration,       TDuration::Seconds(15)                    )\
    xxx(NonreplicatedSSDDowntimeThreshold,   TDuration,       TDuration::Seconds(5)                     )\
    xxx(NonreplicatedHDDDowntimeThreshold,   TDuration,       TDuration::Seconds(15)                    )\
    xxx(Mirror2SSDDowntimeThreshold,         TDuration,       TDuration::Seconds(5)                     )\
    xxx(Mirror3SSDDowntimeThreshold,         TDuration,       TDuration::Seconds(5)                     )\
    xxx(LocalSSDDowntimeThreshold,           TDuration,       TDuration::Seconds(5)                     )\
    xxx(LocalHDDDowntimeThreshold,           TDuration,       TDuration::Seconds(15)                    )\
    xxx(ReportHistogramAsMultipleCounters,   bool,            true                                      )\
    xxx(ReportHistogramAsSingleCounter,      bool,            false                                     )\
    xxx(UseMsUnitsForTimeHistogram,          bool,            false                                     )\
    xxx(StatsFetcherType, NCloud::NProto::EStatsFetcherType, NCloud::NProto::EStatsFetcherType::CGROUP  )\
                                                                                                         \
    xxx(SkipReportingZeroBlocksMetricsForYDBBasedDisks, bool, false                                     )\
    xxx(OpentelemetryTraceConfig,           ::NCloud::NProto::TOpentelemetryTraceConfig, {}             )\
                                                                                                         \
    xxx(ExecutionTimeSizeClasses,       TVector<TSizeInterval>,  {}                                     )\
    xxx(PassTraceIdToBlobstorage,       bool,                    false                                  )\
    xxx(EnableDurableVolumeInfo,        bool,                    false                                  )\
    xxx(VolumeCriticalEventsReportingMode,                                                               \
                                        NProto::EVolumeCriticalEventsReportingMode,                      \
                                                                 NProto::EVolumeCriticalEventsReportingMode::APP_ONLY)\

// BLOCKSTORE_DIAGNOSTICS_CONFIG
// clang-format on

#define BLOCKSTORE_DIAGNOSTICS_DECLARE_CONFIG(name, type, value)               \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
    // BLOCKSTORE_DIAGOSTICS_DECLARE_CONFIG

BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_DIAGNOSTICS_DECLARE_CONFIG)

#undef BLOCKSTORE_DIAGNOSTICS_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return static_cast<TTarget>(value);
}

template <>
TDuration ConvertValue<TDuration, ui32>(const ui32& value)
{
    return TDuration::MilliSeconds(value);
}

template <>
TVolumePerfSettings
ConvertValue<TVolumePerfSettings, NProto::TVolumePerfSettings>(
    const NProto::TVolumePerfSettings& value)
{
    return TVolumePerfSettings(value);
}

template <>
TMonitoringUrlData ConvertValue<TMonitoringUrlData, NProto::TMonitoringUrlData>(
    const NProto::TMonitoringUrlData& value)
{
    return TMonitoringUrlData(value);
}

template <>
TRequestThresholds ConvertValue<TRequestThresholds, TProtoRequestThresholds>(
    const TProtoRequestThresholds& value)
{
    return ConvertRequestThresholds(value);
}

template <>
TVector<TString> ConvertValue(
    const google::protobuf::RepeatedPtrField<TString>& value)
{
    TVector<TString> v;
    for (const auto& x: value) {
        v.push_back(x);
    }
    return v;
}

template <>
TVector<TSizeInterval> ConvertValue(
    const google::protobuf::RepeatedPtrField<
        NProto::TDiagnosticsConfig::TInterval>& value)
{
    TVector<TSizeInterval> v;
    for (const auto& x: value) {
        v.push_back({x.GetStart(), x.GetEnd()});
    }
    return v;
}

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

template <>
void DumpImpl(const TVector<TString>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os << value[i];
    }
}

template <>
void DumpImpl(const TVector<TSizeInterval>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os << ToString(value[i]);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfig::TDiagnosticsConfig(
    NProto::TDiagnosticsConfig diagnosticsConfig)
    : DiagnosticsConfig(std::move(diagnosticsConfig))
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
    type TDiagnosticsConfig::Get##name() const                                 \
    {                                                                          \
        return NYdb::NBS::HasField(DiagnosticsConfig, #name)                   \
                   ? ConvertValue<type>(DiagnosticsConfig.Get##name())         \
                   : Default##name;                                            \
    }                                                                          \
    // BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_CONFIG_GETTER);

#undef BLOCKSTORE_CONFIG_GETTER

TRequestThresholds TDiagnosticsConfig::GetRequestThresholds() const
{
    return ConvertValue<TRequestThresholds>(
        DiagnosticsConfig.GetRequestThresholds());
}

EHistogramCounterOptions TDiagnosticsConfig::GetHistogramCounterOptions() const
{
    EHistogramCounterOptions histogramCounterOptions;
    if (GetReportHistogramAsMultipleCounters()) {
        histogramCounterOptions |=
            EHistogramCounterOption::ReportMultipleCounters;
    }
    if (GetReportHistogramAsSingleCounter()) {
        histogramCounterOptions |= EHistogramCounterOption::ReportSingleCounter;
    }
    if (GetUseMsUnitsForTimeHistogram()) {
        histogramCounterOptions |=
            EHistogramCounterOption::UseMsUnitsForTimeHistogram;
    }
    return histogramCounterOptions;
}

void TDiagnosticsConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
    // BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TDiagnosticsConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER () {                                                                \
        TABLED () {                                                            \
            out << #name;                                                      \
        }                                                                      \
        TABLED () {                                                            \
            DumpImpl(Get##name(), out);                                        \
        }                                                                      \
    }                                                                          \
    // BLOCKSTORE_CONFIG_DUMP

    HTML (out) {
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

TDuration GetDowntimeThreshold(
    const TDiagnosticsConfig& config,
    NCloud::NProto::EStorageMediaKind kind)
{
    switch (kind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            return config.GetSSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
            return config.GetNonreplicatedSSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: {
            return config.GetNonreplicatedHDDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: {
            return config.GetMirror3SSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: {
            return config.GetMirror2SSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL: {
            return config.GetLocalSSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL: {
            return config.GetLocalHDDDowntimeThreshold();
        }
        default: {
            return config.GetHDDDowntimeThreshold();
        }
    }
}

}   // namespace NCloud::NBlockStore

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NCloud::NBlockStore::NProto::EHostNameScheme>(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EHostNameScheme scheme)
{
    out << NCloud::NBlockStore::NProto::EHostNameScheme_Name(scheme);
}

template <>
void Out<NCloud::NBlockStore::NProto::EVolumeCriticalEventsReportingMode>(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EVolumeCriticalEventsReportingMode mode)
{
    out << NCloud::NBlockStore::NProto::EVolumeCriticalEventsReportingMode_Name(
        mode);
}

template <>
void Out<NCloud::NBlockStore::TVolumePerfSettings>(
    IOutputStream& out,
    const NCloud::NBlockStore::TVolumePerfSettings& value)
{
    NCloud::NBlockStore::NProto::TVolumePerfSettings v;
    v.MutableRead()->SetIops(value.Read.Iops);
    v.MutableRead()->SetBandwidth(value.Read.Bandwidth);
    v.MutableWrite()->SetIops(value.Write.Iops);
    v.MutableWrite()->SetBandwidth(value.Write.Bandwidth);
    v.SetCriticalFactor(value.CriticalFactor);
    v.SetThrottlerOvercommit(value.ThrottlerOvercommit);
    v.MutableMinRead()->SetIops(value.MinRead.Iops);
    v.MutableMinRead()->SetBandwidth(value.MinRead.Bandwidth);
    v.MutableMinWrite()->SetIops(value.MinWrite.Iops);
    v.MutableMinWrite()->SetBandwidth(value.MinWrite.Bandwidth);

    SerializeToTextFormat(v, out);
}

template <>
void Out<NCloud::NBlockStore::TMonitoringUrlData>(
    IOutputStream& out,
    const NCloud::NBlockStore::TMonitoringUrlData& value)
{
    NCloud::NBlockStore::NProto::TMonitoringUrlData v;
    v.SetMonitoringClusterName(value.MonitoringClusterName);
    v.SetMonitoringUrl(value.MonitoringUrl);
    v.SetMonitoringProject(value.MonitoringProject);
    v.SetMonitoringVolumeDashboard(value.MonitoringVolumeDashboard);
    v.SetMonitoringPartitionDashboard(value.MonitoringPartitionDashboard);
    v.SetMonitoringNBSAlertsDashboard(value.MonitoringNBSAlertsDashboard);
    v.SetMonitoringNBSTVDashboard(value.MonitoringNBSTVDashboard);
    v.SetMonitoringUrlTemplate(value.MonitoringUrlTemplate);
    SerializeToTextFormat(v, out);
}

template <>
void Out<NCloud::TRequestThresholds>(
    IOutputStream& out,
    const NCloud::TRequestThresholds& value)
{
    OutRequestThresholds(out, value);
}

template <>
void Out<NCloud::NProto::EStatsFetcherType>(
    IOutputStream& out,
    NCloud::NProto::EStatsFetcherType statsFetcherType)
{
    out << NCloud::NProto::EStatsFetcherType_Name(statsFetcherType);
}
