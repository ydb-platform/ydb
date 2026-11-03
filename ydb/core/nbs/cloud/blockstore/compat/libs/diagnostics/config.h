#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/config/diagnostics.pb.h>

#include <ydb/core/nbs/cloud/storage/core/compat/libs/diagnostics/trace_reader.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/size_interval.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram_counter_options.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore {

using NYdb::NBS::EHistogramCounterOption;
using NYdb::NBS::EHistogramCounterOptions;
using NYdb::NBS::TSizeInterval;

////////////////////////////////////////////////////////////////////////////////

struct TVolumePerfSettings: public TAtomicRefCount<TVolumePerfSettings>
{
    struct TPerformanceProfile
    {
        ui32 Iops = 0;
        ui64 Bandwidth = 0;

        bool operator==(const TPerformanceProfile& rhs) const = default;

        [[nodiscard]] bool IsValid() const
        {
            return Iops != 0 && Bandwidth != 0;
        }
    };

    TPerformanceProfile Read;
    TPerformanceProfile Write;

    ui32 CriticalFactor = 0;
    double ThrottlerOvercommit = 1.0;

    TPerformanceProfile MinRead;
    TPerformanceProfile MinWrite;

    TVolumePerfSettings() = default;
    TVolumePerfSettings(const TVolumePerfSettings& rhs) = default;

    TVolumePerfSettings(
        ui32 readIops,
        ui64 readBandwidth,
        ui32 writeIops,
        ui64 writeBandwidth,
        ui32 criticalFactor,
        double throttlerOvercommit)
        : Read(readIops, readBandwidth)
        , Write(writeIops, writeBandwidth)
        , CriticalFactor(criticalFactor)
        , ThrottlerOvercommit(throttlerOvercommit)
    {}

    TVolumePerfSettings(const NProto::TVolumePerfSettings& settings)
        : Read(settings.GetRead().GetIops(), settings.GetRead().GetBandwidth())
        , Write(
              settings.GetWrite().GetIops(),
              settings.GetWrite().GetBandwidth())
        , CriticalFactor(settings.GetCriticalFactor())
        , ThrottlerOvercommit(
              settings.HasThrottlerOvercommit()
                  ? settings.GetThrottlerOvercommit()
                  : 1.0)
        , MinRead(
              settings.GetMinRead().GetIops(),
              settings.GetMinRead().GetBandwidth())
        , MinWrite(
              settings.GetMinWrite().GetIops(),
              settings.GetMinWrite().GetBandwidth())
    {}

    bool IsValid() const
    {
        return Read.IsValid() && Write.IsValid();
    }

    bool operator==(const TVolumePerfSettings& rhs) const
    {
        return Read == rhs.Read && Write == rhs.Write &&
               CriticalFactor == rhs.CriticalFactor;
    }

    bool operator!=(const TVolumePerfSettings& rhs) const = default;
};

////////////////////////////////////////////////////////////////////////////////

struct TMonitoringUrlData: public TAtomicRefCount<TMonitoringUrlData>
{
    TString MonitoringClusterName;
    TString MonitoringUrl;
    TString MonitoringProject;
    TString MonitoringVolumeDashboard;
    TString MonitoringPartitionDashboard;
    TString MonitoringNBSAlertsDashboard;
    TString MonitoringNBSTVDashboard;
    TString MonitoringYDBProject;
    TString MonitoringYDBGroupDashboard;
    TString MonitoringUrlTemplate;

    TMonitoringUrlData()
        : MonitoringProject("nbs")
    {}

    TMonitoringUrlData(const TMonitoringUrlData& rhs) = default;

    explicit TMonitoringUrlData(const NProto::TMonitoringUrlData& data)
        : MonitoringClusterName(data.GetMonitoringClusterName())
        , MonitoringUrl(data.GetMonitoringUrl())
        , MonitoringProject(data.GetMonitoringProject())
        , MonitoringVolumeDashboard(data.GetMonitoringVolumeDashboard())
        , MonitoringPartitionDashboard(data.GetMonitoringPartitionDashboard())
        , MonitoringNBSAlertsDashboard(data.GetMonitoringNBSAlertsDashboard())
        , MonitoringNBSTVDashboard(data.GetMonitoringNBSTVDashboard())
        , MonitoringYDBProject(data.GetMonitoringYDBProject())
        , MonitoringYDBGroupDashboard(data.GetMonitoringYDBGroupDashboard())
        , MonitoringUrlTemplate(data.GetMonitoringUrlTemplate())
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticsConfig
{
private:
    const NProto::TDiagnosticsConfig DiagnosticsConfig;

public:
    TDiagnosticsConfig(NProto::TDiagnosticsConfig diagnosticsConfig = {});

    NProto::EHostNameScheme GetHostNameScheme() const;
    TString GetBastionNameSuffix() const;
    TString GetViewerHostName() const;
    ui32 GetKikimrMonPort() const;
    ui32 GetNbsMonPort() const;
    ui32 GetSamplingRate() const;
    ui32 GetSlowRequestSamplingRate() const;
    TString GetTracesUnifiedAgentEndpoint() const;
    TString GetTracesSyslogIdentifier() const;
    TDuration GetProfileLogTimeThreshold() const;
    bool GetUseAsyncLogger() const;

    bool GetUnsafeLWTrace() const;
    TString GetLWTraceDebugInitializationQuery() const;
    ui32 GetLWTraceShuttleCount() const;

    TVolumePerfSettings GetSsdPerfSettings() const;
    TVolumePerfSettings GetHddPerfSettings() const;
    TVolumePerfSettings GetNonreplPerfSettings() const;
    TVolumePerfSettings GetHddNonreplPerfSettings() const;
    TVolumePerfSettings GetMirror2PerfSettings() const;
    TVolumePerfSettings GetMirror3PerfSettings() const;
    TVolumePerfSettings GetLocalSSDPerfSettings() const;
    TVolumePerfSettings GetLocalHDDPerfSettings() const;
    ui32 GetExpectedIoParallelism() const;
    TVector<TString> GetCloudIdsWithStrictSLA() const;
    TMonitoringUrlData GetMonitoringUrlData() const;

    TString GetCpuWaitFilename() const;

    TDuration GetPostponeTimePredictorInterval() const;
    TDuration GetPostponeTimePredictorMaxTime() const;
    double GetPostponeTimePredictorPercentage() const;

    TDuration GetSSDDowntimeThreshold() const;
    TDuration GetHDDDowntimeThreshold() const;
    TDuration GetNonreplicatedSSDDowntimeThreshold() const;
    TDuration GetNonreplicatedHDDDowntimeThreshold() const;
    TDuration GetMirror3SSDDowntimeThreshold() const;
    TDuration GetMirror2SSDDowntimeThreshold() const;
    TDuration GetLocalSSDDowntimeThreshold() const;
    TDuration GetLocalHDDDowntimeThreshold() const;
    bool GetReportHistogramAsMultipleCounters() const;
    bool GetReportHistogramAsSingleCounter() const;

    TRequestThresholds GetRequestThresholds() const;
    EHistogramCounterOptions GetHistogramCounterOptions() const;
    bool GetUseMsUnitsForTimeHistogram() const;

    NCloud::NProto::EStatsFetcherType GetStatsFetcherType() const;

    bool GetSkipReportingZeroBlocksMetricsForYDBBasedDisks() const;

    [[nodiscard]] NCloud::NProto::TOpentelemetryTraceConfig
    GetOpentelemetryTraceConfig() const;

    [[nodiscard]] TVector<TSizeInterval> GetExecutionTimeSizeClasses() const;

    [[nodiscard]] bool GetPassTraceIdToBlobstorage() const;

    [[nodiscard]] bool GetEnableDurableVolumeInfo() const;

    [[nodiscard]] NProto::EVolumeCriticalEventsReportingMode
    GetVolumeCriticalEventsReportingMode() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

// Returns the DowntimeThreshold corresponding to the media kind.
TDuration GetDowntimeThreshold(
    const TDiagnosticsConfig& config,
    NCloud::NProto::EStorageMediaKind kind);

}   // namespace NCloud::NBlockStore
