#include "user_counter.h"

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram_types.h>

#include <array>

namespace NYdb::NBS::NBlockStore::NUserCounter {

using namespace NMonitoring;
using namespace NYdb::NBS::NStorage::NUserStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf DISK_READ_OPS = "disk.read_ops";
constexpr TStringBuf DISK_READ_OPS_BURST = "disk.read_ops_burst";
constexpr TStringBuf DISK_READ_OPS_IN_FLIGHT = "disk.read_ops_in_flight";
constexpr TStringBuf DISK_READ_OPS_IN_FLIGHT_BURST =
    "disk.read_ops_in_flight_burst";
constexpr TStringBuf DISK_READ_BYTES = "disk.read_bytes";
constexpr TStringBuf DISK_READ_BYTES_BURST = "disk.read_bytes_burst";
constexpr TStringBuf DISK_READ_BYTES_IN_FLIGHT = "disk.read_bytes_in_flight";
constexpr TStringBuf DISK_READ_BYTES_IN_FLIGHT_BURST =
    "disk.read_bytes_in_flight_burst";
constexpr TStringBuf DISK_READ_ERRORS = "disk.read_errors";
constexpr TStringBuf DISK_READ_LATENCY = "disk.read_latency";
constexpr TStringBuf DISK_READ_THROTTLER_DELAY = "disk.read_throttler_delay";
constexpr TStringBuf DISK_WRITE_OPS = "disk.write_ops";
constexpr TStringBuf DISK_WRITE_OPS_BURST = "disk.write_ops_burst";
constexpr TStringBuf DISK_WRITE_OPS_IN_FLIGHT = "disk.write_ops_in_flight";
constexpr TStringBuf DISK_WRITE_OPS_IN_FLIGHT_BURST =
    "disk.write_ops_in_flight_burst";
constexpr TStringBuf DISK_WRITE_BYTES = "disk.write_bytes";
constexpr TStringBuf DISK_WRITE_BYTES_BURST = "disk.write_bytes_burst";
constexpr TStringBuf DISK_WRITE_BYTES_IN_FLIGHT = "disk.write_bytes_in_flight";
constexpr TStringBuf DISK_WRITE_BYTES_IN_FLIGHT_BURST =
    "disk.write_bytes_in_flight_burst";
constexpr TStringBuf DISK_WRITE_ERRORS = "disk.write_errors";
constexpr TStringBuf DISK_WRITE_LATENCY = "disk.write_latency";
constexpr TStringBuf DISK_WRITE_THROTTLER_DELAY = "disk.write_throttler_delay";
constexpr TStringBuf DISK_IO_QUOTA = "disk.io_quota_utilization_percentage";
constexpr TStringBuf DISK_IO_QUOTA_BURST =
    "disk.io_quota_utilization_percentage_burst";

TLabels MakeVolumeLabels(
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId)
{
    return {
        {"service", "compute"},
        {"project", cloudId},
        {"cluster", folderId},
        {"disk", diskId}};
}

TLabels MakeVolumeInstanceLabels(
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId)
{
    auto volumeLabels = MakeVolumeLabels(cloudId, folderId, diskId);
    volumeLabels.Add("instance", instanceId);

    return volumeLabels;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RegisterServiceVolume(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    EHistogramCounterOptions histogramCounterOptions,
    TDynamicCounterPtr src)
{
    const auto commonLabels = MakeVolumeLabels(cloudId, folderId, diskId);

    AddUserMetric(dsc, commonLabels, {{src, "UsedQuota"}}, DISK_IO_QUOTA);
    AddUserMetric(
        dsc,
        commonLabels,
        {{src, "MaxUsedQuota"}},
        DISK_IO_QUOTA_BURST);

    auto readSub = src->FindSubgroup("request", "ReadBlocks");
    AddHistogramUserMetric(
        GetUsBuckets(),
        dsc,
        commonLabels,
        {{readSub, "ThrottlerDelay"}},
        DISK_READ_THROTTLER_DELAY,
        histogramCounterOptions);

    auto writeSub = src->FindSubgroup("request", "WriteBlocks");
    auto zeroSub = src->FindSubgroup("request", "ZeroBlocks");
    AddHistogramUserMetric(
        GetUsBuckets(),
        dsc,
        commonLabels,
        {{writeSub, "ThrottlerDelay"}, {zeroSub, "ThrottlerDelay"}},
        DISK_WRITE_THROTTLER_DELAY,
        histogramCounterOptions);
}

void UnregisterServiceVolume(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId)
{
    const auto commonLabels = MakeVolumeLabels(cloudId, folderId, diskId);

    dsc.RemoveUserMetric(commonLabels, DISK_READ_THROTTLER_DELAY);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_THROTTLER_DELAY);
    dsc.RemoveUserMetric(commonLabels, DISK_IO_QUOTA);
    dsc.RemoveUserMetric(commonLabels, DISK_IO_QUOTA_BURST);
}

void RegisterServerVolumeInstance(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId,
    const bool reportZeroBlocksMetrics,
    EHistogramCounterOptions histogramCounterOptions,
    TDynamicCounterPtr src)
{
    if (instanceId.empty()) {
        return;
    }

    auto commonLabels =
        MakeVolumeInstanceLabels(cloudId, folderId, diskId, instanceId);

    auto readSub = src->FindSubgroup("request", "ReadBlocks");
    AddUserMetric(dsc, commonLabels, {{readSub, "Count"}}, DISK_READ_OPS);
    AddUserMetric(
        dsc,
        commonLabels,
        {{readSub, "MaxCount"}},
        DISK_READ_OPS_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        {{readSub, "Errors/Fatal"}},
        DISK_READ_ERRORS);
    AddUserMetric(
        dsc,
        commonLabels,
        {{readSub, "RequestBytes"}},
        DISK_READ_BYTES);
    AddUserMetric(
        dsc,
        commonLabels,
        {{readSub, "MaxRequestBytes"}},
        DISK_READ_BYTES_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        {{readSub, "InProgress"}},
        DISK_READ_OPS_IN_FLIGHT);
    AddUserMetric(
        dsc,
        commonLabels,
        {{readSub, "MaxInProgress"}},
        DISK_READ_OPS_IN_FLIGHT_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        {{readSub, "InProgressBytes"}},
        DISK_READ_BYTES_IN_FLIGHT);
    AddUserMetric(
        dsc,
        commonLabels,
        {{readSub, "MaxInProgressBytes"}},
        DISK_READ_BYTES_IN_FLIGHT_BURST);
    AddHistogramUserMetric(
        GetTimeBuckets(histogramCounterOptions),
        dsc,
        commonLabels,
        {{readSub, "Time"}},
        DISK_READ_LATENCY,
        histogramCounterOptions);

    auto writeSubgroup = src->FindSubgroup("request", "WriteBlocks");
    auto zeroSubgroup = src->FindSubgroup("request", "ZeroBlocks");
    auto getWriteCounters = [&](const TString& name)
    {
        auto counters = TVector<TBaseDynamicCounters>({{writeSubgroup, name}});
        if (reportZeroBlocksMetrics) {
            counters.push_back({zeroSubgroup, name});
        }
        return counters;
    };

    AddUserMetric(dsc, commonLabels, getWriteCounters("Count"), DISK_WRITE_OPS);
    AddUserMetric(
        dsc,
        commonLabels,
        getWriteCounters("MaxCount"),
        DISK_WRITE_OPS_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        getWriteCounters("Errors/Fatal"),
        DISK_WRITE_ERRORS);
    AddUserMetric(
        dsc,
        commonLabels,
        getWriteCounters("RequestBytes"),
        DISK_WRITE_BYTES);
    AddUserMetric(
        dsc,
        commonLabels,
        getWriteCounters("MaxRequestBytes"),
        DISK_WRITE_BYTES_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        getWriteCounters("InProgress"),
        DISK_WRITE_OPS_IN_FLIGHT);
    AddUserMetric(
        dsc,
        commonLabels,
        getWriteCounters("MaxInProgress"),
        DISK_WRITE_OPS_IN_FLIGHT_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        getWriteCounters("InProgressBytes"),
        DISK_WRITE_BYTES_IN_FLIGHT);
    AddUserMetric(
        dsc,
        commonLabels,
        getWriteCounters("MaxInProgressBytes"),
        DISK_WRITE_BYTES_IN_FLIGHT_BURST);
    AddHistogramUserMetric(
        GetTimeBuckets(histogramCounterOptions),
        dsc,
        commonLabels,
        getWriteCounters("Time"),
        DISK_WRITE_LATENCY,
        histogramCounterOptions);
}

void UnregisterServerVolumeInstance(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId)
{
    if (instanceId.empty()) {
        return;
    }

    const auto commonLabels =
        MakeVolumeInstanceLabels(cloudId, folderId, diskId, instanceId);

    dsc.RemoveUserMetric(commonLabels, DISK_READ_OPS);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_OPS_BURST);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_OPS_IN_FLIGHT);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_OPS_IN_FLIGHT_BURST);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_ERRORS);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_BYTES);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_BYTES_BURST);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_BYTES_IN_FLIGHT);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_BYTES_IN_FLIGHT_BURST);
    dsc.RemoveUserMetric(commonLabels, DISK_READ_LATENCY);

    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_OPS);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_OPS_BURST);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_OPS_IN_FLIGHT);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_OPS_IN_FLIGHT_BURST);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_ERRORS);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_BYTES);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_BYTES_BURST);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_BYTES_IN_FLIGHT);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_BYTES_IN_FLIGHT_BURST);
    dsc.RemoveUserMetric(commonLabels, DISK_WRITE_LATENCY);
}

}   // namespace NYdb::NBS::NBlockStore::NUserCounter
