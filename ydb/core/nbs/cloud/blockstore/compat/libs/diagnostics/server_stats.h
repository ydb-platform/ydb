#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/incomplete_requests.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/executor_counters.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/stats_updater.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>

#include <span>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TMetricRequest
{
    const EBlockStoreRequest RequestType;
    TString ClientId;
    TString DiskId;
    TString Peer;
    IVolumeInfoPtr VolumeInfo;
    ui64 StartIndex = 0;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::STORAGE_MEDIA_HDD;
    ui64 RequestBytes = 0;
    TInstant RequestTimestamp;
    bool Unaligned = false;
    bool CellRequest = false;
    NProto::EVolumeAccessMode AccessMode =
        NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_WRITE;
    NProto::EVolumeMountMode MountMode =
        NProto::EVolumeMountMode::VOLUME_MOUNT_LOCAL;

    TMetricRequest(EBlockStoreRequest requestType)
        : RequestType(requestType)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct IServerStats: public NYdb::NBS::IStats
{
    virtual NYdb::NBS::TExecutorCounters::TExecutorScope StartExecutor() = 0;

    virtual NMonitoring::TDynamicCounters::TCounterPtr GetEndpointCounter(
        NProto::EClientIpcType ipcType) = 0;

    virtual bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) = 0;

    virtual void UnmountVolume(
        const TString& diskId,
        const TString& clientId) = 0;

    virtual void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) = 0;

    virtual ui32 GetBlockSize(const TString& diskId) const = 0;

    virtual void PrepareMetricRequest(
        TMetricRequest& metricRequest,
        TString clientId,
        TString diskId,
        ui64 startIndex,
        ui64 requestBytes,
        bool unaligned) = 0;

    virtual void RequestStarted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const TString& message = {}) = 0;

    virtual void RequestAcquired(
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void RequestSent(
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void ResponseReceived(
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void ResponseSent(
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void RequestCompleted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error) = 0;

    virtual void RequestFastPathHit(
        const TString& diskId,
        const TString& clientId,
        EBlockStoreRequest requestType) = 0;

    virtual void ReportException(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId) = 0;

    virtual void ReportInfo(
        TLog& Log,
        EBlockStoreRequest requestType,
        ui64 requestId,
        const TString& diskId,
        const TString& clientId,
        const TString& message) = 0;

    virtual void AddIncompleteRequest(
        TCallContext& callContext,
        IVolumeInfoPtr volumeInfo,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime time) = 0;

    using TTimeBucket = std::pair<TDuration, ui64>;
    using TSizeBucket = std::pair<ui64, ui64>;

    virtual void BatchCompleted(
        TMetricRequest& metricRequest,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IServerStatsPtr CreateServerStats(
    NYdb::NBS::NBlockStore::IDumpablePtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    NYdb::NBS::IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats);

IServerStatsPtr CreateClientStats(
    NYdb::NBS::NBlockStore::IDumpablePtr config,
    NYdb::NBS::IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    TString instanceId);

IServerStatsPtr CreateServerStatsStub();

}   // namespace NCloud::NBlockStore
