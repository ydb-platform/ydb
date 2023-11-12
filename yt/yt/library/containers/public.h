#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/porto/proto/rpc.pb.h>
#include <library/cpp/yt/misc/enum.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

const int PortoErrorCodeBase = 12000;

DEFINE_ENUM(EPortoErrorCode,
    ((Success)                       ((PortoErrorCodeBase + Porto::EError::Success)))
    ((Unknown)                       ((PortoErrorCodeBase + Porto::EError::Unknown)))
    ((InvalidMethod)                 ((PortoErrorCodeBase + Porto::EError::InvalidMethod)))
    ((ContainerAlreadyExists)        ((PortoErrorCodeBase + Porto::EError::ContainerAlreadyExists)))
    ((ContainerDoesNotExist)         ((PortoErrorCodeBase + Porto::EError::ContainerDoesNotExist)))
    ((InvalidProperty)               ((PortoErrorCodeBase + Porto::EError::InvalidProperty)))
    ((InvalidData)                   ((PortoErrorCodeBase + Porto::EError::InvalidData)))
    ((InvalidValue)                  ((PortoErrorCodeBase + Porto::EError::InvalidValue)))
    ((InvalidState)                  ((PortoErrorCodeBase + Porto::EError::InvalidState)))
    ((NotSupported)                  ((PortoErrorCodeBase + Porto::EError::NotSupported)))
    ((ResourceNotAvailable)          ((PortoErrorCodeBase + Porto::EError::ResourceNotAvailable)))
    ((Permission)                    ((PortoErrorCodeBase + Porto::EError::Permission)))
    ((VolumeAlreadyExists)           ((PortoErrorCodeBase + Porto::EError::VolumeAlreadyExists)))
    ((VolumeNotFound)                ((PortoErrorCodeBase + Porto::EError::VolumeNotFound)))
    ((NoSpace)                       ((PortoErrorCodeBase + Porto::EError::NoSpace)))
    ((Busy)                          ((PortoErrorCodeBase + Porto::EError::Busy)))
    ((VolumeAlreadyLinked)           ((PortoErrorCodeBase + Porto::EError::VolumeAlreadyLinked)))
    ((VolumeNotLinked)               ((PortoErrorCodeBase + Porto::EError::VolumeNotLinked)))
    ((LayerAlreadyExists)            ((PortoErrorCodeBase + Porto::EError::LayerAlreadyExists)))
    ((LayerNotFound)                 ((PortoErrorCodeBase + Porto::EError::LayerNotFound)))
    ((NoValue)                       ((PortoErrorCodeBase + Porto::EError::NoValue)))
    ((VolumeNotReady)                ((PortoErrorCodeBase + Porto::EError::VolumeNotReady)))
    ((InvalidCommand)                ((PortoErrorCodeBase + Porto::EError::InvalidCommand)))
    ((LostError)                     ((PortoErrorCodeBase + Porto::EError::LostError)))
    ((DeviceNotFound)                ((PortoErrorCodeBase + Porto::EError::DeviceNotFound)))
    ((InvalidPath)                   ((PortoErrorCodeBase + Porto::EError::InvalidPath)))
    ((InvalidNetworkAddress)         ((PortoErrorCodeBase + Porto::EError::InvalidNetworkAddress)))
    ((PortoFrozen)                   ((PortoErrorCodeBase + Porto::EError::PortoFrozen)))
    ((LabelNotFound)                 ((PortoErrorCodeBase + Porto::EError::LabelNotFound)))
    ((InvalidLabel)                  ((PortoErrorCodeBase + Porto::EError::InvalidLabel)))
    ((NotFound)                      ((PortoErrorCodeBase + Porto::EError::NotFound)))
    ((SocketError)                   ((PortoErrorCodeBase + Porto::EError::SocketError)))
    ((SocketUnavailable)             ((PortoErrorCodeBase + Porto::EError::SocketUnavailable)))
    ((SocketTimeout)                 ((PortoErrorCodeBase + Porto::EError::SocketTimeout)))
    ((Taint)                         ((PortoErrorCodeBase + Porto::EError::Taint)))
    ((Queued)                        ((PortoErrorCodeBase + Porto::EError::Queued)))
);

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((FailedToStartContainer)  (14000))
);

DEFINE_ENUM(EStatField,
    // CPU
    (CpuUsage)
    (CpuUserUsage)
    (CpuSystemUsage)
    (CpuWait)
    (CpuThrottled)
    (ContextSwitches)
    (ContextSwitchesDelta)
    (ThreadCount)
    (CpuLimit)
    (CpuGuarantee)

    // Memory
    (Rss)
    (MappedFile)
    (MajorPageFaults)
    (MinorPageFaults)
    (FileCacheUsage)
    (AnonMemoryUsage)
    (AnonMemoryLimit)
    (MemoryUsage)
    (MemoryGuarantee)
    (MemoryLimit)
    (MaxMemoryUsage)
    (OomKills)
    (OomKillsTotal)

    // IO
    (IOReadByte)
    (IOWriteByte)
    (IOBytesLimit)
    (IOReadOps)
    (IOWriteOps)
    (IOOps)
    (IOOpsLimit)
    (IOTotalTime)
    (IOWaitTime)

    // Network
    (NetTxBytes)
    (NetTxPackets)
    (NetTxDrops)
    (NetTxLimit)
    (NetRxBytes)
    (NetRxPackets)
    (NetRxDrops)
    (NetRxLimit)
);

DEFINE_ENUM(EEnablePorto,
    (None)
    (Isolate)
    (Full)
);

struct TBind
{
    TString SourcePath;
    TString TargetPath;
    bool ReadOnly;
};

struct TRootFS
{
    TString RootPath;
    bool IsRootReadOnly;
    std::vector<TBind> Binds;
};

struct TDevice
{
    TString DeviceName;
    bool Enabled;
};

struct TInstanceLimits
{
    double Cpu = 0;
    i64 Memory = 0;
    std::optional<i64> NetTx;
    std::optional<i64> NetRx;

    bool operator==(const TInstanceLimits&) const = default;
};

DECLARE_REFCOUNTED_STRUCT(IContainerManager)
DECLARE_REFCOUNTED_STRUCT(IInstanceLauncher)
DECLARE_REFCOUNTED_STRUCT(IInstance)
DECLARE_REFCOUNTED_STRUCT(IPortoExecutor)

DECLARE_REFCOUNTED_CLASS(TPortoHealthChecker)
DECLARE_REFCOUNTED_CLASS(TInstanceLimitsTracker)
DECLARE_REFCOUNTED_CLASS(TPortoProcess)
DECLARE_REFCOUNTED_CLASS(TPortoResourceTracker)
DECLARE_REFCOUNTED_CLASS(TPortoExecutorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TPodSpecConfig)

////////////////////////////////////////////////////////////////////////////////

bool IsValidCGroupType(const TString& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
