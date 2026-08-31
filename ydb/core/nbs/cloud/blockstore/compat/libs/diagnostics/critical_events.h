#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/printable_params.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/volume_labels.h>

#include <util/string/printf.h>
#include <util/system/src_location.h>

#include <utility>

namespace NCloud::NBlockStore {

using NYdb::NBS::NBlockStore::MakeVolumeLabels;
using NYdb::NBS::NBlockStore::PrintParams;
using NYdb::NBS::NBlockStore::TPrintableValue;
using NYdb::NBS::NBlockStore::TVolumeLabels;
using NYdb::NBS::NBlockStore::TVolumeLabelsConstPtr;

using TCritEventParams =
    std::initializer_list<std::pair<TStringBuf, TPrintableValue>>;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CRITICAL_EVENTS(xxx)                                        \
    xxx(VhostQueueRunningError)                                                \
    xxx(PublishDiskStateError)                                                 \
    xxx(EndpointRestoringError)                                                \
    xxx(HangingYdbStatsRequest)                                                \
    xxx(UserNotificationError)                                                 \
    xxx(BackupPathDescriptionsFailure)                                         \
    xxx(RdmaError)                                                             \
    xxx(CounterUpdateRace)                                                     \
    xxx(EndpointStartingError)                                                 \
    xxx(DiskRegistryBackupFailed)                                              \
    xxx(RegisterAgentWithEmptyRackName)                                        \
    xxx(ManuallyPreemptedVolumesFileError)                                     \
    xxx(ServiceProxyWakeupTimerHit)                                            \
    xxx(ReceivedUnknownTaskId)                                                 \
    xxx(UnexpectedBatchMigration)                                              \
    xxx(FreshDeviceNotFoundInConfig)                                           \
    xxx(DiskRegistryDeviceNotFoundSoft)                                        \
    xxx(DiskRegistrySourceDiskNotFound)                                        \
    xxx(EndpointSwitchFailure)                                                 \
    xxx(ExternalEndpointUnexpectedExit)                                        \
    xxx(DiskRegistryResumeDeviceFailed)                                        \
    xxx(DiskRegistryAgentDevicePoolConfigMismatch)                             \
    xxx(DiskRegistryPurgeHostError)                                            \
    xxx(DiskRegistryOccupiedDeviceConfigurationHasChanged)                     \
    xxx(DiskRegistryWrongMigratedDeviceOwnership)                              \
    xxx(DiskRegistryInitialAgentRejectionThresholdExceeded)                    \
    xxx(DiskAgentInconsistentMultiWriteResponse)                               \
    xxx(WrongCellIdInDescribeVolume)                                           \
    xxx(DiskRegistryStateIntegrityBroken)                                      \
    // BLOCKSTORE_CRITICAL_EVENTS

#define BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(xxx)                             \
    xxx(AcquiredDiskEraseAttempt)                                              \
    xxx(DiskAgentConfigMismatch)                                               \
    xxx(DiskAgentDeviceSymlinkMismatch)                                        \
    xxx(DiskAgentIoDuringSecureErase)                                          \
    xxx(DiskAgentSecureEraseDuringIo)                                          \
    xxx(DiskAgentSessionCacheRestoreError)                                     \
    xxx(DiskAgentSessionCacheUpdateError)                                      \
    xxx(UnexpectedIdentifierRepetition)                                        \
    xxx(ChaosGeneratedError)                                                   \
    // BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS

#define BLOCKSTORE_IMPOSSIBLE_EVENTS(xxx)                                      \
    xxx(TabletCommitIdOverflow)                                                \
    xxx(TabletCollectCounterOverflow)                                          \
    xxx(DiskRegistryLogicalPhysicalBlockSizeMismatch)                          \
    xxx(DiskRegistryAgentDeviceNodeIdMismatch)                                 \
    xxx(DiskRegistryPoolDeviceRackMismatch)                                    \
    xxx(DiskRegistryAgentNotFound)                                             \
    xxx(DiskRegistryBadDeviceSizeAdjustment)                                   \
    xxx(DiskRegistryBadDeviceStateAdjustment)                                  \
    xxx(DiskRegistryDuplicateDiskInPlacementGroup)                             \
    xxx(DiskRegistryInvalidPlacementGroupPartition)                            \
    xxx(DiskRegistryDeviceLocationNotFound)                                    \
    xxx(DiskRegistryDiskNotFound)                                              \
    xxx(DiskRegistryPlacementGroupNotFound)                                    \
    xxx(DiskRegistryDeviceListReferencesNonexistentDisk)                       \
    xxx(DiskRegistryPlacementGroupDiskNotFound)                                \
    xxx(DiskRegistryDeviceNotFound)                                            \
    xxx(DiskRegistryNoScheduledNotification)                                   \
    xxx(DiskRegistryDeviceDoesNotBelongToDisk)                                 \
    xxx(DiskRegistryCouldNotAddOutdatedLaggingDevice)                          \
    xxx(DiskRegistryReplicaTableReplaceError)                                  \
    xxx(ResyncUnexpectedWriteOrZeroCounter)                                    \
    xxx(MonitoringResourceNotFound)                                            \
    xxx(DiskRegistryUnexpectedAffectedDisks)                                   \
    xxx(ReadBlockCountMismatch)                                                \
    xxx(CancelRoutineIsNotSet)                                                 \
    xxx(FieldDescriptorNotFound)                                               \
    xxx(DiskRegistryInsertToPendingCleanupFailed)                              \
    xxx(OverlappingRangesDuringMigrationDetected)                              \
    xxx(StartExternalEndpointError)                                            \
    xxx(EmptyRequestSgList)                                                    \
    xxx(LaggingAgentsProxyWrongRecipientActor)                                 \
    xxx(UnexpectedCookie)                                                      \
    xxx(MultiAgentRequestAffectsTwoDevices)                                    \
    xxx(ChecksumCalculationError)                                              \
    xxx(LogicalDiskIdMismatch)                                                 \
    xxx(DeviceReplacementContractBroken)                                       \
    xxx(InflightRequestInvariantViolation)                                     \
    xxx(SetupChannelsOnWrongMediaKindVolume)                                   \
    xxx(DiskRegistryDetachPathWithDependentDisk)                               \
    xxx(DiskDevicesSizeViolation)                                              \
    xxx(RdmaMessageTypeMismatch)                                               \
    xxx(BlockChecksumAbsent)                                                   \
    xxx(CleanupBlobMetaBlocksMismatch)                                         \
    xxx(Bug) /* General software bug event.                                    \
                Used for non-specialized or unclassified errors. */            \
    // BLOCKSTORE_IMPOSSIBLE_EVENTS

/* Report AppImpossibeEvent/Bug with source location log */
#define REPORT_BUG(message, ...)                                               \
    ::NCloud::NBlockStore::ReportBug(                                          \
        (TStringBuilder() << __LOCATION__ << ": " << (message)) __VA_OPT__(, ) \
            __VA_ARGS__)

#define BLOCKSTORE_VOLUME_CRITICAL_EVENTS(xxx)                                 \
    xxx(InvalidTabletConfig)                                                   \
    xxx(ReassignTablet)                                                        \
    xxx(TabletBSFailure)                                                       \
    xxx(DiskAllocationFailure)                                                 \
    xxx(CollectGarbageError)                                                   \
    xxx(MigrationFailed)                                                       \
    xxx(BadMigrationConfig)                                                    \
    xxx(InitFreshBlocksError)                                                  \
    xxx(TrimFreshLogError)                                                     \
    xxx(NrdDestructionError)                                                   \
    xxx(FailedToStartVolumeLocally)                                            \
    xxx(MirroredDiskAllocationCleanupFailure)                                  \
    xxx(MirroredDiskAllocationPlacementGroupCleanupFailure)                    \
    xxx(MirroredDiskDeviceReplacementForbidden)                                \
    xxx(MirroredDiskDeviceReplacementFailure)                                  \
    xxx(MirroredDiskDeviceReplacementRateLimitExceeded)                        \
    xxx(MirroredDiskMinorityChecksumMismatch)                                  \
    xxx(MirroredDiskMajorityChecksumMismatch)                                  \
    xxx(MirroredDiskChecksumMismatchUponRead)                                  \
    xxx(MirroredDiskAddTagFailed)                                              \
    xxx(ResyncFailed)                                                          \
    xxx(AddConfirmedBlobsError)                                                \
    xxx(ConfirmBlobsError)                                                     \
    xxx(MigrationSourceNotFound)                                               \
    xxx(BlockDigestMismatchInBlob)                                             \
    xxx(ErrorWasSentToTheGuestForReliableDisk)                                 \
    xxx(ErrorWasSentToTheGuestForNonReliableDisk)                              \
    xxx(MirroredDiskResyncChecksumMismatch)                                    \
    xxx(ReleaseShadowDiskError)                                                \
    xxx(TrimFreshLogTimeout)                                                   \
    xxx(AddFreshBlocksResultedInError)                                         \
    xxx(OverlappingRequestsDetected)                                           \
    xxx(CrossPartitionRequestDetected)                                         \
    // BLOCKSTORE_VOLUME_CRITICAL_EVENTS

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE(name)                        \
    TString Report##name(const TString& message = "");                         \
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues);                                    \
    TString Report##name(const TCritEventParams& keyValues);                   \
    const TString GetCriticalEventFor##name();                                 \
    // BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE

BLOCKSTORE_CRITICAL_EVENTS(BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE(name)             \
    TString Report##name(const TString& message = "");                         \
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues);                                    \
    TString Report##name(const TCritEventParams& keyValues);                   \
    const TString GetCriticalEventFor##name();                                 \
    // BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE

BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(
    BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_DISK_AGENT_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE(name)                      \
    TString Report##name(const TString& message = "");                         \
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues);                                    \
    TString Report##name(const TCritEventParams& keyValues);                   \
    const TString GetCriticalEventFor##name();                                 \
    // BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE
BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE

#define BLOCKSTORE_DECLARE_VOLUME_CRITICAL_EVENT_ROUTINE(name)                 \
    const TString GetVolumeCriticalEventFor##name();                           \
    const TString GetAppCriticalEventFor##name();                              \
    TString Report##name(                                                      \
        const TString& diskId,                                                 \
        const TString& cloudId,                                                \
        const TString& folderId,                                               \
        const TString& message = "");                                          \
    TString Report##name(                                                      \
        const TString& diskId,                                                 \
        const TString& cloudId,                                                \
        const TString& folderId,                                               \
        const TString& message,                                                \
        const TCritEventParams& keyValues);                                    \
    TString Report##name(                                                      \
        const TString& diskId,                                                 \
        const TString& cloudId,                                                \
        const TString& folderId,                                               \
        const TCritEventParams& keyValues);                                    \
    template <typename... TArgs>                                               \
    TString Report##name(const TVolumeLabels& volumeLabels, TArgs&&... args)   \
    {                                                                          \
        return Report##name(                                                   \
            volumeLabels.DiskId,                                               \
            volumeLabels.CloudId,                                              \
            volumeLabels.FolderId,                                             \
            std::forward<TArgs>(args)...);                                     \
    }                                                                          \
    template <typename... TArgs>                                               \
    TString Report##name(                                                      \
        const TVolumeLabelsConstPtr& volumeLabels,                             \
        TArgs&&... args)                                                       \
    {                                                                          \
        if (!volumeLabels) {                                                   \
            REPORT_BUG(Sprintf(                                                \
                "volumeLabels = nullptr provided for %s report, "              \
                "monitoring metrics will not be updated",                      \
                GetVolumeCriticalEventFor##name().c_str()));                   \
        }                                                                      \
        const auto& labels = volumeLabels                                      \
                                 ? volumeLabels                                \
                                 : MakeVolumeLabels("<nullptr>", "", "");      \
        return Report##name(*labels, std::forward<TArgs>(args)...);            \
    }                                                                          \
    // BLOCKSTORE_DECLARE_VOLUME_CRITICAL_EVENT_ROUTINE

BLOCKSTORE_VOLUME_CRITICAL_EVENTS(
    BLOCKSTORE_DECLARE_VOLUME_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DECLARE_VOLUME_CRITICAL_EVENT_ROUTINE

}   // namespace NCloud::NBlockStore
