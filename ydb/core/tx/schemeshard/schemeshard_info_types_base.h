#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_info_types_helpers.h"
#include "schemeshard_path_element.h"
#include "schemeshard_tx_infly.h"
#include "schemeshard_types.h"

#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/message_seqno.h>

#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NKikimr {

bool PartitionConfigHasExternalBlobsEnabled(const NKikimrSchemeOp::TPartitionConfig& partitionConfig);

namespace NSchemeShard {

class TSchemeShard;

struct TForceShardSplitSettings {
    ui64 ForceShardSplitDataSize;
    bool DisableForceShardSplit;
};

struct TSplitSettings {
    TControlWrapper SplitMergePartCountLimit;
    TControlWrapper FastSplitSizeThreshold;
    TControlWrapper FastSplitRowCountThreshold;
    TControlWrapper FastSplitCpuPercentageThreshold;
    TControlWrapper SplitByLoadEnabled;
    TControlWrapper SplitByLoadMaxShardsDefault;
    TControlWrapper MergeByLoadMinUptimeSec;
    TControlWrapper MergeByLoadMinLowLoadDurationSec;
    TControlWrapper ForceShardSplitDataSize;
    TControlWrapper DisableForceShardSplit;

    TSplitSettings()
        : SplitMergePartCountLimit(2000, -1, 1000000)
        , FastSplitSizeThreshold(4*1000*1000, 100*1000, 4ll*1000*1000*1000)
        , FastSplitRowCountThreshold(100*1000, 1000, 1ll*1000*1000*1000)
        , FastSplitCpuPercentageThreshold(50, 1, 146)
        , SplitByLoadEnabled(1, 0, 1)
        , SplitByLoadMaxShardsDefault(50, 0, 10000)
        , MergeByLoadMinUptimeSec(10*60, 0, 4ll*1000*1000*1000)
        , MergeByLoadMinLowLoadDurationSec(1*60*60, 0, 4ll*1000*1000*1000)
        , ForceShardSplitDataSize(2ULL * 1024 * 1024 * 1024, 10 * 1024 * 1024, 16ULL * 1024 * 1024 * 1024)
        , DisableForceShardSplit(0, 0, 1)
    {}

    void Register(TIntrusivePtr<NKikimr::TControlBoard>& icb) {
        TControlBoard::RegisterSharedControl(SplitMergePartCountLimit,         icb->SchemeShardControls.SplitMergePartCountLimit);
        TControlBoard::RegisterSharedControl(FastSplitSizeThreshold,           icb->SchemeShardControls.FastSplitSizeThreshold);
        TControlBoard::RegisterSharedControl(FastSplitRowCountThreshold,       icb->SchemeShardControls.FastSplitRowCountThreshold);
        TControlBoard::RegisterSharedControl(FastSplitCpuPercentageThreshold,  icb->SchemeShardControls.FastSplitCpuPercentageThreshold);

        TControlBoard::RegisterSharedControl(SplitByLoadEnabled,               icb->SchemeShardControls.SplitByLoadEnabled);
        TControlBoard::RegisterSharedControl(SplitByLoadMaxShardsDefault,      icb->SchemeShardControls.SplitByLoadMaxShardsDefault);
        TControlBoard::RegisterSharedControl(MergeByLoadMinUptimeSec,          icb->SchemeShardControls.MergeByLoadMinUptimeSec);
        TControlBoard::RegisterSharedControl(MergeByLoadMinLowLoadDurationSec, icb->SchemeShardControls.MergeByLoadMinLowLoadDurationSec);

        TControlBoard::RegisterSharedControl(ForceShardSplitDataSize,          icb->SchemeShardControls.ForceShardSplitDataSize);
        TControlBoard::RegisterSharedControl(DisableForceShardSplit,           icb->SchemeShardControls.DisableForceShardSplit);
    }

    TForceShardSplitSettings GetForceShardSplitSettings() const {
        return TForceShardSplitSettings{
            .ForceShardSplitDataSize = ui64(ForceShardSplitDataSize),
            .DisableForceShardSplit = ui64(DisableForceShardSplit) != 0,
        };
    }
};

struct TBackupToS3Settings {
    // Async Replication
    TControlWrapper EnableAsyncReplicationExport;
    TControlWrapper EnableAsyncReplicationImport;
    // Transfer
    TControlWrapper EnableTransferExport;
    TControlWrapper EnableTransferImport;
    // External Data Source
    TControlWrapper EnableExternalDataSourceExport;
    TControlWrapper EnableExternalDataSourceImport;
    // External Table
    TControlWrapper EnableExternalTableExport;
    TControlWrapper EnableExternalTableImport;
    // System Views
    TControlWrapper EnableSysViewPermissionsExport;
    TControlWrapper EnableSysViewPermissionsImport;

    TBackupToS3Settings()
        : EnableAsyncReplicationExport(1, 0, 1)
        , EnableAsyncReplicationImport(1, 0, 1)
        , EnableTransferExport(1, 0, 1)
        , EnableTransferImport(1, 0, 1)
        , EnableExternalDataSourceExport(1, 0, 1)
        , EnableExternalDataSourceImport(1, 0, 1)
        , EnableExternalTableExport(1, 0, 1)
        , EnableExternalTableImport(1, 0, 1)
        , EnableSysViewPermissionsExport(1, 0, 1)
        , EnableSysViewPermissionsImport(1, 0, 1)
    {}

    void Register(TIntrusivePtr<NKikimr::TControlBoard>& icb) {
        TControlBoard::RegisterSharedControl(EnableAsyncReplicationExport, icb->BackupControls.S3Controls.EnableAsyncReplicationExport);
        TControlBoard::RegisterSharedControl(EnableAsyncReplicationImport, icb->BackupControls.S3Controls.EnableAsyncReplicationImport);

        TControlBoard::RegisterSharedControl(EnableTransferExport, icb->BackupControls.S3Controls.EnableTransferExport);
        TControlBoard::RegisterSharedControl(EnableTransferImport, icb->BackupControls.S3Controls.EnableTransferImport);

        TControlBoard::RegisterSharedControl(EnableExternalDataSourceExport, icb->BackupControls.S3Controls.EnableExternalDataSourceExport);
        TControlBoard::RegisterSharedControl(EnableExternalDataSourceImport, icb->BackupControls.S3Controls.EnableExternalDataSourceImport);

        TControlBoard::RegisterSharedControl(EnableExternalTableExport, icb->BackupControls.S3Controls.EnableExternalTableExport);
        TControlBoard::RegisterSharedControl(EnableExternalTableImport, icb->BackupControls.S3Controls.EnableExternalTableImport);

        TControlBoard::RegisterSharedControl(EnableSysViewPermissionsExport, icb->BackupControls.S3Controls.EnableSysViewPermissionsExport);
        TControlBoard::RegisterSharedControl(EnableSysViewPermissionsImport, icb->BackupControls.S3Controls.EnableSysViewPermissionsImport);
    }
};

struct TBackupSettings {
    TBackupToS3Settings S3Settings;

    TBackupSettings() = default;

    void Register(TIntrusivePtr<NKikimr::TControlBoard>& icb) {
        S3Settings.Register(icb);
    }
};

// Per-incremental restore orchestrator rate-limit settings. Sentinel -1 means unbounded.
struct TIncrementalRestoreSettings {
    TControlWrapper MaxIncrementalRestoreTablesInFlight;
    // Wall-clock deadlines (seconds). Sentinel -1 disables the deadline.
    TControlWrapper MaxIncrementalRestoreOverallDurationSeconds;
    TControlWrapper MaxIncrementalRestoreStageDurationSeconds;

    TIncrementalRestoreSettings()
        : MaxIncrementalRestoreTablesInFlight(32, -1, 1000000)
        , MaxIncrementalRestoreOverallDurationSeconds(86400, -1, 604800)
        , MaxIncrementalRestoreStageDurationSeconds(86400, -1, 604800)
    {}

    void Register(TIntrusivePtr<NKikimr::TControlBoard>& icb) {
        TControlBoard::RegisterSharedControl(MaxIncrementalRestoreTablesInFlight,
                                             icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);
        TControlBoard::RegisterSharedControl(MaxIncrementalRestoreOverallDurationSeconds,
                                             icb->SchemeShardControls.MaxIncrementalRestoreOverallDurationSeconds);
        TControlBoard::RegisterSharedControl(MaxIncrementalRestoreStageDurationSeconds,
                                             icb->SchemeShardControls.MaxIncrementalRestoreStageDurationSeconds);
    }
};


struct TAdoptedShard {
    ui64 PrevOwner;
    TLocalShardIdx PrevShardIdx;
};

struct TShardInfo {
    TTabletId TabletID = InvalidTabletId;
    TTxId CurrentTxId = InvalidTxId; ///< @note we support only one modifying transaction on shard at time
    TPathId PathId = InvalidPathId;
    TTabletTypes::EType TabletType = ETabletType::TypeInvalid;
    TChannelsBindings BindedChannels;

    TShardInfo(TTxId txId, TPathId pathId, TTabletTypes::EType type)
       : CurrentTxId(txId)
       , PathId(pathId)
       , TabletType(type)
    {}

    TShardInfo() = default;
    TShardInfo(const TShardInfo& other) = default;
    TShardInfo &operator=(const TShardInfo& other) = default;

    TShardInfo&& WithTabletID(TTabletId tabletId) && {
        TabletID = tabletId;
        return std::move(*this);
    }

    TShardInfo WithTabletID(TTabletId tabletId) const & {
        TShardInfo copy = *this;
        copy.TabletID = tabletId;
        return copy;
    }

    TShardInfo&& WithTabletType(TTabletTypes::EType tabletType) && {
        TabletType = tabletType;
        return std::move(*this);
    }

    TShardInfo WithTabletType(TTabletTypes::EType tabletType) const & {
        TShardInfo copy = *this;
        copy.TabletType = tabletType;
        return copy;
    }

    TShardInfo&& WithBindedChannels(TChannelsBindings bindedChannels) && {
        BindedChannels = std::move(bindedChannels);
        return std::move(*this);
    }

    TShardInfo WithBindedChannels(TChannelsBindings bindedChannels) const & {
        TShardInfo copy = *this;
        copy.BindedChannels = std::move(bindedChannels);
        return copy;
    }

    static TShardInfo RtmrPartitionInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::RTMRPartition);
    }

    static TShardInfo SolomonPartitionInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::KeyValue);
    }

    static TShardInfo DataShardInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::DataShard);
    }

    static TShardInfo PersQShardInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::PersQueue);
    }

    static TShardInfo PQBalancerShardInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::PersQueueReadBalancer);
    }

    static TShardInfo BlockStoreVolumeInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStoreVolume);
    }

    static TShardInfo BlockStorePartitionInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStorePartition);
    }

    static TShardInfo BlockStorePartition2Info(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStorePartition2);
    }

    static TShardInfo BlockStoreVolumeDirectInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStoreVolumeDirect);
    }

    static TShardInfo BlockStorePartitionDirectInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlockStorePartitionDirect);
    }

    static TShardInfo FileStoreInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::FileStore);
    }

    static TShardInfo KesusInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::Kesus);
    }

    static TShardInfo ColumnShardInfo(TTxId txId, TPathId pathId) {
         return TShardInfo(txId, pathId, ETabletType::ColumnShard);
    }

    static TShardInfo SequenceShardInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::SequenceShard);
    }

    static TShardInfo ReplicationControllerInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::ReplicationController);
    }

    static TShardInfo BlobDepotInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::BlobDepot);
    }

    static TShardInfo TestShardSetInfo(TTxId txId, TPathId pathId) {
        return TShardInfo(txId, pathId, ETabletType::TestShard);
    }
};

NProtoBuf::Timestamp SecondsToProtoTimeStamp(ui64 sec);

}
}
