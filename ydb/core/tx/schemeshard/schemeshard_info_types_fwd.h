#pragma once

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NSchemeShard {

enum ESimpleCounters : int;

struct TBindingsRoomsChange;
using TChannelsMapping = TVector<TString>;
using TBindingsRoomsChanges = TMap<TChannelsMapping, TBindingsRoomsChange>;

enum class EUserFacingStorageType {
    Ssd,
    Hdd,
    Ignored
};

struct IQuotaCounters {
    virtual void ChangeStreamShardsCount(i64 delta) = 0;
    virtual void ChangeStreamShardsQuota(i64 delta) = 0;
    virtual void ChangeStreamReservedStorageQuota(i64 delta) = 0;
    virtual void ChangeStreamReservedStorageCount(i64 delta) = 0;
    virtual void ChangeDiskSpaceTablesDataBytes(i64 delta) = 0;
    virtual void ChangeDiskSpaceTablesIndexBytes(i64 delta) = 0;
    virtual void ChangeDiskSpaceTablesTotalBytes(i64 delta) = 0;
    virtual void AddDiskSpaceTables(EUserFacingStorageType storageType, ui64 data, ui64 index) = 0;
    virtual void ChangeDiskSpaceTopicsTotalBytes(ui64 value) = 0;
    virtual void ChangeDiskSpaceHardQuotaBytes(i64 delta) = 0;
    virtual void ChangeDiskSpaceSoftQuotaBytes(i64 delta) = 0;
    virtual void AddDiskSpaceSoftQuotaBytes(EUserFacingStorageType storageType, ui64 addend) = 0;
    virtual void ChangeSmallBlobsVolumeBytes(i64 delta) = 0;
    virtual void ChangeSmallBlobsCount(i64 delta) = 0;
    virtual void ChangeSmallBlobsVolumeHardQuotaBytes(i64 delta) = 0;
    virtual void ChangeSmallBlobsVolumeSoftQuotaBytes(i64 delta) = 0;
    virtual void ChangeSmallBlobsCountHardQuota(i64 delta) = 0;
    virtual void ChangeSmallBlobsCountSoftQuota(i64 delta) = 0;
    virtual void ChangeSimpleCounter(ESimpleCounters counter, i64 delta) = 0;
    virtual void ChangePathCount(i64 delta) = 0;
    virtual void SetPathCount(ui64 value) = 0;
    virtual void SetPathsQuota(ui64 value) = 0;
    virtual void ChangeShardCount(i64 delta) = 0;
    virtual void SetShardCount(ui64 value) = 0;
    virtual void SetShardsQuota(ui64 value) = 0;
};

enum class ETableBackupRestoreKind : ui8 {
    Backup = 0,
    Restore,
};

enum class EIncrementalRestoreState : ui32 {
    Running = 1,
    Finalizing = 2,
    Completed = 3,
    Failed = 4,
};

enum class EIncrementalRestoreItemKind : ui32 {
    Table = 0,
    Index = 1,
    Finalize = 2,
};

struct TSplitSettings;
struct TBackupSettings;
struct TIncrementalRestoreSettings;
struct TTableShardInfo;
struct TPartitionStats;
struct TSubDomainInfo;
struct TTableColumn;
struct TTableInfo;
struct TTableBackupRestoreResult;
struct TTableAlterInfo;
struct TTopicStats;
struct TTopicTabletInfo;
struct TTopicKeyRange;
struct TTopicPartitionInfo;
struct TAdoptedShard;
struct TShardInfo;
struct TTopicInfo;
struct TRtmrVolumeInfo;
struct TSolomonVolumeInfo;
struct TBlockStoreVolumeInfo;
struct TFileStoreInfo;
struct TKesusInfo;
struct TTableIndexInfo;
struct TCdcStreamInfo;
struct TCdcStreamShardStatus;
struct TSequenceInfo;
struct TReplicationInfo;
struct TBlobDepotInfo;
struct TPublicationInfo;
struct TExportInfo;
struct TImportInfo;
struct TExternalTableInfo;
struct TExternalDataSourceInfo;
struct TViewInfo;
struct TResourcePoolInfo;
struct TBackupCollectionInfo;
struct TSysViewInfo;
struct TIncrementalRestoreState;
struct TIncrementalBackupInfo;
struct TIncrementalBackupItem;
struct TFullBackupInfo;
struct TFullBackupItem;
struct TSecretInfo;
struct TStreamingQueryInfo;
struct TTestShardSetInfo;
struct TForcedCompactionInfo;

} // namespace NKikimr::NSchemeShard
