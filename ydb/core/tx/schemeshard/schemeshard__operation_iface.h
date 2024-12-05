#pragma once

#include <ydb/library/login/login.h>
#include <ydb/core/util/token_bucket.h>
#include <ydb/core/tablet/tablet_counters.h>  // for TTabletCountersBase
#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/tx/schemeshard/olap/manager/manager.h>

#include "schemeshard_identificators.h"
#include "schemeshard_path_element.h"
#include "schemeshard_path.h"
#include "schemeshard_utils.h"

#include "schemeshard_info_types.h"
#include "schemeshard__operation.h"


namespace NKikimr::NSchemeShard {

extern const ui64 NEW_TABLE_ALTER_VERSION;

const NKikimrConfig::TDomainsConfig& GetDomainsConfig();

bool FillUniformPartitioning(TVector<TString>& rangeEnds, ui32 keySize, NScheme::TTypeInfo firstKeyColType,
                                    ui32 partitionCount, const NScheme::TTypeRegistry* typeRegistry, TString& errStr);
bool FillSplitPartitioning(TVector<TString>& rangeEnds, const TConstArrayRef<NScheme::TTypeInfo>& keyColTypes,
                                    const ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TSplitBoundary>& boundaries,
                                    TString& errStr);

struct TSchemeshardState : public IQuotaCounters {

    // Self knowledge
    ui64 SelfTabletId_ = 0;
    TActorId SelfActorId_ = TActorId();

    bool IsDomainSchemeShard = false;
    TPathId ParentDomainId = InvalidPathId;

    // Feature flags
    bool EnableBackgroundCompaction = false;
    bool EnableBackgroundCompactionServerless = false;
    bool EnableBorrowedSplitCompaction = false;
    bool EnableMoveIndex = true;
    bool EnableAlterDatabaseCreateHiveFirst = false;
    bool EnablePQConfigTransactionsAtSchemeShard = false;
    bool EnableStatistics = false;
    bool EnableTablePgTypes = false;
    bool EnableServerlessExclusiveDynamicNodes = false;
    bool EnableAddColumsWithDefaults = false;
    bool EnableReplaceIfExistsForExternalEntities = false;
    bool EnableTempTables = false;
    bool EnableTableDatetime64 = false;
    bool EnableResourcePoolsOnServerless = false;
    bool EnableVectorIndex = false;
    bool EnableExternalDataSourcesOnServerless = false;
    bool EnableParameterizedDecimal = false;

    // ICB controls
    TControlWrapper DisablePublicationsOfDropping;

    TSchemeshardState(ui64 selfTabletId)
        : SelfTabletId_(selfTabletId)
        , DisablePublicationsOfDropping(0, 0, 1)
    {
    }

    TTabletId SelfTabletId() const { return TTabletId(SelfTabletId_); }
    // ui64 SelfTabletId() const { return SelfTabletId; }

    TActorId SelfActorId() const { return SelfActorId_; }

    // Memory state
    //

    THashMap<TPathId, TPathElement::TPtr> PathsById;
    TLocalPathId NextLocalPathId = 0;

    THashMap<TPathId, TTableInfo::TPtr> Tables;
    THashMap<TPathId, TTableInfo::TPtr> TTLEnabledTables;

    THashMap<TPathId, TTableIndexInfo::TPtr> Indexes;
    THashMap<TPathId, TCdcStreamInfo::TPtr> CdcStreams;
    THashMap<TPathId, TSequenceInfo::TPtr> Sequences;
    THashMap<TPathId, TReplicationInfo::TPtr> Replications;
    THashMap<TPathId, TBlobDepotInfo::TPtr> BlobDepots;

    THashMap<TPathId, TTxId> TablesWithSnapshots;
    THashMap<TTxId, TSet<TPathId>> SnapshotTables;
    THashMap<TTxId, TStepId> SnapshotsStepIds;

    THashMap<TPathId, TTxId> LockedPaths;

    THashMap<TPathId, TTopicInfo::TPtr> Topics;
    THashMap<TPathId, TRtmrVolumeInfo::TPtr> RtmrVolumes;
    THashMap<TPathId, TSolomonVolumeInfo::TPtr> SolomonVolumes;
    THashMap<TPathId, TSubDomainInfo::TPtr> SubDomains;
    THashMap<TPathId, TBlockStoreVolumeInfo::TPtr> BlockStoreVolumes;
    THashMap<TPathId, TFileStoreInfo::TPtr> FileStoreInfos;
    THashMap<TPathId, TKesusInfo::TPtr> KesusInfos;
    THashMap<TPathId, TOlapStoreInfo::TPtr> OlapStores;
    THashMap<TPathId, TExternalTableInfo::TPtr> ExternalTables;
    THashMap<TPathId, TExternalDataSourceInfo::TPtr> ExternalDataSources;
    THashMap<TPathId, TViewInfo::TPtr> Views;
    THashMap<TPathId, TResourcePoolInfo::TPtr> ResourcePools;
    THashMap<TPathId, TBackupCollectionInfo::TPtr> BackupCollections;

    TTempDirsState TempDirsState;

    TTablesStorage ColumnTables;
    std::shared_ptr<NKikimr::NOlap::NBackground::TSessionsManager> BackgroundSessionsManager;

    // it is only because we need to manage undo of upgrade subdomain, finally remove it
    THashMap<TPathId, TVector<TTabletId>> RevertedMigrations;

    THashMap<TTxId, TOperation::TPtr> Operations;
    THashMap<TTxId, TPublicationInfo> Publications;
    THashMap<TOperationId, TTxState> TxInFlight;

    TTabletCountersBase* TabletCounters = nullptr;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;

    ui64 NextLocalShardIdx = 0;
    THashMap<TShardIdx, TShardInfo> ShardInfos;
    THashMap<TShardIdx, TAdoptedShard> AdoptedShards;
    THashMap<TTabletId, TShardIdx> TabletIdToShardIdx;

    TSet<TPathId> CleanDroppedPathsCandidates;
    TSet<TPathId> CleanDroppedSubDomainsCandidates;

    THashSet<TString> SystemBackupSIDs;

    TTokenBucket DropBlockStoreVolumeRateLimiter;

    TSplitSettings SplitSettings;

    //XXX: scheme operation shouldn't have access to login provider
    NLogin::TLoginProvider LoginProvider;

    NExternalSource::IExternalSourceFactory::TPtr ExternalSourceFactory;

    // Memory state check methods
    //

    bool CheckApplyIf(const NKikimrSchemeOp::TModifyScheme& scheme, TString& errStr);
    bool CheckLocks(const TPathId pathId, const TTxId lockTxId, TString& errStr) const;
    bool CheckLocks(const TPathId pathId, const NKikimrSchemeOp::TModifyScheme& scheme, TString& errStr) const;

    bool CanCreateSnapshot(const TPathId& tablePathId, TTxId txId, NKikimrScheme::EStatus& status, TString& errStr) const;

    // Core methods
    //

    TPathId AllocatePathId() {
       TPathId next = MakeLocalId(NextLocalPathId);
       ++NextLocalPathId;
       return next;
    }

    THolder<TEvDataShard::TEvProposeTransaction> MakeDataShardProposal(const TPathId& pathId, const TOperationId& opId,
        const TString& body, const TActorContext& ctx) const;

    TPathId RootPathId() const {
        return MakeLocalId(TPathElement::RootPathId);
    }
    //XXX: just an alias
    TPathId GetCurrentSubDomainPathId() const {
        return RootPathId();
    }

    //XXX: not used in any way
    bool IsRootPathId(const TPathId& pId) const {
        return pId == RootPathId();
    }

    bool IsServerlessDomain(TSubDomainInfo::TPtr domainInfo) const {
        const auto& resourcesDomainId = domainInfo->GetResourcesDomainId();
        return !IsDomainSchemeShard && resourcesDomainId && resourcesDomainId != ParentDomainId;
    }

    bool IsServerlessDomain(const TPath& domain) const {
        return IsServerlessDomain(domain.DomainInfo());
    }

    // bool IsServerlessDomainGlobal(TPathId domainPathId, TSubDomainInfo::TConstPtr domainInfo) const {
    //     const auto& resourcesDomainId = domainInfo->GetResourcesDomainId();
    //     return IsDomainSchemeShard && resourcesDomainId && resourcesDomainId != domainPathId;
    // }

    TPathId MakeLocalId(const TLocalPathId& localPathId) const {
        return TPathId(SelfTabletId_, localPathId);
    }
    bool IsLocalId(const TPathId& pathId) const {
        return pathId.OwnerId == SelfTabletId_;
    }
    TShardIdx MakeLocalId(const TLocalShardIdx& localShardIdx) const {
        return TShardIdx(SelfTabletId_, localShardIdx);
    }
    bool IsLocalId(const TShardIdx& shardIdx) const {
        return shardIdx.GetOwnerId() == SelfTabletId_;
    }

    TShardIdx GetShardIdx(TTabletId tabletId) const;
    TShardIdx MustGetShardIdx(TTabletId tabletId) const;

    TShardIdx NextShardIdx(const TShardIdx& shardIdx, ui64 inc) const;
    TShardIdx ReserveShardIdxs(ui64 count);
    template <typename T>
    TShardIdx RegisterShardInfo(T&& shardInfo) {
        return RegisterShardInfo(ReserveShardIdxs(1), std::forward<T>(shardInfo));
    }
    template <typename T>
    TShardIdx RegisterShardInfo(const TShardIdx& shardIdx, T&& shardInfo) {
        Y_ABORT_UNLESS(shardIdx.GetOwnerId() == SelfTabletId_);
        const auto localId = ui64(shardIdx.GetLocalId());
        Y_VERIFY_S(localId < NextLocalShardIdx, "shardIdx: " << shardIdx << " NextLocalShardIdx: " << NextLocalShardIdx);
        Y_VERIFY_S(!ShardInfos.contains(shardIdx), "shardIdx: " << shardIdx << " already registered");
        IncrementPathDbRefCount(shardInfo.PathId, "new shard created");
        ShardInfos.emplace(shardIdx, std::forward<T>(shardInfo));
        return shardIdx;
    }

    TTxState& CreateTx(TOperationId opId, TTxState::ETxType txType, TPathId targetPath, TPathId sourcePath = InvalidPathId);
    TTxState* FindTx(TOperationId opId);
    TTxState* FindTxSafe(TOperationId opId, const TTxState::ETxType& txType);

    TMessageSeqNo StartRound(TTxState& state);// For SS -> DS propose events

    void ClearDescribePathCaches(const TPathElement::TPtr node, bool force = false);

    TTabletId GetGlobalHive(const TActorContext& ctx) const;

    enum class EHiveSelection : uint8_t {
        ANY,
        IGNORE_TENANT,
    };

    TTabletId ResolveHive(TPathId pathId, const TActorContext& ctx, EHiveSelection selection) const;
    TTabletId ResolveHive(TPathId pathId, const TActorContext& ctx) const;
    TTabletId ResolveHive(TShardIdx shardIdx, const TActorContext& ctx) const;

    TPathId ResolvePathIdForDomain(TPathId pathId) const;
    TPathId ResolvePathIdForDomain(TPathElement::TPtr pathEl) const;
    TSubDomainInfo::TPtr ResolveDomainInfo(TPathId pathId) const;
    TSubDomainInfo::TPtr ResolveDomainInfo(TPathElement::TPtr pathEl) const;

    THashSet<TShardIdx> CollectAllShards(const THashSet<TPathId>& paths) const;

    void IncrementPathDbRefCount(const TPathId& pathId, const TStringBuf& debug = TStringBuf());
    void DecrementPathDbRefCount(const TPathId& pathId, const TStringBuf& debug = TStringBuf());

    static void FillSeqNo(NKikimrTxDataShard::TFlatSchemeTransaction &tx, TMessageSeqNo seqNo);
    static void FillSeqNo(NKikimrTxColumnShard::TSchemaTxBody &tx, TMessageSeqNo seqNo);

    const NKikimrSubDomains::TProcessingParams& SelectProcessingParams(TPathId id) const;
    const NKikimrSubDomains::TProcessingParams& SelectProcessingParams(TPathElement::TPtr pathEl) const;

    void DropNode(TPathElement::TPtr node, TStepId step, TTxId txId, NIceDb::TNiceDb& db, const TActorContext& ctx);
    void DropPaths(const THashSet<TPathId>& paths, TStepId step, TTxId txId, NIceDb::TNiceDb& db, const TActorContext& ctx);

    ui64 GetAliveChildren(TPathElement::TPtr pathEl, const std::optional<TPathElement::EPathType>& type = std::nullopt) const;

    bool GetBindingsRooms(const TPathId domainId,
                          const NKikimrSchemeOp::TPartitionConfig& partitionConfig,
                          TVector<TStorageRoom>& rooms,
                          THashMap<ui32, ui32>& familyRooms,
                          TChannelsBindings& binding,
                          TString& errStr);

    /**
     * For each existing partition generates possible changes to channels
     * cand per-shard partition config based on an updated partitionConfig
     * for a table in the given domain.
     */
    bool GetBindingsRoomsChanges(
            const TPathId domainId,
            const TVector<TTableShardInfo>& partitions,
            const NKikimrSchemeOp::TPartitionConfig& partitionConfig,
            TBindingsRoomsChanges& changes,
            TString& errStr);

    bool GetChannelsBindings(const TPathId domainId, const TTableInfo::TCPtr tableInfo, TChannelsBindings& binding, TString& errStr)   const;

    /**
     * Generates channels bindings for column shards based on the given storage config
     */
    bool GetOlapChannelsBindings(const TPathId domainId,
                                 const NKikimrSchemeOp::TColumnStorageConfig& channelsConfig,
                                 TChannelsBindings& channelsBindings,
                                 TString& errStr);

    //XXX: mostly for upgrade-subdomain
    NKikimrSchemeOp::TPathVersion GetPathVersion(const TPath& pathEl) const;
    THashSet<TTxId> GetRelatedTransactions(const THashSet<TPathId>& paths, const TActorContext &ctx);
    void MarkAsMigrated(TPathElement::TPtr node, const TActorContext& ctx);

    //XXX: operation-common
    TMaybe<NKikimrSchemeOp::TPartitionConfig> GetTablePartitionConfigWithAlterData(TPathId pathId) const;
    void OnShardRemoved(const TShardIdx& shardIdx);

    bool IsBackupTable(TPathId pathId) const;
    bool IsCompatibleChannelProfileLogic(const TPathId domainId, const TTableInfo::TCPtr tableInfo) const;
    bool IsStorageConfigLogic(const TTableInfo::TCPtr tableInfo) const;

    THashSet<TPathId> ListSubTree(TPathId subdomain_root, const TActorContext& ctx);
    void MarkAsDropping(TPathElement::TPtr node, TTxId txId, const TActorContext& ctx);
    void MarkAsDropping(const THashSet<TPathId>& paths, TTxId txId, const TActorContext& ctx);

    bool ResolveTabletChannels(ui32 profileId, const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolveRtmrChannels(const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolveSolomonChannels(ui32 profileId, const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolveSolomonChannels(const NKikimrSchemeOp::TKeyValueStorageConfig &config, const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolvePqChannels(ui32 profileId, const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolveChannelsByPoolKinds(
        const TVector<TStringBuf>& channelPoolKinds,
        const TPathId domainId,
        TChannelsBindings& channelsBinding) const;
    bool ResolveSubdomainsChannels(const TStoragePools& storagePools, TChannelsBindings& channelsBinding);

    static void SetNbsChannelsParams(
        const google::protobuf::RepeatedPtrField<NKikimrBlockStore::TChannelProfile>& ecps,
        TChannelsBindings& channelsBinding);
    static void SetNfsChannelsParams(
        const google::protobuf::RepeatedPtrField<NKikimrFileStore::TChannelProfile>& ecps,
        TChannelsBindings& channelsBinding);
    static void SetPqChannelsParams(
        const google::protobuf::RepeatedPtrField<NKikimrPQ::TChannelProfile>& ecps,
        TChannelsBindings& channelsBinding);

    void FillTableDescriptionForShardIdx(
        TPathId tableId, TShardIdx shardIdx, NKikimrSchemeOp::TTableDescription* tableDescr,
        TString rangeBegin, TString rangeEnd,
        bool rangeBeginInclusive, bool rangeEndInclusive,
        bool newTable = false);
    void FillTableDescription(TPathId tableId, ui32 partitionIdx, ui64 schemaVersion, NKikimrSchemeOp::TTableDescription* tableDescr);

    TString FillAlterTableTxBody(TPathId tableId, TShardIdx shardIdx, TMessageSeqNo seqNo) const;
    TString FillBackupTxBody(TPathId pathId, const NKikimrSchemeOp::TBackupTask& task, ui32 shardNum, TMessageSeqNo seqNo) const;

    void FillAsyncIndexInfo(const TPathId& tableId, NKikimrTxDataShard::TFlatSchemeTransaction& tx);

    void FillTableSchemaVersion(ui64 schemaVersion, NKikimrSchemeOp::TTableDescription *tableDescr) const;

    void SetPartitioning(TPathId pathId, const std::vector<TShardIdx>& partitioning);
    void SetPartitioning(TPathId pathId, TOlapStoreInfo::TPtr storeInfo);
    void SetPartitioning(TPathId pathId, TColumnTableInfo::TPtr tableInfo);
    void SetPartitioning(TPathId pathId, TTableInfo::TPtr tableInfo, TVector<TTableShardInfo>&& newPartitioning);

    // Split-merge
    void DeleteSplitOp(TOperationId txId, TTxState& txState);
    bool ShardIsUnderSplitMergeOp(const TShardIdx& idx) const;

    void DescribeTable(const TTableInfo& tableInfo, const NScheme::TTypeRegistry* typeRegistry,
                       bool fillConfig, NKikimrSchemeOp::TTableDescription* entry) const;
    void DescribeTableIndex(const TPathId& pathId, const TString& name, TTableIndexInfo::TPtr indexInfo,
        bool fillConfig, bool fillBoundaries, NKikimrSchemeOp::TIndexDescription& entry
    ) const;
    void DescribeCdcStream(const TPathId& pathId, const TString& name, TCdcStreamInfo::TPtr info, NKikimrSchemeOp::TCdcStreamDescription& desc);

    // Persistent state methods
    //

    // path
    void PersistPath(NIceDb::TNiceDb& db, const TPathId& pathId);
    void PersistRemovePath(NIceDb::TNiceDb& db, const TPathElement::TPtr path);
    void PersistLastTxId(NIceDb::TNiceDb& db, const TPathElement::TPtr path);
    void PersistPathDirAlterVersion(NIceDb::TNiceDb& db, const TPathElement::TPtr path);
    void PersistACL(NIceDb::TNiceDb& db, const TPathElement::TPtr path);
    void PersistOwner(NIceDb::TNiceDb& db, const TPathElement::TPtr path);
    void PersistCreateTxId(NIceDb::TNiceDb& db, const TPathId pathId, TTxId txId);
    void PersistCreateStep(NIceDb::TNiceDb& db, const TPathId pathId, TStepId step);
    void PersistDropStep(NIceDb::TNiceDb& db, const TPathId pathId, TStepId step, TOperationId opId);

    // user attrs
    void ApplyAndPersistUserAttrs(NIceDb::TNiceDb& db, const TPathId& pathId);
    void PersistUserAttributes(NIceDb::TNiceDb& db, TPathId pathId, TUserAttributes::TPtr oldAttrs, TUserAttributes::TPtr alterAttrs);
    void PersistAlterUserAttributes(NIceDb::TNiceDb& db, TPathId pathId);

    // table index
    void PersistTableIndex(NIceDb::TNiceDb& db, const TPathId& pathId);
    void PersistTableIndexAlterData(NIceDb::TNiceDb& db, const TPathId& pathId);

    // cdc stream
    void PersistCdcStream(NIceDb::TNiceDb& db, const TPathId& pathId);
    void PersistCdcStreamAlterData(NIceDb::TNiceDb& db, const TPathId& pathId);
    void PersistRemoveCdcStream(NIceDb::TNiceDb& db, const TPathId& tableId);

    static void PersistTxMinStep(NIceDb::TNiceDb& db, const TOperationId opId, TStepId minStep);
    void PersistRemoveTx(NIceDb::TNiceDb& db, const TOperationId opId, const TTxState& txState);
    void PersistTable(NIceDb::TNiceDb &db, const TPathId pathId);
    void PersistChannelsBinding(NIceDb::TNiceDb& db, const TShardIdx shardId, const TChannelsBindings& bindedChannels);
    void PersistTablePartitioning(NIceDb::TNiceDb &db, const TPathId pathId, const TTableInfo::TPtr tableInfo);
    void PersistTablePartitioningDeletion(NIceDb::TNiceDb& db, const TPathId tableId, const TTableInfo::TPtr tableInfo);
    void PersistTablePartitionCondErase(NIceDb::TNiceDb& db, const TPathId& pathId, ui64 id, const TTableInfo::TPtr tableInfo);
    void PersistTablePartitionStats(NIceDb::TNiceDb& db, const TPathId& tableId, ui64 partitionId, const TPartitionStats& stats);
    void PersistTablePartitionStats(NIceDb::TNiceDb& db, const TPathId& tableId, const TShardIdx& shardIdx, const TTableInfo::TPtr tableInfo);
    void PersistTablePartitionStats(NIceDb::TNiceDb& db, const TPathId& tableId, const TTableInfo::TPtr tableInfo);
    void PersistTableCreated(NIceDb::TNiceDb& db, const TPathId tableId);
    void PersistTableAlterVersion(NIceDb::TNiceDb &db, const TPathId pathId, const TTableInfo::TPtr tableInfo);
    void PersistTableFinishColumnBuilding(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo, ui64 colId);
    void PersistTableAltered(NIceDb::TNiceDb &db, const TPathId pathId, const TTableInfo::TPtr tableInfo);
    void PersistAddAlterTable(NIceDb::TNiceDb& db, TPathId pathId, const TTableInfo::TAlterDataPtr alter);
    void PersistPersQueueGroup(NIceDb::TNiceDb &db, TPathId pathId, const TTopicInfo::TPtr);
    void PersistPersQueueGroupStats(NIceDb::TNiceDb &db, const TPathId pathId, const TTopicStats& stats);
    void PersistRemovePersQueueGroup(NIceDb::TNiceDb &db, TPathId pathId);
    void PersistAddPersQueueGroupAlter(NIceDb::TNiceDb &db, TPathId pathId, const TTopicInfo::TPtr);
    void PersistRemovePersQueueGroupAlter(NIceDb::TNiceDb &db, TPathId pathId);
    void PersistPersQueue(NIceDb::TNiceDb &db, TPathId pathId, TShardIdx shardIdx, const TTopicTabletInfo::TTopicPartitionInfo& partitionInfo);
    void PersistRemovePersQueue(NIceDb::TNiceDb &db, TPathId pathId, ui32 pqId);
    void PersistRtmrVolume(NIceDb::TNiceDb &db, TPathId pathId, const TRtmrVolumeInfo::TPtr rtmrVol);
    void PersistRemoveRtmrVolume(NIceDb::TNiceDb &db, TPathId pathId);
    void PersistSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId, const TSolomonVolumeInfo::TPtr rtmrVol);
    void PersistRemoveSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId);
    void PersistAlterSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId, const TSolomonVolumeInfo::TPtr rtmrVol);
    static void PersistAddTxDependency(NIceDb::TNiceDb& db, const TTxId parentOpId, TTxId txId);
    static void PersistRemoveTxDependency(NIceDb::TNiceDb& db, TTxId opId, TTxId dependentOpId);
    void PersistUpdateTxShard(NIceDb::TNiceDb& db, TOperationId txId, TShardIdx shardIdx, ui32 operation);
    void PersistRemoveTxShard(NIceDb::TNiceDb& db, TOperationId txId, TShardIdx shardIdx);
    void PersistShardMapping(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTabletId tabletId, TPathId pathId, TTxId txId, TTabletTypes::EType type);
    void PersistAdoptedShardMapping(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTabletId tabletId, ui64 prevOwner, TLocalShardIdx prevShardIdx);
    void PersistShardPathId(NIceDb::TNiceDb& db, TShardIdx shardIdx, TPathId pathId);
    void PersistDeleteAdopted(NIceDb::TNiceDb& db, TShardIdx shardIdx);

    void PersistSnapshotTable(NIceDb::TNiceDb& db, const TTxId snapshotId, const TPathId tableId);
    void PersistSnapshotStepId(NIceDb::TNiceDb& db, const TTxId snapshotId, const TStepId stepId);
    void PersistDropSnapshot(NIceDb::TNiceDb& db, const TTxId snapshotId, const TPathId tableId);
    void PersistLongLock(NIceDb::TNiceDb& db, const TTxId lockId, const TPathId pathId);
    void PersistUnLock(NIceDb::TNiceDb& db, const TPathId pathId);

    void PersistTxState(NIceDb::TNiceDb& db, const TOperationId opId);
    void ChangeTxState(NIceDb::TNiceDb& db, const TOperationId opId, TTxState::ETxState newState);
    void PersistCancelTx(NIceDb::TNiceDb& db, const TOperationId opId, const TTxState& txState);
    void PersistTxPlanStep(NIceDb::TNiceDb& db, TOperationId opId, TStepId step);


    void PersistShardTx(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTxId txId);
    void PersistUpdateNextPathId(NIceDb::TNiceDb& db) const;
    void PersistUpdateNextShardIdx(NIceDb::TNiceDb& db) const;
    void PersistParentDomain(NIceDb::TNiceDb& db, TPathId parentDomain) const;
    void PersistParentDomainEffectiveACL(NIceDb::TNiceDb& db, const TString& owner, const TString& effectiveACL, ui64 effectiveACLVersion) const;
    void PersistShardsToDelete(NIceDb::TNiceDb& db, const THashSet<TShardIdx>& shardsIdxs);
    void PersistShardDeleted(NIceDb::TNiceDb& db, TShardIdx shardIdx, const TChannelsBindings& bindedChannels);
    void PersistUnknownShardDeleted(NIceDb::TNiceDb& db, TShardIdx shardIdx);
    void PersistTxShardStatus(NIceDb::TNiceDb& db, TOperationId opId, TShardIdx shardIdx, const TTxState::TShardStatus& status);
    void PersistBackupSettings(NIceDb::TNiceDb& db, TPathId pathId, const NKikimrSchemeOp::TBackupTask& settings);
    void PersistBackupDone(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistCompletedBackupRestore(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& info, TTableInfo::TBackupRestoreResult::EKind kind);
    void PersistCompletedBackup(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& backupInfo);
    void PersistCompletedRestore(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& restoreInfo);
    void PersistSchemeLimit(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistStoragePools(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomain(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistRemoveSubDomain(NIceDb::TNiceDb& db, const TPathId& pathId);
    void PersistSubDomainVersion(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainDeclaredSchemeQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainDatabaseQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainState(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainSchemeQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainSecurityStateVersion(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainPrivateShards(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistDeleteSubDomainAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainAuditSettings(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainAuditSettingsAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainServerlessComputeResourcesMode(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistSubDomainServerlessComputeResourcesModeAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain);
    void PersistKesusInfo(NIceDb::TNiceDb& db, TPathId pathId, const TKesusInfo::TPtr);
    void PersistKesusVersion(NIceDb::TNiceDb& db, TPathId pathId, const TKesusInfo::TPtr);
    void PersistAddKesusAlter(NIceDb::TNiceDb& db, TPathId pathId, const TKesusInfo::TPtr);
    void PersistRemoveKesusAlter(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistRemoveKesusInfo(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistRemoveTableIndex(NIceDb::TNiceDb& db, TPathId tableId);
    void PersistRemoveTable(NIceDb::TNiceDb& db, TPathId tableId, const TActorContext& ctx);
    void PersistRevertedMigration(NIceDb::TNiceDb& db, TPathId pathId, TTabletId abandonedSchemeShardId);

    // BlockStore
    void PersistBlockStorePartition(NIceDb::TNiceDb& db, TPathId pathId, ui32 partitionId, TShardIdx shardIdx, ui64 version);
    void PersistBlockStoreVolume(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr);
    void PersistBlockStoreVolumeMountToken(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr volume);
    void PersistAddBlockStoreVolumeAlter(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr);
    void PersistRemoveBlockStoreVolumeAlter(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistRemoveBlockStorePartition(NIceDb::TNiceDb& db, TPathId pathId, ui32 partitionId);
    void PersistRemoveBlockStoreVolume(NIceDb::TNiceDb& db, TPathId pathId);

    // FileStore
    void PersistFileStoreInfo(NIceDb::TNiceDb& db, TPathId pathId, const TFileStoreInfo::TPtr);
    void PersistAddFileStoreAlter(NIceDb::TNiceDb& db, TPathId pathId, const TFileStoreInfo::TPtr);
    void PersistRemoveFileStoreAlter(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistRemoveFileStoreInfo(NIceDb::TNiceDb& db, TPathId pathId);

    // OlapStore
    void PersistOlapStore(NIceDb::TNiceDb& db, TPathId pathId, const TOlapStoreInfo& storeInfo, bool isAlter = false);
    void PersistOlapStoreRemove(NIceDb::TNiceDb& db, TPathId pathId, bool isAlter = false);
    void PersistOlapStoreAlter(NIceDb::TNiceDb& db, TPathId pathId, const TOlapStoreInfo& storeInfo);
    void PersistOlapStoreAlterRemove(NIceDb::TNiceDb& db, TPathId pathId);

    // ColumnTable
    void PersistColumnTable(NIceDb::TNiceDb& db, TPathId pathId, const TColumnTableInfo& tableInfo, bool isAlter = false);
    void PersistColumnTableRemove(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistColumnTableAlter(NIceDb::TNiceDb& db, TPathId pathId, const TColumnTableInfo& tableInfo);
    void PersistColumnTableAlterRemove(NIceDb::TNiceDb& db, TPathId pathId);

    // Sequence
    void PersistSequence(NIceDb::TNiceDb& db, TPathId pathId, const TSequenceInfo& sequenceInfo);
    void PersistSequence(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistSequenceRemove(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistSequenceAlter(NIceDb::TNiceDb& db, TPathId pathId, const TSequenceInfo& sequenceInfo);
    void PersistSequenceAlter(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistSequenceAlterRemove(NIceDb::TNiceDb& db, TPathId pathId);

    // Replication
    void PersistReplication(NIceDb::TNiceDb& db, TPathId pathId, const TReplicationInfo& replicationInfo);
    void PersistReplicationRemove(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistReplicationAlter(NIceDb::TNiceDb& db, TPathId pathId, const TReplicationInfo& replicationInfo);
    void PersistReplicationAlterRemove(NIceDb::TNiceDb& db, TPathId pathId);

    // BlobDepot
    void PersistBlobDepot(NIceDb::TNiceDb& db, TPathId pathId, const TBlobDepotInfo& blobDepotInfo);

    void PersistAddTableShardPartitionConfig(NIceDb::TNiceDb& db, TShardIdx shardIdx, const NKikimrSchemeOp::TPartitionConfig& config);

    // ExternalTable
    void PersistExternalTable(NIceDb::TNiceDb &db, TPathId pathId, const TExternalTableInfo::TPtr externalTable);
    void PersistRemoveExternalTable(NIceDb::TNiceDb& db, TPathId pathId);

    // ExternalDataSource
    void PersistExternalDataSource(NIceDb::TNiceDb &db, TPathId pathId, const TExternalDataSourceInfo::TPtr externalDataSource);
    void PersistRemoveExternalDataSource(NIceDb::TNiceDb& db, TPathId pathId);

    // View
    void PersistView(NIceDb::TNiceDb &db, TPathId pathId);
    void PersistRemoveView(NIceDb::TNiceDb& db, TPathId pathId);

    // ResourcePool
    void PersistResourcePool(NIceDb::TNiceDb& db, TPathId pathId, const TResourcePoolInfo::TPtr resourcePool);
    void PersistRemoveResourcePool(NIceDb::TNiceDb& db, TPathId pathId);

    // BackupCollection
    void PersistBackupCollection(NIceDb::TNiceDb& db, TPathId pathId, const TBackupCollectionInfo::TPtr backupCollection);
    void PersistRemoveBackupCollection(NIceDb::TNiceDb& db, TPathId pathId);

};

}  // namespace NKikimr::NSchemeShard
