#pragma once

#include "dedicated_pipe_pool.h"
#include "operation_queue_timer.h"
#include "schemeshard.h"
#include "schemeshard_export.h"
#include "schemeshard_import.h"
#include "schemeshard_backup.h"
#include "schemeshard_build_index.h"
#include "schemeshard_private.h"
#include "schemeshard_types.h"
#include "schemeshard_path_element.h"
#include "schemeshard_path.h"
#include "schemeshard_domain_links.h"
#include "schemeshard_info_types.h"
#include "schemeshard_tx_infly.h"
#include "schemeshard_utils.h"
#include "schemeshard_schema.h"
#include "schemeshard__operation.h"
#include "schemeshard__stats.h"

#include "olap/manager/manager.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/filestore_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tablet/pipe_tracker.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/local.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/util/pb.h>
#include <ydb/core/util/token_bucket.h>
#include <ydb/core/ydb_convert/table_profiles.h>

#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/filestore/core/filestore.h>

#include <ydb/library/login/login.h>

#include <util/generic/ptr.h>

namespace NKikimr::NSchemeShard::NBackground {
struct TEvListRequest;
}

namespace NKikimr {
namespace NSchemeShard {

extern const ui64 NEW_TABLE_ALTER_VERSION;

class TSchemeShard
    : public TActor<TSchemeShard>
    , public NTabletFlatExecutor::TTabletExecutedFlat
    , public IQuotaCounters
{
private:
    class TPipeClientFactory : public NTabletPipe::IClientFactory {
    public:
        TPipeClientFactory(TSchemeShard* self)
            : Self(self)
        { }

        TActorId CreateClient(const TActorContext& ctx, ui64 tabletId, const NTabletPipe::TClientConfig& pipeConfig) override;

    private:
        TSchemeShard* Self;
    };

    using TCompactionQueue = NOperationQueue::TOperationQueueWithTimer<
        TShardCompactionInfo,
        TCompactionQueueImpl,
        TEvPrivate::EvRunBackgroundCompaction,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BACKGROUND_COMPACTION>;

    class TCompactionStarter : public TCompactionQueue::IStarter {
    public:
        TCompactionStarter(TSchemeShard* self)
            : Self(self)
        { }

        NOperationQueue::EStartStatus StartOperation(const TShardCompactionInfo& info) override {
            return Self->StartBackgroundCompaction(info);
        }

        void OnTimeout(const TShardCompactionInfo& info) override {
            Self->OnBackgroundCompactionTimeout(info);
        }

    private:
        TSchemeShard* Self;
    };

    using TBorrowedCompactionQueue = NOperationQueue::TOperationQueueWithTimer<
        TShardIdx,
        TFifoQueue<TShardIdx>,
        TEvPrivate::EvRunBorrowedCompaction,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BORROWED_COMPACTION>;

    class TBorrowedCompactionStarter : public TBorrowedCompactionQueue::IStarter {
    public:
        TBorrowedCompactionStarter(TSchemeShard* self)
            : Self(self)
        { }

        NOperationQueue::EStartStatus StartOperation(const TShardIdx& shardIdx) override {
            return Self->StartBorrowedCompaction(shardIdx);
        }

        void OnTimeout(const TShardIdx& shardIdx) override {
            Self->OnBorrowedCompactionTimeout(shardIdx);
        }

    private:
        TSchemeShard* Self;
    };

    using TBackgroundCleaningQueue = NOperationQueue::TOperationQueueWithTimer<
        TPathId,
        TFifoQueue<TPathId>,
        TEvPrivate::EvRunBackgroundCleaning,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BACKGROUND_CLEANING>;

    class TBackgroundCleaningStarter : public TBackgroundCleaningQueue::IStarter {
    public:
        TBackgroundCleaningStarter(TSchemeShard* self)
            : Self(self)
        { }

        NOperationQueue::EStartStatus StartOperation(const TPathId& pathId) override {
            return Self->StartBackgroundCleaning(pathId);
        }

        void OnTimeout(const TPathId& pathId) override {
            Self->OnBackgroundCleaningTimeout(pathId);
        }

    private:
        TSchemeShard* Self;
    };

public:
    static constexpr ui32 DefaultPQTabletPartitionsCount = 1;
    static constexpr ui32 MaxPQTabletPartitionsCount = 1000;
    static constexpr ui32 MaxPQGroupTabletsCount = 10*1000;
    static constexpr ui32 MaxPQGroupPartitionsCount = 20*1000;
    static constexpr ui32 MaxPQWriteSpeedPerPartition = 50*1024*1024;
    static constexpr ui32 MaxPQLifetimeSeconds = 31 * 86400;
    static constexpr ui32 PublishChunkSize = 1000;

    static const TSchemeLimits DefaultLimits;

    TIntrusivePtr<TChannelProfiles> ChannelProfiles;

    TTableProfiles TableProfiles;
    bool TableProfilesLoaded = false;
    THashSet<std::pair<ui64, ui32>> TableProfilesWaiters;

    TControlWrapper AllowConditionalEraseOperations;
    TControlWrapper AllowServerlessStorageBilling;
    TControlWrapper DisablePublicationsOfDropping;
    TControlWrapper FillAllocatePQ;

    // Shared with NTabletFlatExecutor::TExecutor
    TControlWrapper MaxCommitRedoMB;

    TSplitSettings SplitSettings;

    struct TTenantInitState {
        enum EInitState {
            InvalidState = 0,
            Uninitialized = 1,
            Inprogress = 2,
            ReadOnlyPreview = 50,
            Done = 100,
        };
    };
    TTenantInitState::EInitState InitState = TTenantInitState::InvalidState;

    // In RO mode we don't accept any modifications from users but process all in-flight operations in normal way
    bool IsReadOnlyMode = false;

    bool IsDomainSchemeShard = false;

    TPathId ParentDomainId = InvalidPathId;
    TString ParentDomainEffectiveACL;
    ui64 ParentDomainEffectiveACLVersion = 0;
    TEffectiveACL ParentDomainCachedEffectiveACL;
    TString ParentDomainOwner;

    THashSet<TString> SystemBackupSIDs;
    TInstant ServerlessStorageLastBillTime;

    TParentDomainLink ParentDomainLink;
    TSubDomainsLinks SubDomainsLinks;

    TVector<TString> RootPathElements;

    ui64 MaxIncompatibleChange = 0;
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

    TTempDirsState TempDirsState;

    TTablesStorage ColumnTables;
    std::shared_ptr<NKikimr::NOlap::NBackground::TSessionsManager> BackgroundSessionsManager;

    // it is only because we need to manage undo of upgrade subdomain, finally remove it
    THashMap<TPathId, TVector<TTabletId>> RevertedMigrations;

    THashMap<TTxId, TOperation::TPtr> Operations;
    THashMap<TTxId, TPublicationInfo> Publications;
    THashMap<TOperationId, TTxState> TxInFlight;

    ui64 NextLocalShardIdx = 0;
    THashMap<TShardIdx, TShardInfo> ShardInfos;
    THashMap<TShardIdx, TAdoptedShard> AdoptedShards;
    THashMap<TTabletId, TShardIdx> TabletIdToShardIdx;
    THashMap<TShardIdx, TVector<TActorId>> ShardDeletionSubscribers; // for tests

    // in case of integral hists we need to remember what values we have set
    struct TPartitionMetrics {
        ui64 SearchHeight = 0;
        ui64 RowDeletes = 0;
        ui32 HoursSinceFullCompaction = 0;
    };
    THashMap<TShardIdx, TPartitionMetrics> PartitionMetricsMap;

    TActorId SchemeBoardPopulator;

    static constexpr ui32 InitiateCachedTxIdsCount = 100;
    TDeque<TTxId> CachedTxIds;
    TActorId TxAllocatorClient;

    TAutoPtr<NTabletPipe::IClientCache> PipeClientCache;
    TPipeTracker PipeTracker;

    TCompactionStarter CompactionStarter;
    TCompactionQueue* CompactionQueue = nullptr;

    TBorrowedCompactionStarter BorrowedCompactionStarter;
    TBorrowedCompactionQueue* BorrowedCompactionQueue = nullptr;

    TBackgroundCleaningStarter BackgroundCleaningStarter;
    TBackgroundCleaningQueue* BackgroundCleaningQueue = nullptr;

    struct TBackgroundCleaningState {
        THashSet<TTxId> TxIds;
        TVector<NKikimr::TPathId> DirsToRemove;

        size_t ObjectsToDrop = 0;
        size_t ObjectsDropped = 0;

        bool NeedToRetryLater = false;
    };
    THashMap<TPathId, TBackgroundCleaningState> BackgroundCleaningState;
    THashMap<TTxId, TPathId> BackgroundCleaningTxToDirPathId;
    NKikimrConfig::TBackgroundCleaningConfig::TRetrySettings BackgroundCleaningRetrySettings;

    // shardIdx -> clientId
    THashMap<TShardIdx, TActorId> RunningBorrowedCompactions;

    THashSet<TShardIdx> ShardsWithBorrowed; // shards have parts from another shards
    THashSet<TShardIdx> ShardsWithLoaned;   // shards have parts loaned to another shards
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

    TShardDeleter ShardDeleter;

    // Counter-strike stuff
    TTabletCountersBase* TabletCounters = nullptr;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;

    TAutoPtr<TSelfPinger> SelfPinger;

    TActorId SysPartitionStatsCollector;

    TActorId TabletMigrator;

    TActorId CdcStreamScanFinalizer;
    ui32 MaxCdcInitialScanShardsInFlight = 10;

    TDuration StatsMaxExecuteTime;
    TDuration StatsBatchTimeout;
    ui32 StatsMaxBatchSize = 0;
    THashMap<TTxState::ETxType, ui32> InFlightLimits;

    // time when we opened the batch
    bool TableStatsBatchScheduled = false;
    bool TablePersistStatsPending = false;
    TStatsQueue<TEvDataShard::TEvPeriodicTableStats> TableStatsQueue;

    bool TopicStatsBatchScheduled = false;
    bool TopicPersistStatsPending = false;
    TStatsQueue<TEvPersQueue::TEvPeriodicTopicStats> TopicStatsQueue;

    TSet<TPathId> CleanDroppedPathsCandidates;
    TSet<TPathId> CleanDroppedSubDomainsCandidates;
    bool CleanDroppedPathsInFly = false;
    bool CleanDroppedPathsDisabled = true;
    bool CleanDroppedSubDomainsInFly = false;

    TTokenBucket DropBlockStoreVolumeRateLimiter;

    TActorId DelayedInitTenantDestination;
    TAutoPtr<TEvSchemeShard::TEvInitTenantSchemeShardResult> DelayedInitTenantReply;

    NExternalSource::IExternalSourceFactory::TPtr ExternalSourceFactory{NExternalSource::CreateExternalSourceFactory({})};

    THolder<TProposeResponse> IgniteOperation(TProposeRequest& request, TOperationContext& context);
    void AbortOperationPropose(const TTxId txId, TOperationContext& context);

    THolder<TEvDataShard::TEvProposeTransaction> MakeDataShardProposal(const TPathId& pathId, const TOperationId& opId,
        const TString& body, const TActorContext& ctx) const;

    TPathId RootPathId() const {
        return MakeLocalId(TPathElement::RootPathId);
    }

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

    bool IsServerlessDomainGlobal(TPathId domainPathId, TSubDomainInfo::TConstPtr domainInfo) const {
        const auto& resourcesDomainId = domainInfo->GetResourcesDomainId();
        return IsDomainSchemeShard && resourcesDomainId && resourcesDomainId != domainPathId;
    }

    TPathId MakeLocalId(const TLocalPathId& localPathId) const {
        return TPathId(TabletID(), localPathId);
    }

    TShardIdx MakeLocalId(const TLocalShardIdx& localShardIdx) const {
        return TShardIdx(TabletID(), localShardIdx);
    }

    bool IsLocalId(const TPathId& pathId) const {
        return pathId.OwnerId == TabletID();
    }

    bool IsLocalId(const TShardIdx& shardIdx) const {
        return shardIdx.GetOwnerId() == TabletID();
    }

    TPathId GetCurrentSubDomainPathId() const {
        return RootPathId();
    }

    TPathId PeekNextPathId() const {
        return MakeLocalId(NextLocalPathId);
    }

    TPathId AllocatePathId() {
       TPathId next = PeekNextPathId();
       ++NextLocalPathId;
       return next;
    }

    TTxId GetCachedTxId(const TActorContext& ctx);

    EAttachChildResult AttachChild(TPathElement::TPtr child);
    bool PathIsActive(TPathId pathId) const;

    // Transient sequence number that monotonically increases within SS tablet generation. It is included in events
    // sent from SS to DS and is used for deduplication.
    ui64 SchemeOpRound = 1;
    TMessageSeqNo StartRound(TTxState& state);// For SS -> DS propose events
    TMessageSeqNo NextRound();

    void Clear();
    void BreakTabletAndRestart(const TActorContext& ctx);

    bool IsSchemeShardConfigured() const;

    void InitializeTabletMigrations();

    ui64 Generation() const;

    void SubscribeConsoleConfigs(const TActorContext& ctx);
    void ApplyConsoleConfigs(const NKikimrConfig::TAppConfig& appConfig, const TActorContext& ctx);
    void ApplyConsoleConfigs(const NKikimrConfig::TFeatureFlags& featureFlags, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvConsoleConfigsTimeout::TPtr& ev, const TActorContext& ctx);

    void ConfigureStatsBatching(
        const NKikimrConfig::TSchemeShardConfig& config,
        const TActorContext &ctx);

    void ConfigureStatsOperations(
        const NKikimrConfig::TSchemeShardConfig& config,
        const TActorContext &ctx);

    void ConfigureCompactionQueues(
        const NKikimrConfig::TCompactionConfig& config,
        const TActorContext &ctx);

    void ConfigureBackgroundCompactionQueue(
        const NKikimrConfig::TCompactionConfig::TBackgroundCompactionConfig& config,
        const TActorContext &ctx);

    void ConfigureBorrowedCompactionQueue(
        const NKikimrConfig::TCompactionConfig::TBorrowedCompactionConfig& config,
        const TActorContext &ctx);

    void ConfigureBackgroundCleaningQueue(
        const NKikimrConfig::TBackgroundCleaningConfig& config,
        const TActorContext &ctx);

    void StartStopCompactionQueues();

    void WaitForTableProfiles(ui64 importId, ui32 itemIdx);
    void LoadTableProfiles(const NKikimrConfig::TTableProfilesConfig* config, const TActorContext& ctx);

    bool ApplyStorageConfig(const TStoragePools& storagePools,
                            const NKikimrSchemeOp::TStorageConfig& storageConfig,
                            TChannelsBindings& channelsBinding,
                            THashMap<TString, ui32>& reverseBinding,
                            TStorageRoom& room,
                            TString& errorMsg);
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

    /**
     * Generates channels bindings for column shards based on the given storage config
     */
    bool GetOlapChannelsBindings(const TPathId domainId,
                                 const NKikimrSchemeOp::TColumnStorageConfig& channelsConfig,
                                 TChannelsBindings& channelsBindings,
                                 TString& errStr);

    bool IsStorageConfigLogic(const TTableInfo::TCPtr tableInfo) const;
    bool IsCompatibleChannelProfileLogic(const TPathId domainId, const TTableInfo::TCPtr tableInfo) const;
    bool GetChannelsBindings(const TPathId domainId, const TTableInfo::TCPtr tableInfo, TChannelsBindings& binding, TString& errStr)   const;

    bool ResolveTabletChannels(ui32 profileId, const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolveRtmrChannels(const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolveSolomonChannels(ui32 profileId, const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolveSolomonChannels(const NKikimrSchemeOp::TKeyValueStorageConfig &config, const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolvePqChannels(ui32 profileId, const TPathId domainId, TChannelsBindings& channelsBinding) const;
    bool ResolveChannelsByPoolKinds(
        const TVector<TStringBuf>& channelPoolKinds,
        const TPathId domainId,
        TChannelsBindings& channelsBinding) const;
    static void SetNbsChannelsParams(
        const google::protobuf::RepeatedPtrField<NKikimrBlockStore::TChannelProfile>& ecps,
        TChannelsBindings& channelsBinding);
    static void SetNfsChannelsParams(
        const google::protobuf::RepeatedPtrField<NKikimrFileStore::TChannelProfile>& ecps,
        TChannelsBindings& channelsBinding);
    static void SetPqChannelsParams(
        const google::protobuf::RepeatedPtrField<NKikimrPQ::TChannelProfile>& ecps,
        TChannelsBindings& channelsBinding);

    bool ResolveSubdomainsChannels(const TStoragePools& storagePools, TChannelsBindings& channelsBinding);

    using TChannelResolveDetails = std::function<bool (ui32 profileId,
                                                      const TChannelProfiles::TProfile& profile,
                                                      const TStoragePools& storagePools,
                                                      TChannelsBindings& channelsBinding)>;
    bool ResolveChannelCommon(ui32 profileId, const TPathId domainId, TChannelsBindings& channelsBinding, TChannelResolveDetails resolveDetails) const;
    static bool ResolveChannelsDetailsAsIs(ui32 /*profileId*/, const TChannelProfiles::TProfile& profile, const TStoragePools& storagePools, TChannelsBindings& channelsBinding);
    static bool TabletResolveChannelsDetails(ui32 profileId, const TChannelProfiles::TProfile& profile, const TStoragePools& storagePools, TChannelsBindings& channelsBinding);

    void ClearDescribePathCaches(const TPathElement::TPtr node, bool force = false);
    TString PathToString(TPathElement::TPtr item);
    NKikimrSchemeOp::TPathVersion  GetPathVersion(const TPath& pathEl) const;
    ui64 GetAliveChildren(TPathElement::TPtr pathEl, const std::optional<TPathElement::EPathType>& type = std::nullopt) const;

    const TTableInfo* GetMainTableForIndex(TPathId indexTableId) const;
    bool IsBackupTable(TPathId pathId) const;

    TPathId ResolvePathIdForDomain(TPathId pathId) const;
    TPathId ResolvePathIdForDomain(TPathElement::TPtr pathEl) const;
    TSubDomainInfo::TPtr ResolveDomainInfo(TPathId pathId) const;
    TSubDomainInfo::TPtr ResolveDomainInfo(TPathElement::TPtr pathEl) const;

    TPathId GetDomainKey(TPathElement::TPtr pathEl) const;
    TPathId GetDomainKey(TPathId pathId) const;

    const NKikimrSubDomains::TProcessingParams& SelectProcessingParams(TPathId id) const;
    const NKikimrSubDomains::TProcessingParams& SelectProcessingParams(TPathElement::TPtr pathEl) const;

    TTabletId SelectCoordinator(TTxId txId, TPathId pathId) const;
    TTabletId SelectCoordinator(TTxId txId, TPathElement::TPtr pathEl) const;

    bool CheckApplyIf(const NKikimrSchemeOp::TModifyScheme& scheme, TString& errStr);
    bool CheckLocks(const TPathId pathId, const TTxId lockTxId, TString& errStr) const;
    bool CheckLocks(const TPathId pathId, const NKikimrSchemeOp::TModifyScheme& scheme, TString& errStr) const;
    bool CheckInFlightLimit(TTxState::ETxType txType, TString& errStr) const;
    bool CheckInFlightLimit(NKikimrSchemeOp::EOperationType opType, TString& errStr) const;
    bool CanCreateSnapshot(const TPathId& tablePathId, TTxId txId, NKikimrScheme::EStatus& status, TString& errStr) const;

    TShardIdx ReserveShardIdxs(ui64 count);
    TShardIdx NextShardIdx(const TShardIdx& shardIdx, ui64 inc) const;
    template <typename T>
    TShardIdx RegisterShardInfo(T&& shardInfo) {
        return RegisterShardInfo(ReserveShardIdxs(1), std::forward<T>(shardInfo));
    }

    template <typename T>
    TShardIdx RegisterShardInfo(const TShardIdx& shardIdx, T&& shardInfo) {
        Y_ABORT_UNLESS(shardIdx.GetOwnerId() == TabletID());
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
    void RemoveTx(const TActorContext &ctx, NIceDb::TNiceDb& db, TOperationId opId, TTxState* txState);
    static TPathElement::EPathState CalcPathState(TTxState::ETxType txType, TPathElement::EPathState oldState);

    TMaybe<NKikimrSchemeOp::TPartitionConfig> GetTablePartitionConfigWithAlterData(TPathId pathId) const;
    void DeleteSplitOp(TOperationId txId, TTxState& txState);
    bool ShardIsUnderSplitMergeOp(const TShardIdx& idx) const;

    THashSet<TShardIdx> CollectAllShards(const THashSet<TPathId>& paths) const;
    void ExamineTreeVFS(TPathId nodeId, std::function<void(TPathElement::TPtr)> func, const TActorContext& ctx);
    THashSet<TPathId> ListSubTree(TPathId subdomain_root, const TActorContext& ctx);
    THashSet<TTxId> GetRelatedTransactions(const THashSet<TPathId>& paths, const TActorContext &ctx);

    void MarkAsDropping(TPathElement::TPtr node, TTxId txId, const TActorContext& ctx);
    void MarkAsDropping(const THashSet<TPathId>& paths, TTxId txId, const TActorContext& ctx);

    void UncountNode(TPathElement::TPtr node);
    void MarkAsMigrated(TPathElement::TPtr node, const TActorContext& ctx);

    void DropNode(TPathElement::TPtr node, TStepId step, TTxId txId, NIceDb::TNiceDb& db, const TActorContext& ctx);
    void DropPaths(const THashSet<TPathId>& paths, TStepId step, TTxId txId, NIceDb::TNiceDb& db, const TActorContext& ctx);

    void DoShardsDeletion(const THashSet<TShardIdx>& shardIdx, const TActorContext& ctx);

    void SetPartitioning(TPathId pathId, const std::vector<TShardIdx>& partitioning);
    void SetPartitioning(TPathId pathId, TOlapStoreInfo::TPtr storeInfo);
    void SetPartitioning(TPathId pathId, TColumnTableInfo::TPtr tableInfo);
    void SetPartitioning(TPathId pathId, TTableInfo::TPtr tableInfo, TVector<TTableShardInfo>&& newPartitioning);
    auto BuildStatsForCollector(TPathId tableId, TShardIdx shardIdx, TTabletId datashardId,
        TMaybe<ui32> nodeId, TMaybe<ui64> startTime, const TPartitionStats& stats);

    bool ReadSysValue(NIceDb::TNiceDb& db, ui64 sysTag, TString& value, TString defValue = TString());
    bool ReadSysValue(NIceDb::TNiceDb& db, ui64 sysTag, ui64& value, ui64 defVal = 0);

    void IncrementPathDbRefCount(const TPathId& pathId, const TStringBuf& debug = TStringBuf());
    void DecrementPathDbRefCount(const TPathId& pathId, const TStringBuf& debug = TStringBuf());

    // incompatible changes
    void BumpIncompatibleChanges(NIceDb::TNiceDb& db, ui64 incompatibleChange);

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
    void PersistSequenceRemove(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistSequenceAlter(NIceDb::TNiceDb& db, TPathId pathId, const TSequenceInfo& sequenceInfo);
    void PersistSequenceAlterRemove(NIceDb::TNiceDb& db, TPathId pathId);

    // Replication
    void PersistReplication(NIceDb::TNiceDb& db, TPathId pathId, const TReplicationInfo& replicationInfo);
    void PersistReplicationRemove(NIceDb::TNiceDb& db, TPathId pathId);
    void PersistReplicationAlter(NIceDb::TNiceDb& db, TPathId pathId, const TReplicationInfo& replicationInfo);
    void PersistReplicationAlterRemove(NIceDb::TNiceDb& db, TPathId pathId);

    // BlobDepot
    void PersistBlobDepot(NIceDb::TNiceDb& db, TPathId pathId, const TBlobDepotInfo& blobDepotInfo);

    void PersistAddTableShardPartitionConfig(NIceDb::TNiceDb& db, TShardIdx shardIdx, const NKikimrSchemeOp::TPartitionConfig& config);

    void PersistPublishingPath(NIceDb::TNiceDb& db, TTxId txId, TPathId pathId, ui64 version);
    void PersistRemovePublishingPath(NIceDb::TNiceDb& db, TTxId txId, TPathId pathId, ui64 version);


    void PersistInitState(NIceDb::TNiceDb& db);

    void PersistStorageBillingTime(NIceDb::TNiceDb& db);

    // ExternalTable
    void PersistExternalTable(NIceDb::TNiceDb &db, TPathId pathId, const TExternalTableInfo::TPtr externalTable);
    void PersistRemoveExternalTable(NIceDb::TNiceDb& db, TPathId pathId);

    // ExternalDataSource
    void PersistExternalDataSource(NIceDb::TNiceDb &db, TPathId pathId, const TExternalDataSourceInfo::TPtr externalDataSource);
    void PersistRemoveExternalDataSource(NIceDb::TNiceDb& db, TPathId pathId);

    void PersistView(NIceDb::TNiceDb &db, TPathId pathId);
    void PersistRemoveView(NIceDb::TNiceDb& db, TPathId pathId);

    // ResourcePool
    void PersistResourcePool(NIceDb::TNiceDb& db, TPathId pathId, const TResourcePoolInfo::TPtr resourcePool);
    void PersistRemoveResourcePool(NIceDb::TNiceDb& db, TPathId pathId);

    TTabletId GetGlobalHive(const TActorContext& ctx) const;

    enum class EHiveSelection : uint8_t {
        ANY,
        IGNORE_TENANT,
    };

    TTabletId ResolveHive(TPathId pathId, const TActorContext& ctx, EHiveSelection selection) const;
    TTabletId ResolveHive(TPathId pathId, const TActorContext& ctx) const;
    TTabletId ResolveHive(TShardIdx shardIdx, const TActorContext& ctx) const;
    TShardIdx GetShardIdx(TTabletId tabletId) const;
    TShardIdx MustGetShardIdx(TTabletId tabletId) const;
    TTabletTypes::EType GetTabletType(TTabletId tabletId) const;

    struct TTxMonitoring;
    //OnRenderAppHtmlPage

    struct TTxInit;
    NTabletFlatExecutor::ITransaction* CreateTxInit();

    struct TTxInitRoot;
    NTabletFlatExecutor::ITransaction* CreateTxInitRoot();

    struct TTxInitRootCompatibility;
    NTabletFlatExecutor::ITransaction* CreateTxInitRootCompatibility(TEvSchemeShard::TEvInitRootShard::TPtr &ev);

    struct TTxInitTenantSchemeShard;
    NTabletFlatExecutor::ITransaction* CreateTxInitTenantSchemeShard(TEvSchemeShard::TEvInitTenantSchemeShard::TPtr &ev);

    struct TActivationOpts {
        TSideEffects::TPublications DelayPublications;
        TVector<ui64> ExportIds;
        TVector<ui64> ImportsIds;
        TVector<TPathId> CdcStreamScans;
        TVector<TPathId> TablesToClean;
        TDeque<TPathId> BlockStoreVolumesToClean;
    };

    void SubscribeToTempTableOwners();

    void ActivateAfterInitialization(const TActorContext& ctx, TActivationOpts&& opts);

    struct TTxInitPopulator;
    NTabletFlatExecutor::ITransaction* CreateTxInitPopulator(TSideEffects::TPublications&& publications);

    struct TTxInitSchema;
    NTabletFlatExecutor::ITransaction* CreateTxInitSchema();

    struct TTxUpgradeSchema;
    NTabletFlatExecutor::ITransaction* CreateTxUpgradeSchema();

    struct TTxCleanTables;
    NTabletFlatExecutor::ITransaction* CreateTxCleanTables(TVector<TPathId> tablesToClean);

    struct TTxCleanBlockStoreVolumes;
    NTabletFlatExecutor::ITransaction* CreateTxCleanBlockStoreVolumes(TDeque<TPathId>&& blockStoreVolumes);

    struct TTxCleanDroppedPaths;
    NTabletFlatExecutor::ITransaction* CreateTxCleanDroppedPaths();

    void ScheduleCleanDroppedPaths();
    void Handle(TEvPrivate::TEvCleanDroppedPaths::TPtr& ev, const TActorContext& ctx);

    void EnqueueBackgroundCompaction(const TShardIdx& shardIdx, const TPartitionStats& stats);
    void UpdateBackgroundCompaction(const TShardIdx& shardIdx, const TPartitionStats& stats);
    bool RemoveBackgroundCompaction(const TShardIdx& shardIdx);

    void EnqueueBorrowedCompaction(const TShardIdx& shardIdx);
    void RemoveBorrowedCompaction(const TShardIdx& shardIdx);

    void EnqueueBackgroundCleaning(const TPathId& pathId);
    void RemoveBackgroundCleaning(const TPathId& pathId);
    std::optional<TTempDirInfo> ResolveTempDirInfo(const TPathId& pathId);

    void UpdateShardMetrics(const TShardIdx& shardIdx, const TPartitionStats& newStats);
    void RemoveShardMetrics(const TShardIdx& shardIdx);

    void ShardRemoved(const TShardIdx& shardIdx);

    NOperationQueue::EStartStatus StartBackgroundCompaction(const TShardCompactionInfo& info);
    void OnBackgroundCompactionTimeout(const TShardCompactionInfo& info);
    void UpdateBackgroundCompactionQueueMetrics();

    NOperationQueue::EStartStatus StartBorrowedCompaction(const TShardIdx& shardIdx);
    void OnBorrowedCompactionTimeout(const TShardIdx& shardIdx);
    void BorrowedCompactionHandleDisconnect(TTabletId tabletId, const TActorId& clientId);
    void UpdateBorrowedCompactionQueueMetrics();

    NOperationQueue::EStartStatus StartBackgroundCleaning(const TPathId& pathId);
    bool ContinueBackgroundCleaning(const TPathId& pathId);
    void OnBackgroundCleaningTimeout(const TPathId& pathId);
    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext& ctx);
    bool CheckOwnerUndelivered(TEvents::TEvUndelivered::TPtr& ev);
    void RetryNodeSubscribe(ui32 nodeId);
    void Handle(TEvPrivate::TEvRetryNodeSubscribe::TPtr& ev, const TActorContext& ctx);
    void HandleBackgroundCleaningTransactionResult(
        TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& result);
    void HandleBackgroundCleaningCompletionResult(const TTxId& txId);
    void CleanBackgroundCleaningState(const TPathId& pathId);
    void ClearTempDirsState();

    struct TTxCleanDroppedSubDomains;
    NTabletFlatExecutor::ITransaction* CreateTxCleanDroppedSubDomains();

    void ScheduleCleanDroppedSubDomains();
    void Handle(TEvPrivate::TEvCleanDroppedSubDomains::TPtr& ev, const TActorContext& ctx);

    struct TTxFixBadPaths;
    NTabletFlatExecutor::ITransaction* CreateTxFixBadPaths();

    struct TTxPublishTenantAsReadOnly;
    NTabletFlatExecutor::ITransaction* CreateTxPublishTenantAsReadOnly(TEvSchemeShard::TEvPublishTenantAsReadOnly::TPtr &ev);

    struct TTxPublishTenant;
    NTabletFlatExecutor::ITransaction* CreateTxPublishTenant(TEvSchemeShard::TEvPublishTenant::TPtr &ev);

    struct TTxMigrate;
    NTabletFlatExecutor::ITransaction* CreateTxMigrate(TEvSchemeShard::TEvMigrateSchemeShard::TPtr &ev);

    struct TTxDescribeScheme;
    NTabletFlatExecutor::ITransaction* CreateTxDescribeScheme(TEvSchemeShard::TEvDescribeScheme::TPtr &ev);

    struct TTxNotifyCompletion;
    NTabletFlatExecutor::ITransaction* CreateTxNotifyTxCompletion(TEvSchemeShard::TEvNotifyTxCompletion::TPtr &ev);

    struct TTxDeleteTabletReply;
    NTabletFlatExecutor::ITransaction* CreateTxDeleteTabletReply(TEvHive::TEvDeleteTabletReply::TPtr& ev);

    struct TTxShardStateChanged;
    NTabletFlatExecutor::ITransaction* CreateTxShardStateChanged(TEvDataShard::TEvStateChanged::TPtr& ev);

    struct TTxRunConditionalErase;
    NTabletFlatExecutor::ITransaction* CreateTxRunConditionalErase(TEvPrivate::TEvRunConditionalErase::TPtr& ev);

    struct TTxScheduleConditionalErase;
    NTabletFlatExecutor::ITransaction* CreateTxScheduleConditionalErase(TEvDataShard::TEvConditionalEraseRowsResponse::TPtr& ev);

    struct TTxSyncTenant;
    NTabletFlatExecutor::ITransaction* CreateTxSyncTenant(TPathId tabletId);
    struct TTxUpdateTenant;
    NTabletFlatExecutor::ITransaction* CreateTxUpdateTenant(TEvSchemeShard::TEvUpdateTenantSchemeShard::TPtr& ev);

    struct TTxPublishToSchemeBoard;
    NTabletFlatExecutor::ITransaction* CreateTxPublishToSchemeBoard(THashMap<TTxId, TDeque<TPathId>>&& paths);
    struct TTxAckPublishToSchemeBoard;
    NTabletFlatExecutor::ITransaction* CreateTxAckPublishToSchemeBoard(NSchemeBoard::NSchemeshardEvents::TEvUpdateAck::TPtr& ev);

    struct TTxOperationPropose;
    NTabletFlatExecutor::ITransaction* CreateTxOperationPropose(TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev);

    struct TTxOperationProposeCancelTx;
    NTabletFlatExecutor::ITransaction* CreateTxOperationPropose(TEvSchemeShard::TEvCancelTx::TPtr& ev);

    struct TTxOperationProgress;
    NTabletFlatExecutor::ITransaction* CreateTxOperationProgress(TOperationId opId);

    struct TTxOperationPlanStep;
    NTabletFlatExecutor::ITransaction* CreateTxOperationPlanStep(TEvTxProcessing::TEvPlanStep::TPtr& ev);

    struct TTxUpgradeAccessDatabaseRights;
    NTabletFlatExecutor::ITransaction* CreateTxUpgradeAccessDatabaseRights(const TActorId& answerTo, bool isDryRun, std::function< NActors::IEventBase* (const TMap<TPathId, TSet<TString>>&) >);

    struct TTxMakeAccessDatabaseNoInheritable;
    NTabletFlatExecutor::ITransaction* CreateTxMakeAccessDatabaseNoInheritable(const TActorId& answerTo, bool isDryRun, std::function< NActors::IEventBase* (const TMap<TPathId, TSet<TString>>&) >);

    struct TTxServerlessStorageBilling;
    NTabletFlatExecutor::ITransaction* CreateTxServerlessStorageBilling();

    struct TTxLogin;
    NTabletFlatExecutor::ITransaction* CreateTxLogin(TEvSchemeShard::TEvLogin::TPtr &ev);

    template <EventBasePtr TEvPtr>
    NTabletFlatExecutor::ITransaction* CreateTxOperationReply(TOperationId id, TEvPtr& ev);

    void PublishToSchemeBoard(THashMap<TTxId, TDeque<TPathId>>&& paths, const TActorContext& ctx);
    void PublishToSchemeBoard(TTxId txId, TDeque<TPathId>&& paths, const TActorContext& ctx);

    void ApplyPartitionConfigStoragePatch(
        NKikimrSchemeOp::TPartitionConfig& config,
        const NKikimrSchemeOp::TPartitionConfig& patch) const;
    void FillTableDescriptionForShardIdx(
        TPathId tableId, TShardIdx shardIdx, NKikimrSchemeOp::TTableDescription* tableDescr,
        TString rangeBegin, TString rangeEnd,
        bool rangeBeginInclusive, bool rangeEndInclusive,
        bool newTable = false);
    void FillTableDescription(TPathId tableId, ui32 partitionIdx, ui64 schemaVersion, NKikimrSchemeOp::TTableDescription* tableDescr);
    static bool FillUniformPartitioning(TVector<TString>& rangeEnds, ui32 keySize, NScheme::TTypeInfo firstKeyColType,
                                        ui32 partitionCount, const NScheme::TTypeRegistry* typeRegistry, TString& errStr);
    static bool FillSplitPartitioning(TVector<TString>& rangeEnds, const TConstArrayRef<NScheme::TTypeInfo>& keyColTypes,
                                      const ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TSplitBoundary>& boundaries,
                                      TString& errStr);

    TString FillAlterTableTxBody(TPathId tableId, TShardIdx shardIdx, TMessageSeqNo seqNo) const;
    TString FillBackupTxBody(TPathId pathId, const NKikimrSchemeOp::TBackupTask& task, ui32 shardNum, TMessageSeqNo seqNo) const;

    static void FillSeqNo(NKikimrTxDataShard::TFlatSchemeTransaction &tx, TMessageSeqNo seqNo);
    static void FillSeqNo(NKikimrTxColumnShard::TSchemaTxBody &tx, TMessageSeqNo seqNo);

    void FillAsyncIndexInfo(const TPathId& tableId, NKikimrTxDataShard::TFlatSchemeTransaction& tx);

    void DescribeTable(const TTableInfo& tableInfo, const NScheme::TTypeRegistry* typeRegistry,
                       bool fillConfig, NKikimrSchemeOp::TTableDescription* entry) const;
    void DescribeTableIndex(const TPathId& pathId, const TString& name,
        bool fillConfig, bool fillBoundaries, NKikimrSchemeOp::TIndexDescription& entry
    ) const;
    void DescribeTableIndex(const TPathId& pathId, const TString& name, TTableIndexInfo::TPtr indexInfo,
        bool fillConfig, bool fillBoundaries, NKikimrSchemeOp::TIndexDescription& entry
    ) const;
    void DescribeCdcStream(const TPathId& pathId, const TString& name, NKikimrSchemeOp::TCdcStreamDescription& desc);
    void DescribeCdcStream(const TPathId& pathId, const TString& name, TCdcStreamInfo::TPtr info, NKikimrSchemeOp::TCdcStreamDescription& desc);
    void DescribeSequence(const TPathId& pathId, const TString& name,
        NKikimrSchemeOp::TSequenceDescription& desc, bool fillSetVal = false);
    void DescribeSequence(const TPathId& pathId, const TString& name, TSequenceInfo::TPtr info,
        NKikimrSchemeOp::TSequenceDescription& desc, bool fillSetVal = false);
    void DescribeReplication(const TPathId& pathId, const TString& name, NKikimrSchemeOp::TReplicationDescription& desc);
    void DescribeReplication(const TPathId& pathId, const TString& name, TReplicationInfo::TPtr info, NKikimrSchemeOp::TReplicationDescription& desc);
    void DescribeBlobDepot(const TPathId& pathId, const TString& name, NKikimrSchemeOp::TBlobDepotDescription& desc);

    void Handle(NKikimr::NOlap::NBackground::TEvExecuteGeneralLocalTransaction::TPtr& ev, const TActorContext& ctx);
    void Handle(NKikimr::NOlap::NBackground::TEvRemoveSession::TPtr& ev, const TActorContext& ctx);


    void Handle(TEvSchemeShard::TEvInitRootShard::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvSchemeShard::TEvInitTenantSchemeShard::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvSchemeShard::TEvModifySchemeTransaction::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvSchemeShard::TEvDescribeScheme::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvSchemeShard::TEvNotifyTxCompletion::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvSchemeShard::TEvCancelTx::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPrivate::TEvProgressOperation::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvPersQueue::TEvProposeTransactionAttachResult::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvHive::TEvCreateTabletReply::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvHive::TEvAdoptTabletReply::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvHive::TEvDeleteTabletReply::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvSubscribeToShardDeletion::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvHive::TEvDeleteOwnerTabletsReply::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvHive::TEvUpdateTabletsObjectReply::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvHive::TEvUpdateDomainReply::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPersQueue::TEvDropTabletReply::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSequenceShard::TEvSequenceShard::TEvFreezeSequenceResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSequenceShard::TEvSequenceShard::TEvRedirectSequenceResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NReplication::TEvController::TEvCreateReplicationResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NReplication::TEvController::TEvAlterReplicationResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NReplication::TEvController::TEvDropReplicationResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvSchemaChanged::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvStateChanged::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPersQueue::TEvUpdateConfigResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBlobDepot::TEvApplyConfigResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvSubDomain::TEvConfigureStatus::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvFileStore::TEvUpdateConfigResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKesus::TEvKesus::TEvSetConfigResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvInitTenantSchemeShardResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvPublishTenantAsReadOnly::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvPublishTenant::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvPublishTenantResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvMigrateSchemeShard::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvMigrateSchemeShardResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvMigrateSchemeShardResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvCompactTableResult::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvCompactBorrowedResult::TPtr &ev, const TActorContext &ctx);


    void Handle(TEvSchemeShard::TEvProcessingRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvSyncTenantSchemeShard::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvUpdateTenantSchemeShard::TPtr& ev, const TActorContext& ctx);

    void Handle(NSchemeBoard::NSchemeshardEvents::TEvUpdateAck::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvTxProcessing::TEvPlanStep::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx);
    void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvDataShard::TEvInitSplitMergeDestinationAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvSplitAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvSplitPartitioningChangedAck::TPtr& ev, const TActorContext& ctx);

    void ExecuteTableStatsBatch(const TActorContext& ctx);
    void ScheduleTableStatsBatch(const TActorContext& ctx);
    void Handle(TEvPrivate::TEvPersistTableStats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvPeriodicTableStats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvGetTableStatsResult::TPtr& ev, const TActorContext& ctx);

    void ExecuteTopicStatsBatch(const TActorContext& ctx);
    void ScheduleTopicStatsBatch(const TActorContext& ctx);
    void Handle(TEvPrivate::TEvPersistTopicStats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvPeriodicTopicStats::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvSchemeShard::TEvFindTabletSubDomainPathId::TPtr& ev, const TActorContext& ctx);

    void ScheduleConditionalEraseRun(const TActorContext& ctx);
    void Handle(TEvPrivate::TEvRunConditionalErase::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvConditionalEraseRowsResponse::TPtr& ev, const TActorContext& ctx);
    void ConditionalEraseHandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx);

    void Handle(NSysView::TEvSysView::TEvGetPartitionStats::TPtr& ev, const TActorContext& ctx);

    void ScheduleServerlessStorageBilling(const TActorContext& ctx);
    void Handle(TEvPrivate::TEvServerlessStorageBilling::TPtr& ev, const TActorContext& ctx);

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvSchemeShard::TEvLogin::TPtr& ev, const TActorContext& ctx);

    void RestartPipeTx(TTabletId tabletId, const TActorContext& ctx);

    TOperationId RouteIncoming(TTabletId tabletId, const TActorContext& ctx);

    // namespace NLongRunningCommon {
    struct TXxport {
        class TTxBase;
        template <typename TInfo, typename TEvRequest, typename TEvResponse> struct TTxGet;
        template <typename TInfo, typename TEvRequest, typename TEvResponse, typename TDerived> struct TTxList;
    };

    void Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvIndexBuilder::TEvCreateResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvCancelTxResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvIndexBuilder::TEvCancelResponse::TPtr& ev, const TActorContext& ctx);
    // } // NLongRunningCommon

    // namespace NExport {
    THashMap<ui64, TExportInfo::TPtr> Exports;
    THashMap<TString, TExportInfo::TPtr> ExportsByUid;
    THashMap<TTxId, std::pair<ui64, ui32>> TxIdToExport;
    THashMap<TTxId, THashSet<ui64>> TxIdToDependentExport;

    void FromXxportInfo(NKikimrExport::TExport& exprt, const TExportInfo::TPtr exportInfo);

    static void PersistCreateExport(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo);
    static void PersistRemoveExport(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo);
    static void PersistExportPathId(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo);
    static void PersistExportState(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo);
    static void PersistExportItemState(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo, ui32 targetIdx);

    struct TExport {
        struct TTxCreate;
        struct TTxGet;
        struct TTxCancel;
        struct TTxCancelAck;
        struct TTxForget;
        struct TTxList;

        struct TTxProgress;
    };

    NTabletFlatExecutor::ITransaction* CreateTxCreateExport(TEvExport::TEvCreateExportRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxGetExport(TEvExport::TEvGetExportRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxCancelExport(TEvExport::TEvCancelExportRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxCancelExportAck(TEvSchemeShard::TEvCancelTxResult::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxForgetExport(TEvExport::TEvForgetExportRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxListExports(TEvExport::TEvListExportsRequest::TPtr& ev);

    NTabletFlatExecutor::ITransaction* CreateTxProgressExport(ui64 id);
    NTabletFlatExecutor::ITransaction* CreateTxProgressExport(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxProgressExport(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxProgressExport(TTxId completedTxId);

    void Handle(TEvExport::TEvCreateExportRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExport::TEvGetExportRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExport::TEvCancelExportRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExport::TEvForgetExportRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExport::TEvListExportsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TAutoPtr<TEventHandle<NSchemeShard::NBackground::TEvListRequest>>& ev, const TActorContext& ctx);

    void ResumeExports(const TVector<ui64>& exportIds, const TActorContext& ctx);
    // } // NExport

    // namespace NImport {
    THashMap<ui64, TImportInfo::TPtr> Imports;
    THashMap<TString, TImportInfo::TPtr> ImportsByUid;
    THashMap<TTxId, std::pair<ui64, ui32>> TxIdToImport;

    void FromXxportInfo(NKikimrImport::TImport& exprt, const TImportInfo::TPtr importInfo);

    static void PersistCreateImport(NIceDb::TNiceDb& db, const TImportInfo::TPtr importInfo);
    static void PersistRemoveImport(NIceDb::TNiceDb& db, const TImportInfo::TPtr importInfo);
    static void PersistImportState(NIceDb::TNiceDb& db, const TImportInfo::TPtr importInfo);
    static void PersistImportItemState(NIceDb::TNiceDb& db, const TImportInfo::TPtr importInfo, ui32 itemIdx);
    static void PersistImportItemScheme(NIceDb::TNiceDb& db, const TImportInfo::TPtr importInfo, ui32 itemIdx);
    static void PersistImportItemDstPathId(NIceDb::TNiceDb& db, const TImportInfo::TPtr importInfo, ui32 itemIdx);

    struct TImport {
        struct TTxCreate;
        struct TTxGet;
        struct TTxCancel;
        struct TTxCancelAck;
        struct TTxForget;
        struct TTxList;

        struct TTxProgress;
    };

    NTabletFlatExecutor::ITransaction* CreateTxCreateImport(TEvImport::TEvCreateImportRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxGetImport(TEvImport::TEvGetImportRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxCancelImport(TEvImport::TEvCancelImportRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxCancelImportAck(TEvSchemeShard::TEvCancelTxResult::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxCancelImportAck(TEvIndexBuilder::TEvCancelResponse::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxForgetImport(TEvImport::TEvForgetImportRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxListImports(TEvImport::TEvListImportsRequest::TPtr& ev);

    NTabletFlatExecutor::ITransaction* CreateTxProgressImport(ui64 id, const TMaybe<ui32>& itemIdx = Nothing());
    NTabletFlatExecutor::ITransaction* CreateTxProgressImport(TEvPrivate::TEvImportSchemeReady::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxProgressImport(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxProgressImport(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxProgressImport(TEvIndexBuilder::TEvCreateResponse::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxProgressImport(TTxId completedTxId);

    void Handle(TEvImport::TEvCreateImportRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvImport::TEvGetImportRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvImport::TEvCancelImportRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvImport::TEvForgetImportRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvImport::TEvListImportsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvImportSchemeReady::TPtr& ev, const TActorContext& ctx);

    void ResumeImports(const TVector<ui64>& ids, const TActorContext& ctx);
    // } // NImport

    // namespace NBackup {
    void Handle(TEvBackup::TEvFetchBackupCollectionsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBackup::TEvListBackupCollectionsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBackup::TEvCreateBackupCollectionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBackup::TEvReadBackupCollectionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBackup::TEvUpdateBackupCollectionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBackup::TEvDeleteBackupCollectionRequest::TPtr& ev, const TActorContext& ctx);
    // } // NBackup

    void FillTableSchemaVersion(ui64 schemaVersion, NKikimrSchemeOp::TTableDescription *tableDescr) const;

    // namespace NIndexBuilder {
    TControlWrapper AllowDataColumnForIndexTable;

    THashMap<TIndexBuildId, TIndexBuildInfo::TPtr> IndexBuilds;
    THashMap<TString, TIndexBuildInfo::TPtr> IndexBuildsByUid;
    THashMap<TTxId, TIndexBuildId> TxIdToIndexBuilds;

    // do not share pipes with operations
    // also do not share pipes between IndexBuilds
    TDedicatedPipePool<TIndexBuildId> IndexBuildPipes;

    void PersistCreateBuildIndex(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexState(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexIssue(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexCancelRequest(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);

    void PersistBuildIndexAlterMainTableTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexAlterMainTableTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexAlterMainTableTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);

    void PersistBuildIndexInitiateTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexInitiateTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexInitiateTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);

    void PersistBuildIndexLockTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexLockTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexLockTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);

    void PersistBuildIndexApplyTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexApplyTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexApplyTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);

    void PersistBuildIndexUnlockTxId(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexUnlockTxStatus(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);
    void PersistBuildIndexUnlockTxDone(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);

    void PersistBuildIndexUploadProgress(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo, const TShardIdx& shardIdx);
    void PersistBuildIndexUploadInitiate(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo, const TShardIdx& shardIdx);
    void PersistBuildIndexBilling(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);

    void PersistBuildIndexForget(NIceDb::TNiceDb& db, const TIndexBuildInfo::TPtr indexInfo);

    struct TIndexBuilder {
        class TTxBase;

        template<typename TRequest, typename TResponse>
        class TTxSimple;

        class TTxCreate;
        struct TTxGet;
        struct TTxCancel;
        struct TTxForget;
        struct TTxList;

        struct TTxProgress;
        struct TTxReply;

        struct TTxPipeReset;
        struct TTxBilling;
    };

    NTabletFlatExecutor::ITransaction* CreateTxCreate(TEvIndexBuilder::TEvCreateRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxGet(TEvIndexBuilder::TEvGetRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxCancel(TEvIndexBuilder::TEvCancelRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxForget(TEvIndexBuilder::TEvForgetRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxList(TEvIndexBuilder::TEvListRequest::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxProgress(TIndexBuildId id);
    NTabletFlatExecutor::ITransaction* CreateTxReply(TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult);
    NTabletFlatExecutor::ITransaction* CreateTxReply(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult);
    NTabletFlatExecutor::ITransaction* CreateTxReply(TTxId completedTxId);
    NTabletFlatExecutor::ITransaction* CreateTxReply(TEvDataShard::TEvBuildIndexProgressResponse::TPtr& progress);
    NTabletFlatExecutor::ITransaction* CreatePipeRetry(TIndexBuildId indexBuildId, TTabletId tabletId);
    NTabletFlatExecutor::ITransaction* CreateTxBilling(TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev);

    void Handle(TEvIndexBuilder::TEvCreateRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvIndexBuilder::TEvGetRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvIndexBuilder::TEvCancelRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvIndexBuilder::TEvForgetRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvIndexBuilder::TEvListRequest::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvDataShard::TEvBuildIndexProgressResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev, const TActorContext& ctx);

    void Resume(const TDeque<TIndexBuildId>& indexIds, const TActorContext& ctx);
    void SetupRouting(const TDeque<TIndexBuildId>& indexIds, const TActorContext& ctx);

    // } //NIndexBuilder

    // namespace NCdcStreamScan {
    struct TCdcStreamScan {
        struct TTxProgress;
    };

    TDedicatedPipePool<TPathId> CdcStreamScanPipes;

    NTabletFlatExecutor::ITransaction* CreateTxProgressCdcStreamScan(TEvPrivate::TEvRunCdcStreamScan::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreateTxProgressCdcStreamScan(TEvDataShard::TEvCdcStreamScanResponse::TPtr& ev);
    NTabletFlatExecutor::ITransaction* CreatePipeRetry(const TPathId& streamPathId, TTabletId tabletId);

    void Handle(TEvPrivate::TEvRunCdcStreamScan::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDataShard::TEvCdcStreamScanResponse::TPtr& ev, const TActorContext& ctx);

    void ResumeCdcStreamScans(const TVector<TPathId>& ids, const TActorContext& ctx);

    void PersistCdcStreamScanShardStatus(NIceDb::TNiceDb& db, const TPathId& streamPathId, const TShardIdx& shardIdx,
        const TCdcStreamInfo::TShardStatus& status);
    void RemoveCdcStreamScanShardStatus(NIceDb::TNiceDb& db, const TPathId& streamPathId, const TShardIdx& shardIdx);
    // } // NCdcStreamScan

    // statistics
    TTabletId StatisticsAggregatorId;
    TActorId SAPipeClientId;
    static constexpr ui64 SendStatsIntervalMinSeconds = 180;
    static constexpr ui64 SendStatsIntervalMaxSeconds = 240;

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr&, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvSendBaseStatsToSA::TPtr& ev, const TActorContext& ctx);

    void InitializeStatistics(const TActorContext& ctx);
    void ResolveSA();
    void ConnectToSA();
    void SendBaseStatsToSA();



public:
    void ChangeStreamShardsCount(i64 delta) override;
    void ChangeStreamShardsQuota(i64 delta) override;
    void ChangeStreamReservedStorageCount(i64 delta) override;
    void ChangeStreamReservedStorageQuota(i64 delta) override;
    void ChangeDiskSpaceTablesDataBytes(i64 delta) override;
    void ChangeDiskSpaceTablesIndexBytes(i64 delta) override;
    void ChangeDiskSpaceTablesTotalBytes(i64 delta) override;
    void AddDiskSpaceTables(EUserFacingStorageType storageType, ui64 data, ui64 index) override;
    void ChangeDiskSpaceTopicsTotalBytes(ui64 value) override;
    void ChangeDiskSpaceQuotaExceeded(i64 delta) override;
    void ChangeDiskSpaceHardQuotaBytes(i64 delta) override;
    void ChangeDiskSpaceSoftQuotaBytes(i64 delta) override;
    void AddDiskSpaceSoftQuotaBytes(EUserFacingStorageType storageType, ui64 addend) override;

    NLogin::TLoginProvider LoginProvider;

private:
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override;
    void DefaultSignalTabletActive(const TActorContext &ctx) override;
    void Cleanup(const TActorContext &ctx);
    void Enqueue(STFUNC_SIG) override;
    void Die(const TActorContext &ctx) override;

    bool ReassignChannelsEnabled() const override {
        return true;
    }

    const TDomainsInfo::TDomain& GetDomainDescription(const TActorContext &ctx) const;
    NKikimrSubDomains::TProcessingParams CreateRootProcessingParams(const TActorContext &ctx);
    static NTabletPipe::TClientConfig GetPipeClientConfig();

public:
    static const NKikimrConfig::TDomainsConfig& GetDomainsConfig();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FLAT_SCHEMESHARD_ACTOR;
    }

    TSchemeShard(const TActorId &tablet, TTabletStorageInfo *info);

    //TTabletId TabletID() const { return TTabletId(ITablet::TabletID()); }
    TTabletId SelfTabletId() const { return TTabletId(ITablet::TabletID()); }

    STFUNC(StateInit);
    STFUNC(StateConfigure);
    STFUNC(StateWork);
    STFUNC(BrokenState);

    // A helper that enforces write-only access to the internal DB (reads must be done from the
    // internal structures)
    class TRwTxBase : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    protected:
        TDuration ExecuteDuration;

    protected:
        TRwTxBase(TSchemeShard* self) : TBase(self) {}

    public:
        virtual ~TRwTxBase() {}

        bool Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) override;
        void Complete(const TActorContext &ctx) override;

        virtual void DoExecute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) = 0;
        virtual void DoComplete(const TActorContext &ctx) = 0;
    };
};

}
}
