#pragma once

#include "schemeshard_info_types_topic.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/util/counted_leaky_bucket.h>

#include <ydb/library/login/protos/login.pb.h>

#include <ydb/public/api/protos/ydb_cms.pb.h>

namespace NKikimr {
namespace NSchemeShard {

using TSchemeQuota = TCountedLeakyBucket;

struct TSchemeQuotas : public TVector<TSchemeQuota> {
    mutable size_t LastKnownSize = 0;
};

enum class EQuotaUsageStatus {
    AboveHardQuota,
    InBetween,
    BelowSoftQuota,
};

enum class EPathCategory : ui8 {
    Regular = 0,
    Backup,
    System,
};

struct TSubDomainInfo: TSimpleRefCount<TSubDomainInfo> {
    using TPtr = TIntrusivePtr<TSubDomainInfo>;
    using TConstPtr = TIntrusiveConstPtr<TSubDomainInfo>;

    struct TDiskSpaceUsage {
        struct TTables {
            ui64 TotalSize = 0;
            ui64 DataSize = 0;
            ui64 IndexSize = 0;
        } Tables;

        struct TTopics {
            ui64 DataSize = 0;
            ui64 UsedReserveSize = 0;
        } Topics;

        struct TStoragePoolUsage {
            ui64 DataSize = 0;
            ui64 IndexSize = 0;
        };
        THashMap<TString, TStoragePoolUsage> StoragePoolsUsage;
    };

    struct TDiskSpaceQuotas {
        ui64 HardQuota;
        ui64 SoftQuota;

        struct TQuotasPair {
            ui64 HardQuota;
            ui64 SoftQuota;
        };
        THashMap<TString, TQuotasPair> StoragePoolsQuotas;

        explicit operator bool() const {
            return HardQuota || SoftQuota || AnyOf(StoragePoolsQuotas, [](const auto& storagePoolQuota) {
                    return storagePoolQuota.second.HardQuota || storagePoolQuota.second.SoftQuota;
                }
            );
        }
    };

    struct TSmallBlobsQuotas {
        ui64 VolumeHardQuota = 0;
        ui64 VolumeSoftQuota = 0;
        ui64 CountHardQuota = 0;
        ui64 CountSoftQuota = 0;
    };

    struct TSmallBlobsUsage {
        ui64 VolumeBytes = 0;
        ui64 Count = 0;
    };

    TSubDomainInfo() = default;
    explicit TSubDomainInfo(ui64 version, const TPathId& resourcesDomainId)
    {
        ProcessingParams.SetVersion(version);
        ResourcesDomainId = resourcesDomainId;
    }

    TSubDomainInfo(ui64 version, ui64 resolution, ui32 bucketsPerMediator, const TPathId& resourcesDomainId)
    {
        ProcessingParams.SetVersion(version);
        ProcessingParams.SetPlanResolution(resolution);
        ProcessingParams.SetTimeCastBucketsPerMediator(bucketsPerMediator);
        ResourcesDomainId = resourcesDomainId;
    }

    TSubDomainInfo(const TSubDomainInfo& other
                   , ui64 planResolution
                   , ui64 timeCastBucketsMediator
                   , TStoragePools additionalPools = {})
        : TSubDomainInfo(other)
    {
        ProcessingParams.SetVersion(other.GetVersion() + 1);

        if (planResolution) {
            Y_ENSURE(other.GetPlanResolution() == 0 || other.GetPlanResolution() == planResolution);
            ProcessingParams.SetPlanResolution(planResolution);
        }

        if (timeCastBucketsMediator) {
            Y_ENSURE(other.GetTCB() == 0 || other.GetTCB() == timeCastBucketsMediator);
            ProcessingParams.SetTimeCastBucketsPerMediator(timeCastBucketsMediator);
        }

        for (auto& toAdd: additionalPools) {
            StoragePools.push_back(toAdd);
        }
    }

    void SetSchemeLimits(const TSchemeLimits& limits, IQuotaCounters* counters = nullptr) {
        SchemeLimits = limits;
        if (counters) {
            counters->SetPathsQuota(limits.MaxPaths);
            counters->SetShardsQuota(limits.MaxShards);
        }
    }

    void MergeSchemeLimits(const NKikimrSubDomains::TSchemeLimits& in, IQuotaCounters* counters = nullptr) {
        SchemeLimits.MergeFromProto(in);
        if (counters) {
            if (in.HasMaxPaths()) {
                counters->SetPathsQuota(SchemeLimits.MaxPaths);
            }
            if (in.HasMaxShards()) {
                counters->SetShardsQuota(SchemeLimits.MaxShards);
            }
        }
    }

    const TSchemeLimits& GetSchemeLimits() const {
        return SchemeLimits;
    }

    ui64 GetVersion() const {
        return ProcessingParams.GetVersion();
    }

    TPtr GetAlter() const {
        return AlterData;
    }

    void SetAlterPrivate(TPtr alterData) {
        AlterData = alterData;
    }

    void SetVersion(ui64 version) {
        Y_ENSURE(ProcessingParams.GetVersion() < version);
        ProcessingParams.SetVersion(version);
    }

    void SetAlter(TPtr alterData) {
        Y_ENSURE(alterData);
        Y_ENSURE(GetVersion() < alterData->GetVersion());
        AlterData = alterData;
    }

    void SetStoragePools(TStoragePools& storagePools, ui64 subDomainVersion) {
        Y_ENSURE(GetVersion() < subDomainVersion);
        StoragePools.swap(storagePools);
        ProcessingParams.SetVersion(subDomainVersion);
    }

    TPathId GetResourcesDomainId() const {
        return ResourcesDomainId;
    }

    void SetResourcesDomainId(const TPathId& domainId) {
        ResourcesDomainId = domainId;
    }

    TTabletId GetSharedHive() const {
        return SharedHive;
    }

    void SetSharedHive(const TTabletId& hiveId) {
        SharedHive = hiveId;
    }

    ui64 GetPlanResolution() const {
        return ProcessingParams.GetPlanResolution();
    }

    ui64 GetTCB() const {
        return ProcessingParams.GetTimeCastBucketsPerMediator();
    }

    TTabletId GetTenantSchemeShardID() const {
        if (!ProcessingParams.HasSchemeShard()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetSchemeShard());
    }

    TTabletId GetTenantHiveID() const {
        if (!ProcessingParams.HasHive()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetHive());
    }

    void SetTenantHiveIDPrivate(const TTabletId& hiveId) {
        ProcessingParams.SetHive(ui64(hiveId));
    }

    TTabletId GetTenantSysViewProcessorID() const {
        if (!ProcessingParams.HasSysViewProcessor()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetSysViewProcessor());
    }

    TTabletId GetTenantStatisticsAggregatorID() const {
        if (!ProcessingParams.HasStatisticsAggregator()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetStatisticsAggregator());
    }

    TTabletId GetTenantBackupControllerID() const {
        if (!ProcessingParams.HasBackupController()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetBackupController());
    }

    TTabletId GetTenantGraphShardID() const {
        if (!ProcessingParams.HasGraphShard()) {
            return InvalidTabletId;
        }
        return TTabletId(ProcessingParams.GetGraphShard());
    }

    ui64 GetPathsInside() const {
        return PathsInsideCount;
    }

    void SetPathsInside(ui64 val) {
        PathsInsideCount = val;
    }

    ui64 GetBackupPaths() const {
        return BackupPathsCount;
    }

    ui64 GetSystemPaths() const {
        return SystemPathsCount;
    }

    void IncPathsInside(IQuotaCounters* counters, ui64 delta = 1, EPathCategory category = EPathCategory::Regular) {
        Y_ENSURE(Max<ui64>() - PathsInsideCount >= delta);
        PathsInsideCount += delta;

        switch (category) {
        case EPathCategory::Backup: {
            Y_ENSURE(Max<ui64>() - BackupPathsCount >= delta);
            BackupPathsCount += delta;
            break;
        }
        case EPathCategory::System: {
            Y_ENSURE(Max<ui64>() - SystemPathsCount >= delta);
            SystemPathsCount += delta;
            break;
        }
        case EPathCategory::Regular: {
            counters->ChangePathCount(delta);
            break;
        }
        }
    }

    void DecPathsInside(IQuotaCounters* counters, ui64 delta = 1, EPathCategory category = EPathCategory::Regular) {
        Y_ENSURE(PathsInsideCount >= delta, "PathsInsideCount: " << PathsInsideCount << " delta: " << delta);
        PathsInsideCount -= delta;

        switch (category) {
        case EPathCategory::Backup: {
            Y_ENSURE(BackupPathsCount >= delta, "BackupPathsCount: " << BackupPathsCount << " delta: " << delta);
            BackupPathsCount -= delta;
            break;
        }
        case EPathCategory::System: {
            Y_ENSURE(SystemPathsCount >= delta, "SystemPathsCount: " << SystemPathsCount << " delta: " << delta);
            SystemPathsCount -= delta;
            break;
        }
        case EPathCategory::Regular: {
            counters->ChangePathCount(-delta);
            break;
        }
        }
    }

    ui64 GetPQPartitionsInside() const {
        return PQPartitionsInsideCount;
    }

    void SetPQPartitionsInside(ui64 val) {
        PQPartitionsInsideCount = val;
    }

    void IncPQPartitionsInside(ui64 delta = 1) {
        Y_ENSURE(Max<ui64>() - PQPartitionsInsideCount >= delta);
        PQPartitionsInsideCount += delta;
    }

    void DecPQPartitionsInside(ui64 delta = 1) {
        Y_ENSURE(PQPartitionsInsideCount >= delta, "PQPartitionsInsideCount: " << PQPartitionsInsideCount << " delta: " << delta);
        PQPartitionsInsideCount -= delta;
    }

    ui64 GetPQGroupsInside() const {
        return PQGroupsInsideCount;
    }

    void SetPQGroupsInside(ui64 val) {
        PQGroupsInsideCount = val;
    }

    void IncPQGroupsInside(ui64 delta = 1) {
        Y_ENSURE(Max<ui64>() - PQGroupsInsideCount >= delta);
        PQGroupsInsideCount += delta;
    }

    void DecPQGroupsInside(ui64 delta = 1) {
        Y_ENSURE(PQGroupsInsideCount >= delta, "PQGroupsInsideCount: " << PQGroupsInsideCount << " delta: " << delta);
        PQGroupsInsideCount -= delta;
    }

    ui64 GetPQReservedStorage() const {
        return PQReservedStorage;
    }

    ui64 GetPQAccountStorage() const {
        const auto& topics = DiskSpaceUsage.Topics;
        return topics.DataSize - std::min(topics.UsedReserveSize, PQReservedStorage) + PQReservedStorage;

    }

    void SetPQReservedStorage(ui64 val) {
        PQReservedStorage = val;
    }

    void IncPQReservedStorage(ui64 delta = 1) {
        Y_ENSURE(Max<ui64>() - PQReservedStorage >= delta);
        PQReservedStorage += delta;
    }

    void DecPQReservedStorage(ui64 delta = 1) {
        Y_ENSURE(PQReservedStorage >= delta, "PQReservedStorage: " << PQReservedStorage << " delta: " << delta);
        PQReservedStorage -= delta;
    }

    void UpdatePQReservedStorage(ui64 oldStorage, ui64 newStorage) {
        if (oldStorage == newStorage)
            return;
        DecPQReservedStorage(oldStorage);
        IncPQReservedStorage(newStorage);
    }

    ui64 GetShardsInside() const {
        return InternalShards.size();
    }

    ui64 GetBackupShards() const {
        return BackupShards.size();
    }

    void UpdateCounters(IQuotaCounters* counters);

    void ActualizeAlterData(const THashMap<TShardIdx, TShardInfo>& allShards, TInstant now, bool isExternal, IQuotaCounters* counters) {
        Y_ENSURE(AlterData);

        AlterData->SetPathsInside(GetPathsInside());
        AlterData->InternalShards.swap(InternalShards);
        AlterData->Initialize(allShards);

        AlterData->SchemeQuotas = SchemeQuotas;
        if (isExternal) {
            AlterData->RemoveSchemeQuotas();
        } else if (!AlterData->DeclaredSchemeQuotas && DeclaredSchemeQuotas) {
            AlterData->DeclaredSchemeQuotas = DeclaredSchemeQuotas;
        } else {
            AlterData->RegenerateSchemeQuotas(now);
        }

        if (!AlterData->DatabaseQuotas && DatabaseQuotas && !isExternal) {
            AlterData->DatabaseQuotas = DatabaseQuotas;
            AlterData->SmallBlobsStorageUnits = SmallBlobsStorageUnits;
        }

        AlterData->DomainStateVersion = DomainStateVersion;
        AlterData->DiskQuotaExceeded = DiskQuotaExceeded;
        AlterData->SmallBlobsQuotaExceeded = SmallBlobsQuotaExceeded;

        // Update usage and recheck quotas (which may have changed by an alter)
        AlterData->DiskSpaceUsage = DiskSpaceUsage;
        AlterData->SmallBlobsUsage = SmallBlobsUsage;
        AlterData->CheckQuotas(counters);

        CountDiskSpaceQuotas(counters, GetDiskSpaceQuotas(), AlterData->GetDiskSpaceQuotas());
        CountSmallBlobsQuotas(counters, GetSmallBlobsQuotas(), AlterData->GetSmallBlobsQuotas());
        CountStreamShardsQuota(counters, GetStreamShardsQuota(), AlterData->GetStreamShardsQuota());
        CountStreamReservedStorageQuota(counters, GetStreamReservedStorageQuota(), AlterData->GetStreamReservedStorageQuota());
    }

    ui64 GetStreamShardsQuota() const {
        return DatabaseQuotas ? DatabaseQuotas->data_stream_shards_quota() : 0;
    }

    ui64 GetStreamReservedStorageQuota() const {
        return DatabaseQuotas ? DatabaseQuotas->data_stream_reserved_storage_quota() : 0;
    }

    TDuration GetTtlMinRunInterval() const {
        static constexpr auto TtlMinRunInterval = TDuration::Minutes(15);

        if (!DatabaseQuotas) {
            return TtlMinRunInterval;
        }

        if (!DatabaseQuotas->ttl_min_run_internal_seconds()) {
            return TtlMinRunInterval;
        }

        return TDuration::Seconds(DatabaseQuotas->ttl_min_run_internal_seconds());
    }

    static void CountDiskSpaceQuotas(IQuotaCounters* counters, const TDiskSpaceQuotas& quotas);

    static void CountDiskSpaceQuotas(IQuotaCounters* counters, const TDiskSpaceQuotas& prev, const TDiskSpaceQuotas& next);

    static void CountSmallBlobsQuotas(IQuotaCounters* counters, const TSmallBlobsQuotas& prev, const TSmallBlobsQuotas& next);

    static void CountStreamShardsQuota(IQuotaCounters* counters, const i64 delta) {
        counters->ChangeStreamShardsQuota(delta);
    }

    static void CountStreamReservedStorageQuota(IQuotaCounters* counters, const i64 delta) {
        counters->ChangeStreamReservedStorageQuota(delta);
    }

    static void CountStreamShardsQuota(IQuotaCounters* counters, const i64& prev, const i64& next) {
        counters->ChangeStreamShardsQuota(next - prev);
    }

    static void CountStreamReservedStorageQuota(IQuotaCounters* counters, const i64& prev, const i64& next) {
        counters->ChangeStreamReservedStorageQuota(next - prev);
    }

    TDiskSpaceQuotas GetDiskSpaceQuotas() const;

    TSmallBlobsQuotas GetSmallBlobsQuotas() const;

    /*
    Checks current disk usage against disk quotas.
    Returns true when DiskQuotaExceeded value has changed and needs to be persisted and pushed to scheme board.
    */
    bool CheckDiskSpaceQuotas(IQuotaCounters* counters);

    /*
    Checks current small-blobs usage against small-blobs quotas.
    Returns true when the SmallBlobsQuotaExceeded flag has changed and needs to be persisted and pushed to scheme board.
    */
    bool CheckSmallBlobsQuotas(IQuotaCounters* counters);

    /*
    Rechecks disk space and small blobs quotas in one go.
    Returns true when any flag changed and needs to be persisted and pushed to scheme board.
    */
    bool CheckQuotas(IQuotaCounters* counters);

    ui64 TotalDiskSpaceUsage() {
        return DiskSpaceUsage.Tables.TotalSize + (AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota() ? GetPQAccountStorage() : 0);
    }

    ui64 DiskSpaceQuotasAvailable() {
        auto quotas = GetDiskSpaceQuotas();
        if (!quotas) {
            return Max<ui64>();
        }

        auto usage = TotalDiskSpaceUsage();
        return usage < quotas.HardQuota ? quotas.HardQuota - usage : 0;
    }

    const TStoragePools& GetStoragePools() const {
        return StoragePools ;
    }

    const TStoragePools& EffectiveStoragePools() const {
        if (StoragePools) {
            return StoragePools;
        }
        if (AlterData) {
            return AlterData->StoragePools;
        }
        return StoragePools;
    }

    void AddStoragePool(const TStoragePool& pool) {
        StoragePools.push_back(pool);
    }

    void AddPrivateShard(TShardIdx shardId) {
        PrivateShards.push_back(shardId);
    }

    TVector<TShardIdx> GetPrivateShards() const {
        return PrivateShards;
    }

    void AddInternalShard(TShardIdx shardId, IQuotaCounters* counters, bool isBackup = false) {
        InternalShards.insert(shardId);
        if (isBackup) {
            BackupShards.insert(shardId);
        } else {
            counters->ChangeShardCount(1);
        }
    }

    const THashSet<TShardIdx>& GetInternalShards() const {
        return InternalShards;
    }

    void AddInternalShards(const TTxState& txState, IQuotaCounters* counters, bool isBackup = false) {
        for (auto txShard: txState.Shards) {
            if (txShard.Operation != TTxState::CreateParts) {
                continue;
            }
            AddInternalShard(txShard.Idx, counters, isBackup);
        }
    }

    void RemoveInternalShard(TShardIdx shardIdx, IQuotaCounters* counters) {
        auto it = InternalShards.find(shardIdx);
        Y_ENSURE(it != InternalShards.end(), "shardIdx: " << shardIdx);
        InternalShards.erase(it);

        if (BackupShards.contains(shardIdx)) {
            BackupShards.erase(shardIdx);
        } else {
            counters->ChangeShardCount(-1);
        }
    }

    const THashSet<TShardIdx>& GetSequenceShards() const {
        return SequenceShards;
    }

    void AddSequenceShard(const TShardIdx& shardIdx) {
        SequenceShards.insert(shardIdx);
    }

    void RemoveSequenceShard(const TShardIdx& shardIdx) {
        auto it = SequenceShards.find(shardIdx);
        Y_ENSURE(it != SequenceShards.end(), "shardIdx: " << shardIdx);
        SequenceShards.erase(it);
    }

    const NKikimrSubDomains::TProcessingParams& GetProcessingParams() const {
        return ProcessingParams;
    }

    TTabletId GetCoordinator(TTxId txId) const {
        Y_ENSURE(IsSupportTransactions());
        return TTabletId(CoordinatorSelector->Select(ui64(txId)));
    }

    bool IsSupportTransactions() const {
        return !PrivateShards.empty() || (CoordinatorSelector && !CoordinatorSelector->List().empty());
    }

    void Initialize(const THashMap<TShardIdx, TShardInfo>& allShards) {
        if (InitiatedAsGlobal) {
            return;
        }

        ProcessingParams.ClearCoordinators();
        TVector<TTabletId> coordinators = FilterPrivateTablets(ETabletType::Coordinator, allShards);
        for (TTabletId coordinator: coordinators) {
            ProcessingParams.AddCoordinators(ui64(coordinator));
        }
        CoordinatorSelector = new TCoordinators(ProcessingParams);

        ProcessingParams.ClearMediators();
        TVector<TTabletId> mediators = FilterPrivateTablets(ETabletType::Mediator, allShards);
        for (TTabletId mediator: mediators) {
            ProcessingParams.AddMediators(ui64(mediator));
        }

        ProcessingParams.ClearSchemeShard();
        TVector<TTabletId> schemeshards = FilterPrivateTablets(ETabletType::SchemeShard, allShards);
        Y_ENSURE(schemeshards.size() <= 1, "size was: " << schemeshards.size());
        if (schemeshards.size()) {
            ProcessingParams.SetSchemeShard(ui64(schemeshards.front()));
        }

        ProcessingParams.ClearHive();
        TVector<TTabletId> hives = FilterPrivateTablets(ETabletType::Hive, allShards);
        Y_ENSURE(hives.size() <= 1, "size was: " << hives.size());
        if (hives.size()) {
            ProcessingParams.SetHive(ui64(hives.front()));
            SetSharedHive(InvalidTabletId); // set off shared hive when our own hive has found
        }

        ProcessingParams.ClearSysViewProcessor();
        TVector<TTabletId> sysViewProcessors = FilterPrivateTablets(ETabletType::SysViewProcessor, allShards);
        Y_ENSURE(sysViewProcessors.size() <= 1, "size was: " << sysViewProcessors.size());
        if (sysViewProcessors.size()) {
            ProcessingParams.SetSysViewProcessor(ui64(sysViewProcessors.front()));
        }

        ProcessingParams.ClearStatisticsAggregator();
        TVector<TTabletId> statisticsAggregators = FilterPrivateTablets(ETabletType::StatisticsAggregator, allShards);
        Y_ENSURE(statisticsAggregators.size() <= 1, "size was: " << statisticsAggregators.size());
        if (statisticsAggregators.size()) {
            ProcessingParams.SetStatisticsAggregator(ui64(statisticsAggregators.front()));
        }

        ProcessingParams.ClearGraphShard();
        TVector<TTabletId> graphs = FilterPrivateTablets(ETabletType::GraphShard, allShards);
        Y_ENSURE(graphs.size() <= 1, "size was: " << graphs.size());
        if (graphs.size()) {
            ProcessingParams.SetGraphShard(ui64(graphs.front()));
        }
    }

    void InitializeAsGlobal(NKikimrSubDomains::TProcessingParams&& processingParams) {
        InitiatedAsGlobal = true;

        Y_ENSURE(processingParams.GetPlanResolution());
        Y_ENSURE(processingParams.GetTimeCastBucketsPerMediator());

        ui64 version = ProcessingParams.GetVersion();
        ProcessingParams = std::move(processingParams);
        ProcessingParams.SetVersion(version);

        CoordinatorSelector = new TCoordinators(ProcessingParams);
    }

    void AggrDiskSpaceUsage(IQuotaCounters* counters, const TPartitionStats& newAggr, const TPartitionStats& oldAggr = {});
    void AggrDiskSpaceUsage(IQuotaCounters* counters, const TDiskSpaceUsageDelta& delta);
    void AggrSmallBlobsUsage(IQuotaCounters* counters, i64 bytesDelta, i64 countDelta);

    void AggrDiskSpaceUsage(const TTopicStats& newAggr, const TTopicStats& oldAggr = {});

    const TDiskSpaceUsage& GetDiskSpaceUsage() const {
        return DiskSpaceUsage;
    }

    const TSmallBlobsUsage& GetSmallBlobsUsage() const {
        return SmallBlobsUsage;
    }

    const TMaybe<NKikimrSubDomains::TSchemeQuotas>& GetDeclaredSchemeQuotas() const {
        return DeclaredSchemeQuotas;
    }

    void SetDeclaredSchemeQuotas(const NKikimrSubDomains::TSchemeQuotas& declaredSchemeQuotas) {
        DeclaredSchemeQuotas.ConstructInPlace(declaredSchemeQuotas);
    }

    const TMaybe<Ydb::Cms::DatabaseQuotas>& GetDatabaseQuotas() const {
        return DatabaseQuotas;
    }

    void SetDatabaseQuotas(const Ydb::Cms::DatabaseQuotas& databaseQuotas) {
        DatabaseQuotas.ConstructInPlace(databaseQuotas);
        RecomputeSmallBlobsStorageUnits();
    }

    void SetDatabaseQuotas(const Ydb::Cms::DatabaseQuotas& databaseQuotas, IQuotaCounters* counters) {
        auto prev = GetDiskSpaceQuotas();
        auto prevSmallBlobs = GetSmallBlobsQuotas();
        auto prevs = GetStreamShardsQuota();
        auto prevrs = GetStreamReservedStorageQuota();
        SetDatabaseQuotas(databaseQuotas);
        auto next = GetDiskSpaceQuotas();
        auto nextSmallBlobs = GetSmallBlobsQuotas();
        auto nexts = GetStreamShardsQuota();
        auto nextrs = GetStreamReservedStorageQuota();
        CountDiskSpaceQuotas(counters, prev, next);
        CountSmallBlobsQuotas(counters, prevSmallBlobs, nextSmallBlobs);
        CountStreamShardsQuota(counters, prevs, nexts);
        CountStreamReservedStorageQuota(counters, prevrs, nextrs);
    }

    void ApplyDeclaredSchemeQuotas(const NKikimrSubDomains::TSchemeQuotas& declaredSchemeQuotas, TInstant now) {
        // Check if there was no change in declared quotas
        if (DeclaredSchemeQuotas) {
            TString prev, next;
            Y_ENSURE(DeclaredSchemeQuotas->SerializeToString(&prev));
            Y_ENSURE(declaredSchemeQuotas.SerializeToString(&next));
            if (prev == next) {
                return; // there was no change in quotas
            }
        }

        // Make a local copy of these quotas and regenerate
        DeclaredSchemeQuotas.ConstructInPlace(declaredSchemeQuotas);
        RegenerateSchemeQuotas(now);
    }

    const TSchemeQuotas& GetSchemeQuotas() const {
        return SchemeQuotas;
    }

    void AddSchemeQuota(const TSchemeQuota& quota) {
        SchemeQuotas.emplace_back(quota);
    }

    void AddSchemeQuota(double bucketSize, TDuration bucketDuration, TInstant now) {
        AddSchemeQuota(TSchemeQuota(bucketSize, bucketDuration, now));
    }

    void RemoveSchemeQuotas() {
        SchemeQuotas.LastKnownSize = Max(SchemeQuotas.LastKnownSize, SchemeQuotas.size());
        SchemeQuotas.clear();
    }

    void RegenerateSchemeQuotas(TInstant now) {
        RemoveSchemeQuotas();
        if (DeclaredSchemeQuotas) {
            for (const auto& declaredQuota : DeclaredSchemeQuotas->GetSchemeQuotas()) {
                double bucketSize = declaredQuota.GetBucketSize();
                TDuration bucketDuration = TDuration::Seconds(declaredQuota.GetBucketSeconds());
                SchemeQuotas.emplace_back(bucketSize, bucketDuration, now);
            }
        }
    }

    bool TryConsumeSchemeQuota(TInstant now) {
        bool ok = true;
        for (auto& quota : SchemeQuotas) {
            quota.Update(now);
            ok &= quota.CanPush(1.0);
        }

        if (!ok) {
            return false;
        }

        for (auto& quota : SchemeQuotas) {
            quota.Push(now, 1.0);
        }

        return true;
    }

    ui64 GetDomainStateVersion() const {
        return DomainStateVersion;
    }

    void SetDomainStateVersion(ui64 version) {
        DomainStateVersion = version;
    }

    bool GetDiskQuotaExceeded() const {
        return DiskQuotaExceeded;
    }

    void SetDiskQuotaExceeded(bool value) {
        DiskQuotaExceeded = value;
    }

    bool GetSmallBlobsQuotaExceeded() const {
        return SmallBlobsQuotaExceeded;
    }

    void SetSmallBlobsQuotaExceeded(bool value) {
        SmallBlobsQuotaExceeded = value;
    }

    const NLoginProto::TSecurityState& GetSecurityState() const {
        return SecurityState;
    }

    void UpdateSecurityState(NLoginProto::TSecurityState state) {
        SecurityState = std::move(state);
    }

    ui64 GetSecurityStateVersion() const {
        return SecurityStateVersion;
    }

    void SetSecurityStateVersion(ui64 securityStateVersion) {
        SecurityStateVersion = securityStateVersion;
    }

    void IncSecurityStateVersion() {
        ++SecurityStateVersion;
    }

    using TMaybeAuditSettings = TMaybe<NKikimrSubDomains::TAuditSettings, NMaybe::TPolicyUndefinedFail>;

    void SetAuditSettings(const NKikimrSubDomains::TAuditSettings& value) {
        AuditSettings.ConstructInPlace(value);
    }

    const TMaybeAuditSettings& GetAuditSettings() const {
        return AuditSettings;
    }

    void ApplyAuditSettings(const TMaybeAuditSettings& diff);

    const TMaybeServerlessComputeResourcesMode& GetServerlessComputeResourcesMode() const {
        return ServerlessComputeResourcesMode;
    }

    void SetServerlessComputeResourcesMode(EServerlessComputeResourcesMode serverlessComputeResourcesMode) {
        Y_ENSURE(serverlessComputeResourcesMode, "Can't set ServerlessComputeResourcesMode to unspecified");
        ServerlessComputeResourcesMode = serverlessComputeResourcesMode;
    }

    ETablesMetricsLevel GetTablesMetricsLevel() const {
        return TablesMetricsLevel;
    }

    void SetTablesMetricsLevel(ETablesMetricsLevel level) {
        TablesMetricsLevel = level;
    }

private:
    bool InitiatedAsGlobal = false;
    NKikimrSubDomains::TProcessingParams ProcessingParams;
    TCoordinators::TPtr CoordinatorSelector;
    TMaybe<NKikimrSubDomains::TSchemeQuotas> DeclaredSchemeQuotas;
    TMaybe<Ydb::Cms::DatabaseQuotas> DatabaseQuotas;
    ui64 DomainStateVersion = 0;
    bool DiskQuotaExceeded = false;
    bool SmallBlobsQuotaExceeded = false;
    // Cached (data_size_hard_quota / 10 TiB) factor used to derive the small-blobs quotas
    double SmallBlobsStorageUnits = 0;

    TVector<TShardIdx> PrivateShards;
    TStoragePools StoragePools;
    TPtr AlterData;

    TSchemeLimits SchemeLimits;
    TSchemeQuotas SchemeQuotas;

    ui64 PathsInsideCount = 0;
    ui64 BackupPathsCount = 0;
    ui64 SystemPathsCount = 0;
    TDiskSpaceUsage DiskSpaceUsage;
    TSmallBlobsUsage SmallBlobsUsage;

    void RecomputeSmallBlobsStorageUnits();
    // Returns whether the quota exceeded status was flipped
    bool ApplyQuotaExceededStatus(EQuotaUsageStatus status, bool& exceeded, ESimpleCounters counter, IQuotaCounters* counters);

    THashSet<TShardIdx> InternalShards;
    THashSet<TShardIdx> BackupShards;
    THashSet<TShardIdx> SequenceShards;

    ui64 PQPartitionsInsideCount = 0;
    ui64 PQGroupsInsideCount = 0;
    ui64 PQReservedStorage = 0;

    TPathId ResourcesDomainId;
    TTabletId SharedHive = InvalidTabletId;
    TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;

    NLoginProto::TSecurityState SecurityState;
    ui64 SecurityStateVersion = 0;

    TMaybeAuditSettings AuditSettings;

    ETablesMetricsLevel TablesMetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified;

    TVector<TTabletId> FilterPrivateTablets(TTabletTypes::EType type, const THashMap<TShardIdx, TShardInfo>& allShards) const {
        TVector<TTabletId> tablets;
        for (auto shardId: PrivateShards) {

            if (!allShards.contains(shardId)) {
                // KIKIMR-9849
                // some private shards, which has been migrated, might be deleted
                continue;
            }

            const auto& shard = allShards.at(shardId);
            if (shard.TabletType == type && shard.TabletID != InvalidTabletId) {
                tablets.push_back(shard.TabletID);
            }
        }
        return tablets;
    }
};

} // namespace NSchemeShard
} // namespace NKikimr
