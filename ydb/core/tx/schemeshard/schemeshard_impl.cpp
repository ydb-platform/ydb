#include "schemeshard.h"
#include "schemeshard_impl.h"
#include "schemeshard_svp_migration.h"
#include "olap/bg_tasks/adapter/adapter.h"
#include "olap/bg_tasks/events/global.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/sys_view/partition_stats/partition_stats.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/stat_service.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <util/random/random.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NSchemeShard {

const ui64 NEW_TABLE_ALTER_VERSION = 1;

namespace {

bool ResolvePoolNames(
    const ui32 channelCount,
    const std::function<TStringBuf(ui32)> &channel2poolKind,
    const TStoragePools &storagePools,
    TChannelsBindings &channelsBinding)
{
    TChannelsBindings result;

    for (ui32 channelId = 0; channelId < channelCount; ++channelId) {
        const auto poolKind = channel2poolKind(channelId);

        auto poolIt = std::find_if(
            storagePools.begin(),
            storagePools.end(),
            [=] (const TStoragePools::value_type& pool) {
                return poolKind == pool.GetKind();
            }
        );

        if (poolIt == storagePools.end()) {
            //unable to construct channel binding with the storage pool
            return false;
        }

        result.emplace_back();
        result.back().SetStoragePoolName(poolIt->GetName());
    }

    channelsBinding.swap(result);
    return true;
}

}   // anonymous namespace

const TSchemeLimits TSchemeShard::DefaultLimits = {};

void TSchemeShard::SubscribeToTempTableOwners() {
    auto ctx = ActorContext();
    auto& tempTablesByOwner = TempTablesState.TempTablesByOwner;
    for (const auto& [ownerActorId, tempTables] : tempTablesByOwner) {
        ctx.Send(new IEventHandle(ownerActorId, SelfId(),
                                new TEvSchemeShard::TEvOwnerActorAck(),
                                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession));
    }
}

void TSchemeShard::ActivateAfterInitialization(const TActorContext& ctx, TActivationOpts&& opts) {
    TPathId subDomainPathId = GetCurrentSubDomainPathId();
    TSubDomainInfo::TPtr domainPtr = ResolveDomainInfo(subDomainPathId);
    LoginProvider.Audience = TPath::Init(subDomainPathId, this).PathString();
    domainPtr->UpdateSecurityState(LoginProvider.GetSecurityState());

    TTabletId sysViewProcessorId = domainPtr->GetTenantSysViewProcessorID();
    auto evInit = MakeHolder<NSysView::TEvSysView::TEvInitPartitionStatsCollector>(
        GetDomainKey(subDomainPathId), sysViewProcessorId ? sysViewProcessorId.GetValue() : 0);
    Send(SysPartitionStatsCollector, evInit.Release());

    Execute(CreateTxInitPopulator(std::move(opts.DelayPublications)), ctx);

    if (opts.TablesToClean) {
        Execute(CreateTxCleanTables(std::move(opts.TablesToClean)), ctx);
    }

    if (opts.BlockStoreVolumesToClean) {
        Execute(CreateTxCleanBlockStoreVolumes(std::move(opts.BlockStoreVolumesToClean)), ctx);
    }

    if (IsDomainSchemeShard) {
        InitializeTabletMigrations();
    }

    ResumeExports(opts.ExportIds, ctx);
    ResumeImports(opts.ImportsIds, ctx);
    ResumeCdcStreamScans(opts.CdcStreamScans, ctx);

    ParentDomainLink.SendSync(ctx);

    ScheduleConditionalEraseRun(ctx);
    ScheduleServerlessStorageBilling(ctx);

    Y_ABORT_UNLESS(CleanDroppedPathsDisabled);
    CleanDroppedPathsDisabled = false;
    ScheduleCleanDroppedPaths();
    ScheduleCleanDroppedSubDomains();

    StartStopCompactionQueues();
    BackgroundCleaningQueue->Start();

    ctx.Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(InitiateCachedTxIdsCount));

    InitializeStatistics(ctx);

    SubscribeToTempTableOwners();

    Become(&TThis::StateWork);
}

void TSchemeShard::InitializeTabletMigrations() {
    std::queue<TMigrationInfo> migrations;

    for (auto& [pathId, subdomain] : SubDomains) {
        auto path = TPath::Init(pathId, this);
        if (path->IsRoot()) { // do not migrate main domain
            continue;
        }
        if (subdomain->GetTenantSchemeShardID() == InvalidTabletId) { // no tenant schemeshard
            continue;
        }

        bool createSVP = false;
        bool createSA = false;
        bool createBCT = false;

        if (subdomain->GetTenantSysViewProcessorID() == InvalidTabletId) {
            createSVP = true;
        }

        if (EnableStatistics &&
            !IsServerlessDomain(subdomain) &&
            subdomain->GetTenantStatisticsAggregatorID() == InvalidTabletId)
        {
            createSA = true;
        }

        if (AppData()->FeatureFlags.GetEnableBackupService() && subdomain->GetTenantBackupControllerID() == InvalidTabletId) {
            createBCT = true;
        }

        if (!createSVP && !createSA && !createBCT) {
            continue;
        }

        auto workingDir = path.Parent().PathString();
        auto dbName = path.LeafName();
        TMigrationInfo migration{workingDir, dbName, createSVP, createSA, createBCT};
        migrations.push(std::move(migration));

        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TabletMigrator - creating tablets"
            << ", working dir: " << workingDir
            << ", db name: " << dbName
            << ", create SVP: " << createSVP
            << ", create SA: " << createSA
            << ", create BCT: " << createBCT
            << ", at schemeshard: " << TabletID());
    }

    if (migrations.empty()) {
        return;
    }

    TabletMigrator = Register(CreateTabletMigrator(TabletID(), SelfId(), std::move(migrations)).Release());
}

ui64 TSchemeShard::Generation() const {
    return Executor()->Generation();
}

struct TAttachOrder {
    TVirtualTimestamp DropAt;
    TVirtualTimestamp CreateAt;
    TPathId PathId;

    explicit TAttachOrder(TPathElement::TPtr path)
        : DropAt(path->GetDropTS())
        , CreateAt(path->GetCreateTS())
        , PathId(path->PathId)
    {}

    bool Less (const TAttachOrder& other, EAttachChildResult& decision) const {
        if (DropAt && other.DropAt) {
            // both dropped

            if (DropAt < other.DropAt) {
                decision = EAttachChildResult::AttachedAsNewerDeleted;
                return true;
            } else {
                decision = EAttachChildResult::RejectAsOlderDeleted;
                return false;
            }
        } else if (DropAt || other.DropAt){
            // some one is dropped
            if (DropAt) {
                decision = EAttachChildResult::AttachedAsActual;
                return true;
            } else {
                decision = EAttachChildResult::RejectAsDeleted;
                return false;
            }
        }  else {
            // no one is dropped

            if (CreateAt && other.CreateAt) {
                // both created
                if (CreateAt < other.CreateAt) {
                    decision = EAttachChildResult::AttachedAsNewerActual;
                    return true;
                } else {
                    decision = EAttachChildResult::RejectAsOlderActual;
                    return false;
                }
            } else if (CreateAt || other.CreateAt) {
                // some one is created
                if (CreateAt.Empty()) {
                    decision = EAttachChildResult::AttachedAsCreatedActual;
                    return true;
                } else {
                    decision = EAttachChildResult::RejectAsInactive;
                    return false;
                }
            } else {
                // no one is created
                if (PathId > other.PathId) {
                    decision = EAttachChildResult::AttachedAsOlderUnCreated;
                    return true;
                } else {
                    decision = EAttachChildResult::RejectAsNewerUnCreated;
                    return false;
                }
            }
        }

        Y_UNREACHABLE();
    }

    TString ToString() const {
        return TStringBuilder()
                << "DropAt: " << DropAt.ToString()
                << " CreateAt: " << CreateAt.ToString()
                << " PathId: " << PathId;

    }
};

THolder<TEvDataShard::TEvProposeTransaction> TSchemeShard::MakeDataShardProposal(
        const TPathId& pathId, const TOperationId& opId,
        const TString& body, const TActorContext& ctx) const
{
    return MakeHolder<TEvDataShard::TEvProposeTransaction>(
        NKikimrTxDataShard::TX_KIND_SCHEME, TabletID(), ctx.SelfID,
        ui64(opId.GetTxId()), body, SelectProcessingParams(pathId)
    );
}

TTxId TSchemeShard::GetCachedTxId(const TActorContext &ctx) {
    TTxId txId = InvalidTxId;
    if (CachedTxIds) {
        txId = CachedTxIds.front();
        CachedTxIds.pop_front();
    }

    if (CachedTxIds.size() == InitiateCachedTxIdsCount / 3) {
        ctx.Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(InitiateCachedTxIdsCount));
    }

    return txId;
}

EAttachChildResult TSchemeShard::AttachChild(TPathElement::TPtr child) {
    Y_ABORT_UNLESS(PathsById.contains(child->ParentPathId));
    TPathElement::TPtr parent = PathsById.at(child->ParentPathId);

    if (!parent->GetChildren().contains(child->Name)) {
        parent->AddChild(child->Name, child->PathId, true);

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "AttachChild: child attached as only one child to the parent"
                         << ", parent id: " << parent->PathId
                         << ", parent name: " << parent->Name
                         << ", child name: " << child->Name
                         << ", child id: " << child->PathId
                         << ", at schemeshard: " << TabletID());

        return EAttachChildResult::AttachedAsOnlyOne;
    }

    Y_VERIFY_S(parent->GetChildren().contains(child->Name),
               "child id:" << child->PathId << " child name: " << child->Name <<
               " parent id: " << parent->PathId << " parent name: " << parent->Name);
    Y_VERIFY_S(PathsById.contains(parent->GetChildren().at(child->Name)),
               "child id:" << child->PathId << " child name: " << child->Name <<
               " parent id: " << parent->PathId << " parent name: " << parent->Name);

    TPathElement::TPtr prevChild = PathsById.at(parent->GetChildren().at(child->Name));

    auto decision = EAttachChildResult::Undefined;

    TAttachOrder prevChildOrder = TAttachOrder(prevChild);
    TAttachOrder childOrder = TAttachOrder(child);

    if (prevChildOrder.Less(childOrder, decision)) {
        parent->AddChild(child->Name, child->PathId, true);
    }

    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "AttachChild: decision is: " << decision
                     << ", parent id: " << parent->PathId
                     << ", parent name: " << parent->Name
                     << ", child name: " << child->Name
                     << ", prev child id: " << prevChild->PathId
                     << ", child id: " << child->PathId
                     << ", prev order: " << prevChildOrder.ToString()
                     << ", child order: " << childOrder.ToString()
                     << ", at schemeshard: " << TabletID());

    return decision;
}

bool TSchemeShard::PathIsActive(TPathId pathId) const {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr path = PathsById.at(pathId);

    if (path->PathId == path->ParentPathId) {
        return true;
    }

    Y_ABORT_UNLESS(PathsById.contains(path->ParentPathId));
    TPathElement::TPtr parent = PathsById.at(path->ParentPathId);

    if (parent->IsExternalSubDomainRoot() && parent->PathState == NKikimrSchemeOp::EPathState::EPathStateUpgrade) {
        // Children list is empty during TPublishGlobal stage as part of upgrade subdomain
        return true;
    }

    if (path->Dropped()) {
        return true;
    }

    Y_VERIFY_S(parent->GetChildren().contains(path->Name),
               " pathId: " << pathId
               << " path name: " << path->Name
               << " parentId: " << parent->PathId
               << " parent Name: " << parent->Name);
    return parent->GetChildren().at(path->Name) == pathId;
}

TMessageSeqNo TSchemeShard::StartRound(TTxState &state) {
    if (!state.SchemeOpSeqNo) {
        state.SchemeOpSeqNo = NextRound();
    }
    return state.SchemeOpSeqNo;
}

TMessageSeqNo TSchemeShard::NextRound() {
    TMessageSeqNo s(Generation(), SchemeOpRound);
    ++SchemeOpRound;
    return s;
}

void TSchemeShard::Clear() {
    PathsById.clear();

    Tables.clear();
    TTLEnabledTables.clear();

    Indexes.clear();
    CdcStreams.clear();
    Sequences.clear();
    Replications.clear();
    BlobDepots.clear();

    TablesWithSnapshots.clear();
    SnapshotTables.clear();
    SnapshotsStepIds.clear();

    LockedPaths.clear();

    Topics.clear();
    RtmrVolumes.clear();
    SolomonVolumes.clear();
    SubDomains.clear();
    BlockStoreVolumes.clear();
    FileStoreInfos.clear();
    KesusInfos.clear();
    OlapStores.clear();
    ExternalTables.clear();
    ExternalDataSources.clear();
    Views.clear();

    ColumnTables = { };
    BackgroundSessionsManager = std::make_shared<NKikimr::NOlap::NBackground::TSessionsManager>(
        std::make_shared<NSchemeShard::NBackground::TAdapter>(SelfId(), NKikimr::NOlap::TTabletId(TabletID()), *this));

    RevertedMigrations.clear();

    Operations.clear();
    Publications.clear();
    TxInFlight.clear();

    ShardInfos.clear();
    AdoptedShards.clear();
    TabletIdToShardIdx.clear();

    PartitionMetricsMap.clear();

    if (CompactionQueue) {
        CompactionQueue->Clear();
        UpdateBackgroundCompactionQueueMetrics();
    }

    if (BorrowedCompactionQueue) {
        BorrowedCompactionQueue->Clear();
        UpdateBorrowedCompactionQueueMetrics();
    }

    ClearTempTablesState();

    ShardsWithBorrowed.clear();
    ShardsWithLoaned.clear();

    for(ui32 idx = 0; idx < TabletCounters->Simple().Size(); ++idx) {
        TabletCounters->Simple()[idx].Set(0);
    }
    TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].Clear();
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_SEARCH_HEIGHT].Clear();
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_FULL_COMPACTION].Clear();
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_ROW_DELETES].Clear();
}

void TSchemeShard::IncrementPathDbRefCount(const TPathId& pathId, const TStringBuf& debug) {
    auto it = PathsById.find(pathId);
    Y_VERIFY_DEBUG_S(it != PathsById.end(), "pathId: " << pathId << " debug: " << debug);
    if (it != PathsById.end()) {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD, "IncrementPathDbRefCount reason " << debug << " for pathId " << pathId << " was " << it->second->DbRefCount);
        size_t newRefCount = ++it->second->DbRefCount;
        Y_DEBUG_ABORT_UNLESS(newRefCount > 0);
    }
}

void TSchemeShard::DecrementPathDbRefCount(const TPathId& pathId, const TStringBuf& debug) {
    auto it = PathsById.find(pathId);
    Y_VERIFY_DEBUG_S(it != PathsById.end(), "pathId " << pathId << " " << debug);
    if (it != PathsById.end()) {
        // FIXME: not all references are accounted right now
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD, "DecrementPathDbRefCount reason " << debug << " for pathId " << pathId << " was " << it->second->DbRefCount);
        Y_DEBUG_ABORT_UNLESS(it->second->DbRefCount > 0);
        if (it->second->DbRefCount > 0) {
            size_t newRefCount = --it->second->DbRefCount;
            if (newRefCount == 0 && it->second->Dropped()) {
                CleanDroppedPathsCandidates.insert(pathId);
                ScheduleCleanDroppedPaths();
            } else if (newRefCount == 1 && it->second->AllChildrenCount == 0 && it->second->Dropped()) {
                auto itSubDomain = SubDomains.find(pathId);
                if (itSubDomain != SubDomains.end() && itSubDomain->second->GetInternalShards().empty()) {
                    // We have an empty dropped subdomain, schedule its deletion
                    CleanDroppedSubDomainsCandidates.insert(pathId);
                    ScheduleCleanDroppedSubDomains();
                }
            }
        }
    }
}

bool TSchemeShard::ApplyStorageConfig(
        const TStoragePools &storagePools,
        const NKikimrSchemeOp::TStorageConfig &storageConfig,
        TChannelsBindings &channelsBinding,
        THashMap<TString, ui32> &reverseBinding,
        TStorageRoom &room, TString &errorMsg)
{
    if (channelsBinding && !reverseBinding) {
        // Build a hash map from storage pool name to an existing channel number
        for (ui32 channel = 1; channel < channelsBinding.size(); ++channel) {
            reverseBinding.emplace(channelsBinding[channel].GetStoragePoolName(), channel);
        }
    }

    auto resolve = [] (const TStoragePools& pools,
            const NKikimrSchemeOp::TStorageSettings& settings)
            -> TStoragePools::const_iterator
    {
        const TString& kind = settings.GetPreferredPoolKind();
        auto it = std::find_if(pools.cbegin(), pools.cend(),
                               [&kind] (const TStoragePools::value_type& pool)
        { return kind == pool.GetKind(); });
        if (it != pools.cend()) {
            return it;
        }

        if (settings.GetAllowOtherKinds()) {
            return pools.cbegin();
        }

        return pools.cend();
    };

    auto allocateChannel = [&] (const TString& poolName) -> ui32 {
        auto it = reverseBinding.find(poolName);
        if (it != reverseBinding.end()) {
            return it->second;
        }

        ui32 channel = channelsBinding.size();
        channelsBinding.emplace_back();
        channelsBinding.back().SetStoragePoolName(poolName);
        reverseBinding[poolName] = channel;
        return channel;
    };

#define LOCAL_CHECK(expr, explanation)           \
    if (Y_UNLIKELY(!(expr))) {               \
    errorMsg = explanation;              \
    return false;                        \
}

    if (channelsBinding.size() < 1) {
        LOCAL_CHECK(storageConfig.HasSysLog(), "no syslog storage setting");
        auto sysLogPool = resolve(storagePools, storageConfig.GetSysLog());
        LOCAL_CHECK(sysLogPool != storagePools.end(), "unable determine pool for syslog storage");

        channelsBinding.emplace_back();
        channelsBinding.back().SetStoragePoolName(sysLogPool->GetName());
    }

    if (channelsBinding.size() < 2) {
        LOCAL_CHECK(storageConfig.HasLog(), "no log storage setting");
        auto logPool = resolve(storagePools, storageConfig.GetLog());
        LOCAL_CHECK(logPool != storagePools.end(), "unable determine pool for log storage");

        ui32 channel = allocateChannel(logPool->GetName());
        Y_ABORT_UNLESS(channel == 1, "Expected to allocate log channel, not %" PRIu32, channel);
    }

    if (room.GetId() == 0) {
        // SysLog and Log always occupy the first 2 channels in the primary room
        room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::SysLog, 0);
        room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::Log, 1);
    }

    if (storageConfig.HasData()) {
        auto dataPool = resolve(storagePools, storageConfig.GetData());
        LOCAL_CHECK(dataPool != storagePools.end(), "definition of data storage present but unable determine pool for it");

        ui32 channel = allocateChannel(dataPool->GetName());
        room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::Data, channel);
    }

    if (storageConfig.HasExternal()) {
        auto externalPool = resolve(storagePools, storageConfig.GetExternal());
        LOCAL_CHECK(externalPool != storagePools.end(), "definition of externalPool storage present but unable determine pool for it");

        ui32 channel = allocateChannel(externalPool->GetName());
        room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::External, channel);
    }

#undef LOCAL_CHECK

    return true;
    return true;
}

void TSchemeShard::ClearDescribePathCaches(const TPathElement::TPtr node, bool force) {
    Y_ABORT_UNLESS(node);

    if ((node->Dropped() || !node->IsCreateFinished()) && !force) {
        return;
    }

    node->PreSerializedChildrenListing.clear();

    if (node->PathType == NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup) {
        Y_ABORT_UNLESS(Topics.contains(node->PathId));
        TTopicInfo::TPtr pqGroup = Topics.at(node->PathId);
        pqGroup->PreSerializedPathDescription.clear();
        pqGroup->PreSerializedPartitionsDescription.clear();
    } else if (node->PathType == NKikimrSchemeOp::EPathType::EPathTypeTable) {
        Y_ABORT_UNLESS(Tables.contains(node->PathId));
        TTableInfo::TPtr tabletInfo = Tables.at(node->PathId);
        tabletInfo->PreSerializedPathDescription.clear();
        tabletInfo->PreSerializedPathDescriptionWithoutRangeKey.clear();
    }
}

bool TSchemeShard::IsStorageConfigLogic(const TTableInfo::TCPtr tableInfo) const {
    const NKikimrSchemeOp::TPartitionConfig& partitionConfig = tableInfo->PartitionConfig();
    const NKikimrSchemeOp::TFamilyDescription* pFamily = nullptr;
    for (const auto& family : partitionConfig.GetColumnFamilies()) {
        if (family.GetId() == 0) {
            pFamily = &family;
        }
    }
    return pFamily && pFamily->HasStorageConfig();
}

bool TSchemeShard::GetBindingsRooms(
        const TPathId domainId,
        const NKikimrSchemeOp::TPartitionConfig& partitionConfig,
        TVector<TStorageRoom> &rooms,
        THashMap<ui32, ui32> &familyRooms,
        TChannelsBindings &channelsBinding,
        TString &errorMsg)
{
    Y_ABORT_UNLESS(rooms.size() >= 1);
    Y_ABORT_UNLESS(rooms[0].GetId() == 0);

    // Find the primary column family
    const NKikimrSchemeOp::TFamilyDescription* pFamily = nullptr;
    for (const auto& family : partitionConfig.GetColumnFamilies()) {
        if (family.GetId() == 0) {
            pFamily = &family;
        }
    }
    Y_ABORT_UNLESS(pFamily && pFamily->HasStorageConfig());

    TSubDomainInfo::TPtr domainInfo = SubDomains.at(domainId);
    auto& storagePools = domainInfo->EffectiveStoragePools();

    if (!storagePools) {
        channelsBinding.clear();
        errorMsg = "database doesn't have storage pools at all";
        return false; //it is a new interface, no indulgences
    }

    THashMap<TString, ui32> reverseBinding;

    if (!ApplyStorageConfig(
            storagePools,
            pFamily->GetStorageConfig(),
            channelsBinding,
            reverseBinding,
            rooms[0],
            errorMsg))
    {
        return false;
    }

    // N.B. channel 0 is used for syslog and cannot be used for anything else
    // Here we use channel 0 as a marker for unassigned (default) channel
    ui32 primaryData = rooms[0].GetChannel(NKikimrStorageSettings::TChannelPurpose::Data, 0);
    ui32 primaryExternal = rooms[0].GetChannel(NKikimrStorageSettings::TChannelPurpose::External, 0);

    THashMap<ui64, ui32> roomByChannels;
    for (const auto& family : partitionConfig.GetColumnFamilies()) {
        if (family.GetId() == 0) {
            continue; // already handled
        }
        if (!family.HasStorageConfig()) {
            continue; // will use the default room
        }

        TStorageRoom newRoom(rooms.size());
        if (!ApplyStorageConfig(
                storagePools,
                family.GetStorageConfig(),
                channelsBinding,
                reverseBinding,
                newRoom,
                errorMsg))
        {
            return false;
        }

        ui32 dataChannel = newRoom.GetChannel(NKikimrStorageSettings::TChannelPurpose::Data, primaryData);
        ui32 externalChannel = newRoom.GetChannel(NKikimrStorageSettings::TChannelPurpose::External, primaryExternal);
        if (dataChannel == primaryData && externalChannel == primaryExternal) {
            continue; // will use the default room
        }

        ui64 cookie = (ui64(externalChannel) << 32) | dataChannel;
        auto it = roomByChannels.find(cookie);
        if (it != roomByChannels.end()) {
            familyRooms[family.GetId()] = it->second;
            continue;
        }

        auto& room = rooms.emplace_back(rooms.size());
        if (dataChannel != 0) {
            room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::Data, dataChannel);
        }
        if (externalChannel != 0) {
            room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::External, externalChannel);
        }
        roomByChannels[cookie] = room.GetId();
        familyRooms[family.GetId()] = room.GetId();
    }

    return true;
}

bool TSchemeShard::GetBindingsRoomsChanges(
        const TPathId domainId,
        const TVector<TTableShardInfo>& partitions,
        const NKikimrSchemeOp::TPartitionConfig& partitionConfig,
        TBindingsRoomsChanges& changes,
        TString& errStr)
{
    for (const auto& shard : partitions) {
        const auto& shardInfo = ShardInfos.at(shard.ShardIdx);
        if (!shardInfo.BindedChannels) {
            // Shard has no existing channel bindings, better leave untouched!
            continue;
        }

        auto shardPoolsMapping = GetPoolsMapping(shardInfo.BindedChannels);

        auto& change = changes[shardPoolsMapping];
        if (change.ChannelsBindings) {
            // This change info is already initialized
            continue;
        }

        change.ChannelsBindings = shardInfo.BindedChannels;

        TVector<TStorageRoom> storageRooms;
        THashMap<ui32, ui32> familyRooms;
        storageRooms.emplace_back(0);
        if (!GetBindingsRooms(domainId, partitionConfig, storageRooms, familyRooms, change.ChannelsBindings, errStr)) {
            return false;
        }
        change.ChannelsBindingsUpdated = (GetPoolsMapping(change.ChannelsBindings) != shardPoolsMapping);

        for (const auto& room : storageRooms) {
            change.PerShardConfig.AddStorageRooms()->CopyFrom(room);
        }
        for (const auto& familyRoom : familyRooms) {
            auto* protoFamily = change.PerShardConfig.AddColumnFamilies();
            protoFamily->SetId(familyRoom.first);
            protoFamily->SetRoom(familyRoom.second);
        }
    }

    return true;
}

bool TSchemeShard::GetOlapChannelsBindings(
        const TPathId domainId,
        const NKikimrSchemeOp::TColumnStorageConfig& channelsConfig,
        TChannelsBindings& channelsBindings,
        TString& errStr)
{
    const ui32 dataChannelCount = channelsConfig.GetDataChannelCount();

    if (dataChannelCount < 1) {
        errStr = "Not enough data channels requested";
        channelsBindings.clear();
        return false;
    }

    if (dataChannelCount > 253) {
        errStr = "Too many data channels requested";
        channelsBindings.clear();
        return false;
    }

    TSubDomainInfo::TPtr domainInfo = SubDomains.at(domainId);
    auto& storagePools = domainInfo->EffectiveStoragePools();
    if (!storagePools) {
        errStr = "Database has no configured storage pools";
        channelsBindings.clear();
        return false;
    }

    auto resolveChannel = [&](const NKikimrSchemeOp::TStorageSettings& settings) {
        const TString& preferredKind = settings.GetPreferredPoolKind();
        auto poolIt = std::find_if(
            storagePools.begin(), storagePools.end(),
            [&preferredKind] (const TStoragePools::value_type& pool) {
                return pool.GetKind() == preferredKind;
            });

        if (poolIt == storagePools.end()) {
            if (!settings.GetAllowOtherKinds()) {
                errStr = Sprintf("Database has no storage pool kind '%s'", preferredKind.c_str());
                channelsBindings.clear();
                return false;
            }
            poolIt = storagePools.begin();
        }

        channelsBindings.emplace_back().SetStoragePoolName(poolIt->GetName());
        return true;
    };

    auto resolveDefaultChannel = [&](ui32 channelIndex) {
        Y_ABORT_UNLESS(ChannelProfiles);
        if (ChannelProfiles->Profiles.empty()) {
            errStr = "Database has no default channel profile configured";
            channelsBindings.clear();
            return false;
        }
        const auto& profile = ChannelProfiles->Profiles[0];
        if (channelIndex >= profile.Channels.size()) {
            errStr = Sprintf("Database default channel profile has no channel %" PRIu32 " configured", channelIndex);
            channelsBindings.clear();
            return false;
        }
        const auto& channel = profile.Channels[channelIndex];
        auto poolIt = std::find_if(
            storagePools.begin(), storagePools.end(),
            [&channel] (const TStoragePools::value_type& pool) {
                return channel.PoolKind == pool.GetKind();
            });

        if (poolIt == storagePools.end()) {
            poolIt = storagePools.begin();
        }

        channelsBindings.emplace_back().SetStoragePoolName(poolIt->GetName());
        return true;
    };

    if (channelsConfig.HasSysLog()) {
        resolveChannel(channelsConfig.GetSysLog());
    } else {
        resolveDefaultChannel(0);
    }

    if (channelsConfig.HasLog()) {
        resolveChannel(channelsConfig.GetLog());
    } else {
        resolveDefaultChannel(1);
    }

    if (channelsConfig.HasData()) {
        resolveChannel(channelsConfig.GetData());
    } else {
        resolveDefaultChannel(2);
    }

    // Make all other data channels use the same storage pool
    TString dataPoolName = channelsBindings.back().GetStoragePoolName();
    for (ui32 dataChannel = 1; dataChannel < dataChannelCount; ++dataChannel) {
        channelsBindings.emplace_back().SetStoragePoolName(dataPoolName);
    }

    return true;
}

bool TSchemeShard::IsCompatibleChannelProfileLogic(const TPathId domainId, const TTableInfo::TCPtr tableInfo) const {
    Y_UNUSED(domainId);

    Y_ABORT_UNLESS(!IsStorageConfigLogic(tableInfo));
    if (!ChannelProfiles || !ChannelProfiles->Profiles.size()) {
        return false;
    }

    const NKikimrSchemeOp::TPartitionConfig& partitionConfig = tableInfo->PartitionConfig();
    ui32 profileId = partitionConfig.GetChannelProfileId();
    if (profileId >= ChannelProfiles->Profiles.size()) {
        return false;
    }

    return true;
}

bool TSchemeShard::GetChannelsBindings(const TPathId domainId, const TTableInfo::TCPtr tableInfo, TChannelsBindings &binding, TString &errStr) const {
    Y_ABORT_UNLESS(!IsStorageConfigLogic(tableInfo));
    Y_ABORT_UNLESS(IsCompatibleChannelProfileLogic(domainId, tableInfo));

    const NKikimrSchemeOp::TPartitionConfig& partitionConfig = tableInfo->PartitionConfig();
    ui32 profileId = partitionConfig.GetChannelProfileId();

    TChannelProfiles::TProfile& profile = ChannelProfiles->Profiles.at(profileId);

    TSubDomainInfo::TPtr domainInfo = SubDomains.at(domainId);
    const TStoragePools& storagePools = domainInfo->EffectiveStoragePools();

    if (storagePools.empty()) {
        errStr = "database doesn't have storage pools at all to create tablet channels to storage pool binding by profile id";
        return false;
    }

    if (!TabletResolveChannelsDetails(profileId, profile, storagePools, binding)) {
        errStr = "database doesn't have required storage pools to create tablet channels to storage pool binding by profile id";
        return false;
    }
    return true;
}

bool TSchemeShard::ResolveTabletChannels(ui32 profileId, const TPathId domainId, TChannelsBindings &channelsBinding) const
{
    return ResolveChannelCommon(profileId, domainId, channelsBinding, &TabletResolveChannelsDetails);
}

bool TSchemeShard::ResolveRtmrChannels(const TPathId domainId, TChannelsBindings &channelsBinding) const
{
    const ui32 profileId = 0;
    return ResolveChannelCommon(profileId, domainId, channelsBinding, &ResolveChannelsDetailsAsIs);
}

bool TSchemeShard::ResolveSolomonChannels(const NKikimrSchemeOp::TKeyValueStorageConfig &config, const TPathId domainId, TChannelsBindings& channelsBinding) const
{
    TSubDomainInfo::TPtr domainInfo = SubDomains.at(domainId);
    auto& storagePools = domainInfo->EffectiveStoragePools();

    if (!storagePools) {
        // no storage pool no binding it's Ok
        channelsBinding.clear();
        return false;
    }

    auto getPoolKind = [&] (ui32 channel) {
        return TStringBuf(config.GetChannel(channel).GetPreferredPoolKind());
    };

    return ResolvePoolNames(
        config.ChannelSize(),
        getPoolKind,
        storagePools,
        channelsBinding
    );
}

bool TSchemeShard::ResolveSolomonChannels(ui32 profileId, const TPathId domainId, TChannelsBindings &channelsBinding) const
{
    return ResolveChannelCommon(profileId, domainId, channelsBinding, &ResolveChannelsDetailsAsIs);
}

bool TSchemeShard::ResolvePqChannels(ui32 profileId, const TPathId domainId, TChannelsBindings &channelsBinding) const
{
    return ResolveChannelCommon(profileId, domainId, channelsBinding, &ResolveChannelsDetailsAsIs);
}

bool TSchemeShard::ResolveChannelsByPoolKinds(
    const TVector<TStringBuf> &channelPoolKinds,
    const TPathId domainId,
    TChannelsBindings &channelsBinding) const
{
    TSubDomainInfo::TPtr domainInfo = SubDomains.at(domainId);
    auto& storagePools = domainInfo->EffectiveStoragePools();

    if (!storagePools || !channelPoolKinds) {
        return false;
    }

    if (!ResolvePoolNames(
            channelPoolKinds.size(),
            [&] (ui32 channel) {
                return channelPoolKinds[channel];
            },
            storagePools,
            channelsBinding
        ))
    {
        return false;
    }

    Y_DEBUG_ABORT_UNLESS(!channelsBinding.empty());
    return !channelsBinding.empty();
}

void TSchemeShard::SetNbsChannelsParams(
    const google::protobuf::RepeatedPtrField<NKikimrBlockStore::TChannelProfile>& ecps,
    TChannelsBindings& channelsBinding)
{
    Y_ABORT_UNLESS(channelsBinding.ysize() == ecps.size());

    for (int i = 0; i < ecps.size(); ++i) {
        channelsBinding[i].SetSize(ecps[i].GetSize());
        // XXX IOPS and Throughput should be split into read/write IOPS
        // and read/write Throughput in Hive
        channelsBinding[i].SetThroughput(
            ecps[i].GetReadBandwidth() + ecps[i].GetWriteBandwidth()
        );
        channelsBinding[i].SetIOPS(
            ecps[i].GetReadIops() + ecps[i].GetWriteIops()
        );
    }
}

void TSchemeShard::SetNfsChannelsParams(
    const google::protobuf::RepeatedPtrField<NKikimrFileStore::TChannelProfile>& ecps,
    TChannelsBindings& channelsBinding)
{
    Y_ABORT_UNLESS(channelsBinding.ysize() == ecps.size());

    for (int i = 0; i < ecps.size(); ++i) {
        channelsBinding[i].SetSize(ecps[i].GetSize());
        // XXX IOPS and Throughput should be split into read/write IOPS
        // and read/write Throughput in Hive
        channelsBinding[i].SetThroughput(
            ecps[i].GetReadBandwidth() + ecps[i].GetWriteBandwidth()
        );
        channelsBinding[i].SetIOPS(
            ecps[i].GetReadIops() + ecps[i].GetWriteIops()
        );
    }
}

void TSchemeShard::SetPqChannelsParams(
    const google::protobuf::RepeatedPtrField<NKikimrPQ::TChannelProfile>& ecps,
    TChannelsBindings& channelsBinding)
{
    Y_ABORT_UNLESS(channelsBinding.ysize() == ecps.size());

    for (int i = 0; i < ecps.size(); ++i) {
        channelsBinding[i].SetSize(ecps[i].GetSize());
        // XXX IOPS and Throughput should be split into read/write IOPS
        // and read/write Throughput in Hive
        channelsBinding[i].SetThroughput(
            ecps[i].GetReadBandwidth() + ecps[i].GetWriteBandwidth()
        );
        channelsBinding[i].SetIOPS(
            ecps[i].GetReadIops() + ecps[i].GetWriteIops()
        );
    }
}

bool TSchemeShard::ResolveSubdomainsChannels(const TStoragePools &storagePools, TChannelsBindings &channelsBinding) {
    if (!ChannelProfiles) {
        channelsBinding.clear();
        return false;
    }
    if (ChannelProfiles->Profiles.empty()) {
        channelsBinding.clear();
        return false;
    }
    if (!storagePools) {
        channelsBinding.clear();
        return false;
    }
    return TabletResolveChannelsDetails(0, ChannelProfiles->Profiles[0], storagePools, channelsBinding);
}

bool TSchemeShard::ResolveChannelCommon(ui32 profileId, const TPathId domainId, TChannelsBindings &channelsBinding, std::function<bool (ui32, const TChannelProfiles::TProfile &, const TStoragePools &, TChannelsBindings &)> resolveDetails) const
{
    Y_ABORT_UNLESS(ChannelProfiles);
    if (ChannelProfiles->Profiles.size() == 0) {
        return false; //there is some tests without channels.txt still
    }
    if(profileId >= ChannelProfiles->Profiles.size()) {
        return false;
    }
    const auto& profile = ChannelProfiles->Profiles[profileId];
    Y_ABORT_UNLESS(profile.Channels.size() > 0);

    TSubDomainInfo::TPtr domainInfo = SubDomains.at(domainId);
    auto& storagePools = domainInfo->EffectiveStoragePools();

    if (!storagePools) {
        // no storage pool no binding it's Ok
        channelsBinding.clear();
        return false;
    }

    return resolveDetails(profileId, profile, storagePools, channelsBinding);
}

bool TSchemeShard::ResolveChannelsDetailsAsIs(
    ui32,
    const TChannelProfiles::TProfile &profile,
    const TStoragePools &storagePools,
    TChannelsBindings &channelsBinding)
{
    return ResolvePoolNames(
        profile.Channels.size(),
        [&] (ui32 channel) {
            return TStringBuf(profile.Channels[channel].PoolKind);
        },
        storagePools,
        channelsBinding
    );
}

bool TSchemeShard::TabletResolveChannelsDetails(ui32 profileId, const TChannelProfiles::TProfile &profile, const TStoragePools &storagePools, TChannelsBindings &channelsBinding)
{
    const bool isDefaultProfile = (0 == profileId);

    TChannelsBindings result;
    std::set<TString> uniqPoolsNames;

    for (ui32 channelId = 0; channelId < profile.Channels.size(); ++channelId) {
        const TChannelProfiles::TProfile::TChannel& channel = profile.Channels[channelId];

        auto poolIt = std::find_if(storagePools.begin(), storagePools.end(), [&channel] (const TStoragePools::value_type& pool) {
            return channel.PoolKind == pool.GetKind();
        });

        // substitute missing pool(s) for the default profile (but not for other profiles)
        if (poolIt == storagePools.end()) {
            if (isDefaultProfile) {
                poolIt = storagePools.begin();
            } else {
                // unable to construct channel binding with the storage pool
                return false;
            }
        }

        // sys log channel is 0, log channel is 1 (always)
        if (0 == channelId || 1 == channelId) {
            result.emplace_back();
            result.back().SetStoragePoolName(poolIt->GetName());
            continue;
        }

        // by the way, channel 1 might be shared with data and ext in a new StorageConfig interface
        // but as we already provide variable like ColumnStorage1Ext2 for the clients
        // we do not want to break compatibility and so, until StorageConfig is mainstream,
        // we should always return at least 3 channels
        if (uniqPoolsNames.insert(poolIt->GetName()).second) {
            result.emplace_back();
            result.back().SetStoragePoolName(poolIt->GetName());
        }
    }

    channelsBinding.swap(result);
    return true;
}

TPathId TSchemeShard::ResolvePathIdForDomain(TPathId pathId) const {
    Y_ABORT_UNLESS(pathId != InvalidPathId);
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    return ResolvePathIdForDomain(PathsById.at(pathId));
}

TPathId TSchemeShard::ResolvePathIdForDomain(TPathElement::TPtr pathEl) const {
    TPathId domainId = pathEl->IsDomainRoot()
            ? pathEl->PathId
            : pathEl->DomainPathId;
    Y_ABORT_UNLESS(PathsById.contains(domainId));
    return domainId;
}

TSubDomainInfo::TPtr TSchemeShard::ResolveDomainInfo(TPathId pathId) const {
    Y_ABORT_UNLESS(pathId != InvalidPathId);
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    return ResolveDomainInfo(PathsById.at(pathId));
}

TSubDomainInfo::TPtr TSchemeShard::ResolveDomainInfo(TPathElement::TPtr pathEl) const {
    TPathId domainId = ResolvePathIdForDomain(pathEl);
    Y_ABORT_UNLESS(SubDomains.contains(domainId));
    auto info = SubDomains.at(domainId);
    Y_ABORT_UNLESS(info);
    return info;
}

TPathId TSchemeShard::GetDomainKey(TPathElement::TPtr pathEl) const {
    auto pathIdForDomain = ResolvePathIdForDomain(pathEl);
    TPathElement::TPtr domainPathElement = PathsById.at(pathIdForDomain);
    Y_ABORT_UNLESS(domainPathElement);
    return domainPathElement->IsRoot() ? ParentDomainId : pathIdForDomain;
}

TPathId TSchemeShard::GetDomainKey(TPathId pathId) const {
    Y_ABORT_UNLESS(pathId != InvalidPathId);
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    return GetDomainKey(PathsById.at(pathId));
}

const NKikimrSubDomains::TProcessingParams &TSchemeShard::SelectProcessingParams(TPathId id) const {
    TPathElement::TPtr item = PathsById.at(id);
    return SelectProcessingParams(item);
}

const NKikimrSubDomains::TProcessingParams &TSchemeShard::SelectProcessingParams(TPathElement::TPtr pathEl) const {
    Y_ABORT_UNLESS(pathEl.Get());

    auto subDomainInfo = ResolveDomainInfo(pathEl);
    if (subDomainInfo->IsSupportTransactions()) {
        return subDomainInfo->GetProcessingParams();
    }

    auto rootDomain = SubDomains.at(RootPathId());
    Y_ABORT_UNLESS(rootDomain->IsSupportTransactions());
    return rootDomain->GetProcessingParams();
}

TTabletId TSchemeShard::SelectCoordinator(TTxId txId, TPathId pathId) const {
    Y_ABORT_UNLESS(pathId != InvalidPathId);
    return SelectCoordinator(txId, PathsById.at(pathId));
}

TTabletId TSchemeShard::SelectCoordinator(TTxId txId, TPathElement::TPtr pathEl) const {
    Y_ABORT_UNLESS(pathEl.Get());

    auto subDomainInfo = ResolveDomainInfo(pathEl->ParentPathId); //for subdomain node we must use parent domain
    if (subDomainInfo->IsSupportTransactions()) {
        return subDomainInfo->GetCoordinator(txId);
    }

    auto rootDomain = SubDomains.at(RootPathId());
    Y_ABORT_UNLESS(rootDomain->IsSupportTransactions());
    return rootDomain->GetCoordinator(txId);
}

TString TSchemeShard::PathToString(TPathElement::TPtr item) {
    Y_ABORT_UNLESS(item);
    TPath path = TPath::Init(item->PathId, this);
    return path.PathString();
}

bool TSchemeShard::CheckApplyIf(const NKikimrSchemeOp::TModifyScheme &scheme, TString &errStr) {
    const auto& conditions = scheme.GetApplyIf();

    for(const auto& item: conditions) {
        if (!item.HasPathId()) {
            continue;
        }
        TLocalPathId localPathId = item.GetPathId();
        const auto pathId = TPathId(TabletID(), localPathId);

        if (!PathsById.contains(pathId)) {
            errStr = TStringBuilder()
                << "fail user constraint: ApplyIf section:"
                << " no path with id " << pathId;
            return false;
        }
        const TPathElement::TPtr pathEl = PathsById.at(pathId);

        if (pathEl->Dropped()) {
            errStr = TStringBuilder()
                << "fail user constraint: ApplyIf section:"
                << " path with id " << pathId << " has been dropped";
            return false;
        }

        if (item.HasPathVersion()) {
            const auto requiredVersion = item.GetPathVersion();
            arc_ui64 actualVersion;
            auto path = TPath::Init(pathId, this);
            auto pathVersion = GetPathVersion(path);

            if (item.HasCheckEntityVersion() && item.GetCheckEntityVersion()) {
                switch(path.Base()->PathType) {
                    case NKikimrSchemeOp::EPathTypePersQueueGroup:
                        actualVersion = pathVersion.GetPQVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeSubDomain:
                    case NKikimrSchemeOp::EPathType::EPathTypeExtSubDomain:
                        actualVersion = pathVersion.GetSubDomainVersion();
                        break;
                    case NKikimrSchemeOp::EPathTypeTable:
                        actualVersion = pathVersion.GetTableSchemaVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeBlockStoreVolume:
                        actualVersion = pathVersion.GetBSVVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeFileStore:
                        actualVersion = pathVersion.GetFileStoreVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeKesus:
                        actualVersion = pathVersion.GetKesusVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeRtmrVolume:
                        actualVersion = pathVersion.GetRTMRVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeSolomonVolume:
                        actualVersion = pathVersion.GetSolomonVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeTableIndex:
                        actualVersion = pathVersion.GetTableIndexVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeColumnStore:
                        actualVersion = pathVersion.GetColumnStoreVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeColumnTable:
                        actualVersion = pathVersion.GetColumnTableVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeCdcStream:
                        actualVersion = pathVersion.GetCdcStreamVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeSequence:
                        actualVersion = pathVersion.GetSequenceVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeReplication:
                        actualVersion = pathVersion.GetReplicationVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeExternalTable:
                        actualVersion = pathVersion.GetExternalTableVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeExternalDataSource:
                        actualVersion = pathVersion.GetExternalDataSourceVersion();
                        break;
                    case NKikimrSchemeOp::EPathType::EPathTypeView:
                        actualVersion = pathVersion.GetViewVersion();
                        break;
                    default:
                        actualVersion = pathVersion.GetGeneralVersion();
                        break;
                }
            } else {
                actualVersion = pathVersion.GetGeneralVersion();
            }

            if (requiredVersion != actualVersion) {
                errStr = TStringBuilder()
                    << "fail user constraint in ApplyIf section:"
                    //FIXME: revert to misspelled text as there is dependency on it in the nbs code.
                    // Dependency on text should be replaced by introducing special error code.
                    << " path version mistmach, path with id " << pathEl->PathId
                    << " has actual version " << actualVersion
                    << " but version " << requiredVersion << " was required";
                return false;
            }
        }

        if (item.HasLockedTxId()) {
            const auto lockOwnerTxId = TTxId(item.GetLockedTxId());

            TString lockErr = "fail user constraint in ApplyIf section:";
            if (!CheckLocks(pathId, lockOwnerTxId, lockErr)) {
                errStr = lockErr;
                return false;
            }
        }
    }

    return true;
}

bool TSchemeShard::CheckLocks(const TPathId pathId, const NKikimrSchemeOp::TModifyScheme &scheme, TString &errStr) const {
    if (scheme.HasLockGuard() && scheme.GetLockGuard().HasOwnerTxId()) {
        return CheckLocks(pathId, TTxId(scheme.GetLockGuard().GetOwnerTxId()), errStr);
    }

    return CheckLocks(pathId, InvalidTxId, errStr);
}

bool TSchemeShard::CheckLocks(const TPathId pathId, const TTxId lockTxId, TString& errStr) const {
    if (lockTxId == InvalidTxId) {
        // check lock is free
        if (LockedPaths.contains(pathId)) {
            auto explain = TStringBuilder()
                << "path '" << pathId << "'"
                << " has been locked by tx: " << LockedPaths.at(pathId);
            errStr.append(explain);
            return false;
        }

        return true;
    }

    // check lock is correct

    if (!LockedPaths.contains(pathId)) {
        auto explain = TStringBuilder()
            << "path '" << pathId << "'"
            << " hasn't been locked at all"
            << " but it is declared that it should be locked by tx: " << lockTxId;
        errStr.append(explain);
        return false;
    }

    if (LockedPaths.at(pathId) != lockTxId) {
        auto explain = TStringBuilder()
            << "path '" << pathId << "'"
            << " has been locked by tx: " << LockedPaths.at(pathId)
            << " but it is declared that it should be locked by: " << lockTxId;
        errStr.append(explain);
        return false;
    }

    return true;
}

bool TSchemeShard::CheckInFlightLimit(TTxState::ETxType txType, TString& errStr) const {
    auto it = InFlightLimits.find(txType);
    if (it == InFlightLimits.end()) {
        return true;
    }

    if (it->second != 0 && TabletCounters->Simple()[TTxState::TxTypeInFlightCounter(txType)].Get() >= it->second) {
        errStr = TStringBuilder() << "the limit of operations with type " << TTxState::TypeName(txType)
            << " has been exceeded"
            << ", limit: " << it->second;
        return false;
    }

    return true;
}

bool TSchemeShard::CheckInFlightLimit(NKikimrSchemeOp::EOperationType opType, TString& errStr) const {
    if (const auto txType = TTxState::ConvertToTxType(opType); txType != TTxState::TxInvalid) {
        return CheckInFlightLimit(txType, errStr);
    }

    return true;
}

bool TSchemeShard::CanCreateSnapshot(const TPathId& tablePathId, TTxId txId, NKikimrScheme::EStatus& status, TString& errStr) const {
    auto it = TablesWithSnapshots.find(tablePathId);
    if (it == TablesWithSnapshots.end()) {
        return true;
    }

    const auto& snapshotTxId = it->second;
    TStepId snapshotStepId;

    if (auto sit = SnapshotsStepIds.find(snapshotTxId); sit != SnapshotsStepIds.end()) {
        snapshotStepId = sit->second;
    }

    if (txId == snapshotTxId) {
        status = NKikimrScheme::StatusAlreadyExists;
        errStr = TStringBuilder()
            << "Snapshot with the same txId already presents for table"
            << ", tableId:" << tablePathId
            << ", txId: " << txId
            << ", snapshotTxId: " << snapshotTxId
            << ", snapshotStepId: " << snapshotStepId;
    } else {
        status = NKikimrScheme::StatusSchemeError;
        errStr = TStringBuilder()
            << "Snapshot with another txId already presents for table, only one snapshot is allowed for table for now"
            << ", tableId:" << tablePathId
            << ", txId: " << txId
            << ", snapshotTxId: " << snapshotTxId
            << ", snapshotStepId: " << snapshotStepId;
    }

    return false;
}

TShardIdx TSchemeShard::ReserveShardIdxs(ui64 count) {
    auto idx = TLocalShardIdx(NextLocalShardIdx);
    NextLocalShardIdx += count;
    return MakeLocalId(idx);
}

TShardIdx TSchemeShard::NextShardIdx(const TShardIdx& shardIdx, ui64 inc) const {
    Y_ABORT_UNLESS(shardIdx.GetOwnerId() == TabletID());

    ui64 nextLocalId = ui64(shardIdx.GetLocalId()) + inc;
    Y_VERIFY_S(nextLocalId < NextLocalShardIdx, "what: nextLocalId: " << nextLocalId << " NextLocalShardIdx: " << NextLocalShardIdx);

    return MakeLocalId(TLocalShardIdx(nextLocalId));
}

const TTableInfo* TSchemeShard::GetMainTableForIndex(TPathId indexTableId) const {
    if (!Tables.contains(indexTableId))
        return nullptr;

    auto pathEl = PathsById.FindPtr(indexTableId);
    if (!pathEl)
        return nullptr;

    TPathId parentId = (*pathEl)->ParentPathId;
    auto parentEl = PathsById.FindPtr(parentId);

    if (!parentEl || !(*parentEl)->IsTableIndex())
        return nullptr;

    TPathId grandParentId = (*parentEl)->ParentPathId;

    if (!Tables.contains(grandParentId))
        return nullptr;

    return Tables.FindPtr(grandParentId)->Get();
}

bool TSchemeShard::IsBackupTable(TPathId pathId) const {
    auto it = Tables.find(pathId);
    if (it == Tables.end()) {
        return false;
    }

    Y_ABORT_UNLESS(it->second);
    return it->second->IsBackup;
}

TPathElement::EPathState TSchemeShard::CalcPathState(TTxState::ETxType txType, TPathElement::EPathState oldState) {
    // Do not change state if PathId is dropped. It can't become alive.
    switch (oldState) {
    case TPathElement::EPathState::EPathStateNotExist:
    case TPathElement::EPathState::EPathStateDrop: // there could be multiple TXs, preserve StateDrop
        return oldState;
    case TPathElement::EPathState::EPathStateUpgrade:
        return oldState;
    default:
        break;
    }

    switch (txType) {
    case TTxState::TxMkDir:
    case TTxState::TxCreateTable:
    case TTxState::TxCopyTable:
    case TTxState::TxCreatePQGroup:
    case TTxState::TxAllocatePQ:
    case TTxState::TxCreateSubDomain:
    case TTxState::TxCreateExtSubDomain:
    case TTxState::TxCreateBlockStoreVolume:
    case TTxState::TxCreateFileStore:
    case TTxState::TxCreateKesus:
    case TTxState::TxCreateSolomonVolume:
    case TTxState::TxCreateRtmrVolume:
    case TTxState::TxCreateTableIndex:
    case TTxState::TxCreateOlapStore:
    case TTxState::TxCreateColumnTable:
    case TTxState::TxCreateCdcStream:
    case TTxState::TxCreateSequence:
    case TTxState::TxCopySequence:
    case TTxState::TxCreateReplication:
    case TTxState::TxCreateBlobDepot:
    case TTxState::TxCreateExternalTable:
    case TTxState::TxCreateExternalDataSource:
    case TTxState::TxCreateView:
    case TTxState::TxCreateContinuousBackup:
        return TPathElement::EPathState::EPathStateCreate;
    case TTxState::TxAlterPQGroup:
    case TTxState::TxAlterTable:
    case TTxState::TxAlterBlockStoreVolume:
    case TTxState::TxAlterFileStore:
    case TTxState::TxAlterKesus:
    case TTxState::TxAlterSubDomain:
    case TTxState::TxAlterExtSubDomain:
    case TTxState::TxAlterExtSubDomainCreateHive:
    case TTxState::TxAlterUserAttributes:
    case TTxState::TxInitializeBuildIndex:
    case TTxState::TxFinalizeBuildIndex:
    case TTxState::TxCreateLock:
    case TTxState::TxDropLock:
    case TTxState::TxAlterTableIndex:
    case TTxState::TxAlterSolomonVolume:
    case TTxState::TxDropTableIndexAtMainTable:
    case TTxState::TxAlterOlapStore:
    case TTxState::TxAlterColumnTable:
    case TTxState::TxAlterCdcStream:
    case TTxState::TxAlterCdcStreamAtTable:
    case TTxState::TxAlterCdcStreamAtTableDropSnapshot:
    case TTxState::TxCreateCdcStreamAtTable:
    case TTxState::TxCreateCdcStreamAtTableWithInitialScan:
    case TTxState::TxDropCdcStreamAtTable:
    case TTxState::TxDropCdcStreamAtTableDropSnapshot:
    case TTxState::TxAlterSequence:
    case TTxState::TxAlterReplication:
    case TTxState::TxAlterBlobDepot:
    case TTxState::TxUpdateMainTableOnIndexMove:
    case TTxState::TxAlterExternalTable:
    case TTxState::TxAlterExternalDataSource:
    case TTxState::TxAlterView:
    case TTxState::TxAlterContinuousBackup:
        return TPathElement::EPathState::EPathStateAlter;
    case TTxState::TxDropTable:
    case TTxState::TxDropPQGroup:
    case TTxState::TxRmDir:
    case TTxState::TxDropSubDomain:
    case TTxState::TxForceDropSubDomain:
    case TTxState::TxForceDropExtSubDomain:
    case TTxState::TxDropBlockStoreVolume:
    case TTxState::TxDropFileStore:
    case TTxState::TxDropKesus:
    case TTxState::TxDropSolomonVolume:
    case TTxState::TxDropTableIndex:
    case TTxState::TxDropOlapStore:
    case TTxState::TxDropColumnTable:
    case TTxState::TxDropCdcStream:
    case TTxState::TxDropSequence:
    case TTxState::TxDropReplication:
    case TTxState::TxDropReplicationCascade:
    case TTxState::TxDropBlobDepot:
    case TTxState::TxDropExternalTable:
    case TTxState::TxDropExternalDataSource:
    case TTxState::TxDropView:
    case TTxState::TxDropContinuousBackup:
        return TPathElement::EPathState::EPathStateDrop;
    case TTxState::TxBackup:
        return TPathElement::EPathState::EPathStateBackup;
    case TTxState::TxRestore:
        return TPathElement::EPathState::EPathStateRestore;
    case TTxState::TxUpgradeSubDomain:
        return TPathElement::EPathState::EPathStateUpgrade;
    case TTxState::TxUpgradeSubDomainDecision:
        return TPathElement::EPathState::EPathStateAlter; // if only TxUpgradeSubDomainDecision hangs under path it is considered just as Alter
    case TTxState::TxSplitTablePartition:
    case TTxState::TxMergeTablePartition:
        break;
    case TTxState::TxFillIndex:
        Y_ABORT("deprecated");
    case TTxState::TxModifyACL:
    case TTxState::TxInvalid:
    case TTxState::TxAssignBlockStoreVolume:
        Y_UNREACHABLE();
    case TTxState::TxMoveTable:
    case TTxState::TxMoveTableIndex:
        return TPathElement::EPathState::EPathStateCreate;
    }
    return oldState;
}

bool TSchemeShard::TRwTxBase::Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) {
    THPTimer cpuTimer;

    // Transactions don't read anything from the DB, they all use in-mem structures and do writes to the DB
    // That's why transactions should never be retried
    txc.DB.NoMoreReadsForTx();

    try {
        DoExecute(txc, ctx);
    } catch (const std::exception& ex) {
        Y_FAIL_S("there must be no leaked exceptions: " << ex.what() << ", at schemeshard: " << Self->TabletID());
    } catch (...) {
        Y_FAIL_S("there must be no leaked exceptions, at schemeshard: " << Self->TabletID());
    }

    ExecuteDuration = TDuration::Seconds(cpuTimer.Passed());
    return true;
}

void TSchemeShard::TRwTxBase::Complete(const TActorContext &ctx) {
    DoComplete(ctx);
}

void TSchemeShard::BumpIncompatibleChanges(NIceDb::TNiceDb& db, ui64 incompatibleChange) {
    if (MaxIncompatibleChange < incompatibleChange) {
        Y_VERIFY_S(incompatibleChange <= Schema::MaxIncompatibleChangeSupported,
            "Attempting to bump incompatible changes to " << incompatibleChange <<
            ", but maximum supported change is " << Schema::MaxIncompatibleChangeSupported);
        // We add a special path on the first incompatible change, which breaks
        // all versions that don't know about incompatible changes. Newer
        // versions will just skip this non-sensible entry.
        if (MaxIncompatibleChange == 0) {
            db.Table<Schema::Paths>().Key(0).Update(
                NIceDb::TUpdate<Schema::Paths::ParentId>(0),
                NIceDb::TUpdate<Schema::Paths::Name>("/incompatible/"));
        }
        // Persist a new maximum incompatible change, this will cause older
        // versions to stop gracefully instead of working inconsistently.
        db.Table<Schema::SysParams>().Key(Schema::SysParam_MaxIncompatibleChange).Update(
            NIceDb::TUpdate<Schema::SysParams::Value>(ToString(incompatibleChange)));
        MaxIncompatibleChange = incompatibleChange;
    }
}

void TSchemeShard::PersistTableIndex(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr element = PathsById.at(pathId);

    Y_ABORT_UNLESS(Indexes.contains(pathId));
    TTableIndexInfo::TPtr index = Indexes.at(pathId);

    Y_ABORT_UNLESS(IsLocalId(element->PathId));
    Y_ABORT_UNLESS(element->IsTableIndex());

    TTableIndexInfo::TPtr alterData = index->AlterData;
    Y_ABORT_UNLESS(alterData);
    Y_ABORT_UNLESS(index->AlterVersion < alterData->AlterVersion);

    db.Table<Schema::TableIndex>().Key(element->PathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::TableIndex::AlterVersion>(alterData->AlterVersion),
                NIceDb::TUpdate<Schema::TableIndex::IndexType>(alterData->Type),
                NIceDb::TUpdate<Schema::TableIndex::State>(alterData->State));

    db.Table<Schema::TableIndexAlterData>().Key(element->PathId.LocalPathId).Delete();

    for (ui32 keyIdx = 0; keyIdx < alterData->IndexKeys.size(); ++keyIdx) {
        db.Table<Schema::TableIndexKeys>().Key(element->PathId.LocalPathId, keyIdx).Update(
                    NIceDb::TUpdate<Schema::TableIndexKeys::KeyName>(alterData->IndexKeys[keyIdx]));

        db.Table<Schema::TableIndexKeysAlterData>().Key(element->PathId.LocalPathId, keyIdx).Delete();
    }

    for (ui32 dataColIdx = 0; dataColIdx < alterData->IndexDataColumns.size(); ++dataColIdx) {
        db.Table<Schema::TableIndexDataColumns>().Key(element->PathId.OwnerId, element->PathId.LocalPathId, dataColIdx).Update(
                    NIceDb::TUpdate<Schema::TableIndexDataColumns::DataColumnName>(alterData->IndexDataColumns[dataColIdx]));

        db.Table<Schema::TableIndexDataColumnsAlterData>().Key(element->PathId.OwnerId, element->PathId.LocalPathId, dataColIdx).Delete();
    }
}

void TSchemeShard::PersistTableIndexAlterData(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr elem = PathsById.at(pathId);

    Y_ABORT_UNLESS(Indexes.contains(pathId));
    TTableIndexInfo::TPtr index = Indexes.at(pathId);

    Y_ABORT_UNLESS(IsLocalId(pathId));
    Y_ABORT_UNLESS(elem->IsTableIndex());

    TTableIndexInfo::TPtr alterData = index->AlterData;
    Y_ABORT_UNLESS(alterData);

    db.Table<Schema::TableIndexAlterData>().Key(elem->PathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::TableIndexAlterData::AlterVersion>(alterData->AlterVersion),
                NIceDb::TUpdate<Schema::TableIndexAlterData::IndexType>(alterData->Type),
                NIceDb::TUpdate<Schema::TableIndexAlterData::State>(alterData->State));

    for (ui32 keyIdx = 0; keyIdx < alterData->IndexKeys.size(); ++keyIdx) {
        db.Table<Schema::TableIndexKeysAlterData>().Key(elem->PathId.LocalPathId, keyIdx).Update(
                    NIceDb::TUpdate<Schema::TableIndexKeysAlterData::KeyName>(alterData->IndexKeys[keyIdx]));
    }

    for (ui32 dataColIdx = 0; dataColIdx < alterData->IndexDataColumns.size(); ++dataColIdx) {
        db.Table<Schema::TableIndexDataColumnsAlterData>().Key(elem->PathId.OwnerId, elem->PathId.LocalPathId, dataColIdx).Update(
                    NIceDb::TUpdate<Schema::TableIndexDataColumnsAlterData::DataColumnName>(alterData->IndexDataColumns[dataColIdx]));
    }
}

void TSchemeShard::PersistCdcStream(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    auto path = PathsById.at(pathId);

    Y_ABORT_UNLESS(CdcStreams.contains(pathId));
    auto stream = CdcStreams.at(pathId);

    Y_ABORT_UNLESS(IsLocalId(pathId));
    Y_ABORT_UNLESS(path->IsCdcStream());

    auto alterData = stream->AlterData;
    Y_ABORT_UNLESS(alterData);
    Y_ABORT_UNLESS(stream->AlterVersion < alterData->AlterVersion);

    db.Table<Schema::CdcStream>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::CdcStream::AlterVersion>(alterData->AlterVersion),
        NIceDb::TUpdate<Schema::CdcStream::Mode>(alterData->Mode),
        NIceDb::TUpdate<Schema::CdcStream::Format>(alterData->Format),
        NIceDb::TUpdate<Schema::CdcStream::VirtualTimestamps>(alterData->VirtualTimestamps),
        NIceDb::TUpdate<Schema::CdcStream::ResolvedTimestampsIntervalMs>(alterData->ResolvedTimestamps.MilliSeconds()),
        NIceDb::TUpdate<Schema::CdcStream::AwsRegion>(alterData->AwsRegion),
        NIceDb::TUpdate<Schema::CdcStream::State>(alterData->State)
    );

    db.Table<Schema::CdcStreamAlterData>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistCdcStreamAlterData(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    auto path = PathsById.at(pathId);

    Y_ABORT_UNLESS(CdcStreams.contains(pathId));
    auto stream = CdcStreams.at(pathId);

    Y_ABORT_UNLESS(IsLocalId(pathId));
    Y_ABORT_UNLESS(path->IsCdcStream());

    auto alterData = stream->AlterData;
    Y_ABORT_UNLESS(alterData);

    db.Table<Schema::CdcStreamAlterData>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::CdcStreamAlterData::AlterVersion>(alterData->AlterVersion),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::Mode>(alterData->Mode),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::Format>(alterData->Format),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::VirtualTimestamps>(alterData->VirtualTimestamps),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::ResolvedTimestampsIntervalMs>(alterData->ResolvedTimestamps.MilliSeconds()),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::AwsRegion>(alterData->AwsRegion),
        NIceDb::TUpdate<Schema::CdcStreamAlterData::State>(alterData->State)
    );
}

void TSchemeShard::PersistRemoveCdcStream(NIceDb::TNiceDb &db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    auto path = PathsById.at(pathId);

    if (!CdcStreams.contains(pathId)) {
        return;
    }

    auto stream = CdcStreams.at(pathId);
    if (stream->AlterData) {
        db.Table<Schema::CdcStreamAlterData>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    }

    db.Table<Schema::CdcStream>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    for (const auto& [shardIdx, _] : stream->ScanShards) {
        RemoveCdcStreamScanShardStatus(db, pathId, shardIdx);
    }

    CdcStreams.erase(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistAlterUserAttributes(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr element = PathsById.at(pathId);

    if (!element->UserAttrs->AlterData) {
        return;
    }

    for (auto& item: element->UserAttrs->AlterData->Attrs) {
        const auto& name = item.first;
        const auto& value = item.second;
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::UserAttributesAlterData>().Key(pathId.LocalPathId, name).Update(
                    NIceDb::TUpdate<Schema::UserAttributesAlterData::AttrValue>(value));
        } else {
            db.Table<Schema::MigratedUserAttributesAlterData>().Key(pathId.OwnerId, pathId.LocalPathId, name).Update(
                NIceDb::TUpdate<Schema::MigratedUserAttributesAlterData::AttrValue>(value));
        }
    }
}

void TSchemeShard::ApplyAndPersistUserAttrs(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr element = PathsById.at(pathId);
    Y_ABORT_UNLESS(element->UserAttrs);

    if (!element->UserAttrs->AlterData) {
        return;
    }

    TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Add(element->UserAttrs->AlterData->Size());
    TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(element->UserAttrs->Size());

    PersistUserAttributes(db, pathId, element->UserAttrs, element->UserAttrs->AlterData);

    element->UserAttrs = element->UserAttrs->AlterData;
    element->UserAttrs->AlterData.Reset();
    element->ApplySpecialAttributes();
}


void TSchemeShard::PersistUserAttributes(NIceDb::TNiceDb& db, TPathId pathId,
                                             TUserAttributes::TPtr oldAttrs, TUserAttributes::TPtr alterAttrs) {
    //remove old version
    if (oldAttrs) {
        for (auto& item: oldAttrs->Attrs) {
            const auto& name = item.first;
            if (pathId.OwnerId == TabletID()) {
                db.Table<Schema::UserAttributes>().Key(pathId.LocalPathId, name).Delete();
            }

            db.Table<Schema::MigratedUserAttributes>().Key(pathId.OwnerId, pathId.LocalPathId, name).Delete();
        }
    }
    //apply new version and clear UserAttributesAlterData
    if (!alterAttrs) {
        return;
    }
    for (auto& item: alterAttrs->Attrs) {
        const auto& name = item.first;
        const auto& value = item.second;
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::UserAttributes>().Key(pathId.LocalPathId, name).Update(
                    NIceDb::TUpdate<Schema::UserAttributes::AttrValue>(value));

            db.Table<Schema::UserAttributesAlterData>().Key(pathId.LocalPathId, name).Delete();
        } else {
            db.Table<Schema::MigratedUserAttributes>().Key(pathId.OwnerId, pathId.LocalPathId, name).Update(
                NIceDb::TUpdate<Schema::MigratedUserAttributes::AttrValue>(value));
        }

        db.Table<Schema::MigratedUserAttributesAlterData>().Key(pathId.OwnerId, pathId.LocalPathId, name).Delete();
    }

    if (pathId.OwnerId == TabletID()) {
        //update UserAttrs's AlterVersion in Paths table
        db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::UserAttrsAlterVersion>(alterAttrs->AlterVersion));
    } else {
        db.Table<Schema::MigratedPaths>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::UserAttrsAlterVersion>(alterAttrs->AlterVersion));
    }
}


void TSchemeShard::PersistLastTxId(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    if (path->PathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Paths::LastTxId>(path->LastTxId));
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::MigratedPaths::LastTxId>(path->LastTxId));
    }
}

void TSchemeShard::PersistPath(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    TPathElement::TPtr elem = PathsById.at(pathId);
    if (IsLocalId(pathId)) {
        db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::Paths::ParentOwnerId>(elem->ParentPathId.OwnerId),
                    NIceDb::TUpdate<Schema::Paths::ParentId>(elem->ParentPathId.LocalPathId),
                    NIceDb::TUpdate<Schema::Paths::Name>(elem->Name),
                    NIceDb::TUpdate<Schema::Paths::PathType>(elem->PathType),
                    NIceDb::TUpdate<Schema::Paths::StepCreated>(elem->StepCreated),
                    NIceDb::TUpdate<Schema::Paths::CreateTxId>(elem->CreateTxId),
                    NIceDb::TUpdate<Schema::Paths::StepDropped>(elem->StepDropped),
                    NIceDb::TUpdate<Schema::Paths::DropTxId>(elem->DropTxId),
                    NIceDb::TUpdate<Schema::Paths::Owner>(elem->Owner),
                    NIceDb::TUpdate<Schema::Paths::ACL>(elem->ACL),
                    NIceDb::TUpdate<Schema::Paths::LastTxId>(elem->LastTxId),
                    NIceDb::TUpdate<Schema::Paths::DirAlterVersion>(elem->DirAlterVersion),
                    NIceDb::TUpdate<Schema::Paths::UserAttrsAlterVersion>(elem->UserAttrs->AlterVersion),
                    NIceDb::TUpdate<Schema::Paths::ACLVersion>(elem->ACLVersion)
                    );
    } else {
        db.Table<Schema::MigratedPaths>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::MigratedPaths::ParentOwnerId>(elem->ParentPathId.OwnerId),
                    NIceDb::TUpdate<Schema::MigratedPaths::ParentLocalId>(elem->ParentPathId.LocalPathId),
                    NIceDb::TUpdate<Schema::MigratedPaths::Name>(elem->Name),
                    NIceDb::TUpdate<Schema::MigratedPaths::PathType>(elem->PathType),
                    NIceDb::TUpdate<Schema::MigratedPaths::StepCreated>(elem->StepCreated),
                    NIceDb::TUpdate<Schema::MigratedPaths::CreateTxId>(elem->CreateTxId),
                    NIceDb::TUpdate<Schema::MigratedPaths::StepDropped>(elem->StepDropped),
                    NIceDb::TUpdate<Schema::MigratedPaths::DropTxId>(elem->DropTxId),
                    NIceDb::TUpdate<Schema::MigratedPaths::Owner>(elem->Owner),
                    NIceDb::TUpdate<Schema::MigratedPaths::ACL>(elem->ACL),
                    NIceDb::TUpdate<Schema::MigratedPaths::LastTxId>(elem->LastTxId),
                    NIceDb::TUpdate<Schema::MigratedPaths::DirAlterVersion>(elem->DirAlterVersion),
                    NIceDb::TUpdate<Schema::MigratedPaths::UserAttrsAlterVersion>(elem->UserAttrs->AlterVersion),
                    NIceDb::TUpdate<Schema::MigratedPaths::ACLVersion>(elem->ACLVersion)
                    );
    }
}

void TSchemeShard::PersistRemovePath(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    Y_ABORT_UNLESS(path->Dropped() && path->DbRefCount == 0);

    // Make sure to cleanup any leftover user attributes for this path
    for (auto& item : path->UserAttrs->Attrs) {
        const auto& name = item.first;
        if (IsLocalId(path->PathId)) {
            db.Table<Schema::UserAttributes>().Key(path->PathId.LocalPathId, name).Delete();
        } else {
            db.Table<Schema::MigratedUserAttributes>().Key(path->PathId.OwnerId, path->PathId.LocalPathId, name).Delete();
        }
    }

    if (IsLocalId(path->PathId)) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Delete();
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Delete();
    }
    PathsById.erase(path->PathId);

    auto itParent = PathsById.find(path->ParentPathId);
    Y_DEBUG_ABORT_UNLESS(itParent != PathsById.end());
    if (itParent != PathsById.end()) {
        itParent->second->RemoveChild(path->Name, path->PathId);
        Y_ABORT_UNLESS(itParent->second->AllChildrenCount > 0);
        --itParent->second->AllChildrenCount;
        DecrementPathDbRefCount(path->ParentPathId, "remove path");
    }
}

void TSchemeShard::PersistPathDirAlterVersion(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    if (path->PathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::DirAlterVersion>(path->DirAlterVersion));
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::DirAlterVersion>(path->DirAlterVersion));
    }
}

void TSchemeShard::PersistSchemeLimit(NIceDb::TNiceDb &db, const TPathId &pathId, const TSubDomainInfo &subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SubDomains::DepthLimit>           (subDomain.GetSchemeLimits().MaxDepth),
        NIceDb::TUpdate<Schema::SubDomains::PathsLimit>           (subDomain.GetSchemeLimits().MaxPaths),
        NIceDb::TUpdate<Schema::SubDomains::ChildrenLimit>        (subDomain.GetSchemeLimits().MaxChildrenInDir),
        NIceDb::TUpdate<Schema::SubDomains::ShardsLimit>          (subDomain.GetSchemeLimits().MaxShards),
        NIceDb::TUpdate<Schema::SubDomains::PathShardsLimit>      (subDomain.GetSchemeLimits().MaxShardsInPath),
        NIceDb::TUpdate<Schema::SubDomains::TableColumnsLimit>    (subDomain.GetSchemeLimits().MaxTableColumns),
        NIceDb::TUpdate<Schema::SubDomains::TableColumnNameLengthLimit>    (subDomain.GetSchemeLimits().MaxTableColumnNameLength),
        NIceDb::TUpdate<Schema::SubDomains::TableKeyColumnsLimit> (subDomain.GetSchemeLimits().MaxTableKeyColumns),
        NIceDb::TUpdate<Schema::SubDomains::TableIndicesLimit>    (subDomain.GetSchemeLimits().MaxTableIndices),
        NIceDb::TUpdate<Schema::SubDomains::TableCdcStreamsLimit>    (subDomain.GetSchemeLimits().MaxTableCdcStreams),
        NIceDb::TUpdate<Schema::SubDomains::AclByteSizeLimit>     (subDomain.GetSchemeLimits().MaxAclBytesSize),
        NIceDb::TUpdate<Schema::SubDomains::ConsistentCopyingTargetsLimit> (subDomain.GetSchemeLimits().MaxConsistentCopyTargets),
        NIceDb::TUpdate<Schema::SubDomains::PathElementLength>             (subDomain.GetSchemeLimits().MaxPathElementLength),
        NIceDb::TUpdate<Schema::SubDomains::ExtraPathSymbolsAllowed>       (subDomain.GetSchemeLimits().ExtraPathSymbolsAllowed),
        NIceDb::TUpdate<Schema::SubDomains::PQPartitionsLimit>             (subDomain.GetSchemeLimits().MaxPQPartitions)
    );
}

void TSchemeShard::PersistStoragePools(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    for (auto pool: subDomain.GetStoragePools()) {
        db.Table<Schema::StoragePools>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Update();
        db.Table<Schema::StoragePoolsAlterData>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Delete();
    }
    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SubDomains::AlterVersion>(subDomain.GetVersion()));
}

void TSchemeShard::PersistInitState(NIceDb::TNiceDb& db) {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_TenantInitState).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString((ui64)InitState)));
}

void TSchemeShard::PersistStorageBillingTime(NIceDb::TNiceDb &db) {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_ServerlessStorageLastBillTime).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString(this->ServerlessStorageLastBillTime.Seconds())));
}

void TSchemeShard::PersistSubDomainAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomainsAlterData::AlterVersion>(subDomain.GetVersion()),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::PlanResolution>(subDomain.GetPlanResolution()),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::TimeCastBuckets>(subDomain.GetTCB()),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::ResourcesDomainOwnerPathId>(subDomain.GetResourcesDomainId().OwnerId),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::ResourcesDomainLocalPathId>(subDomain.GetResourcesDomainId().LocalPathId),
                NIceDb::TUpdate<Schema::SubDomainsAlterData::SharedHiveId>(subDomain.GetSharedHive()));

    if (subDomain.GetDeclaredSchemeQuotas()) {
        TString declaredSchemeQuotas;
        Y_ABORT_UNLESS(subDomain.GetDeclaredSchemeQuotas()->SerializeToString(&declaredSchemeQuotas));
        db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomainsAlterData::DeclaredSchemeQuotas>(declaredSchemeQuotas));
    } else {
        db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TNull<Schema::SubDomainsAlterData::DeclaredSchemeQuotas>());
    }

    if (const auto& databaseQuotas = subDomain.GetDatabaseQuotas()) {
        TString serialized;
        Y_ABORT_UNLESS(databaseQuotas->SerializeToString(&serialized));
        db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomainsAlterData::DatabaseQuotas>(serialized));
    } else {
        db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Update(
                NIceDb::TNull<Schema::SubDomainsAlterData::DatabaseQuotas>());
    }

    PersistSubDomainAuditSettingsAlter(db, pathId, subDomain);
    PersistSubDomainServerlessComputeResourcesModeAlter(db, pathId, subDomain);

    for (auto shardIdx: subDomain.GetPrivateShards()) {
        db.Table<Schema::SubDomainShardsAlterData>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Update();
    }

    for (auto pool: subDomain.GetStoragePools()) {
        db.Table<Schema::StoragePoolsAlterData>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Update();
    }
}

void TSchemeShard::PersistDeleteSubDomainAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& alterDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Delete();

    for (auto shardIdx: alterDomain.GetPrivateShards()) {
        db.Table<Schema::SubDomainShardsAlterData>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Delete();
    }

    for (auto pool: alterDomain.GetStoragePools()) {
        db.Table<Schema::StoragePoolsAlterData>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Delete();
    }
}

void TSchemeShard::PersistSubDomainVersion(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SubDomains::AlterVersion>(subDomain.GetVersion()));
}

void TSchemeShard::PersistSubDomainSecurityStateVersion(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>()
        .Key(pathId.LocalPathId)
        .Update<Schema::SubDomains::SecurityStateVersion>(subDomain.GetSecurityStateVersion());
}

void TSchemeShard::PersistSubDomainPrivateShards(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    for (auto shardIdx: subDomain.GetPrivateShards()) {
        db.Table<Schema::SubDomainShards>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Update();
    }
}

void TSchemeShard::PersistSubDomain(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SubDomains::AlterVersion>(subDomain.GetVersion()),
        NIceDb::TUpdate<Schema::SubDomains::PlanResolution>(subDomain.GetPlanResolution()),
        NIceDb::TUpdate<Schema::SubDomains::TimeCastBuckets>(subDomain.GetTCB()),
        NIceDb::TUpdate<Schema::SubDomains::ResourcesDomainOwnerPathId>(subDomain.GetResourcesDomainId().OwnerId),
        NIceDb::TUpdate<Schema::SubDomains::ResourcesDomainLocalPathId>(subDomain.GetResourcesDomainId().LocalPathId),
        NIceDb::TUpdate<Schema::SubDomains::SharedHiveId>(subDomain.GetSharedHive()));

    PersistSubDomainDeclaredSchemeQuotas(db, pathId, subDomain);
    PersistSubDomainDatabaseQuotas(db, pathId, subDomain);
    PersistSubDomainState(db, pathId, subDomain);

    PersistSubDomainAuditSettings(db, pathId, subDomain);
    PersistSubDomainServerlessComputeResourcesMode(db, pathId, subDomain);

    db.Table<Schema::SubDomainsAlterData>().Key(pathId.LocalPathId).Delete();

    for (auto shardIdx: subDomain.GetPrivateShards()) {
        db.Table<Schema::SubDomainShards>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Update();
        db.Table<Schema::SubDomainShardsAlterData>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Delete();
    }

    PersistStoragePools(db, pathId, subDomain);
}

void TSchemeShard::PersistSubDomainDeclaredSchemeQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (subDomain.GetDeclaredSchemeQuotas()) {
        TString declaredSchemeQuotas;
        Y_ABORT_UNLESS(subDomain.GetDeclaredSchemeQuotas()->SerializeToString(&declaredSchemeQuotas));
        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomains::DeclaredSchemeQuotas>(declaredSchemeQuotas));
    } else {
        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
                NIceDb::TNull<Schema::SubDomains::DeclaredSchemeQuotas>());
    }
}

void TSchemeShard::PersistSubDomainDatabaseQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (const auto& databaseQuotas = subDomain.GetDatabaseQuotas()) {
        TString serialized;
        Y_ABORT_UNLESS(databaseQuotas->SerializeToString(&serialized));
        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomains::DatabaseQuotas>(serialized));
    } else {
        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
                NIceDb::TNull<Schema::SubDomains::DatabaseQuotas>());
    }
}

void TSchemeShard::PersistSubDomainState(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::SubDomains::StateVersion>(subDomain.GetDomainStateVersion()),
            NIceDb::TUpdate<Schema::SubDomains::DiskQuotaExceeded>(subDomain.GetDiskQuotaExceeded()));
}

void TSchemeShard::PersistSubDomainSchemeQuotas(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    const auto& quotas = subDomain.GetSchemeQuotas();

    ui64 idx = 0;
    for (const auto& quota : quotas) {
        if (quota.Dirty) {
            db.Table<Schema::SubDomainSchemeQuotas>().Key(pathId.LocalPathId, idx).Update(
                NIceDb::TUpdate<Schema::SubDomainSchemeQuotas::BucketSize>(quota.BucketSize),
                NIceDb::TUpdate<Schema::SubDomainSchemeQuotas::BucketDurationUs>(quota.BucketDuration.MicroSeconds()),
                NIceDb::TUpdate<Schema::SubDomainSchemeQuotas::Available>(quota.Available),
                NIceDb::TUpdate<Schema::SubDomainSchemeQuotas::LastUpdateUs>(quota.LastUpdate.MicroSeconds()));
            quota.Dirty = false;
        }
        ++idx;
    }

    while (idx < quotas.LastKnownSize) {
        db.Table<Schema::SubDomainSchemeQuotas>().Key(pathId.LocalPathId, idx).Delete();
        ++idx;
    }
    quotas.LastKnownSize = quotas.size();
}

void TSchemeShard::PersistRemoveSubDomain(NIceDb::TNiceDb& db, const TPathId& pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    auto it = SubDomains.find(pathId);
    if (it != SubDomains.end()) {
        TSubDomainInfo::TPtr subDomain = it->second;

        if (subDomain->GetAlter()) {
            PersistDeleteSubDomainAlter(db, pathId, *subDomain->GetAlter());
        }

        for (auto shardIdx: subDomain->GetPrivateShards()) {
            db.Table<Schema::SubDomainShards>().Key(pathId.LocalPathId, shardIdx.GetLocalId()).Delete();
        }

        const auto& quotas = subDomain->GetSchemeQuotas();
        for (ui64 idx = 0; idx < Max(quotas.size(), quotas.LastKnownSize); ++idx) {
            db.Table<Schema::SubDomainSchemeQuotas>().Key(pathId.LocalPathId, idx).Delete();
        }

        for (const auto& pool : subDomain->GetStoragePools()) {
            db.Table<Schema::StoragePools>().Key(pathId.LocalPathId, pool.GetName(), pool.GetKind()).Delete();
        }

        db.Table<Schema::SubDomains>().Key(pathId.LocalPathId).Delete();
        SubDomains.erase(it);
        DecrementPathDbRefCount(pathId);
    }
}

template <class Table>
void PersistSubDomainAuditSettingsImpl(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo::TMaybeAuditSettings& value) {
    using Field = typename Table::AuditSettings;
    if (value) {
        TString serialized;
        Y_ABORT_UNLESS(value->SerializeToString(&serialized));
        db.Table<Table>().Key(pathId.LocalPathId).Update(NIceDb::TUpdate<Field>(serialized));
    } else {
        db.Table<Table>().Key(pathId.LocalPathId).template UpdateToNull<Field>();
    }
}

void TSchemeShard::PersistSubDomainAuditSettings(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    PersistSubDomainAuditSettingsImpl<Schema::SubDomains>(db, pathId, subDomain.GetAuditSettings());
}

void TSchemeShard::PersistSubDomainAuditSettingsAlter(NIceDb::TNiceDb& db, const TPathId& pathId, const TSubDomainInfo& subDomain) {
    PersistSubDomainAuditSettingsImpl<Schema::SubDomainsAlterData>(db, pathId, subDomain.GetAuditSettings());
}

template <class Table>
void PersistSubDomainServerlessComputeResourcesModeImpl(NIceDb::TNiceDb& db, const TPathId& pathId,
                                                        const TMaybeServerlessComputeResourcesMode& value) {
    using Field = typename Table::ServerlessComputeResourcesMode;
    if (value) {
        db.Table<Table>().Key(pathId.LocalPathId).Update(NIceDb::TUpdate<Field>(*value));
    }
}

void TSchemeShard::PersistSubDomainServerlessComputeResourcesMode(NIceDb::TNiceDb& db, const TPathId& pathId,
                                                                  const TSubDomainInfo& subDomain) {
    const auto& serverlessComputeResourcesMode = subDomain.GetServerlessComputeResourcesMode();
    PersistSubDomainServerlessComputeResourcesModeImpl<Schema::SubDomains>(db, pathId, serverlessComputeResourcesMode);
}

void TSchemeShard::PersistSubDomainServerlessComputeResourcesModeAlter(NIceDb::TNiceDb& db, const TPathId& pathId,
                                                                       const TSubDomainInfo& subDomain) {
    const auto& serverlessComputeResourcesMode = subDomain.GetServerlessComputeResourcesMode();
    PersistSubDomainServerlessComputeResourcesModeImpl<Schema::SubDomainsAlterData>(db, pathId, serverlessComputeResourcesMode);
}

void TSchemeShard::PersistACL(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    if (path->PathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::ACL>(path->ACL),
                NIceDb::TUpdate<Schema::Paths::ACLVersion>(path->ACLVersion));
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::ACL>(path->ACL),
            NIceDb::TUpdate<Schema::MigratedPaths::ACLVersion>(path->ACLVersion));
    }
}


void TSchemeShard::PersistOwner(NIceDb::TNiceDb& db, const TPathElement::TPtr path) {
    if (path->PathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(path->PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Paths::Owner>(path->Owner));
    } else {
        db.Table<Schema::MigratedPaths>().Key(path->PathId.OwnerId, path->PathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedPaths::Owner>(path->Owner));
    }
}

void TSchemeShard::PersistCreateTxId(NIceDb::TNiceDb& db, const TPathId pathId, TTxId txId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::CreateTxId>(txId));
}

void TSchemeShard::PersistCreateStep(NIceDb::TNiceDb& db, const TPathId pathId, TStepId step) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    // CreateTxId is saved in PersistPath
    db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::StepCreated>(step));
}

void TSchemeShard::PersistSnapshotTable(NIceDb::TNiceDb& db, const TTxId snapshotId, const TPathId tableId) {
    db.Table<Schema::SnapshotTables>().Key(snapshotId, tableId.OwnerId, tableId.LocalPathId).Update();
}

void TSchemeShard::PersistSnapshotStepId(NIceDb::TNiceDb& db, const TTxId snapshotId, const TStepId stepId) {
    db.Table<Schema::SnapshotSteps>().Key(snapshotId).Update(NIceDb::TUpdate<Schema::SnapshotSteps::StepId>(stepId));
}

void TSchemeShard::PersistDropSnapshot(NIceDb::TNiceDb& db, const TTxId snapshotId, const TPathId tableId) {
    db.Table<Schema::SnapshotTables>().Key(snapshotId, tableId.OwnerId, tableId.LocalPathId).Delete();
    db.Table<Schema::SnapshotSteps>().Key(snapshotId).Delete();
}

void TSchemeShard::PersistLongLock(NIceDb::TNiceDb &db, const TTxId lockId, const TPathId pathId) {
    db.Table<Schema::LongLocks>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::LongLocks::LockId>(lockId));
}

void TSchemeShard::PersistUnLock(NIceDb::TNiceDb& db, const TPathId pathId) {
    db.Table<Schema::LongLocks>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistDropStep(NIceDb::TNiceDb& db, const TPathId pathId, TStepId step, TOperationId opId) {
    Y_ABORT_UNLESS(step, "Drop step must be valid (not 0)");
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Paths>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::Paths::StepDropped>(step),
                NIceDb::TUpdate<Schema::Paths::DropTxId>(opId.GetTxId()));
    } else {
        db.Table<Schema::MigratedPaths>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::MigratedPaths::StepDropped>(step),
                NIceDb::TUpdate<Schema::MigratedPaths::DropTxId>(opId.GetTxId()));
    }
}

void TSchemeShard::PersistTxState(NIceDb::TNiceDb& db, const TOperationId opId) {
    Y_ABORT_UNLESS(TxInFlight.contains(opId));
    TTxState& txState = TxInFlight.at(opId);

    Y_ABORT_UNLESS(txState.TxType != TTxState::TxInvalid);
    Y_ABORT_UNLESS(txState.State != TTxState::Invalid);
    TString extraData;
    if (txState.TxType == TTxState::TxSplitTablePartition || txState.TxType == TTxState::TxMergeTablePartition) {
        Y_ABORT_UNLESS(txState.SplitDescription, "Split Tx must have non-empty split description");
        bool serializeRes = txState.SplitDescription->SerializeToString(&extraData);
        Y_ABORT_UNLESS(serializeRes);
    } else if (txState.TxType == TTxState::TxFinalizeBuildIndex) {
        if (txState.BuildIndexOutcome) {
            bool serializeRes = txState.BuildIndexOutcome->SerializeToString(&extraData);
            Y_ABORT_UNLESS(serializeRes);
        }
    } else if (txState.TxType == TTxState::TxAlterTable) {
        TPathId pathId = txState.TargetPathId;

        Y_VERIFY_S(PathsById.contains(pathId), "Path id " << pathId << " doesn't exist");
        Y_VERIFY_S(PathsById.at(pathId)->IsTable(), "Path id " << pathId << " is not a table");
        Y_VERIFY_S(Tables.FindPtr(pathId), "Table " << pathId << " doesn't exist");

        TTableInfo::TPtr tableInfo = Tables.at(pathId);
        extraData = tableInfo->SerializeAlterExtraData();
    }
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
                NIceDb::TUpdate<Schema::TxInFlightV2::TxType>((ui8)txState.TxType),
                NIceDb::TUpdate<Schema::TxInFlightV2::TargetPathId>(txState.TargetPathId.LocalPathId),
                NIceDb::TUpdate<Schema::TxInFlightV2::State>(txState.State),
                NIceDb::TUpdate<Schema::TxInFlightV2::MinStep>(txState.MinStep),
                NIceDb::TUpdate<Schema::TxInFlightV2::ExtraBytes>(extraData),
                NIceDb::TUpdate<Schema::TxInFlightV2::StartTime>(txState.StartTime.GetValue()),
                NIceDb::TUpdate<Schema::TxInFlightV2::TargetOwnerPathId>(txState.TargetPathId.OwnerId),
                NIceDb::TUpdate<Schema::TxInFlightV2::BuildIndexId>(txState.BuildIndexId),
                NIceDb::TUpdate<Schema::TxInFlightV2::SourceLocalPathId>(txState.SourcePathId.LocalPathId),
                NIceDb::TUpdate<Schema::TxInFlightV2::SourceOwnerId>(txState.SourcePathId.OwnerId),
                NIceDb::TUpdate<Schema::TxInFlightV2::NeedUpdateObject>(txState.NeedUpdateObject),
                NIceDb::TUpdate<Schema::TxInFlightV2::NeedSyncHive>(txState.NeedSyncHive)
                );

    for (const auto& shardOp : txState.Shards) {
        PersistUpdateTxShard(db, opId, shardOp.Idx, shardOp.Operation);
    }
}

void TSchemeShard::PersistTxMinStep(NIceDb::TNiceDb& db, const TOperationId opId, TStepId minStep) {
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
                NIceDb::TUpdate<Schema::TxInFlightV2::MinStep>(minStep)
                );
}

void TSchemeShard::ChangeTxState(NIceDb::TNiceDb& db, const TOperationId opId, TTxState::ETxState newState) {
    Y_VERIFY_S(FindTx(opId),
               "Unknown TxId " << opId.GetTxId()
                               << " PartId " << opId.GetSubTxId());
    Y_ABORT_UNLESS(FindTx(opId)->State != TTxState::Invalid);

    const auto& ctx = TActivationContext::AsActorContext();

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Change state for txid " << opId << " "
                 << (int)TxInFlight[opId].State << " -> " << (int)newState);

    FindTx(opId)->State = newState;
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
        NIceDb::TUpdate<Schema::TxInFlightV2::State>(newState));
}

void TSchemeShard::PersistCancelTx(NIceDb::TNiceDb &db, const TOperationId opId, const TTxState &txState) {
    Y_ABORT_UNLESS(txState.TxType == TTxState::TxBackup || txState.TxType == TTxState::TxRestore);

    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
        NIceDb::TUpdate<Schema::TxInFlightV2::CancelBackup>(txState.Cancel));
}

void TSchemeShard::PersistTxPlanStep(NIceDb::TNiceDb &db, TOperationId opId, TStepId step) {
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Update(
        NIceDb::TUpdate<Schema::TxInFlightV2::PlanStep>(step));
}

void TSchemeShard::PersistRemoveTx(NIceDb::TNiceDb& db, const TOperationId opId, const TTxState& txState) {
    db.Table<Schema::TxInFlightV2>().Key(opId.GetTxId(), opId.GetSubTxId()).Delete();
    for (const auto& shardOp : txState.Shards) {
        PersistRemoveTxShard(db, opId, shardOp.Idx);
    }
}

void TSchemeShard::PersistTable(NIceDb::TNiceDb& db, const TPathId tableId) {
    Y_ABORT_UNLESS(Tables.contains(tableId));
    const TTableInfo::TPtr tableInfo = Tables.at(tableId);

    PersistTableAltered(db, tableId, tableInfo);
    PersistTablePartitioning(db, tableId, tableInfo);
}

void TSchemeShard::PersistChannelsBinding(NIceDb::TNiceDb& db, const TShardIdx shardId, const TChannelsBindings& bindedChannels) {
    for (ui32 channelId = 0; channelId < bindedChannels.size(); ++channelId) {
        const auto& bind = bindedChannels[channelId];
        if (IsLocalId(shardId)) {
            db.Table<Schema::ChannelsBinding>().Key(shardId.GetLocalId(), channelId).Update(
                    NIceDb::TUpdate<Schema::ChannelsBinding::PoolName>(bind.GetStoragePoolName()),
                    NIceDb::TUpdate<Schema::ChannelsBinding::Binding>(bind.SerializeAsString()));
        } else {
            db.Table<Schema::MigratedChannelsBinding>().Key(shardId.GetOwnerId(), shardId.GetLocalId(), channelId).Update(
                NIceDb::TUpdate<Schema::MigratedChannelsBinding::PoolName>(bind.GetStoragePoolName()),
                NIceDb::TUpdate<Schema::MigratedChannelsBinding::Binding>(bind.SerializeAsString()));
        }
    }
}

void TSchemeShard::PersistTablePartitioning(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo) {
    for (ui64 pi = 0; pi < tableInfo->GetPartitions().size(); ++pi) {
        const auto& partition = tableInfo->GetPartitions()[pi];
        if (IsLocalId(pathId) && IsLocalId(partition.ShardIdx)) {
            db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, pi).Update(
                NIceDb::TUpdate<Schema::TablePartitions::RangeEnd>(partition.EndOfRange),
                NIceDb::TUpdate<Schema::TablePartitions::DatashardIdx>(partition.ShardIdx.GetLocalId()),
                NIceDb::TUpdate<Schema::TablePartitions::LastCondErase>(partition.LastCondErase.GetValue()),
                NIceDb::TUpdate<Schema::TablePartitions::NextCondErase>(partition.NextCondErase.GetValue()));
        } else {
            if (IsLocalId(pathId)) {
                // Incompatible change 1:
                // Store migrated shards of local tables in migrated table partitions
                // This change is incompatible with older versions because partitions
                // may no longer be in a single table and will require sorting at load time.
                BumpIncompatibleChanges(db, 1);
            }
            db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Update(
                NIceDb::TUpdate<Schema::MigratedTablePartitions::RangeEnd>(partition.EndOfRange),
                NIceDb::TUpdate<Schema::MigratedTablePartitions::OwnerShardIdx>(partition.ShardIdx.GetOwnerId()),
                NIceDb::TUpdate<Schema::MigratedTablePartitions::LocalShardIdx>(partition.ShardIdx.GetLocalId()),
                NIceDb::TUpdate<Schema::MigratedTablePartitions::LastCondErase>(partition.LastCondErase.GetValue()),
                NIceDb::TUpdate<Schema::MigratedTablePartitions::NextCondErase>(partition.NextCondErase.GetValue()));
        }
    }
    if (IsLocalId(pathId)) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::PartitioningVersion>(++tableInfo->PartitioningVersion));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::PartitioningVersion>(++tableInfo->PartitioningVersion));
    }
}

void TSchemeShard::PersistTablePartitioningDeletion(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo) {
    const auto& partitions = tableInfo->GetPartitions();
    for (ui64 pi = 0; pi < partitions.size(); ++pi) {
        if (IsLocalId(pathId)) {
            db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, pi).Delete();
        }
        db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Delete();
        db.Table<Schema::TablePartitionStats>().Key(pathId.OwnerId, pathId.LocalPathId, pi).Delete();
    }
}

void TSchemeShard::PersistTablePartitionCondErase(NIceDb::TNiceDb& db, const TPathId& pathId, ui64 id, const TTableInfo::TPtr tableInfo) {
    const auto& partition = tableInfo->GetPartitions()[id];

    if (IsLocalId(pathId) && IsLocalId(partition.ShardIdx)) {
        db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, id).Update(
            NIceDb::TUpdate<Schema::TablePartitions::LastCondErase>(partition.LastCondErase.GetValue()),
            NIceDb::TUpdate<Schema::TablePartitions::NextCondErase>(partition.NextCondErase.GetValue()));
    } else {
        if (IsLocalId(pathId)) {
            // Incompatible change 1 (see above)
            BumpIncompatibleChanges(db, 1);
        }
        db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, id).Update(
            NIceDb::TUpdate<Schema::MigratedTablePartitions::LastCondErase>(partition.LastCondErase.GetValue()),
            NIceDb::TUpdate<Schema::MigratedTablePartitions::NextCondErase>(partition.NextCondErase.GetValue()));
    }
}

void TSchemeShard::PersistTablePartitionStats(NIceDb::TNiceDb& db, const TPathId& tableId, ui64 partitionId, const TPartitionStats& stats) {
    if (!AppData()->FeatureFlags.GetEnablePersistentPartitionStats()) {
        return;
    }

    auto persistedStats = db.Table<Schema::TablePartitionStats>().Key(tableId.OwnerId, tableId.LocalPathId, partitionId);
    persistedStats.Update(
        NIceDb::TUpdate<Schema::TablePartitionStats::SeqNoGeneration>(stats.SeqNo.Generation),
        NIceDb::TUpdate<Schema::TablePartitionStats::SeqNoRound>(stats.SeqNo.Round),

        NIceDb::TUpdate<Schema::TablePartitionStats::RowCount>(stats.RowCount),
        NIceDb::TUpdate<Schema::TablePartitionStats::DataSize>(stats.DataSize),
        NIceDb::TUpdate<Schema::TablePartitionStats::IndexSize>(stats.IndexSize),

        NIceDb::TUpdate<Schema::TablePartitionStats::LastAccessTime>(stats.LastAccessTime.GetValue()),
        NIceDb::TUpdate<Schema::TablePartitionStats::LastUpdateTime>(stats.LastUpdateTime.GetValue()),

        NIceDb::TUpdate<Schema::TablePartitionStats::ImmediateTxCompleted>(stats.ImmediateTxCompleted),
        NIceDb::TUpdate<Schema::TablePartitionStats::PlannedTxCompleted>(stats.PlannedTxCompleted),
        NIceDb::TUpdate<Schema::TablePartitionStats::TxRejectedByOverload>(stats.TxRejectedByOverload),
        NIceDb::TUpdate<Schema::TablePartitionStats::TxRejectedBySpace>(stats.TxRejectedBySpace),
        NIceDb::TUpdate<Schema::TablePartitionStats::TxCompleteLag>(stats.TxCompleteLag.GetValue()),
        NIceDb::TUpdate<Schema::TablePartitionStats::InFlightTxCount>(stats.InFlightTxCount),

        NIceDb::TUpdate<Schema::TablePartitionStats::RowUpdates>(stats.RowUpdates),
        NIceDb::TUpdate<Schema::TablePartitionStats::RowDeletes>(stats.RowDeletes),
        NIceDb::TUpdate<Schema::TablePartitionStats::RowReads>(stats.RowReads),
        NIceDb::TUpdate<Schema::TablePartitionStats::RangeReads>(stats.RangeReads),
        NIceDb::TUpdate<Schema::TablePartitionStats::RangeReadRows>(stats.RangeReadRows),

        NIceDb::TUpdate<Schema::TablePartitionStats::CPU>(stats.GetCurrentRawCpuUsage()),
        NIceDb::TUpdate<Schema::TablePartitionStats::Memory>(stats.Memory),
        NIceDb::TUpdate<Schema::TablePartitionStats::Network>(stats.Network),
        NIceDb::TUpdate<Schema::TablePartitionStats::Storage>(stats.Storage),
        NIceDb::TUpdate<Schema::TablePartitionStats::ReadThroughput>(stats.ReadThroughput),
        NIceDb::TUpdate<Schema::TablePartitionStats::WriteThroughput>(stats.WriteThroughput),
        NIceDb::TUpdate<Schema::TablePartitionStats::ReadIops>(stats.ReadIops),
        NIceDb::TUpdate<Schema::TablePartitionStats::WriteIops>(stats.WriteIops),

        NIceDb::TUpdate<Schema::TablePartitionStats::SearchHeight>(stats.SearchHeight),
        NIceDb::TUpdate<Schema::TablePartitionStats::FullCompactionTs>(stats.FullCompactionTs),
        NIceDb::TUpdate<Schema::TablePartitionStats::MemDataSize>(stats.MemDataSize)
    );

    if (!stats.StoragePoolsStats.empty()) {
        NKikimrTableStats::TStoragePoolsStats protobufRepresentation;
        for (const auto& [poolKind, storagePoolStats] : stats.StoragePoolsStats) {
            auto* poolUsage = protobufRepresentation.MutablePoolsUsage()->Add();
            poolUsage->SetPoolKind(poolKind);
            poolUsage->SetDataSize(storagePoolStats.DataSize);
            poolUsage->SetIndexSize(storagePoolStats.IndexSize);
        }
        TString serializedStoragePoolsStats;
        Y_ABORT_UNLESS(protobufRepresentation.SerializeToString(&serializedStoragePoolsStats));
        persistedStats.Update(NIceDb::TUpdate<Schema::TablePartitionStats::StoragePoolsStats>(serializedStoragePoolsStats));
    } else {
        persistedStats.Update(NIceDb::TNull<Schema::TablePartitionStats::StoragePoolsStats>());
    }
}

void TSchemeShard::PersistTablePartitionStats(NIceDb::TNiceDb& db, const TPathId& tableId, const TShardIdx& shardIdx, const TTableInfo::TPtr tableInfo) {
    if (!AppData()->FeatureFlags.GetEnablePersistentPartitionStats()) {
        return;
    }

    const auto& shardToPartition = tableInfo->GetShard2PartitionIdx();
    if (!shardToPartition.contains(shardIdx)) {
        return;
    }

    const auto& tableStats = tableInfo->GetStats();
    if (!tableStats.PartitionStats.contains(shardIdx)) {
        return;
    }

    const ui64 partitionId = shardToPartition.at(shardIdx);
    const auto& stats = tableStats.PartitionStats.at(shardIdx);
    PersistTablePartitionStats(db, tableId, partitionId, stats);
}

void TSchemeShard::PersistTablePartitionStats(NIceDb::TNiceDb& db, const TPathId& tableId, const TTableInfo::TPtr tableInfo) {
    if (!AppData()->FeatureFlags.GetEnablePersistentPartitionStats()) {
        return;
    }

    const auto& tableStats = tableInfo->GetStats();

    for (const auto& [shardIdx, pi] : tableInfo->GetShard2PartitionIdx()) {
        if (!tableStats.PartitionStats.contains(shardIdx)) {
            continue;
        }

        PersistTablePartitionStats(db, tableId, pi, tableStats.PartitionStats.at(shardIdx));
    }
}

void TSchemeShard::PersistPersQueueGroupStats(NIceDb::TNiceDb &db, const TPathId pathId, const TTopicStats& stats) {
    db.Table<Schema::PersQueueGroupStats>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::PersQueueGroupStats::SeqNoGeneration>(stats.SeqNo.Generation),
        NIceDb::TUpdate<Schema::PersQueueGroupStats::SeqNoRound>(stats.SeqNo.Round),

        NIceDb::TUpdate<Schema::PersQueueGroupStats::DataSize>(stats.DataSize),
        NIceDb::TUpdate<Schema::PersQueueGroupStats::UsedReserveSize>(stats.UsedReserveSize)
    );
}

void TSchemeShard::PersistTableAlterVersion(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo) {
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::AlterVersion>(tableInfo->AlterVersion));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::AlterVersion>(tableInfo->AlterVersion));
    }
}

void TSchemeShard::PersistTableFinishColumnBuilding(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo, ui64 colId) {
    const auto& cinfo = tableInfo->Columns.at(colId);
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Columns>().Key(pathId.LocalPathId, colId).Update(
            NIceDb::TUpdate<Schema::Columns::IsBuildInProgress>(cinfo.IsBuildInProgress));

    } else {
        db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Update(
            NIceDb::TUpdate<Schema::MigratedColumns::IsBuildInProgress>(cinfo.IsBuildInProgress));
    }
}

void TSchemeShard::PersistTableAltered(NIceDb::TNiceDb& db, const TPathId pathId, const TTableInfo::TPtr tableInfo) {
    TString partitionConfig;
    Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->PartitionConfig().SerializeToString(&partitionConfig);

    TString ttlSettings;
    if (tableInfo->HasTTLSettings()) {
        Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->TTLSettings().SerializeToString(&ttlSettings);
    }

    TString replicationConfig;
    if (tableInfo->HasReplicationConfig()) {
        Y_PROTOBUF_SUPPRESS_NODISCARD tableInfo->ReplicationConfig().SerializeToString(&replicationConfig);
    }

    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::NextColId>(tableInfo->NextColumnId),
            NIceDb::TUpdate<Schema::Tables::PartitionConfig>(partitionConfig),
            NIceDb::TUpdate<Schema::Tables::AlterVersion>(tableInfo->AlterVersion),
            NIceDb::TUpdate<Schema::Tables::AlterTable>(TString()),
            NIceDb::TUpdate<Schema::Tables::AlterTableFull>(TString()),
            NIceDb::TUpdate<Schema::Tables::TTLSettings>(ttlSettings),
            NIceDb::TUpdate<Schema::Tables::IsBackup>(tableInfo->IsBackup),
            NIceDb::TUpdate<Schema::Tables::ReplicationConfig>(replicationConfig),
            NIceDb::TUpdate<Schema::Tables::IsTemporary>(tableInfo->IsTemporary),
            NIceDb::TUpdate<Schema::Tables::OwnerActorId>(tableInfo->OwnerActorId.ToString()));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::NextColId>(tableInfo->NextColumnId),
            NIceDb::TUpdate<Schema::MigratedTables::PartitionConfig>(partitionConfig),
            NIceDb::TUpdate<Schema::MigratedTables::AlterVersion>(tableInfo->AlterVersion),
            NIceDb::TUpdate<Schema::MigratedTables::AlterTable>(TString()),
            NIceDb::TUpdate<Schema::MigratedTables::AlterTableFull>(TString()),
            NIceDb::TUpdate<Schema::MigratedTables::TTLSettings>(ttlSettings),
            NIceDb::TUpdate<Schema::MigratedTables::IsBackup>(tableInfo->IsBackup),
            NIceDb::TUpdate<Schema::MigratedTables::ReplicationConfig>(replicationConfig),
            NIceDb::TUpdate<Schema::MigratedTables::IsTemporary>(tableInfo->IsTemporary),
            NIceDb::TUpdate<Schema::MigratedTables::OwnerActorId>(tableInfo->OwnerActorId.ToString()));
    }

    for (auto col : tableInfo->Columns) {
        ui32 colId = col.first;
        const TTableInfo::TColumn& cinfo = col.second;
        TString typeData;
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(cinfo.PType, cinfo.PTypeMod);
        if (columnType.TypeInfo) {
            Y_ABORT_UNLESS(columnType.TypeInfo->SerializeToString(&typeData));
        }
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::Columns>().Key(pathId.LocalPathId, colId).Update(
                NIceDb::TUpdate<Schema::Columns::ColName>(cinfo.Name),
                NIceDb::TUpdate<Schema::Columns::ColType>((ui32)columnType.TypeId),
                NIceDb::TUpdate<Schema::Columns::ColTypeData>(typeData),
                NIceDb::TUpdate<Schema::Columns::ColKeyOrder>(cinfo.KeyOrder),
                NIceDb::TUpdate<Schema::Columns::CreateVersion>(cinfo.CreateVersion),
                NIceDb::TUpdate<Schema::Columns::DeleteVersion>(cinfo.DeleteVersion),
                NIceDb::TUpdate<Schema::Columns::Family>(cinfo.Family),
                NIceDb::TUpdate<Schema::Columns::DefaultKind>(cinfo.DefaultKind),
                NIceDb::TUpdate<Schema::Columns::DefaultValue>(cinfo.DefaultValue),
                NIceDb::TUpdate<Schema::Columns::NotNull>(cinfo.NotNull),
                NIceDb::TUpdate<Schema::Columns::IsBuildInProgress>(cinfo.IsBuildInProgress));

            db.Table<Schema::ColumnAlters>().Key(pathId.LocalPathId, colId).Delete();
        } else {
            db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Update(
                NIceDb::TUpdate<Schema::MigratedColumns::ColName>(cinfo.Name),
                NIceDb::TUpdate<Schema::MigratedColumns::ColType>((ui32)columnType.TypeId),
                NIceDb::TUpdate<Schema::MigratedColumns::ColTypeData>(typeData),
                NIceDb::TUpdate<Schema::MigratedColumns::ColKeyOrder>(cinfo.KeyOrder),
                NIceDb::TUpdate<Schema::MigratedColumns::CreateVersion>(cinfo.CreateVersion),
                NIceDb::TUpdate<Schema::MigratedColumns::DeleteVersion>(cinfo.DeleteVersion),
                NIceDb::TUpdate<Schema::MigratedColumns::Family>(cinfo.Family),
                NIceDb::TUpdate<Schema::MigratedColumns::DefaultKind>(cinfo.DefaultKind),
                NIceDb::TUpdate<Schema::MigratedColumns::DefaultValue>(cinfo.DefaultValue),
                NIceDb::TUpdate<Schema::MigratedColumns::NotNull>(cinfo.NotNull),
                NIceDb::TUpdate<Schema::MigratedColumns::IsBuildInProgress>(cinfo.IsBuildInProgress));
        }
        db.Table<Schema::MigratedColumnAlters>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Delete();
    }
}

/// @note Legacy. It's better to use Alter logic here: save new data in AlterData and swap it on complete.
void TSchemeShard::PersistTableCreated(NIceDb::TNiceDb& db, const TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::Tables::AlterVersion>(1));
}

void TSchemeShard::PersistAddAlterTable(NIceDb::TNiceDb& db, TPathId pathId, const TTableInfo::TAlterDataPtr alter) {
    TString proto;
    Y_PROTOBUF_SUPPRESS_NODISCARD alter->TableDescriptionFull->SerializeToString(&proto);
    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::Tables::AlterTableFull>(proto));
    } else {
        db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedTables::AlterTableFull>(proto));
    }

    for (auto col : alter->Columns) {
        ui32 colId = col.first;
        const TTableInfo::TColumn& cinfo = col.second;
        TString typeData;
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(cinfo.PType, cinfo.PTypeMod);
        if (columnType.TypeInfo) {
            Y_ABORT_UNLESS(columnType.TypeInfo->SerializeToString(&typeData));
        }
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::ColumnAlters>().Key(pathId.LocalPathId, colId).Update(
                NIceDb::TUpdate<Schema::ColumnAlters::ColName>(cinfo.Name),
                NIceDb::TUpdate<Schema::ColumnAlters::ColType>((ui32)columnType.TypeId),
                NIceDb::TUpdate<Schema::ColumnAlters::ColTypeData>(typeData),
                NIceDb::TUpdate<Schema::ColumnAlters::ColKeyOrder>(cinfo.KeyOrder),
                NIceDb::TUpdate<Schema::ColumnAlters::CreateVersion>(cinfo.CreateVersion),
                NIceDb::TUpdate<Schema::ColumnAlters::DeleteVersion>(cinfo.DeleteVersion),
                NIceDb::TUpdate<Schema::ColumnAlters::Family>(cinfo.Family),
                NIceDb::TUpdate<Schema::ColumnAlters::DefaultKind>(cinfo.DefaultKind),
                NIceDb::TUpdate<Schema::ColumnAlters::DefaultValue>(cinfo.DefaultValue),
                NIceDb::TUpdate<Schema::ColumnAlters::NotNull>(cinfo.NotNull),
                NIceDb::TUpdate<Schema::ColumnAlters::IsBuildInProgress>(cinfo.IsBuildInProgress));
        } else {
            db.Table<Schema::MigratedColumnAlters>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Update(
                NIceDb::TUpdate<Schema::MigratedColumnAlters::ColName>(cinfo.Name),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::ColType>((ui32)columnType.TypeId),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::ColTypeData>(typeData),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::ColKeyOrder>(cinfo.KeyOrder),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::CreateVersion>(cinfo.CreateVersion),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::DeleteVersion>(cinfo.DeleteVersion),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::Family>(cinfo.Family),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::DefaultKind>(cinfo.DefaultKind),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::DefaultValue>(cinfo.DefaultValue),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::NotNull>(cinfo.NotNull),
                NIceDb::TUpdate<Schema::MigratedColumnAlters::IsBuildInProgress>(cinfo.IsBuildInProgress));
        }
    }
}

void TSchemeShard::PersistPersQueueGroup(NIceDb::TNiceDb& db, TPathId pathId, const TTopicInfo::TPtr pqGroup) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::PersQueueGroups>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::PersQueueGroups::TabletConfig>(pqGroup->TabletConfig),
        NIceDb::TUpdate<Schema::PersQueueGroups::MaxPQPerShard>(pqGroup->MaxPartsPerTablet),
        NIceDb::TUpdate<Schema::PersQueueGroups::AlterVersion>(pqGroup->AlterVersion),
        NIceDb::TUpdate<Schema::PersQueueGroups::TotalGroupCount>(pqGroup->TotalGroupCount),
        NIceDb::TUpdate<Schema::PersQueueGroups::NextPartitionId>(pqGroup->NextPartitionId));
}

void TSchemeShard::PersistRemovePersQueueGroup(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    auto it = Topics.find(pathId);
    if (it != Topics.end()) {
        TTopicInfo::TPtr pqGroup = it->second;

        if (pqGroup->AlterData) {
            PersistRemovePersQueueGroupAlter(db, pathId);
        }

        for (const auto& shard : pqGroup->Shards) {
            for (const auto& pqInfo : shard.second->Partitions) {
                PersistRemovePersQueue(db, pathId, pqInfo->PqId);
            }
        }

        Topics.erase(it);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::PersQueueGroups>().Key(pathId.LocalPathId).Delete();
    db.Table<Schema::PersQueueGroupStats>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistAddPersQueueGroupAlter(NIceDb::TNiceDb& db, TPathId pathId, const TTopicInfo::TPtr alterData) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::PersQueueGroupAlters>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::TabletConfig>(alterData->TabletConfig),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::MaxPQPerShard>(alterData->MaxPartsPerTablet),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::AlterVersion>(alterData->AlterVersion),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::TotalGroupCount>(alterData->TotalGroupCount),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::NextPartitionId>(alterData->NextPartitionId),
        NIceDb::TUpdate<Schema::PersQueueGroupAlters::BootstrapConfig>(alterData->BootstrapConfig));
}

void TSchemeShard::PersistRemovePersQueueGroupAlter(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::PersQueueGroupAlters>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistPersQueue(NIceDb::TNiceDb &db, TPathId pathId, TShardIdx shardIdx, const TTopicTabletInfo::TTopicPartitionInfo& pqInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    Y_ABORT_UNLESS(pqInfo.ParentPartitionIds.size() <= 2);
    auto it = pqInfo.ParentPartitionIds.begin();
    const auto parent = it != pqInfo.ParentPartitionIds.end() ? (it++).cur->val : Max<ui32>();
    const auto adjacentParent = it != pqInfo.ParentPartitionIds.end() ? (it++).cur->val : Max<ui32>();

    db.Table<Schema::PersQueues>()
        .Key(pathId.LocalPathId, pqInfo.PqId)
        .Update(NIceDb::TUpdate<Schema::PersQueues::ShardIdx>(shardIdx.GetLocalId()),
                NIceDb::TUpdate<Schema::PersQueues::GroupId>(pqInfo.GroupId),
                NIceDb::TUpdate<Schema::PersQueues::AlterVersion>(pqInfo.AlterVersion),
                NIceDb::TUpdate<Schema::PersQueues::CreateVersion>(pqInfo.CreateVersion),
                NIceDb::TUpdate<Schema::PersQueues::Status>(pqInfo.Status),
                NIceDb::TUpdate<Schema::PersQueues::Parent>(parent),
                NIceDb::TUpdate<Schema::PersQueues::AdjacentParent>(adjacentParent));

    if (pqInfo.KeyRange) {
        if (pqInfo.KeyRange->FromBound) {
            db.Table<Schema::PersQueues>().Key(pathId.LocalPathId, pqInfo.PqId).Update(
                NIceDb::TUpdate<Schema::PersQueues::RangeBegin>(*pqInfo.KeyRange->FromBound));
        }

        if (pqInfo.KeyRange->ToBound) {
            db.Table<Schema::PersQueues>().Key(pathId.LocalPathId, pqInfo.PqId).Update(
                NIceDb::TUpdate<Schema::PersQueues::RangeEnd>(*pqInfo.KeyRange->ToBound));
        }
    }
}

void TSchemeShard::PersistRemovePersQueue(NIceDb::TNiceDb &db, TPathId pathId, ui32 pqId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::PersQueues>().Key(pathId.LocalPathId, pqId).Delete();
}

void TSchemeShard::PersistRtmrVolume(NIceDb::TNiceDb &db, TPathId pathId, const TRtmrVolumeInfo::TPtr rtmrVol) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::RtmrVolumes>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::RtmrVolumes::PartitionsCount>(rtmrVol->Partitions.size()));

    for (const auto& partition: rtmrVol->Partitions) {
        TString partitionId = TString((const char*)partition.second->Id.dw, sizeof(TGUID));

        db.Table<Schema::RTMRPartitions>().Key(pathId.LocalPathId, partition.second->ShardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::RTMRPartitions::PartitionId>(partitionId),
            NIceDb::TUpdate<Schema::RTMRPartitions::BusKey>(partition.second->BusKey));
    }
}

void TSchemeShard::PersistExternalTable(NIceDb::TNiceDb &db, TPathId pathId, const TExternalTableInfo::TPtr externalTableInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::ExternalTable>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ExternalTable::SourceType>{externalTableInfo->SourceType},
        NIceDb::TUpdate<Schema::ExternalTable::DataSourcePath>{externalTableInfo->DataSourcePath},
        NIceDb::TUpdate<Schema::ExternalTable::Location>{externalTableInfo->Location},
        NIceDb::TUpdate<Schema::ExternalTable::AlterVersion>{externalTableInfo->AlterVersion},
        NIceDb::TUpdate<Schema::ExternalTable::Content>{externalTableInfo->Content});

    for (auto col : externalTableInfo->Columns) {
        ui32 colId = col.first;
        const TTableInfo::TColumn& cinfo = col.second;
        TString typeData;
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(cinfo.PType, cinfo.PTypeMod);
        if (columnType.TypeInfo) {
            Y_ABORT_UNLESS(columnType.TypeInfo->SerializeToString(&typeData));
        }
        db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Update(
            NIceDb::TUpdate<Schema::MigratedColumns::ColName>(cinfo.Name),
            NIceDb::TUpdate<Schema::MigratedColumns::ColType>((ui32)columnType.TypeId),
            NIceDb::TUpdate<Schema::MigratedColumns::ColTypeData>(typeData),
            NIceDb::TUpdate<Schema::MigratedColumns::ColKeyOrder>(cinfo.KeyOrder),
            NIceDb::TUpdate<Schema::MigratedColumns::CreateVersion>(cinfo.CreateVersion),
            NIceDb::TUpdate<Schema::MigratedColumns::DeleteVersion>(cinfo.DeleteVersion),
            NIceDb::TUpdate<Schema::MigratedColumns::Family>(cinfo.Family),
            NIceDb::TUpdate<Schema::MigratedColumns::DefaultKind>(cinfo.DefaultKind),
            NIceDb::TUpdate<Schema::MigratedColumns::DefaultValue>(cinfo.DefaultValue),
            NIceDb::TUpdate<Schema::MigratedColumns::NotNull>(cinfo.NotNull),
            NIceDb::TUpdate<Schema::MigratedColumns::IsBuildInProgress>(cinfo.IsBuildInProgress));
    }
}

void TSchemeShard::PersistRemoveExternalTable(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (ExternalTables.contains(pathId)) {
        auto externalTableInfo = ExternalTables.at(pathId);

        for (auto col : externalTableInfo->Columns) {
            const ui32 colId = col.first;
            db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, colId).Delete();
        }

        ExternalTables.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::ExternalTable>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistExternalDataSource(NIceDb::TNiceDb &db, TPathId pathId, const TExternalDataSourceInfo::TPtr externalDataSourceInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::ExternalDataSource>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ExternalDataSource::AlterVersion>{externalDataSourceInfo->AlterVersion},
        NIceDb::TUpdate<Schema::ExternalDataSource::SourceType>{externalDataSourceInfo->SourceType},
        NIceDb::TUpdate<Schema::ExternalDataSource::Location>{externalDataSourceInfo->Location},
        NIceDb::TUpdate<Schema::ExternalDataSource::Installation>{externalDataSourceInfo->Installation},
        NIceDb::TUpdate<Schema::ExternalDataSource::Auth>{externalDataSourceInfo->Auth.SerializeAsString()},
        NIceDb::TUpdate<Schema::ExternalDataSource::ExternalTableReferences>{externalDataSourceInfo->ExternalTableReferences.SerializeAsString()},
        NIceDb::TUpdate<Schema::ExternalDataSource::Properties>{externalDataSourceInfo->Properties.SerializeAsString()}
    );
}

void TSchemeShard::PersistRemoveExternalDataSource(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (ExternalDataSources.contains(pathId)) {
        ExternalDataSources.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::ExternalDataSource>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistView(NIceDb::TNiceDb &db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    const auto path = PathsById.find(pathId);
    Y_ABORT_UNLESS(path != PathsById.end());
    Y_ABORT_UNLESS(path->second && path->second->IsView());

    const auto view = Views.find(pathId);
    Y_ABORT_UNLESS(view != Views.end());
    TViewInfo::TPtr viewInfo = view->second;
    Y_ABORT_UNLESS(viewInfo);

    db.Table<Schema::View>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::View::AlterVersion>{viewInfo->AlterVersion},
        NIceDb::TUpdate<Schema::View::QueryText>{viewInfo->QueryText});
}

void TSchemeShard::PersistRemoveView(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    if (const auto view = Views.find(pathId); view != Views.end()) {
        Views.erase(view);
    }
    db.Table<Schema::View>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistRemoveRtmrVolume(NIceDb::TNiceDb &db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    auto it = RtmrVolumes.find(pathId);
    if (it != RtmrVolumes.end()) {
        TRtmrVolumeInfo::TPtr rtmrVol = it->second;

        for (const auto& partition : rtmrVol->Partitions) {
            db.Table<Schema::RTMRPartitions>().Key(pathId.LocalPathId, partition.second->ShardIdx.GetLocalId()).Delete();
        }

        RtmrVolumes.erase(it);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::RtmrVolumes>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId, const TSolomonVolumeInfo::TPtr solomonVol) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SolomonVolumes>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SolomonVolumes::Version>(solomonVol->Version));

    db.Table<Schema::AlterSolomonVolumes>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    for (const auto& part: solomonVol->Partitions) {
        db.Table<Schema::SolomonPartitions>().Key(pathId.LocalPathId, part.first.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::SolomonPartitions::PartitionId>(part.second->PartitionId));

        db.Table<Schema::AlterSolomonPartitions>()
            .Key(
                pathId.OwnerId, pathId.LocalPathId,
                part.first.GetOwnerId(), part.first.GetLocalId())
            .Delete();
    }
}

void TSchemeShard::PersistRemoveSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    auto it = SolomonVolumes.find(pathId);
    if (it != SolomonVolumes.end()) {
        TSolomonVolumeInfo::TPtr solomonVol = it->second;

        if (solomonVol->AlterData) {
            for (const auto& part : solomonVol->AlterData->Partitions) {
                db.Table<Schema::AlterSolomonPartitions>()
                    .Key(
                        pathId.OwnerId, pathId.LocalPathId,
                        part.first.GetOwnerId(), part.first.GetLocalId())
                    .Delete();
            }
            db.Table<Schema::AlterSolomonVolumes>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
        }

        for (const auto& part : solomonVol->Partitions) {
            db.Table<Schema::SolomonPartitions>().Key(pathId.LocalPathId, part.first.GetLocalId()).Delete();
        }

        SolomonVolumes.erase(it);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::SolomonVolumes>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistAlterSolomonVolume(NIceDb::TNiceDb &db, TPathId pathId, const TSolomonVolumeInfo::TPtr solomonVol) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    Y_ABORT_UNLESS(solomonVol->AlterData);


    db.Table<Schema::AlterSolomonVolumes>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::AlterSolomonVolumes::Version>(solomonVol->AlterData->Version));

    for (const auto& part: solomonVol->AlterData->Partitions) {
        db.Table<Schema::AlterSolomonPartitions>()
            .Key(
                pathId.OwnerId, pathId.LocalPathId,
                part.first.GetOwnerId(), part.first.GetLocalId())
            .Update(
                NIceDb::TUpdate<Schema::AlterSolomonPartitions::PartitionId>(part.second->PartitionId));
    }
}

void TSchemeShard::PersistAddTxDependency(NIceDb::TNiceDb& db, const TTxId parentOpId, const TTxId opId) {
    db.Table<Schema::TxDependencies>().Key(parentOpId, opId).Update();
}

void TSchemeShard::PersistRemoveTxDependency(NIceDb::TNiceDb& db, TTxId opId, TTxId dependentOpId) {
    db.Table<Schema::TxDependencies>().Key(opId, dependentOpId).Delete();
}

void TSchemeShard::PersistUpdateTxShard(NIceDb::TNiceDb& db, TOperationId opId, TShardIdx shardIdx, ui32 operation) {
    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::TxShardsV2>().Key(opId.GetTxId(), opId.GetSubTxId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::TxShardsV2::Operation>(operation));
    } else {
        db.Table<Schema::MigratedTxShards>().Key(opId.GetTxId(), opId.GetSubTxId(), shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedTxShards::Operation>(operation));
    }
}

void TSchemeShard::PersistRemoveTxShard(NIceDb::TNiceDb& db, TOperationId opId, TShardIdx shardIdx) {
    if (FirstSubTxId == opId.GetSubTxId()) {
        db.Table<Schema::TxShards>().Key(opId.GetTxId(), shardIdx.GetLocalId()).Delete();
    }

    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::TxShardsV2>().Key(opId.GetTxId(), opId.GetSubTxId(), shardIdx.GetLocalId()).Delete();
    }

    db.Table<Schema::MigratedTxShards>().Key(opId.GetTxId(), opId.GetSubTxId(), shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
}

void TSchemeShard::PersistUpdateNextPathId(NIceDb::TNiceDb& db) const {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_NextPathId).Update(
                NIceDb::TUpdate<Schema::SysParams::Value>(ToString(NextLocalPathId)));
}

void TSchemeShard::PersistUpdateNextShardIdx(NIceDb::TNiceDb& db) const {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_NextShardIdx).Update(
                NIceDb::TUpdate<Schema::SysParams::Value>(ToString(NextLocalShardIdx)));
}

void TSchemeShard::PersistParentDomain(NIceDb::TNiceDb& db, TPathId parentDomain) const {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainSchemeShard).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString(parentDomain.OwnerId)));

    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainPathId).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString(parentDomain.LocalPathId)));
}

void TSchemeShard::PersistParentDomainEffectiveACL(NIceDb::TNiceDb& db, const TString& owner, const TString& effectiveACL, ui64 effectiveACLVersion) const {
    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainOwner).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(owner));

    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainEffectiveACL).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(effectiveACL));

    db.Table<Schema::SysParams>().Key(Schema::SysParam_ParentDomainEffectiveACLVersion).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(ToString(effectiveACLVersion)));
}

void TSchemeShard::PersistShardMapping(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTabletId tabletId, TPathId pathId, TTxId txId, TTabletTypes::EType type) {
    if (IsLocalId(shardIdx)) {
        db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Update(
                NIceDb::TUpdate<Schema::Shards::TabletId>(tabletId),
                NIceDb::TUpdate<Schema::Shards::OwnerPathId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::Shards::PathId>(pathId.LocalPathId),
                NIceDb::TUpdate<Schema::Shards::LastTxId>(txId),
                NIceDb::TUpdate<Schema::Shards::TabletType>(type));
    } else {
        db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedShards::TabletId>(tabletId),
            NIceDb::TUpdate<Schema::MigratedShards::OwnerPathId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::MigratedShards::LocalPathId>(pathId.LocalPathId),
            NIceDb::TUpdate<Schema::MigratedShards::LastTxId>(txId),
            NIceDb::TUpdate<Schema::MigratedShards::TabletType>(type));
    }
}

void TSchemeShard::PersistAdoptedShardMapping(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTabletId tabletId, ui64 prevOwner, TLocalShardIdx prevShardIdx) {
    Y_ABORT_UNLESS(IsLocalId(shardIdx));
    db.Table<Schema::AdoptedShards>().Key(shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::AdoptedShards::PrevOwner>(prevOwner),
            NIceDb::TUpdate<Schema::AdoptedShards::PrevShardIdx>(prevShardIdx),
            NIceDb::TUpdate<Schema::AdoptedShards::TabletId>(tabletId));
}

void TSchemeShard::PersistShardPathId(NIceDb::TNiceDb& db, TShardIdx shardIdx, TPathId pathId) {
    if (IsLocalId(shardIdx)) {
        db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Update(
                NIceDb::TUpdate<Schema::Shards::OwnerPathId>(pathId.OwnerId),
                NIceDb::TUpdate<Schema::Shards::PathId>(pathId.LocalPathId));
    } else {
        db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedShards::OwnerPathId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::MigratedShards::LocalPathId>(pathId.LocalPathId));
    }
}

void TSchemeShard::PersistDeleteAdopted(NIceDb::TNiceDb& db, TShardIdx shardIdx) {
    Y_ABORT_UNLESS(IsLocalId(shardIdx));
    db.Table<Schema::AdoptedShards>().Key(shardIdx.GetLocalId()).Delete();
}

void TSchemeShard::PersistShardTx(NIceDb::TNiceDb& db, TShardIdx shardIdx, TTxId txId) {
    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Update(
                NIceDb::TUpdate<Schema::Shards::LastTxId>(txId));
    } else {
        db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedShards::LastTxId>(txId));
    }
}

void TSchemeShard::PersistShardsToDelete(NIceDb::TNiceDb& db, const THashSet<TShardIdx>& shardsIdxs) {
    for (auto& shardIdx : shardsIdxs) {
        if (shardIdx.GetOwnerId() == TabletID()) {
            db.Table<Schema::ShardsToDelete>().Key(shardIdx.GetLocalId()).Update();
        } else {
            db.Table<Schema::MigratedShardsToDelete>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update();
        }
    }
}

void TSchemeShard::PersistShardDeleted(NIceDb::TNiceDb& db, TShardIdx shardIdx, const TChannelsBindings& bindedChannels) {
    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::ShardsToDelete>().Key(shardIdx.GetLocalId()).Delete();
        db.Table<Schema::Shards>().Key(shardIdx.GetLocalId()).Delete();
        for (ui32 channelId = 0; channelId < bindedChannels.size(); ++channelId) {
            db.Table<Schema::ChannelsBinding>().Key(shardIdx.GetLocalId(), channelId).Delete();
        }
        db.Table<Schema::TableShardPartitionConfigs>().Key(shardIdx.GetLocalId()).Delete();
    }

    db.Table<Schema::MigratedShardsToDelete>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    db.Table<Schema::MigratedShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    for (ui32 channelId = 0; channelId < bindedChannels.size(); ++channelId) {
        db.Table<Schema::MigratedChannelsBinding>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId(), channelId).Delete();
    }
    db.Table<Schema::MigratedTableShardPartitionConfigs>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
}

void TSchemeShard::PersistUnknownShardDeleted(NIceDb::TNiceDb& db, TShardIdx shardIdx) {
    if (shardIdx.GetOwnerId() == TabletID()) {
        db.Table<Schema::ShardsToDelete>().Key(shardIdx.GetLocalId()).Delete();
    }

    db.Table<Schema::MigratedShardsToDelete>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
}

void TSchemeShard::PersistTxShardStatus(NIceDb::TNiceDb& db, TOperationId opId, TShardIdx shardIdx, const TTxState::TShardStatus& status) {
    db.Table<Schema::TxShardStatus>()
        .Key(opId.GetTxId(), shardIdx.GetOwnerId(), shardIdx.GetLocalId())
        .Update(
            NIceDb::TUpdate<Schema::TxShardStatus::Success>(status.Success),
            NIceDb::TUpdate<Schema::TxShardStatus::Error>(status.Error),
            NIceDb::TUpdate<Schema::TxShardStatus::BytesProcessed>(status.BytesProcessed),
            NIceDb::TUpdate<Schema::TxShardStatus::RowsProcessed>(status.RowsProcessed)
        );
}

void TSchemeShard::PersistBackupSettings(
        NIceDb::TNiceDb& db,
        TPathId pathId,
        const NKikimrSchemeOp::TBackupTask& settings)
{
#define PERSIST_BACKUP_SETTINGS(Kind) \
    if (settings.Has##Kind()) { \
        if (IsLocalId(pathId)) { \
            db.Table<Schema::BackupSettings>().Key(pathId.LocalPathId).Update( \
                NIceDb::TUpdate<Schema::BackupSettings::TableName>(settings.GetTableName()), \
                NIceDb::TUpdate<Schema::BackupSettings::Kind>(settings.Get##Kind().SerializeAsString()), \
                NIceDb::TUpdate<Schema::BackupSettings::ScanSettings>(settings.GetScanSettings().SerializeAsString()), \
                NIceDb::TUpdate<Schema::BackupSettings::NeedToBill>(settings.GetNeedToBill()), \
                NIceDb::TUpdate<Schema::BackupSettings::TableDescription>(settings.GetTable().SerializeAsString()), \
                NIceDb::TUpdate<Schema::BackupSettings::NumberOfRetries>(settings.GetNumberOfRetries())); \
        } else { \
            db.Table<Schema::MigratedBackupSettings>().Key(pathId.OwnerId, pathId.LocalPathId).Update( \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::TableName>(settings.GetTableName()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::Kind>(settings.Get##Kind().SerializeAsString()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::ScanSettings>(settings.GetScanSettings().SerializeAsString()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::NeedToBill>(settings.GetNeedToBill()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::TableDescription>(settings.GetTable().SerializeAsString()), \
                NIceDb::TUpdate<Schema::MigratedBackupSettings::NumberOfRetries>(settings.GetNumberOfRetries())); \
        } \
    }

    PERSIST_BACKUP_SETTINGS(YTSettings)
    PERSIST_BACKUP_SETTINGS(S3Settings)

#undef PERSIST_BACKUP_SETTINGS
}

void TSchemeShard::PersistCompletedBackupRestore(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& info, TTableInfo::TBackupRestoreResult::EKind kind) {
    TPathId pathId = txState.TargetPathId;

    if (IsLocalId(pathId)) {
        db.Table<Schema::CompletedBackups>().Key(pathId.LocalPathId, txId, info.CompletionDateTime).Update(
            NIceDb::TUpdate<Schema::CompletedBackups::TotalShardCount>(info.TotalShardCount),
            NIceDb::TUpdate<Schema::CompletedBackups::SuccessShardCount>(info.SuccessShardCount),
            NIceDb::TUpdate<Schema::CompletedBackups::StartTime>(info.StartDateTime),
            NIceDb::TUpdate<Schema::CompletedBackups::DataTotalSize>(txState.DataTotalSize),
            NIceDb::TUpdate<Schema::CompletedBackups::Kind>(static_cast<ui8>(kind)));
    } else {
        db.Table<Schema::MigratedCompletedBackups>()
            .Key(pathId.OwnerId, pathId.LocalPathId,
                 txId,
                 info.CompletionDateTime)
            .Update(NIceDb::TUpdate<Schema::MigratedCompletedBackups::TotalShardCount>(info.TotalShardCount),
                    NIceDb::TUpdate<Schema::MigratedCompletedBackups::SuccessShardCount>(info.SuccessShardCount),
                    NIceDb::TUpdate<Schema::MigratedCompletedBackups::StartTime>(info.StartDateTime),
                    NIceDb::TUpdate<Schema::MigratedCompletedBackups::DataTotalSize>(txState.DataTotalSize),
                    NIceDb::TUpdate<Schema::MigratedCompletedBackups::Kind>(static_cast<ui8>(kind)));
    }
}

void TSchemeShard::PersistCompletedBackup(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& backupInfo) {
    PersistCompletedBackupRestore(db, txId, txState, backupInfo, TTableInfo::TBackupRestoreResult::EKind::Backup);
}

void TSchemeShard::PersistCompletedRestore(NIceDb::TNiceDb& db, TTxId txId, const TTxState& txState, const TTableInfo::TBackupRestoreResult& restoreInfo) {
    PersistCompletedBackupRestore(db, txId, txState, restoreInfo, TTableInfo::TBackupRestoreResult::EKind::Restore);
}

void TSchemeShard::PersistBackupDone(NIceDb::TNiceDb& db, TPathId pathId) {
    if (IsLocalId(pathId)) {
        db.Table<Schema::BackupSettings>().Key(pathId.LocalPathId).Delete();
    }

    db.Table<Schema::MigratedBackupSettings>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistBlockStorePartition(NIceDb::TNiceDb& db, TPathId pathId, ui32 partitionId, TShardIdx shardIdx, ui64 version)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::BlockStorePartitions>().Key(pathId.LocalPathId, partitionId).Update(
        NIceDb::TUpdate<Schema::BlockStorePartitions::ShardIdx>(shardIdx.GetLocalId()),
        NIceDb::TUpdate<Schema::BlockStorePartitions::AlterVersion>(version));
}

void TSchemeShard::PersistBlockStoreVolume(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr volume)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString volumeConfig;
    Y_PROTOBUF_SUPPRESS_NODISCARD volume->VolumeConfig.SerializeToString(&volumeConfig);
    db.Table<Schema::BlockStoreVolumes>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::BlockStoreVolumes::VolumeConfig>(volumeConfig),
        NIceDb::TUpdate<Schema::BlockStoreVolumes::AlterVersion>(volume->AlterVersion),
        NIceDb::TUpdate<Schema::BlockStoreVolumes::MountToken>(volume->MountToken));
}

void TSchemeShard::PersistBlockStoreVolumeMountToken(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr volume)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::BlockStoreVolumes>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::BlockStoreVolumes::MountToken>(volume->MountToken),
        NIceDb::TUpdate<Schema::BlockStoreVolumes::TokenVersion>(volume->TokenVersion));
}

void TSchemeShard::PersistAddBlockStoreVolumeAlter(NIceDb::TNiceDb& db, TPathId pathId, const TBlockStoreVolumeInfo::TPtr volume)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString volumeConfig;
    Y_PROTOBUF_SUPPRESS_NODISCARD volume->VolumeConfig.SerializeToString(&volumeConfig);
    db.Table<Schema::BlockStoreVolumeAlters>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::BlockStoreVolumeAlters::VolumeConfig>(volumeConfig),
        NIceDb::TUpdate<Schema::BlockStoreVolumeAlters::AlterVersion>(volume->AlterVersion),
        NIceDb::TUpdate<Schema::BlockStoreVolumeAlters::PartitionCount>(volume->DefaultPartitionCount));
}

void TSchemeShard::PersistRemoveBlockStoreVolumeAlter(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::BlockStoreVolumeAlters>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistRemoveBlockStorePartition(NIceDb::TNiceDb& db, TPathId pathId, ui32 partitionId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::BlockStorePartitions>().Key(pathId.LocalPathId, partitionId).Delete();
}

void TSchemeShard::PersistRemoveBlockStoreVolume(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (BlockStoreVolumes.contains(pathId)) {
        auto volume = BlockStoreVolumes.at(pathId);

        if (volume->AlterData) {
            PersistRemoveBlockStoreVolumeAlter(db, pathId);
        }

        for (auto& shard : volume->Shards) {
            const auto& part = shard.second;
            PersistRemoveBlockStorePartition(db, pathId, part->PartitionId);
        }

        BlockStoreVolumes.erase(pathId);

        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::BlockStoreVolumes>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistFileStoreInfo(NIceDb::TNiceDb& db, TPathId pathId, const TFileStoreInfo::TPtr fs)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD fs->Config.SerializeToString(&config);

    db.Table<Schema::FileStoreInfos>()
        .Key(pathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::FileStoreInfos::Config>(config),
            NIceDb::TUpdate<Schema::FileStoreInfos::Version>(fs->Version));
}

void TSchemeShard::PersistAddFileStoreAlter(NIceDb::TNiceDb& db, TPathId pathId, const TFileStoreInfo::TPtr fs)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD fs->AlterConfig->SerializeToString(&config);

    db.Table<Schema::FileStoreAlters>()
        .Key(pathId.LocalPathId)
        .Update(
            NIceDb::TUpdate<Schema::FileStoreAlters::Config>(config),
            NIceDb::TUpdate<Schema::FileStoreAlters::Version>(fs->AlterVersion));
}

void TSchemeShard::PersistRemoveFileStoreAlter(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::FileStoreAlters>()
        .Key(pathId.LocalPathId)
        .Delete();
}

void TSchemeShard::PersistRemoveFileStoreInfo(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (FileStoreInfos.contains(pathId)) {
        auto fs = FileStoreInfos.at(pathId);

        if (fs->AlterConfig) {
            PersistRemoveFileStoreAlter(db, pathId);
        }

        FileStoreInfos.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    db.Table<Schema::FileStoreInfos>()
        .Key(pathId.LocalPathId)
        .Delete();
}

void TSchemeShard::PersistOlapStore(NIceDb::TNiceDb& db, TPathId pathId, const TOlapStoreInfo& storeInfo, bool isAlter)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serialized;
    TString serializedSharding;
    Y_ABORT_UNLESS(storeInfo.GetDescription().SerializeToString(&serialized));
    Y_ABORT_UNLESS(storeInfo.Sharding.SerializeToString(&serializedSharding));

    if (isAlter) {
        db.Table<Schema::OlapStoresAlters>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::OlapStoresAlters::AlterVersion>(storeInfo.GetAlterVersion()),
            NIceDb::TUpdate<Schema::OlapStoresAlters::Description>(serialized),
            NIceDb::TUpdate<Schema::OlapStoresAlters::Sharding>(serializedSharding));
        if (storeInfo.AlterBody) {
            TString serializedAlterBody;
            Y_ABORT_UNLESS(storeInfo.AlterBody->SerializeToString(&serializedAlterBody));
            db.Table<Schema::OlapStoresAlters>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::OlapStoresAlters::AlterBody>(serializedAlterBody));
        }
    } else {
        db.Table<Schema::OlapStores>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::OlapStores::AlterVersion>(storeInfo.GetAlterVersion()),
            NIceDb::TUpdate<Schema::OlapStores::Description>(serialized),
            NIceDb::TUpdate<Schema::OlapStores::Sharding>(serializedSharding));
    }
}

void TSchemeShard::PersistOlapStoreRemove(NIceDb::TNiceDb& db, TPathId pathId, bool isAlter)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (isAlter) {
        db.Table<Schema::OlapStoresAlters>().Key(pathId.LocalPathId).Delete();
        return;
    }

    if (!OlapStores.contains(pathId)) {
        return;
    }

    auto storeInfo = OlapStores.at(pathId);
    if (storeInfo->AlterData) {
        PersistOlapStoreAlterRemove(db, pathId);
    }

    db.Table<Schema::OlapStores>().Key(pathId.LocalPathId).Delete();
    OlapStores.erase(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistOlapStoreAlter(NIceDb::TNiceDb& db, TPathId pathId, const TOlapStoreInfo& storeInfo)
{
    PersistOlapStore(db, pathId, storeInfo, true);
}

void TSchemeShard::PersistOlapStoreAlterRemove(NIceDb::TNiceDb& db, TPathId pathId)
{
    PersistOlapStoreRemove(db, pathId, true);
}

void TSchemeShard::PersistColumnTable(NIceDb::TNiceDb& db, TPathId pathId, const TColumnTableInfo& tableInfo, bool isAlter)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serialized;
    TString serializedSharding;
    Y_ABORT_UNLESS(tableInfo.Description.SerializeToString(&serialized));
    Y_ABORT_UNLESS(tableInfo.Description.GetSharding().SerializeToString(&serializedSharding));

    if (isAlter) {
        db.Table<Schema::ColumnTablesAlters>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::ColumnTablesAlters::AlterVersion>(tableInfo.AlterVersion),
            NIceDb::TUpdate<Schema::ColumnTablesAlters::Description>(serialized),
            NIceDb::TUpdate<Schema::ColumnTablesAlters::Sharding>(serializedSharding));
        if (tableInfo.AlterBody) {
            TString serializedAlterBody;
            Y_ABORT_UNLESS(tableInfo.AlterBody->SerializeToString(&serializedAlterBody));
            db.Table<Schema::ColumnTablesAlters>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ColumnTablesAlters::AlterBody>(serializedAlterBody));
        }
        if (tableInfo.StandaloneSharding) {
            TString serializedOwnedShards;
            Y_ABORT_UNLESS(tableInfo.StandaloneSharding->SerializeToString(&serializedOwnedShards));
            db.Table<Schema::ColumnTablesAlters>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ColumnTablesAlters::StandaloneSharding>(serializedOwnedShards));
        }
    } else {
        db.Table<Schema::ColumnTables>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::ColumnTables::AlterVersion>(tableInfo.AlterVersion),
            NIceDb::TUpdate<Schema::ColumnTables::Description>(serialized),
            NIceDb::TUpdate<Schema::ColumnTables::Sharding>(serializedSharding));
        if (tableInfo.StandaloneSharding) {
            TString serializedOwnedShards;
            Y_ABORT_UNLESS(tableInfo.StandaloneSharding->SerializeToString(&serializedOwnedShards));
            db.Table<Schema::ColumnTables>().Key(pathId.LocalPathId).Update(
                NIceDb::TUpdate<Schema::ColumnTables::StandaloneSharding>(serializedOwnedShards));
        }
    }
}

void TSchemeShard::PersistColumnTableRemove(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));
    auto tablePtr = ColumnTables.at(pathId);
    if (!tablePtr) {
        return;
    }
    auto& tableInfo = *tablePtr;

    if (tableInfo.AlterData) {
        PersistColumnTableAlterRemove(db, pathId);
    }

    // Unlink table from olap store
    if (!tableInfo.IsStandalone() && tableInfo.GetOlapStorePathIdVerified()) {
        Y_ABORT_UNLESS(OlapStores.contains(tableInfo.GetOlapStorePathIdVerified()));
        auto storeInfo = OlapStores.at(tableInfo.GetOlapStorePathIdVerified());
        storeInfo->ColumnTablesUnderOperation.erase(pathId);
        storeInfo->ColumnTables.erase(pathId);
    }

    db.Table<Schema::ColumnTables>().Key(pathId.LocalPathId).Delete();
    ColumnTables.Drop(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistColumnTableAlter(NIceDb::TNiceDb& db, TPathId pathId, const TColumnTableInfo& tableInfo) {
    PersistColumnTable(db, pathId, tableInfo, true);
}

void TSchemeShard::PersistColumnTableAlterRemove(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::ColumnTablesAlters>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistSequence(NIceDb::TNiceDb& db, TPathId pathId, const TSequenceInfo& sequenceInfo)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    TString serializedSharding;
    Y_ABORT_UNLESS(sequenceInfo.Description.SerializeToString(&serializedDescription));
    Y_ABORT_UNLESS(sequenceInfo.Sharding.SerializeToString(&serializedSharding));

    db.Table<Schema::Sequences>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::Sequences::AlterVersion>(sequenceInfo.AlterVersion),
        NIceDb::TUpdate<Schema::Sequences::Description>(serializedDescription),
        NIceDb::TUpdate<Schema::Sequences::Sharding>(serializedSharding));
}

void TSchemeShard::PersistSequenceRemove(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (!Sequences.contains(pathId)) {
        return;
    }

    auto sequenceInfo = Sequences.at(pathId);
    if (sequenceInfo->AlterData) {
        PersistSequenceAlterRemove(db, pathId);
        sequenceInfo->AlterData = nullptr;
    }

    db.Table<Schema::Sequences>().Key(pathId.LocalPathId).Delete();
    Sequences.erase(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistSequenceAlter(NIceDb::TNiceDb& db, TPathId pathId, const TSequenceInfo& sequenceInfo)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    TString serializedSharding;
    Y_ABORT_UNLESS(sequenceInfo.Description.SerializeToString(&serializedDescription));
    Y_ABORT_UNLESS(sequenceInfo.Sharding.SerializeToString(&serializedSharding));

    db.Table<Schema::SequencesAlters>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::SequencesAlters::AlterVersion>(sequenceInfo.AlterVersion),
        NIceDb::TUpdate<Schema::SequencesAlters::Description>(serializedDescription),
        NIceDb::TUpdate<Schema::SequencesAlters::Sharding>(serializedSharding));
}

void TSchemeShard::PersistSequenceAlterRemove(NIceDb::TNiceDb& db, TPathId pathId)
{
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::SequencesAlters>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistReplication(NIceDb::TNiceDb& db, TPathId pathId, const TReplicationInfo& replicationInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    Y_ABORT_UNLESS(replicationInfo.Description.SerializeToString(&serializedDescription));

    db.Table<Schema::Replications>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::Replications::AlterVersion>(replicationInfo.AlterVersion),
        NIceDb::TUpdate<Schema::Replications::Description>(serializedDescription));
}

void TSchemeShard::PersistReplicationRemove(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    if (!Replications.contains(pathId)) {
        return;
    }

    auto replicationInfo = Replications.at(pathId);
    if (replicationInfo->AlterData) {
        replicationInfo->AlterData = nullptr;
        PersistReplicationAlterRemove(db, pathId);
    }

    Replications.erase(pathId);
    DecrementPathDbRefCount(pathId);
    db.Table<Schema::Replications>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistReplicationAlter(NIceDb::TNiceDb& db, TPathId pathId, const TReplicationInfo& replicationInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString serializedDescription;
    Y_ABORT_UNLESS(replicationInfo.Description.SerializeToString(&serializedDescription));

    db.Table<Schema::ReplicationsAlterData>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ReplicationsAlterData::AlterVersion>(replicationInfo.AlterVersion),
        NIceDb::TUpdate<Schema::ReplicationsAlterData::Description>(serializedDescription));
}

void TSchemeShard::PersistReplicationAlterRemove(NIceDb::TNiceDb& db, TPathId pathId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    db.Table<Schema::ReplicationsAlterData>().Key(pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistBlobDepot(NIceDb::TNiceDb& db, TPathId pathId, const TBlobDepotInfo& blobDepotInfo) {
    Y_ABORT_UNLESS(IsLocalId(pathId));

    TString description;
    const bool success = blobDepotInfo.Description.SerializeToString(&description);
    Y_ABORT_UNLESS(success);

    using T = Schema::BlobDepots;
    db.Table<T>().Key(pathId.LocalPathId).Update(
        NIceDb::TUpdate<T::AlterVersion>(blobDepotInfo.AlterVersion),
        NIceDb::TUpdate<T::Description>(description)
    );
}

void TSchemeShard::PersistKesusInfo(NIceDb::TNiceDb& db, TPathId pathId, const TKesusInfo::TPtr kesus)
{
    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD kesus->Config.SerializeToString(&config);

    if (IsLocalId((pathId))) {
        db.Table<Schema::KesusInfos>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::KesusInfos::Config>(config),
            NIceDb::TUpdate<Schema::KesusInfos::Version>(kesus->Version));
    } else {
        db.Table<Schema::MigratedKesusInfos>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedKesusInfos::Config>(config),
            NIceDb::TUpdate<Schema::MigratedKesusInfos::Version>(kesus->Version));
    }
}

void TSchemeShard::PersistKesusVersion(NIceDb::TNiceDb &db, TPathId pathId, const TKesusInfo::TPtr kesus) {
    if (IsLocalId((pathId))) {
        db.Table<Schema::KesusInfos>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::KesusInfos::Version>(kesus->Version));
    } else {
        db.Table<Schema::MigratedKesusInfos>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedKesusInfos::Version>(kesus->Version));
    }
}

void TSchemeShard::PersistAddKesusAlter(NIceDb::TNiceDb& db, TPathId pathId, const TKesusInfo::TPtr kesus)
{
    Y_ABORT_UNLESS(kesus->AlterConfig);
    Y_ABORT_UNLESS(kesus->AlterVersion);
    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD kesus->AlterConfig->SerializeToString(&config);

    if (IsLocalId((pathId))) {
        db.Table<Schema::KesusAlters>().Key(pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::KesusAlters::Config>(config),
            NIceDb::TUpdate<Schema::KesusAlters::Version>(kesus->AlterVersion));
    } else {
        db.Table<Schema::MigratedKesusAlters>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::MigratedKesusAlters::Config>(config),
            NIceDb::TUpdate<Schema::MigratedKesusAlters::Version>(kesus->AlterVersion));
    }
}

void TSchemeShard::PersistRemoveKesusAlter(NIceDb::TNiceDb& db, TPathId pathId)
{
    if (IsLocalId((pathId))) {
        db.Table<Schema::KesusAlters>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedKesusAlters>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

void TSchemeShard::PersistRemoveKesusInfo(NIceDb::TNiceDb& db, TPathId pathId)
{
    if (KesusInfos.contains(pathId)) {
        auto kesus = KesusInfos.at(pathId);

        if (kesus->AlterConfig) {
            PersistRemoveKesusAlter(db, pathId);
        }

        KesusInfos.erase(pathId);
        DecrementPathDbRefCount(pathId);
    }

    if (IsLocalId(pathId)) {
        db.Table<Schema::KesusInfos>().Key(pathId.LocalPathId).Delete();
    } else {
        db.Table<Schema::MigratedKesusInfos>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    }
}

void TSchemeShard::PersistRevertedMigration(NIceDb::TNiceDb& db, TPathId pathId, TTabletId abandonedSchemeShardId) {
    Y_ABORT_UNLESS(IsLocalId(pathId));
    db.Table<Schema::RevertedMigrations>().Key(pathId.LocalPathId, abandonedSchemeShardId).Update();
}

void TSchemeShard::PersistRemoveTable(NIceDb::TNiceDb& db, TPathId pathId, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    const TPathElement::TPtr path = PathsById.at(pathId);

    if (!Tables.contains(pathId)) {
        return;
    }
    const TTableInfo::TPtr tableInfo = Tables.at(pathId);

    auto clearHistory = [&](const TMap<TTxId, TTableInfo::TBackupRestoreResult>& history) {
        for (auto& bItem: history) {
            TTxId txId = bItem.first;
            const auto& result = bItem.second;

            for (auto& sItem: result.ShardStatuses) {
                auto shard = sItem.first;
                if (IsLocalId(shard)) {
                    db.Table<Schema::ShardBackupStatus>().Key(txId, shard.GetLocalId()).Delete();
                }
                db.Table<Schema::MigratedShardBackupStatus>().Key(txId, shard.GetOwnerId(), shard.GetLocalId()).Delete();
                db.Table<Schema::TxShardStatus>().Key(txId, shard.GetOwnerId(), shard.GetLocalId()).Delete();
            }

            if (IsLocalId(pathId)) {
                db.Table<Schema::CompletedBackups>().Key(pathId.LocalPathId, txId, result.CompletionDateTime).Delete();
            }
            db.Table<Schema::MigratedCompletedBackups>().Key(pathId.OwnerId, pathId.LocalPathId, txId, result.CompletionDateTime).Delete();
        }
    };

    clearHistory(tableInfo->BackupHistory);
    clearHistory(tableInfo->RestoreHistory);

    if (IsLocalId(pathId)) {
        db.Table<Schema::BackupSettings>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedBackupSettings>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    db.Table<Schema::RestoreTasks>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    if (tableInfo->AlterData) {
        auto& alterData = tableInfo->AlterData;
        for (auto& cItem: alterData->Columns) {
            if (pathId.OwnerId == TabletID()) {
                db.Table<Schema::ColumnAlters>().Key(pathId.LocalPathId, cItem.first).Delete();
            }
            db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, cItem.first).Delete();
        }
    }

    for (auto& cItem: tableInfo->Columns) {
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::Columns>().Key(pathId.LocalPathId, cItem.first).Delete();
        }
        db.Table<Schema::MigratedColumns>().Key(pathId.OwnerId, pathId.LocalPathId, cItem.first).Delete();
    }

    for (ui32 pNo = 0; pNo < tableInfo->GetPartitions().size(); ++pNo) {
        if (pathId.OwnerId == TabletID()) {
            db.Table<Schema::TablePartitions>().Key(pathId.LocalPathId, pNo).Delete();
        }
        db.Table<Schema::MigratedTablePartitions>().Key(pathId.OwnerId, pathId.LocalPathId, pNo).Delete();
        db.Table<Schema::TablePartitionStats>().Key(pathId.OwnerId, pathId.LocalPathId, pNo).Delete();

        const auto& shardInfo = tableInfo->GetPartitions().at(pNo);
        if (auto& lag = shardInfo.LastCondEraseLag) {
            TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].DecrementFor(lag->Seconds());
            lag.Clear();
        }
    }

    for (const auto& [_, childPathId]: path->GetChildren()) {
        Y_ABORT_UNLESS(PathsById.contains(childPathId));
        auto childPath = PathsById.at(childPathId);

        if (childPath->IsTableIndex()) {
            PersistRemoveTableIndex(db, childPathId);
        }
    }

    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::Tables>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedTables>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();

    if (tableInfo->IsTTLEnabled()) {
        TTLEnabledTables.erase(pathId);
        TabletCounters->Simple()[COUNTER_TTL_ENABLED_TABLE_COUNT].Sub(1);
    }

    if (TablesWithSnapshots.contains(pathId)) {
        const TTxId snapshotId = TablesWithSnapshots.at(pathId);
        PersistDropSnapshot(db, snapshotId, pathId);

        TablesWithSnapshots.erase(pathId);
        SnapshotTables.at(snapshotId).erase(pathId);
        if (SnapshotTables.at(snapshotId).empty()) {
            SnapshotTables.erase(snapshotId);
        }
        SnapshotsStepIds.erase(snapshotId);
    }

    if (!tableInfo->IsBackup && !tableInfo->IsShardsStatsDetached()) {
        auto subDomainId = ResolvePathIdForDomain(pathId);
        auto subDomainInfo = ResolveDomainInfo(pathId);
        subDomainInfo->AggrDiskSpaceUsage(this, TPartitionStats(), tableInfo->GetStats().Aggregated);
        if (subDomainInfo->CheckDiskSpaceQuotas(this)) {
            PersistSubDomainState(db, subDomainId, *subDomainInfo);
            // Publish is done in a separate transaction, so we may call this directly
            TDeque<TPathId> toPublish;
            toPublish.push_back(subDomainId);
            PublishToSchemeBoard(TTxId(), std::move(toPublish), ctx);
        }
    }

    // sanity check: by this time compaction queue and metrics must be updated already
    for (const auto& p: tableInfo->GetPartitions()) {
        ShardRemoved(p.ShardIdx);
    }

    Tables.erase(pathId);
    DecrementPathDbRefCount(pathId, "remove table");

    if (AppData()->FeatureFlags.GetEnableSystemViews()) {
        auto ev = MakeHolder<NSysView::TEvSysView::TEvRemoveTable>(GetDomainKey(pathId), pathId);
        Send(SysPartitionStatsCollector, ev.Release());
    }
}

void TSchemeShard::PersistRemoveTableIndex(NIceDb::TNiceDb &db, TPathId pathId)
{
    Y_ABORT_UNLESS(PathsById.contains(pathId));
    const TPathElement::TPtr path = PathsById.at(pathId);

    if (!Indexes.contains(pathId)) {
        return;
    }

    const TTableIndexInfo::TPtr index = Indexes.at(pathId);
    for (ui32 kNo = 0; kNo < index->IndexKeys.size(); ++kNo) {
        if (IsLocalId(pathId)) {
            db.Table<Schema::TableIndexKeys>().Key(pathId.LocalPathId, kNo).Delete();
        }
        db.Table<Schema::MigratedTableIndexKeys>().Key(pathId.OwnerId, pathId.LocalPathId, kNo).Delete();
    }

    for (ui32 dNo = 0; dNo < index->IndexDataColumns.size(); ++dNo) {
        db.Table<Schema::TableIndexDataColumns>().Key(pathId.OwnerId, pathId.LocalPathId, dNo).Delete();
    }

    if (index->AlterData) {
        auto alterData = index->AlterData;
        for (ui32 kNo = 0; kNo < alterData->IndexKeys.size(); ++kNo) {
            db.Table<Schema::TableIndexKeysAlterData>().Key(pathId.LocalPathId, kNo).Delete();
        }

        for (ui32 dNo = 0; dNo < alterData->IndexDataColumns.size(); ++dNo) {
            db.Table<Schema::TableIndexDataColumnsAlterData>().Key(pathId.OwnerId, pathId.LocalPathId, dNo).Delete();
        }

        db.Table<Schema::TableIndexAlterData>().Key(pathId.LocalPathId).Delete();
    }

    if (IsLocalId(pathId)) {
        db.Table<Schema::TableIndex>().Key(pathId.LocalPathId).Delete();
    }
    db.Table<Schema::MigratedTableIndex>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    Indexes.erase(pathId);
    DecrementPathDbRefCount(pathId);
}

void TSchemeShard::PersistAddTableShardPartitionConfig(NIceDb::TNiceDb& db, TShardIdx shardIdx, const NKikimrSchemeOp::TPartitionConfig& config)
{
    TString data;
    Y_PROTOBUF_SUPPRESS_NODISCARD config.SerializeToString(&data);

    if (IsLocalId(shardIdx)) {
        db.Table<Schema::TableShardPartitionConfigs>().Key(shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::TableShardPartitionConfigs::PartitionConfig>(data));
    } else {
        db.Table<Schema::MigratedTableShardPartitionConfigs>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::MigratedTableShardPartitionConfigs::PartitionConfig>(data));
    }
}

void TSchemeShard::PersistPublishingPath(NIceDb::TNiceDb& db, TTxId txId, TPathId pathId, ui64 version) {
    IncrementPathDbRefCount(pathId, "publish path");

    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::PublishingPaths>()
            .Key(txId, pathId.LocalPathId, version)
            .Update();
    } else {
        db.Table<Schema::MigratedPublishingPaths>()
            .Key(txId, pathId.OwnerId, pathId.LocalPathId, version)
            .Update();
    }
}

void TSchemeShard::PersistRemovePublishingPath(NIceDb::TNiceDb& db, TTxId txId, TPathId pathId, ui64 version) {
    DecrementPathDbRefCount(pathId, "remove publishing");

    if (pathId.OwnerId == TabletID()) {
        db.Table<Schema::PublishingPaths>()
            .Key(txId, pathId.LocalPathId, version)
            .Delete();
    }

    db.Table<Schema::MigratedPublishingPaths>()
        .Key(txId, pathId.OwnerId, pathId.LocalPathId, version)
        .Delete();
}

TTabletId TSchemeShard::GetGlobalHive(const TActorContext& ctx) const {
    return TTabletId(AppData(ctx)->DomainsInfo->GetHive());
}

TShardIdx TSchemeShard::GetShardIdx(TTabletId tabletId) const {
    const auto* pIdx = TabletIdToShardIdx.FindPtr(tabletId);
    if (!pIdx) {
        return InvalidShardIdx;
    }

    Y_ABORT_UNLESS(*pIdx != InvalidShardIdx);
    return *pIdx;
}

TShardIdx TSchemeShard::MustGetShardIdx(TTabletId tabletId) const {
    auto shardIdx = GetShardIdx(tabletId);
    Y_VERIFY_S(shardIdx != InvalidShardIdx, "Cannot find shard idx for tablet " << tabletId);
    return shardIdx;
}

TTabletTypes::EType TSchemeShard::GetTabletType(TTabletId tabletId) const {
    const auto* pIdx = TabletIdToShardIdx.FindPtr(tabletId);
    if (!pIdx) {
        return TTabletTypes::Unknown;
    }

    Y_ABORT_UNLESS(*pIdx != InvalidShardIdx);
    const auto* pShardInfo = ShardInfos.FindPtr(*pIdx);
    if (!pShardInfo) {
        return TTabletTypes::Unknown;
    }

    return pShardInfo->TabletType;
}

TTabletId TSchemeShard::ResolveHive(TPathId pathId, const TActorContext& ctx, EHiveSelection selection) const {
    if (!PathsById.contains(pathId)) {
        return GetGlobalHive(ctx);
    }

    TSubDomainInfo::TPtr subdomain = ResolveDomainInfo(pathId);

    // for paths inside subdomain and their shards we choose Hive according to that order: tenant, shared, global

    if (selection != EHiveSelection::IGNORE_TENANT && subdomain->GetTenantHiveID()) {
        return subdomain->GetTenantHiveID();
    }

    if (subdomain->GetSharedHive()) {
        return subdomain->GetSharedHive();
    }

    return GetGlobalHive(ctx);
}

TTabletId TSchemeShard::ResolveHive(TPathId pathId, const TActorContext& ctx) const {
    return ResolveHive(pathId, ctx, EHiveSelection::ANY);
}

TTabletId TSchemeShard::ResolveHive(TShardIdx shardIdx, const TActorContext& ctx) const {
    if (!ShardInfos.contains(shardIdx)) {
        return GetGlobalHive(ctx);
    }

    return ResolveHive(ShardInfos.at(shardIdx).PathId, ctx, EHiveSelection::ANY);
}

void TSchemeShard::DoShardsDeletion(const THashSet<TShardIdx>& shardIdxs, const TActorContext& ctx) {
    TMap<TTabletId, THashSet<TShardIdx>> shardsPerHive;
    for (TShardIdx shardIdx : shardIdxs) {
        TTabletId hiveToRequest = ResolveHive(shardIdx, ctx);

        shardsPerHive[hiveToRequest].emplace(shardIdx);
    }

    for (const auto& item: shardsPerHive) {
        const auto& hive = item.first;
        const auto& shards = item.second;
        ShardDeleter.SendDeleteRequests(hive, shards, ShardInfos, ctx);
    }
}

NKikimrSchemeOp::TPathVersion TSchemeShard::GetPathVersion(const TPath& path) const {
    NKikimrSchemeOp::TPathVersion result;

    const auto pathEl = path.Base();
    const auto pathId = pathEl->PathId;

    if (pathEl->Dropped()) {
        result.SetGeneralVersion(Max<ui64>());
        return result;
    }

    ui64 generalVersion = 0;

    if (pathEl->IsCreateFinished()) {
        switch(pathEl->PathType) {
            case NKikimrSchemeOp::EPathType::EPathTypeDir:
                if (pathEl->IsRoot() && IsDomainSchemeShard) {
                    TSubDomainInfo::TPtr subDomain = SubDomains.at(pathId);
                    Y_ABORT_UNLESS(SubDomains.contains(pathId));
                    result.SetSubDomainVersion(subDomain->GetVersion());
                    result.SetSecurityStateVersion(subDomain->GetSecurityStateVersion());
                    generalVersion += result.GetSubDomainVersion();
                    generalVersion += result.GetSecurityStateVersion();
                }
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeSubDomain:
            case NKikimrSchemeOp::EPathType::EPathTypeExtSubDomain: {
                Y_ABORT_UNLESS(!(pathEl->IsRoot() && IsDomainSchemeShard));

                Y_ABORT_UNLESS(SubDomains.contains(pathId));
                TSubDomainInfo::TPtr subDomain = SubDomains.at(pathId);
                result.SetSubDomainVersion(subDomain->GetVersion());
                result.SetSecurityStateVersion(subDomain->GetSecurityStateVersion());
                generalVersion += result.GetSubDomainVersion();
                generalVersion += result.GetSecurityStateVersion();

                if (ui64 version = subDomain->GetDomainStateVersion()) {
                    result.SetSubDomainStateVersion(version);
                    generalVersion += version;
                }
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeTable:
                Y_VERIFY_S(Tables.contains(pathId),
                           "no table with id: " << pathId << ", at schemeshard: " << SelfTabletId());
                result.SetTableSchemaVersion(Tables.at(pathId)->AlterVersion);
                generalVersion += result.GetTableSchemaVersion();

                result.SetTablePartitionVersion(Tables.at(pathId)->PartitioningVersion);
                generalVersion += result.GetTablePartitionVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup:
                Y_ABORT_UNLESS(Topics.contains(pathId));
                result.SetPQVersion(Topics.at(pathId)->AlterVersion);
                generalVersion += result.GetPQVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeBlockStoreVolume:
                Y_ABORT_UNLESS(BlockStoreVolumes.contains(pathId));
                result.SetBSVVersion(BlockStoreVolumes.at(pathId)->AlterVersion);
                generalVersion += result.GetBSVVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeFileStore:
                Y_ABORT_UNLESS(FileStoreInfos.contains(pathId));
                result.SetFileStoreVersion(FileStoreInfos.at(pathId)->Version);
                generalVersion += result.GetFileStoreVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeKesus:
                Y_ABORT_UNLESS(KesusInfos.contains(pathId));
                result.SetKesusVersion(KesusInfos.at(pathId)->Version);
                generalVersion += result.GetKesusVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeRtmrVolume:
                result.SetRTMRVersion(1);
                generalVersion += result.GetRTMRVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeSolomonVolume:
                Y_ABORT_UNLESS(SolomonVolumes.contains(pathId));
                result.SetSolomonVersion(SolomonVolumes.at(pathId)->Version);
                generalVersion += result.GetSolomonVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeTableIndex:
                Y_ABORT_UNLESS(Indexes.contains(pathId));
                result.SetTableIndexVersion(Indexes.at(pathId)->AlterVersion);
                generalVersion += result.GetTableIndexVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeColumnStore:
                Y_VERIFY_S(OlapStores.contains(pathId),
                           "no olap store with id: " << pathId << ", at schemeshard: " << SelfTabletId());
                result.SetColumnStoreVersion(OlapStores.at(pathId)->GetAlterVersion());
                generalVersion += result.GetColumnStoreVersion();
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeColumnTable: {
                Y_VERIFY_S(ColumnTables.contains(pathId),
                           "no olap table with id: " << pathId << ", at schemeshard: " << SelfTabletId());
                auto tableInfo = ColumnTables.at(pathId);

                result.SetColumnTableVersion(tableInfo->AlterVersion);
                generalVersion += result.GetColumnTableVersion();

                if (tableInfo->Description.HasSchema()) {
                    result.SetColumnTableSchemaVersion(tableInfo->Description.GetSchema().GetVersion());
                } else if (tableInfo->Description.HasSchemaPresetId() && tableInfo->GetOlapStorePathIdVerified()) {
                    Y_ABORT_UNLESS(OlapStores.contains(tableInfo->GetOlapStorePathIdVerified()));
                    auto& storeInfo = OlapStores.at(tableInfo->GetOlapStorePathIdVerified());
                    auto& preset = storeInfo->SchemaPresets.at(tableInfo->Description.GetSchemaPresetId());
                    result.SetColumnTableSchemaVersion(tableInfo->Description.GetSchemaPresetVersionAdj() + preset.GetVersion());
                } else {
                    result.SetColumnTableSchemaVersion(tableInfo->Description.GetSchemaPresetVersionAdj());
                }
                generalVersion += result.GetColumnTableSchemaVersion();

                if (tableInfo->Description.HasTtlSettings()) {
                    result.SetColumnTableTtlSettingsVersion(tableInfo->Description.GetTtlSettings().GetVersion());
                }
                generalVersion += result.GetColumnTableTtlSettingsVersion();

                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeCdcStream: {
                Y_ABORT_UNLESS(CdcStreams.contains(pathId));
                result.SetCdcStreamVersion(CdcStreams.at(pathId)->AlterVersion);
                generalVersion += result.GetCdcStreamVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeSequence: {
                auto it = Sequences.find(pathId);
                Y_ABORT_UNLESS(it != Sequences.end());
                result.SetSequenceVersion(it->second->AlterVersion);
                generalVersion += result.GetSequenceVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeReplication: {
                auto it = Replications.find(pathId);
                Y_ABORT_UNLESS(it != Replications.end());
                result.SetReplicationVersion(it->second->AlterVersion);
                generalVersion += result.GetReplicationVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeBlobDepot:
                if (const auto it = BlobDepots.find(pathId); it != BlobDepots.end()) {
                    const ui64 version = it->second->AlterVersion;
                    result.SetBlobDepotVersion(version);
                    generalVersion += version;
                } else {
                    Y_FAIL_S("BlobDepot for path " << pathId << " not found");
                }
                break;
            case NKikimrSchemeOp::EPathType::EPathTypeExternalTable: {
                auto it = ExternalTables.find(pathId);
                Y_ABORT_UNLESS(it != ExternalTables.end());
                result.SetExternalTableVersion(it->second->AlterVersion);
                generalVersion += result.GetExternalTableVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeExternalDataSource: {
                auto it = ExternalDataSources.find(pathId);
                Y_ABORT_UNLESS(it != ExternalDataSources.end());
                result.SetExternalDataSourceVersion(it->second->AlterVersion);
                generalVersion += result.GetExternalDataSourceVersion();
                break;
            }
            case NKikimrSchemeOp::EPathType::EPathTypeView: {
                auto it = Views.find(pathId);
                Y_ABORT_UNLESS(it != Views.end());
                result.SetViewVersion(it->second->AlterVersion);
                generalVersion += result.GetViewVersion();
                break;
            }

            case NKikimrSchemeOp::EPathType::EPathTypeInvalid: {
                Y_UNREACHABLE();
            }
        }
    }


    result.SetChildrenVersion(pathEl->DirAlterVersion); //not only childrens but also acl children's version increases it
    generalVersion += result.GetChildrenVersion();

    result.SetUserAttrsVersion(pathEl->UserAttrs->AlterVersion);
    generalVersion += result.GetUserAttrsVersion();

    result.SetACLVersion(pathEl->ACLVersion); // do not add ACL version to the generalVersion here
    result.SetEffectiveACLVersion(path.GetEffectiveACLVersion()); // ACL version is added to generalVersion here
    generalVersion += result.GetEffectiveACLVersion();

    result.SetGeneralVersion(generalVersion);

    return result;
}

ui64 TSchemeShard::GetAliveChildren(TPathElement::TPtr pathEl, const std::optional<TPathElement::EPathType>& type) const {
    if (!type) {
        return pathEl->GetAliveChildren();
    }

    ui64 count = 0;
    for (const auto& [_, pathId] : pathEl->GetChildren()) {
        Y_ABORT_UNLESS(PathsById.contains(pathId));
        auto childPath = PathsById.at(pathId);

        count += ui64(childPath->PathType == *type);
    }

    return count;
}

TActorId TSchemeShard::TPipeClientFactory::CreateClient(const TActorContext& ctx, ui64 tabletId, const NTabletPipe::TClientConfig& pipeConfig){
    auto clientId = Self->Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, pipeConfig));
    switch (Self->GetTabletType(TTabletId(tabletId))) {
        case ETabletType::SequenceShard: {
            // Every time we create a new pipe to sequenceshard we use a new round
            auto round = Self->NextRound();
            NTabletPipe::SendData(
                ctx.SelfID, clientId,
                new NSequenceShard::TEvSequenceShard::TEvMarkSchemeShardPipe(
                    Self->TabletID(),
                    round.Generation,
                    round.Round));
            break;
        }
        default: {
            // Other tablets don't need anything special
            break;
        }
    }
    return clientId;
}

TSchemeShard::TSchemeShard(const TActorId &tablet, TTabletStorageInfo *info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , AllowConditionalEraseOperations(1, 0, 1)
    , AllowServerlessStorageBilling(0, 0, 1)
    , DisablePublicationsOfDropping(0, 0, 1)
    , FillAllocatePQ(0, 0, 1)
    , SplitSettings()
    , IsReadOnlyMode(false)
    , ParentDomainLink(this)
    , SubDomainsLinks(this)
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(
        new NTabletPipe::TBoundedClientCacheConfig(),
        GetPipeClientConfig(),
        new TPipeClientFactory(this)))
    , PipeTracker(*PipeClientCache)
    , CompactionStarter(this)
    , BorrowedCompactionStarter(this)
    , BackgroundCleaningStarter(this)
    , ShardDeleter(info->TabletID)
    , TableStatsQueue(this,
            COUNTER_STATS_QUEUE_SIZE,
            COUNTER_STATS_WRITTEN,
            COUNTER_STATS_BATCH_LATENCY)
    , TopicStatsQueue(this,
            COUNTER_PQ_STATS_QUEUE_SIZE,
            COUNTER_PQ_STATS_WRITTEN,
            COUNTER_PQ_STATS_BATCH_LATENCY)
    , AllowDataColumnForIndexTable(0, 0, 1)
{
    TabletCountersPtr.Reset(new TProtobufTabletCounters<
                            ESimpleCounters_descriptor,
                            ECumulativeCounters_descriptor,
                            EPercentileCounters_descriptor,
                            ETxTypes_descriptor
                            >());
    TabletCounters = TabletCountersPtr.Get();

    SelfPinger = new TSelfPinger(SelfTabletId(), TabletCounters);
    BackgroundSessionsManager = std::make_shared<NKikimr::NOlap::NBackground::TSessionsManager>(std::make_shared<NBackground::TAdapter>(tablet, NKikimr::NOlap::TTabletId(TabletID()), *this));
}

const TDomainsInfo::TDomain& TSchemeShard::GetDomainDescription(const TActorContext &ctx) const {
    return *AppData(ctx)->DomainsInfo->GetDomain();
}

const NKikimrConfig::TDomainsConfig& TSchemeShard::GetDomainsConfig() {
    Y_ABORT_UNLESS(AppData());
    return AppData()->DomainsConfig;
}

NKikimrSubDomains::TProcessingParams TSchemeShard::CreateRootProcessingParams(const TActorContext &ctx) {
    const auto& domain = GetDomainDescription(ctx);

    Y_ABORT_UNLESS(domain.Coordinators.size());
    return ExtractProcessingParams(domain);
}

NTabletPipe::TClientConfig TSchemeShard::GetPipeClientConfig() {
    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {
        .MinRetryTime = TDuration::MilliSeconds(50),
        .MaxRetryTime = TDuration::Seconds(2),
    };
    return config;
}

void TSchemeShard::FillTableSchemaVersion(ui64 tableSchemaVersion, NKikimrSchemeOp::TTableDescription* tableDescr) const {
    tableDescr->SetTableSchemaVersion(tableSchemaVersion);
}

void TSchemeShard::BreakTabletAndRestart(const TActorContext &ctx) {
    Become(&TThis::BrokenState);
    ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
}

bool TSchemeShard::IsSchemeShardConfigured() const {
    Y_ABORT_UNLESS(InitState != TTenantInitState::InvalidState);
    return InitState == TTenantInitState::Done || InitState == TTenantInitState::ReadOnlyPreview;
}

void TSchemeShard::Die(const TActorContext &ctx) {
    ctx.Send(SchemeBoardPopulator, new TEvents::TEvPoisonPill());
    ctx.Send(TxAllocatorClient, new TEvents::TEvPoisonPill());
    ctx.Send(SysPartitionStatsCollector, new TEvents::TEvPoisonPill());

    if (TabletMigrator) {
        ctx.Send(TabletMigrator, new TEvents::TEvPoisonPill());
    }

    if (CdcStreamScanFinalizer) {
        ctx.Send(CdcStreamScanFinalizer, new TEvents::TEvPoisonPill());
    }

    IndexBuildPipes.Shutdown(ctx);
    CdcStreamScanPipes.Shutdown(ctx);
    ShardDeleter.Shutdown(ctx);
    ParentDomainLink.Shutdown(ctx);

    if (SAPipeClientId) {
        NTabletPipe::CloseClient(SelfId(), SAPipeClientId);
    }

    PipeClientCache->Detach(ctx);

    if (CompactionQueue)
        CompactionQueue->Shutdown(ctx);

    if (BorrowedCompactionQueue) {
        BorrowedCompactionQueue->Shutdown(ctx);
    }

    ClearTempTablesState();
    if (BackgroundCleaningQueue) {
        BackgroundCleaningQueue->Shutdown(ctx);
    }

    return IActor::Die(ctx);
}

void TSchemeShard::OnDetach(const TActorContext &ctx) {
    Die(ctx);
}

void TSchemeShard::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    Die(ctx);
}

void TSchemeShard::OnActivateExecutor(const TActorContext &ctx) {
    const TTabletId selfTabletId = SelfTabletId();

    const auto& selfDomain = GetDomainDescription(ctx);
    IsDomainSchemeShard = selfTabletId == TTabletId(selfDomain.SchemeRoot);
    InitState = TTenantInitState::Uninitialized;

    auto appData = AppData(ctx);
    Y_ABORT_UNLESS(appData);

    EnableBackgroundCompaction = appData->FeatureFlags.GetEnableBackgroundCompaction();
    EnableBackgroundCompactionServerless = appData->FeatureFlags.GetEnableBackgroundCompactionServerless();
    EnableBorrowedSplitCompaction = appData->FeatureFlags.GetEnableBorrowedSplitCompaction();
    EnableMoveIndex = appData->FeatureFlags.GetEnableMoveIndex();
    EnableAlterDatabaseCreateHiveFirst = appData->FeatureFlags.GetEnableAlterDatabaseCreateHiveFirst();
    EnablePQConfigTransactionsAtSchemeShard = appData->FeatureFlags.GetEnablePQConfigTransactionsAtSchemeShard();
    EnableStatistics = appData->FeatureFlags.GetEnableStatistics();
    EnableTablePgTypes = appData->FeatureFlags.GetEnableTablePgTypes();
    EnableServerlessExclusiveDynamicNodes = appData->FeatureFlags.GetEnableServerlessExclusiveDynamicNodes();
    EnableAddColumsWithDefaults = appData->FeatureFlags.GetEnableAddColumsWithDefaults();
    EnableReplaceIfExistsForExternalEntities = appData->FeatureFlags.GetEnableReplaceIfExistsForExternalEntities();
    EnableTempTables = appData->FeatureFlags.GetEnableTempTables();

    ConfigureCompactionQueues(appData->CompactionConfig, ctx);
    ConfigureStatsBatching(appData->SchemeShardConfig, ctx);
    ConfigureStatsOperations(appData->SchemeShardConfig, ctx);

    ConfigureBackgroundCleaningQueue(appData->BackgroundCleaningConfig, ctx);

    if (appData->ChannelProfiles) {
        ChannelProfiles = appData->ChannelProfiles;
    }

    appData->Icb->RegisterSharedControl(AllowConditionalEraseOperations, "SchemeShard_AllowConditionalEraseOperations");
    appData->Icb->RegisterSharedControl(DisablePublicationsOfDropping, "SchemeShard_DisablePublicationsOfDropping");
    appData->Icb->RegisterSharedControl(FillAllocatePQ, "SchemeShard_FillAllocatePQ");

    AllowDataColumnForIndexTable = appData->FeatureFlags.GetEnableDataColumnForIndexTable();
    appData->Icb->RegisterSharedControl(AllowDataColumnForIndexTable, "SchemeShard_AllowDataColumnForIndexTable");

    for (const auto& sid : appData->MeteringConfig.GetSystemBackupSIDs()) {
        SystemBackupSIDs.insert(sid);
    }

    AllowServerlessStorageBilling = appData->FeatureFlags.GetAllowServerlessStorageBillingForSchemeShard();
    appData->Icb->RegisterSharedControl(AllowServerlessStorageBilling, "SchemeShard_AllowServerlessStorageBilling");

    TxAllocatorClient = RegisterWithSameMailbox(CreateTxAllocatorClient(appData));

    SysPartitionStatsCollector = Register(NSysView::CreatePartitionStatsCollector().Release());

    SplitSettings.Register(appData->Icb);

    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    Execute(CreateTxInitSchema(), ctx);

    SubscribeConsoleConfigs(ctx);
}

// This is overriden as noop in order to activate the table only at the end of Init transaction
// when all the in-mem state has been populated
void TSchemeShard::DefaultSignalTabletActive(const TActorContext &ctx) {
    Y_UNUSED(ctx);
}

void TSchemeShard::Cleanup(const TActorContext &ctx) {
    Y_UNUSED(ctx);
}

void TSchemeShard::Enqueue(STFUNC_SIG) {
    Y_FAIL_S("No enqueue method implemented."
              << " unhandled event type: " << ev->GetTypeRewrite()
             << " event: " << ev->ToString());

}

void TSchemeShard::StateInit(STFUNC_SIG) {
    TRACE_EVENT(NKikimrServices::FLAT_TX_SCHEMESHARD);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvents::TEvUndelivered, Handle);

        //console configs
        HFuncTraced(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
        HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        HFunc(TEvPrivate::TEvConsoleConfigsTimeout, Handle);

    default:
        StateInitImpl(ev, SelfId());
    }
}

void TSchemeShard::StateConfigure(STFUNC_SIG) {
    SelfPinger->OnAnyEvent(this->ActorContext());

    TRACE_EVENT(NKikimrServices::FLAT_TX_SCHEMESHARD);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvents::TEvUndelivered, Handle);

        HFuncTraced(NKikimr::NOlap::NBackground::TEvExecuteGeneralLocalTransaction, Handle);
        HFuncTraced(NKikimr::NOlap::NBackground::TEvRemoveSession, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitRootShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitTenantSchemeShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvMigrateSchemeShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenantAsReadOnly, Handle);

        HFuncTraced(TEvSchemeShard::TEvMeasureSelfResponseTime, SelfPinger->Handle);
        HFuncTraced(TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime, SelfPinger->Handle);

        //operation initiate msg, must return error
        HFuncTraced(TEvSchemeShard::TEvModifySchemeTransaction, Handle);
        HFuncTraced(TEvSchemeShard::TEvDescribeScheme, Handle);
        HFuncTraced(TEvSchemeShard::TEvNotifyTxCompletion, Handle);
        HFuncTraced(TEvSchemeShard::TEvCancelTx, Handle);

        //pipes mgs
        HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerDisconnected, Handle);

        //console configs
        HFuncTraced(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
        HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        HFunc(TEvPrivate::TEvConsoleConfigsTimeout, Handle);

    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            ALOG_WARN(NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "StateConfigure:"
                           << " unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
        }
    }
}

void TSchemeShard::StateWork(STFUNC_SIG) {
    SelfPinger->OnAnyEvent(this->ActorContext());

    TRACE_EVENT(NKikimrServices::FLAT_TX_SCHEMESHARD);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvents::TEvUndelivered, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitRootShard, Handle);
        HFuncTraced(NKikimr::NOlap::NBackground::TEvExecuteGeneralLocalTransaction, Handle);
        HFuncTraced(NKikimr::NOlap::NBackground::TEvRemoveSession, Handle);

        HFuncTraced(TEvSchemeShard::TEvMeasureSelfResponseTime, SelfPinger->Handle);
        HFuncTraced(TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime, SelfPinger->Handle);

        //operation initiate msg
        HFuncTraced(TEvSchemeShard::TEvModifySchemeTransaction, Handle);
        HFuncTraced(TEvSchemeShard::TEvDescribeScheme, Handle);
        HFuncTraced(TEvSchemeShard::TEvNotifyTxCompletion, Handle);
        HFuncTraced(TEvSchemeShard::TEvCancelTx, Handle);

        //operation schedule msg
        HFuncTraced(TEvPrivate::TEvProgressOperation, Handle);

        //coordination distributed transactions msg
        HFuncTraced(TEvTxProcessing::TEvPlanStep, Handle);

        //operations managed msg
        HFuncTraced(TEvHive::TEvCreateTabletReply, Handle);
        IgnoreFunc(TEvHive::TEvTabletCreationResult);
        HFuncTraced(TEvHive::TEvAdoptTabletReply, Handle);
        HFuncTraced(TEvHive::TEvDeleteTabletReply, Handle);
        HFuncTraced(TEvHive::TEvDeleteOwnerTabletsReply, Handle);
        HFuncTraced(TEvHive::TEvUpdateTabletsObjectReply, Handle);
        HFuncTraced(TEvHive::TEvUpdateDomainReply, Handle);

        HFuncTraced(TEvDataShard::TEvProposeTransactionResult, Handle);
        HFuncTraced(TEvDataShard::TEvSchemaChanged, Handle);
        HFuncTraced(TEvDataShard::TEvStateChanged, Handle);
        HFuncTraced(TEvDataShard::TEvInitSplitMergeDestinationAck, Handle);
        HFuncTraced(TEvDataShard::TEvSplitAck, Handle);
        HFuncTraced(TEvDataShard::TEvSplitPartitioningChangedAck, Handle);
        HFuncTraced(TEvDataShard::TEvPeriodicTableStats, Handle);
        HFuncTraced(TEvPersQueue::TEvPeriodicTopicStats, Handle);
        HFuncTraced(TEvDataShard::TEvGetTableStatsResult, Handle);

        //
        HFuncTraced(TEvColumnShard::TEvProposeTransactionResult, Handle);
        HFuncTraced(NBackgroundTasks::TEvAddTaskResult, Handle);
        HFuncTraced(TEvColumnShard::TEvNotifyTxCompletionResult, Handle);

        // sequence shard
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvFreezeSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvRedirectSequenceResult, Handle);
        HFuncTraced(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult, Handle);

        // replication
        HFuncTraced(NReplication::TEvController::TEvCreateReplicationResult, Handle);
        HFuncTraced(NReplication::TEvController::TEvAlterReplicationResult, Handle);
        HFuncTraced(NReplication::TEvController::TEvDropReplicationResult, Handle);

        // conditional erase
        HFuncTraced(TEvPrivate::TEvRunConditionalErase, Handle);
        HFuncTraced(TEvDataShard::TEvConditionalEraseRowsResponse, Handle);

        HFuncTraced(TEvPrivate::TEvServerlessStorageBilling, Handle);

        HFuncTraced(NSysView::TEvSysView::TEvGetPartitionStats, Handle);

        HFuncTraced(TEvSubDomain::TEvConfigureStatus, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitTenantSchemeShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvInitTenantSchemeShardResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenantAsReadOnly, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenantAsReadOnlyResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenant, Handle);
        HFuncTraced(TEvSchemeShard::TEvPublishTenantResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvMigrateSchemeShardResult, Handle);
        HFuncTraced(TEvDataShard::TEvMigrateSchemeShardResponse, Handle);
        HFuncTraced(TEvDataShard::TEvCompactTableResult, Handle);
        HFuncTraced(TEvDataShard::TEvCompactBorrowedResult, Handle);

        HFuncTraced(TEvSchemeShard::TEvSyncTenantSchemeShard, Handle);
        HFuncTraced(TEvSchemeShard::TEvProcessingRequest, Handle);

        HFuncTraced(TEvSchemeShard::TEvUpdateTenantSchemeShard, Handle);

        HFuncTraced(NSchemeBoard::NSchemeshardEvents::TEvUpdateAck, Handle);

        HFuncTraced(TEvBlockStore::TEvUpdateVolumeConfigResponse, Handle);
        HFuncTraced(TEvFileStore::TEvUpdateConfigResponse, Handle);
        HFuncTraced(NKesus::TEvKesus::TEvSetConfigResult, Handle);
        HFuncTraced(TEvPersQueue::TEvDropTabletReply, Handle);
        HFuncTraced(TEvPersQueue::TEvUpdateConfigResponse, Handle);
        HFuncTraced(TEvPersQueue::TEvProposeTransactionResult, Handle);
        HFuncTraced(TEvBlobDepot::TEvApplyConfigResult, Handle);

        //pipes mgs
        HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerDisconnected, Handle);

        // namespace NExport {
        HFuncTraced(TEvExport::TEvCreateExportRequest, Handle);
        HFuncTraced(TEvExport::TEvGetExportRequest, Handle);
        HFuncTraced(TEvExport::TEvCancelExportRequest, Handle);
        HFuncTraced(TEvExport::TEvForgetExportRequest, Handle);
        HFuncTraced(TEvExport::TEvListExportsRequest, Handle);
        // } // NExport
        HFuncTraced(NBackground::TEvListRequest, Handle);

        // namespace NImport {
        HFuncTraced(TEvImport::TEvCreateImportRequest, Handle);
        HFuncTraced(TEvImport::TEvGetImportRequest, Handle);
        HFuncTraced(TEvImport::TEvCancelImportRequest, Handle);
        HFuncTraced(TEvImport::TEvForgetImportRequest, Handle);
        HFuncTraced(TEvImport::TEvListImportsRequest, Handle);
        HFuncTraced(TEvPrivate::TEvImportSchemeReady, Handle);
        // } // NImport

        // namespace NBackup {
        HFuncTraced(TEvBackup::TEvFetchBackupCollectionsRequest, Handle);
        HFuncTraced(TEvBackup::TEvListBackupCollectionsRequest, Handle);
        HFuncTraced(TEvBackup::TEvCreateBackupCollectionRequest, Handle);
        HFuncTraced(TEvBackup::TEvReadBackupCollectionRequest, Handle);
        HFuncTraced(TEvBackup::TEvUpdateBackupCollectionRequest, Handle);
        HFuncTraced(TEvBackup::TEvDeleteBackupCollectionRequest, Handle);
        // } // NBackup


        //namespace NIndexBuilder {
        HFuncTraced(TEvIndexBuilder::TEvCreateRequest, Handle);
        HFuncTraced(TEvIndexBuilder::TEvGetRequest, Handle);
        HFuncTraced(TEvIndexBuilder::TEvCancelRequest, Handle);
        HFuncTraced(TEvIndexBuilder::TEvForgetRequest, Handle);
        HFuncTraced(TEvIndexBuilder::TEvListRequest, Handle);
        HFuncTraced(TEvDataShard::TEvBuildIndexProgressResponse, Handle);
        HFuncTraced(TEvPrivate::TEvIndexBuildingMakeABill, Handle);
        // } // NIndexBuilder

        //namespace NCdcStreamScan {
        HFuncTraced(TEvPrivate::TEvRunCdcStreamScan, Handle);
        HFuncTraced(TEvDataShard::TEvCdcStreamScanResponse, Handle);
        // } // NCdcStreamScan

        // namespace NLongRunningCommon {
        HFuncTraced(TEvTxAllocatorClient::TEvAllocateResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
        HFuncTraced(TEvIndexBuilder::TEvCreateResponse, Handle);
        HFuncTraced(TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
        HFuncTraced(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        HFuncTraced(TEvSchemeShard::TEvCancelTxResult, Handle);
        HFuncTraced(TEvIndexBuilder::TEvCancelResponse, Handle);
        // } // NLongRunningCommon

        //console configs
        HFuncTraced(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
        HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        HFunc(TEvPrivate::TEvConsoleConfigsTimeout, Handle);

        HFuncTraced(TEvSchemeShard::TEvFindTabletSubDomainPathId, Handle);

        IgnoreFunc(TEvTxProxy::TEvProposeTransactionStatus);

        HFuncTraced(TEvPrivate::TEvCleanDroppedPaths, Handle);
        HFuncTraced(TEvPrivate::TEvCleanDroppedSubDomains, Handle);
        HFuncTraced(TEvPrivate::TEvSubscribeToShardDeletion, Handle);

        HFuncTraced(TEvPrivate::TEvPersistTableStats, Handle);
        HFuncTraced(TEvPrivate::TEvPersistTopicStats, Handle);

        HFuncTraced(TEvSchemeShard::TEvLogin, Handle);

        HFuncTraced(TEvPersQueue::TEvProposeTransactionAttachResult, Handle);

        HFuncTraced(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        HFuncTraced(TEvPrivate::TEvSendBaseStatsToSA, Handle);

        // for subscriptions on owners
        HFuncTraced(TEvInterconnect::TEvNodeDisconnected, Handle);
        HFuncTraced(TEvPrivate::TEvRetryNodeSubscribe, Handle);

    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            ALOG_WARN(NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "StateWork:"
                           << " unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
        }
        break;
    }
}

void TSchemeShard::BrokenState(STFUNC_SIG) {
    TRACE_EVENT(NKikimrServices::FLAT_TX_SCHEMESHARD);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvTablet::TEvTabletDead, HandleTabletDead);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            ALOG_WARN(NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "BrokenState:"
                           << " unhandled event type: " << ev->GetTypeRewrite()
                           << " event: " << ev->ToString());
        }
        break;
    }
}

void TSchemeShard::DeleteSplitOp(TOperationId operationId, TTxState& txState) {
    Y_ABORT_UNLESS(txState.ShardsInProgress.empty(), "All shards should have already completed their steps");

    TTableInfo::TPtr tableInfo = *Tables.FindPtr(txState.TargetPathId);
    Y_ABORT_UNLESS(tableInfo);
    tableInfo->FinishSplitMergeOp(operationId);
}

bool TSchemeShard::ShardIsUnderSplitMergeOp(const TShardIdx& idx) const {
    const TShardInfo* shardInfo = ShardInfos.FindPtr(idx);
    if (!shardInfo) {
        return false;
    }

    TTxId lastTxId = shardInfo->CurrentTxId;
    if (!lastTxId) {
        return false;
    }

    TTableInfo::TCPtr table = Tables.at(shardInfo->PathId);

    TOperationId lastOpId = TOperationId(lastTxId, 0);
    if (!TxInFlight.contains(lastOpId)) {
        Y_VERIFY_S(!table->IsShardInSplitMergeOp(idx),
                   "shardIdx: " << idx
                   << " pathId: " << shardInfo->PathId);
        return false;
    }

    Y_VERIFY_S(table->IsShardInSplitMergeOp(idx),
               "shardIdx: " << idx
               << " pathId: " << shardInfo->PathId
               << " lastOpId: " << lastOpId);
    return true;
}

TTxState &TSchemeShard::CreateTx(TOperationId opId, TTxState::ETxType txType, TPathId targetPath, TPathId sourcePath) {
    Y_VERIFY_S(!TxInFlight.contains(opId),
               "Trying to create duplicate Tx " << opId);
    TTxState& txState = TxInFlight[opId];
    txState = TTxState(txType, targetPath, sourcePath);
    TabletCounters->Simple()[TTxState::TxTypeInFlightCounter(txType)].Add(1);
    IncrementPathDbRefCount(targetPath, "transaction target path");
    if (sourcePath) {
        IncrementPathDbRefCount(sourcePath, "transaction source path");
    }
    return txState;
}

TTxState *TSchemeShard::FindTx(TOperationId opId) {
    TTxState* txState = TxInFlight.FindPtr(opId);
    return txState;
}

TTxState* TSchemeShard::FindTxSafe(TOperationId opId, const TTxState::ETxType& txType) {
    TTxState* txState = FindTx(opId);
    Y_ABORT_UNLESS(txState);
    Y_ABORT_UNLESS(txState->TxType == txType);
    return txState;
}

void TSchemeShard::RemoveTx(const TActorContext &ctx, NIceDb::TNiceDb &db, TOperationId opId, TTxState *txState) {
    if (!txState) {
        txState = TxInFlight.FindPtr(opId);
    }
    if (!txState) {
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RemoveTx for txid " << opId);
    auto pathId = txState->TargetPathId;

    PersistRemoveTx(db, opId, *txState);
    TabletCounters->Simple()[TTxState::TxTypeInFlightCounter(txState->TxType)].Sub(1);

    if (txState->IsItActuallyMerge()) {
        TabletCounters->Cumulative()[TTxState::TxTypeFinishedCounter(TTxState::TxMergeTablePartition)].Increment(1);
    } else {
        TabletCounters->Cumulative()[TTxState::TxTypeFinishedCounter(txState->TxType)].Increment(1);
    }

    DecrementPathDbRefCount(pathId, "remove txstate target path");
    if (txState->SourcePathId) {
        DecrementPathDbRefCount(txState->SourcePathId, "remove txstate source path");
    }

    TxInFlight.erase(opId); // must be called last, erases txState invalidating txState ptr
}

TMaybe<NKikimrSchemeOp::TPartitionConfig> TSchemeShard::GetTablePartitionConfigWithAlterData(TPathId pathId) const {
    Y_VERIFY_S(PathsById.contains(pathId), "Unknown pathId " << pathId);
    auto pTable = Tables.FindPtr(pathId);
    if (pTable) {
        TTableInfo::TPtr table = *pTable;
        if (table->AlterData) {
            return table->AlterData->PartitionConfigCompatible();
        }
        return table->PartitionConfig();
    }
    return Nothing();
}

void TSchemeShard::ExamineTreeVFS(TPathId nodeId, std::function<void (TPathElement::TPtr)> func, const TActorContext &ctx) {
    TPathElement::TPtr node = PathsById.at(nodeId);
    Y_ABORT_UNLESS(node);

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "ExamineTreeVFS visit path id " << nodeId <<
                " name: " << node->Name <<
                " type: " << NKikimrSchemeOp::EPathType_Name(node->PathType) <<
                " state: " << NKikimrSchemeOp::EPathState_Name(node->PathState) <<
                " stepDropped: " << node->StepDropped <<
                " droppedTxId: " << node->DropTxId <<
                " parent: " << node->ParentPathId);

    // node dropped and no hidden tx is in fly
    if (node->Dropped() && !Operations.contains(node->DropTxId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "ExamineTreeVFS skip path id " << nodeId);

        if (node->IsTable()) { //lets check indexes
            for (auto childrenIt: node->GetChildren()) {
                ExamineTreeVFS(childrenIt.second, func, ctx);
            }
        }
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "ExamineTreeVFS run path id: " << nodeId);

    func(node);

    for (auto childrenIt: node->GetChildren()) {
        ExamineTreeVFS(childrenIt.second, func, ctx);
    }
}

THashSet<TPathId> TSchemeShard::ListSubTree(TPathId subdomain_root, const TActorContext &ctx) {
    THashSet<TPathId> paths;

    auto savePath = [&] (TPathElement::TPtr node) {
        paths.insert(node->PathId);
    };

    ExamineTreeVFS(subdomain_root, savePath, ctx);

    return paths;
}

THashSet<TTxId> TSchemeShard::GetRelatedTransactions(const THashSet<TPathId> &paths, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    THashSet<TTxId> transactions;

    for (const auto& txInFly: TxInFlight) {
        TOperationId opId = txInFly.first;
        const TTxState& state = txInFly.second;

        if (!paths.contains(state.TargetPathId)) {
            continue;
        }

        transactions.insert(opId.GetTxId());
    }

    return transactions;
}

THashSet<TShardIdx> TSchemeShard::CollectAllShards(const THashSet<TPathId> &paths) const {
    THashSet<TShardIdx> shards;

    for (const auto& shardItem: ShardInfos) {
        auto idx = shardItem.first;
        const TShardInfo& info = shardItem.second;

        if (!paths.contains(info.PathId)) {
            continue;
        }

        shards.insert(idx);
    }

    for (const auto& pathId: paths) {
        Y_ABORT_UNLESS(PathsById.contains(pathId));
        TPathElement::TPtr path = PathsById.at(pathId);
        if (!path->IsSubDomainRoot()) {
            continue;
        }
        TSubDomainInfo::TPtr domainInfo = SubDomains.at(pathId);
        const auto& domainShards = domainInfo->GetInternalShards();
        shards.insert(domainShards.begin(), domainShards.end());
    }

    return shards;
}

void TSchemeShard::UncountNode(TPathElement::TPtr node) {
    const auto isBackupTable = IsBackupTable(node->PathId);

    if (node->IsDomainRoot()) {
        ResolveDomainInfo(node->ParentPathId)->DecPathsInside(1, isBackupTable);
    } else {
        ResolveDomainInfo(node)->DecPathsInside(1, isBackupTable);
    }
    PathsById.at(node->ParentPathId)->DecAliveChildren(1, isBackupTable);

    TabletCounters->Simple()[COUNTER_USER_ATTRIBUTES_COUNT].Sub(node->UserAttrs->Size());

    switch (node->PathType) {
    case TPathElement::EPathType::EPathTypeDir:
        TabletCounters->Simple()[COUNTER_DIR_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeRtmrVolume:
        TabletCounters->Simple()[COUNTER_RTMR_VOLUME_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeTable:
        TabletCounters->Simple()[COUNTER_TABLE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypePersQueueGroup:
        TabletCounters->Simple()[COUNTER_PQ_GROUP_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeSubDomain:
        TabletCounters->Simple()[COUNTER_SUB_DOMAIN_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeExtSubDomain:
        TabletCounters->Simple()[COUNTER_EXTSUB_DOMAIN_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeBlockStoreVolume:
        TabletCounters->Simple()[COUNTER_BLOCKSTORE_VOLUME_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeFileStore:
        TabletCounters->Simple()[COUNTER_FILESTORE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeKesus:
        TabletCounters->Simple()[COUNTER_KESUS_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeSolomonVolume:
        TabletCounters->Simple()[COUNTER_SOLOMON_VOLUME_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeTableIndex:
        TabletCounters->Simple()[COUNTER_TABLE_INDEXES_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeColumnStore:
    case TPathElement::EPathType::EPathTypeColumnTable:
        // TODO
        break;
    case TPathElement::EPathType::EPathTypeCdcStream:
        TabletCounters->Simple()[COUNTER_CDC_STREAMS_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeSequence:
        TabletCounters->Simple()[COUNTER_SEQUENCE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeReplication:
        TabletCounters->Simple()[COUNTER_REPLICATION_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeBlobDepot:
        TabletCounters->Simple()[COUNTER_BLOB_DEPOT_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeExternalTable:
        TabletCounters->Simple()[COUNTER_EXTERNAL_TABLE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeExternalDataSource:
        TabletCounters->Simple()[COUNTER_EXTERNAL_DATA_SOURCE_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeView:
        TabletCounters->Simple()[COUNTER_VIEW_COUNT].Sub(1);
        break;
    case TPathElement::EPathType::EPathTypeInvalid:
        Y_ABORT("impossible path type");
    }
}

void TSchemeShard::MarkAsMigrated(TPathElement::TPtr node, const TActorContext &ctx) {
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Mark as Migrated path id " << node->PathId);

    Y_ABORT_UNLESS(!node->Dropped());
    Y_VERIFY_S(PathsById.contains(ResolvePathIdForDomain(node)),
               "details:"
                   << " node->PathId: " << node->PathId
                   << ", node->DomainPathId: " << node->DomainPathId);

    Y_VERIFY_S(PathsById.at(ResolvePathIdForDomain(node))->IsExternalSubDomainRoot(),
               "details:"
                   << " pathId: " << ResolvePathIdForDomain(node)
                   << ", pathType: " << NKikimrSchemeOp::EPathType_Name(PathsById.at(ResolvePathIdForDomain(node))->PathType));

    node->PathState = TPathElement::EPathState::EPathStateMigrated;

    UncountNode(node);
}

void TSchemeShard::MarkAsDropping(TPathElement::TPtr node, TTxId txId, const TActorContext &ctx) {
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Mark as Dropping path id " << node->PathId <<
                " by tx: " << txId);
    if (!node->Dropped()) {
        node->PathState = TPathElement::EPathState::EPathStateDrop;
        node->DropTxId = txId;
    }
}

void TSchemeShard::MarkAsDropping(const THashSet<TPathId> &paths, TTxId txId, const TActorContext &ctx) {
    for (auto id: paths) {
        MarkAsDropping(PathsById.at(id), txId, ctx);
    }
}

void TSchemeShard::DropNode(TPathElement::TPtr node, TStepId step, TTxId txId, NIceDb::TNiceDb &db, const TActorContext &ctx) {
    Y_VERIFY_S(node->PathState == TPathElement::EPathState::EPathStateDrop
               || node->IsMigrated(),
               "path id: " << node->PathId <<
                   " type: " << NKikimrSchemeOp::EPathType_Name(node->PathType) <<
                   " state: " << NKikimrSchemeOp::EPathState_Name(node->PathState) <<
                   " txid: " << txId);

    Y_ABORT_UNLESS(node->DropTxId == txId || node->IsMigrated());

    if (!node->IsMigrated()) {
        UncountNode(node);
    }

    node->SetDropped(step, txId);
    PersistDropStep(db, node->PathId, node->StepDropped, TOperationId(node->DropTxId, 0));

    switch (node->PathType) {
        case TPathElement::EPathType::EPathTypeTable:
            PersistRemoveTable(db, node->PathId, ctx);
            break;
        case TPathElement::EPathType::EPathTypeTableIndex:
            PersistRemoveTableIndex(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypePersQueueGroup:
            PersistRemovePersQueueGroup(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeBlockStoreVolume:
            PersistRemoveBlockStoreVolume(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeFileStore:
            PersistRemoveFileStoreInfo(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeKesus:
            PersistRemoveKesusInfo(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeSolomonVolume:
            PersistRemoveSolomonVolume(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeColumnStore:
            PersistOlapStoreRemove(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeColumnTable:
            PersistColumnTableRemove(db, node->PathId);
            break;
        case TPathElement::EPathType::EPathTypeSubDomain:
        case TPathElement::EPathType::EPathTypeExtSubDomain:
            // N.B. we must not remove subdomains as part of a tree drop
            // SubDomains must be removed when there are no longer
            // any references to them, e.g. all shards have been deleted
            // and all operations have been completed.
            break;
        case TPathElement::EPathType::EPathTypeBlobDepot:
            Y_ABORT("not implemented");
        default:
            // not all path types support removal
            break;
    }

    PersistUserAttributes(db, node->PathId, node->UserAttrs, nullptr);
}

void TSchemeShard::DropPaths(const THashSet<TPathId> &paths, TStepId step, TTxId txId, NIceDb::TNiceDb &db, const TActorContext &ctx) {
    for (auto id: paths) {
        DropNode(PathsById.at(id), step, txId, db, ctx);
    }
}

TString TSchemeShard::FillBackupTxBody(TPathId pathId, const NKikimrSchemeOp::TBackupTask& task, ui32 shardNum, TMessageSeqNo seqNo) const
{
    NKikimrTxDataShard::TFlatSchemeTransaction tx;
    FillSeqNo(tx, seqNo);
    auto backup = tx.MutableBackup();
    backup->CopyFrom(task);
    backup->SetTableId(pathId.LocalPathId);
    backup->SetShardNum(shardNum);

    TString txBody;
    Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
    return txBody;
}

void TSchemeShard::Handle(TEvDataShard::TEvSchemaChanged::TPtr& ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvSchemaChanged"
                    << ", tabletId: " << TabletID()
                    << ", at schemeshard: " << TabletID()
                    << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    const auto tableId = TTabletId(ev->Get()->Record.GetOrigin());

    TActorId ackTo = ev->Get()->GetSource();

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDataShard::TEvSchemaChanged"
                   << " for unknown txId " <<  txId
                   << " message# " << ev->Get()->Record.DebugString());

        auto event = MakeHolder<TEvDataShard::TEvSchemaChangedResult>(ui64(txId));
        ctx.Send(ackTo, event.Release());
        return;
    }

    auto partId = Operations.at(txId)->FindRelatedPartByTabletId(tableId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDataShard::TEvSchemaChanged"
                   << " for unknown part in txId: " <<  txId
                   << " message# " << ev->Get()->Record.DebugString());

        auto event = MakeHolder<TEvDataShard::TEvSchemaChangedResult>(ui64(txId));
        ctx.Send(ackTo, event.Release());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvStateChanged::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvStateChanged"
                    << ", at schemeshard: " << TabletID()
                    << ", message: " << ev->Get()->Record.ShortDebugString());

    Execute(CreateTxShardStateChanged(ev), ctx);
}


void TSchemeShard::Handle(TEvDataShard::TEvInitSplitMergeDestinationAck::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetOperationCookie());
    const auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got InitSplitMergeDestinationAck"
                   << " for unknown txId " << txId
                   << " datashard " << tabletId);
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvSplitAck::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetOperationCookie());
    const auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got SplitAck"
                   << " for unknown txId " << txId
                   << " datashard " << tabletId);
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvSplitPartitioningChangedAck::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetOperationCookie());
    const auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSplitPartitioningChangedAck"
                   << " for unknown txId " << txId
                   << " datashard " << tabletId);
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvDescribeScheme::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxDescribeScheme(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvNotifyTxCompletion::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxNotifyTxCompletion(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvInitRootShard::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxInitRootCompatibility(ev), ctx);
}

void TSchemeShard::Handle(NKikimr::NOlap::NBackground::TEvRemoveSession::TPtr& ev, const TActorContext& ctx) {
    auto txRemove = BackgroundSessionsManager->TxRemove(ev->Get()->GetClassName(), ev->Get()->GetIdentifier());
    AFL_VERIFY(!!txRemove);
    Execute(txRemove.release(), ctx);
}

void TSchemeShard::Handle(NKikimr::NOlap::NBackground::TEvExecuteGeneralLocalTransaction::TPtr& ev, const TActorContext& ctx) {
    Execute(ev->Get()->ExtractTransaction().release(), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvInitTenantSchemeShard::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxInitTenantSchemeShard(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvModifySchemeTransaction::TPtr &ev, const TActorContext &ctx) {
    if (IsReadOnlyMode) {
        ui64 txId = ev->Get()->Record.GetTxId();
        ui64 selfId = TabletID();
        auto result = MakeHolder<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            NKikimrScheme::StatusReadOnly, txId, selfId, "Schema is in ReadOnly mode");

        ctx.Send(ev->Sender, result.Release());

        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Schema modification rejected because of ReadOnly mode"
                       << ", at tablet: " << selfId
                       << " txid: " << txId);
        return;
    }

    Execute(CreateTxOperationPropose(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvProcessingRequest::TPtr& ev, const TActorContext& ctx) {
    const auto processor = ev->Get()->RestoreProcessor();
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "TSchemeShard::Handle"
        << ", at schemeshard: " << TabletID()
        << ", processor: " << (processor ? processor->DebugString() : "nullptr"));
    if (processor) {
        NKikimrScheme::TEvProcessingResponse result;
        processor->Process(*this, result);
        ctx.Send(ev->Sender, new TEvSchemeShard::TEvProcessingResponse(result));
    } else {
        ctx.Send(ev->Sender, new TEvSchemeShard::TEvProcessingResponse("cannot restore processor: " + ev->Get()->Record.GetClassName()));
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvProgressOperation::TPtr &ev, const TActorContext &ctx) {
    const auto txId = TTxId(ev->Get()->TxId);
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPrivate::TEvProgressOperation"
                   << " for unknown txId " << txId);
        return;
    }

    Y_ABORT_UNLESS(ev->Get()->TxPartId != InvalidSubTxId);
    Execute(CreateTxOperationProgress(TOperationId(txId, ev->Get()->TxPartId)), ctx);
}

void TSchemeShard::Handle(TEvPersQueue::TEvProposeTransactionAttachResult::TPtr& ev, const TActorContext& ctx)
{
    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvProposeTransactionAttachResult"
                   << " for unknown txId: " << txId
                   << " message: " << ev->Get()->Record.ShortDebugString());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvProposeTransactionAttachResult but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
    const auto tabletId = TTabletId(ev->Get()->TabletId);
    const TActorId clientId = ev->Get()->ClientId;

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvClientConnected"
                    << ", tabletId: " << tabletId
                    << ", status: " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status)
                    << ", at schemeshard: " << TabletID());

    Y_ABORT_UNLESS(ev->Get()->Leader);

    if (PipeClientCache->OnConnect(ev)) {
        return; //all Ok
    }

    if (IndexBuildPipes.Has(clientId)) {
        Execute(CreatePipeRetry(IndexBuildPipes.GetOwnerId(clientId), IndexBuildPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (CdcStreamScanPipes.Has(clientId)) {
        Execute(CreatePipeRetry(CdcStreamScanPipes.GetOwnerId(clientId), CdcStreamScanPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (ShardDeleter.Has(tabletId, clientId)) {
        ShardDeleter.ResendDeleteRequests(TTabletId(ev->Get()->TabletId), ShardInfos, ctx);
        return;
    }

    if (ParentDomainLink.HasPipeTo(tabletId, clientId)) {
        ParentDomainLink.AtPipeError(ctx);
        return;
    }

    if (clientId == SAPipeClientId) {
        ConnectToSA();
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Failed to connect"
                   << ", to tablet: " << tabletId
                   << ", at schemeshard: " << TabletID());

    BorrowedCompactionHandleDisconnect(tabletId, clientId);
    ConditionalEraseHandleDisconnect(tabletId, clientId, ctx);
    RestartPipeTx(tabletId, ctx);
}

void TSchemeShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    Y_UNUSED(ctx);

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Pipe server connected"
                    << ", at tablet: " << ev->Get()->TabletId);
}

void TSchemeShard::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
    const auto tabletId = TTabletId(ev->Get()->TabletId);
    const TActorId clientId = ev->Get()->ClientId;

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Client pipe"
                    << ", to tablet: " << tabletId
                    << ", from:" << TabletID() << " is reset");

    PipeClientCache->OnDisconnect(ev);

    if (IndexBuildPipes.Has(clientId)) {
        Execute(CreatePipeRetry(IndexBuildPipes.GetOwnerId(clientId), IndexBuildPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (CdcStreamScanPipes.Has(clientId)) {
        Execute(CreatePipeRetry(CdcStreamScanPipes.GetOwnerId(clientId), CdcStreamScanPipes.GetTabletId(clientId)), ctx);
        return;
    }

    if (ShardDeleter.Has(tabletId, clientId)) {
        ShardDeleter.ResendDeleteRequests(tabletId, ShardInfos, ctx);
        return;
    }

    if (ParentDomainLink.HasPipeTo(tabletId, clientId)) {
        ParentDomainLink.AtPipeError(ctx);
        return;
    }

    if (clientId == SAPipeClientId) {
        ConnectToSA();
        return;
    }

    BorrowedCompactionHandleDisconnect(tabletId, clientId);
    ConditionalEraseHandleDisconnect(tabletId, clientId, ctx);
    RestartPipeTx(tabletId, ctx);
}

void TSchemeShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &, const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Server pipe is reset"
                    << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvSyncTenantSchemeShard::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle TEvSyncTenantSchemeShard"
                   << ", at schemeshard: " << TabletID()
                   << ", msg: " << record.DebugString());
    Y_VERIFY_S(IsDomainSchemeShard, "unexpected message: schemeshard: " << TabletID() << " mgs: " << record.DebugString());

    if (SubDomainsLinks.Sync(ev, ctx)) {
        Execute(CreateTxSyncTenant(TPathId(record.GetDomainSchemeShard(), record.GetDomainPathId())), ctx);
    }
}

void TSchemeShard::Handle(TEvSchemeShard::TEvUpdateTenantSchemeShard::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle TEvUpdateTenantSchemeShard"
                   << ", at schemeshard: " << TabletID()
                   << ", msg: " << record.ShortDebugString());
    Y_VERIFY_S(!IsDomainSchemeShard, "unexpected message: schemeshard: " << TabletID() << " mgs: " << record.DebugString());

    Execute(CreateTxUpdateTenant(ev), ctx);
}

void TSchemeShard::Handle(NSchemeBoard::NSchemeshardEvents::TEvUpdateAck::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle TEvUpdateAck"
                   << ", at schemeshard: " << TabletID()
                   << ", msg: " << record.ShortDebugString()
                   << ", cookie: " << ev->Cookie);

    const auto pathId = TPathId(ev->Get()->Record.GetPathOwnerId(), ev->Get()->Record.GetLocalPathId());
    if (DelayedInitTenantReply && DelayedInitTenantDestination && pathId == RootPathId()) {
        ctx.Send(DelayedInitTenantDestination, DelayedInitTenantReply.Release());
        DelayedInitTenantDestination = {};
    }

    const auto txId = TTxId(ev->Cookie);
    if (!txId) {
        // There was no txId, so we are not waiting for an ack
        return;
    }

    if (!Operations.contains(txId) && !Publications.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateAck"
                   << " for unknown txId " << txId
                   << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxAckPublishToSchemeBoard(ev), ctx);
}

void TSchemeShard::Handle(TEvTxProcessing::TEvPlanStep::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxOperationPlanStep(ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvCreateTabletReply::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvCreateTabletReply"
                << " at schemeshard: " << TabletID()
                << " message: " << ev->Get()->Record.ShortDebugString());

    auto shardIdx = TShardIdx(ev->Get()->Record.GetOwner(),
                              TLocalShardIdx(ev->Get()->Record.GetOwnerIdx()));

    if (!ShardInfos.contains(shardIdx)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvCreateTabletReply"
                   << " for unknown shard idx " <<  shardIdx
                   << " tabletId " << ev->Get()->Record.GetTabletID());
        return;
    }

    TShardInfo& shardInfo = ShardInfos[shardIdx];
    const auto txId = shardInfo.CurrentTxId;

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvCreateTabletReply"
                   << " for unknown txId: " << txId
                   << ", shardIdx: " << shardIdx
                   << ", tabletId: " << ev->Get()->Record.GetTabletID()
                   << ", at schemeshard: " << TabletID());
        return;
    }

    TSubTxId partId = Operations.at(txId)->FindRelatedPartByShardIdx(shardIdx, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvCreateTabletReply but partId in unknown"
                       << ", for txId: " << txId
                       << ", shardIdx: " << shardIdx
                       << ", tabletId: " << ev->Get()->Record.GetTabletID()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvAdoptTabletReply::TPtr &ev, const TActorContext &ctx) {
    auto shardIdx = MakeLocalId(TLocalShardIdx(ev->Get()->Record.GetOwnerIdx()));      // internal id

    if (!ShardInfos.contains(shardIdx)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvAdoptTabletReply"
                   << " for unknown shard idx " <<  shardIdx
                   << " tabletId " << ev->Get()->Record.GetTabletID());
        return;
    }

    TShardInfo& shardInfo = ShardInfos[shardIdx];
    const auto txId = shardInfo.CurrentTxId;

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvAdoptTabletReply"
                   << " for unknown txId " << txId
                   << " shardIdx " << shardIdx
                   << " tabletId " << ev->Get()->Record.GetTabletID());

        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvDeleteTabletReply::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Free tablet reply"
                    << ", message: " << ev->Get()->Record.ShortDebugString()
                    << ", at schemeshard: " << TabletID());

    Execute(CreateTxDeleteTabletReply(ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvDeleteOwnerTabletsReply::TPtr &ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Free owner tablets reply"
                    << ", message: " << record.ShortDebugString()
                    << ", at schemeshard: " << TabletID());

    const auto txId = TTxId(record.GetTxId());

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDeleteOwnerTabletsReply"
                       << " for unknown txId " << txId
                       << " ownerID " << record.GetOwner()
                       << " form hive " << record.GetOrigin()
                       << " at schemeshard " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, 0), ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvUpdateTabletsObjectReply::TPtr &ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Update tablets object reply"
                    << ", message: " << record.ShortDebugString()
                    << ", at schemeshard: " << TabletID());

    const auto txId = TTxId(record.GetTxId());
    const auto partId = TSubTxId(record.GetTxPartId());

    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateTabletsObjectReply"
                       << " for unknown txId " << txId
                       << " at schemeshard " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvHive::TEvUpdateDomainReply::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Update domain reply"
                    << ", message: " << record.ShortDebugString()
                    << ", at schemeshard: " << TabletID());

    const auto txId = TTxId(record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateDomainReply"
                       << " for unknown txId " << txId
                       << " at schemeshard " << TabletID());
        return;
    }

    const auto tabletId = TTabletId(record.GetOrigin());
    const auto partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvHive::TEvUpdateDomainReply but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvPersQueue::TEvDropTabletReply::TPtr &ev, const TActorContext &ctx) {

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvDropTabletReply"
                   << " for unknown txId " << txId
                   << ", message: " << ev->Get()->Record.ShortDebugString());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetTabletId());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvDropTabletReply but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvPersQueue::TEvUpdateConfigResponse::TPtr& ev, const TActorContext& ctx)
{
    const TTxId txId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvUpdateConfigResponse"
                   << " for unknown txId " << txId
                   << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }

    const TTabletId tabletId(ev->Get()->Record.GetOrigin());
    const TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateConfigResponse but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx)
{
    const TTxId txId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvPersQueue::TEvProposeTransactionResult"
                   << " for unknown txId " << txId
                   << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }

    const TTabletId tabletId(ev->Get()->Record.GetOrigin());
    const TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvProposeTransactionResult but partId is unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvBlobDepot::TEvApplyConfigResult::TPtr& ev, const TActorContext& ctx) {
    const TTxId txId(ev->Get()->Record.GetTxId());
    const TTabletId tabletId(ev->Get()->Record.GetTabletId());
    if (const auto it = Operations.find(txId); it == Operations.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
           "Got TEvBlobDepot::TEvApplyConfigResult"
           << " for unknown txId " << txId
           << " message " << ev->Get()->Record.ShortDebugString());
    } else if (const TSubTxId partId = it->second->FindRelatedPartByTabletId(tabletId, ctx); partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
           "Got TEvBlobDepot::TEvApplyConfigResult but partId is unknown"
               << ", for txId: " << txId
               << ", tabletId: " << tabletId
               << ", at schemeshard: " << TabletID());
    } else {
        Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
    }
}

void TSchemeShard::Handle(NBackgroundTasks::TEvAddTaskResult::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Handle NBackgroundTasks::TEvAddTaskResult"
        << ", at schemeshard: " << TabletID()
        << ", message: " << ev->Get()->GetDebugString());
    TOperationId id;
    if (!id.DeserializeFromString(ev->Get()->GetTaskId())) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Got NBackgroundTasks::TEvAddTaskResult cannot parse operation id in result"
            << ", message: " << ev->Get()->GetDebugString()
            << ", at schemeshard: " << TabletID());
        return;
    }
    if (!Operations.contains(id.GetTxId())) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Got NBackgroundTasks::TEvAddTaskResult for unknown txId, ignore it"
            << ", txId: " << id.SerializeToString()
            << ", message: " << ev->Get()->GetDebugString()
            << ", at schemeshard: " << TabletID());
        return;
    }
    if (!ev->Get()->IsSuccess()) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Got NBackgroundTasks::TEvAddTaskResult cannot execute"
            << ", txId: " << id.SerializeToString()
            << ", message: " << ev->Get()->GetDebugString()
            << ", at schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxOperationReply(id, ev), ctx);
}

void TSchemeShard::Handle(TEvColumnShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvProposeTransactionResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvColumnShard::TEvProposeTransactionResult for unknown txId, ignore it"
                       << ", txId: " << txId
                       << ", message: " << ev->Get()->Record.ShortDebugString()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvProposeTransactionResult but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvColumnShard::TEvNotifyTxCompletionResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvNotifyTxCompletionResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvColumnShard::TEvNotifyTxCompletionResult for unknown txId, ignore it"
                       << ", txId: " << txId
                       << ", message: " << ev->Get()->Record.ShortDebugString()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvNotifyTxCompletionResult but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvCreateSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvDropSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvUpdateSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvFreezeSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvFreezeSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvRestoreSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvRedirectSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvRedirectSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvGetSequenceResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    TTxId txId = TTxId(ev->Get()->Record.GetTxId());
    TSubTxId partId = TSubTxId(ev->Get()->Record.GetTxPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NReplication::TEvController::TEvCreateReplicationResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvCreateReplicationResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetOperationId().GetTxId());
    const auto partId = TSubTxId(ev->Get()->Record.GetOperationId().GetPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NReplication::TEvController::TEvAlterReplicationResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvAlterReplicationResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetOperationId().GetTxId());
    const auto partId = TSubTxId(ev->Get()->Record.GetOperationId().GetPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(NReplication::TEvController::TEvDropReplicationResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvDropReplicationResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetOperationId().GetTxId());
    const auto partId = TSubTxId(ev->Get()->Record.GetOperationId().GetPartId());
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvProposeTransactionResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Handle TEvProposeTransactionResult"
                << ", at schemeshard: " << TabletID()
                << ", message: " << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvDataShard::TEvProposeTransactionResult for unknown txId, ignore it"
                       << ", txId: " << txId
                       << ", message: " << ev->Get()->Record.ShortDebugString()
                       << ", at schemeshard: " << TabletID());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    TSubTxId partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvProposeTransactionResult but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }
    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvSubDomain::TEvConfigureStatus::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetOnTabletId());

    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSubDomain::TEvConfigureStatus,"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSubDomain::TEvConfigureStatus but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::Handle(TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvBlockStore::TEvUpdateVolumeConfigResponse"
                   << " for unknown txId " << txId
                   << " tabletId " << ev->Get()->Record.GetOrigin());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    auto partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvUpdateVolumeConfigResponse but partId in unknown"
                       << ", for txId: " << txId
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvFileStore::TEvUpdateConfigResponse::TPtr& ev, const TActorContext& ctx) {
    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    if (!Operations.contains(txId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Got TEvFileStore::TEvUpdateConfigResponse"
                << " for unknown txId " << txId
                << " tabletId " << ev->Get()->Record.GetOrigin());
        return;
    }

    auto tabletId = TTabletId(ev->Get()->Record.GetOrigin());
    auto partId = Operations.at(txId)->FindRelatedPartByTabletId(tabletId, ctx);
    if (partId == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Got TEvUpdateVolumeConfigResponse but partId in unknown"
                << ", for txId: " << txId
                << ", tabletId: " << tabletId
                << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(TOperationId(txId, partId), ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvInitTenantSchemeShardResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTenantSchemeShard());

    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvInitTenantSchemeShardResult"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSubDomain::TEvConfigureStatus but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvPublishTenantAsReadOnlyResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTenantSchemeShard());

    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvPublishTenantAsReadOnlyResult"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvPublishTenantAsReadOnlyResult but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvPublishTenantResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTenantSchemeShard());

    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvPublishTenantResult"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got TEvSchemeShard::TEvPublishTenantResult but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}


void TSchemeShard::Handle(NKesus::TEvKesus::TEvSetConfigResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTabletId());


    TOperationId opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got NKesus::TEvKesus::TEvSetConfigResult"
                       << " no route has found by tabletId " << tabletId
                       << " message " << ev->Get()->Record.ShortDebugString());
        return;
    }
    Y_ABORT_UNLESS(opId.GetTxId());

    if (opId.GetSubTxId() == InvalidSubTxId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Got NKesus::TEvKesus::TEvSetConfigResult but partId in unknown"
                       << ", for txId: " << opId.GetTxId()
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

TOperationId TSchemeShard::RouteIncoming(TTabletId tabletId, const TActorContext& ctx) {
    auto transactionIds = PipeTracker.FindTx(ui64(tabletId));

    Y_ABORT_UNLESS(transactionIds.size() <= 1);

    if (transactionIds.empty()) {
        return InvalidOperationId;
    }

    auto txId = TTxId(*transactionIds.begin());
    Y_ABORT_UNLESS(txId);

    if (!Operations.contains(txId)) {
        return InvalidOperationId;
    }

    TOperation::TPtr operation = Operations.at(txId);
    auto subTxId = operation->FindRelatedPartByTabletId(tabletId, ctx);
    return TOperationId(txId, subTxId);
}

void TSchemeShard::RestartPipeTx(TTabletId tabletId, const TActorContext& ctx) {
    for (auto item : PipeTracker.FindTx(ui64(tabletId))) {
        auto txId = TTxId(item);
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Transaction " << txId
                    << " reset current state at schemeshard " << TabletID()
                    << " because pipe to tablet " << tabletId
                    << " disconnected");

        if (!Operations.contains(txId)) {
            continue;
        }

        TOperation::TPtr operation = Operations.at(txId);

        if (!operation->PipeBindedMessages.contains(tabletId)) {
            for (ui64 pipeTrackerCookie : PipeTracker.FindCookies(ui64(txId), ui64(tabletId))) {
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "Pipe attached message is not found, ignore event"
                                << ", opId:" << TOperationId(txId, pipeTrackerCookie)
                                << ", tableId: " << tabletId
                                << ", at schemeshardId: " << TabletID());
            }
            continue;
        }

        for (auto& item: operation->PipeBindedMessages.at(tabletId)) {
            TPipeMessageId msgCookie = item.first;
            TOperation::TPreSerializedMessage& msg = item.second;

            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Pipe attached message is found and resent into the new pipe"
                            << ", opId:" << msg.OpId
                            << ", dst tableId: " << tabletId
                            << ", msg type: " << msg.Type
                            << ", msg cookie: " << msgCookie
                            << ", at schemeshardId: " << TabletID());

            PipeClientCache->Send(ctx, ui64(tabletId),  msg.Type, msg.Data, msgCookie.second);
        }
    }
}

void TSchemeShard::Handle(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx) {
    RenderHtmlPage(ev, ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvCancelTx::TPtr& ev, const TActorContext& ctx) {
    if (IsReadOnlyMode) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Ignoring message TEvSchemeShard::TEvCancelTx" <<
                   " reason# schemeshard in readonly" <<
                   " schemeshard# " << TabletID());
        return;
    }

    Execute(CreateTxOperationPropose(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvPublishTenantAsReadOnly::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxPublishTenantAsReadOnly(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvPublishTenant::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxPublishTenant(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvMigrateSchemeShard::TPtr &ev, const TActorContext &ctx) {
    if (InitState != TTenantInitState::Inprogress) {
        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Ignoring message TEvSchemeShard::TEvMigrateSchemeShard:" <<
                        " reason# schemeshard not in TTenantInitState::Inprogress state" <<
                        " state is " << (ui64) InitState <<
                        " schemeshard# " << TabletID());
        return;
    }
    Execute(CreateTxMigrate(ev), ctx);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvMigrateSchemeShardResult::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTenantSchemeShard());

    auto opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "unable to resolve operation by tabletID: " << tabletId <<
                       " ignore TEvSubDomain::TEvMigrateSchemeShardResult " <<
                       ", at schemeshard: " << TabletID());
        return;
    }

    Y_ABORT_UNLESS(opId.GetSubTxId() == FirstSubTxId);

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvMigrateSchemeShardResponse::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    auto tabletId = TTabletId(record.GetTabletId());

    auto opId = RouteIncoming(tabletId, ctx);
    if (!opId) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "unable to resolve operation by tabletID: " << tabletId <<
                       " ignore TEvDataShard::TEvMigrateSchemeShardResponse " <<
                       ", at schemeshard: " << TabletID());
        return;
    }

    Y_ABORT_UNLESS(opId.GetSubTxId() == FirstSubTxId);

    Execute(CreateTxOperationReply(opId, ev), ctx);
}

void TSchemeShard::ScheduleConditionalEraseRun(const TActorContext& ctx) {
    ctx.Schedule(TDuration::Minutes(1), new TEvPrivate::TEvRunConditionalErase());
}

void TSchemeShard::Handle(TEvPrivate::TEvRunConditionalErase::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle: TEvRunConditionalErase"
        << ", at schemeshard: " << TabletID());

    Execute(CreateTxRunConditionalErase(ev), ctx);
}

void TSchemeShard::ScheduleServerlessStorageBilling(const TActorContext &ctx) {
    ctx.Send(SelfId(), new TEvPrivate::TEvServerlessStorageBilling());
}

void TSchemeShard::Handle(TEvPrivate::TEvServerlessStorageBilling::TPtr &, const TActorContext &ctx) {
    Execute(CreateTxServerlessStorageBilling(), ctx);
}

void TSchemeShard::Handle(TEvDataShard::TEvConditionalEraseRowsResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    const TTabletId tabletId(record.GetTabletID());
    const TShardIdx shardIdx = GetShardIdx(tabletId);

    if (!ShardInfos.contains(shardIdx)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info"
            << ": tabletId: " << tabletId
            << ", at schemeshard: " << TabletID());
        return;
    }

    if (record.GetStatus() == NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ACCEPTED) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Conditional erase accepted"
            << ": tabletId: " << tabletId
            << ", at schemeshard: " << TabletID());
        return;
    }

    Execute(CreateTxScheduleConditionalErase(ev), ctx);
}

void TSchemeShard::Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Handle: TEvAllocateResult"
                   << ": Cookie# " << ev->Cookie
                   << ", at schemeshard: " << TabletID());

    const ui64 id = ev->Cookie;
    if (0 == id) {
        for (auto txId: ev->Get()->TxIds) {
            CachedTxIds.push_back(TTxId(txId));
        }
        return;
    } else if (Exports.contains(id)) {
        return Execute(CreateTxProgressExport(ev), ctx);
    } else if (Imports.contains(id)) {
        return Execute(CreateTxProgressImport(ev), ctx);
    } else if (IndexBuilds.contains(TIndexBuildId(id))) {
        return Execute(CreateTxReply(ev), ctx);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvAllocateResult: "
                   << " Cookie: " << id
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Handle: TEvModifySchemeTransactionResult"
                   << ": txId# " << ev->Get()->Record.GetTxId()
                   << ", status# " << ev->Get()->Record.GetStatus());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());

    if (TxIdToExport.contains(txId)) {
        return Execute(CreateTxProgressExport(ev), ctx);
    } else if (TxIdToImport.contains(txId)) {
        return Execute(CreateTxProgressImport(ev), ctx);
    } else if (TxIdToIndexBuilds.contains(txId)) {
        return Execute(CreateTxReply(ev), ctx);
    } else if (BackgroundCleaningTxs.contains(txId)) {
        return HandleBackgroundCleaningTransactionResult(ev);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvModifySchemeTransactionResult: "
                   << " txId: " << txId
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvIndexBuilder::TEvCreateResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Handle: TEvIndexBuilder::TEvCreateResponse"
                   << ": txId# " << ev->Get()->Record.GetTxId()
                   << ", status# " << ev->Get()->Record.GetStatus());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());

    if (TxIdToImport.contains(txId)) {
        return Execute(CreateTxProgressImport(ev), ctx);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvIndexBuilder::TEvCreateResponse: "
                   << " txId: " << txId
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {
    // just ignore
}

void TSchemeShard::Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle: TEvNotifyTxCompletionResult"
                   << ": txId# " << ev->Get()->Record.GetTxId());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const auto txId = TTxId(ev->Get()->Record.GetTxId());
    bool executed = false;

    if (TxIdToExport.contains(txId) || TxIdToDependentExport.contains(txId)) {
        Execute(CreateTxProgressExport(txId), ctx);
        executed = true;
    }
    if (TxIdToImport.contains(txId)) {
        Execute(CreateTxProgressImport(txId), ctx);
        executed = true;
    }
    if (TxIdToIndexBuilds.contains(txId)) {
        Execute(CreateTxReply(txId), ctx);
        executed = true;
    }
    if (BackgroundCleaningTxs.contains(txId)) {
        HandleBackgroundCleaningCompletionResult(txId);
        executed = true;
    }

    if (executed) {
        return;
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvNotifyTxCompletionResult: "
                   << " txId: " << txId
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvCancelTxResult::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle: TEvCancelTxResult"
                   << ": Cookie: " << ev->Cookie
                   << ", at schemeshard: " << TabletID());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const ui64 id = ev->Cookie;
    if (Exports.contains(id)) {
        return Execute(CreateTxCancelExportAck(ev), ctx);
    } else if (Imports.contains(id)) {
        return Execute(CreateTxCancelImportAck(ev), ctx);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvCancelTxResult"
                   << ": Cookie: " << id
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::Handle(TEvIndexBuilder::TEvCancelResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Handle: TEvIndexBuilder::TEvCancelResponse"
                   << ": Cookie: " << ev->Cookie
                   << ", at schemeshard: " << TabletID());
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Message:\n" << ev->Get()->Record.ShortDebugString());

    const ui64 id = ev->Cookie;
    if (Imports.contains(id)) {
        return Execute(CreateTxCancelImportAck(ev), ctx);
    }

    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "no able to determine destination for message TEvIndexBuilder::TEvCancelResponse"
                   << ": Cookie: " << id
                   << ", at schemeshard: " << TabletID());
}

void TSchemeShard::FillSeqNo(NKikimrTxDataShard::TFlatSchemeTransaction& tx, TMessageSeqNo seqNo) {
    tx.MutableSeqNo()->SetGeneration(seqNo.Generation);
    tx.MutableSeqNo()->SetRound(seqNo.Round);
}

void TSchemeShard::FillSeqNo(NKikimrTxColumnShard::TSchemaTxBody& tx, TMessageSeqNo seqNo) {
    tx.MutableSeqNo()->SetGeneration(seqNo.Generation);
    tx.MutableSeqNo()->SetRound(seqNo.Round);
}

TString TSchemeShard::FillAlterTableTxBody(TPathId pathId, TShardIdx shardIdx, TMessageSeqNo seqNo) const {
    Y_VERIFY_S(Tables.contains(pathId), "Unknown table " << pathId);
    Y_VERIFY_S(PathsById.contains(pathId), "Unknown path " << pathId);

    TPathElement::TPtr path = PathsById.at(pathId);
    TTableInfo::TPtr tableInfo = Tables.at(pathId);
    TTableInfo::TAlterDataPtr alterData = tableInfo->AlterData;

    Y_VERIFY_S(alterData, "No alter data for table " << pathId);

    NKikimrTxDataShard::TFlatSchemeTransaction tx;
    FillSeqNo(tx, seqNo);
    auto proto = tx.MutableAlterTable();
    FillTableSchemaVersion(alterData->AlterVersion, proto);
    proto->SetName(path->Name);

    proto->SetId_Deprecated(pathId.LocalPathId);
    PathIdFromPathId(pathId, proto->MutablePathId());

    for (const auto& col : alterData->Columns) {
        const TTableInfo::TColumn& colInfo = col.second;
        if (colInfo.IsDropped()) {
            auto descr = proto->AddDropColumns();
            descr->SetName(colInfo.Name);
            descr->SetId(colInfo.Id);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(colInfo.PType, colInfo.PTypeMod);
            descr->SetTypeId(columnType.TypeId);
            if (columnType.TypeInfo) {
                *descr->MutableTypeInfo() = *columnType.TypeInfo;
            }
        } else {
            auto descr = proto->AddColumns();
            descr->SetName(colInfo.Name);
            descr->SetId(colInfo.Id);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(colInfo.PType, colInfo.PTypeMod);
            descr->SetTypeId(columnType.TypeId);
            if (columnType.TypeInfo) {
                *descr->MutableTypeInfo() = *columnType.TypeInfo;
            }
            descr->SetFamily(colInfo.Family);
        }
    }

    for (ui32 keyId : alterData->KeyColumnIds) {
        proto->AddKeyColumnIds(keyId);
    }

    proto->MutablePartitionConfig()->CopyFrom(alterData->PartitionConfigCompatible());

    if (auto* patch = tableInfo->PerShardPartitionConfig.FindPtr(shardIdx)) {
        ApplyPartitionConfigStoragePatch(
            *proto->MutablePartitionConfig(),
            *patch);
    }

    TString txBody;
    Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
    return txBody;
}

bool TSchemeShard::FillSplitPartitioning(TVector<TString>& rangeEnds, const TConstArrayRef<NScheme::TTypeInfo>& keyColTypes,
                                             const::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TSplitBoundary> &boundaries,
                                             TString& errStr) {
    for (int i = 0; i < boundaries.size(); ++i) {
        // Convert split boundary to serialized range end
        auto& boundary = boundaries.Get(i);
        TVector<TCell> rangeEnd;
        TSerializedCellVec prefix;
        TVector<TString> memoryOwner;
        if (boundary.HasSerializedKeyPrefix()) {
            prefix.Parse(boundary.GetSerializedKeyPrefix());
            rangeEnd = TVector<TCell>(prefix.GetCells().begin(), prefix.GetCells().end());
        } else if (!NMiniKQL::CellsFromTuple(nullptr, boundary.GetKeyPrefix(), keyColTypes, {}, false, rangeEnd, errStr, memoryOwner)) {
            errStr = Sprintf("Error at split boundary %d: %s", i, errStr.data());
            return false;
        }
        rangeEnd.resize(keyColTypes.size());     // Extend with NULLs
        rangeEnds.push_back(TSerializedCellVec::Serialize(rangeEnd));
    }
    return true;
}

void TSchemeShard::ApplyPartitionConfigStoragePatch(
        NKikimrSchemeOp::TPartitionConfig& config,
        const NKikimrSchemeOp::TPartitionConfig& patch) const
{
    THashMap<ui32, ui32> familyRooms;
    for (const auto& family : patch.GetColumnFamilies()) {
        familyRooms[family.GetId()] = family.GetRoom();
    }

    // Patch column families
    for (size_t i = 0; i < config.ColumnFamiliesSize(); ++i) {
        auto& family = *config.MutableColumnFamilies(i);
        auto it = familyRooms.find(family.GetId());
        if (it != familyRooms.end()) {
            family.SetRoom(it->second);
        } else {
            family.ClearRoom();
        }
    }

    // Copy storage rooms as is
    config.ClearStorageRooms();
    if (patch.StorageRoomsSize()) {
        config.MutableStorageRooms()->CopyFrom(patch.GetStorageRooms());
    }
}

std::optional<TTempTableInfo> TSchemeShard::ResolveTempTableInfo(const TPathId& pathId) {
    auto path = TPath::Init(pathId, this);
    if (!path) {
        return std::nullopt;
    }
    TTempTableInfo info;
    info.Name = path.LeafName();
    info.WorkingDir = path.Parent().PathString();

    TTableInfo::TPtr table = Tables.at(path.Base()->PathId);
    if (!table) {
        return std::nullopt;
    }
    if (!table->IsTemporary) {
        return std::nullopt;
    }

    info.OwnerActorId = table->OwnerActorId;
    return info;
}

// Fills CreateTable transaction for datashard with the specified range
void TSchemeShard::FillTableDescriptionForShardIdx(
        TPathId tableId, TShardIdx shardIdx, NKikimrSchemeOp::TTableDescription* tableDescr,
        TString rangeBegin, TString rangeEnd,
        bool rangeBeginInclusive, bool rangeEndInclusive, bool newTable)
{
    Y_VERIFY_S(Tables.contains(tableId), "Unknown table id " << tableId);
    const TTableInfo::TPtr tinfo = Tables.at(tableId);
    TPathElement::TPtr pinfo = *PathsById.FindPtr(tableId);

    TVector<ui32> keyColumnIds = tinfo->FillDescriptionCache(pinfo);
    if (!tinfo->TableDescription.HasPath()) {
        tinfo->TableDescription.SetPath(PathToString(pinfo));
    }
    tableDescr->CopyFrom(tinfo->TableDescription);

    if (rangeBegin.empty()) {
        // First partition starts with <NULL, NULL, ..., NULL> key
        TVector<TCell> nullKey(keyColumnIds.size());
        rangeBegin = TSerializedCellVec::Serialize(nullKey);
    }

    tableDescr->SetPartitionRangeBegin(std::move(rangeBegin));
    tableDescr->SetPartitionRangeEnd(std::move(rangeEnd));
    tableDescr->SetPartitionRangeBeginIsInclusive(rangeBeginInclusive);
    tableDescr->SetPartitionRangeEndIsInclusive(rangeEndInclusive);

    // Patch partition config for new-style shards
    if (const auto* patch = tinfo->PerShardPartitionConfig.FindPtr(shardIdx)) {
        ApplyPartitionConfigStoragePatch(
            *tableDescr->MutablePartitionConfig(),
            *patch);
    }

    if (tinfo->IsBackup) {
        tableDescr->SetIsBackup(true);
    }

    if (tinfo->HasReplicationConfig()) {
        tableDescr->MutableReplicationConfig()->CopyFrom(tinfo->ReplicationConfig());
    }

    if (AppData()->DisableRichTableDescriptionForTest) {
        return;
    }

    // Fill indexes & cdc streams (if any)
    for (const auto& child : pinfo->GetChildren()) {
        const auto& childName = child.first;
        const auto& childPathId = child.second;

        Y_ABORT_UNLESS(PathsById.contains(childPathId));
        auto childPath = PathsById.at(childPathId);

        if (childPath->Dropped() || childPath->PlannedToDrop()) {
            continue;
        }

        switch (childPath->PathType) {
            case NKikimrSchemeOp::EPathTypeTableIndex: {
                Y_ABORT_UNLESS(Indexes.contains(childPathId));
                auto info = Indexes.at(childPathId);
                DescribeTableIndex(childPathId, childName, newTable ? info->AlterData : info, *tableDescr->MutableTableIndexes()->Add());
                break;
            }

            case NKikimrSchemeOp::EPathTypeCdcStream: {
                Y_VERIFY_S(CdcStreams.contains(childPathId), "Cdc stream not found"
                    << ": pathId# " << childPathId
                    << ", name# " << childName);
                auto info = CdcStreams.at(childPathId);
                DescribeCdcStream(childPathId, childName, info, *tableDescr->MutableCdcStreams()->Add());
                break;
            }

            case NKikimrSchemeOp::EPathTypeSequence: {
                Y_VERIFY_S(Sequences.contains(childPathId), "Sequence not found"
                    << ": path#d# " << childPathId
                    << ", name# " << childName);
                auto info = Sequences.at(childPathId);
                DescribeSequence(childPathId, childName, info, *tableDescr->MutableSequences()->Add());
                break;
            }

            default:
                Y_FAIL_S("Unexpected table's child"
                    << ": tableId# " << tableId
                    << ", childId# " << childPathId
                    << ", childName# " << childName
                    << ", childType# " << static_cast<ui32>(childPath->PathType));
        }
    }
}

// Fills CreateTable transaction that is sent to datashards
void TSchemeShard::FillTableDescription(TPathId tableId, ui32 partitionIdx, ui64 schemaVersion,
    NKikimrSchemeOp::TTableDescription* tableDescr)
{
    Y_VERIFY_S(Tables.contains(tableId), "Unknown table id " << tableId);
    const TTableInfo::TPtr tinfo = Tables.at(tableId);

    TString rangeBegin = (partitionIdx != 0)
        ? tinfo->GetPartitions()[partitionIdx-1].EndOfRange
        : TString();
    TString rangeEnd = tinfo->GetPartitions()[partitionIdx].EndOfRange;

    // For uniform partitioning we include range start and exclude range end
    FillTableDescriptionForShardIdx(
        tableId,
        tinfo->GetPartitions()[partitionIdx].ShardIdx,
        tableDescr,
        std::move(rangeBegin),
        std::move(rangeEnd),
        true /* rangeBeginInclusive */, false /* rangeEndInclusive */, true /* newTable */);
    FillTableSchemaVersion(schemaVersion, tableDescr);
}

bool TSchemeShard::FillUniformPartitioning(TVector<TString>& rangeEnds, ui32 keySize, NScheme::TTypeInfo firstKeyColType, ui32 partitionCount, const NScheme::TTypeRegistry* typeRegistry, TString& errStr) {
    Y_UNUSED(typeRegistry);
    if (partitionCount > 1) {
        // RangeEnd key will have first cell with non-NULL value and rest of the cells with NULLs
        TVector<TCell> rangeEnd(keySize);
        ui64 maxVal = 0;
        ui32 valSz = 0;

        // Check that first key column has integer type
        auto typeId = firstKeyColType.GetTypeId();
        switch(typeId) {
        case NScheme::NTypeIds::Uint32:
            maxVal = Max<ui32>();
            valSz = 4;
            break;
        case NScheme::NTypeIds::Uint64:
            maxVal = Max<ui64>();
            valSz = 8;
            break;
        case NScheme::NTypeIds::Uuid: {
            maxVal = Max<ui64>();
            valSz = 16;
            char buffer[16] = {};

            for (ui32 i = 1; i < partitionCount; ++i) {
                ui64 val = maxVal * (double(i) / partitionCount);
                // Make sure most significant byte is at the start of the byte buffer for UUID comparison.
                val = HostToInet(val);
                WriteUnaligned<ui64>(buffer, val);
                rangeEnd[0] = TCell(buffer, valSz);
                rangeEnds.push_back(TSerializedCellVec::Serialize(rangeEnd));
            }

            return true;
        }
        default:
            errStr = TStringBuilder() << "Unsupported first key column type " << NScheme::TypeName(firstKeyColType) << ", only Uint32 and Uint64 are supported";
            return false;
        }

        // Generate range boundaries
        for (ui32 i = 1; i < partitionCount; ++i) {
            ui64 val = maxVal * (double(i)/partitionCount);
            rangeEnd[0] = TCell((const char*)&val, valSz);
            rangeEnds.push_back(TSerializedCellVec::Serialize(rangeEnd));
        }
    }
    return true;
}

void TSchemeShard::SetPartitioning(TPathId pathId, const std::vector<TShardIdx>& partitioning) {
    if (AppData()->FeatureFlags.GetEnableSystemViews()) {
        TVector<std::pair<ui64, ui64>> shardIndices;
        shardIndices.reserve(partitioning.size());
        for (auto& shardIdx : partitioning) {
            shardIndices.emplace_back(ui64(shardIdx.GetOwnerId()), ui64(shardIdx.GetLocalId()));
        }

        auto path = TPath::Init(pathId, this);
        auto ev = MakeHolder<NSysView::TEvSysView::TEvSetPartitioning>(GetDomainKey(pathId), pathId, path.PathString());
        ev->ShardIndices.swap(shardIndices);
        Send(SysPartitionStatsCollector, ev.Release());
    }
}

void TSchemeShard::SetPartitioning(TPathId pathId, TOlapStoreInfo::TPtr storeInfo) {
    SetPartitioning(pathId, storeInfo->GetColumnShards());
}

void TSchemeShard::SetPartitioning(TPathId pathId, TColumnTableInfo::TPtr tableInfo) {
    SetPartitioning(pathId, tableInfo->BuildOwnedColumnShardsVerified());
}

void TSchemeShard::SetPartitioning(TPathId pathId, TTableInfo::TPtr tableInfo, TVector<TTableShardInfo>&& newPartitioning) {
    if (AppData()->FeatureFlags.GetEnableSystemViews()) {
        TVector<std::pair<ui64, ui64>> shardIndices;
        shardIndices.reserve(newPartitioning.size());
        for (auto& info : newPartitioning) {
            shardIndices.push_back(
                std::make_pair(ui64(info.ShardIdx.GetOwnerId()), ui64(info.ShardIdx.GetLocalId())));
        }

        auto path = TPath::Init(pathId, this);
        auto ev = MakeHolder<NSysView::TEvSysView::TEvSetPartitioning>(GetDomainKey(pathId), pathId, path.PathString());
        ev->ShardIndices.swap(shardIndices);
        Send(SysPartitionStatsCollector, ev.Release());
    }

    if (!tableInfo->IsBackup) {
        // partitions updated:
        // 1. We need to remove some parts from compaction queues.
        // 2. We need to add new ones to the queues. Since we can safely enqueue already
        // enqueued parts, we simple enqueue each part in newPartitioning
        THashSet<TShardIdx> newPartitioningSet;
        newPartitioningSet.reserve(newPartitioning.size());
        const auto& oldPartitioning = tableInfo->GetPartitions();

        for (const auto& p: newPartitioning) {
            if (!oldPartitioning.empty())
                newPartitioningSet.insert(p.ShardIdx);

            const auto& partitionStats = tableInfo->GetStats().PartitionStats;
            auto it = partitionStats.find(p.ShardIdx);
            if (it != partitionStats.end()) {
                EnqueueBackgroundCompaction(p.ShardIdx, it->second);
                UpdateShardMetrics(p.ShardIdx, it->second);
            }
        }

        for (const auto& p: oldPartitioning) {
            if (!newPartitioningSet.contains(p.ShardIdx)) {
                // note that queues might not contain the shard
                ShardRemoved(p.ShardIdx);
            }
        }
    }

    tableInfo->SetPartitioning(std::move(newPartitioning));
}

void TSchemeShard::FillAsyncIndexInfo(const TPathId& tableId, NKikimrTxDataShard::TFlatSchemeTransaction& tx) {
    Y_ABORT_UNLESS(PathsById.contains(tableId));

    auto parent = TPath::Init(tableId, this).Parent();
    Y_ABORT_UNLESS(parent.IsResolved());

    if (!parent.Base()->IsTableIndex()) {
        return;
    }

    Y_ABORT_UNLESS(Indexes.contains(parent.Base()->PathId));
    auto index = Indexes.at(parent.Base()->PathId);

    if (index->Type == TTableIndexInfo::EType::EIndexTypeGlobalAsync) {
        tx.MutableAsyncIndexInfo();
    }
}

bool TSchemeShard::ReadSysValue(NIceDb::TNiceDb &db, ui64 sysTag, TString &value, TString defValue) {
    auto sysParamsRowset = db.Table<Schema::SysParams>().Key(sysTag).Select<Schema::SysParams::Value>();
    if (!sysParamsRowset.IsReady()) {
        return false;
    }

    if (!sysParamsRowset.IsValid()) {
        value = defValue;
        return true;
    }

    value = sysParamsRowset.GetValue<Schema::SysParams::Value>();
    return true;
}

bool TSchemeShard::ReadSysValue(NIceDb::TNiceDb &db, ui64 sysTag, ui64 &value, ui64 defVal) {
    auto sysParamsRowset = db.Table<Schema::SysParams>().Key(sysTag).Select<Schema::SysParams::Value>();
    if (!sysParamsRowset.IsReady()) {
        return false;
    }

    if (!sysParamsRowset.IsValid()) {
        value = defVal;
        return true;
    }

    TString rawValue = sysParamsRowset.GetValue<Schema::SysParams::Value>(); \
    value = FromString<ui64>(rawValue);

    return true;
}

void TSchemeShard::SubscribeConsoleConfigs(const TActorContext &ctx) {
    ctx.Send(
        NConsole::MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
            (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem,
            (ui32)NKikimrConsole::TConfigItem::CompactionConfigItem,
            (ui32)NKikimrConsole::TConfigItem::SchemeShardConfigItem,
            (ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem,
            (ui32)NKikimrConsole::TConfigItem::QueryServiceConfigItem,
        }),
        IEventHandle::FlagTrackDelivery
    );
    ctx.Schedule(TDuration::Seconds(15), new TEvPrivate::TEvConsoleConfigsTimeout);
}

void TSchemeShard::Handle(TEvPrivate::TEvConsoleConfigsTimeout::TPtr&, const TActorContext& ctx) {
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot get console configs");
    LoadTableProfiles(nullptr, ctx);
}

void TSchemeShard::Handle(TEvents::TEvUndelivered::TPtr& ev, const TActorContext& ctx) {
    if (CheckOwnerUndelivered(ev)) {
        return;
    }
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Cannot subscribe to console configs");
    LoadTableProfiles(nullptr, ctx);
}

void TSchemeShard::ApplyConsoleConfigs(const NKikimrConfig::TAppConfig& appConfig, const TActorContext& ctx) {
    if (appConfig.HasFeatureFlags()) {
        ApplyConsoleConfigs(appConfig.GetFeatureFlags(), ctx);
    }

    if (appConfig.HasCompactionConfig()) {
        const auto& compactionConfig = appConfig.GetCompactionConfig();
        ConfigureCompactionQueues(compactionConfig, ctx);
    }

    if (appConfig.HasBackgroundCleaningConfig()) {
        const auto& backgroundCleaningConfig = appConfig.GetBackgroundCleaningConfig();
        ConfigureBackgroundCleaningQueue(backgroundCleaningConfig, ctx);
    }

    if (appConfig.HasSchemeShardConfig()) {
        const auto& schemeShardConfig = appConfig.GetSchemeShardConfig();
        ConfigureStatsBatching(schemeShardConfig, ctx);
        ConfigureStatsOperations(schemeShardConfig, ctx);
    }

    if (appConfig.HasTableProfilesConfig()) {
        LoadTableProfiles(&appConfig.GetTableProfilesConfig(), ctx);
    } else {
        LoadTableProfiles(nullptr, ctx);
    }

    if (appConfig.HasQueryServiceConfig()) {
        const auto& hostnamePatterns = appConfig.GetQueryServiceConfig().GetHostnamePatterns();
        ExternalSourceFactory = NExternalSource::CreateExternalSourceFactory(
            std::vector<TString>(hostnamePatterns.begin(), hostnamePatterns.end()),
            appConfig.GetQueryServiceConfig().GetS3().GetGeneratorPathsLimit()
        );
    }

    if (IsSchemeShardConfigured()) {
        StartStopCompactionQueues();
        if (BackgroundCleaningQueue) {
            BackgroundCleaningQueue->Start();
        }
    }
}

void TSchemeShard::ApplyConsoleConfigs(const NKikimrConfig::TFeatureFlags& featureFlags, const TActorContext& ctx) {
    if (featureFlags.GetAllowServerlessStorageBillingForSchemeShard() != (bool)AllowServerlessStorageBilling) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "ApplyConsoleConfigs: AllowServerlessStorageBillingForSchemeShard has been changed"
                     << ", schemeshardId: " << SelfTabletId()
                     << ", old: " << (bool)AllowServerlessStorageBilling
                     << ", new: " << (bool)featureFlags.GetAllowServerlessStorageBillingForSchemeShard());
        AllowServerlessStorageBilling = (i64)featureFlags.GetAllowServerlessStorageBillingForSchemeShard();
    }

    EnableBackgroundCompaction = featureFlags.GetEnableBackgroundCompaction();
    EnableBackgroundCompactionServerless = featureFlags.GetEnableBackgroundCompactionServerless();
    EnableBorrowedSplitCompaction = featureFlags.GetEnableBorrowedSplitCompaction();
    EnableMoveIndex = featureFlags.GetEnableMoveIndex();
    EnableAlterDatabaseCreateHiveFirst = featureFlags.GetEnableAlterDatabaseCreateHiveFirst();
    EnablePQConfigTransactionsAtSchemeShard = featureFlags.GetEnablePQConfigTransactionsAtSchemeShard();
    EnableStatistics = featureFlags.GetEnableStatistics();
    EnableTablePgTypes = featureFlags.GetEnableTablePgTypes();
    EnableServerlessExclusiveDynamicNodes = featureFlags.GetEnableServerlessExclusiveDynamicNodes();
    EnableAddColumsWithDefaults = featureFlags.GetEnableAddColumsWithDefaults();
    EnableTempTables = featureFlags.GetEnableTempTables();
    EnableReplaceIfExistsForExternalEntities = featureFlags.GetEnableReplaceIfExistsForExternalEntities();
}

void TSchemeShard::ConfigureStatsBatching(const NKikimrConfig::TSchemeShardConfig& config, const TActorContext& ctx) {
    StatsBatchTimeout = TDuration::MilliSeconds(config.GetStatsBatchTimeoutMs());
    StatsMaxBatchSize = config.GetStatsMaxBatchSize();
    StatsMaxExecuteTime = TDuration::MilliSeconds(config.GetStatsMaxExecuteMs());
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "StatsBatching config: StatsBatchTimeout# " << StatsBatchTimeout
                 << ", StatsMaxBatchSize# " << StatsMaxBatchSize
                 << ", StatsMaxExecuteTime# " << StatsMaxExecuteTime);
}

void TSchemeShard::ConfigureStatsOperations(const NKikimrConfig::TSchemeShardConfig& config, const TActorContext& ctx) {
    for (const auto& operationConfig: config.GetInFlightCounterConfig()) {
        ui32 limit = operationConfig.GetInFlightLimit();
        auto txState = TTxState::ConvertToTxType(operationConfig.GetType());
        InFlightLimits[txState] = limit;
    }

    if (InFlightLimits.empty()) {
        NKikimrConfig::TSchemeShardConfig_TInFlightCounterConfig inFlightCounterConfig;
        auto defaultInFlightLimit = inFlightCounterConfig.GetInFlightLimit();
        InFlightLimits[TTxState::ETxType::TxSplitTablePartition] = defaultInFlightLimit;
        InFlightLimits[TTxState::ETxType::TxMergeTablePartition] = defaultInFlightLimit;
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "OperationsProcessing config: using default configuration");
    }

    for (auto it = InFlightLimits.begin(); it != InFlightLimits.end(); ++it) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "OperationsProcessing config: type " << TTxState::TypeName(it->first)
                    << ", limit " << it->second);
    }
}

void TSchemeShard::ConfigureCompactionQueues(
    const NKikimrConfig::TCompactionConfig& compactionConfig,
    const TActorContext &ctx)
{
    if (compactionConfig.HasBackgroundCompactionConfig()) {
        ConfigureBackgroundCompactionQueue(compactionConfig.GetBackgroundCompactionConfig(), ctx);
    } else {
        ConfigureBackgroundCompactionQueue(NKikimrConfig::TCompactionConfig::TBackgroundCompactionConfig(), ctx);
    }

    if (compactionConfig.HasBorrowedCompactionConfig()) {
        ConfigureBorrowedCompactionQueue(compactionConfig.GetBorrowedCompactionConfig(), ctx);
    } else {
        ConfigureBorrowedCompactionQueue(NKikimrConfig::TCompactionConfig::TBorrowedCompactionConfig(), ctx);
    }
}

void TSchemeShard::ConfigureBackgroundCompactionQueue(
    const NKikimrConfig::TCompactionConfig::TBackgroundCompactionConfig& config,
    const TActorContext &ctx)
{
    // note that we use TCompactionQueueImpl::TConfig
    // instead of its base NOperationQueue::TConfig
    TCompactionQueueImpl::TConfig queueConfig;
    queueConfig.SearchHeightThreshold = config.GetSearchHeightThreshold();
    queueConfig.RowDeletesThreshold = config.GetRowDeletesThreshold();
    queueConfig.RowCountThreshold = config.GetRowCountThreshold();
    queueConfig.CompactSinglePartedShards = config.GetCompactSinglePartedShards();

    TCompactionQueue::TConfig compactionConfig;

    // schemeshard specific
    compactionConfig.IsCircular = true;

    compactionConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());
    compactionConfig.WakeupInterval = TDuration::Seconds(config.GetWakeupIntervalSeconds());
    compactionConfig.MinWakeupInterval = TDuration::MilliSeconds(config.GetMinWakeupIntervalMs());
    compactionConfig.InflightLimit = config.GetInflightLimit();
    compactionConfig.RoundInterval = TDuration::Seconds(config.GetRoundSeconds());
    compactionConfig.MaxRate = config.GetMaxRate();
    compactionConfig.MinOperationRepeatDelay = TDuration::Seconds(config.GetMinCompactionRepeatDelaySeconds());

    if (CompactionQueue) {
        CompactionQueue->UpdateConfig(compactionConfig, queueConfig);
    } else {
        CompactionQueue = new TCompactionQueue(
            compactionConfig,
            queueConfig,
            CompactionStarter);
        ctx.RegisterWithSameMailbox(CompactionQueue);
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "BackgroundCompactionQueue configured: Timeout# " << compactionConfig.Timeout
                 << ", compact single parted# " << (queueConfig.CompactSinglePartedShards ? "yes" : "no")
                 << ", Rate# " << CompactionQueue->GetRate()
                 << ", WakeupInterval# " << compactionConfig.WakeupInterval
                 << ", RoundInterval# " << compactionConfig.RoundInterval
                 << ", InflightLimit# " << compactionConfig.InflightLimit
                 << ", MinCompactionRepeatDelaySeconds# " << compactionConfig.MinOperationRepeatDelay
                 << ", MaxRate# " << compactionConfig.MaxRate);
}

void TSchemeShard::ConfigureBorrowedCompactionQueue(
    const NKikimrConfig::TCompactionConfig::TBorrowedCompactionConfig& config,
    const TActorContext &ctx)
{
    TBorrowedCompactionQueue::TConfig compactionConfig;

    compactionConfig.IsCircular = false;
    compactionConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());
    compactionConfig.MinWakeupInterval = TDuration::MilliSeconds(config.GetMinWakeupIntervalMs());
    compactionConfig.InflightLimit = config.GetInflightLimit();
    compactionConfig.MaxRate = config.GetMaxRate();

    if (BorrowedCompactionQueue) {
        BorrowedCompactionQueue->UpdateConfig(compactionConfig);
    } else {
        BorrowedCompactionQueue = new TBorrowedCompactionQueue(
            compactionConfig,
            BorrowedCompactionStarter);
        ctx.RegisterWithSameMailbox(BorrowedCompactionQueue);
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "BorrowedCompactionQueue configured: Timeout# " << compactionConfig.Timeout
                 << ", Rate# " << BorrowedCompactionQueue->GetRate()
                 << ", WakeupInterval# " << compactionConfig.WakeupInterval
                 << ", InflightLimit# " << compactionConfig.InflightLimit);
}

void TSchemeShard::ConfigureBackgroundCleaningQueue(
    const NKikimrConfig::TBackgroundCleaningConfig& config,
    const TActorContext &ctx)
{
    TBackgroundCleaningQueue::TConfig cleaningConfig;

    cleaningConfig.IsCircular = false;
    cleaningConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());
    cleaningConfig.MinWakeupInterval = TDuration::MilliSeconds(config.GetMinWakeupIntervalMs());
    cleaningConfig.InflightLimit = config.GetInflightLimit();
    cleaningConfig.MaxRate = config.GetMaxRate();

    if (config.HasRetrySettings()) {
        BackgroundCleaningRetrySettings = config.GetRetrySettings();
    }

    if (BackgroundCleaningQueue) {
        BackgroundCleaningQueue->UpdateConfig(cleaningConfig);
    } else {
        BackgroundCleaningQueue = new TBackgroundCleaningQueue(
            cleaningConfig,
            BackgroundCleaningStarter);
        ctx.RegisterWithSameMailbox(BackgroundCleaningQueue);
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "BackgroundCleaningQueue configured: Timeout# " << cleaningConfig.Timeout
                 << ", Rate# " << BackgroundCleaningQueue->GetRate()
                 << ", WakeupInterval# " << cleaningConfig.WakeupInterval
                 << ", InflightLimit# " << cleaningConfig.InflightLimit);
}

void TSchemeShard::StartStopCompactionQueues() {
    // note, that we don't need to check current state of compaction queue
    if (IsServerlessDomain(TPath::Init(RootPathId(), this))) {
        if (EnableBackgroundCompactionServerless) {
            CompactionQueue->Start();
        } else {
            CompactionQueue->Stop();
        }
    } else {
        if (EnableBackgroundCompaction) {
            CompactionQueue->Start();
        } else {
            CompactionQueue->Stop();
        }
    }

    BorrowedCompactionQueue->Start();
}

void TSchemeShard::Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr &, const TActorContext &ctx) {
     LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                  "Subscription to Console has been set up"
                  << ", schemeshardId: " << SelfTabletId());
}

void TSchemeShard::Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx) {
    auto &rec = ev->Get()->Record;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
               "Got new config: " << rec.GetConfig().ShortDebugString());

    ApplyConsoleConfigs(rec.GetConfig(), ctx);

    auto resp = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(rec);

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "Send TEvConfigNotificationResponse: " << resp->Record.ShortDebugString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TSchemeShard::ChangeStreamShardsCount(i64 delta) {
    TabletCounters->Simple()[COUNTER_STREAM_SHARDS_COUNT].Add(delta);
}

void TSchemeShard::ChangeStreamShardsQuota(i64 delta) {
    TabletCounters->Simple()[COUNTER_STREAM_SHARDS_QUOTA].Add(delta);
}

void TSchemeShard::ChangeStreamReservedStorageCount(i64 delta) {
    TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE].Add(delta);
}

void TSchemeShard::ChangeStreamReservedStorageQuota(i64 delta) {
    TabletCounters->Simple()[COUNTER_STREAM_RESERVED_STORAGE_QUOTA].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceTablesDataBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_DATA_BYTES].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceTablesIndexBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_INDEX_BYTES].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceTablesTotalBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_TABLES_TOTAL_BYTES].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceTopicsTotalBytes(ui64 value) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_TOPICS_TOTAL_BYTES].Set(value);
}

void TSchemeShard::ChangeDiskSpaceQuotaExceeded(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_QUOTA_EXCEEDED].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceHardQuotaBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_HARD_QUOTA_BYTES].Add(delta);
}

void TSchemeShard::ChangeDiskSpaceSoftQuotaBytes(i64 delta) {
    TabletCounters->Simple()[COUNTER_DISK_SPACE_SOFT_QUOTA_BYTES].Add(delta);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvLogin::TPtr &ev, const TActorContext &ctx) {
    Execute(CreateTxLogin(ev), ctx);
}

void TSchemeShard::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext&) {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    std::unique_ptr<TNavigate> request(ev->Get()->Request.Release());
    if (request->ResultSet.size() != 1) {
        return;
    }
    auto& entry = request->ResultSet.back();
    if (entry.Status != TNavigate::EStatus::Ok) {
        return;
    }

    if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
        StatisticsAggregatorId = TTabletId(entry.DomainInfo->Params.GetStatisticsAggregator());
        ConnectToSA();
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvSendBaseStatsToSA::TPtr&, const TActorContext& ctx) {
    SendBaseStatsToSA();
    auto seconds = SendStatsIntervalMaxSeconds - SendStatsIntervalMinSeconds;
    ctx.Schedule(TDuration::Seconds(SendStatsIntervalMinSeconds + RandomNumber<ui64>(seconds)),
        new TEvPrivate::TEvSendBaseStatsToSA());
}

void TSchemeShard::InitializeStatistics(const TActorContext& ctx) {
    ResolveSA();
    ctx.Schedule(TDuration::Seconds(30), new TEvPrivate::TEvSendBaseStatsToSA());
}

void TSchemeShard::ResolveSA() {
    auto subDomainInfo = SubDomains.at(RootPathId());
    if (IsServerlessDomain(subDomainInfo)) {
        auto resourcesDomainId = subDomainInfo->GetResourcesDomainId();

        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = std::make_unique<TNavigate>();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.TableId = TTableId(resourcesDomainId.OwnerId, resourcesDomainId.LocalPathId);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    } else {
        StatisticsAggregatorId = subDomainInfo->GetTenantStatisticsAggregatorID();
        ConnectToSA();
    }
}

void TSchemeShard::ConnectToSA() {
    if (!EnableStatistics || !StatisticsAggregatorId) {
        return;
    }
    auto policy = NTabletPipe::TClientRetryPolicy::WithRetries();
    NTabletPipe::TClientConfig pipeConfig{policy};
    SAPipeClientId = Register(NTabletPipe::CreateClient(SelfId(), (ui64)StatisticsAggregatorId, pipeConfig));

    auto connect = std::make_unique<NStat::TEvStatistics::TEvConnectSchemeShard>();
    connect->Record.SetSchemeShardId(TabletID());

    NTabletPipe::SendData(SelfId(), SAPipeClientId, connect.release());

    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
        "ConnectToSA()"
        << ", pipe client id: " << SAPipeClientId
        << ", at schemeshard: " << TabletID());
}

void TSchemeShard::SendBaseStatsToSA() {
    if (!EnableStatistics) {
        return;
    }

    if (!SAPipeClientId) {
        ResolveSA();
        if (!StatisticsAggregatorId) {
            return;
        }
    }

    int count = 0;

    NKikimrStat::TSchemeShardStats record;
    for (const auto& [pathId, tableInfo] : Tables) {
        const auto& aggregated = tableInfo->GetStats().Aggregated;
        auto* entry = record.AddEntries();
        auto* entryPathId = entry->MutablePathId();
        entryPathId->SetOwnerId(pathId.OwnerId);
        entryPathId->SetLocalId(pathId.LocalPathId);
        entry->SetRowCount(aggregated.RowCount);
        entry->SetBytesSize(aggregated.DataSize);
        ++count;
    }
    auto columnTablesPathIds = ColumnTables.GetAllPathIds();
    for (const auto& pathId : columnTablesPathIds) {
        const auto& tableInfo = ColumnTables.GetVerified(pathId);
        const auto& aggregated = tableInfo->Stats.Aggregated;
        auto* entry = record.AddEntries();
        auto* entryPathId = entry->MutablePathId();
        entryPathId->SetOwnerId(pathId.OwnerId);
        entryPathId->SetLocalId(pathId.LocalPathId);
        entry->SetRowCount(aggregated.RowCount);
        entry->SetBytesSize(aggregated.DataSize);
        ++count;
    }

    TString stats;
    stats.clear();
    Y_PROTOBUF_SUPPRESS_NODISCARD record.SerializeToString(&stats);

    auto event = std::make_unique<NStat::TEvStatistics::TEvSchemeShardStats>();
    event->Record.SetSchemeShardId(TabletID());
    event->Record.SetStats(stats);

    NTabletPipe::SendData(SelfId(), SAPipeClientId, event.release());

    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
        "SendBaseStatsToSA()"
        << ", path count: " << count
        << ", at schemeshard: " << TabletID());
}

} // namespace NSchemeShard
} // namespace NKikimr
