#include "schemeshard_impl.h"
#include "schemeshard__local_index_migration.h"
#include "schemeshard_svp_migration.h"

#include "olap/bg_tasks/adapter/adapter.h"
#include "olap/bg_tasks/events/global.h"
#include "olap/operations/local_index_helpers.h"
#include "schemeshard.h"
#include "schemeshard__root_shred_manager.h"
#include "schemeshard__tenant_shred_manager.h"
#include "schemeshard_svp_migration.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>  // for TStoragePoolsStats
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/resolver.h>
#include <ydb/core/sys_view/partition_stats/partition_stats.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/bloom_filter_defaults.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/test_tablet/events.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/schemeshard_sysviews_update.h>

#include <ydb/library/login/account_lockout/account_lockout.h>
#include <ydb/library/login/password_checker/password_checker.h>

#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

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
        result.back().SetStoragePoolKind(poolIt->GetKind());
    }

    channelsBinding.swap(result);
    return true;
}

struct TDirectoryEntry {
    NKikimrSchemeOp::EPathType Type;
    TString Owner;
    TMaybe<NKikimrSysView::ESysViewType> SysViewType;
};

}   // anonymous namespace

const TSchemeLimits TSchemeShard::DefaultLimits = {};

void TSchemeShard::SubscribeToTempTableOwners() {
    auto ctx = ActorContext();
    auto& TempDirsByOwner = TempDirsState.TempDirsByOwner;
    for (const auto& [ownerActorId, tempTables] : TempDirsByOwner) {
        ctx.Send(new IEventHandle(ownerActorId, SelfId(),
                                new TEvSchemeShard::TEvOwnerActorAck(),
                                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession));
    }
}

void TSchemeShard::CollectSysViewUpdates(const TActorContext& ctx) {
    TVector<std::pair<TTxId, TModifySysViewRequestInfo>> sysViewUpdates;

    const TPath rootPath = TPath::Root(this);
    const TPath sysViewDirPath = rootPath.Child(TString(NSysView::SysPathName));
    bool needToMakeSysViewDir = false;
    TMap<TString, TDirectoryEntry> sysViewDirContents;

    // make system view dir or collect its contents
    const bool isSysViewDirNameVacant = !sysViewDirPath.Check().IsResolved().NotDeleted().NotUnderDeleting();
    const bool sysViewDirExists = bool(sysViewDirPath.Check().IsResolved().NotDeleted().NotUnderDeleting().IsDirectory());
    if (isSysViewDirNameVacant) { // if it's a first start of SchemeShard
        needToMakeSysViewDir = true;
        TModifySysViewRequestInfo makeSysViewDirRequest;
        makeSysViewDirRequest.OperationType = NKikimrSchemeOp::ESchemeOpMkDir;
        makeSysViewDirRequest.WorkingDir = rootPath.PathString();
        makeSysViewDirRequest.TargetName = sysViewDirPath.LeafName();

        sysViewUpdates.emplace_back(GetCachedTxId(ctx), std::move(makeSysViewDirRequest));
    } else if (sysViewDirExists) { // only if '.sys' entry exists and it's a directory
        for (const auto& [name, pathId] : sysViewDirPath->GetChildren()) {
            const TPath dirEntryPath = TPath::Init(pathId, this);
            const auto checks = dirEntryPath.Check();
            if (checks.IsResolved().NotDeleted().NotUnderDeleting()) {
                TDirectoryEntry dirEntry;
                dirEntry.Type = dirEntryPath->PathType;
                dirEntry.Owner = dirEntryPath->Owner;
                if (checks.IsSysView()) {
                    dirEntry.SysViewType = SysViews.at(pathId)->Type;
                }

                sysViewDirContents.emplace(name, std::move(dirEntry));
            }
        }
    }

    const auto sysViewDirType = IsDomainSchemeShard
        ? NSysView::ISystemViewResolver::ESource::Domain
        : NSysView::ISystemViewResolver::ESource::SubDomain;
    const auto& sysViewsRegistry = NSysView::GetSystemViewResolver().GetSystemViewsTypes(sysViewDirType);

    // create absent system views only if there's no '.sys' entry or '.sys' is a directory
    if (needToMakeSysViewDir || sysViewDirExists) {
        for (const auto& [name, type] : sysViewsRegistry) {
            if (!sysViewDirContents.contains(name)) {
                TModifySysViewRequestInfo createSysViewRequest;
                createSysViewRequest.OperationType = NKikimrSchemeOp::ESchemeOpCreateSysView;
                createSysViewRequest.WorkingDir = sysViewDirPath.PathString();
                createSysViewRequest.TargetName = name;
                createSysViewRequest.SysViewType = type;

                sysViewUpdates.emplace_back(GetCachedTxId(ctx), std::move(createSysViewRequest));
            }
        }
    }

    THashSet<NKikimrSysView::ESysViewType> availableSysViewTypes;
    for (const auto& type : std::views::values(sysViewsRegistry)) {
        availableSysViewTypes.insert(type);
    }

    // drop obsolete system views
    for (const auto& [name, dirEntry] : sysViewDirContents) {
        if (dirEntry.Type == NKikimrSchemeOp::EPathTypeSysView) {
            if (!dirEntry.SysViewType || !availableSysViewTypes.contains(*dirEntry.SysViewType) ||
                (dirEntry.Owner == BUILTIN_ACL_METADATA && !sysViewsRegistry.contains(name))) {
                TModifySysViewRequestInfo dropSysViewRequest;
                dropSysViewRequest.OperationType = NKikimrSchemeOp::ESchemeOpDropSysView;
                dropSysViewRequest.WorkingDir = sysViewDirPath.PathString();
                dropSysViewRequest.TargetName = name;

                sysViewUpdates.emplace_back(GetCachedTxId(ctx), std::move(dropSysViewRequest));
            }
        }
    }

    if (!sysViewUpdates.empty()) {
        Register(CreateSysViewsRosterUpdate(static_cast<TTabletId>(TabletID()), SelfId(), std::move(sysViewUpdates)).Release());
    }
}

void TSchemeShard::CollectLocalIndexMigrations(const TActorContext& ctx) {
    if (!AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject()) {
        return;
    }

    TVector<TLocalIndexMigrationItem> items;

    for (const auto& tablePathId : ColumnTables.GetAllPathIds()) {
        const auto tableInfo = ColumnTables.GetVerifiedPtr(tablePathId);
        if (!tableInfo->IsStandalone()) {
            continue;
        }

        const auto& schema = tableInfo->Description.GetSchema();
        if (schema.IndexesSize() == 0) {
            continue;
        }

        const TPathElement::TPtr tablePath = PathsById.at(tablePathId);
        const auto columnIdToName = NOlap::BuildColumnIdToNameMap(schema);
        const TString workingDir = TPath::Init(tablePathId, this).PathString();

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "LocalIndexMigrator: processing table " << workingDir
            << " with " << schema.IndexesSize() << " index(es)");

        for (const auto& indexProto : schema.GetIndexes()) {
            const TString& indexName = indexProto.GetName();

            // Skip indexes that already exist as live scheme-object children.
            if (const TPathId* childId = tablePath->FindChild(indexName)) {
                const auto& child = PathsById.at(*childId);
                if (child->IsTableIndex() && !child->Dropped()) {
                    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "LocalIndexMigrator: skipping index " << indexName
                        << " (already exists as scheme object)");
                    continue;
                }
            }
            
            if (indexProto.GetImplementationCase() == NKikimrSchemeOp::TOlapIndexDescription::kMaxIndex) {
                continue;
            }

            NKikimrSchemeOp::TIndexCreationConfig indexConfig;
            if (!NOlap::ConvertOlapIndexToCreationConfig(indexProto, columnIdToName, indexConfig)) {
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "LocalIndexMigrator skip index: failed to build creation config"
                    << ", table: " << workingDir << ", index: " << indexName);
                continue;
            }

            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "LocalIndexMigrator: adding index " << indexName << " from table " << workingDir);
            items.emplace_back(TLocalIndexMigrationItem{
                .WorkingDir = workingDir,
                .IndexConfig = std::move(indexConfig),
            });
        }
    }

    // Row tables: legacy prefix bloom filters live as nameless ByKeyFilterPrefixes in the
    // partition config. Synthesize a named TTableIndex scheme object per prefix.
    for (const auto& [tablePathId, tableInfo] : Tables) {
        const auto& partitionConfig = tableInfo->PartitionConfig();
        if (partitionConfig.ByKeyFilterPrefixesSize() == 0) {
            continue;
        }

        const TPathElement::TPtr tablePath = PathsById.at(tablePathId);
        if (tablePath->Dropped() || !tablePath->IsTable()) {
            continue;
        }
        const TPath path = TPath::Init(tablePathId, this);
        if (!path.IsCommonSensePath()) {
            // Skip index impl tables and other non-user tables.
            continue;
        }

        // Ordered primary-key column names.
        TVector<TString> pkColumns;
        pkColumns.reserve(tableInfo->KeyColumnIds.size());
        for (ui32 colId : tableInfo->KeyColumnIds) {
            auto colIt = tableInfo->Columns.find(colId);
            if (colIt == tableInfo->Columns.end()) {
                break;
            }
            pkColumns.push_back(colIt->second.Name);
        }

        const TString workingDir = path.PathString();

        for (const auto& prefix : partitionConfig.GetByKeyFilterPrefixes()) {
            const ui32 prefixLen = prefix.GetPrefixLength();
            if (prefixLen == 0 || prefixLen > pkColumns.size()) {
                continue;
            }

            // Idempotency: skip if a local bloom index over this prefix already exists.
            bool alreadyExists = false;
            for (const auto& [childName, childPathId] : tablePath->GetChildren()) {
                const auto& child = PathsById.at(childPathId);
                if (child->Dropped() || !child->IsTableIndex()) {
                    continue;
                }
                auto indexIt = Indexes.find(childPathId);
                if (indexIt == Indexes.end()
                    || indexIt->second->Type != NKikimrSchemeOp::EIndexTypeLocalBloomFilter) {
                    continue;
                }
                const auto& indexKeys = indexIt->second->IndexKeys;
                if (indexKeys.size() == prefixLen
                    && std::equal(indexKeys.begin(), indexKeys.end(), pkColumns.begin())) {
                    alreadyExists = true;
                    break;
                }
            }
            if (alreadyExists) {
                continue;
            }

            NKikimrSchemeOp::TIndexCreationConfig indexConfig;
            for (ui32 i = 0; i < prefixLen; ++i) {
                indexConfig.AddKeyColumnNames(pkColumns[i]);
            }
            // Legacy prefixes have no name. Use a deterministic convention
            // "idx_bloom_<prefixLen>" to keep the name stable.
            const TString name = TStringBuilder() << "idx_bloom_" << prefixLen;
            if (tablePath->FindChild(name)) {
                LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "CollectLocalIndexMigrations: skipping row bloom prefix " << prefixLen
                    << " for table " << workingDir << " (name '" << name << "' already taken)");
                continue;
            }
            indexConfig.SetName(name);
            indexConfig.SetType(NKikimrSchemeOp::EIndexTypeLocalBloomFilter);
            indexConfig.SetState(NKikimrSchemeOp::EIndexStateReady);
            indexConfig.MutableBloomFilterDescription()->SetFalsePositiveProbability(
                prefix.HasFalsePositiveProbability()
                    ? prefix.GetFalsePositiveProbability()
                    : NTable::DefaultBloomFilterFpp);

            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "CollectLocalIndexMigrations: adding row bloom index " << name << " from table " << workingDir);
            items.emplace_back(TLocalIndexMigrationItem{
                .WorkingDir = workingDir,
                .IndexConfig = std::move(indexConfig),
                .IsColumnTable = false,
            });
        }
    }

    if (items.empty()) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "LocalIndexMigrator: no indexes to migrate");
        return;
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "LocalIndexMigrator: starting migrator for " << items.size() << " index(es)");

    LocalIndexMigratorId = ctx.RegisterWithSameMailbox(CreateLocalIndexMigrator(static_cast<TTabletId>(TabletID()), SelfId(), this, std::move(items)).Release());
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

    if (opts.RestoreTablesToUnmark) {
        Execute(CreateTxUnmarkRestoreTables(std::move(opts.RestoreTablesToUnmark)), ctx);
    }

    if (IsDomainSchemeShard) {
        InitializeTabletMigrations();
    }

    ResumeExports(opts.ExportIds, ctx);
    ResumeImports(opts.ImportsIds, ctx);
    ResumeCdcStreamScans(opts.CdcStreamScans, ctx);
    ResumeIncrementalBackups(opts.IncrementalBackupIds, ctx);
    ResumeFullBackups(opts.FullBackupIds, ctx);

    ParentDomainLink.SendSync(ctx);

    ScheduleConditionalEraseRun(ctx);
    ScheduleServerlessStorageBilling(ctx);

    Y_ABORT_UNLESS(CleanDroppedPathsDisabled);
    CleanDroppedPathsDisabled = false;
    ScheduleCleanDroppedPaths();
    ScheduleCleanDroppedSubDomains();

    StartStopCompactionQueues();
    BackgroundCleaningQueue->Start();

    StartStopShred();

    ctx.Send(TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(InitiateCachedTxIdsCount));

    // Start local index migration if feature flag is enabled
    // This ensures migration starts even if we don't receive a new TEvAllocateResult
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "ActivateAfterInitialization: checking local index migration"
        << ", feature flag: " << AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject()
        << ", LocalIndexMigrationStarted: " << LocalIndexMigrationStarted);
    if (AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject() && !LocalIndexMigrationStarted) {
        LocalIndexMigrationStarted = true;
        CollectLocalIndexMigrations(ctx);
    }

    InitializeStatistics(ctx);

    SubscribeToTempTableOwners();

    InitializeTablePartitionsFormatSweep();

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
            !IsServerlessDomainGlobal(pathId, subdomain) &&
            subdomain->GetTenantStatisticsAggregatorID() == InvalidTabletId)
        {
            createSA = true;
        }

        // NOTE: BackupController tablet is not created eagerly anymore.
        // It is a placeholder with no real functionality, and its eager creation
        // blocks tenant databases in PENDING state during initial configuration.

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

THolder<TEvColumnShard::TEvProposeTransaction> TSchemeShard::MakeColumnShardProposal(
        const TPathId& pathId, const TOperationId& opId,
        const TMessageSeqNo& seqNo, const TString& body, const TActorContext& ctx,
        NKikimrTxColumnShard::ETransactionKind kind) const
{
    return MakeHolder<TEvColumnShard::TEvProposeTransaction>(
        kind, TabletID(), ctx.SelfID,
        ui64(opId.GetTxId()), body, seqNo,  SelectProcessingParams(pathId),
        0, 0
    );
}

THolder<::NActors::IEventBase> TSchemeShard::MakeShardProposal(
        const TPath& path, const TOperationId& opId,
        const TMessageSeqNo& seqNo, const TString& body, const TActorContext& ctx) const
{
    if (path->IsTable()) {
        return MakeDataShardProposal(path->PathId, opId, body, ctx);
    } else if (path->IsColumnTable()) {
        return MakeColumnShardProposal(path->PathId, opId, seqNo, body, ctx);
    } else {
        Y_ABORT();
    }
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

void TSchemeShard::ReturnTxIdToCache(const TTxId txId) {
    CachedTxIds.push_back(txId);
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
    HasOrphanPlaceholders = false;

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
    SysViews.clear();
    Secrets.clear();

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

    if (BackgroundCompactionQueue) {
        BackgroundCompactionQueue->Clear();
        UpdateBackgroundCompactionQueueMetrics();
    }

    if (BorrowedCompactionQueue) {
        BorrowedCompactionQueue->Clear();
        UpdateBorrowedCompactionQueueMetrics();
    }

    if (ForcedCompactionQueue) {
        ForcedCompactionQueue->Clear();
    }

    if (RootShredManager) {
        RootShredManager->Clear();
    }
    if (TenantShredManager) {
        TenantShredManager->Clear();
    }

    ClearTempDirsState();

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
        THashMap<TString, TVector<ui32>> &reverseBinding,
        TStorageRoom &room, TString &errorMsg)
{
    if (channelsBinding && !reverseBinding) {
        // Build a hash map from storage pool name to an existing channel number
        for (ui32 channel = 1; channel < channelsBinding.size(); ++channel) {
            reverseBinding[channelsBinding[channel].GetStoragePoolName()].emplace_back(channel);
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

    auto allocateChannel = [&] (const TStoragePool& pool, const ui32 reuseIndex = 0) -> ui32 {
        auto it = reverseBinding.find(pool.GetName());
        if (it != reverseBinding.end()) {
            // Try to reuse an existing channel if possible
            if (reuseIndex < it->second.size()) {
                // If we have a channel with the required index, use it
                return it->second[reuseIndex];
            }
        }

        // Otherwise, allocate a new channel
        ui32 channel = channelsBinding.size();
        channelsBinding.emplace_back();
        channelsBinding.back().SetStoragePoolName(pool.GetName());
        channelsBinding.back().SetStoragePoolKind(pool.GetKind());
        reverseBinding[pool.GetName()].emplace_back(channel);
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
        channelsBinding.back().SetStoragePoolKind(sysLogPool->GetKind());
    }

    if (channelsBinding.size() < 2) {
        LOCAL_CHECK(storageConfig.HasLog(), "no log storage setting");
        auto logPool = resolve(storagePools, storageConfig.GetLog());
        LOCAL_CHECK(logPool != storagePools.end(), "unable determine pool for log storage");

        ui32 channel = allocateChannel(*logPool);
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

        ui32 channel = allocateChannel(*dataPool);
        room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::Data, channel);
    }

    if (storageConfig.HasExternal()) {
        auto externalPool = resolve(storagePools, storageConfig.GetExternal());
        LOCAL_CHECK(externalPool != storagePools.end(), "definition of externalPool storage present but unable determine pool for it");

        ui32 externalChannelsCount = storageConfig.HasExternalChannelsCount() ? storageConfig.GetExternalChannelsCount() : 1;
        externalChannelsCount = Max<ui32>(externalChannelsCount, 1);
        LOCAL_CHECK(externalChannelsCount < Max<ui8>(), "more than 255 external channels requested");
        for (ui32 i = 0; i < externalChannelsCount; ++i) {
            // In case if we have only 1 external channel, we will have old behavior
            ui32 channel = allocateChannel(*externalPool, i);
            room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::External, channel);
        }
    }

#undef LOCAL_CHECK

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
        tabletInfo->PreserializedTablePartitions.clear();
        tabletInfo->PreserializedTablePartitionsNoKeys.clear();
        tabletInfo->PreserializedTableSplitBoundaries.clear();
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

    THashMap<TString, TVector<ui32>> reverseBinding;

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
    TSet<ui32> primaryExternals = rooms[0].GetExternalChannels(0);

    struct ChannelsKey {
        TSet<ui32> External;
        ui32 Data;

        bool operator==(const ChannelsKey& other) const {
            return External == other.External && Data == other.Data;
        }
    };
    auto keyHasher = [](const ChannelsKey& k) -> size_t {
        return k.Data + std::accumulate(k.External.begin(), k.External.end(), 0);
    };

    THashMap<ChannelsKey, ui32, decltype(keyHasher)> roomByChannels;

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
        TSet<ui32> externalChannels = newRoom.GetExternalChannels(0);
        if (dataChannel == primaryData && externalChannels == primaryExternals) {
            continue; // will use the default room
        }

        ChannelsKey cookie = { externalChannels, dataChannel };

        auto it = roomByChannels.find(cookie);
        if (it != roomByChannels.end()) {
            familyRooms[family.GetId()] = it->second;
            continue;
        }

        auto& room = rooms.emplace_back(rooms.size());
        if (dataChannel != 0) {
            room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::Data, dataChannel);
        }
        for (ui32 externalChannel : externalChannels) {
            if (externalChannel != 0) {
                room.AssignChannel(NKikimrStorageSettings::TChannelPurpose::External, externalChannel);
            }
        }
        roomByChannels[cookie] = room.GetId();
        familyRooms[family.GetId()] = room.GetId();
    }

    return true;
}

bool TSchemeShard::GetBindingsRoomsChanges(
        const TPathId domainId,
        const TVector<TTableShardInfo*>& partitions,
        const NKikimrSchemeOp::TPartitionConfig& partitionConfig,
        TBindingsRoomsChanges& changes,
        TString& errStr)
{
    for (const auto* shard : partitions) {
        const auto& shardInfo = ShardInfos.at(shard->ShardIdx);
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

bool TSchemeShard::CheckApplyIf(const NKikimrSchemeOp::TModifyScheme& scheme, TString& errStr, std::optional<TPathElement::EPathType> pathType) {
    const auto& conditions = scheme.GetApplyIf();

    for (const auto& item: conditions) {
        if (item.HasPathId()) {
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
                        case NKikimrSchemeOp::EPathType::EPathTypeTransfer:
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
                        case NKikimrSchemeOp::EPathType::EPathTypeSysView:
                            actualVersion = pathVersion.GetSysViewVersion();
                            break;
                        case NKikimrSchemeOp::EPathType::EPathTypeSecret:
                            actualVersion = pathVersion.GetSecretVersion();
                            break;
                        case NKikimrSchemeOp::EPathType::EPathTypeStreamingQuery:
                            actualVersion = pathVersion.GetStreamingQueryVersion();
                            break;
                        case NKikimrSchemeOp::EPathType::EPathTypeTestShardSet:
                            actualVersion = pathVersion.GetTestShardSetVersion();
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

        if (AppData()->FeatureFlags.GetEnableAlterDatabase()) {
            if (!item.GetPathTypes().empty()) {
                if (!pathType.has_value()) {
                    errStr = TStringBuilder()
                        << "fail in ApplyIf section:"
                        << " argument `pathType` is undefined,"
                        << " but ApplyIf has non-empty field `PathTypes.`";

                    return false;
                }

                const auto& pathTypes = item.GetPathTypes();
                bool allowed = (std::find(pathTypes.begin(), pathTypes.end(), pathType) != pathTypes.end());
                if (!allowed) {
                    auto enumToString = [](TPathElement::EPathType type) {
                        return NKikimrSchemeOp::EPathType_Name(type);
                    };

                    errStr = TStringBuilder()
                        << "fail in ApplyIf section:"
                        << " wrong Path type."
                        << " Expected types: ";

                        for (int i = 0; i < pathTypes.size(); i++) {
                            errStr += enumToString(static_cast<TPathElement::EPathType>(pathTypes[i]) ) + ",;"[i + 1 == pathTypes.size()] + " ";
                        }

                        errStr += TStringBuilder() << "But actual Path type is " << enumToString(pathType.value());
                    return false;
                }
            }
        } else if (!item.GetPathTypes().empty()) {
            errStr = TStringBuilder()
                << "fail in ApplyIf section:"
                << " Check Path Type is not supported";

            return false;
        }
    }

    return true;
}


} // namespace NSchemeShard
} // namespace NKikimr
