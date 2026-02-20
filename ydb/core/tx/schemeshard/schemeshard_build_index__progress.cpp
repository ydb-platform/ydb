#include "schemeshard_build_index.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_index_utils.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/core/base/table_index.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/core/tx/datashard/upload_stats.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <yql/essentials/public/issue/yql_issue_message.h>


namespace NKikimr {
namespace NSchemeShard {

ui64 gVectorIndexSeed = 0;

// return count, parts, step
static std::tuple<NTableIndex::NKMeans::TClusterId, NTableIndex::NKMeans::TClusterId, NTableIndex::NKMeans::TClusterId>
    ComputeKMeansBoundaries(const NSchemeShard::TTableInfo& tableInfo, const TIndexBuildInfo& buildInfo, ui64 maxShardsInPath) {
    const auto& kmeans = buildInfo.KMeans;
    Y_ENSURE(kmeans.K != 0);
    const auto count = kmeans.ChildCount();
    NTableIndex::NKMeans::TClusterId step = 1;
    auto parts = count;
    auto shards = tableInfo.GetShard2PartitionIdx().size();
    if (!buildInfo.KMeans.NeedsAnotherLevel() || count <= 1 || shards <= 1) {
        return {1, 1, 1};
    }
    for (; 2 * shards <= parts || parts > maxShardsInPath; parts = (parts + 1) / 2) {
        step *= 2;
    }
    return {count, parts, step};
}

class TUploadSampleK: public TActorBootstrapped<TUploadSampleK> {
    using TThis = TUploadSampleK;
    using TBase = TActorBootstrapped<TThis>;

protected:
    TString LogPrefix;
    const TString DatabaseName;
    const TString TargetTable;

    const NKikimrIndexBuilder::TIndexBuildScanSettings ScanSettings;

    const TActorId ResponseActorId;
    const TIndexBuildId BuildId;

    std::shared_ptr<NTxProxy::TUploadTypes> Types;
    TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> InputRows;
    std::shared_ptr<NTxProxy::TUploadRows> UploadRows;

    TActorId Uploader;
    ui32 RetryCount = 0;
    ui32 UploadBytes = 0;

    NDataShard::TUploadStatus UploadStatus;

public:
    TUploadSampleK(const TString& databaseName,
                   const TString& targetTable,
                   const NKikimrIndexBuilder::TIndexBuildScanSettings& scanSettings,
                   const TActorId& responseActorId,
                   TIndexBuildId buildId,
                   std::shared_ptr<NTxProxy::TUploadTypes> types,
                   TVector<std::pair<TSerializedCellVec, TSerializedCellVec>>&& rows)
        : DatabaseName(databaseName)
        , TargetTable(targetTable)
        , ScanSettings(scanSettings)
        , ResponseActorId(responseActorId)
        , BuildId(buildId)
        , Types(types)
        , InputRows(std::move(rows))
    {
        LogPrefix = TStringBuilder()
            << "TUploadSampleK: BuildIndexId: " << BuildId
            << " ResponseActorId: " << ResponseActorId;
    }

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SAMPLE_K_UPLOAD_ACTOR;
    }

    void UploadStatusToMessage(NKikimrIndexBuilder::TEvUploadSampleKResponse& msg) {
        msg.SetUploadStatus(UploadStatus.StatusCode);
        NYql::IssuesToMessage(UploadStatus.Issues, msg.MutableIssues());
    }

    void Describe(IOutputStream& out) const noexcept override {
        out << LogPrefix << Debug();
    }

    TString Debug() const {
        return UploadStatus.ToString();
    }

    void Bootstrap() {
        Become(&TThis::StateWork);

        UploadRows = std::make_shared<NTxProxy::TUploadRows>();
        UploadRows->reserve(InputRows.size());
        for (const auto& row: InputRows) {
            UploadBytes += NDataShard::CountRowCellBytes(row.first.GetCells(), row.second.GetCells());
            UploadRows->emplace_back(TSerializedCellVec{row.first}, TSerializedCellVec::Serialize(row.second.GetCells()));
        }
        InputRows = {}; // release memory

        Upload(false);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
        }
    }

    void HandleWakeup() {
        LOG_D("Retry upload " << Debug());

        if (UploadRows) {
            Upload(true);
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        LOG_D("Handle TEvUploadRowsResponse "
              << Debug()
              << " Uploader: " << Uploader.ToString()
              << " ev->Sender: " << ev->Sender.ToString());

        if (!Uploader) {
            return;
        }
        Y_ENSURE(Uploader == ev->Sender,
                   LogPrefix << "Mismatch"
                             << " Uploader: " << Uploader.ToString()
                             << " ev->Sender: " << ev->Sender.ToString()
                             << Debug());

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues = std::move(ev->Get()->Issues);

        if (UploadStatus.IsRetriable() && RetryCount < ScanSettings.GetMaxBatchRetries()) {
            LOG_N("Got retriable error, " << Debug() << " RetryCount: " << RetryCount);

            this->Schedule(NDataShard::GetRetryWakeupTimeoutBackoff(RetryCount), new TEvents::TEvWakeup());
            return;
        }
        TAutoPtr<TEvIndexBuilder::TEvUploadSampleKResponse> response = new TEvIndexBuilder::TEvUploadSampleKResponse;

        response->Record.SetId(ui64(BuildId));
        response->Record.MutableMeteringStats()->SetUploadRows(UploadRows->size());
        response->Record.MutableMeteringStats()->SetUploadBytes(UploadBytes);

        UploadStatusToMessage(response->Record);

        this->Send(ResponseActorId, response.Release());
        this->PassAway();
    }

    void Upload(bool isRetry) {
        if (isRetry) {
            ++RetryCount;
        } else {
            RetryCount = 0;
        }

        auto actor = NTxProxy::CreateUploadRowsInternal(
            SelfId(),
            DatabaseName,
            TargetTable,
            Types,
            UploadRows,
            NTxProxy::EUploadRowsMode::WriteToTableShadow, // TODO(mbkkt) is it fastest?
            true /*writeToPrivateTable*/,
            true /*writeToIndexImplTable*/);

        Uploader = this->Register(actor);
    }
};

TPath GetBuildPath(TSchemeShard* ss, const TIndexBuildInfo& buildInfo, const TString& tableName) {
    return TPath::Init(buildInfo.TablePathId, ss)
        .Dive(buildInfo.IndexName)
        .Dive(tableName);
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> LockPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo, TTxId txId, const TPath& path)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    propose->Record.SetFailOnExist(false);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLock);
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    modifyScheme.MutableLockConfig()->SetName(path.LeafName());
    modifyScheme.MutableLockConfig()->SetLockTxId(ui64(buildInfo.LockTxId));

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "LockPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateIndexPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.InitiateTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(TPath::Init(buildInfo.DomainPathId, ss).PathString());
    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    if (buildInfo.IsBuildIndex()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexBuild);
        buildInfo.SerializeToProto(ss, modifyScheme.MutableInitiateIndexBuild());
    } else if (buildInfo.IsBuildColumns()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnBuild);
        buildInfo.SerializeToProto(ss, modifyScheme.MutableInitiateColumnBuild());
    } else {
        Y_ENSURE(false, "Unknown operation kind while building CreateIndexPropose");
    }

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "CreateIndexPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropBuildPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ENSURE(buildInfo.IsBuildVectorIndex());

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    auto path = GetBuildPath(ss, buildInfo, buildInfo.KMeans.WriteTo(true));

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    if (path.IsLocked()) {
        // because some impl tables may be not locked, do not pass lock guard for them
        // otherwise `CheckLocks` check would fail
        modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));
    }

    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    modifyScheme.MutableDrop()->SetName(path->Name);

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "DropBuildPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateBuildPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ENSURE(buildInfo.IsBuildVectorIndex());

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);
    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetInternal(true);

    auto path = TPath::Init(buildInfo.TablePathId, ss);
    auto maxShardsInPath = path.DomainInfo()->GetSchemeLimits().MaxShardsInPath;

    const auto& tableInfo = ss->Tables.at(path->PathId);
    NTableIndex::TTableColumns implTableColumns;
    THashSet<TString> indexDataColumns;
    {
        buildInfo.SerializeToProto(ss, modifyScheme.MutableInitiateIndexBuild());
        const auto& indexDesc = modifyScheme.GetInitiateIndexBuild().GetIndex();
        const auto& baseTableColumns = NTableIndex::ExtractInfo(tableInfo);
        auto indexKeys = NTableIndex::ExtractInfo(indexDesc);
        if (buildInfo.IsBuildPrefixedVectorIndex() && buildInfo.KMeans.Level != 1) {
            Y_ENSURE(indexKeys.KeyColumns.size() >= 2);
            indexKeys.KeyColumns.erase(indexKeys.KeyColumns.begin(), indexKeys.KeyColumns.end() - 1);
        }
        implTableColumns = CalcTableImplDescription(buildInfo.IndexType, baseTableColumns, indexKeys);
        Y_ENSURE(indexKeys.KeyColumns.size() >= 1);
        implTableColumns.Columns.emplace(indexKeys.KeyColumns.back());
        modifyScheme.ClearInitiateIndexBuild();
        indexDataColumns = THashSet<TString>(buildInfo.DataColumns.begin(), buildInfo.DataColumns.end());
        indexDataColumns.insert(indexKeys.KeyColumns.back());
    }

    using namespace NTableIndex::NKMeans;
    modifyScheme.SetWorkingDir(path.Dive(buildInfo.IndexName).PathString());
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable);
    auto& op = *modifyScheme.MutableCreateTable();
    auto suffix = buildInfo.KMeans.NextBuildSuffix();
    auto resetPartitionsSettings = [&] {
        auto& config = *op.MutablePartitionConfig();
        config.SetShadowData(false);

        auto& policy = *config.MutablePartitioningPolicy();
        policy.SetSizeToSplit(0); // disable auto split/merge
        policy.ClearFastSplitSettings();
        policy.ClearSplitByLoadSettings();

        op.ClearSplitBoundary();
        return &policy;
    };
    if (buildInfo.IsBuildPrefixedVectorIndex() && buildInfo.KMeans.Level == 1) {
        op.SetName(TString::Join(PostingTable, suffix));
        NTableIndex::FillIndexTableColumns(tableInfo->Columns, implTableColumns.Keys, implTableColumns.Columns, op);
        auto& policy = *resetPartitionsSettings();
        // Prevent merging partitions
        policy.SetMinPartitionsCount(maxShardsInPath);
        policy.SetMaxPartitionsCount(0);

        LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
            "CreateBuildPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

        return propose;
    }

    // FIXME: Do first scan with overlap directly into the next level, but this will require the third variant
    // of Local / Reshuffle scans - one which outputs rows with the Foreign flag, but without Distance
    if (buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1 && buildInfo.KMeans.State != TIndexBuildInfo::TKMeans::Filter) {
        // When OverlapClusters is active, first build table for each level contains 2 additional columns: __ydb_distance and __ydb_foreign,
        // and its primary key has different order - original table's primary key comes first and the cluster ID comes next
        if (buildInfo.KMeans.Level >= buildInfo.KMeans.Levels) {
            indexDataColumns = THashSet<TString>(buildInfo.DataColumns.begin(), buildInfo.DataColumns.end());
        }
        op = NTableIndex::CalcVectorKmeansTreeBuildOverlapTableDesc(tableInfo, tableInfo->PartitionConfig(), indexDataColumns, {}, suffix);
        // Prevent merging partitions
        auto& policy = *resetPartitionsSettings();
        policy.SetMinPartitionsCount(maxShardsInPath);
        policy.SetMaxPartitionsCount(0);
        // This also means that we can directly copy split boundaries from the main table!
        const auto& tableInfo = ss->Tables.at(buildInfo.TablePathId);
        size_t parts = tableInfo->GetPartitions().size();
        for (const auto& x: tableInfo->GetPartitions()) {
            if (--parts > 0) {
                op.AddSplitBoundary()->SetSerializedKeyPrefix(x.EndOfRange);
            }
        }
        LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
            "CreateBuildPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());
        return propose;
    }

    op = NTableIndex::CalcVectorKmeansTreePostingImplTableDesc(tableInfo, tableInfo->PartitionConfig(), indexDataColumns, {}, suffix,
        buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1);
    const auto [count, parts, step] = ComputeKMeansBoundaries(*tableInfo, buildInfo, maxShardsInPath);

    auto& policy = *resetPartitionsSettings();
    static constexpr std::string_view LogPrefix = "Create build table boundaries for ";
    LOG_D(buildInfo.Id << " table " << suffix
        << ", count: " << count << ", parts: " << parts << ", step: " << step
        << ", " << buildInfo.DebugString());
    if (parts > 1) {
        const auto from = buildInfo.KMeans.ChildBegin;
        for (auto i = from + step, e = from + count; i < e; i += step) {
            LOG_D(buildInfo.Id << " table " << suffix << " value: " << i);
            auto cell = TCell::Make(i);
            op.AddSplitBoundary()->SetSerializedKeyPrefix(TSerializedCellVec::Serialize({&cell, 1}));
        }
        // Prevent merging partitions
        policy.SetMinPartitionsCount(maxShardsInPath);
        policy.SetMaxPartitionsCount(0);
    }

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "CreateBuildPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTablePropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ENSURE(buildInfo.IsBuildColumns(), "Unknown operation kind while building AlterMainTablePropose");

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.AlterMainTableTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
    modifyScheme.SetInternal(true);
    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    auto path = TPath::Init(buildInfo.TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    modifyScheme.MutableAlterTable()->SetName(path.LeafName());

    for (const auto& colInfo : buildInfo.BuildColumns) {
        auto col = modifyScheme.MutableAlterTable()->AddColumns();
        NScheme::TTypeInfo typeInfo;
        TString typeMod;
        Ydb::StatusIds::StatusCode status;
        TString error;
        if (!ExtractColumnTypeInfo(typeInfo, typeMod, colInfo.DefaultFromLiteral.type(), status, error)) {
            // todo gvit fix that
            Y_ENSURE(false, "failed to extract column type info");
        }

        col->SetType(NScheme::TypeName(typeInfo, typeMod));
        col->SetName(colInfo.ColumnName);
        col->MutableDefaultFromLiteral()->CopyFrom(colInfo.DefaultFromLiteral);
        col->SetIsBuildInProgress(true);

        if (!colInfo.FamilyName.empty()) {
            col->SetFamilyName(colInfo.FamilyName);
        }

        if (colInfo.NotNull) {
            col->SetNotNull(colInfo.NotNull);
        }

    }

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "AlterMainTablePropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> ApplyPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpApplyIndexBuild);
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(TPath::Init(buildInfo.DomainPathId, ss).PathString());
    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    auto& indexBuild = *modifyScheme.MutableApplyIndexBuild();
    indexBuild.SetTablePath(TPath::Init(buildInfo.TablePathId, ss).PathString());

    if (buildInfo.IsBuildIndex()) {
        indexBuild.SetIndexName(buildInfo.IndexName);
    }

    indexBuild.SetSnapshotTxId(ui64(buildInfo.InitiateTxId));
    indexBuild.SetBuildIndexId(ui64(buildInfo.Id));

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "ApplyPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> UnlockPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.UnlockTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    auto addUnlock = [&](TPath path) {
        NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropLock);
        modifyScheme.SetInternal(true);
        modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

        modifyScheme.SetWorkingDir(path.Parent().PathString());

        auto& lockConfig = *modifyScheme.MutableLockConfig();
        lockConfig.SetName(path.LeafName());
    };

    addUnlock(TPath::Init(buildInfo.TablePathId, ss));

    if (buildInfo.IsValidatingUniqueIndex()
        || buildInfo.IsFlatRelevanceFulltext())
    {
        // Unlock also indexImplTable
        TPath indexImplTablePath = GetBuildPath(ss, buildInfo, NTableIndex::ImplTable);
        if (indexImplTablePath.IsResolved() && indexImplTablePath.IsLocked()) {
            addUnlock(std::move(indexImplTablePath));
        }
    }

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "UnlockPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CancelPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCancelIndexBuild);
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(TPath::Init(buildInfo.DomainPathId, ss).PathString());
    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    auto& indexBuild = *modifyScheme.MutableCancelIndexBuild();
    indexBuild.SetTablePath(TPath::Init(buildInfo.TablePathId, ss).PathString());
    indexBuild.SetIndexName(buildInfo.IndexName);
    indexBuild.SetSnapshotTxId(ui64(buildInfo.InitiateTxId));
    indexBuild.SetBuildIndexId(ui64(buildInfo.Id));

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "CancelPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropColumnsPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.DropColumnsTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropColumnBuild);
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(TPath::Init(buildInfo.DomainPathId, ss).PathString());
    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    auto* columnBuild = modifyScheme.MutableDropColumnBuild();
    columnBuild->SetSnapshotTxId(ui64(buildInfo.InitiateTxId));
    columnBuild->SetBuildIndexId(ui64(buildInfo.Id));

    buildInfo.SerializeToProto(ss, columnBuild->MutableSettings());

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "DropColumnsPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterSequencePropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ENSURE(buildInfo.IsBuildPrefixedVectorIndex(), "Unknown operation kind while building AlterSequencePropose");

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterSequence);
    modifyScheme.SetInternal(true);

    auto path = TPath::Init(buildInfo.TablePathId, ss);
    path.Dive(buildInfo.IndexName);
    path.Dive(NTableIndex::NKMeans::PrefixTable);
    modifyScheme.SetWorkingDir(path.PathString());

    // about 2 * TableSize per each prefix, see PrefixIndexDone and SendPrefixKMeansRequest()
    ui64 minValue = NTableIndex::NKMeans::SetPostingParentFlag(buildInfo.KMeans.ChildBegin + (2 * buildInfo.KMeans.TableSize) * buildInfo.Shards.size());

    auto seq = modifyScheme.MutableSequence();
    seq->SetName(NTableIndex::NKMeans::IdColumnSequence);
    seq->SetMinValue(-0x7FFFFFFFFFFFFFFF);
    seq->SetMaxValue(-1);
    seq->SetStartValue(minValue);
    seq->SetRestart(true);

    LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX,
        "AlterSequencePropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgress: public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TMap<TTabletId, THolder<IEventBase>> ToTabletSend;

    template <bool WithSnapshot = true, typename TRequest>
    TTabletId FillScanRequestCommon(TRequest& request, TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        TTabletId shardId = Self->ShardInfos.at(shardIdx).TabletID;
        request.SetTabletId(ui64(shardId));

        if constexpr (WithSnapshot) {
            if (buildInfo.SnapshotTxId) {
                Y_ENSURE(buildInfo.SnapshotStep);
                request.SetSnapshotTxId(ui64(buildInfo.SnapshotTxId));
                request.SetSnapshotStep(ui64(buildInfo.SnapshotStep));
            }
        }

        auto& shardStatus = buildInfo.Shards.at(shardIdx);
        if constexpr (requires { request.MutableKeyRange(); }) {
            if (shardStatus.LastKeyAck) {
                TSerializedTableRange range = TSerializedTableRange(shardStatus.LastKeyAck, "", false, false);
                range.Serialize(*request.MutableKeyRange());
            } else if (buildInfo.KMeans.Parent == 0) {
                shardStatus.Range.Serialize(*request.MutableKeyRange());
            }
        }

        if constexpr (requires { request.MutableScanSettings(); }) {
            request.MutableScanSettings()->CopyFrom(buildInfo.ScanSettings);
        }

        request.SetSeqNoGeneration(Self->Generation());
        request.SetSeqNoRound(++shardStatus.SeqNoRound);
        return shardId;
    }

    template <typename TRequest>
    void FillScanRequestSeed(TRequest& request) {
        request.SetSeed(gVectorIndexSeed
            ? gVectorIndexSeed
            : request.GetTabletId());
    }

    void FillBuildInfoColumns(TIndexBuildInfo& buildInfo, TTableColumns&& columns) {
        buildInfo.FillIndexColumns.clear();
        buildInfo.FillIndexColumns.reserve(columns.Keys.size());
        for (const auto& x: columns.Keys) {
            buildInfo.FillIndexColumns.emplace_back(x);
            columns.Columns.erase(x);
        }
        buildInfo.FillDataColumns.clear();
        buildInfo.FillDataColumns.reserve(columns.Columns.size());
        for (const auto& x: columns.Columns) {
            buildInfo.FillDataColumns.emplace_back(x);
        }
    }

    void SendSampleKRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ENSURE(buildInfo.IsBuildVectorIndex());
        auto ev = MakeHolder<TEvDataShard::TEvSampleKRequest>();
        ev->Record.SetId(ui64(BuildId));

        if (buildInfo.KMeans.Level == 1) {
            buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());
        } else {
            auto path = GetBuildPath(Self, buildInfo, buildInfo.KMeans.ReadFrom());
            path->PathId.ToProto(ev->Record.MutablePathId());
        }

        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings().settings();
        ev->Record.SetK(buildInfo.KMeans.K);
        ev->Record.SetMaxProbability(buildInfo.Sample.MaxProbability);
        if (buildInfo.KMeans.Parent != 0) {
            auto from = TCell::Make(buildInfo.KMeans.Parent - 1);
            auto to = TCell::Make(buildInfo.KMeans.Parent);
            TSerializedTableRange range{{&from, 1}, false, {&to, 1}, true};
            range.Serialize(*ev->Record.MutableKeyRange());
        }

        ev->Record.AddColumns(buildInfo.IndexColumns.back());
        if (buildInfo.KMeans.Level > 1 && buildInfo.KMeans.OverlapClusters > 1) {
            // Skip rows from "non-domestic" clusters to not affect K-means centroids
            ev->Record.AddColumns(NTableIndex::NKMeans::IsForeignColumn);
        }

        auto shardId = FillScanRequestCommon(ev->Record, shardIdx, buildInfo);
        FillScanRequestSeed(ev->Record);
        LOG_N("TTxBuildProgress: TEvSampleKRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendKMeansReshuffleRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ENSURE(buildInfo.IsBuildVectorIndex());
        auto ev = MakeHolder<TEvDataShard::TEvReshuffleKMeansRequest>();
        ev->Record.SetId(ui64(BuildId));

        auto path = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName);
        if (buildInfo.KMeans.Level == 1) {
            buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());
        } else {
            path.Dive(buildInfo.KMeans.ReadFrom())->PathId.ToProto(ev->Record.MutablePathId());
            path.Rise();
        }

        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings().settings();
        ev->Record.SetUpload(buildInfo.KMeans.GetUpload());
        ev->Record.SetParent(buildInfo.KMeans.Parent);
        ev->Record.SetChild(buildInfo.KMeans.Child);

        Y_ENSURE(buildInfo.Sample.Rows.size() <= buildInfo.KMeans.K);
        auto& clusters = *ev->Record.MutableClusters();
        clusters.Reserve(buildInfo.Sample.Rows.size());
        for (const auto& [_, row] : buildInfo.Sample.Rows) {
            *clusters.Add() = TSerializedCellVec::ExtractCell(row, 0).AsBuf();
        }

        ev->Record.SetDatabaseName(CanonizePath(Self->RootPathElements));
        ev->Record.SetOutputName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());

        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns.back());
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };

        ev->Record.SetOverlapClusters(buildInfo.KMeans.OverlapClusters);
        ev->Record.SetOverlapRatio(buildInfo.KMeans.OverlapRatio);
        ev->Record.SetOverlapOutForeign(buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1);

        auto shardId = FillScanRequestCommon(ev->Record, shardIdx, buildInfo);
        LOG_N("TTxBuildProgress: TEvReshuffleKMeansRequest: " << ToShortDebugString(ev->Record));

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendKMeansRecomputeRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ENSURE(buildInfo.IsBuildVectorIndex());
        auto ev = MakeHolder<TEvDataShard::TEvRecomputeKMeansRequest>();
        ev->Record.SetId(ui64(BuildId));

        auto path = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName);
        if (buildInfo.KMeans.Level == 1) {
            buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());
        } else {
            path.Dive(buildInfo.KMeans.ReadFrom())->PathId.ToProto(ev->Record.MutablePathId());
            path.Rise();
        }

        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings().settings();
        ev->Record.SetParent(buildInfo.KMeans.Parent);

        Y_ENSURE(buildInfo.Sample.Rows.size() <= buildInfo.KMeans.K);
        auto& clusters = *ev->Record.MutableClusters();
        clusters.Reserve(buildInfo.Sample.Rows.size());
        for (const auto& [_, row] : buildInfo.Sample.Rows) {
            *clusters.Add() = TSerializedCellVec::ExtractCell(row, 0).AsBuf();
        }

        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns.back());

        auto shardId = FillScanRequestCommon(ev->Record, shardIdx, buildInfo);
        LOG_N("TTxBuildProgress: TEvRecomputeKMeansRequest: " << ToShortDebugString(ev->Record));

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendKMeansLocalRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ENSURE(buildInfo.IsBuildVectorIndex());
        auto ev = MakeHolder<TEvDataShard::TEvLocalKMeansRequest>();
        ev->Record.SetId(ui64(BuildId));

        auto path = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName);
        if (buildInfo.KMeans.Level == 1) {
            buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());
        } else {
            path.Dive(buildInfo.KMeans.ReadFrom())->PathId.ToProto(ev->Record.MutablePathId());
            path.Rise();
        }
        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings().settings();
        ev->Record.SetK(buildInfo.KMeans.K);
        ev->Record.SetUpload(buildInfo.KMeans.GetUpload());

        ev->Record.SetNeedsRounds(buildInfo.KMeans.Rounds);

        if (buildInfo.KMeans.State != TIndexBuildInfo::TKMeans::MultiLocal) {
            ev->Record.SetParentFrom(buildInfo.KMeans.Parent);
            ev->Record.SetParentTo(buildInfo.KMeans.Parent);
            ev->Record.SetChild(buildInfo.KMeans.Child);
        } else {
            const auto& range = buildInfo.Shards.at(shardIdx).Range;
            const auto [parentFrom, parentTo] = buildInfo.KMeans.RangeToBorders(range);
            const auto childBegin = buildInfo.KMeans.ChildBegin + (parentFrom - buildInfo.KMeans.ParentBegin) * buildInfo.KMeans.K;
            LOG_D("shard " << shardIdx << ", parent range { From: " << parentFrom << ", To: " << parentTo << " }, child begin " << childBegin);
            ev->Record.SetParentFrom(parentFrom);
            ev->Record.SetParentTo(parentTo);
            ev->Record.SetChild(childBegin);
        }

        ev->Record.SetDatabaseName(CanonizePath(Self->RootPathElements));
        ev->Record.SetOutputName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());
        path.Rise().Dive(NTableIndex::NKMeans::LevelTable);
        ev->Record.SetLevelName(path.PathString());

        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns.back());
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };

        ev->Record.SetOverlapClusters(buildInfo.KMeans.OverlapClusters);
        ev->Record.SetOverlapRatio(buildInfo.KMeans.OverlapRatio);
        ev->Record.SetOverlapOutForeign(buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1);

        auto shardId = FillScanRequestCommon(ev->Record, shardIdx, buildInfo);
        FillScanRequestSeed(ev->Record);
        LOG_N("TTxBuildProgress: TEvLocalKMeansRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendPrefixKMeansRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ENSURE(buildInfo.IsBuildPrefixedVectorIndex());
        Y_ENSURE(buildInfo.KMeans.Parent == buildInfo.KMeans.ParentEnd());
        Y_ENSURE(buildInfo.KMeans.Level == 2);

        auto ev = MakeHolder<TEvDataShard::TEvPrefixKMeansRequest>();
        ev->Record.SetId(ui64(BuildId));

        auto path = GetBuildPath(Self, buildInfo, buildInfo.KMeans.ReadFrom());
        path->PathId.ToProto(ev->Record.MutablePathId());
        path.Rise();
        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings().settings();
        ev->Record.SetK(buildInfo.KMeans.K);
        ev->Record.SetUpload(buildInfo.KMeans.GetUpload());

        ev->Record.SetNeedsRounds(buildInfo.KMeans.Rounds);

        const auto shardIndex = buildInfo.Shards.at(shardIdx).Index;
        // about 2 * TableSize see comment in PrefixIndexDone
        ev->Record.SetChild(buildInfo.KMeans.ChildBegin + (2 * buildInfo.KMeans.TableSize) * shardIndex);

        ev->Record.SetDatabaseName(CanonizePath(Self->RootPathElements));
        ev->Record.SetOutputName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());
        path.Rise().Dive(NTableIndex::NKMeans::LevelTable);
        ev->Record.SetLevelName(path.PathString());
        path.Rise().Dive(NTableIndex::NKMeans::PrefixTable);
        ev->Record.SetPrefixName(path.PathString());

        ev->Record.SetPrefixColumns(buildInfo.IndexColumns.size() - 1);
        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns.back());
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };
        const auto& tableInfo = *Self->Tables.at(buildInfo.TablePathId);
        for (ui32 keyPos: tableInfo.KeyColumnIds) {
            ev->Record.AddSourcePrimaryKeyColumns(tableInfo.Columns.at(keyPos).Name);
        }

        ev->Record.SetOverlapClusters(buildInfo.KMeans.OverlapClusters);
        ev->Record.SetOverlapRatio(buildInfo.KMeans.OverlapRatio);
        ev->Record.SetOverlapOutForeign(buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1);

        auto shardId = FillScanRequestCommon<false>(ev->Record, shardIdx, buildInfo);
        FillScanRequestSeed(ev->Record);
        LOG_N("TTxBuildProgress: TEvPrefixKMeansRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendKMeansFilterRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ENSURE(buildInfo.IsBuildVectorIndex());
        Y_ENSURE(buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1);
        auto ev = MakeHolder<TEvDataShard::TEvFilterKMeansRequest>();
        ev->Record.SetId(ui64(BuildId));

        auto path = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName);
        path.Dive(buildInfo.KMeans.ReadFrom())->PathId.ToProto(ev->Record.MutablePathId());
        path.Rise();

        ev->Record.SetUpload(buildInfo.KMeans.GetUpload());
        ev->Record.SetOutputName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());
        ev->Record.SetDatabaseName(CanonizePath(Self->RootPathElements));

        ev->Record.SetOverlapClusters(buildInfo.KMeans.OverlapClusters);
        ev->Record.SetOverlapRatio(buildInfo.KMeans.OverlapRatio);

        const auto& shardStatus = buildInfo.Shards.at(shardIdx);
        if (shardStatus.Range.From.GetCells().size() > 1) {
            // Range start is possibly split in the middle of a token
            ev->Record.SetSkipFirstKey(true);
        }
        if (shardStatus.Range.To.GetCells().size() > 1) {
            // Range end is possibly split in the middle of a token
            ev->Record.SetSkipLastKey(true);
        }

        auto shardId = FillScanRequestCommon<false>(ev->Record, shardIdx, buildInfo);
        LOG_N("TTxBuildProgress: TEvFilterKMeansRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendUploadKMeansBordersRequest(TIndexBuildInfo& buildInfo) {
        auto buildPath = GetBuildPath(Self, buildInfo, buildInfo.KMeans.ReadFrom());
        const auto& buildTableInfo = Self->Tables.at(buildPath->PathId);
        // input build table key = __ydb_parent + original PK
        const size_t keyColumnCount = buildTableInfo->KeyColumnIds.size();

        TColumnTypes columnTypes;
        TString error;
        Y_ENSURE(ExtractTypes(buildTableInfo, columnTypes, error), error);

        // Same column order as in FilterKMeans scan
        auto types = std::make_shared<NTxProxy::TUploadTypes>();
        {
            Ydb::Type type;
            type.set_type_id(NTableIndex::NKMeans::ClusterIdType);
            types->emplace_back(NTableIndex::NKMeans::ParentColumn, type);
        }
        for (const auto& id: buildTableInfo->KeyColumnIds) {
            const auto& col = buildTableInfo->Columns.at(id);
            const auto& name = col.Name;
            if (name != NTableIndex::NKMeans::ParentColumn) {
                Ydb::Type type;
                NScheme::ProtoFromTypeInfo(columnTypes.at(name), type);
                types->emplace_back(name, type);
            }
        }
        for (const auto& col: buildTableInfo->Columns) {
            const auto& name = col.second.Name;
            if (col.second.IsKey() ||
                name == NTableIndex::NKMeans::DistanceColumn ||
                (name == NTableIndex::NKMeans::IsForeignColumn && !buildInfo.KMeans.NeedsAnotherLevel())) {
                continue;
            }
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(columnTypes.at(name), type);
            types->emplace_back(name, type);
        }

        // CellVecs from FilterKMeans scans have the following format:
        // 1) PK columns
        // 2) Parent ID column
        // 3) Distance column
        // 4) Data columns in the same order as in the original table,
        //    including __ydb_foreign if there is going to be another level
        TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> uploadRows;
        auto& borders = buildInfo.KMeans.FilterBorderRows;
        // Primary keys come first so it's totally fine to sort serialized rows :-)
        std::sort(borders.begin(), borders.end());
        TString lastKey;
        TVector<TCell> pk(keyColumnCount);
        TVector<TSerializedCellVec> lastKeyRows;
        auto finishKey = [&]() {
            NKikimr::NKMeans::FilterOverlapRows(lastKeyRows, keyColumnCount,
                buildInfo.KMeans.OverlapClusters, buildInfo.KMeans.OverlapRatio);
            for (auto& row: lastKeyRows) {
                const auto& cells = row.GetCells();
                pk[0] = cells.at(keyColumnCount-1);
                for (size_t i = 0; i < keyColumnCount-1; i++) {
                    pk[i+1] = cells.at(i);
                }
                uploadRows.emplace_back(TSerializedCellVec(pk), TSerializedCellVec(cells.Slice(1 + keyColumnCount)));
            }
            lastKey.clear();
            lastKeyRows.clear();
        };
        for (auto& row: borders) {
            TSerializedCellVec vec;
            Y_ENSURE(TSerializedCellVec::TryParse(row, vec));
            TString pk = TSerializedCellVec::Serialize(vec.GetCells().Slice(0, keyColumnCount-1));
            if (lastKey && lastKey != pk) {
                finishKey();
            }
            lastKey = std::move(pk);
            lastKeyRows.push_back(std::move(vec));
        }
        if (lastKey) {
            finishKey();
        }

        auto outputPath = buildPath.Rise().Dive(buildInfo.KMeans.WriteTo());
        auto actor = new TUploadSampleK(CanonizePath(Self->RootPathElements), outputPath.PathString(),
            buildInfo.ScanSettings, Self->SelfId(), BuildId, types, std::move(uploadRows));

        TActivationContext::AsActorContext().MakeFor(Self->SelfId()).Register(actor);

        LOG_N("TTxBuildProgress: UploadKMeansBorders: " << buildInfo);
    }

    void SendBuildSecondaryIndexRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        auto ev = MakeHolder<TEvDataShard::TEvBuildIndexCreateRequest>();
        ev->Record.SetId(ui64(BuildId));

        ev->Record.SetOwnerId(buildInfo.TablePathId.OwnerId);
        ev->Record.SetPathId(buildInfo.TablePathId.LocalPathId);

        if (buildInfo.IsBuildColumns()) {
            buildInfo.SerializeToProto(Self, ev->Record.MutableColumnBuildSettings());
        } else {
            if (buildInfo.TargetName.empty()) {
                TPath implTable = GetBuildPath(Self, buildInfo,
                    buildInfo.IsBuildPrefixedVectorIndex() ? buildInfo.KMeans.WriteTo() : NTableIndex::ImplTable);
                buildInfo.TargetName = implTable.PathString();

                const auto& implTableInfo = Self->Tables.at(implTable.Base()->PathId);
                FillBuildInfoColumns(buildInfo, NTableIndex::ExtractInfo(implTableInfo));
            }
            *ev->Record.MutableIndexColumns() = {
                buildInfo.FillIndexColumns.begin(),
                buildInfo.FillIndexColumns.end()
            };
            *ev->Record.MutableDataColumns() = {
                buildInfo.FillDataColumns.begin(),
                buildInfo.FillDataColumns.end()
            };
        }

        ev->Record.SetDatabaseName(CanonizePath(Self->RootPathElements));
        ev->Record.SetTargetName(buildInfo.TargetName);

        auto shardId = FillScanRequestCommon(ev->Record, shardIdx, buildInfo);

        LOG_N("TTxBuildProgress: TEvBuildIndexCreateRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendValidateUniqueIndexRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        auto ev = MakeHolder<TEvDataShard::TEvValidateUniqueIndexRequest>();
        auto& record = ev->Record;

        record.SetId(ui64(BuildId));

        auto& indexShardStatus = buildInfo.Shards.at(shardIdx);

        auto path = GetBuildPath(Self, buildInfo, NTableIndex::ImplTable);
        TTableInfo::TPtr table = Self->Tables.at(path->PathId);

        record.SetOwnerId(path->PathId.OwnerId);
        record.SetPathId(path->PathId.LocalPathId);

        record.SetSeqNoGeneration(Self->Generation());
        record.SetSeqNoRound(++indexShardStatus.SeqNoRound);

        *record.MutableIndexColumns() = {
            buildInfo.IndexColumns.begin(),
            buildInfo.IndexColumns.end()
        };

        TTabletId shardId = Self->ShardInfos.at(shardIdx).TabletID;
        record.SetTabletId(ui64(shardId));

        LOG_N("TTxBuildProgress: TEvValidateUniqueIndexRequest: " << record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendUploadSampleKRequest(TIndexBuildInfo& buildInfo) {
        buildInfo.Sample.MakeStrictTop(buildInfo.KMeans.K);
        auto path = GetBuildPath(Self, buildInfo, NTableIndex::NKMeans::LevelTable);
        Y_ENSURE(buildInfo.Sample.Rows.size() <= buildInfo.KMeans.K);

        TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> uploadRows;
        std::array<TCell, 2> pk;
        if (buildInfo.KMeans.IsEmpty) {
            // Generate a fake hierarchy with N levels and 1 cluster on each level for an empty vector index
            auto emptyVector = buildInfo.Clusters->GetEmptyRow();
            TVector<TCell> emptyCells = {TCell(emptyVector.data(), emptyVector.size())};
            for (ui32 level = 1; level <= buildInfo.KMeans.Levels; level++) {
                pk[0] = TCell::Make((ui64)(level-1));
                pk[1] = TCell::Make(level == buildInfo.KMeans.Levels ? NTableIndex::NKMeans::SetPostingParentFlag((ui64)level) : (ui64)level);
                uploadRows.emplace_back(TSerializedCellVec{pk}, emptyCells);
            }
        } else {
            Y_ENSURE(buildInfo.KMeans.Parent < buildInfo.KMeans.Child);
            Y_ENSURE(buildInfo.KMeans.Child != 0);
            uploadRows.reserve(buildInfo.Sample.Rows.size());
            pk[0] = TCell::Make(buildInfo.KMeans.Parent);
            auto child = buildInfo.KMeans.Child;
            if (!buildInfo.KMeans.NeedsAnotherLevel()) {
                child = NTableIndex::NKMeans::SetPostingParentFlag(child);
            } else {
                NTableIndex::NKMeans::EnsureNoPostingParentFlag(child);
            }
            for (auto& [_, row] : buildInfo.Sample.Rows) {
                pk[1] = TCell::Make(child);
                uploadRows.emplace_back(TSerializedCellVec{pk}, TSerializedCellVec(row));
                child++;
            }
        }

        auto types = std::make_shared<NTxProxy::TUploadTypes>(3);
        Ydb::Type type;
        type.set_type_id(NTableIndex::NKMeans::ClusterIdType);
        (*types)[0] = {NTableIndex::NKMeans::ParentColumn, type};
        (*types)[1] = {NTableIndex::NKMeans::IdColumn, type};
        type.set_type_id(Ydb::Type::STRING);
        (*types)[2] = {NTableIndex::NKMeans::CentroidColumn, type};

        auto actor = new TUploadSampleK(CanonizePath(Self->RootPathElements), path.PathString(),
            buildInfo.ScanSettings, Self->SelfId(), BuildId, types, std::move(uploadRows));

        TActivationContext::AsActorContext().MakeFor(Self->SelfId()).Register(actor);

        LOG_N("TTxBuildProgress: TUploadSampleK: " << buildInfo);
    }

    void SendBuildFulltextIndexRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        auto ev = MakeHolder<TEvDataShard::TEvBuildFulltextIndexRequest>();
        ev->Record.SetId(ui64(BuildId));

        buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());

        if (buildInfo.TargetName.empty()) {
            TPath implTable = GetBuildPath(Self, buildInfo, NTableIndex::ImplTable);
            buildInfo.TargetName = implTable.PathString();

            const auto& implTableInfo = Self->Tables.at(implTable.Base()->PathId);
            FillBuildInfoColumns(buildInfo, NTableIndex::ExtractInfo(implTableInfo));
        }
        ev->Record.SetDatabaseName(CanonizePath(Self->RootPathElements));
        ev->Record.SetIndexName(buildInfo.TargetName);
        ev->Record.SetDocsTableName(GetBuildPath(Self, buildInfo, NTableIndex::NFulltext::DocsTable).PathString());
        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TFulltextIndexDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings();
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };

        auto shardId = FillScanRequestCommon(ev->Record, shardIdx, buildInfo);

        LOG_N("TTxBuildProgress: TEvBuildFulltextIndexRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendBuildFulltextDictRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        auto ev = MakeHolder<TEvDataShard::TEvBuildFulltextDictRequest>();
        ev->Record.SetId(ui64(BuildId));
        ev->Record.SetDatabaseName(CanonizePath(Self->RootPathElements));

        auto path = GetBuildPath(Self, buildInfo, NTableIndex::ImplTable);
        path->PathId.ToProto(ev->Record.MutablePathId());
        ev->Record.SetReadShadowData(true);

        path.Rise().Dive(NTableIndex::NFulltext::DictTable);
        ev->Record.SetOutputName(path.PathString());

        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TFulltextIndexDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings();

        const auto& shardStatus = buildInfo.Shards.at(shardIdx);
        if (shardStatus.Range.From.GetCells().size() > 1) {
            // Range start is possibly split in the middle of a token
            ev->Record.SetSkipFirstToken(true);
        }
        if (shardStatus.Range.To.GetCells().size() > 1) {
            // Range end is possibly split in the middle of a token
            ev->Record.SetSkipLastToken(true);
        }

        auto shardId = FillScanRequestCommon<false>(ev->Record, shardIdx, buildInfo);

        LOG_N("TTxBuildProgress: TEvBuildFulltextDictRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendUploadFulltextStatsRequest(TIndexBuildInfo& buildInfo) {
        ui64 totalDocLength = 0;
        ui64 docCount = 0;
        for (auto& [shardIdx, shardStatus]: buildInfo.Shards) {
            docCount += shardStatus.DocCount;
            totalDocLength += shardStatus.TotalDocLength;
        }

        TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> uploadRows;
        uploadRows.emplace_back(TSerializedCellVec{TVector<TCell>{TCell::Make((ui32)0)}},
            TSerializedCellVec{TVector<TCell>{TCell::Make(docCount), TCell::Make(totalDocLength)}});

        auto types = std::make_shared<NTxProxy::TUploadTypes>(3);
        Ydb::Type type;
        type.set_type_id(Ydb::Type::UINT32);
        (*types)[0] = {NTableIndex::NFulltext::IdColumn, type};
        type.set_type_id(NTableIndex::NFulltext::DocCountType);
        (*types)[1] = {NTableIndex::NFulltext::DocCountColumn, type};
        (*types)[2] = {NTableIndex::NFulltext::SumDocLengthColumn, type};

        auto path = GetBuildPath(Self, buildInfo, NTableIndex::NFulltext::StatsTable);
        auto actor = new TUploadSampleK(CanonizePath(Self->RootPathElements), path.PathString(),
            buildInfo.ScanSettings, Self->SelfId(), BuildId, types, std::move(uploadRows));

        TActivationContext::AsActorContext().MakeFor(Self->SelfId()).Register(actor);

        LOG_N("TTxBuildProgress: TUploadFulltextStats: " << buildInfo);
    }

    void SendUploadFulltextBordersRequest(TIndexBuildInfo& buildInfo) {
        TMap<TString, NTableIndex::NFulltext::TDocCount> borders;
        for (auto& [shardIdx, shardStatus]: buildInfo.Shards) {
            if (shardStatus.FirstTokenRows) {
                borders[shardStatus.FirstToken] += shardStatus.FirstTokenRows;
            }
            if (shardStatus.LastTokenRows) {
                borders[shardStatus.LastToken] += shardStatus.LastTokenRows;
            }
        }

        TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> uploadRows;
        for (auto& [token, docCount]: borders) {
            uploadRows.emplace_back(TSerializedCellVec{TVector<TCell>{TCell(token)}},
                TSerializedCellVec{TVector<TCell>{TCell::Make(docCount)}});
        }

        auto mainTablePath = TPath::Init(buildInfo.TablePathId, Self);
        const auto& mainTableInfo = Self->Tables.at(mainTablePath->PathId);

        TColumnTypes baseColumnTypes;
        TString error;
        Y_ENSURE(ExtractTypes(mainTableInfo, baseColumnTypes, error), error);
        Y_ENSURE(buildInfo.IndexColumns.size() == 1);
        auto textColumnInfo = baseColumnTypes.at(buildInfo.IndexColumns[0]);

        auto types = std::make_shared<NTxProxy::TUploadTypes>(2);
        Ydb::Type type;
        NScheme::ProtoFromTypeInfo(textColumnInfo, type);
        (*types)[0] = {NTableIndex::NFulltext::TokenColumn, type};
        type.set_type_id(NTableIndex::NFulltext::DocCountType);
        (*types)[1] = {NTableIndex::NFulltext::FreqColumn, type};

        auto path = GetBuildPath(Self, buildInfo, NTableIndex::NFulltext::DictTable);
        auto actor = new TUploadSampleK(CanonizePath(Self->RootPathElements), path.PathString(),
            buildInfo.ScanSettings, Self->SelfId(), BuildId, types, std::move(uploadRows));

        TActivationContext::AsActorContext().MakeFor(Self->SelfId()).Register(actor);

        LOG_N("TTxBuildProgress: TUploadFulltextStats: " << buildInfo);
    }

    void ClearAfterFill(const TActorContext& ctx, TIndexBuildInfo& buildInfo) {
        buildInfo.DoneShards = {};
        buildInfo.InProgressShards = {};
        buildInfo.ToUploadShards = {};

        ToTabletSend.clear();
        Self->IndexBuildPipes.CloseAll(BuildId, ctx);
    }

    template<typename Send>
    bool SendToShards(TIndexBuildInfo& buildInfo, Send&& send) {
        while (!buildInfo.ToUploadShards.empty() && buildInfo.InProgressShards.size() < buildInfo.MaxInProgressShards) {
            auto shardIdx = buildInfo.ToUploadShards.front();
            buildInfo.ToUploadShards.pop_front();
            buildInfo.InProgressShards.emplace(shardIdx);
            send(shardIdx);
        }

        return buildInfo.InProgressShards.empty() && buildInfo.ToUploadShards.empty();
    }

    void AddShard(TIndexBuildInfo& buildInfo, const TShardIdx& idx, const TIndexBuildShardStatus& status) {
        switch (status.Status) {
            case NKikimrIndexBuilder::EBuildStatus::INVALID:
            case NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
            case NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
            case NKikimrIndexBuilder::EBuildStatus::ABORTED:
                buildInfo.ToUploadShards.emplace_back(idx);
                break;
            case NKikimrIndexBuilder::EBuildStatus::DONE:
                buildInfo.DoneShards.emplace_back(idx);
                break;
            case NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
            case NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                Y_ENSURE(false, "Unreachable");
        }
    }

    bool NoShardsAdded(TIndexBuildInfo& buildInfo) {
        return buildInfo.DoneShards.empty() && buildInfo.InProgressShards.empty() && buildInfo.ToUploadShards.empty();
    }

    void AddAllShards(TIndexBuildInfo& buildInfo) {
        ToTabletSend.clear();
        Self->IndexBuildPipes.CloseAll(BuildId, Self->ActorContext());

        for (const auto& [idx, status] : buildInfo.Shards) {
            AddShard(buildInfo, idx, status);
        }
    }

    void AddGlobalShardsForCurrentParent(TIndexBuildInfo& buildInfo) {
        Y_ENSURE(NoShardsAdded(buildInfo));
        auto it = buildInfo.Cluster2Shards.lower_bound(buildInfo.KMeans.Parent);
        Y_ENSURE(it != buildInfo.Cluster2Shards.end());
        if (it->second.Shards.size() > 1) {
            for (const auto& idx : it->second.Shards) {
                const auto& status = buildInfo.Shards.at(idx);
                AddShard(buildInfo, idx, status);
            }
        }
    }

    void AddLocalClusters(TIndexBuildInfo& buildInfo) {
        Y_ENSURE(NoShardsAdded(buildInfo));
        for (const auto& [to, state] : buildInfo.Cluster2Shards) {
            if (state.Shards.size() == 1) {
                const auto* status = buildInfo.Shards.FindPtr(state.Shards[0]);
                Y_ENSURE(status);
                AddShard(buildInfo, state.Shards[0], *status);
            }
        }
    }

    bool FillSecondaryIndex(TIndexBuildInfo& buildInfo) {
        LOG_D("FillSecondaryIndex Start");

        if (NoShardsAdded(buildInfo)) {
            AddAllShards(buildInfo);
        }

        auto done = SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendBuildSecondaryIndexRequest(shardIdx, buildInfo); }) &&
               buildInfo.DoneShards.size() == buildInfo.Shards.size();

        if (done) {
            LOG_D("FillSecondaryIndex Done");
        }

        return done;
    }

    bool ValidateSecondaryUniqueIndex(TIndexBuildInfo& buildInfo) {
        LOG_D("ValidateSecondaryUniqueIndex Start");

        if (NoShardsAdded(buildInfo)) {
            AddAllShards(buildInfo);
        }

        auto done = SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendValidateUniqueIndexRequest(shardIdx, buildInfo); }) &&
               buildInfo.DoneShards.size() == buildInfo.Shards.size();

        if (done) {
            LOG_D("ValidateSecondaryUniqueIndex Done");
        }

        return done;
    }

    bool FillSecondaryUniqueIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        switch (buildInfo.SubState) {
        case TIndexBuildInfo::ESubState::None: {
            if (FillSecondaryIndex(buildInfo)) {
                ClearAfterFill(TActivationContext::AsActorContext(), buildInfo);

                // After filling unique index we need to validate it.
                // This includes:
                // - Locking index shards
                // - Validating each index shard for index keys uniqueness
                // - Applying cross-shard validation
                NIceDb::TNiceDb db{txc.DB};
                buildInfo.SubState = TIndexBuildInfo::ESubState::UniqIndexValidation;
                Self->PersistBuildIndexState(db, buildInfo);
                Self->PersistBuildIndexShardStatusReset(db, buildInfo);
                ChangeState(BuildId, TIndexBuildInfo::EState::LockBuild);
                Progress(BuildId);
            }
            return false;
        }
        case TIndexBuildInfo::ESubState::UniqIndexValidation: {
            const bool done = ValidateSecondaryUniqueIndex(buildInfo);
            if (done) {
                TString errorDesc;
                if (!PerformCrossShardUniqIndexValidation(buildInfo, errorDesc)) {
                    NIceDb::TNiceDb db(txc.DB);
                    Self->PersistBuildIndexAddIssue(db, buildInfo, errorDesc);
                    ChangeState(BuildId, TIndexBuildInfo::EState::Rejection_Applying);
                    Progress(BuildId);
                    return false;
                }
            }
            return done;
        }
        default:
            Y_ENSURE(false);
        }
    }

    bool FillPrefixKMeans(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (NoShardsAdded(buildInfo)) {
            AddAllShards(buildInfo);
        }
        size_t i = 0;
        for (auto& [shardIdx, shardStatus]: buildInfo.Shards) {
            shardStatus.Index = i++;
        }
        // Set correct start value for the prefix ID sequence
        if (!buildInfo.KMeans.AlterPrefixSequenceDone) {
            // Alter the sequence
            buildInfo.KMeans.AlterPrefixSequenceDone = true;
            NIceDb::TNiceDb db{txc.DB};
            ChangeState(BuildId, TIndexBuildInfo::EState::AlterSequence);
            Progress(BuildId);
            return false;
        }
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendPrefixKMeansRequest(shardIdx, buildInfo); }) &&
               buildInfo.DoneShards.size() == buildInfo.Shards.size();
    }

    bool FillLocalKMeans(TIndexBuildInfo& buildInfo) {
        if (NoShardsAdded(buildInfo)) {
            AddAllShards(buildInfo);
        }
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendKMeansLocalRequest(shardIdx, buildInfo); }) &&
               buildInfo.DoneShards.size() == buildInfo.Shards.size();
    }

    bool SendKMeansSample(TIndexBuildInfo& buildInfo) {
        if (buildInfo.Sample.MaxProbability == 0) {
            buildInfo.ToUploadShards.clear();
            buildInfo.InProgressShards.clear();
            return true;
        }
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendSampleKRequest(shardIdx, buildInfo); });
    }

    bool SendKMeansReshuffle(TIndexBuildInfo& buildInfo) {
        buildInfo.Sample.MakeStrictTop(buildInfo.KMeans.K);
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendKMeansReshuffleRequest(shardIdx, buildInfo); });
    }

    bool SendKMeansRecompute(TIndexBuildInfo& buildInfo) {
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendKMeansRecomputeRequest(shardIdx, buildInfo); });
    }

    bool SendKMeansFilter(TIndexBuildInfo& buildInfo) {
        if (NoShardsAdded(buildInfo)) {
            AddAllShards(buildInfo);
        }
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendKMeansFilterRequest(shardIdx, buildInfo); });
    }

    bool SendKMeansLocal(TIndexBuildInfo& buildInfo) {
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendKMeansLocalRequest(shardIdx, buildInfo); });
    }

    bool SendKMeansBorders(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Collect) {
            LOG_D("FillVectorIndex FilterBorders " << buildInfo.DebugString());
            buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Upload;
            SendUploadKMeansBordersRequest(buildInfo);
            Progress(BuildId);
        } else if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Done) {
            LOG_D("FillVectorIndex FilterBorders Done " << buildInfo.DebugString());
            NIceDb::TNiceDb db{txc.DB};
            buildInfo.SubState = TIndexBuildInfo::ESubState::None;
            Self->PersistBuildIndexState(db, buildInfo);
            for (ui32 row = 0; row < buildInfo.KMeans.FilterBorderRows.size(); row++) {
                db.Table<Schema::KMeansTreeSample>().Key(buildInfo.Id, row).Delete();
            }
            buildInfo.KMeans.FilterBorderRows.clear();
            buildInfo.Sample.Clear();
            return true;
        }
        // Wait for upload
        return false;
    }

    void ClearDoneShards(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (buildInfo.DoneShards.empty()) {
            return;
        }
        NIceDb::TNiceDb db{txc.DB};
        for (const auto& idx : buildInfo.DoneShards) {
            auto& status = buildInfo.Shards.at(idx);
            Self->PersistBuildIndexShardStatusReset(db, BuildId, idx, status);
        }
        buildInfo.DoneShards.clear();
        Self->PersistBuildIndexProcessed(db, buildInfo);
    }

    void PersistKMeansState(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        NIceDb::TNiceDb db{txc.DB};
        Self->PersistBuildIndexKMeansState(db, buildInfo);
    }

    bool FillPrefixedVectorIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        LOG_D("FillPrefixedVectorIndex Start " << buildInfo.DebugString());

        if (buildInfo.KMeans.Level == 1) {
            if (!FillSecondaryIndex(buildInfo)) {
                return false;
            }
            LOG_D("FillPrefixedVectorIndex DoneLevel " << buildInfo.DebugString());

            const ui64 doneShards = buildInfo.DoneShards.size();
            ClearDoneShards(txc, buildInfo);
            // it's approximate but upper bound, so it's ok
            buildInfo.KMeans.TableSize = std::max<ui64>(1, buildInfo.Processed.GetUploadRows());
            buildInfo.KMeans.PrefixIndexDone(doneShards);
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::MultiLocal;
            LOG_D("FillPrefixedVectorIndex PrefixIndexDone " << buildInfo.DebugString());

            PersistKMeansState(txc, buildInfo);
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexShardStatusReset(db, buildInfo);
            ChangeState(BuildId, TIndexBuildInfo::EState::CreateBuild);
            Progress(BuildId);
            return false;
        } else if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::Filter) {
            bool filled = SendKMeansFilter(buildInfo);
            if (!filled) {
                return false;
            }
            ClearDoneShards(txc, buildInfo);
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::FilterBorders;
            buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Collect;
            PersistKMeansState(txc, buildInfo);
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexState(db, buildInfo);
            Progress(BuildId);
            return false;
        } else if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::FilterBorders) {
            if (!SendKMeansBorders(txc, buildInfo)) {
                return false;
            }
            // continue to NextLevel
        } else {
            Y_ENSURE(buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::MultiLocal);
            bool filled = buildInfo.KMeans.Level == 2
                ? FillPrefixKMeans(txc, buildInfo)
                : FillLocalKMeans(buildInfo);
            if (!filled) {
                return false;
            }
            if (buildInfo.KMeans.OverlapClusters > 1) {
                LOG_D("FillPrefixedVectorIndex Filter " << buildInfo.DebugString());
                buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Filter;
                PersistKMeansState(txc, buildInfo);
                ClearDoneShards(txc, buildInfo);
                NIceDb::TNiceDb db{txc.DB};
                Self->PersistBuildIndexShardStatusReset(db, buildInfo);
                ChangeState(BuildId, TIndexBuildInfo::EState::DropBuild);
                Progress(BuildId);
                return false;
            }
            // continue to NextLevel
        }

        LOG_D("FillPrefixedVectorIndex DoneLevel " << buildInfo.DebugString());

        ClearDoneShards(txc, buildInfo);
        const bool needsAnotherLevel = buildInfo.KMeans.NextLevel();
        buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::MultiLocal;
        if (buildInfo.KMeans.Level == 2) {
            buildInfo.KMeans.Parent = buildInfo.KMeans.ParentEnd();
        }
        LOG_D("FillPrefixedVectorIndex NextLevel " << buildInfo.DebugString());

        PersistKMeansState(txc, buildInfo);
        NIceDb::TNiceDb db{txc.DB};
        Self->PersistBuildIndexShardStatusReset(db, buildInfo);
        if (!needsAnotherLevel) {
            LOG_D("FillPrefixedVectorIndex Done " << buildInfo.DebugString());
            return true;
        }
        ChangeState(BuildId, TIndexBuildInfo::EState::DropBuild);
        Progress(BuildId);
        return false;
    }

    bool FillVectorIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        LOG_D("FillVectorIndex Start " << buildInfo.DebugString());

        // (Sample -> Recompute* -> Reshuffle)* -> MultiLocal -> (Filter)? -> NextLevel
        if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::Sample) {
            return FillVectorIndexSamples(txc, buildInfo);
        } else if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::Recompute) {
            if (NoShardsAdded(buildInfo)) {
                AddGlobalShardsForCurrentParent(buildInfo);
            }
            if (!SendKMeansRecompute(buildInfo)) {
                return false;
            }
            ClearDoneShards(txc, buildInfo);
            // Finalize cluster aggregation and move them to KMeansTreeSample
            NIceDb::TNiceDb db{txc.DB};
            bool lastRound = buildInfo.Clusters->NextRound();
            Self->PersistBuildIndexClustersToSample(db, buildInfo);
            if (!lastRound) {
                // Recompute again
                buildInfo.KMeans.Round++;
            } else {
                // Cluster generation completed, save clusters
                LOG_D("FillVectorIndex SendUploadClusters " << buildInfo.DebugString());
                buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Sample;
                buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Upload;
                SendUploadSampleKRequest(buildInfo);
            }
            LOG_D("FillVectorIndex NextState " << buildInfo.DebugString());
            PersistKMeansState(txc, buildInfo);
            Progress(BuildId);
            return false;
        } else if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::Reshuffle) {
            if (NoShardsAdded(buildInfo)) {
                AddGlobalShardsForCurrentParent(buildInfo);
            }
            if (!SendKMeansReshuffle(buildInfo)) {
                return false;
            }
            ClearDoneShards(txc, buildInfo);
            buildInfo.Sample.Clear();
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexSampleForget(db, buildInfo);
            return FillVectorIndexNextParent(txc, buildInfo);
        } else if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::MultiLocal) {
            if (!SendKMeansLocal(buildInfo)) {
                return false;
            }
            ClearDoneShards(txc, buildInfo);
            return FillVectorIndexNextParent(txc, buildInfo);
        } else if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::Filter) {
            if (!SendKMeansFilter(buildInfo)) {
                return false;
            }
            ClearDoneShards(txc, buildInfo);
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::FilterBorders;
            buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Collect;
            PersistKMeansState(txc, buildInfo);
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexState(db, buildInfo);
            Progress(BuildId);
            return false;
        } else if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::FilterBorders) {
            if (!SendKMeansBorders(txc, buildInfo)) {
                return false;
            }
            return FillVectorIndexNextLevel(txc, buildInfo);
        }
        Y_ENSURE(false);
    }

    bool FillVectorIndexSamples(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Collect) {
            if (NoShardsAdded(buildInfo)) {
                AddGlobalShardsForCurrentParent(buildInfo);
                if (!buildInfo.DoneShards.size() && !buildInfo.ToUploadShards.size()) {
                    // No "global" shards to handle - parent only has 1 shard,
                    // it will be handled during the MultiLocal phase
                    return FillVectorIndexNextParent(txc, buildInfo);
                }
                // Otherwise, we collect samples
                LOG_D("FillVectorIndex Samples " << buildInfo.DebugString());
            }
            if (!SendKMeansSample(buildInfo)) {
                return false;
            }
            ClearDoneShards(txc, buildInfo);
            if (buildInfo.Sample.Rows.empty()) {
                // No samples
                if (buildInfo.KMeans.Parent == 0) {
                    // Index is empty - add 1 leaf cluster for future index updates
                    buildInfo.KMeans.IsEmpty = true;
                    PersistKMeansState(txc, buildInfo);
                    return FillVectorIndexNextParent(txc, buildInfo);
                }
                // No data for a specific cluster - should not happen
                // Supported to not crash if we have duplicate clusters for some reason
                return FillVectorIndexNextParent(txc, buildInfo);
            }
            if (buildInfo.KMeans.Rounds > 1) {
                LOG_D("FillVectorIndex Recompute " << buildInfo.DebugString());
                buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Recompute;
                buildInfo.KMeans.Round = 1;
                // Initialize Clusters
                NIceDb::TNiceDb db(txc.DB);
                buildInfo.Sample.MakeStrictTop(buildInfo.KMeans.K);
                Self->PersistBuildIndexSampleToClusters(db, buildInfo);
                buildInfo.Clusters->SetRound(1);
                PersistKMeansState(txc, buildInfo);
                Progress(BuildId);
            } else {
                LOG_D("FillVectorIndex SendUploadSampleKRequest " << buildInfo.DebugString());
                SendUploadSampleKRequest(buildInfo);
                buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Upload;
            }
            return false;
        } else if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Upload) {
            // Just wait until samples are uploaded (saved)
            return false;
        } else if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Done) {
            if (buildInfo.Sample.Rows.empty() && buildInfo.KMeans.Parent == 0) {
                // Done
                return true;
            }
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Reshuffle;
            LOG_D("FillVectorIndex NextState " << buildInfo.DebugString());
            PersistKMeansState(txc, buildInfo);
            Progress(BuildId);
            return false;
        }
        Y_ENSURE(false);
    }

    bool FillVectorIndexNextParent(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (buildInfo.KMeans.NextParent()) {
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Sample;
            LOG_D("FillVectorIndex NextParent " << buildInfo.DebugString());
            PersistKMeansState(txc, buildInfo);
            Progress(BuildId);
            return false;
        }

        if (!buildInfo.Cluster2Shards.empty()) {
            AddLocalClusters(buildInfo);
            buildInfo.Cluster2Shards.clear();
            if (!buildInfo.ToUploadShards.empty()) {
                LOG_D("FillVectorIndex MultiKMeans " << buildInfo.DebugString());
                buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::MultiLocal;
                PersistKMeansState(txc, buildInfo);
                Progress(BuildId);
                return false;
            }
        }

        return FillVectorIndexFilter(txc, buildInfo);
    }

    bool FillVectorIndexFilter(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (!buildInfo.KMeans.IsEmpty && buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1) {
            LOG_D("FillVectorIndex Filter " << buildInfo.DebugString());
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Filter;
            ClearDoneShards(txc, buildInfo);
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexShardStatusReset(db, buildInfo);
            ChangeState(BuildId, buildInfo.KMeans.Level > 1
                ? TIndexBuildInfo::EState::DropBuild
                : TIndexBuildInfo::EState::CreateBuild);
            PersistKMeansState(txc, buildInfo);
            Progress(BuildId);
            return false;
        }

        return FillVectorIndexNextLevel(txc, buildInfo);
    }

    bool FillVectorIndexNextLevel(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (!buildInfo.KMeans.IsEmpty && buildInfo.KMeans.NextLevel()) {
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Sample;
            LOG_D("FillVectorIndex NextLevel " << buildInfo.DebugString());
            PersistKMeansState(txc, buildInfo);
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexShardStatusReset(db, buildInfo);
            ChangeState(BuildId, buildInfo.KMeans.Level > 2 || buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1
                ? TIndexBuildInfo::EState::DropBuild
                : TIndexBuildInfo::EState::CreateBuild);
            Progress(BuildId);
            return false;
        }

        if (buildInfo.KMeans.IsEmpty) {
            if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Collect) {
                LOG_D("FillVectorIndex UploadEmpty " << buildInfo.DebugString());
                buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Upload;
                SendUploadSampleKRequest(buildInfo);
                return false;
            } else if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Upload) {
                // Wait
                return false;
            } else if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Done) {
                // Pass through
            } else {
                Y_ENSURE(false);
            }
        }

        LOG_D("FillVectorIndex Done " << buildInfo.DebugString());
        return true;
    }

    bool FillFulltextIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        bool done = false;

        switch (buildInfo.SubState) {
        case TIndexBuildInfo::ESubState::None:
            // Stage 1 for FLAT_RELEVANCE - build "posting" table (token-documents)
            LOG_D("FillFulltextIndex Posting");
            if (NoShardsAdded(buildInfo)) {
                AddAllShards(buildInfo);
            }
            done = SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendBuildFulltextIndexRequest(shardIdx, buildInfo); }) &&
                buildInfo.DoneShards.size() == buildInfo.Shards.size();
            if (done) {
                LOG_D("FillFulltextIndex Posting Done");
                auto settings = std::get<NKikimrSchemeOp::TFulltextIndexDescription>(buildInfo.SpecializedIndexDescription).GetSettings();
                if (settings.layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE) {
                    NIceDb::TNiceDb db{txc.DB};
                    buildInfo.SubState = TIndexBuildInfo::ESubState::FulltextIndexStats;
                    buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Collect;
                    Self->PersistBuildIndexState(db, buildInfo);
                    Progress(BuildId);
                    done = false;
                }
            }
            break;
        case TIndexBuildInfo::ESubState::FulltextIndexStats:
            // Stage 2 for FLAT_RELEVANCE - build statistics table (DocCount & TotalDocLength)
            if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Collect) {
                LOG_D("FillFulltextIndex SendUploadStats " << buildInfo.DebugString());
                buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Upload;
                SendUploadFulltextStatsRequest(buildInfo);
                Progress(BuildId);
            } else if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Done) {
                LOG_D("FillFulltextIndex UploadStats Done " << buildInfo.DebugString());
                ClearDoneShards(txc, buildInfo);
                NIceDb::TNiceDb db{txc.DB};
                buildInfo.SubState = TIndexBuildInfo::ESubState::FulltextIndexDictionary;
                Self->PersistBuildIndexState(db, buildInfo);
                Self->PersistBuildIndexShardStatusReset(db, buildInfo);
                ChangeState(BuildId, TIndexBuildInfo::EState::LockBuild);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::ESubState::FulltextIndexDictionary:
            // Stage 3 for FLAT_RELEVANCE - build dictionary table
            LOG_D("FillFulltextIndex Dictionary");
            if (NoShardsAdded(buildInfo)) {
                AddAllShards(buildInfo);
            }
            done = SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendBuildFulltextDictRequest(shardIdx, buildInfo); }) &&
                buildInfo.DoneShards.size() == buildInfo.Shards.size();
            if (done) {
                LOG_D("FillFulltextIndex Dictionary Done");
                NIceDb::TNiceDb db{txc.DB};
                buildInfo.SubState = TIndexBuildInfo::ESubState::FulltextIndexBorders;
                buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Collect;
                Self->PersistBuildIndexState(db, buildInfo);
                Progress(BuildId);
                done = false;
            }
            break;
        case TIndexBuildInfo::ESubState::FulltextIndexBorders:
            // Stage 4 for FLAT_RELEVANCE - fill border values for dictionary
            if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Collect) {
                LOG_D("FillFulltextIndex SendUploadBorders " << buildInfo.DebugString());
                buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Upload;
                SendUploadFulltextBordersRequest(buildInfo);
                Progress(BuildId);
            } else if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Done) {
                LOG_D("FillFulltextIndex UploadBorders Done " << buildInfo.DebugString());
                ClearDoneShards(txc, buildInfo);
                NIceDb::TNiceDb db{txc.DB};
                buildInfo.SubState = TIndexBuildInfo::ESubState::None;
                Self->PersistBuildIndexState(db, buildInfo);
                Self->PersistBuildIndexShardStatusReset(db, buildInfo);
                done = true;
            }
            break;
        default:
            Y_ENSURE(false);
        }

        if (done) {
            LOG_D("FillFulltextIndex Done");
        }

        return done;
    }

    bool FillIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        // for now build index impl tables don't need snapshot,
        // because they're used only by build index
        if (!buildInfo.SnapshotTxId && GetShardsPath(buildInfo)->PathId == buildInfo.TablePathId) {
            Y_ENSURE(Self->TablesWithSnapshots.contains(buildInfo.TablePathId));
            Y_ENSURE(Self->TablesWithSnapshots.at(buildInfo.TablePathId) == buildInfo.InitiateTxId);

            buildInfo.SnapshotTxId = buildInfo.InitiateTxId;
            Y_ENSURE(buildInfo.SnapshotTxId);
            buildInfo.SnapshotStep = Self->SnapshotsStepIds.at(buildInfo.SnapshotTxId);
            Y_ENSURE(buildInfo.SnapshotStep);
        }
        if (buildInfo.Shards.empty()) {
            NIceDb::TNiceDb db(txc.DB);
            if (!InitiateShards(db, buildInfo)) {
                return false;
            }
        }
        switch (buildInfo.BuildKind) {
            case TIndexBuildInfo::EBuildKind::BuildSecondaryIndex:
            case TIndexBuildInfo::EBuildKind::BuildColumns:
                return FillSecondaryIndex(buildInfo);
            case TIndexBuildInfo::EBuildKind::BuildSecondaryUniqueIndex:
                return FillSecondaryUniqueIndex(txc, buildInfo);
            case TIndexBuildInfo::EBuildKind::BuildVectorIndex:
                return FillVectorIndex(txc, buildInfo);
            case TIndexBuildInfo::EBuildKind::BuildPrefixedVectorIndex:
                return FillPrefixedVectorIndex(txc, buildInfo);
            case TIndexBuildInfo::EBuildKind::BuildFulltext:
                return FillFulltextIndex(txc, buildInfo);
            default:
                Y_ENSURE(false, buildInfo.InvalidBuildKind());
                return true;
        }
    }

    bool PerformCrossShardUniqIndexValidation(TIndexBuildInfo& buildInfo, TString& errorDesc) const {
        if (buildInfo.Shards.size() == 1) {
            return true;
        }

        LOG_N("TTxBuildProgress: Performing cross shard unique index validation: " << BuildId << " " << buildInfo.State);

        auto path = GetBuildPath(Self, buildInfo, NTableIndex::ImplTable);
        TTableInfo::TPtr table = Self->Tables.at(path->PathId);

        // Make index columns type info
        std::vector<NScheme::TTypeInfoOrder> indexColumnTypeInfos;
        indexColumnTypeInfos.reserve(buildInfo.IndexColumns.size());
        for (const TString& columnName : buildInfo.IndexColumns) {
            bool found = false;
            for (auto&& [_, columnInfo] : table->Columns) {
                if (columnInfo.Name == columnName) {
                    indexColumnTypeInfos.emplace_back(columnInfo.PType);
                    found = true;
                    break;
                }
            }
            Y_ENSURE(found);
        }

        // Arrange shards in ascending order by shard index
        std::vector<const TSerializedTableRange*> sortedRanges;
        sortedRanges.reserve(buildInfo.Shards.size());
        Y_ENSURE(buildInfo.Shards.size() == table->GetPartitions().size());
        for (const auto& x: table->GetPartitions()) {
            auto it = buildInfo.Shards.find(x.ShardIdx);
            Y_ENSURE(it != buildInfo.Shards.end());
            Y_ENSURE(it->second.Status == NKikimrIndexBuilder::EBuildStatus::DONE);
            sortedRanges.emplace_back(&it->second.Range);
        }

        if (!NSchemeShard::PerformCrossShardUniqIndexValidation(indexColumnTypeInfos, buildInfo.IndexColumns, sortedRanges, errorDesc)) {
            LOG_E("TTxBuildProgress: Cross shard index validation failed. " << errorDesc << ". Cancelling unique index build");
            return false;
        }
        return true;
    }

    bool CanProgressBuilding(const TIndexBuildInfo& buildInfo) const {
        return !buildInfo.IsBuildColumns() || Self->EnableAddColumsWithDefaults;
    }

public:
    explicit TTxProgress(TSelf* self, TIndexBuildId buildId)
        : TTxBase(self, buildId, TXTYPE_PROGRESS_INDEX_BUILD)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        Y_ENSURE(buildInfoPtr);
        auto& buildInfo = *buildInfoPtr->get();

        LOG_N("TTxBuildProgress: Execute: " << BuildId << " " << buildInfo.State);
        LOG_D("TTxBuildProgress: Execute: " << BuildId << " " << buildInfo.State << " " << buildInfo);

        if (buildInfo.IsBroken) {
            return true;
        }

        switch (buildInfo.State) {
        case TIndexBuildInfo::EState::Invalid:
            Y_ENSURE(false, "Unreachable");

        case TIndexBuildInfo::EState::Locking:
            if (buildInfo.LockTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.LockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), LockPropose(Self, buildInfo, buildInfo.LockTxId, TPath::Init(buildInfo.TablePathId, Self)), 0, ui64(BuildId));
            } else if (!buildInfo.LockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.LockTxId)));
            } else {
                if (buildInfo.IsBuildColumns()) {
                    ChangeState(BuildId, TIndexBuildInfo::EState::AlterMainTable);
                } else {
                    ChangeState(BuildId, TIndexBuildInfo::EState::Initiating);
                }
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::AlterMainTable:
            Y_ENSURE(buildInfo.IsBuildColumns());
            if (buildInfo.AlterMainTableTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.AlterMainTableTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), AlterMainTablePropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.AlterMainTableTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.AlterMainTableTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Initiating);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::GatheringStatistics:
            ChangeState(BuildId, TIndexBuildInfo::EState::Initiating);
            Progress(BuildId);
            break;
        case TIndexBuildInfo::EState::Initiating:
            if (buildInfo.InitiateTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.InitiateTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CreateIndexPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.InitiateTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.InitiateTxId)));
            } else {
                if (buildInfo.IsBuildVectorIndex() && buildInfo.KMeans.NeedsAnotherLevel()) {
                    ChangeState(BuildId, TIndexBuildInfo::EState::CreateBuild);
                } else {
                    ChangeState(BuildId, TIndexBuildInfo::EState::Filling);
                }
                Progress(BuildId);
            }

            break;
        case TIndexBuildInfo::EState::Filling: {
            if (!CanProgressBuilding(buildInfo) || buildInfo.IsCancellationRequested() || FillIndex(txc, buildInfo)) {
                auto nextState = TIndexBuildInfo::EState::Applying;
                if (!CanProgressBuilding(buildInfo)) {
                    Y_ENSURE(buildInfo.IsBuildColumns());
                    buildInfo.AddIssue(TStringBuilder() << "Adding columns with defaults is disabled");
                    nextState = TIndexBuildInfo::EState::Rejection_DroppingColumns;
                } else if (buildInfo.IsCancellationRequested()) {
                    nextState = buildInfo.IsBuildColumns()
                        ? TIndexBuildInfo::EState::Cancellation_DroppingColumns
                        : TIndexBuildInfo::EState::Cancellation_Applying;
                }

                ClearAfterFill(ctx, buildInfo);
                ChangeState(BuildId, nextState);
                Progress(BuildId);

                // make final bill
                Bill(buildInfo);
            } else {
                AskToScheduleBilling(buildInfo);
            }
            break;
        }
        case TIndexBuildInfo::EState::DropBuild:
            Y_ENSURE(buildInfo.IsBuildVectorIndex());
            Y_ENSURE(buildInfo.KMeans.Level > 2 || buildInfo.KMeans.OverlapClusters > 1 && buildInfo.KMeans.Levels > 1);
            if (buildInfo.ApplyTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), DropBuildPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                buildInfo.ApplyTxId = {};
                buildInfo.ApplyTxStatus = NKikimrScheme::StatusSuccess;
                buildInfo.ApplyTxDone = false;

                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
                Self->PersistBuildIndexApplyTxStatus(db, buildInfo);
                Self->PersistBuildIndexApplyTxDone(db, buildInfo);

                ChangeState(BuildId, TIndexBuildInfo::EState::CreateBuild);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::CreateBuild:
            Y_ENSURE(buildInfo.IsBuildVectorIndex());
            if (buildInfo.ApplyTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CreateBuildPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                buildInfo.SnapshotTxId = {};
                buildInfo.SnapshotStep = {};

                buildInfo.ApplyTxId = {};
                buildInfo.ApplyTxStatus = NKikimrScheme::StatusSuccess;
                buildInfo.ApplyTxDone = false;

                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
                Self->PersistBuildIndexApplyTxStatus(db, buildInfo);
                Self->PersistBuildIndexApplyTxDone(db, buildInfo);

                ChangeState(BuildId, TIndexBuildInfo::EState::Filling);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::LockBuild:
            Y_ENSURE(buildInfo.IsBuildVectorIndex() && (buildInfo.KMeans.Level > 1 ||
                buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::Filter) ||
                buildInfo.SubState == TIndexBuildInfo::ESubState::UniqIndexValidation ||
                buildInfo.SubState == TIndexBuildInfo::ESubState::FulltextIndexDictionary);
            if (buildInfo.ApplyTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                TString tableName;
                if (buildInfo.SubState == TIndexBuildInfo::ESubState::FulltextIndexDictionary) {
                    tableName = NTableIndex::ImplTable;
                } else if (buildInfo.SubState == TIndexBuildInfo::ESubState::UniqIndexValidation) {
                    tableName = NTableIndex::ImplTable;
                } else {
                    tableName = buildInfo.KMeans.ReadFrom();
                }
                Send(Self->SelfId(), LockPropose(Self, buildInfo, buildInfo.ApplyTxId, GetBuildPath(Self, buildInfo, tableName)), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                buildInfo.ApplyTxId = InvalidTxId;
                buildInfo.ApplyTxStatus = NKikimrScheme::StatusSuccess;
                buildInfo.ApplyTxDone = false;

                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
                Self->PersistBuildIndexApplyTxStatus(db, buildInfo);
                Self->PersistBuildIndexApplyTxDone(db, buildInfo);

                ChangeState(BuildId, TIndexBuildInfo::EState::Filling);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::AlterSequence:
            Y_ENSURE(buildInfo.IsBuildPrefixedVectorIndex());
            Y_ENSURE(buildInfo.KMeans.Level == 2);
            if (buildInfo.ApplyTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), AlterSequencePropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                buildInfo.ApplyTxId = {};
                buildInfo.ApplyTxStatus = NKikimrScheme::StatusSuccess;
                buildInfo.ApplyTxDone = false;

                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
                Self->PersistBuildIndexApplyTxStatus(db, buildInfo);
                Self->PersistBuildIndexApplyTxDone(db, buildInfo);

                ChangeState(BuildId, TIndexBuildInfo::EState::Filling);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Applying:
            if (buildInfo.ApplyTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), ApplyPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Unlocking:
            if (buildInfo.UnlockTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Done);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Done:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;
        case TIndexBuildInfo::EState::Cancellation_DroppingColumns:
            Y_ENSURE(buildInfo.IsBuildColumns());
            if (buildInfo.DropColumnsTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.DropColumnsTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), DropColumnsPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.DropColumnsTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.DropColumnsTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Cancellation_Applying);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Cancellation_Applying:
            if (buildInfo.ApplyTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CancelPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Cancellation_Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
            if (buildInfo.UnlockTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Cancelled);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Cancelled:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;
        case TIndexBuildInfo::EState::Rejection_DroppingColumns:
            Y_ENSURE(buildInfo.IsBuildColumns());
            if (buildInfo.DropColumnsTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.DropColumnsTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), DropColumnsPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.DropColumnsTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.DropColumnsTxId)));
            } else {
                auto notApplied = !buildInfo.ApplyTxDone && buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess;
                if (buildInfo.InitiateTxDone && notApplied) {
                    ChangeState(BuildId, TIndexBuildInfo::EState::Rejection_Applying);
                } else {
                    ChangeState(BuildId, TIndexBuildInfo::EState::Rejection_Unlocking);
                }
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Rejection_Applying:
            if (buildInfo.ApplyTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CancelPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Rejection_Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Rejection_Unlocking:
            if (buildInfo.UnlockTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Rejected);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Rejected:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;
        }

        return true;
    }

    void OnUnhandledException(TTransactionContext& txc, const TActorContext& ctx, TIndexBuildInfo* buildInfo, const std::exception& exc) override {
        if (!buildInfo) {
            LOG_N("TTxBuildProgress: OnUnhandledException: BuildIndexId not found "
                << (BuildId == InvalidIndexBuildId ? TString("") : TStringBuilder() << ", id# " << BuildId));
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistBuildIndexAddIssue(db, *buildInfo,
            TStringBuilder() << "Unhandled exception " << exc.what());

        if (buildInfo->State != TIndexBuildInfo::EState::Filling) {
            // no idea how to gracefully stop index build otherwise
            // leave everything as is
            LOG_E("TTxBuildProgress: OnUnhandledException: not a Filling state, id# " << buildInfo->Id
                << ", TIndexBuildInfo: " << *buildInfo);
            return;
        }

        if (buildInfo->IsBuildIndex()) {
            buildInfo->State = TIndexBuildInfo::EState::Rejection_Applying;
        } else if (buildInfo->IsBuildColumns()) {
            buildInfo->State = TIndexBuildInfo::EState::Rejection_DroppingColumns;
        } else {
            Y_ENSURE(false, "TTxBuildProgress: OnUnhandledException: unknown build type");
        }

        Self->PersistBuildIndexState(db, *buildInfo);
        Self->Execute(Self->CreateTxProgress(buildInfo->Id), ctx);
    }

    static TSerializedTableRange InfiniteRange(ui32 columns) {
        TVector<TCell> vec(columns, TCell());
        TArrayRef<TCell> cells(vec);
        return TSerializedTableRange(TSerializedCellVec::Serialize(cells), "", true, false);
    }

    static TSerializedTableRange ParentRange(NTableIndex::NKMeans::TClusterId parent) {
        if (parent == 0) {
            return {};  // empty
        }
        auto from = TCell::Make(parent - 1);
        auto to = TCell::Make(parent);
        return TSerializedTableRange{{&from, 1}, false, {&to, 1}, true};
    }

    TPath GetShardsPath(TIndexBuildInfo& buildInfo) {
        switch (buildInfo.BuildKind) {
            case TIndexBuildInfo::EBuildKind::BuildSecondaryIndex:
            case TIndexBuildInfo::EBuildKind::BuildColumns:
            case TIndexBuildInfo::EBuildKind::BuildFulltext:
                if (buildInfo.SubState == TIndexBuildInfo::ESubState::FulltextIndexDictionary) {
                    return GetBuildPath(Self, buildInfo, NTableIndex::ImplTable);
                }
                return TPath::Init(buildInfo.TablePathId, Self);
            case TIndexBuildInfo::EBuildKind::BuildSecondaryUniqueIndex:
                return buildInfo.IsValidatingUniqueIndex()
                    ? GetBuildPath(Self, buildInfo, NTableIndex::ImplTable)
                    : TPath::Init(buildInfo.TablePathId, Self);
            case TIndexBuildInfo::EBuildKind::BuildVectorIndex:
            case TIndexBuildInfo::EBuildKind::BuildPrefixedVectorIndex:
                if (buildInfo.KMeans.Level == 1 &&
                    buildInfo.KMeans.State != TIndexBuildInfo::TKMeans::Filter &&
                    buildInfo.KMeans.State != TIndexBuildInfo::TKMeans::FilterBorders) {
                    return TPath::Init(buildInfo.TablePathId, Self);
                } else {
                    return GetBuildPath(Self, buildInfo, buildInfo.KMeans.ReadFrom());
                }
            default:
                Y_ENSURE(false, buildInfo.InvalidBuildKind());
        }
    }

    bool InitiateShards(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) {
        LOG_D("InitiateShards " << buildInfo.DebugString());

        Y_ENSURE(buildInfo.Shards.empty());
        Y_ENSURE(buildInfo.ToUploadShards.empty());
        Y_ENSURE(buildInfo.InProgressShards.empty());
        Y_ENSURE(buildInfo.DoneShards.empty());

        TPath path = GetShardsPath(buildInfo);
        if (!path.IsLocked()) { // lock is needed to prevent table shards from being split
            Y_ENSURE(buildInfo.IsBuildVectorIndex() && (buildInfo.KMeans.Level > 1 ||
                buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::Filter));
            ChangeState(buildInfo.Id, TIndexBuildInfo::EState::LockBuild);
            Progress(buildInfo.Id);
            return false;
        }
        Y_ENSURE(path.LockedBy() == buildInfo.LockTxId);
        LOG_D("InitiateShards table: " << path.PathString());

        TTableInfo::TPtr table = Self->Tables.at(path->PathId);

        auto tableColumns = NTableIndex::ExtractInfo(table); // skip dropped columns
        // In case of unique index validation the real range will arrive after index validation for each shard:
        // it will describe the first and the last index keys for further validation.
        TSerializedTableRange shardRange = buildInfo.IsValidatingUniqueIndex() ? TSerializedTableRange{} : InfiniteRange(tableColumns.Keys.size());
        static constexpr std::string_view LogPrefix = "";

        buildInfo.Cluster2Shards.clear();
        for (const auto& x: table->GetPartitions()) {
            Y_ENSURE(Self->ShardInfos.contains(x.ShardIdx));
            TSerializedCellVec bound{x.EndOfRange};
            if (!buildInfo.IsValidatingUniqueIndex()) {
                shardRange.To = bound;
            }
            if (buildInfo.BuildKind == TIndexBuildInfo::EBuildKind::BuildVectorIndex &&
                buildInfo.KMeans.State != TIndexBuildInfo::TKMeans::Filter) {
                LOG_D("InitiateShard " << x.ShardIdx << " range " << buildInfo.KMeans.RangeToDebugStr(shardRange));
                buildInfo.AddParent(shardRange, x.ShardIdx);
            } else {
                LOG_D("InitiateShard " << x.ShardIdx);
            }
            auto [it, emplaced] = buildInfo.Shards.emplace(x.ShardIdx, TIndexBuildShardStatus{std::move(shardRange), ""});
            Y_ENSURE(emplaced);
            if (!buildInfo.IsValidatingUniqueIndex()) {
                shardRange.From = std::move(bound);
            }

            Self->PersistBuildIndexShardStatusInitiate(db, BuildId, x.ShardIdx, it->second);
        }

        return true;
    }

    void DoComplete(const TActorContext& ctx) override {
        for (auto& [shardId, ev]: ToTabletSend) {
            Self->IndexBuildPipes.Send(BuildId, shardId, std::move(ev), ctx);
        }
        ToTabletSend.clear();
    }
};

ITransaction* TSchemeShard::CreateTxProgress(TIndexBuildId id) {
    return new TIndexBuilder::TTxProgress(this, id);
}

struct TSchemeShard::TIndexBuilder::TTxBilling: public TSchemeShard::TIndexBuilder::TTxBase {
private:
    TInstant ScheduledAt;

public:
    explicit TTxBilling(TSelf* self, TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev)
        : TTxBase(self, TIndexBuildId(ev->Get()->BuildId), TXTYPE_PROGRESS_INDEX_BUILD)
        , ScheduledAt(ev->Get()->SendAt)
    {}

    bool DoExecute(TTransactionContext& , const TActorContext& ctx) override {
        LOG_I("TTxReply : TTxBilling, id# " << BuildId);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->get();

        if (!GotScheduledBilling(buildInfo)) {
            return true;
        }

        Bill(buildInfo, ScheduledAt, ctx.Now());

        AskToScheduleBilling(buildInfo);

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

    void OnUnhandledException(TTransactionContext&, const TActorContext&, TIndexBuildInfo*, const std::exception&) override {
        // seems safe to ignore
    }
};

struct TSchemeShard::TIndexBuilder::TTxReply: public TSchemeShard::TIndexBuilder::TTxBase {
public:
    explicit TTxReply(TSelf* self, TIndexBuildId buildId)
        : TTxBase(self, buildId, TXTYPE_PROGRESS_INDEX_BUILD)
    {}

    void OnUnhandledException(TTransactionContext& txc, const TActorContext& ctx, TIndexBuildInfo* buildInfo, const std::exception& exc) override {
        if (!buildInfo) {
            LOG_E("TTxReply : OnUnhandledException BuildIndexId not found"
                << (BuildId == InvalidIndexBuildId ? TString("") : TStringBuilder() << ", id# " << BuildId));
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistBuildIndexAddIssue(db, *buildInfo,
            TStringBuilder() << "Unhandled exception " << exc.what());

        if (buildInfo->State != TIndexBuildInfo::EState::Filling) {
            // most replies are used at Filling stage
            // no idea how to gracefully stop index build otherwise
            // leave everything as is
            LOG_E("TTxReply : OnUnhandledException not a Filling state, id# " << buildInfo->Id
                << ", TIndexBuildInfo: " << *buildInfo);
            return;
        }

        if (buildInfo->IsBuildIndex()) {
            buildInfo->State = TIndexBuildInfo::EState::Rejection_Applying;
        } else if (buildInfo->IsBuildColumns()) {
            buildInfo->State = TIndexBuildInfo::EState::Rejection_DroppingColumns;
        } else {
            Y_ENSURE(false, "TTxReply : OnUnhandledException: unknown build type");
        }

        Self->PersistBuildIndexState(db, *buildInfo);
        Self->Execute(Self->CreateTxProgress(buildInfo->Id), ctx);
    }

    void DoComplete(const TActorContext&) override {
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyRetry: public TSchemeShard::TIndexBuilder::TTxReply {
private:
    TTabletId ShardId;

public:
    explicit TTxReplyRetry(TSelf* self, TIndexBuildId buildId, TTabletId shardId)
        : TTxReply(self, buildId)
        , ShardId(shardId)
    {}

    bool DoExecute([[maybe_unused]] TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& shardIdx = Self->GetShardIdx(ShardId);

        LOG_N("TTxReply : PipeRetry, id# " << BuildId
            << ", shardId# " << ShardId
            << ", shardIdx# " << shardIdx);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->get();
        LOG_D("TTxReply : PipeRetry"
            << ", TIndexBuildInfo: " << buildInfo
            << ", shardId# " << ShardId
            << ", shardIdx# " << shardIdx);

        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        if (buildInfo.State != TIndexBuildInfo::EState::Filling) {
            LOG_I("TTxReply : PipeRetry superfluous event, id# " << BuildId);
            return true;
        }

        // reschedule shard
        if (buildInfo.InProgressShards.erase(shardIdx)) {
            buildInfo.ToUploadShards.emplace_front(shardIdx);

            Self->IndexBuildPipes.Close(BuildId, ShardId, ctx);

            // generate new message with actual LastKeyAck to continue scan
            Progress(BuildId);
        }

        return true;
    }
};

template<typename TEvResponse>
struct TTxShardReply: public TSchemeShard::TIndexBuilder::TTxReply {
protected:
    TEvResponse::TPtr Response;

public:
    explicit TTxShardReply(TSelf* self, TIndexBuildId buildId, TEvResponse::TPtr& response)
        : TTxReply(self, buildId)
        , Response(response)
    {
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = Response->Get()->Record;
        TTabletId shardId = TTabletId(record.GetTabletId());
        TShardIdx shardIdx = Self->GetShardIdx(shardId);

        LOG_N("TTxReply : " << TypeName<TEvResponse>() << ", id# " << BuildId
            << ", shardId# " << shardId
            << ", shardIdx# " << shardIdx);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        if (!buildInfoPtr) {
            return true;
        }

        auto& buildInfo = *buildInfoPtr->get();
        LOG_D("TTxReply : " << TypeName<TEvResponse>()
            << ", TIndexBuildInfo: " << buildInfo
            << ", record: " << ResponseShortDebugString()
            << ", shardId# " << shardId
            << ", shardIdx# " << shardIdx);

        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        if (buildInfo.State != TIndexBuildInfo::EState::Filling) {
            LOG_N("TTxReply : " << TypeName<TEvResponse>() << " superfluous state event, id# " << BuildId
                << ", TIndexBuildInfo: " << buildInfo);
            return true;
        }

        if (!buildInfo.InProgressShards.contains(shardIdx)) {
            LOG_N("TTxReply : " << TypeName<TEvResponse>() << " superfluous shard event, id# " << BuildId
                << ", TIndexBuildInfo: " << buildInfo);
            return true;
        }

        TIndexBuildShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);
        auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
        auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

        if (actualSeqNo != recordSeqNo) {
            LOG_D("TTxReply : " << TypeName<TEvResponse>() << " ignore progress message by seqNo"
                << ", TIndexBuildInfo: " << buildInfo
                << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                << ", record: " << record.ShortDebugString());
            Y_ENSURE(actualSeqNo > recordSeqNo);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto stats = GetMeteringStats();
        shardStatus.Processed += stats;
        buildInfo.Processed += stats;

        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetIssues(), issues);
        shardStatus.DebugMessage = issues.ToString();
        shardStatus.Status = record.GetStatus();

        switch (shardStatus.Status) {
        case NKikimrIndexBuilder::EBuildStatus::INVALID:
            Y_ENSURE(false, "Unreachable");
        case NKikimrIndexBuilder::EBuildStatus::ACCEPTED: // TODO: do we need ACCEPTED?
        case NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS: {
            HandleProgress(shardStatus, buildInfo);
            Self->PersistBuildIndexShardStatus(db, BuildId, shardIdx, shardStatus);
            // no progress
            // no pipe close
            return true;
        }
        case NKikimrIndexBuilder::EBuildStatus::DONE: {
            bool erased = buildInfo.InProgressShards.erase(shardIdx);
            Y_ENSURE(erased);
            buildInfo.DoneShards.emplace_back(shardIdx);
            HandleDone(db, buildInfo);
            Self->PersistBuildIndexShardStatus(db, BuildId, shardIdx, shardStatus);
            Self->IndexBuildPipes.Close(BuildId, shardId, ctx);
            Progress(BuildId);
            return true;
        }
        case NKikimrIndexBuilder::EBuildStatus::ABORTED: {
            // datashard gracefully rebooted, reschedule shard
            bool erased = buildInfo.InProgressShards.erase(shardIdx);
            Y_ENSURE(erased);
            buildInfo.ToUploadShards.emplace_front(shardIdx);
            Self->PersistBuildIndexShardStatus(db, BuildId, shardIdx, shardStatus);
            Self->IndexBuildPipes.Close(BuildId, shardId, ctx);
            Progress(BuildId);
            return true;
        }
        case NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
        case NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST: {
            Self->PersistBuildIndexAddIssue(db, buildInfo, TStringBuilder()
                << "One of the shards report " << shardStatus.Status << " " << shardStatus.DebugMessage
                << " at Filling stage, process has to be canceled"
                << ", shardId: " << shardId
                << ", shardIdx: " << shardIdx);
            Self->PersistBuildIndexShardStatus(db, BuildId, shardIdx, shardStatus);
            Self->IndexBuildPipes.Close(BuildId, shardId, ctx);

            if (buildInfo.IsBuildIndex()) {
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);
            } else if (buildInfo.IsBuildColumns()) {
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_DroppingColumns);
            } else {
                Y_ENSURE(false, "TTxShardReply : DoExecute: unknown build type");
            }

            Progress(BuildId);
            return true;
        }
        }
    }

    virtual void HandleProgress(TIndexBuildShardStatus& shardStatus, TIndexBuildInfo& buildInfo) {
        Y_ENSURE(false, TStringBuilder() << "HandleProgress is unreachable for " << TypeName<TEvResponse>()
            << ", TIndexBuildInfo: " << buildInfo
            << ", shardStatus: " << shardStatus.ToString());
    }

    virtual void HandleDone(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) {
        // no action is needed by default
        Y_UNUSED(db, buildInfo);
    }

    virtual TMeteringStats GetMeteringStats() const {
        auto& record = Response->Get()->Record;
        if constexpr (requires { record.MutableMeteringStats(); }) {
            return record.GetMeteringStats();
        }
        Y_ENSURE(false, "Should be overwritten");
    }

    virtual TString ResponseShortDebugString() const {
        auto& record = Response->Get()->Record;
        return record.ShortDebugString();
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplySampleK: public TTxShardReply<TEvDataShard::TEvSampleKResponse> {
    explicit TTxReplySampleK(TSelf* self, TEvDataShard::TEvSampleKResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }

    void HandleDone(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) override {
        auto& record = Response->Get()->Record;

        if (record.ProbabilitiesSize()) {
            Y_ENSURE(record.RowsSize());
            auto& probabilities = record.GetProbabilities();
            auto& rows = *record.MutableRows();
            Y_ENSURE(probabilities.size() == rows.size());
            auto& sample = buildInfo.Sample.Rows;
            auto from = sample.size();
            for (int i = 0; i != probabilities.size(); ++i) {
                if (probabilities[i] >= buildInfo.Sample.MaxProbability) {
                    break;
                }
                sample.emplace_back(probabilities[i], std::move(rows[i]));
            }
            if (buildInfo.Sample.MakeWeakTop(buildInfo.KMeans.K)) {
                from = 0;
            }
            for (; from < sample.size(); ++from) {
                db.Table<Schema::KMeansTreeSample>().Key(buildInfo.Id, from).Update(
                    NIceDb::TUpdate<Schema::KMeansTreeSample::Probability>(sample[from].P),
                    NIceDb::TUpdate<Schema::KMeansTreeSample::Data>(sample[from].Row)
                );
            }
            for (; from < 2*buildInfo.KMeans.K; ++from) {
                db.Table<Schema::KMeansTreeSample>().Key(buildInfo.Id, from).Delete();
            }
        }
    }

    TString ResponseShortDebugString() const override {
        auto& record = Response->Get()->Record;
        return ToShortDebugString(record);
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyRecomputeKMeans: public TTxShardReply<TEvDataShard::TEvRecomputeKMeansResponse> {
    explicit TTxReplyRecomputeKMeans(TSelf* self, TEvDataShard::TEvRecomputeKMeansResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }

    void HandleDone(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) override {
        auto& record = Response->Get()->Record;
        Y_ENSURE(record.ClustersSize() == buildInfo.Sample.Rows.size());
        Y_ENSURE(record.ClusterSizesSize() == buildInfo.Sample.Rows.size());
        auto& clusters = record.GetClusters();
        auto& sizes = record.GetClusterSizes();
        for (ui32 i = 0; i < buildInfo.Sample.Rows.size(); i++) {
            if (sizes[i] > 0) {
                buildInfo.Clusters->AggregateToCluster(i, clusters[i], sizes[i]);
            }
        }
        buildInfo.Clusters->RecomputeClusters();
        Self->PersistBuildIndexClustersUpdate(db, buildInfo);
    }

    TString ResponseShortDebugString() const override {
        auto& record = Response->Get()->Record;
        return ToShortDebugString(record);
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyFilterKMeans: public TTxShardReply<TEvDataShard::TEvFilterKMeansResponse> {
    explicit TTxReplyFilterKMeans(TSelf* self, TEvDataShard::TEvFilterKMeansResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }

    void HandleDone(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) override {
        auto& record = Response->Get()->Record;
        // Save FirstKeyRows and LastKeyRows to KMeansTreeSample -- it should be empty because
        // all Sample/Reshuffle passes are already completed for the current level during Filter
        Y_ENSURE(buildInfo.Sample.Rows.empty());
        ui32 count = buildInfo.KMeans.FilterBorderRows.size();
        for (const auto& row: record.GetFirstKeyRows()) {
            buildInfo.KMeans.FilterBorderRows.push_back(row);
            db.Table<Schema::KMeansTreeSample>().Key(buildInfo.Id, count).Update(
                NIceDb::TUpdate<Schema::KMeansTreeSample::Probability>(0),
                NIceDb::TUpdate<Schema::KMeansTreeSample::Data>(row)
            );
            count++;
        }
        for (const auto& row: record.GetLastKeyRows()) {
            buildInfo.KMeans.FilterBorderRows.push_back(row);
            db.Table<Schema::KMeansTreeSample>().Key(buildInfo.Id, count).Update(
                NIceDb::TUpdate<Schema::KMeansTreeSample::Probability>(0),
                NIceDb::TUpdate<Schema::KMeansTreeSample::Data>(row)
            );
            count++;
        }
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyLocalKMeans: public TTxShardReply<TEvDataShard::TEvLocalKMeansResponse> {
    explicit TTxReplyLocalKMeans(TSelf* self, TEvDataShard::TEvLocalKMeansResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }

    void HandleDone(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) {
        if (Response->Get()->Record.GetIsEmpty() &&
            buildInfo.KMeans.Parent == 0)
        {
            // We only handle the root level through MultiLocal if the table has exactly 1 shard.
            // If that shard is empty then it means the whole index is empty.
            buildInfo.KMeans.IsEmpty = true;
            Self->PersistBuildIndexKMeansState(db, buildInfo);
        }
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyReshuffleKMeans: public TTxShardReply<TEvDataShard::TEvReshuffleKMeansResponse> {
    explicit TTxReplyReshuffleKMeans(TSelf* self, TEvDataShard::TEvReshuffleKMeansResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyPrefixKMeans: public TTxShardReply<TEvDataShard::TEvPrefixKMeansResponse> {
    explicit TTxReplyPrefixKMeans(TSelf* self, TEvDataShard::TEvPrefixKMeansResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyUploadSample: public TSchemeShard::TIndexBuilder::TTxReply {
private:
    TEvIndexBuilder::TEvUploadSampleKResponse::TPtr UploadSample;

public:
    explicit TTxReplyUploadSample(TSelf* self, TEvIndexBuilder::TEvUploadSampleKResponse::TPtr& uploadSample)
        : TTxReply(self, TIndexBuildId(uploadSample->Get()->Record.GetId()))
        , UploadSample(uploadSample)
    {
    }

    bool DoExecute([[maybe_unused]] TTransactionContext& txc, [[maybe_unused]] const TActorContext& ctx) override {
        const auto& record = UploadSample->Get()->Record;

        LOG_N("TTxReply : TEvUploadSampleKResponse, id# " << BuildId);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        if (!buildInfoPtr) {
            return true;
        }

        auto& buildInfo = *buildInfoPtr->get();
        LOG_D("TTxReply : TEvUploadSampleKResponse"
            << ", TIndexBuildInfo: " << buildInfo
            << ", record: " << record.ShortDebugString());
        Y_ENSURE(buildInfo.IsBuildVectorIndex() || buildInfo.IsBuildFulltextIndex());

        if (buildInfo.State != TIndexBuildInfo::EState::Filling) {
            LOG_I("TTxReply : TEvUploadSampleKResponse superfluous event, id# " << BuildId);
            return true;
        }
        Y_ENSURE(!buildInfo.IsBuildVectorIndex() || buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Upload);

        NIceDb::TNiceDb db(txc.DB);

        buildInfo.Processed += record.GetMeteringStats();
        // As long as we don't try to upload sample in parallel with requests to shards,
        // it's okay to persist Processed not incrementally
        Self->PersistBuildIndexProcessed(db, buildInfo);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetIssues(), issues);

        auto status = record.GetUploadStatus();
        if (status == Ydb::StatusIds::SUCCESS) {
            buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Done;
            Progress(BuildId);
        } else {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            Self->PersistBuildIndexAddIssue(db, buildInfo, issues.ToString());

            if (buildInfo.IsBuildIndex()) {
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);
            } else if (buildInfo.IsBuildColumns()) {
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_DroppingColumns);
            } else {
                Y_ENSURE(false, "TTxReplyUploadSample : DoExecute: unknown build type");
            }

            Progress(BuildId);
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyValidateUniqueIndex: public TTxShardReply<TEvDataShard::TEvValidateUniqueIndexResponse> {
    TTxReplyValidateUniqueIndex(TSelf* self, TEvDataShard::TEvValidateUniqueIndexResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }

    void HandleDone(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) override {
        const auto& record = Response->Get()->Record;
        TTabletId shardId = TTabletId(record.GetTabletId());
        TShardIdx shardIdx = Self->GetShardIdx(shardId);
        TIndexBuildShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

        if (const TString& key = record.GetFirstIndexKey()) {
            shardStatus.Range.From = TSerializedCellVec(key);
        }
        if (const TString& key = record.GetLastIndexKey()) {
            shardStatus.Range.To = TSerializedCellVec(key);
        }

        Self->PersistBuildIndexShardRange(db, BuildId, shardIdx, shardStatus);
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyFulltextIndex: public TTxShardReply<TEvDataShard::TEvBuildFulltextIndexResponse> {
    TTxReplyFulltextIndex(TSelf* self, TEvDataShard::TEvBuildFulltextIndexResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }

    void HandleDone(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) override {
        const auto& record = Response->Get()->Record;
        TTabletId shardId = TTabletId(record.GetTabletId());
        TShardIdx shardIdx = Self->GetShardIdx(shardId);
        TIndexBuildShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

        shardStatus.DocCount = record.GetDocCount();
        shardStatus.TotalDocLength = record.GetTotalDocLength();

        Self->PersistBuildIndexShardStatusFulltext(db, BuildId, shardIdx, shardStatus);
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyFulltextDict: public TTxShardReply<TEvDataShard::TEvBuildFulltextDictResponse> {
    TTxReplyFulltextDict(TSelf* self, TEvDataShard::TEvBuildFulltextDictResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }

    void HandleDone(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) override {
        const auto& record = Response->Get()->Record;

        if (record.GetFirstTokenRows() || record.GetLastTokenRows()) {
            TTabletId shardId = TTabletId(record.GetTabletId());
            TShardIdx shardIdx = Self->GetShardIdx(shardId);
            TIndexBuildShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

            shardStatus.FirstToken = record.GetFirstToken();
            shardStatus.FirstTokenRows = record.GetFirstTokenRows();
            shardStatus.LastToken = record.GetLastToken();
            shardStatus.LastTokenRows = record.GetLastTokenRows();

            Self->PersistBuildIndexShardStatusFulltext(db, BuildId, shardIdx, shardStatus);
        }
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyProgress: public TTxShardReply<TEvDataShard::TEvBuildIndexProgressResponse> {
    explicit TTxReplyProgress(TSelf* self, TEvDataShard::TEvBuildIndexProgressResponse::TPtr& response)
        : TTxShardReply(self, TIndexBuildId(response->Get()->Record.GetId()), response)
    {
    }

    void HandleProgress(TIndexBuildShardStatus& shardStatus, TIndexBuildInfo& buildInfo) override {
        auto& record = Response->Get()->Record;

        if (record.HasLastKeyAck()) {
            if (shardStatus.LastKeyAck) {
                //check that all LastKeyAcks are monotonously increase
                const auto& tableInfo = *Self->Tables.at(buildInfo.TablePathId);
                std::vector<NScheme::TTypeInfo> keyTypes;
                keyTypes.reserve(tableInfo.KeyColumnIds.size());
                for (ui32 keyPos: tableInfo.KeyColumnIds) {
                    keyTypes.emplace_back(tableInfo.Columns.at(keyPos).PType);
                }

                TSerializedCellVec next{shardStatus.LastKeyAck};
                TSerializedCellVec prev{record.GetLastKeyAck()};

                int cmp = CompareBorders<true, true>(next.GetCells(),
                                                        prev.GetCells(),
                                                        true,
                                                        true,
                                                        keyTypes);
                Y_ENSURE(cmp < 0,
                            "check that all LastKeyAcks are monotonously increase"
                                << ", next: " << DebugPrintPoint(keyTypes, next.GetCells(), *AppData()->TypeRegistry)
                                << ", prev: " << DebugPrintPoint(keyTypes, prev.GetCells(), *AppData()->TypeRegistry));
            }

            shardStatus.LastKeyAck = record.GetLastKeyAck();
        }
    }

    TMeteringStats GetMeteringStats() const override {
        auto& record = Response->Get()->Record;
        // secondary index reads and writes almost the same amount of data
        // do not count them separately for simplicity
        TMeteringStats result;
        result.SetUploadRows(record.GetRowsDelta());
        result.SetUploadBytes(record.GetBytesDelta());
        result.SetReadRows(record.GetRowsDelta());
        result.SetReadBytes(record.GetBytesDelta());
        return result;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyCompleted: public TSchemeShard::TIndexBuilder::TTxReply {
private:
    TTxId CompletedTxId;
public:
    explicit TTxReplyCompleted(TSelf* self, TTxId completedTxId)
        : TTxReply(self, InvalidIndexBuildId)
        , CompletedTxId(completedTxId)
    {}

    bool DoExecute(TTransactionContext& txc, [[maybe_unused]] const TActorContext& ctx) override {
        const auto txId = CompletedTxId;

        const auto* buildIdPtr = Self->TxIdToIndexBuilds.FindPtr(txId);
        if (!buildIdPtr) {
            LOG_I("TTxReply : TEvNotifyTxCompletionResult superfluous message"
                << ", txId: " << txId
                << ", BuildIndexId not found");
            return true;
        }

        BuildId = *buildIdPtr;
        auto& buildInfo = *Self->IndexBuilds.at(BuildId);
        LOG_I("TTxReply : TEvNotifyTxCompletionResult, id# " << BuildId
            << ", txId# " << txId);
        LOG_D("TTxReply : TEvNotifyTxCompletionResult"
            << ", TIndexBuildInfo: " << buildInfo
            << ", txId# " << txId);

        NIceDb::TNiceDb db(txc.DB);

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Locking:
        {
            Y_ENSURE(txId == buildInfo.LockTxId);

            buildInfo.LockTxDone = true;
            Self->PersistBuildIndexLockTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        {
            Y_ENSURE(txId == buildInfo.AlterMainTableTxId);

            buildInfo.AlterMainTableTxDone = true;
            Self->PersistBuildIndexAlterMainTableTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Initiating:
        {
            Y_ENSURE(txId == buildInfo.InitiateTxId);

            buildInfo.InitiateTxDone = true;
            Self->PersistBuildIndexInitiateTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Cancellation_DroppingColumns:
        case TIndexBuildInfo::EState::Rejection_DroppingColumns:
        {
            Y_ENSURE(txId == buildInfo.DropColumnsTxId);

            buildInfo.DropColumnsTxDone = true;
            Self->PersistBuildIndexDropColumnsTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::AlterSequence:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Rejection_Applying:
        {
            Y_ENSURE(txId == buildInfo.ApplyTxId, state);

            buildInfo.ApplyTxDone = true;
            Self->PersistBuildIndexApplyTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        {
            Y_ENSURE(txId == buildInfo.UnlockTxId, state);

            buildInfo.UnlockTxDone = true;
            Self->PersistBuildIndexUnlockTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Filling:
        case TIndexBuildInfo::EState::Done:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejected:
            Y_ENSURE(false, "Unreachable " << state);
        }

        Progress(BuildId);

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyModify: public TSchemeShard::TIndexBuilder::TTxReply {
private:
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult;
public:
    explicit TTxReplyModify(TSelf* self, TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult)
        : TTxReply(self, InvalidIndexBuildId)
        , ModifyResult(modifyResult)
    {}

    void ReplyOnCreation(const TIndexBuildInfo& buildInfo,
                         const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        auto responseEv = MakeHolder<TEvIndexBuilder::TEvCreateResponse>(ui64(buildInfo.Id));

        Fill(*responseEv->Record.MutableIndexBuild(), buildInfo);

        auto& response = responseEv->Record;
        response.SetStatus(status);

        if (buildInfo.GetIssue()) {
            AddIssue(response.MutableIssues(), buildInfo.GetIssue());
        }

        LOG_N("TIndexBuilder::TTxReply: ReplyOnCreation"
              << ", BuildIndexId: " << buildInfo.Id
              << ", status: " << Ydb::StatusIds::StatusCode_Name(status)
              << ", error: " << buildInfo.GetIssue()
              << ", replyTo: " << buildInfo.CreateSender.ToString()
              << ", message: " << responseEv->Record.ShortDebugString());

        Send(buildInfo.CreateSender, std::move(responseEv), 0, buildInfo.SenderCookie);
    }

    bool DoExecute(TTransactionContext& txc, [[maybe_unused]] const TActorContext& ctx) override {
        const auto& record = ModifyResult->Get()->Record;
        const auto txId = TTxId(record.GetTxId());

        const auto* buildIdPtr = Self->TxIdToIndexBuilds.FindPtr(txId);
        if (!buildIdPtr) {
            LOG_I("TTxReply : TEvModifySchemeTransactionResult superfluous message"
                << ", cookie: " << ModifyResult->Cookie
                << ", record: " << record.ShortDebugString()
                << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                << ", BuildIndexId not found");
            return true;
        }

        BuildId = *buildIdPtr;
        // We need this because we use buildInfo after EraseBuildInfo
        auto buildInfoPin = Self->IndexBuilds.at(BuildId);
        auto& buildInfo = *buildInfoPin;
        LOG_I("TTxReply : TEvModifySchemeTransactionResult, id# " << BuildId
            << ", cookie: " << ModifyResult->Cookie
            << ", record: " << record.ShortDebugString()
            << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus()));
        LOG_D("TTxReply : TEvModifySchemeTransactionResult"
            << ", TIndexBuildInfo: " << buildInfo
            << ", cookie: " << ModifyResult->Cookie
            << ", record: " << record.ShortDebugString()
            << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus()));

        const auto state = buildInfo.State;
        NIceDb::TNiceDb db(txc.DB);

        auto replyOnCreation = [&] {
            auto statusCode = TranslateStatusCode(record.GetStatus());

            if (statusCode != Ydb::StatusIds::SUCCESS) {
                Self->PersistBuildIndexAddIssue(db, buildInfo, TStringBuilder()
                    << "At " << state << " state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason());
                Self->PersistBuildIndexForget(db, buildInfo);
                EraseBuildInfo(buildInfo);
            }

            ReplyOnCreation(buildInfo, statusCode);
        };

        auto ifErrorMoveTo = [&] (TIndexBuildInfo::EState to) {
            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                Y_ENSURE(false, "NEED MORE TESTING");
                // no op
            } else {
                Self->PersistBuildIndexAddIssue(db, buildInfo, TStringBuilder()
                    << "At " << state << " state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason());
                ChangeState(buildInfo.Id, to);
            }
        };

        switch (state) {
        case TIndexBuildInfo::EState::Locking:
        {
            Y_ENSURE(txId == buildInfo.LockTxId);

            buildInfo.LockTxStatus = record.GetStatus();
            Self->PersistBuildIndexLockTxStatus(db, buildInfo);

            replyOnCreation();
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        {
            Y_ENSURE(txId == buildInfo.AlterMainTableTxId);

            buildInfo.AlterMainTableTxStatus = record.GetStatus();
            Self->PersistBuildIndexAlterMainTableTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Initiating:
        {
            Y_ENSURE(txId == buildInfo.InitiateTxId);

            buildInfo.InitiateTxStatus = record.GetStatus();
            Self->PersistBuildIndexInitiateTxStatus(db, buildInfo);

            if (buildInfo.IsBuildColumns()) {
                ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_DroppingColumns);
            } else {
                ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            }
            break;
        }
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::AlterSequence:
        {
            Y_ENSURE(txId == buildInfo.ApplyTxId);

            if (record.GetStatus() != NKikimrScheme::StatusAccepted &&
                record.GetStatus() != NKikimrScheme::StatusAlreadyExists) {
                // Otherwise we won't cancel the index build correctly
                buildInfo.ApplyTxId = {};
                buildInfo.ApplyTxStatus = NKikimrScheme::StatusSuccess;
                buildInfo.ApplyTxDone = false;
            } else {
                buildInfo.ApplyTxStatus = record.GetStatus();
            }
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Applying);
            break;
        }
        case TIndexBuildInfo::EState::Applying:
        {
            Y_ENSURE(txId == buildInfo.ApplyTxId);

            buildInfo.ApplyTxStatus = record.GetStatus();
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            if (buildInfo.IsBuildColumns()) {
                ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_DroppingColumns);
            } else {
                ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            }
            break;
        }
        case TIndexBuildInfo::EState::Rejection_Applying:
        {
            Y_ENSURE(txId == buildInfo.ApplyTxId);

            buildInfo.ApplyTxStatus = record.GetStatus();
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Unlocking:
        {
            Y_ENSURE(txId == buildInfo.UnlockTxId);

            buildInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            if (buildInfo.IsBuildColumns()) {
                ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_DroppingColumns);
            } else {
                ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            }
            break;
        }
        case TIndexBuildInfo::EState::Cancellation_DroppingColumns:
        {
            Y_ENSURE(txId == buildInfo.DropColumnsTxId);

            buildInfo.DropColumnsTxStatus = record.GetStatus();
            Self->PersistBuildIndexDropColumnsTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Cancellation_Applying);
            break;
        }
        case TIndexBuildInfo::EState::Rejection_DroppingColumns:
        {
            Y_ENSURE(txId == buildInfo.DropColumnsTxId);

            buildInfo.DropColumnsTxStatus = record.GetStatus();
            Self->PersistBuildIndexDropColumnsTxStatus(db, buildInfo);

            auto notApplied = !buildInfo.ApplyTxDone && buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess;
            if (buildInfo.InitiateTxDone && notApplied) {
                ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Applying);
            } else {
                ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            }
            break;
        }
        case TIndexBuildInfo::EState::Cancellation_Applying:
        {
            Y_ENSURE(txId == buildInfo.ApplyTxId);

            buildInfo.ApplyTxStatus = record.GetStatus();
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Cancellation_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        {
            Y_ENSURE(txId == buildInfo.UnlockTxId);

            buildInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Cancelled);
            break;
        }
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        {
            Y_ENSURE(txId == buildInfo.UnlockTxId);

            buildInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejected);
            break;
        }
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Filling:
        case TIndexBuildInfo::EState::Done:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejected:
            Y_ENSURE(false, "Unreachable " << state);
        }

        Progress(BuildId);

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyAllocate: public TSchemeShard::TIndexBuilder::TTxReply {
private:
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult;
public:
    explicit TTxReplyAllocate(TSelf* self, TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult)
        : TTxReply(self, TIndexBuildId(allocateResult->Cookie))
        , AllocateResult(allocateResult)
    {}

    bool DoExecute(TTransactionContext& txc,[[maybe_unused]] const TActorContext& ctx) override {
        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        if (!buildInfoPtr) {
            LOG_I("TTxReply : TEvAllocateResult superfluous message"
                << ", cookie: " << AllocateResult->Cookie
                << ", txId# " << txId
                << ", BuildIndexId not found");
            return true;
        }

        auto& buildInfo = *buildInfoPtr->get();
        LOG_I("TTxReply : TEvAllocateResult, id# " << BuildId
            << ", txId# " << txId);
        LOG_D("TTxReply : TEvAllocateResult"
            << ", TIndexBuildInfo: " << buildInfo
            << ", txId# " << txId);

        NIceDb::TNiceDb db(txc.DB);
        const auto state = buildInfo.State;

        switch (state) {
        case TIndexBuildInfo::EState::AlterMainTable:
            if (!buildInfo.AlterMainTableTxId) {
                buildInfo.AlterMainTableTxId = txId;
                Self->PersistBuildIndexAlterMainTableTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Locking:
            if (!buildInfo.LockTxId) {
                buildInfo.LockTxId = txId;
                Self->PersistBuildIndexLockTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Initiating:
            if (!buildInfo.InitiateTxId) {
                buildInfo.InitiateTxId = txId;
                Self->PersistBuildIndexInitiateTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::AlterSequence:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Rejection_Applying:
            if (!buildInfo.ApplyTxId) {
                buildInfo.ApplyTxId = txId;
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Cancellation_DroppingColumns:
        case TIndexBuildInfo::EState::Rejection_DroppingColumns:
            if (!buildInfo.DropColumnsTxId) {
                buildInfo.DropColumnsTxId = txId;
                Self->PersistBuildIndexDropColumnsTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
            if (!buildInfo.UnlockTxId) {
                buildInfo.UnlockTxId = txId;
                Self->PersistBuildIndexUnlockTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Filling:
        case TIndexBuildInfo::EState::Done:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejected:
            Y_ENSURE(false, "Unreachable " << state);
        }

        Progress(BuildId);

        return true;
    }
};

ITransaction* TSchemeShard::CreateTxReply(TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult) {
    return new TIndexBuilder::TTxReplyAllocate(this, allocateResult);
}

ITransaction* TSchemeShard::CreateTxReply(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult) {
    return new TIndexBuilder::TTxReplyModify(this, modifyResult);
}

ITransaction* TSchemeShard::CreateTxReply(TTxId completedTxId) {
    return new TIndexBuilder::TTxReplyCompleted(this, completedTxId);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvBuildIndexProgressResponse::TPtr& progress) {
    return new TIndexBuilder::TTxReplyProgress(this, progress);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvSampleKResponse::TPtr& sampleK) {
    return new TIndexBuilder::TTxReplySampleK(this, sampleK);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvReshuffleKMeansResponse::TPtr& reshuffle) {
    return new TIndexBuilder::TTxReplyReshuffleKMeans(this, reshuffle);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvRecomputeKMeansResponse::TPtr& recompute) {
    return new TIndexBuilder::TTxReplyRecomputeKMeans(this, recompute);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvFilterKMeansResponse::TPtr& filter) {
    return new TIndexBuilder::TTxReplyFilterKMeans(this, filter);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvLocalKMeansResponse::TPtr& local) {
    return new TIndexBuilder::TTxReplyLocalKMeans(this, local);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvPrefixKMeansResponse::TPtr& prefix) {
    return new TIndexBuilder::TTxReplyPrefixKMeans(this, prefix);
}

ITransaction* TSchemeShard::CreateTxReply(TEvIndexBuilder::TEvUploadSampleKResponse::TPtr& upload) {
    return new TIndexBuilder::TTxReplyUploadSample(this, upload);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvValidateUniqueIndexResponse::TPtr& response) {
    return new TIndexBuilder::TTxReplyValidateUniqueIndex(this, response);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvBuildFulltextIndexResponse::TPtr& response) {
    return new TIndexBuilder::TTxReplyFulltextIndex(this, response);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvBuildFulltextDictResponse::TPtr& response) {
    return new TIndexBuilder::TTxReplyFulltextDict(this, response);
}

ITransaction* TSchemeShard::CreatePipeRetry(TIndexBuildId indexBuildId, TTabletId tabletId) {
    return new TIndexBuilder::TTxReplyRetry(this, indexBuildId, tabletId);
}

ITransaction* TSchemeShard::CreateTxBilling(TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev) {
    return new TIndexBuilder::TTxBilling(this, ev);
}


} // NSchemeShard
} // NKikimr
