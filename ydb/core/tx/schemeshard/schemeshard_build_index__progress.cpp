#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"  // for NTableIndex::ExtractInfo
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/datashard/upload_stats.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/core/ydb_convert/table_description.h>


namespace NKikimr {
namespace NSchemeShard {

// return count, parts, step
static std::tuple<NTableIndex::TClusterId, NTableIndex::TClusterId, NTableIndex::TClusterId> ComputeKMeansBoundaries(const NSchemeShard::TTableInfo& tableInfo, const TIndexBuildInfo& buildInfo) {
    const auto& kmeans = buildInfo.KMeans;
    Y_ASSERT(kmeans.K != 0);
    const auto count = kmeans.ChildCount();
    NTableIndex::TClusterId step = 1;
    auto parts = count;
    auto shards = tableInfo.GetShard2PartitionIdx().size();
    if (!buildInfo.KMeans.NeedsAnotherLevel() || count <= 1 || shards <= 1) {
        return {1, 1, 1};
    }
    for (; 2 * shards <= parts; parts = (parts + 1) / 2) {
        step *= 2;
    }
    return {count, parts, step};
}

class TUploadSampleK: public TActorBootstrapped<TUploadSampleK> {
    using TThis = TUploadSampleK;
    using TBase = TActorBootstrapped<TThis>;

protected:
    TString LogPrefix;
    TString TargetTable;

    const NKikimrIndexBuilder::TIndexBuildScanSettings ScanSettings;

    TActorId ResponseActorId;
    ui64 BuildIndexId = 0;
    TIndexBuildInfo::TSample::TRows Init;

    std::shared_ptr<NTxProxy::TUploadTypes> Types;
    std::shared_ptr<NTxProxy::TUploadRows> Rows;

    TActorId Uploader;
    ui32 RetryCount = 0;
    ui32 RowsBytes = 0;
    NTableIndex::TClusterId Parent = 0;
    NTableIndex::TClusterId Child = 0;

    NDataShard::TUploadStatus UploadStatus;

public:
    TUploadSampleK(TString targetTable,
                   const NKikimrIndexBuilder::TIndexBuildScanSettings& scanSettings,
                   const TActorId& responseActorId,
                   ui64 buildIndexId,
                   TIndexBuildInfo::TSample::TRows init,
                   NTableIndex::TClusterId parent,
                   NTableIndex::TClusterId child)
        : TargetTable(std::move(targetTable))
        , ScanSettings(scanSettings)
        , ResponseActorId(responseActorId)
        , BuildIndexId(buildIndexId)
        , Init(std::move(init))
        , Parent(parent)
        , Child(child)
    {
        LogPrefix = TStringBuilder()
            << "TUploadSampleK: BuildIndexId: " << BuildIndexId
            << " ResponseActorId: " << ResponseActorId;
        Y_ASSERT(!Init.empty());
        Y_ASSERT(Parent < Child);
        Y_ASSERT(Child != 0);
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
        Rows = std::make_shared<NTxProxy::TUploadRows>();
        Rows->reserve(Init.size());
        std::array<TCell, 2> PrimaryKeys;
        PrimaryKeys[0] = TCell::Make(Parent);
        for (auto& [_, row] : Init) {
            RowsBytes += row.size();
            PrimaryKeys[1] = TCell::Make(Child++);
            // TODO(mbkkt) we can avoid serialization of PrimaryKeys every iter
            Rows->emplace_back(TSerializedCellVec{PrimaryKeys}, std::move(row));
        }
        Init = {}; // release memory
        RowsBytes += Rows->size() * TSerializedCellVec::SerializedSize(PrimaryKeys);

        Types = std::make_shared<NTxProxy::TUploadTypes>(3);
        Ydb::Type type;
        type.set_type_id(NTableIndex::ClusterIdType);
        (*Types)[0] = {NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn, type};
        (*Types)[1] = {NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, type};
        type.set_type_id(Ydb::Type::STRING);
        (*Types)[2] = {NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn, type};

        Become(&TThis::StateWork);

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

        if (Rows) {
            Upload(true);
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        LOG_T("Handle TEvUploadRowsResponse "
              << Debug()
              << " Uploader: " << Uploader.ToString()
              << " ev->Sender: " << ev->Sender.ToString());

        if (!Uploader) {
            return;
        }
        Y_VERIFY_S(Uploader == ev->Sender,
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

        response->Record.SetId(BuildIndexId);
        response->Record.SetUploadRows(Rows->size());
        response->Record.SetUploadBytes(RowsBytes);

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
            TargetTable,
            Types,
            Rows,
            NTxProxy::EUploadRowsMode::WriteToTableShadow, // TODO(mbkkt) is it fastest?
            true /*writeToPrivateTable*/);

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

    LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
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
        Y_ABORT("Unknown operation kind while building CreateIndexPropose");
    }

    LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
        "CreateIndexPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropBuildPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ASSERT(buildInfo.IsBuildVectorIndex());

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

    LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
        "DropBuildPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateBuildPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ASSERT(buildInfo.IsBuildVectorIndex());

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);
    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetInternal(true);

    auto path = TPath::Init(buildInfo.TablePathId, ss);
    const auto& tableInfo = ss->Tables.at(path->PathId);
    NTableIndex::TTableColumns implTableColumns;
    THashSet<TString> indexDataColumns;
    {
        buildInfo.SerializeToProto(ss, modifyScheme.MutableInitiateIndexBuild());
        const auto& indexDesc = modifyScheme.GetInitiateIndexBuild().GetIndex();
        const auto& baseTableColumns = NTableIndex::ExtractInfo(tableInfo);
        auto indexKeys = NTableIndex::ExtractInfo(indexDesc);
        if (buildInfo.IsBuildPrefixedVectorIndex() && buildInfo.KMeans.Level != 1) {
            Y_ASSERT(indexKeys.KeyColumns.size() >= 2);
            indexKeys.KeyColumns.erase(indexKeys.KeyColumns.begin(), indexKeys.KeyColumns.end() - 1);
        }
        implTableColumns = CalcTableImplDescription(buildInfo.IndexType, baseTableColumns, indexKeys);
        Y_ABORT_UNLESS(indexKeys.KeyColumns.size() >= 1);
        implTableColumns.Columns.emplace(indexKeys.KeyColumns.back());
        modifyScheme.ClearInitiateIndexBuild();
        indexDataColumns = THashSet<TString>(buildInfo.DataColumns.begin(), buildInfo.DataColumns.end());
        indexDataColumns.insert(indexKeys.KeyColumns.back());
    }

    using namespace NTableIndex::NTableVectorKmeansTreeIndex;
    modifyScheme.SetWorkingDir(path.Dive(buildInfo.IndexName).PathString());
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable);
    auto& op = *modifyScheme.MutableCreateTable();
    std::string_view suffix = buildInfo.KMeans.Level % 2 != 0 ? BuildSuffix0 : BuildSuffix1;
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
        const auto shards = tableInfo->GetShard2PartitionIdx().size();
        policy.SetMinPartitionsCount(shards);
        policy.SetMaxPartitionsCount(shards);

        LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
            "CreateBuildPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

        return propose;
    }
    op = NTableIndex::CalcVectorKmeansTreePostingImplTableDesc(tableInfo, tableInfo->PartitionConfig(), indexDataColumns, {}, suffix);
    const auto [count, parts, step] = ComputeKMeansBoundaries(*tableInfo, buildInfo);

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
    }
    policy.SetMinPartitionsCount(op.SplitBoundarySize() + 1);
    policy.SetMaxPartitionsCount(op.SplitBoundarySize() + 1);

    LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
        "CreateBuildPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTablePropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ABORT_UNLESS(buildInfo.IsBuildColumns(), "Unknown operation kind while building AlterMainTablePropose");

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.AlterMainTableTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
    modifyScheme.SetInternal(true);

    auto path = TPath::Init(buildInfo.TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    modifyScheme.MutableAlterTable()->SetName(path.LeafName());

    for(auto& colInfo : buildInfo.BuildColumns) {
        auto col = modifyScheme.MutableAlterTable()->AddColumns();
        NScheme::TTypeInfo typeInfo;
        TString typeMod;
        Ydb::StatusIds::StatusCode status;
        TString error;
        if (!ExtractColumnTypeInfo(typeInfo, typeMod, colInfo.DefaultFromLiteral.type(), status, error)) {
            // todo gvit fix that
            Y_ABORT("failed to extract column type info");
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

    LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
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

    LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
        "ApplyPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> UnlockPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.UnlockTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropLock);
    modifyScheme.SetInternal(true);
    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    TPath path = TPath::Init(buildInfo.TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());

    auto& lockConfig = *modifyScheme.MutableLockConfig();
    lockConfig.SetName(path.LeafName());

    LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
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

    LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::BUILD_INDEX, 
        "CancelPropose " << buildInfo.Id << " " << buildInfo.State << " " << propose->Record.ShortDebugString());

    return propose;
}

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgress: public TSchemeShard::TIndexBuilder::TTxBase  {
private:
    TIndexBuildId BuildId;

    TMap<TTabletId, THolder<IEventBase>> ToTabletSend;

    template <bool WithSnapshot = true, typename Record>
    TTabletId CommonFillRecord(Record& record, TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        TTabletId shardId = Self->ShardInfos.at(shardIdx).TabletID;
        record.SetTabletId(ui64(shardId));
        if constexpr (WithSnapshot) {
            if (buildInfo.SnapshotTxId) {
                Y_ASSERT(buildInfo.SnapshotStep);
                record.SetSnapshotTxId(ui64(buildInfo.SnapshotTxId));
                record.SetSnapshotStep(ui64(buildInfo.SnapshotStep));
            }
        }

        auto& shardStatus = buildInfo.Shards.at(shardIdx);
        if constexpr (requires { record.MutableKeyRange(); }) {
            if (shardStatus.LastKeyAck) {
                TSerializedTableRange range = TSerializedTableRange(shardStatus.LastKeyAck, "", false, false);
                range.Serialize(*record.MutableKeyRange());
            } else if (buildInfo.KMeans.Parent == 0) {
                shardStatus.Range.Serialize(*record.MutableKeyRange());
            }
        }

        record.SetSeqNoGeneration(Self->Generation());
        record.SetSeqNoRound(++shardStatus.SeqNoRound);
        return shardId;
    }

    void SendSampleKRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ASSERT(buildInfo.IsBuildVectorIndex());
        auto ev = MakeHolder<TEvDataShard::TEvSampleKRequest>();
        ev->Record.SetId(ui64(BuildId));

        if (buildInfo.KMeans.Level == 1) {
            buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());
        } else {
            auto path = GetBuildPath(Self, buildInfo, buildInfo.KMeans.ReadFrom());
            path->PathId.ToProto(ev->Record.MutablePathId());
        }

        ev->Record.SetK(buildInfo.KMeans.K);
        ev->Record.SetMaxProbability(buildInfo.Sample.MaxProbability);
        if (buildInfo.KMeans.Parent != 0) {
            auto from = TCell::Make(buildInfo.KMeans.Parent - 1);
            auto to = TCell::Make(buildInfo.KMeans.Parent);
            TSerializedTableRange range{{&from, 1}, false, {&to, 1}, true};
            range.Serialize(*ev->Record.MutableKeyRange());
        }

        ev->Record.AddColumns(buildInfo.IndexColumns.back());

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);
        ev->Record.SetSeed(ui64(shardId));
        LOG_N("TTxBuildProgress: TEvSampleKRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendKMeansReshuffleRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ASSERT(buildInfo.IsBuildVectorIndex());
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

        auto& clusters = *ev->Record.MutableClusters();
        clusters.Reserve(buildInfo.Sample.Rows.size());
        for (const auto& [_, row] : buildInfo.Sample.Rows) {
            *clusters.Add() = TSerializedCellVec::ExtractCell(row, 0).AsBuf();
        }

        ev->Record.SetPostingName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());

        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns.back());
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);
        [[maybe_unused]] auto toDebugStr = [&](const NKikimrTxDataShard::TEvReshuffleKMeansRequest& record) {
            auto r = record;
            // clusters are not human readable and can be large like 100Kb+
            r.ClearClusters();
            return r.ShortDebugString();
        };
        LOG_N("TTxBuildProgress: TEvReshuffleKMeansRequest: " << toDebugStr(ev->Record));

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendKMeansLocalRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ASSERT(buildInfo.IsBuildVectorIndex());
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

        ev->Record.SetNeedsRounds(3); // TODO(mbkkt) should be configurable

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

        ev->Record.SetPostingName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());
        path.Rise().Dive(NTableIndex::NTableVectorKmeansTreeIndex::LevelTable);
        ev->Record.SetLevelName(path.PathString());

        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns.back());
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);
        ev->Record.SetSeed(ui64(shardId));
        LOG_N("TTxBuildProgress: TEvLocalKMeansRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendPrefixKMeansRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ASSERT(buildInfo.IsBuildPrefixedVectorIndex());
        Y_ASSERT(buildInfo.KMeans.Parent == buildInfo.KMeans.ParentEnd());
        Y_ASSERT(buildInfo.KMeans.Level == 2);

        auto ev = MakeHolder<TEvDataShard::TEvPrefixKMeansRequest>();
        ev->Record.SetId(ui64(BuildId));

        auto path = GetBuildPath(Self, buildInfo, buildInfo.KMeans.ReadFrom());
        path->PathId.ToProto(ev->Record.MutablePathId());
        path.Rise();
        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings().settings();
        ev->Record.SetK(buildInfo.KMeans.K);
        ev->Record.SetUpload(buildInfo.KMeans.GetUpload());

        ev->Record.SetNeedsRounds(3); // TODO(mbkkt) should be configurable

        const auto shardIndex = buildInfo.Shards.at(shardIdx).Index;
        // about 2 * TableSize see comment in PrefixIndexDone
        ev->Record.SetChild(buildInfo.KMeans.ChildBegin + (2 * buildInfo.KMeans.TableSize) * shardIndex);

        ev->Record.SetPostingName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());
        path.Rise().Dive(NTableIndex::NTableVectorKmeansTreeIndex::LevelTable);
        ev->Record.SetLevelName(path.PathString());
        path.Rise().Dive(NTableIndex::NTableVectorKmeansTreeIndex::PrefixTable);
        ev->Record.SetPrefixName(path.PathString());

        ev->Record.SetPrefixColumns(buildInfo.IndexColumns.size() - 1);
        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns.back());
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };

        auto shardId = CommonFillRecord<false>(ev->Record, shardIdx, buildInfo);
        ev->Record.SetSeed(ui64(shardId));
        LOG_N("TTxBuildProgress: TEvPrefixKMeansRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendBuildSecondaryIndexRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        auto ev = MakeHolder<TEvDataShard::TEvBuildIndexCreateRequest>();
        ev->Record.SetBuildIndexId(ui64(BuildId));

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
                auto implTableColumns = NTableIndex::ExtractInfo(implTableInfo);
                buildInfo.FillIndexColumns.clear();
                buildInfo.FillIndexColumns.reserve(implTableColumns.Keys.size());
                for (const auto& x: implTableColumns.Keys) {
                    buildInfo.FillIndexColumns.emplace_back(x);
                    implTableColumns.Columns.erase(x);
                }
                // TODO(mbkkt) why order doesn't matter?
                buildInfo.FillDataColumns.clear();
                buildInfo.FillDataColumns.reserve(implTableColumns.Columns.size());
                for (const auto& x: implTableColumns.Columns) {
                    buildInfo.FillDataColumns.emplace_back(x);
                }
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

        ev->Record.SetTargetName(buildInfo.TargetName);

        ev->Record.MutableScanSettings()->CopyFrom(buildInfo.ScanSettings);

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);

        LOG_N("TTxBuildProgress: TEvBuildIndexCreateRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace(shardId, std::move(ev));
    }

    void SendUploadSampleKRequest(TIndexBuildInfo& buildInfo) {
        buildInfo.Sample.MakeStrictTop(buildInfo.KMeans.K);
        auto path = GetBuildPath(Self, buildInfo, NTableIndex::NTableVectorKmeansTreeIndex::LevelTable);
        Y_ASSERT(buildInfo.Sample.Rows.size() <= buildInfo.KMeans.K);
        auto actor = new TUploadSampleK(path.PathString(),
            buildInfo.ScanSettings, Self->SelfId(), ui64(BuildId),
            buildInfo.Sample.Rows, buildInfo.KMeans.Parent, buildInfo.KMeans.Child);

        TActivationContext::AsActorContext().MakeFor(Self->SelfId()).Register(actor);
        buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Upload;
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

    void AddShard(TIndexBuildInfo& buildInfo, const TShardIdx& idx, const TIndexBuildInfo::TShardStatus& status) {
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
                Y_ABORT("Unreachable");
        }
    }

    void AddAllShards(TIndexBuildInfo& buildInfo) {
        ToTabletSend.clear();
        Self->IndexBuildPipes.CloseAll(BuildId, Self->ActorContext());

        for (const auto& [idx, status] : buildInfo.Shards) {
            AddShard(buildInfo, idx, status);
        }
    }

    bool FillSecondaryIndex(TIndexBuildInfo& buildInfo) {
        LOG_D("FillSecondaryIndex Start");
        
        if (buildInfo.DoneShards.empty() && buildInfo.ToUploadShards.empty() && buildInfo.InProgressShards.empty()) {
            AddAllShards(buildInfo);
        }
        auto done = SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendBuildSecondaryIndexRequest(shardIdx, buildInfo); }) &&
               buildInfo.DoneShards.size() == buildInfo.Shards.size();
        
        if (done) {
            LOG_D("FillSecondaryIndex Done");
        }

        return done;
    }

    bool FillPrefixKMeans(TIndexBuildInfo& buildInfo) {
        if (buildInfo.DoneShards.empty() && buildInfo.ToUploadShards.empty() && buildInfo.InProgressShards.empty()) {
            AddAllShards(buildInfo);
        }
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendPrefixKMeansRequest(shardIdx, buildInfo); }) &&
               buildInfo.DoneShards.size() == buildInfo.Shards.size();
    }

    bool FillLocalKMeans(TIndexBuildInfo& buildInfo) {
        if (buildInfo.DoneShards.empty() && buildInfo.ToUploadShards.empty() && buildInfo.InProgressShards.empty()) {
            AddAllShards(buildInfo);
        }
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendKMeansLocalRequest(shardIdx, buildInfo); }) &&
               buildInfo.DoneShards.size() == buildInfo.Shards.size();
    }

    bool InitSingleKMeans(TIndexBuildInfo& buildInfo) {
        if (!buildInfo.DoneShards.empty() || !buildInfo.InProgressShards.empty() || !buildInfo.ToUploadShards.empty()) {
            return false;
        }
        if (buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::MultiLocal) {
            InitMultiKMeans(buildInfo);
            return false;
        }
        std::array<NScheme::TTypeInfo, 1> typeInfos{ClusterIdTypeId};
        auto range = ParentRange(buildInfo.KMeans.Parent);
        auto addRestricted = [&] (const auto& idx) {
            const auto& status = buildInfo.Shards.at(idx);
            if (!Intersect(typeInfos, range.ToTableRange(), status.Range.ToTableRange()).IsEmptyRange(typeInfos)) {
                AddShard(buildInfo, idx, status);
            }
        };
        if (buildInfo.KMeans.Parent == 0) {
            AddAllShards(buildInfo);
        } else {
            auto it = buildInfo.Cluster2Shards.lower_bound(buildInfo.KMeans.Parent);
            Y_ASSERT(it != buildInfo.Cluster2Shards.end());
            if (it->second.Local == InvalidShardIdx) {
                for (const auto& idx : it->second.Global) {
                    addRestricted(idx);
                }
            }
        }
        if (buildInfo.DoneShards.size() + buildInfo.ToUploadShards.size() <= 1) {
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Local;
        }
        return true;
    }

    bool InitMultiKMeans(TIndexBuildInfo& buildInfo) {
        if (buildInfo.Cluster2Shards.empty()) {
            return false;
        }
        Y_ASSERT(buildInfo.KMeans.Parent != 0);
        for (const auto& [to, state] : buildInfo.Cluster2Shards) {
            if (const auto& [from, local, global] = state; local != InvalidShardIdx) {
                if (const auto* status = buildInfo.Shards.FindPtr(local)) {
                    AddShard(buildInfo, local, *status);
                }
            }
        }
        buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::MultiLocal;
        buildInfo.Cluster2Shards.clear();
        Y_ASSERT(buildInfo.InProgressShards.empty());
        return !buildInfo.ToUploadShards.empty();
    }

    bool SendKMeansSample(TIndexBuildInfo& buildInfo) {
        buildInfo.Sample.MakeStrictTop(buildInfo.KMeans.K);
        if (buildInfo.Sample.MaxProbability == 0) {
            buildInfo.ToUploadShards.clear();
            buildInfo.InProgressShards.clear();
            return true;
        }
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendSampleKRequest(shardIdx, buildInfo); });
    }

    bool SendKMeansReshuffle(TIndexBuildInfo& buildInfo) {
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendKMeansReshuffleRequest(shardIdx, buildInfo); });
    }

    bool SendKMeansLocal(TIndexBuildInfo& buildInfo) {
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendKMeansLocalRequest(shardIdx, buildInfo); });
    }

    bool SendVectorIndex(TIndexBuildInfo& buildInfo) {
        switch (buildInfo.KMeans.State) {
            case TIndexBuildInfo::TKMeans::Sample:
                return SendKMeansSample(buildInfo);
            // TODO(mbkkt)
            // case TIndexBuildInfo::TKMeans::Recompute:
            //     return SendKMeansRecompute(buildInfo);
            case TIndexBuildInfo::TKMeans::Reshuffle:
                return SendKMeansReshuffle(buildInfo);
            case TIndexBuildInfo::TKMeans::Local:
            case TIndexBuildInfo::TKMeans::MultiLocal:
                return SendKMeansLocal(buildInfo);
        }
        return true;
    }

    void ClearDoneShards(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (buildInfo.DoneShards.empty()) {
            return;
        }
        NIceDb::TNiceDb db{txc.DB};
        for (const auto& idx : buildInfo.DoneShards) {
            auto& status = buildInfo.Shards.at(idx);
            Self->PersistBuildIndexUploadReset(db, BuildId, idx, status);
        }
        buildInfo.DoneShards.clear();
        Self->PersistBuildIndexProcessed(db, buildInfo);
    }

    void PersistKMeansState(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        NIceDb::TNiceDb db{txc.DB};
        db.Table<Schema::KMeansTreeProgress>().Key(buildInfo.Id).Update(
            NIceDb::TUpdate<Schema::KMeansTreeProgress::Level>(buildInfo.KMeans.Level),
            NIceDb::TUpdate<Schema::KMeansTreeProgress::State>(buildInfo.KMeans.State),
            NIceDb::TUpdate<Schema::KMeansTreeProgress::Parent>(buildInfo.KMeans.Parent),
            NIceDb::TUpdate<Schema::KMeansTreeProgress::ParentBegin>(buildInfo.KMeans.ParentBegin),
            NIceDb::TUpdate<Schema::KMeansTreeProgress::Child>(buildInfo.KMeans.Child),
            NIceDb::TUpdate<Schema::KMeansTreeProgress::ChildBegin>(buildInfo.KMeans.ChildBegin),
            NIceDb::TUpdate<Schema::KMeansTreeProgress::TableSize>(buildInfo.KMeans.TableSize)
        );
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
            LOG_D("FillPrefixedVectorIndex PrefixIndexDone " << buildInfo.DebugString());

            PersistKMeansState(txc, buildInfo);
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexUploadReset(db, buildInfo);
            ChangeState(BuildId, TIndexBuildInfo::EState::CreateBuild);
            Progress(BuildId);
            return false;
        } else {
            bool filled = buildInfo.KMeans.Level == 2
                ? FillPrefixKMeans(buildInfo)
                : FillLocalKMeans(buildInfo);
            if (!filled) {
                return false;
            }
            LOG_D("FillPrefixedVectorIndex DoneLevel " << buildInfo.DebugString());

            ClearDoneShards(txc, buildInfo);
            Y_ASSERT(buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::MultiLocal);
            const bool needsAnotherLevel = buildInfo.KMeans.NextLevel();
            buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::MultiLocal;
            if (buildInfo.KMeans.Level == 2) {
                buildInfo.KMeans.Parent = buildInfo.KMeans.ParentEnd();
            }
            LOG_D("FillPrefixedVectorIndex NextLevel " << buildInfo.DebugString());

            PersistKMeansState(txc, buildInfo);
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexUploadReset(db, buildInfo);
            if (!needsAnotherLevel) {
                LOG_D("FillPrefixedVectorIndex Done " << buildInfo.DebugString());
                return true;
            }
            ChangeState(BuildId, TIndexBuildInfo::EState::DropBuild);
            Progress(BuildId);
            return false;
        }
    }

    bool FillVectorIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        LOG_D("FillVectorIndex Start " << buildInfo.DebugString());

        if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Upload) {
            return false;
        }
        if (InitSingleKMeans(buildInfo)) {
            LOG_D("FillVectorIndex SingleKMeans " << buildInfo.DebugString());
        }
        if (!SendVectorIndex(buildInfo)) {
            return false;
        }

        if (!buildInfo.Sample.Rows.empty()) {
            if (buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Collect) {
                LOG_D("FillVectorIndex SendUploadSampleKRequest " << buildInfo.DebugString());
                SendUploadSampleKRequest(buildInfo);
                return false;
            }
        }

        LOG_D("FillVectorIndex DoneLevel " << buildInfo.DebugString());
        ClearDoneShards(txc, buildInfo);

        if (!buildInfo.Sample.Rows.empty()) {
            if (buildInfo.KMeans.NextState()) {
                LOG_D("FillVectorIndex NextState " << buildInfo.DebugString());
                PersistKMeansState(txc, buildInfo);
                Progress(BuildId);
                return false;
            }
            buildInfo.Sample.Clear();
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexSampleForget(db, buildInfo);
            LOG_D("FillVectorIndex DoneState " << buildInfo.DebugString());
        }

        if (buildInfo.KMeans.NextParent()) {
            LOG_D("FillVectorIndex NextParent " << buildInfo.DebugString());
            PersistKMeansState(txc, buildInfo);
            Progress(BuildId);
            return false;
        }

        if (InitMultiKMeans(buildInfo)) {
            LOG_D("FillVectorIndex MultiKMeans " << buildInfo.DebugString());
            PersistKMeansState(txc, buildInfo);
            Progress(BuildId);
            return false;
        }

        if (buildInfo.KMeans.NextLevel()) {
            LOG_D("FillVectorIndex NextLevel " << buildInfo.DebugString());
            PersistKMeansState(txc, buildInfo);
            NIceDb::TNiceDb db{txc.DB};
            Self->PersistBuildIndexUploadReset(db, buildInfo);
            ChangeState(BuildId, buildInfo.KMeans.Level > 2
                                    ? TIndexBuildInfo::EState::DropBuild
                                    : TIndexBuildInfo::EState::CreateBuild);
            Progress(BuildId);
            return false;
        }
        LOG_D("FillVectorIndex Done " << buildInfo.DebugString());
        return true;
    }

    bool FillIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        // About Level == 1, for now build index impl tables don't need snapshot,
        // because they're used only by build index
        if (buildInfo.KMeans.Level == 1 && !buildInfo.SnapshotTxId) {
            Y_ABORT_UNLESS(!buildInfo.SnapshotStep);
            Y_ABORT_UNLESS(Self->TablesWithSnapshots.contains(buildInfo.TablePathId));
            Y_ABORT_UNLESS(Self->TablesWithSnapshots.at(buildInfo.TablePathId) == buildInfo.InitiateTxId);

            buildInfo.SnapshotTxId = buildInfo.InitiateTxId;
            Y_ABORT_UNLESS(buildInfo.SnapshotTxId);
            buildInfo.SnapshotStep = Self->SnapshotsStepIds.at(buildInfo.SnapshotTxId);
            Y_ABORT_UNLESS(buildInfo.SnapshotStep);
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
            case TIndexBuildInfo::EBuildKind::BuildVectorIndex:
                return FillVectorIndex(txc, buildInfo);
            case TIndexBuildInfo::EBuildKind::BuildPrefixedVectorIndex:
                return FillPrefixedVectorIndex(txc, buildInfo);
            default:
                Y_ASSERT(false);
                return true;
        }
    }

public:
    explicit TTxProgress(TSelf* self, TIndexBuildId id)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , BuildId(id)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        Y_ABORT_UNLESS(buildInfoPtr);
        auto& buildInfo = *buildInfoPtr->Get();

        LOG_N("TTxBuildProgress: Execute: " << BuildId << " " << buildInfo.State);
        LOG_D("TTxBuildProgress: Execute: " << BuildId << " " << buildInfo.State << " " << buildInfo);

        switch (buildInfo.State) {
        case TIndexBuildInfo::EState::Invalid:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::AlterMainTable:
            if (buildInfo.AlterMainTableTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.AlterMainTableTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), AlterMainTablePropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.AlterMainTableTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.AlterMainTableTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Locking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Locking:
            if (buildInfo.LockTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.LockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), LockPropose(Self, buildInfo, buildInfo.LockTxId, TPath::Init(buildInfo.TablePathId, Self)), 0, ui64(BuildId));
            } else if (!buildInfo.LockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.LockTxId)));
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
            if (buildInfo.IsCancellationRequested() || FillIndex(txc, buildInfo)) {
                ClearAfterFill(ctx, buildInfo);
                ChangeState(BuildId, buildInfo.IsCancellationRequested()
                                         ? TIndexBuildInfo::EState::Cancellation_Applying
                                         : TIndexBuildInfo::EState::Applying);
                Progress(BuildId);

                // make final bill
                Bill(buildInfo);
            } else {
                AskToScheduleBilling(buildInfo);
            }
            break;
        }
        case TIndexBuildInfo::EState::DropBuild:
            Y_ASSERT(buildInfo.IsBuildVectorIndex());
            Y_ASSERT(buildInfo.KMeans.Level > 2);
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
            Y_ASSERT(buildInfo.IsBuildVectorIndex());
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
            Y_ASSERT(buildInfo.IsBuildVectorIndex());
            if (buildInfo.ApplyTxId == InvalidTxId) {
                AllocateTxId(BuildId);
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), LockPropose(Self, buildInfo, buildInfo.ApplyTxId, GetBuildPath(Self, buildInfo, buildInfo.KMeans.ReadFrom())), 0, ui64(BuildId));
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

    static TSerializedTableRange InfiniteRange(ui32 columns) {
        TVector<TCell> vec(columns, TCell());
        TArrayRef<TCell> cells(vec);
        return TSerializedTableRange(TSerializedCellVec::Serialize(cells), "", true, false);
    }

    static TSerializedTableRange ParentRange(NTableIndex::TClusterId parent) {
        if (parent == 0) {
            return {};  // empty
        }
        auto from = TCell::Make(parent - 1);
        auto to = TCell::Make(parent);
        return TSerializedTableRange{{&from, 1}, false, {&to, 1}, true};
    }

    bool InitiateShards(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) {
        LOG_D("InitiateShards " << buildInfo.DebugString());

        Y_ASSERT(buildInfo.Shards.empty());
        Y_ASSERT(buildInfo.ToUploadShards.empty());
        Y_ASSERT(buildInfo.InProgressShards.empty());
        Y_ASSERT(buildInfo.DoneShards.empty());

        TTableInfo::TPtr table;
        if (buildInfo.KMeans.Level == 1) {
            table = Self->Tables.at(buildInfo.TablePathId);
        } else {
            auto path = GetBuildPath(Self, buildInfo, buildInfo.KMeans.ReadFrom());
            table = Self->Tables.at(path->PathId);

            if (!path.IsLocked()) { // lock is needed to prevent table shards from being split
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::LockBuild);
                Progress(buildInfo.Id);
                return false;
            }
            Y_ASSERT(path.LockedBy() == buildInfo.LockTxId);
        }
        auto tableColumns = NTableIndex::ExtractInfo(table); // skip dropped columns
        TSerializedTableRange shardRange = InfiniteRange(tableColumns.Keys.size());
        static constexpr std::string_view LogPrefix = "";

        buildInfo.Cluster2Shards.clear();
        for (const auto& x: table->GetPartitions()) {
            Y_ABORT_UNLESS(Self->ShardInfos.contains(x.ShardIdx));
            TSerializedCellVec bound{x.EndOfRange};
            shardRange.To = bound;
            if (buildInfo.BuildKind == TIndexBuildInfo::EBuildKind::BuildVectorIndex) {
                LOG_D("shard " << x.ShardIdx << " range " << buildInfo.KMeans.RangeToDebugStr(shardRange));
                buildInfo.AddParent(shardRange, x.ShardIdx);
            }
            auto [it, emplaced] = buildInfo.Shards.emplace(x.ShardIdx, TIndexBuildInfo::TShardStatus{std::move(shardRange), "", buildInfo.Shards.size()});
            Y_ASSERT(emplaced);
            shardRange.From = std::move(bound);

            Self->PersistBuildIndexUploadInitiate(db, BuildId, x.ShardIdx, it->second);
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

struct TSchemeShard::TIndexBuilder::TTxBilling: public TSchemeShard::TIndexBuilder::TTxBase  {
private:
    TIndexBuildId BuildIndexId;
    TInstant ScheduledAt;

public:
    explicit TTxBilling(TSelf* self, TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , BuildIndexId(ev->Get()->BuildId)
        , ScheduledAt(ev->Get()->SendAt)
    {}

    bool DoExecute(TTransactionContext& , const TActorContext& ctx) override {
        LOG_I("TTxReply : TTxBilling, id# " << BuildIndexId);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildIndexId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();

        if (!GotScheduledBilling(buildInfo)) {
            return true;
        }

        Bill(buildInfo, ScheduledAt, ctx.Now());

        AskToScheduleBilling(buildInfo);

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }
};

struct TSchemeShard::TIndexBuilder::TTxReply: public TSchemeShard::TIndexBuilder::TTxBase  {
public:
    explicit TTxReply(TSelf* self)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
    {}

    void DoComplete(const TActorContext&) override {
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyRetry: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    struct {
        TIndexBuildId BuildIndexId;
        TTabletId TabletId;
        explicit operator bool() const { return BuildIndexId && TabletId; }
    } PipeRetry;

public:
    explicit TTxReplyRetry(TSelf* self, TIndexBuildId buildId, TTabletId tabletId)
        : TTxReply(self)
        , PipeRetry({buildId, tabletId})
    {}

    bool DoExecute([[maybe_unused]] TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& buildId = PipeRetry.BuildIndexId;
        const auto& tabletId = PipeRetry.TabletId;
        const auto& shardIdx = Self->GetShardIdx(tabletId);

        LOG_N("TTxReply : PipeRetry, id# " << buildId
              << ", tabletId# " << tabletId
              << ", shardIdx# " << shardIdx);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();

        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        LOG_D("TTxReply : PipeRetry"
              << ", TIndexBuildInfo: " << buildInfo);

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            // reschedule shard
            if (buildInfo.InProgressShards.erase(shardIdx)) {
                buildInfo.ToUploadShards.emplace_front(shardIdx);

                Self->IndexBuildPipes.Close(buildId, tabletId, ctx);

                // generate new message with actual LastKeyAck to continue scan
                Progress(buildId);
            }
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << state);
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : PipeRetry superfluous event, id# " << buildId
                  << ", tabletId# " << tabletId
                  << ", shardIdx# " << shardIdx);
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplySampleK: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvDataShard::TEvSampleKResponse::TPtr SampleK;

public:
    explicit TTxReplySampleK(TSelf* self, TEvDataShard::TEvSampleKResponse::TPtr& sampleK)
        : TTxReply(self)
        , SampleK(sampleK)
    {
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = SampleK->Get()->Record;

        LOG_I("TTxReply : TEvSampleKResponse, id# " << record.GetId());

        const auto buildId = TIndexBuildId(record.GetId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        [[maybe_unused]] auto toDebugStr = [&](const NKikimrTxDataShard::TEvSampleKResponse& record) {
            auto r = record;
            // rows are not human readable and can be large like 100Kb+
            r.ClearRows();
            return r.ShortDebugString();
        };
        LOG_D("TTxReply : TEvSampleKResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << toDebugStr(record));

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvSampleKResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            NIceDb::TNiceDb db(txc.DB);
            if (record.ProbabilitiesSize()) {
                Y_ASSERT(record.RowsSize());
                auto& probabilities = record.GetProbabilities();
                auto& rows = *record.MutableRows();
                Y_ASSERT(probabilities.size() == rows.size());
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
                        NIceDb::TUpdate<Schema::KMeansTreeSample::Row>(sample[from].P),
                        NIceDb::TUpdate<Schema::KMeansTreeSample::Data>(sample[from].Row)
                    );
                }
            }

            TBillingStats stats{0, 0, record.GetReadRows(), record.GetReadBytes()};
            shardStatus.Processed += stats;
            buildInfo.Processed += stats;

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();
            shardStatus.Status = record.GetStatus();

            switch (shardStatus.Status) {
            case  NKikimrIndexBuilder::EBuildStatus::DONE:
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.DoneShards.emplace_back(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::ABORTED:
                // datashard gracefully rebooted, reschedule shard
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.ToUploadShards.emplace_front(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                buildInfo.Issue += TStringBuilder()
                    << "One of the shards report BAD_REQUEST at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                return true;
            case  NKikimrIndexBuilder::EBuildStatus::INVALID:
            case  NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
            case  NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
            case  NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
                Y_ABORT("Unreachable");
            }
            Self->PersistBuildIndexUploadProgress(db, buildId, shardIdx, shardStatus);
            Self->IndexBuildPipes.Close(buildId, shardId, ctx);
            Progress(buildId);
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << state);
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvSampleKResponse superfluous message "
                << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyLocalKMeans: public TSchemeShard::TIndexBuilder::TTxReply {
private:
    TEvDataShard::TEvLocalKMeansResponse::TPtr Local;

public:
    explicit TTxReplyLocalKMeans(TSelf* self, TEvDataShard::TEvLocalKMeansResponse::TPtr& local)
        : TTxReply(self)
        , Local(local)
    {
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = Local->Get()->Record;

        LOG_I("TTxReply : TEvLocalKMeansResponse, id# " << record.GetId());

        const auto buildId = TIndexBuildId(record.GetId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvLocalKMeansResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvLocalKMeansResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            TBillingStats stats{record.GetUploadRows(), record.GetUploadBytes(), record.GetReadRows(), record.GetReadBytes()};
            shardStatus.Processed += stats;
            buildInfo.Processed += stats;

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();

            NIceDb::TNiceDb db(txc.DB);
            shardStatus.Status = record.GetStatus();

            switch (shardStatus.Status) {
            case  NKikimrIndexBuilder::EBuildStatus::DONE:
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.DoneShards.emplace_back(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::ABORTED:
                // datashard gracefully rebooted, reschedule shard
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.ToUploadShards.emplace_front(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
            case  NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                buildInfo.Issue += TStringBuilder()
                    << "One of the shards report "<< shardStatus.Status
                    << " at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                return true;
            case  NKikimrIndexBuilder::EBuildStatus::INVALID:
            case  NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
            case  NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
                Y_ABORT("Unreachable");
            }
            Self->PersistBuildIndexUploadProgress(db, buildId, shardIdx, shardStatus);
            Self->IndexBuildPipes.Close(buildId, shardId, ctx);
            Progress(buildId);
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << state);
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvLocalKMeansResponse"
                  << " superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyReshuffleKMeans: public TSchemeShard::TIndexBuilder::TTxReply {
private:
    TEvDataShard::TEvReshuffleKMeansResponse::TPtr Reshuffle;

public:
    explicit TTxReplyReshuffleKMeans(TSelf* self, TEvDataShard::TEvReshuffleKMeansResponse::TPtr& reshuffle)
        : TTxReply(self)
        , Reshuffle(reshuffle)
    {
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = Reshuffle->Get()->Record;

        LOG_I("TTxReply : TEvReshuffleKMeansResponse, id# " << record.GetId());

        const auto buildId = TIndexBuildId(record.GetId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvReshuffleKMeansResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvReshuffleKMeansResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            TBillingStats stats{record.GetUploadRows(), record.GetUploadBytes(), record.GetReadRows(), record.GetReadBytes()};
            shardStatus.Processed += stats;
            buildInfo.Processed += stats;

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();

            NIceDb::TNiceDb db(txc.DB);
            shardStatus.Status = record.GetStatus();

            switch (shardStatus.Status) {
            case  NKikimrIndexBuilder::EBuildStatus::DONE:
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.DoneShards.emplace_back(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::ABORTED:
                // datashard gracefully rebooted, reschedule shard
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.ToUploadShards.emplace_front(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
            case  NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                buildInfo.Issue += TStringBuilder()
                    << "One of the shards report "<< shardStatus.Status
                    << " at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);
                Progress(buildId);
                return true;
            case  NKikimrIndexBuilder::EBuildStatus::INVALID:
            case  NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
            case  NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
                Y_ABORT("Unreachable");
            }
            Self->PersistBuildIndexUploadProgress(db, buildId, shardIdx, shardStatus);
            Self->IndexBuildPipes.Close(buildId, shardId, ctx);
            Progress(buildId);
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << state);
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvReshuffleKMeansResponse"
                  << " superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyPrefixKMeans: public TSchemeShard::TIndexBuilder::TTxReply {
private:
    TEvDataShard::TEvPrefixKMeansResponse::TPtr Prefix;

public:
    explicit TTxReplyPrefixKMeans(TSelf* self, TEvDataShard::TEvPrefixKMeansResponse::TPtr& prefix)
        : TTxReply(self)
        , Prefix(prefix)
    {
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = Prefix->Get()->Record;

        LOG_I("TTxReply : TEvPrefixKMeansResponse, id# " << record.GetId());

        const auto buildId = TIndexBuildId(record.GetId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvPrefixKMeansResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvPrefixKMeansResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            TBillingStats stats{record.GetUploadRows(), record.GetUploadBytes(), record.GetReadRows(), record.GetReadBytes()};
            shardStatus.Processed += stats;
            buildInfo.Processed += stats;

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();

            NIceDb::TNiceDb db(txc.DB);
            shardStatus.Status = record.GetStatus();

            switch (shardStatus.Status) {
            case  NKikimrIndexBuilder::EBuildStatus::DONE:
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.DoneShards.emplace_back(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::ABORTED:
                // datashard gracefully rebooted, reschedule shard
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.ToUploadShards.emplace_front(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
            case  NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                buildInfo.Issue += TStringBuilder()
                    << "One of the shards report "<< shardStatus.Status
                    << " at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                return true;
            case  NKikimrIndexBuilder::EBuildStatus::INVALID:
            case  NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
            case  NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
                Y_ABORT("Unreachable");
            }
            Self->PersistBuildIndexUploadProgress(db, buildId, shardIdx, shardStatus);
            Self->IndexBuildPipes.Close(buildId, shardId, ctx);
            Progress(buildId);
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << state);
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvPrefixKMeansResponse"
                  << " superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyUpload: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvIndexBuilder::TEvUploadSampleKResponse::TPtr Upload;

public:
    explicit TTxReplyUpload(TSelf* self, TEvIndexBuilder::TEvUploadSampleKResponse::TPtr& upload)
        : TTxReply(self)
        , Upload(upload)
    {
    }

    bool DoExecute([[maybe_unused]] TTransactionContext& txc, [[maybe_unused]] const TActorContext& ctx) override {
        auto& record = Upload->Get()->Record;

        LOG_I("TTxReply : TEvUploadSampleKResponse, id# " << record.GetId());

        const auto buildId = TIndexBuildId(record.GetId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvUploadSampleKResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());
        Y_ASSERT(buildInfo.IsBuildVectorIndex());

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            Y_ABORT_UNLESS(buildInfo.Sample.State == TIndexBuildInfo::TSample::EState::Upload);
            NIceDb::TNiceDb db(txc.DB);

            TBillingStats stats{record.GetUploadRows(), record.GetUploadBytes(), 0, 0};
            buildInfo.Processed += stats;
            // As long as we don't try to upload sample in parallel with requests to shards,
            // it's okay to persist Processed not incrementally
            Self->PersistBuildIndexProcessed(db, buildInfo);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);

            auto status = record.GetUploadStatus();
            if (status == Ydb::StatusIds::SUCCESS) {
                buildInfo.Sample.State = TIndexBuildInfo::TSample::EState::Done;
                Progress(buildId);
            } else {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(record.GetIssues(), issues);
                buildInfo.Issue += issues.ToString();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);
                Progress(buildId);
            }
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << state);
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvUploadSampleKResponse superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyProgress: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvDataShard::TEvBuildIndexProgressResponse::TPtr ShardProgress;
public:
    explicit TTxReplyProgress(TSelf* self, TEvDataShard::TEvBuildIndexProgressResponse::TPtr& shardProgress)
        : TTxReply(self)
        , ShardProgress(shardProgress)
    {}


    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const NKikimrTxDataShard::TEvBuildIndexProgressResponse& record = ShardProgress->Get()->Record;

        LOG_I("TTxReply : TEvBuildIndexProgressResponse, id# " << record.GetBuildIndexId());

        const auto buildId = TIndexBuildId(record.GetBuildIndexId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvBuildIndexProgressResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvBuildIndexProgressResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

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
                    Y_VERIFY_S(cmp < 0,
                               "check that all LastKeyAcks are monotonously increase"
                                   << ", next: " << DebugPrintPoint(keyTypes, next.GetCells(), *AppData()->TypeRegistry)
                                   << ", prev: " << DebugPrintPoint(keyTypes, prev.GetCells(), *AppData()->TypeRegistry));
                }

                shardStatus.LastKeyAck = record.GetLastKeyAck();
            }

            // TODO(mbkkt) we should account uploads and reads separately
            TBillingStats stats{record.GetRowsDelta(), record.GetBytesDelta(), record.GetRowsDelta(), record.GetBytesDelta()};
            shardStatus.Processed += stats;
            buildInfo.Processed += stats;

            shardStatus.Status = record.GetStatus();
            shardStatus.UploadStatus = record.GetUploadStatus();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();

            NIceDb::TNiceDb db(txc.DB);
            switch (shardStatus.Status) {
            case  NKikimrIndexBuilder::EBuildStatus::INVALID:
                Y_ABORT("Unreachable");
            case  NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
            case  NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
                // no oop, wait resolution. Progress key are persisted
                Self->PersistBuildIndexUploadProgress(db, buildId, shardIdx, shardStatus);
                return true;
            case  NKikimrIndexBuilder::EBuildStatus::DONE:
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.DoneShards.emplace_back(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::ABORTED:
                // datashard gracefully rebooted, reschedule shard
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.ToUploadShards.emplace_front(shardIdx);
                }
                break;
            case  NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
            case  NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                buildInfo.Issue += TStringBuilder()
                    << "One of the shards report " << shardStatus.Status
                    << " at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);
                Progress(buildId);
                return true;
            }
            Self->PersistBuildIndexUploadProgress(db, buildId, shardIdx, shardStatus);
            Self->IndexBuildPipes.Close(buildId, shardId, ctx);
            Progress(buildId);
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << state);
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvBuildIndexProgressResponse"
                  << " superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyCompleted: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TTxId CompletedTxId;
public:
    explicit TTxReplyCompleted(TSelf* self, TTxId completedTxId)
        : TTxReply(self)
        , CompletedTxId(completedTxId)
    {}

    bool DoExecute(TTransactionContext& txc, [[maybe_unused]] const TActorContext& ctx) override {
        const auto txId = CompletedTxId;
        const auto* buildIdPtr = Self->TxIdToIndexBuilds.FindPtr(txId);
        if (!buildIdPtr) {
            LOG_I("TTxReply : TEvNotifyTxCompletionResult superfluous message"
                  << ", txId: " << txId
                  << ", buildInfoId not found");
            return true;
        }

        const auto buildId = *buildIdPtr;
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        Y_ABORT_UNLESS(buildInfoPtr);
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_I("TTxReply : TEvNotifyTxCompletionResult"
              << ", txId# " << txId
              << ", buildInfoId: " << buildInfo.Id);
        LOG_D("TTxReply : TEvNotifyTxCompletionResult"
              << ", txId# " << txId
              << ", buildInfo: " << buildInfo);

        const auto state = buildInfo.State;
        NIceDb::TNiceDb db(txc.DB);

        switch (state) {
        case TIndexBuildInfo::EState::AlterMainTable:
        {
            Y_ABORT_UNLESS(txId == buildInfo.AlterMainTableTxId);

            buildInfo.AlterMainTableTxDone = true;
            Self->PersistBuildIndexAlterMainTableTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Locking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.LockTxId);

            buildInfo.LockTxDone = true;
            Self->PersistBuildIndexLockTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Initiating:
        {
            Y_ABORT_UNLESS(txId == buildInfo.InitiateTxId);

            buildInfo.InitiateTxDone = true;
            Self->PersistBuildIndexInitiateTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Rejection_Applying:
        {
            Y_VERIFY_S(txId == buildInfo.ApplyTxId, state);

            buildInfo.ApplyTxDone = true;
            Self->PersistBuildIndexApplyTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        {
            Y_VERIFY_S(txId == buildInfo.UnlockTxId, state);

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
            Y_FAIL_S("Unreachable " << state);
        }

        Progress(buildId);

        return true;
    }

};

struct TSchemeShard::TIndexBuilder::TTxReplyModify: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult;
public:
    explicit TTxReplyModify(TSelf* self, TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult)
        : TTxReply(self)
        , ModifyResult(modifyResult)
    {}

    void ReplyOnCreation(const TIndexBuildInfo& buildInfo,
                         const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        auto responseEv = MakeHolder<TEvIndexBuilder::TEvCreateResponse>(ui64(buildInfo.Id));

        Fill(*responseEv->Record.MutableIndexBuild(), buildInfo);

        auto& response = responseEv->Record;
        response.SetStatus(status);

        if (buildInfo.Issue) {
            AddIssue(response.MutableIssues(), buildInfo.Issue);
        }

        LOG_N("TIndexBuilder::TTxReply: ReplyOnCreation"
              << ", BuildIndexId: " << buildInfo.Id
              << ", status: " << Ydb::StatusIds::StatusCode_Name(status)
              << ", error: " << buildInfo.Issue
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
                  << ", txId: " << record.GetTxId()
                  << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                  << ", BuildIndexId not found");
            return true;
        }

        const auto buildId = *buildIdPtr;
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        Y_ABORT_UNLESS(buildInfoPtr);
        // We need this because we use buildInfo after EraseBuildInfo
        auto buildInfoPin = *buildInfoPtr;
        auto& buildInfo = *buildInfoPin;

        LOG_I("TTxReply : TEvModifySchemeTransactionResult"
              << ", BuildIndexId: " << buildId
              << ", cookie: " << ModifyResult->Cookie
              << ", txId: " << record.GetTxId()
              << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus()));

        LOG_D("TTxReply : TEvModifySchemeTransactionResult"
              << ", buildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());

        const auto state = buildInfo.State;
        NIceDb::TNiceDb db(txc.DB);

        auto replyOnCreation = [&] {
            auto statusCode = TranslateStatusCode(record.GetStatus());

            if (statusCode != Ydb::StatusIds::SUCCESS) {
                buildInfo.Issue += TStringBuilder()
                    << "At " << state << " state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                Self->PersistBuildIndexForget(db, buildInfo);
                EraseBuildInfo(buildInfo);
            }

            ReplyOnCreation(buildInfo, statusCode);
        };

        auto ifErrorMoveTo = [&] (TIndexBuildInfo::EState to) {
            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                Y_ABORT("NEED MORE TESTING");
                // no op
            } else {
                buildInfo.Issue += TStringBuilder()
                    << "At " << state << " state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, to);
            }
        };

        switch (state) {
        case TIndexBuildInfo::EState::AlterMainTable:
        {
            Y_ABORT_UNLESS(txId == buildInfo.AlterMainTableTxId);

            buildInfo.AlterMainTableTxStatus = record.GetStatus();
            Self->PersistBuildIndexAlterMainTableTxStatus(db, buildInfo);

            replyOnCreation();
            break;
        }
        case TIndexBuildInfo::EState::Locking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.LockTxId);

            buildInfo.LockTxStatus = record.GetStatus();
            Self->PersistBuildIndexLockTxStatus(db, buildInfo);

            replyOnCreation();
            break;
        }
        case TIndexBuildInfo::EState::Initiating:
        {
            Y_ABORT_UNLESS(txId == buildInfo.InitiateTxId);

            buildInfo.InitiateTxStatus = record.GetStatus();
            Self->PersistBuildIndexInitiateTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::DropBuild:
        case TIndexBuildInfo::EState::CreateBuild:
        case TIndexBuildInfo::EState::LockBuild:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Rejection_Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo.ApplyTxId);

            buildInfo.ApplyTxStatus = record.GetStatus();
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.UnlockTxId);

            buildInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Cancellation_Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo.ApplyTxId);

            buildInfo.ApplyTxStatus = record.GetStatus();
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Cancellation_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.UnlockTxId);

            buildInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Cancelled);
            break;
        }
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.UnlockTxId);

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
            Y_FAIL_S("Unreachable " << state);
        }

        Progress(buildId);

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyAllocate: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult;
public:
    explicit TTxReplyAllocate(TSelf* self, TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult)
        : TTxReply(self)
        , AllocateResult(allocateResult)
    {}

    bool DoExecute(TTransactionContext& txc,[[maybe_unused]] const TActorContext& ctx) override {
        TIndexBuildId buildId = TIndexBuildId(AllocateResult->Cookie);
        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());

        LOG_I("TTxReply : TEvAllocateResult"
              << ", BuildIndexId: " << buildId
              << ", txId# " << txId);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        Y_ABORT_UNLESS(buildInfoPtr);
        auto& buildInfo = *buildInfoPtr->Get();

        LOG_D("TTxReply : TEvAllocateResult"
              << ", buildInfo: " << buildInfo);

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
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Rejection_Applying:
            if (!buildInfo.ApplyTxId) {
                buildInfo.ApplyTxId = txId;
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
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
            Y_FAIL_S("Unreachable " << state);
        }

        Progress(buildId);

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

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvLocalKMeansResponse::TPtr& local) {
    return new TIndexBuilder::TTxReplyLocalKMeans(this, local);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvPrefixKMeansResponse::TPtr& prefix) {
    return new TIndexBuilder::TTxReplyPrefixKMeans(this, prefix);
}

ITransaction* TSchemeShard::CreateTxReply(TEvIndexBuilder::TEvUploadSampleKResponse::TPtr& upload) {
    return new TIndexBuilder::TTxReplyUpload(this, upload);
}

ITransaction* TSchemeShard::CreatePipeRetry(TIndexBuildId indexBuildId, TTabletId tabletId) {
    return new TIndexBuilder::TTxReplyRetry(this, indexBuildId, tabletId);
}

ITransaction* TSchemeShard::CreateTxBilling(TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev) {
    return new TIndexBuilder::TTxBilling(this, ev);
}


} // NSchemeShard
} // NKikimr
