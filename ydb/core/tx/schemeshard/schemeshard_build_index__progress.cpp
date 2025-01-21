#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"  // for NTableIndex::ExtractInfo
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/datashard/upload_stats.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/core/ydb_convert/table_description.h>


namespace NKikimr {
namespace NSchemeShard {

static constexpr const char* Name(TIndexBuildInfo::EState state) noexcept {
    switch (state) {
    case TIndexBuildInfo::EState::Invalid:
        return "Invalid";
    case TIndexBuildInfo::EState::AlterMainTable:
        return "AlterMainTable";
    case TIndexBuildInfo::EState::Locking:
        return "Locking";
    case TIndexBuildInfo::EState::GatheringStatistics:
        return "GatheringStatistics";
    case TIndexBuildInfo::EState::Initiating:
        return "Initiating";
    case TIndexBuildInfo::EState::Filling:
        return "Filling";
    case TIndexBuildInfo::EState::DropBuild:
        return "DropBuild";
    case TIndexBuildInfo::EState::CreateBuild:
        return "CreateBuild";
    case TIndexBuildInfo::EState::Applying:
        return "Applying";
    case TIndexBuildInfo::EState::Unlocking:
        return "Unlocking";
    case TIndexBuildInfo::EState::Done:
        return "Done";
    case TIndexBuildInfo::EState::Cancellation_Applying:
        return "Cancellation_Applying";
    case TIndexBuildInfo::EState::Cancellation_Unlocking:
        return "Cancellation_Unlocking";
    case TIndexBuildInfo::EState::Cancelled:
        return "Cancelled";
    case TIndexBuildInfo::EState::Rejection_Applying:
        return "Rejection_Applying";
    case TIndexBuildInfo::EState::Rejection_Unlocking:
        return "Rejection_Unlocking";
    case TIndexBuildInfo::EState::Rejected:
        return "Rejected";
    }
}

// return count, parts, step
static std::tuple<ui32, ui32, ui32> ComputeKMeansBoundaries(const NSchemeShard::TTableInfo& tableInfo, const TIndexBuildInfo& buildInfo) {
    const auto& kmeans = buildInfo.KMeans;
    Y_ASSERT(kmeans.K != 0);
    Y_ASSERT((kmeans.K & (kmeans.K - 1)) == 0);
    const auto count = TIndexBuildInfo::TKMeans::BinPow(kmeans.K, kmeans.Level);
    ui32 step = 1;
    auto parts = count;
    auto shards = tableInfo.GetShard2PartitionIdx().size();
    if (!buildInfo.KMeans.NeedsAnotherLevel() || shards <= 1) {
        shards = 1;
        parts = 1;
    }
    for (; shards < parts; parts /= 2) {
        step *= 2;
    }
    for (; parts < shards / 2; parts *= 2) {
        Y_ASSERT(step == 1);
    }
    return {count, parts, step};
}

class TUploadSampleK: public TActorBootstrapped<TUploadSampleK> {
    using TThis = TUploadSampleK;
    using TBase = TActorBootstrapped<TThis>;

protected:
    TString LogPrefix;
    TString TargetTable;

    NDataShard::TUploadRetryLimits Limits;

    TActorId ResponseActorId;
    ui64 BuildIndexId = 0;
    TIndexBuildInfo::TSample::TRows Init;

    std::shared_ptr<NTxProxy::TUploadTypes> Types;
    std::shared_ptr<NTxProxy::TUploadRows> Rows;

    TActorId Uploader;
    ui32 RetryCount = 0;
    ui32 RowsBytes = 0;
    ui32 Child = 0;

    NDataShard::TUploadStatus UploadStatus;

public:
    TUploadSampleK(TString targetTable,
                   const TIndexBuildInfo::TLimits& limits,
                   const TActorId& responseActorId,
                   ui64 buildIndexId,
                   TIndexBuildInfo::TSample::TRows init,
                   ui32 child)
        : TargetTable(std::move(targetTable))
        , ResponseActorId(responseActorId)
        , BuildIndexId(buildIndexId)
        , Init(std::move(init))
        , Child(child)
    {
        LogPrefix = TStringBuilder()
            << "TUploadSampleK: BuildIndexId: " << BuildIndexId
            << " ResponseActorId: " << ResponseActorId;
        Limits.MaxUploadRowsRetryCount = limits.MaxRetries;
        Y_ASSERT(!Init.empty());
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
        PrimaryKeys[0] = TCell::Make(ui32{0});
        for (auto& [_, row] : Init) {
            RowsBytes += row.size();
            PrimaryKeys[1] = TCell::Make(ui32{Child++});
            // TODO(mbkkt) we can avoid serialization of PrimaryKeys every iter
            Rows->emplace_back(TSerializedCellVec{PrimaryKeys}, std::move(row));
        }
        Init = {}; // release memory
        RowsBytes += Rows->size() * TSerializedCellVec::SerializedSize(PrimaryKeys);

        Types = std::make_shared<NTxProxy::TUploadTypes>(3);
        Ydb::Type type;
        type.set_type_id(Ydb::Type::UINT32);
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

        if (UploadStatus.IsRetriable() && RetryCount < Limits.MaxUploadRowsRetryCount) {
            LOG_N("Got retriable error, " << Debug() << " RetryCount: " << RetryCount);

            this->Schedule(Limits.GetTimeoutBackouff(RetryCount), new TEvents::TEvWakeup());
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

THolder<TEvSchemeShard::TEvModifySchemeTransaction> LockPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.LockTxId), ss->TabletID());
    propose->Record.SetFailOnExist(false);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLock);
    modifyScheme.SetInternal(true);

    TPath path = TPath::Init(buildInfo.TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    modifyScheme.MutableLockConfig()->SetName(path.LeafName());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> InitiatePropose(
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
        Y_ABORT("Unknown operation kind while building InitiatePropose");
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropBuildPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ASSERT(buildInfo.IsBuildVectorIndex());

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    auto path = TPath::Init(buildInfo.TablePathId, ss).Dive(buildInfo.IndexName);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(path.PathString());

    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    modifyScheme.MutableDrop()->SetName(buildInfo.KMeans.WriteTo(true));

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
    {
        buildInfo.SerializeToProto(ss, modifyScheme.MutableInitiateIndexBuild());
        const auto& indexDesc = modifyScheme.GetInitiateIndexBuild().GetIndex();
        const auto& baseTableColumns = NTableIndex::ExtractInfo(tableInfo);
        const auto& indexKeys = NTableIndex::ExtractInfo(indexDesc);
        implTableColumns = CalcTableImplDescription(buildInfo.IndexType, baseTableColumns, indexKeys);
        Y_ABORT_UNLESS(indexKeys.KeyColumns.size() == 1);
        implTableColumns.Columns.emplace(indexKeys.KeyColumns[0]);
        modifyScheme.ClearInitiateIndexBuild();
    }

    // TODO(mbkkt) for levels greater than zero we need to disable split/merge completely
    // For now it's not guranteed, but very likely
    // But lock is really unconvinient approach (needs to store TxId/etc)
    // So maybe best way to do this is specify something in defintion, that will prevent these operations like IsBackup
    using namespace NTableIndex::NTableVectorKmeansTreeIndex;
    modifyScheme.SetWorkingDir(path.Dive(buildInfo.IndexName).PathString());
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable);
    auto& op = *modifyScheme.MutableCreateTable();
    const char* suffix = buildInfo.KMeans.Level % 2 != 0 ? BuildSuffix0 : BuildSuffix1;
    op = CalcVectorKmeansTreePostingImplTableDesc(tableInfo, tableInfo->PartitionConfig(), implTableColumns, {}, suffix);

    const auto [count, parts, step] = ComputeKMeansBoundaries(*tableInfo, buildInfo);

    auto& config = *op.MutablePartitionConfig();
    config.SetShadowData(true);

    auto& policy = *config.MutablePartitioningPolicy();
    policy.SetSizeToSplit(0); // disable auto split/merge
    policy.SetMinPartitionsCount(parts);
    policy.SetMaxPartitionsCount(parts);
    policy.ClearFastSplitSettings();
    policy.ClearSplitByLoadSettings();

    op.ClearSplitBoundary();
    if (parts <= 1) {
        return propose;
    }
    auto i = buildInfo.KMeans.Parent;
    for (const auto end = i + count;;) {
        i += step;
        if (i >= end) {
            Y_ASSERT(op.SplitBoundarySize() == std::min(count, parts) - 1);
            break;
        }
        auto cell = TCell::Make(i);
        op.AddSplitBoundary()->SetSerializedKeyPrefix(TSerializedCellVec::Serialize({&cell, 1}));
    }
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

    return propose;
}

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgress: public TSchemeShard::TIndexBuilder::TTxBase  {
private:
    TIndexBuildId BuildId;

    TDeque<std::tuple<TTabletId, ui64, THolder<IEventBase>>> ToTabletSend;

    template <typename Record>
    TTabletId CommonFillRecord(Record& record, TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        TTabletId shardId = Self->ShardInfos.at(shardIdx).TabletID;
        record.SetTabletId(ui64(shardId));
        if (buildInfo.SnapshotTxId) {
            Y_ASSERT(buildInfo.SnapshotStep);
            record.SetSnapshotTxId(ui64(buildInfo.SnapshotTxId));
            record.SetSnapshotStep(ui64(buildInfo.SnapshotStep));
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

        if (buildInfo.KMeans.Parent == 0) {
            buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());
        } else {
            auto path = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName);
            path.Dive(buildInfo.KMeans.ReadFrom())->PathId.ToProto(ev->Record.MutablePathId());
        }

        ev->Record.SetK(buildInfo.KMeans.K);
        ev->Record.SetMaxProbability(buildInfo.Sample.MaxProbability);
        if (buildInfo.KMeans.Parent != 0) {
            auto from = TCell::Make(buildInfo.KMeans.Parent - 1);
            auto to = TCell::Make(buildInfo.KMeans.Parent);
            TSerializedTableRange range{{&from, 1}, false, {&to, 1}, true};
            range.Serialize(*ev->Record.MutableKeyRange());
        }

        ev->Record.AddColumns(buildInfo.IndexColumns[0]);

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);
        ev->Record.SetSeed(ui64(shardId));
        LOG_D("TTxBuildProgress: TEvSampleKRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace_back(shardId, ui64(BuildId), std::move(ev));
    }

    void SendKMeansReshuffleRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ASSERT(buildInfo.IsBuildVectorIndex());
        auto ev = MakeHolder<TEvDataShard::TEvReshuffleKMeansRequest>();
        ev->Record.SetId(ui64(BuildId));

        auto path = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName);
        if (buildInfo.KMeans.Parent == 0) {
            buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());
        } else {
            path.Dive(buildInfo.KMeans.ReadFrom())->PathId.ToProto(ev->Record.MutablePathId());
            path.Rise();
        }

        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings().settings();
        ev->Record.SetUpload(buildInfo.KMeans.GetUpload());
        ev->Record.SetParent(buildInfo.KMeans.Parent);
        ev->Record.SetChild(buildInfo.KMeans.ChildBegin);

        auto& clusters = *ev->Record.MutableClusters();
        clusters.Reserve(buildInfo.Sample.Rows.size());
        for (const auto& [_, row] : buildInfo.Sample.Rows) {
            *clusters.Add() = row;
        }

        ev->Record.SetPostingName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());

        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns[0]);
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);
        LOG_D("TTxBuildProgress: TEvReshuffleKMeansRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace_back(shardId, ui64(BuildId), std::move(ev));
    }

    void SendKMeansLocalRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ASSERT(buildInfo.IsBuildVectorIndex());
        auto ev = MakeHolder<TEvDataShard::TEvLocalKMeansRequest>();
        ev->Record.SetId(ui64(BuildId));

        auto path = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName);
        if (buildInfo.KMeans.Parent == 0) {
            buildInfo.TablePathId.ToProto(ev->Record.MutablePathId());
        } else {
            path.Dive(buildInfo.KMeans.ReadFrom())->PathId.ToProto(ev->Record.MutablePathId());
            path.Rise();
        }
        *ev->Record.MutableSettings() = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(
            buildInfo.SpecializedIndexDescription).GetSettings().settings();
        ev->Record.SetK(buildInfo.KMeans.K);
        ev->Record.SetUpload(buildInfo.KMeans.GetUpload());
        ev->Record.SetState(NKikimrTxDataShard::TEvLocalKMeansRequest::SAMPLE);

        ev->Record.SetDoneRounds(0);
        ev->Record.SetNeedsRounds(3); // TODO(mbkkt) should be configurable

        if (buildInfo.KMeans.NeedsAnotherParent()) {
            ev->Record.SetParent(buildInfo.KMeans.Parent);
        } else {
            ev->Record.SetParent(0);
        }
        ev->Record.SetChild(buildInfo.KMeans.ChildBegin);

        ev->Record.SetPostingName(path.Dive(buildInfo.KMeans.WriteTo()).PathString());
        path.Rise().Dive(NTableIndex::NTableVectorKmeansTreeIndex::LevelTable);
        ev->Record.SetLevelName(path.PathString());

        ev->Record.SetEmbeddingColumn(buildInfo.IndexColumns[0]);
        *ev->Record.MutableDataColumns() = {
            buildInfo.DataColumns.begin(), buildInfo.DataColumns.end()
        };

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);
        ev->Record.SetSeed(ui64(shardId));
        LOG_D("TTxBuildProgress: TEvLocalKMeansRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace_back(shardId, ui64(BuildId), std::move(ev));
    }

    void SendBuildIndexRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        auto ev = MakeHolder<TEvDataShard::TEvBuildIndexCreateRequest>();
        ev->Record.SetBuildIndexId(ui64(BuildId));

        ev->Record.SetOwnerId(buildInfo.TablePathId.OwnerId);
        ev->Record.SetPathId(buildInfo.TablePathId.LocalPathId);

        if (buildInfo.IsBuildSecondaryIndex()) {
            if (buildInfo.TargetName.empty()) {
                TPath implTable = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName).Dive(NTableIndex::ImplTable);
                buildInfo.TargetName = implTable.PathString();

                const auto& implTableInfo = Self->Tables.at(implTable.Base()->PathId);
                auto implTableColumns = NTableIndex::ExtractInfo(implTableInfo);
                buildInfo.FillIndexColumns.clear();
                buildInfo.FillIndexColumns.reserve(implTableColumns.Keys.size());
                for (const auto& x: implTableColumns.Keys) {
                    buildInfo.FillIndexColumns.emplace_back(x);
                    implTableColumns.Columns.erase(x);
                }
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
        } else if (buildInfo.IsBuildColumns()) {
            buildInfo.SerializeToProto(Self, ev->Record.MutableColumnBuildSettings());
        }

        ev->Record.SetTargetName(buildInfo.TargetName);

        ev->Record.SetMaxBatchRows(buildInfo.Limits.MaxBatchRows);
        ev->Record.SetMaxBatchBytes(buildInfo.Limits.MaxBatchBytes);
        ev->Record.SetMaxRetries(buildInfo.Limits.MaxRetries);

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);

        LOG_D("TTxBuildProgress: TEvBuildIndexCreateRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace_back(shardId, ui64(BuildId), std::move(ev));
    }

    void SendUploadSampleKRequest(TIndexBuildInfo& buildInfo) {
        auto path = TPath::Init(buildInfo.TablePathId, Self)
                        .Dive(buildInfo.IndexName)
                        .Dive(NTableIndex::NTableVectorKmeansTreeIndex::LevelTable);
        Y_ASSERT(buildInfo.Sample.Rows.size() <= buildInfo.KMeans.K);
        auto actor = new TUploadSampleK(path.PathString(),
            buildInfo.Limits, Self->SelfId(), ui64(BuildId),
            buildInfo.Sample.Rows, buildInfo.KMeans.ChildBegin);

        TActivationContext::AsActorContext().MakeFor(Self->SelfId()).Register(actor);
        buildInfo.Sample.Sent = true;
    }

    void ClearAfterFill(const TActorContext& ctx, TIndexBuildInfo& buildInfo) {
        buildInfo.DoneShards = {};
        buildInfo.InProgressShards = {};
        buildInfo.ToUploadShards = {};

        Self->IndexBuildPipes.CloseAll(BuildId, ctx);
    }

    template<typename Send>
    bool SendToShards(TIndexBuildInfo& buildInfo, Send&& send) {
        while (!buildInfo.ToUploadShards.empty() && buildInfo.InProgressShards.size() < buildInfo.Limits.MaxShards) {
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
        for (const auto& [idx, status] : buildInfo.Shards) {
            AddShard(buildInfo, idx, status);
        }
    }

    bool FillTable(TIndexBuildInfo& buildInfo) {
        if (buildInfo.DoneShards.empty() && buildInfo.ToUploadShards.empty() && buildInfo.InProgressShards.empty()) {
            AddAllShards(buildInfo);
        }
        return SendToShards(buildInfo, [&](TShardIdx shardIdx) { SendBuildIndexRequest(shardIdx, buildInfo); }) &&
               buildInfo.DoneShards.size() == buildInfo.Shards.size();
    }

    bool InitSingleKMeans(TIndexBuildInfo& buildInfo) {
        if (!buildInfo.DoneShards.empty() || !buildInfo.InProgressShards.empty() || !buildInfo.ToUploadShards.empty()
            || buildInfo.KMeans.State == TIndexBuildInfo::TKMeans::Local) {
            return false;
        }
        std::array<NScheme::TTypeInfo, 1> typeInfos{NScheme::NTypeIds::Uint32};
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
        for (const auto& [to, state] : buildInfo.Cluster2Shards) {
            if (const auto& [from, local, global] = state; local != InvalidShardIdx) {
                if (const auto* status = buildInfo.Shards.FindPtr(local)) {
                    AddShard(buildInfo, local, *status);
                }
            }
        }
        buildInfo.KMeans.State = TIndexBuildInfo::TKMeans::Local;
        buildInfo.Cluster2Shards.clear();
        Y_ASSERT(buildInfo.InProgressShards.empty());
        Y_ASSERT(buildInfo.DoneShards.empty());
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

    bool FillVectorIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        if (InitSingleKMeans(buildInfo)) {
            LOG_D("FillIndex::SingleKMeans::Start " << buildInfo.KMeansTreeToDebugStr());
        }
        if (!SendVectorIndex(buildInfo)) {
            return false;
        }
        // TODO(mbkkt) persist buildInfo.KMeans changes

        LOG_D("FillIndex::SendVectorIndex::Done " << buildInfo.KMeansTreeToDebugStr());
        if (!buildInfo.Sample.Rows.empty()) {
            if (!buildInfo.Sample.Sent) {
                LOG_D("FillIndex::SendSample::Start " << buildInfo.KMeansTreeToDebugStr());
                SendUploadSampleKRequest(buildInfo);
                return false;
            }
            LOG_D("FillIndex::SendSample::Done " << buildInfo.KMeansTreeToDebugStr());
        }

        LOG_D("FillIndex::ClearDoneShards " << buildInfo.KMeansTreeToDebugStr());
        ClearDoneShards(txc, buildInfo);

        if (!buildInfo.Sample.Rows.empty()) {
            if (buildInfo.KMeans.NextState()) {
                LOG_D("FillIndex::NextState::Start " << buildInfo.KMeansTreeToDebugStr());
                Progress(BuildId);
                return false;
            }
            buildInfo.Sample.Clear();
            LOG_D("FillIndex::NextState::Done " << buildInfo.KMeansTreeToDebugStr());
        }

        if (buildInfo.KMeans.NextParent()) {
            LOG_D("FillIndex::NextParent::Start " << buildInfo.KMeansTreeToDebugStr());
            Progress(BuildId);
            return false;
        }

        if (InitMultiKMeans(buildInfo)) {
            LOG_D("FillIndex::MultiKMeans::Start " << buildInfo.KMeansTreeToDebugStr());
            Progress(BuildId);
            return false;
        }

        if (buildInfo.KMeans.NextLevel()) {
            LOG_D("FillIndex::NextLevel::Start " << buildInfo.KMeansTreeToDebugStr());
            ChangeState(BuildId, TIndexBuildInfo::EState::DropBuild);
            Progress(BuildId);
            return false;
        }
        LOG_D("FillIndex::Done " << buildInfo.KMeansTreeToDebugStr());
        return true;
    }

    bool FillIndex(TTransactionContext& txc, TIndexBuildInfo& buildInfo) {
        // About Parent == 0, for now build index impl tables don't need snapshot,
        // because they're used only by build index
        if (buildInfo.KMeans.Parent == 0 && !buildInfo.SnapshotTxId) {
            Y_ABORT_UNLESS(!buildInfo.SnapshotStep);
            Y_ABORT_UNLESS(Self->TablesWithSnapshots.contains(buildInfo.TablePathId));
            Y_ABORT_UNLESS(Self->TablesWithSnapshots.at(buildInfo.TablePathId) == buildInfo.InitiateTxId);

            buildInfo.SnapshotTxId = buildInfo.InitiateTxId;
            Y_ABORT_UNLESS(buildInfo.SnapshotTxId);
            buildInfo.SnapshotStep = Self->SnapshotsStepIds.at(buildInfo.SnapshotTxId);
            Y_ABORT_UNLESS(buildInfo.SnapshotStep);
        }
        if (buildInfo.Shards.empty()) {
            LOG_D("FillIndex::InitiateShards " << buildInfo.KMeansTreeToDebugStr());
            NIceDb::TNiceDb db(txc.DB);
            InitiateShards(db, buildInfo);
        }
        if (buildInfo.IsBuildVectorIndex()) {
            return FillVectorIndex(txc, buildInfo);
        } else {
            Y_ASSERT(buildInfo.IsBuildSecondaryIndex() || buildInfo.IsBuildColumns());
            return FillTable(buildInfo);
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

        LOG_I("TTxBuildProgress: Resume"
              << ": id# " << BuildId);
        LOG_D("TTxBuildProgress: Resume"
              << ": " << buildInfo);

        switch (buildInfo.State) {
        case TIndexBuildInfo::EState::Invalid:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::AlterMainTable:
            if (buildInfo.AlterMainTableTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
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
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.LockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), LockPropose(Self, buildInfo), 0, ui64(BuildId));
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
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.InitiateTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), InitiatePropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.InitiateTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.InitiateTxId)));
            } else {
                if (buildInfo.IsBuildVectorIndex() && buildInfo.KMeans.NeedsAnotherLevel()) {
                    ChangeState(BuildId, TIndexBuildInfo::EState::DropBuild);
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
            if (buildInfo.ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), DropBuildPropose(Self, buildInfo), 0, ui64(BuildId));
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
                Self->PersistBuildIndexUploadReset(db, buildInfo);
                Self->PersistBuildIndexProcessed(db, buildInfo);

                ChangeState(BuildId, TIndexBuildInfo::EState::CreateBuild);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::CreateBuild:
            Y_ASSERT(buildInfo.IsBuildVectorIndex());
            if (buildInfo.ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CreateBuildPropose(Self, buildInfo), 0, ui64(BuildId));
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
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
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
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
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
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
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
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
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
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
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
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
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

    static TSerializedTableRange ParentRange(ui32 parent) {
        if (parent == 0) {
            return {};  // empty
        }
        auto from = TCell::Make(parent - 1);
        auto to = TCell::Make(parent);
        return TSerializedTableRange{{&from, 1}, false, {&to, 1}, true};
    }

    void InitiateShards(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) {
        Y_ASSERT(buildInfo.Shards.empty());
        Y_ASSERT(buildInfo.ToUploadShards.empty());
        Y_ASSERT(buildInfo.InProgressShards.empty());
        Y_ASSERT(buildInfo.DoneShards.empty());

        TTableInfo::TPtr table;
        if (buildInfo.KMeans.Parent == 0) {
            table = Self->Tables.at(buildInfo.TablePathId);
        } else {
            auto path = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName);
            table = Self->Tables.at(path.Dive(buildInfo.KMeans.ReadFrom())->PathId);
        }
        auto tableColumns = NTableIndex::ExtractInfo(table); // skip dropped columns
        TSerializedTableRange shardRange = InfiniteRange(tableColumns.Keys.size());
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD, TStringBuilder() << "From: " << shardRange.From.GetCells().size() << "To: " << shardRange.To.GetCells().size());

        buildInfo.Cluster2Shards.clear();
        for (const auto& x: table->GetPartitions()) {
            Y_ABORT_UNLESS(Self->ShardInfos.contains(x.ShardIdx));
            TSerializedCellVec bound{x.EndOfRange};
            shardRange.To = bound;
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD, TStringBuilder() << "From: " << shardRange.From.GetCells().size() << "To: " << shardRange.To.GetCells().size());
            buildInfo.AddParent(shardRange, x.ShardIdx);
            auto [it, emplaced] = buildInfo.Shards.emplace(x.ShardIdx, TIndexBuildInfo::TShardStatus{std::move(shardRange), ""});
            Y_ASSERT(emplaced);
            shardRange.From = std::move(bound);

            Self->PersistBuildIndexUploadInitiate(db, BuildId, x.ShardIdx, it->second);
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        for (auto& x: ToTabletSend) {
            Self->IndexBuildPipes.Create(BuildId, std::get<0>(x), std::move(std::get<2>(x)), ctx);
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
        LOG_I("TTxReply : TTxBilling"
              << ", buildIndexId# " << BuildIndexId);

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

        LOG_I("TTxReply : PipeRetry"
              << ", buildIndexId# " << buildId
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
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : PipeRetry"
                  << " superfluous event"
                  << ", buildIndexId# " << buildId
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

        LOG_I("TTxReply : TEvSampleKResponse, Id# " << record.GetId());

        const auto buildId = TIndexBuildId(record.GetId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvSampleKResponse"
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
                LOG_D("TTxReply : TEvSampleKResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            if (record.ProbabilitiesSize()) {
                Y_ASSERT(record.RowsSize());
                auto& probabilities = record.GetProbabilities();
                auto& rows = *record.MutableRows();
                Y_ASSERT(probabilities.size() == rows.size());
                for (int i = 0; i != probabilities.size(); ++i) {
                    if (probabilities[i] >= buildInfo.Sample.MaxProbability) {
                        break;
                    }
                    buildInfo.Sample.Rows.emplace_back(probabilities[i], std::move(rows[i]));
                }
                buildInfo.Sample.MakeWeakTop(buildInfo.KMeans.K);
            }

            TBillingStats stats{0, 0, record.GetReadRows(), record.GetReadBytes()};
            shardStatus.Processed += stats;
            buildInfo.Processed += stats;

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();
            shardStatus.Status = record.GetStatus();

            NIceDb::TNiceDb db(txc.DB);
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
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
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

        LOG_I("TTxReply : TEvLocalKMeansResponse, Id# " << record.GetId());

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
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
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

        LOG_I("TTxReply : TEvReshuffleKMeansResponse, Id# " << record.GetId());

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
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
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

        LOG_I("TTxReply : TEvUploadSampleKResponse, Id# " << record.GetId());

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
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Filling);
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
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
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

        LOG_I("TTxReply : TEvBuildIndexProgressResponse"
              << ", buildIndexId# " << record.GetBuildIndexId());

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
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
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
            Y_FAIL_S("Unreachable " << Name(state));
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
              << ", replyTo: " << buildInfo.CreateSender.ToString());
        LOG_D("Message:\n" << responseEv->Record.ShortDebugString());

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
                    << "At " << Name(state) << " state got unsuccess propose result"
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
                    << "At " << Name(state) << " state got unsuccess propose result"
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
            Y_FAIL_S("Unreachable " << Name(state));
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
            Y_FAIL_S("Unreachable " << Name(state));
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
