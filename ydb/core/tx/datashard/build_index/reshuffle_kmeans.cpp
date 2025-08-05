#include "kmeans_helper.h"
#include "../datashard_impl.h"
#include "../scan_common.h"
#include "../upload_stats.h"
#include "../buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {
using namespace NKMeans;

/*
 * TReshuffleKMeansScan performs a post-processing step in a distributed K-means pipeline.
 * It reassigns data points to given clusters and uploads the reshuffled results.
 * It scans either a MAIN or BUILD table shard, while the output rows go to the BUILD or POSTING table.
 *
 * Request:
 * - The client sends TEvReshuffleKMeansRequest with:
 *   - Parent: ID of the scanned cluster
 *     - If Parent=0, the entire table shard is scanned
 *   - Child, serving as the base ID for the new cluster IDs computed in this local stage
 *   - Clusters: list of centroids to which input rows will be reassigned
 *   - Upload mode (MAIN_TO_BUILD, MAIN_TO_POSTING, BUILD_TO_BUILD or BUILD_TO_POSTING)
 *     determining input and output layouts
 *   - The embedding column name and additional data columns to be used for K-means
 *   - Name of the target table for row results ("posting" or "build")
 *
 * Execution Flow:
 * - TReshuffleKMeansScan scans the relevant input shard range
 * - For each input row:
 *   - The closest cluster (from the provided centroids) is determined
 *   - The row is annotated with a new Child+cluster index ID
 *   - The row and any specified data columns are written to the output table
 */

class TReshuffleKMeansScan: public TActor<TReshuffleKMeansScan>, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::EKMeansState;

    NTableIndex::TClusterId Parent = 0;
    NTableIndex::TClusterId Child = 0;

    EState UploadState;

    IDriver* Driver = nullptr;

    TLead Lead;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    TBatchRowsUploader Uploader;

    TBufferData* OutputBuf = nullptr;

    const ui32 Dimensions = 0;
    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;

    ui32 RetryCount = 0;

    const TIndexBuildScanSettings ScanSettings;

    TTags ScanTags;

    TUploadStatus UploadStatus;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse> Response;

    bool IsExhausted = false;

    std::unique_ptr<IClusters> Clusters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::RESHUFFLE_KMEANS_SCAN_ACTOR;
    }

    TReshuffleKMeansScan(ui64 tabletId, const TUserTable& table, TLead&& lead,
        const NKikimrTxDataShard::TEvReshuffleKMeansRequest& request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse>&& response,
        std::unique_ptr<IClusters>&& clusters)
        : TActor(&TThis::StateWork)
        , Parent(request.GetParent())
        , Child(request.GetChild())
        , UploadState(request.GetUpload())
        , Lead(std::move(lead))
        , TabletId(tabletId)
        , BuildId(request.GetId())
        , Uploader(request.GetScanSettings())
        , Dimensions(request.GetSettings().vector_dimension())
        , ScanSettings(request.GetScanSettings())
        , ResponseActorId(responseActorId)
        , Response(std::move(response))
        , Clusters(std::move(clusters))
    {
        LOG_I("Create " << Debug());

        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        NTable::TTag embeddingTag;
        ScanTags = MakeScanTags(table, embedding, data, EmbeddingPos, DataPos, embeddingTag);
        Lead.SetTags(ScanTags);
        OutputBuf = Uploader.AddDestination(request.GetOutputName(), MakeOutputTypes(table, UploadState, embedding, data));
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;
        Uploader.SetOwner(SelfId());

        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final
    {
        auto& record = Response->Record;
        record.MutableMeteringStats()->SetReadRows(ReadRows);
        record.MutableMeteringStats()->SetReadBytes(ReadBytes);
        record.MutableMeteringStats()->SetCpuTimeUs(Driver->GetTotalCpuTimeUs());

        Uploader.Finish(record, abort);

        if (Response->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            LOG_N("Done " << Debug() << " " << Response->Record.ShortDebugString());
        } else {
            LOG_E("Failed " << Debug() << " " << Response->Record.ShortDebugString());
        }
        Send(ResponseActorId, Response.Release());

        Driver = nullptr;
        this->PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& out) const noexcept final
    {
        out << Debug();
    }

    EScan PageFault() noexcept final
    {
        LOG_T("PageFault " << Debug());
        return EScan::Feed;
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final
    {
    try {
        LOG_T("Seek " << seq << " " << Debug());

        if (IsExhausted) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        lead = Lead;

        return EScan::Feed;
    } catch (const std::exception& exc) {
        Uploader.AddIssue(exc);
        return EScan::Final;
    }
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final
    {
    try {
        // LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        Feed(key, *row);

        return Uploader.ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    } catch (const std::exception& exc) {
        Uploader.AddIssue(exc);
        return EScan::Final;
    }
    }

    EScan Exhausted() noexcept final
    {
    try {
        LOG_T("Exhausted " << Debug());

        IsExhausted = true;

        // call Seek to wait uploads
        return EScan::Reset;
    } catch (const std::exception& exc) {
        Uploader.AddIssue(exc);
        return EScan::Final;
    }
    }

protected:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("StateWork unexpected event type: " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString() << " " << Debug());
        }
    }

    void HandleWakeup(const NActors::TActorContext& /*ctx*/)
    {
        LOG_D("Retry upload " << Debug());

        Uploader.RetryUpload();
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx)
    {
        LOG_D("Handle TEvUploadRowsResponse " << Debug()
            << " ev->Sender: " << ev->Sender.ToString());

        if (!Driver) {
            return;
        }

        Uploader.Handle(ev);

        if (Uploader.GetUploadStatus().IsSuccess()) {
            Driver->Touch(EScan::Feed);
            return;
        }

        if (auto retryAfter = Uploader.GetRetryAfter(); retryAfter) {
            LOG_N("Got retriable error, " << Debug() << " " << Uploader.GetUploadStatus().ToString());
            ctx.Schedule(*retryAfter, new TEvents::TEvWakeup());
            return;
        }

        LOG_N("Got error, abort scan, " << Debug() << " " << Uploader.GetUploadStatus().ToString());

        Driver->Touch(EScan::Final);
    }

    TString Debug() const
    {
        return TStringBuilder() << "TReshuffleKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " Parent: " << Parent << " Child: " << Child
            << " " << Clusters->Debug()
            << " " << Uploader.Debug();
    }

    void Feed(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        switch (UploadState) {
            case EState::UPLOAD_MAIN_TO_BUILD:
                FeedMainToBuild(key, row);
                break;
            case EState::UPLOAD_MAIN_TO_POSTING:
                FeedMainToPosting(key, row);
                break;
            case EState::UPLOAD_BUILD_TO_BUILD:
                FeedBuildToBuild(key, row);
                break;
            case EState::UPLOAD_BUILD_TO_POSTING:
                FeedBuildToPosting(key, row);
                break;
            default:
                Y_ENSURE(false);
        }
    }

    void FeedMainToBuild(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, key, row, key, false);
        }
    }

    void FeedMainToPosting(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, key, row.Slice(DataPos), key, true);
        }
    }

    void FeedBuildToBuild(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, key.Slice(1), row, key, false);
        }
    }

    void FeedBuildToPosting(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, key.Slice(1), row.Slice(DataPos), key, true);
        }
    }
};

class TDataShard::TTxHandleSafeReshuffleKMeansScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeReshuffleKMeansScan(TDataShard* self, TEvDataShard::TEvReshuffleKMeansRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) final
    {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) final
    {
    }

private:
    TEvDataShard::TEvReshuffleKMeansRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvReshuffleKMeansRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeReshuffleKMeansScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvReshuffleKMeansRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    auto rowVersion = request.HasSnapshotStep() || request.HasSnapshotTxId()
        ? TRowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId())
        : GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvReshuffleKMeansResponse>();
        response->Record.SetId(id);
        response->Record.SetTabletId(TabletID());
        response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
        response->Record.SetRequestSeqNoRound(seqNo.Round);

        LOG_N("Starting TReshuffleKMeansScan TabletId: " << TabletID()
            << " " << ToShortDebugString(request)
            << " row version " << rowVersion);

        // Note: it's very unlikely that we have volatile txs before this snapshot
        if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
            VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
            return;
        }

        auto badRequest = [&](const TString& error) {
            response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
            auto issue = response->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(error);
        };
        auto trySendBadRequest = [&] {
            if (response->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST) {
                LOG_E("Rejecting TReshuffleKMeansScan bad request TabletId: " << TabletID()
                    << " " << ToShortDebugString(request)
                    << " with response " << response->Record.ShortDebugString());
                ctx.Send(ev->Sender, std::move(response));
                return true;
            } else {
                return false;
            }
        };

        // 1. Validating table and path existence
        if (request.GetTabletId() != TabletID()) {
            badRequest(TStringBuilder() << "Wrong shard " << request.GetTabletId() << " this is " << TabletID());
        }
        if (!IsStateActive()) {
            badRequest(TStringBuilder() << "Shard " << TabletID() << " is " << State << " and not ready for requests");
        }
        const auto pathId = TPathId::FromProto(request.GetPathId());
        const auto* userTableIt = GetUserTables().FindPtr(pathId.LocalPathId);
        if (!userTableIt) {
            badRequest(TStringBuilder() << "Unknown table id: " << pathId.LocalPathId);
        }
        if (trySendBadRequest()) {
            return;
        }
        const auto& userTable = **userTableIt;

        // 2. Validating request fields
        if (request.HasSnapshotStep() || request.HasSnapshotTxId()) {
            const TSnapshotKey snapshotKey(pathId, rowVersion.Step, rowVersion.TxId);
            if (!SnapshotManager.FindAvailable(snapshotKey)) {
                badRequest(TStringBuilder() << "Unknown snapshot for path id " << pathId.OwnerId << ":" << pathId.LocalPathId
                    << ", snapshot step is " << snapshotKey.Step << ", snapshot tx is " << snapshotKey.TxId);
            }
        }

        if (request.GetUpload() != NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD
            && request.GetUpload() != NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING
            && request.GetUpload() != NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD
            && request.GetUpload() != NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING)
        {
            badRequest("Wrong upload");
        }

        const auto parent = request.GetParent();
        NTable::TLead lead;
        if (parent == 0) {
            if (request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD
                || request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING)
            {
                badRequest("Wrong upload for zero parent");
            }
            lead.To({}, NTable::ESeek::Lower);
        } else if (request.GetUpload() == NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD
            || request.GetUpload() == NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING)
        {
            badRequest("Wrong upload for non-zero parent");
        } else {
            TCell from, to;
            const auto range = CreateRangeFrom(userTable, request.GetParent(), from, to);
            if (range.IsEmptyRange(userTable.KeyColumnTypes)) {
                badRequest(TStringBuilder() << " requested range doesn't intersect with table range");
            }
            lead = CreateLeadFrom(range);
        }

        if (!request.HasOutputName()) {
            badRequest(TStringBuilder() << "Empty output table name");
        }

        auto tags = GetAllTags(userTable);
        if (!tags.contains(request.GetEmbeddingColumn())) {
            badRequest(TStringBuilder() << "Unknown embedding column: " << request.GetEmbeddingColumn());
        }
        for (auto dataColumn : request.GetDataColumns()) {
            if (!tags.contains(dataColumn)) {
                badRequest(TStringBuilder() << "Unknown data column: " << dataColumn);
            }
        }

        // 3. Validating vector index settings
        TString error;
        auto clusters = NKikimr::NKMeans::CreateClusters(request.GetSettings(), 0, error);
        if (!clusters) {
            badRequest(error);
        } else if (request.ClustersSize() < 1) {
            badRequest("Should be requested for at least one cluster");
        } else if (!clusters->SetClusters(TVector<TString>{request.GetClusters().begin(), request.GetClusters().end()})) {
            badRequest("Clusters have invalid format");
        }

        if (trySendBadRequest()) {
            return;
        }

        TAutoPtr<NTable::IScan> scan = new TReshuffleKMeansScan(
            TabletID(), userTable, std::move(lead), request, ev->Sender, std::move(response), std::move(clusters)
        );

        StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvReshuffleKMeansResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TReshuffleKMeansScan");
    }
}

}
