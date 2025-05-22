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

// This scan needed to run kmeans reshuffle which is part of global kmeans run.
class TReshuffleKMeansScanBase: public TActor<TReshuffleKMeansScanBase>, public NTable::IScan {
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

    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse> Response;

    bool IsExhausted = false;

    virtual TString Debug() const = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::RESHUFFLE_KMEANS_SCAN_ACTOR;
    }

    TReshuffleKMeansScanBase(ui64 tabletId, const TUserTable& table, TLead&& lead,
                             const NKikimrTxDataShard::TEvReshuffleKMeansRequest& request,
                             const TActorId& responseActorId,
                             TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse>&& response)
        : TActor{&TThis::StateWork}
        , Parent{request.GetParent()}
        , Child{request.GetChild()}
        , UploadState{request.GetUpload()}
        , Lead{std::move(lead)}
        , TabletId(tabletId)
        , BuildId{request.GetId()}
        , Uploader(request.GetScanSettings())
        , Dimensions(request.GetSettings().vector_dimension())
        , ScanSettings(request.GetScanSettings())
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
    {
        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        NTable::TTag embeddingTag;
        ScanTags = MakeUploadTags(table, embedding, data, EmbeddingPos, DataPos, embeddingTag);
        Lead.SetTags(ScanTags);
        OutputBuf = Uploader.AddDestination(request.GetOutputName(), MakeOutputTypes(table, UploadState, embedding, data));
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;
        Uploader.SetOwner(SelfId());

        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) final
    {
        auto& record = Response->Record;
        record.SetReadRows(ReadRows);
        record.SetReadBytes(ReadBytes);
        
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

    void Describe(IOutputStream& out) const final
    {
        out << Debug();
    }

    EScan PageFault() final
    {
        LOG_T("PageFault " << Debug());
        return EScan::Feed;
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
};

template <typename TMetric>
class TReshuffleKMeansScan final : public TReshuffleKMeansScanBase {
    TClusters<TMetric> Clusters;

    TString Debug() const
    {
        return TStringBuilder() << "TReshuffleKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " Parent: " << Parent << " Child: " << Child
            << " " << Clusters.Debug()
            << " " << Uploader.Debug();
    }

public:
    TReshuffleKMeansScan(ui64 tabletId, const TUserTable& table, TLead&& lead, NKikimrTxDataShard::TEvReshuffleKMeansRequest& request,
                         const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse>&& response)
        : TReshuffleKMeansScanBase{tabletId, table, std::move(lead), request, responseActorId, std::move(response)}
        , Clusters(TVector<TString>{request.GetClusters().begin(), request.GetClusters().end()}, request.GetSettings().vector_dimension())
    {
        LOG_I("Create " << Debug());
    }

    EScan Seek(TLead& lead, ui64 seq) final
    {
        LOG_D("Seek " << seq << " " << Debug());

        if (IsExhausted) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        lead = Lead;

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final
    {
        LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountBytes(key, row);

        Feed(key, *row);

        return Uploader.ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    }

    EScan Exhausted() final
    {
        LOG_D("Exhausted " << Debug());

        IsExhausted = true;
        
        // call Seek to wait uploads
        return EScan::Reset;
    }

private:
    void Feed(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        switch (UploadState) {
            case EState::UPLOAD_MAIN_TO_BUILD:
                FeedUploadMain2Build(key, row);
                break;
            case EState::UPLOAD_MAIN_TO_POSTING:
                FeedUploadMain2Posting(key, row);
                break;
            case EState::UPLOAD_BUILD_TO_BUILD:
                FeedUploadBuild2Build(key, row);
                break;
            case EState::UPLOAD_BUILD_TO_POSTING:
                FeedUploadBuild2Posting(key, row);
                break;
            default:
                Y_ASSERT(false);
        }
    }

    void FeedUploadMain2Build(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters.FindCluster(row, EmbeddingPos); pos) {
            AddRowMainToBuild(*OutputBuf, Child + *pos, key, row);
        }
    }

    void FeedUploadMain2Posting(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters.FindCluster(row, EmbeddingPos); pos) {
            AddRowMainToPosting(*OutputBuf, Child + *pos, key, row, DataPos);
        }
    }

    void FeedUploadBuild2Build(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters.FindCluster(row, EmbeddingPos); pos) {
            AddRowBuildToBuild(*OutputBuf, Child + *pos, key, row);
        }
    }

    void FeedUploadBuild2Posting(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters.FindCluster(row, EmbeddingPos); pos) {
            AddRowBuildToPosting(*OutputBuf, Child + *pos, key, row, DataPos);
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

    auto response = MakeHolder<TEvDataShard::TEvReshuffleKMeansResponse>();
    response->Record.SetId(id);
    response->Record.SetTabletId(TabletID());
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);

    LOG_N("Starting TReshuffleKMeansScan TabletId: " << TabletID() 
        << " " << request.ShortDebugString()
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
                << " " << request.ShortDebugString()
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

    if (request.ClustersSize() < 1) {
        badRequest("Should be requested at least single cluster");
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

    if (trySendBadRequest()) {
        return;
    }

    // 3. Validating vector index settings
    TAutoPtr<NTable::IScan> scan;
    auto createScan = [&]<typename T> {
        scan = new TReshuffleKMeansScan<T>{
            TabletID(), userTable, std::move(lead), request, ev->Sender, std::move(response),
        };
    };
    MakeScan(request, createScan, badRequest);
    if (!scan) {
        auto sent = trySendBadRequest();
        Y_ENSURE(sent);
        return;
    }

    StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
}

}
