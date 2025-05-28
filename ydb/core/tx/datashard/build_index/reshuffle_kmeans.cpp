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

    ui32 K = 0;

    EState UploadState;

    IDriver* Driver = nullptr;

    TLead Lead;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    std::vector<TString> Clusters;

    // Upload
    std::shared_ptr<NTxProxy::TUploadTypes> TargetTypes;

    TString TargetTable;

    TBufferData ReadBuf;
    TBufferData WriteBuf;

    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;

    ui32 RetryCount = 0;

    TActorId Uploader;
    const TIndexBuildScanSettings ScanSettings;

    TTags UploadScan;

    TUploadStatus UploadStatus;

    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;

    // Response
    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse> Response;

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
        , K{static_cast<ui32>(request.ClustersSize())}
        , UploadState{request.GetUpload()}
        , Lead{std::move(lead)}
        , TabletId(tabletId)
        , BuildId{request.GetId()}
        , Clusters{request.GetClusters().begin(), request.GetClusters().end()}
        , TargetTable{request.GetPostingName()}
        , ScanSettings(request.GetScanSettings())
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
    {
        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        // scan tags
        NTable::TTag embeddingTag;
        UploadScan = MakeUploadTags(table, embedding, data, EmbeddingPos, DataPos, embeddingTag);
        // upload types
        TargetTypes = MakeUploadTypes(table, UploadState, embedding, data);
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;
        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final
    {
        LOG_D("Seek " << Debug());
        if (seq == 0) {
            lead = std::move(Lead);
            lead.SetTags(UploadScan);
            return EScan::Feed;
        }
        if (!WriteBuf.IsEmpty()) {
            return EScan::Sleep;
        }
        if (!ReadBuf.IsEmpty()) {
            ReadBuf.FlushTo(WriteBuf);
            Upload(false);
            return EScan::Sleep;
        }
        if (UploadStatus.IsNone()) {
            UploadStatus.StatusCode = Ydb::StatusIds::SUCCESS;
        }
        return EScan::Final;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final
    {
        if (Uploader) {
            Send(Uploader, new TEvents::TEvPoison);
            Uploader = {};
        }

        auto& record = Response->Record;
        record.SetReadRows(ReadRows);
        record.SetReadBytes(ReadBytes);
        record.SetUploadRows(UploadRows);
        record.SetUploadBytes(UploadBytes);
        if (abort != EAbort::None) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
        } else if (UploadStatus.IsSuccess()) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
        } else {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        }
        NYql::IssuesToMessage(UploadStatus.Issues, record.MutableIssues());

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

    TString Debug() const
    {
        return TStringBuilder() << "TReshuffleKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " Parent: " << Parent << " Child: " << Child
            << " Target: " << TargetTable << " K: " << K << " Clusters: " << Clusters.size()
            << " ReadBuf size: " << ReadBuf.Size() << " WriteBuf size: " << WriteBuf.Size();
    }

    EScan PageFault() noexcept final
    {
        LOG_T("PageFault " << Debug());

        if (!ReadBuf.IsEmpty() && WriteBuf.IsEmpty()) {
            ReadBuf.FlushTo(WriteBuf);
            Upload(false);
        }

        return EScan::Feed;
    }

protected:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("StateWork unexpected event type: " << ev->GetTypeRewrite() 
                    << " event: " << ev->ToString() << " " << Debug());
        }
    }

    void HandleWakeup()
    {
        LOG_I("Retry upload " << Debug());

        if (!WriteBuf.IsEmpty()) {
            Upload(true);
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev)
    {
        LOG_D("Handle TEvUploadRowsResponse " << Debug() << " Uploader: " << Uploader.ToString()
                                              << " ev->Sender: " << ev->Sender.ToString());

        if (Uploader) {
            Y_VERIFY_S(Uploader == ev->Sender, "Mismatch"
                << " Uploader: " << Uploader.ToString()
                << " Sender: " << ev->Sender.ToString());
        } else {
            Y_ABORT_UNLESS(Driver == nullptr);
            return;
        }

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues = ev->Get()->Issues;
        if (UploadStatus.IsSuccess()) {
            UploadRows += WriteBuf.GetRows();
            UploadBytes += WriteBuf.GetBytes();
            WriteBuf.Clear();
            if (HasReachedLimits(ReadBuf, ScanSettings)) {
                ReadBuf.FlushTo(WriteBuf);
                Upload(false);
            }

            Driver->Touch(EScan::Feed);
            return;
        }

        if (RetryCount < ScanSettings.GetMaxBatchRetries() && UploadStatus.IsRetriable()) {
            LOG_N("Got retriable error, " << Debug() << " " << UploadStatus.ToString());

            Schedule(GetRetryWakeupTimeoutBackoff(RetryCount), new TEvents::TEvWakeup);
            return;
        }

        LOG_N("Got error, abort scan, " << Debug() << " " << UploadStatus.ToString());

        Driver->Touch(EScan::Final);
    }

    EScan FeedUpload()
    {
        if (!HasReachedLimits(ReadBuf, ScanSettings)) {
            return EScan::Feed;
        }
        if (!WriteBuf.IsEmpty()) {
            return EScan::Sleep;
        }
        ReadBuf.FlushTo(WriteBuf);
        Upload(false);
        return EScan::Feed;
    }

    void Upload(bool isRetry)
    {
        if (isRetry) {
            ++RetryCount;
        } else {
            RetryCount = 0;
        }

        auto actor = NTxProxy::CreateUploadRowsInternal(
            this->SelfId(), TargetTable, TargetTypes, WriteBuf.GetRowsData(),
            NTxProxy::EUploadRowsMode::WriteToTableShadow, true /*writeToPrivateTable*/);

        Uploader = this->Register(actor);
    }
};

template <typename TMetric>
class TReshuffleKMeansScan final: public TReshuffleKMeansScanBase, private TCalculation<TMetric> {
public:
    TReshuffleKMeansScan(ui64 tabletId, const TUserTable& table, TLead&& lead, NKikimrTxDataShard::TEvReshuffleKMeansRequest& request,
                         const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse>&& response)
        : TReshuffleKMeansScanBase{tabletId, table, std::move(lead), request, responseActorId, std::move(response)}
    {
        this->Dimensions = request.GetSettings().vector_dimension();
        LOG_I("Create " << Debug());
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row_) noexcept final
    {
        LOG_T("Feed " << Debug());
        
        ++ReadRows;
        ReadBytes += CountBytes(key, row_);
        auto row = *row_;
        
        switch (UploadState) {
            case EState::UPLOAD_MAIN_TO_BUILD:
                return FeedUploadMain2Build(key, row);
            case EState::UPLOAD_MAIN_TO_POSTING:
                return FeedUploadMain2Posting(key, row);
            case EState::UPLOAD_BUILD_TO_BUILD:
                return FeedUploadBuild2Build(key, row);
            case EState::UPLOAD_BUILD_TO_POSTING:
                return FeedUploadBuild2Posting(key, row);
            default:
                return EScan::Final;
        }
    }

private:
    EScan FeedUploadMain2Build(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos >= K) {
            return EScan::Feed;
        }
        AddRowMain2Build(ReadBuf, Child + pos, key, row);
        return FeedUpload();
    }

    EScan FeedUploadMain2Posting(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos >= K) {
            return EScan::Feed;
        }
        AddRowMain2Posting(ReadBuf, Child + pos, key, row, DataPos);
        return FeedUpload();
    }

    EScan FeedUploadBuild2Build(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos >= K) {
            return EScan::Feed;
        }
        AddRowBuild2Build(ReadBuf, Child + pos, key, row);
        return FeedUpload();
    }

    EScan FeedUploadBuild2Posting(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos >= K) {
            return EScan::Feed;
        }
        AddRowBuild2Posting(ReadBuf, Child + pos, key, row, DataPos);
        return FeedUpload();
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

    TCell from, to;
    const auto range = CreateRangeFrom(userTable, request.GetParent(), from, to);
    if (range.IsEmptyRange(userTable.KeyColumnTypes)) {
        badRequest(TStringBuilder() << " requested range doesn't intersect with table range");
    }

    if (!request.HasPostingName()) {
        badRequest(TStringBuilder() << "Empty posting table name");
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
            TabletID(), userTable, CreateLeadFrom(range), request, ev->Sender, std::move(response),
        };
    };
    MakeScan(request, createScan, badRequest);
    if (!scan) {
        auto sent = trySendBadRequest();
        Y_ENSURE(sent);
        return;
    }

    if (const auto* recCard = ScanManager.Get(id)) {
        if (recCard->SeqNo == seqNo) {
            // do no start one more scan
            return;
        }

        for (auto scanId : recCard->ScanIds) {
            CancelScan(userTable.LocalTid, scanId);
        }
        ScanManager.Drop(id);
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10); // TODO(mbkkt) Should be different group?
    const auto scanId = QueueScan(userTable.LocalTid, std::move(scan), 0, scanOpts);
    ScanManager.Set(id, seqNo).push_back(scanId);
}

}
