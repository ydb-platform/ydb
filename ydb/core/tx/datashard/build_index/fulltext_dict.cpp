#include "common_helper.h"
#include "../datashard_impl.h"
#include "../scan_common.h"
#include "../upload_stats.h"
#include "../buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/fulltext.h>
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
using namespace NTableIndex::NFulltext;
using namespace NKikimr::NFulltext;

/*
 * TBuildFulltextDictScan aggregates rows from the fulltext posting table and calculates
 * token frequencies, i.e. the number of documents containing each token.
 *
 * This scan takes the indexImplTable and writes output to indexImplTokensTable.
 *
 * Source columns: __ydb_token, <PK columns>, __ydb_freq
 * Destination columns: __ydb_token, __ydb_freq
 *
 * Request:
 * - The client sends TEvBuildFulltextDictRequest with:
 *   - Name of the target table
 *   - SkipFirstToken, SkipLastToken
 *
 * Execution Flow:
 * - TBuildFulltextDictScan scans the whole input shard
 * - If SkipFirstToken is specified in the request:
 *   - The first __ydb_token and the number of rows with it are copied to the response protobuf.
 * - If SkipLastToken is specified in the request:
 *   - The last __ydb_token and the number of rows with it are copied to the response protobuf.
 * - For all other input rows, namely, for every token:
 *   - The number of matching rows in the source table is calculated.
 *   - The token is inserted into TokensTable with along with the calculated number.
 */

class TBuildFulltextDictScan: public TActor<TBuildFulltextDictScan>, public IActorExceptionHandler, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::EKMeansState;

    EState UploadState;

    IDriver* Driver = nullptr;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    TBatchRowsUploader Uploader;

    TBufferData* OutputBuf = nullptr;

    bool SkipFirstToken = false;
    bool SkipLastToken = false;
    TString FirstToken;
    TString LastToken;
    ui64 FirstTokenRows = 0;
    ui64 LastTokenRows = 0;

    ui32 RetryCount = 0;

    const TIndexBuildScanSettings ScanSettings;

    TUploadStatus UploadStatus;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvBuildFulltextDictResponse> Response;

    bool IsExhausted = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::BUILD_FULLTEXT_DICTIONARY;
    }

    TBuildFulltextDictScan(ui64 tabletId, const TUserTable& table,
        const NKikimrTxDataShard::TEvBuildFulltextDictRequest& request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvBuildFulltextDictResponse>&& response)
        : TActor(&TThis::StateWork)
        , TabletId(tabletId)
        , BuildId(request.GetId())
        , Uploader(request.GetDatabaseName(), request.GetScanSettings())
        , SkipFirstToken(request.GetSkipFirstToken())
        , SkipLastToken(request.GetSkipLastToken())
        , ScanSettings(request.GetScanSettings())
        , ResponseActorId(responseActorId)
        , Response(std::move(response))
    {
        LOG_I("Create " << Debug());

        auto types = GetAllTypes(table);

        auto uploadTypes = std::make_shared<NTxProxy::TUploadTypes>();
        {
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(types.at(TokenColumn), type);
            uploadTypes->emplace_back(TokenColumn, type);
        }
        {
            Ydb::Type type;
            type.set_type_id(DocCountType);
            uploadTypes->emplace_back(FreqColumn, type);
        }

        OutputBuf = Uploader.AddDestination(request.GetOutputName(), uploadTypes);
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;
        Uploader.SetOwner(SelfId());

        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(const std::exception& exc) final
    {
        Uploader.AddIssue(exc);
        return Finish(EStatus::Exception);
    }

    TAutoPtr<IDestructable> Finish(EStatus status) final
    {
        auto& record = Response->Record;
        record.MutableMeteringStats()->SetReadRows(ReadRows);
        record.MutableMeteringStats()->SetReadBytes(ReadBytes);
        record.MutableMeteringStats()->SetCpuTimeUs(Driver->GetTotalCpuTimeUs());

        record.SetFirstToken(FirstToken);
        record.SetLastToken(LastToken);
        record.SetFirstTokenRows(FirstTokenRows);
        record.SetLastTokenRows(LastTokenRows);

        Uploader.Finish(record, status);

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

    bool OnUnhandledException(const std::exception& exc) final
    {
        if (!Driver) {
            return false;
        }
        Driver->Throw(exc);
        return true;
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

    EScan Seek(TLead& lead, ui64 seq) final
    {
        LOG_T("Seek " << seq << " " << Debug());

        if (IsExhausted) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        lead.To({}, NTable::ESeek::Lower);

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final
    {
        // LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        Feed(key, *row);

        return Uploader.ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    }

    EScan Exhausted() final
    {
        LOG_T("Exhausted " << Debug());

        IsExhausted = true;
        if (LastToken) {
            FinishToken(true);
        }

        // call Seek to wait uploads
        return EScan::Reset;
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
        return TStringBuilder() << "TBuildFulltextDictScan TabletId: " << TabletId << " Id: " << BuildId
            << " SkipFirstToken: " << SkipFirstToken << " SkipLastToken: " << SkipLastToken
            << " " << Uploader.Debug();
    }

    void Feed(TArrayRef<const TCell> key, TArrayRef<const TCell>)
    {
        auto token = key.at(0).AsBuf();
        if (SkipFirstToken) {
            if (!FirstToken) {
                FirstToken = TString(token);
                FirstTokenRows++;
                return;
            }
            if (FirstToken == token) {
                FirstTokenRows++;
                return;
            } else {
                // first token is skipped
                SkipFirstToken = false;
            }
        }
        if (LastToken && LastToken != token) {
            FinishToken(false);
        }
        if (!LastToken) {
            LastToken = TString(token);
        }
        LastTokenRows++;
    }

    void FinishToken(bool last)
    {
        if (last && SkipLastToken) {
            return;
        }
        TVector<TCell> pk = {TCell(LastToken)};
        TVector<TCell> freq = {TCell::Make(LastTokenRows)};
        OutputBuf->AddRow(pk, freq, pk);
        LastToken.clear();
        LastTokenRows = 0;
    }
};

class TDataShard::TTxHandleSafeBuildFulltextDictScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeBuildFulltextDictScan(TDataShard* self, TEvDataShard::TEvBuildFulltextDictRequest::TPtr&& ev)
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
    TEvDataShard::TEvBuildFulltextDictRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvBuildFulltextDictRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeBuildFulltextDictScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvBuildFulltextDictRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    auto rowVersion = request.HasSnapshotStep() || request.HasSnapshotTxId()
        ? TRowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId())
        : GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvBuildFulltextDictResponse>();
        FillScanResponseCommonFields(*response, id, TabletID(), seqNo);

        LOG_N("Starting TBuildFulltextDictScan TabletId: " << TabletID()
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
                LOG_E("Rejecting TBuildFulltextDictScan bad request TabletId: " << TabletID()
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

        if (!request.GetOutputName()) {
            badRequest(TStringBuilder() << "Empty output table name");
        }

        // 3. Validating fulltext index settings
        if (!request.HasSettings()) {
            badRequest(TStringBuilder() << "Missing fulltext index settings");
        } else {
            TString error;
            if (!NKikimr::NFulltext::ValidateSettings(request.GetSettings(), error)) {
                badRequest(error);
            }
        }

        if (trySendBadRequest()) {
            return;
        }

        TAutoPtr<NTable::IScan> scan = new TBuildFulltextDictScan(
            TabletID(), userTable, request, ev->Sender, std::move(response)
        );

        StartScan(this, std::move(scan), id, seqNo, rowVersion, request.GetReadShadowData() ? userTable.ShadowTid : userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvBuildFulltextDictResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TBuildFulltextDictScan");
    }
}

}
