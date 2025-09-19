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

class TBuildFulltextIndexScan: public TActor<TBuildFulltextIndexScan>, public IActorExceptionHandler, public NTable::IScan {
    IDriver* Driver = nullptr;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    TTags ScanTags;
    TString TextColumn;
    Ydb::Table::FulltextIndexSettings::Analyzers TextAnalyzers;

    TBatchRowsUploader Uploader;
    TBufferData* UploadBuf = nullptr;

    const NKikimrTxDataShard::TEvBuildFulltextIndexRequest Request;
    const TActorId ResponseActorId;
    const TAutoPtr<TEvDataShard::TEvBuildFulltextIndexResponse> Response;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::BUILD_FULLTEXT_INDEX;
    }

    TBuildFulltextIndexScan(ui64 tabletId, const TUserTable& table, NKikimrTxDataShard::TEvBuildFulltextIndexRequest request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvBuildFulltextIndexResponse>&& response)
        : TActor{&TThis::StateWork}
        , TabletId(tabletId)
        , BuildId{request.GetId()}
        , Uploader(request.GetScanSettings())
        , Request(std::move(request))
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
    {
        LOG_I("Create " << Debug());

        Y_ENSURE(Request.settings().columns().size() == 1);
        TextColumn = Request.settings().columns().at(0).column();
        TextAnalyzers = Request.settings().columns().at(0).analyzers();

        auto tags = GetAllTags(table);
        auto types = GetAllTypes(table);

        {
            ScanTags.push_back(tags.at(TextColumn));

            for (auto dataColumn : Request.GetDataColumns()) {
                if (dataColumn != TextColumn) {
                    ScanTags.push_back(tags.at(dataColumn));
                }
            }
        }

        {
            auto uploadTypes = std::make_shared<NTxProxy::TUploadTypes>();
            auto addType = [&](const auto& column) {
                auto it = types.find(column);
                if (it != types.end()) {
                    Ydb::Type type;
                    NScheme::ProtoFromTypeInfo(it->second, type);
                    uploadTypes->emplace_back(it->first, type);
                    types.erase(it);
                }
            };
            {
                Ydb::Type type;
                type.set_type_id(TokenType);
                uploadTypes->emplace_back(TokenColumn, type);
            }
            for (const auto& column : table.KeyColumnIds) {
                addType(table.Columns.at(column).Name);
            }
            for (auto dataColumn : Request.GetDataColumns()) {
                addType(dataColumn);
            }
            UploadBuf = Uploader.AddDestination(Request.GetIndexName(), std::move(uploadTypes));
        }
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;
        Uploader.SetOwner(SelfId());

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) final
    {
        LOG_T("Seek " << seq << " " << Debug());

        if (seq) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        lead.To(ScanTags, {}, NTable::ESeek::Lower);

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final
    {
        // LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        TVector<TCell> uploadKey(::Reserve(key.size() + 1));
        TVector<TCell> uploadValue(::Reserve(Request.GetDataColumns().size()));
        
        TString text((*row).at(0).AsBuf());
        auto tokens = Analyze(text, TextAnalyzers);
        for (const auto& token : tokens) {
            uploadKey.clear();
            uploadKey.push_back(TCell(token));
            uploadKey.insert(uploadKey.end(), key.begin(), key.end());
            
            uploadValue.clear();
            size_t index = 1; // skip text column
            for (auto dataColumn : Request.GetDataColumns()) {
                if (dataColumn != TextColumn) {
                    uploadValue.push_back(row.Get(index++));
                } else {
                    uploadValue.push_back(TCell(text));
                }
            }

            UploadBuf->AddRow(uploadKey, uploadValue);
        }

        return Uploader.ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    }

    EScan PageFault() final
    {
        LOG_T("PageFault " << Debug());
        return EScan::Feed;
    }

    EScan Exhausted() final
    {
        LOG_T("Exhausted " << Debug());

        // call Seek to wait uploads
        return EScan::Reset;
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
        return TStringBuilder() << "TBuildFulltextIndexScan TabletId: " << TabletId << " Id: " << BuildId
            << " " << Uploader.Debug();
    }
};

class TDataShard::TTxHandleSafeBuildFulltextIndexScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeBuildFulltextIndexScan(TDataShard* self, TEvDataShard::TEvBuildFulltextIndexRequest::TPtr&& ev)
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
    TEvDataShard::TEvBuildFulltextIndexRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvBuildFulltextIndexRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeBuildFulltextIndexScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvBuildFulltextIndexRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    TRowVersion rowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId());
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvBuildFulltextIndexResponse>();
        FillScanResponseCommonFields(*response, id, TabletID(), seqNo);

        LOG_N("Starting TBuildFulltextIndexScan TabletId: " << TabletID()
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
                LOG_E("Rejecting TBuildFulltextIndexScan bad request TabletId: " << TabletID()
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
        if (!request.HasSnapshotStep() || !request.HasSnapshotTxId()) {
            badRequest(TStringBuilder() << "Missing snapshot");
        } else {
            const TSnapshotKey snapshotKey(pathId, rowVersion.Step, rowVersion.TxId);
            if (!SnapshotManager.FindAvailable(snapshotKey)) {
                badRequest(TStringBuilder() << "Unknown snapshot for path id " << pathId.OwnerId << ":" << pathId.LocalPathId
                    << ", snapshot step is " << snapshotKey.Step << ", snapshot tx is " << snapshotKey.TxId);
            }
        }

        if (!request.GetIndexName()) {
            badRequest(TStringBuilder() << "Empty index table name");
        }

        auto tags = GetAllTags(userTable);
        for (auto column : request.GetSettings().columns()) {
            if (!tags.contains(column.column())) {
                badRequest(TStringBuilder() << "Unknown key column: " << column.column());
            }
        }
        for (auto dataColumn : request.GetDataColumns()) {
            if (!tags.contains(dataColumn)) {
                badRequest(TStringBuilder() << "Unknown data column: " << dataColumn);
            }
        }

        if (trySendBadRequest()) {
            return;
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

        // 4. Creating scan
        TAutoPtr<NTable::IScan> scan = new TBuildFulltextIndexScan(TabletID(), userTable,
            request, ev->Sender, std::move(response));

        StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvBuildFulltextIndexResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TBuildFulltextIndexScan");
    }
}

}
