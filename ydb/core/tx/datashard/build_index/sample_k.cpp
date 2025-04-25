#include "kmeans_helper.h"
#include "../datashard_impl.h"
#include "../range_ops.h"
#include "../scan_common.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {
using namespace NKMeans;

class TSampleKScan final: public TActor<TSampleKScan>, public NTable::IScan {
protected:
    const TTags ScanTags;
    const TVector<NScheme::TTypeInfo> KeyTypes;

    const TSerializedTableRange TableRange;
    const TSerializedTableRange RequestedRange;
    const ui64 K;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    TSampler Sampler;

    IDriver* Driver = nullptr;

    TLead Lead;

    const TAutoPtr<TEvDataShard::TEvSampleKResponse> Response;
    const TActorId ResponseActorId;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SAMPLE_K_SCAN_ACTOR;
    }

    TSampleKScan(ui64 tabletId, const TUserTable& table, NKikimrTxDataShard::TEvSampleKRequest& request,
                 const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvSampleKResponse>&& response,
                 const TSerializedTableRange& range)
        : TActor(&TThis::StateWork)
        , ScanTags(BuildTags(table, request.GetColumns()))
        , KeyTypes(table.KeyColumnTypes)
        , TableRange(table.Range)
        , RequestedRange(range)
        , K(request.GetK())
        , TabletId(tabletId)
        , BuildId(request.GetId())
        , Sampler(request.GetK(), request.GetSeed(), request.GetMaxProbability())
        , Response(std::move(response))
        , ResponseActorId(responseActorId)
    {
        LOG_I("Create " << Debug());

        auto scanRange = Intersect(KeyTypes, RequestedRange.ToTableRange(), TableRange.ToTableRange());

        if (scanRange.From) {
            auto seek = scanRange.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            Lead.To(ScanTags, scanRange.From, seek);
        } else {
            Lead.To(ScanTags, {}, NTable::ESeek::Lower);
        }

        if (scanRange.To) {
            Lead.Until(scanRange.To, scanRange.InclusiveTo);
        }
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) final {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

        LOG_I("Prepare " << Debug());

        Driver = driver;

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) final {
        LOG_D("Seek " << seq << " " << Debug());

        lead = Lead;

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final {
        LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountBytes(key, row);

        Sampler.Add([&row](){
            return TSerializedCellVec::Serialize(*row);
        });

        if (Sampler.GetMaxProbability() == 0) {
            return EScan::Final;
        }

        return EScan::Feed;
    }

    EScan Exhausted() final
    {
        LOG_D("Exhausted " << Debug());

        return EScan::Final;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) final {
        auto& record = Response->Record;
        record.SetReadRows(ReadRows);
        record.SetReadBytes(ReadBytes);

        if (abort != NTable::EAbort::None) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
        } else {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
            FillResponse();
        }

        if (Response->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            LOG_N("Done " << Debug() << " " << Response->Record.ShortDebugString());
        } else {
            LOG_E("Failed " << Debug() << " " << Response->Record.ShortDebugString());
        }
        Send(ResponseActorId, Response.Release());

        Driver = nullptr;
        PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& out) const final {
        out << Debug();
    }

    TString Debug() const {
        return TStringBuilder() << "TSampleKScan TabletId: " << TabletId << " Id: " << BuildId
            << " K: " << K
            << " " << Sampler.Debug();
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            default:
                LOG_E("StateWork unexpected event type: " << ev->GetTypeRewrite() 
                    << " event: " << ev->ToString() << " " << Debug());
        }
    }

    void FillResponse() {
        auto [maxRows, dataRows] = Sampler.Finish();

        std::sort(maxRows.begin(), maxRows.end());
        auto& record = Response->Record;
        for (auto& [p, i] : maxRows) {
            record.AddProbabilities(p);
            record.AddRows(std::move(dataRows[i]));
        }
        record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
    }
};

class TDataShard::TTxHandleSafeSampleKScan: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeSampleKScan(TDataShard* self, TEvDataShard::TEvSampleKRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev)) {
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) {
        // nothing
    }

private:
    TEvDataShard::TEvSampleKRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvSampleKRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeSampleKScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvSampleKRequest::TPtr& ev, const TActorContext& ctx) {
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    auto rowVersion = request.HasSnapshotStep() || request.HasSnapshotTxId()
        ? TRowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId())
        : GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    auto response = MakeHolder<TEvDataShard::TEvSampleKResponse>();
    response->Record.SetId(id);
    response->Record.SetTabletId(TabletID());
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);

    LOG_N("Starting TSampleKScan TabletId: " << TabletID()
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
            LOG_E("Rejecting TSampleKScan bad request TabletId: " << TabletID()
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

    if (request.GetK() < 1) {
        badRequest("Should be requested on at least one row");
    }

    if (request.GetMaxProbability() <= 0) {
        badRequest("Max probability should be positive");
    }

    TSerializedTableRange requestedRange;
    requestedRange.Load(request.GetKeyRange());
    auto scanRange = Intersect(userTable.KeyColumnTypes, requestedRange.ToTableRange(), userTable.Range.ToTableRange());
    if (scanRange.IsEmptyRange(userTable.KeyColumnTypes)) {
        badRequest(TStringBuilder() << " requested range doesn't intersect with table range"
            << " requestedRange: " << DebugPrintRange(userTable.KeyColumnTypes, requestedRange.ToTableRange(), *AppData()->TypeRegistry)
            << " tableRange: " << DebugPrintRange(userTable.KeyColumnTypes, userTable.Range.ToTableRange(), *AppData()->TypeRegistry)
            << " scanRange: " << DebugPrintRange(userTable.KeyColumnTypes, scanRange, *AppData()->TypeRegistry));
    }

    if (request.ColumnsSize() < 1) {
        badRequest("Should be requested at least one column");
    }
    auto tags = GetAllTags(userTable);
    for (auto column : request.GetColumns()) {
        if (!tags.contains(column)) {
            badRequest(TStringBuilder() << "Unknown column: " << column);
        }
    }

    if (trySendBadRequest()) {
        return;
    }

    // 3. Creating scan
    TAutoPtr<NTable::IScan> scan = new TSampleKScan(TabletID(), userTable,
        request, ev->Sender, std::move(response),
        requestedRange);

    StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
}

}
