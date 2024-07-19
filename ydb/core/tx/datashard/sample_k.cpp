#include "scan_common.h"
#include "datashard_impl.h"
#include "upload_stats.h"
#include "range_ops.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/kqp/common/kqp_types.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/core/ydb_convert/ydb_convert.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NDataShard {

class TSampleKScan: public TActor<TSampleKScan>, public NTable::IScan {
protected:
    const TAutoPtr<TEvDataShard::TEvSampleKResponse> Response;
    const TActorId ResponseActorId;

    const TTags ScanTags;
    const TVector<NScheme::TTypeInfo> KeyTypes;

    const TSerializedTableRange TableRange;
    const TSerializedTableRange RequestedRange;
    const ui64 K;

    IDriver* Driver = nullptr;

    struct Probability {
        ui64 p = 0;
        ui64 i = 0;

        bool operator==(const Probability&) const noexcept = default;
        auto operator<=>(const Probability&) const noexcept = default;
    };

    TReallyFastRng32 Rng;
    ui64 MaxProbability = 0;
    std::vector<Probability> MaxRows;
    std::vector<TString> DataRows;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BUILD_INDEX_SCAN_ACTOR; // TODO(mbkkt) Should be different activity type?
    }

    TSampleKScan(TAutoPtr<TEvDataShard::TEvSampleKResponse>&& response,
                 const TActorId& responseActorId,
                 const TSerializedTableRange& range,
                 ui64 k,
                 ui64 seed,
                 TProtoColumnsCRef columns,
                 const TUserTable& tableInfo)
        : TActor(&TThis::StateWork)
        , Response(std::move(response))
        , ResponseActorId(responseActorId)
        , ScanTags(BuildTags(tableInfo, columns))
        , KeyTypes(tableInfo.KeyColumnTypes)
        , TableRange(tableInfo.Range)
        , RequestedRange(range)
        , K(k)
        , Rng(seed)
    {
    }

    ~TSampleKScan() override = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept override {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Prepared " << Debug());

        Driver = driver;

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept override {
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Seek no " << seq << " " << Debug());
        if (seq) {
            SendResponse();
            return EScan::Final;
        }

        auto scanRange = Intersect(KeyTypes, RequestedRange.ToTableRange(), TableRange.ToTableRange());

        if (bool(scanRange.From)) {
            auto seek = scanRange.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            lead.To(ScanTags, scanRange.From, seek);
        } else {
            lead.To(ScanTags, {}, NTable::ESeek::Lower);
        }

        if (bool(scanRange.To)) {
            lead.Until(scanRange.To, scanRange.InclusiveTo);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final {
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Feed key " << DebugPrintPoint(KeyTypes, key, *AppData()->TypeRegistry)
                                << " " << Debug());

        const auto probability = Rng.GenRand64();
        if (DataRows.size() < K) {
            MaxRows.push_back({probability, DataRows.size()});
            DataRows.emplace_back(TSerializedCellVec::Serialize(*row));
            if (DataRows.size() == K) {
                std::make_heap(MaxRows.begin(), MaxRows.end());
                MaxProbability = MaxRows.front().p;
            }
        } else if (probability < MaxProbability) {
            SetRow(row, probability);
        }

        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override {
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());
        if (abort != EAbort::None) {
            Response->Record.SetStatus(NKikimrTxDataShard::TEvSampleKResponse::ABORTED);

            LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                       Debug());
            ctx.Send(ResponseActorId, Response.Release());
        } else {
            SendResponse();
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Finish " << Debug());
        Driver = nullptr;
        PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& out) const noexcept override {
        out << Debug();
    }

    TString Debug() const {
        return {}; // TODO(mbkkt)
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            default:
                LOG_ERROR(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                          "TSampleKScan: StateWork unexpected event type: %" PRIx32 " event: %s",
                          ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

    void SetRow(const TRow& row, ui64 p) {
        std::pop_heap(MaxRows.begin(), MaxRows.end());
        DataRows[MaxRows.back().i] = TSerializedCellVec::Serialize(*row);
        MaxRows.back().p = p;
        std::push_heap(MaxRows.begin(), MaxRows.end());
        MaxProbability = MaxRows.front().p;
    }

    void SendResponse() {
        if (!Response) {
            return;
        }
        auto ctx = TActivationContext::AsActorContext().MakeFor(SelfId());
        std::sort(MaxRows.begin(), MaxRows.end());
        for (auto& [p, i] : MaxRows) {
            Response->Record.AddProbabilities(p);
            Response->Record.AddRows(std::move(DataRows[i]));
        }
        Response->Record.SetStatus(NKikimrTxDataShard::TEvSampleKResponse::DONE);
        // TODO(mbkkt) some retry? how?
        ctx.Send(ResponseActorId, Response.Release());
    }
};

class TDataShard::TTxHandleSafeSampleKScan: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeSampleKScan(TDataShard* self, TEvDataShard::TEvSampleKRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
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
    const auto& record = ev->Get()->Record;
    TRowVersion rowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion,
                                                     std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }
    const ui64 id = record.GetId();

    auto response = MakeHolder<TEvDataShard::TEvSampleKResponse>();
    response->Record.SetId(id);
    response->Record.SetTabletId(TabletID());

    TScanRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);

    auto badRequest = [&](const TString& error) {
        response->Record.SetStatus(NKikimrTxDataShard::TEvSampleKResponse::BAD_REQUEST);
        auto issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);
        ctx.Send(ev->Sender, std::move(response));
    };

    if (const ui64 shardId = record.GetTabletId(); shardId != TabletID()) {
        badRequest(TStringBuilder() << "Wrong shard " << shardId << " this is " << TabletID());
        return;
    }

    const TTableId tableId{record.GetOwnerId(), record.GetPathId()};
    const auto* userTableIt = GetUserTables().FindPtr(tableId.PathId.LocalPathId);
    if (!userTableIt) {
        badRequest(TStringBuilder() << "Unknown table id: " << tableId.PathId.LocalPathId);
        return;
    }
    Y_ABORT_UNLESS(*userTableIt);
    const auto& userTable = **userTableIt;

    if (const auto* recCard = ScanManager.Get(id)) {
        if (recCard->SeqNo == seqNo) {
            // do no start one more scan
            return;
        }

        CancelScan(userTable.LocalTid, recCard->ScanId);
        ScanManager.Drop(id);
    }

    TSerializedTableRange requestedRange;
    requestedRange.Load(record.GetKeyRange());

    auto scanRange = Intersect(userTable.KeyColumnTypes, requestedRange.ToTableRange(), userTable.Range.ToTableRange());

    if (scanRange.IsEmptyRange(userTable.KeyColumnTypes)) {
        badRequest(TStringBuilder() << " requested range doesn't intersect with table range"
                                    << " requestedRange: " << DebugPrintRange(userTable.KeyColumnTypes, requestedRange.ToTableRange(), *AppData()->TypeRegistry)
                                    << " tableRange: " << DebugPrintRange(userTable.KeyColumnTypes, userTable.Range.ToTableRange(), *AppData()->TypeRegistry)
                                    << " scanRange: " << DebugPrintRange(userTable.KeyColumnTypes, scanRange, *AppData()->TypeRegistry));
        return;
    }

    if (!record.HasSnapshotStep() || !record.HasSnapshotTxId()) {
        badRequest(TStringBuilder() << " request doesn't have Shapshot Step or TxId");
        return;
    }

    const TSnapshotKey snapshotKey(tableId.PathId, rowVersion.Step, rowVersion.TxId);
    const TSnapshot* snapshot = SnapshotManager.FindAvailable(snapshotKey);
    if (!snapshot) {
        badRequest(TStringBuilder()
                   << "no snapshot has been found"
                   << " , path id is " << tableId.PathId.OwnerId << ":" << tableId.PathId.LocalPathId
                   << " , snapshot step is " << snapshotKey.Step
                   << " , snapshot tx is " << snapshotKey.TxId);
        return;
    }

    if (!IsStateActive()) {
        badRequest(TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
        return;
    }

    if (record.GetK() < 1) {
        badRequest(TStringBuilder() << "Should be requested at least single row");
        return;
    }

    if (record.ColumnsSize() < 1) {
        badRequest(TStringBuilder() << "Should be requested at least single column");
        return;
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10); // TODO(mbkkt) Should be different group?

    const auto scanId = QueueScan(userTable.LocalTid,
                                  new TSampleKScan(
                                      std::move(response),
                                      ev->Sender,
                                      requestedRange,
                                      record.GetK(),
                                      record.GetSeed(),
                                      record.GetColumns(),
                                      userTable),
                                  ev->Cookie,
                                  scanOpts);

    TScanRecord recCard = {scanId, seqNo};
    ScanManager.Set(id, recCard);
}

}
