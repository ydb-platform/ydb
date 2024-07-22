#include "datashard_impl.h"
#include "range_ops.h"
#include "scan_common.h"
#include "upload_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)

namespace NKikimr::NDataShard {

class TSampleKScan final: public TActor<TSampleKScan>, public NTable::IScan {
protected:
    const TAutoPtr<TEvDataShard::TEvSampleKResponse> Response;
    const TActorId ResponseActorId;

    const TTags ScanTags;
    const TVector<NScheme::TTypeInfo> KeyTypes;

    const TSerializedTableRange TableRange;
    const TSerializedTableRange RequestedRange;
    const ui64 K;

    IDriver* Driver = nullptr;

    struct TProbability {
        ui64 P = 0;
        ui64 I = 0;

        bool operator==(const TProbability&) const noexcept = default;
        auto operator<=>(const TProbability&) const noexcept = default;
    };

    TReallyFastRng32 Rng;
    ui64 MaxProbability = 0;
    std::vector<TProbability> MaxRows;
    std::vector<TString> DataRows;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SAMPLE_K_SCAN_ACTOR;
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

    ~TSampleKScan() final = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

        LOG_T("Prepare " << Debug());

        Driver = driver;

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final {
        Y_ABORT_UNLESS(seq == 0);
        LOG_T("Seek " << Debug());

        auto scanRange = Intersect(KeyTypes, RequestedRange.ToTableRange(), TableRange.ToTableRange());

        if (scanRange.From) {
            auto seek = scanRange.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            lead.To(ScanTags, scanRange.From, seek);
        } else {
            lead.To(ScanTags, {}, NTable::ESeek::Lower);
        }

        if (scanRange.To) {
            lead.Until(scanRange.To, scanRange.InclusiveTo);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final {
        LOG_T("Feed key " << DebugPrintPoint(KeyTypes, key, *AppData()->TypeRegistry) << " " << Debug());

        const auto probability = Rng.GenRand64();
        if (DataRows.size() < K) {
            MaxRows.push_back({probability, DataRows.size()});
            DataRows.emplace_back(TSerializedCellVec::Serialize(*row));
            if (DataRows.size() == K) {
                std::make_heap(MaxRows.begin(), MaxRows.end());
                MaxProbability = MaxRows.front().P;
            }
        } else if (probability < MaxProbability) {
            ReplaceRow(row, probability);
        }

        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final {
        Y_ABORT_UNLESS(Response);
        if (abort == EAbort::None) {
            FillResponse();
        } else {
            Response->Record.SetStatus(NKikimrTxDataShard::TEvSampleKResponse::ABORTED);
        }
        LOG_T("Finish " << Debug());
        TActivationContext::AsActorContext().MakeFor(SelfId()).Send(ResponseActorId, Response.Release());
        Driver = nullptr;
        PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& out) const noexcept final {
        out << Debug();
    }

    EScan Exhausted() noexcept final {
        return EScan::Final;
    }

    TString Debug() const {
        if (!Response) {
            return "empty TSampleKScan";
        }
        auto& rec = Response->Record;
        return TStringBuilder() << "TSampleKScan:"
                                << "id: " << rec.GetId()
                                << ", shard: " << rec.GetTabletId()
                                << ", generation: " << rec.GetRequestSeqNoGeneration()
                                << ", round: " << rec.GetRequestSeqNoRound();
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            default:
                LOG_E("TSampleKScan: StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString() << " " << Debug());
        }
    }

    void ReplaceRow(const TRow& row, ui64 p) {
        std::pop_heap(MaxRows.begin(), MaxRows.end());
        DataRows[MaxRows.back().I] = TSerializedCellVec::Serialize(*row);
        MaxRows.back().P = p;
        std::push_heap(MaxRows.begin(), MaxRows.end());
        MaxProbability = MaxRows.front().P;
    }

    void FillResponse() {
        std::sort(MaxRows.begin(), MaxRows.end());
        for (auto& [p, i] : MaxRows) {
            Response->Record.AddProbabilities(p);
            Response->Record.AddRows(std::move(DataRows[i]));
        }
        Response->Record.SetStatus(NKikimrTxDataShard::TEvSampleKResponse::DONE);
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

    const auto pathId = PathIdFromPathId(record.GetPathId());
    const auto* userTableIt = GetUserTables().FindPtr(pathId.LocalPathId);
    if (!userTableIt) {
        badRequest(TStringBuilder() << "Unknown table id: " << pathId.LocalPathId);
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

    const TSnapshotKey snapshotKey(pathId, rowVersion.Step, rowVersion.TxId);
    const TSnapshot* snapshot = SnapshotManager.FindAvailable(snapshotKey);
    if (!snapshot) {
        badRequest(TStringBuilder()
                   << "no snapshot has been found"
                   << " , path id is " << pathId.OwnerId << ":" << pathId.LocalPathId
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
