#include "incr_restore_scan.h"
#include "change_exchange_impl.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/tx/datashard/change_record_body_serializer.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/tx/datashard/change_collector.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/datashard/stream_scan_common.h>
#include <ydb/core/tx/datashard/incr_restore_helpers.h>

namespace NKikimr::NDataShard {

using namespace NActors;
using namespace NTable;

class TIncrementalRestoreScan
    : public IActorCallback
    , public NTable::IScan
    , protected TChangeRecordBodySerializer
{
    using TLimits = NStreamScan::TLimits;
    using TBuffer = NStreamScan::TBuffer;
    using TChange = IDataShardChangeCollector::TChange;

public:
    explicit TIncrementalRestoreScan(
        std::function<IActor*()> changeSenderFactory,
        ui64 txId,
        const TPathId& tablePathId,
        const TPathId& targetPathId)
        : IActorCallback(static_cast<TReceiveFunc>(&TIncrementalRestoreScan::StateWork), NKikimrServices::TActivity::CDC_STREAM_SCAN_ACTOR)
        , ChangeSenderFactory(changeSenderFactory)
        // , DataShard{self->SelfId(), self->TabletID()}
        , TxId(txId)
        , TablePathId(tablePathId)
        , TargetPathId(targetPathId)
        , ReadVersion({})
        , Limits({})
        // , ValueTags(InitValueTags(self, tablePathId))
    {}

    void Registered(TActorSystem*, const TActorId&) override {
        ChangeSender = RegisterWithSameMailbox(ChangeSenderFactory());
    }

    void PassAway() override {
        Send(ChangeSender, new TEvents::TEvPoisonPill());

        IActorCallback::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            // hFunc(TEvDataShard::TEvCdcStreamScanRequest, Handle);
            // hFunc(TDataShard::TEvPrivate::TEvCdcStreamScanContinue, Handle);
            // hFunc(TEvents::TEvWakeup, Start);
            // hFunc(NChangeExchange::TEvChangeExchange::TEvRequestRecords, Handle);
            // IgnoreFunc(NChangeExchange::TEvChangeExchange::TEvRemoveRecords);
            // hFunc(TEvChangeExchange::TEvAllSent, Handle);
            // IgnoreFunc(TDataShard::TEvPrivate::TEvConfirmReadonlyLease);
            default: Y_ABORT("unexpected event Type# 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    IScan::TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) noexcept override {
        TlsActivationContext->AsActorContext().RegisterWithSameMailbox(this);
        Driver = driver;
        Y_ABORT_UNLESS(!LastKey || LastKey->GetCells().size() == scheme->Tags(true).size());

        return {EScan::Sleep, {}};
    }

    EScan Seek(TLead& lead, ui64) noexcept override {
        if (LastKey) {
            lead.To(ValueTags, LastKey->GetCells(), ESeek::Upper);
        } else {
            lead.To(ValueTags, {}, ESeek::Lower);
        }

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept override {
        Buffer.AddRow(key, *row);
        if (Buffer.Bytes() < Limits.BatchMaxBytes) {
            if (Buffer.Rows() < Limits.BatchMaxRows) {
                return EScan::Feed;
            }
        } else {
            if (Buffer.Rows() < Limits.BatchMinRows) {
                return EScan::Feed;
            }
        }

        Progress();
        return EScan::Sleep;
    }

    EScan Exhausted() noexcept override {
        NoMoreData = true;

        if (!Buffer) {
            return EScan::Sleep;
        }

        return Progress();
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override {
        // Send(DataShard.ActorId, new TEvDataShard::TEvRestoreFinished{TxId});

        if (abort != EAbort::None) {
            // Reply(NKikimrTxDataShard::TEvCdcStreamScanResponse::ABORTED);
        } else {
            // Reply(NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE);
        }

        PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& o) const noexcept override {
        o << "IncrRestoreScan {"
          << " TxId: " << TxId
          << " TablePathId: " << TablePathId
          << " TargetPathId: " << TargetPathId
        << " }";
    }

    EScan Progress() {
        // Stats.RowsProcessed += Buffer.Rows();
        // Stats.BytesProcessed += Buffer.Bytes();

        // auto& ctx = TlsActivationContext->AsActorContext();
        // auto TabletID = [&]() { return DataShard.TabletId; };
        // LOG_D("IncrRestore@Progress()"
            // << ": Buffer.Rows()# " << Buffer.Rows());

        // auto reservationCookie = Self->ReserveChangeQueueCapacity(Buffer.Rows());
        auto rows = Buffer.Flush();
        TVector<TChange> changeRecords;
        TVector<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo> records;

        // auto table = Self->GetUserTables().at(TablePathId.LocalPathId);
        NDataShard::TUserTable::TCPtr table;
        for (auto& [k, v] : rows) {
            // LOG_D("IncrRestore@Progress()#iter"
                // << ": k.GetCells().size()# " << k.GetCells().size() << ", v.GetCells().size()# " << v.GetCells().size());
            const auto key = NStreamScan::MakeKey(k.GetCells(), table);
            const auto& keyTags = table->KeyColumnIds;
            NKikimrChangeExchange::TDataChange body;
            if (auto updates = NIncrRestoreHelpers::MakeRestoreUpdates(v.GetCells(), ValueTags, table); updates) {
                Serialize(body, ERowOp::Upsert, key, keyTags, *updates);
            } else {
                Serialize(body, ERowOp::Erase, key, keyTags, {});
            }
            auto recordPtr = TChangeRecordBuilder(TChangeRecord::EKind::AsyncIndex)
                .WithOrder(++Order)
                .WithGroup(0)
                .WithStep(ReadVersion.Step)
                .WithTxId(ReadVersion.TxId)
                .WithPathId(TargetPathId)
                .WithTableId(TablePathId)
                .WithSchemaVersion(table->GetTableSchemaVersion())
                .WithBody(body.SerializeAsString())
                .WithSource(TChangeRecord::ESource::InitialScan)
                .Build();

            const auto& record = *recordPtr;

            records.emplace_back(record.GetOrder(), record.GetPathId(), record.GetBody().size());
            PendingRecords.emplace(record.GetOrder(), recordPtr);
        }

        Send(ChangeSender, new NChangeExchange::TEvChangeExchange::TEvEnqueueRecords(records));

        if (NoMoreData) {
            // Send(ChangeSender, new TEvChangeExchange::TEvNoMoreData());
        }

        return NoMoreData ? EScan::Sleep : EScan::Feed;
    }
private:
    // const TDataShardId DataShard;
    std::function<IActor*()> ChangeSenderFactory;
    TActorId ReplyTo;
    const ui64 TxId;
    const TPathId TablePathId;
    const TPathId TargetPathId;
    const TRowVersion ReadVersion;
    const TVector<TTag> ValueTags;
    const TMaybe<TSerializedCellVec> LastKey;
    const TLimits Limits;
    IDriver* Driver;
    bool NoMoreData;
    TBuffer Buffer;
    // TStats Stats;
    // TDataShard* Self;
    ui64 Order = 0;
    TActorId ChangeSender;
    TMap<ui64, TChangeRecord::TPtr> PendingRecords;
};

THolder<NTable::IScan> CreateIncrementalRestoreScan(
        std::function<IActor*()> changeSenderFactory,
        TPathId tablePathId,
        const TPathId& targetPathId,
        ui64 txId)
{
     return MakeHolder<TIncrementalRestoreScan>(
        changeSenderFactory,
        txId,
        tablePathId,
        targetPathId);
}

} // namespace NKikimr::NDataShard
