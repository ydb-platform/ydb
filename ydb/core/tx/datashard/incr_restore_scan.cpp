#include "incr_restore_scan.h"
#include "change_exchange_impl.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/tx/datashard/change_record_body_serializer.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/change_record.h>
#include <ydb/core/change_exchange/change_exchange.h>

namespace NKikimr::NDataShard {

using namespace NActors;
using namespace NTable;

class TIncrementalRestoreScan
    : public IActorCallback
    , public NTable::IScan
    , protected TChangeRecordBodySerializer
{
    struct TLimits {
        ui32 BatchMaxBytes;
        ui32 BatchMinRows;
        ui32 BatchMaxRows;

        // TLimits(const NKikimrTxDataShard::TEvCdcStreamScanRequest::TLimits& proto)
        //     : BatchMaxBytes(proto.GetBatchMaxBytes())
        //     , BatchMinRows(proto.GetBatchMinRows())
        //     , BatchMaxRows(proto.GetBatchMaxRows())
        // {
        // }
    };

    class TBuffer {
    public:
        void AddRow(TArrayRef<const TCell> key, TArrayRef<const TCell> value) {
            const auto& [k, v] = Data.emplace_back(
                TSerializedCellVec(key),
                TSerializedCellVec(value)
            );
            ByteSize += k.GetBuffer().size() + v.GetBuffer().size();
        }

        auto&& Flush() {
            ByteSize = 0;
            return std::move(Data);
        }

        ui64 Bytes() const {
            return ByteSize;
        }

        ui64 Rows() const {
            return Data.size();
        }

        explicit operator bool() const {
            return !Data.empty();
        }

    private:
        TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> Data; // key & value (if any)
        ui64 ByteSize = 0;
    };

    struct TChange {
        ui64 Order;
        ui64 Group;
        ui64 Step;
        ui64 TxId;
        TPathId PathId;
        ui64 BodySize;
        TPathId TableId;
        ui64 SchemaVersion;
        ui64 LockId = 0;
        ui64 LockOffset = 0;

        TInstant CreatedAt() const {
            return Group
                ? TInstant::MicroSeconds(Group)
                : TInstant::MilliSeconds(Step);
        }
    };

    static TVector<TRawTypeValue> MakeKey(TArrayRef<const TCell> cells, TUserTable::TCPtr table) {
        TVector<TRawTypeValue> key(Reserve(cells.size()));

        Y_ABORT_UNLESS(cells.size() == table->KeyColumnTypes.size());
        for (TPos pos = 0; pos < cells.size(); ++pos) {
            key.emplace_back(cells.at(pos).AsRef(), table->KeyColumnTypes.at(pos));
        }

        return key;
    }

    static std::optional<TVector<TUpdateOp>> MakeRestoreUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, TUserTable::TCPtr table) {
        Y_ABORT_UNLESS(cells.size() >= 1);
        TVector<TUpdateOp> updates(::Reserve(cells.size() - 1));

        bool foundSpecialColumn = false;
        Y_ABORT_UNLESS(cells.size() == tags.size());
        for (TPos pos = 0; pos < cells.size(); ++pos) {
            const auto tag = tags.at(pos);
            auto it = table->Columns.find(tag);
            Y_ABORT_UNLESS(it != table->Columns.end());
            if (it->second.Name == "__ydb_incrBackupImpl_deleted") {
                if (const auto& cell = cells.at(pos); !cell.IsNull() && cell.AsValue<bool>()) {
                    return std::nullopt;
                }
                foundSpecialColumn = true;
                continue;
            }
            updates.emplace_back(tag, ECellOp::Set, TRawTypeValue(cells.at(pos).AsRef(), it->second.Type));
        }
        Y_ABORT_UNLESS(foundSpecialColumn);

        return updates;
    }

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
            const auto key = MakeKey(k.GetCells(), table);
            const auto& keyTags = table->KeyColumnIds;
            NKikimrChangeExchange::TDataChange body;
            if (auto updates = MakeRestoreUpdates(v.GetCells(), ValueTags, table); updates) {
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
