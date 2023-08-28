#include "datashard_impl.h"

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NDataShard {

using namespace NTabletFlatExecutor;

class TDataShard::TTxRequestChangeRecords: public TTransactionBase<TDataShard> {
    using Schema = TDataShard::Schema;

    bool Precharge(NIceDb::TNiceDb& db) {
        size_t bodiesSize = 0;
        bool ok = true;

        for (const auto& [_, records] : Self->ChangeRecordsRequested) {
            for (const auto& record : records) {
                auto itQueue = Self->ChangesQueue.find(record.Order);
                if (itQueue == Self->ChangesQueue.end()) {
                    continue;
                }

                if (bodiesSize && (bodiesSize + record.BodySize) > MemLimit) {
                    break;
                }

                bodiesSize += record.BodySize;

                if (itQueue->second.LockId) {
                    ok = ok && db.Table<Schema::LockChangeRecords>().Key(itQueue->second.LockId, itQueue->second.LockOffset).Precharge();
                    ok = ok && db.Table<Schema::LockChangeRecordDetails>().Key(itQueue->second.LockId, itQueue->second.LockOffset).Precharge();
                } else {
                    ok = ok && db.Table<Schema::ChangeRecords>().Key(record.Order).Precharge();
                    ok = ok && db.Table<Schema::ChangeRecordDetails>().Key(record.Order).Precharge();
                }
            }
        }

        return ok;
    }

    bool Select(NIceDb::TNiceDb& db) {
        for (auto& [recipient, records] : Self->ChangeRecordsRequested) {
            if (!records) {
                continue;
            }

            auto it = records.begin();
            while (it != records.end()) {
                auto itQueue = Self->ChangesQueue.find(it->Order);
                if (itQueue == Self->ChangesQueue.end()) {
                    RecordsToForget[recipient].emplace_back(it->Order);
                    it = records.erase(it);
                    continue;
                }

                if (MemUsage && (MemUsage + it->BodySize) > MemLimit) {
                    break;
                }

                if (itQueue->second.LockId) {
                    auto itCommit = Self->CommittedLockChangeRecords.find(itQueue->second.LockId);
                    if (itCommit == Self->CommittedLockChangeRecords.end()) {
                        Y_VERIFY_DEBUG_S(false, "Unexpected change record " << it->Order << " from an uncommitted lock " << itQueue->second.LockId);
                        RecordsToForget[recipient].emplace_back(it->Order);
                        it = records.erase(it);
                        continue;
                    }

                    auto basic = db.Table<Schema::LockChangeRecords>().Key(itQueue->second.LockId, itQueue->second.LockOffset).Select();
                    auto details = db.Table<Schema::LockChangeRecordDetails>().Key(itQueue->second.LockId, itQueue->second.LockOffset).Select();

                    if (!basic.IsReady() || !details.IsReady()) {
                        return false;
                    }

                    if (!basic.IsValid() && !details.IsValid()) {
                        RecordsToForget[recipient].emplace_back(it->Order);
                        it = records.erase(it);
                        continue;
                    }

                    Y_VERIFY_S(basic.IsValid() && details.IsValid(), "Inconsistent basic and details"
                        << ", basic.IsValid: " << basic.IsValid()
                        << ", details.IsValid: " << details.IsValid()
                        << ", recipient: " << recipient
                        << ", records.size: " << records.size()
                        << ", it->Order: " << it->Order
                        << ", it->BodySize: " << it->BodySize
                        << ", LockId: " << itQueue->second.LockId
                        << ", LockOffset: " << itQueue->second.LockOffset);

                    const auto schemaVersion = basic.GetValue<Schema::LockChangeRecords::SchemaVersion>();
                    const auto tableId = TPathId(
                        basic.GetValue<Schema::LockChangeRecords::TableOwnerId>(),
                        basic.GetValue<Schema::LockChangeRecords::TablePathId>()
                    );

                    TUserTable::TCPtr schema;
                    if (schemaVersion) {
                        const auto snapshotKey = TSchemaSnapshotKey(tableId, schemaVersion);
                        if (const auto* snapshot = Self->GetSchemaSnapshotManager().FindSnapshot(snapshotKey)) {
                            schema = snapshot->Schema;
                        }
                    }

                    RecordsToSend[recipient].emplace_back(TChangeRecordBuilder(details.GetValue<Schema::LockChangeRecordDetails::Kind>())
                        .WithOrder(it->Order)
                        .WithGroup(itCommit->second.Group)
                        .WithStep(itCommit->second.Step)
                        .WithTxId(itCommit->second.TxId)
                        .WithPathId(TPathId(
                            basic.GetValue<Schema::LockChangeRecords::PathOwnerId>(),
                            basic.GetValue<Schema::LockChangeRecords::LocalPathId>()
                        ))
                        .WithTableId(tableId)
                        .WithSchemaVersion(schemaVersion)
                        .WithSchema(schema)
                        .WithBody(details.GetValue<Schema::LockChangeRecordDetails::Body>())
                        .WithLockId(itQueue->second.LockId)
                        .WithLockOffset(itQueue->second.LockOffset)
                        .Build());
                } else {
                    auto basic = db.Table<Schema::ChangeRecords>().Key(it->Order).Select();
                    auto details = db.Table<Schema::ChangeRecordDetails>().Key(it->Order).Select();

                    if (!basic.IsReady() || !details.IsReady()) {
                        return false;
                    }

                    if (!basic.IsValid() && !details.IsValid()) {
                        RecordsToForget[recipient].emplace_back(it->Order);
                        it = records.erase(it);
                        continue;
                    }

                    Y_VERIFY_S(basic.IsValid() && details.IsValid(), "Inconsistent basic and details"
                        << ", basic.IsValid: " << basic.IsValid()
                        << ", details.IsValid: " << details.IsValid()
                        << ", recipient: " << recipient
                        << ", records.size: " << records.size()
                        << ", it->Order: " << it->Order
                        << ", it->BodySize: " << it->BodySize);

                    const auto schemaVersion = basic.GetValue<Schema::ChangeRecords::SchemaVersion>();
                    const auto tableId = TPathId(
                        basic.GetValue<Schema::ChangeRecords::TableOwnerId>(),
                        basic.GetValue<Schema::ChangeRecords::TablePathId>()
                    );

                    TUserTable::TCPtr schema;
                    if (schemaVersion) {
                        const auto snapshotKey = TSchemaSnapshotKey(tableId, schemaVersion);
                        if (const auto* snapshot = Self->GetSchemaSnapshotManager().FindSnapshot(snapshotKey)) {
                            schema = snapshot->Schema;
                        }
                    }
                    
                    RecordsToSend[recipient].emplace_back(TChangeRecordBuilder(details.GetValue<Schema::ChangeRecordDetails::Kind>())
                        .WithOrder(it->Order)
                        .WithGroup(basic.GetValue<Schema::ChangeRecords::Group>())
                        .WithStep(basic.GetValue<Schema::ChangeRecords::PlanStep>())
                        .WithTxId(basic.GetValue<Schema::ChangeRecords::TxId>())
                        .WithPathId(TPathId(
                            basic.GetValue<Schema::ChangeRecords::PathOwnerId>(),
                            basic.GetValue<Schema::ChangeRecords::LocalPathId>()
                        ))
                        .WithTableId(tableId)
                        .WithSchemaVersion(schemaVersion)
                        .WithSchema(schema)
                        .WithBody(details.GetValue<Schema::ChangeRecordDetails::Body>())
                        .Build());
                }

                MemUsage += it->BodySize;
                it = records.erase(it);
            }
        }

        return true;
    }

public:
    explicit TTxRequestChangeRecords(TDataShard* self)
        : TTransactionBase(self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_REQUEST_CHANGE_RECORDS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxRequestChangeRecords Execute"
            << ": at tablet# " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        if (!Precharge(db) || !Select(db)) {
            return false;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        size_t sent = 0;
        for (auto& kv : RecordsToSend) {
            const auto& to = kv.first;
            auto& records = kv.second;

            sent += records.size();

            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Send " << records.size() << " change records"
                << ": to# " << to
                << ", at tablet# " << Self->TabletID());
            ctx.Send(to, new TEvChangeExchange::TEvRecords(std::move(records)));
        }

        size_t forgotten = 0;
        for (auto& kv : RecordsToForget) {
            const auto& to = kv.first;
            auto& records = kv.second;

            forgotten += records.size();

            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Forget " << records.size() << " change records"
                << ": to# " << to
                << ", at tablet# " << Self->TabletID());
            ctx.Send(to, new TEvChangeExchange::TEvForgetRecords(std::move(records)));
        }

        size_t left = Accumulate(Self->ChangeRecordsRequested, (size_t)0, [](size_t sum, const auto& kv) {
            return sum + kv.second.size();
        });

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxRequestChangeRecords Complete"
            << ": sent# " << sent
            << ", forgotten# " << forgotten
            << ", left# " << left
            << ", at tablet# " << Self->TabletID());

        Self->SetCounter(COUNTER_CHANGE_RECORDS_REQUESTED, left);
        Self->IncCounter(COUNTER_CHANGE_RECORDS_SENT, sent);
        Self->IncCounter(COUNTER_CHANGE_RECORDS_FORGOTTEN, forgotten);

        if (left) {
            Self->Execute(new TTxRequestChangeRecords(Self), ctx);
        } else {
            Self->RequestChangeRecordsInFly = false;
        }
    }

private:
    static constexpr size_t MemLimit = 512_KB;
    size_t MemUsage = 0;

    THashMap<TActorId, TVector<TChangeRecord>> RecordsToSend;
    THashMap<TActorId, TVector<ui64>> RecordsToForget;

}; // TTxRequestChangeRecords

class TDataShard::TTxRemoveChangeRecords: public TTransactionBase<TDataShard> {
    void FillActivationList() {
        if (!Self->ChangesQueue) {
            if (!Self->ChangeExchangeSplitter.Done()) {
                ChangeExchangeSplit = true;
            } else {
                for (const auto dstTabletId : Self->ChangeSenderActivator.GetDstSet()) {
                    if (Self->SplitSrcSnapshotSender.Acked(dstTabletId)) {
                        ActivationList.insert(dstTabletId);
                    }
                }
            }
        }
    }

public:
    explicit TTxRemoveChangeRecords(TDataShard* self)
        : TTransactionBase(self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_REMOVE_CHANGE_RECORDS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxRemoveChangeRecords Execute"
            << ": records# " << Self->ChangeRecordsToRemove.size()
            << ", at tablet# " << Self->TabletID());

        if (!Self->ChangeRecordsToRemove) {
            FillActivationList();
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto it = Self->ChangeRecordsToRemove.begin();
        while (RemovedCount < BucketSize && it != Self->ChangeRecordsToRemove.end()) {
            Self->RemoveChangeRecord(db, *it);

            it = Self->ChangeRecordsToRemove.erase(it);
            ++RemovedCount;
        }

        FillActivationList();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxRemoveChangeRecords Complete"
            << ": removed# " << RemovedCount
            << ", left# " << Self->ChangeRecordsToRemove.size()
            << ", at tablet# " << Self->TabletID());

        if (Self->ChangeRecordsToRemove) {
            Self->Execute(new TTxRemoveChangeRecords(Self), ctx);
        } else {
            Self->RemoveChangeRecordsInFly = false;
        }

        if (ChangeExchangeSplit) {
            Self->ChangeExchangeSplitter.DoSplit(ctx);
        }

        for (const auto dstTabletId : ActivationList) {
            if (!Self->ChangeSenderActivator.Acked(dstTabletId)) {
                Self->ChangeSenderActivator.DoSend(dstTabletId, ctx);
            }
        }
    }

private:
    static constexpr size_t BucketSize = 1000;
    size_t RemovedCount = 0;
    THashSet<ui64> ActivationList;
    bool ChangeExchangeSplit = false;

}; // TTxRemoveChangeRecords

class TDataShard::TTxChangeExchangeSplitAck: public TTransactionBase<TDataShard> {
public:
    explicit TTxChangeExchangeSplitAck(TDataShard* self)
        : TTransactionBase(self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CHANGE_EXCHANGE_SPLIT_ACK;
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxChangeExchangeSplitAck Execute"
            << ", at tablet# " << Self->TabletID());

        Y_VERIFY(!Self->ChangesQueue);

        Self->ChangeExchangeSplitter.Ack();
        Y_VERIFY(Self->ChangeExchangeSplitter.Done());

        for (const auto dstTabletId : Self->ChangeSenderActivator.GetDstSet()) {
            if (Self->SplitSrcSnapshotSender.Acked(dstTabletId)) {
                ActivationList.insert(dstTabletId);
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxChangeExchangeSplitAck Complete"
            << ", at tablet# " << Self->TabletID());

        for (const auto dstTabletId : ActivationList) {
            if (!Self->ChangeSenderActivator.Acked(dstTabletId)) {
                Self->ChangeSenderActivator.DoSend(dstTabletId, ctx);
            }
        }
    }

private:
    THashSet<ui64> ActivationList;

}; // TTxChangeExchangeSplitAck

/// Request
void TDataShard::Handle(TEvChangeExchange::TEvRequestRecords::TPtr& ev, const TActorContext& ctx) {
    ChangeRecordsRequested[ev->Sender].insert(ev->Get()->Records.begin(), ev->Get()->Records.end());
    SetCounter(COUNTER_CHANGE_QUEUE_SIZE, Accumulate(ChangeRecordsRequested, (size_t)0, [](size_t sum, const auto& kv) {
        return sum + kv.second.size();
    }));
    ScheduleRequestChangeRecords(ctx);
}

void TDataShard::ScheduleRequestChangeRecords(const TActorContext& ctx) {
    if (ChangeRecordsRequested && !RequestChangeRecordsInFly) {
        ctx.Send(SelfId(), new TEvPrivate::TEvRequestChangeRecords);
        RequestChangeRecordsInFly = true;
    }
}

void TDataShard::Handle(TEvPrivate::TEvRequestChangeRecords::TPtr&, const TActorContext& ctx) {
    Execute(new TTxRequestChangeRecords(this), ctx);
}

/// Remove
void TDataShard::Handle(TEvChangeExchange::TEvRemoveRecords::TPtr& ev, const TActorContext& ctx) {
    ChangeRecordsToRemove.insert(ev->Get()->Records.begin(), ev->Get()->Records.end());
    ScheduleRemoveChangeRecords(ctx);
}

void TDataShard::ScheduleRemoveChangeRecords(const TActorContext& ctx) {
    if (ChangeRecordsToRemove && !RemoveChangeRecordsInFly) {
        ctx.Send(SelfId(), new TEvPrivate::TEvRemoveChangeRecords);
        RemoveChangeRecordsInFly = true;
    }
}

void TDataShard::Handle(TEvPrivate::TEvRemoveChangeRecords::TPtr&, const TActorContext& ctx) {
    Execute(new TTxRemoveChangeRecords(this), ctx);
}

/// SplitAck
void TDataShard::Handle(TEvChangeExchange::TEvSplitAck::TPtr&, const TActorContext& ctx) {
    Execute(new TTxChangeExchangeSplitAck(this), ctx);
}

}
