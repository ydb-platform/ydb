#include "datashard_impl.h"

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

#include <ydb/library/aclib/user_context.h>

#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr::NDataShard {

using namespace NTabletFlatExecutor;

class TDataShard::TTxRequestChangeRecords: public TTransactionBase<TDataShard> {
    using Schema = TDataShard::Schema;
    using IChangeRecord = NChangeExchange::IChangeRecord;

    bool Precharge(NIceDb::TNiceDb& db) {
        size_t bodiesSize = 0;
        bool ok = true;

        for (const auto& [recipient, records] : Self->ChangeRecordsRequested) {
            if (Self->ChangeRecordsWaitingVisibility.contains(recipient)) {
                // Select skips this recipient until its records become visible.
                // Precharging it would consume the batch budget, and a missing
                // page would postpone the whole transaction, delaying ready
                // recipients such as async indexes.
                continue;
            }
            for (const auto& record : records) {
                auto itQueue = Self->ChangesQueue.find(record.Order);
                if (itQueue == Self->ChangesQueue.end()) {
                    continue;
                }

                if (bodiesSize && (bodiesSize + record.BodySize) > MemLimit) {
                    break;
                }

                bodiesSize += record.BodySize;

                // Precharge every record in the batch even if an earlier page is not ready.
                if (itQueue->second.LockId) {
                    ok &= db.Table<Schema::LockChangeRecords>().Key(itQueue->second.LockId, itQueue->second.LockOffset).Precharge();
                    ok &= db.Table<Schema::LockChangeRecordDetails>().Key(itQueue->second.LockId, itQueue->second.LockOffset).Precharge();
                } else {
                    ok &= db.Table<Schema::ChangeRecords>().Key(record.Order).Precharge();
                    ok &= db.Table<Schema::ChangeRecordDetails>().Key(record.Order).Precharge();
                }
            }
        }

        return ok;
    }

    template <typename TTable>
    using TEqualKeyIterator = typename TTable::Operations::template EqualKeyIterator<TTable, typename TTable::TKey::KeyValuesType>;

    template <typename TTable>
    using TFullRowset = typename TTable::Operations::template Rowset<TTable, TEqualKeyIterator<TTable>, typename TTable::TColumns>;

    struct TLoadResult {
        NTable::EReady Ready;
        TIntrusivePtr<NKikimr::NDataShard::TChangeRecord> Record;

        TLoadResult() = default;
        TLoadResult(NTable::EReady ready)
            : Ready(ready)
        {
        }

        explicit TLoadResult(NTable::EReady ready, TIntrusivePtr<NKikimr::NDataShard::TChangeRecord> record)
            : Ready(ready)
            , Record(record)
        {
        }
    };

    template <typename TBasicTable, typename TDetailsTable, bool HaveLock>
    TLoadResult LoadRecord(ui64 order, const std::optional<TCommittedLockChangeRecords>& commited,
            const TFullRowset<TBasicTable>& basic, const TFullRowset<TDetailsTable>& details) const
    {
        if (!basic.IsReady() || !details.IsReady()) {
            return NTable::EReady::Page;
        }

        if (!basic.IsValid() && !details.IsValid()) {
            return NTable::EReady::Gone;
        }

        Y_ENSURE(basic.IsValid() && details.IsValid(), "Inconsistent basic and details"
            << ", basic.IsValid: " << basic.IsValid()
            << ", details.IsValid: " << details.IsValid()
            << ", order: " << order);

        const auto schemaVersion = basic.template GetValue<typename TBasicTable::SchemaVersion>();
        const auto tableId = TPathId(
            basic.template GetValue<typename TBasicTable::TableOwnerId>(),
            basic.template GetValue<typename TBasicTable::TablePathId>()
        );

        TUserTable::TCPtr schema;
        if (schemaVersion) {
            const auto snapshotKey = TSchemaSnapshotKey(tableId, schemaVersion);
            if (const auto* snapshot = Self->GetSchemaSnapshotManager().FindSnapshot(snapshotKey)) {
                schema = snapshot->Schema;
            }
        }

        TChangeRecord::ESource source = TChangeRecord::ESource::Unspecified;
        if (details.template HaveValue<typename TDetailsTable::Source>()) {
            source = details.template GetValue<typename TDetailsTable::Source>();
        }

        auto builder = TChangeRecordBuilder(details.template GetValue<typename TDetailsTable::Kind>())
            .WithOrder(order)
            .WithPathId(TPathId(
                basic.template GetValue<typename TBasicTable::PathOwnerId>(),
                basic.template GetValue<typename TBasicTable::LocalPathId>()
            ))
            .WithTableId(tableId)
            .WithSchemaVersion(schemaVersion)
            .WithSchema(schema)
            .WithBody(details.template GetValue<typename TDetailsTable::Body>())
            .WithSource(source);

        NACLib::TUserContextBuilder userCtxBuilder;
        if (details.template HaveValue<typename TDetailsTable::UserSID>()) {
            userCtxBuilder.WithUserSID(details.template GetValue<typename TDetailsTable::UserSID>());
        }
        if (details.template HaveValue<typename TDetailsTable::UserTraceId>()) {
            const auto value = details.template GetValue<typename TDetailsTable::UserTraceId>();

            NActorsProto::TTraceId serializedTraceId;
            serializedTraceId.SetData(value);
            userCtxBuilder.WithUserTraceId(NWilson::TTraceId(serializedTraceId));
        }
        builder.WithUserCtx(userCtxBuilder.Build());

        if constexpr (HaveLock) {
            Y_ENSURE(commited);
            builder
                .WithGroup(commited->Group)
                .WithStep(commited->Step)
                .WithTxId(commited->TxId);
        } else {
            Y_ENSURE(!commited);
            builder
                .WithGroup(basic.template GetValue<typename TBasicTable::Group>())
                .WithStep(basic.template GetValue<typename TBasicTable::PlanStep>())
                .WithTxId(basic.template GetValue<typename TBasicTable::TxId>());
        }

        return TLoadResult(NTable::EReady::Data, builder.Build());
    }

    bool Select(NIceDb::TNiceDb& db) {
        for (auto& [recipient, records] : Self->ChangeRecordsRequested) {
            // This transaction may be serving other recipients. Waiting queues
            // are not precharged and must stay queued until visibility activation.
            if (!records || Self->ChangeRecordsWaitingVisibility.contains(recipient)) {
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

                TLoadResult result;

                if (itQueue->second.LockId) {
                    auto itCommit = Self->CommittedLockChangeRecords.find(itQueue->second.LockId);
                    if (itCommit == Self->CommittedLockChangeRecords.end()) {
                        Y_VERIFY_DEBUG_S(false, "Unexpected change record " << it->Order << " from an uncommitted lock " << itQueue->second.LockId);
                        RecordsToForget[recipient].emplace_back(it->Order);
                        it = records.erase(it);
                        continue;
                    }

                    result = LoadRecord<Schema::LockChangeRecords, Schema::LockChangeRecordDetails, true>(it->Order, itCommit->second,
                        db.Table<Schema::LockChangeRecords>().Key(itQueue->second.LockId, itQueue->second.LockOffset).Select(),
                        db.Table<Schema::LockChangeRecordDetails>().Key(itQueue->second.LockId, itQueue->second.LockOffset).Select());
                } else {
                    result = LoadRecord<Schema::ChangeRecords, Schema::ChangeRecordDetails, false>(it->Order, std::nullopt,
                        db.Table<Schema::ChangeRecords>().Key(it->Order).Select(),
                        db.Table<Schema::ChangeRecordDetails>().Key(it->Order).Select());
                }

                switch (result.Ready) {
                case NTable::EReady::Page:
                    return false;
                case NTable::EReady::Gone:
                    RecordsToForget[recipient].emplace_back(it->Order);
                    it = records.erase(it);
                    continue;
                case NTable::EReady::Data:
                    break;
                }

                const auto kind = result.Record->GetKind();
                if (kind == IChangeRecord::EKind::CdcDataChange ||
                    kind == IChangeRecord::EKind::CdcHeartbeat)
                {
                    const TRowVersion version(result.Record->GetStep(), result.Record->GetTxId());
                    if (Self->MediatorStateWaiting || version.Step > Self->GetMaxObservedStep()) {
                        // Keep the body requested: PendingBody in the change
                        // sender prevents later records (including heartbeats)
                        // from overtaking it. Other senders, such as async
                        // indexes, may continue independently.
                        RecordsToWait[recipient] = {it->Order, version.Step};
                        break;
                    }
                }

                if (itQueue->second.LockId) {
                    RecordsToSend[recipient].push_back(
                        TChangeRecordBuilder(result.Record)
                            .WithLockId(itQueue->second.LockId)
                            .WithLockOffset(itQueue->second.LockOffset)
                            .WithUserCtx(result.Record->GetUserCtx())
                            .Build()
                    );
                } else {
                    RecordsToSend[recipient].push_back(std::move(result.Record));
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
        YDB_LOG_INFO_CTX(ctx, "TTxRequestChangeRecords Execute",
            {"tabletId", Self->TabletID()});

        // Unlike prepared bodies, waits describe the current head of each
        // request queue and must be recalculated after a transaction restart.
        RecordsToWait.clear();

        NIceDb::TNiceDb db(txc.DB);
        if (!Precharge(db) || !Select(db)) {
            return false;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        // A newer generation may have frozen our in-memory visibility edges
        // while this transaction was committing. It will reload the persisted
        // records; the old generation must not publish any new observations.
        if (Self->InMemoryVarsFrozen) {
            Self->RequestChangeRecordsInFly = false;
            return;
        }

        size_t sent = 0;
        for (auto& kv : RecordsToSend) {
            const auto& to = kv.first;
            auto& records = kv.second;

            sent += records.size();

            YDB_LOG_DEBUG_CTX(ctx, "Send change records",
                {"recordsCount", records.size()},
                {"to", to},
                {"tabletId", Self->TabletID()});
            for (const auto& record : records) {
                if (record->GetKind() == IChangeRecord::EKind::CdcDataChange) {
                    // Complete also guarantees that the loaded record is durable.
                    Self->PromoteCdcReadVisibility(TRowVersion(record->GetStep(), record->GetTxId()));
                }
            }
            ctx.Send(to, new NChangeExchange::TEvChangeExchange::TEvRecords(std::move(records)));
        }

        size_t forgotten = 0;
        for (auto& kv : RecordsToForget) {
            const auto& to = kv.first;
            auto& records = kv.second;

            forgotten += records.size();

            YDB_LOG_DEBUG_CTX(ctx, "Forget change records",
                {"recordsCount", records.size()},
                {"to", to},
                {"tabletId", Self->TabletID()});
            ctx.Send(to, new NChangeExchange::TEvChangeExchange::TEvForgetRecords(std::move(records)));
        }

        size_t left = Accumulate(Self->ChangeRecordsRequested, (size_t)0, [](size_t sum, const auto& kv) {
            return sum + kv.second.size();
        });

        YDB_LOG_INFO_CTX(ctx, "TTxRequestChangeRecords Complete",
            {"sent", sent},
            {"forgotten", forgotten},
            {"left", left},
            {"tabletId", Self->TabletID()});

        Self->SetCounter(COUNTER_CHANGE_RECORDS_REQUESTED, left);
        Self->IncCounter(COUNTER_CHANGE_RECORDS_SENT, sent);
        Self->IncCounter(COUNTER_CHANGE_RECORDS_FORGOTTEN, forgotten);

        Self->RequestChangeRecordsInFly = false;
        for (const auto& [recipient, wait] : RecordsToWait) {
            Self->WaitChangeRecordsVisibility(recipient, wait.Order, wait.Step);
        }
        Self->ScheduleRequestChangeRecords(ctx);
    }

private:
    static constexpr size_t MemLimit = 512_KB;
    size_t MemUsage = 0;

    THashMap<TActorId, TVector<TChangeRecord::TPtr>> RecordsToSend;
    THashMap<TActorId, TVector<ui64>> RecordsToForget;
    THashMap<TActorId, TChangeRecordsVisibilityWait> RecordsToWait;

}; // TTxRequestChangeRecords

class TDataShard::TTxRemoveChangeRecords: public TTransactionBase<TDataShard> {
    void FillActivationList() {
        if (!Self->ChangesQueue) {
            if (!Self->ChangeExchangeSplitter.Done()) {
                ChangeExchangeSplit = true;
            } else {
                for (const auto dstTabletId : Self->ChangeSenderActivator.GetDstSet()) {
                    if (Self->SplitSrcSnapshotSender.Acked(dstTabletId) && !Self->ChangeSenderActivator.Acked(dstTabletId)) {
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
        YDB_LOG_INFO_CTX(ctx, "TTxRemoveChangeRecords Execute",
            {"recordsCount", Self->ChangeRecordsToRemove.size()},
            {"tabletId", Self->TabletID()});

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
        YDB_LOG_INFO_CTX(ctx, "TTxRemoveChangeRecords Complete",
            {"removed", RemovedCount},
            {"left", Self->ChangeRecordsToRemove.size()},
            {"tabletId", Self->TabletID()});

        if (Self->ChangeRecordsToRemove) {
            Self->Execute(new TTxRemoveChangeRecords(Self), ctx);
        } else {
            Self->RemoveChangeRecordsInFly = false;
        }

        if (ChangeExchangeSplit) {
            Self->KillChangeSender(ctx);
            Self->ChangeExchangeSplitter.DoSplit(ctx);
        }

        for (const auto dstTabletId : ActivationList) {
            Self->ChangeSenderActivator.DoSend(dstTabletId, ctx);
        }

        Self->ActivateChangeRecordsWaitingVisibility(ctx);
        Self->CheckStateChange(ctx);
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
        YDB_LOG_NOTICE_CTX(ctx, "TTxChangeExchangeSplitAck Execute",
            {"tabletId", Self->TabletID()});

        Y_ENSURE(!Self->ChangesQueue);

        Self->ChangeExchangeSplitter.Ack();
        Y_ENSURE(Self->ChangeExchangeSplitter.Done());

        for (const auto dstTabletId : Self->ChangeSenderActivator.GetDstSet()) {
            if (Self->SplitSrcSnapshotSender.Acked(dstTabletId) && !Self->ChangeSenderActivator.Acked(dstTabletId)) {
                ActivationList.insert(dstTabletId);
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_NOTICE_CTX(ctx, "TTxChangeExchangeSplitAck Complete",
            {"tabletId", Self->TabletID()});

        for (const auto dstTabletId : ActivationList) {
            Self->ChangeSenderActivator.DoSend(dstTabletId, ctx);
        }
    }

private:
    THashSet<ui64> ActivationList;

}; // TTxChangeExchangeSplitAck

/// Request
void TDataShard::Handle(NChangeExchange::TEvChangeExchange::TEvRequestRecords::TPtr& ev, const TActorContext& ctx) {
    auto& records = ChangeRecordsRequested[ev->Sender];
    records.insert(ev->Get()->Records.begin(), ev->Get()->Records.end());
    if (auto it = ChangeRecordsWaitingVisibility.find(ev->Sender);
        it != ChangeRecordsWaitingVisibility.end() &&
        (!records || records.begin()->Order != it->second.Order))
    {
        // A sender may re-request an earlier record after a delivery failure.
        // Its body must not wait for the visibility of a later record.
        ChangeRecordsWaitingVisibility.erase(it);
    }
    SetCounter(COUNTER_CHANGE_RECORDS_REQUESTED, Accumulate(ChangeRecordsRequested, (size_t)0, [](size_t sum, const auto& kv) {
        return sum + kv.second.size();
    }));
    ScheduleRequestChangeRecords(ctx);
}

void TDataShard::WaitChangeRecordsVisibility(const TActorId& recipient, ui64 order, ui64 step) {
    const auto it = ChangeRecordsRequested.find(recipient);
    if (it == ChangeRecordsRequested.end() || !it->second ||
        it->second.begin()->Order != order || !ChangesQueue.contains(order))
    {
        // The queue may have changed between Execute and Complete.
        return;
    }

    if (!MediatorStateWaiting) {
        if (step <= GetMaxObservedStep()) {
            return;
        }
        WaitPlanStep(step);
        // The shared timecast entry may have advanced while subscribing.
        if (step <= GetMaxObservedStep()) {
            return;
        }
    }

    ChangeRecordsWaitingVisibility[recipient] = {order, step};
}

void TDataShard::ActivateChangeRecordsWaitingVisibility(const TActorContext& ctx) {
    if (InMemoryVarsFrozen || ChangeRecordsWaitingVisibility.empty()) {
        return;
    }

    bool activated = false;
    for (auto it = ChangeRecordsWaitingVisibility.begin(); it != ChangeRecordsWaitingVisibility.end();) {
        const auto requested = ChangeRecordsRequested.find(it->first);
        const auto& wait = it->second;
        if (requested == ChangeRecordsRequested.end() || !requested->second ||
            requested->second.begin()->Order != wait.Order || !ChangesQueue.contains(wait.Order))
        {
            ChangeRecordsWaitingVisibility.erase(it++);
            activated = true;
            continue;
        }

        if (MediatorStateWaiting) {
            ++it;
            continue;
        }

        if (wait.Step > GetMaxObservedStep()) {
            WaitPlanStep(wait.Step);
        }
        // Recheck even when WaitPlanStep did not subscribe: the shared entry
        // may already have advanced, with no further notification pending.
        if (wait.Step <= GetMaxObservedStep()) {
            ChangeRecordsWaitingVisibility.erase(it++);
            activated = true;
        } else {
            ++it;
        }
    }

    if (activated) {
        ScheduleRequestChangeRecords(ctx);
    }
}

void TDataShard::PromoteCdcReadVisibility(const TRowVersion& version) {
    // Called only for durable CDC data records immediately before publication.
    // Observe the same time barrier as immediate write replies, and never
    // change state that has already been handed to a newer generation.
    Y_ENSURE(!InMemoryVarsFrozen);
    Y_ENSURE(!MediatorStateWaiting);
    Y_ENSURE(version.Step <= GetMaxObservedStep());

    if (version <= SnapshotManager.GetImmediateWriteEdge()) {
        // Publishing CDC is an external observation even if the write's RPC
        // reply is lost. Preserve the HEAD read floor during state migration.
        SnapshotManager.PromoteImmediateWriteEdgeReplied(version);
    }
}

void TDataShard::ScheduleRequestChangeRecords(const TActorContext& ctx) {
    if (RequestChangeRecordsInFly || InMemoryVarsFrozen) {
        return;
    }

    for (const auto& [recipient, records] : ChangeRecordsRequested) {
        if (records && !ChangeRecordsWaitingVisibility.contains(recipient)) {
            RequestChangeRecordsInFly = true;
            ctx.Send(SelfId(), new TEvPrivate::TEvRequestChangeRecords);
            return;
        }
    }
}

void TDataShard::Handle(TEvPrivate::TEvRequestChangeRecords::TPtr&, const TActorContext& ctx) {
    Execute(new TTxRequestChangeRecords(this), ctx);
}

/// Remove
void TDataShard::Handle(NChangeExchange::TEvChangeExchange::TEvRemoveRecords::TPtr& ev, const TActorContext& ctx) {
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


#undef YDB_LOG_THIS_FILE_COMPONENT
