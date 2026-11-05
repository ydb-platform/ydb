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
                        RecordsToWait[recipient] = version;
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
                const TRowVersion version(record->GetStep(), record->GetTxId());
                if (record->GetKind() == IChangeRecord::EKind::CdcDataChange &&
                    version <= Self->SnapshotManager.GetImmediateWriteEdge())
                {
                    // CDC is an external observation of a committed write even
                    // if its original client never receives a reply. Remember
                    // that observation in the state migrated to a new leader.
                    Self->SnapshotManager.PromoteImmediateWriteEdgeReplied(version);
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
        for (const auto& [recipient, version] : RecordsToWait) {
            Self->WaitChangeRecordsVisible(recipient, version);
        }
        Self->ScheduleRequestChangeRecords(ctx);
    }

private:
    static constexpr size_t MemLimit = 512_KB;
    size_t MemUsage = 0;

    THashMap<TActorId, TVector<TChangeRecord::TPtr>> RecordsToSend;
    THashMap<TActorId, TVector<ui64>> RecordsToForget;
    THashMap<TActorId, TRowVersion> RecordsToWait;

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
    ChangeRecordsRequested[ev->Sender].insert(ev->Get()->Records.begin(), ev->Get()->Records.end());
    SetCounter(COUNTER_CHANGE_RECORDS_REQUESTED, Accumulate(ChangeRecordsRequested, (size_t)0, [](size_t sum, const auto& kv) {
        return sum + kv.second.size();
    }));
    ScheduleRequestChangeRecords(ctx);
}

void TDataShard::ScheduleRequestChangeRecords(const TActorContext& ctx) {
    if (RequestChangeRecordsInFly || InMemoryVarsFrozen) {
        return;
    }
    for (const auto& [recipient, records] : ChangeRecordsRequested) {
        if (records && !ChangeRecordsWaitingVisibility.contains(recipient)) {
            ctx.Send(SelfId(), new TEvPrivate::TEvRequestChangeRecords);
            RequestChangeRecordsInFly = true;
            return;
        }
    }
}

void TDataShard::WaitChangeRecordsVisible(TActorId recipient, TRowVersion version) {
    if (!ChangeRecordsWaitingVisibility.insert(recipient).second) {
        co_return;
    }

    if (MediatorStateWaiting) {
        co_await MediatorStateWaitingCoroutines.Wait();
    }
    while (version.Step > GetMaxObservedStep()) {
        WaitPlanStep(version.Step);
        // The shared timecast entry may have advanced while subscribing.
        if (version.Step <= GetMaxObservedStep()) {
            break;
        }
        // Background CDC work must not count towards user transaction
        // admission limits via Pipeline.WaitingCoroutinesCount().
        co_await ChangeRecordsVisibilityChanged.Wait();
    }

    ChangeRecordsWaitingVisibility.erase(recipient);
    ScheduleRequestChangeRecords(ActorContext());
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

