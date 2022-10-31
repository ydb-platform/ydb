#include "change_collector.h"
#include "change_collector_async_index.h"
#include "change_collector_cdc_stream.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"

#include <util/generic/vector.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TChangeCollectorProxy: public IDataShardChangeCollector {
public:
    TChangeCollectorProxy(TDataShard& dataShard, bool isImmediateTx)
        : DataShard(dataShard)
    {
        if (!isImmediateTx) {
            Group = 0;
        }
    }

    void AddUnderlying(THolder<IBaseChangeCollector> collector) {
        Underlying.emplace_back(std::move(collector));
    }

    bool NeedToReadKeys() const override {
        for (const auto& collector : Underlying) {
            if (collector->NeedToReadKeys()) {
                return true;
            }
        }

        return false;
    }

    void SetReadVersion(const TRowVersion& readVersion) override {
        for (auto& collector : Underlying) {
            collector->SetReadVersion(readVersion);
        }
    }

    void SetWriteVersion(const TRowVersion& writeVersion) override {
        WriteVersion = writeVersion;
        for (auto& collector : Underlying) {
            collector->SetWriteVersion(writeVersion);
        }
    }

    void SetWriteTxId(ui64 txId) override {
        for (auto& collector : Underlying) {
            collector->SetWriteTxId(txId);
        }
    }

    bool Collect(const TTableId& tableId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates) override
    {
        for (auto& collector : Underlying) {
            if (!collector->Collect(tableId, rop, key, updates)) {
                return false;
            }
        }

        return true;
    }

    const TVector<TChange>& GetCollected() const override {
        CollectedBuf.clear();

        if (!LockChanges.empty()) {
            std::copy(LockChanges.begin(), LockChanges.end(), std::back_inserter(CollectedBuf));
        }

        for (const auto& collector : Underlying) {
            const auto& collected = collector->GetCollected();
            std::copy(collected.begin(), collected.end(), std::back_inserter(CollectedBuf));
        }

        return CollectedBuf;
    }

    TVector<TChange>&& GetCollected() override {
        CollectedBuf.clear();

        if (!LockChanges.empty()) {
            std::move(LockChanges.begin(), LockChanges.end(), std::back_inserter(CollectedBuf));
        }

        for (auto& collector : Underlying) {
            auto collected = std::move(collector->GetCollected());
            std::move(collected.begin(), collected.end(), std::back_inserter(CollectedBuf));
        }

        return std::move(CollectedBuf);
    }

    void Reset() override {
        for (auto& collector : Underlying) {
            collector->Reset();
        }

        CollectedBuf.clear();
    }

    void CommitLockChanges(ui64 lockId, const TVector<TChange>& changes, TTransactionContext& txc) override {
        if (changes.empty()) {
            return;
        }

        NIceDb::TNiceDb db(txc.DB);

        ui64 count = changes.back().LockOffset + 1;
        ui64 order = DataShard.AllocateChangeRecordOrder(db, count);

        if (!Group) {
            Group = DataShard.AllocateChangeRecordGroup(db);
            for (auto& collector : Underlying) {
                collector->SetGroup(*Group);
            }
        }

        LockChanges.reserve(LockChanges.size() + changes.size());
        for (const auto& change : changes) {
            TChange fixed = change;
            fixed.Order = order + change.LockOffset;
            fixed.Group = *Group;
            fixed.Step = WriteVersion.Step;
            fixed.TxId = WriteVersion.TxId;
            LockChanges.push_back(fixed);
        }

        DataShard.PersistCommitLockChangeRecords(txc, order, lockId, *Group, WriteVersion);
    }

private:
    TDataShard& DataShard;
    TMaybe<ui64> Group;
    TRowVersion WriteVersion = TRowVersion::Min();
    TVector<THolder<IBaseChangeCollector>> Underlying;
    TVector<TChange> LockChanges;
    mutable TVector<TChange> CollectedBuf;

}; // TChangeCollectorProxy

IDataShardChangeCollector* CreateChangeCollector(TDataShard& dataShard, IDataShardUserDb& userDb, NTable::TDatabase& db, const TUserTable& table, bool isImmediateTx) {
    const bool hasAsyncIndexes = table.HasAsyncIndexes();
    const bool hasCdcStreams = table.HasCdcStreams();

    if (!hasAsyncIndexes && !hasCdcStreams) {
        return nullptr;
    }

    auto proxy = MakeHolder<TChangeCollectorProxy>(dataShard, isImmediateTx);

    if (hasAsyncIndexes) {
        proxy->AddUnderlying(MakeHolder<TAsyncIndexChangeCollector>(&dataShard, userDb, db, isImmediateTx));
    }

    if (hasCdcStreams) {
        proxy->AddUnderlying(MakeHolder<TCdcStreamChangeCollector>(&dataShard, userDb, db, isImmediateTx));
    }

    return proxy.Release();
}

IDataShardChangeCollector* CreateChangeCollector(TDataShard& dataShard, IDataShardUserDb& userDb, NTable::TDatabase& db, ui64 tableId, bool isImmediateTx) {
    Y_VERIFY(dataShard.GetUserTables().contains(tableId));
    const TUserTable& tableInfo = *dataShard.GetUserTables().at(tableId);
    return CreateChangeCollector(dataShard, userDb, db, tableInfo, isImmediateTx);
}

} // NDataShard
} // NKikimr
