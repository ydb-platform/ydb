#include "change_collector.h"
#include "change_collector_async_index.h"
#include "change_collector_cdc_stream.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"

#include <util/generic/vector.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TChangeCollectorProxy
    : public IDataShardChangeCollector
    , public IBaseChangeCollectorSink
{
public:
    TChangeCollectorProxy(TDataShard* self, NTable::TDatabase& db, bool isImmediateTx)
        : Self(self)
        , Db(db)
    {
        if (!isImmediateTx) {
            Group = 0;
        }
    }

    void AddUnderlying(THolder<IBaseChangeCollector> collector) {
        Underlying.emplace_back(std::move(collector));
    }

    void OnRestart() override {
        for (auto& collector : Underlying) {
            collector->OnRestart();
        }

        Collected.clear();
    }

    bool NeedToReadKeys() const override {
        for (const auto& collector : Underlying) {
            if (collector->NeedToReadKeys()) {
                return true;
            }
        }

        return false;
    }

    bool OnUpdate(const TTableId& tableId, ui32 localTid, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates,
        const TRowVersion& writeVersion) override
    {
        Y_UNUSED(localTid);
        WriteVersion = writeVersion;
        WriteTxId = 0;
        for (auto& collector : Underlying) {
            if (!collector->Collect(tableId, rop, key, updates)) {
                return false;
            }
        }

        return true;
    }

    bool OnUpdateTx(const TTableId& tableId, ui32 localTid, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates,
        ui64 writeTxId) override
    {
        Y_UNUSED(localTid);
        WriteTxId = writeTxId;
        for (auto& collector : Underlying) {
            if (!collector->Collect(tableId, rop, key, updates)) {
                return false;
            }
        }

        return true;
    }

    const TVector<TChange>& GetCollected() const override {
        return Collected;
    }

    TVector<TChange>&& GetCollected() override {
        return std::move(Collected);
    }

    void CommitLockChanges(ui64 lockId, const TRowVersion& writeVersion) override {
        NIceDb::TNiceDb db(Db);

        if (!Group) {
            Group = Self->AllocateChangeRecordGroup(db);
        }

        Self->CommitLockChangeRecords(db, lockId, *Group, writeVersion, Collected);
    }

    TVersionState GetVersionState() override {
        return TVersionState{
            .WriteVersion = WriteVersion,
            .WriteTxId = WriteTxId,
        };
    }

    void SetVersionState(const TVersionState& state) override {
        WriteVersion = state.WriteVersion;
        WriteTxId = state.WriteTxId;
    }

    void AddChange(const TTableId& tableId, const TPathId& pathId, TChangeRecord::EKind kind, const TDataChange& body) override {
        NIceDb::TNiceDb db(Db);

        Y_VERIFY_S(Self->IsUserTable(tableId), "Unknown table: " << tableId);
        auto userTable = Self->GetUserTables().at(tableId.PathId.LocalPathId);
        Y_VERIFY(userTable->GetTableSchemaVersion());

        TChangeRecordBuilder builder(kind);
        if (!WriteTxId) {
            if (!Group) {
                Group = Self->AllocateChangeRecordGroup(db);
            }
            builder
                .WithOrder(Self->AllocateChangeRecordOrder(db))
                .WithGroup(*Group)
                .WithStep(WriteVersion.Step)
                .WithTxId(WriteVersion.TxId);
        } else {
            ui64 lockOffset = Self->GetNextChangeRecordLockOffset(WriteTxId);
            builder
                .WithLockId(WriteTxId)
                .WithLockOffset(lockOffset);
        }

        auto record = builder
            .WithPathId(pathId)
            .WithTableId(tableId.PathId)
            .WithSchemaVersion(userTable->GetTableSchemaVersion())
            .WithBody(body.SerializeAsString())
            .Build();

        Self->PersistChangeRecord(db, record);
        if (record.GetLockId() == 0) {
            Collected.push_back(TChange{
                .Order = record.GetOrder(),
                .Group = record.GetGroup(),
                .Step = record.GetStep(),
                .TxId = record.GetTxId(),
                .PathId = record.GetPathId(),
                .BodySize = record.GetBody().size(),
                .TableId = record.GetTableId(),
                .SchemaVersion = record.GetSchemaVersion(),
                .LockId = record.GetLockId(),
                .LockOffset = record.GetLockOffset(),
            });
        }
    }

private:
    TDataShard* Self;
    NTable::TDatabase& Db;

    TMaybe<ui64> Group;
    TVector<THolder<IBaseChangeCollector>> Underlying;
    TVector<TChange> Collected;

    TRowVersion WriteVersion;
    ui64 WriteTxId = 0;

}; // TChangeCollectorProxy

IDataShardChangeCollector* CreateChangeCollector(TDataShard& dataShard, IDataShardUserDb& userDb, NTable::TDatabase& db, const TUserTable& table, bool isImmediateTx) {
    const bool hasAsyncIndexes = table.HasAsyncIndexes();
    const bool hasCdcStreams = table.HasCdcStreams();

    if (!hasAsyncIndexes && !hasCdcStreams) {
        return nullptr;
    }

    auto proxy = MakeHolder<TChangeCollectorProxy>(&dataShard, db, isImmediateTx);

    if (hasAsyncIndexes) {
        proxy->AddUnderlying(MakeHolder<TAsyncIndexChangeCollector>(&dataShard, userDb, *proxy));
    }

    if (hasCdcStreams) {
        proxy->AddUnderlying(MakeHolder<TCdcStreamChangeCollector>(&dataShard, userDb, *proxy));
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

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::IDataShardChangeCollector::TChange, o, x) {
    o << "{"
      << " Order: " << x.Order
      << " PathId: " << x.PathId
      << " BodySize: " << x.BodySize
      << " TableId: " << x.TableId
      << " SchemaVersion: " << x.SchemaVersion
    << " }";
}
