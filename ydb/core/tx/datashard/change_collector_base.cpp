#include "change_collector_base.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/util/yverify_stream.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;
using namespace NTable;

TBaseChangeCollector::TBaseChangeCollector(TDataShard* self, IDataShardUserDb& userDb, TDatabase& db, bool isImmediateTx)
    : Self(self)
    , UserDb(userDb)
    , Db(db)
{
    if (!isImmediateTx) {
        Group = 0;
    }
}

bool TBaseChangeCollector::NeedToReadKeys() const {
    return false;
}

void TBaseChangeCollector::SetReadVersion(const TRowVersion& readVersion) {
    Y_UNUSED(readVersion);
}

void TBaseChangeCollector::SetWriteVersion(const TRowVersion& writeVersion) {
    WriteVersion = writeVersion;
}

void TBaseChangeCollector::SetWriteTxId(ui64 txId) {
    WriteTxId = txId;
}

void TBaseChangeCollector::SetGroup(ui64 group) {
    if (!Group) {
        Group = group;
    }
}

const TVector<IChangeCollector::TChange>& TBaseChangeCollector::GetCollected() const {
    return Collected;
}

TVector<IChangeCollector::TChange>&& TBaseChangeCollector::GetCollected() {
    return std::move(Collected);
}

void TBaseChangeCollector::Reset() {
    Collected.clear();
}

void TBaseChangeCollector::Persist(
        const TTableId& tableId, // origin table
        const TPathId& pathId, // target object (table, stream, etc...)
        TChangeRecord::EKind kind, const TDataChange& body)
{
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
        ui64 lockOffset = Self->GetNextChangeRecordLockOffset(WriteTxId) + Collected.size();
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

} // NDataShard
} // NKikimr
