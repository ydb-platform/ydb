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

void TBaseChangeCollector::SerializeCells(TSerializedCells& out, TArrayRef<const TRawTypeValue> in, TArrayRef<const TTag> tags) {
    Y_VERIFY_S(in.size() == tags.size(), "Count doesn't match"
        << ": in# " << in.size()
        << ", tags# " << tags.size());

    TVector<TCell> cells(Reserve(in.size()));
    for (size_t i = 0; i < in.size(); ++i) {
        out.AddTags(tags.at(i));
        cells.emplace_back(in.at(i).AsRef());
    }

    out.SetData(TSerializedCellVec::Serialize(cells));
}

void TBaseChangeCollector::SerializeCells(TSerializedCells& out, TArrayRef<const TUpdateOp> in) {
    if (!in) {
        return;
    }

    TVector<TCell> cells(Reserve(in.size()));
    for (const auto& op : in) {
        Y_VERIFY_S(op.Op == ECellOp::Set, "Unexpected cell op: " << op.Op.Raw());

        out.AddTags(op.Tag);
        cells.emplace_back(op.AsCell());
    }

    out.SetData(TSerializedCellVec::Serialize(cells));
}

void TBaseChangeCollector::SerializeCells(TSerializedCells& out, const TRowState& state, TArrayRef<const TTag> tags) {
    Y_VERIFY_S(state.Size() == tags.size(), "Count doesn't match"
        << ": state# " << state.Size()
        << ", tags# " << tags.size());

    TVector<TCell> cells(Reserve(state.Size()));
    for (TPos pos = 0; pos < state.Size(); ++pos) {
        out.AddTags(tags.at(pos));
        cells.emplace_back(state.Get(pos));
    }

    out.SetData(TSerializedCellVec::Serialize(cells));
}

void TBaseChangeCollector::Serialize(TDataChange& out, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags, TArrayRef<const TUpdateOp> updates)
{
    SerializeCells(*out.MutableKey(), key, keyTags);

    switch (rop) {
    case ERowOp::Upsert:
        SerializeCells(*out.MutableUpsert(), updates);
        break;
    case ERowOp::Erase:
        out.MutableErase();
        break;
    case ERowOp::Reset:
        SerializeCells(*out.MutableReset(), updates);
        break;
    default:
        Y_FAIL_S("Unsupported row op: " << static_cast<ui8>(rop));
    }
}

void TBaseChangeCollector::Serialize(TDataChange& out, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags,
        const TRowState* oldState, const TRowState* newState, TArrayRef<const TTag> valueTags)
{
    Serialize(out, rop, key, keyTags, {});

    if (oldState) {
        SerializeCells(*out.MutableOldImage(), *oldState, valueTags);
    }

    if (newState) {
        SerializeCells(*out.MutableNewImage(), *newState, valueTags);
    }
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
