#pragma once

#include "change_record.h"

#include <ydb/core/engine/minikql/change_collector_iface.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/tablet_flat/flat_database.h>

#include <util/generic/maybe.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;

class TBaseChangeCollector: public NMiniKQL::IChangeCollector {
    using TDataChange = NKikimrChangeExchange::TChangeRecord::TDataChange;
    using TSerializedCells = TDataChange::TSerializedCells;

    static void SerializeCells(TSerializedCells& out, TArrayRef<const TRawTypeValue> in, TArrayRef<const NTable::TTag> tags);
    static void SerializeCells(TSerializedCells& out, TArrayRef<const NTable::TUpdateOp> in);
    static void SerializeCells(TSerializedCells& out, const NTable::TRowState& state, TArrayRef<const NTable::TTag> tags);

protected:
    static void Serialize(TDataChange& out, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags, TArrayRef<const NTable::TUpdateOp> updates);
    static void Serialize(TDataChange& out, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags,
        const NTable::TRowState* oldState, const NTable::TRowState* newState, TArrayRef<const NTable::TTag> valueTags);

    void Persist(const TTableId& tableId, const TPathId& pathId, TChangeRecord::EKind kind, const TDataChange& body);

public:
    explicit TBaseChangeCollector(TDataShard* self, NTable::TDatabase& db, bool isImmediateTx);

    bool NeedToReadKeys() const override;
    void SetReadVersion(const TRowVersion& readVersion) override;
    void SetWriteVersion(const TRowVersion& writeVersion) override;

    const TVector<TChange>& GetCollected() const override;
    TVector<TChange>&& GetCollected() override;
    void Reset() override;

    // there is no Collect, still abstract

protected:
    TDataShard* Self;
    NTable::TDatabase& Db;

    TRowVersion WriteVersion;
    TMaybe<ui64> Group;

    TVector<TChange> Collected;

}; // TBaseChangeCollector

} // NDataShard
} // NKikimr
