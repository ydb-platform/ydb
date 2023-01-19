#pragma once

#include "change_record.h"
#include "change_record_body_serializer.h"

#include <ydb/core/engine/minikql/change_collector_iface.h>
#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/tablet_flat/flat_database.h>

#include <util/generic/maybe.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;
class IDataShardUserDb;

class IBaseChangeCollector : public NMiniKQL::IChangeCollector {
public:
    virtual void SetGroup(ui64 group) = 0;
};

class TBaseChangeCollector
    : public IBaseChangeCollector
    , protected TChangeRecordBodySerializer
{
    using TDataChange = NKikimrChangeExchange::TChangeRecord::TDataChange;

protected:
    void Persist(const TTableId& tableId, const TPathId& pathId, TChangeRecord::EKind kind, const TDataChange& body);

public:
    explicit TBaseChangeCollector(TDataShard* self, IDataShardUserDb& userDb, NTable::TDatabase& db, bool isImmediateTx);

    bool NeedToReadKeys() const override;
    void SetReadVersion(const TRowVersion& readVersion) override;
    void SetWriteVersion(const TRowVersion& writeVersion) override;
    void SetWriteTxId(ui64 txId) override;
    void SetGroup(ui64 group) override;

    const TVector<TChange>& GetCollected() const override;
    TVector<TChange>&& GetCollected() override;
    void Reset() override;

    // there is no Collect, still abstract

protected:
    TDataShard* Self;
    IDataShardUserDb& UserDb;
    NTable::TDatabase& Db;

    TRowVersion WriteVersion;
    ui64 WriteTxId = 0;
    TMaybe<ui64> Group;

    TVector<TChange> Collected;

}; // TBaseChangeCollector

} // NDataShard
} // NKikimr
