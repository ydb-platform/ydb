#pragma once
#include "change_collector.h"
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

class IBaseChangeCollectorSink {
public:
    using TDataChange = NKikimrChangeExchange::TDataChange;

    struct TVersionState {
        TRowVersion WriteVersion;
        ui64 WriteTxId = 0;
    };

public:
    virtual TVersionState GetVersionState() = 0;
    virtual void SetVersionState(const TVersionState& state) = 0;
    virtual void AddChange(const TTableId& tableId, const TPathId& pathId, TChangeRecord::EKind kind, const TDataChange& body) = 0;

protected:
    ~IBaseChangeCollectorSink() = default;
};

class IBaseChangeCollector {
public:
    virtual ~IBaseChangeCollector() = default;

    virtual void OnRestart() = 0;
    virtual bool NeedToReadKeys() const = 0;

    virtual bool Collect(const TTableId& tableId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates) = 0;
};

class TBaseChangeCollector
    : public IBaseChangeCollector
    , protected TChangeRecordBodySerializer
{
    using TDataChange = NKikimrChangeExchange::TDataChange;

public:
    explicit TBaseChangeCollector(TDataShard* self, IDataShardUserDb& userDb, IBaseChangeCollectorSink& sink);

    void OnRestart() override;
    bool NeedToReadKeys() const override;

    // abstract class, subclasses need to implement Collect

protected:
    TDataShard* Self;
    IDataShardUserDb& UserDb;
    IBaseChangeCollectorSink& Sink;

}; // TBaseChangeCollector

} // NDataShard
} // NKikimr
