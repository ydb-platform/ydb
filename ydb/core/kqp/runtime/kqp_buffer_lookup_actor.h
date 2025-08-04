#pragma once

#include "kqp_write_table.h"
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NKqp {

struct IKqpBufferTableLookupCallbacks {
    virtual ~IKqpBufferTableLookupCallbacks() = default;

    virtual void OnLookupTaskFinished() = 0;
};

class IKqpBufferTableLookup : public TThrRefBase {
public:
    virtual void SetLookupSettings(ui64 cookie, const std::vector<ui32>& columns) = 0;

    virtual void AddLookupTask(ui64 cookie, const std::vector<TConstArrayRef<TCell>>& keys) = 0;
    virtual bool HasResult(ui64 cookie) = 0;
    virtual void ExtractResult(ui64 cookie, std::function<void(TConstArrayRef<TCell>)>&& callback) = 0;

    virtual TTableId GetTableId() const = 0;
    virtual const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const = 0;
    virtual ui32 LookupColumnsCount(ui64 cookie) const = 0;
};

struct TKqpBufferTableLookupSettings {
    IKqpBufferTableLookupCallbacks* Callbacks = nullptr;
    TTableId TableId;
    TString TablePath;

    ui64 LockTxId;
    ui64 LockNodeId;
    NKikimrDataEvents::ELockMode lockMode;
    std::optional<NKikimrDataEvents::TMvccSnapshot> MvccSnapshot;

    IKqpTransactionManagerPtr TxManager;

    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;

    TActorId SessionActorId;
    TIntrusivePtr<TKqpCounters> Counters;
};

using IKqpBufferTableLookupPtr = TIntrusivePtr<IKqpBufferTableLookup>;

IKqpBufferTableLookupPtr CreateKqpBufferTableLookup(TKqpBufferTableLookupSettings&& settings);

}
}
