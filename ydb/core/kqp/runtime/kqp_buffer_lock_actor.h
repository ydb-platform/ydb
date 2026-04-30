#pragma once

#include "kqp_write_table.h"
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>


namespace NKikimr {
namespace NKqp {

struct IKqpBufferTableLookupCallbacks;

// Will be deleted after locks support in EvWrite and EvRead
class IKqpBufferTableLock {
public:
    virtual ~IKqpBufferTableLock() = default;

    virtual void SetLockSettings(
        ui64 cookie,
        TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> keyColumns) = 0;

    virtual void AddLockTask(
        ui64 cookie,
        const std::vector<TConstArrayRef<TCell>>& rows) = 0;

    virtual bool HasResult(ui64 cookie) = 0;
    virtual bool IsEmpty(ui64 cookie) = 0;
    virtual void ExtractResult(ui64 cookie, std::function<void(const TOwnedCellVec& row, bool modified)>&& callback) = 0;

    virtual TTableId GetTableId() const = 0;
    virtual const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const = 0;

    virtual void FillStats(NYql::NDqProto::TDqTaskStats* stats) = 0;
    virtual void Terminate() = 0;
    virtual void Unlink() = 0;
};

struct TKqpBufferLockSettings {
    IKqpBufferTableLookupCallbacks* Callbacks = nullptr;
    TTableId TableId;
    TString TablePath;

    ui64 LockTxId;
    ui64 LockNodeId;
    NKikimrDataEvents::ELockMode LockMode;
    ui64 QuerySpanId = 0;
    std::optional<NKikimrDataEvents::TMvccSnapshot> MvccSnapshot;

    IKqpTransactionManagerPtr TxManager;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;

    TActorId SessionActorId;
    TIntrusivePtr<TKqpCounters> Counters;

    NWilson::TTraceId ParentTraceId;
};

std::pair<IKqpBufferTableLock*, NActors::IActor*> CreateKqpBufferTableLock(TKqpBufferLockSettings&& settings);

}
}
