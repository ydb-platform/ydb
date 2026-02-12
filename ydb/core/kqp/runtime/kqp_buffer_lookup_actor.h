#pragma once

#include "kqp_write_table.h"
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>


namespace NKikimr {
namespace NKqp {

struct IKqpBufferTableLookupCallbacks {
    virtual ~IKqpBufferTableLookupCallbacks() = default;

    virtual void OnLookupTaskFinished() = 0;
    virtual void OnLookupError(
        NYql::NDqProto::StatusIds::StatusCode statusCode,
        NYql::EYqlIssueCode id,
        const TString& message,
        const NYql::TIssues& subIssues) = 0;
};

class IKqpBufferTableLookup {
public:
    virtual ~IKqpBufferTableLookup() = default;

    virtual void SetLookupSettings(
        ui64 cookie,
        size_t lookupKeyPrefix,
        TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> keyColumns,
        TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> lookupColumns) = 0;

    virtual void AddLookupTask(
        ui64 cookie,
        const std::vector<TConstArrayRef<TCell>>& keys) = 0;
    virtual void AddUniqueCheckTask(
        ui64 cookie,
        const std::vector<TConstArrayRef<TCell>>& keys,
        bool immediateFail) = 0;
    virtual bool HasResult(ui64 cookie) = 0;
    virtual bool IsEmpty(ui64 cookie) = 0;
    virtual void ExtractResult(ui64 cookie, std::function<void(TConstArrayRef<TCell>)>&& callback) = 0;

    virtual TTableId GetTableId() const = 0;
    virtual const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const = 0;
    virtual ui32 LookupColumnsCount(ui64 cookie) const = 0;

    virtual void FillStats(NYql::NDqProto::TDqTaskStats* stats) = 0;

    // Clear all memory
    virtual void Terminate() = 0;
};

struct TKqpBufferTableLookupSettings {
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

std::pair<IKqpBufferTableLookup*, NActors::IActor*> CreateKqpBufferTableLookup(TKqpBufferTableLookupSettings&& settings);

}
}
