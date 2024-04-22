#pragma once

#include "key_validator.h"
#include "datashard_user_db.h"
#include "datashard_user_table.h"
#include "operation.h"

#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tx/locks/locks.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/engine/minikql/minikql_engine_host_counters.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr {
namespace NDataShard {

class TValidatedWriteTxOperation: TMoveOnly {
public:
    std::tuple<NKikimrTxDataShard::TError::EKind, TString> ParseOperation(const NEvents::TDataEvents::TEvWrite& ev, const NKikimrDataEvents::TEvWrite::TOperation& recordOperation, const TUserTable::TTableInfos& tableInfos, ui64 tabletId, TKeyValidator& keyValidator);
    TVector<TKeyValidator::TColumnWriteMeta> GetColumnWrites() const;
    void SetTxKeys(const TUserTable& tableInfo, ui64 tabletId, TKeyValidator& keyValidator);
    
    ui64 ComputeTxSize() const;
private:
    YDB_READONLY_DEF(NKikimrDataEvents::TEvWrite::TOperation::EOperationType, OperationType);
    YDB_READONLY_DEF(TTableId, TableId);
    YDB_READONLY_DEF(std::vector<ui32>, ColumnIds);
    YDB_READONLY_DEF(TSerializedCellMatrix, Matrix);
};

class TValidatedWriteTx: TNonCopyable, public TValidatedTx {
public:
    using TPtr = std::shared_ptr<TValidatedWriteTx>;

    TValidatedWriteTx(TDataShard* self, ui64 globalTxId, TInstant receivedAt, const NEvents::TDataEvents::TEvWrite& ev,
            bool mvccSnapshotRead);
    ~TValidatedWriteTx();

    EType GetType() const override { 
        return EType::WriteTx; 
    };

    static constexpr ui64 MaxReorderTxKeys() {
        return 100;
    }

    ui64 GetTxId() const override {
        return GlobalTxId;
    }

    bool NeedDiagnostics() const {
        return true;
    }
    bool CollectStats() const {
        return true;
    }
    bool Ready() const {
        return ErrCode == NKikimrTxDataShard::TError::OK;
    }
    bool RequirePrepare() const {
        return ErrCode == NKikimrTxDataShard::TError::SNAPSHOT_NOT_READY_YET;
    }
    bool HasWrites() const {
        return TxInfo().HasWrites();
    }
    bool HasLockedWrites() const {
        return HasWrites() && LockTxId;
    }
    bool HasDynamicWrites() const {
        return TxInfo().DynKeysCount != 0;
    }

    TKeyValidator& GetKeyValidator() {
        return KeyValidator;
    }
    const TKeyValidator& GetKeyValidator() const {
        return KeyValidator;
    }

    bool CanCancel();
    bool CheckCancelled();

    ui32 ExtractKeys(const NTable::TScheme& scheme, bool allowErrors);
    bool ReValidateKeys(const NTable::TScheme& scheme);

    ui64 HasOperations() const {
        return !Operations.empty();
    }
    ui32 KeysCount() const {
        return TxInfo().WritesCount;
    }

    void ReleaseTxData();

    ui64 GetMemoryConsumption() const override {
        return GetTxSize();
    }

    bool IsTxInfoLoaded() const {
        return TxInfo().Loaded;
    }

    bool HasOutReadsets() const {
        return TxInfo().HasOutReadsets;
    }
    bool HasInReadsets() const {
        return TxInfo().HasInReadsets;
    }

    const NMiniKQL::IEngineFlat::TValidationInfo& TxInfo() const {
        return KeyValidator.GetInfo();
    }

private:
    void ComputeTxSize();

private:
    TKeyValidator KeyValidator;

    const ui64 TabletId;
    const bool IsImmediate;

    YDB_READONLY_DEF(std::vector<TValidatedWriteTxOperation>, Operations);

    YDB_READONLY_DEF(ui64, LockTxId);
    YDB_READONLY_DEF(ui32, LockNodeId);

    YDB_READONLY_DEF(ui64, GlobalTxId);
    YDB_READONLY_DEF(std::optional<NKikimrDataEvents::TKqpLocks>, KqpLocks);
    YDB_READONLY_DEF(TInstant, ReceivedAt);
    YDB_READONLY_DEF(std::optional<ui64>, OverloadSubscribe);
    YDB_READONLY_DEF(bool, MvccSnapshotRead);

    YDB_READONLY_DEF(ui64, TxSize);

    YDB_READONLY_DEF(NKikimrTxDataShard::TError::EKind, ErrCode);
    YDB_READONLY_DEF(TString, ErrStr);
    YDB_READONLY_DEF(bool, IsReleased);
};

class TWriteOperation : public TOperation {
    friend class TExecuteWriteUnit;
public:
    using TPtr = TIntrusivePtr<TWriteOperation>;

    static TWriteOperation* CastWriteOperation(TOperation::TPtr op);
    static TWriteOperation* TryCastWriteOperation(TOperation::TPtr op);

    explicit TWriteOperation(const TBasicOpInfo& op, ui64 tabletId);
    explicit TWriteOperation(const TBasicOpInfo& op, NEvents::TDataEvents::TEvWrite::TPtr&& ev, TDataShard* self);
    ~TWriteOperation();

    void FillTxData(TValidatedWriteTx::TPtr dataTx);
    void FillTxData(TDataShard* self, const TActorId& target, const TString& txBody, const TVector<TSysTables::TLocksTable::TLock>& locks, ui64 artifactFlags);
    void FillVolatileTxData(TDataShard* self);

    TString GetTxBody() const;
    void SetTxBody(const TString& txBody);
    void ClearTxBody();

    void Deactivate() override {
        ClearTxBody();

        TOperation::Deactivate();
    }

    ui32 ExtractKeys(const NTable::TScheme& scheme) {
        return WriteTx ? WriteTx->ExtractKeys(scheme, false) : 0;
    }

    bool ReValidateKeys(const NTable::TScheme& scheme) {
        return WriteTx ? WriteTx->ReValidateKeys(scheme) : true;
    }

    void MarkAsUsingSnapshot() {
        SetUsingSnapshotFlag();
    }

    bool IsTxDataReleased() const {
        return ReleasedTxDataSize > 0;
    }

    enum EArtifactFlags {
        OUT_RS_STORED = (1 << 0),
        LOCKS_STORED = (1 << 1),
    };
    void MarkOutRSStored() {
        ArtifactFlags |= OUT_RS_STORED;
    }

    bool IsOutRSStored() {
        return ArtifactFlags & OUT_RS_STORED;
    }

    void MarkLocksStored() {
        ArtifactFlags |= LOCKS_STORED;
    }

    bool IsLocksStored() {
        return ArtifactFlags & LOCKS_STORED;
    }

    void DbStoreLocksAccessLog(NTable::TDatabase& txcDb);
    void DbStoreArtifactFlags(NTable::TDatabase& txcDb);

    ui64 GetMemoryConsumption() const;

    ui64 GetRequiredMemory() const {
        Y_ABORT_UNLESS(!GetTxCacheUsage() || !IsTxDataReleased());
        ui64 requiredMem = GetTxCacheUsage() + GetReleasedTxDataSize();
        if (!requiredMem)
            requiredMem = GetMemoryConsumption();
        return requiredMem;
    }

    void ReleaseTxData(NTabletFlatExecutor::TTxMemoryProviderBase& provider);
    ERestoreDataStatus RestoreTxData(TDataShard* self, NTable::TDatabase& db);

    // TOperation iface.
    void BuildExecutionPlan(bool loaded) override;

    bool HasKeysInfo() const override {
        return WriteTx ? WriteTx->TxInfo().Loaded : false;
    }

    const NMiniKQL::IEngineFlat::TValidationInfo& GetKeysInfo() const override {
        if (WriteTx) {
            Y_ABORT_UNLESS(WriteTx->TxInfo().Loaded);
            return WriteTx->TxInfo();
        }
        // For scheme tx global reader and writer flags should
        // result in all required dependencies.
        return TOperation::GetKeysInfo();
    }

    ui64 LockTxId() const override {
        return WriteTx ? WriteTx->GetLockTxId() : 0;
    }

    ui32 LockNodeId() const override {
        return WriteTx ? WriteTx->GetLockNodeId() : 0;
    }

    bool HasLockedWrites() const override {
        return WriteTx ? WriteTx->HasLockedWrites() : false;
    }

    ui64 IncrementPageFaultCount() {
        return ++PageFaultCount;
    }

    const TValidatedWriteTx::TPtr& GetWriteTx() const { 
        return WriteTx; 
    }
    TValidatedWriteTx::TPtr& GetWriteTx() {
        return WriteTx;
    }
    TValidatedWriteTx::TPtr BuildWriteTx(TDataShard* self);

    void ClearWriteTx() { 
        WriteTx = nullptr; 
    }

    const std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>& GetWriteResult() const {
        return WriteResult;
    }
    std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>&& ReleaseWriteResult() {
        return std::move(WriteResult);
    }

    void SetError(const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg);
    void SetWriteResult(std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>&& writeResult);

    bool OnStopping(TDataShard& self, const TActorContext& ctx) override;
    void OnCleanup(TDataShard& self, std::vector<std::unique_ptr<IEventHandle>>& replies) override;

private:
    void TrackMemory() const;
    void UntrackMemory() const;

private:
    std::unique_ptr<NEvents::TDataEvents::TEvWrite> WriteRequest;
    std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> WriteResult;

    TValidatedWriteTx::TPtr WriteTx;

    const ui64 TabletId;

    YDB_READONLY_DEF(TActorId, Recipient);
    YDB_READONLY_DEF(ui64, ArtifactFlags);
    YDB_ACCESSOR_DEF(ui64, TxCacheUsage);
    YDB_ACCESSOR_DEF(ui64, ReleasedTxDataSize);
    YDB_ACCESSOR_DEF(ui64, SchemeShardId);
    YDB_ACCESSOR_DEF(ui64, SubDomainPathId);
    YDB_ACCESSOR_DEF(NKikimrSubDomains::TProcessingParams, ProcessingParams);
    
    ui64 PageFaultCount = 0;
};

} // NDataShard
} // NKikimr
