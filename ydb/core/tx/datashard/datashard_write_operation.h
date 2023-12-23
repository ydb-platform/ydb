#pragma once

#include "datashard_impl.h"
#include "datashard_locks.h"
#include "datashard__engine_host.h"
#include "operation.h"

#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr {
namespace NDataShard {


class TValidatedWriteTx: TNonCopyable {
public:
    using TPtr = std::shared_ptr<TValidatedWriteTx>;

    TValidatedWriteTx(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx, const TStepOrder& stepTxId, TInstant receivedAt, const NEvents::TDataEvents::TEvWrite::TPtr& ev);

    ~TValidatedWriteTx();

    static constexpr ui64 MaxReorderTxKeys() {
        return 100;
    }

    NKikimrTxDataShard::TError::EKind Code() const {
        return ErrCode;
    }
    const TString GetError() const {
        return ErrStr;
    }

    TStepOrder StepTxId() const {
        return StepTxId_;
    }
    ui64 TxId() const {
        return StepTxId_.TxId;
    }
    ui64 TabletId() const {
        return TabletId_;
    }
    const NEvents::TDataEvents::TEvWrite::TPtr& Ev() const {
        return Ev_;
    }

    const NKikimrDataEvents::TEvWrite& Record() const {
        return Ev_->Get()->Record;
    }

    const NKikimrDataEvents::TEvWrite::TOperation& RecordOperation() const {
        //TODO Only one operation is supported now
        return Record().operations(0);
    }

    const TTableId& TableId() const {
        return TableId_;
    }

    const TSerializedCellMatrix Matrix() const {
        return Matrix_;
    }

    ui64 LockTxId() const {
        return Record().locktxid();
    }
    ui32 LockNodeId() const {
        return Record().locknodeid();
    }
    bool Immediate() const {
        return Record().txmode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE;
    }
    bool NeedDiagnostics() const {
        return true;
    }
    bool CollectStats() const {
        return true;
    }
    TInstant ReceivedAt() const {
        return ReceivedAt_;
    }
    TInstant Deadline() const {
        return Deadline_;
    }

    bool Ready() const {
        return ErrCode == NKikimrTxDataShard::TError::OK;
    }
    bool RequirePrepare() const {
        return ErrCode == NKikimrTxDataShard::TError::SNAPSHOT_NOT_READY_YET;
    }
    bool RequireWrites() const {
        return TxInfo().HasWrites() || !Immediate();
    }
    bool HasWrites() const {
        return TxInfo().HasWrites();
    }
    bool HasLockedWrites() const {
        return HasWrites() && LockTxId();
    }
    bool HasDynamicWrites() const {
        return TxInfo().DynKeysCount != 0;
    }

    // TODO: It's an expensive operation (Precharge() inside). We need avoid it.
    TEngineBay::TSizes CalcReadSizes(bool needsTotalKeysSize) const {
        return EngineBay.CalcSizes(needsTotalKeysSize);
    }

    ui64 GetMemoryAllocated() const {
        return EngineBay.GetEngine() ? EngineBay.GetEngine()->GetMemoryAllocated() : 0;
    }

    NMiniKQL::IEngineFlat* GetEngine() {
        return EngineBay.GetEngine();
    }
    void DestroyEngine() {
        EngineBay.DestroyEngine();
    }
    const NMiniKQL::TEngineHostCounters& GetCounters() {
        return EngineBay.GetCounters();
    }
    void ResetCounters() {
        EngineBay.ResetCounters();
    }

    bool CanCancel();
    bool CheckCancelled();

    void SetWriteVersion(TRowVersion writeVersion) {
        EngineBay.SetWriteVersion(writeVersion);
    }
    void SetReadVersion(TRowVersion readVersion) {
        EngineBay.SetReadVersion(readVersion);
    }
    void SetVolatileTxId(ui64 txId) {
        EngineBay.SetVolatileTxId(txId);
    }

    void CommitChanges(const TTableId& tableId, ui64 lockId, const TRowVersion& writeVersion) {
        EngineBay.CommitChanges(tableId, lockId, writeVersion);
    }

    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const {
        return EngineBay.GetCollectedChanges();
    }
    void ResetCollectedChanges() {
        EngineBay.ResetCollectedChanges();
    }

    TVector<ui64> GetVolatileCommitTxIds() const {
        return EngineBay.GetVolatileCommitTxIds();
    }
    const absl::flat_hash_set<ui64>& GetVolatileDependencies() const {
        return EngineBay.GetVolatileDependencies();
    }
    std::optional<ui64> GetVolatileChangeGroup() const {
        return EngineBay.GetVolatileChangeGroup();
    }
    bool GetVolatileCommitOrdered() const {
        return EngineBay.GetVolatileCommitOrdered();
    }

    TActorId Source() const {
        return Source_;
    }
    void SetSource(const TActorId& actorId) {
        Source_ = actorId;
    }
    void SetStep(ui64 step) {
        StepTxId_.Step = step;
    }
    bool IsProposed() const {
        return Source_ != TActorId();
    }

    inline const ::NKikimrDataEvents::TKqpLocks& GetKqpLocks() const {
        return Record().locks();
    }

    bool ParseRecord(TDataShard* self);
    void SetTxKeys(const ::google::protobuf::RepeatedField<::NProtoBuf::uint32>& columnIds, const NScheme::TTypeRegistry& typeRegistry, const TActorContext& ctx);

    ui32 ExtractKeys(bool allowErrors);
    bool ReValidateKeys();

    ui64 GetTxSize() const {
        return TxSize;
    }
    ui32 KeysCount() const {
        return TxInfo().WritesCount;
    }

    void SetTxCacheUsage(ui64 val) {
        TxCacheUsage = val;
    }
    ui64 GetTxCacheUsage() const {
        return TxCacheUsage;
    }

    void ReleaseTxData();
    bool IsTxDataReleased() const {
        return IsReleased;
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
        return EngineBay.TxInfo();
    }

private:
    //TODO: YDB_READONLY
    TStepOrder StepTxId_;
    ui64 TabletId_;
    TTableId TableId_;
    const TUserTable* TableInfo_;
    const NEvents::TDataEvents::TEvWrite::TPtr& Ev_;
    TSerializedCellMatrix Matrix_;
    TActorId Source_;
    TEngineBay EngineBay;
    NKikimrTxDataShard::TError::EKind ErrCode;
    TString ErrStr;
    ui64 TxSize;
    ui64 TxCacheUsage;
    bool IsReleased;
    const TInstant ReceivedAt_;  // For local timeout tracking
    TInstant Deadline_;

    void ComputeTxSize();
};

class TWriteOperation : public TOperation {
    friend class TWriteUnit;
public:
    explicit TWriteOperation(const TBasicOpInfo& op, NEvents::TDataEvents::TEvWrite::TPtr ev, TDataShard* self, TTransactionContext& txc, const TActorContext& ctx);

    ~TWriteOperation();

    void FillTxData(TValidatedWriteTx::TPtr dataTx);
    void FillTxData(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx, const TActorId& target, NEvents::TDataEvents::TEvWrite::TPtr&& ev, const TVector<TSysTables::TLocksTable::TLock>& locks, ui64 artifactFlags);
    void FillVolatileTxData(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx);

    const NEvents::TDataEvents::TEvWrite::TPtr& GetEv() const {
        return Ev_;
    }
    void SetEv(const NEvents::TDataEvents::TEvWrite::TPtr& ev) {
        UntrackMemory();
        Ev_ = ev;
        TrackMemory();
    }
    void ClearEv() {
        UntrackMemory();
        Ev_.Reset();
        TrackMemory();
    }

    ui64 GetSchemeShardId() const {
        return SchemeShardId;
    }
    void SetSchemeShardId(ui64 id) {
        SchemeShardId = id;
    }
    ui64 GetSubDomainPathId() const {
        return SubDomainPathId;
    }
    void SetSubDomainPathId(ui64 pathId) {
        SubDomainPathId = pathId;
    }

    const NKikimrSubDomains::TProcessingParams& GetProcessingParams() const {
        return ProcessingParams;
    }
    void SetProcessingParams(const NKikimrSubDomains::TProcessingParams& params)
    {
        ProcessingParams.CopyFrom(params);
    }

    void Deactivate() override {
        ClearEv();

        TOperation::Deactivate();
    }

    ui32 ExtractKeys() {
        return WriteTx_ ? WriteTx_->ExtractKeys(false) : 0;
    }

    bool ReValidateKeys() {
        return WriteTx_ ? WriteTx_->ReValidateKeys() : true;
    }

    void MarkAsUsingSnapshot() {
        SetUsingSnapshotFlag();
    }

    void SetTxCacheUsage(ui64 val) {
        TxCacheUsage = val;
    }
    ui64 GetTxCacheUsage() const {
        return TxCacheUsage;
    }

    ui64 GetReleasedTxDataSize() const {
        return ReleasedTxDataSize;
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

    void DbStoreLocksAccessLog(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx);
    void DbStoreArtifactFlags(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx);

    ui64 GetMemoryConsumption() const;

    ui64 GetRequiredMemory() const {
        Y_ABORT_UNLESS(!GetTxCacheUsage() || !IsTxDataReleased());
        ui64 requiredMem = GetTxCacheUsage() + GetReleasedTxDataSize();
        if (!requiredMem)
            requiredMem = GetMemoryConsumption();
        return requiredMem;
    }

    void ReleaseTxData(NTabletFlatExecutor::TTxMemoryProviderBase& provider, const TActorContext& ctx);
    ERestoreDataStatus RestoreTxData(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx);
    void FinalizeWriteTxPlan();

    // TOperation iface.
    void BuildExecutionPlan(bool loaded) override;

    bool HasKeysInfo() const override {
        return WriteTx_ ? WriteTx_->TxInfo().Loaded : false;
    }

    const NMiniKQL::IEngineFlat::TValidationInfo& GetKeysInfo() const override {
        if (WriteTx_) {
            Y_ABORT_UNLESS(WriteTx_->TxInfo().Loaded);
            return WriteTx_->TxInfo();
        }
        // For scheme tx global reader and writer flags should
        // result in all required dependencies.
        return TOperation::GetKeysInfo();
    }

    ui64 LockTxId() const override {
        return WriteTx_ ? WriteTx_->LockTxId() : 0;
    }

    ui32 LockNodeId() const override {
        return WriteTx_ ? WriteTx_->LockNodeId() : 0;
    }

    bool HasLockedWrites() const override {
        return WriteTx_ ? WriteTx_->HasLockedWrites() : false;
    }

    ui64 IncrementPageFaultCount() {
        return ++PageFaultCount;
    }

    const TValidatedWriteTx::TPtr& WriteTx() const { 
        return WriteTx_; 
    }
    TValidatedWriteTx::TPtr BuildWriteTx(TDataShard* self, TTransactionContext& txc, const TActorContext& ctx);

    void ClearWriteTx() { 
        WriteTx_ = nullptr; 
    }

    const NKikimrDataEvents::TEvWrite& Record() const {
        return Ev_->Get()->Record;
    }

    const std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>& WriteResult() const {
        return WriteResult_;
    }
    std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>&& WriteResult() {
        return std::move(WriteResult_);
    }

    void SetError(const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg);
    void SetWriteResult(std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>&& writeResult);

private:
    void TrackMemory() const;
    void UntrackMemory() const;

private:
    NEvents::TDataEvents::TEvWrite::TPtr Ev_;
    TValidatedWriteTx::TPtr WriteTx_;
    std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> WriteResult_;

    // TODO: move to persistent part of operation's flags
    ui64 ArtifactFlags;
    ui64 TxCacheUsage;
    ui64 ReleasedTxDataSize;
    ui64 SchemeShardId;
    ui64 SubDomainPathId;
    NKikimrSubDomains::TProcessingParams ProcessingParams;
    ui64 PageFaultCount = 0;
};

} // NDataShard
} // NKikimr
