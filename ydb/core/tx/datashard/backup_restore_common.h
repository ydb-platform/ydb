#pragma once

#include "defs.h"

#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit.h"

#include <ydb/core/base/row_version.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NDataShard {

template <typename TEvCancel>
class TBackupRestoreUnitBase : public TExecutionUnit {
protected:
    using TBase = TBackupRestoreUnitBase<TEvCancel>;

    virtual bool IsRelevant(TActiveTransaction* tx) const = 0;

    virtual bool IsWaiting(TOperation::TPtr op) const = 0;
    virtual void SetWaiting(TOperation::TPtr op) = 0;
    virtual void ResetWaiting(TOperation::TPtr op) = 0;
    virtual bool Run(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) = 0;

    virtual bool HasResult(TOperation::TPtr op) const = 0;
    virtual bool ProcessResult(TOperation::TPtr op, const TActorContext& ctx) = 0;

    virtual void Cancel(TActiveTransaction* tx, const TActorContext& ctx) = 0;

    void Abort(TOperation::TPtr op, const TActorContext& ctx, const TString& error) {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, error);

        BuildResult(op)->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, error);
        ResetWaiting(op);

        Cancel(tx, ctx);
    }

private:
    void ProcessEvent(TAutoPtr<NActors::IEventHandle>& ev, TOperation::TPtr op, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            OHFunc(TEvCancel, Handle);
        }
    }

    void Handle(typename TEvCancel::TPtr&, TOperation::TPtr op, const TActorContext& ctx) {
        if (IsWaiting(op)) {
            Abort(op, ctx, TStringBuilder() << "Interrupted " << GetKind() << " operation " << *op
                << " while waiting to finish at " << DataShard.TabletID());
        }
    }

    void PersistResult(TOperation::TPtr op, TTransactionContext& txc) {
        auto* schemeOp = DataShard.FindSchemaTx(op->GetTxId());
        Y_ABORT_UNLESS(schemeOp);

        NIceDb::TNiceDb db(txc.DB);
        DataShard.PersistSchemeTxResult(db, *schemeOp);
    }

public:
    TBackupRestoreUnitBase(EExecutionUnitKind kind, TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(kind, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override final {
        if (!IsWaiting(op)) {
            return true;
        }

        if (HasResult(op)) {
            return true;
        }

        if (op->HasPendingInputEvents()) {
            return true;
        }

        return false;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override final {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        if (!IsRelevant(tx)) {
            return EExecutionStatus::Executed;
        }

        if (!IsWaiting(op)) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Starting a " << GetKind() << " operation"
                << " at " << DataShard.TabletID());

            if (!Run(op, txc, ctx)) {
                return EExecutionStatus::Executed;
            }

            SetWaiting(op);
            Y_DEBUG_ABORT_UNLESS(!HasResult(op));
        }

        if (HasResult(op)) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "" << GetKind() << " complete"
                << " at " << DataShard.TabletID());

            ResetWaiting(op);
            if (ProcessResult(op, ctx)) {
                PersistResult(op, txc);
            } else {
                Y_DEBUG_ABORT_UNLESS(!HasResult(op));
                op->SetWaitingForRestartFlag();
                ctx.Schedule(TDuration::Seconds(1), new TDataShard::TEvPrivate::TEvRestartOperation(op->GetTxId()));
            }
        }

        while (op->HasPendingInputEvents()) {
            ProcessEvent(op->InputEvents().front(), op, ctx);
            op->InputEvents().pop();
        }

        if (IsWaiting(op)) {
            return EExecutionStatus::Continue;
        }

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override final {
    }

}; // TBackupRestoreUnitBase

namespace NBackupRestore {

using TVirtualTimestamp = TRowVersion;

enum class EStorageType {
    YT,
    S3,
};

struct TLogMetadata : TSimpleRefCount<TLogMetadata> {
    using TPtr = TIntrusivePtr<TLogMetadata>;

    const TVirtualTimestamp StartVts;
    TString ConsistencyKey;
    EStorageType StorageType;
    TString StoragePath;
};

struct TFullBackupMetadata : TSimpleRefCount<TFullBackupMetadata> {
    using TPtr = TIntrusivePtr<TFullBackupMetadata>;

    const TVirtualTimestamp SnapshotVts;
    TString ConsistencyKey;
    TLogMetadata::TPtr FollowingLog;
    EStorageType StorageType;
    TString StoragePath;
};

class TMetadata {
public:
    TMetadata() = default;
    TMetadata(TVector<TFullBackupMetadata::TPtr>&& fullBackups, TVector<TLogMetadata::TPtr>&& logs);

    void AddFullBackup(TFullBackupMetadata::TPtr fullBackup);
    void AddLog(TLogMetadata::TPtr log);
    void SetConsistencyKey(const TString& key);

    TString Serialize() const;
    static TMetadata Deserialize(const TString& metadata);
private:
    TString ConsistencyKey;
    TMap<TVirtualTimestamp, TFullBackupMetadata::TPtr> FullBackups;
    TMap<TVirtualTimestamp, TLogMetadata::TPtr> Logs;
};

} // NBackupRestore

} // NDataShard
} // NKikimr
