#pragma once

#include "schemeshard.h"
#include "schemeshard_private.h"
#include "schemeshard_tx_infly.h"
#include "schemeshard_types.h"
#include "schemeshard__operation_side_effects.h"
#include "schemeshard__operation_memory_changes.h"
#include "schemeshard__operation_db_changes.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/blob_depot/events.h>

#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/filestore/core/filestore.h>
#include <ydb/services/bg_tasks/service.h>

#include <util/generic/ptr.h>
#include <util/generic/set.h>

#define SCHEMESHARD_INCOMING_EVENTS(action) \
    action(TEvHive::TEvCreateTabletReply,        NSchemeShard::TXTYPE_CREATE_TABLET_REPLY)               \
    action(TEvHive::TEvAdoptTabletReply,         NSchemeShard::TXTYPE_CREATE_TABLET_REPLY)               \
    action(TEvHive::TEvDeleteTabletReply,        NSchemeShard::TXTYPE_FREE_TABLET_RESULT)                \
    action(TEvHive::TEvDeleteOwnerTabletsReply,           NSchemeShard::TXTYPE_FREE_OWNER_TABLETS_RESULT)\
\
    action(TEvDataShard::TEvProposeTransactionResult,     NSchemeShard::TXTYPE_DATASHARD_PROPOSE_RESULT) \
    action(TEvDataShard::TEvSchemaChanged,       NSchemeShard::TXTYPE_DATASHARD_SCHEMA_CHANGED)          \
    action(TEvDataShard::TEvStateChanged,        NSchemeShard::TXTYPE_DATASHARD_STATE_RESULT)            \
    action(TEvDataShard::TEvInitSplitMergeDestinationAck, NSchemeShard::TXTYPE_INIT_SPLIT_DST_ACK)       \
    action(TEvDataShard::TEvSplitAck,            NSchemeShard::TXTYPE_SPLIT_ACK)                         \
    action(TEvDataShard::TEvSplitPartitioningChangedAck,  NSchemeShard::TXTYPE_SPLIT_PARTITIONING_CHANGED_DST_ACK) \
\
    action(TEvColumnShard::TEvProposeTransactionResult,   NSchemeShard::TXTYPE_COLUMNSHARD_PROPOSE_RESULT)              \
    action(NBackgroundTasks::TEvAddTaskResult,            NSchemeShard::TXTYPE_ADD_BACKGROUND_TASK_RESULT)              \
    action(TEvColumnShard::TEvNotifyTxCompletionResult,   NSchemeShard::TXTYPE_COLUMNSHARD_NOTIFY_TX_COMPLETION_RESULT) \
\
    action(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult,   NSchemeShard::TXTYPE_SEQUENCESHARD_CREATE_SEQUENCE_RESULT)   \
    action(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult,     NSchemeShard::TXTYPE_SEQUENCESHARD_DROP_SEQUENCE_RESULT)     \
    action(NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult,   NSchemeShard::TXTYPE_SEQUENCESHARD_UPDATE_SEQUENCE_RESULT)   \
    action(NSequenceShard::TEvSequenceShard::TEvFreezeSequenceResult,   NSchemeShard::TXTYPE_SEQUENCESHARD_FREEZE_SEQUENCE_RESULT)   \
    action(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult,  NSchemeShard::TXTYPE_SEQUENCESHARD_RESTORE_SEQUENCE_RESULT)  \
    action(NSequenceShard::TEvSequenceShard::TEvRedirectSequenceResult, NSchemeShard::TXTYPE_SEQUENCESHARD_REDIRECT_SEQUENCE_RESULT) \
\
    action(NReplication::TEvController::TEvCreateReplicationResult, NSchemeShard::TXTYPE_CREATE_REPLICATION_RESULT) \
    action(NReplication::TEvController::TEvDropReplicationResult,   NSchemeShard::TXTYPE_DROP_REPLICATION_RESULT)   \
\
    action(TEvSubDomain::TEvConfigureStatus,     NSchemeShard::TXTYPE_SUBDOMAIN_CONFIGURE_RESULT)        \
\
    action(TEvSchemeShard::TEvInitTenantSchemeShardResult,   NSchemeShard::TXTYPE_SUBDOMAIN_CONFIGURE_RESULT)  \
    action(TEvSchemeShard::TEvMigrateSchemeShardResult,      NSchemeShard::TXTYPE_SUBDOMAIN_MIGRATE_RESULT)    \
    action(TEvSchemeShard::TEvPublishTenantAsReadOnlyResult, NSchemeShard::TXTYPE_SUBDOMAIN_MIGRATE_RESULT)    \
    action(TEvPrivate::TEvCommitTenantUpdate,                    NSchemeShard::TXTYPE_SUBDOMAIN_MIGRATE_RESULT)    \
    action(TEvPrivate::TEvUndoTenantUpdate,                      NSchemeShard::TXTYPE_SUBDOMAIN_MIGRATE_RESULT)    \
    action(TEvSchemeShard::TEvPublishTenantResult,           NSchemeShard::TXTYPE_SUBDOMAIN_MIGRATE_RESULT)    \
    action(TEvSchemeShard::TEvRewriteOwnerResult,            NSchemeShard::TXTYPE_SUBDOMAIN_MIGRATE_RESULT)    \
    action(TEvDataShard::TEvMigrateSchemeShardResponse,          NSchemeShard::TXTYPE_SUBDOMAIN_MIGRATE_RESULT)    \
\
    action(TEvBlockStore::TEvUpdateVolumeConfigResponse,  NSchemeShard::TXTYPE_BLOCKSTORE_CONFIG_RESULT) \
    action(TEvFileStore::TEvUpdateConfigResponse,         NSchemeShard::TXTYPE_FILESTORE_CONFIG_RESULT)  \
    action(NKesus::TEvKesus::TEvSetConfigResult,          NSchemeShard::TXTYPE_KESUS_CONFIG_RESULT)      \
    action(TEvPersQueue::TEvDropTabletReply,              NSchemeShard::TXTYPE_DROP_TABLET_RESULT)       \
    action(TEvPersQueue::TEvUpdateConfigResponse,         NSchemeShard::TXTYPE_PERSQUEUE_CONFIG_RESULT)  \
    action(TEvBlobDepot::TEvApplyConfigResult,            NSchemeShard::TXTYPE_BLOB_DEPOT_CONFIG_RESULT) \
\
    action(TEvPrivate::TEvOperationPlan,                   NSchemeShard::TXTYPE_PLAN_STEP)                             \
    action(TEvPrivate::TEvPrivate::TEvCompletePublication, NSchemeShard::TXTYPE_NOTIFY_OPERATION_COMPLETE_PUBLICATION) \
    action(TEvPrivate::TEvPrivate::TEvCompleteBarrier,     NSchemeShard::TXTYPE_NOTIFY_OPERATION_COMPLETE_BARRIER)     \


namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard;

struct TOperationContext {
public:
    TSchemeShard* SS;
    const TActorContext& Ctx;
    TSideEffects& OnComplete;
    TMemoryChanges& MemChanges;
    TStorageChanges& DbChanges;

    TMaybe<NACLib::TUserToken> UserToken;
    bool IsAllowedPrivateTables = false;

private:
    NTabletFlatExecutor::TTransactionContext& Txc;
    bool ProtectDB = false;
    bool DirectAccessGranted = false;

public:
    TOperationContext(
            TSchemeShard* ss,
            NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx,
            TSideEffects& onComplete, TMemoryChanges& memChanges, TStorageChanges& dbChange,
            TMaybe<NACLib::TUserToken>&& userToken)
        : SS(ss)
        , Ctx(ctx)
        , OnComplete(onComplete)
        , MemChanges(memChanges)
        , DbChanges(dbChange)
        , UserToken(userToken)
        , Txc(txc)
    {}
    TOperationContext(
            TSchemeShard* ss,
            NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx,
            TSideEffects& onComplete, TMemoryChanges& memChanges, TStorageChanges& dbChange)
        : TOperationContext(ss, txc, ctx, onComplete, memChanges, dbChange, Nothing())
    {}

    NTable::TDatabase& GetDB() {
        Y_VERIFY_S(ProtectDB == false,
                 "there is attempt to write to the DB when it is protected,"
                 " in that case all writes should be done over TStorageChanges"
                 " in order to maintain revert the changes");
        DirectAccessGranted = true;
        return GetTxc().DB;
    }

    NTabletFlatExecutor::TTransactionContext& GetTxc() const {
        return Txc;
    }

    bool IsUndoChangesSafe() const {
        return !DirectAccessGranted;
    }

    class TDbGuard {
        bool PrevVal;
        bool& Protect;
    public:
        TDbGuard(TOperationContext& ctx)
            : PrevVal(ctx.ProtectDB)
            , Protect(ctx.ProtectDB)
        {
            Protect = true;
        }

        ~TDbGuard() {
            Protect = PrevVal;
        }
    };

    TDbGuard DbGuard() {
        return TDbGuard(*this);
    }
};

using TProposeRequest = NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction;
using TProposeResponse = NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult;
using TTxTransaction = NKikimrSchemeOp::TModifyScheme;

class IOperationBase {
public:
    virtual ~IOperationBase() = default;

#define DefaultDebugReply(TEvType, TxType) \
    static TString DebugReply(const TEvType::TPtr&);

    SCHEMESHARD_INCOMING_EVENTS(DefaultDebugReply)
#undef DefaultDebugReply

#define DefaultHandleReply(TEvType, ...) \
    virtual void HandleReply(TEvType::TPtr&, TOperationContext&);

    SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
#undef DefaultHandleReply

    virtual void ProgressState(TOperationContext& context) = 0;
};

class ISubOperationBase: public TSimpleRefCount<ISubOperationBase>, public IOperationBase {
public:
    using TPtr = TIntrusivePtr<ISubOperationBase>;

    virtual THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) = 0;

    // call it inside multipart operations after failed propose
    virtual void AbortPropose(TOperationContext& context) = 0;

    // call it only before execute ForceDrop operation for path
    virtual void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) = 0;

    // getters
    virtual const TOperationId& GetOperationId() const = 0;
    virtual const TTxTransaction& GetTransaction() const = 0;
};

class TSubOperationState {
private:
    TString LogHint;
    TSet<ui32> MsgToIgnore;

    virtual TString DebugHint() const = 0;

public:

    using TPtr = THolder<TSubOperationState>;
    virtual ~TSubOperationState() = default;

#define DefaultDebugReply(TEvType, TxType) \
    TString DebugReply(const TEvType::TPtr&);

    SCHEMESHARD_INCOMING_EVENTS(DefaultDebugReply)
#undef DefaultDebugReply

#define DefaultHandleReply(TEvType, ...)          \
    virtual bool HandleReply(TEvType::TPtr&, TOperationContext&);

    SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
#undef DefaultHandleReply

    void IgnoreMessages(TString debugHint, TSet<ui32> mgsIds);

    virtual bool ProgressState(TOperationContext& context) = 0;
};

class TSubOperationBase: public ISubOperationBase {
protected:
    const TOperationId OperationId;
    const TTxTransaction Transaction;

public:
    explicit TSubOperationBase(const TOperationId& id)
        : OperationId(id)
    {
    }

    explicit TSubOperationBase(const TOperationId& id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {
    }

    const TOperationId& GetOperationId() const override final {
        return OperationId;
    }

    const TTxTransaction& GetTransaction() const override final {
        return Transaction;
    }
};

class TSubOperation: public TSubOperationBase {
    TTxState::ETxState State = TTxState::Invalid;
    TSubOperationState::TPtr Base = nullptr;

protected:
    virtual TTxState::ETxState NextState(TTxState::ETxState state) const = 0;
    virtual TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) = 0;

    virtual void StateDone(TOperationContext& context) {
        auto state = NextState(GetState());
        SetState(state);

        if (state != TTxState::Invalid) {
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    using TSubOperationBase::TSubOperationBase;

    explicit TSubOperation(const TOperationId& id, TTxState::ETxState state)
        : TSubOperationBase(id)
        , State(state)
    {
    }

    TTxState::ETxState GetState() const {
        return State;
    }

    void SetState(TTxState::ETxState state) {
        State = state;
        Base = SelectStateFunc(state);
    }

    void ProgressState(TOperationContext& context) override {
        Y_VERIFY(Base);
        const bool isDone = Base->ProgressState(context);
        if (isDone) {
            StateDone(context);
        }
    }

    #define DefaultHandleReply(TEvType, ...)                                       \
        void HandleReply(TEvType::TPtr& ev, TOperationContext& context) override { \
            Y_VERIFY(Base);                                                        \
            bool isDone = Base->HandleReply(ev, context);                          \
            if (isDone) {                                                          \
                StateDone(context);                                                \
            }                                                                      \
        }

        SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
    #undef DefaultHandleReply
};

template <typename T>
ISubOperationBase::TPtr MakeSubOperation(const TOperationId& id) {
    return new T(id);
}

template <typename T, typename... Args>
ISubOperationBase::TPtr MakeSubOperation(const TOperationId& id, const TTxTransaction& tx, Args&&... args) {
    return new T(id, tx, std::forward<Args>(args)...);
}

template <typename T, typename... Args>
ISubOperationBase::TPtr MakeSubOperation(const TOperationId& id, TTxState::ETxState state, Args&&... args) {
    auto result = MakeHolder<T>(id, state, std::forward<Args>(args)...);
    result->SetState(state);
    return result.Release();
}

ISubOperationBase::TPtr CreateReject(TOperationId id, THolder<TProposeResponse> response);
ISubOperationBase::TPtr CreateReject(TOperationId id, NKikimrScheme::EStatus status, const TString& message);

ISubOperationBase::TPtr CreateMkDir(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateMkDir(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateRmDir(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateRmDir(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateModifyACL(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateModifyACL(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAlterUserAttrs(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterUserAttrs(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateForceDropUnsafe(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateForceDropUnsafe(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewTable(TOperationId id, const TTxTransaction& tx, const THashSet<TString>& localSequences = { });
ISubOperationBase::TPtr CreateNewTable(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateCopyTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateCopyTable(TOperationId id, TTxState::ETxState state);
TVector<ISubOperationBase::TPtr> CreateCopyTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);

ISubOperationBase::TPtr CreateAlterTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterTable(TOperationId id, TTxState::ETxState state);
TVector<ISubOperationBase::TPtr> CreateConsistentAlterTable(TOperationId id, const TTxTransaction& tx, TOperationContext& context);

ISubOperationBase::TPtr CreateSplitMerge(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateSplitMerge(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateDropTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropTable(TOperationId id, TTxState::ETxState state);

TVector<ISubOperationBase::TPtr> CreateBuildIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperationBase::TPtr> ApplyBuildIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperationBase::TPtr> CancelBuildIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);

TVector<ISubOperationBase::TPtr> CreateDropIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperationBase::TPtr CreateDropTableIndexAtMainTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropTableIndexAtMainTable(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateUpdateMainTableOnIndexMove(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateUpdateMainTableOnIndexMove(TOperationId id, TTxState::ETxState state);

/// CDC
// Create
TVector<ISubOperationBase::TPtr> CreateNewCdcStream(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperationBase::TPtr CreateNewCdcStreamImpl(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewCdcStreamImpl(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateNewCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool initialScan);
ISubOperationBase::TPtr CreateNewCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool initialScan);
// Alter
TVector<ISubOperationBase::TPtr> CreateAlterCdcStream(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperationBase::TPtr CreateAlterCdcStreamImpl(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterCdcStreamImpl(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateAlterCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool dropSnapshot);
ISubOperationBase::TPtr CreateAlterCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool dropSnapshot);
// Drop
TVector<ISubOperationBase::TPtr> CreateDropCdcStream(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperationBase::TPtr CreateDropCdcStreamImpl(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropCdcStreamImpl(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateDropCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool dropSnapshot);
ISubOperationBase::TPtr CreateDropCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool dropSnapshot);

ISubOperationBase::TPtr CreateBackup(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateBackup(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateRestore(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateRestore(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateTxCancelTx(TEvSchemeShard::TEvCancelTx::TPtr ev);

TVector<ISubOperationBase::TPtr> CreateIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperationBase::TPtr> CreateDropIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);

ISubOperationBase::TPtr CreateNewTableIndex(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewTableIndex(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateDropTableIndex(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropTableIndex(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateAlterTableIndex(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterTableIndex(TOperationId id, TTxState::ETxState state);

TVector<ISubOperationBase::TPtr> CreateConsistentCopyTables(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);

ISubOperationBase::TPtr CreateNewOlapStore(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewOlapStore(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateAlterOlapStore(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterOlapStore(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateDropOlapStore(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropOlapStore(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewColumnTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewColumnTable(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateAlterColumnTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterColumnTable(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateDropColumnTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropColumnTable(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewBSV(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewBSV(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAlterBSV(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterBSV(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAssignBSV(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAssignBSV(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateDropBSV(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropBSV(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewPQ(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewPQ(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAlterPQ(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterPQ(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateDropPQ(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropPQ(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAllocatePQ(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAllocatePQ(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateDeallocatePQ(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDeallocatePQ(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateSubDomain(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAlterSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterSubDomain(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateCompatibleSubdomainDrop(TSchemeShard* ss, TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateCompatibleSubdomainAlter(TSchemeShard* ss, TOperationId id, const TTxTransaction& tx);

ISubOperationBase::TPtr CreateUpgradeSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateUpgradeSubDomain(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateUpgradeSubDomainDecision(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateUpgradeSubDomainDecision(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateDropSubdomain(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropSubdomain(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateForceDropSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateForceDropSubDomain(TOperationId id, TTxState::ETxState state);


/// ExtSubDomain
// Create
ISubOperationBase::TPtr CreateExtSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateExtSubDomain(TOperationId id, TTxState::ETxState state);

// Alter
TVector<ISubOperationBase::TPtr> CreateCompatibleAlterExtSubDomain(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);
ISubOperationBase::TPtr CreateAlterExtSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterExtSubDomain(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateAlterExtSubDomainCreateHive(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterExtSubDomainCreateHive(TOperationId id, TTxState::ETxState state);

// Drop
ISubOperationBase::TPtr CreateForceDropExtSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateForceDropExtSubDomain(TOperationId id, TTxState::ETxState state);


ISubOperationBase::TPtr CreateNewKesus(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewKesus(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAlterKesus(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterKesus(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateDropKesus(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropKesus(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewRTMR(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewRTMR(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewSolomon(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewSolomon(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAlterSolomon(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterSolomon(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateDropSolomon(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropSolomon(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateInitializeBuildIndexMainTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateInitializeBuildIndexMainTable(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateInitializeBuildIndexImplTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateInitializeBuildIndexImplTable(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateFinalizeBuildIndexImplTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateFinalizeBuildIndexImplTable(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateFinalizeBuildIndexMainTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateFinalizeBuildIndexMainTable(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateLock(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateLock(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr DropLock(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr DropLock(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewFileStore(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewFileStore(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAlterFileStore(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterFileStore(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateDropFileStore(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropFileStore(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateAlterLogin(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterLogin(TOperationId id, TTxState::ETxState state);

TVector<ISubOperationBase::TPtr> CreateConsistentMoveTable(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperationBase::TPtr> CreateConsistentMoveIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);

ISubOperationBase::TPtr CreateMoveTable(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateMoveTable(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateMoveIndex(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateMoveIndex(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateMoveTableIndex(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateMoveTableIndex(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewSequence(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewSequence(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateDropSequence(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropSequence(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewReplication(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewReplication(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateDropReplication(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropReplication(TOperationId id, TTxState::ETxState state);

ISubOperationBase::TPtr CreateNewBlobDepot(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateNewBlobDepot(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateAlterBlobDepot(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateAlterBlobDepot(TOperationId id, TTxState::ETxState state);
ISubOperationBase::TPtr CreateDropBlobDepot(TOperationId id, const TTxTransaction& tx);
ISubOperationBase::TPtr CreateDropBlobDepot(TOperationId id, TTxState::ETxState state);

}
}
