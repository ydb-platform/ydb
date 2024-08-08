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

#include <util/generic/ptr.h>
#include <util/generic/set.h>

#define SCHEMESHARD_INCOMING_EVENTS(action) \
    action(TEvHive::TEvCreateTabletReply,        NSchemeShard::TXTYPE_CREATE_TABLET_REPLY)               \
    action(TEvHive::TEvAdoptTabletReply,         NSchemeShard::TXTYPE_CREATE_TABLET_REPLY)               \
    action(TEvHive::TEvDeleteTabletReply,        NSchemeShard::TXTYPE_FREE_TABLET_RESULT)                \
    action(TEvHive::TEvDeleteOwnerTabletsReply,           NSchemeShard::TXTYPE_FREE_OWNER_TABLETS_RESULT)\
    action(TEvHive::TEvUpdateTabletsObjectReply, NSchemeShard::TXTYPE_CREATE_TABLET_REPLY)               \
    action(TEvHive::TEvUpdateDomainReply,        NSchemeShard::TXTYPE_UPDATE_DOMAIN_REPLY)               \
\
    action(TEvDataShard::TEvProposeTransactionResult,     NSchemeShard::TXTYPE_DATASHARD_PROPOSE_RESULT) \
    action(TEvDataShard::TEvSchemaChanged,       NSchemeShard::TXTYPE_DATASHARD_SCHEMA_CHANGED)          \
    action(TEvDataShard::TEvStateChanged,        NSchemeShard::TXTYPE_DATASHARD_STATE_RESULT)            \
    action(TEvDataShard::TEvInitSplitMergeDestinationAck, NSchemeShard::TXTYPE_INIT_SPLIT_DST_ACK)       \
    action(TEvDataShard::TEvSplitAck,            NSchemeShard::TXTYPE_SPLIT_ACK)                         \
    action(TEvDataShard::TEvSplitPartitioningChangedAck,  NSchemeShard::TXTYPE_SPLIT_PARTITIONING_CHANGED_DST_ACK) \
\
    action(TEvColumnShard::TEvProposeTransactionResult,   NSchemeShard::TXTYPE_COLUMNSHARD_PROPOSE_RESULT)              \
    action(TEvColumnShard::TEvNotifyTxCompletionResult,   NSchemeShard::TXTYPE_COLUMNSHARD_NOTIFY_TX_COMPLETION_RESULT) \
\
    action(NSequenceShard::TEvSequenceShard::TEvCreateSequenceResult,   NSchemeShard::TXTYPE_SEQUENCESHARD_CREATE_SEQUENCE_RESULT)   \
    action(NSequenceShard::TEvSequenceShard::TEvDropSequenceResult,     NSchemeShard::TXTYPE_SEQUENCESHARD_DROP_SEQUENCE_RESULT)     \
    action(NSequenceShard::TEvSequenceShard::TEvUpdateSequenceResult,   NSchemeShard::TXTYPE_SEQUENCESHARD_UPDATE_SEQUENCE_RESULT)   \
    action(NSequenceShard::TEvSequenceShard::TEvFreezeSequenceResult,   NSchemeShard::TXTYPE_SEQUENCESHARD_FREEZE_SEQUENCE_RESULT)   \
    action(NSequenceShard::TEvSequenceShard::TEvRestoreSequenceResult,  NSchemeShard::TXTYPE_SEQUENCESHARD_RESTORE_SEQUENCE_RESULT)  \
    action(NSequenceShard::TEvSequenceShard::TEvRedirectSequenceResult, NSchemeShard::TXTYPE_SEQUENCESHARD_REDIRECT_SEQUENCE_RESULT) \
    action(NSequenceShard::TEvSequenceShard::TEvGetSequenceResult, NSchemeShard::TXTYPE_SEQUENCESHARD_GET_SEQUENCE_RESULT) \
\
    action(NReplication::TEvController::TEvCreateReplicationResult, NSchemeShard::TXTYPE_CREATE_REPLICATION_RESULT) \
    action(NReplication::TEvController::TEvAlterReplicationResult,  NSchemeShard::TXTYPE_ALTER_REPLICATION_RESULT)  \
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
    action(TEvPersQueue::TEvProposeTransactionResult,     NSchemeShard::TXTYPE_PERSQUEUE_PROPOSE_RESULT) \
    action(TEvBlobDepot::TEvApplyConfigResult,            NSchemeShard::TXTYPE_BLOB_DEPOT_CONFIG_RESULT) \
\
    action(TEvPrivate::TEvOperationPlan,                   NSchemeShard::TXTYPE_PLAN_STEP)                             \
    action(TEvPrivate::TEvPrivate::TEvCompletePublication, NSchemeShard::TXTYPE_NOTIFY_OPERATION_COMPLETE_PUBLICATION) \
    action(TEvPrivate::TEvPrivate::TEvCompleteBarrier,     NSchemeShard::TXTYPE_NOTIFY_OPERATION_COMPLETE_BARRIER)     \
\
    action(TEvPersQueue::TEvProposeTransactionAttachResult, NSchemeShard::TXTYPE_PERSQUEUE_PROPOSE_ATTACH_RESULT)


namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard;
class TPath;

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

class ISubOperationState {
public:
    virtual ~ISubOperationState() = default;

    template <EventBasePtr TEvPtr>
    static TString DebugReply(const TEvPtr& ev);

#define DefaultHandleReply(TEvType, ...) \
    virtual bool HandleReply(TEvType::TPtr& ev, TOperationContext& context);

    SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
#undef DefaultHandleReply

    virtual bool ProgressState(TOperationContext& context) = 0;
};

class TSubOperationState: public ISubOperationState {
    TString LogHint;
    TSet<ui32> MsgToIgnore;

    virtual TString DebugHint() const = 0;

public:
    using TPtr = THolder<TSubOperationState>;

#define DefaultHandleReply(TEvType, ...) \
    bool HandleReply(TEvType::TPtr& ev, TOperationContext& context) override;

    SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
#undef DefaultHandleReply

    void IgnoreMessages(TString debugHint, TSet<ui32> mgsIds);
};

class ISubOperation: public TSimpleRefCount<ISubOperation>, public ISubOperationState {
public:
    using TPtr = TIntrusivePtr<ISubOperation>;

    virtual THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) = 0;

    // call it inside multipart operations after failed propose
    virtual void AbortPropose(TOperationContext& context) = 0;

    // call it only before execute ForceDrop operation for path
    virtual void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) = 0;

    // getters
    virtual const TOperationId& GetOperationId() const = 0;
    virtual const TTxTransaction& GetTransaction() const = 0;
};

class TSubOperationBase: public ISubOperation {
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
    TSubOperationState::TPtr StateFunc = nullptr;

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
        StateFunc = SelectStateFunc(state);
    }

    bool ProgressState(TOperationContext& context) override {
        return Progress(context, &ISubOperationState::ProgressState, context);
    }

    #define DefaultHandleReply(TEvType, ...) \
        bool HandleReply(TEvType::TPtr& ev, TOperationContext& context) override;

        SCHEMESHARD_INCOMING_EVENTS(DefaultHandleReply)
    #undef DefaultHandleReply

private:
    template <typename... Args>
    using TFunc = bool(ISubOperationState::*)(Args...);

    template <typename... Args>
    bool Progress(TOperationContext& context, TFunc<Args...> func, Args&&... args) {
        Y_ABORT_UNLESS(StateFunc);
        const bool isDone = std::invoke(func, StateFunc.Get(), std::forward<Args>(args)...);
        if (isDone) {
            StateDone(context);
        }

        return true;
    }
};

template <typename T>
ISubOperation::TPtr MakeSubOperation(const TOperationId& id) {
    return new T(id);
}

template <typename T, typename... Args>
ISubOperation::TPtr MakeSubOperation(const TOperationId& id, const TTxTransaction& tx, Args&&... args) {
    return new T(id, tx, std::forward<Args>(args)...);
}

template <typename T, typename... Args>
ISubOperation::TPtr MakeSubOperation(const TOperationId& id, TTxState::ETxState state, Args&&... args) {
    auto result = MakeHolder<T>(id, state, std::forward<Args>(args)...);
    result->SetState(state);
    return result.Release();
}

ISubOperation::TPtr CreateReject(TOperationId id, THolder<TProposeResponse> response);
ISubOperation::TPtr CreateReject(TOperationId id, NKikimrScheme::EStatus status, const TString& message);

ISubOperation::TPtr CreateMkDir(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateMkDir(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateRmDir(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateRmDir(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateModifyACL(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateModifyACL(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAlterUserAttrs(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterUserAttrs(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateForceDropUnsafe(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateForceDropUnsafe(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewTable(TOperationId id, const TTxTransaction& tx, const THashSet<TString>& localSequences = { });
ISubOperation::TPtr CreateNewTable(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateCopyTable(TOperationId id, const TTxTransaction& tx,
    const THashSet<TString>& localSequences = { });
ISubOperation::TPtr CreateCopyTable(TOperationId id, TTxState::ETxState state);
TVector<ISubOperation::TPtr> CreateCopyTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);

ISubOperation::TPtr CreateAlterTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterTable(TOperationId id, TTxState::ETxState state);
TVector<ISubOperation::TPtr> CreateConsistentAlterTable(TOperationId id, const TTxTransaction& tx, TOperationContext& context);

ISubOperation::TPtr CreateSplitMerge(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateSplitMerge(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateDropTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropTable(TOperationId id, TTxState::ETxState state);

TVector<ISubOperation::TPtr> CreateBuildOrCheckColumn(TOperationId id, const TTxTransaction& tx, TOperationContext& context);

TVector<ISubOperation::TPtr> CreateBuildIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperation::TPtr> ApplyBuildIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperation::TPtr> CancelBuildIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);

TVector<ISubOperation::TPtr> CreateDropIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperation::TPtr CreateDropTableIndexAtMainTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropTableIndexAtMainTable(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateUpdateMainTableOnIndexMove(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateUpdateMainTableOnIndexMove(TOperationId id, TTxState::ETxState state);

// External Table
// Create
TVector<ISubOperation::TPtr> CreateNewExternalTable(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperation::TPtr CreateNewExternalTable(TOperationId id, TTxState::ETxState state);
// Alter
ISubOperation::TPtr CreateAlterExternalTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterExternalTable(TOperationId id, TTxState::ETxState state);
// Drop
ISubOperation::TPtr CreateDropExternalTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropExternalTable(TOperationId id, TTxState::ETxState state);

// External Data Source
// Create
TVector<ISubOperation::TPtr> CreateNewExternalDataSource(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperation::TPtr CreateNewExternalDataSource(TOperationId id, TTxState::ETxState state);
// Alter
ISubOperation::TPtr CreateAlterExternalDataSource(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterExternalDataSource(TOperationId id, TTxState::ETxState state);
// Drop
ISubOperation::TPtr CreateDropExternalDataSource(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropExternalDataSource(TOperationId id, TTxState::ETxState state);

// View
// Create
ISubOperation::TPtr CreateNewView(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewView(TOperationId id, TTxState::ETxState state);
// Drop
ISubOperation::TPtr CreateDropView(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropView(TOperationId id, TTxState::ETxState state);

/// CDC
// Create
TVector<ISubOperation::TPtr> CreateNewCdcStream(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperation::TPtr CreateNewCdcStreamImpl(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewCdcStreamImpl(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateNewCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool initialScan);
ISubOperation::TPtr CreateNewCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool initialScan);
// Alter
TVector<ISubOperation::TPtr> CreateAlterCdcStream(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperation::TPtr CreateAlterCdcStreamImpl(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterCdcStreamImpl(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateAlterCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool dropSnapshot);
ISubOperation::TPtr CreateAlterCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool dropSnapshot);
// Drop
TVector<ISubOperation::TPtr> CreateDropCdcStream(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
ISubOperation::TPtr CreateDropCdcStreamImpl(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropCdcStreamImpl(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateDropCdcStreamAtTable(TOperationId id, const TTxTransaction& tx, bool dropSnapshot);
ISubOperation::TPtr CreateDropCdcStreamAtTable(TOperationId id, TTxState::ETxState state, bool dropSnapshot);

/// Continuous Backup
// Create
TVector<ISubOperation::TPtr> CreateNewContinuousBackup(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperation::TPtr> CreateAlterContinuousBackup(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperation::TPtr> CreateDropContinuousBackup(TOperationId id, const TTxTransaction& tx, TOperationContext& context);

ISubOperation::TPtr CreateBackup(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateBackup(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateRestore(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateRestore(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateTxCancelTx(TEvSchemeShard::TEvCancelTx::TPtr ev);

TVector<ISubOperation::TPtr> CreateIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperation::TPtr> CreateDropIndexedTable(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);

ISubOperation::TPtr CreateNewTableIndex(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewTableIndex(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateDropTableIndex(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropTableIndex(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateAlterTableIndex(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterTableIndex(TOperationId id, TTxState::ETxState state);

TVector<ISubOperation::TPtr> CreateConsistentCopyTables(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);

ISubOperation::TPtr CreateNewOlapStore(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewOlapStore(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateAlterOlapStore(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterOlapStore(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateDropOlapStore(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropOlapStore(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewColumnTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewColumnTable(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateAlterColumnTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterColumnTable(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateDropColumnTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropColumnTable(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewBSV(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewBSV(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAlterBSV(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterBSV(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAssignBSV(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAssignBSV(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateDropBSV(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropBSV(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewPQ(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewPQ(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAlterPQ(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterPQ(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateDropPQ(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropPQ(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAllocatePQ(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAllocatePQ(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateDeallocatePQ(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDeallocatePQ(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateSubDomain(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAlterSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterSubDomain(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateCompatibleSubdomainDrop(TSchemeShard* ss, TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateCompatibleSubdomainAlter(TSchemeShard* ss, TOperationId id, const TTxTransaction& tx);

ISubOperation::TPtr CreateUpgradeSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateUpgradeSubDomain(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateUpgradeSubDomainDecision(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateUpgradeSubDomainDecision(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateDropSubdomain(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropSubdomain(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateForceDropSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateForceDropSubDomain(TOperationId id, TTxState::ETxState state);


/// ExtSubDomain
// Create
ISubOperation::TPtr CreateExtSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateExtSubDomain(TOperationId id, TTxState::ETxState state);

// Alter
TVector<ISubOperation::TPtr> CreateCompatibleAlterExtSubDomain(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context);
ISubOperation::TPtr CreateAlterExtSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterExtSubDomain(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateAlterExtSubDomainCreateHive(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterExtSubDomainCreateHive(TOperationId id, TTxState::ETxState state);

// Drop
ISubOperation::TPtr CreateForceDropExtSubDomain(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateForceDropExtSubDomain(TOperationId id, TTxState::ETxState state);


ISubOperation::TPtr CreateNewKesus(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewKesus(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAlterKesus(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterKesus(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateDropKesus(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropKesus(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewRTMR(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewRTMR(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewSolomon(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewSolomon(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAlterSolomon(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterSolomon(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateDropSolomon(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropSolomon(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateInitializeBuildIndexMainTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateInitializeBuildIndexMainTable(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateInitializeBuildIndexImplTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateInitializeBuildIndexImplTable(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateFinalizeBuildIndexImplTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateFinalizeBuildIndexImplTable(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateFinalizeBuildIndexMainTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateFinalizeBuildIndexMainTable(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateLock(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateLock(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr DropLock(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr DropLock(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewFileStore(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewFileStore(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAlterFileStore(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterFileStore(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateDropFileStore(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropFileStore(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateAlterLogin(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterLogin(TOperationId id, TTxState::ETxState state);

TVector<ISubOperation::TPtr> CreateConsistentMoveTable(TOperationId id, const TTxTransaction& tx, TOperationContext& context);
TVector<ISubOperation::TPtr> CreateConsistentMoveIndex(TOperationId id, const TTxTransaction& tx, TOperationContext& context);

ISubOperation::TPtr CreateMoveTable(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateMoveTable(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateMoveIndex(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateMoveIndex(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateMoveTableIndex(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateMoveTableIndex(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewSequence(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewSequence(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateDropSequence(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropSequence(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateCopySequence(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateCopySequence(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateAlterSequence(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterSequence(TOperationId id, TTxState::ETxState state);

ISubOperation::TPtr CreateNewReplication(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewReplication(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateAlterReplication(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterReplication(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateDropReplication(TOperationId id, const TTxTransaction& tx, bool cascade);
ISubOperation::TPtr CreateDropReplication(TOperationId id, TTxState::ETxState state, bool cascade);

ISubOperation::TPtr CreateNewBlobDepot(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewBlobDepot(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateAlterBlobDepot(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterBlobDepot(TOperationId id, TTxState::ETxState state);
ISubOperation::TPtr CreateDropBlobDepot(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropBlobDepot(TOperationId id, TTxState::ETxState state);

// Resource Pool
// Create
ISubOperation::TPtr CreateNewResourcePool(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateNewResourcePool(TOperationId id, TTxState::ETxState state);
// Alter
ISubOperation::TPtr CreateAlterResourcePool(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateAlterResourcePool(TOperationId id, TTxState::ETxState state);
// Drop
ISubOperation::TPtr CreateDropResourcePool(TOperationId id, const TTxTransaction& tx);
ISubOperation::TPtr CreateDropResourcePool(TOperationId id, TTxState::ETxState state);

// returns Reject in case of error, nullptr otherwise
ISubOperation::TPtr CascadeDropTableChildren(TVector<ISubOperation::TPtr>& result, const TOperationId& id, const TPath& table);

TVector<ISubOperation::TPtr> CreateRestoreIncrementalBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context);

}
}
