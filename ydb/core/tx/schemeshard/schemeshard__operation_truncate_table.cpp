#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/base/auth.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/subdomain.h>


namespace {

using namespace NKikimr;
using namespace NSchemeShard;

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TTruncateTable TConfigureParts"
            << " operationId# " << OperationId;
    }

public:
    TConfigureParts(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
    }

    bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
        Y_UNUSED(ev);
        Y_UNUSED(context);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        Y_UNUSED(context);
        return false;
    }
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "TTruncateTable TPropose"
            << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {TEvDataShard::TEvProposeTransactionResult::EventType});
    }

    bool HandleReply(TEvDataShard::TEvSchemaChanged::TPtr& ev, TOperationContext& context) override {
        Y_UNUSED(ev);
        Y_UNUSED(context);
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        Y_UNUSED(ev);
        Y_UNUSED(context);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        Y_UNUSED(context);
        return false;
    }
};


class TTruncateTable: public TSubOperation {
public:
    using TSubOperation::TSubOperation;
    static TTxState::ETxState NextState() {
        return TTxState::ConfigureParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::ProposedWaitParts;
        case TTxState::ProposedWaitParts:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::ProposedWaitParts:
            return MakeHolder<NTableState::TProposedWaitParts>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        THolder<TProposeResponse> result;
        result.Reset(new TEvSchemeShard::TEvModifySchemeTransactionResult(
            NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(ssId)));
        TString errStr;

        const auto& op = Transaction.GetTruncateTable();
        const auto stringTablePath = NKikimr::JoinPath({Transaction.GetWorkingDir(), op.GetTableName()});
        TPath tablePath = TPath::Resolve(stringTablePath, context.SS);
         {
            if (tablePath->IsColumnTable()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "Column tables don`t support TRUNCATE TABLE");
                return result;
            }

            if (!tablePath->IsTable()) {
                result->SetError(NKikimrScheme::StatusPreconditionFailed, "You can use TRUNCATE TABLE only on tables");
                return result;
            }

            TPath::TChecker checks = tablePath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsResolved()
                .NotDeleted()
                .NotBackupTable()
                .NotAsyncReplicaTable()
                .NotUnderTheSameOperation(OperationId.GetTxId())
                .NotUnderOperation();
                // TODO flown4qqqq (need add check on cdc streams)

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        if (!context.SS->CheckLocks(tablePath.Base()->PathId, Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusMultipleModifications, errStr);
            return result;
        }

        // TODO flown4qqqq (need persist the op)

        result->SetPathId(tablePath.Base()->PathId.LocalPathId);

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TTruncateTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        Y_UNUSED(forceDropTxId);
        Y_UNUSED(context);
        Y_ABORT("no AbortUnsafe for TTruncateTable");
    }
};

}

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateTruncateTable(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TTruncateTable>(id, tx);
}

ISubOperation::TPtr CreateTruncateTable(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state == TTxState::Invalid || state == TTxState::Propose);
    return MakeSubOperation<TTruncateTable>(id);
}

}
