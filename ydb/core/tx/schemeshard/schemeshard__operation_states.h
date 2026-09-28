#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

/**
 * Common operation state that simply proposes to the coordinator and completes.
 * Used for operations that don't need complex state management.
 */
class TEmptyPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    virtual const char* Name() const override final { return "TEmptyPropose"; }

public:
    TEmptyPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages({});
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        YDB_LOG_INFO_CTX(context.Ctx, "",
            {"txType", TTxState::TypeName(txState->TxType)},
        );

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));

        return true;
    }
};

/**
 * Common operation state that waits for a copy table barrier to complete.
 * Used for operations that need to wait for copy table operations to finish.
 */
class TWaitCopyTableBarrier: public TSubOperationState {
private:
    TOperationId OperationId;
    TTxState::ETxState NextState;

    virtual const char* Name() const override final { return "TWaitCopyTableBarrier"; }

public:
    TWaitCopyTableBarrier(TOperationId id, TTxState::ETxState nextState = TTxState::Done)
        : OperationId(id)
        , NextState(nextState)
    {
        IgnoreMessages({ TEvHive::TEvCreateTabletReply::EventType
            , TEvDataShard::TEvProposeTransactionResult::EventType
            , TEvPrivate::TEvOperationPlan::EventType
            , TEvDataShard::TEvSchemaChanged::EventType }
        );
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        YDB_LOG_INFO_CTX(context.Ctx, "",
            {"msg", ev->Get()->ToString()},
        );

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        context.SS->ChangeTxState(db, OperationId, NextState);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        YDB_LOG_INFO_CTX(context.Ctx, "",
            {"txType", TTxState::TypeName(txState->TxType)},
        );

        context.OnComplete.Barrier(OperationId, "CopyTableBarrier");
        return false;
    }
};

} // namespace NKikimr::NSchemeShard

#undef YDB_LOG_THIS_FILE_COMPONENT
