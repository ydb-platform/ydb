#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

/**
 * Common operation state that simply proposes to the coordinator and completes.
 * Used for operations that don't need complex state management.
 */
class TEmptyPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TEmptyPropose, operationId " << OperationId << ", ";
    }

public:
    TEmptyPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            DebugHint() << "ProgressState, operation type " << TTxState::TypeName(txState->TxType));

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
    TString OperationName;
    TTxState::ETxState NextState;

    TString DebugHint() const override {
        return TStringBuilder()
                << OperationName << "::TWaitCopyTableBarrier"
                << " operationId: " << OperationId;
    }

public:
    TWaitCopyTableBarrier(TOperationId id, const TString& operationName = "TOperation", TTxState::ETxState nextState = TTxState::Done)
        : OperationId(id)
        , OperationName(operationName)
        , NextState(nextState)
    {
        IgnoreMessages(DebugHint(),
            { TEvHive::TEvCreateTabletReply::EventType
            , TEvDataShard::TEvProposeTransactionResult::EventType
            , TEvPrivate::TEvOperationPlan::EventType
            , TEvDataShard::TEvSchemaChanged::EventType }
        );
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        TTabletId ssId = context.SS->SelfTabletId();

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   DebugHint() << " HandleReply TEvPrivate::TEvCompleteBarrier"
                               << ", msg: " << ev->Get()->ToString()
                               << ", at tablet# " << ssId);

        NIceDb::TNiceDb db(context.GetDB());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        context.SS->ChangeTxState(db, OperationId, NextState);
        return true;
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                DebugHint() << "ProgressState, operation type "
                            << TTxState::TypeName(txState->TxType));

        context.OnComplete.Barrier(OperationId, "CopyTableBarrier");
        return false;
    }
};

} // namespace NKikimr::NSchemeShard
