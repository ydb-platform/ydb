#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

/**
 * Base class for sub-operations that need proper context-aware state management.
 * This class provides common implementations for operations that:
 * 1. Always use SetState with context for proper state transitions
 * 2. Only use the context-aware SelectStateFunc overload
 * 
 * Derived classes should override SelectStateFunc(TTxState::ETxState, TOperationContext&)
 * instead of the non-context version.
 * 
 * For operations that need custom NextState logic with context, they should override
 * StateDone to call their custom NextState method.
 */
class TSubOperationWithContext : public TSubOperation {
protected:
    using TSubOperation::SelectStateFunc;
    using TSubOperation::NextState;
    /**
     * Properly handles state transitions by calling SetState with context
     * and activating the transaction when moving to a valid next state.
     * 
     * Derived classes can override this if they need custom StateDone logic.
     * 
     * @param context The operation context containing database and completion handlers
     */
    void StateDone(TOperationContext& context) override {
        if (GetState() == TTxState::Done) {
            return;
        }
        
        TTxState::ETxState nextState;
        nextState = NextState(GetState());
        
        SetState(nextState, context);
        
        if (nextState != TTxState::Invalid) {
            context.OnComplete.ActivateTx(OperationId);
        }
    }

    /**
     * Default implementation that prevents use of the non-context SelectStateFunc.
     * Derived classes should override SelectStateFunc(TTxState::ETxState, TOperationContext&)
     * instead of this method.
     * 
     * @param state The transaction state (unused)
     * @return Always aborts, forcing use of context-aware version
     */
    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState) override {
        Y_ABORT("Unreachable code: TSubOperationWithContext should only use context-aware SelectStateFunc");
    }

public:
    using TSubOperation::TSubOperation;
};

} // namespace NKikimr::NSchemeShard
