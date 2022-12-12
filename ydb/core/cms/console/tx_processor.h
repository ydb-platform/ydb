#pragma once
#include "defs.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr::NConsole {

using NTabletFlatExecutor::ITransaction;

class ITxExecutor {
public:
    virtual void Execute(ITransaction *transaction, const TActorContext &ctx) = 0;
};

/**
 * Class to organize ordered transactions execution. Single
 * processor executes transactions in a strict order only one
 * at a time.
 *
 * Multiple processors may be linked into a tree. Each tree
 * node can start new transaction in case there is no parent
 * running tx or having tx in its queue and no child running
 * tx. Thus tree root executes global transactions which have
 * exclusive state ownership and never intersect with other
 * transactions. Leaf nodes refer to more local transactions
 * (e.g. transaction for some tenant) which may go in parallel.
 *
 * New processor may be created by simply calling
 * GetSubProcessor method. If processor is not going to have
 * more sub-processors and is likely to have no more transactions
 * in the nearest future then it is reasonable to mark it as
 * a temporary processor. Temporary processor is automatically
 * unlinked from parent processor once it has no transactions
 * to run and no live sub-processors.
 *
 * Console tablet uses following processors organization:
 *   'console'       - root permanent processor used for state
 *    |                load, tablet config updates
 *    |-'configs'    - permanent processor used for all config
 *    |                updates
 *    |-'tenants'    - permanent processor used for all tenant
 *                     transactions
 */
class TTxProcessor : public TThrRefBase {
private:
    enum class EState {
        // Can run transactions
        ACTIVE,
        // Going to be locked by parent but has running tx
        // or a sub-processor with running tx.
        LOCKING,
        // Cannot run transactions because parent is running
        // (or going to run) some tx.
        LOCKED_BY_PARENT,
        // Has nothing to do and passed control to
        // sub-processors.
        LOCKED_BY_CHILDREN,
    };

public:
    using TPtr = TIntrusivePtr<TTxProcessor>;

    TTxProcessor(ITxExecutor &executor,
                 const TString &name,
                 ui32 service,
                 TTxProcessor::TPtr parent = nullptr,
                 bool temporary = false);

    /**
     * Get existing or create a new sub-processor. Created
     * sub-processot becomes active if processor is locked
     * by children or is active and has no tx to run.
     */
    TTxProcessor::TPtr GetSubProcessor(const TString &name,
                                       const TActorContext &ctx,
                                       bool temporary = true,
                                       ui32 service = 0);
    /**
     * Enqueue next tx for execution. If processor is active
     * and runs no tx then start new transaction.
     */
    void ProcessTx(ITransaction *tx,
                   const TActorContext &ctx);
    /**
     * This method should be called by all transactions started
     * by this processor upon completion.
     */
    void TxCompleted(ITransaction *tx,
                     const TActorContext &ctx);

    /**
     * Clear processor of all sub-processors and transactions.
     */
    void Clear();

private:
    void RemoveSubProcessor(TTxProcessor::TPtr sub,
                            const TActorContext &ctx);

    /**
     * Switch into ACTIVE state and start next tx if any
     * is in queue.
     */
    void Activate(const TActorContext &ctx);
    /**
     * If there are sub-processors then switch into
     * LOCKED_BY_CHILDREN state and activate them.
     */
    void ActivateChildren(const TActorContext &ctx);
    /**
     * Stop tx execution. Called by parent to get rid of children
     * Lock. Return true if stopped and false otherwise.
     * If false is returned then call parent's Start method after
     * tx execution is stopped.
     */
    bool Lock(const TActorContext &ctx);
    /**
     * This method is called when processor is locked by children
     * but got something to run. Tries to lock children and become
     * active.
     */
    void TryToLockChildren(const TActorContext &ctx);
    /**
     * Try to continue tx execution. If processor is locked
     * by parent then it means parent unlocks it. If processor
     * is locked by children then it means some of children
     * became locked.
     */
    void Start(const TActorContext &ctx);

    /**
     * Check if we need to become active and all children are now locked.
     */
    void CheckActivation(const TActorContext &ctx);
    /**
     * Check if we need to be locked by parent and now may be locked.
     * If required and possible then change state and notify parent
     * via Start method call.
     */
    void CheckLocks(const TActorContext &ctx);
    /**
     * Check if this processor is temporary and it's time to unlink
     * (and die). Return true if unlinked from parent.
     */
    bool CheckTemporary(const TActorContext &ctx);
    /**
     * Process next tx from the queue if possible. If we are not
     * locked and there is nothing to
     */
    void ProcessNextTx(const TActorContext &ctx);

    ITxExecutor &Executor;
    TString Name;
    ui32 Service;
    EState State;
    bool Temporary;
    ITransaction *ActiveTx;
    TDeque<THolder<ITransaction>> TxQueue;
    TTxProcessor *Parent;
    TMap<TString, TTxProcessor::TPtr> SubProcessors;
    TString LogPrefix;
};

} // namespace NKikimr::NConsole
