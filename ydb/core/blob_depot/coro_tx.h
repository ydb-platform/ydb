#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    struct TExDead {};

    using TTokens = std::vector<std::weak_ptr<TToken>>;

    class TCoroTx : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        class TContext;
        std::unique_ptr<TContext> Context;
        TTransactionContext *TxContext = nullptr;
        static thread_local TCoroTx *Current;

    public:
        TCoroTx(TBlobDepot *self, TTokens&& tokens, std::function<void()> body);
        TCoroTx(TCoroTx& predecessor);
        ~TCoroTx();

    private:
        bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override;
        void Complete(const TActorContext&) override;

    public:
        static NTabletFlatExecutor::TTransactionContext *GetTxc();
        static TCoroTx *CurrentTx(); // obtain pointer to current tx
        static void FinishTx(); // finish this transaction; function returns on Complete() entry
        static void RestartTx(); // restart transaction; function returns on next Execute() entry
        static void RunSuccessorTx(); // restart in new transaction -- called after FinishTx()
    };

} // NKikimr::NBlobDepot
