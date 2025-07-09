#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    struct TExDead {};

    using TTokens = std::vector<std::weak_ptr<TToken>>;

    class TCoroTx : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
    public:
        class TContextBase {
        protected:
            TTransactionContext *TxContext = nullptr;

        public:
            NTabletFlatExecutor::TTransactionContext *GetTxc();
            void FinishTx(); // finish this transaction; function returns on Complete() entry
            void RestartTx(); // restart transaction; function returns on next Execute() entry
            void RunSuccessorTx(); // restart in new transaction -- called after FinishTx()
            NTabletFlatExecutor::TTransactionContext& operator *() { return *GetTxc(); }
        };

    private:
        class TContext;
        std::unique_ptr<TContext> Context;

    public:
        TCoroTx(TBlobDepot *self, TTokens&& tokens, std::function<void(TContextBase&)> body);
        TCoroTx(TCoroTx& predecessor);
        ~TCoroTx();

    private:
        bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override;
        void Complete(const TActorContext&) override;
    };

} // NKikimr::NBlobDepot
