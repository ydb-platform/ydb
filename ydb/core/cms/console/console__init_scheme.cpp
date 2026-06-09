#include "console_impl.h"

namespace NKikimr::NConsole {

class TConsole::TTxInitScheme : public TTransactionBase<TConsole> {
public:
    TTxInitScheme(TConsole *self)
        : TBase(self)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TConsole::TTxInitScheme Execute");

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TConsole::TTxInitScheme Complete");

        Self->TxProcessor->ProcessTx(Self->CreateTxLoadState(), ctx);
        Self->TxProcessor->TxCompleted(this, ctx);
    }
};

ITransaction *TConsole::CreateTxInitScheme()
{
    return new TTxInitScheme(this);
}

} // namespace NKikimr::NConsole
