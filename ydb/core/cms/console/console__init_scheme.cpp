#include "console_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

namespace NKikimr::NConsole {

class TConsole::TTxInitScheme : public TTransactionBase<TConsole> {
public:
    TTxInitScheme(TConsole *self)
        : TBase(self)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        YDB_LOG_CTX_DEBUG(ctx, "TConsole::TTxInitScheme Execute");

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        YDB_LOG_CTX_DEBUG(ctx, "TConsole::TTxInitScheme Complete");

        Self->TxProcessor->ProcessTx(Self->CreateTxLoadState(), ctx);
        Self->TxProcessor->TxCompleted(this, ctx);
    }
};

ITransaction *TConsole::CreateTxInitScheme()
{
    return new TTxInitScheme(this);
}

} // namespace NKikimr::NConsole
