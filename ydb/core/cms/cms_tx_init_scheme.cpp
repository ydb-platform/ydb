#include "cms_impl.h"
#include "scheme.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

namespace NKikimr::NCms {

class TCms::TTxInitScheme : public TTransactionBase<TCms> {
public:
    TTxInitScheme(TCms *self)
        : TBase(self)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "TTxInitScheme Execute");

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "TTxInitScheme Complete");

        Self->Execute(Self->CreateTxLoadState(), ctx);
    }
};

ITransaction *TCms::CreateTxInitScheme() {
    return new TTxInitScheme(this);
}

} // namespace NKikimr::NCms
