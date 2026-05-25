#include "mediator_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_MEDIATOR

namespace NKikimr {
namespace NTxMediator {

using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

struct TTxMediator::TTxSchema : public TTransactionBase<TTxMediator> {
    TTxSchema(TSelf *mediator)
        : TBase(mediator)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        YDB_LOG_CTX_INFO(ctx, "TTxSchema Complete",
            {"tablet", Self->TabletID()});
        Self->Execute(Self->CreateTxUpgrade(), ctx);
    }
};

ITransaction* TTxMediator::CreateTxSchema() {
    return new TTxSchema(this);
}

}
}
