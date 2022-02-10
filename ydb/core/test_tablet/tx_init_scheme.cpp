#include "test_shard_impl.h"
#include "scheme.h"

namespace NKikimr::NTestShard {

    class TTestShard::TTxInitScheme : public TTransactionBase<TTestShard> {
    public:
        TTxInitScheme(TTestShard *self)
            : TTransactionBase(self)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            Self->Execute(Self->CreateTxLoadEverything(), ctx);
        }
    };

    ITransaction *TTestShard::CreateTxInitScheme() {
        return new TTxInitScheme(this);
    }

} // NKikimr::NTestShard
