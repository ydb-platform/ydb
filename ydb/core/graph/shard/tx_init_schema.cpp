#include "shard_impl.h"
#include "log.h"
#include "schema.h"

namespace NKikimr {
namespace NGraph {

class TTxInitSchema : public TTransactionBase<TGraphShard> {
public:
    TTxInitSchema(TGraphShard* shard)
        : TBase(shard)
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("TTxInitScheme::Execute");
        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();
        db.Table<Schema::State>().Key(TString("version")).Update<Schema::State::ValueUI64>(1);
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("TTxInitScheme::Complete");
        Self->ExecuteTxStartup();
    }
};

void TGraphShard::ExecuteTxInitSchema() {
    Execute(new TTxInitSchema(this));
}

} // NGraph
} // NKikimr

