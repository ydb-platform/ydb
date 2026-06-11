#include "shard_impl.h"
#include "log.h"
#include "schema.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRAPH

namespace NKikimr {
namespace NGraph {

class TTxInitSchema : public TTransactionBase<TGraphShard> {
public:
    TTxInitSchema(TGraphShard* shard)
        : TBase(shard)
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_INIT_SCHEMA; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxInitScheme::Execute",
            {"logPrefix", GetLogPrefix()});
        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<Schema>();
        db.Table<Schema::State>().Key(TString("version")).Update<Schema::State::ValueUI64>(1);
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("TTxInitScheme::Complete",
            {"logPrefix", GetLogPrefix()});
        Self->ExecuteTxStartup();
    }
};

void TGraphShard::ExecuteTxInitSchema() {
    Execute(new TTxInitSchema(this));
}

} // NGraph
} // NKikimr

