#include "shard_impl.h"
#include "log.h"
#include "schema.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRAPH

namespace NKikimr {
namespace NGraph {

class TTxChangeBackend : public TTransactionBase<TGraphShard> {
private:
    EBackendType Backend;
public:
    TTxChangeBackend(TGraphShard* shard, EBackendType backend)
        : TBase(shard)
        , Backend(backend)
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_CHANGE_BACKEND; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxChangeBackend::Execute",
            {"logPrefix", GetLogPrefix()},
            {"backend", static_cast<ui64>(Backend)});
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::State>().Key(TString("backend")).Update<Schema::State::ValueUI64>(static_cast<ui64>(Backend));
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("TTxChangeBackend::Complete",
            {"logPrefix", GetLogPrefix()});
    }
};

void TGraphShard::ExecuteTxChangeBackend(EBackendType backend) {
    Execute(new TTxChangeBackend(this, backend));
}

} // NGraph
} // NKikimr

