#include "shard_impl.h"
#include "log.h"
#include "schema.h"

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
        BLOG_D("TTxChangeBackend::Execute (" << static_cast<ui64>(Backend) << ")");
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::State>().Key(TString("backend")).Update<Schema::State::ValueUI64>(static_cast<ui64>(Backend));
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("TTxChangeBackend::Complete");
    }
};

void TGraphShard::ExecuteTxChangeBackend(EBackendType backend) {
    Execute(new TTxChangeBackend(this, backend));
}

} // NGraph
} // NKikimr

