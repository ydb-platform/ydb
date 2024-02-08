#include "shard_impl.h"
#include "log.h"
#include "schema.h"
#include "backends.h"

namespace NKikimr {
namespace NGraph {

class TTxStartup : public TTransactionBase<TGraphShard> {
public:
    TTxStartup(TGraphShard* shard)
        : TBase(shard)
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_STARTUP; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("TTxStartup::Execute");
        NIceDb::TNiceDb db(txc.DB);
        {
            auto row = db.Table<Schema::State>().Key(TString("backend")).Select();
            if (!row.IsReady()) {
                return false;
            }
            if (!row.EndOfSet()) {
                ui64 backend = row.GetValue<Schema::State::ValueUI64>();
                if (backend >= 0 && backend <= 2) {
                    Self->BackendType = static_cast<EBackendType>(backend);
                }
            }
        }
        {
            auto rowset = db.Table<Schema::MetricsIndex>().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                Self->LocalBackend.MetricsIndex[rowset.GetValue<Schema::MetricsIndex::Name>()] = rowset.GetValue<Schema::MetricsIndex::Id>();
                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("TTxStartup::Complete");
        Self->OnReadyToWork();
    }
};

void TGraphShard::ExecuteTxStartup() {
    Execute(new TTxStartup(this));
}

} // NGraph
} // NKikimr

