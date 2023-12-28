#include "shard_impl.h"
#include "log.h"

namespace NKikimr {
namespace NGraph {

class TTxStoreMetrics : public TTransactionBase<TGraphShard> {
private:
    TMetricsData Data;

public:
    TTxStoreMetrics(TGraphShard* shard, TMetricsData&& data)
        : TBase(shard)
        , Data(std::move(data))
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_STORE_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("TTxStoreMetrics::Execute");
        switch (Self->BackendType) {
            case EBackendType::Memory:
                Self->MemoryBackend.StoreMetrics(std::move(Data));
                return true;
            case EBackendType::Local:
                return Self->LocalBackend.StoreMetrics(txc, std::move(Data));
            case EBackendType::External:
                // TODO
                break;
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("TTxStoreMetrics::Complete");
    }
};

void TGraphShard::ExecuteTxStoreMetrics(TMetricsData&& data) {
    Execute(new TTxStoreMetrics(this, std::move(data)));
}

} // NGraph
} // NKikimr

