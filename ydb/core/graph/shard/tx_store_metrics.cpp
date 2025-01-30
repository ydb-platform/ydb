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
        return Self->LocalBackend.StoreMetrics(txc, std::move(Data));
    }

    void Complete(const TActorContext&) override {
        BLOG_D("TTxStoreMetrics::Complete");
    }
};

void TGraphShard::ExecuteTxStoreMetrics(TMetricsData&& data) {
    AggregateMetrics(data);
    switch (BackendType) {
        case EBackendType::Memory:
            MemoryBackend.StoreMetrics(std::move(data));
            break;
        case EBackendType::Local:
            Execute(new TTxStoreMetrics(this, std::move(data)));
            break;
        case EBackendType::External:
            // TODO
            break;
    }
}

} // NGraph
} // NKikimr

