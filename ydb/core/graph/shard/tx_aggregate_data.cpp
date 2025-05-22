#include "shard_impl.h"
#include "log.h"

namespace NKikimr {
namespace NGraph {

class TTxAggregateData : public TTransactionBase<TGraphShard> {
private:
    const TAggregateSettings& Settings;
public:
    TTxAggregateData(TGraphShard* shard, const TAggregateSettings& settings)
        : TBase(shard)
        , Settings(settings)
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_AGGREGATE_DATA; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("TTxAggregateData::Execute (" << Settings.ToString() << ")");
        TInstant now = TActivationContext::Now();
        return Self->LocalBackend.AggregateData(txc, now, Settings);
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("TTxAggregateData::Complete");
    }
};

void TGraphShard::ExecuteTxAggregateData(const TAggregateSettings& settings) {
    switch (BackendType) {
        case EBackendType::Memory:
            MemoryBackend.AggregateData(TActivationContext::Now(), settings);
            break;
        case EBackendType::Local:
            Execute(new TTxAggregateData(this, settings));
            break;
        case EBackendType::External:
            // TODO
            break;
    }
}

} // NGraph
} // NKikimr

