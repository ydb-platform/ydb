#include "shard_impl.h"
#include "log.h"

namespace NKikimr {
namespace NGraph {

class TTxGetMetrics : public TTransactionBase<TGraphShard> {
private:
    TEvGraph::TEvGetMetrics::TPtr Event;
    NKikimrGraph::TEvMetricsResult Result;
public:
    TTxGetMetrics(TGraphShard* shard, TEvGraph::TEvGetMetrics::TPtr ev)
        : TBase(shard)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_GET_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("TTxGetMetrics::Execute");
        switch (Self->BackendType) {
            case EBackendType::Memory:
                Self->MemoryBackend.GetMetrics(Event->Get()->Record, Result);
                return true;
            case EBackendType::Local:
                return Self->LocalBackend.GetMetrics(txc, Event->Get()->Record, Result);
            case EBackendType::External:
                break;
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("TTxGetMetric::Complete");
        BLOG_TRACE("TxGetMetrics returned " << Result.TimeSize() << " points for request " << Event->Cookie);
        ctx.Send(Event->Sender, new TEvGraph::TEvMetricsResult(std::move(Result)), 0, Event->Cookie);
    }
};

void TGraphShard::ExecuteTxGetMetrics(TEvGraph::TEvGetMetrics::TPtr ev) {
    Execute(new TTxGetMetrics(this, ev));
}

} // NGraph
} // NKikimr

