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
        ALOG_DEBUG(NKikimrServices::GRAPH, GetLogPrefix() <<"TTxGetMetrics::Execute");
        return Self->LocalBackend.GetMetrics(txc, Event->Get()->Record, Result);
    }

    void Complete(const TActorContext& ctx) override {
        ALOG_DEBUG(NKikimrServices::GRAPH, GetLogPrefix() <<"TTxGetMetric::Complete");
        ALOG_TRACE(NKikimrServices::GRAPH, GetLogPrefix() <<"TxGetMetrics returned " << Result.TimeSize() << " points for request " << Event->Cookie);
        ctx.Send(Event->Sender, new TEvGraph::TEvMetricsResult(std::move(Result)), 0, Event->Cookie);
    }
};

void TGraphShard::ExecuteTxGetMetrics(TEvGraph::TEvGetMetrics::TPtr ev) {
    switch (BackendType) {
        case EBackendType::Memory: {
            NKikimrGraph::TEvMetricsResult result;
            MemoryBackend.GetMetrics(ev->Get()->Record, result);
            ALOG_TRACE(NKikimrServices::GRAPH, GetLogPrefix() <<"GetMetrics returned " << result.TimeSize() << " points for request " << ev->Cookie);
            Send(ev->Sender, new TEvGraph::TEvMetricsResult(std::move(result)), 0, ev->Cookie);
            break;
        }
        case EBackendType::Local:
            Execute(new TTxGetMetrics(this, ev));
            break;
        case EBackendType::External:
            // TODO
            break;
    }
}

} // NGraph
} // NKikimr

