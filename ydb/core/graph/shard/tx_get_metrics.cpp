#include "shard_impl.h"
#include "log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRAPH

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
        YDB_LOG_DEBUG("TTxGetMetrics::Execute",
            {"LogPrefix", GetLogPrefix()});
        return Self->LocalBackend.GetMetrics(txc, Event->Get()->Record, Result);
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxGetMetric::Complete",
            {"LogPrefix", GetLogPrefix()});
        YDB_LOG_TRACE("TxGetMetrics returned points for request",
            {"LogPrefix", GetLogPrefix()},
            {"TimeSize", Result.TimeSize()},
            {"Cookie", Event->Cookie});
        ctx.Send(Event->Sender, new TEvGraph::TEvMetricsResult(std::move(Result)), 0, Event->Cookie);
    }
};

void TGraphShard::ExecuteTxGetMetrics(TEvGraph::TEvGetMetrics::TPtr ev) {
    switch (BackendType) {
        case EBackendType::Memory: {
            NKikimrGraph::TEvMetricsResult result;
            MemoryBackend.GetMetrics(ev->Get()->Record, result);
            YDB_LOG_TRACE("GetMetrics returned points for request",
                {"LogPrefix", GetLogPrefix()},
                {"TimeSize", result.TimeSize()},
                {"Cookie", ev->Cookie});
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

