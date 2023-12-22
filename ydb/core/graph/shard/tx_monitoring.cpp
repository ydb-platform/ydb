#include "shard_impl.h"
#include "log.h"

namespace NKikimr {
namespace NGraph {

class TTxMonitoring : public TTransactionBase<TGraphShard> {
private:
    NMon::TEvRemoteHttpInfo::TPtr Event;

public:
    TTxMonitoring(TGraphShard* shard, NMon::TEvRemoteHttpInfo::TPtr ev)
        : TBase(shard)
        , Event(std::move(ev))
    {}

    TTxType GetTxType() const override { return NGraphShard::TXTYPE_MONITORING; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        BLOG_D("TTxMonitoring::Execute");
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("TTxMonitoring::Complete");
        ctx.Send(Event->Sender, new NMon::TEvRemoteHttpInfoRes("<html><p>OK!</p></html>"));
    }
};

void TGraphShard::ExecuteTxMonitoring(NMon::TEvRemoteHttpInfo::TPtr ev) {
    Execute(new TTxMonitoring(this, std::move(ev)));
}

} // NGraph
} // NKikimr

