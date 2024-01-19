#include "columnshard_impl.h"
#include "columnshard_view.h"

namespace NKikimr::NColumnShard {

class TTxMonitoring : public TTransactionBase<TColumnShard> {
public:
    TTxMonitoring(TColumnShard* self, const NMon::TEvRemoteHttpInfo::TPtr& ev)
        : TBase(self)
        , HttpInfoEvent(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    //TTxType GetTxType() const override { return TXTYPE_INIT; }

private:
    NMon::TEvRemoteHttpInfo::TPtr HttpInfoEvent;
    NJson::TJsonValue JsonReport = NJson::JSON_MAP;
};


bool TTxMonitoring::Execute(TTransactionContext& txc, const TActorContext&) {
    return Self->TablesManager.FillMonitoringReport(txc, JsonReport["tables_manager"]);
}

void TTxMonitoring::Complete(const TActorContext& ctx) {
    ctx.Send(HttpInfoEvent->Sender, new NMon::TEvRemoteJsonInfoRes(JsonReport.GetStringRobust()));
}

bool TColumnShard::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) {
    if (!Executor() || !Executor()->GetStats().IsActive) {
         return false;
    }

    if (!ev) {
        return true;
    }

    Execute(new TTxMonitoring(this, ev), ctx);
    return true;
}

}
