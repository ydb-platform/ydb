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
        TStringBuilder html;
        html << "<html>";
        html << "<style>";
        html << "table.simple-table1 th { text-align: center; }";
        html << "table.simple-table1 td { padding: 1px 3px; }";
        html << "table.simple-table1 td:nth-child(1) { text-align: right; }";
        html << "</style>";

        html << "<table class='simple-table1'>";

        html << "<tr><td>Backend</td><td>";
        switch (Self->BackendType) {
            case EBackendType::Memory:
                html << "Memory";
                break;
            case EBackendType::Local:
                html << "Local";
                break;
            case EBackendType::External:
                html << "External";
                break;
        }
        html << "</td></tr>";

        html << "<tr><td>Memory.MetricsSize</td><td>" << Self->MemoryBackend.MetricsIndex.size() << "</td></tr>";
        html << "<tr><td>Memory.RecordsSize</td><td>" << Self->MemoryBackend.MetricsValues.size() << "</td></tr>";

        html << "<tr><td>Local.MetricsSize</td><td>" << Self->LocalBackend.MetricsIndex.size() << "</td></tr>";
        html << "<tr><td>StartTimestamp</td><td>" << Self->StartTimestamp << "</td></tr>";
        html << "<tr><td>ClearTimestamp</td><td>" << Self->ClearTimestamp << "</td></tr>";

        html << "</table>";
        html << "</html>";
        ctx.Send(Event->Sender, new NMon::TEvRemoteHttpInfoRes(html));
    }
};

void TGraphShard::ExecuteTxMonitoring(NMon::TEvRemoteHttpInfo::TPtr ev) {
    if (ev->Get()->Cgi().Has("action")) {
        if (ev->Get()->Cgi().Get("action") == "change_backend") {
            ui64 backend = FromStringWithDefault(ev->Get()->Cgi().Get("backend"), 0);
            if (backend >= 0 && backend <= 2) {
                ExecuteTxChangeBackend(static_cast<EBackendType>(backend));
                Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes("<html><p>ok</p></html>"));
                return;
            }
        }
        Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes("<html><p>bad parameters</p></html>"));
        return;
    }
    Execute(new TTxMonitoring(this, std::move(ev)));
}

} // NGraph
} // NKikimr

