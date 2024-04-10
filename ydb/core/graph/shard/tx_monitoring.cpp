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

    static TString DumpMetricsIndex(const std::unordered_map<TString, ui64>& metricsIndex) {
        TStringBuilder str;
        str << metricsIndex.size();
        if (!metricsIndex.empty()) {
            str << " (";
            bool wasItem = false;
            for (const auto& [name, idx] : metricsIndex) {
                if (wasItem) {
                    str << ", ";
                }
                str << name;
                wasItem = true;
            }
            str << ")";
        }
        return str;
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

        html << "<tr><td>Memory.MetricsSize</td><td>" << DumpMetricsIndex(Self->MemoryBackend.MetricsIndex) << "</td></tr>";
        html << "<tr><td>Memory.RecordsSize</td><td>" << Self->MemoryBackend.MetricsValues.size() << "</td></tr>";

        html << "<tr><td>Local.MetricsSize</td><td>" << DumpMetricsIndex(Self->LocalBackend.MetricsIndex) << "</td></tr>";
        html << "<tr><td>AggregateTimestamp</td><td>" << Self->AggregateTimestamp.ToStringUpToSeconds() << "</td></tr>";
        html << "<tr><td style='vertical-align:top'>AggregateSettings</td><td>";
        for (bool wasLine = false; const auto& settings : Self->AggregateSettings) {
            if (wasLine) {
                html << "<br>";
            }
            html << settings.ToString();
            wasLine = true;
        }
        html << "</td></tr>";

        html << "<tr><td>CurrentTimestamp</td><td>" << Self->MetricsData.Timestamp.ToStringUpToSeconds() << "</td></tr>";

        html << "<tr><td style='vertical-align:top'>CurrentMetricsData</td><td>";
        bool wasLine = false;
        for (const auto& [name, value] : Self->MetricsData.Values) {
            if (wasLine) {
                html << "<br>";
            }
            html << name << "=" << value;
            wasLine = true;
        }
        for (const auto& [name, value] : Self->MetricsData.HistogramValues) {
            if (wasLine) {
                html << "<br>";
            }
            html << "histogram " << name << " " << value.size() << " points";
            wasLine = true;
        }
        for (const auto& [name, value] : Self->MetricsData.ArithmeticValues) {
            if (wasLine) {
                html << "<br>";
            }
            html << "arithmetic " << name << " " << value.ValueA << " " << value.Op << " " << value.ValueB; 
            wasLine = true;
        }
        html << "</td></tr>";

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

