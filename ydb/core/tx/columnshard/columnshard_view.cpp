#include "columnshard_impl.h"
#include "columnshard_view.h"

namespace NKikimr::NColumnShard {

class TTxMonitoring: public TTransactionBase<TColumnShard> {
public:
    TTxMonitoring(TColumnShard* self, const NMon::TEvRemoteHttpInfo::TPtr& ev)
        : TBase(self)
        , HttpInfoEvent(ev) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    //TTxType GetTxType() const override { return TXTYPE_INIT; }

private:
    NMon::TEvRemoteHttpInfo::TPtr HttpInfoEvent;
    NJson::TJsonValue JsonReport = NJson::JSON_MAP;
    TString RenderCompactionPage();
    TString RenderMainPage();
};

inline TString TEscapeHtml(const TString& in) {
    TString out;
    out.reserve(in.size());
    for (char c : in) {
        switch (c) {
            case '<':
                out += "&lt;";
                break;
            case '>':
                out += "&gt;";
                break;
            case '&':
                out += "&amp;";
                break;
            case '\'':
                out += "&#39;";
                break;
            case '\"':
                out += "&#34;";
                break;
            default:
                out += c;
                break;
        }
    }

    return out;
}

bool TTxMonitoring::Execute(TTransactionContext& txc, const TActorContext&) {
    return Self->TablesManager.FillMonitoringReport(txc, JsonReport["tables_manager"]);
}

template <typename T>
void TPrintErrorTable(TStringStream& html, THashMap<TString, std::queue<T>> errors, const std::string& errorType) {
    html << R"(
        <style>
            .error-table {
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 1em;
                font-family: Arial, sans-serif;
            }
            .error-table th,
            .error-table td {
                border: 1px solid #ddd;
                padding: 8px;
                word-break: break-all;
                text-align: left;
                vertical-align: top;
            }
            .error-table th {
                background-color: #f2f2f2;
                font-weight: bold;
            }
            .error-table tr:nth-child(even) {
                background-color: #fafafa;
            }
            .error-table tr:hover {
                background-color: #f4f8ff;
            }
        </style>
    )";

    if (errors.empty()) {
        html << "No " << errorType << " errors<br />";
    } else {
        html << "<b>" << errorType << " errors:</b><br />";
        html << "<table class='error-table'>"
                "<tr><th>Tier</th><th>Time</th><th>Error</th></tr>";

        for (auto [tier, queue] : errors) {
            while (!queue.empty()) {
                const auto& element = queue.front();
                html << "<tr>"
                    << "<td>" << TEscapeHtml(tier) << "</td>"
                    << "<td>" << element.Time.ToString() << "</td>"
                    << "<td style=\"max-width:420px;\">" << TEscapeHtml(element.Reason) << "</td>"
                    << "</tr>";
                queue.pop();
            }
        }

        html << "</table><br />";
    }
}

TString TTxMonitoring::RenderMainPage() {
    const auto& cgi = HttpInfoEvent->Get()->Cgi();
    std::map<std::pair<ui64, ui64>, NJson::TJsonValue> schemaVersions;
    for (const auto& item : JsonReport["tables_manager"]["schema_versions"].GetArray()) {
        auto& schemaVersion = schemaVersions[std::make_pair<ui64, ui64>(item["SinceStep"].GetInteger(), item["SinceTxId"].GetInteger())];
        schemaVersion = item;
    }
    size_t countVersions = std::min(size_t(10), schemaVersions.size());
    if (const auto& countVersionsParam = cgi.Get("CountVersions")) {
        try {
            countVersions = std::min(size_t(std::stoul(countVersionsParam)), schemaVersions.size());
        } catch (...) {
            // nothing, use default value
        }
    }
    JsonReport["tables_manager"].EraseValue("schema_versions");
    TStringStream html;
    html << "<h3>Special Values</h3>";
    html << "<b>CurrentSchemeShardId:</b> " << Self->CurrentSchemeShardId << "<br />";
    html << "<b>ProcessingParams:</b> " << Self->ProcessingParams.value_or(NKikimrSubDomains::TProcessingParams{}).ShortDebugString()
         << "<br />";
    html << "<b>LastPlannedStep:</b> " << Self->LastPlannedStep << "<br />";
    html << "<b>LastPlannedTxId :</b> " << Self->LastPlannedTxId << "<br />";
    html << "<b>LastSchemaSeqNoGeneration :</b> " << Self->LastSchemaSeqNo.Generation << "<br />";
    html << "<b>LastSchemaSeqNoRound :</b> " << Self->LastSchemaSeqNo.Round << "<br />";
    html << "<b>LastExportNumber :</b> " << Self->LastExportNo << "<br />";
    if (const auto& tabletPathId = Self->TablesManager.GetTabletPathIdOptional()) {
        html << "<b>SchemeShardLocalPathId :</b> " << tabletPathId->SchemeShardLocalPathId << "<br />";
        html << "<b>InternalPathId :</b> " << tabletPathId->InternalPathId << "<br />";
    } else {
        html << "<b>SchemeShardLocalPathId :</b> " << "None" << "<br />";
        html << "<b>InternalPathId :</b> " << "None" << "<br />";
    }
    html << "<b>Table/Store Path :</b> " << Self->OwnerPath << "<br />";
    html << "<b>LastCompletedStep :</b> " << Self->LastCompletedTx.GetPlanStep() << "<br />";
    html << "<b>LastCompletedTxId :</b> " << Self->LastCompletedTx.GetTxId() << "<br />";
    html << "<b>LastNormalizerSequentialId :</b> " << Self->NormalizerController.GetLastSavedNormalizerId() << "<br />";
    html << "<b>SubDomainLocalPathId :</b> " << Self->SpaceWatcher->SubDomainPathId.value_or(0) << "<br />";
    html << "<b>SubDomainOutOfSpace :</b> " << Self->SpaceWatcher->SubDomainOutOfSpace << "<br />";
    html << "<h3>Tables Manager</h3>";
    html << "<h4>Status</h4>";
    html << "<pre>" << JsonReport << "</pre><br />";
    html << "<h4>Top " << countVersions << " of " << schemaVersions.size() << " Versions</h4>";
    size_t counter = 0;
    for (const auto& [_, schemaVersion] : schemaVersions) {
        html << counter;
        html << "<pre>" << schemaVersion << "</pre><br />";
        if (++counter == countVersions) {
            break;
        }
    }

    html << "<h3><a href=\"app?Compaction=true&TabletID=" << cgi.Get("TabletID") << "\"> Compaction </a></h3>";

    html << "<h3>Tiering Errors</h3>";
    auto readErrors = Self->Counters.GetEvictionCounters().TieringErrors->GetAllReadErrors();
    auto writeErrors = Self->Counters.GetEvictionCounters().TieringErrors->GetAllWriteErrors();

    TPrintErrorTable(html, readErrors, "read");
    TPrintErrorTable(html, writeErrors, "write");

    return html.Str();
}


TString TTxMonitoring::RenderCompactionPage() {
    TStringStream html;
    const auto& cgi = HttpInfoEvent->Get()->Cgi();
    auto engine = Self->TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    for (auto [tableId, _] : Self->TablesManager.GetTables()) {
        html << "<h3>TableId : " << tableId << "</h3>";
        auto& compaction = engine.GetGranuleVerified(tableId).GetOptimizerPlanner();
        auto json = compaction.SerializeToJsonVisual();
        html << "<pre>";
        NJson::WriteJson(&html, &json, true, true, true);
        html << "</pre>";
    }
    return html.Str();
}

void TTxMonitoring::Complete(const TActorContext& ctx) {
    auto cgi = HttpInfoEvent->Get()->Cgi();
    auto path = HttpInfoEvent->Get()->PathInfo();
    TString htmlResult;

    if (cgi.Has("Compaction") && cgi.Get("Compaction") == "true") {
        htmlResult = RenderCompactionPage();
    } else {
        htmlResult = RenderMainPage();
    }
    ctx.Send(HttpInfoEvent->Sender, new NMon::TEvRemoteHttpInfoRes(htmlResult));
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

}   // namespace NKikimr::NColumnShard
