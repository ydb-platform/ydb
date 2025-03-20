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
    const auto& cgi = HttpInfoEvent->Get()->Cgi();
    std::map<std::pair<ui64, ui64>, NJson::TJsonValue> schemaVersions;
    for (const auto& item: JsonReport["tables_manager"]["schema_versions"].GetArray()) {
        auto& schemaVersion = schemaVersions[std::make_pair<ui64, ui64>(item["SinceStep"].GetInteger(), item["SinceTxId"].GetInteger())];
        schemaVersion = item;
    }
    size_t countVersions = std::min(10ul, schemaVersions.size());
    if (const auto& countVersionsParam = cgi.Get("countVersions")) {
        try {
            countVersions = std::min(std::stoul(countVersionsParam), schemaVersions.size());
        } catch (...) {
            // nothing, use default value
        }
    }
    JsonReport["tables_manager"].EraseValue("schema_versions");
    TStringStream html;
    html << "<h3>Special Values</h3>";
    html << "<b>CurrentSchemeShardId:</b> " << Self->CurrentSchemeShardId << "<br />";
    html << "<b>ProcessingParams:</b> " << Self->ProcessingParams.value_or(NKikimrSubDomains::TProcessingParams{}).ShortDebugString() << "<br />";
    html << "<b>LastPlannedStep:</b> " << Self->LastPlannedStep << "<br />";
    html << "<b>LastPlannedTxId :</b> " << Self->LastPlannedTxId << "<br />";
    html << "<b>LastSchemaSeqNoGeneration :</b> " << Self->LastSchemaSeqNo.Generation << "<br />";
    html << "<b>LastSchemaSeqNoRound :</b> " << Self->LastSchemaSeqNo.Round << "<br />";
    html << "<b>LastExportNumber :</b> " << Self->LastExportNo << "<br />";
    html << "<b>OwnerPathId :</b> " << Self->OwnerPathId << "<br />";
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
    for (const auto& [_, schemaVersion]: schemaVersions) {
        html << counter;
        html << "<pre>" << schemaVersion << "</pre><br />";
        if (++counter == countVersions) {
            break;
        }
    }
    ctx.Send(HttpInfoEvent->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
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
