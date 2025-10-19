#include "scan_diagnostics_actor.h"

#include <contrib/libs/fmt/include/fmt/format.h>

namespace NKikimr::NColumnShard::NDiagnostics {

TScanDiagnosticsActor::TScanDiagnosticsActor()
    : TActor(&TThis::StateMain) {    
}

void TScanDiagnosticsActor::Handle(const NMon::TEvRemoteHttpInfo::TPtr& ev) {
    TStringBuilder htmlResult;
    htmlResult << R"(<script src="https://dreampuf.github.io/GraphvizOnline/viz-global.js"></script>)";
    htmlResult << RenderScanDiagnostics(LastPublicScans, "public");
    htmlResult << RenderScanDiagnostics(LastInternalScans, "internal");
    Send(ev->Sender, std::make_unique<NMon::TEvRemoteHttpInfoRes>(htmlResult));
}

TString TScanDiagnosticsActor::RenderScanDiagnostics(const std::deque<TScanDiagnosticsInfo>& lastScans, const TString& tag) {
    TStringBuilder htmlResult;
    int i = 0;
    for (auto it = lastScans.rbegin(); it != lastScans.rend(); ++it, i++) {
        const auto& info = *it;
        htmlResult << RenderScanDiagnosticsInfo(info, i, tag);
    }
    return htmlResult;
}

TString TScanDiagnosticsActor::RenderScanDiagnosticsInfo(const TScanDiagnosticsInfo& info, int id, const TString& tag) {
    using namespace fmt::literals;
    return fmt::format(R"(
        <h3> Scan {tag} {id} </h3>
        <h4> Request Message </h4>
        <pre>{request_message}</pre>
        <h4> PK Ranges Filter </h4>
        <pre>{pk_ranges_filter}</pre>
        <h4> SSA Program </h4>
        <pre>{ssa_program}</pre>
        <h4> Program Graph </h4>
        <script>
            Viz.instance().then(function(viz) {{
                var dot = String.raw`{dot_graph}`;
                var svg = viz.renderSVGElement(dot);
                document.getElementById('{tag}_graph_{id}').appendChild(svg);
            }}).catch(function(err) {{
                document.getElementById('{tag}_graph_{id}').textContent = err.toString();
            }});
        </script>
        <div id="{tag}_graph_{id}"></div>
        <hr>
    )", 
    "tag"_a = tag,
    "id"_a = id,
    "request_message"_a = info.RequestMessage,
    "pk_ranges_filter"_a = info.PKRangesFilter,
    "ssa_program"_a = info.SSAProgram,
    "dot_graph"_a = info.DotGraph);
}

void TScanDiagnosticsActor::Handle(const NColumnShard::TEvPrivate::TEvReportScanDiagnostics::TPtr& ev) {
    auto& event = *ev->Get();
    TScanDiagnosticsInfo info(std::move(event.RequestMessage), std::move(event.DotGraph), std::move(event.SSAProgram), std::move(event.PKRangesFilter));
    if (event.IsPublicScan) {
        AddScanDiagnostics(std::move(info), LastPublicScans);
    } else {
        AddScanDiagnostics(std::move(info), LastInternalScans);
    }
}

void TScanDiagnosticsActor::AddScanDiagnostics(TScanDiagnosticsInfo&& info, std::deque<TScanDiagnosticsInfo>& lastScans) {
    lastScans.emplace_back(std::move(info));
    if (lastScans.size() > MaxScans) {
        lastScans.pop_front();
    }
}

}

