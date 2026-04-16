#include "scan_diagnostics_actor.h"

#include <ydb/core/mon/mon.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <library/cpp/monlib/service/pages/resource_mon_page.h>

namespace NKikimr::NColumnShard::NDiagnostics {
    
namespace {

void ReplaceAll(TString& str, const TString& from, const TString& to) {
    if (from.empty()) {
        return;
    }
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != TString::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
}

}

void TScanDiagnosticsActor::Bootstrap() {
    TMon* mon = AppData()->Mon;
    if (mon) {
        mon->Register(new NMonitoring::TResourceMonPage("columnshard/viz-global.js", "viz-global.js", NMonitoring::TResourceMonPage::JAVASCRIPT));
    }
    Become(&TThis::StateMain);
}

void TScanDiagnosticsActor::Handle(const NMon::TEvRemoteHttpInfo::TPtr& ev) {
    TStringBuilder htmlResult;
    htmlResult << R"(<script language="javascript" type="text/javascript" src="../columnshard/viz-global.js"></script>)";
    htmlResult << RenderScanDiagnostics(LastPublicScans, "public");
    htmlResult << RenderScanDiagnostics(LastInternalScans, "internal");
    Send(ev->Sender, std::make_unique<NMon::TEvRemoteHttpInfoRes>(htmlResult));
}

TString TScanDiagnosticsActor::RenderScanDiagnostics(const std::deque<std::shared_ptr<TScanDiagnosticsInfo>>& lastScans, const TString& tag) {
    TStringBuilder htmlResult;
    int i = 0;
    for (auto it = lastScans.rbegin(); it != lastScans.rend(); ++it, i++) {
        const auto& info = *it;
        htmlResult << RenderScanDiagnosticsInfo(*info, i, tag);
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
        <h4> Scan Iterator </h4>
        <pre>{scan_iterator}</pre>
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
    "dot_graph"_a = info.DotGraph,
    "scan_iterator"_a = info.ScanIterator);
}

void TScanDiagnosticsActor::Handle(const NColumnShard::TEvPrivate::TEvReportScanDiagnostics::TPtr& ev) {
    auto& event = *ev->Get();
    auto info = std::make_shared<TScanDiagnosticsInfo>(event.RequestId, std::move(event.RequestMessage), std::move(event.DotGraph), std::move(event.SSAProgram), std::move(event.PKRangesFilter));
    if (event.IsPublicScan) {
        AddScanDiagnostics(info, LastPublicScans);
    } else {
        AddScanDiagnostics(info, LastInternalScans);
    }
}

void TScanDiagnosticsActor::Handle(const NColumnShard::TEvPrivate::TEvReportScanIteratorDiagnostics::TPtr& ev) {
    auto& event = *ev->Get();
    auto it = RequestToInfo.find(event.RequestId);
    if (it == RequestToInfo.end()) {
        return;
    }
    ReplaceAll(event.ScanIteratorDiagnostics, ";;", ";");
    ReplaceAll(event.ScanIteratorDiagnostics, ");", ");\n");
    ReplaceAll(event.ScanIteratorDiagnostics, ";};", ";};\n");
    ReplaceAll(event.ScanIteratorDiagnostics, "steps:{", "steps:\n{");
    it->second->ScanIterator = std::move(event.ScanIteratorDiagnostics);
}

void TScanDiagnosticsActor::AddScanDiagnostics(const std::shared_ptr<TScanDiagnosticsInfo>& info, std::deque<std::shared_ptr<TScanDiagnosticsInfo>>& lastScans) {
    lastScans.emplace_back(info);
    RequestToInfo[info->RequestId] = lastScans.back();
    if (lastScans.size() > MaxScans) {
        ui64 requestId = lastScans.front()->RequestId;
        RequestToInfo.erase(requestId);
        lastScans.pop_front();
    }
}

}

