#include "viewer.h"
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <algorithm>
#include <array>
#include <cmath>

namespace NKikimr::NActorSystemMonitoring {

void CalculateRates(const TSnapshot& previous, TSnapshot* current) {
    if (!previous.Monotonic || current->Monotonic <= previous.Monotonic) {
        return;
    }
    const double seconds = (current->Monotonic - previous.Monotonic).MicroSeconds() / 1e6;
    for (size_t i = 0; i < std::min(previous.Pools.size(), current->Pools.size()); ++i) {
        auto& pool = current->Pools[i];
        const auto& old = previous.Pools[i];
        if (pool.CpuUs < old.CpuUs || pool.Events < old.Events) {
            continue;
        }
        pool.CpuCores = (pool.CpuUs - old.CpuUs) / (seconds * 1e6);
        pool.EventsPerSecond = (pool.Events - old.Events) / seconds;
        pool.HasRate = true;
    }
}

namespace {
TString Escape(TStringBuf value) {
    TStringBuilder out;
    for (char c : value) {
        switch (c) {
            case '&': out << "&amp;"; break;
            case '<': out << "&lt;"; break;
            case '>': out << "&gt;"; break;
            case '"': out << "&quot;"; break;
            default: out << c;
        }
    }
    return out;
}
TString Number(double value) {
    return std::isfinite(value) ? Sprintf("%.2f", value) : TString("unavailable");
}
TString State(const TPoolSnapshot& pool) {
    if (pool.Config.IsIo) { return "IO pool"; }
    TStringBuilder out;
    if (pool.State.IsNeedy) { out << "<span class='asm-badge'>Needy</span> "; }
    if (pool.State.IsStarved) { out << "<span class='asm-badge asm-warn'>Starved</span> "; }
    if (pool.State.IsHoggish) { out << "<span class='asm-badge'>Hoggish</span> "; }
    return out.empty() ? TString("Balanced") : TString(out);
}

}

TString RenderPage(const TSnapshot& s, TStringBuf tab, TInstant) {
    if (tab != "runtime" && tab != "pools" && tab != "harmonizer" && tab != "events") { tab = "overview"; }
    TStringBuilder out;
    out << "<style>"
        ".asm{margin:16px 12px;color:#30343b}"
        ".asm header,.asm .asm-summary{display:flex;justify-content:space-between;gap:24px;align-items:baseline}"
        ".asm nav{display:flex;gap:24px;border-bottom:1px solid #ddd;padding:18px 0;margin-bottom:24px}"
        ".asm nav .selected{font-weight:bold;color:#30343b}.asm-grid{display:grid;grid-template-columns:1fr 1fr;gap:32px}"
        ".asm table{width:100%;border-collapse:collapse;margin:20px 0}.asm td,.asm th{padding:9px 8px;border-bottom:1px solid #e1e4e8;text-align:right;white-space:nowrap}"
        ".asm td:first-child,.asm th:first-child{text-align:left}.asm-scroll{overflow:auto}.asm-muted{color:#777;font-size:13px}"
        ".asm-badge{background:#edf1f5;padding:3px 7px;border-radius:4px}.asm-warn{background:#fff0d4}"
        ".asm details{margin:16px 0}.asm summary{cursor:pointer}.asm pre{margin:12px 0;white-space:pre-wrap;overflow-wrap:anywhere}"
        "@media(max-width:800px){.asm-grid{grid-template-columns:1fr}.asm header{display:block}}"
        "</style><div class='asm'><header><div><span class='asm-muted'>Node " << s.NodeId
        << "</span></div></header><nav>";
    for (const auto& [key, name] : std::array<std::pair<TStringBuf,TStringBuf>,5>{{{"overview","Overview"},{"runtime","Runtime"},{"pools","Pools"},{"harmonizer","Harmonizer"},{"events","Events"}}}) {
        out << "<a href='?tab=" << key << "'" << (key == tab ? " class='selected'" : "") << ">" << name << "</a>";
    }
    out << "</nav>";
    if (tab == "overview") {
        out << "<h3>Actor system configuration</h3><p>"
            << (s.AutoConfigured ? "Auto-configured" : "Explicit configuration")
            << " &middot; " << s.Pools.size() << " pools</p>"
            << "<div class='asm-scroll'><table><thead><tr><th>Pool</th><th>Type</th><th>Threads</th>"
            << "<th>Min threads</th><th>Max threads</th><th>Priority</th><th>Shared threads</th></tr></thead><tbody>";
        for (size_t i = 0; i < s.Pools.size(); ++i) {
            const auto& config = s.Pools[i].Config;
            out << "<tr><td>" << i << " &middot; " << Escape(config.Name)
                << "</td><td>" << (config.IsIo ? "IO" : "BASIC")
                << "</td><td>" << Escape(config.Threads)
                << "</td><td>" << (config.IsIo ? TString("&mdash;") : Escape(config.MinThreads))
                << "</td><td>" << (config.IsIo ? TString("&mdash;") : Escape(config.MaxThreads))
                << "</td><td>" << (config.IsIo ? TString("&mdash;") : Escape(config.Priority))
                << "</td><td>" << (config.IsIo ? TString("&mdash;") : Escape(config.SharedThreads)) << "</td></tr>";
        }
        out << "</tbody></table></div>";
        for (size_t i = 0; i < s.Pools.size(); ++i) {
            const auto& config = s.Pools[i].Config;
            out << "<details><summary>" << i << " &middot; " << Escape(config.Name)
                << " &middot; all configured parameters</summary><pre>" << Escape(config.Parameters) << "</pre></details>";
        }
        out << "<details><summary>Scheduler and system parameters</summary><pre>"
            << Escape(s.SystemParameters) << "</pre></details>";
    }
    if (tab == "runtime") {
        double cpu = 0, events = 0; i64 actors = 0; bool rate = false;
        for (const auto& p : s.Pools) { cpu += p.CpuCores; events += p.EventsPerSecond; actors += p.Actors; rate |= p.HasRate; }
        out << "<div class='asm-summary'><h4>CPU <strong>" << (rate ? Number(cpu) : TString("warming up"))
            << "</strong> cores</h4><h4>Events <strong>" << (rate ? Number(events) : TString("warming up"))
            << "</strong> / s</h4><h4>Actors <strong>" << std::max<i64>(0, actors) << "</strong></h4></div>";
        out << R"HTML(
<link rel="stylesheet" href="../static/metric-chart/chart.css">
<div id="asm-runtime">
  <div class="asm-chart-controls">
    <label>History <select id="asm-period"><option value="60">1 min</option><option value="300" selected>5 min</option><option value="900">15 min</option><option value="3600">1 hour</option></select></label>
    <button id="asm-now" type="button">Now</button>
    <button id="asm-refresh" type="button">Refresh</button>
    <label><input id="asm-live" type="checkbox"> Live via JSON</label>
    <span id="asm-chart-status" role="status"></span>
  </div>
  <div class="asm-grid">
    <section><h4><a href="metrics?metric=actor_system.pool.cpu_cores">CPU usage &middot; cores</a></h4><div id="asm-cpu"></div></section>
    <section><h4><a href="metrics?metric=actor_system.pool.events_per_second">Processed events / s</a></h4><div id="asm-events"></div></section>
  </div>
</div>
<style>.asm-chart-controls{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:8px 0 16px}.asm-chart-controls label{margin:0}.asm-chart-controls button,.asm-chart-controls select{font:inherit;padding:3px 8px}.asm-chart-controls [role=status]{color:#777}.asm-chart-controls .asm-chart-error{color:#a32f2f}</style>
<script type="module">
import {createMetricChart,createMetricChartCursorGroup} from '../static/metric-chart/chart.js';
import {createInMemoryMetricsClient} from '../static/metric-chart/client.js';
const root=document.getElementById('asm-runtime');
const find=id=>root.querySelector('#asm-'+id);
const client=createInMemoryMetricsClient({endpoint:'metrics'});
const cursorGroup=createMetricChartCursorGroup();
const panels=[{id:'cpu',metric:'actor_system.pool.cpu_cores',title:'CPU usage',unit:'cores'},{id:'events',metric:'actor_system.pool.events_per_second',title:'Processed events / s',unit:'number'}];
const queries=panels.map(({id,metric})=>({id,metric,filters:[]}));
let series=[],fixed=null,controller,timer,version=0;
function pause(){find('live').checked=false;clearTimeout(timer);}
const charts=panels.map(panel=>({panel,chart:createMetricChart(find(panel.id),{cursorGroup,legend:true,settings:{type:'area',height:280,unit:panel.unit,format:'{pool}'},onPin:pause,onRangeChange:({from,to})=>{pause();fixed=[from,to];draw();}})}));
function draw(){
  const end=fixed?fixed[1]:series.length?Math.max(...series.map(s=>s.end)):Date.now();
  const begin=fixed?fixed[0]:end-Number(find('period').value)*1000;
  for(const {panel,chart} of charts)chart.setData({series:series.filter(s=>s.queryId===panel.id),begin,end,title:panel.title,emptyText:'No retained data. Check that actor_system. is allowed in the metrics registry.'});
}
async function refresh(){
  clearTimeout(timer);controller?.abort();controller=new AbortController();const current=++version;
  find('chart-status').textContent='Loading metrics...';find('chart-status').className='';
  try{
    const result=await client.queryMany(queries,{seconds:find('period').value,signal:controller.signal});
    if(current!==version)return;
    const colors=new Map();
    series=result.series.map(s=>{const pool=s.labelValues.find(l=>l.name==='pool_id')?.value??s.key;if(!colors.has(pool))colors.set(pool,s.color);return {...s,color:colors.get(pool)};});draw();
    find('chart-status').textContent='Updated '+new Date(result.catalog.timestamp_ms).toLocaleTimeString()+(result.limited?' - Series limit reached':'');
  }catch(error){
    if(error.name==='AbortError'||current!==version)return;
    find('chart-status').className='asm-chart-error';find('chart-status').textContent=error.message+' - Showing last successful data (stale)';
  }finally{if(current===version&&find('live').checked&&!fixed)timer=setTimeout(refresh,2000);}
}
find('refresh').addEventListener('click',refresh);
find('now').addEventListener('click',()=>{fixed=null;refresh();});
find('period').addEventListener('change',()=>{fixed=null;refresh();});
find('live').addEventListener('change',()=>{clearTimeout(timer);if(find('live').checked){fixed=null;refresh();}});
window.addEventListener('pagehide',event=>{++version;clearTimeout(timer);controller?.abort();if(!event.persisted)for(const {chart} of charts)chart.destroy();});
window.addEventListener('pageshow',event=>{if(event.persisted)refresh();});
refresh();
</script>
)HTML";
    }
    if (tab == "runtime" || tab == "pools") {
        out << "<div class='asm-scroll'><table><thead><tr><th>Pool</th><th>CPU cores</th><th>Events / s</th><th>Actors</th><th>Threads</th><th>State</th></tr></thead><tbody>";
        for (size_t i=0; i<s.Pools.size(); ++i) {
            const auto& p=s.Pools[i];
            out << "<tr><td>" << i << " &middot; " << Escape(p.Config.Name) << "</td><td>" << (p.HasRate ? Number(p.CpuCores) : TString("&mdash;"))
                << "</td><td>" << (p.HasRate ? Number(p.EventsPerSecond) : TString("&mdash;")) << "</td><td>" << std::max<i64>(0,p.Actors)
                << "</td><td>" << Number(p.Stats.CurrentThreadCount) << "</td><td>" << State(p) << "</td></tr>";
        }
        out << "</tbody></table></div>";
    }
    if (tab == "pools") {
        out << "<h3>CPU allocation</h3><div class='asm-scroll'><table><tr><th>Pool</th><th>Minimum</th><th>Current limit</th><th>Maximum</th><th>Possible maximum</th><th>Shared CPU quota</th></tr>";
        for (const auto& p:s.Pools) { if (p.Config.IsIo) {continue;}
            out << "<tr><td>" << Escape(p.Config.Name) << "</td><td>" << Number(p.State.MinLimit) << "</td><td>" << Number(p.State.CurrentLimit)
                << "</td><td>" << Number(p.State.MaxLimit) << "</td><td>" << Number(p.State.PossibleMaxLimit) << "</td><td>" << Number(p.State.SharedCpuQuota) << "</td></tr>";
        }
        out << "</table></div>";
    }
    if (tab == "events") {
        out << "<h3>Pool counters &middot; since start</h3><div class='asm-scroll'><table><thead><tr><th>Pool</th>";
        for (TStringBuf name : PoolCounterNames) {
            const auto shortName = name.SubStr(TStringBuf("actor_system.pool.").size());
            out << "<th><a href='metrics?metric=" << name << "'>" << shortName << "</a></th>";
        }
        out << "</tr></thead><tbody>";
        for (const auto& pool : s.Pools) {
            out << "<tr><td>" << Escape(pool.Config.Name) << "</td>";
            for (ui64 value : pool.Counters) {
                out << "<td>" << value << "</td>";
            }
            out << "</tr>";
        }
        out << "</tbody></table></div>";
    }
    if (tab == "harmonizer") {
        out << "<div class='asm-grid'><section><h3>CPU budget</h3><table><tr><td>Budget</td><td>" << Number(s.Harmonizer.Budget)
            << "</td></tr><tr><td>Shared free CPU</td><td>" << Number(s.Harmonizer.SharedFreeCpu)
            << "</td></tr></table></section><section><h3>Wakeup</h3><table><tr><td>Average awakening</td><td>" << Number(s.Harmonizer.AvgAwakeningTimeUs)
            << " &micro;s</td></tr><tr><td>Average waking up</td><td>" << Number(s.Harmonizer.AvgWakingUpTimeUs) << " &micro;s</td></tr></table></section></div>"
            << "<h3>Thread adjustments &middot; since start</h3><div class='asm-scroll'><table><tr><th>Pool</th><th>State</th><th>Added: needy</th><th>Added: exchange</th><th>Removed: starved</th><th>Removed: hoggish</th><th>Removed: exchange</th></tr>";
        for (const auto& p:s.Pools) { if (p.Config.IsIo) { continue; }
            out << "<tr><td>" << Escape(p.Config.Name) << "</td><td>" << State(p) << "</td><td>" << p.Stats.IncreasingThreadsByNeedyState
                << "</td><td>" << p.Stats.IncreasingThreadsByExchange << "</td><td>" << p.Stats.DecreasingThreadsByStarvedState
                << "</td><td>" << p.Stats.DecreasingThreadsByHoggishState << "</td><td>" << p.Stats.DecreasingThreadsByExchange << "</td></tr>";
        }
        out << "</table></div>";
    }
    out << "</div>";
    return out;
}
} // namespace NKikimr::NActorSystemMonitoring
