#include "viewer.h"

#include <library/cpp/json/json_writer.h>
#include <util/string/cast.h>
#include <algorithm>
#include <cmath>
#include <deque>

namespace NKikimr::NInMemoryMetricsMonitoring {
namespace {
using namespace NActors;

template<class TLabels>
NJson::TJsonValue Labels(const TLabels& labels) {
    NJson::TJsonValue result(NJson::JSON_ARRAY);
    for (const auto& label : labels) {
        NJson::TJsonValue item(NJson::JSON_MAP);
        item["name"] = label.Name;
        item["value"] = label.Value;
        result.AppendValue(std::move(item));
    }
    return result;
}

NJson::TJsonValue NumericValue(const TLineNumericValue& value) {
    return std::visit([](const auto& item) -> NJson::TJsonValue {
        using TValue = std::decay_t<decltype(item)>;
        if constexpr (std::is_same_v<TValue, std::monostate>) {
            return NJson::TJsonValue(NJson::JSON_NULL);
        } else if constexpr (std::is_same_v<TValue, bool>) {
            return NJson::TJsonValue(item ? "1" : "0");
        } else {
            if constexpr (std::is_floating_point_v<TValue>) {
                if (!std::isfinite(item)) {
                    return NJson::TJsonValue(NJson::JSON_NULL);
                }
            }
            // Strings preserve ui64/i64 precision across a JavaScript client.
            return NJson::TJsonValue(ToString(item));
        }
    }, value);
}

struct THistory {
    std::deque<NJson::TJsonValue> Points;
    bool Truncated = false;
};

void CapturePoint(void* opaque, TInstant timestamp, std::span<const TLineNumericValue> values) {
    auto* history = static_cast<THistory*>(opaque);
    NJson::TJsonValue point(NJson::JSON_MAP);
    point["timestamp_ms"] = timestamp.MilliSeconds();
    auto& output = point["values"];
    output.SetType(NJson::JSON_ARRAY);
    for (size_t i = 0; i < std::min(values.size(), MaxHistoryFields); ++i) {
        output.AppendValue(NumericValue(values[i]));
    }
    if (history->Points.size() == MaxHistoryPoints) {
        history->Points.pop_front();
        history->Truncated = true;
    }
    history->Points.push_back(std::move(point));
}
} // namespace

TString SerializeSnapshot(const TInMemorySnapshot& snapshot, const TInMemoryMetricsStats& stats,
                          const TInMemoryMetricsConfig& config, TInstant now, TDuration period,
                          bool includeHistory) {
    NJson::TJsonValue root(NJson::JSON_MAP);
    root["timestamp_ms"] = now.MilliSeconds();
    const auto begin = now >= TInstant::Zero() + period ? now - period : TInstant::Zero();
    root["begin_ms"] = begin.MilliSeconds();
    auto& limits = root["config"];
    limits["memory_bytes"] = config.MemoryBytes;
    limits["chunk_size_bytes"] = config.ChunkSizeBytes;
    limits["max_lines"] = config.MaxLines;
    limits["max_pending_requests"] = config.MaxPendingRequests;
    auto& prefixes = limits["allowed_prefixes"];
    prefixes.SetType(NJson::JSON_ARRAY);
    for (const auto& prefix : config.AllowedMetricPrefixes) {
        prefixes.AppendValue(prefix);
    }
    auto& usage = root["stats"];
    usage["memory_used_bytes"] = stats.MemoryUsedBytes;
    usage["committed_bytes"] = stats.CommittedBytes;
    usage["free_chunks"] = stats.FreeChunks;
    usage["used_chunks"] = stats.UsedChunks;
    usage["sealed_chunks"] = stats.SealedChunks;
    usage["writable_chunks"] = stats.WritableChunks;
    usage["retiring_chunks"] = stats.RetiringChunks;
    usage["lines"] = stats.Lines;
    usage["closed_lines"] = stats.ClosedLines;
    usage["append_failures_total"] = stats.AppendFailuresTotal;
    auto& lines = root["lines"];
    lines.SetType(NJson::JSON_ARRAY);
    snapshot.Read([&](const TSnapshotView& view) {
        NJson::TJsonValue common(NJson::JSON_ARRAY);
        view.ForEachCommonLabel([&](const TLabel& label) {
            NJson::TJsonValue item(NJson::JSON_MAP);
            item["name"] = label.Name;
            item["value"] = label.Value;
            common.AppendValue(std::move(item));
        });
        root["common_labels"] = std::move(common);
        view.ForEachLine([&](const TLineSnapshot& line) {
            NJson::TJsonValue item(NJson::JSON_MAP);
            item["id"] = line.LineId;
            item["chunks"] = line.GetChunkCount();
            item["name"] = line.Name;
            item["labels"] = Labels(line.Labels);
            item["frontend"] = line.Meta.FrontendName();
            item["closed"] = line.Closed;
            if (line.Closed) {
                item["closed_at_ms"] = line.ClosedAt.MilliSeconds();
            }
            const auto* frontend = line.Meta.Frontend;
            item["readable"] = frontend && frontend->ReadNumericRange;
            auto& fields = item["fields"];
            fields.SetType(NJson::JSON_ARRAY);
            if (frontend && !frontend->Fields.empty()) {
                for (size_t i = 0; i < std::min(frontend->Fields.size(), MaxHistoryFields); ++i) {
                    NJson::TJsonValue field(NJson::JSON_MAP);
                    field["name"] = frontend->Fields[i].Name;
                    field["labels"] = Labels(frontend->Fields[i].Labels);
                    fields.AppendValue(std::move(field));
                }
                item["fields_truncated"] = frontend->Fields.size() > MaxHistoryFields;
            } else {
                NJson::TJsonValue field(NJson::JSON_MAP);
                field["name"] = line.Name;
                field["labels"].SetType(NJson::JSON_ARRAY);
                fields.AppendValue(std::move(field));
                item["fields_truncated"] = false;
            }
            if (includeHistory) {
                THistory history;
                if (frontend && frontend->ReadNumericRange) {
                    frontend->ReadNumericRange(line, begin, now, &history, CapturePoint);
                }
                item["truncated"] = history.Truncated;
                auto& points = item["points"];
                points.SetType(NJson::JSON_ARRAY);
                for (auto& point : history.Points) {
                    points.AppendValue(std::move(point));
                }
            }
            lines.AppendValue(std::move(item));
        });
    });
    return NJson::WriteJson(root, false);
}

TString RenderPage() {
    return R"HTML(
<link rel='stylesheet' href='../static/metric-chart/chart.css'>
<style>
.container.imm-page{width:100%;max-width:none;margin-left:0;margin-right:0;padding:0 12px;box-sizing:border-box}.imm-page>.imm{margin:8px 0;padding:0}
.imm{max-width:none;margin:8px auto;padding:0 14px;color:#30343b;font:13px/1.4 Arial,sans-serif}.imm header,.imm-controls,.imm-stats{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.imm header{justify-content:space-between}.imm-controls{padding:6px 0;background:white;border:0;border-bottom:1px solid #e3e7ed;border-radius:0;margin:8px 0}.imm select,.imm input[type=search],.imm button{padding:3px 7px;border:1px solid #cbd2dc;border-radius:5px;background:white}.imm select{max-width:100%}.imm button,.imm input,.imm select{font:inherit;line-height:18px;box-sizing:border-box}.imm button{cursor:pointer}.imm button:hover{background:#edf3fa}.imm table{width:100%;border-collapse:collapse;white-space:nowrap}.imm td,.imm th{padding:5px 8px;border-bottom:1px solid #e4e8ee;text-align:left}.imm th{background:#f5f7fa}.imm-scroll{overflow:auto}.imm-muted{color:#707985;font-size:12px}.imm-error{color:#a32828}.imm details{margin:16px 0}.imm summary{cursor:pointer}.imm a{color:#246da2}.imm-chart{position:relative;border:1px solid #e3e7ed;border-radius:8px;padding:8px;margin-bottom:8px;background:white}.imm-dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:8px}.imm-legend-name{white-space:normal;min-width:200px}.imm-legend-hidden{opacity:.45}.imm-sort{border:0!important;background:none!important;padding:0!important;font-weight:bold}.imm #imm-count{margin:6px 0}.imm [data-control=field]{min-width:0}.imm-empty{padding:70px 20px;text-align:center;color:#707985}.imm-note{padding:10px 14px;background:#fff7e5;border-radius:5px;margin:10px 0}.imm-active{color:#246da2}
@media(max-width:650px){.imm{padding:0 10px}.imm-controls{gap:10px}.imm [data-control=field]{min-width:0;width:100%}.imm-stats{gap:8px}}
.imm-query{border:1px solid #e3e7ed;border-radius:8px;padding:8px;margin-top:8px}.imm-query-heading,.imm-query-actions,.imm-condition{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin:6px 0}.imm-query input,.imm-query textarea{padding:3px 7px;border:1px solid #cbd2dc;border-radius:5px}.imm-query textarea{display:block;width:100%;box-sizing:border-box;font-family:monospace}.imm-query code{display:block;overflow-wrap:anywhere;color:#246da2}.imm-query select{appearance:none;-webkit-appearance:none;background-image:none}.imm-query button[aria-pressed=true]{background:#e4eef9}.imm-condition input{max-width:100%;box-sizing:border-box}

.imm-query{background:#f1f2f4;padding:6px 8px}.imm-query-heading{margin:0 0 4px}.imm-query-heading strong{margin-right:auto}.imm-query-letter{color:#527aff}.imm [data-control=builder]:not([hidden]){display:flex;align-items:center;flex-wrap:wrap;gap:3px;padding:3px;background:white;border:1px solid #bfc4cc;border-radius:7px}.imm [data-control=conditions]{display:contents}.imm-metric-chip,.imm-condition{display:inline-flex;align-items:center;gap:3px;background:#eee;border-radius:4px;padding:0 5px;margin:0;max-width:100%}.imm-metric-chip select{min-width:0;max-width:100%}.imm-metric-chip>span,.imm-condition input[data-part=label]{color:#8850c5}.imm-query .imm-condition input,.imm-query .imm-condition select,.imm-query .imm-condition button,.imm-query .imm-metric-chip select{border:0;background:transparent;padding:2px 1px;font-size:13px}.imm-condition input{width:110px;min-width:0}.imm-condition input[data-part=value]{width:130px}.imm-condition select{width:45px}.imm-condition:focus-within{outline:2px solid #527aff}.imm-query .imm-condition input:focus,.imm-query .imm-condition select:focus{outline:0;background:white}.imm-query .imm-condition button{color:#7c8188;font-size:17px}.imm-query-help{margin:0!important;font-size:12px;color:#707985}.imm [data-control=query-preview][hidden]{display:none}.imm-code-editor{position:relative;background:white;border:1px solid #bfc4cc;border-radius:5px;overflow:hidden}.imm-code-editor pre,.imm-code-editor textarea{margin:0!important;padding:4px 8px 4px 30px!important;font:13px/20px monospace!important;white-space:pre-wrap;overflow-wrap:anywhere;box-sizing:border-box;border:0!important;min-height:28px;width:100%}.imm-code-editor textarea{position:absolute;inset:0;height:100%;resize:none;background:transparent;color:transparent;caret-color:#30343b;overflow:auto}.imm-code-editor pre{pointer-events:none}.imm-line-number{position:absolute;left:10px;top:4px;font:13px/20px monospace;color:#707985}.imm-token-string{color:#2578ed}.imm-token-label{color:#8850c5}.imm-token-op{color:#707985}
.imm details.imm-editor{margin:8px 0}.imm-editor>summary{font-size:12px;color:#707985;display:list-item}.imm-editor>summary span{margin-left:12px}.imm-query-actions{margin-bottom:0}.imm-query-actions .imm-query-help{margin-left:auto!important}.imm button[aria-pressed=true]{background:#e4eef9}.imm #imm-stats{font-size:12px;color:#707985}.imm [data-control=builder] .imm-condition input{line-height:18px}.imm-suggestions{position:fixed;z-index:1000;overflow:auto;padding:4px;background:white;border:1px solid #cbd2dc;border-radius:5px;box-shadow:0 4px 16px #0002;box-sizing:border-box}.imm-suggestions button{display:block;width:100%;text-align:left;border:0;border-radius:3px;padding:4px 8px;overflow-wrap:anywhere}.imm-suggestions button[aria-selected=true]{background:#e4eef9}.imm-suggestions>div{padding:4px 8px}

.imm #imm-charts{display:grid;gap:8px}.imm #imm-charts>.imm-chart{min-width:0}.imm-chart-title{font-weight:bold;margin:0 0 4px}.imm #imm-charts.imm-separated{grid-template-columns:repeat(var(--columns,2),minmax(0,1fr))}@media(max-width:800px){.imm #imm-charts.imm-separated{grid-template-columns:1fr}}
.imm-settings{margin-left:auto;position:relative}.imm-settings>summary{cursor:pointer;border:1px solid #cbd2dc;border-radius:4px;padding:3px 7px;list-style:none}.imm-settings[open]>summary{background:#e4eef9}.imm-settings-panel{position:fixed;right:12px;top:120px;width:280px;max-width:calc(100vw - 24px);max-height:calc(100vh - 136px);overflow:auto;box-sizing:border-box;background:white;border:1px solid #cbd2dc;border-radius:6px;box-shadow:0 4px 20px #0002;padding:10px;z-index:10;display:grid;gap:7px}.imm-settings-panel>label,.imm-settings-panel fieldset>label{display:flex;justify-content:space-between;align-items:center;gap:8px}.imm-settings-panel input[type=number],.imm-settings-panel input[type=text]{width:110px}.imm-settings-panel select{width:110px}.imm-settings-panel [hidden]{display:none!important}.imm-settings-panel [role=alert]{color:#a32828}.imm-settings-panel fieldset{margin:3px 0 0;border:1px solid #e4e8ee;border-radius:4px;padding:7px;display:grid;gap:7px}.imm-settings-panel #imm-line-target{width:100%;min-width:0}.imm-settings-panel #imm-line-settings>label:first-of-type{display:grid}.imm-chart-heading{display:flex;align-items:center;justify-content:space-between;gap:8px}.imm-chart-heading button{font-size:12px}
</style>
<div class='imm' id='imm-root'>
<header><nav><a href='metrics?page=overview'>Overview</a></nav><span id='imm-status' class='imm-muted' role='status'>Loading metrics...</span></header>
<details class='imm-editor' open><summary>Query editor <span class='imm-muted'>Ctrl/Cmd + Enter to apply</span></summary><div id='imm-editors'></div><div class='imm-query-actions'><button id='imm-apply' type='button'>Apply queries</button><button id='imm-add-query' type='button'>Add query</button></div></details><template id='imm-editor-template'><section class='imm-query'>
<div class='imm-query-heading'><strong>Query <span class='imm-query-letter'></span></strong><button type='button' data-control='remove-query'>Remove query</button><button data-control='mode-builder' type='button' aria-pressed='true'>Builder</button><button data-control='mode-text' type='button' aria-pressed='false'>Text query</button></div>
<div data-control='builder'><label class='imm-metric-chip'><span>metric</span> = <select data-control='field' aria-label='Metric field'></select></label><div data-control='conditions'></div><datalist data-control='label-names'></datalist><button data-control='add' type='button'>Add label filter</button></div>
<div data-control='text-editor' hidden><div class='imm-code-editor'><span class='imm-line-number' aria-hidden='true'>1</span><pre data-control='highlight' aria-hidden='true'></pre><textarea data-control='expression' aria-label='Selector expression' maxlength='2048' rows='1' spellcheck='false'></textarea></div></div>

<code data-control='query-preview' hidden></code><div class='imm-query-actions'><button data-control='clear-query' type='button'>Clear label filters</button><details class='imm-query-help'><summary>Query syntax</summary><p class='imm-muted'>All filters are combined with AND. Glob: * any text, ? one character, | alternatives. Negative filters also match missing labels.</p></details><span data-control='query-error' class='imm-error' role='alert'></span></div>
</section></template>

<div id='imm-charts'><div class='imm-chart' id='imm-chart'><div class='imm-empty'>Loading history...</div></div></div>
<div class='imm-controls'>
<input type='search' id='imm-filter' aria-label='Filter series' placeholder='Filter by labels'>
<button id='imm-reset' type='button'>Reset filter</button>
<label>History <select id='imm-period' aria-label='History range'><option value='60'>1 min</option><option value='300' selected>5 min</option><option value='900'>15 min</option><option value='3600'>1 hour</option></select></label>
<button id='imm-now' type='button'>Now</button><button id='imm-refresh' type='button'>Refresh</button>
<label><input type='checkbox' id='imm-auto'> Live via JSON</label><button id='imm-toggle-legend' type='button' aria-pressed='true' aria-controls='imm-legend-panel'>Legend</button>
<details class='imm-settings' id='imm-settings'><summary>Settings</summary><div class='imm-settings-panel'><label><input id='imm-separate' type='checkbox'> One chart per query</label><label id='imm-columns-label' hidden>Charts per row <select id='imm-columns' aria-label='Charts per row'><option>1</option><option selected>2</option><option>3</option></select></label><label id='imm-settings-target-label' hidden>Chart <select id='imm-settings-target' aria-label='Chart settings target'><option value='all'>All charts</option></select></label>
<label>Display <select id='imm-display' aria-label='Display type'><option value='line'>Lines</option><option value='area'>Stacked area</option></select></label>
<label>Fill under lines <input id='imm-fill' type='checkbox'></label>
<label>Line name format <input id='imm-format' type='text' maxlength='512' placeholder='{metric} / {pool}'></label>
<label>Height, px <input id='imm-height' aria-label='Chart height' type='number' min='160' max='800' step='1' value='360'></label>
<label>Units <select id='imm-unit' aria-label='Value units'><option value='number'>Number</option><option value='bytes'>Bytes (IEC)</option><option value='percent'>Percent</option><option value='seconds'>Seconds</option><option value='milliseconds'>Milliseconds</option><option value='cores'>CPU cores</option></select></label>
<label>Decimals <select id='imm-precision' aria-label='Decimal places'><option value='auto'>Auto</option><option>0</option><option>1</option><option>2</option><option>3</option><option>4</option><option>5</option><option>6</option></select></label>
<label>Y minimum <input id='imm-y-min' aria-label='Y minimum' type='number' step='any' placeholder='Auto'></label>
<label>Y maximum <input id='imm-y-max' aria-label='Y maximum' type='number' step='any' placeholder='Auto'></label>
<label>Show legend <input id='imm-settings-legend' type='checkbox' checked></label>
<span class='imm-muted'>Y limits use raw metric units.</span><span id='imm-settings-error' role='alert'></span><button id='imm-settings-reset' type='button'>Reset settings</button>
<fieldset id='imm-line-settings' hidden><legend>Line appearance</legend>
<label>Line <select id='imm-line-target' aria-label='Line settings target'></select></label>
<label>Display <select id='imm-line-display' aria-label='Line display type'><option value=''>Use chart setting</option><option value='line'>Lines</option><option value='area'>Stacked area</option></select></label>
<label>Fill <select id='imm-line-fill' aria-label='Line fill'><option value=''>Use chart setting</option><option value='true'>On</option><option value='false'>Off</option></select></label>
<label>Name format <input id='imm-line-format' aria-label='Line name format override' type='text' maxlength='512' placeholder='Use chart setting'></label>
<label>Color <input id='imm-line-color' aria-label='Line color' type='color'></label>
<label>Width, px <input id='imm-line-width' aria-label='Line width' type='number' min='1' max='6' step='1' value='2'></label>
<button id='imm-line-reset' type='button'>Reset line appearance</button>
</fieldset></div></details></div>
<div id='imm-notes' role='status'></div>
<p id='imm-count' class='imm-muted'></p>
<div class='imm-scroll' id='imm-legend-panel'><table><thead><tr><th><input id='imm-all' type='checkbox' checked aria-label='Show all series'></th><th><button class='imm-sort' data-sort='name' type='button'>Series</button></th><th><button class='imm-sort' data-sort='last' type='button'>Last</button></th><th><button class='imm-sort' data-sort='min' type='button'>Min</button></th><th><button class='imm-sort' data-sort='max' type='button'>Max</button></th><th><button class='imm-sort' data-sort='avg' type='button'>Avg</button></th><th>Samples / state</th></tr></thead><tbody id='imm-legend'></tbody></table></div>

</div>
<script type='module'>
import {createMetricChart,seriesStats,defaultChartSettings,formatMetricValue,formatSeriesName} from '../static/metric-chart/chart.js';
import {createInMemoryMetricsClient,parseQuery,formatQuery} from '../static/metric-chart/client.js';
(() => {
 const $=id=>document.getElementById('imm-'+id);
 $('root').closest('.container')?.classList.add('imm-page');
 const params=new URLSearchParams(location.search);
 let field=params.get('metric')||'harmonizer.budget', lines=[], series=[], common=[], hidden=new Set(), fixed=null, begin=0,end=0,controller,timer,version=0,sort='name',direction=1,lastToggle=null,limited=false;
 const text=(tag,value,cls)=>{const n=document.createElement(tag);n.textContent=value;if(cls)n.className=cls;return n;};
 const numeric=v=>v===null||v===undefined?null:Number.isFinite(Number(v))?Number(v):null;
 const visible=()=>series.map(s=>({...s,...lineSettings.get(s.key)})).map(s=>({...s,display:formatSeriesName(s,s.format??settingsFor(s.queryId).format)})).filter(s=>(s.display+' '+s.metric+' '+s.name+' '+s.labels).toLowerCase().includes($('filter').value.toLowerCase()));
 const client=createInMemoryMetricsClient({endpoint:location.pathname});
 let charts=[],chartSettings={...defaultChartSettings,legend:true};
 const querySettings=new Map(),lineSettings=new Map();
 const settingsFor=id=>$('separate').checked?{...chartSettings,...querySettings.get(id)}:chartSettings;
 const settingsTarget=()=>$('settings-target').value==='all'?null:Number($('settings-target').value);
 let editors=[],appliedQueries=[],nextQueryId=0;
 const pageControl=$;
 function createEditor(initial){
  const id=++nextQueryId,card=pageControl('editor-template').content.firstElementChild.cloneNode(true);
  for(const control of card.querySelectorAll('[data-control]'))control.id='imm-q'+id+'-'+control.dataset.control;
  const global$=pageControl;
  const $=name=>card.querySelector('[data-control="'+name+'"]')||(name==='root'?card:global$(name));
  $('editors').append(card);
  let textMode=false,field=initial.metric;
  card.querySelector('.imm-query-letter').textContent=String.fromCharCode(64+id);

 function draftQuery(){return {metric:$('field').value,filters:[...$('conditions').children].map(row=>({label:row.querySelector('[data-part=label]').value,op:row.querySelector('[data-part=op]').value,value:row.querySelector('[data-part=value]').value}))};}
 function highlightQuery(){
  const source=$('expression').value,pattern=/"(?:\\.|[^"\\])*"|[A-Za-z_][A-Za-z0-9_.-]*|!==|==|!=|=/g;let start=0;$('highlight').replaceChildren();
  for(const match of source.matchAll(pattern)){if(match.index>start)$('highlight').append(document.createTextNode(source.slice(start,match.index)));const token=match[0],suffix=source.slice(match.index+token.length);const cls=token[0]==='"'&&!/^\s*(?:!==|==|!=|=)/.test(suffix)?'imm-token-string':/^[!=]/.test(token)?'imm-token-op':'imm-token-label';$('highlight').append(text('span',token,cls));start=match.index+token.length;}
  $('highlight').append(document.createTextNode(source.slice(start)+(source.endsWith('\n')?' ':'')));
 }
 let suggestionInput=null,suggestionOptions=[],suggestionIndex=-1;
 const suggestionPopup=text('div','','imm-suggestions');suggestionPopup.id='imm-suggestions-'+id;suggestionPopup.setAttribute('role','listbox');suggestionPopup.hidden=true;$('root').append(suggestionPopup);
 function closeSuggestions(){if(suggestionInput){suggestionInput.setAttribute('aria-expanded','false');suggestionInput.removeAttribute('aria-activedescendant');}suggestionPopup.hidden=true;suggestionInput=null;suggestionOptions=[];suggestionIndex=-1;}
 function selectSuggestion(index){if(!suggestionInput||!suggestionOptions[index])return;const input=suggestionInput;input.value=suggestionOptions[index];input.dispatchEvent(new Event('input',{bubbles:true}));closeSuggestions();input.focus();}
 function highlightSuggestion(){for(const [index,option] of [...suggestionPopup.querySelectorAll('[role=option]')].entries())option.setAttribute('aria-selected',String(index===suggestionIndex));const option=suggestionPopup.querySelector('[aria-selected=true]');if(option){suggestionInput.setAttribute('aria-activedescendant',option.id);option.scrollIntoView({block:'nearest'});}}
 function showSuggestions(input){
  if(suggestionInput&&suggestionInput!==input)closeSuggestions();suggestionInput=input;
  const source=input.dataset.part==='label'?[...$('label-names').options].map(o=>o.value):labelSuggestions(input.closest('.imm-condition').querySelector('[data-part=label]').value);
  const needle=input.value.toLocaleLowerCase();suggestionOptions=source.filter(value=>value.toLocaleLowerCase().includes(needle)).slice(0,50);suggestionIndex=-1;suggestionPopup.replaceChildren();
  for(const [index,value] of suggestionOptions.entries()){const option=text('button',value);option.type='button';option.tabIndex=-1;option.id='imm-suggestion-'+id+'-'+index;option.setAttribute('role','option');option.setAttribute('aria-selected','false');option.addEventListener('pointerdown',e=>e.preventDefault());option.addEventListener('click',()=>selectSuggestion(index));suggestionPopup.append(option);}
  if(!suggestionOptions.length)suggestionPopup.append(text('div','No suggestions','imm-muted'));
  const rect=input.getBoundingClientRect(),width=Math.min(320,innerWidth-16);suggestionPopup.style.width=width+'px';suggestionPopup.style.left=Math.max(8,Math.min(rect.left,innerWidth-width-8))+'px';suggestionPopup.style.top=(rect.bottom+4)+'px';suggestionPopup.style.maxHeight=Math.max(60,Math.min(220,innerHeight-rect.bottom-12))+'px';suggestionPopup.hidden=false;input.setAttribute('aria-expanded','true');input.removeAttribute('aria-activedescendant');
 }
 function attachSuggestions(input){
  input.removeAttribute('list');input.autocomplete='off';input.setAttribute('role','combobox');input.setAttribute('aria-autocomplete','list');input.setAttribute('aria-controls',suggestionPopup.id);input.setAttribute('aria-expanded','false');
  input.addEventListener('focus',()=>showSuggestions(input));input.addEventListener('input',()=>showSuggestions(input));input.addEventListener('blur',closeSuggestions);
  input.addEventListener('keydown',e=>{if(e.key==='Escape'){e.preventDefault();closeSuggestions();}else if(e.key==='ArrowDown'||e.key==='ArrowUp'){e.preventDefault();if(suggestionPopup.hidden)showSuggestions(input);if(suggestionOptions.length){suggestionIndex=suggestionIndex<0?(e.key==='ArrowDown'?0:suggestionOptions.length-1):(suggestionIndex+(e.key==='ArrowDown'?1:-1)+suggestionOptions.length)%suggestionOptions.length;highlightSuggestion();}}else if(e.key==='Enter'&&!e.ctrlKey&&!e.metaKey&&!suggestionPopup.hidden&&suggestionIndex>=0){e.preventDefault();selectSuggestion(suggestionIndex);}});
 }
 const outsidePopup=e=>{if(!suggestionPopup.contains(e.target)&&e.target!==suggestionInput)closeSuggestions();},scrollPopup=e=>{if(!suggestionPopup.contains(e.target))closeSuggestions();};document.addEventListener('pointerdown',outsidePopup);window.addEventListener('scroll',scrollPopup,true);window.addEventListener('resize',closeSuggestions);
 const queryMeasure=document.createElement('canvas').getContext('2d');
 function fitDraft(){
  const fit=(control,value,extra,limit)=>{if(queryMeasure)queryMeasure.font=getComputedStyle(control).font;const width=queryMeasure?queryMeasure.measureText(value).width:value.length*7;control.style.width=Math.min(limit,Math.ceil(width)+extra)+'px';};
  fit($('field'),$('field').value,8,420);
  for(const row of $('conditions').children){for(const input of row.querySelectorAll('input'))fit(input,input.value||input.placeholder,6,180);const op=row.querySelector('select');fit(op,op.value,6,50);}
 }
 function updateDraft(){fitDraft();if(!textMode)$('expression').value=formatQuery(draftQuery());$('query-preview').textContent=$('expression').value;highlightQuery();}
 function labelSuggestions(label){return [...new Set(lines.flatMap(l=>l.fields.filter(f=>f.name===$('field').value).flatMap(f=>[...common,...l.labels,...f.labels].filter(v=>v.name===label).map(v=>v.value))))].sort();}
 function addCondition(filter={label:'',op:'==',value:''}){
  if($('conditions').children.length>=12){$('query-error').textContent='At most 12 label filters are allowed.';return;}
  const row=document.createElement('div');row.className='imm-condition';
  const label=document.createElement('input');label.dataset.part='label';label.setAttribute('aria-label','Label name');label.placeholder='Label';label.maxLength=128;label.value=filter.label;label.setAttribute('list','imm-label-names');
  const op=document.createElement('select');op.dataset.part='op';op.setAttribute('aria-label','Match operator');for(const [value,title] of [['==','Equals'],['!==','Does not equal'],['=','Matches glob'],['!=','Does not match glob']]){const option=text('option',value);option.title=title;option.value=value;op.append(option);}op.value=filter.op;
  const value=document.createElement('input');value.dataset.part='value';value.setAttribute('aria-label','Label value');value.placeholder='Value';value.maxLength=256;value.value=filter.value;
  const remove=text('button','\u00d7');remove.type='button';remove.setAttribute('aria-label','Remove label filter');remove.addEventListener('click',()=>{row.remove();updateDraft();});
  row.append(label,op,value,remove);$('conditions').append(row);
  label.addEventListener('input',updateDraft);op.addEventListener('change',updateDraft);value.addEventListener('input',updateDraft);updateDraft();attachSuggestions(label);attachSuggestions(value);if(!filter.label)label.focus();
 }
 function refreshSuggestions(){const names=[...new Set(lines.flatMap(l=>l.fields.filter(f=>f.name===$('field').value).flatMap(f=>[...common,...l.labels,...f.labels].map(v=>v.name))))].sort();$('label-names').replaceChildren();for(const name of names){const option=text('option',name);option.value=name;$('label-names').append(option);}}
 function loadDraft(query){closeSuggestions();if(![...$('field').options].some(o=>o.value===query.metric)){const option=text('option',query.metric);option.value=query.metric;$('field').append(option);}$('field').value=query.metric;$('conditions').replaceChildren();for(const f of query.filters)addCondition(f);$('expression').value=formatQuery(query);$('query-preview').textContent=$('expression').value;highlightQuery();refreshSuggestions();fitDraft();}
 function changeMode(next){closeSuggestions();try{if(!next)loadDraft(parseQuery($('expression').value));else updateDraft();textMode=next;$('builder').hidden=next;$('text-editor').hidden=!next;$('mode-builder').setAttribute('aria-pressed',String(!next));$('mode-text').setAttribute('aria-pressed',String(next));$('query-error').textContent='';}catch(e){$('query-error').textContent=e.message;}}
 function read(){if(!textMode)updateDraft();try{const query=parseQuery($('expression').value);$('query-error').textContent='';return query;}catch(e){$('query-error').textContent=e.message;throw e;}}
 function metadata(){
  const fields=[...new Set(lines.flatMap(l=>l.fields.map(f=>f.name)))].sort(),draftMetric=$('field').value||field;
  if(!fields.includes(draftMetric))fields.push(draftMetric);
  const signature=JSON.stringify(fields);
  if($('field').dataset.signature!==signature){$('field').replaceChildren();for(const f of fields){const o=text('option',f);o.value=f;$('field').append(o);}$('field').dataset.signature=signature;}
  $('field').value=draftMetric;refreshSuggestions();updateDraft();
 }
 $('field').addEventListener('change',()=>{refreshSuggestions();updateDraft();});
 $('add').addEventListener('click',()=>addCondition());$('mode-builder').addEventListener('click',()=>changeMode(false));$('mode-text').addEventListener('click',()=>changeMode(true));$('expression').addEventListener('input',updateDraft);
 $('clear-query').addEventListener('click',()=>loadDraft({metric:$('field').value,filters:[]}));
 $('remove-query').addEventListener('click',()=>{if(editors.length===1)return;closeSuggestions();document.removeEventListener('pointerdown',outsidePopup);window.removeEventListener('scroll',scrollPopup,true);window.removeEventListener('resize',closeSuggestions);card.remove();editors=editors.filter(e=>e.id!==id);appliedQueries=appliedQueries.filter(q=>q.id!==id);querySettings.delete(id);renumber();remember();refresh();});
 loadDraft(initial);if(lines.length)metadata();
 const result={id,card,read,metadata};return result;
 }
 function renumber(){editors.forEach((editor,index)=>{editor.card.querySelector('.imm-query-letter').textContent=String.fromCharCode(65+index);editor.card.querySelector('[data-control=remove-query]').disabled=editors.length===1;});$('add-query').disabled=editors.length>=8;}
 function addEditor(query){if(editors.length>=8)return;editors.push(createEditor(query));renumber();}
 function applyQueries(){try{const next=editors.map(e=>({...e.read(),id:e.id}));appliedQueries=next;fixed=null;remember();refresh();}catch(error){$('status').textContent='Correct the invalid query before applying.';}}
 function remember(){const url=new URL(location.href);url.searchParams.delete('line');url.searchParams.delete('metric');url.searchParams.delete('query');url.searchParams.set('queries',JSON.stringify(appliedQueries.map(q=>formatQuery(q))));url.searchParams.set('separate',String($('separate').checked));url.searchParams.set('columns',$('columns').value);history.replaceState(null,'',url);}
 function showMetadata(data){common=data.common_labels||[];editors.forEach(e=>e.metadata());}
 const stats=s=>seriesStats(s,begin,end);
 function legend(){
  const list=visible().filter(s=>settingsFor(s.queryId).legend).map(s=>({...s,stat:stats(s)}));
  $('legend-panel').hidden=!list.length;$('toggle-legend').setAttribute('aria-pressed',String(list.length>0));
  list.sort((a,b)=>{if(sort==='name')return direction*a.display.localeCompare(b.display);const x=numeric(a.stat[sort]),y=numeric(b.stat[sort]);return x===null?y===null?0:1:y===null?-1:direction*(x-y);});
  $('legend').replaceChildren();$('count').textContent=list.length+' / '+series.length+' series'+(limited?' (matching lines limited)':'');
  list.forEach((s,index)=>{const r=document.createElement('tr');if(hidden.has(s.key))r.className='imm-legend-hidden';const c=document.createElement('td'),check=document.createElement('input');check.type='checkbox';check.checked=!hidden.has(s.key);check.setAttribute('aria-label','Show '+(s.display));
   check.addEventListener('click',e=>{const anchor=list.findIndex(line=>line.key===lastToggle);if(e.shiftKey&&anchor!==-1){for(let i=Math.min(index,anchor);i<=Math.max(index,anchor);i++)check.checked?hidden.delete(list[i].key):hidden.add(list[i].key);}else check.checked?hidden.delete(s.key):hidden.add(s.key);lastToggle=s.key;draw();});c.append(check);r.append(c);
   const name=text('td',s.display,'imm-legend-name'),dot=text('span','','imm-dot');dot.style.background=s.color;name.prepend(dot);r.append(name);
   for(const f of ['last','min','max','avg'])r.append(text('td',formatMetricValue(s.stat[f],settingsFor(s.queryId))));
   r.append(text('td',s.stat.count+' / '+(!s.readable?'Unsupported':s.closed?'Closed':s.points.length?'Active':'No retained samples')+(s.truncated?' / truncated':'')));$('legend').append(r);
  });$('all').checked=list.length>0&&list.every(s=>!hidden.has(s.key));$('all').indeterminate=list.some(s=>hidden.has(s.key))&&list.some(s=>!hidden.has(s.key));
 }
 function draw(){
  if(fixed){[begin,end]=fixed;}else{end=series.length?Math.max(...series.map(s=>s.end)):Date.now();begin=end-Number($('period').value)*1000;}

  legend();for(const chart of charts)chart.destroy();charts=[];$('charts').replaceChildren();$('notes').replaceChildren();
  if(series.some(s=>s.truncated))$('notes').append(text('p','History is truncated to the latest 1000 points per line.','imm-note'));
  if(limited)$('notes').append(text('p','Comparison is limited to 16 matching lines per query and 64 series in total.','imm-note'));
  const separate=$('separate').checked;$('charts').classList.toggle('imm-separated',separate);$('charts').style.setProperty('--columns',$('columns').value);$('columns-label').hidden=!separate;updateSettingsTargets();
  const groups=separate&&appliedQueries.length?appliedQueries.map((q,index)=>({id:q.id,title:'Query '+String.fromCharCode(65+index)+' \u00b7 '+q.metric})): [{id:null,title:'All queries'}];
  for(const group of groups){const chart=text('div','','imm-chart');$('charts').append(chart);if(separate){const heading=text('div','','imm-chart-heading'),button=text('button','Settings');button.type='button';button.setAttribute('aria-label','Settings for '+group.title);button.addEventListener('click',()=>{$('settings-target').value=String(group.id);$('settings').open=true;loadSettings();});heading.append(text('div',group.title,'imm-chart-title'),button);chart.append(heading);}drawChart(chart,group);}
 }
 function drawChart(element,group){
  const active=visible().filter(s=>!hidden.has(s.key)&&(group.id===null||s.queryId===group.id));
  const host=document.createElement('div');element.append(host);
  const chart=createMetricChart(host,{settings:settingsFor(group.id),onPin:()=>{$('auto').checked=false;clearTimeout(timer);},onRangeChange:({from,to})=>{fixed=[from,to];$('auto').checked=false;clearTimeout(timer);draw();}});charts.push(chart);
  chart.setData({series:active,begin,end,title:group.title,emptyText:!appliedQueries.length?'No applied queries. Use Apply queries.':!series.length?'No lines match the applied query.':active.length?'No retained numeric samples in this interval':'No visible series. Select rows in the legend.'});
 }
 async function refresh(){
  clearTimeout(timer);if(controller)controller.abort();controller=new AbortController();const current=++version,signal=controller.signal;$('status').textContent='Loading metrics...';$('status').className='imm-muted';
  try{const data=await client.request({seconds:$('period').value,signal});if(current!==version)return;lines=data.lines;showMetadata(data);
   const response=await client.queryMany(appliedQueries,{seconds:$('period').value,signal,catalog:data});if(current!==version)return;limited=response.limited;const result=response.series;
   series=result;for(const key of lineSettings.keys())if(!result.some(s=>s.key===key))lineSettings.delete(key);hidden=new Set([...hidden].filter(k=>result.some(s=>s.key===k)));lastToggle=null;draw();$('status').textContent='Updated '+new Date().toLocaleTimeString();
  }catch(e){if(e.name==='AbortError')return;$('status').textContent=e.message+' \u00b7 Showing last successful data (stale)';$('status').className='imm-error';}
  finally{if(current===version&&$('auto').checked&&!fixed)timer=setTimeout(refresh,2000);}
 }
 function updateSettingsTargets(){
  const previous=$('settings-target').value;$('settings-target').replaceChildren(text('option','All charts'));
  $('settings-target').firstElementChild.value='all';
  if($('separate').checked)for(const [index,query] of appliedQueries.entries()){const option=text('option','Query '+String.fromCharCode(65+index)+' - '+query.metric);option.value=query.id;$('settings-target').append(option);}
  $('settings-target').value=[...$('settings-target').options].some(o=>o.value===previous)?previous:'all';$('settings-target-label').hidden=!$('separate').checked;loadSettings();
 }
 function loadSettings(){
  const id=settingsTarget(),settings=id===null?chartSettings:settingsFor(id);
  $('display').value=settings.type;$('fill').checked=settings.fill;$('format').value=settings.format;$('height').value=settings.height;$('unit').value=settings.unit;$('precision').value=settings.precision===null?'auto':settings.precision;$('y-min').value=settings.min??'';$('y-max').value=settings.max??'';$('settings-legend').checked=settings.legend;$('settings-error').textContent='';$('fill').disabled=settings.type==='area';
  const previous=$('line-target').value;$('line-target').replaceChildren();
  if(id!==null)for(const line of series.filter(s=>s.queryId===id)){const option=text('option',line.name+(line.labels?' - '+line.labels:''));option.value=line.key;$('line-target').append(option);}
  if([...$('line-target').options].some(o=>o.value===previous))$('line-target').value=previous;
  $('line-settings').hidden=id===null||!$('line-target').options.length;loadLineSettings();
 }
 function loadLineSettings(){
  const line=series.find(s=>s.key===$('line-target').value);if(!line)return;
  const override=lineSettings.get(line.key)||{};$('line-display').value=override.type||'';$('line-format').value=override.format||'';$('line-fill').value=override.fill===undefined?'':String(override.fill);$('line-color').value=override.color||line.color;$('line-width').value=override.width||2;const area=(override.type||settingsFor(line.queryId).type)==='area';$('line-fill').disabled=area;$('line-width').disabled=area;
 }
 function storeSettings(patch){const id=settingsTarget();if(id===null)chartSettings={...chartSettings,...patch};else querySettings.set(id,{...querySettings.get(id),...patch});draw();}
 function applySettings(){
  const minimum=$('y-min').value===''?null:Number($('y-min').value),maximum=$('y-max').value===''?null:Number($('y-max').value),height=Number($('height').value);
  if(!$('height').checkValidity()||height<160||height>800||!$('y-min').checkValidity()||!$('y-max').checkValidity()||(minimum!==null&&!Number.isFinite(minimum))||(maximum!==null&&!Number.isFinite(maximum))||(minimum!==null&&maximum!==null&&minimum>=maximum)){$('settings-error').textContent='Use height 160-800 and Y minimum below maximum.';return;}
  const next={type:$('display').value,fill:$('fill').checked,format:$('format').value,height,unit:$('unit').value,precision:$('precision').value==='auto'?null:Number($('precision').value),min:minimum,max:maximum},id=settingsTarget(),current=id===null?chartSettings:settingsFor(id);
  storeSettings(Object.fromEntries(Object.entries(next).filter(([key,value])=>value!==current[key])));
 }
 for(const name of ['display','fill','format','height','unit','precision','y-min','y-max'])$(name).addEventListener('input',applySettings);
 $('settings-target').addEventListener('change',loadSettings);
 $('settings-legend').addEventListener('input',()=>storeSettings({legend:$('settings-legend').checked}));
 $('settings-reset').addEventListener('click',()=>{const id=settingsTarget();if(id===null){chartSettings={...defaultChartSettings,legend:true};querySettings.clear();lineSettings.clear();}else querySettings.delete(id);draw();});
 $('toggle-legend').addEventListener('click',()=>{const show=$('legend-panel').hidden;chartSettings.legend=show;for(const settings of querySettings.values())delete settings.legend;draw();});
 $('line-target').addEventListener('change',loadLineSettings);
 for(const name of ['line-display','line-fill','line-format','line-color','line-width'])$(name).addEventListener('input',()=>{if(!$('line-width').checkValidity())return;const key=$('line-target').value;if(!key)return;lineSettings.set(key,{type:$('line-display').value,format:$('line-format').value||undefined,fill:$('line-fill').value===''?undefined:$('line-fill').value==='true',color:$('line-color').value,width:Number($('line-width').value)});draw();});
 $('line-reset').addEventListener('click',()=>{lineSettings.delete($('line-target').value);draw();});
 $('root').addEventListener('keydown',e=>{if(e.key==='Enter'&&(e.ctrlKey||e.metaKey)&&!e.defaultPrevented){e.preventDefault();applyQueries();}});
 $('apply').addEventListener('click',applyQueries);$('add-query').addEventListener('click',()=>{addEditor({metric:editors.at(-1).card.querySelector('[data-control=field]').value,filters:[]});});
 $('separate').addEventListener('change',()=>{remember();draw();});$('columns').addEventListener('change',()=>{remember();draw();});
 $('filter').addEventListener('input',()=>{lastToggle=null;draw();});$('reset').addEventListener('click',()=>{$('filter').value='';lastToggle=null;draw();});
 $('all').addEventListener('change',()=>{for(const s of visible())$('all').checked?hidden.delete(s.key):hidden.add(s.key);draw();});
 for(const b of document.querySelectorAll('.imm-sort'))b.addEventListener('click',()=>{const next=b.dataset.sort;direction=sort===next?-direction:next==='name'?1:-1;sort=next;lastToggle=null;legend();});
 $('period').addEventListener('change',()=>{fixed=null;refresh();});$('now').addEventListener('click',()=>{fixed=null;refresh();});$('refresh').addEventListener('click',refresh);
 $('auto').addEventListener('change',()=>{clearTimeout(timer);if($('auto').checked){fixed=null;refresh();}});
  window.addEventListener('pagehide',event=>{if(!event.persisted)for(const chart of charts)chart.destroy();++version;clearTimeout(timer);if(controller)controller.abort();});
 window.addEventListener('pageshow',event=>{if(event.persisted)refresh();});
 try{
  const sources=params.has('queries')?JSON.parse(params.get('queries')):[params.get('query')||field];
  if(!Array.isArray(sources)||sources.length>8||sources.some(q=>typeof q!=='string'))throw Error('Expected at most 8 queries.');
  const initial=sources.map(parseQuery);for(const query of initial)addEditor(query);if(!initial.length)addEditor({metric:field,filters:[]});
  appliedQueries=initial.map((query,index)=>({...query,id:editors[index].id}));$('separate').checked=params.get('separate')==='true';$('columns').value=['1','2','3'].includes(params.get('columns'))?params.get('columns'):'2';refresh();
 }catch(e){if(!editors.length)addEditor({metric:field,filters:[]});$('status').textContent='Invalid query URL: '+e.message;}

})();
</script>
)HTML";
}



TString RenderOverviewPage() {
    return R"HTML(
<link rel='stylesheet' href='../static/metric-chart/chart.css'>
<style>
.container.imo-page{width:100%;max-width:none;margin:0;padding:0 12px;box-sizing:border-box}.imo{font:13px/1.4 Arial,sans-serif;color:#30343b;margin:8px 0}.imo header{display:flex;align-items:center;gap:12px;flex-wrap:wrap}.imo header span{margin-left:auto;color:#707985}.imo a{color:#246da2}.imo button,.imo input,.imo select{font:inherit;padding:3px 7px;border:1px solid #cbd2dc;border-radius:4px;background:white}.imo h3{font-size:14px;margin:14px 0 6px}.imo table{width:100%;border-collapse:collapse}.imo th,.imo td{text-align:left;padding:5px 8px;border-bottom:1px solid #e4e8ee;vertical-align:top}.imo th{background:#f5f7fa}.imo-scroll{overflow:auto}.imo #imo-summary{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}.imo-summary-item{padding:8px 12px;background:#f5f7fa;border:1px solid #e3e7ed;border-radius:5px}.imo dl{display:grid;grid-template-columns:max-content 1fr;gap:4px 16px;margin:8px 0}.imo dd{margin:0;overflow-wrap:anywhere}.imo td a{display:block}.imo-error{color:#a32828!important}.imo-details{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:24px}.imo-details section{min-width:0}.imo-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.imo-card{min-width:0;border:1px solid #e3e7ed;border-radius:8px;padding:8px;background:white}.imo-card h3{margin:0 0 6px}.imo-card nav{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px}.imo-card nav a{font-size:12px}@media(max-width:800px){.imo-grid{grid-template-columns:1fr}}@media(max-width:700px){.imo-details{grid-template-columns:1fr;gap:0}}
</style>
<div class='imo' id='imo-root'>
<header><a href='metrics'>Metric viewer</a><label>History <select id='imo-period' aria-label='History range'><option value='60'>1 min</option><option value='300' selected>5 min</option><option value='900'>15 min</option><option value='3600'>1 hour</option></select></label><button id='imo-now' type='button'>Now</button><label><input id='imo-live' type='checkbox'> Live via JSON</label><button id='imo-refresh' type='button'>Refresh</button><span id='imo-status' role='status'>Loading registry...</span></header>
<div id='imo-summary'></div>
<div class='imo-grid' id='imo-cards'></div>
<div class='imo-details'><section><h3>Registry configuration</h3><dl id='imo-config'></dl></section>
<section><h3>Storage state</h3><dl id='imo-storage'></dl></section></div>
<section><h3>Chunk allocation by storage line</h3><div id='imo-allocation'></div></section>
<h3>Metric lines</h3><input id='imo-filter' type='search' aria-label='Filter metric lines' placeholder='Filter by metric or labels'>
<div class='imo-scroll'><table><thead><tr><th>ID</th><th>Line</th><th>Metrics</th><th>Labels</th><th>Frontend</th><th>Chunks</th><th>State</th></tr></thead><tbody id='imo-lines'></tbody></table></div>
</div>
<script type='module' src='../static/inmemory-metrics/overview.js'></script>
)HTML";
}

} // namespace NKikimr::NInMemoryMetricsMonitoring
