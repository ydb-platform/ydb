import {createAllocationBar} from '../metric-chart/allocation.js';
import {createMetricChart} from '../metric-chart/chart.js';
import {createInMemoryMetricsClient} from '../metric-chart/client.js';

const $=id=>document.getElementById('imo-'+id);
const text=(tag,value)=>{const element=document.createElement(tag);element.textContent=value;return element;};
$('root').closest('.container')?.classList.add('imo-page');
const client=createInMemoryMetricsClient({endpoint:'metrics'});
const panels=[
    {title:'Memory',unit:'bytes',names:{memory_used_bytes:'Allocated chunk memory',committed_bytes:'Recorded bytes'},metrics:['memory_used_bytes','committed_bytes']},
    {title:'Metric lines',metrics:['lines','closed_lines']},
    {title:'Chunks',type:'area',names:{used_chunks:'Used',free_chunks:'Free'},metrics:['used_chunks','free_chunks']},
    {title:'Append failures',metrics:['append_failures_total']},
];
const queries=panels.flatMap(panel=>panel.metrics.map(metric=>({id:metric,metric:'inmemory_metrics.'+metric,filters:[]})));
let data=null,series=[],fixed=null,selectedLine=null,controller,timer,version=0;
const allocation=createAllocationBar($('allocation'),{legend:false,maxSegments:64,onPin:pause,onSelect:segment=>{selectedLine=segment.lineId;$('filter').value=segment.name;drawLines();}});
function pause(){clearTimeout(timer);$('live').checked=false;}
const cards=panels.map(panel=>{
    const card=text('section','');card.className='imo-card';card.append(text('h3',panel.title));
    const links=text('nav','');for(const metric of panel.metrics){const link=text('a',metric);link.href='metrics?'+new URLSearchParams({metric:'inmemory_metrics.'+metric});links.append(link);}card.append(links);
    const host=text('div','');card.append(host);$('cards').append(card);
    const chart=createMetricChart(host,{legend:true,settings:{type:panel.type||'line',height:240,unit:panel.unit||'number',format:'{metric}'},onPin:pause,onCursorChange:time=>{for(const {chart} of cards)chart.setCursor(time);},onRangeChange:({from,to})=>{pause();fixed=[from,to];draw();}});
    return {panel,chart};
});
function draw(){
    const end=fixed?fixed[1]:series.length?Math.max(...series.map(s=>s.end)):Date.now(),begin=fixed?fixed[0]:end-Number($('period').value)*1000;
    for(const {panel,chart} of cards)chart.setData({series:series.filter(s=>panel.metrics.includes(s.queryId)).map(s=>({...s,format:panel.names?.[s.queryId]||'{metric}'})),begin,end,title:panel.title,emptyText:'No retained data. Check that inmemory_metrics. is allowed in the registry.'});
}
 const labels=values=>(values||[]).map(label=>label.name+'='+label.value).join(', ');
 function entries(id,values){$(id).replaceChildren();for(const [label,value] of values)$(id).append(text('dt',label),text('dd',value===undefined?'Unavailable':String(value)));}
 function drawLines(){
  if(!data)return;const needle=$('filter').value.toLowerCase();$('lines').replaceChildren();
  for(const line of data.lines){if(selectedLine!==null&&String(line.id)!==String(selectedLine))continue;if(!(line.name+' '+line.fields.map(field=>field.name).join(' ')+' '+labels(line.labels)).toLowerCase().includes(needle))continue;const row=document.createElement('tr'),name=text('td',line.name),dot=text('span','');dot.className='ymc-dot';dot.style.background=allocation.getColor('line-'+line.id);name.prepend(dot);row.addEventListener('pointerenter',()=>allocation.highlight('line-'+line.id));row.addEventListener('pointerleave',()=>allocation.highlight(null));row.append(text('td',line.id),name);const metrics=document.createElement('td');for(const field of line.fields){const link=text('a',field.name);link.href='metrics?'+new URLSearchParams({metric:field.name});metrics.append(link);}row.append(metrics,text('td',labels(line.labels)),text('td',line.frontend),text('td',line.chunks??'Unavailable'),text('td',(line.closed?'Closed':'Active')+(line.readable?'':' / Unsupported')));$('lines').append(row);}
 }

function drawRegistry(){
 const c=data.config,s=data.stats;$('summary').replaceChildren();
 allocation.setData({capacity:Math.floor(c.memory_bytes/c.chunk_size_bytes),free:s.free_chunks,segments:data.lines.map(line=>({key:'line-'+line.id,lineId:line.id,name:line.name,label:line.name+(labels(line.labels)?' · '+labels(line.labels):'')+' (#'+line.id+')',value:line.chunks}))});
   for(const value of ['Memory '+(s.memory_used_bytes/1048576).toFixed(2)+' / '+(c.memory_bytes/1048576).toFixed(2)+' MiB','Lines '+s.lines,'Closed lines '+s.closed_lines,'Append failures '+s.append_failures_total]){const item=text('div',value);item.className='imo-summary-item';$('summary').append(item);}
   entries('config',[['Memory limit',c.memory_bytes+' bytes'],['Chunk size',c.chunk_size_bytes+' bytes'],['Line limit',c.max_lines],['Pending request limit',c.max_pending_requests],['Allowed prefixes',c.allowed_prefixes.join(', ')],['Common labels',labels(data.common_labels)]]);
   entries('storage',[['Committed bytes',s.committed_bytes],['Free chunks',s.free_chunks],['Used chunks',s.used_chunks],['Sealed chunks',s.sealed_chunks],['Writable chunks',s.writable_chunks],['Retiring chunks',s.retiring_chunks]]);drawLines();
}
async function refresh(){
    clearTimeout(timer);controller?.abort();controller=new AbortController();const current=++version;
    $('status').textContent='Loading metrics...';$('status').className='';
    try{
        const catalog=await client.request({seconds:$('period').value,signal:controller.signal});if(current!==version)return;
        data=catalog;drawRegistry();
        const response=await client.queryMany(queries,{seconds:$('period').value,signal:controller.signal,catalog});if(current!==version)return;
        series=response.series;
        draw();$('status').textContent='Updated '+new Date(response.catalog.timestamp_ms).toLocaleTimeString();
    }catch(error){if(error.name==='AbortError')return;$('status').className='imo-error';$('status').textContent=error.message+' · Showing last successful data (stale)';}
    finally{if(current===version&&$('live').checked&&!fixed)timer=setTimeout(refresh,2000);}
}
$('refresh').addEventListener('click',refresh);$('filter').addEventListener('input',()=>{selectedLine=null;drawLines();});
$('now').addEventListener('click',()=>{fixed=null;refresh();});
$('period').addEventListener('change',()=>{fixed=null;refresh();});
$('live').addEventListener('change',()=>{clearTimeout(timer);if($('live').checked){fixed=null;refresh();}});
window.addEventListener('pagehide',event=>{++version;clearTimeout(timer);controller?.abort();if(!event.persisted){allocation.destroy();for(const {chart} of cards)chart.destroy();}});
window.addEventListener('pageshow',event=>{if(event.persisted)refresh();});
refresh();
