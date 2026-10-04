import {createMetricChart} from '../metric-chart/chart.js';
import {createInMemoryMetricsClient} from '../metric-chart/client.js';

const $=id=>document.getElementById('imd-'+id);
const text=(tag,value)=>{const element=document.createElement(tag);element.textContent=value;return element;};
$('root').closest('.container')?.classList.add('imd-page');
const client=createInMemoryMetricsClient({endpoint:'metrics'});
const panels=[
    {title:'Memory, bytes',metrics:['memory_used_bytes','committed_bytes']},
    {title:'Metric lines',metrics:['lines','closed_lines']},
    {title:'Chunks',metrics:['used_chunks','free_chunks']},
    {title:'Append failures',metrics:['append_failures_total']},
];
const queries=panels.flatMap(panel=>panel.metrics.map(metric=>({id:metric,metric:'inmemory_metrics.'+metric,filters:[]})));
let series=[],fixed=null,controller,timer,version=0;
function pause(){clearTimeout(timer);$('live').checked=false;}
const cards=panels.map(panel=>{
    const card=text('section','');card.className='imd-card';card.append(text('h3',panel.title));
    const links=text('nav','');for(const metric of panel.metrics){const link=text('a',metric);link.href='metrics?'+new URLSearchParams({metric:'inmemory_metrics.'+metric});links.append(link);}card.append(links);
    const host=text('div','');card.append(host);$('cards').append(card);
    const chart=createMetricChart(host,{legend:true,onPin:pause,onRangeChange:({from,to})=>{pause();fixed=[from,to];draw();}});
    return {panel,chart};
});
function draw(){
    const end=fixed?fixed[1]:series.length?Math.max(...series.map(s=>s.end)):Date.now(),begin=fixed?fixed[0]:end-Number($('period').value)*1000;
    for(const {panel,chart} of cards)chart.setData({series:series.filter(s=>panel.metrics.includes(s.queryId)),begin,end,title:panel.title,emptyText:'No retained data. Check that inmemory_metrics. is allowed in the registry.'});
}
async function refresh(){
    clearTimeout(timer);controller?.abort();controller=new AbortController();const current=++version;
    $('status').textContent='Loading metrics...';$('status').className='';
    try{
        const response=await client.queryMany(queries,{seconds:$('period').value,signal:controller.signal});if(current!==version)return;
        series=response.series;const {stats,config}=response.catalog;$('summary').replaceChildren();
        for(const value of ['Memory limit '+(config.memory_bytes/1048576).toFixed(2)+' MiB','Lines '+stats.lines+' / '+config.max_lines,'Append failures '+stats.append_failures_total])$('summary').append(text('span',value));
        draw();$('status').textContent='Updated '+new Date(response.catalog.timestamp_ms).toLocaleTimeString();
    }catch(error){if(error.name==='AbortError')return;$('status').className='imd-error';$('status').textContent=error.message+' · Showing last successful data (stale)';}
    finally{if(current===version&&$('live').checked&&!fixed)timer=setTimeout(refresh,2000);}
}
$('refresh').addEventListener('click',refresh);
$('now').addEventListener('click',()=>{fixed=null;refresh();});
$('period').addEventListener('change',()=>{fixed=null;refresh();});
$('live').addEventListener('change',()=>{clearTimeout(timer);if($('live').checked){fixed=null;refresh();}});
window.addEventListener('pagehide',event=>{++version;clearTimeout(timer);controller?.abort();if(!event.persisted)for(const {chart} of cards)chart.destroy();});
window.addEventListener('pageshow',event=>{if(event.persisted)refresh();});
refresh();
