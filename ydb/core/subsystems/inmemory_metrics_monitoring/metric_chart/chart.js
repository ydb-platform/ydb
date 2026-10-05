 const text=(tag,value,cls)=>{const n=document.createElement(tag);n.textContent=value;if(cls)n.className=cls;return n;};
 const fmt=v=>v===null||v===undefined?'\u2014':typeof v==='string'?v:Number(v).toLocaleString(undefined,{maximumSignificantDigits:3});
export const defaultChartSettings={type:'line',fill:false,format:'',height:360,unit:'number',precision:null,min:null,max:null};
export function formatSeriesName(series,format=''){
 if(!format)return series.display;
 const labels=new Map((series.labelValues||[]).map(label=>[label.name,label.value]));
 const fields={metric:series.metric,name:series.name,query:series.queryLabel,labels:series.labels};
 return format.replace(/\{([^{}]+)\}/g,(token,key)=>{
  const value=key.startsWith('label:')?labels.get(key.slice(6)):Object.hasOwn(fields,key)?fields[key]:labels.get(key);
  return value===undefined?token:String(value);
 });
}
export function formatMetricValue(value,{unit='number',precision=null}={}){
 if(value===null||value===undefined)return '\u2014';
 if(unit==='number'&&precision===null)return fmt(value);
 let number=Number(value),suffix='';
 if(!Number.isFinite(number))return String(value);
 if(unit==='bytes'){const units=['B','KiB','MiB','GiB','TiB','PiB'];let index=0;while(Math.abs(number)>=1024&&index<units.length-1){number/=1024;index++;}suffix=' '+units[index];}
 else suffix={percent:'%',seconds:' s',milliseconds:' ms',cores:' cores'}[unit]||'';
 return number.toLocaleString(undefined,precision===null?{maximumSignificantDigits:3}:{minimumFractionDigits:precision,maximumFractionDigits:precision})+suffix;
}
// Interpolate sampled lines and preserve discontinuities of on-change lines.
function sampleAt(series,time,before=false){
 const points=series.points;let a=0,b=points.length;
 while(a<b){const m=(a+b)>>1;if(points[m].time<time||(!before&&points[m].time===time))a=m+1;else b=m;}
 const previous=points[a-1],next=points[a];
 if(series.step)return previous&&(!series.closed||time<=points.at(-1).time)?previous.value:null;
 if(next?.time===time)return next.value;
 if(previous?.time===time)return previous.value;
 if(!previous||!next||previous.value===null||next.value===null)return null;
 return previous.value+(next.value-previous.value)*(time-previous.time)/(next.time-previous.time);
}
function stackBase(areas,index,time,value,before=false){
 let base=0;for(let i=0;i<index;i++){const lower=sampleAt(areas[i],time,before);if(lower!==null&&(lower<0)===(value<0))base+=lower;}return base;
}
// Yagr requires a shared timeline. Duplicate timestamps preserve both sides of
// discontinuities in stacked layers, including explicit null gaps.
export function prepareChartKitSeries(series,begin,end){
 const allTimes=[...new Set([begin,end,...series.flatMap(s=>s.points.filter(p=>p.time>=begin&&p.time<=end).map(p=>p.time))])].sort((a,b)=>a-b);
 // Bound aligned cells, not only source samples: unrelated timestamps would
 // otherwise multiply each retained history by every other history's length.
 const graphCount=series.reduce((count,s)=>count+(s.type==='area'?2*(Number(s.points.some(p=>p.value!==null&&p.value>=0))+Number(s.points.some(p=>p.value<0))):1),0);
 const limit=Math.max(2,Math.floor(1000000/(2*Math.max(1,graphCount))));
 const sampled=allTimes.length>limit;
 const times=sampled?Array.from({length:limit},(_,i)=>allTimes[Math.round(i*(allTimes.length-1)/(limit-1))]):allTimes;
 const align=s=>{const values=times.flatMap(time=>[sampleAt(s,time,true),sampleAt(s,time)]);
  if(sampled){let index=0;for(let i=1;i<times.length;i++){let gap=false;while(index<s.points.length&&s.points[index].time<times[i]){const p=s.points[index++];if(p.time>times[i-1]&&p.value===null)gap=true;}if(gap)values[i*2]=null;}}
  return values;
 };
 const timeline=times.flatMap(time=>[time,time]),graphs=[],pairs=[];
 const positive=new Float64Array(timeline.length),negativeBase=new Float64Array(timeline.length);
 for(const s of series){
  const common={id:s.key,name:s.display,color:s.color,spanGaps:false,interpolation:s.step?'left':'linear',showInLegend:false,showInTooltip:false};
  if(s.type==='area'){
   const samples=align(s).map((value,i)=>{const stack=value<0?negativeBase:positive,base=stack[i];if(value!==null)stack[i]+=value;return {value,base,top:value===null?null:stack[i]};});
   for(const negative of [false,true]){
    if(!samples.some(p=>p.value!==null&&(p.value<0)===negative))continue;
    const top=graphs.length,sign=p=>p.value!==null&&(p.value<0)===negative;
    graphs.push({...common,id:s.key+':'+negative+':top',color:'transparent',data:samples.map(p=>sign(p)?p.top:null)});
    graphs.push({...common,id:s.key+':'+negative+':base',color:'transparent',data:samples.map(p=>sign(p)?p.base:null)});
    pairs.push({top,base:top+1,negative,color:s.color});
   }
  }else{
   graphs.push({...common,color:s.fill&&/^#[0-9a-f]{6}$/i.test(s.color)?s.color+'2e':s.color,type:(!s.step&&s.points.filter(p=>p.value!==null).length===1?'dots':s.fill?'area':'line'),pointsSize:6,lineColor:s.color,lineWidth:s.width||2,width:s.width||2,data:align(s)});
  }
 }
 // Yagr reverses graph order before handing it to uPlot; bands use uPlot indices.
 const idx=i=>graphs.length-i;
 const bands=pairs.map(p=>({series:p.negative?[idx(p.base),idx(p.top)]:[idx(p.top),idx(p.base)],fill:p.color}));
 const extent=[0,0];let hasValues=false;for(const graph of graphs)for(const value of graph.data)if(value!==null){hasValues=true;extent[0]=Math.min(extent[0],value);extent[1]=Math.max(extent[1],value);}
 return {timeline,graphs,bands,extent,hasValues,sampled};
}
export function seriesStats(s,begin,end){
  const pts=s.points.filter(p=>p.time>=begin&&p.time<=end), valid=pts.filter(p=>p.value!==null);let avg=null;
  if(s.step){let sum=0,duration=0;for(let i=0;i<s.points.length;i++){const p=s.points[i],a=Math.max(begin,p.time),b=Math.min(end,i+1<s.points.length?s.points[i+1].time:end);if(p.value!==null&&b>a){sum+=p.value*(b-a);duration+=b-a;}}if(duration)avg=sum/duration;}
  else if(valid.length)avg=valid.reduce((a,p)=>a+p.value,0)/valid.length;
  const valueAtEnd=s.step?s.points.filter(p=>p.time<=end).at(-1):pts.at(-1);
  // Include the predecessor in step statistics only when it covers the selected interval.
  let exact=valid.map(p=>p.raw);if(s.step){const prev=s.points.filter(p=>p.time<begin).at(-1);if(prev&&prev.value!==null){exact.push(prev.raw);}}
  exact.sort((a,b)=>{if(/^-?\d+$/.test(a)&&/^-?\d+$/.test(b)){const x=BigInt(a),y=BigInt(b);return x<y?-1:x>y?1:0;}return Number(a)-Number(b);});
  return {last:valueAtEnd&&valueAtEnd.value!==null?valueAtEnd.raw:null,min:exact.length?exact[0]:null,max:exact.length?exact.at(-1):null,avg,count:pts.length};
 }
// The active chart owns the group's cursor and tooltip. Moving to another
// chart releases the previous pin instead of leaving two different timestamps.
export function createMetricChartCursorGroup(){
 const charts=new Set();
 return {
  add(chart){charts.add(chart);},
  remove(chart){charts.delete(chart);},
  update(source,time){for(const chart of charts)if(chart!==source)chart.setCursor(time,{replacePinned:true});},
 };
}
export function createMetricChart(chart,options={}){
 const hidden=new Set();
 if(!document.querySelector('link[data-metric-chartkit]')){const link=document.createElement('link');link.rel='stylesheet';link.href=new URL('./chartkit.css',import.meta.url).href;link.dataset.metricChartkit='';document.head.append(link);}
 let unmount=null,generation=0;
 let settings={...defaultChartSettings,...options.settings};
 const format=value=>formatMetricValue(value,settings);
 let data={series:[],begin:0,end:1,title:'Metrics'},destroyed=false,lastWidth=chart.clientWidth,tooltipLayer=null,cursorTime=null,drawCursor=()=>{},cursorPinned=false,releaseTooltip=()=>{};
 const publishCursor=time=>{options.cursorGroup?.update(api,time);options.onCursorChange?.(time);};
 chart.classList.add('ymc');
 chart.style.setProperty('--ymc-height',settings.height+'px');
 function render(){
  // Measure before clearing content: removing the chart can temporarily remove
  // the page scrollbar and inflate the measured width by its full thickness.
  const width=chart.clientWidth;lastWidth=width;
  const current=++generation;unmount?.();unmount=null;
  tooltipLayer?.remove();tooltipLayer=null;drawCursor=()=>{};cursorPinned=false;releaseTooltip=()=>{};
  chart.replaceChildren();
  const {begin,end,title,emptyText}=data;
  const active=data.series.filter(s=>!hidden.has(s.key)).map(s=>({...s,type:s.type||settings.type,fill:s.fill??settings.fill,display:formatSeriesName(s,s.format??settings.format)}));
  const areas=active.filter(s=>s.type==='area'),prepared=prepareChartKitSeries(active,begin,end);
  if(!prepared.hasValues){chart.append(text('div',data.series.length&&!active.length?'No visible series. Select rows in the legend.':emptyText||'No retained numeric samples in this interval','ymc-empty'));renderLegend();return;}
  const extent=prepared.extent;
  let lo=settings.min??extent[0],hi=settings.max??extent[1];if(lo>=hi){if(settings.max!==null&&settings.min===null)lo=hi-1;else hi=lo+1;}
  const axisSettings={...settings,precision:settings.precision===null?null:Math.min(3,settings.precision)};
  const labels=Array.from({length:5},(_,i)=>formatMetricValue(hi-(hi-lo)*i/4,axisSettings));
  const H=innerWidth<=650?Math.min(settings.height,260):settings.height;
  const axisWidth=Math.max(options.plotLeft||55,...labels.map(label=>label.length*7+14));
  const host=text('div','','ymc-canvas');host.style.height=H+'px';host.setAttribute('role','img');host.setAttribute('aria-label',title+' comparison chart');chart.append(host);
  import('./chartkit.js').then(({mountChartKit})=>{
   if(destroyed||current!==generation)return;
   unmount=mountChartKit(host,{
    data:{timeline:prepared.timeline,graphs:prepared.graphs},
    libraryConfig:{
     chart:{size:{height:H,padding:[20,20,0,0]},timeMultiplier:1,select:{zoom:false}},
     scales:{x:{range:()=>[begin,end]},y:{range:()=>[lo,hi]}},
     axes:{x:{font:'12px Arial',size:45,values:(_,ticks)=>ticks.map(t=>new Date(t).toLocaleTimeString())},y:{font:'12px Arial',size:axisWidth,values:(_,ticks)=>ticks.map(v=>formatMetricValue(v,axisSettings))}},
     bands:prepared.bands,legend:{show:false},tooltip:{show:false},markers:{show:false},cursor:{x:{visible:false},y:{visible:false},maxMarkers:0},
    },
   },widget=>{
    if(destroyed||current!==generation)return;
    attachInteraction(widget);
   },()=>{if(!destroyed&&current===generation)host.replaceChildren(text('div','Unable to render chart','ymc-empty'));});
  }).catch(()=>{if(!destroyed&&current===generation)host.replaceChildren(text('div','Unable to load chart engine','ymc-empty'));});
  if(prepared.sampled)chart.append(text('div','Plot sampled to '+prepared.timeline.length/2+' timestamps; tooltip and statistics use retained samples.','ymc-render-note'));
  renderLegend();
  function attachInteraction(widget){
  const plot=widget.uplot.over,u=widget.uplot;
  const x=t=>u.valToPos(t,'x'),y=v=>u.valToPos(v,'y');
  const cursor=text('div','','ymc-cursor'),selection=text('div','','ymc-selection');cursor.dataset.cursor='time';plot.append(selection,cursor);
  drawCursor=()=>{const visible=cursorTime!==null&&cursorTime>=begin&&cursorTime<=end;cursor.hidden=!visible;if(visible)cursor.style.left=x(cursorTime)+'px';};drawCursor();
  const tip=text('div','','ymc-tooltip');tip.hidden=true;tooltipLayer=text('div','','ymc ymc-tooltip-layer');tooltipLayer.append(tip);document.body.append(tooltipLayer);let start=null,pinned=false,tooltipSort='value',tooltipDirection=-1,tooltipData=null;
  const pos=e=>{
   const rect=plot.getBoundingClientRect(),px=(e.clientX-rect.left)*plot.clientWidth/rect.width,py=(e.clientY-rect.top)*plot.clientHeight/rect.height;
   const inside=px>=0&&px<=plot.clientWidth&&py>=0&&py<=plot.clientHeight;
   const clamped=Math.max(0,Math.min(plot.clientWidth,px));
   return {px:clamped,py,inside,time:u.posToVal(clamped,'x')};
  };
  function at(s,t){let a=0,b=s.points.length;while(a<b){const m=(a+b)>>1;if(s.points[m].time<=t)a=m+1;else b=m;}if(s.step)return a?s.points[a-1]:null;const p=s.points[a-1],q=s.points[a];return !p?q:!q?p:t-p.time<=q.time-t?p:q;}
  const setPinned=value=>{pinned=value;cursorPinned=value;tip.classList.toggle('ymc-tooltip-pinned',value);if(value)options.onPin?.();const hint=tip.querySelector('footer');if(hint)hint.textContent=value?'Pinned. Click the chart again to move the tooltip.':'Click the chart to pin the tooltip.';};
  releaseTooltip=()=>{setPinned(false);tip.hidden=true;};
  function renderTooltip(){
   const {time,rows,nearest}=tooltipData;tip.replaceChildren();const header=text('header',''),close=text('button','Close');close.type='button';close.setAttribute('aria-label','Close tooltip');close.addEventListener('click',()=>{setPinned(false);tip.hidden=true;cursorTime=null;drawCursor();publishCursor(null);});header.append(text('strong',new Date(time).toLocaleString()),close);tip.append(header);
   const table=document.createElement('table'),head=document.createElement('thead'),headRow=document.createElement('tr');for(const [key,title] of [['name','Series'],['value','Value']]){const cell=document.createElement('th'),button=text('button',title+(tooltipSort===key?(tooltipDirection<0?' \u2193':' \u2191'):''),'ymc-sort');button.type='button';button.addEventListener('click',()=>{tooltipDirection=tooltipSort===key?-tooltipDirection:key==='value'?-1:1;tooltipSort=key;renderTooltip();});cell.append(button);headRow.append(cell);}head.append(headRow);table.append(head);const body=document.createElement('tbody');
   const ordered=[...rows].sort((a,b)=>{if(tooltipSort==='name')return tooltipDirection*(a.series.display).localeCompare(b.series.display);const av=a.point&&a.point.value,bv=b.point&&b.point.value;return av===null?bv===null?0:1:bv===null?-1:tooltipDirection*(av-bv);});
   for(const row of ordered){const tr=document.createElement('tr');if(row.series.key===nearest)tr.className='ymc-tooltip-nearest';const label=text('td',row.series.display),dot=text('span','','ymc-dot');dot.style.background=row.series.color;label.prepend(dot);tr.append(label,text('td',row.point&&row.point.value!==null?format(row.point.raw):'\u2014'));body.append(tr);}table.append(body);
   const sumRows=rows.filter(row=>row.point&&row.point.value!==null),sum=sumRows.reduce((total,row)=>total+row.point.value,0),foot=document.createElement('tfoot'),sumRow=document.createElement('tr');sumRow.append(text('th','Sum'),text('th',sumRows.length&&Number.isFinite(sum)?format(sum):'\u2014'));foot.append(sumRow);table.append(foot);tip.append(table,text('footer',pinned?'Pinned. Click the chart again to move the tooltip.':'Click the chart to pin the tooltip.'));
  }
  const hover=e=>{if(pinned&&start===null)return;const p=pos(e);if(start===null&&!p.inside){tip.hidden=true;cursorTime=null;drawCursor();publishCursor(null);return;}cursorTime=p.time;drawCursor();publishCursor(p.time);if(start!==null){selection.style.left=Math.min(start.px,p.px)+'px';selection.style.width=Math.abs(start.px-p.px)+'px';return;}
   const rows=active.map(series=>({series,point:at(series,p.time)})),py=p.py;let nearest=null,distance=Infinity;for(const row of rows)if(row.point&&row.point.value!==null){let delta=Math.abs(y(row.point.value)-py);if(row.series.type==='area'){const value=sampleAt(row.series,p.time);if(value===null)continue;const base=stackBase(areas,areas.findIndex(s=>s.key===row.series.key),p.time,value),a=y(base),b=y(base+value);delta=Math.max(Math.min(a,b)-py,py-Math.max(a,b),0);}if(delta<distance){distance=delta;nearest=row.series.key;}}tooltipData={time:p.time,rows,nearest};renderTooltip();tip.hidden=false;
   const margin=8,gap=16,leftSpace=e.clientX-margin,rightSpace=innerWidth-e.clientX-margin;
   tip.style.width=Math.min(480,Math.max(120,Math.max(leftSpace,rightSpace)-gap))+'px';tip.style.maxHeight=Math.max(80,Math.min(430,innerHeight-2*margin))+'px';
   const width=tip.offsetWidth,height=tip.offsetHeight;
   const left=e.clientX+gap+width<=innerWidth-margin?e.clientX+gap:e.clientX-gap-width;
   tip.style.left=Math.max(margin,Math.min(left,innerWidth-width-margin))+'px';
   tip.style.top=Math.max(margin,Math.min(e.clientY+12,innerHeight-height-margin))+'px';};plot.addEventListener('pointermove',hover);
  plot.addEventListener('pointerleave',()=>{if(start===null&&!pinned){tip.hidden=true;cursorTime=null;drawCursor();publishCursor(null);}});
  plot.addEventListener('pointerdown',e=>{if(e.button!==0)return;const point=pos(e);if(!point.inside)return;start=point;plot.setPointerCapture(e.pointerId);if(!pinned)tip.hidden=true;});
  plot.addEventListener('pointerup',e=>{if(!start)return;const p=pos(e);selection.style.width='0';if(Math.abs(start.px-p.px)>8){const range={from:Math.min(start.time,p.time),to:Math.max(start.time,p.time)};start=null;options.onRangeChange?.(range);}else{start=null;const wasPinned=pinned;setPinned(false);hover(e);setPinned(p.inside&&!wasPinned);}});
  plot.addEventListener('pointercancel',()=>{start=null;cursorTime=null;publishCursor(null);render();});
  }
 }
 function renderLegend(){
  if(options.legend){
   const legend=text('div','','ymc-legend');
   for(const s of data.series){const button=text('button',formatSeriesName(s,s.format??settings.format));button.type='button';button.setAttribute('aria-pressed',String(!hidden.has(s.key)));const dot=text('span','','ymc-dot');dot.style.background=s.color;button.prepend(dot);button.addEventListener('click',()=>{hidden.has(s.key)?hidden.delete(s.key):hidden.add(s.key);render();});legend.append(button);}chart.append(legend);
  }
 }
 const observer=new ResizeObserver(()=>{const width=chart.clientWidth;if(!destroyed&&width!==lastWidth){lastWidth=width;render();}});observer.observe(chart);
 const api={
  setCursor(time,{replacePinned=false}={}){if(destroyed)return;if(replacePinned)releaseTooltip();else if(cursorPinned)return;cursorTime=Number.isFinite(time)?time:null;drawCursor();},
  setSettings(next){settings={...settings,...next};chart.style.setProperty('--ymc-height',settings.height+'px');render();},
  setData(next){if(destroyed)return;data=next;for(const key of hidden)if(!data.series.some(s=>s.key===key))hidden.delete(key);render();},
  destroy(){destroyed=true;++generation;unmount?.();unmount=null;options.cursorGroup?.remove(api);observer.disconnect();tooltipLayer?.remove();tooltipLayer=null;chart.replaceChildren();chart.classList.remove('ymc');},
 };
 options.cursorGroup?.add(api);return api;
}
