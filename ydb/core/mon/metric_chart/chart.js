const ns='http://www.w3.org/2000/svg';
let nextChartId=0;
 const text=(tag,value,cls)=>{const n=document.createElement(tag);n.textContent=value;if(cls)n.className=cls;return n;};
 const node=(tag,attrs,value)=>{const n=document.createElementNS(ns,tag);for(const [k,v] of Object.entries(attrs))n.setAttribute(k,v);if(value!==undefined)n.textContent=value;return n;};
 const fmt=v=>v===null||v===undefined?'\u2014':typeof v==='string'?v:Number(v).toLocaleString(undefined,{maximumSignificantDigits:6});
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
export function createMetricChart(chart,options={}){
 const clipId='ymc-clip-'+(++nextChartId),hidden=new Set();
 let data={series:[],begin:0,end:1,title:'Metrics'},destroyed=false,lastWidth=chart.clientWidth,tooltipLayer=null;
 chart.classList.add('ymc');
 function render(){
  tooltipLayer?.remove();tooltipLayer=null;
  chart.replaceChildren();
  const {begin,end,title,emptyText}=data;
  const active=data.series.filter(s=>!hidden.has(s.key));
  const values=active.flatMap(s=>{
   const values=s.points.filter(p=>p.value!==null&&p.time>=begin&&p.time<=end).map(p=>p.value);
   if(s.step){const predecessor=s.points.filter(p=>p.time<begin).at(-1);if(predecessor&&predecessor.value!==null&&(!s.closed||s.points.at(-1).time>begin))values.push(predecessor.value);}
   return values;
  });
  if(!values.length){chart.append(text('div',data.series.length&&!active.length?'No visible series. Select rows in the legend.':emptyText||'No retained numeric samples in this interval','ymc-empty'));renderLegend();return;}
  let lo=Math.min(0,...values),hi=Math.max(0,...values);if(lo===hi)hi=lo+1;
  const W=Math.max(320,chart.clientWidth-24),L=75,R=W-20,T=20,B=290,x=t=>L+(t-begin)*(R-L)/(end-begin),y=v=>B-(v-lo)*(B-T)/(hi-lo);
  const svg=node('svg',{viewBox:'0 0 '+W+' 340',preserveAspectRatio:'none',role:'img','aria-label':title+' comparison chart'});
  const defs=node('defs',{}),clip=node('clipPath',{id:clipId});clip.append(node('rect',{x:L,y:T,width:R-L,height:B-T}));defs.append(clip);svg.append(defs);
  for(let i=0;i<=4;i++){const py=T+(B-T)*i/4;svg.append(node('line',{x1:L,x2:R,y1:py,y2:py,stroke:'#e8ebef'}),node('text',{x:L-10,y:py+4,'text-anchor':'end'},fmt(hi-(hi-lo)*i/4)));}
  const ticks=W<600?2:4;for(let i=0;i<=ticks;i++){const px=L+(R-L)*i/ticks;svg.append(node('text',{x:px,y:B+30,'text-anchor':i===0?'start':i===ticks?'end':'middle'},new Date(begin+(end-begin)*i/ticks).toLocaleTimeString()));}
  const graph=node('g',{'clip-path':'url(#'+clipId+')'});
  for(const s of active){let path='',continuous=false;for(const p of s.points){if(p.value===null){continuous=false;continue;}path+=continuous?(s.step?' H '+x(p.time)+' V '+y(p.value):' L '+x(p.time)+' '+y(p.value)):' M '+x(p.time)+' '+y(p.value);continuous=true;}if(s.step&&continuous)path+=' H '+x(Math.min(end,s.closed?s.points.at(-1).time:end));graph.append(node('path',{d:path,fill:'none',stroke:s.color,'stroke-width':2,'data-series':s.key}));for(const p of s.points){if(p.value!==null&&p.time>=begin&&p.time<=end&&s.points.length===1)graph.append(node('circle',{cx:x(p.time),cy:y(p.value),r:3,fill:s.color}));}}svg.append(graph);
  const cursor=node('line',{x1:L,x2:L,y1:T,y2:B,stroke:'#8b97a7','stroke-dasharray':'4 3',visibility:'hidden'}),selection=node('rect',{x:L,y:T,width:0,height:B-T,fill:'#2678bc',opacity:.12});svg.append(selection,cursor);
  const tip=text('div','','ymc-tooltip');tip.hidden=true;chart.append(svg);tooltipLayer=text('div','','ymc ymc-tooltip-layer');tooltipLayer.append(tip);document.body.append(tooltipLayer);let start=null,pinned=false,tooltipSort='value',tooltipDirection=-1,tooltipData=null;
  const pos=e=>{const r=svg.getBoundingClientRect(),px=Math.max(L,Math.min(R,(e.clientX-r.left)*W/r.width));return {px,time:begin+(px-L)*(end-begin)/(R-L)};};
  function at(s,t){let a=0,b=s.points.length;while(a<b){const m=(a+b)>>1;if(s.points[m].time<=t)a=m+1;else b=m;}if(s.step)return a?s.points[a-1]:null;const p=s.points[a-1],q=s.points[a];return !p?q:!q?p:t-p.time<=q.time-t?p:q;}
  const setPinned=value=>{pinned=value;tip.classList.toggle('ymc-tooltip-pinned',value);if(value)options.onPin?.();const hint=tip.querySelector('footer');if(hint)hint.textContent=value?'Pinned. Click the chart again to move the tooltip.':'Click the chart to pin the tooltip.';};
  function renderTooltip(){
   const {time,rows,nearest}=tooltipData;tip.replaceChildren();const header=text('header',''),close=text('button','Close');close.type='button';close.setAttribute('aria-label','Close tooltip');close.addEventListener('click',()=>{setPinned(false);tip.hidden=true;cursor.setAttribute('visibility','hidden');});header.append(text('strong',new Date(time).toLocaleString()),close);tip.append(header);
   const table=document.createElement('table'),head=document.createElement('thead'),headRow=document.createElement('tr');for(const [key,title] of [['name','Series'],['value','Value']]){const cell=document.createElement('th'),button=text('button',title+(tooltipSort===key?(tooltipDirection<0?' \u2193':' \u2191'):''),'ymc-sort');button.type='button';button.addEventListener('click',()=>{tooltipDirection=tooltipSort===key?-tooltipDirection:key==='value'?-1:1;tooltipSort=key;renderTooltip();});cell.append(button);headRow.append(cell);}head.append(headRow);table.append(head);const body=document.createElement('tbody');
   const ordered=[...rows].sort((a,b)=>{if(tooltipSort==='name')return tooltipDirection*(a.series.display).localeCompare(b.series.display);const av=a.point&&a.point.value,bv=b.point&&b.point.value;return av===null?bv===null?0:1:bv===null?-1:tooltipDirection*(av-bv);});
   for(const row of ordered){const tr=document.createElement('tr');if(row.series.key===nearest)tr.className='ymc-tooltip-nearest';const label=text('td',row.series.display),dot=text('span','','ymc-dot');dot.style.background=row.series.color;label.prepend(dot);tr.append(label,text('td',row.point&&row.point.value!==null?fmt(row.point.raw):'\u2014'));body.append(tr);}table.append(body);
   const sumRows=rows.filter(row=>row.point&&row.point.value!==null),sum=sumRows.reduce((total,row)=>total+row.point.value,0),foot=document.createElement('tfoot'),sumRow=document.createElement('tr');sumRow.append(text('th','Sum'),text('th',sumRows.length&&Number.isFinite(sum)?fmt(sum):'\u2014'));foot.append(sumRow);table.append(foot);tip.append(table,text('footer',pinned?'Pinned. Click the chart again to move the tooltip.':'Click the chart to pin the tooltip.'));
  }
  const hover=e=>{if(pinned&&start===null)return;const p=pos(e);cursor.setAttribute('x1',p.px);cursor.setAttribute('x2',p.px);cursor.setAttribute('visibility','visible');if(start!==null){selection.setAttribute('x',Math.min(start.px,p.px));selection.setAttribute('width',Math.abs(start.px-p.px));return;}
   const rows=active.map(series=>({series,point:at(series,p.time)})),rect=svg.getBoundingClientRect(),py=(e.clientY-rect.top)*340/rect.height;let nearest=null,distance=Infinity;for(const row of rows)if(row.point&&row.point.value!==null){const delta=Math.abs(y(row.point.value)-py);if(delta<distance){distance=delta;nearest=row.series.key;}}tooltipData={time:p.time,rows,nearest};renderTooltip();tip.hidden=false;
   const margin=8,gap=16,leftSpace=e.clientX-margin,rightSpace=innerWidth-e.clientX-margin;
   tip.style.width=Math.min(480,Math.max(120,Math.max(leftSpace,rightSpace)-gap))+'px';tip.style.maxHeight=Math.max(80,Math.min(430,innerHeight-2*margin))+'px';
   const width=tip.offsetWidth,height=tip.offsetHeight;
   const left=e.clientX+gap+width<=innerWidth-margin?e.clientX+gap:e.clientX-gap-width;
   tip.style.left=Math.max(margin,Math.min(left,innerWidth-width-margin))+'px';
   tip.style.top=Math.max(margin,Math.min(e.clientY+12,innerHeight-height-margin))+'px';};svg.addEventListener('pointermove',hover);
  svg.addEventListener('pointerleave',()=>{if(start===null&&!pinned){tip.hidden=true;cursor.setAttribute('visibility','hidden');}});
  svg.addEventListener('pointerdown',e=>{if(e.button!==0)return;start=pos(e);svg.setPointerCapture(e.pointerId);if(!pinned)tip.hidden=true;});
  svg.addEventListener('pointerup',e=>{if(!start)return;const p=pos(e);if(Math.abs(start.px-p.px)>8){const range={from:Math.min(start.time,p.time),to:Math.max(start.time,p.time)};start=null;options.onRangeChange?.(range);}else{start=null;const wasPinned=pinned;setPinned(false);hover(e);setPinned(!wasPinned);}});
  svg.addEventListener('pointercancel',()=>{start=null;render();});
  renderLegend();
 }
 function renderLegend(){
  if(options.legend){
   const legend=text('div','','ymc-legend');
   for(const s of data.series){const button=text('button',s.display);button.type='button';button.setAttribute('aria-pressed',String(!hidden.has(s.key)));const dot=text('span','','ymc-dot');dot.style.background=s.color;button.prepend(dot);button.addEventListener('click',()=>{hidden.has(s.key)?hidden.delete(s.key):hidden.add(s.key);render();});legend.append(button);}chart.append(legend);
  }
 }
 const observer=new ResizeObserver(()=>{const width=chart.clientWidth;if(!destroyed&&width!==lastWidth){lastWidth=width;render();}});observer.observe(chart);
 return {
  setData(next){if(destroyed)return;data=next;for(const key of hidden)if(!data.series.some(s=>s.key===key))hidden.delete(key);render();},
  destroy(){destroyed=true;observer.disconnect();tooltipLayer?.remove();tooltipLayer=null;chart.replaceChildren();chart.classList.remove('ymc');},
 };
}
