const colors=['#2678bc','#d45575','#ed7926','#3a9c70','#8e61c9','#399b9b','#b69532'];
export const allocationColor=key=>colors[Array.from(String(key)).reduce((n,c)=>(n*31+c.charCodeAt(0))>>>0,0)%colors.length];
const element=(tag,value='')=>{const node=document.createElement(tag);node.textContent=value;return node;};
// Values and capacity use the same unit. No registry or metrics dependency.
export function createAllocationBar(host,{onSelect,maxSegments=24,unit='chunks',legend=true,onPin}={}){
 let destroyed=false,layer=null,highlight=()=>{},colorFor=allocationColor;
 const clear=()=>{layer?.remove();layer=null;highlight=()=>{};};
 host.classList.add('ymc-allocation');
 return {
  setData({segments=[],capacity=0,free=0}){
   if(destroyed)return;
   clear();host.replaceChildren();
   const valid=value=>Number.isFinite(value)&&value>=0?value:0;
   const sorted=segments.map(s=>({...s,value:valid(s.value)})).filter(s=>s.value>0).sort((a,b)=>b.value-a.value||String(a.key).localeCompare(String(b.key)));
   const allocated=sorted.reduce((sum,s)=>sum+s.value,0),available=valid(free),total=Math.max(valid(capacity),allocated+available),limit=Math.max(1,Math.min(64,Math.floor(maxSegments)||24));
   const shown=sorted.slice(0,limit);
   const omitted=sorted.slice(limit),omittedKeys=new Set(omitted.map(s=>s.key));
   if(omitted.length)shown.push({key:'others',label:'Other lines ('+omitted.length+')',value:omitted.reduce((sum,s)=>sum+s.value,0),color:'#8995a5'});
   const remainder=total-allocated-available;
   if(remainder>0)shown.push({key:'unattributed',label:'Other allocated / not captured',value:remainder,color:'#b8a58d'});
   if(available>0)shown.push({key:'free',label:'Free',value:available,color:'#e4e8ee'});
   const number=value=>value.toLocaleString(),summary=element('div',number(allocated)+' '+unit+' attributed to '+sorted.length+' lines / '+number(total)+' total');summary.className='ymc-allocation-summary';host.append(summary);
   if(!total){host.append(element('p','No allocation data'));return;}
   const bar=element('div');bar.className='ymc-allocation-bar';bar.setAttribute('role','img');bar.setAttribute('aria-label','Allocation: '+shown.map(s=>s.label+' '+number(s.value)+' '+unit).join('; '));
   const keys=new Map(),tip=element('div'),tipLayer=element('div');tip.hidden=true;tip.className='ymc-tooltip';tipLayer.className='ymc ymc-tooltip-layer';tipLayer.append(tip);document.body.append(tipLayer);layer=tipLayer;let pinned=false;
   const legendNode=element('div');legendNode.className='ymc-allocation-legend';
   const show=(s,event)=>{
    if(pinned)return;
    tip.classList.remove('ymc-tooltip-pinned');tip.replaceChildren();const header=element('header'),close=element('button','Close');close.type='button';close.setAttribute('aria-label','Close allocation tooltip');close.addEventListener('click',()=>{pinned=false;tip.classList.remove('ymc-tooltip-pinned');tip.hidden=true;});header.append(element('strong','Chunk allocation'),close);tip.append(header);
    const table=element('table'),head=element('thead'),heading=element('tr');heading.append(element('th','Storage line'),element('th',unit),element('th','Share'));head.append(heading);table.append(head);
    const rows=shown.slice(0,15);if(!rows.includes(s)){if(rows.length===15)rows.pop();rows.unshift(s);}const body=element('tbody');
    for(const owner of rows){const row=element('tr');if(owner.key===s.key)row.className='ymc-tooltip-nearest';const name=element('td',owner.label),dot=element('span');dot.className='ymc-dot';dot.style.background=owner.color||allocationColor(owner.key);name.prepend(dot);row.append(name,element('td',number(owner.value)),element('td',(100*owner.value/total).toPrecision(3)+'%'));body.append(row);}table.append(body);
    const foot=element('tfoot'),sum=element('tr');sum.append(element('th','Total'),element('th',number(total)),element('th','100%'));foot.append(sum);table.append(foot);tip.append(table,element('footer',(shown.length>rows.length?'Other segments: '+(shown.length-rows.length)+'. ':'')+'Click to pin the tooltip.'));tip.hidden=false;tip.style.maxHeight=Math.max(0,Math.min(430,innerHeight-24))+'px';
    const margin=12,width=tip.offsetWidth,height=tip.offsetHeight;let left=event.clientX+16;if(left+width>innerWidth-margin)left=event.clientX-width-16;tip.style.left=Math.max(margin,Math.min(left,innerWidth-width-margin))+'px';tip.style.top=Math.max(margin,Math.min(event.clientY+12,innerHeight-height-margin))+'px';
   };
   for(const s of shown){
    const color=s.color||allocationColor(s.key),description=s.label+': '+number(s.value)+' '+unit+' ('+(100*s.value/total).toFixed(1)+'%)';
    const part=element('span');part.style.width=(100*s.value/total)+'%';part.style.background=color;part.setAttribute('aria-label',description);bar.append(part);keys.set(s.key,part);part.addEventListener('pointermove',event=>show(s,event));part.addEventListener('pointerleave',()=>{if(!pinned)tip.hidden=true;});part.addEventListener('click',event=>{if(pinned){pinned=false;show(s,event);}else{show(s,event);pinned=true;onPin?.();tip.classList.add('ymc-tooltip-pinned');tip.querySelector('footer').textContent='Pinned. Close to resume hover.';}});
    if(legend){const item=element(onSelect&&s.selectable!==false&&!['others','unattributed','free'].includes(s.key)?'button':'span',s.label+' '+number(s.value));item.title=description;
    const dot=element('span');dot.className='ymc-dot';dot.style.background=color;item.prepend(dot);
    if(item.tagName==='BUTTON'){item.type='button';item.addEventListener('click',()=>onSelect(s));}
    legendNode.append(item);}
   }
   const shownColors=new Map(shown.map(s=>[s.key,s.color||allocationColor(s.key)]));
   colorFor=key=>omittedKeys.has(key)?'#8995a5':shownColors.get(key)||allocationColor(key);
   highlight=key=>{const target=keys.get(key)|| (omittedKeys.has(key)?keys.get('others'):null);for(const part of keys.values())part.style.opacity=target&&part!==target?'0.3':'1';};
   host.append(bar);if(legend)host.append(legendNode);
  },
  highlight(key){if(!destroyed)highlight(key);},
  getColor(key){return colorFor(key);},
  destroy(){destroyed=true;clear();host.replaceChildren();host.classList.remove('ymc-allocation');},
 };
}
