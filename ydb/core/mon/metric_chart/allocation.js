const colors=['#2678bc','#d45575','#ed7926','#3a9c70','#8e61c9','#399b9b','#b69532'];
const element=(tag,value='')=>{const node=document.createElement(tag);node.textContent=value;return node;};
// Values and capacity use the same unit. No registry or metrics dependency.
export function createAllocationBar(host,{onSelect,maxSegments=24,unit='chunks'}={}){
 let destroyed=false;
 host.classList.add('ymc-allocation');
 return {
  setData({segments=[],capacity=0,free=0}){
   if(destroyed)return;
   host.replaceChildren();
   const valid=value=>Number.isFinite(value)&&value>=0?value:0;
   const sorted=segments.map(s=>({...s,value:valid(s.value)})).filter(s=>s.value>0).sort((a,b)=>b.value-a.value||String(a.key).localeCompare(String(b.key)));
   const allocated=sorted.reduce((sum,s)=>sum+s.value,0),available=valid(free),total=Math.max(valid(capacity),allocated+available),limit=Math.max(1,Math.min(64,Math.floor(maxSegments)||24));
   const shown=sorted.slice(0,limit);
   const omitted=sorted.slice(limit);
   if(omitted.length)shown.push({key:'others',label:'Other lines ('+omitted.length+')',value:omitted.reduce((sum,s)=>sum+s.value,0),color:'#8995a5'});
   const remainder=total-allocated-available;
   if(remainder>0)shown.push({key:'unattributed',label:'Other allocated / not captured',value:remainder,color:'#b8a58d'});
   if(available>0)shown.push({key:'free',label:'Free',value:available,color:'#e4e8ee'});
   const number=value=>value.toLocaleString(),summary=element('div',number(allocated)+' '+unit+' attributed to '+sorted.length+' lines / '+number(total)+' total');summary.className='ymc-allocation-summary';host.append(summary);
   if(!total){host.append(element('p','No allocation data'));return;}
   const bar=element('div');bar.className='ymc-allocation-bar';bar.setAttribute('role','img');bar.setAttribute('aria-label','Allocation: '+shown.map(s=>s.label+' '+number(s.value)+' '+unit).join('; '));
   const legend=element('div');legend.className='ymc-allocation-legend';
   for(const s of shown){
    const color=s.color||colors[Array.from(String(s.key)).reduce((n,c)=>(n*31+c.charCodeAt(0))>>>0,0)%colors.length],description=s.label+': '+number(s.value)+' '+unit+' ('+(100*s.value/total).toFixed(1)+'%)';
    const part=element('span');part.style.width=(100*s.value/total)+'%';part.style.background=color;part.title=description;bar.append(part);
    const item=element(onSelect&&s.selectable!==false&&!['others','unattributed','free'].includes(s.key)?'button':'span',s.label+' '+number(s.value));item.title=description;
    const dot=element('span');dot.className='ymc-dot';dot.style.background=color;item.prepend(dot);
    if(item.tagName==='BUTTON'){item.type='button';item.addEventListener('click',()=>onSelect(s));}
    legend.append(item);
   }
   host.append(bar,legend);
  },
  destroy(){destroyed=true;host.replaceChildren();host.classList.remove('ymc-allocation');},
 };
}
