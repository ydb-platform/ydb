const palette=['#2678bc','#e77b28','#42a16c','#9062c4','#d45575','#399b9b','#a28735','#647aa6'];
const numeric=v=>v===null||v===undefined?null:Number.isFinite(Number(v))?Number(v):null;
const labelText=ls=>(ls||[]).map(l=>l.name+'='+l.value).join(', ');
const key=line=>line.name+'|'+JSON.stringify([...line.labels].sort((a,b)=>a.name.localeCompare(b.name)));
const color=(k,line)=>{const pool=(line.labels||[]).find(l=>l.name==='pool_id');if(pool&&/^\d+$/.test(pool.value))return palette[Number(pool.value)%palette.length];let h=0;for(const c of k)h=(h*31+c.charCodeAt(0))>>>0;return palette[h%palette.length];};
export function parseQuery(source) {
  if(source.length>2048)throw Error('Query is limited to 2048 characters.');
  let rest=source.trim();const metric=/^[A-Za-z_][A-Za-z0-9_.]*/.exec(rest);
  if(!metric||metric[0].length>256)throw Error('Expected a metric field name.');
  rest=rest.slice(metric[0].length).trim();const filters=[];
  if(!rest)return {metric:metric[0],filters};
  if(!rest.startsWith('{')||!rest.endsWith('}'))throw Error('Expected label filters inside { ... }.');
  rest=rest.slice(1,-1).trim();
  while(rest){
   const token=/^("(?:\\.|[^"\\])*"|[A-Za-z_][A-Za-z0-9_.-]*)\s*(!==|==|!=|=)\s*("(?:\\.|[^"\\])*")/.exec(rest);
   if(!token)throw Error('Expected label operator "value". Operators: ==, !==, =, !=.');
   let label,value;try{label=token[1][0]==='"'?JSON.parse(token[1]):token[1];value=JSON.parse(token[3]);}catch(e){throw Error('Invalid quoted string.');}
   if(!label||label.length>128||value.length>256)throw Error('Label names are limited to 128 characters, values to 256.');
   if(filters.length===12)throw Error('At most 12 label filters are allowed.');
   filters.push({label,op:token[2],value});rest=rest.slice(token[0].length).trim();
   if(rest){if(rest[0]!==',')throw Error('Expected a comma between filters.');rest=rest.slice(1).trim();if(!rest)throw Error('Expected a filter after the comma.');}
  }
  return {metric:metric[0],filters};
 }
export function formatQuery(query){return query.metric+'{'+query.filters.map(f=>JSON.stringify(f.label)+f.op+JSON.stringify(f.value)).join(', ')+'}';}
 function globMatch(value,pattern){
  return pattern.split('|').some(part=>{let i=0,j=0,star=-1,retry=0;
   while(i<value.length){if(j<part.length&&(part[j]==='?'||part[j]===value[i])){i++;j++;}else if(part[j]==='*'){star=j++;retry=i;}else if(star>=0){j=star+1;i=++retry;}else return false;}
   while(part[j]==='*')j++;return j===part.length;
  });
 }
export function queryMatches(labels,filters){
  const map=new Map(labels.map(l=>[l.name,l.value]));
  return filters.every(f=>{const exists=map.has(f.label),value=map.get(f.label),negative=f.op==='!='||f.op==='!==';
   const match=exists&&(f.op==='=='||f.op==='!=='?value===f.value:globMatch(value,f.value));return negative?!match:match;});
 }

export function createInMemoryMetricsClient({endpoint}){
 async function request({line,seconds=300,signal}={}){
  const url=new URL(endpoint,location.href);url.searchParams.set('format','json');url.searchParams.set('seconds',seconds);if(line)url.searchParams.set('line',line);else url.searchParams.delete('line');
  const response=await fetch(url,{signal,cache:'no-store',credentials:'same-origin'}),data=await response.json();if(!response.ok)throw Error(data.error||'HTTP '+response.status);return data;
 }
 async function queryMany(queries,{seconds=300,signal,catalog}={}){
  if(queries.length>8)throw Error('At most 8 queries are allowed.');
  catalog=catalog||await request({seconds,signal});const common=catalog.common_labels||[],result=[],details=new Map();let limited=false;
  for(const [queryIndex,query] of queries.entries()){
   const matching=catalog.lines.filter(l=>l.fields.some(f=>f.name===query.metric&&queryMatches([...common,...l.labels,...f.labels],query.filters)));if(matching.length>16)limited=true;
   for(const line of matching.slice(0,16)){
    if(result.length>=64){limited=true;break;}
    if(!details.has(line.id))details.set(line.id,await request({line:line.id,seconds,signal}));
    const detail=details.get(line.id),row=detail.lines[0],index=line.fields.findIndex(f=>f.name===query.metric),k=query.id+'|'+query.metric+'|'+key(line),labelValues=[...common,...line.labels,...line.fields[index].labels],labels=labelText(labelValues);
    result.push({key:k,queryId:query.id,color:queries.length===1?color(k,line):palette[result.length%palette.length],name:line.name,metric:query.metric,queryLabel:String.fromCharCode(65+queryIndex),labelValues,labels,display:String.fromCharCode(65+queryIndex)+' \u00b7 '+query.metric+(labels?' \u00b7 '+labels:''),step:line.frontend==='on_change',readable:line.readable,closed:line.closed,truncated:row&&row.truncated,end:detail.timestamp_ms,points:(row?row.points:[]).map(p=>({time:p.timestamp_ms,raw:p.values[index],value:numeric(p.values[index])}))});
   }
  }
  return {catalog,series:result,limited};
 }
 return {request,queryMany};
}
