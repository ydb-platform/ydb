"""Distributed run editor. Placement stays in the detached template snapshot."""

CSS = """
.distributed-editor .distributed-layout{display:grid;grid-template-columns:13rem minmax(0,1fr);gap:1.5rem;margin-top:1rem}
.distributed-editor .distributed-items{display:flex;flex-direction:column;gap:.4rem;align-self:start}
.distributed-editor .distributed-items button{text-align:left;overflow-wrap:anywhere}
.distributed-editor .distributed-items button[aria-pressed=true]{background:var(--panel);border-color:var(--accent)}
.distributed-editor .distributed-items small{display:block;color:var(--muted)}
.distributed-editor .distributed-summary{padding:.7rem 0;color:var(--muted);overflow-wrap:anywhere}
.distributed-editor .distributed-host{border:1px solid var(--line);border-radius:6px;padding:.8rem;margin:.7rem 0;overflow-wrap:anywhere}
@media(max-width:650px){.distributed-editor .distributed-layout{grid-template-columns:1fr}.distributed-editor .distributed-items{flex-direction:row;flex-wrap:wrap}}
"""

JS = r"""
const distributedView=new Map();
const distributedHosts=new Map();
const hostRecord=id=>({name:distributedHosts.get(id)||id});
function distributedDefault(template,tenant){
  const clients={};
  for(const node of template.nodes.filter(n=>n.role==='cli')){
    clients[node.name]={tenant,dataset:'shared-kv',workload:{type:'kv',operation:'upsert',options:{'init-upserts':1000}},
      client:{threads:1},load:{parameter:'threads',values:[1], 'allow-errors':false}}
  }
  if(!Object.keys(clients).length)throw Error('The template needs a CLI node.');
  return {'cluster-template':JSON.parse(JSON.stringify(template)),storage:{'cpu-count':4},
    tenants:Object.fromEntries(template.tenants.map(t=>[t.path,{'cpu-count':4}])),
    'cli-nodes':clients,measurement:{warmup:2,duration:10,repetitions:1,'verification-repetitions':0}}
}
async function chooseDistributedProfile(profile,name){
  const host=editorHost,original=editor.yaml;
  try{
    const records=await editorApi('/api/cluster-templates');
    if(host!==editorHost||original!==editor.yaml||!location.hash.startsWith('#new'))return;
    if(!records.length)throw Error('Add a cluster template before creating a distributed run.');
    const dialog=document.createElement('dialog');
    dialog.innerHTML='<h2>Distributed run</h2><div class=form-grid>'+localSelect('distributed-template','Cluster template',records[0].id,
      records.map(r=>r.id))+'<div class=field><label for=distributed-tenant>Initial target tenant</label><select id=distributed-tenant></select></div></div>'+
      '<div class=toolbar><button type=button id=distributed-cancel>Cancel</button>'+
      '<button type=button class=primary id=distributed-use>Use template</button></div><div id=distributed-error role=alert></div>';
    // localSelect takes strings; explicit options retain stable template IDs.
    dialog.querySelector('#distributed-template').innerHTML=records.map(r=>'<option value="'+esc(r.id)+'">'+esc(r.name)+'</option>').join('');
    const refresh=()=>{const record=records.find(r=>r.id===dialog.querySelector('#distributed-template').value);
      dialog.querySelector('#distributed-tenant').innerHTML=record.tenants.filter(t=>record.nodes.some(n=>n.role==='dynamic'&&n.tenant===t.path))
        .map(t=>'<option>'+esc(t.path)+'</option>').join('')};
    refresh();dialog.querySelector('#distributed-template').onchange=refresh;
    const previous=document.activeElement;
    dialog.onclose=()=>{dialog.remove();previous?.focus();
      if(editor.yaml===original&&host===editorHost&&location.hash.startsWith('#new'))renderNew()};
    dialog.querySelector('#distributed-cancel').onclick=()=>dialog.close();
    dialog.querySelector('#distributed-use').onclick=async()=>{
      const button=dialog.querySelector('#distributed-use');button.disabled=true;
      try{
        const record=records.find(r=>r.id===dialog.querySelector('#distributed-template').value);
        const raw=distributedDefault(record,dialog.querySelector('#distributed-tenant').value);
        const model=JSON.parse(JSON.stringify(editor.model));
        const item=profile?model.profiles.find(p=>p.key===profile.key):{};
        if(!profile)model.profiles.push(item);
        Object.assign(item,{benchmark:'distributed-ydb',name,key:'distributed-ydb/'+name,distributed_config:raw});
        const yaml=serializeConfig(model),validated=await editorApi('/api/editor-config',jsonOptions({yaml,perf:false}));
        if(!dialog.isConnected||host!==editorHost||original!==editor.yaml)return;
        editor.model=validated;editor.yaml=yaml;editor.perf=false;editor.selected=item.key;saveDraft();dialog.close();renderNew('builder');
      }catch(error){dialog.querySelector('#distributed-error').innerHTML=displayError(error)}finally{button.disabled=false}
    };
    document.body.append(dialog);dialog.showModal();
  }catch(error){document.querySelector('#editor-message').innerHTML=displayError(error)}
}
function serializeDistributedYdb(lines,profile){
  function append(value,prefix){
    for(const [key,item] of Object.entries(value)){
      const name=prefix+JSON.stringify(key)+':';
      if(item&&typeof item==='object'&&!Array.isArray(item)&&Object.keys(item).length){lines.push(name);append(item,prefix+'  ')}
      else lines.push(name+' '+JSON.stringify(item));
    }
  }
  append(profile.distributed_config,'    ');
}
function distributedProfileEditor(profile){
  const raw=profile.distributed_config;
  if(!raw?.['cli-nodes'])return '<div class=notice>This profile uses the legacy single-CLI load controller. Its YAML is preserved.</div>'+
    '<button type=button id=distributed-convert>Convert to fixed-load Builder</button>';
  const view=distributedView.get(profile.key)||{tab:'Cluster',item:''};distributedView.set(profile.key,view);
  const template=raw['cluster-template'],clients=raw['cli-nodes'];
  const tabs=['Cluster','Storage','Tenants','Load generators','Run policy'];
  let content='',object,path,items=[];
  if(view.tab==='Storage'){object=raw.storage||{};path=['storage']}
  if(view.tab==='Tenants'){
    items=template.tenants.map(t=>[t.path,t.path]);
    if(!items.some(([key])=>key===view.item))view.item=items[0]?.[0];
    object=raw.tenants?.[view.item]||{};path=['tenants',view.item];
  }
  if(view.tab==='Load generators'){
    items=Object.entries(clients).map(([name,c])=>[name,name+' · '+c.workload.operation]);
    if(!items.some(([key])=>key===view.item))view.item=items[0]?.[0];
    object=clients[view.item];path=['cli-nodes',view.item];
  }
  const input=(label,keys,value,type='text')=>'<div class=field><label>'+esc(label)+
    '<input data-distributed-path="'+esc(JSON.stringify(keys))+'" type="'+type+'" value="'+esc(value??'')+'"></label></div>';
  const select=(label,keys,value,values)=>'<div class=field><label>'+esc(label)+
    '<select data-distributed-path="'+esc(JSON.stringify(keys))+'">'+
    values.map(v=>'<option '+(v===value?'selected':'')+'>'+esc(v)+'</option>').join('')+'</select></label></div>';
  if(view.tab==='Cluster'){
    content='<strong>'+esc(template.name)+'</strong>'+template.host_ids.map(host=>
      '<div class=distributed-host><strong>'+esc(hostRecord(host)?.name||host)+'</strong><div>'+
      template.nodes.filter(n=>n.host_id===host).map(n=>esc(n.name)+' · '+esc(n.role)).join('<br>')+'</div></div>').join('');
  }else if(view.tab==='Storage'||view.tab==='Tenants'){
    content='<div class=distributed-summary>'+template.nodes.filter(n=>view.tab==='Storage'?n.role==='static':n.role==='dynamic'&&n.tenant===view.item)
      .map(n=>esc(n.name)).join(' · ')+'</div><div class=form-grid>'+input('vCPU per node',[...path,'cpu-count'],object['cpu-count']??4,'number')+
      ['use-shared-threads','use-united-pool','use-ring-queue'].map(k=>select(k,[...path,k],String(object[k]??(k==='use-ring-queue')),['false','true'])).join('')+'</div>';
  }else if(view.tab==='Load generators'){
    const node=template.nodes.find(n=>n.name===view.item),peers=Object.entries(clients).filter(([name,c])=>c.tenant===object.tenant&&c.dataset===object.dataset);
    content='<div class=distributed-summary>'+esc(node?.name)+' · '+esc(hostRecord(node?.host_id)?.name||node?.host_id)+' · affinity from template</div><div class=form-grid>'+
      select('Target tenant',[...path,'tenant'],object.tenant,template.tenants.map(t=>t.path))+input('Dataset',[...path,'dataset'],object.dataset)+'</div>'+
      '<div class=distributed-summary>KV · '+(peers.length>1?
        'Shared with '+peers.filter(([n])=>n!==view.item).map(([n])=>esc(n)).join(', '):'Independent dataset')+
      ' · initialize once</div><div class=form-grid>'+
      select('Operation',[...path,'workload','operation'],object.workload.operation,['upsert','select','read-rows','mixed'])+
      input('Client threads',[...path,'client','threads'],object.client?.threads??1,'number')+
      select('Load parameter',[...path,'load','parameter'],object.load.parameter,['threads','rate'])+
      input('Fixed load',[...path,'load','values',0],object.load.values[0],'number')+'</div>'+
      '<h3>Dataset options</h3><div class=muted>Changes apply to '+peers.map(([n])=>esc(n)).join(', ')+'.</div><div class=form-grid>'+
      localYdbWorkloadDefinition('kv').options.map(o=>input(o.name,[...path,'workload','options',o.name],object.workload.options?.[o.name]??o.default,'number')).join('')+'</div>';
  }else{
    content='<div class=distributed-summary>Simultaneous fixed load · '+Object.keys(clients).length+' CLI generators</div><div class=form-grid>'+
      [['warmup','Warm-up, s',2],['duration','Duration, s',10],['repetitions','Repetitions',1]].map(([k,label,d])=>input(label,['measurement',k],raw.measurement?.[k]??d,'number')).join('')+
      select('Allow failed requests (all CLI)', ['cli-nodes',Object.keys(clients)[0],'load','allow-errors'],
        String(Object.values(clients)[0].load['allow-errors']??false),['false','true'])+'</div>';
  }
  return '<div class=distributed-editor><div class=view-tabs>'+tabs.map(t=>
    '<button type=button data-distributed-tab="'+t+'" aria-pressed="'+(view.tab===t)+'">'+t+'</button>').join('')+
    '</div><div class="'+(items.length?'distributed-layout':'')+'">'+(items.length?'<div class=distributed-items>'+items.map(([key,label])=>
      '<button type=button data-distributed-item="'+esc(key)+'" aria-pressed="'+(view.item===key)+'">'+esc(label)+'</button>').join('')+
    '</div>':'')+'<section>'+content+'</section></div></div>';
}
function bindDistributedEditor(profile){
  const convert=document.querySelector('#distributed-convert');
  if(convert){convert.onclick=async()=>{
    if(!confirm('Replace the legacy workload/search settings with fixed-load defaults? The placement snapshot is retained.'))return;
    const raw=profile.distributed_config;const next=distributedDefault(raw['cluster-template'],raw.tenant);
    await commitDistributed(profile,next);
  };return}
  const view=distributedView.get(profile.key);
  document.querySelectorAll('[data-distributed-tab]').forEach(b=>b.onclick=()=>{view.tab=b.dataset.distributedTab;view.item='';renderNew()});
  document.querySelectorAll('[data-distributed-item]').forEach(b=>b.onclick=()=>{view.item=b.dataset.distributedItem;renderNew()});
  document.querySelectorAll('[data-distributed-path]').forEach(input=>input.onchange=async()=>{
    const raw=JSON.parse(JSON.stringify(profile.distributed_config)),path=JSON.parse(input.dataset.distributedPath);
    let target=raw;for(const key of path.slice(0,-1))target=target[key]??=(typeof key==='number'?[]:{});
    const key=path.at(-1),value=input.type==='number'?Number(input.value):(['use-shared-threads','use-united-pool','use-ring-queue','allow-errors'].includes(key)?input.value==='true':input.value);
    if(input.type==='number'&&(!input.value.trim()||!Number.isSafeInteger(value)||value<0)){
      document.querySelector('#editor-message').innerHTML=displayError(Error('Enter a non-negative integer.'));return}
    target[key]=value;
    if(path[0]==='cli-nodes'&&path[2]==='load'&&key==='allow-errors'){
      for(const client of Object.values(raw['cli-nodes']))client.load['allow-errors']=value;
    }
    if(path[0]==='cli-nodes'&&path[2]==='workload'&&path[3]==='options'){
      const selected=raw['cli-nodes'][path[1]];
      for(const client of Object.values(raw['cli-nodes']))if(client.tenant===selected.tenant&&client.dataset===selected.dataset){
        client.workload.options={...selected.workload.options};
      }
    }
    await commitDistributed(profile,raw);
  });
}
async function commitDistributed(profile,raw){
  const host=editorHost,previous=editor.yaml,model=JSON.parse(JSON.stringify(editor.model));
  const controls=[...document.querySelectorAll('.distributed-editor input,.distributed-editor select,.distributed-editor button')];
  controls.forEach(input=>input.disabled=true);
  model.profiles.find(p=>p.key===profile.key).distributed_config=raw;
  const yaml=serializeConfig(model);
  try{const validated=await editorApi('/api/editor-config',jsonOptions({yaml,perf:editor.perf}));
    if(host!==editorHost||previous!==editor.yaml||!location.hash.startsWith('#new'))return;
    editor.model=validated;editor.yaml=yaml;saveDraft();renderNew();
  }catch(error){if(host===editorHost&&previous===editor.yaml&&location.hash.startsWith('#new'))document.querySelector('#editor-message').innerHTML=displayError(error)}
  finally{controls.forEach(input=>input.disabled=false)}
}
"""
