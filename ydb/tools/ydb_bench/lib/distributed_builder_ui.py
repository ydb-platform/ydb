"""Distributed run editor. Placement stays in the detached template snapshot."""

CSS = """
.distributed-editor>.tabs{flex-wrap:wrap}
.distributed-editor .field{min-width:0}
.distributed-editor .field>select{width:100%;min-width:0;max-width:100%}
.distributed-editor .field>label:has([data-distributed-path]:not([type=checkbox])){display:flex;flex-direction:column;gap:.35rem;align-items:stretch}
.distributed-editor .actor-settings{display:flex;flex-wrap:wrap;align-items:end;gap:1rem;max-width:48rem}
.distributed-editor .actor-settings>.field{width:10rem}
.distributed-editor .actor-flags{display:flex;flex-wrap:wrap;gap:.6rem 1rem;padding-bottom:.5rem}
.distributed-editor .actor-flags label{display:flex;align-items:center;gap:.35rem}
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
function distributedSetLoadMode(raw,name,mode){
  raw.measurement??={};
  const clients=raw['cli-nodes'],client=clients[name];
  if(mode!=='fixed'&&Object.entries(clients).some(([n,c])=>n!==name&&c.load.search))
    throw Error('Only one CLI may own search. Set the current search CLI to fixed load first.');
  const parameter=client.load.parameter,allow=client.load['allow-errors']??false;
  client.load=mode==='fixed'?{parameter,values:[client.load.search?.start??1],'allow-errors':allow}:
    {parameter,'allow-errors':allow,search:{start:1,maximum:256,multiplier:2,'resolution-percent':2},
      objective:mode==='latency-slo'?{type:mode,percentile:'p99','max-ms':20,'max-errors':0,'min-achieved-rate-ratio':0.98}:
        {type:mode,'target-role':'dynamic','plateau-gain-percent':2,'plateau-points':2,'cpu-saturation-percent':95}};
  if(!Object.values(clients).some(c=>c.load.search))raw.measurement['verification-repetitions']=0;
}
function distributedTargetTenants(template){
  return template.tenants.filter(t=>template.nodes.some(n=>n.role==='dynamic'&&n.tenant===t.path)).map(t=>t.path);
}
function distributedDefault(template,tenant=distributedTargetTenants(template)[0]){
  if(!tenant)throw Error('The template needs a tenant with a dynamic node.');
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
  const host=editorHost,original=editor.yaml,request=++chooseDistributedProfile.version;
  const active=()=>request===chooseDistributedProfile.version&&host===editorHost&&original===editor.yaml&&location.hash.startsWith('#new');
  try{
    const records=await editorApi('/api/cluster-templates');
    if(!active())return;
    const record=records.find(r=>r.nodes.some(n=>n.role==='cli')&&distributedTargetTenants(r).length);
    if(!record)throw Error('Add a cluster template with a CLI node and a tenant with dynamic nodes.');
    const model=JSON.parse(JSON.stringify(editor.model));
    const item={benchmark:'distributed-ydb',name,key:'distributed-ydb/'+name,distributed_config:distributedDefault(record)};
    if(profile)model.profiles[model.profiles.findIndex(p=>p.key===profile.key)]=item;else model.profiles.push(item);
    const yaml=serializeConfig(model),validated=await editorApi('/api/editor-config',jsonOptions({yaml,perf:false}));
    if(!active())return;
    editor.model=validated;editor.yaml=yaml;editor.perf=false;editor.selected=item.key;saveDraft();renderNew('builder');
  }catch(error){if(active()){
    const benchmark=document.querySelector('#benchmark');if(benchmark&&profile)benchmark.value=profile.benchmark;
    document.querySelector('#editor-message').innerHTML=displayError(error);
  }}
}
chooseDistributedProfile.version=0;
function distributedReplaceTemplate(raw,template){
  const next=distributedDefault(template),previous=raw['cli-nodes'];
  if(!previous)throw Error('Convert the legacy profile before changing its template.');
  const removed=Object.keys(previous).filter(name=>!Object.hasOwn(next['cli-nodes'],name));
  const targets=distributedTargetTenants(template),retargeted=[];
  next.storage=JSON.parse(JSON.stringify(raw.storage||next.storage));
  next['reset-disks']=false;
  next.measurement=JSON.parse(JSON.stringify(raw.measurement||next.measurement));
  if(Object.hasOwn(raw,'timeout'))next.timeout=raw.timeout;
  for(const name of Object.keys(next.tenants))if(Object.hasOwn(raw.tenants||{},name))next.tenants[name]=JSON.parse(JSON.stringify(raw.tenants[name]));
  const allow=Object.values(previous)[0]?.load?.['allow-errors']??false;
  for(const [name,client] of Object.entries(next['cli-nodes'])){
    if(Object.hasOwn(previous,name)){
      next['cli-nodes'][name]=JSON.parse(JSON.stringify(previous[name]));
      if(!targets.includes(previous[name].tenant)){next['cli-nodes'][name].tenant=targets[0];retargeted.push(name)}
    }else client.load['allow-errors']=allow;
  }
  // New generators must not accidentally join a preserved dataset with different options.
  for(const [name,client] of Object.entries(next['cli-nodes']))if(!Object.hasOwn(previous,name)){
    const used=new Set(Object.entries(next['cli-nodes']).filter(([other,c])=>other!==name&&c.tenant===client.tenant).map(([,c])=>c.dataset));
    let suffix=1;while(used.has(client.dataset))client.dataset='new-kv-'+suffix++;
  }
  if(!Object.values(next['cli-nodes']).some(c=>c.load.search))next.measurement['verification-repetitions']=0;
  return {next,removed,retargeted};
}
function distributedProfileControls(profile){
  const template=profile.distributed_config['cluster-template'];
  const benchmarks=editor.model?.benchmarks||[{name:profile.benchmark}];
  return '<div class=form-grid><div class=field><label for=benchmark>Benchmark</label><select id=benchmark>'+
    benchmarks.map(b=>'<option value="'+esc(b.name)+'" '+(b.name===profile.benchmark?'selected':'')+'>'+esc(b.name)+'</option>').join('')+
    '</select></div><div class=field><label for=distributed-template>Cluster template</label><select id=distributed-template disabled>'+
    '<option value="">'+esc(template?.name||'Saved placement')+' · saved snapshot</option></select></div></div>';
}
async function bindDistributedTemplate(profile){
  const select=document.querySelector('#distributed-template'),host=editorHost,original=editor.yaml;
  const active=()=>select.isConnected&&host===editorHost&&original===editor.yaml&&location.hash.startsWith('#new');
  try{
    const records=await editorApi('/api/cluster-templates');if(!active())return;
    select.innerHTML+=records.map((r,index)=>'<option value="'+index+'">'+esc(r.name)+' · revision '+esc(r.revision)+'</option>').join('');
    select.disabled=!records.length||!profile.distributed_config['cli-nodes'];
    select.onchange=async()=>{
      if(!active()||select.value==='')return;
      try{
        const {next,removed,retargeted}=distributedReplaceTemplate(profile.distributed_config,records[Number(select.value)]);
        if((removed.length||retargeted.length)&&!confirm([
          removed.length?'Remove CLI settings: '+removed.join(', ')+'.':'',
          retargeted.length?'Reset target tenant for: '+retargeted.join(', ')+'.':''
        ].filter(Boolean).join(' ')+' Apply template?'))return;
        await commitDistributed(profile,next);
      }catch(error){if(active())document.querySelector('#editor-message').innerHTML=displayError(error)}
      finally{if(select.isConnected)select.value=''}
    };
  }catch(error){if(active())document.querySelector('#editor-message').innerHTML=displayError(error)}
}
function serializeDistributedYdb(lines,profile){
  function append(value,prefix){
    for(const [key,item] of Object.entries(value)){
      const name=prefix+(Array.isArray(value)?'-':JSON.stringify(key)+':');
      if(item&&typeof item==='object'&&Object.keys(item).length){lines.push(name);append(item,prefix+'  ')}
      else lines.push(name+' '+JSON.stringify(item));
    }
  }
  append(profile.distributed_config,'    ');
}
function distributedProfileEditor(profile){
  const raw=profile.distributed_config;
  const actions='';
  const controls=distributedProfileControls(profile);
  if(!raw?.['cli-nodes'])return controls+'<div class=notice>This profile uses the legacy single-CLI load controller. Its YAML is preserved.</div>'+
    '<button type=button id=distributed-convert>Convert to fixed-load Builder</button>'+actions;
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
    '<input data-distributed-path="'+esc(JSON.stringify(keys))+'" type="'+type+'" '+(type==='number'?'step=any ':'')+'value="'+esc(value??'')+'"></label></div>';
  const select=(label,keys,value,values)=>'<div class=field><label>'+esc(label)+
    '<select data-distributed-path="'+esc(JSON.stringify(keys))+'">'+
    values.map(v=>'<option '+(v===value?'selected':'')+'>'+esc(v)+'</option>').join('')+'</select></label></div>';
  if(view.tab==='Cluster'){
    content='<strong>'+esc(template.name)+'</strong>'+template.host_ids.map(host=>
      '<div class=distributed-host><strong>'+esc(hostRecord(host)?.name||host)+'</strong><div class=table-scroll><table>'+
      '<thead><tr><th>Node</th><th>Type</th><th>Tenant</th><th>DC / rack</th><th>Affinity</th><th>Binary</th></tr></thead><tbody>'+
      template.nodes.filter(n=>n.host_id===host).map(n=>{
        const affinity=n.affinity||{},location=n.location||{};
        const placement=affinity.kind==='manual'?'CPUs: '+(affinity.cpus||[]).join(', '):
          affinity.mode==='none'?'No pinning':(affinity.mode||'—')+(affinity.count?' · '+affinity.count+' CPUs':'');
        return '<tr><td>'+esc(n.name)+'</td><td>'+esc(n.role)+'</td><td>'+
          esc(n.role==='cli'?'—':n.role==='static'?'Shared infrastructure':n.tenant||'Unassigned')+'</td><td>'+
          esc(n.role==='cli'?'—':[location.data_center,location.rack].filter(Boolean).join(' / ')||'Unassigned')+
          '</td><td>'+esc(placement)+'</td><td>'+esc(n.binary||'—')+'</td></tr>';
      }).join('')+'</tbody></table></div></div>').join('');
  }else if(view.tab==='Storage'||view.tab==='Tenants'){
    content='<div class=distributed-summary>'+template.nodes.filter(n=>view.tab==='Storage'?n.role==='static':n.role==='dynamic'&&n.tenant===view.item)
      .map(n=>esc(n.name)).join(' · ')+'</div><div class=actor-settings>'+input('vCPU per node',[...path,'cpu-count'],object['cpu-count']??4,'number')+
      '<div class=actor-flags>'+['use-shared-threads','use-united-pool','use-ring-queue'].map(k=>'<label><input type=checkbox data-distributed-path="'+
        esc(JSON.stringify([...path,k]))+'" '+((object[k]??(k==='use-ring-queue'))?'checked':'')+'> '+esc(k)+'</label>').join('')+'</div></div>';
    if(view.tab==='Storage')content+='<p><label><input type=checkbox data-distributed-path="'+esc(JSON.stringify(['reset-disks']))+'" '+
      (raw['reset-disks']?'checked':'')+'> Reset existing disks before each cluster start</label></p>'+
      '<p class=muted>Destructive: clears YDB metadata on all configured persistent files and block devices. '+
      'Use dedicated benchmark disks only. Existing data is lost. New temporary files do not need this permission.</p>';
  }else if(view.tab==='Load generators'){
    const node=template.nodes.find(n=>n.name===view.item),peers=Object.entries(clients).filter(([name,c])=>c.tenant===object.tenant&&c.dataset===object.dataset);
    const definition=localYdbWorkloadDefinition(object.workload.type),load=object.load,mode=load.search?load.objective.type:'fixed';
    const searchOwner=Object.entries(clients).find(([name,c])=>name!==view.item&&c.load.search)?.[0];
    content='<div class=distributed-summary>'+esc(node?.name)+' · '+esc(hostRecord(node?.host_id)?.name||node?.host_id)+' · affinity from template</div><div class=form-grid>'+
      select('Target tenant',[...path,'tenant'],object.tenant,distributedTargetTenants(template))+input('Dataset',[...path,'dataset'],object.dataset)+'</div>'+
      '<div class=distributed-summary>'+esc(object.workload.type)+' · '+(peers.length>1?
        'Shared with '+peers.filter(([n])=>n!==view.item).map(([n])=>esc(n)).join(', '):'Independent dataset')+
      ' · initialize once</div><div class=form-grid>'+
      select('Workload',[...path,'workload','type'],object.workload.type,['kv','stock'])+
      select('Operation',[...path,'workload','operation'],object.workload.operation,definition.operations)+
      input('Client threads',[...path,'client','threads'],object.client?.threads??1,'number')+
      select('Load parameter',[...path,'load','parameter'],object.load.parameter,['threads','rate'])+
      select('Objective',[...path,'load-mode'],mode,searchOwner?['fixed']:['fixed','latency-slo','maximize-throughput'])+
      (mode==='fixed'?input('Fixed load',[...path,'load','values',0],load.values[0],'number'):
        [['start','Start',1],['maximum','Maximum',256],['multiplier','Growth multiplier',2],['resolution-percent','Resolution (%)',2]]
          .map(([k,label,d])=>input(label,[...path,'load','search',k],load.search[k]??d,'number')).join(''))+'</div>'+
      (searchOwner?'<div class=muted>Search is controlled by '+esc(searchOwner)+'.</div>':'')+
      (mode==='latency-slo'?'<h3>Latency SLO</h3><div class=form-grid>'+
        select('Percentile',[...path,'load','objective','percentile'],load.objective.percentile??'p99',Object.keys(definition.slo_metrics||{}))+
        [['max-ms','Maximum latency (ms)',20],['max-errors','Maximum errors',0],['min-achieved-rate-ratio','Minimum achieved rate ratio',0.98]]
          .map(([k,label,d])=>input(label,[...path,'load','objective',k],load.objective[k]??d,'number')).join('')+'</div>':'')+
      (mode==='maximize-throughput'?'<div class=form-grid>'+
        select('Target role',[...path,'load','objective','target-role'],load.objective['target-role']??'dynamic',['static','dynamic','total'])+
        [['plateau-gain-percent','Plateau gain (%)',2],['plateau-points','Plateau comparisons',2],['cpu-saturation-percent','CPU saturation (%)',95]]
          .map(([k,label,d])=>input(label,[...path,'load','objective',k],load.objective[k]??d,'number')).join('')+'</div>':'')+
      '<h3>Dataset options</h3><div class=muted>Workload type and options apply to '+peers.map(([n])=>esc(n)).join(', ')+'.</div>'+
      (object.workload.type==='stock'?'<div class=muted>Stock uses one shared dataset per tenant (fixed table names).</div>':'')+'<div class=form-grid>'+
      definition.options.map(o=>input(o.name,[...path,'workload','options',o.name],object.workload.options?.[o.name]??o.default,'number')).join('')+'</div>';
  }else{
    const owner=Object.entries(clients).find(([name,c])=>c.load.search)?.[0];
    content='<div class=distributed-summary>'+(owner?'Search: '+esc(owner):'Simultaneous fixed load')+' · '+Object.keys(clients).length+' CLI generators</div><div class=form-grid>'+
      [['warmup','Warm-up, s',2],['duration','Duration, s',10],['repetitions','Repetitions',1]].map(([k,label,d])=>input(label,['measurement',k],raw.measurement?.[k]??d,'number')).join('')+
      (owner?input('Verification repetitions',['measurement','verification-repetitions'],raw.measurement?.['verification-repetitions']??0,'number'):'')+
      select('Allow failed requests (all CLI)', ['cli-nodes',Object.keys(clients)[0],'load','allow-errors'],
        String(Object.values(clients)[0].load['allow-errors']??false),['false','true'])+'</div>'+
      (owner?'<p class=muted>Verification uses independent measurements at the selected load. Set repetitions to 0 to disable verification.</p>':'')+
      '<p class=muted>Allow failed requests applies to all CLI generators. Failed requests remain visible in results but do not limit load search.</p>';
  }
  return '<div class=distributed-editor>'+controls+'<div class=tabs>'+tabs.map(t=>
    '<button type=button class="'+(view.tab===t?'active':'')+'" data-distributed-tab="'+t+'" aria-pressed="'+(view.tab===t)+'">'+t+'</button>').join('')+
    '</div><div class="'+(items.length?'distributed-layout':'')+'">'+(items.length?'<div class=distributed-items>'+items.map(([key,label])=>
      '<button type=button data-distributed-item="'+esc(key)+'" aria-pressed="'+(view.item===key)+'">'+esc(label)+'</button>').join('')+
    '</div>':'')+'<section>'+content+'</section></div>'+actions+'</div>';
}
function bindDistributedEditor(profile){
  bindDistributedTemplate(profile);
  document.querySelector('#benchmark').onchange=event=>{
    const benchmark=editor.model.benchmarks.find(b=>b.name===event.target.value);
    if(benchmark.name===profile.benchmark)return;
    if(editor.model.profiles.some(p=>p!==profile&&p.benchmark===benchmark.name&&p.name===profile.name)){
      event.target.value=profile.benchmark;document.querySelector('#editor-message').innerHTML=displayError(Error('A profile with this benchmark and name already exists.'));return;
    }
    if(!confirm('Replace distributed settings with defaults for '+benchmark.name+'?')){event.target.value=profile.benchmark;return}
    const next={benchmark:benchmark.name,name:profile.name,key:benchmark.name+'/'+profile.name,
      parameters:Object.fromEntries(benchmark.parameters.map(p=>[p.name,p.default])),threads:[1],duration:3,repetitions:1,
      affinity:['none'],background_load:['none']};
    if(benchmark.profile_kind==='local-ydb'){
      next.local_ydb=defaultLocalYdb();next.threads=[64];next.duration=30;next.affinity=['roles'];
    }
    editor.model.profiles[editor.model.profiles.indexOf(profile)]=next;
    editor.selected=next.key;editor.yaml=serializeConfig(editor.model);saveDraft();renderNew();
  };
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
    const key=path.at(-1),value=input.type==='checkbox'?input.checked:
      input.type==='number'?Number(input.value):(key==='allow-errors'?input.value==='true':input.value);
    if(input.type==='number'&&(!input.value.trim()||!Number.isFinite(value)||value<0)){
      document.querySelector('#editor-message').innerHTML=displayError(Error('Enter a non-negative number.'));return}
    if(key==='load-mode'){
      try{distributedSetLoadMode(raw,path[1],value)}catch(error){document.querySelector('#editor-message').innerHTML=displayError(error);return}
      await commitDistributed(profile,raw);return;
    }
    target[key]=value;
    if(path[0]==='cli-nodes'&&path[2]==='workload'&&key==='type'){
      const selected=raw['cli-nodes'][path[1]],definition=localYdbWorkloadDefinition(value);
      for(const client of Object.values(raw['cli-nodes']))if(client.tenant===selected.tenant&&client.dataset===selected.dataset)
        client.workload={type:value,operation:definition.default_operation,options:Object.fromEntries(definition.options.map(o=>[o.name,o.default]))};
    }
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
