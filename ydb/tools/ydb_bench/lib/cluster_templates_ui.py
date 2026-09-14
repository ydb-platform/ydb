"""Placement-template editor assets for the existing benchmark web application."""

CSS = r"""
.ct-hosts{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(420px,100%),1fr));gap:24px;align-items:start}
.ct-host{padding:0;background:transparent;border:0;min-width:0}
.ct-hosts>.ct-host{border:1px solid var(--line);border-radius:6px;padding:12px}
.ct-host>.ct-host{margin-top:12px}
.ct-host>.runs-toolbar,.ct-host>.ct-zone-header{border-bottom:1px solid var(--line);padding-bottom:8px;margin:0 0 10px}
.ct-host>.ct-host>.ct-zone-header{border:0;color:var(--muted);padding:0;margin:0 0 5px}
.ct-zone-header{display:flex;flex-wrap:wrap;align-items:center;gap:.6rem}
.ct-zone-header>strong{min-width:0;overflow-wrap:anywhere}
.ct-remove{color:#b42332;border:0;background:transparent;font-size:22px;min-width:32px;min-height:32px;padding:0}
.ct-edit{display:inline-flex;align-items:center;justify-content:center;border:0;background:transparent;min-width:32px;min-height:32px;padding:0;color:var(--muted)}
.ct-edit:hover{color:var(--accent)}
.ct-node-row{position:relative;margin:7px 0}
.ct-node-row>.ct-remove{position:absolute;top:4px;right:5px}
.ct-node{display:block;width:100%;text-align:left;margin:0;padding:10px 12px;overflow-wrap:anywhere}
.ct-node-title{display:block;font-weight:500;padding-right:30px}.ct-node-title em{font-style:normal;color:var(--muted);margin-left:10px}
.ct-node span{display:block;color:var(--muted);margin-top:4px}
.ct-node[aria-pressed=true]{border-color:var(--accent);box-shadow:inset 3px 0 var(--accent)}
.ct-node[draggable=true]{cursor:grab}.ct-host.ct-drop{outline:2px solid var(--accent);background:var(--panel)}
.ct-empty{color:var(--muted);padding:14px 0}.ct-placement-tabs{display:flex;gap:.35rem;margin:16px 0;border-bottom:1px solid var(--line)}
.ct-placement-tabs button{padding:.6rem .8rem;border:1px solid transparent;border-radius:6px 6px 0 0;background:transparent;color:var(--muted);margin-bottom:-1px}
.ct-placement-tabs button[aria-pressed=true]{background:#fff;color:var(--text);font-weight:650;border-color:var(--line);border-bottom-color:#fff}
.ct-rack{margin-top:12px;padding:10px;border:1px solid var(--line);border-radius:4px;min-height:70px}
.ct-dialog{width:min(440px,calc(100vw - 32px));border:1px solid var(--line);border-radius:6px;padding:20px}
.ct-dialog::backdrop{background:#0004}
.ct-create-actions{justify-content:flex-end}
.ct-editor{border-top:1px solid var(--line);margin-top:20px;padding-top:16px}
.ct-fields{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(190px,100%),1fr));gap:12px}
.ct-fields label{display:grid;gap:5px;min-width:0}.ct-fields input,.ct-fields select{width:100%;min-width:0}
.ct-flags{display:flex;gap:16px;flex-wrap:wrap;margin-top:12px}
.ct-pop{position:fixed;inset:auto;margin:0;padding:16px;width:min(680px,calc(100vw - 24px));max-height:80vh;
overflow:auto;border:1px solid var(--line);border-radius:6px;background:#fff;color:var(--text);box-shadow:0 6px 24px #0003;z-index:200}
.ct-pop::backdrop{background:transparent}.ct-pop-tabs{display:flex;gap:6px;border-bottom:1px solid var(--line);margin-bottom:12px}
.ct-pop-tabs button{border:0;border-radius:0;border-bottom:2px solid transparent}
.ct-pop-tabs button[aria-pressed=true]{color:var(--accent);border-bottom-color:var(--accent)}
.ct-numas{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(240px,100%),1fr));gap:16px;margin-top:12px}
.ct-chip{border-top:1px solid var(--line);padding-top:7px;margin-top:7px}
.ct-group-title{display:flex;align-items:center;justify-content:space-between;gap:6px;margin-bottom:7px}
.ct-cores{display:grid;grid-template-columns:repeat(auto-fit,minmax(42px,1fr));gap:5px}
.ct-core{display:grid;gap:3px;padding:3px;background:var(--panel);border-radius:4px;align-content:start}
.ct-cpu{padding:5px 1px;min-width:0;font-variant-numeric:tabular-nums}
.ct-cpu[aria-pressed=true]{background:var(--accent);color:#fff}
.ct-cpu.ct-overlap{border-bottom:3px solid var(--warn)}
.ct-cpu.ct-other{position:relative;background:#e2e5eb;color:var(--text);opacity:1;padding-bottom:12px}
.ct-cpu.ct-chiplet::after,.ct-cpu.ct-numa::after{content:'';position:absolute;left:20%;right:20%;bottom:4px;height:2px;background:var(--muted)}
.ct-cpu.ct-numa::after{box-shadow:0 -4px 0 var(--muted)}
.ct-scope-bar{height:3px;background:var(--accent);margin:8px 0}
.ct-scope-bar.ct-double{box-shadow:0 -5px 0 var(--accent);margin-top:12px}
.ct-pop-footer{display:flex;gap:10px;justify-content:flex-end;margin-top:14px}
.ct-mask{overflow-wrap:anywhere;margin-top:10px}.ct-small-action{padding:2px 6px}
@media(pointer:coarse){.ct-cpu{min-height:44px}.ct-remove,.ct-edit{min-width:44px;min-height:44px}.ct-node-title{padding-right:42px}}
"""

JS = r"""
function ctNormalizeAffinity(a){
  const result={...a};
  if(result.kind==='strategy'&&result.scope==='numa')result.mode='pack-numa';
  delete result.scope;return result;
}
function ctPlacementScope(a){
  if(a.kind!=='strategy')return 'exclusive';
  const mode=ctNormalizeAffinity(a).mode;
  return mode==='pack-numa'?'numa':mode.endsWith('-chiplet')?'chiplet':'exclusive';
}
async function ctResolvePlacements(nodes,load){
  const result=new Map();
  await Promise.all([...new Set(nodes.map(n=>n.host_id))].map(async host=>{
    const local=nodes.filter(n=>n.host_id===host),used=new Set(local.flatMap(n=>n.affinity.kind==='manual'?n.affinity.cpus:[]));
    let blocked='';
    const shared=n=>ctPlacementScope(n.affinity)!=='exclusive';
    for(const node of [...local.filter(n=>!shared(n)),...local.filter(shared).sort((a,b)=>(ctPlacementScope(a.affinity)==='numa')-(ctPlacementScope(b.affinity)==='numa'))]){
      const a={...ctNormalizeAffinity(node.affinity),scope:ctPlacementScope(node.affinity)};
      if(a.kind==='manual'){result.set(node,{supported:true,cpus:[...a.cpus]});continue}
      if(a.mode==='none'){result.set(node,{supported:true,cpus:[]});continue}
      if(blocked){result.set(node,{supported:false,reason:'Placement blocked by '+blocked});continue}
      try{
        if(shared(node)){
          const response=await load(host),t=response.topology;
          if(!t?.allowed_cpus?.length)throw Error('Host topology unavailable');
          const allowed=new Set(t.allowed_cpus),groups=(a.scope==='numa'?t.numa_nodes:t.chiplets)||[];
          const units=groups.map((g,i)=>({cpus:g.cpus.filter(c=>allowed.has(c)),numa:g.numa_node??g.id??i}));
          if(!Number.isInteger(a.count)||a.count<1)throw Error('Reserve must be a positive integer');
          const reserve=[],cpus=new Set();
          const occupied=u=>u.cpus.filter(c=>used.has(c)).length;
          units.sort((x,y)=>a.mode.startsWith('spread-numa')?occupied(x)-occupied(y)||x.numa-y.numa:
            x.numa-y.numa||occupied(x)-occupied(y));
          for(const unit of units){
            const free=unit.cpus.filter(c=>!used.has(c)&&!reserve.includes(c));
            if(!free.length)continue;
            // Keep SMT siblings adjacent when choosing accounting slots, without narrowing affinity.
            const ordered=[...new Set((t.physical_cores||[]).flatMap(core=>core.filter(c=>free.includes(c))).concat(free))];
            reserve.push(...ordered.slice(0,a.count-reserve.length));unit.cpus.forEach(c=>cpus.add(c));
            if(reserve.length===a.count)break;
          }
          if(reserve.length!==a.count)throw Error('Not enough free CPU capacity for shared '+a.scope+' affinity');
          result.set(node,{supported:true,cpus:[...cpus].sort((a,b)=>a-b),reserved_cpus:reserve,scope:a.scope});
          reserve.forEach(c=>used.add(c));continue;
        }
        const response=await load(host,a,[...used]),p=response.placement;
        if(!p?.supported)throw Error(p?.reason||'Placement preview unavailable');
        if(used.size&&!Array.isArray(p.excluded_cpus))throw Error('Update this host to support joint placement');
        if(!Array.isArray(p.cpus)||!p.cpus.length||p.cpus.some(c=>used.has(c)))throw Error('Host returned an overlapping placement');
        result.set(node,p);p.cpus.forEach(c=>used.add(c));
      }catch(e){result.set(node,{supported:false,reason:e.message||String(e)});blocked=node.name}
    }
  }));
  return result;
}
function ctMoveNode(record,index,kind,target,resetManual=false){
  const n=record.nodes[index];if(!n)throw Error('Node no longer exists');
  if(n.role==='cli'&&kind!=='physical')throw Error('CLI load generators only have physical placement');
  if(kind==='physical'){
    if(!record.host_ids.includes(target))throw Error('Select a template host');
    if(n.host_id===target)return;
    if(n.affinity.kind==='manual'&&!resetManual)throw Error('Clear the manual CPU mask before moving to another host');
    if(n.affinity.kind==='manual')n.affinity={kind:'strategy',mode:'none',count:n.vcpu||8};
    n.host_id=target;
  }else if(kind==='logical'){
    const [dc,rack]=target;
    if((dc&&!record.data_centers.some(d=>d.name===dc&&(!rack||d.racks.includes(rack))))||(!dc&&rack))throw Error('Unknown rack');
    if(dc&&!rack)throw Error('Select a rack');
    n.location={data_center:dc,rack,body:rack?n.name:''};
  }else if(kind==='tenants'){
    if(n.role==='static')throw Error('Static nodes are shared cluster infrastructure');
    if(target&&!record.tenants.some(t=>t.path===target))throw Error('Unknown tenant');
    n.tenant=target;
  }else throw Error('Unknown placement view');
}
function ctDefaultNode(host,role,index){
  return {name:role+'-'+index,role,host_id:host,binary:'bundled',
    sector_map:{count:1,size_gib:64},affinity:{kind:'strategy',mode:'none',count:8}};
}
function ctNormalizeNodePlacement(n){
  n.location??={data_center:'',rack:'',body:''};n.tenant??='';
  if(n.role==='cli')n.location={data_center:'',rack:'',body:''};
  if(n.role!=='dynamic')n.tenant='';
  if(n.role!=='cli')n.location.body=n.location.rack?n.name:'';
}
function ctRenameNode(record,node,value){
  const name=value.trim();
  if(!name||name.length>80)throw Error('Node name must contain 1 to 80 characters');
  if(record.nodes.some(other=>other!==node&&other.name.trim()===name))throw Error('Node names must be unique');
  node.name=name;ctNormalizeNodePlacement(node);
}
function ctRemoveLocation(record,dcName,rack){
  const dc=record.data_centers.find(d=>d.name===dcName);
  if(!dc||rack!==undefined&&!dc.racks.includes(rack))throw Error('Location no longer exists');
  record.nodes.forEach(n=>{
    if(n.location?.data_center===dcName&&(rack===undefined||n.location.rack===rack))n.location={data_center:'',rack:'',body:''};
  });
  if(rack===undefined)record.data_centers=record.data_centers.filter(d=>d!==dc);
  else dc.racks=dc.racks.filter(r=>r!==rack);
}
function ctRemoveTenant(record,path){
  if(!record.tenants.some(t=>t.path===path))throw Error('Tenant no longer exists');
  record.nodes.forEach(n=>{if(n.tenant===path)n.tenant=''});
  record.tenants=record.tenants.filter(t=>t.path!==path);
}
function ctRemoveHost(record,host,target){
  if(!record.host_ids.includes(host))throw Error('Host no longer exists');
  const nodes=record.nodes.filter(n=>n.host_id===host);
  if(nodes.length&&(!record.host_ids.includes(target)||target===host))throw Error('Select another template host for these nodes');
  nodes.forEach(n=>ctMoveNode(record,record.nodes.indexOf(n),'physical',target,true));
  record.host_ids=record.host_ids.filter(h=>h!==host);
}
function ctAffinityLabel(value){const a=ctNormalizeAffinity(value);return a.kind==='manual'?'CPU '+cpuRanges(a.cpus):
  a.mode==='none'?'No pinning':a.mode;}
function ctNextRack(dc){let i=1;while(dc.racks.includes(dc.name+'-R'+i))i++;return dc.name+'-R'+i;}
function ctShortHost(name,names){
  const short=name.split('.')[0];
  return names.filter(other=>other.split('.')[0]===short).length>1?name:short;
}
function ctRemoveButton(attributes,label){return '<button class=ct-remove '+attributes+' title="'+esc(label)+'" aria-label="'+esc(label)+'">×</button>';}
function ctNodeInfo(node,view,hostName){
  const details=[];
  if(view!=='physical')details.push('Host: '+hostName(node.host_id));
  if(node.role!=='cli'){
    if(view!=='logical')details.push([node.location?.data_center,node.location?.rack].filter(Boolean).join(' / ')||'No logical location');
    if(view!=='tenants')details.push(node.role==='static'?'Shared infrastructure':'Tenant: '+(node.tenant||'Unassigned'));
  }
  return details.join(' · ');
}
async function renderClusterTemplates(id){
  clearRefresh();const generation=(renderClusterTemplates.version||0)+1,current=location.hash;
  renderClusterTemplates.version=generation;
  const active=()=>generation===renderClusterTemplates.version&&location.hash===current;
  try{
    const [records,directory]=await Promise.all([api('/api/cluster-templates'),api('/api/hosts')]);
    if(!active())return;
    const hosts=[directory.local,...directory.hosts],hostName=id=>hosts.find(h=>h.id===id)?.name||'Unavailable host';
    const shortHost=id=>ctShortHost(hostName(id),hosts.map(h=>h.name));
    if(!id){
      app.innerHTML=shell('cluster-templates','<div class=runs-toolbar><span class=muted>Placement templates · this server</span>'+
        '<a class="button primary" href="#cluster-templates/new">New template</a></div>'+
        (records.length?'<div class=table-scroll><table><thead><tr><th>Template</th><th>Hosts</th><th>Nodes</th><th>Updated</th></tr></thead><tbody>'+
        records.map(r=>'<tr><td><a href="#cluster-templates/'+enc(r.id)+'">'+esc(r.name)+'</a></td><td>'+
          [...new Set(r.nodes.map(n=>hostName(n.host_id)))].map(esc).join(', ')+'</td><td>'+r.nodes.length+
          '</td><td>'+humanTime(r.updated_at)+'</td></tr>').join('')+'</tbody></table></div>':'<p>No saved templates.</p>'));
      return;
    }
    let record=id==='new'?{name:'New cluster',nodes:[ctDefaultNode(directory.local.id,'static',1),
      ctDefaultNode(directory.local.id,'dynamic',1),ctDefaultNode(directory.local.id,'cli',1)]}:
      JSON.parse(JSON.stringify(records.find(r=>r.id===id)||null));
    if(!record)throw Error('Template no longer exists');
    record.nodes.forEach(n=>n.affinity=ctNormalizeAffinity(n.affinity));
    record.host_ids??=[...new Set(record.nodes.map(n=>n.host_id))];record.data_centers??=[];record.tenants??=[];
    record.nodes.forEach(ctNormalizeNodePlacement);
    record.nodes.forEach(n=>{
      const dc=record.data_centers.find(dc=>dc.name===n.location.data_center);
      if(dc&&!n.location.rack){if(!dc.racks.length)dc.racks.push(ctNextRack(dc));n.location.rack=dc.racks[0];ctNormalizeNodePlacement(n)}
    });
    let selected=0,saving=false,view='physical',dragged=null;
    let previewVersion=0;
    const topology=async(host,affinity,excluded=[])=>{
      const query=affinity?.kind==='strategy'?'?mode='+enc(affinity.mode)+'&cpus='+enc(affinity.count)+'&exclude='+enc(excluded.join(',')):'';
      return api('/api/hosts/'+enc(host)+'/api/system-topology'+query);
    };
    async function refreshCards(){
      const version=++previewVersion,nodes=record.nodes.map(n=>({...n,affinity:JSON.parse(JSON.stringify(n.affinity))}));
      const plans=await ctResolvePlacements(nodes,topology);
      if(!active()||version!==previewVersion)return;
      nodes.forEach((node,i)=>{
        const p=plans.get(node),target=app.querySelector('[data-ct-plan="'+i+'"]');
        if(target){target.textContent=p.supported?(node.affinity.mode==='none'?'':
          'Affinity: '+cpuRanges(p.cpus)):p.reason;
          target.className=p.supported?'':'error'}
      });
    }
    function dialog(title,fields,submit){
      const pop=document.createElement('dialog');pop.className='ct-dialog';pop.setAttribute('aria-label',title);
      pop.innerHTML='<form><h3>'+esc(title)+'</h3><div class=ct-fields>'+fields+'</div><p class=error role=alert></p>'+
        '<div class=ct-pop-footer><button type=button>Cancel</button><button class=primary type=submit>'+esc(title)+'</button></div></form>';
      app.appendChild(pop);pop.onclose=()=>pop.remove();pop.querySelector('[type=button]').onclick=()=>pop.close();
      pop.querySelector('form').onsubmit=e=>{e.preventDefault();try{submit(new FormData(e.currentTarget));pop.close();draw()}
        catch(error){pop.querySelector('[role=alert]').textContent=error.message}};pop.showModal();
    }
    function move(index,kind,target){
      const n=record.nodes[index];
      if(kind==='physical'&&n.host_id!==target&&n.affinity.kind==='manual'){
        draw();
        dialog('Move node','<p>Move '+esc(n.name)+' to '+esc(hostName(target))+' and clear its manual CPU mask? '+
          'Select affinity again on the destination host.</p>',()=>ctMoveNode(record,index,kind,target,true));return;
      }
      try{ctMoveNode(record,index,kind,target);draw()}catch(e){app.querySelector('#ct-error').innerHTML=displayError(e)}
    }
    function draw(){
      if(!active())return;
      if(view!=='physical'&&record.nodes[selected]?.role==='cli')selected=record.nodes.findIndex(n=>n.role!=='cli');
      if(view==='physical'&&selected<0&&record.nodes.length)selected=0;
      const n=record.nodes[selected];
      const targets=[];
      const card=(node,i)=>'<div class=ct-node-row><button class=ct-node draggable=true data-ct-node="'+i+'" aria-pressed="'+(selected===i)+'">'+
        '<strong class=ct-node-title>'+esc(node.name)+'<em>'+esc(node.role)+'</em></strong>'+
        '<span title="'+esc(hostName(node.host_id))+'">'+esc(ctNodeInfo(node,view,shortHost))+'</span>'+
        (view==='physical'?'<span>'+esc(ctAffinityLabel(node.affinity))+'</span><span data-ct-plan="'+i+'">Calculating placement…</span>':'')+
        '</button>'+ctRemoveButton('data-ct-remove-node="'+i+'"','Remove node '+node.name)+'</div>';
      const cards=filter=>record.nodes.map((node,i)=>(view==='physical'||node.role!=='cli')&&filter(node)?card(node,i):'').join('')||'<p class=ct-empty>No nodes</p>';
      const zone=(title,target,filter,actions='',description='',fullTitle='')=>{
        const index=targets.push(target)-1;
        const content=cards(filter);
        return '<section class=ct-host data-ct-drop="'+index+'"><div class=ct-zone-header><strong title="'+esc(fullTitle||title)+'">'+esc(title)+'</strong>'+
          (actions?'<div class=runs-actions>'+actions+'</div>':'')+'</div>'+
          (description?'<p class=muted>'+esc(description)+'</p>':'')+content+'</section>';
      };
      let layout='';
      if(view==='physical')layout=record.host_ids.map((host,i)=>zone(shortHost(host),host,node=>node.host_id===host,
        ctRemoveButton('data-ct-delete-host="'+i+'"','Remove host '+hostName(host)),'',hostName(host))).join('');
      if(view==='logical'){
        layout=record.data_centers.map((dc,i)=>'<section class=ct-host><div class=runs-toolbar><strong>'+esc(dc.name)+
          '</strong><div class=runs-actions><button data-ct-rack="'+i+'">Add rack</button>'+
          ctRemoveButton('data-ct-delete-dc="'+i+'"','Remove DC '+dc.name)+'</div></div>'+dc.racks.map((rack,j)=>
            zone(rack,[dc.name,rack],node=>node.location?.data_center===dc.name&&node.location?.rack===rack,
              ctRemoveButton('data-ct-delete-rack="'+i+':'+j+'"','Remove rack '+rack))).join('')+'</section>').join('')+
          (record.nodes.some(n=>n.role!=='cli'&&!n.location?.data_center)?zone('Unassigned',['',''],node=>!node.location?.data_center):'');
      }
      if(view==='tenants')layout=record.tenants.map((tenant,i)=>zone(tenant.path,tenant.path,node=>node.role!=='static'&&node.tenant===tenant.path,
        '<button class=ct-edit data-ct-tenant="'+i+'" title="Edit tenant" aria-label="Edit tenant '+esc(tenant.path)+'">'+
        '<svg width=16 height=16 viewBox="0 0 24 24" fill=none stroke=currentColor stroke-width=1.8 aria-hidden=true focusable=false>'+
        '<path d="M15 5l4 4M4 20l4-1L20 7a2.8 2.8 0 0 0-4-4L4 15z"/></svg></button>'+
        ctRemoveButton('data-ct-delete-tenant="'+i+'"','Remove tenant '+tenant.path),
        tenant.storage_kind.toUpperCase()+' · '+tenant.storage_groups+' storage groups')).join('')+
        (record.nodes.some(n=>n.role==='dynamic'&&!n.tenant)?zone('Unassigned','',node=>node.role==='dynamic'&&!node.tenant):'')+
        '<section class=ct-host><strong>Shared cluster infrastructure</strong>'+cards(node=>node.role==='static')+'</section>';
      app.innerHTML=shell('cluster-templates',
        '<div class=runs-toolbar><a href="#cluster-templates">Cluster templates</a><div class=runs-actions>'+
        '<button id=ct-copy>Copy template</button>'+(record.id?'<button id=ct-delete>Delete template</button>':'')+
        '<button id=ct-save class=primary>Save template</button></div></div><div id=ct-error></div>'+
        '<div class=ct-fields><label>Template name<input id=ct-name maxlength=200 value="'+esc(record.name)+'"></label></div>'+
        '<p class=muted>Placement only · does not start a cluster</p><div class="profile-tabs ct-placement-tabs">'+
        ['physical','logical','tenants'].map(v=>'<button data-ct-view="'+v+'" class="'+(view===v?'active':'')+'" aria-pressed="'+(view===v)+'">'+
          v[0].toUpperCase()+v.slice(1)+'</button>').join('')+'</div><div class="runs-toolbar ct-create-actions">'+
        (view==='physical'?'<button id=ct-add '+(!record.host_ids.length?'disabled':'')+'>Add node</button>':'')+'<button id=ct-group>'+
        (view==='physical'?'Add host':view==='logical'?'Add DC':'Add tenant')+'</button></div><div class=ct-hosts>'+layout+'</div>'+
        (n?'<section class=ct-editor><div class=runs-toolbar><strong>'+esc(n.name)+'</strong><div class=runs-actions>'+
          (view==='physical'?'<button id=ct-duplicate>Duplicate node</button>':'')+
          ctRemoveButton('id=ct-remove','Remove node '+n.name)+'</div></div><div class=ct-fields>'+
          '<label>Name<input data-ct-field=name maxlength=80 value="'+esc(n.name)+'"></label><label>Type<select data-ct-field=role>'+
          ['static','dynamic','cli'].map(v=>'<option '+(n.role===v?'selected':'')+'>'+v+'</option>').join('')+'</select></label>'+
          '<label>Host<select data-ct-field=host_id>'+(!hosts.some(h=>h.id===n.host_id)?'<option value="'+esc(n.host_id)+'">Unavailable host</option>':'')+
          hosts.filter(h=>record.host_ids.includes(h.id)).map(h=>'<option value="'+esc(h.id)+'" '+(n.host_id===h.id?'selected':'')+'>'+esc(h.name)+'</option>').join('')+'</select></label>'+
          (n.role==='cli'?'':'<label>DC<select data-ct-location=data_center><option value="">Unassigned</option>'+record.data_centers.map(dc=>
            '<option '+(n.location?.data_center===dc.name?'selected':'')+'>'+esc(dc.name)+'</option>').join('')+'</select></label>'+
          '<label>Rack<select data-ct-location=rack '+(!n.location?.data_center?'disabled':'')+'>'+
          (record.data_centers.find(dc=>dc.name===n.location?.data_center)?.racks||[]).map(rack=>
            '<option '+(n.location?.rack===rack?'selected':'')+'>'+esc(rack)+'</option>').join('')+'</select></label>'+
          '<label>Body<input readonly value="'+esc(n.location?.rack?n.name:'Unassigned')+'"></label>')+
          (n.role!=='dynamic'?'':'<label>Tenant<select id=ct-node-tenant><option value="">Unassigned</option>'+record.tenants.map(t=>
            '<option '+(n.tenant===t.path?'selected':'')+'>'+esc(t.path)+'</option>').join('')+'</select></label>')+
          '<label>Binary · bundled, version or path<input data-ct-field=binary value="'+esc(n.binary)+'"></label>'+
          '<label>CPU affinity<button id=ct-affinity aria-haspopup=dialog>'+esc(ctAffinityLabel(n.affinity))+'</button></label>'+
          (n.role==='static'?'<label>SectorMap count<input type=number min=1 max=64 data-ct-disk=count value="'+n.sector_map.count+'"></label>'+
            '<label>SectorMap size · GiB<input type=number min=1 max=1048576 data-ct-disk=size_gib value="'+n.sector_map.size_gib+'"></label>':'')+
          '</div></section>':''));
      const error=e=>{if(active())app.querySelector('#ct-error').innerHTML=displayError(e)};
      app.querySelector('#ct-name').oninput=e=>record.name=e.target.value;
      app.querySelectorAll('[data-ct-view]').forEach(b=>b.onclick=()=>{view=b.dataset.ctView;draw()});
      const clearDrag=()=>{dragged=null;app.querySelectorAll('.ct-drop').forEach(b=>b.classList.remove('ct-drop'))};
      app.onkeydown=e=>{if(e.key==='Escape')clearDrag()};
      app.querySelectorAll('[data-ct-node]').forEach(b=>{
        b.onclick=()=>{selected=+b.dataset.ctNode;draw()};
        b.ondragstart=e=>{dragged=+b.dataset.ctNode;e.dataTransfer.effectAllowed='move';e.dataTransfer.setData('text/plain',record.nodes[dragged].name)};
        b.ondragend=clearDrag;
      });
      app.querySelectorAll('[data-ct-drop]').forEach(b=>{
        const valid=()=>dragged!==null&&(view==='physical'||record.nodes[dragged].role!=='cli')&&(view!=='tenants'||record.nodes[dragged].role==='dynamic');
        b.ondragover=e=>{if(valid()){e.preventDefault();e.stopPropagation();e.dataTransfer.dropEffect='move';b.classList.add('ct-drop')}};
        b.ondragleave=e=>{if(!b.contains(e.relatedTarget))b.classList.remove('ct-drop')};
        b.ondrop=e=>{e.preventDefault();e.stopPropagation();if(valid()){const index=dragged;clearDrag();move(index,view,targets[+b.dataset.ctDrop])}};
      });
      app.querySelectorAll('[data-ct-location]').forEach(f=>f.onchange=()=>{
        if(f.dataset.ctLocation==='data_center'){
          const dc=record.data_centers.find(dc=>dc.name===f.value);
          if(dc&&!dc.racks.length)dc.racks.push(ctNextRack(dc));
          move(selected,'logical',[f.value,dc?.racks[0]||'']);
        }else move(selected,'logical',[n.location.data_center,f.value]);
      });
      const tenantSelect=app.querySelector('#ct-node-tenant');if(tenantSelect)tenantSelect.onchange=()=>move(selected,'tenants',tenantSelect.value);
      app.querySelectorAll('[data-ct-field]').forEach(f=>f.onchange=()=>{
        const field=f.dataset.ctField;if(field==='host_id'){move(selected,'physical',f.value);return}
        if(field==='name'){
          try{ctRenameNode(record,n,f.value);draw()}catch(e){draw();error(e)}
          return;
        }
        n[field]=f.type==='number'?Number(f.value):f.value;
        if(field==='role'){n.sector_map??={count:1,size_gib:64};ctNormalizeNodePlacement(n)}
        draw();
      });
      app.querySelectorAll('[data-ct-disk]').forEach(f=>f.onchange=()=>n.sector_map[f.dataset.ctDisk]=Number(f.value));
      const unique=role=>{let i=1;while(record.nodes.some(n=>n.name===role+'-'+i))i++;return role+'-'+i};
      const add=app.querySelector('#ct-add');if(add)add.onclick=()=>{if(record.nodes.length>=64)return error('At most 64 nodes');
        const node=ctDefaultNode(record.host_ids[0],'dynamic',1);node.location={data_center:'',rack:'',body:''};node.tenant='';
        node.name=unique('dynamic');record.nodes.push(node);selected=record.nodes.length-1;draw()};
      const addName=(title,existing,submit,suggested='')=>dialog(title,'<label>Name<input name=name required maxlength=80 autofocus value="'+esc(suggested)+'"></label>',data=>{
        const name=data.get('name').trim();if(!name||existing.includes(name))throw Error('Enter a unique name');
        if(existing.length>=64)throw Error('At most 64 entries');submit(name);
      });
      const editTenant=index=>{
        const t=record.tenants[index]||{path:'/Root/',storage_kind:'ssd',storage_groups:1};
        dialog(index===undefined?'Add tenant':'Edit tenant','<label>Database path<input name=path required maxlength=80 value="'+esc(t.path)+'"></label>'+
          '<label>Storage kind<select name=kind>'+['ssd','hdd'].map(k=>'<option '+(t.storage_kind===k?'selected':'')+'>'+k+'</option>').join('')+'</select></label>'+
          '<label>Storage groups<input name=groups type=number min=1 max=64 required value="'+t.storage_groups+'"></label>',data=>{
            const path=data.get('path').trim();
            if(!/^\/Root\/[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)*$/.test(path))throw Error('Use /Root/name with valid path components');
            if(record.tenants.some((v,i)=>i!==index&&v.path===path))throw Error('Tenant already exists');
            const value={path,storage_kind:data.get('kind'),storage_groups:Number(data.get('groups'))};
            if(index===undefined){if(record.tenants.length>=64)throw Error('At most 64 tenants');record.tenants.push(value)}
            else{record.tenants[index]=value;record.nodes.forEach(n=>{if(n.tenant===t.path)n.tenant=path})}
          });
      };
      app.querySelectorAll('[data-ct-tenant]').forEach(b=>b.onclick=()=>editTenant(+b.dataset.ctTenant));
      app.querySelectorAll('[data-ct-delete-tenant]').forEach(b=>b.onclick=()=>{
        const t=record.tenants[+b.dataset.ctDeleteTenant],count=record.nodes.filter(n=>n.tenant===t.path).length;
        const remove=()=>ctRemoveTenant(record,t.path);
        if(count)dialog('Remove tenant','<p>'+count+' nodes will have no tenant. Physical and logical placement will be kept.</p>',remove);
        else{remove();draw()}
      });
      app.querySelectorAll('[data-ct-delete-host]').forEach(b=>b.onclick=()=>{
        const host=record.host_ids[+b.dataset.ctDeleteHost],nodes=record.nodes.filter(n=>n.host_id===host);
        if(!nodes.length){ctRemoveHost(record,host);draw();return}
        const destinations=record.host_ids.filter(h=>h!==host);
        if(!destinations.length){error('Add another host to the template before removing a host with nodes');return}
        dialog('Remove host','<p>Move '+nodes.length+' nodes to another host. Logical placement and tenants will be kept. '+
          'Manual CPU masks will be cleared; placement strategies will be recalculated.</p><label>Destination host<select name=host>'+
          destinations.map(h=>'<option value="'+esc(h)+'">'+esc(hostName(h))+'</option>').join('')+'</select></label>',
          data=>ctRemoveHost(record,host,data.get('host')));
      });
      app.querySelectorAll('[data-ct-rack]').forEach(b=>b.onclick=()=>{
        const dc=record.data_centers[+b.dataset.ctRack];addName('Add rack',dc.racks,name=>dc.racks.push(name),ctNextRack(dc));
      });
      const deleteLocation=(dc,rack)=>{
        const count=record.nodes.filter(n=>n.location?.data_center===dc.name&&(rack===undefined||n.location.rack===rack)).length;
        const remove=()=>ctRemoveLocation(record,dc.name,rack);
        if(count)dialog(rack===undefined?'Remove DC':'Remove rack','<p>'+count+' nodes will move to Unassigned. '+
          'Physical placement and tenant assignments will be kept.</p>',remove);
        else{remove();draw()}
      };
      app.querySelectorAll('[data-ct-delete-dc]').forEach(b=>b.onclick=()=>deleteLocation(record.data_centers[+b.dataset.ctDeleteDc]));
      app.querySelectorAll('[data-ct-delete-rack]').forEach(b=>b.onclick=()=>{
        const [i,j]=b.dataset.ctDeleteRack.split(':').map(Number),dc=record.data_centers[i];deleteLocation(dc,dc.racks[j]);
      });
      const group=app.querySelector('#ct-group'),available=hosts.filter(h=>!record.host_ids.includes(h.id));
      group.disabled=view==='physical'&&!available.length;
      group.onclick=()=>{
        if(view==='tenants'){editTenant();return}
        if(view==='logical'){addName('Add DC',record.data_centers.map(dc=>dc.name),name=>record.data_centers.push({name,racks:[name+'-R1']}));return}
        dialog('Add host','<label>Host<select name=host>'+available.map(h=>'<option value="'+esc(h.id)+'">'+esc(h.name)+'</option>').join('')+'</select></label>',
          data=>{if(record.host_ids.length>=64)throw Error('At most 64 hosts');record.host_ids.push(data.get('host'))});
      };
      const removeNode=index=>dialog('Remove node','<p>Remove '+esc(record.nodes[index].name)+' from this template? '+
        'Its placement and tenant assignment will be removed. Running processes are not affected.</p>',()=>{
        record.nodes.splice(index,1);selected=Math.max(0,Math.min(selected,record.nodes.length-1));
      });
      app.querySelectorAll('[data-ct-remove-node]').forEach(b=>b.onclick=()=>removeNode(+b.dataset.ctRemoveNode));
      if(n){
        app.querySelector('#ct-remove').onclick=()=>removeNode(selected);
        const duplicate=app.querySelector('#ct-duplicate');if(duplicate)duplicate.onclick=()=>{if(record.nodes.length>=64)return error('At most 64 nodes');
          const copy=JSON.parse(JSON.stringify(n));copy.name=unique(n.role);ctNormalizeNodePlacement(copy);record.nodes.push(copy);selected=record.nodes.length-1;draw()};
        app.querySelector('#ct-affinity').onclick=e=>openAffinity(n,e.currentTarget);
      }
      app.querySelector('#ct-copy').onclick=()=>{delete record.id;delete record.revision;record.name+=' copy';draw()};
      const del=app.querySelector('#ct-delete');if(del)del.onclick=async()=>{
        if(!confirm('Delete template "'+record.name+'"?'))return;
        try{await api('/api/cluster-templates/delete',jsonOptions({id:record.id,revision:record.revision}));if(active())setRoute('cluster-templates')}catch(e){error(e)}
      };
      app.querySelector('#ct-save').onclick=async e=>{
        if(saving)return;saving=true;e.currentTarget.disabled=true;
        try{record=await api('/api/cluster-templates',jsonOptions(record));if(active()){
          if(id!==record.id)setRoute('cluster-templates/'+record.id);else{draw();app.querySelector('#ct-error').textContent='Saved'}
        }}catch(e){error(e)}finally{saving=false;const b=app.querySelector('#ct-save');if(b)b.disabled=false}
      };
      if(view==='physical')refreshCards();else previewVersion++;
    }
    async function openAffinity(node,anchor){
      const pop=document.createElement('div');pop.className='ct-pop';pop.setAttribute('popover','auto');
      pop.setAttribute('role','dialog');pop.setAttribute('aria-label','CPU affinity');app.appendChild(pop);
      const rect=anchor.getBoundingClientRect();pop.style.left=Math.max(12,Math.min(rect.left,innerWidth-704))+'px';
      pop.style.top=Math.max(12,Math.min(rect.bottom+6,innerHeight-360))+'px';
      let closed=false,request=0,data=null,mask=[],message='',busy=false;
      let popupPreviews=new Map(),popupErrors=[];
      const draft=JSON.parse(JSON.stringify(node.affinity));
      const close=()=>{if(closed)return;closed=true;removeEventListener('hashchange',close);pop.dispatchEvent(new Event('ct-cleanup'));
        if(pop.isConnected){if(pop.hidePopover)try{pop.hidePopover()}catch{}pop.remove()}if(anchor.isConnected)anchor.focus()};
      addEventListener('hashchange',close);
      if(pop.showPopover){pop.addEventListener('toggle',e=>{if(e.newState==='closed')close()});pop.showPopover()}
      else{const outside=e=>{if(!pop.contains(e.target)&&e.target!==anchor)close()};setTimeout(()=>{if(!closed)document.addEventListener('pointerdown',outside)},0);
        pop.addEventListener('ct-cleanup',()=>document.removeEventListener('pointerdown',outside),{once:true})}
      pop.addEventListener('keydown',e=>{if(e.key==='Escape'){e.preventDefault();close()}});
      const alive=()=>!closed&&active()&&pop.isConnected;
      async function refresh(){
        const version=++request;busy=true;message='';paint();
        try{
          const originals=record.nodes.filter(n=>n.host_id===node.host_id);
          const nodes=originals.map(n=>({...n,affinity:n===node?JSON.parse(JSON.stringify(draft)):n.affinity}));
          const plans=await ctResolvePlacements(nodes,topology);
          const response=await topology(node.host_id);if(!alive()||version!==request)return;
          data=response;
          popupPreviews=new Map();popupErrors=[];nodes.forEach((n,i)=>{const p=plans.get(n);
            if(p.supported)popupPreviews.set(originals[i],p);else if(originals[i]!==node)popupErrors.push(n.name+': '+p.reason)});
          if(draft.kind==='strategy'){
            const p=plans.get(nodes[originals.indexOf(node)]);
            if(!p?.supported)throw Error(p?.reason||'Placement preview unavailable');
            mask=p.cpus||[];
          }else mask=[...draft.cpus];
          const allowed=new Set(data.topology.allowed_cpus);if(mask.some(c=>!allowed.has(c)))throw Error('Some selected CPUs are no longer available');
        }catch(e){if(alive()&&version===request)message=e.message||String(e)}
        finally{if(alive()&&version===request){busy=false;paint()}}
      }
      function paint(){
        if(!alive())return;
        const manual=draft.kind==='manual',t=data?.topology,owners=new Map();
        record.nodes.filter(x=>x!==node&&x.host_id===node.host_id).forEach(x=>{
          const p=popupPreviews.get(x),cpus=x.affinity.kind==='manual'?x.affinity.cpus:p?.reserved_cpus||p?.cpus||[];
          cpus.forEach(c=>owners.set(c,{name:x.name,scope:p?.scope||'exclusive'}));
        });
        const used=new Set(owners.keys()),own=popupPreviews.get(node),reserved=own?.reserved_cpus||mask;
        const groups=t?.numa_nodes?.length?t.numa_nodes:[{id:0,cpus:t?.allowed_cpus||[]}];
        pop.innerHTML='<div class=runs-toolbar><strong>CPU affinity · '+esc(hostName(node.host_id))+'</strong><button id=ct-close aria-label="Close affinity picker">×</button></div>'+
          '<div class=ct-pop-tabs><button data-kind=strategy aria-pressed="'+!manual+'">Strategy</button>'+
          '<button data-kind=manual aria-pressed="'+manual+'">Manual</button></div>'+
          (!manual?'<div class=ct-fields><label>Placement<select id=ct-mode '+(busy?'disabled':'')+'>'+ (data?.affinity||[{mode:draft.mode,supported:true}]).map(a=>
            '<option value="'+esc(a.mode)+'" '+(draft.mode===a.mode?'selected':'')+' '+(!a.supported?'disabled':'')+'>'+esc(a.mode)+'</option>').join('')+
            '</select></label>'+
            '<label>CPU reserve<input id=ct-count type=number min=1 max=65536 value="'+draft.count+'" '+
            (busy||draft.mode==='none'?'disabled':'')+'></label></div>':'')+
          (busy?'<p role=status>Loading placement…</p>':'')+(message?displayError(message):'')+
          (t?'<div class=ct-numas>'+groups.map((g,gi)=>{
            const allowed=new Set(g.cpus),chips=(t.chiplets||[]).filter(c=>c.numa_node===g.id);
            const covered=new Set(chips.flatMap(c=>c.cpus));const rest=g.cpus.filter(c=>!covered.has(c));
            if(rest.length)chips.push({cpus:rest,label:'Other CPUs'});
            return '<section><div class=ct-group-title><strong>NUMA '+esc(g.id)+'</strong>'+
              (manual?'<button class=ct-small-action data-numa="'+gi+'">Select NUMA</button>':'')+'</div>'+
              chips.map((ch,ci)=>{
                const cpus=ch.cpus.filter(c=>allowed.has(c)),set=new Set(cpus),cores=[],seen=new Set();
                for(const core of t.physical_cores||[]){const items=core.filter(c=>set.has(c));if(items.length){cores.push(items);items.forEach(c=>seen.add(c))}}
                cpus.filter(c=>!seen.has(c)).forEach(c=>cores.push([c]));
                return '<div class=ct-chip><div class=ct-group-title><span>'+esc(ch.label||'Chiplet '+ci)+' · '+
                  cpus.filter(c=>!used.has(c)&&!reserved.includes(c)).length+'/'+cpus.length+' free</span>'+
                  (manual?'<button class=ct-small-action data-cpus="'+cpus.join(',')+'">Select</button>':'')+'</div>'+
                  (own?.scope&&cpus.some(c=>mask.includes(c))?'<div class="ct-scope-bar '+(own.scope==='numa'?'ct-double':'')+'"></div>':'')+'<div class=ct-cores>'+cores.map(core=>
                    '<div class=ct-core>'+core.map(c=>'<button class="ct-cpu '+(used.has(c)?'ct-other ct-'+owners.get(c).scope:'')+'" data-cpus="'+c+'" title="'+
                      esc(used.has(c)?owners.get(c).name+' · '+owners.get(c).scope+' reservation':'CPU '+c)+'" aria-label="CPU '+c+
                      (used.has(c)?', '+esc(owners.get(c).name)+' '+owners.get(c).scope+' reservation':'')+'" aria-pressed="'+reserved.includes(c)+'" '+
                      (!manual||busy?'disabled':'')+'>'+c+'</button>').join('')+'</div>').join('')+'</div></div>';
              }).join('')+'</section>';
          }).join('')+'</div>':'')+
          '<div class=ct-mask>'+(!manual&&draft.mode==='none'?'No pinning':mask.length+' logical CPUs · '+esc(cpuRanges(mask)))+'</div>'+
          '<button type=button class=ct-small-action title="Vertical pairs are SMT siblings. Grey cells reserve capacity for other nodes: '+
          'one line means chiplet, two mean NUMA; no line means fixed placement. Bars show the selected node affinity. Reservations are not CPU quotas.">?</button>'+
          popupErrors.map(e=>displayError(e)).join('')+
          '<div class=ct-pop-footer><button id=ct-cancel>Cancel</button><button id=ct-apply class=primary '+
          (busy||message||!data||(manual&&!mask.length)?'disabled':'')+'>Apply</button></div>';
        pop.querySelector('#ct-close').onclick=close;pop.querySelector('#ct-cancel').onclick=close;
        pop.querySelectorAll('[data-kind]').forEach(b=>b.onclick=()=>{
          draft.kind=b.dataset.kind;if(draft.kind==='manual')draft.cpus=[...mask];else{draft.mode??='pack-numa-pack-chiplet';draft.count??=mask.length||8}refresh()
        });
        function toggle(cpus){if(busy)return;const remove=cpus.every(c=>mask.includes(c)),set=new Set(mask);
          cpus.forEach(c=>remove?set.delete(c):set.add(c));mask=[...set].sort((a,b)=>a-b);draft.cpus=mask;
          message=mask.some(c=>!data.topology.allowed_cpus.includes(c))?'Some selected CPUs are no longer available':'';refresh()}
        pop.querySelectorAll('[data-cpus]').forEach(b=>b.onclick=()=>toggle(b.dataset.cpus.split(',').filter(Boolean).map(Number)));
        pop.querySelectorAll('[data-numa]').forEach(b=>b.onclick=()=>toggle(groups[+b.dataset.numa].cpus));
        const mode=pop.querySelector('#ct-mode');if(mode)mode.onchange=()=>{draft.mode=mode.value;delete draft.scope;refresh()};
        const count=pop.querySelector('#ct-count');if(count)count.onchange=()=>{draft.count=Number(count.value);refresh()};
        pop.querySelector('#ct-apply').onclick=()=>{
          node.affinity=manual?{kind:'manual',cpus:[...mask]}:{kind:'strategy',mode:draft.mode,count:draft.count};
          close();draw();app.querySelector('#ct-affinity')?.focus();
        };
        const height=pop.getBoundingClientRect().height;
        pop.style.top=Math.max(12,Math.min(rect.bottom+6,innerHeight-height-12))+'px';
      }
      refresh();
    }
    draw();
  }catch(e){if(active())app.innerHTML=shell('cluster-templates',displayError(e))}
}
"""
