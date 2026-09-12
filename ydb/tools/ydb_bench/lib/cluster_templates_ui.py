"""Placement-template editor assets for the existing benchmark web application."""

CSS = r"""
.ct-hosts{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(260px,100%),1fr));gap:16px}
.ct-host{padding:12px;background:var(--panel);border:1px solid var(--line);border-radius:6px;min-width:0}
.ct-node{display:block;width:100%;text-align:left;margin:8px 0;padding:10px;overflow-wrap:anywhere}
.ct-node span{display:block;color:var(--muted);margin-top:4px}
.ct-node[aria-pressed=true]{border-color:var(--accent);box-shadow:inset 3px 0 var(--accent)}
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
@media(pointer:coarse){.ct-cpu{min-height:44px}}
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
function ctDefaultNode(host,role,index){
  return {name:role+'-'+index,role,host_id:host,binary:'bundled',vcpu:8,
    actor_system:{use_shared_threads:false,use_united_pool:false,use_ring_queue:true},
    sector_map:{count:1,size_gib:64},affinity:{kind:'strategy',mode:'none',count:8}};
}
function ctAffinityLabel(value){const a=ctNormalizeAffinity(value);return a.kind==='manual'?'CPU '+cpuRanges(a.cpus):
  a.mode==='none'?'No pinning':a.mode+' · '+a.count+' CPU';}
async function renderClusterTemplates(id){
  clearRefresh();const generation=(renderClusterTemplates.version||0)+1,current=location.hash;
  renderClusterTemplates.version=generation;
  const active=()=>generation===renderClusterTemplates.version&&location.hash===current;
  try{
    const [records,directory]=await Promise.all([api('/api/cluster-templates'),api('/api/hosts')]);
    if(!active())return;
    const hosts=[directory.local,...directory.hosts],hostName=id=>hosts.find(h=>h.id===id)?.name||'Unavailable host';
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
      ctDefaultNode(hosts[1]?.id||directory.local.id,'dynamic',1),ctDefaultNode(directory.local.id,'cli',1)]}:
      JSON.parse(JSON.stringify(records.find(r=>r.id===id)||null));
    if(!record)throw Error('Template no longer exists');
    record.nodes.forEach(n=>n.affinity=ctNormalizeAffinity(n.affinity));
    let selected=0,saving=false;
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
          (p.reserved_cpus?'Reserve '+p.reserved_cpus.length+' CPU · affinity ':'')+p.cpus.length+' CPUs · '+cpuRanges(p.cpus)):p.reason;
          target.className=p.supported?'':'error'}
      });
    }
    function draw(){
      if(!active())return;
      const n=record.nodes[selected];
      const groups=[...new Set(record.nodes.map(n=>n.host_id))];
      app.innerHTML=shell('cluster-templates',
        '<div class=runs-toolbar><a href="#cluster-templates">Cluster templates</a><div class=runs-actions>'+
        '<button id=ct-copy>Copy template</button>'+(record.id?'<button id=ct-delete>Delete</button>':'')+
        '<button id=ct-save class=primary>Save template</button></div></div><div id=ct-error></div>'+
        '<div class=ct-fields><label>Template name<input id=ct-name maxlength=200 value="'+esc(record.name)+'"></label></div>'+
        '<p class=muted>Placement only · does not start a cluster</p><div class=ct-hosts>'+groups.map(host=>
          '<section class=ct-host><strong>'+esc(hostName(host))+'</strong>'+record.nodes.map((node,i)=>node.host_id!==host?'':
            '<button class=ct-node data-ct-node="'+i+'" aria-pressed="'+(selected===i)+'">'+esc(node.name)+' · '+esc(node.role)+
            '<span>'+(node.role==='cli'?'':esc(node.vcpu)+' vCPU · ')+esc(ctAffinityLabel(node.affinity))+'</span>'+
            '<span data-ct-plan="'+i+'">Calculating placement…</span></button>').join('')+
          '</section>').join('')+'</div><p><button id=ct-add>Add node</button></p>'+
        (n?'<section class=ct-editor><div class=runs-toolbar><strong>'+esc(n.name)+'</strong><div class=runs-actions>'+
          '<button id=ct-duplicate>Duplicate node</button><button id=ct-remove>Remove node</button></div></div><div class=ct-fields>'+
          '<label>Name<input data-ct-field=name maxlength=80 value="'+esc(n.name)+'"></label><label>Type<select data-ct-field=role>'+
          ['static','dynamic','cli'].map(v=>'<option '+(n.role===v?'selected':'')+'>'+v+'</option>').join('')+'</select></label>'+
          '<label>Host<select data-ct-field=host_id>'+(!hosts.some(h=>h.id===n.host_id)?'<option value="'+esc(n.host_id)+'">Unavailable host</option>':'')+
          hosts.map(h=>'<option value="'+esc(h.id)+'" '+(n.host_id===h.id?'selected':'')+'>'+esc(h.name)+'</option>').join('')+'</select></label>'+
          '<label>Binary · bundled, version or path<input data-ct-field=binary value="'+esc(n.binary)+'"></label>'+
          (n.role!=='cli'?'<label>Actor-system vCPU<input type=number min=1 max=65536 data-ct-field=vcpu value="'+n.vcpu+'"></label>':'')+
          '<label>CPU affinity<button id=ct-affinity aria-haspopup=dialog>'+esc(ctAffinityLabel(n.affinity))+'</button></label>'+
          (n.role==='static'?'<label>SectorMap count<input type=number min=1 max=64 data-ct-disk=count value="'+n.sector_map.count+'"></label>'+
            '<label>SectorMap size · GiB<input type=number min=1 max=1048576 data-ct-disk=size_gib value="'+n.sector_map.size_gib+'"></label>':'')+
          '</div>'+(n.role!=='cli'?'<div class=ct-flags>'+['use_shared_threads','use_united_pool','use_ring_queue'].map(k=>
            '<label><input type=checkbox data-ct-flag="'+k+'" '+(n.actor_system[k]?'checked':'')+'>'+esc(k)+'</label>').join('')+'</div>':'')+'</section>':''));
      const error=e=>{if(active())app.querySelector('#ct-error').innerHTML=displayError(e)};
      app.querySelector('#ct-name').oninput=e=>record.name=e.target.value;
      app.querySelectorAll('[data-ct-node]').forEach(b=>b.onclick=()=>{selected=+b.dataset.ctNode;draw()});
      app.querySelectorAll('[data-ct-field]').forEach(f=>f.onchange=()=>{
        const field=f.dataset.ctField;n[field]=f.type==='number'?Number(f.value):f.value;
        if(field==='role'){n.vcpu??=8;n.actor_system??=ctDefaultNode('',n.role,1).actor_system;n.sector_map??={count:1,size_gib:64}}
        if(field==='host_id')n.affinity={kind:'strategy',mode:'none',count:8};
        draw();
      });
      app.querySelectorAll('[data-ct-disk]').forEach(f=>f.onchange=()=>n.sector_map[f.dataset.ctDisk]=Number(f.value));
      app.querySelectorAll('[data-ct-flag]').forEach(f=>f.onchange=()=>n.actor_system[f.dataset.ctFlag]=f.checked);
      const unique=role=>{let i=1;while(record.nodes.some(n=>n.name===role+'-'+i))i++;return role+'-'+i};
      app.querySelector('#ct-add').onclick=()=>{if(record.nodes.length>=64)return error('At most 64 nodes');
        const node=ctDefaultNode(directory.local.id,'dynamic',1);node.name=unique('dynamic');record.nodes.push(node);selected=record.nodes.length-1;draw()};
      if(n){
        app.querySelector('#ct-remove').onclick=()=>{record.nodes.splice(selected,1);selected=Math.max(0,selected-1);draw()};
        app.querySelector('#ct-duplicate').onclick=()=>{if(record.nodes.length>=64)return error('At most 64 nodes');
          const copy=JSON.parse(JSON.stringify(n));copy.name=unique(n.role);record.nodes.push(copy);selected=record.nodes.length-1;draw()};
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
      refreshCards();
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
