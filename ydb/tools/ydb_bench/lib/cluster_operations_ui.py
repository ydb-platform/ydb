"""Dedicated-cluster command forms and retained operation results."""

CSS = r"""
.cluster-operations-layout{display:grid;grid-template-columns:12rem minmax(0,1fr);gap:1.5rem}
.cluster-operations-nav{display:flex;flex-direction:column;gap:.5rem}
.cluster-operations-nav button{text-align:left}
.cluster-operations-nav button[aria-pressed=true]{color:var(--accent);background:var(--panel);border-color:var(--accent)}
#operation-form .runs-actions{display:flex;justify-content:flex-end}
.cluster-operations-history{margin-top:1.5rem}
.cluster-operation-result{white-space:pre-wrap;overflow-wrap:anywhere}
@media(max-width:650px){.cluster-operations-layout{grid-template-columns:1fr}}
"""

JS = r"""
async function mountClusterOperations(container,id){
  const definitions={
    'create-pool':{label:'Create DDisk pool',fields:[
      ['name','Pool name','bench-ddisk'],['box','Box ID',1],['groups','DDisk groups',1],
      ['domains','Fail domains per group',3],['domain_begin','Domain level begin',10],
      ['domain_end','Domain level end',40],['disk_type','PDisk type',['SSD','ROT','NVME']]]},
    'create-partition':{label:'Create NBS partition',fields:[
      ['disk_id','Disk ID','bench-volume-1'],['pool','DDisk pool','bench-ddisk'],
      ['block_size','Block size, bytes',4096],['blocks_count','Block count',262144],
      ['media','Storage media',['ssd','mem']],['batch_size','Sync requests batch size',100]]},
    write:{label:'Write blocks',fields:[['disk_id','Disk ID','bench-volume-1'],['start','Start block',0],
      ['blocks_count','Block count',1],['block_size','Block size, bytes',4096],['pattern','Fill pattern','test-data']]},
    read:{label:'Read blocks',fields:[['disk_id','Disk ID','bench-volume-1'],['start','Start block',0],['blocks_count','Block count',1]]}
  };
  let command='create-partition',active=false,busy=false,request=null;const drafts={};
  const url='/api/runs/'+enc(id)+'/cluster-operations';
  container.innerHTML='<div class=cluster-operations-layout><nav class=cluster-operations-nav aria-label="Cluster operations">'+
    Object.entries(definitions).map(([key,d])=>'<button type=button data-operation="'+key+'">'+d.label+'</button>').join('')+
    '</nav><section><div class=run-section-title><h3 id=operation-title></h3><span id=operation-state class=muted></span></div>'+
    '<form id=operation-form><div class=form-grid id=operation-fields></div><p class=muted id=operation-hint></p>'+
    '<div class=runs-actions><button class=primary id=operation-submit>Execute</button></div></form>'+
    '<div id=operation-message aria-live=polite></div></section></div>'+
    '<section class=cluster-operations-history><h3>Operation history</h3><div id=operation-history></div></section>';
  const form=container.querySelector('form'),message=container.querySelector('#operation-message');
  function parameters(){return Object.fromEntries(definitions[command].fields.map(([key,label,value])=>
    [key,typeof value==='number'?Number(form.elements[key].value):form.elements[key].value]))}
  function enable(){
    container.querySelector('#operation-state').textContent=active?'Cluster ready':'Cluster is not active';
    for(const field of form.elements)field.disabled=busy||!active;
    for(const button of container.querySelectorAll('[data-operation]'))button.disabled=busy;
  }
  function render(){
    const d=definitions[command];container.querySelector('#operation-title').textContent=d.label;
    container.querySelector('#operation-fields').innerHTML=d.fields.map(([key,label,value])=>{
      const current=drafts[command]?.[key]??(Array.isArray(value)?value[0]:value);
      return '<label class=field>'+label+(Array.isArray(value)?'<select name="'+key+'">'+value.map(v=>
        '<option '+(v===current?'selected':'')+'>'+esc(v)+'</option>').join('')+'</select>':
        '<input required name="'+key+'" type="'+(typeof value==='number'?'number':'text')+'" '+
        (typeof value==='number'?'min="'+(['start','domain_begin'].includes(key)?0:1)+'" step="1" ':'')+
        'value="'+esc(current)+'">')+'</label>';
    }).join('');
    container.querySelector('#operation-submit').textContent=d.label;
    container.querySelector('#operation-hint').textContent=command==='write'?
      'Replaces data in the selected range. The pattern fills complete blocks; maximum write is 64 KiB. Block size must match the partition.':
      command==='read'?'Up to 16 blocks. Binary response buffers are shown as base64.':
      command==='create-pool'?'Geometry uses one realm and one DDisk per fail domain. Placement must fit the cluster.':'';
    for(const button of container.querySelectorAll('[data-operation]'))button.setAttribute('aria-pressed',String(button.dataset.operation===command));
    enable();
  }
  async function refresh(){
    const data=await api(url);if(!container.isConnected)return;
    active=data.active;enable();
    container.querySelector('#operation-history').innerHTML=data.history.length?data.history.slice().reverse().map(row=>
      '<details><summary>'+esc(row.started_at)+' · '+esc(definitions[row.command]?.label||row.command)+' · '+esc(row.status)+
      '</summary><pre class=cluster-operation-result>'+esc(JSON.stringify(row,null,2))+'</pre></details>').join(''):
      '<p class=muted>No operations yet.</p>';
  }
  for(const button of container.querySelectorAll('[data-operation]'))button.onclick=()=>{
    drafts[command]=parameters();command=button.dataset.operation;request=null;message.innerHTML='';render();
  };
  form.oninput=()=>{request=null};
  form.onsubmit=async event=>{
    event.preventDefault();if(busy||!active)return;
    if(!request){
      const bytes=crypto.getRandomValues(new Uint8Array(16));bytes[6]=(bytes[6]&15)|64;bytes[8]=(bytes[8]&63)|128;
      const hex=Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');
      request={id:[hex.slice(0,8),hex.slice(8,12),hex.slice(12,16),hex.slice(16,20),hex.slice(20)].join('-'),command,parameters:parameters()};
    }
    busy=true;enable();message.textContent='Executing…';
    try{
      const row=await api(url,jsonOptions(request));if(!container.isConnected)return;
      message.innerHTML='<p>'+esc(row.status)+'</p><pre class=cluster-operation-result>'+esc(JSON.stringify(row.response||row.error,null,2))+'</pre>';
      request=null;
    }catch(error){if(container.isConnected)message.innerHTML=displayError(error)+
      '<p class=muted>Outcome may be unknown. Submit unchanged parameters again to retrieve the same operation; it will not be replayed.</p>'}
    finally{busy=false;if(container.isConnected){enable();refresh().catch(error=>{message.innerHTML=displayError(error)})}}
  };
  render();
  async function poll(){if(!container.isConnected)return;try{await refresh()}catch(error){
    if(container.isConnected){active=false;enable();message.innerHTML=displayError(error)}}
    if(container.isConnected)setTimeout(poll,3000);
  }
  poll();
}
"""
