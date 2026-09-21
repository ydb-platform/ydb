"""Generic protobuf configuration form embedded in cluster templates."""

CSS = r"""
.cc-fields details{padding:0;margin:4px 0}
.cc-fields details.cc-section{border:1px solid var(--line);border-radius:5px;margin:12px 0;overflow:hidden}
.cc-branch{border-left:1px solid var(--line);margin:0 12px 8px 22px;padding-left:10px}
.cc-section>.cc-branch{border-left:0;margin:0;padding:0 8px 8px}
.cc-fields summary{cursor:pointer;overflow-wrap:anywhere;padding-right:35px}
.cc-fields details>.cc-remove{float:right;margin-top:-25px}
.cc-row{display:grid;grid-template-columns:minmax(180px,290px) minmax(70px,140px) minmax(100px,1fr) auto;align-items:center;gap:12px;padding:10px 16px;border-bottom:1px solid var(--line)}
.cc-row:last-child{border-bottom:0}
.cc-inherited>.cc-row{border-bottom:1px solid var(--line)}
.cc-inherited:last-child>.cc-row{border-bottom:0}
.cc-row>label{display:contents}
.cc-row code{overflow-wrap:anywhere}
.cc-row .cc-type{display:none}
.cc-origin{color:var(--muted)}
.cc-row>label>input[type=checkbox]{justify-self:start}
.cc-row input:not([type=checkbox]),.cc-row select{max-width:100%;width:100%}
.cc-row textarea{width:100%;min-height:80px}
.cc-fields .cc-row>button{border:0;background:transparent;color:var(--accent);padding:4px;justify-self:end}
.cc-fields .cc-remove{margin-left:0}
.cc-fields .cc-item{margin:2px 0}
.cc-list .cc-origin,.cc-inherited .cc-origin,.cc-inherited .cc-branch .cc-section-status{display:none}
.cc-list .cc-row,.cc-inherited .cc-row{grid-template-columns:minmax(140px,290px) minmax(70px,180px) 1fr}
.cc-list .cc-row>.cc-remove,.cc-inherited .cc-row>button{justify-self:end}
.cc-fields .cc-more{margin:8px 16px;color:var(--accent)}
.cc-caption{overflow-wrap:anywhere}
.cc-yaml{width:100%;min-height:360px;font-family:monospace}
.cc-fields .cc-type{font-size:.85em;margin-left:8px}
.cc-inherit{display:inline-flex;align-items:center;gap:5px;margin-left:16px;font-size:.85em;font-weight:normal}
#cc-editor>label{display:grid;gap:5px;max-width:440px}
.cc-fields summary{display:flex;align-items:center;gap:12px;padding:8px 12px;flex-wrap:wrap;min-height:36px}
.cc-fields .cc-section>summary{background:var(--panel);border-bottom:1px solid var(--line);padding:12px 16px}
.cc-section-status{margin-left:auto;color:var(--muted);font-weight:normal}
.cc-fields summary .cc-remove{order:2;border:0;background:transparent;color:var(--muted);padding:4px}
.cc-fields summary .cc-add{order:3}
.cc-fields summary .cc-add{margin-left:0}
.cc-fields summary::before{content:'▸';font-size:22px;line-height:1;width:20px;flex:0 0 20px;text-align:center}
.cc-fields details[open]>summary::before{content:'▾'}
.cc-fields details>.cc-remove{float:none;margin-top:0}
.cc-inherited{color:var(--muted)}
.cc-controls{display:flex;align-items:end;gap:16px;flex-wrap:wrap;margin:16px 0}
.cc-controls>label{display:grid;gap:5px}
.cc-controls>.cc-add{margin-left:auto}
.cc-modes{display:flex;gap:16px;flex-wrap:wrap}
.cc-modes button{padding:8px 0;border:0;border-bottom:2px solid transparent;border-radius:0;background:transparent}
.cc-modes button[aria-pressed=true]{color:var(--accent);border-bottom-color:var(--accent)}
.cc-picker{width:min(600px,calc(100vw - 32px))}
.cc-picker label{display:grid;gap:6px;margin:12px 0}
.cc-picker select{width:100%}
.cc-picker .cc-row{grid-template-columns:minmax(140px,1fr) minmax(80px,1fr);padding:12px 0}
.cc-picker .cc-origin{display:none}
@media(max-width:650px){.cc-row{grid-template-columns:minmax(0,1fr) minmax(70px,120px)}.cc-origin{grid-column:1}.cc-section-status{margin-left:0}.cc-inherit{margin-left:0}}
"""

JS = r"""
let clusterConfigSchema;
function ccItemCaption(item,index){
  if(item&&typeof item==='object')for(const key of ['name','kind','id','domain_id','ssid']){
    if(item[key]!==undefined&&typeof item[key]!=='object'&&String(item[key]).length)return key+' · '+String(item[key]);
  }
  return 'Item '+(index+1);
}
function ccDefault(field,schema){
  if(field.map)return {};
  if(field.repeated)return [];
  if(field.message)return {};
  if(field.enum)return Object.keys(field.enum).find(k=>field.enum[k]===field.default)||Object.keys(field.enum)[0];
  return field.default??'';
}
function ccAdd(object,field,schema){
  if(field.oneof){
    const fields=Object.values(schema.messages).find(fields=>fields.includes(field))||[];
    if(fields.some(f=>f!==field&&f.oneof===field.oneof&&Object.hasOwn(object,f.name)))
      throw Error('Remove the current '+field.oneof+' value before choosing another variant');
  }
  object[field.name]=ccDefault(field,schema);
}
function ccInherited(base,replacements,path){
  if(replacements.some(p=>p.length<=path.length&&p.every((key,i)=>key===path[i])))return {};
  const value=path.reduce((value,key)=>value&&typeof value==='object'&&!Array.isArray(value)?value[key]:undefined,base);
  return value&&typeof value==='object'&&!Array.isArray(value)?value:{};
}
function ccDomain(record){return record.ydb_config?.domains_config?.domain?.[0]?.name||record.ydb_config?.domain_name||'Root'}
function ccMappingPath(root,target,path=[]){
  if(!root||typeof root!=='object'||Array.isArray(root))return null;
  if(root===target)return path;
  for(const [key,value] of Object.entries(root)){const found=ccMappingPath(value,target,[...path,key]);if(found)return found}
  return null;
}
function ccPruneReplacements(record){
  for(const [tenant,paths] of Object.entries(record.ydb_tenant_replacements||{})){
    const config=record.ydb_tenant_configs?.[tenant];
    if(!config){delete record.ydb_tenant_replacements[tenant];continue}
    record.ydb_tenant_replacements[tenant]=paths.filter(path=>{
      const value=path.reduce((value,key)=>value&&!Array.isArray(value)&&typeof value==='object'?value[key]:null,config);
      return value&&typeof value==='object'&&!Array.isArray(value);
    });
  }
}
function ccSetDomain(record,name){
  if(!/^[A-Za-z0-9_-]+$/.test(name))throw Error('Use letters, digits, underscore or dash for the domain name');
  const old='/'+ccDomain(record)+'/',next='/'+name+'/';
  const config=record.ydb_config??={};config.domains_config??={};
  config.domains_config.domain??=[{domain_id:1}];
  if(config.domains_config.domain.length!==1)throw Error('Cluster form requires exactly one domain; use Configuration');
  config.domains_config.domain[0].name=name;
  if(Object.hasOwn(config,'domain_name'))config.domain_name=name;
  for(const tenant of record.tenants||[])if(tenant.path.startsWith(old))tenant.path=next+tenant.path.slice(old.length);
  for(const node of record.nodes||[])if(node.tenant?.startsWith(old))node.tenant=next+node.tenant.slice(old.length);
  for(const key of ['ydb_tenant_configs','ydb_tenant_replacements'])if(record[key])record[key]=Object.fromEntries(Object.entries(record[key])
    .map(([path,config])=>[path.startsWith(old)?next+path.slice(old.length):path,config]));
}
async function renderClusterConfig(record,changeView,active,initialView='configuration'){
  app.innerHTML=shell('cluster-templates','<p>Loading YDB configuration schema…</p>');
  try{
    clusterConfigSchema??=await api('/api/cluster-config/schema');
    if(!active())return;
    const schema=clusterConfigSchema;record.ydb_config??={};let mode=initialView==='configuration'?'form':initialView,yamlText=null,busy=false;
    let scope='',originalYaml=null,effective=true;record.ydb_tenant_configs??={};
    const fingerprint=()=>{const value=JSON.parse(JSON.stringify(record));
      for(const key of ['ydb_tenant_configs','ydb_tenant_replacements']){
        value[key]=Object.fromEntries(Object.entries(value[key]||{}).filter(([,v])=>Object.keys(v).length));
      }return JSON.stringify(value)};
    let savedFingerprint=fingerprint();
    function dirty(){const status=app.querySelector('#cc-dirty');if(status)status.textContent=fingerprint()===savedFingerprint?'':'Unsaved changes'}
    const payload=()=>({config:record.ydb_config,tenant_configs:record.ydb_tenant_configs,tenant_replacements:record.ydb_tenant_replacements||{},tenants:(record.tenants||[]).map(t=>t.path)});
    const selectedConfig=()=>scope?(record.ydb_tenant_configs[scope]??={}):record.ydb_config;
    if(mode==='yaml'){yamlText=(await api('/api/cluster-config/validate',jsonOptions(payload()))).yaml;originalYaml=yamlText;if(!active())return}
    let version=0;const openPaths=new Set(),listLimits=new Map();
    const valid=()=>active()&&!!app.querySelector('#cc-editor');
    function error(e){if(valid())app.querySelector('#cc-message').textContent=e.message||String(e)}
    function yamlNotice(title,message){
      if(!valid())return;
      const dialog=document.createElement('dialog');dialog.className='import-dialog';dialog.setAttribute('aria-label',title);
      const heading=document.createElement('h3');heading.textContent=title;
      const body=document.createElement('p');body.style.whiteSpace='pre-wrap';body.textContent=message;
      const actions=document.createElement('div');actions.className='toolbar';
      const close=document.createElement('button');close.className='primary';close.textContent='OK';actions.append(close);
      const dismiss=()=>{dialog.close();dialog.remove();app.querySelector('#cc-apply, #cc-scope')?.focus()};
      close.onclick=dismiss;dialog.oncancel=e=>{e.preventDefault();dismiss()};
      dialog.append(heading,body,actions);app.append(dialog);dialog.showModal();close.focus();
    }
    function button(text,fn){const b=document.createElement('button');b.type='button';b.textContent=text;b.onclick=()=>{try{fn()}catch(e){error(e)}};return b}
    function inheritedFor(object){
      const path=scope?ccMappingPath(selectedConfig(),object):null;
      return path?ccInherited(record.ydb_config,record.ydb_tenant_replacements?.[scope]||[],path):{};
    }
    function pickField(object,name,path,trigger){
      if(!inputsValid())return;
      const fields=schema.messages[name].filter(f=>!Object.hasOwn(object,f.name));
      const inherited=inheritedFor(object),pop=document.createElement('dialog');pop.className='ct-dialog cc-picker';
      pop.setAttribute('aria-label',path==='config'?'Add section':'Add field');
      pop.innerHTML='<h3>'+(path==='config'?'Add section':'Add field')+'</h3><label>Search<input type=search placeholder="Field name…"></label>'+
        '<label>Field<select size=8></select></label><div class=cc-picker-value></div><p role=alert></p>'+
        '<div class=runs-actions><button type=button data-cancel>Cancel</button><button type=button class=primary data-add>Add</button></div>';
      app.append(pop);const search=pop.querySelector('input'),select=pop.querySelector('select'),value=pop.querySelector('.cc-picker-value');
      const add=pop.querySelector('[data-add]');let chosen=null,draft={};
      const close=()=>{pop.close();pop.remove();if(trigger?.isConnected)trigger.focus()};
      pop.querySelector('[data-cancel]').onclick=close;pop.oncancel=e=>{e.preventDefault();close()};
      function choose(){
        chosen=fields.find(f=>f.name===select.value);value.replaceChildren();add.disabled=!chosen;
        if(!chosen){add.textContent='Add';pop.querySelector('[role=alert]').textContent='No matching fields';return}
        pop.querySelector('[role=alert]').textContent='';
        const overridden=Object.hasOwn(inherited,chosen.name);add.textContent=overridden?'Override':'Add';
        draft={[chosen.name]:overridden?JSON.parse(JSON.stringify(inherited[chosen.name])):ccDefault(chosen,schema)};
        if(!chosen.message&&!chosen.repeated){renderField(value,draft,chosen.name,chosen,path+'.'+chosen.name,0);value.querySelector('.cc-remove')?.remove()}
      }
      function filter(){select.replaceChildren();for(const field of fields.filter(f=>f.name.toLowerCase().includes(search.value.toLowerCase()))){
        const option=document.createElement('option');option.value=field.name;option.textContent=field.name+(Object.hasOwn(inherited,field.name)?' · inherited':'');select.append(option)}
        if(select.options.length)select.selectedIndex=0;choose()}
      search.oninput=filter;select.onchange=choose;
      add.onclick=()=>{if(!chosen||![...value.querySelectorAll('input,select,textarea')].every(el=>el.reportValidity()))return;
        try{ccAdd(object,chosen,schema);object[chosen.name]=chosen.message&&!chosen.repeated?{}:draft[chosen.name];close();openPaths.add(path+'.'+chosen.name);drawFields()}
        catch(e){pop.querySelector('[role=alert]').textContent=e.message}};
      filter();pop.showModal();search.focus();
    }
    function renderMessage(parent,object,name,path,depth=0){
      if(depth>35){parent.textContent='Maximum editor depth reached; use YAML.';return}
      const fields=schema.messages[name],known=new Map(fields.map(f=>[f.name,f]));
      for(const [key,value] of Object.entries(object)){
        const field=known.get(key);
        renderField(parent,object,key,field,path+'.'+key,depth);
      }
      if(scope&&effective)for(const [key,value] of Object.entries(inheritedFor(object))){
        const field=known.get(key);if(Object.hasOwn(object,key))continue;
        renderInherited(parent,object,key,value,field,path+'.'+key,depth);
      }
      if(depth>0){const add=button('+',()=>pickField(object,name,path,add));add.className='cc-add';add.setAttribute('aria-label','Add field in '+path);
        parent.querySelector(':scope > summary')?.append(add)}
    }
    function renderInherited(parent,object,key,value,field,path,depth){
      const row=document.createElement('div');row.className='cc-inherited';
      renderField(row,{[key]:JSON.parse(JSON.stringify(value))},key,field,path,depth);
      row.querySelectorAll('button:not(.cc-more)').forEach(b=>b.remove());row.querySelectorAll('input,select,textarea').forEach(el=>el.disabled=true);
      const target=row.querySelector('summary')||row.querySelector('.cc-row');
      row.querySelectorAll('.cc-origin').forEach(label=>label.textContent='From cluster defaults');
      if(target.tagName==='SUMMARY')target.querySelector('.cc-section-status').textContent='From cluster defaults';
      const override=button('Override',()=>{if(field)ccAdd(object,field.name===key?field:{...field,name:key},schema);
        object[key]=value&&typeof value==='object'&&!Array.isArray(value)?{}:JSON.parse(JSON.stringify(value));drawFields()});
      override.style.marginLeft='auto';target.append(override);parent.append(row);
    }
    function renderField(parent,object,key,field,path,depth){
      const remove=()=>{if(field?.message&&Object.keys(object[key]||{}).length&&!confirm('Remove '+path+' and its values?'))return;delete object[key];drawFields()};
      if(field&&(field.message||field.repeated)){
        const details=document.createElement('details');details.dataset.ccPath=path;details.open=depth===0||openPaths.has(path);
        details.className=depth===0?'cc-section':'cc-object';if(field.repeated)details.classList.add('cc-list');
        const summary=document.createElement('summary'),caption=document.createElement('span');caption.className='cc-caption';
        caption.textContent=key+(field.map?' · map':field.repeated?' · '+(Array.isArray(object[key])?object[key].length:0)+' items':'');
        summary.append(caption);details.append(summary);
        const mappingPath=scope?ccMappingPath(selectedConfig(),object[key]):null;
        if(mappingPath?.length){
          const label=document.createElement('label');label.className='cc-inherit';
          label.title='Enabled: merge with inherited values. Disabled: replace this section entirely.';
          const input=document.createElement('input');input.type='checkbox';input.setAttribute('aria-label','!inherit '+path);
          input.checked=!(record.ydb_tenant_replacements?.[scope]||[]).some(p=>JSON.stringify(p)===JSON.stringify(mappingPath));
          label.onclick=e=>e.stopPropagation();input.onchange=()=>{
            record.ydb_tenant_replacements??={};
            const paths=(record.ydb_tenant_replacements[scope]||[]).filter(p=>JSON.stringify(p)!==JSON.stringify(mappingPath));
            if(!input.checked)paths.push(mappingPath);record.ydb_tenant_replacements[scope]=paths;drawFields();
          };
          label.append(input,document.createTextNode('!inherit'));summary.append(label);
        }
        const status=document.createElement('span');status.className='cc-section-status';
        const replacement=mappingPath&&(record.ydb_tenant_replacements?.[scope]||[]).some(p=>JSON.stringify(p)===JSON.stringify(mappingPath));
        status.textContent=mappingPath?(replacement?'Replace entire section':'Inherit · '+Object.keys(object[key]||{}).length+' overrides'):'';summary.append(status);
        const del=button('Unset',remove);del.className='cc-remove';del.setAttribute('aria-label','Unset '+path);summary.append(del);
        summary.addEventListener('click',e=>{if(e.target.closest('button'))e.preventDefault()});
        if(field.map){
          const [kf,vf]=schema.messages[field.message];
          for(const mapKey of Object.keys(object[key]))renderField(details,object[key],mapKey,{...vf,repeated:false},path+'['+mapKey+']',depth+1);
          if(scope&&effective)for(const [mapKey,value] of Object.entries(inheritedFor(object[key]))){
            if(!Object.hasOwn(object[key],mapKey))renderInherited(details,object[key],mapKey,value,{...vf,repeated:false},path+'['+mapKey+']',depth+1);
          }
          const row=document.createElement('div');row.className='cc-row';const input=document.createElement('input');
          input.placeholder='New map key';input.setAttribute('aria-label','New key for '+path);
          row.append(input,button('Add entry',()=>{const k=input.value;if(!k||['__proto__','constructor','prototype'].includes(k)||Object.hasOwn(object[key],k))throw Error('Enter a unique map key');
            if(kf.type===8&&!['true','false'].includes(k))throw Error('Map key must be true or false');
            object[key][k]=ccDefault(vf,schema);drawFields()}));details.append(row);
        }else if(field.repeated){
          if(!Array.isArray(object[key])){details.append(document.createTextNode('Invalid list; correct it in YAML.'));parent.append(details);return}
          const singleton=path==='config.domains_config.domain'&&object[key].length===1&&field.message;
          if(singleton){caption.textContent=key+' · '+(object[key][0].name||'Domain');
            renderMessage(details,object[key][0],field.message,path+'[0]',depth+1);
          }else{
          const limit=listLimits.get(scope+':'+path)||20;
          object[key].slice(0,limit).forEach((item,i)=>{
            const entry=document.createElement('div');entry.className='cc-item';
            const holder={[i]:item};renderField(entry,holder,String(i),{...field,repeated:false},path+'['+i+']',depth+1);
            const itemCaption=entry.querySelector('.cc-caption');if(itemCaption)itemCaption.textContent=ccItemCaption(item,i);
            entry.addEventListener('change',()=>{object[key][i]=holder[i]});
            entry.addEventListener('input',()=>{object[key][i]=holder[i];if(itemCaption)itemCaption.textContent=ccItemCaption(holder[i],i)});
            const removeButton=entry.querySelector('.cc-remove');if(removeButton)removeButton.onclick=()=>{object[key].splice(i,1);drawFields()};
            // Nested message edits share the original object; scalar edits copy back on change.
            details.append(entry);
          });
          if(object[key].length>limit){const more=button('Show more · '+(object[key].length-limit)+' remaining',()=>{
            listLimits.set(scope+':'+path,limit+20);drawFields()});more.className='cc-more';details.append(more)}
          details.append(button('Add item',()=>{object[key].push(ccDefault({...field,repeated:false},schema));drawFields()}));
          }
        }else renderMessage(details,object[key],field.message,path,depth+1);
        const branch=document.createElement('div');branch.className='cc-branch';
        while(details.children.length>1)branch.append(details.children[1]);details.append(branch);
        parent.append(details);return;
      }
      const row=document.createElement('div');row.className='cc-row';const label=document.createElement('label');
      const fieldName=document.createElement('code');fieldName.textContent=key;label.append(fieldName);
      const type=document.createElement('span');type.className='muted cc-type';
      type.textContent=!field?'unknown · preserved':field.enum?'enum':field.type===8?'bool':field.type===9?'string':field.type===12?'bytes · base64':'number';
      label.append(type);
      let input;
      if(!field){input=document.createElement('textarea');input.value=JSON.stringify(object[key],null,2);
        input.oninput=()=>{try{object[key]=JSON.parse(input.value);input.setCustomValidity('')}catch(e){input.setCustomValidity('Invalid JSON');error(e)}}}
      else if(field.type===8){input=document.createElement('input');input.type='checkbox';input.checked=object[key];input.onchange=()=>object[key]=input.checked}
      else if(field.enum){input=document.createElement('select');const current=object[key];
        const selected=typeof current==='boolean'?(current?'VALUE_TRUE':'VALUE_FALSE'):typeof current==='number'?Object.keys(field.enum).find(k=>field.enum[k]===current):String(current);
        for(const name of Object.keys(field.enum)){const opt=document.createElement('option');opt.value=name;opt.textContent=name;
          opt.selected=name.toLowerCase()===selected?.toLowerCase();input.append(opt)}
        input.onchange=()=>object[key]=input.value;
      }else{input=document.createElement('input');input.value=object[key];
        input.oninput=()=>{input.setCustomValidity('');if([9,12].includes(field.type)){object[key]=input.value;return}
          if([3,4,6,16,18].includes(field.type)){object[key]=input.value;return}
          const value=Number(input.value);if(!input.value||!Number.isFinite(value)){
            input.setCustomValidity('Invalid number');error(Error('Invalid number at '+path));return}object[key]=value;};}
      label.append(input);const inherited=Object.hasOwn(inheritedFor(object),key);
      const origin=document.createElement('span');origin.className='cc-origin';origin.textContent=scope?'Tenant override':'Cluster default';
      const del=button(inherited?'Reset to inherited':'Unset',remove);del.className='cc-remove';
      del.setAttribute('aria-label',(inherited?'Reset to inherited ':'Unset ')+path);row.append(label,origin,del);parent.append(row);
    }
    function inputsValid(){return ![...app.querySelectorAll('#cc-fields input,#cc-fields textarea')].some(input=>!input.reportValidity())}
    function drawFields(){if(!valid()||!inputsValid())return;
      ccPruneReplacements(record);
      app.querySelectorAll('[data-cc-path]').forEach(d=>{if(d.open)openPaths.add(d.dataset.ccPath);else openPaths.delete(d.dataset.ccPath)});
      if(mode==='cluster'){drawCluster();return}
      const container=app.querySelector('#cc-fields');container.replaceChildren();renderMessage(container,selectedConfig(),schema.root,'config');dirty()}
    function drawCluster(){
      const container=app.querySelector('#cc-fields'),config=record.ydb_config;
      container.innerHTML='<div class=ct-fields><label>Domain name<input id=cc-domain value="'+esc(ccDomain(record))+'"></label>'+
        '<label>Erasure<select id=cc-erasure>'+['none','block-4-2','mirror-3-dc'].map(v=>'<option>'+v+'</option>').join('')+'</select></label></div>'+
        '<p class=muted>Renaming the domain also updates tenant paths in this template. Existing run drafts are unchanged.</p>'+
        '<p id=cc-placement-warning class=muted></p><div id=cc-cluster-extra></div>';
      const domain=container.querySelector('#cc-domain');domain.oninput=()=>{try{ccSetDomain(record,domain.value);domain.setCustomValidity('')}
        catch(e){domain.setCustomValidity(e.message);error(e)}};
      const erasure=container.querySelector('#cc-erasure'),current=config.static_erasure??config.erasure??config.self_management_config?.erasure_species??'none';
      if(![...erasure.options].some(o=>o.value===current)){const option=document.createElement('option');option.value=option.textContent=current;erasure.append(option)}
      erasure.value=current;
      const warn=()=>{container.querySelector('#cc-placement-warning').textContent=erasure.value==='block-4-2'?
        'Placement requires at least 8 distinct racks with matching disks.':erasure.value==='mirror-3-dc'?
        'Placement requires at least 3 DCs, each with 3 distinct racks with matching disks.':'No data redundancy. Suitable for experiments only.'};
      erasure.onchange=()=>{let existing=false;for(const key of ['static_erasure','erasure'])if(Object.hasOwn(config,key)){config[key]=erasure.value;existing=true}
        if(Object.hasOwn(config.self_management_config||{},'erasure_species')||!existing){config.self_management_config??={};config.self_management_config.erasure_species=erasure.value}warn()};warn();
      const extra=container.querySelector('#cc-cluster-extra');
      const domainsField=schema.messages[schema.root].find(f=>f.name==='domains_config');
      const domainsFields=schema.messages[domainsField.message],domainField=domainsFields.find(f=>f.name==='domain');
      const poolField=schema.messages[domainField.message].find(f=>f.name==='storage_pool_types');
      for(const [title,key,field,owner] of [
        ['State storage','state_storage',domainsFields.find(f=>f.name==='state_storage'),config.domains_config],
        ['Storage pool types','storage_pool_types',poolField,Object.hasOwn(config,'storage_pool_types')?config:config.domains_config?.domain?.[0]]]){
        const section=document.createElement('section'),heading=document.createElement('h3');heading.textContent=title;section.append(heading);
        if(owner&&Object.hasOwn(owner,key))renderField(section,owner,key,field,'config.domains_config.'+key,0);
        else{const hint=document.createElement('p');hint.className='muted';hint.textContent=key==='state_storage'?
          'Automatic: generated by YDB from cluster placement.':'Automatic: SSD/HDD pools from the disks in the template.';section.append(hint);
          section.append(button('Configure '+title.toLowerCase(),()=>{ccSetDomain(record,ccDomain(record));
            const target=key==='state_storage'?config.domains_config:config.domains_config.domain[0];
            if(key==='state_storage'){
              const ids=record.nodes.flatMap((n,i)=>n.role==='static'?[i+1]:[]);
              if(!ids.length)throw Error('Add a static node first');
              target[key]=[{ssid:1,ring:{node:ids,nto_select:Math.min(5,ids.length%2?ids.length:ids.length-1)}}];
            }else{
              const media=[...new Set(record.nodes.filter(n=>n.role==='static').flatMap(n=>ctNodeDisks(n).map(d=>d.media)))];
              if(!media.length)throw Error('Add a disk to a static node first');
              target[key]=media.map(kind=>({kind,pool_config:{box_id:1,kind,erasure_species:erasure.value,vdisk_kind:'Default',
                pdisk_filter:[{property:[{type:kind==='ssd'?'SSD':'ROT'}]}]}}));
            }draw()}));}
        extra.append(section);
      }
      for(const key of ['fail_domain_type','default_disk_type']){
        const section=document.createElement('section');
        const field=schema.messages[schema.root].find(f=>f.name===key);
        if(Object.hasOwn(config,key))renderField(section,config,key,field,'config.'+key,0);
        else section.append(button('Configure '+key.replaceAll('_',' '),()=>{config[key]=key==='fail_domain_type'?'rack':'SSD';draw()}));
        extra.append(section);
      }
    }
    async function check(fromYaml=false){
      if(!fromYaml&&!inputsValid())throw Error('Correct invalid fields before continuing');
      const snapshot=()=>fromYaml?JSON.stringify([yamlText,record]):JSON.stringify(payload());
      const before=snapshot();
      const response=await api('/api/cluster-config/'+(fromYaml?'apply':'validate'),jsonOptions(fromYaml?{yaml:yamlText,template:record}:payload()));
      if(before!==snapshot())throw Error('Configuration changed during validation; try again');
      return response;
    }
    function draw(){
      if(!active())return;version++;
      app.innerHTML=shell('cluster-templates','<div id=cc-editor><div class=runs-toolbar><a href="#cluster-templates">Cluster templates</a><div class=runs-actions>'+
        '<button id=cc-export>Download YAML</button><button id=cc-validate>Validate types</button>'+
        '<span id=cc-dirty class=muted role=status></span><button id=cc-save class=primary>Save template</button></div></div>'+
        '<label>Template name<input id=cc-name value="'+esc(record.name)+'"></label><div class="profile-tabs ct-placement-tabs">'+
        ['cluster','physical','logical','tenants','configuration','yaml'].map(v=>'<button data-cc-view="'+v+'" aria-pressed="'+(v===(mode==='form'?'configuration':mode))+'">'+
          (v==='yaml'?'YAML':v[0].toUpperCase()+v.slice(1))+'</button>').join('')+'</div>'+
        '<div id=cc-message role=status></div>'+
        (mode==='cluster'?'<div id=cc-fields class=cc-fields></div>':mode==='form'?
        '<div class=cc-controls><label>Configuration scope<select id=cc-scope><option value="">Cluster defaults</option>'+record.tenants.map(t=>
          '<option value="'+esc(t.path)+'">Tenant '+esc(t.path)+'</option>').join('')+'</select></label>'+
        '<div class=cc-modes aria-label="Configuration display"><button id=cc-overrides>Overrides only</button><button id=cc-effective>Effective configuration</button></div>'+
        '<button id=cc-add-section class=cc-add>+ Add section</button></div><div id=cc-fields class=cc-fields></div>':
        '<textarea id=cc-text class=cc-yaml aria-label="YDB configuration YAML"></textarea><div class=runs-actions><button id=cc-apply>Apply YAML</button></div>')+'</div>');
      app.querySelector('#cc-name').oninput=e=>record.name=e.target.value;
      app.querySelectorAll('[data-cc-view]').forEach(b=>b.onclick=async()=>{const next=b.dataset.ccView==='configuration'?'form':b.dataset.ccView;if(next===mode)return;
        if(!inputsValid())return;
        if(mode==='yaml'&&yamlText!==originalYaml&&!confirm('Leave YAML without applying it?'))return;
        if(next==='yaml'){const request=version;try{const result=await check();if(!valid()||version!==request)return;
          yamlText=result.yaml;originalYaml=yamlText;mode='yaml';draw()}catch(e){error(e)};return}
        if(['form','cluster'].includes(next)){mode=next;draw()}else changeView(next)});
      if(mode==='cluster')drawCluster();
      else if(mode==='form'){
        const updateModes=()=>{for(const [id,state] of [['cc-effective',true],['cc-overrides',false]]){
          const button=app.querySelector('#'+id);button.hidden=!scope;button.setAttribute('aria-pressed',String(effective===state));
          button.onclick=()=>{effective=state;updateModes();drawFields()}}};updateModes();
        app.querySelector('#cc-add-section').onclick=e=>pickField(selectedConfig(),schema.root,'config',e.currentTarget);
        if(scope&&!record.tenants.some(t=>t.path===scope))scope='';
        app.querySelector('#cc-scope').value=scope;app.querySelector('#cc-scope').onchange=e=>{
          if(!inputsValid()){e.target.value=scope;return}scope=e.target.value;updateModes();drawFields()};
        drawFields()}
      else{app.querySelector('#cc-text').value=yamlText;app.querySelector('#cc-text').oninput=e=>yamlText=e.target.value;
        app.querySelector('#cc-apply').onclick=async()=>{const request=version;
          try{const result=await check(true);if(!valid()||version!==request)return;
            Object.assign(record,result.template);mode='form';draw();
            const added=Object.entries(result.added).filter(([,items])=>items.length).map(([kind,items])=>kind.replaceAll('_',' ')+': '+items.join(', '));
            yamlNotice('YAML applied',['Applied to the draft. Save template to persist changes.',...added,
              result.added.tenants.length?'New tenants: SSD, 1 storage group. Review these settings in Tenants.':'',
              result.unknown.length?'Preserved unknown fields: '+result.unknown.join(', '):''].filter(Boolean).join('\n'));
          }catch(e){if(valid()&&version===request)yamlNotice('Cannot apply YAML',e.message||String(e))}};}
      app.querySelector('#cc-validate').onclick=async()=>{const request=version;
        try{const result=await check(mode==='yaml');if(!valid()||version!==request)return;
          error(result.unknown.length?'Unvalidated fields preserved: '+result.unknown.join(', '):'Known field types are valid')}catch(e){error(e)}};
      app.querySelector('#cc-save').disabled=mode==='yaml';
      app.querySelector('#cc-export').onclick=async()=>{const request=version;try{const result=await check(mode==='yaml');
        if(!valid()||version!==request)return;const link=document.createElement('a');
        link.href=URL.createObjectURL(new Blob([result.yaml],{type:'application/yaml'}));link.download='ydb-config.yaml';
        link.click();URL.revokeObjectURL(link.href)}catch(e){error(e)}};
      app.querySelector('#cc-save').onclick=async()=>{if(busy||!inputsValid())return;busy=true;const request=version;app.querySelector('#cc-save').disabled=true;
        const before=JSON.stringify(record);
        try{const saved=await api('/api/cluster-templates',jsonOptions(record));if(!valid()||version!==request)return;
          if(before!==JSON.stringify(record)){record.id=saved.id;record.revision=saved.revision;error('Saved earlier values; new edits remain unsaved');return}
          Object.assign(record,saved);
          savedFingerprint=fingerprint();dirty();
          if(!location.hash.endsWith('/'+record.id))setRoute('cluster-templates/'+record.id);else error('Saved')}
        catch(e){error(e)}finally{busy=false;if(valid())app.querySelector('#cc-save').disabled=mode==='yaml'};};
      app.querySelector('#cc-editor').addEventListener('input',dirty);app.querySelector('#cc-editor').addEventListener('change',dirty);dirty();
    }
    draw();
  }catch(e){if(active())app.innerHTML=shell('cluster-templates',displayError(e)+'<a href="#cluster-templates">Cluster templates</a>')}
}
"""
