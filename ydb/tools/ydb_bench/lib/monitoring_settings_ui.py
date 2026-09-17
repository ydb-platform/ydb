"""Monitoring settings page in the existing benchmark shell."""

CSS = """
.settings-link{display:inline-flex;align-items:center;justify-content:center;min-width:2.3rem;
min-height:2.3rem;color:var(--muted);border-bottom:2px solid transparent}
.settings-link[aria-current=page]{color:var(--accent);border-bottom-color:var(--accent)}
.monitoring-layout{display:grid;grid-template-columns:minmax(0,2fr) minmax(16rem,1fr);gap:2rem}
.monitoring-form label.field{display:grid;gap:.4rem;margin:1rem 0}
.monitoring-form input[type=url],.monitoring-form input[type=password]{width:100%;min-width:0}
.monitoring-host{display:flex;justify-content:space-between;gap:1rem;padding:.8rem 0;border-bottom:1px solid var(--line)}
.monitoring-actions{display:flex;justify-content:flex-end;gap:1rem;align-items:center;margin-top:1.4rem}
.grafana-dialog{width:min(40rem,calc(100vw - 2rem))}.grafana-dashboard{display:flex;align-items:center;justify-content:space-between;gap:1rem;padding:1rem 0;border-bottom:1px solid var(--line)}
.grafana-dashboard small{display:block;color:var(--muted);margin-top:.3rem}.grafana-dialog .field{display:grid;gap:.4rem;margin-bottom:1rem}
@media(max-width:700px){.monitoring-layout{grid-template-columns:1fr}}
"""

JS = r"""
function grafanaLink(base,uid,runId,host,selection,from,to,datasource,scoped){
  const url=new URL(base.replace(/\/$/,'')+'/d/'+encodeURIComponent(uid));
  url.searchParams.set('from',String(Math.floor(from)));
  url.searchParams.set('to',String(Math.max(Math.ceil(to),Math.floor(from)+1000)));
  url.searchParams.set('timezone','browser');
  for(const [key,value] of Object.entries({bench_host:host,bench_run:runDisplay(runId),
    bench_benchmark:selection.benchmark||'$__all',bench_profile:selection.profile||'$__all',bench_attempt:selection.attempt||'$__all'}))url.searchParams.set('var-'+key,value);
  if(scoped)url.searchParams.set('var-database','$__all');
  if(datasource)url.searchParams.set('var-ds',datasource);
  return url.href;
}
async function mountGrafana(container,runId,selection,from,to){
  if(!container?.isConnected)return;
  const ticket={};container.grafanaTicket=ticket;
  container.replaceChildren();
  if(!Number.isFinite(from)||!Number.isFinite(to))return;
  try{
    const config=await api('/api/grafana/config');
    if(!container.isConnected||container.grafanaTicket!==ticket||!config.configured)return;
    const button=document.createElement('button');button.textContent='Open in Grafana';container.append(button);
    button.onclick=()=>showGrafanaChooser(button,runId,selection,from,to,config.local_id);
  }catch{}
}
async function showGrafanaChooser(opener,runId,selection,from,to,localId){
  document.querySelector('#grafana-dialog')?.close();
  const dialog=document.createElement('dialog');dialog.id='grafana-dialog';dialog.className='import-dialog grafana-dialog';
  dialog.setAttribute('aria-labelledby','grafana-title');
  dialog.innerHTML='<h2 id=grafana-title>Open in Grafana</h2><p class=muted>The attempt time range is applied when opening a dashboard.</p>'+
    '<div id=grafana-content>Loading dashboards…</div><p id=grafana-error role=alert></p><div class=toolbar><button id=grafana-retry>Refresh</button><button id=grafana-close>Close</button></div>';
  document.body.append(dialog);dialog.showModal();
  const close=()=>dialog.close();window.addEventListener('hashchange',close);
  dialog.onclose=()=>{window.removeEventListener('hashchange',close);dialog.remove();if(opener.isConnected)opener.focus()};
  dialog.querySelector('#grafana-close').onclick=close;
  let loading=false;
  const load=async()=>{
    if(loading)return;loading=true;
    const content=dialog.querySelector('#grafana-content'),error=dialog.querySelector('#grafana-error');
    error.textContent='';dialog.querySelector('#grafana-retry').disabled=true;
    try{
      const data=await api('/api/grafana/catalog');if(!dialog.isConnected)return;
      content.innerHTML='<label class=field>Prometheus datasource<select id=grafana-datasource>'+data.datasources.map(item=>
        '<option value="'+esc(item.uid)+'">'+esc(item.name)+'</option>').join('')+'</select></label>'+
        (data.truncated?'<p class=notice>Only the first 1000 dashboards are shown.</p>':'');
      const source=content.querySelector('select');
      if(data.datasources.some(item=>item.uid===data.datasource_uid))source.value=data.datasource_uid;
      const host=splitRunRef(runId)?.host||splitRunRef(decodeURIComponent(location.hash.split('/')[1]||''))?.host||viewedHost||localId;
      for(const item of data.dashboards){
        const row=document.createElement('div');row.className='grafana-dashboard';
        const label=document.createElement('div');label.textContent=item.title;
        const detail=document.createElement('small');
        detail.textContent=item.bundle?'Bundled · filters by host, run, profile and attempt':'Existing dashboard · benchmark filters depend on its queries';
        label.append(detail);row.append(label);
        if(item.installed){
          const link=document.createElement('a');link.className='button';link.textContent='Open';link.target='_blank';link.rel='noopener noreferrer';
          const update=()=>{link.href=grafanaLink(data.url,item.uid,runId,host,selection,from,to,source.value,!!item.bundle)};update();source.addEventListener('change',update);row.append(link);
        }else{
          const install=document.createElement('button');install.className='primary';install.textContent='Install and open';install.disabled=!source.options.length;row.append(install);
          install.onclick=async()=>{
            install.disabled=true;error.textContent='';
            try{
              await api('/api/grafana/install',jsonOptions({bundle:item.bundle,datasource_uid:source.value,revision:data.revision}));
              if(!dialog.isConnected)return;
              const url=grafanaLink(data.url,item.uid,runId,host,selection,from,to,source.value,true);
              const link=document.createElement('a');link.className='button primary';link.textContent='Installed — open';
              link.href=url;link.target='_blank';link.rel='noopener noreferrer';install.replaceWith(link);
              link.click();
            }catch(reason){if(dialog.isConnected){error.textContent=reason.message;install.disabled=false}}
          };
        }
        content.append(row);
      }
    }catch(reason){if(dialog.isConnected){content.textContent='Could not load dashboards.';error.textContent=reason.message}}
    finally{loading=false;if(dialog.isConnected)dialog.querySelector('#grafana-retry').disabled=false}
  };
  dialog.querySelector('#grafana-retry').onclick=load;await load();
}
async function mountMetricsExport(container,runId,selection={}){
  const path='/api/runs/'+enc(runId)+'/metrics-export',query=new URLSearchParams(selection).toString();
  async function refresh(){
    if(!container.isConnected)return;
    try{
      const value=await api(path+(query?'?'+query:''));
      if(!container.isConnected)return;
      container.replaceChildren();
      if(!value.configured)return;
      const button=document.createElement('button'),message=document.createElement('span');
      message.className='muted';message.setAttribute('role','status');
      const busy=['preparing','exporting'].includes(value.state);
      button.textContent=value.state==='completed'?'Metrics exported':busy?'Exporting metrics…':'Export metrics to Prometheus';
      button.disabled=busy||value.state==='completed';
      message.textContent=value.error||(value.state==='preparing'?
        (value.phase==='checking'?'Checking samples in Prometheus…':'Preparing archives…'):'');
      message.hidden=!message.textContent;
      container.append(button,message);
      button.onclick=async()=>{
        button.disabled=true;message.hidden=false;message.textContent='Starting export…';
        try{await api(path,jsonOptions(selection));await refresh()}
        catch(error){if(container.isConnected){message.textContent=error.message;button.disabled=false}}
      };
      if(busy)setTimeout(refresh,2000);
    }catch(error){if(container.isConnected)container.textContent='Metrics export unavailable: '+error.message}
  }
  await refresh();
}

async function renderMonitoringSettings(){
  clearRefresh();
  const route=location.hash;
  try{
    const [value,directory]=await Promise.all([api('/api/monitoring-settings'),api('/api/hosts')]);
    if(location.hash!==route)return;
    const names=new Map([directory.local,...directory.hosts].map(h=>[h.id,h.name]));
    const owner=names.get(value.owner_id)||value.owner_id;
    const hosts=[{id:value.local_id,name:directory.local.name,revision:value.revision},...value.hosts];
    app.innerHTML=shell('settings','<div class="tabs"><a class=active href="#settings" aria-current=page>Monitoring</a></div>'+
      '<form id=monitoring-form class=monitoring-form><div class=monitoring-layout><section>'+
      '<label class=field>Prometheus URL<input id=monitoring-prometheus type=url value="'+
      esc(value.settings.prometheus_url)+'" placeholder="https://prometheus.example"></label>'+
      '<p class=muted>Address reachable from benchmark hosts. Leave empty to disable the integration.</p>'+
      '<label class=field>Prometheus token · optional<input id=monitoring-token type=password autocomplete="new-password" '+
      'placeholder="'+(value.settings.has_prometheus_token?'Leave empty to keep saved token':'Bearer token')+'"></label>'+
      (value.settings.has_prometheus_token?'<p>Saved token: <code>'+esc(value.settings.prometheus_token_mask)+'</code></p>':'')+
      (value.settings.has_prometheus_token?'<label><input id=monitoring-remove-token type=checkbox> Remove saved token</label>':'')+
      '<p class=muted>Stored on benchmark hosts and synchronized with peers. Saved tokens are never returned to the browser.</p>'+
      '<p class=muted>Export archived metrics from a finished run or attempt. Prometheus requires the remote-write receiver and a historical sample window covering the run.</p>'+
      '<label class=field>Grafana URL · optional<input id=monitoring-grafana type=url value="'+esc(value.settings.grafana_url)+'" placeholder="https://grafana.example"></label>'+
      '<p class=muted>Grafana base address reachable from your browser.</p>'+
      '<label class=field>Grafana API URL<input id=monitoring-grafana-api type=url value="'+esc(value.settings.grafana_api_url||'')+'" placeholder="Same as Grafana URL"></label>'+
      '<p class=muted>Reachable from the settings owner. Requests from other benchmark hosts go through that server.</p>'+
      '<label class=field>Grafana service-account token<input id=monitoring-grafana-token type=password autocomplete="new-password" placeholder="'+
      (value.settings.has_grafana_token?'Leave empty to keep saved token':'Bearer token')+'"></label>'+
      (value.settings.has_grafana_token?'<p>Saved token: <code>'+esc(value.settings.grafana_token_mask)+'</code> <button type=button id=grafana-remove-token>Remove token</button></p>':'')+
      '<p class=muted>Dashboard read and creation permissions are needed. Existing dashboards are never overwritten.</p>'+
      '<label class=field>Default Prometheus datasource UID<input id=monitoring-grafana-datasource value="'+esc(value.settings.grafana_datasource_uid||'')+'"></label>'+
      '</section><section><div class=runs-toolbar><strong>Configuration sync</strong>'+
      '<button type=button id=monitoring-refresh>Refresh</button></div><p class=muted>Owner: '+esc(owner)+' · Revision '+esc(value.revision)+'</p>'+
      hosts.map(h=>'<div class=monitoring-host><span>'+esc(h.name||h.id)+'</span><span>'+
        esc(h.revision===null?'Unavailable':h.revision===value.revision?'Up to date':'Pending sync')+'</span></div>').join('')+
      '<p class=muted>Peers synchronize every 30 seconds.</p></section></div><div id=monitoring-error role=alert>'+esc(value.error)+'</div>'+
      '<div class=monitoring-actions><span id=monitoring-message role=status></span><button type=submit class=primary>Save for all hosts</button></div></form>');
    const form=app.querySelector('#monitoring-form');
    let removeGrafanaToken=false;
    const removeToken=form.querySelector('#grafana-remove-token');
    if(removeToken)removeToken.onclick=()=>{removeGrafanaToken=!removeGrafanaToken;removeToken.textContent=removeGrafanaToken?'Undo removal':'Remove token'};
    form.onsubmit=async event=>{
      event.preventDefault();const button=form.querySelector('[type=submit]');button.disabled=true;
      form.querySelector('#monitoring-error').textContent='';
      try{
        await api('/api/monitoring-settings',jsonOptions({revision:value.revision,settings:{
          prometheus_url:form.querySelector('#monitoring-prometheus').value.trim(),
          prometheus_token:form.querySelector('#monitoring-remove-token')?.checked?'':(form.querySelector('#monitoring-token').value||null),
          grafana_url:form.querySelector('#monitoring-grafana').value.trim(),
          grafana_api_url:form.querySelector('#monitoring-grafana-api').value.trim(),
          grafana_token:removeGrafanaToken?'':(form.querySelector('#monitoring-grafana-token').value||null),
          grafana_datasource_uid:form.querySelector('#monitoring-grafana-datasource').value.trim()}}));
        if(location.hash!==route)return;
        await renderMonitoringSettings();
        if(location.hash===route)app.querySelector('#monitoring-message').textContent='Saved. Peer synchronization may take up to 30 seconds.';
      }catch(error){if(form.isConnected)form.querySelector('#monitoring-error').textContent=error.message}
      finally{button.disabled=false}
    };
    form.querySelector('#monitoring-refresh').onclick=async event=>{
      event.currentTarget.disabled=true;
      try{await api('/api/monitoring-settings/refresh',jsonOptions({}));if(location.hash===route)await renderMonitoringSettings()}
      catch(error){if(form.isConnected){form.querySelector('#monitoring-error').textContent=error.message;form.querySelector('#monitoring-refresh').disabled=false}}
    };
  }catch(error){if(location.hash===route)app.innerHTML=shell('settings',displayError(error))}
}
"""
