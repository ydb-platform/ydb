"""A small, same-origin browser client of the supervisor's existing control API."""

PAGE = r'''<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>ydbd_mocks_control</title>
<style>
body { font: 15px system-ui, sans-serif; margin: 32px auto; max-width: 1100px; padding: 0 20px; color: #20242a; }
h1 { font-size: 24px; } p { color: #59616c; line-height: 1.5; }
table { width: 100%; border-collapse: collapse; text-align: left; }
th, td { padding: 12px; border-bottom: 1px solid #ddd; } th { background: #f5f6f8; }
button { margin-right: 8px; padding: 6px 12px; cursor: pointer; } button:disabled { cursor: default; }
.pile { background: #f5f6f8; font-weight: 600; } #error { color: #b42318; min-height: 1.5em; }
</style>
<h1>ydbd_mocks_control</h1>
<p>Crash a host to close its gRPC listener and remove it from the pile's available nodes.
Each pile shares one mock process; the pile buttons crash or start that process.
Start restores availability; rejoin a disconnected pile in bridge_skipper.</p>
<p>Keep bridge_skipper open alongside this page. Switch it to Auto to watch failover;
confirmation takes several health rounds.</p>
<div id="error" role="alert"></div>
<table><thead><tr><th>Pile</th><th>Host</th><th>Endpoint</th><th>Status</th><th>Controls</th></tr></thead>
<tbody id="hosts"></tbody></table>
<p id="updated">Connecting…</p>
<script>
'use strict';
let inventory, busy = false;
const hosts = document.getElementById('hosts'), error = document.getElementById('error');
async function api(path, body) {
  const response = await fetch('/api/v1/' + path, body === undefined ? {} : {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
  });
  const result = await response.json();
  if (!response.ok || result.ok === false) throw new Error(result.error || JSON.stringify(result));
  return result;
}
function cell(row, value) {
  const td = row.insertCell(); td.textContent = value; return td;
}
function controls(row, path, body, crashDisabled, startDisabled) {
  const td = row.insertCell();
  for (const [label, disabled, change] of [
      ['Crash', crashDisabled, path === 'process' ? {action: 'kill'} : {on: true}],
      ['Start', startDisabled, path === 'process' ? {action: 'revive'} : {on: false}]]) {
    const button = document.createElement('button'); button.textContent = label; button.disabled = busy || disabled;
    button.id = JSON.stringify([path, body.pile ?? body.target, label]);
    button.onclick = async () => {
      busy = true; error.textContent = '';
      document.querySelectorAll('button').forEach(b => b.disabled = true);
      try { await api(path, {...body, ...change}); } catch (e) { error.textContent = e.message; }
      finally { busy = false; await refresh(); }
    };
    td.append(button);
  }
}
async function refresh() {
  try {
    inventory = inventory || await api('manifest');
    const state = await api('state');
    const rows = document.createElement('tbody');
    for (const pile of inventory.piles) {
      const instance = state.instances[pile.name], snapshot = instance.state;
      const row = rows.insertRow(); row.className = 'pile';
      cell(row, pile.name || '(unnamed)'); cell(row, 'All hosts'); cell(row, '');
      cell(row, !instance.alive ? 'Process stopped' : instance.responsive ? 'Process running' : 'Process unresponsive');
      controls(row, 'process', {pile: pile.name}, !instance.alive, instance.alive && instance.responsive);
      for (const node of inventory.nodes.filter(n => n.pile === pile.name)) {
        const status = snapshot && snapshot.nodes.find(n => n.label === node.label);
        const down = status && !!status.faults.down;
        const ownPile = snapshot && snapshot.piles.find(p => p.name === pile.name);
        const lifecycle = ownPile && ['DISCONNECTED', 'INACTIVE', 'NOT_SYNCHRONIZED'].includes(ownPile.public);
        const row = rows.insertRow(); cell(row, ''); cell(row, node.label); cell(row, node.dial_endpoint);
        cell(row, !instance.alive ? 'Process stopped' : !instance.responsive ? 'Unknown' :
          down ? 'Crashed' : lifecycle ? ownPile.public : 'Running');
        controls(row, 'fault', {target: node.label, op: 'down'}, !instance.responsive || down,
          !instance.responsive || !down);
      }
    }
    const focused = hosts.contains(document.activeElement) ? document.activeElement.id : null;
    hosts.replaceChildren(...rows.children);
    const restored = focused && document.getElementById(focused);
    if (restored && !restored.disabled) restored.focus({preventScroll: true});
    document.getElementById('updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch (e) { error.textContent = e.message; }
}
async function poll() { if (!busy) await refresh(); setTimeout(poll, 1000); }
poll();
</script></html>'''
