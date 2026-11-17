'use strict';
// Test-only Node process. The shipped UI is embedded C++ and needs no Node.
const assert = require('node:assert/strict');
assert.equal(process.version, 'v24.22.1', 'browser tests use pinned repository Node');
const fs = require('node:fs');
const path = require('node:path');
const bundle = process.env.SKIPPER_BROWSER_BUNDLE;
const playwright = require(path.join(bundle, 'node_modules/playwright-core'));
const url = process.env.SKIPPER_URL;
const artifacts = process.env.SKIPPER_ARTIFACTS;
const byId = (page, id) => page.getByTestId(id);

function failOnPageError(page) {
  page.on('pageerror', error => { throw error; });
}

async function until(predicate, message) {
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 40));
  }
  throw new Error(`Timed out: ${message}`);
}

async function state(page) {
  const response = await page.request.get(url + '/api/state');
  assert.equal(response.status(), 200);
  return response.json();
}

async function screenshot(page, name) {
  await page.screenshot({ path: path.join(artifacts, name + '.png'), fullPage: true });
}

async function pinnedFonts(page) {
  const fonts = fs.readdirSync(path.join(bundle, 'fonts')).sort().filter(name => /\.(ttf|otf)$/i.test(name));
  assert.ok(fonts.length, 'bundle includes pinned fonts');
  const encoded = fs.readFileSync(path.join(bundle, 'fonts', fonts[0])).toString('base64');
  await page.addStyleTag({ content: `@font-face{font-family:SkipperTest;src:url(data:font/ttf;base64,${encoded})}html,body,button,select,input{font-family:SkipperTest,sans-serif!important}` });
  await page.evaluate(() => document.fonts.ready);
}

async function startPage(browser, options = {}) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 }, ...options });
  const page = await context.newPage();
  failOnPageError(page);
  const response = await page.goto(url);
  assert.equal(response.status(), 200);
  const raw = await response.text();
  assert.ok(raw.includes('Bridge Skipper') && raw.includes('UTC') && raw.includes('PRIMARY') && raw.includes('SYNCHRONIZED') && raw.includes('data-pile-name="a"'), 'initial HTML contains actual content and labeled UTC');
  await byId(page, 'pile-card').first().waitFor();
  await pinnedFonts(page);
  return { context, page };
}

async function manual(browser) {
  const { context, page } = await startPage(browser);
  assert.equal((await state(page)).mode, 'observe');
  await byId(page, 'manual-mode').click();
  await until(async () => (await state(page)).mode === 'manual', 'Manual acknowledged');
  const review = page.locator('[data-action="failover"][data-pile-name="b"]').first();
  await review.click();
  const dialog = byId(page, 'review-dialog');
  await dialog.waitFor({ state: 'visible' });
  await until(async () => !(await byId(page, 'review-execute').isDisabled()) &&
    (await dialog.innerText()).includes('DISCONNECTED'), 'complete failover review displayed');
  assert.match(await dialog.innerText(), /DISCONNECTED/);
  assert.match(await dialog.innerText(), /Zero data loss/i);
  assert.match(await dialog.innerText(), /grpc:\/\//);
  const generation = (await state(page)).view.generation;
  const exact = JSON.parse(await dialog.locator('details').filter({ has: page.locator('summary').filter({ hasText: /^Exact update \/ Copy CLI$/ }) }).locator('pre').first().textContent());
  assert.equal(exact.expected_generation, generation, 'review names the exact expected generation field');
  await byId(page, 'review-cancel').click();
  assert.equal(await review.evaluate(element => element === document.activeElement), true, 'Cancel restores opener focus');
  await review.click();
  await byId(page, 'review-execute').click();
  await until(async () => {
    const current = await state(page);
    return current.pending_mutation !== null ||
      current.view?.piles.some(pile => pile.name === 'b' && pile.state === 'DISCONNECTED');
  }, 'one execution handed off');
  await until(async () => {
    const current = await state(page);
    return current.view?.piles.some(pile => pile.name === 'b' && pile.state === 'DISCONNECTED');
  }, 'observed transition');
  await until(async () => await page.locator('[data-testid="pile-card"][data-pile-name="b"] .state-badge').innerText() === 'DISCONNECTED',
    'browser card reconciles observed transition');
  await screenshot(page, 'manual');
  await context.close();
}

async function autoOff(browser) {
  const { context, page } = await startPage(browser);
  await byId(page, 'auto-mode').click();
  await byId(page, 'review-dialog').waitFor({ state: 'visible' });
  assert.match(await byId(page, 'review-dialog').innerText(), /Auto/i);
  await byId(page, 'review-execute').click();
  await until(async () => (await state(page)).mode === 'auto', 'Auto acknowledged');
  await byId(page, 'auto-mode').click();
  await until(async () => (await state(page)).mode === 'observe', 'Off acknowledged');
  await until(async () => /Observe/i.test(await byId(page, 'effective-mode').innerText()), 'Observe controls reconciled');
  assert.match(await byId(page, 'effective-mode').innerText(), /Observe/i);
  await screenshot(page, 'auto-off');
  await context.close();
}

async function pendingOff(browser) {
  const { context, page } = await startPage(browser);
  await byId(page, 'auto-mode').click();
  await byId(page, 'review-dialog').waitFor({ state: 'visible' });
  assert.match(await byId(page, 'review-dialog').innerText(), /DISCONNECTED/);
  await byId(page, 'review-execute').click();
  await until(async () => (await state(page)).last_mutation?.outcome === 'AMBIGUOUS', 'ambiguous backend outcome');
  await byId(page, 'auto-mode').click();
  await until(async () => (await state(page)).mode === 'observe', 'Off acknowledged over pending request');
  await until(async () => (await byId(page, 'effective-mode').innerText()).includes('Observe'), 'canonical Observe controls');
  assert.match(await page.locator('#pending-status').innerText(), /may still complete/i);
  assert.equal(await page.locator('[data-testid="pile-card"][data-pile-name="a"]').locator('.state-badge').innerText(), 'PRIMARY');
  assert.equal(await page.locator('[data-testid="pile-card"][data-pile-name="b"]').locator('.state-badge').innerText(), 'SYNCHRONIZED');
  assert.equal(await page.locator('#review-failover').isDisabled(), true, 'Observe has no execute/retry control');
  await screenshot(page, 'pending-off');
  await context.close();
}

async function mockControlFocus(browser) {
  const context = await browser.newContext();
  const page = await context.newPage();
  failOnPageError(page);
  const piles = ['pile1', 'pile2'];
  const nodes = piles.flatMap(pile => [1, 2, 3].map(number => ({
    label: `mock-${pile}-${number}`, pile, dial_endpoint: `127.0.0.1:${22100 + piles.indexOf(pile) * 10 + number - 1}`,
  })));
  const manifest = { cluster_id: 'focus-fixture', piles: piles.map(name => ({ name })), nodes };
  const state = { cluster_id: manifest.cluster_id, instances: Object.fromEntries(piles.map((pile, index) => [pile, {
    alive: true, responsive: true, state: {
      piles: [{ name: pile, public: index === 0 ? 'PRIMARY' : 'SYNCHRONIZED' }],
      nodes: nodes.filter(node => node.pile === pile).map(node => ({ label: node.label, faults: {} })),
    },
  }])) };
  const actions = [];
  await page.route(url + '/', route => route.fulfill({
    contentType: 'text/html', body: fs.readFileSync(path.join(artifacts, 'mock-control.html'), 'utf8'),
  }));
  await page.route('**/api/v1/**', async route => {
    const endpoint = new URL(route.request().url()).pathname;
    let body;
    if (endpoint === '/api/v1/manifest') body = manifest;
    else if (endpoint === '/api/v1/state') body = state;
    else {
      assert.equal(endpoint, '/api/v1/fault');
      const action = route.request().postDataJSON();
      actions.push(action);
      const node = nodes.find(node => node.label === action.target);
      assert.ok(node, 'keyboard action targets a fixture host');
      state.instances[node.pile].state.nodes.find(status => status.label === node.label).faults.down = action.on;
      body = { ok: true };
    }
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(body) });
  });
  await page.goto(url);
  const host = 'mock-pile2-2';
  const row = page.getByRole('row').filter({ has: page.getByRole('cell', { name: host, exact: true }) });
  const crash = row.getByRole('button', { name: 'Crash', exact: true });
  await crash.waitFor();
  await crash.focus();
  const updated = await page.locator('#updated').textContent();
  await until(async () => await page.locator('#updated').textContent() !== updated, 'control table refresh completed');
  assert.equal(await crash.evaluate(element => element === document.activeElement), true,
    'a background refresh preserves the focused host action');
  await page.keyboard.press('Enter');
  await until(async () => await row.getByRole('cell', { name: 'Crashed', exact: true }).count() === 1,
    'keyboard Crash is reflected in host status');
  assert.deepEqual(actions, [{ target: host, op: 'down', on: true }], 'Enter sends exactly the focused host action');
  await context.close();
}

function fixtures(original, count, expired = false) {
  const data = structuredClone(original);
  data.controller_boot_id = 'fixture';
  data.snapshot_revision = 'fixture:snapshot:1';
  const at = '2026-11-01T05:30:00Z'; // DST overlap in America/New_York.
  data.now = '2026-11-01T06:40:30Z';
  data.mode = 'observe';
  data.mode_text = 'Observe · collection continues; no new dispatch.';
  data.incident = { active: true, piles: ['pile-01'] };
  data.decision = { kind: 'BLOCKED', reason: 'Fixture pending observation', blocker: 'NO_VIEW', plan: null, cli_command: null };
  data.decision_text = 'Actions blocked: Fixture pending observation';
  data.failover_action = { label: 'Review failover', available: false, reason: 'No Ready failover plan' };
  data.cards = Array.from({ length: count }, (_, index) => {
    const name = `pile-${String(index + 1).padStart(2, '0')}`;
    const evidence = { age_ms: expired ? 630000 : 3000, ttl_ms: 30000, expired,
      expired_for_ms: expired ? 600000 : 0, sample_at: '2026-11-01T06:30:00Z', expires_at: '2026-11-01T06:30:30Z' };
    const metadata = { verdict: index === 0 ? 'FAILED' : 'HEALTHY', reasons: ['Fixture evidence'],
      fallback_red: false, self_quorum: true, evidence: { ...evidence, source: 'controller_assessment' },
      assessment: expired ? 'expired' : 'available', down_since: null, confirmed: index === 0 };
    return { name, display_name: name, state: index === 0 ? 'PRIMARY' : 'SYNCHRONIZED', state_current: !expired,
      assessment: expired ? 'expired' : 'available', verdict: expired ? 'UNKNOWN' : index === 0 ? 'FAILED' : 'HEALTHY',
      last_known_verdict: index === 0 ? 'FAILED' : 'HEALTHY', confirmed: index === 0, incident: index === 0,
      health_text: expired ? 'Health unknown · expired 10 min ago' : index === 0 ? 'Failed · confirmed' : 'Healthy',
      evidence_age_text: expired ? 'Last sample 10 min ago' : 'Last sample 3 s ago',
      context_action: index === 0 ? null : 'switchover',
      evidence, state_evidence: evidence, endpoint_uris: [], reasons: metadata.reasons,
      fallback_red: metadata.fallback_red, self_quorum: metadata.self_quorum, down_since: null,
      current_assessment: structuredClone(metadata), last_known_assessment: structuredClone(metadata),
      actions: Object.fromEntries(['switchover', 'take_offline', 'rejoin', 'failover'].map(action =>
        [action, { label: action, available: false, reason: 'Enable Manual and obtain fresh evidence' }])) };
  });
  data.piles = data.cards.map(card => ({ name: card.name, ...structuredClone(card.current_assessment), connectivity: [] }));
  data.last_known_piles = [];
  data.endpoints = [];
  data.view = { ...data.view, piles: data.cards.map(card => ({ name: card.name, state: card.state })) };
  data.history = { rows: [{ id: 'fixture:1', at, type: 'view', generation: '42', origin: 'baseline',
    summary: 'Observation baseline', origin_label: 'Baseline' },
    { id: 'fixture:2', at: '2026-11-01T06:30:00Z', type: 'view', generation: '43', origin: 'external',
      summary: 'Observed pile state transition', origin_label: 'External/unknown' }], coverage_gap: true, next_cursor: 'fixture:1' };
  data.logs = { boot_id: 'fixture', rows: [{ id: 'fixture:1', at, level: 'info', message: 'Retained fixture log' }],
    coverage_gap: true, next_cursor: 'fixture:1', capture_level: 'info' };
  return data;
}

async function fixturePage(browser, data, options = {}) {
  const context = await browser.newContext(options);
  const page = await context.newPage();
  failOnPageError(page);
  await page.route('**/api/state', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data) }));
  await page.route('**/api/logs*', route => {
    // This mock is the fixture's server: the product consumes its authoritative
    // filtered order and cursor, just as it does the real diagnostic endpoint.
    const query = new URL(route.request().url()).searchParams;
    const asciiFold = value => value.replace(/[A-Z]/g, character => character.toLowerCase());
    const rank = { trace: 0, debug: 1, info: 2, warn: 3, error: 4, fatal: 5 };
    const after = query.get('after') || '';
    const index = (data.logs.rows || []).findIndex(row => (row.cursor || row.id) === after);
    const candidates = after && index >= 0 ? data.logs.rows.slice(index + 1) : data.logs.rows || [];
    const rows = candidates.filter(row => (rank[row.level] ?? 2) >= (rank[query.get('level') || 'info'] ?? 2) &&
      asciiFold(row.message || '').includes(asciiFold(query.get('pile') || '')) &&
      asciiFold(JSON.stringify(row)).includes(asciiFold(query.get('search') || '')));
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ...data.logs, rows,
      coverage_gap: !!data.logs.coverage_gap || !!after && index < 0 }) });
  });
  await page.route('**/api/history*', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data.history) }));
  await page.goto(url);
  await pinnedFonts(page);
  await until(async () => await byId(page, 'pile-card').count() === data.cards.length &&
    await byId(page, 'pile-card').first().getAttribute('data-pile-name') === data.cards[0].name, 'fixture cards');
  return { context, page };
}

async function presentation(browser) {
  const response = await fetch(url + '/api/state');
  const original = await response.json();
  for (const [width, height] of [[640, 800], [960, 720], [1280, 800], [1600, 900]]) {
    for (const count of [2, 3, 4, 9, 20]) {
      const { context, page } = await fixturePage(browser, fixtures(original, count), { viewport: { width, height } });
      const cards = byId(page, 'pile-card');
      const boxes = await cards.evaluateAll(elements => elements.map(element => {
        const box = element.getBoundingClientRect();
        return { x: Math.round(box.x), y: Math.round(box.y), width: Math.round(box.width) };
      }));
      const columns = width < 760 ? 1 : width < 1440 ? 2 : 3;
      assert.equal(new Set(boxes.slice(0, Math.min(count, columns)).map(box => box.x)).size, Math.min(count, columns));
      if (count > columns) assert.equal(boxes[columns].x, boxes[0].x, 'incomplete rows align left');
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'no page horizontal overflow');
      assert.equal(await cards.first().evaluate(element => getComputedStyle(element).borderTopWidth), '2px');
      await cards.last().scrollIntoViewIfNeeded();
      assert.ok((await cards.last().boundingBox()).height > 0, 'last card reachable by vertical scroll');
      await byId(page, 'theme-select').selectOption('dark');
      const dark = await byId(page, 'logs-tail').evaluate(element => getComputedStyle(element).backgroundColor);
      await byId(page, 'theme-select').selectOption('light');
      const light = await byId(page, 'logs-tail').evaluate(element => getComputedStyle(element).backgroundColor);
      assert.notEqual(light, dark, 'logs follow whole-page theme');
      await screenshot(page, `layout-${width}-${count}`);
      await context.close();
    }
  }
  const data = fixtures(original, 4);
  data.cards[2].name = data.cards[2].display_name = 'exact pile with <tags> & "quotes" ' + 'long-name-'.repeat(20);
  const { context, page } = await fixturePage(browser, data, { viewport: { width: 640, height: 800 } });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  await byId(page, 'pile-card').first().locator('.more summary').click();
  const disabled = byId(page, 'pile-card').first().locator('[data-action="switchover"]').first();
  assert.equal(await disabled.isDisabled(), true);
  const reasonId = await disabled.getAttribute('aria-describedby');
  assert.ok(reasonId, 'disabled action is linked to a reason');
  assert.match(await page.locator('#' + reasonId).innerText(), /Enable Manual/);
  const wrapper = disabled.locator('..');
  await wrapper.focus();
  assert.equal(await wrapper.getAttribute('tabindex'), '0', 'disabled reason is reachable by keyboard');
  await byId(page, 'pile-card').first().locator('.more summary').click();
  const details = page.locator('[data-details]').first();
  await details.focus();
  await page.keyboard.press('Enter');
  await byId(page, 'evidence-drawer').waitFor({ state: 'visible' });
  await page.keyboard.press('Escape');
  assert.equal(await details.evaluate(element => element === document.activeElement), true, 'drawer restores focus');
  await page.locator('#log-follow').uncheck();
  const summary = page.locator('#logs-tail summary').filter({ hasText: 'Retained fixture log' }).first();
  await summary.evaluate(element => {
    const range = document.createRange();
    range.selectNodeContents(element);
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
  });
  const selection = await page.evaluate(() => window.getSelection().toString());
  data.logs.rows.push({ id: 'fixture:3', at: '2026-11-01T06:30:00Z', level: 'warn', message: 'Appended fixture log' });
  data.logs.next_cursor = 'fixture:3';
  await until(async () => await page.locator('#logs-tail summary').filter({ hasText: 'Appended fixture log' }).count() === 1, 'logs append');
  assert.equal(await page.evaluate(() => window.getSelection().toString()), selection, 'appending preserves log selection');
  assert.equal(await page.locator('#log-follow').isChecked(), false, 'Follow remains paused');
  await page.locator('#jump-latest').click();
  assert.equal(await page.locator('#log-follow').isChecked(), true, 'Jump resumes Follow');
  await page.locator('#log-search').fill('Appended');
  await until(async () => await page.locator('#logs-tail .log-row:visible').count() === 1, 'log filter applied');
  assert.match(await page.locator('#download-logs').getAttribute('href'), /search=Appended/);
  // Playwright retargets OPTION inside LABEL to its SELECT for isDisabled;
  // assert the native option property so the actual unavailable choice is tested.
  assert.equal(await page.locator('#log-level option[value="debug"]').evaluate(option => option.disabled), true, 'uncaptured Debug is unavailable');
  assert.match(await page.locator('#capture-level').innerText(), /Debug filtering unavailable/i);
  data.history = { boot_id: 'fixture', rows: Array.from({ length: 4 }, (_, index) => ({
    cursor: 'fixture:' + (101 + index), at: new Date(Date.parse('2026-11-01T05:30:00Z') + index * 1000).toISOString(),
    type: 'view', generation: String(index + 1), message: 'Retained transition ' + (index + 1),
    summary: 'Retained transition ' + (index + 1), origin: 'external', origin_label: 'External/unknown'
  })), next_cursor: 'fixture:104', coverage_gap: false };
  const expandedReply = page.waitForResponse(response => new URL(response.url()).pathname === '/api/history');
  await page.locator('#history-expand').click();
  assert.equal((await (await expandedReply).json()).rows.length, 4, 'Show more reads the complete retained history');
  await until(async () => await page.locator('.history-table tbody tr').count() === 4, 'expanded retained history rendered');
  data.history.rows.push({ cursor: 'fixture:105', at: '2026-11-01T05:30:05Z', type: 'view', generation: '5',
    message: 'Live expanded-history transition', summary: 'Live expanded-history transition', origin: 'external', origin_label: 'External/unknown' });
  data.history.next_cursor = 'fixture:105';
  await until(async () => await page.locator('.history-table tbody tr').filter({ hasText: 'Live expanded-history transition' }).count() === 1,
    'new retained transition appears while history remains expanded');
  assert.match(await page.locator('#history-expand').innerText(), /Show three/i, 'live updates preserve the expanded view');
  await page.locator('#history-expand').click();
  const latestThree = ['Retained transition 3', 'Retained transition 4', 'Live expanded-history transition'];
  await until(async () => JSON.stringify(await page.locator('.history-table tbody tr td:nth-child(3)').allInnerTexts()) === JSON.stringify(latestThree),
    'collapsed history shows the latest three observed transitions');
  assert.equal(await page.locator('.history-table tbody tr').count(), 3, 'Show three uses the latest observed history');
  await screenshot(page, 'long-content');
  await context.close();
}

function logRow(index, message = 'Retained row ' + index, level = 'info', prefix = 'logs') {
  return { cursor: prefix + ':' + index, at: new Date(Date.parse('2026-11-01T06:30:00Z') + index).toISOString(), level, message };
}

async function logFixturePage(browser, data, readLogs) {
  // Fixture rows and their canonical state share a boot. A synthetic boot
  // transition invalidates the real SSR ring window before fixture ingestion.
  const firstCursor = String(data.logs.rows?.[0]?.cursor || data.logs.rows?.[0]?.id || '');
  const separator = firstCursor.lastIndexOf(':');
  const boot = data.logs.boot_id || (separator >= 0 ? firstCursor.slice(0, separator) : data.controller_boot_id);
  data.controller_boot_id = boot;
  data.snapshot_revision = boot + ':snapshot:1';
  data.logs.boot_id = boot;
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  failOnPageError(page);
  await page.route('**/api/state', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data) }));
  await page.route('**/api/history*', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data.history) }));
  await page.route('**/api/logs*', readLogs);
  await page.goto(url);
  await pinnedFonts(page);
  await until(async () => await byId(page, 'pile-card').first().getAttribute('data-pile-name') === data.cards[0].name, 'log fixture state');
  return { context, page };
}

async function logFilterRace(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  const data = fixtures(original, 2);
  const excerpt = Array.from({ length: 100 }, (_, index) => logRow(index + 4901, index + 4901 === 4950 ? 'needle newer retained in the state excerpt' : 'Recent Info ' + index, 'info', 'filter'));
  data.logs = { rows: excerpt, next_cursor: 'filter:5000', capture_level: 'debug', coverage_gap: false };
  const held = deferred();
  const oldRequested = deferred();
  const filteredRequests = [];
  let holdOld = false;
  let seeded = false;
  const needle = logRow(42, 'needle retained beyond the 100-row state excerpt', 'debug', 'filter');
  const newerNeedle = excerpt.find(row => row.cursor === 'filter:4950');
  const { context, page } = await logFixturePage(browser, data, async route => {
    const query = new URL(route.request().url()).searchParams;
    const request = { after: query.get('after') || '', search: query.get('search') || '', level: query.get('level') || '' };
    if (holdOld && request.search === '' && request.level === 'info') {
      holdOld = false;
      oldRequested.resolve();
      await held.promise;
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
        rows: [logRow(9000, 'Delayed old-query row', 'info', 'filter')],
        next_cursor: 'filter:9000', capture_level: 'debug', coverage_gap: false
      }) });
      return;
    }
    if (request.search === 'needle' && request.level === 'debug') {
      filteredRequests.push(request);
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
        rows: request.after === '' ? [needle, newerNeedle] : [], next_cursor: 'filter:10000', capture_level: 'debug', coverage_gap: false
      }) });
      return;
    }
    seeded = true;
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data.logs) });
  });
  await until(async () => seeded && await page.locator('#log-level option[value="debug"]').evaluate(option => !option.disabled), 'full Info query and Debug capture');
  holdOld = true;
  await oldRequested.promise;
  await page.locator('#log-level').selectOption('debug');
  await page.locator('#log-search').fill('needle');
  held.resolve();
  await until(async () => filteredRequests.length >= 1, 'new filtered full query after old reply');
  assert.equal(filteredRequests[0].after, '', 'changing filters must fetch full retained coverage, ignoring the delayed old-query cursor and state excerpt cursor');
  await until(async () => await page.locator('#logs-tail summary').filter({ hasText: needle.message }).count() === 1, 'older matching retained record fetched');
  await until(async () => filteredRequests.length >= 2, 'next filtered cursor read');
  assert.equal(filteredRequests[1].after, 'filter:10000', 'state excerpts must not rewind the full filtered-query cursor');
  assert.deepEqual(await page.locator('#logs-tail .log-row:visible summary').allInnerTexts(), [needle.message, newerNeedle.message],
    'the complete filtered API response supplies both retained matches in server order');
  await screenshot(page, 'log-filter-race');
  await context.close();
  await liveLogFeedOwnsRowsMetadataAndCursor(browser);
}

async function liveLogFeedOwnsRowsMetadataAndCursor(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  const initial = [logRow(1, 'API first record', 'info', 'live'), logRow(2, 'API second record', 'info', 'live')];
  const metadata = { boot_id: 'live', next_cursor: 'live:2', retained_count: 2, total_count: 2,
    dropped_count: 0, oldest_at: initial[0].at, newest_at: initial[1].at, capture_level: 'debug', coverage_gap: false };
  data.logs = { ...metadata, rows: initial };
  const requests = [];
  const requestedFilter = deferred(), releaseFilter = deferred();
  const filtered = [
    { id: 'server-first', at: '2026-11-01T06:30:09Z', level: 'info', message: 'Authoritative first result' },
    { id: 'server-second', at: '2026-11-01T06:30:01Z', level: 'info', message: 'Authoritative second result' }
  ];
  const recovered = { id: 'server-recovered', at: '2026-11-01T06:30:02Z', level: 'info', message: 'Authoritative recovered result' };
  let holdFilter = true;
  let recover = false;
  const { context, page } = await logFixturePage(browser, data, async route => {
    const query = new URL(route.request().url()).searchParams;
    const after = query.get('after') || '', search = query.get('search') || '';
    requests.push({ after, search });
    if (search === 'server-alias' && holdFilter) {
      holdFilter = false;
      requestedFilter.resolve();
      await releaseFilter.promise;
    }
    if (recover && search === 'server-alias') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ...metadata,
        rows: after === 'live:3' ? [] : [recovered], next_cursor: 'live:3', retained_count: 1,
        total_count: 3, dropped_count: 2, coverage_gap: after !== 'live:3' }) });
    }
    const rows = after ? [] : search === 'server-alias' ? filtered : initial;
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ...metadata, rows }) });
  });
  await until(async () => JSON.stringify((await visibleLogRecords(page)).map(row => row.message)) === JSON.stringify(initial.map(row => row.message)),
    'API full response seeds the live view');
  await until(async () => requests.some(request => request.after === 'live:2'), 'normal polling uses the accepted API cursor');
  data.cluster_id = 'Later state projection received';
  data.logs = { ...metadata, rows: [logRow(999, 'State excerpt must never enter live logs', 'info', 'live')],
    next_cursor: 'live:999', retained_count: 999, total_count: 999, capture_level: 'info', coverage_gap: true };
  await until(async () => await page.locator('#cluster-id').textContent() === data.cluster_id, 'later state projection applied');
  assert.deepEqual((await visibleLogRecords(page)).map(row => row.message), initial.map(row => row.message),
    'later state excerpts cannot inject live records');
  assert.match(await page.locator('#log-retention').textContent(), /Retained 2 · dropped 0 · total 2/,
    'later state excerpts cannot overwrite API retention metadata');
  assert.match(await page.locator('#capture-level').textContent(), /Capture: debug/);
  assert.equal(await page.locator('#log-coverage').textContent(), '', 'later state excerpts cannot inject API coverage loss');
  const requestCount = requests.length;
  await until(async () => requests.length > requestCount, 'another serialized API delta read');
  assert.equal(requests.at(-1).after, 'live:2', 'later state excerpts cannot change the accepted API cursor');

  const first = page.locator('#logs-tail .log-row').filter({ has: page.locator('summary').filter({ hasText: /^API first record$/ }) });
  await first.evaluate(element => {
    window.__feedPinned = element;
    const range = document.createRange();
    range.selectNodeContents(element.querySelector('summary'));
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
  });
  // Playwright fill selects/focuses the input and may replace the document
  // selection. Dispatch this query change while the log range is still pinned.
  const pinnedAtFilter = await page.locator('#log-search').evaluate(input => {
    const selection = window.getSelection();
    const pinned = selection && !selection.isCollapsed && selection.rangeCount > 0 &&
      selection.getRangeAt(0).intersectsNode(window.__feedPinned);
    if (!pinned) return false;
    input.value = 'server-alias';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    return true;
  });
  assert.equal(pinnedAtFilter, true, 'a noncollapsed log selection exists immediately before the filter event');
  await requestedFilter.promise;
  assert.equal(await page.locator('#logs-tail .log-row:visible').count(), 0, 'previous-query rows hide before the server answers');
  assert.equal(await page.evaluate(() => window.__feedPinned.isConnected), true, 'filtering preserves the pinned DOM node');
  assert.equal(requests.at(-1).after, '', 'a changed query requests authoritative full coverage');
  releaseFilter.resolve();
  await until(async () => (await visibleLogRecords(page)).length === 2, 'authoritative server filtered results displayed');
  assert.deepEqual((await visibleLogRecords(page)).map(row => row.id), filtered.map(row => row.id),
    'server filter eligibility and supplied order remain authoritative despite message aliases and backward timestamps');
  assert.equal(await page.evaluate(() => window.__feedPinned.isConnected && window.__feedPinned.hidden), true,
    'a pinned unmatched prior-query node stays hidden and unchanged');
  await page.evaluate(() => window.getSelection().removeAllRanges());
  await until(async () => await page.evaluate(() => !window.__feedPinned.isConnected), 'released prior-query node is removed on reconciliation');
  const currentPin = page.locator('#logs-tail .log-row').filter({
    has: page.locator('summary').filter({ hasText: /^Authoritative first result$/ })
  });
  await currentPin.evaluate(element => {
    window.__recoveryPinned = element;
    const range = document.createRange();
    range.selectNodeContents(element.querySelector('summary'));
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
  });
  recover = true;
  await until(async () => (await visibleLogRecords(page)).some(row => row.id === recovered.id), 'expired cursor returns full retained filtered coverage');
  assert.deepEqual((await visibleLogRecords(page)).map(row => row.id), [filtered[0].id, recovered.id],
    'coverage_gap full response removes absent unpinned rows while preserving the pinned exception');
  assert.equal(await page.evaluate(() => window.__recoveryPinned.isConnected && !window.__recoveryPinned.hidden), true,
    'same-query recovery preserves the selected node without hiding it');
  await context.close();
}

async function logSelectionRetention(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  const data = fixtures(original, 2);
  // State is only a diagnostic excerpt; this case isolates full-ring ingestion.
  data.logs = { rows: Array.from({ length: 100 }, (_, index) => logRow(index + 4901, 'Retention record ' + (index + 4901), 'info', 'retention')),
    next_cursor: 'retention:5000', capture_level: 'info', coverage_gap: false };
  let highWater = 5000;
  let seededFull = false;
  let holdNextRead = false;
  const pausedRequest = deferred();
  const releasePaused = deferred();
  const requests = [];
  const { context, page } = await logFixturePage(browser, data, async route => {
    const query = new URL(route.request().url()).searchParams;
    const cursor = query.get('after') || '';
    const parsed = /^retention:(\d+)$/.exec(cursor);
    const after = seededFull && parsed ? Number(parsed[1]) : 0;
    seededFull = true;
    requests.push({ after, highWater });
    if (holdNextRead) {
      holdNextRead = false;
      pausedRequest.resolve();
      await releasePaused.promise;
    }
    const first = Math.max(after + 1, highWater - 4999);
    const rows = Array.from({ length: Math.max(0, highWater - first + 1) }, (_, index) => {
      const sequence = first + index;
      return logRow(sequence, sequence === 1 ? 'selection-anchor' : 'Retention record ' + sequence, 'info', 'retention');
    });
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
      rows, next_cursor: 'retention:' + highWater, capture_level: 'info', coverage_gap: false
    }) });
  });
  const tail = byId(page, 'logs-tail');
  await until(async () => await tail.locator('.log-row').count() === 5000 &&
    await tail.locator('summary').filter({ hasText: /^selection-anchor$/ }).count() === 1, '5000 retained rows seeded');
  await page.locator('#log-follow').uncheck();
  const selectContents = async locator => locator.evaluate(element => {
    const selection = window.getSelection();
    const range = document.createRange();
    range.selectNodeContents(element);
    selection.removeAllRanges();
    selection.addRange(range);
    return selection.toString();
  });
  const anchor = tail.locator('summary').filter({ hasText: /^selection-anchor$/ });
  const selected = await selectContents(anchor);
  highWater = 6000;
  await until(async () => await tail.locator('summary').filter({ hasText: /^Retention record 6000$/ }).count() === 1, 'first appended batch received');
  assert.equal(await page.evaluate(() => window.getSelection().toString()), selected, 'selected log text survives eviction');
  assert.ok(await tail.locator('.log-row').count() <= 5000, 'log retention remains bounded with a selected log row');
  assert.equal(await tail.locator('summary').filter({ hasText: /^Retention record 5000$/ }).count(), 1,
    'newer excerpt rows cannot be evicted ahead of older backfilled rows');
  assert.match(await page.locator('#log-coverage').innerText(), /coverage gap|paused/i, 'local pruning or paused ingestion is explicit');

  const externalSelection = await selectContents(page.locator('h1'));
  highWater = 7000;
  await until(async () => await tail.locator('summary').filter({ hasText: /^Retention record 7000$/ }).count() === 1, 'second appended batch received');
  assert.equal(await page.evaluate(() => window.getSelection().toString()), externalSelection, 'selection outside logs is preserved');
  assert.ok(await tail.locator('.log-row').count() <= 5000, 'selection outside the log tail cannot disable retention');
  const headerJumpRead = page.waitForResponse(response => response.url().includes('/api/logs?') && new URL(response.url()).searchParams.get('after') === '');
  await page.locator('#jump-latest').evaluate(button => button.click());
  await (await headerJumpRead).finished();
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  assert.equal(await page.evaluate(() => window.getSelection().toString()), externalSelection, 'Jump does not clear a wholly external selection');

  const allSelected = await selectContents(tail);
  assert.ok(allSelected.length > 100000, 'full retained tail is selected for the pause case');
  highWater = 8000;
  await until(async () => /paused/i.test(await page.locator('#log-coverage').innerText()), 'explicit all-selected ingestion pause');
  assert.ok(await tail.locator('.log-row').count() <= 5000, 'all-selected tail remains bounded');
  assert.ok(await page.evaluate(() => window.getSelection().toString()) === allSelected, 'all selected log text survives the ingestion pause');
  // Hold the next full-query reply so a duplicate-only state excerpt cannot
  // have its accidental banner reset hidden by another full-query callback.
  const duplicateState = page.waitForResponse(response => response.url().endsWith('/api/state'));
  holdNextRead = true;
  await pausedRequest.promise;
  await (await duplicateState).finished();
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  assert.match(await page.locator('#log-coverage').innerText(), /paused/i, 'duplicate state excerpt cannot erase the local pause warning');
  assert.ok(await page.evaluate(() => window.getSelection().toString()) === allSelected, 'duplicate state excerpt preserves the selected tail');
  const fullReadsBeforeResume = requests.filter(request => request.highWater === 8000 && request.after === 0).length;
  await page.locator('#jump-latest').evaluate(button => button.click());
  assert.equal(await page.evaluate(() => window.getSelection().toString()), '', 'Jump explicitly releases a tail selection to resume ingestion');
  releasePaused.resolve();
  await until(async () => requests.filter(request => request.highWater === 8000 && request.after === 0).length > fullReadsBeforeResume, 'Jump resets full retained-log cursor');
  await until(async () => await tail.locator('summary').filter({ hasText: /^Retention record 8000$/ }).count() === 1, 'Jump resumes retained cursor read');
  assert.ok(await tail.locator('.log-row').count() <= 5000, 'resumed ingestion remains bounded');
  await screenshot(page, 'log-selection-retention');
  await context.close();
}

async function visibleLogRecords(page) {
  return page.locator('#logs-tail .log-row:visible').evaluateAll(elements =>
    elements.map(element => JSON.parse(element.querySelector('pre').textContent)));
}

async function logPredicates(browser) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  const all = await (await page.request.get(url + '/api/logs?level=info')).json();
  assert.ok(all.rows.some(row => row.message.toLowerCase().includes('cursor')), 'real pile name appears in a retained structured message');
  assert.ok(all.rows.some(row => !row.message.toLowerCase().includes('cursor')), 'real retained ring also contains unrelated messages');
  await page.goto(url);
  await pinnedFonts(page);
  const filteredReply = page.waitForResponse(response => {
    const target = new URL(response.url());
    return target.pathname === '/api/logs' && target.searchParams.get('pile') === 'cursor' &&
      !target.searchParams.get('after') && !target.searchParams.get('search');
  });
  await page.locator('#log-pile').fill('cursor');
  const expected = await (await filteredReply).json();
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  const displayedIds = (await visibleLogRecords(page)).map(row => row.cursor);
  assert.deepEqual(displayedIds, expected.rows.map(row => row.cursor), 'pile display matches server message-only predicate, excluding outer cursor metadata');
  const exported = await (await page.request.get(url + '/api/logs/export?level=info&pile=cursor')).text();
  const exportedIds = exported.trim().split('\n').map(line => JSON.parse(line)).filter(row => row.cursor).map(row => row.cursor);
  assert.deepEqual(displayedIds, exportedIds, 'pile display and filtered retained export expose the same record ids');

  await page.locator('#log-pile').fill('');
  const compactSearch = '"level":"info"';
  const searchReply = page.waitForResponse(response => {
    const target = new URL(response.url());
    return target.pathname === '/api/logs' && target.searchParams.get('search') === compactSearch && !target.searchParams.get('after');
  });
  await page.locator('#log-search').fill(compactSearch);
  const compactExpected = await (await searchReply).json();
  assert.ok(compactExpected.rows.length > 0, 'compact outer JSON search has positive server matches');
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  assert.deepEqual((await visibleLogRecords(page)).map(row => row.cursor), compactExpected.rows.map(row => row.cursor),
    'browser general search matches the server compact JSON serialization');
  await screenshot(page, 'log-predicates');
  await context.close();

  // Opaque UTF-8 diagnostic messages are legitimate API records. The server's
  // TString::to_lower folds ASCII bytes only, including for the pile predicate.
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  const rows = [logRow(1, 'ПИЛА ready', 'info', 'utf8'), logRow(2, 'ASCII-FOLD-PROBE READY', 'info', 'utf8')];
  data.logs = { rows, next_cursor: 'utf8:2', capture_level: 'info', coverage_gap: false };
  const asciiFold = value => value.replace(/[A-Z]/g, character => String.fromCharCode(character.charCodeAt(0) + 32));
  const fixture = await logFixturePage(browser, data, route => {
    const query = new URL(route.request().url()).searchParams;
    const pile = asciiFold(query.get('pile') || '');
    const search = asciiFold(query.get('search') || '');
    const matching = rows.filter(row => (!pile || asciiFold(row.message).includes(pile)) &&
      (!search || asciiFold(JSON.stringify(row)).includes(search)));
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
      rows: matching, next_cursor: 'utf8:2', capture_level: 'info', coverage_gap: false
    }) });
  });
  const utf8 = fixture.page;
  async function assertAsciiFilter(field, value, ids) {
    const reply = utf8.waitForResponse(response => {
      const target = new URL(response.url());
      return target.pathname === '/api/logs' && target.searchParams.get(field) === value && !target.searchParams.get('after');
    });
    await utf8.locator(field === 'pile' ? '#log-pile' : '#log-search').fill(value);
    const expectedRows = (await (await reply).json()).rows;
    assert.deepEqual(expectedRows.map(row => row.cursor), ids, 'fixture follows server ASCII-only folding');
    await utf8.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    assert.deepEqual((await visibleLogRecords(utf8)).map(row => row.cursor), ids,
      field + ' filter preserves exact UTF-8 case while folding ASCII only');
  }
  await assertAsciiFilter('pile', 'ascii-fold-probe', ['utf8:2']);
  await assertAsciiFilter('pile', 'ПИЛА', ['utf8:1']);
  await assertAsciiFilter('pile', 'пила', []);
  await utf8.locator('#log-pile').fill('');
  await assertAsciiFilter('search', 'ASCII-FOLD-PROBE', ['utf8:2']);
  await assertAsciiFilter('search', 'ПИЛА', ['utf8:1']);
  await assertAsciiFilter('search', 'пила', []);
  await screenshot(utf8, 'log-ascii-fold');
  await fixture.context.close();
  await retainedServerWindow(browser);
}

async function retainedServerWindow(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  for (const pinned of [false, true]) {
    const boot = pinned ? 'selected-window' : 'unselected-window';
    const old = logRow(1, 'window-old-info', 'info', boot);
    const other = logRow(2, 'window-other-info', 'info', boot);
    const initial = { boot_id: boot, retained_count: 2, total_count: 2, dropped_count: 0,
      next_cursor: boot + ':2', oldest_at: old.at, newest_at: other.at,
      rows: [old, other], capture_level: 'debug', coverage_gap: false };
    const advanced = { boot_id: boot, retained_count: 5000, total_count: 8000, dropped_count: 3000,
      next_cursor: boot + ':8000', oldest_at: logRow(3001).at, newest_at: logRow(8000).at,
      rows: [], capture_level: 'debug', coverage_gap: true };
    const data = fixtures(original, 2);
    data.controller_boot_id = boot;
    data.logs = initial;
    let current = initial;
    const { context, page } = await logFixturePage(browser, data, route => route.fulfill({
      status: 200, contentType: 'application/json', body: JSON.stringify(current)
    }));
    const oldRow = page.locator('#logs-tail summary').filter({ hasText: /^window-old-info$/ });
    const otherRow = page.locator('#logs-tail summary').filter({ hasText: /^window-other-info$/ });
    await until(async () => await oldRow.count() === 1 && await otherRow.count() === 1, 'older Info records cached');
    let selected = '';
    if (pinned) {
      selected = await oldRow.evaluate(element => {
        const range = document.createRange();
        range.selectNodeContents(element);
        const selection = window.getSelection();
        selection.removeAllRanges();
        selection.addRange(range);
        return selection.toString();
      });
    }
    // All 5000 newly retained records are Debug. The default Info+ reply is
    // empty, while its global metadata proves both cached Info rows expired.
    current = advanced;
    data.logs = advanced;
    await until(async () => /total\s+8[,.\s]*000/.test(await page.locator('#log-retention').innerText()), 'global retained window advanced despite empty Info response');
    assert.equal(await otherRow.count(), 0, 'unselected cached Info rows outside the server retained window disappear');
    if (pinned) {
      assert.equal(await oldRow.count(), 1, 'selected expired row may remain as an explicit local exception');
      assert.equal(await page.evaluate(() => window.getSelection().toString()), selected, 'selected expired text is preserved');
      assert.match(await page.locator('#log-coverage').innerText(), /Browser coverage gap|selected.*outside|outside.*retained/i,
        'selected server-evicted row has an explicit local coverage warning');
      await page.evaluate(() => window.getSelection().removeAllRanges());
      await until(async () => await oldRow.count() === 0, 'released selected row is pruned to the current server window');
    } else {
      assert.equal(await oldRow.count(), 0, 'ordinary display cannot outlive the server ring behind an Info filter');
    }
    await screenshot(page, pinned ? 'selected-server-window' : 'server-window');
    await context.close();
  }
}

async function defaultInfoDebug(browser) {
  const api = await (await fetch(url + '/api/logs?level=debug')).json();
  assert.equal(api.capture_level, 'debug');
  assert.ok(api.rows.some(row => row.level === 'debug'), 'Debug records are genuinely captured before the page opens');
  const ssrContext = await browser.newContext({ javaScriptEnabled: false, viewport: { width: 1280, height: 800 } });
  const ssr = await ssrContext.newPage();
  await ssr.goto(url);
  assert.equal(await ssr.locator('#log-level').inputValue(), 'info');
  const ssrLevels = (await visibleLogRecords(ssr)).map(row => row.level);
  assert.ok(ssrLevels.length > 0, 'server HTML contains retained Info+ records');
  assert.ok(ssrLevels.every(level => !['debug', 'trace'].includes(level)), 'default server-rendered Info+ view excludes captured Debug/Trace');
  await screenshot(ssr, 'debug-capture-info-ssr');
  await ssrContext.close();

  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  await page.goto(url);
  await pinnedFonts(page);
  assert.equal(await page.locator('#log-level').inputValue(), 'info');
  assert.ok((await visibleLogRecords(page)).every(row => !['debug', 'trace'].includes(row.level)),
    'hydration preserves the acknowledged Info+ filter for existing rows');
  assert.equal(await page.locator('#log-level option[value="debug"]').evaluate(option => option.disabled), false,
    'captured Debug remains available for an explicit filter');
  await page.locator('#log-level').selectOption('debug');
  await until(async () => (await visibleLogRecords(page)).some(row => row.level === 'debug'), 'explicit Debug filter reveals captured records');
  await page.locator('#log-level').selectOption('info');
  await until(async () => (await visibleLogRecords(page)).every(row => !['debug', 'trace'].includes(row.level)), 'Info filter hides previously loaded Debug rows');
  const retained = await (await page.request.get(url + '/api/logs?level=debug')).json();
  assert.ok(retained.rows.some(row => row.level === 'debug'), 'display filtering never deletes retained Debug records');
  await screenshot(page, 'debug-capture-info-hydrated');
  await context.close();
}

async function canonicalPresentation(browser) {
  // Replay one real server response so no-JS and hydration observe exactly the
  // same pending request, evidence ages, and diagnostic excerpt.
  const response = await fetch(url);
  assert.equal(response.status, 200);
  const html = await response.text();
  const initial = html.match(/<script type="application\/json" id="initial-state">([\s\S]*?)<\/script>/);
  assert.ok(initial, 'real HTML supplies its canonical presentation');
  const snapshot = JSON.parse(initial[1]);
  assert.ok(snapshot.pending_mutation && snapshot.last_mutation, 'real ambiguous request is represented');
  assert.equal(typeof snapshot.mode_text, 'string');
  assert.equal(typeof snapshot.decision_text, 'string');
  assert.deepEqual(snapshot.operation_details, {
    pending: snapshot.pending_mutation, last_attempt: snapshot.last_mutation, next_retry_at: snapshot.retry_at
  });
  const expected = {
    mode: snapshot.mode_text, decision: snapshot.decision_text, operation: snapshot.operation_details,
    cards: snapshot.cards.map(card => ({
      name: card.name, health: card.health_text, evidence: card.evidence_age_text, action: card.context_action
    }))
  };
  const documents = [];
  for (const javaScriptEnabled of [false, true]) {
    const context = await browser.newContext({ javaScriptEnabled, viewport: { width: 1280, height: 800 } });
    // Freeze elapsed time only; the initial document and API payload are real.
    await context.addInitScript(() => Object.defineProperty(performance, 'now', { value: () => 0 }));
    const page = await context.newPage();
    await page.route(url + '/', route => route.fulfill({ status: 200, contentType: 'text/html', body: html }));
    await page.route('**/api/state', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(snapshot) }));
    await page.route('**/api/logs*', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(snapshot.logs) }));
    await page.goto(url);
    const actual = {
      mode: await page.locator('#effective-mode').textContent(),
      decision: await page.locator('#decision-title').textContent(),
      operation: JSON.parse(await page.locator('#operation-json').textContent()),
      cards: await byId(page, 'pile-card').evaluateAll(cards => cards.map(card => ({
        name: card.dataset.pileName, health: card.querySelector('.health').textContent,
        evidence: card.querySelector('.evidence-age').textContent,
        action: card.querySelector('.context-action button')?.dataset.action || null
      })))
    };
    assert.deepEqual(actual, expected, (javaScriptEnabled ? 'hydrated' : 'no-JS') + ' document consumes the published presentation');
    const rows = await visibleLogRecords(page);
    assert.ok(rows.length, 'real diagnostic excerpt has records');
    assert.ok(rows.every(row => typeof row.summary === 'string'), 'real diagnostic records publish their labels');
    assert.deepEqual(await page.locator('#logs-tail .log-row:visible summary').allTextContents(), rows.map(row => row.summary));
    documents.push(actual);
    await context.close();
  }
  assert.deepEqual(documents[0], documents[1], 'hydration preserves the server-rendered document');
}

async function canonicalLabels(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  data.mode_text = 'Observe · canonical mode explanation';
  data.decision_text = 'Actions blocked: canonical decision explanation';
  data.cards[1].health_text = 'Healthy · canonical assessment explanation';
  data.logs.rows = [{ ...logRow(1, JSON.stringify({ component: 'raw-component', event: 'raw-event', message: 'raw diagnostic detail' }), 'info', 'fixture'),
    summary: 'Canonical diagnostic summary' }];
  data.logs.next_cursor = 'fixture:1';
  const { context, page } = await fixturePage(browser, data);
  assert.equal(await page.locator('#effective-mode').textContent(), data.mode_text);
  assert.equal(await page.locator('#decision-title').textContent(), data.decision_text);
  assert.equal(await byId(page, 'pile-card').nth(1).locator('.health').textContent(), data.cards[1].health_text);
  await until(async () => await page.locator('#logs-tail summary').filter({ hasText: /^Canonical diagnostic summary$/ }).count() === 1,
    'diagnostic summary is rendered verbatim instead of reparsing the raw message');
  const canonicalRow = page.locator('#logs-tail .log-row').filter({
    has: page.locator('summary').filter({ hasText: /^Canonical diagnostic summary$/ })
  });
  const detail = JSON.parse(await canonicalRow.locator('pre').textContent());
  assert.equal(detail.message, data.logs.rows[0].message,
    'expanded log detail retains the original structured message');
  await context.close();
  await canonicalHistoryCoverage(browser);
  await compactDrawerReferences(browser);
}

async function compactDrawerReferences(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  const data = fixtures(original, 2);
  const card = data.cards[0];
  const name = 'pile "quoted" <&>';
  card.name = card.display_name = name;
  card.verdict = 'UNKNOWN';
  card.assessment = 'unavailable';
  card.health_text = 'Assessment unavailable';
  card.reasons = ['Current BSC warmup'];
  card.confirmed = false;
  card.last_known_verdict = 'FAILED';
  card.current_assessment = { ...card.current_assessment, verdict: 'UNKNOWN', reasons: card.reasons,
    assessment: 'unavailable', confirmed: false, evidence: { ...card.current_assessment.evidence,
      age_ms: null, sample_at: null, expires_at: null, expired: false, expired_for_ms: null } };
  card.last_known_assessment = { ...card.last_known_assessment, verdict: 'FAILED', reasons: ['Historical storage failure'] };
  const connectivity = reporter => ({ vantage_pile: name, reporter_node_id: reporter, reporter_is_root: true,
    generation: data.view.generation, age_ms: 3000, evidence: card.last_known_assessment.evidence,
    static_nodes: 3, connected_static_nodes: 2, has_node_quorum: true, in_config_quorum: true,
    in_data_quorum: true, binding_in_progress: false });
  const current = { name, ...card.current_assessment, connectivity: [connectivity(111)] };
  const retained = { name, ...card.last_known_assessment, connectivity: [connectivity(222)] };
  data.piles[0] = current;
  data.last_known_piles = [retained];
  const endpoint = { ...original.endpoints[0], uri: 'grpc://quoted-"<&>:2135', pile: name,
    identity: 'VERIFIED', ignored: true, last_error: 'Exact endpoint error', selfcheck_mode: 'FALLBACK',
    selfcheck_age_ms: 3000, truncated: true, notes: ['Exact SelfCheck diagnostic note'] };
  data.endpoints = [endpoint, { ...endpoint, uri: 'grpc://unrelated:2135', pile: 'unrelated', notes: ['Unrelated endpoint note'] }];
  card.endpoint_uris = [endpoint.uri];
  data.view.piles[0].name = name;
  const { context, page } = await fixturePage(browser, data);
  await byId(page, 'pile-card').first().locator('[data-details]').click();
  const drawer = byId(page, 'evidence-drawer');
  await drawer.waitFor({ state: 'visible' });
  const text = await drawer.innerText();
  for (const expected of ['Current BSC warmup', 'Historical storage failure', endpoint.uri, 'Exact endpoint error',
    'Exact SelfCheck diagnostic note', 'FALLBACK', 'truncated', 'Reporter 111', 'Reporter 222']) {
    assert.ok(text.includes(expected), 'drawer resolves diagnostic source: ' + expected);
  }
  assert.ok(!text.includes('Unrelated endpoint note'), 'exact URI membership excludes unrelated endpoints');
  const raw = JSON.parse(await page.locator('#evidence-content details pre').textContent());
  assert.deepEqual(raw, { card, current_pile: current, last_known_pile: retained, endpoints: [endpoint] },
    'raw drawer preserves all normalized current/historical/endpoint sources');
  await context.close();
}

async function canonicalHistoryCoverage(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  assert.equal(original.history.dropped_count, 0, 'fresh real history has no pruning');
  assert.equal(original.logs.dropped_count, 0, 'fresh real logs have no pruning');
  assert.equal(original.history.coverage_gap, false, 'a fresh initial history read is not a coverage loss');
  assert.equal(original.logs.coverage_gap, false, 'a fresh initial log read is not a coverage loss');
  const live = await startPage(browser);
  assert.equal(await live.page.locator('#history-coverage').textContent(), '');
  assert.equal(await live.page.locator('#log-coverage').textContent(), '');
  await live.context.close();

  const data = fixtures(original, 2);
  const gapText = 'Generations 43–44 not observed.';
  const rows = [
    { cursor: 'fixture:2', at: '2026-11-01T06:30:00Z', type: 'view', generation: '45', origin: 'external',
      message: 'Observed pile state transition', coverage_gap: true, coverage_gap_text: gapText,
      summary: 'Observed pile state transition · ' + gapText, origin_label: 'External/unknown' },
    { cursor: 'fixture:3', at: '2026-11-01T06:30:01Z', type: 'mode', mode: 'manual', origin: 'operator',
      summary: 'Mode manual', origin_label: 'Operator' },
    { cursor: 'fixture:4', at: '2026-11-01T06:30:02Z', type: 'decision', reason: 'CONFIRMED_DOWN: pile-01', origin: 'skipper',
      summary: 'CONFIRMED_DOWN: pile-01', origin_label: 'Skipper' }
  ];
  data.history = { boot_id: 'fixture', rows, next_cursor: 'fixture:4', coverage_gap: false, tail_clipped: true,
    retained_count: 4, dropped_count: 0, total_count: 4 };
  data.logs = { boot_id: 'fixture', rows: Array.from({ length: 100 }, (_, index) => logRow(index + 2, 'Clipped tail record ' + (index + 2), 'info', 'fixture')),
    next_cursor: 'fixture:101', coverage_gap: false, tail_clipped: true,
    retained_count: 101, dropped_count: 0, total_count: 101, oldest_at: logRow(1).at, newest_at: logRow(101).at, capture_level: 'info' };
  const { context, page } = await fixturePage(browser, data);
  const expected = rows.map(row => ({ summary: row.summary, origin: row.origin_label }));
  const displayed = () => page.locator('#history-rows tr').evaluateAll(elements => elements.map(row => ({
    summary: row.cells[2].textContent, origin: row.cells[3].textContent
  })));
  assert.deepEqual(await displayed(), expected, 'collapsed history consumes gap summaries and origin labels');
  assert.equal(await page.locator('#history-coverage').textContent(), '', 'a clipped tail alone has no history gap banner');
  assert.equal(await page.locator('#log-coverage').textContent(), '', 'a clipped tail alone has no log gap banner');

  data.history.rows = [{ cursor: 'fixture:1', at: '2026-11-01T06:29:00Z', type: 'view', generation: '42',
    origin: 'baseline', summary: 'Observation baseline', origin_label: 'Baseline' }, ...rows];
  data.history.tail_clipped = false;
  await page.locator('#history-expand').click();
  const expanded = data.history.rows.map(row => ({ summary: row.summary, origin: row.origin_label }));
  await until(async () => JSON.stringify(await displayed()) === JSON.stringify(expanded), 'expanded history preserves gap summaries and origins');
  data.history.coverage_gap = true;
  data.logs.coverage_gap = true;
  await until(async () => /coverage gap/i.test(await page.locator('#history-coverage').textContent()) &&
    /coverage gap/i.test(await page.locator('#log-coverage').textContent()), 'real cursor loss remains explicit');
  await context.close();
}

async function disabledFailover(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  const data = fixtures(original, 2);
  data.mode = 'manual';
  data.mode_text = 'Manual · one reviewed request per approval.';
  data.controller_ready = true;
  data.decision = { kind: 'READY', reason: 'CONFIRMED_DOWN: pile-01', blocker: null,
    plan: { expected_generation: data.view.generation,
      update: { 'pile-01': 'DISCONNECTED', 'pile-02': 'PRIMARY' }, endpoint: 'grpc://fixture:2135' } };
  data.rejected_generation = data.view.generation;
  data.decision_text = `Refused by server at generation ${data.view.generation}; no sends at this generation until a newer generation or restart.`;
  data.dispatch_refusal = data.decision_text;
  data.auto_activation_text = 'Auto dispatches policy-approved failover and guarded retries. ' + data.dispatch_refusal;
  data.failover_action = { label: 'Review failover', available: false, reason: 'REJECTED_AT_GENERATION' };
  data.cards[0].proposed = 'Proposed disconnect';
  data.cards[1].proposed = 'Proposed new primary';
  for (const card of data.cards) {
    card.context_action = 'failover';
    card.actions.failover = { ...data.failover_action };
  }
  const { context, page } = await fixturePage(browser, data, { viewport: { width: 1280, height: 800 } });
  const button = page.locator('#review-failover');
  assert.equal(await button.isDisabled(), true, 'incident failover button follows canonical eligibility');
  const reasonId = await button.getAttribute('aria-describedby');
  assert.ok(reasonId, 'incident disabled action links its accessible reason');
  assert.match(await page.locator('#' + reasonId).innerText(), /REJECTED_AT_GENERATION/);
  await byId(page, 'auto-mode').click();
  await byId(page, 'review-dialog').waitFor({ state: 'visible' });
  const activation = await page.locator('#review-body').innerText();
  assert.match(activation, /will not send|no sends/i, 'Auto activation explains the current generation refusal');
  assert.ok(activation.includes(data.view.generation), 'Auto activation identifies the refused generation');
  assert.match(activation, /newer generation|restart/i, 'Auto activation explains how the refusal can end');
  assert.doesNotMatch(activation, /may be dispatched immediately/i, 'a latched generation has no immediate dispatch promise');
  await screenshot(page, 'disabled-failover');
  await context.close();

  data.rejected_generation = String(BigInt(data.view.generation) - 1n);
  data.decision_text = 'Plan ready · awaiting operator execution.';
  data.dispatch_refusal = null;
  data.auto_activation_text = 'Auto dispatches policy-approved failover and guarded retries. A Ready plan may be dispatched immediately.';
  data.failover_action = { label: 'Review failover', available: true, reason: '' };
  for (const card of data.cards) {
    card.actions.failover = { ...data.failover_action };
  }
  const unlatched = await fixturePage(browser, data, { viewport: { width: 1280, height: 800 } });
  await byId(unlatched.page, 'auto-mode').click();
  await byId(unlatched.page, 'review-dialog').waitFor({ state: 'visible' });
  assert.match(await unlatched.page.locator('#review-body').innerText(), /may be dispatched immediately/i,
    'a refusal at an older generation does not suppress the normal Auto activation explanation');
  await unlatched.context.close();

  const html = await (await fetch(url)).text();
  for (const mode of ['manual', 'observe']) {
    const disabled = structuredClone(data);
    const rejected = mode === 'manual';
    const age = rejected ? 29500 : 30000;
    const reason = rejected ? 'REJECTED_AT_GENERATION' : 'Enable Manual mode for reviewed execution';
    disabled.mode = mode;
    disabled.mode_text = rejected ? 'Manual · one reviewed request per approval.'
      : 'Observe · collection continues; no new dispatch.';
    disabled.rejected_generation = rejected ? disabled.view.generation : null;
    disabled.dispatch_refusal = rejected
      ? `Refused by server at generation ${disabled.view.generation}; no sends at this generation until a newer generation or restart.`
      : null;
    disabled.decision.reason = rejected ? 'REJECTED_BY_SERVER' : 'CONFIRMED_DOWN: pile-01';
    disabled.decision_text = disabled.dispatch_refusal || 'Plan ready · not dispatched.';
    disabled.auto_activation_text = 'Auto dispatches policy-approved failover and guarded retries. '
      + (disabled.dispatch_refusal || 'A Ready plan may be dispatched immediately.');
    disabled.failover_action = { label: 'Review failover', available: false, reason };
    const evidence = { ...disabled.view.evidence, age_ms: age, ttl_ms: 30000,
      sample_at: new Date(Date.parse(disabled.now) - age).toISOString(),
      expires_at: new Date(Date.parse(disabled.now) - age + 30000).toISOString(),
      expired: age >= 30000, expired_for_ms: Math.max(0, age - 30000) };
    disabled.view.age_ms = age;
    disabled.view.evidence = evidence;
    disabled.last_observed_view = structuredClone(disabled.view);
    disabled.state_age_text = (rejected ? 'State: 29 s ago' : 'State stale · 30 s ago');
    for (const card of disabled.cards) {
      card.state_evidence = structuredClone(evidence);
      card.state_current = rejected;
      for (const action of Object.values(card.actions)) {
        action.available = false;
        action.reason = reason;
      }
      if (!rejected) {
        // Read-time expiry preserves the snapshot's Ready plan, but its
        // non-Manual action reason takes precedence over stale observations.
        card.assessment = 'unavailable';
        card.verdict = 'UNKNOWN';
        card.health_text = 'Assessment unavailable';
      }
    }
    const initial = html.replace(/(<script type="application\/json" id="initial-state">)[\s\S]*?(<\/script>)/,
      (_match, open, close) => open + JSON.stringify(disabled).replaceAll('<', '\\u003c') + close);
    const context = await browser.newContext();
    const page = await context.newPage();
    failOnPageError(page);
    await page.clock.install({ time: new Date('2026-11-01T06:40:30Z') });
    await page.clock.pauseAt(new Date('2026-11-01T06:40:31Z'));
    await page.route(url + '/', route => route.fulfill({ status: 200, contentType: 'text/html', body: initial }));
    await page.route('**/api/state', route => route.abort());
    await page.route('**/api/logs*', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(disabled.logs) }));
    await page.goto(url);
    const assertReason = async () => {
      const button = page.locator('#review-failover');
      assert.equal(await button.isVisible(), true, mode + ' retains its Ready plan action');
      assert.equal(await button.isDisabled(), true, mode + ' action remains canonically disabled');
      const reasonId = await button.getAttribute('aria-describedby');
      assert.ok(reasonId, mode + ' disabled action links its canonical reason');
      assert.equal(await page.locator('#' + reasonId).textContent(), reason,
        mode + ' preserves the supplied disabled reason');
      assert.equal(await page.locator('#review-failover-action').getAttribute('title'), reason);
      for (const card of disabled.cards) {
        const element = page.locator('[data-testid="pile-card"][data-pile-name="' + card.name + '"]');
        for (const group of ['.context-action', '.more-menu']) {
          const buttons = element.locator(group + ' [data-action]');
          const count = group === '.context-action' ? 1 : 4;
          assert.equal(await buttons.count(), count, mode + ' renders its ' + group + ' actions');
          const actions = await buttons.evaluateAll(elements => elements.map(button => ({
            disabled: button.disabled,
            reason: document.getElementById(button.getAttribute('aria-describedby'))?.textContent,
            title: button.parentElement.title,
            tabindex: button.parentElement.getAttribute('tabindex'),
          })));
          assert.deepEqual(actions, Array(count).fill({ disabled: true, reason, title: reason, tabindex: '0' }),
            mode + ' ' + group + ' preserves canonical disabled reasons and keyboard access');
        }
      }
    };
    await assertReason();
    // Failed polls retain the received response while the browser clock crosses
    // the rejected view's TTL; Observe starts with a view expired at read time.
    await page.clock.runFor(1000);
    await assertReason();
    await context.close();
  }
}

function deferred() {
  let resolve;
  const promise = new Promise(ready => { resolve = ready; });
  return { promise, resolve };
}

function reviewedFixturePlan(data, sequence = 1) {
  const before = Object.fromEntries(data.cards.map(card => [card.name, card.state]));
  return { plan_id: 'fixture-reviewed-' + sequence, cluster_id: data.cluster_id, mode_revision: data.mode_revision,
    reason: 'Fixture reviewed switchover', expires_at: new Date(Date.parse(data.now) + 120000).toISOString(),
    before, after: { ...before, 'pile-02': 'PROMOTED' }, alternate_synchronized_pile_names: [],
    plan: { expected_generation: data.view.generation, update: { 'pile-02': 'PROMOTED' }, endpoint: 'grpc://fixture:2135' } };
}

async function manualReviewSnapshotScope(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  data.mode = 'auto';
  data.mode_text = 'Auto · policy-approved failover enabled.';
  const { context, page } = await fixturePage(browser, data);
  const commands = [];
  await page.route('**/api/mode', route => {
    commands.push(route.request().postDataJSON());
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
  });
  await byId(page, 'manual-mode').click();
  await byId(page, 'review-dialog').waitFor({ state: 'visible' });
  data.snapshot_revision = 'fixture:snapshot:changed';
  data.cluster_id = 'Manual snapshot change received';
  await until(async () => await page.locator('#cluster-id').textContent() === data.cluster_id, 'snapshot change during Manual activation review');
  assert.equal(await byId(page, 'review-execute').isDisabled(), false, 'Manual activation depends on boot and mode revision, not health snapshot');
  await byId(page, 'review-execute').click();
  await until(async () => commands.length === 1, 'Manual activation remains executable');
  assert.equal(commands[0].mode, 'manual');
  assert.equal(commands[0].expected_boot_id, data.controller_boot_id);
  assert.equal(commands[0].expected_mode_revision, data.mode_revision);
  await context.close();
}

async function modeReviewFailure(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  for (const [mode, code, status] of [
    ['auto', 'SNAPSHOT_CHANGED', 409],
    ['auto', 'PENDING_MUTATION', 409],
    ['manual', 'STATE_WRITE_FAILED', 503],
  ]) {
    const data = fixtures(original, 2);
    data.mode = mode === 'manual' ? 'auto' : 'observe';
    data.mode_text = data.mode === 'auto' ? 'Auto · policy-approved failover enabled.'
      : 'Observe · collection continues; no new dispatch.';
    data.mode_revision = '10';
    const { context, page } = await fixturePage(browser, data);
    const commands = [];
    await page.route('**/api/mode', route => {
      commands.push(route.request().postDataJSON());
      // Matching mode commands consume their revision on refusal as well as success.
      data.mode_revision = String(BigInt(data.mode_revision) + 1n);
      if (commands.length === 1) {
        if (code === 'SNAPSHOT_CHANGED') data.snapshot_revision = 'fixture:snapshot:changed';
        return route.fulfill({ status, contentType: 'application/json',
          body: JSON.stringify({ error: code, reason: 'Fixture mode activation refused' }) });
      }
      data.mode = mode;
      return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    });
    await byId(page, mode + '-mode').click();
    const dialog = byId(page, 'review-dialog');
    await dialog.waitFor({ state: 'visible' });
    await byId(page, 'review-execute').click();
    await until(async () => commands.length === 1 &&
      (await page.locator('#command-status').textContent()).includes(code) &&
      !(await page.locator('#refresh-plan').isDisabled()), 'mode refusal reconciled');
    assert.equal(await dialog.isVisible(), true, code + ' keeps its failed review open');
    const error = page.locator('#review-status');
    assert.equal(await error.isVisible(), true, code + ' is visible inside the modal');
    assert.equal(await error.getAttribute('role'), 'status');
    assert.equal(await error.getAttribute('aria-live'), 'polite');
    assert.match(await error.textContent(), new RegExp(code));
    assert.equal(await byId(page, 'review-execute').isDisabled(), true, 'a consumed review cannot be resent');
    assert.equal(await page.locator('#refresh-plan').isVisible(), true);
    // Another authoritative poll must retain the concrete refusal rather than
    // replacing it with a generic stale-review message or retrying the POST.
    data.cluster_id = 'Poll after ' + mode + ' ' + code;
    await until(async () => await page.locator('#cluster-id').textContent() === data.cluster_id,
      'poll following failed activation');
    assert.match(await error.textContent(), new RegExp(code));
    assert.equal(commands.length, 1, 'state polling never resends a refused activation');
    await page.locator('#refresh-plan').click();
    assert.equal(await byId(page, 'review-execute').isDisabled(), false, 'explicit refresh obtains the new revision');
    assert.equal(commands.length, 1, 'refresh alone does not execute');
    await byId(page, 'review-execute').click();
    await until(async () => commands.length === 2 && !(await dialog.isVisible()), 'explicit refreshed activation acknowledged');
    assert.equal(commands[0].expected_mode_revision, '10');
    assert.equal(commands[1].expected_mode_revision, '11', 'retry uses the consumed revision successor');
    assert.equal(commands[1].reviewed_snapshot_revision, data.snapshot_revision);
    await context.close();
  }
}

async function autoReviewRefresh(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  const { context, page } = await fixturePage(browser, data);
  await byId(page, 'auto-mode').click();
  await byId(page, 'review-dialog').waitFor({ state: 'visible' });
  data.snapshot_revision = 'fixture:snapshot:changed';
  data.decision.reason = 'New evidence to review';
  data.cluster_id = 'Auto snapshot change received';
  await until(async () => await page.locator('#cluster-id').textContent() === data.cluster_id, 'snapshot change during Auto activation review');
  assert.equal(await byId(page, 'review-execute').isDisabled(), true, 'Auto activation retains the snapshot guard');
  assert.equal(await page.locator('#refresh-plan').isVisible(), true, 'stale mode review has a usable refresh path');
  await page.locator('#refresh-plan').click();
  await until(async () => !(await byId(page, 'review-execute').isDisabled()) &&
    (await page.locator('#review-body').innerText()).includes(data.decision.reason), 'mode review refresh uses the new snapshot');
  await byId(page, 'review-cancel').click();
  assert.equal(await byId(page, 'auto-mode').evaluate(element => element === document.activeElement), true,
    'refresh preserves the original mode opener for Cancel focus restoration');
  await context.close();
}

async function pollFailurePreservesHandoff(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  data.mode = 'manual';
  data.mode_text = 'Manual · one reviewed request per approval.';
  data.controller_ready = true;
  data.cards[1].actions.switchover = { label: 'Switchover here', available: true, reason: '' };
  const { context, page } = await fixturePage(browser, data);
  await page.route('**/api/plans', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(reviewedFixturePlan(data)) }));
  await page.route('**/api/operations', route => route.fulfill({ status: 202, contentType: 'application/json', body: JSON.stringify({ operation_id: 'handoff-preservation-sentinel' }) }));
  let failPoll = false;
  await page.route('**/api/state', route => route.fulfill(failPoll
    ? { status: 503, contentType: 'application/json', body: '{}' }
    : { status: 200, contentType: 'application/json', body: JSON.stringify(data) }));
  await page.locator('[data-action="switchover"][data-pile-name="pile-02"]').first().click();
  await until(async () => !(await byId(page, 'review-execute').isDisabled()), 'review prepared');
  await byId(page, 'review-execute').click();
  await until(async () => (await page.locator('#command-status').textContent()).includes('handoff-preservation-sentinel'), 'command handoff acknowledged');
  const handoff = await page.locator('#command-status').textContent();
  failPoll = true;
  await page.locator('#refresh').click();
  await until(async () => /unavailable|503/i.test(await page.locator('#controller-status').textContent()), 'failed state poll displayed');
  assert.equal(await page.locator('#command-status').textContent(), handoff, 'poll errors preserve the operation handoff status');
  assert.match(await page.locator('#controller-status').textContent(), /503/, 'controller status carries the poll failure reason');
  failPoll = false;
  await page.locator('#refresh').click();
  await until(async () => await page.locator('#controller-status').textContent() === 'Controller connected', 'successful poll clears connectivity error');
  assert.equal(await page.locator('#command-status').textContent(), handoff);
  await context.close();
}

async function forcedThemeColorScheme(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  for (const [colorScheme, selected] of [['dark', 'light'], ['light', 'dark']]) {
    const { context, page } = await fixturePage(browser, fixtures(original, 2), { colorScheme });
    await byId(page, 'theme-select').selectOption(selected);
    assert.equal(await page.locator('html').evaluate(element => getComputedStyle(element).colorScheme), selected,
      'forced theme determines native control and scrollbar color scheme despite opposite OS preference');
    await context.close();
  }
}

async function pendingOperationControls(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  data.mode = 'manual';
  data.mode_text = 'Manual · one reviewed request per approval.';
  data.cards[1].actions.switchover = { label: 'Switchover here', available: true, reason: '' };
  const { context, page } = await fixturePage(browser, data);
  let plans = 0, operations = 0;
  const handedOff = deferred(), releaseOperation = deferred(), modes = [];
  await page.route('**/api/plans', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(reviewedFixturePlan(data, ++plans)) }));
  await page.route('**/api/operations', async route => {
    ++operations;
    handedOff.resolve();
    await releaseOperation.promise;
    await route.fulfill({ status: 202, contentType: 'application/json', body: JSON.stringify({ operation_id: 'held-operation' }) });
  });
  await page.route('**/api/mode', route => {
    modes.push(route.request().postDataJSON());
    data.mode = 'observe';
    data.mode_text = 'Observe · collection continues; no new dispatch.';
    data.mode_revision = String(BigInt(data.mode_revision) + 1n);
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
  });
  await page.locator('[data-action="switchover"][data-pile-name="pile-02"]').first().click();
  await until(async () => !(await byId(page, 'review-execute').isDisabled()), 'operation review prepared');
  const reviewedBody = await page.locator('#review-body').textContent();
  await byId(page, 'review-execute').click();
  await handedOff.promise;
  await page.locator('#refresh-plan').evaluate(button => button.click());
  assert.equal(await page.locator('#review-body').textContent(), reviewedBody, 'Refresh cannot replace a review while its operation POST is pending');
  assert.equal(plans, 1);
  await page.locator('#review-off').click();
  await until(async () => modes.length === 1 && (await byId(page, 'effective-mode').textContent()).includes('Observe'),
    'Off remains responsive while the operation handoff POST is pending');
  assert.equal(modes[0].mode, 'observe');
  await page.locator('#review-off').click();
  await page.waitForTimeout(150);
  assert.equal(modes.length, 1, 'Off skips a redundant Observe command');
  assert.equal(operations, 1, 'mode controls never repeat the operation handoff');
  releaseOperation.resolve();
  await until(async () => (await page.locator('#command-status').textContent()).includes('held-operation'), 'pending operation response settles');
  assert.equal(plans, 1);
  assert.equal(operations, 1);
  await context.close();
}

async function incidentEvidenceUsesAffectedPile(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  data.cards[0].incident = false;
  data.cards[0].verdict = 'HEALTHY';
  data.cards[0].health_text = 'Healthy';
  data.cards[1].incident = true;
  data.cards[1].verdict = 'FAILED';
  data.cards[1].health_text = 'Failed · confirmed';
  for (const [index, card] of data.cards.entries()) {
    card.confirmed = index === 1;
    card.last_known_verdict = card.verdict;
    for (const assessment of [card.current_assessment, card.last_known_assessment, data.piles[index]]) {
      assessment.verdict = card.verdict;
      assessment.confirmed = card.confirmed;
    }
  }
  data.incident = { active: true, piles: ['pile-02'] };
  const { context, page } = await fixturePage(browser, data);
  await page.locator('#view-evidence').click();
  await byId(page, 'evidence-drawer').waitFor({ state: 'visible' });
  assert.equal(await page.locator('#evidence-content h2').textContent(), 'pile-02', 'incident evidence opens the affected second pile');
  await context.close();
}

async function cachedTimeFormatting(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  const rows = Array.from({ length: 200 }, (_, index) => logRow(index + 1, 'Formatter load record ' + (index + 1), 'info', 'fixture'));
  data.logs = { boot_id: 'fixture', rows, next_cursor: 'fixture:200', capture_level: 'info', coverage_gap: false,
    retained_count: 200, total_count: 200, dropped_count: 0, oldest_at: rows[0].at, newest_at: rows.at(-1).at };
  const context = await browser.newContext({ timezoneId: 'Europe/Belgrade' });
  await context.addInitScript(() => {
    window.__dateFormatConstructions = [];
    const original = Intl.DateTimeFormat;
    Intl.DateTimeFormat = new Proxy(original, {
      construct(target, args) {
        window.__dateFormatConstructions.push(args);
        return Reflect.construct(target, args);
      },
      apply(target, receiver, args) {
        window.__dateFormatConstructions.push(args);
        return Reflect.apply(target, receiver, args);
      }
    });
  });
  const page = await context.newPage();
  failOnPageError(page);
  await page.route('**/api/state', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data) }));
  await page.route('**/api/logs*', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ...data.logs,
    rows: new URL(route.request().url()).searchParams.get('after') ? [] : rows }) }));
  await page.goto(url);
  await until(async () => (await visibleLogRecords(page)).some(row => row.message === 'Formatter load record 200'), 'many retained timestamps rendered');
  const constructors = await page.evaluate(() => window.__dateFormatConstructions.length);
  assert.ok(constructors <= 8, `formatter construction is bounded per zone, independent of 200 rows; got ${constructors}`);
  assert.match(await page.locator('#logs-tail time').last().getAttribute('title'), /Europe\/Belgrade/);
  await context.close();
}

async function stableExpandedHistory(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  data.history = { boot_id: 'fixture', rows: Array.from({ length: 4 }, (_, index) => ({
    cursor: 'fixture:' + (index + 1), at: logRow(index).at, type: 'view', generation: String(index + 1),
    summary: 'Unchanged history record ' + (index + 1), origin: 'external', origin_label: 'External/unknown'
  })), next_cursor: 'fixture:4', coverage_gap: false };
  const { context, page } = await fixturePage(browser, data);
  let reads = 0;
  await page.route('**/api/history*', route => {
    ++reads;
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data.history) });
  });
  await page.locator('#history-expand').click();
  await until(async () => await page.locator('#history-rows tr').count() === 4, 'expanded history populated');
  await page.evaluate(() => { window.__historyNodes = [...document.querySelectorAll('#history-rows tr')]; });
  const initialReads = reads;
  await until(async () => reads >= initialReads + 2, 'unchanged history polled twice');
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  assert.equal(await page.evaluate(() => window.__historyNodes.every((node, index) =>
    node === document.querySelectorAll('#history-rows tr')[index] && node.isConnected)), true,
    'same boot and unchanged next_cursor preserve expanded history row nodes');
  await context.close();
}

async function freshEvidenceExpiresOnClock(browser) {
  const data = fixtures(await (await fetch(url + '/api/state')).json(), 2);
  const card = data.cards[1];
  card.evidence = { ...card.evidence, age_ms: 29500, ttl_ms: 30000, expired: false, expired_for_ms: 0 };
  card.assessment = 'available';
  card.verdict = 'HEALTHY';
  card.health_text = 'Healthy';
  card.actions.switchover = { label: 'Switchover here', available: true, reason: '' };
  const html = await (await fetch(url)).text();
  const initial = html.replace(/(<script type="application\/json" id="initial-state">)[\s\S]*?(<\/script>)/,
    (_match, open, close) => open + JSON.stringify(data).replaceAll('<', '\\u003c') + close);
  const context = await browser.newContext();
  const page = await context.newPage();
  failOnPageError(page);
  await page.clock.install({ time: new Date('2026-11-01T06:40:30Z') });
  await page.clock.pauseAt(new Date('2026-11-01T06:40:31Z'));
  await page.route(url + '/', route => route.fulfill({ status: 200, contentType: 'text/html', body: initial }));
  await page.route('**/api/state', route => route.abort());
  await page.route('**/api/logs*', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data.logs) }));
  await page.goto(url);
  const target = byId(page, 'pile-card').nth(1);
  assert.equal(await target.locator('.health').textContent(), 'Healthy');
  assert.equal(await target.locator('[data-action="switchover"]').first().isDisabled(), false);
  await page.clock.runFor(1000);
  assert.match(await target.locator('.health').textContent(), /Health unknown.*Expired just now/);
  assert.equal(await target.locator('[data-action="switchover"]').first().isDisabled(), true);
  await page.clock.fastForward(60000);
  assert.match(await target.locator('.health').textContent(), /Health unknown.*expired 1 min ago/);
  await context.close();
}

async function stage1ExclusionExpiresOnClock(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  const html = await (await fetch(url)).text();
  for (const expiring of ['state_evidence', 'participation_evidence']) {
    const data = fixtures(original, 4);
    data.incident = { active: false, piles: [] };
    const primary = data.cards[0];
    primary.verdict = primary.last_known_verdict = 'HEALTHY';
    primary.health_text = 'Healthy';
    primary.incident = primary.confirmed = false;
    primary.current_assessment.verdict = primary.last_known_assessment.verdict = 'HEALTHY';
    primary.current_assessment.confirmed = primary.last_known_assessment.confirmed = false;
    const evidence = age => ({ ...primary.evidence, age_ms: age, ttl_ms: 30000,
      expired: age >= 30000, expired_for_ms: Math.max(0, age - 30000) });
    const stateEvidence = evidence(expiring === 'state_evidence' ? 29500 : 1000);
    data.view.age_ms = stateEvidence.age_ms;
    data.view.evidence = structuredClone(stateEvidence);
    primary.state_evidence = structuredClone(stateEvidence);
    for (const [index, state] of [[1, 'NOT_SYNCHRONIZED'], [2, 'DISCONNECTED'], [3, 'INACTIVE']]) {
      const card = data.cards[index];
      card.state = state;
      card.state_current = true;
      card.participating = false;
      card.assessment = 'not_assessed';
      card.verdict = 'UNKNOWN';
      card.last_known_verdict = 'FAILED';
      card.incident = card.confirmed = false;
      card.health_text = 'Not assessed';
      card.current_assessment = null;
      card.evidence = { ...evidence(40000), source: 'controller_assessment' };
      card.last_known_assessment = { ...card.last_known_assessment, verdict: 'FAILED',
        assessment: 'expired', evidence: structuredClone(card.evidence), confirmed: false };
      card.state_evidence = structuredClone(stateEvidence);
      card.participation_evidence = { ...evidence(index === 1 && expiring !== 'participation_evidence' ? 1000 : 29500),
        source: 'controller_participation' };
      card.context_action = state === 'DISCONNECTED' ? 'rejoin' : null;
    }
    data.view.piles = data.cards.map(card => ({ name: card.name, state: card.state }));
    data.piles = [{ name: primary.name, ...structuredClone(primary.current_assessment), connectivity: [] }];
    data.last_known_piles = data.cards.slice(1).map(card => ({ name: card.name,
      ...structuredClone(card.last_known_assessment), connectivity: [] }));
    const serialized = JSON.stringify(data);
    const initial = html.replace(/(<script type="application\/json" id="initial-state">)[\s\S]*?(<\/script>)/,
      (_match, open, close) => open + serialized.replaceAll('<', '\\u003c') + close);
    const context = await browser.newContext();
    const page = await context.newPage();
    failOnPageError(page);
    await page.clock.install({ time: new Date('2026-11-01T06:40:30Z') });
    await page.clock.pauseAt(new Date('2026-11-01T06:40:31Z'));
    await page.route(url + '/', route => route.fulfill({ status: 200, contentType: 'text/html', body: initial }));
    await page.route('**/api/state', route => route.abort());
    await page.route('**/api/logs*', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data.logs) }));
    await page.goto(url);
    const target = byId(page, 'pile-card').nth(1);
    assert.equal(await target.locator('.health').textContent(), 'Not assessed');
    assert.equal(await target.evaluate(card => card.classList.contains('failed')), false);
    assert.equal(await target.locator('.alert-icon').isVisible(), false);
    assert.equal(await page.locator('#incident-piles').textContent(), '');
    await page.clock.runFor(1000);
    assert.equal(await byId(page, 'pile-card').first().locator('.health').textContent(),
      expiring === 'state_evidence' ? 'Assessment unavailable' : 'Healthy',
      'current health requires a current state proof');
    assert.equal(await byId(page, 'pile-card').first().locator('.last-known').textContent(),
      expiring === 'state_evidence' ? 'Last known: HEALTHY' : '',
      'state expiry retains the last known health');
    for (const index of [2, 3]) {
      const excluded = byId(page, 'pile-card').nth(index);
      assert.equal(await excluded.locator('.health').textContent(), 'Not assessed', 'lifecycle exclusion survives local TTL');
      assert.equal(await excluded.evaluate(card => card.classList.contains('failed')), false);
      assert.equal(await excluded.locator('.alert-icon').isVisible(), false);
    }
    assert.match(await target.locator('.health').textContent(), /Health unknown.*expired/, expiring + ' expiry removes the stage-one exclusion');
    assert.equal(await target.evaluate(card => card.classList.contains('failed')), true, 'retained failure restores the incident border');
    assert.equal(await target.locator('.alert-icon').isVisible(), true, 'retained failure restores the incident icon');
    assert.equal(await page.locator('#incident-strip').evaluate(strip => strip.classList.contains('incident')), true);
    assert.equal(await page.locator('#incident-piles').textContent(), 'Affected piles: ' + data.cards[1].name);
    await context.close();
  }
}

async function manualActionsExpireWithStateProof(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  const html = await (await fetch(url)).text();
  const cases = [
    { kind: 'lifecycle', actions: ['switchover', 'take_offline'], ages: [29500, 29000] },
    { kind: 'failover', actions: ['failover'], ages: [29500, 29000] },
    { kind: 'rejoin', actions: ['rejoin'], ages: [29500, 29000, 1000] },
  ];
  for (const { kind, actions, ages } of cases) {
    for (const stateAge of ages) {
      const data = fixtures(original, 2);
      data.mode = 'manual';
      data.mode_text = 'Manual · one reviewed request per approval.';
      data.controller_ready = true;
      data.incident = { active: kind === 'failover', piles: kind === 'failover' ? [data.cards[1].name] : [] };
      data.decision = { kind: 'STABLE', reason: 'No participating-pile incident', blocker: null,
        plan: null, cli_command: null };
      data.decision_text = data.decision.reason;
      const evidence = age => ({ age_ms: age, ttl_ms: 30000, expired: age >= 30000,
        expired_for_ms: Math.max(0, age - 30000) });
      const stateEvidence = { ...evidence(stateAge), source: 'GetClusterState' };
      data.view.age_ms = stateAge;
      data.view.evidence = structuredClone(stateEvidence);
      for (const card of data.cards) {
        card.state_current = true;
        card.state_evidence = structuredClone(stateEvidence);
        card.evidence = { ...card.evidence, ...evidence(1000), source: 'controller_assessment' };
        card.participating = true;
        card.participation_evidence = { ...evidence(1000), source: 'controller_participation' };
        card.verdict = card.last_known_verdict = 'HEALTHY';
        card.health_text = 'Healthy';
        card.incident = card.confirmed = false;
        card.current_assessment = { ...card.current_assessment, verdict: 'HEALTHY', confirmed: false,
          evidence: structuredClone(card.evidence) };
        card.last_known_assessment = structuredClone(card.current_assessment);
      }
      const card = data.cards[1];
      if (kind === 'failover') {
        card.verdict = card.last_known_verdict = 'FAILED';
        card.health_text = 'Failed · confirmed';
        card.incident = card.confirmed = true;
        card.current_assessment.verdict = 'FAILED';
        card.current_assessment.confirmed = true;
        card.last_known_assessment = structuredClone(card.current_assessment);
        card.context_action = 'failover';
        card.proposed = 'Proposed disconnect';
        data.decision = { kind: 'READY', reason: 'Confirmed failed pile', blocker: null,
          plan: { expected_generation: data.view.generation, update: { [card.name]: 'DISCONNECTED' },
            endpoint: 'grpc://fixture:2135' } };
      } else if (kind === 'rejoin') {
        card.state = 'DISCONNECTED';
        card.participating = false;
        card.assessment = 'not_assessed';
        card.verdict = 'UNKNOWN';
        card.last_known_verdict = 'FAILED';
        card.health_text = 'Not assessed';
        card.current_assessment = null;
        card.evidence = { ...card.evidence, ...evidence(40000) };
        card.last_known_assessment = { ...card.last_known_assessment, verdict: 'FAILED',
          assessment: 'expired', evidence: structuredClone(card.evidence) };
        card.context_action = 'rejoin';
      }
      for (const action of actions) {
        card.actions[action] = { ...card.actions[action], available: true, reason: '' };
      }
      data.view.piles = data.cards.map(item => ({ name: item.name, state: item.state }));
      data.piles = data.cards.filter(item => item.current_assessment).map(item => ({ name: item.name,
        ...structuredClone(item.current_assessment), connectivity: [] }));
      data.last_known_piles = kind === 'rejoin' ? [{ name: card.name,
        ...structuredClone(card.last_known_assessment), connectivity: [] }] : [];
      data.failover_action = kind === 'failover' ? { ...card.actions.failover } : data.failover_action;
      const initial = html.replace(/(<script type="application\/json" id="initial-state">)[\s\S]*?(<\/script>)/,
        (_match, open, close) => open + JSON.stringify(data).replaceAll('<', '\\u003c') + close);
      const context = await browser.newContext();
      const page = await context.newPage();
      failOnPageError(page);
      await page.clock.install({ time: new Date('2026-11-01T06:40:30Z') });
      await page.clock.pauseAt(new Date('2026-11-01T06:40:31Z'));
      await page.route(url + '/', route => route.fulfill({ status: 200, contentType: 'text/html', body: initial }));
      await page.route('**/api/state', route => route.abort());
      await page.route('**/api/logs*', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data.logs) }));
      await page.goto(url);
      const target = page.locator('[data-testid="pile-card"][data-pile-name="' + card.name + '"]');
      const assertActions = async disabled => {
        for (const action of actions) {
          const buttons = target.locator('[data-action="' + action + '"]');
          const count = await buttons.count();
          assert.ok(count, action + ' is rendered');
          assert.equal(await buttons.evaluateAll((elements, expected) => elements.every(button => button.disabled === expected), disabled),
            true, action + ' follows the current state proof');
          assert.deepEqual(await buttons.evaluateAll(elements => elements.map(button =>
            button.parentNode.querySelector('.action-reason').textContent)),
            Array(count).fill(disabled ? 'Fresh coherent observations required' : ''),
            action + ' exposes the state freshness reason');
        }
        if (kind === 'failover') {
          const button = page.locator('#review-failover');
          assert.equal(await button.isDisabled(), disabled, 'incident failover follows the selected state proof');
          const reason = await button.getAttribute('aria-describedby');
          assert.ok(reason, 'incident failover links its freshness reason');
          assert.equal(await page.locator('#' + reason).textContent(),
            disabled ? 'Fresh coherent observations required' : '');
        }
      };
      assert.equal(await target.locator('.health').textContent(), card.health_text);
      await assertActions(false);
      // One-second UI ticks land exactly on TTL for age 29 s, and beyond it
      // for age 29.5 s. Both retain fresh 1-second assessment evidence.
      await page.clock.runFor(stateAge === 29500 ? 499 : 999);
      await assertActions(false);
      await page.clock.runFor(stateAge === 29500 ? 501 : 1);
      const stateExpired = stateAge !== 1000;
      await assertActions(stateExpired);
      assert.equal(await target.locator('.state-badge').textContent(), card.state, 'expiry retains the observed state');
      assert.equal(await target.locator('.health').textContent(), stateExpired && kind !== 'rejoin'
        ? 'Assessment unavailable' : card.health_text);
      if (stateExpired && kind !== 'rejoin') {
        assert.equal(await target.locator('.last-known').textContent(), 'Last known: ' + card.last_known_verdict);
      }
      await target.locator('[data-details]').click();
      const diagnostic = JSON.parse(await page.locator('#evidence-content details pre').textContent());
      assert.deepEqual(diagnostic.card, card, 'local expiry does not rewrite the received canonical card');
      await context.close();
    }
  }
}

async function reviewRace(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  for (const replacement of ['another-plan', 'auto-mode']) {
    const data = fixtures(original, 3);
    data.mode = 'manual';
    data.mode_text = 'Manual · one reviewed request per approval.';
    for (const card of data.cards.slice(1)) card.actions.switchover = {
      label: 'Switchover here', available: true, reason: ''
    };
    const { context, page } = await fixturePage(browser, data, { viewport: { width: 1280, height: 800 } });
    const held = deferred();
    const requested = deferred();
    const executions = [];
    await page.route('**/api/plans', async route => {
      const request = route.request().postDataJSON();
      const target = request.target_pile;
      assert.ok(['pile-02', 'pile-03'].includes(target));
      if (target === 'pile-02') {
        requested.resolve();
        await held.promise;
      }
      const before = Object.fromEntries(data.cards.map(card => [card.name, card.state]));
      const after = { ...before, [target]: 'PROMOTED' };
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
        plan_id: 'fixture-plan-' + target, cluster_id: data.cluster_id,
        mode_revision: data.mode_revision, reason: 'Fixture switchover ' + target,
        expires_at: new Date(Date.parse(data.now) + 120000).toISOString(), before, after,
        plan: { expected_generation: data.view.generation, update: { [target]: 'PROMOTED' }, endpoint: 'grpc://fixture:2135' }
      }) });
    });
    await page.route('**/api/operations', route => {
      executions.push(route.request().postDataJSON().plan_id);
      return route.fulfill({ status: 202, contentType: 'application/json', body: JSON.stringify({ operation_id: 'fixture-operation' }) });
    });
    await page.locator('[data-action="switchover"][data-pile-name="pile-02"]').first().click();
    await requested.promise;
    await byId(page, 'review-cancel').click();
    if (replacement === 'another-plan') {
      await page.locator('[data-action="switchover"][data-pile-name="pile-03"]').first().click();
      await until(async () => (await page.locator('#review-body').innerText()).includes('Fixture switchover pile-03'), 'replacement plan B displayed');
    } else {
      await byId(page, 'auto-mode').click();
      await until(async () => await page.locator('#review-title').innerText() === 'Enable Auto mode', 'replacement Auto review');
    }
    const oldResponse = page.waitForResponse(response => response.url().endsWith('/api/plans') &&
      response.request().postDataJSON().target_pile === 'pile-02');
    held.resolve();
    await (await oldResponse).finished();
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    if (replacement === 'another-plan') {
      const changed = await page.locator('#review-body .review-vector tr.changed').innerText();
      assert.ok(changed.includes('pile-03') && !changed.includes('pile-02'), 'delayed cancelled plan A must not replace reviewed plan B');
      await byId(page, 'review-execute').click();
      await until(async () => executions.length === 1, 'replacement plan B execute');
      assert.deepEqual(executions, ['fixture-plan-pile-03']);
    } else {
      assert.equal(await page.locator('#review-title').innerText(), 'Enable Auto mode', 'delayed cancelled plan must not replace Auto review');
      assert.equal(await page.locator('#review-body .review-vector').count(), 0);
      assert.deepEqual(executions, []);
    }
    await screenshot(page, 'review-race-' + replacement);
    await context.close();
  }
}

async function reviewAlternates(browser) {
  const { context, page } = await startPage(browser);
  const response = page.waitForResponse(value => value.url().endsWith('/api/plans'));
  await page.locator('[data-action="switchover"][data-pile-name="b"]').first().click();
  const plan = await (await response).json();
  const dialog = byId(page, 'review-dialog');
  await until(async () => !(await byId(page, 'review-execute').isDisabled()), 'switchover review ready');
  assert.equal(await dialog.locator('p').filter({ hasText: 'Alternate synchronized piles after this change:' }).innerText(),
    'Alternate synchronized piles after this change: a', 'old primary is the alternate, rather than the promoted target');
  assert.deepEqual(plan.alternate_synchronized_pile_names, ['a']);
  assert.equal(plan.alternate_synchronized_piles, 1);
  await screenshot(page, 'switchover-alternates');
  await byId(page, 'review-cancel').click();
  const card = page.locator('[data-testid="pile-card"][data-pile-name="b"]');
  await card.locator('.more summary').click();
  const offlineResponse = page.waitForResponse(value => value.url().endsWith('/api/plans'));
  await card.locator('[data-action="take_offline"]').click();
  const offline = await (await offlineResponse).json();
  await until(async () => !(await byId(page, 'review-execute').isDisabled()), 'take offline review ready');
  assert.match(await dialog.innerText(), /Zero data loss\. Application availability is not guaranteed\./,
    'the durability statement explicitly distinguishes application availability');
  assert.match(await dialog.innerText(), /After this change: no alternate synchronized pile remains for failover\./);
  assert.deepEqual(offline.alternate_synchronized_pile_names, []);
  assert.equal(offline.alternate_synchronized_piles, 0);
  await screenshot(page, 'take-offline-no-alternate');
  await byId(page, 'review-cancel').click();
  await context.close();
}

async function rejoinRetainedEvidence(browser) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  const response = await page.goto(url);
  assert.equal(response.status(), 200);
  await pinnedFonts(page);
  const current = await state(page);
  const target = current.cards.find(card => card.name === 'b');
  assert.equal(target.state, 'DISCONNECTED');
  assert.equal(target.assessment, 'not_assessed');
  assert.equal(target.evidence.expired, true);
  assert.equal(target.actions.rejoin.available, true);
  const button = page.locator('[data-action="rejoin"][data-pile-name="b"]').first();
  assert.equal(await button.isDisabled(), false, 'retained target health does not gate canonically available Rejoin');
  await button.click();
  const dialog = byId(page, 'review-dialog');
  await until(async () => !(await byId(page, 'review-execute').isDisabled()) &&
    (await dialog.innerText()).includes('NOT_SYNCHRONIZED'), 'complete Rejoin review opens');
  await byId(page, 'review-execute').click();
  await until(async () => {
    const observed = (await state(page)).view?.piles.find(pile => pile.name === 'b')?.state;
    return observed === 'NOT_SYNCHRONIZED' || observed === 'SYNCHRONIZED';
  }, 'Rejoin observed by real controller');
  await until(async () => ['NOT_SYNCHRONIZED', 'SYNCHRONIZED'].includes(
    await page.locator('[data-testid="pile-card"][data-pile-name="b"] .state-badge').innerText()),
  'browser reconciles observed Rejoin');
  await screenshot(page, 'rejoin-retained-evidence');
  await context.close();
}

async function time(browser) {
  const original = await (await fetch(url + '/api/state')).json();
  for (const timezoneId of ['Europe/Belgrade', 'America/New_York', 'UTC']) {
    const data = fixtures(original, 2, true);
    const { context, page } = await fixturePage(browser, data, { timezoneId, locale: 'en-GB', viewport: { width: 1280, height: 800 } });
    assert.match(await page.locator('body').innerText(), /expired 10 min ago/i);
    assert.match(await page.locator('body').innerText(), new RegExp(timezoneId.replace('/', '\\/')));
    const raw = await page.evaluate(() => [...document.querySelectorAll('time[datetime]')].map(element => element.dateTime));
    assert.ok(raw.every(value => /Z$/.test(value)), 'UTC remains canonical in datetime attributes');
    const first = await page.locator('time[datetime="2026-11-01T05:30:00Z"]').first().innerText();
    const second = await page.locator('time[datetime="2026-11-01T06:30:00Z"]').first().innerText();
    if (timezoneId === 'America/New_York') {
      assert.match(first, /01:30:00.*GMT-4/);
      assert.match(second, /01:30:00.*GMT-5/);
    } else if (timezoneId === 'Europe/Belgrade') {
      assert.match(first, /06:30:00.*GMT\+1/);
      assert.match(second, /07:30:00.*GMT\+1/);
    } else {
      assert.match(first, /05:30:00.*GMT/);
      assert.match(second, /06:30:00.*GMT/);
    }
    await screenshot(page, 'time-' + timezoneId.replace('/', '-'));
    await context.close();
  }
}

(async () => {
  const browser = await playwright.chromium.launch({ headless: true, executablePath: process.env.SKIPPER_BROWSER_EXECUTABLE });
  try {
    const scenarios = { manual, auto_off: autoOff, pending_off: pendingOff, presentation, mock_control_focus: mockControlFocus, time, review_race: reviewRace, disabled_failover: disabledFailover,
      log_filter_race: logFilterRace, log_selection_retention: logSelectionRetention,
      log_predicates: logPredicates, default_info_debug: defaultInfoDebug, rejoin_retained_evidence: rejoinRetainedEvidence,
      review_alternates: reviewAlternates, canonical_presentation: canonicalPresentation, canonical_labels: canonicalLabels,
      manual_review_scope: manualReviewSnapshotScope, auto_review_refresh: autoReviewRefresh, mode_review_failure: modeReviewFailure,
      poll_handoff_status: pollFailurePreservesHandoff, forced_theme_scheme: forcedThemeColorScheme,
      pending_operation_controls: pendingOperationControls, incident_evidence_target: incidentEvidenceUsesAffectedPile,
      cached_time_formatting: cachedTimeFormatting, stable_expanded_history: stableExpandedHistory,
      fresh_evidence_expiry: freshEvidenceExpiresOnClock, stage1_exclusion_expiry: stage1ExclusionExpiresOnClock,
      manual_actions_state_expiry: manualActionsExpireWithStateProof };
    assert.ok(scenarios[process.env.SKIPPER_SCENARIO], 'known test scenario');
    await scenarios[process.env.SKIPPER_SCENARIO](browser);
  } catch (error) {
    let index = 0;
    for (const context of browser.contexts()) for (const page of context.pages()) {
      try { await screenshot(page, 'failure-' + process.env.SKIPPER_SCENARIO + '-' + index++); } catch {}
    }
    throw error;
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
