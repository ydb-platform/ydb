const Dataset = require('libs/dataset/v2');

function loadRows() {
    try {
        const rows = Dataset.getDatasetRows({datasetName: 'data'});
        if (rows && rows.length) {
            return rows;
        }
    } catch (e) {
        console.log('[prepare] getDatasetRows failed', String(e));
    }
    const loaded = Editor.getLoadedData() || {};
    const block = loaded.data;
    if (!block || !block.result || !block.result.data) {
        return [];
    }
    const titles = (block.result.fields || []).map(function(field) { return field.title; });
    return (block.result.data.Data || []).map(function(row) {
        const obj = {};
        titles.forEach(function(title, i) { obj[title] = row[i]; });
        return obj;
    });
}

function num(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function ms(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value > 1e12 ? value : value * 1000;
    }
    const parsed = Date.parse(String(value));
    return Number.isFinite(parsed) ? parsed : null;
}

function esc(value) {
    return String(value == null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function firstParam(name, fallback) {
    const values = Editor.getParam(name);
    if (values && values.length && values[0] !== '') {
        return String(values[0]);
    }
    return fallback;
}

function isPostcommit(row) {
    return /postcommit/i.test(String(row.job_name || '')) || /postcommit/i.test(String(row.workflow || ''));
}

const hidePostcommit = firstParam('cap_pc', 'hide') !== 'show';
const zone = firstParam('cap_zone', 'ru-central1-d');

function keepQuotaName(name) {
    const at = String(name).indexOf('@');
    if (zone === 'all') {
        return at === -1;
    }
    return String(name).endsWith('@' + zone);
}

function quotaKey(name) {
    const at = String(name).indexOf('@');
    return at === -1 ? name : name.slice(0, at);
}

function keepRunner(row) {
    return zone === 'all' || String(row.zone || '') === zone;
}

function fmt(value) {
    const n = Math.round(value);
    return n.toLocaleString('en-US');
}

const RESOURCES = [
    {title: 'RAM', unit: 'GiB', quota: 'ram_gib', demand: 'ram'},
    {title: 'vCPU', unit: 'vCPU', quota: 'cores', demand: 'cores'},
    {title: 'VM slots', unit: 'VMs', quota: 'instances', demand: 'count'},
    {title: 'SSD', unit: 'GiB', quota: 'nrd_gib', demand: 'ssd'},
];

const byTime = {};
loadRows().forEach(function(row) {
    const t = ms(row.sampled_at);
    if (t == null) {
        return;
    }
    const slot = byTime[t] || (byTime[t] = {
        quota: {},
        queue: {cores: 0, ram: 0, ssd: 0, count: 0, sized: false},
        queues: [],
        jobs: [],
        idles: [],
    });
    if (row.kind === 'quota') {
        if (!keepQuotaName(row.name)) {
            return;
        }
        slot.quota[quotaKey(row.name)] = {
            used: num(row.usage_value) || 0,
            limit: num(row.limit_value) || 0,
        };
    } else if (row.kind === 'runner' && row.state === 'idle') {
        if (!keepRunner(row)) {
            return;
        }
        slot.idles.push({cores: num(row.cores) || 0, ram_gib: num(row.ram_gib) || 0});
    } else if (row.kind === 'queue' && !(hidePostcommit && isPostcommit(row))) {
        slot.jobs.push(row);
        const cores = num(row.cores) || 0;
        const ram = num(row.ram_gib) || 0;
        const ssd = num(row.nrd_gib) || 0;
        slot.queue.cores += cores;
        slot.queue.ram += ram;
        slot.queue.ssd += ssd;
        slot.queue.count += num(row.usage_value) || 0;
        if (cores || ram || ssd) {
            slot.queue.sized = true;
        }
        const workflow = String(row.workflow || row.name || '');
        let group = null;
        slot.queues.forEach(function(item) {
            if (item.workflow === workflow) {
                group = item;
            }
        });
        if (!group) {
            group = {workflow: workflow, count: 0, ram: 0, cores: 0, ssd: 0, jobs: []};
            slot.queues.push(group);
        }
        group.count += num(row.usage_value) || 0;
        group.ram += ram;
        group.cores += cores;
        group.ssd += ssd;
        if (row.run_url || row.job_name) {
            group.jobs.push({
                job: String(row.job_name || ''),
                url: String(row.run_url || ''),
                pr: String(row.pr_number || ''),
                title: String(row.pr_title || ''),
                branch: String(row.branch || ''),
                age: num(row.age_sec),
                ram: ram,
            });
        }
    }
});

const times = Object.keys(byTime).map(Number).sort(function(a, b) { return a - b; }).slice(-48);

function askedOf(queue, key) {
    if (key === 'ram') {
        return queue.ram;
    }
    if (key === 'cores') {
        return queue.cores;
    }
    if (key === 'ssd') {
        return queue.ssd;
    }
    return queue.count;
}

function profileKey(cores, ram) {
    const c = Math.round((num(cores) || 0) / 4) * 4;
    const r = Math.round((num(ram) || 0) / 16) * 16;
    if (c <= 0 || r <= 0) {
        return '';
    }
    return c + 'x' + r;
}

function leftoverAfterIdle(jobs, idles) {
    const pool = {};
    (idles || []).forEach(function(row) {
        const key = profileKey(row.cores, row.ram_gib);
        if (!key) {
            return;
        }
        pool[key] = (pool[key] || 0) + 1;
    });
    const leftover = {cores: 0, ram: 0, ssd: 0, count: 0};
    const leftoverJobs = [];
    let covered = 0;
    (jobs || []).forEach(function(row) {
        const n = Math.max(1, Math.round(num(row.usage_value) || 1));
        const key = profileKey(row.cores, row.ram_gib);
        let need = n;
        if (key && pool[key]) {
            const take = Math.min(need, pool[key]);
            pool[key] -= take;
            covered += take;
            need -= take;
        }
        if (need > 0) {
            leftover.count += need;
            leftover.ram += (num(row.ram_gib) || 0) * (need / n);
            leftover.cores += (num(row.cores) || 0) * (need / n);
            leftover.ssd += (num(row.nrd_gib) || 0) * (need / n);
            leftoverJobs.push({
                ram: (num(row.ram_gib) || 0) / n,
                cores: (num(row.cores) || 0) / n,
                ssd: (num(row.nrd_gib) || 0) / n,
                count: need,
            });
        }
    });
    return {leftover: leftover, leftoverJobs: leftoverJobs, covered: covered};
}

const snapshots = times.map(function(t) {
    const slot = byTime[t];
    const cover = leftoverAfterIdle(slot.jobs, slot.idles);
    const resources = RESOURCES.map(function(resource) {
        const quota = slot.quota[resource.quota] || {used: 0, limit: 0};
        const asked = askedOf(slot.queue, resource.demand);
        const leftover = askedOf(cover.leftover, resource.demand);
        const room = Math.max(0, quota.limit - quota.used);
        return {
            title: resource.title,
            unit: resource.unit,
            used: quota.used,
            asked: asked,
            leftover: leftover,
            limit: quota.limit,
            fits: Math.min(leftover, room),
            short: Math.max(0, leftover - room),
        };
    });
    const queues = slot.queues.slice().sort(function(a, b) { return b.ram - a.ram || b.count - a.count; });
    queues.forEach(function(group) {
        (group.jobs || []).sort(function(a, b) { return (b.age || 0) - (a.age || 0); });
    });
    return {
        id: String(t),
        label: new Date(t).toISOString().slice(0, 16).replace('T', ' '),
        sized: slot.queue.sized,
        count: slot.queue.count,
        covered: cover.covered,
        leftoverJobs: cover.leftoverJobs,
        resources: resources,
        queues: queues,
    };
});

const scales = {};
RESOURCES.forEach(function(resource) {
    let scale = 1;
    snapshots.forEach(function(snapshot) {
        const row = null;
        let found = null;
        snapshot.resources.forEach(function(item) {
            if (item.title === resource.title) {
                found = item;
            }
        });
        if (found) {
            scale = Math.max(scale, found.limit, found.used + found.leftover);
        }
    });
    scales[resource.title] = scale;
});

module.exports = {
    render: Editor.wrapFn({
        args: [{snapshots: snapshots, scales: scales, hidePostcommit: hidePostcommit}],
        fn: function(options, cfg) {
            function esc(value) {
                return String(value == null ? '' : value)
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;');
            }
            function fmt(value) {
                return Math.round(Number(value) || 0).toLocaleString('en-US');
            }
            function pct(value, limit) {
                const v = Number(value);
                const l = Number(limit);
                if (!Number.isFinite(v) || !Number.isFinite(l) || l <= 0) {
                    return '';
                }
                const p = Math.max(0, v / l * 100);
                if (p === 0) {
                    return '0%';
                }
                if (p < 0.05) {
                    return '<0.1%';
                }
                return p.toFixed(1) + '%';
            }
            function withPct(amount, limit) {
                const p = pct(amount, limit);
                return p ? ' (' + p + ')' : '';
            }
            const box = Number(options && (options.height || options.innerHeight));
            const height = box > 80 ? box : 720;
            const snapshots = (cfg && cfg.snapshots) || [];
            const scales = (cfg && cfg.scales) || {};
            const hidePostcommit = !!(cfg && cfg.hidePostcommit);
            const state = (typeof Chart !== 'undefined' && typeof Chart.getState === 'function')
                ? (Chart.getState() || {})
                : {};
            const fallback = snapshots.length ? snapshots[snapshots.length - 1].id : '';
            const selectedId = state.selectedId || fallback;
            let selected = snapshots.length ? snapshots[snapshots.length - 1] : null;
            snapshots.forEach(function(snapshot) {
                if (snapshot.id === selectedId) {
                    selected = snapshot;
                }
            });

            const titles = [];
            if (selected) {
                selected.resources.forEach(function(item) { titles.push(item.title); });
            }
            const panels = titles.map(function(title) {
                const scale = scales[title] || 1;
                const columns = snapshots.map(function(snapshot) {
                    let point = null;
                    snapshot.resources.forEach(function(item) {
                        if (item.title === title) {
                            point = item;
                        }
                    });
                    point = point || {used: 0, fits: 0, short: 0, limit: 0};
                    const usedH = point.used / scale * 100;
                    const fitH = point.fits / scale * 100;
                    const shortH = point.short / scale * 100;
                    const limitTop = 100 - point.limit / scale * 100;
                    const on = snapshot.id === selectedId;
                    return '<div data-id="' + esc(snapshot.id) + '" style="position:relative;flex:1;min-width:0;height:100%;cursor:pointer;outline:' +
                        (on ? '2px solid #222' : 'none') + ';outline-offset:-2px">' +
                        '<div data-id="' + esc(snapshot.id) + '" style="position:absolute;left:0;right:0;top:' + limitTop.toFixed(2) + '%;border-top:1px dashed #888"></div>' +
                        '<div data-id="' + esc(snapshot.id) + '" style="position:absolute;left:1px;right:1px;bottom:0;height:' + usedH.toFixed(2) + '%;background:#4E79A7"></div>' +
                        '<div data-id="' + esc(snapshot.id) + '" style="position:absolute;left:1px;right:1px;bottom:' + usedH.toFixed(2) + '%;height:' + fitH.toFixed(2) + '%;background:#F6C85F"></div>' +
                        '<div data-id="' + esc(snapshot.id) + '" style="position:absolute;left:1px;right:1px;bottom:' + (usedH + fitH).toFixed(2) + '%;height:' + shortH.toFixed(2) + '%;background:#E15759"></div>' +
                        '</div>';
                }).join('');
                return '<div style="margin-top:14px">' +
                    '<div style="font-weight:650;margin-bottom:4px">' + esc(title) + '</div>' +
                    '<div style="height:88px;display:flex;gap:2px;align-items:stretch">' + columns + '</div></div>';
            }).join('');

            let detail = '<div style="margin-top:12px;color:#777">Click a column.</div>';
            if (selected) {
                function runnersFor(item) {
                    const field = item.title === 'RAM' ? 'ram' : (item.title === 'vCPU' ? 'cores' : (item.title === 'SSD' ? 'ssd' : 'vm'));
                    const sizes = [];
                    (selected.leftoverJobs || []).forEach(function(job) {
                        const count = Math.round(job.count || 0);
                        if (!count) {
                            return;
                        }
                        const size = field === 'vm' ? 1 : (job[field] || 0);
                        for (let i = 0; i < count; i += 1) {
                            sizes.push(size);
                        }
                    });
                    sizes.sort(function(a, b) { return b - a; });
                    let left = Math.max(0, (item.limit || 0) - (item.used || 0));
                    let short = 0;
                    sizes.forEach(function(size) {
                        if (size <= left + 0.001) {
                            left -= size;
                        } else {
                            short += 1;
                        }
                    });
                    return {asked: selected.count || 0, leftover: sizes.length, short: short, covered: selected.covered || 0};
                }
                function cell(amount, unit, runners, isShort, limit) {
                    const share = withPct(amount, limit);
                    if (!runners) {
                        return (amount ? fmt(amount) + ' ' + unit : '0') + share;
                    }
                    const people = runners + (runners === 1 ? ' runner' : ' runners');
                    const text = fmt(amount) + ' ' + unit + share + ' · ' + people;
                    return isShort ? text + ' need new VMs' : text;
                }
                const resourceRows = selected.resources.map(function(item) {
                    const fit = runnersFor(item);
                    const overflow = item.short > 0
                        ? 'color:#E15759;font-weight:650'
                        : '';
                    return '<tr><td style="padding:4px 8px">' + esc(item.title) + '</td>' +
                        '<td style="padding:4px 8px;text-align:right">' + fmt(item.used) + ' ' + esc(item.unit) + withPct(item.used, item.limit) + '</td>' +
                        '<td style="padding:4px 8px;text-align:right">' + cell(item.asked, item.unit, fit.asked, false, item.limit) + '</td>' +
                        '<td style="padding:4px 8px;text-align:right">' + cell(item.leftover, item.unit, fit.leftover, false, item.limit) + '</td>' +
                        '<td style="padding:4px 8px;text-align:right;' + overflow + '">' + cell(item.short, item.unit, fit.short, true, item.limit) + '</td>' +
                        '<td style="padding:4px 8px;text-align:right">' + fmt(item.limit) + ' ' + esc(item.unit) + '</td></tr>';
                }).join('');
                function ageText(sec) {
                    const s = Number(sec);
                    if (!Number.isFinite(s)) {
                        return '—';
                    }
                    if (s < 3600) {
                        return Math.round(s / 60) + 'm';
                    }
                    if (s < 86400) {
                        return Math.round(s / 3600) + 'h';
                    }
                    return (s / 86400).toFixed(1) + 'd';
                }
                function jobLink(job) {
                    const label = esc(job.job || 'open job');
                    if (!job.url) {
                        return label;
                    }
                    return '<a href="' + esc(job.url) + '" target="_blank" style="color:#1a73e8">' + label + '</a>';
                }
                function prLink(job) {
                    if (!job.pr) {
                        return '';
                    }
                    return '<a href="https://github.com/ydb-platform/ydb/pull/' + encodeURIComponent(job.pr) +
                        '" target="_blank" style="color:#1a73e8">#' + esc(job.pr) + '</a>';
                }
                const openWf = state.openWf || {};
                const queueRows = (selected.queues || []).map(function(item) {
                    const jobs = item.jobs || [];
                    const opened = !!openWf[item.workflow];
                    const inner = jobs.map(function(job) {
                        return '<tr><td style="padding:3px 8px">' + prLink(job) + '</td>' +
                            '<td style="padding:3px 8px">' + esc((job.title || '').slice(0, 80)) + '</td>' +
                            '<td style="padding:3px 8px">' + jobLink(job) + '</td>' +
                            '<td style="padding:3px 8px">' + esc(job.branch || '') + '</td>' +
                            '<td style="padding:3px 8px;text-align:right">' + ageText(job.age) + '</td>' +
                            '<td style="padding:3px 8px;text-align:right">' + fmt(job.ram) + '</td></tr>';
                    }).join('');
                    const body = (opened && jobs.length)
                        ? '<table style="border-collapse:collapse;margin:4px 0 8px 16px"><thead><tr style="color:#777;text-align:left">' +
                            '<th style="padding:3px 8px">PR</th><th style="padding:3px 8px">PR title</th>' +
                            '<th style="padding:3px 8px">job</th><th style="padding:3px 8px">branch</th>' +
                            '<th style="padding:3px 8px">waiting</th><th style="padding:3px 8px">RAM, GiB</th>' +
                            '</tr></thead><tbody>' + inner + '</tbody></table>'
                        : '';
                    return '<div data-id="wf:' + esc(item.workflow) + '" style="border-top:1px solid #eee;padding:4px 0;cursor:pointer">' +
                        (opened ? '▾ ' : '▸ ') +
                        '<b data-id="wf:' + esc(item.workflow) + '">' + esc(item.workflow) + '</b> · ' +
                        fmt(item.count) + ' waiting · ' + fmt(item.ram) + ' GiB RAM</div>' + body;
                }).join('');
                const sizeNote = (!selected.sized && selected.count)
                    ? '<div style="color:#888;font-size:12px">This older snapshot has the job count, not the runner size.</div>'
                    : '';
                detail = '<div style="margin-top:14px;font-weight:650">' + esc(selected.label) + ' UTC</div>' +
                    sizeNote +
                    '<table style="border-collapse:collapse;margin-top:6px"><thead><tr style="color:#777;text-align:left">' +
                    '<th style="padding:4px 8px">quota</th><th style="padding:4px 8px">in use</th>' +
                    '<th style="padding:4px 8px">queue asks</th><th style="padding:4px 8px">after idle</th>' +
                    '<th style="padding:4px 8px">needs new VMs</th>' +
                    '<th style="padding:4px 8px">limit</th></tr></thead><tbody>' + resourceRows + '</tbody></table>' +
                    '<div style="color:#888;font-size:12px;margin-top:4px">Percents are of the quota limit. Idle runners of the same size take jobs first. Yellow/red is leftover demand vs free quota. ' +
                    (selected.covered ? selected.covered + ' waiting jobs are covered by idle.' : 'No idle of the same size for this queue.') + '</div>' +
                    '<div style="margin-top:10px;font-weight:650">Queue in this snapshot</div>' +
                    '<div style="color:#888;font-size:12px">Open a workflow, then the job name. Scroll this list if it is longer than the widget.</div>' +
                    (queueRows || '<div style="padding:4px 8px;color:#777">queue is empty</div>');
            }

            const html = '<div style="height:' + height + 'px;box-sizing:border-box;overflow:hidden;display:flex;flex-direction:column;padding:12px 16px;font:13px/1.35 ui-sans-serif,system-ui,sans-serif;color:#222">' +
                '<div>Click a column. Blue is quota already in use (includes idle). Yellow/red is leftover queue after idle runners of the same size take matching jobs. The dashed line is the quota limit. The table under the chart is the column you clicked.' +
                (hidePostcommit
                    ? ' Postcommit is hidden: the gate does not start it while a PR-check is waiting. Use the Postcommit selector to show it.'
                    : '') +
                '</div>' +
                '<div style="margin-top:6px;font-size:12px;color:#444">' +
                '<span style="margin-right:12px"><i style="display:inline-block;width:8px;height:8px;background:#4E79A7;margin-right:4px"></i>in use</span>' +
                '<span style="margin-right:12px"><i style="display:inline-block;width:8px;height:8px;background:#F6C85F;margin-right:4px"></i>leftover, still fits</span>' +
                '<span><i style="display:inline-block;width:8px;height:8px;background:#E15759;margin-right:4px"></i>leftover, needs new VMs</span></div>' +
                panels +
                '<div style="flex:1;min-height:120px;overflow:auto;margin-top:4px">' + detail + '</div></div>';
            return Editor.generateHtml(html);
        },
    }),
    events: {
        click: Editor.wrapFn({
            fn: function(event) {
                const target = event && event.target;
                if (!target || !target.getAttribute) {
                    return;
                }
                let node = target;
                let id = '';
                for (let hop = 0; node && hop < 8; hop += 1) {
                    if (node.tagName === 'A') {
                        return;
                    }
                    id = node.getAttribute && node.getAttribute('data-id') || '';
                    if (id) {
                        break;
                    }
                    node = node.parentNode;
                }
                if (!id || typeof Chart === 'undefined' || typeof Chart.setState !== 'function') {
                    return;
                }
                const prev = Chart.getState() || {};
                if (id.indexOf('wf:') === 0) {
                    const wf = id.slice(3);
                    const openWf = {};
                    const current = prev.openWf || {};
                    Object.keys(current).forEach(function(key) { openWf[key] = current[key]; });
                    openWf[wf] = !openWf[wf];
                    Chart.setState({selectedId: prev.selectedId, openWf: openWf});
                    return;
                }
                Chart.setState({selectedId: id, openWf: prev.openWf});
            },
        }),
    },
};
