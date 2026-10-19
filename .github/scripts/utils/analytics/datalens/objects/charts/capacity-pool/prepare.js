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

function ageText(sec) {
    const s = num(sec);
    if (s == null) {
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

function stamp(value) {
    const t = ms(value);
    if (t == null) {
        return '—';
    }
    return new Date(t).toISOString().slice(0, 16).replace('T', ' ');
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

function scopedName(name) {
    return zone === 'all' ? name : name + '@' + zone;
}

function keepRunner(row) {
    return zone === 'all' || String(row.zone || '') === zone;
}

const rows = loadRows();
let latest = null;
rows.forEach(function(row) {
    const t = ms(row.sampled_at);
    if (t != null && (latest == null || t > latest)) {
        latest = t;
    }
});
const current = rows.filter(function(row) { return ms(row.sampled_at) === latest; });

function one(kind, name) {
    for (let i = 0; i < current.length; i++) {
        if (current[i].kind === kind && current[i].name === name) {
            return current[i];
        }
    }
    return {};
}

const newest = one('summary', scopedName('runners.newest'));
const born = one('summary', scopedName('runners.created_24h'));
const ghrun = one('summary', scopedName('runners.ghrun'));
const idleSum = one('summary', scopedName('runners.idle'));
const hiddenPostcommit = current.filter(function(row) {
    return row.kind === 'queue' && isPostcommit(row);
}).length;
const queues = current.filter(function(row) {
    return row.kind === 'queue' && !(hidePostcommit && isPostcommit(row));
});
queues.sort(function(a, b) { return (num(b.age_sec) || 0) - (num(a.age_sec) || 0); });
const idle = current.filter(function(row) {
    return row.kind === 'runner' && row.state === 'idle' && keepRunner(row);
});
idle.sort(function(a, b) { return String(a.name).localeCompare(String(b.name)); });
const busy = current.filter(function(row) {
    return row.kind === 'runner' && row.state === 'busy' && keepRunner(row) && !(hidePostcommit && isPostcommit(row));
});
const busyRam = busy.reduce(function(acc, row) { return acc + (num(row.ram_gib) || 0); }, 0);
const busyCores = busy.reduce(function(acc, row) { return acc + (num(row.cores) || 0); }, 0);
const byWf = {};
busy.forEach(function(row) {
    const workflow = String(row.workflow || '(no workflow)');
    const job = String(row.job_name || 'job');
    const wf = byWf[workflow] || (byWf[workflow] = {workflow: workflow, jobs: {}, count: 0, ram: 0, cores: 0});
    wf.count += 1;
    wf.ram += num(row.ram_gib) || 0;
    wf.cores += num(row.cores) || 0;
    const jobGroup = wf.jobs[job] || (wf.jobs[job] = {job: job, runners: [], ram: 0, cores: 0});
    jobGroup.runners.push(row);
    jobGroup.ram += num(row.ram_gib) || 0;
    jobGroup.cores += num(row.cores) || 0;
});

function mapRunners(rows) {
    return rows.slice().sort(function(a, b) {
        const byPr = String(a.pr_number || '999999999').localeCompare(String(b.pr_number || '999999999'), undefined, {numeric: true});
        return byPr || String(a.name).localeCompare(String(b.name));
    }).map(function(row) {
        return {
            name: row.name,
            url: row.run_url || '',
            pr: row.pr_number || '',
            title: row.pr_title || '',
            branch: row.branch || '',
            ram: Math.round(num(row.ram_gib) || 0),
            cores: Math.round(num(row.cores) || 0),
        };
    });
}

const busyWfs = Object.keys(byWf).map(function(name) {
    const wf = byWf[name];
    const jobs = Object.keys(wf.jobs).map(function(jobName) {
        const job = wf.jobs[jobName];
        return {
            key: wf.workflow + '|' + job.job,
            job: job.job,
            count: job.runners.length,
            countShare: busy.length ? Math.round(job.runners.length / busy.length * 100) : 0,
            cores: Math.round(job.cores),
            coresShare: busyCores ? Math.round(job.cores / busyCores * 100) : 0,
            ram: Math.round(job.ram),
            ramShare: busyRam ? Math.round(job.ram / busyRam * 100) : 0,
            runners: mapRunners(job.runners),
        };
    });
    jobs.sort(function(a, b) { return b.ram - a.ram || b.count - a.count; });
    return {
        key: wf.workflow,
        workflow: wf.workflow,
        jobN: jobs.length,
        count: wf.count,
        countShare: busy.length ? Math.round(wf.count / busy.length * 100) : 0,
        cores: Math.round(wf.cores),
        coresShare: busyCores ? Math.round(wf.cores / busyCores * 100) : 0,
        ram: Math.round(wf.ram),
        ramShare: busyRam ? Math.round(wf.ram / busyRam * 100) : 0,
        jobs: jobs,
    };
});
busyWfs.sort(function(a, b) { return b.ram - a.ram || b.count - a.count; });

const waiting = {};
queues.forEach(function(row) {
    const workflow = String(row.workflow || row.name || '');
    const group = waiting[workflow] || (waiting[workflow] = {workflow: workflow, jobs: [], ram: 0, cores: 0, nrd: 0, age: 0, count: 0});
    group.jobs.push(row);
    group.count += num(row.usage_value) || 1;
    group.ram += num(row.ram_gib) || 0;
    group.cores += num(row.cores) || 0;
    group.nrd += num(row.nrd_gib) || 0;
    group.age = Math.max(group.age, num(row.age_sec) || 0);
});
const waitingList = Object.keys(waiting).map(function(key) { return waiting[key]; });
waitingList.sort(function(a, b) { return b.age - a.age || b.jobs.length - a.jobs.length; });
const waitGroups = waitingList.map(function(group) {
    return {
        workflow: group.workflow,
        count: group.count,
        ram: Math.round(group.ram),
        cores: Math.round(group.cores),
        nrd: Math.round(group.nrd),
        age: group.age,
        jobs: group.jobs.slice().sort(function(a, b) {
            return (num(b.age_sec) || 0) - (num(a.age_sec) || 0);
        }).map(function(row) {
            return {
                job: row.job_name || 'open job',
                url: row.run_url || '',
                pr: row.pr_number || '',
                title: row.pr_title || '',
                branch: row.branch || '',
                age: row.age_sec,
                ram: Math.round(num(row.ram_gib) || 0),
                cores: Math.round(num(row.cores) || 0),
                nrd: Math.round(num(row.nrd_gib) || 0),
            };
        }),
    };
});

const idleItems = idle.slice(0, 80).map(function(row) {
    return {
        name: row.name,
        cores: row.cores ? Math.round(row.cores) : '?',
        ram: row.ram_gib ? Math.round(row.ram_gib) : '?',
    };
});

module.exports = {
    render: Editor.wrapFn({
        args: [{
            latest: stamp(latest),
            newestWhen: stamp(newest.created_at),
            newestAge: ageText(newest.age_sec),
            newestJob: newest.job_name || '—',
            tone: (num(newest.age_sec) || 0) > 86400 ? '#E15759' : '#59A14F',
            born: Math.round(num(born.usage_value) || 0),
            ghrun: Math.round(num(ghrun.usage_value) || 0),
            idleN: Math.round(num(idleSum.usage_value) || 0),
            idleRam: Math.round(num(idleSum.ram_gib) || 0),
            idleCores: Math.round(num(idleSum.cores) || 0),
            busyN: busy.length,
            wfN: busyWfs.length,
            busyWfs: busyWfs,
            waitGroups: waitGroups,
            hiddenPostcommit: hiddenPostcommit,
            hidePostcommit: hidePostcommit,
            idleItems: idleItems,
        }],
        fn: function(options, cfg) {
            function esc(value) {
                return String(value == null ? '' : value)
                    .replace(/&/g, '&amp;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;');
            }
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
            function prLink(item) {
                if (!item.pr) {
                    return '';
                }
                return '<a href="https://github.com/ydb-platform/ydb/pull/' + encodeURIComponent(item.pr) +
                    '" target="_blank" style="color:#1a73e8">#' + esc(item.pr) + '</a>';
            }
            function href(item, label) {
                if (!item.url) {
                    return esc(label);
                }
                return '<a href="' + esc(item.url) + '" target="_blank" style="color:#1a73e8">' + esc(label) + '</a>';
            }
            function fmt(value) {
                return Math.round(Number(value) || 0).toLocaleString('en-US');
            }
            function pct(value) {
                return (Number(value) || 0) + '%';
            }
            function metricCells(row) {
                return '<td style="padding:6px 8px;text-align:right">' + fmt(row.count) + '</td>' +
                    '<td style="padding:6px 8px;text-align:right;color:#777">' + pct(row.countShare) + '</td>' +
                    '<td style="padding:6px 8px;text-align:right">' + fmt(row.cores) + '</td>' +
                    '<td style="padding:6px 8px;text-align:right;color:#777">' + pct(row.coresShare) + '</td>' +
                    '<td style="padding:6px 8px;text-align:right">' + fmt(row.ram) + '</td>' +
                    '<td style="padding:6px 8px;text-align:right;color:#777">' + pct(row.ramShare) + '</td>';
            }
            const th = 'padding:6px 8px;text-align:right;color:#777;font-weight:500;border-bottom:1px solid #e6e6e6';
            const state = (typeof Chart !== 'undefined' && typeof Chart.getState === 'function')
                ? (Chart.getState() || {})
                : {};
            const openBusyWf = state.openBusyWf || {};
            const openBusyJob = state.openBusyJob || {};
            const openWait = state.openWait || {};
            const busyHead = '<thead><tr>' +
                '<th style="' + th + ';text-align:left">workflow / job</th>' +
                '<th style="' + th + '">jobs</th>' +
                '<th style="' + th + '">runners</th><th style="' + th + '">%</th>' +
                '<th style="' + th + '">vCPU</th><th style="' + th + '">%</th>' +
                '<th style="' + th + '">RAM, GiB</th><th style="' + th + '">%</th></tr></thead>';
            const busyRows = (cfg.busyWfs || []).map(function(wf) {
                const wfOpen = !!openBusyWf[wf.key];
                const head = '<tr data-id="bw:' + esc(wf.key) + '" style="border-top:1px solid #eee;cursor:pointer">' +
                    '<td style="padding:7px 8px"><span data-id="bw:' + esc(wf.key) + '">' +
                    (wfOpen ? '▾ ' : '▸ ') + '</span><b data-id="bw:' + esc(wf.key) + '">' + esc(wf.workflow) + '</b></td>' +
                    '<td style="padding:7px 8px;text-align:right">' + wf.jobN + '</td>' +
                    metricCells(wf) + '</tr>';
                if (!wfOpen) {
                    return head;
                }
                const jobs = (wf.jobs || []).map(function(job) {
                    const jobOpen = !!openBusyJob[job.key];
                    const jobHead = '<tr data-id="bj:' + esc(job.key) + '" style="cursor:pointer;background:#fafafa">' +
                        '<td style="padding:6px 8px 6px 28px"><span data-id="bj:' + esc(job.key) + '">' +
                        (jobOpen ? '▾ ' : '▸ ') + '</span>' + esc(job.job) + '</td>' +
                        '<td style="padding:6px 8px;text-align:right;color:#bbb">—</td>' +
                        metricCells(job) + '</tr>';
                    if (!jobOpen) {
                        return jobHead;
                    }
                    const inner = (job.runners || []).map(function(row) {
                        const title = String(row.title || '');
                        return '<tr><td style="padding:3px 8px">' + prLink(row) + '</td>' +
                            '<td style="padding:3px 8px">' + esc(title.length > 80 ? title.slice(0, 77) + '…' : title) + '</td>' +
                            '<td style="padding:3px 8px;font-family:ui-monospace,monospace">' + href(row, row.name) + '</td>' +
                            '<td style="padding:3px 8px">' + esc(row.branch || '') + '</td>' +
                            '<td style="padding:3px 8px;text-align:right">' + fmt(row.cores) + '</td>' +
                            '<td style="padding:3px 8px;text-align:right">' + fmt(row.ram) + '</td></tr>';
                    }).join('');
                    return jobHead +
                        '<tr><td colspan="8" style="padding:0 8px 10px 28px">' +
                        '<table style="border-collapse:collapse;width:100%"><thead><tr style="color:#777;text-align:left">' +
                        '<th style="padding:3px 8px">PR</th><th style="padding:3px 8px">PR title</th>' +
                        '<th style="padding:3px 8px">runner</th><th style="padding:3px 8px">target branch</th>' +
                        '<th style="padding:3px 8px;text-align:right">vCPU</th>' +
                        '<th style="padding:3px 8px;text-align:right">RAM, GiB</th></tr></thead><tbody>' +
                        inner + '</tbody></table></td></tr>';
                }).join('');
                return head + jobs;
            }).join('');
            const waitHead = '<thead><tr>' +
                '<th style="' + th + ';text-align:left">workflow</th>' +
                '<th style="' + th + '">waiting</th>' +
                '<th style="' + th + '">oldest</th>' +
                '<th style="' + th + '">vCPU</th>' +
                '<th style="' + th + '">RAM, GiB</th>' +
                '<th style="' + th + '">SSD, GiB</th></tr></thead>';
            const waitRows = (cfg.waitGroups || []).map(function(group) {
                const opened = !!openWait[group.workflow];
                const head = '<tr data-id="wait:' + esc(group.workflow) + '" style="border-top:1px solid #eee;cursor:pointer">' +
                    '<td style="padding:7px 8px"><span data-id="wait:' + esc(group.workflow) + '">' +
                    (opened ? '▾ ' : '▸ ') + '</span><b data-id="wait:' + esc(group.workflow) + '">' +
                    esc(group.workflow) + '</b></td>' +
                    '<td style="padding:7px 8px;text-align:right">' + fmt(group.count) + '</td>' +
                    '<td style="padding:7px 8px;text-align:right">' + ageText(group.age) + '</td>' +
                    '<td style="padding:7px 8px;text-align:right">' + fmt(group.cores) + '</td>' +
                    '<td style="padding:7px 8px;text-align:right">' + fmt(group.ram) + '</td>' +
                    '<td style="padding:7px 8px;text-align:right">' + fmt(group.nrd) + '</td></tr>';
                if (!opened) {
                    return head;
                }
                const inner = (group.jobs || []).map(function(row) {
                    const title = String(row.title || '');
                    return '<tr><td style="padding:3px 8px">' + prLink(row) + '</td>' +
                        '<td style="padding:3px 8px">' + esc(title.length > 80 ? title.slice(0, 77) + '…' : title) + '</td>' +
                        '<td style="padding:3px 8px">' + href(row, row.job) + '</td>' +
                        '<td style="padding:3px 8px">' + esc(row.branch || '') + '</td>' +
                        '<td style="padding:3px 8px;text-align:right">' + ageText(row.age) + '</td>' +
                        '<td style="padding:3px 8px;text-align:right">' + fmt(row.cores) + '</td>' +
                        '<td style="padding:3px 8px;text-align:right">' + fmt(row.ram) + '</td>' +
                        '<td style="padding:3px 8px;text-align:right">' + fmt(row.nrd) + '</td></tr>';
                }).join('');
                return head +
                    '<tr><td colspan="6" style="padding:0 8px 10px 28px">' +
                    '<table style="border-collapse:collapse;width:100%"><thead><tr style="color:#777;text-align:left">' +
                    '<th style="padding:3px 8px">PR</th><th style="padding:3px 8px">PR title</th>' +
                    '<th style="padding:3px 8px">job</th><th style="padding:3px 8px">branch</th>' +
                    '<th style="padding:3px 8px;text-align:right">waiting</th>' +
                    '<th style="padding:3px 8px;text-align:right">vCPU</th>' +
                    '<th style="padding:3px 8px;text-align:right">RAM, GiB</th>' +
                    '<th style="padding:3px 8px;text-align:right">SSD, GiB</th></tr></thead><tbody>' +
                    inner + '</tbody></table></td></tr>';
            }).join('');
            const idleNames = (cfg.idleItems || []).map(function(row) {
                return '<li style="margin:0 16px 2px 0"><span style="color:#888">' +
                    esc(row.cores) + 'c/' + esc(row.ram) + 'G</span> ' + esc(row.name) + '</li>';
            }).join('');
            const box = Number(options && (options.height || options.innerHeight));
            const height = box > 80 ? box : 720;
            return Editor.generateHtml(
                '<div style="height:' + height + 'px;box-sizing:border-box;overflow:auto;padding:12px 16px;font:13px/1.4 ui-sans-serif,system-ui,sans-serif;color:#222;font-variant-numeric:tabular-nums">' +
                '<div style="color:#666;margin-bottom:8px">Latest snapshot ' + esc(cfg.latest) + ' UTC</div>' +
                '<div style="display:flex;gap:8px;flex-wrap:wrap">' +
                '<div style="flex:1.4;min-width:220px;padding:12px 14px;border:1px solid #e6e6e6;border-radius:8px">' +
                '<div style="color:#666;font-size:12px">Newest runner created</div>' +
                '<div style="font-size:22px;font-weight:650;color:' + esc(cfg.tone) + '">' + esc(cfg.newestWhen) + ' UTC</div>' +
                '<div>' + esc(cfg.newestJob) + ' · ' + esc(cfg.newestAge) + ' ago</div></div>' +
                '<div style="flex:1;min-width:140px;padding:12px 14px;border:1px solid #e6e6e6;border-radius:8px">' +
                '<div style="color:#666;font-size:12px">Runners created in the last 24h</div>' +
                '<div style="font-size:28px;font-weight:650">' + cfg.born + '</div></div>' +
                '<div style="flex:1;min-width:140px;padding:12px 14px;border:1px solid #e6e6e6;border-radius:8px">' +
                '<div style="color:#666;font-size:12px">Runners up / sitting idle</div>' +
                '<div style="font-size:28px;font-weight:650">' + cfg.ghrun +
                '<span style="font-size:16px;color:#666"> / ' + cfg.idleN + '</span></div>' +
                '<div style="color:#777;font-size:12px">idle: ' + cfg.idleCores + ' vCPU · ' + cfg.idleRam + ' GiB RAM</div></div>' +
                '</div>' +
                '<div style="margin-top:14px;font-weight:650">' + cfg.busyN + ' busy runners in ' + cfg.wfN + ' workflows</div>' +
                '<div style="color:#777;font-size:12px;margin:2px 0 6px">Open a workflow for builds / postcommit, then a job for the runner link. Shares are of all busy runners.</div>' +
                '<table style="border-collapse:collapse;width:100%">' + busyHead + '<tbody>' +
                (busyRows || '<tr><td style="padding:8px;color:#777" colspan="8">none</td></tr>') +
                '</tbody></table>' +
                '<div style="margin-top:16px;font-weight:650">Jobs waiting for a runner</div>' +
                '<div style="color:#777;font-size:12px;margin:2px 0 6px">Same waiting jobs as Free quota. Idle of the same size take them first. Open a workflow for the GitHub run.' +
                (cfg.hidePostcommit && cfg.hiddenPostcommit
                    ? ' Postcommit is hidden (' + cfg.hiddenPostcommit + ' jobs) — switch Postcommit jobs to show.'
                    : '') +
                '</div>' +
                '<table style="border-collapse:collapse;width:100%">' + waitHead + '<tbody>' +
                (waitRows || '<tr><td style="padding:8px;color:#777" colspan="6">' +
                    (cfg.hidePostcommit && cfg.hiddenPostcommit
                        ? 'No PR / non-postcommit queue. ' + cfg.hiddenPostcommit + ' postcommit jobs are hidden.'
                        : 'queue is empty') +
                    '</td></tr>') +
                '</tbody></table>' +
                '<div style="margin-top:12px;font-weight:650">Idle runners (' + (cfg.idleItems || []).length + '), not running a job</div>' +
                '<ul style="list-style:none;padding:0;margin:6px 0 0;display:flex;flex-wrap:wrap;font:12px/1.45 ui-monospace,monospace">' +
                (idleNames || '<li style="color:#777">none</li>') + '</ul></div>'
            );
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
                const openBusyWf = {};
                const openBusyJob = {};
                const openWait = {};
                Object.keys(prev.openBusyWf || {}).forEach(function(key) { openBusyWf[key] = prev.openBusyWf[key]; });
                Object.keys(prev.openBusyJob || {}).forEach(function(key) { openBusyJob[key] = prev.openBusyJob[key]; });
                Object.keys(prev.openWait || {}).forEach(function(key) { openWait[key] = prev.openWait[key]; });
                if (id.indexOf('bw:') === 0) {
                    const key = id.slice(3);
                    openBusyWf[key] = !openBusyWf[key];
                    Chart.setState({openBusyWf: openBusyWf, openBusyJob: openBusyJob, openWait: openWait});
                    return;
                }
                if (id.indexOf('bj:') === 0) {
                    const key = id.slice(3);
                    openBusyJob[key] = !openBusyJob[key];
                    Chart.setState({openBusyWf: openBusyWf, openBusyJob: openBusyJob, openWait: openWait});
                    return;
                }
                if (id.indexOf('wait:') === 0) {
                    const key = id.slice(5);
                    openWait[key] = !openWait[key];
                    Chart.setState({openBusyWf: openBusyWf, openBusyJob: openBusyJob, openWait: openWait});
                }
            },
        }),
    },
};
