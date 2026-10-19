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

const splitBy = firstParam('cap_by', 'workflow') === 'branch' ? 'branch' : 'workflow';
const hidePostcommit = firstParam('cap_pc', 'hide') !== 'show';
const zone = firstParam('cap_zone', 'ru-central1-d');

function isPostcommit(row) {
    return /postcommit/i.test(String(row.job_name || '')) || /postcommit/i.test(String(row.workflow || ''));
}

function bucket(row) {
    if (row.state !== 'busy') {
        if (row.state === 'idle') {
            return 'idle, no job';
        }
        if (row.state === 'other') {
            return 'other VMs';
        }
        if (row.state === 'queued') {
            return 'queued, runner already assigned';
        }
        return row.state || 'unknown';
    }
    if (splitBy === 'branch') {
        return row.branch ? String(row.branch) : 'no target branch';
    }
    const job = String(row.job_name || '');
    const workflow = String(row.workflow || '');
    if (/postcommit/i.test(job) || /postcommit/i.test(workflow)) {
        return 'postcommit';
    }
    return workflow || 'unnamed workflow';
}

const PALETTE = ['#4E79A7', '#F28E2B', '#E15759', '#76B7B2', '#59A14F', '#EDC948', '#B07AA1', '#9C755F', '#FF9DA7', '#BAB0AC'];

const rows = loadRows();
const byTime = {};
rows.forEach(function(row) {
    if (hidePostcommit && row.state === 'busy' && isPostcommit(row)) {
        return;
    }
    if (zone !== 'all' && String(row.zone || '') !== zone) {
        return;
    }
    const t = ms(row.sampled_at);
    if (t == null) {
        return;
    }
    const slot = byTime[t] || (byTime[t] = {});
    const key = bucket(row);
    const cell = slot[key] || (slot[key] = {ram: 0, cores: 0, runners: 0});
    cell.ram += num(row.ram_gib) || 0;
    cell.cores += num(row.cores) || 0;
    cell.runners += 1;
});
const times = Object.keys(byTime).map(Number).sort(function(a, b) { return a - b; }).slice(-48);
const totals = {};
times.forEach(function(t) {
    Object.keys(byTime[t]).forEach(function(key) {
        totals[key] = (totals[key] || 0) + byTime[t][key].ram;
    });
});
const keys = Object.keys(totals).sort(function(a, b) { return totals[b] - totals[a]; });
const color = {};
keys.forEach(function(key, i) { color[key] = PALETTE[i % PALETTE.length]; });

function partsAt(t) {
    const slot = byTime[t] || {};
    return Object.keys(slot).map(function(key) {
        return {key: key, ram: slot[key].ram, cores: slot[key].cores, runners: slot[key].runners};
    }).sort(function(a, b) { return b.ram - a.ram; });
}

let maxRam = 1;
const snapshots = times.map(function(t) {
    const parts = partsAt(t);
    const total = parts.reduce(function(acc, part) { return acc + part.ram; }, 0);
    if (total > maxRam) {
        maxRam = total;
    }
    return {
        id: String(t),
        label: new Date(t).toISOString().slice(0, 16).replace('T', ' '),
        tick: new Date(t).toISOString().slice(11, 16),
        total: total,
        parts: parts,
    };
});

const splitTitle = splitBy === 'branch' ? 'target branch' : 'workflow';

module.exports = {
    render: Editor.wrapFn({
        args: [{
            snapshots: snapshots,
            keys: keys,
            color: color,
            maxRam: maxRam,
            splitTitle: splitTitle,
        }],
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
            function share(part, total) {
                return total ? Math.round(part / total * 100) : 0;
            }
            const snapshots = (cfg && cfg.snapshots) || [];
            const keys = (cfg && cfg.keys) || [];
            const color = (cfg && cfg.color) || {};
            const maxRam = (cfg && cfg.maxRam) || 1;
            const splitTitle = (cfg && cfg.splitTitle) || 'workflow';
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

            const columns = snapshots.map(function(snapshot, index) {
                const bars = (snapshot.parts || []).map(function(part) {
                    const h = Math.max(0, part.ram / maxRam * 100);
                    return '<div data-id="' + esc(snapshot.id) + '" style="height:' + h.toFixed(2) +
                        '%;background:' + (color[part.key] || '#888') + '"></div>';
                }).join('');
                const on = snapshot.id === selectedId;
                const align = index > snapshots.length * 0.65 ? 'right:0' : 'left:0';
                const tipLines = (snapshot.parts || []).map(function(part) {
                    return '<div style="display:flex;justify-content:space-between;gap:16px">' +
                        '<span><i style="display:inline-block;width:8px;height:8px;border-radius:2px;background:' +
                        (color[part.key] || '#888') + ';margin-right:6px"></i>' + esc(part.key) + '</span>' +
                        '<span>' + part.runners + ' · ' + fmt(part.cores) + ' vCPU · ' + fmt(part.ram) + ' GiB</span></div>';
                }).join('');
                return '<div data-id="' + esc(snapshot.id) + '" class="cap-col" style="position:relative;flex:1;min-width:0;display:flex;flex-direction:column;height:100%;cursor:pointer;outline:' +
                    (on ? '2px solid #222' : 'none') + ';outline-offset:-2px">' +
                    '<div data-id="' + esc(snapshot.id) + '" style="flex:1;display:flex;flex-direction:column-reverse">' + bars + '</div>' +
                    '<div data-id="' + esc(snapshot.id) + '" style="height:16px;font-size:10px;color:#888;text-align:center">' +
                    (snapshots.length > 16 ? '' : esc(snapshot.tick)) + '</div>' +
                    '<div class="cap-tip" style="display:none;position:absolute;z-index:5;' + align +
                    ';bottom:20px;background:#fff;border:1px solid #e4e4e4;box-shadow:0 6px 18px rgba(0,0,0,.12);padding:8px 10px;min-width:220px;font-size:12px">' +
                    '<div style="font-weight:650;margin-bottom:4px">' + esc(snapshot.label) + ' UTC</div>' +
                    tipLines + '</div></div>';
            }).join('');

            const legend = keys.map(function(key) {
                return '<span style="display:inline-flex;align-items:center;gap:4px;margin:0 10px 4px 0">' +
                    '<i style="width:8px;height:8px;border-radius:2px;background:' + (color[key] || '#888') +
                    ';display:inline-block"></i>' + esc(key) + '</span>';
            }).join('');

            const selectedTotal = selected ? selected.total : 0;
            const selectedCores = selected
                ? (selected.parts || []).reduce(function(acc, part) { return acc + (part.cores || 0); }, 0)
                : 0;
            const selectedRunners = selected
                ? (selected.parts || []).reduce(function(acc, part) { return acc + (part.runners || 0); }, 0)
                : 0;
            const tableRows = selected
                ? (selected.parts || []).map(function(part) {
                    return '<tr><td style="padding:6px 10px">' + esc(part.key) + '</td>' +
                        '<td style="padding:6px 10px;text-align:right">' + fmt(part.runners) + '</td>' +
                        '<td style="padding:6px 10px;text-align:right;color:#777">' + share(part.runners, selectedRunners) + '%</td>' +
                        '<td style="padding:6px 10px;text-align:right">' + fmt(part.cores) + '</td>' +
                        '<td style="padding:6px 10px;text-align:right;color:#777">' + share(part.cores, selectedCores) + '%</td>' +
                        '<td style="padding:6px 10px;text-align:right">' + fmt(part.ram) + '</td>' +
                        '<td style="padding:6px 10px;text-align:right;color:#777">' + share(part.ram, selectedTotal) + '%</td></tr>';
                }).join('')
                : '';
            const when = selected ? selected.label + ' UTC' : 'no snapshots yet';

            const box = Number(options && (options.height || options.innerHeight));
            const height = box > 80 ? box : 560;
            return Editor.generateHtml(
                '<style>.cap-col:hover .cap-tip{display:block !important}</style>' +
                '<div style="height:' + height + 'px;box-sizing:border-box;overflow:auto;padding:12px 16px;font:13px/1.4 ui-sans-serif,system-ui,sans-serif;color:#222;font-variant-numeric:tabular-nums">' +
                '<div style="display:flex;justify-content:space-between;gap:12px;margin-bottom:8px">' +
                '<div>Bars are RAM. Table is runners / vCPU / RAM, split by <b>' + esc(splitTitle) + '</b>.</div>' +
                '<div style="color:#777">' + esc(when) + '</div></div>' +
                '<div style="height:220px;display:flex;gap:2px;align-items:stretch">' + columns + '</div>' +
                '<div style="margin-top:8px;color:#444;font-size:12px">' + legend + '</div>' +
                '<div style="margin-top:4px;color:#888;font-size:12px">Click a column to update the table. Hover still shows the same snapshot.</div>' +
                '<div style="margin-top:12px;font-weight:650">Who holds the pool (' +
                esc(splitTitle) + ') — ' + esc(when) + '</div>' +
                '<table style="border-collapse:collapse;width:100%;margin-top:6px"><thead><tr style="color:#777;text-align:right;border-bottom:1px solid #e6e6e6">' +
                '<th style="padding:6px 10px;text-align:left">' + esc(splitTitle) + '</th>' +
                '<th style="padding:6px 10px">runners</th><th style="padding:6px 10px">%</th>' +
                '<th style="padding:6px 10px">vCPU</th><th style="padding:6px 10px">%</th>' +
                '<th style="padding:6px 10px">RAM, GiB</th><th style="padding:6px 10px">%</th></tr></thead><tbody>' +
                (tableRows || '<tr><td style="padding:6px 10px;color:#777">no runners</td></tr>') +
                '</tbody></table></div>'
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
                    id = node.getAttribute && node.getAttribute('data-id') || '';
                    if (id) {
                        break;
                    }
                    node = node.parentNode;
                }
                if (!id) {
                    return;
                }
                if (typeof Chart !== 'undefined' && typeof Chart.setState === 'function') {
                    Chart.setState({selectedId: id});
                }
            },
        }),
    },
};
