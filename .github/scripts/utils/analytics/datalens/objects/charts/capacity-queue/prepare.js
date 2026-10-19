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

function toDateMs(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value > 1e12 ? value : value * 1000;
    }
    const ms = Date.parse(String(value));
    return Number.isFinite(ms) ? ms : null;
}

function toNumber(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function percentile(values, q) {
    if (!values.length) {
        return null;
    }
    const sorted = values.slice().sort(function(a, b) { return a - b; });
    const idx = (sorted.length - 1) * q;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) {
        return sorted[lo];
    }
    return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

function formatMin(min) {
    if (min === null || min === undefined) {
        return '—';
    }
    if (min < 1) {
        return Math.round(min * 60) + 's';
    }
    if (min < 60) {
        return Math.round(min) + 'm';
    }
    return Math.floor(min / 60) + 'h ' + Math.round(min % 60) + 'm';
}

const HOUR_MS = 60 * 60 * 1000;
const now = Date.now();
const minutes = [];
loadRows().forEach(function(row) {
    if (String(row.step_name || '') !== 'queue') {
        return;
    }
    const start = toDateMs(row.start_ts);
    const sec = toNumber(row.duration_sec);
    if (start === null || sec === null || sec <= 0) {
        return;
    }
    const gotRunner = start + sec * 1000;
    if (gotRunner < now - HOUR_MS || gotRunner > now + 5 * 60 * 1000) {
        return;
    }
    minutes.push(sec / 60);
});

const p90 = percentile(minutes, 0.9);
const tone = p90 === null ? '#888' : (p90 < 5 ? '#59A14F' : (p90 < 20 ? '#F28E2B' : '#E15759'));
const html = '<div style="padding:12px 16px;font:13px/1.35 ui-sans-serif,system-ui,sans-serif;color:#222">' +
    '<div style="color:#666;font-size:12px">Queue p90, last 60 minutes</div>' +
    '<div style="font-size:28px;font-weight:650;letter-spacing:-0.03em;color:' + tone + '">' + formatMin(p90) + '</div>' +
    '<div style="color:#777;font-size:12px">How long a job waited for a runner, 90th percentile. ' +
    'Same queue duration as Queue on the CI tab. ' + minutes.length +
    ' jobs got a runner in this hour.</div></div>';

module.exports = {
    render: Editor.wrapFn({
        args: [{html: html}],
        fn: function(_options, cfg) {
            return Editor.generateHtml(cfg.html || '<div style="padding:16px">no queue data</div>');
        },
    }),
};
