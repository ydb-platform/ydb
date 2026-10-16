const Dataset = require('libs/dataset/v2');

function firstParam(name, fallback) {
    const values = Editor.getParam(name);
    if (values && values.length && values[0] !== '') {
        return String(values[0]);
    }
    return fallback || '';
}

function paramList(name) {
    const values = Editor.getParam(name) || [];
    return values.map(String).filter(function(value) { return value !== ''; });
}

const metric = firstParam('step', '') || firstParam('metric', 'checkout');
const METRIC_LABEL = {
    job: 'total · job',
    checkout: 'job · Checkout',
    clean_ya_cache: '- step · clean ya cache',
    init: '- step · init',
    setup_cache: '- step · setup cache',
    queue: 'job · Queue',
    graph_compare: 'step · graph_compare',
    checkout_head: '- step · checkout head',
    ya_make_try_1: 'step · ya_make --try 1',
    ya_make_try_2: 'step · ya_make --try 2',
    ya_make_try_3: 'step · ya_make --try 3',
    postprocess_try: 'step · postprocess',
    transform_build_results: 'step · transform_build_results',
    fail_checker: 'step · fail_checker',
    generate_summary: 'step · generate_summary',
    s3_sync: 'step · s3 sync',
    upload_tests_results: 'step · upload_tests_results',
    ydbd_cached_build: 'step · ydbd_cached_build',
    ya_build_try_1: '-- substep · ya build try 1',
    ya_build_try_2: '-- substep · ya build try 2',
    ya_build_try_3: '-- substep · ya build try 3',
    ya_build_rebuild_try_1: '-- substep · ya build try 1',
    ya_build_rebuild_try_2: '-- substep · ya build try 2',
    ya_build_rebuild_try_3: '-- substep · ya build try 3',
    ya_cache_download_try_1: '-- substep · ya cache download try 1',
    ya_cache_download_try_2: '-- substep · ya cache download try 2',
    ya_cache_download_try_3: '-- substep · ya cache download try 3',
    ya_cache_upload_try_1: '-- substep · ya cache upload try 1',
    ya_cache_upload_try_2: '-- substep · ya cache upload try 2',
    ya_cache_upload_try_3: '-- substep · ya cache upload try 3',
};
function perTryStep(metricName) {
    const match = /^(prepare_ya_make|postprocess_try|transform_build_results|fail_checker|generate_summary|upload_tests_results|s3_sync)_try_([0-9]+)$/.exec(String(metricName || ''));
    if (!match) {
        return null;
    }
    const titles = {
        prepare_ya_make: 'prepare ya make',
        postprocess_try: 'postprocess',
        transform_build_results: 'transform_build_results',
        fail_checker: 'fail_checker',
        generate_summary: 'generate_summary',
        upload_tests_results: 'upload_tests_results',
        s3_sync: 's3 sync',
    };
    return {base: match[1], n: match[2], title: titles[match[1]]};
}
const perTry = perTryStep(metric);
const metricLabel = perTry ? ('-- substep · ' + perTry.title + ' try ' + perTry.n) : (METRIC_LABEL[metric] || metric);
const p90StepRaw = firstParam('p90_step', 'hour');
const p90Step = p90StepRaw === 'day' || p90StepRaw === 'week' ? p90StepRaw : 'hour';
const pChoices = {50: 0.5, 75: 0.75, 90: 0.9, 95: 0.95, 99: 0.99};
const pKey = pChoices[Number(firstParam('p_q', '90'))] ? Number(firstParam('p_q', '90')) : 90;
const pQuantile = pChoices[pKey];
const pName = 'p' + pKey;
const longRaw = firstParam('long_th', 'p');
const longFixed = longRaw !== 'p' && Number(longRaw) > 0 ? Number(longRaw) : null;

function parseLoadedRows(loaded, sourceName) {
    const block = loaded && loaded[sourceName];
    if (!block) {
        return [];
    }
    if (block.result && block.result.data) {
        const titles = (block.result.fields || []).map(function(f) { return f.title; });
        return (block.result.data.Data || []).map(function(row) {
            const obj = {};
            titles.forEach(function(title, i) {
                obj[title] = row[i];
            });
            return obj;
        });
    }
    return [];
}

function loadRows() {
    try {
        const rows = Dataset.getDatasetRows({datasetName: 'data'});
        if (rows && rows.length) {
            return rows;
        }
    } catch (e) {
        console.log('[prepare] getDatasetRows failed', String(e));
    }
    return parseLoadedRows(Editor.getLoadedData(), 'data');
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
    const hours = Math.floor(min / 60);
    const minutes = Math.round(min % 60);
    return hours + 'h ' + minutes + 'm';
}

function pad2(n) {
    return (n < 10 ? '0' : '') + n;
}

function weekStartKey(ms) {
    const date = new Date(ms);
    const mondayOffset = (date.getDay() + 6) % 7;
    date.setDate(date.getDate() - mondayOffset);
    return date.getFullYear() + '-' + pad2(date.getMonth() + 1) + '-' + pad2(date.getDate());
}

function bucketKey(ms) {
    const date = new Date(ms);
    const day = date.getFullYear() + '-' + pad2(date.getMonth() + 1) + '-' + pad2(date.getDate());
    if (p90Step === 'hour') {
        return day + 'T' + pad2(date.getHours());
    }
    if (p90Step === 'week') {
        return weekStartKey(ms);
    }
    return day;
}

function bucketStart(key) {
    if (key.length === 13) {
        return new Date(key.slice(0, 10) + 'T' + key.slice(11) + ':00:00').getTime();
    }
    return new Date(key.slice(0, 10) + 'T12:00:00').getTime();
}

function boundMs(token, endOfDay) {
    if (!token) {
        return null;
    }
    if (token.indexOf('__relative_') === 0) {
        const match = token.slice('__relative_'.length).match(/^([+-]?\d+)([dwMyh])/);
        if (!match) {
            return null;
        }
        const amount = Number(match[1]);
        const unit = match[2];
        const date = new Date();
        if (unit === 'h') {
            date.setUTCHours(date.getUTCHours() + amount);
            return date.getTime();
        }
        if (unit === 'd') {
            date.setUTCDate(date.getUTCDate() + amount);
        } else if (unit === 'w') {
            date.setUTCDate(date.getUTCDate() + amount * 7);
        } else if (unit === 'M') {
            date.setUTCMonth(date.getUTCMonth() + amount);
        } else if (unit === 'y') {
            date.setUTCFullYear(date.getUTCFullYear() + amount);
        }
        if (endOfDay) {
            date.setUTCHours(23, 59, 59, 999);
        } else {
            date.setUTCHours(0, 0, 0, 0);
        }
        return date.getTime();
    }
    const parsed = Date.parse(token.length <= 10 ? token + (endOfDay ? 'T23:59:59.999Z' : 'T00:00:00Z') : token);
    return Number.isFinite(parsed) ? parsed : null;
}

function splitInterval(raw) {
    const value = String(raw || '');
    if (value.indexOf('__interval_') !== 0) {
        return null;
    }
    const body = value.slice('__interval_'.length);
    const marker = '__relative_';
    const first = body.indexOf(marker);
    const second = first >= 0 ? body.indexOf(marker, first + marker.length) : -1;
    let startToken = '';
    let endToken = '';
    if (first === 0 && second >= 0) {
        startToken = body.slice(0, second).replace(/_+$/, '');
        endToken = body.slice(second);
    } else if (first > 0) {
        startToken = body.slice(0, first).replace(/_+$/, '');
        endToken = body.slice(first);
    } else {
        const splitAt = body.indexOf('_', body.indexOf('T'));
        if (splitAt < 0) {
            return null;
        }
        startToken = body.slice(0, splitAt);
        endToken = body.slice(splitAt + 1);
    }
    return {start: boundMs(startToken, false), end: boundMs(endToken, true)};
}

const workflowFilter = paramList('workflow').concat(paramList('wf')).filter(function(value, index, list) {
    return list.indexOf(value) === index;
});
const jobFilter = paramList('job_name');
const branchFilter = paramList('branch').length ? paramList('branch') : paramList('tl_branch');
const presetFilter = paramList('tl_preset');
const interval = splitInterval(firstParam('interval', ''));
const dateFrom = firstParam('tl_from', '');
const dateTo = firstParam('tl_to', '');
const fromMs = interval ? interval.start : (dateFrom ? Date.parse(dateFrom + 'T00:00:00Z') : null);
const toMs = interval ? interval.end : (dateTo ? Date.parse(dateTo + 'T23:59:59.999Z') : null);
const rangeStart = (Number.isFinite(fromMs) && Number.isFinite(toMs) && fromMs > toMs) ? toMs : fromMs;
const rangeEnd = (Number.isFinite(fromMs) && Number.isFinite(toMs) && fromMs > toMs) ? fromMs : toMs;
const loaded = loadRows();
const raw = loaded.filter(function(row) {
    if (workflowFilter.length && workflowFilter.indexOf(String(row.workflow || '')) === -1) {
        return false;
    }
    if (jobFilter.length && jobFilter.indexOf(String(row.job_name || '')) === -1) {
        return false;
    }
    if (branchFilter.length && branchFilter.indexOf(String(row.branch || '')) === -1) {
        return false;
    }
    if (perTry && String(row.ya_try || '') !== perTry.n) {
        return false;
    }
    return true;
});
function phaseTryNumber(metricName) {
    const ya = /^ya_(?:build_rebuild|build|tests|cache_download|cache_upload)_try_([0-9]+)$/.exec(String(metricName || ''));
    return ya ? ya[1] : '';
}
const selectedPhaseTry = phaseTryNumber(metric);
let rowsForPoints = raw;
if (selectedPhaseTry) {
    const grouped = {};
    raw.forEach(function(row) {
        if (String(row.ya_try || '') !== selectedPhaseTry) {
            return;
        }
        const key = String(row.run_id || '') + '|' + String(row.github_job_id || '');
        if (!grouped[key]) {
            grouped[key] = Object.assign({}, row);
            grouped[key].duration_sec = 0;
            grouped[key].duration_ms = 0;
        }
        grouped[key].duration_sec += Number(row.duration_sec) || 0;
        grouped[key].duration_ms += Number(row.duration_ms) || 0;
        const start = toDateMs(row.start_ts);
        const current = toDateMs(grouped[key].start_ts);
        if (current === null || (start !== null && start < current)) {
            grouped[key].start_ts = row.start_ts;
        }
    });
    rowsForPoints = Object.keys(grouped).map(function(key) { return grouped[key]; });
}
const points = [];
rowsForPoints.forEach(function(row, index) {
    const start = toDateMs(row.start_ts);
    const sec = toNumber(row.duration_sec);
    if (start === null || sec === null || sec <= 0) {
        return;
    }
    if (Number.isFinite(rangeStart) && start < rangeStart) {
        return;
    }
    if (Number.isFinite(rangeEnd) && start > rangeEnd) {
        return;
    }
    points.push({
        id: metric + '-' + index,
        start: start,
        bucket: bucketKey(start),
        minutes: sec / 60,
        preset: String(row.build_preset || 'other'),
        workflow: String(row.workflow || 'workflow'),
        job: String(row.job_name || ''),
        branch: String(row.branch || ''),
        pr: String(row.pr_number || ''),
        runId: String(row.run_id || ''),
        jobId: String(row.github_job_id || ''),
        url: String(row.run_url || ''),
        commit: String(row.commit || ''),
        kind: String(row.event_name || ''),
        conclusion: String(row.conclusion || ''),
    });
});

const p90All = percentile(points.map(function(p) { return p.minutes; }), pQuantile);
const SLOW_MIN = longFixed !== null ? longFixed : (p90All === null ? Infinity : p90All);
const slowByP = longFixed === null;

const byBucket = {};
points.forEach(function(p) {
    const key = bucketKey(p.start);
    if (!byBucket[key]) {
        byBucket[key] = [];
    }
    byBucket[key].push(p.minutes);
});
const series = Object.keys(byBucket).sort().map(function(key) {
    const values = byBucket[key];
    return {
        key: key,
        start: bucketStart(key),
        p50: percentile(values, 0.5),
        p90: percentile(values, pQuantile),
        max: Math.max.apply(null, values),
        n: values.length,
        slow: values.filter(function(v) { return v >= SLOW_MIN; }).length,
    };
});

const first = series[0] || null;
const last = series.length ? series[series.length - 1] : null;
const startValue = first ? first.p90 : null;
const endValue = last ? last.p90 : null;
const delta = (startValue !== null && endValue !== null) ? endValue - startValue : null;
const deltaPct = (delta !== null && startValue) ? (delta / startValue) * 100 : null;
const startShare = first && first.n ? first.slow / first.n : null;
const endShare = last && last.n ? last.slow / last.n : null;
const shareDelta = (startShare !== null && endShare !== null) ? endShare - startShare : null;

function bucketLabel(item) {
    if (!item) {
        return '—';
    }
    const key = String(item.key || '');
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    function dayPart(ms) {
        const date = new Date(ms);
        return date.getDate() + ' ' + months[date.getMonth()];
    }
    function timePart(ms) {
        const date = new Date(ms);
        return (date.getHours() < 10 ? '0' : '') + date.getHours() + ':00';
    }
    let fromMs;
    let toMs;
    if (p90Step === 'hour') {
        fromMs = new Date(key.slice(0, 10) + 'T' + key.slice(11) + ':00:00').getTime();
        toMs = fromMs + 3600 * 1000;
        return dayPart(fromMs) + ' ' + timePart(fromMs) + '-' + timePart(toMs);
    }
    fromMs = new Date(key.slice(0, 10) + 'T00:00:00').getTime();
    toMs = fromMs + (p90Step === 'week' ? 7 : 1) * 24 * 3600 * 1000;
    return dayPart(fromMs) + '-' + dayPart(toMs);
}

const chartConfig = {
    pName: pName,
    window: p90Step,
    metricLabel: metricLabel,
    startWindow: bucketLabel(first),
    endWindow: bucketLabel(last),
    startValue: startValue,
    endValue: endValue,
    startN: first ? first.n : 0,
    endN: last ? last.n : 0,
    startSlow: first ? first.slow : 0,
    endSlow: last ? last.slow : 0,
    startShare: startShare,
    endShare: endShare,
    shareDelta: shareDelta,
    slowLabel: slowByP
        ? (pName + ' ' + formatMin(SLOW_MIN === Infinity ? null : SLOW_MIN))
        : formatMin(SLOW_MIN),
    delta: delta,
    deltaPct: deltaPct,
};

module.exports = {
    render: Editor.wrapFn({
        args: [chartConfig],
        fn: function(options, cfg) {
            function formatMin(min) {
                if (min === null || min === undefined || !Number.isFinite(min)) {
                    return '—';
                }
                if (min < 1) {
                    return Math.round(min * 60) + 's';
                }
                if (min < 60) {
                    return Math.round(min) + 'm';
                }
                const hours = Math.floor(min / 60);
                const minutes = Math.round(min % 60);
                return hours + 'h ' + minutes + 'm';
            }
            if (cfg.startValue === null || cfg.endValue === null) {
                return Editor.generateHtml('<div style="padding:12px;font:13px ui-sans-serif,system-ui,sans-serif;color:#888">No ' + (cfg.pName || 'p') + ' for these filters.</div>');
            }
            function pctShare(value) {
                if (value === null || value === undefined || !Number.isFinite(value)) {
                    return '—';
                }
                return Math.round(value * 100) + '%';
            }
            function chip(flat, down, text) {
                const color = flat ? '#555' : (down ? '#2f7d32' : '#c0392b');
                return '<b style="color:' + color + '">' + text + '</b>';
            }
            const title = String(cfg.metricLabel || '').replace(/^[\s\-]*(substep|step|job)\s*·\s*/i, '');
            const down = cfg.delta < 0;
            const flat = Math.abs(cfg.delta) < 0.05;
            const sign = cfg.delta > 0 ? '+' : (cfg.delta < 0 ? '-' : '');
            const pDiff = sign + formatMin(Math.abs(cfg.delta)) +
                (cfg.deltaPct === null ? '' : ' (' + sign + Math.abs(Math.round(cfg.deltaPct)) + '%)');
            const shareDown = (cfg.shareDelta || 0) < 0;
            const shareFlat = cfg.shareDelta === null || Math.abs(cfg.shareDelta) < 0.005;
            const shareSign = (cfg.shareDelta || 0) > 0 ? '+' : ((cfg.shareDelta || 0) < 0 ? '-' : '');
            const shareDiff = cfg.shareDelta === null
                ? '—'
                : (shareSign + Math.abs(Math.round(cfg.shareDelta * 100)) + ' pp');
            const startShare = pctShare(cfg.startShare) + ' (' + (cfg.startSlow || 0) + '/' + (cfg.startN || 0) + ')';
            const endShare = pctShare(cfg.endShare) + ' (' + (cfg.endSlow || 0) + '/' + (cfg.endN || 0) + ')';
            const startWindow = cfg.startWindow || '—';
            const endWindow = cfg.endWindow || '—';
            const html = [
                '<div style="padding:8px 12px;font:13px/1.45 ui-sans-serif,system-ui,sans-serif;color:#222">',
                '<div style="color:#888;margin-bottom:8px">' + title + ' · ' + (cfg.window || '') +
                    ' · long >= ' + (cfg.slowLabel || '') + '</div>',
                '<div><span style="color:#888">first window</span> <b>' + startWindow + '</b></div>',
                '<div><span style="color:#888">last window</span> <b>' + endWindow + '</b></div>',
                '<div style="margin-top:6px">' + (cfg.pName || 'p') + ': <b>' +
                    formatMin(cfg.startValue) + '</b> -&gt; <b>' + formatMin(cfg.endValue) +
                    '</b> Δ ' + chip(flat, down, pDiff) + '</div>',
                '<div>count share: <b>' + startShare + '</b> -&gt; <b>' + endShare +
                    '</b> Δ ' + chip(shareFlat, shareDown, shareDiff) + '</div>',
                '</div>',
            ].join('');
            return Editor.generateHtml(html);
        },
    }),
};
