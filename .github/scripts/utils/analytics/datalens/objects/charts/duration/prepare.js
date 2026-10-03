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
    'Build and test': 'job · Build and test',
    graph_compare: '- step · graph_compare',
    checkout_head: '- step · checkout head',
    ya_make_try_1: '- step · ya_make --try 1',
    ya_make_try_2: '- step · ya_make --try 2',
    ya_make_try_3: '- step · ya_make --try 3',
    ya_build_try_1: '-- substep · ya build try 1',
    ya_build_rebuild_try_1: '-- substep · ya build try 1',
    ya_tests_try_1: '-- substep · tests try 1',
    ya_build_try_2: '-- substep · ya build try 2',
    ya_build_rebuild_try_2: '-- substep · ya build try 2',
    ya_tests_try_2: '-- substep · tests try 2',
    ya_build_try_3: '-- substep · ya build try 3',
    ya_build_rebuild_try_3: '-- substep · ya build try 3',
    ya_tests_try_3: '-- substep · tests try 3',
    ya_cache_download_try_1: '-- substep · ya cache download try 1',
    ya_cache_download_try_2: '-- substep · ya cache download try 2',
    ya_cache_download_try_3: '-- substep · ya cache download try 3',
    ya_cache_upload_try_1: '-- substep · ya cache upload try 1',
    ya_cache_upload_try_2: '-- substep · ya cache upload try 2',
    ya_cache_upload_try_3: '-- substep · ya cache upload try 3',
    postprocess_try: '- step · postprocess',
    transform_build_results: '- step · transform_build_results',
    fail_checker: '- step · fail_checker',
    generate_summary: '- step · generate_summary',
    s3_sync: '- step · s3 sync',
    upload_tests_results: '- step · upload_tests_results',
    ydbd_cached_build: '- step · ydbd_cached_build',
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
const viewRaw = firstParam('tl_view', 'all');
const viewMode = (
    viewRaw === 'points' || viewRaw === 'p' || viewRaw === 'share_count' || viewRaw === 'share_time'
) ? viewRaw : 'all';

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
    return new Date(key.slice(0, 10) + 'T00:00:00').getTime();
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
    return true;
});

function phaseTryNumber(metricName) {
    const name = String(metricName || '');
    const ya = /^ya_(?:build_rebuild|build|tests|cache_download|cache_upload)_try_([0-9]+)$/.exec(name);
    if (ya) {
        return ya[1];
    }
    const other = perTryStep(name);
    return other ? other.n : '';
}
const selectedPhaseTry = phaseTryNumber(metric);
let timelineRows = raw;
if (selectedPhaseTry) {
    const grouped = {};
    raw.forEach(function(row) {
        if (String(row.ya_try || '') !== selectedPhaseTry) {
            return;
        }
        const key = String(row.ci_run_id || row.run_id || '') + '|' + String(row.run_attempt || '') + '|' + String(row.github_job_id || '');
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
    timelineRows = Object.keys(grouped).map(function(key) { return grouped[key]; });
}
const points = [];
timelineRows.forEach(function(row, index) {
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
        id: String(metric || 'step').replace(/[^A-Za-z0-9_-]/g, '_') + '-' + index,
        start: start,
        bucket: bucketKey(start),
        minutes: sec / 60,
        preset: String(row.build_preset || 'other'),
        workflow: String(row.workflow || 'workflow'),
        job: String(row.job_name || ''),
        branch: String(row.branch || ''),
        pr: String(row.pr_number || ''),
        runId: String(row.ci_run_id || row.run_id || ''),
        attempt: String(row.run_attempt || ''),
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
        slowMinutes: values.filter(function(v) { return v >= SLOW_MIN; }).reduce(function(sum, v) { return sum + v; }, 0),
        totalMinutes: values.reduce(function(sum, v) { return sum + v; }, 0),
    };
});

const COLORS = ['#4E79A7', '#E15759', '#59A14F', '#F28E2B', '#B07AA1', '#76B7B2', '#EDC948', '#FF9DA7', '#9C755F', '#BAB0AC'];
function uniqueNames(items, key) {
    const out = [];
    items.forEach(function(item) {
        const name = item[key] || 'other';
        if (out.indexOf(name) === -1) {
            out.push(name);
        }
    });
    return out;
}
const colorBy = uniqueNames(points, 'workflow').length > 1 ? 'workflow' : 'preset';
const colorNames = uniqueNames(points, colorBy);
const colorOf = {};
colorNames.forEach(function(name, index) {
    colorOf[name] = COLORS[index % COLORS.length];
});

const slowCount = points.filter(function(p) { return p.minutes >= SLOW_MIN; }).length;
const totalMinutes = points.reduce(function(sum, p) { return sum + p.minutes; }, 0);
const slowMinutes = points.filter(function(p) { return p.minutes >= SLOW_MIN; })
    .reduce(function(sum, p) { return sum + p.minutes; }, 0);
const slowShareCount = points.length ? slowCount / points.length : 0;
const slowShareTime = totalMinutes ? slowMinutes / totalMinutes : 0;

if (typeof Editor !== 'undefined' && typeof Editor.updateConfig === 'function') {
    try {
        Editor.updateConfig({
            actionParams: {enable: true, fields: ['gantt_run']},
        });
    } catch (e) {
        console.log('[prepare] updateConfig', String(e));
    }
}

function captionDay(ms) {
    if (!Number.isFinite(ms)) {
        return '';
    }
    const iso = new Date(ms).toISOString();
    const clock = iso.slice(11, 16);
    if (clock === '00:00' || clock === '23:59') {
        return iso.slice(0, 10);
    }
    return iso.slice(0, 10) + ' ' + clock;
}

const rangeCaption = (Number.isFinite(rangeStart) || Number.isFinite(rangeEnd))
    ? ((Number.isFinite(rangeStart) ? captionDay(rangeStart) : 'start') + ' – ' + (Number.isFinite(rangeEnd) ? captionDay(rangeEnd) : 'now'))
    : 'all dates';

const filterCaption = [
    metricLabel,
    rangeCaption,
    'workflow ' + (workflowFilter.length ? workflowFilter.join(', ') : 'all'),
    'target branch ' + (branchFilter.join(', ') || 'all'),
    'build ' + (presetFilter.join(', ') || 'all'),
    jobFilter.length ? jobFilter.join(', ') : '',
].join(' · ');

const chartConfig = {
    metricLabel: metricLabel,
    filterCaption: filterCaption,
    points: points,
    series: series,
    p90Step: p90Step,
    pName: pName,
    viewMode: viewMode,
    pKey: String(pKey),
    slowMin: SLOW_MIN,
    slowByP: slowByP,
    slowLabel: slowByP
        ? (pName + ' ' + formatMin(SLOW_MIN === Infinity ? null : SLOW_MIN))
        : formatMin(SLOW_MIN),
    slowCount: slowCount,
    slowShareCount: slowShareCount,
    slowShareTime: slowShareTime,
    totalMinutes: totalMinutes,
    p90All: p90All,
    rangeStart: Number.isFinite(rangeStart) ? rangeStart : null,
    rangeEnd: Number.isFinite(rangeEnd) ? rangeEnd : null,
    selectedId: firstParam('selected_id', ''),
    runId: firstParam('run_id', ''),
    tlBranch: branchFilter,
    tlPreset: presetFilter,
    colorBy: colorBy,
    colorNames: colorNames,
    colorOf: colorOf,
    kind: firstParam('kind', ''),
    metric: metric,
    palette: {
        relwithdebinfo: '#4E79A7',
        'release-asan': '#E15759',
        other: '#76B7B2',
    },
};

module.exports = {
    render: Editor.wrapFn({
        libs: ['d3@7.9.0'],
        args: [chartConfig],
        fn: function(options, cfg) {
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
            try {
                const size = options && options.size ? options.size : options;
                const rawW = Number(size && (size.width || size.innerWidth));
                const rawH = Number(size && (size.height || size.innerHeight));
                const width = rawW > 50 ? rawW : 800;
                const height = rawH > 80 ? rawH : 360;
                const legendNamesEarly = cfg.colorNames || [];
                function legendItemWidth(name) {
                    return Math.max(88, String(name).length * 6.2 + 22);
                }
                const legendMax = Math.max(160, width - 320);
                let legendRows = 1;
                let legendCursor = 0;
                legendNamesEarly.forEach(function(name) {
                    const itemW = legendItemWidth(name);
                    if (legendCursor > 0 && legendCursor + itemW > legendMax) {
                        legendRows += 1;
                        legendCursor = itemW;
                    } else {
                        legendCursor += itemW;
                    }
                });
                const margin = {top: 26 + legendRows * 16 + 16, right: 64, bottom: 56, left: 52};
                const w = width - margin.left - margin.right;
                const h = height - margin.top - margin.bottom;
                const svg = d3.create('svg')
                    .attr('viewBox', '0 0 ' + width + ' ' + height)
                    .attr('width', '100%')
                    .attr('height', '100%')
                    .attr('preserveAspectRatio', 'none')
                    .style('display', 'block');
                const allPoints = cfg.points || [];
                const chartState = (typeof Chart !== 'undefined' && typeof Chart.getState === 'function')
                    ? (Chart.getState() || {})
                    : {};
                let selectedId = cfg.selectedId || chartState.selectedId || '';
                const metricKey = String(cfg.metric || '').replace(/[^A-Za-z0-9_-]/g, '_');
                if (selectedId && metricKey && selectedId.indexOf(metricKey + '-') !== 0) {
                    selectedId = '';
                }
                let selected = null;
                for (let i = 0; i < allPoints.length; i++) {
                    if (allPoints[i] && allPoints[i].id === selectedId) {
                        selected = allPoints[i];
                        break;
                    }
                }
                if (!selected && cfg.runId) {
                    for (let i = 0; i < allPoints.length; i++) {
                        if (allPoints[i] && String(allPoints[i].runId) === String(cfg.runId)) {
                            selected = allPoints[i];
                            break;
                        }
                    }
                }
                if (!allPoints.length) {
                    svg.append('text')
                        .attr('x', width / 2)
                        .attr('y', height / 2)
                        .attr('text-anchor', 'middle')
                        .attr('fill', '#888')
                        .text('No ' + cfg.metricLabel + ' rows.');
                    return Editor.generateHtml(svg.node());
                }

                function pct(value) {
                    const p = (value || 0) * 100;
                    if (p <= 0) {
                        return '0%';
                    }
                    if (p >= 99.95) {
                        return '100%';
                    }
                    if (p >= 99.5) {
                        return p.toFixed(1) + '%';
                    }
                    return Math.round(p) + '%';
                }
                const shareCaption = 'total ' + allPoints.length + ' ops  ·  ' +
                    cfg.slowCount + ' ops ≥ ' + (cfg.slowLabel || formatMin(cfg.slowMin)) + ' (' +
                    pct(cfg.slowShareCount) + ' count, ' + pct(cfg.slowShareTime) + ' time)';

                let showPoints = cfg.viewMode !== 'p';
                const showLine = cfg.viewMode !== 'points';
                const showDist = cfg.viewMode === 'share_count' || cfg.viewMode === 'share_time';
                const distByTime = cfg.viewMode === 'share_time';
                const MAX_DOTS = 8000;
                if (showPoints && allPoints.length > MAX_DOTS) {
                    if (!showDist) {
                        svg.append('text')
                            .attr('x', width / 2)
                            .attr('y', height / 2 - 16)
                            .attr('text-anchor', 'middle')
                            .attr('fill', '#333')
                            .attr('font-size', '16px')
                            .attr('font-weight', '600')
                            .text(allPoints.length + ' points, limit is ' + MAX_DOTS);
                        svg.append('text')
                            .attr('x', width / 2)
                            .attr('y', height / 2 + 12)
                            .attr('text-anchor', 'middle')
                            .attr('fill', '#666')
                            .attr('font-size', '13px')
                            .text('Narrow the filters: dates, workflow, job, branch or build.');
                        return Editor.generateHtml(svg.node());
                    }
                    showPoints = false;
                }
                const drawPoints = allPoints;

                const pointExtent = d3.extent(allPoints, function(d) { return d.start; });
                const xDomainStart = cfg.rangeStart !== null ? Math.min(cfg.rangeStart, pointExtent[0]) : pointExtent[0];
                const xDomainEnd = cfg.rangeEnd !== null ? Math.max(cfg.rangeEnd, pointExtent[1]) : pointExtent[1];
                const x = d3.scaleTime()
                    .domain([xDomainStart, xDomainEnd])
                    .range([0, w]);
                const lineMax = d3.max(cfg.series || [], function(d) { return d.p90; }) || 0;
                const dataMax = showPoints
                    ? (d3.max(allPoints, function(d) { return d.minutes; }) || 0)
                    : lineMax;
                const includeSlow = showPoints && dataMax >= cfg.slowMin * 0.6;
                const yMax = showPoints
                    ? Math.max(includeSlow ? cfg.slowMin : 0, dataMax, dataMax < 1 ? 1 / 60 : 0)
                    : Math.max(lineMax, lineMax < 1 ? 1 / 60 : 0);
                const y = d3.scaleLinear().domain([0, yMax * 1.12 || 1]).range([h, 0]);
                const palette = cfg.palette || {};
                function formatTick(v) {
                    if (yMax < 2) {
                        return Math.round(v * 60) + 's';
                    }
                    return v + 'm';
                }

                const g = svg.append('g').attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');
                const spanMs = x.domain()[1] - x.domain()[0];
                const tickCount = Math.max(4, Math.min(8, Math.floor(w / 120)));
                const xStart = x.domain()[0];
                const xEnd = x.domain()[1];
                let tickEvery;
                let tickFormat;
                if (spanMs <= 6 * 3600 * 1000) {
                    tickEvery = d3.timeHour.every(1);
                    tickFormat = d3.timeFormat('%H:%M');
                } else if (spanMs <= 36 * 3600 * 1000) {
                    tickEvery = d3.timeHour.every(3);
                    tickFormat = d3.timeFormat('%d %b %H:%M');
                } else if (spanMs <= 21 * 24 * 3600 * 1000) {
                    tickEvery = d3.timeDay.every(Math.max(1, Math.ceil((spanMs / 86400000) / tickCount)));
                    tickFormat = d3.timeFormat('%d %b');
                } else if (spanMs <= 140 * 24 * 3600 * 1000) {
                    tickEvery = d3.timeWeek.every(Math.max(1, Math.ceil((spanMs / (7 * 86400000)) / tickCount)));
                    tickFormat = d3.timeFormat('%d %b');
                } else {
                    tickEvery = d3.timeMonth.every(Math.max(1, Math.ceil((spanMs / (30 * 86400000)) / tickCount)));
                    tickFormat = d3.timeFormat('%b %Y');
                }
                g.append('g')
                    .attr('transform', 'translate(0,' + h + ')')
                    .call(
                        d3.axisBottom(x)
                            .tickValues(tickEvery.range(tickEvery.floor(xStart), xEnd))
                            .tickFormat(tickFormat)
                    )
                    .selectAll('text')
                    .attr('font-size', '11px');
                g.append('g').call(d3.axisLeft(y).ticks(6).tickFormat(formatTick));
                const yRight = g.append('g')
                    .attr('transform', 'translate(' + w + ',0)')
                    .call(d3.axisRight(y).ticks(6).tickFormat(formatTick));
                yRight.selectAll('text').attr('fill', showLine ? '#F28E2B' : '#333').attr('font-size', '11px');
                yRight.selectAll('path, line').attr('stroke', showLine ? '#F28E2B' : '#ccc');

                if (includeSlow) {
                    g.append('line')
                        .attr('x1', 0).attr('x2', w)
                        .attr('y1', y(cfg.slowMin)).attr('y2', y(cfg.slowMin))
                        .attr('stroke', '#E15759').attr('stroke-dasharray', '5,4').attr('opacity', 0.7);
                    g.append('text')
                        .attr('x', 4).attr('y', y(cfg.slowMin) - 6)
                        .attr('fill', '#E15759').attr('font-size', '11px')
                        .text(cfg.slowLabel || formatMin(cfg.slowMin));
                }

                const series = cfg.series || [];
                const winMs = cfg.p90Step === 'hour'
                    ? 3600 * 1000
                    : (cfg.p90Step === 'week' ? 7 * 24 * 3600 * 1000 : 24 * 3600 * 1000);
                if (showDist) {
                    series.forEach(function(bucket, index) {
                        if (!bucket || !bucket.n) {
                            return;
                        }
                        const x0 = x(bucket.start);
                        const x1 = x(bucket.start + winMs);
                        const barW = Math.max(2, x1 - x0 - 1);
                        if (x1 < 0 || x0 > w) {
                            return;
                        }
                        const slowShare = distByTime
                            ? (bucket.totalMinutes ? bucket.slowMinutes / bucket.totalMinutes : 0)
                            : (bucket.n ? bucket.slow / bucket.n : 0);
                        const hasFast = distByTime
                            ? (bucket.slowMinutes < bucket.totalMinutes)
                            : (bucket.slow < bucket.n);
                        let slowH = h * slowShare;
                        let fastH = h - slowH;
                        if (hasFast && fastH < 2) {
                            fastH = 2;
                            slowH = h - 2;
                        }
                        g.append('rect')
                            .attr('class', 'win')
                            .attr('data-id', 'win-' + index)
                            .attr('x', x0 + 0.5)
                            .attr('y', h - fastH)
                            .attr('width', barW)
                            .attr('height', Math.max(0, fastH))
                            .attr('fill', '#4E79A7')
                            .attr('opacity', 0.16);
                        g.append('rect')
                            .attr('class', 'win')
                            .attr('data-id', 'win-' + index)
                            .attr('x', x0 + 0.5)
                            .attr('y', h - fastH - slowH)
                            .attr('width', barW)
                            .attr('height', Math.max(0, slowH))
                            .attr('fill', '#E15759')
                            .attr('opacity', 0.28);
                    });
                }
                if (showLine && series.length === 1) {
                    g.append('line')
                        .attr('x1', 0).attr('x2', w)
                        .attr('y1', y(series[0].p90)).attr('y2', y(series[0].p90))
                        .attr('stroke', '#F28E2B').attr('stroke-width', 2.5);
                    g.append('text')
                        .attr('x', w - 4).attr('y', y(series[0].p90) - 6)
                        .attr('text-anchor', 'end').attr('fill', '#F28E2B').attr('font-size', '11px')
                        .text((cfg.pName || 'p90') + ' ' + formatMin(series[0].p90));
                } else if (showLine && series.length > 1) {
                    const gapMs = cfg.p90Step === 'hour' ? 3 * 3600 * 1000 : (cfg.p90Step === 'week' ? 10 * 24 * 3600 * 1000 : 36 * 3600 * 1000);
                    const line = d3.line()
                        .defined(function(d, i) {
                            if (i === 0) {
                                return true;
                            }
                            const prev = series[i - 1];
                            return prev ? (d.start - prev.start) <= gapMs : true;
                        })
                        .x(function(d) { return x(d.start + winMs / 2); })
                        .y(function(d) { return y(d.p90); });
                    g.append('path')
                        .datum(series)
                        .attr('fill', 'none')
                        .attr('stroke', '#F28E2B')
                        .attr('stroke-width', 2.5)
                        .attr('d', line);
                }
                if (showLine && !showPoints) {
                    g.selectAll('circle.pline')
                        .data(series)
                        .enter()
                        .append('circle')
                        .attr('class', function(d) {
                            return 'pline pk-' + String(d.key || '').replace(/[^A-Za-z0-9_-]/g, '_');
                        })
                        .attr('cx', function(d) { return x(d.start + winMs / 2); })
                        .attr('cy', function(d) { return y(d.p90); })
                        .attr('r', 5)
                        .attr('fill', '#F28E2B')
                        .attr('stroke', '#fff')
                        .attr('stroke-width', 1.5)
                        .style('cursor', 'pointer');
                }

                g.selectAll('circle')
                    .data(showPoints ? drawPoints : [])
                    .enter()
                    .append('circle')
                    .attr('class', 'pt')
                    .attr('data-id', function(d) { return d.id; })
                    .attr('cx', function(d) { return x(d.start); })
                    .attr('cy', function(d) { return y(d.minutes); })
                    .attr('r', function(d) {
                        if (selected && d.id === selected.id) {
                            return 7;
                        }
                        return d.minutes >= cfg.slowMin ? 5 : 4;
                    })
                    .attr('fill', function(d) {
                        const key = cfg.colorBy === 'workflow' ? d.workflow : d.preset;
                        return (cfg.colorOf && cfg.colorOf[key]) || '#76B7B2';
                    })
                    .attr('opacity', function(d) {
                        if (!selected) {
                            return 0.85;
                        }
                        return d.id === selected.id ? 1 : 0.45;
                    });

                svg.append('text')
                    .attr('x', 12).attr('y', 16)
                    .attr('fill', '#333').attr('font-size', '13px').attr('font-weight', '600')
                    .text(cfg.filterCaption || cfg.metricLabel || '');

                const legendNames = cfg.colorNames || [];
                const legend = svg.append('g').attr('transform', 'translate(' + margin.left + ',34)');
                let legendX = 0;
                let legendY = 0;
                for (let i = 0; i < legendNames.length && showPoints; i++) {
                    const name = legendNames[i];
                    const itemW = legendItemWidth(name);
                    if (legendX > 0 && legendX + itemW > legendMax) {
                        legendX = 0;
                        legendY += 16;
                    }
                    legend.append('circle').attr('cx', legendX).attr('cy', legendY).attr('r', 4)
                        .attr('fill', (cfg.colorOf && cfg.colorOf[name]) || '#76B7B2');
                    legend.append('text').attr('x', legendX + 10).attr('y', legendY + 4).attr('font-size', '11px')
                        .attr('fill', '#888').text(name);
                    legendX += itemW;
                }
                if (showDist) {
                    legend.append('rect').attr('x', legendX).attr('y', legendY - 4).attr('width', 10).attr('height', 8)
                        .attr('fill', '#4E79A7').attr('opacity', 0.35);
                    legend.append('rect').attr('x', legendX + 10).attr('y', legendY - 4).attr('width', 10).attr('height', 8)
                        .attr('fill', '#E15759').attr('opacity', 0.45);
                    legend.append('text').attr('x', legendX + 24).attr('y', legendY + 4).attr('font-size', '11px')
                        .attr('fill', '#888')
                        .text((distByTime ? 'time share / ' : 'count share / ') + (cfg.p90Step || 'hour'));
                    legendX += 118;
                }
                if (showLine) {
                    legend.append('line')
                        .attr('x1', legendX).attr('x2', legendX + 16)
                        .attr('y1', legendY).attr('y2', legendY).attr('stroke', '#F28E2B').attr('stroke-width', 2);
                    legend.append('text')
                        .attr('x', legendX + 20).attr('y', legendY + 4).attr('font-size', '11px').attr('fill', '#888')
                        .text((cfg.pName || 'p90') + ' / ' + (cfg.p90Step || 'hour'));
                }

                svg.append('text')
                    .attr('x', width - 12).attr('y', 16).attr('text-anchor', 'end')
                    .attr('fill', '#333').attr('font-size', '12px')
                    .text(shareCaption);

                if (selected) {
                    const items = [];
                    items.push({text: formatMin(selected.minutes), href: ''});
                    items.push({
                        text: new Date(selected.start).toISOString().replace('T', ' ').slice(0, 16) + ' UTC',
                        href: '',
                    });
                    const isPr = selected.kind && String(selected.kind).indexOf('pull_request') === 0;
                    if (isPr && selected.pr && selected.pr !== '0') {
                        items.push({
                            text: 'PR #' + selected.pr,
                            href: 'https://github.com/ydb-platform/ydb/pull/' + selected.pr,
                        });
                    }
                    if (selected.url) {
                        items.push({text: 'Run ' + selected.runId, href: selected.url});
                    }
                    if (selected.runId) {
                        let gantt = 'https://datalens.ru/135ob2ntmr0ok-ci-metrics?gantt_run=' + encodeURIComponent(selected.runId);
                        items.push({text: 'Gantt', href: gantt});
                    }
                    if (selected.url && selected.jobId) {
                        items.push({
                            text: 'job log',
                            href: String(selected.url).replace(/\/+$/, '') + '/job/' + selected.jobId,
                        });
                    }
                    if (selected.job) {
                        items.push({text: selected.job, href: ''});
                    }
                    if (selected.branch) {
                        items.push({text: selected.branch, href: ''});
                    }
                    if (selected.commit) {
                        items.push({
                            text: selected.commit.slice(0, 8),
                            href: 'https://github.com/ydb-platform/ydb/commit/' + selected.commit,
                        });
                    }
                    let dx = 12;
                    for (let i = 0; i < items.length; i++) {
                        const item = items[i];
                        const host = item.href
                            ? svg.append('a').attr('href', item.href).attr('target', '_blank')
                            : svg;
                        host.append('text')
                            .attr('x', dx)
                            .attr('y', height - 18)
                            .attr('fill', item.href ? '#4E79A7' : '#333')
                            .attr('font-size', '12px')
                            .text(item.text);
                        dx += item.text.length * 7 + 16;
                    }
                } else {
                    svg.append('text')
                        .attr('x', 12).attr('y', height - 18)
                        .attr('fill', '#888').attr('font-size', '11px')
                        .text(showDist
                            ? ('Columns = long ' + (distByTime ? 'time' : 'count') +
                               ' per ' + (cfg.p90Step || 'hour') + '. Click a point for details.')
                            : (cfg.viewMode === 'p'
                                ? (cfg.pName || 'p90') + ' by ' + (cfg.p90Step || 'hour') + '.'
                                : 'Click a point for details.'));
                }

                return Editor.generateHtml(svg.node());
            } catch (err) {
                console.log('[render] fail', String(err));
                return Editor.generateHtml(
                    '<div style="padding:16px;font:13px sans-serif;color:#c00">render: ' +
                    String(err) + '</div>'
                );
            }
        },
    }),

    tooltip: {
        renderer: Editor.wrapFn({
            args: [chartConfig],
            fn: function(event, cfg) {
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
                const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                function stamp(ms, withTime) {
                    const date = new Date(ms);
                    const day = date.getDate() + ' ' + months[date.getMonth()];
                    if (!withTime) {
                        return day;
                    }
                    const hh = (date.getHours() < 10 ? '0' : '') + date.getHours();
                    const mm = (date.getMinutes() < 10 ? '0' : '') + date.getMinutes();
                    return day + ' ' + hh + ':' + mm;
                }
                function bucketInterval(bucket) {
                    const step = cfg.p90Step === 'hour' ? 'hour' : (cfg.p90Step === 'week' ? 'week' : 'day');
                    const rawKey = String((bucket && bucket.key) || '');
                    let fromMs;
                    let toMs;
                    if (step === 'hour') {
                        fromMs = new Date(rawKey.slice(0, 10) + 'T' + rawKey.slice(11) + ':00:00').getTime();
                        toMs = fromMs + 3600 * 1000;
                    } else if (step === 'week') {
                        fromMs = new Date(rawKey.slice(0, 10) + 'T00:00:00').getTime();
                        toMs = fromMs + 7 * 24 * 3600 * 1000;
                    } else {
                        fromMs = new Date(rawKey.slice(0, 10) + 'T00:00:00').getTime();
                        toMs = fromMs + 24 * 3600 * 1000;
                    }
                    if (step === 'day') {
                        return stamp(fromMs, false);
                    }
                    if (step === 'week') {
                        return stamp(fromMs, false) + ' – ' + stamp(toMs, false);
                    }
                    return stamp(fromMs, true) + ' – ' + stamp(toMs, true);
                }
                const target = event && event.target;
                if (!target || !target.getAttribute) {
                    return null;
                }
                const winId = target.getAttribute('data-id') || '';
                if (winId.indexOf('win-') === 0) {
                    const bucket = (cfg.series || [])[Number(winId.slice(4))];
                    if (!bucket) {
                        return null;
                    }
                    function pctShare(value, incomplete) {
                        const p = (value || 0) * 100;
                        if (incomplete && p >= 99.95) {
                            return '99.9%';
                        }
                        if (p >= 99.5 && p < 99.95) {
                            return p.toFixed(1) + '%';
                        }
                        return Math.round(p) + '%';
                    }
                    const countShare = bucket.n ? bucket.slow / bucket.n : 0;
                    const timeShare = bucket.totalMinutes ? bucket.slowMinutes / bucket.totalMinutes : 0;
                    const byTime = cfg.viewMode === 'share_time';
                    const hasFast = bucket.slow < bucket.n;
                    const headline = byTime
                        ? (pctShare(timeShare, hasFast) + ' time')
                        : (bucket.slow + ' / ' + bucket.n + ' ops');
                    const detail = byTime
                        ? (bucket.slow + ' / ' + bucket.n + ' ops · ' + pctShare(countShare, false) + ' count')
                        : (pctShare(countShare, false) + ' count · ' + pctShare(timeShare, hasFast) + ' time');
                    return Editor.generateHtml(
                        '<div style="padding:8px 10px;font:13px/1.35 ui-sans-serif,system-ui,sans-serif;color:#222;max-width:300px">' +
                        '<div style="color:#333;font-size:13px;font-weight:600">' + bucketInterval(bucket) + '</div>' +
                        '<div style="color:#777;font-size:12px;margin-top:2px">' +
                        (byTime ? 'time share' : 'count share') + ' · long ≥ ' +
                        (cfg.slowLabel || formatMin(cfg.slowMin)) + '</div>' +
                        '<b style="display:block;margin-top:6px;font-size:18px">' + headline + '</b>' +
                        '<div style="margin-top:6px;color:#444">long ' + formatMin(bucket.slowMinutes) +
                        ' · total ' + formatMin(bucket.totalMinutes) + '</div>' +
                        '<div style="margin-top:2px;color:#444">' + detail + '</div>' +
                        '<div style="margin-top:4px;color:#777;font-size:12px">' + (cfg.pName || 'p90') + ' ' +
                        formatMin(bucket.p90) + '</div>' +
                        '</div>'
                    );
                }
                const plineMatch = String(target.getAttribute('class') || '').match(/\bpk-([A-Za-z0-9_-]+)/);
                const plineKey = plineMatch ? plineMatch[1] : '';
                if (plineKey) {
                    const buckets = cfg.series || [];
                    let bucket = null;
                    for (let i = 0; i < buckets.length; i++) {
                        const key = String((buckets[i] && buckets[i].key) || '').replace(/[^A-Za-z0-9_-]/g, '_');
                        if (key === plineKey) {
                            bucket = buckets[i];
                            break;
                        }
                    }
                    if (!bucket) {
                        return null;
                    }
                    const step = cfg.p90Step === 'hour' ? 'hour' : (cfg.p90Step === 'week' ? 'week' : 'day');
                    return Editor.generateHtml(
                        '<div style="padding:8px 10px;font:13px/1.35 ui-sans-serif,system-ui,sans-serif;color:#222;max-width:320px">' +
                        '<div style="color:#333;font-size:13px;font-weight:600">' + bucketInterval(bucket) + '</div>' +
                        '<div style="color:#777;font-size:12px;margin-top:2px">' + (cfg.pName || 'p90') + ' · ' + step + '</div>' +
                        '<b style="display:block;margin-top:6px;font-size:18px;letter-spacing:-0.02em">' + formatMin(bucket.p90) + '</b>' +
                        '<div style="margin-top:6px;color:#444">p50 ' + formatMin(bucket.p50) + ' · n ' + bucket.n +
                        ' · long ' + (bucket.slow || 0) + '/' + bucket.n +
                        ' (' + Math.round((bucket.n ? (bucket.slow || 0) / bucket.n : 0) * 100) + '%)</div>' +
                        '<div style="margin-top:4px;color:#777;font-size:12px">long ' +
                        formatMin(bucket.slowMinutes) + ' · total ' + formatMin(bucket.totalMinutes) + '</div>' +
                        '</div>'
                    );
                }
                let node = target;
                let id = '';
                for (let hop = 0; node && hop < 4; hop += 1) {
                    id = node.getAttribute && node.getAttribute('data-id') || '';
                    if (id) {
                        break;
                    }
                    node = node.parentNode;
                }
                const list = cfg.points || [];
                let point = null;
                for (let i = 0; i < list.length; i++) {
                    if (list[i] && list[i].id === id) {
                        point = list[i];
                        break;
                    }
                }
                if (!point) {
                    return null;
                }
                const started = new Date(point.start);
                function two(n) {
                    return (n < 10 ? '0' : '') + n;
                }
                const when = started.getUTCDate() + ' ' + months[started.getUTCMonth()] + ' ' +
                    two(started.getUTCHours()) + ':' + two(started.getUTCMinutes()) + ' UTC';
                const buckets = cfg.series || [];
                let bucket = null;
                for (let i = 0; i < buckets.length; i++) {
                    if (buckets[i] && buckets[i].key === point.bucket) {
                        bucket = buckets[i];
                        break;
                    }
                }
                function esc(value) {
                    return String(value || '')
                        .replace(/&/g, '&amp;')
                        .replace(/</g, '&lt;')
                        .replace(/>/g, '&gt;');
                }
                const step = cfg.p90Step === 'hour' ? 'this hour' : (cfg.p90Step === 'week' ? 'this week' : 'this day');
                const meta = [point.preset, point.branch, point.pr ? 'PR ' + point.pr : ''].filter(Boolean);
                const stats = bucket
                    ? (cfg.pName || 'p90') + ' ' + formatMin(bucket.p90) + ' ' + step +
                      ' · p50 ' + formatMin(bucket.p50) + ' · n ' + bucket.n
                    : '';
                return Editor.generateHtml(
                    '<div style="padding:8px 10px;font:13px/1.35 ui-sans-serif,system-ui,sans-serif;color:#222;max-width:280px">' +
                    '<div style="display:flex;align-items:baseline;justify-content:space-between;gap:16px">' +
                    '<b style="font-size:18px;letter-spacing:-0.02em">' + formatMin(point.minutes) + '</b>' +
                    '<span style="color:#777;font-size:12px;white-space:nowrap">' + when + '</span>' +
                    '</div>' +
                    '<div style="margin-top:8px;font-weight:600">' + esc(point.workflow) + '</div>' +
                    '<div style="color:#333">' + esc(point.job) + '</div>' +
                    (meta.length ? '<div style="margin-top:4px;color:#666;font-size:12px">' + esc(meta.join(' · ')) + '</div>' : '') +
                    (point.commit ? '<div style="margin-top:2px;font:12px/1.35 ui-monospace,monospace;color:#444">' + esc(point.commit).slice(0, 7) + '</div>' : '') +
                    (stats ? '<div style="margin-top:8px;padding-top:6px;border-top:1px solid #e6e6e6;color:#777;font-size:12px">' + stats + '</div>' : '') +
                    '</div>'
                );
            },
        }),
    },

    events: {
        click: Editor.wrapFn({
            args: [chartConfig],
            fn: function(event, cfg) {
                const target = event && event.target;
                if (!target || !target.getAttribute) {
                    return;
                }
                let node = target;
                let id = '';
                for (let hop = 0; node && hop < 4; hop += 1) {
                    id = node.getAttribute && node.getAttribute('data-id') || '';
                    if (id) {
                        break;
                    }
                    node = node.parentNode;
                }
                if (!id) {
                    return;
                }
                const list = cfg.points || [];
                let point = null;
                for (let i = 0; i < list.length; i++) {
                    if (list[i] && list[i].id === id) {
                        point = list[i];
                        break;
                    }
                }
                if (!point) {
                    return;
                }
                if (typeof Chart !== 'undefined' && typeof Chart.updateActionParams === 'function') {
                    Chart.updateActionParams({
                        gantt_run: [point.runId || ''],
                        gantt_attempt: [point.attempt || ''],
                        run_label: [''],
                        pr_number: [point.pr || ''],
                        pr: [point.pr || ''],
                        selected_id: [id],
                    });
                }
                if (typeof Chart !== 'undefined' && typeof Chart.setState === 'function') {
                    Chart.setState({selectedId: id});
                }
            },
        }),
    },
};
