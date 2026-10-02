const Dataset = require('libs/dataset/v2');

function firstParam(name, fallback) {
    const values = Editor.getParam(name);
    if (values && values.length && values[0] !== '') {
        return String(values[0]);
    }
    return fallback || '';
}

function parseLoadedRows(loaded, sourceName) {
    const block = loaded[sourceName];
    console.log('[prepare] parseLoadedRows source', sourceName, 'block keys', block && Object.keys(block));
    if (!block) {
        return [];
    }
    if (block.result && block.result.data) {
        const meta = block.result.fields || [];
        const titles = meta.map(function(f) { return f.title; });
        const raw = block.result.data.Data || [];
        console.log('[prepare] dataset-result rows', raw.length, 'titles', titles);
        return raw.map(function(row) {
            const obj = {};
            titles.forEach(function(title, i) {
                obj[title] = row[i];
            });
            return obj;
        });
    }
    if (Array.isArray(block)) {
        const columnNames = block
            .filter(function(item) { return item && item.event === 'metadata'; })
            .map(function(item) { return item.data && item.data.names; })[0] || [];
        const rows = [];
        block.filter(function(item) { return item && item.event === 'row'; }).forEach(function(item) {
            const rowItem = item.data || [];
            const obj = {};
            rowItem.forEach(function(field, index) {
                obj[columnNames[index]] = field;
            });
            rows.push(obj);
        });
        return rows;
    }
    return [];
}

function loadRows(sourceName) {
    const loaded = Editor.getLoadedData();
    console.log('[prepare] getLoadedData keys', loaded && Object.keys(loaded));
    if (loaded && loaded[sourceName] && loaded[sourceName].error) {
        console.log('[prepare] source error', loaded[sourceName].error);
    }
    try {
        const rows = Dataset.getDatasetRows({datasetName: sourceName});
        console.log('[prepare] getDatasetRows', rows && rows.length);
        return rows || [];
    } catch (e) {
        console.log('[prepare] getDatasetRows failed', String(e));
        return parseLoadedRows(loaded, sourceName);
    }
}

function toNumber(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function formatDuration(sec) {
    if (sec === null || sec === undefined || !Number.isFinite(sec)) {
        return '—';
    }
    if (sec < 1) {
        return (Math.round(sec * 10) / 10) + 's';
    }
    if (sec < 60) {
        return Math.round(sec) + 's';
    }
    const hours = Math.floor(sec / 3600);
    const minutes = Math.floor((sec % 3600) / 60);
    const seconds = Math.round(sec % 60);
    if (hours > 0) {
        return hours + 'h ' + minutes + 'm';
    }
    if (seconds === 0) {
        return minutes + 'm';
    }
    return minutes + 'm ' + seconds + 's';
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

function indentLabel(level, text) {
    if (level <= 0) {
        return text;
    }
    if (level === 1) {
        return '    ' + text;
    }
    return '        ' + text;
}

function statusInfo(conclusion) {
    const key = String(conclusion || '').toLowerCase();
    if (key === 'success') {
        return {key: 'success', color: '#2DA55D', label: 'Successful', hollow: false};
    }
    if (key === 'failure') {
        return {key: 'failure', color: '#E24B4A', label: 'Failed', hollow: false};
    }
    if (key === 'cancelled') {
        return {key: 'cancelled', color: '#8A8A8A', label: 'Cancelled', hollow: false};
    }
    if (key === 'skipped') {
        return {key: 'skipped', color: '#B0B0B0', label: 'Skipped', hollow: true};
    }
    return {key: 'in_progress', color: '#E6A817', label: 'In progress', hollow: false};
}

function pickJobConclusion(items) {
    let fromJobRow = '';
    let fromField = '';
    const childKeys = [];
    items.forEach(function(item) {
        if (item.step === 'job' && item.conclusion && item.conclusion !== 'in_progress') {
            fromJobRow = item.conclusion;
        }
        if (!fromField && item.jobConclusion && item.jobConclusion !== 'in_progress') {
            fromField = item.jobConclusion;
        }
        if (item.conclusion) {
            childKeys.push(String(item.conclusion).toLowerCase());
        }
    });
    return fromJobRow || fromField || workflowStatusOf(childKeys);
}

function workflowStatusOf(statuses) {
    const keys = statuses.map(function(value) { return String(value || '').toLowerCase(); });
    if (!keys.length || keys.some(function(value) { return value === 'in_progress'; })) {
        return 'in_progress';
    }
    if (keys.some(function(value) { return value === 'failure'; })) {
        return 'failure';
    }
    if (keys.some(function(value) { return value === 'cancelled'; })) {
        return 'cancelled';
    }
    if (keys.every(function(value) { return value === 'skipped'; })) {
        return 'skipped';
    }
    return 'success';
}

console.log('[prepare] start params', Editor.getParams());

function parseRunRef(raw) {
    const text = String(raw || '').trim();
    let attempt = '';
    const hash = text.match(/#(\d+)\s*$/);
    if (hash) {
        attempt = hash[1];
    }
    const slash = text.match(/(\d{6,})\/(\d+)\s*$/);
    if (slash) {
        return {runId: slash[1], attempt: slash[2]};
    }
    const parts = text.split('·');
    let runId = '';
    for (let i = parts.length - 1; i >= 0; i--) {
        const token = parts[i].trim().split(' ')[0];
        if (/^\d{6,}$/.test(token)) {
            runId = token;
            break;
        }
    }
    if (!runId && /^\d{6,}$/.test(text.split(/[\s/]/)[0])) {
        runId = text.split(/[\s/]/)[0];
    }
    return {runId: runId, attempt: attempt};
}

const selectorRef = parseRunRef(firstParam('run_label', ''));
const urlRef = parseRunRef(firstParam('run_id', '') || firstParam('run', ''));
const clickRef = parseRunRef(firstParam('gantt_run', ''));
const clickAttempt = firstParam('gantt_attempt', '') || firstParam('run_attempt', '');
const runIdParam = selectorRef.runId || clickRef.runId || urlRef.runId;
const attemptParam = selectorRef.attempt || clickAttempt || clickRef.attempt || urlRef.attempt;
let rows = loadRows('data');
console.log('[prepare] loaded rows', rows.length, 'run', runIdParam, 'attempt', attemptParam);

function latestAttemptOf(items) {
    let bestAttempt = '';
    let bestStart = -1;
    items.forEach(function(row) {
        const start = toDateMs(row.start_ts);
        if (start === null) {
            return;
        }
        if (start > bestStart) {
            bestStart = start;
            bestAttempt = String(row.run_attempt || '');
        }
    });
    return bestAttempt;
}

function latestRunRef(items) {
    let best = {runId: '', attempt: ''};
    let bestStart = -1;
    items.forEach(function(row) {
        const id = String(row.run_id || '');
        const start = toDateMs(row.start_ts);
        if (!id || start === null) {
            return;
        }
        if (start > bestStart) {
            bestStart = start;
            best = {runId: id, attempt: String(row.run_attempt || '')};
        }
    });
    return best;
}

function sameAttempt(row, attempt) {
    return String(row.run_attempt || '') === String(attempt || '');
}

function keepOneAttempt(items, attempt) {
    if (!items.length) {
        return items;
    }
    if (attempt) {
        const hit = items.filter(function(row) { return sameAttempt(row, attempt); });
        if (hit.length) {
            return hit;
        }
    }
    const latest = latestAttemptOf(items);
    if (!latest) {
        return items;
    }
    return items.filter(function(row) { return sameAttempt(row, latest); });
}

const runInScope = runIdParam && rows.some(function(row) { return String(row.run_id) === runIdParam; });
if (runInScope) {
    rows = keepOneAttempt(rows.filter(function(row) { return String(row.run_id) === runIdParam; }), attemptParam);
} else if (runIdParam) {
    console.log('[prepare] requested run_id missing from data', runIdParam, 'rows', rows.length);
    rows = [];
} else if (rows.length) {
    const latest = latestRunRef(rows);
    rows = keepOneAttempt(rows.filter(function(row) { return String(row.run_id) === latest.runId; }), latest.attempt);
    console.log('[prepare] latest run', latest, 'rows', rows.length);
}

const rawBars = [];
rows.forEach(function(row, index) {
    const start = toDateMs(row.start_ts);
    const durationMs = toNumber(row.duration_ms);
    if (start === null || durationMs === null || durationMs <= 0) {
        return;
    }
    const durationSec = durationMs / 1000;
    rawBars.push({
        id: 's' + index,
        job: String(row.job_name || 'job'),
        jobId: normalizeJobId(row.github_job_id) || ('row-' + index),
        step: String(row.step_name || 'step') === 'ya_rebuild' ? 'ya_build' : String(row.step_name || 'step'),
        source: String(row.event_source || row.source || ''),
        start: start,
        finish: start + durationMs,
        durationSec: durationSec,
        durationLabel: formatDuration(durationSec),
        conclusion: String(row.conclusion || ''),
        jobConclusion: String(row.job_conclusion || ''),
        runId: String(row.run_id || ''),
        attempt: String(row.run_attempt || ''),
        preset: String(row.build_preset || ''),
        pr: realPr(row.pr_number),
        url: String(row.run_url || ''),
        branch: String(row.branch || ''),
        commit: String(row.commit || ''),
        workflow: String(row.workflow || ''),
    });
});

(function attachYaPhaseToGithubJobs() {
    const ghNameById = {};
    rawBars.forEach(function(item) {
        if ((item.source !== 'github_job' && item.source !== 'github_step') || isWorkflowAlias(item.job) || !item.jobId) {
            return;
        }
        if (item.source === 'github_job' && item.step === 'job') {
            ghNameById[item.jobId] = item.job;
            return;
        }
        if (!ghNameById[item.jobId]) {
            ghNameById[item.jobId] = item.job;
        }
    });
    const jobsWithPhases = {};
    rawBars.forEach(function(item) {
        if (item.source === 'ya_phase' && item.jobId) {
            jobsWithPhases[item.jobId] = true;
        }
    });
    for (let i = rawBars.length - 1; i >= 0; i -= 1) {
        if (isWrapperStep(rawBars[i], jobsWithPhases)) {
            rawBars.splice(i, 1);
        }
    }
    rawBars.forEach(function(item) {
        if (!isWorkflowAlias(item.job)) {
            return;
        }
        if (ghNameById[item.jobId]) {
            item.job = ghNameById[item.jobId];
        } else if (item.preset) {
            item.job = 'Build and test ' + item.preset;
        }
    });
})();

function normalizeJobId(value) {
    if (value === null || value === undefined || value === '') {
        return '';
    }
    const n = Number(value);
    if (Number.isFinite(n) && n > 10000) {
        return String(Math.round(n));
    }
    const s = String(value).trim();
    if (!s || s === '0' || s === 'null' || s === 'undefined') {
        return '';
    }
    return s;
}

function isWorkflowAlias(name) {
    const n = String(name || '');
    return n === 'PR-check' || n.indexOf('Postcommit') === 0;
}

function isWrapperStep(item, jobsWithPhases) {
    if (item.source !== 'github_step') {
        return false;
    }
    const n = String(item.step || '').toLowerCase();
    if (n === 'complete job') {
        return true;
    }
    // Hide the GitHub wrapper only when ya_phase children replace it.
    return n === 'build and test' && !!(jobsWithPhases && jobsWithPhases[item.jobId]);
}

function firstNonEmpty(items, key) {
    for (let i = 0; i < items.length; i += 1) {
        const value = items[i][key];
        if (value && value !== '0' && value !== 'null') {
            return value;
        }
    }
    return '';
}

function githubJobUrl(runUrl, jobId) {
    if (!runUrl || !jobId) {
        return '';
    }
    const base = String(runUrl).split('?')[0].replace(/\/+$/, '');
    if (base.indexOf('/actions/runs/') === -1) {
        return runUrl;
    }
    return base + '/job/' + jobId;
}

function realPr(value) {
    if (value === null || value === undefined || value === '') {
        return '';
    }
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0 || n === 10) {
        return '';
    }
    return String(n);
}

function githubPrUrl(pr) {
    const clean = realPr(pr);
    if (!clean) {
        return '';
    }
    return 'https://github.com/ydb-platform/ydb/pull/' + clean;
}

function githubCommitUrl(commit) {
    if (!commit) {
        return '';
    }
    return 'https://github.com/ydb-platform/ydb/commit/' + commit;
}

function groupByJob(items) {
    const groups = {};
    const order = [];
    items.forEach(function(item) {
        const key = String(item.jobId || item.job);
        if (!groups[key]) {
            groups[key] = [];
            order.push(key);
        }
        groups[key].push(item);
    });
    return order.map(function(key) { return groups[key]; });
}

function isTestStep(step) {
    const name = String(step.step || '').toLowerCase();
    return name.indexOf('test') !== -1 || name.indexOf('ya') !== -1;
}

function nestJobItems(jobItems) {
    const steps = [];
    const phases = [];
    jobItems.forEach(function(item) {
        if (item.source === 'ya_phase') {
            phases.push(item);
        } else {
            steps.push(item);
        }
    });
    steps.sort(function(a, b) { return a.start - b.start; });
    phases.sort(function(a, b) { return a.start - b.start; });

    let host = null;
    steps.forEach(function(step) {
        if (!isTestStep(step)) {
            return;
        }
        if (!host || (step.finish - step.start) > (host.finish - host.start)) {
            host = step;
        }
    });

    const used = {};
    const nested = [];
    steps.forEach(function(step) {
        nested.push({item: step, hostId: ''});
        if (host && step.id !== host.id) {
            return;
        }
        phases.forEach(function(phase) {
            if (used[phase.id]) {
                return;
            }
            const insideHost = host && phase.start >= host.start && phase.start < host.finish;
            const insideStep = phase.start >= step.start && phase.start < step.finish;
            if (host ? insideHost : insideStep) {
                used[phase.id] = true;
                nested.push({item: phase, hostId: (host || step).id});
            }
        });
    });
    phases.forEach(function(phase) {
        if (!used[phase.id]) {
            nested.push({item: phase, hostId: ''});
        }
    });
    return nested;
}

const t0All = rawBars.length
    ? Math.min.apply(null, rawBars.map(function(b) { return b.start; }))
    : 0;

function elapsedShort(ms) {
    const sec = Math.max(0, (ms - t0All) / 1000);
    if (sec < 60) {
        return '+' + Math.round(sec) + 's';
    }
    const hours = Math.floor(sec / 3600);
    const minutes = Math.floor((sec % 3600) / 60);
    if (hours > 0) {
        return '+' + hours + 'h ' + minutes + 'm';
    }
    return '+' + minutes + 'm';
}

function jobWorkStart(items) {
    const working = items.filter(function(x) { return x.step !== 'queue'; });
    const pool = working.length ? working : items;
    return Math.min.apply(null, pool.map(function(x) { return x.start; }));
}

const jobGroups = groupByJob(rawBars);
jobGroups.sort(function(a, b) {
    const aStart = jobWorkStart(a);
    const bStart = jobWorkStart(b);
    if (aStart !== bStart) {
        return aStart - bStart;
    }
    return String(a[0].jobId).localeCompare(String(b[0].jobId));
});

const bars = [];
const labelByLane = {};

jobGroups.forEach(function(jobItems, jobIndex) {
    const jobRow = jobItems.filter(function(item) { return item.step === 'job'; })[0] || null;
    const childItems = jobItems.filter(function(item) { return item.step !== 'job'; });
    const spanItems = childItems.length ? childItems : jobItems;
    const first = jobRow || spanItems[0];
    const jobStart = jobRow ? jobRow.start : Math.min.apply(null, spanItems.map(function(x) { return x.start; }));
    const jobFinish = jobRow ? jobRow.finish : Math.max.apply(null, spanItems.map(function(x) { return x.finish; }));
    const wallSec = jobRow ? jobRow.durationSec : (jobFinish - jobStart) / 1000;
    const wallLabel = jobRow ? jobRow.durationLabel : formatDuration(wallSec);
    const jobLane = 'job-' + jobIndex + '-' + first.jobId;
    const titleRow = jobItems.filter(function(item) {
        return item.source === 'github_job' && item.step === 'job' && !isWorkflowAlias(item.job);
    })[0] || jobItems.filter(function(item) {
        return (item.source === 'github_job' || item.source === 'github_step') && !isWorkflowAlias(item.job);
    })[0] || first;
    let jobTitle = titleRow.job;
    if (isWorkflowAlias(jobTitle) && titleRow.preset) {
        jobTitle = 'Build and test ' + titleRow.preset;
    } else if (titleRow.preset && String(jobTitle).indexOf(titleRow.preset) === -1) {
        jobTitle = jobTitle + ' · ' + titleRow.preset;
    }
    const workStart = jobWorkStart(jobItems);
    const jobConclusion = pickJobConclusion(jobItems);
    const jobStatus = statusInfo(jobConclusion);
    labelByLane[jobLane] = jobTitle + '  ' + elapsedShort(workStart);

    bars.push({
        id: jobLane,
        lane: jobLane,
        level: 0,
        parentId: '',
        childCount: 0,
        isGroup: true,
        job: first.job,
        jobId: first.jobId,
        step: first.job,
        source: 'job',
        start: jobStart,
        finish: jobFinish,
        durationSec: wallSec,
        durationLabel: wallLabel,
        conclusion: jobConclusion,
        jobConclusion: jobConclusion,
        statusColor: jobStatus.color,
        statusLabel: jobStatus.label,
        statusHollow: jobStatus.hollow,
        runId: first.runId,
        attempt: first.attempt,
        preset: first.preset,
        pr: first.pr,
        url: first.url,
        branch: first.branch,
        commit: first.commit,
        workflow: first.workflow,
        jobUrl: githubJobUrl(first.url, first.jobId),
    });

    const nested = nestJobItems(childItems);
    nested.forEach(function(entry, stepIndex) {
        const item = entry.item;
        const level = entry.hostId ? 2 : 1;
        const lane = jobLane + '-n' + stepIndex;
        labelByLane[lane] = item.step;
        bars.push({
            id: item.id,
            lane: lane,
            level: level,
            parentId: entry.hostId || jobLane,
            childCount: 0,
            isGroup: false,
            job: item.job,
            jobId: item.jobId,
            step: item.step,
            source: item.source,
            start: item.start,
            finish: item.finish,
            durationSec: item.durationSec,
            durationLabel: item.durationLabel,
            conclusion: item.conclusion,
            jobConclusion: item.jobConclusion,
            runId: item.runId,
            attempt: item.attempt,
            preset: item.preset,
            pr: item.pr,
            url: item.url,
            branch: item.branch,
            commit: item.commit,
            workflow: item.workflow,
            jobUrl: githubJobUrl(item.url, item.jobId),
        });
    });
});

function isTryStep(step) {
    return String(step || '').indexOf('ya_make_try_') === 0;
}
function isTryWork(step) {
    return step === 'ya_build' || step === 'ya_rebuild' || step === 'ya_tests' || step === 'ya_cache_download' || step === 'ya_cache_upload';
}
bars.forEach(function(bar) {
    if (!isTryWork(bar.step)) {
        return;
    }
    let host = null;
    bars.forEach(function(tryBar) {
        if (!isTryStep(tryBar.step) || tryBar.jobId !== bar.jobId) {
            return;
        }
        if (bar.start >= tryBar.start && bar.start < tryBar.finish) {
            host = tryBar;
        }
    });
    if (host) {
        bar.parentId = host.id;
        bar.level = (host.level || 1) + 1;
    }
});
(function orderUnderParents() {
    const childrenOf = {};
    const roots = [];
    const seenParent = {};
    bars.forEach(function(bar) { seenParent[bar.id] = true; });
    bars.forEach(function(bar) {
        if (bar.parentId && seenParent[bar.parentId]) {
            if (!childrenOf[bar.parentId]) {
                childrenOf[bar.parentId] = [];
            }
            childrenOf[bar.parentId].push(bar);
        } else {
            roots.push(bar);
        }
    });
    const ordered = [];
    function walk(node) {
        ordered.push(node);
        (childrenOf[node.id] || []).forEach(walk);
    }
    roots.forEach(walk);
    bars.length = 0;
    ordered.forEach(function(bar) { bars.push(bar); });
})();
const sharedLane = {};
bars.forEach(function(bar) {
    if (!isTryWork(bar.step)) {
        return;
    }
    const key = String(bar.parentId || '') + '|' + bar.step;
    if (!sharedLane[key]) {
        sharedLane[key] = bar.lane;
    } else {
        bar.lane = sharedLane[key];
    }
});
const childCount = {};
bars.forEach(function(bar) {
    if (!bar.parentId) {
        return;
    }
    childCount[bar.parentId] = (childCount[bar.parentId] || 0) + 1;
});
bars.forEach(function(bar) {
    bar.childCount = childCount[bar.id] || 0;
});
console.log('[prepare] jobs', jobGroups.length, 'bars', bars.length);


bars.forEach(function(bar) {
    if (bar.isGroup || bar.level === 0) {
        bar.kind = 'job';
    } else if (bar.step === 'queue') {
        bar.kind = 'queue';
    } else if (String(bar.step || '').indexOf('ya_make_try_') === 0) {
        bar.kind = 'try';
    } else if (bar.step === 'ya_cache_download' || bar.step === 'ya_cache_upload') {
        bar.kind = 'cache';
    } else if (bar.step === 'ya_build' || bar.step === 'ya_rebuild' || bar.step === 'ya_tests') {
        bar.kind = 'work';
    } else if (bar.level === 1) {
        bar.kind = 'step';
    } else {
        bar.kind = 'phase';
    }
});
const lanes = bars.map(function(b) { return b.lane; });
const colorDomain = ['job', 'queue', 'step', 'phase', 'try', 'work', 'cache'].filter(function(name) {
    return bars.some(function(b) { return b.kind === name; });
});
const legendLabel = {
    job: 'job',
    queue: 'queue',
    step: 'step',
    phase: 'phase',
    try: 'try',
    work: 'build / tests',
    cache: 'cache',
};
const palette = {
    job: '#2F3A4A',
    queue: '#EDC948',
    step: '#4C78A8',
    phase: '#F28E2B',
    try: '#B07AA1',
    work: '#59A14F',
    cache: '#76B7B2',
};
const fallbackPalette = ['#4E79A7', '#F28E2B', '#E15759', '#76B7B2', '#59A14F'];

const wallSec = bars.length
    ? (Math.max.apply(null, bars.map(function(b) { return b.finish; })) -
       Math.min.apply(null, bars.map(function(b) { return b.start; }))) / 1000
    : 0;
const leafBars = bars.filter(function(b) { return !b.isGroup; });
const sumSec = leafBars.reduce(function(acc, b) { return acc + b.durationSec; }, 0);

const t0 = bars.length ? Math.min.apply(null, bars.map(function(b) { return b.start; })) : 0;

const jobStatuses = bars.filter(function(bar) { return bar.isGroup; }).map(function(bar) { return bar.jobConclusion; });
const workflowStatus = statusInfo(workflowStatusOf(jobStatuses));

const chartConfig = {
    bars: bars,
    lanes: lanes,
    labelByLane: labelByLane,
    colorDomain: colorDomain,
    legendLabel: legendLabel,
    palette: palette,
    fallbackPalette: fallbackPalette,
    runId: bars.length ? bars[0].runId : runIdParam,
    totalRows: rawBars.length,
    wallLabel: formatDuration(wallSec),
    sumLabel: formatDuration(sumSec),
    jobCount: jobGroups.length,
    t0: t0,
    meta: {
        pr: firstNonEmpty(rawBars, 'pr'),
        prUrl: githubPrUrl(firstNonEmpty(rawBars, 'pr')),
        runId: firstNonEmpty(rawBars, 'runId'),
        runUrl: firstNonEmpty(rawBars, 'url'),
        branch: firstNonEmpty(rawBars, 'branch'),
        commit: firstNonEmpty(rawBars, 'commit'),
        commitUrl: githubCommitUrl(firstNonEmpty(rawBars, 'commit')),
        workflow: firstNonEmpty(rawBars, 'workflow'),
        attempt: firstNonEmpty(rawBars, 'attempt'),
        workflowStatus: workflowStatus.key,
        workflowStatusColor: workflowStatus.color,
        workflowStatusLabel: workflowStatus.label,
        workflowStatusHollow: workflowStatus.hollow,
    },
};

console.log('[prepare] wall', chartConfig.wallLabel, 'jobs', chartConfig.jobCount);

module.exports = {
    render: Editor.wrapFn({
        libs: ['d3@7.9.0'],
        args: [chartConfig],
        fn: function(options, cfg) {
            const width = Number(options.width) || 1200;
            const margin = {top: 70, right: 72, bottom: 56, left: 300};
            const height = Math.max(160, Number(options.height) || 400);
            const w = width - margin.left - margin.right;
            const h = height - margin.top - margin.bottom;

            const svg = d3.create('svg')
                .attr('width', width)
                .attr('height', height)
                .style('display', 'block');

            if (!cfg.bars.length) {
                svg.append('text')
                    .attr('x', width / 2)
                    .attr('y', height / 2)
                    .attr('text-anchor', 'middle')
                    .attr('fill', 'var(--g-color-text-secondary)')
                    .text('No rows. See Console.');
                return Editor.generateHtml(svg.node());
            }

            const byId = {};
            cfg.bars.forEach(function(bar) { byId[bar.id] = bar; });
            const state = (typeof Chart !== 'undefined' && typeof Chart.getState === 'function')
                ? (Chart.getState() || {})
                : {};
            const fullStart = d3.min(cfg.bars, function(d) { return d.start; });
            const fullEnd = d3.max(cfg.bars, function(d) { return d.finish; });
            const zoomStart = Number(state.zoomStart);
            const zoomEnd = Number(state.zoomEnd);
            const viewStart = zoomStart < zoomEnd ? Math.max(fullStart, zoomStart) : fullStart;
            const viewEnd = zoomStart < zoomEnd ? Math.min(fullEnd, zoomEnd) : fullEnd;
            const x = d3.scaleTime()
                .domain([viewStart, viewEnd])
                .range([0, w]);
            function isOpen(bar, st) {
                const flag = st.openIds && st.openIds[bar.id];
                if (flag === true || flag === false) {
                    return flag;
                }
                return !!(bar.isGroup || bar.level === 0 || String(bar.step || '').indexOf('ya_make_try_') === 0);
            }
            function shown(bar) {
                let parentId = bar.parentId;
                while (parentId) {
                    const parent = byId[parentId];
                    if (!parent) {
                        break;
                    }
                    if (!isOpen(parent, state)) {
                        return false;
                    }
                    parentId = parent.parentId;
                }
                return true;
            }
            const visible = cfg.bars.filter(shown);
            const laneOrder = [];
            visible.forEach(function(bar) {
                if (laneOrder.indexOf(bar.lane) < 0) {
                    laneOrder.push(bar.lane);
                }
            });
            const y = d3.scaleBand()
                .domain(laneOrder)
                .range([0, h])
                .padding(0.18);

            function colorOf(bar) {
                const key = bar && bar.kind ? bar.kind : bar;
                if (cfg.palette[key]) {
                    return cfg.palette[key];
                }
                return cfg.fallbackPalette[0];
            }

            const g = svg.append('g')
                .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

            function elapsedLabel(value) {
                const ms = value && value.getTime ? value.getTime() : Number(value);
                const rounded = Math.round(Math.max(0, (ms - cfg.t0) / 1000));
                const hours = Math.floor(rounded / 3600);
                const minutes = Math.floor((rounded % 3600) / 60);
                const seconds = rounded % 60;
                const secText = (seconds < 10 ? '0' : '') + seconds + 's';
                if (hours > 0) {
                    return hours + 'h ' + minutes + 'm ' + secText;
                }
                if (minutes > 0) {
                    return minutes + 'm ' + secText;
                }
                return rounded + 's';
            }

            g.append('g')
                .attr('transform', 'translate(0,' + h + ')')
                .call(d3.axisBottom(x).ticks(8).tickFormat(elapsedLabel));

            g.append('g').call(d3.axisLeft(y).tickSize(0).tickFormat(function() { return ''; }));

            cfg.bars.filter(function(d) { return d.isGroup; }).forEach(function(jobBar) {
                const children = visible.filter(function(bar) {
                    let parentId = bar.parentId;
                    while (parentId) {
                        if (parentId === jobBar.id) {
                            return true;
                        }
                        const parent = byId[parentId];
                        parentId = parent ? parent.parentId : '';
                    }
                    return false;
                });
                const first = children[0] || jobBar;
                const last = children.length ? children[children.length - 1] : jobBar;
                const top = y(first.lane);
                const bottom = y(last.lane) + y.bandwidth();
                if (top === undefined || bottom === undefined || isNaN(top)) {
                    return;
                }
                g.append('rect')
                    .attr('x', -margin.left + 8)
                    .attr('y', top - 2)
                    .attr('width', width - 16)
                    .attr('height', bottom - top + 4)
                    .attr('fill', 'var(--g-color-base-generic)')
                    .attr('opacity', 0.35)
                    .style('pointer-events', 'none');
            });

            const guideRows = [];
            const guideSeen = {};
            visible.forEach(function(bar) {
                if (guideSeen[bar.lane]) {
                    return;
                }
                guideSeen[bar.lane] = true;
                guideRows.push(bar);
            });
            g.append('g')
                .attr('class', 'row-guides')
                .selectAll('line')
                .data(guideRows)
                .enter()
                .append('line')
                .attr('x1', 0)
                .attr('x2', function(d) { return Math.max(0, x(d.start)); })
                .attr('y1', function(d) { return y(d.lane) + y.bandwidth() / 2; })
                .attr('y2', function(d) { return y(d.lane) + y.bandwidth() / 2; })
                .attr('stroke', 'var(--g-color-line-generic, #ccc)')
                .attr('stroke-width', 1)
                .style('pointer-events', 'none');

            let selectedId = '';
            if (typeof Chart !== 'undefined' && typeof Chart.getState === 'function') {
                const st = Chart.getState() || {};
                selectedId = st.selectedId || '';
            }
            let selected = null;
            for (let i = 0; i < cfg.bars.length; i++) {
                if (cfg.bars[i] && cfg.bars[i].id === selectedId) {
                    selected = cfg.bars[i];
                    break;
                }
            }

            function nextLaneStart(d) {
                let nextStart = null;
                visible.forEach(function(other) {
                    if (other.lane !== d.lane || other === d || other.start <= d.start) {
                        return;
                    }
                    if (nextStart === null || other.start < nextStart) {
                        nextStart = other.start;
                    }
                });
                return nextStart;
            }
            function drawnWidth(d) {
                const timeW = Math.max(0, x(d.finish) - x(d.start));
                let width = Math.max(4, timeW);
                const nextStart = nextLaneStart(d);
                if (nextStart !== null) {
                    const room = x(nextStart) - x(d.start) - 1;
                    if (room > 0) {
                        width = Math.min(width, room);
                    }
                }
                return Math.max(timeW, Math.min(width, Math.max(timeW, 4)));
            }
            function barBox(d) {
                const left = x(d.start);
                const right = left + drawnWidth(d);
                const x0 = Math.max(0, Math.min(w, left));
                const x1 = Math.max(0, Math.min(w, right));
                return {x: x0, width: Math.max(0, x1 - x0)};
            }
            g.selectAll('rect.bar')
                .data(visible)
                .enter()
                .append('rect')
                .attr('class', function(d) { return 'bar bar-' + d.id; })
                .attr('x', function(d) { return barBox(d).x; })
                .attr('y', function(d) { return y(d.lane); })
                .attr('width', function(d) { return barBox(d).width; })
                .attr('height', function(d) { return d.isGroup ? y.bandwidth() : Math.max(6, y.bandwidth() - 2); })
                .attr('rx', 3)
                .attr('fill', function(d) { return colorOf(d); })
                .attr('opacity', function(d) { return d.isGroup ? 1 : 0.9; })
                .attr('stroke', function(d) { return selected && d.id === selected.id ? '#333' : 'none'; })
                .attr('stroke-width', function(d) { return selected && d.id === selected.id ? 1.5 : 0; })
                .style('cursor', 'pointer');

            function durationLayout(d) {
                const text = d.durationLabel || '';
                const textW = text.length * 6.6;
                const left = x(d.start);
                const barW = drawnWidth(d);
                const barRight = left + barW;
                if (barRight <= 0 || left >= w) {
                    return {x: 0, anchor: 'start', fill: 'transparent', text: ''};
                }
                if (barW >= textW + 8) {
                    return {x: barRight - 4, anchor: 'end', fill: d.kind === 'queue' ? '#333' : '#fff', text: text};
                }
                const nextStart = nextLaneStart(d);
                const room = nextStart === null ? 1e9 : x(nextStart) - (barRight + 6);
                if (room >= textW + 4) {
                    return {x: barRight + 6, anchor: 'start', fill: 'var(--g-color-text-secondary)', text: text};
                }
                return {x: barRight, anchor: 'start', fill: 'transparent', text: ''};
            }
            g.selectAll('text.bar-duration')
                .data(visible)
                .enter()
                .append('text')
                .attr('class', 'bar-duration')
                .attr('x', function(d) { return durationLayout(d).x; })
                .attr('y', function(d) { return y(d.lane) + y.bandwidth() / 2; })
                .attr('dy', '0.35em')
                .attr('text-anchor', function(d) { return durationLayout(d).anchor; })
                .attr('fill', function(d) { return durationLayout(d).fill; })
                .attr('font-size', '11px')
                .style('pointer-events', 'none')
                .text(function(d) { return durationLayout(d).text; });

            const meta = cfg.meta || {};
            const headerItems = [];
            if (meta.pr) {
                headerItems.push({text: 'PR #' + meta.pr, href: meta.prUrl || ''});
            }
            if (meta.runId) {
                headerItems.push({text: 'Run ' + meta.runId, href: meta.runUrl || ''});
            }
            if (meta.branch) {
                headerItems.push({text: meta.branch, href: ''});
            }
            if (meta.commit) {
                headerItems.push({text: String(meta.commit).slice(0, 8), href: meta.commitUrl || ''});
            }
            if (meta.workflow) {
                headerItems.push({
                    text: meta.workflow + ' · ' + (meta.workflowStatusLabel || ''),
                    href: '',
                    statusColor: meta.workflowStatusColor || '',
                    statusHollow: !!meta.workflowStatusHollow,
                });
            }
            if (meta.attempt && meta.attempt !== '1') {
                headerItems.push({text: 'attempt ' + meta.attempt, href: ''});
            }
            let headerX = 12;
            headerItems.forEach(function(item) {
                if (item.statusColor) {
                    svg.append('circle')
                        .attr('cx', headerX + 5)
                        .attr('cy', 12)
                        .attr('r', 4.5)
                        .attr('fill', item.statusHollow ? 'transparent' : item.statusColor)
                        .attr('stroke', item.statusColor)
                        .attr('stroke-width', 1.5);
                    headerX += 16;
                }
                const host = item.href
                    ? svg.append('a').attr('href', item.href).attr('target', '_blank')
                    : svg;
                host.append('text')
                    .attr('x', headerX)
                    .attr('y', 16)
                    .attr('fill', item.href ? '#4E79A7' : 'var(--g-color-text-secondary)')
                    .attr('font-size', '12px')
                    .attr('text-decoration', item.href ? 'underline' : 'none')
                    .text(item.text);
                headerX += item.text.length * 7 + 16;
            });

            const legend = svg.append('g')
                .attr('transform', 'translate(' + margin.left + ',36)');
            const legendNames = cfg.legendLabel || {};
            let legendX = 0;
            cfg.colorDomain.forEach(function(name) {
                const label = legendNames[name] || name;
                legend.append('rect')
                    .attr('x', legendX)
                    .attr('y', -7)
                    .attr('width', 10)
                    .attr('height', 10)
                    .attr('rx', 2)
                    .attr('fill', colorOf(name));
                legend.append('text')
                    .attr('x', legendX + 14)
                    .attr('y', 2)
                    .attr('fill', 'var(--g-color-text-secondary)')
                    .attr('font-size', '11px')
                    .text(label);
                legendX += 14 + label.length * 7 + 16;
            });

            svg.append('text')
                .attr('x', width - 12)
                .attr('y', 16)
                .attr('text-anchor', 'end')
                .attr('fill', 'var(--g-color-text-primary)')
                .attr('font-size', '13px')
                .text('total ' + cfg.wallLabel + '  ·  sum ' + cfg.sumLabel + '  ·  ' + cfg.jobCount + ' jobs');
            if (zoomStart < zoomEnd) {
                svg.append('text')
                    .attr('class', 'zoom-reset')
                    .attr('x', width - 12)
                    .attr('y', 34)
                    .attr('text-anchor', 'end')
                    .attr('fill', '#4E79A7')
                    .attr('font-size', '12px')
                    .style('cursor', 'pointer')
                    .text('Reset zoom');
            }

            if (selected) {
                const items = [];
                items.push({text: selected.durationLabel, href: ''});
                items.push({
                    text: new Date(selected.start).toISOString().replace('T', ' ').slice(0, 19) + ' UTC',
                    href: '',
                });
                if (selected.pr) {
                    items.push({
                        text: 'PR #' + selected.pr,
                        href: 'https://github.com/ydb-platform/ydb/pull/' + selected.pr,
                    });
                }
                if (selected.url) {
                    items.push({text: 'Run ' + selected.runId, href: selected.url});
                }
                if (selected.jobUrl) {
                    items.push({text: 'job log', href: selected.jobUrl});
                }
                if (selected.job) {
                    items.push({text: selected.job, href: ''});
                }
                if (selected.step && !selected.isGroup) {
                    items.push({text: selected.step, href: ''});
                }
                if (selected.branch) {
                    items.push({text: selected.branch, href: ''});
                }
                if (selected.commit) {
                    items.push({
                        text: String(selected.commit).slice(0, 8),
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
                        .attr('y', height - 16)
                        .attr('fill', item.href ? '#4E79A7' : 'var(--g-color-text-primary)')
                        .attr('font-size', '12px')
                        .text(item.text);
                    dx += item.text.length * 7 + 16;
                }
            } else {
                svg.append('text')
                    .attr('x', 12)
                    .attr('y', height - 16)
                    .attr('fill', 'var(--g-color-text-secondary)')
                    .attr('font-size', '11px')
                    .text('Click a row name to zoom that interval. ▸ expands a row. Click a bar for links below.');
            }

            const labeledLane = {};
            visible.forEach(function(bar) {
                if (labeledLane[bar.lane]) {
                    return;
                }
                labeledLane[bar.lane] = true;
                const yMid = y(bar.lane) + y.bandwidth() / 2;
                const x0 = -margin.left + 8 + (bar.level || 0) * 14;
                if (bar.childCount > 0) {
                    g.append('text')
                        .attr('class', 'tree-tog tog-' + bar.id)
                        .attr('x', x0)
                        .attr('y', yMid)
                        .attr('dy', '0.35em')
                        .attr('font-size', '12px')
                        .attr('fill', 'var(--g-color-text-secondary)')
                        .style('cursor', 'pointer')
                        .text(isOpen(bar, state) ? '▾' : '▸');
                }
                let nameX = x0 + (bar.childCount ? 16 : 2);
                if (bar.isGroup && bar.statusColor) {
                    g.append('circle')
                        .attr('class', 'job-status status-' + bar.id)
                        .attr('cx', nameX + 5)
                        .attr('cy', yMid)
                        .attr('r', 4.5)
                        .attr('fill', bar.statusHollow ? 'transparent' : bar.statusColor)
                        .attr('stroke', bar.statusColor)
                        .attr('stroke-width', 1.5)
                        .style('pointer-events', 'none');
                    nameX += 16;
                }
                g.append('text')
                    .attr('class', 'tree-lab name-' + bar.id)
                    .attr('x', nameX)
                    .attr('y', yMid)
                    .attr('dy', '0.35em')
                    .attr('font-size', bar.isGroup ? '12px' : '11px')
                    .attr('font-weight', bar.isGroup ? '600' : '400')
                    .attr('fill', state.zoomId === bar.id ? '#4E79A7' : 'var(--g-color-text-primary)')
                    .style('cursor', 'pointer')
                    .text(cfg.labelByLane[bar.lane] || bar.step || '');
            });

            return Editor.generateHtml(svg.node());
        },
    }),

    tooltip: {
        renderer: Editor.wrapFn({
            args: [chartConfig],
            fn: function(event, cfg) {
                const target = event && event.target;
                if (!target || !target.getAttribute) {
                    return null;
                }
                const cls = String(target.getAttribute('class') || '');
                const idMatch = cls.match(/\bbar-([A-Za-z0-9_-]+)/);
                const id = idMatch ? idMatch[1] : '';
                const bar = cfg.bars.find(function(item) { return item.id === id; });
                if (!bar) {
                    return null;
                }
                function esc(value) {
                    return String(value || '')
                        .replace(/&/g, '&amp;')
                        .replace(/</g, '&lt;')
                        .replace(/>/g, '&gt;');
                }
                function two(n) {
                    return (n < 10 ? '0' : '') + n;
                }
                function stamp(ms) {
                    const date = new Date(ms);
                    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                    return date.getUTCDate() + ' ' + months[date.getUTCMonth()] + ' ' +
                        two(date.getUTCHours()) + ':' + two(date.getUTCMinutes()) + ':' + two(date.getUTCSeconds());
                }
                function elapsedAt(ms) {
                    const rounded = Math.round(Math.max(0, (ms - cfg.t0) / 1000));
                    const hours = Math.floor(rounded / 3600);
                    const minutes = Math.floor((rounded % 3600) / 60);
                    const seconds = rounded % 60;
                    const secText = (seconds < 10 ? '0' : '') + seconds + 's';
                    if (hours > 0) {
                        return hours + 'h ' + minutes + 'm ' + secText;
                    }
                    if (minutes > 0) {
                        return minutes + 'm ' + secText;
                    }
                    return rounded + 's';
                }
                const startDay = new Date(bar.start).toISOString().slice(0, 10);
                const endDay = new Date(bar.finish).toISOString().slice(0, 10);
                const endClock = stamp(bar.finish).split(' ').slice(2).join(' ');
                const range = (startDay === endDay
                    ? stamp(bar.start) + ' → ' + endClock
                    : stamp(bar.start) + ' → ' + stamp(bar.finish)) + ' UTC';
                const elapsedRange = elapsedAt(bar.start) + ' → ' + elapsedAt(bar.finish);
                const title = bar.isGroup ? bar.job : bar.step;
                const statusText = bar.isGroup ? (bar.statusLabel || bar.jobConclusion || '') : (bar.conclusion || '');
                const meta = [bar.source, statusText, bar.preset].filter(Boolean).join(' · ');
                return Editor.generateHtml(
                    '<div style="padding:8px 10px;font:13px/1.35 ui-sans-serif,system-ui,sans-serif;color:#222;max-width:320px">' +
                    '<div style="display:flex;align-items:baseline;justify-content:space-between;gap:16px">' +
                    '<b style="font-size:18px;letter-spacing:-0.02em">' + esc(bar.durationLabel) + '</b>' +
                    '<span style="color:#777;font-size:12px;white-space:nowrap">' + range + '</span>' +
                    '</div>' +
                    '<div style="margin-top:4px;color:#555;font-size:12px">' + elapsedRange + '</div>' +
                    '<div style="margin-top:8px;font-weight:600">' + esc(title) + '</div>' +
                    (bar.isGroup
                        ? '<div style="color:#777;font-size:12px">job wall</div>'
                        : '<div style="color:#333">' + esc(bar.job) + '</div>') +
                    (meta ? '<div style="margin-top:6px;padding-top:6px;border-top:1px solid #e6e6e6;color:#666;font-size:12px">' + esc(meta) + '</div>' : '') +
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
                const cls = String(target.getAttribute('class') || '');
                function chartState() {
                    return (typeof Chart !== 'undefined' && typeof Chart.getState === 'function' && Chart.getState()) || {};
                }
                if (cls.indexOf('zoom-reset') !== -1 && typeof Chart !== 'undefined' && typeof Chart.setState === 'function') {
                    const prev = chartState();
                    Chart.setState({selectedId: prev.selectedId || '', openIds: prev.openIds || {}, zoomStart: null, zoomEnd: null, zoomId: ''});
                    return;
                }
                function isOpen(bar, st) {
                    const flag = st.openIds && st.openIds[bar.id];
                    if (flag === true || flag === false) {
                        return flag;
                    }
                    return !!(bar.isGroup || bar.level === 0 || String(bar.step || '').indexOf('ya_make_try_') === 0);
                }
                const nameMatch = cls.match(/\bname-([A-Za-z0-9_-]+)/);
                if (nameMatch && typeof Chart !== 'undefined' && typeof Chart.setState === 'function') {
                    const nameId = nameMatch[1];
                    const named = cfg.bars.find(function(item) { return item.id === nameId; });
                    if (!named) {
                        return;
                    }
                    const prev = chartState();
                    if (prev.zoomId === nameId) {
                        Chart.setState({selectedId: prev.selectedId || '', openIds: prev.openIds || {}, zoomStart: null, zoomEnd: null, zoomId: ''});
                        return;
                    }
                    let start = named.start;
                    let end = named.finish;
                    cfg.bars.forEach(function(item) {
                        if (item.lane !== named.lane) {
                            return;
                        }
                        if (item.start < start) {
                            start = item.start;
                        }
                        if (item.finish > end) {
                            end = item.finish;
                        }
                    });
                    const span = Math.max(1000, end - start);
                    const pad = Math.max(250, span * 0.02);
                    Chart.setState({
                        selectedId: nameId,
                        openIds: prev.openIds || {},
                        zoomStart: start - pad,
                        zoomEnd: end + pad,
                        zoomId: nameId,
                    });
                    return;
                }
                const togMatch = cls.match(/\btog-([A-Za-z0-9_-]+)/);
                if (togMatch && typeof Chart !== 'undefined' && typeof Chart.setState === 'function') {
                    const togId = togMatch[1];
                    const node = cfg.bars.find(function(item) { return item.id === togId; });
                    if (!node || !node.childCount) {
                        return;
                    }
                    const prev = (typeof Chart.getState === 'function' && Chart.getState()) || {};
                    const openIds = Object.assign({}, prev.openIds || {});
                    openIds[togId] = !isOpen(node, prev);
                    Chart.setState({selectedId: prev.selectedId || '', openIds: openIds, zoomStart: prev.zoomStart, zoomEnd: prev.zoomEnd, zoomId: prev.zoomId || ''});
                    return;
                }
                const idMatch = cls.match(/\bbar-([A-Za-z0-9_-]+)/);
                const id = idMatch ? idMatch[1] : '';
                if (!id) {
                    return;
                }
                const bar = cfg.bars.find(function(item) { return item.id === id; });
                if (!bar) {
                    return;
                }
                if (typeof Chart !== 'undefined' && typeof Chart.setState === 'function') {
                    const prev = chartState();
                    Chart.setState({selectedId: id, openIds: prev.openIds || {}, zoomStart: prev.zoomStart, zoomEnd: prev.zoomEnd, zoomId: prev.zoomId || ''});
                }
            },
        }),
    },
};
