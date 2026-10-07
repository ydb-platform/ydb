const {buildSource} = require('libs/dataset/v2');

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
const branches = paramList('branch').length ? paramList('branch') : paramList('tl_branch');
const presets = paramList('tl_preset');
const jobs = paramList('job_name');
const workflows = paramList('workflow').concat(paramList('wf')).filter(function(value, index, list) {
    return list.indexOf(value) === index;
});
const datasetId = Editor.getId('dataset');

const where = [];
if (metric === 'queue') {
    where.push({column: 'step_name', operation: 'EQ', values: ['queue']});
} else if (metric === 's3_sync' || /^s3_sync_try_[0-9]+$/.test(metric)) {
    where.push({column: 'step_name', operation: 'STARTSWITH', values: ['s3_sync']});
} else if (/^ya_(?:build_rebuild|build)_try_[0-9]+$/.test(metric)) {
    where.push({column: 'step_name', operation: 'IN', values: ['ya_build', 'ya_rebuild']});
} else if (/^ya_cache_download_try_[0-9]+$/.test(metric)) {
    where.push({column: 'step_name', operation: 'EQ', values: ['ya_cache_download']});
} else if (/^ya_cache_upload_try_[0-9]+$/.test(metric)) {
    where.push({column: 'step_name', operation: 'EQ', values: ['ya_cache_upload']});
} else if (/^ya_tests_try_[0-9]+$/.test(metric)) {
    where.push({column: 'step_name', operation: 'EQ', values: ['ya_tests']});
} else if (/^(prepare_ya_make|postprocess_try|transform_build_results|fail_checker|generate_summary|upload_tests_results)_try_[0-9]+$/.test(metric)) {
    where.push({column: 'step_name', operation: 'EQ', values: [metric.replace(/_try_[0-9]+$/, '')]});
} else if (metric === 'checkout') {
    where.push({column: 'step_name', operation: 'EQ', values: ['Checkout']});
} else if (metric === 'job') {
    where.push({column: 'source', operation: 'EQ', values: ['github_job']});
    where.push({column: 'step_name', operation: 'EQ', values: ['job']});
} else {
    where.push({column: 'step_name', operation: 'EQ', values: [metric]});
}
if (branches.length) {
    where.push({column: 'branch', operation: 'IN', values: branches});
}
if (presets.length) {
    where.push({column: 'build_preset', operation: 'IN', values: presets});
}
if (jobs.length) {
    where.push({column: 'job_name', operation: 'IN', values: jobs});
}
if (workflows.length) {
    where.push({column: 'workflow', operation: 'IN', values: workflows});
}
const statuses = paramList('job_conclusion');
if (statuses.length) {
    where.push({column: 'job_conclusion', operation: 'IN', values: statuses});
}
function pad2(n) {
    return (n < 10 ? '0' : '') + n;
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
function ymd(ms) {
    const date = new Date(ms);
    return date.getUTCFullYear() + '-' + pad2(date.getUTCMonth() + 1) + '-' + pad2(date.getUTCDate());
}
const interval = splitInterval(firstParam('interval', ''));
const dateFrom = firstParam('tl_from', '');
const dateTo = firstParam('tl_to', '');
let fromMs = interval ? interval.start : (dateFrom ? Date.parse(dateFrom + 'T00:00:00Z') : null);
let toMs = interval ? interval.end : (dateTo ? Date.parse(dateTo + 'T23:59:59.999Z') : null);
if (Number.isFinite(fromMs) && Number.isFinite(toMs) && fromMs > toMs) {
    const swap = fromMs;
    fromMs = toMs;
    toMs = swap;
}
if (Number.isFinite(fromMs)) {
    where.push({column: 'event_date', operation: 'GTE', values: [ymd(fromMs)]});
}
if (Number.isFinite(toMs)) {
    where.push({column: 'event_date', operation: 'LTE', values: [ymd(toMs)]});
}
const source = buildSource({
    datasetId: datasetId,
    columns: [
        'event_date',
        'ci_run_id',
        'github_job_id',
        'pr_number',
        'workflow',
        'job_name',
        'build_preset',
        'branch',
        'event_name',
        'commit',
        'step_name',
        'ya_try',
        'source',
        'start_ts',
        'duration_ms',
        'duration_sec',
        'conclusion',
        'job_conclusion',
        'run_url',
    ],
    where: where,
    order_by: [{direction: 'DESC', column: 'start_ts'}],
    limit: 50000,
});

module.exports = {
    data: Object.assign({}, source, {ui: true}),
};
