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
