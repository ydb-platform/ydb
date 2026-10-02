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

const params = Editor.getParams();
const workflows = paramList('workflow').concat(paramList('wf')).filter(function(value, index, list) {
    return list.indexOf(value) === index;
});
const branches = paramList('branch').length ? paramList('branch') : paramList('tl_branch');
const prNumber = firstParam('pr_number', '') || firstParam('pr', '');
const jobNames = paramList('job_name');
const presets = paramList('build_preset');
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
const runId = selectorRef.runId || clickRef.runId || urlRef.runId;
const runAttempt = selectorRef.attempt || clickAttempt || clickRef.attempt || urlRef.attempt;
const maxRows = Math.max(100, parseInt(firstParam('max_rows', '3000'), 10) || 3000);
const datasetId = Editor.getId('dataset');

console.log('[sources] start');
console.log('[sources] params', params);
console.log('[sources] datasetId', datasetId);
console.log('[sources] filters', {workflows: workflows, branches: branches, prNumber: prNumber, jobNames: jobNames, presets: presets, runId: runId, runAttempt: runAttempt, maxRows: maxRows});

const statuses = paramList('job_conclusion');
const where = [];
if (runId) {
    where.push({column: 'run_id', operation: 'EQ', values: [runId]});
    if (runAttempt) {
        where.push({column: 'run_attempt', operation: 'EQ', values: [runAttempt]});
    }
} else if (prNumber) {
    where.push({column: 'pr_number', operation: 'EQ', values: [prNumber]});
} else {
    if (workflows.length) {
        where.push({column: 'workflow', operation: 'IN', values: workflows});
    }
    if (branches.length) {
        where.push({column: 'branch', operation: 'IN', values: branches});
    }
    if (presets.length) {
        where.push({column: 'build_preset', operation: 'IN', values: presets});
    }
}
if (!runId && jobNames.length) {
    where.push({column: 'job_name', operation: 'IN', values: jobNames});
}
if (!runId && statuses.length) {
    where.push({column: 'job_conclusion', operation: 'IN', values: statuses});
}

const source = buildSource({
    datasetId: datasetId,
    columns: [
        'event_date',
        'run_id',
        'github_job_id',
        'run_attempt',
        'pr_number',
        'workflow',
        'job_name',
        'build_preset',
        'source',
        'step_name',
        'start_ts',
        'duration_ms',
        'conclusion',
        'job_conclusion',
        'run_url',
        'branch',
        'commit',
    ],
    where: where,
    order_by: [{direction: 'DESC', column: 'start_ts'}],
    limit: maxRows,
});

console.log('[sources] where', where);
console.log('[sources] built source keys', source && Object.keys(source));

module.exports = {
    data: source,
};
