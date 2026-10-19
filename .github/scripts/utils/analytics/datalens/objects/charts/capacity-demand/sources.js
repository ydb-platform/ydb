const {buildSource} = require('libs/dataset/v2');

const source = buildSource({
    datasetId: Editor.getId('dataset'),
    columns: [
        'sampled_at', 'kind', 'name', 'workflow', 'job_name', 'branch', 'run_url',
        'pr_number', 'pr_title', 'state', 'zone', 'cores', 'ram_gib', 'nrd_gib',
        'limit_value', 'usage_value', 'age_sec',
    ],
    where: [{column: 'kind', operation: 'IN', values: ['quota', 'queue', 'runner']}],
    order_by: [{direction: 'DESC', column: 'sampled_at'}],
    limit: 20000,
});

module.exports = {
    data: Object.assign({}, source, {ui: true}),
};
