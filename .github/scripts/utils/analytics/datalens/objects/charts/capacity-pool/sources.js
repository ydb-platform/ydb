const {buildSource} = require('libs/dataset/v2');

const source = buildSource({
    datasetId: Editor.getId('dataset'),
    columns: [
        'sampled_at', 'kind', 'name', 'workflow', 'job_name', 'branch', 'run_url', 'pr_number', 'pr_title', 'state', 'zone',
        'cores', 'ram_gib', 'nrd_gib', 'usage_value', 'free_value', 'created_at', 'age_sec',
    ],
    where: [{column: 'kind', operation: 'IN', values: ['summary', 'queue', 'runner']}],
    order_by: [{direction: 'DESC', column: 'sampled_at'}],
    limit: 20000,
});

module.exports = {
    data: Object.assign({}, source, {ui: true}),
};
