const {buildSource} = require('libs/dataset/v2');

const source = buildSource({
    datasetId: Editor.getId('dataset'),
    columns: [
        'sampled_at', 'kind', 'name', 'workflow', 'job_name', 'branch', 'state', 'zone', 'ram_gib', 'cores',
    ],
    where: [{column: 'kind', operation: 'EQ', values: ['runner']}],
    order_by: [{direction: 'ASC', column: 'sampled_at'}],
    limit: 50000,
});

module.exports = {
    data: Object.assign({}, source, {ui: true}),
};
