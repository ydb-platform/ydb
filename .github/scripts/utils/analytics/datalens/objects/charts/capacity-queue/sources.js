const {buildSource} = require('libs/dataset/v2');

const since = new Date(Date.now() - 36 * 3600 * 1000);
const ymd = since.toISOString().slice(0, 10);

const source = buildSource({
    datasetId: Editor.getId('dataset'),
    columns: ['start_ts', 'duration_sec', 'step_name', 'workflow'],
    where: [
        {column: 'step_name', operation: 'EQ', values: ['queue']},
        {column: 'event_date', operation: 'GTE', values: [ymd]},
    ],
    order_by: [{direction: 'DESC', column: 'start_ts'}],
    limit: 20000,
});

module.exports = {
    data: Object.assign({}, source, {ui: true}),
};
