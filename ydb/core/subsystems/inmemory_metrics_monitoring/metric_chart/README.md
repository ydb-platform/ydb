# Embedding metric charts

Add `ydb/core/subsystems/inmemory_metrics_monitoring/metric_chart` to the C++ consumer's `PEERDIR`. During monitoring
setup, call `NKikimr::NMetricChart::RegisterResources(mon)` once. This publishes
`static/metric-chart/chart.js`, `client.js` and `chart.css` from binary resources.
ChartKit, its Yagr Canvas renderer, React and styles are bundled into the binary.
No external CDN or browser-side package resolution is required.

Load the stylesheet and import the modules relative to the monitoring root.
For a page under `/actors/`, the following paths also work through `/node/<id>/`:

```html
<link rel="stylesheet" href="../static/metric-chart/chart.css">
<div id="cpu"></div>
<script type="module">
import {createMetricChart} from '../static/metric-chart/chart.js';
import {createInMemoryMetricsClient, parseQuery} from '../static/metric-chart/client.js';

const client = createInMemoryMetricsClient({endpoint: 'metrics'});
const chart = createMetricChart(document.getElementById('cpu'), {
    legend: true,
    onRangeChange: ({from, to}) => show(from, to),
});
let result;
function show(begin, end) {
    chart.setData({series: result.series, begin, end, title: 'Pool CPU'});
}
result = await client.queryMany([
    {...parseQuery('actor_system.pool.cpu_cores{"pool"=="User"}'), id: 'cpu'},
], {seconds: 300});
const end = result.catalog.timestamp_ms;
show(end - 300000, end);
// Call chart.destroy() when removing the component.
</script>
```

The chart accepts `{series, begin, end, title, emptyText}`. Times are Unix
milliseconds and `end > begin`. A series has a stable `key`, `display`, `color`,
`step`, `closed`, and sorted `points: [{time, raw, value}]`. `raw` preserves exact
text; `value` is a finite number or null for plotting. `step` distinguishes
on-change values from sampled lines. `seriesStats(series, begin, end)` shares the
viewer's last/min/max/average calculation. Pass `onPin` to pause live refresh when
a tooltip is pinned. ResizeObserver follows the container width; destroy
releases it and removes the component's DOM.

The data client is optional: other sources can provide the same series format.
`request({line, seconds, signal})` reads the viewer's existing JSON protocol;
`queryMany(queries, {seconds, signal, catalog})` returns `{catalog, series, limited}`.
It fetches histories sequentially and reuses each line response within the batch.
Limits remain 8 selectors, 16 matching series per selector, and 64 series per batch. Use an
AbortController and discard stale results when switching requests. Authentication
uses the existing same-origin monitoring session. Cross-origin access requires
configuration of the destination server; the module does not change access rules.

The in-memory overview at `/actors/metrics?page=overview` is a second consumer.
It uses the same chart and JSON client without the viewer's query editor.

Pass `settings: {type: 'area', height: 240, unit: 'bytes', precision: 2,
min: 0, max: null}` to `createMetricChart`, or call `chart.setSettings(patch)`
to update presentation without fetching data. Defaults are line, 360 px,
number, automatic precision and automatic Y limits including zero. Units:
number, bytes (IEC), percent, seconds, milliseconds, cores. Unit selection
formats values without converting percent or time scales. Y limits use raw
metric units; null restores automatic bounds. Supply finite bounds with
min < max, height 160..800 and precision null or 0..6. Stacked areas preserve on-change steps and null gaps.
`formatMetricValue(value, settings)` formats axes, tooltip and legend values.
With default settings, exact textual values remain intact.

A series may override `type` (line or area) and `width` (line stroke pixels); `color`
is already per-series. Empty type inherits chart settings. Viewer split mode
allows chart settings per query and appearance overrides per retained line.

`area` renders stacked layers: each layer thickness is its raw value, with
positive and negative values stacked separately. Tooltip values remain raw.
`fill: true` shades the region under ordinary lines without stacking them;
per-series `fill` overrides the chart setting. The adapter aligns both sides of each timestamp, preserving steps and gaps.
Aligned graph data is capped at one million cells. Larger timestamp unions
are sampled for display; tooltip values and statistics use retained samples.
A note below the chart indicates when display sampling is active. Automatic numeric formatting uses three significant digits;
axis labels reserve space according to their length.

Set `settings.format` to a series name template. `{metric}`, `{name}`, `{query}`
and `{labels}` refer to metric field, registry line name, query letter and all
labels. `{pool}` substitutes the `pool` label; `{label:metric}` accesses a label
whose name conflicts with a built-in field. Unknown placeholders remain visible.
An empty format preserves the default name. Series `format` overrides the chart
format; series metadata uses `metric`, `queryLabel` and `labelValues` (the label
name/value array). The shared JSON client supplies these fields. Names are plain
text and never HTML, and formatting does not change series keys or query matching.

ChartKit uses Yagr/uPlot to draw axes and series in Canvas.
Axis text keeps its 12 px font at every chart width and configured height.
Overview chunks stack used and free counts; together they cover the chunk pool.
Memory compares allocated chunk capacity and recorded payload as ordinary lines.

Use `onCursorChange(time)` and `chart.setCursor(time)` to synchronize a vertical cursor across embedded charts. The time is a Unix timestamp in milliseconds; `null` clears the cursor. `setCursor` updates only the marker and does not emit the callback or open a tooltip. The marker survives redraws and is hidden outside the chart interval.
A pinned tooltip keeps its local cursor until unpinned; external cursor updates do not move that marker.

### Allocation bar

Import `createAllocationBar` from `static/metric-chart/allocation.js` and load `chart.css`. It accepts `setData({capacity, free, segments: [{key, label, value, color?}]})`, `destroy()`, and options `unit`, `maxSegments` (24 by default, capped at 64), `onSelect(segment)`. Values share a unit. Largest owners appear separately; remaining owners are summed as Other lines. Unattributed allocation and free capacity stay distinct. Names are plain text. The component has no registry dependency.

Overview uses `lines[].chunks` from the viewer JSON endpoint: retained snapshot chunks per storage line, including shared group fields only once. Counts describe the current snapshot and do not depend on the history interval. Reserved or retiring chunks absent from the line snapshot appear as unattributed allocation. Registry statistics and line capture can differ during concurrent writes; the bar scales to the larger observed total rather than creating negative segments.

Set `legend:false` when the owner table acts as the legend. `getColor(key)` returns the displayed owner color (including the aggregate color for omitted owners), and `highlight(key)` highlights its bar segment. The bar tooltip uses a body portal, flips at viewport edges, lists bounded owner values and percentages, and can be pinned by clicking a segment.

### Shared cursor

Use one cursor group for charts with the same time interval:

```js
import {createMetricChart, createMetricChartCursorGroup} from './chart.js';
const cursorGroup = createMetricChartCursorGroup();
const chart = createMetricChart(host, {cursorGroup, plotLeft: 100});
chart.setData({series, begin, end, title: 'Metrics'});
```

The active chart updates the other charts by timestamp. Moving to another chart
releases the previous pinned tooltip, so only one timestamp is selected in the
group. `destroy()` removes the chart from the group. `plotLeft` reserves a common
minimum width for the Y axis; use the same value on aligned dashboard charts.
Charts without a cursor group keep independent pinned tooltips.

Pointer positions use the Canvas plot overlay and Yagr's coordinate conversion.
Layout width is measured before replacing content, to avoid measuring the
temporary disappearance of a page scrollbar during redraw.

### Bundled engine

The prebuilt `chartkit.js` and `chartkit.css` resources contain ChartKit with
its Yagr plugin, React and styles. The JavaScript bundle exports `mountChartKit`
behind the plain JavaScript embedding API. The adapter retains exact-value
tooltips, legends, cursor groups and range selection. ChartKit assets are
lazy-loaded on the first nonempty chart.

The adapter aligns independent histories to one timeline, retaining both sides
of on-change transitions and null gaps. Stacked areas use explicit Canvas bands
so positive and negative layers remain separate; tooltips and statistics read
original samples. The existing client limits apply before alignment.

Build sources, package manifests and lockfiles for this bundle are not kept in
this repository. When replacing the prebuilt resources, update
`chartkit.js.LEGAL.txt` and `THIRD_PARTY_LICENSES.txt` to match the included
libraries. The C++ build embeds these assets directly without invoking npm.
