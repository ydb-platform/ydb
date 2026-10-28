---
name: ydb-metric-chart-ui
description: Use when embedding shared YDB metric charts or allocation bars into a monitoring UI and connecting data, settings, and cursor interactions.
---

# Embed metric charts in a monitoring page

Paths below are relative to the repository root. This skill covers the shared
module, not editing generated bundles or implementing metric producers.

## Source map

| Contract | Source |
|---|---|
| Public embedding API and options | `ydb/core/subsystems/inmemory_metrics_monitoring/metric_chart/README.md` |
| C++ binary resources | `ydb/core/subsystems/inmemory_metrics_monitoring/metric_chart/resources.h`, `resources.cpp`, `ya.make` |
| Chart adapter, cursor group, formatting | `ydb/core/subsystems/inmemory_metrics_monitoring/metric_chart/chart.js` |
| Selectors and JSON client | `ydb/core/subsystems/inmemory_metrics_monitoring/metric_chart/client.js` |
| Allocation bar | `ydb/core/subsystems/inmemory_metrics_monitoring/metric_chart/allocation.js` |
| Existing overview consumer | `ydb/core/subsystems/inmemory_metrics_monitoring/overview.js` |
| Existing actor-system consumer | `ydb/core/subsystems/actor_system_monitoring/viewer.cpp` |
| Data endpoint and serializer | `ydb/core/subsystems/inmemory_metrics_monitoring/subsystem.cpp`, `viewer.cpp` |

## Connect the page

1. Read the consumer's nearest `AGENTS.md` and the module README. Reuse the public
   adapter instead of adding another renderer or copying chart internals.
2. Add the metric_chart directory above to the consumer's `PEERDIR`. Include
   `resources.h` and register `NKikimr::NMetricChart::RegisterResources(mon)` once
   during monitoring setup; check whether the owning setup already registers it.
3. Load `chart.css` and import `chart.js` as an ES module. For a page under
   `/actors/`, use `../static/metric-chart/` and the relative `metrics` endpoint.
   Check URLs under both direct monitoring and `/node/<id>/` routing. Other page
   locations must resolve paths relative to their own route.
4. Create one chart per container. Supply a bounded positive-height layout that
   resizes with the page. Do not scale SVG or Canvas text through CSS transforms.
5. Read data through `createInMemoryMetricsClient` for the existing JSON endpoint,
   or adapt another source to the series contract in the README. Retain stable
   keys, names, field labels, exact `raw` values and finite `value`/null plotting
   values. Keep points sorted by Unix milliseconds; require `end > begin`.
   Preserve steps and missing data instead of converting gaps to zero.
6. Cancel obsolete requests with `AbortController` and also discard obsolete
   results by generation. Handle errors, empty results and `limited`. Retain the
   last successful plot with an explicit stale status on refresh failure.
   Refresh through JSON, not page navigation; make live refresh optional.
7. Destroy charts and bars, abort requests and clear timers when removing the
   page/component. If using pagehide/pageshow, handle restored browser pages
   without destroying a component that will be reused.

## Minimal consumer

The following page is served under `/actors/`. It creates a one-shot chart;
add bounded refresh and error handling from the steps above for a live page.

```html
<link rel="stylesheet" href="../static/metric-chart/chart.css">
<div id="cpu"></div>
<script type="module">
import {createMetricChart, createMetricChartCursorGroup}
    from '../static/metric-chart/chart.js';
import {createInMemoryMetricsClient, parseQuery}
    from '../static/metric-chart/client.js';
const client = createInMemoryMetricsClient({endpoint: 'metrics'});
const cursorGroup = createMetricChartCursorGroup();
const chart = createMetricChart(document.getElementById('cpu'), {
    cursorGroup, legend: true,
    settings: {type: 'area', height: 240, unit: 'cores', format: '{pool}'},
});
const controller = new AbortController();
window.addEventListener('pagehide', event => {
    if (!event.persisted) { controller.abort(); chart.destroy(); }
});
try {
    const result = await client.queryMany([
        {...parseQuery('actor_system.pool.cpu_cores'), id: 'cpu'},
    ], {seconds: 300, signal: controller.signal});
    const end = result.catalog.timestamp_ms;
    chart.setData({series: result.series, begin: end - 300000, end,
        title: 'Pool CPU', emptyText: 'No retained CPU data'});
} catch (error) {
    if (error.name !== 'AbortError') {
        document.getElementById('cpu').textContent = error.message;
    }
}
</script>
```

## Choose presentation and interactions

1. Use `type:'line'` to compare independent quantities. `fill:true` shades lines
   without stacking. `type:'area'` stacks raw values; each layer's thickness is
   its value. Stack only quantities with compatible units and additive meaning.
   Positive and negative layers stack separately; tooltip values remain raw.
2. Use `setSettings` for type, height, unit, precision, limits and name format;
   do not refetch data just for appearance. Units format values, not convert
   their scale. Use raw units for min/max. Follow README option bounds.
3. Keep pool colors consistent across panels. Names use plain-text templates
   such as `{pool}` or `{metric}`; labels are not HTML. Formatting must not
   change stable series identities or query selection.
4. Share one `createMetricChartCursorGroup()` across panels with the same
   interval. Use a common `plotLeft` for aligned plot boundaries. Use timestamps,
   not pointer pixel offsets, for synchronization. Do not create a parallel
   cursor or tooltip implementation. Pass `onPin` to pause live refresh.
5. Choose a combined plot or separate containers according to units and scale.
   For separate charts, keep a common time interval and controls. Expose compact
   settings; avoid duplicating metric/query names and explanatory text above
   every panel. Keep tooltips visible at viewport edges and test pinned state.
6. For ownership/capacity, use `createAllocationBar` from `allocation.js`.
   Supply capacity, free and bounded owner segments in one unit. Physical group
   chunks count once, not once per logical field. Use `legend:false`, `getColor`
   and `highlight` when a table acts as the legend. The API contract is in README.
7. Keep selector, point and alignment limits visible. Display sampling does not
   reduce storage history; retain exact values for tooltips and statistics.
   Do not promise unlimited rendering or infer missing values from sampling.

## Validation

1. Check `chart.js`, `client.js`, `allocation.js` and their tests in the module
   for the changed public contract. Run relevant lightweight JavaScript checks
   when changing code; follow active build rules for requested C++ builds.
2. In a running UI, check empty/loading/error/stale states, grouped fields with
   different pool labels, exact large integers, step transitions and null gaps.
3. Check line versus stacked area, formatting/units, responsive width, small
   containers, axis readability, shared cursor position, tooltip flipping and
   pin/unpin, zoom/Now, and live refresh cancellation after query changes.
4. Check direct and node-proxied routes, resources, same-origin authentication,
   and teardown. Inspect console errors and request counts; settings changes
   should not trigger data fetches. For documentation-only changes, verify
   snippets against source and run the instruction checker without a full build.
