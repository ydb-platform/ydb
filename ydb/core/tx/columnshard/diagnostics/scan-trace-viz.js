(function(global) {
    'use strict';

    const TIME_RE = /^\s*([\d.]+)/;
    const KV_RE = /(\w+)='([^']*)'/g;
    const EVENT_NAME_RE = /YDB_CS_SCAN\.(\w+)\(/;

    const EVENT_CONFIG = {
        StartScan: {color: 'green', symbol: 'star', size: 14, y: 5},
        ColumnEngineForLogsSelect: {color: 'blue', symbol: 'diamond', size: 12, y: 4.5},
        ScanStartSource: {color: 'dodgerblue', symbol: 'triangle-up', size: 6, y: 3},
        ScanFinishSource: {color: 'orange', symbol: 'triangle-down', size: 6, y: 2},
        SendResult: {color: 'red', symbol: 'circle', size: 7, y: 1},
        AckReceived: {color: 'gray', symbol: 'x', size: 5, y: 0.5},
        ScanFinished: {color: 'darkgreen', symbol: 'star', size: 14, y: 5},
    };

    function escapeHtml(value) {
        return String(value).replace(/[&<>"']/g, (c) => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&#34;',
            "'": '&#39;',
        })[c]);
    }

    function renderError(container, prefix, message, traceUrl) {
        container.replaceChildren();
        const messageParagraph = document.createElement('p');
        messageParagraph.textContent = `${prefix}: ${message}`;
        container.appendChild(messageParagraph);

        const linkParagraph = document.createElement('p');
        const link = document.createElement('a');
        link.href = traceUrl;
        link.textContent = 'Open raw trace log';
        linkParagraph.appendChild(link);
        container.appendChild(linkParagraph);
    }

    function minEventTime(events) {
        let minT = Infinity;
        for (const event of events) {
            if (event.t < minT) {
                minT = event.t;
            }
        }
        return minT;
    }

    function parseLog(text) {
        const allEvents = [];
        for (const line of text.split('\n')) {
            const tMatch = line.match(TIME_RE);
            if (!tMatch) {
                continue;
            }
            const nameMatch = line.match(EVENT_NAME_RE);
            if (!nameMatch) {
                continue;
            }
            const params = {};
            let kvMatch;
            KV_RE.lastIndex = 0;
            while ((kvMatch = KV_RE.exec(line)) !== null) {
                params[kvMatch[1]] = kvMatch[2];
            }
            allEvents.push({t: parseFloat(tMatch[1]), name: nameMatch[1], params});
        }
        return allEvents;
    }

    function getScanId(params) {
        return params.scanId || 'unknown';
    }

    function groupEventsByScan(allEvents) {
        const groups = new Map();
        for (const ev of allEvents) {
            const scanId = getScanId(ev.params);
            if (!groups.has(scanId)) {
                groups.set(scanId, []);
            }
            groups.get(scanId).push(ev);
        }
        return [...groups.entries()].sort((a, b) => minEventTime(a[1]) - minEventTime(b[1]));
    }

    function normalizeScanTimes(events) {
        const startEv = events.find((e) => e.name === 'StartScan') || events[0];
        const t0 = startEv.t;
        return events.map((ev) => ({...ev, t: ev.t - t0}));
    }

    function classifyEvents(allEvents) {
        const sourceStarts = {};
        const sourceIntervals = [];
        const sendResults = [];

        for (const ev of allEvents) {
            const name = ev.name;
            const p = ev.params;
            const t = ev.t;
            const scanId = getScanId(p);

            if (name === 'ScanStartSource') {
                const sid = p.sourceId || '';
                const key = `${scanId}:${sid}`;
                sourceStarts[key] = {
                    t,
                    scanId,
                    blobBytes: parseInt(p.blobBytes || '0', 10),
                    rawBytes: parseInt(p.rawBytes || '0', 10),
                };
            } else if (name === 'ScanFinishSource') {
                const sid = p.sourceId || '';
                const key = `${scanId}:${sid}`;
                const startInfo = sourceStarts[key];
                if (startInfo) {
                    delete sourceStarts[key];
                    sourceIntervals.push({
                        scanId,
                        start_t: startInfo.t,
                        end_t: t,
                        sourceId: sid,
                        startBlobBytes: startInfo.blobBytes,
                        blobBytes: parseInt(p.blobBytes || '0', 10),
                        rawBytes: parseInt(p.rawBytes || '0', 10),
                        totalRows: parseInt(p.totalRows || '0', 10),
                        filteredRows: parseInt(p.filteredRows || '0', 10),
                    });
                }
            } else if (name === 'SendResult') {
                sendResults.push({
                    scanId,
                    t,
                    sourceId: p.sourceId || '',
                    rows: parseInt(p.rows || '0', 10),
                    bytes: parseInt(p.bytes || '0', 10),
                    cpuTimeMs: parseFloat(p.cpuTimeMs || '0'),
                    waitTimeMs: parseFloat(p.waitTimeMs || '0'),
                    elapsedMs: parseFloat(p.elapsedMs || '0'),
                    finished: p.finished || '0',
                });
            }
        }

        sendResults.sort((a, b) => a.t - b.t);
        sourceIntervals.sort((a, b) => a.start_t - b.start_t);
        return {sourceIntervals, sendResults};
    }

    function buildCumulativeSeries(sourceIntervals) {
        const finishEvents = sourceIntervals.map((si) => {
            let readBytes = si.blobBytes - si.startBlobBytes;
            if (readBytes < 0) {
                readBytes = si.blobBytes;
            }
            return {t: si.end_t, readBytes, sourceId: si.sourceId};
        }).sort((a, b) => a.t - b.t);

        const cumulTimes = [];
        const cumulMb = [];
        const cumulHover = [];
        let runningTotal = 0;
        for (const ev of finishEvents) {
            runningTotal += ev.readBytes;
            cumulTimes.push(ev.t);
            cumulMb.push(runningTotal / 1e6);
            cumulHover.push(
                `<b>Cumulative Bytes Read</b><br>` +
                `time: ${ev.t.toFixed(3)} ms<br>` +
                `total read: ${(runningTotal / 1e6).toFixed(1)} MB<br>` +
                `source ${escapeHtml(ev.sourceId)} finished<br>` +
                `this source read: ${(ev.readBytes / 1e6).toFixed(1)} MB`
            );
        }
        return {cumulTimes, cumulMb, cumulHover};
    }

    function buildActiveSeries(sourceIntervals) {
        const activeEv = [];
        for (const si of sourceIntervals) {
            activeEv.push({t: si.start_t, delta: 1, sourceId: si.sourceId, blobBytes: si.blobBytes});
            activeEv.push({t: si.end_t, delta: -1, sourceId: si.sourceId, blobBytes: si.blobBytes});
        }
        activeEv.sort((a, b) => a.t - b.t);

        const activeTimes = [];
        const activeCounts = [];
        const activeHover = [];
        let count = 0;
        for (const ev of activeEv) {
            count += ev.delta;
            activeTimes.push(ev.t);
            activeCounts.push(count);
            activeHover.push(
                `<b>Active Sources</b><br>` +
                `time: ${ev.t.toFixed(3)} ms<br>` +
                `count: ${count}<br>` +
                `source: ${escapeHtml(ev.sourceId)}<br>` +
                `blobBytes: ${(ev.blobBytes / 1e6).toFixed(1)} MB`
            );
        }
        return {activeTimes, activeCounts, activeHover};
    }

    function interpCumul(t, cumulTimes, cumulMb) {
        if (!cumulTimes.length) {
            return 0;
        }
        if (t < cumulTimes[0]) {
            return 0;
        }
        if (t >= cumulTimes[cumulTimes.length - 1]) {
            return cumulMb[cumulMb.length - 1];
        }
        let lo = 0;
        let hi = cumulTimes.length - 1;
        while (lo < hi) {
            const mid = Math.floor((lo + hi + 1) / 2);
            if (cumulTimes[mid] <= t) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return cumulMb[lo];
    }

    function buildSpeedSeries(cumulTimes, cumulMb, bucketMs) {
        const speedTimes = [];
        const speedValsMbs = [];
        const speedHover = [];
        if (cumulTimes.length < 2) {
            return {speedTimes, speedValsMbs, speedHover};
        }

        const tMin = cumulTimes[0];
        const tMax = cumulTimes[cumulTimes.length - 1];
        let bucketStart = tMin;
        while (bucketStart < tMax) {
            const bucketEnd = Math.min(bucketStart + bucketMs, tMax);
            const dtMs = bucketEnd - bucketStart;
            if (dtMs <= 0) {
                break;
            }
            const mbStart = interpCumul(bucketStart, cumulTimes, cumulMb);
            const mbEnd = interpCumul(bucketEnd, cumulTimes, cumulMb);
            const dMb = mbEnd - mbStart;
            const rateMbs = (dMb / dtMs) * 1000.0;
            const tMid = (bucketStart + bucketEnd) / 2.0;
            speedTimes.push(tMid);
            speedValsMbs.push(Math.max(0, rateMbs));
            speedHover.push(
                `<b>Read Speed (avg ${bucketMs.toFixed(0)}ms)</b><br>` +
                `bucket: ${bucketStart.toFixed(1)} – ${bucketEnd.toFixed(1)} ms<br>` +
                `speed: ${rateMbs.toFixed(1)} MB/s<br>` +
                `Δread: ${dMb.toFixed(1)} MB<br>` +
                `Δtime: ${dtMs.toFixed(1)} ms`
            );
            bucketStart = bucketEnd;
        }
        return {speedTimes, speedValsMbs, speedHover};
    }

    function formatHoverParam(key, value) {
        const safeKey = escapeHtml(key);
        const safeValue = escapeHtml(value);
        const num = parseFloat(value);
        if (!Number.isNaN(num)) {
            if (num > 1e6) {
                return `${safeKey}: ${(num / 1e6).toFixed(1)} M`;
            }
            if (num > 1e3) {
                return `${safeKey}: ${(num / 1e3).toFixed(1)} K`;
            }
            return `${safeKey}: ${safeValue}`;
        }
        return `${safeKey}: ${safeValue}`;
    }

    function addPlaceholderTrace(traces, xaxis, yaxis) {
        traces.push({
            type: 'scatter',
            x: [0],
            y: [0],
            mode: 'markers',
            marker: {opacity: 0},
            showlegend: false,
            hoverinfo: 'skip',
            xaxis,
            yaxis,
        });
    }

    function getScanTitle(scanId, events) {
        const startScan = events.find((e) => e.name === 'StartScan');
        const txId = startScan?.params.txId || events.find((e) => e.params.txId)?.params.txId || '?';
        const pathId = startScan?.params.pathId || events.find((e) => e.params.pathId)?.params.pathId || '?';
        return `Scan scanId=${scanId}, txId=${txId}, pathId=${pathId}`;
    }

    function buildFigure(allEvents, sourceIntervals, sendResults, cumul, active, speed) {
        const traces = [];
        const layout = {
            grid: {rows: 5, columns: 1, pattern: 'independent', roworder: 'top to bottom', ygap: 0.08},
            height: 1300,
            hovermode: 'closest',
            showlegend: true,
            legend: {
                orientation: 'v',
                x: 1.02,
                xanchor: 'left',
                y: 0.12,
                yanchor: 'bottom',
                font: {size: 10},
                bgcolor: 'rgba(255,255,255,0.8)',
                bordercolor: '#ddd',
                borderwidth: 1,
            },
            margin: {t: 20, r: 160, b: 90, l: 70},
            template: 'plotly_white',
        };

        let hasRow1 = false;
        let hasRow2 = false;
        let hasRow3 = false;
        let hasRow4 = false;
        let hasRow5 = false;

        if (cumul.cumulTimes.length) {
            hasRow1 = true;
            traces.push({
                type: 'scatter',
                x: cumul.cumulTimes,
                y: cumul.cumulMb,
                mode: 'lines',
                fill: 'tozeroy',
                fillcolor: 'rgba(31,119,180,0.25)',
                line: {color: 'rgb(31,119,180)', width: 1.5},
                name: 'Cumulative Read',
                hovertext: cumul.cumulHover,
                hoverinfo: 'text',
                showlegend: false,
                xaxis: 'x',
                yaxis: 'y',
            });
        }

        if (speed.speedTimes.length) {
            hasRow2 = true;
            traces.push({
                type: 'scatter',
                x: speed.speedTimes,
                y: speed.speedValsMbs,
                mode: 'lines',
                fill: 'tozeroy',
                fillcolor: 'rgba(140,86,75,0.25)',
                line: {color: 'rgb(140,86,75)', width: 1.5},
                name: 'Read Speed',
                hovertext: speed.speedHover,
                hoverinfo: 'text',
                showlegend: false,
                xaxis: 'x2',
                yaxis: 'y2',
            });
        }

        if (active.activeTimes.length) {
            hasRow3 = true;
            traces.push({
                type: 'scatter',
                x: active.activeTimes,
                y: active.activeCounts,
                mode: 'lines',
                fill: 'tozeroy',
                fillcolor: 'rgba(44,160,44,0.25)',
                line: {color: 'rgb(44,160,44)', width: 1.5},
                name: 'Active Sources',
                hovertext: active.activeHover,
                hoverinfo: 'text',
                showlegend: false,
                xaxis: 'x3',
                yaxis: 'y3',
            });
        }

        const sendTimes = sendResults.map((sr) => sr.t);
        const sendRows = sendResults.map((sr) => sr.rows);
        if (sendTimes.length) {
            hasRow4 = true;
            const hoverSendRows = sendResults.map((sr) =>
                `<b>SendResult</b><br>` +
                `time: ${sr.t.toFixed(3)} ms<br>` +
                `sourceId: ${escapeHtml(sr.sourceId)}<br>` +
                `rows: ${sr.rows}<br>` +
                `bytes: ${escapeHtml(sr.bytes)}<br>` +
                `cpuTimeMs: ${sr.cpuTimeMs.toFixed(1)}<br>` +
                `waitTimeMs: ${sr.waitTimeMs.toFixed(1)}<br>` +
                `elapsedMs: ${sr.elapsedMs.toFixed(3)}<br>` +
                `finished: ${escapeHtml(sr.finished)}`
            );

            const stemX = [];
            const stemY = [];
            for (let i = 0; i < sendTimes.length; ++i) {
                stemX.push(sendTimes[i], sendTimes[i], null);
                stemY.push(0, sendRows[i], null);
            }
            traces.push({
                type: 'scatter',
                x: stemX,
                y: stemY,
                mode: 'lines',
                line: {color: 'rgba(148,103,189,0.6)', width: 2},
                name: 'Rows (stems)',
                showlegend: false,
                hoverinfo: 'skip',
                xaxis: 'x4',
                yaxis: 'y4',
            });
            traces.push({
                type: 'scatter',
                x: sendTimes,
                y: sendRows,
                mode: 'markers',
                marker: {color: 'rgba(148,103,189,0.9)', size: 7, symbol: 'circle'},
                name: 'Rows Returned',
                hovertext: hoverSendRows,
                hoverinfo: 'text',
                showlegend: false,
                xaxis: 'x4',
                yaxis: 'y4',
            });
        }

        const eventsByType = {};
        for (const ev of allEvents) {
            if (!eventsByType[ev.name]) {
                eventsByType[ev.name] = [];
            }
            eventsByType[ev.name].push(ev);
        }

        for (const [etype, cfg] of Object.entries(EVENT_CONFIG)) {
            const evs = eventsByType[etype] || [];
            if (!evs.length) {
                continue;
            }
            const hoverTexts = evs.map((e) => {
                const parts = [`<b>${escapeHtml(e.name)}</b>`, `time: ${e.t.toFixed(3)} ms`];
                for (const [k, v] of Object.entries(e.params)) {
                    if (k === 'pathId' || k === 'tabletId' || k === 'txId' || k === 'scanId') {
                        continue;
                    }
                    parts.push(formatHoverParam(k, v));
                }
                return parts.join('<br>');
            });
            traces.push({
                type: 'scatter',
                x: evs.map((e) => e.t),
                y: evs.map(() => cfg.y),
                mode: 'markers',
                name: etype,
                marker: {symbol: cfg.symbol, size: cfg.size, color: cfg.color, opacity: 0.7},
                hovertext: hoverTexts,
                hoverinfo: 'text',
                xaxis: 'x5',
                yaxis: 'y5',
            });
            hasRow5 = true;
        }

        const maxBlob = sourceIntervals.reduce((max, si) => Math.max(max, si.blobBytes), 0);
        for (const si of sourceIntervals) {
            const duration = si.end_t - si.start_t;
            const blobMb = si.blobBytes / 1e6;
            const rawMb = si.rawBytes / 1e6;
            const intensity = maxBlob > 0 ? si.blobBytes / maxBlob : 0;
            const b = Math.floor(150 + 105 * intensity);
            traces.push({
                type: 'scatter',
                x: [si.start_t, si.end_t],
                y: [3, 2],
                mode: 'lines',
                line: {color: `rgba(100,100,${b},0.25)`, width: 1},
                showlegend: false,
                hovertext: [
                    `<b>Source ${escapeHtml(si.sourceId)}</b><br>` +
                    `duration: ${duration.toFixed(1)} ms<br>` +
                    `blobBytes: ${blobMb.toFixed(1)} MB<br>` +
                    `rawBytes: ${rawMb.toFixed(1)} MB<br>` +
                    `totalRows: ${si.totalRows}<br>` +
                    `filteredRows: ${si.filteredRows}`,
                ],
                hoverinfo: 'text',
                xaxis: 'x5',
                yaxis: 'y5',
            });
            hasRow5 = true;
        }

        if (!hasRow1) {
            addPlaceholderTrace(traces, 'x', 'y');
        }
        if (!hasRow2) {
            addPlaceholderTrace(traces, 'x2', 'y2');
        }
        if (!hasRow3) {
            addPlaceholderTrace(traces, 'x3', 'y3');
        }
        if (!hasRow4) {
            addPlaceholderTrace(traces, 'x4', 'y4');
        }
        if (!hasRow5) {
            addPlaceholderTrace(traces, 'x5', 'y5');
        }

        layout.yaxis = {title: {text: 'MB'}};
        layout.yaxis2 = {title: {text: 'MB/s'}};
        layout.yaxis3 = {title: {text: 'Count'}};
        layout.yaxis4 = {title: {text: 'rows'}};
        layout.yaxis5 = {
            title: {text: 'Events timeline'},
            tickmode: 'array',
            tickvals: [0.5, 1, 2, 3, 4.5, 5],
            ticktext: ['Ack', 'Send', 'Finish', 'Start', 'Select', 'Scan'],
        };
        layout.xaxis5 = {
            title: {text: 'Time since StartScan (ms)'},
            rangeslider: {visible: true, thickness: 0.03},
        };

        return {traces, layout};
    }

    function renderScanFigure(container, figure, traceUrl) {
        if (!figure.traces.length) {
            return false;
        }
        try {
            global.Plotly.newPlot(container, figure.traces, figure.layout, {responsive: true});
            return true;
        } catch (plotError) {
            const message = plotError && plotError.message ? plotError.message : String(plotError);
            renderError(container, 'Failed to render chart', message, traceUrl);
            return false;
        }
    }

    function renderScanTraceVisualization(traceUrl, containerId) {
        const container = document.getElementById(containerId);
        if (!container) {
            return;
        }
        container.innerHTML = '<p>Loading trace log...</p>';

        fetch(traceUrl, {credentials: 'same-origin'})
            .then((response) => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                return response.text();
            })
            .then((text) => {
                const allEvents = parseLog(text);
                if (!allEvents.length) {
                    container.innerHTML = '<p>No YDB_CS_SCAN events found in trace log.</p>';
                    return;
                }
                if (!global.Plotly) {
                    container.innerHTML = '<p>Plotly is not loaded.</p>';
                    return;
                }

                const scanGroups = groupEventsByScan(allEvents);
                container.innerHTML = '';
                let rendered = 0;

                for (let i = 0; i < scanGroups.length; ++i) {
                    const [scanId, scanEvents] = scanGroups[i];
                    const normalizedEvents = normalizeScanTimes(scanEvents);
                    const {sourceIntervals, sendResults} = classifyEvents(normalizedEvents);
                    const cumul = buildCumulativeSeries(sourceIntervals);
                    const active = buildActiveSeries(sourceIntervals);
                    const speed = buildSpeedSeries(cumul.cumulTimes, cumul.cumulMb, 300.0);
                    const scanTitle = getScanTitle(scanId, scanEvents);
                    const figure = buildFigure(normalizedEvents, sourceIntervals, sendResults, cumul, active, speed);

                    const section = document.createElement('div');
                    section.style.marginBottom = '48px';
                    if (i > 0) {
                        section.style.borderTop = '2px solid #ddd';
                        section.style.paddingTop = '16px';
                    }

                    const titleEl = document.createElement('h4');
                    titleEl.style.margin = '0 0 12px 0';
                    titleEl.style.fontFamily = 'Arial, sans-serif';
                    titleEl.textContent = scanTitle;
                    section.appendChild(titleEl);

                    const chartDiv = document.createElement('div');
                    chartDiv.style.width = '100%';
                    chartDiv.id = `scan-trace-viz-${scanId}`;
                    section.appendChild(chartDiv);
                    container.appendChild(section);

                    if (renderScanFigure(chartDiv, figure, traceUrl)) {
                        rendered += 1;
                    }
                }

                if (!rendered) {
                    container.innerHTML = '<p>No plottable YDB_CS_SCAN events found in trace log.</p>';
                }
            })
            .catch((error) => {
                const message = error && error.message ? error.message : String(error);
                renderError(container, 'Failed to load trace', message, traceUrl);
            });
    }

    global.renderScanTraceVisualization = renderScanTraceVisualization;
})(window);
