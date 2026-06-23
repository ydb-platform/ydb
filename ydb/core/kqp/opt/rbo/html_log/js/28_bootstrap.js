/* ══════════════════════════════════════════════════════════════
   Initialization
   ══════════════════════════════════════════════════════════════ */

function initializeOptimizerTraceDom() {
    var mount = mountRuntime();
    if (mount.domInitialized || !hasDOM()) return;
    mount.domInitialized = true;

    ensureActiveTraceLoaded();
    renderOptimizerTraceAppShell();
    renderTraceShell();
    syncTracePicker();
    initTheme();
    initTopBarControls();
    initRuleResize();
    initWheelScroll();
    TraceActionEvents.bind();
    initFullscreenScrollbarReveal();
    initDiffMoveArrows();
    document.addEventListener('click', closeFullscreenOnOutsideClick, true);
    document.addEventListener('click', closeTopBarPopovers, true);
    window.addEventListener('keydown', function(ev) {
        if (ev.key === 'Escape') {
            closeTopBarPopovers();
            if (isFullscreenOpen()) {
                closeRuleFullscreen(ev);
            }
        }
    });

    initLazyRuleRendering();
    updateDiffButton();
    scheduleTraceAnchorLineUpdate();
}

function reloadOptimizerTraceData() {
    var trace = traceRuntime();
    var index = trace.activeTraceIndex;
    if (trace.activeTraceLoaded) saveActiveTraceSession();
    resetTraceSwitchRuntime(index);
    trace.activeTraceLoaded = false;
    loadTraceData(index);
    if (hasDOM() && mountRuntime().domInitialized) {
        renderLoadedTrace({ resetScroll: true });
    }
}

function bindOptimizerTraceDataLoadedEvent() {
    var mount = mountRuntime();
    if (mount.dataListenerBound || !hasDOM() || !document.addEventListener) return;
    document.addEventListener('optimizer-trace-data-loaded', reloadOptimizerTraceData);
    mount.dataListenerBound = true;
}

function mountOptimizerTraceViewer(options) {
    options = options || {};
    var mount = mountRuntime();
    if (mount.started) return OptimizerTraceViewer;
    mount.started = true;

    if (!hasDOM()) {
        ensureActiveTraceLoaded();
        return OptimizerTraceViewer;
    }

    bindOptimizerTraceDataLoadedEvent();
    if (!options.deferDom &&
        document.readyState &&
        document.readyState !== 'loading') {
        initializeOptimizerTraceDom();
        return OptimizerTraceViewer;
    }

    if (!document.readyState) ensureActiveTraceLoaded();

    if (!mount.domListenerBound && typeof window !== 'undefined' && window.addEventListener) {
        window.addEventListener('DOMContentLoaded', initializeOptimizerTraceDom);
        mount.domListenerBound = true;
    }
    return OptimizerTraceViewer;
}


function optimizerTraceBuildProfile() {
    return typeof TRACE_VIEW_BUILD_PROFILE === 'string' ? TRACE_VIEW_BUILD_PROFILE : 'debug';
}

function optimizerTraceReleaseApi() {
    return {
        actions: TraceActions,
        getTraceData: traceDataRoot,
        mount: mountOptimizerTraceViewer
    };
}


var OptimizerTraceViewer = (function(root) {
    var api = optimizerTraceReleaseApi();

    if (root) root.OptimizerTraceViewer = api;
    return api;
})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : this));

OptimizerTraceViewer.mount();
