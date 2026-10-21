function createTraceActionContext() {
    function rootWindow() {
        return typeof window !== 'undefined' ? window : null;
    }

    function rootDocument() {
        return typeof document !== 'undefined' ? document : null;
    }

    function elementById(id) {
        var doc = rootDocument();
        return doc && doc.getElementById ? doc.getElementById(id) : null;
    }

    function addWindowListener(type, listener) {
        var win = rootWindow();
        if (win && win.addEventListener) win.addEventListener(type, listener);
    }

    function requestFrame(fn) {
        if (typeof scheduleRenderDomWork === 'function') {
            return scheduleRenderDomWork('', function(visualCtx) {
                return fn(visualCtx);
            }, {
                epochScopes: ['trace'],
                label: 'action-frame',
                surfaces: ['unknown']
            });
        }
        var win = rootWindow();
        return win && win.requestAnimationFrame
            ? win.requestAnimationFrame(fn)
            : setTimeout(fn, 0);
    }

    function cancelFrame(id) {
        if (typeof cancelRenderFrameWork === 'function' && cancelRenderFrameWork(id)) return;
        var win = rootWindow();
        var cancel = win && win.cancelAnimationFrame ? win.cancelAnimationFrame : clearTimeout;
        cancel(id);
    }

    function scheduleTimeout(fn, delay) {
        return setTimeout(fn, delay);
    }

    function scheduleInterval(fn, delay) {
        return setInterval(fn, delay);
    }

    function cancelTimeout(id) {
        clearTimeout(id);
    }

    return {
        allRules: function() { return currentAllRules(); },
        diffState: function() { return diffRuntime(); },
        fullscreenState: function() { return fullscreenRuntime(); },
        searchState: function() { return searchRuntime(); },
        traceGroups: function() { return currentTraceGroups(); },
        traceState: function() { return traceRuntime(); },
        uiState: function() { return currentUiState(); },
        dom: {
            addWindowListener: addWindowListener,
            byId: elementById
        },
        timers: {
            clearTimeout: cancelTimeout,
            setInterval: scheduleInterval,
            setTimeout: scheduleTimeout
        },
        frame: {
            cancel: cancelFrame,
            request: requestFrame
        }
    };
}

var TraceActionContext = createTraceActionContext();
