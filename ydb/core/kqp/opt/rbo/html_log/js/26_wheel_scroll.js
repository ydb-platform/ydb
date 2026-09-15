/* ══════════════════════════════════════════════════════════════
   Horizontal wheel scroll
   ══════════════════════════════════════════════════════════════ */

var WHEEL_LINE_PX = 36;
var WHEEL_LINES_PER_NOTCH = 3;
var WHEEL_PIXEL_NOTCH_PX = 100;
var WHEEL_PAGE_NOTCHES = 6;
var WHEEL_COARSE_NOTCH_PX = 108;
var WHEEL_MAX_NOTCHES_PER_EVENT = 3;
var WHEEL_FRAME_MS = 1000 / 60;
var WHEEL_DIRECT_GAIN = 1.0;
var WHEEL_COARSE_BASE_GAIN = 0.24;
var WHEEL_COARSE_BURST_GAIN = 0.11;
var WHEEL_COARSE_MAX_GAIN = 0.64;
var WHEEL_COARSE_DECAY = 0.72;
var WHEEL_COARSE_SMOOTH_DECAY = 0.84;
var WHEEL_COARSE_STOP = 1.0;
var WHEEL_COARSE_MAX_PENDING = 960;
var WHEEL_COARSE_MAX_STEP = 68;
var WHEEL_BURST_WINDOW_MS = 160;

var wheelScrollRemaining = 0;
var wheelScrollFrame = 0;
var wheelScrollLastFrameTime = 0;
var wheelScrollLastEventTime = 0;
var wheelScrollBurst = 0;
var wheelScrollDirection = 0;

function normalizedWheelDeltaY(event, traceEl) {
    if (event.deltaMode === 1) return event.deltaY * WHEEL_LINE_PX;
    if (event.deltaMode === 2) return event.deltaY * Math.max(1, traceEl.clientWidth);
    return event.deltaY;
}

function normalizedWheelDeltaX(event, traceEl) {
    if (event.deltaMode === 1) return event.deltaX * WHEEL_LINE_PX;
    if (event.deltaMode === 2) return event.deltaX * Math.max(1, traceEl.clientWidth);
    return event.deltaX;
}

function isCoarseWheelEvent(event) {
    if (event.deltaMode !== 0) return true;

    var rawDelta = Math.abs(event.deltaY || 0);
    var wheelDelta = Math.abs(event.wheelDelta || 0);
    if (wheelDelta >= 120 && rawDelta >= 40 && Math.abs(wheelDelta % 120) < 0.001) return true;
    return rawDelta >= 40 && Math.abs(rawDelta - Math.round(rawDelta)) < 0.001;
}

function clampWheelNotches(notches) {
    if (notches > WHEEL_MAX_NOTCHES_PER_EVENT) return WHEEL_MAX_NOTCHES_PER_EVENT;
    if (notches < -WHEEL_MAX_NOTCHES_PER_EVENT) return -WHEEL_MAX_NOTCHES_PER_EVENT;
    return notches;
}

function snapWheelNotches(notches) {
    var direction = wheelDeltaDirection(notches);
    var abs = Math.abs(notches);
    if (!direction) return 0;
    if (abs >= 0.85 && abs <= 1.25) return direction;
    if (abs >= 1.75 && abs <= 2.25) return direction * 2;
    if (abs >= 2.75 && abs <= 3.25) return direction * 3;
    return notches;
}

function wheelDeltaNotches(event) {
    var wheelDelta = Number(event.wheelDelta || 0);
    if (!isFinite(wheelDelta) || Math.abs(wheelDelta) < 120) return null;
    if (Math.abs(Math.abs(wheelDelta) % 120) >= 0.001) return null;

    var deltaY = Number(event.deltaY || 0);
    if (deltaY && wheelDeltaDirection(deltaY) === wheelDeltaDirection(wheelDelta)) {
        return null;
    }
    return -wheelDelta / 120;
}

function coarseWheelNotches(event, traceEl) {
    var wheelDeltaFallback = wheelDeltaNotches(event);
    if (wheelDeltaFallback !== null) {
        return clampWheelNotches(snapWheelNotches(wheelDeltaFallback));
    }

    if (event.deltaMode === 1) {
        return clampWheelNotches(snapWheelNotches((event.deltaY || 0) / WHEEL_LINES_PER_NOTCH));
    }
    if (event.deltaMode === 2) {
        return clampWheelNotches((event.deltaY || 0) * WHEEL_PAGE_NOTCHES);
    }
    return clampWheelNotches(snapWheelNotches((event.deltaY || 0) / WHEEL_PIXEL_NOTCH_PX));
}

function resetWheelScrollBurst() {
    wheelScrollLastEventTime = 0;
    wheelScrollBurst = 0;
    wheelScrollDirection = 0;
}

function wheelDeltaDirection(delta) {
    if (delta < 0) return -1;
    if (delta > 0) return 1;
    return 0;
}

function wheelEventTime(event) {
    var time = Number(event.timeStamp);
    return isFinite(time) && time > 0 ? time : Date.now();
}

function coarseWheelGainForEvent(event, deltaY) {
    var direction = wheelDeltaDirection(deltaY);
    var time = wheelEventTime(event);

    if (!direction ||
        direction !== wheelScrollDirection ||
        !wheelScrollLastEventTime ||
        time - wheelScrollLastEventTime > WHEEL_BURST_WINDOW_MS) {
        wheelScrollBurst = 0;
    } else {
        wheelScrollBurst++;
    }

    wheelScrollDirection = direction;
    wheelScrollLastEventTime = time;
    return Math.min(
        WHEEL_COARSE_MAX_GAIN,
        WHEEL_COARSE_BASE_GAIN + wheelScrollBurst * WHEEL_COARSE_BURST_GAIN
    );
}

function scaledWheelDeltaY(event, traceEl, coarse) {
    if (!coarse) return normalizedWheelDeltaY(event, traceEl) * WHEEL_DIRECT_GAIN;

    var notches = coarseWheelNotches(event, traceEl);
    return notches * WHEEL_COARSE_NOTCH_PX * coarseWheelGainForEvent(event, notches);
}

function coarseWheelScrollDistance(event, traceEl) {
    return scaledWheelDeltaY(event, traceEl, true) / Math.max(0.05, 1 - WHEEL_COARSE_DECAY);
}

function clampWheelScrollRemaining(remaining) {
    if (remaining > WHEEL_COARSE_MAX_PENDING) return WHEEL_COARSE_MAX_PENDING;
    if (remaining < -WHEEL_COARSE_MAX_PENDING) return -WHEEL_COARSE_MAX_PENDING;
    return remaining;
}

function wheelScrollStepForFrame(remaining, frameScale) {
    var progress = 1 - Math.pow(WHEEL_COARSE_SMOOTH_DECAY, frameScale);
    var step = remaining * progress;
    var maxStepScale = Math.max(0.5, Math.min(1.5, frameScale));
    var maxStep = WHEEL_COARSE_MAX_STEP * maxStepScale;
    if (step > maxStep) return maxStep;
    if (step < -maxStep) return -maxStep;
    return step;
}

function applyTraceWheelDelta(traceEl, delta) {
    var before = traceEl.scrollLeft || 0;
    var max = Math.max(0, traceScrollWidthValue(traceEl) - traceEl.clientWidth);
    var next = Math.max(0, Math.min(max, before + delta));
    return setTraceScrollLeft(traceEl, next, {
        maxScrollLeft: max,
        runVirtualScrollHandler: true
    });
}

function nestedWheelScrollTarget(event) {
    if (!event || !event.target || !event.target.closest) return null;

    var tabbedInfo = event.target.closest('.rule-info-panel.info-panel-tabbed');
    var tabPanel = tabbedInfo && tabbedInfo.querySelector
        ? tabbedInfo.querySelector('.info-tab-panel')
        : null;

    return event.target.closest('.rule-tree-wrap') ||
        event.target.closest('.info-tab-panel') ||
        tabPanel ||
        event.target.closest('.rule-info-panel');
}

function canScrollVerticallyInDirection(el, deltaY) {
    if (!el) return false;

    var direction = wheelDeltaDirection(deltaY);
    if (!direction) return false;

    var scrollTop = el.scrollTop || 0;
    var maxScroll = Math.max(0, (el.scrollHeight || 0) - (el.clientHeight || 0));
    if (!canScrollVertically(el)) return false;

    if (direction < 0) return scrollTop > 0.5;
    return scrollTop < maxScroll - 0.5;
}

function canScrollVertically(el) {
    if (!el) return false;
    var maxScroll = Math.max(0, (el.scrollHeight || 0) - (el.clientHeight || 0));
    return maxScroll > 0.5;
}

function canScrollHorizontallyInDirection(el, deltaX) {
    if (!el) return false;

    var direction = wheelDeltaDirection(deltaX);
    if (!direction) return false;

    var scrollLeft = el.scrollLeft || 0;
    var maxScroll = Math.max(0, (el.scrollWidth || 0) - (el.clientWidth || 0));
    if (!canScrollHorizontally(el)) return false;

    if (direction < 0) return scrollLeft > 0.5;
    return scrollLeft < maxScroll - 0.5;
}

function canScrollHorizontally(el) {
    if (!el) return false;
    var maxScroll = Math.max(0, (el.scrollWidth || 0) - (el.clientWidth || 0));
    return maxScroll > 0.5;
}

function verticalWheelScrollInfo(target, deltaY) {
    if (!wheelDeltaDirection(deltaY)) return null;
    return {
        axis: 'vertical',
        target: target,
        scrollable: canScrollVertically(target),
        canMove: canScrollVerticallyInDirection(target, deltaY)
    };
}

function horizontalWheelScrollInfo(target, deltaX) {
    if (!wheelDeltaDirection(deltaX)) return null;
    return {
        axis: 'horizontal',
        target: target,
        scrollable: canScrollHorizontally(target),
        canMove: canScrollHorizontallyInDirection(target, deltaX)
    };
}

function nestedWheelScrollInfo(event, traceEl) {
    var target = nestedWheelScrollTarget(event);
    if (!target) return null;

    var deltaX = normalizedWheelDeltaX(event, traceEl);
    var deltaY = normalizedWheelDeltaY(event, traceEl);

    if (event.shiftKey) {
        return horizontalWheelScrollInfo(target, deltaY);
    }

    if (Math.abs(deltaX) > Math.abs(deltaY)) {
        var horizontal = horizontalWheelScrollInfo(target, deltaX);
        var vertical = verticalWheelScrollInfo(target, deltaY);
        if (horizontal && !horizontal.scrollable) {
            return horizontal;
        }
        if (horizontal && horizontal.scrollable) {
            if (horizontal.canMove || !(vertical && vertical.scrollable && vertical.canMove)) {
                return horizontal;
            }
        }
        return vertical || horizontal;
    }

    return verticalWheelScrollInfo(target, deltaY);
}

function scheduleWheelInertia(traceEl) {
    if (wheelScrollFrame) return;

    wheelScrollFrame = scheduleRenderDomWork('wheel-scroll-inertia', function(visualCtx) {
        wheelScrollFrame = 0;
        if (Math.abs(wheelScrollRemaining) < WHEEL_COARSE_STOP) {
            wheelScrollRemaining = 0;
            wheelScrollLastFrameTime = 0;
            return;
        }

        var now = Date.now();
        var elapsed = wheelScrollLastFrameTime ? now - wheelScrollLastFrameTime : WHEEL_FRAME_MS;
        wheelScrollLastFrameTime = now;
        elapsed = Math.max(1, Math.min(50, elapsed));
        var frameScale = elapsed / WHEEL_FRAME_MS;
        var step = wheelScrollStepForFrame(wheelScrollRemaining, frameScale);

        if (!applyTraceWheelDelta(traceEl, step)) {
            wheelScrollRemaining = 0;
            wheelScrollLastFrameTime = 0;
            return;
        }

        wheelScrollRemaining -= step;
        scheduleWheelInertia(traceEl);
    }, {
        epochScopes: ['trace', 'virtual'],
        label: 'wheel-scroll-inertia',
        surfaces: ['trace-canvas', 'trace-scrollbar'],
        onDiscard: function() {
            wheelScrollFrame = 0;
            wheelScrollRemaining = 0;
            wheelScrollLastFrameTime = 0;
        }
    });
}

function resetWheelScrollRuntime() {
    if (wheelScrollFrame) {
        cancelRenderFrameWork(wheelScrollFrame);
        wheelScrollFrame = 0;
    }
    wheelScrollRemaining = 0;
    wheelScrollLastFrameTime = 0;
    resetWheelScrollBurst();
}

function initWheelScroll() {
    var traceEl = document.querySelector('.trace');
    if (!traceEl) return;
    traceEl.addEventListener('wheel', function(event) {
        if (event.ctrlKey) return;
        var nestedInfo = nestedWheelScrollInfo(event, traceEl);
        if (nestedInfo && nestedInfo.scrollable) {
            if (!nestedInfo.canMove && nestedInfo.axis === 'horizontal' && event.preventDefault) {
                event.preventDefault();
            }
            return;
        }
        if (Math.abs(event.deltaY) > Math.abs(event.deltaX)) {
            var coarse = isCoarseWheelEvent(event);
            if (coarse) {
                wheelScrollRemaining = clampWheelScrollRemaining(
                    wheelScrollRemaining + coarseWheelScrollDistance(event, traceEl)
                );
                scheduleWheelInertia(traceEl);
            } else {
                wheelScrollRemaining = 0;
                resetWheelScrollBurst();
                applyTraceWheelDelta(traceEl, scaledWheelDeltaY(event, traceEl, false));
            }
            event.preventDefault();
        }
    }, { passive: false });
}
