/* ══════════════════════════════════════════════════════════════
   Rule tile resize
   ══════════════════════════════════════════════════════════════ */

function loadRuleWidth() {
    try {
        var saved = window.localStorage.getItem(RULE_WIDTH_STORAGE_KEY);
        return saved ? clampRuleWidth(saved) : RULE_WIDTH_DEFAULT;
    } catch (err) {
        return RULE_WIDTH_DEFAULT;
    }
}

function applyRuleWidth(width) {
    var clamped = clampRuleWidth(width);
    invalidateTraceMeasuredWidthCache();
    document.documentElement.style.setProperty('--rule-width', clamped + 'px');
    return clamped;
}

function previewRuleResizeWidth(drag, width) {
    var clamped = clampRuleWidth(width);
    invalidateTraceScrollbarGeometry();
    if (drag) {
        drag.currentWidth = clamped;
        drag.previewWidthPending = true;
    }
    return clamped;
}

function commitRuleResizeWidth(drag, width) {
    var clamped = applyRuleWidth(width);
    if (drag) {
        drag.currentWidth = clamped;
        drag.previewWidthPending = false;
        drag.globalWidthCommitted = true;
    }
    return clamped;
}

function saveRuleWidth(width) {
    try {
        window.localStorage.setItem(RULE_WIDTH_STORAGE_KEY, String(clampRuleWidth(width)));
    } catch (err) {
        /* Storage can be unavailable for local files in some browsers. */
    }
}

function initRuleWidth() {
    applyRuleWidth(loadRuleWidth());
}

function parseRuleCellId(id) {
    if (!id || id.indexOf('rule-') !== 0) return null;
    var parts = id.substring(5).split('-');
    if (parts.length !== 3) return null;

    var si = Number(parts[0]);
    var gi = Number(parts[1]);
    var ri = Number(parts[2]);
    if (!Number.isFinite(si) || !Number.isFinite(gi) || !Number.isFinite(ri)) {
        return null;
    }
    return { si: si, gi: gi, ri: ri };
}

function ruleResizeAnchorClientLeft(cell) {
    if (!cell || !cell.getBoundingClientRect) return null;
    var rect = cell.getBoundingClientRect();
    var left = rect ? Number(rect.left) : NaN;
    return Number.isFinite(left) ? left : null;
}

function ruleResizeAnchorViewportOffset(cell, traceEl, ref) {
    if (ref && traceEl) {
        var interval = ruleLayoutInterval(ref.si, ref.gi, ref.ri);
        if (interval && Number.isFinite(interval.left)) {
            return interval.left - traceScrollLeftValue(traceEl);
        }
    }

    var cellLeft = ruleResizeAnchorClientLeft(cell);
    var traceRect = traceEl && traceEl.getBoundingClientRect
        ? traceEl.getBoundingClientRect()
        : null;
    var traceLeft = traceRect ? Number(traceRect.left) : NaN;
    if (Number.isFinite(cellLeft) && Number.isFinite(traceLeft)) {
        return cellLeft - traceLeft;
    }

    return Number.isFinite(cellLeft) ? cellLeft : null;
}

function ruleResizeTraceElement(drag) {
    if (!hasDOM()) return null;

    if (drag && drag.traceEl && drag.traceEl.isConnected !== false) {
        return drag.traceEl;
    }

    if (!document.querySelector) return null;

    var cell = drag && drag.anchorRuleId && document.getElementById
        ? document.getElementById(drag.anchorRuleId)
        : null;
    var traceEl = cell && cell.closest ? cell.closest('.trace') : null;
    if (!traceEl) traceEl = document.querySelector('.trace');
    if (!traceEl || traceEl.isConnected === false) return null;
    return traceEl;
}

function ruleResizeAnchorRef(drag) {
    if (!drag) return null;
    if (drag.anchorRef) return drag.anchorRef;
    return parseRuleCellId(drag.anchorRuleId);
}

function ruleResizeDragViewportOffset(drag, traceEl) {
    var viewportOffset = Number(drag && drag.anchorViewportOffset);
    if (Number.isFinite(viewportOffset)) return viewportOffset;

    var anchorClientLeft = Number(drag && drag.anchorClientLeft);
    if (!Number.isFinite(anchorClientLeft)) return null;

    var traceRect = traceEl && traceEl.getBoundingClientRect
        ? traceEl.getBoundingClientRect()
        : null;
    var traceLeft = traceRect ? Number(traceRect.left) : NaN;
    return Number.isFinite(traceLeft) ? anchorClientLeft - traceLeft : anchorClientLeft;
}

function ruleResizeModelAnchorScrollLeft(drag, model, traceEl) {
    var ref = ruleResizeAnchorRef(drag);
    if (!ref || !model || !model.rules || !traceEl) return null;

    var interval = model.rules[ruleKey(ref.si, ref.gi, ref.ri)];
    if (!interval || !Number.isFinite(interval.left)) return null;

    var viewportOffset = ruleResizeDragViewportOffset(drag, traceEl);
    if (!Number.isFinite(viewportOffset)) return null;

    var desired = interval.left - viewportOffset;
    if (!Number.isFinite(desired)) return null;
    return Math.max(0, Math.min(ruleResizeRestorableMaxScrollLeft(model, traceEl), desired));
}

function ruleResizeViewportWidth(traceEl) {
    var viewport = traceEl && (traceEl.clientWidth || window.innerWidth || 0);
    return Number.isFinite(viewport) && viewport > 0 ? viewport : 0;
}

function ruleResizeRestorableMaxScrollLeft(model, traceEl) {
    return traceRestorableMaxScrollLeft(model, traceEl);
}

function ruleResizeDesiredScrollLeft(drag, model) {
    var traceEl = ruleResizeTraceElement(drag);
    if (!traceEl) return null;

    var desired = ruleResizeLogicalAnchorScrollLeft(drag && drag.logicalAnchor, model, traceEl);
    if (Number.isFinite(desired)) return desired;

    return ruleResizeModelAnchorScrollLeft(drag, model, traceEl);
}

function captureRuleResizeLogicalAnchor(drag) {
    if (!drag || !drag.anchorRuleId || typeof document === 'undefined') return false;

    var traceEl = ruleResizeTraceElement(drag);
    if (!traceEl) return null;

    var ref = ruleResizeAnchorRef(drag);
    if (!ref) return null;

    var interval = ruleLayoutInterval(ref.si, ref.gi, ref.ri);
    if (!interval) return null;

    var screenOffset = null;
    var cell = document.getElementById ? document.getElementById(drag.anchorRuleId) : null;
    if (cell && cell.getBoundingClientRect && traceEl.getBoundingClientRect) {
        var cellRect = cell.getBoundingClientRect();
        var traceRect = traceEl.getBoundingClientRect();
        if (cellRect &&
            traceRect &&
            Number.isFinite(cellRect.left) &&
            Number.isFinite(traceRect.left)) {
            screenOffset = cellRect.left - traceRect.left;
        }
    }
    if (!Number.isFinite(screenOffset)) {
        screenOffset = interval.left - traceScrollLeftValue(traceEl);
    }
    if (!Number.isFinite(screenOffset)) return null;

    drag.traceEl = traceEl;
    drag.logicalAnchor = {
        kind: 'rule',
        si: ref.si,
        gi: ref.gi,
        ri: ref.ri,
        offset: screenOffset,
        left: interval.left,
        right: interval.right,
        width: interval.width,
        sourceOpen: effectiveRuleOpen(ref.si, ref.gi, ref.ri)
    };
    return drag.logicalAnchor;
}

function ruleResizeLogicalAnchorScrollLeft(anchor, model, traceEl) {
    if (!anchor || !model || !traceEl) return null;

    if (ruleResizeViewportWidth(traceEl) <= 0) return null;

    var interval = anchorIntervalForRef(model, anchor);
    if (!interval || !Number.isFinite(interval.left)) return null;

    var desired = interval.left - anchor.offset;
    if (!Number.isFinite(desired)) return null;
    return Math.max(0, Math.min(ruleResizeRestorableMaxScrollLeft(model, traceEl), desired));
}

function refineRuleResizeLogicalAnchorFromDom(anchor, model, traceEl) {
    if (!anchor || !model || !traceEl || !hasDOM() || !traceEl.getBoundingClientRect) return null;

    var domEl = exactAnchorDomElement(anchor);
    if (!domEl || !domEl.getBoundingClientRect) return null;

    if (ruleResizeViewportWidth(traceEl) <= 0) return null;

    var traceRect = traceEl.getBoundingClientRect();
    var rect = domEl.getBoundingClientRect();
    if (!traceRect ||
        !rect ||
        !Number.isFinite(traceRect.left) ||
        !Number.isFinite(rect.left)) {
        return null;
    }

    var desired = traceScrollLeftValue(traceEl) + rect.left - (traceRect.left + anchor.offset);
    if (!Number.isFinite(desired)) return null;
    desired = Math.max(0, Math.min(ruleResizeRestorableMaxScrollLeft(model, traceEl), desired));

    preserveVirtualRefreshScroll(traceEl, model, desired);
    var restored = traceScrollLeftValue(traceEl);
    updateVirtualRangeForScrollLeft(traceEl, restored);
    preserveVirtualRefreshScroll(traceEl, model, restored);
    return restored;
}

function removeRuleResizeGuide(drag) {
    var guide = drag && drag.guideEl;
    if (guide && guide.parentNode) guide.parentNode.removeChild(guide);
    if (drag) {
        drag.guideEl = null;
        drag.guideLabelEl = null;
    }
}

function ensureRuleResizeGuide(drag) {
    if (!drag || !hasDOM() || !document.body) return null;
    if (drag.guideEl) return drag.guideEl;

    var guide = document.createElement('div');
    guide.className = 'rule-resize-guide';
    guide.setAttribute('aria-hidden', 'true');
    var label = document.createElement('span');
    label.className = 'rule-resize-guide-label';
    guide.appendChild(label);
    document.body.appendChild(guide);
    drag.guideEl = guide;
    drag.guideLabelEl = label;
    return guide;
}

function updateRuleResizeGuide(drag, width) {
    if (!drag || !drag.deferredLiveResize) return;

    var guide = ensureRuleResizeGuide(drag);
    if (!guide || !guide.style) return;
    var left = Number(drag.anchorClientLeft);
    if (!Number.isFinite(left)) left = Number(drag.startX) || 0;
    left += clampRuleWidth(width);

    guide.style.top = Math.max(0, Number(drag.guideTop) || 0) + 'px';
    guide.style.height = Math.max(24, Number(drag.guideHeight) || 24) + 'px';
    guide.style.transform = 'translate3d(' + (Math.round(left * 10) / 10) + 'px, 0, 0)';
    if (drag.guideLabelEl) {
        drag.guideLabelEl.textContent = Math.round(clampRuleWidth(width)) + 'px';
    }
}


function resizeMountedRuleKeyDiff(expected, actual) {
    expected = expected || {};
    actual = actual || {};
    var missing = [];
    var unexpected = [];
    for (var expectedKey in expected) {
        if (!Object.prototype.hasOwnProperty.call(expected, expectedKey)) continue;
        if (!actual[expectedKey]) missing.push(expectedKey);
    }
    for (var actualKey in actual) {
        if (!Object.prototype.hasOwnProperty.call(actual, actualKey)) continue;
        if (!expected[actualKey]) unexpected.push(actualKey);
    }
    missing.sort();
    unexpected.sort();
    return {
        expectedCount: Object.keys(expected).length,
        actualCount: Object.keys(actual).length,
        missing: missing,
        unexpected: unexpected
    };
}

function assertResizeReconciliationMountedState(context) {
    if (!hasDOM() || !virtualRuntime().virtualizerReady) return;

    var model = getTraceLayoutModel();
    var expected = mountedRulesForModel(model);
    var actual = virtualRuntime().mountedRuleKeys;
    if (sameMountedRuleKeys(expected, actual)) return;

    throwRecordedInvariantFailure(
        'resize-reconciliation',
        context || 'finish resize',
        'resize_reconciliation_mounted_keys_mismatch',
        'virtualization.mountedRuleKeys',
        'Resize reconciliation detected impossible mounted state',
        resizeMountedRuleKeyDiff(expected, actual)
    );
}

function resizeDeferredFrameWork() {
    var resize = resizeRuntime();
    if (!resize.deferredFrameWork) {
        resize.deferredFrameWork = {
            treeMaterializers: false,
            traceAnchor: false,
            diffArrows: false
        };
    }
    return resize.deferredFrameWork;
}

function markResizeDeferredFrameWork(flags) {
    var work = resizeDeferredFrameWork();
    flags = flags || {};
    if (flags.treeMaterializers) work.treeMaterializers = true;
    if (flags.traceAnchor) work.traceAnchor = true;
    if (flags.diffArrows) work.diffArrows = true;
}

function clearResizeDeferredFrameWork() {
    var work = resizeDeferredFrameWork();
    work.treeMaterializers = false;
    work.traceAnchor = false;
    work.diffArrows = false;
}

function flushResizeDeferredFrameWork(options) {
    var work = resizeDeferredFrameWork();
    options = options || {};
    var force = !!options.force;
    var hasWork = force || work.treeMaterializers || work.traceAnchor || work.diffArrows;
    if (!hasWork) return;

    var refreshTrees = force || work.treeMaterializers;
    var updateAnchor = force || work.traceAnchor;
    var updateDiffArrows = force || work.diffArrows;
    clearResizeDeferredFrameWork();

    runRenderFramePhaseNow(
        'deferred',
        'resize-deferred-frame-work',
        function runResizeDeferredFrameWork(visualCtx) {
            if (refreshTrees) refreshVisibleTreeMaterializers();
            if (updateAnchor) updateTraceAnchorLine();
            if (updateDiffArrows) scheduleDiffMoveArrows();
        },
        {
            epochScopes: ['trace', 'render', 'diff', 'virtual'],
            surfaces: ['rule-tree', 'trace-anchor-line', 'diff-arrows']
        }
    );
}

function applyRuleResizeFrame() {
    var drag = resizeRuntime().ruleResizeDrag;
    if (!drag) return;

    drag.frame = null;
    previewRuleResizeWidth(drag, drag.pendingWidth);
    updateRuleResizeGuide(drag, drag.currentWidth);
}

function scheduleRuleResizeFrame(width) {
    var drag = resizeRuntime().ruleResizeDrag;
    if (!drag) return;

    drag.pendingWidth = width;
    if (drag.frame !== null) return;

    drag.frame = scheduleRenderDomWork(
        'rule-resize-frame',
        function runScheduledRuleResizeFrame() {
            applyRuleResizeFrame();
        },
        {
            epochScopes: ['trace', 'virtual'],
            label: 'rule-resize-frame',
            surfaces: ['trace-scrollbar'],
            onDiscard: function() {
                var activeDrag = resizeRuntime().ruleResizeDrag;
                if (activeDrag === drag) activeDrag.frame = null;
            }
        }
    );
}

function flushRuleResizeFrame() {
    var drag = resizeRuntime().ruleResizeDrag;
    if (!drag) return;

    if (drag.frame !== null) {
        cancelRenderFrameWork(drag.frame);
        drag.frame = null;
    }
    applyRuleResizeFrame();
}

function startRuleResize(ev) {
    if (ev.button !== 0) return;

    var handle = ev.target.closest ? ev.target.closest('.rule-resize-handle') : null;
    if (!handle) return;

    var cell = handle.closest ? handle.closest('.rule-cell.expanded') : null;
    if (!cell || cell.classList.contains('fullscreen-rule')) {
        return;
    }

    ev.preventDefault();
    ev.stopPropagation();

    var ref = parseRuleCellId(cell.id);
    var width = cell.getBoundingClientRect().width;
    var traceEl = cell.closest ? cell.closest('.trace') : null;
    var traceRect = traceEl && traceEl.getBoundingClientRect ? traceEl.getBoundingClientRect() : null;
    var drag = {
        pointerId: ev.pointerId,
        startX: ev.clientX,
        startScrollLeft: traceEl ? traceEl.scrollLeft : 0,
        startRuleWidth: currentRuleWidthPx(),
        startWidth: width,
        currentWidth: width,
        pendingWidth: width,
        traceEl: traceEl,
        anchorRuleId: cell.id,
        anchorRef: ref,
        anchorClientLeft: ruleResizeAnchorClientLeft(cell),
        anchorViewportOffset: ruleResizeAnchorViewportOffset(cell, traceEl, ref),
        deferredLiveResize: true,
        guideEl: null,
        guideLabelEl: null,
        guideTop: traceRect && Number.isFinite(traceRect.top) ? traceRect.top : 0,
        guideHeight: traceRect && Number.isFinite(traceRect.height) ? traceRect.height : 0,
        previewWidthPending: false,
        globalWidthCommitted: false,
        frame: null
    };
    resizeRuntime().ruleResizeDrag = drag;
    bumpRuntimeEpoch('resize');

    document.body.classList.add('rule-resizing');
    document.body.classList.add('rule-resizing-deferred');
    updateRuleResizeGuide(drag, width);
    clearResizeDeferredFrameWork();

    if (handle.setPointerCapture && ev.pointerId !== undefined) {
        try { handle.setPointerCapture(ev.pointerId); } catch (err) {}
    }

    window.addEventListener('pointermove', updateRuleResize);
    window.addEventListener('pointerup', finishRuleResize);
    window.addEventListener('pointercancel', finishRuleResize);
}

function updateRuleResize(ev) {
    var drag = resizeRuntime().ruleResizeDrag;
    if (!drag) return;
    if (ev.pointerId !== undefined &&
        drag.pointerId !== undefined &&
        ev.pointerId !== drag.pointerId) {
        return;
    }

    ev.preventDefault();
    scheduleRuleResizeFrame(
        drag.startWidth + ev.clientX - drag.startX
    );
}

function finishRuleResize(ev) {
    var drag = resizeRuntime().ruleResizeDrag;
    if (!drag) return;
    if (ev && ev.pointerId !== undefined &&
        drag.pointerId !== undefined &&
        ev.pointerId !== drag.pointerId) {
        return;
    }

    flushRuleResizeFrame();
    captureRuleResizeLogicalAnchor(drag);
    if (drag.previewWidthPending) {
        commitRuleResizeWidth(drag, drag.currentWidth);
    }
    if (drag.globalWidthCommitted) {
        updateAllLayoutWidths();
    }
    var model = rebuildTraceLayoutModel();
    syncTraceCanvasWidth(model);
    var desiredScrollLeft = ruleResizeDesiredScrollLeft(drag, model);
    saveRuleWidth(drag.currentWidth);
    removeRuleResizeGuide(drag);
    resizeRuntime().ruleResizeDrag = null;
    bumpRuntimeEpoch('resize');
    document.body.classList.remove('rule-resizing');
    document.body.classList.remove('rule-resizing-deferred');

    window.removeEventListener('pointermove', updateRuleResize);
    window.removeEventListener('pointerup', finishRuleResize);
    window.removeEventListener('pointercancel', finishRuleResize);
    refreshVirtualRowsNow(
        Number.isFinite(desiredScrollLeft)
            ? { desiredScrollLeft: desiredScrollLeft }
            : undefined
    );
    var refinedScrollLeft = refineRuleResizeLogicalAnchorFromDom(
        drag.logicalAnchor,
        model,
        ruleResizeTraceElement(drag)
    );
    if (Number.isFinite(refinedScrollLeft) &&
        Math.abs(refinedScrollLeft - desiredScrollLeft) > VIEWPORT_ANCHOR_EPSILON) {
        desiredScrollLeft = refinedScrollLeft;
        refreshVirtualRowsNow({ desiredScrollLeft: desiredScrollLeft });
    }
    syncTraceScrollbarGeometry(false);
    assertResizeReconciliationMountedState('finish resize');
    scheduleVisibleRuleRenderScan();
    markResizeDeferredFrameWork({
        treeMaterializers: true,
        traceAnchor: true,
        diffArrows: true
    });
    flushResizeDeferredFrameWork({ force: true });
}

function resetRuleWidth(ev) {
    var handle = ev.target.closest ? ev.target.closest('.rule-resize-handle') : null;
    if (!handle) return;

    ev.preventDefault();
    ev.stopPropagation();
    var traceEl = handle.closest ? handle.closest('.trace') : null;
    var cell = handle.closest ? handle.closest('.rule-cell.expanded') : null;
    var ref = cell ? parseRuleCellId(cell.id) : null;
    var drag = {
        traceEl: traceEl,
        anchorRuleId: cell ? cell.id : null,
        anchorRef: ref
    };
    captureRuleResizeLogicalAnchor(drag);
    var width = applyRuleWidth(RULE_WIDTH_DEFAULT);
    updateAllLayoutWidths();
    var model = rebuildTraceLayoutModel();
    syncTraceCanvasWidth(model);
    var desiredScrollLeft = ruleResizeDesiredScrollLeft(drag, model);
    refreshVirtualRowsNow(
        Number.isFinite(desiredScrollLeft)
            ? { desiredScrollLeft: desiredScrollLeft }
            : undefined
    );
    var refinedScrollLeft = refineRuleResizeLogicalAnchorFromDom(drag.logicalAnchor, model, traceEl);
    if (Number.isFinite(refinedScrollLeft) &&
        Math.abs(refinedScrollLeft - desiredScrollLeft) > VIEWPORT_ANCHOR_EPSILON) {
        desiredScrollLeft = refinedScrollLeft;
        refreshVirtualRowsNow({ desiredScrollLeft: desiredScrollLeft });
    }
    updateTraceAnchorLine();
    saveRuleWidth(width);
}

function initRuleResize() {
    initRuleWidth();
    document.addEventListener('pointerdown', startRuleResize);
    document.addEventListener('dblclick', resetRuleWidth);
}
