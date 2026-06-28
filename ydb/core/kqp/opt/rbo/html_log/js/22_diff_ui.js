/* ══════════════════════════════════════════════════════════════
   A/B Diff — selection and UI
   ══════════════════════════════════════════════════════════════ */

function currentDiffSession() {
    return {
        a: diffRuntime().diffA,
        b: diffRuntime().diffB,
        savedState: diffRuntime().savedDiffState,
        savedViewport: diffRuntime().savedDiffViewport
    };
}

function applyDiffSession(session) {
    session = TraceDiffState.cloneSession(session);
    var hadInvalidSide = false;
    if (session.a && !ruleSupportsDiff(session.a)) {
        session.a = null;
        hadInvalidSide = true;
    }
    if (session.b && !ruleSupportsDiff(session.b)) {
        session.b = null;
        hadInvalidSide = true;
    }
    if (hadInvalidSide) {
        session.a = null;
        session.b = null;
    }
    if (!session.a || !session.b) {
        session.savedState = null;
        session.savedViewport = null;
    }
    diffRuntime().diffA = session.a;
    diffRuntime().diffB = session.b;
    diffRuntime().savedDiffState = session.savedState;
    diffRuntime().savedDiffViewport = session.savedViewport;
    if (!diffRuntime().savedDiffState) clearDiffMoveArrows();
}

function withDiffRenderTransition(label, details, fn) {
    if (typeof fn !== 'function') return false;

    var diff = diffRuntime();
    if (diff.diffTransitionOwnerId) return fn();

    var owner = createVisualSurfaceOwner(
        'diff',
        label || 'diff-render-transition',
        ['diff-arrows', 'rule-tree'],
        details
    );
    if (!acquireVisualSurfaceOwners(owner.surfaces, owner)) return false;

    diff.diffTransitionOwnerId = owner.id;
    try {
        return fn(owner);
    } finally {
        diff.diffTransitionOwnerId = null;
        releaseVisualSurfaceOwners(owner.id);
    }
}

function diffRenderCommitDetails(details) {
    details = cloneVisualDetails(details);
    var ownerId = currentDiffRenderOwnerId();
    if (ownerId && details.ownerId === undefined) details.ownerId = ownerId;
    return details;
}

function canCommitDiffRuleTree(label, target, details) {
    details = diffRenderCommitDetails(details);
    if (!canCommitVisualWork('rule-tree', label, details)) return false;
    if (target && target.isConnected === false) {
        if (target.id) details.targetId = target.id;
        recordVisualCommitDenied('rule-tree', label, 'detached_container', details);
        return false;
    }
    return true;
}

function clearDiffSelectionButtons(which) {
    if ((which === 'a' || which === 'both') && diffRuntime().diffA) {
        var aBtn = document.getElementById('abtn-' + diffRuntime().diffA.si + '-' + diffRuntime().diffA.gi + '-' + diffRuntime().diffA.ri);
        if (aBtn) {
            aBtn.classList.remove('selected-a');
            aBtn.classList.remove('diff-pending');
            aBtn.innerHTML = 'A';
        }
    }
    if ((which === 'b' || which === 'both') && diffRuntime().diffB) {
        var bBtn = document.getElementById('bbtn-' + diffRuntime().diffB.si + '-' + diffRuntime().diffB.gi + '-' + diffRuntime().diffB.ri);
        if (bBtn) {
            bBtn.classList.remove('selected-b');
            bBtn.classList.remove('diff-pending');
            bBtn.innerHTML = 'B';
        }
    }
}

var diffButtonLabels = ['Clear AB', 'Close delta'];

function updateDiffButtonMeasure(measureEl) {
    if (!measureEl || !document.createElement) return;

    var labelsKey = diffButtonLabels.join('\n');
    if (measureEl.getAttribute && measureEl.getAttribute('data-diff-labels') === labelsKey) return;

    while (measureEl.firstChild) measureEl.removeChild(measureEl.firstChild);
    for (var i = 0; i < diffButtonLabels.length; i++) {
        var option = document.createElement('span');
        option.textContent = diffButtonLabels[i];
        measureEl.appendChild(option);
    }
    if (measureEl.setAttribute) measureEl.setAttribute('data-diff-labels', labelsKey);
}

function ensureDiffButtonParts(btn) {
    var labelEl = btn.querySelector ? btn.querySelector('.diff-label') : null;
    var measureEl = btn.querySelector ? btn.querySelector('.diff-measure') : null;
    var canBuildParts = document.createElement && btn.appendChild && btn.removeChild;

    if ((!labelEl || !measureEl) && canBuildParts) {
        while (btn.firstChild) btn.removeChild(btn.firstChild);

        measureEl = document.createElement('span');
        measureEl.className = 'diff-measure';
        measureEl.setAttribute('aria-hidden', 'true');
        btn.appendChild(measureEl);

        labelEl = document.createElement('span');
        labelEl.className = 'diff-label';
        btn.appendChild(labelEl);
    }

    updateDiffButtonMeasure(measureEl);
    return {
        labelEl: labelEl
    };
}

function setDiffButtonLabel(btn, label) {
    var parts = ensureDiffButtonParts(btn);
    if (parts.labelEl) {
        parts.labelEl.textContent = label;
    } else {
        btn.textContent = label;
    }
}

function updateDiffButton() {
    var btn = document.getElementById('diff-button');
    if (!btn) return;
    if (diffRuntime().savedDiffState) {
        btn.hidden = false;
        setDiffButtonLabel(btn, 'Close delta');
        btn.title = 'Close diff view';
        btn.className = 'ctrl-btn diff-reset diff-active';
    } else if (diffRuntime().diffA || diffRuntime().diffB) {
        btn.hidden = false;
        setDiffButtonLabel(btn, 'Clear AB');
        btn.title = 'Clear A/B selection';
        btn.className = 'ctrl-btn diff-reset';
    } else {
        btn.hidden = true;
        setDiffButtonLabel(btn, 'Clear AB');
        btn.title = 'Clear A/B selection';
        btn.className = 'ctrl-btn diff-reset';
    }
}

function onDiffButtonClick() {
    TraceActions.diff.onDiffButtonClick();
}

function exitDiffKeepOtherFromRule(keepWhich, si, gi, ri) {
    TraceActions.diff.exitDiffKeepOtherFromRule(keepWhich, si, gi, ri);
}

function setDiffA(si, gi, ri, ev) {
    TraceActions.diff.setDiffA(si, gi, ri, ev);
}

function setDiffB(si, gi, ri, ev) {
    TraceActions.diff.setDiffB(si, gi, ri, ev);
}

function clearDiffSelection(which) {
    TraceActions.diff.clearDiffSelection(which);
}

function sameRuleRef(ref, si, gi, ri) {
    return RuleRefs.same(ref, si, gi, ri);
}

function isDiffActive() {
    return !!diffRuntime().savedDiffState && !!diffRuntime().diffA && !!diffRuntime().diffB;
}

function isActiveDiffRule(si, gi, ri) {
    return ruleSupportsDiff(ruleRef(si, gi, ri)) &&
        isDiffActive() &&
        (sameRuleRef(diffRuntime().diffA, si, gi, ri) || sameRuleRef(diffRuntime().diffB, si, gi, ri));
}

function isOtherActiveDiffRule(ref, si, gi, ri) {
    return isActiveDiffRule(si, gi, ri) &&
        !sameRuleRef(ref, si, gi, ri);
}

function diffSideForRule(si, gi, ri) {
    if (!ruleSupportsDiff(ruleRef(si, gi, ri))) return '';
    if (!isDiffActive()) return '';
    if (sameRuleRef(diffRuntime().diffA, si, gi, ri)) return 'a';
    if (sameRuleRef(diffRuntime().diffB, si, gi, ri)) return 'b';
    return '';
}

function shouldRenderFullscreenDiff(si, gi, ri) {
    return isFullscreenDiff() && isFullscreenRule(si, gi, ri);
}

function diffRuleKey(ref) {
    return ref.si + '-' + ref.gi + '-' + ref.ri;
}

function diffPairKeyForRefs(refA, refB) {
    if (!refA || !refB) return '';
    var fields = diffFieldSelectionSignature();
    var pairKey = diffRuleKey(refA) + '|' + diffRuleKey(refB);
    return fields ? pairKey + '|fields:' + fields.length + ':' + fields : pairKey;
}

function activeDiffPairKey() {
    return diffPairKeyForRefs(diffRuntime().diffA, diffRuntime().diffB);
}

function diffMoveScopeKey() {
    return diffRuntime().diffA && diffRuntime().diffB
        ? 'diff-' + diffRuleKey(diffRuntime().diffA) + '-' + diffRuleKey(diffRuntime().diffB)
        : 'diff-pending';
}

function invalidateActiveDiffResult() {
    diffRuntime().activeDiffCache = { pairKey: '', result: null };
}

function cachedDiffResultForPair(pairKey) {
    return diffRuntime().activeDiffCache.pairKey === pairKey ? diffRuntime().activeDiffCache.result : null;
}

function storeActiveDiffResult(pairKey, result) {
    diffRuntime().activeDiffCache = { pairKey: pairKey, result: result };
    return result;
}

function hasPendingDiffForPair(pairKey) {
    return !!diffRuntime().pendingDiffJob && diffRuntime().pendingDiffJob.pairKey === pairKey;
}

function activeDiffPendingWithoutResult() {
    var pairKey = activeDiffPairKey();
    return !!pairKey && hasPendingDiffForPair(pairKey) && !cachedDiffResultForPair(pairKey);
}

function currentDiffResult() {
    if (!diffRuntime().diffA || !diffRuntime().diffB) return null;
    if (!ruleSupportsDiff(diffRuntime().diffA) || !ruleSupportsDiff(diffRuntime().diffB)) return null;
    var pairKey = activeDiffPairKey();
    var cached = cachedDiffResultForPair(pairKey);
    if (cached) return cached;

    var rawA = rawRuleIndex(diffRuntime().diffA.si, diffRuntime().diffA.gi, diffRuntime().diffA.ri);
    var rawB = rawRuleIndex(diffRuntime().diffB.si, diffRuntime().diffB.gi, diffRuntime().diffB.ri);
    return storeActiveDiffResult(
        pairKey,
        TreeDiff.diff(
            tracePlanTree(diffRuntime().diffA.si, rawA),
            tracePlanTree(diffRuntime().diffB.si, rawB),
            { compareFields: activeDiffFieldKeys() }
        )
    );
}

function activeDiffResultForRender() {
    var pairKey = activeDiffPairKey();
    if (!pairKey) return null;
    var cached = cachedDiffResultForPair(pairKey);
    if (cached || hasPendingDiffForPair(pairKey)) return cached;
    return currentDiffResult();
}

function diffPlanForRef(ref) {
    var store = currentTraceStore();
    return TraceStore.materializeRuleTree(store, TraceStore.ruleHandleForRef(store, ref));
}

function diffFrame(fn) {
    return scheduleRenderModelWork('pending-diff-worker', function(visualCtx) {
        return fn(visualCtx);
    }, {
        epochScopes: ['trace', 'diff'],
        label: 'pending-diff-worker',
        surfaceReason: 'worker startup only; DOM result validates diff job and visual commit guards'
    });
}

function cancelDiffFrame(id) {
    if (!cancelRenderFrameWork(id)) renderFrameCancel(id);
}

function createTreeDiffWorker() {
    if (typeof Worker === 'undefined' ||
        typeof Blob === 'undefined' ||
        typeof URL === 'undefined' ||
        !URL.createObjectURL) {
        return null;
    }

    var source =
        'var createTreeDiffModule = ' + createTreeDiffModule.toString() + ';\n' +
        'var TreeDiff = createTreeDiffModule();\n' +
        'self.onmessage = function(ev) {\n' +
        '  var data = ev.data || {};\n' +
        '  if (data.type !== "diff") return;\n' +
        '  try {\n' +
        '    self.postMessage({ type: "result", jobId: data.jobId, pairKey: data.pairKey, result: TreeDiff.diff(data.a, data.b, { compareFields: data.compareFields || [] }) });\n' +
        '  } catch (err) {\n' +
        '    self.postMessage({ type: "error", jobId: data.jobId, pairKey: data.pairKey, message: err && err.message ? err.message : String(err) });\n' +
        '  }\n' +
        '};\n';

    try {
        var blob = new Blob([source], { type: 'text/javascript' });
        var url = URL.createObjectURL(blob);
        var worker = new Worker(url);
        if (URL.revokeObjectURL) URL.revokeObjectURL(url);
        return worker;
    } catch (err) {
        return null;
    }
}

function pendingDiffJobMatches(jobId, pairKey) {
    return !!diffRuntime().pendingDiffJob &&
        diffRuntime().pendingDiffJob.id === jobId &&
        diffRuntime().pendingDiffJob.pairKey === pairKey &&
        diffRuntime().pendingDiffJob.traceEpoch === currentRuntimeEpoch().trace &&
        diffRuntime().pendingDiffJob.diffEpoch === currentRuntimeEpoch().diff &&
        activeDiffPairKey() === pairKey;
}

function clearCompletedPendingDiffJob(jobId, pairKey) {
    if (!pendingDiffJobMatches(jobId, pairKey)) return false;
    var worker = diffRuntime().pendingDiffJob.worker;
    diffRuntime().pendingDiffJob = null;
    if (worker && worker.terminate) worker.terminate();
    updateDiffSelectionButtonContent();
    return true;
}

function completePendingDiffJob(jobId, pairKey, result) {
    if (!clearCompletedPendingDiffJob(jobId, pairKey)) return;
    storeActiveDiffResult(pairKey, result);
    if (syncFullscreenModeWithDiff()) {
        var fullscreenRef = fullscreenCurrentRule();
        if (fullscreenRef) {
            updateRuleDisplay(fullscreenRef.si, fullscreenRef.gi, fullscreenRef.ri);
            refreshVirtualRowsNow();
        }
    }
    renderDiffInlineTrees(result);
    rerenderActiveFullscreenDiff(result);
    updateDiffButton();
}

function runDiffJobSyncFallback(job) {
    var result = TreeDiff.diff(job.planA, job.planB, { compareFields: job.compareFields || [] });
    if (pendingDiffJobMatches(job.id, job.pairKey)) {
        completePendingDiffJob(job.id, job.pairKey, result);
    }
}

function startPendingDiffWorker(job) {
    if (!pendingDiffJobMatches(job.id, job.pairKey)) return;

    var worker = createTreeDiffWorker();
    if (!worker) {
        runDiffJobSyncFallback(job);
        return;
    }

    job.worker = worker;
    worker.onmessage = function(ev) {
        var data = ev.data || {};
        if (!pendingDiffJobMatches(data.jobId, data.pairKey)) return;
        if (data.type === 'result') {
            completePendingDiffJob(data.jobId, data.pairKey, data.result);
        } else if (data.type === 'error') {
            runDiffJobSyncFallback(job);
        }
    };
    worker.onerror = function() {
        if (!pendingDiffJobMatches(job.id, job.pairKey)) return;
        if (job.worker && job.worker.terminate) job.worker.terminate();
        job.worker = null;
        runDiffJobSyncFallback(job);
    };

    try {
        worker.postMessage({
            type: 'diff',
            jobId: job.id,
            pairKey: job.pairKey,
            a: job.planA,
            b: job.planB,
            compareFields: job.compareFields || []
        });
    } catch (err) {
        if (worker.terminate) worker.terminate();
        job.worker = null;
        runDiffJobSyncFallback(job);
    }
}

function cancelPendingDiffJob() {
    if (!diffRuntime().pendingDiffJob) return false;

    var job = diffRuntime().pendingDiffJob;
    bumpRuntimeEpoch('diff');
    diffRuntime().pendingDiffJob = null;
    if (job.frameId !== null && job.frameId !== undefined) {
        cancelDiffFrame(job.frameId);
    }
    if (job.worker && job.worker.terminate) {
        job.worker.terminate();
    }
    updateDiffSelectionButtonContent();
    return true;
}

function beginPendingDiffJob(pairKey) {
    cancelPendingDiffJob();
    if (!ruleSupportsDiff(diffRuntime().diffA) || !ruleSupportsDiff(diffRuntime().diffB)) return;
    var diffEpoch = bumpRuntimeEpoch('diff');

    diffRuntime().pendingDiffJob = {
        id: diffRuntime().nextDiffJobId++,
        pairKey: pairKey,
        traceEpoch: currentRuntimeEpoch().trace,
        diffEpoch: diffEpoch,
        planA: diffPlanForRef(diffRuntime().diffA),
        planB: diffPlanForRef(diffRuntime().diffB),
        compareFields: activeDiffFieldKeys(),
        frameId: null,
        worker: null
    };

    updateDiffSelectionButtonContent();

    diffRuntime().pendingDiffJob.frameId = diffFrame(function() {
        var job = diffRuntime().pendingDiffJob;
        if (!job || !pendingDiffJobMatches(job.id, pairKey)) return;
        job.frameId = null;
        startPendingDiffWorker(job);
    });
}

function cachedRuleFeature(ref, feature, compute) {
    var key = ref.si + '-' + rawRuleIndex(ref.si, ref.gi, ref.ri);
    var cache = currentRuleFeatureCache();
    var cached = cache[key];
    if (!cached) cached = cache[key] = {};
    if (!(feature in cached)) cached[feature] = compute();
    return cached[feature];
}

function invalidateCachedRuleFeaturesForRawRule(si, rawIdx) {
    var cache = currentRuleFeatureCache();
    if (!cache) return false;
    delete cache[si + '-' + rawIdx];
    if (typeof syncMountedRuleFeaturesForRawRule === 'function') {
        syncMountedRuleFeaturesForRawRule(si, rawIdx);
    }
    return true;
}

function planHasAnyFields(node) {
    if (!node) return false;
    if (node.a && node.a.length > 0) return true;
    var children = node.c || [];
    for (var i = 0; i < children.length; i++) {
        if (planHasAnyFields(children[i])) return true;
    }
    return false;
}

function planHasAnyPinned(node) {
    if (!node) return false;
    if (node.a && node.a.length > 0) return true;
    var children = node.c || [];
    for (var i = 0; i < children.length; i++) {
        if (planHasAnyPinned(children[i])) return true;
    }
    return false;
}

function planHasAnyFieldDetails(node) {
    if (!node) return false;
    var attrs = Array.isArray(node.a) ? node.a : [];
    for (var i = 0; i < attrs.length; i++) {
        if (TraceSchema.fieldRowHasDetails(attrs[i])) return true;
    }
    var children = node.c || [];
    for (var c = 0; c < children.length; c++) {
        if (planHasAnyFieldDetails(children[c])) return true;
    }
    return false;
}

function ruleHasFields(ref) {
    if (ruleIsTextTile(ref)) return false;
    return cachedRuleFeature(ref, 'fields', function() {
        var summary = TraceStore.ruleSummaryForRef(currentTraceStore(), ref);
        return !!(summary && summary.features && summary.features.fields);
    });
}

function ruleHasPinned(ref) {
    if (pinnedFieldCount() === 0) return false;
    if (ruleIsTextTile(ref)) return false;
    return cachedRuleFeature(ref, 'pinned', function() {
        var summary = TraceStore.ruleSummaryForRef(currentTraceStore(), ref);
        return !!(summary && summary.features && summary.features.pinned);
    });
}

function ruleHasInfo(ref) {
    if (ruleIsTextTile(ref)) return false;
    if (typeof ruleHasOpenInfoDetails === 'function' && ruleHasOpenInfoDetails(ref)) return true;
    return cachedRuleFeature(ref, 'info', function() {
        var summary = TraceStore.ruleSummaryForRef(currentTraceStore(), ref);
        return !!(summary && summary.features && summary.features.info);
    });
}

function fullscreenFeatureButtonsHtml(ref) {
    var html = '';

    html += fullscreenDiffRuleFeatureButtonHtml(ref, 'fields', 'Toggle fields', 'info');
    html += fullscreenDiffRuleFeatureButtonHtml(ref, 'pinned', 'Toggle pinned fields', 'drawing-pin');
    html += fullscreenDiffRuleFeatureButtonHtml(ref, 'info', 'Toggle info', 'file-text');

    return html;
}

function fullscreenDiffRuleFeatureButtonHtml(ref, feature, title, iconName) {
    var available = ruleHasFeature(ref.si, ref.gi, ref.ri, feature);
    var active = available && ruleFeatureButtonActive(feature, ref.si, ref.gi, ref.ri);
    return '<button class="tbtn icon' + (active ? ' active' : '') + '" ' +
            (available ? '' : 'disabled ') +
            traceActionAttr('toggle-rule-' + feature, ref.si, ref.gi, ref.ri) + ' ' +
            'title="' + title + '" aria-label="' + title + '">' +
            traceIconSvg(iconName) + '</button>';
}

function fullscreenDiffSelectorButtonsHtml(ref) {
    var flat = findFlatIndex(ref.si, ref.gi, ref.ri);
    var rules = currentAllRules();
    var prevTarget = flat > 0 ? rules[flat - 1] : null;
    var nextTarget = flat < rules.length - 1 ? rules[flat + 1] : null;
    var prevDisabled = !prevTarget || isOtherActiveDiffRule(ref, prevTarget.stageIdx, prevTarget.groupIdx, prevTarget.ruleIdx);
    var nextDisabled = !nextTarget || isOtherActiveDiffRule(ref, nextTarget.stageIdx, nextTarget.groupIdx, nextTarget.ruleIdx);
    return '<button class="tbtn ab' + (sameRuleRef(diffRuntime().diffA, ref.si, ref.gi, ref.ri) ? ' selected-a' : '') +
           diffButtonPendingClass(ref, 'a') +
           '"' + traceActionAttr('set-diff-a', ref.si, ref.gi, ref.ri) + '>' +
           diffButtonContentHtml(ref, 'a') + '</button>' +
           '<button class="tbtn ab' + (sameRuleRef(diffRuntime().diffB, ref.si, ref.gi, ref.ri) ? ' selected-b' : '') +
           diffButtonPendingClass(ref, 'b') +
           '"' + traceActionAttr('set-diff-b', ref.si, ref.gi, ref.ri) + '>' +
           diffButtonContentHtml(ref, 'b') + '</button>' +
           navButtonHtml('prev', ref, prevDisabled, true) +
           navButtonHtml('next', ref, nextDisabled, true);
}

function fullscreenDiffRuleHeaderHtml(ref) {
    return '<div class="rule-title-bar"' +
           traceActionAttr('close-rule-fullscreen-to-rule-from-title', ref.si, ref.gi, ref.ri) + '>' +
           '<div class="title-left">' +
           '<div class="rule-title">' + ruleTitleHtml(ref) + '</div>' +
           fullscreenFeatureButtonsHtml(ref) +
           '</div>' +
           '<div class="title-right">' + fullscreenDiffSelectorButtonsHtml(ref) + '</div>' +
           '</div>';
}

function fullscreenDiffInfoHtml(ref, side) {
    if (!ruleState(ref.si, ref.gi, ref.ri).info || !ruleHasInfo(ref)) return '';
    var idScope = 'fsdiff-' + (side || 'rule');
    var key = scopedRuleKey(ref, { idScope: idScope });
    var stateKey = ref.si + '-' + ref.gi + '-' + ref.ri;
    var savedH = resizeRuntime().infoPanelHeights[key] ||
        resizeRuntime().infoPanelHeights['fs-' + stateKey] ||
        resizeRuntime().infoPanelHeights[stateKey];
    var panelStyle = savedH
        ? ' style="height:' + savedH + 'px;max-height:none"'
        : ' style="max-height:min(40vh, 520px)"';
    var sections = TraceStore.infoSectionsForRef(currentTraceStore(), ref);
    var tabbed = !!infoTabsModel(sections, ref.si, ref.gi, ref.ri);
    return '<div class="info-panel-resizer" aria-hidden="true"></div>' +
           '<div class="rule-info-panel visible' + (tabbed ? ' info-panel-tabbed' : '') +
           '" id="info-' + key + '"' + panelStyle + '>' +
           '<div class="rule-info-content" id="info-content-' + key + '">' +
           infoPanelHtml(
               sections,
               currentSearchQuery(),
               currentSearchScope(),
               ref.si,
               ref.gi,
               ref.ri,
               { idScope: idScope }
           ) +
           '</div></div>';
}

function diffTreePinnedHeaderHtml(key, showPinned, minWidth) {
    if (pinnedFieldCount() <= 0 || !showPinned) return '';
    var width = Math.max(1, Number(minWidth) || 0);
    var html = '<div class="tree-pinned-header visible" id="pinhdr-' + key + '"' +
        (width ? ' style="min-width:' + width + 'px"' : '') + '>';
    for (var col = 0; col < pinnedFieldCount(); col++) {
        html += pinnedFieldHeaderCellHtml(
            col,
            htmlEscape(tracePinnedFieldName(col)),
            { unpinTitle: null }
        );
    }
    html += '<span style="width:16px;flex-shrink:0;display:inline-block"></span>';
    html += '<span class="tree-plan-label">Plan</span>';
    html += '</div>';
    return html;
}

function diffTreeRenderModel(diffResult, key, side, ref, moveScope) {
    var showPinned = ref ? effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'pinned') : false;
    var showFields = ref ? effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'fields') : false;
    var treeSession = treeSessionForRule(key);
    moveScope = moveScope || diffMoveScopeKey();
    treeSession.showFields = showFields;
    var renderModel = DiffRenderer.buildSideRenderModel(diffResult, side, {
        showFields: showFields,
        showPinned: showPinned,
        treeSession: treeSession,
        moveScope: moveScope
    });
    return {
        key: key,
        side: side,
        ref: ref || null,
        moveScope: moveScope,
        showPinned: showPinned,
        showFields: showFields,
        treeSession: treeSession,
        tree: renderModel.tree,
        model: renderModel.model,
        renderOptions: renderModel.renderOptions
    };
}

function diffTreeRootHtml(renderModel, bodyHtml, includeId) {
    var model = renderModel && renderModel.model || {};
    var minWidth = Math.max(1, Number(model.minWidth) || 0);
    return diffTreePinnedHeaderHtml(renderModel.key, renderModel.showPinned, minWidth) +
        '<div class="tree-root' + (renderModel.showPinned ? ' pinned-active' : '') + '"' +
        (includeId ? ' id="treeroot-' + renderModel.key + '"' : '') +
        ' style="min-width:' + minWidth + 'px">' +
        bodyHtml +
        '</div>';
}

function renderDiffTreeHtml(diffResult, key, side, ref, moveScope) {
    var renderModel = diffTreeRenderModel(diffResult, key, side, ref, moveScope);
    return diffTreeRootHtml(
        renderModel,
        renderFullTreeRowsFromModel(
            renderModel.model,
            key,
            renderModel.showFields,
            renderModel.showPinned,
            '',
            '',
            renderModel.renderOptions
        ),
        false
    );
}

function diffContainerSupportsVirtualTree(container) {
    return !!(container &&
        typeof container.getElementsByClassName === 'function' &&
        typeof container.querySelector === 'function' &&
        hasDOM() &&
        document.getElementById);
}

function renderDiffTreeHtmlIntoContainer(container, renderModel) {
    if (!container) return false;
    var previousMaterializer = treeMaterializerForContainer(container);
    var previousState = previousMaterializer && previousMaterializer.state;
    if (previousState && previousState.container === container) {
        saveTreeMaterializerPaneAnchor(previousState);
    }
    clearVirtualTreeContainerState(container);
    container.innerHTML = diffTreeRootHtml(
        renderModel,
        renderFullTreeRowsFromModel(
            renderModel.model,
            renderModel.key,
            renderModel.showFields,
            renderModel.showPinned,
            '',
            '',
            renderModel.renderOptions
        ),
        true
    );

    return true;
}

function renderDiffTreeIntoContainer(container, diffResult, key, side, ref, moveScope, visualCtx) {
    var renderModel = diffTreeRenderModel(diffResult, key, side, ref, moveScope);
    if (renderModel.tree && diffContainerSupportsVirtualTree(container)) {
        renderVirtualTree(
            container,
            renderModel.tree,
            key,
            renderModel.showFields,
            renderModel.showPinned,
            '',
            '',
            visualCtx,
            renderModel.renderOptions
        );
        return renderModel;
    }
    renderDiffTreeHtmlIntoContainer(container, renderModel);
    return renderModel;
}

function renderFullscreenDiffSide(ref, side, diffResult, key, moveScope) {
    return '<div class="rule-cell expanded fullscreen-diff-rule fullscreen-diff-side" data-side="' +
           htmlEscape(side || '') + '">' +
           '<div class="rule-exp-wrap">' +
           fullscreenDiffRuleHeaderHtml(ref) +
           '<div class="rule-content">' +
           '<div class="rule-tree-pane">' +
           fullscreenCloseButtonHtml(ref) +
           '<div class="rule-tree-wrap diff-tree-wrap">' +
           renderDiffTreeHtml(diffResult, key, side, ref, moveScope) +
           '</div>' +
           '</div>' +
           fullscreenDiffInfoHtml(ref, side) +
           '</div>' +
           '</div>' +
           '</div>';
}

function renderFullscreenDiffSideShell(ref, side, key) {
    return '<div class="rule-cell expanded fullscreen-diff-rule fullscreen-diff-side" data-side="' +
           htmlEscape(side || '') + '">' +
           '<div class="rule-exp-wrap">' +
           fullscreenDiffRuleHeaderHtml(ref) +
           '<div class="rule-content">' +
           '<div class="rule-tree-pane">' +
           fullscreenCloseButtonHtml(ref) +
           '<div class="rule-tree-wrap diff-tree-wrap" id="tree-' + key + '"></div>' +
           '</div>' +
           fullscreenDiffInfoHtml(ref, side) +
           '</div>' +
           '</div>' +
           '</div>';
}

function mountFullscreenDiffSideTree(container, key, side, ref, diffResult, moveScope) {
    if (!container || !container.querySelector) return false;
    var treeContainer = container.querySelector('#tree-' + key);
    if (!treeContainer && document.getElementById) {
        treeContainer = document.getElementById('tree-' + key);
    }
    if (!treeContainer) return false;
    renderDiffTreeIntoContainer(treeContainer, diffResult, key, side, ref, moveScope);
    return true;
}

function mountFullscreenDiffPairTrees(container, keyPrefix, diffResult) {
    var mountedA = mountFullscreenDiffSideTree(
        container,
        keyPrefix + '-a',
        'a',
        diffRuntime().diffA,
        diffResult,
        keyPrefix
    );
    var mountedB = mountFullscreenDiffSideTree(
        container,
        keyPrefix + '-b',
        'b',
        diffRuntime().diffB,
        diffResult,
        keyPrefix
    );
    return mountedA && mountedB;
}

function renderDiffRuleTree(ref, side, diffResult) {
    var key = diffRuleKey(ref);
    var container = document.getElementById('tree-' + key);
    if (!container) {
        clearRuleTreePending(ref.si, ref.gi, ref.ri);
        return;
    }
    if (!canCommitDiffRuleTree('diff-rule-tree', container, {
        ruleKey: ruleKey(ref.si, ref.gi, ref.ri),
        renderKey: key,
        side: side
    })) return;

    clearRuleTreePending(ref.si, ref.gi, ref.ri);
    container.classList.remove('fullscreen-diff-wrap');
    container.classList.remove('diff-tree-wrap');
    if (!diffResult) {
        renderPendingDiffPlainTree(ref);
        return;
    }

    container.classList.add('diff-tree-wrap');
    renderDiffTreeIntoContainer(container, diffResult, key, side, ref);
    ruleState(ref.si, ref.gi, ref.ri).rendered = {
        diffSide: side,
        showFields: effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'fields'),
        showPinned: effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'pinned'),
        pinnedFields: pinnedFieldRenderSignature(),
        diffFields: diffFieldSelectionSignature()
    };
    updateRuleFeatureButtons(ref.si, ref.gi, ref.ri);
    scheduleDiffMoveArrows();
}

function renderDiffInlineTrees(diffResult) {
    return withDiffRenderTransition('render-diff-inline-trees', {
        pairKey: activeDiffPairKey()
    }, function() {
        if (!diffRuntime().diffA || !diffRuntime().diffB) return false;
        if (diffResult === undefined) diffResult = activeDiffResultForRender();

        renderDiffRuleTree(diffRuntime().diffA, 'a', diffResult);
        renderDiffRuleTree(diffRuntime().diffB, 'b', diffResult);
        if (typeof cancelDiffMoveArrowUpdate === 'function') cancelDiffMoveArrowUpdate();
        scheduleDiffMoveArrows();
        return true;
    });
}

function renderFullscreenDiffPair(container, fullscreenKey, diffResult) {
    return withDiffRenderTransition('render-fullscreen-diff-pair', {
        pairKey: activeDiffPairKey(),
        fullscreenKey: fullscreenKey
    }, function() {
        if (diffResult === undefined) diffResult = activeDiffResultForRender();
        if (!diffResult) return false;
        var keyPrefix = 'fs-' + fullscreenKey;
        if (!canCommitDiffRuleTree('fullscreen-diff-pair', container, {
            fullscreenKey: fullscreenKey
        })) return false;

        container.classList.add('fullscreen-diff-wrap');
        container.innerHTML =
            '<div class="fullscreen-diff-pair">' +
                renderFullscreenDiffSideShell(diffRuntime().diffA, 'a', keyPrefix + '-a') +
                renderFullscreenDiffSideShell(diffRuntime().diffB, 'b', keyPrefix + '-b') +
            '</div>';
        if (!mountFullscreenDiffPairTrees(container, keyPrefix, diffResult)) {
            container.innerHTML =
                '<div class="fullscreen-diff-pair">' +
                    renderFullscreenDiffSide(diffRuntime().diffA, 'a', diffResult, keyPrefix + '-a', keyPrefix) +
                    renderFullscreenDiffSide(diffRuntime().diffB, 'b', diffResult, keyPrefix + '-b', keyPrefix) +
                '</div>';
        }
        scheduleDiffMoveArrows();
        return true;
    });
}

var DIFF_MOVE_ARROW_MAX_PATHS = 80;
var diffArrowSurface = createVisualSurface({
    name: 'diff-arrows',
    epochScopes: ['trace', 'diff', 'virtual'],
    domGenerations: ['diffArrows'],
    allowMissingTarget: true,
    resolveTarget: function(ref, options) {
        if (options && options.target) return options.target;
        return currentDiffMoveArrowOverlay();
    }
});

function currentDiffMoveArrowOverlay() {
    var arrows = diffArrowRuntime();
    var overlay = arrows.diffMoveArrowOverlay;
    if (overlay && overlay.isConnected === false) {
        arrows.diffMoveArrowOverlay = null;
        overlay = null;
    }
    if (!overlay && hasDOM() && document.querySelector) {
        overlay = document.querySelector('.diff-move-arrows');
        if (overlay) arrows.diffMoveArrowOverlay = overlay;
    }
    return overlay || null;
}

function traceScrollPositionForDiffArrows(traceEl) {
    if (!traceEl) return { left: 0, top: 0 };
    var left = Number(traceEl.scrollLeft || 0);
    var top = Number(traceEl.scrollTop || 0);
    return {
        left: Number.isFinite(left) ? left : 0,
        top: Number.isFinite(top) ? top : 0
    };
}

function traceElementForDiffArrows() {
    return hasDOM() && document.querySelector ? document.querySelector('.trace') : null;
}

function captureDiffMoveArrowScrollAnchor(traceEl) {
    var arrows = diffArrowRuntime();
    var pos = traceScrollPositionForDiffArrows(traceEl);
    arrows.diffMoveArrowTraceScrollLeft = pos.left;
    arrows.diffMoveArrowTraceScrollTop = pos.top;
}

function resetDiffMoveArrowScrollTransform(traceEl, options) {
    options = options || {};
    var visualCtx = options.visualContext || null;
    if (!visualCommitContextIsBranded(visualCtx)) {
        var reset = false;
        runRenderFramePhaseNow('deferred', 'reset-diff-arrow-scroll-transform', function(ctx) {
            reset = resetDiffMoveArrowScrollTransform(traceEl, {
                visualContext: ctx
            });
        }, {
            epochScopes: ['trace', 'diff', 'virtual'],
            surfaces: ['diff-arrows'],
            withVisualContext: true
        });
        return reset;
    }

    var overlay = diffArrowRuntime().diffMoveArrowOverlay;
    if (!overlay || !overlay.style) {
        return resetDiffMoveArrowScrollTransformForCommit(traceEl, overlay);
    }

    var committed = false;
    diffArrowSurface.commit(visualCtx, {
        label: 'reset-diff-arrow-scroll-transform',
        target: overlay
    }, function(target) {
        committed = resetDiffMoveArrowScrollTransformForCommit(traceEl, target);
    });
    return committed;
}

function resetDiffMoveArrowScrollTransformForCommit(traceEl, overlay) {
    var arrows = diffArrowRuntime();
    captureDiffMoveArrowScrollAnchor(traceEl);
    arrows.diffMoveArrowTranslateX = 0;
    arrows.diffMoveArrowTranslateY = 0;
    if (overlay && overlay.style) {
        overlay.style.transform = '';
    }
    return true;
}

function syncDiffMoveArrowsForTraceScroll(traceEl, options) {
    options = options || {};
    if (!hasDOM()) return false;
    var arrows = diffArrowRuntime();
    var overlay = arrows.diffMoveArrowOverlay;
    if (!overlay || !overlay.style || overlay.style.display === 'none') return false;
    if (!diffRuntime().diffA || !diffRuntime().diffB) return false;

    var visualCtx = options.visualContext || null;
    if (!visualCommitContextIsBranded(visualCtx)) {
        var synced = false;
        runRenderFramePhaseNow('deferred', 'sync-diff-arrows-for-scroll', function(ctx) {
            synced = syncDiffMoveArrowsForTraceScroll(traceEl, {
                visualContext: ctx
            });
        }, {
            epochScopes: ['trace', 'diff', 'virtual'],
            surfaces: ['diff-arrows'],
            withVisualContext: true
        });
        return synced;
    }

    var committed = false;
    diffArrowSurface.commit(visualCtx, {
        label: 'sync-diff-arrows-for-scroll',
        target: overlay
    }, function(target) {
        var pos = traceScrollPositionForDiffArrows(traceEl);
        if (arrows.diffMoveArrowTraceScrollLeft === null ||
                arrows.diffMoveArrowTraceScrollTop === null) {
            arrows.diffMoveArrowTraceScrollLeft = pos.left;
            arrows.diffMoveArrowTraceScrollTop = pos.top;
            return;
        }

        var dx = arrows.diffMoveArrowTraceScrollLeft - pos.left;
        var dy = arrows.diffMoveArrowTraceScrollTop - pos.top;
        arrows.diffMoveArrowTraceScrollLeft = pos.left;
        arrows.diffMoveArrowTraceScrollTop = pos.top;
        if (Math.abs(dx) < 0.01 && Math.abs(dy) < 0.01) return;

        arrows.diffMoveArrowTranslateX += dx;
        arrows.diffMoveArrowTranslateY += dy;
        target.style.transform = 'translate3d(' +
            arrows.diffMoveArrowTranslateX.toFixed(2) + 'px, ' +
            arrows.diffMoveArrowTranslateY.toFixed(2) + 'px, 0)';
        committed = true;
    });
    return committed;
}

function initDiffMoveArrows() {
    if (!hasDOM() || diffArrowRuntime().diffMoveArrowListenersReady) return;
    diffArrowRuntime().diffMoveArrowListenersReady = true;
    if (document.addEventListener) {
        document.addEventListener('scroll', scheduleDiffMoveArrows, true);
    }
    if (typeof window !== 'undefined' && window.addEventListener) {
        window.addEventListener('resize', scheduleDiffMoveArrows);
        if (window.visualViewport && window.visualViewport.addEventListener) {
            window.visualViewport.addEventListener('scroll', scheduleDiffMoveArrows);
            window.visualViewport.addEventListener('resize', scheduleDiffMoveArrows);
        }
    }
}

function scheduleDiffMoveArrows() {
    if (!hasDOM()) return;
    var hasRenderedDiff = !!(diffRuntime().savedDiffState &&
        diffRuntime().diffA &&
        diffRuntime().diffB);
    if (!hasRenderedDiff && !diffArrowRuntime().diffMoveArrowOverlay) return;
    if (diffArrowRuntime().diffMoveArrowFrame !== null) return;

    var diffArrowsDomGeneration = currentDiffArrowsDomGeneration();
    diffArrowRuntime().diffMoveArrowFrame = scheduleRenderDeferredWork(
        'diff-move-arrows',
        function runScheduledDiffMoveArrows(visualCtx) {
            diffArrowRuntime().diffMoveArrowFrame = null;
            updateDiffMoveArrows(visualCtx, diffArrowsDomGeneration);
        },
        {
            epochScopes: ['trace', 'diff', 'virtual'],
            label: 'diff-move-arrows',
            surfaces: ['diff-arrows'],
            withVisualContext: true,
            onDiscard: function() {
                diffArrowRuntime().diffMoveArrowFrame = null;
            }
        }
    );
}

function clearDiffMoveArrows(visualCtx) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        var cleared = false;
        runRenderFramePhaseNow('deferred', 'clear-diff-arrows', function(ctx) {
            cleared = clearDiffMoveArrows(ctx);
        }, {
            epochScopes: ['trace', 'diff', 'virtual'],
            ownerId: currentDiffRenderOwnerId(),
            surfaces: ['diff-arrows'],
            withVisualContext: true
        });
        return cleared;
    }

    var overlay = currentDiffMoveArrowOverlay();
    if (!overlay) {
        captureDiffMoveArrowScrollAnchor(traceElementForDiffArrows());
        return true;
    }

    return diffArrowSurface.commit(visualCtx, {
        label: 'clear-diff-arrows',
        target: overlay
    }, function(target) {
        clearDiffMoveArrowsForCommit(target);
    });
}

function clearDiffMoveArrowsForCommit(overlay) {
    overlay = overlay || currentDiffMoveArrowOverlay();
    if (!overlay) {
        captureDiffMoveArrowScrollAnchor(traceElementForDiffArrows());
        return false;
    }
    resetDiffMoveArrowScrollTransformForCommit(traceElementForDiffArrows(), overlay);
    overlay.innerHTML = '';
    overlay.style.display = 'none';
    bumpDiffArrowsDomGeneration();
    return true;
}

function ensureDiffMoveArrowOverlayForCommit(width, height) {
    if (!document.createElementNS || !document.body || !document.body.appendChild) return null;

    var overlay = diffArrowRuntime().diffMoveArrowOverlay;
    if (overlay && overlay.isConnected === false) {
        recordVisualCommitDenied('diff-arrows', 'diff-arrow-overlay', 'detached_overlay');
        diffArrowRuntime().diffMoveArrowOverlay = null;
        overlay = null;
    }

    if (!overlay) {
        overlay = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        overlay.setAttribute('class', 'diff-move-arrows');
        overlay.setAttribute('aria-hidden', 'true');
        document.body.appendChild(overlay);
        diffArrowRuntime().diffMoveArrowOverlay = overlay;
    }

    overlay.setAttribute('width', width);
    overlay.setAttribute('height', height);
    overlay.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
    overlay.style.width = width + 'px';
    overlay.style.height = height + 'px';
    overlay.style.display = 'block';
    resetDiffMoveArrowScrollTransformForCommit(traceElementForDiffArrows(), overlay);
    overlay.innerHTML = '';
    return overlay;
}

function diffMoveViewportSize() {
    var visualViewport = typeof window !== 'undefined' ? window.visualViewport : null;
    var width = visualViewport && Number.isFinite(visualViewport.width)
        ? visualViewport.width
        : ((typeof window !== 'undefined' && window.innerWidth) ||
            (document.documentElement && document.documentElement.clientWidth) || 0);
    var height = visualViewport && Number.isFinite(visualViewport.height)
        ? visualViewport.height
        : ((typeof window !== 'undefined' && window.innerHeight) ||
            (document.documentElement && document.documentElement.clientHeight) || 0);
    var offsetLeft = visualViewport && Number.isFinite(visualViewport.offsetLeft)
        ? visualViewport.offsetLeft
        : 0;
    var offsetTop = visualViewport && Number.isFinite(visualViewport.offsetTop)
        ? visualViewport.offsetTop
        : 0;

    width = Math.round(width);
    height = Math.round(height);
    if (!Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) {
        return null;
    }
    return {
        width: width,
        height: height,
        offsetLeft: offsetLeft,
        offsetTop: offsetTop
    };
}

function diffMoveViewportFromOverlay(svg, fallback) {
    fallback = fallback || {};
    var viewport = {
        width: fallback.width,
        height: fallback.height,
        offsetLeft: Number.isFinite(fallback.offsetLeft) ? fallback.offsetLeft : 0,
        offsetTop: Number.isFinite(fallback.offsetTop) ? fallback.offsetTop : 0
    };
    if (!svg || !svg.getBoundingClientRect) return viewport;

    var rect = svg.getBoundingClientRect();
    if (finiteRect(rect)) {
        viewport.offsetLeft = rect.left;
        viewport.offsetTop = rect.top;
    }
    return viewport;
}

function diffMoveRectForViewport(rect, viewport) {
    var offsetLeft = viewport && Number.isFinite(viewport.offsetLeft) ? viewport.offsetLeft : 0;
    var offsetTop = viewport && Number.isFinite(viewport.offsetTop) ? viewport.offsetTop : 0;

    return {
        left: rect.left - offsetLeft,
        right: rect.right - offsetLeft,
        top: rect.top - offsetTop,
        bottom: rect.bottom - offsetTop
    };
}

function finiteRect(rect) {
    return rect &&
           Number.isFinite(rect.left) &&
           Number.isFinite(rect.right) &&
           Number.isFinite(rect.top) &&
           Number.isFinite(rect.bottom) &&
           rect.right > rect.left &&
           rect.bottom > rect.top &&
           Math.abs(rect.left) < 100000 &&
           Math.abs(rect.right) < 100000 &&
           Math.abs(rect.top) < 100000 &&
           Math.abs(rect.bottom) < 100000;
}

function clipMoveEndpointGeometryAgainstInfoPanel(node, geometry, viewport) {
    var content = node && node.closest ? node.closest('.rule-content') : null;
    if (!content || !content.querySelectorAll) return geometry;

    var panels = content.querySelectorAll('.rule-info-panel.visible');
    for (var i = 0; i < panels.length; i++) {
        var panel = panels[i];
        if (!panel || !panel.getBoundingClientRect) continue;

        var panelRect = diffMoveRectForViewport(panel.getBoundingClientRect(), viewport);
        if (!finiteRect(panelRect)) continue;
        if (panelRect.right <= geometry.visibleLeft ||
                panelRect.left >= geometry.visibleRight ||
                panelRect.bottom <= geometry.visibleTop ||
                panelRect.top >= geometry.visibleBottom) {
            continue;
        }

        geometry.visibleBottom = Math.min(geometry.visibleBottom, panelRect.top);
        if (geometry.visibleBottom <= geometry.visibleTop + 1) return null;
    }

    return geometry;
}

function visibleMoveEndpointGeometry(node, viewport) {
    if (!node || !node.getBoundingClientRect || !node.querySelector || !node.closest) return null;

    var row = node.firstElementChild;
    var main = row && row.querySelector ? row.querySelector('.tree-node-main') : null;
    if (!main || !main.getBoundingClientRect) return null;

    var wrap = node.closest('.rule-tree-wrap');
    if (!wrap || !wrap.getBoundingClientRect) return null;

    var mainRect = diffMoveRectForViewport(main.getBoundingClientRect(), viewport);
    var wrapRect = diffMoveRectForViewport(wrap.getBoundingClientRect(), viewport);
    if (!finiteRect(mainRect) || !finiteRect(wrapRect)) return null;

    var visibleLeft = Math.max(mainRect.left, wrapRect.left, 0);
    var visibleRight = Math.min(mainRect.right, wrapRect.right, viewport.width);
    var visibleTop = Math.max(mainRect.top, wrapRect.top, 0);
    var visibleBottom = Math.min(mainRect.bottom, wrapRect.bottom, viewport.height);
    if (visibleRight <= visibleLeft + 1 || visibleBottom <= visibleTop + 1) return null;

    return clipMoveEndpointGeometryAgainstInfoPanel(node, {
        mainRect: mainRect,
        visibleLeft: visibleLeft,
        visibleRight: visibleRight,
        visibleTop: visibleTop,
        visibleBottom: visibleBottom
    }, viewport);
}

function diffMoveSourceEndpoint(node, viewport) {
    var geometry = visibleMoveEndpointGeometry(node, viewport);
    if (!geometry) return null;

    return {
        x: Math.max(0, Math.min(viewport.width, geometry.visibleRight - 0.5)),
        y: Math.max(0, Math.min(viewport.height, (geometry.visibleTop + geometry.visibleBottom) / 2))
    };
}

function diffMoveTargetEndpoint(node, viewport) {
    var geometry = visibleMoveEndpointGeometry(node, viewport);
    if (!geometry) return null;
    if (geometry.mainRect.left < geometry.visibleLeft - 0.5 ||
        geometry.mainRect.left > geometry.visibleRight - 1) {
        return null;
    }

    return {
        x: Math.max(0, Math.min(viewport.width, geometry.visibleLeft + 0.5)),
        y: Math.max(0, Math.min(viewport.height, (geometry.visibleTop + geometry.visibleBottom) / 2))
    };
}

function diffMoveEndpointsConnectable(from, to) {
    return !!(from && to && to.x > from.x + 1);
}

function diffConnectorPathData(from, to) {
    if (!from || !to) return '';

    var dx = to.x - from.x;
    var direction = dx >= 0 ? 1 : -1;
    var distance = Math.abs(dx);
    var curve = Math.min(180, Math.max(42, distance * 0.42));
    var c1x = from.x + direction * curve;
    var c2x = to.x - direction * curve;

    if (distance < 16) {
        return 'M ' + from.x.toFixed(1) + ' ' + from.y.toFixed(1) +
            ' L ' + to.x.toFixed(1) + ' ' + to.y.toFixed(1);
    }

    return 'M ' + from.x.toFixed(1) + ' ' + from.y.toFixed(1) +
        ' C ' + c1x.toFixed(1) + ' ' + from.y.toFixed(1) +
        ', ' + c2x.toFixed(1) + ' ' + to.y.toFixed(1) +
        ', ' + to.x.toFixed(1) + ' ' + to.y.toFixed(1);
}

function diffConnectorHead(from, to) {
    var distance = Math.abs(to.x - from.x) || 1;
    var direction = to.x >= from.x ? 1 : -1;
    var headLength = Math.min(10, Math.max(4, distance * 0.35), distance * 0.65);
    var headWidth = Math.min(8, Math.max(4, headLength * 0.8));
    var base = {
        x: to.x - direction * headLength,
        y: to.y
    };

    return {
        base: base,
        width: headWidth,
        points: [
            to,
            { x: base.x, y: base.y - headWidth / 2 },
            { x: base.x, y: base.y + headWidth / 2 }
        ]
    };
}

function diffConnectorPointsAttr(points) {
    var parts = [];
    for (var i = 0; i < points.length; i++) {
        parts.push(points[i].x.toFixed(1) + ',' + points[i].y.toFixed(1));
    }
    return parts.join(' ');
}

function appendDiffConnectorForCommit(svg, from, to, kind) {
    var head = diffConnectorHead(from, to);
    var pathData = diffConnectorPathData(from, head.base);
    if (!pathData) return false;

    var path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('class', 'diff-link-path diff-link-' + (kind || 'move'));
    path.setAttribute('d', pathData);
    svg.appendChild(path);

    var polygon = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
    polygon.setAttribute('class', 'diff-link-head diff-link-' + (kind || 'move'));
    polygon.setAttribute('points', diffConnectorPointsAttr(head.points));
    svg.appendChild(polygon);
    return true;
}

function diffMoveArrowRoot() {
    if (!document.querySelector) return document;

    var fullscreenCell = document.querySelector('.rule-cell.fullscreen-rule');
    if (!fullscreenCell) return document;
    if (!fullscreenCell.querySelector) return null;
    return fullscreenCell.querySelector('.fullscreen-diff-pair');
}

function collectDiffLinkPairs() {
    var root = diffMoveArrowRoot();
    if (!root || !root.querySelectorAll) return {};

    var nodes = root.querySelectorAll('[data-diff-link-id][data-diff-link-scope]');
    var pairs = {};
    for (var i = 0; i < nodes.length; i++) {
        var node = nodes[i];
        var id = node.getAttribute('data-diff-link-id');
        var scope = node.getAttribute('data-diff-link-scope');
        var side = node.getAttribute('data-diff-link-side');
        var kind = node.getAttribute('data-diff-link-kind') || 'move';
        if (!id || !scope || (side !== 'from' && side !== 'to')) continue;

        var key = scope + ':' + id;
        if (!pairs[key]) pairs[key] = {};
        pairs[key].kind = kind;
        pairs[key][side] = node;
    }
    return pairs;
}

function updateDiffMoveArrows(visualCtx, diffArrowsDomGeneration) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        var expectedDomGeneration = visualCtx;
        var updated = false;
        runRenderFramePhaseNow('deferred', 'update-diff-arrows', function(ctx) {
            updated = updateDiffMoveArrows(ctx, expectedDomGeneration);
        }, {
            epochScopes: ['trace', 'diff', 'virtual'],
            surfaces: ['diff-arrows'],
            withVisualContext: true
        });
        return updated;
    }

    diffArrowRuntime().diffMoveArrowFrame = null;
    if (!hasDOM() || !diffRuntime().savedDiffState ||
            !diffRuntime().diffA || !diffRuntime().diffB) {
        return clearDiffMoveArrows(visualCtx);
    }
    var commitOptions = {
        label: 'update-diff-arrows'
    };
    if (diffArrowsDomGeneration !== undefined) {
        commitOptions.domGenerations = {
            diffArrows: diffArrowsDomGeneration
        };
    }

    return diffArrowSurface.commit(visualCtx, commitOptions, function() {
        if (!hasDOM() || !diffRuntime().savedDiffState ||
                !diffRuntime().diffA || !diffRuntime().diffB) {
            clearDiffMoveArrowsForCommit();
            return;
        }
        var viewport = diffMoveViewportSize();
        if (!viewport) {
            clearDiffMoveArrowsForCommit();
            return;
        }

        var pairs = collectDiffLinkPairs();
        var svg = ensureDiffMoveArrowOverlayForCommit(viewport.width, viewport.height);
        if (!svg) return;
        viewport = diffMoveViewportFromOverlay(svg, viewport);

        var count = 0;
        for (var key in pairs) {
            if (!Object.prototype.hasOwnProperty.call(pairs, key)) continue;
            if (count >= DIFF_MOVE_ARROW_MAX_PATHS) break;

            var pair = pairs[key];
            var from = diffMoveSourceEndpoint(pair.from, viewport);
            var to = diffMoveTargetEndpoint(pair.to, viewport);
            if (!diffMoveEndpointsConnectable(from, to)) continue;

            if (appendDiffConnectorForCommit(svg, from, to, pair.kind)) count++;
        }

        if (count) {
            bumpDiffArrowsDomGeneration();
        } else {
            clearDiffMoveArrowsForCommit(svg);
        }
    });
}

/** Close diff entirely, clearing both selections. */
function closeDiffFull() {
    TraceActions.diff.closeDiffFull();
}

/** Exit diff but keep one selection (the 'other' side). */
function restoreReleasedDiffRuleViewport(restoreViewport, releasedAnchor, releasedRef) {
    if (releasedRef) {
        var restored = restoreRuleAnchorWithFallback(
            releasedAnchor,
            function() { return ruleFullyVisibleInTrace(releasedRef); },
            function() { return centerRuleInTrace(releasedRef); }
        );
        if (restored) return;
    }

    restoreSearchViewportState(restoreViewport);
}

function exitDiffKeepOther(keepWhich, releasedRef) {
    TraceActions.diff.exitDiffKeepOther(keepWhich, releasedRef);
}

/** Compute and display the diff between A and B. */
function showDiffInline(anchorRef) {
    TraceActions.diff.showDiffInline(anchorRef);
}
