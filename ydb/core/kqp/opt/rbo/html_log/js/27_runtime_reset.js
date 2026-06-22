function cancelScheduledFrame(id) {
    if (id === null || id === undefined) return;
    if (typeof cancelRenderFrameWork === 'function' && cancelRenderFrameWork(id)) return;
    var cancelFrame = (typeof window !== 'undefined' && window.cancelAnimationFrame) ||
        clearTimeout;
    cancelFrame(id);
}

function clearTimerMap(timers) {
    timers = timers || {};
    for (var key in timers) {
        if (Object.prototype.hasOwnProperty.call(timers, key)) {
            clearTimeout(timers[key]);
        }
    }
}

function cancelPayloadBlockJobs(jobs) {
    jobs = jobs || {};
    for (var key in jobs) {
        if (!Object.prototype.hasOwnProperty.call(jobs, key)) continue;
        var job = jobs[key];
        if (job && job.workerJob && typeof job.workerJob.cancel === 'function') {
            job.workerJob.cancel();
        }
    }
}

function resetLazyRenderRuntime() {
    var lazy = TraceRuntime.state.lazyRender;
    bumpRuntimeEpoch('render');
    if (lazy.ruleRenderTimer !== null) {
        cancelScheduledFrame(lazy.ruleRenderTimer);
        lazy.ruleRenderTimer = null;
    }
    if (lazy.visibleRuleScanTimer !== null) {
        cancelScheduledFrame(lazy.visibleRuleScanTimer);
        lazy.visibleRuleScanTimer = null;
    }
    if (lazy.visibleRuleCheckTimer !== null) {
        cancelScheduledFrame(lazy.visibleRuleCheckTimer);
        lazy.visibleRuleCheckTimer = null;
    }

    clearTimerMap(lazy.delayedRuleLoadingTimers);
    cancelPayloadBlockJobs(lazy.payloadBlockJobs);
    lazy.delayedRuleLoadingTimers = {};
    lazy.ruleRenderQueue = [];
    lazy.queuedRuleRenders = {};
    lazy.pendingVisibleRuleChecks = {};
    lazy.payloadBlockJobs = {};
}

function resetVirtualizationRuntime() {
    var virtual = TraceRuntime.state.virtualization;
    bumpRuntimeEpoch('virtual');
    bumpRuntimeEpoch('layout');
    if (virtual.virtualRenderFrame !== null) {
        cancelScheduledFrame(virtual.virtualRenderFrame);
        virtual.virtualRenderFrame = null;
    }
    virtual.virtualRenderFrameCancel = null;
    if (virtual.traceMeasuredWidthObserver && virtual.traceMeasuredWidthObserver.disconnect) {
        virtual.traceMeasuredWidthObserver.disconnect();
    }

    virtual.mountedStageKeys = {};
    virtual.mountedRuleKeys = {};
    virtual.mountedVirtualRange = null;
    virtual.stageShellSignature = '';
    virtual.virtualRowSignatureCache = {};
    if (typeof clearDetachedRuleElementCache === 'function') {
        clearDetachedRuleElementCache();
    } else {
        virtual.detachedRuleElementCache = {};
        virtual.detachedRuleElementCacheOrder = [];
    }
    virtual.layoutWidthPartsCache = null;
    virtual.virtualRange = { left: 0, right: Infinity };
    virtual.traceLayoutModel = null;
    virtual.traceMeasuredWidthCache = {};
    clearTraceLayoutDirtyRegions();
    virtual.visibleStageCountCache = null;
    virtual.traceMeasuredWidthObserver = null;
    virtual.traceVirtualLayoutDirty = false;
    virtual.traceLayoutGeneration = 0;
    virtual.traceViewportGeneration = 0;
    virtual.lastTraceViewportInteractionAt = 0;
    virtual.lastTraceViewportInteractionGeneration = 0;
    virtual.suppressNextVirtualScroll = null;
    virtual.lastVirtualRefresh = null;
    virtual.traceScrollWidthCache = null;
}

function cancelDiffMoveArrowUpdate() {
    var arrows = TraceRuntime.state.diffArrows;
    if (arrows.diffMoveArrowFrame !== null) {
        cancelScheduledFrame(arrows.diffMoveArrowFrame);
        arrows.diffMoveArrowFrame = null;
    }
    arrows.diffMoveArrowTraceScrollLeft = null;
    arrows.diffMoveArrowTraceScrollTop = null;
    arrows.diffMoveArrowTranslateX = 0;
    arrows.diffMoveArrowTranslateY = 0;
}

function resetDiffRuntime() {
    var diff = TraceRuntime.state.diff;
    bumpRuntimeEpoch('diff');
    cancelPendingDiffJob();
    diff.activeDiffCache = { pairKey: '', result: null };
    cancelDiffMoveArrowUpdate();
    clearDiffMoveArrows();
    applyDiffSession(TraceDiffState.closeSession(currentDiffSession()).session);
    updateDiffButton();
}

function resetSearchRuntime() {
    var search = TraceRuntime.state.search;
    bumpRuntimeEpoch('search');
    cancelScheduledSearch();
    cancelAutoGlobalSearchTimer('reset');
    cancelSearchMatchCollectionJob('reset');
    cancelVisibleSearchScanJob('reset');
    cancelSearchIndexWorkerJob();
    cancelSearchLabelHighlightJob('reset');
    cancelCollapsedSearchIndicatorJob('reset');
    if (search.collapsedSearchIndicatorRetryFrame) {
        cancelRenderFrameWork(search.collapsedSearchIndicatorRetryFrame);
        search.collapsedSearchIndicatorRetryFrame = null;
    }
    cancelSearchDecorationJob('reset');
    cancelPendingHighlights();
    if (search.searchNavRepeatState) stopSearchNavRepeat();

    search.savedSearchState = null;
    search.savedSearchViewport = null;
    search.searchNavigationCommitted = false;
    search.searchLayoutApplied = false;
    search.searchExpandOverlay = null;
    search.visibleSearchScanFrame = null;
    search.visibleSearchScanJob = null;
    search.autoGlobalSearchTimer = null;
    search.autoGlobalSearchTimerCancel = null;
    search.autoGlobalSearchIntent = null;
    search.searchDirty = false;
    search.searchInputDebounceStartedAt = 0;
    search.searchRunReason = '';
    search.searchLayers = {
        visible: {
            query: '',
            scope: 'tree-rules',
            mode: 'find',
            traceGeneration: 0,
            payloadGeneration: 0,
            projectionGeneration: 0,
            knownCount: 0,
            matches: [],
            searchedRules: {}
        },
        localExactCache: {
            entries: {},
            order: [],
            hits: 0,
            misses: 0
        },
        lazyIndex: {
            key: '',
            query: '',
            scope: 'tree-rules',
            traceGeneration: 0,
            ruleCount: 0,
            entries: {},
            order: [],
            version: 0,
            complete: false
        },
        lazyIndexCache: {
            entries: {},
            order: [],
            hits: 0,
            misses: 0
        },
        globalSummaryCache: {
            entries: {},
            order: [],
            hits: 0,
            misses: 0
        },
        globalJob: {
            nextJobId: 1,
            activeJob: null
        }
    };
    search.ruleSearchIndex = null;
    search.searchIndexWorkerJob = null;
    search.searchIndexProgress = { state: 'unbuilt', mode: 'none', completed: 0, total: 0 };
    search.activeSearchQueryEpoch = 0;
    search.searchResultEpoch = 0;
    search.searchResultState = 'idle';
    search.searchResultQuery = '';
    search.searchResultScope = 'tree-rules';
    search.searchResultMode = 'find';
    search.pendingSearchQuery = '';
    search.pendingSearchScope = 'tree-rules';
    search.pendingSearchMode = 'find';
    search.searchResultStaleForNavigation = false;
    search.nextSearchTransactionId = 1;
    search.activeSearchTransaction = null;
    search.nextSearchMatchCollectionJobId = 1;
    search.searchMatchCollectionJob = null;
    search.searchResultCache = {};
    search.searchResultCacheOrder = [];
    search.searchExpandOverlayCache = {};
    search.searchExpandOverlayCacheOrder = [];
    search.searchCacheTraceGeneration = 0;
    search.searchCachePayloadGeneration = -1;
    search.searchCacheBaseLayoutGeneration = 0;
    search.searchCacheIndexMode = '';
    search.searchCacheShowEmptyStages = false;
    search.searchBaseLayoutGeneration = 0;
    search.suppressSearchBaseLayoutCacheDirty = 0;
    search.searchResultCacheHits = 0;
    search.searchResultCacheMisses = 0;
    search.searchExpandOverlayCacheHits = 0;
    search.searchExpandOverlayCacheMisses = 0;
    search.nextSearchLabelHighlightJobId = 1;
    search.searchLabelHighlightJob = null;
    search.nextSearchDecorationJobId = 1;
    search.searchDecorationJob = null;
    search.mountedSearchLabelCache = null;
    search.mountedSearchLabelCacheScans = 0;
    search.mountedSearchLabelCacheHits = 0;
    search.searchTimingEpoch = 0;
    search.searchTimingCurrent = null;
    search.searchTimingHistory = [];
    search.lastSearchQuery = '';
    search.searchMatches = [];
    search.searchExpandableMatchCount = 0;
    search.searchNavigationLocationCache = null;
    search.currentSearchMatchIndex = -1;
    search.searchNavigationMatchIndex = -1;
    search.activeSearchMatchRecord = null;
    search.pendingSearchActivation = null;
    search.collapsedSearchIndicators = emptyCollapsedSearchIndicators();
    search.collapsedSearchIndicatorCount = 0;
    search.collapsedSearchIndicatorsDirty = false;
    search.nextCollapsedSearchIndicatorJobId = 1;
    search.collapsedSearchIndicatorJob = null;
    search.collapsedSearchIndicatorRetryFrame = null;
    currentUiState().searchToken++;

    var box = hasDOM() ? document.getElementById('search-box') : null;
    if (box) box.value = '';
    clearActiveSearchMatch();
    updateSearchStatus(0);
}

function resetFullscreenRuntime() {
    var fullscreen = fullscreenRuntime();
    if (fullscreen.fullscreenNavRepeatState) stopFullscreenNavRepeat();
    if (fullscreen.fullscreenScrollbarRevealTimer !== null) {
        clearTimeout(fullscreen.fullscreenScrollbarRevealTimer);
        fullscreen.fullscreenScrollbarRevealTimer = null;
    }
    setFullscreenState(null, null);
    if (hasDOM() && document.body) {
        document.body.classList.remove('fullscreen-active');
        document.body.classList.remove('fullscreen-scrollbar-active');
    }
    clearFullscreenBackgroundInert();
    removeFullscreenOverlay();
}

function resetRuleResizeRuntime() {
    var resize = resizeRuntime();
    var drag = resize.ruleResizeDrag;
    var columnDrag = resize.nodeColumnResizeDrag;
    resize.nodeColumnResizeDrag = null;
    if (hasDOM() && document.body) {
        document.body.classList.remove('node-column-resizing');
    }
    if (!drag) {
        if (typeof clearResizeDeferredFrameWork === 'function') clearResizeDeferredFrameWork();
        if (columnDrag) bumpRuntimeEpoch('resize');
        return;
    }
    if (drag.frame !== null) cancelScheduledFrame(drag.frame);
    if (typeof removeRuleResizeGuide === 'function') removeRuleResizeGuide(drag);
    resize.ruleResizeDrag = null;
    bumpRuntimeEpoch('resize');
    if (hasDOM() && document.body) {
        document.body.classList.remove('rule-resizing');
        document.body.classList.remove('rule-resizing-deferred');
    }
    if (typeof window !== 'undefined' && window.removeEventListener) {
        window.removeEventListener('pointermove', updateRuleResize);
        window.removeEventListener('pointerup', finishRuleResize);
        window.removeEventListener('pointercancel', finishRuleResize);
    }
    if (typeof clearResizeDeferredFrameWork === 'function') clearResizeDeferredFrameWork();
}

function resetVisualRuntime() {
    var visual = visualRuntime();
    var traceSwitchOwnerId = visual.traceSwitchOwnerId;
    var preservedOwners = [];
    if (traceSwitchOwnerId) {
        for (var surface in visual.surfaceOwners) {
            if (!Object.prototype.hasOwnProperty.call(visual.surfaceOwners, surface)) continue;
            if (visual.surfaceOwners[surface].id === traceSwitchOwnerId) {
                preservedOwners.push(cloneVisualOwner(visual.surfaceOwners[surface]));
            }
        }
    }
    clearVisualSurfaceOwners();
    visual.deferredWork = {};
    for (var i = 0; i < preservedOwners.length; i++) {
        visual.surfaceOwners[preservedOwners[i].surface] = preservedOwners[i];
    }
}

function resetTraceAnchorRuntime() {
    var anchor = traceAnchorRuntime();
    cancelTraceAnchorLineUpdate();
    if (anchor.traceAnchorLineEl && anchor.traceAnchorLineEl.parentNode) {
        anchor.traceAnchorLineEl.parentNode.removeChild(anchor.traceAnchorLineEl);
    }
    anchor.traceAnchorLineEl = null;
    anchor.traceAnchorPreviewActive = false;
    anchor.traceAnchorPreviewHovered = false;
    anchor.traceAnchorPreviewHoverTarget = null;
    anchor.traceAnchorPreviewFocused = false;
    anchor.traceAnchorPreviewFocusTarget = null;
    anchor.traceAnchorPreviewPointerFocused = false;
    anchor.traceAnchorPreviewPointerFocusTarget = null;
    anchor.traceAnchorPreviewSearchFocused = false;
    anchor.traceAnchorPreviewSearchFocusTarget = null;
}

function traceSessionKey(index) {
    index = Math.max(0, Number(index) || 0);
    return String(index);
}

function cloneTraceSessionRuleRef(ref) {
    if (!ref) return null;
    return { si: ref.si, gi: ref.gi, ri: ref.ri };
}

function cloneTraceSessionViewport(state) {
    if (!state) return null;
    return {
        traceLeft: state.traceLeft || 0,
        traceTop: state.traceTop || 0,
        windowX: state.windowX || 0,
        windowY: state.windowY || 0
    };
}

function cloneTraceSessionLayout(snapshot) {
    if (!snapshot) return null;
    if (!Array.isArray(snapshot)) return null;
    return snapshot.map(function(stage) {
        stage = stage || {};
        return {
            open: !!stage.open,
            groups: (stage.groups || []).map(function(group) {
                group = group || {};
                return {
                    open: !!group.open,
                    rules: (group.rules || []).map(function(rule) {
                        rule = rule || {};
                        var copy = { open: !!rule.open };
                        if ('fields' in rule) copy.fields = !!rule.fields;
                        if ('pinned' in rule) copy.pinned = !!rule.pinned;
                        if ('info' in rule) copy.info = !!rule.info;
                        return copy;
                    })
                };
            })
        };
    });
}

function cloneTraceSessionSearchMatch(match) {
    if (!match) return null;
    var copy = {
        type: match.type,
        field: match.field,
        occurrence: match.occurrence || 0
    };
    if (match.si !== undefined) copy.si = match.si;
    if (match.gi !== undefined) copy.gi = match.gi;
    if (match.ri !== undefined) copy.ri = match.ri;
    if (match.rawIdx !== undefined) copy.rawIdx = match.rawIdx;
    return copy;
}

function currentTraceSessionSearchQuery() {
    if (!hasDOM()) return searchRuntime().lastSearchQuery || '';

    var box = document.getElementById('search-box');
    return (box ? box.value : searchRuntime().lastSearchQuery || '').trim();
}

function captureTraceSessionSearchIntent() {
    var query = currentTraceSessionSearchQuery();
    if (!query && !currentUiState().searchActive) return null;

    return {
        query: query,
        mode: hasDOM() ? currentSearchMode() : 'find',
        scope: hasDOM() ? currentSearchScope() : 'tree-rules',
        currentMatchIndex: searchRuntime().currentSearchMatchIndex,
        activeMatch: cloneTraceSessionSearchMatch(activeSearchMatch()),
        layoutApplied: !!searchRuntime().searchLayoutApplied,
        navigationCommitted: !!searchRuntime().searchNavigationCommitted,
        savedState: cloneTraceSessionLayout(searchRuntime().savedSearchState),
        savedViewport: cloneTraceSessionViewport(searchRuntime().savedSearchViewport)
    };
}

function captureTraceSessionDiffIntent() {
    var session = currentDiffSession();
    if (!session.a && !session.b) return null;

    return {
        a: cloneTraceSessionRuleRef(session.a),
        b: cloneTraceSessionRuleRef(session.b),
        inlineActive: !!(session.savedState && session.a && session.b),
        savedState: cloneTraceSessionLayout(session.savedState),
        savedViewport: cloneTraceSessionViewport(session.savedViewport)
    };
}

function cloneTraceSessionSearchIntent(intent) {
    if (!intent) return null;
    return {
        query: intent.query || '',
        mode: intent.mode || 'find',
        scope: intent.scope || 'tree-rules',
        currentMatchIndex: intent.currentMatchIndex,
        activeMatch: cloneTraceSessionSearchMatch(intent.activeMatch),
        layoutApplied: !!intent.layoutApplied,
        navigationCommitted: !!intent.navigationCommitted,
        savedState: cloneTraceSessionLayout(intent.savedState),
        savedViewport: cloneTraceSessionViewport(intent.savedViewport)
    };
}

function cloneTraceSessionDiffIntent(intent) {
    if (!intent) return null;
    return {
        a: cloneTraceSessionRuleRef(intent.a),
        b: cloneTraceSessionRuleRef(intent.b),
        inlineActive: !!intent.inlineActive,
        savedState: cloneTraceSessionLayout(intent.savedState),
        savedViewport: cloneTraceSessionViewport(intent.savedViewport)
    };
}

function cloneTraceSession(session) {
    if (!session) return null;
    return {
        layout: cloneTraceSessionLayout(session.layout),
        viewport: cloneTraceSessionViewport(session.viewport),
        search: cloneTraceSessionSearchIntent(session.search),
        diff: cloneTraceSessionDiffIntent(session.diff)
    };
}

function captureTraceSession() {
    saveVisibleTreeMaterializerAnchors();
    return {
        layout: cloneTraceSessionLayout(saveLayoutState()),
        viewport: cloneTraceSessionViewport(saveSearchViewportState()),
        search: captureTraceSessionSearchIntent(),
        diff: captureTraceSessionDiffIntent()
    };
}

function saveActiveTraceSession() {
    var trace = traceRuntime();
    if (!trace.activeTraceLoaded) return;
    trace.traceSessions[traceSessionKey(trace.activeTraceIndex)] = captureTraceSession();
}

function traceSessionForIndex(index) {
    return cloneTraceSession(traceRuntime().traceSessions[traceSessionKey(index)]);
}

function traceSessionHasViewport(session) {
    return !!(session && session.viewport);
}

function normalizedTraceSessionRuleRef(ref) {
    if (!ref) return null;

    var si = Number(ref.si);
    var gi = Number(ref.gi);
    var ri = Number(ref.ri);
    if (!Number.isInteger(si) || !Number.isInteger(gi) || !Number.isInteger(ri)) {
        return null;
    }
    if (!RuleRefs.isValid(currentTraceGroups(), currentUiState(), si, gi, ri)) {
        return null;
    }
    return { si: si, gi: gi, ri: ri };
}

function restoreTraceSessionLayoutBeforeRender(session) {
    if (!session || !session.layout) return;
    TraceState.applyLayoutState(
        currentUiState(),
        currentTraceGroups(),
        cloneTraceSessionLayout(session.layout)
    );
    invalidateTraceMeasuredWidthCache();
}

function restoreTraceSessionDiffBeforeRender(session) {
    if (!session || !session.diff) return;

    var a = normalizedTraceSessionRuleRef(session.diff.a);
    var b = normalizedTraceSessionRuleRef(session.diff.b);
    if (a && !ruleSupportsDiff(a)) a = null;
    if (b && !ruleSupportsDiff(b)) b = null;
    var inlineActive = !!(session.diff.inlineActive && a && b && session.diff.savedState);
    if (!a && !b) return;

    applyDiffSession({
        a: a,
        b: b,
        savedState: inlineActive ? cloneTraceSessionLayout(session.diff.savedState) : null,
        savedViewport: inlineActive ? cloneTraceSessionViewport(session.diff.savedViewport) : null
    });
}

function restoreTraceSessionBeforeRender(session) {
    restoreTraceSessionLayoutBeforeRender(session);
    restoreTraceSessionDiffBeforeRender(session);
}

function normalizeTraceSessionSearchMode(mode) {
    return 'find';
}

function restoreTraceSessionSearchControls(intent) {
    if (!intent || !intent.query || !hasDOM()) return false;

    var box = document.getElementById('search-box');
    if (box) box.value = intent.query;
    syncTraceAnchorPreviewActive();
    return true;
}

function restoredTraceSessionSearchMatchIndex(intent) {
    if (!intent) return -1;
    if (intent.activeMatch) {
        var activeIndex = findSearchMatchIndex(intent.activeMatch);
        if (activeIndex >= 0) return activeIndex;
    }

    var index = Math.floor(Number(intent.currentMatchIndex));
    if (!Number.isFinite(index)) return -1;
    if (index < 0 || index >= searchRuntime().searchMatches.length) return -1;
    return index;
}

function restoreTraceSessionSearch(intent) {
    if (!restoreTraceSessionSearchControls(intent)) return;

    var scope = currentSearchScope();
    var mode = normalizeTraceSessionSearchMode(intent.mode);
    beginSearchTimingRun(intent.query, scope, mode);
    var transactionToken = activeSearchTransactionToken();
    var state = currentUiState();
    var search = searchRuntime();
    state.searchActive = true;
    state.searchToken++;
    search.searchDirty = false;
    search.savedSearchState = cloneTraceSessionLayout(intent.savedState);
    search.savedSearchViewport = cloneTraceSessionViewport(intent.savedViewport);
    search.searchNavigationCommitted = !!intent.navigationCommitted;
    search.searchLayoutApplied = !!intent.layoutApplied;
    search.lastSearchQuery = intent.query;

    ensureSearchIndex();

    var collectedSearch = collectSearchStateForCurrentSearch(intent.query, scope);
    var owner = createSearchTransactionResultOwner(
        transactionToken,
        'trace-session-search-restore',
        { currentness: { payload: true } }
    );
    var collapsedIndicatorsApplied = false;
    if (owner) {
        collapsedIndicatorsApplied = owner.applyResults({
            query: intent.query,
            scope: scope,
            mode: mode,
            state: 'ready',
            matches: collectedSearch.matches,
            collapsedIndicators: collectedSearch.collapsedIndicators,
            preserveActive: false
        });
    } else {
        replaceSearchMatches(collectedSearch.matches, false);
        markSearchResultReady(intent.query, scope, mode);
    }
    updateSearchLabelHighlights(intent.query, scope, {
        collapsedIndicators: collectedSearch.collapsedIndicators,
        skipCollapsedIndicators: collapsedIndicatorsApplied,
        searchTransactionToken: transactionToken
    });

    var index = restoredTraceSessionSearchMatchIndex(intent);
    if (index >= 0) {
        setActiveSearchMatch(index, {
            preserveLayout: true,
            preserveViewport: true
        });
    } else {
        updateSearchStatus(search.searchMatches.length);
    }

    scheduleHighlightJobs(
        collectRenderJobsForCurrentState(intent.query, false, scope),
        state.searchToken,
        function() {
            updateSearchStatus(search.searchMatches.length);
        },
        {
            searchTransactionToken: transactionToken
        }
    );
}

function restoreTraceSessionViewport(session) {
    if (!session || !session.viewport) return;
    restoreSearchViewportState(session.viewport);
}

function restartRestoredInlineDiff(session) {
    if (!session || !session.diff) return;

    updateDiffButton();
    updateDiffSelectionButtonContent();

    if (!isDiffActive()) {
        scheduleDiffMoveArrows();
        return;
    }

    var pairKey = activeDiffPairKey();
    if (!pairKey) return;
    beginPendingDiffJob(pairKey);
    renderPendingDiffPlainTrees();
    scheduleDiffMoveArrows();
}

function restoreTraceSessionAfterRender(session) {
    if (!session) return;
    restoreTraceSessionViewport(session);
    restoreTraceSessionSearch(session.search);
    restartRestoredInlineDiff(session);
}

function resetTraceSwitchRuntime(toTraceIndex) {
    markTraceLayoutDirtyTraceSwitch(traceRuntime().activeTraceIndex, toTraceIndex);
    bumpRuntimeEpoch('trace');
    closeTopBarPopovers();
    resetRenderFrameRuntime();
    resetRuleResizeRuntime();
    resetFullscreenRuntime();
    resetDiffRuntime();
    resetSearchRuntime();
    resetLazyRenderRuntime();
    resetVirtualizationRuntime();
    resetVisualRuntime();
    resetTraceAnchorRuntime();
    resetWheelScrollRuntime();
}

function refreshTraceControlsAfterLoad() {
    syncTracePicker();
    updateBulkDetailControls();
    updateCycleButtonLabel();
    updateDiffButton();
}

function renderLoadedTrace(options) {
    renderTraceShell();
    clearTraceMeasuredWidthCache();
    if (!useVirtualizedDisplayRefresh()) updateAllLayoutWidths();
    updateVirtualRange();
    refreshVirtualRowsNow();
    scheduleVisibleRuleRenderScan();
    updateSearchLabelHighlights('', currentSearchScope(), {
        collapsedIndicators: emptyCollapsedSearchIndicators()
    });
    refreshTraceControlsAfterLoad();
    if (!options || options.resetScroll !== false) resetTraceScroll();
}

function setActiveTrace(index) {
    return TraceActions.trace.setActiveTrace(index);
}
