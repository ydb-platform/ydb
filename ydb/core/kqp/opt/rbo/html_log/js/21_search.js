/* ══════════════════════════════════════════════════════════════
   Search
   ══════════════════════════════════════════════════════════════ */

var SEARCH_CHUNKED_STAGE_THRESHOLD = 240;
var SEARCH_MATCH_COLLECTION_STAGE_BATCH = 40;
var SEARCH_TIMING_HISTORY_LIMIT = 8;
var SEARCH_TIMING_ENTRY_LIMIT = 160;
var SEARCH_RESULT_CACHE_LIMIT = 12;
var SEARCH_LOCAL_EXACT_CACHE_LIMIT = 24;
var SEARCH_LAZY_INDEX_CACHE_LIMIT = 8;
var SEARCH_GLOBAL_SUMMARY_CACHE_LIMIT = 8;
var SEARCH_EXPAND_OVERLAY_CACHE_LIMIT = 8;
var SEARCH_SCROLL_BACKPRESSURE_MS = 140;
var SEARCH_PARTIAL_COMMIT_MIN_INTERVAL_MS = 120;
var SEARCH_PARTIAL_COMMIT_BATCH_INTERVAL = 4;
var SEARCH_FIND_NEXT_RULE_BATCH_SIZE = 4;
var SEARCH_VISIBLE_SCAN_RULE_BATCH_SIZE = 4;
var SEARCH_INLINE_GLOBAL_SUMMARY_NODE_THRESHOLD = 1000;

function searchTimingNow() {
    if (typeof performance !== 'undefined' && performance && performance.now) {
        return performance.now();
    }
    return Date.now ? Date.now() : 0;
}

function roundedSearchTiming(value) {
    value = Number(value);
    if (!Number.isFinite(value) || value < 0) return 0;
    return Math.round(value * 100) / 100;
}

function pushSearchTimingHistory(run) {
    if (!run) return;
    var search = searchRuntime();
    run.finishedAt = roundedSearchTiming(searchTimingNow());
    search.searchTimingHistory.push(run);
    if (search.searchTimingHistory.length > SEARCH_TIMING_HISTORY_LIMIT) {
        search.searchTimingHistory.splice(0, search.searchTimingHistory.length - SEARCH_TIMING_HISTORY_LIMIT);
    }
}

function beginSearchTimingRun(query, scope, mode) {
    var search = searchRuntime();
    cancelSearchMatchCollectionJob('new-search');
    pushSearchTimingHistory(search.searchTimingCurrent);
    search.restoredSearchIndicatorMatches = null;
    markPendingSearchIntent(query, scope, mode);
    search.searchTimingCurrent = {
        epoch: ++search.searchTimingEpoch,
        query: String(query || ''),
        scope: String(scope || ''),
        mode: String(mode || ''),
        traceGeneration: Number(currentTraceStore().traceGeneration) || 0,
        startedAt: roundedSearchTiming(searchTimingNow()),
        updatedAt: roundedSearchTiming(searchTimingNow()),
        totals: {},
        entries: []
    };
    search.activeSearchQueryEpoch = search.searchTimingCurrent.epoch;
    beginSearchTransaction(query, scope, mode);
    search.searchResultState = 'pending';
    return search.searchTimingCurrent;
}

function currentSearchTimingEpoch() {
    var run = searchRuntime().searchTimingCurrent;
    return run ? run.epoch : 0;
}

function currentSearchPayloadGeneration() {
    var materialization = currentTraceStore().materialization || {};
    return Math.max(0, Math.floor(Number(materialization.generation)) || 0);
}

function currentSearchLayoutGeneration() {
    return Math.max(0, Math.floor(Number(virtualRuntime().traceLayoutGeneration)) || 0);
}

function currentSearchViewportGeneration() {
    return Math.max(0, Math.floor(Number(virtualRuntime().traceViewportGeneration)) || 0);
}

function currentTraceViewportInteractionGeneration() {
    return Math.max(0, Math.floor(Number(virtualRuntime().lastTraceViewportInteractionGeneration)) || 0);
}

function traceViewportInteractionFresh() {
    var generation = currentTraceViewportInteractionGeneration();
    if (!generation) return false;
    var lastAt = Number(virtualRuntime().lastTraceViewportInteractionAt) || 0;
    return searchTimingNow() - lastAt < SEARCH_SCROLL_BACKPRESSURE_MS;
}

function searchShouldYieldToViewportInteraction(state) {
    state = state || {};
    if (!traceViewportInteractionFresh()) return false;
    var generation = currentTraceViewportInteractionGeneration();
    if (!generation || state.viewportYieldGeneration === generation) return false;
    state.viewportYieldGeneration = generation;
    return true;
}

function createSearchTransaction(query, scope, mode) {
    var search = searchRuntime();
    var runtimeEpoch = currentRuntimeEpoch();
    var store = currentTraceStore();
    var id = search.nextSearchTransactionId++;
    return {
        id: id,
        ownerId: 'search-transaction-' + id,
        query: String(query || ''),
        scope: String(scope || ''),
        mode: String(mode || ''),
        traceGeneration: Number(store && store.traceGeneration) || 0,
        traceEpoch: runtimeEpoch.trace,
        searchEpoch: runtimeEpoch.search,
        payloadGeneration: currentSearchPayloadGeneration(),
        layoutEpoch: currentSearchLayoutGeneration(),
        viewportEpoch: currentSearchViewportGeneration(),
        timingEpoch: currentSearchTimingEpoch(),
        createdAt: roundedSearchTiming(searchTimingNow()),
        phaseLog: []
    };
}

function beginSearchTransaction(query, scope, mode) {
    var tx = createSearchTransaction(query, scope, mode);
    searchRuntime().activeSearchTransaction = tx;
    return tx;
}

function searchTransactionFrameToken(tx) {
    return {
        trace: tx && tx.traceEpoch,
        search: tx && tx.searchEpoch
    };
}

function searchTransactionToken(tx) {
    if (!tx) return null;
    return {
        id: tx.id,
        ownerId: tx.ownerId || '',
        query: tx.query,
        scope: tx.scope,
        mode: tx.mode,
        traceGeneration: tx.traceGeneration,
        traceEpoch: tx.traceEpoch,
        searchEpoch: tx.searchEpoch,
        payloadGeneration: tx.payloadGeneration,
        layoutEpoch: tx.layoutEpoch,
        viewportEpoch: tx.viewportEpoch,
        timingEpoch: tx.timingEpoch
    };
}

function activeSearchTransactionToken() {
    return searchTransactionToken(searchRuntime().activeSearchTransaction);
}

function refreshSearchTransactionPayloadGeneration(tx) {
    if (tx) tx.payloadGeneration = currentSearchPayloadGeneration();
    return tx;
}

function refreshSearchTransactionLayoutGeneration(tx) {
    if (tx) tx.layoutEpoch = currentSearchLayoutGeneration();
    return tx;
}

function refreshSearchTransactionTokenLayoutGeneration(token) {
    if (!token) return token;
    token.layoutEpoch = currentSearchLayoutGeneration();
    var tx = searchRuntime().activeSearchTransaction;
    if (tx && tx.id === token.id) refreshSearchTransactionLayoutGeneration(tx);
    return token;
}

function searchTransactionTokenCurrent(token, options) {
    if (!token) return true;
    options = options || {};
    var tx = searchRuntime().activeSearchTransaction;
    var runtimeEpoch = currentRuntimeEpoch();
    var store = currentTraceStore();
    if (!tx) return false;
    if (tx.id !== token.id) return false;
    if (tx.query !== token.query || tx.scope !== token.scope || tx.mode !== token.mode) return false;
    if (Number(tx.traceGeneration) !== Number(token.traceGeneration)) return false;
    if (Number(store && store.traceGeneration) !== Number(token.traceGeneration)) return false;
    if (Number(tx.traceEpoch) !== Number(token.traceEpoch)) return false;
    if (Number(runtimeEpoch.trace) !== Number(token.traceEpoch)) return false;
    if (Number(tx.searchEpoch) !== Number(token.searchEpoch)) return false;
    if (Number(runtimeEpoch.search) !== Number(token.searchEpoch)) return false;
    if (Number(tx.timingEpoch) !== Number(token.timingEpoch)) return false;
    if (options.payload &&
            Number(currentSearchPayloadGeneration()) !== Number(token.payloadGeneration)) {
        return false;
    }
    if (options.layout &&
            Number(currentSearchLayoutGeneration()) !== Number(token.layoutEpoch)) {
        return false;
    }
    if (options.viewport &&
            Number(currentSearchViewportGeneration()) !== Number(token.viewportEpoch)) {
        return false;
    }
    return true;
}

function searchTransactionIntentCurrent(token, query, scope, mode) {
    if (!token) return true;
    var intent = normalizeSearchIntent(query, scope, mode);
    return token.query === intent.query &&
        token.scope === intent.scope &&
        token.mode === intent.mode;
}

function createSearchTransactionResultOwner(transactionToken, label, options) {
    options = options || {};
    var currentness = options.currentness || { payload: true };
    if (!transactionToken ||
            !searchTransactionTokenCurrent(transactionToken, currentness)) {
        return null;
    }

    return {
        token: transactionToken,
        label: label || 'search-results',
        currentness: currentness,
        applyResults: function(result) {
            return applySearchTransactionResults(this, result);
        },
        markReady: function(query, scope, mode) {
            return applySearchTransactionResults(this, {
                query: query,
                scope: scope,
                mode: mode,
                state: 'ready'
            });
        },
        ensureIntent: function(query, scope, mode) {
            return applySearchTransactionResults(this, {
                query: query,
                scope: scope,
                mode: mode,
                state: 'intent'
            });
        }
    };
}

function searchTransactionResultOwnerCurrent(owner) {
    return !!owner &&
        !!owner.token &&
        searchTransactionTokenCurrent(owner.token, owner.currentness);
}

function searchTransactionResultState(state) {
    if (state === 'partial-visible') return 'partial-visible';
    if (state === 'partial') return 'partial';
    if (state === 'intent') return 'intent';
    return 'ready';
}

function applySearchTransactionResults(owner, result) {
    result = result || {};
    if (!searchTransactionResultOwnerCurrent(owner)) return false;
    if (!searchTransactionIntentCurrent(
            owner.token,
            result.query,
            result.scope,
            result.mode)) {
        return false;
    }

    var state = searchTransactionResultState(result.state);
    if (state === 'partial-visible') {
        markSearchResultPartial(result.query, result.scope, result.mode, 'partial-visible');
    } else if (state === 'partial') {
        markSearchResultPartial(result.query, result.scope, result.mode, 'partial');
    } else if (state === 'intent') {
        ensureCommittedSearchResultIntent(result.query, result.scope, result.mode);
    } else {
        markSearchResultReady(result.query, result.scope, result.mode);
    }

    if (Object.prototype.hasOwnProperty.call(result, 'matches')) {
        replaceSearchMatches(result.matches, result.preserveActive !== false, {
            deferStatus: !!result.deferStatus,
            skipNavigationLocationCache: !!result.skipNavigationLocationCache ||
                state === 'partial' ||
                state === 'partial-visible'
        });
    }
    if (Object.prototype.hasOwnProperty.call(result, 'expandableMatchCount')) {
        searchRuntime().searchExpandableMatchCount = normalizeSearchCount(result.expandableMatchCount);
    }
    if (Object.prototype.hasOwnProperty.call(result, 'collapsedIndicators') &&
            typeof applyCollapsedSearchIndicators === 'function') {
        applyCollapsedSearchIndicators(result.collapsedIndicators, {
            query: result.query || '',
            scope: result.scope || '',
            searchTransactionToken: owner.token,
            allowWithoutSearchTransaction: !!result.allowWithoutSearchTransaction
        });
    }
    return true;
}

function recordSearchTransactionPhase(tx, phase, label, status) {
    if (!tx) return;
    tx.phaseLog.push({
        phase: phase || '',
        label: label || '',
        status: status || '',
        at: roundedSearchTiming(searchTimingNow() - tx.createdAt)
    });
    if (tx.phaseLog.length > 24) tx.phaseLog.splice(0, tx.phaseLog.length - 24);
}

function runSearchTransactionPhase(tx, phase, label, fn, surfaces) {
    if (!tx || typeof fn !== 'function') return false;

    var result;
    var ran = runRenderFramePhaseNow(phase, label, function(visualCtx) {
        result = fn(visualCtx);
    }, {
        token: searchTransactionFrameToken(tx),
        ownerId: tx.ownerId || '',
        surfaces: surfaces || [],
        onDiscard: function(reason) {
            recordSearchTransactionPhase(tx, phase, label, reason || 'discarded');
        }
    });

    if (ran) recordSearchTransactionPhase(tx, phase, label, 'run');
    return ran ? result : false;
}

function runSearchTransactionPhases(tx, phases) {
    phases = phases || [];
    for (var i = 0; i < phases.length; i++) {
        var step = phases[i] || {};
        if (runSearchTransactionPhase(
                tx,
                step.phase,
                step.label,
                step.run,
                step.surfaces) === false) {
            return false;
        }
    }
    return true;
}

function cancelSearchMatchCollectionJob(reason) {
    var search = searchRuntime();
    var job = search.searchMatchCollectionJob;
    if (!job) return false;
    job.cancelled = true;
    job.cancelReason = reason || 'cancelled';
    if (job.frameHandle) cancelRenderFrameWork(job.frameHandle);
    if (search.searchMatchCollectionJob === job) search.searchMatchCollectionJob = null;
    return true;
}

function cancelSearchExecutionPastPreview(reason) {
    cancelSearchMatchCollectionJob(reason || 'pending-search-input');
    cancelAutoGlobalSearchTimer(reason || 'pending-search-input');
    cancelGlobalSearchSummaryJob(reason || 'pending-search-input');
    cancelVisibleSearchScanJob(reason || 'pending-search-input');
    cancelPendingHighlights();
}

function searchIndexRecordCount(index) {
    var records = index && index.searchRecords;
    if (!records) return 0;
    return (records.stageTitles || []).length +
        (records.groupTitles || []).length +
        (records.ruleTitles || []).length +
        (records.treeLabels || []).length +
        (records.textTiles || []).length;
}

function shouldChunkSearchMatchCollection(query, scope, mode) {
    if (!query) return false;
    var index = searchRuntime().ruleSearchIndex;
    if (!index || !index.searchRecords) return false;
    if (TraceStore.stageCount(currentTraceStore()) >= SEARCH_CHUNKED_STAGE_THRESHOLD) return true;
    return searchIndexRecordCount(index) >= SEARCH_CHUNKED_STAGE_THRESHOLD * 20;
}

function searchMatchIdentityPart(value) {
    return value === undefined || value === null ? '' : String(value);
}

function searchMatchStableKey(match) {
    var record = searchMatchStableRecord(match);
    if (!record) return '';
    var target = record.ruleHandle || record.groupHandle || record.stageHandle || '';
    if (!target) {
        target = [
            searchMatchIdentityPart(record.si),
            searchMatchIdentityPart(record.gi),
            searchMatchIdentityPart(record.ri),
            searchMatchIdentityPart(record.rawIdx)
        ].join(':');
    }
    return [
        searchMatchIdentityPart(record.traceGeneration),
        searchMatchIdentityPart(record.type),
        searchMatchIdentityPart(target),
        searchMatchIdentityPart(record.field),
        searchMatchIdentityPart(record.path),
        record.field !== 'name' &&
            record.fieldOrdinal !== undefined && record.recordOccurrence !== undefined
            ? 'record:' + searchMatchIdentityPart(record.fieldOrdinal) + ':' +
                searchMatchIdentityPart(record.recordOccurrence)
            : 'occurrence:' + searchMatchIdentityPart(record.occurrence)
    ].join('|');
}

function searchMatchRenderedTargetId(match) {
    if (!match) return '';
    return searchRenderedTargetId({
        type: match.type,
        si: match.si,
        gi: match.gi,
        ri: match.ri,
        field: match.field,
        path: match.path
    });
}

function searchMatchRenderedMatchId(match) {
    var targetId = searchMatchRenderedTargetId(match);
    if (!targetId) return '';
    var occurrence = match && match.recordOccurrence !== undefined
        ? match.recordOccurrence
        : (match && match.occurrence !== undefined ? match.occurrence : 0);
    return searchRenderedMatchId(targetId, occurrence);
}

function seedSearchMatchKeyIndex(target, keyIndex) {
    target = target || [];
    keyIndex = keyIndex || {};
    for (var i = 0; i < target.length; i++) {
        var key = searchMatchStableKey(target[i]);
        if (key && keyIndex[key] === undefined) keyIndex[key] = i;
    }
    return keyIndex;
}

function mergeSearchMatchProperties(target, source) {
    if (!target || !source) return target;
    for (var key in source) {
        if (!Object.prototype.hasOwnProperty.call(source, key)) continue;
        if (key === 'occurrence' || key === 'occurrenceBase') {
            target[key] = source[key];
        } else if (target[key] === undefined) {
            target[key] = source[key];
        }
    }
    return target;
}

function appendSearchMatchBatch(target, batch, keyIndex) {
    target = target || [];
    batch = batch || [];
    keyIndex = seedSearchMatchKeyIndex(target, keyIndex);
    var appended = 0;
    for (var i = 0; i < batch.length; i++) {
        var match = batch[i];
        var key = searchMatchStableKey(match);
        if (key && keyIndex[key] !== undefined) {
            mergeSearchMatchProperties(target[keyIndex[key]], match);
            continue;
        }
        if (key) keyIndex[key] = target.length;
        target.push(match);
        appended++;
    }
    return appended;
}

function completeChunkedSearchCollection(job) {
    if (searchRuntime().searchMatchCollectionJob === job) {
        searchRuntime().searchMatchCollectionJob = null;
    }
    if (!searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true, layout: true })) return;

    recordSearchTiming('match-collection', searchTimingNow() - job.startedAt, {
        mode: job.mode,
        scope: job.scope,
        chunked: true,
        batches: job.batches,
        matchCount: job.allMatches.length
    });

    var allMatches = rememberSearchResultMatches(
        job.query,
        job.scope,
        filterSearchMatchesForVisibleStages(filterUnavailableSearchMatches(job.allMatches))
    );
    job.onComplete({
        matches: filterSearchMatchesForMode(allMatches, job.mode),
        expandableMatchCount: allMatches.length,
        collapsedIndicators: measureSearchTiming('collapsed-indicator-collection', function() {
            return collectCollapsedSearchIndicatorsFromMatches(job.query, allMatches);
        }, {
            mode: job.mode,
            matchCount: allMatches.length,
            chunked: true
        })
    });
}

function ensureCommittedSearchResultIntent(query, scope, mode) {
    var search = searchRuntime();
    var intent = normalizeSearchIntent(query, scope, mode);
    if (search.searchResultQuery === intent.query &&
            search.searchResultScope === intent.scope &&
            search.searchResultMode === intent.mode &&
            search.searchResultEpoch) {
        return;
    }
    search.searchResultEpoch = search.activeSearchQueryEpoch || currentSearchTimingEpoch();
    search.searchResultQuery = intent.query;
    search.searchResultScope = intent.scope;
    search.searchResultMode = intent.mode;
    search.pendingSearchQuery = intent.query;
    search.pendingSearchScope = intent.scope;
    search.pendingSearchMode = intent.mode;
    search.searchResultStaleForNavigation = false;
}

function markSearchResultPartial(query, scope, mode, state) {
    ensureCommittedSearchResultIntent(query, scope, mode);
    searchRuntime().searchResultState = state || 'partial';
}

function shouldSchedulePartialSearchMatchCommit(job) {
    if (!job || !job.allMatches || !job.allMatches.length) return false;
    if (!job.lastPartialCommitAt) return true;
    if (job.batches - (job.lastPartialCommitBatch || 0) >= SEARCH_PARTIAL_COMMIT_BATCH_INTERVAL) {
        return true;
    }
    return searchTimingNow() - job.lastPartialCommitAt >= SEARCH_PARTIAL_COMMIT_MIN_INTERVAL_MS;
}

function schedulePartialSearchMatchCommit(job) {
    if (!job || job.cancelled) return false;
    if (!shouldSchedulePartialSearchMatchCommit(job)) return false;
    var batchCount = job.batches;
    var visibleMatches = filterSearchMatchesForMode(job.allMatches, job.mode);
    refreshSearchTransactionTokenLayoutGeneration(job.searchTransactionToken);
    job.streamedBatches = batchCount;
    job.streamedMatchCount = visibleMatches.length;
    job.lastPartialCommitAt = searchTimingNow();
    job.lastPartialCommitBatch = batchCount;

    scheduleRenderDomWork(
        'search-partial-match-results',
        function commitPartialSearchMatchBatch(visualCtx) {
            if (searchRuntime().searchMatchCollectionJob !== job ||
                    job.cancelled ||
                    batchCount !== job.streamedBatches ||
                    !searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true, layout: true })) {
                return;
            }

            var owner = createSearchTransactionResultOwner(
                job.searchTransactionToken,
                'search-partial-match-results',
                { currentness: { payload: true, layout: true } }
            );
            if (!owner) return;
            owner.applyResults({
                query: job.query,
                scope: job.scope,
                mode: job.mode,
                state: 'partial',
                matches: visibleMatches,
                preserveActive: true,
                skipNavigationLocationCache: true
            });
        },
        {
            token: searchTransactionFrameToken(job.searchTransactionToken),
            ownerId: job.searchTransactionToken && job.searchTransactionToken.ownerId || '',
            label: 'search-partial-match-results',
            surfaces: ['search-marks'],
            onDiscard: function() {}
        }
    );
    return true;
}

function scheduleSearchMatchCollectionChunk(job) {
    job.frameHandle = scheduleRenderModelWork(
        'search-match-collection',
        function runSearchMatchCollectionChunk(visualCtx) {
            job.frameHandle = null;
            if (searchRuntime().searchMatchCollectionJob !== job ||
                    job.cancelled ||
                    !searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true, layout: true })) {
                if (searchRuntime().searchMatchCollectionJob === job) {
                    searchRuntime().searchMatchCollectionJob = null;
                }
                return;
            }

            if (searchShouldYieldToViewportInteraction(job)) {
                recordSearchTiming('match-collection-yield', 0, {
                    mode: job.mode,
                    scope: job.scope,
                    viewportGeneration: currentTraceViewportInteractionGeneration()
                });
                scheduleSearchMatchCollectionChunk(job);
                return;
            }

            var batch = measureSearchTiming('match-collection-chunk', function() {
                return job.collector.next(SEARCH_MATCH_COLLECTION_STAGE_BATCH);
            }, {
                mode: job.mode,
                scope: job.scope,
                batch: job.batches + 1,
                stageIndex: job.collector.stageIndex,
                chunked: true
            });
            job.batches++;
            var appended = appendSearchMatchBatch(job.allMatches, batch.matches, job.matchKeys);
            job.duplicateMatches += Math.max(0, (batch.matches || []).length - appended);
            schedulePartialSearchMatchCommit(job);

            if (batch.done) {
                completeChunkedSearchCollection(job);
                return;
            }
            scheduleSearchMatchCollectionChunk(job);
        },
        {
            token: searchTransactionFrameToken(job.searchTransactionToken),
            ownerId: job.searchTransactionToken && job.searchTransactionToken.ownerId || '',
            label: 'search-match-collection',
            surfaces: ['search-marks', 'trace-canvas', 'trace-anchor-line'],
            onDiscard: function(reason) {
                if (searchRuntime().searchMatchCollectionJob === job) {
                    job.cancelled = true;
                    job.cancelReason = job.cancelReason || reason || 'discarded';
                    searchRuntime().searchMatchCollectionJob = null;
                }
            }
        }
    );
}

function scheduleChunkedSearchStateCollection(query, scope, mode, searchTransactionTokenValue, onComplete) {
    var index = searchRuntime().ruleSearchIndex;
    var collector = TraceSearch.createRecordMatchCollector(
        currentTraceStore(),
        index,
        query,
        normalizedSearchCollectionScope(scope)
    );
    if (!collector) return false;

    cancelSearchMatchCollectionJob('replaced');
    var job = {
        id: searchRuntime().nextSearchMatchCollectionJobId++,
        query: String(query || ''),
        scope: normalizedSearchCollectionScope(scope),
        mode: mode,
        searchTransactionToken: searchTransactionTokenValue,
        collector: collector,
        allMatches: [],
        matchKeys: {},
        batches: 0,
        duplicateMatches: 0,
        startedAt: searchTimingNow(),
        frameHandle: null,
        cancelled: false,
        cancelReason: '',
        streamedBatches: 0,
        streamedMatchCount: 0,
        lastPartialCommitAt: 0,
        lastPartialCommitBatch: 0,
        viewportYieldGeneration: 0,
        onComplete: onComplete
    };
    searchRuntime().searchMatchCollectionJob = job;
    scheduleSearchMatchCollectionChunk(job);
    return true;
}

function currentSearchResultReadiness(query, scope, mode) {
    if (!query) return 'cleared';
    if (searchRuntime().searchIndexProgress &&
        searchRuntime().searchIndexProgress.state === 'failed') {
        return 'failed';
    }
    return 'full';
}

function normalizeSearchModeValue(mode) {
    return 'find';
}

function normalizeSearchIntent(query, scope, mode) {
    return {
        query: String(query || '').trim(),
        scope: normalizeSearchScopeValue(scope || 'tree-rules'),
        mode: normalizeSearchModeValue(mode)
    };
}

function searchBaseLayoutCacheDirtySuppressed() {
    return (Number(searchRuntime().suppressSearchBaseLayoutCacheDirty) || 0) > 0;
}

function markSearchBaseLayoutCacheDirty(region) {
    if (!region || region.type === 'search-expansion') return;
    if (searchBaseLayoutCacheDirtySuppressed()) return;
    searchRuntime().searchBaseLayoutGeneration =
        (Number(searchRuntime().searchBaseLayoutGeneration) || 0) + 1;
}

function withSearchBaseLayoutCacheDirtySuppressed(callback) {
    searchRuntime().suppressSearchBaseLayoutCacheDirty =
        (Number(searchRuntime().suppressSearchBaseLayoutCacheDirty) || 0) + 1;
    try {
        return callback();
    } finally {
        searchRuntime().suppressSearchBaseLayoutCacheDirty = Math.max(
            0,
            (Number(searchRuntime().suppressSearchBaseLayoutCacheDirty) || 0) - 1
        );
    }
}

function currentSearchIndexCacheMode() {
    var search = searchRuntime();
    if (search.ruleSearchIndex && search.ruleSearchIndex.mode) {
        return String(search.ruleSearchIndex.mode);
    }
    if (search.searchIndexProgress && search.searchIndexProgress.mode) {
        return String(search.searchIndexProgress.mode);
    }
    return 'none';
}

function clearSearchResultCaches() {
    var search = searchRuntime();
    search.searchResultCache = {};
    search.searchResultCacheOrder = [];
    search.searchExpandOverlayCache = {};
    search.searchExpandOverlayCacheOrder = [];
    if (search.searchLayers && search.searchLayers.localExactCache) {
        search.searchLayers.localExactCache.entries = {};
        search.searchLayers.localExactCache.order = [];
    }
}

function ensureSearchCachesCurrent() {
    var search = searchRuntime();
    var traceGeneration = Number(currentTraceStore().traceGeneration) || 0;
    var payloadGeneration = currentSearchPayloadGeneration();
    var baseLayoutGeneration = Number(search.searchBaseLayoutGeneration) || 0;
    var indexMode = currentSearchIndexCacheMode();
    var emptyStagesVisible = showEmptyStages();

    if (search.searchCacheTraceGeneration !== traceGeneration ||
            search.searchCachePayloadGeneration !== payloadGeneration ||
            search.searchCacheBaseLayoutGeneration !== baseLayoutGeneration ||
            search.searchCacheIndexMode !== indexMode ||
            search.searchCacheShowEmptyStages !== emptyStagesVisible) {
        clearSearchResultCaches();
        search.searchCacheTraceGeneration = traceGeneration;
        search.searchCachePayloadGeneration = payloadGeneration;
        search.searchCacheBaseLayoutGeneration = baseLayoutGeneration;
        search.searchCacheIndexMode = indexMode;
        search.searchCacheShowEmptyStages = emptyStagesVisible;
    }
}

function normalizedSearchCacheQuery(query) {
    return String(query || '').trim().toLowerCase();
}

function searchResultCacheKey(query, scope) {
    ensureSearchCachesCurrent();
    return JSON.stringify([
        searchRuntime().searchCacheTraceGeneration,
        searchRuntime().searchCachePayloadGeneration,
        searchRuntime().searchCacheBaseLayoutGeneration,
        searchRuntime().searchCacheIndexMode,
        searchRuntime().searchCacheShowEmptyStages ? 1 : 0,
        normalizedSearchCacheQuery(query),
        normalizedSearchCollectionScope(scope)
    ]);
}

function cloneSearchCacheValue(value) {
    if (!value || typeof value !== 'object') return value;
    if (Array.isArray(value)) {
        var items = [];
        for (var i = 0; i < value.length; i++) {
            items.push(cloneSearchCacheValue(value[i]));
        }
        return items;
    }

    var clone = {};
    for (var key in value) {
        if (!Object.prototype.hasOwnProperty.call(value, key)) continue;
        clone[key] = cloneSearchCacheValue(value[key]);
    }
    return clone;
}

function cloneSearchMatchList(matches) {
    matches = matches || [];
    var clones = [];
    for (var i = 0; i < matches.length; i++) {
        clones.push(cloneSearchCacheValue(matches[i]));
    }
    return clones;
}

function setRestoredSearchIndicatorMatches(query, scope, matches) {
    query = String(query || '');
    scope = normalizedSearchCollectionScope(scope || 'tree-rules');
    searchRuntime().restoredSearchIndicatorMatches = {
        query: query,
        scope: scope,
        traceGeneration: Number(currentTraceStore().traceGeneration) || 0,
        matches: cloneSearchMatchList(matches)
    };
}

function restoredSearchIndicatorMatches(query, scope) {
    var restored = searchRuntime().restoredSearchIndicatorMatches;
    query = String(query || '');
    scope = normalizedSearchCollectionScope(scope || 'tree-rules');
    if (!restored ||
            restored.query !== query ||
            restored.scope !== scope ||
            Number(restored.traceGeneration) !== (Number(currentTraceStore().traceGeneration) || 0)) {
        return [];
    }
    return cloneSearchMatchList(restored.matches);
}

function rememberBoundedSearchCache(cache, order, key, value, limit) {
    if (!cache[key]) order.push(key);
    cache[key] = value;
    while (order.length > limit) {
        var evicted = order.shift();
        delete cache[evicted];
    }
}

function cachedSearchResultMatches(query, scope) {
    var search = searchRuntime();
    var key = searchResultCacheKey(query, scope);
    var entry = search.searchResultCache[key];
    if (!entry) {
        search.searchResultCacheMisses++;
        return null;
    }
    search.searchResultCacheHits++;
    return cloneSearchMatchList(entry.matches);
}

function rememberSearchResultMatches(query, scope, matches) {
    if (!String(query || '').trim()) return matches || [];
    if (searchScopeUsesBodyPayloads(scope)) return matches || [];
    var search = searchRuntime();
    var key = searchResultCacheKey(query, scope);
    rememberBoundedSearchCache(
        search.searchResultCache,
        search.searchResultCacheOrder,
        key,
        {
            resultEpoch: search.searchResultEpoch || search.activeSearchQueryEpoch || currentSearchTimingEpoch(),
            matches: cloneSearchMatchList(matches)
        },
        SEARCH_RESULT_CACHE_LIMIT
    );
    return matches || [];
}

function mergeUniqueSearchMatches() {
    var merged = [];
    var seen = {};
    var seenRendered = {};
    for (var a = 0; a < arguments.length; a++) {
        var list = arguments[a] || [];
        for (var i = 0; i < list.length; i++) {
            var match = list[i];
            var key = searchMatchStableKey(match);
            if (seen[key]) continue;
            var renderedKey = searchMatchRenderedMatchId(match);
            if (renderedKey && seenRendered[renderedKey]) continue;
            seen[key] = true;
            if (renderedKey) seenRendered[renderedKey] = true;
            merged.push(match);
        }
    }
    return merged;
}

function searchTotalRuleCount(store) {
    store = store || currentTraceStore();
    var total = 0;
    for (var si = 0; si < TraceStore.stageCount(store); si++) {
        total += TraceStore.stageRuleCount(store, si);
    }
    return total;
}

function searchRuleOrdinalForRawIndex(si, rawIdx) {
    var ordinal = Math.max(0, Math.floor(Number(rawIdx)) || 0);
    for (var stageIndex = 0; stageIndex < si; stageIndex++) {
        ordinal += TraceStore.stageRuleCount(currentTraceStore(), stageIndex);
    }
    return ordinal;
}

function lazySearchIndexKey(query, scope) {
    return JSON.stringify([
        Number(currentTraceStore().traceGeneration) || 0,
        normalizedSearchCacheQuery(query),
        normalizedSearchCollectionScope(scope)
    ]);
}

function createLazySearchIndex(query, scope) {
    return {
        key: lazySearchIndexKey(query, scope),
        query: String(query || ''),
        scope: normalizedSearchCollectionScope(scope),
        traceGeneration: Number(currentTraceStore().traceGeneration) || 0,
        ruleCount: searchTotalRuleCount(currentTraceStore()),
        entries: {},
        order: [],
        version: 0,
        complete: false
    };
}

function ensureLazySearchIndexCache(layers) {
    if (!layers) return null;
    if (!layers.lazyIndexCache) {
        layers.lazyIndexCache = {
            entries: {},
            order: [],
            hits: 0,
            misses: 0
        };
    }
    if (!layers.lazyIndexCache.entries) layers.lazyIndexCache.entries = {};
    if (!Array.isArray(layers.lazyIndexCache.order)) layers.lazyIndexCache.order = [];
    return layers.lazyIndexCache;
}

function touchLazySearchIndexCacheKey(cache, key) {
    if (!cache || !key) return;
    var order = cache.order || [];
    for (var i = order.length - 1; i >= 0; i--) {
        if (order[i] === key) order.splice(i, 1);
    }
    order.push(key);
    cache.order = order;
}

function rememberLazySearchIndex(layers, index) {
    if (!layers || !index || !index.key) return index;
    var cache = ensureLazySearchIndexCache(layers);
    if (!cache) return index;
    cache.entries[index.key] = index;
    touchLazySearchIndexCacheKey(cache, index.key);
    while (cache.order.length > SEARCH_LAZY_INDEX_CACHE_LIMIT) {
        var evicted = cache.order.shift();
        if (!evicted) continue;
        if (layers.lazyIndex && layers.lazyIndex.key === evicted) {
            touchLazySearchIndexCacheKey(cache, evicted);
            continue;
        }
        delete cache.entries[evicted];
    }
    return index;
}

function cachedLazySearchIndex(layers, key) {
    var cache = ensureLazySearchIndexCache(layers);
    if (!cache || !key) return null;
    var index = cache.entries[key] || null;
    if (!index) {
        cache.misses = (Number(cache.misses) || 0) + 1;
        return null;
    }
    cache.hits = (Number(cache.hits) || 0) + 1;
    touchLazySearchIndexCacheKey(cache, key);
    return index;
}

function lazySearchIndexFor(query, scope, create) {
    var layers = searchRuntime().searchLayers;
    if (!layers) return null;
    var key = lazySearchIndexKey(query, scope);
    var index = layers.lazyIndex || null;
    if (index && index.key === key) {
        rememberLazySearchIndex(layers, index);
        return index;
    }
    index = cachedLazySearchIndex(layers, key);
    if (index) {
        layers.lazyIndex = index;
        return index;
    }
    if (!index || index.key !== key) {
        if (!create) return null;
        index = createLazySearchIndex(query, scope);
        layers.lazyIndex = index;
        rememberLazySearchIndex(layers, index);
    }
    return index;
}

function lazySearchEntry(index, ordinal, create) {
    if (!index) return null;
    ordinal = Math.max(0, Math.floor(Number(ordinal)) || 0);
    var key = String(ordinal);
    var entry = index.entries[key];
    if (!entry && create) {
        entry = {
            ordinal: ordinal,
            state: 'unscanned',
            count: 0,
            mask: 0,
            ruleHandle: '',
            exactMatches: null,
            lowerBoundMatches: null
        };
        index.entries[key] = entry;
        index.order.push(ordinal);
        index.order.sort(function(a, b) { return a - b; });
    }
    return entry || null;
}

function lazySearchMaskForMatches(matches) {
    var mask = 0;
    matches = matches || [];
    for (var i = 0; i < matches.length; i++) {
        var match = matches[i] || {};
        if (match.field === 'name') {
            mask |= searchSummaryMaskValue('title');
        } else if (match.field === 'label') {
            if (match.type === 'rule' && traceRuleType(match.si, match.rawIdx) === 'text') {
                mask |= searchSummaryMaskValue('text');
            } else {
                mask |= searchSummaryMaskValue('tree');
            }
        }
    }
    return mask;
}

function mergeLazySearchExactMatches(query, scope, ref, matches) {
    if (!ref) return null;
    var index = lazySearchIndexFor(query, scope, true);
    if (!index) return null;
    var rawIdx = ref.rawIdx === undefined || ref.rawIdx === null
        ? rawRuleIndex(ref.si, ref.gi, ref.ri)
        : ref.rawIdx;
    var ordinal = searchRuleOrdinalForRawIndex(ref.si, rawIdx);
    var entry = lazySearchEntry(index, ordinal, true);
    var cloned = cloneSearchMatchList(matches || []);
    entry.state = 'scanned';
    entry.si = ref.si;
    entry.gi = ref.gi;
    entry.ri = ref.ri;
    entry.rawIdx = rawIdx;
    entry.ruleHandle = TraceStore.ruleHandle(currentTraceStore(), ref.si, rawIdx) || '';
    entry.count = cloned.length;
    entry.mask = lazySearchMaskForMatches(cloned);
    entry.exactMatches = cloned;
    entry.lowerBoundMatches = null;
    index.version++;
    markLazySearchIndexCompleteIfScanned(index);
    return entry;
}

function mergeLazySearchLowerBoundMatches(query, scope, ref, matches) {
    if (!ref) return null;
    matches = matches || [];
    if (!matches.length) return null;

    var index = lazySearchIndexFor(query, scope, true);
    if (!index) return null;
    var rawIdx = ref.rawIdx === undefined || ref.rawIdx === null
        ? rawRuleIndex(ref.si, ref.gi, ref.ri)
        : ref.rawIdx;
    var ordinal = searchRuleOrdinalForRawIndex(ref.si, rawIdx);
    var entry = lazySearchEntry(index, ordinal, true);
    if (entry.state === 'scanned') return entry;

    var cloned = cloneSearchMatchList(matches);
    entry.state = 'partial';
    entry.si = ref.si;
    entry.gi = ref.gi;
    entry.ri = ref.ri;
    entry.rawIdx = rawIdx;
    entry.ruleHandle = TraceStore.ruleHandle(currentTraceStore(), ref.si, rawIdx) || '';
    entry.count = Math.max(normalizeSearchCount(entry.count), cloned.length);
    entry.mask |= lazySearchMaskForMatches(cloned);
    entry.lowerBoundMatches = cloned;
    index.version++;
    return entry;
}

function markLazySearchIndexCompleteIfScanned(index) {
    if (!index || index.complete || index.ruleCount <= 0) return false;
    if (!index.order || index.order.length < index.ruleCount) return false;
    for (var ordinal = 0; ordinal < index.ruleCount; ordinal++) {
        var entry = index.entries && index.entries[String(ordinal)];
        if (!entry || entry.state !== 'scanned') return false;
    }
    index.complete = true;
    index.version++;
    return true;
}

function mergeLazySearchSummary(query, scope, summary, options) {
    options = options || {};
    summary = cloneSearchGlobalSummary(summary);
    var index = lazySearchIndexFor(query, scope, true);
    if (!index || !summary) return null;
    var seen = {};
    for (var i = 0; i < summary.counts.length; i++) {
        var ref = searchRuleRefForGlobalSummaryRow(summary, i);
        if (!ref) continue;
        var ordinal = searchRuleOrdinalForRawIndex(ref.si, ref.rawIdx);
        seen[String(ordinal)] = true;
        var entry = lazySearchEntry(index, ordinal, true);
        entry.state = 'scanned';
        entry.si = ref.si;
        entry.gi = ref.gi;
        entry.ri = ref.ri;
        entry.rawIdx = ref.rawIdx;
        entry.ruleHandle = summary.ruleHandles[i] || TraceStore.ruleHandle(currentTraceStore(), ref.si, ref.rawIdx) || '';
        entry.count = normalizeSearchCount(summary.counts[i]);
        entry.mask = Number(summary.masks && summary.masks[i]) || 0;
        entry.lowerBoundMatches = null;
        if (entry.exactMatches && entry.exactMatches.length !== entry.count) {
            entry.exactMatches = null;
        }
    }
    if (options.complete) {
        for (var ordinal = 0; ordinal < index.ruleCount; ordinal++) {
            if (seen[String(ordinal)]) continue;
            var zeroEntry = lazySearchEntry(index, ordinal, true);
            var zeroRef = searchRuleRefForOrdinal(ordinal);
            zeroEntry.state = 'scanned';
            zeroEntry.si = zeroRef && zeroRef.si;
            zeroEntry.gi = zeroRef && zeroRef.gi;
            zeroEntry.ri = zeroRef && zeroRef.ri;
            zeroEntry.rawIdx = zeroRef && zeroRef.rawIdx;
            zeroEntry.ruleHandle = zeroRef ? TraceStore.ruleHandle(currentTraceStore(), zeroRef.si, zeroRef.rawIdx) || '' : '';
            zeroEntry.count = 0;
            zeroEntry.mask = 0;
            zeroEntry.exactMatches = [];
            zeroEntry.lowerBoundMatches = null;
        }
        index.complete = true;
    }
    index.version++;
    return index;
}

function lazySearchKnownMatches(query, scope) {
    var index = lazySearchIndexFor(query, scope, false);
    if (!index) return [];
    var matches = [];
    var ordinals = index.order.slice().sort(function(a, b) { return a - b; });
    for (var i = 0; i < ordinals.length; i++) {
        var entry = index.entries[String(ordinals[i])];
        if (!entry || entry.state !== 'scanned' || !entry.exactMatches) continue;
        for (var m = 0; m < entry.exactMatches.length; m++) {
            matches.push(cloneSearchCacheValue(entry.exactMatches[m]));
        }
    }
    return matches;
}

function lazySearchKnownCount(query, scope) {
    var index = lazySearchIndexFor(query, scope, false);
    if (!index) return 0;
    var total = 0;
    for (var key in index.entries) {
        if (!Object.prototype.hasOwnProperty.call(index.entries, key)) continue;
        var entry = index.entries[key];
        if (entry && (entry.state === 'scanned' || entry.state === 'partial')) {
            total += normalizeSearchCount(entry.count);
        }
    }
    return total;
}

function lazySearchKnownOrdinalForMatch(query, scope, match) {
    var index = lazySearchIndexFor(query, scope, false);
    if (!index || !match || match.type !== 'rule') return 0;
    var ordinal = searchMatchRuleOrdinal(match);
    if (ordinal < 0) return 0;
    var total = 0;
    var ordinals = index.order.slice().sort(function(a, b) { return a - b; });
    for (var i = 0; i < ordinals.length; i++) {
        var entry = index.entries[String(ordinals[i])];
        if (!entry || (entry.state !== 'scanned' && entry.state !== 'partial')) continue;
        if (ordinals[i] < ordinal) {
            total += normalizeSearchCount(entry.count);
            continue;
        }
        if (ordinals[i] !== ordinal) continue;
        var localIndex = 0;
        var exact = entry.state === 'scanned'
            ? (entry.exactMatches || [])
            : (entry.lowerBoundMatches || []);
        var targetKey = searchMatchStableKey(match);
        for (var m = 0; m < exact.length; m++) {
            if (searchMatchStableKey(exact[m]) === targetKey) {
                localIndex = m;
                break;
            }
        }
        return total + localIndex + 1;
    }
    return 0;
}

function lazySearchIndexComplete(query, scope) {
    var index = lazySearchIndexFor(query, scope, false);
    if (!index || !index.complete) return false;
    return index.order.length >= index.ruleCount;
}

function lazySearchRuleScanned(query, scope, ordinal) {
    var index = lazySearchIndexFor(query, scope, false);
    var entry = lazySearchEntry(index, ordinal, false);
    return !!(entry && entry.state === 'scanned');
}

function lazySearchFirstUnscannedOrdinal(query, scope, startOrdinal, direction) {
    var index = lazySearchIndexFor(query, scope, true);
    if (!index || !index.ruleCount) return -1;
    direction = direction < 0 ? -1 : 1;
    var ordinal = Math.max(0, Math.min(index.ruleCount - 1, Math.floor(Number(startOrdinal)) || 0));
    for (var visited = 0; visited < index.ruleCount; visited++) {
        if (!lazySearchRuleScanned(query, scope, ordinal)) return ordinal;
        ordinal = (ordinal + direction + index.ruleCount) % index.ruleCount;
    }
    index.complete = true;
    return -1;
}

function searchGlobalSummaryCacheKey(query, scope) {
    return JSON.stringify([
        Number(currentTraceStore().traceGeneration) || 0,
        normalizedSearchCacheQuery(query),
        normalizedSearchCollectionScope(scope)
    ]);
}

function cloneSearchGlobalSummary(summary) {
    summary = summary || {};
    return {
        query: String(summary.query || ''),
        scope: normalizedSearchCollectionScope(summary.scope || 'tree-rules'),
        totalCount: normalizeSearchCount(summary.totalCount),
        titleMatches: Array.isArray(summary.titleMatches)
            ? summary.titleMatches.map(function(match) {
                match = match || {};
                return {
                    type: String(match.type || ''),
                    si: Math.max(0, Math.floor(Number(match.si)) || 0),
                    gi: match.gi === undefined || match.gi === null
                        ? undefined
                        : Math.max(0, Math.floor(Number(match.gi)) || 0),
                    count: normalizeSearchCount(match.count),
                    order: Math.max(0, Math.floor(Number(match.order)) || 0)
                };
            })
            : [],
        ruleOrdinals: Array.isArray(summary.ruleOrdinals) ? summary.ruleOrdinals.slice() : [],
        ruleHandles: Array.isArray(summary.ruleHandles) ? summary.ruleHandles.slice() : [],
        counts: Array.isArray(summary.counts) ? summary.counts.slice() : [],
        orders: Array.isArray(summary.orders) ? summary.orders.slice() : [],
        masks: Array.isArray(summary.masks) ? summary.masks.slice() : []
    };
}

function rememberGlobalSearchSummary(query, scope, summary) {
    var layers = searchRuntime().searchLayers;
    if (!layers || !layers.globalSummaryCache) return summary || null;
    var key = searchGlobalSummaryCacheKey(query, scope);
    var normalized = cloneSearchGlobalSummary(summary);
    rememberBoundedSearchCache(
        layers.globalSummaryCache.entries,
        layers.globalSummaryCache.order,
        key,
        normalized,
        SEARCH_GLOBAL_SUMMARY_CACHE_LIMIT
    );
    return normalized;
}

function cachedGlobalSearchSummary(query, scope) {
    var layers = searchRuntime().searchLayers;
    if (!layers || !layers.globalSummaryCache) return null;
    var key = searchGlobalSummaryCacheKey(query, scope);
    var summary = layers.globalSummaryCache.entries[key];
    if (!summary) {
        layers.globalSummaryCache.misses++;
        return null;
    }
    layers.globalSummaryCache.hits++;
    return cloneSearchGlobalSummary(summary);
}

function peekGlobalSearchSummary(query, scope) {
    var layers = searchRuntime().searchLayers;
    if (!layers || !layers.globalSummaryCache) return null;
    var key = searchGlobalSummaryCacheKey(query, scope);
    var summary = layers.globalSummaryCache.entries[key];
    return summary ? cloneSearchGlobalSummary(summary) : null;
}

function cancelGlobalSearchSummaryJob(reason) {
    var layers = searchRuntime().searchLayers;
    var globalJob = layers && layers.globalJob;
    if (!globalJob || !globalJob.activeJob) return false;
    var active = globalJob.activeJob;
    globalJob.activeJob = null;
    active.cancelReason = reason || 'cancelled';
    if (active.workerJob && typeof active.workerJob.cancel === 'function') {
        active.workerJob.cancel();
    }
    return true;
}

function globalSearchJobKind() {
    var active = activeGlobalSearchSummaryJob();
    return active && active.kind || '';
}

function activeGlobalSearchSummaryJobCurrent(active) {
    var layers = searchRuntime().searchLayers;
    return !!active &&
        !!layers &&
        !!layers.globalJob &&
        layers.globalJob.activeJob === active &&
        runtimeTokenCurrent(active.runtimeToken) &&
        Number(active.traceGeneration) === Number(currentTraceStore().traceGeneration);
}

function estimateSearchTreeNodeCount(node, limit) {
    if (!node) return 0;
    var count = 0;
    var stack = [node];
    while (stack.length) {
        var current = stack.pop();
        if (!current) continue;
        count++;
        if (count > limit) return count;
        var children = current.c || [];
        for (var i = 0; i < children.length; i++) stack.push(children[i]);
    }
    return count;
}

function shouldUseInlineGlobalSummaryJob(store, scope) {
    if (!searchScopeUsesBodyPayloads(scope)) return false;
    var limit = SEARCH_INLINE_GLOBAL_SUMMARY_NODE_THRESHOLD;
    var total = 0;
    for (var si = 0; si < TraceStore.stageCount(store); si++) {
        for (var rawIdx = 0; rawIdx < TraceStore.stageRuleCount(store, si); rawIdx++) {
            if (TraceStore.ruleType(store, si, rawIdx) === 'text') continue;
            total += estimateSearchTreeNodeCount(TraceStore.planTree(store, si, rawIdx), limit - total);
            if (total > limit) return true;
        }
    }
    return false;
}

function buildInlineGlobalSummaryJob(store, query, scope) {
    var cancelled = false;
    var timer = null;
    var traceGeneration = Number(store && store.traceGeneration) || 0;
    var job = {
        traceGeneration: traceGeneration,
        cancel: function() {
            cancelled = true;
            if (timer !== null) {
                clearTimeout(timer);
                timer = null;
            }
        },
        promise: null
    };

    job.promise = new Promise(function(resolve, reject) {
        timer = setTimeout(function() {
            timer = null;
            if (cancelled) return;
            try {
                resolve(TraceSearch.buildGlobalSummary(
                    store,
                    query,
                    scope,
                    TraceSearchWorker.SUMMARY_FIELD_MASKS
                ));
            } catch (err) {
                reject(err);
            }
        }, 0);
    });

    return job;
}

function startGlobalSearchSummaryJob(query, scope, options) {
    options = options || {};
    query = String(query || '');
    scope = normalizedSearchCollectionScope(scope || 'tree-rules');

    var cached = options.ignoreCache ? null : cachedGlobalSearchSummary(query, scope);
    if (cached) {
        mergeLazySearchSummary(query, scope, cached, { complete: true });
        return {
            cached: true,
            query: query,
            scope: scope,
            promise: Promise.resolve(cached),
            cancel: function() {}
        };
    }

    var layers = searchRuntime().searchLayers;
    if (!layers || !layers.globalJob) return null;
    cancelGlobalSearchSummaryJob('replaced');

    var store = currentTraceStore();
    var workerJob = shouldUseInlineGlobalSummaryJob(store, scope)
        ? buildInlineGlobalSummaryJob(store, query, scope)
        : TraceSearchWorker.buildGlobalSummary(store, query, scope, {
            traceGeneration: store && store.traceGeneration
        });
    if (!workerJob) return null;

    var active = {
        id: layers.globalJob.nextJobId++,
        kind: options.kind || 'global-summary',
        query: query,
        scope: scope,
        traceGeneration: Number(store && store.traceGeneration) || 0,
        payloadGeneration: currentSearchPayloadGeneration(),
        runtimeToken: runtimeToken(),
        workerJob: workerJob,
        cancelReason: ''
    };
    layers.globalJob.activeJob = active;

    var promise = workerJob.promise.then(function(summary) {
        if (!activeGlobalSearchSummaryJobCurrent(active)) {
            if (layers.globalJob.activeJob === active) layers.globalJob.activeJob = null;
            return null;
        }
        layers.globalJob.activeJob = null;
        mergeLazySearchSummary(query, scope, summary, { complete: true });
        return rememberGlobalSearchSummary(query, scope, summary);
    }, function(error) {
        if (layers.globalJob.activeJob === active) {
            layers.globalJob.activeJob = null;
        }
        throw error;
    });

    return {
        cached: false,
        activeJob: active,
        query: query,
        scope: scope,
        promise: promise,
        cancel: function(reason) {
            if (layers.globalJob.activeJob !== active) return false;
            return cancelGlobalSearchSummaryJob(reason || 'cancelled');
        }
    };
}

function scanLazySearchRuleOrdinal(query, scope, ordinal) {
    var ref = searchRuleRefForOrdinal(ordinal);
    if (!ref) return null;
    var ruleHandleValue = TraceStore.ruleHandle(currentTraceStore(), ref.si, ref.rawIdx);
    if (ruleHandleValue) TraceStore.ensureRulePayload(currentTraceStore(), ruleHandleValue);
    var matches = scanRuleExactSearchMatches(query, scope, ref.si, ref.gi, ref.ri, ref.rawIdx);
    return mergeLazySearchExactMatches(query, scope, ref, matches);
}

function startLazyFindNextSearchJob(query, scope, direction, startOrdinal) {
    query = String(query || '');
    scope = normalizedSearchCollectionScope(scope || 'tree-rules');
    direction = direction < 0 ? -1 : 1;
    var index = lazySearchIndexFor(query, scope, true);
    if (!query || !index || !index.ruleCount) return null;

    var layers = searchRuntime().searchLayers;
    if (!layers || !layers.globalJob) return null;
    cancelGlobalSearchSummaryJob('replaced');

    var active = {
        id: layers.globalJob.nextJobId++,
        kind: 'find-next',
        query: query,
        scope: scope,
        direction: direction,
        traceGeneration: Number(currentTraceStore().traceGeneration) || 0,
        payloadGeneration: currentSearchPayloadGeneration(),
        runtimeToken: runtimeToken(),
        workerJob: null,
        cancelReason: '',
        timer: null
    };
    layers.globalJob.activeJob = active;

    var ordinal = lazySearchFirstUnscannedOrdinal(query, scope, startOrdinal, direction);
    if (ordinal < 0) {
        index.complete = true;
        layers.globalJob.activeJob = null;
        return {
            cached: true,
            activeJob: null,
            promise: Promise.resolve({ found: false, complete: true }),
            cancel: function() { return false; }
        };
    }

    var scanned = 0;
    var blockedByPendingPayload = false;
    var promise = new Promise(function(resolve) {
        function finish(result) {
            if (active.timer !== null) {
                clearTimeout(active.timer);
                active.timer = null;
            }
            if (layers.globalJob.activeJob === active) layers.globalJob.activeJob = null;
            resolve(result || null);
        }

        active.workerJob = {
            cancel: function() {
                active.cancelReason = active.cancelReason || 'cancelled';
                if (active.timer !== null) {
                    clearTimeout(active.timer);
                    active.timer = null;
                }
                finish(null);
            }
        };

        function step() {
            active.timer = null;
            if (!activeGlobalSearchSummaryJobCurrent(active)) return finish(null);
            for (var i = 0; i < SEARCH_FIND_NEXT_RULE_BATCH_SIZE && scanned < index.ruleCount; i++) {
                if (!activeGlobalSearchSummaryJobCurrent(active)) return finish(null);
                if (!lazySearchRuleScanned(query, scope, ordinal)) {
                    var entry = scanLazySearchRuleOrdinal(query, scope, ordinal);
                    if (!entry) {
                        blockedByPendingPayload = true;
                        scanned++;
                        ordinal = (ordinal + direction + index.ruleCount) % index.ruleCount;
                        continue;
                    }
                    if (entry && normalizeSearchCount(entry.count) > 0) {
                        return finish({
                            found: true,
                            entry: entry,
                            ordinal: ordinal,
                            direction: direction
                        });
                    }
                }
                scanned++;
                ordinal = (ordinal + direction + index.ruleCount) % index.ruleCount;
            }
            if (scanned >= index.ruleCount) {
                if (!blockedByPendingPayload) index.complete = true;
                return finish({ found: false, complete: !blockedByPendingPayload });
            }
            active.timer = setTimeout(step, 0);
        }

        active.timer = setTimeout(step, 0);
    });

    return {
        cached: false,
        activeJob: active,
        query: query,
        scope: scope,
        promise: promise,
        cancel: function(reason) {
            if (layers.globalJob.activeJob !== active) return false;
            return cancelGlobalSearchSummaryJob(reason || 'cancelled');
        }
    };
}

function currentSearchIntentMatches(query, scope) {
    var intent = currentSearchInputIntent();
    return !!intent.query &&
        intent.query === String(query || '') &&
        intent.scope === normalizedSearchCollectionScope(scope || 'tree-rules');
}

function promoteCompletedLazySearchIfCurrent(query, scope) {
    query = String(query || '');
    scope = normalizedSearchCollectionScope(scope || 'tree-rules');
    if (!lazySearchIndexComplete(query, scope)) return false;
    if (!currentSearchIntentMatches(query, scope)) return false;

    var mode = currentSearchMode();
    rebuildTraceLayoutModel();
    var state = collectLazySearchState(query, scope, mode);
    replaceSearchMatches(state.matches, true, { deferStatus: true });
    searchRuntime().searchResultQuery = query;
    searchRuntime().searchResultScope = scope;
    searchRuntime().searchResultMode = mode;
    searchRuntime().searchResultState = 'ready';
    searchRuntime().searchExpandableMatchCount = state.expandableMatchCount;
    scheduleSearchDecoration(query, scope, {
        collapsedIndicators: state.collapsedIndicators,
        searchMatches: state.matches,
        searchTransactionToken: activeSearchTransactionToken(),
        updateStatus: true
    });
    updateSearchStatus(state.matches.length);
    return true;
}

function applyGlobalSummaryToCurrentSearch(query, scope, summary) {
    query = String(query || '');
    scope = normalizedSearchCollectionScope(scope || 'tree-rules');
    if (!summary || !currentSearchIntentMatches(query, scope)) return false;

    var mode = currentSearchMode();
    var state = collectGlobalSummarySearchState(query, scope, mode, summary);
    replaceSearchMatches(state.matches, true, { deferStatus: true });
    markSearchResultReady(query, scope, mode);
    searchRuntime().searchExpandableMatchCount = state.expandableMatchCount;
    scheduleSearchDecoration(query, scope, {
        collapsedIndicators: state.collapsedIndicators,
        searchMatches: state.matches,
        allowWithoutSearchTransaction: true,
        updateStatus: true
    });
    updateSearchStatus(state.matches.length);
    return true;
}

function cancelVisibleSearchScanJob(reason) {
    var search = searchRuntime();
    var job = search.visibleSearchScanJob;
    if (!job) return false;
    job.cancelled = true;
    job.cancelReason = reason || 'cancelled';
    if (search.visibleSearchScanFrame !== null) {
        cancelRenderFrameWork(search.visibleSearchScanFrame);
    }
    search.visibleSearchScanFrame = null;
    if (search.visibleSearchScanJob === job) search.visibleSearchScanJob = null;
    return true;
}

function visibleSearchScanJobCurrent(job) {
    if (!job || job.cancelled || searchRuntime().visibleSearchScanJob !== job) return false;
    if (!runtimeTokenCurrent(job.runtimeToken)) return false;
    if (Number(job.traceGeneration) !== Number(currentTraceStore().traceGeneration)) return false;
    if (Number(job.payloadGeneration) !== Number(currentSearchPayloadGeneration())) return false;
    if (!currentSearchIntentMatches(job.query, job.scope)) return false;
    if (!searchScopeUsesBodyPayloads(job.scope)) return false;
    if (lazySearchIndexComplete(job.query, job.scope)) return false;
    return true;
}

function finishVisibleSearchScanJob(job) {
    var search = searchRuntime();
    if (search.visibleSearchScanJob === job) search.visibleSearchScanJob = null;
    if (search.visibleSearchScanFrame === job.frameHandle) search.visibleSearchScanFrame = null;
    job.frameHandle = null;
}

function scheduleVisibleSearchScanFrame(job) {
    if (!job || job.cancelled || job.frameHandle !== null) return;
    job.frameHandle = scheduleRenderDeferredWork(
        'visible-search-scan',
        function runVisibleSearchScanFrame() {
            searchRuntime().visibleSearchScanFrame = null;
            job.frameHandle = null;
            runVisibleSearchScanChunk(job);
        },
        {
            epochScopes: ['trace', 'search', 'layout'],
            label: 'visible-search-scan',
            surfaces: ['search-marks', 'rule-tree', 'rule-info'],
            onDiscard: function() {
                if (searchRuntime().visibleSearchScanJob === job) {
                    searchRuntime().visibleSearchScanJob = null;
                }
                if (searchRuntime().visibleSearchScanFrame === job.frameHandle) {
                    searchRuntime().visibleSearchScanFrame = null;
                }
                job.frameHandle = null;
            }
        }
    );
    searchRuntime().visibleSearchScanFrame = job.frameHandle;
}

function scheduleVisibleSearchScannedMatchDecoration(query, scope, matches) {
    matches = matches || [];
    var labelMatches = [];
    for (var i = 0; i < matches.length; i++) {
        if (matches[i] && matches[i].type === 'rule' && matches[i].field === 'label') {
            labelMatches.push(matches[i]);
        }
    }
    if (!labelMatches.length) return false;

    scheduleSearchLayoutDecoration(query, scope, labelMatches);
    return true;
}

function runVisibleSearchScanChunk(job) {
    if (!visibleSearchScanJobCurrent(job)) return finishVisibleSearchScanJob(job);

    if (!job.refs) {
        job.refs = collectVisibleSearchRuleRefs();
        job.index = 0;
    }

    var scannedAny = false;
    var scannedMatches = [];
    var scanned = 0;
    while (job.index < job.refs.length && scanned < SEARCH_VISIBLE_SCAN_RULE_BATCH_SIZE) {
        var ref = job.refs[job.index++];
        if (ref && !lazySearchRuleScanned(job.query, job.scope, ref.ordinal)) {
            var matches = scanRuleExactSearchMatches(job.query, job.scope, ref.si, ref.gi, ref.ri, ref.rawIdx);
            if (matches) {
                scannedAny = true;
                scanned++;
                for (var m = 0; m < matches.length; m++) scannedMatches.push(matches[m]);
            }
        }
    }

    if (promoteCompletedLazySearchIfCurrent(job.query, job.scope)) {
        return finishVisibleSearchScanJob(job);
    }

    if (scannedAny && currentSearchIntentMatches(job.query, job.scope)) {
        searchRuntime().searchExpandableMatchCount = Math.max(
            normalizeSearchCount(searchRuntime().searchExpandableMatchCount),
            lazySearchKnownCount(job.query, job.scope)
        );
        refreshCollapsedSearchIndicatorsForKnownMatches(job.query, job.scope);
        scheduleVisibleSearchScannedMatchDecoration(job.query, job.scope, scannedMatches);
        updateSearchStatus(searchRuntime().searchMatches.length);
    }

    if (job.index < job.refs.length) {
        return scheduleVisibleSearchScanFrame(job);
    }

    finishVisibleSearchScanJob(job);
}

function scheduleVisibleSearchWarmup(reason) {
    if (!currentUiState().searchActive) return false;
    var intent = currentSearchInputIntent();
    if (!intent.query || !searchScopeUsesBodyPayloads(intent.scope)) return false;
    if (peekGlobalSearchSummary(intent.query, intent.scope)) return false;
    if (promoteCompletedLazySearchIfCurrent(intent.query, intent.scope)) return true;
    if (lazySearchIndexComplete(intent.query, intent.scope)) return false;

    var search = searchRuntime();
    var existing = search.visibleSearchScanJob;
    if (existing) {
        if (existing.query === intent.query &&
                existing.scope === intent.scope &&
                visibleSearchScanJobCurrent(existing)) {
            return true;
        }
        cancelVisibleSearchScanJob(reason || 'replaced');
    }

    var job = {
        query: intent.query,
        scope: intent.scope,
        traceGeneration: Number(currentTraceStore().traceGeneration) || 0,
        payloadGeneration: currentSearchPayloadGeneration(),
        runtimeToken: runtimeToken(),
        refs: null,
        index: 0,
        frameHandle: null,
        cancelled: false,
        cancelReason: ''
    };
    search.visibleSearchScanJob = job;
    scheduleVisibleSearchScanFrame(job);
    return true;
}

function searchIntentsEqual(a, b) {
    return !!a && !!b &&
        a.query === b.query &&
        a.scope === b.scope &&
        a.mode === b.mode;
}

function currentSearchInputQuery() {
    if (hasDOM() && document.getElementById) {
        var box = document.getElementById('search-box');
        if (box) return String(box.value || '').trim();
    }
    return currentSearchQuery();
}

function currentSearchInputIntent() {
    return normalizeSearchIntent(
        currentSearchInputQuery(),
        currentSearchScope(),
        currentSearchMode()
    );
}

function committedSearchResultIntent() {
    var search = searchRuntime();
    return normalizeSearchIntent(
        search.searchResultQuery || '',
        search.searchResultScope || 'tree-rules',
        search.searchResultMode || 'find'
    );
}

function searchResultMatchesIntent(query, scope, mode) {
    return searchIntentsEqual(
        committedSearchResultIntent(),
        normalizeSearchIntent(query, scope, mode)
    );
}

function markPendingSearchIntent(query, scope, mode) {
    var search = searchRuntime();
    var intent = normalizeSearchIntent(query, scope, mode);
    search.pendingSearchQuery = intent.query;
    search.pendingSearchScope = intent.scope;
    search.pendingSearchMode = intent.mode;
    search.searchResultStaleForNavigation = !!intent.query &&
        search.searchMatches.length > 0 &&
        !searchResultMatchesIntent(intent.query, intent.scope, intent.mode);
}

function markPendingSearchIntentFromControls() {
    var intent = currentSearchInputIntent();
    markPendingSearchIntent(intent.query, intent.scope, intent.mode);
    return intent;
}

function markSearchResultReady(query, scope, mode) {
    var intent = normalizeSearchIntent(query, scope, mode);
    ensureCommittedSearchResultIntent(intent.query, intent.scope, intent.mode);
    searchRuntime().searchResultState = currentSearchResultReadiness(intent.query, intent.scope, intent.mode);
}

function clearCommittedSearchResultIntent() {
    var search = searchRuntime();
    search.searchResultQuery = '';
    search.searchResultScope = 'tree-rules';
    search.searchResultMode = 'find';
    search.pendingSearchQuery = '';
    search.pendingSearchScope = 'tree-rules';
    search.pendingSearchMode = 'find';
    search.searchResultStaleForNavigation = false;
    search.searchExpandableMatchCount = 0;
}

function currentSearchResultReadyForNavigation() {
    var intent = currentSearchInputIntent();
    if (!intent.query) return false;
    var resultState = searchRuntime().searchResultState;
    if (resultState === 'partial') return false;
    return !searchRuntime().searchResultStaleForNavigation &&
        searchResultMatchesIntent(intent.query, intent.scope, intent.mode);
}

function recordSearchTimingForEpoch(epoch, label, elapsedMs, details) {
    var run = searchRuntime().searchTimingCurrent;
    if (!run || (epoch && run.epoch !== epoch)) return;
    if (!label) return;

    var durationMs = roundedSearchTiming(elapsedMs);
    var bucket = run.totals[label];
    if (!bucket) {
        bucket = {
            count: 0,
            totalMs: 0,
            maxMs: 0,
            lastMs: 0
        };
        run.totals[label] = bucket;
    }

    bucket.count++;
    bucket.totalMs = roundedSearchTiming(bucket.totalMs + durationMs);
    bucket.maxMs = Math.max(bucket.maxMs, durationMs);
    bucket.lastMs = durationMs;
    run.updatedAt = roundedSearchTiming(searchTimingNow());

    var entry = {
        label: label,
        durationMs: durationMs,
        atMs: roundedSearchTiming(run.updatedAt - run.startedAt)
    };
    if (details) entry.details = details;
    run.entries.push(entry);
    if (run.entries.length > SEARCH_TIMING_ENTRY_LIMIT) {
        run.entries.splice(0, run.entries.length - SEARCH_TIMING_ENTRY_LIMIT);
    }
}

function recordSearchTiming(label, elapsedMs, details) {
    recordSearchTimingForEpoch(currentSearchTimingEpoch(), label, elapsedMs, details);
}

function measureSearchTimingForEpoch(epoch, label, fn, details) {
    if (typeof fn !== 'function') return undefined;
    var startedAt = searchTimingNow();
    try {
        return fn();
    } finally {
        recordSearchTimingForEpoch(epoch, label, searchTimingNow() - startedAt, details);
    }
}

function measureSearchTiming(label, fn, details) {
    return measureSearchTimingForEpoch(currentSearchTimingEpoch(), label, fn, details);
}

function withSearchMarksTransition(label, details, fn) {
    if (typeof fn !== 'function') return false;

    var search = searchRuntime();
    if (search.searchMarksTransitionOwnerId) return fn();

    var owner = createVisualSurfaceOwner(
        'search',
        label || 'search-marks-transition',
        ['search-marks'],
        details
    );
    if (!acquireVisualSurfaceOwners(owner.surfaces, owner)) return false;

    search.searchMarksTransitionOwnerId = owner.id;
    try {
        return fn(owner);
    } finally {
        search.searchMarksTransitionOwnerId = null;
        releaseVisualSurfaceOwners(owner.id);
    }
}

function scheduleSearch(ev) {
    TraceActions.search.scheduleSearch(ev);
}

function cancelScheduledSearch() {
    if (searchRuntime().searchFrame === null) return;
    var cancelFrame = searchRuntime().searchFrameCancel || window.cancelAnimationFrame || clearTimeout;
    cancelFrame(searchRuntime().searchFrame);
    searchRuntime().searchFrame = null;
    searchRuntime().searchFrameCancel = null;
    searchRuntime().searchInputDebounceStartedAt = 0;
}

function runSearchNow(force) {
    TraceActions.search.runSearchNow(force);
}

function traceSearchIndexProgressTotal(store) {
    store = store || currentTraceStore();
    var total = 0;
    for (var si = 0; si < TraceStore.stageCount(store); si++) {
        total += TraceStore.stageRuleCount(store, si);
    }
    return total;
}

function setSearchIndexProgress(state, mode, completed, total, store) {
    searchRuntime().searchIndexProgress = {
        state: state,
        mode: mode || 'none',
        completed: Math.max(0, Math.floor(Number(completed)) || 0),
        total: Math.max(0, Math.floor(Number(total)) || 0),
        traceGeneration: store ? Number(store.traceGeneration) || 0 : Number(currentTraceStore().traceGeneration) || 0
    };
}

function setSearchIndexReadyProgress(index, store) {
    var total = traceSearchIndexProgressTotal(store);
    setSearchIndexProgress('ready', index && index.mode || 'full', total, total, store);
}

function refreshSearchIndexProgressFromRuntime() {
    var search = searchRuntime();
    if (search.searchIndexWorkerJob) return;
    if (search.ruleSearchIndex) {
        setSearchIndexReadyProgress(search.ruleSearchIndex, currentTraceStore());
    } else {
        setSearchIndexProgress('unbuilt', 'none', 0, 0, currentTraceStore());
    }
}

function cancelSearchIndexWorkerJob() {
    var search = searchRuntime();
    var job = search.searchIndexWorkerJob;
    search.searchIndexWorkerJob = null;
    if (job && typeof job.cancel === 'function') job.cancel();
    refreshSearchIndexProgressFromRuntime();
}

function searchScopeUsesBodyPayloads(scope) {
    return true;
}

function normalizedSearchCollectionScope(scope) {
    return 'tree-rules';
}

function searchPayloadStateNeedsMaterialization(state) {
    return state === TraceStore.PAYLOAD_STATES.UNREQUESTED ||
           state === TraceStore.PAYLOAD_STATES.STALE;
}


function ensureSearchIndex(scope) {
    var hasExplicitScope = scope !== undefined && scope !== null && scope !== '';
    if (hasExplicitScope) scope = normalizedSearchCollectionScope(scope);
    var search = searchRuntime();
    if (search.ruleSearchIndex && search.ruleSearchIndex.mode === 'title') {
        return;
    }

    cancelSearchIndexWorkerJob();
    search.ruleSearchIndex = TraceSearch.buildTitleIndex(currentTraceStore());
    setSearchIndexReadyProgress(search.ruleSearchIndex, currentTraceStore());
}

function collectTitleSearchMatches(query, scope) {
    ensureSearchIndex(scope);
    return filterSearchMatchesForVisibleStages(TraceSearch.collectMatches(
        currentTraceStore(),
        searchRuntime().ruleSearchIndex,
        query,
        normalizedSearchCollectionScope(scope || 'tree-rules')
    ) || []);
}

function currentSearchScope() {
    return 'tree-rules';
}

function currentSearchMode() {
    return 'find';
}

function currentSearchArea() {
    return 'all';
}

function searchMatchFieldAvailable(match) {
    return !match || match.type !== 'rule' ||
        match.field === 'name' ||
        match.field === 'label';
}

function filterUnavailableSearchMatches(matches) {
    matches = matches || [];
    var filtered = [];
    for (var i = 0; i < matches.length; i++) {
        if (searchMatchFieldAvailable(matches[i])) filtered.push(matches[i]);
    }
    return filtered;
}

function stageMatchesScope(si, lowerQuery, scope) {
    return String(traceStageName(si)).toLowerCase().indexOf(lowerQuery) !== -1;
}

function groupMatchesScope(si, gi, lowerQuery, scope) {
    return String(traceGroupName(si, gi)).toLowerCase().indexOf(lowerQuery) !== -1;
}

function countSearchOccurrences(text, lowerQuery) {
    return TraceSearch.countOccurrences(text, lowerQuery);
}

function planSearchIncludesRuleField(field) {
    return field === 'name' || field === 'label';
}

function ruleSearchMatchContext(matches, si, gi, ri, rawIdx, lowerQuery, scope) {
    return {
        matches: matches,
        si: si,
        gi: gi,
        ri: ri,
        rawIdx: rawIdx,
        lowerQuery: lowerQuery,
        scope: scope,
        fieldOccurrences: {},
        recordOccurrences: {}
    };
}

function searchMatchRecordOccurrenceKey(field, path, extra) {
    var hasPath = Array.isArray(path) || typeof path === 'string';
    var parts = ['field:' + String(field || '')];
    if (hasPath) {
        parts.push('path:' + (Array.isArray(path) ? path.join('.') : String(path || '')));
    }
    var wroteSpecific = hasPath;
    extra = extra || {};
    return wroteSpecific ? parts.join('|') : '';
}

function pushRuleTextSearchMatches(ctx, field, text, path, extra) {
    if (!planSearchIncludesRuleField(field)) return;

    text = String(text == null ? '' : text).toLowerCase();
    var count = countSearchOccurrences(text, ctx.lowerQuery);
    var occurrenceKey = field;
    var start = ctx.fieldOccurrences[occurrenceKey] || 0;
    var recordOccurrenceKey = searchMatchRecordOccurrenceKey(field, path, extra);
    var recordStart = recordOccurrenceKey ? (ctx.recordOccurrences[recordOccurrenceKey] || 0) : 0;

    for (var n = 0; n < count; n++) {
        var match = {
            type: 'rule',
            si: ctx.si,
            gi: ctx.gi,
            ri: ctx.ri,
            rawIdx: ctx.rawIdx,
            field: field,
            occurrence: start + n
        };
        if (recordOccurrenceKey) match.recordOccurrence = recordStart + n;
        if (Array.isArray(path)) match.path = path.join('.');
        if (extra) {
            for (var key in extra) {
                if (Object.prototype.hasOwnProperty.call(extra, key)) {
                    match[key] = extra[key];
                }
            }
        }
        ctx.matches.push(match);
    }

    ctx.fieldOccurrences[occurrenceKey] = start + count;
    if (recordOccurrenceKey) ctx.recordOccurrences[recordOccurrenceKey] = recordStart + count;
}

function pushTreeNodeSearchMatches(ctx, node, path) {
    if (!node || !node.l) return;
    path = Array.isArray(path) ? path : [];

    pushRuleTextSearchMatches(ctx, 'label', node.l, path);

    if (node.c) {
        for (var j = 0; j < node.c.length; j++) {
            pushTreeNodeSearchMatches(ctx, node.c[j], path.concat(j));
        }
    }
}

function pushStageSearchMatches(matches, si, lowerQuery, scope) {
    if (!stageMatchesScope(si, lowerQuery, scope)) return;
    var stageText = String(traceStageName(si)).toLowerCase();
    var stageMatchCount = countSearchOccurrences(stageText, lowerQuery);
    for (var n = 0; n < stageMatchCount; n++) {
        matches.push({ type: 'stage', si: si, field: 'stage', occurrence: n });
    }
}

function pushGroupSearchMatches(matches, si, gi, lowerQuery, scope) {
    if (groupRuleCount(si, gi) <= 1 || !groupMatchesScope(si, gi, lowerQuery, scope)) return;
    var groupText = String(traceGroupName(si, gi)).toLowerCase();
    var groupMatchCount = countSearchOccurrences(groupText, lowerQuery);
    for (var n = 0; n < groupMatchCount; n++) {
        matches.push({
            type: 'group',
            si: si,
            gi: gi,
            field: 'group',
            occurrence: n
        });
    }
}

function pushMaterializedRuleSearchMatches(matches, si, gi, ri, rawIdx, lowerQuery, scope) {
    var ctx = ruleSearchMatchContext(matches, si, gi, ri, rawIdx, lowerQuery, scope);

    pushRuleTextSearchMatches(ctx, 'name', traceRuleName(si, rawIdx));

    if (traceRuleType(si, rawIdx) === 'text') {
        pushRuleTextSearchMatches(ctx, 'label', traceRuleText(si, rawIdx));
        return;
    }

    pushTreeNodeSearchMatches(ctx, tracePlanTree(si, rawIdx), []);
}

function scanRuleTitleSearchMatches(query, scope, si, gi, ri, rawIdx) {
    var lowerQuery = String(query || '').toLowerCase();
    var matches = [];
    if (!lowerQuery) return matches;

    var ctx = ruleSearchMatchContext(matches, si, gi, ri, rawIdx, lowerQuery, scope);
    pushRuleTextSearchMatches(ctx, 'name', traceRuleName(si, rawIdx));
    matches = filterUnavailableSearchMatches(matches);
    mergeLazySearchLowerBoundMatches(query, scope, {
        si: si,
        gi: gi,
        ri: ri,
        rawIdx: rawIdx
    }, matches);
    return matches;
}

function rulePayloadBucketReadyForExactSearch(store, bucket, ruleHandleValue) {
    var states = TraceStore.PAYLOAD_STATES;
    var state = TraceStore.payloadState(store, bucket, ruleHandleValue).state;
    if (state === states.UNREQUESTED || state === states.STALE || state === states.PENDING) {
        TraceStore.ensureRulePayload(store, ruleHandleValue);
        state = TraceStore.payloadState(store, bucket, ruleHandleValue).state;
    }
    return state === states.RENDERED || state === states.EMPTY;
}

function rulePayloadReadyForExactSearch(scope, si, rawIdx) {
    var store = currentTraceStore();
    var ruleHandleValue = TraceStore.ruleHandle(store, si, rawIdx);
    if (!ruleHandleValue) return false;

    if (traceRuleType(si, rawIdx) === 'text') {
        return rulePayloadBucketReadyForExactSearch(store, 'text', ruleHandleValue);
    }

    return rulePayloadBucketReadyForExactSearch(store, 'trees', ruleHandleValue);
}

function scanRuleExactSearchMatches(query, scope, si, gi, ri, rawIdx) {
    var lowerQuery = String(query || '').toLowerCase();
    var matches = [];
    if (!lowerQuery) return matches;
    if (!rulePayloadReadyForExactSearch(scope, si, rawIdx)) return null;
    pushMaterializedRuleSearchMatches(matches, si, gi, ri, rawIdx, lowerQuery, scope);
    matches = filterUnavailableSearchMatches(matches);
    mergeLazySearchExactMatches(query, scope, {
        si: si,
        gi: gi,
        ri: ri,
        rawIdx: rawIdx
    }, matches);
    return matches;
}

function searchViewportRangeForCollection() {
    var traceEl = traceElement();
    return traceEl
        ? traceViewportRange(traceEl)
        : { left: -Infinity, right: Infinity, width: Infinity };
}

function layoutItemSearchInterval(item) {
    if (!item) return null;
    return {
        left: item.globalLeft,
        right: item.globalLeft + item.width,
        width: item.width
    };
}

function layoutItemIntersectsSearchRange(item, range) {
    return traceIntervalsIntersect(layoutItemSearchInterval(item), range);
}

function collectVisibleGroupRuleSearchMatches(matches, si, gi, lowerQuery, scope) {
    for (var ri = 0; ri < groupRuleCount(si, gi); ri++) {
        var ruleMatches = collectVisibleRuleSearchMatches(matches, si, gi, ri, lowerQuery, scope);
        if (!ruleMatches) continue;
        for (var r = 0; r < ruleMatches.length; r++) matches.push(ruleMatches[r]);
    }
}

function collectVisibleStageHiddenSearchMatches(matches, si, lowerQuery, scope) {
}

function collectVisibleRuleSearchMatches(matches, si, gi, ri, lowerQuery, scope) {
    var rawIdx = rawRuleIndex(si, gi, ri);
    if (effectiveRuleOpen(si, gi, ri)) {
        return scanRuleExactSearchMatches(lowerQuery, scope, si, gi, ri, rawIdx) ||
            scanRuleTitleSearchMatches(lowerQuery, scope, si, gi, ri, rawIdx);
    }
    return scanRuleTitleSearchMatches(lowerQuery, scope, si, gi, ri, rawIdx);
}

function collectVisibleSearchRowMatches(matches, row, range, lowerQuery, scope) {
    if (!row || !Array.isArray(row.items)) return;

    var start = firstRowItemIntersectingRange(row, range);
    for (var i = start; i < row.items.length; i++) {
        var item = row.items[i];
        if (!item || item.globalLeft >= range.right) break;
        if (!layoutItemIntersectsSearchRange(item, range)) continue;

        if (item.kind === 'group') {
            pushGroupSearchMatches(matches, item.si, item.gi, lowerQuery, scope);
            if (item.row) {
                collectVisibleSearchRowMatches(matches, item.row, range, lowerQuery, scope);
            }
        } else if (item.kind === 'rule') {
            var ruleMatches = collectVisibleRuleSearchMatches(
                matches,
                item.si,
                item.gi,
                item.ri,
                lowerQuery,
                scope
            );
            if (!ruleMatches) continue;
            for (var m = 0; m < ruleMatches.length; m++) matches.push(ruleMatches[m]);
        }
    }
}

function collectVisibleSearchMatches(query, scope) {
    scope = normalizedSearchCollectionScope(scope);
    var lowerQuery = String(query || '').toLowerCase();
    var matches = [];
    if (!lowerQuery) return matches;

    var model = getTraceLayoutModel();
    var range = searchViewportRangeForCollection();
    var visibleStages = visibleStagesForModel(model);
    var start = firstVisibleStageIntersectingRange(visibleStages, range);

    for (var i = start; i < visibleStages.length; i++) {
        var stage = visibleStages[i];
        if (!stage || stage.left >= range.right) break;
        if (!stageModelIntersectsRange(stage, range)) continue;

        pushStageSearchMatches(matches, stage.si, lowerQuery, scope);
        if (stage.open) {
            collectVisibleSearchRowMatches(matches, stage.row, range, lowerQuery, scope);
        } else {
            collectVisibleStageHiddenSearchMatches(matches, stage.si, lowerQuery, scope);
        }
    }

    return matches;
}

function pushVisibleSearchRuleRef(refs, seen, si, gi, ri) {
    if (!validRuleRef(si, gi, ri)) return;
    if (!isRuleExpandedInLayout(si, gi, ri)) return;
    var rawIdx = rawRuleIndex(si, gi, ri);
    var ordinal = searchRuleOrdinalForRawIndex(si, rawIdx);
    var key = String(ordinal);
    if (seen[key]) return;
    seen[key] = true;
    refs.push({
        si: si,
        gi: gi,
        ri: ri,
        rawIdx: rawIdx,
        ordinal: ordinal
    });
}

function collectVisibleSearchRowRuleRefs(refs, seen, row, range) {
    if (!row || !Array.isArray(row.items)) return;

    var start = firstRowItemIntersectingRange(row, range);
    for (var i = start; i < row.items.length; i++) {
        var item = row.items[i];
        if (!item || item.globalLeft >= range.right) break;
        if (!layoutItemIntersectsSearchRange(item, range)) continue;

        if (item.kind === 'group') {
            if (item.row) {
                collectVisibleSearchRowRuleRefs(refs, seen, item.row, range);
            }
        } else if (item.kind === 'rule') {
            pushVisibleSearchRuleRef(refs, seen, item.si, item.gi, item.ri);
        }
    }
}

function collectVisibleSearchRuleRefs() {
    var refs = [];
    var seen = {};
    var model = getTraceLayoutModel();
    var range = searchViewportRangeForCollection();
    var visibleStages = visibleStagesForModel(model);
    var start = firstVisibleStageIntersectingRange(visibleStages, range);

    for (var i = start; i < visibleStages.length; i++) {
        var stage = visibleStages[i];
        if (!stage || stage.left >= range.right) break;
        if (!stageModelIntersectsRange(stage, range)) continue;
        if (!stage.open) continue;
        collectVisibleSearchRowRuleRefs(refs, seen, stage.row, range);
    }

    return refs;
}

function collectLazySearchState(query, scope, mode) {
    var allMatches = filterSearchMatchesForVisibleStages(filterUnavailableSearchMatches(mergeUniqueSearchMatches(
        lazySearchKnownMatches(query, scope),
        collectTitleSearchMatches(query, scope)
    )));
    var filteredMatches = filterSearchMatchesForMode(allMatches, mode || currentSearchMode());
    return {
        matches: filteredMatches,
        expandableMatchCount: Math.max(allMatches.length, lazySearchKnownCount(query, scope)),
        collapsedIndicators: measureSearchTiming('collapsed-indicator-collection', function() {
            return collectCollapsedSearchIndicatorsFromMatches(query, allMatches);
        }, { mode: mode || currentSearchMode(), matchCount: allMatches.length, lazyComplete: true })
    };
}

function filterSearchMatchesForVisibleStages(matches) {
    matches = matches || [];
    if (showEmptyStages()) return matches;

    var filtered = [];
    for (var i = 0; i < matches.length; i++) {
        var match = matches[i];
        if (match.type === 'stage' && !stageVisible(match.si)) continue;
        filtered.push(match);
    }
    return filtered;
}

function collectSearchMatchesFromIndex(query, scope) {
    scope = normalizedSearchCollectionScope(scope);
    var bodyScope = searchScopeUsesBodyPayloads(scope);
    var cached = bodyScope ? null : cachedSearchResultMatches(query, scope);
    if (cached) return cached;

    var matches;
    var globalSummary = bodyScope ? peekGlobalSearchSummary(query, scope) : null;
    if (globalSummary) {
        matches = searchMatchesFromGlobalSummary(globalSummary);
    } else if (bodyScope) {
        var visibleMatches = collectVisibleSearchMatches(query, scope);
        matches = mergeUniqueSearchMatches(
            mergeUniqueSearchMatches(
                visibleMatches,
                lazySearchKnownMatches(query, scope)
            ),
            restoredSearchIndicatorMatches(query, scope)
        );
    } else {
        ensureSearchIndex(scope);
        matches = TraceSearch.collectMatches(currentTraceStore(), searchRuntime().ruleSearchIndex, query, scope);
    }
    matches = filterSearchMatchesForVisibleStages(filterUnavailableSearchMatches(matches));
    return rememberSearchResultMatches(query, scope, matches);
}

function searchMatchTreeSession(match) {
    if (!match || match.type !== 'rule') return null;
    if (typeof activeSearchExpandOverlay === 'function' && activeSearchExpandOverlay()) {
        return null;
    }
    if (typeof existingTreeSessionForRule !== 'function') return null;
    return existingTreeSessionForRule(ruleKey(match.si, match.gi, match.ri));
}

function treePidProperAncestors(pid) {
    pid = String(pid || '');
    if (!pid) return [];
    var parts = pid.split('-');
    var ancestors = [];
    for (var i = 1; i < parts.length; i++) {
        ancestors.push(parts.slice(0, i).join('-'));
    }
    return ancestors;
}

function searchMatchTreePathVisibleInCurrentProjection(match) {
    if (!searchMatchHasTreePath(match)) return true;
    if (typeof activeSearchExpandOverlay === 'function' && activeSearchExpandOverlay()) {
        return true;
    }

    var pid = searchMatchTreePathPid(match);
    if (!pid) return true;

    var session = searchMatchTreeSession(match);
    var collapsed = session && session.collapsed || {};
    var ancestors = treePidProperAncestors(pid);
    for (var i = 0; i < ancestors.length; i++) {
        if (collapsed[ancestors[i]]) return false;
    }

    return true;
}

function ruleMatchAvailableInCurrentLayout(match) {
    if (!match || match.type !== 'rule') return false;
    var modelRule = getTraceLayoutModel().rules[ruleKey(match.si, match.gi, match.ri)];
    if (!modelRule) return false;

    var rule = effectiveRuleState(match.si, match.gi, match.ri);
    if (!searchMatchTreePathVisibleInCurrentProjection(match)) return false;
    if (match.field === 'name') return true;
    if (match.field === 'label') return rule.open;
    return false;
}

function searchMatchAvailableInCurrentLayout(match) {
    if (!match) return false;
    if (match.type === 'stage') return stageVisible(match.si);
    if (match.type === 'group') {
        return stageVisible(match.si) &&
            effectiveStageOpen(match.si) &&
            groupRuleCount(match.si, match.gi) > 1;
    }
    if (match.type === 'rule') return ruleMatchAvailableInCurrentLayout(match);
    return false;
}

function filterSearchMatchesForMode(matches, mode) {
    matches = filterUnavailableSearchMatches(matches);
    var filtered = [];
    for (var i = 0; i < matches.length; i++) {
        if (searchMatchAvailableInCurrentLayout(matches[i])) filtered.push(matches[i]);
    }
    return filtered;
}

function collectSearchMatchesForCurrentSearch(query, scope) {
    return filterSearchMatchesForMode(collectSearchMatchesFromIndex(query, scope), currentSearchMode());
}

function collectSearchStateForCurrentSearch(query, scope) {
    var mode = currentSearchMode();
    var globalSummary = searchScopeUsesBodyPayloads(scope)
        ? peekGlobalSearchSummary(query, scope)
        : null;
    if (globalSummary) {
        return collectGlobalSummarySearchState(query, scope, mode, globalSummary);
    }
    var allMatches = measureSearchTiming('match-collection', function() {
        return collectSearchMatchesFromIndex(query, scope);
    }, { mode: mode, scope: scope });
    allMatches = filterUnavailableSearchMatches(allMatches);
    var filteredMatches = filterSearchMatchesForMode(allMatches, mode);
    return {
        matches: filteredMatches,
        expandableMatchCount: Math.max(allMatches.length, lazySearchKnownCount(query, scope)),
        collapsedIndicators: measureSearchTiming('collapsed-indicator-collection', function() {
            return collectCollapsedSearchIndicatorsFromMatches(query, allMatches);
        }, { mode: mode, matchCount: allMatches.length })
    };
}

function collectGlobalSummarySearchState(query, scope, mode, summary) {
    var allMatches = filterUnavailableSearchMatches(searchMatchesFromGlobalSummary(summary));
    var matches = filterSearchMatchesForMode(allMatches, mode);
    return {
        matches: matches,
        expandableMatchCount: normalizeSearchCount(summary && summary.totalCount),
        collapsedIndicators: measureSearchTiming('collapsed-indicator-collection', function() {
            return collectCollapsedSearchIndicatorsFromMatches(query, allMatches);
        }, { mode: mode, matchCount: allMatches.length, summary: true })
    };
}

function collectVisibleSearchPreviewState(query, scope, mode) {
    var allMatches = measureSearchTiming('visible-preview-collection', function() {
        return collectVisibleSearchMatches(query, scope);
    }, { mode: mode, scope: scope });
    allMatches = mergeUniqueSearchMatches(
        mergeUniqueSearchMatches(
            allMatches,
            lazySearchKnownMatches(query, scope)
        ),
        collectTitleSearchMatches(query, scope)
    );
    allMatches = filterSearchMatchesForVisibleStages(filterUnavailableSearchMatches(allMatches));
    var filteredMatches = filterSearchMatchesForMode(allMatches, mode);
    var visibleMatches = filteredMatches.filter(function(match) {
        return match && match.type === 'rule' ? true : searchMatchIntersectsTraceViewport(match);
    });
    return {
        matches: visibleMatches,
        expandableMatchCount: Math.max(visibleMatches.length, lazySearchKnownCount(query, scope)),
        collapsedIndicators: measureSearchTiming('collapsed-indicator-collection', function() {
            return collectCollapsedSearchIndicatorsFromMatches(query, allMatches);
        }, { mode: mode, matchCount: allMatches.length, preview: true })
    };
}

function knownSearchMatchesForCollapsedIndicators(query, scope) {
    var committedMatches = searchResultMatchesIntent(query, scope, currentSearchMode())
        ? (searchRuntime().searchMatches || [])
        : [];
    var globalSummary = searchScopeUsesBodyPayloads(scope)
        ? peekGlobalSearchSummary(query, scope)
        : null;
    return filterSearchMatchesForVisibleStages(filterUnavailableSearchMatches(mergeUniqueSearchMatches(
        mergeUniqueSearchMatches(
            mergeUniqueSearchMatches(
                committedMatches,
                lazySearchKnownMatches(query, scope)
            ),
            mergeUniqueSearchMatches(
                globalSummary ? searchMatchesFromGlobalSummary(globalSummary) : [],
                restoredSearchIndicatorMatches(query, scope)
            )
        ),
        collectTitleSearchMatches(query, scope)
    )));
}

function refreshCollapsedSearchIndicatorsForKnownMatches(query, scope) {
    if (!currentSearchIntentMatches(query, scope)) return false;
    var matches = knownSearchMatchesForCollapsedIndicators(query, scope);
    var indicators = collectCollapsedSearchIndicatorsFromMatches(query, matches);
    var token = activeSearchTransactionToken();
    if (token && !searchTransactionTokenCurrent(token, { payload: true })) token = null;
    applyCollapsedSearchIndicators(indicators, {
        query: query || '',
        scope: scope || '',
        searchTransactionToken: token,
        allowWithoutSearchTransaction: !token
    });
    return true;
}

function searchVisibleRuleKeyFromMatch(match) {
    if (!match || match.type !== 'rule') return '';
    var record = searchMatchStableRecord(match);
    if (record && record.ruleHandle) return String(record.ruleHandle);
    return [
        searchMatchIdentityPart(match.si),
        searchMatchIdentityPart(match.gi),
        searchMatchIdentityPart(match.ri),
        searchMatchIdentityPart(match.rawIdx)
    ].join(':');
}

function rememberVisibleSearchLayer(query, scope, mode, matches) {
    var search = searchRuntime();
    if (!search.searchLayers) return;
    var visible = search.searchLayers.visible || {};
    matches = matches || [];
    var searchedRules = {};
    for (var i = 0; i < matches.length; i++) {
        var key = searchVisibleRuleKeyFromMatch(matches[i]);
        if (key) searchedRules[key] = true;
    }

    visible.query = String(query || '');
    visible.scope = normalizedSearchCollectionScope(scope);
    visible.mode = normalizeSearchModeValue(mode);
    visible.traceGeneration = Number(currentTraceStore().traceGeneration) || 0;
    visible.payloadGeneration = currentSearchPayloadGeneration();
    visible.projectionGeneration = currentSearchLayoutGeneration();
    visible.knownCount = matches.length;
    visible.matches = cloneSearchMatchList(matches);
    visible.searchedRules = searchedRules;
    search.searchLayers.visible = visible;
}

function collectSearchMatchesForExpansion(query, scope, mode) {
    return filterUnavailableSearchMatches(collectSearchMatchesFromIndex(query, scope));
}

function clearSearchMatches(options) {
    options = options || {};
    clearActiveSearchMatch({
        searchTransactionToken: options.searchTransactionToken,
        allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction
    });
    searchRuntime().searchMatches = [];
    searchRuntime().searchExpandableMatchCount = 0;
    searchRuntime().currentSearchMatchIndex = -1;
    searchRuntime().searchNavigationMatchIndex = -1;
    searchRuntime().activeSearchMatchRecord = null;
    clearPendingSearchActivation();
    if (!options.deferStatus) updateSearchStatus(0);
}

function normalizeSearchCount(count) {
    count = Math.floor(Number(count));
    if (!Number.isFinite(count) || count < 0) return 0;
    return count;
}

function exactSearchNumber(count) {
    return String(normalizeSearchCount(count)).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function compactSearchNumber(count) {
    count = normalizeSearchCount(count);
    if (count < 1000) return String(count);

    function compactWithUnit(value, divisor, suffix) {
        var scaled = value / divisor;
        var rounded = scaled < 10 ? Math.round(scaled * 10) / 10 : Math.round(scaled);
        return String(rounded).replace(/\.0$/, '') + suffix;
    }

    if (count < 999500) return compactWithUnit(count, 1000, 'k');
    if (count < 999500000) return compactWithUnit(count, 1000000, 'm');
    return compactWithUnit(count, 1000000000, 'b');
}

function searchMatchCountLabel(count) {
    count = normalizeSearchCount(count);
    return compactSearchNumber(count) + ' match' + (count === 1 ? '' : 'es');
}

function currentSearchExpandableMatchCount(matchCount) {
    matchCount = normalizeSearchCount(matchCount);
    return normalizeSearchCount(searchRuntime().searchExpandableMatchCount || matchCount);
}

function searchMatchOrdinalLabel(current, total) {
    return compactSearchNumber(current) + ' / ' + compactSearchNumber(total);
}

function setSearchInfo(infoEl, text, title) {
    infoEl.textContent = text;
    infoEl.title = title || '';
    if (title && infoEl.setAttribute) {
        infoEl.setAttribute('aria-label', title);
    } else if (infoEl.removeAttribute) {
        infoEl.removeAttribute('aria-label');
    }
}

function activeGlobalSearchSummaryJob() {
    var layers = searchRuntime().searchLayers;
    return layers && layers.globalJob ? layers.globalJob.activeJob : null;
}

function currentGlobalSearchSummaryForStatus() {
    var intent = currentSearchInputIntent();
    if (!intent.query) return null;
    return peekGlobalSearchSummary(intent.query, intent.scope);
}

function globalSearchSummaryPrefixBefore(summary, summaryIndex) {
    if (!summary || !Array.isArray(summary.counts)) return 0;
    var total = 0;
    for (var i = 0; i < summaryIndex && i < summary.counts.length; i++) {
        total += normalizeSearchCount(summary.counts[i]);
    }
    return total;
}

function searchMatchRuleHandle(match) {
    var record = searchMatchStableRecord(match);
    return record && record.ruleHandle ? String(record.ruleHandle) : '';
}

function localSearchMatchIndexWithinRule(matchIndex, ruleHandle) {
    if (!ruleHandle || matchIndex < 0) return -1;
    var matches = searchRuntime().searchMatches || [];
    var localIndex = -1;
    for (var i = 0; i <= matchIndex && i < matches.length; i++) {
        if (searchMatchRuleHandle(matches[i]) !== ruleHandle) continue;
        localIndex++;
    }
    return localIndex;
}

function globalSearchOrdinalForLocalMatch(summary, matchIndex) {
    if (!summary || matchIndex < 0) return 0;
    var matches = searchRuntime().searchMatches || [];
    var match = matches[matchIndex];
    var ruleHandle = searchMatchRuleHandle(match);
    if (!ruleHandle || !Array.isArray(summary.ruleHandles)) return 0;
    var summaryIndex = summary.ruleHandles.indexOf(ruleHandle);
    if (summaryIndex < 0) return 0;
    var localIndex = localSearchMatchIndexWithinRule(matchIndex, ruleHandle);
    var ruleCount = normalizeSearchCount(summary.counts && summary.counts[summaryIndex]);
    if (localIndex < 0 || localIndex >= ruleCount) return 0;
    return globalSearchSummaryPrefixBefore(summary, summaryIndex) + localIndex + 1;
}

function localExactSearchCacheKey(query, scope, ruleHandleValue) {
    return JSON.stringify([
        Number(currentTraceStore().traceGeneration) || 0,
        currentSearchPayloadGeneration(),
        currentSearchLayoutGeneration(),
        normalizedSearchCacheQuery(query),
        normalizedSearchCollectionScope(scope),
        String(ruleHandleValue || '')
    ]);
}

function cachedLocalExactSearchMatches(query, scope, ruleHandleValue) {
    var layers = searchRuntime().searchLayers;
    var cache = layers && layers.localExactCache;
    if (!cache || !cache.entries) return null;
    var key = localExactSearchCacheKey(query, scope, ruleHandleValue);
    var entry = cache.entries[key];
    if (!entry) {
        cache.misses++;
        return null;
    }
    cache.hits++;
    return cloneSearchMatchList(entry.matches);
}

function rememberLocalExactSearchMatches(query, scope, ruleHandleValue, matches) {
    var layers = searchRuntime().searchLayers;
    var cache = layers && layers.localExactCache;
    if (!cache) return matches || [];
    var key = localExactSearchCacheKey(query, scope, ruleHandleValue);
    rememberBoundedSearchCache(
        cache.entries,
        cache.order,
        key,
        { matches: cloneSearchMatchList(matches) },
        SEARCH_LOCAL_EXACT_CACHE_LIMIT
    );
    return matches || [];
}

function searchRuleRefForRawIndex(si, rawIdx) {
    for (var gi = 0; gi < groupCount(si); gi++) {
        for (var ri = 0; ri < groupRuleCount(si, gi); ri++) {
            if (rawRuleIndex(si, gi, ri) === rawIdx) {
                return { si: si, gi: gi, ri: ri, rawIdx: rawIdx };
            }
        }
    }
    return null;
}

function searchRuleRefForOrdinal(ruleOrdinal) {
    ruleOrdinal = Math.floor(Number(ruleOrdinal));
    if (!Number.isFinite(ruleOrdinal) || ruleOrdinal < 0) return null;
    var remaining = ruleOrdinal;
    for (var si = 0; si < TraceStore.stageCount(currentTraceStore()); si++) {
        var count = TraceStore.stageRuleCount(currentTraceStore(), si);
        if (remaining < count) return searchRuleRefForRawIndex(si, remaining);
        remaining -= count;
    }
    return null;
}

function searchRuleRefForHandle(ruleHandleValue) {
    ruleHandleValue = String(ruleHandleValue || '');
    if (!ruleHandleValue) return null;
    var store = currentTraceStore();
    for (var si = 0; si < TraceStore.stageCount(store); si++) {
        for (var rawIdx = 0; rawIdx < TraceStore.stageRuleCount(store, si); rawIdx++) {
            if (String(TraceStore.ruleHandle(store, si, rawIdx) || '') === ruleHandleValue) {
                return searchRuleRefForRawIndex(si, rawIdx);
            }
        }
    }
    return null;
}

function searchRuleRefForGlobalSummaryRow(summary, summaryIndex) {
    if (!summary) return null;
    var ruleHandleValue = summary.ruleHandles && summary.ruleHandles[summaryIndex];
    return searchRuleRefForHandle(ruleHandleValue) ||
        searchRuleRefForOrdinal(summary.ruleOrdinals && summary.ruleOrdinals[summaryIndex]);
}

function searchSummaryMaskHas(mask, name) {
    return !!(Number(mask) & searchSummaryMaskValue(name));
}

function searchSummaryMaskHasBody(mask) {
    return searchSummaryMaskHas(mask, 'tree') || searchSummaryMaskHas(mask, 'text');
}

function pushSearchMatchFromGlobalSummary(matches, ref, field, mask, count, order) {
    matches.push({
        type: 'rule',
        si: ref.si,
        gi: ref.gi,
        ri: ref.ri,
        rawIdx: ref.rawIdx,
        field: field,
        occurrence: 0,
        summaryMask: mask,
        summaryCount: count,
        summaryOrder: order
    });
}

function searchMatchesFromGlobalSummary(summary) {
    var matches = [];
    if (!summary || !Array.isArray(summary.counts)) return matches;
    var titleMatches = Array.isArray(summary.titleMatches) ? summary.titleMatches : [];
    for (var titleIndex = 0; titleIndex < titleMatches.length; titleIndex++) {
        var title = titleMatches[titleIndex] || {};
        var titleCount = normalizeSearchCount(title.count);
        if (!titleCount) continue;
        if (title.type === 'stage') {
            matches.push({
                type: 'stage',
                si: Math.max(0, Math.floor(Number(title.si)) || 0),
                field: 'stage',
                occurrence: 0,
                summaryCount: titleCount,
                summaryOrder: Math.max(0, Math.floor(Number(title.order)) || 0)
            });
        } else if (title.type === 'group') {
            matches.push({
                type: 'group',
                si: Math.max(0, Math.floor(Number(title.si)) || 0),
                gi: Math.max(0, Math.floor(Number(title.gi)) || 0),
                field: 'group',
                occurrence: 0,
                summaryCount: titleCount,
                summaryOrder: Math.max(0, Math.floor(Number(title.order)) || 0)
            });
        }
    }
    for (var i = 0; i < summary.counts.length; i++) {
        if (!normalizeSearchCount(summary.counts[i])) continue;
        var ref = searchRuleRefForGlobalSummaryRow(summary, i);
        if (!ref) continue;
        var mask = Number(summary.masks && summary.masks[i]) || 0;
        var count = normalizeSearchCount(summary.counts[i]);
        var order = Math.max(0, Math.floor(Number(summary.orders && summary.orders[i])) || 0);
        if (searchSummaryMaskHas(mask, 'title')) {
            pushSearchMatchFromGlobalSummary(matches, ref, 'name', mask, count, order);
        }
        if (searchSummaryMaskHasBody(mask)) {
            pushSearchMatchFromGlobalSummary(matches, ref, 'label', mask, count, order);
        }
    }
    matches.sort(function(a, b) {
        var orderA = a && a.summaryOrder !== undefined ? Number(a.summaryOrder) : 0;
        var orderB = b && b.summaryOrder !== undefined ? Number(b.summaryOrder) : 0;
        if (orderA !== orderB) return orderA - orderB;
        return 0;
    });
    return matches;
}

function collectLocalExactSearchMatchesForRule(query, scope, ref) {
    if (!ref) return [];
    scope = normalizedSearchCollectionScope(scope);
    var store = currentTraceStore();
    var ruleHandleValue = TraceStore.ruleHandle(store, ref.si, ref.rawIdx);
    if (!ruleHandleValue) return [];
    var cached = cachedLocalExactSearchMatches(query, scope, ruleHandleValue);
    if (cached) return cached;

    TraceStore.ensureRulePayload(store, ruleHandleValue);
    var lowerQuery = String(query || '').toLowerCase();
    var matches = [];
    if (!lowerQuery) return matches;
    pushMaterializedRuleSearchMatches(matches, ref.si, ref.gi, ref.ri, ref.rawIdx, lowerQuery, scope);
    matches = filterUnavailableSearchMatches(matches);
    matches = filterSearchMatchesForMode(matches, currentSearchMode());
    return rememberLocalExactSearchMatches(query, scope, ruleHandleValue, matches);
}

function globalSearchSummaryTargetAt(summary, ordinal) {
    if (!summary) return null;
    var total = normalizeSearchCount(summary.totalCount);
    if (!total) return null;
    ordinal = ((Math.floor(Number(ordinal)) || 0) % total + total) % total;
    var offset = 0;
    for (var i = 0; i < summary.counts.length; i++) {
        var count = normalizeSearchCount(summary.counts[i]);
        if (ordinal < offset + count) {
            return {
                summaryIndex: i,
                localMatchIndex: ordinal - offset,
                globalOrdinal: ordinal
            };
        }
        offset += count;
    }
    return null;
}

function currentGlobalSearchOrdinal(summary) {
    var ordinal = globalSearchOrdinalForLocalMatch(
        summary,
        searchRuntime().currentSearchMatchIndex
    );
    return ordinal > 0 ? ordinal - 1 : -1;
}

function globalSearchNavigationOrdinal(summary, direction) {
    var total = normalizeSearchCount(summary && summary.totalCount);
    if (!total) return -1;
    direction = direction < 0 ? -1 : 1;
    var current = currentGlobalSearchOrdinal(summary);
    if (current < 0) return direction < 0 ? total - 1 : 0;
    return (current + direction + total) % total;
}

function activateGlobalSearchExactMatch(intent, matches, index, options) {
    if (!matches || !matches.length) return false;
    index = Math.floor(Number(index));
    if (!Number.isFinite(index) || index < 0 || index >= matches.length) return false;

    replaceSearchMatches(matches, false, {
        deferStatus: true
    });
    ensureCommittedSearchResultIntent(intent.query, intent.scope, intent.mode);
    searchRuntime().searchResultState = 'partial-visible';
    return setActiveSearchMatch(index, {
        preserveLayout: !!(options && options.preserveLayout),
        preserveViewport: !!(options && options.preserveViewport)
    });
}

function globalSearchSummaryIndexForRuleHandle(summary, ruleHandle) {
    if (!summary || !ruleHandle || !Array.isArray(summary.ruleHandles)) return -1;
    return summary.ruleHandles.indexOf(ruleHandle);
}

function navigateGlobalSearchWithinCurrentRule(direction, summary, intent, options) {
    var current = searchNavigationMatch() || activeSearchMatch();
    var ruleHandleValue = searchMatchRuleHandle(current);
    var summaryIndex = globalSearchSummaryIndexForRuleHandle(summary, ruleHandleValue);
    if (summaryIndex < 0) return false;
    var ref = searchRuleRefForGlobalSummaryRow(summary, summaryIndex);
    var matches = collectLocalExactSearchMatchesForRule(intent.query, intent.scope, ref);
    if (!matches.length) return false;
    var currentIndex = findSearchMatchIndex(current);
    var localIndex = currentIndex >= 0
        ? localSearchMatchIndexWithinRule(currentIndex, ruleHandleValue)
        : findSearchMatchIndexByStableRecord(current);
    if (localIndex < 0 || localIndex >= matches.length) return false;
    var nextIndex = localIndex + (direction < 0 ? -1 : 1);
    if (nextIndex < 0 || nextIndex >= matches.length) return false;
    return activateGlobalSearchExactMatch(intent, matches, nextIndex, options);
}

function navigateGlobalSearchSummaryRule(direction, summary, startIndex, intent, options) {
    if (!summary || !Array.isArray(summary.counts) || !summary.counts.length) return false;
    var count = summary.counts.length;
    direction = direction < 0 ? -1 : 1;
    var index = ((Math.floor(Number(startIndex)) || 0) % count + count) % count;
    for (var visited = 0; visited < count; visited++) {
        if (normalizeSearchCount(summary.counts[index])) {
            var ref = searchRuleRefForGlobalSummaryRow(summary, index);
            var matches = collectLocalExactSearchMatchesForRule(intent.query, intent.scope, ref);
            if (matches.length) {
                return activateGlobalSearchExactMatch(
                    intent,
                    matches,
                    direction < 0 ? matches.length - 1 : 0,
                    options
                );
            }
        }
        index = (index + direction + count) % count;
    }
    return false;
}

function navigateGlobalSearchSummaryMatch(direction, summary, options) {
    summary = summary || currentGlobalSearchSummaryForStatus();
    if (!summary || !normalizeSearchCount(summary.totalCount)) return false;
    var intent = currentSearchInputIntent();
    if (navigateGlobalSearchWithinCurrentRule(direction, summary, intent, options)) return true;

    var current = searchNavigationMatch() || activeSearchMatch();
    var currentSummaryIndex = globalSearchSummaryIndexForRuleHandle(
        summary,
        searchMatchRuleHandle(current)
    );
    var ordinal = globalSearchNavigationOrdinal(summary, direction);
    var target = globalSearchSummaryTargetAt(summary, ordinal);
    var startIndex = currentSummaryIndex >= 0
        ? currentSummaryIndex + (direction < 0 ? -1 : 1)
        : (target ? target.summaryIndex : 0);
    return navigateGlobalSearchSummaryRule(direction, summary, startIndex, intent, options);
}

function searchMatchRuleOrdinal(match) {
    if (!match || match.type !== 'rule') return -1;
    var rawIdx = match.rawIdx === undefined || match.rawIdx === null
        ? rawRuleIndex(match.si, match.gi, match.ri)
        : match.rawIdx;
    return searchRuleOrdinalForRawIndex(match.si, rawIdx);
}

function lazyFindNextStartOrdinal(direction, count) {
    var index = currentSearchNavigationMatchIndex(count);
    if (index < 0) index = searchRuntime().currentSearchMatchIndex;
    if (index < 0 && count > 0) return -1;
    var current = index >= 0 ? searchRuntime().searchMatches[index] : null;
    var ordinal = searchMatchRuleOrdinal(current);
    var total = searchTotalRuleCount(currentTraceStore());
    if (!total) return -1;
    if (ordinal < 0) return direction < 0 ? total - 1 : 0;
    return (ordinal + (direction < 0 ? -1 : 1) + total) % total;
}

function shouldStartLazyFindNextNavigation(direction, count) {
    var intent = currentSearchInputIntent();
    if (!intent.query || !searchScopeUsesBodyPayloads(intent.scope)) return false;
    if (currentSearchResultReadyForNavigation() &&
            searchRuntime().searchResultState !== 'partial-visible') {
        return false;
    }
    if (lazySearchIndexComplete(intent.query, intent.scope)) return false;
    count = normalizeSearchCount(count);
    if (!count) return true;
    var index = currentSearchNavigationMatchIndex(count);
    if (index < 0) index = searchRuntime().currentSearchMatchIndex;
    if (index < 0) return false;
    return direction < 0 ? index <= 0 : index >= count - 1;
}

function startLazyFindNextNavigation(direction) {
    var intent = currentSearchInputIntent();
    var count = searchRuntime().searchMatches.length;
    if (!shouldStartLazyFindNextNavigation(direction, count)) return false;
    var startOrdinal = lazyFindNextStartOrdinal(direction, count);
    if (startOrdinal < 0) return false;
    var run = startLazyFindNextSearchJob(intent.query, intent.scope, direction, startOrdinal);
    updateSearchStatus(searchRuntime().searchMatches.length);
    if (!run) return false;
    run.promise.then(function(result) {
        updateSearchStatus(searchRuntime().searchMatches.length);
        if (!result || !result.found || !result.entry) return;
        refreshSearchTransactionPayloadGeneration(searchRuntime().activeSearchTransaction);
        var matches = filterUnavailableSearchMatches(mergeUniqueSearchMatches(
            searchRuntime().searchMatches || [],
            lazySearchKnownMatches(intent.query, intent.scope)
        ));
        replaceSearchMatches(matches, true, { deferStatus: true });
        ensureCommittedSearchResultIntent(intent.query, intent.scope, intent.mode);
        searchRuntime().searchResultState = lazySearchIndexComplete(intent.query, intent.scope)
            ? 'ready'
            : 'partial-visible';
        var targetMatches = result.entry.exactMatches || [];
        var target = direction < 0
            ? targetMatches[targetMatches.length - 1]
            : targetMatches[0];
        var targetIndex = findSearchMatchIndex(target);
        if (targetIndex >= 0) {
            setActiveSearchMatch(targetIndex, {
                preserveLayout: true,
                preserveViewport: false
            });
        } else {
            updateSearchStatus(searchRuntime().searchMatches.length);
        }
    }, function(error) {
        if (typeof console !== 'undefined' && console && console.warn) {
            console.warn('Lazy find-next search failed', error);
        }
        updateSearchStatus(searchRuntime().searchMatches.length);
    });
    return true;
}

function updateGlobalSearchButtonState(hasQuery) {
    var btn = document.getElementById('search-global-button');
    if (!btn) return;
    var kind = globalSearchJobKind();
    var running = kind === 'global-summary' ||
        kind === 'auto-global-summary' ||
        kind === 'find-next';
    var blockedByOtherJob = !!kind && !running;
    var runningTitle = globalSearchJobKind() === 'find-next' ? 'Cancel find next' : 'Cancel global search';
    btn.disabled = blockedByOtherJob || (!running && !hasQuery);
    if (btn.classList && btn.classList.toggle) {
        btn.classList.toggle('search-global-running', running);
    }
    if (btn.setAttribute) {
        btn.setAttribute('aria-label', running ? runningTitle : 'Search all matches');
        btn.setAttribute('title', running ? runningTitle : 'Search all matches');
    }
    btn.title = running ? runningTitle : 'Search all matches';
    var visualState = running ? 'running' : 'idle';
    var previousState = btn.getAttribute ? btn.getAttribute('data-search-global-state') : btn.searchGlobalState;
    if (previousState === visualState) return;
    if (btn.setAttribute) btn.setAttribute('data-search-global-state', visualState);
    btn.searchGlobalState = visualState;
    btn.innerHTML = running
        ? '<span class="diff-spinner search-global-spinner" aria-hidden="true"></span>'
        : traceIconSvg('globe');
}

function updateSearchExpandButtonState(hasQuery) {
    var btn = document.getElementById('search-expand-button');
    if (!btn) return;
    var kind = globalSearchJobKind();
    var running = kind === 'expand';
    var blockedByOtherJob = !!kind && !running;
    var runningTitle = 'Cancel expand search';
    btn.disabled = blockedByOtherJob || btn.disabled;
    if (btn.classList && btn.classList.toggle) {
        btn.classList.toggle('search-expand-running', running);
    }
    if (btn.setAttribute) {
        btn.setAttribute('aria-label', running ? runningTitle : 'Expand matches');
        btn.setAttribute('title', running ? runningTitle : 'Expand matches');
    }
    btn.title = running ? runningTitle : 'Expand matches';
    var visualState = running ? 'running' : 'idle';
    var previousState = btn.getAttribute ? btn.getAttribute('data-search-expand-state') : btn.searchExpandState;
    if (previousState === visualState) return;
    if (btn.setAttribute) btn.setAttribute('data-search-expand-state', visualState);
    btn.searchExpandState = visualState;
    btn.innerHTML = running
        ? '<span class="diff-spinner search-global-spinner" aria-hidden="true"></span>'
        : traceIconSvg('stack');
}

function updateSearchStatus(matchCount) {
    matchCount = normalizeSearchCount(matchCount);
    var infoEl = document.getElementById('search-info');
    var prevBtn = document.getElementById('search-prev-button');
    var nextBtn = document.getElementById('search-next-button');
    var expandBtn = document.getElementById('search-expand-button');
    var clearBtn = document.getElementById('search-clear-button');
    var wrap = document.querySelector ? document.querySelector('.search-wrap') : null;
    var hasQuery = !!currentSearchQuery();
    var globalSummary = hasQuery ? currentGlobalSearchSummaryForStatus() : null;
    var globalMatchCount = globalSummary ? normalizeSearchCount(globalSummary.totalCount) : 0;
    var hasGlobalExactCount = !!globalSummary;
    var globalJobActive = !!activeGlobalSearchSummaryJob();
    var hasMatches = matchCount > 0;
    var staleForNavigation = !!searchRuntime().searchResultStaleForNavigation;
    var resultState = searchRuntime().searchResultState;
    var partialVisibleResults = resultState === 'partial-visible';
    var partialResults = resultState === 'partial' || partialVisibleResults;
    var hasNavigableMatches = hasMatches && !staleForNavigation &&
        (!partialResults || partialVisibleResults);
    var hasExpandableMatches = currentSearchExpandableMatchCount(matchCount) > 0 &&
        !staleForNavigation &&
        !partialResults;
    if (!hasExpandableMatches && hasQuery && !staleForNavigation) {
        hasExpandableMatches = globalJobActive ||
            (hasGlobalExactCount && globalMatchCount > 0) ||
            (partialVisibleResults && searchScopeUsesBodyPayloads(currentSearchScope()));
    }

    if (wrap) wrap.classList.toggle('search-has-query', hasQuery);
    updateGlobalSearchButtonState(hasQuery);
    if (prevBtn) prevBtn.disabled = !hasNavigableMatches;
    if (nextBtn) nextBtn.disabled = !hasNavigableMatches;
    if (expandBtn) expandBtn.disabled = !hasExpandableMatches;
    if (clearBtn) clearBtn.disabled = !hasQuery;
    updateSearchExpandButtonState(hasQuery);

    if (!infoEl) return;
    if (!hasQuery) {
        setSearchInfo(infoEl, '', '');
    } else if (staleForNavigation) {
        setSearchInfo(infoEl, 'updating', 'Search results are updating');
    } else {
        var pendingActivation = currentPendingSearchActivationForStatus(matchCount);
        if (pendingActivation) {
            var pendingOrdinal = normalizeSearchCount(pendingActivation.index + 1);
            var pendingTotal = matchCount;
            if (hasGlobalExactCount) {
                var pendingGlobalOrdinal = globalSearchOrdinalForLocalMatch(
                    globalSummary,
                    pendingActivation.index
                );
                if (pendingGlobalOrdinal > 0) {
                    pendingOrdinal = pendingGlobalOrdinal;
                    pendingTotal = globalMatchCount;
                }
            }
            if (!hasGlobalExactCount && partialVisibleResults &&
                    searchScopeUsesBodyPayloads(currentSearchScope())) {
                var pendingMatch = pendingActivation.match ||
                    searchRuntime().searchMatches[pendingActivation.index];
                var knownPendingOrdinal = lazySearchKnownOrdinalForMatch(
                    currentSearchQuery(),
                    currentSearchScope(),
                    pendingMatch
                );
                pendingOrdinal = Math.max(pendingOrdinal, knownPendingOrdinal || 0);
                pendingTotal = Math.max(
                    pendingTotal,
                    currentSearchExpandableMatchCount(pendingTotal)
                );
                setSearchInfo(
                    infoEl,
                    'activating ' + compactSearchNumber(pendingOrdinal) +
                        ' / ≥' + compactSearchNumber(pendingTotal) + '...',
                    'Activating match ' + exactSearchNumber(pendingOrdinal) +
                        ' / at least ' + exactSearchNumber(pendingTotal)
                );
            } else {
                setSearchInfo(
                    infoEl,
                    'activating ' + searchMatchOrdinalLabel(pendingOrdinal, pendingTotal) + '...',
                    'Activating match ' + exactSearchNumber(pendingOrdinal) + ' / ' +
                    exactSearchNumber(pendingTotal)
                );
            }
        } else if (partialResults) {
            if (hasGlobalExactCount) {
                var globalCurrent = globalSearchOrdinalForLocalMatch(
                    globalSummary,
                    searchRuntime().currentSearchMatchIndex
                );
                if (globalCurrent > 0 && globalMatchCount > 0) {
                    setSearchInfo(
                        infoEl,
                        searchMatchOrdinalLabel(globalCurrent, globalMatchCount),
                        exactSearchNumber(globalCurrent) + ' / ' +
                        exactSearchNumber(globalMatchCount) + ' matches'
                    );
                } else if (globalMatchCount > 0) {
                    setSearchInfo(
                        infoEl,
                        searchMatchCountLabel(globalMatchCount),
                        exactSearchNumber(globalMatchCount) + ' match' +
                            (globalMatchCount === 1 ? '' : 'es')
                    );
                } else {
                    setSearchInfo(infoEl, 'no matches', '');
                }
            } else {
                var partialKnownCount = Math.max(
                    matchCount,
                    currentSearchExpandableMatchCount(matchCount)
                );
                if (partialVisibleResults && searchRuntime().currentSearchMatchIndex >= 0) {
                    var activeOrdinal = lazySearchKnownOrdinalForMatch(
                        currentSearchQuery(),
                        currentSearchScope(),
                        activeSearchMatch()
                    ) || normalizeSearchCount(searchRuntime().currentSearchMatchIndex + 1);
                    setSearchInfo(
                        infoEl,
                        compactSearchNumber(activeOrdinal) + ' / ≥' + compactSearchNumber(partialKnownCount),
                        exactSearchNumber(activeOrdinal) + ' / at least ' +
                            exactSearchNumber(partialKnownCount) + ' matches'
                    );
                } else {
                    if (partialVisibleResults && partialKnownCount === 0) {
                        setSearchInfo(infoEl, 'no visible matches', 'No visible/local matches known yet');
                    } else {
                        setSearchInfo(
                            infoEl,
                            partialVisibleResults
                                ? '≥' + compactSearchNumber(partialKnownCount) + ' match' +
                                    (partialKnownCount === 1 ? '' : 'es')
                                : (hasMatches ? searchMatchCountLabel(matchCount) + '...' : 'updating'),
                            partialVisibleResults
                                ? exactSearchNumber(partialKnownCount) + ' or more visible/local match' +
                                    (partialKnownCount === 1 ? '' : 'es')
                                : (hasMatches
                                    ? exactSearchNumber(matchCount) + ' partial match' +
                                        (matchCount === 1 ? '' : 'es')
                                    : 'Search results are updating')
                        );
                    }
                }
            }
        } else if (!hasMatches) {
            if (hasGlobalExactCount && globalMatchCount > 0) {
                setSearchInfo(
                    infoEl,
                    searchMatchCountLabel(globalMatchCount),
                    exactSearchNumber(globalMatchCount) + ' match' +
                        (globalMatchCount === 1 ? '' : 'es')
                );
            } else {
                setSearchInfo(infoEl, 'no matches', '');
            }
        } else if (searchRuntime().currentSearchMatchIndex >= 0) {
            var currentMatch = normalizeSearchCount(searchRuntime().currentSearchMatchIndex + 1);
            if (hasGlobalExactCount) {
                var currentGlobalMatch = globalSearchOrdinalForLocalMatch(
                    globalSummary,
                    searchRuntime().currentSearchMatchIndex
                );
                if (currentGlobalMatch > 0 && globalMatchCount > 0) {
                    currentMatch = currentGlobalMatch;
                    matchCount = globalMatchCount;
                }
            }
            setSearchInfo(
                infoEl,
                searchMatchOrdinalLabel(currentMatch, matchCount),
                exactSearchNumber(currentMatch) + ' / ' +
                    exactSearchNumber(matchCount) + ' matches'
            );
        } else {
            if (hasGlobalExactCount && globalMatchCount > 0) matchCount = globalMatchCount;
            setSearchInfo(
                infoEl,
                searchMatchCountLabel(matchCount),
                exactSearchNumber(matchCount) + ' match' + (matchCount === 1 ? '' : 'es')
            );
        }
    }
}

function searchMarkCommitOptions(options) {
    options = options || {};
    var commitOptions = {};
    for (var key in options) {
        if (!Object.prototype.hasOwnProperty.call(options, key)) continue;
        commitOptions[key] = options[key];
    }
    if (!Object.prototype.hasOwnProperty.call(commitOptions, 'searchTransactionToken')) {
        commitOptions.searchTransactionToken = activeSearchTransactionToken();
    }
    return commitOptions;
}

function clearActiveSearchMatch(visualCtx, options) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        options = visualCtx || options || {};
        return runVisualSurfaceCommitNow(
            'clear-active-search-match',
            ['search-marks'],
            ['trace', 'search'],
            function(ctx) {
                return clearActiveSearchMatch(ctx, options);
            }
        );
    }
    var changed = false;
    var commitOptions = searchMarkCommitOptions(options);
    commitOptions.label = 'clear-active-search-match';
    commitSearchMarksSurface(visualCtx, commitOptions, function() {
        changed = clearActiveSearchMatchForCommit();
        if (changed) bumpSearchMarksDomGeneration();
    });
    return changed;
}

function clearActiveSearchMatchForCommit() {
    var htmlActive = document.querySelectorAll('mark.active-search-match');
    var svgActive = document.querySelectorAll('.svg-search-mark.active-search-match');
    var active = [];
    for (var h = 0; h < htmlActive.length; h++) active.push(htmlActive[h]);
    for (var s = 0; s < svgActive.length; s++) active.push(svgActive[s]);
    var changed = false;
    for (var i = 0; i < active.length; i++) {
        if (active[i] && active[i].isConnected === false) {
            recordVisualCommitDenied('search-marks', 'clear-active-search-match-mark', 'detached_target');
            continue;
        }
        active[i].classList.remove('active-search-match');
        changed = true;
    }
    return changed;
}

function isVisibleSearchMark(mark) {
    if (!mark) return false;
    return mark.getClientRects().length > 0;
}

function searchMarkMatchesField(mark, field) {
    if (!field) return true;
    return mark.getAttribute && mark.getAttribute('data-search-field') === field;
}

function searchMarkInInactiveChrome(mark) {
    if (!mark || !mark.closest) return false;

    var ruleCol = mark.closest('.rule-col-wrap');
    if (ruleCol) {
        var ruleCell = ruleCol.closest ? ruleCol.closest('.rule-cell') : null;
        if (ruleCell && ruleCell.classList && ruleCell.classList.contains('expanded')) {
            return true;
        }
    }

    var ruleExp = mark.closest('.rule-exp-wrap');
    if (ruleExp) {
        var collapsedRuleCell = ruleExp.closest ? ruleExp.closest('.rule-cell') : null;
        if (collapsedRuleCell && collapsedRuleCell.classList &&
            collapsedRuleCell.classList.contains('collapsed')) {
            return true;
        }
    }

    var groupCol = mark.closest('.group-col-wrap');
    if (groupCol) {
        var groupCell = groupCol.closest ? groupCol.closest('.group-cell') : null;
        if (groupCell && groupCell.classList && groupCell.classList.contains('g-expanded')) {
            return true;
        }
    }

    var groupExp = mark.closest('.group-exp-wrap');
    if (groupExp) {
        var collapsedGroupCell = groupExp.closest ? groupExp.closest('.group-cell') : null;
        if (collapsedGroupCell && collapsedGroupCell.classList &&
            collapsedGroupCell.classList.contains('g-collapsed')) {
            return true;
        }
    }

    return false;
}

function refreshSearchMatches(preserveActive) {
    if (!preserveActive) {
        clearPendingSearchActivation();
        clearSearchNavigationMatchIndex();
        clearActiveSearchMatch();
        searchRuntime().currentSearchMatchIndex = -1;
        searchRuntime().activeSearchMatchRecord = null;
    } else if (searchRuntime().currentSearchMatchIndex >= searchRuntime().searchMatches.length) {
        clearPendingSearchActivation();
        clearSearchNavigationMatchIndex();
        searchRuntime().currentSearchMatchIndex = -1;
        searchRuntime().activeSearchMatchRecord = null;
        clearActiveSearchMatch();
    } else if (currentSearchNavigationMatchIndex() < 0) {
        clearSearchNavigationMatchIndex();
    }
    updateSearchStatus(searchRuntime().searchMatches.length);
}

function replaceSearchMatches(indexedMatches, preserveActive, options) {
    options = options || {};
    var previousPending = preserveActive &&
        pendingSearchActivationCurrent(searchRuntime().pendingSearchActivation)
        ? searchRuntime().pendingSearchActivation
        : null;
    var previousPendingRecord = previousPending
        ? (previousPending.matchRecord || searchMatchStableRecord(previousPending.match))
        : null;
    var previousActive = preserveActive ? activeSearchMatch() : null;
    var previousActiveRecord = preserveActive
        ? (previousActive || searchRuntime().activeSearchMatchRecord)
        : null;
    var previousNavigation = preserveActive ? searchNavigationMatch() : null;
    clearPendingSearchActivation();
    clearSearchNavigationMatchIndex();
    clearActiveSearchMatch();
    searchRuntime().searchMatches = indexedMatches || [];
    searchRuntime().currentSearchMatchIndex = -1;
    searchRuntime().activeSearchMatchRecord = null;

    if (previousActiveRecord) {
        searchRuntime().currentSearchMatchIndex = findSearchMatchIndex(previousActiveRecord);
        if (searchRuntime().currentSearchMatchIndex >= 0) {
            setActiveSearchMatchRecord(activeSearchMatch());
        } else {
            searchRuntime().activeSearchMatchRecord = searchMatchStableRecord(previousActiveRecord);
        }
    }
    if (previousNavigation) {
        setSearchNavigationMatchIndex(findSearchMatchIndex(previousNavigation), previousNavigation);
    }
    if (currentSearchNavigationMatchIndex() < 0 && previousPendingRecord) {
        var previousPendingIndex = findSearchMatchIndexByStableRecord(previousPendingRecord);
        if (previousPendingIndex >= 0) {
            setSearchNavigationMatchIndex(
                previousPendingIndex,
                searchRuntime().searchMatches[previousPendingIndex]
            );
        }
    }
    if (currentSearchNavigationMatchIndex() < 0 && searchRuntime().currentSearchMatchIndex >= 0) {
        setSearchNavigationMatchIndex(searchRuntime().currentSearchMatchIndex, activeSearchMatch());
    }

    if (options.skipNavigationLocationCache) {
        clearSearchNavigationLocationCache();
    } else {
        refreshSearchNavigationLocationCache();
    }
    if (!options.deferStatus) updateSearchStatus(searchRuntime().searchMatches.length);
}

function traceElement() {
    if (!hasDOM()) return null;
    return document.querySelector ? document.querySelector('.trace') : null;
}

function traceViewportWidth(traceEl) {
    if (!traceEl) return 0;
    return traceEl.clientWidth || window.innerWidth || 0;
}

function maxTraceScrollLeft(traceEl) {
    if (!traceEl) return 0;
    return traceRestorableMaxScrollLeft(rebuildTraceLayoutModel(), traceEl);
}

function saveSearchViewportState() {
    if (!hasDOM()) return null;

    var traceEl = traceElement();
    return {
        traceLeft: traceEl ? (traceEl.scrollLeft || 0) : 0,
        traceTop: traceEl ? (traceEl.scrollTop || 0) : 0,
        windowX: window.pageXOffset || 0,
        windowY: window.pageYOffset || 0
    };
}

function restoreSearchViewportState(state, options) {
    options = options || {};
    if (!state || !hasDOM()) return;

    var traceEl = traceElement();
    if (traceEl) {
        traceEl.scrollTop = state.traceTop || 0;
        setTraceScrollLeft(traceEl, state.traceLeft, {
            maxScrollLeft: maxTraceScrollLeft(traceEl),
            suppressVirtualRefresh: true,
            updateVirtualRange: true,
            refreshVirtualRows: true,
            scheduleVisibleRuleRenderScan: !options.deferVisibleRuleScan,
            scheduleTraceAnchorLineUpdate: !options.deferAnchorPreview
        });
    }

    if (window.scrollTo) {
        window.scrollTo(state.windowX || 0, state.windowY || 0);
    }
}

function restoreSearchNonHorizontalViewportState(state) {
    if (!state || !hasDOM()) return;

    var traceEl = traceElement();
    if (traceEl) {
        traceEl.scrollTop = state.traceTop || 0;
    }

    if (window.scrollTo) {
        window.scrollTo(state.windowX || 0, state.windowY || 0);
    }
}

function traceViewportRange(traceEl) {
    var left = traceEl ? (traceEl.scrollLeft || 0) : 0;
    var width = traceViewportWidth(traceEl);
    return {
        left: left,
        right: left + width,
        width: width
    };
}

function traceIntervalsIntersect(a, b) {
    return !!a && !!b && a.right > b.left && a.left < b.right;
}

function stageTraceInterval(si, model) {
    model = model || getTraceLayoutModel();
    var stage = model.stages[si];
    if (!stage) return null;
    return {
        type: 'stage',
        si: si,
        left: stage.left,
        right: stage.left + stage.width,
        width: stage.width
    };
}

function groupTraceInterval(si, gi, model) {
    model = model || getTraceLayoutModel();
    var group = model.groups && model.groups[si + '-' + gi];
    if (group) {
        return {
            type: 'group',
            si: si,
            gi: gi,
            left: group.left,
            right: group.right,
            width: group.width
        };
    }
    return null;
}

function groupSearchMatchInterval(si, gi, model) {
    var group = groupTraceInterval(si, gi, model);
    if (!group) return null;

    var width = effectiveGroupOpen(si, gi)
        ? Math.min(group.width, GROUP_SEARCH_TITLE_WIDTH)
        : group.width;
    return {
        type: 'group',
        si: si,
        gi: gi,
        left: group.left,
        right: group.left + width,
        width: width
    };
}

function copySearchMatchTraceInterval(interval) {
    if (!interval) return null;
    var copy = {
        left: interval.left,
        right: interval.right,
        width: interval.width
    };
    if (interval.type !== undefined) copy.type = interval.type;
    if (interval.kind !== undefined) copy.kind = interval.kind;
    if (interval.si !== undefined) copy.si = interval.si;
    if (interval.gi !== undefined) copy.gi = interval.gi;
    if (interval.ri !== undefined) copy.ri = interval.ri;
    return copy;
}

function rawSearchMatchTraceInterval(match, model) {
    if (!match) return null;
    model = model || getTraceLayoutModel();

    if (match.type === 'stage') {
        return stageTraceInterval(match.si, model);
    }

    if (match.type === 'group') {
        return groupSearchMatchInterval(match.si, match.gi, model);
    }

    if (match.type === 'rule') {
        var rule = model.rules[ruleKey(match.si, match.gi, match.ri)];
        if (!rule) {
            var hiddenTarget = typeof collapsedSearchIndicatorTarget === 'function'
                ? collapsedSearchIndicatorTarget(match)
                : null;
            return typeof collapsedSearchIndicatorInterval === 'function'
                ? collapsedSearchIndicatorInterval(hiddenTarget, model)
                : null;
        }
        return {
            type: 'rule',
            si: match.si,
            gi: match.gi,
            ri: match.ri,
            left: rule.left,
            right: rule.right,
            width: rule.width
        };
    }

    return null;
}

function clearSearchNavigationLocationCache() {
    searchRuntime().searchNavigationLocationCache = null;
}

function searchNavigationLayoutModelCacheable() {
    return !!virtualRuntime().traceLayoutModel &&
        !(virtualRuntime().traceLayoutDirtyRegions || []).length;
}

function searchNavigationLocationCacheCurrent(cache) {
    var search = searchRuntime();
    var store = currentTraceStore();
    return !!cache &&
        searchNavigationLayoutModelCacheable() &&
        cache.matches === search.searchMatches &&
        cache.traceGeneration === (Number(store && store.traceGeneration) || 0) &&
        cache.layoutGeneration === currentSearchLayoutGeneration();
}

function refreshSearchNavigationLocationCache() {
    var search = searchRuntime();
    var matches = search.searchMatches || [];
    var model = virtualRuntime().traceLayoutModel;
    if (!searchNavigationLayoutModelCacheable() || !matches.length) {
        clearSearchNavigationLocationCache();
        return null;
    }

    var cache = {
        matches: matches,
        traceGeneration: Number(currentTraceStore().traceGeneration) || 0,
        layoutGeneration: currentSearchLayoutGeneration(),
        totalWidth: Number(model.totalWidth) || 0,
        intervals: [],
        keyIndex: {}
    };

    for (var i = 0; i < matches.length; i++) {
        var interval = rawSearchMatchTraceInterval(matches[i], model);
        cache.intervals[i] = copySearchMatchTraceInterval(interval);
        var key = searchMatchStableKey(matches[i]);
        if (key && cache.keyIndex[key] === undefined) cache.keyIndex[key] = i;
    }

    search.searchNavigationLocationCache = cache;
    return cache;
}

function currentSearchNavigationLocationCache() {
    var search = searchRuntime();
    if (searchNavigationLocationCacheCurrent(search.searchNavigationLocationCache)) {
        return search.searchNavigationLocationCache;
    }
    return refreshSearchNavigationLocationCache();
}

function cachedSearchMatchTraceInterval(match) {
    var cache = currentSearchNavigationLocationCache();
    if (!cache || !match) return null;

    var key = searchMatchStableKey(match);
    var index = key ? cache.keyIndex[key] : undefined;
    if (index === undefined) return null;
    return copySearchMatchTraceInterval(cache.intervals[index]);
}

function searchMatchTraceInterval(match, model) {
    if (model) return rawSearchMatchTraceInterval(match, model);
    return cachedSearchMatchTraceInterval(match) || rawSearchMatchTraceInterval(match);
}

function searchMatchIntersectsTraceViewport(match) {
    var traceEl = traceElement();
    if (!traceEl) return true;

    return traceIntervalsIntersect(
        searchMatchTraceInterval(match),
        traceViewportRange(traceEl)
    );
}

function searchMatchIntervalViewportDistance(interval, range) {
    if (!interval) return Infinity;
    if (traceIntervalsIntersect(interval, range)) return 0;
    if (interval.right <= range.left) return range.left - interval.right;
    return interval.left - range.right;
}

function searchNavigationIntervalAt(index, cache) {
    if (cache && cache.intervals) return cache.intervals[index] || null;
    return searchMatchTraceInterval(searchRuntime().searchMatches[index]);
}

function searchMatchVisualPriority(match) {
    if (!match) return 0;
    if (match.type === 'stage') return 3;
    if (match.type === 'group') return 2;
    return 1;
}

function betterVisibleBeginningSearchMatch(candidate, best) {
    if (!candidate) return best;
    if (!best) return candidate;
    if (candidate.interval.left < best.interval.left - VIEWPORT_ANCHOR_EPSILON) return candidate;
    if (candidate.interval.left > best.interval.left + VIEWPORT_ANCHOR_EPSILON) return best;
    if (candidate.priority > best.priority) return candidate;
    if (candidate.priority < best.priority) return best;
    return candidate.index < best.index ? candidate : best;
}

function closestSearchMatchIndex() {
    var matches = searchRuntime().searchMatches;
    var traceEl = traceElement();
    if (!traceEl) return matches.length ? 0 : -1;

    var range = traceViewportRange(traceEl);
    var cache = currentSearchNavigationLocationCache();
    var bestVisibleBeginning = null;
    var bestIndex = -1;
    var bestDistance = Infinity;
    for (var i = 0; i < matches.length; i++) {
        var interval = searchNavigationIntervalAt(i, cache);
        if (interval &&
                interval.left >= range.left - VIEWPORT_ANCHOR_EPSILON &&
                interval.left < range.right) {
            bestVisibleBeginning = betterVisibleBeginningSearchMatch({
                index: i,
                interval: interval,
                priority: searchMatchVisualPriority(matches[i])
            }, bestVisibleBeginning);
        }

        var distance = searchMatchIntervalViewportDistance(interval, range);
        if (distance < bestDistance) {
            bestDistance = distance;
            bestIndex = i;
        }
    }
    return bestVisibleBeginning ? bestVisibleBeginning.index : bestIndex;
}

function normalizeSearchMatchIndexValue(index, count) {
    count = normalizeSearchCount(count === undefined ? searchRuntime().searchMatches.length : count);
    index = Math.floor(Number(index));
    return Number.isFinite(index) && index >= 0 && index < count ? index : -1;
}

function clearSearchNavigationMatchIndex() {
    searchRuntime().searchNavigationMatchIndex = -1;
}

function currentSearchNavigationMatchIndex(count) {
    return normalizeSearchMatchIndexValue(searchRuntime().searchNavigationMatchIndex, count);
}

function setSearchNavigationMatchIndex(index, match) {
    var normalized = normalizeSearchMatchIndexValue(index);
    if (normalized < 0) {
        clearSearchNavigationMatchIndex();
        return -1;
    }
    if (match && !sameSearchMatch(searchRuntime().searchMatches[normalized], match)) {
        clearSearchNavigationMatchIndex();
        return -1;
    }
    searchRuntime().searchNavigationMatchIndex = normalized;
    return normalized;
}

function searchNavigationMatch() {
    var index = currentSearchNavigationMatchIndex();
    return index >= 0 ? searchRuntime().searchMatches[index] : null;
}

function searchNavigationIndex(direction, count) {
    if (!count) return -1;
    var pendingActivation = searchRuntime().pendingSearchActivation;
    if (pendingSearchActivationCurrent(pendingActivation)) {
        return (pendingActivation.index + direction + count) % count;
    }
    var navigationIndex = currentSearchNavigationMatchIndex(count);
    if (navigationIndex >= 0) {
        return (navigationIndex + direction + count) % count;
    }
    if (searchRuntime().currentSearchMatchIndex < 0) {
        var index = closestSearchMatchIndex();
        if (index < 0) return direction < 0 ? count - 1 : 0;
        return index;
    }
    return (searchRuntime().currentSearchMatchIndex + direction + count) % count;
}

var SEARCH_RENDERED_TARGET_RESOLVED = 'resolved';
var SEARCH_RENDERED_TARGET_MATERIALIZE_NEEDED = 'materialize-needed';
var SEARCH_RENDERED_TARGET_FAILED = 'failed';

function searchMatchHasTreePath(match) {
    return !!(match && match.type === 'rule' &&
        match.path !== undefined &&
        match.field === 'label');
}

function searchMatchTreePathPid(match) {
    if (!searchMatchHasTreePath(match)) return '';
    if (Array.isArray(match.path)) return match.path.length ? '0-' + match.path.join('-') : '0';
    return typeof match.path === 'string' ? (match.path ? '0-' + match.path.replace(/\./g, '-') : '0') : '';
}

function searchMatchRuleDomKeys(match) {
    if (!match || match.type !== 'rule') return [];
    var base = ruleKey(match.si, match.gi, match.ri);
    return isFullscreenRule(match.si, match.gi, match.ri)
        ? ['fs-' + base, base]
        : [base, 'fs-' + base];
}

function elementIsInsideRoot(el, root) {
    if (!el || !root) return false;
    if (el === root) return true;
    return !root.contains || root.contains(el);
}

function searchTreeContainerForRoot(root) {
    if (!root) return null;
    if (root.classList && root.classList.contains &&
            root.classList.contains('rule-tree-wrap')) {
        return root;
    }
    return root.querySelector ? root.querySelector('.rule-tree-wrap') : null;
}

function searchTreeMaterializerForRoot(root) {
    if (typeof treeMaterializerForContainer !== 'function') return null;
    return treeMaterializerForContainer(searchTreeContainerForRoot(root));
}

function searchTreePathElementFromMaterializer(root, match, materializer) {
    materializer = materializer || searchTreeMaterializerForRoot(root);
    var range = searchMatchMaterializerRange(match, materializer);
    if (!range || range.status !== 'resolved' || !materializer || !materializer.state) {
        return null;
    }
    if (typeof materializer.ensureRowsMounted === 'function') {
        var mounted = materializer.ensureRowsMounted(range);
        if (!mounted || mounted.status !== 'resolved') return null;
    }
    var row = materializer.state.rows && materializer.state.rows[range.start];
    var pid = row && row.pid || searchMatchTreePathPid(match);
    if (!pid) return null;
    var keys = searchMatchRuleDomKeys(match);
    for (var i = 0; i < keys.length; i++) {
        var targetId = 'tn-' + keys[i] + '-' + pid;
        var el = document.getElementById(targetId);
        if (elementIsInsideRoot(el, root)) return el;
    }
    return null;
}

function treePathElementForSearchMatch(root, match) {
    if (!root || !hasDOM() || !document.getElementById) return null;
    var pid = searchMatchTreePathPid(match);
    if (!pid) return null;

    var materializer = searchTreeMaterializerForRoot(root);
    if (materializer && materializer.state) {
        return searchTreePathElementFromMaterializer(root, match, materializer);
    }

    var keys = searchMatchRuleDomKeys(match);
    for (var i = 0; i < keys.length; i++) {
        var nodeId = 'tn-' + keys[i] + '-' + pid;
        var el = document.getElementById(nodeId);
        if (elementIsInsideRoot(el, root)) return el;
    }
    return null;
}

function collectSearchMarksInRoot(root, field, match) {
    if (!root || !root.querySelectorAll) return [];
    var htmlNodes = root.querySelectorAll('mark');
    var svgNodes = root.querySelectorAll('.svg-search-mark');
    var nodes = [];
    for (var h = 0; h < htmlNodes.length; h++) nodes.push(htmlNodes[h]);
    for (var s = 0; s < svgNodes.length; s++) nodes.push(svgNodes[s]);
    var marks = [];
    var expectedMatchId = match ? searchMatchRenderedMatchId(match) : '';
    for (var i = 0; i < nodes.length; i++) {
        if (expectedMatchId &&
                (!nodes[i].getAttribute ||
                    nodes[i].getAttribute('data-search-match-id') !== expectedMatchId)) {
            continue;
        }
        if (searchMarkMatchesField(nodes[i], field) && !searchMarkInInactiveChrome(nodes[i])) {
            marks.push(nodes[i]);
        }
    }
    return marks;
}

function emptySearchRenderedMarkSet(reason) {
    return {
        marks: [],
        pathScoped: false,
        status: SEARCH_RENDERED_TARGET_FAILED,
        reason: reason || ''
    };
}

function finalizeSearchRenderedMarkSet(result, marks, emptyReason) {
    result.marks = marks || [];
    if (result.marks.length) {
        result.status = SEARCH_RENDERED_TARGET_RESOLVED;
        result.reason = '';
    } else {
        result.status = SEARCH_RENDERED_TARGET_FAILED;
        result.reason = emptyReason || 'no-rendered-marks';
    }
    return result;
}

function searchMarksForSearchMatch(match) {
    var result = emptySearchRenderedMarkSet('');
    if (typeof defaultMarksForSearchMatch === 'function' &&
            marksForSearchMatch !== defaultMarksForSearchMatch) {
        return finalizeSearchRenderedMarkSet(
            result,
            marksForSearchMatch(match) || [],
            'custom-mark-provider-empty'
        );
    }
    if (!match) return emptySearchRenderedMarkSet('invalid-match');
    if (!hasDOM()) return emptySearchRenderedMarkSet('dom-unavailable');

    var root = null;
    if (match.type === 'rule') {
        if (isFullscreenRule(match.si, match.gi, match.ri)) {
            root = document.getElementById('fullscreen-rule');
        }
        if (!root) root = document.getElementById('rule-' + ruleKey(match.si, match.gi, match.ri));
    } else if (match.type === 'group') {
        root = document.getElementById('group-' + groupSearchIndicatorKey(match.si, match.gi));
    } else if (match.type === 'stage') {
        var expanded = document.getElementById('stage-exp-' + match.si);
        var collapsed = document.getElementById('stage-col-' + match.si);
        var expandedVisible = expanded && expanded.getClientRects && expanded.getClientRects().length > 0;
        var collapsedVisible = collapsed && collapsed.getClientRects && collapsed.getClientRects().length > 0;
        root = expandedVisible ? expanded : (collapsedVisible ? collapsed : (expanded || collapsed));
    }
    if (!root) return emptySearchRenderedMarkSet('target-root-missing');

    if (searchMatchHasTreePath(match)) {
        var pathRoot = treePathElementForSearchMatch(root, match);
        result.pathScoped = true;
        if (!pathRoot) {
            result.status = SEARCH_RENDERED_TARGET_MATERIALIZE_NEEDED;
            result.reason = 'tree-path-not-mounted';
            return result;
        }
        root = pathRoot;
    }

    return finalizeSearchRenderedMarkSet(
        result,
        collectSearchMarksInRoot(root, match.field, match),
        result.pathScoped ? 'tree-path-has-no-rendered-mark' : 'no-rendered-marks'
    );
}

function marksForSearchMatch(match) {
    return searchMarksForSearchMatch(match).marks;
}

var defaultMarksForSearchMatch = marksForSearchMatch;

function searchRenderedTargetResolution(status, reason, match, markSet, mark, occurrence) {
    markSet = markSet || emptySearchRenderedMarkSet(reason);
    return {
        status: status,
        reason: reason || '',
        match: match || null,
        markSet: markSet,
        marks: markSet.marks || [],
        mark: mark || null,
        occurrence: Number.isFinite(Number(occurrence)) ? Number(occurrence) : -1,
        pathScoped: !!markSet.pathScoped
    };
}

function resolveSearchMatchRenderedTargetWithMarkSet(match, markSet) {
    markSet = markSet || searchMarksForSearchMatch(match);
    if (markSet.status === SEARCH_RENDERED_TARGET_MATERIALIZE_NEEDED) {
        return searchRenderedTargetResolution(
            SEARCH_RENDERED_TARGET_MATERIALIZE_NEEDED,
            markSet.reason || 'materialize-needed',
            match,
            markSet,
            null,
            -1
        );
    }
    if (markSet.status !== SEARCH_RENDERED_TARGET_RESOLVED) {
        return searchRenderedTargetResolution(
            SEARCH_RENDERED_TARGET_FAILED,
            markSet.reason || 'target-not-rendered',
            match,
            markSet,
            null,
            -1
        );
    }

    var marks = markSet.marks || [];
    var occurrence = 0;
    return searchRenderedTargetResolution(
        SEARCH_RENDERED_TARGET_RESOLVED,
        '',
        match,
        markSet,
        marks[occurrence],
        occurrence
    );
}

function resolveSearchMatchRenderedTarget(match) {
    return resolveSearchMatchRenderedTargetWithMarkSet(match, searchMarksForSearchMatch(match));
}

function searchTreeContainerForMatch(match) {
    if (!match || match.type !== 'rule' || !hasDOM() || !document.getElementById) return null;
    var root = null;
    if (isFullscreenRule(match.si, match.gi, match.ri)) {
        root = document.getElementById('fullscreen-rule');
    }
    if (!root) root = document.getElementById('rule-' + ruleKey(match.si, match.gi, match.ri));
    return root && root.querySelector ? root.querySelector('.rule-tree-wrap') : null;
}

function searchTreeMaterializerForMatch(match) {
    if (typeof treeMaterializerForContainer !== 'function') return null;
    return treeMaterializerForContainer(searchTreeContainerForMatch(match));
}

function searchMatchMaterializerRange(match, materializer) {
    if (!searchMatchHasTreePath(match) ||
            !materializer ||
            !materializer.state ||
            typeof rowRangeForTarget !== 'function') {
        return null;
    }
    var pid = searchMatchTreePathPid(match);
    if (!pid) return null;
    return rowRangeForTarget(materializer.state.rows || [], { type: 'node', pid: pid });
}

function materializeSearchMatchWithMaterializer(match) {
    var materializer = searchTreeMaterializerForMatch(match);
    var range = searchMatchMaterializerRange(match, materializer);
    if (!range || range.status !== 'resolved' ||
            !materializer || typeof materializer.scrollRowRangeIntoView !== 'function') {
        return false;
    }
    var scroll = materializer.scrollRowRangeIntoView(range, { block: 'start' });
    return !!(scroll && scroll.status === 'resolved');
}

function materializeSearchMatchRenderedTarget(match, resolution) {
    if (!resolution ||
            resolution.status !== SEARCH_RENDERED_TARGET_MATERIALIZE_NEEDED ||
            !searchMatchHasTreePath(match)) {
        return false;
    }
    return materializeSearchMatchWithMaterializer(match);
}

function visibleSearchMarkResolution(match, resolution) {
    if (!resolution || resolution.status !== SEARCH_RENDERED_TARGET_RESOLVED) return resolution;
    if (isVisibleSearchMark(resolution.mark)) return resolution;
    if (!searchMatchHasTreePath(match)) return resolution;
    if (!materializeSearchMatchWithMaterializer(match)) return resolution;
    return resolveSearchMatchRenderedTarget(match);
}

function clampScrollValue(value, max) {
    if (!Number.isFinite(value)) return 0;
    if (!Number.isFinite(max) || max <= 0) return 0;
    return Math.max(0, Math.min(max, value));
}

function scrollElementWithinContainer(el, container, padding) {
    if (!el || !container ||
        !el.getBoundingClientRect || !container.getBoundingClientRect) {
        return;
    }

    padding = padding || 0;
    var elRect = el.getBoundingClientRect();
    var containerRect = container.getBoundingClientRect();
    var nextLeft = container.scrollLeft || 0;
    var nextTop = container.scrollTop || 0;

    if (elRect.left < containerRect.left + padding) {
        nextLeft -= containerRect.left + padding - elRect.left;
    } else if (elRect.right > containerRect.right - padding) {
        nextLeft += elRect.right - (containerRect.right - padding);
    }

    if (elRect.top < containerRect.top + padding) {
        nextTop -= containerRect.top + padding - elRect.top;
    } else if (elRect.bottom > containerRect.bottom - padding) {
        nextTop += elRect.bottom - (containerRect.bottom - padding);
    }

    container.scrollLeft = clampScrollValue(
        nextLeft,
        (container.scrollWidth || 0) - (container.clientWidth || 0)
    );
    container.scrollTop = clampScrollValue(
        nextTop,
        (container.scrollHeight || 0) - (container.clientHeight || 0)
    );
}

function scrollSearchMarkIntoView(mark) {
    if (!mark || !mark.closest) return;

    var innerScroll = mark.closest('.info-tab-panel') ||
        mark.closest('.rule-tree-wrap, .rule-info-panel');
    if (innerScroll) {
        scrollElementWithinContainer(mark, innerScroll, 12);
    }
}

function searchNavigationTotalWidth() {
    var cache = currentSearchNavigationLocationCache();
    if (cache && cache.totalWidth > 0) return cache.totalWidth;
    return getTraceLayoutModel().totalWidth || 0;
}

function scrollSearchMatchIntoView(match) {
    if (!hasDOM()) return false;

    var traceEl = traceElement();
    var interval = searchMatchTraceInterval(match);
    if (!traceEl || !interval) return false;

    var viewport = traceViewportWidth(traceEl);
    var target = match && match.type === 'stage'
        ? interval.left
        : interval.left + interval.width / 2 - viewport / 2;
    setTraceScrollLeft(traceEl, target, {
        maxScrollLeft: searchNavigationTotalWidth() - viewport,
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: true
    });
    return true;
}

function activateRenderedSearchMark(visualCtx, match, options) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        options = match;
        match = visualCtx;
        return runVisualSurfaceCommitNow(
            'activate-search-mark',
            ['search-marks'],
            ['trace', 'search'],
            function(ctx) {
                return activateRenderedSearchMark(ctx, match, options);
            }
        );
    }

    var activated = false;
    var commitOptions = searchMarkCommitOptions(options);
    commitOptions.label = 'activate-search-mark';
    commitSearchMarksSurface(visualCtx, commitOptions, function() {
        var activation = options && options.activation;
        if (activation &&
                !pendingSearchActivationCurrent(activation, match) &&
                !searchActivationIsCommitted(activation, match)) {
            return;
        }
        var changed = false;
        if (!activation) changed = clearActiveSearchMatchForCommit();
        var resolution = resolveSearchMatchRenderedTarget(match);
        if (resolution.status === SEARCH_RENDERED_TARGET_MATERIALIZE_NEEDED &&
                materializeSearchMatchRenderedTarget(match, resolution)) {
            resolution = resolveSearchMatchRenderedTarget(match);
        }
        if (resolution.status !== SEARCH_RENDERED_TARGET_RESOLVED) {
            if (changed) bumpSearchMarksDomGeneration();
            return;
        }

        var mark = resolution.mark;
        var preserveInnerScroll = options && options.preserveInnerScroll;
        if (!preserveInnerScroll) {
            resolution = visibleSearchMarkResolution(match, resolution);
            mark = resolution.mark;
        }
        if (!isVisibleSearchMark(mark)) {
            if (changed) bumpSearchMarksDomGeneration();
            return;
        }
        if (mark && mark.isConnected === false) {
            recordVisualCommitDenied('search-marks', 'activate-search-mark-target', 'detached_target', {
                type: match && match.type || '',
                field: match && match.field || ''
            });
            if (changed) bumpSearchMarksDomGeneration();
            return;
        }

        if (activation) changed = clearActiveSearchMatchForCommit();
        mark.classList.add('active-search-match');
        bumpSearchMarksDomGeneration();
        if (!preserveInnerScroll) scrollSearchMarkIntoView(mark);
        if (activation && !commitSearchActivationCandidate(activation, match)) {
            mark.classList.remove('active-search-match');
            bumpSearchMarksDomGeneration();
            return;
        }
        activated = true;
    });
    return activated;
}

function sameSearchMatch(a, b) {
    if (!a || !b) return false;
    var aKey = searchMatchStableKey(a);
    var bKey = searchMatchStableKey(b);
    if (aKey && bKey) return aKey === bKey;
    return a.type === b.type &&
           a.si === b.si &&
           a.gi === b.gi &&
           a.ri === b.ri &&
           a.field === b.field &&
           a.occurrence === b.occurrence;
}

function findSearchMatchIndex(match) {
    if (!match) return -1;
    for (var i = 0; i < searchRuntime().searchMatches.length; i++) {
        if (sameSearchMatch(searchRuntime().searchMatches[i], match)) return i;
    }
    return -1;
}

function findSearchMatchIndexByStableRecord(record) {
    if (!record) return -1;
    var key = searchMatchStableKey(record);
    if (!key) return -1;
    for (var i = 0; i < searchRuntime().searchMatches.length; i++) {
        if (searchMatchStableKey(searchRuntime().searchMatches[i]) === key) return i;
    }
    return -1;
}

function searchMatchStableRecord(match) {
    if (!match) return null;
    var store = currentTraceStore();
    var occurrence = match.recordOccurrence !== undefined
        ? match.recordOccurrence
        : (match.occurrence || 0);
    var record = {
        type: match.type,
        field: match.field,
        occurrence: occurrence,
        traceGeneration: Number(store && store.traceGeneration) || 0
    };

    if (match.si !== undefined) record.si = match.si;
    if (match.gi !== undefined) record.gi = match.gi;
    if (match.ri !== undefined) record.ri = match.ri;
    if (match.rawIdx !== undefined) record.rawIdx = match.rawIdx;
    if (match.occurrenceBase !== undefined) record.occurrenceBase = match.occurrenceBase;
    if (match.fieldOrdinal !== undefined) record.fieldOrdinal = match.fieldOrdinal;
    if (match.recordOccurrence !== undefined) record.recordOccurrence = match.recordOccurrence;
    if (match.path !== undefined) record.path = match.path;

    if (match.type === 'stage') {
        record.stageHandle = TraceStore.stageHandle(store, match.si);
    } else if (match.type === 'group') {
        record.stageHandle = TraceStore.stageHandle(store, match.si);
        record.groupHandle = TraceStore.groupHandle(store, match.si, match.gi);
    } else if (match.type === 'rule') {
        var rawIdx = match.rawIdx !== undefined
            ? match.rawIdx
            : rawRuleIndex(match.si, match.gi, match.ri);
        record.rawIdx = rawIdx;
        record.stageHandle = TraceStore.stageHandle(store, match.si);
        record.groupHandle = TraceStore.groupHandle(store, match.si, match.gi);
        record.ruleHandle = TraceStore.ruleHandle(store, match.si, rawIdx);
    }
    return record;
}

function setActiveSearchMatchRecord(match) {
    searchRuntime().activeSearchMatchRecord = searchMatchStableRecord(match);
}

function activeSearchMatch() {
    return searchRuntime().currentSearchMatchIndex >= 0 ? searchRuntime().searchMatches[searchRuntime().currentSearchMatchIndex] : null;
}

function clearPendingSearchActivation(activation) {
    var pending = searchRuntime().pendingSearchActivation;
    if (!pending) return false;
    if (activation && pending !== activation && pending.id !== activation.id) return false;
    searchRuntime().pendingSearchActivation = null;
    return true;
}

function searchActivationIndexValid(activation) {
    if (!activation) return false;
    var index = Math.floor(Number(activation.index));
    return Number.isFinite(index) &&
        index >= 0 &&
        index < searchRuntime().searchMatches.length;
}

function searchActivationMatchAtIndex(activation) {
    if (!searchActivationIndexValid(activation)) return null;
    return searchRuntime().searchMatches[activation.index] || null;
}

function pendingSearchActivationCurrent(activation, match) {
    var pending = searchRuntime().pendingSearchActivation;
    if (!activation || !pending || pending.id !== activation.id) return false;
    var indexedMatch = searchActivationMatchAtIndex(pending);
    if (!indexedMatch || !sameSearchMatch(indexedMatch, pending.match)) return false;
    return !match || sameSearchMatch(indexedMatch, match);
}

function pendingSearchActivationMatch() {
    var pending = searchRuntime().pendingSearchActivation;
    if (!pendingSearchActivationCurrent(pending)) return null;
    return searchActivationMatchAtIndex(pending);
}

function currentPendingSearchActivationForStatus(matchCount) {
    var pending = searchRuntime().pendingSearchActivation;
    matchCount = normalizeSearchCount(matchCount);
    if (!pendingSearchActivationCurrent(pending)) return null;
    if (searchActivationIsCommitted(pending)) {
        clearPendingSearchActivation(pending);
        return null;
    }
    return pending.index < matchCount ? pending : null;
}

function beginSearchMatchActivation(index, match) {
    var count = searchRuntime().searchMatches.length;
    if (!count || !match) {
        clearPendingSearchActivation();
        return null;
    }
    index = Math.floor(Number(index));
    if (!Number.isFinite(index)) return null;
    index = ((index % count) + count) % count;
    var indexedMatch = searchRuntime().searchMatches[index];
    if (!sameSearchMatch(indexedMatch, match)) return null;

    var activation = {
        id: searchRuntime().nextSearchActivationId++,
        index: index,
        match: match,
        matchRecord: searchMatchStableRecord(match)
    };
    searchRuntime().pendingSearchActivation = activation;
    setSearchNavigationMatchIndex(index, match);
    return activation;
}

function searchActivationIsCommitted(activation, match) {
    if (!activation || !searchActivationIndexValid(activation)) return false;
    if (searchRuntime().currentSearchMatchIndex !== activation.index) return false;
    var indexedMatch = searchActivationMatchAtIndex(activation);
    return sameSearchMatch(indexedMatch, match || activation.match);
}

function commitSearchActivationCandidate(activation, match) {
    if (!activation) return false;
    if (searchActivationIsCommitted(activation, match)) {
        setSearchNavigationMatchIndex(activation.index, match || activation.match);
        clearPendingSearchActivation(activation);
        return true;
    }
    if (!pendingSearchActivationCurrent(activation, match)) return false;

    var indexedMatch = searchActivationMatchAtIndex(activation);
    if (!indexedMatch) {
        clearPendingSearchActivation(activation);
        return false;
    }

    searchRuntime().currentSearchMatchIndex = activation.index;
    setSearchNavigationMatchIndex(activation.index, indexedMatch);
    setActiveSearchMatchRecord(indexedMatch);
    clearPendingSearchActivation(activation);
    updateSearchStatus(searchRuntime().searchMatches.length);
    return true;
}

function finishSearchActivationIfActivated(activation, match, activated) {
    if (!activated) return false;
    if (!activation) return true;
    return commitSearchActivationCandidate(activation, match);
}

function activeSearchMatchIsRule(si, gi, ri) {
    var match = activeSearchMatch();
    return !!match &&
           match.type === 'rule' &&
           match.si === si &&
           match.gi === gi &&
           match.ri === ri;
}

function activeSearchMatchUsesLabels(match) {
    return !!match && (match.field === 'stage' || match.field === 'group' || match.field === 'name');
}

function reactivateActiveSearchMatchForRule(si, gi, ri) {
    var pending = searchRuntime().pendingSearchActivation;
    var pendingMatch = pendingSearchActivationMatch();
    if (pendingMatch &&
            pendingMatch.type === 'rule' &&
            pendingMatch.si === si &&
            pendingMatch.gi === gi &&
            pendingMatch.ri === ri) {
        activateRenderedSearchMark(pendingMatch, { activation: pending });
        return;
    }
    if (activeSearchMatchIsRule(si, gi, ri)) {
        activateRenderedSearchMark(activeSearchMatch());
    }
}

function reactivateActiveSearchMatchForLabels() {
    var pending = searchRuntime().pendingSearchActivation;
    var pendingMatch = pendingSearchActivationMatch();
    if (activeSearchMatchUsesLabels(pendingMatch)) {
        activateRenderedSearchMark(pendingMatch, { activation: pending });
        return;
    }
    var match = activeSearchMatch();
    if (activeSearchMatchUsesLabels(match)) {
        activateRenderedSearchMark(match);
    }
}

function searchMatchActivationStillCurrent(match, activationOptions) {
    var activation = activationOptions && activationOptions.activation;
    if (activation) return pendingSearchActivationCurrent(activation, match);
    return sameSearchMatch(match, activeSearchMatch());
}

function scheduleSearchMatchActivationRetry(match, renderMarks, attempts, activationOptions) {
    if (!attempts || attempts <= 0) {
        var exhaustedActivation = activationOptions && activationOptions.activation;
        if (exhaustedActivation &&
                pendingSearchActivationCurrent(exhaustedActivation, match)) {
            clearPendingSearchActivation(exhaustedActivation);
            updateSearchStatus(searchRuntime().searchMatches.length);
        }
        return;
    }

    var token = currentUiState().searchToken;
    var transactionToken = activeSearchTransactionToken();
    var searchMarksDomGeneration = currentSearchMarksDomGeneration();
    var virtualRowsDomGeneration = currentVirtualRowsDomGeneration();
    function clearDiscardedPendingActivation() {
        var activation = activationOptions && activationOptions.activation;
        if (activation && pendingSearchActivationCurrent(activation, match)) {
            clearPendingSearchActivation(activation);
            updateSearchStatus(searchRuntime().searchMatches.length);
        }
    }
    scheduleRenderDeferredWork(
        '',
        function runSearchMatchActivationRetry(visualCtx) {
            if (token !== currentUiState().searchToken ||
                !searchTransactionTokenCurrent(transactionToken) ||
                !searchMatchActivationStillCurrent(match, activationOptions)) {
                return;
            }
            if (!canCommitSearchMarks('search-match-activation-retry', null, {
                searchMarksDomGeneration: searchMarksDomGeneration,
                virtualRowsDomGeneration: virtualRowsDomGeneration,
                searchTransactionToken: transactionToken
            })) {
                clearDiscardedPendingActivation();
                return;
            }
            renderMarks();
            if (!finishSearchActivationIfActivated(
                    activationOptions && activationOptions.activation,
                    match,
                    activateRenderedSearchMark(match, activationOptions))) {
                scheduleSearchMatchActivationRetry(match, renderMarks, attempts - 1, activationOptions);
            }
        },
        {
            token: transactionToken
                ? searchTransactionFrameToken(transactionToken)
                : renderFrameEpochToken(['trace', 'search']),
            ownerId: transactionToken && transactionToken.ownerId || '',
            label: 'search-match-activation-retry',
            surfaces: ['search-marks', 'rule-tree', 'rule-info'],
            onDiscard: clearDiscardedPendingActivation
        }
    );
}

function renderSearchRuleMarks(match) {
    var query = currentSearchQuery();
    var scope = currentSearchScope();

    refreshVirtualRowsNow();
    rerenderRuleTree(match.si, match.gi, match.ri, query, false, scope);
    requestRuleInfoRenderIfVisible(match.si, match.gi, match.ri);
    updateSearchLabelHighlights(query, scope);
}

function renderSearchStageMarks() {
    updateSearchLabelHighlights(currentSearchQuery(), currentSearchScope());
}

function renderSearchGroupTarget(match, preserveLayout, preserveViewport, activation) {
    if (activeSearchExpandOverlay()) preserveLayout = true;
    if (!preserveLayout && !effectiveStageOpen(match.si)) {
        invalidateTraceMeasuredWidthCache();
        refreshLayoutTransition(TraceState.openStage(currentUiState(), currentTraceGroups(), match.si));
        rebuildTraceLayoutModel();
        refreshSearchNavigationLocationCache();
    }
    if (preserveViewport) {
        refreshVirtualRowsNow();
    } else {
        if (!scrollSearchMatchIntoView(match)) scrollGroupIntoView(match.si, match.gi);
    }
    var activationOptions = { preserveViewport: preserveViewport, activation: activation };
    if (tryFastActivateMountedSearchMatch(match, activationOptions)) return true;
    renderSearchStageMarks();
    if (finishSearchActivationIfActivated(
            activation,
            match,
            activateRenderedSearchMark(match, activationOptions))) {
        return true;
    }
    if (searchMatchActivationStillCurrent(match, activationOptions)) {
        scheduleSearchMatchActivationRetry(match, renderSearchStageMarks, 2, activationOptions);
    }
    return false;
}

function ensureSearchMatchDetailsVisible(match) {
}

function requestSearchTargetPayload(match) {
    if (!match || match.type !== 'rule') return;

    var store = currentTraceStore();
    var rawIdx = match.rawIdx !== undefined
        ? match.rawIdx
        : rawRuleIndex(match.si, match.gi, match.ri);
    var ruleHandleValue = TraceStore.ruleHandle(store, match.si, rawIdx);
    if (!ruleHandleValue) return;

    var bucket = TraceStore.ruleType(store, match.si, rawIdx) === 'text'
        ? 'text'
        : 'trees';
    var state = TraceStore.payloadState(store, bucket, ruleHandleValue).state;
    if (searchPayloadStateNeedsMaterialization(state)) {
        TraceStore.beginPayloadMaterialization(store, bucket, ruleHandleValue, {
            searchTransactionToken: activeSearchTransactionToken()
        });
    }
}

function searchSummaryMaskValue(name) {
    return TraceSearchWorker &&
        TraceSearchWorker.SUMMARY_FIELD_MASKS &&
        TraceSearchWorker.SUMMARY_FIELD_MASKS[name] || 0;
}

function searchMatchNeedsExpandedRule(match) {
    return !!match && match.type === 'rule' && match.field === 'label';
}

function emptySearchExpandOverlay(query, scope) {
    var search = searchRuntime();
    var intent = normalizeSearchIntent(query, scope, 'expand');
    var store = currentTraceStore();
    return {
        mode: 'expand',
        query: intent.query,
        scope: intent.scope,
        resultEpoch: search.searchResultEpoch || 0,
        traceGeneration: Number(store && store.traceGeneration) || 0,
        payloadGeneration: currentSearchPayloadGeneration(),
        matchCount: 0,
        stages: {},
        matchedGroups: {},
        groups: {},
        rules: {},
        stageSummaries: {},
        groupSummaries: {}
    };
}

function searchHashUpdate(hash, value) {
    value = String(value === undefined || value === null ? '' : value);
    for (var i = 0; i < value.length; i++) {
        hash ^= value.charCodeAt(i);
        hash = Math.imul(hash, 16777619) >>> 0;
    }
    hash ^= 31;
    return Math.imul(hash, 16777619) >>> 0;
}

function searchMatchSetSignature(matches) {
    matches = matches || [];
    var hash = 2166136261 >>> 0;
    for (var i = 0; i < matches.length; i++) {
        var match = matches[i] || {};
        hash = searchHashUpdate(hash, match.type);
        hash = searchHashUpdate(hash, match.field);
        hash = searchHashUpdate(hash, match.si);
        hash = searchHashUpdate(hash, match.gi);
        hash = searchHashUpdate(hash, match.ri);
        hash = searchHashUpdate(hash, match.rawIdx);
        hash = searchHashUpdate(hash, match.occurrence);
        hash = searchHashUpdate(hash, match.path);
        hash = searchHashUpdate(hash, match.summaryMask);
        hash = searchHashUpdate(hash, match.summaryCount);
        hash = searchHashUpdate(hash, match.summaryOrder);
    }
    return matches.length + ':' + hash.toString(16);
}

function searchExpandOverlayCacheKey(query, scope, matches) {
    ensureSearchCachesCurrent();
    return JSON.stringify([
        searchRuntime().searchCacheTraceGeneration,
        searchRuntime().searchCachePayloadGeneration,
        searchRuntime().searchCacheBaseLayoutGeneration,
        searchRuntime().searchCacheIndexMode,
        searchRuntime().searchCacheShowEmptyStages ? 1 : 0,
        normalizedSearchCacheQuery(query),
        normalizedSearchCollectionScope(scope),
        searchMatchSetSignature(matches)
    ]);
}

function cloneSearchOverlayMap(map) {
    var clone = {};
    map = map || {};
    for (var key in map) {
        if (!Object.prototype.hasOwnProperty.call(map, key)) continue;
        clone[key] = cloneSearchCacheValue(map[key]);
    }
    return clone;
}

function cloneSearchExpandOverlayForCache(overlay) {
    if (!overlay) return null;
    return {
        mode: 'expand',
        query: String(overlay.query || ''),
        scope: String(overlay.scope || ''),
        resultEpoch: Number(overlay.resultEpoch) || 0,
        traceGeneration: Number(overlay.traceGeneration) || 0,
        payloadGeneration: Number(overlay.payloadGeneration) || 0,
        matchCount: Number(overlay.matchCount) || 0,
        stages: cloneSearchOverlayMap(overlay.stages),
        matchedGroups: cloneSearchOverlayMap(overlay.matchedGroups),
        groups: cloneSearchOverlayMap(overlay.groups),
        rules: cloneSearchOverlayMap(overlay.rules),
        stageSummaries: cloneSearchOverlayMap(overlay.stageSummaries),
        groupSummaries: cloneSearchOverlayMap(overlay.groupSummaries)
    };
}

function cachedSearchExpandOverlay(query, scope, matches) {
    var search = searchRuntime();
    var key = searchExpandOverlayCacheKey(query, scope, matches);
    var entry = search.searchExpandOverlayCache[key];
    if (!entry) {
        search.searchExpandOverlayCacheMisses++;
        return null;
    }
    search.searchExpandOverlayCacheHits++;
    var overlay = cloneSearchExpandOverlayForCache(entry.overlay);
    if (overlay) overlay.resultEpoch = search.searchResultEpoch || 0;
    return overlay;
}

function rememberSearchExpandOverlay(query, scope, matches, overlay) {
    var search = searchRuntime();
    var key = searchExpandOverlayCacheKey(query, scope, matches);
    rememberBoundedSearchCache(
        search.searchExpandOverlayCache,
        search.searchExpandOverlayCacheOrder,
        key,
        { overlay: cloneSearchExpandOverlayForCache(overlay) },
        SEARCH_EXPAND_OVERLAY_CACHE_LIMIT
    );
    return overlay;
}

function buildSearchExpandOverlay(query, scope, matches) {
    var lowerQuery = String(query || '').toLowerCase();
    var overlay = emptySearchExpandOverlay(query, scope);
    matches = matches || collectSearchMatchesFromIndex(query, scope);
    for (var i = 0; i < matches.length; i++) {
        addSearchLayoutMatch(matches[i], overlay, lowerQuery, scope);
    }
    return rememberSearchExpandOverlay(query, scope, matches, overlay);
}

function ensureSearchExpandOverlayStageSummary(overlay, si) {
    var key = String(si);
    var summary = overlay.stageSummaries[key];
    if (!summary) {
        var stage = TraceStore.stageSummary(currentTraceStore(), si) || {};
        summary = {
            si: si,
            groupCount: Number(stage.groupCount) || groupCount(si),
            ruleCount: Number(stage.ruleCount) || traceStageRuleCount(si),
            openGroupCount: 0,
            matchedGroupCount: 0,
            matchedRuleCount: 0,
            openRuleCount: 0
        };
        overlay.stageSummaries[key] = summary;
    }
    return summary;
}

function ensureSearchExpandOverlayGroupSummary(overlay, si, gi) {
    var key = searchExpandOverlayGroupKey(si, gi);
    var summary = overlay.groupSummaries[key];
    if (!summary) {
        var stage = TraceStore.stageSummary(currentTraceStore(), si) || {};
        var group = stage.groups && stage.groups[gi] || {};
        summary = {
            si: si,
            gi: gi,
            ruleCount: Array.isArray(group.ruleIndices)
                ? group.ruleIndices.length
                : groupRuleCount(si, gi),
            open: false,
            matched: false,
            matchedRuleCount: 0,
            openRuleCount: 0,
            ruleIndices: [],
            ruleIndexMap: {}
        };
        overlay.groupSummaries[key] = summary;
        ensureSearchExpandOverlayStageSummary(overlay, si);
    }
    return summary;
}

function addSearchExpandOverlaySummaryRuleIndex(summary, ri) {
    if (!summary) return false;
    if (!Array.isArray(summary.ruleIndices)) summary.ruleIndices = [];
    if (!summary.ruleIndexMap || typeof summary.ruleIndexMap !== 'object') {
        summary.ruleIndexMap = {};
        for (var i = 0; i < summary.ruleIndices.length; i++) {
            summary.ruleIndexMap[String(summary.ruleIndices[i])] = true;
        }
    }

    ri = Math.floor(Number(ri));
    if (!Number.isInteger(ri) || ri < 0) return false;
    var key = String(ri);
    if (summary.ruleIndexMap[key]) return false;
    summary.ruleIndexMap[key] = true;
    summary.ruleIndices.push(ri);
    return true;
}

function markSearchExpandOverlayStage(overlay, si) {
    overlay.stages[String(si)] = true;
    ensureSearchExpandOverlayStageSummary(overlay, si);
}

function markSearchExpandOverlayGroup(overlay, si, gi) {
    var key = searchExpandOverlayGroupKey(si, gi);
    if (overlay.groups[key]) return;
    overlay.groups[key] = true;
    var summary = ensureSearchExpandOverlayGroupSummary(overlay, si, gi);
    summary.open = true;
    ensureSearchExpandOverlayStageSummary(overlay, si).openGroupCount++;
}

function markSearchExpandOverlayMatchedGroup(overlay, si, gi) {
    var key = searchExpandOverlayGroupKey(si, gi);
    if (overlay.matchedGroups[key]) return;
    overlay.matchedGroups[key] = true;
    var summary = ensureSearchExpandOverlayGroupSummary(overlay, si, gi);
    summary.matched = true;
    ensureSearchExpandOverlayStageSummary(overlay, si).matchedGroupCount++;
}

function markSearchExpandOverlayRule(overlay, match, expandRule, lowerQuery, scope) {
    var key = ruleKey(match.si, match.gi, match.ri);
    var rawIdx = match.rawIdx !== undefined
        ? match.rawIdx
        : rawRuleIndex(match.si, match.gi, match.ri);
    var existing = overlay.rules[key];
    var wasOpen = !!(existing && existing.open);
    overlay.rules[key] = {
        si: match.si,
        gi: match.gi,
        ri: match.ri,
        rawIdx: rawIdx,
        open: !!expandRule
    };
    markSearchExpandOverlayStage(overlay, match.si);
    markSearchExpandOverlayGroup(overlay, match.si, match.gi);
    var groupSummary = ensureSearchExpandOverlayGroupSummary(overlay, match.si, match.gi);
    var stageSummary = ensureSearchExpandOverlayStageSummary(overlay, match.si);
    addSearchExpandOverlaySummaryRuleIndex(groupSummary, match.ri);
    if (!existing) {
        groupSummary.matchedRuleCount++;
        stageSummary.matchedRuleCount++;
    }
    if (!wasOpen && expandRule) {
        groupSummary.openRuleCount++;
        stageSummary.openRuleCount++;
    }
}

function markSearchExpandOverlayRuleOpen(overlay, si, gi, ri) {
    if (!overlay) return false;
    if (!overlay.rules) overlay.rules = {};

    var key = searchExpandOverlayRuleKey(si, gi, ri);
    var existing = overlay.rules[key];
    var wasOpen = !!(existing && existing.open);

    markSearchExpandOverlayStage(overlay, si);
    markSearchExpandOverlayGroup(overlay, si, gi);

    if (existing) {
        existing.open = true;
    } else {
        overlay.rules[key] = {
            si: si,
            gi: gi,
            ri: ri,
            rawIdx: rawRuleIndex(si, gi, ri),
            open: true
        };
    }

    var groupSummary = ensureSearchExpandOverlayGroupSummary(overlay, si, gi);
    var stageSummary = ensureSearchExpandOverlayStageSummary(overlay, si);
    addSearchExpandOverlaySummaryRuleIndex(groupSummary, ri);
    if (!wasOpen) {
        groupSummary.openRuleCount++;
        stageSummary.openRuleCount++;
    }
    return !wasOpen;
}

function openActiveSearchExpandOverlayRule(si, gi, ri) {
    var overlay = activeSearchExpandOverlay();
    if (!overlay) return null;
    if (searchOverlayStageOpen(overlay, si) &&
            searchOverlayGroupOpen(overlay, si, gi) &&
            searchOverlayRuleOpen(overlay, si, gi, ri)) {
        return emptySearchLayoutTransitionSummary();
    }

    var previous = cloneSearchExpandOverlayForCache(overlay);
    markSearchExpandOverlayRuleOpen(overlay, si, gi, ri);
    return searchExpandOverlayTransitionSummary(previous, overlay, { sparse: true });
}

function addSearchLayoutMatch(match, overlay, lowerQuery, scope) {
    if (!match) return;
    overlay.matchCount++;
    if (match.type === 'stage') {
        ensureSearchExpandOverlayStageSummary(overlay, match.si);
    } else if (match.type === 'group') {
        markSearchExpandOverlayStage(overlay, match.si);
        markSearchExpandOverlayMatchedGroup(overlay, match.si, match.gi);
    } else if (match.type === 'rule') {
        markSearchExpandOverlayRule(
            overlay,
            match,
            searchMatchNeedsExpandedRule(match),
            lowerQuery,
            scope
        );
    }
}

function emptySearchLayoutTransitionSummary() {
    return {
        changed: false,
        stages: [],
        groups: [],
        rules: [],
        ruleDisplays: [],
        openedStages: [],
        stageWidths: [],
        groupWidths: []
    };
}

function pushSearchTransitionRef(list, keySet, key, ref) {
    if (keySet[key]) return;
    keySet[key] = true;
    list.push(ref);
}

function searchOverlayStageOpen(overlay, si) {
    if (!overlay) return !!stageState(si).open;
    return !!(overlay.stages && overlay.stages[String(si)]);
}

function searchOverlayGroupOpen(overlay, si, gi) {
    if (!overlay) return !!groupState(si, gi).open;
    if (groupRuleCount(si, gi) <= 1) return true;
    return !!(overlay.groups && overlay.groups[searchExpandOverlayGroupKey(si, gi)]);
}

function searchOverlayRuleOpen(overlay, si, gi, ri) {
    if (!overlay) return !!ruleState(si, gi, ri).open;
    var rule = overlay.rules && overlay.rules[searchExpandOverlayRuleKey(si, gi, ri)];
    return !!(rule && rule.open);
}

function parseSearchExpandOverlayRefKey(key, expectedParts) {
    var parts = String(key || '').split('-');
    if (parts.length !== expectedParts) return null;
    var ref = {
        si: Math.floor(Number(parts[0]))
    };
    if (expectedParts > 1) ref.gi = Math.floor(Number(parts[1]));
    if (expectedParts > 2) ref.ri = Math.floor(Number(parts[2]));
    if (!Number.isInteger(ref.si) || ref.si < 0 || ref.si >= currentStageCount()) return null;
    if (expectedParts > 1 &&
            (!Number.isInteger(ref.gi) || ref.gi < 0 || ref.gi >= groupCount(ref.si))) {
        return null;
    }
    if (expectedParts > 2 &&
            (!Number.isInteger(ref.ri) || ref.ri < 0 || ref.ri >= groupRuleCount(ref.si, ref.gi))) {
        return null;
    }
    return ref;
}

function collectSearchExpandOverlaySparseKeys(overlay, stageKeys, groupKeys, ruleKeys) {
    if (!overlay) return;

    function collectMapKeys(map, target) {
        if (!map) return;
        for (var key in map) {
            if (Object.prototype.hasOwnProperty.call(map, key)) target[key] = true;
        }
    }

    collectMapKeys(overlay.stages, stageKeys);
    collectMapKeys(overlay.groups, groupKeys);
    collectMapKeys(overlay.matchedGroups, groupKeys);
    collectMapKeys(overlay.groupSummaries, groupKeys);
    collectMapKeys(overlay.rules, ruleKeys);

    for (var groupKey in groupKeys) {
        if (!Object.prototype.hasOwnProperty.call(groupKeys, groupKey)) continue;
        var groupRef = parseSearchExpandOverlayRefKey(groupKey, 2);
        if (groupRef) stageKeys[String(groupRef.si)] = true;
    }
    for (var ruleKeyValue in ruleKeys) {
        if (!Object.prototype.hasOwnProperty.call(ruleKeys, ruleKeyValue)) continue;
        var ruleRef = parseSearchExpandOverlayRefKey(ruleKeyValue, 3);
        if (!ruleRef) continue;
        stageKeys[String(ruleRef.si)] = true;
        groupKeys[searchExpandOverlayGroupKey(ruleRef.si, ruleRef.gi)] = true;
    }
}

function searchExpandOverlaySparseTransitionSummary(oldOverlay, newOverlay) {
    var change = emptySearchLayoutTransitionSummary();
    var stageKeys = {};
    var groupKeys = {};
    var ruleKeys = {};
    var pushedStageKeys = {};
    var pushedGroupKeys = {};
    var pushedRuleKeys = {};
    var pushedRuleDisplayKeys = {};
    var openedStageKeys = {};
    var stageWidthKeys = {};
    var groupWidthKeys = {};

    collectSearchExpandOverlaySparseKeys(oldOverlay, stageKeys, groupKeys, ruleKeys);
    collectSearchExpandOverlaySparseKeys(newOverlay, stageKeys, groupKeys, ruleKeys);

    for (var stageKey in stageKeys) {
        if (!Object.prototype.hasOwnProperty.call(stageKeys, stageKey)) continue;
        var stageRef = parseSearchExpandOverlayRefKey(stageKey, 1);
        if (!stageRef) continue;
        var stageBefore = searchOverlayStageOpen(oldOverlay, stageRef.si);
        var stageAfter = searchOverlayStageOpen(newOverlay, stageRef.si);
        if (stageBefore === stageAfter) continue;
        change.changed = true;
        pushSearchTransitionRef(change.stages, pushedStageKeys, String(stageRef.si), { si: stageRef.si });
        pushSearchTransitionRef(change.stageWidths, stageWidthKeys, String(stageRef.si), { si: stageRef.si });
        if (stageAfter) {
            pushSearchTransitionRef(change.openedStages, openedStageKeys, String(stageRef.si), { si: stageRef.si });
        }
    }

    for (var groupKey in groupKeys) {
        if (!Object.prototype.hasOwnProperty.call(groupKeys, groupKey)) continue;
        var groupRef = parseSearchExpandOverlayRefKey(groupKey, 2);
        if (!groupRef) continue;
        var groupBefore = searchOverlayGroupOpen(oldOverlay, groupRef.si, groupRef.gi);
        var groupAfter = searchOverlayGroupOpen(newOverlay, groupRef.si, groupRef.gi);
        if (groupBefore === groupAfter) continue;
        change.changed = true;
        pushSearchTransitionRef(change.groups, pushedGroupKeys, groupKey, groupRef);
        pushSearchTransitionRef(change.groupWidths, groupWidthKeys, groupKey, groupRef);
        pushSearchTransitionRef(change.stageWidths, stageWidthKeys, String(groupRef.si), { si: groupRef.si });
    }

    for (var ruleKeyValue in ruleKeys) {
        if (!Object.prototype.hasOwnProperty.call(ruleKeys, ruleKeyValue)) continue;
        var ruleRef = parseSearchExpandOverlayRefKey(ruleKeyValue, 3);
        if (!ruleRef) continue;
        var ruleBefore = searchOverlayRuleOpen(oldOverlay, ruleRef.si, ruleRef.gi, ruleRef.ri);
        var ruleAfter = searchOverlayRuleOpen(newOverlay, ruleRef.si, ruleRef.gi, ruleRef.ri);
        if (ruleBefore === ruleAfter) continue;

        var groupWidthKey = searchExpandOverlayGroupKey(ruleRef.si, ruleRef.gi);
        change.changed = true;
        pushSearchTransitionRef(change.rules, pushedRuleKeys, ruleKeyValue, ruleRef);
        pushSearchTransitionRef(change.ruleDisplays, pushedRuleDisplayKeys, ruleKeyValue, ruleRef);
        pushSearchTransitionRef(change.groupWidths, groupWidthKeys, groupWidthKey, {
            si: ruleRef.si,
            gi: ruleRef.gi
        });
        pushSearchTransitionRef(change.stageWidths, stageWidthKeys, String(ruleRef.si), { si: ruleRef.si });
    }

    if (!change.changed && (oldOverlay || newOverlay)) change.changed = true;
    return change;
}

function shouldUseSparseSearchExpandTransition(options) {
    return !!(options && options.sparse &&
        typeof useVirtualizedDisplayRefresh === 'function' &&
        useVirtualizedDisplayRefresh());
}

function searchExpandOverlayTransitionSummary(oldOverlay, newOverlay, options) {
    if (shouldUseSparseSearchExpandTransition(options)) {
        return searchExpandOverlaySparseTransitionSummary(oldOverlay, newOverlay);
    }

    var change = emptySearchLayoutTransitionSummary();
    var stageKeys = {};
    var groupKeys = {};
    var ruleKeys = {};
    var ruleDisplayKeys = {};
    var openedStageKeys = {};
    var stageWidthKeys = {};
    var groupWidthKeys = {};

    for (var si = 0; si < currentStageCount(); si++) {
        var stageBefore = searchOverlayStageOpen(oldOverlay, si);
        var stageAfter = searchOverlayStageOpen(newOverlay, si);
        if (stageBefore !== stageAfter) {
            change.changed = true;
            pushSearchTransitionRef(change.stages, stageKeys, String(si), { si: si });
            pushSearchTransitionRef(change.stageWidths, stageWidthKeys, String(si), { si: si });
            if (stageAfter) {
                pushSearchTransitionRef(change.openedStages, openedStageKeys, String(si), { si: si });
            }
        }

        for (var gi = 0; gi < groupCount(si); gi++) {
            var groupBefore = searchOverlayGroupOpen(oldOverlay, si, gi);
            var groupAfter = searchOverlayGroupOpen(newOverlay, si, gi);
            var groupKey = searchExpandOverlayGroupKey(si, gi);
            if (groupBefore !== groupAfter) {
                change.changed = true;
                pushSearchTransitionRef(change.groups, groupKeys, groupKey, { si: si, gi: gi });
                pushSearchTransitionRef(change.groupWidths, groupWidthKeys, groupKey, { si: si, gi: gi });
                pushSearchTransitionRef(change.stageWidths, stageWidthKeys, String(si), { si: si });
            }

            for (var ri = 0; ri < groupRuleCount(si, gi); ri++) {
                var ruleBefore = searchOverlayRuleOpen(oldOverlay, si, gi, ri);
                var ruleAfter = searchOverlayRuleOpen(newOverlay, si, gi, ri);
                if (ruleBefore === ruleAfter) continue;

                var key = searchExpandOverlayRuleKey(si, gi, ri);
                change.changed = true;
                pushSearchTransitionRef(change.rules, ruleKeys, key, { si: si, gi: gi, ri: ri });
                pushSearchTransitionRef(change.ruleDisplays, ruleDisplayKeys, key, { si: si, gi: gi, ri: ri });
                pushSearchTransitionRef(change.groupWidths, groupWidthKeys, groupKey, { si: si, gi: gi });
                pushSearchTransitionRef(change.stageWidths, stageWidthKeys, String(si), { si: si });
            }
        }
    }

    return change;
}

function clearSearchExpandOverlay() {
    var search = searchRuntime();
    if (!search.searchExpandOverlay) return false;
    search.searchExpandOverlay = null;
    return true;
}

function commitSearchExpandOverlayToLayoutState(overlay) {
    if (!overlay) return emptySearchLayoutTransitionSummary();

    var change = searchExpandOverlayTransitionSummary(null, overlay);
    for (var si = 0; si < currentStageCount(); si++) {
        var stage = stageState(si);
        if (stage) stage.open = !!(overlay.stages && overlay.stages[String(si)]);

        for (var gi = 0; gi < groupCount(si); gi++) {
            var group = groupState(si, gi);
            if (group) {
                group.open = groupRuleCount(si, gi) <= 1 ||
                    !!(overlay.groups && overlay.groups[searchExpandOverlayGroupKey(si, gi)]);
            }

            for (var ri = 0; ri < groupRuleCount(si, gi); ri++) {
                var rule = ruleState(si, gi, ri);
                if (!rule) continue;

                var ruleOverlay = overlay.rules &&
                    overlay.rules[searchExpandOverlayRuleKey(si, gi, ri)];
                rule.open = !!(ruleOverlay && ruleOverlay.open);

            }
        }
    }

    return change;
}

function commitSearchMatchesToLayout(query, scope, matches, options) {
    options = options || {};
    query = String(query || '').trim();
    scope = normalizeSearchScopeValue(scope || 'tree-rules');
    matches = matches || [];
    if (!query || !matches.length) return false;

    var layoutChange = null;
    var sourceAnchor = options.viewportAnchor
        ? searchViewportAnchorSourceForExpandLayout(options.viewportAnchor, matches)
        : null;
    measureSearchTiming('expand-layout-commit', function() {
        var overlay = cachedSearchExpandOverlay(query, scope, matches) ||
            buildSearchExpandOverlay(query, scope, matches);
        layoutChange = commitSearchExpandOverlayToLayoutState(overlay);

        var previousOverlay = activeSearchExpandOverlay();
        if (previousOverlay) {
            var overlayChange = searchExpandOverlayTransitionSummary(previousOverlay, null);
            if (overlayChange.changed) {
                layoutChange = layoutChange
                    ? TraceState.mergeTransitionSummary(layoutChange, overlayChange)
                    : overlayChange;
            }
        }

        clearSearchExpandOverlay();
        searchRuntime().savedSearchState = null;
        searchRuntime().savedSearchViewport = null;
        searchRuntime().searchNavigationCommitted = false;
        searchRuntime().searchLayoutApplied = false;

        if (layoutChange && layoutChange.changed) {
            markTraceLayoutDirtySearchExpansion(query, scope, matches, layoutChange);
            var refreshTransition = function() {
                refreshLayoutTransitionDisplays(layoutChange, options);
            };
            if (sourceAnchor) {
                refreshTransition();
            } else {
                preserveTraceViewportAnchor(refreshTransition);
            }
        }
    }, { scope: scope });

    if (!layoutChange || !layoutChange.changed) {
        if (options.preserveProvidedMatches) {
            refreshSearchMatchesForCurrentLayout(matches, { deferStatus: true });
        } else {
            refreshSearchStateForCurrentLayout();
        }
        updateSearchLabelHighlights(query, scope, options.preserveProvidedMatches
            ? { searchMatches: matches, skipCollapsedIndicators: true }
            : undefined);
        scheduleSearchLayoutDecoration(query, scope, matches);
        updateSearchStatus(searchRuntime().searchMatches.length);
        return true;
    }

    measureSearchTiming('layout-model-rebuild', function() {
        rebuildTraceLayoutModel();
        refreshSearchNavigationLocationCache();
    }, { reason: 'search-expand-commit' });
    if (options.preserveProvidedMatches) {
        refreshSearchMatchesForCurrentLayout(matches, { deferStatus: true });
    } else {
        refreshSearchStateForCurrentLayout();
    }
    updateSearchLabelHighlights(query, scope, options.preserveProvidedMatches
        ? { searchMatches: matches, skipCollapsedIndicators: true }
        : undefined);
    scheduleSearchLayoutDecoration(query, scope, matches);
    updateSearchStatus(searchRuntime().searchMatches.length);

    if (sourceAnchor || options.viewportState) {
        var restoredAnchor = sourceAnchor
            ? searchViewportAnchorForExpandLayout(sourceAnchor, matches)
            : null;
        if (restoredAnchor && restoreTraceViewportAnchor(restoredAnchor)) {
            restoreSearchNonHorizontalViewportState(options.viewportState);
        } else {
            restoreSearchViewportState(options.viewportState);
        }
    }
    return true;
}

function ensureCommittedSearchExpandResultIntent(query, scope) {
    var search = searchRuntime();
    var intent = normalizeSearchIntent(query, scope, 'expand');
    if (search.searchResultQuery === intent.query &&
            search.searchResultScope === intent.scope &&
            search.searchResultMode === 'expand' &&
            search.searchResultEpoch) {
        return;
    }
    search.searchResultEpoch = search.activeSearchQueryEpoch || currentSearchTimingEpoch();
    search.searchResultQuery = intent.query;
    search.searchResultScope = intent.scope;
    search.searchResultMode = 'expand';
    search.pendingSearchQuery = intent.query;
    search.pendingSearchScope = intent.scope;
    search.pendingSearchMode = 'expand';
    search.searchResultStaleForNavigation = false;
}

function searchMatchedRuleFilter(matches) {
    var matchedRules = {};
    matches = matches || [];

    for (var i = 0; i < matches.length; i++) {
        var match = matches[i];
        if (!match || match.type !== 'rule') continue;
        matchedRules[ruleKey(match.si, match.gi, match.ri)] = true;
    }

    return function(si, gi, ri) {
        return !!matchedRules[ruleKey(si, gi, ri)];
    };
}

function searchViewportAnchorSourceForExpandLayout(anchor, matches) {
    return traceViewportAnchorSourceForExpandedLayout(anchor, searchMatchedRuleFilter(matches));
}

function searchViewportAnchorForExpandLayout(anchor, matches) {
    return traceViewportAnchorForExpandedLayout(anchor, searchMatchedRuleFilter(matches));
}

function preserveFullscreenLayoutPath(layoutChange) {
    var fullscreenRef = fullscreenCurrentRule();
    if (!fullscreenRef || !validRuleRef(fullscreenRef.si, fullscreenRef.gi, fullscreenRef.ri)) {
        return layoutChange;
    }
    return TraceState.mergeTransitionSummary(
        layoutChange,
        openRulePath(fullscreenRef.si, fullscreenRef.gi, fullscreenRef.ri)
    );
}


function renderSearchRuleTarget(match, preserveLayout, preserveViewport, activation) {
    if (activeSearchExpandOverlay()) preserveLayout = true;
    if (!preserveLayout) ensureSearchMatchDetailsVisible(match);
    if (!preserveLayout) {
        ensureRuleVisible(match.si, match.gi, match.ri);
    }
    if (preserveViewport) {
        refreshVirtualRowsNow();
    } else {
        if (!scrollSearchMatchIntoView(match)) scrollRuleIntoView(match.si, match.gi, match.ri);
    }
    requestSearchTargetPayload(match);
    var activationOptions = { preserveViewport: preserveViewport, activation: activation };
    if (tryFastActivateMountedSearchMatch(match, activationOptions)) return true;
    renderSearchRuleMarks(match);
    if (finishSearchActivationIfActivated(
            activation,
            match,
            activateRenderedSearchMark(match, activationOptions))) {
        return true;
    }
    if (searchMatchActivationStillCurrent(match, activationOptions)) {
        scheduleSearchMatchActivationRetry(match, function() {
            renderSearchRuleMarks(match);
        }, 2, activationOptions);
    }
    return false;
}

function renderSearchStageTarget(match, preserveViewport, activation) {
    if (preserveViewport) {
        refreshVirtualRowsNow();
    } else {
        if (!scrollSearchMatchIntoView(match)) scrollStageIntoView(match.si);
    }
    var activationOptions = { preserveViewport: preserveViewport, activation: activation };
    if (tryFastActivateMountedSearchMatch(match, activationOptions)) return true;
    renderSearchStageMarks();
    if (finishSearchActivationIfActivated(
            activation,
            match,
            activateRenderedSearchMark(match, activationOptions))) {
        return true;
    }
    if (searchMatchActivationStillCurrent(match, activationOptions)) {
        scheduleSearchMatchActivationRetry(match, renderSearchStageMarks, 2, activationOptions);
    }
    return false;
}

function searchMatchActivationOptions(options) {
    var activationOptions = {
        preserveViewport: !!(options && options.preserveViewport)
    };
    if (options && options.activation) activationOptions.activation = options.activation;
    return activationOptions;
}

function canFastActivateMountedSearchMatch(match) {
    if (!match) return false;
    if (match.type !== 'rule' || !isFullscreenRule(match.si, match.gi, match.ri)) {
        if (!searchMatchIntersectsTraceViewport(match)) return false;
    }
    if (match.type === 'rule' &&
            typeof mountedRuleNeedsViewportRender === 'function' &&
            mountedRuleNeedsViewportRender(match.si, match.gi, match.ri)) {
        return false;
    }
    return resolveSearchMatchRenderedTarget(match).status === SEARCH_RENDERED_TARGET_RESOLVED;
}

function tryFastActivateMountedSearchMatch(match, options) {
    if (!canFastActivateMountedSearchMatch(match)) return false;
    var activationOptions = searchMatchActivationOptions(options);
    return finishSearchActivationIfActivated(
        activationOptions.activation,
        match,
        activateRenderedSearchMark(match, activationOptions)
    );
}

function setActiveSearchMatch(index, options) {
    var count = searchRuntime().searchMatches.length;
    if (!count) {
        clearPendingSearchActivation();
        clearSearchNavigationMatchIndex();
        searchRuntime().currentSearchMatchIndex = -1;
        searchRuntime().activeSearchMatchRecord = null;
        updateSearchStatus(0);
        return false;
    }

    var nextIndex = Math.floor(Number(index));
    if (!Number.isFinite(nextIndex)) return false;
    nextIndex = ((nextIndex % count) + count) % count;
    var match = searchRuntime().searchMatches[nextIndex];
    var activation = beginSearchMatchActivation(nextIndex, match);
    if (!activation) return false;
    updateSearchStatus(count);

    if (!match) return false;

    options = options || {};
    options.activation = activation;
    if (tryFastActivateMountedSearchMatch(match, options)) return true;

    if (match.type === 'rule') {
        return renderSearchRuleTarget(
            match,
            options && options.preserveLayout,
            options && options.preserveViewport,
            activation
        );
    } else if (match.type === 'group') {
        return renderSearchGroupTarget(
            match,
            options && options.preserveLayout,
            options && options.preserveViewport,
            activation
        );
    } else if (match.type === 'stage') {
        return renderSearchStageTarget(match, options && options.preserveViewport, activation);
    }
    return false;
}

var SEARCH_NAV_REPEAT_DELAY_MS = 320;
var SEARCH_NAV_REPEAT_INTERVAL_MS = 105;

function clearSearchNavRepeatSuppression(state) {
    if (searchRuntime().searchNavRepeatSuppressClick !== state) return;
    searchRuntime().searchNavRepeatSuppressClick = null;
}

function suppressNextSearchNavClick(direction, ms) {
    var state = {
        direction: direction,
        until: Date.now() + ms
    };
    searchRuntime().searchNavRepeatSuppressClick = state;
    scheduleRuntimeTimeout(function() {
        clearSearchNavRepeatSuppression(state);
    }, ms, {
        epochScopes: ['trace'],
        label: 'search-nav-repeat-suppression'
    });
}

function shouldSuppressSearchNavClick(direction) {
    var state = searchRuntime().searchNavRepeatSuppressClick;
    if (!state) return false;
    if (state.direction !== direction) return false;
    if (Date.now() > state.until) {
        searchRuntime().searchNavRepeatSuppressClick = null;
        return false;
    }
    searchRuntime().searchNavRepeatSuppressClick = null;
    return true;
}

function removeSearchNavRepeatListeners() {
    if (typeof window === 'undefined' || !window.removeEventListener) return;
    window.removeEventListener('pointerup', stopSearchNavRepeat);
    window.removeEventListener('pointercancel', stopSearchNavRepeat);
    window.removeEventListener('blur', stopSearchNavRepeat);
}

function stopSearchNavRepeat(ev) {
    var state = searchRuntime().searchNavRepeatState;
    if (!state) return;
    if (ev && ev.pointerId !== undefined &&
        state.pointerId !== undefined &&
        ev.pointerId !== state.pointerId) {
        return;
    }

    clearTimeout(state.delayTimer);
    clearInterval(state.intervalTimer);
    removeSearchNavRepeatListeners();
    suppressNextSearchNavClick(state.direction, 700);
    searchRuntime().searchNavRepeatState = null;
}

function startSearchNavRepeat(direction, ev) {
    TraceActions.search.startSearchNavRepeat(direction, ev);
}

function handleSearchNavButtonClick(direction, ev) {
    TraceActions.search.handleSearchNavButtonClick(direction, ev);
}

function navigateSearchMatch(direction, ev, forceRefresh) {
    return TraceActions.search.navigateSearchMatch(direction, ev, forceRefresh);
}

function updateSearchSelectionForVisibleMatches(options) {
    options = options || {};
    var count = searchRuntime().searchMatches.length;
    if (!count) {
        clearPendingSearchActivation();
        clearSearchNavigationMatchIndex();
        if (!options.deferStatus) updateSearchStatus(0);
        return;
    }

    if (searchRuntime().currentSearchMatchIndex >= searchRuntime().searchMatches.length) {
        clearPendingSearchActivation();
        clearSearchNavigationMatchIndex();
        clearActiveSearchMatch();
        searchRuntime().currentSearchMatchIndex = -1;
        searchRuntime().activeSearchMatchRecord = null;
    } else if (currentSearchNavigationMatchIndex(count) < 0) {
        clearSearchNavigationMatchIndex();
    }
    if (!options.deferStatus) updateSearchStatus(count);
}

function exitSearch() {
    return withSearchMarksTransition('exit-search', {}, function() {
        cancelScheduledSearch();
        cancelAutoGlobalSearchTimer('exit-search');
        searchRuntime().searchDirty = false;
        cancelGlobalSearchSummaryJob('exit-search');
        cancelPendingHighlights();
        var restoreSearchOrigin = searchRuntime().searchLayoutApplied && !searchRuntime().searchNavigationCommitted;
        var commitSearchLayout = searchRuntime().searchLayoutApplied && searchRuntime().searchNavigationCommitted;
        var previousExpandOverlay = activeSearchExpandOverlay();
        var restoreState = restoreSearchOrigin ? searchRuntime().savedSearchState : null;
        var restoreViewport = restoreSearchOrigin ? searchRuntime().savedSearchViewport : null;
        searchRuntime().savedSearchState = null;
        searchRuntime().savedSearchViewport = null;
        searchRuntime().searchNavigationCommitted = false;
        searchRuntime().searchLayoutApplied = false;
        searchRuntime().searchResultEpoch = searchRuntime().activeSearchQueryEpoch || currentSearchTimingEpoch();
        searchRuntime().searchResultState = 'cleared';
        var state = currentUiState();
        state.searchActive = false;
        searchRuntime().lastSearchQuery = '';
        state.searchToken++;

        var layoutChange = null;
        if (restoreState) {
            layoutChange = preserveFullscreenLayoutPath(applyLayoutState(restoreState));
        } else if (commitSearchLayout && previousExpandOverlay) {
            layoutChange = commitSearchExpandOverlayToLayoutState(previousExpandOverlay);
        }

        if (previousExpandOverlay) {
            var overlayChange = searchExpandOverlayTransitionSummary(previousExpandOverlay, null);
            if (overlayChange.changed) {
                layoutChange = layoutChange
                    ? TraceState.mergeTransitionSummary(layoutChange, overlayChange)
                    : overlayChange;
            }
        }

        clearSearchExpandOverlay();
        clearCommittedSearchResultIntent();

        if (layoutChange) {
            markTraceLayoutDirtySearchExpansion('', currentSearchScope(), [], layoutChange);
            refreshLayoutTransitionDisplays(layoutChange, { deferVirtualRefresh: !!restoreViewport });
        }
        if (restoreViewport) {
            restoreSearchViewportState(restoreViewport);
        }

        var clearScope = currentSearchScope();
        clearSearchMatches({
            deferStatus: true,
            allowWithoutSearchTransaction: true
        });
        clearStaleSearchMarks('', clearScope, {
            searchTransactionToken: null,
            allowWithoutSearchTransaction: true
        });
        var clearJobs = collectRenderJobsForCurrentState('', false);
        appendMarkedRuleClearJobs(clearJobs);
        scheduleSearchDecoration('', clearScope, {
            collapsedIndicators: emptyCollapsedSearchIndicators(),
            highlightJobs: clearJobs,
            searchToken: state.searchToken,
            searchTransactionToken: null,
            allowWithoutSearchTransaction: true,
            syncFullscreen: true,
            updateStatus: true
        });
        return true;
    });
}

function clearSearchInput(ev) {
    TraceActions.search.clearSearchInput(ev);
}

function handleSearchBoxKeydown(ev) {
    TraceActions.search.handleSearchBoxKeydown(ev);
}

function doSearch() {
    TraceActions.search.doSearch();
}

function cancelSearchDecorationJob(reason) {
    var search = searchRuntime();
    var job = search.searchDecorationJob;
    if (!job) return false;
    job.cancelled = true;
    job.cancelReason = reason || 'cancelled';
    if (job.frameHandle) cancelRenderFrameWork(job.frameHandle);
    if (search.searchDecorationJob === job) search.searchDecorationJob = null;
    return true;
}

function searchDecorationJobCurrent(job) {
    if (!job || job.cancelled || searchRuntime().searchDecorationJob !== job) return false;
    if (job.searchToken !== undefined && job.searchToken !== currentUiState().searchToken) {
        cancelSearchDecorationJob('stale-search-token');
        return false;
    }
    if (!searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true })) {
        cancelSearchDecorationJob('stale-search-transaction');
        return false;
    }
    return true;
}

function searchDecorationCompletionCurrent(job) {
    if (!job || job.cancelled) return false;
    if (job.searchToken !== undefined && job.searchToken !== currentUiState().searchToken) return false;
    return searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true });
}

function completeSearchDecorationJob(job) {
    if (searchRuntime().searchDecorationJob === job) {
        searchRuntime().searchDecorationJob = null;
    }
}

function scheduleSearchDecoration(query, scope, options) {
    options = options || {};
    cancelSearchDecorationJob('replaced');

    var transactionToken = Object.prototype.hasOwnProperty.call(options, 'searchTransactionToken')
        ? options.searchTransactionToken
        : activeSearchTransactionToken();
    var search = searchRuntime();
    var highlightJobs = options.highlightJobs || null;
    if (typeof discardDetachedRuleElementsWithStaleSearchRender === 'function') {
        discardDetachedRuleElementsWithStaleSearchRender(query, scope);
    }
    if (highlightJobs) {
        appendMarkedRuleClearJobs(highlightJobs, query, scope);
    }
    var job = {
        id: search.nextSearchDecorationJobId++,
        query: query || '',
        scope: scope || '',
        searchToken: Object.prototype.hasOwnProperty.call(options, 'searchToken')
            ? options.searchToken
            : currentUiState().searchToken,
        collapsedIndicators: Object.prototype.hasOwnProperty.call(options, 'collapsedIndicators')
            ? options.collapsedIndicators
            : null,
        skipCollapsedIndicators: !!options.skipCollapsedIndicators,
        searchMatches: options.searchMatches || null,
        highlightJobs: highlightJobs,
        updateStatus: options.updateStatus !== false,
        syncFullscreen: !!options.syncFullscreen,
        anchorPreview: options.anchorPreview !== false,
        allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction,
        searchTransactionToken: transactionToken,
        ownerId: transactionToken && transactionToken.ownerId || '',
        frameHandle: null,
        cancelled: false,
        cancelReason: ''
    };

    search.searchDecorationJob = job;
    function scheduleSearchDecorationFrame() {
        job.frameHandle = scheduleRenderDeferredWork(
            'search-decoration',
            function runSearchDecorationFrame(visualCtx) {
                return runScheduledSearchDecoration(visualCtx);
            },
            {
                token: transactionToken
                    ? searchTransactionFrameToken(transactionToken)
                    : renderFrameEpochToken(['trace', 'search']),
                ownerId: job.ownerId,
                label: 'search-decoration',
                surfaces: ['search-marks', 'rule-tree', 'rule-info', 'fullscreen-shell', 'trace-anchor-line'],
                onDiscard: function(reason) {
                    if (searchRuntime().searchDecorationJob === job) {
                        job.cancelled = true;
                        job.cancelReason = reason || 'discarded';
                        searchRuntime().searchDecorationJob = null;
                    }
                }
            }
        );
    }

    function runScheduledSearchDecoration(visualCtx) {
            job.frameHandle = null;
            if (!searchDecorationJobCurrent(job)) return;
            if (searchShouldYieldToViewportInteraction(job)) {
                recordSearchTiming('search-decoration-yield', 0, {
                    viewportGeneration: currentTraceViewportInteractionGeneration()
                });
                scheduleSearchDecorationFrame();
                return;
            }

            updateSearchLabelHighlights(job.query, job.scope, {
                collapsedIndicators: job.collapsedIndicators,
                searchMatches: job.searchMatches,
                skipCollapsedIndicators: job.skipCollapsedIndicators,
                searchTransactionToken: job.searchTransactionToken,
                allowWithoutSearchTransaction: job.allowWithoutSearchTransaction
            });

            if (job.highlightJobs) {
                scheduleHighlightJobs(job.highlightJobs, job.searchToken, function() {
                    if (job.updateStatus && searchDecorationCompletionCurrent(job)) {
                        updateSearchStatus(searchRuntime().searchMatches.length);
                    }
                    completeSearchDecorationJob(job);
                }, {
                    searchTransactionToken: job.searchTransactionToken,
                    allowWithoutSearchTransaction: job.allowWithoutSearchTransaction,
                    onCancel: function() {
                        if (job.updateStatus && searchDecorationCompletionCurrent(job)) {
                            updateSearchStatus(searchRuntime().searchMatches.length);
                        }
                        completeSearchDecorationJob(job);
                    }
                });
            } else if (job.updateStatus) {
                updateSearchStatus(searchRuntime().searchMatches.length);
                completeSearchDecorationJob(job);
            } else {
                completeSearchDecorationJob(job);
            }

            if (job.syncFullscreen && isFullscreenOpen()) {
                syncFullscreenOverlay(job.query, job.scope);
            }
            if (job.anchorPreview) scheduleTraceAnchorLineUpdate();
    }

    scheduleSearchDecorationFrame();
    return job;
}

function cancelPendingHighlights() {
    cancelSearchDecorationJob('cancel-pending-highlights');
    searchRuntime().activeHighlightToken++;
    if (searchRuntime().activeHighlightTimer !== null) {
        cancelRenderFrameWork(searchRuntime().activeHighlightTimer);
        searchRuntime().activeHighlightTimer = null;
    }
}

function scheduleHighlightJobs(jobs, token, onComplete, options) {
    options = options || {};
    var transactionToken = Object.prototype.hasOwnProperty.call(options, 'searchTransactionToken')
        ? options.searchTransactionToken
        : activeSearchTransactionToken();
    if (!jobs.length) {
        if (onComplete &&
                token === currentUiState().searchToken &&
                searchTransactionTokenCurrent(transactionToken, { payload: true })) {
            onComplete();
        }
        return;
    }

    var index = 0;
    var myToken = ++searchRuntime().activeHighlightToken;
    var searchMarksDomGeneration = currentSearchMarksDomGeneration();
    var virtualRowsDomGeneration = currentVirtualRowsDomGeneration();
    var searchTimingEpoch = currentSearchTimingEpoch();
    var settled = false;
    var viewportYieldState = { viewportYieldGeneration: 0 };

    function cancelHighlightJobs(reason) {
        searchRuntime().activeHighlightTimer = null;
        if (settled) return;
        settled = true;
        if (typeof options.onCancel === 'function') {
            options.onCancel(reason || 'cancelled');
        }
    }

    function completeHighlightJobs() {
        searchRuntime().activeHighlightTimer = null;
        if (settled) return;
        settled = true;
        if (onComplete &&
                token === currentUiState().searchToken &&
                searchTransactionTokenCurrent(transactionToken, { payload: true })) {
            onComplete();
        }
    }

    function scheduleChunk() {
        searchRuntime().activeHighlightTimer = scheduleRenderDeferredWork(
            'search-highlight-chunk',
            function runScheduledSearchHighlightChunk(visualCtx) {
                runChunk(visualCtx);
            },
            {
                token: transactionToken
                    ? searchTransactionFrameToken(transactionToken)
                    : renderFrameEpochToken(['trace', 'search']),
                ownerId: transactionToken && transactionToken.ownerId || '',
                label: 'search-highlight-chunk',
                surfaces: ['search-marks', 'rule-tree', 'rule-info'],
                onDiscard: function() {
                    cancelHighlightJobs('discarded');
                }
            }
        );
    }

    function runChunk(visualCtx) {
        if (myToken !== searchRuntime().activeHighlightToken ||
            token !== currentUiState().searchToken ||
            !searchTransactionTokenCurrent(transactionToken, { payload: true })) {
            cancelHighlightJobs('stale-search-highlight');
            return;
        }
        if (searchShouldYieldToViewportInteraction(viewportYieldState)) {
            recordSearchTiming('search-highlight-yield', 0, {
                viewportGeneration: currentTraceViewportInteractionGeneration()
            });
            scheduleChunk();
            return;
        }
        if (!canCommitSearchMarks('search-highlight-chunk', null, {
                searchMarksDomGeneration: searchMarksDomGeneration,
                virtualRowsDomGeneration: virtualRowsDomGeneration,
                searchTransactionToken: transactionToken,
                allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction
        })) {
            cancelHighlightJobs('stale-search-dom');
            return;
        }

        var start = Date.now();
        while (index < jobs.length && Date.now() - start < 10) {
            var job = jobs[index++];
            job.searchMarksDomGeneration = searchMarksDomGeneration;
            job.virtualRowsDomGeneration = virtualRowsDomGeneration;
            job.searchTimingEpoch = searchTimingEpoch;
            job.searchTransactionToken = transactionToken;
            job.allowWithoutSearchTransaction = !!options.allowWithoutSearchTransaction;
            applySearchRenderJob(job);
        }

        if (index < jobs.length) {
            scheduleChunk();
        } else {
            completeHighlightJobs();
        }
    }

    scheduleChunk();
}

function resolveSearchRenderJobRuleCell(job) {
    if (!job || !hasDOM() || !document.getElementById) return null;
    return document.getElementById('rule-' + ruleKey(job.si, job.gi, job.ri));
}

function searchRenderJobRuleCellCurrent(job, details) {
    var cell = resolveSearchRenderJobRuleCell(job);
    if (cell && cell.isConnected === false) {
        recordVisualCommitDenied('search-marks', 'search-render-job', 'detached_target', {
            ruleKey: details.ruleKey,
            targetId: cell.id || ''
        });
        return false;
    }
    return true;
}

function applySearchRenderJob(job) {
    if (job && job.searchTransactionToken &&
            !searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true })) {
        return;
    }
    var details = {
        ruleKey: ruleKey(job.si, job.gi, job.ri)
    };
    if (job.searchMarksDomGeneration !== undefined) {
        details.searchMarksDomGeneration = job.searchMarksDomGeneration;
    }
    if (job.virtualRowsDomGeneration !== undefined) {
        details.virtualRowsDomGeneration = job.virtualRowsDomGeneration;
    }
    details.searchTransactionToken = job.searchTransactionToken || activeSearchTransactionToken();
    if (job.allowWithoutSearchTransaction) {
        details.allowWithoutSearchTransaction = true;
    }
    if (!canCommitSearchMarks('search-render-job', null, details)) return;
    if (!searchRenderJobRuleCellCurrent(job, details)) return;

    if (job.fields !== undefined) {
        if (!activeSearchExpandOverlay()) {
            TraceState.setRuleFeature(currentUiState(), job.si, job.gi, job.ri, 'fields', job.fields);
        }
        var fieldsBtn = document.getElementById('fieldbtn-' + job.si + '-' + job.gi + '-' + job.ri);
        if (fieldsBtn) {
            var fieldsAvailable = ruleHasFeature(job.si, job.gi, job.ri, 'fields');
            fieldsBtn.disabled = !fieldsAvailable;
            fieldsBtn.classList.toggle('active',
                fieldsAvailable && ruleFeatureButtonActive('fields', job.si, job.gi, job.ri));
        }
    }

    if (job.pinned !== undefined) {
        if (!activeSearchExpandOverlay()) {
            TraceState.setRuleFeature(currentUiState(), job.si, job.gi, job.ri, 'pinned', job.pinned);
        }
        var pinBtn = document.getElementById('pinbtn-' + job.si + '-' + job.gi + '-' + job.ri);
        if (pinBtn) {
            var pinnedAvailable = ruleHasFeature(job.si, job.gi, job.ri, 'pinned');
            pinBtn.disabled = !pinnedAvailable;
            pinBtn.classList.toggle('active',
                pinnedAvailable && ruleFeatureButtonActive('pinned', job.si, job.gi, job.ri));
        }
    }

    if (job.info !== undefined) {
        if (!activeSearchExpandOverlay()) {
            TraceState.setRuleFeature(currentUiState(), job.si, job.gi, job.ri, 'info', job.info);
        }
        measureSearchTimingForEpoch(job.searchTimingEpoch, 'info-panel-highlight-jobs', function() {
            updateRuleInfoDisplay(job.si, job.gi, job.ri);
        }, details);
    }

    if (job.force) {
        measureSearchTimingForEpoch(job.searchTimingEpoch, 'rule-tree-highlight-jobs', function() {
            renderRuleTreeImmediatelyIfMounted(job.si, job.gi, job.ri, job.query, true, job.scope);
        }, details);
        if (hasDOM()) {
            var infoContent = document.getElementById('info-content-' + job.si + '-' + job.gi + '-' + job.ri);
            if (job.info || infoContent && infoContent.querySelector && infoContent.querySelector('mark')) {
                measureSearchTimingForEpoch(job.searchTimingEpoch, 'info-panel-highlight-jobs', function() {
                    renderRuleInfoPanel(job.si, job.gi, job.ri, job.query, job.scope);
                }, details);
            }
        }
    } else {
        measureSearchTimingForEpoch(job.searchTimingEpoch, 'rule-tree-highlight-jobs', function() {
            requestRuleTreeRenderIfVisible(job.si, job.gi, job.ri, job.query, false, job.scope);
        }, details);
    }
}

function collectRenderJobsForCurrentState(query, includeUnrendered, searchScope) {
    var jobs = [];
    query = query || '';
    var renderScope = query ? (searchScope || currentSearchScope()) : '';

    forEachRule(function(si, gi, ri, rule) {
        var prev = rule.rendered;
        if (!includeUnrendered && !prev) return;

        if (!prev ||
            prev.query !== query ||
            prev.scope !== renderScope ||
            prev.showFields !== effectiveRuleFeature(si, gi, ri, 'fields') ||
            prev.showPinned !== effectiveRuleFeature(si, gi, ri, 'pinned')) {
            jobs.push({
                si: si,
                gi: gi,
                ri: ri,
                query: query,
                scope: renderScope,
                fields: effectiveRuleFeature(si, gi, ri, 'fields'),
                pinned: effectiveRuleFeature(si, gi, ri, 'pinned'),
                info: effectiveRuleFeature(si, gi, ri, 'info')
            });
        }
    });

    return jobs;
}

function appendSearchMatchRenderJobs(jobs, query, scope, matches) {
    jobs = jobs || [];
    matches = matches || [];
    var jobsByKey = {};
    for (var i = 0; i < jobs.length; i++) {
        jobsByKey[ruleKey(jobs[i].si, jobs[i].gi, jobs[i].ri)] = jobs[i];
    }

    var renderScope = query ? (scope || currentSearchScope()) : '';
    for (var m = 0; m < matches.length; m++) {
        var match = matches[m];
        if (!match || match.type !== 'rule') continue;
        if (!validRuleRef(match.si, match.gi, match.ri)) continue;

        var key = ruleKey(match.si, match.gi, match.ri);
        if (jobsByKey[key]) {
            jobsByKey[key].force = true;
            continue;
        }

        jobsByKey[key] = {
            si: match.si,
            gi: match.gi,
            ri: match.ri,
            query: query,
            scope: renderScope,
            force: true,
            fields: effectiveRuleFeature(match.si, match.gi, match.ri, 'fields'),
            pinned: effectiveRuleFeature(match.si, match.gi, match.ri, 'pinned'),
            info: effectiveRuleFeature(match.si, match.gi, match.ri, 'info')
        };
        jobs.push(jobsByKey[key]);
    }
    return jobs;
}

function scheduleSearchLayoutDecoration(query, scope, matches) {
    var jobs = collectRenderJobsForCurrentState(query, false, scope);
    appendSearchMatchRenderJobs(jobs, query, scope, matches);
    scheduleSearchDecoration(query, scope, {
        searchMatches: searchRuntime().searchMatches || matches || [],
        skipCollapsedIndicators: true,
        highlightJobs: jobs,
        searchToken: currentUiState().searchToken,
        searchTransactionToken: activeSearchTransactionToken(),
        syncFullscreen: true,
        anchorPreview: true,
        updateStatus: true
    });
}

function appendMarkedRuleClearJobs(jobs, query, scope) {
    if (!hasDOM() || !document.querySelectorAll) return jobs;

    query = query || '';
    var renderScope = query ? (scope || currentSearchScope()) : '';
    var jobsByKey = {};
    for (var i = 0; i < jobs.length; i++) {
        jobsByKey[ruleKey(jobs[i].si, jobs[i].gi, jobs[i].ri)] = jobs[i];
    }

    var marks = document.querySelectorAll('.rule-cell[id^="rule-"] mark');
    for (var m = 0; m < marks.length; m++) {
        var cell = marks[m].closest ? marks[m].closest('.rule-cell[id^="rule-"]') : null;
        var ref = cell ? parseRuleKeyId(cell.id, 'rule-') : null;
        if (!ref || !validRuleRef(ref.si, ref.gi, ref.ri)) continue;

        var key = ruleKey(ref.si, ref.gi, ref.ri);
        if (jobsByKey[key]) {
            jobsByKey[key].force = true;
            continue;
        }

        jobsByKey[key] = {
            si: ref.si,
            gi: ref.gi,
            ri: ref.ri,
            query: query,
            scope: renderScope,
            force: true,
            fields: effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'fields'),
            pinned: effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'pinned'),
            info: effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'info')
        };
        jobs.push(jobsByKey[key]);
    }
    return jobs;
}

function rerenderAllTrees(query, force, searchScope) {
    forEachRule(function(si, gi, ri) {
        if (force) invalidateRuleTree(si, gi, ri);
        requestRuleTreeRenderIfVisible(si, gi, ri, query, force, searchScope);
    });
    syncFullscreenOverlay(query, searchScope);
}
