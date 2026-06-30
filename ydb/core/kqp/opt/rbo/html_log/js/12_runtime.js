function createTraceRuntime() {
    function tracePrimaryIndex(selection) {
        selection = Array.isArray(selection) ? selection : [];
        var index = Math.floor(Number(selection[0]));
        return Number.isFinite(index) && index >= 0 ? index : 0;
    }

    function defineDerivedTraceIndex(bucket) {
        Object.defineProperty(bucket, 'activeTraceIndex', {
            enumerable: true,
            configurable: true,
            get: function() {
                return tracePrimaryIndex(this.activeTraceSelection);
            },
            set: function(index) {
                index = Math.floor(Number(index));
                this.activeTraceSelection = [Number.isFinite(index) && index >= 0 ? index : 0];
            }
        });
        return bucket;
    }

    function collapsedIndicators() {
        return { stages: {}, groups: {}, rules: {} };
    }

    function traceBucket() {
        return defineDerivedTraceIndex({
            activeTraceSelection: [0],
            activeTraceLoaded: false,
            traceStoreStatus: { state: 'unloaded', title: '', message: '' },
            stageCount: 0,
            traceStore: TraceStore.empty(),
            allRules: [],
            uiState: TraceState.emptyUiState(),
            ruleFeatureCache: {},
            showEmptyStages: false,
            pinnedFieldSelections: {},
            pinnedFieldWidths: {},
            pinnedFieldAutoWidths: {},
            diffFieldSelections: {},
            diffFieldSelectionsHydrated: {},
            traceSessions: {},
            treeSessions: {},
            treeMaterializers: {},
            rulePaneScrollSessions: {},
            ruleInfoTabSessions: {}
        });
    }

    function virtualizationBucket() {
        return {
            traceLayoutModel: null,
            virtualRange: { left: 0, right: Infinity },
            mountedVirtualRange: null,
            virtualizerReady: false,
            virtualRenderFrame: null,
            virtualRenderFrameCancel: null,
            mountedStageKeys: {},
            mountedRuleKeys: {},
            stageShellSignature: '',
            virtualRowSignatureCache: {},
            detachedRuleElementCache: {},
            detachedRuleElementCacheOrder: [],
            measuringTraceLayoutWidth: false,
            suppressTraceMeasuredWidth: false,
            traceMeasuredWidthCache: {},
            layoutWidthPartsCache: null,
            traceLayoutDirtyRegions: [],
            traceLayoutDirtyRegionKeys: {},
            lastTraceLayoutDirtyRegions: [],
            visibleStageCountCache: null,
            traceMeasuredWidthObserver: null,
            traceVirtualLayoutDirty: false,
            traceLayoutGeneration: 0,
            traceViewportGeneration: 0,
            lastTraceViewportInteractionAt: 0,
            lastTraceViewportInteractionGeneration: 0,
            traceVirtualScrollTarget: null,
            suppressNextVirtualScroll: null,
            virtualRowsDomGeneration: 0,
            traceCanvasDomGeneration: 0,
            traceScrollWidthCache: null,
            virtualRefreshDeferredForResize: false,
            lastVirtualRefresh: null,
            traceVirtualResizeListenerReady: false
        };
    }

    function frameQueueSet() {
        return {
            model: [],
            dom: [],
            deferred: []
        };
    }

    function frameKeySet() {
        return {
            model: {},
            dom: {},
            deferred: {}
        };
    }

    function frameBucket() {
        return {
            frameId: null,
            deferredFrameId: null,
            nextJobId: 1,
            nextVisualContextId: 1,
            activeFrameId: 0,
            activePhase: 'idle',
            completedFrames: 0,
            discardedJobs: 0,
            discardedByLabel: {},
            queues: frameQueueSet(),
            queuedKeys: frameKeySet(),
            lastFrameLog: []
        };
    }

    function visualBucket() {
        return {
            nextOwnerId: 1,
            surfaceOwners: {},
            deniedCommits: [],
            traceSwitchOwnerId: null,
            deferredWork: {}
        };
    }

    function lazyRenderBucket() {
        return {
            ruleRenderQueue: [],
            queuedRuleRenders: {},
            ruleRenderTimer: null,
            delayedRuleLoadingTimers: {},
            visibleRuleScanTimer: null,
            visibleRuleCheckTimer: null,
            pendingVisibleRuleChecks: {},
            payloadBlockJobs: {}
        };
    }

    function searchVisibleStateBucket() {
        return {
            query: '',
            scope: 'tree-rules',
            mode: 'find',
            traceGeneration: 0,
            payloadGeneration: 0,
            projectionGeneration: 0,
            knownCount: 0,
            matches: [],
            searchedRules: {}
        };
    }

    function searchLocalExactCacheBucket() {
        return {
            entries: {},
            order: [],
            hits: 0,
            misses: 0
        };
    }

    function searchGlobalSummaryCacheBucket() {
        return {
            entries: {},
            order: [],
            hits: 0,
            misses: 0
        };
    }

    function searchGlobalJobStateBucket() {
        return {
            nextJobId: 1,
            activeJob: null
        };
    }

    function searchLazyIndexBucket() {
        return {
            key: '',
            query: '',
            scope: 'tree-rules',
            traceGeneration: 0,
            ruleCount: 0,
            entries: {},
            order: [],
            version: 0,
            complete: false
        };
    }

    function searchLazyIndexCacheBucket() {
        return {
            entries: {},
            order: [],
            hits: 0,
            misses: 0
        };
    }

    function searchStateLayersBucket() {
        return {
            visible: searchVisibleStateBucket(),
            localExactCache: searchLocalExactCacheBucket(),
            lazyIndex: searchLazyIndexBucket(),
            lazyIndexCache: searchLazyIndexCacheBucket(),
            globalSummaryCache: searchGlobalSummaryCacheBucket(),
            globalJob: searchGlobalJobStateBucket()
        };
    }

    function searchBucket() {
        return {
            savedSearchState: null,
            savedSearchViewport: null,
            searchNavigationCommitted: false,
            searchLayoutApplied: false,
            searchExpandOverlay: null,
            searchFrame: null,
            searchFrameCancel: null,
            autoGlobalSearchTimer: null,
            autoGlobalSearchTimerCancel: null,
            autoGlobalSearchIntent: null,
            visibleSearchScanFrame: null,
            visibleSearchScanJob: null,
            searchInputDebounceStartedAt: 0,
            searchRunReason: '',
            searchDirty: false,
            searchLayers: searchStateLayersBucket(),
            ruleSearchIndex: null,
            searchIndexWorkerJob: null,
            searchIndexProgress: { state: 'unbuilt', mode: 'none', completed: 0, total: 0 },
            activeSearchQueryEpoch: 0,
            searchResultEpoch: 0,
            searchResultState: 'idle',
            searchResultQuery: '',
            searchResultScope: 'tree-rules',
            searchResultMode: 'find',
            searchExpandableMatchCount: 0,
            pendingSearchQuery: '',
            pendingSearchScope: 'tree-rules',
            pendingSearchMode: 'find',
            searchResultStaleForNavigation: false,
            nextSearchTransactionId: 1,
            activeSearchTransaction: null,
            nextSearchMatchCollectionJobId: 1,
            searchMatchCollectionJob: null,
            searchResultCache: {},
            searchResultCacheOrder: [],
            searchExpandOverlayCache: {},
            searchExpandOverlayCacheOrder: [],
            searchCacheTraceGeneration: 0,
            searchCachePayloadGeneration: -1,
            searchCacheBaseLayoutGeneration: 0,
            searchCacheIndexMode: '',
            searchCacheShowEmptyStages: false,
            searchBaseLayoutGeneration: 0,
            suppressSearchBaseLayoutCacheDirty: 0,
            searchResultCacheHits: 0,
            searchResultCacheMisses: 0,
            searchExpandOverlayCacheHits: 0,
            searchExpandOverlayCacheMisses: 0,
            searchTimingEpoch: 0,
            searchTimingCurrent: null,
            searchTimingHistory: [],
            searchMarksDomGeneration: 0,
            searchMarksTransitionOwnerId: null,
            nextSearchLabelHighlightJobId: 1,
            searchLabelHighlightJob: null,
            mountedSearchLabelCache: null,
            mountedSearchLabelCacheScans: 0,
            mountedSearchLabelCacheHits: 0,
            nextSearchDecorationJobId: 1,
            searchDecorationJob: null,
            activeHighlightToken: 0,
            activeHighlightTimer: null,
            lastSearchQuery: '',
            searchMatches: [],
            restoredSearchIndicatorMatches: null,
            currentSearchMatchIndex: -1,
            searchNavigationMatchIndex: -1,
            activeSearchMatchRecord: null,
            nextSearchActivationId: 1,
            pendingSearchActivation: null,
            collapsedSearchIndicators: collapsedIndicators(),
            collapsedSearchIndicatorCount: 0,
            collapsedSearchIndicatorsDirty: false,
            nextCollapsedSearchIndicatorJobId: 1,
            collapsedSearchIndicatorJob: null,
            collapsedSearchIndicatorRetryFrame: null,
            searchNavRepeatState: null,
            searchNavRepeatSuppressClick: null
        };
    }

    function diffBucket() {
        return {
            diffA: null,
            diffB: null,
            savedDiffState: null,
            savedDiffViewport: null,
            activeDiffCache: { pairKey: '', result: null },
            pendingDiffJob: null,
            nextDiffJobId: 1,
            diffTransitionOwnerId: null
        };
    }

    function diffArrowBucket() {
        return {
            diffMoveArrowOverlay: null,
            diffMoveArrowFrame: null,
            diffMoveArrowListenersReady: false,
            diffMoveArrowTraceScrollLeft: null,
            diffMoveArrowTraceScrollTop: null,
            diffMoveArrowTranslateX: 0,
            diffMoveArrowTranslateY: 0,
            diffArrowsDomGeneration: 0
        };
    }

    function fullscreenBucket() {
        return {
            fullscreenScrollbarRevealTimer: null,
            fullscreenScrollbarRevealReady: false,
            fullscreenNavRepeatState: null,
            fullscreenNavRepeatSuppressClick: null,
            fullscreenNavRepeatSuppressDocumentClickUntil: 0,
            fullscreenTransitionOwnerId: null,
            fullscreenDomGeneration: 0
        };
    }

    function resizeBucket() {
        return {
            ruleResizeDrag: null,
            pinnedFieldResizeDrag: null,
            deferredFrameWork: {
                treeMaterializers: false,
                traceAnchor: false,
                diffArrows: false
            },
            infoPanelHeights: {}
        };
    }

    function traceAnchorBucket() {
        return {
            traceAnchorLineEl: null,
            traceAnchorLineFrame: null,
            lastAnchorRestore: null,
            traceAnchorPreviewActive: false,
            traceAnchorPreviewHovered: false,
            traceAnchorPreviewHoverTarget: null,
            traceAnchorPreviewFocused: false,
            traceAnchorPreviewFocusTarget: null,
            traceAnchorPreviewPointerFocused: false,
            traceAnchorPreviewPointerFocusTarget: null,
            traceAnchorPreviewSearchFocused: false,
            traceAnchorPreviewSearchFocusTarget: null
        };
    }

    function actionEventsBucket() {
        return { boundTarget: null };
    }

    function mountBucket() {
        return {
            started: false,
            domInitialized: false,
            domListenerBound: false,
            dataListenerBound: false
        };
    }

    function diagnosticsBucket() {
        return { lastInvariantFailures: [] };
    }

    var state = {
        epochs: RuntimeEpochs.createState(),
        trace: traceBucket(),
        virtualization: virtualizationBucket(),
        frame: frameBucket(),
        visual: visualBucket(),
        lazyRender: lazyRenderBucket(),
        search: searchBucket(),
        diff: diffBucket(),
        diffArrows: diffArrowBucket(),
        fullscreen: fullscreenBucket(),
        resize: resizeBucket(),
        traceAnchor: traceAnchorBucket(),
        actionEvents: actionEventsBucket(),
        mount: mountBucket(),
        diagnostics: diagnosticsBucket()
    };

    function bucketState(name) {
        return function() { return state[name]; };
    }

    function bumpServiceEpoch(scope) {
        return RuntimeEpochs.bump(state.epochs, scope);
    }

    function createSearchStateService() {
        var bucket = bucketState('search');

        function markChanged() {
            return bumpServiceEpoch('search');
        }

        function cancelSupersededFrame(cancelReason) {
            var search = bucket();
            if (typeof search.searchFrameCancel === 'function') {
                search.searchFrameCancel(cancelReason || 'superseded-search-frame');
            }
            search.searchFrame = null;
            search.searchFrameCancel = null;
        }

        function setActiveMatch(index, record) {
            var search = bucket();
            index = Math.floor(Number(index));
            search.pendingSearchActivation = null;
            search.currentSearchMatchIndex = Number.isFinite(index) ? index : -1;
            search.searchNavigationMatchIndex = search.currentSearchMatchIndex;
            search.activeSearchMatchRecord = record || null;
            return markChanged();
        }

        function clearMatches(reason) {
            var search = bucket();
            search.searchMatches = [];
            search.searchExpandableMatchCount = 0;
            search.currentSearchMatchIndex = -1;
            search.searchNavigationMatchIndex = -1;
            search.activeSearchMatchRecord = null;
            search.pendingSearchActivation = null;
            search.searchResultState = reason || 'cleared';
            return markChanged();
        }

        function setMatches(matches, options) {
            options = options || {};
            var search = bucket();
            search.searchMatches = Array.isArray(matches) ? matches.slice() : [];
            search.searchExpandableMatchCount = options.expandableMatchCount !== undefined
                ? Math.max(0, Math.floor(Number(options.expandableMatchCount)) || 0)
                : search.searchMatches.length;
            search.pendingSearchActivation = null;
            if (options.activeIndex !== undefined || options.activeMatchRecord !== undefined) {
                setActiveMatch(options.activeIndex, options.activeMatchRecord);
            } else if (search.currentSearchMatchIndex >= search.searchMatches.length) {
                search.currentSearchMatchIndex = -1;
                search.searchNavigationMatchIndex = -1;
                search.activeSearchMatchRecord = null;
            } else if (search.searchNavigationMatchIndex >= search.searchMatches.length) {
                search.searchNavigationMatchIndex = -1;
            }
            if (options.resultState) search.searchResultState = options.resultState;
            return markChanged();
        }

        function setScheduledFrame(frameId, cancel) {
            var search = bucket();
            if (search.searchFrame !== null && search.searchFrame !== frameId) {
                cancelSupersededFrame('superseded-search-frame');
            }
            search.searchFrame = frameId === undefined ? null : frameId;
            search.searchFrameCancel = typeof cancel === 'function' ? cancel : null;
            return markChanged();
        }

        function setLayoutSnapshot(savedState, savedViewport) {
            var search = bucket();
            search.savedSearchState = savedState || null;
            search.savedSearchViewport = savedViewport || null;
            search.searchLayoutApplied = !!savedState;
            search.searchNavigationCommitted = false;
            return markChanged();
        }

        function clearLayoutSnapshot() {
            var search = bucket();
            search.savedSearchState = null;
            search.savedSearchViewport = null;
            search.searchLayoutApplied = false;
            search.searchNavigationCommitted = false;
            search.searchExpandOverlay = null;
            return markChanged();
        }

        return {
            state: bucket,
            activeMatchRecord: function() { return bucket().activeSearchMatchRecord; },
            cancelSupersededFrame: cancelSupersededFrame,
            clearLayoutSnapshot: clearLayoutSnapshot,
            clearMatches: clearMatches,
            currentMatchIndex: function() { return bucket().currentSearchMatchIndex; },
            layoutApplied: function() { return !!bucket().searchLayoutApplied; },
            matches: function() { return bucket().searchMatches.slice(); },
            markDirty: function(dirty) {
                bucket().searchDirty = dirty !== false;
                return markChanged();
            },
            resultState: function() { return bucket().searchResultState; },
            setActiveMatch: setActiveMatch,
            setLayoutSnapshot: setLayoutSnapshot,
            setMatches: setMatches,
            setScheduledFrame: setScheduledFrame,
            transaction: function() { return bucket().activeSearchTransaction; }
        };
    }

    function createDiffStateService() {
        var bucket = bucketState('diff');

        function markChanged() {
            return bumpServiceEpoch('diff');
        }

        function cloneRef(ref) {
            return ref
                ? { si: ref.si, gi: ref.gi, ri: ref.ri }
                : null;
        }

        function cancelPendingJob(reason) {
            var diff = bucket();
            var job = diff.pendingDiffJob;
            if (!job) return false;
            if (typeof job.cancel === 'function') {
                job.cancel(reason || 'superseded-diff-job');
            } else if (job.worker && typeof job.worker.terminate === 'function') {
                job.worker.terminate();
            }
            diff.pendingDiffJob = null;
            markChanged();
            return true;
        }

        function setPendingJob(job) {
            var diff = bucket();
            if (diff.pendingDiffJob && diff.pendingDiffJob !== job) {
                cancelPendingJob('superseded-diff-job');
            }
            diff.pendingDiffJob = job || null;
            return markChanged();
        }

        function setActiveRefs(a, b) {
            var diff = bucket();
            diff.diffA = cloneRef(a);
            diff.diffB = cloneRef(b);
            diff.activeDiffCache = { pairKey: '', result: null };
            return markChanged();
        }

        function setSavedSession(savedState, savedViewport) {
            var diff = bucket();
            diff.savedDiffState = savedState || null;
            diff.savedDiffViewport = savedViewport || null;
            return markChanged();
        }

        return {
            state: bucket,
            activeRefs: function() {
                var diff = bucket();
                return {
                    a: cloneRef(diff.diffA),
                    b: cloneRef(diff.diffB)
                };
            },
            cacheActiveResult: function(pairKey, result) {
                bucket().activeDiffCache = {
                    pairKey: pairKey || '',
                    result: result || null
                };
                return markChanged();
            },
            cancelPendingJob: cancelPendingJob,
            clearActiveRefs: function() {
                return setActiveRefs(null, null);
            },
            clearSavedSession: function() {
                return setSavedSession(null, null);
            },
            clearTransitionOwner: function(ownerId) {
                var diff = bucket();
                if (ownerId && diff.diffTransitionOwnerId !== ownerId) return false;
                diff.diffTransitionOwnerId = null;
                markChanged();
                return true;
            },
            pendingJob: function() { return bucket().pendingDiffJob; },
            savedSession: function() {
                var diff = bucket();
                return {
                    state: diff.savedDiffState,
                    viewport: diff.savedDiffViewport
                };
            },
            setActiveRefs: setActiveRefs,
            setPendingJob: setPendingJob,
            setSavedSession: setSavedSession,
            setTransitionOwner: function(ownerId) {
                bucket().diffTransitionOwnerId = ownerId || null;
                return markChanged();
            }
        };
    }

    function createFullscreenStateService() {
        var bucket = bucketState('fullscreen');

        function markChanged() {
            return bumpServiceEpoch('fullscreen');
        }

        return {
            state: bucket,
            bumpDomGeneration: function() {
                var fullscreen = bucket();
                fullscreen.fullscreenDomGeneration =
                    (Number(fullscreen.fullscreenDomGeneration) || 0) + 1;
                markChanged();
                return fullscreen.fullscreenDomGeneration;
            },
            clearNavRepeatState: function() {
                bucket().fullscreenNavRepeatState = null;
                return markChanged();
            },
            clearTransitionOwner: function(ownerId) {
                var fullscreen = bucket();
                if (ownerId && fullscreen.fullscreenTransitionOwnerId !== ownerId) return false;
                fullscreen.fullscreenTransitionOwnerId = null;
                markChanged();
                return true;
            },
            domGeneration: function() {
                return bucket().fullscreenDomGeneration || 0;
            },
            navRepeatState: function() {
                return bucket().fullscreenNavRepeatState || null;
            },
            scrollbarRevealReady: function() {
                return !!bucket().fullscreenScrollbarRevealReady;
            },
            setNavRepeatState: function(nextState) {
                bucket().fullscreenNavRepeatState = nextState || null;
                return markChanged();
            },
            setScrollbarRevealReady: function(ready) {
                bucket().fullscreenScrollbarRevealReady = ready !== false;
                return markChanged();
            },
            setScrollbarRevealTimer: function(timerId, cancelPrevious) {
                var fullscreen = bucket();
                if (fullscreen.fullscreenScrollbarRevealTimer !== null &&
                        fullscreen.fullscreenScrollbarRevealTimer !== timerId &&
                        typeof cancelPrevious === 'function') {
                    cancelPrevious(fullscreen.fullscreenScrollbarRevealTimer);
                }
                fullscreen.fullscreenScrollbarRevealTimer =
                    timerId === undefined ? null : timerId;
                return markChanged();
            },
            setTransitionOwner: function(ownerId) {
                bucket().fullscreenTransitionOwnerId = ownerId || null;
                return markChanged();
            },
            transitionOwner: function() {
                return bucket().fullscreenTransitionOwnerId || null;
            }
        };
    }

    function copyKeyMap(source) {
        var copy = {};
        source = source || {};
        for (var key in source) {
            if (!Object.prototype.hasOwnProperty.call(source, key)) continue;
            if (source[key]) copy[key] = true;
        }
        return copy;
    }

    function createVirtualRowsStateService() {
        var bucket = bucketState('virtualization');

        function markVirtualChanged() {
            return bumpServiceEpoch('virtual');
        }

        function markLayoutChanged() {
            bumpServiceEpoch('layout');
            return markVirtualChanged();
        }

        function cancelSupersededFrame(cancelReason) {
            var virtual = bucket();
            if (typeof virtual.virtualRenderFrameCancel === 'function') {
                virtual.virtualRenderFrameCancel(cancelReason || 'superseded-virtual-frame');
            }
            virtual.virtualRenderFrame = null;
            virtual.virtualRenderFrameCancel = null;
        }

        function setVirtualRenderFrame(frameId, cancel) {
            var virtual = bucket();
            if (virtual.virtualRenderFrame !== null &&
                    virtual.virtualRenderFrame !== frameId) {
                cancelSupersededFrame('superseded-virtual-frame');
            }
            virtual.virtualRenderFrame = frameId === undefined ? null : frameId;
            virtual.virtualRenderFrameCancel = typeof cancel === 'function' ? cancel : null;
            return markVirtualChanged();
        }

        return {
            state: bucket,
            bumpTraceCanvasDomGeneration: function() {
                var virtual = bucket();
                virtual.traceCanvasDomGeneration =
                    (Number(virtual.traceCanvasDomGeneration) || 0) + 1;
                markVirtualChanged();
                return virtual.traceCanvasDomGeneration;
            },
            bumpVirtualRowsDomGeneration: function() {
                var virtual = bucket();
                virtual.virtualRowsDomGeneration =
                    (Number(virtual.virtualRowsDomGeneration) || 0) + 1;
                markVirtualChanged();
                return virtual.virtualRowsDomGeneration;
            },
            cancelSupersededFrame: cancelSupersededFrame,
            clearDeferredForResize: function() {
                bucket().virtualRefreshDeferredForResize = false;
                return markVirtualChanged();
            },
            clearVirtualRenderFrame: function(frameId) {
                var virtual = bucket();
                if (frameId !== undefined && virtual.virtualRenderFrame !== frameId) return false;
                virtual.virtualRenderFrame = null;
                virtual.virtualRenderFrameCancel = null;
                markVirtualChanged();
                return true;
            },
            invalidateLayout: function(reason) {
                var virtual = bucket();
                virtual.traceLayoutModel = null;
                virtual.visibleStageCountCache = null;
                virtual.layoutWidthPartsCache = null;
                virtual.traceVirtualLayoutDirty = true;
                virtual.traceLayoutGeneration =
                    (Number(virtual.traceLayoutGeneration) || 0) + 1;
                if (reason) {
                    virtual.traceLayoutDirtyRegions.push({ type: 'layout', reason: reason });
                }
                return markLayoutChanged();
            },
            layoutGeneration: function() {
                return bucket().traceLayoutGeneration || 0;
            },
            layoutModel: function() {
                return bucket().traceLayoutModel || null;
            },
            mountedRuleKeys: function() {
                return copyKeyMap(bucket().mountedRuleKeys);
            },
            mountedStageKeys: function() {
                return copyKeyMap(bucket().mountedStageKeys);
            },
            replaceMountedRuleKeys: function(keys) {
                bucket().mountedRuleKeys = copyKeyMap(keys);
                return markVirtualChanged();
            },
            replaceMountedVirtualRange: function(range) {
                var virtual = bucket();
                virtual.mountedVirtualRange = range ? {
                    left: Math.max(0, Number(range.left) || 0),
                    right: Math.max(0, Number(range.right) || 0)
                } : null;
                return markVirtualChanged();
            },
            replaceMountedStageKeys: function(keys) {
                bucket().mountedStageKeys = copyKeyMap(keys);
                return markVirtualChanged();
            },
            setDeferredForResize: function(deferred) {
                bucket().virtualRefreshDeferredForResize = deferred !== false;
                return markVirtualChanged();
            },
            setLayoutModel: function(model) {
                var virtual = bucket();
                virtual.traceLayoutModel = model || null;
                virtual.traceLayoutGeneration =
                    (Number(virtual.traceLayoutGeneration) || 0) + 1;
                return markLayoutChanged();
            },
            setVirtualRange: function(range) {
                var virtual = bucket();
                var next = {
                    left: Math.max(0, Number(range && range.left) || 0),
                    right: Math.max(0, Number(range && range.right) || 0)
                };
                var previous = virtual.virtualRange || {};
                if (previous.left !== next.left || previous.right !== next.right) {
                    virtual.traceViewportGeneration =
                        (Number(virtual.traceViewportGeneration) || 0) + 1;
                }
                virtual.virtualRange = next;
                return markVirtualChanged();
            },
            setVirtualRenderFrame: setVirtualRenderFrame,
            virtualRange: function() {
                var range = bucket().virtualRange || {};
                return {
                    left: Number(range.left) || 0,
                    right: Number(range.right) || 0
                };
            }
        };
    }

    function createLazyRenderStateService() {
        var bucket = bucketState('lazyRender');

        function markChanged() {
            return bumpServiceEpoch('render');
        }

        function setTimer(field, timerId, cancelPrevious, cancelReason) {
            var lazy = bucket();
            if (lazy[field] !== null && lazy[field] !== timerId &&
                    typeof cancelPrevious === 'function') {
                cancelPrevious(lazy[field], cancelReason || 'superseded-lazy-timer');
            }
            lazy[field] = timerId === undefined ? null : timerId;
            return markChanged();
        }

        return {
            state: bucket,
            clearQueuedRuleRender: function(key) {
                var lazy = bucket();
                if (key) delete lazy.queuedRuleRenders[key];
                lazy.ruleRenderQueue = lazy.ruleRenderQueue.filter(function(job) {
                    return !key || job.key !== key;
                });
                return markChanged();
            },
            clearRuleRenderQueue: function() {
                var lazy = bucket();
                lazy.ruleRenderQueue = [];
                lazy.queuedRuleRenders = {};
                return markChanged();
            },
            dequeueRuleRender: function() {
                var lazy = bucket();
                var job = lazy.ruleRenderQueue.shift() || null;
                if (job && job.key) delete lazy.queuedRuleRenders[job.key];
                markChanged();
                return job;
            },
            enqueueRuleRender: function(job) {
                if (!job || !job.key) return false;
                var lazy = bucket();
                var existing = lazy.queuedRuleRenders[job.key];
                if (existing) {
                    for (var i = 0; i < lazy.ruleRenderQueue.length; i++) {
                        if (lazy.ruleRenderQueue[i].key === job.key) {
                            lazy.ruleRenderQueue[i] = job;
                            break;
                        }
                    }
                } else {
                    lazy.ruleRenderQueue.push(job);
                }
                lazy.queuedRuleRenders[job.key] = job;
                markChanged();
                return true;
            },
            pendingVisibleRuleChecks: function() {
                var checks = {};
                var source = bucket().pendingVisibleRuleChecks || {};
                for (var key in source) {
                    if (!Object.prototype.hasOwnProperty.call(source, key)) continue;
                    checks[key] = source[key];
                }
                return checks;
            },
            queueLength: function() {
                return bucket().ruleRenderQueue.length;
            },
            setPendingVisibleRuleCheck: function(key, check) {
                if (!key) return false;
                bucket().pendingVisibleRuleChecks[key] = check || {};
                markChanged();
                return true;
            },
            setRuleRenderTimer: function(timerId, cancelPrevious) {
                return setTimer('ruleRenderTimer', timerId, cancelPrevious, 'superseded-rule-render-timer');
            },
            setVisibleRuleCheckTimer: function(timerId, cancelPrevious) {
                return setTimer('visibleRuleCheckTimer', timerId, cancelPrevious, 'superseded-visible-rule-check-timer');
            },
            setVisibleRuleScanTimer: function(timerId, cancelPrevious) {
                return setTimer('visibleRuleScanTimer', timerId, cancelPrevious, 'superseded-visible-rule-scan-timer');
            },
            takePendingVisibleRuleChecks: function() {
                var lazy = bucket();
                var checks = {};
                for (var key in lazy.pendingVisibleRuleChecks) {
                    if (!Object.prototype.hasOwnProperty.call(lazy.pendingVisibleRuleChecks, key)) continue;
                    checks[key] = lazy.pendingVisibleRuleChecks[key];
                }
                lazy.pendingVisibleRuleChecks = {};
                markChanged();
                return checks;
            }
        };
    }

    function createTraceSwitchStateService() {
        return {
            activeTraceIndex: function() {
                return tracePrimaryIndex(state.trace.activeTraceSelection);
            },
            activeTraceSelection: function() {
                return Array.isArray(state.trace.activeTraceSelection)
                    ? state.trace.activeTraceSelection.slice()
                    : [tracePrimaryIndex(state.trace.activeTraceSelection)];
            },
            clearVisualOwner: function(ownerId) {
                if (ownerId && state.visual.traceSwitchOwnerId !== ownerId) return false;
                state.visual.traceSwitchOwnerId = null;
                return true;
            },
            setActiveTrace: function(index, options) {
                options = options || {};
                index = Math.floor(Number(index));
                if (!Number.isFinite(index) || index < 0) index = 0;
                state.trace.activeTraceSelection = [index];
                state.trace.activeTraceLoaded = options.loaded === true;
                RuntimeEpochs.bump(state.epochs, 'trace');
                return tracePrimaryIndex(state.trace.activeTraceSelection);
            },
            setActiveTraceSelection: function(selection, options) {
                options = options || {};
                selection = normalizeTraceSelection(selection);
                state.trace.activeTraceSelection = selection.slice();
                state.trace.activeTraceLoaded = options.loaded === true;
                RuntimeEpochs.bump(state.epochs, 'trace');
                return tracePrimaryIndex(state.trace.activeTraceSelection);
            },
            setTraceLoaded: function(loaded) {
                state.trace.activeTraceLoaded = loaded !== false;
                RuntimeEpochs.bump(state.epochs, 'trace');
                return state.trace.activeTraceLoaded;
            },
            setTraceStoreStatus: function(status) {
                status = status || {};
                state.trace.traceStoreStatus = {
                    state: status.state || 'unloaded',
                    title: status.title || '',
                    message: status.message || ''
                };
                RuntimeEpochs.bump(state.epochs, 'trace');
                return state.trace.traceStoreStatus;
            },
            setVisualOwner: function(ownerId) {
                state.visual.traceSwitchOwnerId = ownerId || null;
                return state.visual.traceSwitchOwnerId;
            },
            state: function() {
                var selection = Array.isArray(state.trace.activeTraceSelection)
                    ? state.trace.activeTraceSelection.slice()
                    : [tracePrimaryIndex(state.trace.activeTraceSelection)];
                return {
                    activeTraceIndex: tracePrimaryIndex(selection),
                    activeTraceSelection: selection,
                    activeTraceLoaded: state.trace.activeTraceLoaded,
                    traceStoreStatus: state.trace.traceStoreStatus,
                    visualOwnerId: state.visual.traceSwitchOwnerId
                };
            },
            visualOwnerId: function() {
                return state.visual.traceSwitchOwnerId || null;
            }
        };
    }

    var services = {
        actionEvents: { state: bucketState('actionEvents') },
        diagnostics: { state: bucketState('diagnostics') },
        diff: createDiffStateService(),
        diffArrows: { state: bucketState('diffArrows') },
        epochs: { state: bucketState('epochs') },
        frame: { state: bucketState('frame') },
        fullscreen: createFullscreenStateService(),
        lazyRender: createLazyRenderStateService(),
        mount: { state: bucketState('mount') },
        resize: { state: bucketState('resize') },
        search: createSearchStateService(),
        trace: {
            state: bucketState('trace'),
            allRules: function() { return state.trace.allRules; },
            ruleFeatureCache: function() { return state.trace.ruleFeatureCache; },
            store: function() { return state.trace.traceStore; },
            uiState: function() { return state.trace.uiState; }
        },
        traceAnchor: { state: bucketState('traceAnchor') },
        traceSwitch: createTraceSwitchStateService(),
        visual: { state: bucketState('visual') },
        virtualization: createVirtualRowsStateService()
    };

    return {
        state: state,
        services: services,
        collapsedIndicators: collapsedIndicators
    };
}

var TraceRuntime = createTraceRuntime();

function traceRuntime() {
    return TraceRuntime.services.trace.state();
}

function currentUiState() {
    return TraceRuntime.services.trace.uiState();
}

function currentAllRules() {
    return TraceRuntime.services.trace.allRules();
}

function currentRuleFeatureCache() {
    return TraceRuntime.services.trace.ruleFeatureCache();
}

function currentRuntimeEpoch() {
    return TraceRuntime.services.epochs.state();
}

function searchRuntime() {
    return TraceRuntime.services.search.state();
}

function virtualRuntime() {
    return TraceRuntime.services.virtualization.state();
}

function frameRuntime() {
    return TraceRuntime.services.frame.state();
}

function visualRuntime() {
    return TraceRuntime.services.visual.state();
}

function diffRuntime() {
    return TraceRuntime.services.diff.state();
}

function lazyRuntime() {
    return TraceRuntime.services.lazyRender.state();
}

function diffArrowRuntime() {
    return TraceRuntime.services.diffArrows.state();
}

function currentDiffRenderOwnerId() {
    return diffRuntime().diffTransitionOwnerId || '';
}

function fullscreenRuntime() {
    return TraceRuntime.services.fullscreen.state();
}

function resizeRuntime() {
    return TraceRuntime.services.resize.state();
}

function traceAnchorRuntime() {
    return TraceRuntime.services.traceAnchor.state();
}

function actionEventsRuntime() {
    return TraceRuntime.services.actionEvents.state();
}

function mountRuntime() {
    return TraceRuntime.services.mount.state();
}

function diagnosticsRuntime() {
    return TraceRuntime.services.diagnostics.state();
}

function cloneVisualDetails(details) {
    if (!details || typeof details !== 'object') return {};
    var copy = {};
    for (var key in details) {
        if (!Object.prototype.hasOwnProperty.call(details, key)) continue;
        copy[key] = details[key];
    }
    return copy;
}

function createVisualSurfaceOwner(kind, label, surfaces, details) {
    var visual = visualRuntime();
    var id = (kind || 'owner') + '-' + visual.nextOwnerId++;
    return {
        id: id,
        kind: kind || 'owner',
        label: label || '',
        surfaces: (surfaces || []).slice(),
        traceEpoch: currentRuntimeEpoch().trace,
        renderEpoch: currentRuntimeEpoch().render,
        virtualEpoch: currentRuntimeEpoch().virtual,
        startedAt: Date.now ? Date.now() : 0,
        details: cloneVisualDetails(details)
    };
}

function cloneVisualOwner(owner) {
    if (!owner) return null;
    return {
        id: owner.id,
        kind: owner.kind,
        label: owner.label,
        surface: owner.surface,
        surfaces: (owner.surfaces || []).slice(),
        traceEpoch: owner.traceEpoch,
        renderEpoch: owner.renderEpoch,
        virtualEpoch: owner.virtualEpoch,
        startedAt: owner.startedAt,
        details: cloneVisualDetails(owner.details)
    };
}

function visualSurfaceOwner(surface) {
    return visualRuntime().surfaceOwners[surface] || null;
}

function recordVisualCommitDenied(surface, label, reason, details) {
    var visual = visualRuntime();
    visual.deniedCommits.unshift({
        surface: surface || '',
        label: label || '',
        reason: reason || 'denied',
        owner: cloneVisualOwner(visualSurfaceOwner(surface)),
        traceEpoch: currentRuntimeEpoch().trace,
        renderEpoch: currentRuntimeEpoch().render,
        virtualEpoch: currentRuntimeEpoch().virtual,
        details: cloneVisualDetails(details)
    });
    if (visual.deniedCommits.length > 20) {
        visual.deniedCommits.length = 20;
    }
}

function acquireVisualSurfaceOwner(surface, owner) {
    if (!surface || !owner || !owner.id) return false;
    var active = visualSurfaceOwner(surface);
    if (active && active.id !== owner.id) {
        recordVisualCommitDenied(surface, owner.label, 'surface_already_owned', {
            requestedOwnerId: owner.id,
            activeOwnerId: active.id
        });
        return false;
    }

    var copy = cloneVisualOwner(owner);
    copy.surface = surface;
    visualRuntime().surfaceOwners[surface] = copy;
    return true;
}

function acquireVisualSurfaceOwners(surfaces, owner) {
    surfaces = surfaces || [];
    for (var i = 0; i < surfaces.length; i++) {
        var active = visualSurfaceOwner(surfaces[i]);
        if (active && active.id !== owner.id) {
            recordVisualCommitDenied(surfaces[i], owner.label, 'surface_already_owned', {
                requestedOwnerId: owner.id,
                activeOwnerId: active.id
            });
            return false;
        }
    }

    for (var j = 0; j < surfaces.length; j++) {
        acquireVisualSurfaceOwner(surfaces[j], owner);
    }
    return true;
}

function releaseVisualSurfaceOwners(ownerId) {
    var owners = visualRuntime().surfaceOwners;
    var released = false;
    for (var surface in owners) {
        if (!Object.prototype.hasOwnProperty.call(owners, surface)) continue;
        if (!ownerId || owners[surface].id === ownerId) {
            delete owners[surface];
            released = true;
        }
    }
    return released;
}

function clearVisualSurfaceOwners() {
    visualRuntime().surfaceOwners = {};
}

function traceSwitchVisualSurfaces() {
    return [
        'virtual-rows',
        'trace-canvas',
        'trace-scrollbar',
        'rule-cell',
        'rule-tree',
        'rule-info',
        'search-marks',
        'diff-arrows',
        'fullscreen-shell',
        'trace-anchor-line',
        'collapsed-label-lens',
        'control-chrome'
    ];
}

function withTraceSwitchVisualTransition(label, details, fn) {
    if (typeof fn !== 'function') return false;

    var visual = visualRuntime();
    if (visual.traceSwitchOwnerId) return fn();

    clearVisualSurfaceOwners();
    var owner = createVisualSurfaceOwner(
        'trace-switch',
        label || 'trace-switch',
        traceSwitchVisualSurfaces(),
        details
    );
    if (!acquireVisualSurfaceOwners(owner.surfaces, owner)) return false;

    TraceRuntime.services.traceSwitch.setVisualOwner(owner.id);
    try {
        return fn(owner);
    } finally {
        TraceRuntime.services.traceSwitch.clearVisualOwner(owner.id);
        releaseVisualSurfaceOwners(owner.id);
    }
}

function canCommitVisualWork(surface, label, options) {
    options = options || {};
    var owner = visualSurfaceOwner(surface);
    if (!owner) return true;
    if (options.ownerId && owner.id === options.ownerId) return true;
    if (visualRuntime().traceSwitchOwnerId && owner.id === visualRuntime().traceSwitchOwnerId) return true;

    recordVisualCommitDenied(surface, label, options.reason || 'surface_owned', {
        requestedOwnerId: options.ownerId || '',
        activeOwnerId: owner.id
    });
    return false;
}

function searchMarkCommitTransactionCurrent(label, details) {
    details = details || {};
    if (details.allowWithoutSearchTransaction) return true;

    var token = Object.prototype.hasOwnProperty.call(details, 'searchTransactionToken')
        ? details.searchTransactionToken
        : activeSearchTransactionToken();
    var currentness = details.searchTransactionCurrentness || { payload: true };
    if (token && searchTransactionTokenCurrent(token, currentness)) return true;

    recordVisualCommitDenied(
        'search-marks',
        label,
        token ? 'stale_search_transaction' : 'missing_search_transaction_token',
        details
    );
    return false;
}

function canCommitSearchMarks(label, target, details) {
    details = cloneVisualDetails(details);
    if (searchRuntime().searchMarksTransitionOwnerId && details.ownerId === undefined) {
        details.ownerId = searchRuntime().searchMarksTransitionOwnerId;
    }
    if (details.searchMarksDomGeneration !== undefined &&
            !searchMarksDomGenerationCurrent(details.searchMarksDomGeneration)) {
        recordVisualCommitDenied('search-marks', label, 'stale_dom_generation', details);
        return false;
    }
    if (details.virtualRowsDomGeneration !== undefined &&
            !virtualRowsDomGenerationCurrent(details.virtualRowsDomGeneration)) {
        recordVisualCommitDenied('search-marks', label, 'stale_dom_generation', details);
        return false;
    }
    if (!canCommitVisualWork('search-marks', label, details)) return false;
    if (target && target.isConnected === false) {
        if (target.id) details.targetId = target.id;
        recordVisualCommitDenied('search-marks', label, 'detached_target', details);
        return false;
    }
    if (!searchMarkCommitTransactionCurrent(label, details)) return false;
    return true;
}

function currentSearchMarksDomGeneration() {
    return searchRuntime().searchMarksDomGeneration || 0;
}

function bumpSearchMarksDomGeneration() {
    searchRuntime().searchMarksDomGeneration = currentSearchMarksDomGeneration() + 1;
    return searchRuntime().searchMarksDomGeneration;
}

function searchMarksDomGenerationCurrent(generation) {
    return Number(generation) === currentSearchMarksDomGeneration();
}

function currentDiffArrowsDomGeneration() {
    return diffArrowRuntime().diffArrowsDomGeneration || 0;
}

function bumpDiffArrowsDomGeneration() {
    diffArrowRuntime().diffArrowsDomGeneration = currentDiffArrowsDomGeneration() + 1;
    return diffArrowRuntime().diffArrowsDomGeneration;
}

function diffArrowsDomGenerationCurrent(generation) {
    return Number(generation) === currentDiffArrowsDomGeneration();
}

function currentFullscreenDomGeneration() {
    return fullscreenRuntime().fullscreenDomGeneration || 0;
}

function bumpFullscreenDomGeneration() {
    fullscreenRuntime().fullscreenDomGeneration = currentFullscreenDomGeneration() + 1;
    return fullscreenRuntime().fullscreenDomGeneration;
}

function fullscreenDomGenerationCurrent(generation) {
    return Number(generation) === currentFullscreenDomGeneration();
}

function traceBuildProfile() {
    if (typeof optimizerTraceBuildProfile === 'function') {
        return optimizerTraceBuildProfile();
    }
    return 'debug';
}

function traceDiagnosticsEnabled() {
    return traceBuildProfile() !== 'release';
}

function cloneInvariantIssue(issue) {
    issue = issue || {};
    var copy = {
        code: issue.code || 'invariant_failed',
        path: issue.path || '',
        message: issue.message || ''
    };
    if (issue.details !== undefined) copy.details = issue.details;
    return copy;
}

function recordInvariantFailure(kind, context, validation, summary) {
    var diagnostics = diagnosticsRuntime();
    var entry = {
        kind: kind || 'invariant',
        context: context || '',
        summary: summary || 'Invariant failed',
        traceGeneration: currentRuntimeEpoch().trace,
        renderGeneration: currentRuntimeEpoch().render,
        searchGeneration: currentRuntimeEpoch().search,
        diffGeneration: currentRuntimeEpoch().diff,
        virtualGeneration: currentRuntimeEpoch().virtual,
        layoutGeneration: currentRuntimeEpoch().layout,
        fullscreenGeneration: currentRuntimeEpoch().fullscreen,
        resizeGeneration: currentRuntimeEpoch().resize,
        errors: []
    };

    if (validation && validation.errors && validation.errors.length) {
        for (var i = 0; i < validation.errors.length && i < 8; i++) {
            entry.errors.push(cloneInvariantIssue(validation.errors[i]));
        }
        entry.errorCount = validation.errors.length;
    } else if (validation && validation.code) {
        entry.errors.push(cloneInvariantIssue(validation));
        entry.errorCount = 1;
    } else {
        entry.errorCount = 0;
    }

    diagnostics.lastInvariantFailures.unshift(entry);
    if (diagnostics.lastInvariantFailures.length > 8) {
        diagnostics.lastInvariantFailures.length = 8;
    }
    return entry;
}

function throwRecordedInvariantFailure(kind, context, code, path, message, details) {
    var validation = {
        ok: false,
        errors: [{
            code: code,
            path: path,
            message: message,
            details: details
        }]
    };
    recordInvariantFailure(kind, context, validation, message);
    throw new Error(message);
}

var RULE_WIDTH_DEFAULT = 520;
var RULE_WIDTH_MIN = 360;
var RULE_WIDTH_MAX_FALLBACK = 900;
var RULE_WIDTH_MAX_ABSOLUTE = 1400;
var RULE_WIDTH_MAX_VIEWPORT_RATIO = 0.9;
var RULE_WIDTH_STORAGE_KEY = 'optimizerTraceRuleWidth';
var RULE_COLLAPSED_WIDTH = 16;
var GROUP_COLLAPSED_WIDTH = RULE_COLLAPSED_WIDTH;
var RULE_GAP_WIDTH = 1;
var STAGE_COLLAPSED_WIDTH = 28;
var STAGE_GAP_WIDTH = 3;
var EMPTY_STAGE_EXPANDED_WIDTH = 390;
var STAGE_SHELL_VIRTUALIZATION_THRESHOLD = 160;
var STAGE_SHELL_OVERSCAN_SCREENS = 0.75;
var STAGE_SHELL_MAX_MOUNTED = 120;
var DIFF_ENDPOINT_STAGE_CONTEXT = 1;
var GROUP_SEARCH_TITLE_WIDTH = 220;
var TRACE_PADDING_LEFT = 16;
var TRACE_PADDING_RIGHT = 16;
var STAGE_TITLE_FALLBACK_MAX_WIDTH = 420;
var STAGE_TITLE_FALLBACK_BASE_WIDTH = 58;
var STAGE_TITLE_FALLBACK_CHAR_WIDTH = 6.7;
var GROUP_TITLE_FALLBACK_MAX_WIDTH = 16384;
var GROUP_TITLE_FALLBACK_BASE_WIDTH = 75;
var GROUP_TITLE_FALLBACK_CHAR_WIDTH = 6.2;
var RULE_TITLE_FALLBACK_MIN_WIDTH = 160;
var RULE_TITLE_FALLBACK_MAX_WIDTH = 16384;
var RULE_TITLE_FALLBACK_BASE_WIDTH = 52;
var RULE_TITLE_FALLBACK_CHAR_WIDTH = 8.8;
var RULE_TITLE_FALLBACK_NAV_CONTROL_WIDTH = 42;
var RULE_TITLE_FALLBACK_DIFF_CONTROL_WIDTH = 30;
var RULE_TITLE_FALLBACK_FEATURE_CONTROL_WIDTH = 16;

function bumpRuntimeEpoch(scope) {
    return RuntimeEpochs.bump(currentRuntimeEpoch(), scope);
}

function runtimeToken() {
    return RuntimeEpochs.traceToken(currentRuntimeEpoch());
}

function runtimeTokenCurrent(token) {
    return RuntimeEpochs.tokenCurrent(currentRuntimeEpoch(), token);
}

function runtimeJobCurrent(job) {
    return RuntimeEpochs.renderJobCurrent(currentRuntimeEpoch(), job);
}

var RENDER_FRAME_PHASES = {
    model: true,
    dom: true,
    deferred: true
};

var RENDER_FRAME_EPOCH_SCOPES = [
    'trace',
    'render',
    'search',
    'diff',
    'virtual',
    'layout',
    'fullscreen',
    'resize'
];

var VISUAL_COMMIT_CONTEXT = typeof Symbol === 'function'
    ? Symbol('optimizer-trace-visual-commit-context')
    : '__optimizerTraceVisualCommitContext';

function normalizeRenderFramePhase(phase) {
    return RENDER_FRAME_PHASES[phase] ? phase : 'dom';
}

function cloneVisualDomGenerations(generations) {
    var copy = {};
    generations = generations || {};
    for (var key in generations) {
        if (!Object.prototype.hasOwnProperty.call(generations, key)) continue;
        copy[key] = generations[key];
    }
    return copy;
}

function captureCurrentVisualDomGenerations() {
    var generations = {
        searchMarks: currentSearchMarksDomGeneration(),
        diffArrows: currentDiffArrowsDomGeneration(),
        fullscreen: currentFullscreenDomGeneration()
    };
    if (typeof currentVirtualRowsDomGeneration === 'function') {
        generations.virtualRows = currentVirtualRowsDomGeneration();
    }
    if (typeof currentTraceCanvasDomGeneration === 'function') {
        generations.traceCanvas = currentTraceCanvasDomGeneration();
    }
    return generations;
}

function visualDomGenerationCurrent(name, generation) {
    if (name === 'searchMarks') return searchMarksDomGenerationCurrent(generation);
    if (name === 'diffArrows') return diffArrowsDomGenerationCurrent(generation);
    if (name === 'fullscreen') return fullscreenDomGenerationCurrent(generation);
    if (name === 'virtualRows' && typeof virtualRowsDomGenerationCurrent === 'function') {
        return virtualRowsDomGenerationCurrent(generation);
    }
    if (name === 'traceCanvas' && typeof traceCanvasDomGenerationCurrent === 'function') {
        return traceCanvasDomGenerationCurrent(generation);
    }
    return true;
}

function createVisualCommitContext(job, options) {
    job = job || {};
    options = options || {};
    var frame = frameRuntime();
    var ctx = {
        id: frame.nextVisualContextId++,
        jobId: job.id || 0,
        frameId: frame.activeFrameId,
        phase: options.phase || job.phase || '',
        label: job.label || options.label || '',
        ownerId: job.ownerId || options.ownerId || '',
        surfaces: cloneRenderFrameSurfaces(job.surfaces || options.surfaces),
        epochToken: cloneRenderFrameToken(job.token || options.token),
        domGenerations: captureCurrentVisualDomGenerations()
    };
    ctx[VISUAL_COMMIT_CONTEXT] = true;
    return ctx;
}

function visualCommitContextIsBranded(ctx) {
    return !!ctx && ctx[VISUAL_COMMIT_CONTEXT] === true;
}

function visualCommitContextCurrent(ctx) {
    return visualCommitContextIsBranded(ctx) &&
        renderFrameEpochCurrent(ctx.epochToken);
}

function assertVisualCommitContext(ctx) {
    if (!visualCommitContextIsBranded(ctx)) {
        throw new Error('visual commit requires scheduler context');
    }
    if (!renderFrameEpochCurrent(ctx.epochToken)) {
        throw new Error('visual commit context is stale');
    }
    return ctx;
}

function visualCommitContextIncludesSurface(ctx, surface) {
    var surfaces = ctx && ctx.surfaces || [];
    for (var i = 0; i < surfaces.length; i++) {
        if (surfaces[i] === surface) return true;
    }
    return false;
}

function visualCommitContextHasEpochScope(ctx, scope) {
    return !!ctx && !!ctx.epochToken &&
        Object.prototype.hasOwnProperty.call(ctx.epochToken, scope);
}

function visualCommitContextHasDomGeneration(ctx, name) {
    return !!ctx && !!ctx.domGenerations &&
        Object.prototype.hasOwnProperty.call(ctx.domGenerations, name);
}

function createVisualSurface(config) {
    config = config || {};
    var name = config.name || '';
    var epochScopes = (config.epochScopes || []).slice();
    var domGenerations = (config.domGenerations || []).slice();
    var resolveTarget = typeof config.resolveTarget === 'function'
        ? config.resolveTarget
        : function() { return null; };
    var allowMissingTarget = !!config.allowMissingTarget;

    return {
        name: name,
        epochScopes: epochScopes.slice(),
        domGenerations: domGenerations.slice(),
        commit: function(ctx, options, writer) {
            assertVisualCommitContext(ctx);
            options = options || {};
            var label = options.label || ctx.label;
            if (typeof writer !== 'function') {
                throw new Error('visual surface commit requires writer');
            }
            if (!name || !visualCommitContextIncludesSurface(ctx, name)) {
                recordVisualCommitDenied(name, label, 'surface_not_declared', {
                    visualContextId: ctx.id,
                    contextSurfaces: cloneRenderFrameSurfaces(ctx.surfaces)
                });
                return false;
            }
            for (var i = 0; i < epochScopes.length; i++) {
                if (!visualCommitContextHasEpochScope(ctx, epochScopes[i])) {
                    recordVisualCommitDenied(name, label, 'missing_epoch_scope', {
                        visualContextId: ctx.id,
                        missingEpochScope: epochScopes[i]
                    });
                    return false;
                }
            }
            for (var j = 0; j < domGenerations.length; j++) {
                var generationName = domGenerations[j];
                if (!visualCommitContextHasDomGeneration(ctx, generationName) ||
                        !visualDomGenerationCurrent(generationName, ctx.domGenerations[generationName])) {
                    recordVisualCommitDenied(name, label, 'stale_dom_generation', {
                        visualContextId: ctx.id,
                        domGeneration: generationName
                    });
                    return false;
                }
            }
            var expectedDomGenerations = options.domGenerations || {};
            for (var expectedName in expectedDomGenerations) {
                if (!Object.prototype.hasOwnProperty.call(expectedDomGenerations, expectedName)) continue;
                if (!visualDomGenerationCurrent(expectedName, expectedDomGenerations[expectedName])) {
                    recordVisualCommitDenied(name, label, 'stale_dom_generation', {
                        visualContextId: ctx.id,
                        domGeneration: expectedName
                    });
                    return false;
                }
            }
            if (!canCommitVisualWork(name, label, {
                    ownerId: ctx.ownerId,
                    reason: 'surface_owned',
                    visualContextId: ctx.id
            })) {
                return false;
            }
            var target = resolveTarget(options.ref || options, options, ctx);
            if (!target && !allowMissingTarget) {
                recordVisualCommitDenied(name, label, 'missing_target', {
                    visualContextId: ctx.id
                });
                return false;
            }
            if (target && target.isConnected === false) {
                recordVisualCommitDenied(name, label, options.detachedReason || 'detached_target', {
                    visualContextId: ctx.id,
                    targetId: target.id || ''
                });
                return false;
            }
            writer(target, options, ctx);
            return true;
        }
    };
}

function runVisualSurfaceCommitNow(label, surfaces, epochScopes, callback) {
    var result;
    var ownerId = '';
    if (surfaces && surfaces.indexOf('rule-tree') >= 0 &&
            typeof currentDiffRenderOwnerId === 'function') {
        ownerId = currentDiffRenderOwnerId() || '';
    }
    if (!ownerId && surfaces && surfaces.indexOf('diff-arrows') >= 0 &&
            typeof currentDiffRenderOwnerId === 'function') {
        ownerId = currentDiffRenderOwnerId() || '';
    }
    if (!ownerId && surfaces && surfaces.indexOf('search-marks') >= 0 &&
            typeof searchRuntime === 'function') {
        ownerId = searchRuntime().searchMarksTransitionOwnerId || '';
    }
    if (!ownerId && surfaces && surfaces.indexOf('fullscreen-shell') >= 0 &&
            typeof fullscreenRuntime === 'function') {
        ownerId = fullscreenRuntime().fullscreenTransitionOwnerId || '';
    }
    runRenderFramePhaseNow('deferred', label, function(visualCtx) {
        result = callback(visualCtx);
    }, {
        epochScopes: epochScopes || ['trace', 'render'],
        label: label,
        ownerId: ownerId,
        surfaces: surfaces,
        withVisualContext: true
    });
    return result;
}

function renderFrameRequest(callback) {
    var frame = (typeof window !== 'undefined' && window.requestAnimationFrame) ||
        function(fn) { return setTimeout(fn, 0); };
    return frame(callback);
}

function renderFrameCancel(id) {
    if (id === null || id === undefined) return;
    var cancelFrame = (typeof window !== 'undefined' && window.cancelAnimationFrame) ||
        clearTimeout;
    cancelFrame(id);
}

function scheduleRuntimeTimeout(callback, delay, options) {
    options = options || {};
    var token = options.token || renderFrameEpochToken(options.epochScopes || ['trace']);
    var label = options.label || 'runtime-timeout';
    return setTimeout(function() {
        if (!renderFrameEpochCurrent(token)) {
            recordRenderFrameDiscard(label, 'stale-epoch');
            return;
        }
        callback();
    }, delay);
}

function renderFrameEpochToken(scopes) {
    var runtimeEpoch = currentRuntimeEpoch();
    var selected = scopes && scopes.length ? scopes : RENDER_FRAME_EPOCH_SCOPES;
    var token = {};
    for (var i = 0; i < selected.length; i++) {
        var scope = selected[i];
        token[scope] = runtimeEpoch[scope];
    }
    return token;
}

function renderFrameEpochCurrent(token) {
    if (!token) return true;
    var runtimeEpoch = currentRuntimeEpoch();
    for (var scope in token) {
        if (!Object.prototype.hasOwnProperty.call(token, scope)) continue;
        if (runtimeEpoch[scope] !== token[scope]) return false;
    }
    return true;
}

function cloneRenderFrameToken(token) {
    var copy = {};
    token = token || {};
    for (var scope in token) {
        if (!Object.prototype.hasOwnProperty.call(token, scope)) continue;
        copy[scope] = token[scope];
    }
    return copy;
}

function cloneRenderFrameSurfaces(surfaces) {
    return Array.isArray(surfaces) ? surfaces.slice() : [];
}

function renderFrameLog(phase, label, status, details) {
    var frame = frameRuntime();
    details = details || {};
    var visualContext = details.visualContext || null;
    frame.lastFrameLog.push({
        frameId: frame.activeFrameId,
        visualContextId: visualContext ? visualContext.id : 0,
        visualContextLabel: visualContext ? visualContext.label : '',
        phase: phase,
        label: label || '',
        status: status || 'run',
        discardReason: status && status !== 'run' ? status : '',
        ownerId: details.ownerId || '',
        surfaces: cloneRenderFrameSurfaces(details.surfaces),
        epochToken: cloneRenderFrameToken(details.token),
        domGenerations: cloneVisualDomGenerations(
            visualContext ? visualContext.domGenerations : details.domGenerations
        ),
        traceEpoch: currentRuntimeEpoch().trace,
        renderEpoch: currentRuntimeEpoch().render,
        searchEpoch: currentRuntimeEpoch().search,
        diffEpoch: currentRuntimeEpoch().diff,
        virtualEpoch: currentRuntimeEpoch().virtual,
        layoutEpoch: currentRuntimeEpoch().layout,
        fullscreenEpoch: currentRuntimeEpoch().fullscreen,
        resizeEpoch: currentRuntimeEpoch().resize
    });
    if (frame.lastFrameLog.length > 80) {
        frame.lastFrameLog.splice(0, frame.lastFrameLog.length - 80);
    }
}

function recordRenderFrameDiscard(label, reason) {
    label = label || '';
    reason = reason || 'discarded';
    var discardedByLabel = frameRuntime().discardedByLabel;
    if (!discardedByLabel[label]) discardedByLabel[label] = {};
    discardedByLabel[label][reason] = (discardedByLabel[label][reason] || 0) + 1;
}

function clearRenderFrameJobKey(job) {
    if (!job || !job.key) return;
    if (frameRuntime().queuedKeys[job.phase][job.key] === job) {
        delete frameRuntime().queuedKeys[job.phase][job.key];
    }
}

function discardRenderFrameJob(job, reason) {
    if (!job || job.discarded) return;
    job.discarded = true;
    if (job.handle) job.handle.cancelled = true;
    clearRenderFrameJobKey(job);
    frameRuntime().discardedJobs++;
    renderFrameLog(job.phase, job.label, reason || 'discarded', {
        token: job.token,
        ownerId: job.ownerId,
        surfaces: job.surfaces
    });
    recordRenderFrameDiscard(job.label, reason || 'discarded');
    if (job.onDiscard) job.onDiscard(reason || 'discarded');
}

function renderFrameJobCurrent(job) {
    return !!job && !job.cancelled && !(job.handle && job.handle.cancelled) &&
        renderFrameEpochCurrent(job.token);
}

function renderFrameJobCanCommit(job) {
    if (!job || !job.surfaces || !job.surfaces.length) return true;
    for (var i = 0; i < job.surfaces.length; i++) {
        if (!canCommitVisualWork(job.surfaces[i], job.label, {
            ownerId: job.ownerId,
            reason: 'frame_surface_owned',
            frameJobId: job.id
        })) {
            return false;
        }
    }
    return true;
}

function runRenderFrameJob(job, visualContext) {
    return job.run(visualContext);
}

function drainRenderFramePhase(phase) {
    var frame = frameRuntime();
    var queue = frame.queues[phase];
    frame.queues[phase] = [];
    frame.queuedKeys[phase] = {};
    frame.activePhase = phase;

    for (var i = 0; i < queue.length; i++) {
        var job = queue[i];
        if (!renderFrameJobCurrent(job)) {
            discardRenderFrameJob(job, job && job.cancelled ? 'cancelled' : 'stale-epoch');
            continue;
        }
        if (!renderFrameJobCanCommit(job)) {
            discardRenderFrameJob(job, 'surface-owned');
            continue;
        }

        clearRenderFrameJobKey(job);
        var visualContext = createVisualCommitContext(job, { phase: phase });
        if (!visualCommitContextCurrent(visualContext)) {
            discardRenderFrameJob(job, 'stale-context');
            continue;
        }
        renderFrameLog(phase, job.label, 'run', {
            token: job.token,
            ownerId: job.ownerId,
            surfaces: job.surfaces,
            visualContext: visualContext
        });
        runRenderFrameJob(job, visualContext);
    }
}

function hasRenderFrameWork(phase) {
    return frameRuntime().queues[phase].length > 0;
}

function hasLiveRenderFrameWork(phase) {
    var queue = frameRuntime().queues[phase];
    for (var i = 0; i < queue.length; i++) {
        var job = queue[i];
        if (job && !job.discarded && !job.cancelled &&
                !(job.handle && job.handle.cancelled)) {
            return true;
        }
    }
    return false;
}

function hasRenderFrameCommitWork() {
    return hasRenderFrameWork('model') || hasRenderFrameWork('dom');
}

function cancelIdleRenderFramesIfEmpty() {
    var frame = frameRuntime();
    if (frame.activePhase !== 'idle' ||
            hasLiveRenderFrameWork('model') ||
            hasLiveRenderFrameWork('dom') ||
            hasLiveRenderFrameWork('deferred')) {
        return;
    }
    if (frame.frameId !== null) {
        renderFrameCancel(frame.frameId);
        frame.frameId = null;
    }
    if (frame.deferredFrameId !== null) {
        renderFrameCancel(frame.deferredFrameId);
        frame.deferredFrameId = null;
    }
}

function scheduleRenderDeferredFrame() {
    var frame = frameRuntime();
    if (frame.deferredFrameId !== null || !hasRenderFrameWork('deferred')) return;
    frame.deferredFrameId = renderFrameRequest(function runRenderDeferredFrame() {
        frameRuntime().deferredFrameId = null;
        frameRuntime().activeFrameId++;
        drainRenderFramePhase('deferred');
        frameRuntime().activePhase = 'idle';
        frameRuntime().completedFrames++;
        if (hasRenderFrameCommitWork()) scheduleRenderFrame();
        else if (hasRenderFrameWork('deferred')) scheduleRenderDeferredFrame();
    });
}

function runRenderFrame() {
    frameRuntime().frameId = null;
    frameRuntime().activeFrameId++;
    drainRenderFramePhase('model');
    drainRenderFramePhase('dom');
    frameRuntime().activePhase = 'idle';
    frameRuntime().completedFrames++;

    scheduleRenderDeferredFrame();
    if (hasRenderFrameCommitWork()) scheduleRenderFrame();
}

function scheduleRenderFrame() {
    var frame = frameRuntime();
    if (frame.frameId !== null) return;
    frame.frameId = renderFrameRequest(runRenderFrame);
}

function renderFrameJobHandle(job) {
    return {
        __traceRenderFrameJob: true,
        id: job.id,
        phase: job.phase,
        key: job.key || '',
        cancelled: false,
        job: job
    };
}

function scheduleRenderFrameWork(phase, key, callback, options) {
    phase = normalizeRenderFramePhase(phase);
    options = options || {};
    var frame = frameRuntime();
    var label = options.label || key || phase;
    var token = options.token || renderFrameEpochToken(options.epochScopes);

    if (key) {
        var existing = frame.queuedKeys[phase][key];
        if (existing && !existing.discarded && !(existing.handle && existing.handle.cancelled)) {
            existing.run = callback;
            existing.token = token;
            existing.label = label;
            existing.onDiscard = options.onDiscard || null;
            existing.ownerId = options.ownerId || '';
            existing.surfaces = cloneRenderFrameSurfaces(options.surfaces);
            return existing.handle;
        }
    }

    var job = {
        id: frame.nextJobId++,
        phase: phase,
        key: key || '',
        label: label,
        token: token,
        ownerId: options.ownerId || '',
        surfaces: cloneRenderFrameSurfaces(options.surfaces),
        run: callback,
        onDiscard: options.onDiscard || null,
        cancelled: false,
        discarded: false,
        handle: null
    };
    job.handle = renderFrameJobHandle(job);
    frame.queues[phase].push(job);
    if (key) frame.queuedKeys[phase][key] = job;

    if (frame.activePhase !== 'idle') return job.handle;

    if (phase === 'deferred' && !hasRenderFrameCommitWork() && frame.frameId === null) {
        scheduleRenderDeferredFrame();
    } else {
        scheduleRenderFrame();
    }
    return job.handle;
}

function scheduleRenderModelWork(key, callback, options) {
    return scheduleRenderFrameWork('model', key, callback, options);
}

function scheduleRenderDomWork(key, callback, options) {
    return scheduleRenderFrameWork('dom', key, callback, options);
}

function scheduleRenderDeferredWork(key, callback, options) {
    return scheduleRenderFrameWork('deferred', key, callback, options);
}

function cancelRenderFrameWork(handle) {
    if (!handle || !handle.__traceRenderFrameJob) return false;
    handle.cancelled = true;
    if (handle.job) {
        handle.job.cancelled = true;
        discardRenderFrameJob(handle.job, 'cancelled');
    }
    cancelIdleRenderFramesIfEmpty();
    return true;
}

function runRenderFramePhaseNow(phase, label, callback, options) {
    phase = normalizeRenderFramePhase(phase);
    options = options || {};
    var token = options.token || renderFrameEpochToken(options.epochScopes);
    if (!renderFrameEpochCurrent(token)) {
        frameRuntime().discardedJobs++;
        renderFrameLog(phase, label, 'stale-epoch', {
            token: token,
            ownerId: options.ownerId || '',
            surfaces: options.surfaces
        });
        recordRenderFrameDiscard(label, 'stale-epoch');
        if (options.onDiscard) options.onDiscard('stale-epoch');
        return false;
    }

    var frame = frameRuntime();
    var previousPhase = frame.activePhase;
    if (frame.activePhase === 'idle') frame.activeFrameId++;
    frame.activePhase = phase;
    var visualContext = createVisualCommitContext({
        id: 0,
        phase: phase,
        label: label,
        token: token,
        ownerId: options.ownerId || '',
        surfaces: cloneRenderFrameSurfaces(options.surfaces)
    }, { phase: phase });
    if (!visualCommitContextCurrent(visualContext)) {
        frameRuntime().discardedJobs++;
        renderFrameLog(phase, label, 'stale-context', {
            token: token,
            ownerId: options.ownerId || '',
            surfaces: options.surfaces,
            visualContext: visualContext
        });
        recordRenderFrameDiscard(label, 'stale-context');
        if (options.onDiscard) options.onDiscard('stale-context');
        frame.activePhase = previousPhase;
        if (previousPhase === 'idle') {
            if (hasLiveRenderFrameWork('model') || hasLiveRenderFrameWork('dom')) {
                scheduleRenderFrame();
            } else if (hasLiveRenderFrameWork('deferred')) {
                scheduleRenderDeferredFrame();
            }
        }
        return false;
    }
    renderFrameLog(phase, label, 'run', {
        token: token,
        ownerId: options.ownerId || '',
        surfaces: options.surfaces,
        visualContext: visualContext
    });
    try {
        callback(visualContext);
    } finally {
        frame.activePhase = previousPhase;
        if (previousPhase === 'idle') {
            if (hasLiveRenderFrameWork('model') || hasLiveRenderFrameWork('dom')) {
                scheduleRenderFrame();
            } else if (hasLiveRenderFrameWork('deferred')) {
                scheduleRenderDeferredFrame();
            }
        }
    }
    return true;
}

function resetRenderFrameRuntime() {
    var frame = frameRuntime();
    if (frame.frameId !== null) renderFrameCancel(frame.frameId);
    if (frame.deferredFrameId !== null) renderFrameCancel(frame.deferredFrameId);
    frame.frameId = null;
    frame.deferredFrameId = null;
    frame.nextVisualContextId = 1;
    frame.activePhase = 'idle';
    frame.queues = {
        model: [],
        dom: [],
        deferred: []
    };
    frame.queuedKeys = {
        model: {},
        dom: {},
        deferred: {}
    };
    frame.discardedJobs = 0;
    frame.discardedByLabel = {};
    frame.lastFrameLog = [];
}

function validRuleRef(si, gi, ri) {
    return RuleRefs.isValid(currentTraceStore().groups, currentUiState(), si, gi, ri);
}

function normalizeTraceData(trace, index) {
    return TraceSchema.normalizeTraceData(trace, index);
}

function normalizeTraceSelection(selection, traces) {
    traces = Array.isArray(traces) ? traces : traceDataTraces();
    selection = Array.isArray(selection) ? selection : [selection];
    var result = [];
    var seen = {};
    for (var i = 0; i < selection.length; i++) {
        var index = Math.floor(Number(selection[i]));
        if (!Number.isFinite(index) || index < 0 || index >= traces.length || seen[index]) continue;
        seen[index] = true;
        result.push(index);
    }
    if (!result.length && traces.length) {
        var current = traceRuntime().activeTraceSelection;
        var fallback = Array.isArray(current) ? Math.floor(Number(current[0])) : 0;
        fallback = Math.max(0, Math.min(traces.length - 1, fallback));
        result.push(fallback);
    }
    return result;
}

function activeTraceSelection() {
    return normalizeTraceSelection(traceRuntime().activeTraceSelection);
}

function traceSelectionKey(selection) {
    selection = normalizeTraceSelection(selection);
    return selection.length === 1 ? String(selection[0]) : 'combo:' + selection.join(',');
}

function activeTraceSelectionKey() {
    return traceSelectionKey(activeTraceSelection());
}

function traceTitleForIndex(index, traces) {
    traces = Array.isArray(traces) ? traces : traceDataTraces();
    var trace = traces[index] ? normalizeTraceData(traces[index], index) : null;
    return trace && trace.title || ('Trace ' + (index + 1));
}

function traceSelectionTitle(selection, traces) {
    traces = Array.isArray(traces) ? traces : traceDataTraces();
    selection = normalizeTraceSelection(selection, traces);
    if (selection.length <= 1) return traceTitleForIndex(selection[0] || 0, traces);
    var first = traceTitleForIndex(selection[0], traces);
    var second = traceTitleForIndex(selection[1], traces);
    if (selection.length === 2) return '2 traces: ' + first + ' + ' + second;
    return selection.length + ' traces: ' + first + ' + ' + (selection.length - 1) + ' more';
}

function copyTraceStageArray(values, si) {
    return Array.isArray(values) && Array.isArray(values[si]) ? values[si].slice() : [];
}

function copyTraceGroups(values, si) {
    var groups = Array.isArray(values) && Array.isArray(values[si]) ? values[si] : [];
    return groups.map(function(group) {
        group = group || {};
        return {
            name: String(group.name || ''),
            ri: Array.isArray(group.ri) ? group.ri.slice() : []
        };
    });
}

function mergeTraceFeatureAvailability(target, source) {
    source = source || {};
    return {
        fields: !!(target.fields || source.fields),
        pinned: !!(target.pinned || source.pinned),
        info: !!(target.info || source.info)
    };
}

function pushUniqueByKey(items, item, keyName) {
    if (!item) return;
    keyName = keyName || 'key';
    var key = String(item[keyName] || '');
    if (!key) return;
    for (var i = 0; i < items.length; i++) {
        if (String(items[i] && items[i][keyName] || '') === key) return;
    }
    var copy = {};
    for (var prop in item) {
        if (Object.prototype.hasOwnProperty.call(item, prop)) copy[prop] = item[prop];
    }
    items.push(copy);
}

function pushUniqueString(items, value) {
    value = String(value || '');
    if (value && items.indexOf(value) < 0) items.push(value);
}

function clonePinnedFieldPreset(preset, traceTitle) {
    if (!preset || typeof preset !== 'object') return null;
    var copy = {};
    for (var key in preset) {
        if (!Object.prototype.hasOwnProperty.call(preset, key)) continue;
        copy[key] = Array.isArray(preset[key]) ? preset[key].slice() : preset[key];
    }
    if (traceTitle && copy.label) copy.label = traceTitle + ' / ' + copy.label;
    else if (traceTitle && copy.name) copy.name = traceTitle + ' / ' + copy.name;
    return copy;
}

function copyRegistryRecordFields(record) {
    var copy = {};
    if (!record || typeof record !== 'object') return copy;
    for (var key in record) {
        if (Object.prototype.hasOwnProperty.call(record, key)) copy[key] = record[key];
    }
    return copy;
}

function mergePayloadRegistriesForSelection(normalizedTraces, selection) {
    var registry = null;
    var allSame = true;
    for (var i = 0; i < normalizedTraces.length; i++) {
        var current = normalizedTraces[i] && normalizedTraces[i].payloadRegistry || null;
        if (!current) continue;
        if (!registry) registry = current;
        else if (registry !== current) allSame = false;
    }
    if (allSame) return { registry: registry, remapTileId: function(_, tileId) { return tileId; } };

    var merged = TracePayloadRegistry.create();
    for (var ti = 0; ti < normalizedTraces.length; ti++) {
        var source = normalizedTraces[ti] && normalizedTraces[ti].payloadRegistry || null;
        if (!source) continue;
        var prefix = 'trace-' + selection[ti] + ':';
        var blocks = source.blocks || {};
        for (var blockId in blocks) {
            if (!Object.prototype.hasOwnProperty.call(blocks, blockId)) continue;
            var block = copyRegistryRecordFields(blocks[blockId]);
            block.id = prefix + blockId;
            TracePayloadRegistry.registerBlock(merged, block);
        }
        var tiles = source.tiles || {};
        for (var tileId in tiles) {
            if (!Object.prototype.hasOwnProperty.call(tiles, tileId)) continue;
            var tile = copyRegistryRecordFields(tiles[tileId]);
            tile.id = prefix + (tile.id || tile.tileId || tileId);
            tile.blockId = prefix + (tile.blockId || tile.payloadBlockId || '');
            TracePayloadRegistry.registerTile(merged, tile);
        }
    }
    return {
        registry: merged,
        remapTileId: function(sourceOrdinal, tileId) {
            return tileId ? 'trace-' + selection[sourceOrdinal] + ':' + tileId : tileId;
        },
        remapBlockId: function(sourceOrdinal, blockId) {
            return blockId ? 'trace-' + selection[sourceOrdinal] + ':' + blockId : blockId;
        }
    };
}

function copyPayloadRefForComposite(ref, sourceOrdinal, registryMerge) {
    if (!ref || typeof ref !== 'object') return ref || null;
    var copy = {};
    for (var key in ref) {
        if (Object.prototype.hasOwnProperty.call(ref, key)) copy[key] = ref[key];
    }
    if (registryMerge && registryMerge.remapTileId) {
        if (copy.tileId) copy.tileId = registryMerge.remapTileId(sourceOrdinal, copy.tileId);
        if (copy.blockId && registryMerge.remapBlockId) {
            copy.blockId = registryMerge.remapBlockId(sourceOrdinal, copy.blockId);
        }
    }
    return copy;
}

function normalizedTraceStageCount(trace) {
    trace = trace || {};
    return Math.max(
        (trace.stageNames || []).length,
        (trace.groups || []).length,
        (trace.ruleNames || []).length,
        (trace.planTrees || []).length,
        (trace.ruleTypes || []).length,
        (trace.ruleText || []).length,
        (trace.ruleInfo || []).length,
        (trace.sourceBlockIds || []).length,
        (trace.tilePayloadRefs || []).length
    );
}

function composeSelectedTraceData(selection) {
    var rawTraces = traceDataTraces();
    selection = normalizeTraceSelection(selection, rawTraces);
    if (selection.length <= 1) return normalizeTraceData(rawTraces[selection[0] || 0], selection[0] || 0);

    var normalized = selection.map(function(index) {
        return normalizeTraceData(rawTraces[index], index);
    });
    var registryMerge = mergePayloadRegistriesForSelection(normalized, selection);
    var composite = {
        schemaVersion: 2,
        traceIndex: selection[0],
        title: traceSelectionTitle(selection, rawTraces),
        stageNames: [],
        groups: [],
        ruleNames: [],
        ruleTypes: [],
        ruleText: [],
        planTrees: [],
        ruleInfo: [],
        sourceBlockIds: [],
        tilePayloadRefs: [],
        tileFeatures: [],
        nodeFields: [],
        pinnedFields: [],
        declaredPinnedFields: [],
        pinnedFieldPresets: [],
        diffFieldPresets: [],
        traceFeatureAvailability: { fields: false, pinned: false, info: false },
        payloadRegistry: registryMerge.registry
    };

    for (var ti = 0; ti < normalized.length; ti++) {
        var trace = normalized[ti];
        var traceTitle = trace.title || traceTitleForIndex(selection[ti], rawTraces);
        composite.traceFeatureAvailability = mergeTraceFeatureAvailability(
            composite.traceFeatureAvailability,
            trace.traceFeatureAvailability
        );
        (trace.nodeFields || []).forEach(function(field) { pushUniqueByKey(composite.nodeFields, field, 'key'); });
        (trace.pinnedFields || []).forEach(function(key) { pushUniqueString(composite.pinnedFields, key); });
        (trace.declaredPinnedFields || trace.pinnedFields || []).forEach(function(key) {
            pushUniqueString(composite.declaredPinnedFields, key);
        });
        (trace.pinnedFieldPresets || []).forEach(function(preset) {
            var cloned = clonePinnedFieldPreset(preset, traceTitle);
            if (cloned) composite.pinnedFieldPresets.push(cloned);
        });
        (trace.diffFieldPresets || []).forEach(function(preset) {
            var cloned = clonePinnedFieldPreset(preset, traceTitle);
            if (cloned) composite.diffFieldPresets.push(cloned);
        });

        for (var si = 0; si < normalizedTraceStageCount(trace); si++) {
            var stageName = trace.stageNames[si] || ('Stage ' + (si + 1));
            composite.stageNames.push(traceTitle + ' / ' + stageName);
            composite.groups.push(copyTraceGroups(trace.groups, si));
            composite.ruleNames.push(copyTraceStageArray(trace.ruleNames, si));
            composite.ruleTypes.push(copyTraceStageArray(trace.ruleTypes, si));
            composite.ruleText.push(copyTraceStageArray(trace.ruleText, si));
            composite.planTrees.push(copyTraceStageArray(trace.planTrees, si));
            composite.ruleInfo.push(copyTraceStageArray(trace.ruleInfo, si));
            composite.sourceBlockIds.push(copyTraceStageArray(trace.sourceBlockIds, si));
            composite.tileFeatures.push(copyTraceStageArray(trace.tileFeatures, si));
            composite.tilePayloadRefs.push(copyTraceStageArray(trace.tilePayloadRefs, si).map(function(ref) {
                return copyPayloadRefForComposite(ref, ti, registryMerge);
            }));
        }
    }
    return composite;
}

function emptyUiState() {
    return TraceState.emptyUiState();
}

function buildAllRulesForActiveTrace() {
    traceRuntime().allRules = TraceStore.allRules(currentTraceStore());
}

var LARGE_TRACE_CONTENT_INITIAL_STAGE_THRESHOLD = 120;
var LARGE_TRACE_CONTENT_INITIAL_RULE_THRESHOLD = 500;

function traceStoreRuleCount(store) {
    var count = 0;
    var groups = store && store.groups || [];
    for (var si = 0; si < groups.length; si++) {
        var stageGroups = groups[si] || [];
        for (var gi = 0; gi < stageGroups.length; gi++) {
            count += stageGroups[gi] && stageGroups[gi].ri ? stageGroups[gi].ri.length : 0;
        }
    }
    return count;
}

function largeTracePrefersContentInitialState(store) {
    if (!store) return false;
    return TraceStore.stageCount(store) >= LARGE_TRACE_CONTENT_INITIAL_STAGE_THRESHOLD ||
        traceStoreRuleCount(store) >= LARGE_TRACE_CONTENT_INITIAL_RULE_THRESHOLD;
}

function createInitialUiState() {
    var store = currentTraceStore();
    var state = TraceState.createInitialUiState(TraceStore.stageCount(store), store.groups);
    if (largeTracePrefersContentInitialState(store)) {
        TraceState.applyGlobalCycleState(state, store.groups, 3);
    }
    return state;
}

function loadTraceData(selection) {
    bumpRuntimeEpoch('trace');
    bumpRuntimeEpoch('render');
    bumpRuntimeEpoch('search');
    bumpRuntimeEpoch('diff');
    bumpRuntimeEpoch('virtual');
    bumpRuntimeEpoch('layout');
    bumpRuntimeEpoch('fullscreen');
    bumpRuntimeEpoch('resize');
    var traces = traceDataTraces();
    selection = normalizeTraceSelection(selection, traces);
    var traceState = traceRuntime();
    traceState.activeTraceSelection = selection.slice();

    var trace = composeSelectedTraceData(selection);
    traceState.traceStore = TraceStore.create(trace, {
        traceGeneration: currentRuntimeEpoch().trace
    });
    traceState.traceStoreStatus = traceDataLoadStatus();
    traceState.stageCount = TraceStore.stageCount(traceState.traceStore);

    buildAllRulesForActiveTrace();
    traceState.uiState = createInitialUiState();
    traceState.ruleFeatureCache = {};
    traceState.treeMaterializers = {};
    traceState.activeTraceLoaded = true;
}

var SESSION_TRACE_SELECTION_KEY = 'otv_activeTraceSelection';

function saveActiveTraceSelectionToSession(selection) {
    try {
        sessionStorage.setItem(
            SESSION_TRACE_SELECTION_KEY,
            normalizeTraceSelection(selection).join(',')
        );
    } catch (e) {}
}

function ensureActiveTraceLoaded() {
    if (!traceRuntime().activeTraceLoaded) {
        try {
            var saved = sessionStorage.getItem(SESSION_TRACE_SELECTION_KEY);
            if (saved !== null) {
                var traces = traceDataTraces();
                var selection = normalizeTraceSelection(String(saved).split(','), traces);
                traceRuntime().activeTraceSelection = selection;
            }
        } catch (e) {}
        loadTraceData(activeTraceSelection());
    }
}

function findFlatIndex(si, gi, ri) {
    var rules = currentAllRules();
    for (var i = 0; i < rules.length; i++) {
        if (rules[i].stageIdx === si &&
            rules[i].groupIdx === gi &&
            rules[i].ruleIdx === ri) {
            return i;
        }
    }
    return -1;
}

function clampRuleWidth(width) {
    width = Math.round(Number(width));
    if (!Number.isFinite(width)) return RULE_WIDTH_DEFAULT;
    return Math.max(RULE_WIDTH_MIN, Math.min(ruleWidthMax(), width));
}

function clampRuleWidthForLayout(width) {
    width = Math.round(Number(width));
    if (!Number.isFinite(width)) return RULE_WIDTH_DEFAULT;
    return Math.max(RULE_WIDTH_MIN, Math.min(RULE_WIDTH_MAX_ABSOLUTE, width));
}

function ruleWidthMax() {
    if (typeof document === 'undefined') return RULE_WIDTH_MAX_FALLBACK;

    var viewport = 0;
    var traceEl = document.querySelector ? document.querySelector('.trace') : null;
    if (traceEl) viewport = traceEl.clientWidth || 0;
    if (!viewport && typeof window !== 'undefined') viewport = window.innerWidth || 0;
    if (!Number.isFinite(viewport) || viewport <= 0) return RULE_WIDTH_MAX_FALLBACK;

    return Math.max(
        RULE_WIDTH_MIN,
        Math.min(
            RULE_WIDTH_MAX_ABSOLUTE,
            Math.round(viewport * RULE_WIDTH_MAX_VIEWPORT_RATIO)
        )
    );
}

function stageState(si) { return currentUiState().stages[si]; }
function groupState(si, gi) { return currentUiState().stages[si].groups[gi]; }
function ruleState(si, gi, ri) { return currentUiState().stages[si].groups[gi].rules[ri]; }
function findRuleState(si, gi, ri) {
    var state = currentUiState();
    return state.stages[si] &&
           state.stages[si].groups[gi] &&
           state.stages[si].groups[gi].rules[ri];
}

function activeSearchExpandOverlay() {
    var search = searchRuntime();
    var overlay = search.searchExpandOverlay;
    if (!overlay || !search.searchLayoutApplied) return null;
    if (overlay.mode !== 'expand') return null;
    if (overlay.query !== search.searchResultQuery || overlay.scope !== search.searchResultScope) return null;

    var store = currentTraceStore();
    if (store && Number(overlay.traceGeneration) !== Number(store.traceGeneration)) return null;
    return overlay;
}

function searchExpandOverlayGroupKey(si, gi) {
    return si + '-' + gi;
}

function searchExpandOverlayRuleKey(si, gi, ri) {
    return ruleKey(si, gi, ri);
}

function searchExpandOverlayGroupSummary(overlay, si, gi) {
    if (!overlay || !overlay.groupSummaries) return null;
    return overlay.groupSummaries[searchExpandOverlayGroupKey(si, gi)] || null;
}

function activeSearchExpandOverlayGroupSummary(si, gi) {
    return searchExpandOverlayGroupSummary(activeSearchExpandOverlay(), si, gi);
}

function effectiveStageOpen(si) {
    var overlay = activeSearchExpandOverlay();
    if (!overlay) return !!stageState(si).open;
    return !!(overlay.stages && overlay.stages[String(si)]);
}

function effectiveGroupOpen(si, gi) {
    var overlay = activeSearchExpandOverlay();
    if (!overlay) return !!groupState(si, gi).open;
    if (groupRuleCount(si, gi) <= 1) return true;
    return !!(overlay.groups && overlay.groups[searchExpandOverlayGroupKey(si, gi)]);
}

function effectiveRuleOpen(si, gi, ri) {
    var overlay = activeSearchExpandOverlay();
    if (!overlay) return !!ruleState(si, gi, ri).open;
    var rule = overlay.rules && overlay.rules[searchExpandOverlayRuleKey(si, gi, ri)];
    return !!(rule && rule.open);
}

function effectiveRuleFeature(si, gi, ri, feature) {
    var rule = ruleState(si, gi, ri);
    if (!feature || !rule) return false;
    if (feature === 'pinned' && pinnedFieldCount() === 0) return false;
    return !!rule[feature];
}

function effectiveRuleState(si, gi, ri) {
    var state = ruleState(si, gi, ri);
    var overlay = activeSearchExpandOverlay();
    if (!overlay) return state;
    return {
        open: effectiveRuleOpen(si, gi, ri),
        fields: effectiveRuleFeature(si, gi, ri, 'fields'),
        pinned: effectiveRuleFeature(si, gi, ri, 'pinned'),
        info: effectiveRuleFeature(si, gi, ri, 'info'),
        rendered: state.rendered,
        renderQueued: state.renderQueued,
        infoRendered: state.infoRendered
    };
}
function currentTraceStore() {
    return traceRuntime().traceStore;
}
function currentStageCount() { return TraceStore.stageCount(currentTraceStore()); }
function currentTraceGroups() { return currentTraceStore().groups; }
function stageHasRules(si) { return TraceStore.hasStageRules(currentTraceStore(), si); }
function traceHasEmptyStages() {
    for (var si = 0; si < currentStageCount(); si++) {
        if (!stageHasRules(si)) return true;
    }
    return false;
}
function showEmptyStages() { return !!traceRuntime().showEmptyStages; }
function stageVisible(si) { return showEmptyStages() || stageHasRules(si); }
function groupCount(si) { return TraceStore.groupCount(currentTraceStore(), si); }
function groupRuleCount(si, gi) { return TraceStore.groupRuleCount(currentTraceStore(), si, gi); }
function rawRuleIndex(si, gi, ri) { return TraceStore.rawRuleIndex(currentTraceStore(), si, gi, ri); }
function currentRuleSessionKey(ref) {
    if (!ref) return '';
    var selectionKey = '';
    try {
        selectionKey = activeTraceSelectionKey();
    } catch (err) {}

    var handle = '';
    try {
        handle = TraceStore.ruleHandleForRef(currentTraceStore(), ref) || '';
    } catch (err2) {}
    if (!handle) {
        try {
            handle = ruleKey(ref.si, ref.gi, ref.ri);
        } catch (err3) {}
    }
    return handle ? ((selectionKey || 'trace') + ':' + handle) : '';
}
function pinnedFieldSelectionKey() {
    var store = currentTraceStore();
    return 'trace:' + (store && store.traceIndex !== undefined ? store.traceIndex : traceRuntime().activeTraceIndex || 0);
}

function traceNodeFields() {
    return TraceStore.nodeFields(currentTraceStore());
}

function traceDefaultPinnedFieldKeys() {
    return TraceStore.pinnedFields(currentTraceStore()).slice();
}

function tracePinnedFieldPresets() {
    return TraceStore.pinnedFieldPresets(currentTraceStore());
}

function traceDiffFieldPresets() {
    return TraceStore.diffFieldPresets(currentTraceStore());
}

function traceNodeFieldAvailableMap() {
    var fields = traceNodeFields();
    var available = {};
    for (var i = 0; i < fields.length; i++) available[fields[i].key] = true;
    return available;
}

var PINNED_FIELD_STORAGE_KEY = 'optimizerTracePinnedFields';
var PINNED_FIELD_STORAGE_MAX_ENTRIES = 32;
var PINNED_FIELD_WIDTH_STORAGE_KEY = 'optimizerTracePinnedFieldWidths';
var DIFF_FIELD_STORAGE_KEY = 'optimizerTraceDiffFields';
var DIFF_FIELD_STORAGE_MAX_ENTRIES = 32;
var PINNED_FIELD_WIDTH_DEFAULT = 72;
var PINNED_FIELD_WIDTH_MIN = 44;
var PINNED_FIELD_WIDTH_MAX = 260;
var PINNED_FIELD_WIDTH_AUTO_MAX = 180;
var PINNED_FIELD_WIDTH_TEXT_PX = 7;
var PINNED_FIELD_WIDTH_VALUE_PADDING = 18;
var PINNED_FIELD_WIDTH_HEADER_PADDING = 34;

function pinnedFieldStorage() {
    try {
        return typeof window !== 'undefined' && window.localStorage ? window.localStorage : null;
    } catch (e) {
        return null;
    }
}

/**
 * Pinned field selections persist per field schema, not per trace index, so the
 * same kind of trace keeps its curated fields across reloads and files.
 */
function pinnedFieldSchemaSignature() {
    var fields = traceNodeFields();
    if (!fields.length) return '';
    var keys = [];
    for (var i = 0; i < fields.length; i++) keys.push(fields[i].key);
    return keys.join('\n');
}

function readStoredPinnedFieldSelections() {
    var storage = pinnedFieldStorage();
    if (!storage) return {};
    try {
        var parsed = JSON.parse(storage.getItem(PINNED_FIELD_STORAGE_KEY) || 'null');
        return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch (e) {
        return {};
    }
}

function persistPinnedFieldSelection(keys) {
    var storage = pinnedFieldStorage();
    var signature = pinnedFieldSchemaSignature();
    if (!storage || !signature) return;
    try {
        var stored = readStoredPinnedFieldSelections();
        delete stored[signature];
        if (Array.isArray(keys)) stored[signature] = keys.slice();
        var signatures = Object.keys(stored);
        for (var i = 0; signatures.length - i > PINNED_FIELD_STORAGE_MAX_ENTRIES; i++) {
            delete stored[signatures[i]];
        }
        storage.setItem(PINNED_FIELD_STORAGE_KEY, JSON.stringify(stored));
    } catch (e) {}
}

function readStoredDiffFieldSelections() {
    var storage = pinnedFieldStorage();
    if (!storage) return {};
    try {
        var parsed = JSON.parse(storage.getItem(DIFF_FIELD_STORAGE_KEY) || 'null');
        return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch (e) {
        return {};
    }
}

function persistDiffFieldSelection(keys) {
    var storage = pinnedFieldStorage();
    var signature = pinnedFieldSchemaSignature();
    if (!storage || !signature) return;
    try {
        var stored = readStoredDiffFieldSelections();
        delete stored[signature];
        if (Array.isArray(keys)) stored[signature] = keys.slice();
        var signatures = Object.keys(stored);
        for (var i = 0; signatures.length - i > DIFF_FIELD_STORAGE_MAX_ENTRIES; i++) {
            delete stored[signatures[i]];
        }
        storage.setItem(DIFF_FIELD_STORAGE_KEY, JSON.stringify(stored));
    } catch (e) {}
}

function clampPinnedFieldWidth(width) {
    width = Number(width);
    if (!Number.isFinite(width)) return PINNED_FIELD_WIDTH_DEFAULT;
    return Math.max(PINNED_FIELD_WIDTH_MIN, Math.min(PINNED_FIELD_WIDTH_MAX, Math.round(width)));
}

function readStoredPinnedFieldWidths() {
    var storage = pinnedFieldStorage();
    if (!storage) return {};
    try {
        var parsed = JSON.parse(storage.getItem(PINNED_FIELD_WIDTH_STORAGE_KEY) || 'null');
        return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch (e) {
        return {};
    }
}

function persistPinnedFieldWidths(widths) {
    var storage = pinnedFieldStorage();
    var signature = pinnedFieldSchemaSignature();
    if (!storage || !signature) return;
    try {
        var stored = readStoredPinnedFieldWidths();
        delete stored[signature];
        var next = {};
        var available = traceNodeFieldAvailableMap();
        var hasWidths = false;
        widths = widths && typeof widths === 'object' ? widths : {};
        for (var key in widths) {
            if (!Object.prototype.hasOwnProperty.call(widths, key) || !available[key]) continue;
            var width = clampPinnedFieldWidth(widths[key]);
            next[key] = width;
            hasWidths = true;
        }
        if (hasWidths) stored[signature] = next;
        var signatures = Object.keys(stored);
        for (var i = 0; signatures.length - i > PINNED_FIELD_STORAGE_MAX_ENTRIES; i++) {
            delete stored[signatures[i]];
        }
        storage.setItem(PINNED_FIELD_WIDTH_STORAGE_KEY, JSON.stringify(stored));
    } catch (e) {}
}

function hydratePinnedFieldSelection() {
    var signature = pinnedFieldSchemaSignature();
    if (!signature) return;
    var trace = traceRuntime();
    var selectionKey = pinnedFieldSelectionKey();
    if (!trace.pinnedFieldSelectionsHydrated) trace.pinnedFieldSelectionsHydrated = {};
    if (trace.pinnedFieldSelectionsHydrated[selectionKey]) return;
    trace.pinnedFieldSelectionsHydrated[selectionKey] = true;
    if (!trace.pinnedFieldSelections ||
            Object.prototype.hasOwnProperty.call(trace.pinnedFieldSelections, selectionKey)) {
        return;
    }
    var stored = readStoredPinnedFieldSelections()[signature];
    if (Array.isArray(stored)) trace.pinnedFieldSelections[selectionKey] = stored.slice();
}

function hydrateDiffFieldSelection() {
    var signature = pinnedFieldSchemaSignature();
    if (!signature) return;
    var trace = traceRuntime();
    var selectionKey = pinnedFieldSelectionKey();
    if (!trace.diffFieldSelectionsHydrated) trace.diffFieldSelectionsHydrated = {};
    if (trace.diffFieldSelectionsHydrated[selectionKey]) return;
    trace.diffFieldSelectionsHydrated[selectionKey] = true;
    if (!trace.diffFieldSelections) trace.diffFieldSelections = {};
    if (Object.prototype.hasOwnProperty.call(trace.diffFieldSelections, selectionKey)) return;
    var stored = readStoredDiffFieldSelections()[signature];
    if (Array.isArray(stored)) trace.diffFieldSelections[selectionKey] = stored.slice();
}

function hydratePinnedFieldWidths() {
    var signature = pinnedFieldSchemaSignature();
    if (!signature) return;
    var trace = traceRuntime();
    var selectionKey = pinnedFieldSelectionKey();
    if (!trace.pinnedFieldWidthsHydrated) trace.pinnedFieldWidthsHydrated = {};
    if (trace.pinnedFieldWidthsHydrated[selectionKey]) return;
    trace.pinnedFieldWidthsHydrated[selectionKey] = true;
    if (!trace.pinnedFieldWidths) trace.pinnedFieldWidths = {};
    if (Object.prototype.hasOwnProperty.call(trace.pinnedFieldWidths, selectionKey)) return;
    var stored = readStoredPinnedFieldWidths()[signature];
    if (stored && typeof stored === 'object' && !Array.isArray(stored)) {
        trace.pinnedFieldWidths[selectionKey] = stored;
    }
}

function activePinnedFieldKeys() {
    hydratePinnedFieldSelection();
    var trace = traceRuntime();
    var key = pinnedFieldSelectionKey();
    var selected = trace.pinnedFieldSelections && trace.pinnedFieldSelections[key];
    var defaults = traceDefaultPinnedFieldKeys();
    var source = Array.isArray(selected) ? selected : defaults;
    var available = traceNodeFieldAvailableMap();
    var result = [];
    var seen = {};
    for (var i = 0; i < source.length; i++) {
        var fieldKey = String(source[i] || '');
        if (!fieldKey || !available[fieldKey] || seen[fieldKey]) continue;
        seen[fieldKey] = true;
        result.push(fieldKey);
    }
    return result;
}

function activePinnedFieldKeySet() {
    var keys = activePinnedFieldKeys();
    var set = {};
    for (var i = 0; i < keys.length; i++) set[keys[i]] = true;
    return set;
}

function activeDiffFieldKeys() {
    hydrateDiffFieldSelection();
    var trace = traceRuntime();
    var key = pinnedFieldSelectionKey();
    var selected = trace.diffFieldSelections && trace.diffFieldSelections[key];
    var source = Array.isArray(selected) ? selected : [];
    var available = traceNodeFieldAvailableMap();
    var result = [];
    var seen = {};
    for (var i = 0; i < source.length; i++) {
        var fieldKey = String(source[i] || '');
        if (!fieldKey || !available[fieldKey] || seen[fieldKey]) continue;
        seen[fieldKey] = true;
        result.push(fieldKey);
    }
    return result;
}

function pinnedFieldWidthMap() {
    hydratePinnedFieldWidths();
    var trace = traceRuntime();
    var selectionKey = pinnedFieldSelectionKey();
    if (!trace.pinnedFieldWidths) trace.pinnedFieldWidths = {};
    if (!trace.pinnedFieldWidths[selectionKey]) trace.pinnedFieldWidths[selectionKey] = {};
    return trace.pinnedFieldWidths[selectionKey];
}

function pinnedFieldAutoWidthCache() {
    var trace = traceRuntime();
    if (!trace.pinnedFieldAutoWidths) trace.pinnedFieldAutoWidths = {};
    return trace.pinnedFieldAutoWidths;
}

function pinnedFieldAutoWidthCacheKey(key) {
    var epoch = currentRuntimeEpoch();
    var materialization = currentTraceStore().materialization || {};
    return pinnedFieldSelectionKey() + '\n' +
        pinnedFieldSchemaSignature() + '\n' +
        String(epoch && epoch.trace || 0) + '\n' +
        String(Math.max(0, Math.floor(Number(materialization.generation)) || 0)) + '\n' +
        String(key || '');
}

function estimatePinnedFieldTextWidth(text, paddingPx) {
    return Math.ceil(String(text == null ? '' : text).length * PINNED_FIELD_WIDTH_TEXT_PX + paddingPx);
}

function includePinnedFieldMeasuredText(state, text, paddingPx) {
    text = String(text == null ? '' : text);
    if (!text) return false;
    state.width = Math.max(state.width, estimatePinnedFieldTextWidth(text, paddingPx));
    if (state.width >= PINNED_FIELD_WIDTH_AUTO_MAX) {
        state.width = PINNED_FIELD_WIDTH_AUTO_MAX;
        return true;
    }
    return false;
}

function measurePinnedFieldTree(node, store, key, state) {
    if (!node) return true;
    if (includePinnedFieldMeasuredText(
            state,
            TraceStore.pinnedFieldValue(store, node, key),
            PINNED_FIELD_WIDTH_VALUE_PADDING)) {
        return false;
    }
    var children = Array.isArray(node.c) ? node.c : [];
    for (var i = 0; i < children.length; i++) {
        if (measurePinnedFieldTree(children[i], store, key, state) === false) return false;
    }
    return true;
}

function autoPinnedFieldWidthForKey(key) {
    key = String(key || '');
    if (!key) return PINNED_FIELD_WIDTH_DEFAULT;
    var cache = pinnedFieldAutoWidthCache();
    var cacheKey = pinnedFieldAutoWidthCacheKey(key);
    if (Object.prototype.hasOwnProperty.call(cache, cacheKey)) return cache[cacheKey];

    var store = currentTraceStore();
    var state = {
        width: Math.max(
            PINNED_FIELD_WIDTH_DEFAULT,
            estimatePinnedFieldTextWidth(
                TraceStore.nodeFieldLabel(store, key),
                PINNED_FIELD_WIDTH_HEADER_PADDING))
    };
    if (state.width >= PINNED_FIELD_WIDTH_AUTO_MAX) {
        state.width = PINNED_FIELD_WIDTH_AUTO_MAX;
    } else {
        var done = false;
        for (var si = 0; si < currentStageCount() && !done; si++) {
            for (var gi = 0; gi < groupCount(si) && !done; gi++) {
                for (var ri = 0; ri < groupRuleCount(si, gi) && !done; ri++) {
                    var rawIdx = rawRuleIndex(si, gi, ri);
                    var handle = TraceStore.ruleHandle(store, si, rawIdx);
                    var payloadState = TraceStore.payloadState(store, 'trees', handle).state;
                    if (payloadState !== TraceStore.PAYLOAD_STATES.RENDERED &&
                            payloadState !== TraceStore.PAYLOAD_STATES.EMPTY) {
                        continue;
                    }
                    var tree = TraceStore.materializeRuleTree(store, handle);
                    done = measurePinnedFieldTree(tree, store, key, state) === false;
                }
            }
        }
    }
    cache[cacheKey] = clampPinnedFieldWidth(Math.min(PINNED_FIELD_WIDTH_AUTO_MAX, state.width));
    return cache[cacheKey];
}

function pinnedFieldManualWidthForKey(key) {
    key = String(key || '');
    if (!key) return null;
    var map = pinnedFieldWidthMap();
    return Object.prototype.hasOwnProperty.call(map, key)
        ? clampPinnedFieldWidth(map[key])
        : null;
}

function pinnedFieldWidthForKey(key) {
    key = String(key || '');
    if (!key) return PINNED_FIELD_WIDTH_DEFAULT;
    var manualWidth = pinnedFieldManualWidthForKey(key);
    return manualWidth === null ? autoPinnedFieldWidthForKey(key) : manualWidth;
}

function activePinnedFieldWidthTotal() {
    var keys = activePinnedFieldKeys();
    var total = 0;
    for (var i = 0; i < keys.length; i++) total += pinnedFieldWidthForKey(keys[i]);
    return total;
}

function setPinnedFieldWidth(key, width) {
    key = String(key || '');
    var available = traceNodeFieldAvailableMap();
    if (!key || !available[key]) return false;
    var nextWidth = clampPinnedFieldWidth(width);
    var map = pinnedFieldWidthMap();
    var current = pinnedFieldWidthForKey(key);
    if (current === nextWidth) return false;
    if (nextWidth === autoPinnedFieldWidthForKey(key)) {
        delete map[key];
    } else {
        map[key] = nextWidth;
    }
    persistPinnedFieldWidths(map);
    bumpRuntimeEpoch('render');
    return true;
}

function setPinnedFieldSelection(keys) {
    var available = traceNodeFieldAvailableMap();
    var next = [];
    var seen = {};
    var source = Array.isArray(keys) ? keys : [];
    for (var i = 0; i < source.length; i++) {
        var fieldKey = String(source[i] || '');
        if (!fieldKey || !available[fieldKey] || seen[fieldKey]) continue;
        seen[fieldKey] = true;
        next.push(fieldKey);
    }
    var current = activePinnedFieldKeys();
    if (next.join('\n') === current.join('\n')) return false;

    traceRuntime().pinnedFieldSelections[pinnedFieldSelectionKey()] = next;
    persistPinnedFieldSelection(next);
    bumpRuntimeEpoch('render');
    return true;
}

function setDiffFieldSelection(keys) {
    var available = traceNodeFieldAvailableMap();
    var next = [];
    var seen = {};
    var source = Array.isArray(keys) ? keys : [];
    for (var i = 0; i < source.length; i++) {
        var fieldKey = String(source[i] || '');
        if (!fieldKey || !available[fieldKey] || seen[fieldKey]) continue;
        seen[fieldKey] = true;
        next.push(fieldKey);
    }
    var current = activeDiffFieldKeys();
    if (next.join('\n') === current.join('\n')) return false;

    traceRuntime().diffFieldSelections[pinnedFieldSelectionKey()] = next;
    persistDiffFieldSelection(next);
    bumpRuntimeEpoch('diff');
    return true;
}

function reorderPinnedField(key, dropIndex) {
    key = String(key || '');
    var keys = activePinnedFieldKeys().slice();
    var fromIndex = keys.indexOf(key);
    if (fromIndex === -1 || keys.length < 2) return false;

    dropIndex = Number(dropIndex);
    if (!Number.isFinite(dropIndex)) return false;
    dropIndex = Math.max(0, Math.min(keys.length, Math.round(dropIndex)));

    var insertIndex = dropIndex;
    if (fromIndex < insertIndex) insertIndex--;
    insertIndex = Math.max(0, Math.min(keys.length - 1, insertIndex));
    if (insertIndex === fromIndex) return false;

    keys.splice(fromIndex, 1);
    keys.splice(insertIndex, 0, key);
    return setPinnedFieldSelection(keys);
}

function setPinnedFieldVisible(key, visible) {
    key = String(key || '');
    var available = traceNodeFieldAvailableMap();
    if (!key || !available[key]) return false;

    var next = activePinnedFieldKeys().slice();
    var index = next.indexOf(key);
    if (visible && index === -1) next.push(key);
    if (!visible && index !== -1) next.splice(index, 1);
    return setPinnedFieldSelection(next);
}

function setDiffFieldVisible(key, visible) {
    key = String(key || '');
    var available = traceNodeFieldAvailableMap();
    if (!key || !available[key]) return false;

    var next = activeDiffFieldKeys().slice();
    var index = next.indexOf(key);
    if (visible && index === -1) next.push(key);
    if (!visible && index !== -1) next.splice(index, 1);
    return setDiffFieldSelection(next);
}

function setAllPinnedFieldsVisible(visible) {
    if (!visible) return setPinnedFieldSelection([]);
    var fields = traceNodeFields();
    var keys = [];
    for (var i = 0; i < fields.length; i++) keys.push(fields[i].key);
    return setPinnedFieldSelection(keys);
}

function setAllDiffFieldsVisible(visible) {
    if (!visible) return setDiffFieldSelection([]);
    var fields = traceNodeFields();
    var keys = [];
    for (var i = 0; i < fields.length; i++) keys.push(fields[i].key);
    return setDiffFieldSelection(keys);
}

function soloPinnedField(key) {
    return setPinnedFieldSelection([key]);
}

function soloDiffField(key) {
    return setDiffFieldSelection([key]);
}

function setPinnedFieldPreset(index) {
    index = Number(index);
    if (!Number.isInteger(index) || index < 0) return false;
    var presets = tracePinnedFieldPresets();
    var preset = presets[index];
    if (!preset) return false;
    return setPinnedFieldSelection(preset && preset.keys || []);
}

function setDiffFieldPreset(index) {
    index = Number(index);
    if (!Number.isInteger(index) || index < 0) return false;
    var presets = traceDiffFieldPresets();
    var preset = presets[index];
    if (!preset) return false;
    return setDiffFieldSelection(preset && preset.keys || []);
}

function resetPinnedFieldsToDefault() {
    hydratePinnedFieldSelection();
    var trace = traceRuntime();
    var selectionKey = pinnedFieldSelectionKey();
    if (!trace.pinnedFieldSelections ||
            !Object.prototype.hasOwnProperty.call(trace.pinnedFieldSelections, selectionKey)) {
        return false;
    }
    delete trace.pinnedFieldSelections[selectionKey];
    persistPinnedFieldSelection(null);
    bumpRuntimeEpoch('render');
    return true;
}

function resetDiffFieldsToDefault() {
    hydrateDiffFieldSelection();
    var trace = traceRuntime();
    var selectionKey = pinnedFieldSelectionKey();
    if (!trace.diffFieldSelections ||
            !Object.prototype.hasOwnProperty.call(trace.diffFieldSelections, selectionKey)) {
        return false;
    }
    delete trace.diffFieldSelections[selectionKey];
    persistDiffFieldSelection(null);
    bumpRuntimeEpoch('diff');
    return true;
}

function pinnedFieldsAreDefault() {
    hydratePinnedFieldSelection();
    var trace = traceRuntime();
    var selectionKey = pinnedFieldSelectionKey();
    return !trace.pinnedFieldSelections ||
        !Object.prototype.hasOwnProperty.call(trace.pinnedFieldSelections, selectionKey);
}

function diffFieldsAreDefault() {
    hydrateDiffFieldSelection();
    var trace = traceRuntime();
    var selectionKey = pinnedFieldSelectionKey();
    return !trace.diffFieldSelections ||
        !Object.prototype.hasOwnProperty.call(trace.diffFieldSelections, selectionKey);
}

function pinnedFieldCount() { return activePinnedFieldKeys().length; }
/**
 * Identity of the current pinned field selection as rendered into rule trees.
 * Stored in rule render state so lazy visibility scans re-render trees
 * whose pinned fields (and pinned-field-suppressed metadata rows) are out of date.
 */
function pinnedFieldRenderSignature() {
    var keys = activePinnedFieldKeys();
    var parts = [];
    for (var i = 0; i < keys.length; i++) {
        parts.push(keys[i] + ':' + pinnedFieldWidthForKey(keys[i]));
    }
    return parts.join('\n');
}

function diffFieldSelectionSignature() {
    return activeDiffFieldKeys().join('\n');
}

function tracePinnedFieldKey(col) {
    var keys = activePinnedFieldKeys();
    return keys[col] || '';
}
function ruleKey(si, gi, ri) { return RuleRefs.key(si, gi, ri); }
function traceStageName(si) { return TraceStore.stageName(currentTraceStore(), si); }
function traceStageRuleCount(si) { return TraceStore.stageRuleCount(currentTraceStore(), si); }
function traceGroupName(si, gi) { return TraceStore.groupName(currentTraceStore(), si, gi); }
function traceRuleName(si, rawIdx) { return TraceStore.ruleName(currentTraceStore(), si, rawIdx); }
function ruleDisplayName(ref) {
    return TraceStore.ruleNameForRef(currentTraceStore(), ref);
}
function traceRuleType(si, rawIdx) { return TraceStore.ruleType(currentTraceStore(), si, rawIdx); }
function traceRuleText(si, rawIdx) {
    var store = currentTraceStore();
    return TraceStore.materializeTextTile(store, TraceStore.ruleHandle(store, si, rawIdx));
}
function tracePlanTree(si, rawIdx) {
    var store = currentTraceStore();
    return TraceStore.materializeRuleTree(store, TraceStore.ruleHandle(store, si, rawIdx));
}
function traceRuleInfo(si, rawIdx) {
    var store = currentTraceStore();
    return TraceStore.materializeRuleInfo(store, TraceStore.ruleHandle(store, si, rawIdx));
}
function tracePinnedFieldName(col) {
    return TraceStore.nodeFieldLabel(currentTraceStore(), tracePinnedFieldKey(col));
}

function tracePinnedFieldValue(node, col) {
    var keys = activePinnedFieldKeys();
    return TraceStore.pinnedFieldValue(currentTraceStore(), node, keys[col] || '');
}

function traceNodeFieldRows(node) {
    var store = currentTraceStore();
    var rows = TraceStore.nodeFieldRows(store, node);
    var excludeKeys = activePinnedFieldKeySet();
    var result = [];
    for (var i = 0; i < rows.length; i++) {
        var row = rows[i];
        if (!row || excludeKeys[TraceStore.fieldRowFieldKey(row)]) continue;
        if (Object.defineProperty) {
            Object.defineProperty(row, 'metaIndex', {
                value: i,
                enumerable: false,
                configurable: true
            });
        } else {
            row.metaIndex = i;
        }
        result.push(row);
    }
    return result;
}

function ruleRefValidForCurrentTrace(ref) {
    return !!ref && RuleRefs.isValid(currentTraceStore().groups, currentUiState(), ref.si, ref.gi, ref.ri);
}

function ruleIsTextTile(ref) {
    return ruleRefValidForCurrentTrace(ref) &&
        TraceStore.ruleTypeForRef(currentTraceStore(), ref) === 'text';
}

function ruleSupportsDiff(ref) {
    return ruleRefValidForCurrentTrace(ref) && !ruleIsTextTile(ref);
}

function forEachGroup(callback) {
    for (var si = 0; si < currentStageCount(); si++) {
        for (var gi = 0; gi < groupCount(si); gi++) {
            callback(si, gi, groupState(si, gi));
        }
    }
}

function forEachRule(callback) {
    for (var si = 0; si < currentStageCount(); si++) {
        for (var gi = 0; gi < groupCount(si); gi++) {
            for (var ri = 0; ri < groupRuleCount(si, gi); ri++) {
                callback(si, gi, ri, ruleState(si, gi, ri), rawRuleIndex(si, gi, ri));
            }
        }
    }
}

function setLayoutPanelsOpen(open) {
    var change = TraceState.setLayoutPanelsOpen(currentUiState(), currentTraceGroups(), open);
    invalidateTraceMeasuredWidthCache();
    return change;
}

function collapseLayout() {
    return setLayoutPanelsOpen(false);
}


function openRulePath(si, gi, ri) {
    var change = TraceState.openRulePath(currentUiState(), currentTraceGroups(), si, gi, ri);
    markTraceLayoutDirtyStagesFromTransition(change, 'rule-open-path-stage');
    markTraceLayoutDirtyGroupsFromTransition(change, 'rule-open-path-group');
    markTraceLayoutDirtyRulesFromTransition(change, 'rule-open-path');
    invalidateTraceMeasuredWidthCache();
    return change;
}

function openRulePathForSearchMatch(si, gi, ri, expandRule) {
    var change = TraceState.openRulePathForSearchMatch(currentUiState(), currentTraceGroups(), si, gi, ri, expandRule);
    markTraceLayoutDirtyStagesFromTransition(change, 'search-rule-open-path-stage');
    markTraceLayoutDirtyGroupsFromTransition(change, 'search-rule-open-path-group');
    markTraceLayoutDirtyRulesFromTransition(change, 'search-rule-open-path');
    invalidateTraceMeasuredWidthCache();
    return change;
}

function widthCalc(parts) {
    return TraceLayout.widthPartsCss(parts);
}

function newLayoutWidthPartsCache() {
    return {
        traceEpoch: currentRuntimeEpoch().trace,
        rules: {},
        groupExpanded: {},
        topLevelGroups: {},
        stages: {},
        searchExpandGroupGeometry: {}
    };
}

function resetLayoutWidthPartsCache() {
    virtualRuntime().layoutWidthPartsCache = newLayoutWidthPartsCache();
    return virtualRuntime().layoutWidthPartsCache;
}

function layoutWidthPartsCache() {
    var cache = virtualRuntime().layoutWidthPartsCache;
    if (!cache || cache.traceEpoch !== currentRuntimeEpoch().trace) {
        cache = resetLayoutWidthPartsCache();
    }
    return cache;
}

function copyWidthParts(parts) {
    return {
        rules: parts.rules,
        px: parts.px
    };
}

function cachedWidthParts(bucket, key, compute) {
    if (bucket[key]) return copyWidthParts(bucket[key]);
    bucket[key] = compute();
    return copyWidthParts(bucket[key]);
}

var TraceGeometry = (function() {
    function zeroWidthParts() {
        return { rules: 0, px: 0 };
    }

    function fixedPxWidthParts(px) {
        return { rules: 0, px: Math.max(0, Math.ceil(Number(px) || 0)) };
    }

    function openRuleWidthParts() {
        return { rules: 1, px: 0 };
    }

    function ruleRefKey(si, gi, ri) {
        return searchExpandOverlayRuleKey(si, gi, ri);
    }

    function activeOverlayRule(si, gi, ri) {
        var overlay = activeSearchExpandOverlay();
        return overlay && overlay.rules && overlay.rules[ruleRefKey(si, gi, ri)] || null;
    }

    function ruleIsOpen(si, gi, ri) {
        var overlay = activeSearchExpandOverlay();
        if (overlay) {
            var rule = overlay.rules && overlay.rules[ruleRefKey(si, gi, ri)];
            return !!(rule && rule.open);
        }
        return effectiveRuleOpen(si, gi, ri);
    }

    function ruleNeedsReadableTitle(si, gi, ri) {
        if (ruleIsOpen(si, gi, ri)) return false;
        return !!activeOverlayRule(si, gi, ri);
    }

    function ruleWidthReason(si, gi, ri) {
        if (ruleIsOpen(si, gi, ri)) return 'open-rule';
        if (ruleNeedsReadableTitle(si, gi, ri)) return 'title-readable-rule';
        return 'collapsed-rule';
    }

    function ruleTitleFallbackText(ref) {
        var rawIdx = rawRuleIndex(ref.si, ref.gi, ref.ri);
        return (rawIdx + 1) + '. ' + ruleDisplayName(ref);
    }

    function ruleTitleFallbackChromeWidthPx(ref) {
        var width = RULE_TITLE_FALLBACK_BASE_WIDTH +
            RULE_TITLE_FALLBACK_NAV_CONTROL_WIDTH;
        if (ruleSupportsDiff(ref)) {
            width += RULE_TITLE_FALLBACK_DIFF_CONTROL_WIDTH;
            width += RULE_TITLE_FALLBACK_FEATURE_CONTROL_WIDTH * 3;
        }
        return width;
    }

    function ruleTitleFallbackWidthForTextPx(text, chromeWidth) {
        var width = (chromeWidth || 0) +
            String(text).length * RULE_TITLE_FALLBACK_CHAR_WIDTH;
        return Math.min(
            RULE_TITLE_FALLBACK_MAX_WIDTH,
            Math.max(RULE_TITLE_FALLBACK_MIN_WIDTH, Math.ceil(width))
        );
    }

    function ruleTitleFallbackWidthPx(si, gi, ri) {
        var rawIdx = rawRuleIndex(si, gi, ri);
        var ref = { si: si, gi: gi, ri: ri };
        if (rawIdx < 0) return RULE_TITLE_FALLBACK_MIN_WIDTH;
        return ruleTitleFallbackWidthForTextPx(
            ruleTitleFallbackText(ref),
            ruleTitleFallbackChromeWidthPx(ref)
        );
    }

    function groupedOpenRuleTitleMinWidthPx(si, gi, ri) {
        return 0;
    }

    function ruleWidthParts(si, gi, ri) {
        var reason = ruleWidthReason(si, gi, ri);
        var key = ruleKey(si, gi, ri) + ':' + reason;
        return cachedWidthParts(layoutWidthPartsCache().rules, key, function() {
            if (reason === 'open-rule') {
                return openRuleWidthParts();
            }
            if (reason === 'title-readable-rule') {
                return fixedPxWidthParts(ruleTitleFallbackWidthPx(si, gi, ri));
            }
            return fixedPxWidthParts(RULE_COLLAPSED_WIDTH);
        });
    }

    function addWidthParts(total, part) {
        total.rules += part.rules;
        total.px += part.px;
    }

    function collapsedRuleRangeWidthParts(totalRuleCount, startRi, endRi) {
        totalRuleCount = Math.max(0, Math.floor(Number(totalRuleCount)) || 0);
        startRi = Math.max(0, Math.floor(Number(startRi)) || 0);
        endRi = Math.min(totalRuleCount, Math.max(startRi, Math.floor(Number(endRi)) || 0));

        var count = Math.max(0, endRi - startRi);
        if (!count) return zeroWidthParts();

        var gapCount = count;
        if (endRi >= totalRuleCount) gapCount = Math.max(0, count - 1);
        return fixedPxWidthParts(count * RULE_COLLAPSED_WIDTH + gapCount * RULE_GAP_WIDTH);
    }

    function groupRuleWidthPartsSignature(si, gi) {
        var summary = activeSearchExpandOverlayGroupSummary(si, gi);
        if (summary && summary.open) {
            var parts = [];
            var count = groupRuleCount(si, gi);
            var indices = Array.isArray(summary.ruleIndices) ? summary.ruleIndices : [];
            for (var i = 0; i < indices.length; i++) {
                var ri = Math.floor(Number(indices[i]));
                if (!Number.isInteger(ri) || ri < 0 || ri >= count) continue;
                parts.push(ri + ':' + ruleWidthReason(si, gi, ri));
            }
            return 'search-open:' + count + ':' + parts.join(',');
        }

        var states = [];
        for (var stateRi = 0; stateRi < groupRuleCount(si, gi); stateRi++) {
            states.push(ruleWidthReason(si, gi, stateRi));
        }
        return states.join('|');
    }

    function groupExpandedWidthParts(si, gi) {
        var count = groupRuleCount(si, gi);
        var summary = activeSearchExpandOverlayGroupSummary(si, gi);
        var key = si + '-' + gi + ':' + count + ':' + groupRuleWidthPartsSignature(si, gi);

        return cachedWidthParts(layoutWidthPartsCache().groupExpanded, key, function() {
            var total = zeroWidthParts();
            if (summary && summary.open) {
                var matched = {};
                var matchedCount = 0;
                var indices = Array.isArray(summary.ruleIndices) ? summary.ruleIndices : [];
                for (var i = 0; i < indices.length; i++) {
                    var ri = Math.floor(Number(indices[i]));
                    if (!Number.isInteger(ri) || ri < 0 || ri >= count) continue;
                    var matchedKey = String(ri);
                    if (matched[matchedKey]) continue;
                    matched[matchedKey] = true;
                    matchedCount++;
                    addWidthParts(total, ruleWidthParts(si, gi, ri));
                }
                total.px += Math.max(0, count - matchedCount) * RULE_COLLAPSED_WIDTH;
                if (count > 1) total.px += (count - 1) * RULE_GAP_WIDTH;
                return total;
            }

            for (var ri = 0; ri < count; ri++) {
                addWidthParts(total, ruleWidthParts(si, gi, ri));
            }
            if (count > 1) total.px += (count - 1) * RULE_GAP_WIDTH;
            return total;
        });
    }

    function topLevelGroupWidthParts(si, gi) {
        var count = groupRuleCount(si, gi);
        if (count <= 1) return ruleWidthParts(si, gi, 0);

        var open = effectiveGroupOpen(si, gi);
        var key = si + '-' + gi + ':' + count + ':' +
            (open ? 'open:' + groupRuleWidthPartsSignature(si, gi) : 'closed');
        return cachedWidthParts(layoutWidthPartsCache().topLevelGroups, key, function() {
            return open
                ? groupExpandedWidthParts(si, gi)
                : fixedPxWidthParts(GROUP_COLLAPSED_WIDTH);
        });
    }

    function stageWidthPartsSignature(si) {
        var overlay = activeSearchExpandOverlay();
        if (overlay) {
            var overlayParts = [];
            var overlayGroupCount = groupCount(si);
            for (var overlayGi = 0; overlayGi < overlayGroupCount; overlayGi++) {
                var overlayRuleCount = groupRuleCount(si, overlayGi);
                var groupSummary = searchExpandOverlayGroupSummary(overlay, si, overlayGi);
                if (overlayRuleCount > 1) {
                    overlayParts.push(
                        overlayRuleCount + ':' +
                        (groupSummary && groupSummary.open
                            ? 'open:' + groupRuleWidthPartsSignature(si, overlayGi)
                            : 'closed')
                    );
                } else {
                    overlayParts.push(
                        overlayRuleCount + ':' +
                        (groupSummary && (
                            groupSummary.openRuleCount > 0 ||
                            groupSummary.matchedRuleCount > 0
                        )
                            ? ruleWidthReason(si, overlayGi, 0)
                            : 'single-closed')
                    );
                }
            }
            return overlayParts.join('|');
        }

        var parts = [];
        var count = groupCount(si);
        for (var gi = 0; gi < count; gi++) {
            var ruleCount = groupRuleCount(si, gi);
            parts.push(
                ruleCount + ':' +
                (ruleCount > 1
                    ? (effectiveGroupOpen(si, gi) ? 'open:' + groupRuleWidthPartsSignature(si, gi) : 'closed')
                    : ruleWidthReason(si, gi, 0))
            );
        }
        return parts.join('|');
    }

    function stageExpandedWidthParts(si) {
        var count = groupCount(si);
        var key = si + ':' + count + ':' + stageWidthPartsSignature(si);

        return cachedWidthParts(layoutWidthPartsCache().stages, key, function() {
            var total = zeroWidthParts();
            for (var gi = 0; gi < count; gi++) {
                addWidthParts(total, topLevelGroupWidthParts(si, gi));
            }
            if (count > 1) total.px += (count - 1) * RULE_GAP_WIDTH;
            return total;
        });
    }

    function stageTitleFallbackWidthPx(si) {
        if (currentStageCount() <= STAGE_SHELL_VIRTUALIZATION_THRESHOLD) return 0;

        var title = traceStageName(si) + ' (' + traceStageRuleCount(si) + ')';
        var width = STAGE_TITLE_FALLBACK_BASE_WIDTH +
            String(title).length * STAGE_TITLE_FALLBACK_CHAR_WIDTH;
        return Math.min(STAGE_TITLE_FALLBACK_MAX_WIDTH, Math.ceil(width));
    }

    function groupTitleFallbackWidthPx(si, gi) {
        if (!effectiveGroupOpen(si, gi) || groupRuleCount(si, gi) <= 1) return 0;

        var title = traceGroupName(si, gi) + ' (x' + groupRuleCount(si, gi) + ')';
        var width = GROUP_TITLE_FALLBACK_BASE_WIDTH +
            String(title).length * GROUP_TITLE_FALLBACK_CHAR_WIDTH;
        return Math.min(GROUP_TITLE_FALLBACK_MAX_WIDTH, Math.ceil(width));
    }

    return {
        addWidthParts: addWidthParts,
        collapsedRuleRangeWidthParts: collapsedRuleRangeWidthParts,
        groupExpandedWidthParts: groupExpandedWidthParts,
        groupRuleWidthPartsSignature: groupRuleWidthPartsSignature,
        groupTitleFallbackWidthPx: groupTitleFallbackWidthPx,
        groupedOpenRuleTitleMinWidthPx: groupedOpenRuleTitleMinWidthPx,
        ruleNeedsReadableTitle: ruleNeedsReadableTitle,
        ruleTitleFallbackChromeWidthPx: ruleTitleFallbackChromeWidthPx,
        ruleTitleFallbackText: ruleTitleFallbackText,
        ruleTitleFallbackWidthForTextPx: ruleTitleFallbackWidthForTextPx,
        ruleTitleFallbackWidthPx: ruleTitleFallbackWidthPx,
        ruleWidthParts: ruleWidthParts,
        ruleWidthReason: ruleWidthReason,
        stageExpandedWidthParts: stageExpandedWidthParts,
        stageTitleFallbackWidthPx: stageTitleFallbackWidthPx,
        stageWidthPartsSignature: stageWidthPartsSignature,
        topLevelGroupWidthParts: topLevelGroupWidthParts
    };
})();

function ruleWidthParts(si, gi, ri) {
    return TraceGeometry.ruleWidthParts(si, gi, ri);
}

function addWidthParts(total, part) {
    TraceGeometry.addWidthParts(total, part);
}

function ruleWidthReason(si, gi, ri) {
    return TraceGeometry.ruleWidthReason(si, gi, ri);
}

function ruleNeedsReadableTitle(si, gi, ri) {
    return TraceGeometry.ruleNeedsReadableTitle(si, gi, ri);
}

function ruleTitleFallbackText(ref) {
    return TraceGeometry.ruleTitleFallbackText(ref);
}

function ruleTitleFallbackChromeWidthPx(ref) {
    return TraceGeometry.ruleTitleFallbackChromeWidthPx(ref);
}

function ruleTitleFallbackWidthForTextPx(text, chromeWidth) {
    return TraceGeometry.ruleTitleFallbackWidthForTextPx(text, chromeWidth);
}

function ruleTitleFallbackWidthPx(si, gi, ri) {
    return TraceGeometry.ruleTitleFallbackWidthPx(si, gi, ri);
}

function groupedOpenRuleTitleMinWidthPx(si, gi, ri) {
    return TraceGeometry.groupedOpenRuleTitleMinWidthPx(si, gi, ri);
}

function collapsedRuleRangeWidthParts(totalRuleCount, startRi, endRi) {
    return TraceGeometry.collapsedRuleRangeWidthParts(totalRuleCount, startRi, endRi);
}

function groupRuleWidthPartsSignature(si, gi) {
    return TraceGeometry.groupRuleWidthPartsSignature(si, gi);
}

function groupExpandedWidthParts(si, gi) {
    return TraceGeometry.groupExpandedWidthParts(si, gi);
}

function topLevelGroupWidthParts(si, gi) {
    return TraceGeometry.topLevelGroupWidthParts(si, gi);
}

function stageWidthPartsSignature(si) {
    return TraceGeometry.stageWidthPartsSignature(si);
}

function stageExpandedWidthParts(si) {
    return TraceGeometry.stageExpandedWidthParts(si);
}

function stageTitleFallbackWidthPx(si) {
    return TraceGeometry.stageTitleFallbackWidthPx(si);
}

function groupTitleFallbackWidthPx(si, gi) {
    return TraceGeometry.groupTitleFallbackWidthPx(si, gi);
}

function updateGroupLayoutWidth(si, gi) {
    if (groupRuleCount(si, gi) <= 1) return;
    invalidateTraceMeasuredWidthCache();

    var el = document.getElementById('group-' + si + '-' + gi);
    if (!el) return;
    el.style.setProperty(
        '--group-width',
        topLevelGroupLayoutWidthPx(si, gi, currentRuleWidthPx()) + 'px'
    );
}

function updateStageLayoutWidth(si) {
    invalidateTraceMeasuredWidthCache();
    var el = document.getElementById('stage-exp-' + si);
    if (!el || !stageHasRules(si)) return;
    el.style.setProperty('--stage-width', widthCalc(stageExpandedWidthParts(si)));
}

function updateStageLayoutWidths(si) {
    for (var gi = 0; gi < groupCount(si); gi++) {
        updateGroupLayoutWidth(si, gi);
    }
    updateStageLayoutWidth(si);
}

function updateAllLayoutWidths() {
    invalidateTraceMeasuredWidthCache();
    for (var si = 0; si < currentStageCount(); si++) {
        updateStageLayoutWidths(si);
    }
}
