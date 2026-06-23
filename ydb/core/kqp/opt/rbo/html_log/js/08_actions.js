var SEARCH_INPUT_DEBOUNCE_MS = 48;
var SEARCH_INPUT_MAX_WAIT_MS = 120;
var SEARCH_AUTO_GLOBAL_DEBOUNCE_MS = 1500;

function cancelAutoGlobalSearchTimer(reason) {
    var search = searchRuntime();
    if (search.autoGlobalSearchTimer === null || search.autoGlobalSearchTimer === undefined) {
        return false;
    }
    var timer = search.autoGlobalSearchTimer;
    var cancel = search.autoGlobalSearchTimerCancel || clearTimeout;
    search.autoGlobalSearchTimer = null;
    search.autoGlobalSearchTimerCancel = null;
    search.autoGlobalSearchIntent = null;
    cancel(timer);
    return true;
}

function setAutoGlobalSearchTimer(timer, cancel, intent) {
    var search = searchRuntime();
    cancelAutoGlobalSearchTimer('replaced');
    search.autoGlobalSearchTimer = timer;
    search.autoGlobalSearchTimerCancel = cancel || clearTimeout;
    search.autoGlobalSearchIntent = intent || null;
}

function createTraceActions(actionContext) {
    var actionCtx = actionContext || TraceActionContext;

    function allRules() { return actionCtx.allRules(); }
    function diffState() { return actionCtx.diffState(); }
    function fullscreenState() { return actionCtx.fullscreenState(); }
    function searchState() { return actionCtx.searchState(); }
    function traceGroups() { return actionCtx.traceGroups(); }
    function traceState() { return actionCtx.traceState(); }
    function uiState() { return actionCtx.uiState(); }
    function byId(id) { return actionCtx.dom.byId(id); }
    function addWindowListener(type, listener) {
        actionCtx.dom.addWindowListener(type, listener);
    }
    function scheduleTimeout(fn, delay) {
        return actionCtx.timers.setTimeout(fn, delay);
    }
    function scheduleInterval(fn, delay) {
        return actionCtx.timers.setInterval(fn, delay);
    }

    function clearTimer(id) {
        actionCtx.timers.clearTimeout(id);
    }

    function stopEvent(ev) {
        if (ev && ev.stopPropagation) ev.stopPropagation();
    }

    function consumeEvent(ev) {
        stopEvent(ev);
        if (ev && ev.preventDefault) ev.preventDefault();
    }

    function runLayoutTransition(mutator) {
        var changed = false;
        preserveTraceViewportAnchor(function() {
            changed = refreshLayoutTransition(mutator());
        });
        if (changed) updateCycleButtonLabel();
        return changed;
    }

    function toggleStage(si) {
        runLayoutTransition(function() {
            return TraceState.toggleStage(uiState(), traceGroups(), si);
        });
    }

    function toggleStageRules(si, ev) {
        stopEvent(ev);
        if (!stageHasRules(si)) return;

        runLayoutTransition(function() {
            return TraceState.toggleStageRules(uiState(), traceGroups(), si);
        });
    }

    function toggleGroup(si, gi, ev) {
        stopEvent(ev);
        var count = groupRuleCount(si, gi);
        if (count <= 1) return;

        runLayoutTransition(function() {
            return TraceState.toggleGroup(uiState(), traceGroups(), si, gi);
        });
    }

    function toggleGroupRules(si, gi, ev) {
        stopEvent(ev);
        var count = groupRuleCount(si, gi);
        if (count <= 1 || !groupState(si, gi).open) return;

        runLayoutTransition(function() {
            return TraceState.toggleGroupRules(uiState(), traceGroups(), si, gi);
        });
    }

    function toggleRule(si, gi, ri, ev) {
        stopEvent(ev);
        if (isTitleControlClick(ev)) return;
        if (isFullscreenRule(si, gi, ri)) {
            closeRuleFullscreen(ev);
            return;
        }
        var wasOpen = !!ruleState(si, gi, ri).open;
        var lockTraceHorizontalScroll = !!ev && !wasOpen &&
            typeof withTraceHorizontalScrollLock === 'function';
        if (wasOpen &&
                typeof captureMountedRulePaneScroll === 'function') {
            captureMountedRulePaneScroll(si, gi, ri);
        }
        var transition = function() {
            return TraceState.toggleRule(uiState(), traceGroups(), si, gi, ri);
        };
        if (lockTraceHorizontalScroll) {
            withTraceHorizontalScrollLock('click-expand-rule', function() {
                runLayoutTransition(transition);
            });
        } else {
            runLayoutTransition(transition);
        }
    }

    function onCollapsedRuleClick(si, gi, ri, ev) {
        stopEvent(ev);
        if (!ruleState(si, gi, ri).open) toggleRule(si, gi, ri, ev);
    }

    function onCollapsedGroupClick(si, gi, ev) {
        stopEvent(ev);
        if (!groupState(si, gi).open) toggleGroup(si, gi, null);
    }

    function setAllRuleFeatureAction(feature, visible, ev) {
        consumeEvent(ev);

        var changed = false;
        var query = currentSearchQuery();
        visible = !!visible;

        withRuleLocalViewportStability('set-all-rule-feature:' + feature, function() {
            forEachRule(function(si, gi, ri, rule) {
                if (!ruleHasFeature(si, gi, ri, feature)) return;
                var sessionChanged = feature === 'fields'
                    ? clearRuleFieldRowSessionOverrides(si, gi, ri)
                    : false;
                if (rule[feature] === visible && !sessionChanged) return;

                if (rule[feature] !== visible) {
                    TraceState.setRuleFeature(uiState(), si, gi, ri, feature, visible);
                }
                changed = true;

                if (feature === 'info') {
                    updateRuleInfoDisplay(si, gi, ri);
                    refreshRuleInfoLayoutAfterFeatureChange(si, gi, ri);
                } else {
                    updateRuleFeatureButtons(si, gi, ri);
                    requestRuleTreeRenderIfVisible(si, gi, ri, query, false, currentSearchScope());
                }
            });

            if (changed) {
                rerenderActiveFullscreenDiff();
                if (!isFullscreenDiff()) {
                    var fullscreenRef = fullscreenCurrentRule();
                    if (fullscreenRef) {
                        rerenderFullscreenAfterFeatureChange(fullscreenRef.si, fullscreenRef.gi, fullscreenRef.ri);
                    }
                }
                markCollapsedSearchIndicatorsDirty();
                refreshSearchStateForCurrentLayout();
            }
        });
        updateBulkDetailControls();
    }

    function nodeColumnFeedbackAction(ev) {
        var target = ev && ev.target && ev.target.closest
            ? ev.target.closest('[data-trace-action]')
            : null;
        return target && target.getAttribute ? target.getAttribute('data-trace-action') : '';
    }

    function parseNodeColumnFeedbackRuleKey(ruleKeyValue) {
        ruleKeyValue = String(ruleKeyValue || '');
        if (ruleKeyValue.indexOf('fs-') === 0) ruleKeyValue = ruleKeyValue.substring(3);
        var parts = ruleKeyValue.split('-');
        if (parts.length !== 3) return null;
        var ref = {
            si: Number(parts[0]),
            gi: Number(parts[1]),
            ri: Number(parts[2])
        };
        if (!Number.isFinite(ref.si) || !Number.isFinite(ref.gi) || !Number.isFinite(ref.ri)) {
            return null;
        }
        return ruleRefValidForCurrentTrace(ref) ? ref : null;
    }

    function nodeColumnFeedbackRuleRef(ev) {
        var target = ev && ev.target && ev.target.closest ? ev.target : null;
        if (!target) return null;

        var header = target.closest('.tree-pinned-header[id^="pinhdr-"]');
        if (header && header.id) {
            return parseNodeColumnFeedbackRuleKey(header.id.substring('pinhdr-'.length));
        }

        var root = target.closest('[id^="treeroot-"]');
        if (root && root.id) {
            return parseNodeColumnFeedbackRuleKey(root.id.substring('treeroot-'.length));
        }
        return null;
    }

    function nodeColumnFeedbackFeature(visible, ev) {
        var action = nodeColumnFeedbackAction(ev);
        if (visible && action === 'pin-node-column') return 'pinned';
        if (!visible && action === 'unpin-node-column') return 'fields';
        return '';
    }

    function setRuleFeatureVisibleForNodeColumnFeedback(ref, feature) {
        if (!ref || !feature || !ruleHasFeature(ref.si, ref.gi, ref.ri, feature)) return false;
        var change = TraceState.setRuleFeature(uiState(), ref.si, ref.gi, ref.ri, feature, true);
        if (!change.changed) return false;
        updateRuleFeatureButtons(ref.si, ref.gi, ref.ri);
        return true;
    }

    function renderNodeColumnFeedbackFeatureChange(ref, feature) {
        if (!ref || !feature) return;
        renderRuleFeatureChange(feature, ref.si, ref.gi, ref.ri);
        markCollapsedSearchIndicatorsDirty();
        refreshSearchStateForCurrentLayout();
        updateBulkDetailControls();
    }

    function applyNodeColumnProjectionRefresh() {
        var query = currentSearchQuery();
        var scope = currentSearchScope();

        clearTraceMeasuredWidthCache();
        updateAllLayoutWidths();
        refreshVirtualRowsNow();
        /* Queue forced renders only after the virtual rows refresh: it
           bumps the rows DOM generation, which would invalidate any
           render job queued beforehand (stale_dom_generation). */
        forEachRule(function(si, gi, ri) {
            requestRuleTreeRenderIfVisible(si, gi, ri, query, true, scope);
        });
        scheduleVisibleRuleRenderScan();
        rerenderActiveFullscreenDiff();
        if (!isFullscreenDiff()) {
            var fullscreenRef = fullscreenCurrentRule();
            if (fullscreenRef) {
                rerenderFullscreenAfterFeatureChange(fullscreenRef.si, fullscreenRef.gi, fullscreenRef.ri);
            }
        }
        markCollapsedSearchIndicatorsDirty();
        refreshSearchStateForCurrentLayout();
        updateBulkDetailControls();
        scheduleTraceAnchorLineUpdate();
        scheduleDiffMoveArrows();
    }

    function refreshNodeColumnProjection() {
        withViewportAnchors('node-column-projection', applyNodeColumnProjectionRefresh);
    }

    function runNodeColumnProjectionMutation(reason, mutation, unchanged) {
        withViewportAnchors(reason || 'node-column-projection-mutation', function() {
            if (mutation()) {
                applyNodeColumnProjectionRefresh();
            } else if (unchanged) {
                unchanged();
            } else {
                updateBulkDetailControls();
            }
        });
    }

    function setNodeColumnVisibleAction(key, visible, ev) {
        consumeEvent(ev);
        runNodeColumnProjectionMutation('node-column-visible', function() {
            var feedbackFeature = nodeColumnFeedbackFeature(visible, ev);
            var feedbackRef = feedbackFeature ? nodeColumnFeedbackRuleRef(ev) : null;
            var columnChanged = setNodeColumnVisible(key, visible);
            var featureChanged = setRuleFeatureVisibleForNodeColumnFeedback(feedbackRef, feedbackFeature);
            if (!columnChanged && featureChanged) {
                renderNodeColumnFeedbackFeatureChange(feedbackRef, feedbackFeature);
            }
            return columnChanged;
        });
    }

    function soloNodeColumnAction(key, ev) {
        consumeEvent(ev);
        runNodeColumnProjectionMutation('node-column-solo', function() {
            return soloNodeColumn(key);
        });
    }

    function setAllNodeColumnsAction(visible, ev) {
        consumeEvent(ev);
        runNodeColumnProjectionMutation('node-column-set-all', function() {
            return setAllNodeColumnsVisible(visible);
        });
    }

    function applyNodeColumnPresetAction(index, ev) {
        consumeEvent(ev);
        runNodeColumnProjectionMutation('node-column-preset', function() {
            return setNodeColumnPreset(index);
        });
    }

    function resetNodeColumnsAction(ev) {
        consumeEvent(ev);
        runNodeColumnProjectionMutation('node-column-reset', function() {
            return resetNodeColumnsToDefault();
        });
    }

    function refreshDiffFieldProjection() {
        cancelPendingDiffJob();
        invalidateActiveDiffResult();
        if (isDiffActive() && activeDiffPairKey()) {
            beginPendingDiffJob(activeDiffPairKey());
            renderPendingDiffPlainTrees();
            syncFullscreenModeWithDiff();
        }
        rerenderActiveFullscreenDiff();
        updateDiffSelectionButtonContent();
        updateBulkDetailControls();
        scheduleDiffMoveArrows();
    }

    function setDiffFieldVisibleAction(key, visible, ev) {
        consumeEvent(ev);
        if (setDiffFieldVisible(key, visible)) {
            refreshDiffFieldProjection();
        } else {
            updateBulkDetailControls();
        }
    }

    function soloDiffFieldAction(key, ev) {
        consumeEvent(ev);
        if (soloDiffField(key)) {
            refreshDiffFieldProjection();
        } else {
            updateBulkDetailControls();
        }
    }

    function setAllDiffFieldsAction(visible, ev) {
        consumeEvent(ev);
        if (setAllDiffFieldsVisible(visible)) {
            refreshDiffFieldProjection();
        } else {
            updateBulkDetailControls();
        }
    }

    function resetDiffFieldsAction(ev) {
        consumeEvent(ev);
        if (resetDiffFieldsToDefault()) {
            refreshDiffFieldProjection();
        } else {
            updateBulkDetailControls();
        }
    }

    function setNodeColumnWidthAction(key, width, ev) {
        consumeEvent(ev);
        runNodeColumnProjectionMutation('node-column-width', function() {
            return setNodeColumnWidth(key, width);
        }, function() {});
    }

    function reorderNodeColumnAction(key, dropIndex, ev) {
        consumeEvent(ev);
        runNodeColumnProjectionMutation('node-column-reorder', function() {
            return reorderNodeColumn(key, dropIndex);
        });
    }

    function setEmptyStagesVisibleAction(visible, ev) {
        consumeEvent(ev);

        visible = !!visible;
        if (traceState().showEmptyStages === visible) {
            updateEmptyStageControls();
            return;
        }

        traceState().showEmptyStages = visible;
        preserveTraceViewportAnchor(function() {
            renderTraceShell();
            clearTraceMeasuredWidthCache();
            updateAllLayoutWidths();
            refreshVirtualRowsNow();
            scheduleVisibleRuleRenderScan();
        });
        markCollapsedSearchIndicatorsDirty();
        refreshSearchStateForCurrentLayout();
        updateBulkDetailControls();
        updateCycleButtonLabel();
        scheduleTraceAnchorLineUpdate();
        scheduleDiffMoveArrows();
    }

    function enterRuleFullscreenAction(si, gi, ri) {
        withViewportAnchors('enter-rule-fullscreen', function() {
            setFullscreenFocus(si, gi, ri, 'rule');
        });
    }

    function enterDiffFullscreenAction(si, gi, ri) {
        withViewportAnchors('enter-diff-fullscreen', function() {
            setFullscreenFocus(si, gi, ri, 'diff');
        });
    }

    function setFullscreenRuleAction(si, gi, ri) {
        withViewportAnchors('set-fullscreen-rule', function() {
            setFullscreenFocus(si, gi, ri);
        });
    }

    function closeRuleFullscreenAction(ev) {
        closeFullscreen(ev);
    }

    function closeRuleFullscreenToRuleAction(si, gi, ri, ev) {
        closeFullscreen(ev, ruleRef(si, gi, ri));
    }

    function closeRuleFullscreenFromTitleAction(ev) {
        if (isTitleControlClick(ev)) {
            stopEvent(ev);
            return;
        }
        closeRuleFullscreen(ev);
    }

    function closeRuleFullscreenToRuleFromTitleAction(si, gi, ri, ev) {
        if (isTitleControlClick(ev)) {
            stopEvent(ev);
            return;
        }
        closeRuleFullscreenToRule(si, gi, ri, ev);
    }

    function closeFullscreenOnOutsideClickAction(ev) {
        if (!isFullscreenOpen()) return;

        if (ev.target.closest && ev.target.closest('.rule-cell.fullscreen-rule')) return;
        if (shouldSuppressFullscreenNavDocumentClick(ev)) return;
        closeRuleFullscreen(ev);
    }

    function toggleRuleFullscreenAction(si, gi, ri, ev) {
        stopEvent(ev);

        if (isFullscreenRule(si, gi, ri)) {
            closeRuleFullscreen(ev);
            return;
        }

        setFullscreenRule(si, gi, ri);
    }

    function featureButtonId(feature, si, gi, ri) {
        if (feature === 'fields') return 'fieldbtn-' + si + '-' + gi + '-' + ri;
        if (feature === 'pinned') return 'pinbtn-' + si + '-' + gi + '-' + ri;
        return '';
    }

    function syncFeatureButton(feature, si, gi, ri) {
        var id = featureButtonId(feature, si, gi, ri);
        var btn = id ? byId(id) : null;
        if (btn) {
            var available = ruleHasFeature(si, gi, ri, feature);
            btn.disabled = !available;
            btn.classList.toggle('active', available && ruleFeatureButtonActive(feature, si, gi, ri));
        }
    }

    function renderRuleFeatureChange(feature, si, gi, ri) {
        if (feature === 'info') {
            updateRuleInfoDisplay(si, gi, ri);
            refreshRuleInfoLayoutAfterFeatureChange(si, gi, ri);
            rerenderFullscreenAfterFeatureChange(si, gi, ri);
            return;
        }

        rerenderRuleAfterFeatureChange(si, gi, ri);
        syncFeatureButton(feature, si, gi, ri);
    }

    function updateDisabledFullscreenPeerFeature(feature, ref) {
        if (!ref) return;
        if (feature === 'info') {
            updateRuleInfoDisplay(ref.si, ref.gi, ref.ri);
            refreshRuleInfoLayoutAfterFeatureChange(ref.si, ref.gi, ref.ri);
        } else {
            updateRuleFeatureButtons(ref.si, ref.gi, ref.ri);
        }
    }

    function toggleRuleFeatureAction(si, gi, ri, feature, ev) {
        stopEvent(ev);
        if (!ruleHasFeature(si, gi, ri, feature)) {
            updateBulkDetailControls();
            return;
        }
        withRuleLocalViewportStability('toggle-rule-feature:' + feature, function() {
            var change = TraceState.toggleRuleFeature(uiState(), si, gi, ri, feature);
            if (!change.changed) return;
            var state = ruleState(si, gi, ri);
            var disablesPeer = feature === 'fields' || feature === 'info';
            var disabledOther = disablesPeer && state[feature]
                ? disableOtherFullscreenDiffFeature(si, gi, ri, feature)
                : null;

            renderRuleFeatureChange(feature, si, gi, ri);
            updateDisabledFullscreenPeerFeature(feature, disabledOther);
            markCollapsedSearchIndicatorsDirty();
            refreshSearchStateForCurrentLayout();
            updateBulkDetailControls();
        });
    }

    function setRuleFieldsExpanded(si, gi, ri, expanded) {
        if (!ruleHasFeature(si, gi, ri, 'fields')) return;

        expanded = !!expanded;
        var state = ruleState(si, gi, ri);
        var featureChanged = !!state.fields !== expanded;
        var sessionChanged = clearRuleFieldRowSessionOverrides(si, gi, ri);
        if (!featureChanged && !sessionChanged) {
            syncFeatureButton('fields', si, gi, ri);
            return;
        }

        withRuleLocalViewportStability('set-rule-fields-expanded', function() {
            if (featureChanged) {
                TraceState.setRuleFeature(uiState(), si, gi, ri, 'fields', expanded);
            }
            var disabledOther = expanded
                ? disableOtherFullscreenDiffFeature(si, gi, ri, 'fields')
                : null;

            renderRuleFeatureChange('fields', si, gi, ri);
            updateDisabledFullscreenPeerFeature('fields', disabledOther);
            markCollapsedSearchIndicatorsDirty();
            refreshSearchStateForCurrentLayout();
            updateBulkDetailControls();
        });
    }

    function toggleRuleFieldsAction(si, gi, ri, ev) {
        stopEvent(ev);
        setRuleFieldsExpanded(si, gi, ri, !ruleFeatureButtonActive('fields', si, gi, ri));
    }

    function toggleRulePinnedAction(si, gi, ri, ev) {
        toggleRuleFeatureAction(si, gi, ri, 'pinned', ev);
    }

    function toggleRuleInfoAction(si, gi, ri, ev) {
        toggleRuleFeatureAction(si, gi, ri, 'info', ev);
    }

    function setRuleInfoTabAction(si, gi, ri, tabId, ev) {
        stopEvent(ev);
        if (typeof setRuleInfoTab === 'function') {
            withRuleLocalViewportStability('set-rule-info-tab', function() {
                setRuleInfoTab(si, gi, ri, tabId, ev);
            });
        }
    }

    function setRuleInfoSwitcherAction(si, gi, ri, switcherKey, switcherId, ev) {
        stopEvent(ev);
        if (typeof setRuleInfoSwitcher === 'function') {
            withRuleLocalViewportStability('set-rule-info-switcher', function() {
                setRuleInfoSwitcher(si, gi, ri, switcherKey, switcherId, ev);
            });
        }
    }

    function scrollFieldDetailsAfterRender(si, gi, ri, metaIndex, idScope) {
        actionCtx.frame.request(function() {
            if (typeof scrollRuleInfoFieldDetailIntoView === 'function') {
                scrollRuleInfoFieldDetailIntoView(si, gi, ri, metaIndex, {
                    idScope: idScope || ''
                });
            }
        });
    }

    function renderFieldDetailsPanel(si, gi, ri, metaIndex, idScope) {
        var query = currentSearchQuery();
        var scope = currentSearchScope();
        if (idScope === 'fs') {
            if (typeof rerenderFullscreenInfoPanel === 'function') {
                rerenderFullscreenInfoPanel(si, gi, ri);
            }
        } else {
            updateRuleInfoDisplay(si, gi, ri);
            renderRuleInfoPanel(si, gi, ri, query, scope);
            refreshRuleInfoLayoutAfterFeatureChange(si, gi, ri);
        }
        scrollFieldDetailsAfterRender(si, gi, ri, metaIndex, idScope);
    }

    function openFieldDetailsAction(nodeId, metaIndex, ev) {
        consumeEvent(ev);
        if (typeof parseNormalTreeNodeId !== 'function' ||
                typeof setRuleInfoFieldDetails !== 'function') {
            return false;
        }

        var ref = parseNormalTreeNodeId(nodeId);
        if (!ref || !validRuleRef(ref.si, ref.gi, ref.ri)) return false;
        metaIndex = Math.max(0, Math.floor(Number(metaIndex)));
        if (!Number.isFinite(metaIndex)) metaIndex = 0;

        return withRuleLocalViewportStability('open-info-field-details', function() {
            if (!setRuleInfoFieldDetails(ref.si, ref.gi, ref.ri, ref.path, metaIndex, null)) {
                return false;
            }

            var change = TraceState.setRuleFeature(uiState(), ref.si, ref.gi, ref.ri, 'info', true);
            if (change.changed) {
                renderRuleFeatureChange('info', ref.si, ref.gi, ref.ri);
                markCollapsedSearchIndicatorsDirty();
                refreshSearchStateForCurrentLayout();
                updateBulkDetailControls();
            }
            renderFieldDetailsPanel(
                ref.si,
                ref.gi,
                ref.ri,
                metaIndex,
                String(nodeId || '').indexOf('tn-fs-') === 0 ? 'fs' : ''
            );
            return true;
        });
    }

    function renderInfoFieldDetailsControlChange(si, gi, ri, ev) {
        var idScope = ev && ev.target && ev.target.closest && ev.target.closest('.fullscreen-root') ? 'fs' : '';
        if (idScope === 'fs' && typeof rerenderFullscreenInfoPanel === 'function') {
            rerenderFullscreenInfoPanel(si, gi, ri);
        } else {
            renderRuleInfoPanel(si, gi, ri, currentSearchQuery(), currentSearchScope());
            refreshRuleInfoLayoutAfterFeatureChange(si, gi, ri);
        }
    }

    function pinInfoFieldDetailsAction(si, gi, ri, tabId, ev) {
        if (ev === undefined && tabId &&
                (tabId.stopPropagation || tabId.preventDefault)) {
            ev = tabId;
            tabId = '';
        }
        stopEvent(ev);
        if (typeof pinRuleInfoFieldDetails !== 'function') return false;
        return withRuleLocalViewportStability('pin-info-field-details', function() {
            if (!pinRuleInfoFieldDetails(si, gi, ri, tabId, ev)) return false;
            renderInfoFieldDetailsControlChange(si, gi, ri, ev);
            return true;
        });
    }

    function closeInfoFieldDetailsAction(si, gi, ri, tabId, ev) {
        if (ev === undefined && tabId &&
                (tabId.stopPropagation || tabId.preventDefault)) {
            ev = tabId;
            tabId = '';
        }
        stopEvent(ev);
        if (typeof closeRuleInfoFieldDetails !== 'function') return false;
        return withRuleLocalViewportStability('close-info-field-details', function() {
            if (!closeRuleInfoFieldDetails(si, gi, ri, tabId, ev)) return false;
            renderInfoFieldDetailsControlChange(si, gi, ri, ev);
            return true;
        });
    }

    function navigateRuleDirectionAction(direction, si, gi, ri, ev, blockOtherDiffRule) {
        stopEvent(ev);
        var target = navTarget(direction, si, gi, ri);
        if (!target) return false;
        if (blockOtherDiffRule &&
            isOtherActiveDiffRule(ruleRef(si, gi, ri), target.stageIdx, target.groupIdx, target.ruleIdx)) {
            return false;
        }
        if (isFullscreenOpen()) {
            setFullscreenRule(target.stageIdx, target.groupIdx, target.ruleIdx);
            return true;
        }
        ensureRuleVisible(target.stageIdx, target.groupIdx, target.ruleIdx);
        settleNavigatedRule(target.stageIdx, target.groupIdx, target.ruleIdx);
        return true;
    }

    function navigatePrevAction(si, gi, ri, ev) {
        return navigateRuleDirectionAction('prev', si, gi, ri, ev, false);
    }

    function navigateNextAction(si, gi, ri, ev) {
        return navigateRuleDirectionAction('next', si, gi, ri, ev, false);
    }

    function groupNavigateDirectionAction(direction, si, gi, ev) {
        stopEvent(ev);
        if (isFullscreenOpen()) return;

        var idx;
        if (direction === 'prev') {
            idx = findFlatIndex(si, gi, 0);
            if (idx <= 0) return;
            idx--;
        } else {
            var count = groupRuleCount(si, gi);
            idx = findFlatIndex(si, gi, count - 1);
            if (idx >= allRules().length - 1) return;
            idx++;
        }

        var target = allRules()[idx];
        ensureRuleVisible(target.stageIdx, target.groupIdx, target.ruleIdx);
        settleNavigatedRule(target.stageIdx, target.groupIdx, target.ruleIdx);
    }

    function groupNavigatePrevAction(si, gi, ev) {
        groupNavigateDirectionAction('prev', si, gi, ev);
    }

    function groupNavigateNextAction(si, gi, ev) {
        groupNavigateDirectionAction('next', si, gi, ev);
    }

    function startFullscreenNavRepeatAction(direction, si, gi, ri, ev, blockOtherDiffRule) {
        consumeEvent(ev);
        if (!isFullscreenOpen()) return;
        if (isRuleNavBlocked(direction, si, gi, ri, blockOtherDiffRule)) return;

        stopFullscreenNavRepeat();

        var state = {
            direction: direction,
            si: si,
            gi: gi,
            ri: ri,
            pointerId: ev && ev.pointerId
        };
        fullscreenState().fullscreenNavRepeatState = state;
        suppressNextNavClick(direction, 1200);

        if (!navigateRuleDirectionAction(direction, si, gi, ri, null, blockOtherDiffRule)) {
            stopFullscreenNavRepeat(ev);
            return;
        }
        state.delayTimer = scheduleTimeout(function() {
            if (fullscreenState().fullscreenNavRepeatState !== state) return;
            state.intervalTimer = scheduleInterval(function() {
                if (!stepFullscreenNavRepeat(direction)) stopFullscreenNavRepeat();
            }, FULLSCREEN_NAV_REPEAT_INTERVAL_MS);
        }, FULLSCREEN_NAV_REPEAT_DELAY_MS);

        if (ev && ev.target && ev.target.setPointerCapture && ev.pointerId !== undefined) {
            try { ev.target.setPointerCapture(ev.pointerId); } catch (err) {}
        }
        addWindowListener('pointerup', stopFullscreenNavRepeat);
        addWindowListener('pointercancel', stopFullscreenNavRepeat);
        addWindowListener('blur', stopFullscreenNavRepeat);
    }

    function handleNavButtonClickAction(direction, si, gi, ri, ev, blockOtherDiffRule) {
        consumeEvent(ev);
        if (shouldSuppressNavClick(direction)) return false;
        return navigateRuleDirectionAction(direction, si, gi, ri, ev, blockOtherDiffRule);
    }

    function mergeLayoutChange(left, right) {
        if (!left) return right;
        if (!right) return left;
        return TraceState.mergeTransitionSummary(left, right);
    }

    function keepFullscreenRulePathOpen(layoutChange) {
        var ref = fullscreenCurrentRule();
        if (!ref) return layoutChange;
        return mergeLayoutChange(layoutChange, openRulePath(ref.si, ref.gi, ref.ri));
    }

    function refreshLayoutChangeOrAll(layoutChange) {
        if (layoutChange && layoutChange.changed) {
            refreshLayoutTransitionDisplays(layoutChange);
        } else {
            updateAllDisplays();
        }
    }

    function refreshAfterStandaloneDiffSelectionClear() {
        var fullscreenRef = fullscreenCurrentRule();
        syncFullscreenModeWithDiff();
        refreshLayoutChangeOrAll(keepFullscreenRulePathOpen(null));
        rerenderAllTrees(currentSearchQuery(), true);
        if (fullscreenRef && fullscreenCurrentRule()) {
            renderFullscreenRuleImmediately(
                fullscreenRef.si,
                fullscreenRef.gi,
                fullscreenRef.ri,
                currentSearchQuery(),
                currentSearchScope()
            );
        }
        updateSearchLabelHighlights(currentSearchQuery(), currentSearchScope());
        scheduleDiffMoveArrows();
    }

    function onDiffButtonClickAction() {
        if (diffState().savedDiffState) {
            closeDiffFull();
        } else if (diffState().diffA || diffState().diffB) {
            cancelPendingDiffJob();
            invalidateActiveDiffResult();
            clearDiffSelection('both');
            updateDiffButton();
        }
    }

    function exitDiffKeepOtherFromRuleAction(keepWhich, si, gi, ri) {
        var keepFullscreen = isFullscreenOpen();
        exitDiffKeepOther(keepWhich, ruleRef(si, gi, ri));
        if (keepFullscreen) setFullscreenRule(si, gi, ri);
    }

    function diffSideButtonId(side, si, gi, ri) {
        return (side === 'a' ? 'abtn-' : 'bbtn-') + si + '-' + gi + '-' + ri;
    }

    function diffSideSelectedClass(side) {
        return side === 'a' ? 'selected-a' : 'selected-b';
    }

    function setDiffSideAction(side, si, gi, ri, ev) {
        consumeEvent(ev);
        return withDiffRenderTransition('diff-pair-change', {
            side: side,
            ruleKey: ruleKey(si, gi, ri)
        }, function() {
            if (!ruleSupportsDiff(ruleRef(si, gi, ri))) return false;

            var outcome = TraceDiffState.selectSide(
                currentDiffSession(),
                side,
                TraceDiffState.ruleRef(si, gi, ri)
            );

            if (outcome.action === 'exit-keep-other') {
                cancelPendingDiffJob();
                invalidateActiveDiffResult();
                exitDiffKeepOtherFromRule(outcome.keepWhich, si, gi, ri);
                updateDiffButton();
                return true;
            }

            if (outcome.action === 'deselect') {
                cancelPendingDiffJob();
                invalidateActiveDiffResult();
                clearDiffSelection(outcome.clearWhich);
                updateDiffButton();
                return true;
            }

            cancelPendingDiffJob();
            invalidateActiveDiffResult();
            clearDiffSelectionButtons(outcome.clearWhich);
            applyDiffSession(outcome.session);
            refreshVirtualRowsNow();
            var btn = byId(diffSideButtonId(side, si, gi, ri));
            if (btn) btn.classList.add(diffSideSelectedClass(side));
            updateDiffSelectionButtonContent();

            if (outcome.shouldShowInline) {
                showDiffInline(ruleRef(si, gi, ri));
            }
            updateDiffButton();
            syncFullscreenOverlay();
            return true;
        });
    }

    function setDiffAAction(si, gi, ri, ev) {
        setDiffSideAction('a', si, gi, ri, ev);
    }

    function setDiffBAction(si, gi, ri, ev) {
        setDiffSideAction('b', si, gi, ri, ev);
    }

    function clearDiffSelectionAction(which) {
        return withDiffRenderTransition('diff-selection-clear', {
            which: which || 'both'
        }, function() {
            clearDiffSelectionButtons(which);
            applyDiffSession(TraceDiffState.clearSelection(currentDiffSession(), which).session);
            refreshAfterStandaloneDiffSelectionClear();
            return true;
        });
    }

    function closeDiffFullAction() {
        return withDiffRenderTransition('close-diff-full', {
            pairKey: activeDiffPairKey()
        }, function() {
            cancelPendingDiffJob();
            invalidateActiveDiffResult();
            var outcome = TraceDiffState.closeSession(currentDiffSession());
            var restoreViewport = outcome.restoreViewport;
            var layoutChange = null;
            if (outcome.restoreState) {
                layoutChange = applyLayoutState(outcome.restoreState);
            }
            clearDiffSelectionButtons('both');
            applyDiffSession(outcome.session);
            syncFullscreenModeWithDiff();
            layoutChange = keepFullscreenRulePathOpen(layoutChange);
            refreshLayoutChangeOrAll(layoutChange);
            restoreSearchViewportState(restoreViewport);
            rerenderAllTrees(currentSearchQuery(), true);
            updateSearchLabelHighlights(currentSearchQuery(), currentSearchScope());
            updateDiffButton();
            return true;
        });
    }

    function exitDiffKeepOtherAction(keepWhich, releasedRef) {
        return withDiffRenderTransition('exit-diff-keep-other', {
            keepWhich: keepWhich || '',
            releasedRuleKey: releasedRef ? ruleKey(releasedRef.si, releasedRef.gi, releasedRef.ri) : ''
        }, function() {
            cancelPendingDiffJob();
            invalidateActiveDiffResult();
            var outcome = TraceDiffState.exitKeepOther(currentDiffSession(), keepWhich);
            var restoreViewport = outcome.restoreViewport;
            var releasedAnchor = captureRuleViewportAnchor(releasedRef);
            var layoutChange = null;
            if (outcome.restoreState) {
                layoutChange = applyLayoutState(outcome.restoreState);
            }
            clearDiffSelectionButtons(outcome.clearWhich);
            applyDiffSession(outcome.session);
            syncFullscreenModeWithDiff();
            layoutChange = keepFullscreenRulePathOpen(layoutChange);
            refreshLayoutChangeOrAll(layoutChange);
            restoreReleasedDiffRuleViewport(restoreViewport, releasedAnchor, releasedRef);
            rerenderAllTrees(currentSearchQuery(), true);
            updateSearchLabelHighlights(currentSearchQuery(), currentSearchScope());
            return true;
        });
    }

    function showDiffInlineAction(anchorRef) {
        return withDiffRenderTransition('show-diff-inline', {
            pairKey: activeDiffPairKey()
        }, function() {
            if (!diffState().diffA || !diffState().diffB) return false;
            if (!ruleSupportsDiff(diffState().diffA) || !ruleSupportsDiff(diffState().diffB)) return false;
            cancelPendingHighlights();
            var pairKey = activeDiffPairKey();
            var cached = cachedDiffResultForPair(pairKey);
            var startDiff = TraceDiffState.beginInline(
                currentDiffSession(),
                diffState().savedDiffState ? null : saveLayoutState(),
                diffState().savedDiffState ? null : saveSearchViewportState()
            );
            applyDiffSession(startDiff.session);
            var fullscreen = isFullscreenOpen();
            var anchor = fullscreen ? null : captureRuleViewportAnchor(anchorRef);

            var layoutChange = collapseLayout();
            var searchOverlay = activeSearchExpandOverlay();
            if (searchOverlay) {
                clearSearchExpandOverlay();
                layoutChange = TraceState.mergeTransitionSummary(
                    layoutChange,
                    searchExpandOverlayTransitionSummary(searchOverlay, null)
                );
                searchState().searchLayoutApplied = false;
            }

            var targets = [diffState().diffA, diffState().diffB];
            for (var t = 0; t < 2; t++) {
                var d = targets[t];
                layoutChange = TraceState.mergeTransitionSummary(
                    layoutChange,
                    openRulePath(d.si, d.gi, d.ri)
                );
            }
            if (cached) {
                syncFullscreenModeWithDiff();
            } else if (fullscreen) {
                TraceState.syncFullscreenMode(uiState(), 'rule');
            }
            if (layoutChange.changed) {
                refreshLayoutTransitionDisplays(layoutChange);
            } else {
                updateAllDisplays();
            }
            if (!fullscreen) {
                restoreDiffActivationViewport(anchor);
            }

            if (cached) {
                renderDiffInlineTrees(cached);
                rerenderActiveFullscreenDiff(cached);
            } else {
                beginPendingDiffJob(pairKey);
                renderPendingDiffPlainTrees();
                if (fullscreen) syncFullscreenOverlay();
            }

            updateDiffButton();
            return true;
        });
    }

    function scheduleInputSearchAction() {
        var search = searchState();
        var now = Date.now ? Date.now() : 0;
        var startedAt = search.searchInputDebounceStartedAt || now;
        var elapsed = Math.max(0, now - startedAt);
        var delay = Math.max(0, Math.min(
            SEARCH_INPUT_DEBOUNCE_MS,
            SEARCH_INPUT_MAX_WAIT_MS - elapsed
        ));
        var token = runtimeToken();

        cancelScheduledSearch();
        search.searchInputDebounceStartedAt = startedAt;
        search.searchFrameCancel = clearTimer;
        search.searchFrame = scheduleTimeout(function() {
            search.searchFrame = null;
            search.searchFrameCancel = null;
            search.searchInputDebounceStartedAt = 0;
            if (!runtimeTokenCurrent(token)) return;
            doSearch();
            scheduleAutoGlobalSearchAction('input-settled');
        }, delay);
    }

    function autoGlobalSearchEligible(intent) {
        intent = intent || currentSearchInputIntent();
        if (!intent.query || !searchScopeUsesBodyPayloads(intent.scope)) return false;
        if (peekGlobalSearchSummary(intent.query, intent.scope)) return false;
        if (lazySearchIndexComplete(intent.query, intent.scope)) return false;
        return true;
    }

    function autoGlobalSearchCurrent(intent) {
        if (!autoGlobalSearchEligible(intent)) return false;
        if (searchState().searchDirty || searchState().searchFrame !== null) return false;
        if (!searchIntentsEqual(currentSearchInputIntent(), intent)) return false;
        return searchResultMatchesIntent(intent.query, intent.scope, intent.mode);
    }

    function startAutoGlobalSearchAction(intent) {
        intent = normalizeSearchIntent(
            intent && intent.query,
            intent && intent.scope,
            intent && intent.mode
        );
        if (!autoGlobalSearchCurrent(intent)) return false;

        var layers = searchState().searchLayers || {};
        var globalJob = layers.globalJob || {};
        if (globalJob.activeJob) return false;

        var run = startGlobalSearchSummaryJob(intent.query, intent.scope, {
            kind: 'auto-global-summary'
        });
        updateSearchStatus(searchState().searchMatches.length);
        if (!run) return false;

        run.promise.then(function(summary) {
            if (searchIntentsEqual(currentSearchInputIntent(), intent)) {
                applyGlobalSummaryToCurrentSearch(intent.query, intent.scope, summary);
                updateSearchStatus(searchState().searchMatches.length);
            }
        }, function(error) {
            if (typeof console !== 'undefined' && console && console.warn) {
                console.warn('Auto global search failed', error);
            }
            if (searchIntentsEqual(currentSearchInputIntent(), intent)) {
                updateSearchStatus(searchState().searchMatches.length);
            }
        });
        return true;
    }

    function scheduleAutoGlobalSearchAction(reason) {
        var intent = currentSearchInputIntent();
        cancelAutoGlobalSearchTimer(reason || 'auto-global-rescheduled');
        if (!autoGlobalSearchEligible(intent)) return false;

        var token = runtimeToken();
        var timer = scheduleTimeout(function() {
            var search = searchState();
            if (search.autoGlobalSearchTimer !== timer) return;
            search.autoGlobalSearchTimer = null;
            search.autoGlobalSearchTimerCancel = null;
            search.autoGlobalSearchIntent = null;
            if (!runtimeTokenCurrent(token)) return;
            startAutoGlobalSearchAction(intent);
        }, SEARCH_AUTO_GLOBAL_DEBOUNCE_MS);
        setAutoGlobalSearchTimer(timer, clearTimer, intent);
        return true;
    }

    function scheduleSearchAction(ev) {
        if (ev && ev.isComposing) return;

        searchState().searchDirty = true;
        searchState().searchRunReason = ev && ev.type === 'input' ? 'input' : 'scheduled';
        markPendingSearchIntentFromControls();
        updateSearchStatus(searchState().searchMatches.length);
        cancelAutoGlobalSearchTimer('pending-search-input');
        if (ev && ev.type === 'input') {
            cancelSearchExecutionPastPreview('pending-search-input');
            cancelGlobalSearchSummaryJob('pending-search-input');
            scheduleInputSearchAction();
            return;
        }
        searchState().searchInputDebounceStartedAt = 0;
        if (searchState().searchFrame !== null) return;

        var token = runtimeToken();
        searchState().searchFrameCancel = actionCtx.frame.cancel;
        searchState().searchFrame = actionCtx.frame.request(function() {
            searchState().searchFrame = null;
            searchState().searchFrameCancel = null;
            if (!runtimeTokenCurrent(token)) return;
            doSearch();
            scheduleAutoGlobalSearchAction('scheduled-search-settled');
        });
    }

    function runSearchNowAction(force) {
        if (!force && !searchState().searchDirty && searchState().searchFrame === null) return;
        searchState().searchRunReason = force ? 'explicit' : 'manual';
        cancelScheduledSearch();
        searchState().searchInputDebounceStartedAt = 0;
        doSearch();
    }

    function startSearchNavRepeatAction(direction, ev) {
        if (ev && ev.button !== undefined && ev.button !== 0) return;
        consumeEvent(ev);
        if (ev && ev.currentTarget && ev.currentTarget.disabled) return;

        stopSearchNavRepeat();

        var state = {
            direction: direction,
            pointerId: ev && ev.pointerId
        };
        searchState().searchNavRepeatState = state;

        navigateSearchMatch(direction, ev);
        state.delayTimer = scheduleTimeout(function() {
            if (searchState().searchNavRepeatState !== state) return;
            state.intervalTimer = scheduleInterval(function() {
                navigateSearchMatch(direction);
            }, SEARCH_NAV_REPEAT_INTERVAL_MS);
        }, SEARCH_NAV_REPEAT_DELAY_MS);

        if (ev && ev.target && ev.target.setPointerCapture && ev.pointerId !== undefined) {
            try { ev.target.setPointerCapture(ev.pointerId); } catch (err) {}
        }
        addWindowListener('pointerup', stopSearchNavRepeat);
        addWindowListener('pointercancel', stopSearchNavRepeat);
        addWindowListener('blur', stopSearchNavRepeat);
    }

    function handleSearchNavButtonClickAction(direction, ev) {
        consumeEvent(ev);
        if (shouldSuppressSearchNavClick(direction)) return;
        navigateSearchMatch(direction, ev);
    }

    function navigateSearchMatchAction(direction, ev, forceRefresh) {
        consumeEvent(ev);

        var globalSummary = currentGlobalSearchSummaryForStatus();
        runSearchNow(!!forceRefresh && !currentSearchResultReadyForNavigation() && !globalSummary);
        refreshSearchMatches(true);

        if (globalSummary && searchScopeUsesBodyPayloads(currentSearchScope())) {
            return navigateGlobalSearchSummaryMatch(direction, globalSummary, {
                preserveLayout: true,
                preserveViewport: false
            });
        }

        if (!currentSearchResultReadyForNavigation()) {
            if (globalSummary) {
                return navigateGlobalSearchSummaryMatch(direction, globalSummary, {
                    preserveLayout: true,
                    preserveViewport: false
                });
            }
            clearPendingSearchActivation();
            updateSearchStatus(searchState().searchMatches.length);
            return false;
        }

        var count = searchState().searchMatches.length;
        if (!count) {
            return startLazyFindNextNavigation(direction);
        }

        if (startLazyFindNextNavigation(direction)) return true;

        var index = searchNavigationIndex(direction, count);

        setActiveSearchMatch(index, {
            preserveLayout: true,
            preserveViewport: false
        });
        return true;
    }

    function expandSearchMatchesAction(ev) {
        consumeEvent(ev);
        closeSettingsPopover();
        cancelAutoGlobalSearchTimer('expand-search');

        var layers = searchState().searchLayers || {};
        var globalJob = layers.globalJob || {};
        if (globalJob.activeJob) {
            if (globalJob.activeJob.kind !== 'expand') {
                updateSearchStatus(searchState().searchMatches.length);
                return false;
            }
            cancelGlobalSearchSummaryJob('user-cancelled');
            updateSearchStatus(searchState().searchMatches.length);
            return false;
        }

        var intent = currentSearchInputIntent();
        var globalSummary = currentGlobalSearchSummaryForStatus();
        var bodyScope = searchScopeUsesBodyPayloads(intent.scope);
        var readyForNavigation = currentSearchResultReadyForNavigation();
        var resultState = searchState().searchResultState;
        var completeForExpand = readyForNavigation &&
            !(bodyScope && resultState === 'partial-visible');
        if (completeForExpand || !bodyScope) {
            runSearchNow(!completeForExpand && !globalSummary);
        }
        refreshSearchMatches(true);

        readyForNavigation = currentSearchResultReadyForNavigation();
        resultState = searchState().searchResultState;
        completeForExpand = readyForNavigation &&
            !(bodyScope && resultState === 'partial-visible');
        if (!completeForExpand) {
            if (globalSummary) {
                var summaryMatches = searchMatchesFromGlobalSummary(globalSummary);
                if (!summaryMatches.length) return false;
                return commitSearchMatchesToLayout(
                    intent.query,
                    intent.scope,
                    summaryMatches,
                    {
                        sparseTransition: true,
                        viewportState: saveSearchViewportState(),
                        viewportAnchor: captureTraceViewportAnchor()
                    }
                );
            }
            if (!intent.query || !bodyScope) {
                clearPendingSearchActivation();
                updateSearchStatus(searchState().searchMatches.length);
                return false;
            }
            var run = startGlobalSearchSummaryJob(intent.query, intent.scope, {
                kind: 'expand'
            });
            updateSearchStatus(searchState().searchMatches.length);
            if (!run) return false;
            run.promise.then(function(summary) {
                if (!summary) {
                    updateSearchStatus(searchState().searchMatches.length);
                    return;
                }
                var matches = searchMatchesFromGlobalSummary(summary);
                if (!matches.length) {
                    updateSearchStatus(searchState().searchMatches.length);
                    return;
                }
                commitSearchMatchesToLayout(
                    intent.query,
                    intent.scope,
                    matches,
                    {
                        sparseTransition: true,
                        viewportState: saveSearchViewportState(),
                        viewportAnchor: captureTraceViewportAnchor()
                    }
                );
                updateSearchStatus(searchState().searchMatches.length);
            }, function(error) {
                if (typeof console !== 'undefined' && console && console.warn) {
                    console.warn('Global expand search failed', error);
                }
                updateSearchStatus(searchState().searchMatches.length);
            });
            return true;
        }

        var matches = collectSearchMatchesForExpansion(
            intent.query,
            intent.scope,
            intent.mode
        );
        if (!matches.length) return false;

        return commitSearchMatchesToLayout(
            intent.query,
            intent.scope,
            matches,
            {
                sparseTransition: true,
                viewportState: saveSearchViewportState(),
                viewportAnchor: captureTraceViewportAnchor()
            }
        );
    }

    function runGlobalSearchAction(ev) {
        consumeEvent(ev);
        closeSettingsPopover();
        cancelAutoGlobalSearchTimer('manual-global-search');

        var layers = searchState().searchLayers || {};
        var globalJob = layers.globalJob || {};
        if (globalJob.activeJob) {
            if (globalJob.activeJob.kind === 'expand') {
                updateSearchStatus(searchState().searchMatches.length);
                return false;
            }
            cancelGlobalSearchSummaryJob('user-cancelled');
            updateSearchStatus(searchState().searchMatches.length);
            return false;
        }

        var intent = currentSearchInputIntent();
        if (!intent.query) {
            updateSearchStatus(searchState().searchMatches.length);
            return false;
        }

        runSearchNow(false);
        refreshSearchMatches(true);

        var run = startGlobalSearchSummaryJob(intent.query, intent.scope);
        updateSearchStatus(searchState().searchMatches.length);
        if (!run) return false;

        run.promise.then(function(summary) {
            applyGlobalSummaryToCurrentSearch(intent.query, intent.scope, summary);
            updateSearchStatus(searchState().searchMatches.length);
        }, function(error) {
            if (typeof console !== 'undefined' && console && console.warn) {
                console.warn('Global search failed', error);
            }
            updateSearchStatus(searchState().searchMatches.length);
        });
        return true;
    }

    function clearSearchInputAction(ev) {
        consumeEvent(ev);

        closeSettingsPopover();
        cancelAutoGlobalSearchTimer('clear-search');

        var box = byId('search-box');
        if (box) {
            box.value = '';
            if (box.focus) box.focus();
        }

        cancelGlobalSearchSummaryJob('clear-search');
        exitSearch();
    }

    function handleSearchBoxKeydownAction(ev) {
        if (!ev || ev.isComposing) return;
        if (ev.key === 'Escape') {
            clearSearchInput(ev);
            closeMobileSearchAction(null);
        } else if (ev.key === 'Enter') {
            navigateSearchMatchAction(ev.shiftKey ? -1 : 1, ev, !ev.repeat);
        }
    }

    function controlsElement() {
        return typeof document !== 'undefined' && document.querySelector
            ? document.querySelector('.controls')
            : null;
    }

    function openMobileSearchAction(ev) {
        consumeEvent(ev);
        var controls = controlsElement();
        if (!controls) return;
        controls.classList.add('search-expanded');
        var box = byId('search-box');
        if (box && box.focus) box.focus();
    }

    function closeMobileSearchAction(ev) {
        if (ev) consumeEvent(ev);
        var controls = controlsElement();
        if (!controls || !controls.classList.contains('search-expanded')) return;
        controls.classList.remove('search-expanded');
        var icon = byId('search-icon-button');
        if (icon && icon.focus) icon.focus();
    }

    function doSearchAction() {
        var timingQuery = currentSearchQuery();
        var timingScope = currentSearchScope();
        var timingMode = currentSearchMode();
        var runReason = searchState().searchRunReason || '';
        searchState().searchRunReason = '';
        return withSearchMarksTransition('search-query-change', {
            query: timingQuery,
            scope: timingScope,
            mode: timingMode
        }, function() {
            var box = byId('search-box');
            var query = box ? box.value.trim() : '';
            var scope = currentSearchScope();
            var mode = currentSearchMode();

            beginSearchTimingRun(query, scope, mode);
            var tx = searchState().activeSearchTransaction;
            var searchRun = {
                empty: !query,
                runReason: runReason,
                state: null,
                collectedSearch: null,
                indexedMatches: [],
                transactionToken: null,
                partialKind: '',
                collapsedIndicatorsApplied: false
            };

            var domPhase = {
                phase: 'dom',
                label: 'search-query-dom',
                surfaces: ['search-marks', 'trace-canvas', 'trace-anchor-line'],
                run: function() {
                    if (searchRun.empty) return exitSearch();
                    if (!searchTransactionTokenCurrent(searchRun.transactionToken, { payload: true })) {
                        return false;
                    }

                    clearStaleSearchMarks(query, scope, {
                        searchTransactionToken: searchRun.transactionToken
                    });
                    var state = searchRun.state || uiState();
                    var indexedMatches = searchRun.indexedMatches || [];
                    var resultOwner = createSearchTransactionResultOwner(
                        searchRun.transactionToken,
                        'search-query-results',
                        { currentness: { payload: true } }
                    );
                    if (!resultOwner) return false;
                    searchRun.previousExpandOverlay = activeSearchExpandOverlay();
                    state.searchToken++;
                    var resultState = searchRun.partialKind === 'preview' ? 'partial-visible' : 'ready';
                    if (!resultOwner.applyResults({
                            query: query,
                            scope: scope,
                            mode: mode,
                            state: resultState,
                            matches: indexedMatches,
                            expandableMatchCount: searchRun.collectedSearch &&
                                searchRun.collectedSearch.expandableMatchCount,
                            collapsedIndicators: searchRun.collectedSearch &&
                                searchRun.collectedSearch.collapsedIndicators,
                            preserveActive: true,
                            deferStatus: true
                    })) return false;
                    if (searchRun.collectedSearch &&
                            searchRun.collectedSearch.collapsedIndicators) {
                        searchRun.collapsedIndicatorsApplied = true;
                    }

                    if (!searchRun.partialKind && searchRun.previousExpandOverlay) {
                        clearSearchExpandOverlay();
                        searchState().searchLayoutApplied = false;
                        var overlayChange = searchExpandOverlayTransitionSummary(
                            searchRun.previousExpandOverlay,
                            null
                        );
                        markTraceLayoutDirtySearchExpansion(query, scope, indexedMatches, overlayChange);
                        refreshLayoutTransitionDisplays(overlayChange);
                        searchRun.collectedSearch = collectSearchStateForCurrentSearch(query, scope);
                        searchRun.indexedMatches = searchRun.collectedSearch.matches;
                        indexedMatches = searchRun.indexedMatches || [];
                        if (!resultOwner.applyResults({
                                query: query,
                                scope: scope,
                                mode: mode,
                                state: resultState,
                                matches: indexedMatches,
                                expandableMatchCount: searchRun.collectedSearch &&
                                    searchRun.collectedSearch.expandableMatchCount,
                                collapsedIndicators: searchRun.collectedSearch &&
                                    searchRun.collectedSearch.collapsedIndicators,
                                preserveActive: true,
                                deferStatus: true
                        })) return false;
                        if (searchRun.collectedSearch &&
                                searchRun.collectedSearch.collapsedIndicators) {
                            searchRun.collapsedIndicatorsApplied = true;
                        }
                    }

                    updateSearchSelectionForVisibleMatches({ deferStatus: true });
                    searchState().lastSearchQuery = query;
                    return true;
                }
            };
            var deferredPhase = {
                phase: 'deferred',
                label: 'search-query-deferred',
                surfaces: ['search-marks', 'rule-tree', 'rule-info'],
                run: function() {
                    if (searchRun.empty) return true;
                    var state = searchRun.state || uiState();
                    var highlightJobs = searchRun.partialKind
                        ? appendSearchMatchRenderJobs([], query, scope, searchRun.indexedMatches)
                        : collectRenderJobsForCurrentState(query, false, scope);
                    scheduleSearchDecoration(query, scope, {
                        collapsedIndicators: searchRun.collectedSearch &&
                            searchRun.collectedSearch.collapsedIndicators,
                        searchMatches: searchRun.indexedMatches,
                        skipCollapsedIndicators: searchRun.collapsedIndicatorsApplied,
                        highlightJobs: highlightJobs,
                        searchToken: state.searchToken,
                        searchTransactionToken: searchRun.transactionToken,
                        syncFullscreen: !searchRun.partialKind,
                        anchorPreview: true,
                        updateStatus: true
                    });
                    return true;
                }
            };
            var modelPhase = {
                phase: 'model',
                label: 'search-query-model',
                run: function() {
                    searchState().searchDirty = false;
                    cancelPendingHighlights();

                    if (searchRun.empty) return true;

                    var state = uiState();
                    searchRun.state = state;
                    if (!state.searchActive) {
                        searchState().savedSearchState = saveLayoutState();
                        searchState().savedSearchViewport = saveSearchViewportState();
                        searchState().searchNavigationCommitted = false;
                        searchState().searchLayoutApplied = false;
                        state.searchActive = true;
                    }

                    if (searchScopeUsesBodyPayloads(scope)) {
                        var completedSummary = peekGlobalSearchSummary(query, scope);
                        if (completedSummary) {
                            searchRun.partialKind = '';
                            searchRun.collectedSearch = collectGlobalSummarySearchState(
                                query,
                                scope,
                                mode,
                                completedSummary
                            );
                            searchRun.indexedMatches = searchRun.collectedSearch.matches;
                            refreshSearchTransactionPayloadGeneration(tx);
                            searchRun.transactionToken = searchTransactionToken(tx);
                        } else {
                            searchRun.partialKind = 'preview';
                            searchRun.collectedSearch = collectVisibleSearchPreviewState(query, scope, mode);
                            searchRun.indexedMatches = searchRun.collectedSearch.matches;
                            rememberVisibleSearchLayer(query, scope, mode, searchRun.indexedMatches);
                            refreshSearchTransactionPayloadGeneration(tx);
                            refreshSearchTransactionLayoutGeneration(tx);
                            searchRun.transactionToken = searchTransactionToken(tx);
                        }
                        return true;
                    }

                    measureSearchTiming('index-readiness-build', function() {
                        ensureSearchIndex(scope);
                    }, { scope: scope });
                    refreshSearchTransactionPayloadGeneration(tx);
                    searchRun.transactionToken = searchTransactionToken(tx);

                    if (shouldChunkSearchMatchCollection(query, scope, mode)) {
                        searchRun.partialKind = 'preview';
                        searchRun.collectedSearch = collectVisibleSearchPreviewState(query, scope, mode);
                        searchRun.indexedMatches = searchRun.collectedSearch.matches;
                        refreshSearchTransactionLayoutGeneration(tx);
                        searchRun.transactionToken = searchTransactionToken(tx);
                        if (scheduleChunkedSearchStateCollection(
                            query,
                            scope,
                            mode,
                            searchRun.transactionToken,
                            function(collectedSearch) {
                                searchRun.partialKind = '';
                                searchRun.collectedSearch = collectedSearch;
                                searchRun.indexedMatches = collectedSearch.matches;
                                runSearchTransactionPhases(tx, [domPhase, deferredPhase]);
                            }
                        )) {
                            return true;
                        }
                        searchRun.partialKind = '';
                    }

                    searchRun.collectedSearch = collectSearchStateForCurrentSearch(query, scope);
                    searchRun.indexedMatches = searchRun.collectedSearch.matches;
                    return true;
                }
            };

            return runSearchTransactionPhases(tx, [modelPhase, domPhase, deferredPhase]);
        });
    }

    function cycleGlobalStateAction() {
        var state = TraceState.nextGlobalCycleState(uiState(), traceGroups());
        preserveTraceViewportAnchor(function() {
            var change = TraceState.applyGlobalCycleState(uiState(), traceGroups(), state, globalLayoutPresets);
            refreshLayoutTransitionDisplays(change);
        });
    }

    function setActiveTraceAction(index) {
        var traces = traceDataTraces();
        if (!traces.length) return;
        index = Math.max(0, Math.min(traces.length - 1, Number(index) || 0));
        saveActiveTraceIndexToSession(index);
        if (index === traceState().activeTraceIndex && traceState().activeTraceLoaded) {
            syncTracePicker();
            return;
        }

        return withTraceSwitchVisualTransition('trace-switch', {
            fromTraceIndex: traceState().activeTraceIndex,
            toTraceIndex: index
        }, function() {
            saveActiveTraceSession();
            resetTraceSwitchRuntime(index);
            loadTraceData(index);
            var session = traceSessionForIndex(index);
            restoreTraceSessionBeforeRender(session);
            renderLoadedTrace({ resetScroll: !traceSessionHasViewport(session) });
            restoreTraceSessionAfterRender(session);
            return true;
        });
    }

    function setThemeAction(theme, ev) {
        consumeEvent(ev);
        applyTheme(theme, true, isThemeDark(currentAppliedTheme()));
    }

    function setThemeDarkModeAction(dark, ev) {
        consumeEvent(ev);

        var family = themeFamilyForTheme(currentAppliedTheme());
        if (!family || (dark && !family.dark) || (!dark && !family.light)) {
            updateThemeControls(currentAppliedTheme());
            return;
        }
        applyTheme(family.id, true, !!dark);
    }

    function focusFirstIn(el) {
        if (!el) return;
        var focusable = el.querySelector('button:not([disabled]), [tabindex="0"]:not([disabled]), input:not([disabled]), select:not([disabled])');
        if (focusable && focusable.focus) focusable.focus();
    }

    function closeSettingsPopoverAction() {
        var popover = byId('settings-popover');
        var btn = byId('settings-button');
        closeNodeColumnsMenuAction();
        closeDiffFieldsMenuAction();
        if (popover && !popover.hidden) {
            var focusInPopover = popover.contains && popover.contains(document.activeElement);
            popover.hidden = true;
            if (focusInPopover && btn && btn.focus) btn.focus();
        }
        if (btn) btn.classList.remove('active');
    }

    function closeNodeColumnsMenuAction() {
        var menu = byId('node-columns-menu');
        var btn = byId('node-columns-button');
        if (menu) menu.hidden = true;
        if (btn) {
            if (btn.classList && btn.classList.remove) btn.classList.remove('active');
            if (btn.setAttribute) btn.setAttribute('aria-expanded', 'false');
        }
    }

    function closeDiffFieldsMenuAction() {
        var menu = byId('diff-fields-menu');
        var btn = byId('diff-fields-button');
        if (menu) menu.hidden = true;
        if (btn) {
            if (btn.classList && btn.classList.remove) btn.classList.remove('active');
            if (btn.setAttribute) btn.setAttribute('aria-expanded', 'false');
        }
    }

    function toggleNodeColumnsMenuAction(ev) {
        consumeEvent(ev);

        var menu = byId('node-columns-menu');
        var btn = byId('node-columns-button');
        if (!menu) return;

        var open = menu.hidden;
        if (open) closeDiffFieldsMenuAction();
        menu.hidden = !open;
        if (btn) {
            if (btn.classList && btn.classList.toggle) btn.classList.toggle('active', open);
            if (btn.setAttribute) btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        }
        if (open) focusFirstIn(menu);
    }

    function toggleDiffFieldsMenuAction(ev) {
        consumeEvent(ev);

        var menu = byId('diff-fields-menu');
        var btn = byId('diff-fields-button');
        if (!menu) return;

        var open = menu.hidden;
        if (open) closeNodeColumnsMenuAction();
        menu.hidden = !open;
        if (btn) {
            if (btn.classList && btn.classList.toggle) btn.classList.toggle('active', open);
            if (btn.setAttribute) btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        }
        if (open) focusFirstIn(menu);
    }

    function toggleSettingsPopoverAction(ev) {
        consumeEvent(ev);

        var popover = byId('settings-popover');
        var btn = byId('settings-button');
        if (!popover) return;
        var open = popover.hidden;
        closeNodeColumnsMenuAction();
        closeDiffFieldsMenuAction();
        popover.hidden = !open;
        if (open) updateBulkDetailControls();
        if (btn) btn.classList.toggle('active', open);
        if (open) focusFirstIn(popover);
    }

    function closeTopBarPopoversAction(ev) {
        if (ev && ev.target.closest &&
            (ev.target.closest('.search-wrap') || ev.target.closest('.settings-wrap'))) {
            return;
        }
        closeSettingsPopoverAction();
    }

    function treeToggleAction(nodeId, ev) {
        stopEvent(ev);
        withRuleLocalViewportStability('tree-toggle', function() {
            if (toggleTreeNodeWithMaterializer(nodeId)) {
                scheduleDiffMoveArrows();
                return;
            }

            var parsed = normalTreeSessionForNodeId(nodeId);
            if (!parsed || !parsed.session || !parsed.pid) return;
            var collapsed = !normalTreeNodeCollapsed(parsed.session, parsed.pid);
            if (!setNormalTreeNodeCollapsed(nodeId, collapsed)) return;
            rerenderNormalTreeForNodeId(nodeId);
            scheduleDiffMoveArrows();
        });
    }

    function treeMetaToggleAction(nodeId, ev) {
        stopEvent(ev);
        withRuleLocalViewportStability('tree-meta-toggle', function() {
            if (toggleTreeFieldRowsWithMaterializer(nodeId)) {
                var parsedMaterialized = normalTreeSessionForNodeId(nodeId);
                if (parsedMaterialized && parsedMaterialized.ref) {
                    updateRuleFeatureButtons(parsedMaterialized.ref.si, parsedMaterialized.ref.gi, parsedMaterialized.ref.ri);
                }
                return;
            }

            var parsed = normalTreeSessionForNodeId(nodeId);
            if (!parsed || !parsed.session || !parsed.pid) return;
            var showFields = parsed.ref
                ? effectiveRuleFeature(parsed.ref.si, parsed.ref.gi, parsed.ref.ri, 'fields')
                : !!(parsed.session && parsed.session.showFields);
            var visible = normalTreeNodeFieldRowsVisible(parsed.session, parsed.pid, showFields);
            if (!setNormalTreeFieldRowsCollapsed(nodeId, visible)) return;
            rerenderNormalTreeForNodeId(nodeId);
            if (parsed.ref) updateRuleFeatureButtons(parsed.ref.si, parsed.ref.gi, parsed.ref.ri);
        });
    }

    var layoutActions = {
        cycleGlobalState: cycleGlobalStateAction,
        onCollapsedGroupClick: onCollapsedGroupClick,
        onCollapsedRuleClick: onCollapsedRuleClick,
        toggleGroup: toggleGroup,
        toggleGroupRules: toggleGroupRules,
        toggleRule: toggleRule,
        toggleStage: toggleStage,
        toggleStageRules: toggleStageRules
    };

    var detailActions = {
        applyNodeColumnPreset: applyNodeColumnPresetAction,
        closeInfoFieldDetails: closeInfoFieldDetailsAction,
        openFieldDetails: openFieldDetailsAction,
        pinInfoFieldDetails: pinInfoFieldDetailsAction,
        resetDiffFields: resetDiffFieldsAction,
        reorderNodeColumn: reorderNodeColumnAction,
        resetNodeColumns: resetNodeColumnsAction,
        setAllDiffFields: setAllDiffFieldsAction,
        setAllNodeColumns: setAllNodeColumnsAction,
        setDiffFieldVisible: setDiffFieldVisibleAction,
        setAllRuleFeature: setAllRuleFeatureAction,
        setNodeColumnWidth: setNodeColumnWidthAction,
        setNodeColumnVisible: setNodeColumnVisibleAction,
        soloDiffField: soloDiffFieldAction,
        soloNodeColumn: soloNodeColumnAction,
        setRuleInfoTab: setRuleInfoTabAction,
        setRuleInfoSwitcher: setRuleInfoSwitcherAction,
        toggleRuleInfo: toggleRuleInfoAction,
        toggleRuleFields: toggleRuleFieldsAction,
        toggleRulePinned: toggleRulePinnedAction
    };

    var fullscreenActions = {
        closeFullscreenOnOutsideClick: closeFullscreenOnOutsideClickAction,
        closeRuleFullscreen: closeRuleFullscreenAction,
        closeRuleFullscreenFromTitle: closeRuleFullscreenFromTitleAction,
        closeRuleFullscreenToRule: closeRuleFullscreenToRuleAction,
        closeRuleFullscreenToRuleFromTitle: closeRuleFullscreenToRuleFromTitleAction,
        enterDiffFullscreen: enterDiffFullscreenAction,
        enterRuleFullscreen: enterRuleFullscreenAction,
        setFullscreenRule: setFullscreenRuleAction,
        toggleRuleFullscreen: toggleRuleFullscreenAction
    };

    var navigationActions = {
        groupNavigateNext: groupNavigateNextAction,
        groupNavigatePrev: groupNavigatePrevAction,
        handleNavButtonClick: handleNavButtonClickAction,
        navigateNext: navigateNextAction,
        navigatePrev: navigatePrevAction,
        navigateRuleDirection: navigateRuleDirectionAction,
        startFullscreenNavRepeat: startFullscreenNavRepeatAction
    };

    var diffActions = {
        clearDiffSelection: clearDiffSelectionAction,
        closeDiffFull: closeDiffFullAction,
        exitDiffKeepOther: exitDiffKeepOtherAction,
        exitDiffKeepOtherFromRule: exitDiffKeepOtherFromRuleAction,
        onDiffButtonClick: onDiffButtonClickAction,
        setDiffA: setDiffAAction,
        setDiffB: setDiffBAction,
        showDiffInline: showDiffInlineAction
    };

    var searchActions = {
        clearSearchInput: clearSearchInputAction,
        closeMobileSearch: closeMobileSearchAction,
        doSearch: doSearchAction,
        expandSearchMatches: expandSearchMatchesAction,
        handleSearchBoxKeydown: handleSearchBoxKeydownAction,
        handleSearchNavButtonClick: handleSearchNavButtonClickAction,
        navigateSearchMatch: navigateSearchMatchAction,
        openMobileSearch: openMobileSearchAction,
        runGlobalSearch: runGlobalSearchAction,
        runSearchNow: runSearchNowAction,
        scheduleSearch: scheduleSearchAction,
        startSearchNavRepeat: startSearchNavRepeatAction
    };

    var traceActions = {
        setActiveTrace: setActiveTraceAction
    };

    var themeActions = {
        setTheme: setThemeAction,
        setThemeDarkMode: setThemeDarkModeAction
    };

    var topBarActions = {
        closeSettingsPopover: closeSettingsPopoverAction,
        closeTopBarPopovers: closeTopBarPopoversAction,
        setEmptyStagesVisible: setEmptyStagesVisibleAction,
        toggleDiffFieldsMenu: toggleDiffFieldsMenuAction,
        toggleNodeColumnsMenu: toggleNodeColumnsMenuAction,
        toggleSettingsPopover: toggleSettingsPopoverAction
    };

    var treeActions = {
        toggle: treeToggleAction,
        toggleMeta: treeMetaToggleAction
    };

    return {
        detail: detailActions,
        diff: diffActions,
        fullscreen: fullscreenActions,
        layout: layoutActions,
        navigation: navigationActions,
        search: searchActions,
        theme: themeActions,
        topBar: topBarActions,
        trace: traceActions,
        tree: treeActions,

        clearDiffSelection: diffActions.clearDiffSelection,
        clearSearchInput: searchActions.clearSearchInput,
        closeDiffFull: diffActions.closeDiffFull,
        closeFullscreenOnOutsideClick: fullscreenActions.closeFullscreenOnOutsideClick,
        closeRuleFullscreen: fullscreenActions.closeRuleFullscreen,
        closeRuleFullscreenFromTitle: fullscreenActions.closeRuleFullscreenFromTitle,
        closeRuleFullscreenToRule: fullscreenActions.closeRuleFullscreenToRule,
        closeRuleFullscreenToRuleFromTitle: fullscreenActions.closeRuleFullscreenToRuleFromTitle,
        closeSettingsPopover: topBarActions.closeSettingsPopover,
        closeTopBarPopovers: topBarActions.closeTopBarPopovers,
        cycleGlobalState: layoutActions.cycleGlobalState,
        doSearch: searchActions.doSearch,
        enterDiffFullscreen: fullscreenActions.enterDiffFullscreen,
        enterRuleFullscreen: fullscreenActions.enterRuleFullscreen,
        exitDiffKeepOther: diffActions.exitDiffKeepOther,
        exitDiffKeepOtherFromRule: diffActions.exitDiffKeepOtherFromRule,
        groupNavigateNext: navigationActions.groupNavigateNext,
        groupNavigatePrev: navigationActions.groupNavigatePrev,
        handleNavButtonClick: navigationActions.handleNavButtonClick,
        handleSearchBoxKeydown: searchActions.handleSearchBoxKeydown,
        handleSearchNavButtonClick: searchActions.handleSearchNavButtonClick,
        navigateNext: navigationActions.navigateNext,
        navigatePrev: navigationActions.navigatePrev,
        navigateRuleDirection: navigationActions.navigateRuleDirection,
        navigateSearchMatch: searchActions.navigateSearchMatch,
        onDiffButtonClick: diffActions.onDiffButtonClick,
        onCollapsedGroupClick: layoutActions.onCollapsedGroupClick,
        onCollapsedRuleClick: layoutActions.onCollapsedRuleClick,
        runSearchNow: searchActions.runSearchNow,
        runGlobalSearch: searchActions.runGlobalSearch,
        scheduleSearch: searchActions.scheduleSearch,
        setAllRuleFeature: detailActions.setAllRuleFeature,
        setActiveTrace: traceActions.setActiveTrace,
        setDiffA: diffActions.setDiffA,
        setDiffB: diffActions.setDiffB,
        setEmptyStagesVisible: topBarActions.setEmptyStagesVisible,
        setFullscreenRule: fullscreenActions.setFullscreenRule,
        setRuleInfoTab: detailActions.setRuleInfoTab,
        setRuleInfoSwitcher: detailActions.setRuleInfoSwitcher,
        setTheme: themeActions.setTheme,
        setThemeDarkMode: themeActions.setThemeDarkMode,
        showDiffInline: diffActions.showDiffInline,
        startFullscreenNavRepeat: navigationActions.startFullscreenNavRepeat,
        startSearchNavRepeat: searchActions.startSearchNavRepeat,
        toggleGroup: layoutActions.toggleGroup,
        toggleGroupRules: layoutActions.toggleGroupRules,
        toggleRule: layoutActions.toggleRule,
        toggleRuleFullscreen: fullscreenActions.toggleRuleFullscreen,
        toggleRuleInfo: detailActions.toggleRuleInfo,
        toggleRuleFields: detailActions.toggleRuleFields,
        toggleRulePinned: detailActions.toggleRulePinned,
        toggleSettingsPopover: topBarActions.toggleSettingsPopover,
        toggleStage: layoutActions.toggleStage,
        toggleStageRules: layoutActions.toggleStageRules,
        treeToggle: treeActions.toggle,
        treeMetaToggle: treeActions.toggleMeta
    };
}

var TraceActions = createTraceActions(TraceActionContext);
