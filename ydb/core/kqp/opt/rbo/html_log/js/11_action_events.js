var TraceActionEvents = (function() {
    var collapsedLabelLensSource = null;
    var collapsedLabelLensEl = null;
    var INFO_TARGET_CONTROL_HOVER_DELAY_MS = 120;
    var INFO_TARGET_PREVIEW_APPLY_HOLD_MS = 480;
    var INFO_TARGET_PREVIEW_APPLIED_FLASH_MS = TRACE_NAV_BLINK_MS;
    var INFO_TARGET_PREVIEW_HOLD_MOVE_PX = 6;
    var COLLAPSED_RULE_LENS_ANCHOR_Y = 16;
    var COLLAPSED_STAGE_LENS_ANCHOR_Y = 17;
    var delayedInfoTargetHover = null;
    var infoTargetPreviewHold = null;
    var suppressedInfoTargetPreviewClick = null;

    function collapsedLabelSource(target) {
        if (!target || !target.closest) return null;
        return target.closest('.stage-col, .group-cell.g-collapsed, .rule-cell.collapsed');
    }

    function collapsedLabelTextEl(source) {
        if (!source || !source.querySelector) return null;
        if (source.classList && source.classList.contains('stage-col')) {
            return source.querySelector('.stage-col-text[data-search-label]');
        }
        if (source.classList && source.classList.contains('g-collapsed')) {
            return source.querySelector('.group-col-text[data-search-label]');
        }
        if (source.classList && source.classList.contains('collapsed')) {
            return source.querySelector('.rule-col-text[data-search-label]');
        }
        return null;
    }

    function collapsedLabelText(source) {
        var label = collapsedLabelTextEl(source);
        if (!label || !label.getAttribute) return '';
        return label.getAttribute('data-search-label') || label.textContent || '';
    }

    function ensureCollapsedLabelLens(traceEl) {
        if (!traceEl || !document.createElement) return null;

        var overlayParent = document.body || traceEl;
        var overlay = document.querySelector ?
            document.querySelector('.collapsed-label-overlay') :
            null;
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.className = 'collapsed-label-overlay';
            overlay.setAttribute('aria-hidden', 'true');
            overlayParent.appendChild(overlay);
        } else if (overlay.parentNode !== overlayParent) {
            overlayParent.appendChild(overlay);
        }

        if (!collapsedLabelLensEl || !collapsedLabelLensEl.parentNode) {
            collapsedLabelLensEl = document.createElement('div');
            collapsedLabelLensEl.className = 'collapsed-label-lens';
            collapsedLabelLensEl.hidden = true;
            collapsedLabelLensEl.appendChild(document.createElement('span'));
            overlay.appendChild(collapsedLabelLensEl);
        } else if (collapsedLabelLensEl.parentNode !== overlay) {
            overlay.appendChild(collapsedLabelLensEl);
        }
        if (!collapsedLabelLensEl.firstElementChild) {
            collapsedLabelLensEl.appendChild(document.createElement('span'));
        }

        return collapsedLabelLensEl;
    }

    function clampNumber(value, min, max) {
        if (max < min) return min;
        return Math.max(min, Math.min(max, value));
    }

    function collapsedLabelAnchorY(source, sourceRect) {
        if (source.classList && source.classList.contains('stage-col')) {
            return sourceRect.top + COLLAPSED_STAGE_LENS_ANCHOR_Y;
        }
        if (source.classList &&
            (source.classList.contains('collapsed') || source.classList.contains('g-collapsed'))) {
            return sourceRect.top + COLLAPSED_RULE_LENS_ANCHOR_Y;
        }
        return sourceRect.top + Math.min(32, (sourceRect.height || 0) / 4);
    }

    function positionCollapsedLabelLens(source, lens, traceEl) {
        if (!source || !lens || !traceEl ||
            !source.getBoundingClientRect || !traceEl.getBoundingClientRect ||
            !lens.getBoundingClientRect) {
            return false;
        }

        var sourceRects = source.getClientRects ? source.getClientRects() : [];
        if (!sourceRects || !sourceRects.length) return false;

        var sourceRect = source.getBoundingClientRect();
        var traceRect = traceEl.getBoundingClientRect();
        var gap = 10;

        lens.style.transform = 'translate(-9999px, -9999px)';
        lens.hidden = false;
        var lensRect = lens.getBoundingClientRect();
        var lensWidth = Math.ceil(lensRect.width || 0);
        var lensHeight = Math.ceil(lensRect.height || 0);

        var sourceLeft = sourceRect.left;
        var sourceRight = sourceRect.right;
        var anchorY = collapsedLabelAnchorY(source, sourceRect);
        var viewportLeft = traceRect.left + gap;
        var viewportRight = traceRect.right - gap;
        var viewportTop = gap;
        var viewportHeight = (typeof window !== 'undefined' && window.innerHeight) ||
            (document.documentElement && document.documentElement.clientHeight) ||
            traceRect.bottom;
        var viewportBottom = viewportHeight - gap;

        var left = sourceRight + gap;
        if (left + lensWidth > viewportRight) left = sourceLeft - lensWidth - gap;
        left = clampNumber(left, viewportLeft, viewportRight - lensWidth);

        var top = anchorY - lensHeight / 2;
        top = clampNumber(top, viewportTop, viewportBottom - lensHeight);

        lens.style.transform = 'translate(' + Math.round(left) + 'px, ' + Math.round(top) + 'px)';
        return true;
    }

    function showCollapsedLabelLens(source) {
        if (!source) {
            hideCollapsedLabelLens();
            return;
        }

        var text = collapsedLabelText(source);
        if (!text) {
            hideCollapsedLabelLens();
            return;
        }

        var traceEl = source.closest ? source.closest('.trace') : document.querySelector('.trace');
        var lens = ensureCollapsedLabelLens(traceEl);
        if (!lens) return;

        collapsedLabelLensSource = source;
        if (lens.firstElementChild) {
            lens.firstElementChild.textContent = text;
        } else {
            lens.textContent = text;
        }
        lens.setAttribute('title', text);
        if (!positionCollapsedLabelLens(source, lens, traceEl)) hideCollapsedLabelLens();
    }

    function showCollapsedLabelLensFromTarget(target) {
        var source = collapsedLabelSource(target);
        if (source) showCollapsedLabelLens(source);
    }

    function scheduleFocusedCollapsedLabelLens(source) {
        if (!source || typeof scheduleRenderDomWork !== 'function') return;
        var run = function(visualCtx) {
            if (source.isConnected === false) return;
            if (!document.activeElement || !source.contains(document.activeElement)) return;
            showCollapsedLabelLens(source);
        };

        scheduleRenderDomWork('focused-collapsed-label-lens', function(visualCtx) {
            return run(visualCtx);
        }, {
            epochScopes: ['trace', 'render', 'virtual', 'search'],
            label: 'focused-collapsed-label-lens',
            surfaces: ['collapsed-label-lens']
        });
    }

    function hideCollapsedLabelLens(source) {
        if (source && source !== collapsedLabelLensSource) return;
        if (collapsedLabelLensEl) collapsedLabelLensEl.hidden = true;
        collapsedLabelLensSource = null;
    }

    function interactiveInfoTargetHost(target) {
        if (!target || !target.closest) return null;
        var host = target.closest(
            '[data-trace-action][data-info-targets], [data-trace-action][data-info-primary-target]'
        );
        if (!host || !host.getAttribute) return null;
        return host.getAttribute('data-trace-action') === 'set-info-tab' ? host : null;
    }

    function infoTargetPreviewHoldHost(target) {
        if (!target || !target.closest) return null;
        var host = target.closest(
            '[data-trace-action][data-info-targets], [data-trace-action][data-info-primary-target]'
        );
        if (!host || !host.getAttribute) return null;
        if (host.getAttribute('data-info-target-hover-mode') === 'highlight') return null;
        var action = host.getAttribute('data-trace-action') || '';
        return action === 'activate-info-target' ? null : host;
    }

    function cancelDelayedInfoTargetHover() {
        if (!delayedInfoTargetHover) return false;
        clearTimeout(delayedInfoTargetHover.timer);
        delayedInfoTargetHover = null;
        return true;
    }

    function cancelDelayedInfoTargetHoverForTarget(target, relatedTarget) {
        var host = interactiveInfoTargetHost(target);
        if (!host) return false;
        if (relatedTarget && host.contains && host.contains(relatedTarget)) return false;
        return cancelDelayedInfoTargetHover();
    }

    function cancelDelayedInfoTargetHoverForHost(host) {
        if (!delayedInfoTargetHover || delayedInfoTargetHover.host !== host) return false;
        return cancelDelayedInfoTargetHover();
    }

    function scheduleInteractiveInfoTargetHover(host) {
        if (!host) return false;
        if (delayedInfoTargetHover && delayedInfoTargetHover.host === host) return true;
        cancelDelayedInfoTargetHover();

        var state = { host: host, timer: null };
        state.timer = scheduleRuntimeTimeout(function() {
            if (delayedInfoTargetHover !== state) return;
            delayedInfoTargetHover = null;
            if (host.isConnected === false) return;
            InfoTargetController.handleInfoTargetMouseOver(host);
        }, INFO_TARGET_CONTROL_HOVER_DELAY_MS, {
            epochScopes: ['trace', 'render', 'virtual'],
            label: 'interactive-info-target-hover'
        });
        delayedInfoTargetHover = state;
        return true;
    }

    function clearInfoTargetPreviewAppliedTimer(host) {
        if (!host || host.__infoTargetPreviewAppliedTimer === undefined ||
            host.__infoTargetPreviewAppliedTimer === null) {
            return false;
        }
        clearTimeout(host.__infoTargetPreviewAppliedTimer);
        host.__infoTargetPreviewAppliedTimer = null;
        return true;
    }

    function setInfoTargetPreviewHoldVisual(state) {
        var host = state && state.host;
        if (!host || !host.classList) return false;
        if (!InfoTargetController.infoTargetPreviewRestores ||
            !InfoTargetController.infoTargetPreviewRestores(host)) {
            return false;
        }
        state.visualShown = true;
        clearInfoTargetPreviewAppliedTimer(host);
        host.classList.remove('info-target-preview-applied');
        host.classList.add('info-target-preview-hold');
        if (host.style && host.style.setProperty) {
            host.style.setProperty('--info-target-preview-hold-ms', INFO_TARGET_PREVIEW_APPLY_HOLD_MS + 'ms');
        }
        return true;
    }

    function clearInfoTargetPreviewHoldVisual(state) {
        var host = state && state.host;
        if (!host) return false;
        if (host.classList) host.classList.remove('info-target-preview-hold');
        if (host.style && host.style.removeProperty) {
            host.style.removeProperty('--info-target-preview-hold-ms');
        }
        return true;
    }

    function flashInfoTargetPreviewApplied(host) {
        if (!host || !host.classList) return false;
        clearInfoTargetPreviewAppliedTimer(host);
        host.classList.add('info-target-preview-applied');
        host.__infoTargetPreviewAppliedTimer = scheduleRuntimeTimeout(function() {
            if (host.classList) host.classList.remove('info-target-preview-applied');
            host.__infoTargetPreviewAppliedTimer = null;
        }, INFO_TARGET_PREVIEW_APPLIED_FLASH_MS, {
            epochScopes: ['trace', 'render', 'virtual'],
            label: 'info-target-preview-applied-flash'
        });
        return true;
    }

    function clearInfoTargetPreviewHold(options) {
        options = options || {};
        if (!infoTargetPreviewHold) return false;
        var host = infoTargetPreviewHold.host;
        if (infoTargetPreviewHold.timer !== null) {
            clearTimeout(infoTargetPreviewHold.timer);
        }
        if (infoTargetPreviewHold.visualProbeTimer !== null) {
            clearTimeout(infoTargetPreviewHold.visualProbeTimer);
        }
        cancelDelayedInfoTargetHoverForHost(host);
        clearInfoTargetPreviewHoldVisual(infoTargetPreviewHold);
        if (!options.keepSuppressedClick &&
            suppressedInfoTargetPreviewClick &&
            suppressedInfoTargetPreviewClick.host === host) {
            suppressedInfoTargetPreviewClick = null;
        }
        infoTargetPreviewHold = null;
        return true;
    }

    function startInfoTargetPreviewHold(ev) {
        var host = infoTargetPreviewHoldHost(ev && ev.target);
        if (!host) return false;
        if (ev.button !== undefined && ev.button !== 0) return false;

        clearInfoTargetPreviewHold();
        var state = {
            host: host,
            pointerId: ev.pointerId,
            startX: Number(ev.clientX) || 0,
            startY: Number(ev.clientY) || 0,
            timer: null,
            visualProbeTimer: null,
            applied: false,
            visualShown: false
        };
        var probeVisual = function() {
            if (infoTargetPreviewHold !== state || state.applied || state.visualShown) return;
            setInfoTargetPreviewHoldVisual(state);
        };
        state.timer = scheduleRuntimeTimeout(function() {
            if (infoTargetPreviewHold !== state) return;
            state.timer = null;
            if (state.visualProbeTimer !== null) {
                clearTimeout(state.visualProbeTimer);
                state.visualProbeTimer = null;
            }
            if (host.isConnected === false) {
                clearInfoTargetPreviewHoldVisual(state);
                return;
            }
            var previewMoved = InfoTargetController.infoTargetPreviewRestores &&
                InfoTargetController.infoTargetPreviewRestores(host);
            if (!InfoTargetController.commitInfoTargetPreview(host)) {
                clearInfoTargetPreviewHoldVisual(state);
                return;
            }
            state.applied = true;
            clearInfoTargetPreviewHoldVisual(state);
            if (state.visualShown || previewMoved) flashInfoTargetPreviewApplied(host);
            suppressedInfoTargetPreviewClick = { host: host };
            applyInfoTargetPreviewHoldAction(host);
            InfoTargetController.clearTargetHighlights();
        }, INFO_TARGET_PREVIEW_APPLY_HOLD_MS, {
            epochScopes: ['trace', 'render', 'virtual'],
            label: 'info-target-preview-apply-hold'
        });
        state.visualProbeTimer = scheduleRuntimeTimeout(function() {
            if (infoTargetPreviewHold !== state) return;
            state.visualProbeTimer = null;
            probeVisual();
        }, INFO_TARGET_CONTROL_HOVER_DELAY_MS + 16, {
            epochScopes: ['trace', 'render', 'virtual'],
            label: 'info-target-preview-hold-visual-probe'
        });
        infoTargetPreviewHold = state;
        probeVisual();
        return true;
    }

    function consumeSuppressedInfoTargetPreviewClick(el, ev) {
        if (!suppressedInfoTargetPreviewClick) return false;
        var host = suppressedInfoTargetPreviewClick.host;
        var matched = el === host ||
            (host && host.contains && host.contains(el)) ||
            (el && el.contains && el.contains(host));
        suppressedInfoTargetPreviewClick = null;
        if (!matched) return false;
        if (ev && ev.preventDefault) ev.preventDefault();
        if (ev && ev.stopPropagation) ev.stopPropagation();
        return true;
    }

    function handleInfoTargetPreviewHoldPointerMove(ev) {
        if (!infoTargetPreviewHold) return;
        if (ev.pointerId !== undefined && ev.pointerId !== infoTargetPreviewHold.pointerId) return;
        var dx = (Number(ev.clientX) || 0) - infoTargetPreviewHold.startX;
        var dy = (Number(ev.clientY) || 0) - infoTargetPreviewHold.startY;
        if (Math.sqrt(dx * dx + dy * dy) > INFO_TARGET_PREVIEW_HOLD_MOVE_PX) {
            if (infoTargetPreviewHold.applied) return;
            clearInfoTargetPreviewHold();
        }
    }

    function handleInfoTargetPreviewHoldPointerUp(ev) {
        if (!infoTargetPreviewHold) return;
        if (ev.pointerId !== undefined && ev.pointerId !== infoTargetPreviewHold.pointerId) return;
        var applied = !!infoTargetPreviewHold.applied;
        if (applied) {
            if (ev.preventDefault) ev.preventDefault();
            if (ev.stopPropagation) ev.stopPropagation();
        }
        clearInfoTargetPreviewHold({ keepSuppressedClick: applied });
    }

    function handleInfoTargetPreviewHoldPointerCancel(ev) {
        if (!infoTargetPreviewHold) return;
        if (ev.pointerId !== undefined && ev.pointerId !== infoTargetPreviewHold.pointerId) return;
        clearInfoTargetPreviewHold();
    }

    function handleMouseOver(ev) {
        showCollapsedLabelLensFromTarget(ev.target);
        InfoTargetController.handleTraceRefMouseOver(ev.target);
        if (scheduleInteractiveInfoTargetHover(interactiveInfoTargetHost(ev.target))) return;
        InfoTargetController.handleInfoTargetMouseOver(ev.target);
    }

    function handleMouseOut(ev) {
        var labelSource = collapsedLabelSource(ev.target);
        if (labelSource) {
            var labelRelated = ev.relatedTarget;
            if (!labelRelated || !labelSource.contains(labelRelated)) {
                hideCollapsedLabelLens(labelSource);
            }
        }

        cancelDelayedInfoTargetHoverForTarget(ev.target, ev.relatedTarget);
        InfoTargetController.handleTraceRefMouseOut(ev.target, ev.relatedTarget);
        InfoTargetController.handleInfoTargetMouseOut(ev.target, ev.relatedTarget);
    }

    function actionAttr(action, si, gi, ri) {
        var html = ' data-trace-action="' + action + '"';
        if (si !== undefined) html += ' data-si="' + si + '"';
        if (gi !== undefined) html += ' data-gi="' + gi + '"';
        if (ri !== undefined) html += ' data-ri="' + ri + '"';
        return html;
    }

    function intAttr(el, name) {
        var value = Number(el.getAttribute(name));
        return Number.isInteger(value) ? value : 0;
    }

    function searchDirection(el) {
        return intAttr(el, 'data-direction') < 0 ? -1 : 1;
    }

    function navDirection(el) {
        return el.getAttribute('data-direction') === 'prev' ? 'prev' : 'next';
    }

    function boolAttr(el, name) {
        return el.getAttribute(name) === 'true';
    }

    function applyInfoTargetPreviewHoldAction(host) {
        if (!host || !host.getAttribute) return false;
        var action = host.getAttribute('data-trace-action') || '';
        var si = intAttr(host, 'data-si');
        var gi = intAttr(host, 'data-gi');
        var ri = intAttr(host, 'data-ri');

        if (action === 'set-info-tab') {
            TraceActions.detail.setRuleInfoTab(si, gi, ri, host.getAttribute('data-tab-id') || '', null);
            return true;
        }
        if (action === 'set-info-switcher') {
            TraceActions.detail.setRuleInfoSwitcher(
                si,
                gi,
                ri,
                host.getAttribute('data-switcher-key') || '',
                host.getAttribute('data-switcher-id') || '',
                null
            );
            return true;
        }
        return false;
    }

    function actionTarget(ev) {
        if (!ev || !ev.target || !ev.target.closest) return null;
        return ev.target.closest('[data-trace-action]');
    }

    function handleClick(ev) {
        cancelDelayedInfoTargetHover();
        var el = actionTarget(ev);
        if (!el) return;
        if (consumeSuppressedInfoTargetPreviewClick(el, ev)) return;
        hideCollapsedLabelLens();

        var action = el.getAttribute('data-trace-action');
        var si = intAttr(el, 'data-si');
        var gi = intAttr(el, 'data-gi');
        var ri = intAttr(el, 'data-ri');

        switch (action) {
        case 'cycle-global-state':
            TraceActions.layout.cycleGlobalState();
            break;
        case 'toggle-stage':
            TraceActions.layout.toggleStage(si);
            break;
        case 'toggle-stage-rules':
            TraceActions.layout.toggleStageRules(si, ev);
            break;
        case 'toggle-group':
            TraceActions.layout.toggleGroup(si, gi, ev);
            break;
        case 'toggle-group-rules':
            TraceActions.layout.toggleGroupRules(si, gi, ev);
            break;
        case 'collapsed-group-click':
            TraceActions.layout.onCollapsedGroupClick(si, gi, ev);
            break;
        case 'toggle-rule':
            TraceActions.layout.toggleRule(si, gi, ri, ev);
            break;
        case 'toggle-rule-fullscreen':
            TraceActions.fullscreen.toggleRuleFullscreen(si, gi, ri, ev);
            break;
        case 'collapsed-rule-click':
            TraceActions.layout.onCollapsedRuleClick(si, gi, ri, ev);
            break;
        case 'toggle-rule-fields':
            TraceActions.detail.toggleRuleFields(si, gi, ri, ev);
            break;
        case 'toggle-rule-pinned':
            TraceActions.detail.toggleRulePinned(si, gi, ri, ev);
            break;
        case 'toggle-rule-info':
            TraceActions.detail.toggleRuleInfo(si, gi, ri, ev);
            break;
        case 'open-metadata-details':
            TraceActions.detail.openFieldDetails(
                el.getAttribute('data-node-id') || '',
                el.getAttribute('data-meta-index') || '',
                ev
            );
            break;
        case 'pin-info-metadata-details':
            TraceActions.detail.pinInfoFieldDetails(
                si,
                gi,
                ri,
                el.getAttribute('data-tab-id') || '',
                ev
            );
            break;
        case 'close-info-metadata-details':
            TraceActions.detail.closeInfoFieldDetails(
                si,
                gi,
                ri,
                el.getAttribute('data-tab-id') || '',
                ev
            );
            break;
        case 'set-info-tab':
            TraceActions.detail.setRuleInfoTab(si, gi, ri, el.getAttribute('data-tab-id') || '', ev);
            break;
        case 'set-info-switcher':
            TraceActions.detail.setRuleInfoSwitcher(
                si,
                gi,
                ri,
                el.getAttribute('data-switcher-key') || '',
                el.getAttribute('data-switcher-id') || '',
                ev
            );
            break;
        case 'activate-info-target':
            InfoTargetController.handleInfoTargetClick(el, ev);
            break;
        case 'set-all-rule-feature':
            TraceActions.detail.setAllRuleFeature(
                el.getAttribute('data-detail-feature') || '',
                el.getAttribute('data-detail-action') === 'show',
                ev
            );
            break;
        case 'set-empty-stages-visible':
            TraceActions.topBar.setEmptyStagesVisible(
                el.getAttribute('data-empty-stages-action') === 'show',
                ev
            );
            break;
        case 'on-diff-button-click':
            TraceActions.diff.onDiffButtonClick();
            break;
        case 'set-diff-a':
            TraceActions.diff.setDiffA(si, gi, ri, ev);
            break;
        case 'set-diff-b':
            TraceActions.diff.setDiffB(si, gi, ri, ev);
            break;
        case 'navigate-rule':
            TraceActions.navigation.handleNavButtonClick(
                navDirection(el),
                si,
                gi,
                ri,
                ev,
                boolAttr(el, 'data-block-other-diff-rule')
            );
            break;
        case 'group-navigate':
            if (navDirection(el) === 'prev') {
                TraceActions.navigation.groupNavigatePrev(si, gi, ev);
            } else {
                TraceActions.navigation.groupNavigateNext(si, gi, ev);
            }
            break;
        case 'close-rule-fullscreen-to-rule':
            TraceActions.fullscreen.closeRuleFullscreenToRule(si, gi, ri, ev);
            break;
        case 'close-rule-fullscreen-to-rule-from-title':
            TraceActions.fullscreen.closeRuleFullscreenToRuleFromTitle(si, gi, ri, ev);
            break;
        case 'search-nav':
            TraceActions.search.handleSearchNavButtonClick(searchDirection(el), ev);
            break;
        case 'expand-search-matches':
            TraceActions.search.expandSearchMatches(ev);
            break;
        case 'global-search':
            TraceActions.search.runGlobalSearch(ev);
            break;
        case 'clear-search-input':
            TraceActions.search.clearSearchInput(ev);
            break;
        case 'search-box':
            TraceActions.topBar.closeSettingsPopover();
            break;
        case 'open-mobile-search':
            TraceActions.search.openMobileSearch(ev);
            break;
        case 'close-mobile-search':
            TraceActions.search.closeMobileSearch(ev);
            break;
        case 'toggle-settings-popover':
            TraceActions.topBar.toggleSettingsPopover(ev);
            break;
        case 'toggle-node-columns-menu':
            TraceActions.topBar.toggleNodeColumnsMenu(ev);
            break;
        case 'toggle-diff-fields-menu':
            TraceActions.topBar.toggleDiffFieldsMenu(ev);
            break;
        case 'reset-node-columns':
            TraceActions.detail.resetNodeColumns(ev);
            break;
        case 'reset-diff-fields':
            TraceActions.detail.resetDiffFields(ev);
            break;
        case 'solo-node-column':
            TraceActions.detail.soloNodeColumn(el.getAttribute('data-node-column-key') || '', ev);
            break;
        case 'solo-diff-field':
            TraceActions.detail.soloDiffField(el.getAttribute('data-diff-field-key') || '', ev);
            break;
        case 'set-all-node-columns':
            TraceActions.detail.setAllNodeColumns(el.getAttribute('data-visible') === 'true', ev);
            break;
        case 'set-all-diff-fields':
            TraceActions.detail.setAllDiffFields(el.getAttribute('data-visible') === 'true', ev);
            break;
        case 'apply-node-column-preset':
            TraceActions.detail.applyNodeColumnPreset(
                el.getAttribute('data-node-column-preset-index') || '',
                ev
            );
            break;
        case 'pin-node-column':
            TraceActions.detail.setNodeColumnVisible(el.getAttribute('data-node-column-key') || '', true, ev);
            break;
        case 'unpin-node-column':
            TraceActions.detail.setNodeColumnVisible(el.getAttribute('data-node-column-key') || '', false, ev);
            break;
        case 'set-theme-dark-mode':
            TraceActions.theme.setThemeDarkMode(el.getAttribute('data-dark-mode') === 'true', ev);
            break;
        case 'tree-toggle':
            TraceActions.tree.toggle(el.getAttribute('data-node-id') || '', ev);
            break;
        case 'tree-meta-toggle':
            TraceActions.tree.toggleMeta(el.getAttribute('data-node-id') || '', ev);
            break;
        case 'tree-row-reverse-blink':
            InfoTargetController.reverseBlinkTreeNode(el.getAttribute('data-node-id') || '', ev);
            break;
        default:
            break;
        }
    }

    function handlePointerDown(ev) {
        var previewHoldStarted = startInfoTargetPreviewHold(ev);
        if (!previewHoldStarted) cancelDelayedInfoTargetHover();
        var el = actionTarget(ev);
        if (!el) return;
        if (el.getAttribute('data-trace-action') === 'search-nav') {
            TraceActions.search.startSearchNavRepeat(searchDirection(el), ev);
        } else if (el.getAttribute('data-trace-action') === 'navigate-rule') {
            TraceActions.navigation.startFullscreenNavRepeat(
                navDirection(el),
                intAttr(el, 'data-si'),
                intAttr(el, 'data-gi'),
                intAttr(el, 'data-ri'),
                ev,
                boolAttr(el, 'data-block-other-diff-rule')
            );
        }
    }

    function handleChange(ev) {
        var el = actionTarget(ev);
        if (!el) return;

        switch (el.getAttribute('data-trace-action')) {
        case 'set-active-trace':
            TraceActions.trace.setActiveTrace(el.value);
            break;
        case 'theme-select':
            TraceActions.theme.setTheme(el.value, ev);
            break;
        case 'toggle-node-column':
            TraceActions.detail.setNodeColumnVisible(
                el.getAttribute('data-node-column-key') || '',
                !!el.checked,
                ev
            );
            break;
        case 'toggle-diff-field':
            TraceActions.detail.setDiffFieldVisible(
                el.getAttribute('data-diff-field-key') || '',
                !!el.checked,
                ev
            );
            break;
        default:
            break;
        }
    }

    function handleInput(ev) {
        var el = actionTarget(ev);
        if (!el) return;
        if (el.getAttribute('data-trace-action') === 'search-box') {
            TraceActions.search.scheduleSearch(ev);
        } else if (el.getAttribute('data-trace-action') === 'node-columns-filter') {
            applyNodeColumnsFilter();
        } else if (el.getAttribute('data-trace-action') === 'diff-fields-filter') {
            applyDiffFieldsFilter();
        }
    }

    function handleKeyDown(ev) {
        var el = actionTarget(ev);
        if (!el) return;
        if (el.getAttribute('data-trace-action') === 'search-box') {
            TraceActions.search.handleSearchBoxKeydown(ev);
            return;
        }
        if (ev.key === 'Enter' || ev.key === ' ') {
            var tag = el.tagName ? el.tagName.toUpperCase() : '';
            if (tag !== 'BUTTON' && tag !== 'INPUT' && tag !== 'SELECT' && tag !== 'A') {
                ev.preventDefault();
                handleClick(ev);
            }
        }
    }

    function handleFocusIn(ev) {
        var labelSource = collapsedLabelSource(ev.target);
        if (labelSource) {
            showCollapsedLabelLens(labelSource);
            scheduleFocusedCollapsedLabelLens(labelSource);
        } else {
            hideCollapsedLabelLens();
        }

        var el = actionTarget(ev);
        if (!el) return;
        if (el.getAttribute('data-trace-action') === 'search-box') {
            TraceActions.topBar.closeSettingsPopover();
        }
    }

    function handleFocusOut(ev) {
        var labelSource = collapsedLabelSource(ev.target);
        if (!labelSource) return;
        var related = ev.relatedTarget;
        if (related && labelSource.contains(related)) return;
        hideCollapsedLabelLens(labelSource);
    }

    var infoPanelDrag = null;
    var nodeColumnResizeDrag = null;
    var nodeColumnReorderDrag = null;
    var NODE_COLUMN_REORDER_DRAG_THRESHOLD_PX = 4;
    var INFO_PANEL_RESIZE_MIN_HEIGHT = 60;

    function nodeColumnResizeHandle(ev) {
        return ev.target && ev.target.closest && ev.target.closest('.pinned-header-resize');
    }

    function nodeColumnReorderCell(ev) {
        if (!ev || !ev.target || !ev.target.closest) return null;
        if (nodeColumnResizeHandle(ev)) return null;
        if (ev.target.closest('.pinned-header-unpin')) return null;
        return ev.target.closest('.pinned-header-cell[data-node-column-key]');
    }

    function clearNodeColumnReorderClasses(drag) {
        var header = drag && drag.header;
        if (!header || !header.querySelectorAll) return;
        var cells = header.querySelectorAll('.pinned-header-cell');
        for (var i = 0; i < cells.length; i++) {
            if (!cells[i].classList) continue;
            cells[i].classList.remove(
                'node-column-drag-source',
                'node-column-drop-before',
                'node-column-drop-after'
            );
        }
    }

    function nodeColumnDropIndexFromPointer(drag, clientX) {
        var cells = drag && drag.header && drag.header.querySelectorAll
            ? drag.header.querySelectorAll('.pinned-header-cell[data-node-column-index]')
            : [];
        if (!cells || !cells.length) return -1;
        for (var i = 0; i < cells.length; i++) {
            var rect = cells[i].getBoundingClientRect ? cells[i].getBoundingClientRect() : null;
            if (!rect) continue;
            if (clientX < rect.left + rect.width / 2) return i;
        }
        return cells.length;
    }

    function nodeColumnDropIndexWouldMove(drag, dropIndex) {
        if (!drag || !Number.isFinite(dropIndex)) return false;
        if (dropIndex < 0) return false;
        return dropIndex !== drag.startIndex && dropIndex !== drag.startIndex + 1;
    }

    function updateNodeColumnReorderPreview(drag, dropIndex) {
        clearNodeColumnReorderClasses(drag);
        if (!drag || !drag.header || !drag.header.querySelectorAll) return;
        if (drag.cell && drag.cell.classList) drag.cell.classList.add('node-column-drag-source');
        if (!nodeColumnDropIndexWouldMove(drag, dropIndex)) return;

        var cells = drag.header.querySelectorAll('.pinned-header-cell[data-node-column-index]');
        if (!cells || !cells.length) return;
        if (dropIndex <= 0 && cells[0].classList) {
            cells[0].classList.add('node-column-drop-before');
            return;
        }
        if (dropIndex >= cells.length && cells[cells.length - 1].classList) {
            cells[cells.length - 1].classList.add('node-column-drop-after');
            return;
        }
        if (cells[dropIndex] && cells[dropIndex].classList) {
            cells[dropIndex].classList.add('node-column-drop-before');
        }
    }

    function endNodeColumnReorderDrag(ev, commit) {
        var drag = nodeColumnReorderDrag || resizeRuntime().nodeColumnReorderDrag;
        if (!drag) return;
        if (ev && ev.pointerId !== undefined &&
            drag.pointerId !== undefined &&
            ev.pointerId !== drag.pointerId) {
            return;
        }
        clearNodeColumnReorderClasses(drag);
        if (document.body && document.body.classList) document.body.classList.remove('node-column-reordering');
        nodeColumnReorderDrag = null;
        resizeRuntime().nodeColumnReorderDrag = null;
        if (drag.cell && drag.cell.releasePointerCapture && drag.pointerId !== undefined) {
            try { drag.cell.releasePointerCapture(drag.pointerId); } catch (err) {}
        }
        if (commit && drag.active && nodeColumnDropIndexWouldMove(drag, drag.dropIndex)) {
            TraceActions.detail.reorderNodeColumn(drag.key, drag.dropIndex, ev);
        }
    }

    function applyNodeColumnResizePreview(index, width) {
        if (!hasDOM() || !document.querySelectorAll) return;
        var px = clampNodeColumnWidth(width) + 'px';
        var cells = document.querySelectorAll('[data-node-column-index="' + String(index) + '"]');
        for (var i = 0; i < cells.length; i++) {
            if (cells[i].style && cells[i].style.setProperty) {
                cells[i].style.setProperty('--pinned-column-width', px);
            }
        }
    }

    function handleNodeColumnResizePointerDown(ev) {
        if (ev.button !== 0) return;
        var handle = nodeColumnResizeHandle(ev);
        if (!handle) return;
        var cell = handle.closest ? handle.closest('.pinned-header-cell') : null;
        var key = handle.getAttribute('data-node-column-key') || '';
        var index = Number(handle.getAttribute('data-node-column-index'));
        if (!key || !Number.isFinite(index)) return;

        ev.preventDefault();
        ev.stopPropagation();
        var rect = cell && cell.getBoundingClientRect ? cell.getBoundingClientRect() : null;
        var startWidth = rect && Number.isFinite(rect.width)
            ? rect.width
            : nodeColumnWidthForKey(key);
        nodeColumnResizeDrag = {
            handle: handle,
            key: key,
            index: index,
            pointerId: ev.pointerId,
            startX: ev.clientX,
            startWidth: startWidth,
            currentWidth: startWidth
        };
        resizeRuntime().nodeColumnResizeDrag = nodeColumnResizeDrag;
        handle.classList.add('resizing');
        if (document.body && document.body.classList) document.body.classList.add('node-column-resizing');
        if (handle.setPointerCapture && ev.pointerId !== undefined) {
            try { handle.setPointerCapture(ev.pointerId); } catch (err) {}
        }
    }

    function handleNodeColumnResizePointerMove(ev) {
        var drag = nodeColumnResizeDrag || resizeRuntime().nodeColumnResizeDrag;
        if (!drag) return;
        if (ev.pointerId !== undefined &&
            drag.pointerId !== undefined &&
            ev.pointerId !== drag.pointerId) {
            return;
        }
        ev.preventDefault();
        drag.currentWidth = clampNodeColumnWidth(drag.startWidth + ev.clientX - drag.startX);
        applyNodeColumnResizePreview(drag.index, drag.currentWidth);
    }

    function handleNodeColumnResizePointerUp(ev) {
        var drag = nodeColumnResizeDrag || resizeRuntime().nodeColumnResizeDrag;
        if (!drag) return;
        if (ev && ev.pointerId !== undefined &&
            drag.pointerId !== undefined &&
            ev.pointerId !== drag.pointerId) {
            return;
        }
        if (drag.handle && drag.handle.classList) drag.handle.classList.remove('resizing');
        if (document.body && document.body.classList) document.body.classList.remove('node-column-resizing');
        nodeColumnResizeDrag = null;
        resizeRuntime().nodeColumnResizeDrag = null;
        TraceActions.detail.setNodeColumnWidth(drag.key, drag.currentWidth, ev);
    }

    function handleNodeColumnReorderPointerDown(ev) {
        if (ev.button !== 0) return;
        var cell = nodeColumnReorderCell(ev);
        if (!cell) return;
        var header = cell.closest ? cell.closest('.tree-pinned-header') : null;
        var key = cell.getAttribute('data-node-column-key') || '';
        var index = Number(cell.getAttribute('data-node-column-index'));
        if (!header || !key || !Number.isFinite(index)) return;

        nodeColumnReorderDrag = {
            cell: cell,
            header: header,
            key: key,
            pointerId: ev.pointerId,
            startX: ev.clientX,
            startY: ev.clientY,
            startIndex: index,
            dropIndex: index,
            active: false
        };
        resizeRuntime().nodeColumnReorderDrag = nodeColumnReorderDrag;
        if (cell.setPointerCapture && ev.pointerId !== undefined) {
            try { cell.setPointerCapture(ev.pointerId); } catch (err) {}
        }
    }

    function handleNodeColumnReorderPointerMove(ev) {
        var drag = nodeColumnReorderDrag || resizeRuntime().nodeColumnReorderDrag;
        if (!drag) return;
        if (ev.pointerId !== undefined &&
            drag.pointerId !== undefined &&
            ev.pointerId !== drag.pointerId) {
            return;
        }

        var dx = ev.clientX - drag.startX;
        var dy = ev.clientY - drag.startY;
        if (!drag.active) {
            if (Math.sqrt(dx * dx + dy * dy) < NODE_COLUMN_REORDER_DRAG_THRESHOLD_PX) return;
            drag.active = true;
            if (document.body && document.body.classList) document.body.classList.add('node-column-reordering');
        }

        ev.preventDefault();
        drag.dropIndex = nodeColumnDropIndexFromPointer(drag, ev.clientX);
        updateNodeColumnReorderPreview(drag, drag.dropIndex);
    }

    function handleNodeColumnReorderPointerUp(ev) {
        endNodeColumnReorderDrag(ev, true);
    }

    function handleInfoResizerPointerDown(ev) {
        var resizer = ev.target && ev.target.closest && ev.target.closest('.info-panel-resizer');
        if (!resizer) return;
        var panel = resizer.nextElementSibling;
        if (!panel || !panel.classList.contains('rule-info-panel')) return;
        ev.preventDefault();
        resizer.setPointerCapture(ev.pointerId);
        resizer.classList.add('resizing');
        panel.style.maxHeight = 'none';
        panel.removeAttribute('data-info-auto-height');
        var key = (panel.id || '').replace(/^info-/, '');
        infoPanelDrag = {
            resizer: resizer,
            panel: panel,
            key: key,
            startY: ev.clientY,
            startH: panel.getBoundingClientRect().height,
            maxH: infoPanelResizeMaxHeight(panel, resizer)
        };
    }

    function infoPanelResizeMaxHeight(panel, resizer) {
        if (!panel || !panel.closest) return Infinity;

        var content = panel.closest('.rule-content');
        var contentRect = content && content.getBoundingClientRect ?
            content.getBoundingClientRect() :
            null;
        var resizerRect = resizer && resizer.getBoundingClientRect ?
            resizer.getBoundingClientRect() :
            null;
        var contentHeight = contentRect ? Number(contentRect.height) : NaN;
        var resizerHeight = resizerRect ? Number(resizerRect.height) : 0;
        if (Number.isFinite(contentHeight) && contentHeight > 0) {
            return Math.max(INFO_PANEL_RESIZE_MIN_HEIGHT, contentHeight - Math.max(0, resizerHeight));
        }

        return Infinity;
    }

    function clampInfoPanelResizeHeight(height, maxHeight) {
        var upper = Number.isFinite(maxHeight) ?
            Math.max(INFO_PANEL_RESIZE_MIN_HEIGHT, maxHeight) :
            maxHeight;
        return clampNumber(height, INFO_PANEL_RESIZE_MIN_HEIGHT, upper);
    }

    function handleInfoResizerPointerMove(ev) {
        if (!infoPanelDrag) return;
        var delta = infoPanelDrag.startY - ev.clientY;
        var newH = clampInfoPanelResizeHeight(
            infoPanelDrag.startH + delta,
            infoPanelDrag.maxH
        );
        infoPanelDrag.currentH = newH;
        infoPanelDrag.panel.style.height = newH + 'px';
    }

    function handleInfoResizerPointerUp() {
        if (!infoPanelDrag) return;
        infoPanelDrag.resizer.classList.remove('resizing');
        if (infoPanelDrag.key) {
            resizeRuntime().infoPanelHeights[infoPanelDrag.key] =
                infoPanelDrag.panel.getBoundingClientRect().height;
        }
        infoPanelDrag = null;
    }

    function bind() {
        if (!hasDOM()) return;
        var state = actionEventsRuntime();
        if (!document.addEventListener || state.boundTarget === document) return;
        if (state.boundTarget && state.boundTarget.removeEventListener) {
            state.boundTarget.removeEventListener('change', handleChange);
            state.boundTarget.removeEventListener('click', handleClick);
            state.boundTarget.removeEventListener('focusout', handleFocusOut);
            state.boundTarget.removeEventListener('focusin', handleFocusIn);
            state.boundTarget.removeEventListener('input', handleInput);
            state.boundTarget.removeEventListener('keydown', handleKeyDown);
            state.boundTarget.removeEventListener('mouseover', handleMouseOver);
            state.boundTarget.removeEventListener('mouseout', handleMouseOut);
            state.boundTarget.removeEventListener('pointerdown', handlePointerDown);
            state.boundTarget.removeEventListener('pointermove', handleInfoTargetPreviewHoldPointerMove);
            state.boundTarget.removeEventListener('pointerup', handleInfoTargetPreviewHoldPointerUp);
            state.boundTarget.removeEventListener('pointercancel', handleInfoTargetPreviewHoldPointerCancel);
            state.boundTarget.removeEventListener('pointerdown', handleInfoResizerPointerDown);
            state.boundTarget.removeEventListener('pointermove', handleInfoResizerPointerMove);
            state.boundTarget.removeEventListener('pointerup', handleInfoResizerPointerUp);
            state.boundTarget.removeEventListener('pointercancel', handleInfoResizerPointerUp);
            state.boundTarget.removeEventListener('pointerdown', handleNodeColumnResizePointerDown);
            state.boundTarget.removeEventListener('pointermove', handleNodeColumnResizePointerMove);
            state.boundTarget.removeEventListener('pointerup', handleNodeColumnResizePointerUp);
            state.boundTarget.removeEventListener('pointercancel', handleNodeColumnResizePointerUp);
            state.boundTarget.removeEventListener('pointerdown', handleNodeColumnReorderPointerDown);
            state.boundTarget.removeEventListener('pointermove', handleNodeColumnReorderPointerMove);
            state.boundTarget.removeEventListener('pointerup', handleNodeColumnReorderPointerUp);
            state.boundTarget.removeEventListener('pointercancel', endNodeColumnReorderDrag);
        }
        document.addEventListener('change', handleChange);
        document.addEventListener('click', handleClick);
        document.addEventListener('focusout', handleFocusOut);
        document.addEventListener('focusin', handleFocusIn);
        document.addEventListener('input', handleInput);
        document.addEventListener('keydown', handleKeyDown);
        document.addEventListener('mouseover', handleMouseOver);
        document.addEventListener('mouseout', handleMouseOut);
        document.addEventListener('pointerdown', handlePointerDown);
        document.addEventListener('pointermove', handleInfoTargetPreviewHoldPointerMove);
        document.addEventListener('pointerup', handleInfoTargetPreviewHoldPointerUp);
        document.addEventListener('pointercancel', handleInfoTargetPreviewHoldPointerCancel);
        document.addEventListener('pointerdown', handleInfoResizerPointerDown);
        document.addEventListener('pointermove', handleInfoResizerPointerMove);
        document.addEventListener('pointerup', handleInfoResizerPointerUp);
        document.addEventListener('pointercancel', handleInfoResizerPointerUp);
        document.addEventListener('pointerdown', handleNodeColumnResizePointerDown);
        document.addEventListener('pointermove', handleNodeColumnResizePointerMove);
        document.addEventListener('pointerup', handleNodeColumnResizePointerUp);
        document.addEventListener('pointercancel', handleNodeColumnResizePointerUp);
        document.addEventListener('pointerdown', handleNodeColumnReorderPointerDown);
        document.addEventListener('pointermove', handleNodeColumnReorderPointerMove);
        document.addEventListener('pointerup', handleNodeColumnReorderPointerUp);
        document.addEventListener('pointercancel', endNodeColumnReorderDrag);
        state.boundTarget = document;
    }

    return {
        actionAttr: actionAttr,
        bind: bind,
        clearGraphHighlights: InfoTargetController.clearGraphHighlights,
        handleClick: handleClick,
        hideCollapsedLabelLens: hideCollapsedLabelLens,
        scheduleFocusedCollapsedLabelLens: scheduleFocusedCollapsedLabelLens
    };
})();
