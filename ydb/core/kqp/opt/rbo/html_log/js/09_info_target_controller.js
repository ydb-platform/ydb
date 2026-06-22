var TRACE_NAV_BLINK_MS = 160;

var InfoTargetController = (function() {
    var activeHighlightRequest = null;
    var activeHoverPreview = null;
    var graphHighlightedIds = [];
    var highlightedTargetEls = [];
    var activeBlinkEls = [];
    var activeAppliedPulseSourceEls = [];
    var activeAppliedPulseTargetEls = [];
    var activeTargetOverlayEls = [];
    var activeAppliedPulseOverlayEls = [];
    var hoverEpoch = 0;
    var blinkEpoch = 0;
    var appliedPulseEpoch = 0;
    var targetHighlightClass = 'info-target-highlighted';
    var targetRangeHighlightClass = 'info-target-range-highlighted';
    var targetBlinkClass = 'trace-nav-blink';
    var targetAppliedPulseClass = 'info-target-applied-target';
    var targetAppliedRangeClass = 'info-target-applied-range';
    var sourceAppliedPulseClass = 'info-target-applied-source';
    var graphHighlightClass = 'graph-highlighted';
    var defaultBlinkMs = TRACE_NAV_BLINK_MS;
    var defaultAppliedPulseMs = TRACE_NAV_BLINK_MS;
    var selectionOutlineMarginPx = 4;
    var selectionVisibleRatioThreshold = 0.4;
    var treeSelectionTopInsetPx = 2;
    var treeSelectionSideInsetPx = 3;
    var treeSelectionReadableLabelPx = 180;

    function rootDocument() {
        return typeof document !== 'undefined' ? document : null;
    }

    function byId(id) {
        var doc = rootDocument();
        return doc && doc.getElementById ? doc.getElementById(id) : null;
    }

    function addClass(el, className) {
        if (el && el.classList) el.classList.add(className);
    }

    function removeClass(el, className) {
        if (el && el.classList) el.classList.remove(className);
    }

    function createDivWithClasses(classNames) {
        var doc = rootDocument();
        if (!doc || !doc.createElement) return null;
        var el = doc.createElement('div');
        classNames = String(classNames || '').split(/\s+/);
        for (var i = 0; i < classNames.length; i++) {
            if (classNames[i]) addClass(el, classNames[i]);
        }
        return el;
    }

    function appendChild(parent, child) {
        if (!parent || !child || !parent.appendChild) return false;
        try {
            parent.appendChild(child);
            return true;
        } catch (err) {
            return false;
        }
    }

    function removeElement(el) {
        if (!el) return false;
        try {
            if (el.remove) {
                el.remove();
                return true;
            }
            if (el.parentNode && el.parentNode.removeChild) {
                el.parentNode.removeChild(el);
                return true;
            }
        } catch (err) {}
        return false;
    }

    function removeOverlayEls(elements) {
        elements = elements || [];
        for (var i = 0; i < elements.length; i++) {
            removeElement(elements[i]);
        }
    }

    function forceAnimationRestart(el) {
        if (!el) return;
        try {
            if (el.offsetWidth !== undefined) void el.offsetWidth;
            else if (el.getBBox) el.getBBox();
        } catch (err) {}
    }

    function timerFromOptions(options) {
        if (options && options.timers && options.timers.setTimeout) return options.timers.setTimeout;
        return typeof setTimeout === 'function' ? setTimeout : null;
    }

    function uniqueElements(elements) {
        var result = [];
        for (var i = 0; i < elements.length; i++) {
            var el = elements[i];
            if (!el) continue;
            var seen = false;
            for (var j = 0; j < result.length; j++) {
                if (result[j] === el) {
                    seen = true;
                    break;
                }
            }
            if (!seen) result.push(el);
        }
        return result;
    }

    function treeRowElement(el) {
        if (!el) return null;
        if (el.classList && el.classList.contains && el.classList.contains('tree-node-row')) return el;
        if (el.querySelector) {
            var row = el.querySelector('.tree-node-row');
            if (row) return row;
        }
        return el;
    }

    function isTreeNodeRow(el) {
        return !!(el && el.classList && el.classList.contains && el.classList.contains('tree-node-row'));
    }

    function closestWithClass(el, className) {
        for (var cur = el; cur; cur = cur.parentElement) {
            if (cur.classList && cur.classList.contains && cur.classList.contains(className)) return cur;
        }
        return null;
    }

    function beginHoverSession() {
        hoverEpoch += 1;
        return hoverEpoch;
    }

    function beginBlinkSession() {
        blinkEpoch += 1;
        removeBlinkEls();
        return blinkEpoch;
    }

    function sameStringList(left, right) {
        if (!left || !right || left.length !== right.length) return false;
        for (var i = 0; i < left.length; i++) {
            if (left[i] !== right[i]) return false;
        }
        return true;
    }

    function sameNumberList(left, right) {
        if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
        for (var i = 0; i < left.length; i++) {
            if (left[i] !== right[i]) return false;
        }
        return true;
    }

    function numberListStartsWith(value, prefix) {
        if (!Array.isArray(value) || !Array.isArray(prefix) || value.length < prefix.length) return false;
        for (var i = 0; i < prefix.length; i++) {
            if (value[i] !== prefix[i]) return false;
        }
        return true;
    }

    function traceRefNodeIds(value) {
        var parts = String(value || '').split(',');
        var nodeIds = [];
        for (var i = 0; i < parts.length; i++) {
            var id = parts[i].trim();
            if (id) nodeIds.push(id);
        }
        return nodeIds;
    }

    function closestTraceRef(target) {
        if (!target || !target.closest) return null;
        var el = target.closest('[data-trace-ref]');
        if (el && el.getAttribute && el.getAttribute('data-trace-ref-mode') === 'passive') {
            return null;
        }
        return el;
    }

    function closestInfoTargetHost(target) {
        if (!target || !target.closest) return null;
        return target.closest('[data-info-targets], [data-info-primary-target]');
    }

    function intAttr(el, name) {
        if (!el || !el.getAttribute) return null;
        var value = Number(el.getAttribute(name));
        return Number.isInteger(value) ? value : null;
    }

    function idScopeForElement(el) {
        if (el && el.closest) {
            var scoped = el.closest('[data-info-id-scope]');
            var scope = scoped && scoped.getAttribute ? scoped.getAttribute('data-info-id-scope') : '';
            if (scope) return scope;
        }
        return el && el.closest && el.closest('.fullscreen-root') ? 'fs' : '';
    }

    function idScopeOf(context) {
        return context && context.idScope ? String(context.idScope) : '';
    }

    function idScopePrefix(idScope) {
        return idScope ? idScope + '-' : '';
    }

    function treeNodeIdScope(nodeId) {
        return String(nodeId || '').indexOf('tn-fs-') === 0 ? 'fs' : '';
    }

    function diffTargetScopeInfo(idScope) {
        idScope = String(idScope || '');
        if (idScope === 'fsdiff-a') return { side: 'a', fullscreen: true };
        if (idScope === 'fsdiff-b') return { side: 'b', fullscreen: true };
        return null;
    }

    function contextFromInfoTargetHost(el) {
        return {
            si: intAttr(el, 'data-si'),
            gi: intAttr(el, 'data-gi'),
            ri: intAttr(el, 'data-ri'),
            idScope: idScopeForElement(el)
        };
    }

    function parseTargetJson(value, fallback) {
        if (!value) return fallback;
        try {
            var parsed = JSON.parse(value);
            return parsed === undefined || parsed === null ? fallback : parsed;
        } catch (err) {
            return fallback;
        }
    }

    function targetsFromHost(el) {
        var parsed = parseTargetJson(el && el.getAttribute ? el.getAttribute('data-info-targets') : '', []);
        return Array.isArray(parsed) ? parsed : [parsed];
    }

    function primaryTargetFromHost(el) {
        return parseTargetJson(el && el.getAttribute ? el.getAttribute('data-info-primary-target') : '', null);
    }

    function stopEvent(ev) {
        if (!ev) return;
        if (ev.preventDefault) ev.preventDefault();
        if (ev.stopPropagation) ev.stopPropagation();
    }

    function removeGraphHighlights() {
        for (var i = 0; i < graphHighlightedIds.length; i++) {
            removeClass(byId(graphHighlightedIds[i]), graphHighlightClass);
        }
        graphHighlightedIds = [];
    }

    function removeTargetHighlights() {
        for (var i = 0; i < highlightedTargetEls.length; i++) {
            removeClass(highlightedTargetEls[i], targetHighlightClass);
            removeClass(highlightedTargetEls[i], targetRangeHighlightClass);
        }
        highlightedTargetEls = [];
        removeOverlayEls(activeTargetOverlayEls);
        activeTargetOverlayEls = [];
    }

    function clearTargetHighlights(session) {
        if (session !== undefined && session !== hoverEpoch) return false;
        if (activeHoverPreview && (session === undefined || activeHoverPreview.session === session)) {
            clearHostHoverPreview(activeHoverPreview);
            activeHoverPreview = null;
        }
        beginHoverSession();
        activeHighlightRequest = null;
        removeTargetHighlights();
        return true;
    }

    function clearGraphHighlights(session) {
        if (session !== undefined && session !== hoverEpoch) return false;
        beginHoverSession();
        removeGraphHighlights();
        return true;
    }

    function setGraphHighlights(nodeIds, session) {
        if (sameStringList(nodeIds, graphHighlightedIds)) return false;
        removeGraphHighlights();
        graphHighlightedIds = nodeIds.slice();
        for (var i = 0; i < graphHighlightedIds.length; i++) {
            addClass(byId(graphHighlightedIds[i]), graphHighlightClass);
        }
        if (session !== undefined) hoverEpoch = session;
        return true;
    }

    function formatPx(value) {
        value = Number(value);
        if (!Number.isFinite(value)) value = 0;
        return (Math.round(value * 100) / 100) + 'px';
    }

    function treeSelectionOverlayForRow(el) {
        var wrap = closestWithClass(el, 'rule-tree-wrap');
        if (!wrap) return null;
        var overlay = wrap.__traceSelectionOverlay || null;
        if (overlay && overlay.parentElement === wrap) return overlay;

        overlay = createDivWithClasses('tree-selection-overlay');
        if (!overlay) return null;
        if (!appendChild(wrap, overlay)) return null;
        wrap.__traceSelectionOverlay = overlay;
        return overlay;
    }

    function drawTreeSelectionOverlay(el, union, clip, options) {
        options = options || {};
        var wrap = closestWithClass(el, 'rule-tree-wrap');
        var wrapRect = elementRect(wrap);
        if (!wrap || !wrapRect || !union) return null;

        var topInset = treeSelectionTopInsetPx;
        var left = union.left - treeSelectionSideInsetPx;
        var right = union.right + treeSelectionSideInsetPx;
        if (clip) {
            left = Math.max(left, clip.left);
            right = Math.min(right, clip.right);
        }
        var width = right - left;
        var height = rectHeight(union) - topInset * 2;
        if (width <= 1 || height <= 1) return null;

        var overlay = treeSelectionOverlayForRow(el);
        if (!overlay) return null;

        var rect = createDivWithClasses('tree-selection-ring' + (options.applied ? ' tree-selection-ring-applied' : ''));
        if (!rect || !rect.style) return null;
        rect.style.setProperty('--tree-selection-left', formatPx(left - wrapRect.left + (wrap.scrollLeft || 0)));
        rect.style.setProperty('--tree-selection-top', formatPx(union.top - wrapRect.top + (wrap.scrollTop || 0) + topInset));
        rect.style.setProperty('--tree-selection-width', formatPx(width));
        rect.style.setProperty('--tree-selection-height', formatPx(height));
        if (!appendChild(overlay, rect)) return null;
        return rect;
    }

    function configureRangeHighlight(el, rangeElements, options) {
        options = options || {};
        rangeElements = uniqueElements(rangeElements || []);
        if (!el || !el.style) return false;
        if (!rangeElements.length) {
            if (!isTreeNodeRow(el)) return false;
            rangeElements = [el];
        } else if (rangeElements.length === 1 && !isTreeNodeRow(rangeElements[0])) {
            return false;
        }

        var rects = [];
        for (var i = 0; i < rangeElements.length; i++) {
            var rect = treeRowHighlightRect(rangeElements[i]);
            if (rect) rects.push(rect);
        }
        var union = unionRects(rects);
        if (!union) return false;

        var clip = treeRowHighlightClipRect(el);
        addClass(el, options.rangeClass || targetRangeHighlightClass);
        return drawTreeSelectionOverlay(el, union, clip, options);
    }

    function normalizeHighlightSpecs(items) {
        var specs = [];
        items = Array.isArray(items) ? items : [];
        for (var i = 0; i < items.length; i++) {
            var item = items[i];
            var el = item && item.element ? item.element : item;
            if (!el) continue;
            var rangeElements = item && item.rangeElements ? item.rangeElements : null;
            var existing = null;
            for (var j = 0; j < specs.length; j++) {
                if (specs[j].element === el) {
                    existing = specs[j];
                    break;
                }
            }
            if (existing) {
                if (rangeElements && rangeElements.length > 1) existing.rangeElements = rangeElements;
            } else {
                specs.push({ element: el, rangeElements: rangeElements });
            }
        }
        return specs;
    }

    function highlightElements(elements, options) {
        options = options || {};
        var session = options.session;
        if (session === undefined) session = beginHoverSession();
        if (session !== hoverEpoch) return { status: 'failed', reason: 'stale-session', session: session, count: 0 };

        removeTargetHighlights();
        var highlightSpecs = normalizeHighlightSpecs(elements);
        highlightedTargetEls = [];
        for (var i = 0; i < highlightSpecs.length; i++) {
            var el = highlightSpecs[i].element;
            highlightedTargetEls.push(el);
            addClass(el, options.className || targetHighlightClass);
            if ((options.className || targetHighlightClass) === targetHighlightClass) {
                var overlayEl = configureRangeHighlight(el, highlightSpecs[i].rangeElements);
                if (overlayEl) activeTargetOverlayEls.push(overlayEl);
            }
        }
        highlightedTargetEls = uniqueElements(highlightedTargetEls);
        return { status: highlightedTargetEls.length ? 'resolved' : 'failed', session: session, count: highlightedTargetEls.length };
    }

    function highlightInfoTargets(targets, context, options) {
        options = options || {};
        var session = options.session;
        if (session === undefined) session = beginHoverSession();
        if (!Array.isArray(targets)) targets = targets ? [targets] : [];

        var elements = [];
        var highlightSpecs = [];
        var results = [];
        for (var i = 0; i < targets.length; i++) {
            var result = resolveInfoTarget(targets[i], context);
            results.push(result);
            appendResolvedElements(elements, result);
            appendResolvedHighlightSpecs(highlightSpecs, result);
        }

        var highlight = highlightElements(highlightSpecs, {
            session: session,
            className: options.className || targetHighlightClass
        });
        if (session === hoverEpoch) {
            activeHighlightRequest = {
                targets: targets,
                context: context,
                className: options.className || targetHighlightClass,
                session: session
            };
        }
        return {
            status: aggregateResolutionStatus(results),
            session: session,
            count: highlight.count,
            results: results
        };
    }

    function reapplyActiveTargetHighlights() {
        if (!activeHighlightRequest) return false;
        if (activeHighlightRequest.session !== hoverEpoch) {
            activeHighlightRequest = null;
            return false;
        }
        var request = activeHighlightRequest;
        var result = highlightInfoTargets(request.targets, request.context, {
            session: request.session,
            className: request.className
        });
        return !!result && result.count > 0;
    }

    function handleTraceRefMouseOver(target) {
        var el = closestTraceRef(target);
        if (!el || !el.getAttribute) return false;
        closeActiveHoverPreview({ restore: true, clearHighlights: true });
        var session = beginHoverSession();
        el.__infoTargetHoverSession = session;
        return setGraphHighlights(traceRefNodeIds(el.getAttribute('data-trace-ref') || ''), session);
    }

    function handleTraceRefMouseOut(target, relatedTarget) {
        var el = closestTraceRef(target);
        if (!el) return false;
        if (relatedTarget && el.contains && el.contains(relatedTarget)) return false;
        return clearGraphHighlights(el.__infoTargetHoverSession);
    }

    function clearHostHoverPreview(preview) {
        var host = preview && preview.host;
        if (host && host.__infoTargetHoverPreview === preview) {
            host.__infoTargetHoverPreview = null;
        }
    }

    function restoreHoverPreview(preview) {
        if (!preview || !preview.restoreOnLeave) return false;
        restoreSelectionScrollSnapshot(preview.scrollSnapshot);
        return true;
    }

    function closeActiveHoverPreview(options) {
        options = options || {};
        var preview = activeHoverPreview;
        if (!preview) return false;

        activeHoverPreview = null;
        clearHostHoverPreview(preview);
        if (options.restore !== false) {
            restoreHoverPreview(preview);
        }
        if (options.clearHighlights) {
            clearTargetHighlights(preview.session);
        }
        return true;
    }

    function closeOtherHoverPreview(host) {
        if (!activeHoverPreview) return false;
        if (activeHoverPreview.host === host) return false;
        return closeActiveHoverPreview({ restore: true, clearHighlights: true });
    }

    function commitHoverPreviewForHost(host) {
        if (!activeHoverPreview || activeHoverPreview.host !== host) {
            if (host) host.__infoTargetHoverPreview = null;
            return false;
        }
        return closeActiveHoverPreview({ restore: false, clearHighlights: false });
    }

    function commitInfoTargetPreview(target) {
        var el = closestInfoTargetHost(target);
        if (!el || hoverModeForHost(el) !== 'preview') return false;
        if (!activeHoverPreview || activeHoverPreview.host !== el) {
            if (!handlePreviewInfoTargetMouseOver(el)) return false;
        }
        return commitHoverPreviewForHost(el);
    }

    function infoTargetPreviewRestores(target) {
        var el = closestInfoTargetHost(target);
        if (!el) return false;
        var preview = el.__infoTargetHoverPreview;
        return !!(preview && activeHoverPreview === preview && preview.restoreOnLeave);
    }

    function handleInfoTargetMouseOver(target) {
        var el = closestInfoTargetHost(target);
        if (!el) return false;
        return handlePreviewInfoTargetMouseOver(el);
    }

    function handleInfoTargetMouseOut(target, relatedTarget) {
        var el = closestInfoTargetHost(target);
        if (!el) return false;
        if (relatedTarget && el.contains && el.contains(relatedTarget)) return false;
        return finishInfoTargetHover(el);
    }

    function removeBlinkEls() {
        for (var i = 0; i < activeBlinkEls.length; i++) {
            removeClass(activeBlinkEls[i], targetBlinkClass);
        }
        activeBlinkEls = [];
    }

    function removeAppliedPulseEls() {
        for (var i = 0; i < activeAppliedPulseSourceEls.length; i++) {
            removeClass(activeAppliedPulseSourceEls[i], sourceAppliedPulseClass);
        }
        for (var j = 0; j < activeAppliedPulseTargetEls.length; j++) {
            removeClass(activeAppliedPulseTargetEls[j], targetAppliedPulseClass);
            removeClass(activeAppliedPulseTargetEls[j], targetAppliedRangeClass);
        }
        activeAppliedPulseSourceEls = [];
        activeAppliedPulseTargetEls = [];
        removeOverlayEls(activeAppliedPulseOverlayEls);
        activeAppliedPulseOverlayEls = [];
    }

    function beginAppliedPulseSession() {
        appliedPulseEpoch += 1;
        removeAppliedPulseEls();
        return appliedPulseEpoch;
    }

    function clearBlinks(session) {
        if (session !== undefined && session !== blinkEpoch) return false;
        blinkEpoch += 1;
        removeBlinkEls();
        return true;
    }

    function pulseAppliedSelection(sourceEl, highlightSpecs, options) {
        options = options || {};
        var session = options.session;
        if (session === undefined) session = beginAppliedPulseSession();
        if (session !== appliedPulseEpoch) return { status: 'failed', reason: 'stale-session', session: session, count: 0 };

        var count = 0;
        if (sourceEl) {
            removeClass(sourceEl, sourceAppliedPulseClass);
            forceAnimationRestart(sourceEl);
            addClass(sourceEl, sourceAppliedPulseClass);
            activeAppliedPulseSourceEls.push(sourceEl);
            count++;
        }

        var specs = normalizeHighlightSpecs(highlightSpecs);
        for (var i = 0; i < specs.length; i++) {
            var el = specs[i].element;
            if (!el) continue;
            removeClass(el, targetAppliedPulseClass);
            removeClass(el, targetAppliedRangeClass);
            forceAnimationRestart(el);
            addClass(el, targetAppliedPulseClass);
            var overlayEl = configureRangeHighlight(el, specs[i].rangeElements, {
                rangeClass: targetAppliedRangeClass,
                applied: true
            });
            if (overlayEl) {
                forceAnimationRestart(overlayEl);
                activeAppliedPulseOverlayEls.push(overlayEl);
            }
            activeAppliedPulseTargetEls.push(el);
            count++;
        }

        activeAppliedPulseSourceEls = uniqueElements(activeAppliedPulseSourceEls);
        activeAppliedPulseTargetEls = uniqueElements(activeAppliedPulseTargetEls);

        var timer = timerFromOptions(options);
        var duration = Number.isFinite(options.durationMs) ? options.durationMs : defaultAppliedPulseMs;
        if (timer && count) {
            timer(function() {
                if (session !== appliedPulseEpoch) return;
                removeAppliedPulseEls();
            }, duration);
        }

        return { status: count ? 'resolved' : 'failed', session: session, count: count };
    }

    function ruleCoordsForTarget(target, context) {
        if (!target || !target.type) return null;
        context = context || {};
        if (Number.isInteger(context.si) && Number.isInteger(context.gi) && Number.isInteger(context.ri)) {
            return { si: context.si, gi: context.gi, ri: context.ri };
        }
        return null;
    }

    function normalTreeNodeId(si, gi, ri, path, idScope) {
        if (!Array.isArray(path) || !path.length) return '';
        for (var i = 0; i < path.length; i++) {
            if (!Number.isInteger(path[i]) || path[i] < 0) return '';
        }
        return 'tn-' + idScopePrefix(idScope) + si + '-' + gi + '-' + ri + '-' + path.join('-');
    }

    function normalTreeNodeIdForPid(coords, idScope, pid) {
        pid = String(pid || '');
        if (!coords || !pid) return '';
        var path = typeof treePidPath === 'function' ? treePidPath(pid) : null;
        if (!path) return '';
        return normalTreeNodeId(coords.si, coords.gi, coords.ri, path, idScope);
    }

    function infoTargetIsNodeLike(target) {
        return !!target && (
            target.type === 'tree' ||
            target.type === 'tree-subtree' ||
            target.type === 'node' ||
            target.type === 'subtree'
        );
    }

    function infoTargetIsSubtree(target) {
        return !!target && (target.type === 'tree-subtree' || target.type === 'subtree');
    }

    function infoTargetNodeRef(target, coords) {
        if (!target) return null;
        if (Array.isArray(target.path)) return target.path;
        if (target.nodeId) return { nodeId: target.nodeId };
        return null;
    }

    function traceRefNodeIdForTarget(target, context) {
        if (!infoTargetIsNodeLike(target)) return '';
        var coords = ruleCoordsForTarget(target, context);
        if (!coords) return '';
        var idScope = idScopeOf(context);
        if (Array.isArray(target.path)) {
            return normalTreeNodeId(coords.si, coords.gi, coords.ri, target.path, idScope);
        }
        var materializer = treeMaterializerForRuleTarget(coords, idScope);
        var range = rowRangeForInfoTreeTarget(target, materializer, coords);
        var row = range && range.status === 'resolved' &&
            materializer && materializer.state && materializer.state.rows
            ? materializer.state.rows[range.start]
            : null;
        return row && row.pid ? normalTreeNodeIdForPid(coords, idScope, row.pid) : '';
    }

    function ruleElement(si, gi, ri, idScope) {
        if (idScope === 'fs') {
            // The overlay hosts exactly one rule; other rules are behind the
            // modal and must not resolve while it is open.
            var el = byId('fullscreen-rule');
            if (!el || !el.getAttribute) return null;
            return el.getAttribute('data-rule-key') === (si + '-' + gi + '-' + ri) ? el : null;
        }
        return byId('rule-' + si + '-' + gi + '-' + ri);
    }

    function treeContainerElement(coords, idScope) {
        if (!coords) return null;
        var key = idScopePrefix(idScope) + coords.si + '-' + coords.gi + '-' + coords.ri;
        var el = byId('tree-' + key);
        if (el) return el;
        var ruleEl = ruleElement(coords.si, coords.gi, coords.ri, idScope);
        return ruleEl && ruleEl.querySelector ? ruleEl.querySelector('.rule-tree-wrap') : null;
    }

    function cssEscapeIdent(value) {
        value = String(value == null ? '' : value);
        if (typeof CSS !== 'undefined' && CSS && typeof CSS.escape === 'function') {
            return CSS.escape(value);
        }
        return value.replace(/["\\]/g, '\\$&');
    }

    function diffTreeContainerElement(coords, idScope) {
        var info = diffTargetScopeInfo(idScope);
        if (!coords) return null;
        if (!info) {
            var normalContainer = treeContainerElement(coords, idScope);
            return normalContainer &&
                normalContainer.classList &&
                normalContainer.classList.contains &&
                normalContainer.classList.contains('diff-tree-wrap')
                ? normalContainer
                : null;
        }
        if (!hasDOM() || !document.querySelector) return null;
        if (info.fullscreen) {
            var side = document.querySelector(
                '.fullscreen-diff-side[data-side="' + cssEscapeIdent(info.side) + '"]'
            );
            return side && side.querySelector ? side.querySelector('.rule-tree-wrap.diff-tree-wrap') : null;
        }
        return null;
    }

    function diffNodeElementForTarget(target, coords, idScope) {
        var container = diffTreeContainerElement(coords, idScope);
        if (!container || !container.querySelector) return null;
        if (target && target.nodeId) {
            return container.querySelector(
                '[data-info-target-node-id="' + cssEscapeIdent(target.nodeId) + '"]'
            );
        }
        if (Array.isArray(target && target.path)) {
            return container.querySelector('[data-diff-pid="' + cssEscapeIdent(target.path.join('-')) + '"]');
        }
        return null;
    }

    function resolveDiffTreeTarget(target, coords, idScope) {
        var nodeEl = diffNodeElementForTarget(target, coords, idScope);
        if (!nodeEl) {
            return diffTreeContainerElement(coords, idScope)
                ? failed(target, 'diff-node-not-found')
                : null;
        }
        var row = treeRowElement(nodeEl);
        if (row && !elementHiddenByInlineStyle(row, diffTreeContainerElement(coords, idScope))) {
            return resolved(target, row, 'diff-tree-node-mounted');
        }
        return materializeNeeded(target, 'diff-tree-node-not-rendered');
    }

    function resolveDiffTreeSubtreeTarget(target, coords, idScope) {
        var container = diffTreeContainerElement(coords, idScope);
        if (!container) return null;
        var rootEl = diffNodeElementForTarget(target, coords, idScope);
        if (!rootEl) return failed(target, 'diff-subtree-not-found');
        var rows = diffMountedSubtreeRows(rootEl, container);
        if (!rows.length) return materializeNeeded(target, 'diff-subtree-not-rendered');
        return resolved(target, rows[0], 'diff-tree-subtree-mounted', rows, [{
            element: rows[0],
            rangeElements: rows
        }]);
    }

    function diffMountedSubtreeRows(rootEl, container) {
        var rows = [];
        if (!rootEl || !container || !container.querySelectorAll) return rows;
        var rootPid = rootEl.getAttribute ? rootEl.getAttribute('data-diff-pid') : '';
        if (!rootPid) return rows;
        var nodes = container.querySelectorAll('[data-diff-pid]');
        for (var i = 0; i < nodes.length; i++) {
            var pid = nodes[i].getAttribute ? nodes[i].getAttribute('data-diff-pid') : '';
            if (pid !== rootPid && pid.indexOf(rootPid + '-') !== 0) continue;
            var row = treeRowElement(nodes[i]);
            if (row && !elementHiddenByInlineStyle(row, container)) rows.push(row);
        }
        return uniqueElements(rows);
    }

    function treeMaterializerForRuleTarget(coords, idScope) {
        if (typeof treeMaterializerForContainer !== 'function') return null;
        return treeMaterializerForContainer(treeContainerElement(coords, idScope));
    }

    function rowRangeForInfoTreeTarget(target, materializer, coords) {
        if (!target || !materializer || !materializer.state ||
                typeof rowRangeForTarget !== 'function') {
            return null;
        }
        var rows = materializer.state.rows || [];
        var type = infoTargetIsSubtree(target) ? 'subtree' : 'node';
        return rowRangeForTarget(rows, { type: type, nodeRef: infoTargetNodeRef(target, coords) });
    }

    function materializerRangeMounted(materializer, range) {
        if (!materializer || !range || range.status !== 'resolved' ||
                typeof materializer.mountedRange !== 'function') {
            return false;
        }
        var mounted = materializer.mountedRange();
        return mounted && range.start >= mounted.start && range.end <= mounted.end;
    }

    function materializerOwnsTreeTarget(materializer) {
        return !!(materializer && materializer.state && materializer.state.rows);
    }

    function infoSectionElement(si, gi, ri, sectionIndex, idScope) {
        return byId('info-section-' + idScopePrefix(idScope) + si + '-' + gi + '-' + ri + '-' + sectionIndex);
    }

    function resolved(target, element, reason, elements, highlightSpecs) {
        return {
            status: 'resolved',
            target: target,
            element: element || null,
            elements: Array.isArray(elements) ? elements : (element ? [element] : []),
            highlightSpecs: Array.isArray(highlightSpecs) ? highlightSpecs : null,
            reason: reason || ''
        };
    }

    function materializeNeeded(target, reason) {
        return {
            status: 'materialize-needed',
            target: target || null,
            element: null,
            reason: reason || 'not-mounted'
        };
    }

    function failed(target, reason) {
        return {
            status: 'failed',
            target: target || null,
            element: null,
            reason: reason || 'invalid-target'
        };
    }

    function aggregateResolutionStatus(results) {
        var hasMaterializeNeeded = false;
        var hasResolved = false;
        for (var i = 0; i < results.length; i++) {
            if (results[i].status === 'resolved') hasResolved = true;
            else if (results[i].status === 'materialize-needed') hasMaterializeNeeded = true;
        }
        if (hasResolved) return 'resolved';
        if (hasMaterializeNeeded) return 'materialize-needed';
        return 'failed';
    }

    function appendResolvedElements(elements, result) {
        if (!result || result.status !== 'resolved') return;
        var resolvedElements = Array.isArray(result.elements) && result.elements.length ?
            result.elements :
            (result.element ? [result.element] : []);
        for (var i = 0; i < resolvedElements.length; i++) {
            if (resolvedElements[i]) elements.push(resolvedElements[i]);
        }
    }

    function appendResolvedHighlightSpecs(highlightSpecs, result) {
        if (!result || result.status !== 'resolved') return;
        if (Array.isArray(result.highlightSpecs) && result.highlightSpecs.length) {
            for (var i = 0; i < result.highlightSpecs.length; i++) {
                if (result.highlightSpecs[i]) highlightSpecs.push(result.highlightSpecs[i]);
            }
            return;
        }
        var elements = [];
        appendResolvedElements(elements, result);
        for (var j = 0; j < elements.length; j++) {
            highlightSpecs.push({ element: elements[j] });
        }
    }

    function resolveTreeTarget(target, context) {
        var coords = ruleCoordsForTarget(target, context);
        if (!coords) return failed(target, 'missing-rule-context');
        var idScope = idScopeOf(context);
        var pathNodeId = Array.isArray(target.path)
            ? normalTreeNodeId(coords.si, coords.gi, coords.ri, target.path, idScope)
            : '';
        if (Array.isArray(target.path) && !pathNodeId) return failed(target, 'invalid-tree-path');
        if (!Array.isArray(target.path) && !target.nodeId) return failed(target, 'invalid-node-target');

        var diffResolution = resolveDiffTreeTarget(target, coords, idScope);
        if (diffResolution) return diffResolution;

        var materializer = treeMaterializerForRuleTarget(coords, idScope);
        var range = rowRangeForInfoTreeTarget(target, materializer, coords);
        if (materializerOwnsTreeTarget(materializer)) {
            if (!range) return failed(target, 'tree-range-unavailable');
            if (range.status !== 'resolved') {
                if (materializer && typeof materializer.canRevealNodePath === 'function' &&
                        materializer.canRevealNodePath({
                            path: target.path,
                            nodeId: target.nodeId,
                            field: undefined
                        })) {
                    return materializeNeeded(target, range.reason || 'tree-node-not-found');
                }
                return failed(target, range.reason || 'tree-node-not-found');
            }
            if (!materializerRangeMounted(materializer, range)) {
                return materializeNeeded(target, 'tree-node-not-mounted');
            }
            var row = materializer.state.rows && materializer.state.rows[range.start];
            var rangeNodeId = row && row.pid ? normalTreeNodeIdForPid(coords, idScope, row.pid) : '';
            var rangeNodeEl = rangeNodeId ? byId(rangeNodeId) : null;
            if (rangeNodeEl) return resolved(target, treeRowElement(rangeNodeEl), 'tree-node-mounted');
            return materializeNeeded(target, 'tree-node-mounted-dom-missing');
        }

        var nodeEl = pathNodeId ? byId(pathNodeId) : null;
        if (nodeEl) return resolved(target, treeRowElement(nodeEl), 'tree-node-mounted');
        if (!ruleElement(coords.si, coords.gi, coords.ri, idScope)) return materializeNeeded(target, 'rule-not-mounted');
        return materializeNeeded(target, 'tree-node-not-rendered');
    }

    function elementHiddenByInlineStyle(el, stopAt) {
        for (var cur = el; cur && cur !== stopAt; cur = cur.parentElement) {
            if (cur.hidden) return true;
            if (cur.style && cur.style.display === 'none') return true;
        }
        return false;
    }

    function collectMountedSubtreeRows(ruleEl, coords, rootPid) {
        var rows = [];
        rootPid = String(rootPid || '');
        if (!rootPid || !ruleEl || !ruleEl.querySelectorAll) return rows;
        var allRows = ruleEl.querySelectorAll('.tree-node-row[data-node-id]');
        for (var i = 0; i < allRows.length; i++) {
            var nodeId = allRows[i].getAttribute ? allRows[i].getAttribute('data-node-id') : '';
            var ref = parseNormalTreeNodeId(nodeId);
            var pid = ref && typeof treePathId === 'function' ? treePathId(ref.path) : '';
            if (!ref ||
                ref.si !== coords.si ||
                ref.gi !== coords.gi ||
                ref.ri !== coords.ri ||
                (pid !== rootPid && pid.indexOf(rootPid + '-') !== 0) ||
                elementHiddenByInlineStyle(allRows[i], ruleEl)) {
                continue;
            }
            rows.push(allRows[i]);
        }
        return uniqueElements(rows);
    }

    function collectMaterializerSubtreeRows(materializer, coords, idScope, range, ruleEl) {
        var rows = [];
        if (!materializerOwnsTreeTarget(materializer) || !range ||
                range.status !== 'resolved' || !ruleEl) {
            return rows;
        }
        var modelRows = materializer.state.rows || [];
        for (var i = range.start; i < range.end; i++) {
            var row = modelRows[i];
            if (!row || row.kind !== 'node' || !row.pid) continue;
            var rowEl = treeRowElement(byId(normalTreeNodeIdForPid(coords, idScope, row.pid)));
            if (rowEl && !elementHiddenByInlineStyle(rowEl, ruleEl)) rows.push(rowEl);
        }
        return uniqueElements(rows);
    }

    function resolveTreeSubtreeTarget(target, context) {
        var coords = ruleCoordsForTarget(target, context);
        if (!coords) return failed(target, 'missing-rule-context');
        var idScope = idScopeOf(context);
        var pathNodeId = Array.isArray(target.path)
            ? normalTreeNodeId(coords.si, coords.gi, coords.ri, target.path, idScope)
            : '';
        if (Array.isArray(target.path) && !pathNodeId) return failed(target, 'invalid-tree-path');
        if (!Array.isArray(target.path) && !target.nodeId) return failed(target, 'invalid-node-target');

        var diffResolution = resolveDiffTreeSubtreeTarget(target, coords, idScope);
        if (diffResolution) return diffResolution;

        var ruleEl = ruleElement(coords.si, coords.gi, coords.ri, idScope);
        if (!ruleEl) return materializeNeeded(target, 'rule-not-mounted');

        var materializer = treeMaterializerForRuleTarget(coords, idScope);
        var range = rowRangeForInfoTreeTarget(target, materializer, coords);
        var rootPid = '';
        if (materializerOwnsTreeTarget(materializer)) {
            if (!range) return failed(target, 'tree-range-unavailable');
            if (range.status !== 'resolved') {
                if (materializer && typeof materializer.canRevealNodePath === 'function' &&
                        materializer.canRevealNodePath({
                            path: target.path,
                            nodeId: target.nodeId,
                            field: undefined
                        })) {
                    return materializeNeeded(target, range.reason || 'tree-subtree-not-found');
                }
                return failed(target, range.reason || 'tree-subtree-not-found');
            }
            if (!materializerRangeMounted(materializer, range)) {
                return materializeNeeded(target, 'tree-subtree-not-mounted');
            }
            var rangeRow = materializer.state.rows && materializer.state.rows[range.start];
            rootPid = rangeRow && rangeRow.pid || '';

            var materializerRows = collectMaterializerSubtreeRows(materializer, coords, idScope, range, ruleEl);
            if (materializerRows.length) {
                return resolved(target, materializerRows[0], 'tree-subtree-mounted', materializerRows, [{
                    element: materializerRows[0],
                    rangeElements: materializerRows
                }]);
            }
            return materializeNeeded(target, 'tree-subtree-root-not-rendered');
        }

        var rootEl = byId(rootPid ? normalTreeNodeIdForPid(coords, idScope, rootPid) : pathNodeId);
        if (!rootEl) return materializeNeeded(target, 'tree-subtree-root-not-rendered');
        var rootRow = treeRowElement(rootEl);
        var rows = collectMountedSubtreeRows(ruleEl, coords, rootPid || (typeof treePathId === 'function' ? treePathId(target.path) : ''));
        if (rootRow && !elementHiddenByInlineStyle(rootRow, ruleEl)) {
            rows = uniqueElements([rootRow].concat(rows));
        }
        if (!rows.length) {
            if (rootRow && !elementHiddenByInlineStyle(rootRow, ruleEl)) rows.push(rootRow);
        }
        if (rows.length) {
            return resolved(target, rows[0], 'tree-subtree-mounted', rows, [{
                element: rows[0],
                rangeElements: rows
            }]);
        }
        return materializeNeeded(target, 'tree-subtree-not-rendered');
    }

    function resolveInfoTarget(target, context) {
        if (!target || !target.type) return failed(target, 'missing-target-type');
        if (target.type === 'tree' || target.type === 'node') {
            return resolveTreeTarget(target, context);
        }
        if (target.type === 'tree-subtree' || target.type === 'subtree') {
            return resolveTreeSubtreeTarget(target, context);
        }
        return failed(target, 'unsupported-target-type');
    }

    function clampScroll(value, max) {
        value = Number(value);
        max = Number(max);
        if (!Number.isFinite(value)) value = 0;
        if (!Number.isFinite(max) || max <= 0) return 0;
        return Math.max(0, Math.min(max, value));
    }

    function elementContains(container, el) {
        if (!container || !el) return false;
        if (container === el) return true;
        if (container.contains) {
            try {
                return container.contains(el);
            } catch (err) {}
        }
        for (var cur = el; cur; cur = cur.parentElement) {
            if (cur === container) return true;
        }
        return false;
    }

    function isScrollableContainer(el) {
        if (!el) return false;
        var clientWidth = Number(el.clientWidth) || 0;
        var clientHeight = Number(el.clientHeight) || 0;
        if (clientWidth <= 0 || clientHeight <= 0) return false;
        var scrollWidth = Number(el.scrollWidth) || clientWidth;
        var scrollHeight = Number(el.scrollHeight) || clientHeight;
        return scrollWidth > clientWidth || scrollHeight > clientHeight;
    }

    function isGlobalTraceViewport(el) {
        return !!(el && el.classList && el.classList.contains && el.classList.contains('trace'));
    }

    function addUniqueContainer(containers, el) {
        if (!el || isGlobalTraceViewport(el) || !isScrollableContainer(el)) return false;
        for (var i = 0; i < containers.length; i++) {
            if (containers[i] === el) return false;
        }
        containers.push(el);
        return true;
    }

    function collectScrollableAncestors(el, containers) {
        for (var cur = el && el.parentElement; cur; cur = cur.parentElement) {
            addUniqueContainer(containers, cur);
        }
    }

    function addKnownTargetScrollContainers(targets, context, containers) {
        targets = Array.isArray(targets) ? targets : [];
        for (var i = 0; i < targets.length; i++) {
            var coords = ruleCoordsForTarget(targets[i], context);
            if (!coords) continue;
            var ruleEl = ruleElement(coords.si, coords.gi, coords.ri, idScopeOf(context));
            if (!ruleEl) continue;
            collectScrollableAncestors(ruleEl, containers);
            if (!ruleEl.querySelectorAll) continue;
            var known = ruleEl.querySelectorAll('.rule-tree-wrap');
            for (var j = 0; j < known.length; j++) addUniqueContainer(containers, known[j]);
        }
    }

    function scrollSnapshotFromContainers(containers) {
        var entries = [];
        for (var i = 0; i < containers.length; i++) {
            entries.push({
                el: containers[i],
                left: containers[i].scrollLeft || 0,
                top: containers[i].scrollTop || 0
            });
        }
        return entries.length ? { entries: entries } : null;
    }

    function captureSelectionScrollSnapshot(elements, targets, context) {
        var containers = [];
        addKnownTargetScrollContainers(targets, context, containers);
        elements = Array.isArray(elements) ? elements : [];
        for (var i = 0; i < elements.length; i++) {
            collectScrollableAncestors(elements[i], containers);
        }
        return scrollSnapshotFromContainers(containers);
    }

    function mergeScrollSnapshots(first, second) {
        if (!first) return second || null;
        if (!second) return first;
        var entries = first.entries ? first.entries.slice() : [];
        var secondEntries = second.entries || [];
        for (var i = 0; i < secondEntries.length; i++) {
            var exists = false;
            for (var j = 0; j < entries.length; j++) {
                if (entries[j].el === secondEntries[i].el) {
                    exists = true;
                    break;
                }
            }
            if (!exists) entries.push(secondEntries[i]);
        }
        return entries.length ? { entries: entries } : null;
    }

    function restoreSelectionScrollSnapshot(snapshot) {
        if (!snapshot || !snapshot.entries) return false;
        for (var i = 0; i < snapshot.entries.length; i++) {
            var entry = snapshot.entries[i];
            if (!entry || !entry.el) continue;
            entry.el.scrollLeft = entry.left || 0;
            entry.el.scrollTop = entry.top || 0;
        }
        return true;
    }

    function finiteRect(rect) {
        return rect &&
            Number.isFinite(rect.left) &&
            Number.isFinite(rect.right) &&
            Number.isFinite(rect.top) &&
            Number.isFinite(rect.bottom);
    }

    function rectWidth(rect) {
        return Math.max(0, rect.right - rect.left);
    }

    function rectHeight(rect) {
        return Math.max(0, rect.bottom - rect.top);
    }

    function intersectRects(left, right) {
        if (!finiteRect(left) || !finiteRect(right)) return null;
        var rect = {
            left: Math.max(left.left, right.left),
            right: Math.min(left.right, right.right),
            top: Math.max(left.top, right.top),
            bottom: Math.min(left.bottom, right.bottom)
        };
        if (rect.right < rect.left || rect.bottom < rect.top) return null;
        return rect;
    }

    function elementRect(el) {
        if (!el || !el.getBoundingClientRect) return null;
        var rect = null;
        try {
            rect = el.getBoundingClientRect();
        } catch (err) {
            return null;
        }
        if (!finiteRect(rect)) return null;
        return {
            left: rect.left,
            right: rect.right,
            top: rect.top,
            bottom: rect.bottom
        };
    }

    function treeMaterializerForTreeRow(el) {
        if (typeof treeMaterializerForContainer !== 'function') return null;
        var container = closestWithClass(el, 'rule-tree-wrap');
        return treeMaterializerForContainer(container);
    }

    function treeRowMaterializerRange(el, materializer) {
        if (!el || !materializer || !materializer.state ||
                typeof rowRangeForTarget !== 'function' ||
                typeof parseNormalTreeNodeId !== 'function') {
            return null;
        }
        var nodeId = el.getAttribute ? el.getAttribute('data-node-id') : '';
        var ref = parseNormalTreeNodeId(nodeId);
        if (!ref) return null;
        var rows = materializer.state.rows || [];
        var range = rowRangeForTarget(rows, { type: 'node', path: ref.path });
        return range && range.status === 'resolved' ? range : null;
    }

    function treeRowMaterializerLaneRect(el, rowRect) {
        var materializer = treeMaterializerForTreeRow(el);
        var range = treeRowMaterializerRange(el, materializer);
        if (!range || !materializer || typeof materializer.measureVisibleLane !== 'function') {
            return null;
        }
        var lane = materializer.measureVisibleLane(range);
        if (!lane || lane.status !== 'resolved' || lane.right <= lane.left) return null;
        return {
            left: lane.left,
            right: lane.right,
            top: rowRect.top,
            bottom: rowRect.bottom
        };
    }

    function rectsForSelector(el, selector) {
        var rects = [];
        if (!el || typeof el.querySelectorAll !== 'function') return rects;
        var nodes = el.querySelectorAll(selector);
        for (var i = 0; nodes && i < nodes.length; i++) {
            var rect = elementRect(nodes[i]);
            if (rect && finiteRect(rect) && rect.right > rect.left) rects.push(rect);
        }
        return rects;
    }

    function treeRowContentRect(el, rowRect) {
        var rects = rectsForSelector(el, '.tree-node-main > .tree-toggle, .tree-node-main > .tree-label');
        if (!rects.length) {
            rects = rectsForSelector(el, '.tree-toggle, .tree-label');
        }
        var rect = unionRects(rects);
        if (!rect) return null;
        return {
            left: rect.left,
            right: rect.right,
            top: rowRect.top,
            bottom: rowRect.bottom
        };
    }

    function firstElementForSelector(el, selector) {
        if (!el || typeof el.querySelector !== 'function') return null;
        try {
            return el.querySelector(selector);
        } catch (err) {
            return null;
        }
    }

    function treeRowReadabilityRect(el, rowRect) {
        if (!isTreeNodeRow(el)) return null;
        rowRect = rowRect || elementRect(el);
        if (!rowRect) return null;

        var rects = rectsForSelector(el, '.tree-node-main > .tree-toggle');
        if (!rects.length) rects = rectsForSelector(el, '.tree-toggle');

        var label = firstElementForSelector(el, '.tree-node-main > .tree-label') ||
            firstElementForSelector(el, '.tree-label');
        var labelRect = elementRect(label);
        if (labelRect) {
            rects.push({
                left: labelRect.left,
                right: Math.min(labelRect.right, labelRect.left + treeSelectionReadableLabelPx),
                top: rowRect.top,
                bottom: rowRect.bottom
            });
        }

        var rect = unionRects(rects);
        if (!rect) return null;
        return {
            left: rect.left,
            right: rect.right,
            top: rowRect.top,
            bottom: rowRect.bottom
        };
    }

    function treeRowHighlightRect(el) {
        var rect = elementRect(el);
        if (!rect || !isTreeNodeRow(el)) {
            return rect;
        }

        return treeRowContentRect(el, rect) || rect;
    }

    function selectionScrollContentRect(el) {
        if (!isTreeNodeRow(el)) return elementRect(el);
        return treeRowHighlightRect(el);
    }

    function selectionScrollHorizontalRect(el, rect) {
        if (!rect) return null;
        var horizontalRect = isTreeNodeRow(el)
            ? (treeRowReadabilityRect(el) || rect)
            : rect;
        if (!isTreeNodeRow(el)) return horizontalRect;
        return {
            left: horizontalRect.left - treeSelectionSideInsetPx,
            right: horizontalRect.right + treeSelectionSideInsetPx,
            top: horizontalRect.top,
            bottom: horizontalRect.bottom
        };
    }

    function treeRowPathDepth(el) {
        if (!el || typeof parseNormalTreeNodeId !== 'function') return null;
        var nodeId = el.getAttribute ? el.getAttribute('data-node-id') : '';
        var ref = parseNormalTreeNodeId(nodeId);
        return ref && ref.path && Number.isFinite(ref.path.length) ? ref.path.length : null;
    }

    function treeSelectionLogicalLeftInContainer(el, container, viewport) {
        var contentRect = selectionScrollContentRect(el);
        var horizontalRect = selectionScrollHorizontalRect(el, contentRect);
        if (!horizontalRect) return null;
        var logicalLeft = horizontalRect.left - viewport.left + (container.scrollLeft || 0);
        return Number.isFinite(logicalLeft) ? Math.max(0, logicalLeft) : null;
    }

    function treeSelectionRestingInset(container, viewport) {
        if (!container || !container.classList || !container.classList.contains('rule-tree-wrap')) return 0;
        if (Number.isFinite(container.__traceTreeSelectionRestingInset)) {
            return container.__traceTreeSelectionRestingInset;
        }
        if (typeof container.querySelectorAll !== 'function') return 0;

        var rows = container.querySelectorAll('.tree-node-row');
        var inset = null;
        var bestDepth = Infinity;
        for (var i = 0; rows && i < rows.length; i++) {
            var row = rows[i];
            if (!isTreeNodeRow(row)) continue;
            var depth = treeRowPathDepth(row);
            if (depth !== null && depth > 1) continue;
            var logicalLeft = treeSelectionLogicalLeftInContainer(row, container, viewport);
            if (logicalLeft === null) continue;
            if (depth !== null && depth < bestDepth) {
                bestDepth = depth;
                inset = logicalLeft;
            } else if (depth === bestDepth || (depth === null && bestDepth === Infinity)) {
                inset = inset === null ? logicalLeft : Math.min(inset, logicalLeft);
            }
        }

        if (inset === null) return 0;
        container.__traceTreeSelectionRestingInset = inset;
        return inset;
    }

    function treeRowHighlightClipRect(el) {
        if (!isTreeNodeRow(el)) return null;
        var wrap = closestWithClass(el, 'rule-tree-wrap');
        var wrapRect = elementRect(wrap);
        if (!wrapRect) return null;

        var clip = wrapRect;
        var root = closestWithClass(el, 'tree-root');
        var rootRect = elementRect(root);
        if (rootRect) {
            clip = intersectRects(clip, rootRect);
            if (!clip || rectWidth(clip) <= 1) {
                var edge = rootRect.left >= wrapRect.right ? wrapRect.right : wrapRect.left;
                return {
                    left: edge,
                    right: edge,
                    top: wrapRect.top,
                    bottom: wrapRect.bottom
                };
            }
        }
        var rowRect = elementRect(el);
        var laneRect = treeRowMaterializerLaneRect(el, rowRect);
        if (laneRect && clip) {
            clip = {
                left: Math.max(clip.left, laneRect.left),
                right: Math.max(laneRect.left, laneRect.right),
                top: clip.top,
                bottom: clip.bottom
            };
        }
        return clip;
    }

    function expandedRect(rect, margin) {
        margin = Number.isFinite(margin) ? margin : 0;
        return {
            left: rect.left - margin,
            right: rect.right + margin,
            top: rect.top - margin,
            bottom: rect.bottom + margin
        };
    }

    function unionRects(rects) {
        if (!rects.length) return null;
        var result = {
            left: rects[0].left,
            right: rects[0].right,
            top: rects[0].top,
            bottom: rects[0].bottom
        };
        for (var i = 1; i < rects.length; i++) {
            result.left = Math.min(result.left, rects[i].left);
            result.right = Math.max(result.right, rects[i].right);
            result.top = Math.min(result.top, rects[i].top);
            result.bottom = Math.max(result.bottom, rects[i].bottom);
        }
        return result;
    }

    function viewportRectForContainer(container) {
        var rect = elementRect(container);
        if (rect) return viewportRectAdjustedForTreeChrome(container, rect);
        var width = Number(container && container.clientWidth) || 0;
        var height = Number(container && container.clientHeight) || 0;
        return viewportRectAdjustedForTreeChrome(container, { left: 0, right: width, top: 0, bottom: height });
    }

    function viewportRectAdjustedForTreeChrome(container, rect) {
        if (!container || !rect ||
                !container.classList ||
                !container.classList.contains ||
                !container.classList.contains('rule-tree-wrap') ||
                typeof container.querySelector !== 'function') {
            return rect;
        }

        var header = container.querySelector('.tree-pinned-header.visible');
        var headerRect = elementRect(header);
        if (!headerRect ||
                headerRect.bottom <= rect.top ||
                headerRect.top >= rect.bottom) {
            return rect;
        }

        return {
            left: rect.left,
            right: rect.right,
            top: Math.min(rect.bottom, Math.max(rect.top, headerRect.bottom)),
            bottom: rect.bottom
        };
    }

    function rectAxisEnoughVisible(rectStart, rectEnd, viewportStart, viewportEnd) {
        var rectSpan = rectEnd - rectStart;
        var viewportSpan = viewportEnd - viewportStart;
        var requiredSpan = Math.min(rectSpan, viewportSpan);
        if (requiredSpan <= 0) return false;
        var visibleSpan = Math.max(0, Math.min(rectEnd, viewportEnd) - Math.max(rectStart, viewportStart));
        return visibleSpan / requiredSpan >= selectionVisibleRatioThreshold;
    }

    function rectHorizontallyEnoughVisible(rect, viewport) {
        return rectAxisEnoughVisible(rect.left, rect.right, viewport.left, viewport.right);
    }

    function rectVerticallyEnoughVisible(rect, viewport) {
        return rectAxisEnoughVisible(rect.top, rect.bottom, viewport.top, viewport.bottom);
    }

    function selectionRectEnoughVisible(rect, horizontalRect, viewport) {
        return rectHorizontallyEnoughVisible(horizontalRect || rect, viewport) &&
            rectVerticallyEnoughVisible(rect, viewport);
    }

    function rectFitsVertically(rect, viewport) {
        return rectHeight(rect) <= rectHeight(viewport);
    }

    function rectAxisDistance(rectStart, rectEnd, viewportStart, viewportEnd) {
        if (rectEnd < viewportStart) return viewportStart - rectEnd;
        if (rectStart > viewportEnd) return rectStart - viewportEnd;
        return 0;
    }

    function rectVerticalViewportDistance(rect, viewport) {
        return rectAxisDistance(rect.top, rect.bottom, viewport.top, viewport.bottom);
    }

    function rectHorizontalViewportDistance(rect, viewport) {
        return rectAxisDistance(rect.left, rect.right, viewport.left, viewport.right);
    }

    function closestSelectionItem(items, viewport) {
        var best = null;
        var bestVerticalDistance = Infinity;
        var bestHorizontalDistance = Infinity;
        var bestTopDistance = Infinity;
        for (var i = 0; i < items.length; i++) {
            var item = items[i];
            var rect = item && item.revealRect;
            if (!rect) continue;
            var verticalDistance = rectVerticalViewportDistance(rect, viewport);
            var horizontalRect = item.horizontalRect || item.contentRect || rect;
            var horizontalDistance = horizontalRect
                ? rectHorizontalViewportDistance(horizontalRect, viewport)
                : 0;
            var topDistance = Math.abs(rect.top - viewport.top);
            if (!best ||
                    verticalDistance < bestVerticalDistance ||
                    (verticalDistance === bestVerticalDistance &&
                        horizontalDistance < bestHorizontalDistance) ||
                    (verticalDistance === bestVerticalDistance &&
                        horizontalDistance === bestHorizontalDistance &&
                        topDistance < bestTopDistance)) {
                best = item;
                bestVerticalDistance = verticalDistance;
                bestHorizontalDistance = horizontalDistance;
                bestTopDistance = topDistance;
            }
        }
        return best;
    }

    function selectionItemsHaveVisibleTarget(items, viewport) {
        for (var i = 0; i < items.length; i++) {
            if (selectionRectEnoughVisible(items[i].revealRect, items[i].horizontalRect, viewport)) {
                return true;
            }
        }
        return false;
    }

    function selectionElementsHaveVisibleTarget(elements) {
        elements = uniqueElements(elements || []);
        if (!elements.length) return false;

        var containers = [];
        for (var i = 0; i < elements.length; i++) {
            collectScrollableAncestors(elements[i], containers);
        }

        for (var c = 0; c < containers.length; c++) {
            var container = containers[c];
            var viewport = viewportRectForContainer(container);
            var items = [];
            for (var e = 0; e < elements.length; e++) {
                if (!elementContains(container, elements[e])) continue;
                var contentRect = selectionScrollContentRect(elements[e]);
                if (!contentRect) continue;
                var horizontalRect = selectionScrollHorizontalRect(elements[e], contentRect);
                items.push({
                    revealRect: expandedRect(contentRect, selectionOutlineMarginPx),
                    horizontalRect: horizontalRect || contentRect
                });
            }
            if (items.length && selectionItemsHaveVisibleTarget(items, viewport)) return true;
        }
        return false;
    }

    function revealDelta(rect, viewport) {
        var dx = 0;
        var dy = 0;
        var rectW = rectWidth(rect);
        var rectH = rectHeight(rect);
        var viewportW = rectWidth(viewport);
        var viewportH = rectHeight(viewport);

        if (rectW > viewportW) {
            if (rect.left < viewport.left || rect.left >= viewport.right) dx = rect.left - viewport.left;
        } else if (rect.left < viewport.left) {
            dx = rect.left - viewport.left;
        } else if (rect.right > viewport.right) {
            dx = rect.right - viewport.right;
        }

        if (rectH > viewportH) {
            if (rect.top < viewport.top || rect.top >= viewport.bottom) dy = rect.top - viewport.top;
        } else if (rect.top < viewport.top) {
            dy = rect.top - viewport.top;
        } else if (rect.bottom > viewport.bottom) {
            dy = rect.bottom - viewport.bottom;
        }

        return { dx: dx, dy: dy };
    }

    function applyContainerScrollDelta(container, delta) {
        if (!container || !delta) return false;
        var oldLeft = container.scrollLeft || 0;
        var oldTop = container.scrollTop || 0;
        var maxLeft = Math.max(0, (Number(container.scrollWidth) || 0) - (Number(container.clientWidth) || 0));
        var maxTop = Math.max(0, (Number(container.scrollHeight) || 0) - (Number(container.clientHeight) || 0));
        container.scrollLeft = clampScroll(oldLeft + delta.dx, maxLeft);
        container.scrollTop = clampScroll(oldTop + delta.dy, maxTop);
        return (container.scrollLeft || 0) !== oldLeft || (container.scrollTop || 0) !== oldTop;
    }

    function scrollSelectionElementsIntoView(elements) {
        elements = uniqueElements(elements || []);
        if (!elements.length) return { scrolled: false, impossible: false };

        var containers = [];
        for (var i = 0; i < elements.length; i++) {
            collectScrollableAncestors(elements[i], containers);
        }

        var scrolled = false;
        var impossible = false;
        for (var c = 0; c < containers.length; c++) {
            var container = containers[c];
            var viewport = viewportRectForContainer(container);
            var rects = [];
            var items = [];

            for (var e = 0; e < elements.length; e++) {
                if (!elementContains(container, elements[e])) continue;
                var contentRect = selectionScrollContentRect(elements[e]);
                if (!contentRect) continue;
                var horizontalRect = selectionScrollHorizontalRect(elements[e], contentRect);
                var revealRect = expandedRect(contentRect, selectionOutlineMarginPx);
                rects.push(revealRect);
                items.push({
                    element: elements[e],
                    contentRect: contentRect,
                    revealRect: revealRect,
                    horizontalRect: horizontalRect || contentRect
                });
            }

            if (!items.length) continue;

            if (selectionItemsHaveVisibleTarget(items, viewport)) continue;

            var union = unionRects(rects);
            var anchorItem = closestSelectionItem(items, viewport) || items[0];
            var targetRect = union;
            if (!union) continue;
            if (!rectFitsVertically(union, viewport)) {
                impossible = true;
                targetRect = anchorItem && anchorItem.revealRect ? anchorItem.revealRect : rects[0];
            }

            var delta = { dx: 0, dy: 0 };
            if (targetRect && !rectVerticallyEnoughVisible(targetRect, viewport)) {
                delta.dy = targetRect.top - viewport.top;
            }

            var horizontalAnchorRect = anchorItem && anchorItem.horizontalRect;
            if (horizontalAnchorRect && !rectHorizontallyEnoughVisible(horizontalAnchorRect, viewport)) {
                delta.dx = horizontalAnchorRect.left - viewport.left -
                    treeSelectionRestingInset(container, viewport);
            }
            scrolled = applyContainerScrollDelta(container, delta) || scrolled;
        }

        return { scrolled: scrolled, impossible: impossible };
    }

    function scrollElementIntoViewByAncestors(el) {
        if (!el) return { scrolled: false };
        var containers = [];
        collectScrollableAncestors(el, containers);
        var rect = elementRect(el);
        if (!rect) return { scrolled: false };

        var scrolled = false;
        for (var i = 0; i < containers.length; i++) {
            if (!elementContains(containers[i], el)) continue;
            scrolled = applyContainerScrollDelta(
                containers[i],
                revealDelta(rect, viewportRectForContainer(containers[i]))
            ) || scrolled;
        }
        return { scrolled: scrolled };
    }

    function resolveSelectionTargets(targets, context) {
        targets = Array.isArray(targets) ? targets : [];
        var results = [];
        var elements = [];
        var highlightSpecs = [];
        for (var i = 0; i < targets.length; i++) {
            var result = resolveInfoTarget(targets[i], context);
            results.push(result);
            appendResolvedElements(elements, result);
            appendResolvedHighlightSpecs(highlightSpecs, result);
        }
        elements = uniqueElements(elements);
        return {
            status: aggregateResolutionStatus(results),
            results: results,
            elements: elements,
            highlightSpecs: normalizeHighlightSpecs(highlightSpecs)
        };
    }

    function materializeTreeTargetWithMaterializer(target, coords, idScope) {
        if (!infoTargetIsNodeLike(target)) return false;
        var materializer = treeMaterializerForRuleTarget(coords, idScope);
        var range = rowRangeForInfoTreeTarget(target, materializer, coords);
        if ((!range || range.status !== 'resolved') &&
                materializer && typeof materializer.revealNodePath === 'function') {
            var reveal = materializer.revealNodePath({
                path: target.path,
                nodeId: target.nodeId,
                field: undefined
            });
            if (reveal && reveal.status === 'resolved') {
                materializer = treeMaterializerForRuleTarget(coords, idScope);
                range = rowRangeForInfoTreeTarget(target, materializer, coords);
            }
        }
        if (!range || range.status !== 'resolved' ||
                !materializer || typeof materializer.scrollRowRangeIntoView !== 'function') {
            return false;
        }
        var scroll = materializer.scrollRowRangeIntoView(range, { block: 'nearest' });
        return !!(scroll && scroll.status === 'resolved');
    }

    function materializeTreeTarget(target, coords, idScope) {
        return materializeTreeTargetWithMaterializer(target, coords, idScope);
    }

    function materializeTargetWouldScroll(target, context) {
        if (!infoTargetIsNodeLike(target)) return true;
        var coords = ruleCoordsForTarget(target, context);
        if (!coords) return true;
        var materializer = treeMaterializerForRuleTarget(coords, idScopeOf(context));
        var range = rowRangeForInfoTreeTarget(target, materializer, coords);
        if (!range || range.status !== 'resolved' ||
                !materializer ||
                typeof materializer.measureRowRange !== 'function' ||
                typeof materializer.viewportRange !== 'function') {
            return true;
        }

        var bounds = materializer.measureRowRange(range);
        var viewport = materializer.viewportRange();
        if (!bounds || bounds.status !== 'resolved' || !viewport) return true;

        var top = Number(bounds.top);
        var bottom = Number(bounds.bottom);
        var viewportTop = Number(viewport.top);
        var viewportBottom = Number(viewport.bottom);
        if (!Number.isFinite(top) || !Number.isFinite(bottom) ||
                !Number.isFinite(viewportTop) || !Number.isFinite(viewportBottom)) {
            return true;
        }
        return top < viewportTop || bottom > viewportBottom;
    }

    function materializeTarget(target, context) {
        if (!target) return failed(target, 'missing-target');
        var coords = ruleCoordsForTarget(target, context);
        if (!coords) return failed(target, 'invalid-rule-ref');
        var idScope = idScopeOf(context);

        if (idScope === 'fs') {
            if (materializeTreeTarget(target, coords, idScope)) {
                return resolveInfoTarget(target, context);
            }
            return resolveInfoTarget(target, context);
        }

        if (materializeTreeTarget(target, coords, idScope)) {
            return resolveInfoTarget(target, context);
        }

        return resolveInfoTarget(target, context);
    }

    function targetAlreadyAttempted(target, attempted) {
        for (var i = 0; i < attempted.length; i++) {
            if (attempted[i] === target) return true;
        }
        return false;
    }

    function chooseMaterializeTarget(primary, resolution, attempted) {
        attempted = attempted || [];
        if (primary) {
            for (var i = 0; i < resolution.results.length; i++) {
                if (resolution.results[i].target === primary &&
                    resolution.results[i].status === 'materialize-needed' &&
                    !targetAlreadyAttempted(primary, attempted)) {
                    return primary;
                }
            }
        }

        for (var j = 0; j < resolution.results.length; j++) {
            if (resolution.results[j].status === 'materialize-needed' &&
                !targetAlreadyAttempted(resolution.results[j].target, attempted)) {
                return resolution.results[j].target;
            }
        }
        return null;
    }

    function materializeSelectionTargets(targets, primary, context, options) {
        var resolution = resolveSelectionTargets(targets, context);
        var materialized = false;
        var materializeResult = null;
        var attempted = [];
        var previewMode = options && options.mode === 'preview';
        for (var guard = 0; guard < targets.length; guard++) {
            var materialize = chooseMaterializeTarget(primary, resolution, attempted);
            if (!materialize) break;
            if (previewMode &&
                    selectionElementsHaveVisibleTarget(resolution.elements) &&
                    materializeTargetWouldScroll(materialize, context)) {
                break;
            }
            attempted.push(materialize);
            materializeResult = materializeTarget(materialize, context);
            if (materializeResult && materializeResult.status === 'resolved') {
                materialized = true;
            }
            resolution = resolveSelectionTargets(targets, context);
        }

        return {
            resolution: resolution,
            materialized: materialized,
            materializeResult: materializeResult
        };
    }

    function primaryElementForSelection(primary, resolution) {
        if (primary) {
            for (var i = 0; i < resolution.results.length; i++) {
                if (resolution.results[i].target === primary &&
                    resolution.results[i].status === 'resolved') {
                    var elements = [];
                    appendResolvedElements(elements, resolution.results[i]);
                    if (elements.length) return elements[0];
                }
            }
        }
        return resolution.elements.length ? resolution.elements[0] : null;
    }

    function selectionActivation(primary, targets, context, options) {
        var allTargets = previewTargets(primary, targets);
        if (!allTargets.length) return { status: 'failed', reason: 'missing-targets', count: 0 };

        var beforeScroll = captureSelectionScrollSnapshot([], allTargets, context);
        var materialized = materializeSelectionTargets(allTargets, primary, context, options);
        var resolution = materialized.resolution;
        var afterScroll = captureSelectionScrollSnapshot(resolution.elements, allTargets, context);
        var scrollSnapshot = mergeScrollSnapshots(beforeScroll, afterScroll);
        var primaryElement = primaryElementForSelection(primary, resolution);
        var scroll = scrollSelectionElementsIntoView(resolution.elements);

        return {
            status: resolution.status,
            count: resolution.elements.length,
            results: resolution.results,
            elements: resolution.elements,
            highlightSpecs: resolution.highlightSpecs,
            primaryElement: primaryElement,
            materialized: !!materialized.materialized,
            materializeResult: materialized.materializeResult,
            scrolled: !!scroll.scrolled,
            impossible: !!scroll.impossible,
            scrollSnapshot: scrollSnapshot
        };
    }

    function blinkElements(elements, options) {
        options = options || {};
        var session = options.session;
        if (session === undefined) session = beginBlinkSession();
        if (session !== blinkEpoch) return { status: 'failed', reason: 'stale-session', session: session, count: 0 };

        var blinkClass = options.className || targetBlinkClass;
        var blinkEls = uniqueElements(elements || []);
        for (var i = 0; i < blinkEls.length; i++) {
            removeClass(blinkEls[i], blinkClass);
            forceAnimationRestart(blinkEls[i]);
            addClass(blinkEls[i], blinkClass);
            activeBlinkEls.push(blinkEls[i]);
        }

        var timer = timerFromOptions(options);
        var duration = Number.isFinite(options.durationMs) ? options.durationMs : defaultBlinkMs;
        if (timer && blinkEls.length) {
            timer(function() {
                if (session !== blinkEpoch) return;
                for (var j = 0; j < blinkEls.length; j++) {
                    removeClass(blinkEls[j], blinkClass);
                }
                activeBlinkEls = activeBlinkEls.filter(function(el) {
                    for (var k = 0; k < blinkEls.length; k++) {
                        if (blinkEls[k] === el) return false;
                    }
                    return true;
                });
            }, duration);
        }

        return { status: blinkEls.length ? 'resolved' : 'failed', session: session, count: blinkEls.length };
    }

    function targetMatchesTreeNode(target, treeRef, context) {
        if (!target || !treeRef) return false;
        var coords = ruleCoordsForTarget(target, context);
        if (!coords ||
            coords.si !== treeRef.si ||
            coords.gi !== treeRef.gi ||
            coords.ri !== treeRef.ri) {
            return false;
        }
        if (target.type === 'tree') return sameNumberList(target.path, treeRef.path);
        if (target.type === 'tree-subtree') return numberListStartsWith(treeRef.path, target.path);
        if (target.type === 'node' || target.type === 'subtree') {
            var materializer = treeMaterializerForRuleTarget(coords, idScopeOf(context));
            var range = rowRangeForInfoTreeTarget(target, materializer, coords);
            var row = range && range.status === 'resolved' &&
                materializer && materializer.state && materializer.state.rows
                ? materializer.state.rows[range.start]
                : null;
            if (!row || !row.path) return false;
            if (target.type === 'subtree') return numberListStartsWith(treeRef.path, row.path);
            return sameNumberList(row.path, treeRef.path);
        }
        return false;
    }

    function hostTargetsMatchTreeNode(host, treeRef) {
        var context = contextFromInfoTargetHost(host);
        var targets = targetsFromHost(host);
        var primary = primaryTargetFromHost(host);
        if (primary) targets = [primary].concat(targets);
        for (var i = 0; i < targets.length; i++) {
            if (targetMatchesTreeNode(targets[i], treeRef, context)) return true;
        }
        return false;
    }

    function infoTargetHostsForTreeNode(ruleEl, treeRef) {
        var result = [];
        if (!ruleEl || !ruleEl.querySelectorAll || !treeRef) return result;
        var hosts = ruleEl.querySelectorAll('[data-info-targets], [data-info-primary-target]');
        for (var i = 0; i < hosts.length; i++) {
            if (hostTargetsMatchTreeNode(hosts[i], treeRef)) result.push(hosts[i]);
        }
        return result;
    }

    function graphElementsForTreeNode(ruleEl, nodeId) {
        var result = [];
        if (!ruleEl || !ruleEl.querySelectorAll) return result;
        var allRefEls = ruleEl.querySelectorAll('[data-trace-ref]');
        for (var i = 0; i < allRefEls.length; i++) {
            var nodeIds = traceRefNodeIds(allRefEls[i].getAttribute('data-trace-ref') || '');
            for (var j = 0; j < nodeIds.length; j++) {
                if (nodeIds[j] === nodeId) {
                    result.push(allRefEls[i]);
                    break;
                }
            }
        }
        return result;
    }

    function reverseBlinkElementVisible(el) {
        if (!el) return false;
        if (el.hidden) return false;
        if (el.closest) {
            try {
                if (el.closest('[hidden]')) return false;
            } catch (err) {}
        }
        if (el.getClientRects) {
            try {
                return el.getClientRects().length > 0;
            } catch (err) {}
        }
        return true;
    }

    function reverseBlinkElementsForTreeNode(ruleEl, nodeId, treeRef) {
        if (!ruleEl) return [];
        return uniqueElements(
            graphElementsForTreeNode(ruleEl, nodeId)
                .concat(infoTargetHostsForTreeNode(ruleEl, treeRef))
        ).filter(reverseBlinkElementVisible);
    }

    function reverseBlinkTreeNode(nodeId, ev, options) {
        var treeRef = parseNormalTreeNodeId(nodeId);
        if (!treeRef) return false;
        var idScope = treeNodeIdScope(nodeId);
        var ruleEl = ruleElement(treeRef.si, treeRef.gi, treeRef.ri, idScope);
        if (!ruleEl) return false;

        var elements = reverseBlinkElementsForTreeNode(ruleEl, nodeId, treeRef);
        if (!elements.length) return false;

        stopEvent(ev);
        var result = blinkElements(elements, options || {});
        return result.count > 0;
    }

    function previewTargets(primary, targets) {
        var result = [];
        if (primary) result.push(primary);
        targets = Array.isArray(targets) ? targets : [];
        for (var i = 0; i < targets.length; i++) result.push(targets[i]);
        return result;
    }

    function hoverModeForHost(el) {
        var mode = el && el.getAttribute ? el.getAttribute('data-info-target-hover-mode') : '';
        return mode === 'highlight' ? 'highlight' : 'preview';
    }

    function handleHighlightInfoTargetMouseOver(el, primary, targets) {
        closeOtherHoverPreview(el);
        var session = beginHoverSession();
        var context = contextFromInfoTargetHost(el);
        var resolution = resolveSelectionTargets(previewTargets(primary, targets), context);

        el.__infoTargetHoverSession = session;
        el.__infoTargetHoverPreview = null;

        if (!resolution.elements.length) {
            // Target preview is best-effort: unresolved or unmounted targets
            // should leave no substitute highlight behind.
            return true;
        }

        var result = highlightElements(resolution.highlightSpecs, { session: session });
        return result.count > 0;
    }

    function handlePreviewInfoTargetMouseOver(el) {
        var primary = primaryTargetFromHost(el);
        var targets = targetsFromHost(el);
        var allTargets = previewTargets(primary, targets);
        if (!allTargets.length) return false;

        if (hoverModeForHost(el) === 'highlight') {
            return handleHighlightInfoTargetMouseOver(el, primary, targets);
        }

        if (activeHoverPreview && activeHoverPreview.host === el) {
            if (activeHoverPreview.session === hoverEpoch) return true;
            closeActiveHoverPreview({ restore: true, clearHighlights: false });
        }
        closeOtherHoverPreview(el);

        var session = beginHoverSession();
        var context = contextFromInfoTargetHost(el);
        var activation = selectionActivation(primary, targets, context, { mode: 'preview' });
        var shouldRestore = activation.scrolled || activation.materialized;

        el.__infoTargetHoverSession = session;
        el.__infoTargetHoverPreview = {
            host: el,
            session: session,
            scrollSnapshot: activation.scrollSnapshot,
            restoreOnLeave: shouldRestore
        };
        activeHoverPreview = el.__infoTargetHoverPreview;

        if (!activation.count) {
            restoreSelectionScrollSnapshot(activation.scrollSnapshot);
            clearHostHoverPreview(el.__infoTargetHoverPreview);
            if (activeHoverPreview && activeHoverPreview.host === el) activeHoverPreview = null;
            el.__infoTargetHoverPreview = null;
            return true;
        }

        var result = highlightElements(activation.highlightSpecs, { session: session });
        if (shouldRestore && el.isConnected === false) {
            finishInfoTargetHover(el);
        }
        return result.count > 0;
    }

    function finishInfoTargetHover(el) {
        var session = el.__infoTargetHoverSession;

        var preview = el.__infoTargetHoverPreview;
        if (preview) {
            if (activeHoverPreview !== preview) return false;
            session = preview.session;
            if (session !== undefined && session !== hoverEpoch) return false;
            closeActiveHoverPreview({ restore: true, clearHighlights: false });
        } else if (session !== undefined && session !== hoverEpoch) {
            return false;
        }
        return clearTargetHighlights(session);
    }

    function handleInfoTargetClick(target, ev, options) {
        var el = closestInfoTargetHost(target);
        if (!el) return false;

        var primary = primaryTargetFromHost(el);
        var targets = targetsFromHost(el);
        var allTargets = previewTargets(primary, targets);
        if (!allTargets.length) return false;
        var chosen = primary || (targets.length === 1 ? targets[0] : null);

        stopEvent(ev);
        commitHoverPreviewForHost(el);
        var context = contextFromInfoTargetHost(el);

        var activation = selectionActivation(primary || chosen, targets, context);
        clearBlinks();
        if (activation.count) {
            pulseAppliedSelection(el, activation.highlightSpecs, options || {});
        }
        return activation.status === 'resolved' || activation.status === 'materialize-needed';
    }

    function ensureRuleInfoVisible(si, gi, ri) {
        var state = null;
        try { state = ruleState(si, gi, ri); } catch (err) {}
        if (!state || state.info) return false;

        var change = TraceState.setRuleFeature(uiState(), si, gi, ri, 'info', true);
        if (change.changed) {
            renderRuleFeatureChange('info', si, gi, ri);
            markCollapsedSearchIndicatorsDirty();
            refreshSearchStateForCurrentLayout();
            updateBulkDetailControls();
            return true;
        }
        return false;
    }

    function findInfoTabButton(si, gi, ri, tabId, options) {
        options = options || {};
        tabId = String(tabId || '');
        if (!tabId) return null;

        var scope = options.container || ruleElement(si, gi, ri, options.idScope);
        if (!scope || !scope.querySelectorAll) return null;
        var buttons = scope.querySelectorAll('.info-tab');
        for (var i = 0; i < buttons.length; i++) {
            if ((buttons[i].getAttribute && buttons[i].getAttribute('data-tab-id')) === tabId) {
                return buttons[i];
            }
        }
        return null;
    }

    function scrollActiveTabIntoView(si, gi, ri, tabId, options) {
        var tab = findInfoTabButton(si, gi, ri, tabId, options);
        if (!tab) return false;

        // Scroll only the tab strip itself: this runs during lazy panel
        // renders, where native scrollIntoView would also scroll the trace
        // viewport and ancestor panes.
        var strip = closestWithClass(tab, 'info-tabs');
        if (!strip) return false;
        var stripRect = elementRect(strip);
        var tabRect = elementRect(tab);
        if (!stripRect || !tabRect) return false;

        var delta = revealDelta(tabRect, stripRect);
        if (delta.dx) {
            var maxLeft = Math.max(0, (Number(strip.scrollWidth) || 0) - (Number(strip.clientWidth) || 0));
            strip.scrollLeft = clampScroll((strip.scrollLeft || 0) + delta.dx, maxLeft);
        }
        return true;
    }

    return {
        clearBlinks: clearBlinks,
        clearGraphHighlights: clearGraphHighlights,
        clearTargetHighlights: clearTargetHighlights,
        commitInfoTargetPreview: commitInfoTargetPreview,
        handleInfoTargetClick: handleInfoTargetClick,
        handleInfoTargetMouseOut: handleInfoTargetMouseOut,
        handleInfoTargetMouseOver: handleInfoTargetMouseOver,
        infoTargetPreviewRestores: infoTargetPreviewRestores,
        highlightElements: highlightElements,
        highlightInfoTargets: highlightInfoTargets,
        handleTraceRefMouseOut: handleTraceRefMouseOut,
        handleTraceRefMouseOver: handleTraceRefMouseOver,
        reapplyActiveTargetHighlights: reapplyActiveTargetHighlights,
        reverseBlinkTreeNode: reverseBlinkTreeNode,
        resolveInfoTarget: resolveInfoTarget,
        scrollActiveTabIntoView: scrollActiveTabIntoView,
        traceRefNodeIdForTarget: traceRefNodeIdForTarget,
        traceRefNodeIds: traceRefNodeIds
    };
})();
