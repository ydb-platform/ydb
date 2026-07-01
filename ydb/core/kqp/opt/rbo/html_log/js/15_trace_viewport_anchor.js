function scrollTargetForRule(si, gi, ri) {
    var interval = ruleLayoutInterval(si, gi, ri);
    if (!interval || !hasDOM()) return null;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return null;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    var target = interval.left + interval.width / 2 - viewport / 2;
    var max = maxTraceScrollForRestore(traceEl, getTraceLayoutModel(), viewport);
    return Math.max(0, Math.min(max, target));
}

function ruleIntersectsTraceViewport(si, gi, ri) {
    if (!hasDOM()) return true;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return true;

    var interval = ruleLayoutInterval(si, gi, ri);
    if (!interval) return false;

    var width = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(width) || width <= 0) return true;

    var left = traceEl.scrollLeft || 0;
    var right = left + width;
    return interval.right > left && interval.left < right;
}

function scrollRuleIntoView(si, gi, ri) {
    if (!hasDOM()) return false;

    var traceEl = document.querySelector('.trace');
    var target = scrollTargetForRule(si, gi, ri);
    if (!traceEl || target === null) return false;

    var model = getTraceLayoutModel();
    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    setTraceScrollLeft(traceEl, target, {
        maxScrollLeft: maxTraceScrollForRestore(traceEl, model, viewport),
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: true
    });
    return true;
}

function centerRuleInTrace(ref) {
    if (!ref) return false;
    return scrollRuleIntoView(ref.si, ref.gi, ref.ri);
}

function restoreRuleAnchorWithFallback(anchor, acceptAnchor, fallback) {
    var keptAnchor = restoreRuleViewportAnchor(anchor);
    if (keptAnchor && (!acceptAnchor || acceptAnchor())) return true;
    return fallback ? fallback() : false;
}

function cancelTraceWheelGlide() {
    /* Pending coarse-wheel glide is denominated in pre-transition pixels;
       letting it keep applying after an anchored reposition drags the view
       off the restored anchor. */
    if (typeof resetWheelScrollRuntime === 'function') resetWheelScrollRuntime();
}

function roundTraceScrollToDevicePixels(value) {
    var dpr = typeof window !== 'undefined' ? Number(window.devicePixelRatio) : 1;
    if (!Number.isFinite(dpr) || dpr <= 0) dpr = 1;
    return Math.round(value * dpr) / dpr;
}

function traceAnchorOffsetForRestoredWidth(anchor, newWidth) {
    var offset = Number(anchor && anchor.offset);
    if (!Number.isFinite(offset)) return 0;
    if (offset >= 0) return offset;

    var oldWidth = Number(anchor && anchor.width);
    newWidth = Number(newWidth);
    if (!Number.isFinite(oldWidth) || oldWidth <= 0 ||
        !Number.isFinite(newWidth) || newWidth <= 0) {
        return offset;
    }

    var local = Math.min(-offset, oldWidth);
    if (local <= newWidth) return offset;
    /* The viewport started inside the anchor and the anchor shrank past that
       point; pinning the left edge would place the whole anchor offscreen-left
       and the view would appear to jump forward. Keep the proportional
       interior point instead. */
    return -(local * newWidth / oldWidth);
}

function captureRuleViewportAnchor(ref) {
    if (!ref || !hasDOM() || !document.querySelector) return null;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return null;

    var interval = ruleLayoutInterval(ref.si, ref.gi, ref.ri);
    if (!interval) return null;

    return {
        si: ref.si,
        gi: ref.gi,
        ri: ref.ri,
        offset: interval.left - (traceEl.scrollLeft || 0)
    };
}

function restoreRuleViewportAnchor(anchor) {
    if (!anchor || !hasDOM() || !document.querySelector) return false;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return false;

    var model = rebuildTraceLayoutModel();
    var interval = ruleAnchorInterval(model, anchor.si, anchor.gi, anchor.ri);
    if (!interval) return false;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(viewport) || viewport <= 0) return false;

    refreshVirtualRowsNow();
    model = getTraceLayoutModel();
    interval = ruleAnchorInterval(model, anchor.si, anchor.gi, anchor.ri);
    if (!interval) return false;

    var max = maxTraceScrollForRestore(traceEl, model, viewport);
    setTraceScrollLeft(traceEl, interval.left - anchor.offset, {
        maxScrollLeft: max,
        roundToDevicePixels: true,
        cancelWheelGlide: true,
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: true
    });
    return true;
}

function ruleFullyVisibleInTrace(ref) {
    if (!ref || !hasDOM() || !document.querySelector) return false;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return false;

    var interval = ruleLayoutInterval(ref.si, ref.gi, ref.ri);
    if (!interval) return false;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(viewport) || viewport <= 0) return false;

    var left = traceEl.scrollLeft || 0;
    var right = left + viewport;
    return interval.left >= left - VIEWPORT_ANCHOR_EPSILON &&
           interval.right <= right + VIEWPORT_ANCHOR_EPSILON;
}

function ruleVisibleEnoughForNavigation(ref) {
    if (!ref || !hasDOM() || !document.querySelector) return false;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return false;

    var interval = ruleLayoutInterval(ref.si, ref.gi, ref.ri);
    if (!interval) return false;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(viewport) || viewport <= 0) return false;

    if (interval.width <= viewport + VIEWPORT_ANCHOR_EPSILON) {
        return ruleFullyVisibleInTrace(ref);
    }

    var left = traceEl.scrollLeft || 0;
    var right = left + viewport;
    var visible = Math.min(interval.right, right) - Math.max(interval.left, left);
    return visible >= viewport * 0.85 - VIEWPORT_ANCHOR_EPSILON;
}

function diffPairFullyVisibleInTrace() {
    return ruleFullyVisibleInTrace(diffRuntime().diffA) && ruleFullyVisibleInTrace(diffRuntime().diffB);
}

function ruleIntervalFullyVisibleAtScrollLeft(interval, scrollLeft, viewport) {
    if (!interval ||
            !Number.isFinite(scrollLeft) ||
            !Number.isFinite(viewport) ||
            viewport <= 0) {
        return false;
    }

    var right = scrollLeft + viewport;
    return interval.left >= scrollLeft - VIEWPORT_ANCHOR_EPSILON &&
           interval.right <= right + VIEWPORT_ANCHOR_EPSILON;
}

function diffPairFullyVisibleAtScrollLeft(scrollLeft, model, viewport) {
    if (!diffRuntime().diffA || !diffRuntime().diffB) return false;

    var a = ruleAnchorInterval(model, diffRuntime().diffA.si, diffRuntime().diffA.gi, diffRuntime().diffA.ri);
    var b = ruleAnchorInterval(model, diffRuntime().diffB.si, diffRuntime().diffB.gi, diffRuntime().diffB.ri);
    return ruleIntervalFullyVisibleAtScrollLeft(a, scrollLeft, viewport) &&
           ruleIntervalFullyVisibleAtScrollLeft(b, scrollLeft, viewport);
}

function diffCenterScrollTarget() {
    if (!diffRuntime().diffA || !diffRuntime().diffB || !hasDOM()) return null;
    if (!document.querySelector) return null;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return null;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(viewport) || viewport <= 0) return null;

    var model = rebuildTraceLayoutModel();
    var a = ruleAnchorInterval(model, diffRuntime().diffA.si, diffRuntime().diffA.gi, diffRuntime().diffA.ri);
    var b = ruleAnchorInterval(model, diffRuntime().diffB.si, diffRuntime().diffB.gi, diffRuntime().diffB.ri);
    if (!a || !b) return null;

    var center = ((a.left + a.width / 2) + (b.left + b.width / 2)) / 2;
    var max = traceRestorableMaxScrollLeft(model, traceEl);
    return Math.max(0, Math.min(max, center - viewport / 2));
}

function centerDiffPairInTrace() {
    var traceEl = hasDOM() && document.querySelector ? document.querySelector('.trace') : null;
    var target = diffCenterScrollTarget();
    if (!traceEl || target === null) return false;

    var model = getTraceLayoutModel();
    setTraceScrollLeft(traceEl, target, {
        maxScrollLeft: maxModelScrollLeft(model, traceEl),
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: true
    });
    return true;
}

function centerDiffPairInTraceIfFullyVisible() {
    var traceEl = hasDOM() && document.querySelector ? document.querySelector('.trace') : null;
    var target = diffCenterScrollTarget();
    if (!traceEl || target === null) return false;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(viewport) || viewport <= 0) return false;

    var model = getTraceLayoutModel();
    if (!diffPairFullyVisibleAtScrollLeft(target, model, viewport)) return false;

    setTraceScrollLeft(traceEl, target, {
        maxScrollLeft: maxModelScrollLeft(model, traceEl),
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: true
    });
    return true;
}

function restoreDiffActivationViewport(anchor) {
    var keptAnchor = restoreRuleViewportAnchor(anchor);
    if (keptAnchor && diffPairFullyVisibleInTrace()) return true;
    if (centerDiffPairInTraceIfFullyVisible()) return true;
    return keptAnchor ? restoreRuleViewportAnchor(anchor) : false;
}

function scrollStageIntoView(si) {
    if (!hasDOM()) return;

    var traceEl = document.querySelector('.trace');
    var model = getTraceLayoutModel();
    var stage = model.stages[si];
    if (!traceEl || !stage) return;

    var max = maxModelScrollLeft(model, traceEl);
    setTraceScrollLeft(traceEl, stage.left, {
        maxScrollLeft: max,
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: true
    });
}

function scrollGroupIntoView(si, gi) {
    if (!hasDOM()) return;

    var traceEl = document.querySelector('.trace');
    var model = getTraceLayoutModel();
    var group = groupSearchMatchInterval(si, gi, model);
    if (!traceEl || !group) return;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    var target = group.left + group.width / 2 - viewport / 2;
    var max = maxModelScrollLeft(model, traceEl);
    setTraceScrollLeft(traceEl, target, {
        maxScrollLeft: max,
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: true
    });
}

function groupLayoutInterval(model, si, gi) {
    if (!model) return null;

    var indexed = model.groups && model.groups[si + '-' + gi];
    if (indexed) {
        return {
            kind: 'group',
            si: indexed.si,
            gi: indexed.gi,
            left: indexed.left,
            right: indexed.right,
            width: indexed.width
        };
    }

    for (var i = 0; i < model.rows.length; i++) {
        var row = model.rows[i];
        if (row.gi !== null && row.gi !== undefined) continue;

        for (var j = 0; j < row.items.length; j++) {
            var item = row.items[j];
            if (item.kind === 'group' && item.si === si && item.gi === gi) {
                return {
                    kind: 'group',
                    si: si,
                    gi: gi,
                    left: item.globalLeft,
                    right: item.globalLeft + item.width,
                    width: item.width
                };
            }
        }
    }

    return null;
}

function ruleAnchorInterval(model, si, gi, ri) {
    if (!model) return null;

    var rule = model.rules[ruleKey(si, gi, ri)];
    if (rule) {
        return {
            kind: 'rule',
            si: rule.si,
            gi: rule.gi,
            ri: rule.ri,
            left: rule.left,
            right: rule.right,
            width: rule.width
        };
    }

    var group = groupLayoutInterval(model, si, gi);
    if (group) {
        return {
            kind: 'rule',
            si: si,
            gi: gi,
            ri: ri,
            left: group.left,
            right: group.right,
            width: group.width
        };
    }

    var stage = stageLayoutInterval(model, si);
    if (!stage) return null;
    return {
        kind: 'rule',
        si: si,
        gi: gi,
        ri: ri,
        left: stage.left,
        right: stage.right,
        width: stage.width
    };
}

function stageLayoutInterval(model, si) {
    var stage = model && model.stages[si];
    if (!stage || stage.hidden || stage.width <= 0) return null;
    return {
        kind: 'stage',
        si: si,
        left: stage.left,
        right: stage.left + stage.width,
        width: stage.width
    };
}

function anchorIntervalForRef(model, anchor) {
    if (!model || !anchor) return null;

    if (anchor.kind === 'rule') {
        return ruleAnchorInterval(model, anchor.si, anchor.gi, anchor.ri);
    }
    if (anchor.gi !== null && anchor.gi !== undefined) {
        var group = groupLayoutInterval(model, anchor.si, anchor.gi);
        if (group) return group;
    }
    return stageLayoutInterval(model, anchor.si);
}

function traceAnchorRefOpen(anchor) {
    if (!anchor) return undefined;
    if (anchor.kind === 'stage') return effectiveStageOpen(anchor.si);
    if (anchor.kind === 'group') return effectiveGroupOpen(anchor.si, anchor.gi);
    if (anchor.kind === 'rule') return effectiveRuleOpen(anchor.si, anchor.gi, anchor.ri);
    return undefined;
}

function traceAnchorWithInterval(anchor, interval) {
    if (!anchor || !interval) return anchor;
    return {
        kind: anchor.kind,
        si: anchor.si,
        gi: anchor.gi,
        ri: anchor.ri,
        offset: anchor.offset,
        left: interval.left,
        right: interval.right,
        width: interval.width,
        sourceOpen: anchor.sourceOpen !== undefined ? anchor.sourceOpen : traceAnchorRefOpen(anchor)
    };
}

function traceAnchorLocalOffset(anchor) {
    var width = Number(anchor && anchor.width);
    if (!Number.isFinite(width) || width <= 0) return 0;

    var local = -(Number(anchor.offset) || 0);
    if (!Number.isFinite(local)) return 0;
    return Math.max(0, Math.min(width, local));
}

function traceAnchorScreenOffset(anchor, local) {
    var offset = Number(anchor && anchor.offset);
    return (Number.isFinite(offset) ? offset : 0) + (Number(local) || 0);
}

function traceAnchorScrollLeft(anchor) {
    var left = Number(anchor && anchor.left);
    var offset = Number(anchor && anchor.offset);
    if (!Number.isFinite(left) || !Number.isFinite(offset)) return null;
    return left - offset;
}

function traceRuleAllowedForAnchor(ruleFilter, si, gi, ri) {
    return !ruleFilter || !!ruleFilter(si, gi, ri);
}

function traceGroupHasAllowedRule(ruleFilter, si, gi) {
    var count = groupRuleCount(si, gi);
    for (var ri = 0; ri < count; ri++) {
        if (traceRuleAllowedForAnchor(ruleFilter, si, gi, ri)) return true;
    }
    return false;
}

function traceCollapsedGroupAnchorAtStagePoint(anchor, ruleFilter) {
    if (!anchor || anchor.kind !== 'stage' || !effectiveStageOpen(anchor.si)) return null;

    var model = getTraceLayoutModel();
    var stage = stageLayoutInterval(model, anchor.si);
    if (!stage) return null;

    var sourceLeft = Number.isFinite(Number(anchor.left)) ? Number(anchor.left) : stage.left;
    var sourceLocal = traceAnchorLocalOffset(anchor);
    var point = sourceLeft + sourceLocal;
    var scrollLeft = traceAnchorScrollLeft(anchor);
    if (scrollLeft === null) scrollLeft = sourceLeft - (Number(anchor.offset) || 0);

    for (var gi = 0; gi < groupCount(anchor.si); gi++) {
        if (groupRuleCount(anchor.si, gi) <= 1 || effectiveGroupOpen(anchor.si, gi)) continue;

        var group = groupLayoutInterval(model, anchor.si, gi);
        if (!group) continue;
        if (point < group.left - VIEWPORT_ANCHOR_EPSILON ||
            point > group.right + VIEWPORT_ANCHOR_EPSILON) {
            continue;
        }
        if (!traceGroupHasAllowedRule(ruleFilter, anchor.si, gi)) continue;

        return {
            kind: 'group',
            si: anchor.si,
            gi: gi,
            ri: undefined,
            offset: group.left - scrollLeft,
            left: group.left,
            right: group.right,
            width: group.width,
            sourceOpen: false
        };
    }

    return null;
}

function traceViewportAnchorSourceForExpandedLayout(anchor, ruleFilter) {
    if (!anchor) return anchor;

    var groupAnchor = traceCollapsedGroupAnchorAtStagePoint(anchor, ruleFilter);
    if (groupAnchor) return groupAnchor;

    var interval = anchorIntervalForRef(getTraceLayoutModel(), anchor);
    return traceAnchorWithInterval(anchor, interval);
}

function traceRuleIntervalDistance(rule, point) {
    if (point >= rule.left && point <= rule.right) return 0;
    return point < rule.left ? rule.left - point : point - rule.right;
}

function traceRuleIntervalForGroupPoint(model, si, gi, point, ruleFilter) {
    var best = null;
    var bestDistance = Infinity;
    var count = groupRuleCount(si, gi);

    for (var ri = 0; ri < count; ri++) {
        if (!traceRuleAllowedForAnchor(ruleFilter, si, gi, ri)) continue;

        var rule = model.rules[ruleKey(si, gi, ri)];
        if (!rule) continue;

        var distance = traceRuleIntervalDistance(rule, point);
        if (!best ||
            distance < bestDistance - VIEWPORT_ANCHOR_EPSILON ||
            (Math.abs(distance - bestDistance) <= VIEWPORT_ANCHOR_EPSILON &&
             rule.left < best.left)) {
            best = rule;
            bestDistance = distance;
        }
    }

    return best;
}

function traceGroupAnchorForExpandedPoint(anchor, group, targetLocal, screenOffset) {
    return {
        kind: 'group',
        si: anchor.si,
        gi: anchor.gi,
        ri: undefined,
        offset: screenOffset - targetLocal,
        left: group.left,
        right: group.right,
        width: group.width,
        sourceOpen: effectiveGroupOpen(anchor.si, anchor.gi)
    };
}

function traceViewportAnchorForExpandedLayout(anchor, ruleFilter) {
    if (!anchor ||
        anchor.kind !== 'group' ||
        anchor.sourceOpen !== false ||
        !effectiveGroupOpen(anchor.si, anchor.gi)) {
        return anchor;
    }

    var model = getTraceLayoutModel();
    var group = groupLayoutInterval(model, anchor.si, anchor.gi);
    if (!group || group.width <= 0) return anchor;

    var sourceWidth = Number(anchor.width);
    if (!Number.isFinite(sourceWidth) || sourceWidth <= 0) return anchor;

    var sourceLocal = traceAnchorLocalOffset(anchor);
    var targetLocal = Math.max(0, Math.min(
        group.width,
        sourceLocal * group.width / sourceWidth
    ));
    var targetPoint = group.left + targetLocal;
    var screenOffset = traceAnchorScreenOffset(anchor, sourceLocal);
    var rule = traceRuleIntervalForGroupPoint(model, anchor.si, anchor.gi, targetPoint, ruleFilter);
    if (!rule || rule.width <= 0) {
        return traceGroupAnchorForExpandedPoint(anchor, group, targetLocal, screenOffset);
    }
    if (!ruleFilter && traceRuleIntervalDistance(rule, targetPoint) > VIEWPORT_ANCHOR_EPSILON) {
        return traceGroupAnchorForExpandedPoint(anchor, group, targetLocal, screenOffset);
    }

    var localWithinRule = Math.max(0, Math.min(rule.width, targetPoint - rule.left));

    return {
        kind: 'rule',
        si: rule.si,
        gi: rule.gi,
        ri: rule.ri,
        offset: screenOffset - localWithinRule,
        left: rule.left,
        right: rule.right,
        width: rule.width,
        sourceOpen: effectiveRuleOpen(rule.si, rule.gi, rule.ri)
    };
}

var VIEWPORT_ANCHOR_EPSILON = 0.5;
var TRACE_ANCHOR_MARKER_WIDTH = 2;
var TRACE_ANCHOR_FALLBACK_HEIGHT = 28;
var TRACE_ANCHOR_FALLBACK_BOTTOM_GAP = 18;
var traceAnchorSurface = createVisualSurface({
    name: 'trace-anchor-line',
    epochScopes: ['trace', 'virtual'],
    allowMissingTarget: true,
    resolveTarget: function(ref, options) {
        options = options || {};
        if (options.target) return options.target;
        if (traceAnchorRuntime().traceAnchorLineEl) return traceAnchorRuntime().traceAnchorLineEl;
        if (options.selector && hasDOM() && document.querySelector) {
            return document.querySelector(options.selector);
        }
        return null;
    }
});

function commitTraceAnchorSurface(visualCtx, options, writer) {
    return traceAnchorSurface.commit(visualCtx, options || {}, writer);
}

function runTraceAnchorSurfaceCommitNow(label, callback) {
    return runVisualSurfaceCommitNow(
        label,
        ['trace-anchor-line'],
        ['trace', 'virtual'],
        callback
    );
}

function validAnchorRect(el) {
    if (!el || !el.getBoundingClientRect) return false;
    var rect = el.getBoundingClientRect();
    return Number.isFinite(rect.left) &&
           Number.isFinite(rect.right) &&
           rect.right > rect.left + VIEWPORT_ANCHOR_EPSILON;
}

function stageAnchorDomElement(si) {
    if (!hasDOM()) return null;

    var exp = document.getElementById('stage-exp-' + si);
    if (validAnchorRect(exp)) return exp;

    var col = document.getElementById('stage-col-' + si);
    return validAnchorRect(col) ? col : null;
}

function exactAnchorDomElement(anchor) {
    if (!anchor || !hasDOM()) return null;

    if (anchor.kind === 'stage') {
        return stageAnchorDomElement(anchor.si);
    }
    if (anchor.kind === 'group') {
        var group = document.getElementById('group-' + anchor.si + '-' + anchor.gi);
        return validAnchorRect(group) ? group : null;
    }
    if (anchor.kind === 'rule') {
        var rule = document.getElementById('rule-' + ruleKey(anchor.si, anchor.gi, anchor.ri));
        return validAnchorRect(rule) ? rule : null;
    }
    return null;
}

function anchorPriority(kind) {
    if (kind === 'stage') return 3;
    if (kind === 'group') return 2;
    return 1;
}

function anchorCandidate(kind, interval, si, gi, ri) {
    if (!interval) return null;
    return {
        kind: kind,
        si: si,
        gi: gi,
        ri: ri,
        left: interval.left,
        right: interval.right,
        width: interval.width,
        priority: anchorPriority(kind)
    };
}

function domAnchorCandidate(kind, traceRect, scrollLeft, si, gi, ri) {
    var el = exactAnchorDomElement({ kind: kind, si: si, gi: gi, ri: ri });
    if (!el) return null;

    var rect = el.getBoundingClientRect();
    return anchorCandidate(kind, {
        left: rect.left - traceRect.left + scrollLeft,
        right: rect.right - traceRect.left + scrollLeft,
        width: rect.right - rect.left
    }, si, gi, ri);
}

function traceDomViewportAnchorCandidates(traceEl) {
    if (!hasDOM() || !traceEl || !traceEl.getBoundingClientRect) return [];

    var traceRect = traceEl.getBoundingClientRect();
    if (!Number.isFinite(traceRect.left)) return [];

    var scrollLeft = traceEl.scrollLeft || 0;
    var candidates = [];

    for (var si = 0; si < currentStageCount(); si++) {
        var stageCandidate = domAnchorCandidate('stage', traceRect, scrollLeft, si);
        if (stageCandidate) candidates.push(stageCandidate);

        for (var gi = 0; gi < groupCount(si); gi++) {
            var groupCandidate = domAnchorCandidate('group', traceRect, scrollLeft, si, gi);
            if (groupCandidate) candidates.push(groupCandidate);
        }
    }

    var ruleEls = document.querySelectorAll ?
        document.querySelectorAll('.rule-cell[id^="rule-"]') :
        [];
    for (var i = 0; i < ruleEls.length; i++) {
        var ref = parseRuleKeyId(ruleEls[i].id, 'rule-');
        if (!ref || !validAnchorRect(ruleEls[i])) continue;

        var rect = ruleEls[i].getBoundingClientRect();
        candidates.push(anchorCandidate('rule', {
            left: rect.left - traceRect.left + scrollLeft,
            right: rect.right - traceRect.left + scrollLeft,
            width: rect.right - rect.left
        }, ref.si, ref.gi, ref.ri));
    }

    return candidates;
}

function traceModelViewportAnchorCandidates(model) {
    var candidates = [];

    for (var si = 0; si < currentStageCount(); si++) {
        candidates.push(anchorCandidate(
            'stage',
            stageLayoutInterval(model, si),
            si
        ));

        for (var gi = 0; gi < groupCount(si); gi++) {
            candidates.push(anchorCandidate(
                'group',
                groupLayoutInterval(model, si, gi),
                si,
                gi
            ));
        }
    }

    for (var key in model.rules) {
        if (!Object.prototype.hasOwnProperty.call(model.rules, key)) continue;
        var rule = model.rules[key];
        candidates.push(anchorCandidate(
            'rule',
            rule,
            rule.si,
            rule.gi,
            rule.ri
        ));
    }

    return candidates;
}

function betterAnchorCandidate(candidate, best) {
    if (!candidate) return best;
    if (!best) return candidate;
    if (candidate.left < best.left - VIEWPORT_ANCHOR_EPSILON) return candidate;
    if (candidate.left > best.left + VIEWPORT_ANCHOR_EPSILON) return best;
    if (candidate.priority > best.priority) return candidate;
    if (candidate.priority < best.priority) return best;
    return candidate.right < best.right ? candidate : best;
}

function firstVisibleBeginning(candidates, left, right) {
    var best = null;
    for (var i = 0; i < candidates.length; i++) {
        var candidate = candidates[i];
        if (!candidate) continue;
        if (candidate.left < left - VIEWPORT_ANCHOR_EPSILON ||
            candidate.left >= right) continue;

        best = betterAnchorCandidate(candidate, best);
    }
    return best;
}

function firstContainingViewportStart(candidates, left, right) {
    var best = null;
    for (var i = 0; i < candidates.length; i++) {
        var candidate = candidates[i];
        if (!candidate || candidate.left > left || candidate.right <= left) continue;
        if (!best ||
            candidate.left > best.left + VIEWPORT_ANCHOR_EPSILON ||
            (Math.abs(candidate.left - best.left) <= VIEWPORT_ANCHOR_EPSILON &&
             candidate.priority > best.priority)) {
            best = candidate;
        }
    }
    return best;
}

function firstVisibleInterval(candidates, left, right) {
    var best = null;
    for (var i = 0; i < candidates.length; i++) {
        var candidate = candidates[i];
        if (!candidate || candidate.right <= left || candidate.left >= right) continue;
        best = betterAnchorCandidate(candidate, best);
    }
    return best;
}

function captureTraceViewportAnchor() {
    if (!hasDOM() || !document.querySelector) return null;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return null;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(viewport) || viewport <= 0) return null;

    var left = traceEl.scrollLeft || 0;
    var right = left + viewport;
    var candidates = traceDomViewportAnchorCandidates(traceEl);
    if (!candidates.length) {
        var model = rebuildTraceLayoutModel();
        candidates = traceModelViewportAnchorCandidates(model);
    }
    var anchor = firstVisibleBeginning(candidates, left, right) ||
                 firstContainingViewportStart(candidates, left, right) ||
                 firstVisibleInterval(candidates, left, right);
    if (!anchor) return null;

    return {
        kind: anchor.kind,
        si: anchor.si,
        gi: anchor.gi,
        ri: anchor.ri,
        offset: anchor.left - left,
        left: anchor.left,
        right: anchor.right,
        width: anchor.width,
        sourceOpen: traceAnchorRefOpen(anchor)
    };
}

function maxTraceScrollForRestore(traceEl, model, viewport) {
    return traceRestorableMaxScrollLeft(model, traceEl);
}

function restoreTraceViewportAnchorFromDom(anchor, traceEl, model, viewport, options) {
    options = options || {};
    if (!traceEl || !traceEl.getBoundingClientRect) return false;

    var domEl = exactAnchorDomElement(anchor);
    if (!domEl) return false;

    var traceRect = traceEl.getBoundingClientRect();
    var rect = domEl.getBoundingClientRect();
    if (!Number.isFinite(traceRect.left) || !Number.isFinite(rect.left)) return false;

    var offset = traceAnchorOffsetForRestoredWidth(anchor, rect.right - rect.left);
    var targetScreenLeft = traceRect.left + offset;
    var max = maxTraceScrollForRestore(traceEl, model, viewport);
    var target = (traceEl.scrollLeft || 0) + rect.left - targetScreenLeft;
    setTraceScrollLeft(traceEl, target, {
        maxScrollLeft: max,
        roundToDevicePixels: true,
        cancelWheelGlide: true,
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: !options.deferAnchorPreview
    });
    traceAnchorRuntime().lastAnchorRestore = {
        path: 'dom',
        offset: offset,
        target: target,
        max: max,
        applied: traceEl.scrollLeft || 0
    };
    return true;
}

function restoreTraceViewportAnchor(anchor, options) {
    options = options || {};
    if (!anchor || !hasDOM() || !document.querySelector) return false;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return false;

    var viewport = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(viewport) || viewport <= 0) return false;

    var model = rebuildTraceLayoutModel();
    var interval = anchorIntervalForRef(model, anchor);
    if (!interval) return false;

    refreshVirtualRowsNow();
    model = getTraceLayoutModel();

    if (restoreTraceViewportAnchorFromDom(anchor, traceEl, model, viewport, options)) return true;

    interval = anchorIntervalForRef(model, anchor);
    if (!interval) return false;

    var offset = traceAnchorOffsetForRestoredWidth(anchor, interval.width);
    var max = maxTraceScrollForRestore(traceEl, model, viewport);
    var target = interval.left - offset;
    setTraceScrollLeft(traceEl, target, {
        maxScrollLeft: max,
        roundToDevicePixels: true,
        cancelWheelGlide: true,
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceAnchorLineNow: !options.deferAnchorPreview
    });
    traceAnchorRuntime().lastAnchorRestore = {
        path: 'model',
        offset: offset,
        target: target,
        max: max,
        applied: traceEl.scrollLeft || 0
    };
    return true;
}

var VIEWPORT_ANCHOR_RESTORE_TOLERANCE_PX = 4;

function viewportAnchorRuleRefFromKey(key) {
    key = String(key || '');
    if (key.indexOf('fs-') === 0) key = key.substring(3);
    var parts = key.split('-');
    if (parts.length !== 3) return null;
    var ref = {
        si: Number(parts[0]),
        gi: Number(parts[1]),
        ri: Number(parts[2])
    };
    if (!Number.isInteger(ref.si) || !Number.isInteger(ref.gi) || !Number.isInteger(ref.ri)) {
        return null;
    }
    if (typeof ruleRefValidForCurrentTrace === 'function' &&
            !ruleRefValidForCurrentTrace(ref)) {
        return null;
    }
    return ref;
}

function viewportAnchorRuleRefFromElement(el) {
    if (!el || !el.getAttribute) return null;
    var key = el.getAttribute('data-rule-key') || '';
    if (!key && el.id && el.id.indexOf('rule-') === 0) key = el.id.substring(5);
    return viewportAnchorRuleRefFromKey(key);
}

function viewportAnchorRuleKey(ref) {
    return ref ? ref.si + '-' + ref.gi + '-' + ref.ri : '';
}

function viewportAnchorCaptureRulePaneSnapshots() {
    var result = {
        refs: [],
        refSeen: {},
        info: []
    };
    if (!hasDOM() || !document.querySelectorAll) return result;

    var cells = document.querySelectorAll('.rule-cell[id^="rule-"], .rule-cell[data-rule-key]');
    for (var i = 0; i < cells.length; i++) {
        var ref = viewportAnchorRuleRefFromElement(cells[i]);
        if (!ref) continue;
        var key = viewportAnchorRuleKey(ref);
        if (!result.refSeen[key]) {
            result.refSeen[key] = true;
            result.refs.push(ref);
        }
        if (typeof captureMountedRulePaneScroll === 'function') {
            captureMountedRulePaneScroll(ref.si, ref.gi, ref.ri);
        }
        if (typeof captureInfoScrollSnapshot !== 'function') continue;
        var content = cells[i].querySelector && cells[i].querySelector('[id^="info-content-"]');
        if (!content || !content.id) continue;
        result.info.push({
            id: content.id,
            ref: ref,
            snapshot: captureInfoScrollSnapshot(content)
        });
    }
    return result;
}

function viewportAnchorCaptureTreeMaterializers() {
    var result = [];
    if (typeof activeTreeMaterializers !== 'function' ||
            typeof saveTreeMaterializerPaneAnchor !== 'function') {
        return result;
    }
    var materializers = activeTreeMaterializers();
    for (var i = 0; i < materializers.length; i++) {
        var state = materializers[i] && materializers[i].state;
        var anchor = typeof saveTreeMaterializerPaneAnchorForNextRender === 'function'
            ? saveTreeMaterializerPaneAnchorForNextRender(state)
            : saveTreeMaterializerPaneAnchor(state);
        if (!anchor) continue;
        result.push({
            ruleKey: state && state.ruleKey || '',
            treeInstanceId: state && state.treeInstanceId || '',
            anchor: anchor
        });
    }
    return result;
}

function captureViewportAnchors(reason, options) {
    options = options || {};
    var panes = options.panes !== false ? viewportAnchorCaptureRulePaneSnapshots() : {
        refs: [],
        refSeen: {},
        info: []
    };
    return {
        reason: reason || 'viewport-anchors',
        options: options,
        trace: options.trace === false
            ? null
            : traceViewportAnchorSourceForExpandedLayout(captureTraceViewportAnchor()),
        panes: panes,
        trees: options.trees === false ? [] : viewportAnchorCaptureTreeMaterializers()
    };
}

function viewportAnchorNormalizedTreeRuleKey(ruleKey) {
    ruleKey = String(ruleKey || '');
    return ruleKey.indexOf('fs-') === 0 ? ruleKey.substring(3) : ruleKey;
}

function viewportAnchorPreferTreeAnchor(existing, candidate) {
    if (!candidate || !candidate.anchor) return existing || null;
    if (!existing || !existing.anchor) return candidate;
    var existingKey = String(existing.ruleKey || '');
    var candidateKey = String(candidate.ruleKey || '');
    if (candidateKey.indexOf('fs-') === 0 && existingKey.indexOf('fs-') !== 0) {
        return candidate;
    }
    return existing;
}

function viewportAnchorTreeAnchorsByNormalizedRule(snapshot) {
    var anchorsByRule = {};
    if (!snapshot || !Array.isArray(snapshot.trees)) return anchorsByRule;
    for (var t = 0; t < snapshot.trees.length; t++) {
        var item = snapshot.trees[t];
        if (!item || !item.ruleKey || !item.anchor) continue;
        var key = viewportAnchorNormalizedTreeRuleKey(item.ruleKey);
        anchorsByRule[key] = viewportAnchorPreferTreeAnchor(anchorsByRule[key], item);
    }
    return anchorsByRule;
}

function restoreViewportPaneSnapshots(snapshot) {
    if (!snapshot || !snapshot.panes) return;

    for (var r = 0; r < snapshot.panes.refs.length; r++) {
        var ref = snapshot.panes.refs[r];
        if (typeof restoreRuleTreePaneScroll === 'function') {
            restoreRuleTreePaneScroll(ref, hasDOM() && document.getElementById
                ? document.getElementById('tree-' + viewportAnchorRuleKey(ref))
                : null);
        }
        if (typeof restoreRuleInfoPaneScroll === 'function') {
            restoreRuleInfoPaneScroll(ref.si, ref.gi, ref.ri);
        }
    }

    if (typeof restoreInfoScrollSnapshot === 'function' && hasDOM() && document.getElementById) {
        for (var i = 0; i < snapshot.panes.info.length; i++) {
            var info = snapshot.panes.info[i];
            var content = document.getElementById(info.id);
            if (!content) continue;
            restoreInfoScrollSnapshot(content, info.snapshot);
        }
    }

    for (var c = 0; c < snapshot.panes.refs.length; c++) {
        var capturedRef = snapshot.panes.refs[c];
        if (typeof captureMountedRulePaneScroll === 'function') {
            captureMountedRulePaneScroll(capturedRef.si, capturedRef.gi, capturedRef.ri);
        }
    }
}

function restoreViewportTreeMaterializerAnchors(snapshot) {
    if (!snapshot ||
            typeof activeTreeMaterializers !== 'function' ||
            typeof restoreTreeMaterializerPaneAnchor !== 'function') {
        return;
    }
    var anchorsByRule = viewportAnchorTreeAnchorsByNormalizedRule(snapshot);
    var materializers = activeTreeMaterializers();
    for (var i = 0; i < materializers.length; i++) {
        var state = materializers[i] && materializers[i].state;
        var item = state && anchorsByRule[viewportAnchorNormalizedTreeRuleKey(state.ruleKey || '')];
        var anchor = item && item.anchor;
        if (!state || !state.session || !anchor) continue;
        state.session.anchor = anchor;
        state.session.scrollLeft = anchor.scrollLeft;
        if (typeof restoreTreeMaterializerPaneAnchorForViewportRestore === 'function') {
            restoreTreeMaterializerPaneAnchorForViewportRestore(
                materializers[i],
                snapshot.reason || 'viewport-anchor-restore'
            );
        } else {
            restoreTreeMaterializerPaneAnchor(state);
        }
    }
}


function restoreViewportAnchors(snapshot) {
    if (!snapshot) return false;
    refreshDirtySearchStateForCurrentLayout();
    restoreViewportTreeMaterializerAnchors(snapshot);
    restoreViewportPaneSnapshots(snapshot);

    var traceRestoredOk = false;
    var restoredTraceAnchor = null;
    if (snapshot.trace) {
        restoredTraceAnchor = traceViewportAnchorForExpandedLayout(snapshot.trace);
        traceRestoredOk = restoreTraceViewportAnchor(restoredTraceAnchor);
    }
    return traceRestoredOk;
}

function withViewportAnchors(reason, callback, options) {
    var snapshot = captureViewportAnchors(reason, options);
    cancelTraceWheelGlide();
    try {
        return callback();
    } finally {
        restoreViewportAnchors(snapshot);
    }
}

function withRuleLocalViewportStability(reason, callback, options) {
    options = options || {};
    var traceLock = captureTraceHorizontalScrollLock(reason);
    var snapshotOptions = Object.assign({}, options, { trace: false });
    var snapshot = captureViewportAnchors(reason, snapshotOptions);
    cancelTraceWheelGlide();
    try {
        return callback();
    } finally {
        restoreViewportAnchors(snapshot);
        restoreTraceHorizontalScrollLock(traceLock);
    }
}

function preserveTraceViewportAnchor(callback) {
    return withViewportAnchors('trace-layout', callback);
}

// Some user-local mutations, such as clicking a collapsed rule open, must leave
// horizontal trace scroll under the user's control after anchor restoration.
function traceHorizontalScrollLockElement(snapshot) {
    if (!snapshot || !hasDOM() || !document.querySelector) return null;
    if (snapshot.traceEl && snapshot.traceEl.isConnected !== false) return snapshot.traceEl;
    return document.querySelector('.trace');
}

function restoreTraceHorizontalScrollLock(snapshot) {
    var traceEl = traceHorizontalScrollLockElement(snapshot);
    if (!traceEl) return false;

    var target = Number(snapshot && snapshot.scrollLeft);
    if (!Number.isFinite(target)) target = 0;

    var model = typeof getTraceLayoutModel === 'function' ? getTraceLayoutModel() : null;
    var viewport = traceEl.clientWidth ||
        (typeof window !== 'undefined' && window.innerWidth) ||
        0;
    var options = {
        suppressVirtualRefresh: true,
        updateVirtualRange: true,
        refreshVirtualRows: true,
        updateTraceScrollbar: true,
        updateTraceAnchorLineNow: true
    };
    if (model && typeof maxTraceScrollForRestore === 'function') {
        options.maxScrollLeft = maxTraceScrollForRestore(traceEl, model, viewport);
    }
    return setTraceScrollLeft(traceEl, target, options);
}

function captureTraceHorizontalScrollLock(reason) {
    if (!hasDOM() || !document.querySelector) return null;
    var traceEl = document.querySelector('.trace');
    if (!traceEl) return null;
    return {
        reason: reason || 'trace-horizontal-scroll-lock',
        traceEl: traceEl,
        scrollLeft: traceEl.scrollLeft || 0
    };
}

function withTraceHorizontalScrollLock(reason, callback) {
    var snapshot = captureTraceHorizontalScrollLock(reason);
    try {
        return callback();
    } finally {
        restoreTraceHorizontalScrollLock(snapshot);
    }
}

function ensureTraceAnchorLine(traceEl, visualCtx) {
    if (!hasDOM() || !document.createElement || !traceEl || !traceEl.appendChild) return null;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceAnchorSurfaceCommitNow('ensure-trace-anchor-line', function(ctx) {
            return ensureTraceAnchorLine(traceEl, ctx);
        });
    }

    var anchorState = traceAnchorRuntime();
    var line = null;
    var target = anchorState.traceAnchorLineEl || traceEl;
    commitTraceAnchorSurface(visualCtx, {
        label: 'ensure-trace-anchor-line',
        target: target
    }, function() {
        if (anchorState.traceAnchorLineEl) {
            if (anchorState.traceAnchorLineEl.parentNode !== traceEl) {
                traceEl.appendChild(anchorState.traceAnchorLineEl);
            }
            line = anchorState.traceAnchorLineEl;
            return;
        }

        anchorState.traceAnchorLineEl = document.createElement('div');
        anchorState.traceAnchorLineEl.className = 'trace-anchor-line';
        anchorState.traceAnchorLineEl.setAttribute('aria-hidden', 'true');
        traceEl.appendChild(anchorState.traceAnchorLineEl);
        line = anchorState.traceAnchorLineEl;
    });
    return line;
}

function hideTraceAnchorLine(visualCtx) {
    var line = traceAnchorRuntime().traceAnchorLineEl;
    if (!line) return false;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceAnchorSurfaceCommitNow('hide-trace-anchor-line', function(ctx) {
            return hideTraceAnchorLine(ctx);
        });
    }

    var committed = false;
    commitTraceAnchorSurface(visualCtx, {
        label: 'hide-trace-anchor-line',
        target: line
    }, function(target) {
        target.style.display = 'none';
        committed = true;
    });
    return committed;
}

function validAnchorPreviewRect(rect) {
    return rect &&
           Number.isFinite(rect.left) &&
           Number.isFinite(rect.top) &&
           Number.isFinite(rect.bottom) &&
           rect.bottom > rect.top + VIEWPORT_ANCHOR_EPSILON;
}

function traceRectHeight(traceRect) {
    if (traceRect && Number.isFinite(traceRect.height)) return traceRect.height;
    if (traceRect &&
        Number.isFinite(traceRect.top) &&
        Number.isFinite(traceRect.bottom)) {
        return traceRect.bottom - traceRect.top;
    }
    return 0;
}

function traceAnchorPreviewContentLeft(anchor, model) {
    var interval = anchorIntervalForRef(model, anchor);
    return interval && Number.isFinite(interval.left) ? interval.left : null;
}

function traceAnchorPreviewGeometry(anchor, traceEl, traceRect, model) {
    var scrollLeft = traceEl.scrollLeft || 0;
    var viewportWidth = traceEl.clientWidth || window.innerWidth || 0;
    if (!Number.isFinite(viewportWidth) || viewportWidth < 0) viewportWidth = 0;
    var viewportLeft = scrollLeft;
    var viewportRight = scrollLeft + viewportWidth;
    var contentLeft = traceAnchorPreviewContentLeft(anchor, model);
    var el = exactAnchorDomElement(anchor);
    if (el && el.getBoundingClientRect) {
        var rect = el.getBoundingClientRect();
        if (validAnchorPreviewRect(rect)) {
            var traceHeight = traceRectHeight(traceRect);
            var top = Math.max(0, rect.top - traceRect.top);
            var bottom = Math.min(traceHeight, rect.bottom - traceRect.top);
            if (bottom > top + VIEWPORT_ANCHOR_EPSILON) {
                return {
                    left: contentLeft !== null ? contentLeft : rect.left - traceRect.left + scrollLeft,
                    top: top,
                    height: bottom - top,
                    viewportLeft: viewportLeft,
                    viewportRight: viewportRight,
                    exact: true
                };
            }
        }
    }

    var fallbackTraceHeight = traceRectHeight(traceRect);
    var markerHeight = Math.min(
        TRACE_ANCHOR_FALLBACK_HEIGHT,
        Math.max(16, fallbackTraceHeight - TRACE_ANCHOR_FALLBACK_BOTTOM_GAP)
    );
    var markerTop = Math.max(
        0,
        fallbackTraceHeight - TRACE_ANCHOR_FALLBACK_BOTTOM_GAP - markerHeight
    );
    return {
        left: contentLeft !== null ? contentLeft : scrollLeft + anchor.offset,
        top: markerTop,
        height: markerHeight,
        viewportLeft: viewportLeft,
        viewportRight: viewportRight,
        exact: false
    };
}

function traceAnchorMarkerLeft(anchor, geometry) {
    var left = geometry.left;
    if (geometry.exact && anchor.kind !== 'stage') {
        left -= TRACE_ANCHOR_MARKER_WIDTH / 2;
    }
    var viewportLeft = Number(geometry.viewportLeft);
    var viewportRight = Number(geometry.viewportRight);
    if (Number.isFinite(viewportLeft) && Number.isFinite(viewportRight)) {
        var maxLeft = Math.max(viewportLeft, viewportRight - TRACE_ANCHOR_MARKER_WIDTH);
        left = Math.max(viewportLeft, Math.min(maxLeft, left));
    }
    return left;
}

function updateTraceAnchorLine(visualCtx) {
    traceAnchorRuntime().traceAnchorLineFrame = null;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceAnchorSurfaceCommitNow('trace-anchor-line', function(ctx) {
            return updateTraceAnchorLine(ctx);
        });
    }
    if (!hasDOM()) return;
    if (!traceAnchorRuntime().traceAnchorPreviewActive) {
        return hideTraceAnchorLine(visualCtx);
    }

    var traceEl = document.querySelector('.trace');
    if (!traceEl || !traceEl.getBoundingClientRect) {
        return hideTraceAnchorLine(visualCtx);
    }

    var line = ensureTraceAnchorLine(traceEl, visualCtx);
    if (!line) return false;

    var traceRect = traceEl.getBoundingClientRect();
    var anchor = captureTraceViewportAnchor();
    var committed = false;

    commitTraceAnchorSurface(visualCtx, {
        label: 'trace-anchor-line',
        target: line
    }, function(target) {
        if (!anchor || !Number.isFinite(anchor.offset)) {
            target.style.display = 'none';
            committed = true;
            return;
        }

        target.style.display = 'block';
        var geometry = traceAnchorPreviewGeometry(anchor, traceEl, traceRect, getTraceLayoutModel());
        target.style.left = '0px';
        target.style.top = '0px';
        target.style.transform = 'translate3d(' +
            traceAnchorMarkerLeft(anchor, geometry) + 'px,' +
            geometry.top + 'px,0)';
        target.style.width = TRACE_ANCHOR_MARKER_WIDTH + 'px';
        target.style.height = geometry.height + 'px';
        target.setAttribute('data-anchor-label', traceAnchorPreviewLabel(anchor));
        target.title = traceAnchorTitle(anchor);
        committed = true;
    });
    return committed;
}

function cancelTraceAnchorLineUpdate() {
    if (traceAnchorRuntime().traceAnchorLineFrame === null) return;

    cancelRenderFrameWork(traceAnchorRuntime().traceAnchorLineFrame);
    traceAnchorRuntime().traceAnchorLineFrame = null;
}

function updateTraceAnchorLineNow() {
    cancelTraceAnchorLineUpdate();
    updateTraceAnchorLine();
}

function scheduleTraceAnchorLineUpdate() {
    var anchorState = traceAnchorRuntime();
    if (!hasDOM() || !document.body || !document.createElement ||
        !anchorState.traceAnchorPreviewActive || anchorState.traceAnchorLineFrame !== null) {
        return;
    }

    anchorState.traceAnchorLineFrame = scheduleRenderDeferredWork(
        'trace-anchor-line',
        function runScheduledTraceAnchorLineUpdate(visualCtx) {
            traceAnchorRuntime().traceAnchorLineFrame = null;
            updateTraceAnchorLine(visualCtx);
        },
        {
            epochScopes: ['trace', 'virtual'],
            label: 'trace-anchor-line',
            surfaces: ['trace-anchor-line'],
            withVisualContext: true,
            onDiscard: function() {
                traceAnchorRuntime().traceAnchorLineFrame = null;
            }
        }
    );
}

function traceAnchorLabel(anchor) {
    if (!anchor) return '';
    if (anchor.kind === 'stage') return 'stage ' + (anchor.si + 1);
    if (anchor.kind === 'group') return 'group ' + (anchor.si + 1) + '.' + (anchor.gi + 1);
    return 'rule ' + (anchor.si + 1) + '.' + (anchor.gi + 1) + '.' + (anchor.ri + 1);
}

function traceAnchorPreviewLabel(anchor) {
    return 'Stays in place: ' + traceAnchorLabel(anchor);
}

function traceAnchorTitle(anchor) {
    return traceAnchorLabel(anchor) + ' stays in place when layout changes';
}

function setTraceAnchorPreviewActive(active) {
    traceAnchorRuntime().traceAnchorPreviewActive = !!active;
    if (traceAnchorRuntime().traceAnchorPreviewActive) {
        updateTraceAnchorLine();
    } else {
        hideTraceAnchorLine();
    }
}

function traceAnchorPreviewEventTarget(ev) {
    return ev && (ev.currentTarget || ev.target) || null;
}

function traceAnchorElementHovered(el) {
    if (!el || !el.matches) return null;
    try {
        return !!el.matches(':hover');
    } catch (err) {
        return null;
    }
}

function syncTraceAnchorPreviewActive() {
    var anchorState = traceAnchorRuntime();
    if (hasDOM()) {
        var activeElement = document.activeElement || null;
        var cycleButton = document.getElementById('cycle-button');
        var searchExpandButton = document.getElementById('search-expand-button');
        var searchBox = document.getElementById('search-box');
        var focusTarget = anchorState.traceAnchorPreviewFocusTarget;
        var pointerFocusTarget = anchorState.traceAnchorPreviewPointerFocusTarget;
        var searchFocusTarget = anchorState.traceAnchorPreviewSearchFocusTarget;
        if (anchorState.traceAnchorPreviewFocused && (
                (focusTarget && activeElement !== focusTarget) ||
                (!focusTarget && activeElement && activeElement !== cycleButton))) {
            anchorState.traceAnchorPreviewFocused = false;
            anchorState.traceAnchorPreviewFocusTarget = null;
        }
        if (anchorState.traceAnchorPreviewPointerFocused &&
                pointerFocusTarget &&
                activeElement !== pointerFocusTarget) {
            anchorState.traceAnchorPreviewPointerFocused = false;
            anchorState.traceAnchorPreviewPointerFocusTarget = null;
        }
        if (anchorState.traceAnchorPreviewSearchFocused && (
                (searchFocusTarget &&
                    activeElement !== searchFocusTarget &&
                    activeElement !== searchBox) ||
                (!searchFocusTarget &&
                    activeElement &&
                    activeElement !== searchExpandButton &&
                    activeElement !== searchBox))) {
            anchorState.traceAnchorPreviewSearchFocused = false;
            anchorState.traceAnchorPreviewSearchFocusTarget = null;
        }
        if (anchorState.traceAnchorPreviewHovered) {
            var hovered = traceAnchorElementHovered(anchorState.traceAnchorPreviewHoverTarget);
            if (hovered === false) {
                anchorState.traceAnchorPreviewHovered = false;
                anchorState.traceAnchorPreviewHoverTarget = null;
            }
        }
    }
    setTraceAnchorPreviewActive(
        anchorState.traceAnchorPreviewHovered ||
        anchorState.traceAnchorPreviewFocused ||
        anchorState.traceAnchorPreviewSearchFocused
    );
}

function showTraceAnchorPreview(ev) {
    traceAnchorRuntime().traceAnchorPreviewHovered = true;
    traceAnchorRuntime().traceAnchorPreviewHoverTarget = traceAnchorPreviewEventTarget(ev);
    syncTraceAnchorPreviewActive();
}

function hideTraceAnchorPreview(ev) {
    var target = traceAnchorPreviewEventTarget(ev);
    if (!target ||
            !traceAnchorRuntime().traceAnchorPreviewHoverTarget ||
            target === traceAnchorRuntime().traceAnchorPreviewHoverTarget) {
        traceAnchorRuntime().traceAnchorPreviewHovered = false;
        traceAnchorRuntime().traceAnchorPreviewHoverTarget = null;
    }
    syncTraceAnchorPreviewActive();
}

function pointerFocusTraceAnchorPreview(ev) {
    traceAnchorRuntime().traceAnchorPreviewPointerFocused = true;
    traceAnchorRuntime().traceAnchorPreviewPointerFocusTarget = traceAnchorPreviewEventTarget(ev);
    traceAnchorRuntime().traceAnchorPreviewFocused = false;
    traceAnchorRuntime().traceAnchorPreviewFocusTarget = null;
    syncTraceAnchorPreviewActive();
}

function focusTraceAnchorPreview(ev) {
    traceAnchorRuntime().traceAnchorPreviewFocused = !traceAnchorRuntime().traceAnchorPreviewPointerFocused;
    traceAnchorRuntime().traceAnchorPreviewFocusTarget = traceAnchorRuntime().traceAnchorPreviewFocused ?
        traceAnchorPreviewEventTarget(ev) :
        null;
    syncTraceAnchorPreviewActive();
}

function blurTraceAnchorPreview() {
    traceAnchorRuntime().traceAnchorPreviewFocused = false;
    traceAnchorRuntime().traceAnchorPreviewFocusTarget = null;
    traceAnchorRuntime().traceAnchorPreviewPointerFocused = false;
    traceAnchorRuntime().traceAnchorPreviewPointerFocusTarget = null;
    syncTraceAnchorPreviewActive();
}

function focusSearchAnchorPreview(ev) {
    traceAnchorRuntime().traceAnchorPreviewSearchFocused = true;
    traceAnchorRuntime().traceAnchorPreviewSearchFocusTarget = traceAnchorPreviewEventTarget(ev);
    syncTraceAnchorPreviewActive();
}

function blurSearchAnchorPreview(ev) {
    if (ev && ev.currentTarget && ev.currentTarget.contains &&
        ev.relatedTarget && ev.currentTarget.contains(ev.relatedTarget)) {
        return;
    }
    traceAnchorRuntime().traceAnchorPreviewSearchFocused = false;
    traceAnchorRuntime().traceAnchorPreviewSearchFocusTarget = null;
    syncTraceAnchorPreviewActive();
}

function initTraceAnchorPreviewControl() {
    if (!hasDOM()) return;
    var btn = document.getElementById('cycle-button');
    if (btn && !btn.__traceAnchorPreviewBound) {
        btn.__traceAnchorPreviewBound = true;
        btn.title = 'Preview the point that stays in place when expanding or collapsing all';
        if (btn.addEventListener) {
            btn.addEventListener('mouseenter', showTraceAnchorPreview);
            btn.addEventListener('mouseleave', hideTraceAnchorPreview);
            btn.addEventListener('pointerdown', pointerFocusTraceAnchorPreview);
            btn.addEventListener('mousedown', pointerFocusTraceAnchorPreview);
            btn.addEventListener('focus', focusTraceAnchorPreview);
            btn.addEventListener('blur', blurTraceAnchorPreview);
        }
    }

    var searchExpandBtn = document.getElementById('search-expand-button');
    if (searchExpandBtn && !searchExpandBtn.__traceAnchorPreviewBound) {
        searchExpandBtn.__traceAnchorPreviewBound = true;
        if (searchExpandBtn.addEventListener) {
            searchExpandBtn.addEventListener('mouseenter', showTraceAnchorPreview);
            searchExpandBtn.addEventListener('mouseleave', hideTraceAnchorPreview);
            searchExpandBtn.addEventListener('focus', focusSearchAnchorPreview);
            searchExpandBtn.addEventListener('blur', blurSearchAnchorPreview);
        }
    }
}
