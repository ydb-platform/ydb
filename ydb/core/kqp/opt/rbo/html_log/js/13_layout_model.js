/* ══════════════════════════════════════════════════════════════
   Horizontal rule virtualization and canonical layout model
   ══════════════════════════════════════════════════════════════ */

var VIRTUAL_OVERSCAN_SCREENS = 2;
var VIRTUAL_OVERSCAN_MAX_SCREENS = 4;
var SEARCH_EXPAND_SPARSE_GROUP_ROW_THRESHOLD = 64;

function hasDOM() {
    return typeof document !== 'undefined';
}

function currentRuleWidthPx() {
    if (!hasDOM() || !document.documentElement) return RULE_WIDTH_DEFAULT;

    var raw = '';
    if (window.getComputedStyle) {
        raw = window.getComputedStyle(document.documentElement).getPropertyValue('--rule-width');
    }
    return clampRuleWidthForLayout(parseFloat(raw) || RULE_WIDTH_DEFAULT);
}

function tracePaddingPx(side) {
    if (!hasDOM() || !document.querySelector || !window.getComputedStyle) {
        return side === 'right' ? TRACE_PADDING_RIGHT : TRACE_PADDING_LEFT;
    }

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return side === 'right' ? TRACE_PADDING_RIGHT : TRACE_PADDING_LEFT;

    var prop = side === 'right' ? 'padding-right' : 'padding-left';
    var raw = window.getComputedStyle(traceEl).getPropertyValue(prop);
    var value = parseFloat(raw);
    return Number.isFinite(value) ? value : (side === 'right' ? TRACE_PADDING_RIGHT : TRACE_PADDING_LEFT);
}

function enforceRuleWidthBounds() {
    return applyRuleWidth(currentRuleWidthPx());
}

function widthPartsToPx(parts, ruleWidth) {
    return TraceLayout.widthPartsToPx(parts, ruleWidth);
}

function ruleWidthPx(si, gi, ri, ruleWidth) {
    return widthPartsToPx(ruleWidthParts(si, gi, ri), ruleWidth);
}

function topLevelGroupWidthPx(si, gi, ruleWidth) {
    return widthPartsToPx(topLevelGroupWidthParts(si, gi), ruleWidth);
}

function stageExpandedWidthPx(si, ruleWidth) {
    return widthPartsToPx(stageExpandedWidthParts(si), ruleWidth);
}

function clearTraceMeasuredWidthCache(cacheKey) {
    if (cacheKey) {
        delete virtualRuntime().traceMeasuredWidthCache[cacheKey];
    } else {
        virtualRuntime().traceMeasuredWidthCache = {};
    }
    resetLayoutWidthPartsCache();
    virtualRuntime().traceLayoutModel = null;
    virtualRuntime().visibleStageCountCache = null;
    virtualRuntime().traceLayoutGeneration = (virtualRuntime().traceLayoutGeneration || 0) + 1;
    bumpRuntimeEpoch('layout');
    invalidateTraceScrollbarGeometry();
}

function traceLayoutDirtyRegionKey(region) {
    if (!region) return '';
    return [
        region.type || '',
        region.si === undefined ? '' : region.si,
        region.gi === undefined ? '' : region.gi,
        region.ri === undefined ? '' : region.ri
    ].join(':');
}

function markTraceLayoutDirtyRegion(region) {
    if (!region || !region.type) return;
    var dirtyRegions = virtualRuntime().traceLayoutDirtyRegions;
    if (!Array.isArray(dirtyRegions)) {
        dirtyRegions = [];
        virtualRuntime().traceLayoutDirtyRegions = dirtyRegions;
    }
    var dirtyRegionKeys = virtualRuntime().traceLayoutDirtyRegionKeys;
    if (!dirtyRegionKeys || typeof dirtyRegionKeys !== 'object') {
        dirtyRegionKeys = {};
        for (var i = 0; i < dirtyRegions.length; i++) {
            dirtyRegionKeys[traceLayoutDirtyRegionKey(dirtyRegions[i])] = true;
        }
        virtualRuntime().traceLayoutDirtyRegionKeys = dirtyRegionKeys;
    }
    var key = traceLayoutDirtyRegionKey(region);
    if (dirtyRegionKeys[key]) return;
    dirtyRegionKeys[key] = true;
    dirtyRegions.push(region);
    virtualRuntime().traceLayoutGeneration = (virtualRuntime().traceLayoutGeneration || 0) + 1;
    bumpRuntimeEpoch('layout');
    markSearchBaseLayoutCacheDirty(region);
}

function copyTraceLayoutDirtyRegion(region) {
    var copy = {};
    for (var key in region) {
        if (Object.prototype.hasOwnProperty.call(region, key)) copy[key] = region[key];
    }
    return copy;
}

function copyTraceLayoutDirtyRegions(regions) {
    if (!Array.isArray(regions)) return [];
    var copies = [];
    for (var i = 0; i < regions.length; i++) {
        if (regions[i]) copies.push(copyTraceLayoutDirtyRegion(regions[i]));
    }
    return copies;
}

function markTraceLayoutDirtyRule(si, gi, ri, reason) {
    markTraceLayoutDirtyRegion({
        type: 'rule',
        si: si,
        gi: gi,
        ri: ri,
        reason: reason || 'rule-width'
    });
}

function markTraceLayoutDirtyGroup(si, gi, reason) {
    markTraceLayoutDirtyRegion({
        type: 'group',
        si: si,
        gi: gi,
        reason: reason || 'group-open-close'
    });
}

function markTraceLayoutDirtyStage(si, reason) {
    markTraceLayoutDirtyRegion({
        type: 'stage',
        si: si,
        reason: reason || 'stage-open-close'
    });
}

function markTraceLayoutDirtySearchExpansion(query, scope, matches, change) {
    markTraceLayoutDirtyRegion({
        type: 'search-expansion',
        query: String(query || ''),
        scope: scope || '',
        matchCount: Array.isArray(matches) ? matches.length : 0,
        changed: !!(change && change.changed),
        reason: 'search-expansion'
    });
}

function markTraceLayoutDirtyTraceSwitch(fromTraceIndex, toTraceIndex) {
    markTraceLayoutDirtyRegion({
        type: 'trace-switch',
        fromTraceIndex: fromTraceIndex === undefined ? null : fromTraceIndex,
        toTraceIndex: toTraceIndex === undefined ? null : toTraceIndex,
        reason: 'trace-switch'
    });
}

function markTraceLayoutDirtyRulesFromTransition(change, reason) {
    if (!change || !Array.isArray(change.rules)) return;
    for (var i = 0; i < change.rules.length; i++) {
        markTraceLayoutDirtyRule(
            change.rules[i].si,
            change.rules[i].gi,
            change.rules[i].ri,
            reason
        );
    }
}

function markTraceLayoutDirtyStagesFromTransition(change, reason) {
    if (!change || !Array.isArray(change.stages)) return;
    for (var i = 0; i < change.stages.length; i++) {
        markTraceLayoutDirtyStage(change.stages[i].si, reason);
    }
}

function markTraceLayoutDirtyGroupsFromTransition(change, reason) {
    if (!change || !Array.isArray(change.groups)) return;
    for (var i = 0; i < change.groups.length; i++) {
        markTraceLayoutDirtyGroup(
            change.groups[i].si,
            change.groups[i].gi,
            reason
        );
    }
}

function clearTraceLayoutDirtyRegions() {
    var dirtyRegions = virtualRuntime().traceLayoutDirtyRegions;
    if (Array.isArray(dirtyRegions) && dirtyRegions.length) {
        virtualRuntime().lastTraceLayoutDirtyRegions =
            copyTraceLayoutDirtyRegions(dirtyRegions);
    }
    virtualRuntime().traceLayoutDirtyRegions = [];
    virtualRuntime().traceLayoutDirtyRegionKeys = {};
}

function invalidateTraceMeasuredWidthCache(cacheKey) {
    clearTraceMeasuredWidthCache(cacheKey);
    virtualRuntime().traceVirtualLayoutDirty = true;
}

function withTraceMeasuredWidthSuppressed(callback) {
    var previous = virtualRuntime().suppressTraceMeasuredWidth;
    virtualRuntime().suppressTraceMeasuredWidth = true;
    try {
        return callback();
    } finally {
        virtualRuntime().suppressTraceMeasuredWidth = previous;
    }
}

function onTraceMeasuredWidthResize(entries) {
    if (!entries.length) return;
    if (isFullscreenOpen()) return;
    if (typeof shouldVirtualizeStageShellsForCurrentTrace === 'function' &&
        shouldVirtualizeStageShellsForCurrentTrace()) return;

    invalidateTraceMeasuredWidthCache();
    if (resizeRuntime().ruleResizeDrag) {
        scheduleTraceAnchorLineUpdate();
        return;
    }

    scheduleVirtualRowsRefresh();
    scheduleTraceAnchorLineUpdate();
}

function ensureTraceMeasuredWidthObserver() {
    if (!hasDOM() || typeof ResizeObserver === 'undefined') return null;
    if (virtualRuntime().traceMeasuredWidthObserver) return virtualRuntime().traceMeasuredWidthObserver;

    virtualRuntime().traceMeasuredWidthObserver = new ResizeObserver(onTraceMeasuredWidthResize);
    return virtualRuntime().traceMeasuredWidthObserver;
}

function observeMeasuredWidthElement(el, cacheKey) {
    var observer = ensureTraceMeasuredWidthObserver();
    if (!observer || !el) return;

    if (el.__traceMeasuredWidthCacheKey === cacheKey) return;
    el.__traceMeasuredWidthCacheKey = cacheKey;
    observer.observe(el);
}

function measuredElementWidth(id, cacheKey) {
    if (!hasDOM() || !document.getElementById) return null;
    if (typeof shouldVirtualizeStageShellsForCurrentTrace === 'function' &&
        shouldVirtualizeStageShellsForCurrentTrace()) return null;
    if (virtualRuntime().measuringTraceLayoutWidth || virtualRuntime().suppressTraceMeasuredWidth) return null;

    cacheKey = cacheKey || id;
    if (Object.prototype.hasOwnProperty.call(virtualRuntime().traceMeasuredWidthCache, cacheKey)) {
        return virtualRuntime().traceMeasuredWidthCache[cacheKey];
    }

    var el = document.getElementById(id);
    if (!el || !el.getBoundingClientRect) return null;
    observeMeasuredWidthElement(el, cacheKey);

    virtualRuntime().measuringTraceLayoutWidth = true;
    try {
        var rect = el.getBoundingClientRect();
        var width = rect.right - rect.left;
        if (!Number.isFinite(width) || width <= 0) return null;
        virtualRuntime().traceMeasuredWidthCache[cacheKey] = width;
        return width;
    } finally {
        virtualRuntime().measuringTraceLayoutWidth = false;
    }
}

function measuredGroupWidth(si, gi, fallbackWidth) {
    var state = effectiveGroupOpen(si, gi) ? 'open' : 'closed';
    var id = 'group-' + si + '-' + gi;
    return measuredElementWidth(id, id + ':' + state + ':' + fallbackWidth);
}

function measuredStageWidth(si, open, fallbackWidth) {
    var id = (open ? 'stage-exp-' : 'stage-col-') + si;
    return measuredElementWidth(id, id + ':' + fallbackWidth);
}

function topLevelGroupLayoutWidthPx(si, gi, ruleWidth) {
    return Math.max(
        topLevelGroupWidthPx(si, gi, ruleWidth),
        groupTitleFallbackWidthPx(si, gi)
    );
}

function rowModel(rowId, si, gi, globalLeft) {
    return TraceLayout.rowModel(rowId, si, gi, globalLeft);
}

function addRowItem(row, item, gapAfter) {
    TraceLayout.addRowItem(row, item, gapAfter);
}

function ruleRowItem(si, gi, ri, ruleWidth) {
    var reason = ruleWidthReason(si, gi, ri);
    var item = {
        kind: 'rule',
        si: si,
        gi: gi,
        ri: ri,
        key: ruleKey(si, gi, ri),
        width: ruleWidthPx(si, gi, ri, ruleWidth),
        widthReason: reason
    };
    if (reason === 'title-readable-rule') {
        item.titleFallbackWidth = ruleTitleFallbackWidthPx(si, gi, ri);
    }
    return item;
}

function addSparseRuleRangeSpacerSegment(row, segment, ruleWidth) {
    if (!segment || segment.kind !== 'spacer') return;
    var parts = segment.widthParts || { rules: 0, px: 0 };
    var width = widthPartsToPx(parts, ruleWidth);
    if (width <= 0) return;
    addRowItem(row, {
        kind: 'spacer',
        si: segment.si,
        gi: segment.gi,
        key: segment.key,
        width: width,
        widthParts: parts,
        startRi: segment.startRi,
        endRi: segment.endRi
    }, 0);
}

function pushSparseGroupRuleIndex(indexMap, indices, count, ri) {
    ri = Math.floor(Number(ri));
    if (!Number.isInteger(ri) || ri < 0 || ri >= count) return;
    var key = String(ri);
    if (indexMap[key]) return;
    indexMap[key] = true;
    indices.push(ri);
}

function pushSparseGroupRuleRef(indexMap, indices, si, gi, count, ref) {
    if (!ref || ref.si !== si || ref.gi !== gi) return;
    if (!validRuleRef(ref.si, ref.gi, ref.ri)) return;
    pushSparseGroupRuleIndex(indexMap, indices, count, ref.ri);
}

function searchExpandSparseGroupRuleIndices(si, gi, summary) {
    var count = groupRuleCount(si, gi);
    var indexMap = {};
    var indices = [];

    if (summary && Array.isArray(summary.ruleIndices)) {
        for (var i = 0; i < summary.ruleIndices.length; i++) {
            pushSparseGroupRuleIndex(indexMap, indices, count, summary.ruleIndices[i]);
        }
    }

    if (typeof isDiffActive === 'function' && isDiffActive()) {
        pushSparseGroupRuleRef(indexMap, indices, si, gi, count, diffRuntime().diffA);
        pushSparseGroupRuleRef(indexMap, indices, si, gi, count, diffRuntime().diffB);
    }
    if (typeof fullscreenCurrentRule === 'function') {
        pushSparseGroupRuleRef(indexMap, indices, si, gi, count, fullscreenCurrentRule());
    }

    indices.sort(function(a, b) { return a - b; });
    return indices;
}

function sparseRuleRangeGeometrySegment(si, gi, startRi, endRi, totalRuleCount) {
    var parts = collapsedRuleRangeWidthParts(totalRuleCount, startRi, endRi);
    if (parts.px <= 0 && parts.rules <= 0) return null;
    return {
        kind: 'spacer',
        si: si,
        gi: gi,
        key: 'spacer-' + si + '-' + gi + '-' + startRi + '-' + endRi,
        startRi: startRi,
        endRi: endRi,
        widthParts: parts
    };
}

function searchExpandSparseGroupGeometryCacheKey(si, gi, count, indices) {
    return si + '-' + gi + ':' + count + ':' + indices.join(',');
}

function buildSearchExpandSparseGroupGeometry(si, gi, count, indices) {
    var segments = [];
    var nextRi = 0;

    for (var i = 0; i < indices.length; i++) {
        var ri = indices[i];
        if (ri < nextRi) continue;
        var spacer = sparseRuleRangeGeometrySegment(si, gi, nextRi, ri, count);
        if (spacer) segments.push(spacer);
        segments.push({
            kind: 'rule',
            si: si,
            gi: gi,
            ri: ri
        });
        nextRi = ri + 1;
    }

    var trailing = sparseRuleRangeGeometrySegment(si, gi, nextRi, count, count);
    if (trailing) segments.push(trailing);

    return {
        si: si,
        gi: gi,
        ruleCount: count,
        ruleIndices: indices.slice(),
        segments: segments
    };
}

function searchExpandSparseGroupGeometry(si, gi, summary) {
    var count = groupRuleCount(si, gi);
    var indices = searchExpandSparseGroupRuleIndices(si, gi, summary);
    var key = searchExpandSparseGroupGeometryCacheKey(si, gi, count, indices);
    var bucket = layoutWidthPartsCache().searchExpandGroupGeometry;
    if (!bucket[key]) {
        bucket[key] = buildSearchExpandSparseGroupGeometry(si, gi, count, indices);
    }
    return bucket[key];
}

function shouldUseSparseSearchExpandGroupRow(si, gi, summary) {
    return !!(summary && summary.open &&
        groupRuleCount(si, gi) > SEARCH_EXPAND_SPARSE_GROUP_ROW_THRESHOLD);
}

function buildSparseSearchExpandGroupRuleRowModel(si, gi, globalLeft, ruleWidth, summary) {
    var row = rowModel('group-' + si + '-' + gi, si, gi, globalLeft);
    var count = groupRuleCount(si, gi);
    var geometry = searchExpandSparseGroupGeometry(si, gi, summary);
    var segments = geometry && Array.isArray(geometry.segments) ? geometry.segments : [];

    for (var i = 0; i < segments.length; i++) {
        var segment = segments[i];
        if (!segment) continue;
        if (segment.kind === 'spacer') {
            addSparseRuleRangeSpacerSegment(row, segment, ruleWidth);
            continue;
        }
        var ri = segment.ri;
        addRowItem(row, ruleRowItem(si, gi, ri, ruleWidth), ri < count - 1 ? RULE_GAP_WIDTH : 0);
    }

    return row;
}

function buildGroupRuleRowModel(si, gi, globalLeft, ruleWidth) {
    var summary = activeSearchExpandOverlayGroupSummary(si, gi);
    if (shouldUseSparseSearchExpandGroupRow(si, gi, summary)) {
        return buildSparseSearchExpandGroupRuleRowModel(si, gi, globalLeft, ruleWidth, summary);
    }

    var row = rowModel('group-' + si + '-' + gi, si, gi, globalLeft);
    var count = groupRuleCount(si, gi);

    for (var ri = 0; ri < count; ri++) {
        addRowItem(row, ruleRowItem(si, gi, ri, ruleWidth), ri < count - 1 ? RULE_GAP_WIDTH : 0);
    }

    return row;
}

function buildStageRuleRowModel(si, globalLeft, ruleWidth, stageModel, model) {
    var row = rowModel('stage-' + si, si, null, globalLeft);
    var groupsInStage = groupCount(si);

    for (var gi = 0; gi < groupsInStage; gi++) {
        var count = groupRuleCount(si, gi);
        var gapAfter = gi < groupsInStage - 1 ? RULE_GAP_WIDTH : 0;

        if (count <= 1) {
            addRowItem(row, ruleRowItem(si, gi, 0, ruleWidth), gapAfter);
            continue;
        }

        var groupLeft = row.width;
        var groupContentWidth = topLevelGroupWidthPx(si, gi, ruleWidth);
        var groupTitleWidth = groupTitleFallbackWidthPx(si, gi);
        var fallbackGroupWidth = Math.max(groupContentWidth, groupTitleWidth);
        var measuredWidth = measuredGroupWidth(si, gi, fallbackGroupWidth);
        var groupWidth = measuredWidth === null
            ? fallbackGroupWidth
            : Math.max(fallbackGroupWidth, measuredWidth);
        var groupItem = {
            kind: 'group',
            si: si,
            gi: gi,
            key: si + '-' + gi,
            width: groupWidth,
            contentWidth: groupContentWidth,
            titleFallbackWidth: groupTitleWidth,
            row: null
        };

        if (effectiveGroupOpen(si, gi)) {
            groupItem.row = buildGroupRuleRowModel(si, gi, globalLeft + groupLeft, ruleWidth);
            if (groupWidth > groupItem.row.width) groupItem.row.surfaceWidth = groupWidth;
            model.rows.push(groupItem.row);
        }

        addRowItem(row, groupItem, gapAfter);
    }

    stageModel.row = row;
    model.rows.push(row);
    return row;
}

function indexRuleIntervals(model) {
    TraceLayout.indexRuleIntervals(model);
}

function traceLayoutIndex(paddingLeft, paddingRight) {
    return {
        paddingLeft: paddingLeft,
        paddingRight: paddingRight,
        visibleStageCount: 0,
        visibleStages: [],
        visibleStagePrefixes: [],
        visibleStagePrefixWidth: 0
    };
}

function addTraceLayoutVisibleStage(model, stageModel) {
    if (!model || !model.index || !stageModel || stageModel.hidden) return;
    var before = model.index.visibleStagePrefixWidth || 0;
    var outerWidth = stageModel.width + STAGE_GAP_WIDTH;
    model.index.visibleStageCount++;
    model.index.visibleStages.push(stageModel);
    model.index.visibleStagePrefixes.push({
        si: stageModel.si,
        left: stageModel.left,
        right: stageModel.left + stageModel.width,
        width: stageModel.width,
        outerWidth: outerWidth,
        prefixWidthBefore: before,
        prefixWidthAfter: before + outerWidth
    });
    model.index.visibleStagePrefixWidth = before + outerWidth;
}

function layoutInvariantResult() {
    return {
        ok: true,
        errors: [],
        warnings: [],
        stats: {
            stages: 0,
            rows: 0,
            items: 0,
            rules: 0
        }
    };
}

function pushLayoutInvariantIssue(result, severity, code, path, message, details) {
    var issue = {
        code: code,
        path: path,
        message: message
    };
    if (details !== undefined) issue.details = details;
    result[severity].push(issue);
    if (severity === 'errors') result.ok = false;
}

function pushLayoutInvariantError(result, code, path, message, details) {
    pushLayoutInvariantIssue(result, 'errors', code, path, message, details);
}

function finiteLayoutNumber(value) {
    return typeof value === 'number' && Number.isFinite(value);
}

function nearLayoutNumber(actual, expected, tolerance) {
    return finiteLayoutNumber(actual) &&
           finiteLayoutNumber(expected) &&
           Math.abs(actual - expected) <= tolerance;
}

function validateLayoutNumber(result, value, path, options) {
    if (!finiteLayoutNumber(value)) {
        pushLayoutInvariantError(result, 'non_finite_number', path, 'Expected a finite number', value);
        return false;
    }
    if (value < -options.tolerance) {
        pushLayoutInvariantError(result, 'negative_number', path, 'Expected a non-negative number', value);
        return false;
    }
    return true;
}

function layoutIntegerInRange(value, min, maxExclusive) {
    return Number.isInteger(value) && value >= min && value < maxExclusive;
}

function validateTraceLayoutRow(model, row, rowIndex, rowIds, ruleItems, groupItems, result, options) {
    var path = 'rows[' + rowIndex + ']';
    if (!row || typeof row !== 'object') {
        pushLayoutInvariantError(result, 'row_missing', path, 'Row must be an object');
        return;
    }

    result.stats.rows++;
    if (!row.id) {
        pushLayoutInvariantError(result, 'row_id_missing', path + '.id', 'Row id is required');
    } else if (rowIds[row.id]) {
        pushLayoutInvariantError(result, 'row_id_duplicate', path + '.id', 'Row id must be unique', row.id);
    } else {
        rowIds[row.id] = row;
    }

    validateLayoutNumber(result, row.globalLeft, path + '.globalLeft', options);
    validateLayoutNumber(result, row.width, path + '.width', options);
    if (!Array.isArray(row.items)) {
        pushLayoutInvariantError(result, 'row_items_missing', path + '.items', 'Row items must be an array');
        return;
    }

    var rowStageValid = layoutIntegerInRange(row.si, 0, currentStageCount());
    if (!rowStageValid) {
        pushLayoutInvariantError(result, 'row_stage_invalid', path + '.si', 'Row stage index is invalid', row.si);
    }
    if (rowStageValid && row.gi !== null && row.gi !== undefined &&
        !layoutIntegerInRange(row.gi, 0, groupCount(row.si))) {
        pushLayoutInvariantError(result, 'row_group_invalid', path + '.gi', 'Row group index is invalid', row.gi);
    }

    var expectedLeft = 0;
    for (var i = 0; i < row.items.length; i++) {
        var item = row.items[i];
        var itemPath = path + '.items[' + i + ']';
        if (!item || typeof item !== 'object') {
            pushLayoutInvariantError(result, 'row_item_missing', itemPath, 'Row item must be an object');
            continue;
        }
        result.stats.items++;

        if (item.kind !== 'rule' && item.kind !== 'group' && item.kind !== 'spacer') {
            pushLayoutInvariantError(result, 'row_item_kind_invalid', itemPath + '.kind', 'Row item kind is invalid', item.kind);
        }
        if (item.si !== row.si) {
            pushLayoutInvariantError(result, 'row_item_stage_mismatch', itemPath + '.si', 'Row item stage must match row stage', {
                row: row.si,
                item: item.si
            });
        }

        validateLayoutNumber(result, item.left, itemPath + '.left', options);
        validateLayoutNumber(result, item.globalLeft, itemPath + '.globalLeft', options);
        validateLayoutNumber(result, item.width, itemPath + '.width', options);
        validateLayoutNumber(result, item.gapAfter || 0, itemPath + '.gapAfter', options);
        validateLayoutNumber(result, item.outerWidth, itemPath + '.outerWidth', options);

        if (!nearLayoutNumber(item.left, expectedLeft, options.tolerance)) {
            pushLayoutInvariantError(result, 'row_item_left_mismatch', itemPath + '.left', 'Row item local left does not match accumulated width', {
                actual: item.left,
                expected: expectedLeft
            });
        }
        if (!nearLayoutNumber(item.globalLeft, row.globalLeft + item.left, options.tolerance)) {
            pushLayoutInvariantError(result, 'row_item_global_left_mismatch', itemPath + '.globalLeft', 'Row item global left does not match row origin plus local left', {
                actual: item.globalLeft,
                expected: row.globalLeft + item.left
            });
        }
        if (!nearLayoutNumber(item.outerWidth, item.width + (item.gapAfter || 0), options.tolerance)) {
            pushLayoutInvariantError(result, 'row_item_outer_width_mismatch', itemPath + '.outerWidth', 'Row item outer width must equal width plus trailing gap', {
                actual: item.outerWidth,
                expected: item.width + (item.gapAfter || 0)
            });
        }

        if (item.kind === 'rule') {
            validateTraceLayoutRuleItem(model, row, item, itemPath, ruleItems, result, options);
        } else if (item.kind === 'group') {
            validateTraceLayoutGroupItem(model, row, item, itemPath, groupItems, result, options);
        } else if (item.kind === 'spacer') {
            validateTraceLayoutSpacerItem(model, row, item, itemPath, result, options);
        }

        expectedLeft += finiteLayoutNumber(item.outerWidth) ? item.outerWidth : 0;
    }

    if (!nearLayoutNumber(row.width, expectedLeft, options.tolerance)) {
        pushLayoutInvariantError(result, 'row_width_mismatch', path + '.width', 'Row width must equal the sum of item outer widths', {
            actual: row.width,
            expected: expectedLeft
        });
    }
}

function validateTraceLayoutSpacerItem(model, row, item, path, result, options) {
    if (!item.key) {
        pushLayoutInvariantError(result, 'spacer_key_missing', path + '.key', 'Spacer item key is required');
    }
    if (row.gi !== null && row.gi !== undefined && item.gi !== row.gi) {
        pushLayoutInvariantError(result, 'spacer_row_group_mismatch', path + '.gi', 'Nested spacer item must belong to the row group', {
            row: row.gi,
            item: item.gi
        });
    }
    if (item.widthParts) {
        var expectedWidth = widthPartsToPx(item.widthParts, model.ruleWidth);
        if (!nearLayoutNumber(item.width, expectedWidth, options.tolerance)) {
            pushLayoutInvariantError(result, 'spacer_width_parts_mismatch', path + '.width', 'Spacer item width does not match its width parts', {
                actual: item.width,
                expected: expectedWidth
            });
        }
    }
}

function validateTraceLayoutRuleItem(model, row, item, path, ruleItems, result, options) {
    if (!layoutIntegerInRange(item.si, 0, currentStageCount())) {
        pushLayoutInvariantError(result, 'rule_stage_invalid', path + '.si', 'Rule stage index is invalid', item.si);
        return;
    }
    if (!layoutIntegerInRange(item.gi, 0, groupCount(item.si))) {
        pushLayoutInvariantError(result, 'rule_group_invalid', path + '.gi', 'Rule group index is invalid', item.gi);
        return;
    }
    if (!layoutIntegerInRange(item.ri, 0, groupRuleCount(item.si, item.gi))) {
        pushLayoutInvariantError(result, 'rule_index_invalid', path + '.ri', 'Rule index is invalid', item.ri);
        return;
    }
    if (row.gi !== null && row.gi !== undefined && item.gi !== row.gi) {
        pushLayoutInvariantError(result, 'rule_row_group_mismatch', path + '.gi', 'Nested rule item must belong to the row group', {
            row: row.gi,
            item: item.gi
        });
    }

    var expectedKey = ruleKey(item.si, item.gi, item.ri);
    if (item.key !== expectedKey) {
        pushLayoutInvariantError(result, 'rule_key_mismatch', path + '.key', 'Rule item key does not match its indices', {
            actual: item.key,
            expected: expectedKey
        });
    }
    if (ruleItems[expectedKey]) {
        pushLayoutInvariantError(result, 'rule_item_duplicate', path + '.key', 'Rule item appears in more than one visible row', expectedKey);
    }
    ruleItems[expectedKey] = {
        item: item,
        rowId: row.id
    };

    var expectedWidth = ruleWidthPx(item.si, item.gi, item.ri, model.ruleWidth);
    if (!nearLayoutNumber(item.width, expectedWidth, options.tolerance)) {
        pushLayoutInvariantError(result, 'rule_width_mismatch', path + '.width', 'Rule item width does not match rule state', {
            actual: item.width,
            expected: expectedWidth
        });
    }
}

function validateTraceLayoutGroupItem(model, row, item, path, groupItems, result, options) {
    if (row.gi !== null && row.gi !== undefined) {
        pushLayoutInvariantError(result, 'nested_group_item', path + '.kind', 'Group items are only valid in stage rows');
    }
    if (!layoutIntegerInRange(item.si, 0, currentStageCount())) {
        pushLayoutInvariantError(result, 'group_stage_invalid', path + '.si', 'Group stage index is invalid', item.si);
        return;
    }
    if (!layoutIntegerInRange(item.gi, 0, groupCount(item.si))) {
        pushLayoutInvariantError(result, 'group_index_invalid', path + '.gi', 'Group index is invalid', item.gi);
        return;
    }

    var expectedKey = item.si + '-' + item.gi;
    if (item.key !== expectedKey) {
        pushLayoutInvariantError(result, 'group_key_mismatch', path + '.key', 'Group item key does not match its indices', {
            actual: item.key,
            expected: expectedKey
        });
    }
    if (groupItems[expectedKey]) {
        pushLayoutInvariantError(result, 'group_item_duplicate', path + '.key', 'Group item appears in more than one visible row', expectedKey);
    }
    groupItems[expectedKey] = {
        item: item,
        rowId: row.id
    };

    var groupOpen = effectiveGroupOpen(item.si, item.gi);
    if (groupOpen && !item.row) {
        pushLayoutInvariantError(result, 'open_group_row_missing', path + '.row', 'Open group item must reference its nested row');
    }
    if (!groupOpen && item.row) {
        pushLayoutInvariantError(result, 'closed_group_row_present', path + '.row', 'Closed group item must not reference a nested row');
    }
    if (item.row) {
        if (item.row.id !== 'group-' + item.si + '-' + item.gi) {
            pushLayoutInvariantError(result, 'group_row_id_mismatch', path + '.row.id', 'Group row id does not match group item', item.row.id);
        }
        if (!nearLayoutNumber(item.row.globalLeft, item.globalLeft, options.tolerance)) {
            pushLayoutInvariantError(result, 'group_row_left_mismatch', path + '.row.globalLeft', 'Group row global left must match group item global left', {
                actual: item.row.globalLeft,
                expected: item.globalLeft
            });
        }
    }
}

function markTraceLayoutReachableRows(row, rowIds, reachableRows, result, sourcePath) {
    if (!row || !row.id) return;
    if (!rowIds[row.id]) {
        pushLayoutInvariantError(result, 'reachable_row_not_indexed', sourcePath || 'row', 'Reachable row must be present in model.rows', row.id);
        return;
    }
    if (reachableRows[row.id]) return;

    reachableRows[row.id] = true;
    if (!Array.isArray(row.items)) return;

    for (var i = 0; i < row.items.length; i++) {
        var item = row.items[i];
        if (item && item.kind === 'group' && item.row) {
            markTraceLayoutReachableRows(
                item.row,
                rowIds,
                reachableRows,
                result,
                (sourcePath || row.id) + '.items[' + i + '].row'
            );
        }
    }
}

function validateTraceLayoutStage(model, si, rowIds, reachableRows, result, options) {
    var stage = model.stages[si];
    var path = 'stages[' + si + ']';
    if (!stage || typeof stage !== 'object') {
        pushLayoutInvariantError(result, 'stage_missing', path, 'Stage model is missing');
        return;
    }

    result.stats.stages++;
    if (stage.si !== si) {
        pushLayoutInvariantError(result, 'stage_index_mismatch', path + '.si', 'Stage index does not match its position', {
            actual: stage.si,
            expected: si
        });
    }
    validateLayoutNumber(result, stage.left, path + '.left', options);
    validateLayoutNumber(result, stage.width, path + '.width', options);

    if (si >= currentStageCount()) {
        pushLayoutInvariantError(result, 'stage_out_of_trace', path, 'Stage model has no matching current trace stage', si);
        return;
    }

    var visible = stageVisible(si);
    var expectedHidden = !visible;
    if (!!stage.hidden !== expectedHidden) {
        pushLayoutInvariantError(result, 'stage_visibility_mismatch', path + '.hidden', 'Stage hidden flag does not match current trace visibility', {
            hidden: !!stage.hidden,
            visible: visible
        });
    }

    if (stage.hidden) {
        if (Math.abs(stage.width) > options.tolerance) {
            pushLayoutInvariantError(result, 'hidden_stage_width', path + '.width', 'Hidden stage must have zero width', stage.width);
        }
        if (stage.row) {
            pushLayoutInvariantError(result, 'hidden_stage_row', path + '.row', 'Hidden stage must not have a row');
        }
        return;
    }

    if (stage.open !== effectiveStageOpen(si)) {
        pushLayoutInvariantError(result, 'stage_open_mismatch', path + '.open', 'Stage open flag does not match current UI state', {
            model: stage.open,
            state: effectiveStageOpen(si)
        });
    }

    var expectsRow = stage.open && stageHasRules(si);
    if (expectsRow && !stage.row) {
        pushLayoutInvariantError(result, 'open_stage_row_missing', path + '.row', 'Open stage with rules must have a row');
    }
    if (!expectsRow && stage.row) {
        pushLayoutInvariantError(result, 'closed_stage_row_present', path + '.row', 'Closed or empty stage must not have a row');
    }
    if (!stage.row) return;

    if (stage.row.id !== 'stage-' + si) {
        pushLayoutInvariantError(result, 'stage_row_id_mismatch', path + '.row.id', 'Stage row id does not match stage', stage.row.id);
    }
    if (!nearLayoutNumber(stage.row.globalLeft, stage.left, options.tolerance)) {
        pushLayoutInvariantError(result, 'stage_row_left_mismatch', path + '.row.globalLeft', 'Stage row global left must match stage left', {
            actual: stage.row.globalLeft,
            expected: stage.left
        });
    }
    if (stage.row.width - stage.width > options.tolerance) {
        pushLayoutInvariantError(result, 'stage_width_too_small', path + '.width', 'Stage width must cover its row width', {
            stageWidth: stage.width,
            rowWidth: stage.row.width
        });
    }
    if (!rowIds[stage.row.id]) {
        pushLayoutInvariantError(result, 'stage_row_not_indexed', path + '.row', 'Stage row must be present in model.rows', stage.row.id);
    } else {
        markTraceLayoutReachableRows(stage.row, rowIds, reachableRows, result, path + '.row');
    }
}

function validateTraceLayoutRuleIndex(model, ruleItems, result, options) {
    if (!model.rules || typeof model.rules !== 'object') {
        pushLayoutInvariantError(result, 'rule_index_missing_all', 'rules', 'Model rule index must be an object');
        return;
    }

    var seen = {};
    for (var key in ruleItems) {
        if (!Object.prototype.hasOwnProperty.call(ruleItems, key)) continue;
        var entry = ruleItems[key];
        var item = entry.item;
        var interval = model.rules[key];
        var path = 'rules.' + key;
        result.stats.rules++;

        if (!interval) {
            pushLayoutInvariantError(result, 'rule_index_missing', path, 'Visible rule item is missing from model.rules', key);
            continue;
        }
        seen[key] = true;
        if (interval.si !== item.si || interval.gi !== item.gi || interval.ri !== item.ri ||
            interval.key !== key || interval.rowId !== entry.rowId) {
            pushLayoutInvariantError(result, 'rule_index_identity_mismatch', path, 'Rule interval identity does not match row item', {
                interval: {
                    si: interval.si,
                    gi: interval.gi,
                    ri: interval.ri,
                    key: interval.key,
                    rowId: interval.rowId
                },
                item: {
                    si: item.si,
                    gi: item.gi,
                    ri: item.ri,
                    key: key,
                    rowId: entry.rowId
                }
            });
        }
        if (!nearLayoutNumber(interval.left, item.globalLeft, options.tolerance) ||
            !nearLayoutNumber(interval.right, item.globalLeft + item.width, options.tolerance) ||
            !nearLayoutNumber(interval.width, item.width, options.tolerance)) {
            pushLayoutInvariantError(result, 'rule_index_geometry_mismatch', path, 'Rule interval geometry does not match row item', {
                interval: {
                    left: interval.left,
                    right: interval.right,
                    width: interval.width
                },
                item: {
                    left: item.globalLeft,
                    right: item.globalLeft + item.width,
                    width: item.width
                }
            });
        }
    }

    for (var indexKey in model.rules) {
        if (!Object.prototype.hasOwnProperty.call(model.rules, indexKey)) continue;
        if (seen[indexKey]) continue;
        pushLayoutInvariantError(result, 'rule_index_stale', 'rules.' + indexKey, 'Model rule index contains a rule that is not in visible rows', indexKey);
    }
}

function validateTraceLayoutGroupIndex(model, groupItems, result, options) {
    if (!model.groups || typeof model.groups !== 'object') {
        pushLayoutInvariantError(result, 'group_index_missing_all', 'groups', 'Model group index must be an object');
        return;
    }

    var seen = {};
    for (var key in groupItems) {
        if (!Object.prototype.hasOwnProperty.call(groupItems, key)) continue;
        var entry = groupItems[key];
        var item = entry.item;
        var interval = model.groups[key];
        var path = 'groups.' + key;

        if (!interval) {
            pushLayoutInvariantError(result, 'group_index_missing', path, 'Visible group item is missing from model.groups', key);
            continue;
        }
        seen[key] = true;
        if (interval.si !== item.si || interval.gi !== item.gi ||
            interval.key !== key || interval.rowId !== entry.rowId) {
            pushLayoutInvariantError(result, 'group_index_identity_mismatch', path, 'Group interval identity does not match row item', {
                interval: {
                    si: interval.si,
                    gi: interval.gi,
                    key: interval.key,
                    rowId: interval.rowId
                },
                item: {
                    si: item.si,
                    gi: item.gi,
                    key: key,
                    rowId: entry.rowId
                }
            });
        }
        if (!nearLayoutNumber(interval.left, item.globalLeft, options.tolerance) ||
            !nearLayoutNumber(interval.right, item.globalLeft + item.width, options.tolerance) ||
            !nearLayoutNumber(interval.width, item.width, options.tolerance)) {
            pushLayoutInvariantError(result, 'group_index_geometry_mismatch', path, 'Group interval geometry does not match row item', {
                interval: {
                    left: interval.left,
                    right: interval.right,
                    width: interval.width
                },
                item: {
                    left: item.globalLeft,
                    right: item.globalLeft + item.width,
                    width: item.width
                }
            });
        }
    }

    for (var indexKey in model.groups) {
        if (!Object.prototype.hasOwnProperty.call(model.groups, indexKey)) continue;
        if (seen[indexKey]) continue;
        pushLayoutInvariantError(result, 'group_index_stale', 'groups.' + indexKey, 'Model group index contains a group that is not in visible rows', indexKey);
    }
}

function intervalBucketContains(bucket, interval) {
    if (!Array.isArray(bucket)) return false;
    for (var i = 0; i < bucket.length; i++) {
        if (bucket[i] === interval) return true;
    }
    return false;
}

function validateStageIntervalBucket(result, index, bucketName, collectionName, collection, items, missingCode, staleCode) {
    var buckets = index[bucketName];
    if (!buckets || typeof buckets !== 'object' || Array.isArray(buckets)) {
        pushLayoutInvariantError(result, bucketName + '_missing', 'index.' + bucketName, 'Stage interval bucket index must be an object');
        return;
    }

    for (var key in items) {
        if (!Object.prototype.hasOwnProperty.call(items, key)) continue;
        var item = items[key].item;
        var interval = collection[key];
        if (!interval) continue;
        if (!intervalBucketContains(buckets[String(item.si)], interval)) {
            pushLayoutInvariantError(result, missingCode, 'index.' + bucketName + '[' + item.si + ']', 'Stage interval bucket is missing a visible interval', key);
        }
    }

    for (var stageKey in buckets) {
        if (!Object.prototype.hasOwnProperty.call(buckets, stageKey)) continue;
        var bucket = buckets[stageKey];
        if (!Array.isArray(bucket)) {
            pushLayoutInvariantError(result, bucketName + '_bucket_invalid', 'index.' + bucketName + '[' + stageKey + ']', 'Stage interval bucket must be an array');
            continue;
        }
        for (var i = 0; i < bucket.length; i++) {
            var intervalEntry = bucket[i];
            if (!intervalEntry || collection[intervalEntry.key] !== intervalEntry ||
                String(intervalEntry.si) !== String(stageKey)) {
                pushLayoutInvariantError(result, staleCode, 'index.' + bucketName + '[' + stageKey + '][' + i + ']', 'Stage interval bucket contains a stale interval', {
                    bucket: bucketName,
                    collection: collectionName,
                    key: intervalEntry && intervalEntry.key,
                    si: intervalEntry && intervalEntry.si
                });
            }
        }
    }
}

function validateTraceLayoutStageIntervalIndexes(model, ruleItems, groupItems, result) {
    var index = model.index;
    if (!index || typeof index !== 'object') return;
    validateStageIntervalBucket(
        result,
        index,
        'stageRuleIntervals',
        'rules',
        model.rules || {},
        ruleItems,
        'stage_rule_interval_missing',
        'stage_rule_interval_stale'
    );
    validateStageIntervalBucket(
        result,
        index,
        'stageGroupIntervals',
        'groups',
        model.groups || {},
        groupItems,
        'stage_group_interval_missing',
        'stage_group_interval_stale'
    );
}

function validateTraceLayoutVisibleStageIndex(model, result, options) {
    var index = model.index;
    if (!index || typeof index !== 'object') {
        pushLayoutInvariantError(result, 'layout_index_missing', 'index', 'Model layout index must be an object');
        return;
    }
    if (!Array.isArray(index.visibleStages)) {
        pushLayoutInvariantError(result, 'visible_stage_index_missing', 'index.visibleStages', 'Visible stage index must be an array');
        return;
    }
    if (!Array.isArray(index.visibleStagePrefixes)) {
        pushLayoutInvariantError(result, 'visible_stage_prefix_index_missing', 'index.visibleStagePrefixes', 'Visible stage prefix index must be an array');
        return;
    }

    validateLayoutNumber(result, index.paddingLeft, 'index.paddingLeft', options);
    validateLayoutNumber(result, index.paddingRight, 'index.paddingRight', options);
    validateLayoutNumber(result, index.visibleStagePrefixWidth, 'index.visibleStagePrefixWidth', options);

    var visibleStages = [];
    for (var si = 0; si < model.stages.length; si++) {
        if (model.stages[si] && !model.stages[si].hidden) visibleStages.push(model.stages[si]);
    }

    if (index.visibleStageCount !== visibleStages.length) {
        pushLayoutInvariantError(result, 'visible_stage_count_mismatch', 'index.visibleStageCount', 'Visible stage count index must match model stages', {
            actual: index.visibleStageCount,
            expected: visibleStages.length
        });
    }
    if (index.visibleStages.length !== visibleStages.length) {
        pushLayoutInvariantError(result, 'visible_stage_index_length_mismatch', 'index.visibleStages.length', 'Visible stage index length must match model stages', {
            actual: index.visibleStages.length,
            expected: visibleStages.length
        });
    }
    if (index.visibleStagePrefixes.length !== visibleStages.length) {
        pushLayoutInvariantError(result, 'visible_stage_prefix_length_mismatch', 'index.visibleStagePrefixes.length', 'Visible stage prefix index length must match model stages', {
            actual: index.visibleStagePrefixes.length,
            expected: visibleStages.length
        });
    }

    var prefix = 0;
    for (var i = 0; i < visibleStages.length; i++) {
        var stage = visibleStages[i];
        var indexedStage = index.visibleStages[i];
        var entry = index.visibleStagePrefixes[i];

        if (indexedStage !== stage) {
            pushLayoutInvariantError(result, 'visible_stage_index_mismatch', 'index.visibleStages[' + i + ']', 'Visible stage index entry must reference the matching model stage', {
                actual: indexedStage && indexedStage.si,
                expected: stage.si
            });
        }
        if (!entry || typeof entry !== 'object') {
            pushLayoutInvariantError(result, 'visible_stage_prefix_missing', 'index.visibleStagePrefixes[' + i + ']', 'Visible stage prefix entry must be an object');
            continue;
        }

        var path = 'index.visibleStagePrefixes[' + i + ']';
        validateLayoutNumber(result, entry.left, path + '.left', options);
        validateLayoutNumber(result, entry.right, path + '.right', options);
        validateLayoutNumber(result, entry.width, path + '.width', options);
        validateLayoutNumber(result, entry.outerWidth, path + '.outerWidth', options);
        validateLayoutNumber(result, entry.prefixWidthBefore, path + '.prefixWidthBefore', options);
        validateLayoutNumber(result, entry.prefixWidthAfter, path + '.prefixWidthAfter', options);

        if (entry.si !== stage.si) {
            pushLayoutInvariantError(result, 'visible_stage_prefix_stage_mismatch', path + '.si', 'Visible stage prefix must identify the matching stage', {
                actual: entry.si,
                expected: stage.si
            });
        }
        if (!nearLayoutNumber(entry.left, stage.left, options.tolerance) ||
            !nearLayoutNumber(entry.right, stage.left + stage.width, options.tolerance) ||
            !nearLayoutNumber(entry.width, stage.width, options.tolerance)) {
            pushLayoutInvariantError(result, 'visible_stage_prefix_geometry_mismatch', path, 'Visible stage prefix geometry must match the stage model', {
                entry: {
                    left: entry.left,
                    right: entry.right,
                    width: entry.width
                },
                stage: {
                    left: stage.left,
                    right: stage.left + stage.width,
                    width: stage.width
                }
            });
        }
        if (!nearLayoutNumber(entry.prefixWidthBefore, prefix, options.tolerance) ||
            !nearLayoutNumber(entry.outerWidth, stage.width + STAGE_GAP_WIDTH, options.tolerance) ||
            !nearLayoutNumber(entry.prefixWidthAfter, prefix + entry.outerWidth, options.tolerance)) {
            pushLayoutInvariantError(result, 'visible_stage_prefix_width_mismatch', path, 'Visible stage prefix widths must match accumulated visible stage widths', {
                actual: {
                    before: entry.prefixWidthBefore,
                    outer: entry.outerWidth,
                    after: entry.prefixWidthAfter
                },
                expected: {
                    before: prefix,
                    outer: stage.width + STAGE_GAP_WIDTH,
                    after: prefix + stage.width + STAGE_GAP_WIDTH
                }
            });
        }

        prefix += stage.width + STAGE_GAP_WIDTH;
    }

    if (!nearLayoutNumber(index.visibleStagePrefixWidth, prefix, options.tolerance)) {
        pushLayoutInvariantError(result, 'visible_stage_prefix_total_mismatch', 'index.visibleStagePrefixWidth', 'Visible stage prefix total must match accumulated visible stage widths', {
            actual: index.visibleStagePrefixWidth,
            expected: prefix
        });
    }
    if (finiteLayoutNumber(index.paddingLeft) &&
        finiteLayoutNumber(index.paddingRight) &&
        !nearLayoutNumber(model.totalWidth, index.paddingLeft + prefix + index.paddingRight, options.tolerance)) {
        pushLayoutInvariantError(result, 'visible_stage_prefix_total_width_mismatch', 'totalWidth', 'Model total width must match padding plus visible stage prefix width', {
            actual: model.totalWidth,
            expected: index.paddingLeft + prefix + index.paddingRight
        });
    }
}

function validateTraceLayoutModel(model, options) {
    options = options || {};
    options.tolerance = Number.isFinite(options.tolerance) ? Math.max(0, options.tolerance) : 0.01;

    var result = layoutInvariantResult();
    if (!model || typeof model !== 'object') {
        pushLayoutInvariantError(result, 'model_missing', 'model', 'Layout model must be an object');
        return result;
    }

    validateLayoutNumber(result, model.ruleWidth, 'ruleWidth', options);
    if (model.ruleWidth <= 0) {
        pushLayoutInvariantError(result, 'rule_width_non_positive', 'ruleWidth', 'Rule width must be positive', model.ruleWidth);
    }
    validateLayoutNumber(result, model.totalWidth, 'totalWidth', options);

    if (!Array.isArray(model.stages)) {
        pushLayoutInvariantError(result, 'stages_missing', 'stages', 'Model stages must be an array');
        return result;
    }
    if (!Array.isArray(model.rows)) {
        pushLayoutInvariantError(result, 'rows_missing', 'rows', 'Model rows must be an array');
        return result;
    }
    if (model.stages.length !== currentStageCount()) {
        pushLayoutInvariantError(result, 'stage_count_mismatch', 'stages.length', 'Model stage count must match the current trace', {
            actual: model.stages.length,
            expected: currentStageCount()
        });
    }

    var rowIds = {};
    var ruleItems = {};
    var groupItems = {};
    for (var ri = 0; ri < model.rows.length; ri++) {
        validateTraceLayoutRow(model, model.rows[ri], ri, rowIds, ruleItems, groupItems, result, options);
    }

    var reachableRows = {};
    var previousLeft = -Infinity;
    var previousVisibleStage = null;
    for (var si = 0; si < model.stages.length; si++) {
        validateTraceLayoutStage(model, si, rowIds, reachableRows, result, options);
        var stage = model.stages[si];
        if (!stage || !finiteLayoutNumber(stage.left)) continue;
        if (stage.left + options.tolerance < previousLeft) {
            pushLayoutInvariantError(result, 'stage_left_regressed', 'stages[' + si + '].left', 'Stage left positions must be monotonic', {
                actual: stage.left,
                previous: previousLeft
            });
        }
        if (previousVisibleStage && !stage.hidden &&
            stage.left + options.tolerance < previousVisibleStage.left + previousVisibleStage.width) {
            pushLayoutInvariantError(result, 'stage_overlap', 'stages[' + si + '].left', 'Visible stages must not overlap', {
                actual: stage.left,
                previousRight: previousVisibleStage.left + previousVisibleStage.width
            });
        }
        previousLeft = Math.max(previousLeft, stage.left);
        if (!stage.hidden) previousVisibleStage = stage;
    }

    for (var rowId in rowIds) {
        if (!Object.prototype.hasOwnProperty.call(rowIds, rowId)) continue;
        if (!reachableRows[rowId]) {
            pushLayoutInvariantError(result, 'row_unreachable', 'rows.' + rowId, 'Row is not reachable from a stage or open group', rowId);
        }
    }

    validateTraceLayoutRuleIndex(model, ruleItems, result, options);
    validateTraceLayoutGroupIndex(model, groupItems, result, options);
    validateTraceLayoutStageIntervalIndexes(model, ruleItems, groupItems, result);
    validateTraceLayoutVisibleStageIndex(model, result, options);

    var maxRight = 0;
    for (var i = 0; i < model.rows.length; i++) {
        var currentRow = model.rows[i];
        if (!currentRow || !Array.isArray(currentRow.items)) continue;
        for (var j = 0; j < currentRow.items.length; j++) {
            var currentItem = currentRow.items[j];
            if (!currentItem || !finiteLayoutNumber(currentItem.globalLeft) ||
                !finiteLayoutNumber(currentItem.width)) continue;
            maxRight = Math.max(maxRight, currentItem.globalLeft + currentItem.width);
        }
    }
    if (maxRight - model.totalWidth > options.tolerance) {
        pushLayoutInvariantError(result, 'total_width_too_small', 'totalWidth', 'Model total width must cover all visible row items', {
            actual: model.totalWidth,
            minimum: maxRight
        });
    }

    return result;
}

function traceLayoutInvariantSummary(validation, context) {
    var prefix = 'Trace layout invariant failed';
    if (context) prefix += ' (' + context + ')';
    if (!validation || !validation.errors || !validation.errors.length) return prefix;

    var parts = [];
    for (var i = 0; i < validation.errors.length && i < 5; i++) {
        var error = validation.errors[i];
        parts.push(error.code + ' at ' + error.path + ': ' + error.message);
    }
    if (validation.errors.length > parts.length) {
        parts.push('+' + (validation.errors.length - parts.length) + ' more');
    }
    return prefix + ': ' + parts.join('; ');
}

function assertTraceLayoutModel(model, context, options) {
    var validation = validateTraceLayoutModel(model, options);
    if (!validation.ok) {
        var summary = traceLayoutInvariantSummary(validation, context);
        recordInvariantFailure('layout-model', context, validation, summary);
        throw new Error(summary);
    }
    return validation;
}

function buildTraceLayoutModel(ruleWidth) {
    ruleWidth = clampRuleWidthForLayout(ruleWidth || currentRuleWidthPx());
    var paddingLeft = tracePaddingPx('left');
    var paddingRight = tracePaddingPx('right');

    var model = {
        ruleWidth: ruleWidth,
        index: traceLayoutIndex(paddingLeft, paddingRight),
        stages: [],
        rows: [],
        rules: {},
        groups: {},
        totalWidth: paddingLeft + paddingRight
    };
    var x = paddingLeft;

    for (var si = 0; si < currentStageCount(); si++) {
        if (!stageVisible(si)) {
            model.stages.push({
                si: si,
                open: false,
                hidden: true,
                left: x,
                width: 0,
                row: null
            });
            continue;
        }

        var open = effectiveStageOpen(si);
        var fallbackWidth = open
            ? (stageHasRules(si) ? stageExpandedWidthPx(si, ruleWidth) : EMPTY_STAGE_EXPANDED_WIDTH)
            : STAGE_COLLAPSED_WIDTH;
        if (open) fallbackWidth = Math.max(fallbackWidth, stageTitleFallbackWidthPx(si));
        var measuredWidth = measuredStageWidth(si, open, fallbackWidth);
        var width = measuredWidth === null
            ? fallbackWidth
            : Math.max(fallbackWidth, measuredWidth);
        var stageModel = {
            si: si,
            open: open,
            left: x,
            width: width,
            row: null
        };

        if (open && stageHasRules(si)) {
            buildStageRuleRowModel(si, x, ruleWidth, stageModel, model);
            stageModel.width = Math.max(stageModel.width, stageModel.row.width);
            width = stageModel.width;
        }

        model.stages.push(stageModel);
        addTraceLayoutVisibleStage(model, stageModel);
        x += width + STAGE_GAP_WIDTH;
    }

    model.totalWidth = x + paddingRight;
    indexRuleIntervals(model);
    return model;
}

function rebuildTraceLayoutModel() {
    function rebuild() {
        virtualRuntime().traceLayoutModel = buildTraceLayoutModel(currentRuleWidthPx());
        clearTraceLayoutDirtyRegions();
        virtualRuntime().traceLayoutGeneration = (virtualRuntime().traceLayoutGeneration || 0) + 1;
        bumpRuntimeEpoch('layout');
        return virtualRuntime().traceLayoutModel;
    }

    if (virtualRuntime().traceVirtualLayoutDirty && !virtualRuntime().suppressTraceMeasuredWidth) {
        return withTraceMeasuredWidthSuppressed(rebuild);
    }
    return rebuild();
}

function getTraceLayoutModel() {
    return virtualRuntime().traceLayoutModel || rebuildTraceLayoutModel();
}

function ruleLayoutInterval(si, gi, ri) {
    var model = getTraceLayoutModel();
    return model.rules[ruleKey(si, gi, ri)] || null;
}

function itemIntersectsRange(item, range) {
    return TraceLayout.itemIntersectsRange(item, range);
}

function sameRuleRefValues(ref, si, gi, ri) {
    return RuleRefs.same(ref, si, gi, ri);
}

function shouldForceMountRule(si, gi, ri) {
    if (isDiffActive() &&
        (sameRuleRefValues(diffRuntime().diffA, si, gi, ri) || sameRuleRefValues(diffRuntime().diffB, si, gi, ri))) {
        return true;
    }
    return false;
}

function shouldMountRuleItem(item) {
    if (shouldForceMountRule(item.si, item.gi, item.ri)) return true;
    return itemIntersectsRange(item, virtualRuntime().virtualRange);
}
