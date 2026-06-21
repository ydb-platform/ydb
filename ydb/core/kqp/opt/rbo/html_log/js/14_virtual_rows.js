function virtualWidthAttr(width) {
    return '';
}

function virtualTitleFallbackWidthAttr(width) {
    width = Math.max(0, Math.round((Number(width) || 0) * 100) / 100);
    return width > 0 ? ' data-title-fallback-width="' + width + '"' : '';
}

function virtualDebugWidthAttr(width) {
    return '';
}

function virtualStyle(width, marginRight) {
    var style = 'flex:0 0 ' + width + 'px;width:' + width +
                'px;min-width:' + width + 'px;max-width:' + width + 'px;';
    if (marginRight) style += 'margin-right:' + marginRight + 'px;';
    return style;
}

function virtualStyleAttr(width, marginRight) {
    return ' style="' + virtualStyle(width, marginRight) + '"';
}

function virtualSurfaceWidthStyle(width) {
    return 'width:' + width + 'px;min-width:' + width +
           'px;max-width:' + width + 'px;';
}

function virtualSurfaceWidthStyleAttr(width) {
    return ' style="' + virtualSurfaceWidthStyle(width) + '"';
}

function spacerHtml(width) {
    if (width <= 0) return '';
    return '<div class="rule-virtual-spacer" aria-hidden="true"' +
           virtualWidthAttr(width) +
           ' style="' + virtualStyle(width, 0) + '"></div>';
}

function scopedRuleDomId(prefix, ref, options) {
    var id = ref.si + '-' + ref.gi + '-' + ref.ri;
    return prefix + '-' + (options && options.idScope ? options.idScope + '-' : '') + id;
}

function scopedRuleKey(ref, options) {
    return (options && options.idScope ? options.idScope + '-' : '') +
        ref.si + '-' + ref.gi + '-' + ref.ri;
}

function ruleFeatureButtonHtml(ref, feature, idPrefix, title, iconName, options) {
    var id = ref.si + '-' + ref.gi + '-' + ref.ri;
    var available = ruleHasFeature(ref.si, ref.gi, ref.ri, feature);
    var active = available && ruleFeatureButtonActive(feature, ref.si, ref.gi, ref.ri);
    return '<button class="tbtn icon' + (active ? ' active' : '') +
           '" id="' + scopedRuleDomId(idPrefix, ref, options) + '" ' +
           (available ? '' : 'disabled ') +
           traceActionAttr('toggle-rule-' + feature, ref.si, ref.gi, ref.ri) +
           ' title="' + title + '" aria-label="' + title + '">' +
           traceIconSvg(iconName) + '</button>';
}

function navButtonHtml(direction, ref, disabled, blockOtherDiffRule) {
    var label = direction === 'prev' ? '&#8592;' : '&#8594;';
    var ariaLabel = direction === 'prev' ? 'Previous rule' : 'Next rule';
    return '<button class="tbtn nav"' +
           traceActionAttr('navigate-rule', ref.si, ref.gi, ref.ri) +
           ' data-direction="' + direction + '"' +
           (blockOtherDiffRule ? ' data-block-other-diff-rule="true"' : '') +
           ' aria-label="' + ariaLabel + '"' +
           (disabled ? ' disabled' : '') + '>' + label + '</button>';
}

function diffSpinnerHtml() {
    return '<span class="diff-spinner" aria-hidden="true"></span>';
}

function diffButtonPendingForRef(ref, side) {
    if (!diffRuntime().pendingDiffJob || !diffRuntime().diffA || !diffRuntime().diffB) return false;
    if (diffRuntime().pendingDiffJob.pairKey !== activeDiffPairKey()) return false;
    return side === 'a'
        ? sameRuleRefValues(diffRuntime().diffA, ref.si, ref.gi, ref.ri)
        : sameRuleRefValues(diffRuntime().diffB, ref.si, ref.gi, ref.ri);
}

function diffButtonContentHtml(ref, side) {
    return diffButtonPendingForRef(ref, side) ? diffSpinnerHtml() : side.toUpperCase();
}

function diffButtonPendingClass(ref, side) {
    return diffButtonPendingForRef(ref, side) ? ' diff-pending' : '';
}

function updateDiffSelectionButtonContent() {
    if (typeof document === 'undefined') return;
    var pairs = [
        { ref: diffRuntime().diffA, side: 'a', prefix: 'abtn-' },
        { ref: diffRuntime().diffB, side: 'b', prefix: 'bbtn-' }
    ];
    for (var i = 0; i < pairs.length; i++) {
        var item = pairs[i];
        if (!item.ref) continue;
        var btn = document.getElementById(item.prefix + refArgsDashed(item.ref));
        if (!btn) continue;
        var pending = diffButtonPendingForRef(item.ref, item.side);
        btn.innerHTML = pending ? diffSpinnerHtml() : item.side.toUpperCase();
        if (btn.classList && btn.classList.toggle) {
            btn.classList.toggle('diff-pending', pending);
        } else if (btn.classList) {
            if (pending && btn.classList.add) btn.classList.add('diff-pending');
            if (!pending && btn.classList.remove) btn.classList.remove('diff-pending');
        }
    }
}

function refArgsDashed(ref) {
    return RuleRefs.key(ref);
}

function traceActionAttr(action, si, gi, ri) {
    return TraceActionEvents.actionAttr(action, si, gi, ri);
}

function treeActionAttr(action, nodeId) {
    return ' data-trace-action="' + action + '" data-node-id="' + nodeId + '"';
}

function fullscreenCloseButtonHtml(ref) {
    return '<button class="tbtn fullscreen tree-fullscreen tree-fullscreen-close" ' +
           traceActionAttr('close-rule-fullscreen-to-rule', ref.si, ref.gi, ref.ri) +
           ' title="Exit fullscreen" aria-label="Exit fullscreen">' +
           traceIconSvg('exit-fullscreen') + '</button>';
}

function ruleTitleBarHtml(ref, options) {
    var flat = findFlatIndex(ref.si, ref.gi, ref.ri);
    var diffable = ruleSupportsDiff(ref);
    var html = '<div class="rule-title-bar"' +
               traceActionAttr('toggle-rule', ref.si, ref.gi, ref.ri) + '>';
    html += '<div class="title-left">';
    html += '<div class="rule-title">' + ruleTitleHtml(ref) + '</div>';

    if (diffable) {
        html += ruleFeatureButtonHtml(ref, 'fields', 'fieldbtn', 'Toggle fields', 'info', options);
        html += ruleFeatureButtonHtml(ref, 'pinned', 'pinbtn', 'Toggle pinned columns', 'drawing-pin', options);
        html += ruleFeatureButtonHtml(ref, 'info', 'infobtn', 'Toggle info', 'file-text', options);
    }

    html += '</div><div class="title-right">';
    if (diffable) {
        html += '<button class="tbtn ab' +
                (sameRuleRefValues(diffRuntime().diffA, ref.si, ref.gi, ref.ri) ? ' selected-a' : '') +
                diffButtonPendingClass(ref, 'a') +
                '" id="' + scopedRuleDomId('abtn', ref, options) + '"' +
                traceActionAttr('set-diff-a', ref.si, ref.gi, ref.ri) + '>' +
                diffButtonContentHtml(ref, 'a') + '</button>';
        html += '<button class="tbtn ab' +
                (sameRuleRefValues(diffRuntime().diffB, ref.si, ref.gi, ref.ri) ? ' selected-b' : '') +
                diffButtonPendingClass(ref, 'b') +
                '" id="' + scopedRuleDomId('bbtn', ref, options) + '"' +
                traceActionAttr('set-diff-b', ref.si, ref.gi, ref.ri) + '>' +
                diffButtonContentHtml(ref, 'b') + '</button>';
    }
    html += navButtonHtml('prev', ref, flat <= 0);
    html += navButtonHtml('next', ref, flat >= currentAllRules().length - 1);
    html += '</div></div>';
    return html;
}

function ruleInfoPanelShellHtml(ref, options) {
    if (!ruleHasInfo(ref)) return '';
    var key = scopedRuleKey(ref, options);
    var stateKey = ref.si + '-' + ref.gi + '-' + ref.ri;
    var visible = shouldShowRuleInfoPanelShell(ref.si, ref.gi, ref.ri);
    // Drag-resize stores heights under the panel's scoped key; a fullscreen
    // panel falls back to the inline height until resized in fullscreen.
    var savedH = resizeRuntime().infoPanelHeights[key] ||
        resizeRuntime().infoPanelHeights[stateKey];
    var panelStyle = '';
    if (visible && savedH) {
        panelStyle = ' style="height:' + savedH + 'px;max-height:none"';
    } else if (visible) {
        panelStyle = ' style="max-height:min(40vh, 520px)"';
    }
    return '<div class="info-panel-resizer' + (visible ? '' : ' hidden') +
           '" aria-hidden="true"></div>' +
           '<div class="rule-info-panel' + (visible ? ' visible' : '') +
           '" id="info-' + key + '"' + panelStyle + '>' +
           '<div class="rule-info-content" id="info-content-' + key + '"></div></div>';
}

function formatCollapsedCount(value) {
    value = Math.max(0, Math.floor(Number(value) || 0));
    if (value < 1000) return String(value);

    var units = [
        { suffix: 'm', value: 1000000 },
        { suffix: 'k', value: 1000 }
    ];
    for (var i = 0; i < units.length; i++) {
        var unit = units[i];
        if (value < unit.value) continue;

        var compact = Math.floor(value / unit.value);
        if (compact < 100) return compact + unit.suffix;
        return '99' + unit.suffix + '+';
    }

    return String(value);
}

function collapsedRuleNumberLabel(ref) {
    return formatCollapsedCount(rawRuleIndex(ref.si, ref.gi, ref.ri) + 1);
}

function collapsedGroupCountLabel(si, gi) {
    return '&times;' + formatCollapsedCount(groupRuleCount(si, gi));
}

function ruleCellBodyHtml(ref, options) {
    var name = ruleDisplayName(ref);
    var num = rawRuleIndex(ref.si, ref.gi, ref.ri) + 1;
    var key = scopedRuleKey(ref, options);
    var isText = ruleIsTextTile(ref);
    return '<div class="rule-col-wrap">' +
           '<div class="rule-col-num" title="Rule ' + num + '">' +
           collapsedRuleNumberLabel(ref) + '</div>' +
           '<div class="rule-col-text" data-search-kind="rule" data-search-label="' +
           htmlEscape(name) + '" data-search-si="' + ref.si + '" data-search-gi="' +
           ref.gi + '" data-search-ri="' + ref.ri + '">' + htmlEscape(name) + '</div>' +
           '</div>' +
           '<div class="rule-exp-wrap">' +
           ruleTitleBarHtml(ref, options) +
           '<div class="rule-content">' +
           '<div class="rule-tree-pane">' +
           '<button class="tbtn fullscreen tree-fullscreen" id="fsbtn-' + key +
           '"' + traceActionAttr('toggle-rule-fullscreen', ref.si, ref.gi, ref.ri) +
           ' title="Enter fullscreen" aria-label="Enter fullscreen">' +
           traceIconSvg('enter-fullscreen') + '</button>' +
           fullscreenCloseButtonHtml(ref) +
           '<div class="rule-tree-wrap' + (isText ? ' rule-text-wrap' : '') +
           '" id="tree-' + key + '"></div>' +
           '</div>' +
           ruleInfoPanelShellHtml(ref, options) +
           '</div>' +
           '</div>' +
           '<div class="rule-resize-handle" title="Drag to resize rule tiles; double-click to reset"></div>';
}

function renderRuleCellHtml(item) {
    var ref = ruleRef(item.si, item.gi, item.ri);
    var state = effectiveRuleState(item.si, item.gi, item.ri);
    var marginRight = item.gapAfter || 0;
    var titleReadable = item.widthReason === 'title-readable-rule' ||
        ruleNeedsReadableTitle(item.si, item.gi, item.ri);
    var classes = 'rule-cell ' + (state.open ? 'expanded' : 'collapsed') +
        (titleReadable ? ' title-readable' : '') +
        collapsedSearchIndicatorClass('rules', item.key) +
        (ruleIsTextTile(ref) ? ' text-rule' : '') +
        (diffSideForRule(item.si, item.gi, item.ri) ? ' diff-rule' : '');
    return '<div class="' + classes + '" id="rule-' + item.key +
               '"' + traceActionAttr('collapsed-rule-click', ref.si, ref.gi, ref.ri) +
               virtualDebugWidthAttr(item.width + marginRight) +
               virtualStyleAttr(item.width, marginRight) + '>' +
               ruleCellBodyHtml(ref) +
               '</div>';
}

function groupCollapsedHtml(si, gi) {
    var name = traceGroupName(si, gi);
    var count = groupRuleCount(si, gi);
    return '<div class="group-col-wrap">' +
           '<div class="group-col-count" title="' + count + ' rules">' +
           collapsedGroupCountLabel(si, gi) + '</div>' +
           '<div class="group-col-text" data-search-kind="group" data-search-label="' +
           htmlEscape(name) + '" data-search-si="' + si + '" data-search-gi="' +
           gi + '">' + htmlEscape(name) + '</div>' +
           '</div>';
}

function groupTitleBarHtml(si, gi) {
    var name = traceGroupName(si, gi);
    var count = groupRuleCount(si, gi);
    var first = findFlatIndex(si, gi, 0);
    var last = findFlatIndex(si, gi, count - 1);

    return '<div class="group-title-bar">' +
           '<button class="group-toggle-btn"' +
           traceActionAttr('toggle-group-rules', si, gi) +
           ' aria-label="Toggle group rules">' + traceIconSvg('stack') + '</button>' +
           '<div class="group-title"' + traceActionAttr('toggle-group', si, gi) + '>' +
           '<span class="group-title-name" data-search-kind="group" data-search-label="' +
           htmlEscape(name) + '" data-search-si="' + si + '" data-search-gi="' +
           gi + '">' + htmlEscape(name) + '</span> (&times;' + count + ')</div>' +
           '<div class="group-nav">' +
           '<button class="group-nav-btn"' + traceActionAttr('group-navigate', si, gi) +
           ' data-direction="prev" aria-label="Previous group"' + (first <= 0 ? ' disabled' : '') +
           '>&#8592;</button>' +
           '<button class="group-nav-btn"' + traceActionAttr('group-navigate', si, gi) +
           ' data-direction="next" aria-label="Next group"' +
           (last >= currentAllRules().length - 1 ? ' disabled' : '') +
           '>&#8594;</button>' +
           '</div></div>';
}

function renderGroupCellHtml(item) {
    var open = effectiveGroupOpen(item.si, item.gi);
    var marginRight = item.gapAfter || 0;
    var rowHtml = open && item.row ? renderVirtualRowHtml(item.row) : '';
    var style = virtualStyle(item.width, marginRight) + '--group-width:' + item.width + 'px;';

    return '<div class="group-cell ' + (open ? 'g-expanded' : 'g-collapsed') +
           collapsedSearchIndicatorClass('groups', item.key) +
           '" id="group-' + item.key + '"' +
           traceActionAttr('collapsed-group-click', item.si, item.gi) +
           virtualTitleFallbackWidthAttr(item.titleFallbackWidth) +
           virtualDebugWidthAttr(item.width + marginRight) +
           ' style="' + style + '">' +
           groupCollapsedHtml(item.si, item.gi) +
           '<div class="group-exp-wrap">' +
           groupTitleBarHtml(item.si, item.gi) +
           '<div class="group-rules-area virtual-row" data-virtual-row="group-' +
           item.key + '"' + virtualSurfaceWidthStyleAttr(item.width) + '>' + rowHtml + '</div>' +
           '</div></div>';
}

function virtualRowSurfaceWidth(row) {
    if (!row) return 0;
    return Math.max(row.width || 0, row.surfaceWidth || 0);
}

function renderVirtualRowHtml(row) {
    var html = '';
    var spacerWidth = 0;

    function flushSpacer() {
        if (spacerWidth > 0) {
            html += spacerHtml(spacerWidth);
            spacerWidth = 0;
        }
    }

    for (var i = 0; i < row.items.length; i++) {
        var item = row.items[i];

        if (item.kind === 'spacer') {
            spacerWidth += item.outerWidth;
            continue;
        }

        if (item.kind === 'rule' && !shouldMountRuleItem(item)) {
            spacerWidth += item.outerWidth;
            continue;
        }

        flushSpacer();
        if (item.kind === 'rule') {
            html += renderRuleCellHtml(item);
        } else if (item.kind === 'group') {
            html += renderGroupCellHtml(item);
        }
    }

    spacerWidth += Math.max(0, virtualRowSurfaceWidth(row) - row.width);
    flushSpacer();
    return html;
}

function ruleRunSignature(item) {
    var ref = ruleRef(item.si, item.gi, item.ri);
    return [
        'R',
        item.key,
        traceRuleType(item.si, rawRuleIndex(item.si, item.gi, item.ri)),
        effectiveRuleOpen(item.si, item.gi, item.ri) ? 'o' : 'c',
        ruleHasInfo(ref) ? 'info-shell' : '',
        searchRuntime().collapsedSearchIndicators &&
            searchRuntime().collapsedSearchIndicators.rules &&
            searchRuntime().collapsedSearchIndicators.rules[item.key] ? 'h' : '',
        diffSideForRule(item.si, item.gi, item.ri) || '',
        sameRuleRefValues(diffRuntime().diffA, item.si, item.gi, item.ri) ? 'a' : '',
        sameRuleRefValues(diffRuntime().diffB, item.si, item.gi, item.ri) ? 'b' : ''
    ].join(':');
}

function groupRunSignature(item) {
    return [
        'G',
        item.key,
        effectiveGroupOpen(item.si, item.gi) ? 'o' : 'c',
        searchRuntime().collapsedSearchIndicators &&
            searchRuntime().collapsedSearchIndicators.groups &&
            searchRuntime().collapsedSearchIndicators.groups[item.key] ? 'h' : ''
    ].join(':');
}

function virtualRowRuns(row) {
    var runs = [];
    var spacerWidth = 0;

    function flushSpacer() {
        if (spacerWidth > 0) {
            runs.push({
                kind: 'spacer',
                width: spacerWidth,
                signature: 'S'
            });
            spacerWidth = 0;
        }
    }

    for (var i = 0; i < row.items.length; i++) {
        var item = row.items[i];

        if (item.kind === 'spacer') {
            spacerWidth += item.outerWidth;
            continue;
        }

        if (item.kind === 'rule' && !shouldMountRuleItem(item)) {
            spacerWidth += item.outerWidth;
            continue;
        }

        flushSpacer();
        if (item.kind === 'rule') {
            runs.push({
                kind: 'rule',
                item: item,
                width: item.width,
                marginRight: item.gapAfter || 0,
                signature: ruleRunSignature(item)
            });
        } else if (item.kind === 'group') {
            runs.push({
                kind: 'group',
                item: item,
                width: item.width,
                marginRight: item.gapAfter || 0,
                signature: groupRunSignature(item)
            });
        }
    }

    spacerWidth += Math.max(0, virtualRowSurfaceWidth(row) - row.width);
    flushSpacer();
    return runs;
}

var DETACHED_RULE_ELEMENT_CACHE_LIMIT = 16;

function copyRenderCacheRecord(record) {
    if (!record || typeof record !== 'object') return null;
    var copy = {};
    for (var key in record) {
        if (!Object.prototype.hasOwnProperty.call(record, key)) continue;
        var value = record[key];
        if (value === null || typeof value !== 'object') copy[key] = value;
    }
    return copy;
}

function renderCacheRecordsEqual(a, b) {
    if (!a || !b) return a === b;
    var key;
    for (key in a) {
        if (!Object.prototype.hasOwnProperty.call(a, key)) continue;
        if (a[key] !== b[key]) return false;
    }
    for (key in b) {
        if (!Object.prototype.hasOwnProperty.call(b, key)) continue;
        if (!Object.prototype.hasOwnProperty.call(a, key)) return false;
    }
    return true;
}

function ruleElementTreeWrap(el, key) {
    if (!el || !el.querySelector) return null;
    return el.querySelector('#tree-' + key) || el.querySelector('.rule-tree-wrap');
}

function rulePaneScrollSessionKey(ref) {
    if (!ref) return '';
    var traceIndex = 0;
    try {
        traceIndex = Math.max(0, Number(traceRuntime().activeTraceIndex) || 0);
    } catch (err) {}
    return traceIndex + ':' + ruleKey(ref.si, ref.gi, ref.ri);
}

function rulePaneScrollSession(ref) {
    var trace = null;
    try { trace = traceRuntime(); } catch (err) {}
    if (!trace || !ref) return null;
    if (!trace.rulePaneScrollSessions) trace.rulePaneScrollSessions = {};
    var key = rulePaneScrollSessionKey(ref);
    if (!key) return null;
    if (!trace.rulePaneScrollSessions[key]) trace.rulePaneScrollSessions[key] = {};
    return trace.rulePaneScrollSessions[key];
}

function captureScrollablePane(session, name, el) {
    if (!session || !name || !el) return false;
    var left = Math.max(0, Number(el.scrollLeft) || 0);
    var top = Math.max(0, Number(el.scrollTop) || 0);
    if (left <= 0 && top <= 0) {
        if (el.isConnected === true) delete session[name];
        return false;
    }
    session[name] = { left: left, top: top };
    return true;
}

function effectiveInfoScrollPane(panel) {
    if (!panel) return null;
    if (panel.classList && panel.classList.contains &&
            panel.classList.contains('info-panel-tabbed') &&
            panel.querySelector) {
        return panel.querySelector('.info-tab-panel') || panel;
    }
    return panel;
}

function treePaneScrollOwnedByMaterializer(tree) {
    return !!(typeof treeMaterializerOwnsPaneScroll === 'function' &&
        treeMaterializerOwnsPaneScroll(tree));
}

function restoreMaterializedTreePaneAfterReattach(tree) {
    return !!(typeof restoreTreeMaterializerPaneScrollAfterReattach === 'function' &&
        restoreTreeMaterializerPaneScrollAfterReattach(tree));
}

function captureRulePaneScrollFromElement(el, ref) {
    if (!el || !ref || !el.querySelector) return false;
    if (el.classList && el.classList.contains('collapsed') &&
            !el.classList.contains('expanded')) {
        return false;
    }
    var session = rulePaneScrollSession(ref);
    if (!session) return false;

    var key = ruleKey(ref.si, ref.gi, ref.ri);
    var captured = false;
    var tree = ruleElementTreeWrap(el, key);
    if (ruleTreeWrapHasReusableContent(tree)) {
        if (typeof captureTreeMaterializerPaneScroll === 'function' &&
                captureTreeMaterializerPaneScroll(tree)) {
            captured = true;
        } else if (!treePaneScrollOwnedByMaterializer(tree)) {
            captured = captureScrollablePane(session, 'tree', tree) || captured;
        }
    }
    captured = captureScrollablePane(
        session,
        'info',
        effectiveInfoScrollPane(el.querySelector('#info-' + key + '.rule-info-panel'))
    ) || captured;
    return captured;
}

function captureMountedRulePaneScroll(si, gi, ri) {
    if (!hasDOM() || !document.getElementById || !validRuleRef(si, gi, ri)) return false;
    var ref = ruleRef(si, gi, ri);
    return captureRulePaneScrollFromElement(
        document.getElementById('rule-' + ruleKey(si, gi, ri)),
        ref
    );
}

function restoreScrollablePane(session, name, el) {
    if (!session || !session[name] || !el) return false;
    var saved = session[name];
    var maxLeft = Math.max(0, (Number(el.scrollWidth) || 0) - (Number(el.clientWidth) || 0));
    var maxTop = Math.max(0, (Number(el.scrollHeight) || 0) - (Number(el.clientHeight) || 0));
    el.scrollLeft = Math.max(0, Math.min(maxLeft, Number(saved.left) || 0));
    el.scrollTop = Math.max(0, Math.min(maxTop, Number(saved.top) || 0));
    return true;
}

function restoreRuleTreePaneScroll(ref, container) {
    if (!ref || !container) return false;
    if (treePaneScrollOwnedByMaterializer(container)) {
        return restoreMaterializedTreePaneAfterReattach(container);
    }
    var session = rulePaneScrollSession(ref);
    return restoreScrollablePane(session, 'tree', container);
}

function restoreRuleInfoPaneScroll(si, gi, ri) {
    if (!hasDOM() || !document.getElementById || !validRuleRef(si, gi, ri)) return false;
    var ref = ruleRef(si, gi, ri);
    var session = rulePaneScrollSession(ref);
    return restoreScrollablePane(
        session,
        'info',
        effectiveInfoScrollPane(document.getElementById('info-' + ruleKey(si, gi, ri)))
    );
}

function treeWrapHasPendingOnly(tree) {
    return !!(tree && tree.querySelector && (
        tree.querySelector('.tree-loading[data-render-state="queued"]') ||
        tree.querySelector('.tree-loading[data-payload-state="pending"]')
    ));
}

function treeWrapHasDomContent(tree) {
    if (!tree) return false;
    if ('firstChild' in tree) return !!tree.firstChild;
    if (tree.childNodes && typeof tree.childNodes.length === 'number') {
        return tree.childNodes.length > 0;
    }
    if (typeof tree.childElementCount === 'number') {
        return tree.childElementCount > 0;
    }
    if (tree.children && typeof tree.children.length === 'number') {
        return tree.children.length > 0;
    }
    return false;
}

function ruleTreeWrapHasReusableContent(tree) {
    return !!(treeWrapHasDomContent(tree) && !treeWrapHasPendingOnly(tree));
}

function ruleElementHasReusableTree(el, key) {
    var tree = ruleElementTreeWrap(el, key);
    return ruleTreeWrapHasReusableContent(tree);
}

function ruleElementHasEmptyTree(el, key) {
    var tree = ruleElementTreeWrap(el, key);
    return !treeWrapHasDomContent(tree) || treeWrapHasPendingOnly(tree);
}

function detachedRuleElementCacheKey(ref) {
    return ref ? ruleKey(ref.si, ref.gi, ref.ri) : '';
}

function clearRuleElementFlashState(el) {
    if (!el || !el.classList) return false;
    if (el.classList.contains && !el.classList.contains('flash-cell')) return false;
    if (typeof clearRuleTitleFlashElement === 'function') {
        return clearRuleTitleFlashElement(el);
    }
    el.classList.remove('flash-cell');
    return true;
}

function clearTransientRuleElementState(el) {
    if (!el) return false;
    var cleared = false;
    if (el.classList) {
        cleared = clearRuleElementFlashState(el) || cleared;
    }
    if (!el.querySelectorAll) return cleared;

    var flashEls = el.querySelectorAll('.flash-cell');
    for (var i = 0; i < flashEls.length; i++) {
        cleared = clearRuleElementFlashState(flashEls[i]) || cleared;
    }
    return cleared;
}

function removeDetachedRuleElementCacheKey(key) {
    var order = virtualRuntime().detachedRuleElementCacheOrder || [];
    for (var i = order.length - 1; i >= 0; i--) {
        if (order[i] === key) order.splice(i, 1);
    }
    virtualRuntime().detachedRuleElementCacheOrder = order;
}

function discardDetachedRuleElementCacheEntry(entry, key) {
    if (!entry || !entry.element) return;
    var ref = parseRuleKeyId('rule-' + (key || entry.key || ''), 'rule-');
    if (ref) captureRulePaneScrollFromElement(entry.element, ref);
    var tree = ruleElementTreeWrap(entry.element, key || entry.key || '');
    if (tree && typeof clearVirtualTreeContainerState === 'function') {
        clearVirtualTreeContainerState(tree);
    }
    entry.element = null;
}

function discardDetachedRuleElementForKey(key) {
    var cache = virtualRuntime().detachedRuleElementCache || {};
    var entry = cache[key];
    if (entry) discardDetachedRuleElementCacheEntry(entry, key);
    delete cache[key];
    virtualRuntime().detachedRuleElementCache = cache;
    removeDetachedRuleElementCacheKey(key);
}

function discardDetachedRuleElementForRule(si, gi, ri) {
    discardDetachedRuleElementForKey(ruleKey(si, gi, ri));
}

function clearDetachedRuleElementCache() {
    var cache = virtualRuntime().detachedRuleElementCache || {};
    for (var key in cache) {
        if (!Object.prototype.hasOwnProperty.call(cache, key)) continue;
        discardDetachedRuleElementCacheEntry(cache[key], key);
    }
    virtualRuntime().detachedRuleElementCache = {};
    virtualRuntime().detachedRuleElementCacheOrder = [];
}

function touchDetachedRuleElementCacheKey(key) {
    removeDetachedRuleElementCacheKey(key);
    virtualRuntime().detachedRuleElementCacheOrder.push(key);
}

function enforceDetachedRuleElementCacheLimit() {
    var order = virtualRuntime().detachedRuleElementCacheOrder || [];
    while (order.length > DETACHED_RULE_ELEMENT_CACHE_LIMIT) {
        discardDetachedRuleElementForKey(order[0]);
        order = virtualRuntime().detachedRuleElementCacheOrder || [];
    }
}

function detachedRuleElementEntryCurrent(entry, run) {
    if (!entry || !entry.element || !run || run.kind !== 'rule') return false;
    if (entry.element.isConnected === true) return false;
    if (entry.signature !== run.signature) return false;

    var epochs = currentRuntimeEpoch();
    if (entry.traceEpoch !== epochs.trace ||
            entry.renderEpoch !== epochs.render ||
            entry.searchEpoch !== epochs.search ||
            entry.diffEpoch !== epochs.diff) {
        return false;
    }

    var item = run.item;
    var rule = findRuleState(item.si, item.gi, item.ri);
    if (!rule || rule.renderQueued) return false;
    if (!renderCacheRecordsEqual(rule.rendered, entry.rendered)) return false;
    if (!renderCacheRecordsEqual(rule.infoRendered, entry.infoRendered)) return false;
    if (!ruleElementHasReusableTree(entry.element, item.key)) return false;
    return virtualElementDomKey(entry.element) === 'rule-' + item.key &&
        canReuseVirtualElementForRun(entry.element, run);
}

function detachedRuleElementSearchMarksCurrent(entry, query, scope) {
    if (!entry || !entry.element || !entry.element.querySelectorAll) return true;

    var expected = String(query || '').toLowerCase();
    var marks = entry.element.querySelectorAll('mark[data-search-field]');
    for (var i = 0; i < marks.length; i++) {
        var mark = marks[i];
        var field = mark && mark.getAttribute ? mark.getAttribute('data-search-field') || '' : '';
        var text = String(mark && mark.textContent || '').toLowerCase();
        if (!expected || text !== expected || !treeHighlightAllowed(field, scope)) return false;
    }
    return true;
}

function detachedRuleElementSearchRenderCurrent(entry, query, scope) {
    query = query || '';
    var renderScope = query ? (scope || currentSearchScope()) : '';
    var rendered = entry && entry.rendered;
    if (!rendered) return false;
    if ((rendered.query || '') !== query) return false;
    if ((rendered.scope || '') !== renderScope) return false;
    return detachedRuleElementSearchMarksCurrent(entry, query, renderScope);
}

function discardDetachedRuleElementsWithStaleSearchRender(query, scope) {
    var cache = virtualRuntime().detachedRuleElementCache || {};
    for (var key in cache) {
        if (!Object.prototype.hasOwnProperty.call(cache, key)) continue;
        if (!detachedRuleElementSearchRenderCurrent(cache[key], query, scope)) {
            discardDetachedRuleElementForKey(key);
        }
    }
}

function cacheDetachedRuleElementForReuse(el) {
    if (!el) return false;
    var ref = parseRuleKeyId(virtualElementDomKey(el), 'rule-');
    if (!ref || !validRuleRef(ref.si, ref.gi, ref.ri)) return false;
    captureRulePaneScrollFromElement(el, ref);

    var key = detachedRuleElementCacheKey(ref);
    var rule = findRuleState(ref.si, ref.gi, ref.ri);
    if (!rule || !rule.rendered || rule.renderQueued) return false;
    if (!ruleElementHasReusableTree(el, key)) return false;
    clearTransientRuleElementState(el);

    var epochs = currentRuntimeEpoch();
    discardDetachedRuleElementForKey(key);
    virtualRuntime().detachedRuleElementCache[key] = {
        key: key,
        element: el,
        signature: el.__virtualRunSignature || ruleRunSignature({
            key: key,
            si: ref.si,
            gi: ref.gi,
            ri: ref.ri
        }),
        rendered: copyRenderCacheRecord(rule.rendered),
        infoRendered: copyRenderCacheRecord(rule.infoRendered),
        traceEpoch: epochs.trace,
        renderEpoch: epochs.render,
        searchEpoch: epochs.search,
        diffEpoch: epochs.diff
    };
    touchDetachedRuleElementCacheKey(key);
    enforceDetachedRuleElementCacheLimit();
    return true;
}

function takeDetachedRuleElementForRun(run) {
    if (!run || run.kind !== 'rule') return null;
    var key = run.item.key;
    var cache = virtualRuntime().detachedRuleElementCache || {};
    var entry = cache[key];
    if (!entry) return null;

    if (!detachedRuleElementEntryCurrent(entry, run)) {
        discardDetachedRuleElementForKey(key);
        return null;
    }

    delete cache[key];
    virtualRuntime().detachedRuleElementCache = cache;
    removeDetachedRuleElementCacheKey(key);
    clearTransientRuleElementState(entry.element);
    return entry.element;
}

function cacheMountedRuleElementForKey(key) {
    if (!hasDOM() || !document.getElementById) return false;
    return cacheDetachedRuleElementForReuse(document.getElementById('rule-' + key));
}

function cacheMountedRuleElementsForStageShellReplacement() {
    var mounted = virtualRuntime().mountedRuleKeys || {};
    for (var key in mounted) {
        if (!Object.prototype.hasOwnProperty.call(mounted, key)) continue;
        cacheMountedRuleElementForKey(key);
    }
}

function cacheVirtualRowRuleElements(rowEl) {
    if (!rowEl || !rowEl.children) return;
    for (var i = 0; i < rowEl.children.length; i++) {
        var child = rowEl.children[i];
        if (virtualElementDomKey(child).indexOf('rule-') === 0) {
            cacheDetachedRuleElementForReuse(child);
        } else if (child && child.querySelectorAll) {
            var nested = child.querySelectorAll('.rule-cell[id^="rule-"]');
            for (var j = 0; j < nested.length; j++) {
                cacheDetachedRuleElementForReuse(nested[j]);
            }
        }
    }
}

function virtualProjectionResult() {
    return {
        ok: true,
        errors: [],
        warnings: [],
        stats: {
            rows: 0,
            runs: 0,
            mountedRules: 0,
            mountedGroups: 0,
            spacers: 0
        }
    };
}

function pushVirtualProjectionIssue(result, severity, code, path, message, details) {
    var issue = {
        code: code,
        path: path,
        message: message
    };
    if (details !== undefined) issue.details = details;
    result[severity].push(issue);
    if (severity === 'errors') result.ok = false;
}

function pushVirtualProjectionError(result, code, path, message, details) {
    pushVirtualProjectionIssue(result, 'errors', code, path, message, details);
}

function finiteVirtualNumber(value) {
    return typeof value === 'number' && Number.isFinite(value);
}

function nearVirtualNumber(actual, expected, tolerance) {
    return finiteVirtualNumber(actual) &&
           finiteVirtualNumber(expected) &&
           Math.abs(actual - expected) <= tolerance;
}

function validateVirtualRunNumber(result, value, path, options) {
    if (!finiteVirtualNumber(value)) {
        pushVirtualProjectionError(result, 'virtual_non_finite_number', path, 'Expected a finite number', value);
        return false;
    }
    if (value < -options.tolerance) {
        pushVirtualProjectionError(result, 'virtual_negative_number', path, 'Expected a non-negative number', value);
        return false;
    }
    return true;
}

function expectedVirtualRowRunDescriptors(row) {
    var descriptors = [];
    var spacerWidth = 0;

    function flushSpacer() {
        if (spacerWidth > 0) {
            descriptors.push({
                kind: 'spacer',
                key: 'spacer',
                width: spacerWidth,
                marginRight: 0,
                outerWidth: spacerWidth,
                signature: 'S'
            });
            spacerWidth = 0;
        }
    }

    if (!row || !Array.isArray(row.items)) return descriptors;

    for (var i = 0; i < row.items.length; i++) {
        var item = row.items[i];
        if (!item || typeof item !== 'object') continue;
        if (item.kind === 'spacer') {
            spacerWidth += item.outerWidth;
            continue;
        }
        if (item.kind === 'rule' && !shouldMountRuleItem(item)) {
            spacerWidth += item.outerWidth;
            continue;
        }

        flushSpacer();
        if (item.kind === 'rule') {
            descriptors.push({
                kind: 'rule',
                key: item.key,
                item: item,
                width: item.width,
                marginRight: item.gapAfter || 0,
                outerWidth: item.width + (item.gapAfter || 0),
                signature: ruleRunSignature(item)
            });
        } else if (item.kind === 'group') {
            descriptors.push({
                kind: 'group',
                key: item.key,
                item: item,
                width: item.width,
                marginRight: item.gapAfter || 0,
                outerWidth: item.width + (item.gapAfter || 0),
                signature: groupRunSignature(item)
            });
        }
    }

    spacerWidth += Math.max(0, virtualRowSurfaceWidth(row) - row.width);
    flushSpacer();
    return descriptors;
}

function virtualRunOuterWidth(run) {
    if (!run) return 0;
    if (run.kind === 'spacer') return run.width || 0;
    return (run.width || 0) + (run.marginRight || 0);
}

function validateVirtualRowRuns(row, runs, options) {
    options = options || {};
    options.tolerance = Number.isFinite(options.tolerance) ? Math.max(0, options.tolerance) : 0.01;

    var result = virtualProjectionResult();
    result.stats.rows = row ? 1 : 0;

    if (!row || typeof row !== 'object') {
        pushVirtualProjectionError(result, 'virtual_row_missing', 'row', 'Virtual row must be an object');
        return result;
    }
    if (!Array.isArray(row.items)) {
        pushVirtualProjectionError(result, 'virtual_row_items_missing', 'row.items', 'Virtual row items must be an array');
        return result;
    }
    if (!Array.isArray(runs)) {
        pushVirtualProjectionError(result, 'virtual_runs_missing', 'runs', 'Virtual runs must be an array');
        return result;
    }

    var expected = expectedVirtualRowRunDescriptors(row);
    if (runs.length !== expected.length) {
        pushVirtualProjectionError(result, 'virtual_run_count_mismatch', 'runs.length', 'Virtual run count does not match row projection', {
            actual: runs.length,
            expected: expected.length
        });
    }

    var total = 0;
    for (var i = 0; i < runs.length; i++) {
        var run = runs[i];
        var descriptor = expected[i] || null;
        var path = 'runs[' + i + ']';
        if (!run || typeof run !== 'object') {
            pushVirtualProjectionError(result, 'virtual_run_missing', path, 'Virtual run must be an object');
            continue;
        }

        result.stats.runs++;
        if (run.kind === 'spacer') result.stats.spacers++;
        if (run.kind === 'rule') result.stats.mountedRules++;
        if (run.kind === 'group') result.stats.mountedGroups++;

        if (run.kind !== 'spacer' && run.kind !== 'rule' && run.kind !== 'group') {
            pushVirtualProjectionError(result, 'virtual_run_kind_invalid', path + '.kind', 'Virtual run kind is invalid', run.kind);
        }
        validateVirtualRunNumber(result, run.width, path + '.width', options);
        if (run.kind !== 'spacer') {
            validateVirtualRunNumber(result, run.marginRight || 0, path + '.marginRight', options);
        } else if (run.marginRight !== undefined && Math.abs(run.marginRight) > options.tolerance) {
            pushVirtualProjectionError(result, 'virtual_spacer_margin', path + '.marginRight', 'Spacer runs must not have margins', run.marginRight);
        }

        if (descriptor) {
            if (run.kind !== descriptor.kind) {
                pushVirtualProjectionError(result, 'virtual_run_kind_mismatch', path + '.kind', 'Virtual run kind does not match expected projection', {
                    actual: run.kind,
                    expected: descriptor.kind
                });
            }
            if (!nearVirtualNumber(run.width, descriptor.width, options.tolerance)) {
                pushVirtualProjectionError(result, 'virtual_run_width_mismatch', path + '.width', 'Virtual run width does not match expected projection', {
                    actual: run.width,
                    expected: descriptor.width
                });
            }
            if (!nearVirtualNumber(run.marginRight || 0, descriptor.marginRight, options.tolerance)) {
                pushVirtualProjectionError(result, 'virtual_run_margin_mismatch', path + '.marginRight', 'Virtual run margin does not match expected projection', {
                    actual: run.marginRight || 0,
                    expected: descriptor.marginRight
                });
            }
            if (run.kind !== 'spacer' && (!run.item || run.item.key !== descriptor.key)) {
                pushVirtualProjectionError(result, 'virtual_run_item_mismatch', path + '.item', 'Virtual run item does not match expected row item', {
                    actual: run.item && run.item.key,
                    expected: descriptor.key
                });
            }
            if (run.signature !== descriptor.signature) {
                pushVirtualProjectionError(result, 'virtual_run_signature_mismatch', path + '.signature', 'Virtual run signature does not match expected state', {
                    actual: run.signature,
                    expected: descriptor.signature
                });
            }
        }

        total += virtualRunOuterWidth(run);
    }

    var expectedRowWidth = virtualRowSurfaceWidth(row);
    if (!nearVirtualNumber(total, expectedRowWidth, options.tolerance)) {
        pushVirtualProjectionError(result, 'virtual_row_width_mismatch', 'row.width', 'Virtual runs must cover the full logical row width', {
            actual: total,
            expected: expectedRowWidth
        });
    }

    return result;
}

function mergeVirtualProjectionValidation(target, source) {
    target.ok = target.ok && source.ok;
    for (var i = 0; i < source.errors.length; i++) target.errors.push(source.errors[i]);
    for (var j = 0; j < source.warnings.length; j++) target.warnings.push(source.warnings[j]);
    target.stats.rows += source.stats.rows;
    target.stats.runs += source.stats.runs;
    target.stats.mountedRules += source.stats.mountedRules;
    target.stats.mountedGroups += source.stats.mountedGroups;
    target.stats.spacers += source.stats.spacers;
}

function validateTraceLayoutProjection(model, options) {
    options = options || {};
    var result = virtualProjectionResult();
    if (!model || !Array.isArray(model.rows)) {
        pushVirtualProjectionError(result, 'projection_model_missing', 'model', 'Layout model with rows is required');
        return result;
    }

    for (var i = 0; i < model.rows.length; i++) {
        var row = model.rows[i];
        var rowValidation = validateVirtualRowRuns(row, virtualRowRuns(row), options);
        mergeVirtualProjectionValidation(result, rowValidation);
    }
    return result;
}

function traceLayoutProjectionSummary(validation, context) {
    var prefix = 'Trace virtual projection invariant failed';
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

function assertTraceLayoutProjection(model, context, options) {
    var validation = validateTraceLayoutProjection(model, options);
    if (!validation.ok) {
        var summary = traceLayoutProjectionSummary(validation, context);
        recordInvariantFailure('layout-projection', context, validation, summary);
        throw new Error(summary);
    }
    return validation;
}

function virtualRowSignature(runs) {
    var parts = [];
    for (var i = 0; i < runs.length; i++) {
        parts.push(runs[i].signature);
    }
    return parts.join('|');
}

function virtualRunHtml(run) {
    if (run.kind === 'spacer') return spacerHtml(run.width);
    if (run.kind === 'rule') return renderRuleCellHtml(run.item);
    if (run.kind === 'group') return renderGroupCellHtml(run.item);
    return '';
}

function virtualRunDomKey(run) {
    if (run.kind === 'rule') return 'rule-' + run.item.key;
    if (run.kind === 'group') return 'group-' + run.item.key;
    if (run.kind === 'spacer') return 'spacer';
    return '';
}

function virtualElementDomKey(el) {
    if (!el) return '';
    if (el.id && (el.id.indexOf('rule-') === 0 || el.id.indexOf('group-') === 0)) {
        return el.id;
    }
    if (el.classList && el.classList.contains('rule-virtual-spacer')) return 'spacer';
    return '';
}

function virtualRunElementHasRequiredStructure(el, run) {
    if (!el || !run) return false;
    if (run.kind === 'group' && run.item.row) {
        return !!(el.querySelector &&
            el.querySelector('[data-virtual-row="' + run.item.row.id + '"]'));
    }
    return true;
}

function virtualRunElementMatchesState(el, run) {
    if (!el || !run || !el.classList) return false;

    if (run.kind === 'rule') {
        var ruleOpen = effectiveRuleOpen(run.item.si, run.item.gi, run.item.ri);
        var isText = ruleIsTextTile(ruleRef(run.item.si, run.item.gi, run.item.ri));
        return el.classList.contains(ruleOpen ? 'expanded' : 'collapsed') &&
               !el.classList.contains(ruleOpen ? 'collapsed' : 'expanded') &&
               el.classList.contains('text-rule') === isText;
    }

    if (run.kind === 'group') {
        var groupOpen = effectiveGroupOpen(run.item.si, run.item.gi);
        return el.classList.contains(groupOpen ? 'g-expanded' : 'g-collapsed') &&
               !el.classList.contains(groupOpen ? 'g-collapsed' : 'g-expanded');
    }

    return true;
}

function virtualRunElementMatches(el, run) {
    return virtualElementDomKey(el) === virtualRunDomKey(run) &&
           canReuseVirtualElementForRun(el, run);
}

function virtualRowDomMatchesRuns(rowEl, runs) {
    if (!rowEl || rowEl.children.length !== runs.length) return false;
    for (var i = 0; i < runs.length; i++) {
        if (!virtualRunElementMatches(rowEl.children[i], runs[i])) return false;
    }
    return true;
}

function virtualRowNeedsDetachedRuleRestore(rowEl, runs) {
    if (!rowEl || !runs || rowEl.children.length < runs.length) return false;
    for (var i = 0; i < runs.length; i++) {
        var run = runs[i];
        if (!run || run.kind !== 'rule') continue;
        if (!virtualRuntime().detachedRuleElementCache[run.item.key]) continue;
        if (virtualElementDomKey(rowEl.children[i]) !== virtualRunDomKey(run)) continue;
        if (ruleElementHasEmptyTree(rowEl.children[i], run.item.key)) return true;
    }
    return false;
}

function registerVirtualRowSignatures(row) {
    if (!row) return;

    var runs = virtualRowRuns(row);
    virtualRuntime().virtualRowSignatureCache[row.id] = virtualRowSignature(runs);

    for (var i = 0; i < runs.length; i++) {
        if (runs[i].kind === 'group' && runs[i].item.row) {
            registerVirtualRowSignatures(runs[i].item.row);
        }
    }
}

function stampVirtualRowElementSignatures(row, rowEl) {
    if (!row || !rowEl) return;

    var runs = virtualRowRuns(row);
    for (var i = 0; i < runs.length && i < rowEl.children.length; i++) {
        rowEl.children[i].__virtualRunSignature = runs[i].signature;
        if (runs[i].kind === 'group' && runs[i].item.row) {
            stampVirtualRowElementSignatures(
                runs[i].item.row,
                rowEl.children[i].querySelector('[data-virtual-row="' + runs[i].item.row.id + '"]')
            );
        }
    }
}

function setVirtualBoxWidth(el, width) {
    var px = width + 'px';
    el.style.flexBasis = px;
    el.style.width = px;
    el.style.minWidth = px;
    el.style.maxWidth = px;
}

function setVirtualSurfaceWidth(el, width) {
    if (!el || !el.style) return;
    var px = width + 'px';
    el.style.width = px;
    el.style.minWidth = px;
    el.style.maxWidth = px;
}

function setVirtualMargin(el, marginRight) {
    el.style.marginRight = marginRight ? marginRight + 'px' : '';
}

function syncVirtualRunElement(el, run) {
    if (run.kind === 'spacer') {
        setVirtualBoxWidth(el, run.width);
        el.__virtualRunSignature = run.signature;
        return;
    }

    setVirtualBoxWidth(el, run.width);
    setVirtualMargin(el, run.marginRight);

    if (run.kind === 'group' && run.item.row) {
        setVirtualCssCustomProperty(el, '--group-width', run.width + 'px');
        var nested = el.querySelector('[data-virtual-row="' + run.item.row.id + '"]');
        setVirtualSurfaceWidth(nested, run.width);
        syncVirtualRowElement(run.item.row, nested);
    }
    syncVirtualRunSearchIndicatorState(el, run);
    el.__virtualRunSignature = run.signature;

    if (run.kind === 'rule' &&
            typeof ensureVisibleExpandedRuleBody === 'function' &&
            validRuleRef(run.item.si, run.item.gi, run.item.ri) &&
            isRuleExpandedInLayout(run.item.si, run.item.gi, run.item.ri)) {
        ensureVisibleExpandedRuleBody(run.item.si, run.item.gi, run.item.ri, {
            render: false,
            reason: 'virtual-sync'
        });
    }
}

function virtualRunSearchIndicatorActive(run) {
    var indicators = searchRuntime().collapsedSearchIndicators || {};
    if (run.kind === 'rule') {
        return !!(indicators.rules && indicators.rules[run.item.key]);
    }
    if (run.kind === 'group') {
        return !!(indicators.groups && indicators.groups[run.item.key]);
    }
    return false;
}

function syncVirtualRunSearchIndicatorState(el, run) {
    if (!el || !el.classList || !run) return;
    if (run.kind !== 'rule' && run.kind !== 'group') return;

    var active = virtualRunSearchIndicatorActive(run);
    if (el.classList.contains('search-hidden-match') !== active) {
        el.classList.toggle('search-hidden-match', active);
    }

    if (run.kind === 'rule') {
    }
}

function createVirtualRunElement(run) {
    var wrapper = document.createElement('div');
    wrapper.innerHTML = virtualRunHtml(run);
    return wrapper.firstElementChild;
}

function findReusableVirtualRunElement(rowEl, key, startIndex, run) {
    if (key === 'spacer') return null;

    for (var i = startIndex; i < rowEl.children.length; i++) {
        if (virtualElementDomKey(rowEl.children[i]) === key &&
            canReuseVirtualElementForRun(rowEl.children[i], run)) return rowEl.children[i];
    }
    return null;
}

function canReuseVirtualElementForRun(el, run) {
    if (!el) return false;
    var signature = el.__virtualRunSignature || '';
    return (!signature || !run || signature === run.signature) &&
           virtualRunElementMatchesState(el, run) &&
           virtualRunElementHasRequiredStructure(el, run);
}

function clearRuleRenderState(si, gi, ri) {
    var rule = findRuleState(si, gi, ri);
    if (!rule) return;

    captureMountedRulePaneScroll(si, gi, ri);
    discardDetachedRuleElementForRule(si, gi, ri);
    rule.rendered = null;
    rule.infoRendered = null;
    rule.renderQueued = false;
    clearRuleTreePending(si, gi, ri);
}

function clearVirtualRunRenderState(run) {
    if (run.kind === 'rule') {
        clearRuleRenderState(run.item.si, run.item.gi, run.item.ri);
    } else if (run.kind === 'group' && run.item.row) {
        invalidateMountedRulesInRow(run.item.row);
    }
}

function clearVirtualElementRenderState(el, options) {
    options = options || {};
    if (options.cache && cacheDetachedRuleElementForReuse(el)) return;

    var key = virtualElementDomKey(el);
    var ref;

    if (key.indexOf('rule-') === 0) {
        if (options.preserveRuleRenderState) return;
        ref = parseRuleKeyId(key, 'rule-');
        if (ref) clearRuleRenderState(ref.si, ref.gi, ref.ri);
    } else if (key.indexOf('group-') === 0) {
        ref = parseGroupKeyId(key);
        if (ref) invalidateMountedRulesInGroup(ref.si, ref.gi);
    }
}

function afterDetachedRuleElementReattach(el, run) {
    if (!el || !run || run.kind !== 'rule') return;
    var item = run.item;
    if (typeof syncReattachedRuleElementFeatureState === 'function') {
        syncReattachedRuleElementFeatureState(el, item.si, item.gi, item.ri);
    }

    var ref = ruleRef(item.si, item.gi, item.ri);
    var session = rulePaneScrollSession(ref);
    var tree = ruleElementTreeWrap(el, item.key);
    if (tree && treePaneScrollOwnedByMaterializer(tree)) {
        restoreMaterializedTreePaneAfterReattach(tree);
    } else if (session && tree) {
        restoreScrollablePane(session, 'tree', tree);
    }
    if (session) {
        restoreScrollablePane(
            session,
            'info',
            effectiveInfoScrollPane(el.querySelector('#info-' + item.key + '.rule-info-panel'))
        );
    }
}

function ensureVirtualRunElement(rowEl, run, index) {
    var key = virtualRunDomKey(run);
    var current = rowEl.children[index] || null;
    var el = null;

    if (virtualElementDomKey(current) === key) {
        if (run.kind === 'rule' && ruleElementHasEmptyTree(current, run.item.key)) {
            el = takeDetachedRuleElementForRun(run);
            if (el) {
                rowEl.insertBefore(el, current);
                clearVirtualElementRenderState(current, { preserveRuleRenderState: true });
                rowEl.removeChild(current);
                afterDetachedRuleElementReattach(el, run);
                return el;
            }
        }
        if (canReuseVirtualElementForRun(current, run)) return current;

        el = createVirtualRunElement(run);
        if (!el) return null;

        rowEl.insertBefore(el, current);
        clearVirtualElementRenderState(current);
        rowEl.removeChild(current);
        clearVirtualRunRenderState(run);
        return el;
    }

    el = findReusableVirtualRunElement(rowEl, key, index + 1, run);
    if (el) {
        rowEl.insertBefore(el, current);
        return el;
    }

    el = takeDetachedRuleElementForRun(run);
    if (el) {
        rowEl.insertBefore(el, current);
        afterDetachedRuleElementReattach(el, run);
        return el;
    }

    el = createVirtualRunElement(run);
    if (!el) return null;

    rowEl.insertBefore(el, current);
    clearVirtualRunRenderState(run);
    return el;
}

function reconcileVirtualRowElement(row, rowEl, runs) {
    for (var i = 0; i < runs.length; i++) {
        var el = ensureVirtualRunElement(rowEl, runs[i], i);
        if (el) syncVirtualRunElement(el, runs[i]);
    }

    while (rowEl.children.length > runs.length) {
        var extra = rowEl.children[runs.length];
        clearVirtualElementRenderState(extra, { cache: true });
        rowEl.removeChild(extra);
    }

    registerVirtualRowSignatures(row);
}

function syncVirtualRowElement(row, rowEl) {
    if (!rowEl) return;

    var runs = virtualRowRuns(row);
    var signature = virtualRowSignature(runs);
    var cacheKey = row.id;

    if (virtualRuntime().virtualRowSignatureCache[cacheKey] !== signature ||
        !virtualRowDomMatchesRuns(rowEl, runs) ||
        virtualRowNeedsDetachedRuleRestore(rowEl, runs)) {
        reconcileVirtualRowElement(row, rowEl, runs);
        return;
    }

    for (var i = 0; i < runs.length; i++) {
        syncVirtualRunElement(rowEl.children[i], runs[i]);
    }
}

function parseGroupKeyId(id) {
    if (!id || id.indexOf('group-') !== 0) return null;
    var parts = id.substring('group-'.length).split('-');
    if (parts.length !== 2) return null;

    var si = Number(parts[0]);
    var gi = Number(parts[1]);
    if (!Number.isFinite(si) || !Number.isFinite(gi)) return null;
    return { si: si, gi: gi };
}

function invalidateMountedRulesInGroup(si, gi) {
    for (var ri = 0; ri < groupRuleCount(si, gi); ri++) {
        if (isRuleExpandedInLayout(si, gi, ri)) {
            clearRuleRenderState(si, gi, ri);
        }
    }
}

function clearUnmountedRuleRenderState(nextMounted) {
    for (var key in virtualRuntime().mountedRuleKeys) {
        if (!Object.prototype.hasOwnProperty.call(virtualRuntime().mountedRuleKeys, key)) continue;
        if (nextMounted[key]) continue;

        var ref = parseRuleKeyId('rule-' + key, 'rule-');
        if (!ref) continue;
        if (cacheMountedRuleElementForKey(key) ||
                virtualRuntime().detachedRuleElementCache[key]) {
            continue;
        }
        clearRuleRenderState(ref.si, ref.gi, ref.ri);
    }
}

function invalidateMountedRulesInRow(row) {
    if (!row) return;

    for (var i = 0; i < row.items.length; i++) {
        var item = row.items[i];
        if (item.kind === 'rule') {
            if (!shouldMountRuleItem(item)) continue;
            clearRuleRenderState(item.si, item.gi, item.ri);
        } else if (item.kind === 'group') {
            invalidateMountedRulesInRow(item.row);
        }
    }
}

function shouldMountRuleItemInRange(item, range) {
    if (shouldForceMountRule(item.si, item.gi, item.ri)) return true;
    return itemIntersectsRange(item, range);
}

function mountedRulesForModelFullScan(model, range) {
    var next = {};
    range = range || virtualRuntime().virtualRange;
    for (var i = 0; i < model.rows.length; i++) {
        var row = model.rows[i];
        if (!modelRowStageMounted(row, model)) continue;
        for (var j = 0; j < row.items.length; j++) {
            var item = row.items[j];
            if (item.kind === 'rule' && shouldMountRuleItemInRange(item, range)) {
                next[item.key] = true;
            }
        }
    }
    return next;
}

function mountedRulesForModelSlowScan(model, range) {
    var next = mountedRulesForModelFullScan(model, range);
    addForcedMountedRules(next, model);
    return next;
}

function firstRowItemIntersectingRange(row, range) {
    if (!row || !Array.isArray(row.items) || !range) return 0;

    var lo = 0;
    var hi = row.items.length;
    while (lo < hi) {
        var mid = (lo + hi) >> 1;
        var item = row.items[mid];
        var right = item ? item.globalLeft + item.width : -Infinity;
        if (right >= range.left) hi = mid;
        else lo = mid + 1;
    }
    return lo;
}

function collectMountedRulesFromRowRange(row, range, next) {
    if (!row || !Array.isArray(row.items)) return;

    var start = firstRowItemIntersectingRange(row, range);
    for (var i = start; i < row.items.length; i++) {
        var item = row.items[i];
        if (!item || item.globalLeft > range.right) break;

        if (item.kind === 'rule') {
            if (shouldMountRuleItemInRange(item, range)) next[item.key] = true;
        } else if (item.kind === 'group' && item.row && itemIntersectsRange(item, range)) {
            collectMountedRulesFromRowRange(item.row, range, next);
        }
    }
}

function addForcedMountedRule(next, model, ref) {
    if (!ref) return;
    var key = ruleKey(ref.si, ref.gi, ref.ri);
    if (model.rules && model.rules[key]) next[key] = true;
}

function addForcedMountedRules(next, model) {
    if (!isDiffActive()) return;
    addForcedMountedRule(next, model, diffRuntime().diffA);
    addForcedMountedRule(next, model, diffRuntime().diffB);
}

function mountedRulesForModel(model, range) {
    var next = {};
    var stages = visibleStagesForModel(model);
    range = range || virtualRuntime().virtualRange;
    for (var i = 0; i < stages.length; i++) {
        var stage = stages[i];
        if (!stage || !stage.row || !modelRowStageMounted(stage.row, model)) continue;
        if (!stageModelIntersectsRange(stage, range)) continue;
        collectMountedRulesFromRowRange(stage.row, range, next);
    }
    addForcedMountedRules(next, model);
    return next;
}

function sameMountedRuleKeys(left, right) {
    left = left || {};
    right = right || {};

    for (var leftKey in left) {
        if (!Object.prototype.hasOwnProperty.call(left, leftKey)) continue;
        if (!right[leftKey]) return false;
    }
    for (var rightKey in right) {
        if (!Object.prototype.hasOwnProperty.call(right, rightKey)) continue;
        if (!left[rightKey]) return false;
    }
    return true;
}

function mountedRuleNeedsViewportRender(si, gi, ri) {
    if (!isRuleExpandedInLayout(si, gi, ri)) return false;

    var key = ruleKey(si, gi, ri);
    var cell = document.getElementById('rule-' + key);
    if (!cell || !elementIntersectsLazyViewport(cell)) return false;

    return ruleTreeRenderNeeded(
        si,
        gi,
        ri,
        currentSearchQuery(),
        false,
        currentSearchScope()
    );
}

function virtualOverscanScreens() {
    var minWidth = RULE_WIDTH_MIN;
    var maxWidth = ruleWidthMax();
    if (!Number.isFinite(maxWidth) || maxWidth <= minWidth) return VIRTUAL_OVERSCAN_SCREENS;

    var resizeRevealScreens = maxWidth / minWidth - 1;
    return Math.min(
        VIRTUAL_OVERSCAN_MAX_SCREENS,
        Math.max(VIRTUAL_OVERSCAN_SCREENS, resizeRevealScreens)
    );
}

function updateVirtualRange() {
    if (!hasDOM()) return;
    var traceEl = document.querySelector('.trace');
    if (!traceEl) return;

    updateVirtualRangeForScrollLeft(traceEl, traceScrollLeftValue(traceEl));
}

function traceScrollLeftValue(traceEl) {
    var left = traceEl ? Number(traceEl.scrollLeft) : 0;
    return Number.isFinite(left) ? Math.max(0, left) : 0;
}

function traceScrollWidthValue(traceEl) {
    if (!traceEl) return 0;

    var cache = virtualRuntime().traceScrollWidthCache;
    var canvasGeneration = currentTraceCanvasDomGeneration();
    var rowsGeneration = currentVirtualRowsDomGeneration();
    var clientWidth = Number(traceEl.clientWidth) || 0;
    if (cache &&
            cache.target === traceEl &&
            cache.traceCanvasDomGeneration === canvasGeneration &&
            cache.virtualRowsDomGeneration === rowsGeneration &&
            cache.clientWidth === clientWidth) {
        return cache.scrollWidth;
    }

    var scrollWidth = Number(traceEl.scrollWidth) || 0;
    virtualRuntime().traceScrollWidthCache = {
        target: traceEl,
        traceCanvasDomGeneration: canvasGeneration,
        virtualRowsDomGeneration: rowsGeneration,
        clientWidth: clientWidth,
        scrollWidth: scrollWidth
    };
    return scrollWidth;
}

function updateVirtualRangeForScrollLeft(traceEl, scrollLeft) {
    var width = traceEl.clientWidth || window.innerWidth || 0;
    var overscan = Math.max(width * virtualOverscanScreens(), currentRuleWidthPx());
    scrollLeft = Number(scrollLeft);
    if (!Number.isFinite(scrollLeft)) scrollLeft = 0;
    scrollLeft = Math.max(0, scrollLeft);
    var nextRange = {
        left: Math.max(0, scrollLeft - overscan),
        right: scrollLeft + width + overscan
    };
    var previousRange = virtualRuntime().virtualRange || {};
    if (previousRange.left !== nextRange.left || previousRange.right !== nextRange.right) {
        virtualRuntime().traceViewportGeneration = (virtualRuntime().traceViewportGeneration || 0) + 1;
    }
    virtualRuntime().virtualRange = nextRange;
}

function clampTraceScrollLeftToModel(model) {
    if (!hasDOM() || !model) return false;
    var traceEl = document.querySelector('.trace');
    if (!traceEl) return false;

    var maxScrollLeft = traceRestorableMaxScrollLeft(model, traceEl);
    if (traceScrollLeftValue(traceEl) <= maxScrollLeft) return false;

    return setTraceScrollLeft(traceEl, maxScrollLeft, {
        model: model,
        suppressVirtualRefresh: true
    });
}

function traceScrollViewportWidth(traceEl) {
    var viewportWidth = traceEl.clientWidth || window.innerWidth || 0;
    return Number.isFinite(viewportWidth) && viewportWidth > 0 ? viewportWidth : 0;
}

function traceLogicalMaxScrollLeft(model, traceEl) {
    if (!model || !traceEl) return 0;
    var viewportWidth = traceScrollViewportWidth(traceEl);
    var totalWidth = Number(model.totalWidth);
    if (!Number.isFinite(totalWidth) || viewportWidth <= 0) return 0;
    return Math.max(0, totalWidth - viewportWidth);
}

function tracePhysicalMaxScrollLeft(traceEl) {
    if (!traceEl) return 0;
    var viewportWidth = traceScrollViewportWidth(traceEl);
    if (viewportWidth <= 0) return 0;
    return Math.max(0, traceScrollWidthValue(traceEl) - viewportWidth);
}

function traceRestorableMaxScrollLeft(model, traceEl) {
    return Math.max(
        traceLogicalMaxScrollLeft(model, traceEl),
        tracePhysicalMaxScrollLeft(traceEl)
    );
}

function maxModelScrollLeft(model, traceEl) {
    return traceRestorableMaxScrollLeft(model, traceEl);
}

function traceScrollTransactionMax(traceEl, options) {
    options = options || {};
    var explicitMax = Number(options.maxScrollLeft);
    if (Number.isFinite(explicitMax)) return Math.max(0, explicitMax);
    if (options.model) return maxModelScrollLeft(options.model, traceEl);

    var viewportWidth = traceEl ? (traceEl.clientWidth || 0) : 0;
    return Math.max(0, traceScrollWidthValue(traceEl) - viewportWidth);
}

function traceScrollTransactionTarget(traceEl, target, options) {
    options = options || {};
    var max = traceScrollTransactionMax(traceEl, options);
    var next = Number(target);
    if (!Number.isFinite(next)) next = 0;
    next = Math.max(0, Math.min(max, next));
    if (options.roundToDevicePixels && typeof roundTraceScrollToDevicePixels === 'function') {
        next = roundTraceScrollToDevicePixels(next);
        next = Math.max(0, Math.min(max, next));
    }
    return next;
}

function traceScrollTransactionRefreshOptions(options) {
    if (!options || options.refreshVirtualRows === true) return {};
    return typeof options.refreshVirtualRows === 'object' ? options.refreshVirtualRows : {};
}

function syncTraceScrollTransactionDiffArrows(traceEl, options, changed) {
    options = options || {};
    if (!changed && !options.forceVisualSync) return;
    if (options.syncDiffMoveArrows !== false &&
            typeof syncDiffMoveArrowsForTraceScroll === 'function') {
        syncDiffMoveArrowsForTraceScroll(traceEl);
    }
    if (options.scheduleDiffMoveArrows !== false &&
            typeof scheduleDiffMoveArrows === 'function') {
        scheduleDiffMoveArrows();
    }
}

function setTraceScrollLeft(traceEl, target, options) {
    options = options || {};
    if (!traceEl) return false;

    if (options.cancelWheelGlide && typeof resetWheelScrollRuntime === 'function') {
        resetWheelScrollRuntime();
    }

    var before = traceScrollLeftValue(traceEl);
    var next = traceScrollTransactionTarget(traceEl, target, options);
    var changed = Math.abs(next - before) > VIEWPORT_ANCHOR_EPSILON;
    if (changed) traceEl.scrollLeft = next;

    if (options.suppressVirtualRefresh) suppressNextVirtualScrollRefresh(traceEl);

    if (changed && options.runVirtualScrollHandler &&
            typeof onTraceVirtualScroll === 'function') {
        onTraceVirtualScroll();
        return true;
    }

    if (options.updateVirtualRange) {
        updateVirtualRangeForScrollLeft(traceEl, traceScrollLeftValue(traceEl));
    }
    if (options.refreshVirtualRows && typeof refreshVirtualRowsNow === 'function') {
        refreshVirtualRowsNow(traceScrollTransactionRefreshOptions(options));
    } else if (options.scheduleVirtualRowsRefresh &&
            typeof scheduleVirtualRowsRefresh === 'function') {
        scheduleVirtualRowsRefresh();
    }
    if (options.scheduleVisibleRuleRenderScan &&
            typeof scheduleVisibleRuleRenderScan === 'function') {
        scheduleVisibleRuleRenderScan();
    }
    if (options.updateTraceScrollbar && typeof updateTraceScrollbar === 'function') {
        updateTraceScrollbar(!!options.traceScrollbarActive);
    }
    if (options.updateTraceAnchorLineNow && typeof updateTraceAnchorLineNow === 'function') {
        updateTraceAnchorLineNow();
    } else if (options.scheduleTraceAnchorLineUpdate &&
            typeof scheduleTraceAnchorLineUpdate === 'function') {
        scheduleTraceAnchorLineUpdate();
    }

    syncTraceScrollTransactionDiffArrows(traceEl, options, changed);
    return changed;
}

function preserveVirtualRefreshScroll(traceEl, model, desiredScrollLeft) {
    if (!traceEl || !model) return false;

    return setTraceScrollLeft(traceEl, desiredScrollLeft, {
        model: model,
        suppressVirtualRefresh: true
    });
}

function virtualScrollSuppressionNow() {
    if (typeof performance !== 'undefined' && performance && performance.now) {
        return performance.now();
    }
    return Date.now ? Date.now() : 0;
}

function suppressNextVirtualScrollRefresh(traceEl) {
    if (!traceEl) return;
    virtualRuntime().suppressNextVirtualScroll = {
        scrollLeft: traceScrollLeftValue(traceEl),
        expiresAt: virtualScrollSuppressionNow() + 250
    };
}

function consumeSuppressedVirtualScrollRefresh(traceEl) {
    var suppression = virtualRuntime().suppressNextVirtualScroll;
    if (!traceEl) return false;

    var now = virtualScrollSuppressionNow();
    if (suppression) {
        virtualRuntime().suppressNextVirtualScroll = null;
        if (now <= suppression.expiresAt &&
            Math.abs(traceScrollLeftValue(traceEl) - suppression.scrollLeft) <= VIEWPORT_ANCHOR_EPSILON) {
            return true;
        }
    }

    var last = virtualRuntime().lastVirtualRefresh;
    return !!last &&
           now - last.at <= 250 &&
           Math.abs(traceScrollLeftValue(traceEl) - last.scrollLeft) <= VIEWPORT_ANCHOR_EPSILON;
}

function recordTraceViewportInteraction() {
    virtualRuntime().lastTraceViewportInteractionAt = virtualScrollSuppressionNow();
    virtualRuntime().lastTraceViewportInteractionGeneration =
        (Number(virtualRuntime().lastTraceViewportInteractionGeneration) || 0) + 1;
}

var TRACE_SCROLL_INTERACTION_ACTIVE_MS = 150;

function traceViewportInteractionActive() {
    var last = Number(virtualRuntime().lastTraceViewportInteractionAt) || 0;
    if (!last) return false;
    var elapsed = virtualScrollSuppressionNow() - last;
    return elapsed >= 0 && elapsed <= TRACE_SCROLL_INTERACTION_ACTIVE_MS;
}

function markVirtualRefreshComplete(traceEl) {
    if (!traceEl) return;
    virtualRuntime().lastVirtualRefresh = {
        scrollLeft: traceScrollLeftValue(traceEl),
        at: virtualScrollSuppressionNow()
    };
}

function virtualScrollRefreshSlackPx(traceEl) {
    if (!traceEl) return 0;
    var width = traceEl.clientWidth || window.innerWidth || 0;
    if (!(width > 0)) return 0;

    var slack = Math.max(width * virtualOverscanScreens(), currentRuleWidthPx());
    if (shouldVirtualizeStageShellsForCurrentTrace()) {
        slack = Math.min(
            slack,
            Math.max(width * STAGE_SHELL_OVERSCAN_SCREENS, currentRuleWidthPx())
        );
    }
    // Half the effective overscan: the cells/shells mounted by the last
    // refresh still cover the viewport with at least the other half spare.
    return slack / 2;
}

function virtualScrollWithinRefreshSlack(traceEl) {
    if (!traceEl) return false;
    if (virtualRuntime().traceVirtualLayoutDirty) return false;

    var last = virtualRuntime().lastVirtualRefresh;
    if (!last || !Number.isFinite(last.scrollLeft)) return false;

    var slack = virtualScrollRefreshSlackPx(traceEl);
    if (!(slack > 0)) return false;
    return Math.abs(traceScrollLeftValue(traceEl) - last.scrollLeft) <= slack;
}

function currentVirtualRowsDomGeneration() {
    return virtualRuntime().virtualRowsDomGeneration || 0;
}

function bumpVirtualRowsDomGeneration() {
    virtualRuntime().virtualRowsDomGeneration = currentVirtualRowsDomGeneration() + 1;
    return virtualRuntime().virtualRowsDomGeneration;
}

function virtualRowsDomGenerationCurrent(generation) {
    return currentVirtualRowsDomGeneration() === generation;
}

function canCommitVirtualRows(label, options) {
    options = options || {};
    if (options.traceCanvasDomGeneration !== undefined &&
            !traceCanvasDomGenerationCurrent(options.traceCanvasDomGeneration)) {
        recordVisualCommitDenied('trace-canvas', label, 'stale_dom_generation', options);
        return false;
    }
    return canCommitVisualWork('virtual-rows', label, options) &&
        canCommitVisualWork('trace-canvas', label, options);
}

var virtualRowsSurface = createVisualSurface({
    name: 'virtual-rows',
    epochScopes: ['trace'],
    allowMissingTarget: true,
    resolveTarget: function(ref, options) {
        options = options || {};
        if (options.target) return options.target;
        if (options.targetId && hasDOM() && document.getElementById) {
            return document.getElementById(options.targetId);
        }
        if (options.stageIndex !== undefined) return stageRulesRowElement(options.stageIndex);
        if (options.selector && hasDOM() && document.querySelector) {
            return document.querySelector(options.selector);
        }
        return null;
    }
});

function commitVirtualRowsSurface(visualCtx, options, writer) {
    options = options || {};
    if (!canCommitVirtualRows(options.label || 'virtual-rows-surface', options)) return false;
    return virtualRowsSurface.commit(visualCtx, options, writer);
}

function currentTraceCanvasDomGeneration() {
    return virtualRuntime().traceCanvasDomGeneration || 0;
}

function bumpTraceCanvasDomGeneration() {
    virtualRuntime().traceCanvasDomGeneration = currentTraceCanvasDomGeneration() + 1;
    return virtualRuntime().traceCanvasDomGeneration;
}

function traceCanvasDomGenerationCurrent(generation) {
    return Number(generation) === currentTraceCanvasDomGeneration();
}

function traceCanvasElement() {
    if (!hasDOM() || !document.getElementById) return null;
    var canvas = document.getElementById('trace-canvas');
    if (!canvas || !canvas.classList || !canvas.classList.contains ||
            !canvas.classList.contains('trace-canvas')) {
        return null;
    }
    return canvas;
}

function modelTracePaddingPx(model, side) {
    var index = modelLayoutIndex(model);
    if (index) {
        var value = side === 'left' ? index.paddingLeft : index.paddingRight;
        if (Number.isFinite(value)) return value;
    }
    return tracePaddingPx(side);
}

function traceCanvasWidthForModel(model) {
    if (!model) return 0;
    return Math.max(
        0,
        model.totalWidth - modelTracePaddingPx(model, 'left') - modelTracePaddingPx(model, 'right')
    );
}

function syncTraceCanvasWidthStyle(canvas, width) {
    if (!canvas || !canvas.style) return false;

    var px = width + 'px';
    var changed = canvas.style.flexBasis !== px ||
        canvas.style.width !== px ||
        canvas.style.minWidth !== px ||
        canvas.style.maxWidth !== px;
    canvas.style.flexBasis = px;
    canvas.style.width = px;
    canvas.style.minWidth = px;
    canvas.style.maxWidth = px;
    if (changed) bumpTraceCanvasDomGeneration();
    return true;
}

function syncTraceCanvasWidth(model) {
    var canvas = traceCanvasElement();
    if (!canvas || !model) return;

    var width = traceCanvasWidthForModel(model);
    syncTraceCanvasWidthStyle(canvas, width);
}

function stageShellVirtualizationEnabledForCount(count) {
    count = Number(count) || 0;
    return count > STAGE_SHELL_VIRTUALIZATION_THRESHOLD;
}

function modelLayoutIndex(model) {
    return model && model.index && Array.isArray(model.index.visibleStages)
        ? model.index
        : null;
}

function visibleStageCountForModel(model) {
    var index = modelLayoutIndex(model);
    if (index && Number.isFinite(index.visibleStageCount)) {
        return index.visibleStageCount;
    }
    if (!model || !Array.isArray(model.stages)) return 0;

    var count = 0;
    for (var i = 0; i < model.stages.length; i++) {
        if (model.stages[i] && !model.stages[i].hidden) count++;
    }
    return count;
}

function currentVisibleStageCount() {
    var cache = virtualRuntime().visibleStageCountCache;
    var epoch = currentRuntimeEpoch().trace;
    var showEmpty = showEmptyStages();
    var stageCount = currentStageCount();

    if (cache &&
        cache.traceEpoch === epoch &&
        cache.showEmptyStages === showEmpty &&
        cache.stageCount === stageCount) {
        return cache.count;
    }

    var count = 0;
    for (var si = 0; si < stageCount; si++) {
        if (stageVisible(si)) count++;
    }
    virtualRuntime().visibleStageCountCache = {
        traceEpoch: epoch,
        showEmptyStages: showEmpty,
        stageCount: stageCount,
        count: count
    };
    return count;
}

function shouldVirtualizeStageShells(model) {
    var count = model ? visibleStageCountForModel(model) : currentVisibleStageCount();
    return stageShellVirtualizationEnabledForCount(count);
}

function shouldVirtualizeStageShellsForCurrentTrace() {
    return shouldVirtualizeStageShells(null);
}

function stageShellViewportRange(traceEl) {
    var width = traceEl ? (traceEl.clientWidth || window.innerWidth || 0) : 0;
    var left = traceScrollLeftValue(traceEl);
    return {
        left: left,
        right: left + Math.max(0, width),
        width: Math.max(0, width)
    };
}

function stageShellProjectionRange(traceEl) {
    var viewport = stageShellViewportRange(traceEl);
    var overscan = Math.max(
        viewport.width * STAGE_SHELL_OVERSCAN_SCREENS,
        currentRuleWidthPx()
    );
    return {
        left: Math.max(0, viewport.left - overscan),
        right: viewport.right + overscan,
        width: viewport.width
    };
}

function stageModelIntersectsRange(stage, range) {
    return !!stage && !!range && !stage.hidden &&
           stage.left + stage.width >= range.left &&
           stage.left <= range.right;
}

function visibleStagesForModel(model) {
    var index = modelLayoutIndex(model);
    if (index) return index.visibleStages;

    var stages = [];
    if (!model || !Array.isArray(model.stages)) return stages;
    for (var si = 0; si < model.stages.length; si++) {
        if (model.stages[si] && !model.stages[si].hidden) stages.push(model.stages[si]);
    }
    return stages;
}

function firstVisibleStageIntersectingRange(stages, range) {
    if (!stages || !stages.length || !range) return 0;

    var lo = 0;
    var hi = stages.length;
    while (lo < hi) {
        var mid = (lo + hi) >> 1;
        var stage = stages[mid];
        var right = stage ? stage.left + stage.width : -Infinity;
        if (right > range.left) hi = mid;
        else lo = mid + 1;
    }
    return lo;
}

function forcedMountedStageKeys() {
    var keys = {};

    function mark(si) {
        if (!Number.isFinite(si)) return;
        keys[String(si)] = true;
    }

    function markRef(ref) {
        if (!ref) return;
        mark(ref.si);
    }

    function markDiffEndpointIsland(ref) {
        if (!ref || !Number.isFinite(ref.si)) return;
        var context = Math.max(0, Math.floor(Number(DIFF_ENDPOINT_STAGE_CONTEXT)) || 0);
        for (var offset = -context; offset <= context; offset++) {
            mark(ref.si + offset);
        }
    }

    markDiffEndpointIsland(diffRuntime().diffA);
    markDiffEndpointIsland(diffRuntime().diffB);
    if (typeof fullscreenCurrentRule === 'function') {
        markRef(fullscreenCurrentRule());
    }

    return keys;
}

function stageDistanceToRange(stage, range) {
    if (!stage || !range) return Infinity;
    var left = stage.left;
    var right = stage.left + stage.width;
    if (right > range.left && left < range.right) return 0;
    return right <= range.left ? range.left - right : left - range.right;
}

function maxProjectedStageShells(traceEl) {
    var viewport = stageShellViewportRange(traceEl);
    var collapsedOuterWidth = Math.max(1, STAGE_COLLAPSED_WIDTH + STAGE_GAP_WIDTH);
    var visibleCapacity = Math.ceil(viewport.width / collapsedOuterWidth) + 8;
    return Math.max(STAGE_SHELL_MAX_MOUNTED, visibleCapacity);
}

function projectedStageModelsFullScan(model, traceEl) {
    if (!model || !Array.isArray(model.stages)) return [];
    if (!shouldVirtualizeStageShells(model)) {
        return visibleStagesForModel(model).slice();
    }

    var projectionRange = stageShellProjectionRange(traceEl);
    var viewportRange = stageShellViewportRange(traceEl);
    var forced = forcedMountedStageKeys();
    var candidates = [];

    for (var i = 0; i < model.stages.length; i++) {
        var stage = model.stages[i];
        if (!stage || stage.hidden) continue;
        var key = String(stage.si);
        if (!forced[key] && !stageModelIntersectsRange(stage, projectionRange)) continue;
        candidates.push({
            stage: stage,
            forced: !!forced[key],
            distance: stageDistanceToRange(stage, viewportRange)
        });
    }

    var limit = maxProjectedStageShells(traceEl);
    if (candidates.length > limit) {
        candidates.sort(function(a, b) {
            if (a.forced !== b.forced) return a.forced ? -1 : 1;
            if (a.distance !== b.distance) return a.distance - b.distance;
            return a.stage.si - b.stage.si;
        });
        candidates = candidates.slice(0, limit);
    }

    candidates.sort(function(a, b) {
        return a.stage.si - b.stage.si;
    });

    var stages = [];
    for (var j = 0; j < candidates.length; j++) {
        stages.push(candidates[j].stage);
    }
    return stages;
}

function projectedStageModels(model, traceEl) {
    if (!model || !Array.isArray(model.stages)) return [];
    var visibleStages = visibleStagesForModel(model);
    if (!shouldVirtualizeStageShells(model)) {
        return visibleStages.slice();
    }

    var projectionRange = stageShellProjectionRange(traceEl);
    var viewportRange = stageShellViewportRange(traceEl);
    var forced = forcedMountedStageKeys();
    var candidates = [];
    var candidateKeys = {};

    function pushCandidate(stage, isForced) {
        if (!stage || stage.hidden) return;
        var key = String(stage.si);
        if (candidateKeys[key]) {
            if (isForced) {
                for (var c = 0; c < candidates.length; c++) {
                    if (String(candidates[c].stage.si) === key) {
                        candidates[c].forced = true;
                        candidates[c].distance = stageDistanceToRange(stage, viewportRange);
                        break;
                    }
                }
            }
            return;
        }
        candidateKeys[key] = true;
        candidates.push({
            stage: stage,
            forced: !!isForced,
            distance: stageDistanceToRange(stage, viewportRange)
        });
    }

    var start = firstVisibleStageIntersectingRange(visibleStages, projectionRange);
    for (var i = start; i < visibleStages.length; i++) {
        var stage = visibleStages[i];
        if (!stage || stage.left >= projectionRange.right) break;
        pushCandidate(stage, !!forced[String(stage.si)]);
    }

    for (var forcedKey in forced) {
        if (!Object.prototype.hasOwnProperty.call(forced, forcedKey)) continue;
        var forcedStage = model.stages[Number(forcedKey)];
        pushCandidate(forcedStage, true);
    }

    var limit = maxProjectedStageShells(traceEl);
    if (candidates.length > limit) {
        candidates.sort(function(a, b) {
            if (a.forced !== b.forced) return a.forced ? -1 : 1;
            if (a.distance !== b.distance) return a.distance - b.distance;
            return a.stage.si - b.stage.si;
        });
        candidates = candidates.slice(0, limit);
    }

    candidates.sort(function(a, b) {
        return a.stage.si - b.stage.si;
    });

    var stages = [];
    for (var j = 0; j < candidates.length; j++) {
        stages.push(candidates[j].stage);
    }
    return stages;
}

function projectedStageKeyMap(stages) {
    var keys = {};
    for (var i = 0; i < stages.length; i++) {
        keys[String(stages[i].si)] = true;
    }
    return keys;
}

function stageSpacerHtml(width) {
    if (width <= 0) return '';
    return '<div class="stage-virtual-spacer" aria-hidden="true"' +
           virtualWidthAttr(width) +
           ' style="' + virtualStyle(width, 0) + '"></div>';
}

function virtualStageProjectionSignature(model, stages) {
    var parts = [
        shouldVirtualizeStageShells(model) ? 'virtual' : 'eager',
        Math.round(traceCanvasWidthForModel(model) * 100) / 100
    ];
    for (var i = 0; i < stages.length; i++) {
        var stage = stages[i];
        parts.push(
            stage.si + ':' +
            Math.round((stage.left - modelTracePaddingPx(model, 'left')) * 100) / 100 + ':' +
            Math.round(stage.width * 100) / 100 + ':' +
            (stage.open ? 'o' : 'c') + ':' +
            (stageHasRules(stage.si) ? 'r' : 'e') + ':' +
            (searchRuntime().collapsedSearchIndicators &&
             searchRuntime().collapsedSearchIndicators.stages &&
             searchRuntime().collapsedSearchIndicators.stages[String(stage.si)] ? 's' : '')
        );
    }
    return parts.join('|');
}

function virtualStageShellStructureSignature(stage) {
    return [
        stage.si,
        stage.open ? 'o' : 'c',
        stageHasRules(stage.si) ? 'r' : 'e',
        searchRuntime().collapsedSearchIndicators &&
            searchRuntime().collapsedSearchIndicators.stages &&
            searchRuntime().collapsedSearchIndicators.stages[String(stage.si)] ? 's' : ''
    ].join(':');
}

function stageShellRunsForProjection(model, stages) {
    var runs = [];
    var cursor = 0;
    var paddingLeft = modelTracePaddingPx(model, 'left');

    for (var i = 0; i < stages.length; i++) {
        var stage = stages[i];
        var localLeft = Math.max(0, stage.left - paddingLeft);
        var spacerWidth = localLeft - cursor;
        if (spacerWidth > 0) runs.push({ kind: 'spacer', width: spacerWidth });
        runs.push({
            kind: 'stage',
            si: stage.si,
            width: stage.width,
            signature: virtualStageShellStructureSignature(stage)
        });
        cursor = localLeft + stage.width + STAGE_GAP_WIDTH;
    }
    return runs;
}

function stageShellRunHtml(run) {
    if (run.kind === 'spacer') return stageSpacerHtml(run.width);
    return stageShellHtml(run.si, true, { width: run.width });
}

function renderStageShellRunsHtml(runs) {
    var html = '';
    for (var i = 0; i < runs.length; i++) {
        html += stageShellRunHtml(runs[i]);
    }
    return html;
}

function renderVirtualStageShellProjectionHtml(model, stages) {
    return renderStageShellRunsHtml(stageShellRunsForProjection(model, stages));
}

function syncEagerStageShellsFromModel(model) {
    if (!model || !hasDOM()) return false;
    for (var si = 0; si < currentStageCount(); si++) {
        var stage = model.stages && model.stages[si];
        if (!stage || stage.hidden) continue;

        var colEl = document.getElementById('stage-col-' + si);
        var expEl = document.getElementById('stage-exp-' + si);
        if (!colEl || !expEl) continue;

        var open = !!stage.open;
        setVirtualCssCustomProperty(colEl, 'display', open ? 'none' : 'flex');
        setVirtualCssCustomProperty(expEl, 'display', open ? 'flex' : 'none');
        if (stageHasRules(si)) {
            setVirtualCssCustomProperty(expEl, '--stage-width', widthCalc(resizeStageWidthParts(stage)));
        }
    }
    return true;
}

function stageShellRunDomKey(run) {
    return run.kind === 'spacer' ? 'spacer' : 'stage-' + run.si;
}

function stageShellElementDomKey(el) {
    if (!el || !el.classList || !el.classList.contains) return '';
    if (el.classList.contains('stage-virtual-spacer')) return 'spacer';
    if (el.classList.contains('stage-wrap') && el.getAttribute) {
        var si = el.getAttribute('data-stage-si');
        if (si !== null && si !== undefined && si !== '') return 'stage-' + si;
    }
    return '';
}

function canReuseStageShellElement(el, run) {
    if (!el || !run) return false;
    if (run.kind === 'spacer') return true;
    return !!el.__stageShellStructureSignature &&
        el.__stageShellStructureSignature === run.signature;
}

function stampStageShellElementSignatures(canvas, runs) {
    if (!canvas || !canvas.children) return;
    for (var i = 0; i < runs.length && i < canvas.children.length; i++) {
        if (runs[i].kind === 'stage') {
            canvas.children[i].__stageShellStructureSignature = runs[i].signature;
        }
    }
}

function cacheStageShellRuleElements(el) {
    if (!el || !el.querySelectorAll) return;
    var cells = el.querySelectorAll('.rule-cell[id^="rule-"]');
    for (var i = 0; i < cells.length; i++) {
        cacheDetachedRuleElementForReuse(cells[i]);
    }
}

function removeStageShellElement(canvas, el) {
    cacheStageShellRuleElements(el);
    canvas.removeChild(el);
}

function createStageShellRunElement(run) {
    var wrapper = document.createElement('div');
    wrapper.innerHTML = stageShellRunHtml(run);
    var el = wrapper.firstElementChild;
    if (el && run.kind === 'stage') {
        el.__stageShellStructureSignature = run.signature;
    }
    return el;
}

function findReusableStageShellElement(canvas, key, startIndex, run) {
    if (key === 'spacer') return null;
    for (var i = startIndex; i < canvas.children.length; i++) {
        var el = canvas.children[i];
        if (stageShellElementDomKey(el) === key &&
            canReuseStageShellElement(el, run)) {
            return el;
        }
    }
    return null;
}

function syncStageShellRunElement(el, run) {
    setVirtualBoxWidth(el, run.width);
    if (run.kind === 'spacer') {
        return;
    }

    var exp = el.querySelector ? el.querySelector('.stage-exp') : null;
    if (!exp) return;
    setVirtualBoxWidth(exp, run.width);
    setVirtualCssCustomProperty(exp, '--stage-width', run.width + 'px');
}

function reconcileVirtualStageShells(canvas, runs) {
    var structural = false;
    var expectedKeys = {};
    var i;

    for (i = 0; i < runs.length; i++) {
        if (runs[i].kind === 'stage') expectedKeys[stageShellRunDomKey(runs[i])] = true;
    }

    for (i = 0; i < runs.length; i++) {
        var run = runs[i];
        var key = stageShellRunDomKey(run);
        var current = canvas.children[i] || null;

        while (current) {
            var currentKey = stageShellElementDomKey(current);
            if (currentKey === 'spacer' || !currentKey || expectedKeys[currentKey]) break;
            removeStageShellElement(canvas, current);
            structural = true;
            current = canvas.children[i] || null;
        }

        var el = null;
        if (stageShellElementDomKey(current) === key) {
            if (canReuseStageShellElement(current, run)) {
                el = current;
            } else {
                el = createStageShellRunElement(run);
                if (!el) continue;
                canvas.insertBefore(el, current);
                removeStageShellElement(canvas, current);
                structural = true;
            }
        } else {
            el = findReusableStageShellElement(canvas, key, i + 1, run) ||
                createStageShellRunElement(run);
            if (!el) continue;
            canvas.insertBefore(el, current);
            structural = true;
        }
        syncStageShellRunElement(el, run);
    }

    while (canvas.children.length > runs.length) {
        removeStageShellElement(canvas, canvas.children[runs.length]);
        structural = true;
    }
    return structural;
}

function syncVirtualStageShells(model, traceEl) {
    var canvas = traceCanvasElement();
    if (!canvas || !model) return false;

    var stages = projectedStageModels(model, traceEl);
    var mountedStageKeys = projectedStageKeyMap(stages);
    virtualRuntime().mountedStageKeys = mountedStageKeys;

    if (!shouldVirtualizeStageShells(model)) {
        syncEagerStageShellsFromModel(model);
        if (!virtualRuntime().stageShellSignature && !canvas.__stageShellSignature) {
            virtualRuntime().stageShellSignature = '';
            canvas.__stageShellSignature = '';
            return false;
        }

        cacheMountedRuleElementsForStageShellReplacement();
        canvas.innerHTML = renderVirtualStageShellProjectionHtml(model, stages);
        bumpTraceCanvasDomGeneration();
        bumpVirtualRowsDomGeneration();
        virtualRuntime().stageShellSignature = '';
        canvas.__stageShellSignature = '';
        return true;
    }

    var signature = virtualStageProjectionSignature(model, stages);
    if (virtualRuntime().stageShellSignature === signature &&
        canvas.__stageShellSignature === signature) {
        return false;
    }

    var runs = stageShellRunsForProjection(model, stages);
    var structural;
    if (!canvas.children || !canvas.children.length) {
        canvas.innerHTML = renderStageShellRunsHtml(runs);
        stampStageShellElementSignatures(canvas, runs);
        structural = runs.length > 0;
    } else {
        structural = reconcileVirtualStageShells(canvas, runs);
    }

    if (structural) {
        bumpTraceCanvasDomGeneration();
        bumpVirtualRowsDomGeneration();
    }
    virtualRuntime().stageShellSignature = signature;
    canvas.__stageShellSignature = signature;
    return true;
}

function modelRowStageMounted(row, model) {
    if (!row || !shouldVirtualizeStageShells(model)) return true;
    return !!virtualRuntime().mountedStageKeys[String(row.si)];
}

function restoreVirtualRefreshScroll(traceEl, model, desiredScrollLeft) {
    if (!preserveVirtualRefreshScroll(traceEl, model, desiredScrollLeft)) return false;

    var restoredScrollLeft = traceScrollLeftValue(traceEl);
    updateVirtualRangeForScrollLeft(traceEl, restoredScrollLeft);
    preserveVirtualRefreshScroll(traceEl, model, restoredScrollLeft);
    return true;
}

function desiredVirtualRefreshScrollLeft(traceEl, options) {
    if (options && Number.isFinite(options.desiredScrollLeft)) {
        return Math.max(0, Number(options.desiredScrollLeft));
    }
    return traceScrollLeftValue(traceEl);
}

function traceLayoutModelForVirtualRefresh(options) {
    if (options && options.forceRebuild) return rebuildTraceLayoutModel();
    return getTraceLayoutModel();
}

function stageRulesRowElement(si) {
    if (!hasDOM()) return null;
    var stage = document.getElementById('stage-exp-' + si);
    return stage ? stage.querySelector('.rules-row') : null;
}

function resizeStageWidthParts(stage) {
    if (!stage || stage.hidden || !stage.open || !stageHasRules(stage.si)) {
        return { rules: 0, px: stage ? stage.width || 0 : 0 };
    }
    return stageExpandedWidthParts(stage.si);
}

function setVirtualCssCustomProperty(el, name, value) {
    if (!el || !el.style) return;
    if (el.style.setProperty) {
        el.style.setProperty(name, value);
    } else {
        el.style[name] = value;
    }
}

function inlineVisibleRuleRenderBudgetAllows(start, renderedAny, budgetMs) {
    if (!renderedAny) return true;
    if (typeof Date === 'undefined' || !Date.now) return false;
    return Date.now() - start < budgetMs;
}

function scheduleMountedRuleVisibilityChecks(nextMounted, previousMounted, options) {
    options = options || {};
    var inlineRender = !!options.inlineEmptyTreeRender;
    var inlineStart = typeof Date !== 'undefined' && Date.now ? Date.now() : 0;
    var inlineRenderedAny = false;
    var inlineBudget = options.inlineRenderBudgetMs || RULE_SCROLL_INLINE_RENDER_BUDGET_MS;
    var inlineQueryReady = false;
    var query = '';
    var scope = '';

    for (var key in nextMounted) {
        if (!Object.prototype.hasOwnProperty.call(nextMounted, key)) continue;
        var ref = parseRuleKeyId('rule-' + key, 'rule-');
        if (!ref || !isRuleExpandedInLayout(ref.si, ref.gi, ref.ri)) continue;
        if (!previousMounted || !previousMounted[key] ||
            mountedRuleNeedsViewportRender(ref.si, ref.gi, ref.ri)) {
            var renderedInline = false;
            if (inlineRender &&
                    typeof renderVisibleEmptyRuleTreeImmediately === 'function' &&
                    inlineVisibleRuleRenderBudgetAllows(
                        inlineStart,
                        inlineRenderedAny,
                        inlineBudget
                    )) {
                if (!inlineQueryReady) {
                    query = currentSearchQuery();
                    scope = currentSearchScope();
                    inlineQueryReady = true;
                }
                renderedInline = renderVisibleEmptyRuleTreeImmediately(
                    ref.si,
                    ref.gi,
                    ref.ri,
                    query,
                    scope
                );
                inlineRenderedAny = inlineRenderedAny || renderedInline;
            }
            if (!renderedInline && typeof showVisibleRuleTreePendingIfEmpty === 'function') {
                showVisibleRuleTreePendingIfEmpty(ref.si, ref.gi, ref.ri, 'virtual-scroll');
            }
            if (!renderedInline) scheduleRuleVisibilityCheck(ref.si, ref.gi, ref.ri);
        }
    }
}

function refreshVirtualRowsCoreForCommit(options) {
    if (!virtualRuntime().virtualizerReady || !hasDOM()) return false;
    bumpVirtualRowsDomGeneration();

    var traceEl = document.querySelector('.trace');
    var desiredScrollLeft = desiredVirtualRefreshScrollLeft(traceEl, options);
    var model = traceLayoutModelForVirtualRefresh(options);
    if (!options || !options.deferCanvasWidth) syncTraceCanvasWidth(model);
    restoreVirtualRefreshScroll(traceEl, model, desiredScrollLeft);
    if (clampTraceScrollLeftToModel(model)) {
        desiredScrollLeft = traceScrollLeftValue(traceEl);
        restoreVirtualRefreshScroll(traceEl, model, desiredScrollLeft);
    }
    updateVirtualRange();
    syncVirtualStageShells(model, traceEl);

    var nextMounted = mountedRulesForModel(model);
    var previousMounted = virtualRuntime().mountedRuleKeys;
    clearUnmountedRuleRenderState(nextMounted);

    for (var si = 0; si < currentStageCount(); si++) {
        var rowEl = stageRulesRowElement(si);
        if (!rowEl || rowEl.classList.contains('empty-rules-row')) continue;

        rowEl.classList.add('virtual-row');
        if (rowEl.setAttribute) rowEl.setAttribute('data-virtual-row', 'stage-' + si);
        var stageModel = model.stages[si];
        if (stageModel && stageModel.row) {
            syncVirtualRowElement(stageModel.row, rowEl);
        } else if (treeWrapHasDomContent(rowEl)) {
            cacheVirtualRowRuleElements(rowEl);
            rowEl.innerHTML = '';
            virtualRuntime().virtualRowSignatureCache['stage-' + si] = '';
        }
    }
    restoreVirtualRefreshScroll(traceEl, model, desiredScrollLeft);
    virtualRuntime().mountedRuleKeys = nextMounted;
    virtualRuntime().mountedVirtualRange = virtualRuntime().virtualRange ? {
        left: Number(virtualRuntime().virtualRange.left) || 0,
        right: Number(virtualRuntime().virtualRange.right) || 0
    } : null;
    updateSearchLabelHighlights(currentSearchQuery(), currentSearchScope(), {
        skipCollapsedIndicators: !searchRuntime().collapsedSearchIndicatorsDirty
    });

    scheduleMountedRuleVisibilityChecks(nextMounted, previousMounted, {
        inlineEmptyTreeRender: !(options && options.allowDuringResize) &&
            !traceViewportInteractionActive()
    });

    updateTraceScrollbar(false);
    scheduleTraceAnchorLineUpdate();
    markVirtualRefreshComplete(traceEl);
    scheduleVisibleSearchWarmup('virtual-refresh');
    return true;
}

function refreshVirtualRowsCore(visualCtx, options) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        options = visualCtx || {};
        var immediateResult = false;
        runVisualSurfaceCommitNow(
            'virtual-rows-core',
            ['virtual-rows', 'trace-canvas'],
            ['trace'],
            function(ctx) {
                immediateResult = refreshVirtualRowsCore(ctx, options);
            }
        );
        return immediateResult;
    }

    options = options || {};
    var result = false;
    var committed = commitVirtualRowsSurface(visualCtx, {
        label: 'virtual-rows-core',
        traceCanvasDomGeneration: currentTraceCanvasDomGeneration(),
        domGenerations: {
            virtualRows: currentVirtualRowsDomGeneration(),
            traceCanvas: currentTraceCanvasDomGeneration()
        }
    }, function() {
        result = refreshVirtualRowsCoreForCommit(options);
    });
    return committed ? result : false;
}

function refreshVirtualRows(options, visualCtx) {
    options = options || {};
    virtualRuntime().virtualRenderFrame = null;

    function runCore(coreOptions) {
        return visualCtx
            ? refreshVirtualRowsCore(visualCtx, coreOptions)
            : refreshVirtualRowsCore(coreOptions);
    }

    TraceActionEvents.hideCollapsedLabelLens();
    if (!options.skipSearchStateRefresh) {
        refreshDirtySearchStateForCurrentLayout();
    }
    if (!virtualRuntime().virtualizerReady || !hasDOM()) {
        syncFullscreenBackgroundInert();
        return;
    }

    var traceEl = document.querySelector('.trace');
    var desiredScrollLeft = desiredVirtualRefreshScrollLeft(traceEl, options);
    var refreshOptions = { desiredScrollLeft: desiredScrollLeft };
    if (virtualRuntime().traceVirtualLayoutDirty) {
        virtualRuntime().traceVirtualLayoutDirty = false;
        if (shouldVirtualizeStageShellsForCurrentTrace()) {
            if (runCore({
                    desiredScrollLeft: desiredScrollLeft,
                    forceRebuild: true
            }) === false) return;
            syncFullscreenBackgroundInert();
            return;
        }
        var dirtyRefreshCommitted = true;
        withTraceMeasuredWidthSuppressed(function() {
            dirtyRefreshCommitted = runCore({
                desiredScrollLeft: desiredScrollLeft,
                deferCanvasWidth: true,
                forceRebuild: true
            }) !== false;
        });
        clearTraceMeasuredWidthCache();
        if (!dirtyRefreshCommitted) return;
    }

    if (runCore(refreshOptions) === false) return;
    if (virtualRuntime().virtualRenderFrame !== null) {
        cancelRenderFrameWork(virtualRuntime().virtualRenderFrame);
        virtualRuntime().virtualRenderFrame = null;
    }
    syncFullscreenBackgroundInert();
}

function scheduleVirtualRowsRefresh() {
    if (!virtualRuntime().virtualizerReady || !hasDOM()) return;
    if (virtualRuntime().virtualRenderFrame !== null) return;

    virtualRuntime().virtualRenderFrame = scheduleRenderDomWork(
        'virtual-rows-refresh',
        function runScheduledVirtualRowsRefresh(visualCtx) {
            virtualRuntime().virtualRenderFrame = null;
            refreshVirtualRows({}, visualCtx);
        },
        {
            epochScopes: ['trace'],
            label: 'virtual-rows-refresh',
            surfaces: ['virtual-rows', 'trace-canvas', 'trace-scrollbar', 'fullscreen-shell'],
            withVisualContext: true,
            onDiscard: function() {
                virtualRuntime().virtualRenderFrame = null;
            }
        }
    );
}

function refreshVirtualRowsNow(options) {
    options = options || {};
    if (virtualRuntime().virtualRenderFrame !== null) {
        cancelRenderFrameWork(virtualRuntime().virtualRenderFrame);
        virtualRuntime().virtualRenderFrame = null;
    }

    if (!options.skipSearchStateRefresh) {
        refreshDirtySearchStateForCurrentLayout();
    }
    refreshVirtualRows(options);
}

function onTraceVirtualScroll() {
    TraceActionEvents.hideCollapsedLabelLens();
    recordTraceViewportInteraction();
    var traceEl = document.querySelector('.trace');
    var suppressRefresh = consumeSuppressedVirtualScrollRefresh(traceEl) ||
        virtualScrollWithinRefreshSlack(traceEl);
    updateVirtualRange();
    updateTraceScrollbar(true);
    if (!suppressRefresh) scheduleVirtualRowsRefresh();
    scheduleVisibleRuleRenderScan();
    updateTraceAnchorLineNow();
    syncDiffMoveArrowsForTraceScroll(traceEl);
    scheduleDiffMoveArrows();
}

function onTraceVirtualResize() {
    TraceActionEvents.hideCollapsedLabelLens();
    enforceRuleWidthBounds();
    updateAllLayoutWidths();
    updateVirtualRange();
    invalidateTraceScrollbarGeometry();
    refreshTraceScrollbarGeometry(false);
    scheduleVirtualRowsRefresh();
    scheduleVisibleRuleRenderScan();
    scheduleTraceAnchorLineUpdate();
}

function initRuleVirtualization() {
    if (!hasDOM()) return;

    var traceEl = document.querySelector('.trace');
    if (!traceEl) return;

    virtualRuntime().virtualizerReady = true;
    updateVirtualRange();
    refreshVirtualRowsNow();
    scheduleTraceAnchorLineUpdate();

    if (virtualRuntime().traceVirtualScrollTarget !== traceEl) {
        if (virtualRuntime().traceVirtualScrollTarget && virtualRuntime().traceVirtualScrollTarget.removeEventListener) {
            virtualRuntime().traceVirtualScrollTarget.removeEventListener('scroll', onTraceVirtualScroll);
        }
        traceEl.addEventListener('scroll', onTraceVirtualScroll, { passive: true });
        virtualRuntime().traceVirtualScrollTarget = traceEl;
    }
    if (!virtualRuntime().traceVirtualResizeListenerReady && typeof window !== 'undefined' && window.addEventListener) {
        window.addEventListener('resize', onTraceVirtualResize);
        virtualRuntime().traceVirtualResizeListenerReady = true;
    }
}
