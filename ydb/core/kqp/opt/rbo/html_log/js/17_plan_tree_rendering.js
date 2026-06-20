/* ══════════════════════════════════════════════════════════════
   Plan tree rendering (normal mode, with compact connector runs)
   ══════════════════════════════════════════════════════════════

   Connector runs encode all depth columns in one SVG path per row.
   This keeps connector drawing visual-only while avoiding one DOM node
   per depth cell.
   ══════════════════════════════════════════════════════════════ */

var TREE_META_KEY_ALIGN_CAP_CH = 18;
var TREE_NODE_ROW_HEIGHT = 22;
var TREE_META_ROW_HEIGHT = 17;
var VIRTUAL_TREE_OVERSCAN_PX = 720;
var TREE_LABEL_CHAR_PX = 7.4;
var TREE_CONNECTOR_PX = 20;
var TREE_CONNECTOR_CELL_PX = 20;
var TREE_CONNECTOR_LINE_X = 8;
var nextTreeMaterializerInstanceId = 1;
var ruleTreeSurface = createVisualSurface({
    name: 'rule-tree',
    epochScopes: ['trace', 'render'],
    domGenerations: ['virtualRows'],
    resolveTarget: function(ref, options) {
        if (options && options.target) return options.target;
        if (options && options.state && options.state.ruleKey && hasDOM() && document.getElementById) {
            var root = document.getElementById('treeroot-' + options.state.ruleKey);
            if (root && root.querySelector) return root.querySelector('.tree-virtual-rows');
        }
        var renderKey = options && options.renderKey || '';
        if (!renderKey && ref) renderKey = ruleKey(ref.si, ref.gi, ref.ri);
        if (!renderKey || !hasDOM() || !document.getElementById) return null;
        return document.getElementById('tree-' + renderKey);
    }
});

function treePinnedHeaderHtml(ruleKey, showPinned, query, scope, minWidth) {
    var headerHtml = '';
    if (pinnedColumnCount() > 0 && showPinned) {
        var width = Math.max(1, Number(minWidth) || 0);
        headerHtml += '<div class="tree-pinned-header visible" id="pinhdr-' + ruleKey + '" ' +
            'style="min-width:' + width + 'px">';
        for (var col = 0; col < pinnedColumnCount(); col++) {
            var label = tracePinnedColumnName(col);
            headerHtml += pinnedColumnHeaderCellHtml(
                col,
                htmlEscape(label),
                {
                    labelTitle: label,
                    unpinTitle: 'Unpin column'
                }
            );
        }
        headerHtml += '<span style="width:16px;flex-shrink:0;display:inline-block"></span>';
        headerHtml += '<span class="tree-plan-label">Plan</span>';
        headerHtml += '</div>';
    }
    return headerHtml;
}

function pinnedColumnCellAttrs(col) {
    var keys = activeNodeColumnKeys();
    var key = keys[col] || '';
    return ' data-node-column-index="' + col + '"' +
           ' data-node-column-key="' + htmlEscape(key) + '"' +
           ' style="--pinned-column-width:' + nodeColumnWidthForKey(key) + 'px"';
}

function pinnedColumnHeaderCellHtml(col, labelHtml, options) {
    options = options || {};
    var keys = activeNodeColumnKeys();
    var key = keys[col] || '';
    var hasUnpin = !!options.unpinTitle;
    var labelTitle = options.labelTitle === undefined
        ? ''
        : ' title="' + htmlEscape(options.labelTitle) + '"';
    var html = '<span class="pinned-header-cell' + (hasUnpin ? ' has-unpin' : '') + '"' +
               pinnedColumnCellAttrs(col) +
               ' data-node-column-reorder="true" draggable="false">' +
               '<span class="pinned-header-label"' + labelTitle + '>' + labelHtml + '</span>';
    if (hasUnpin) {
        html += '<button type="button" class="pinned-header-unpin" ' +
                'data-trace-action="unpin-node-column" ' +
                'data-node-column-key="' + htmlEscape(key) + '" ' +
                'title="' + options.unpinTitle + '" ' +
                'aria-label="' + options.unpinTitle + '">' +
                traceIconSvg('drawing-pin') + '</button>';
    }
    html += '<span class="pinned-header-resize" role="separator" aria-orientation="vertical" ' +
            'data-node-column-index="' + col + '" ' +
            'data-node-column-key="' + htmlEscape(key) + '" ' +
            'title="Drag to resize column" aria-label="Resize column"></span>' +
            '</span>';
    return html;
}

function treeRuleRefFromRuleKey(ruleKeyValue) {
    var parts = String(ruleKeyValue || '').split('-');
    if (parts.length < 3) return {};
    var offset = parts.length - 3;
    var si = Number(parts[offset]);
    var gi = Number(parts[offset + 1]);
    var ri = Number(parts[offset + 2]);
    if (!Number.isFinite(si) || !Number.isFinite(gi) || !Number.isFinite(ri)) return {};
    return { si: si, gi: gi, ri: ri };
}

function treeSearchPathFromPid(pid) {
    var path = treePidPath(pid);
    if (!path || !path.length) return null;
    return path.slice(1).join('.');
}

function treeSearchContext(ruleKeyValue, pid, extra) {
    var ref = treeRuleRefFromRuleKey(ruleKeyValue) || {};
    var context = {
        type: 'rule',
        si: ref.si,
        gi: ref.gi,
        ri: ref.ri
    };
    if (pid !== undefined && pid !== null) context.path = treeSearchPathFromPid(pid);
    return searchContextWith(context, extra || {}) || context;
}

function renderTree(container, tree, ruleKey, showFields, showPinned, query, scope, visualCtx) {
    container.classList.remove('render-pending');
    renderVirtualTree(container, tree, ruleKey, showFields, showPinned, query, scope, visualCtx);
}

function renderTextTile(container, text, query, scope, ruleKeyValue) {
    container.classList.remove('render-pending');
    container.innerHTML = '<pre class="rule-text-content">' +
        treeHighlight(text, query, scope, 'label', treeSearchContext(ruleKeyValue, null)) +
        '</pre>';
}

function treeChildren(node) {
    return node && Array.isArray(node.c) ? node.c : [];
}

function treeFieldRows(node) {
    return traceNodeFieldRows(node);
}

function treeFieldRowKey(row) {
    return TraceStore.fieldRowKey(row);
}

function treeFieldRowValue(row) {
    return TraceStore.fieldRowValue(row);
}

function treeFieldRowFieldKey(row) {
    return TraceStore.fieldRowFieldKey(row);
}

function treeFieldRowMetaIndex(row, fallback) {
    var index = row && row.metaIndex !== undefined ? Number(row.metaIndex) : Number(fallback);
    if (!Number.isFinite(index)) return Math.max(0, Math.floor(Number(fallback) || 0));
    return Math.max(0, Math.floor(index));
}

function treeFieldDetailsActionAttrs(nodeId, metaIndex) {
    return treeActionAttr('open-metadata-details', nodeId) +
        ' data-meta-index="' + htmlEscape(String(metaIndex)) + '"';
}

function treeConnectorPath(prefixes, rowHeight) {
    prefixes = prefixes || [];
    var mid = rowHeight / 2;
    var d = [];
    for (var i = 0; i < prefixes.length; i++) {
        var kind = prefixes[i];
        var x = i * TREE_CONNECTOR_CELL_PX + TREE_CONNECTOR_LINE_X;
        var right = (i + 1) * TREE_CONNECTOR_CELL_PX;
        if (kind === 'pipe') {
            d.push('M' + x + ' 0V' + rowHeight);
        } else if (kind === 'branch') {
            d.push('M' + x + ' 0V' + rowHeight + 'M' + x + ' ' + mid + 'H' + right);
        } else if (kind === 'elbow') {
            d.push('M' + x + ' 0V' + mid + 'M' + x + ' ' + mid + 'H' + right);
        }
    }
    return d.join('');
}

function renderTreeConnectorSvg(prefixes, rowHeight, extraClass, attrs) {
    prefixes = prefixes || [];
    if (!prefixes.length) return '';
    var width = prefixes.length * TREE_CONNECTOR_CELL_PX;
    var path = treeConnectorPath(prefixes, rowHeight);
    if (!path) return '<span class="tree-connector-spacer" style="width:' + width + 'px"></span>';
    return '<svg class="tree-connector-run' + (extraClass ? ' ' + extraClass : '') +
        '" width="' + width + '" height="' + rowHeight +
        '" viewBox="0 0 ' + width + ' ' + rowHeight +
        '" aria-hidden="true" focusable="false"' + (attrs || '') + '>' +
        '<path class="tree-connector-path" d="' + path + '"></path></svg>';
}

function renderTreeNodeConnectors(parentPrefix, branchType) {
    var prefixes = (parentPrefix || []).slice();
    if (branchType) prefixes.push(branchType);
    if (!prefixes.length) return '';
    var lineX = (prefixes.length - 1) * TREE_CONNECTOR_CELL_PX + TREE_CONNECTOR_LINE_X;
    return renderTreeConnectorSvg(
        prefixes,
        TREE_NODE_ROW_HEIGHT,
        'tree-node-connectors',
        ' data-tree-connector-branch="' + htmlEscape(branchType || '') +
            '" data-tree-connector-line-x="' + lineX + '"'
    );
}

function renderTreeMetaConnectors(ancestorPrefixes, hasChildren, childrenCollapsed) {
    ancestorPrefixes = ancestorPrefixes || [];
    var drawPrefixes = ancestorPrefixes.slice();
    var hasChildContinuation = !!hasChildren && !childrenCollapsed;
    if (hasChildContinuation) drawPrefixes.push('pipe');
    if (!drawPrefixes.length) return '';

    var layoutWidth = ancestorPrefixes.length * TREE_CONNECTOR_CELL_PX;
    var drawWidth = Math.max(drawPrefixes.length * TREE_CONNECTOR_CELL_PX, TREE_CONNECTOR_CELL_PX);
    var path = treeConnectorPath(drawPrefixes, TREE_META_ROW_HEIGHT);
    return '<span class="tree-connector-meta-run" style="width:' + layoutWidth + 'px">' +
        '<svg class="tree-connector-run tree-meta-connectors" width="' + drawWidth +
        '" height="' + TREE_META_ROW_HEIGHT + '" viewBox="0 0 ' + drawWidth + ' ' +
        TREE_META_ROW_HEIGHT + '" aria-hidden="true" focusable="false">' +
        '<path class="tree-connector-path" d="' + path + '"></path></svg></span>';
}


function allocateTreeMaterializerInstanceId() {
    return 'tree-' + (nextTreeMaterializerInstanceId++);
}

function ensureTreeMaterializerStateIdentity(state) {
    if (!state) return null;
    if (!state.treeInstanceId) state.treeInstanceId = allocateTreeMaterializerInstanceId();
    if (!Number.isFinite(Number(state.treeModelVersion))) state.treeModelVersion = 0;
    if (!Number.isFinite(Number(state.treeProjectionVersion))) state.treeProjectionVersion = 0;
    return state.treeInstanceId;
}

function bumpTreeMaterializerModelVersion(state) {
    ensureTreeMaterializerStateIdentity(state);
    state.treeModelVersion = Math.max(0, Number(state.treeModelVersion) || 0) + 1;
    state.treeProjectionVersion = 0;
    return state.treeModelVersion;
}

function bumpTreeMaterializerProjectionVersion(state) {
    ensureTreeMaterializerStateIdentity(state);
    state.treeProjectionVersion = Math.max(0, Number(state.treeProjectionVersion) || 0) + 1;
    return state.treeProjectionVersion;
}

function treeMaterializerIdentityForState(state) {
    ensureTreeMaterializerStateIdentity(state);
    return state ? {
        treeInstanceId: state.treeInstanceId || '',
        treeModelVersion: Number(state.treeModelVersion) || 0,
        treeProjectionVersion: Number(state.treeProjectionVersion) || 0,
        ruleKey: state.ruleKey || ''
    } : null;
}

function clearVirtualTreeContainerState(container) {
    if (!container) return;
    var state = container.__traceVirtualTreeState;
    var materializer = container.__traceTreeMaterializer || state && state.materializer || null;
    if (state && container.isConnected !== false) saveVirtualTreeAnchor(state);
    if (state && state.resizeObserver) {
        try { state.resizeObserver.disconnect(); } catch (err) {}
    }
    unregisterTreeMaterializer(state, materializer);
    container.__traceVirtualTreeState = null;
    container.__traceTreeMaterializer = null;
    container.__traceTreeMaterializerState = null;
    container.__traceTreeInstanceId = null;
    container.classList.remove('large-tree-container');
}

function normalizeTreeRuleKey(ruleKey) {
    ruleKey = String(ruleKey || '');
    return ruleKey.indexOf('fs-') === 0 ? ruleKey.substring(3) : ruleKey;
}

function treeSessionKey(ruleKey) {
    var traceIndex = 0;
    try {
        traceIndex = Math.max(0, Number(traceRuntime().activeTraceIndex) || 0);
    } catch (err) {}
    return traceIndex + ':' + normalizeTreeRuleKey(ruleKey);
}

function treeSessionForRule(ruleKey) {
    var trace = null;
    try { trace = traceRuntime(); } catch (err) {}
    if (!trace) {
        return {
            collapsed: {},
            fieldRowsOpen: {},
            fieldRowsClosed: {},
            anchor: null,
            scrollLeft: 0
        };
    }

    if (!trace.treeSessions) trace.treeSessions = {};
    var key = treeSessionKey(ruleKey);
    var session = trace.treeSessions[key];
    if (!session) {
        session = trace.treeSessions[key] = {
            collapsed: {},
            fieldRowsOpen: {},
            fieldRowsClosed: {},
            anchor: null,
            scrollLeft: 0
        };
    }
    if (!session.collapsed) session.collapsed = {};
    if (!session.fieldRowsOpen) session.fieldRowsOpen = {};
    if (!session.fieldRowsClosed) session.fieldRowsClosed = {};
    return session;
}

function existingTreeSessionForRule(ruleKey) {
    var trace = null;
    try { trace = traceRuntime(); } catch (err) {}
    if (!trace || !trace.treeSessions) return null;
    return trace.treeSessions[treeSessionKey(ruleKey)] || null;
}

function clearObjectKeys(obj) {
    var changed = false;
    if (!obj) return changed;
    for (var key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key)) {
            delete obj[key];
            changed = true;
        }
    }
    return changed;
}

function clearTreeSessionFieldRowOverrides(session) {
    if (!session) return false;
    if (!session.fieldRowsOpen) session.fieldRowsOpen = {};
    if (!session.fieldRowsClosed) session.fieldRowsClosed = {};
    return clearObjectKeys(session.fieldRowsOpen) || clearObjectKeys(session.fieldRowsClosed);
}

function clearRuleFieldRowSessionOverrides(si, gi, ri) {
    var trace = null;
    try { trace = traceRuntime(); } catch (err) {}
    if (!trace || !trace.treeSessions) return false;

    var baseKey = treeSessionKey(ruleKey(si, gi, ri));
    var changed = false;
    for (var key in trace.treeSessions) {
        if (!Object.prototype.hasOwnProperty.call(trace.treeSessions, key)) continue;
        if (key !== baseKey && key.indexOf(baseKey + '-') !== 0) continue;
        changed = clearTreeSessionFieldRowOverrides(trace.treeSessions[key]) || changed;
    }
    return changed;
}

function treeMaterializerRegistry() {
    var trace = null;
    try { trace = traceRuntime(); } catch (err) {}
    if (!trace) return {};
    if (!trace.treeMaterializers) trace.treeMaterializers = {};
    return trace.treeMaterializers;
}

function treeMaterializerRegistryKey(state) {
    var key = state && state.ruleKey !== undefined ? String(state.ruleKey) : '';
    return key || null;
}

function unregisterTreeMaterializer(state, materializer) {
    var key = treeMaterializerRegistryKey(state);
    if (!key) return;
    var registry = treeMaterializerRegistry();
    if (registry[key] === materializer || !materializer) delete registry[key];
}

function registerTreeMaterializer(state, materializer) {
    var key = treeMaterializerRegistryKey(state);
    if (!key || !materializer) return;
    treeMaterializerRegistry()[key] = materializer;
}

function activeTreeMaterializers() {
    var registry = treeMaterializerRegistry();
    var materializers = [];
    for (var key in registry) {
        if (!Object.prototype.hasOwnProperty.call(registry, key)) continue;
        var materializer = registry[key];
        var state = materializer && materializer.state;
        var container = state && state.container;
        if (!state || !container || container.isConnected === false) {
            delete registry[key];
            continue;
        }
        materializers.push(materializer);
    }
    return materializers;
}

function normalTreeSessionForNodeId(nodeId) {
    return treeSessionForNodeId(nodeId);
}

function normalTreeNodeCollapsed(session, pid) {
    return treeRowModelNodeCollapsed(session, pid);
}

function normalTreeNodeFieldRowsVisible(session, pid, showFields) {
    if (!session) return !!showFields;
    return treeRowModelFieldRowsVisible(session, pid, showFields);
}

function shouldInvalidatePeerRuleRenderForTreeNode(nodeId) {
    return String(nodeId || '').indexOf('tn-fs-') === 0;
}

function setNormalTreeNodeCollapsed(nodeId, collapsed) {
    var parsed = normalTreeSessionForNodeId(nodeId);
    if (!parsed) return false;
    setTreeSessionNodeCollapsed(parsed.session, parsed.pid, collapsed);
    if (parsed.ref && shouldInvalidatePeerRuleRenderForTreeNode(nodeId)) {
        clearRuleRenderState(parsed.ref.si, parsed.ref.gi, parsed.ref.ri);
    }
    return true;
}

function setNormalTreeFieldRowsCollapsed(nodeId, collapsed) {
    var parsed = normalTreeSessionForNodeId(nodeId);
    if (!parsed) return false;
    var showFields = parsed.ref
        ? effectiveRuleFeature(parsed.ref.si, parsed.ref.gi, parsed.ref.ri, 'fields')
        : !!(parsed.session && parsed.session.showFields);
    setTreeSessionFieldRowsCollapsed(parsed.session, parsed.pid, showFields, collapsed);
    if (parsed.ref && shouldInvalidatePeerRuleRenderForTreeNode(nodeId)) {
        clearRuleRenderState(parsed.ref.si, parsed.ref.gi, parsed.ref.ri);
    }
    return true;
}

function rerenderNormalTreeForNodeId(nodeId) {
    var parsed = normalTreeSessionForNodeId(nodeId);
    if (!parsed || !parsed.ref) return false;

    var query = typeof currentSearchQuery === 'function' ? currentSearchQuery() : '';
    var scope = typeof currentSearchScope === 'function' ? currentSearchScope() : '';
    if (isFullscreenRule(parsed.ref.si, parsed.ref.gi, parsed.ref.ri)) {
        renderFullscreenRuleImmediately(parsed.ref.si, parsed.ref.gi, parsed.ref.ri, query, scope);
    } else {
        rerenderRuleTree(parsed.ref.si, parsed.ref.gi, parsed.ref.ri, query, true, scope);
    }
    return true;
}

function renderVirtualTree(container, tree, ruleKey, showFields, showPinned, query, scope, visualCtx, renderOptions) {
    var previousState = container.__traceVirtualTreeState;
    if (previousState) saveTreeMaterializerPaneAnchor(previousState);

    var searchExpandActive = query &&
        typeof activeSearchExpandOverlay === 'function' &&
        activeSearchExpandOverlay();
    var session = searchExpandActive
        ? { collapsed: {}, fieldRowsOpen: {}, fieldRowsClosed: {}, anchor: null, scrollLeft: 0 }
        : treeSessionForRule(ruleKey);
    var state = container.__traceVirtualTreeState;
    if (!state || state.tree !== tree || state.ruleKey !== ruleKey) {
        if (state && state.resizeObserver) {
            try { state.resizeObserver.disconnect(); } catch (err) {}
        }
        state = {
            mode: 'virtual',
            tree: tree,
            ruleKey: ruleKey,
            treeInstanceId: allocateTreeMaterializerInstanceId(),
            treeModelVersion: 0,
            treeProjectionVersion: 0,
            collapsed: session.collapsed,
            fieldRowsOpen: session.fieldRowsOpen,
            fieldRowsClosed: session.fieldRowsClosed,
            rows: [],
            renderedStart: -1,
            renderedEnd: -1,
            renderFrame: null,
            resizeObserver: null
        };
        container.__traceVirtualTreeState = state;
    }

    state.container = container;
    state.mode = 'virtual';
    ensureTreeMaterializerStateIdentity(state);
    state.session = session;
    state.collapsed = session.collapsed;
    state.fieldRowsOpen = session.fieldRowsOpen;
    state.fieldRowsClosed = session.fieldRowsClosed;
    state.showFields = !!showFields;
    state.showPinned = !!showPinned;
    state.scope = scope || '';
    state.query = query || '';
    applyTreeRenderOptionsToState(state, renderOptions);
    buildVirtualTreeRows(state);
    var initialRowsHtml = initialVirtualTreeRowsHtml(state);

    container.classList.add('large-tree-container');
    container.innerHTML = treePinnedHeaderHtml(ruleKey, showPinned, state.query, state.scope, state.minWidth) +
        '<div class="tree-root large-tree-root' + (showPinned ? ' pinned-active' : '') +
        '" id="treeroot-' + ruleKey + '" data-large-tree-root="true" style="height:' +
        state.totalHeight + 'px;min-width:' + state.minWidth + 'px">' +
        '<div class="tree-virtual-rows">' + initialRowsHtml + '</div>' +
        '</div>';

    state.rootEl = document.getElementById('treeroot-' + ruleKey);
    state.rowsEl = state.rootEl ? state.rootEl.querySelector('.tree-virtual-rows') : null;
    if (state.rootEl) {
        state.rootEl.__traceVirtualTreeState = state;
        state.rootEl.__traceTreeInstanceId = state.treeInstanceId;
    }
    attachTreeMaterializer(container, state.rootEl, state, VirtualTreeMaterializer(state));
    ensureVirtualTreeScrollListener(container);
    observeVirtualTreeContainer(state);
    restoreTreeMaterializerPaneAnchor(state);
    renderVirtualTreeVisibleRows(visualCtx, state, true);
    saveTreeMaterializerPaneAnchor(state);
}

function initialVirtualTreeRowsHtml(state) {
    if (!state) return '';
    var container = state.container || {};
    var top = Math.max(0, Number(container.scrollTop) || 0);
    var height = Math.max(0, Number(container.clientHeight) || 0) || 640;
    var projection = buildTreeProjection(state, { top: top, height: height }, VIRTUAL_TREE_OVERSCAN_PX);
    var range = { start: projection.start, end: projection.end };
    state.renderedStart = range.start;
    state.renderedEnd = range.end;
    bumpTreeMaterializerProjectionVersion(state);
    return virtualTreeProjectionHtml(state, projection, virtualTreeDebugOverlayHtml(state, range));
}

function observeVirtualTreeContainer(state) {
    if (!state || !state.container || typeof ResizeObserver === 'undefined') return;
    if (state.resizeObserver) {
        try { state.resizeObserver.disconnect(); } catch (err) {}
    }
    state.resizeObserver = new ResizeObserver(function() {
        ensureTreeViewportRowsMounted(state, 'resize-observer');
    });
    state.resizeObserver.observe(state.container);
}

function ensureVirtualTreeScrollListener(container) {
    if (!container || container.__traceVirtualTreeScrollListener ||
            typeof container.addEventListener !== 'function') {
        return;
    }
    container.__traceVirtualTreeScrollListener = function() {
        var state = container.__traceVirtualTreeState || container.__traceTreeMaterializerState;
        if (state) {
            saveTreeMaterializerPaneAnchor(state);
            if (container.__traceVirtualTreeState === state) {
                ensureTreeViewportRowsMounted(container, 'scroll');
            }
        }
    };
    container.addEventListener('scroll', container.__traceVirtualTreeScrollListener, { passive: true });
}

function treePathId(path) {
    return path.join('-');
}

function treePidPath(pid) {
    pid = String(pid || '');
    if (!pid) return null;
    var parts = pid.split('-');
    var path = [];
    for (var i = 0; i < parts.length; i++) {
        var value = Number(parts[i]);
        if (!Number.isInteger(value) || value < 0) return null;
        path.push(value);
    }
    return path.length ? path : null;
}

function treeNodeId(ruleKey, pid) {
    return 'tn-' + ruleKey + '-' + pid;
}

function treeNodeCollapsed(state, pid) {
    return !!state.collapsed[pid];
}

function treeRowModelNodeCollapsed(session, pid) {
    return !!(session && session.collapsed && session.collapsed[pid]);
}

function setTreeSessionNodeCollapsed(session, pid, collapsed) {
    if (!session || !pid) return false;
    if (!session.collapsed) session.collapsed = {};
    if (collapsed) {
        session.collapsed[pid] = true;
    } else {
        delete session.collapsed[pid];
    }
    return true;
}

function treeRowModelFieldRowsVisible(session, pid, showFields) {
    if (showFields) return !(session && session.fieldRowsClosed && session.fieldRowsClosed[pid]);
    return !!(session && session.fieldRowsOpen && session.fieldRowsOpen[pid]);
}

function treeFieldRowsFullyExpanded(tree, session, showFields) {
    if (!tree || !tree.l) return null;

    var hasFieldRows = false;
    var stack = [{ node: tree, pid: '0' }];
    while (stack.length) {
        var item = stack.pop();
        var node = item.node;
        if (!node || !node.l) continue;

        if (treeFieldRows(node).length) {
            hasFieldRows = true;
            if (!treeRowModelFieldRowsVisible(session, item.pid, !!showFields)) return false;
        }

        var children = treeChildren(node);
        for (var i = children.length - 1; i >= 0; i--) {
            stack.push({
                node: children[i],
                pid: item.pid + '-' + i
            });
        }
    }

    return hasFieldRows ? true : false;
}

function ruleFieldRowsFullyExpanded(si, gi, ri) {
    var showFields = effectiveRuleFeature(si, gi, ri, 'fields');
    var store = currentTraceStore();
    var rawIdx = rawRuleIndex(si, gi, ri);
    var ruleHandleValue = TraceStore.ruleHandle(store, si, rawIdx);
    if (ruleHandleValue) {
        var payloadState = TraceStore.payloadState(store, 'trees', ruleHandleValue).state;
        if (payloadState !== TraceStore.PAYLOAD_STATES.RENDERED &&
                payloadState !== TraceStore.PAYLOAD_STATES.EMPTY) {
            return !!showFields;
        }
    }
    try {
        var tree = tracePlanTree(si, rawIdx);
        var expanded = treeFieldRowsFullyExpanded(tree, treeSessionForRule(ruleKey(si, gi, ri)), showFields);
        return expanded === null ? !!showFields : !!expanded;
    } catch (err) {
        return !!showFields;
    }
}

function ruleFeatureButtonActive(feature, si, gi, ri) {
    if (feature === 'fields') {
        return ruleFieldRowsFullyExpanded(si, gi, ri);
    }
    if (feature === 'pinned' && pinnedColumnCount() === 0) {
        return false;
    }
    return effectiveRuleFeature(si, gi, ri, feature);
}

function setTreeSessionFieldRowsCollapsed(session, pid, showFields, collapsed) {
    if (!session || !pid) return false;
    if (!session.fieldRowsOpen) session.fieldRowsOpen = {};
    if (!session.fieldRowsClosed) session.fieldRowsClosed = {};

    if (showFields) {
        if (collapsed) {
            session.fieldRowsClosed[pid] = true;
        } else {
            delete session.fieldRowsClosed[pid];
        }
    } else if (collapsed) {
        delete session.fieldRowsOpen[pid];
    } else {
        session.fieldRowsOpen[pid] = true;
    }
    return true;
}

function treeEstimateTextWidth(text) {
    return String(text == null ? '' : text).length * TREE_LABEL_CHAR_PX;
}

function treeRowModelEstimateNodeWidth(row, options) {
    options = options || {};
    var pinnedWidth = options.showPinned ? activeNodeColumnWidthTotal() : 0;
    var connectors = row.isRoot
        ? 16
        : row.parentPrefix.length * TREE_CONNECTOR_PX + TREE_CONNECTOR_PX + 16;
    return Math.ceil(pinnedWidth + connectors + 24 + treeEstimateTextWidth(row.node.l));
}

function treeRowModelEstimateFieldRowWidth(row, options) {
    options = options || {};
    var pinnedWidth = options.showPinned ? activeNodeColumnWidthTotal() : 0;
    var meta = row.meta;
    var key = treeFieldRowKey(meta);
    var val = treeFieldRowValue(meta);
    return Math.ceil(
        pinnedWidth +
        row.metaPrefix.length * TREE_CONNECTOR_PX +
        TREE_CONNECTOR_PX +
        18 +
        fieldRowKeyWidthCh(key, row.keyAlignCh) * TREE_LABEL_CHAR_PX +
        treeEstimateTextWidth(val) +
        48
    );
}

/*
 * Canonical tree row model invariants:
 * - every visible tree line is represented by one row object, regardless of
 *   whether the tree is fully rendered or virtually projected;
 * - rows are contiguous in display order: row.index === array index, row.top
 *   starts at zero, and each next top equals previous top + height;
 * - node and field rows use stable, unique row keys and signatures so DOM
 *   projection can reconcile without confusing stale rows for current rows;
 * - totalHeight is the exact visible row height, with a minimum of one pixel
 *   for empty defensive trees; minWidth is the estimated full logical width.
 */
function buildTreeRows(tree, session, options) {
    session = session || {};
    options = options || {};
    var rows = [];
    var top = 0;
    var maxWidth = 1;
    var stack = [{
        node: tree,
        pid: '0',
        path: [0],
        parentPrefix: [],
        childPrefix: [],
        isRoot: true,
        isLast: true
    }];

    while (stack.length) {
        var item = stack.pop();
        var node = item.node;
        if (!node || !node.l) continue;

        var children = treeChildren(node);
        var meta = treeFieldRows(node);
        var collapsed = treeRowModelNodeCollapsed(session, item.pid);
        var nodeRow = {
            kind: 'node',
            index: rows.length,
            node: node,
            pid: item.pid,
            path: item.path,
            parentPrefix: item.parentPrefix,
            childPrefix: item.childPrefix,
            isRoot: item.isRoot,
            isLast: item.isLast,
            hasChildren: children.length > 0,
            hasMeta: meta.length > 0,
            collapsed: collapsed,
            top: top,
            height: TREE_NODE_ROW_HEIGHT
        };
        rows.push(nodeRow);
        maxWidth = Math.max(maxWidth, treeRowModelEstimateNodeWidth(nodeRow, options));
        top += TREE_NODE_ROW_HEIGHT;

        if (meta.length && treeRowModelFieldRowsVisible(session, item.pid, !!options.showFields)) {
            var keyAlignCh = fieldRowsBlockKeyAlignCh(meta);
            for (var mi = 0; mi < meta.length; mi++) {
                var metaRow = {
                    kind: 'fields',
                    index: rows.length,
                    node: node,
                    pid: item.pid,
                    path: item.path,
                    meta: meta[mi],
                    metaIndex: treeFieldRowMetaIndex(meta[mi], mi),
                    metaPrefix: item.isRoot ? [] : item.childPrefix,
                    keyAlignCh: keyAlignCh,
                    hasChildren: children.length > 0,
                    collapsed: collapsed,
                    top: top,
                    height: TREE_META_ROW_HEIGHT
                };
                rows.push(metaRow);
                maxWidth = Math.max(maxWidth, treeRowModelEstimateFieldRowWidth(metaRow, options));
                top += TREE_META_ROW_HEIGHT;
            }
        }

        if (!children.length || collapsed) continue;
        for (var ci = children.length - 1; ci >= 0; ci--) {
            var isLast = ci === children.length - 1;
            var childPrefix = item.isRoot ? [] : item.childPrefix.slice();
            childPrefix.push(isLast ? 'space' : 'pipe');
            stack.push({
                node: children[ci],
                pid: item.pid + '-' + ci,
                path: item.path.concat(ci),
                parentPrefix: item.isRoot ? [] : item.childPrefix,
                childPrefix: childPrefix,
                isRoot: false,
                isLast: isLast
            });
        }
    }

    return {
        rows: rows,
        totalHeight: Math.max(1, top),
        minWidth: Math.max(1, maxWidth)
    };
}


function buildVirtualTreeRows(state) {
    var session = state && state.session ? state.session : state;
    var model = buildTreeRows(state.tree, session, {
        showFields: !!state.showFields,
        showPinned: !!state.showPinned
    });

    state.rows = model.rows;
    state.totalHeight = model.totalHeight;
    state.minWidth = model.minWidth;
    bumpTreeMaterializerModelVersion(state);
    state.renderedStart = -1;
    state.renderedEnd = -1;
}

function applyTreeRenderOptionsToState(state, renderOptions) {
    if (!state) return state;
    renderOptions = renderOptions || {};
    state.treeRenderKind = renderOptions.treeRenderKind || '';
    state.rowDecorations = renderOptions.rowDecorations || null;
    state.renderSignature = renderOptions.renderSignature || '';
    state.moveScope = renderOptions.moveScope || '';
    state.diffSide = renderOptions.diffSide || '';
    return state;
}

function attachTreeMaterializer(container, rootEl, state, materializer) {
    if (!container || !materializer) return materializer || null;
    ensureTreeMaterializerStateIdentity(state);
    materializer.treeInstanceId = state && state.treeInstanceId || '';
    if (state) state.materializer = materializer;
    container.__traceTreeMaterializer = materializer;
    container.__traceTreeMaterializerState = state || null;
    container.__traceTreeInstanceId = state && state.treeInstanceId || '';
    if (rootEl) {
        rootEl.__traceTreeMaterializer = materializer;
        rootEl.__traceTreeMaterializerState = state || null;
        rootEl.__traceTreeInstanceId = state && state.treeInstanceId || '';
    }
    registerTreeMaterializer(state, materializer);
    return materializer;
}

function treeMaterializerResult(status, details) {
    details = details || {};
    details.status = status;
    return details;
}

function treeMaterializerResolved(details) {
    return treeMaterializerResult('resolved', details);
}

function treeMaterializerPending(reason, details) {
    details = details || {};
    details.reason = reason || 'pending';
    return treeMaterializerResult('pending', details);
}

function treeMaterializerFailed(reason, details) {
    details = details || {};
    details.reason = reason || 'failed';
    return treeMaterializerResult('failed', details);
}

function normalizeTreeMaterializerRange(materializer, range) {
    var rows = materializer && materializer.state && materializer.state.rows || [];
    var start = Math.floor(Number(range && range.start));
    var end = Math.floor(Number(range && range.end));
    if (!Number.isFinite(start) || !Number.isFinite(end)) {
        return treeMaterializerFailed('invalid-range');
    }
    start = Math.max(0, Math.min(rows.length, start));
    end = Math.max(start, Math.min(rows.length, end));
    if (start === end) return treeMaterializerFailed('empty-range');
    return treeMaterializerResolved({ start: start, end: end, rows: rows });
}

function treeMaterializerRowRangeBounds(materializer, range) {
    var normalized = normalizeTreeMaterializerRange(materializer, range);
    if (normalized.status !== 'resolved') return normalized;

    var rows = normalized.rows;
    var first = rows[normalized.start];
    var last = rows[normalized.end - 1];
    if (!first || !last) return treeMaterializerFailed('row-not-found');

    return treeMaterializerResolved({
        start: normalized.start,
        end: normalized.end,
        top: first.top,
        bottom: last.top + last.height,
        height: Math.max(0, last.top + last.height - first.top)
    });
}

function createTreeViewport(materializer) {
    return {
        materializer: materializer,
        viewportRect: function() {
            return treeViewportRect(this);
        },
        contentRect: function() {
            return treeViewportContentRect(this);
        },
        scrollTop: function() {
            return treeViewportScrollTop(this);
        },
        scrollLeft: function() {
            return treeViewportScrollLeft(this);
        },
        setScroll: function(top, left, reason) {
            return treeViewportSetScroll(this, top, left, reason);
        },
        captureAnchor: function(anchorPolicy) {
            return treeViewportCaptureAnchor(this, anchorPolicy);
        },
        restoreAnchor: function(anchor) {
            return treeViewportRestoreAnchor(this, anchor);
        },
        canFitRange: function(range) {
            return treeViewportCanFitRange(this, range);
        },
        minimalScrollForRange: function(range, policy) {
            return treeViewportMinimalScrollForRange(this, range, policy);
        },
        maxScroll: function() {
            return treeViewportMaxScroll(this);
        }
    };
}

function treeMaterializerViewport(materializer) {
    if (!materializer) return null;
    if (!materializer._treeViewport) {
        materializer._treeViewport = createTreeViewport(materializer);
    }
    return materializer._treeViewport;
}

function treeViewportMaterializer(viewport) {
    return viewport && viewport.materializer || null;
}

function treeViewportState(viewport) {
    var materializer = treeViewportMaterializer(viewport);
    return materializer && materializer.state || null;
}

function treeViewportRootTop(viewport) {
    var state = treeViewportState(viewport);
    var rootEl = state && state.rootEl;
    return Number(rootEl && rootEl.offsetTop) || 0;
}

function treeViewportContainer(viewport) {
    var state = treeViewportState(viewport);
    return state && state.container || null;
}

function treeViewportHeight(viewport) {
    var container = treeViewportContainer(viewport);
    return Math.max(0, Number(container && container.clientHeight) || 0);
}

function treeViewportWidth(viewport) {
    var container = treeViewportContainer(viewport);
    return Math.max(0, Number(container && container.clientWidth) || 0);
}

function treeViewportContentWidth(viewport) {
    var state = treeViewportState(viewport);
    var container = treeViewportContainer(viewport);
    var rootEl = state && state.rootEl;
    return Math.max(
        treeViewportWidth(viewport),
        Number(state && state.minWidth) || 0,
        Number(rootEl && rootEl.scrollWidth) || 0,
        Number(container && container.scrollWidth) || 0
    );
}

function treeViewportPinnedHeaderHeight(viewport) {
    var state = treeViewportState(viewport);
    if (!state || !state.showPinned) return 0;
    var container = state.container || treeViewportContainer(viewport);
    if (!container || !container.querySelector) return 0;
    var header = container.querySelector('.tree-pinned-header.visible');
    if (!header) return 0;

    var height = Number(header.offsetHeight) || 0;
    if (height <= 0 && header.getBoundingClientRect) {
        var rect = header.getBoundingClientRect();
        height = Number(rect && rect.height) || 0;
    }
    return Math.max(0, height);
}

function treeViewportTopOcclusion(viewport) {
    return treeViewportPinnedHeaderHeight(viewport);
}

function treeViewportRawScrollTop(viewport) {
    var container = treeViewportContainer(viewport);
    return Math.max(0, Number(container && container.scrollTop) || 0);
}

function treeViewportScrollTop(viewport) {
    return treeViewportRawScrollTop(viewport) + treeViewportTopOcclusion(viewport);
}

function treeViewportScrollLeft(viewport) {
    var container = treeViewportContainer(viewport);
    return Math.max(0, Number(container && container.scrollLeft) || 0);
}

function treeViewportRect(viewport) {
    var topOcclusion = treeViewportTopOcclusion(viewport);
    var top = treeViewportScrollTop(viewport);
    var left = treeViewportScrollLeft(viewport);
    var height = Math.max(0, treeViewportHeight(viewport) - topOcclusion);
    var width = treeViewportWidth(viewport);
    return {
        top: top,
        bottom: top + height,
        left: left,
        right: left + width,
        height: height,
        width: width
    };
}

function treeViewportContentRect(viewport) {
    var state = treeViewportState(viewport);
    var top = treeViewportRootTop(viewport);
    var height = Math.max(0, Number(state && state.totalHeight) || 0);
    var width = treeViewportContentWidth(viewport);
    return {
        top: top,
        bottom: top + height,
        left: 0,
        right: width,
        height: height,
        width: width
    };
}

function treeViewportMaxScroll(viewport) {
    var viewportRect = treeViewportRect(viewport);
    var contentRect = treeViewportContentRect(viewport);
    return {
        top: Math.max(0, contentRect.bottom - viewportRect.height),
        left: Math.max(0, contentRect.width - viewportRect.width)
    };
}

function treeViewportRawMaxScrollTop(viewport, effectiveMaxTop) {
    var container = treeViewportContainer(viewport);
    var nativeMax = Number(container && container.scrollHeight) - Number(container && container.clientHeight);
    if (Number.isFinite(nativeMax) && nativeMax > 0) return Math.max(0, nativeMax);
    return Math.max(0, (Number(effectiveMaxTop) || 0) - treeViewportTopOcclusion(viewport));
}

function treeViewportSetScroll(viewport, top, left, reason) {
    var container = treeViewportContainer(viewport);
    if (!container) return treeMaterializerFailed('missing-container');
    var currentTop = treeViewportScrollTop(viewport);
    var currentLeft = treeViewportScrollLeft(viewport);
    var max = treeViewportMaxScroll(viewport);
    var nextTop = Number.isFinite(Number(top)) ? Number(top) : currentTop;
    var nextLeft = Number.isFinite(Number(left)) ? Number(left) : currentLeft;
    nextTop = clampTreeScrollValue(nextTop, max.top);
    nextLeft = clampTreeScrollValue(nextLeft, max.left);
    container.scrollTop = clampTreeScrollValue(nextTop - treeViewportTopOcclusion(viewport),
        treeViewportRawMaxScrollTop(viewport, max.top));
    container.scrollLeft = nextLeft;
    return treeMaterializerResolved({
        scrollTop: treeViewportScrollTop(viewport),
        scrollLeft: nextLeft,
        reason: reason || ''
    });
}

function treeViewportCaptureAnchor(viewport, anchorPolicy) {
    var rect = treeViewportRect(viewport);
    return treeMaterializerResolved({
        scrollTop: rect.top,
        scrollLeft: rect.left,
        policy: anchorPolicy || null
    });
}

function treeViewportRestoreAnchor(viewport, anchor) {
    if (!anchor) return treeMaterializerFailed('missing-anchor');
    return treeViewportSetScroll(viewport, anchor.scrollTop, anchor.scrollLeft, 'restore-anchor');
}

function treeViewportBoundsForRange(viewport, range) {
    if (range &&
        Number.isFinite(Number(range.top)) &&
        Number.isFinite(Number(range.bottom))) {
        return treeMaterializerResolved({
            start: range.start,
            end: range.end,
            top: Number(range.top),
            bottom: Number(range.bottom),
            height: Math.max(0, Number(range.bottom) - Number(range.top))
        });
    }
    return treeMaterializerRowRangeBounds(treeViewportMaterializer(viewport), range);
}

function treeViewportCanFitRange(viewport, range) {
    var bounds = treeViewportBoundsForRange(viewport, range);
    if (bounds.status !== 'resolved') return bounds;
    var rect = treeViewportRect(viewport);
    return treeMaterializerResolved({
        canFit: bounds.height <= rect.height,
        height: bounds.height,
        viewportHeight: rect.height
    });
}

function treeViewportMinimalScrollForRange(viewport, range, policy) {
    var bounds = treeViewportBoundsForRange(viewport, range);
    if (bounds.status !== 'resolved') return bounds;
    var container = treeViewportContainer(viewport);
    if (!container) return treeMaterializerFailed('missing-container');

    policy = policy || {};
    var viewportRect = treeViewportRect(viewport);
    var contentRect = treeViewportContentRect(viewport);
    var rootRelativeTop = Math.max(0, viewportRect.top - contentRect.top);
    var current = viewportRect.top;
    var desired = current;
    if (policy.block === 'start' || bounds.height > viewportRect.height) {
        desired = contentRect.top + bounds.top;
    } else if (bounds.top < rootRelativeTop) {
        desired = contentRect.top + bounds.top;
    } else if (bounds.bottom > rootRelativeTop + viewportRect.height) {
        desired = contentRect.top + bounds.bottom - viewportRect.height;
    }
    var max = treeViewportMaxScroll(viewport);
    return treeMaterializerResolved({
        scrollTop: clampTreeScrollValue(desired, max.top),
        scrollLeft: viewportRect.left
    });
}

function treeMaterializerRootTop(materializer) {
    var viewport = treeMaterializerViewport(materializer);
    return viewport ? treeViewportRootTop(viewport) : 0;
}

function treeMaterializerCurrentContentViewport(materializer) {
    var viewport = treeMaterializerViewport(materializer);
    var viewportRect = viewport ? treeViewportRect(viewport) : { top: 0, height: 0 };
    var contentRect = viewport ? treeViewportContentRect(viewport) : { top: 0 };
    var rootTop = contentRect.top;
    var top = Math.max(0, viewportRect.top - rootTop);
    var height = viewportRect.height;
    return { top: top, bottom: top + height, height: height, rootTop: rootTop };
}

function treeMaterializerMinimalScrollTopForRange(materializer, bounds, policy) {
    var viewport = treeMaterializerViewport(materializer);
    if (!viewport) return treeMaterializerFailed('missing-viewport');
    return treeViewportMinimalScrollForRange(viewport, bounds, policy);
}

function createTreeMaterializer(kind, state) {
    ensureTreeMaterializerStateIdentity(state);
    var materializer = {
        kind: kind,
        state: state,
        treeInstanceId: state && state.treeInstanceId || '',
        mode: function() {
            return treeMaterializerMode(materializer);
        },
        mountedRange: function() {
            return treeMaterializerMountedRange(materializer);
        },
        viewport: function() {
            return treeMaterializerViewport(materializer);
        },
        visibleRowRange: function(overscanPx) {
            return treeMaterializerVisibleRowRange(materializer, overscanPx);
        },
        viewportRange: function() {
            return treeMaterializerViewportRange(materializer);
        },
        viewportRenderState: function(container) {
            return treeMaterializerViewportRenderStateForMaterializer(materializer, container);
        },
        ensureRowsMounted: function(range) {
            return treeMaterializerEnsureRowsMounted(materializer, range);
        },
        scrollRowRangeIntoView: function(range, policy) {
            return treeMaterializerScrollRowRangeIntoView(materializer, range, policy);
        },
        canRevealNodePath: function(target) {
            return treeMaterializerCanRevealNodePath(materializer, target);
        },
        revealNodePath: function(target) {
            return treeMaterializerRevealNodePath(materializer, target);
        },
        measureRowRange: function(range) {
            return treeMaterializerMeasureRowRange(materializer, range);
        },
        measureVisibleLane: function(range) {
            return treeMaterializerMeasureVisibleLane(materializer, range);
        },
        syncAfterScroll: function() {
            return treeMaterializerSyncAfterScroll(materializer);
        },
        syncAfterResize: function() {
            return treeMaterializerSyncAfterResize(materializer);
        }
    };
    return materializer;
}

function VirtualTreeMaterializer(state) {
    return createTreeMaterializer('virtual', state);
}

function treeMaterializerMode(materializer) {
    return materializer && materializer.kind || 'unknown';
}

function treeMaterializerForState(state) {
    if (!state) return null;
    state.mode = 'virtual';
    ensureTreeMaterializerStateIdentity(state);
    if (treeMaterializerMatchesState(state.materializer, state)) return state.materializer;
    state.materializer = null;

    var materializer = null;
    if (state.container && state.container.__traceTreeMaterializerState === state) {
        materializer = state.container.__traceTreeMaterializer || null;
    }
    if (!materializer && state.rootEl && state.rootEl.__traceTreeMaterializerState === state) {
        materializer = state.rootEl.__traceTreeMaterializer || null;
    }
    if (!materializer) {
        materializer = VirtualTreeMaterializer(state);
    }
    state.materializer = materializer;
    return materializer;
}

function treeMaterializerMatchesState(materializer, state) {
    if (!materializer || !state) return false;
    ensureTreeMaterializerStateIdentity(state);
    if (materializer.state !== state) return false;
    if (materializer.treeInstanceId && materializer.treeInstanceId !== state.treeInstanceId) return false;
    materializer.treeInstanceId = state.treeInstanceId;
    return true;
}

function treeMaterializerHostMatchesState(host, state) {
    if (!host || !state) return false;
    ensureTreeMaterializerStateIdentity(state);
    var hostState = host.__traceVirtualTreeState || host.__traceTreeMaterializerState || null;
    if (hostState && hostState !== state) return false;
    var hostInstanceId = host.__traceTreeInstanceId || '';
    if (hostInstanceId && hostInstanceId !== state.treeInstanceId) return false;
    return true;
}

function treeMaterializerMountedRange(materializer) {
    var state = materializer && materializer.state;
    var rows = state && state.rows || [];
    var start = Math.max(0, Math.min(rows.length, Number(state && state.renderedStart) || 0));
    var end = Math.max(start, Math.min(rows.length, Number(state && state.renderedEnd) || 0));
    return { start: start, end: end };
}

function treeMaterializerVisibleRowRange(materializer, overscanPx) {
    var state = materializer && materializer.state;
    var rows = state && state.rows || [];
    if (!state || !state.container || !state.rootEl) return { start: 0, end: -1 };

    var viewport = treeMaterializerViewport(materializer);
    var viewportRect = viewport ? treeViewportRect(viewport) : { top: 0, height: 0 };
    var contentRect = viewport ? treeViewportContentRect(viewport) : { top: 0 };
    var top = Math.max(0, viewportRect.top - contentRect.top - (Number(overscanPx) || 0));
    var height = viewportRect.height || 640;
    var bottom = Math.max(top, viewportRect.top - contentRect.top + height + (Number(overscanPx) || 0));
    var start = 0;
    while (start < rows.length && rows[start].top + rows[start].height < top) start++;
    var end = start;
    while (end < rows.length && rows[end].top <= bottom) end++;
    return { start: start, end: end };
}

function treeMaterializerViewportRange(materializer) {
    var state = materializer && materializer.state;
    if (!state || !state.container || !state.rootEl) return { top: 0, bottom: 0 };
    var current = treeMaterializerCurrentContentViewport(materializer);
    return { top: current.top, bottom: current.bottom };
}

function treeMaterializerProjectionForCurrentViewport(materializer) {
    var state = materializer && materializer.state;
    if (!state) return null;
    var viewportRange = treeMaterializerViewportRange(materializer);
    return buildTreeProjection(state, {
        top: viewportRange.top,
        bottom: viewportRange.bottom
    }, VIRTUAL_TREE_OVERSCAN_PX);
}

function treeMaterializerMountedRowsIntersectViewport(materializer, container) {
    var state = materializer && materializer.state;
    container = container || state && state.container;
    if (!container || !state || !state.rows || !state.rows.length) return true;
    if (!container.querySelectorAll) return true;
    if ((Number(container.clientHeight) || 0) <= 0) return true;

    var viewport = treeMaterializerViewportRange(materializer);
    if (viewport.bottom <= viewport.top) return true;

    var nodes = container.querySelectorAll('.tree-virtual-row');
    if (!nodes.length) return false;

    for (var i = 0; i < nodes.length; i++) {
        var rowNode = nodes[i];
        var rowIndex = rowNode.getAttribute
            ? Math.floor(Number(rowNode.getAttribute('data-tree-row')))
            : NaN;
        var modelRow = Number.isFinite(rowIndex) ? state.rows[rowIndex] : null;
        var rowTop = modelRow
            ? modelRow.top
            : parseFloat(rowNode.style && rowNode.style.top);
        var rowHeight = modelRow
            ? modelRow.height
            : parseFloat(rowNode.style && rowNode.style.height);
        if (!Number.isFinite(rowTop) || !Number.isFinite(rowHeight)) continue;
        if (rowTop + rowHeight > viewport.top && rowTop < viewport.bottom) return true;
    }

    return false;
}

function treeMaterializerViewportRenderStateForMaterializer(materializer, container) {
    if (!materializer) {
        return { state: 'not-virtual-tree', valid: true, needsRender: false };
    }
    var state = materializer.state;
    container = container || state && state.container;
    if (!state || !state.rootEl || !state.rowsEl) {
        return { state: 'virtual-tree-missing-root', valid: false, needsRender: true };
    }
    if (!state.rows || !state.rows.length) {
        return { state: 'rendered', valid: true, needsRender: false };
    }

    var projection = treeMaterializerProjectionForCurrentViewport(materializer);
    var range = projection || treeMaterializerVisibleRowRange(materializer, VIRTUAL_TREE_OVERSCAN_PX);
    if (range.end > range.start &&
            (state.renderedStart !== range.start || state.renderedEnd !== range.end)) {
        return {
            state: 'virtual-tree-stale-window',
            valid: false,
            needsRender: true,
            expectedStart: range.start,
            expectedEnd: range.end,
            renderedStart: state.renderedStart,
            renderedEnd: state.renderedEnd
        };
    }

    var validation = projection && validateLargeTreeProjectionDom(state, state.rowsEl, projection);
    if (validation && !validation.ok) {
        return {
            state: 'virtual-tree-projection-mismatch',
            valid: false,
            needsRender: true,
            expectedStart: range.start,
            expectedEnd: range.end,
            renderedStart: state.renderedStart,
            renderedEnd: state.renderedEnd,
            coverageErrors: validation.errors
        };
    }

    if (!treeMaterializerMountedRowsIntersectViewport(materializer, container)) {
        return {
            state: 'virtual-tree-blank-window',
            valid: false,
            needsRender: true,
            renderedStart: state.renderedStart,
            renderedEnd: state.renderedEnd
        };
    }

    return { state: 'rendered', valid: true, needsRender: false };
}

function treeViewportRowsMountTarget(target, containerOverride) {
    var materializer = null;
    var container = containerOverride || null;
    if (target && typeof target.mode === 'function' && target.state) {
        materializer = target;
        container = container || target.state && target.state.container || null;
    } else if (target && target.rows && target.container) {
        materializer = treeMaterializerForState(target);
        container = container || target.container || null;
    } else {
        container = target || container || null;
        materializer = treeMaterializerForContainer(container);
        if (!materializer && container && container.__traceVirtualTreeState) {
            materializer = treeMaterializerForState(container.__traceVirtualTreeState);
        }
    }
    return {
        materializer: materializer,
        container: container
    };
}

function ensureTreeViewportRowsMounted(target, reason, containerOverride) {
    var mountTarget = treeViewportRowsMountTarget(target, containerOverride);
    var materializer = mountTarget.materializer;
    var container = mountTarget.container;
    var before = treeMaterializerViewportRenderStateForMaterializer(materializer, container);
    var state = materializer && materializer.state;
    before.materializeReason = reason || '';
    before.rendered = false;
    if (!state || before.valid || !before.needsRender) return before;

    var rendered = !!renderVirtualTreeVisibleRows(state, true);

    var after = treeMaterializerViewportRenderStateForMaterializer(materializer, container);
    if (treeMaterializerRenderStateIsInvalid(after)) {
        var retried = !!renderVirtualTreeVisibleRows(state, true, true);
        var retryAfter = treeMaterializerViewportRenderStateForMaterializer(materializer, container);
        retryAfter.materializeReason = reason || '';
        retryAfter.rendered = rendered || retried ||
            before.state !== retryAfter.state ||
            retryAfter.valid !== before.valid;
        retryAfter.retried = true;
        return retryAfter;
    }
    after.materializeReason = reason || '';
    after.rendered = rendered || before.state !== after.state || after.valid !== before.valid;
    return after;
}

function treeMaterializerRangeWithinMounted(materializer, range) {
    var normalized = normalizeTreeMaterializerRange(materializer, range);
    if (normalized.status !== 'resolved') return false;
    var mounted = treeMaterializerMountedRange(materializer);
    return normalized.start >= mounted.start && normalized.end <= mounted.end;
}

function treeMaterializerEnsureRowsMounted(materializer, range) {
    var normalized = normalizeTreeMaterializerRange(materializer, range);
    if (normalized.status !== 'resolved') return normalized;
    if (treeMaterializerRangeWithinMounted(materializer, normalized)) {
        return treeMaterializerResolved({ start: normalized.start, end: normalized.end });
    }

    var state = materializer && materializer.state;
    if (!state || !state.rowsEl) return treeMaterializerPending('missing-virtual-row-container');
    var visible = treeMaterializerVisibleRowRange(materializer, VIRTUAL_TREE_OVERSCAN_PX);
    if (normalized.start >= visible.start && normalized.end <= visible.end) {
        ensureTreeViewportRowsMounted(materializer, 'row-range');
        if (treeMaterializerRangeWithinMounted(materializer, normalized)) {
            return treeMaterializerResolved({ start: normalized.start, end: normalized.end });
        }
    }
    return treeMaterializerPending('range-outside-mounted-window', {
        start: normalized.start,
        end: normalized.end,
        mountedStart: state.renderedStart,
        mountedEnd: state.renderedEnd
    });
}

function treeMaterializerScrollRowRangeIntoView(materializer, range, policy) {
    var bounds = treeMaterializerRowRangeBounds(materializer, range);
    if (bounds.status !== 'resolved') return bounds;

    var scroll = treeMaterializerMinimalScrollTopForRange(materializer, bounds, policy);
    if (scroll.status !== 'resolved') return scroll;

    var state = materializer && materializer.state;
    if (!state || !state.container) return treeMaterializerFailed('missing-container');
    var viewport = treeMaterializerViewport(materializer);
    var applied = viewport
        ? viewport.setScroll(scroll.scrollTop, scroll.scrollLeft, 'row-range')
        : treeMaterializerFailed('missing-viewport');
    if (applied.status !== 'resolved') return applied;
    if (state.rowsEl) {
        ensureTreeViewportRowsMounted(materializer, 'row-range-scroll');
        saveVirtualTreeAnchor(state);
    }
    return treeMaterializerResolved({
        start: bounds.start,
        end: bounds.end,
        scrollTop: applied.scrollTop,
        scrollLeft: applied.scrollLeft
    });
}

function treeMaterializerMeasureRowRange(materializer, range) {
    var bounds = treeMaterializerRowRangeBounds(materializer, range);
    if (bounds.status !== 'resolved') return bounds;
    return treeMaterializerResolved({
        start: bounds.start,
        end: bounds.end,
        top: bounds.top,
        bottom: bounds.bottom,
        height: bounds.height
    });
}

function treeMaterializerMeasureVisibleLane(materializer, range) {
    var bounds = treeMaterializerRowRangeBounds(materializer, range);
    if (bounds.status !== 'resolved') return bounds;
    var state = materializer && materializer.state;
    var container = state && state.container;
    var rootEl = state && state.rootEl;
    var left = 0;
    var right = Math.max(0, Number(container && container.clientWidth) || 0);
    if (rootEl && rootEl.getBoundingClientRect && container && container.getBoundingClientRect) {
        var rootRect = rootEl.getBoundingClientRect();
        var containerRect = container.getBoundingClientRect();
        left = Math.max(rootRect.left, containerRect.left);
        right = rootRect.right;
    }
    return treeMaterializerResolved({
        start: bounds.start,
        end: bounds.end,
        left: left,
        right: Math.max(left, right),
        top: bounds.top,
        bottom: bounds.bottom,
        height: bounds.height
    });
}

function treeMaterializerSyncAfterScroll(materializer) {
    var state = materializer && materializer.state;
    if (!state) return treeMaterializerFailed('missing-state');
    saveVirtualTreeAnchor(state);
    var mounted = ensureTreeViewportRowsMounted(materializer, 'sync-scroll');
    return treeMaterializerResolved({
        rendered: !!(mounted && mounted.rendered)
    });
}

function treeMaterializerSyncAfterResize(materializer) {
    var state = materializer && materializer.state;
    if (!state) return treeMaterializerFailed('missing-state');
    var mounted = ensureTreeViewportRowsMounted(materializer, 'sync-resize');
    return treeMaterializerResolved({
        rendered: !!(mounted && mounted.rendered)
    });
}

function treeMaterializerRebuildRows(state) {
    if (!state || !state.tree) return treeMaterializerFailed('missing-tree');
    var session = state.session || state;
    var model = buildTreeRows(state.tree, session, {
        showFields: !!state.showFields,
        showPinned: !!state.showPinned
    });
    state.rows = model.rows;
    state.totalHeight = model.totalHeight;
    state.minWidth = model.minWidth;
    bumpTreeMaterializerModelVersion(state);
    state.renderedStart = -1;
    state.renderedEnd = -1;
    return treeMaterializerResolved({ rows: model.rows });
}

function treeNodePidById(root, nodeId) {
    nodeId = String(nodeId || '');
    if (!root || !nodeId) return null;
    var stack = [{ node: root, pid: '0' }];
    while (stack.length) {
        var item = stack.pop();
        if (item.node && String(item.node.id || '') === nodeId) return item.pid;
        var children = treeChildren(item.node);
        for (var i = children.length - 1; i >= 0; i--) {
            stack.push({
                node: children[i],
                pid: item.pid + '-' + i
            });
        }
    }
    return null;
}

function treeMaterializerRevealPid(state, target) {
    if (!target) return null;
    if (typeof target === 'string' || Array.isArray(target)) return treeRowModelNodeRefPid(target);
    if (typeof target === 'object') {
        if (target.nodeId && !target.pid && !target.path && !target.nodeRef) {
            return treeNodePidById(state && state.tree, target.nodeId);
        }
        return treeRowModelNodeRefPid(
            target.pid || target.nodeRef || target.path || target.ref
        );
    }
    return null;
}

function treeMaterializerNodeForPid(tree, pid) {
    if (!tree || !pid) return null;
    if (pid === '0') return tree;
    var parts = String(pid).split('-');
    if (parts[0] !== '0') return null;
    var node = tree;
    for (var i = 1; i < parts.length; i++) {
        var index = Number(parts[i]);
        var children = treeChildren(node);
        if (!Number.isInteger(index) || index < 0 || index >= children.length) return null;
        node = children[index];
    }
    return node || null;
}

function treeMaterializerCanRevealNodePath(materializer, target) {
    var state = materializer && materializer.state;
    var pid = treeMaterializerRevealPid(state, target);
    return !!(state && state.tree && pid && treeMaterializerNodeForPid(state.tree, pid));
}

function treeMaterializerRevealNodePath(materializer, target) {
    var state = materializer && materializer.state;
    var pid = treeMaterializerRevealPid(state, target);
    if (!state || !pid) return treeMaterializerFailed(!pid ? 'invalid-node-ref' : 'missing-state');
    if (!treeMaterializerCanRevealNodePath(materializer, target)) {
        return treeMaterializerFailed('node-not-found');
    }

    saveTreeMaterializerPaneAnchorForNextRender(state);
    var changed = expandTreePathToPid(state, pid, target && target.field);
    if (changed || !Array.isArray(state.rows) || !state.rows.length) {
        var rebuilt = treeMaterializerRebuildRows(state);
        if (rebuilt.status !== 'resolved') return rebuilt;
        syncVirtualTreeRootGeometry(state);
    }

    return treeMaterializerResolved({
        pid: pid,
        rendered: false,
        rows: state.rows
    });
}

function treeMaterializerForContainer(container) {
    if (!container) return null;
    var materializer = container.__traceTreeMaterializer || null;
    var materializerState = materializer && materializer.state || null;
    var state = container.__traceVirtualTreeState ||
        container.__traceTreeMaterializerState ||
        (materializerState && materializerState.container === container ? materializerState : null);
    if (!treeMaterializerHostMatchesState(container, state)) return null;
    if (treeMaterializerMatchesState(materializer, state)) return materializer;
    if (state) return treeMaterializerForState(state);
    return null;
}

function treeMaterializerForNodeId(nodeId) {
    if (!hasDOM() || !nodeId || !document.getElementById) return null;
    var nodeEl = document.getElementById(nodeId);
    var root = nodeEl && nodeEl.closest ? nodeEl.closest('.tree-root') : null;
    var materializer = root && root.__traceTreeMaterializer || null;
    var materializerState = materializer && materializer.state || null;
    var state = root && (root.__traceVirtualTreeState ||
        root.__traceTreeMaterializerState ||
        (materializerState && materializerState.rootEl === root ? materializerState : null)) || null;
    if (!treeMaterializerHostMatchesState(root, state)) return null;
    if (treeMaterializerMatchesState(materializer, state)) return materializer;
    return state ? treeMaterializerForState(state) : null;
}

function syncTreeMaterializerSessionState(state, session) {
    if (!state || !session) return false;
    if (!session.collapsed) session.collapsed = {};
    if (!session.fieldRowsOpen) session.fieldRowsOpen = {};
    if (!session.fieldRowsClosed) session.fieldRowsClosed = {};
    state.session = session;
    state.collapsed = session.collapsed;
    state.fieldRowsOpen = session.fieldRowsOpen;
    state.fieldRowsClosed = session.fieldRowsClosed;
    return true;
}

function applyTreeMaterializerRowsChanged(materializer) {
    var state = materializer && materializer.state;
    if (!state) return false;

    var rebuilt = treeMaterializerRebuildRows(state);
    if (rebuilt.status !== 'resolved') return false;

    syncVirtualTreeRootGeometry(state);
    restoreVirtualTreeAnchor(state);
    saveVirtualTreeAnchor(state);
    return ensureTreeViewportRowsMounted(materializer, 'rows-changed').valid !== false;
}

function toggleTreeNodeWithMaterializer(nodeId) {
    var materializer = treeMaterializerForNodeId(nodeId);
    var state = materializer && materializer.state;
    var parsed = treeSessionForNodeId(nodeId);
    if (!materializer || !state || !parsed || !parsed.pid || !parsed.session) return false;

    syncTreeMaterializerSessionState(state, parsed.session);
    saveTreeMaterializerPaneAnchorForNextRender(state);

    var collapsed = !treeRowModelNodeCollapsed(parsed.session, parsed.pid);
    if (!setTreeSessionNodeCollapsed(parsed.session, parsed.pid, collapsed)) return false;

    invalidateVirtualTreePeerRenders(state);
    applyTreeMaterializerRowsChanged(materializer);
    return true;
}

function toggleTreeFieldRowsWithMaterializer(nodeId) {
    var materializer = treeMaterializerForNodeId(nodeId);
    var state = materializer && materializer.state;
    var parsed = treeSessionForNodeId(nodeId);
    if (!materializer || !state || !parsed || !parsed.pid || !parsed.session) return false;

    syncTreeMaterializerSessionState(state, parsed.session);
    saveTreeMaterializerPaneAnchorForNextRender(state);

    var showFields = !!state.showFields;
    var visible = treeRowModelFieldRowsVisible(parsed.session, parsed.pid, showFields);
    if (!setTreeSessionFieldRowsCollapsed(parsed.session, parsed.pid, showFields, visible)) return false;

    invalidateVirtualTreePeerRenders(state);
    applyTreeMaterializerRowsChanged(materializer);
    return true;
}

function treeMaterializerOwnsPaneScroll(container) {
    var materializer = treeMaterializerForContainer(container);
    var mode = materializer && typeof materializer.mode === 'function'
        ? materializer.mode()
        : '';
    return mode === 'virtual';
}

function captureTreeMaterializerPaneScroll(container) {
    var materializer = treeMaterializerForContainer(container);
    var state = materializer && materializer.state;
    return !!saveTreeMaterializerPaneAnchorForNextRender(state);
}

function restoreTreeMaterializerPaneScrollAfterReattach(container) {
    if (!treeMaterializerOwnsPaneScroll(container)) return false;
    var materializer = treeMaterializerForContainer(container);
    var state = materializer && materializer.state;
    if (!state || state.container !== container) return false;

    var restored = restoreTreeMaterializerPaneAnchor(state);
    ensureTreeViewportRowsMounted(state, 'reattach');
    return restored;
}

function treeMaterializerViewportRenderState(container) {
    var materializer = treeMaterializerForContainer(container);
    if (!materializer && container && container.__traceVirtualTreeState) {
        materializer = treeMaterializerForState(container.__traceVirtualTreeState);
    }
    return treeMaterializerViewportRenderStateForMaterializer(materializer, container);
}

function treeMaterializerRenderStateIsInvalid(state) {
    return !!(state && (
        state.state === 'virtual-tree-missing-root' ||
        state.state === 'virtual-tree-stale-window' ||
        state.state === 'virtual-tree-blank-window' ||
        state.state === 'virtual-tree-projection-mismatch'
    ));
}

function treeRowModelNodeRefPid(nodeRef) {
    if (nodeRef === null || nodeRef === undefined) return null;
    if (typeof nodeRef === 'string') return nodeRef;
    if (Array.isArray(nodeRef)) return nodeRef.length ? treePathId(nodeRef) : '0';
    if (typeof nodeRef === 'object') {
        if (typeof nodeRef.pid === 'string') return nodeRef.pid;
        if (Array.isArray(nodeRef.path)) return treeRowModelNodeRefPid(nodeRef.path);
    }
    return null;
}

function treeRowModelNodeRefSourceId(nodeRef) {
    return nodeRef && typeof nodeRef === 'object' && typeof nodeRef.nodeId === 'string'
        ? nodeRef.nodeId
        : '';
}

function treeRowRangeResolved(start, end) {
    return { status: 'resolved', start: start, end: end };
}

function treeRowRangeFailed(reason) {
    return { status: 'failed', reason: reason || 'not-found' };
}

function treeRowFieldMatchesRecordRow(row, recordRow) {
    if (!row || row.kind !== 'fields') return false;
    if (recordRow === undefined || recordRow === null || recordRow === '') return true;

    var rowKey = String(recordRow);
    if (rowKey.indexOf('attr:') === 0) {
        var attrIndex = Number(rowKey.substring('attr:'.length));
        var attr = Array.isArray(row.node && row.node.a) && Number.isFinite(attrIndex)
            ? row.node.a[attrIndex]
            : null;
        var attrKey = Array.isArray(attr)
            ? attr[0]
            : attr && (attr.key !== undefined ? attr.key : attr.id);
        return !!attr && !!row.meta && String(treeFieldRowFieldKey(row.meta)) === String(attrKey);
    }
    if (rowKey.indexOf('field:') === 0) {
        var fieldKey = rowKey.substring('field:'.length);
        return !!fieldKey && !!row.meta &&
            String(treeFieldRowFieldKey(row.meta)) === String(fieldKey);
    }

    var metaIndex = Number(rowKey);
    return Number.isFinite(metaIndex) && row.metaIndex === metaIndex;
}

function rowForNodeRef(rows, nodeRef) {
    var sourceId = treeRowModelNodeRefSourceId(nodeRef);
    if (sourceId && Array.isArray(rows)) {
        for (var byId = 0; byId < rows.length; byId++) {
            var idRow = rows[byId];
            if (idRow && idRow.kind === 'node' && idRow.node &&
                    String(idRow.node.id || '') === sourceId) {
                return idRow;
            }
        }
    }
    var pid = treeRowModelNodeRefPid(nodeRef);
    if (!pid || !Array.isArray(rows)) return null;
    for (var i = 0; i < rows.length; i++) {
        var row = rows[i];
        if (row && row.kind === 'node' && row.pid === pid) return row;
    }
    return null;
}

function subtreeRange(rows, nodeRef) {
    var row = rowForNodeRef(rows, nodeRef);
    if (!row) return treeRowRangeFailed('node-not-found');

    var start = row.index;
    var pid = row.pid;
    var childPrefix = pid + '-';
    var end = start + 1;
    while (end < rows.length) {
        var nextPid = String(rows[end] && rows[end].pid || '');
        if (nextPid !== pid && nextPid.indexOf(childPrefix) !== 0) break;
        end++;
    }
    return treeRowRangeResolved(start, end);
}

function rowRangeForTarget(rows, targetRef) {
    if (!targetRef || typeof targetRef !== 'object' || Array.isArray(targetRef)) {
        var row = rowForNodeRef(rows, targetRef);
        return row ? treeRowRangeResolved(row.index, row.index + 1) :
            treeRowRangeFailed('node-not-found');
    }

    var type = targetRef.type || targetRef.kind || 'node';
    var nodeRef = targetRef.nodeRef || targetRef.pid || targetRef.path ||
        (targetRef.nodeId ? { nodeId: targetRef.nodeId } : null);
    if (type === 'subtree') return subtreeRange(rows, nodeRef);
    if (type === 'node') {
        var nodeRow = rowForNodeRef(rows, nodeRef);
        return nodeRow ? treeRowRangeResolved(nodeRow.index, nodeRow.index + 1) :
            treeRowRangeFailed('node-not-found');
    }
    if (type === 'fields') {
        var ownerRow = rowForNodeRef(rows, nodeRef);
        var pid = ownerRow && ownerRow.pid || treeRowModelNodeRefPid(nodeRef);
        var metaIndex = Math.floor(Number(targetRef.metaIndex));
        var hasMetaIndex = Number.isFinite(metaIndex);
        var hasRecordRow = targetRef.recordRow !== undefined &&
            targetRef.recordRow !== null &&
            targetRef.recordRow !== '';
        if (!pid || (!hasMetaIndex && targetRef.recordRow !== undefined && !hasRecordRow)) {
            return treeRowRangeFailed('invalid-fields-ref');
        }
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            if (!row || row.kind !== 'fields' || row.pid !== pid) continue;
            if ((hasMetaIndex && row.metaIndex === metaIndex) ||
                    (!hasMetaIndex && treeRowFieldMatchesRecordRow(row, targetRef.recordRow))) {
                return treeRowRangeResolved(i, i + 1);
            }
        }
        return treeRowRangeFailed('fields-row-not-found');
    }
    return treeRowRangeFailed('unsupported-target-type');
}

function treePidDepth(pid) {
    pid = String(pid || '');
    if (!pid) return 0;
    return Math.max(0, pid.split('-').length - 1);
}

function treeRowKey(row) {
    if (!row) return '';
    if (row.kind === 'fields') return 'meta:' + row.pid + ':' + row.metaIndex;
    return 'node:' + row.pid;
}

function treeRowSignature(row) {
    if (!row) return '';
    var parts = [
        treeRowKey(row),
        row.kind || '',
        row.pid || '',
        row.height || 0,
        row.isRoot ? 'root' : '',
        row.isLast ? 'last' : '',
        row.hasChildren ? 'children' : '',
        row.hasMeta ? 'fields' : '',
        row.collapsed ? 'collapsed' : 'open'
    ];
    if (row.kind === 'fields') {
        parts.push(
            row.metaIndex || 0,
            treeFieldRowFieldKey(row.meta),
            treeFieldRowKey(row.meta),
            treeFieldRowValue(row.meta),
            row.keyAlignCh || 0,
            (row.metaPrefix || []).join(',')
        );
    } else {
        parts.push(
            row.node && row.node.id || '',
            row.node && row.node.op || '',
            row.node && row.node.l || '',
            (row.parentPrefix || []).join(','),
            (row.childPrefix || []).join(',')
        );
    }
    return parts.join('|');
}

function treeProjectionTotalHeight(model, rows) {
    var totalHeight = Number(model && model.totalHeight);
    if (Number.isFinite(totalHeight) && totalHeight >= 0) return totalHeight;
    if (!rows || !rows.length) return 0;
    var last = rows[rows.length - 1];
    return Math.max(0, Number(last.top) + Number(last.height) || 0);
}

function treeProjectionViewport(viewport, totalHeight) {
    viewport = viewport || {};
    var top = Number(
        viewport.top !== undefined ? viewport.top :
        viewport.scrollTop !== undefined ? viewport.scrollTop :
        0
    );
    var bottom = Number(viewport.bottom);
    var height = Number(viewport.height);
    if (!Number.isFinite(top)) top = 0;
    top = Math.max(0, top);
    if (Number.isFinite(bottom)) {
        bottom = Math.max(top, bottom);
        height = bottom - top;
    } else {
        if (!Number.isFinite(height) || height < 0) height = 0;
        bottom = top + height;
    }
    var maxTop = Math.max(0, Number(totalHeight) || 0);
    top = Math.min(top, maxTop);
    bottom = Math.max(top, Math.min(bottom, maxTop));
    height = Math.max(0, bottom - top);
    return { top: top, bottom: bottom, height: height };
}

function buildTreeProjection(model, viewport, overscanPx) {
    var rows = Array.isArray(model) ? model : model && model.rows || [];
    var totalHeight = treeProjectionTotalHeight(model, rows);
    var visible = treeProjectionViewport(viewport, totalHeight);
    var overscan = Math.max(0, Number(overscanPx) || 0);
    var overscanTop = Math.max(0, visible.top - overscan);
    var overscanBottom = Math.min(totalHeight, visible.bottom + overscan);

    var start = 0;
    while (start < rows.length && rows[start].top + rows[start].height < overscanTop) start++;

    var end = start;
    while (end < rows.length && rows[end].top <= overscanBottom) end++;

    var projectedRows = [];
    var rowHeight = 0;
    for (var i = start; i < end; i++) {
        var row = rows[i];
        var height = Math.max(0, Number(row && row.height) || 0);
        rowHeight += height;
        projectedRows.push({
            index: i,
            key: treeRowKey(row),
            signature: treeRowSignature(row),
            top: Math.max(0, Number(row && row.top) || 0),
            height: height,
            row: row
        });
    }

    var topSpacerHeight = projectedRows.length ? projectedRows[0].top : totalHeight;
    var mountedEnd = projectedRows.length
        ? projectedRows[projectedRows.length - 1].top + projectedRows[projectedRows.length - 1].height
        : topSpacerHeight;
    var bottomSpacerHeight = Math.max(0, totalHeight - mountedEnd);

    return {
        start: start,
        end: end,
        rows: projectedRows,
        topSpacerHeight: topSpacerHeight,
        bottomSpacerHeight: bottomSpacerHeight,
        rowHeight: rowHeight,
        totalHeight: totalHeight,
        viewportTop: visible.top,
        viewportBottom: visible.bottom,
        viewportHeight: visible.height,
        overscanTop: overscanTop,
        overscanBottom: overscanBottom,
        mountedTop: topSpacerHeight,
        mountedBottom: mountedEnd
    };
}

function treeRowDepth(row) {
    return row ? treePidDepth(row.pid) : 0;
}

function treeParentPid(pid) {
    pid = String(pid || '');
    var lastDash = pid.lastIndexOf('-');
    return lastDash > 0 ? pid.substring(0, lastDash) : '';
}

function expandTreePathToPid(state, pid, field) {
    if (!state || !pid) return false;
    var changed = false;
    var session = state.session || state;
    if (!session.collapsed) session.collapsed = {};
    if (!session.fieldRowsOpen) session.fieldRowsOpen = {};
    if (!session.fieldRowsClosed) session.fieldRowsClosed = {};
    state.collapsed = session.collapsed;
    state.fieldRowsOpen = session.fieldRowsOpen;
    state.fieldRowsClosed = session.fieldRowsClosed;
    var parent = treeParentPid(pid);
    while (parent) {
        if (session.collapsed[parent]) {
            delete session.collapsed[parent];
            changed = true;
        }
        parent = treeParentPid(parent);
    }
    if (field === 'fields') {
        if (session.fieldRowsClosed[pid]) {
            delete session.fieldRowsClosed[pid];
            changed = true;
        }
        if (!state.showFields && !session.fieldRowsOpen[pid]) {
            session.fieldRowsOpen[pid] = true;
            changed = true;
        }
    }
    return changed;
}

function clampTreeScrollValue(value, max) {
    value = Number(value);
    max = Number(max);
    if (!Number.isFinite(value)) value = 0;
    if (!Number.isFinite(max) || max <= 0) return 0;
    return Math.max(0, Math.min(max, value));
}

function treeMaterializerPlanStartOffset(state) {
    if (!state || !state.showPinned) return 0;

    var total = 0;
    var container = state.container;
    var cells = container && container.querySelectorAll
        ? container.querySelectorAll('.tree-pinned-header.visible .pinned-header-cell')
        : null;
    if (cells && cells.length) {
        for (var i = 0; i < cells.length; i++) {
            var rect = cells[i].getBoundingClientRect ? cells[i].getBoundingClientRect() : null;
            total += Number(rect && rect.width) || Number(cells[i].offsetWidth) || 0;
        }
        if (total > 0) return total;
    }

    return activeNodeColumnWidthTotal();
}

function treeMaterializerPlanScrollLeft(state, rawScrollLeft) {
    return (Number(rawScrollLeft) || 0) - treeMaterializerPlanStartOffset(state);
}

function captureTreeMaterializerAnchor(state) {
    if (!state || !state.container || !state.rows || !state.rows.length) return null;
    ensureTreeMaterializerStateIdentity(state);

    var materializer = treeMaterializerForState(state);
    var viewportRange = materializer ? materializer.viewportRange() : null;
    var top = Math.max(0, Number(viewportRange && viewportRange.top) || 0);
    var rows = state.rows;
    var index = 0;
    while (index < rows.length && rows[index].top + rows[index].height <= top) index++;
    if (index >= rows.length) index = rows.length - 1;

    var row = rows[index];
    var offset = Math.max(0, Math.min(row.height - 1, top - row.top));
    return {
        ruleKey: state.ruleKey || '',
        treeInstanceId: state.treeInstanceId || '',
        treeModelVersion: Number(state.treeModelVersion) || 0,
        treeProjectionVersion: Number(state.treeProjectionVersion) || 0,
        mode: state.mode || treeMaterializerMode(materializer),
        rowKey: treeRowKey(row),
        kind: row.kind,
        pid: row.pid,
        metaIndex: row.kind === 'fields' ? row.metaIndex : null,
        depth: treeRowDepth(row),
        index: index,
        offsetPx: offset,
        scrollLeft: state.container.scrollLeft || 0,
        planScrollLeft: treeMaterializerPlanScrollLeft(state, state.container.scrollLeft || 0)
    };
}

function saveTreeMaterializerPaneAnchor(state) {
    if (!state || !state.session) return null;
    var anchor = captureTreeMaterializerAnchor(state);
    if (!anchor) return null;
    state.session.anchor = anchor;
    state.session.scrollLeft = anchor.scrollLeft;
    return anchor;
}

function saveTreeMaterializerPaneAnchorForNextRender(state) {
    var anchor = saveTreeMaterializerPaneAnchor(state);
    if (anchor) state.anchorSavedForNextRender = true;
    return anchor;
}

function saveVirtualTreeAnchor(state) {
    return saveTreeMaterializerPaneAnchor(state);
}

function virtualTreeAnchorCompatibleWithState(state, anchor) {
    if (!state || !anchor) return false;
    ensureTreeMaterializerStateIdentity(state);

    var anchorRuleKey = anchor.ruleKey !== undefined ? normalizeTreeRuleKey(anchor.ruleKey) : '';
    var stateRuleKey = normalizeTreeRuleKey(state.ruleKey || '');
    if (anchorRuleKey && anchorRuleKey !== stateRuleKey) return false;

    var anchorInstance = String(anchor.treeInstanceId || '');
    if (anchorInstance && anchorInstance === state.treeInstanceId) {
        var anchorModelVersion = Number(anchor.treeModelVersion);
        var stateModelVersion = Number(state.treeModelVersion) || 0;
        if (Number.isFinite(anchorModelVersion) && anchorModelVersion > stateModelVersion) {
            return false;
        }
    }

    return true;
}

function virtualTreeAnchorScrollLeft(state, anchor) {
    var planLeft = Number(anchor && anchor.planScrollLeft);
    if (Number.isFinite(planLeft)) {
        if (planLeft >= 0) {
            return planLeft + treeMaterializerPlanStartOffset(state);
        }
        if (!(state && state.showPinned)) {
            return 0;
        }
    }

    var anchorLeft = Number(anchor && anchor.scrollLeft);
    if (Number.isFinite(anchorLeft)) return Math.max(0, anchorLeft);

    var sessionLeft = Number(state && state.session && state.session.scrollLeft);
    return Number.isFinite(sessionLeft) ? Math.max(0, sessionLeft) : 0;
}

function findTreeRowIndexByKey(state, key) {
    if (!state || !state.rows || !key) return -1;
    for (var i = 0; i < state.rows.length; i++) {
        if (treeRowKey(state.rows[i]) === key) return i;
    }
    return -1;
}

function findTreeAncestorRowIndex(state, pid) {
    pid = String(pid || '');
    while (pid) {
        var index = findTreeRowIndexByKey(state, 'node:' + pid);
        if (index >= 0) return index;
        pid = treeParentPid(pid);
    }
    return -1;
}

function findTreeSameLevelNeighborIndex(state, anchor) {
    if (!state || !state.rows || !state.rows.length || !anchor) return -1;

    var rows = state.rows;
    var start = Math.max(0, Math.min(rows.length - 1, Math.floor(Number(anchor.index) || 0)));
    var depth = Math.max(0, Math.floor(Number(anchor.depth) || 0));
    for (var delta = 0; delta < rows.length; delta++) {
        var before = start - delta;
        if (before >= 0 && rows[before].kind === 'node' && treeRowDepth(rows[before]) === depth) {
            return before;
        }

        var after = start + delta;
        if (after !== before && after < rows.length &&
            rows[after].kind === 'node' && treeRowDepth(rows[after]) === depth) {
            return after;
        }
    }
    return -1;
}

function virtualTreeAnchorRowIndex(state, anchor) {
    if (!state || !anchor) return -1;

    var index = findTreeRowIndexByKey(state, anchor.rowKey);
    if (index >= 0) return index;

    index = findTreeAncestorRowIndex(state, anchor.pid);
    if (index >= 0) return index;

    return findTreeSameLevelNeighborIndex(state, anchor);
}

function restoreTreeMaterializerPaneAnchor(state) {
    if (!state || !state.container || !state.rows || !state.rows.length) return false;
    var anchor = state.session && state.session.anchor;
    if (!anchor) return false;
    if (!virtualTreeAnchorCompatibleWithState(state, anchor)) return false;

    var rowIndex = virtualTreeAnchorRowIndex(state, anchor);
    if (rowIndex < 0) return false;

    var row = state.rows[rowIndex];
    var materializer = treeMaterializerForState(state);
    var viewport = materializer && materializer.viewport();
    var rootTop = treeMaterializerRootTop(materializer);
    var offset = Math.max(0, Math.min(row.height - 1, Number(anchor.offsetPx) || 0));
    var applied = viewport
        ? viewport.setScroll(
            rootTop + row.top + offset,
            virtualTreeAnchorScrollLeft(state, anchor),
            'restore-virtual-tree-anchor'
        )
        : treeMaterializerFailed('missing-viewport');
    return applied.status === 'resolved';
}

function restoreTreeMaterializerPaneAnchorForViewportRestore(materializer, reason) {
    var state = materializer && materializer.state || materializer;
    var restored = restoreTreeMaterializerPaneAnchor(state);
    if (materializer) {
        ensureTreeViewportRowsMounted(materializer, reason || 'viewport-anchor-restore');
    }
    return restored;
}

function restoreVirtualTreeAnchor(state) {
    return restoreTreeMaterializerPaneAnchor(state);
}

function syncVirtualTreeRootGeometry(state) {
    if (!state || !state.rootEl) return;
    state.rootEl.style.height = state.totalHeight + 'px';
    state.rootEl.style.minWidth = state.minWidth + 'px';
}

function virtualTreeDebugOverlayHtml(state, range) {
    return '';
}

function renderVirtualTreeVisibleRows(visualCtx, state, force, reResolveTarget) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        reResolveTarget = !!force;
        force = state;
        state = visualCtx;
        return runVisualSurfaceCommitNow(
            'virtual-tree-visible-rows',
            ['rule-tree'],
            ['trace', 'render'],
            function(ctx) {
                return renderVirtualTreeVisibleRows(ctx, state, force, reResolveTarget);
            }
        );
    }
    if (!state || !state.rowsEl) return;

    var rendered = false;
    var commitOptions = {
        label: 'virtual-tree-visible-rows',
        state: state,
        detachedReason: 'detached_container'
    };
    if (!reResolveTarget) {
        commitOptions.target = state.rowsEl;
    }
    ruleTreeSurface.commit(visualCtx, commitOptions, function(target) {
        rendered = renderVirtualTreeVisibleRowsForCommit(state, target, force);
    });
    return rendered;
}

function renderVirtualTreeVisibleRowsForCommit(state, target, force) {
    if (!state || !target) return false;
    state.rowsEl = target;
    if (state.container && state.container.__traceVirtualTreeState &&
            state.container.__traceVirtualTreeState !== state) {
        recordVisualCommitDenied('rule-tree', 'virtual-tree-visible-rows', 'repurposed_container', {
            ruleKey: state.ruleKey || ''
        });
        return false;
    }
    var expectedRootId = 'treeroot-' + (state.ruleKey || '');
    if (state.rootEl && state.rootEl.id && state.rootEl.id !== expectedRootId) {
        recordVisualCommitDenied('rule-tree', 'virtual-tree-visible-rows', 'repurposed_container', {
            ruleKey: state.ruleKey || '',
            expectedId: expectedRootId,
            actualId: state.rootEl.id
        });
        return false;
    }
    var materializer = treeMaterializerForState(state);
    var viewportRange = materializer.viewportRange();
    var projection = buildTreeProjection(state, {
        top: viewportRange.top,
        bottom: viewportRange.bottom
    }, VIRTUAL_TREE_OVERSCAN_PX);
    var range = { start: projection.start, end: projection.end };
    if (!force && range.start === state.renderedStart && range.end === state.renderedEnd) return false;

    var debugHtml = virtualTreeDebugOverlayHtml(state, range);
    if (reconcileVirtualTreeProjectionDom(target, state, projection, debugHtml)) {
        state.renderedStart = range.start;
        state.renderedEnd = range.end;
        bumpTreeMaterializerProjectionVersion(state);
        if (typeof InfoTargetController !== 'undefined' && InfoTargetController &&
                typeof InfoTargetController.reapplyActiveTargetHighlights === 'function') {
            InfoTargetController.reapplyActiveTargetHighlights();
        }
        return true;
    }

    target.innerHTML = virtualTreeProjectionHtml(state, projection, debugHtml);
    state.renderedStart = range.start;
    state.renderedEnd = range.end;
    bumpTreeMaterializerProjectionVersion(state);
    if (typeof InfoTargetController !== 'undefined' && InfoTargetController &&
            typeof InfoTargetController.reapplyActiveTargetHighlights === 'function') {
        InfoTargetController.reapplyActiveTargetHighlights();
    }
    return true;
}

function treeRowDecoration(state, row) {
    var decorations = state && state.rowDecorations;
    if (!decorations || !row || !row.pid) return null;
    return decorations[row.pid] || null;
}

function treeRowDecorationClass(state, row) {
    var decoration = treeRowDecoration(state, row);
    return decoration && decoration.className ? String(decoration.className) : '';
}

function treeRowDecorationAttrs(state, row) {
    var decoration = treeRowDecoration(state, row);
    return decoration && decoration.attrs ? String(decoration.attrs) : '';
}

function treeRowDecorationFieldChanges(state, row) {
    var decoration = treeRowDecoration(state, row);
    return decoration ? decoration.fieldChanges || null : null;
}

function treeRowDecorationDiffSide(state, row) {
    var decoration = treeRowDecoration(state, row);
    return decoration && decoration.diffSide ? String(decoration.diffSide) : null;
}

function treeRowDecorationLabelHtml(state, row) {
    var decoration = treeRowDecoration(state, row);
    return decoration && decoration.labelHtml !== undefined ? String(decoration.labelHtml) : null;
}

function treeRowDecorationRenderSignature(state, row) {
    var decoration = treeRowDecoration(state, row);
    return decoration && decoration.signature ? String(decoration.signature) : '';
}

function treeNodeWrapperClass(state, row) {
    var classes = 'tree-node';
    var extra = treeRowDecorationClass(state, row);
    return extra ? classes + ' ' + extra : classes;
}

function virtualTreeProjectionRowRenderSignature(projected, state) {
    return [
        projected.signature,
        treeRowDecorationRenderSignature(state, projected.row),
        state && state.renderSignature || '',
        state && state.query || '',
        state && state.scope || '',
        state && state.showPinned ? 'pinned' : '',
        activeNodeColumnKeys().map(function(key) {
            return key + ':' + nodeColumnWidthForKey(key);
        }).join(',')
    ].join('|');
}

function virtualTreeProjectionSpacerHtml(kind, height) {
    return '<div class="tree-virtual-spacer tree-virtual-spacer-' + kind + '" aria-hidden="true" ' +
        'data-tree-projection-key="spacer:' + kind + '" data-tree-spacer="' + kind +
        '" data-tree-render-signature="' + kind + ':' + height +
        '" style="height:' + height + 'px"></div>';
}

function virtualTreeProjectionRowHtml(projected, state) {
    var renderSignature = virtualTreeProjectionRowRenderSignature(projected, state);
    return '<div class="tree-virtual-row" data-tree-row="' + projected.index +
        '" data-tree-instance="' + htmlEscape(state && state.treeInstanceId || '') +
        '" data-tree-model-version="' + htmlEscape(String(state && state.treeModelVersion || 0)) +
        '" data-tree-projection-key="row:' + htmlEscape(projected.key) +
        '" data-tree-row-key="' + htmlEscape(projected.key) +
        '" data-tree-row-signature="' + htmlEscape(projected.signature) +
        '" data-tree-render-signature="' + htmlEscape(renderSignature) +
        '" style="top:' + projected.top + 'px;height:' + projected.height + 'px">' +
        renderVirtualTreeRow(projected.row, state) +
        '</div>';
}

function virtualTreeProjectionHtml(state, projection, debugHtml) {
    var html = debugHtml || '';
    html += '<div class="tree-virtual-spacer tree-virtual-spacer-top" aria-hidden="true" ' +
        'data-tree-projection-key="spacer:top" data-tree-spacer="top" ' +
        'data-tree-render-signature="top:' + projection.topSpacerHeight +
        '" style="height:' + projection.topSpacerHeight + 'px"></div>';
    for (var i = 0; i < projection.rows.length; i++) {
        html += virtualTreeProjectionRowHtml(projection.rows[i], state);
    }
    html += '<div class="tree-virtual-spacer tree-virtual-spacer-bottom" aria-hidden="true" ' +
        'data-tree-projection-key="spacer:bottom" data-tree-spacer="bottom" ' +
        'data-tree-render-signature="bottom:' + projection.bottomSpacerHeight +
        '" style="height:' + projection.bottomSpacerHeight + 'px"></div>';
    return html;
}

function virtualTreeProjectionDescriptors(state, projection, debugHtml) {
    var descriptors = [];
    if (debugHtml) {
        descriptors.push({
            key: 'debug',
            signature: 'debug:' + debugHtml,
            html: debugHtml
        });
    }
    descriptors.push({
        key: 'spacer:top',
        signature: 'top:' + projection.topSpacerHeight,
        html: virtualTreeProjectionSpacerHtml('top', projection.topSpacerHeight)
    });
    for (var i = 0; i < projection.rows.length; i++) {
        var projected = projection.rows[i];
        descriptors.push({
            key: 'row:' + projected.key,
            signature: virtualTreeProjectionRowRenderSignature(projected, state),
            html: virtualTreeProjectionRowHtml(projected, state)
        });
    }
    descriptors.push({
        key: 'spacer:bottom',
        signature: 'bottom:' + projection.bottomSpacerHeight,
        html: virtualTreeProjectionSpacerHtml('bottom', projection.bottomSpacerHeight)
    });
    return descriptors;
}

function virtualTreeProjectionElementKey(el) {
    return el && el.getAttribute ? el.getAttribute('data-tree-projection-key') || '' : '';
}

function virtualTreeProjectionElementSignature(el) {
    return el && el.getAttribute ? el.getAttribute('data-tree-render-signature') || '' : '';
}

function createVirtualTreeProjectionElement(html) {
    if (!hasDOM() || !document.createElement) return null;
    var wrapper = document.createElement('div');
    wrapper.innerHTML = html;
    return wrapper.firstElementChild || null;
}

function reconcileVirtualTreeProjectionDom(target, state, projection, debugHtml) {
    if (!hasDOM() || !target || !target.appendChild || !target.removeChild || !target.children) {
        return false;
    }

    var descriptors = virtualTreeProjectionDescriptors(state, projection, debugHtml);
    var existingByKey = {};
    for (var i = 0; i < target.children.length; i++) {
        var child = target.children[i];
        var key = virtualTreeProjectionElementKey(child);
        if (key && !existingByKey[key]) existingByKey[key] = child;
    }

    var desired = [];
    for (var di = 0; di < descriptors.length; di++) {
        var descriptor = descriptors[di];
        var el = existingByKey[descriptor.key] || null;
        if (!el || virtualTreeProjectionElementSignature(el) !== descriptor.signature) {
            el = createVirtualTreeProjectionElement(descriptor.html);
            if (!el) return false;
        }
        desired.push(el);
        target.appendChild(el);
    }

    for (var ci = target.children.length - 1; ci >= 0; ci--) {
        var current = target.children[ci];
        if (desired.indexOf(current) === -1) target.removeChild(current);
    }
    return true;
}

function decodeTreeHtmlAttr(value) {
    return String(value || '')
        .replace(/&#(\d+);/g, function(_, code) {
            return String.fromCharCode(Number(code) || 0);
        })
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&');
}

function treeProjectionAttrMap(tag) {
    var attrs = {};
    String(tag || '').replace(/([a-zA-Z0-9:-]+)="([^"]*)"/g, function(_, name, value) {
        attrs[name] = decodeTreeHtmlAttr(value);
        return '';
    });
    return attrs;
}

function treeProjectionStylePx(styleText, name) {
    var match = String(styleText || '').match(new RegExp(name + '\\s*:\\s*([-0-9.]+)px'));
    return match ? Number(match[1]) : NaN;
}

function treeProjectionElementSnapshotFromAttrs(attrs) {
    attrs = attrs || {};
    var style = attrs.style || '';
    return {
        key: attrs['data-tree-projection-key'] || '',
        signature: attrs['data-tree-render-signature'] || '',
        treeInstanceId: attrs['data-tree-instance'] || '',
        treeModelVersion: attrs['data-tree-model-version'] || '',
        spacer: attrs['data-tree-spacer'] || '',
        rowIndex: Number.isFinite(Number(attrs['data-tree-row'])) ? Math.floor(Number(attrs['data-tree-row'])) : -1,
        rowKey: attrs['data-tree-row-key'] || '',
        rowSignature: attrs['data-tree-row-signature'] || '',
        top: treeProjectionStylePx(style, 'top'),
        height: treeProjectionStylePx(style, 'height')
    };
}

function treeProjectionElementSnapshot(el) {
    if (!el || !el.getAttribute) return null;
    var style = el.style || {};
    return {
        key: el.getAttribute('data-tree-projection-key') || '',
        signature: el.getAttribute('data-tree-render-signature') || '',
        treeInstanceId: el.getAttribute('data-tree-instance') || '',
        treeModelVersion: el.getAttribute('data-tree-model-version') || '',
        spacer: el.getAttribute('data-tree-spacer') || '',
        rowIndex: Number.isFinite(Number(el.getAttribute('data-tree-row')))
            ? Math.floor(Number(el.getAttribute('data-tree-row')))
            : -1,
        rowKey: el.getAttribute('data-tree-row-key') || '',
        rowSignature: el.getAttribute('data-tree-row-signature') || '',
        top: Number.isFinite(Number.parseFloat(style.top)) ? Number.parseFloat(style.top) : NaN,
        height: Number.isFinite(Number.parseFloat(style.height)) ? Number.parseFloat(style.height) : NaN
    };
}

function collectTreeProjectionDomSnapshot(target) {
    var elements = [];
    if (target && target.children && target.children.length) {
        for (var i = 0; i < target.children.length; i++) {
            var snapshot = treeProjectionElementSnapshot(target.children[i]);
            if (snapshot && snapshot.key) elements.push(snapshot);
        }
    } else {
        var html = String(target && target.innerHTML || '');
        html.replace(/<div\b[^>]*data-tree-projection-key="[^"]*"[^>]*>/g, function(tag) {
            var snapshot = treeProjectionElementSnapshotFromAttrs(treeProjectionAttrMap(tag));
            if (snapshot.key) elements.push(snapshot);
            return '';
        });
    }
    return {
        elements: elements,
        rows: elements.filter(function(el) { return el.key.indexOf('row:') === 0; }),
        topSpacer: elements.filter(function(el) { return el.key === 'spacer:top'; })[0] || null,
        bottomSpacer: elements.filter(function(el) { return el.key === 'spacer:bottom'; })[0] || null
    };
}

function pushLargeTreeProjectionIssue(errors, code, path, message, details) {
    errors.push({
        code: code,
        path: path,
        message: message,
        details: details || null
    });
}

function largeTreeProjectionNumbersEqual(a, b) {
    return Math.abs((Number(a) || 0) - (Number(b) || 0)) <= 0.01;
}

function validateLargeTreeProjectionDom(state, target, projection) {
    ensureTreeMaterializerStateIdentity(state);
    projection = projection || buildTreeProjection(state || {}, {
        top: 0,
        bottom: 0
    }, VIRTUAL_TREE_OVERSCAN_PX);
    var errors = [];
    var rows = Array.isArray(state && state.rows) ? state.rows : [];
    var renderedStart = Number(state && state.renderedStart);
    var renderedEnd = Number(state && state.renderedEnd);
    if (Number.isFinite(renderedStart) && renderedStart !== projection.start) {
        pushLargeTreeProjectionIssue(errors, 'large_tree_rendered_start_mismatch', 'renderedStart',
            'Rendered start does not match projection start', {
                renderedStart: renderedStart,
                projectionStart: projection.start
            });
    }
    if (Number.isFinite(renderedEnd) && renderedEnd !== projection.end) {
        pushLargeTreeProjectionIssue(errors, 'large_tree_rendered_end_mismatch', 'renderedEnd',
            'Rendered end does not match projection end', {
                renderedEnd: renderedEnd,
                projectionEnd: projection.end
            });
    }
    if (!largeTreeProjectionNumbersEqual(
            projection.topSpacerHeight + projection.rowHeight + projection.bottomSpacerHeight,
            projection.totalHeight)) {
        pushLargeTreeProjectionIssue(errors, 'large_tree_projection_height_mismatch', 'projection.totalHeight',
            'Projection spacer and row heights must sum to total height', {
                topSpacerHeight: projection.topSpacerHeight,
                rowHeight: projection.rowHeight,
                bottomSpacerHeight: projection.bottomSpacerHeight,
                totalHeight: projection.totalHeight
            });
    }
    if (projection.totalHeight > 0 && projection.viewportHeight > 0 && projection.rows.length) {
        if (projection.mountedTop > projection.viewportTop + 0.01) {
            pushLargeTreeProjectionIssue(errors, 'large_tree_projection_misses_viewport_top', 'projection.mountedTop',
                'Projection mounted rows must cover viewport top', {
                    mountedTop: projection.mountedTop,
                    viewportTop: projection.viewportTop
                });
        }
        if (projection.mountedBottom + 0.01 < projection.viewportBottom) {
            pushLargeTreeProjectionIssue(errors, 'large_tree_projection_misses_viewport_bottom', 'projection.mountedBottom',
                'Projection mounted rows must cover viewport bottom', {
                    mountedBottom: projection.mountedBottom,
                    viewportBottom: projection.viewportBottom
                });
        }
    }

    var dom = collectTreeProjectionDomSnapshot(target);
    if (!dom.topSpacer) {
        pushLargeTreeProjectionIssue(errors, 'large_tree_top_spacer_missing', 'dom.spacer:top',
            'Large tree projection top spacer is missing');
    } else if (!largeTreeProjectionNumbersEqual(dom.topSpacer.height, projection.topSpacerHeight)) {
        pushLargeTreeProjectionIssue(errors, 'large_tree_top_spacer_height_mismatch', 'dom.spacer:top.height',
            'Large tree projection top spacer height does not match model', {
                actual: dom.topSpacer.height,
                expected: projection.topSpacerHeight
            });
    }
    if (!dom.bottomSpacer) {
        pushLargeTreeProjectionIssue(errors, 'large_tree_bottom_spacer_missing', 'dom.spacer:bottom',
            'Large tree projection bottom spacer is missing');
    } else if (!largeTreeProjectionNumbersEqual(dom.bottomSpacer.height, projection.bottomSpacerHeight)) {
        pushLargeTreeProjectionIssue(errors, 'large_tree_bottom_spacer_height_mismatch', 'dom.spacer:bottom.height',
            'Large tree projection bottom spacer height does not match model', {
                actual: dom.bottomSpacer.height,
                expected: projection.bottomSpacerHeight
            });
    }
    if (dom.rows.length !== projection.rows.length) {
        pushLargeTreeProjectionIssue(errors, 'large_tree_dom_row_count_mismatch', 'dom.rows.length',
            'Mounted large tree DOM row count does not match projection', {
                actual: dom.rows.length,
                expected: projection.rows.length
            });
    }
    for (var i = 0; i < projection.rows.length; i++) {
        var expected = projection.rows[i];
        var actual = dom.rows[i];
        if (!actual) continue;
        var expectedRenderSignature = virtualTreeProjectionRowRenderSignature(expected, state);
        var expectedTreeInstanceId = state && state.treeInstanceId || '';
        var expectedTreeModelVersion = String(state && state.treeModelVersion || 0);
        if (actual.rowIndex !== expected.index ||
                actual.rowKey !== expected.key ||
                actual.rowSignature !== expected.signature ||
                actual.signature !== expectedRenderSignature ||
                !largeTreeProjectionNumbersEqual(actual.top, expected.top) ||
                !largeTreeProjectionNumbersEqual(actual.height, expected.height)) {
            pushLargeTreeProjectionIssue(errors, 'large_tree_dom_row_mismatch', 'dom.rows[' + i + ']',
                'Mounted large tree DOM row does not match projection row', {
                    actual: actual,
                    expected: {
                        index: expected.index,
                        key: expected.key,
                        rowSignature: expected.signature,
                        renderSignature: expectedRenderSignature,
                        top: expected.top,
                        height: expected.height
                    }
                });
        }
        if (actual.treeInstanceId !== expectedTreeInstanceId) {
            pushLargeTreeProjectionIssue(errors, 'large_tree_dom_tree_instance_mismatch', 'dom.rows[' + i + '].treeInstanceId',
                'Mounted large tree DOM row belongs to a different tree instance', {
                    actual: actual.treeInstanceId,
                    expected: expectedTreeInstanceId
                });
        }
        if (actual.treeModelVersion !== expectedTreeModelVersion) {
            pushLargeTreeProjectionIssue(errors, 'large_tree_dom_tree_model_version_mismatch', 'dom.rows[' + i + '].treeModelVersion',
                'Mounted large tree DOM row belongs to a different tree model version', {
                    actual: actual.treeModelVersion,
                    expected: expectedTreeModelVersion
                });
        }
        if (rows[expected.index] !== expected.row) {
            pushLargeTreeProjectionIssue(errors, 'large_tree_projection_row_identity_mismatch', 'rows[' + expected.index + ']',
                'Projection row must reference the current logical row model', {
                    index: expected.index
                });
        }
    }
    return {
        ok: errors.length === 0,
        errors: errors,
        ruleKey: state && state.ruleKey || '',
        identity: treeMaterializerIdentityForState(state),
        rows: rows.length,
        totalHeight: projection.totalHeight,
        viewport: {
            top: projection.viewportTop,
            bottom: projection.viewportBottom,
            height: projection.viewportHeight
        },
        projection: {
            start: projection.start,
            end: projection.end,
            mountedTop: projection.mountedTop,
            mountedBottom: projection.mountedBottom,
            topSpacerHeight: projection.topSpacerHeight,
            bottomSpacerHeight: projection.bottomSpacerHeight,
            rowHeight: projection.rowHeight
        },
        dom: {
            rowCount: dom.rows.length,
            rowKeys: dom.rows.map(function(row) { return row.rowKey; }),
            rowSignatures: dom.rows.map(function(row) { return row.rowSignature; }),
            renderSignatures: dom.rows.map(function(row) { return row.signature; }),
            treeInstanceIds: dom.rows.map(function(row) { return row.treeInstanceId; }),
            treeModelVersions: dom.rows.map(function(row) { return row.treeModelVersion; }),
            topSpacerHeight: dom.topSpacer ? dom.topSpacer.height : null,
            bottomSpacerHeight: dom.bottomSpacer ? dom.bottomSpacer.height : null
        }
    };
}


function renderVirtualTreeRow(row, state) {
    if (row.kind === 'fields') return renderVirtualTreeMetaRow(row, state);
    return renderVirtualTreeNodeRow(row, state);
}

function renderFullTreeRowsFromModel(model, ruleKey, showFields, showPinned, query, scope, renderOptions) {
    var state = {
        ruleKey: ruleKey,
        showFields: !!showFields,
        showPinned: !!showPinned,
        query: query || '',
        statQuery: query || '',
        scope: scope || ''
    };
    applyTreeRenderOptionsToState(state, renderOptions);
    var html = '';
    var rows = model && model.rows || [];
    for (var i = 0; i < rows.length; i++) {
        html += '<div class="tree-full-row" data-tree-row="' + i + '">' +
            renderTreeModelRow(rows[i], state) +
            '</div>';
    }
    return html;
}

function renderTreeModelRow(row, state) {
    if (row.kind === 'fields') return renderTreeModelMetaRow(row, state);
    return renderTreeModelNodeRow(row, state);
}

function renderVirtualTreeNodeRow(row, state) {
    return renderTreeModelNodeRow(row, state);
}

function renderTreeModelNodeRow(row, state) {
    var nodeId = treeNodeId(state.ruleKey, row.pid);
    var searchContext = treeSearchContext(state.ruleKey, row.pid);
    var html = '<div id="' + nodeId + '" class="' + htmlEscape(treeNodeWrapperClass(state, row)) + '"' +
        treeRowDecorationAttrs(state, row) + '>';
    html += '<div class="tree-node-row"' + treeActionAttr('tree-row-reverse-blink', nodeId) + '>';
    var statQuery = state.statQuery !== undefined ? state.statQuery : state.query;
    var fieldChanges = treeRowDecorationFieldChanges(state, row);
    var diffSide = treeRowDecorationDiffSide(state, row);
    html += renderPinnedCells(row.node, state.showPinned, statQuery || '', state.scope,
        fieldChanges, diffSide, searchContext);
    if (!row.isRoot) {
        html += renderTreeNodeConnectors(row.parentPrefix, row.isLast ? 'elbow' : 'branch');
    }
    html += '<span class="tree-node-main">';
    if (row.hasChildren) {
        html += '<span class="tree-toggle" id="' + nodeId + '-tog" aria-expanded="' +
                (row.collapsed ? 'false' : 'true') + '"' +
                treeActionAttr('tree-toggle', nodeId) + '>' +
                (row.collapsed ? '\u25B6' : '\u25BC') + '</span>';
    } else {
        html += '<span class="tree-toggle leaf">\u2022</span>';
    }
    html += renderLabelSpan(
        row.node,
        nodeId,
        row.hasMeta,
        state.query,
        state.scope,
        searchContext,
        treeRowDecorationLabelHtml(state, row)
    );
    html += '</span></div></div>';
    return html;
}

/**
 * Hover affordance that promotes an attribute-backed metadata row to a stat
 * column. Rows without a field key (plain node metadata) are not pinnable.
 */
function treeMetaPinButtonHtml(fieldKey, availableFields) {
    fieldKey = String(fieldKey || '');
    if (!fieldKey || !availableFields[fieldKey]) return '';
    return '<button type="button" class="tree-meta-pin" ' +
           'data-trace-action="pin-node-column" ' +
           'data-node-column-key="' + htmlEscape(fieldKey) + '" ' +
           'title="Show as column" aria-label="Show as column">' +
           traceIconSvg('drawing-pin-filled') + '</button>';
}

function treeFieldRowPadHtml(fieldKey, availableFields) {
    return '<span class="tree-meta-pad">' +
           treeMetaPinButtonHtml(fieldKey, availableFields) +
           '</span>';
}

function renderVirtualTreeMetaRow(row, state) {
    return renderTreeModelMetaRow(row, state);
}

function renderTreeModelMetaRow(row, state) {
    var nodeId = treeNodeId(state.ruleKey, row.pid);
    var metaKey = treeFieldRowKey(row.meta);
    var metaValue = treeFieldRowValue(row.meta);
    var metaFieldKey = treeFieldRowFieldKey(row.meta);
    var keyPart = metaKey === metaFieldKey ? 'field' : 'key';
    var fieldChanges = treeRowDecorationFieldChanges(state, row);
    var diffSide = treeRowDecorationDiffSide(state, row);
    var searchBase = treeSearchContext(state.ruleKey, row.pid, {
        fieldKey: metaFieldKey,
        row: 'attr:' + row.metaIndex,
        metaIndex: row.metaIndex
    });
    var keyWidthCh = fieldRowKeyWidthCh(metaKey, row.keyAlignCh);
    var html = '<div class="tree-meta-block tree-meta-virtual-block' +
        (row.collapsed ? ' children-collapsed' : '') +
        '" id="' + nodeId + '-meta-' + row.metaIndex +
        '" style="--tree-meta-key-align:' + row.keyAlignCh + 'ch">';
    html += '<div class="tree-meta-row">';
    if (pinnedColumnCount() > 0) {
        for (var col = 0; col < pinnedColumnCount(); col++) {
            html += '<span class="pinned-val-cell"' + pinnedColumnCellAttrs(col) + '></span>';
        }
    }
    html += renderTreeMetaConnectors(row.metaPrefix, row.hasChildren, row.collapsed);
    html += treeFieldRowPadHtml(metaFieldKey, traceNodeFieldAvailableMap());
    html += '<button type="button" class="tree-meta-key tree-meta-key-action" ' +
            'style="--tree-meta-key-width:' + keyWidthCh + 'ch" ' +
            'title="' + htmlEscape(metaKey) + '" ' +
            'aria-label="Open field details for ' + htmlEscape(metaKey) + '"' +
            treeFieldDetailsActionAttrs(nodeId, row.metaIndex) + '>' +
            htmlEscape(metaKey) + ':&nbsp;</button>';
    var fieldChange = diffFieldChangeForKey(fieldChanges, metaFieldKey);
    html += '<span class="tree-meta-val" title="' + htmlEscape(metaValue) + '">' +
            diffFieldValueHtml(metaValue, fieldChange, diffSide, state.query, state.scope,
                searchContextWith(searchBase, { part: 'value' })) + '</span>';
    html += '</div></div>';
    return html;
}

function treePathIdFromNodeId(state, nodeId) {
    if (!state || !nodeId) return '';
    var prefix = 'tn-' + state.ruleKey + '-';
    return nodeId.indexOf(prefix) === 0 ? nodeId.substring(prefix.length) : '';
}

function treeRuleRefFromRuleKey(ruleKey) {
    var parts = normalizeTreeRuleKey(ruleKey).split('-');
    if (parts.length < 3) return null;

    var si = Number(parts[0]);
    var gi = Number(parts[1]);
    var ri = Number(parts[2]);
    if (!Number.isFinite(si) || !Number.isFinite(gi) || !Number.isFinite(ri)) return null;
    return { si: si, gi: gi, ri: ri };
}

function invalidateVirtualTreePeerRenders(state) {
    if (!state || String(state.ruleKey || '').indexOf('fs-') !== 0) return;
    var ref = treeRuleRefFromRuleKey(state && state.ruleKey);
    if (!ref) return;
    clearRuleRenderState(ref.si, ref.gi, ref.ri);
}

function saveVisibleTreeMaterializerAnchors() {
    var materializers = activeTreeMaterializers();
    for (var i = 0; i < materializers.length; i++) {
        saveTreeMaterializerPaneAnchor(materializers[i].state);
    }
}

function auditVisibleTreeMaterializers(reason) {
    var materializers = activeTreeMaterializers();
    var audit = {
        reason: reason || 'audit-visible',
        checked: 0,
        repaired: 0,
        invalid: []
    };
    for (var i = 0; i < materializers.length; i++) {
        if (treeMaterializerMode(materializers[i]) !== 'virtual') continue;
        var before = treeMaterializerViewportRenderStateForMaterializer(materializers[i]);
        audit.checked++;
        if (!treeMaterializerRenderStateIsInvalid(before)) continue;
        var after = ensureTreeViewportRowsMounted(materializers[i], reason || 'audit-visible');
        if (after && after.valid) {
            audit.repaired++;
        } else {
            audit.invalid.push({
                ruleKey: materializers[i].state && materializers[i].state.ruleKey || '',
                before: before,
                after: after || null
            });
        }
    }
    audit.valid = audit.invalid.length === 0;
    return audit;
}

function refreshVisibleTreeMaterializers() {
    return auditVisibleTreeMaterializers('refresh-visible');
}



function commonDiffPrefixLength(a, b) {
    var max = Math.min(a.length, b.length);
    var i = 0;
    while (i < max && a.charAt(i) === b.charAt(i)) i++;
    return i;
}

function commonDiffSuffixLength(a, b, prefixLength) {
    var max = Math.min(a.length, b.length) - prefixLength;
    var i = 0;
    while (i < max &&
           a.charAt(a.length - 1 - i) === b.charAt(b.length - 1 - i)) {
        i++;
    }
    return i;
}

function diffFieldChangeForKey(fieldChanges, key) {
    key = String(key || '');
    return fieldChanges && key &&
        Object.prototype.hasOwnProperty.call(fieldChanges, key)
        ? fieldChanges[key]
        : null;
}

function diffFieldSidePresent(change, side) {
    if (!change) return true;
    return side === 'a' ? change.presentA !== false : change.presentB !== false;
}

function diffFieldOtherValue(change, side) {
    if (!change) return '';
    return String(side === 'a' ? change.b || '' : change.a || '');
}

function diffFieldValueHtml(value, change, side, query, scope, searchContext) {
    value = String(value == null ? '' : value);
    if (!change || !(side === 'a' || side === 'b')) {
        return htmlEscape(value);
    }

    var cls = side === 'a' ? 'diff-label-del' : 'diff-label-add';
    if (!diffFieldSidePresent(change, side)) {
        return '';
    }

    var other = diffFieldOtherValue(change, side);
    if (value === other) {
        return '<span class="' + cls + '">' + (value ? htmlEscape(value) : '(empty)') + '</span>';
    }

    var prefixLength = commonDiffPrefixLength(value, other);
    var suffixLength = commonDiffSuffixLength(value, other, prefixLength);
    var prefix = value.substring(0, prefixLength);
    var middle = value.substring(prefixLength, value.length - suffixLength);
    var suffix = suffixLength ? value.substring(value.length - suffixLength) : '';

    if (!middle) {
        return '<span class="' + cls + '">' + (value ? htmlEscape(value) : '(empty)') + '</span>';
    }

    return htmlEscape(prefix) +
        (middle ? '<span class="' + cls + '">' + htmlEscape(middle) + '</span>' : '') +
        htmlEscape(suffix);
}

/** Render stat value cells for a node row. */
function renderPinnedCells(node, showPinned, query, scope, fieldChanges, diffSide, searchContext) {
    if (pinnedColumnCount() === 0 || !showPinned) return '';
    var html = '';
    var rows = treeFieldRows(node);
    for (var col = 0; col < pinnedColumnCount(); col++) {
        var key = tracePinnedColumnKey(col);
        var rawValue = traceNodeColumnValue(node, col);
        var change = diffFieldChangeForKey(fieldChanges, key);
        var rowKey = '';
        for (var rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            if (treeFieldRowFieldKey(rows[rowIndex]) === key) {
                rowKey = 'attr:' + treeFieldRowMetaIndex(rows[rowIndex], rowIndex);
                break;
            }
        }
        var valueContext = searchContextWith(searchContext, {
            fieldKey: key,
            row: rowKey,
            part: 'value',
            pinnedSurface: true
        });
        var val = rawValue || change
            ? diffFieldValueHtml(rawValue, change, diffSide, query, scope, valueContext)
            : '';
        var title = rawValue ? ' title="' + htmlEscape(rawValue) + '"' : '';
        html += '<span class="pinned-val-cell"' + pinnedColumnCellAttrs(col) + title + '>' + val + '</span>';
    }
    return html;
}

/** Render the label span (clickable if metadata is present). */
function renderLabelSpan(node, nodeId, hasMeta, query, scope, searchContext, labelHtmlOverride) {
    var label = labelHtmlOverride !== undefined && labelHtmlOverride !== null
        ? labelHtmlOverride
        : treeHighlight(node.l, query, scope, 'label', searchContext);
    if (hasMeta) {
        return '<span class="tree-label clickable"' +
               treeActionAttr('tree-meta-toggle', nodeId) + '>' +
               label + '</span>';
    }
    return '<span class="tree-label">' + label + '</span>';
}


function metaTextLength(value) {
    return String(value == null ? '' : value).length;
}

function metaKeyCh(value) {
    return metaTextLength(value) + 2;
}

function fieldRowsBlockKeyAlignCh(rows) {
    var maxCh = 0;
    for (var i = 0; rows && i < rows.length; i++) {
        maxCh = Math.max(maxCh, metaKeyCh(treeFieldRowKey(rows[i])));
    }
    return Math.min(maxCh, TREE_META_KEY_ALIGN_CAP_CH);
}

function fieldRowKeyWidthCh(value, alignCh) {
    return Math.max(metaKeyCh(value), alignCh);
}

/** Toggle tree node expand/collapse. */
function treeToggle(event, nodeId) {
    TraceActions.tree.toggle(nodeId, event);
}

function parseNormalTreeNodeId(nodeId) {
    if (!nodeId || nodeId.indexOf('tn-') !== 0) return null;

    var parts = nodeId.substring(3).split('-');
    if (parts[0] === 'fs') parts.shift();
    if (parts.length < 4) return null;

    var si = Number(parts[0]);
    var gi = Number(parts[1]);
    var ri = Number(parts[2]);
    if (!Number.isFinite(si) || !Number.isFinite(gi) || !Number.isFinite(ri)) {
        return null;
    }

    var path = [];
    for (var i = 3; i < parts.length; i++) {
        var segment = Number(parts[i]);
        if (!Number.isFinite(segment)) return null;
        path.push(segment);
    }
    if (!path.length || path[0] !== 0) return null;

    return { si: si, gi: gi, ri: ri, path: path };
}

function parseScopedTreeNodeId(nodeId) {
    if (!nodeId || nodeId.indexOf('tn-') !== 0) return null;

    var parts = nodeId.substring(3).split('-');
    var lastScopePart = -1;
    for (var i = 0; i < parts.length; i++) {
        var segment = Number(parts[i]);
        if (!Number.isFinite(segment)) lastScopePart = i;
    }
    if (lastScopePart < 0 || lastScopePart >= parts.length - 1) return null;

    var pathParts = parts.slice(lastScopePart + 1);
    if (!pathParts.length || pathParts[0] !== '0') return null;
    for (var pi = 0; pi < pathParts.length; pi++) {
        if (!Number.isFinite(Number(pathParts[pi]))) return null;
    }

    var scopedRuleKey = parts.slice(0, lastScopePart + 1).join('-');
    if (!scopedRuleKey) return null;
    return {
        ruleKey: scopedRuleKey,
        path: pathParts.map(function(part) { return Number(part); })
    };
}

function treeSessionForNodeId(nodeId) {
    var ref = parseNormalTreeNodeId(nodeId);
    if (ref) {
        var key = ruleKey(ref.si, ref.gi, ref.ri);
        return {
            ref: ref,
            ruleKey: key,
            pid: treePathId(ref.path),
            session: treeSessionForRule(key)
        };
    }

    var scoped = parseScopedTreeNodeId(nodeId);
    if (!scoped) return null;
    return {
        ref: null,
        ruleKey: scoped.ruleKey,
        pid: treePathId(scoped.path),
        session: treeSessionForRule(scoped.ruleKey)
    };
}

function treeNodeForPath(root, path) {
    var node = root;
    for (var i = 1; i < path.length; i++) {
        if (!node || !node.c || !node.c[path[i]]) return null;
        node = node.c[path[i]];
    }
    return node;
}

/** Toggle field-row visibility for a single tree node. */
function treeMetaToggle(event, nodeId) {
    TraceActions.tree.toggleMeta(nodeId, event);
}
