/* ══════════════════════════════════════════════════════════════
   Rule-level actions (fields, pinned fields, info toggles)
   ══════════════════════════════════════════════════════════════ */

var ruleInfoSurface = createVisualSurface({
    name: 'rule-info',
    epochScopes: ['trace', 'render'],
    domGenerations: ['virtualRows'],
    resolveTarget: function(ref, options) {
        if (options && options.target) return options.target;
        if (!ref || !hasDOM() || !document.getElementById) return null;
        return document.getElementById('info-content-' + ruleKey(ref.si, ref.gi, ref.ri));
    }
});

var INFO_FIELD_DETAILS_TAB_ID = '__field_details__';
var INFO_FIELD_DETAILS_PINNED_TAB_PREFIX = '__field_details_pinned__';

function infoRenderKey(si, gi, ri, options) {
    return (options && options.idScope ? options.idScope + '-' : '') + ruleKey(si, gi, ri);
}

function infoContentElement(si, gi, ri, options) {
    if (!hasDOM() || !document.getElementById) return null;
    return document.getElementById('info-content-' + infoRenderKey(si, gi, ri, options));
}

function infoScrollPaneForContent(container) {
    if (!container || !container.closest) return null;
    var panel = container.closest('.rule-info-panel');
    if (!panel) return null;
    if (typeof effectiveInfoScrollPane === 'function') {
        return effectiveInfoScrollPane(panel) || panel;
    }
    return panel;
}

function clampInfoScroll(value, max) {
    value = Number(value) || 0;
    max = Math.max(0, Number(max) || 0);
    return Math.max(0, Math.min(max, value));
}

function infoScrollMax(el, axis) {
    if (!el) return 0;
    if (axis === 'left') {
        return Math.max(0, (Number(el.scrollWidth) || 0) - (Number(el.clientWidth) || 0));
    }
    return Math.max(0, (Number(el.scrollHeight) || 0) - (Number(el.clientHeight) || 0));
}

function infoElementAnchorKey(el) {
    if (!el || !el.classList) return null;
    if (el.id) return { type: 'id', value: el.id };
    if (el.classList.contains('info-tabs')) return { type: 'tabs' };
    if (el.classList.contains('info-tab-panel')) return { type: 'tab-panel' };

    var switcher = el.classList.contains('info-switcher')
        ? el
        : (el.closest ? el.closest('.info-switcher[data-info-switcher-key]') : null);
    if (!switcher || !switcher.getAttribute) return null;

    var key = switcher.getAttribute('data-info-switcher-key') || '';
    if (!key) return null;
    if (el.classList.contains('info-switcher-head')) return { type: 'switcher-head', value: key };
    if (el.classList.contains('info-switcher-selector')) return { type: 'switcher-selector', value: key };
    return { type: 'switcher', value: key };
}

function addInfoAnchorCandidate(result, seen, el) {
    if (!el || seen.indexOf(el) >= 0) return;
    seen.push(el);
    result.push(el);
}

function infoAnchorCandidates(pane) {
    var result = [];
    var seen = [];
    addInfoAnchorCandidate(result, seen, pane);
    if (!pane || !pane.querySelectorAll) return result;

    var nodes = pane.querySelectorAll(
        '[id^="info-section-"], .info-tabs, .info-tab-panel, ' +
        '.info-switcher[data-info-switcher-key], .info-switcher-head, .info-switcher-selector'
    );
    for (var i = 0; i < nodes.length; i++) {
        addInfoAnchorCandidate(result, seen, nodes[i]);
    }
    return result;
}

function infoAnchorVisible(rect, viewport) {
    return !!(rect && viewport && rect.bottom >= viewport.top && rect.top <= viewport.bottom);
}

function captureInfoScrollSnapshot(container) {
    var pane = infoScrollPaneForContent(container);
    if (!pane) return null;

    var snapshot = {
        left: pane.scrollLeft || 0,
        top: pane.scrollTop || 0,
        anchor: null
    };
    if (!pane.getBoundingClientRect) return snapshot;

    var viewport = pane.getBoundingClientRect();
    if (!viewport) return snapshot;

    var candidates = infoAnchorCandidates(pane);
    var best = null;
    for (var i = 0; i < candidates.length; i++) {
        var el = candidates[i];
        if (!el || !el.getBoundingClientRect) continue;
        if (el !== pane && pane.contains && !pane.contains(el)) continue;
        var key = infoElementAnchorKey(el);
        if (!key) continue;
        var rect = el.getBoundingClientRect();
        if (!infoAnchorVisible(rect, viewport)) continue;
        var score = Math.abs(rect.top - viewport.top);
        if (!best || score < best.score) {
            best = {
                key: key,
                offsetTop: rect.top - viewport.top,
                score: score
            };
        }
    }
    if (best) {
        snapshot.anchor = {
            key: best.key,
            offsetTop: best.offsetTop
        };
    }
    return snapshot;
}

function findInfoSwitcherElement(container, switcherKey) {
    if (!container || !container.querySelectorAll) return null;
    var switchers = container.querySelectorAll('.info-switcher[data-info-switcher-key]');
    switcherKey = String(switcherKey || '');
    for (var i = 0; i < switchers.length; i++) {
        if ((switchers[i].getAttribute('data-info-switcher-key') || '') === switcherKey) {
            return switchers[i];
        }
    }
    return null;
}

function findInfoScrollAnchor(container, anchor) {
    if (!container || !anchor || !anchor.key) return null;
    var key = anchor.key;
    if (key.type === 'id') {
        var byId = hasDOM() && document.getElementById ? document.getElementById(key.value) : null;
        return byId && container.contains && container.contains(byId) ? byId : null;
    }
    if (key.type === 'tabs') {
        return container.querySelector ? container.querySelector('.info-tabs') : null;
    }
    if (key.type === 'tab-panel') {
        return container.querySelector ? container.querySelector('.info-tab-panel') : null;
    }

    var switcher = findInfoSwitcherElement(container, key.value);
    if (!switcher) return null;
    if (key.type === 'switcher-head') {
        return switcher.querySelector ? switcher.querySelector('.info-switcher-head') || switcher : switcher;
    }
    if (key.type === 'switcher-selector') {
        return switcher.querySelector ? switcher.querySelector('.info-switcher-selector') || switcher : switcher;
    }
    return switcher;
}

function restoreInfoScrollSnapshot(container, snapshot) {
    if (!snapshot) return false;
    var pane = infoScrollPaneForContent(container);
    if (!pane) return false;

    pane.scrollLeft = clampInfoScroll(snapshot.left, infoScrollMax(pane, 'left'));
    var restoredTop = false;
    var anchorEl = findInfoScrollAnchor(container, snapshot.anchor);
    if (anchorEl && anchorEl !== pane && anchorEl.getBoundingClientRect && pane.getBoundingClientRect) {
        var viewport = pane.getBoundingClientRect();
        var rect = anchorEl.getBoundingClientRect();
        var delta = (rect.top - viewport.top) - (Number(snapshot.anchor.offsetTop) || 0);
        pane.scrollTop = clampInfoScroll((pane.scrollTop || 0) + delta, infoScrollMax(pane, 'top'));
        restoredTop = true;
    }
    if (!restoredTop) {
        pane.scrollTop = clampInfoScroll(snapshot.top, infoScrollMax(pane, 'top'));
    }
    return true;
}

function ruleInfoTabSessionKey(si, gi, ri) {
    if (typeof currentRuleSessionKey === 'function') {
        var key = currentRuleSessionKey(ruleRef(si, gi, ri));
        if (key) return key;
    }
    var traceIndex = 0;
    try {
        traceIndex = Math.max(0, Number(traceRuntime().activeTraceIndex) || 0);
    } catch (err) {}
    return traceIndex + ':' + ruleKey(si, gi, ri);
}

function ruleInfoTabSession(si, gi, ri) {
    var trace = null;
    try { trace = traceRuntime(); } catch (err) {}
    if (!trace) return null;
    if (!trace.ruleInfoTabSessions) trace.ruleInfoTabSessions = {};
    var key = ruleInfoTabSessionKey(si, gi, ri);
    if (!trace.ruleInfoTabSessions[key]) trace.ruleInfoTabSessions[key] = {};
    return trace.ruleInfoTabSessions[key];
}

function existingRuleInfoTabSession(si, gi, ri) {
    var trace = null;
    try { trace = traceRuntime(); } catch (err) {}
    if (!trace || !trace.ruleInfoTabSessions) return null;
    return trace.ruleInfoTabSessions[ruleInfoTabSessionKey(si, gi, ri)] || null;
}

function ruleInfoSwitcherSession(si, gi, ri) {
    var session = ruleInfoTabSession(si, gi, ri);
    if (!session) return null;
    if (!session.switchers) session.switchers = {};
    return session.switchers;
}

function copyNumberPath(path) {
    var result = [];
    path = Array.isArray(path) ? path : [];
    for (var i = 0; i < path.length; i++) {
        var value = Number(path[i]);
        if (!Number.isInteger(value)) return [];
        result.push(value);
    }
    return result;
}

function infoFieldDetailsAnchorId(si, gi, ri, metaIndex, options) {
    var scopePrefix = options && options.idScope ? options.idScope + '-' : '';
    return 'info-meta-detail-' + scopePrefix + si + '-' + gi + '-' + ri + '-' + metaIndex;
}

function infoFieldDetailsMetaIndex(metaIndex) {
    metaIndex = Math.max(0, Math.floor(Number(metaIndex)));
    return Number.isFinite(metaIndex) ? metaIndex : 0;
}

function infoFieldDetailsPinnedTabId(nodePath, metaIndex) {
    var path = copyNumberPath(nodePath);
    var pathKey = path.length ? path.join('-') : 'root';
    return INFO_FIELD_DETAILS_PINNED_TAB_PREFIX + pathKey + '-' + infoFieldDetailsMetaIndex(metaIndex);
}

function infoFieldDetailsEntryCopy(state, pinned) {
    state = state || {};
    var path = copyNumberPath(state.nodePath);
    if (!path.length || path[0] !== 0) return null;

    var metaIndex = infoFieldDetailsMetaIndex(state.metaIndex);
    var id = pinned
        ? (state.id || infoFieldDetailsPinnedTabId(path, metaIndex))
        : INFO_FIELD_DETAILS_TAB_ID;
    return {
        id: String(id),
        nodePath: path,
        metaIndex: metaIndex,
        previousTabId: state.previousTabId || '',
        pinned: !!pinned
    };
}

function ruleInfoFieldDetailsState(si, gi, ri) {
    var session = ruleInfoTabSession(si, gi, ri);
    return session && session.fieldDetails || null;
}

function ruleInfoPinnedFieldDetailsStates(si, gi, ri) {
    var session = ruleInfoTabSession(si, gi, ri);
    return session && Array.isArray(session.pinnedFieldDetails)
        ? session.pinnedFieldDetails
        : [];
}

function ruleInfoFieldDetailsStates(si, gi, ri) {
    var states = [];
    var pinned = ruleInfoPinnedFieldDetailsStates(si, gi, ri);
    for (var i = 0; i < pinned.length; i++) {
        var pinnedState = infoFieldDetailsEntryCopy(pinned[i], true);
        if (pinnedState) states.push(pinnedState);
    }

    var draft = infoFieldDetailsEntryCopy(ruleInfoFieldDetailsState(si, gi, ri), false);
    if (draft) states.push(draft);
    return states;
}

function ruleHasOpenInfoDetails(ref) {
    if (!ref) return false;
    var session = existingRuleInfoTabSession(ref.si, ref.gi, ref.ri);
    if (!session) return false;

    if (infoFieldDetailsEntryCopy(session.fieldDetails, false)) return true;
    var pinned = Array.isArray(session.pinnedFieldDetails) ? session.pinnedFieldDetails : [];
    for (var i = 0; i < pinned.length; i++) {
        if (infoFieldDetailsEntryCopy(pinned[i], true)) return true;
    }
    return false;
}

function ruleInfoFieldDetailsStateById(si, gi, ri, tabId) {
    tabId = String(tabId || '');
    var states = ruleInfoFieldDetailsStates(si, gi, ri);
    for (var i = 0; i < states.length; i++) {
        if (states[i].id === tabId) return states[i];
    }
    return null;
}

function modelHasInfoTabId(model, tabId) {
    if (!model || !model.tabs) return false;
    tabId = String(tabId || '');
    for (var i = 0; i < model.tabs.length; i++) {
        if (model.tabs[i].id === tabId) return true;
    }
    return false;
}

function setRuleInfoFieldDetails(si, gi, ri, nodePath, metaIndex, ev) {
    if (ev === undefined && metaIndex &&
            (metaIndex.stopPropagation || metaIndex.preventDefault)) {
        ev = metaIndex;
    }
    if (ev && ev.stopPropagation) ev.stopPropagation();
    if (ev && ev.preventDefault) ev.preventDefault();
    if (!validRuleRef(si, gi, ri)) return false;

    var path = copyNumberPath(nodePath);
    if (!path.length || path[0] !== 0) return false;

    var session = ruleInfoTabSession(si, gi, ri);
    if (!session) return false;
    metaIndex = infoFieldDetailsMetaIndex(metaIndex);
    var previous = session.activeTabId && session.activeTabId !== INFO_FIELD_DETAILS_TAB_ID
        ? session.activeTabId
        : (session.fieldDetails && session.fieldDetails.previousTabId || '');
    session.fieldDetails = {
        nodePath: path,
        metaIndex: metaIndex,
        previousTabId: previous
    };
    session.activeTabId = INFO_FIELD_DETAILS_TAB_ID;

    var rule = ruleState(si, gi, ri);
    if (rule) rule.infoRendered = null;
    return true;
}

function pinRuleInfoFieldDetails(si, gi, ri, tabId, ev) {
    if (ev === undefined && tabId &&
            (tabId.stopPropagation || tabId.preventDefault)) {
        ev = tabId;
        tabId = '';
    }
    if (ev && ev.stopPropagation) ev.stopPropagation();
    if (ev && ev.preventDefault) ev.preventDefault();
    if (!validRuleRef(si, gi, ri)) return false;

    tabId = String(tabId || INFO_FIELD_DETAILS_TAB_ID);
    if (tabId !== INFO_FIELD_DETAILS_TAB_ID) return false;

    var session = ruleInfoTabSession(si, gi, ri);
    var draft = infoFieldDetailsEntryCopy(session && session.fieldDetails, false);
    if (!session || !draft) return false;

    var pinned = infoFieldDetailsEntryCopy({
        id: infoFieldDetailsPinnedTabId(draft.nodePath, draft.metaIndex),
        nodePath: draft.nodePath,
        metaIndex: draft.metaIndex,
        previousTabId: draft.previousTabId
    }, true);
    if (!pinned) return false;

    if (!Array.isArray(session.pinnedFieldDetails)) session.pinnedFieldDetails = [];
    var found = false;
    for (var i = 0; i < session.pinnedFieldDetails.length; i++) {
        if (session.pinnedFieldDetails[i] && session.pinnedFieldDetails[i].id === pinned.id) {
            found = true;
            break;
        }
    }
    if (!found) session.pinnedFieldDetails.push(pinned);

    delete session.fieldDetails;
    session.activeTabId = pinned.id;

    var rule = ruleState(si, gi, ri);
    if (rule) rule.infoRendered = null;
    return true;
}

function closeRuleInfoFieldDetails(si, gi, ri, tabId, ev) {
    if (ev === undefined && tabId &&
            (tabId.stopPropagation || tabId.preventDefault)) {
        ev = tabId;
        tabId = '';
    }
    if (ev && ev.stopPropagation) ev.stopPropagation();
    if (ev && ev.preventDefault) ev.preventDefault();
    if (!validRuleRef(si, gi, ri)) return false;

    var session = ruleInfoTabSession(si, gi, ri);
    if (!session) return false;

    var activeBefore = session.activeTabId || '';
    tabId = String(tabId || '');
    if (!tabId) {
        tabId = ruleInfoFieldDetailsStateById(si, gi, ri, activeBefore)
            ? activeBefore
            : INFO_FIELD_DETAILS_TAB_ID;
    }

    var removed = null;
    if (tabId === INFO_FIELD_DETAILS_TAB_ID) {
        removed = infoFieldDetailsEntryCopy(session.fieldDetails, false);
        if (!removed) return false;
        delete session.fieldDetails;
    } else {
        var pinned = Array.isArray(session.pinnedFieldDetails) ? session.pinnedFieldDetails : [];
        for (var i = 0; i < pinned.length; i++) {
            var candidate = infoFieldDetailsEntryCopy(pinned[i], true);
            if (candidate && candidate.id === tabId) {
                removed = candidate;
                pinned.splice(i, 1);
                break;
            }
        }
        if (!removed) return false;
        session.pinnedFieldDetails = pinned;
    }

    var previous = removed.previousTabId || '';
    var sections = traceRuleInfo(si, rawRuleIndex(si, gi, ri));
    var model = infoTabsModel(sections, si, gi, ri);
    if (activeBefore !== removed.id && modelHasInfoTabId(model, activeBefore)) {
        session.activeTabId = activeBefore;
    } else if (modelHasInfoTabId(model, previous)) {
        session.activeTabId = previous;
    } else {
        session.activeTabId = model && model.tabs && model.tabs.length ? model.tabs[0].id : '';
    }

    var rule = ruleState(si, gi, ri);
    if (rule) {
        rule.infoRendered = null;
        if (!ruleHasInfo(ruleRef(si, gi, ri))) rule.info = false;
    }
    return true;
}

function infoTabId(section, index) {
    section = section || {};
    return section.id !== undefined && section.id !== null && section.id !== ''
        ? String(section.id)
        : 'tab-' + String(index);
}

function infoSwitcherSectionPath(parentPath, sectionIndex, domIndex, sharedIndex) {
    var base = parentPath ? String(parentPath) : '';
    var local = sharedIndex
        ? String(sectionIndex) + '-' + String(domIndex)
        : String(sectionIndex);
    return base ? base + '/' + local : local;
}

function infoSwitcherDescendantPath(switcherPath, optionId) {
    return String(switcherPath || '') + '@' + String(optionId || '');
}

function infoSwitcherKey(section, index, path) {
    section = section || {};
    return section.id !== undefined && section.id !== null && section.id !== ''
        ? String(section.id)
        : 'switcher-section-' + String(path !== undefined && path !== null && path !== '' ? path : index);
}

function infoSwitcherOptionId(switcher, index) {
    switcher = switcher || {};
    return switcher.id !== undefined && switcher.id !== null && switcher.id !== ''
        ? String(switcher.id)
        : 'option-' + String(index);
}

function infoSwitcherOptionById(section, switcherId) {
    var switchers = section && section.options || [];
    switcherId = String(switcherId || '');
    for (var i = 0; i < switchers.length; i++) {
        if (infoSwitcherOptionId(switchers[i], i) === switcherId) return switchers[i];
    }
    return null;
}

function activeInfoSwitcherId(section, si, gi, ri, sectionIndex, switcherPath) {
    var switchers = section && section.options || [];
    if (!switchers.length) return '';
    var key = infoSwitcherKey(section, sectionIndex, switcherPath);
    var session = ruleInfoSwitcherSession(si, gi, ri);
    var active = session && session[key] || '';
    if (active && infoSwitcherOptionById(section, active)) return active;

    active = section.defaultOptionId && infoSwitcherOptionById(section, section.defaultOptionId)
        ? String(section.defaultOptionId)
        : infoSwitcherOptionId(switchers[0], 0);
    if (session) session[key] = active;
    return active;
}

function findInfoSwitcherSection(sections, switcherKey, inheritedIndex, switcherPath, switcherDepth, railOwner) {
    sections = sections || [];
    switcherKey = String(switcherKey || '');
    switcherDepth = Number.isFinite(Number(switcherDepth)) ? Number(switcherDepth) : 0;
    for (var i = 0; i < sections.length; i++) {
        var section = sections[i] || {};
        // Switcher keys are derived from the top-level section index, matching
        // the search index enumeration (sections nested in tabs/switchers
        // inherit their containing top-level section's index).
        var sectionIndex = inheritedIndex !== undefined ? inheritedIndex : i;
        var sectionPath = infoSwitcherSectionPath(
            switcherPath,
            sectionIndex,
            i,
            inheritedIndex !== undefined
        );
        var sectionSwitcherKey = section.type === 'switcher'
            ? infoSwitcherKey(section, sectionIndex, sectionPath)
            : '';
        if (section.type === 'switcher' && sectionSwitcherKey === switcherKey) {
            return {
                section: section,
                index: sectionIndex,
                path: sectionPath,
                depth: switcherDepth,
                key: sectionSwitcherKey,
                railOwner: switcherDepth > 0 ? (railOwner || null) : null
            };
        }
        if (section.type === 'tab') {
            var nested = findInfoSwitcherSection(
                section.sections || [],
                switcherKey,
                sectionIndex,
                switcherPath,
                switcherDepth,
                railOwner
            );
            if (nested) return nested;
        } else if (section.type === 'switcher') {
            var nextRailOwner = railOwner || {
                section: section,
                index: sectionIndex,
                path: sectionPath,
                depth: switcherDepth,
                key: sectionSwitcherKey
            };
            var switchers = section.options || [];
            for (var v = 0; v < switchers.length; v++) {
                var widgets = switchers[v] && Array.isArray(switchers[v].widgets) && switchers[v].widgets.length
                    ? switchers[v].widgets
                    : (switchers[v] && switchers[v].widget ? [switchers[v].widget] : []);
                var optionPath = infoSwitcherDescendantPath(
                    sectionPath,
                    infoSwitcherOptionId(switchers[v], v)
                );
                var child = findInfoSwitcherSection(
                    widgets,
                    switcherKey,
                    sectionIndex,
                    optionPath,
                    switcherDepth + 1,
                    nextRailOwner
                );
                if (child) return child;
            }
        }
    }
    return null;
}

function setRuleInfoSwitcher(si, gi, ri, switcherKey, switcherId, ev) {
    if (ev && ev.stopPropagation) ev.stopPropagation();
    if (ev && ev.preventDefault) ev.preventDefault();
    if (!validRuleRef(si, gi, ri)) return false;

    var sections = traceRuleInfo(si, rawRuleIndex(si, gi, ri));
    var found = findInfoSwitcherSection(sections, switcherKey);
    if (!found || !found.section) return false;

    var switchers = found.section.options || [];
    if (!switchers.length) return false;
    switcherId = String(switcherId || '');
    if (!infoSwitcherOptionById(found.section, switcherId)) {
        switcherId = infoSwitcherOptionId(switchers[0], 0);
    }

    var session = ruleInfoSwitcherSession(si, gi, ri);
    if (session && session[switcherKey] === switcherId) return false;
    if (session) session[switcherKey] = switcherId;

    var current = currentInfoQueryScope();
    var renderSwitcherKey = found.railOwner && found.railOwner.key ? found.railOwner.key : switcherKey;
    if (!renderRuleInfoSwitcherIsland(si, gi, ri, renderSwitcherKey, current.query, current.scope)) {
        var rule = ruleState(si, gi, ri);
        if (rule) rule.infoRendered = null;
        renderRuleInfoPanel(si, gi, ri, current.query, current.scope);
    }
    if (typeof rerenderFullscreenInfoPanel === 'function') {
        rerenderFullscreenInfoPanel(si, gi, ri);
    }
    return true;
}

function infoFieldDetailsNode(si, gi, ri, state) {
    if (!state || !Array.isArray(state.nodePath)) return null;
    var tree = tracePlanTree(si, rawRuleIndex(si, gi, ri));
    return treeNodeForPath(tree, state.nodePath);
}

function infoFieldDetailsRowMetaIndex(row, fallback) {
    var index = row && row.metaIndex !== undefined ? Number(row.metaIndex) : Number(fallback);
    if (!Number.isFinite(index)) return Math.max(0, Math.floor(Number(fallback) || 0));
    return Math.max(0, Math.floor(index));
}

function infoFieldDetailsSections(si, gi, ri, state) {
    var node = infoFieldDetailsNode(si, gi, ri, state);
    if (!node) {
        return [{
            type: 'text',
            title: 'Details',
            content: 'Field details are unavailable for this node.'
        }];
    }

    var rows = TraceStore.nodeFieldRows(currentTraceStore(), node);
    var plainRows = [];
    var sections = [{
        type: 'fieldDetailsHeader',
        label: 'Operator',
        nodeLabel: node.op || node.l || ''
    }];

    for (var i = 0; i < rows.length; i++) {
        var row = rows[i];
        if (TraceStore.fieldRowHasDetails(row)) {
            sections.push({
                type: 'fieldDetailsGroup',
                key: TraceStore.fieldRowKey(row),
                fieldKey: TraceStore.fieldRowFieldKey(row),
                metaIndex: infoFieldDetailsRowMetaIndex(row, i),
                details: TraceStore.fieldRowDetails(row)
            });
        } else {
            plainRows.push({
                key: TraceStore.fieldRowKey(row),
                fieldKey: TraceStore.fieldRowFieldKey(row),
                value: TraceStore.fieldRowValue(row),
                metaIndex: infoFieldDetailsRowMetaIndex(row, i)
            });
        }
    }

    if (plainRows.length) {
        sections.splice(1, 0, {
            type: 'fieldDetailsTable',
            rows: plainRows
        });
    }
    return sections;
}

function infoFieldDetailsNodeTarget(node) {
    var nodeId = node && node.id !== undefined ? String(node.id) : '';
    return nodeId ? { type: 'node', nodeId: nodeId } : null;
}

function infoFieldDetailsTab(si, gi, ri, state) {
    if (!state) return null;
    var node = infoFieldDetailsNode(si, gi, ri, state);
    var target = infoFieldDetailsNodeTarget(node);
    return {
        id: state.id || INFO_FIELD_DETAILS_TAB_ID,
        title: node && node.op ? String(node.op) : 'Details',
        temporary: !state.pinned,
        fieldDetails: true,
        fieldDetailsPinned: !!state.pinned,
        pinnable: !state.pinned,
        closeable: true,
        metaIndex: state.metaIndex,
        targets: target ? [target] : [],
        primaryTarget: target,
        hoverMode: 'preview',
        sections: infoFieldDetailsSections(si, gi, ri, state)
    };
}

function infoTabsModel(sections, si, gi, ri) {
    sections = sections || [];
    var hasTabs = false;
    var loose = [];
    var looseIndices = [];
    var tabs = [];
    var fieldDetailsTabs = [];
    if (si !== undefined) {
        var fieldDetailsStates = ruleInfoFieldDetailsStates(si, gi, ri);
        for (var s = 0; s < fieldDetailsStates.length; s++) {
            var fieldDetailsTab = infoFieldDetailsTab(si, gi, ri, fieldDetailsStates[s]);
            if (fieldDetailsTab) fieldDetailsTabs.push(fieldDetailsTab);
        }
    }

    for (var i = 0; i < sections.length; i++) {
        var section = sections[i] || {};
        if (section.type === 'tab') {
            hasTabs = true;
            if (!TraceSchema.infoSectionsHaveContent(section.sections || [])) continue;
            tabs.push({
                id: infoTabId(section, i),
                title: String(section.title || ('Tab ' + (tabs.length + 1))),
                sections: section.sections || [],
                targets: Array.isArray(section.targets) ? section.targets : [],
                primaryTarget: section.primaryTarget || null,
                hoverMode: section.hoverMode || 'preview',
                topIndex: i
            });
        } else if (TraceSchema.infoSectionsHaveContent([section])) {
            loose.push(section);
            looseIndices.push(i);
        }
    }

    if (!hasTabs && !fieldDetailsTabs.length) return null;
    if (loose.length) {
        tabs.unshift({
            id: '__info__',
            title: 'Info',
            sections: loose,
            sectionIndices: looseIndices
        });
    }
    for (var t = 0; t < fieldDetailsTabs.length; t++) {
        tabs.push(fieldDetailsTabs[t]);
    }
    return tabs.length ? { tabs: tabs } : null;
}

function activeInfoTabId(sections, si, gi, ri) {
    var model = infoTabsModel(sections, si, gi, ri);
    if (!model) return '';
    var session = ruleInfoTabSession(si, gi, ri);
    var active = session && session.activeTabId || '';
    for (var i = 0; i < model.tabs.length; i++) {
        if (model.tabs[i].id === active) return active;
    }
    active = model.tabs[0].id;
    if (session) session.activeTabId = active;
    return active;
}

function setRuleInfoTab(si, gi, ri, tabId, ev) {
    if (ev && ev.stopPropagation) ev.stopPropagation();
    if (ev && ev.preventDefault) ev.preventDefault();
    if (!validRuleRef(si, gi, ri)) return false;
    var sections = traceRuleInfo(si, rawRuleIndex(si, gi, ri));
    var model = infoTabsModel(sections, si, gi, ri);
    if (!model) return false;

    tabId = String(tabId || '');
    var valid = false;
    for (var i = 0; i < model.tabs.length; i++) {
        if (model.tabs[i].id === tabId) {
            valid = true;
            break;
        }
    }
    if (!valid) tabId = model.tabs[0].id;

    var session = ruleInfoTabSession(si, gi, ri);
    if (session && session.activeTabId === tabId) return false;
    if (session) session.activeTabId = tabId;

    var current = currentInfoQueryScope();
    if (!renderRuleInfoTabIsland(si, gi, ri, tabId, current.query, current.scope)) {
        var rule = ruleState(si, gi, ri);
        if (rule) rule.infoRendered = null;
        renderRuleInfoPanel(si, gi, ri, current.query, current.scope);
    }
    if (typeof rerenderFullscreenInfoPanel === 'function') {
        rerenderFullscreenInfoPanel(si, gi, ri);
    }
    return true;
}

function ruleTreeTargetMatchesRef(container, label, ref, renderKey) {
    var key = ref ? ruleKey(ref.si, ref.gi, ref.ri) : '';
    var expectedId = renderKey ? 'tree-' + renderKey : '';
    if (container && expectedId && container.id && container.id !== expectedId) {
        recordVisualCommitDenied('rule-tree', label, 'repurposed_container', {
            ruleKey: key,
            expectedId: expectedId,
            actualId: container.id
        });
        return false;
    }
    return true;
}

function renderPlainRuleTreeInto(visualCtx, container, ref, renderKey, query, searchScope) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        searchScope = query;
        query = renderKey;
        renderKey = ref;
        ref = container;
        container = visualCtx;
        return runVisualSurfaceCommitNow(
            'plain-rule-tree',
            ['rule-tree'],
            ['trace', 'render'],
            function(ctx) {
                return renderPlainRuleTreeInto(ctx, container, ref, renderKey, query, searchScope);
            }
        );
    }

    var rendered = null;
    ruleTreeSurface.commit(visualCtx, {
        label: 'plain-rule-tree',
        ref: ref,
        renderKey: renderKey,
        target: container,
        detachedReason: 'detached_container'
    }, function(target) {
        if (!ruleTreeTargetMatchesRef(target, 'plain-rule-tree', ref, renderKey)) return;
        rendered = renderPlainRuleTreeIntoForCommit(target, ref, renderKey, query, searchScope, visualCtx);
    });
    return rendered;
}

function payloadStateNeedsMaterialization(state) {
    var states = TraceStore.PAYLOAD_STATES;
    return state === states.UNREQUESTED ||
        state === states.PENDING ||
        state === states.STALE;
}

function payloadBlockJobKey(store, blockId) {
    return String(Number(store && store.traceGeneration) || 0) + '|' + String(blockId || '');
}

function payloadBlockJobMap() {
    var lazy = lazyRuntime();
    if (!lazy.payloadBlockJobs) lazy.payloadBlockJobs = {};
    return lazy.payloadBlockJobs;
}

function pendingPayloadMaterializationFor(store, bucket, ruleHandleValue) {
    var pending = TraceStore.pendingMaterializations(store);
    for (var i = 0; i < pending.length; i++) {
        if (pending[i].bucket === bucket && pending[i].ruleHandle === ruleHandleValue) {
            return pending[i];
        }
    }
    return null;
}

function payloadBlockJobCurrent(blockJob) {
    return !!(blockJob &&
        currentTraceStore() === blockJob.store &&
        Number(currentTraceStore().traceGeneration) === Number(blockJob.traceGeneration));
}

function scheduleResolvedRulePayloadRender(waiter) {
    if (!waiter || !waiter.ref) return;
    if (!validRuleRef(waiter.ref.si, waiter.ref.gi, waiter.ref.ri)) return;
    syncRuleFeaturesAfterPayloadDecode(waiter.ref.si, waiter.ref.gi, waiter.ref.ri);
    requestRuleTreeRenderIfVisible(
        waiter.ref.si,
        waiter.ref.gi,
        waiter.ref.ri,
        currentSearchQuery(),
        false,
        currentSearchScope()
    );
    requestRuleInfoRenderIfVisible(waiter.ref.si, waiter.ref.gi, waiter.ref.ri);
}

function finishPayloadBlockJob(blockJob, result, error) {
    var jobs = payloadBlockJobMap();
    if (jobs[blockJob.key] === blockJob) delete jobs[blockJob.key];
    if (!payloadBlockJobCurrent(blockJob)) return;

    if (error) {
        TracePayloadRegistry.markFailed(blockJob.registry, blockJob.blockId, error);
    } else {
        TracePayloadRegistry.setDecodedBlock(
            blockJob.registry,
            result && result.blockId || blockJob.blockId,
            result && result.value,
            result && result.rawBytes
        );
    }

    for (var i = 0; i < blockJob.waiterOrder.length; i++) {
        var key = blockJob.waiterOrder[i];
        var waiter = blockJob.waiters[key];
        if (!waiter) continue;

        var pending = pendingPayloadMaterializationFor(blockJob.store, waiter.bucket, waiter.ruleHandle);
        if (!pending || pending.requestId !== waiter.request.requestId) continue;

        TraceStore.ensureRulePayload(blockJob.store, waiter.ruleHandle);
        scheduleResolvedRulePayloadRender(waiter);
    }
}

function ensurePayloadBlockJob(store, blockId) {
    var key = payloadBlockJobKey(store, blockId);
    var jobs = payloadBlockJobMap();
    if (jobs[key]) return jobs[key];

    var workerJob = TracePayloadWorker.decodeBlock(store.payloadRegistry, blockId, {
        traceGeneration: store.traceGeneration
    });
    if (!workerJob) return null;

    var blockJob = {
        key: key,
        blockId: blockId,
        traceGeneration: Number(store.traceGeneration) || 0,
        store: store,
        registry: store.payloadRegistry,
        workerJob: workerJob,
        waiters: {},
        waiterOrder: []
    };
    jobs[key] = blockJob;

    workerJob.promise.then(function(result) {
        finishPayloadBlockJob(blockJob, result, null);
    }, function(error) {
        finishPayloadBlockJob(blockJob, null, error);
    });

    return blockJob;
}

function attachPayloadBlockWaiter(blockJob, request, ref) {
    if (!blockJob || !request) return false;
    var key = request.bucket + '|' + request.ruleHandle;
    if (!blockJob.waiters[key]) blockJob.waiterOrder.push(key);
    blockJob.waiters[key] = {
        request: request,
        bucket: request.bucket,
        ruleHandle: request.ruleHandle,
        ref: ref ? { si: ref.si, gi: ref.gi, ri: ref.ri } : null
    };
    return true;
}

function requestAsyncRulePayloadForRender(store, ref, ruleHandleValue, bucket) {
    var payloadRef = TraceStore.payloadRefForRule(store, ruleHandleValue);
    if (!payloadRef || !payloadRef.blockId) {
        return TraceStore.ensureRulePayload(store, ruleHandleValue);
    }

    if (TracePayloadRegistry.decodedBlock(store.payloadRegistry, payloadRef.blockId)) {
        return TraceStore.ensureRulePayload(store, ruleHandleValue);
    }

    var blockJob = ensurePayloadBlockJob(store, payloadRef.blockId);
    if (!blockJob) {
        return TraceStore.ensureRulePayload(store, ruleHandleValue);
    }

    var request = pendingPayloadMaterializationFor(store, bucket, ruleHandleValue);
    if (!request) {
        request = TraceStore.beginPayloadMaterialization(store, bucket, ruleHandleValue);
    }
    if (!request) {
        return TraceStore.ensureRulePayload(store, ruleHandleValue);
    }

    attachPayloadBlockWaiter(blockJob, request, ref);
    return {
        status: 'pending',
        blockId: payloadRef.blockId,
        payloadIndex: payloadRef.payloadIndex,
        request: request
    };
}

function renderPlainRuleTreeIntoForCommit(container, ref, renderKey, query, searchScope, visualCtx) {
    var rawIdx = rawRuleIndex(ref.si, ref.gi, ref.ri);
    query = query || '';
    var renderScope = query ? (searchScope || currentSearchScope()) : '';
    var isText = ruleIsTextTile(ref);
    var store = currentTraceStore();
    var ruleHandleValue = TraceStore.ruleHandle(store, ref.si, rawIdx);
    var payloadBucket = isText ? 'text' : 'trees';
    var initialPayloadState = TraceStore.payloadState(store, payloadBucket, ruleHandleValue).state;
    var payloadDecoded = false;
    if (payloadStateNeedsMaterialization(initialPayloadState)) {
        var payloadResult = requestAsyncRulePayloadForRender(store, ref, ruleHandleValue, payloadBucket);
        payloadDecoded = !!(payloadResult && payloadResult.status === 'decoded');
    }
    if (!isText && payloadDecoded) syncRuleFeaturesAfterPayloadDecode(ref.si, ref.gi, ref.ri);
    var payloadRecord = TraceStore.payloadState(store, payloadBucket, ruleHandleValue);
    var showFields = !isText && effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'fields');
    var showPinned = !isText && effectiveRuleFeature(ref.si, ref.gi, ref.ri, 'pinned');

    container.classList.remove('fullscreen-diff-wrap');
    container.classList.remove('diff-tree-wrap');
    if (renderPayloadFailureIfNeeded(container, ref, payloadBucket)) {
        return {
            query: query,
            scope: renderScope,
            showFields: showFields,
            showPinned: showPinned,
            payloadState: TraceStore.PAYLOAD_STATES.FAILED
        };
    }
    if (renderPayloadPendingIfNeeded(container, ref, payloadBucket)) {
        return {
            query: query,
            scope: renderScope,
            showFields: showFields,
            showPinned: showPinned,
            payloadState: TraceStore.PAYLOAD_STATES.PENDING
        };
    }
    if (isText) {
        renderTextTile(container, traceRuleText(ref.si, rawIdx), query, renderScope,
            ruleKey(ref.si, ref.gi, ref.ri));
    } else {
        renderTree(container, tracePlanTree(ref.si, rawIdx), renderKey,
                   showFields, showPinned, query, renderScope, visualCtx);
    }
    restoreRuleTreePaneScroll(ref, container);
    return {
        query: query,
        scope: renderScope,
        showFields: showFields,
        showPinned: showPinned,
        payloadState: payloadRecord.state
    };
}

function payloadHandleKeyForBucket(bucket) {
    return bucket === 'trees' ? 'tree' : bucket;
}

function payloadFailureMessage(error) {
    if (!error) return 'Payload materialization failed.';
    if (error.message !== undefined && error.message !== null) return String(error.message);
    return String(error);
}

function renderPayloadPendingIfNeeded(container, ref, bucket) {
    var store = currentTraceStore();
    var rawIdx = rawRuleIndex(ref.si, ref.gi, ref.ri);
    var ruleHandleValue = TraceStore.ruleHandle(store, ref.si, rawIdx);
    var state = TraceStore.payloadState(store, bucket, ruleHandleValue);
    if (state.state !== TraceStore.PAYLOAD_STATES.PENDING) return false;

    container.classList.add('render-pending');
    container.innerHTML =
        '<div class="tree-loading" data-payload-state="pending"' +
        ' data-rule-handle="' + htmlEscape(ruleHandleValue || '') + '"' +
        ' aria-hidden="true"></div>';
    return true;
}

function renderRuleTreeLoadingPlaceholder(visualCtx, container, ref, reason) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        reason = ref;
        ref = container;
        container = visualCtx;
        return runVisualSurfaceCommitNow(
            'rule-tree-loading-placeholder',
            ['rule-tree'],
            ['trace', 'render'],
            function(ctx) {
                return renderRuleTreeLoadingPlaceholder(ctx, container, ref, reason);
            }
        );
    }
    if (ruleBodyContainerHasContent(container)) return false;
    var renderKey = ref ? ruleKey(ref.si, ref.gi, ref.ri) : '';

    var rendered = false;
    var commitOptions = {
        label: 'rule-tree-loading-placeholder',
        ref: ref,
        renderKey: renderKey,
        detachedReason: 'detached_container'
    };
    if (container) commitOptions.target = container;
    ruleTreeSurface.commit(visualCtx, commitOptions, function(target) {
        if (!target || ruleBodyContainerHasContent(target)) return;
        if (!ruleTreeTargetMatchesRef(target, 'rule-tree-loading-placeholder', ref, renderKey)) return;
        target.classList.add('render-pending');
        target.innerHTML =
            '<div class="tree-loading" data-render-state="queued"' +
            ' data-render-reason="' + htmlEscape(reason || 'lazy') + '"' +
            ' aria-hidden="true"></div>';
        rendered = true;
    });
    return rendered;
}

function ruleBodyContainerHasContent(container) {
    if (!container) return false;
    var virtualState = container.__traceVirtualTreeState;
    if (virtualState && virtualState.rowsEl && virtualState.rowsEl !== container &&
            ruleBodyContainerHasContent(virtualState.rowsEl)) {
        return true;
    }
    if ('firstChild' in container) return !!container.firstChild;
    if (container.childNodes && typeof container.childNodes.length === 'number') {
        return container.childNodes.length > 0;
    }
    if (typeof container.childElementCount === 'number') {
        return container.childElementCount > 0;
    }
    if (container.children && typeof container.children.length === 'number') {
        return container.children.length > 0;
    }
    return false;
}

function infoPanelContentHasContent(container) {
    return ruleBodyContainerHasContent(container);
}

function ruleBodyRenderState(container) {
    if (!container) {
        return { state: 'missing', valid: false };
    }

    if (!ruleBodyContainerHasContent(container)) {
        return { state: 'invalid-empty', valid: false };
    }

    if (container.querySelector) {
        if (container.querySelector('.payload-error[data-payload-state="failed"]')) {
            return { state: 'payload-failed', valid: true };
        }
        if (container.querySelector('.tree-loading[data-payload-state="pending"]')) {
            return { state: 'payload-pending', valid: true };
        }
        if (container.querySelector('.tree-loading[data-render-state="queued"]')) {
            return { state: 'render-queued', valid: true };
        }
    }

    if (typeof treeMaterializerViewportRenderState === 'function') {
        var materializerState = treeMaterializerViewportRenderState(container);
        if (materializerState && !materializerState.valid) return materializerState;
    }

    return { state: 'rendered', valid: true };
}

function ensureVisibleExpandedRuleBody(si, gi, ri, options) {
    options = options || {};
    var result = {
        state: 'skipped',
        valid: false,
        rendered: false,
        placeholder: false,
        reason: ''
    };

    if (!validRuleRef(si, gi, ri)) {
        result.reason = 'invalid-rule-ref';
        return result;
    }
    if (!isRuleExpandedInLayout(si, gi, ri)) {
        result.reason = 'not-expanded';
        return result;
    }
    if (!hasDOM()) {
        result.reason = 'dom-unavailable';
        return result;
    }

    var key = ruleKey(si, gi, ri);
    var container = document.getElementById('tree-' + key);
    var cell = document.getElementById('rule-' + key);
    if (!container || !cell) {
        result.state = !container ? 'missing' : 'cell-missing';
        result.reason = !container ? 'tree-missing' : 'cell-missing';
        return result;
    }
    if (!elementIntersectsLazyViewport(cell)) {
        var offscreenState = ruleBodyRenderState(container);
        result.state = offscreenState.state;
        result.valid = offscreenState.valid;
        result.reason = 'offscreen';
        return result;
    }

    var state = ruleBodyRenderState(container);
    var query = options.query !== undefined ? options.query : currentSearchQuery();
    var searchScope = options.searchScope !== undefined ? options.searchScope : currentSearchScope();
    var force = !!options.force;

    if (!state.valid && state.needsRender &&
            typeof ensureTreeViewportRowsMounted === 'function' &&
            options.render !== false) {
        var materializerResult = ensureTreeViewportRowsMounted(container, 'visible-rule-body');
        result.rendered = !!(materializerResult && materializerResult.rendered);
        state = ruleBodyRenderState(container);
    }

    if ((state.state === 'invalid-empty' ||
            state.state === 'render-queued' ||
            state.state === 'payload-pending') &&
            options.render !== false &&
            ruleTreeRenderNeeded(si, gi, ri, query, force, searchScope)) {
        result.rendered = renderRuleTreeImmediatelyIfMounted(si, gi, ri, query, force, searchScope);
        state = ruleBodyRenderState(container);
    }

    if (state.state === 'invalid-empty' && options.placeholder !== false) {
        result.placeholder = renderRuleTreeLoadingPlaceholder(
            null,
            ruleRef(si, gi, ri),
            options.reason || 'visible'
        );
        state = ruleBodyRenderState(container);
    }

    result.state = state.state;
    result.valid = state.valid;
    result.reason = state.valid ? '' : state.state;
    return result;
}

function renderPayloadFailureIfNeeded(container, ref, bucket) {
    var store = currentTraceStore();
    var rawIdx = rawRuleIndex(ref.si, ref.gi, ref.ri);
    var ruleHandleValue = TraceStore.ruleHandle(store, ref.si, rawIdx);
    var state = TraceStore.payloadState(store, bucket, ruleHandleValue);
    if (state.state !== TraceStore.PAYLOAD_STATES.FAILED) return false;

    var summary = TraceStore.ruleSummaryForRef(store, ref) || {};
    var payloadHandles = summary.payloadHandles || {};
    var payloadHandle = payloadHandles[payloadHandleKeyForBucket(bucket)] || ruleHandleValue;
    container.classList.remove('render-pending');
    container.innerHTML =
        '<div class="payload-error" role="alert" data-payload-state="failed"' +
        ' data-rule-handle="' + htmlEscape(ruleHandleValue || '') + '"' +
        ' data-payload-handle="' + htmlEscape(payloadHandle || '') + '">' +
        '<div class="payload-error-title">Unable to render payload</div>' +
        '<div class="payload-error-rule">' + htmlEscape(ruleDisplayName(ref)) + '</div>' +
        '<div class="payload-error-detail">' + htmlEscape(payloadFailureMessage(state.error)) + '</div>' +
        '<div class="payload-error-context">' + htmlEscape(payloadHandle || '') + '</div>' +
        '</div>';
    return true;
}

function renderPlainRuleTree(visualCtx, ref, query, searchScope) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        searchScope = query;
        query = ref;
        ref = visualCtx;
        visualCtx = null;
    }
    var ruleKeyValue = ruleKey(ref.si, ref.gi, ref.ri);
    var container = document.getElementById('tree-' + ruleKeyValue);
    if (!container) {
        clearRuleTreePending(ref.si, ref.gi, ref.ri);
        return false;
    }

    clearRuleTreePending(ref.si, ref.gi, ref.ri);
    ruleState(ref.si, ref.gi, ref.ri).renderQueued = false;

    var rendered = visualCtx
        ? renderPlainRuleTreeInto(visualCtx, null, ref, ruleKeyValue, query, searchScope)
        : runVisualSurfaceCommitNow(
            'plain-rule-tree',
            ['rule-tree'],
            ['trace', 'render'],
            function(ctx) {
                return renderPlainRuleTreeInto(ctx, null, ref, ruleKeyValue, query, searchScope);
            }
        );
    if (!rendered) return false;
    ruleState(ref.si, ref.gi, ref.ri).rendered = {
        query: rendered.query,
        scope: rendered.scope,
        showFields: rendered.showFields,
        showPinned: rendered.showPinned,
        pinnedFields: pinnedFieldRenderSignature(),
        payloadState: rendered.payloadState
    };
    updateRuleFeatureButtons(ref.si, ref.gi, ref.ri);
    reactivateActiveSearchMatchForRule(ref.si, ref.gi, ref.ri);
    return true;
}

function renderPendingDiffPlainTree(ref) {
    if (!ref) return;
    if (!plainRuleTreeRenderNeeded(ref.si, ref.gi, ref.ri,
                                  currentSearchQuery(), currentSearchScope())) {
        return;
    }
    renderPlainRuleTree(ref, currentSearchQuery(), currentSearchScope());
}

function renderPendingDiffPlainTrees() {
    renderPendingDiffPlainTree(diffRuntime().diffA);
    renderPendingDiffPlainTree(diffRuntime().diffB);
}

function rerenderRuleTree(visualCtx, si, gi, ri, query, force, searchScope, diffResult) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        diffResult = searchScope;
        searchScope = force;
        force = query;
        query = ri;
        ri = gi;
        gi = si;
        si = visualCtx;
        visualCtx = null;
    }
    var ruleKeyValue = ruleKey(si, gi, ri);
    var container = document.getElementById('tree-' + ruleKeyValue);
    if (!container) {
        clearRuleTreePending(si, gi, ri);
        return;
    }
    clearRuleTreePending(si, gi, ri);
    ruleState(si, gi, ri).renderQueued = false;

    var diffSide = diffSideForRule(si, gi, ri);
    if (diffSide) {
        renderDiffRuleTree({ si: si, gi: gi, ri: ri }, diffSide, activeDiffResultForRender());
        return;
    }

    if (!force && !plainRuleTreeRenderNeeded(si, gi, ri, query, searchScope)) {
        return;
    }

    if (visualCtx) {
        renderPlainRuleTree(visualCtx, { si: si, gi: gi, ri: ri }, query, searchScope);
    } else {
        renderPlainRuleTree({ si: si, gi: gi, ri: ri }, query, searchScope);
    }
}

var RULE_RENDER_BUDGET_MS = 12;
var RULE_LOADING_DELAY_MS = 120;
var RULE_SCROLL_INLINE_RENDER_BUDGET_MS = 6;
var LAZY_RENDER_MARGIN = 180;

function parseRuleKeyId(id, prefix) {
    if (!id || id.indexOf(prefix) !== 0) return null;
    var parts = id.substring(prefix.length).split('-');
    if (parts.length !== 3) return null;

    var si = Number(parts[0]);
    var gi = Number(parts[1]);
    var ri = Number(parts[2]);
    if (!Number.isFinite(si) || !Number.isFinite(gi) || !Number.isFinite(ri)) {
        return null;
    }
    return { si: si, gi: gi, ri: ri };
}

function isRuleExpandedInLayout(si, gi, ri) {
    return effectiveStageOpen(si) &&
           (groupRuleCount(si, gi) <= 1 || effectiveGroupOpen(si, gi)) &&
           effectiveRuleOpen(si, gi, ri);
}

function lazyViewportRectHasBox(rect) {
    if (!rect) return false;
    if (rect.width > 0 || rect.height > 0) return true;
    return rect.left !== rect.right || rect.top !== rect.bottom;
}

function elementIntersectsLazyViewport(el) {
    if (!el || typeof el.getBoundingClientRect !== 'function') return false;
    var rect = el.getBoundingClientRect();
    if (!lazyViewportRectHasBox(rect)) return false;
    var margin = LAZY_RENDER_MARGIN;
    var width = window.innerWidth || document.documentElement.clientWidth;
    var height = window.innerHeight || document.documentElement.clientHeight;
    return rect.bottom >= -margin &&
           rect.top <= height + margin &&
           rect.right >= -margin &&
           rect.left <= width + margin;
}

function mountedRuleTreeIsEmpty(si, gi, ri) {
    if (!hasDOM()) return false;
    var container = document.getElementById('tree-' + ruleKey(si, gi, ri));
    var state = ruleBodyRenderState(container);
    return state.state === 'invalid-empty' ||
        state.state === 'render-queued' ||
        state.state === 'payload-pending' ||
        (typeof treeMaterializerRenderStateIsInvalid === 'function' &&
            treeMaterializerRenderStateIsInvalid(state));
}

function currentRulePayloadState(si, gi, ri) {
    var ref = ruleRef(si, gi, ri);
    var store = currentTraceStore();
    var bucket = ruleIsTextTile(ref) ? 'text' : 'trees';
    var handle = TraceStore.ruleHandle(store, si, rawRuleIndex(si, gi, ri));
    return TraceStore.payloadState(store, bucket, handle).state;
}

function plainRuleTreeRenderNeeded(si, gi, ri, query, searchScope) {
    var rule = ruleState(si, gi, ri);
    var prev = rule.rendered;
    if (!prev) return true;
    if (mountedRuleTreeIsEmpty(si, gi, ri)) return true;
    if (prev.diffSide) return true;

    query = query || '';
    var renderScope = query ? (searchScope || currentSearchScope()) : '';
    return prev.query !== query ||
           prev.scope !== renderScope ||
           prev.showFields !== effectiveRuleFeature(si, gi, ri, 'fields') ||
           prev.showPinned !== effectiveRuleFeature(si, gi, ri, 'pinned') ||
           prev.pinnedFields !== pinnedFieldRenderSignature() ||
           prev.payloadState !== currentRulePayloadState(si, gi, ri);
}

function ruleTreeRenderNeeded(si, gi, ri, query, force, searchScope) {
    if (force) return true;

    var rule = ruleState(si, gi, ri);
    var prev = rule.rendered;
    if (!prev) return true;
    if (mountedRuleTreeIsEmpty(si, gi, ri)) return true;

    var showFields = effectiveRuleFeature(si, gi, ri, 'fields');
    var showPinned = effectiveRuleFeature(si, gi, ri, 'pinned');

    var diffSide = diffSideForRule(si, gi, ri);
    if (diffSide && activeDiffPendingWithoutResult()) {
        return plainRuleTreeRenderNeeded(si, gi, ri, query, searchScope);
    }
    if (diffSide) {
        return prev.diffSide !== diffSide ||
               prev.showFields !== showFields ||
               prev.showPinned !== showPinned ||
               prev.pinnedFields !== pinnedFieldRenderSignature() ||
               prev.diffFields !== diffFieldSelectionSignature();
    }

    query = query || '';
    var renderScope = query ? (searchScope || currentSearchScope()) : '';
    return prev.query !== query ||
           prev.scope !== renderScope ||
           prev.showFields !== showFields ||
           prev.showPinned !== showPinned ||
           prev.pinnedFields !== pinnedFieldRenderSignature() ||
           prev.payloadState !== currentRulePayloadState(si, gi, ri);
}

function showRuleTreePendingIfStillNeeded(si, gi, ri, query, force, searchScope) {
    var key = ruleKey(si, gi, ri);
    delete lazyRuntime().delayedRuleLoadingTimers[key];

    if (!isRuleExpandedInLayout(si, gi, ri)) return;
    if (!ruleTreeRenderNeeded(si, gi, ri, query, force, searchScope)) return;

    var container = document.getElementById('tree-' + key);
    var cell = document.getElementById('rule-' + key);
    if (!container || !elementIntersectsLazyViewport(cell)) return;

    renderRuleTreeLoadingPlaceholder(null, ruleRef(si, gi, ri), 'lazy');
}

function clearRuleTreePending(si, gi, ri) {
    var key = ruleKey(si, gi, ri);
    var timer = lazyRuntime().delayedRuleLoadingTimers[key];
    if (timer !== undefined) {
        clearTimeout(timer);
        delete lazyRuntime().delayedRuleLoadingTimers[key];
    }

    if (typeof document === 'undefined') return;
    var container = document.getElementById('tree-' + key);
    if (!container) return;
    container.classList.remove('render-pending');
}

function markRuleTreePending(si, gi, ri, query, force, searchScope) {
    var key = ruleKey(si, gi, ri);
    if (lazyRuntime().delayedRuleLoadingTimers[key] !== undefined) return;

    var pendingJob = {
        key: key,
        si: si,
        gi: gi,
        ri: ri,
        query: query || '',
        force: !!force,
        scope: searchScope || '',
        traceEpoch: currentRuntimeEpoch().trace,
        renderEpoch: currentRuntimeEpoch().render,
        virtualRowsDomGeneration: currentVirtualRowsDomGeneration(),
        searchTransactionToken: query ? activeSearchTransactionToken() : null
    };

    lazyRuntime().delayedRuleLoadingTimers[key] = scheduleRuntimeTimeout(function() {
        var job = lazyRuntime().queuedRuleRenders[key];
        var currentJob = job || pendingJob;
        delete lazyRuntime().delayedRuleLoadingTimers[key];
        if (!runtimeJobCurrent(currentJob)) return;
        if (!virtualRowsDomGenerationCurrent(currentJob.virtualRowsDomGeneration)) return;
        if (!searchTransactionTokenCurrent(currentJob.searchTransactionToken)) return;

        var currentQuery = currentSearchQuery();
        var currentScope = currentQuery ? currentSearchScope() : '';
        if ((currentJob.query || '') !== currentQuery) return;
        if ((currentJob.query ? currentJob.scope || '' : '') !== currentScope) return;

        showRuleTreePendingIfStillNeeded(
            si,
            gi,
            ri,
            currentJob.query,
            currentJob.force,
            currentJob.scope
        );
    }, RULE_LOADING_DELAY_MS, {
        epochScopes: ['trace', 'render', 'virtual', 'search'],
        label: 'lazy-rule-loading-placeholder'
    });
}

function showVisibleRuleTreePendingIfEmpty(si, gi, ri, reason) {
    var result = ensureVisibleExpandedRuleBody(si, gi, ri, {
        render: false,
        reason: reason || 'visible'
    });
    return !!result.placeholder;
}

function queueRuleTreeRender(si, gi, ri, query, force, searchScope) {
    if (!validRuleRef(si, gi, ri)) return;
    if (!isRuleExpandedInLayout(si, gi, ri)) return;
    if (!ruleTreeRenderNeeded(si, gi, ri, query, force, searchScope)) {
        requestRuleInfoRenderIfVisible(si, gi, ri);
        return;
    }

    markRuleTreePending(si, gi, ri, query, force, searchScope);

    var key = ruleKey(si, gi, ri);
    var existing = lazyRuntime().queuedRuleRenders[key];
    if (existing) {
        existing.query = query || '';
        existing.force = existing.force || !!force;
        existing.scope = searchScope || '';
        existing.traceEpoch = currentRuntimeEpoch().trace;
        existing.renderEpoch = currentRuntimeEpoch().render;
        existing.virtualRowsDomGeneration = currentVirtualRowsDomGeneration();
        existing.searchTransactionToken = query ? activeSearchTransactionToken() : null;
        ruleState(si, gi, ri).renderQueued = true;
        scheduleRuleRenderQueue();
        return;
    }

    var job = {
        key: key,
        si: si,
        gi: gi,
        ri: ri,
        query: query || '',
        force: !!force,
        scope: searchScope || '',
        traceEpoch: currentRuntimeEpoch().trace,
        renderEpoch: currentRuntimeEpoch().render,
        virtualRowsDomGeneration: currentVirtualRowsDomGeneration(),
        searchTransactionToken: query ? activeSearchTransactionToken() : null
    };
    lazyRuntime().queuedRuleRenders[key] = job;
    ruleState(si, gi, ri).renderQueued = true;
    lazyRuntime().ruleRenderQueue.push(job);
    scheduleRuleRenderQueue();
}

function renderRuleTreeImmediatelyIfMounted(si, gi, ri, query, force, searchScope) {
    if (!validRuleRef(si, gi, ri)) return false;
    if (!isRuleExpandedInLayout(si, gi, ri)) return false;
    if (!hasDOM()) return false;

    var key = ruleKey(si, gi, ri);
    var container = document.getElementById('tree-' + key);
    if (!container) return false;

    if (ruleTreeRenderNeeded(si, gi, ri, query, force, searchScope)) {
        rerenderRuleTree(si, gi, ri, query, force, searchScope);
    }
    requestRuleInfoRenderIfVisible(si, gi, ri);
    return true;
}

function renderVisibleEmptyRuleTreeImmediately(si, gi, ri, query, searchScope) {
    if (!mountedRuleTreeIsEmpty(si, gi, ri)) return false;
    var result = ensureVisibleExpandedRuleBody(si, gi, ri, {
        query: query,
        searchScope: searchScope,
        render: true,
        placeholder: false,
        reason: 'inline-visible'
    });
    return !!result.rendered;
}

function scheduleRuleRenderQueue() {
    if (lazyRuntime().ruleRenderTimer !== null) return;
    lazyRuntime().ruleRenderTimer = scheduleRenderDeferredWork(
        'lazy-rule-render-queue',
        function runScheduledRuleRenderQueue(visualCtx) {
            runRuleRenderQueue(visualCtx);
        },
        {
            epochScopes: ['trace', 'render'],
            label: 'lazy-rule-render-queue',
            surfaces: ['rule-tree', 'rule-info', 'search-marks'],
            withVisualContext: true,
            onDiscard: function() {
                lazyRuntime().ruleRenderTimer = null;
                if (lazyRuntime().ruleRenderQueue.length) scheduleRuleRenderQueue();
            }
        }
    );
}

function runRuleRenderQueue(visualCtx) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runVisualSurfaceCommitNow(
            'lazy-rule-render-queue',
            ['rule-tree', 'rule-info', 'search-marks'],
            ['trace', 'render'],
            function(ctx) {
                return runRuleRenderQueue(ctx);
            }
        );
    }

    lazyRuntime().ruleRenderTimer = null;

    var start = Date.now();
    var renderedAny = false;
    while (lazyRuntime().ruleRenderQueue.length &&
           (!renderedAny || Date.now() - start < RULE_RENDER_BUDGET_MS)) {
        var job = lazyRuntime().ruleRenderQueue.shift();
        delete lazyRuntime().queuedRuleRenders[job.key];

        if (!runtimeJobCurrent(job) || !validRuleRef(job.si, job.gi, job.ri)) {
            continue;
        }

        var rule = ruleState(job.si, job.gi, job.ri);
        rule.renderQueued = false;

        if (!virtualRowsDomGenerationCurrent(job.virtualRowsDomGeneration)) {
            recordVisualCommitDenied('rule-tree', 'lazy-rule-render-queue', 'stale_dom_generation', {
                ruleKey: job.key,
                virtualRowsDomGeneration: job.virtualRowsDomGeneration
            });
            continue;
        }

        if (!isRuleExpandedInLayout(job.si, job.gi, job.ri)) {
            clearRuleTreePending(job.si, job.gi, job.ri);
            continue;
        }

        var cell = document.getElementById('rule-' + job.key);
        if (!elementIntersectsLazyViewport(cell)) {
            clearRuleTreePending(job.si, job.gi, job.ri);
            continue;
        }

        job.query = currentSearchQuery();
        job.scope = job.query ? currentSearchScope() : '';
        if (!ruleTreeRenderNeeded(job.si, job.gi, job.ri, job.query, job.force, job.scope)) {
            clearRuleTreePending(job.si, job.gi, job.ri);
            requestRuleInfoRenderIfVisible(visualCtx, job.si, job.gi, job.ri);
            continue;
        }

        rerenderRuleTree(visualCtx, job.si, job.gi, job.ri, job.query, job.force, job.scope);
        requestRuleInfoRenderIfVisible(visualCtx, job.si, job.gi, job.ri);
        renderedAny = true;
    }

    if (lazyRuntime().ruleRenderQueue.length) scheduleRuleRenderQueue();
}

function requestRuleTreeRenderIfVisible(si, gi, ri, query, force, searchScope) {
    if (!validRuleRef(si, gi, ri)) return;
    if (!isRuleExpandedInLayout(si, gi, ri)) return;
    if (!ruleTreeRenderNeeded(si, gi, ri, query, force, searchScope)) {
        requestRuleInfoRenderIfVisible(si, gi, ri);
        return;
    }

    var key = ruleKey(si, gi, ri);
    var container = document.getElementById('tree-' + key);
    var cell = document.getElementById('rule-' + key);
    if (!container || !cell) return;

    if (!elementIntersectsLazyViewport(cell)) {
        return;
    }

    queueRuleTreeRender(si, gi, ri, query, force, searchScope);
}

function requestVisibleRuleRenders(query, force, searchScope) {
    // Only mounted rule cells can intersect the lazy viewport, so scanning
    // the mounted key set instead of every rule keeps this O(mounted).
    var mounted = virtualRuntime().mountedRuleKeys || {};
    for (var key in mounted) {
        if (!Object.prototype.hasOwnProperty.call(mounted, key)) continue;
        var ref = parseRuleKeyId('rule-' + key, 'rule-');
        if (!ref) continue;
        requestRuleTreeRenderIfVisible(ref.si, ref.gi, ref.ri, query, force, searchScope);
    }
}

function scheduleRuleVisibilityCheck(si, gi, ri) {
    if (!validRuleRef(si, gi, ri)) return;
    var key = ruleKey(si, gi, ri);
    lazyRuntime().pendingVisibleRuleChecks[key] = {
        si: si,
        gi: gi,
        ri: ri,
        traceEpoch: currentRuntimeEpoch().trace,
        renderEpoch: currentRuntimeEpoch().render
    };

    if (lazyRuntime().visibleRuleCheckTimer !== null) return;
    lazyRuntime().visibleRuleCheckTimer = scheduleRenderDeferredWork(
        'visible-rule-checks',
        function runScheduledPendingRuleVisibilityChecks(visualCtx) {
            runPendingRuleVisibilityChecks(visualCtx);
        },
        {
            epochScopes: ['trace', 'render'],
            label: 'visible-rule-checks',
            surfaces: ['rule-tree', 'rule-info', 'search-marks'],
            onDiscard: function() {
                lazyRuntime().visibleRuleCheckTimer = null;
            }
        }
    );
}

function runPendingRuleVisibilityChecks(visualCtx) {
    lazyRuntime().visibleRuleCheckTimer = null;

    var pending = lazyRuntime().pendingVisibleRuleChecks;
    lazyRuntime().pendingVisibleRuleChecks = {};

    for (var key in pending) {
        if (!Object.prototype.hasOwnProperty.call(pending, key)) continue;
        var ref = pending[key];
        if (!runtimeJobCurrent(ref) || !validRuleRef(ref.si, ref.gi, ref.ri)) continue;
        requestRuleTreeRenderIfVisible(
            ref.si,
            ref.gi,
            ref.ri,
            currentSearchQuery(),
            false,
            currentSearchScope()
        );
        requestRuleInfoRenderIfVisible(ref.si, ref.gi, ref.ri);
    }
}

function scheduleStageRuleVisibilityChecks(si) {
    for (var gi = 0; gi < groupCount(si); gi++) {
        for (var ri = 0; ri < groupRuleCount(si, gi); ri++) {
            if (isRuleExpandedInLayout(si, gi, ri)) {
                scheduleRuleVisibilityCheck(si, gi, ri);
            }
        }
    }
}

function scheduleVisibleRuleRenderScan() {
    if (lazyRuntime().visibleRuleScanTimer !== null) return;
    lazyRuntime().visibleRuleScanTimer = scheduleRenderDeferredWork(
        'visible-rule-render-scan',
        function runScheduledVisibleRuleRenderScan(visualCtx) {
            lazyRuntime().visibleRuleScanTimer = null;
            requestVisibleRuleRenders(currentSearchQuery(), false, currentSearchScope());
        },
        {
            epochScopes: ['trace'],
            label: 'visible-rule-render-scan',
            surfaces: ['rule-tree', 'rule-info', 'search-marks'],
            onDiscard: function() {
                lazyRuntime().visibleRuleScanTimer = null;
            }
        }
    );
}

function initLazyRuleRenderTriggers() {
    initRuleVirtualization();
}

function initLazyRuleRendering() {
    updateAllLayoutWidths();
    initLazyRuleRenderTriggers();
    scheduleVisibleRuleRenderScan();
}

function invalidateRuleTree(si, gi, ri) {
    var rule = findRuleState(si, gi, ri);
    if (rule) {
        if (typeof discardDetachedRuleElementForRule === 'function') {
            discardDetachedRuleElementForRule(si, gi, ri);
        }
        rule.rendered = null;
    }
}

function updateRuleFeatureButtons(si, gi, ri) {
    var metaBtn = document.getElementById('fieldbtn-' + si + '-' + gi + '-' + ri);
    if (metaBtn) {
        var fieldsAvailable = ruleHasFeature(si, gi, ri, 'fields');
        metaBtn.disabled = !fieldsAvailable;
        metaBtn.classList.toggle('active', fieldsAvailable && ruleFeatureButtonActive('fields', si, gi, ri));
    }

    var pinBtn = document.getElementById('pinbtn-' + si + '-' + gi + '-' + ri);
    if (pinBtn) {
        var pinnedAvailable = ruleHasFeature(si, gi, ri, 'pinned');
        pinBtn.disabled = !pinnedAvailable;
        pinBtn.classList.toggle('active', pinnedAvailable && ruleFeatureButtonActive('pinned', si, gi, ri));
    }

    updateRuleInfoDisplay(si, gi, ri);
}

function syncRuleFeaturesAfterPayloadDecode(si, gi, ri) {
    var rule = findRuleState(si, gi, ri);
    if (!rule) return;

    var features = ['fields', 'pinned', 'info'];
    for (var i = 0; i < features.length; i++) {
        var feature = features[i];
        if (rule[feature] && !ruleHasFeature(si, gi, ri, feature)) {
            TraceState.setRuleFeature(currentUiState(), si, gi, ri, feature, false);
        }
    }

    updateRuleFeatureButtons(si, gi, ri);
}

function syncMountedRuleFeaturesForRawRule(si, rawIdx) {
    if (typeof currentTraceGroups !== 'function') return;
    var groups = currentTraceGroups()[si] || [];
    for (var gi = 0; gi < groups.length; gi++) {
        var rawIndices = groups[gi] && groups[gi].ri || [];
        for (var ri = 0; ri < rawIndices.length; ri++) {
            if (rawIndices[ri] === rawIdx) {
                syncRuleFeaturesAfterPayloadDecode(si, gi, ri);
            }
        }
    }
}

function updateRuleInfoDisplay(si, gi, ri) {
    var panel = document.getElementById('info-' + si + '-' + gi + '-' + ri);
    var infoBtn = document.getElementById('infobtn-' + si + '-' + gi + '-' + ri);
    var infoAvailable = ruleHasFeature(si, gi, ri, 'info');
    if (panel) {
        var visible = shouldShowRuleInfoPanelShell(si, gi, ri);
        panel.classList.toggle('visible', visible);
        if (panel.previousElementSibling &&
            panel.previousElementSibling.classList.contains('info-panel-resizer')) {
            panel.previousElementSibling.classList.toggle('hidden', !visible);
        }
        if (visible) {
            var savedH = resizeRuntime().infoPanelHeights[si + '-' + gi + '-' + ri];
            var autoH = Number(panel.getAttribute('data-info-auto-height') || 0);
            if (savedH) {
                panel.style.height = savedH + 'px';
                panel.style.maxHeight = 'none';
                panel.removeAttribute('data-info-auto-height');
            } else if (autoH > 0) {
                panel.style.height = autoH + 'px';
                panel.style.maxHeight = 'none';
            } else {
                panel.style.height = '';
                panel.style.maxHeight = 'min(40vh, 520px)';
                panel.removeAttribute('data-info-auto-height');
            }
            requestRuleInfoRenderIfVisible(si, gi, ri);
        }
        if (!infoAvailable) removeRuleInfoPanelShellElement(panel);
    }
    if (infoBtn) {
        infoBtn.disabled = !infoAvailable;
        infoBtn.classList.toggle('active', infoAvailable && ruleFeatureButtonActive('info', si, gi, ri));
    }
}

function syncReattachedRuleElementFeatureState(el, si, gi, ri) {
    if (!el || !el.querySelector) return;
    var id = si + '-' + gi + '-' + ri;

    var metaBtn = el.querySelector('#fieldbtn-' + id);
    if (metaBtn) {
        var fieldsAvailable = ruleHasFeature(si, gi, ri, 'fields');
        metaBtn.disabled = !fieldsAvailable;
        metaBtn.classList.toggle('active', fieldsAvailable && ruleFeatureButtonActive('fields', si, gi, ri));
    }
    var pinBtn = el.querySelector('#pinbtn-' + id);
    if (pinBtn) {
        var pinnedAvailable = ruleHasFeature(si, gi, ri, 'pinned');
        pinBtn.disabled = !pinnedAvailable;
        pinBtn.classList.toggle('active', pinnedAvailable && ruleFeatureButtonActive('pinned', si, gi, ri));
    }
    var infoBtn = el.querySelector('#infobtn-' + id);
    if (infoBtn) {
        var infoAvailable = ruleHasFeature(si, gi, ri, 'info');
        infoBtn.disabled = !infoAvailable;
        infoBtn.classList.toggle('active', infoAvailable && ruleFeatureButtonActive('info', si, gi, ri));
    }

    var panel = el.querySelector('#info-' + id);
    if (!panel || !panel.classList) return;
    if (!ruleHasFeature(si, gi, ri, 'info')) {
        removeRuleInfoPanelShellElement(panel);
        return;
    }
    var visible = shouldShowRuleInfoPanelShell(si, gi, ri);
    if (panel.classList.contains('visible') === visible) return;

    panel.classList.toggle('visible', visible);
    if (panel.previousElementSibling &&
        panel.previousElementSibling.classList.contains('info-panel-resizer')) {
        panel.previousElementSibling.classList.toggle('hidden', !visible);
    }
    if (visible) {
        var savedH = resizeRuntime().infoPanelHeights[id];
        if (savedH) {
            panel.style.height = savedH + 'px';
            panel.style.maxHeight = 'none';
        } else {
            panel.style.height = '';
            panel.style.maxHeight = 'min(40vh, 520px)';
        }
        panel.removeAttribute('data-info-auto-height');
    }
}

function refreshRuleInfoLayoutAfterFeatureChange(si, gi, ri) {
    markTraceLayoutDirtyRule(si, gi, ri, 'info-panel-height');
    scheduleVirtualRowsRefresh();
    scheduleTraceAnchorLineUpdate();
    scheduleDiffMoveArrows();
}

function shouldShowRuleInfoPanelShell(si, gi, ri) {
    return effectiveRuleFeature(si, gi, ri, 'info') &&
        ruleHasFeature(si, gi, ri, 'info') &&
        !shouldRenderFullscreenDiff(si, gi, ri);
}

function removeRuleInfoPanelShellElement(panel) {
    if (!panel) return false;
    var resizer = panel.previousElementSibling &&
        panel.previousElementSibling.classList &&
        panel.previousElementSibling.classList.contains('info-panel-resizer')
        ? panel.previousElementSibling
        : null;

    if (resizer) {
        if (resizer.remove) resizer.remove();
        else if (resizer.parentNode && resizer.parentNode.removeChild) resizer.parentNode.removeChild(resizer);
    }

    if (panel.remove) panel.remove();
    else if (panel.parentNode && panel.parentNode.removeChild) panel.parentNode.removeChild(panel);
    return true;
}

function ensureRuleInfoPanelShellMounted(si, gi, ri) {
    if (!hasDOM() || !document.getElementById) return false;

    var ref = ruleRef(si, gi, ri);
    if (!ruleHasInfo(ref)) return false;

    var key = ruleKey(si, gi, ri);
    if (document.getElementById('info-content-' + key)) return true;

    var cell = document.getElementById('rule-' + key);
    if (!cell || !cell.querySelector) return false;

    var content = cell.querySelector('.rule-exp-wrap > .rule-content') ||
        cell.querySelector('.rule-content');
    if (!content || !content.insertAdjacentHTML) return false;

    content.insertAdjacentHTML('beforeend', ruleInfoPanelShellHtml(ref));
    return !!document.getElementById('info-content-' + key);
}

function updateAllInfoDisplays() {
    forEachRule(function(si, gi, ri) {
        updateRuleInfoDisplay(si, gi, ri);
    });
}

function ruleRef(si, gi, ri) {
    return RuleRefs.make(si, gi, ri);
}

function ruleHasFeature(si, gi, ri, feature) {
    var ref = ruleRef(si, gi, ri);
    if (ruleIsTextTile(ref)) return false;
    switch (feature) {
    case 'fields':
        return ruleHasFields(ref);
    case 'pinned':
        return ruleHasPinned(ref);
    case 'info':
        return ruleHasInfo(ref);
    default:
        return false;
    }
}

function normalizeSearchScopeValue(scope) {
    return 'tree-rules';
}

function featureBulkStatus(feature) {
    var total = 0;
    var visible = 0;

    if (feature === 'pinned' && pinnedFieldCount() === 0) {
        return { total: 0, visible: 0 };
    }

    forEachRule(function(si, gi, ri, rule) {
        if (!ruleHasFeature(si, gi, ri, feature)) return;
        total++;
        if (feature === 'fields'
                ? effectiveRuleFeature(si, gi, ri, 'fields')
                : rule[feature]) {
            visible++;
        }
    });

    return { total: total, visible: visible };
}

function updateBulkDetailControls() {
    var features = ['fields', 'pinned', 'info'];
    var visibleRows = 0;

    for (var i = 0; i < features.length; i++) {
        var feature = features[i];
        var status = featureBulkStatus(feature);
        var row = document.querySelector('[data-detail-row="' + feature + '"]');
        var showBtn = document.querySelector(
            '[data-detail-feature="' + feature + '"][data-detail-action="show"]'
        );
        var hideBtn = document.querySelector(
            '[data-detail-feature="' + feature + '"][data-detail-action="hide"]'
        );
        var disabled = status.total === 0;
        var showActive = !disabled && status.visible === status.total;
        var hideActive = !disabled && status.visible === 0;

        if (!disabled) visibleRows++;
        if (row) {
            row.hidden = disabled;
            row.classList.toggle('disabled', false);
        }
        if (showBtn) {
            showBtn.disabled = disabled;
            showBtn.classList.toggle('active', showActive);
            showBtn.setAttribute('aria-pressed', showActive ? 'true' : 'false');
        }
        if (hideBtn) {
            hideBtn.disabled = disabled;
            hideBtn.classList.toggle('active', hideActive);
            hideBtn.setAttribute('aria-pressed', hideActive ? 'true' : 'false');
        }
    }

    if (updateEmptyStageControls()) visibleRows++;
    updatePinnedFieldControls();
    updateDiffFieldControls();

    var detailSection = document.querySelector('[data-settings-section="details"]');
    if (detailSection) detailSection.hidden = visibleRows === 0;
}

var PINNED_FIELDS_FILTER_MIN_FIELDS = 9;

function pinnedFieldRowHtml(field, checked) {
    var key = field && field.key ? String(field.key) : '';
    var label = field && field.label ? String(field.label) : key;
    return '<label class="pinned-field-row">' +
           '<input type="checkbox" data-trace-action="toggle-pinned-field" ' +
           'data-pinned-field-key="' + htmlEscape(key) + '"' +
           (checked ? ' checked' : '') + '>' +
           '<span class="pinned-field-label">' + htmlEscape(label) + '</span>' +
           '<button type="button" class="pinned-field-only" ' +
           'data-trace-action="solo-pinned-field" ' +
           'data-pinned-field-key="' + htmlEscape(key) + '" ' +
           'title="Show only this field">only</button>' +
           '</label>';
}

function diffFieldRowHtml(field, checked) {
    var key = field && field.key ? String(field.key) : '';
    var label = field && field.label ? String(field.label) : key;
    return '<label class="pinned-field-row">' +
           '<input type="checkbox" data-trace-action="toggle-diff-field" ' +
           'data-diff-field-key="' + htmlEscape(key) + '"' +
           (checked ? ' checked' : '') + '>' +
           '<span class="pinned-field-label">' + htmlEscape(label) + '</span>' +
           '<button type="button" class="pinned-field-only" ' +
           'data-trace-action="solo-diff-field" ' +
           'data-diff-field-key="' + htmlEscape(key) + '" ' +
           'title="Include only this field in diff">only</button>' +
           '</label>';
}

function pinnedFieldPresetButtonHtml(preset, index) {
    var label = preset && preset.label ? String(preset.label) : '';
    if (!label) return '';
    return '<button type="button" class="pinned-fields-action pinned-fields-preset" ' +
           'data-trace-action="apply-pinned-field-preset" ' +
           'data-pinned-field-preset-index="' + htmlEscape(String(index)) + '" ' +
           'title="Apply field preset: ' + htmlEscape(label) + '">' +
           htmlEscape(label) + '</button>';
}

function diffFieldPresetButtonHtml(preset, index) {
    var label = preset && preset.label ? String(preset.label) : '';
    if (!label) return '';
    return '<button type="button" class="diff-fields-action diff-fields-preset" ' +
           'data-trace-action="apply-diff-field-preset" ' +
           'data-diff-field-preset-index="' + htmlEscape(String(index)) + '" ' +
           'title="Apply diff field preset: ' + htmlEscape(label) + '">' +
           htmlEscape(label) + '</button>';
}

function diffFieldsActionButtonHtml(action, label, title, attrs) {
    attrs = attrs || '';
    return '<button type="button" class="diff-fields-action" ' +
           'data-trace-action="' + action + '" ' + attrs +
           'title="' + htmlEscape(title) + '">' +
           htmlEscape(label) + '</button>';
}

function applyPinnedFieldsFilter() {
    var input = document.getElementById ? document.getElementById('pinned-fields-filter') : null;
    var list = document.getElementById ? document.getElementById('pinned-fields-list') : null;
    if (!list) return;

    var query = input && !input.hidden ? String(input.value || '').trim().toLowerCase() : '';
    var rows = list.querySelectorAll('.pinned-field-row');
    for (var i = 0; i < rows.length; i++) {
        var label = rows[i].querySelector('.pinned-field-label');
        var text = label ? String(label.textContent || '').toLowerCase() : '';
        rows[i].hidden = !!query && text.indexOf(query) === -1;
    }
}

function applyDiffFieldsFilter() {
    var input = document.getElementById ? document.getElementById('diff-fields-filter') : null;
    var list = document.getElementById ? document.getElementById('diff-fields-list') : null;
    if (!list) return;

    var query = input && !input.hidden ? String(input.value || '').trim().toLowerCase() : '';
    var rows = list.querySelectorAll('.pinned-field-row');
    for (var i = 0; i < rows.length; i++) {
        var label = rows[i].querySelector('.pinned-field-label');
        var text = label ? String(label.textContent || '').toLowerCase() : '';
        rows[i].hidden = !!query && text.indexOf(query) === -1;
    }
}

function updatePinnedFieldControls() {
    var section = document.querySelector('[data-settings-section="pinned-fields"]');
    var list = document.getElementById ? document.getElementById('pinned-fields-list') : null;
    var menu = document.getElementById ? document.getElementById('pinned-fields-menu') : null;
    var button = document.getElementById ? document.getElementById('pinned-fields-button') : null;
    var summary = document.getElementById ? document.getElementById('pinned-fields-summary') : null;
    if (!section || !list) return;

    var fields = traceNodeFields();
    var hasFields = fields.length > 0;
    section.hidden = !hasFields;
    if (!hasFields) {
        list.innerHTML = '';
        if (menu) menu.hidden = true;
        if (button) {
            if (button.classList && button.classList.remove) button.classList.remove('active');
            if (button.setAttribute) button.setAttribute('aria-expanded', 'false');
        }
        if (summary) summary.textContent = 'Default';
        return;
    }

    var active = activePinnedFieldKeys();
    var activeSet = {};
    for (var i = 0; i < active.length; i++) activeSet[active[i]] = true;

    var html = '';
    for (var f = 0; f < fields.length; f++) {
        html += pinnedFieldRowHtml(fields[f], !!activeSet[fields[f].key]);
    }
    list.innerHTML = html;

    var filter = document.getElementById ? document.getElementById('pinned-fields-filter') : null;
    if (filter) {
        var filterVisible = fields.length >= PINNED_FIELDS_FILTER_MIN_FIELDS;
        filter.hidden = !filterVisible;
        if (!filterVisible) filter.value = '';
    }
    applyPinnedFieldsFilter();

    if (summary) {
        summary.textContent = pinnedFieldsAreDefault()
            ? 'Default'
            : String(active.length) + ' / ' + String(fields.length);
    }

    var presets = tracePinnedFieldPresets();
    var actions = section.querySelector('.pinned-fields-actions');
    if (actions) {
        var actionHtml = '';
        for (var p = 0; p < presets.length; p++) {
            actionHtml += pinnedFieldPresetButtonHtml(presets[p], p);
        }
        actions.innerHTML = actionHtml;
        actions.hidden = !actionHtml;
    }
}

function updateDiffFieldControls() {
    var section = document.querySelector('[data-settings-section="diff-fields"]');
    var list = document.getElementById ? document.getElementById('diff-fields-list') : null;
    var menu = document.getElementById ? document.getElementById('diff-fields-menu') : null;
    var button = document.getElementById ? document.getElementById('diff-fields-button') : null;
    var summary = document.getElementById ? document.getElementById('diff-fields-summary') : null;
    if (!section || !list) return false;

    var fields = traceNodeFields();
    var hasFields = fields.length > 0;
    section.hidden = !hasFields;
    if (!hasFields) {
        list.innerHTML = '';
        if (menu) menu.hidden = true;
        if (button) {
            if (button.classList && button.classList.remove) button.classList.remove('active');
            if (button.setAttribute) button.setAttribute('aria-expanded', 'false');
        }
        if (summary) summary.textContent = 'None';
        return false;
    }

    var active = activeDiffFieldKeys();
    var activeSet = {};
    for (var i = 0; i < active.length; i++) activeSet[active[i]] = true;

    var html = '';
    for (var f = 0; f < fields.length; f++) {
        html += diffFieldRowHtml(fields[f], !!activeSet[fields[f].key]);
    }
    list.innerHTML = html;

    var filter = document.getElementById ? document.getElementById('diff-fields-filter') : null;
    if (filter) {
        var filterVisible = fields.length >= PINNED_FIELDS_FILTER_MIN_FIELDS;
        filter.hidden = !filterVisible;
        if (!filterVisible) filter.value = '';
    }
    applyDiffFieldsFilter();

    if (summary) {
        summary.textContent = active.length === 0
            ? 'None'
            : String(active.length) + ' / ' + String(fields.length);
    }

    var actions = section.querySelector('.diff-fields-actions');
    if (actions) {
        var actionHtml = '';
        var presets = traceDiffFieldPresets();
        for (var p = 0; p < presets.length; p++) {
            actionHtml += diffFieldPresetButtonHtml(presets[p], p);
        }
        actionHtml += diffFieldsActionButtonHtml(
            'set-all-diff-fields',
            'All',
            'Include all fields in diff',
            'data-visible="true"'
        );
        actionHtml += diffFieldsActionButtonHtml(
            'set-all-diff-fields',
            'None',
            'Do not include fields in diff',
            'data-visible="false"'
        );
        if (!diffFieldsAreDefault()) {
            actionHtml += diffFieldsActionButtonHtml(
                'reset-diff-fields',
                'Reset',
                'Reset diff fields to default'
            );
        }
        actions.innerHTML = actionHtml;
        actions.hidden = !actionHtml;
    }
    return true;
}

function updateEmptyStageControls() {
    var row = document.querySelector('[data-empty-stages-row]');
    var showBtn = document.querySelector('[data-empty-stages-action="show"]');
    var hideBtn = document.querySelector('[data-empty-stages-action="hide"]');
    var hasEmpty = traceHasEmptyStages();
    var visible = showEmptyStages();

    if (row) row.hidden = !hasEmpty;
    if (showBtn) {
        showBtn.disabled = !hasEmpty;
        showBtn.classList.toggle('active', hasEmpty && visible);
        showBtn.setAttribute('aria-pressed', hasEmpty && visible ? 'true' : 'false');
    }
    if (hideBtn) {
        hideBtn.disabled = !hasEmpty;
        hideBtn.classList.toggle('active', hasEmpty && !visible);
        hideBtn.setAttribute('aria-pressed', hasEmpty && !visible ? 'true' : 'false');
    }

    return hasEmpty;
}

function setAllRuleFeature(feature, visible, ev) {
    TraceActions.detail.setAllRuleFeature(feature, visible, ev);
}

function ruleTitleHtml(ref) {
    var name = ruleDisplayName(ref);
    var num = rawRuleIndex(ref.si, ref.gi, ref.ri) + 1;
    return '<span class="rule-num">' + num + '.</span>' +
           '<span class="rule-title-name" data-search-kind="rule" ' +
           'data-search-si="' + ref.si + '" data-search-gi="' + ref.gi +
           '" data-search-ri="' + ref.ri + '" ' +
           'data-search-label="' + htmlEscape(name) + '">' +
           htmlEscape(name) + '</span>';
}

function fullscreenState() {
    return TraceState.fullscreenState(currentUiState());
}

function fullscreenCurrentRule() {
    return TraceState.fullscreenCurrentRule(currentUiState());
}

function isFullscreenOpen() {
    return !!fullscreenCurrentRule();
}

function fullscreenModeForRule(ref) {
    return isActiveDiffRule(ref.si, ref.gi, ref.ri) ? 'diff' : 'rule';
}

function setFullscreenState(mode, ref) {
    var result;
    if (!mode || !ref) {
        result = TraceState.clearFullscreen(currentUiState());
        if (result && result.changed) bumpRuntimeEpoch('fullscreen');
        return result;
    }

    if (mode === 'diff' && !isActiveDiffRule(ref.si, ref.gi, ref.ri)) {
        mode = 'rule';
    }

    result = TraceState.setFullscreenState(currentUiState(), mode, ref);
    if (result && result.changed) bumpRuntimeEpoch('fullscreen');
    return result;
}

function syncFullscreenModeWithDiff() {
    var ref = fullscreenCurrentRule();
    if (!ref) return false;

    var mode = fullscreenModeForRule(ref);
    var result = TraceState.syncFullscreenMode(currentUiState(), mode);
    if (result && result.changed) bumpRuntimeEpoch('fullscreen');
    return result.changed;
}

function isFullscreenDiff() {
    var ref = fullscreenCurrentRule();
    return !!ref &&
        fullscreenState().mode === 'diff' &&
        isActiveDiffRule(ref.si, ref.gi, ref.ri);
}

function isFullscreenRule(si, gi, ri) {
    return sameRuleRefValues(fullscreenCurrentRule(), si, gi, ri);
}

function updateFullscreenButton(si, gi, ri) {
    var btn = document.getElementById('fsbtn-' + si + '-' + gi + '-' + ri);
    if (!btn) return;

    btn.classList.remove('active');
    btn.title = 'Enter fullscreen';
    btn.setAttribute('aria-label', 'Enter fullscreen');
    btn.innerHTML = traceIconSvg('enter-fullscreen');
}

function setFullscreenFocus(si, gi, ri, mode) {
    var ref = ruleRef(si, gi, ri);
    return withFullscreenTransition('set-fullscreen-focus', {
        ruleKey: ruleKey(si, gi, ri),
        mode: mode || fullscreenModeForRule(ref)
    }, function() {
        var previous = fullscreenCurrentRule();
        hideFullscreenScrollbars();
        setFullscreenState(mode || fullscreenModeForRule(ref), ref);

        if (previous) {
            updateRuleDisplay(previous.si, previous.gi, previous.ri);
            updateFullscreenButton(previous.si, previous.gi, previous.ri);
            if (isDiffActive()) renderDiffInlineTrees(activeDiffResultForRender());
        }

        openRulePath(si, gi, ri);

        toggleFullscreenBodyClass('fullscreen-active', true, 'fullscreen-active-class');
        updateGroupLayoutWidth(si, gi);
        updateStageLayoutWidth(si);
        updateStageDisplay(si);
        updateGroupDisplay(si, gi);
        refreshVirtualRowsNow();
        updateCycleButtonLabel();
        updateFullscreenButton(si, gi, ri);
        updateSearchLabelHighlights(currentSearchQuery(), currentSearchScope());
        renderFullscreenRuleImmediately(si, gi, ri, currentSearchQuery(), currentSearchScope());
        return true;
    });
}

function enterRuleFullscreen(si, gi, ri) {
    TraceActions.fullscreen.enterRuleFullscreen(si, gi, ri);
}

function enterDiffFullscreen(si, gi, ri) {
    TraceActions.fullscreen.enterDiffFullscreen(si, gi, ri);
}

function setFullscreenRule(si, gi, ri) {
    TraceActions.fullscreen.setFullscreenRule(si, gi, ri);
}

function hideFullscreenScrollbars() {
    var fullscreen = fullscreenRuntime();
    if (fullscreen.fullscreenScrollbarRevealTimer !== null) {
        clearTimeout(fullscreen.fullscreenScrollbarRevealTimer);
        fullscreen.fullscreenScrollbarRevealTimer = null;
    }
    toggleFullscreenBodyClass('fullscreen-scrollbar-active', false, 'fullscreen-scrollbar-class');
}

function showFullscreenScrollbarsForUserInput(ev) {
    if (!isFullscreenOpen() || !hasDOM() || !document.body) return;
    if (isFullscreenDiff()) return;
    if (ev && ev.target && ev.target.closest &&
        !ev.target.closest('.rule-cell.fullscreen-rule .rule-tree-wrap')) {
        return;
    }

    toggleFullscreenBodyClass('fullscreen-scrollbar-active', true, 'fullscreen-scrollbar-class');
    var fullscreenDomGeneration = currentFullscreenDomGeneration();
    var fullscreen = fullscreenRuntime();
    if (fullscreen.fullscreenScrollbarRevealTimer !== null) clearTimeout(fullscreen.fullscreenScrollbarRevealTimer);
    fullscreen.fullscreenScrollbarRevealTimer = scheduleRuntimeTimeout(function() {
        fullscreen.fullscreenScrollbarRevealTimer = null;
        toggleFullscreenBodyClass('fullscreen-scrollbar-active', false, 'fullscreen-scrollbar-class', {
            fullscreenDomGeneration: fullscreenDomGeneration
        });
    }, 900, {
        epochScopes: ['trace', 'fullscreen'],
        label: 'fullscreen-scrollbar-hide'
    });
}

function fullscreenScrollPaneTarget(target) {
    if (!target || !target.closest) return null;
    return target.closest('.rule-cell.fullscreen-rule .rule-tree-wrap') ||
        target.closest('.rule-cell.fullscreen-rule .rule-info-panel');
}

function fullscreenScrollEventShouldBeContained(ev) {
    if (!isFullscreenOpen() || !ev) return false;
    if (ev.type === 'wheel' && ev.ctrlKey) return false;

    return !fullscreenScrollPaneTarget(ev.target);
}

function containFullscreenShellScroll(ev) {
    if (!fullscreenScrollEventShouldBeContained(ev)) return;
    if (ev.cancelable === false || !ev.preventDefault) return;
    ev.preventDefault();
}

function isScrollKey(ev) {
    if (!ev) return false;
    return ev.key === 'ArrowUp' ||
        ev.key === 'ArrowDown' ||
        ev.key === 'PageUp' ||
        ev.key === 'PageDown' ||
        ev.key === 'Home' ||
        ev.key === 'End' ||
        ev.key === ' ';
}

function initFullscreenScrollbarReveal() {
    if (fullscreenRuntime().fullscreenScrollbarRevealReady) return;
    fullscreenRuntime().fullscreenScrollbarRevealReady = true;
    document.addEventListener('wheel', containFullscreenShellScroll, { capture: true, passive: false });
    document.addEventListener('touchmove', containFullscreenShellScroll, { capture: true, passive: false });
    document.addEventListener('wheel', showFullscreenScrollbarsForUserInput, { passive: true });
    document.addEventListener('touchmove', showFullscreenScrollbarsForUserInput, { passive: true });
    document.addEventListener('keydown', function(ev) {
        if (!isScrollKey(ev)) return;
        showFullscreenScrollbarsForUserInput(ev);
    }, true);
}

function revealRuleAfterFullscreenExit(ref) {
    if (!ref || !hasDOM()) return;

    ensureRuleVisible(ref.si, ref.gi, ref.ri);
    if (ruleIntersectsTraceViewport(ref.si, ref.gi, ref.ri)) return;

    scrollRuleIntoView(ref.si, ref.gi, ref.ri);
    requestRuleTreeRenderIfVisible(ref.si, ref.gi, ref.ri,
                                   currentSearchQuery(), false, currentSearchScope());
}

function closeFullscreen(ev, revealRef) {
    if (ev) {
        ev.stopPropagation();
        ev.preventDefault();
    }
    if (!isFullscreenOpen()) return;

    var previous = fullscreenCurrentRule();
    return withFullscreenTransition('close-fullscreen', {
        ruleKey: ruleKey(previous.si, previous.gi, previous.ri),
        revealRuleKey: revealRef ? ruleKey(revealRef.si, revealRef.gi, revealRef.ri) : ''
    }, function() {
        saveVisibleTreeMaterializerAnchors();
        var centerDiffPair = !revealRef && isFullscreenDiff();
        var target = revealRef || previous;
        setFullscreenState(null, null);
        hideFullscreenScrollbars();
        toggleFullscreenBodyClass('fullscreen-active', false, 'fullscreen-active-class');
        clearFullscreenBackgroundInert();
        removeFullscreenOverlay();
        updateRuleDisplay(previous.si, previous.gi, previous.ri);
        refreshVirtualRowsNow();
        renderRuleTreeImmediatelyIfMounted(previous.si, previous.gi, previous.ri,
                                           currentSearchQuery(), false, currentSearchScope());
        updateFullscreenButton(previous.si, previous.gi, previous.ri);
        updateSearchLabelHighlights(currentSearchQuery(), currentSearchScope());
        if (isDiffActive()) renderDiffInlineTrees(activeDiffResultForRender());
        if (centerDiffPair) {
            centerDiffPairInTrace();
        } else {
            revealRuleAfterFullscreenExit(target);
        }
        return true;
    });
}

function closeRuleFullscreen(ev) {
    TraceActions.fullscreen.closeRuleFullscreen(ev);
}

function closeRuleFullscreenToRule(si, gi, ri, ev) {
    TraceActions.fullscreen.closeRuleFullscreenToRule(si, gi, ri, ev);
}

function isTitleControlClick(ev) {
    return !!(ev && ev.target && ev.target.closest &&
        ev.target.closest('button, input, select, textarea, label'));
}

function closeRuleFullscreenFromTitle(ev) {
    TraceActions.fullscreen.closeRuleFullscreenFromTitle(ev);
}

function closeRuleFullscreenToRuleFromTitle(si, gi, ri, ev) {
    TraceActions.fullscreen.closeRuleFullscreenToRuleFromTitle(si, gi, ri, ev);
}

function closeFullscreenOnOutsideClick(ev) {
    TraceActions.fullscreen.closeFullscreenOnOutsideClick(ev);
}

function toggleRuleFullscreen(si, gi, ri, ev) {
    TraceActions.fullscreen.toggleRuleFullscreen(si, gi, ri, ev);
}

function toggleRuleFields(si, gi, ri, ev) {
    TraceActions.detail.toggleRuleFields(si, gi, ri, ev);
}

function otherActiveDiffRule(si, gi, ri) {
    if (!isDiffActive()) return null;

    if (sameRuleRef(diffRuntime().diffA, si, gi, ri) &&
        diffRuntime().diffB && !sameRuleRef(diffRuntime().diffB, si, gi, ri)) {
        return diffRuntime().diffB;
    }

    if (sameRuleRef(diffRuntime().diffB, si, gi, ri) &&
        diffRuntime().diffA && !sameRuleRef(diffRuntime().diffA, si, gi, ri)) {
        return diffRuntime().diffA;
    }

    return null;
}

function toggleRulePinned(si, gi, ri, ev) {
    TraceActions.detail.toggleRulePinned(si, gi, ri, ev);
}

function disableOtherFullscreenDiffFeature(si, gi, ri, feature) {
    if (!isFullscreenDiff()) return null;

    var other = otherActiveDiffRule(si, gi, ri);
    if (!other || !ruleState(other.si, other.gi, other.ri)[feature]) return null;

    var change = TraceState.setRuleFeature(currentUiState(), other.si, other.gi, other.ri, feature, false);
    if (change.changed && feature === 'fields') {
        clearRuleFieldRowSessionOverrides(other.si, other.gi, other.ri);
    }
    return change.changed ? other : null;
}

function toggleRuleInfo(si, gi, ri, ev) {
    TraceActions.detail.toggleRuleInfo(si, gi, ri, ev);
}

function rerenderRuleAfterFeatureChange(si, gi, ri) {
    requestRuleTreeRenderIfVisible(si, gi, ri, currentSearchQuery(), false, currentSearchScope());
    rerenderFullscreenAfterFeatureChange(si, gi, ri);
}

function renderRuleInfoPanel(visualCtx, si, gi, ri, query, scope) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        scope = query;
        query = ri;
        ri = gi;
        gi = si;
        si = visualCtx;
        return runVisualSurfaceCommitNow(
            'rule-info-panel',
            ['rule-info', 'search-marks'],
            ['trace', 'render'],
            function(ctx) {
                return renderRuleInfoPanel(ctx, si, gi, ri, query, scope);
            }
        );
    }

    var container = document.getElementById('info-content-' + si + '-' + gi + '-' + ri);
    if (!container) {
        ensureRuleInfoPanelShellMounted(si, gi, ri);
        container = document.getElementById('info-content-' + si + '-' + gi + '-' + ri);
    }
    if (!container) return false;

    query = query || '';
    scope = query ? (scope || currentSearchScope()) : '';
    var rule = ruleState(si, gi, ri);
    var prev = rule.infoRendered;
    if (prev && infoPanelContentHasContent(container) &&
        prev.query === query &&
        prev.scope === scope) return false;

    var rendered = false;
    var ref = ruleRef(si, gi, ri);
    var commitOptions = {
        label: 'rule-info-panel',
        ref: ref,
        detachedReason: 'detached_container'
    };
    if (!hasDOM() || !document.getElementById) commitOptions.target = container;
    ruleInfoSurface.commit(visualCtx, commitOptions, function(target) {
        var snapshot = captureInfoScrollSnapshot(target);
        TraceStore.ensureRulePayload(
            currentTraceStore(),
            TraceStore.ruleHandleForRef(currentTraceStore(), ref)
        );
        if (!ruleHasInfo(ref)) {
            rule.info = false;
            rule.infoRendered = null;
            target.innerHTML = '';
            updateRuleInfoDisplay(si, gi, ri);
            rendered = true;
            return;
        }
        renderInfoPanel(target, traceRuleInfo(si, rawRuleIndex(si, gi, ri)), query, scope, si, gi, ri);
        rule.infoRendered = {
            query: query,
            scope: scope
        };
        restoreRuleInfoPaneScroll(si, gi, ri);
        restoreInfoScrollSnapshot(target, snapshot);
        if (typeof captureMountedRulePaneScroll === 'function') {
            captureMountedRulePaneScroll(si, gi, ri);
        }
        reactivateActiveSearchMatchForRule(si, gi, ri);
        rendered = true;
    });
    return rendered;
}

function infoTabBarHtml(model, activeId, query, scope, si, gi, ri, options) {
    options = options || {};
    if (!model || !model.tabs || !model.tabs.length) return '';
    var html = '<div class="info-tabs-head"><div class="info-tabs" role="tablist">';
    for (var i = 0; i < model.tabs.length; i++) {
        var tab = model.tabs[i];
        var active = tab.id === activeId;
        var tabSearchContext = {
            type: 'rule',
            si: si,
            gi: gi,
            ri: ri,
            tabId: tab.id,
            section: Number.isInteger(tab.topIndex) ? tab.topIndex : i,
            part: 'title'
        };
        var targetAttrs = infoTargetDataAttrs(tab.targets, tab.primaryTarget, options);
        if (targetAttrs) {
            targetAttrs += ' data-info-target-hover-mode="' + htmlEscape(tab.hoverMode || 'preview') + '"';
        }
        html += '<span class="info-tab-item' +
            (tab.temporary ? ' temporary' : '') +
            (tab.fieldDetails ? ' field-details' : '') +
            (tab.fieldDetailsPinned ? ' pinned' : '') +
            (active ? ' active' : '') + '">';
        html += '<button type="button" class="info-tab' + (active ? ' active' : '') + '"' +
                traceActionAttr('set-info-tab', si, gi, ri) +
                targetAttrs +
                ' data-tab-id="' + htmlEscape(tab.id) + '"' +
                ' role="tab" aria-selected="' + (active ? 'true' : 'false') + '">' +
                (tab.fieldDetails
                    ? '<span class="info-tab-label field-details-label">' +
                            traceIconSvg('info') +
                        '<span class="info-tab-title">' +
                            infoHighlight(tab.title, query, scope, tabSearchContext) +
                        '</span></span>'
                    : infoHighlight(tab.title, query, scope, tabSearchContext)) +
                '</button>';
        if (tab.fieldDetails) {
            if (tab.fieldDetailsPinned) {
                html += '<button type="button" class="info-tab-pin pinned" disabled ' +
                    'title="Pinned details" aria-label="Pinned details">' +
                    traceIconSvg('drawing-pin-filled') + '</button>';
            } else if (tab.pinnable) {
                html += '<button type="button" class="info-tab-pin" ' +
                    traceActionAttr('pin-info-metadata-details', si, gi, ri) +
                    ' data-tab-id="' + htmlEscape(tab.id) + '"' +
                    ' title="Pin details" aria-label="Pin details">' +
                    traceIconSvg('drawing-pin') + '</button>';
            }
        }
        if (tab.closeable) {
            html += '<button type="button" class="info-tab-close" ' +
                traceActionAttr('close-info-metadata-details', si, gi, ri) +
                ' data-tab-id="' + htmlEscape(tab.id) + '"' +
                ' title="Close details" aria-label="Close details">' +
                traceIconSvg('cross') + '</button>';
        }
        html += '</span>';
    }
    html += '</div></div>';
    return html;
}

function infoTargetAttrValue(value) {
    return htmlEscape(JSON.stringify(value));
}

function infoTargetDataAttrs(targets, primary, options) {
    targets = Array.isArray(targets) ? targets : [];
    if (!targets.length && !primary) return '';
    options = options || {};

    var attrs = ' data-info-targets="' + infoTargetAttrValue(targets) + '"';
    if (primary) {
        attrs += ' data-info-primary-target="' + infoTargetAttrValue(primary) + '"';
    }
    if (options.idScope) {
        attrs += ' data-info-id-scope="' + htmlEscape(options.idScope) + '"';
    }
    return attrs;
}

function infoSearchContextWith(context, extra) {
    return searchContextWith(context || null, extra || null);
}

function infoRecordSearchContext(context, sectionIndex, part, extra) {
    return infoSearchContextWith(infoSearchContextWith(context || null, {
        section: sectionIndex,
        part: part
    }), extra || null);
}

function infoSectionTargetAttrs(section, si, gi, ri, options) {
    var targets = section && Array.isArray(section.targets) ? section.targets : [];
    var primary = section && section.primaryTarget ? section.primaryTarget : null;
    if (!targets.length && !primary) return '';

    return traceActionAttr('activate-info-target', si, gi, ri) +
        infoTargetDataAttrs(targets, primary, options);
}

function infoTargetStableKey(target) {
    try {
        return JSON.stringify(target || null);
    } catch (err) {
        return String(target);
    }
}

function appendInfoTargetsUnique(result, seen, targets) {
    targets = Array.isArray(targets) ? targets : [];
    for (var i = 0; i < targets.length; i++) {
        if (!targets[i]) continue;
        var key = infoTargetStableKey(targets[i]);
        if (seen[key]) continue;
        seen[key] = true;
        result.push(targets[i]);
    }
}

function collectInfoSectionTargets(section, result, seen) {
    if (Array.isArray(section)) {
        for (var a = 0; a < section.length; a++) {
            collectInfoSectionTargets(section[a], result, seen);
        }
        return;
    }
    if (!section) return;
    appendInfoTargetsUnique(result, seen, section.targets);
    if (section.primaryTarget) appendInfoTargetsUnique(result, seen, [section.primaryTarget]);

    if (section.type === 'tab') {
        var nested = section.sections || [];
        for (var i = 0; i < nested.length; i++) collectInfoSectionTargets(nested[i], result, seen);
    } else if (section.type === 'graphModel') {
        var nodes = section.nodes || [];
        for (var n = 0; n < nodes.length; n++) {
            collectInfoSectionTargets(nodes[n], result, seen);
        }
        var edges = section.edges || [];
        for (var e = 0; e < edges.length; e++) {
            collectInfoSectionTargets(edges[e], result, seen);
        }
    } else if (section.type === 'switcher') {
        var switchers = section.options || [];
        for (var v = 0; v < switchers.length; v++) {
            var widgets = switchers[v] && Array.isArray(switchers[v].widgets) && switchers[v].widgets.length
                ? switchers[v].widgets
                : (switchers[v] && switchers[v].widget ? [switchers[v].widget] : []);
            collectInfoSectionTargets(widgets, result, seen);
        }
    }
}

function infoSectionTargetSummary(section) {
    var targets = [];
    var seen = {};
    collectInfoSectionTargets(section, targets, seen);
    var primary = section && section.primaryTarget ? section.primaryTarget : (targets[0] || null);
    return { targets: targets, primary: primary };
}

function warningSeverityLabel(severity) {
    if (severity === 'error') return 'Error';
    if (severity === 'info') return 'Info';
    return 'Warning';
}

function infoWarningHtml(section, query, scope, si, gi, ri, sectionIndex, searchContext, options) {
    var severity = section.severity || 'warning';
    var attrs = infoSectionTargetAttrs(section, si, gi, ri, options);
    var interactive = attrs ? ' role="button" tabindex="0"' : '';
    var html = '<div class="info-warning info-warning-' + htmlEscape(severity) + '"' +
        attrs + interactive + '>';
    html += '<div class="info-warning-head">' +
        '<span class="info-warning-badge">' + warningSeverityLabel(severity) + '</span>' +
        '</div>';
    if (section.message) {
        html += '<div class="info-warning-message">' +
            infoHighlight(section.message, query, scope,
                infoRecordSearchContext(searchContext, sectionIndex, 'message')) + '</div>';
    }
    if (section.details) {
        html += '<div class="info-warning-details">' +
            infoHighlight(section.details, query, scope,
                infoRecordSearchContext(searchContext, sectionIndex, 'details')) + '</div>';
    }
    html += '</div>';
    return html;
}

function infoGraphModelHtml(section, query, scope, si, gi, ri, sectionIndex, options) {
    if (typeof InfoGraphModel === 'undefined' || !InfoGraphModel || !InfoGraphModel.render) {
        return '<div class="info-graph-model info-graph-model-fallback" data-graph-layout="fallback">' +
            '<div class="info-graph-model-fallback-title">Graph layout unavailable</div>' +
            '</div>';
    }
    return InfoGraphModel.render(section, query, scope, {
        si: si,
        gi: gi,
        ri: ri,
        sectionIndex: sectionIndex,
        idScope: options && options.idScope ? options.idScope : '',
        domIndex: options && options.domIndex !== undefined ? options.domIndex : sectionIndex,
        searchContext: options && options.infoSearchContext || null
    }, {
        infoTargetDataAttrs: infoTargetDataAttrs,
        traceActionAttr: traceActionAttr
    });
}

function infoSwitcherSelectorOptionsHtml(section, activeId, query, scope, si, gi, ri, switcherKey, searchContext, sectionIndex, options) {
    var switchers = section && section.options || [];
    var html = '';
    for (var i = 0; i < switchers.length; i++) {
        var switcher = switchers[i] || {};
        var id = infoSwitcherOptionId(switcher, i);
        var active = id === activeId;
        var summary = infoSectionTargetSummary(
            Array.isArray(switcher.widgets) && switcher.widgets.length ? switcher.widgets : switcher.widget
        );
        html += '<button type="button" class="info-switcher-option' + (active ? ' active' : '') + '"' +
            traceActionAttr('set-info-switcher', si, gi, ri) +
            infoTargetDataAttrs(summary.targets, summary.primary, options) +
            ' data-switcher-key="' + htmlEscape(switcherKey) + '"' +
            ' data-switcher-id="' + htmlEscape(id) + '"' +
            ' role="tab" aria-selected="' + (active ? 'true' : 'false') + '">' +
            infoHighlight(
                switcher.title || id,
                query,
                scope,
                infoRecordSearchContext(searchContext, sectionIndex, 'switcher-title', {
                    switcherKey: switcherKey,
                    switcherId: id
                })
            ) +
            '</button>';
    }
    return html;
}

function infoSwitcherSelectorHtml(section, activeId, query, scope, si, gi, ri, switcherKey, searchContext, switcherDepth, sectionIndex, options) {
    var switchers = section && section.options || [];
    if (!switchers.length) return '';
    return '<div class="info-switcher-selector" data-info-switcher-selector-depth="' +
        htmlEscape(String(switcherDepth || 0)) + '" role="tablist">' +
        infoSwitcherSelectorOptionsHtml(
            section,
            activeId,
            query,
            scope,
            si,
            gi,
            ri,
            switcherKey,
            searchContext,
            sectionIndex,
            options
        ) +
        '</div>';
}

function infoSwitcherOptionWidgets(switcher) {
    if (switcher && Array.isArray(switcher.widgets) && switcher.widgets.length) return switcher.widgets;
    if (switcher && switcher.widget) return [switcher.widget];
    return [];
}

function infoSwitcherNestedRailHtml(widgets, query, scope, si, gi, ri, sectionIndex, options) {
    widgets = widgets || [];
    options = options || {};
    var html = '';
    var parentPath = options.switcherPath || '';
    var childDepth = Number.isFinite(Number(options.switcherDepth))
        ? Number(options.switcherDepth)
        : 0;
    var parentSearchContext = options.infoSearchContext || null;
    for (var i = 0; i < widgets.length; i++) {
        var child = widgets[i] || {};
        if (child.type !== 'switcher') continue;
        var childPath = infoSwitcherSectionPath(parentPath, sectionIndex, i, true);
        var childKey = infoSwitcherKey(child, sectionIndex, childPath);
        var childActiveId = activeInfoSwitcherId(child, si, gi, ri, sectionIndex, childPath);
        var childActive = infoSwitcherOptionById(child, childActiveId) || (child.options || [])[0] || null;
        var childSearchContext = infoSearchContextWith(parentSearchContext, {
            switcherKey: childKey,
            switcherId: childActiveId
        });
        var childWidgets = infoSwitcherOptionWidgets(childActive);
        html += infoSwitcherNestedRailHtml(
            childWidgets,
            query,
            scope,
            si,
            gi,
            ri,
            sectionIndex,
            {
                switcherPath: infoSwitcherDescendantPath(childPath, childActiveId),
                switcherDepth: childDepth + 1,
                infoSearchContext: childSearchContext,
                idScope: options && options.idScope
            }
        );
        html += infoSwitcherSelectorHtml(
            child,
            childActiveId,
            query,
            scope,
            si,
            gi,
            ri,
            childKey,
            parentSearchContext,
            childDepth,
            sectionIndex,
            options
        );
    }
    return html;
}

function infoSwitcherBodyHtml(section, activeSwitcher, activeId, query, scope, si, gi, ri, sectionIndex, options) {
    if (activeSwitcher && (activeSwitcher.widget || (Array.isArray(activeSwitcher.widgets) && activeSwitcher.widgets.length))) {
        var widgets = infoSwitcherOptionWidgets(activeSwitcher);
        var switcherPath = options && options.switcherPath
            ? infoSwitcherDescendantPath(options.switcherPath, activeId)
            : '';
        return '<div class="info-switcher-body" role="tabpanel">' +
            infoSectionsHtml(
                widgets,
                query,
                scope,
                si,
                gi,
                ri,
                {
                    disableSectionIds: true,
                    sharedSectionIndex: sectionIndex,
                    idScope: options && options.idScope ? options.idScope : '',
                    embedded: true,
                    showTitle: section.showChildTitles === true,
                    domIndex: options && options.domIndex !== undefined ? options.domIndex : sectionIndex,
                    switcherPath: switcherPath,
                    switcherDepth: (options && Number.isFinite(Number(options.switcherDepth))
                        ? Number(options.switcherDepth) + 1
                        : 1),
                    suppressSwitcherHeads: true,
                    infoSearchContext: options && options.infoSearchContext || null
                }
            ) +
            '</div>';
    }
    return '<div class="info-empty">No options</div>';
}

function infoSwitcherHtml(section, query, scope, si, gi, ri, sectionIndex, options) {
    var switchers = section && section.options || [];
    var switcherPath = options && options.switcherPath !== undefined
        ? options.switcherPath
        : String(sectionIndex);
    var switcherDepth = options && Number.isFinite(Number(options.switcherDepth))
        ? Number(options.switcherDepth)
        : 0;
    var switcherKey = infoSwitcherKey(section, sectionIndex, switcherPath);
    var activeId = activeInfoSwitcherId(section, si, gi, ri, sectionIndex, switcherPath);
    var activeSwitcher = infoSwitcherOptionById(section, activeId) || switchers[0] || null;
    var parentSearchContext = options && options.infoSearchContext || null;
    var activeSearchContext = infoSearchContextWith(parentSearchContext, {
        switcherKey: switcherKey,
        switcherId: activeId
    });

    var suppressHead = options && options.suppressSwitcherHeads === true;
    var html = '<div class="info-switcher' + (suppressHead ? ' info-switcher-headless' : '') +
        '" data-info-switcher-key="' + htmlEscape(switcherKey) +
        '" data-info-switcher-depth="' + htmlEscape(String(switcherDepth)) + '">';
    if (!suppressHead) {
        html += '<div class="info-switcher-head">';
        html += '<div class="info-switcher-title">' +
            infoHighlight(section.title || 'Switchers', query, scope,
                infoRecordSearchContext(parentSearchContext, sectionIndex, 'title')) + '</div>';
        html += '<div class="info-switcher-control-rail">';
        html += infoSwitcherNestedRailHtml(
            infoSwitcherOptionWidgets(activeSwitcher),
            query,
            scope,
            si,
            gi,
            ri,
            sectionIndex,
            {
                switcherPath: infoSwitcherDescendantPath(switcherPath, activeId),
                switcherDepth: switcherDepth + 1,
                infoSearchContext: activeSearchContext,
                idScope: options && options.idScope
            }
        );
        html += infoSwitcherSelectorHtml(
            section,
            activeId,
            query,
            scope,
            si,
            gi,
            ri,
            switcherKey,
            parentSearchContext,
            switcherDepth,
            sectionIndex,
            options
        );
        html += '</div>';
        html += '</div>';
    }
    html += infoSwitcherBodyHtml(
        section,
        activeSwitcher,
        activeId,
        query,
        scope,
        si,
        gi,
        ri,
        sectionIndex,
        {
            idScope: options && options.idScope,
            domIndex: options && options.domIndex,
            switcherPath: switcherPath,
            switcherDepth: switcherDepth,
            suppressSwitcherHeads: suppressHead,
            infoSearchContext: activeSearchContext
        }
    );
    html += '</div>';
    return html;
}

function infoFieldDetailsHeaderHtml(section) {
    return '<div class="info-metadata-details-head">' +
        '<div class="info-metadata-details-key">' + htmlEscape(section.label || 'Operator') + '</div>' +
        '<div class="info-metadata-details-node">' + htmlEscape(section.nodeLabel || '') + '</div>' +
        '</div>';
}

function infoFieldDetailsSearchContext(baseContext, fieldKey, metaIndex) {
    metaIndex = infoFieldDetailsMetaIndex(metaIndex);
    return infoSearchContextWith(baseContext, {
        fieldDetail: true,
        fieldKey: fieldKey || '',
        row: 'attr:' + metaIndex,
        metaIndex: metaIndex,
        tabId: INFO_FIELD_DETAILS_TAB_ID
    });
}

function infoFieldDetailsTableHtml(section, query, scope, si, gi, ri, sectionIndex, options) {
    var rows = section && section.rows || [];
    var html = '<div class="info-table-wrap info-metadata-table-wrap">' +
        '<table class="info-table info-metadata-table info-table-mono-value">';
    for (var r = 0; r < rows.length; r++) {
        var row = rows[r] || {};
        var id = infoFieldDetailsAnchorId(si, gi, ri, row.metaIndex, options);
        var rowSearchContext = infoFieldDetailsSearchContext(
            options && options.infoSearchContext,
            row.fieldKey || row.key || '',
            row.metaIndex
        );
        html += '<tr id="' + htmlEscape(id) + '" class="info-metadata-detail-row">' +
            '<td>' + infoHighlight(row.key || '', query, scope,
                infoRecordSearchContext(rowSearchContext, sectionIndex, 'key', { row: r })) + '</td>' +
            '<td>' + infoHighlight(row.value || '', query, scope,
                infoRecordSearchContext(rowSearchContext, sectionIndex, 'value', { row: r })) + '</td>' +
            '</tr>';
    }
    html += '</table></div>';
    return html;
}

function infoFieldDetailsWidgetTitleHtml(section, detail, query, scope, sectionIndex, groupSearchContext, widgetIndex) {
    var title = detail && detail.title ? String(detail.title) : '';
    if (!title || title === String(section.key || '')) return '';
    return '<div class="info-section-title info-metadata-detail-widget-title">' +
        infoHighlight(title, query, scope,
            infoRecordSearchContext(groupSearchContext, sectionIndex, 'widget-title', { widget: widgetIndex })) +
        '</div>';
}

function infoFieldDetailsGroupHtml(section, query, scope, si, gi, ri, sectionIndex, options) {
    var id = infoFieldDetailsAnchorId(si, gi, ri, section.metaIndex, options);
    var details = section.details || [];
    var single = details.length === 1;
    var groupSearchContext = infoFieldDetailsSearchContext(
        options && options.infoSearchContext,
        section.fieldKey || section.key || '',
        section.metaIndex
    );
    var html = '<div id="' + htmlEscape(id) + '" class="' +
        (single ? 'info-metadata-detail-single' : 'info-metadata-detail-group') + '">' +
        '<div class="info-metadata-detail-group-head">' +
        '<div class="info-metadata-detail-group-key">' +
        infoHighlight(section.key || '', query, scope,
            infoRecordSearchContext(groupSearchContext, sectionIndex, 'title')) +
        '</div>';
    html += '</div>';

    for (var i = 0; i < details.length; i++) {
        var detail = details[i] || {};
        html += '<div class="info-metadata-detail-widget">';
        if (!single) {
            html += infoFieldDetailsWidgetTitleHtml(
                section,
                detail,
                query,
                scope,
                sectionIndex,
                groupSearchContext,
                i
            );
        }
        if (detail.type === 'tab') {
            html += infoSectionsHtml(detail.sections || [], query, scope, si, gi, ri, {
                disableSectionIds: true,
                sharedSectionIndex: i,
                idScope: options && options.idScope,
                infoSearchContext: groupSearchContext,
                embedded: true
            });
        } else {
            html += infoSectionBodyHtml(detail, query, scope, si, gi, ri, i, {
                idScope: options && options.idScope,
                domIndex: i,
                infoSearchContext: groupSearchContext,
                embedded: true
            });
        }
        html += '</div>';
    }
    html += '</div>';
    return html;
}

function infoTextContentHtml(section, query, scope, searchContext, sectionIndex) {
    var numbered = section.wrap === false && section.lineNumbers === true;
    var classes = 'info-text' +
        (section.wrap === false ? ' info-text-unwrapped' : '') +
        (numbered ? ' info-text-numbered' : '');
    var content = String(section.content == null ? '' : section.content);

    if (numbered) {
        var lines = content.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
        var html = '<div class="' + classes + '">';
        for (var i = 0; i < lines.length; i++) {
            html += '<div class="info-text-line">' +
                '<span class="info-text-line-number">' + (i + 1) + '</span>' +
                '<span class="info-text-line-content">' +
                infoHighlight(lines[i], query, scope,
                    infoRecordSearchContext(searchContext, sectionIndex, 'line', { line: i })) +
                '</span></div>';
        }
        html += '</div>';
        return html;
    }

    return '<div class="' + classes + '">' +
        infoHighlight(content, query, scope,
            infoRecordSearchContext(searchContext, sectionIndex, 'content')) + '</div>';
}

function infoListHtml(section, query, scope, searchContext, sectionIndex) {
    var items = section && section.items || [];
    var ordered = section && section.ordered === true;
    var mono = section && section.monospace || {};
    var classes = 'info-list info-list-' + (section && section.layout === 'stack' ? 'stack' : 'flow') +
        (ordered ? ' info-list-ordered' : '');
    if (mono.text) classes += ' info-list-mono-text';
    if (mono.detail) classes += ' info-list-mono-detail';
    var html = '<div class="' + classes + '">';
    for (var i = 0; i < items.length; i++) {
        var item = items[i] || {};
        html += '<div class="info-list-item">';
        if (ordered) {
            html += '<span class="info-list-marker">' + String(i + 1) + '.</span>';
        }
        html += '<span class="info-list-main">';
        html += '<span class="info-list-text">' +
            infoHighlight(item.text, query, scope,
                infoRecordSearchContext(searchContext, sectionIndex, 'list-text', { row: i })) + '</span>';
        if (item.detail) {
            html += '<span class="info-list-detail">' +
                infoHighlight(item.detail, query, scope,
                    infoRecordSearchContext(searchContext, sectionIndex, 'list-detail', { row: i })) + '</span>';
        }
        html += '</span></div>';
    }
    html += '</div>';
    return html;
}

function infoTableClassName(section) {
    var mono = section && section.monospace || {};
    var classes = 'info-table';
    if (mono.key) classes += ' info-table-mono-key';
    if (mono.value) classes += ' info-table-mono-value';
    return classes;
}

function infoTableHtml(section, query, scope, searchContext, sectionIndex) {
    var rows = section && section.rows || [];
    var html = '<div class="info-table-wrap"><table class="' +
        infoTableClassName(section) + '">';
    for (var r = 0; r < rows.length; r++) {
        html += '<tr><td>' +
                infoHighlight(rows[r][0], query, scope,
                    infoRecordSearchContext(searchContext, sectionIndex, 'key', { row: r })) +
                '</td><td>' +
                infoHighlight(rows[r][1], query, scope,
                    infoRecordSearchContext(searchContext, sectionIndex, 'value', { row: r })) +
                '</td></tr>';
    }
    html += '</table></div>';
    return html;
}

function infoSectionBodyHtml(section, query, scope, si, gi, ri, sectionIndex, options) {
    section = section || {};
    options = options || {};
    var html = '';

    var showTitle = section.title && section.type !== 'switcher' &&
        (!options.embedded || options.showTitle === true);
    if (showTitle) {
        html += '<div class="info-section-title">' +
                infoHighlight(section.title, query, scope,
                    infoRecordSearchContext(options.infoSearchContext, sectionIndex, 'title')) + '</div>';
    }

    if (section.type === 'fieldDetailsHeader') {
        html += infoFieldDetailsHeaderHtml(section);
    } else if (section.type === 'fieldDetailsTable') {
        html += infoFieldDetailsTableHtml(section, query, scope, si, gi, ri, sectionIndex, options);
    } else if (section.type === 'fieldDetailsGroup') {
        html += infoFieldDetailsGroupHtml(section, query, scope, si, gi, ri, sectionIndex, options);
    } else if (section.type === 'table' && section.rows) {
        html += infoTableHtml(section, query, scope, options.infoSearchContext, sectionIndex);
    } else if (section.type === 'list') {
        html += infoListHtml(section, query, scope, options.infoSearchContext, sectionIndex);
    } else if (section.type === 'graphModel') {
        html += infoGraphModelHtml(section, query, scope, si, gi, ri, sectionIndex, options);
    } else if (section.type === 'text') {
        html += infoTextContentHtml(section, query, scope, options.infoSearchContext, sectionIndex);
    } else if (section.type === 'warning') {
        html += infoWarningHtml(section, query, scope, si, gi, ri, sectionIndex, options.infoSearchContext, options);
    } else if (section.type === 'switcher') {
        html += infoSwitcherHtml(section, query, scope, si, gi, ri, sectionIndex, options);
    }

    return html;
}

function infoSectionIdPrefix(si, gi, ri, options) {
    var scopePrefix = options && options.idScope ? options.idScope + '-' : '';
    return 'info-section-' + scopePrefix + si + '-' + gi + '-' + ri + '-';
}

function infoSectionsHtml(sections, query, scope, si, gi, ri, options) {
    if (!sections || !sections.length) {
        return '';
    }

    query = query || '';
    scope = scope || '';
    options = options || {};
    var baseSearchContext = infoSearchContextWith(options.infoSearchContext || null, {
        type: 'rule',
        si: si,
        gi: gi,
        ri: ri
    });
    // Section ids carry the top-level section index.
    // Sections nested in a real tab share the tab's index for switcher keys but
    // get no ids of their own: the tab panel wrapper owns the tab's id.
    var hasSectionIds = si !== undefined &&
        !options.disableSectionIds &&
        options.sharedSectionIndex === undefined;
    var idPrefix = infoSectionIdPrefix(si, gi, ri, options);

    var html = '';
    for (var i = 0; i < sections.length; i++) {
        var section = sections[i];
        var sectionIndex = options.sectionIndices
            ? options.sectionIndices[i]
            : (options.sharedSectionIndex !== undefined ? options.sharedSectionIndex : i);
        var sectionPath = infoSwitcherSectionPath(
            options.switcherPath || '',
            sectionIndex,
            i,
            options.sharedSectionIndex !== undefined
        );
        var idAttr = hasSectionIds && Number.isInteger(sectionIndex)
            ? ' id="' + idPrefix + sectionIndex + '"'
            : '';
        html += '<div class="info-section"' + idAttr + '>';
        html += infoSectionBodyHtml(section, query, scope, si, gi, ri, sectionIndex, {
            idScope: options.idScope,
            domIndex: i,
            embedded: options.embedded,
            showTitle: options.showTitle,
            switcherPath: sectionPath,
            switcherDepth: options.switcherDepth || 0,
            suppressSwitcherHeads: options.suppressSwitcherHeads === true,
            infoSearchContext: baseSearchContext
        });
        html += '</div>';
    }
    return html;
}

function activeInfoTabFromModel(model, activeId) {
    if (!model || !model.tabs) return null;
    for (var i = 0; i < model.tabs.length; i++) {
        if (model.tabs[i].id === activeId) {
            return model.tabs[i];
        }
    }
    return null;
}

function infoTabSectionsOptions(activeTab, options) {
    options = options || {};
    var sectionsOptions = {
        idScope: options.idScope,
        infoSearchContext: activeTab
            ? infoSearchContextWith(options.infoSearchContext, { tabId: activeTab.id })
            : (options.infoSearchContext || null)
    };
    if (activeTab && (activeTab.temporary || activeTab.fieldDetails)) {
        sectionsOptions.disableSectionIds = true;
        return sectionsOptions;
    }
    if (activeTab && Array.isArray(activeTab.sectionIndices)) {
        sectionsOptions.sectionIndices = activeTab.sectionIndices;
    } else if (activeTab && Number.isInteger(activeTab.topIndex)) {
        sectionsOptions.sharedSectionIndex = activeTab.topIndex;
    }
    return sectionsOptions;
}

function infoTabPanelId(activeTab, si, gi, ri, options) {
    options = options || {};
    if (!activeTab || !Number.isInteger(activeTab.topIndex) || si === undefined) return '';
    return infoSectionIdPrefix(si, gi, ri, options) + activeTab.topIndex;
}

function infoTabPanelHtml(activeTab, query, scope, si, gi, ri, options) {
    options = options || {};
    var panelId = infoTabPanelId(activeTab, si, gi, ri, options);
    var panelIdAttr = panelId ? ' id="' + htmlEscape(panelId) + '"' : '';
    return '<div class="info-tab-panel" role="tabpanel"' + panelIdAttr + '>' +
        infoSectionsHtml(
            activeTab ? activeTab.sections : [],
            query,
            scope,
            si,
            gi,
            ri,
            infoTabSectionsOptions(activeTab, options)
        ) +
        '</div>';
}

function applyInfoTabPanelAttrs(panel, activeTab, si, gi, ri, options) {
    options = options || {};
    if (!panel) return false;
    var panelId = infoTabPanelId(activeTab, si, gi, ri, options);
    if (panelId) {
        panel.id = panelId;
    } else {
        panel.removeAttribute('id');
    }
    panel.setAttribute('role', 'tabpanel');
    return true;
}

function infoPanelHtml(sections, query, scope, si, gi, ri, options) {
    options = options || {};
    var model = infoTabsModel(sections, si, gi, ri);
    if (!model) {
        if (!TraceSchema.infoSectionsHaveContent(sections)) return '';
        return infoSectionsHtml(sections, query, scope, si, gi, ri, {
            idScope: options.idScope
        });
    }

    var activeId = activeInfoTabId(sections, si, gi, ri);
    var activeTab = activeInfoTabFromModel(model, activeId);

    return infoTabBarHtml(model, activeId, query, scope, si, gi, ri, options) +
        infoTabPanelHtml(activeTab, query, scope, si, gi, ri, options);
}

function infoPanelDefaultMaxHeightPx() {
    if (!hasDOM()) return 520;
    var viewportHeight = (typeof window !== 'undefined' && window.innerHeight) ||
        (document.documentElement && document.documentElement.clientHeight) ||
        0;
    if (!(viewportHeight > 0)) return 520;
    return Math.min(viewportHeight * 0.4, 520);
}

function setInfoPanelAutoHeight(panel, height, si, gi, ri, options) {
    if (!panel || !(height > 0)) return false;

    var previousHeight = panel.getBoundingClientRect ?
        panel.getBoundingClientRect().height :
        0;
    var previousAuto = Number(panel.getAttribute('data-info-auto-height') || 0);
    var nextHeight = Math.max(60, Math.ceil(height));

    panel.style.height = nextHeight + 'px';
    panel.style.maxHeight = 'none';
    panel.setAttribute('data-info-auto-height', String(nextHeight));

    if (Math.abs(previousHeight - nextHeight) <= 0.5 ||
        Math.abs(previousAuto - nextHeight) <= 0.5) {
        return false;
    }

    if (options && options.idScope) {
        // Fullscreen overlay panels live outside the trace layout.
        return true;
    }

    markTraceLayoutDirtyRule(si, gi, ri, 'info-panel-auto-height');
    scheduleVirtualRowsRefresh();
    scheduleTraceAnchorLineUpdate();
    scheduleDiffMoveArrows();
    return true;
}

function infoTabPanelHtmlForMeasure(model, activeId, tab, query, scope, si, gi, ri, options) {
    options = options || {};
    return infoTabBarHtml(model, activeId, query, scope, si, gi, ri, options) +
        '<div class="info-tab-panel" role="tabpanel">' +
        infoSectionsHtml(tab.sections || [], query, scope, si, gi, ri, {
            idScope: options.idScope,
            disableSectionIds: true,
            sharedSectionIndex: Number.isInteger(tab.topIndex) ? tab.topIndex : undefined,
            sectionIndices: Array.isArray(tab.sectionIndices) ? tab.sectionIndices : undefined,
            infoSearchContext: infoSearchContextWith(null, { tabId: tab.id })
        }) +
        '</div>';
}

function applyTabbedInfoPanelAutoHeight(container, sections, query, scope, si, gi, ri, options) {
    if (!hasDOM() || !container || !container.closest || !document.createElement) return false;

    var model = infoTabsModel(sections, si, gi, ri);
    if (!model || !model.tabs || model.tabs.length < 2) return false;

    var idScope = options && options.idScope ? options.idScope : '';
    var resizeKey = (idScope ? idScope + '-' : '') + si + '-' + gi + '-' + ri;
    if (resizeRuntime().infoPanelHeights[resizeKey]) return false;

    var panel = container.closest('.rule-info-panel');
    if (!panel || !panel.classList || !panel.classList.contains('visible')) return false;
    if (!panel.getBoundingClientRect) return false;

    var width = container.getBoundingClientRect ? container.getBoundingClientRect().width : 0;
    if (!(width > 0)) return false;

    var activeId = activeInfoTabId(sections, si, gi, ri);
    var measure = document.createElement('div');
    measure.className = 'info-panel-tab-measure';
    measure.style.position = 'absolute';
    measure.style.left = '0';
    measure.style.top = '0';
    measure.style.width = Math.ceil(width) + 'px';
    measure.style.height = 'auto';
    measure.style.maxHeight = 'none';
    measure.style.overflow = 'visible';
    measure.style.visibility = 'hidden';
    measure.style.pointerEvents = 'none';
    measure.style.zIndex = '-1';
    measure.style.contain = 'layout style';

    var maxHeight = 0;
    panel.appendChild(measure);
    try {
        for (var i = 0; i < model.tabs.length; i++) {
            measure.innerHTML = infoTabPanelHtmlForMeasure(
                model,
                model.tabs[i].id,
                model.tabs[i],
                query,
                scope,
                si,
                gi,
                ri,
                options
            );
            var rect = measure.getBoundingClientRect ? measure.getBoundingClientRect() : null;
            maxHeight = Math.max(maxHeight, measure.scrollHeight || 0, rect ? rect.height : 0);
        }
    } finally {
        if (measure.parentNode) measure.parentNode.removeChild(measure);
    }

    if (!(maxHeight > 0)) return false;
    return setInfoPanelAutoHeight(
        panel,
        Math.min(maxHeight, infoPanelDefaultMaxHeightPx()),
        si,
        gi,
        ri,
        options
    );
}

function syncInfoOptionButtons(root, attrName, activeId, activeClass) {
    if (!root || !root.querySelectorAll) return false;
    var selector = attrName === 'data-tab-id'
        ? '.info-tab[' + attrName + ']'
        : 'button[' + attrName + ']';
    var buttons = root.querySelectorAll(selector);
    if (!buttons.length) return false;
    activeId = String(activeId || '');
    activeClass = activeClass || 'active';
    for (var i = 0; i < buttons.length; i++) {
        var button = buttons[i];
        var active = (button.getAttribute(attrName) || '') === activeId;
        if (button.classList) {
            if (active) button.classList.add(activeClass);
            else button.classList.remove(activeClass);
        }
        button.setAttribute('aria-selected', active ? 'true' : 'false');
    }
    return true;
}

function firstElementFromHtml(html) {
    if (!hasDOM() || !document.createElement) return null;
    var holder = document.createElement('div');
    holder.innerHTML = html || '';
    return holder.firstElementChild;
}

function currentInfoQueryScope() {
    var query = hasDOM() ? currentSearchQuery() : '';
    return {
        query: query,
        scope: query ? currentSearchScope() : ''
    };
}

function markRuleInfoRendered(si, gi, ri, query, scope) {
    var rule = ruleState(si, gi, ri);
    if (!rule) return false;
    rule.infoRendered = {
        query: query || '',
        scope: query ? (scope || currentSearchScope()) : ''
    };
    return true;
}

function finishInfoIslandRender(container, snapshot, si, gi, ri) {
    restoreInfoScrollSnapshot(container, snapshot);
    if (typeof captureMountedRulePaneScroll === 'function') {
        captureMountedRulePaneScroll(si, gi, ri);
    }
    reactivateActiveSearchMatchForRule(si, gi, ri);
}

function renderInfoSwitcherIslandInto(container, section, sectionIndex, query, scope, si, gi, ri, options) {
    if (!container || !section) return false;
    var switcherKey = infoSwitcherKey(section, sectionIndex, options && options.switcherPath);
    var switcherEl = findInfoSwitcherElement(container, switcherKey);
    if (!switcherEl) return false;

    var nextSwitcher = firstElementFromHtml(
        infoSwitcherHtml(section, query, scope, si, gi, ri, sectionIndex, options)
    );
    if (!nextSwitcher) return false;
    if (switcherEl.parentNode) switcherEl.parentNode.replaceChild(nextSwitcher, switcherEl);
    else return false;
    return true;
}

function renderRuleInfoSwitcherIsland(visualCtx, si, gi, ri, switcherKey, query, scope, options) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        options = scope;
        scope = query;
        query = switcherKey;
        switcherKey = ri;
        ri = gi;
        gi = si;
        si = visualCtx;
        return runVisualSurfaceCommitNow(
            'rule-info-switcher-island',
            ['rule-info', 'search-marks'],
            ['trace', 'render'],
            function(ctx) {
                return renderRuleInfoSwitcherIsland(ctx, si, gi, ri, switcherKey, query, scope, options);
            }
        );
    }

    options = options || {};
    var container = infoContentElement(si, gi, ri, options);
    if (!container) return false;

    var sections = traceRuleInfo(si, rawRuleIndex(si, gi, ri));
    var found = findInfoSwitcherSection(sections, switcherKey);
    if (!found || !found.section) return false;

    var rendered = false;
    var snapshot = captureInfoScrollSnapshot(container);
    var commitOptions = {
        label: 'rule-info-switcher-island',
        ref: ruleRef(si, gi, ri),
        target: container,
        detachedReason: 'detached_container'
    };
    ruleInfoSurface.commit(visualCtx, commitOptions, function(target) {
        rendered = renderInfoSwitcherIslandInto(
            target,
            found.section,
            found.index,
            query || '',
            query ? (scope || currentSearchScope()) : '',
            si,
            gi,
            ri,
            Object.assign({}, options, { switcherPath: found.path, switcherDepth: found.depth })
        );
        if (!rendered) return;
        markRuleInfoRendered(si, gi, ri, query, scope);
        finishInfoIslandRender(target, snapshot, si, gi, ri);
    });
    return rendered;
}

function renderInfoTabIslandInto(container, sections, activeId, query, scope, si, gi, ri, options) {
    if (!container || !container.querySelector) return false;
    var model = infoTabsModel(sections, si, gi, ri);
    if (!model) return false;
    var activeTab = activeInfoTabFromModel(model, activeId);
    if (!activeTab) return false;

    var tabs = container.querySelector('.info-tabs');
    var panel = container.querySelector('.info-tab-panel');
    if (!tabs || !panel) return false;
    if (!syncInfoOptionButtons(tabs, 'data-tab-id', activeId, 'active')) return false;

    applyInfoTabPanelAttrs(panel, activeTab, si, gi, ri, options);
    panel.innerHTML = infoSectionsHtml(
        activeTab.sections || [],
        query,
        scope,
        si,
        gi,
        ri,
        infoTabSectionsOptions(activeTab, options)
    );
    return true;
}

function renderRuleInfoTabIsland(visualCtx, si, gi, ri, tabId, query, scope, options) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        options = scope;
        scope = query;
        query = tabId;
        tabId = ri;
        ri = gi;
        gi = si;
        si = visualCtx;
        return runVisualSurfaceCommitNow(
            'rule-info-tab-island',
            ['rule-info', 'search-marks'],
            ['trace', 'render'],
            function(ctx) {
                return renderRuleInfoTabIsland(ctx, si, gi, ri, tabId, query, scope, options);
            }
        );
    }

    options = options || {};
    var container = infoContentElement(si, gi, ri, options);
    if (!container) return false;
    var sections = traceRuleInfo(si, rawRuleIndex(si, gi, ri));
    var model = infoTabsModel(sections, si, gi, ri);
    if (!model) return false;

    var rendered = false;
    var snapshot = captureInfoScrollSnapshot(container);
    var commitOptions = {
        label: 'rule-info-tab-island',
        ref: ruleRef(si, gi, ri),
        target: container,
        detachedReason: 'detached_container'
    };
    ruleInfoSurface.commit(visualCtx, commitOptions, function(target) {
        rendered = renderInfoTabIslandInto(
            target,
            sections,
            activeInfoTabId(sections, si, gi, ri),
            query || '',
            query ? (scope || currentSearchScope()) : '',
            si,
            gi,
            ri,
            options
        );
        if (!rendered) return;
        markRuleInfoRendered(si, gi, ri, query, scope);
        applyTabbedInfoPanelAutoHeight(target, sections, query, scope, si, gi, ri, options);
        if (typeof InfoTargetController !== 'undefined' &&
            InfoTargetController &&
            InfoTargetController.scrollActiveTabIntoView) {
            InfoTargetController.scrollActiveTabIntoView(si, gi, ri, activeInfoTabId(sections, si, gi, ri), {
                container: target,
                idScope: options.idScope
            });
        }
        finishInfoIslandRender(target, snapshot, si, gi, ri);
    });
    return rendered;
}

function renderInfoPanel(container, sections, query, scope, si, gi, ri, options) {
    var model = infoTabsModel(sections, si, gi, ri);
    var panel = container && container.closest ? container.closest('.rule-info-panel') : null;
    if (panel && panel.classList) panel.classList.toggle('info-panel-tabbed', !!model);
    var snapshot = captureInfoScrollSnapshot(container);
    container.innerHTML = infoPanelHtml(sections, query, scope, si, gi, ri, options);
    applyTabbedInfoPanelAutoHeight(container, sections, query, scope, si, gi, ri, options);
    if (typeof InfoTargetController !== 'undefined' &&
        InfoTargetController &&
        InfoTargetController.scrollActiveTabIntoView) {
        InfoTargetController.scrollActiveTabIntoView(si, gi, ri, activeInfoTabId(sections, si, gi, ri), {
            container: container
        });
    }
    restoreInfoScrollSnapshot(container, snapshot);
}

function scrollRuleInfoFieldDetailIntoView(si, gi, ri, metaIndex, options) {
    if (!hasDOM() || !document.getElementById) return false;
    options = options || {};
    metaIndex = Math.max(0, Math.floor(Number(metaIndex)));
    if (!Number.isFinite(metaIndex)) metaIndex = 0;
    var container = infoContentElement(si, gi, ri, options);
    var target = document.getElementById(infoFieldDetailsAnchorId(si, gi, ri, metaIndex, options));
    if (!container || !target) return false;

    var pane = infoScrollPaneForContent(container);
    if (!pane || !pane.getBoundingClientRect || !target.getBoundingClientRect) return false;
    var paneRect = pane.getBoundingClientRect();
    var targetRect = target.getBoundingClientRect();
    var topDelta = targetRect.top - paneRect.top - 8;
    if (Math.abs(topDelta) > 1) {
        pane.scrollTop = clampInfoScroll((pane.scrollTop || 0) + topDelta, infoScrollMax(pane, 'top'));
    }
    if (target.classList) {
        target.classList.remove('info-metadata-detail-blink');
        if (target.offsetWidth !== undefined) target.offsetWidth;
        target.classList.add('info-metadata-detail-blink');
    }
    return true;
}

function infoLoadingHtml() {
    return '<div class="info-loading">Rendering info</div>';
}

function renderRuleInfoLoading(visualCtx, container, ref) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        ref = container;
        container = visualCtx;
        return runVisualSurfaceCommitNow(
            'rule-info-loading',
            ['rule-info'],
            ['trace', 'render'],
            function(ctx) {
                return renderRuleInfoLoading(ctx, container, ref);
            }
        );
    }

    if (!container || infoPanelContentHasContent(container)) return false;
    var rendered = false;
    var commitOptions = {
        label: 'rule-info-loading',
        ref: ref,
        detachedReason: 'detached_container'
    };
    if (!hasDOM() || !document.getElementById) commitOptions.target = container;
    ruleInfoSurface.commit(visualCtx, commitOptions, function(target) {
        target.innerHTML = infoLoadingHtml();
        rendered = true;
    });
    return rendered;
}

function infoPanelNeedsRender(si, gi, ri, query, scope) {
    var rule = ruleState(si, gi, ri);
    var prev = rule.infoRendered;
    query = query || '';
    scope = query ? (scope || currentSearchScope()) : '';
    if (!prev || prev.query !== query || prev.scope !== scope) return true;

    if (hasDOM()) {
        var content = document.getElementById('info-content-' + ruleKey(si, gi, ri));
        if (content && !infoPanelContentHasContent(content)) return true;
    }

    return false;
}

function requestRuleInfoRenderIfVisible(visualCtx, si, gi, ri) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        ri = gi;
        gi = si;
        si = visualCtx;
        visualCtx = null;
    }
    var rule = ruleState(si, gi, ri);
    if (!effectiveRuleFeature(si, gi, ri, 'info') || !ruleHasInfo(ruleRef(si, gi, ri))) return;
    if (!isRuleExpandedInLayout(si, gi, ri)) return;

    var panel = document.getElementById('info-' + si + '-' + gi + '-' + ri);
    var content = document.getElementById('info-content-' + si + '-' + gi + '-' + ri);
    if (!panel || !content || !panel.classList.contains('visible')) return;

    if (!elementIntersectsLazyViewport(document.getElementById('rule-' + si + '-' + gi + '-' + ri))) {
        if (!rule.infoRendered && !infoPanelContentHasContent(content)) {
            if (visualCtx) renderRuleInfoLoading(visualCtx, content, ruleRef(si, gi, ri));
            else renderRuleInfoLoading(content, ruleRef(si, gi, ri));
        }
        return;
    }

    if (infoPanelNeedsRender(si, gi, ri, currentSearchQuery(), currentSearchScope())) {
        if (!rule.infoRendered && !infoPanelContentHasContent(content)) {
            if (visualCtx) renderRuleInfoLoading(visualCtx, content, ruleRef(si, gi, ri));
            else renderRuleInfoLoading(content, ruleRef(si, gi, ri));
        }
        if (visualCtx) {
            renderRuleInfoPanel(visualCtx, si, gi, ri, currentSearchQuery(), currentSearchScope());
        } else {
            renderRuleInfoPanel(si, gi, ri, currentSearchQuery(), currentSearchScope());
        }
    }
}
