/* ══════════════════════════════════════════════════════════════
   Display updates (expand/collapse for stage, group, rule)
   ══════════════════════════════════════════════════════════════ */

function updateStageDisplay(si) {
    markCollapsedSearchIndicatorsDirty();
    markTraceLayoutDirtyStage(si, 'stage-display');
    var colEl = document.getElementById('stage-col-' + si);
    var expEl = document.getElementById('stage-exp-' + si);
    if (!colEl || !expEl) return;
    colEl.style.display = effectiveStageOpen(si) ? 'none' : 'flex';
    expEl.style.display = effectiveStageOpen(si) ? 'flex' : 'none';
    scheduleVirtualRowsRefresh();
    scheduleDiffMoveArrows();
}

function updateGroupDisplay(si, gi) {
    markCollapsedSearchIndicatorsDirty();
    markTraceLayoutDirtyGroup(si, gi, 'group-display');
    if (groupRuleCount(si, gi) <= 1) return;
    var el = document.getElementById('group-' + si + '-' + gi);
    if (el) {
        el.className = 'group-cell ' + (effectiveGroupOpen(si, gi) ? 'g-expanded' : 'g-collapsed') +
            collapsedSearchIndicatorClass('groups', groupSearchIndicatorKey(si, gi));
    }
    scheduleVirtualRowsRefresh();
    scheduleDiffMoveArrows();
}

function updateRuleDisplay(si, gi, ri) {
    markCollapsedSearchIndicatorsDirty();
    markTraceLayoutDirtyRule(si, gi, ri, 'rule-display');
    var el = document.getElementById('rule-' + si + '-' + gi + '-' + ri);
    if (el) {
        el.className = 'rule-cell ' +
            (effectiveRuleOpen(si, gi, ri) ? 'expanded' : 'collapsed') +
            collapsedSearchIndicatorClass('rules', ruleKey(si, gi, ri)) +
            (ruleIsTextTile(ruleRef(si, gi, ri)) ? ' text-rule' : '') +
            (diffSideForRule(si, gi, ri) ? ' diff-rule' : '');
    }
    if (effectiveRuleOpen(si, gi, ri)) {
        scheduleRuleVisibilityCheck(si, gi, ri);
    }
    scheduleVirtualRowsRefresh();
    scheduleDiffMoveArrows();
}

function useVirtualizedDisplayRefresh() {
    return typeof shouldVirtualizeStageShellsForCurrentTrace === 'function' &&
           shouldVirtualizeStageShellsForCurrentTrace();
}

function updateAllDisplays(options) {
    if (useVirtualizedDisplayRefresh()) {
        markCollapsedSearchIndicatorsDirty();
        invalidateTraceMeasuredWidthCache();
        if (!options || !options.deferVirtualRefresh) {
            refreshVirtualRowsNow();
            scheduleVisibleRuleRenderScan();
        }
        scheduleTraceAnchorLineUpdate();
        updateCycleButtonLabel();
        scheduleDiffMoveArrows();
        syncFullscreenOverlay();
        return;
    }

    updateAllLayoutWidths();
    for (var si = 0; si < currentStageCount(); si++) {
        updateStageDisplay(si);
    }
    forEachGroup(function(si, gi) {
        updateGroupDisplay(si, gi);
    });
    forEachRule(function(si, gi, ri) {
        updateRuleDisplay(si, gi, ri);
    });
    if (!options || !options.deferVirtualRefresh) {
        refreshVirtualRowsNow();
        scheduleVisibleRuleRenderScan();
    }
    scheduleTraceAnchorLineUpdate();
    updateCycleButtonLabel();
    scheduleDiffMoveArrows();
    syncFullscreenOverlay();
}

function refreshLayoutTransition(change, options) {
    options = options || {};
    if (!change || !change.changed) return false;
    markTraceLayoutDirtyStagesFromTransition(change, 'layout-transition-stage');
    markTraceLayoutDirtyGroupsFromTransition(change, 'layout-transition-group');
    markTraceLayoutDirtyRulesFromTransition(change, 'layout-transition-rule');

    if (useVirtualizedDisplayRefresh()) {
        markCollapsedSearchIndicatorsDirty();
        invalidateTraceMeasuredWidthCache();
        scheduleVirtualRowsRefresh();
        if (!options.deferExpensiveVisuals) scheduleDiffMoveArrows();
        return true;
    }

    for (var i = 0; i < change.groupWidths.length; i++) {
        updateGroupLayoutWidth(change.groupWidths[i].si, change.groupWidths[i].gi);
    }
    for (var i = 0; i < change.stageWidths.length; i++) {
        updateStageLayoutWidth(change.stageWidths[i].si);
    }
    for (var i = 0; i < change.stages.length; i++) {
        updateStageDisplay(change.stages[i].si);
    }
    for (var i = 0; i < change.groups.length; i++) {
        updateGroupDisplay(change.groups[i].si, change.groups[i].gi);
    }
    var ruleDisplays = change.ruleDisplays || change.rules;
    for (var i = 0; i < ruleDisplays.length; i++) {
        updateRuleDisplay(ruleDisplays[i].si, ruleDisplays[i].gi, ruleDisplays[i].ri);
    }
    for (var i = 0; i < change.openedStages.length; i++) {
        scheduleStageRuleVisibilityChecks(change.openedStages[i].si);
    }
    return true;
}

function refreshLayoutTransitionDisplays(change, options) {
    options = options || {};
    refreshLayoutTransition(change, options);
    if (!options || !options.deferVirtualRefresh) {
        refreshVirtualRowsNow({
            skipSearchStateRefresh: !!options.skipSearchStateRefresh
        });
        scheduleVisibleRuleRenderScan();
    }
    if (options.deferExpensiveVisuals) {
        updateCycleButtonLabel();
        return;
    }
    scheduleTraceAnchorLineUpdate();
    updateCycleButtonLabel();
    scheduleDiffMoveArrows();
}

/* ══════════════════════════════════════════════════════════════
   Flash animation
   ══════════════════════════════════════════════════════════════ */

var RULE_TITLE_FLASH_CLASS = 'flash-cell';
var RULE_TITLE_FLASH_ANIMATION = 'traceNavFlash';

function ruleTitleBarElement(si, gi, ri) {
    var id = 'rule-' + si + '-' + gi + '-' + ri;
    var el = document.getElementById(id);
    if (!el || !el.querySelector) return null;
    return el.querySelector('.rule-title-bar');
}

function clearRuleTitleFlashElement(el) {
    if (!el || !el.classList) return false;
    el.classList.remove(RULE_TITLE_FLASH_CLASS);
    if (el.__traceRuleTitleFlashCleanup && el.removeEventListener) {
        el.removeEventListener('animationend', el.__traceRuleTitleFlashCleanup);
    }
    el.__traceRuleTitleFlashCleanup = null;
    return true;
}

function scheduleRuleTitleFlashCleanup(el, token) {
    function cleanup(ev) {
        if (ev && ev.animationName && ev.animationName !== RULE_TITLE_FLASH_ANIMATION) return;
        if (!el || el.__traceRuleTitleFlashToken !== token) return;
        clearRuleTitleFlashElement(el);
    }

    el.__traceRuleTitleFlashCleanup = cleanup;
    if (el.addEventListener) {
        el.addEventListener('animationend', cleanup);
    }
    scheduleRuntimeTimeout(cleanup, TRACE_NAV_BLINK_MS + 80, {
        epochScopes: ['trace', 'render', 'virtual'],
        label: 'navigation-title-flash-clear'
    });
}

function flashRuleTitle(si, gi, ri) {
    var el = ruleTitleBarElement(si, gi, ri);
    if (!el) return;
    clearRuleTitleFlashElement(el);
    el.__traceRuleTitleFlashToken = (el.__traceRuleTitleFlashToken || 0) + 1;
    void el.offsetWidth;
    el.classList.add(RULE_TITLE_FLASH_CLASS);
    scheduleRuleTitleFlashCleanup(el, el.__traceRuleTitleFlashToken);
}


/* ══════════════════════════════════════════════════════════════
   Stage / Group / Rule interactions
   ══════════════════════════════════════════════════════════════ */

function toggleStage(si) {
    TraceActions.layout.toggleStage(si);
}

function toggleStageRules(si, ev) {
    TraceActions.layout.toggleStageRules(si, ev);
}

function toggleGroup(si, gi, ev) {
    TraceActions.layout.toggleGroup(si, gi, ev);
}

function toggleGroupRules(si, gi, ev) {
    TraceActions.layout.toggleGroupRules(si, gi, ev);
}

function onCollapsedGroupClick(si, gi, ev) {
    TraceActions.layout.onCollapsedGroupClick(si, gi, ev);
}

function toggleRule(si, gi, ri, ev) {
    TraceActions.layout.toggleRule(si, gi, ri, ev);
}

function onCollapsedRuleClick(si, gi, ri, ev) {
    TraceActions.layout.onCollapsedRuleClick(si, gi, ri, ev);
}


/* ══════════════════════════════════════════════════════════════
   Navigation
   ══════════════════════════════════════════════════════════════ */

function ensureRuleVisible(si, gi, ri) {
    var change = TraceState.ensureRuleVisible(currentUiState(), currentTraceGroups(), si, gi, ri);
    if (refreshLayoutTransition(change)) {
        refreshVirtualRowsNow();
        updateCycleButtonLabel();
        scheduleDiffMoveArrows();
    }
}

function settleNavigatedRule(si, gi, ri) {
    if (!ruleVisibleEnoughForNavigation(ruleRef(si, gi, ri))) {
        scrollRuleIntoView(si, gi, ri);
    }
    requestRuleTreeRenderIfVisible(si, gi, ri, currentSearchQuery(), false, currentSearchScope());

    scheduleRenderDeferredWork('navigation-title-flash-' + ruleKey(si, gi, ri), function(visualCtx) {
        flashRuleTitle(si, gi, ri);
    }, {
        epochScopes: ['trace', 'render', 'virtual'],
        label: 'navigation-title-flash',
        surfaces: ['rule-cell']
    });
}

var FULLSCREEN_NAV_REPEAT_DELAY_MS = 320;
var FULLSCREEN_NAV_REPEAT_INTERVAL_MS = 105;

function navTarget(direction, si, gi, ri) {
    var idx = findFlatIndex(si, gi, ri);
    var step = direction === 'prev' ? -1 : 1;
    var nextIdx = idx + step;
    var rules = currentAllRules();
    if (nextIdx < 0 || nextIdx >= rules.length) return null;
    return rules[nextIdx];
}

function isRuleNavBlocked(direction, si, gi, ri, blockOtherDiffRule) {
    var target = navTarget(direction, si, gi, ri);
    if (!target) return true;
    return !!blockOtherDiffRule &&
        isOtherActiveDiffRule(
            ruleRef(si, gi, ri),
            target.stageIdx,
            target.groupIdx,
            target.ruleIdx
        );
}

function navigateRuleDirection(direction, si, gi, ri, ev, blockOtherDiffRule) {
    return TraceActions.navigation.navigateRuleDirection(direction, si, gi, ri, ev, blockOtherDiffRule);
}

function navigatePrev(si, gi, ri, ev) {
    return TraceActions.navigation.navigatePrev(si, gi, ri, ev);
}

function navigateNext(si, gi, ri, ev) {
    return TraceActions.navigation.navigateNext(si, gi, ri, ev);
}

function clearFullscreenNavRepeatSuppression(state) {
    if (fullscreenRuntime().fullscreenNavRepeatSuppressClick !== state) return;
    fullscreenRuntime().fullscreenNavRepeatSuppressClick = null;
}

function shouldSuppressNavClick(direction) {
    var fullscreen = fullscreenRuntime();
    var state = fullscreen.fullscreenNavRepeatSuppressClick;
    if (!state) return false;
    if (state.direction !== direction) return false;
    if (Date.now() > state.until) {
        fullscreen.fullscreenNavRepeatSuppressClick = null;
        return false;
    }
    fullscreen.fullscreenNavRepeatSuppressClick = null;
    return true;
}

function suppressNextNavClick(direction, ms) {
    var state = {
        direction: direction,
        until: Date.now() + ms
    };
    var fullscreen = fullscreenRuntime();
    fullscreen.fullscreenNavRepeatSuppressClick = state;
    fullscreen.fullscreenNavRepeatSuppressDocumentClickUntil = state.until;
    scheduleRuntimeTimeout(function() {
        clearFullscreenNavRepeatSuppression(state);
    }, ms, {
        epochScopes: ['trace'],
        label: 'fullscreen-nav-repeat-suppression'
    });
}

function shouldSuppressFullscreenNavDocumentClick(ev) {
    var fullscreen = fullscreenRuntime();
    if (!fullscreen.fullscreenNavRepeatSuppressDocumentClickUntil) return false;
    if (Date.now() > fullscreen.fullscreenNavRepeatSuppressDocumentClickUntil) {
        fullscreen.fullscreenNavRepeatSuppressDocumentClickUntil = 0;
        return false;
    }

    fullscreen.fullscreenNavRepeatSuppressDocumentClickUntil = 0;
    if (ev) {
        if (ev.stopPropagation) ev.stopPropagation();
        if (ev.preventDefault) ev.preventDefault();
    }
    return true;
}

function stepFullscreenNavRepeat(direction) {
    var ref = fullscreenCurrentRule();
    if (!ref) return false;
    return navigateRuleDirection(
        direction,
        ref.si,
        ref.gi,
        ref.ri,
        null,
        false
    );
}

function removeFullscreenNavRepeatListeners() {
    if (typeof window === 'undefined') return;
    if (!window.removeEventListener) return;
    window.removeEventListener('pointerup', stopFullscreenNavRepeat);
    window.removeEventListener('pointercancel', stopFullscreenNavRepeat);
    window.removeEventListener('blur', stopFullscreenNavRepeat);
}

function stopFullscreenNavRepeat(ev) {
    var fullscreen = fullscreenRuntime();
    var state = fullscreen.fullscreenNavRepeatState;
    if (!state) return;
    if (ev && ev.pointerId !== undefined &&
        state.pointerId !== undefined &&
        ev.pointerId !== state.pointerId) {
        return;
    }

    clearTimeout(state.delayTimer);
    clearInterval(state.intervalTimer);
    removeFullscreenNavRepeatListeners();
    suppressNextNavClick(state.direction, 700);
    fullscreen.fullscreenNavRepeatState = null;
}

function startFullscreenNavRepeat(direction, si, gi, ri, ev, blockOtherDiffRule) {
    TraceActions.navigation.startFullscreenNavRepeat(direction, si, gi, ri, ev, blockOtherDiffRule);
}

function handleNavButtonClick(direction, si, gi, ri, ev, blockOtherDiffRule) {
    return TraceActions.navigation.handleNavButtonClick(direction, si, gi, ri, ev, blockOtherDiffRule);
}

function groupNavigatePrev(si, gi, ev) {
    TraceActions.navigation.groupNavigatePrev(si, gi, ev);
}

function groupNavigateNext(si, gi, ev) {
    TraceActions.navigation.groupNavigateNext(si, gi, ev);
}


/* ══════════════════════════════════════════════════════════════
   Global cycle (expand/collapse all)
   ══════════════════════════════════════════════════════════════ */

var cycleLabels = ['Collapse All', 'Expand Stages', 'Expand Groups', 'Expand Rules', 'Expand All'];
var globalLayoutPresets = [
    { stagesOpen: false, groupsOpen: false, rules: 'none' },
    { stagesOpen: true, groupsOpen: 'preserve', rules: 'preserve' },
    { stagesOpen: true, groupsOpen: true, rules: 'preserve' },
    { stagesOpen: true, groupsOpen: true, rules: 'last' },
    { stagesOpen: true, groupsOpen: true, rules: 'all' }
];

function allStagesOpen() {
    return TraceState.allStagesOpen(currentUiState(), currentTraceGroups());
}

function allGroupsOpen() {
    return TraceState.allGroupsOpen(currentUiState(), currentTraceGroups());
}

function anyExpandableRuleOpen() {
    return TraceState.anyExpandableRuleOpen(currentUiState(), currentTraceGroups());
}

function allRulesOpen() {
    return TraceState.allRulesOpen(currentUiState(), currentTraceGroups());
}

function nextGlobalCycleState() {
    return TraceState.nextGlobalCycleState(currentUiState(), currentTraceGroups());
}

function updateCycleButtonMeasure(measureEl) {
    if (!measureEl || !document.createElement) return;

    var labelsKey = cycleLabels.join('\n');
    if (measureEl.getAttribute('data-cycle-labels') === labelsKey) return;

    while (measureEl.firstChild) measureEl.removeChild(measureEl.firstChild);
    for (var i = 0; i < cycleLabels.length; i++) {
        var option = document.createElement('span');
        option.textContent = cycleLabels[i];
        measureEl.appendChild(option);
    }
    measureEl.setAttribute('data-cycle-labels', labelsKey);
}

function ensureCycleButtonParts(btn) {
    var labelEl = btn.querySelector ? btn.querySelector('.cycle-label') : null;
    var numEl = btn.querySelector ? btn.querySelector('.cycle-number') : null;
    var measureEl = btn.querySelector ? btn.querySelector('.cycle-measure') : null;

    if ((!labelEl || !numEl || !measureEl) && document.createElement) {
        while (btn.firstChild) btn.removeChild(btn.firstChild);

        measureEl = document.createElement('span');
        measureEl.className = 'cycle-measure';
        measureEl.setAttribute('aria-hidden', 'true');
        btn.appendChild(measureEl);

        labelEl = document.createElement('span');
        labelEl.className = 'cycle-label';
        btn.appendChild(labelEl);

        numEl = document.createElement('span');
        numEl.className = 'cycle-number';
        numEl.setAttribute('aria-hidden', 'true');
        btn.appendChild(numEl);
    }

    updateCycleButtonMeasure(measureEl);
    return {
        labelEl: labelEl,
        numEl: numEl
    };
}

function updateCycleButtonLabel() {
    if (!hasDOM()) return;
    var btn = document.getElementById('cycle-button');
    if (!btn) return;
    var nextState = nextGlobalCycleState();
    var label = cycleLabels[nextState];
    var parts = ensureCycleButtonParts(btn);
    var labelEl = parts.labelEl;
    if (labelEl) {
        labelEl.textContent = label;
    } else {
        btn.textContent = label;
    }
    var numEl = parts.numEl;
    if (numEl) numEl.textContent = String(nextState);
    btn.setAttribute('title', label);
    btn.setAttribute('aria-label', label);
}

function applyGlobalCycleState(state) {
    var change = TraceState.applyGlobalCycleState(currentUiState(), currentTraceGroups(), state, globalLayoutPresets);
    invalidateTraceMeasuredWidthCache();
    return change;
}

function cycleGlobalState() {
    TraceActions.layout.cycleGlobalState();
}

var defaultTheme = 'stone-dark';
var themeFamilies = [
    { id: 'stone', label: 'Stone', light: 'stone-light', dark: 'stone-dark' },
    { id: 'slate', label: 'Slate', light: 'slate-light', dark: 'slate-dark' },
    { id: 'monokai', label: 'Monokai', light: 'monokai-light', dark: 'monokai' },
    { id: 'modus', label: 'Modus', light: 'modus-operandi', dark: 'modus-vivendi' }
];

function themeFamilyForTheme(theme) {
    for (var i = 0; i < themeFamilies.length; i++) {
        var family = themeFamilies[i];
        if (theme === family.id || theme === family.light || theme === family.dark) {
            return family;
        }
    }
    return null;
}

function themeNames() {
    var names = [];
    for (var i = 0; i < themeFamilies.length; i++) {
        if (themeFamilies[i].light) names.push(themeFamilies[i].light);
        if (themeFamilies[i].dark) names.push(themeFamilies[i].dark);
    }
    return names.length ? names : [defaultTheme];
}

function currentAppliedTheme() {
    var names = themeNames();
    for (var i = 0; i < names.length; i++) {
        if (document.body.classList.contains('theme-' + names[i])) {
            return names[i];
        }
    }
    return defaultTheme;
}

function isThemeDark(theme) {
    var family = themeFamilyForTheme(theme);
    return !!(family && family.dark === theme);
}

function themeForFamily(family, darkPreferred) {
    if (!family) family = themeFamilyForTheme(defaultTheme);
    if (darkPreferred && family.dark) return family.dark;
    if (!darkPreferred && family.light) return family.light;
    return family.dark || family.light || defaultTheme;
}

function resolveTheme(theme, darkPreferred) {
    var family = themeFamilyForTheme(theme) || themeFamilyForTheme(defaultTheme);
    var modeRequested = typeof darkPreferred === 'boolean';
    var explicitVariant = (theme === family.light || theme === family.dark) &&
        !(modeRequested && theme === family.id);
    if (explicitVariant) {
        return { family: family, theme: theme };
    }

    var useDark = modeRequested
        ? darkPreferred
        : isThemeDark(currentAppliedTheme());
    return { family: family, theme: themeForFamily(family, useDark) };
}

function updateThemeControls(resolved) {
    if (typeof resolved === 'string') resolved = resolveTheme(resolved);

    var select = document.getElementById('theme-select');
    if (select) select.value = resolved.family.id;

    var lightBtn = document.getElementById('theme-light-button');
    var darkBtn = document.getElementById('theme-dark-button');
    var hasLight = !!resolved.family.light;
    var hasDark = !!resolved.family.dark;
    var isDark = !!(resolved.family.dark && resolved.theme === resolved.family.dark);
    if (lightBtn) {
        lightBtn.disabled = !hasLight;
        var lightActive = hasLight && !isDark;
        lightBtn.classList.toggle('active', lightActive);
        lightBtn.setAttribute('aria-pressed', lightActive ? 'true' : 'false');
        lightBtn.title = hasLight ? 'Use light mode' : 'This theme has no light mode';
    }
    if (darkBtn) {
        darkBtn.disabled = !hasDark;
        var darkActive = hasDark && isDark;
        darkBtn.classList.toggle('active', darkActive);
        darkBtn.setAttribute('aria-pressed', darkActive ? 'true' : 'false');
        darkBtn.title = hasDark ? 'Use dark mode' : 'This theme has no dark mode';
    }
}

function applyTheme(theme, persist, darkPreferred) {
    var resolved = resolveTheme(theme, darkPreferred);
    var names = themeNames();

    for (var i = 0; i < names.length; i++) {
        document.body.classList.remove('theme-' + names[i]);
    }
    document.body.classList.add('theme-' + resolved.theme);
    invalidateTraceMeasuredWidthCache();
    scheduleVirtualRowsRefresh();
    scheduleTraceAnchorLineUpdate();

    var select = document.getElementById('theme-select');
    if (select) select.value = resolved.family.id;
    updateThemeControls(resolved);

    if (persist !== false) {
        try { window.localStorage.setItem('optimizerTraceTheme', resolved.theme); } catch (e) {}
    }
}

function initTheme() {
    var savedTheme = null;
    try { savedTheme = window.localStorage.getItem('optimizerTraceTheme'); } catch (e) {}
    applyTheme(savedTheme || currentAppliedTheme(), false);
}

function setTheme(theme, ev) {
    TraceActions.theme.setTheme(theme, ev);
}

function setThemeDarkMode(dark, ev) {
    TraceActions.theme.setThemeDarkMode(dark, ev);
}

function closeSettingsPopover() {
    TraceActions.topBar.closeSettingsPopover();
}

function toggleSettingsPopover(ev) {
    TraceActions.topBar.toggleSettingsPopover(ev);
}

function closeTopBarPopovers(ev) {
    TraceActions.topBar.closeTopBarPopovers(ev);
}

function initTopBarControls() {
    updateCycleButtonLabel();
    initTraceAnchorPreviewControl();
}


/* ══════════════════════════════════════════════════════════════
   Layout state save/restore (for search and diff)
   ══════════════════════════════════════════════════════════════ */

function snapshotLayout(includeFeatures) {
    return TraceState.snapshotLayout(currentUiState(), includeFeatures);
}

function saveLayoutState() {
    return snapshotLayout(true);
}

function applyLayoutState(snapshot) {
    var change = TraceState.applyLayoutState(currentUiState(), currentTraceGroups(), snapshot);
    invalidateTraceMeasuredWidthCache();
    updateAllInfoDisplays();
    updateBulkDetailControls();
    return change;
}

function currentSearchQuery() {
    var box = document.getElementById('search-box');
    return (currentUiState().searchActive && box) ? box.value.trim() : '';
}
