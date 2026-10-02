var TraceState = (function() {
    function emptyUiState() {
        return {
            stages: [],
            fullscreen: {
                mode: null,
                currentRule: null
            },
            searchActive: false,
            searchToken: 0
        };
    }

    function ruleState() {
        return {
            open: false,
            fields: false,
            pinned: false,
            info: false,
            rendered: null,
            renderQueued: false,
            infoRendered: null
        };
    }

    function createInitialUiState(stageCount, groups) {
        var state = emptyUiState();

        for (var si = 0; si < stageCount; si++) {
            var stageGroups = groups[si] || [];
            var stage = { open: stageGroups.length > 0, groups: [] };

            for (var gi = 0; gi < stageGroups.length; gi++) {
                var count = stageGroups[gi].ri.length;
                var group = { open: count <= 1, rules: [] };

                for (var ri = 0; ri < count; ri++) {
                    group.rules.push(ruleState());
                }

                stage.groups.push(group);
            }

            state.stages.push(stage);
        }

        return state;
    }

    function buildAllRules(stageCount, groups) {
        var allRules = [];
        for (var si = 0; si < stageCount; si++) {
            for (var gi = 0; gi < groups[si].length; gi++) {
                var group = groups[si][gi];
                for (var ri = 0; ri < group.ri.length; ri++) {
                    allRules.push({
                        stageIdx: si,
                        groupIdx: gi,
                        ruleIdx: ri,
                        rawRuleIdx: group.ri[ri]
                    });
                }
            }
        }
        return allRules;
    }

    function snapshotLayout(uiState, includeFeatures) {
        return uiState.stages.map(function(stage) {
            return {
                open: stage.open,
                groups: stage.groups.map(function(group) {
                    return {
                        open: group.open,
                        rules: group.rules.map(function(rule) {
                            var copy = { open: rule.open };
                            if (includeFeatures) {
                                copy.fields = rule.fields;
                                copy.pinned = rule.pinned;
                                copy.info = rule.info;
                            }
                            return copy;
                        })
                    };
                })
            };
        });
    }

    function stageGroups(groups, si) {
        return groups[si] || [];
    }

    function hasStageRules(groups, si) {
        return stageGroups(groups, si).length > 0;
    }

    function groupRuleCount(groups, si, gi) {
        var group = stageGroups(groups, si)[gi];
        return group && group.ri ? group.ri.length : 0;
    }

    function transitionSummary() {
        return {
            changed: false,
            stages: [],
            groups: [],
            rules: [],
            ruleDisplays: [],
            openedStages: [],
            stageWidths: [],
            groupWidths: [],
            _stageKeys: {},
            _groupKeys: {},
            _ruleKeys: {},
            _ruleDisplayKeys: {},
            _openedStageKeys: {},
            _stageWidthKeys: {},
            _groupWidthKeys: {}
        };
    }

    function cleanSummary(change) {
        delete change._stageKeys;
        delete change._groupKeys;
        delete change._ruleKeys;
        delete change._ruleDisplayKeys;
        delete change._openedStageKeys;
        delete change._stageWidthKeys;
        delete change._groupWidthKeys;
        return change;
    }

    function mergeRefs(target, source) {
        var keys = {};
        for (var i = 0; i < target.length; i++) {
            keys[refKey(target[i])] = true;
        }
        for (var i = 0; i < source.length; i++) {
            var key = refKey(source[i]);
            if (keys[key]) continue;
            keys[key] = true;
            target.push(source[i]);
        }
    }

    function refKey(ref) {
        var parts = [ref.si];
        if (ref.gi !== undefined) parts.push(ref.gi);
        if (ref.ri !== undefined) parts.push(ref.ri);
        return parts.join('-');
    }

    function mergeTransitionSummary(target, source) {
        if (!target) return source;
        if (!source) return target;
        target.changed = target.changed || source.changed;
        target.featuresChanged = target.featuresChanged || source.featuresChanged;
        mergeRefs(target.stages, source.stages);
        mergeRefs(target.groups, source.groups);
        mergeRefs(target.rules, source.rules);
        mergeRefs(target.ruleDisplays, source.ruleDisplays || source.rules);
        mergeRefs(target.openedStages, source.openedStages);
        mergeRefs(target.stageWidths, source.stageWidths);
        mergeRefs(target.groupWidths, source.groupWidths);
        return target;
    }

    function markRef(change, list, keys, key, ref) {
        if (keys[key]) return;
        keys[key] = true;
        list.push(ref);
    }

    function markStage(change, si) {
        markRef(change, change.stages, change._stageKeys, String(si), { si: si });
    }

    function markGroup(change, si, gi) {
        markRef(change, change.groups, change._groupKeys, si + '-' + gi, { si: si, gi: gi });
    }

    function markRule(change, si, gi, ri) {
        markRef(change, change.rules, change._ruleKeys, si + '-' + gi + '-' + ri, {
            si: si,
            gi: gi,
            ri: ri
        });
    }

    function markRuleDisplay(change, si, gi, ri) {
        markRef(change, change.ruleDisplays, change._ruleDisplayKeys, si + '-' + gi + '-' + ri, {
            si: si,
            gi: gi,
            ri: ri
        });
    }

    function markOpenedStage(change, si) {
        markRef(change, change.openedStages, change._openedStageKeys, String(si), { si: si });
    }

    function markStageWidth(change, si) {
        markRef(change, change.stageWidths, change._stageWidthKeys, String(si), { si: si });
    }

    function markGroupWidth(change, si, gi) {
        markRef(change, change.groupWidths, change._groupWidthKeys, si + '-' + gi, { si: si, gi: gi });
    }

    function setStageOpen(change, uiState, si, open) {
        var stage = uiState.stages[si];
        if (!stage || stage.open === open) return false;
        stage.open = open;
        change.changed = true;
        markStage(change, si);
        if (open) markOpenedStage(change, si);
        return true;
    }

    function setGroupOpen(change, uiState, si, gi, open) {
        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        if (!group || group.open === open) return false;
        group.open = open;
        change.changed = true;
        markGroup(change, si, gi);
        return true;
    }

    function setRuleOpen(change, uiState, si, gi, ri, open) {
        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        var rule = group && group.rules[ri];
        if (!rule || rule.open === open) return false;
        rule.open = open;
        change.changed = true;
        markRule(change, si, gi, ri);
        markRuleDisplay(change, si, gi, ri);
        return true;
    }

    function toggleStage(uiState, groups, si) {
        var change = transitionSummary();
        var stage = uiState.stages[si];
        if (!stage) return cleanSummary(change);

        setStageOpen(change, uiState, si, !stage.open);
        markStageWidth(change, si);
        return cleanSummary(change);
    }

    function openStage(uiState, groups, si) {
        var change = transitionSummary();
        if (!hasStageRules(groups, si)) return cleanSummary(change);

        setStageOpen(change, uiState, si, true);
        markStageWidth(change, si);
        return cleanSummary(change);
    }

    function stageAnyRuleOpen(uiState, groups, si) {
        var stage = uiState.stages[si];
        if (!stage) return false;

        for (var gi = 0; gi < stageGroups(groups, si).length; gi++) {
            var count = groupRuleCount(groups, si, gi);
            var group = stage.groups[gi];
            if (!group) continue;
            if (count > 1) {
                if (group.open) {
                    for (var ri = 0; ri < count; ri++) {
                        if (group.rules[ri] && group.rules[ri].open) return true;
                    }
                }
            } else if (count === 1 && group.rules[0] && group.rules[0].open) {
                return true;
            }
        }
        return false;
    }

    function toggleStageRules(uiState, groups, si) {
        var change = transitionSummary();
        if (!hasStageRules(groups, si)) return cleanSummary(change);

        var anyOpen = stageAnyRuleOpen(uiState, groups, si);
        for (var gi = 0; gi < stageGroups(groups, si).length; gi++) {
            var count = groupRuleCount(groups, si, gi);
            if (count > 1) {
                setGroupOpen(change, uiState, si, gi, !anyOpen);
                markGroupWidth(change, si, gi);
            }
            for (var ri = 0; ri < count; ri++) {
                setRuleOpen(change, uiState, si, gi, ri, !anyOpen);
                markRuleDisplay(change, si, gi, ri);
            }
        }
        markStageWidth(change, si);
        return cleanSummary(change);
    }

    function toggleGroup(uiState, groups, si, gi) {
        var change = transitionSummary();
        var count = groupRuleCount(groups, si, gi);
        if (count <= 1) return cleanSummary(change);

        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        if (!group) return cleanSummary(change);

        var opening = !group.open;
        setGroupOpen(change, uiState, si, gi, opening);
        if (opening) {
            for (var ri = 0; ri < count; ri++) {
                setRuleOpen(change, uiState, si, gi, ri, ri === count - 1);
                markRuleDisplay(change, si, gi, ri);
            }
        }
        markGroupWidth(change, si, gi);
        markStageWidth(change, si);
        return cleanSummary(change);
    }

    function toggleGroupRules(uiState, groups, si, gi) {
        var change = transitionSummary();
        var count = groupRuleCount(groups, si, gi);
        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        if (count <= 1 || !group || !group.open) return cleanSummary(change);

        var anyOpen = false;
        for (var ri = 0; ri < count; ri++) {
            if (group.rules[ri] && group.rules[ri].open) {
                anyOpen = true;
                break;
            }
        }
        for (var ri = 0; ri < count; ri++) {
            setRuleOpen(change, uiState, si, gi, ri, !anyOpen);
            markRuleDisplay(change, si, gi, ri);
        }
        markGroupWidth(change, si, gi);
        markStageWidth(change, si);
        return cleanSummary(change);
    }

    function openClosedGroupForRuleReveal(change, uiState, groups, si, gi, ri, expandRule) {
        var count = groupRuleCount(groups, si, gi);
        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        if (count <= 1 || !group || group.open) return false;

        setGroupOpen(change, uiState, si, gi, true);
        for (var r = 0; r < count; r++) {
            setRuleOpen(change, uiState, si, gi, r, r === ri && !!expandRule);
            markRuleDisplay(change, si, gi, r);
        }
        markGroupWidth(change, si, gi);
        markStageWidth(change, si);
        return true;
    }

    function ensureRuleVisible(uiState, groups, si, gi, ri) {
        var change = transitionSummary();
        var stage = uiState.stages[si];
        if (!stage) return cleanSummary(change);

        if (setStageOpen(change, uiState, si, true)) markStageWidth(change, si);

        if (!openClosedGroupForRuleReveal(change, uiState, groups, si, gi, ri, true) &&
                setRuleOpen(change, uiState, si, gi, ri, true)) {
            markGroupWidth(change, si, gi);
            markStageWidth(change, si);
        }

        return cleanSummary(change);
    }

    function toggleRule(uiState, groups, si, gi, ri) {
        var change = transitionSummary();
        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        var rule = group && group.rules[ri];
        if (!rule) return cleanSummary(change);

        setRuleOpen(change, uiState, si, gi, ri, !rule.open);
        markGroupWidth(change, si, gi);
        markStageWidth(change, si);
        return cleanSummary(change);
    }

    function setLayoutPanelsOpen(uiState, groups, open) {
        var change = transitionSummary();
        for (var si = 0; si < uiState.stages.length; si++) {
            setStageOpen(change, uiState, si, hasStageRules(groups, si) && open);
            markStageWidth(change, si);
            for (var gi = 0; gi < stageGroups(groups, si).length; gi++) {
                var count = groupRuleCount(groups, si, gi);
                setGroupOpen(change, uiState, si, gi, count <= 1 || open);
                if (count > 1) markGroupWidth(change, si, gi);
                for (var ri = 0; ri < count; ri++) {
                    setRuleOpen(change, uiState, si, gi, ri, open);
                }
            }
        }
        return cleanSummary(change);
    }

    function openRulePath(uiState, groups, si, gi, ri) {
        var change = transitionSummary();
        var stage = uiState.stages[si];
        if (!stage) return cleanSummary(change);

        if (setStageOpen(change, uiState, si, true)) markStageWidth(change, si);
        if (!openClosedGroupForRuleReveal(change, uiState, groups, si, gi, ri, true) &&
                setRuleOpen(change, uiState, si, gi, ri, true)) {
            markGroupWidth(change, si, gi);
            markStageWidth(change, si);
        }
        return cleanSummary(change);
    }

    function openRulePathForSearchMatch(uiState, groups, si, gi, ri, expandRule) {
        var change = transitionSummary();
        var stage = uiState.stages[si];
        if (!stage) return cleanSummary(change);

        if (setStageOpen(change, uiState, si, true)) markStageWidth(change, si);
        if (!openClosedGroupForRuleReveal(change, uiState, groups, si, gi, ri, expandRule) &&
                setRuleOpen(change, uiState, si, gi, ri, !!expandRule)) {
            markGroupWidth(change, si, gi);
            markStageWidth(change, si);
        }
        return cleanSummary(change);
    }

    function allStagesOpen(uiState, groups) {
        for (var si = 0; si < uiState.stages.length; si++) {
            if (hasStageRules(groups, si) && !uiState.stages[si].open) return false;
        }
        return true;
    }

    function allGroupsOpen(uiState, groups) {
        for (var si = 0; si < uiState.stages.length; si++) {
            if (!hasStageRules(groups, si)) continue;
            for (var gi = 0; gi < stageGroups(groups, si).length; gi++) {
                if (groupRuleCount(groups, si, gi) > 1 &&
                    uiState.stages[si].groups[gi] &&
                    !uiState.stages[si].groups[gi].open) {
                    return false;
                }
            }
        }
        return true;
    }

    function anyExpandableRuleOpen(uiState, groups) {
        for (var si = 0; si < uiState.stages.length; si++) {
            for (var gi = 0; gi < stageGroups(groups, si).length; gi++) {
                if (groupRuleCount(groups, si, gi) <= 1) continue;
                var group = uiState.stages[si].groups[gi];
                if (!group) continue;
                for (var ri = 0; ri < group.rules.length; ri++) {
                    if (group.rules[ri].open) return true;
                }
            }
        }
        return false;
    }

    function allRulesOpen(uiState, groups) {
        for (var si = 0; si < uiState.stages.length; si++) {
            for (var gi = 0; gi < stageGroups(groups, si).length; gi++) {
                var group = uiState.stages[si].groups[gi];
                if (!group) continue;
                for (var ri = 0; ri < groupRuleCount(groups, si, gi); ri++) {
                    if (!group.rules[ri] || !group.rules[ri].open) return false;
                }
            }
        }
        return true;
    }

    function allRepresentativeRulesOpen(uiState, groups) {
        for (var si = 0; si < uiState.stages.length; si++) {
            for (var gi = 0; gi < stageGroups(groups, si).length; gi++) {
                var count = groupRuleCount(groups, si, gi);
                if (count <= 0) continue;

                var group = uiState.stages[si].groups[gi];
                var rule = group && group.rules[count - 1];
                if (!rule || !rule.open) return false;
            }
        }
        return true;
    }

    function nextGlobalCycleState(uiState, groups) {
        if (allStagesOpen(uiState, groups) &&
            allGroupsOpen(uiState, groups) &&
            allRulesOpen(uiState, groups)) {
            return 0;
        }
        if (!allStagesOpen(uiState, groups)) return 1;
        if (!allGroupsOpen(uiState, groups)) return 2;
        if (!allRepresentativeRulesOpen(uiState, groups)) return 3;
        return 4;
    }

    function groupOpenForGlobalPreset(preset, count, currentOpen) {
        if (count <= 1) return true;
        if (preset && preset.groupsOpen === 'preserve') return !!currentOpen;
        return !!(preset && preset.groupsOpen);
    }

    function ruleOpenForGlobalPreset(preset, count, ri, currentOpen) {
        if (preset && preset.rules === 'preserve') return !!currentOpen;
        if (!preset || preset.rules === 'none') return false;
        if (count <= 1) return !!currentOpen || preset.rules === 'last' || preset.rules === 'all';
        if (preset.rules === 'all') return true;
        return !!currentOpen || ri === count - 1;
    }

    function applyGlobalCycleState(uiState, groups, state, presets) {
        var change = transitionSummary();
        var fallbackPresets = presets || [
            { stagesOpen: false, groupsOpen: false, rules: 'none' },
            { stagesOpen: true, groupsOpen: 'preserve', rules: 'preserve' },
            { stagesOpen: true, groupsOpen: true, rules: 'preserve' },
            { stagesOpen: true, groupsOpen: true, rules: 'last' },
            { stagesOpen: true, groupsOpen: true, rules: 'all' }
        ];
        var preset = fallbackPresets[state] || fallbackPresets[0];

        for (var si = 0; si < uiState.stages.length; si++) {
            markStageWidth(change, si);
            if (!hasStageRules(groups, si)) {
                setStageOpen(change, uiState, si, false);
                continue;
            }

            setStageOpen(change, uiState, si, preset.stagesOpen);
            for (var gi = 0; gi < stageGroups(groups, si).length; gi++) {
                var count = groupRuleCount(groups, si, gi);
                var group = uiState.stages[si].groups[gi];
                if (count > 1) markGroupWidth(change, si, gi);
                setGroupOpen(change, uiState, si, gi, groupOpenForGlobalPreset(
                    preset,
                    count,
                    group && group.open
                ));
                for (var ri = 0; ri < count; ri++) {
                    var rule = group && group.rules[ri];
                    setRuleOpen(
                        change,
                        uiState,
                        si,
                        gi,
                        ri,
                        ruleOpenForGlobalPreset(preset, count, ri, rule && rule.open)
                    );
                }
            }
        }
        return cleanSummary(change);
    }

    function featureSummary() {
        return {
            changed: false,
            rules: []
        };
    }

    function validRuleFeature(feature) {
        return feature === 'fields' || feature === 'pinned' || feature === 'info';
    }

    function addFeatureRule(change, si, gi, ri, feature, value) {
        change.rules.push({
            si: si,
            gi: gi,
            ri: ri,
            feature: feature,
            value: value
        });
    }

    function setRuleFeatureOnRule(change, rule, feature, value) {
        if (rule[feature] === value) return;
        rule[feature] = value;
        change.featuresChanged = true;
    }

    function setRuleFeature(uiState, si, gi, ri, feature, value) {
        var change = featureSummary();
        if (!validRuleFeature(feature)) return change;
        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        var rule = group && group.rules[ri];
        value = !!value;
        if (!rule || rule[feature] === value) return change;

        rule[feature] = value;
        change.changed = true;
        addFeatureRule(change, si, gi, ri, feature, rule[feature]);
        return change;
    }

    function toggleRuleFeature(uiState, si, gi, ri, feature) {
        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        var rule = group && group.rules[ri];
        if (!rule || !validRuleFeature(feature)) return featureSummary();
        return setRuleFeature(uiState, si, gi, ri, feature, !rule[feature]);
    }

    function setRuleFeatures(uiState, si, gi, ri, features) {
        var change = featureSummary();
        var stage = uiState.stages[si];
        var group = stage && stage.groups[gi];
        var rule = group && group.rules[ri];
        if (!rule || !features) return change;

        for (var i = 0; i < 3; i++) {
            var feature = ['fields', 'pinned', 'info'][i];
            if (!Object.prototype.hasOwnProperty.call(features, feature)) continue;
            if (rule[feature] === features[feature]) continue;
            rule[feature] = !!features[feature];
            change.changed = true;
            addFeatureRule(change, si, gi, ri, feature, rule[feature]);
        }
        return change;
    }

    function cloneRuleRef(ref) {
        return ref ? { si: ref.si, gi: ref.gi, ri: ref.ri } : null;
    }

    function sameRuleRef(a, b) {
        if (!a || !b) return !a && !b;
        return a.si === b.si && a.gi === b.gi && a.ri === b.ri;
    }

    function fullscreenState(uiState) {
        if (!uiState.fullscreen) {
            uiState.fullscreen = {
                mode: null,
                currentRule: null
            };
        }
        return uiState.fullscreen;
    }

    function fullscreenCurrentRule(uiState) {
        var state = fullscreenState(uiState);
        return state.mode && state.currentRule ? state.currentRule : null;
    }

    function setFullscreenState(uiState, mode, ref) {
        var state = fullscreenState(uiState);
        var previous = {
            mode: state.mode,
            currentRule: cloneRuleRef(state.currentRule)
        };

        if (!mode || !ref) {
            state.mode = null;
            state.currentRule = null;
        } else {
            state.mode = mode;
            state.currentRule = cloneRuleRef(ref);
        }

        return {
            changed: previous.mode !== state.mode ||
                !sameRuleRef(previous.currentRule, state.currentRule),
            previous: previous,
            current: {
                mode: state.mode,
                currentRule: cloneRuleRef(state.currentRule)
            }
        };
    }

    function clearFullscreen(uiState) {
        return setFullscreenState(uiState, null, null);
    }

    function syncFullscreenMode(uiState, mode) {
        var state = fullscreenState(uiState);
        var previous = state.mode;
        if (!state.currentRule || !mode || state.mode === mode) {
            return {
                changed: false,
                previousMode: previous,
                currentMode: state.mode
            };
        }

        state.mode = mode;
        return {
            changed: true,
            previousMode: previous,
            currentMode: state.mode
        };
    }

    function applyLayoutState(uiState, groups, snapshot) {
        var change = transitionSummary();
        change.featuresChanged = false;
        for (var si = 0; si < uiState.stages.length; si++) {
            var savedStage = snapshot[si];
            var liveStage = uiState.stages[si];
            if (!savedStage || !liveStage) continue;
            setStageOpen(change, uiState, si, savedStage.open);
            markStageWidth(change, si);

            for (var gi = 0; gi < liveStage.groups.length; gi++) {
                var savedGroup = savedStage.groups[gi];
                var liveGroup = liveStage.groups[gi];
                if (!savedGroup || !liveGroup) continue;
                var count = groupRuleCount(groups, si, gi);
                setGroupOpen(change, uiState, si, gi, savedGroup.open);
                if (count > 1) markGroupWidth(change, si, gi);

                for (var ri = 0; ri < liveGroup.rules.length; ri++) {
                    var savedRule = savedGroup.rules[ri];
                    var liveRule = liveGroup.rules[ri];
                    if (!savedRule || !liveRule) continue;
                    setRuleOpen(change, uiState, si, gi, ri, savedRule.open);
                    if ('fields' in savedRule) setRuleFeatureOnRule(change, liveRule, 'fields', savedRule.fields);
                    if ('pinned' in savedRule) setRuleFeatureOnRule(change, liveRule, 'pinned', savedRule.pinned);
                    if ('info' in savedRule) setRuleFeatureOnRule(change, liveRule, 'info', savedRule.info);
                }
            }
        }
        return cleanSummary(change);
    }

    return {
        allGroupsOpen: allGroupsOpen,
        allRulesOpen: allRulesOpen,
        allStagesOpen: allStagesOpen,
        anyExpandableRuleOpen: anyExpandableRuleOpen,
        applyLayoutState: applyLayoutState,
        applyGlobalCycleState: applyGlobalCycleState,
        buildAllRules: buildAllRules,
        clearFullscreen: clearFullscreen,
        createInitialUiState: createInitialUiState,
        emptyUiState: emptyUiState,
        ensureRuleVisible: ensureRuleVisible,
        fullscreenCurrentRule: fullscreenCurrentRule,
        fullscreenState: fullscreenState,
        mergeTransitionSummary: mergeTransitionSummary,
        nextGlobalCycleState: nextGlobalCycleState,
        openRulePath: openRulePath,
        openRulePathForSearchMatch: openRulePathForSearchMatch,
        openStage: openStage,
        setFullscreenState: setFullscreenState,
        setRuleFeature: setRuleFeature,
        setRuleFeatures: setRuleFeatures,
        setLayoutPanelsOpen: setLayoutPanelsOpen,
        snapshotLayout: snapshotLayout,
        syncFullscreenMode: syncFullscreenMode,
        toggleGroup: toggleGroup,
        toggleGroupRules: toggleGroupRules,
        toggleRuleFeature: toggleRuleFeature,
        toggleRule: toggleRule,
        toggleStage: toggleStage,
        toggleStageRules: toggleStageRules
    };
})();
