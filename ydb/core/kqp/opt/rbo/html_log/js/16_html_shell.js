/* ══════════════════════════════════════════════════════════════
   HTML helpers
   ══════════════════════════════════════════════════════════════ */

function htmlEscape(text) {
    text = String(text == null ? '' : text);
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function searchMarkAttrs(attrs) {
    if (!attrs) return '';
    var result = '';
    var targetId = attrs['data-search-target-id'] || '';
    var occurrenceBase = Math.max(0, Math.floor(Number(attrs['data-search-occurrence-base']) || 0));
    var occurrence = arguments.length > 1
        ? Math.max(0, Math.floor(Number(arguments[1]) || 0))
        : 0;
    for (var key in attrs) {
        if (!Object.prototype.hasOwnProperty.call(attrs, key)) continue;
        if (key === 'data-search-occurrence-base') continue;
        var value = attrs[key];
        if (value === undefined || value === null || value === '') continue;
        result += ' ' + key + '="' + htmlEscape(value) + '"';
    }
    if (targetId) {
        result += ' data-search-match-id="' +
            htmlEscape(searchRenderedMatchId(targetId, occurrenceBase + occurrence)) + '"';
    }
    return result;
}

function htmlHighlight(text, query, field, attrs) {
    text = String(text == null ? '' : text);
    if (!query) return htmlEscape(text);
    var lowerText = text.toLowerCase();
    var lowerQuery = query.toLowerCase();
    var markAttrs = field ? ' data-search-field="' + htmlEscape(field) + '"' : '';
    var result = '';
    var pos = 0;
    var occurrence = 0;
    while (pos < text.length) {
        var idx = lowerText.indexOf(lowerQuery, pos);
        if (idx === -1) {
            result += htmlEscape(text.substring(pos));
            break;
        }
        result += htmlEscape(text.substring(pos, idx));
        result += '<mark' + markAttrs + searchMarkAttrs(attrs, occurrence) + '>' +
                  htmlEscape(text.substring(idx, idx + query.length)) + '</mark>';
        pos = idx + query.length;
        occurrence++;
    }
    return result;
}

function searchRenderedIdentityPart(value) {
    return value === undefined || value === null ? '' : String(value);
}

function searchRenderedTargetId(source) {
    source = source || {};
    var keys = [
        'type',
        'si',
        'gi',
        'ri',
        'field',
        'path',
        'row',
        'part',
        'fieldKey',
        'pinnedSurface',
        'tabId',
        'switcherKey',
        'switcherId',
        'section',
        'node',
        'edge',
        'line',
        'metaIndex',
        'fieldDetail'
    ];
    var parts = [];
    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        if (source[key] === undefined || source[key] === null || source[key] === '') continue;
        parts.push(key + '=' + searchRenderedIdentityPart(source[key]));
    }
    return parts.length ? parts.join('|') : '';
}

function searchRenderedMatchId(targetId, occurrence) {
    return String(targetId || '') + '|occurrence=' +
        searchRenderedIdentityPart(Math.max(0, Math.floor(Number(occurrence) || 0)));
}

function searchContextWith(context, extra) {
    var result = {};
    var wrote = false;
    context = context || {};
    extra = extra || {};
    for (var key in context) {
        if (!Object.prototype.hasOwnProperty.call(context, key)) continue;
        result[key] = context[key];
        wrote = true;
    }
    for (var extraKey in extra) {
        if (!Object.prototype.hasOwnProperty.call(extra, extraKey)) continue;
        result[extraKey] = extra[extraKey];
        wrote = true;
    }
    return wrote ? result : null;
}

function searchMarkContextAttrs(field, context) {
    context = context || {};
    var source = searchContextWith(context, {
        type: context.type || 'rule',
        field: context.field || field || ''
    }) || {};
    var targetId = searchRenderedTargetId(source);
    var attrs = {};
    if (targetId) attrs['data-search-target-id'] = targetId;
    if (context.tabId) attrs['data-search-tab-id'] = context.tabId;
    if (context.switcherKey) attrs['data-search-switcher-key'] = context.switcherKey;
    if (context.switcherId) attrs['data-search-switcher-id'] = context.switcherId;
    if (context.occurrenceBase !== undefined) {
        attrs['data-search-occurrence-base'] = context.occurrenceBase;
    }
    return attrs;
}

function structuralHighlightAllowed(kind, scope) {
    return kind === 'stage' || kind === 'group' || kind === 'rule';
}

function structuralSearchField(kind) {
    if (kind === 'group') return 'group';
    if (kind === 'rule') return 'name';
    if (kind === 'stage') return 'stage';
    return '';
}

var SEARCH_LABEL_HIGHLIGHT_SIGNATURE_PROP = '__traceSearchLabelHighlightSignature';
var SEARCH_LABEL_HIGHLIGHT_CHUNK_SIZE = 80;
var SEARCH_LABEL_HIGHLIGHT_FRAME_BUDGET_MS = 6;
var COLLAPSED_SEARCH_INDICATOR_CHUNK_SIZE = 512;
var COLLAPSED_SEARCH_INDICATOR_FRAME_BUDGET_MS = 6;
var searchMarksSurface = createVisualSurface({
    name: 'search-marks',
    epochScopes: ['trace', 'search'],
    domGenerations: ['searchMarks', 'virtualRows'],
    allowMissingTarget: true,
    resolveTarget: function(ref, options) {
        return options && options.target || null;
    }
});

function commitSearchMarksSurface(visualCtx, options, writer) {
    options = options || {};
    if (!canCommitSearchMarks(options.label || 'search-marks-surface', options.target || null, options)) {
        return false;
    }
    return searchMarksSurface.commit(visualCtx, options, writer);
}

function searchLabelContext(label, kind) {
    if (!label || !label.getAttribute) return null;
    return {
        type: kind || '',
        field: structuralSearchField(kind),
        si: label.getAttribute('data-search-si') || '',
        gi: label.getAttribute('data-search-gi') || '',
        ri: label.getAttribute('data-search-ri') || ''
    };
}

function searchLabelHighlightHtml(text, kind, query, scope, context) {
    return structuralHighlightAllowed(kind, scope)
        ? htmlHighlight(text, query, structuralSearchField(kind),
            searchMarkContextAttrs(structuralSearchField(kind), context))
        : htmlEscape(text);
}

function cancelSearchLabelHighlightJob(reason) {
    var search = searchRuntime();
    var job = search.searchLabelHighlightJob;
    if (!job) return false;
    job.cancelled = true;
    job.cancelReason = reason || 'cancelled';
    if (job.frameHandle) cancelRenderFrameWork(job.frameHandle);
    if (search.searchLabelHighlightJob === job) search.searchLabelHighlightJob = null;
    return true;
}

function searchLabelHighlightJobCurrent(job) {
    if (!job || job.cancelled || searchRuntime().searchLabelHighlightJob !== job) return false;
    if (!searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true })) {
        cancelSearchLabelHighlightJob('stale-search-transaction');
        return false;
    }
    if (!virtualRowsDomGenerationCurrent(job.virtualRowsDomGeneration)) {
        cancelSearchLabelHighlightJob('stale-virtual-rows-dom-generation');
        return false;
    }
    if (!searchMarksDomGenerationCurrent(job.searchMarksDomGeneration)) {
        cancelSearchLabelHighlightJob('stale-search-marks-dom-generation');
        return false;
    }
    return true;
}

function searchLabelHighlightChunkSize() {
    return Math.max(1, Math.floor(Number(SEARCH_LABEL_HIGHLIGHT_CHUNK_SIZE)) || 1);
}

function searchLabelHighlightFrameBudgetMs() {
    return Math.max(1, Number(SEARCH_LABEL_HIGHLIGHT_FRAME_BUDGET_MS) || 1);
}

function mountedSearchLabelSnapshotCurrent(cache) {
    if (!cache) return false;
    if (cache.documentRef !== document) return false;
    if (cache.traceEpoch !== currentRuntimeEpoch().trace) return false;
    if (cache.virtualRowsDomGeneration !== currentVirtualRowsDomGeneration()) return false;
    return true;
}

function mountedSearchLabelSnapshot(options) {
    options = options || {};
    if (!hasDOM() || !document.querySelectorAll) return [];

    var search = searchRuntime();
    var cache = search.mountedSearchLabelCache;
    if (!options.forceScan && mountedSearchLabelSnapshotCurrent(cache)) {
        search.mountedSearchLabelCacheHits++;
        return cache.labels;
    }

    var labels = Array.prototype.slice.call(document.querySelectorAll('[data-search-label]'));
    search.mountedSearchLabelCache = {
        labels: labels,
        documentRef: document,
        traceEpoch: currentRuntimeEpoch().trace,
        virtualRowsDomGeneration: currentVirtualRowsDomGeneration()
    };
    search.mountedSearchLabelCacheScans++;
    return labels;
}

function applySearchLabelHighlightForCommit(label, job) {
    if (!label || label.isConnected === false) {
        recordVisualCommitDenied('search-marks', 'search-label-highlight', 'detached_target', {
            query: job.query,
            scope: job.scope
        });
        return false;
    }
    var text = label.getAttribute('data-search-label') || '';
    var kind = label.getAttribute('data-search-kind') || '';
    var html = searchLabelHighlightHtml(text, kind, job.query, job.scope,
        searchLabelContext(label, kind));
    if (label[SEARCH_LABEL_HIGHLIGHT_SIGNATURE_PROP] === html &&
            label.innerHTML === html) {
        return false;
    }
    label.innerHTML = html;
    label[SEARCH_LABEL_HIGHLIGHT_SIGNATURE_PROP] = html;
    return true;
}

function completeSearchLabelHighlightJob(job) {
    if (searchRuntime().searchLabelHighlightJob === job) {
        searchRuntime().searchLabelHighlightJob = null;
    }
}

function scheduleSearchLabelHighlightChunk(job) {
    job.frameHandle = scheduleRenderDeferredWork(
        'search-label-highlight-chunk',
        function runScheduledSearchLabelHighlightChunk(visualCtx) {
            job.frameHandle = null;
            var measure = typeof measureSearchTimingForEpoch === 'function'
                ? measureSearchTimingForEpoch
                : function(epoch, label, fn) { return fn(); };
            measure(job.timingEpoch, 'label-highlight-update', function() {
                runSearchLabelHighlightChunk(visualCtx, job);
            }, {
                scope: job.scope,
                queryLength: String(job.query || '').length,
                chunked: true
            });
        },
        {
            token: job.searchTransactionToken
                ? searchTransactionFrameToken(job.searchTransactionToken)
                : renderFrameEpochToken(['trace', 'search']),
            ownerId: job.ownerId || '',
            label: 'search-label-highlight-chunk',
            surfaces: ['search-marks'],
            withVisualContext: true,
            onDiscard: function(reason) {
                if (searchRuntime().searchLabelHighlightJob === job) {
                    job.cancelled = true;
                    job.cancelReason = reason || 'discarded';
                    searchRuntime().searchLabelHighlightJob = null;
                }
            }
        }
    );
}

function runSearchLabelHighlightChunk(visualCtx, job) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        job = visualCtx;
        return runVisualSurfaceCommitNow(
            'search-label-highlight-chunk',
            ['search-marks'],
            ['trace', 'search'],
            function(ctx) {
                return runSearchLabelHighlightChunk(ctx, job);
            }
        );
    }
    if (!searchLabelHighlightJobCurrent(job)) return false;

    var committed = false;
    commitSearchMarksSurface(visualCtx, {
        label: 'search-label-highlight-chunk',
        searchTransactionToken: job.searchTransactionToken,
        allowWithoutSearchTransaction: !!job.allowWithoutSearchTransaction,
        domGenerations: {
            searchMarks: job.searchMarksDomGeneration,
            virtualRows: job.virtualRowsDomGeneration
        }
    }, function() {
        var startedAt = typeof searchTimingNow === 'function' ? searchTimingNow() : Date.now();
        var limit = searchLabelHighlightChunkSize();
        var budgetMs = searchLabelHighlightFrameBudgetMs();
        var processed = 0;
        var changed = false;

        var labels = mountedSearchLabelSnapshot({ forceScan: true });
        job.labelCount = labels.length;
        while (job.index < labels.length && processed < limit) {
            var label = labels[job.index++];
            processed++;
            if (applySearchLabelHighlightForCommit(label, job)) {
                changed = true;
                job.changedCount++;
            }
            if (processed > 0) {
                var now = typeof searchTimingNow === 'function' ? searchTimingNow() : Date.now();
                if (now - startedAt >= budgetMs) break;
            }
        }

        job.processedCount += processed;
        if (changed) {
            job.searchMarksDomGeneration = bumpSearchMarksDomGeneration();
        }
        committed = true;
    });
    if (!committed) return false;

    reactivateActiveSearchMatchForLabels();

    if (job.index < (job.labelCount || 0)) {
        scheduleSearchLabelHighlightChunk(job);
    } else {
        completeSearchLabelHighlightJob(job);
    }
    return true;
}

function emptyCollapsedSearchIndicators() {
    return { stages: {}, groups: {}, rules: {} };
}

function hiddenSearchIndicatorEnabled(query) {
    if (!query) return false;
    return true;
}

function groupSearchIndicatorKey(si, gi) {
    return si + '-' + gi;
}

function collapsedSearchIndicatorClass(bucket, key) {
    return searchRuntime().collapsedSearchIndicators &&
           searchRuntime().collapsedSearchIndicators[bucket] &&
           searchRuntime().collapsedSearchIndicators[bucket][key]
        ? ' search-hidden-match'
        : '';
}

function collapsedSearchIndicatorTarget(match) {
    if (!match) return null;

    if (match.type === 'group') {
        if (!effectiveStageOpen(match.si)) {
            return {
                bucket: 'stages',
                key: String(match.si),
                si: match.si
            };
        }
        return null;
    }

    if (match.type !== 'rule') return null;

    if (!effectiveStageOpen(match.si)) {
        return {
            bucket: 'stages',
            key: String(match.si),
            si: match.si
        };
    }

    if (groupRuleCount(match.si, match.gi) > 1 && !effectiveGroupOpen(match.si, match.gi)) {
        return {
            bucket: 'groups',
            key: groupSearchIndicatorKey(match.si, match.gi),
            si: match.si,
            gi: match.gi
        };
    }

    if (!effectiveRuleOpen(match.si, match.gi, match.ri) && match.field !== 'name') {
        return {
            bucket: 'rules',
            key: ruleKey(match.si, match.gi, match.ri),
            si: match.si,
            gi: match.gi,
            ri: match.ri
        };
    }

    return null;
}

function addSearchIndicator(indicators, bucket, key) {
    if (indicators && indicators[bucket]) indicators[bucket][key] = true;
}

function searchIndicatorAreaTarget(match, target) {
    if (target) return target;
    if (!match || match.type !== 'rule') return null;
    return {
        bucket: 'rules',
        key: ruleKey(match.si, match.gi, match.ri),
        si: match.si,
        gi: match.gi,
        ri: match.ri
    };
}

function collapsedSearchIndicatorInterval(target, model) {
    if (!target) return null;
    if (target.bucket === 'stages') return stageTraceInterval(target.si, model);
    if (target.bucket === 'groups') return groupTraceInterval(target.si, target.gi, model);
    if (target.bucket === 'rules') {
        return searchMatchTraceInterval({
            type: 'rule',
            si: target.si,
            gi: target.gi,
            ri: target.ri
        }, model);
    }
    return null;
}

function collapsedSearchIndicatorInCurrentArea(target, model) {
    return true;
}

function collectCollapsedSearchIndicatorsFromMatches(query, matches) {
    var indicators = emptyCollapsedSearchIndicators();
    if (!hiddenSearchIndicatorEnabled(query)) return indicators;

    matches = matches || [];
    var model = getTraceLayoutModel();

    for (var i = 0; i < matches.length; i++) {
        var target = collapsedSearchIndicatorTarget(matches[i]);
        if (!target) continue;
        var areaTarget = searchIndicatorAreaTarget(matches[i], target);
        if (!areaTarget || !collapsedSearchIndicatorInCurrentArea(areaTarget, model)) continue;

        addSearchIndicator(indicators, target.bucket, target.key);
    }

    return indicators;
}

function collectCollapsedSearchIndicators(query, scope, matches) {
    if (!matches) matches = collectSearchMatchesFromIndex(query, scope);
    return collectCollapsedSearchIndicatorsFromMatches(query, matches);
}

function indicatorMapCount(map) {
    var count = 0;
    for (var key in map) {
        if (Object.prototype.hasOwnProperty.call(map, key)) count++;
    }
    return count;
}

function collapsedSearchIndicatorTotal(indicators) {
    return indicatorMapCount(indicators.stages) +
           indicatorMapCount(indicators.groups) +
           indicatorMapCount(indicators.rules);
}

function searchIndicatorCommitDetails(options) {
    options = options || {};
    var details = options.details || {};
    if (options.ownerId &&
            details.ownerId === undefined &&
            !searchRuntime().searchMarksTransitionOwnerId) {
        details.ownerId = options.ownerId;
    }
    if (Object.prototype.hasOwnProperty.call(options, 'searchTransactionToken') &&
            !Object.prototype.hasOwnProperty.call(details, 'searchTransactionToken')) {
        details.searchTransactionToken = options.searchTransactionToken;
    }
    if (options.allowWithoutSearchTransaction) {
        details.allowWithoutSearchTransaction = true;
    }
    return details;
}

function searchIndicatorCommitDetailsWith(options, extras) {
    var base = searchIndicatorCommitDetails(options);
    var details = {};
    var key;
    for (key in base) {
        if (Object.prototype.hasOwnProperty.call(base, key)) details[key] = base[key];
    }
    extras = extras || {};
    for (key in extras) {
        if (Object.prototype.hasOwnProperty.call(extras, key)) details[key] = extras[key];
    }
    return details;
}

function parseIndicatorKeyParts(key, count) {
    var raw = String(key || '').split('-');
    if (raw.length !== count) return null;
    var parts = [];
    for (var i = 0; i < raw.length; i++) {
        var value = Number(raw[i]);
        if (!Number.isInteger(value) || value < 0) return null;
        parts.push(value);
    }
    return parts;
}

function parentVirtualRowElement(el) {
    while (el) {
        if (el.getAttribute && el.getAttribute('data-virtual-row')) return el;
        el = el.parentElement || el.parentNode || null;
    }
    return null;
}

function refreshVirtualRowSignatureCacheForElement(el) {
    if (typeof virtualRuntime !== 'function') return;
    var rowEl = parentVirtualRowElement(el);
    var rowId = rowEl && rowEl.getAttribute && rowEl.getAttribute('data-virtual-row');
    if (!rowId || !rowEl.children) return;
    var signatures = [];
    for (var i = 0; i < rowEl.children.length; i++) {
        signatures.push(rowEl.children[i].__virtualRunSignature || '');
    }
    virtualRuntime().virtualRowSignatureCache[rowId] = signatures.join('|');
}

function refreshRuleVirtualSearchSignature(key) {
    if (!hasDOM() || !document.getElementById || typeof ruleRunSignature !== 'function') return;
    var parts = parseIndicatorKeyParts(key, 3);
    if (!parts) return;
    var cell = document.getElementById('rule-' + key);
    if (!cell || !cell.__virtualRunSignature) return;
    cell.__virtualRunSignature = ruleRunSignature({
        key: key,
        si: parts[0],
        gi: parts[1],
        ri: parts[2]
    });
    refreshVirtualRowSignatureCacheForElement(cell);
}

function refreshGroupVirtualSearchSignature(key) {
    if (!hasDOM() || !document.getElementById || typeof groupRunSignature !== 'function') return;
    var parts = parseIndicatorKeyParts(key, 2);
    if (!parts) return;
    var cell = document.getElementById('group-' + key);
    if (!cell || !cell.__virtualRunSignature) return;
    cell.__virtualRunSignature = groupRunSignature({
        key: key,
        si: parts[0],
        gi: parts[1]
    });
    refreshVirtualRowSignatureCacheForElement(cell);
}

function refreshSearchHiddenMatchVirtualSignature(el) {
    var target = searchHiddenIndicatorTargetFromElementId(el && el.id || '');
    if (!target) return;
    if (target.bucket === 'groups') refreshGroupVirtualSearchSignature(target.key);
    if (target.bucket === 'rules') refreshRuleVirtualSearchSignature(target.key);
}

function setSearchHiddenMatchIndicator(el, active, options) {
    if (!el || !el.classList) return false;
    if (!canCommitSearchMarks(
            'collapsed-search-indicator',
            el,
            searchIndicatorCommitDetails(options))) {
        return false;
    }
    active = !!active;
    if (el.classList.contains &&
            el.classList.contains('search-hidden-match') === active) {
        return false;
    }
    el.classList.toggle('search-hidden-match', active);
    refreshSearchHiddenMatchVirtualSignature(el);
    if (!(options && options.deferGeneration)) bumpSearchMarksDomGeneration();
    return true;
}

function setStageSearchHiddenMatchIndicator(key, active, options) {
    if (!document.getElementById) return false;
    var changed = false;
    changed = setSearchHiddenMatchIndicator(
        document.getElementById('stage-col-' + key),
        active,
        options
    ) || changed;
    changed = setSearchHiddenMatchIndicator(
        document.getElementById('stage-exp-' + key),
        active,
        options
    ) || changed;
    return changed;
}

function setGroupSearchHiddenMatchIndicator(key, active, options) {
    if (!document.getElementById) return false;
    return setSearchHiddenMatchIndicator(document.getElementById('group-' + key), active, options);
}

function setRuleSearchHiddenMatchIndicator(key, active, options) {
    if (!document.getElementById) return false;
    return setSearchHiddenMatchIndicator(document.getElementById('rule-' + key), active, options);
}

function searchHiddenIndicatorTargetFromElementId(id) {
    id = id || '';
    if (id.indexOf('stage-col-') === 0) {
        return { bucket: 'stages', key: id.substring('stage-col-'.length) };
    }
    if (id.indexOf('stage-exp-') === 0) {
        return { bucket: 'stages', key: id.substring('stage-exp-'.length) };
    }
    if (id.indexOf('group-') === 0) {
        return { bucket: 'groups', key: id.substring('group-'.length) };
    }
    if (id.indexOf('rule-') === 0) {
        return { bucket: 'rules', key: id.substring('rule-'.length) };
    }
    return null;
}

function searchHiddenIndicatorExpected(indicators, target) {
    return !!(indicators &&
        target &&
        indicators[target.bucket] &&
        indicators[target.bucket][target.key]);
}

function syncMountedSearchHiddenMatchIndicators(indicators, options) {
    if (!hasDOM() || !document.querySelectorAll) return false;
    indicators = indicators || emptyCollapsedSearchIndicators();
    var changed = false;

    function syncElement(el, target, active) {
        if (!el || !el.classList) return;
        active = !!active;
        if (el.classList.contains &&
                el.classList.contains('search-hidden-match') === active) {
            return;
        }
        if (setSearchHiddenMatchIndicator(el, active, {
                deferGeneration: true,
                details: searchIndicatorCommitDetailsWith(options, {
                    bucket: target && target.bucket || '',
                    key: target && target.key || ''
                })
            })) {
            changed = true;
        }
    }

    var stale = document.querySelectorAll('.search-hidden-match');
    for (var i = 0; i < stale.length; i++) {
        var target = searchHiddenIndicatorTargetFromElementId(stale[i].id || '');
        if (!searchHiddenIndicatorExpected(indicators, target)) syncElement(stale[i], target, false);
    }

    if (document.getElementById) {
        var key;
        for (key in indicators.stages) {
            if (!Object.prototype.hasOwnProperty.call(indicators.stages, key)) continue;
            changed = setStageSearchHiddenMatchIndicator(key, true, {
                deferGeneration: true,
                details: searchIndicatorCommitDetailsWith(options, {
                    bucket: 'stages',
                    key: key
                })
            }) || changed;
        }
        for (key in indicators.groups) {
            if (!Object.prototype.hasOwnProperty.call(indicators.groups, key)) continue;
            changed = setGroupSearchHiddenMatchIndicator(key, true, {
                deferGeneration: true,
                details: searchIndicatorCommitDetailsWith(options, {
                    bucket: 'groups',
                    key: key
                })
            }) || changed;
        }
        for (key in indicators.rules) {
            if (!Object.prototype.hasOwnProperty.call(indicators.rules, key)) continue;
            changed = setRuleSearchHiddenMatchIndicator(key, true, {
                deferGeneration: true,
                details: searchIndicatorCommitDetailsWith(options, {
                    bucket: 'rules',
                    key: key
                })
            }) || changed;
        }
    }

    if (changed && !(options && options.deferGeneration)) bumpSearchMarksDomGeneration();
    return changed;
}

function deferredSearchIndicatorSyncOptions(options) {
    var next = {};
    options = options || {};
    for (var key in options) {
        if (Object.prototype.hasOwnProperty.call(options, key)) next[key] = options[key];
    }
    next.deferGeneration = true;
    return next;
}

function syncMountedCollapsedSearchIndicators(indicators, options) {
    var syncOptions = deferredSearchIndicatorSyncOptions(options);
    var changed = false;
    changed = syncMountedSearchHiddenMatchIndicators(indicators, syncOptions) || changed;
    if (changed && !(options && options.deferGeneration)) bumpSearchMarksDomGeneration();
    return changed;
}

function addCollapsedSearchIndicatorOperations(operations, previous, next, bucket, setter) {
    previous = previous || {};
    next = next || {};
    var key;
    for (key in previous) {
        if (Object.prototype.hasOwnProperty.call(previous, key) &&
            !Object.prototype.hasOwnProperty.call(next, key)) {
            operations.push({
                bucket: bucket,
                key: key,
                active: false,
                setter: setter
            });
        }
    }
    for (key in next) {
        if (Object.prototype.hasOwnProperty.call(next, key)) {
            operations.push({
                bucket: bucket,
                key: key,
                active: true,
                setter: setter
            });
        }
    }
}

function collapsedSearchIndicatorChunkSize() {
    return Math.max(1, Math.floor(Number(COLLAPSED_SEARCH_INDICATOR_CHUNK_SIZE)) || 1);
}

function collapsedSearchIndicatorFrameBudgetMs() {
    return Math.max(1, Number(COLLAPSED_SEARCH_INDICATOR_FRAME_BUDGET_MS) || 1);
}

function collapsedSearchIndicatorRetryableCancelReason(reason) {
    return reason === 'stale-search-marks-dom-generation' ||
        reason === 'stale-virtual-rows-dom-generation' ||
        reason === 'stale-context' ||
        reason === 'surface-owned';
}

function scheduleCollapsedSearchIndicatorRetry(job, reason) {
    if (!job || !collapsedSearchIndicatorRetryableCancelReason(reason)) return false;

    var search = searchRuntime();
    search.collapsedSearchIndicatorsDirty = true;
    if (search.collapsedSearchIndicatorRetryFrame) return false;

    search.collapsedSearchIndicatorRetryFrame = scheduleRenderDeferredWork(
        'collapsed-search-indicator-retry',
        function runCollapsedSearchIndicatorRetry() {
            searchRuntime().collapsedSearchIndicatorRetryFrame = null;
            if (!searchRuntime().collapsedSearchIndicatorsDirty) return;
            if (job.searchTransactionToken &&
                    !searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true })) {
                return;
            }
            applyCollapsedSearchIndicators(
                searchRuntime().collapsedSearchIndicators || emptyCollapsedSearchIndicators(),
                {
                    query: job.query || '',
                    scope: job.scope || '',
                    searchTransactionToken: job.searchTransactionToken,
                    allowWithoutSearchTransaction: !!job.allowWithoutSearchTransaction
                }
            );
        },
        {
            token: job.searchTransactionToken
                ? searchTransactionFrameToken(job.searchTransactionToken)
                : renderFrameEpochToken(['trace', 'search']),
            ownerId: job.ownerId || '',
            label: 'collapsed-search-indicator-retry',
            surfaces: ['search-marks'],
            onDiscard: function() {
                searchRuntime().collapsedSearchIndicatorRetryFrame = null;
            }
        }
    );
    return true;
}

function cancelCollapsedSearchIndicatorJob(reason) {
    var search = searchRuntime();
    var job = search.collapsedSearchIndicatorJob;
    if (!job) return false;
    job.cancelled = true;
    job.cancelReason = reason || 'cancelled';
    if (search.collapsedSearchIndicatorJob === job) search.collapsedSearchIndicatorJob = null;
    var frameHandle = job.frameHandle;
    job.frameHandle = null;
    if (frameHandle) cancelRenderFrameWork(frameHandle);
    scheduleCollapsedSearchIndicatorRetry(job, job.cancelReason);
    return true;
}

function collapsedSearchIndicatorJobCurrent(job) {
    if (!job || job.cancelled || searchRuntime().collapsedSearchIndicatorJob !== job) return false;
    if (!searchTransactionTokenCurrent(job.searchTransactionToken, { payload: true })) {
        cancelCollapsedSearchIndicatorJob('stale-search-transaction');
        return false;
    }
    if (!virtualRowsDomGenerationCurrent(job.virtualRowsDomGeneration)) {
        cancelCollapsedSearchIndicatorJob('stale-virtual-rows-dom-generation');
        return false;
    }
    if (!searchMarksDomGenerationCurrent(job.searchMarksDomGeneration)) {
        cancelCollapsedSearchIndicatorJob('stale-search-marks-dom-generation');
        return false;
    }
    return true;
}

function scheduleCollapsedSearchIndicatorChunk(job) {
    job.frameHandle = scheduleRenderDeferredWork(
        'collapsed-search-indicator-chunk',
        function runScheduledCollapsedSearchIndicatorChunk(visualCtx) {
            job.frameHandle = null;
            runCollapsedSearchIndicatorChunk(job);
        },
        {
            token: job.searchTransactionToken
                ? searchTransactionFrameToken(job.searchTransactionToken)
                : renderFrameEpochToken(['trace', 'search']),
            ownerId: job.ownerId || '',
            label: 'collapsed-search-indicator-chunk',
            surfaces: ['search-marks'],
            onDiscard: function(reason) {
                if (searchRuntime().collapsedSearchIndicatorJob === job) {
                    cancelCollapsedSearchIndicatorJob(reason || 'discarded');
                }
            }
        }
    );
}

function runCollapsedSearchIndicatorChunk(job) {
    if (!collapsedSearchIndicatorJobCurrent(job)) return false;

    var startedAt = typeof searchTimingNow === 'function' ? searchTimingNow() : Date.now();
    var limit = collapsedSearchIndicatorChunkSize();
    var budgetMs = collapsedSearchIndicatorFrameBudgetMs();
    var processed = 0;
    var changed = false;

    while (job.index < job.operations.length && processed < limit) {
        var op = job.operations[job.index++];
        processed++;
        var details = {
            query: job.query,
            scope: job.scope,
            bucket: op.bucket,
            key: op.key,
            searchMarksDomGeneration: job.searchMarksDomGeneration,
            virtualRowsDomGeneration: job.virtualRowsDomGeneration,
            searchTransactionToken: job.searchTransactionToken
        };
        if (job.allowWithoutSearchTransaction) {
            details.allowWithoutSearchTransaction = true;
        }
        if (job.ownerId && !searchRuntime().searchMarksTransitionOwnerId) {
            details.ownerId = job.ownerId;
        }
        if (op.setter(op.key, op.active, {
                deferGeneration: true,
                details: details
            })) {
            changed = true;
            job.changedCount++;
        }
        if (processed > 0) {
            var now = typeof searchTimingNow === 'function' ? searchTimingNow() : Date.now();
            if (now - startedAt >= budgetMs) break;
        }
    }

    job.processedCount += processed;
    var finished = job.index >= job.operations.length;
    var syncChanged = false;
    if (finished) {
        var syncDetails = {
            query: job.query,
            scope: job.scope,
            searchMarksDomGeneration: currentSearchMarksDomGeneration(),
            virtualRowsDomGeneration: currentVirtualRowsDomGeneration(),
            searchTransactionToken: job.searchTransactionToken
        };
        if (job.allowWithoutSearchTransaction) {
            syncDetails.allowWithoutSearchTransaction = true;
        }
        if (job.ownerId && !searchRuntime().searchMarksTransitionOwnerId) {
            syncDetails.ownerId = job.ownerId;
        }
        syncChanged = syncMountedCollapsedSearchIndicators(
            searchRuntime().collapsedSearchIndicators,
            {
                deferGeneration: true,
                details: syncDetails
            }
        );
    }
    if (changed || syncChanged) {
        job.searchMarksDomGeneration = bumpSearchMarksDomGeneration();
    }

    if (!finished) {
        scheduleCollapsedSearchIndicatorChunk(job);
    } else if (searchRuntime().collapsedSearchIndicatorJob === job) {
        searchRuntime().collapsedSearchIndicatorJob = null;
    }
    return true;
}

function buildCollapsedSearchIndicatorOperations(previous, next) {
    var operations = [];
    addCollapsedSearchIndicatorOperations(
        operations,
        previous.stages,
        next.stages,
        'stages',
        setStageSearchHiddenMatchIndicator
    );
    addCollapsedSearchIndicatorOperations(
        operations,
        previous.groups,
        next.groups,
        'groups',
        setGroupSearchHiddenMatchIndicator
    );
    addCollapsedSearchIndicatorOperations(
        operations,
        previous.rules,
        next.rules,
        'rules',
        setRuleSearchHiddenMatchIndicator
    );
    return operations;
}

function applyCollapsedSearchIndicators(next, options) {
    if (!hasDOM()) return;
    options = options || {};
    cancelCollapsedSearchIndicatorJob('replaced');
    var transactionToken = Object.prototype.hasOwnProperty.call(options, 'searchTransactionToken')
        ? options.searchTransactionToken
        : activeSearchTransactionToken();
    var commitDetails = {
        query: options.query || currentSearchQuery(),
        scope: options.scope || currentSearchScope(),
        searchTransactionToken: transactionToken
    };
    if (options.allowWithoutSearchTransaction) {
        commitDetails.allowWithoutSearchTransaction = true;
    }
    if (!canCommitSearchMarks('collapsed-search-indicators', null, commitDetails)) return;

    next = next || emptyCollapsedSearchIndicators();
    var search = searchRuntime();
    var previous = search.collapsedSearchIndicators || emptyCollapsedSearchIndicators();
    var operations = buildCollapsedSearchIndicatorOperations(previous, next);

    search.collapsedSearchIndicators = next;
    search.collapsedSearchIndicatorCount = collapsedSearchIndicatorTotal(next);
    search.collapsedSearchIndicatorsDirty = false;
    if (!operations.length) {
        var syncDetails = {
            query: commitDetails.query,
            scope: commitDetails.scope,
            searchMarksDomGeneration: currentSearchMarksDomGeneration(),
            virtualRowsDomGeneration: currentVirtualRowsDomGeneration(),
            searchTransactionToken: transactionToken
        };
        if (options.allowWithoutSearchTransaction) {
            syncDetails.allowWithoutSearchTransaction = true;
        }
        if (transactionToken && transactionToken.ownerId &&
                !searchRuntime().searchMarksTransitionOwnerId) {
            syncDetails.ownerId = transactionToken.ownerId;
        }
        syncMountedCollapsedSearchIndicators(next, {
            details: syncDetails
        });
        return;
    }

    var job = {
        id: search.nextCollapsedSearchIndicatorJobId++,
        query: options.query || currentSearchQuery(),
        scope: options.scope || currentSearchScope(),
        operations: operations,
        index: 0,
        processedCount: 0,
        changedCount: 0,
        searchTransactionToken: transactionToken,
        ownerId: transactionToken && transactionToken.ownerId || '',
        allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction,
        virtualRowsDomGeneration: currentVirtualRowsDomGeneration(),
        searchMarksDomGeneration: currentSearchMarksDomGeneration(),
        frameHandle: null,
        cancelled: false,
        cancelReason: ''
    };
    search.collapsedSearchIndicatorJob = job;
    runCollapsedSearchIndicatorChunk(job);
}

function markCollapsedSearchIndicatorsDirty() {
    searchRuntime().collapsedSearchIndicatorsDirty = true;
}

function searchModeDependsOnCurrentLayout() {
    return true;
}

function refreshSearchMatchesForCurrentLayout(matches, options) {
    options = options || {};
    var previousActive = activeSearchMatch();
    clearPendingSearchActivation();
    searchRuntime().searchMatches = matches || [];

    if (previousActive) {
        searchRuntime().currentSearchMatchIndex = findSearchMatchIndex(previousActive);
        if (searchRuntime().currentSearchMatchIndex < 0) {
            searchRuntime().searchNavigationMatchIndex = -1;
            clearActiveSearchMatch();
        } else {
            searchRuntime().searchNavigationMatchIndex = searchRuntime().currentSearchMatchIndex;
        }
    } else if (searchRuntime().currentSearchMatchIndex >= searchRuntime().searchMatches.length) {
        searchRuntime().currentSearchMatchIndex = -1;
        searchRuntime().searchNavigationMatchIndex = -1;
        clearActiveSearchMatch();
    } else if (searchRuntime().searchNavigationMatchIndex >= searchRuntime().searchMatches.length) {
        searchRuntime().searchNavigationMatchIndex = -1;
    }

    if (!options.deferStatus) updateSearchStatus(searchRuntime().searchMatches.length);
}

function refreshSearchStateForCurrentLayout() {
    if (!hasDOM() || !document.getElementById || !currentUiState().searchActive) return false;

    var query = currentSearchQuery();
    if (!query || !searchModeDependsOnCurrentLayout()) {
        applyCollapsedSearchIndicators(emptyCollapsedSearchIndicators(), {
            allowWithoutSearchTransaction: true
        });
        return false;
    }

    var scope = currentSearchScope();
    var searchState = collectSearchStateForCurrentSearch(query, scope);
    var transactionToken = activeSearchTransactionToken();
    if (transactionToken && !searchTransactionTokenCurrent(transactionToken, { payload: true })) {
        transactionToken = null;
    }
    refreshSearchMatchesForCurrentLayout(searchState.matches, { deferStatus: true });
    scheduleSearchDecoration(query, scope, {
        collapsedIndicators: searchState.collapsedIndicators,
        searchMatches: searchState.matches,
        searchTransactionToken: transactionToken,
        allowWithoutSearchTransaction: !transactionToken,
        updateStatus: true
    });
    return true;
}

function refreshDirtySearchStateForCurrentLayout() {
    if (!searchRuntime().collapsedSearchIndicatorsDirty) return false;
    if (!hasDOM() || !document.getElementById || !currentUiState().searchActive) {
        searchRuntime().collapsedSearchIndicatorsDirty = false;
        return false;
    }
    return refreshSearchStateForCurrentLayout();
}

function updateSearchLabelHighlights(query, scope, options) {
    var measure = typeof measureSearchTiming === 'function'
        ? measureSearchTiming
        : function(label, fn) { return fn(); };
    return measure('label-highlight-update', function() {
        options = options || {};
        cancelSearchLabelHighlightJob('replaced');
        var transactionToken = Object.prototype.hasOwnProperty.call(options, 'searchTransactionToken')
            ? options.searchTransactionToken
            : activeSearchTransactionToken();
        if (!canCommitSearchMarks('search-label-highlights', null, {
            query: query || '',
            scope: scope || '',
            searchTransactionToken: transactionToken,
            allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction
        })) return false;
        var search = searchRuntime();
        var job = {
            id: search.nextSearchLabelHighlightJobId++,
            query: query || '',
            scope: scope || '',
            labelCount: 0,
            index: 0,
            processedCount: 0,
            changedCount: 0,
            timingEpoch: currentSearchTimingEpoch(),
            searchTransactionToken: transactionToken,
            allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction,
            ownerId: transactionToken && transactionToken.ownerId || '',
            virtualRowsDomGeneration: currentVirtualRowsDomGeneration(),
            searchMarksDomGeneration: currentSearchMarksDomGeneration(),
            frameHandle: null,
            cancelled: false,
            cancelReason: ''
        };
        search.searchLabelHighlightJob = job;
        runSearchLabelHighlightChunk(job);
        if (options.skipCollapsedIndicators) {
            return true;
        }
        if (options.collapsedIndicators) {
            applyCollapsedSearchIndicators(options.collapsedIndicators, {
                query: query || '',
                scope: scope || '',
                searchTransactionToken: transactionToken,
                allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction
            });
        } else if (!options.skipCollapsedIndicators) {
            applyCollapsedSearchIndicators(
                collectCollapsedSearchIndicators(query, scope, options.searchMatches),
                {
                    query: query || '',
                    scope: scope || '',
                    searchTransactionToken: transactionToken,
                    allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction
                }
            );
        }
        return true;
    }, { scope: scope || '', queryLength: String(query || '').length });
}

function unwrapSearchMark(mark) {
    if (!mark || !mark.parentNode) return;
    var parent = mark.parentNode;
    parent.replaceChild(document.createTextNode(mark.textContent || ''), mark);
    if (parent.normalize) parent.normalize();
    bumpSearchMarksDomGeneration();
}

function clearStaleSearchMarks(query, scope, options) {
    if (!hasDOM() || !document.querySelectorAll) return;
    options = options || {};
    var transactionToken = Object.prototype.hasOwnProperty.call(options, 'searchTransactionToken')
        ? options.searchTransactionToken
        : activeSearchTransactionToken();
    if (!canCommitSearchMarks('clear-stale-search-marks', null, {
        query: query || '',
        scope: scope || '',
        searchTransactionToken: transactionToken,
        allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction
    })) return;

    var expected = String(query || '').toLowerCase();
    var marks = document.querySelectorAll('mark[data-search-field]');
    for (var i = 0; i < marks.length; i++) {
        var mark = marks[i];
        if (!canCommitSearchMarks('clear-stale-search-mark', mark, {
            query: query || '',
            scope: scope || '',
            searchTransactionToken: transactionToken,
            allowWithoutSearchTransaction: !!options.allowWithoutSearchTransaction
        })) continue;
        var field = mark.getAttribute('data-search-field') || '';
        var text = String(mark.textContent || '').toLowerCase();
        if (!expected || text !== expected || !treeHighlightAllowed(field, scope)) {
            unwrapSearchMark(mark);
        }
    }
}

function treeHighlightAllowed(part, scope) {
    return part === 'label';
}

function treeHighlight(text, query, scope, part, context) {
    return treeHighlightAllowed(part, scope)
        ? htmlHighlight(text, query, part, searchMarkContextAttrs(part, context))
        : htmlEscape(text);
}

function infoHighlight(text, query, scope, context) {
    return htmlEscape(text);
}

function traceRootElement() {
    if (!hasDOM()) return null;
    return document.getElementById('trace-root') ||
        (document.querySelector ? document.querySelector('.trace') : null);
}

function detailBulkRowHtml(feature, label, titleName) {
    return '<div class="detail-bulk-row" data-detail-row="' + feature + '">' +
           '<span class="detail-bulk-label">' + label + '</span>' +
           '<span class="segmented-control detail-bulk-actions">' +
           '<button type="button" data-detail-feature="' + feature + '" data-detail-action="hide" ' +
           'data-trace-action="set-all-rule-feature" ' +
           'title="Hide ' + titleName + ' in all tiles">Hide</button>' +
           '<button type="button" data-detail-feature="' + feature + '" data-detail-action="show" ' +
           'data-trace-action="set-all-rule-feature" ' +
           'title="Show ' + titleName + ' in all tiles">Show</button>' +
           '</span></div>';
}

function emptyStagesRowHtml() {
    return '<div class="detail-bulk-row empty-stages-row" data-empty-stages-row>' +
           '<span class="detail-bulk-label">Empty stages</span>' +
           '<span class="segmented-control detail-bulk-actions">' +
           '<button type="button" data-empty-stages-action="hide" ' +
           'data-trace-action="set-empty-stages-visible" ' +
           'title="Hide empty stages">Hide</button>' +
           '<button type="button" data-empty-stages-action="show" ' +
           'data-trace-action="set-empty-stages-visible" ' +
           'title="Show empty stages">Show</button>' +
           '</span></div>';
}

function pinnedFieldsSectionHtml() {
    return '<div class="settings-section pinned-fields-section" ' +
           'data-settings-section="pinned-fields" hidden>' +
           '<button type="button" class="settings-overflow-row pinned-fields-menu-button" ' +
           'id="pinned-fields-button" data-trace-action="toggle-pinned-fields-menu" ' +
           'aria-expanded="false" aria-controls="pinned-fields-menu">' +
           '<span class="settings-overflow-label">Pinned fields</span>' +
           '<span class="settings-overflow-value" id="pinned-fields-summary">Default</span>' +
           '<span class="settings-overflow-caret" aria-hidden="true"></span></button>' +
           '<div class="pinned-fields-menu" id="pinned-fields-menu" hidden>' +
           '<input type="search" class="pinned-fields-filter" id="pinned-fields-filter" ' +
           'data-trace-action="pinned-fields-filter" placeholder="Filter fields" ' +
           'autocomplete="off" spellcheck="false" aria-label="Filter fields" hidden>' +
           '<div class="pinned-fields-list" id="pinned-fields-list"></div>' +
           '<div class="pinned-fields-actions" hidden></div></div></div>';
}

function diffFieldsSectionHtml() {
    return '<div class="settings-section diff-fields-section" ' +
           'data-settings-section="diff-fields" hidden>' +
           '<button type="button" class="settings-overflow-row diff-fields-menu-button" ' +
           'id="diff-fields-button" data-trace-action="toggle-diff-fields-menu" ' +
           'aria-expanded="false" aria-controls="diff-fields-menu">' +
           '<span class="settings-overflow-label">Diff fields</span>' +
           '<span class="settings-overflow-value" id="diff-fields-summary">None</span>' +
           '<span class="settings-overflow-caret" aria-hidden="true"></span></button>' +
           '<div class="diff-fields-menu" id="diff-fields-menu" hidden>' +
           '<input type="search" class="diff-fields-filter" id="diff-fields-filter" ' +
           'data-trace-action="diff-fields-filter" placeholder="Filter fields" ' +
           'autocomplete="off" spellcheck="false" aria-label="Filter fields" hidden>' +
           '<div class="diff-fields-list" id="diff-fields-list"></div>' +
           '<div class="diff-fields-actions" hidden></div></div></div>';
}

function optimizerTraceAppShellHtml() {
    return '<div class="top-bar"><div class="controls">' +
           '<div class="left-controls">' +
           '<h1 class="trace-title" id="trace-title">RBO Trace</h1>' +
           '<span class="trace-picker-wrap" id="trace-picker-wrap" hidden>' +
           '<button type="button" class="trace-picker" id="trace-picker-button" ' +
           'data-trace-action="toggle-trace-picker-menu" aria-expanded="false" ' +
           'aria-controls="trace-picker-menu" hidden></button>' +
           '<div class="trace-picker-menu" id="trace-picker-menu" hidden></div>' +
           '</span></div>' +
           '<div class="center-controls">' +
           '<button class="ctrl-btn cycle-btn" id="cycle-button" ' +
           'data-trace-action="cycle-global-state" ' +
           'title="Expand Groups" aria-label="Expand Groups">' +
           '<span class="cycle-measure" aria-hidden="true">' +
           '<span>Collapse All</span><span>Expand Stages</span>' +
           '<span>Expand Groups</span><span>Expand Rules</span>' +
           '<span>Expand All</span></span>' +
           '<span class="cycle-label">Expand Groups</span>' +
           '<span class="cycle-number" aria-hidden="true">2</span>' +
           '</button>' +
           '<button class="search-icon-btn" id="search-icon-button" ' +
           'data-trace-action="open-mobile-search" title="Search" ' +
           'aria-label="Open search">' + traceIconSvg('search') + '</button>' +
           '<div class="search-wrap">' +
           '<input class="search-box" id="search-box" type="search" ' +
           'placeholder="Search plans..." autocomplete="off" aria-label="Search plans" ' +
           'data-trace-action="search-box">' +
           '<span class="search-info" id="search-info" aria-live="polite" aria-atomic="true"></span>' +
           '<span class="search-action-group">' +
           '<button class="search-nav-btn" id="search-prev-button" ' +
           'data-trace-action="search-nav" data-direction="-1" ' +
           'title="Previous match" aria-label="Previous match" disabled>' +
           traceIconSvg('arrow-up') + '</button>' +
           '<button class="search-nav-btn" id="search-next-button" ' +
           'data-trace-action="search-nav" data-direction="1" ' +
           'title="Next match" aria-label="Next match" disabled>' +
           traceIconSvg('arrow-down') + '</button>' +
           '<button class="search-nav-btn search-expand-btn" id="search-expand-button" ' +
           'data-trace-action="expand-search-matches" ' +
           'title="Expand matches" aria-label="Expand matches" disabled>' +
           traceIconSvg('stack') + '</button>' +
           '<button class="search-nav-btn search-global-btn" id="search-global-button" ' +
           'data-trace-action="global-search" ' +
           'title="Search all matches" aria-label="Search all matches" disabled>' +
           traceIconSvg('globe') + '</button>' +
           '<button class="search-clear-btn" id="search-clear-button" ' +
           'data-trace-action="clear-search-input" title="Clear search" ' +
           'aria-label="Clear search" disabled>' +
           traceIconSvg('cross') + '</button>' +
           '</span>' +
           '</div>' +
           '<button class="search-close-btn" id="search-close-button" ' +
           'data-trace-action="close-mobile-search" title="Close search" ' +
           'aria-label="Close search">&larr;</button>' +
           '</div>' +
           '<div class="right-controls">' +
           '<button class="ctrl-btn diff-reset" id="diff-button" ' +
           'data-trace-action="on-diff-button-click" title="Clear A/B selection" hidden>' +
           '<span class="diff-measure" aria-hidden="true">' +
           '<span>Clear AB</span><span>Close delta</span></span>' +
           '<span class="diff-label">Clear AB</span>' +
           '</button>' +
           '<div class="settings-wrap">' +
           '<button class="ctrl-btn icon-btn" id="settings-button" ' +
           'data-trace-action="toggle-settings-popover" title="Settings" aria-label="Settings">' +
           traceIconSvg('mixer-horizontal') + '</button>' +
           '<div class="settings-popover" id="settings-popover" hidden>' +
           '<div class="settings-section" data-settings-section="details">' +
           '<div class="settings-menu-title">Details</div>' +
           emptyStagesRowHtml() +
           detailBulkRowHtml('fields', 'Fields', 'fields') +
           detailBulkRowHtml('pinned', 'Pinned fields', 'pinned fields') +
           detailBulkRowHtml('info', 'Information', 'information') +
           '</div>' +
           pinnedFieldsSectionHtml() +
           diffFieldsSectionHtml() +
           '<div class="settings-section">' +
           '<div class="settings-menu-title">Appearance</div>' +
           '<div class="appearance-row">' +
           '<label class="appearance-label" for="theme-select">Theme</label>' +
           '<select class="theme-select-native" id="theme-select" data-trace-action="theme-select">' +
           '<option value="stone" selected>Stone</option>' +
           '<option value="slate">Slate</option>' +
           '<option value="monokai">Monokai</option>' +
           '<option value="modus">Modus</option>' +
           '</select></div>' +
           '<div class="appearance-row">' +
           '<span class="appearance-label">Mode</span>' +
           '<span class="segmented-control theme-mode-switch" role="group" aria-label="Theme mode">' +
           '<button type="button" id="theme-light-button" ' +
           'data-trace-action="set-theme-dark-mode" data-dark-mode="false">Light</button>' +
           '<button type="button" id="theme-dark-button" ' +
           'data-trace-action="set-theme-dark-mode" data-dark-mode="true">Dark</button>' +
           '</span></div></div></div></div></div></div></div>' +
           '<div class="trace-viewport"><div class="trace" id="trace-root"></div>' +
           traceScrollbarHtml() +
           '</div>';
}

function renderOptimizerTraceAppShell() {
    if (!hasDOM() || !document.getElementById) return;
    if (document.getElementById('trace-root')) return;

    var mount = document.getElementById('optimizer-trace-app');
    if (mount) {
        mount.innerHTML = optimizerTraceAppShellHtml();
        return;
    }

    if (document.body && document.body.insertAdjacentHTML) {
        document.body.insertAdjacentHTML('afterbegin', optimizerTraceAppShellHtml());
    }
}

function traceScrollbarViewportElement(traceEl) {
    if (!traceEl) return null;
    var parent = traceEl.parentElement;
    if (parent && parent.classList && parent.classList.contains('trace-viewport')) {
        return parent;
    }
    return traceEl;
}

var TRACE_SCROLLBAR_MIN_THUMB_PX = 36;
var TRACE_SCROLLBAR_IDLE_MS = 760;
var TRACE_SCROLLBAR_KEY_STEP_PX = 72;
var TRACE_SCROLLBAR_PAGE_RATIO = 0.88;
var TRACE_SCROLLBAR_TRACK_FRAME_MS = 1000 / 60;
var TRACE_SCROLLBAR_TRACK_SMOOTH_PAGE_MS = 140;
var TRACE_SCROLLBAR_TRACK_TARGET_MS = 260;
var traceScrollbarActivityTimer = 0;
var traceScrollbarActiveTraceEl = null;
var traceScrollbarLastActiveTime = 0;
var traceScrollbarDrag = null;
var traceScrollbarElementCache = null;
var traceScrollbarGeometry = null;
var traceScrollbarFrame = 0;
var traceScrollbarPendingActive = false;
var traceScrollbarPress = null;
var traceScrollbarTrackPress = null;
var traceScrollbarSurface = createVisualSurface({
    name: 'trace-scrollbar',
    epochScopes: ['trace', 'virtual'],
    allowMissingTarget: true,
    resolveTarget: function(ref, options) {
        options = options || {};
        if (options.target) return options.target;
        var elements = options.elements || traceScrollbarElementCache || {};
        if (options.part === 'thumb') return elements.thumb || null;
        if (options.part === 'trace') return elements.traceEl || null;
        if (options.part === 'viewport') return elements.viewport || null;
        return elements.bar || null;
    }
});

function commitTraceScrollbarSurface(visualCtx, options, writer) {
    return traceScrollbarSurface.commit(visualCtx, options || {}, writer);
}

function runTraceScrollbarSurfaceCommitNow(label, callback) {
    return runVisualSurfaceCommitNow(
        label,
        ['trace-scrollbar'],
        ['trace', 'virtual'],
        callback
    );
}

function traceScrollbarHtml() {
    return '<div class="trace-scrollbar" role="scrollbar" tabindex="0" hidden ' +
           'aria-label="Horizontal trace scroll" aria-orientation="horizontal" ' +
           'aria-controls="trace-root" aria-valuemin="0" aria-valuemax="0" aria-valuenow="0">' +
           '<div class="trace-scrollbar-thumb"></div>' +
           '</div>';
}

function ensureTraceScrollbarElement(viewport, visualCtx) {
    if (!viewport || !viewport.querySelector) return null;

    var bar = viewport.querySelector('.trace-scrollbar');
    if (!bar && viewport.insertAdjacentHTML) {
        if (!visualCommitContextIsBranded(visualCtx)) {
            return runTraceScrollbarSurfaceCommitNow('ensure-trace-scrollbar', function(ctx) {
                return ensureTraceScrollbarElement(viewport, ctx);
            });
        }
        commitTraceScrollbarSurface(visualCtx, {
            label: 'ensure-trace-scrollbar',
            target: viewport,
            part: 'viewport'
        }, function() {
            viewport.insertAdjacentHTML('beforeend', traceScrollbarHtml());
            bar = viewport.querySelector('.trace-scrollbar');
        });
    }
    return bar;
}

function setTraceScrollbarActive(traceEl, visualCtx) {
    if (!traceEl || !traceEl.classList || !hasDOM()) return;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceScrollbarSurfaceCommitNow('trace-scrollbar-active', function(ctx) {
            return setTraceScrollbarActive(traceEl, ctx);
        });
    }

    return commitTraceScrollbarSurface(visualCtx, {
        label: 'trace-scrollbar-active',
        target: traceEl,
        part: 'trace'
    }, function() {
        if (traceScrollbarActiveTraceEl && traceScrollbarActiveTraceEl !== traceEl) {
            traceScrollbarActiveTraceEl.classList.remove('trace-scrollbar-active');
            if (traceScrollbarActivityTimer) {
                clearTimeout(traceScrollbarActivityTimer);
                traceScrollbarActivityTimer = 0;
            }
        }
        traceScrollbarActiveTraceEl = traceEl;
        traceScrollbarLastActiveTime = traceScrollbarNow();
        traceEl.classList.add('trace-scrollbar-active');
        if (traceScrollbarActivityTimer) return;
        scheduleTraceScrollbarInactive(traceEl);
    });
}

function traceScrollbarNow() {
    if (typeof performance !== 'undefined' && performance && performance.now) {
        return performance.now();
    }
    return Date.now ? Date.now() : 0;
}

function clearTraceScrollbarActive(traceEl, visualCtx) {
    if (!traceEl || !traceEl.classList) return false;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceScrollbarSurfaceCommitNow('trace-scrollbar-inactive', function(ctx) {
            return clearTraceScrollbarActive(traceEl, ctx);
        });
    }

    return commitTraceScrollbarSurface(visualCtx, {
        label: 'trace-scrollbar-inactive',
        target: traceEl,
        part: 'trace'
    }, function(target) {
        target.classList.remove('trace-scrollbar-active');
        if (traceScrollbarActiveTraceEl === target) traceScrollbarActiveTraceEl = null;
        traceScrollbarActivityTimer = 0;
    });
}

function scheduleTraceScrollbarInactive(traceEl, delay) {
    delay = delay === undefined ? TRACE_SCROLLBAR_IDLE_MS : Math.max(0, delay);
    traceScrollbarActivityTimer = setTimeout(function() {
        var remaining = TRACE_SCROLLBAR_IDLE_MS - (traceScrollbarNow() - traceScrollbarLastActiveTime);
        if (remaining > 0) {
            traceScrollbarActivityTimer = 0;
            scheduleTraceScrollbarInactive(traceEl, remaining);
            return;
        }
        clearTraceScrollbarActive(traceEl);
    }, delay);
}

function traceScrollbarStopEvent(ev) {
    if (ev && ev.stopPropagation) ev.stopPropagation();
    if (ev && ev.preventDefault) ev.preventDefault();
}

function traceScrollbarPressPointerMatches(ev) {
    return !traceScrollbarPress ||
        traceScrollbarPress.pointerId === null ||
        !ev ||
        ev.pointerId === undefined ||
        ev.pointerId === traceScrollbarPress.pointerId;
}

function startTraceScrollbarPress(traceEl, ev, visualCtx) {
    if (!traceEl || !traceEl.classList) return;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceScrollbarSurfaceCommitNow('trace-scrollbar-press-start', function(ctx) {
            return startTraceScrollbarPress(traceEl, ev, ctx);
        });
    }

    endTraceScrollbarPress(null, visualCtx);
    traceScrollbarPress = {
        pointerId: ev && ev.pointerId !== undefined ? ev.pointerId : null,
        traceEl: traceEl
    };
    commitTraceScrollbarSurface(visualCtx, {
        label: 'trace-scrollbar-press-start',
        target: traceEl,
        part: 'trace'
    }, function(target) {
        target.classList.add('trace-scrollbar-pressing');
    });
    if (hasDOM() && document.addEventListener) {
        document.addEventListener('pointerup', endTraceScrollbarPress, true);
        document.addEventListener('pointercancel', endTraceScrollbarPress, true);
    }
}

function endTraceScrollbarPress(ev, visualCtx) {
    if (!traceScrollbarPress || !traceScrollbarPressPointerMatches(ev)) return;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceScrollbarSurfaceCommitNow('trace-scrollbar-press-end', function(ctx) {
            return endTraceScrollbarPress(ev, ctx);
        });
    }

    var press = traceScrollbarPress;
    traceScrollbarPress = null;
    if (press.traceEl && press.traceEl.classList) {
        commitTraceScrollbarSurface(visualCtx, {
            label: 'trace-scrollbar-press-end',
            target: press.traceEl,
            part: 'trace'
        }, function(target) {
            target.classList.remove('trace-scrollbar-pressing');
        });
    }
    if (hasDOM() && document.removeEventListener) {
        document.removeEventListener('pointerup', endTraceScrollbarPress, true);
        document.removeEventListener('pointercancel', endTraceScrollbarPress, true);
    }
}

function setTraceScrollbarDragging(traceEl, active, visualCtx) {
    if (!traceEl || !traceEl.classList) return false;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceScrollbarSurfaceCommitNow('trace-scrollbar-dragging', function(ctx) {
            return setTraceScrollbarDragging(traceEl, active, ctx);
        });
    }

    return commitTraceScrollbarSurface(visualCtx, {
        label: 'trace-scrollbar-dragging',
        target: traceEl,
        part: 'trace',
        active: !!active
    }, function(target) {
        target.classList.toggle('trace-scrollbar-dragging', !!active);
    });
}

function traceScrollbarMetrics(traceEl, bar) {
    var clientWidth = traceEl && traceEl.clientWidth ? traceEl.clientWidth : 0;
    var scrollWidth = traceEl && traceEl.scrollWidth ? traceEl.scrollWidth : 0;
    var maxScroll = Math.max(0, scrollWidth - clientWidth);
    var scrollable = maxScroll > 1;
    var scrollLeft = Math.max(0, Math.min(maxScroll, traceEl ? (traceEl.scrollLeft || 0) : 0));
    var trackWidth = bar && bar.clientWidth ? bar.clientWidth : clientWidth;
    var thumbWidth = 0;

    if (scrollable && trackWidth > 0 && scrollWidth > 0) {
        thumbWidth = Math.min(
            trackWidth,
            Math.max(TRACE_SCROLLBAR_MIN_THUMB_PX, trackWidth * clientWidth / scrollWidth)
        );
    }

    var thumbTravel = Math.max(0, trackWidth - thumbWidth);
    var thumbLeft = maxScroll && thumbTravel ? thumbTravel * scrollLeft / maxScroll : 0;
    return {
        clientWidth: clientWidth,
        maxScroll: maxScroll,
        scrollable: scrollable,
        scrollLeft: scrollLeft,
        scrollWidth: scrollWidth,
        thumbLeft: thumbLeft,
        thumbTravel: thumbTravel,
        thumbWidth: thumbWidth,
        trackWidth: trackWidth
    };
}

function clearTraceScrollbarCache() {
    endTraceScrollbarTrackPress(null);
    endTraceScrollbarPress(null);
    traceScrollbarElementCache = null;
    invalidateTraceScrollbarGeometry();
}

function invalidateTraceScrollbarGeometry() {
    traceScrollbarGeometry = null;
}

function cancelTraceScrollbarFrame() {
    if (!traceScrollbarFrame) return;

    if (!cancelRenderFrameWork(traceScrollbarFrame)) {
        renderFrameCancel(traceScrollbarFrame);
    } else if (!hasRenderFrameWork('deferred') && frameRuntime().deferredFrameId !== null) {
        renderFrameCancel(frameRuntime().deferredFrameId);
        frameRuntime().deferredFrameId = null;
    }
    traceScrollbarFrame = 0;
    traceScrollbarPendingActive = false;
}

function traceScrollbarElements(visualCtx) {
    if (!hasDOM()) return;

    var traceEl = traceRootElement();
    var viewport = traceScrollbarViewportElement(traceEl);
    if (!traceEl || !viewport || !viewport.querySelector) return;

    if (traceScrollbarElementCache &&
        traceScrollbarElementCache.traceEl === traceEl &&
        traceScrollbarElementCache.viewport === viewport &&
        traceScrollbarElementCache.bar &&
        traceScrollbarElementCache.thumb) {
        return traceScrollbarElementCache;
    }

    var bar = ensureTraceScrollbarElement(viewport, visualCtx);
    if (!bar) return;

    var thumb = bar.querySelector('.trace-scrollbar-thumb');
    if (!thumb) return;

    traceScrollbarElementCache = {
        bar: bar,
        thumb: thumb,
        traceEl: traceEl,
        viewport: viewport
    };
    return traceScrollbarElementCache;
}

function traceScrollbarGeometryValid(elements) {
    return !!(traceScrollbarGeometry &&
        elements &&
        traceScrollbarGeometry.traceEl === elements.traceEl &&
        traceScrollbarGeometry.viewport === elements.viewport &&
        traceScrollbarGeometry.bar === elements.bar &&
        traceScrollbarGeometry.thumb === elements.thumb);
}

function traceScrollbarScrollableGeometry(elements, visualCtx) {
    if (!traceScrollbarGeometryValid(elements)) {
        refreshTraceScrollbarGeometry(false, visualCtx);
    }
    return traceScrollbarGeometryValid(elements) ? traceScrollbarGeometry : null;
}

function refreshTraceScrollbarGeometry(active, visualCtx) {
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceScrollbarSurfaceCommitNow('trace-scrollbar-geometry', function(ctx) {
            return refreshTraceScrollbarGeometry(active, ctx);
        });
    }

    var elements = traceScrollbarElements(visualCtx);
    if (!elements) return;

    var traceEl = elements.traceEl;
    var bar = elements.bar;
    var thumb = elements.thumb;
    var metrics = traceScrollbarMetrics(traceEl, bar);

    metrics.bar = bar;
    metrics.thumb = thumb;
    metrics.traceEl = traceEl;
    metrics.viewport = elements.viewport;

    return commitTraceScrollbarSurface(visualCtx, {
        label: 'trace-scrollbar-geometry',
        target: bar,
        elements: elements
    }, function() {
        traceScrollbarGeometry = metrics;
        bar.hidden = !metrics.scrollable;
        if (bar.setAttribute) {
            bar.setAttribute('aria-valuemax', String(Math.round(metrics.maxScroll)));
            bar.setAttribute('aria-valuenow', String(Math.round(metrics.scrollLeft)));
            bar.setAttribute('aria-hidden', metrics.scrollable ? 'false' : 'true');
        }
        traceEl.classList.toggle('trace-scrollbar-visible', metrics.scrollable);
        if (!metrics.scrollable) {
            clearTraceScrollbarActive(traceEl, visualCtx);
            setTraceScrollbarDragging(traceEl, false, visualCtx);
            if (traceScrollbarActivityTimer) {
                clearTimeout(traceScrollbarActivityTimer);
                traceScrollbarActivityTimer = 0;
            }
            endTraceScrollbarPress(null, visualCtx);
            if (traceScrollbarActiveTraceEl === traceEl) traceScrollbarActiveTraceEl = null;
            return;
        }

        thumb.style.width = metrics.thumbWidth + 'px';
        applyTraceScrollbarPosition(elements, metrics, active, visualCtx);
    });
}

function syncTraceScrollbarGeometry(active) {
    cancelTraceScrollbarFrame();
    refreshTraceScrollbarGeometry(active);
}

function applyTraceScrollbarPosition(elements, geometry, active, visualCtx) {
    if (!elements || !geometry || !geometry.scrollable) return;
    if (!visualCommitContextIsBranded(visualCtx)) {
        return runTraceScrollbarSurfaceCommitNow('trace-scrollbar-position', function(ctx) {
            return applyTraceScrollbarPosition(elements, geometry, active, ctx);
        });
    }

    var traceEl = elements.traceEl;
    var scrollLeft = Math.max(0, Math.min(geometry.maxScroll, traceEl.scrollLeft || 0));
    var thumbLeft = geometry.maxScroll && geometry.thumbTravel
        ? geometry.thumbTravel * scrollLeft / geometry.maxScroll
        : 0;

    return commitTraceScrollbarSurface(visualCtx, {
        label: 'trace-scrollbar-position',
        target: elements.thumb,
        elements: elements,
        part: 'thumb'
    }, function() {
        elements.thumb.style.transform = 'translate3d(' + thumbLeft + 'px, 0, 0)';
        if (elements.bar.setAttribute) {
            elements.bar.setAttribute('aria-valuenow', String(Math.round(scrollLeft)));
        }

        if (active) setTraceScrollbarActive(traceEl, visualCtx);
    });
}

function flushTraceScrollbarPosition(visualCtx) {
    traceScrollbarFrame = 0;

    var active = traceScrollbarPendingActive;
    traceScrollbarPendingActive = false;

    var elements = traceScrollbarElements(visualCtx);
    if (!elements) return;

    var geometry = traceScrollbarScrollableGeometry(elements, visualCtx);
    if (!geometry || !geometry.scrollable) return;

    applyTraceScrollbarPosition(elements, geometry, active, visualCtx);
}

function updateTraceScrollbar(active) {
    var elements = traceScrollbarElements();
    if (!elements) return;

    if (traceScrollbarGeometryValid(elements)) {
        if (active) traceScrollbarPendingActive = false;
        applyTraceScrollbarPosition(elements, traceScrollbarGeometry, active);
        return;
    }

    if (active) traceScrollbarPendingActive = true;
    if (traceScrollbarFrame) return;

    traceScrollbarFrame = scheduleRenderDeferredWork(
        'trace-scrollbar-position',
        flushTraceScrollbarPosition,
        {
            epochScopes: ['trace', 'virtual'],
            label: 'trace-scrollbar-position',
            surfaces: ['trace-scrollbar'],
            withVisualContext: true,
            onDiscard: function() {
                traceScrollbarFrame = 0;
                traceScrollbarPendingActive = false;
            }
        }
    );
}

function setTraceScrollbarScrollLeft(traceEl, scrollLeft) {
    if (!traceEl) return false;

    var maxScroll = Math.max(0, traceScrollWidthValue(traceEl) - (traceEl.clientWidth || 0));
    return setTraceScrollLeft(traceEl, scrollLeft, {
        maxScrollLeft: maxScroll,
        runVirtualScrollHandler: true
    });
}

function traceScrollbarTargetScrollForTrackClick(metrics, trackX) {
    if (!metrics || !metrics.thumbTravel) return 0;

    var targetThumbLeft = trackX - metrics.thumbWidth / 2;
    targetThumbLeft = Math.max(0, Math.min(metrics.thumbTravel, targetThumbLeft));
    return Math.max(0, Math.min(
        metrics.maxScroll,
        targetThumbLeft * metrics.maxScroll / metrics.thumbTravel
    ));
}

function traceScrollbarTrackPressPointerMatches(ev) {
    return !traceScrollbarTrackPress ||
        traceScrollbarTrackPress.pointerId === null ||
        !ev ||
        ev.pointerId === undefined ||
        ev.pointerId === traceScrollbarTrackPress.pointerId;
}

function traceScrollbarStepTowardTrackPress(press, stepScroll) {
    if (!press || !press.traceEl) return true;

    var current = Math.max(0, Math.min(press.maxScroll, press.traceEl.scrollLeft || 0));
    var reached = press.direction > 0
        ? current >= press.targetScrollLeft - 0.01
        : current <= press.targetScrollLeft + 0.01;
    if (reached || stepScroll <= 0) return true;

    var next = press.direction > 0
        ? Math.min(press.targetScrollLeft, current + stepScroll)
        : Math.max(press.targetScrollLeft, current - stepScroll);
    var changed = setTraceScrollbarScrollLeft(press.traceEl, next);
    setTraceScrollbarActive(press.traceEl);
    return !changed || Math.abs(next - press.targetScrollLeft) <= 0.01;
}

function scheduleTraceScrollbarTrackFrame(press) {
    if (!press || press.frame) return;

    var frame = (typeof window !== 'undefined' && window.requestAnimationFrame) ||
        function(fn) { return setTimeout(fn, TRACE_SCROLLBAR_TRACK_FRAME_MS); };

    press.frame = frame(function(now) {
        press.frame = 0;
        if (traceScrollbarTrackPress !== press) return;

        now = Number(now);
        if (!isFinite(now) || now <= 0) now = traceScrollbarNow();
        var elapsed = press.lastFrameTime
            ? now - press.lastFrameTime
            : TRACE_SCROLLBAR_TRACK_FRAME_MS;
        press.lastFrameTime = now;
        elapsed = Math.max(1, Math.min(50, elapsed));

        var pageStep = press.pageScroll * elapsed / TRACE_SCROLLBAR_TRACK_SMOOTH_PAGE_MS;
        var targetStep = press.distance > 0
            ? press.distance * elapsed / TRACE_SCROLLBAR_TRACK_TARGET_MS
            : 0;
        var step = Math.max(pageStep, targetStep);
        if (traceScrollbarStepTowardTrackPress(press, step)) return;
        scheduleTraceScrollbarTrackFrame(press);
    });
}

function startTraceScrollbarTrackPress(elements, metrics, trackX, direction, ev) {
    if (!elements || !metrics) return;

    var traceEl = elements.traceEl;
    var bar = elements.bar;
    var targetScrollLeft = traceScrollbarTargetScrollForTrackClick(metrics, trackX);
    var currentScrollLeft = Math.max(0, Math.min(metrics.maxScroll, traceEl.scrollLeft || 0));
    var press = {
        bar: bar,
        distance: Math.abs(targetScrollLeft - currentScrollLeft),
        direction: direction,
        frame: 0,
        lastFrameTime: 0,
        maxScroll: metrics.maxScroll,
        pageScroll: metrics.clientWidth * TRACE_SCROLLBAR_PAGE_RATIO,
        pointerId: ev && ev.pointerId !== undefined ? ev.pointerId : null,
        targetScrollLeft: targetScrollLeft,
        traceEl: traceEl
    };

    traceScrollbarTrackPress = press;
    if (bar && bar.setPointerCapture && ev && ev.pointerId !== undefined) {
        try {
            bar.setPointerCapture(ev.pointerId);
        } catch (err) {}
    }
    if (hasDOM() && document.addEventListener) {
        document.addEventListener('pointerup', endTraceScrollbarTrackPress, true);
        document.addEventListener('pointercancel', endTraceScrollbarTrackPress, true);
    }

    scheduleTraceScrollbarTrackFrame(press);
}

function endTraceScrollbarTrackPress(ev) {
    if (!traceScrollbarTrackPress || !traceScrollbarTrackPressPointerMatches(ev)) return;

    var press = traceScrollbarTrackPress;
    traceScrollbarTrackPress = null;
    if (press.frame) {
        var cancelFrame = (typeof window !== 'undefined' && window.cancelAnimationFrame) ||
            clearTimeout;
        cancelFrame(press.frame);
        press.frame = 0;
    }
    var pointerId = ev && ev.pointerId !== undefined ? ev.pointerId : press.pointerId;
    if (press.bar && press.bar.releasePointerCapture && pointerId !== null) {
        try {
            press.bar.releasePointerCapture(pointerId);
        } catch (err) {}
    }
    if (hasDOM() && document.removeEventListener) {
        document.removeEventListener('pointerup', endTraceScrollbarTrackPress, true);
        document.removeEventListener('pointercancel', endTraceScrollbarTrackPress, true);
    }
    endTraceScrollbarPress(ev);
    if (ev) traceScrollbarStopEvent(ev);
}

function onTraceScrollbarPointerDown(ev) {
    if (ev && ev.button !== undefined && ev.button !== 0) return;
    if (!hasDOM()) return;

    var elements = traceScrollbarElements();
    if (!elements) return;

    var traceEl = elements.traceEl;
    var bar = elements.bar;
    if (!bar || bar.hidden) return;

    invalidateTraceScrollbarGeometry();
    var thumb = elements.thumb;
    var metrics = traceScrollbarScrollableGeometry(elements);
    if (!metrics || !metrics.scrollable) return;

    var eventTarget = ev ? ev.target : null;
    var onThumb = !!(thumb && (eventTarget === thumb ||
        (eventTarget && eventTarget.closest && eventTarget.closest('.trace-scrollbar-thumb'))));
    var clientX = ev && typeof ev.clientX === 'number' ? ev.clientX : 0;
    var currentScrollLeft = Math.max(0, Math.min(metrics.maxScroll, traceEl.scrollLeft || 0));
    var currentThumbLeft = metrics.maxScroll && metrics.thumbTravel
        ? metrics.thumbTravel * currentScrollLeft / metrics.maxScroll
        : 0;

    endTraceScrollbarTrackPress(null);
    traceScrollbarStopEvent(ev);
    setTraceScrollbarActive(traceEl);
    startTraceScrollbarPress(traceEl, ev);
    if (!onThumb) {
        var rect = bar.getBoundingClientRect ? bar.getBoundingClientRect() : { left: 0 };
        var trackX = clientX - rect.left;
        if (trackX < currentThumbLeft) {
            startTraceScrollbarTrackPress(elements, metrics, trackX, -1, ev);
        } else if (trackX > currentThumbLeft + metrics.thumbWidth) {
            startTraceScrollbarTrackPress(elements, metrics, trackX, 1, ev);
        }
        return;
    }

    applyTraceScrollbarPosition(elements, metrics, false);
    var trackRect = bar.getBoundingClientRect ? bar.getBoundingClientRect() : { left: 0 };
    var grabOffset = Math.max(0, Math.min(
        metrics.thumbWidth,
        clientX - trackRect.left - currentThumbLeft
    ));

    traceScrollbarDrag = {
        bar: bar,
        grabOffset: grabOffset,
        maxScroll: metrics.maxScroll,
        pointerId: ev && ev.pointerId !== undefined ? ev.pointerId : null,
        startScrollLeft: currentScrollLeft,
        startThumbLeft: currentThumbLeft,
        thumbTravel: metrics.thumbTravel,
        trackLeft: trackRect.left,
        traceEl: traceEl
    };
    setTraceScrollbarDragging(traceEl, true);

    if (bar.setPointerCapture && ev && ev.pointerId !== undefined) {
        try {
            bar.setPointerCapture(ev.pointerId);
        } catch (err) {
            // Pointer capture is an enhancement; document listeners still keep drag usable.
        }
    }
    if (document.addEventListener) {
        document.addEventListener('pointermove', onTraceScrollbarPointerMove, true);
        document.addEventListener('pointerup', endTraceScrollbarDrag, true);
        document.addEventListener('pointercancel', endTraceScrollbarDrag, true);
    }
}

function traceScrollbarPointerMatches(ev) {
    return !traceScrollbarDrag ||
        traceScrollbarDrag.pointerId === null ||
        !ev ||
        ev.pointerId === undefined ||
        ev.pointerId === traceScrollbarDrag.pointerId;
}

function onTraceScrollbarPointerMove(ev) {
    if (!traceScrollbarDrag || !traceScrollbarPointerMatches(ev)) return;

    var clientX = ev && typeof ev.clientX === 'number'
        ? ev.clientX
        : traceScrollbarDrag.trackLeft + traceScrollbarDrag.startThumbLeft + traceScrollbarDrag.grabOffset;
    var thumbLeft = Math.max(0, Math.min(
        traceScrollbarDrag.thumbTravel,
        clientX - traceScrollbarDrag.trackLeft - traceScrollbarDrag.grabOffset
    ));
    var next = traceScrollbarDrag.thumbTravel
        ? thumbLeft * traceScrollbarDrag.maxScroll / traceScrollbarDrag.thumbTravel
        : 0;
    setTraceScrollbarScrollLeft(traceScrollbarDrag.traceEl, next);
    traceScrollbarStopEvent(ev);
}

function endTraceScrollbarDrag(ev) {
    if (!traceScrollbarDrag || !traceScrollbarPointerMatches(ev)) return;

    var drag = traceScrollbarDrag;
    traceScrollbarDrag = null;
    if (drag.traceEl && drag.traceEl.classList) {
        setTraceScrollbarDragging(drag.traceEl, false);
    }
    endTraceScrollbarPress(ev);
    if (drag.bar && drag.bar.releasePointerCapture && ev && ev.pointerId !== undefined) {
        try {
            drag.bar.releasePointerCapture(ev.pointerId);
        } catch (err) {}
    }
    if (hasDOM() && document.removeEventListener) {
        document.removeEventListener('pointermove', onTraceScrollbarPointerMove, true);
        document.removeEventListener('pointerup', endTraceScrollbarDrag, true);
        document.removeEventListener('pointercancel', endTraceScrollbarDrag, true);
    }
    updateTraceScrollbar(true);
    traceScrollbarStopEvent(ev);
}

function onTraceScrollbarKeyDown(ev) {
    var elements = traceScrollbarElements();
    if (!elements) return;

    var traceEl = elements.traceEl;
    var geometry = traceScrollbarScrollableGeometry(elements);
    if (!geometry || !geometry.scrollable) return;

    var maxScroll = geometry.maxScroll;
    var next = traceEl.scrollLeft || 0;
    if (ev.key === 'ArrowLeft') next -= TRACE_SCROLLBAR_KEY_STEP_PX;
    else if (ev.key === 'ArrowRight') next += TRACE_SCROLLBAR_KEY_STEP_PX;
    else if (ev.key === 'PageUp') next -= geometry.clientWidth * TRACE_SCROLLBAR_PAGE_RATIO;
    else if (ev.key === 'PageDown') next += geometry.clientWidth * TRACE_SCROLLBAR_PAGE_RATIO;
    else if (ev.key === 'Home') next = 0;
    else if (ev.key === 'End') next = maxScroll;
    else return;

    traceScrollbarStopEvent(ev);
    setTraceScrollbarScrollLeft(traceEl, next);
}

function initTraceScrollbar() {
    if (!hasDOM()) return;

    var traceEl = traceRootElement();
    var viewport = traceScrollbarViewportElement(traceEl);
    if (!traceEl || !viewport || !viewport.querySelector) return;

    var bar = ensureTraceScrollbarElement(viewport);
    if (!bar || bar.__traceScrollbarReady || !bar.addEventListener) return;

    bar.__traceScrollbarReady = true;
    bar.addEventListener('pointerdown', onTraceScrollbarPointerDown);
    bar.addEventListener('pointermove', onTraceScrollbarPointerMove);
    bar.addEventListener('pointerup', endTraceScrollbarDrag);
    bar.addEventListener('pointercancel', endTraceScrollbarDrag);
    bar.addEventListener('lostpointercapture', endTraceScrollbarDrag);
    bar.addEventListener('keydown', onTraceScrollbarKeyDown);
}

function emptyStageCardHtml() {
    return '<div class="empty-stage-card">' +
           '<span class="empty-stage-dot" aria-hidden="true"></span>' +
           '<div class="empty-stage-copy">' +
           '<div class="empty-stage-title">No rules in this stage</div>' +
           '<div class="empty-stage-subtitle">Optimizer did not apply rules here</div>' +
           '</div></div>';
}

function stageShellHtml(si, forceVisible, options) {
    if (!forceVisible && !stageVisible(si)) return '';

    options = options || {};
    var name = traceStageName(si);
    var count = traceStageRuleCount(si);
    var empty = !stageHasRules(si);
    var open = effectiveStageOpen(si);
    var width = Number(options.width);
    var fixedWidth = Number.isFinite(width) && width > 0 ? width : 0;
    var wrapStyle = fixedWidth ? ' style="' + virtualStyle(fixedWidth, 0) + '"' : '';
    var stageWidthStyle = fixedWidth
        ? ';--stage-width:' + fixedWidth + 'px;' + virtualStyle(fixedWidth, 0)
        : '';
    var html = '<div class="stage-wrap' + (empty ? ' stage-empty' : '') + '"' + wrapStyle +
               ' data-stage-si="' + si + '">';

    html += '<div class="stage-col' + collapsedSearchIndicatorClass('stages', String(si)) +
            '" id="stage-col-' + si + '" role="button" tabindex="0" style="display:' +
            (open ? 'none' : 'flex') + '"' + traceActionAttr('toggle-stage', si) + '>' +
            '<div class="stage-col-arrow">' + traceIconSvg('triangle-right') + '</div>' +
            '<div class="stage-col-text" data-search-kind="stage" data-search-label="' +
            htmlEscape(name) + '" data-search-si="' + si + '">' + htmlEscape(name) + '</div>' +
            '<div class="stage-col-count">(' + count + ')</div>' +
            '</div>';

    html += '<div class="stage-exp' + collapsedSearchIndicatorClass('stages', String(si)) +
            '" id="stage-exp-' + si + '" style="display:' +
            (open ? 'flex' : 'none') + stageWidthStyle + '">';
    html += '<div class="stage-hdr" role="button" tabindex="0"' + traceActionAttr('toggle-stage', si) + '>';
    if (!empty) {
        html += '<button class="stage-toggle-btn"' +
                traceActionAttr('toggle-stage-rules', si) +
                ' aria-label="Toggle stage rules">' + traceIconSvg('stack') + '</button>';
    }
    html += '<span class="stage-hdr-arrow">' + traceIconSvg('triangle-down') + '</span>' +
            '<span class="stage-hdr-name" data-search-kind="stage" data-search-label="' +
            htmlEscape(name) + '" data-search-si="' + si + '">' + htmlEscape(name) + '</span>' +
            '<span class="stage-hdr-count">(' + count + ')</span>' +
            '</div>';

    html += '<div class="rules-row' + (empty ? ' empty-rules-row' : '') +
            '" data-virtual-row="stage-' + si + '"' +
            (empty ? ' role="button" tabindex="0"' + traceActionAttr('toggle-stage', si) : '') + '>';
    if (empty) html += emptyStageCardHtml();
    html += '</div></div></div>';
    return html;
}

function traceLoadingShellHtml() {
    var status = traceRuntime().traceStoreStatus || {};
    var title = status.title || 'Loading trace data';
    var message = status.message || 'Preparing optimizer trace data.';
    return '<div class="trace-status-canvas" id="trace-status-canvas">' +
           '<div class="trace-load-state trace-loading-state" role="status" aria-live="polite">' +
           '<span class="trace-loading-spinner" aria-hidden="true"></span>' +
           '<span class="trace-loading-copy">' +
           '<span class="trace-loading-title">' + htmlEscape(title) + '</span>' +
           '<span class="trace-loading-message">' + htmlEscape(message) + '</span>' +
           '</span></div></div>';
}

function clearRenderedTraceShellCaches() {
    virtualRuntime().stageShellSignature = '';
    virtualRuntime().mountedStageKeys = {};
    virtualRuntime().mountedRuleKeys = {};
    virtualRuntime().mountedVirtualRange = null;
    virtualRuntime().virtualRowSignatureCache = {};
}

function renderTraceShell() {
    if (!hasDOM()) return;
    var root = traceRootElement();
    if (!root) return;
    TraceActionEvents.hideCollapsedLabelLens();

    if (traceRuntime().traceStoreStatus &&
            traceRuntime().traceStoreStatus.state === 'loading') {
        root.innerHTML = traceLoadingShellHtml();
        bumpTraceCanvasDomGeneration();
        clearRenderedTraceShellCaches();
        clearTraceScrollbarCache();
        initTraceScrollbar();
        refreshTraceScrollbarGeometry(false);
        return;
    }

    if (typeof shouldVirtualizeStageShellsForCurrentTrace === 'function' &&
        shouldVirtualizeStageShellsForCurrentTrace()) {
        root.innerHTML = '<div class="trace-canvas" id="trace-canvas"></div>';
        bumpTraceCanvasDomGeneration();
        clearRenderedTraceShellCaches();

        if (typeof rebuildTraceLayoutModel === 'function') {
            updateVirtualRange();
            var model = rebuildTraceLayoutModel();
            syncTraceCanvasWidth(model);
            syncVirtualStageShells(model, root);
        }
        clearTraceScrollbarCache();
        initTraceScrollbar();
        refreshTraceScrollbarGeometry(false);
        return;
    }

    var html = '';
    var renderedStages = 0;
    for (var si = 0; si < currentStageCount(); si++) {
        if (stageVisible(si)) {
            html += stageShellHtml(si);
            renderedStages++;
        }
    }
    if (!renderedStages && currentStageCount() > 0) {
        for (var fallbackSi = 0; fallbackSi < currentStageCount(); fallbackSi++) {
            html += stageShellHtml(fallbackSi, true);
        }
    }
    root.innerHTML = '<div class="trace-canvas" id="trace-canvas">' + html + '</div>';
    bumpTraceCanvasDomGeneration();
    clearRenderedTraceShellCaches();
    clearTraceScrollbarCache();
    initTraceScrollbar();
    refreshTraceScrollbarGeometry(false);
}

function tracePickerMenuHtml() {
    var html = '';
    var traces = traceDataTraces();
    var selection = activeTraceSelection();
    for (var i = 0; i < traces.length; i++) {
        var trace = normalizeTraceData(traces[i], i);
        var order = selection.indexOf(i);
        var selected = order >= 0;
        var singleSelected = selected && selection.length <= 1;
        html += '<div class="trace-picker-option' + (selected ? ' selected' : '') + '">' +
            '<button type="button" class="trace-picker-order' + (selected ? ' selected' : '') + '" ' +
            'data-trace-action="toggle-trace-selection" data-trace-index="' + i + '" ' +
            'aria-label="' + htmlEscape(selected ? 'Hide trace ' + trace.title : 'Show trace ' + trace.title) + '"' +
            (singleSelected ? ' aria-disabled="true"' : '') + '>' +
            (selected ? String(order + 1) : '') + '</button>' +
            '<button type="button" class="trace-picker-option-title" ' +
            'data-trace-action="set-active-trace-only" data-trace-index="' + i + '">' +
            htmlEscape(trace.title) + '</button>' +
            '</div>';
    }
    return html;
}

function tracePickerFont(style) {
    if (!style) return '';
    if (style.font) return style.font;
    return [
        style.fontStyle || 'normal',
        style.fontVariant || 'normal',
        style.fontWeight || '400',
        style.fontSize || '17px',
        style.fontFamily || 'sans-serif'
    ].join(' ');
}

function tracePickerPreferredWidth(picker, traces) {
    var fallback = 260;
    var minWidth = 144;
    var chromeReserve = 18;
    if (!hasDOM() || !picker || !document.createElement || !window.getComputedStyle) return fallback;

    var style = window.getComputedStyle(picker);
    var canvas = document.createElement('canvas');
    var ctx = canvas && canvas.getContext ? canvas.getContext('2d') : null;
    if (!ctx || !style) return fallback;

    ctx.font = tracePickerFont(style);
    var maxTextWidth = 0;
    for (var i = 0; i < traces.length; i++) {
        var trace = normalizeTraceData(traces[i], i);
        var metrics = ctx.measureText(String(trace.title || ''));
        maxTextWidth = Math.max(maxTextWidth, metrics && metrics.width ? metrics.width : 0);
    }

    var padding =
        (parseFloat(style.paddingLeft) || 0) +
        (parseFloat(style.paddingRight) || 0);
    var border =
        (parseFloat(style.borderLeftWidth) || 0) +
        (parseFloat(style.borderRightWidth) || 0);
    return Math.ceil(Math.max(minWidth, maxTextWidth + padding + border + chromeReserve));
}

function updateTracePickerPreferredWidth(picker, wrap, traces) {
    if (!wrap || !wrap.style) return;
    if (!picker || traces.length <= 1) {
        if (wrap.style.removeProperty) {
            wrap.style.removeProperty('--trace-picker-width');
        } else if (wrap.style.setProperty) {
            wrap.style.setProperty('--trace-picker-width', '');
        }
        return;
    }
    if (wrap.style.setProperty) {
        wrap.style.setProperty('--trace-picker-width', tracePickerPreferredWidth(picker, traces) + 'px');
    }
}

function syncTracePicker() {
    if (!hasDOM()) return;
    var title = document.getElementById('trace-title');
    var button = document.getElementById('trace-picker-button');
    var menu = document.getElementById('trace-picker-menu');
    var wrap = document.getElementById('trace-picker-wrap');
    var traces = traceDataTraces();
    var selection = activeTraceSelection();
    var selectionKey = traceSelectionKey(selection);
    if (title) {
        title.textContent = 'RBO Trace';
        title.hidden = false;
    }
    if (wrap) {
        wrap.hidden = traces.length <= 1;
        if (wrap.parentElement && wrap.parentElement.classList) {
            wrap.parentElement.classList.toggle('trace-picker-active', traces.length > 1);
        }
    }
    if (button) {
        button.hidden = traces.length <= 1;
        button.textContent = traceSelectionTitle(selection, traces);
        button.title = button.textContent;
    }
    if (menu && (menu.__traceOptionCount !== traces.length || menu.__traceSelectionKey !== selectionKey)) {
        menu.innerHTML = tracePickerMenuHtml();
        menu.__traceOptionCount = traces.length;
        menu.__traceSelectionKey = selectionKey;
    }
    updateTracePickerPreferredWidth(button, wrap, traces);
}

function resetTraceScroll() {
    if (!hasDOM()) return;
    var traceEl = traceRootElement();
    if (!traceEl) return;
    setTraceScrollLeft(traceEl, 0, {
        updateTraceScrollbar: true
    });
    traceEl.scrollTop = 0;
}
