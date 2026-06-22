var RuntimeEpochs = (function() {
    function createState() {
        return {
            trace: 0,
            render: 0,
            search: 0,
            diff: 0,
            virtual: 0,
            layout: 0,
            fullscreen: 0,
            resize: 0
        };
    }

    function bump(state, scope) {
        if (!Object.prototype.hasOwnProperty.call(state, scope)) {
            state[scope] = 0;
        }
        state[scope]++;
        return state[scope];
    }

    function traceToken(state) {
        return {
            trace: state.trace
        };
    }

    function tokenCurrent(state, token) {
        return !!token && token.trace === state.trace;
    }

    function renderJobCurrent(state, job) {
        if (!job) return false;
        var scopes = ['trace', 'render', 'search', 'diff', 'virtual', 'layout', 'fullscreen', 'resize'];
        for (var i = 0; i < scopes.length; i++) {
            var scope = scopes[i];
            var key = scope + 'Epoch';
            if (job[key] !== undefined && job[key] !== state[scope]) return false;
        }
        return true;
    }

    return {
        bump: bump,
        createState: createState,
        renderJobCurrent: renderJobCurrent,
        tokenCurrent: tokenCurrent,
        traceToken: traceToken
    };
})();

var TraceLayout = (function() {
    function widthPartsCss(parts) {
        parts = parts || { rules: 0, px: 0 };
        if (!parts.rules) return (parts.px || 0) + 'px';
        if (!parts.px) return 'calc(' + parts.rules + ' * var(--rule-width))';
        return 'calc(' + parts.rules + ' * var(--rule-width) + ' + parts.px + 'px)';
    }

    function widthPartsToPx(parts, ruleWidth) {
        parts = parts || { rules: 0, px: 0 };
        return (parts.rules || 0) * ruleWidth + (parts.px || 0);
    }

    function rowModel(rowId, si, gi, globalLeft) {
        return {
            id: rowId,
            si: si,
            gi: gi,
            globalLeft: globalLeft,
            width: 0,
            items: []
        };
    }

    function addRowItem(row, item, gapAfter) {
        item.left = row.width;
        item.globalLeft = row.globalLeft + row.width;
        item.gapAfter = gapAfter || 0;
        item.outerWidth = item.width + item.gapAfter;
        row.items.push(item);
        row.width += item.outerWidth;
    }

    function intervalBucket(index, bucketName, si) {
        var buckets = index[bucketName];
        var key = String(si);
        if (!buckets[key]) buckets[key] = [];
        return buckets[key];
    }

    function pushStageInterval(index, bucketName, interval) {
        intervalBucket(index, bucketName, interval.si).push(interval);
    }

    function ruleIntervalForItem(item, row) {
        return {
            si: item.si,
            gi: item.gi,
            ri: item.ri,
            key: item.key,
            left: item.globalLeft,
            right: item.globalLeft + item.width,
            width: item.width,
            rowId: row.id
        };
    }

    function groupIntervalForItem(item, row) {
        return {
            si: item.si,
            gi: item.gi,
            key: item.key,
            left: item.globalLeft,
            right: item.globalLeft + item.width,
            width: item.width,
            rowId: row.id
        };
    }

    function indexLayoutIntervals(model) {
        model.rules = {};
        model.groups = {};
        if (!model.index) model.index = {};
        model.index.stageRuleIntervals = {};
        model.index.stageGroupIntervals = {};

        for (var i = 0; i < model.rows.length; i++) {
            var row = model.rows[i];
            for (var j = 0; j < row.items.length; j++) {
                var item = row.items[j];
                var interval;
                if (item.kind === 'rule') {
                    interval = ruleIntervalForItem(item, row);
                    model.rules[item.key] = interval;
                    pushStageInterval(model.index, 'stageRuleIntervals', interval);
                } else if (item.kind === 'group') {
                    interval = groupIntervalForItem(item, row);
                    model.groups[item.key] = interval;
                    pushStageInterval(model.index, 'stageGroupIntervals', interval);
                }
            }
        }
    }

    function indexRuleIntervals(model) {
        indexLayoutIntervals(model);
    }

    function itemIntersectsRange(item, range) {
        return item.globalLeft + item.width >= range.left &&
               item.globalLeft <= range.right;
    }

    return {
        addRowItem: addRowItem,
        indexLayoutIntervals: indexLayoutIntervals,
        indexRuleIntervals: indexRuleIntervals,
        itemIntersectsRange: itemIntersectsRange,
        rowModel: rowModel,
        widthPartsCss: widthPartsCss,
        widthPartsToPx: widthPartsToPx
    };
})();

function createTraceSearchIndexer(storeApi) {
    function emptySearchRecords() {
        return {
            stageTitles: [],
            groupTitles: [],
            ruleTitles: [],
            treeLabels: [],
            textTiles: []
        };
    }

    function pushSearchRecord(bucket, base, text) {
        text = text === undefined || text === null ? '' : String(text);
        if (!text) return;
        var record = {};
        for (var key in base) {
            if (Object.prototype.hasOwnProperty.call(base, key)) record[key] = base[key];
        }
        record.text = text;
        record.lowerText = text.toLowerCase();
        bucket.push(record);
    }

    function searchRecordTraceGeneration(store) {
        var generation = Number(store && store.traceGeneration);
        return Number.isInteger(generation) && generation >= 0 ? generation : 0;
    }

    function searchStoreTraceHandle(store) {
        return storeApi.traceHandle ? storeApi.traceHandle(store) : (store && store.traceHandle || null);
    }

    function searchStageHandle(store, si) {
        return storeApi.stageHandle ? storeApi.stageHandle(store, si) : null;
    }

    function searchGroupHandle(store, si, gi) {
        return storeApi.groupHandle ? storeApi.groupHandle(store, si, gi) : null;
    }

    function searchRuleHandle(store, si, rawIdx) {
        return storeApi.ruleHandle ? storeApi.ruleHandle(store, si, rawIdx) : null;
    }

    function searchRulePlacements(store, si, rawIdx) {
        var placements = [];
        for (var gi = 0; gi < storeApi.groupCount(store, si); gi++) {
            for (var ri = 0; ri < storeApi.groupRuleCount(store, si, gi); ri++) {
                if (storeApi.rawRuleIndex(store, si, gi, ri) !== rawIdx) continue;
                placements.push({
                    gi: gi,
                    ri: ri,
                    groupHandle: searchGroupHandle(store, si, gi)
                });
            }
        }
        return placements;
    }

    function stageRecordBase(store, si, field) {
        return {
            type: 'stage',
            field: field || 'stage',
            traceGeneration: searchRecordTraceGeneration(store),
            traceHandle: searchStoreTraceHandle(store),
            si: si,
            stageHandle: searchStageHandle(store, si),
            occurrenceBase: 0
        };
    }

    function groupRecordBase(store, si, gi, field) {
        return {
            type: 'group',
            field: field || 'group',
            traceGeneration: searchRecordTraceGeneration(store),
            traceHandle: searchStoreTraceHandle(store),
            si: si,
            gi: gi,
            stageHandle: searchStageHandle(store, si),
            groupHandle: searchGroupHandle(store, si, gi),
            occurrenceBase: 0
        };
    }

    function createRuleRecordContext(store, records, si, rawIdx) {
        return {
            store: store,
            records: records,
            traceGeneration: searchRecordTraceGeneration(store),
            traceHandle: searchStoreTraceHandle(store),
            stageHandle: searchStageHandle(store, si),
            ruleHandle: searchRuleHandle(store, si, rawIdx),
            placements: searchRulePlacements(store, si, rawIdx),
            si: si,
            rawIdx: rawIdx,
            treeOrder: 0,
            fieldOrdinals: {}
        };
    }

    function nextRuleFieldOrdinal(ctx, field) {
        if (!ctx) return 0;
        var value = ctx.fieldOrdinals[field] || 0;
        ctx.fieldOrdinals[field] = value + 1;
        return value;
    }

    function ruleRecordBase(ctx, field, extra) {
        var base = {
            type: 'rule',
            field: field,
            traceGeneration: ctx && ctx.traceGeneration || 0,
            traceHandle: ctx && ctx.traceHandle || null,
            si: ctx && ctx.si,
            rawIdx: ctx && ctx.rawIdx,
            stageHandle: ctx && ctx.stageHandle || null,
            ruleHandle: ctx && ctx.ruleHandle || null,
            placements: ctx && ctx.placements ? ctx.placements.slice() : [],
            fieldOrdinal: nextRuleFieldOrdinal(ctx, field),
            occurrenceBase: 0
        };
        extra = extra || {};
        for (var key in extra) {
            if (Object.prototype.hasOwnProperty.call(extra, key)) base[key] = extra[key];
        }
        return base;
    }

    function compareSearchRecordsByOrder(a, b) {
        var orderA = Number(a && a.order) || 0;
        var orderB = Number(b && b.order) || 0;
        if (orderA !== orderB) return orderA - orderB;
        return 0;
    }

    function searchRecordBuckets(records) {
        records = records || {};
        return [
            records.stageTitles || [],
            records.groupTitles || [],
            records.ruleTitles || [],
            records.treeLabels || [],
            records.textTiles || []
        ];
    }

    function finalizeSearchRecordIdentity(records) {
        var byRuleField = {};
        var buckets = searchRecordBuckets(records);

        for (var b = 0; b < buckets.length; b++) {
            for (var i = 0; i < buckets[b].length; i++) {
                var record = buckets[b][i];
                if (!record || record.type !== 'rule') continue;
                var key = String(record.si) + ':' + String(record.rawIdx) + ':' + String(record.field);
                if (!byRuleField[key]) byRuleField[key] = [];
                byRuleField[key].push(record);
            }
        }

        for (var ruleFieldKey in byRuleField) {
            if (!Object.prototype.hasOwnProperty.call(byRuleField, ruleFieldKey)) continue;
            var list = byRuleField[ruleFieldKey].sort(compareSearchRecordsByOrder);
            for (var index = 0; index < list.length; index++) {
                list[index].fieldOrdinal = index;
                list[index].occurrenceBase = 0;
            }
        }

        return records;
    }

    function buildIndex(store) {
        var index = [];
        var records = emptySearchRecords();
        index.searchRecords = records;
        index.mode = 'full';

        for (var si = 0; si < storeApi.stageCount(store); si++) {
            index[si] = [];
            pushSearchRecord(records.stageTitles, {
                type: 'stage',
                field: 'stage',
                traceGeneration: searchRecordTraceGeneration(store),
                traceHandle: searchStoreTraceHandle(store),
                si: si,
                stageHandle: searchStageHandle(store, si),
                occurrenceBase: 0
            }, storeApi.stageName(store, si));

            for (var gi = 0; gi < storeApi.groupCount(store, si); gi++) {
                pushSearchRecord(records.groupTitles, groupRecordBase(store, si, gi), storeApi.groupName(store, si, gi));
            }

            for (var ri = 0; ri < storeApi.stageRuleCount(store, si); ri++) {
                var recordContext = createRuleRecordContext(store, records, si, ri);
                var parts = storeApi.ruleType(store, si, ri) === 'text'
                    ? buildTextSearchIndex(storeApi.ruleText(store, si, ri), recordContext)
                    : buildTreeSearchIndex(store, storeApi.planTree(store, si, ri), recordContext);
                parts.stage = String(storeApi.stageName(store, si)).toLowerCase();
                parts.name = String(storeApi.ruleName(store, si, ri)).toLowerCase();
                pushSearchRecord(records.ruleTitles, ruleRecordBase(recordContext, 'name', {
                    order: 0
                }), storeApi.ruleName(store, si, ri));
                index[si][ri] = parts;
            }
        }

        finalizeSearchRecordIdentity(records);
        return index;
    }

    function buildTitleIndex(store) {
        var index = [];
        var records = emptySearchRecords();
        index.searchRecords = records;
        index.mode = 'title';

        for (var si = 0; si < storeApi.stageCount(store); si++) {
            index[si] = [];
            var stageText = String(storeApi.stageName(store, si)).toLowerCase();
            pushSearchRecord(records.stageTitles, stageRecordBase(store, si), storeApi.stageName(store, si));

            for (var gi = 0; gi < storeApi.groupCount(store, si); gi++) {
                pushSearchRecord(records.groupTitles, groupRecordBase(store, si, gi), storeApi.groupName(store, si, gi));
            }

            for (var ri = 0; ri < storeApi.stageRuleCount(store, si); ri++) {
                var name = String(storeApi.ruleName(store, si, ri)).toLowerCase();
                index[si][ri] = {
                    stage: stageText,
                    name: name,
                    label: ''
                };
                var recordContext = createRuleRecordContext(store, records, si, ri);
                pushSearchRecord(records.ruleTitles, ruleRecordBase(recordContext, 'name', {
                    order: 0
                }), storeApi.ruleName(store, si, ri));
            }
        }

        finalizeSearchRecordIdentity(records);
        return index;
    }

    function emptyRuleSearchIndex() {
        return {
            stage: '',
            name: '',
            label: ''
        };
    }

    function buildTextSearchIndex(text, recordContext) {
        var parts = emptyRuleSearchIndex();
        parts.label = String(text == null ? '' : text).toLowerCase();
        if (recordContext && recordContext.records) {
            pushSearchRecord(recordContext.records.textTiles, ruleRecordBase(recordContext, 'label', {
                order: 200000
            }), text);
        }
        return parts;
    }

    function buildTreeSearchIndex(store, node, recordContext) {
        var parts = { label: [] };
        collectTreeSearchIndex(node, parts, recordContext, []);
        var index = emptyRuleSearchIndex();
        index.label = parts.label.join('\n').toLowerCase();
        return index;
    }

    function collectTreeSearchIndex(node, parts, recordContext, path) {
        if (!node || !node.l) return;

        var label = String(node.l);
        parts.label.push(label);
        if (recordContext && recordContext.records) {
            pushSearchRecord(recordContext.records.treeLabels, ruleRecordBase(recordContext, 'label', {
                path: path.join('.'),
                order: 200000 + recordContext.treeOrder++
            }), label);
        }

        if (node.c) {
            for (var j = 0; j < node.c.length; j++) {
                collectTreeSearchIndex(node.c[j], parts, recordContext, path.concat(j));
            }
        }
    }

    function countOccurrences(text, lowerQuery) {
        if (!text || !lowerQuery) return 0;

        var count = 0;
        var pos = 0;
        while (pos < text.length) {
            var idx = text.indexOf(lowerQuery, pos);
            if (idx === -1) break;
            count++;
            pos = idx + lowerQuery.length;
        }
        return count;
    }

    function stageMatchesQuery(store, si, lowerQuery) {
        return String(storeApi.stageName(store, si)).toLowerCase().indexOf(lowerQuery) !== -1;
    }

    function groupMatchesQuery(store, si, gi, lowerQuery) {
        return String(storeApi.groupName(store, si, gi)).toLowerCase().indexOf(lowerQuery) !== -1;
    }

    function planSearchIncludesRuleField(field) {
        return field === 'name' || field === 'label';
    }

    function ruleMatchContext(matches, si, gi, ri, rawIdx, lowerQuery, scope) {
        return {
            matches: matches,
            store: null,
            si: si,
            gi: gi,
            ri: ri,
            rawIdx: rawIdx,
            lowerQuery: lowerQuery,
            scope: scope,
            fieldOccurrences: {},
            recordOccurrences: {}
        };
    }

    function searchMatchRecordOccurrenceKey(field, path, row) {
        if (!Array.isArray(path) && typeof path !== 'string') return '';
        var pathKey = Array.isArray(path) ? path.join('.') : String(path || '');
        var key = field + ':path:' + pathKey;
        if (row !== undefined && row !== null) key += ':row:' + String(row);
        return key;
    }

    function pushRuleTextMatches(ctx, field, text, path, row) {
        if (!planSearchIncludesRuleField(field)) return;

        text = String(text == null ? '' : text).toLowerCase();
        var count = countOccurrences(text, ctx.lowerQuery);
        var start = ctx.fieldOccurrences[field] || 0;
        var recordOccurrenceKey = searchMatchRecordOccurrenceKey(field, path, row);
        var recordStart = recordOccurrenceKey ? (ctx.recordOccurrences[recordOccurrenceKey] || 0) : 0;

        for (var n = 0; n < count; n++) {
            var match = {
                type: 'rule',
                si: ctx.si,
                gi: ctx.gi,
                ri: ctx.ri,
                rawIdx: ctx.rawIdx,
                field: field,
                occurrence: start + n
            };
            if (recordOccurrenceKey) match.recordOccurrence = recordStart + n;
            if (Array.isArray(path)) match.path = path.join('.');
            if (row !== undefined) match.row = row;
            ctx.matches.push(match);
        }

        ctx.fieldOccurrences[field] = start + count;
        if (recordOccurrenceKey) ctx.recordOccurrences[recordOccurrenceKey] = recordStart + count;
    }

    function pushTreeNodeMatches(ctx, node, path) {
        if (!node || !node.l) return;
        path = Array.isArray(path) ? path : [];

        pushRuleTextMatches(ctx, 'label', node.l, path);

        if (node.c) {
            for (var j = 0; j < node.c.length; j++) {
                pushTreeNodeMatches(ctx, node.c[j], path.concat(j));
            }
        }
    }

    function pushRuleMatches(store, index, matches, si, gi, ri, rawIdx, lowerQuery, scope) {
        var ctx = ruleMatchContext(matches, si, gi, ri, rawIdx, lowerQuery, scope);
        ctx.store = store;
        var ruleIndex = index[si][rawIdx];

        pushRuleTextMatches(ctx, 'name', storeApi.ruleName(store, si, rawIdx));
        if (index.mode === 'title') return;

        if (storeApi.ruleType(store, si, rawIdx) === 'text') {
            pushRuleTextMatches(ctx, 'label', storeApi.ruleText(store, si, rawIdx));
            return;
        }

        pushTreeNodeMatches(ctx, storeApi.planTree(store, si, rawIdx), []);
    }

    function recordMatchCount(record, lowerQuery) {
        return countOccurrences(record && record.lowerText || '', lowerQuery);
    }

    function mapRecordCount(map, key, count) {
        if (!count) return;
        map[key] = (map[key] || 0) + count;
    }

    function searchRecordRuleKey(record) {
        return String(record.si) + ':' + String(record.rawIdx);
    }

    function searchRecordGroupKey(record) {
        return String(record.si) + ':' + String(record.gi);
    }

    function addRuleRecordMatch(map, record, count) {
        if (!count) return;
        var key = searchRecordRuleKey(record);
        if (!map[key]) map[key] = [];
        map[key].push({
            record: record,
            count: count
        });
    }

    function compareRuleRecordMatches(a, b) {
        var orderA = Number(a && a.record && a.record.order) || 0;
        var orderB = Number(b && b.record && b.record.order) || 0;
        if (orderA !== orderB) return orderA - orderB;
        var fieldA = a && a.record && a.record.field || '';
        var fieldB = b && b.record && b.record.field || '';
        if (fieldA < fieldB) return -1;
        if (fieldA > fieldB) return 1;
        return 0;
    }

    function recordMatchesPlanSearch(record) {
        if (!record) return false;
        if (record.part === 'pinned-header') return false;
        return record.field === 'name' || record.field === 'label';
    }

    function collectRecordQueryState(store, index, lowerQuery, scope, options) {
        var records = index && index.searchRecords;
        if (!records) return null;

        var state = {
            stages: {},
            groups: {},
            rules: {}
        };

        var stageRecords = records.stageTitles || [];
        for (var si = 0; si < stageRecords.length; si++) {
            mapRecordCount(
                state.stages,
                String(stageRecords[si].si),
                recordMatchCount(stageRecords[si], lowerQuery)
            );
        }

        var groupRecords = records.groupTitles || [];
        for (var gi = 0; gi < groupRecords.length; gi++) {
            mapRecordCount(
                state.groups,
                searchRecordGroupKey(groupRecords[gi]),
                recordMatchCount(groupRecords[gi], lowerQuery)
            );
        }

        var buckets = [
            records.ruleTitles || [],
            records.treeLabels || [],
            records.textTiles || []
        ];
        for (var b = 0; b < buckets.length; b++) {
            for (var r = 0; r < buckets[b].length; r++) {
                var record = buckets[b][r];
                if (!recordMatchesPlanSearch(record)) continue;
                addRuleRecordMatch(state.rules, record, recordMatchCount(record, lowerQuery));
            }
        }

        for (var key in state.rules) {
            if (Object.prototype.hasOwnProperty.call(state.rules, key)) {
                state.rules[key].sort(compareRuleRecordMatches);
            }
        }

        return state;
    }

    function pushRecordBackedRuleMatches(matches, recordMatches, si, gi, ri, rawIdx) {
        if (!recordMatches || !recordMatches.length) return;

        var fieldOccurrences = {};
        for (var i = 0; i < recordMatches.length; i++) {
            var record = recordMatches[i].record;
            var field = record.field;
            var start = fieldOccurrences[field] || 0;
            for (var n = 0; n < recordMatches[i].count; n++) {
                var match = {
                    type: 'rule',
                    si: si,
                    gi: gi,
                    ri: ri,
                    rawIdx: rawIdx,
                    field: field,
                    occurrence: start + n,
                    occurrenceBase: start,
                    fieldOrdinal: record.fieldOrdinal,
                    recordOccurrence: n
                };
                if (record.path !== undefined) match.path = record.path;
                if (record.row !== undefined) match.row = record.row;
                if (record.part !== undefined) match.part = record.part;
                matches.push(match);
            }
            fieldOccurrences[field] = start + recordMatches[i].count;
        }
    }

    function pushRecordBackedStageMatches(store, recordState, matches, si) {
        var stageMatchCount = recordState.stages[String(si)] || 0;
        for (var n = 0; n < stageMatchCount; n++) {
            matches.push({ type: 'stage', si: si, field: 'stage', occurrence: n });
        }

        for (var gi = 0; gi < storeApi.groupCount(store, si); gi++) {
            var groupMatchCount = storeApi.groupRuleCount(store, si, gi) > 1
                ? recordState.groups[String(si) + ':' + String(gi)] || 0
                : 0;
            for (var g = 0; g < groupMatchCount; g++) {
                matches.push({
                    type: 'group',
                    si: si,
                    gi: gi,
                    field: 'group',
                    occurrence: g
                });
            }

            for (var ri = 0; ri < storeApi.groupRuleCount(store, si, gi); ri++) {
                var rawIdx = storeApi.rawRuleIndex(store, si, gi, ri);
                pushRecordBackedRuleMatches(
                    matches,
                    recordState.rules[String(si) + ':' + String(rawIdx)],
                    si,
                    gi,
                    ri,
                    rawIdx
                );
            }
        }
    }

    function createRecordMatchCollector(store, index, query, scope, options) {
        var lowerQuery = String(query || '').toLowerCase();
        var recordState = collectRecordQueryState(store, index, lowerQuery, scope, options);
        if (!recordState) return null;
        var stageIndex = 0;
        var stageTotal = storeApi.stageCount(store);

        return {
            mode: index && index.mode || 'unknown',
            stageTotal: stageTotal,
            get stageIndex() {
                return stageIndex;
            },
            done: !lowerQuery || stageIndex >= stageTotal,
            next: function(maxStages) {
                maxStages = Math.max(1, Math.floor(Number(maxStages)) || stageTotal || 1);
                var matches = [];
                if (!lowerQuery || stageIndex >= stageTotal) {
                    this.done = true;
                    return {
                        matches: matches,
                        done: true,
                        stageIndex: stageIndex,
                        stageTotal: stageTotal
                    };
                }

                var end = Math.min(stageTotal, stageIndex + maxStages);
                for (var si = stageIndex; si < end; si++) {
                    pushRecordBackedStageMatches(store, recordState, matches, si);
                }
                stageIndex = end;
                this.done = stageIndex >= stageTotal;
                return {
                    matches: matches,
                    done: this.done,
                    stageIndex: stageIndex,
                    stageTotal: stageTotal
                };
            }
        };
    }

    function collectMatchesFromRecords(store, index, query, scope, options) {
        var lowerQuery = String(query || '').toLowerCase();
        var matches = [];
        if (!lowerQuery) return matches;
        var collector = createRecordMatchCollector(store, index, query, scope, options);
        if (!collector) return null;

        while (!collector.done) {
            var batch = collector.next(collector.stageTotal || 1);
            for (var i = 0; i < batch.matches.length; i++) matches.push(batch.matches[i]);
        }

        return matches;
    }

    function collectMatchesByTreeWalk(store, index, query, scope) {
        var lowerQuery = String(query || '').toLowerCase();
        var matches = [];
        if (!lowerQuery) return matches;

        for (var si = 0; si < storeApi.stageCount(store); si++) {
            if (stageMatchesQuery(store, si, lowerQuery)) {
                var stageText = String(storeApi.stageName(store, si)).toLowerCase();
                var stageMatchCount = countOccurrences(stageText, lowerQuery);
                for (var n = 0; n < stageMatchCount; n++) {
                    matches.push({ type: 'stage', si: si, field: 'stage', occurrence: n });
                }
            }

            for (var gi = 0; gi < storeApi.groupCount(store, si); gi++) {
                if (storeApi.groupRuleCount(store, si, gi) > 1 &&
                    groupMatchesQuery(store, si, gi, lowerQuery)) {
                    var groupText = String(storeApi.groupName(store, si, gi)).toLowerCase();
                    var groupMatchCount = countOccurrences(groupText, lowerQuery);
                    for (var g = 0; g < groupMatchCount; g++) {
                        matches.push({
                            type: 'group',
                            si: si,
                            gi: gi,
                            field: 'group',
                            occurrence: g
                        });
                    }
                }

                for (var ri = 0; ri < storeApi.groupRuleCount(store, si, gi); ri++) {
                    pushRuleMatches(
                        store,
                        index,
                        matches,
                        si,
                        gi,
                        ri,
                        storeApi.rawRuleIndex(store, si, gi, ri),
                        lowerQuery,
                        scope
                    );
                }
            }
        }

        return matches;
    }

    function collectMatches(store, index, query, scope, options) {
        var recordMatches = collectMatchesFromRecords(store, index, query, scope, options);
        if (recordMatches) return recordMatches;
        return collectMatchesByTreeWalk(store, index, query, scope);
    }

    function globalSummaryFieldMask(masks, name) {
        return masks && masks[name] || 0;
    }

    function searchRuleOrdinal(store, si, rawIdx) {
        var ordinal = 0;
        for (var stageIndex = 0; stageIndex < si; stageIndex++) {
            ordinal += storeApi.stageRuleCount(store, stageIndex);
        }
        return ordinal + rawIdx;
    }

    function addGlobalSummaryPart(summary, masks, count, maskName) {
        count = Number(count) || 0;
        if (count <= 0) return;
        summary.count += count;
        summary.mask |= globalSummaryFieldMask(masks, maskName);
    }

    function countGlobalSummaryText(text, lowerQuery) {
        return countOccurrences(String(text == null ? '' : text).toLowerCase(), lowerQuery);
    }

    function countGlobalSummaryTreeLabels(node, lowerQuery) {
        if (!node) return 0;
        var count = 0;
        var stack = [node];

        while (stack.length) {
            var current = stack.pop();
            if (!current) continue;

            count += countGlobalSummaryText(
                current.l !== undefined ? current.l : current.label,
                lowerQuery
            );

            var children = Array.isArray(current.c)
                ? current.c
                : (Array.isArray(current.children) ? current.children : []);
            for (var j = children.length - 1; j >= 0; j--) {
                stack.push(children[j]);
            }
        }

        return count;
    }

    function buildGlobalSummary(store, query, scope, masks) {
        store = store || {};
        query = String(query || '');
        scope = scope || 'tree-rules';
        var lowerQuery = query.toLowerCase();
        var rows = [];
        var total = 0;

        if (!lowerQuery) {
            return {
                query: query,
                scope: String(scope),
                totalCount: 0,
                ruleOrdinals: [],
                ruleHandles: [],
                counts: [],
                masks: []
            };
        }

        for (var si = 0; si < storeApi.stageCount(store); si++) {
            for (var rawIdx = 0; rawIdx < storeApi.stageRuleCount(store, si); rawIdx++) {
                var item = {
                    ruleOrdinal: searchRuleOrdinal(store, si, rawIdx),
                    ruleHandle: searchRuleHandle(store, si, rawIdx),
                    count: 0,
                    mask: 0
                };

                addGlobalSummaryPart(
                    item,
                    masks,
                    countGlobalSummaryText(storeApi.ruleName(store, si, rawIdx), lowerQuery),
                    'title'
                );

                if (storeApi.ruleType(store, si, rawIdx) === 'text') {
                    addGlobalSummaryPart(
                        item,
                        masks,
                        countGlobalSummaryText(storeApi.ruleText(store, si, rawIdx), lowerQuery),
                        'text'
                    );
                } else {
                    addGlobalSummaryPart(
                        item,
                        masks,
                        countGlobalSummaryTreeLabels(storeApi.planTree(store, si, rawIdx), lowerQuery),
                        'tree'
                    );
                }

                if (item.count > 0) {
                    rows.push(item);
                    total += item.count;
                }
            }
        }

        return {
            query: query,
            scope: String(scope),
            totalCount: total,
            ruleOrdinals: rows.map(function(row) { return row.ruleOrdinal; }),
            ruleHandles: rows.map(function(row) { return row.ruleHandle; }),
            counts: rows.map(function(row) { return row.count; }),
            masks: rows.map(function(row) { return row.mask; })
        };
    }

    return {
        buildIndex: buildIndex,
        buildGlobalSummary: buildGlobalSummary,
        buildTitleIndex: buildTitleIndex,
        buildTextSearchIndex: buildTextSearchIndex,
        buildTreeSearchIndex: buildTreeSearchIndex,
        collectMatches: collectMatches,
        collectMatchesByTreeWalk: collectMatchesByTreeWalk,
        collectMatchesFromRecords: collectMatchesFromRecords,
        createRecordMatchCollector: createRecordMatchCollector,
        countOccurrences: countOccurrences
    };
}

var TraceSearch = createTraceSearchIndexer(TraceStore);

var TraceDiffState = (function() {
    function cloneRef(ref) {
        return ref ? { si: ref.si, gi: ref.gi, ri: ref.ri } : null;
    }

    function cloneSession(session) {
        session = session || {};
        return {
            a: cloneRef(session.a),
            b: cloneRef(session.b),
            savedState: session.savedState || null,
            savedViewport: session.savedViewport || null
        };
    }

    function sameRef(a, b) {
        if (!a || !b) return false;
        return a.si === b.si && a.gi === b.gi && a.ri === b.ri;
    }

    function ruleRef(si, gi, ri) {
        return { si: si, gi: gi, ri: ri };
    }

    function oppositeSide(side) {
        return side === 'a' ? 'b' : 'a';
    }

    function isActive(session) {
        return !!(session && session.savedState && session.a && session.b);
    }

    function selectSide(session, side, ref) {
        var next = cloneSession(session);
        ref = cloneRef(ref);
        var current = next[side];
        var otherSide = oppositeSide(side);

        if (sameRef(current, ref)) {
            if (isActive(next)) {
                return {
                    action: 'exit-keep-other',
                    keepWhich: otherSide,
                    releasedRef: ref,
                    session: next
                };
            }
            next[side] = null;
            return {
                action: 'deselect',
                clearWhich: side,
                session: next
            };
        }

        next[side] = ref;
        return {
            action: 'select',
            clearWhich: side,
            selectedSide: side,
            selectedRef: ref,
            shouldShowInline: !!next[otherSide],
            session: next
        };
    }

    function clearSelection(session, which) {
        var next = cloneSession(session);
        if (which === 'a' || which === 'both') next.a = null;
        if (which === 'b' || which === 'both') next.b = null;
        return {
            action: 'clear',
            clearWhich: which,
            session: next
        };
    }

    function closeSession(session) {
        var next = cloneSession(session);
        var restoreState = next.savedState;
        var restoreViewport = next.savedViewport;
        next.a = null;
        next.b = null;
        next.savedState = null;
        next.savedViewport = null;
        return {
            action: 'close',
            restoreState: restoreState,
            restoreViewport: restoreViewport,
            session: next
        };
    }

    function exitKeepOther(session, keepWhich) {
        var next = cloneSession(session);
        var restoreState = next.savedState;
        var restoreViewport = next.savedViewport;
        if (keepWhich === 'a') next.b = null;
        else next.a = null;
        next.savedState = null;
        next.savedViewport = null;
        return {
            action: 'exit-keep-other',
            clearWhich: keepWhich === 'a' ? 'b' : 'a',
            restoreState: restoreState,
            restoreViewport: restoreViewport,
            session: next
        };
    }

    function beginInline(session, savedState, savedViewport) {
        var next = cloneSession(session);
        var started = false;
        if (!next.savedState) {
            next.savedState = savedState;
            next.savedViewport = savedViewport;
            started = true;
        }
        return {
            action: 'begin-inline',
            started: started,
            session: next
        };
    }

    function ruleKey(ref) {
        return ref.si + '-' + ref.gi + '-' + ref.ri;
    }

    function pairKey(session) {
        session = session || {};
        return session.a && session.b ? ruleKey(session.a) + '|' + ruleKey(session.b) : '';
    }

    return {
        beginInline: beginInline,
        clearSelection: clearSelection,
        closeSession: closeSession,
        cloneSession: cloneSession,
        exitKeepOther: exitKeepOther,
        isActive: isActive,
        pairKey: pairKey,
        ruleKey: ruleKey,
        ruleRef: ruleRef,
        sameRef: sameRef,
        selectSide: selectSide
    };
})();
