function createTraceSearchSnapshotStore() {
    function stageArray(store, key, si) {
        return store && store[key] && Array.isArray(store[key][si]) ? store[key][si] : [];
    }

    function stageGroups(store, si) {
        return stageArray(store, 'groups', si);
    }

    function group(store, si, gi) {
        return stageGroups(store, si)[gi] || null;
    }

    function groupRuleIndices(item) {
        if (!item) return [];
        return Array.isArray(item.ri) ? item.ri : (Array.isArray(item.ruleIndices) ? item.ruleIndices : []);
    }

    function stageCount(store) {
        store = store || {};
        return Math.max(
            store.stageNames && store.stageNames.length || 0,
            store.groups && store.groups.length || 0,
            store.ruleNames && store.ruleNames.length || 0,
            store.ruleTypes && store.ruleTypes.length || 0,
            store.ruleText && store.ruleText.length || 0,
            store.planTrees && store.planTrees.length || 0
        );
    }

    function stageRuleCount(store, si) {
        return stageArray(store, 'planTrees', si).length;
    }

    function stageName(store, si) {
        return store && store.stageNames && store.stageNames[si] || '';
    }

    function traceHandle(store) {
        return store && store.traceHandle || 'trace:0';
    }

    function stageHandle(store, si) {
        return traceHandle(store) + '/stage:' + si;
    }

    function groupHandle(store, si, gi) {
        return stageHandle(store, si) + '/group:' + gi;
    }

    function ruleHandle(store, si, rawIdx) {
        return stageHandle(store, si) + '/rule:' + rawIdx;
    }

    function groupName(store, si, gi) {
        var item = group(store, si, gi);
        return item ? item.name || '' : '';
    }

    function groupCount(store, si) {
        return stageGroups(store, si).length;
    }

    function groupRuleCount(store, si, gi) {
        return groupRuleIndices(group(store, si, gi)).length;
    }

    function rawRuleIndex(store, si, gi, ri) {
        return groupRuleIndices(group(store, si, gi))[ri];
    }

    function ruleName(store, si, rawIdx) {
        return stageArray(store, 'ruleNames', si)[rawIdx] || '';
    }

    function ruleType(store, si, rawIdx) {
        return stageArray(store, 'ruleTypes', si)[rawIdx] === 'text' ? 'text' : 'plan';
    }

    function ruleText(store, si, rawIdx) {
        return stageArray(store, 'ruleText', si)[rawIdx] || '';
    }

    function planTree(store, si, rawIdx) {
        return stageArray(store, 'planTrees', si)[rawIdx] || null;
    }

    return {
        groupCount: groupCount,
        groupName: groupName,
        groupRuleCount: groupRuleCount,
        groupHandle: groupHandle,
        planTree: planTree,
        rawRuleIndex: rawRuleIndex,
        ruleHandle: ruleHandle,
        ruleName: ruleName,
        ruleText: ruleText,
        ruleType: ruleType,
        stageCount: stageCount,
        stageGroups: stageGroups,
        stageHandle: stageHandle,
        stageName: stageName,
        stageRuleCount: stageRuleCount,
        traceHandle: traceHandle
    };
}

var TraceSearchWorker = (function() {
    var BUILD_GLOBAL_SUMMARY_OPERATION = 'build-global-search-summary';
    var SUMMARY_FIELD_MASKS = {
        title: 1,
        tree: 2,
        text: 4
    };

    function canCreateWorker() {
        var global = traceDataGlobal();
        var host = global.Worker ? global : (global.window || {});
        return !!host.Worker &&
            !!host.Blob &&
            !!host.URL &&
            !!host.URL.createObjectURL;
    }

    function searchWorkerDecoderAssets() {
        var decoder = traceDataGlobal().OptimizerTraceBrotliDecoder || {};
        return {
            source: typeof decoder.workerSource === 'string' ? decoder.workerSource : '',
            wasmBase64: typeof decoder.wasmBase64 === 'string' ? decoder.wasmBase64 : ''
        };
    }

    function workerSource() {
        var assets = searchWorkerDecoderAssets();
        return [
            'var createTraceWorkerProtocol = ' + createTraceWorkerProtocol.toString() + ';',
            'var TraceWorkerProtocol = createTraceWorkerProtocol();',
            'var traceDataBase64UrlToBytes = ' + traceDataBase64UrlToBytes.toString() + ';',
            'var traceDataBytesToString = ' + traceDataBytesToString.toString() + ';',
            assets.source || '',
            'if (self.OptimizerTraceBrotliDecoder && typeof self.OptimizerTraceBrotliDecoder.setWasmBase64 === "function") {',
            '  self.OptimizerTraceBrotliDecoder.setWasmBase64(' + JSON.stringify(assets.wasmBase64 || '') + ');',
            '}',
            'var createTraceSearchSnapshotStore = ' + createTraceSearchSnapshotStore.toString() + ';',
            'var createTraceSearchIndexer = ' + createTraceSearchIndexer.toString() + ';',
            'var TraceSearch = createTraceSearchIndexer(createTraceSearchSnapshotStore());',
            'var TRACE_SEARCH_SUMMARY_FIELD_MASKS = ' + JSON.stringify(SUMMARY_FIELD_MASKS) + ';',
            'var parseSearchPayloadEnvelope = ' + parseSearchPayloadEnvelope.toString() + ';',
            'var decodedSearchPayloadBlock = ' + decodedSearchPayloadBlock.toString() + ';',
            'var searchPayloadAt = ' + searchPayloadAt.toString() + ';',
            'var materializeSearchStorePayloads = ' + materializeSearchStorePayloads.toString() + ';',
            'var buildTraceSearchGlobalSummary = ' + buildTraceSearchGlobalSummary.toString() + ';',
            'self.onmessage = function(ev) {',
            '  var request = ev.data || {};',
            '  try {',
            '    var payload = request.payload || {};',
            '    if (request.operation === ' + JSON.stringify(BUILD_GLOBAL_SUMMARY_OPERATION) + ') {',
            '      self.postMessage(TraceWorkerProtocol.resultForRequest(request, {',
            '        summary: buildTraceSearchGlobalSummary(payload.store || {}, payload.query || "", payload.scope || "tree-rules")',
            '      }));',
            '      return;',
            '    }',
            '  } catch (err) {',
            '    self.postMessage(TraceWorkerProtocol.errorForRequest(request, err, {',
            '      operation: request.operation,',
            '      sourceBlockId: null',
            '    }));',
            '  }',
            '};',
            ''
        ].join('\n');
    }

    function buildTraceSearchGlobalSummary(store, query, scope) {
        materializeSearchStorePayloads(store || {});
        return TraceSearch.buildGlobalSummary(
            store || {},
            query,
            scope || 'tree-rules',
            TRACE_SEARCH_SUMMARY_FIELD_MASKS
        );
    }

    function payloadBlockMap(registry) {
        registry = registry || {};
        var result = {};
        var blocks = registry.blocks || {};
        for (var key in blocks) {
            if (!Object.prototype.hasOwnProperty.call(blocks, key)) continue;
            var block = blocks[key] || {};
            var id = String(block.id || block.blockId || key);
            if (!id) continue;
            result[id] = {
                id: id,
                payload: block.payload || null,
                rawBytes: Math.max(0, Math.floor(Number(block.rawBytes)) || 0),
                state: block.state || 'unloaded',
                error: block.error || null
            };
        }
        return result;
    }

    function decodedPayloadBlockMap(registry) {
        registry = registry || {};
        var result = {};
        var entries = registry.cache && registry.cache.entries || {};
        for (var key in entries) {
            if (!Object.prototype.hasOwnProperty.call(entries, key)) continue;
            if (entries[key]) result[key] = entries[key].value;
        }
        return result;
    }

    function parseSearchPayloadEnvelope(payload) {
        payload = payload || {};
        if (payload.compression !== 'brotli') {
            throw new Error('Unsupported trace payload compression: ' + String(payload.compression || ''));
        }
        if (payload.encoding && payload.encoding !== 'base64url') {
            throw new Error('Unsupported trace payload encoding: ' + String(payload.encoding || ''));
        }
        var decoder = null;
        if (typeof self !== 'undefined' && self.OptimizerTraceBrotliDecoder) {
            decoder = self.OptimizerTraceBrotliDecoder;
        } else if (typeof traceDataGlobal === 'function') {
            decoder = traceDataGlobal().OptimizerTraceBrotliDecoder;
        }
        if (!decoder || typeof decoder.decode !== 'function') {
            throw new Error('Brotli-compressed trace payload requires the embedded Brotli decoder');
        }
        var compressed = traceDataBase64UrlToBytes(payload.body || '');
        var jsonBytes = decoder.decode(compressed);
        return JSON.parse(traceDataBytesToString(jsonBytes));
    }

    function decodedSearchPayloadBlock(store, blockId) {
        blockId = String(blockId || '');
        if (!blockId) return null;
        if (store.decodedPayloadBlocks &&
                Object.prototype.hasOwnProperty.call(store.decodedPayloadBlocks, blockId)) {
            return store.decodedPayloadBlocks[blockId];
        }
        var block = store.payloadBlocks && store.payloadBlocks[blockId];
        if (!block || !block.payload) return null;
        var decoded = parseSearchPayloadEnvelope(block.payload);
        if (!store.decodedPayloadBlocks) store.decodedPayloadBlocks = {};
        store.decodedPayloadBlocks[blockId] = decoded;
        return decoded;
    }

    function searchPayloadAt(store, ref) {
        if (!ref || !ref.blockId) return null;
        var decoded = decodedSearchPayloadBlock(store, ref.blockId);
        var payloads = decoded && Array.isArray(decoded.payloads) ? decoded.payloads : [];
        var index = Math.max(0, Math.floor(Number(ref.payloadIndex)) || 0);
        return index < payloads.length ? payloads[index] : null;
    }

    function materializeSearchStorePayloads(store) {
        store = store || {};
        var refsByStage = Array.isArray(store.tilePayloadRefs) ? store.tilePayloadRefs : [];
        if (!refsByStage.length) return store;
        if (!Array.isArray(store.planTrees)) store.planTrees = [];
        if (!Array.isArray(store.ruleText)) store.ruleText = [];

        for (var si = 0; si < refsByStage.length; si++) {
            var refs = Array.isArray(refsByStage[si]) ? refsByStage[si] : [];
            if (!Array.isArray(store.planTrees[si])) store.planTrees[si] = [];
            if (!Array.isArray(store.ruleText[si])) store.ruleText[si] = [];
            for (var rawIdx = 0; rawIdx < refs.length; rawIdx++) {
                var ref = refs[rawIdx];
                if (!ref || !ref.tileId) continue;
                var payload = searchPayloadAt(store, ref);
                if (!payload) continue;
                var type = store.ruleTypes &&
                    store.ruleTypes[si] &&
                    store.ruleTypes[si][rawIdx] === 'text'
                    ? 'text'
                    : 'plan';
                if (type === 'text') {
                    store.ruleText[si][rawIdx] = String(payload.text || '');
                } else if (payload.tree) {
                    store.planTrees[si][rawIdx] = payload.tree;
                }
            }
        }

        return store;
    }

    function createWorker() {
        if (!canCreateWorker()) return null;
        try {
            var global = traceDataGlobal();
            var host = global.Worker ? global : (global.window || {});
            var blob = new host.Blob([workerSource()], { type: 'text/javascript' });
            var url = host.URL.createObjectURL(blob);
            var worker = new host.Worker(url);
            if (host.URL.revokeObjectURL) host.URL.revokeObjectURL(url);
            return worker;
        } catch (err) {
            return null;
        }
    }

    function snapshotArray(value) {
        return Array.isArray(value) ? value : [];
    }

    function compactStore(store) {
        store = store || {};
        return {
            traceGeneration: store.traceGeneration,
            traceHandle: store.traceHandle,
            stageNames: snapshotArray(store.stageNames),
            groups: snapshotArray(store.groups),
            ruleNames: snapshotArray(store.ruleNames),
            ruleTypes: snapshotArray(store.ruleTypes),
            ruleText: snapshotArray(store.ruleText),
            planTrees: snapshotArray(store.planTrees),
            tilePayloadRefs: snapshotArray(store.tilePayloadRefs),
            payloadBlocks: payloadBlockMap(store.payloadRegistry),
            decodedPayloadBlocks: decodedPayloadBlockMap(store.payloadRegistry)
        };
    }

    function normalizedTraceGeneration(value) {
        value = Number(value);
        return Number.isInteger(value) && value >= 0 ? value : 0;
    }

    function workerError(errorPayload) {
        errorPayload = errorPayload || {};
        var error = new Error(errorPayload.message || 'Trace search worker failed');
        error.name = errorPayload.name || 'Error';
        error.stack = errorPayload.stack || error.stack;
        error.details = errorPayload.details || null;
        return error;
    }

    function buildGlobalSummary(store, query, scope, options) {
        options = options || {};
        if (options.disableWorker) return null;

        var worker = createWorker();
        if (!worker) return null;

        var traceGeneration = normalizedTraceGeneration(
            options.traceGeneration !== undefined ? options.traceGeneration : store && store.traceGeneration
        );
        var state = TraceWorkerProtocol.createState(traceGeneration);
        var request = TraceWorkerProtocol.beginRequest(state, BUILD_GLOBAL_SUMMARY_OPERATION, {
            store: compactStore(store),
            query: String(query || ''),
            scope: String(scope || 'tree-rules')
        });
        var cancelled = false;

        var job = {
            request: request,
            traceGeneration: request.traceGeneration,
            worker: worker,
            cancel: function() {
                cancelled = true;
                if (worker && worker.terminate) worker.terminate();
            },
            promise: null
        };

        job.promise = new Promise(function(resolve, reject) {
            function finish(callback, value) {
                if (cancelled) return;
                cancelled = true;
                if (worker && worker.terminate) worker.terminate();
                callback(value);
            }

            worker.onmessage = function(ev) {
                var response = ev && ev.data || {};
                var completion = TraceWorkerProtocol.completeResponse(state, response);
                if (!completion.accepted) {
                    finish(reject, new Error('Trace search worker response rejected: ' + completion.reason));
                    return;
                }
                if (response.type === 'error') {
                    finish(reject, workerError(response.error));
                    return;
                }
                var responsePayload = response.payload || {};
                finish(resolve, responsePayload.summary || {
                    query: String(query || ''),
                    scope: String(scope || 'tree-rules'),
                    totalCount: 0,
                    ruleOrdinals: [],
                    ruleHandles: [],
                    counts: [],
                    masks: []
                });
            };

            worker.onerror = function(err) {
                finish(reject, new Error(err && err.message ? err.message : 'Trace search worker failed'));
            };

            worker.postMessage(request);
        });

        return job;
    }

    return {
        buildGlobalSummary: buildGlobalSummary,
        BUILD_GLOBAL_SUMMARY_OPERATION: BUILD_GLOBAL_SUMMARY_OPERATION,
        compactStore: compactStore,
        createWorker: createWorker,
        materializeSearchStorePayloads: materializeSearchStorePayloads,
        SUMMARY_FIELD_MASKS: SUMMARY_FIELD_MASKS,
        snapshotStore: createTraceSearchSnapshotStore(),
        workerSource: workerSource
    };
})();
