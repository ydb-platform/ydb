
/* ══════════════════════════════════════════════════════════════
   Active trace data and layout state
   ══════════════════════════════════════════════════════════════ */

var embeddedOptimizerTraceData = null;
var embeddedOptimizerTraceDataLoaded = false;
var embeddedOptimizerTraceDataStatus = { state: 'unloaded', title: '', message: '' };

function traceDataStatusFromData(data) {
    if (data && data.traceDataStatus && typeof data.traceDataStatus === 'object') {
        return data.traceDataStatus;
    }
    var traces = data && Array.isArray(data.traces) ? data.traces : [];
    if (traces[0] && traces[0].traceDataStatus && typeof traces[0].traceDataStatus === 'object') {
        return traces[0].traceDataStatus;
    }
    return null;
}

function traceDataGlobal() {
    if (typeof globalThis !== 'undefined') return globalThis;
    if (typeof window !== 'undefined') return window;
    if (typeof self !== 'undefined') return self;
    return this;
}

function traceDataBase64UrlToBytes(input) {
    input = String(input || '').replace(/\s+/g, '');
    var alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
    var out = [];
    var buffer = 0;
    var bits = 0;

    for (var i = 0; i < input.length; i++) {
        var value = alphabet.indexOf(input.charAt(i));
        if (value < 0) throw new Error('Invalid compressed trace data base64url payload');
        buffer = (buffer << 6) | value;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push((buffer >> bits) & 0xFF);
        }
    }

    return typeof Uint8Array !== 'undefined' ? new Uint8Array(out) : out;
}

function traceDataBytesToString(bytes) {
    if (typeof TextDecoder !== 'undefined') {
        return new TextDecoder('utf-8').decode(bytes);
    }
    var text = '';
    for (var i = 0; i < bytes.length; i++) {
        text += String.fromCharCode(bytes[i]);
    }
    try {
        return decodeURIComponent(escape(text));
    } catch (_err) {
        return text;
    }
}

function parseTraceDataScriptElement(el) {
    var compression = el.getAttribute ? String(el.getAttribute('data-compression') || '') : '';
    if (!compression || compression === 'identity') {
        return JSON.parse(el.textContent || '');
    }
    if (compression !== 'brotli') {
        throw new Error('Unsupported optimizer trace data compression: ' + compression);
    }

    var decoder = traceDataGlobal().OptimizerTraceBrotliDecoder;
    if (!decoder || typeof decoder.decode !== 'function') {
        throw new Error('Brotli-compressed optimizer trace data requires the embedded Brotli decoder');
    }
    var compressed = traceDataBase64UrlToBytes(el.textContent || '');
    var jsonBytes = decoder.decode(compressed);
    return JSON.parse(traceDataBytesToString(jsonBytes));
}

function traceDataHasOwn(obj, key) {
    return !!obj && Object.prototype.hasOwnProperty.call(obj, key);
}

function traceLogString(value, fallback) {
    if (value === undefined || value === null || value === '') return fallback || '';
    return String(value);
}

function traceLogKey(id, name, fallback) {
    return traceLogString(id, traceLogString(name, fallback));
}

function traceLogArray(value) {
    return Array.isArray(value) ? value : [];
}

function traceLogObject(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
}

function traceLogCloneChainContext(context) {
    context = context || {};
    return {
        appendId: traceLogString(context.appendId, ''),
        traceId: traceLogString(context.traceId, ''),
        traceTitle: traceLogString(context.traceTitle, ''),
        fieldDefinitions: traceLogArray(context.fieldDefinitions).slice(),
        pinnedFields: traceLogArray(context.pinnedFields).slice(),
        pinnedFieldPresets: traceLogArray(context.pinnedFieldPresets).slice(),
        diffFieldPresets: traceLogArray(context.diffFieldPresets).slice(),
        stageId: traceLogString(context.stageId, ''),
        stageTitle: traceLogString(context.stageTitle, ''),
        groupId: traceLogString(context.groupId, ''),
        groupTitle: traceLogString(context.groupTitle, '')
    };
}

function traceLogApplyArrayContext(context, source, key) {
    if (source && traceDataHasOwn(source, key)) {
        context[key] = traceLogArray(source[key]).slice();
    }
}

function traceLogApplyNamedContext(context, source, idKey, titleKey, fallbackTitleKey, targetIdKey, targetTitleKey) {
    source = source || {};
    if (traceDataHasOwn(source, idKey)) {
        context[targetIdKey] = traceLogString(source[idKey], context[targetIdKey]);
    }
    if (traceDataHasOwn(source, titleKey) || traceDataHasOwn(source, fallbackTitleKey)) {
        context[targetTitleKey] = traceLogString(
            source[titleKey],
            traceLogString(source[fallbackTitleKey], context[targetTitleKey])
        );
    }
}

function traceLogFindOrCreateTrace(root, traceMap, record) {
    var title = traceLogString(record.traceTitle, traceLogString(record.trace, 'Trace'));
    var key = traceLogKey(record.traceId, record.trace, title);
    var trace = traceMap[key];
    if (!trace) {
        trace = {
            schemaVersion: 2,
            title: title,
            fieldDefinitions: [],
            pinnedFields: [],
            pinnedFieldPresets: [],
            diffFieldPresets: [],
            stages: [],
            _stageMap: {}
        };
        traceMap[key] = trace;
        root.traces.push(trace);
    }

    if (record.traceTitle !== undefined || record.trace !== undefined) {
        trace.title = title;
    }
    if (record.fieldDefinitions !== undefined) {
        trace.fieldDefinitions = traceLogArray(record.fieldDefinitions);
    }
    if (record.pinnedFields !== undefined) {
        trace.pinnedFields = traceLogArray(record.pinnedFields);
    }
    if (record.pinnedFieldPresets !== undefined) {
        trace.pinnedFieldPresets = traceLogArray(record.pinnedFieldPresets);
    }
    if (record.diffFieldPresets !== undefined) {
        trace.diffFieldPresets = traceLogArray(record.diffFieldPresets);
    }

    return trace;
}

function traceLogFindOrCreateStage(trace, record) {
    var name = traceLogString(record.stageTitle, traceLogString(record.stage, 'Stage'));
    var key = traceLogKey(record.stageId, record.stage, name);
    var stage = trace._stageMap[key];
    if (!stage) {
        stage = {
            name: name,
            groups: [],
            tiles: []
        };
        trace._stageMap[key] = stage;
        trace.stages.push(stage);
    }
    if (record.stageTitle !== undefined || record.stage !== undefined) {
        stage.name = name;
    }
    return stage;
}

function traceLogFindOrCreateGroup(stage, record, tile) {
    var fallback = tile && tile.title || 'Rule';
    var name = traceLogString(record.groupTitle, traceLogString(record.group, fallback));
    var id = traceLogString(record.groupId, '');

    var last = stage.groups.length ? stage.groups[stage.groups.length - 1] : null;
    if (last && (id ? last._id === id : last.name === name)) {
        return last;
    }

    var group = {
        name: name,
        tileIndices: []
    };
    if (id) group._id = id;
    stage.groups.push(group);
    return group;
}

function traceLogNodeHasFields(node) {
    if (!node) return false;
    if (Array.isArray(node.fields) && node.fields.length) return true;
    var children = Array.isArray(node.children) ? node.children : [];
    for (var i = 0; i < children.length; i++) {
        if (traceLogNodeHasFields(children[i])) return true;
    }
    return false;
}

function traceLogInfoWidgetHasContent(widget) {
    widget = widget || {};
    var type = String(widget.type || '').toLowerCase();
    if (type === 'text' || type === 'code' || type === 'unwrappedtext') {
        return widget.text !== undefined && widget.text !== null && String(widget.text) !== '';
    }
    if (type === 'table') {
        return Array.isArray(widget.rows) && widget.rows.length > 0;
    }
    if (type === 'list') {
        return Array.isArray(widget.items) && widget.items.some(function(item) {
            if (item && typeof item === 'object') return !!(item.text || item.detail);
            return item !== undefined && item !== null && String(item) !== '';
        });
    }
    if (type === 'warning') {
        return !!(widget.message || widget.details);
    }
    if (type === 'graph' || type === 'graphmodel') {
        var graph = widget.graph || widget;
        return (Array.isArray(graph.nodes) && graph.nodes.length > 0) ||
            (Array.isArray(graph.edges) && graph.edges.length > 0);
    }
    if (type === 'switcher') {
        var options = Array.isArray(widget.options) ? widget.options : [];
        for (var i = 0; i < options.length; i++) {
            var widgets = Array.isArray(options[i] && options[i].widgets)
                ? options[i].widgets
                : [];
            for (var w = 0; w < widgets.length; w++) {
                if (traceLogInfoWidgetHasContent(widgets[w])) return true;
            }
        }
    }
    return false;
}

function traceLogInfoPanelHasContent(infoPanel) {
    var tabs = infoPanel && Array.isArray(infoPanel.tabs) ? infoPanel.tabs : [];
    for (var i = 0; i < tabs.length; i++) {
        var widgets = Array.isArray(tabs[i] && tabs[i].widgets) ? tabs[i].widgets : [];
        for (var w = 0; w < widgets.length; w++) {
            if (traceLogInfoWidgetHasContent(widgets[w])) return true;
        }
    }
    return false;
}

function traceLogTraceFeatureAvailability(trace) {
    var hasFields = false;
    var hasPinnedValues = false;
    var hasInfo = false;
    var pinned = Array.isArray(trace.pinnedFields) ? trace.pinnedFields : [];

    for (var si = 0; si < trace.stages.length; si++) {
        var tiles = Array.isArray(trace.stages[si].tiles) ? trace.stages[si].tiles : [];
        for (var ri = 0; ri < tiles.length; ri++) {
            var tile = tiles[ri] || {};
            var features = traceLogObject(tile.features);
            if (features) {
                if (features.fields) hasFields = true;
                if (features.pinned) hasPinnedValues = true;
                if (features.info) hasInfo = true;
            }
            if (tile.type !== 'text' && traceLogNodeHasFields(tile.tree)) {
                hasFields = true;
                if (pinned.length) hasPinnedValues = true;
            }
            if (tile.type !== 'text' && traceLogInfoPanelHasContent(tile.infoPanel)) {
                hasInfo = true;
            }
        }
    }

    return {
        fields: hasFields,
        pinned: hasPinnedValues,
        info: hasInfo
    };
}

function traceLogFinalizeRoot(root) {
    if (root._payloadRegistry && TracePayloadRegistry.status(root._payloadRegistry).blockCount) {
        root.payloadRegistry = root._payloadRegistry;
    }
    for (var ti = 0; ti < root.traces.length; ti++) {
        var trace = root.traces[ti];
        if (root.payloadRegistry) {
            Object.defineProperty(trace, 'payloadRegistry', {
                value: root.payloadRegistry,
                enumerable: false,
                configurable: true
            });
        }
        trace.featureAvailability = traceLogTraceFeatureAvailability(trace);
        delete trace._stageMap;
        var stages = Array.isArray(trace.stages) ? trace.stages : [];
        for (var si = 0; si < stages.length; si++) {
            var groups = Array.isArray(stages[si].groups) ? stages[si].groups : [];
            for (var gi = 0; gi < groups.length; gi++) {
                delete groups[gi]._id;
            }
        }
    }
    delete root._appendMap;
    delete root._traceLastAppend;
    delete root._payloadRegistry;
    return root.traces.length ? root : null;
}

function traceLogExpandChainedRecord(root, record) {
    if (!root._appendMap) root._appendMap = {};
    if (!root._traceLastAppend) root._traceLastAppend = {};

    var appendId = traceLogString(record.appendId, '');
    if (!appendId) {
        throw new Error('Trace log v3 record appendId must not be empty');
    }
    if (root._appendMap[appendId]) {
        throw new Error('Duplicate trace log appendId: ' + appendId);
    }

    var prevAppendId = traceLogString(record.prevAppendId, '');
    var previous = null;
    if (prevAppendId) {
        previous = root._appendMap[prevAppendId];
        if (!previous) {
            throw new Error('Trace log prevAppendId was not found: ' + prevAppendId);
        }
    }

    var context = traceLogCloneChainContext(previous);
    var trace = traceLogObject(record.trace);
    if (trace) {
        traceLogApplyNamedContext(context, trace, 'id', 'name', 'title', 'traceId', 'traceTitle');
        traceLogApplyArrayContext(context, trace, 'fieldDefinitions');
        traceLogApplyArrayContext(context, trace, 'pinnedFields');
        traceLogApplyArrayContext(context, trace, 'pinnedFieldPresets');
        traceLogApplyArrayContext(context, trace, 'diffFieldPresets');
    } else if (record.trace !== undefined) {
        context.traceTitle = traceLogString(record.trace, context.traceTitle || 'Trace');
        context.traceId = traceLogString(record.traceId, context.traceId || context.traceTitle);
    }
    if (record.traceId !== undefined) {
        context.traceId = traceLogString(record.traceId, context.traceId);
    }
    if (record.traceTitle !== undefined) {
        context.traceTitle = traceLogString(record.traceTitle, context.traceTitle || 'Trace');
    }
    traceLogApplyArrayContext(context, record, 'fieldDefinitions');
    traceLogApplyArrayContext(context, record, 'pinnedFields');
    traceLogApplyArrayContext(context, record, 'pinnedFieldPresets');
    traceLogApplyArrayContext(context, record, 'diffFieldPresets');

    var stage = traceLogObject(record.stage);
    if (stage) {
        traceLogApplyNamedContext(context, stage, 'id', 'name', 'title', 'stageId', 'stageTitle');
    } else if (record.stage !== undefined) {
        context.stageTitle = traceLogString(record.stage, context.stageTitle || 'Stage');
        context.stageId = traceLogString(record.stageId, context.stageId || context.stageTitle);
    }
    if (record.stageId !== undefined) {
        context.stageId = traceLogString(record.stageId, context.stageId);
    }
    if (record.stageTitle !== undefined) {
        context.stageTitle = traceLogString(record.stageTitle, context.stageTitle || 'Stage');
    }

    var group = traceLogObject(record.group);
    if (group) {
        traceLogApplyNamedContext(context, group, 'id', 'name', 'title', 'groupId', 'groupTitle');
    } else if (record.group !== undefined) {
        context.groupTitle = traceLogString(record.group, context.groupTitle || '');
        context.groupId = traceLogString(record.groupId, context.groupId || context.groupTitle);
    }
    if (record.groupId !== undefined) {
        context.groupId = traceLogString(record.groupId, context.groupId);
    }
    if (record.groupTitle !== undefined) {
        context.groupTitle = traceLogString(record.groupTitle, context.groupTitle || '');
    }

    var tile = record.tile || {};
    if (!context.traceId) throw new Error('Trace log v3 record is missing trace id context');
    if (!context.traceTitle) context.traceTitle = 'Trace';
    if (!context.stageId) throw new Error('Trace log v3 record is missing stage id context');
    if (!context.stageTitle) context.stageTitle = 'Stage';
    if (!context.groupTitle) context.groupTitle = traceLogString(tile.title, 'Rule');
    if (!context.groupId) context.groupId = context.groupTitle;

    var lastAppendId = traceLogString(root._traceLastAppend[context.traceId], '');
    if (prevAppendId) {
        if (lastAppendId !== prevAppendId) {
            throw new Error('Trace log v3 record does not extend the current trace chain');
        }
    } else if (lastAppendId) {
        throw new Error('Trace log v3 record starts a second chain for trace: ' + context.traceId);
    }

    context.appendId = appendId;
    root._appendMap[appendId] = traceLogCloneChainContext(context);
    root._traceLastAppend[context.traceId] = appendId;

    return {
        type: record.type,
        traceId: context.traceId,
        traceTitle: context.traceTitle,
        fieldDefinitions: context.fieldDefinitions,
        pinnedFields: context.pinnedFields,
        pinnedFieldPresets: context.pinnedFieldPresets,
        diffFieldPresets: context.diffFieldPresets,
        stageId: context.stageId,
        stageTitle: context.stageTitle,
        groupId: context.groupId,
        groupTitle: context.groupTitle,
        tile: tile,
        ruleTitle: record.ruleTitle,
        rule: record.rule
    };
}

function traceLogPayloadBlockId(block, element, index) {
    var id = traceLogString(block && (block.blockId || block.id), '');
    if (id) return id;
    var seq = element && element.getAttribute ? traceLogString(element.getAttribute('data-seq'), '') : '';
    return seq ? 'payload-block:' + seq : 'payload-block:' + index;
}

function traceLogTilePayloadRawBytes(tile) {
    var size = traceLogObject(tile && tile.size);
    var value = size && size.rawBytes !== undefined ? size.rawBytes : tile && tile.payloadRawBytes;
    value = Math.floor(Number(value));
    return Number.isFinite(value) && value >= 0 ? value : 0;
}

function traceLogRegisterPayloadBlock(root, block, element, index) {
    if (Number(block && block.schemaVersion) !== 3 || !block.payload) return '';
    if (!root._payloadRegistry) root._payloadRegistry = TracePayloadRegistry.create();
    var blockId = traceLogPayloadBlockId(block, element, index);
    TracePayloadRegistry.registerBlock(root._payloadRegistry, {
        id: blockId,
        element: element || null,
        payload: block.payload,
        rawBytes: block.payload && block.payload.rawBytes,
        recordCount: Array.isArray(block.records) ? block.records.length : 0
    });
    return blockId;
}

function traceLogRegisterTilePayload(root, tile, blockId) {
    if (!root._payloadRegistry || !tile || !blockId || tile.payloadIndex === undefined) return;
    var tileId = traceLogString(tile.id, '');
    if (!tileId) return;
    var rawBytes = traceLogTilePayloadRawBytes(tile);
    TracePayloadRegistry.registerTile(root._payloadRegistry, {
        tileId: tileId,
        blockId: blockId,
        payloadIndex: tile.payloadIndex,
        payloadRawBytes: rawBytes
    });
    tile.payloadBlockId = blockId;
    tile.payloadRawBytes = rawBytes;
    if (tile.sourceBlockId === undefined) tile.sourceBlockId = blockId;
}

function replayOptimizerTraceLogBlock(root, traceMap, block, options) {
    options = options || {};
    var schemaVersion = block ? Number(block.schemaVersion) : 0;
    if (!block || schemaVersion !== 3 || !Array.isArray(block.records)) {
        throw new Error('Invalid optimizer trace log block');
    }
    if (schemaVersion > Number(root.schemaVersion || 0)) {
        root.schemaVersion = schemaVersion;
    }
    var payloadBlockId = traceLogRegisterPayloadBlock(root, block, options.element, options.index);

    for (var i = 0; i < block.records.length; i++) {
        var record = block.records[i] || {};
        record = traceLogExpandChainedRecord(root, record);
        var type = traceLogString(record.type, 'tile');
        var trace = traceLogFindOrCreateTrace(root, traceMap, record);
        if (type === 'trace_meta' || type === 'metadata') continue;
        if (type !== 'tile' && type !== 'rule') continue;

        var tile = record.tile || {};
        if (!traceDataHasOwn(tile, 'title')) {
            tile.title = traceLogString(record.ruleTitle, traceLogString(record.rule, 'Rule'));
        }
        traceLogRegisterTilePayload(root, tile, payloadBlockId);
        var stage = traceLogFindOrCreateStage(trace, record);
        var group = traceLogFindOrCreateGroup(stage, record, tile);
        var tileIndex = stage.tiles.length;
        stage.tiles.push(tile);
        group.tileIndices.push(tileIndex);
    }
}

function parseOptimizerTraceLogBlocks() {
    if (!hasDOM() || !document.querySelectorAll) return null;

    var elements = document.querySelectorAll('script.ot-l');
    if (!elements || !elements.length) return null;

    var root = { schemaVersion: 2, traces: [], _appendMap: {}, _traceLastAppend: {} };
    var traceMap = {};
    var parsedAny = false;

    for (var i = 0; i < elements.length; i++) {
        try {
            replayOptimizerTraceLogBlock(root, traceMap, parseTraceDataScriptElement(elements[i]), {
                element: elements[i],
                index: i
            });
            parsedAny = true;
        } catch (err) {
            if (parsedAny) break;
            throw err;
        }
    }

    return traceLogFinalizeRoot(root);
}

function setEmbeddedTraceDataStatus(state, title, message) {
    embeddedOptimizerTraceDataStatus = {
        state: String(state || 'unloaded'),
        title: String(title || ''),
        message: String(message || '')
    };
    return embeddedOptimizerTraceDataStatus;
}

function traceDataLoadStatus() {
    parseEmbeddedTraceData();
    return {
        state: embeddedOptimizerTraceDataStatus.state,
        title: embeddedOptimizerTraceDataStatus.title,
        message: embeddedOptimizerTraceDataStatus.message
    };
}

function parseEmbeddedTraceData() {
    if (embeddedOptimizerTraceDataLoaded) return embeddedOptimizerTraceData;
    embeddedOptimizerTraceDataLoaded = true;

    if (!hasDOM() || !document.getElementById) return null;

    var el = document.getElementById('optimizer-trace-data');
    if (el) {
        try {
            embeddedOptimizerTraceData = parseTraceDataScriptElement(el);
            var jsonStatus = traceDataStatusFromData(embeddedOptimizerTraceData);
            if (jsonStatus) {
                setEmbeddedTraceDataStatus(
                    jsonStatus.state || 'ready',
                    jsonStatus.title || '',
                    jsonStatus.message || ''
                );
            } else {
                setEmbeddedTraceDataStatus('ready');
            }
        } catch (err) {
            var globalObj = traceDataGlobal();
            if (globalObj && globalObj.console && globalObj.console.error) {
                globalObj.console.error('Failed to parse optimizer trace data', err);
            }
            embeddedOptimizerTraceData = null;
            setEmbeddedTraceDataStatus(
                'failed',
                'Trace Data Error',
                err && err.message ? err.message : String(err)
            );
        }
        return embeddedOptimizerTraceData;
    }

    try {
        embeddedOptimizerTraceData = parseOptimizerTraceLogBlocks();
        if (embeddedOptimizerTraceData) {
            setEmbeddedTraceDataStatus('ready');
            return embeddedOptimizerTraceData;
        }
    } catch (err) {
        var globalObj = traceDataGlobal();
        if (globalObj && globalObj.console && globalObj.console.error) {
            globalObj.console.error('Failed to parse optimizer trace log blocks', err);
        }
        embeddedOptimizerTraceData = null;
        setEmbeddedTraceDataStatus(
            'failed',
            'Trace Data Error',
            err && err.message ? err.message : String(err)
        );
        return null;
    }

    setEmbeddedTraceDataStatus('ready');
    return null;
}

function traceDataRoot() {
    var embedded = parseEmbeddedTraceData();
    if (embedded) return embedded;
    return { traces: [] };
}

function traceDataTraces() {
    var data = traceDataRoot();
    return Array.isArray(data.traces) ? data.traces : [];
}

var TracePayloadRegistry = (function() {
    var DEFAULT_MAX_BYTES = 256 * 1024 * 1024;

    function hasOwn(object, key) {
        return Object.prototype.hasOwnProperty.call(object, key);
    }

    function stringId(value) {
        if (value === undefined || value === null) return '';
        return String(value);
    }

    function finiteNonNegative(value, fallback) {
        value = Math.floor(Number(value));
        return Number.isFinite(value) && value >= 0 ? value : fallback;
    }

    function create(options) {
        options = options || {};
        return {
            blocks: {},
            tiles: {},
            cache: {
                maxBytes: finiteNonNegative(options.maxBytes, DEFAULT_MAX_BYTES),
                totalBytes: 0,
                entries: {},
                order: []
            },
            protections: {}
        };
    }

    function registerBlock(registry, block) {
        registry = registry || create();
        block = block || {};
        var blockId = stringId(block.id || block.blockId);
        if (!blockId) {
            blockId = 'payload-block:' + Object.keys(registry.blocks).length;
        }
        var record = {
            id: blockId,
            payload: block.payload || null,
            rawBytes: finiteNonNegative(block.rawBytes, 0),
            recordCount: finiteNonNegative(block.recordCount, 0),
            state: block.state || 'unloaded',
            error: block.error || null
        };
        if (block.element) {
            Object.defineProperty(record, 'element', {
                value: block.element,
                enumerable: false,
                configurable: true
            });
        }
        registry.blocks[blockId] = record;
        return registry.blocks[blockId];
    }

    function registerTile(registry, tile) {
        registry = registry || create();
        tile = tile || {};
        var tileId = stringId(tile.id || tile.tileId);
        var blockId = stringId(tile.blockId || tile.payloadBlockId);
        if (!tileId || !blockId) return null;
        registry.tiles[tileId] = {
            tileId: tileId,
            blockId: blockId,
            payloadIndex: finiteNonNegative(tile.payloadIndex, 0),
            payloadRawBytes: finiteNonNegative(tile.payloadRawBytes, finiteNonNegative(tile.rawBytes, 0))
        };
        return registry.tiles[tileId];
    }

    function block(registry, blockId) {
        blockId = stringId(blockId);
        return registry && registry.blocks && hasOwn(registry.blocks, blockId)
            ? registry.blocks[blockId]
            : null;
    }

    function tile(registry, tileId) {
        tileId = stringId(tileId);
        return registry && registry.tiles && hasOwn(registry.tiles, tileId)
            ? registry.tiles[tileId]
            : null;
    }

    function tilePayloadRef(registry, tileId) {
        var ref = tile(registry, tileId);
        if (!ref) return null;
        var owner = block(registry, ref.blockId);
        return {
            tileId: ref.tileId,
            blockId: ref.blockId,
            payloadIndex: ref.payloadIndex,
            payloadRawBytes: ref.payloadRawBytes,
            blockRawBytes: owner ? owner.rawBytes : 0,
            state: owner ? owner.state : 'missing',
            error: owner ? owner.error : null
        };
    }

    function removeOrderKey(order, key) {
        var index = order.indexOf(key);
        if (index >= 0) order.splice(index, 1);
    }

    function touch(cache, key) {
        removeOrderKey(cache.order, key);
        cache.order.push(key);
    }

    function protectionOwners(registry, blockId) {
        blockId = stringId(blockId);
        if (!blockId) return null;
        if (!registry.protections[blockId]) registry.protections[blockId] = {};
        return registry.protections[blockId];
    }

    function isProtected(registry, blockId) {
        var owners = registry && registry.protections && registry.protections[stringId(blockId)];
        return !!owners && Object.keys(owners).length > 0;
    }

    function protectBlock(registry, blockId, owner) {
        registry = registry || create();
        var owners = protectionOwners(registry, blockId);
        if (!owners) return false;
        owners[stringId(owner) || 'default'] = true;
        return true;
    }

    function unprotectBlock(registry, blockId, owner) {
        var id = stringId(blockId);
        var owners = registry && registry.protections && registry.protections[id];
        if (!owners) return false;
        delete owners[stringId(owner) || 'default'];
        if (!Object.keys(owners).length) delete registry.protections[id];
        evict(registry);
        return true;
    }

    function protectTile(registry, tileId, owner) {
        var ref = tile(registry, tileId);
        return ref ? protectBlock(registry, ref.blockId, owner) : false;
    }

    function unprotectTile(registry, tileId, owner) {
        var ref = tile(registry, tileId);
        return ref ? unprotectBlock(registry, ref.blockId, owner) : false;
    }

    function evict(registry) {
        var cache = registry && registry.cache;
        if (!cache) return;
        var guard = 0;
        while (cache.totalBytes > cache.maxBytes && cache.order.length && guard++ < cache.order.length + 1) {
            var evictIndex = -1;
            for (var i = 0; i < cache.order.length; i++) {
                if (!isProtected(registry, cache.order[i])) {
                    evictIndex = i;
                    break;
                }
            }
            if (evictIndex < 0) break;
            var key = cache.order.splice(evictIndex, 1)[0];
            var entry = cache.entries[key];
            if (!entry) continue;
            cache.totalBytes -= entry.bytes;
            delete cache.entries[key];
            var owner = block(registry, key);
            if (owner && owner.state === 'decoded') owner.state = 'unloaded';
        }
    }

    function setMaxBytes(registry, maxBytes) {
        registry.cache.maxBytes = finiteNonNegative(maxBytes, DEFAULT_MAX_BYTES);
        evict(registry);
        return registry.cache.maxBytes;
    }

    function setDecodedBlock(registry, blockId, value, bytes) {
        registry = registry || create();
        blockId = stringId(blockId);
        if (!blockId) return null;
        if (!block(registry, blockId)) registerBlock(registry, { id: blockId });
        var cache = registry.cache;
        var existing = cache.entries[blockId];
        if (existing) cache.totalBytes -= existing.bytes;
        var entry = {
            blockId: blockId,
            value: value,
            bytes: finiteNonNegative(bytes, 0)
        };
        cache.entries[blockId] = entry;
        cache.totalBytes += entry.bytes;
        touch(cache, blockId);
        registry.blocks[blockId].state = 'decoded';
        registry.blocks[blockId].error = null;
        evict(registry);
        return cache.entries[blockId] || null;
    }

    function decodedBlock(registry, blockId) {
        var cache = registry && registry.cache;
        blockId = stringId(blockId);
        if (!cache || !hasOwn(cache.entries, blockId)) return null;
        touch(cache, blockId);
        return cache.entries[blockId];
    }

    function markFailed(registry, blockId, error) {
        blockId = stringId(blockId);
        if (!blockId) return null;
        var owner = block(registry, blockId) || registerBlock(registry, { id: blockId });
        owner.state = 'failed';
        owner.error = error && error.message ? error.message : String(error || 'Payload decode failed');
        var cached = registry.cache.entries[blockId];
        if (cached) {
            registry.cache.totalBytes -= cached.bytes;
            delete registry.cache.entries[blockId];
            removeOrderKey(registry.cache.order, blockId);
        }
        return owner;
    }

    function parsePayloadEnvelope(payload) {
        payload = payload || {};
        if (payload.compression !== 'brotli') {
            throw new Error('Unsupported trace payload compression: ' + String(payload.compression || ''));
        }
        if (payload.encoding && payload.encoding !== 'base64url') {
            throw new Error('Unsupported trace payload encoding: ' + String(payload.encoding || ''));
        }
        var decoder = traceDataGlobal().OptimizerTraceBrotliDecoder;
        if (!decoder || typeof decoder.decode !== 'function') {
            throw new Error('Brotli-compressed trace payload requires the embedded Brotli decoder');
        }
        var compressed = traceDataBase64UrlToBytes(payload.body || '');
        var jsonBytes = decoder.decode(compressed);
        return JSON.parse(traceDataBytesToString(jsonBytes));
    }

    function decodeBlock(registry, blockId) {
        registry = registry || create();
        blockId = stringId(blockId);
        if (!blockId) return { status: 'failed', error: 'missing-block-id', value: null };
        var cached = decodedBlock(registry, blockId);
        if (cached) return { status: 'decoded', value: cached.value, entry: cached };
        var owner = block(registry, blockId);
        if (!owner) return { status: 'failed', error: 'missing-block', value: null };
        if (owner.state === 'failed') return { status: 'failed', error: owner.error, value: null };
        try {
            var decoded = parsePayloadEnvelope(owner.payload);
            var entry = setDecodedBlock(registry, blockId, decoded, owner.rawBytes);
            return { status: 'decoded', value: entry ? entry.value : decoded, entry: entry };
        } catch (err) {
            markFailed(registry, blockId, err);
            return { status: 'failed', error: err && err.message ? err.message : String(err), value: null };
        }
    }

    function decodedTilePayload(registry, tileId) {
        var ref = tile(registry, tileId);
        if (!ref) return { status: 'missing-tile', value: null };
        var decoded = decodeBlock(registry, ref.blockId);
        if (decoded.status !== 'decoded') return decoded;
        var payloads = decoded.value && Array.isArray(decoded.value.payloads)
            ? decoded.value.payloads
            : [];
        if (ref.payloadIndex < 0 || ref.payloadIndex >= payloads.length) {
            return { status: 'failed', error: 'payload-index-out-of-range', value: null };
        }
        return {
            status: 'decoded',
            blockId: ref.blockId,
            payloadIndex: ref.payloadIndex,
            value: payloads[ref.payloadIndex]
        };
    }

    function decodedEntries(registry) {
        var cache = registry && registry.cache;
        if (!cache) return [];
        return cache.order.map(function(key) {
            return cache.entries[key];
        }).filter(Boolean);
    }

    function status(registry) {
        registry = registry || create();
        return {
            blockCount: Object.keys(registry.blocks).length,
            tileCount: Object.keys(registry.tiles).length,
            decodedCount: registry.cache.order.length,
            totalBytes: registry.cache.totalBytes,
            maxBytes: registry.cache.maxBytes,
            protectedBlockCount: Object.keys(registry.protections).length
        };
    }

    return {
        block: block,
        create: create,
        decodeBlock: decodeBlock,
        decodedBlock: decodedBlock,
        decodedEntries: decodedEntries,
        decodedTilePayload: decodedTilePayload,
        isProtected: isProtected,
        markFailed: markFailed,
        protectBlock: protectBlock,
        protectTile: protectTile,
        registerBlock: registerBlock,
        registerTile: registerTile,
        setDecodedBlock: setDecodedBlock,
        setMaxBytes: setMaxBytes,
        status: status,
        tile: tile,
        tilePayloadRef: tilePayloadRef,
        unprotectBlock: unprotectBlock,
        unprotectTile: unprotectTile
    };
})();

var TracePayloadWorker = (function() {
    var DECODE_BLOCK_OPERATION = 'decode-payload-block';

    function canCreateWorker() {
        return typeof Worker !== 'undefined' &&
            typeof Blob !== 'undefined' &&
            typeof URL !== 'undefined' &&
            !!URL.createObjectURL;
    }

    function normalizedTraceGeneration(value) {
        value = Number(value);
        return Number.isInteger(value) && value >= 0 ? value : 0;
    }

    function workerDecoderAssets() {
        var decoder = traceDataGlobal().OptimizerTraceBrotliDecoder || {};
        return {
            source: typeof decoder.workerSource === 'string' ? decoder.workerSource : '',
            wasmBase64: typeof decoder.wasmBase64 === 'string' ? decoder.wasmBase64 : ''
        };
    }

    function workerSource(decoderSource, wasmBase64) {
        return [
            'var createTraceWorkerProtocol = ' + createTraceWorkerProtocol.toString() + ';',
            'var TraceWorkerProtocol = createTraceWorkerProtocol();',
            'var traceDataBase64UrlToBytes = ' + traceDataBase64UrlToBytes.toString() + ';',
            'var traceDataBytesToString = ' + traceDataBytesToString.toString() + ';',
            decoderSource,
            'if (!self.OptimizerTraceBrotliDecoder || typeof self.OptimizerTraceBrotliDecoder.decode !== "function") {',
            '  throw new Error("Payload worker Brotli decoder is unavailable");',
            '}',
            'if (typeof self.OptimizerTraceBrotliDecoder.setWasmBase64 === "function") {',
            '  self.OptimizerTraceBrotliDecoder.setWasmBase64(' + JSON.stringify(wasmBase64) + ');',
            '}',
            'function parsePayloadEnvelope(payload) {',
            '  payload = payload || {};',
            '  if (payload.compression !== "brotli") {',
            '    throw new Error("Unsupported trace payload compression: " + String(payload.compression || ""));',
            '  }',
            '  if (payload.encoding && payload.encoding !== "base64url") {',
            '    throw new Error("Unsupported trace payload encoding: " + String(payload.encoding || ""));',
            '  }',
            '  var compressed = traceDataBase64UrlToBytes(payload.body || "");',
            '  var jsonBytes = self.OptimizerTraceBrotliDecoder.decode(compressed);',
            '  return JSON.parse(traceDataBytesToString(jsonBytes));',
            '}',
            'self.onmessage = function(ev) {',
            '  var request = ev.data || {};',
            '  try {',
            '    var payload = request.payload || {};',
            '    if (request.operation === ' + JSON.stringify(DECODE_BLOCK_OPERATION) + ') {',
            '      self.postMessage(TraceWorkerProtocol.resultForRequest(request, {',
            '        blockId: payload.blockId || "",',
            '        rawBytes: payload.rawBytes || 0,',
            '        value: parsePayloadEnvelope(payload.payload || {})',
            '      }));',
            '      return;',
            '    }',
            '    throw new Error("Unsupported payload worker operation: " + String(request.operation || ""));',
            '  } catch (err) {',
            '    self.postMessage(TraceWorkerProtocol.errorForRequest(request, err, {',
            '      operation: request.operation,',
            '      sourceBlockId: request.payload && request.payload.blockId || null',
            '    }));',
            '  }',
            '};',
            ''
        ].join('\n');
    }

    function createWorker() {
        if (!canCreateWorker()) return null;
        var assets = workerDecoderAssets();
        if (!assets.source || !assets.wasmBase64) return null;
        try {
            var blob = new Blob([workerSource(assets.source, assets.wasmBase64)], { type: 'text/javascript' });
            var url = URL.createObjectURL(blob);
            var worker = new Worker(url);
            if (URL.revokeObjectURL) URL.revokeObjectURL(url);
            return worker;
        } catch (err) {
            return null;
        }
    }

    function workerError(errorPayload) {
        errorPayload = errorPayload || {};
        var error = new Error(errorPayload.message || 'Trace payload worker failed');
        error.name = errorPayload.name || 'Error';
        error.stack = errorPayload.stack || error.stack;
        error.details = errorPayload.details || null;
        return error;
    }

    function decodeBlock(registry, blockId, options) {
        options = options || {};
        var owner = TracePayloadRegistry.block(registry, blockId);
        if (!owner || owner.state === 'failed') return null;

        var worker = createWorker();
        if (!worker) return null;

        var traceGeneration = normalizedTraceGeneration(options.traceGeneration);
        var state = TraceWorkerProtocol.createState(traceGeneration);
        var request = TraceWorkerProtocol.beginRequest(state, DECODE_BLOCK_OPERATION, {
            blockId: owner.id || blockId,
            rawBytes: owner.rawBytes || 0,
            payload: owner.payload || null
        });
        var cancelled = false;

        var job = {
            request: request,
            traceGeneration: request.traceGeneration,
            blockId: owner.id || blockId,
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
                    finish(reject, new Error('Trace payload worker response rejected: ' + completion.reason));
                    return;
                }
                if (response.type === 'error') {
                    finish(reject, workerError(response.error));
                    return;
                }
                var responsePayload = response.payload || {};
                finish(resolve, {
                    blockId: responsePayload.blockId || job.blockId,
                    rawBytes: responsePayload.rawBytes || owner.rawBytes || 0,
                    value: responsePayload.value
                });
            };

            worker.onerror = function(err) {
                finish(reject, new Error(err && err.message ? err.message : 'Trace payload worker failed'));
            };

            worker.postMessage(request);
        });

        return job;
    }

    return {
        canCreateWorker: canCreateWorker,
        createWorker: createWorker,
        decodeBlock: decodeBlock,
        DECODE_BLOCK_OPERATION: DECODE_BLOCK_OPERATION,
        workerSource: workerSource
    };
})();

/* Clean-core modules. These stay dependency-free so the runtime can be emitted
   as one static script or embedded by the C++ trace writer. */
var TraceSchema = (function() {
    var SUPPORTED_SCHEMA_VERSION = 2;

    function asArray(value) {
        return Array.isArray(value) ? value : [];
    }

    function stringValue(value, fallback) {
        if (value === undefined || value === null) return fallback || '';
        return String(value);
    }

    function normalizeFeatureAvailability(value) {
        if (!value) return null;
        return {
            fields: !!value.fields,
            pinned: !!value.pinned,
            info: !!value.info
        };
    }

    function pairValue(row, keyName, valueName) {
        if (Array.isArray(row)) {
            return [
                stringValue(row[0], ''),
                stringValue(row[1], '')
            ];
        }
        row = row || {};
        return [
            stringValue(row[keyName || 'key'], ''),
            stringValue(row[valueName || 'value'], '')
        ];
    }

    function normalizeInfoPartMonospace(source, specs) {
        function readFlag(obj, names) {
            for (var i = 0; i < names.length; i++) {
                if (obj && obj[names[i]] === true) return true;
            }
            return false;
        }

        function enableToken(result, token) {
            var value = stringValue(token, '').toLowerCase();
            if (value === 'both' || value === 'all') {
                for (var i = 0; i < specs.length; i++) result[specs[i].key] = true;
                return;
            }
            for (var j = 0; j < specs.length; j++) {
                var spec = specs[j];
                for (var k = 0; k < spec.tokens.length; k++) {
                    if (value === spec.tokens[k]) {
                        result[spec.key] = true;
                        return;
                    }
                }
            }
        }

        var result = {};
        for (var p = 0; p < specs.length; p++) {
            result[specs[p].key] = readFlag(source, specs[p].names);
        }

        var option = source && (source.monospace !== undefined ? source.monospace : source.mono);
        if (option === true) {
            for (var t = 0; t < specs.length; t++) result[specs[t].key] = true;
        } else if (typeof option === 'string') {
            enableToken(result, option);
        } else if (Array.isArray(option)) {
            for (var i = 0; i < option.length; i++) {
                enableToken(result, option[i]);
            }
        } else if (option && typeof option === 'object') {
            for (var s = 0; s < specs.length; s++) {
                var optionSpec = specs[s];
                for (var n = 0; n < optionSpec.objectNames.length; n++) {
                    if (option[optionSpec.objectNames[n]]) {
                        result[optionSpec.key] = true;
                        break;
                    }
                }
            }
        }
        return result;
    }

    function normalizeInfoTableMonospace(section) {
        return normalizeInfoPartMonospace(section, [
            {
                key: 'key',
                names: ['monospaceKey', 'monospaceKeys', 'monoKey', 'monoKeys'],
                tokens: ['key', 'keys'],
                objectNames: ['key', 'keys']
            },
            {
                key: 'value',
                names: ['monospaceValue', 'monospaceValues', 'monoValue', 'monoValues'],
                tokens: ['value', 'values'],
                objectNames: ['value', 'values']
            }
        ]);
    }

    function normalizeInfoListMonospace(section) {
        return normalizeInfoPartMonospace(section, [
            {
                key: 'text',
                names: ['monospaceText', 'monospaceTexts', 'monoText', 'monoTexts'],
                tokens: ['text', 'texts'],
                objectNames: ['text', 'texts']
            },
            {
                key: 'detail',
                names: ['monospaceDetail', 'monospaceDetails', 'monoDetail', 'monoDetails'],
                tokens: ['detail', 'details'],
                objectNames: ['detail', 'details']
            }
        ]);
    }

    function normalizeInfoGraphMonospace(section, graph) {
        var sectionMonospace = normalizeInfoPartMonospace(section, [
            {
                key: 'node',
                names: ['monospaceNode', 'monospaceNodes', 'monoNode', 'monoNodes'],
                tokens: ['node', 'nodes'],
                objectNames: ['node', 'nodes']
            },
            {
                key: 'edge',
                names: ['monospaceEdge', 'monospaceEdges', 'monoEdge', 'monoEdges'],
                tokens: ['edge', 'edges'],
                objectNames: ['edge', 'edges']
            }
        ]);
        var graphMonospace = normalizeInfoPartMonospace(graph, [
            {
                key: 'node',
                names: ['monospaceNode', 'monospaceNodes', 'monoNode', 'monoNodes'],
                tokens: ['node', 'nodes'],
                objectNames: ['node', 'nodes']
            },
            {
                key: 'edge',
                names: ['monospaceEdge', 'monospaceEdges', 'monoEdge', 'monoEdges'],
                tokens: ['edge', 'edges'],
                objectNames: ['edge', 'edges']
            }
        ]);
        return {
            node: sectionMonospace.node || graphMonospace.node,
            edge: sectionMonospace.edge || graphMonospace.edge
        };
    }

    function fieldRowKey(row) {
        if (Array.isArray(row)) return stringValue(row[0], '');
        row = row || {};
        return stringValue(row.key, '');
    }

    function fieldRowValue(row) {
        if (Array.isArray(row)) return stringValue(row[1], '');
        row = row || {};
        return stringValue(row.value, '');
    }

    function fieldRowDetails(row) {
        if (!row || Array.isArray(row)) return [];
        return Array.isArray(row.details) ? row.details : [];
    }

    function fieldRowFieldKey(row) {
        row = row || {};
        return stringValue(row.fieldKey, '');
    }

    function fieldRowHasDetails(row) {
        return fieldRowDetails(row).length > 0;
    }

    function hasOwn(obj, key) {
        return !!obj && Object.prototype.hasOwnProperty.call(obj, key);
    }

    function normalizeInfoSectionType(rawType) {
        rawType = stringValue(rawType, '').toLowerCase();
        if (rawType === 'table' ||
            rawType === 'tab' ||
            rawType === 'list' ||
            rawType === 'warning') {
            return rawType;
        }
        if (rawType === 'switcher') return 'switcher';
        return 'text';
    }

    function isUnwrappedInfoTextType(rawType) {
        rawType = stringValue(rawType, '').toLowerCase();
        return rawType === 'unwrappedtext' ||
            rawType === 'textunwrapped' ||
            rawType === 'unwrapped-text' ||
            rawType === 'text-unwrapped' ||
            rawType === 'text_nowrap' ||
            rawType === 'nowraptext' ||
            rawType === 'nowrap-text';
    }

    function infoContext(path, context) {
        context = context || {};
        return {
            traceIndex: Number.isInteger(context.traceIndex) ? context.traceIndex : 0,
            stageIndex: Number.isInteger(context.stageIndex) ? context.stageIndex : -1,
            ruleIndex: Number.isInteger(context.ruleIndex) ? context.ruleIndex : -1,
            path: path || ''
        };
    }

    function infoTargetError(code, path, message, context, value) {
        var error = infoContext(path, context);
        error.code = code;
        error.message = message;
        if (value !== undefined) error.value = stringValue(value, '');
        return error;
    }

    function asTargetArray(value) {
        if (value === undefined || value === null) return [];
        return Array.isArray(value) ? value : [value];
    }

    function inferInfoTargetType(target) {
        var rawType = stringValue(target.type, '').toLowerCase();
        if (rawType === 'subtree') return 'subtree';
        if (rawType === 'node') return 'node';
        if (rawType === 'rule') return 'rule';
        if (rawType) return rawType;
        if (hasOwn(target, 'nodeId')) return 'node';
        return '';
    }

    function normalizeInfoTarget(target, context, path) {
        if (!target || typeof target !== 'object') {
            return {
                error: infoTargetError(
                    'invalid_info_target',
                    path,
                    'Info target must be an object',
                    context
                )
            };
        }

        var type = inferInfoTargetType(target);
        if (!type) {
            return {
                error: infoTargetError(
                    'missing_info_target_type',
                    path,
                    'Info target must declare type or canonical target fields',
                    context
                )
            };
        }

        if (type === 'node' || type === 'subtree') {
            var nodeId = stringValue(target.nodeId, '');
            if (!nodeId) {
                return {
                    error: infoTargetError(
                        'invalid_node_target',
                        path,
                        'Node and subtree info targets require a non-empty nodeId',
                        context
                    )
                };
            }
            return { target: { type: type, nodeId: nodeId } };
        }

        return {
            error: infoTargetError(
                'invalid_info_target',
                path,
                'Info target type is not supported',
                context,
                type
            )
        };
    }

    function normalizeInfoTargets(value, context, path) {
        var rawTargets = asTargetArray(value);
        var result = { targets: [], errors: [] };
        for (var i = 0; i < rawTargets.length; i++) {
            var normalized = normalizeInfoTarget(rawTargets[i], context, path + '[' + i + ']');
            if (normalized.target) result.targets.push(normalized.target);
            if (normalized.error) result.errors.push(normalized.error);
        }
        return result;
    }

    function attachInfoTargets(normalized, section, context, path) {
        var errors = [];
        var targets = normalizeInfoTargets(section.targets, context, path + '.targets');
        if (targets.targets.length) normalized.targets = targets.targets;
        errors = errors.concat(targets.errors);

        if (section.primaryTarget !== undefined) {
            var primary = normalizeInfoTarget(
                section.primaryTarget,
                context,
                path + '.primaryTarget'
            );
            if (primary.target) normalized.primaryTarget = primary.target;
            if (primary.error) errors.push(primary.error);
        }

        if (errors.length) normalized.targetErrors = errors;
    }

    function normalizeGraphModelNode(node, index, context, path) {
        node = node || {};
        var normalized = {
            id: stringValue(node.id, 'node-' + index),
            label: stringValue(node.title, '')
        };
        var text = stringValue(node.text, '');
        if (text) normalized.text = text;
        if (node.monospace === true || node.mono === true) normalized.monospace = true;
        attachInfoTargets(normalized, node, context, path);
        return normalized;
    }

    function normalizeGraphModelEdge(edge, index, context, path) {
        edge = edge || {};
        var normalized = {
            from: stringValue(edge.from, ''),
            to: stringValue(edge.to, '')
        };
        var id = stringValue(edge.id, '');
        if (id) normalized.id = id;
        var label = stringValue(edge.label, '');
        if (label) normalized.label = label;
        var text = stringValue(edge.text, '');
        if (text) normalized.text = text;
        if (edge.monospace === true || edge.mono === true) normalized.monospace = true;
        attachInfoTargets(normalized, edge, context, path);
        return normalized;
    }

    function normalizeFieldRow(field, context, path) {
        if (!field || typeof field !== 'object') return null;
        var key = stringValue(field.key, '');
        if (!key) return null;
        var details = normalizeInfoSections(field.details, 0, context, path + '.details');
        var row = {
            key: key,
            value: stringValue(field.value, '')
        };
        if (details.length) row.details = details;
        return row;
    }

    function normalizePlanNode(node, context, path) {
        node = node || {};
        context = context || {};
        path = path || 'tree';
        var normalized = {
            l: stringValue(node.label, ''),
            c: []
        };
        var nodeId = stringValue(node.id, '');
        if (nodeId) normalized.id = nodeId;
        var op = stringValue(node.op, '');
        if (op) normalized.op = op;

        var fields = asArray(node.fields);
        if (fields.length) {
            var normalizedFields = [];
            for (var f = 0; f < fields.length; f++) {
                var fieldRow = normalizeFieldRow(fields[f], context, path + '.fields[' + f + ']');
                if (fieldRow) normalizedFields.push(fieldRow);
            }
            if (normalizedFields.length) {
                normalized.a = normalizedFields;
            }
        }

        var children = asArray(node.children);
        for (var i = 0; i < children.length; i++) {
            normalized.c.push(normalizePlanNode(
                children[i],
                context,
                path + '.c[' + i + ']'
            ));
        }
        return normalized;
    }

    function normalizeTilePayload(payload, type, context, path) {
        payload = payload || {};
        type = type === 'text' ? 'text' : 'plan';
        context = context || {};
        path = path || 'tilePayload';
        if (type === 'text') {
            return {
                type: 'text',
                text: stringValue(payload.text, '')
            };
        }
        return {
            type: 'plan',
            tree: normalizePlanNode(
                payload.tree || { label: '', children: [] },
                context,
                path + '.tree'
            ),
            info: normalizeInfoSections(
                infoPanelTabsAsSections(payload.infoPanel),
                0,
                context,
                path + '.infoPanel'
            )
        };
    }

    function normalizeNodeField(field) {
        if (field && typeof field === 'object') {
            var key = stringValue(field.key, '');
            if (!key) return null;
            return {
                key: key,
                label: stringValue(field.label, key)
            };
        }
        return null;
    }

    function collectNodeFieldKeysFromTree(node, seen, fields) {
        if (!node || typeof node !== 'object') return;
        var attrs = asArray(node.a);
        for (var i = 0; i < attrs.length; i++) {
            var key = fieldRowKey(attrs[i]);
            if (!key || seen[key]) continue;
            seen[key] = true;
            fields.push({
                key: key,
                label: key
            });
        }
        var children = asArray(node.c);
        for (var c = 0; c < children.length; c++) {
            collectNodeFieldKeysFromTree(children[c], seen, fields);
        }
    }

    function collectNodeFieldKeys(planTrees, seen, fields) {
        planTrees = asArray(planTrees);
        for (var si = 0; si < planTrees.length; si++) {
            var stageTrees = asArray(planTrees[si]);
            for (var ri = 0; ri < stageTrees.length; ri++) {
                collectNodeFieldKeysFromTree(stageTrees[ri], seen, fields);
            }
        }
    }

    function normalizeNodeFields(trace, planTrees) {
        var fields = [];
        var seen = {};

        function addField(field) {
            if (!field || !field.key || seen[field.key]) return;
            seen[field.key] = true;
            fields.push({
                key: field.key,
                label: field.label || field.key
            });
        }

        var explicit = asArray(trace.fieldDefinitions);
        for (var i = 0; i < explicit.length; i++) {
            addField(normalizeNodeField(explicit[i]));
        }

        collectNodeFieldKeys(planTrees, seen, fields);

        return fields;
    }

    function normalizePinnedFieldKeyList(rawKeys, fields) {
        var available = {};
        for (var i = 0; i < fields.length; i++) available[fields[i].key] = true;

        var keys = [];
        var seen = {};
        rawKeys = asArray(rawKeys);
        for (var k = 0; k < rawKeys.length; k++) {
            var key = stringValue(rawKeys[k], '');
            if (!key || !available[key] || seen[key]) continue;
            seen[key] = true;
            keys.push(key);
        }
        return keys;
    }

    function normalizePinnedFieldKeys(trace, fields) {
        return normalizePinnedFieldKeyList(trace && trace.pinnedFields, fields);
    }

    function fieldPresetRawKeys(preset) {
        if (!preset || typeof preset !== 'object') return [];
        if (Object.prototype.hasOwnProperty.call(preset, 'keys')) return preset.keys;
        if (Object.prototype.hasOwnProperty.call(preset, 'fields')) return preset.fields;
        if (Object.prototype.hasOwnProperty.call(preset, 'fieldKeys')) return preset.fieldKeys;
        if (Object.prototype.hasOwnProperty.call(preset, 'pinnedFields')) return preset.pinnedFields;
        return [];
    }

    function normalizeFieldPresets(rawPresets, fields) {
        rawPresets = asArray(rawPresets);
        var presets = [];
        for (var i = 0; i < rawPresets.length; i++) {
            var preset = rawPresets[i];
            if (!preset || typeof preset !== 'object') continue;
            var label = stringValue(preset.label || preset.name || preset.title, '');
            var keys = normalizePinnedFieldKeyList(fieldPresetRawKeys(preset), fields);
            if (!label || !keys.length) continue;
            presets.push({ label: label, keys: keys });
        }
        return presets;
    }

    function normalizePinnedFieldPresets(trace, fields) {
        return normalizeFieldPresets(trace && trace.pinnedFieldPresets, fields);
    }

    function normalizeDiffFieldPresets(trace, fields) {
        return normalizeFieldPresets(trace && trace.diffFieldPresets, fields);
    }

    function normalizeInfoSwitcherOption(option, index, depth, context, path) {
        option = option || {};
        var rawWidgets = asArray(option.widgets);
        var widgets = rawWidgets.length
            ? normalizeInfoSections(rawWidgets, depth + 1, context, path + '.widgets')
            : [];
        return {
            id: stringValue(option.id, 'option-' + index),
            title: stringValue(option.title, ''),
            widget: widgets[0] || normalizeInfoSection({}, depth + 1, context, path + '.widgets[0]'),
            widgets: widgets
        };
    }

    function normalizeInfoListItem(item) {
        if (Array.isArray(item)) {
            return {
                text: stringValue(item[0], ''),
                detail: stringValue(item[1], '')
            };
        }
        if (item && typeof item === 'object') {
            return {
                text: stringValue(
                    item.text !== undefined ? item.text :
                        (item.title !== undefined ? item.title : item.label),
                    ''
                ),
                detail: stringValue(
                    item.detail !== undefined ? item.detail :
                        (item.description !== undefined ? item.description : item.value),
                    ''
                )
            };
        }
        return {
            text: stringValue(item, ''),
            detail: ''
        };
    }

    function normalizeInfoSection(section, depth, context, path) {
        section = section || {};
        depth = Math.max(0, Number(depth) || 0);
        context = context || {};
        path = path || 'info';
        var rawType = stringValue(section.type, '').toLowerCase();
        var type = normalizeInfoSectionType(section.type);
        var normalized = {
            type: type,
            title: stringValue(section.title, '')
        };
        attachInfoTargets(normalized, section, context, path);

        if (rawType === 'graph' && section.graph) {
            normalized.type = 'graphModel';
            var graph = section.graph || {};
            normalized.layout = graph.layout && typeof graph.layout === 'object'
                ? {
                    rankdir: stringValue(graph.layout.rankdir, ''),
                    ranksep: stringValue(graph.layout.ranksep, ''),
                    nodesep: stringValue(graph.layout.nodesep, '')
                }
                : {};
            if (graph.directed === false) normalized.directed = false;
            var graphMonospace = normalizeInfoGraphMonospace(section, graph);
            if (graphMonospace.node || graphMonospace.edge) normalized.monospace = graphMonospace;
            normalized.nodes = asArray(graph.nodes).map(function(node, i) {
                return normalizeGraphModelNode(node, i, context, path + '.graph.nodes[' + i + ']');
            });
            normalized.edges = asArray(graph.edges).map(function(edge, i) {
                return normalizeGraphModelEdge(edge, i, context, path + '.graph.edges[' + i + ']');
            });
        } else if (type === 'table') {
            normalized.rows = asArray(section.rows)
                .map(function(row) {
                    return pairValue(row, 'key', 'value');
                });
            var tableMonospace = normalizeInfoTableMonospace(section);
            if (tableMonospace.key || tableMonospace.value) normalized.monospace = tableMonospace;
        } else if (type === 'list') {
            normalized.ordered = section.ordered === true;
            normalized.layout = stringValue(section.layout, 'flow').toLowerCase() === 'stack'
                ? 'stack'
                : 'flow';
            var listMonospace = normalizeInfoListMonospace(section);
            if (listMonospace.text || listMonospace.detail) normalized.monospace = listMonospace;
            normalized.items = asArray(section.items).map(normalizeInfoListItem);
        } else if (type === 'tab') {
            normalized.id = stringValue(section.id, '');
            normalized.hoverMode = stringValue(section.hoverMode, 'preview').toLowerCase() || 'preview';
            normalized.sections = normalizeInfoSections(
                section.widgets,
                depth + 1,
                context,
                path + '.widgets'
            );
        } else if (type === 'warning') {
            var severity = stringValue(section.severity, 'warning').toLowerCase();
            normalized.severity = severity === 'info' || severity === 'warning' || severity === 'error'
                ? severity : 'warning';
            normalized.message = stringValue(section.message, '');
            normalized.details = stringValue(section.details, '');
        } else if (type === 'switcher') {
            normalized.id = stringValue(section.id, '');
            normalized.defaultOptionId = stringValue(
                section.defaultOptionId,
                ''
            );
            normalized.selectorStyle = stringValue(section.selectorStyle, 'segmented');
            normalized.showChildTitles = section.showChildTitles === true;
            normalized.options = asArray(section.options).map(function(option, i) {
                return normalizeInfoSwitcherOption(option, i, depth, context, path + '.options[' + i + ']');
            });
        } else {
            normalized.content = stringValue(section.text, '');
            normalized.wrap = !isUnwrappedInfoTextType(rawType) && section.wrap !== false;
            normalized.lineNumbers = normalized.wrap === false && section.lineNumbers === true;
        }
        return normalized;
    }

    function normalizeInfoSections(sections, depth, context, path) {
        return asArray(sections).map(function(section, index) {
            return normalizeInfoSection(section, depth || 0, context, (path || 'info') + '[' + index + ']');
        });
    }

    function normalizedInfoSectionHasContent(section) {
        if (!section || typeof section !== 'object') return false;

        if (section.type === 'tab') {
            return normalizedInfoSectionsHaveContent(section.sections);
        }
        if (section.type === 'switcher') {
            var options = Array.isArray(section.options) ? section.options : [];
            for (var i = 0; i < options.length; i++) {
                var widgets = options[i] && Array.isArray(options[i].widgets) ? options[i].widgets : [];
                if (normalizedInfoSectionsHaveContent(widgets)) return true;
            }
            return false;
        }
        if (section.type === 'text') {
            return !!section.content;
        }
        if (section.type === 'table') {
            return Array.isArray(section.rows) && section.rows.length > 0;
        }
        if (section.type === 'list') {
            return Array.isArray(section.items) && section.items.some(function(item) {
                return !!(item && (item.text || item.detail));
            });
        }
        if (section.type === 'warning') {
            return !!(section.message || section.details);
        }
        if (section.type === 'graphModel') {
            return (Array.isArray(section.nodes) && section.nodes.length > 0) ||
                (Array.isArray(section.edges) && section.edges.length > 0);
        }
        return section.type === 'fieldDetailsHeader' ||
            section.type === 'fieldDetailsTable' ||
            section.type === 'fieldDetailsGroup';
    }

    function normalizedInfoSectionsHaveContent(sections) {
        sections = Array.isArray(sections) ? sections : [];
        for (var i = 0; i < sections.length; i++) {
            if (normalizedInfoSectionHasContent(sections[i])) return true;
        }
        return false;
    }

    function normalizeRuleType(rule) {
        rule = rule || {};
        var raw = stringValue(rule.type, '').toLowerCase();
        if (raw === 'text') return 'text';
        if (rule.text !== undefined && rule.tree === undefined) {
            return 'text';
        }
        return 'plan';
    }

    function normalizeRuleText(rule) {
        rule = rule || {};
        return stringValue(rule.text, '');
    }

    function buildConsecutiveGroups(ruleNames) {
        var result = [];
        for (var ri = 0; ri < ruleNames.length; ) {
            var name = ruleNames[ri] || ('Rule ' + (ri + 1));
            var group = { name: name, ri: [ri] };
            ri++;
            while (ri < ruleNames.length && ruleNames[ri] === name) {
                group.ri.push(ri);
                ri++;
            }
            result.push(group);
        }
        return result;
    }

    function normalizeGroups(rawGroups, ruleNames, ruleCount) {
        rawGroups = asArray(rawGroups);
        if (!rawGroups.length && ruleCount > 0) return buildConsecutiveGroups(ruleNames);

        var groups = [];
        for (var gi = 0; gi < rawGroups.length; gi++) {
            var raw = rawGroups[gi] || {};
            var rawIndices = asArray(raw.tileIndices);
            var indices = [];
            var seen = {};
            for (var ri = 0; ri < rawIndices.length; ri++) {
                var index = Number(rawIndices[ri]);
                if (!Number.isInteger(index) || index < 0 || index >= ruleCount || seen[index]) {
                    continue;
                }
                seen[index] = true;
                indices.push(index);
            }
            if (!indices.length) continue;

            var fallbackName = ruleNames[indices[0]] || ('Group ' + (groups.length + 1));
            groups.push({
                name: stringValue(raw.name, fallbackName),
                ri: indices
            });
        }
        return groups;
    }

    function normalizeStageParts(
            name,
            rawGroups,
            rawRuleNames,
            rawTrees,
            rawInfo,
            rawTypes,
            rawText,
            rawSourceBlockIds,
            rawPayloadRefs,
            rawTileFeatures,
            context) {
        rawRuleNames = asArray(rawRuleNames);
        rawTrees = asArray(rawTrees);
        rawInfo = asArray(rawInfo);
        rawTypes = asArray(rawTypes);
        rawText = asArray(rawText);
        rawSourceBlockIds = asArray(rawSourceBlockIds);
        rawPayloadRefs = asArray(rawPayloadRefs);
        rawTileFeatures = asArray(rawTileFeatures);
        context = context || {};

        var ruleCount = Math.max(
            rawRuleNames.length,
            rawTrees.length,
            rawInfo.length,
            rawTypes.length,
            rawText.length,
            rawSourceBlockIds.length,
            rawPayloadRefs.length,
            rawTileFeatures.length
        );

        var ruleNames = [];
        var planTrees = [];
        var ruleInfo = [];
        var ruleTypes = [];
        var ruleText = [];
        var sourceBlockIds = [];
        var tilePayloadRefs = [];
        var tileFeatures = [];
        for (var ri = 0; ri < ruleCount; ri++) {
            var type = rawTypes[ri] === 'text' ? 'text' : 'plan';
            var hasPayloadRef = !!(rawPayloadRefs[ri] && rawPayloadRefs[ri].tileId);
            ruleNames[ri] = stringValue(rawRuleNames[ri], 'Rule ' + (ri + 1));
            ruleTypes[ri] = type;
            ruleText[ri] = type === 'text' && !hasPayloadRef ? stringValue(rawText[ri], '') : '';
            var ruleContext = {
                traceIndex: Number.isInteger(context.traceIndex) ? context.traceIndex : 0,
                stageIndex: Number.isInteger(context.stageIndex) ? context.stageIndex : -1,
                ruleIndex: ri
            };
            planTrees[ri] = type === 'text'
                ? null
                : (hasPayloadRef && rawTrees[ri] === undefined ? null : normalizePlanNode(
                    rawTrees[ri] || { label: ruleNames[ri], children: [] },
                    ruleContext,
                    'stages[' + (Number.isInteger(context.stageIndex) ? context.stageIndex : -1) +
                        '].tiles[' + ri + '].tree'
                ));
            ruleInfo[ri] = type === 'text'
                ? []
                : (hasPayloadRef && rawInfo[ri] === undefined ? [] : normalizeInfoSections(
                    rawInfo[ri],
                    0,
                    ruleContext,
                    'stages[' + (Number.isInteger(context.stageIndex) ? context.stageIndex : -1) +
                        '].tiles[' + ri + '].infoPanel'
                ));
            sourceBlockIds[ri] = stringValue(rawSourceBlockIds[ri], '');
            tilePayloadRefs[ri] = rawPayloadRefs[ri] || null;
            tileFeatures[ri] = rawTileFeatures[ri] || null;
        }

        return {
            name: stringValue(name, ''),
            groups: normalizeGroups(rawGroups, ruleNames, ruleCount),
            ruleNames: ruleNames,
            planTrees: planTrees,
            ruleInfo: ruleInfo,
            ruleTypes: ruleTypes,
            ruleText: ruleText,
            sourceBlockIds: sourceBlockIds,
            tilePayloadRefs: tilePayloadRefs,
            tileFeatures: tileFeatures
        };
    }

    function infoPanelTabsAsSections(infoPanel) {
        var tabs = infoPanel && Array.isArray(infoPanel.tabs) ? infoPanel.tabs : [];
        return tabs.map(function(tab) {
            tab = tab || {};
            var section = {};
            for (var key in tab) {
                if (hasOwn(tab, key)) section[key] = tab[key];
            }
            section.type = 'tab';
            if (section.sections === undefined && section.widgets !== undefined) {
                section.sections = section.widgets;
            }
            return section;
        });
    }

    function ruleInfoSections(rule, type) {
        if (type === 'text') return [];
        if (rule && rule.infoPanel && Array.isArray(rule.infoPanel.tabs)) {
            return infoPanelTabsAsSections(rule.infoPanel);
        }
        return [];
    }

    function tilePayloadRef(rule) {
        rule = rule || {};
        var tileId = stringValue(rule.id, '');
        var blockId = stringValue(rule.payloadBlockId, '');
        if (!tileId || !blockId || rule.payloadIndex === undefined) return null;
        return {
            tileId: tileId,
            blockId: blockId,
            payloadIndex: Math.max(0, Math.floor(Number(rule.payloadIndex)) || 0),
            payloadRawBytes: Math.max(0, Math.floor(Number(rule.payloadRawBytes)) || 0)
        };
    }

    function tileFeatureSummary(rule, type) {
        var features = rule && rule.features && typeof rule.features === 'object'
            ? rule.features
            : null;
        if (!features) return null;
        type = type === 'text' ? 'text' : 'plan';
        return {
            tree: type === 'plan',
            fields: type === 'plan' && !!features.fields,
            pinned: type === 'plan' && !!features.pinned,
            info: type === 'plan' && !!features.info,
            text: type === 'text',
            diff: type === 'plan'
        };
    }

    function normalizeCanonicalTrace(trace, index) {
        var rawStages = asArray(trace.stages);
        if (!rawStages.length) {
            rawStages = [{
                name: 'No stages',
                groups: [],
                tiles: []
            }];
        }
        var stageNames = [];
        var groups = [];
        var ruleNames = [];
        var planTrees = [];
        var ruleInfo = [];
        var ruleTypes = [];
        var ruleText = [];
        var sourceBlockIds = [];
        var tilePayloadRefs = [];
        var tileFeatures = [];
        var traceSourceBlockId = trace.sourceBlockId !== undefined ? trace.sourceBlockId : '';

        for (var si = 0; si < rawStages.length; si++) {
            var stage = rawStages[si] || {};
            var tiles = asArray(stage.tiles);
            var rawRuleNames = [];
            var rawTrees = [];
            var rawInfo = [];
            var rawTypes = [];
            var rawText = [];
            var rawSourceBlockIds = [];
            var rawPayloadRefs = [];
            var rawTileFeatures = [];
            var stageSourceBlockId = stage.sourceBlockId !== undefined ? stage.sourceBlockId : traceSourceBlockId;

            for (var ri = 0; ri < tiles.length; ri++) {
                var rule = tiles[ri] || {};
                var type = normalizeRuleType(rule);
                rawRuleNames[ri] = rule.title;
                rawTypes[ri] = type;
                rawText[ri] = type === 'text' ? normalizeRuleText(rule) : '';
                rawTrees[ri] = rule.tree;
                rawInfo[ri] = ruleInfoSections(rule, type);
                rawSourceBlockIds[ri] = rule.sourceBlockId !== undefined ? rule.sourceBlockId : stageSourceBlockId;
                rawPayloadRefs[ri] = tilePayloadRef(rule);
                rawTileFeatures[ri] = tileFeatureSummary(rule, type);
            }

            var normalized = normalizeStageParts(
                stage.name !== undefined ? stage.name : stage.title,
                stage.groups,
                rawRuleNames,
                rawTrees,
                rawInfo,
                rawTypes,
                rawText,
                rawSourceBlockIds,
                rawPayloadRefs,
                rawTileFeatures,
                {
                    traceIndex: index,
                    stageIndex: si
                }
            );
            stageNames[si] = normalized.name || ('Stage ' + (si + 1));
            groups[si] = normalized.groups;
            ruleNames[si] = normalized.ruleNames;
            planTrees[si] = normalized.planTrees;
            ruleInfo[si] = normalized.ruleInfo;
            ruleTypes[si] = normalized.ruleTypes;
            ruleText[si] = normalized.ruleText;
            sourceBlockIds[si] = normalized.sourceBlockIds;
            tilePayloadRefs[si] = normalized.tilePayloadRefs;
            tileFeatures[si] = normalized.tileFeatures;
        }

        return makeTraceDocument(
            trace,
            index,
            stageNames,
            groups,
            ruleNames,
            planTrees,
            ruleInfo,
            ruleTypes,
            ruleText,
            sourceBlockIds,
            tilePayloadRefs,
            tileFeatures
        );
    }

    function normalizedSchemaVersion(value) {
        if (value === undefined || value === null || value === '') return null;
        value = Number(value);
        return Number.isInteger(value) && value >= 0 ? value : null;
    }

    function schemaVersionLabel(version) {
        return version === null ? 'missing or invalid' : String(version);
    }

    function hasSupportedSchema(trace) {
        var version = normalizedSchemaVersion(trace && trace.schemaVersion);
        return version === SUPPORTED_SCHEMA_VERSION;
    }

    function unsupportedSchemaTrace(trace, index) {
        var version = normalizedSchemaVersion(trace && trace.schemaVersion);
        var title = stringValue(trace && trace.title, 'Trace ' + (Number(index) + 1 || 1));
        var message = 'Unsupported optimizer trace schema version ' + schemaVersionLabel(version) +
            '. This viewer supports schema version ' + SUPPORTED_SCHEMA_VERSION + '.';
        return makeTraceDocument(
            {
                schemaVersion: SUPPORTED_SCHEMA_VERSION,
                title: 'Unsupported Trace Schema'
            },
            index,
            ['Trace Data'],
            [[{ name: 'Unsupported Trace Schema', ri: [0] }]],
            [['Unsupported Trace Schema']],
            [[null]],
            [[[]]],
            [['text']],
            [[title + '\n' + message]],
            [[stringValue(trace && trace.sourceBlockId, '')]],
            [[]],
            [[]]
        );
    }

    function canonicalStages(stageNames, groups, ruleNames, planTrees, ruleInfo, ruleTypes, ruleText, sourceBlockIds, tilePayloadRefs, tileFeatures) {
        var stages = [];
        for (var si = 0; si < stageNames.length; si++) {
            var rules = [];
            for (var ri = 0; ri < ruleNames[si].length; ri++) {
                var payloadRef = tilePayloadRefs && tilePayloadRefs[si] && tilePayloadRefs[si][ri] || null;
                var features = tileFeatures && tileFeatures[si] && tileFeatures[si][ri] || null;
                if (ruleTypes[si] && ruleTypes[si][ri] === 'text') {
                    var textRule = {
                        name: ruleNames[si][ri],
                        type: 'text',
                        text: ruleText[si] ? ruleText[si][ri] || '' : '',
                        sourceBlockId: sourceBlockIds[si] ? sourceBlockIds[si][ri] || '' : ''
                    };
                    if (payloadRef) textRule.payloadRef = payloadRef;
                    if (features) textRule.features = features;
                    rules.push(textRule);
                } else {
                    var planRule = {
                        name: ruleNames[si][ri],
                        tree: planTrees[si][ri],
                        info: ruleInfo[si][ri],
                        sourceBlockId: sourceBlockIds[si] ? sourceBlockIds[si][ri] || '' : ''
                    };
                    if (payloadRef) planRule.payloadRef = payloadRef;
                    if (features) planRule.features = features;
                    rules.push(planRule);
                }
            }
            stages.push({
                name: stageNames[si],
                groups: groups[si],
                rules: rules
            });
        }
        return stages;
    }

    function makeTraceDocument(trace, index, stageNames, groups, ruleNames, planTrees, ruleInfo, ruleTypes, ruleText, sourceBlockIds, tilePayloadRefs, tileFeatures) {
        var availability = normalizeFeatureAvailability(trace.featureAvailability);
        var nodeFields = normalizeNodeFields(trace, planTrees);
        var pinnedFields = normalizePinnedFieldKeys(trace, nodeFields);
        var pinnedFieldPresets = normalizePinnedFieldPresets(trace, nodeFields);
        var diffFieldPresets = normalizeDiffFieldPresets(trace, nodeFields);
        index = Number(index);
        if (!Number.isInteger(index) || index < 0) index = 0;
        return {
            schemaVersion: normalizedSchemaVersion(trace.schemaVersion),
            traceIndex: index,
            title: stringValue(trace.title, 'Trace ' + (index + 1)),
            stageNames: stageNames,
            groups: groups,
            ruleNames: ruleNames,
            ruleTypes: ruleTypes || [],
            ruleText: ruleText || [],
            nodeFields: nodeFields,
            pinnedFields: pinnedFields,
            declaredPinnedFields: asArray(trace && trace.pinnedFields).map(function(key) {
                return stringValue(key, '');
            }).filter(function(key) {
                return !!key;
            }),
            pinnedFieldPresets: pinnedFieldPresets,
            diffFieldPresets: diffFieldPresets,
            traceFeatureAvailability: availability,
            planTrees: planTrees,
            ruleInfo: ruleInfo,
            sourceBlockIds: sourceBlockIds || [],
            tilePayloadRefs: tilePayloadRefs || [],
            tileFeatures: tileFeatures || [],
            payloadRegistry: trace.payloadRegistry || null,
            stages: canonicalStages(
                stageNames,
                groups,
                ruleNames,
                planTrees,
                ruleInfo,
                ruleTypes || [],
                ruleText || [],
                sourceBlockIds || [],
                tilePayloadRefs || [],
                tileFeatures || []
            )
        };
    }

    function normalizeTraceData(trace, index) {
        trace = trace || {};
        if (!hasSupportedSchema(trace)) return unsupportedSchemaTrace(trace, index);
        return normalizeCanonicalTrace(trace, index);
    }

    return {
        fieldRowDetails: fieldRowDetails,
        fieldRowFieldKey: fieldRowFieldKey,
        fieldRowHasDetails: fieldRowHasDetails,
        fieldRowKey: fieldRowKey,
        fieldRowValue: fieldRowValue,
        infoSectionsHaveContent: normalizedInfoSectionsHaveContent,
        normalizeTraceData: normalizeTraceData,
        normalizeTilePayload: normalizeTilePayload
    };
})();

var RuleRefs = (function() {
    function make(si, gi, ri) {
        return { si: si, gi: gi, ri: ri };
    }

    function key(si, gi, ri) {
        if (typeof si === 'object' && si) return si.si + '-' + si.gi + '-' + si.ri;
        return si + '-' + gi + '-' + ri;
    }

    function args(ref) {
        return ref.si + ',' + ref.gi + ',' + ref.ri;
    }

    function same(ref, si, gi, ri) {
        return !!ref && ref.si === si && ref.gi === gi && ref.ri === ri;
    }

    function rawRuleIndex(groups, si, gi, ri) {
        return groups[si][gi].ri[ri];
    }

    function isValid(groups, state, si, gi, ri) {
        return !!state.stages[si] &&
               !!state.stages[si].groups[gi] &&
               !!state.stages[si].groups[gi].rules[ri] &&
               !!groups[si] &&
               !!groups[si][gi] &&
               ri >= 0 &&
               ri < groups[si][gi].ri.length;
    }

    return {
        args: args,
        key: key,
        make: make,
        rawRuleIndex: rawRuleIndex,
        same: same,
        isValid: isValid
    };
})();

var TraceStore = (function() {
    var PAYLOAD_STATES = {
        UNREQUESTED: 'unrequested',
        PENDING: 'pending',
        RENDERED: 'rendered',
        EMPTY: 'empty',
        UNAVAILABLE: 'unavailable',
        FAILED: 'failed',
        STALE: 'stale'
    };
    var VALID_PAYLOAD_STATES = {};
    Object.keys(PAYLOAD_STATES).forEach(function(key) {
        VALID_PAYLOAD_STATES[PAYLOAD_STATES[key]] = true;
    });
    var VALID_PAYLOAD_BUCKETS = {
        trees: true,
        info: true,
        text: true,
        diff: true
    };
    var DEFAULT_MATERIALIZED_PAYLOAD_CACHE_LIMIT = 128;

    function normalizedTraceIndex(trace) {
        var index = Number(trace && trace.traceIndex);
        return Number.isInteger(index) && index >= 0 ? index : 0;
    }

    function traceHandleForIndex(index) {
        return 'trace:' + index;
    }

    function copyArray(value) {
        return Array.isArray(value) ? value.slice() : [];
    }

    function buildNodeFieldMap(fields) {
        var map = {};
        fields = Array.isArray(fields) ? fields : [];
        for (var i = 0; i < fields.length; i++) {
            var field = fields[i] || {};
            var key = String(field.key || '');
            if (!key || map[key]) continue;
            map[key] = {
                key: key,
                label: String(field.label || key)
            };
        }
        return map;
    }

    function fieldRowKey(row) {
        if (Array.isArray(row)) return String(row[0] === undefined || row[0] === null ? '' : row[0]);
        row = row || {};
        return String(row.key === undefined || row.key === null ? '' : row.key);
    }

    function fieldRowValue(row) {
        if (Array.isArray(row)) return String(row[1] === undefined || row[1] === null ? '' : row[1]);
        row = row || {};
        return String(row.value === undefined || row.value === null ? '' : row.value);
    }

    function fieldRowDetails(row) {
        if (!row || Array.isArray(row) || !Array.isArray(row.details)) return [];
        return row.details;
    }

    function buildHandles(trace) {
        var traceIndex = normalizedTraceIndex(trace);
        var traceHandle = traceHandleForIndex(traceIndex);
        var stageNames = trace.stageNames || [];
        var groups = trace.groups || [];
        var ruleNames = trace.ruleNames || [];
        var planTrees = trace.planTrees || [];
        var ruleTypes = trace.ruleTypes || [];
        var ruleText = trace.ruleText || [];
        var ruleInfo = trace.ruleInfo || [];
        var sourceBlockIds = trace.sourceBlockIds || [];
        var stageCount = Math.max(
            stageNames.length,
            groups.length,
            ruleNames.length,
            planTrees.length,
            ruleTypes.length,
            ruleText.length,
            ruleInfo.length,
            sourceBlockIds.length
        );
        var stageHandles = [];
        var groupHandles = [];
        var ruleHandles = [];

        for (var si = 0; si < stageCount; si++) {
            var stageHandle = traceHandle + '/stage:' + si;
            var stageGroups = groups[si] || [];
            var stageRuleNames = ruleNames[si] || [];
            var stagePlanTrees = planTrees[si] || [];
            var stageRuleTypes = ruleTypes[si] || [];
            var stageRuleText = ruleText[si] || [];
            var stageRuleInfo = ruleInfo[si] || [];
            var stageSourceBlockIds = sourceBlockIds[si] || [];
            var ruleCount = Math.max(
                stageRuleNames.length,
                stagePlanTrees.length,
                stageRuleTypes.length,
                stageRuleText.length,
                stageRuleInfo.length,
                stageSourceBlockIds.length
            );

            stageHandles[si] = stageHandle;
            groupHandles[si] = [];
            for (var gi = 0; gi < stageGroups.length; gi++) {
                groupHandles[si][gi] = stageHandle + '/group:' + gi;
            }

            ruleHandles[si] = [];
            for (var ri = 0; ri < ruleCount; ri++) {
                ruleHandles[si][ri] = stageHandle + '/rule:' + ri;
            }
        }

        return {
            traceIndex: traceIndex,
            traceHandle: traceHandle,
            stageHandles: stageHandles,
            groupHandles: groupHandles,
            ruleHandles: ruleHandles
        };
    }

    function treeHasFields(node) {
        if (!node || typeof node !== 'object') return false;
        if (Array.isArray(node.a) && node.a.length) return true;
        var children = Array.isArray(node.c) ? node.c : [];
        for (var i = 0; i < children.length; i++) {
            if (treeHasFields(children[i])) return true;
        }
        return false;
    }

    function treeHasPinned(node) {
        if (!node || typeof node !== 'object') return false;
        if (Array.isArray(node.a) && node.a.some(function(row) {
            return row && fieldRowValue(row) !== '';
        })) return true;
        var children = Array.isArray(node.c) ? node.c : [];
        for (var i = 0; i < children.length; i++) {
            if (treeHasPinned(children[i])) return true;
        }
        return false;
    }

    function ruleTypeFromTrace(trace, si, rawIdx) {
        return trace.ruleTypes[si] && trace.ruleTypes[si][rawIdx] === 'text'
            ? 'text'
            : 'plan';
    }

    function tilePayloadRefFromTrace(trace, si, rawIdx) {
        return trace.tilePayloadRefs && trace.tilePayloadRefs[si] && trace.tilePayloadRefs[si][rawIdx] || null;
    }

    function tileFeaturesFromTrace(trace, si, rawIdx) {
        return trace.tileFeatures && trace.tileFeatures[si] && trace.tileFeatures[si][rawIdx] || null;
    }

    function normalizeRuleFeaturesFromSummary(type, features) {
        features = features || {};
        var isText = type === 'text';
        return {
            tree: !isText,
            fields: !isText && !!features.fields,
            pinned: !isText && !!features.pinned,
            info: !isText && !!features.info,
            text: isText,
            diff: !isText
        };
    }

    function ruleFeatures(trace, si, rawIdx) {
        var type = ruleTypeFromTrace(trace, si, rawIdx);
        var summaryFeatures = tileFeaturesFromTrace(trace, si, rawIdx);
        if (summaryFeatures || tilePayloadRefFromTrace(trace, si, rawIdx)) {
            return normalizeRuleFeaturesFromSummary(type, summaryFeatures);
        }
        var tree = trace.planTrees[si] && trace.planTrees[si][rawIdx] || null;
        var info = trace.ruleInfo[si] && trace.ruleInfo[si][rawIdx] || [];
        var text = trace.ruleText[si] && trace.ruleText[si][rawIdx] || '';
        return {
            tree: type === 'plan' && !!tree,
            fields: type === 'plan' && treeHasFields(tree),
            pinned: type === 'plan' && treeHasPinned(tree),
            info: type === 'plan' && TraceSchema.infoSectionsHaveContent(info),
            text: type === 'text' && text.length > 0,
            diff: type === 'plan' && !!tree
        };
    }

    function buildRuleHandleIndex(handles) {
        var index = {};
        for (var si = 0; si < handles.ruleHandles.length; si++) {
            for (var ri = 0; ri < handles.ruleHandles[si].length; ri++) {
                index[handles.ruleHandles[si][ri]] = { si: si, rawIdx: ri };
            }
        }
        return index;
    }

    function sourceBlockIdForRule(trace, si, rawIdx) {
        return trace.sourceBlockIds && trace.sourceBlockIds[si] && trace.sourceBlockIds[si][rawIdx] || '';
    }

    function payloadHandleFor(ruleHandleValue, kind) {
        return ruleHandleValue + '/payload:' + kind;
    }

    function payloadHandlesForRule(ruleHandleValue) {
        return {
            tree: payloadHandleFor(ruleHandleValue, 'tree'),
            info: payloadHandleFor(ruleHandleValue, 'info'),
            text: payloadHandleFor(ruleHandleValue, 'text'),
            diff: payloadHandleFor(ruleHandleValue, 'diff')
        };
    }

    function buildSummary(trace, handles) {
        var summary = {
            trace: {
                handle: handles.traceHandle,
                index: handles.traceIndex,
                schemaVersion: trace.schemaVersion,
                title: trace.title || 'Trace',
                featureAvailability: trace.traceFeatureAvailability || null,
                stageCount: handles.stageHandles.length
            },
            stages: []
        };

        for (var si = 0; si < handles.stageHandles.length; si++) {
            var groups = trace.groups[si] || [];
            var ruleNames = trace.ruleNames[si] || [];
            var stage = {
                handle: handles.stageHandles[si],
                index: si,
                name: trace.stageNames[si] || '',
                groupCount: groups.length,
                ruleCount: handles.ruleHandles[si].length,
                groups: [],
                rules: []
            };

            for (var gi = 0; gi < groups.length; gi++) {
                stage.groups.push({
                    handle: handles.groupHandles[si][gi],
                    index: gi,
                    name: groups[gi] && groups[gi].name || '',
                    ruleIndices: copyArray(groups[gi] && groups[gi].ri)
                });
            }

            for (var ri = 0; ri < handles.ruleHandles[si].length; ri++) {
                var ruleHandleValue = handles.ruleHandles[si][ri];
                stage.rules.push({
                    handle: ruleHandleValue,
                    index: ri,
                    name: ruleNames[ri] || '',
                    type: ruleTypeFromTrace(trace, si, ri),
                    features: ruleFeatures(trace, si, ri),
                    sourceBlockId: sourceBlockIdForRule(trace, si, ri),
                    payloadHandles: payloadHandlesForRule(ruleHandleValue)
                });
            }
            summary.stages.push(stage);
        }

        return summary;
    }

    function buildPayloads(trace, handles) {
        var payloads = {
            trees: {},
            info: {},
            text: {},
            diff: {}
        };

        for (var si = 0; si < handles.ruleHandles.length; si++) {
            for (var ri = 0; ri < handles.ruleHandles[si].length; ri++) {
                var handle = handles.ruleHandles[si][ri];
                if (tilePayloadRefFromTrace(trace, si, ri)) continue;
                if (ruleTypeFromTrace(trace, si, ri) === 'text') {
                    payloads.text[handle] = trace.ruleText[si] && trace.ruleText[si][ri] || '';
                    continue;
                }

                payloads.trees[handle] = trace.planTrees[si] && trace.planTrees[si][ri] || null;
                payloads.info[handle] = trace.ruleInfo[si] && trace.ruleInfo[si][ri] || [];
                payloads.diff[handle] = {
                    tree: payloads.trees[handle],
                    info: payloads.info[handle]
                };
            }
        }

        return payloads;
    }

    function payloadHasContent(bucket, value) {
        if (bucket === 'info') return Array.isArray(value) && value.length > 0;
        if (bucket === 'text') return value !== undefined && value !== null && String(value).length > 0;
        if (bucket === 'diff') return !!(value && value.tree);
        return !!value;
    }

    function payloadStateFor(bucket, available, value) {
        if (!available) return PAYLOAD_STATES.UNAVAILABLE;
        return payloadHasContent(bucket, value)
            ? PAYLOAD_STATES.RENDERED
            : PAYLOAD_STATES.EMPTY;
    }

    function payloadStateRecord(state, error) {
        if (!VALID_PAYLOAD_STATES[state]) {
            throw new Error('Unknown trace payload state: ' + state);
        }
        return {
            state: state,
            error: error || null
        };
    }

    function buildPayloadStates(trace, handles, payloads) {
        var payloadStates = {
            trees: {},
            info: {},
            text: {},
            diff: {}
        };

        for (var si = 0; si < handles.ruleHandles.length; si++) {
            for (var ri = 0; ri < handles.ruleHandles[si].length; ri++) {
                var handle = handles.ruleHandles[si][ri];
                var isText = ruleTypeFromTrace(trace, si, ri) === 'text';
                var payloadRef = tilePayloadRefFromTrace(trace, si, ri);
                var features = ruleFeatures(trace, si, ri);
                if (payloadRef) {
                    payloadStates.trees[handle] = payloadStateRecord(
                        isText ? PAYLOAD_STATES.UNAVAILABLE : PAYLOAD_STATES.UNREQUESTED
                    );
                    payloadStates.info[handle] = payloadStateRecord(
                        isText ? PAYLOAD_STATES.UNAVAILABLE :
                            (features.info ? PAYLOAD_STATES.UNREQUESTED : PAYLOAD_STATES.EMPTY)
                    );
                    payloadStates.text[handle] = payloadStateRecord(
                        isText ? PAYLOAD_STATES.UNREQUESTED : PAYLOAD_STATES.UNAVAILABLE
                    );
                    payloadStates.diff[handle] = payloadStateRecord(
                        isText ? PAYLOAD_STATES.UNAVAILABLE : PAYLOAD_STATES.UNREQUESTED
                    );
                    continue;
                }
                payloadStates.trees[handle] = payloadStateRecord(
                    payloadStateFor('trees', !isText, payloads.trees[handle])
                );
                payloadStates.info[handle] = payloadStateRecord(
                    payloadStateFor('info', !isText, payloads.info[handle])
                );
                payloadStates.text[handle] = payloadStateRecord(
                    payloadStateFor('text', isText, payloads.text[handle])
                );
                payloadStates.diff[handle] = payloadStateRecord(
                    payloadStateFor('diff', !isText, payloads.diff[handle])
                );
            }
        }

        return payloadStates;
    }

    function buildPayloadDiagnostics(trace, handles) {
        var diagnostics = {};
        var kinds = ['tree', 'info', 'text', 'diff'];
        for (var si = 0; si < handles.ruleHandles.length; si++) {
            for (var ri = 0; ri < handles.ruleHandles[si].length; ri++) {
                var ruleHandleValue = handles.ruleHandles[si][ri];
                var sourceBlockId = sourceBlockIdForRule(trace, si, ri);
                var payloadRef = tilePayloadRefFromTrace(trace, si, ri);
                for (var ki = 0; ki < kinds.length; ki++) {
                    var kind = kinds[ki];
                    var payloadHandleValue = payloadHandleFor(ruleHandleValue, kind);
                    diagnostics[payloadHandleValue] = {
                        payloadHandle: payloadHandleValue,
                        ruleHandle: ruleHandleValue,
                        kind: kind,
                        sourceBlockId: sourceBlockId,
                        payloadRef: payloadRef
                    };
                }
            }
        }
        return diagnostics;
    }

    function empty() {
        return create({
            traceIndex: 0,
            title: 'Trace',
            stageNames: [],
            groups: [],
            ruleNames: [],
            ruleTypes: [],
            ruleText: [],
            nodeFields: [],
            pinnedFields: [],
            declaredPinnedFields: [],
            pinnedFieldPresets: [],
            diffFieldPresets: [],
            traceFeatureAvailability: null,
            planTrees: [],
            ruleInfo: [],
            sourceBlockIds: []
        });
    }

    function normalizedCacheLimit(limit) {
        limit = Math.floor(Number(limit));
        if (!Number.isFinite(limit) || limit < 0) return DEFAULT_MATERIALIZED_PAYLOAD_CACHE_LIMIT;
        return limit;
    }

    function createMaterializedPayloadCache(limit) {
        return {
            limit: normalizedCacheLimit(limit),
            entries: {},
            order: []
        };
    }

    function normalizedTraceGeneration(value) {
        value = Number(value);
        return Number.isInteger(value) && value >= 0 ? value : 0;
    }

    function create(trace, options) {
        trace = trace || {};
        options = options || {};
        var handles = buildHandles(trace);
        var ruleHandleIndex = buildRuleHandleIndex(handles);
        var summary = buildSummary(trace, handles);
        var payloads = buildPayloads(trace, handles);
        var payloadStates = buildPayloadStates(trace, handles, payloads);
        var payloadDiagnostics = buildPayloadDiagnostics(trace, handles);
        var materializedPayloadCache = createMaterializedPayloadCache(options.materializedPayloadCacheLimit);
        var traceGeneration = normalizedTraceGeneration(options.traceGeneration);
        return {
            trace: trace,
            traceIndex: handles.traceIndex,
            traceGeneration: traceGeneration,
            traceHandle: handles.traceHandle,
            stageHandles: handles.stageHandles,
            groupHandles: handles.groupHandles,
            ruleHandles: handles.ruleHandles,
            ruleHandleIndex: ruleHandleIndex,
            summary: summary,
            payloads: payloads,
            payloadStates: payloadStates,
            payloadDiagnostics: payloadDiagnostics,
            payloadRegistry: trace.payloadRegistry || null,
            materialization: {
                nextRequestId: 1,
                pending: {}
            },
            materializedPayloadCache: materializedPayloadCache,
            title: trace.title || 'Trace',
            stageNames: trace.stageNames || [],
            groups: trace.groups || [],
            ruleNames: trace.ruleNames || [],
            ruleTypes: trace.ruleTypes || [],
            ruleText: trace.ruleText || [],
            nodeFields: trace.nodeFields || [],
            pinnedFields: trace.pinnedFields || [],
            declaredPinnedFields: trace.declaredPinnedFields || trace.pinnedFields || [],
            pinnedFieldPresets: trace.pinnedFieldPresets || [],
            diffFieldPresets: trace.diffFieldPresets || [],
            nodeFieldMap: buildNodeFieldMap(trace.nodeFields || []),
            traceFeatureAvailability: trace.traceFeatureAvailability || null,
            planTrees: trace.planTrees || [],
            ruleInfo: trace.ruleInfo || [],
            tilePayloadRefs: trace.tilePayloadRefs || [],
            tileFeatures: trace.tileFeatures || [],
            sourceBlockIds: trace.sourceBlockIds || [],
            stages: trace.stages || []
        };
    }

    function stageCount(store) {
        return store.stageNames.length;
    }

    function stageGroups(store, si) {
        return store.groups[si] || [];
    }

    function group(store, si, gi) {
        return stageGroups(store, si)[gi] || null;
    }

    function stageName(store, si) {
        return store.stageNames[si] || '';
    }

    function traceHandle(store) {
        return store && store.traceHandle || traceHandleForIndex(0);
    }

    function stageHandle(store, si) {
        return store && store.stageHandles && store.stageHandles[si] || null;
    }

    function groupHandle(store, si, gi) {
        return store && store.groupHandles && store.groupHandles[si] && store.groupHandles[si][gi] || null;
    }

    function ruleHandle(store, si, rawIdx) {
        return store && store.ruleHandles && store.ruleHandles[si] && store.ruleHandles[si][rawIdx] || null;
    }

    function hasOwn(object, key) {
        return Object.prototype.hasOwnProperty.call(object, key);
    }

    function indexFromId(handles, id) {
        if (!Array.isArray(handles)) return -1;
        if (typeof id === 'number' && Number.isInteger(id) && id >= 0 && id < handles.length) {
            return id;
        }
        if (typeof id === 'string') {
            for (var i = 0; i < handles.length; i++) {
                if (handles[i] === id) return i;
            }
        }
        return -1;
    }

    function traceMatches(store, traceId) {
        if (!store) return false;
        if (traceId === undefined || traceId === null) return true;
        if (traceId === store.traceHandle) return true;
        var index = Number(traceId);
        return Number.isInteger(index) && index === store.traceIndex;
    }

    function traceSummary(store) {
        return store && store.summary ? store.summary.trace : null;
    }

    function stageSummary(store, si) {
        return store && store.summary && store.summary.stages[si] || null;
    }

    function groupSummary(store, si, gi) {
        var stage = stageSummary(store, si);
        return stage && stage.groups[gi] || null;
    }

    function ruleSummary(store, si, rawIdx) {
        var stage = stageSummary(store, si);
        return stage && stage.rules[rawIdx] || null;
    }

    function getTraceSummary(store, traceId) {
        return traceMatches(store, traceId) ? traceSummary(store) : null;
    }

    function getStageSummary(store, traceId, stageId) {
        if (!traceMatches(store, traceId)) return null;
        return stageSummary(store, indexFromId(store.stageHandles, stageId));
    }

    function groupRuleRawIndex(store, si, gi, ruleId) {
        var group = store && store.groups[si] && store.groups[si][gi] || null;
        if (!group || !Array.isArray(group.ri)) return -1;
        if (typeof ruleId === 'number' && Number.isInteger(ruleId) && ruleId >= 0 && ruleId < group.ri.length) {
            return group.ri[ruleId];
        }
        for (var i = 0; i < group.ri.length; i++) {
            var rawIdx = group.ri[i];
            if (ruleHandle(store, si, rawIdx) === ruleId) return rawIdx;
        }
        return -1;
    }

    function getRuleSummary(store, traceId, stageId, groupId, ruleId) {
        if (!traceMatches(store, traceId)) return null;
        var si = indexFromId(store.stageHandles, stageId);
        if (si < 0) return null;

        if (typeof ruleId === 'string') {
            var directRawIdx = indexFromId(store.ruleHandles[si], ruleId);
            if (directRawIdx >= 0) return ruleSummary(store, si, directRawIdx);
        }

        var gi = indexFromId(store.groupHandles[si], groupId);
        if (gi < 0) return null;
        var rawIdx = groupRuleRawIndex(store, si, gi, ruleId);
        return rawIdx >= 0 ? ruleSummary(store, si, rawIdx) : null;
    }

    function ruleLocationForHandle(store, ruleHandleValue) {
        return store && store.ruleHandleIndex && store.ruleHandleIndex[ruleHandleValue] || null;
    }

    function tilePayloadRefForRuleHandle(store, ruleHandleValue) {
        var location = ruleLocationForHandle(store, ruleHandleValue);
        return location && store.tilePayloadRefs &&
            store.tilePayloadRefs[location.si] &&
            store.tilePayloadRefs[location.si][location.rawIdx] || null;
    }

    function setPayloadValue(store, bucket, ruleHandleValue, value, available) {
        if (!store.payloads) store.payloads = {};
        if (!store.payloads[bucket]) store.payloads[bucket] = {};
        if (available) {
            store.payloads[bucket][ruleHandleValue] = value;
            materializedPayloadCacheSet(store, bucket, ruleHandleValue, value);
        } else {
            delete store.payloads[bucket][ruleHandleValue];
            materializedPayloadCacheDelete(store, bucket, ruleHandleValue);
        }
        return setPayloadState(
            store,
            bucket,
            ruleHandleValue,
            payloadStateFor(bucket, !!available, value)
        );
    }

    function materializedRuleFeatures(store, si, rawIdx) {
        var type = ruleTypeFromTrace(store, si, rawIdx);
        var tree = store.planTrees[si] && store.planTrees[si][rawIdx] || null;
        var info = store.ruleInfo[si] && store.ruleInfo[si][rawIdx] || [];
        var text = store.ruleText[si] && store.ruleText[si][rawIdx] || '';
        return {
            tree: type === 'plan' && !!tree,
            fields: type === 'plan' && treeHasFields(tree),
            pinned: type === 'plan' && treeHasPinned(tree),
            info: type === 'plan' && TraceSchema.infoSectionsHaveContent(info),
            text: type === 'text' && text.length > 0,
            diff: type === 'plan' && !!tree
        };
    }

    function refreshRuleSummaryFeatures(store, si, rawIdx) {
        var features = materializedRuleFeatures(store, si, rawIdx);
        if (!store.tileFeatures) store.tileFeatures = [];
        if (!store.tileFeatures[si]) store.tileFeatures[si] = [];
        store.tileFeatures[si][rawIdx] = {
            fields: !!features.fields,
            pinned: !!features.pinned,
            info: !!features.info
        };

        var summaryRule = store.summary &&
            store.summary.stages &&
            store.summary.stages[si] &&
            store.summary.stages[si].rules &&
            store.summary.stages[si].rules[rawIdx];
        if (summaryRule) summaryRule.features = features;
        store.traceFeatureAvailability = null;
        if (typeof invalidateCachedRuleFeaturesForRawRule === 'function') {
            invalidateCachedRuleFeaturesForRawRule(si, rawIdx);
        }
        return features;
    }

    function declaredPinnedFieldMap(store) {
        var map = {};
        var keys = Array.isArray(store && store.declaredPinnedFields) ? store.declaredPinnedFields : [];
        for (var i = 0; i < keys.length; i++) {
            var key = String(keys[i] || '');
            if (key) map[key] = true;
        }
        return map;
    }

    function pinnedFieldContains(store, key) {
        var fields = Array.isArray(store && store.pinnedFields) ? store.pinnedFields : [];
        return fields.indexOf(key) >= 0;
    }

    function discoverNodeFieldsFromTree(store, node, declaredFields, state) {
        if (!store || !node || typeof node !== 'object') return state;
        declaredFields = declaredFields || declaredPinnedFieldMap(store);
        state = state || { fieldsChanged: false, pinnedFieldsChanged: false };
        if (!store.nodeFields) store.nodeFields = [];
        if (!store.nodeFieldMap) store.nodeFieldMap = {};
        if (!store.pinnedFields) store.pinnedFields = [];

        var attrs = Array.isArray(node.a) ? node.a : [];
        for (var i = 0; i < attrs.length; i++) {
            var key = fieldRowKey(attrs[i]);
            if (!key) continue;
            if (!store.nodeFieldMap[key]) {
                store.nodeFieldMap[key] = { key: key, label: key };
                store.nodeFields.push(store.nodeFieldMap[key]);
                state.fieldsChanged = true;
            }
            if (declaredFields[key] && !pinnedFieldContains(store, key)) {
                store.pinnedFields.push(key);
                state.pinnedFieldsChanged = true;
            }
        }

        var children = Array.isArray(node.c) ? node.c : [];
        for (var c = 0; c < children.length; c++) {
            discoverNodeFieldsFromTree(store, children[c], declaredFields, state);
        }
        return state;
    }

    function markRulePayloadFailed(store, ruleHandleValue, error) {
        var location = ruleLocationForHandle(store, ruleHandleValue);
        if (!location) return;
        var isText = ruleTypeFromTrace(store, location.si, location.rawIdx) === 'text';
        if (isText) {
            setPayloadState(store, 'text', ruleHandleValue, PAYLOAD_STATES.FAILED, error);
            return;
        }
        setPayloadState(store, 'trees', ruleHandleValue, PAYLOAD_STATES.FAILED, error);
        setPayloadState(store, 'info', ruleHandleValue, PAYLOAD_STATES.FAILED, error);
        setPayloadState(store, 'diff', ruleHandleValue, PAYLOAD_STATES.FAILED, error);
    }

    function applyDecodedTilePayload(store, ruleHandleValue, payload) {
        var location = ruleLocationForHandle(store, ruleHandleValue);
        if (!location) return { applied: false, reason: 'missing-rule-handle' };
        var si = location.si;
        var rawIdx = location.rawIdx;
        var type = ruleTypeFromTrace(store, si, rawIdx);
        var normalized = TraceSchema.normalizeTilePayload(
            payload,
            type,
            {
                traceIndex: store.traceIndex,
                stageIndex: si,
                ruleIndex: rawIdx
            },
            'payloads[' + ruleHandleValue + ']'
        );

        if (type === 'text') {
            if (!store.ruleText[si]) store.ruleText[si] = [];
            store.ruleText[si][rawIdx] = normalized.text;
            setPayloadValue(store, 'text', ruleHandleValue, normalized.text, true);
            return { applied: true, state: payloadState(store, 'text', ruleHandleValue).state };
        }

        if (!store.planTrees[si]) store.planTrees[si] = [];
        if (!store.ruleInfo[si]) store.ruleInfo[si] = [];
        store.planTrees[si][rawIdx] = normalized.tree;
        store.ruleInfo[si][rawIdx] = normalized.info || [];
        discoverNodeFieldsFromTree(store, normalized.tree);
        refreshRuleSummaryFeatures(store, si, rawIdx);
        setPayloadValue(store, 'trees', ruleHandleValue, normalized.tree, true);
        setPayloadValue(store, 'info', ruleHandleValue, store.ruleInfo[si][rawIdx], true);
        setPayloadValue(store, 'diff', ruleHandleValue, {
            tree: normalized.tree,
            info: store.ruleInfo[si][rawIdx]
        }, true);
        return { applied: true, state: payloadState(store, 'trees', ruleHandleValue).state };
    }

    function ensureRulePayload(store, ruleHandleValue) {
        var ref = tilePayloadRefForRuleHandle(store, ruleHandleValue);
        if (!ref) return { status: 'eager' };
        var location = ruleLocationForHandle(store, ruleHandleValue);
        if (!location) return { status: 'missing-rule-handle' };
        var bucket = ruleTypeFromTrace(store, location.si, location.rawIdx) === 'text' ? 'text' : 'trees';
        var state = payloadState(store, bucket, ruleHandleValue).state;
        if (state === PAYLOAD_STATES.RENDERED || state === PAYLOAD_STATES.EMPTY) {
            return { status: 'ready' };
        }
        if (state === PAYLOAD_STATES.FAILED) return { status: 'failed' };
        var decoded = TracePayloadRegistry.decodedTilePayload(store.payloadRegistry, ref.tileId);
        if (decoded.status !== 'decoded') {
            markRulePayloadFailed(store, ruleHandleValue, decoded.error || decoded.status);
            return { status: 'failed', error: decoded.error || decoded.status };
        }
        var applied = applyDecodedTilePayload(store, ruleHandleValue, decoded.value || {});
        return applied.applied
            ? { status: 'decoded', blockId: decoded.blockId, payloadIndex: decoded.payloadIndex }
            : { status: 'failed', error: applied.reason };
    }

    function materializePayload(store, bucket, ruleHandleValue) {
        if (payloadState(store, bucket, ruleHandleValue).state === PAYLOAD_STATES.UNREQUESTED) {
            ensureRulePayload(store, ruleHandleValue);
        }
        var payloads = store && store.payloads && store.payloads[bucket] || {};
        var record = payloadState(store, bucket, ruleHandleValue);
        if (record.state !== PAYLOAD_STATES.RENDERED &&
            record.state !== PAYLOAD_STATES.EMPTY) {
            return null;
        }
        var cached = materializedPayloadCacheGet(store, bucket, ruleHandleValue);
        if (cached) return cached.value;
        return hasOwn(payloads, ruleHandleValue)
            ? materializedPayloadCacheSet(store, bucket, ruleHandleValue, payloads[ruleHandleValue]).value
            : null;
    }

    function materializeRuleTree(store, ruleHandleValue) {
        return materializePayload(store, 'trees', ruleHandleValue);
    }

    function materializeRuleInfo(store, ruleHandleValue) {
        return materializePayload(store, 'info', ruleHandleValue);
    }

    function materializeTextTile(store, ruleHandleValue) {
        return materializePayload(store, 'text', ruleHandleValue);
    }

    function payloadState(store, bucket, ruleHandleValue) {
        var states = store && store.payloadStates && store.payloadStates[bucket] || {};
        return hasOwn(states, ruleHandleValue)
            ? states[ruleHandleValue]
            : payloadStateRecord(PAYLOAD_STATES.UNAVAILABLE);
    }

    function clearPendingMaterializationForPayload(store, bucket, ruleHandleValue) {
        var materialization = store && store.materialization;
        var pending = materialization && materialization.pending;
        if (!pending) return false;

        var key = materializationKey(bucket, ruleHandleValue);
        if (!hasOwn(pending, key)) return false;
        delete pending[key];
        materialization.generation = Math.max(0, Math.floor(Number(materialization.generation)) || 0) + 1;
        return true;
    }

    function setPayloadState(store, bucket, ruleHandleValue, state, error) {
        if (!store.payloadStates) store.payloadStates = {};
        if (!store.payloadStates[bucket]) store.payloadStates[bucket] = {};
        if (state !== PAYLOAD_STATES.PENDING) {
            clearPendingMaterializationForPayload(store, bucket, ruleHandleValue);
        }
        store.payloadStates[bucket][ruleHandleValue] = payloadStateRecord(state, error);
        return store.payloadStates[bucket][ruleHandleValue];
    }

    function normalizePayloadBucket(bucket) {
        bucket = String(bucket || '');
        return VALID_PAYLOAD_BUCKETS[bucket] ? bucket : '';
    }

    function materializationState(store) {
        if (!store.materialization) {
            store.materialization = {
                nextRequestId: 1,
                generation: 0,
                pending: {}
            };
        }
        if (!store.materialization.pending) store.materialization.pending = {};
        store.materialization.nextRequestId = Math.max(1, Math.floor(Number(store.materialization.nextRequestId)) || 1);
        store.materialization.generation = Math.max(0, Math.floor(Number(store.materialization.generation)) || 0);
        return store.materialization;
    }

    function materializationKey(bucket, ruleHandleValue) {
        return bucket + '|' + ruleHandleValue;
    }

    function cloneOpaqueRecord(record) {
        if (!record || typeof record !== 'object') return null;
        var copy = {};
        for (var key in record) {
            if (!hasOwn(record, key)) continue;
            copy[key] = record[key];
        }
        return copy;
    }

    function beginPayloadMaterialization(store, bucket, ruleHandleValue, options) {
        options = options || {};
        bucket = normalizePayloadBucket(bucket);
        if (!store || !bucket || !ruleHandleValue) return null;

        var materialization = materializationState(store);
        var request = {
            requestId: materialization.nextRequestId++,
            traceGeneration: normalizedTraceGeneration(store.traceGeneration),
            bucket: bucket,
            ruleHandle: ruleHandleValue,
            key: materializationKey(bucket, ruleHandleValue)
        };
        if (options.searchTransactionToken) {
            request.searchTransactionToken = cloneOpaqueRecord(options.searchTransactionToken);
        }
        materialization.pending[request.key] = request;
        setPayloadState(store, bucket, ruleHandleValue, PAYLOAD_STATES.PENDING);
        materialization.generation++;
        return request;
    }

    function materializedPayloadCacheKey(store, bucket, ruleHandleValue) {
        return 'generation:' + normalizedTraceGeneration(store && store.traceGeneration) +
            '|' + bucket + '|' + ruleHandleValue;
    }

    function materializedPayloadCacheRemoveKey(cache, key) {
        var index = cache.order.indexOf(key);
        if (index >= 0) cache.order.splice(index, 1);
    }

    function materializedPayloadCacheDelete(store, bucket, ruleHandleValue) {
        var cache = store && store.materializedPayloadCache;
        if (!cache) return;
        var key = materializedPayloadCacheKey(store, bucket, ruleHandleValue);
        materializedPayloadCacheRemoveKey(cache, key);
        delete cache.entries[key];
    }

    function materializedPayloadCacheTouch(cache, key) {
        materializedPayloadCacheRemoveKey(cache, key);
        cache.order.push(key);
    }

    function materializedPayloadCacheEvict(cache) {
        while (cache.order.length > cache.limit) {
            var evicted = cache.order.shift();
            delete cache.entries[evicted];
        }
    }

    function materializedPayloadCacheGet(store, bucket, ruleHandleValue) {
        var cache = store && store.materializedPayloadCache;
        if (!cache) return null;
        var key = materializedPayloadCacheKey(store, bucket, ruleHandleValue);
        if (!hasOwn(cache.entries, key)) return null;
        materializedPayloadCacheTouch(cache, key);
        return cache.entries[key];
    }

    function materializedPayloadCacheSet(store, bucket, ruleHandleValue, value) {
        if (!store.materializedPayloadCache) {
            store.materializedPayloadCache = createMaterializedPayloadCache();
        }
        var cache = store.materializedPayloadCache;
        var key = materializedPayloadCacheKey(store, bucket, ruleHandleValue);
        cache.entries[key] = {
            key: key,
            traceGeneration: normalizedTraceGeneration(store && store.traceGeneration),
            bucket: bucket,
            ruleHandle: ruleHandleValue,
            value: value
        };
        materializedPayloadCacheTouch(cache, key);
        materializedPayloadCacheEvict(cache);
        return cache.entries[key] || { key: key, bucket: bucket, ruleHandle: ruleHandleValue, value: value };
    }

    function materializedPayloadCacheEntries(store) {
        var cache = store && store.materializedPayloadCache;
        if (!cache) return [];
        return cache.order.map(function(key) {
            return cache.entries[key];
        }).filter(Boolean);
    }

    function setMaterializedPayloadCacheLimit(store, limit) {
        if (!store.materializedPayloadCache) {
            store.materializedPayloadCache = createMaterializedPayloadCache(limit);
        }
        store.materializedPayloadCache.limit = normalizedCacheLimit(limit);
        materializedPayloadCacheEvict(store.materializedPayloadCache);
        return store.materializedPayloadCache.limit;
    }

    function cloneMaterializationRequest(request) {
        var clone = {
            requestId: request.requestId,
            traceGeneration: request.traceGeneration,
            bucket: request.bucket,
            ruleHandle: request.ruleHandle,
            key: request.key
        };
        if (request.searchTransactionToken) {
            clone.searchTransactionToken = cloneOpaqueRecord(request.searchTransactionToken);
        }
        return clone;
    }

    function pendingMaterializations(store) {
        var materialization = store && store.materialization;
        var pending = materialization && materialization.pending || {};
        return Object.keys(pending).sort().map(function(key) {
            return cloneMaterializationRequest(pending[key]);
        });
    }

    function completePayloadMaterialization(store, request, value, options) {
        options = options || {};
        if (!store || !request) {
            return { applied: false, reason: 'missing-request' };
        }

        var bucket = normalizePayloadBucket(request.bucket);
        var ruleHandleValue = request.ruleHandle;
        if (!bucket || !ruleHandleValue) {
            return { applied: false, reason: 'invalid-request' };
        }

        if (normalizedTraceGeneration(store.traceGeneration) !==
                normalizedTraceGeneration(request.traceGeneration)) {
            return { applied: false, reason: 'stale-generation' };
        }

        var materialization = materializationState(store);
        var key = materializationKey(bucket, ruleHandleValue);
        var pending = materialization.pending[key];
        if (!pending || pending.requestId !== request.requestId) {
            return { applied: false, reason: 'stale-request' };
        }
        delete materialization.pending[key];
        materialization.generation++;

        var state = options.state || (options.error ? PAYLOAD_STATES.FAILED : payloadStateFor(bucket, true, value));
        if (state === PAYLOAD_STATES.RENDERED || state === PAYLOAD_STATES.EMPTY) {
            if (!store.payloads) store.payloads = {};
            if (!store.payloads[bucket]) store.payloads[bucket] = {};
            store.payloads[bucket][ruleHandleValue] = value;
            materializedPayloadCacheSet(store, bucket, ruleHandleValue, value);
        } else {
            materializedPayloadCacheDelete(store, bucket, ruleHandleValue);
        }

        return {
            applied: true,
            state: setPayloadState(store, bucket, ruleHandleValue, state, options.error).state
        };
    }

    function payloadDiagnostics(store, payloadHandleValue) {
        var diagnostics = store && store.payloadDiagnostics || {};
        return hasOwn(diagnostics, payloadHandleValue) ? diagnostics[payloadHandleValue] : null;
    }

    function hasStageRules(store, si) {
        return stageGroups(store, si).length > 0;
    }

    function stageRuleCount(store, si) {
        var rules = store.planTrees[si] || [];
        return rules.length;
    }

    function groupName(store, si, gi) {
        var item = group(store, si, gi);
        return item ? item.name || '' : '';
    }

    function groupCount(store, si) {
        return stageGroups(store, si).length;
    }

    function groupRuleCount(store, si, gi) {
        var item = group(store, si, gi);
        return item && item.ri ? item.ri.length : 0;
    }

    function rawRuleIndex(store, si, gi, ri) {
        return RuleRefs.rawRuleIndex(store.groups, si, gi, ri);
    }

    function ruleName(store, si, rawIdx) {
        return store.ruleNames[si] && store.ruleNames[si][rawIdx] || '';
    }

    function ruleNameForRef(store, ref) {
        return ruleName(store, ref.si, rawRuleIndex(store, ref.si, ref.gi, ref.ri));
    }

    function ruleHandleForRef(store, ref) {
        if (!ref) return null;
        return ruleHandle(store, ref.si, rawRuleIndex(store, ref.si, ref.gi, ref.ri));
    }

    function ruleSummaryForRef(store, ref) {
        if (!ref) return null;
        return ruleSummary(store, ref.si, rawRuleIndex(store, ref.si, ref.gi, ref.ri));
    }

    function ruleType(store, si, rawIdx) {
        return store.ruleTypes[si] && store.ruleTypes[si][rawIdx] === 'text'
            ? 'text'
            : 'plan';
    }

    function ruleTypeForRef(store, ref) {
        return ruleType(store, ref.si, rawRuleIndex(store, ref.si, ref.gi, ref.ri));
    }

    function ruleText(store, si, rawIdx) {
        return store.ruleText[si] && store.ruleText[si][rawIdx] || '';
    }

    function ruleTextForRef(store, ref) {
        return ruleText(store, ref.si, rawRuleIndex(store, ref.si, ref.gi, ref.ri));
    }

    function planTree(store, si, rawIdx) {
        return store.planTrees[si] && store.planTrees[si][rawIdx] || null;
    }

    function planTreeForRef(store, ref) {
        return planTree(store, ref.si, rawRuleIndex(store, ref.si, ref.gi, ref.ri));
    }

    function infoSections(store, si, rawIdx) {
        return store.ruleInfo[si] && store.ruleInfo[si][rawIdx] || [];
    }

    function infoSectionsForRef(store, ref) {
        return infoSections(store, ref.si, rawRuleIndex(store, ref.si, ref.gi, ref.ri));
    }

    function nodeFields(store) {
        return store.nodeFields || [];
    }

    function pinnedFields(store) {
        return store.pinnedFields || [];
    }

    function pinnedFieldPresets(store) {
        return store.pinnedFieldPresets || [];
    }

    function diffFieldPresets(store) {
        return store.diffFieldPresets || [];
    }

    function nodeFieldLabel(store, key) {
        key = String(key || '');
        var field = store.nodeFieldMap && store.nodeFieldMap[key];
        return field && field.label || key;
    }

    function nodeAttributeValue(node, key) {
        var attrs = node && Array.isArray(node.a) ? node.a : [];
        for (var i = 0; i < attrs.length; i++) {
            if (attrs[i] && fieldRowKey(attrs[i]) === key) {
                return fieldRowValue(attrs[i]);
            }
        }
        return '';
    }

    function pinnedFieldValue(store, node, key) {
        key = String(key || '');
        return String(nodeAttributeValue(node, key) || '');
    }

    /**
     * Field rows listed in excludeKeys are omitted so values already shown as
     * pinned fields are not repeated in the inline field row block.
     */
    function nodeFieldRows(store, node, excludeKeys) {
        var rows = [];
        var attrs = node && Array.isArray(node.a) ? node.a : [];
        for (var i = 0; i < attrs.length; i++) {
            var key = fieldRowKey(attrs[i]);
            if (!attrs[i] || !key) continue;
            if (excludeKeys && excludeKeys[key]) continue;
            var value = fieldRowValue(attrs[i]);
            var details = fieldRowDetails(attrs[i]);
            var row = {
                key: nodeFieldLabel(store, key),
                value: value,
                fieldKey: key
            };
            if (details.length) row.details = details;
            rows.push(row);
        }
        return rows;
    }

    function allRules(store) {
        return TraceState.buildAllRules(stageCount(store), store.groups);
    }

    return {
        allRules: allRules,
        beginPayloadMaterialization: beginPayloadMaterialization,
        completePayloadMaterialization: completePayloadMaterialization,
        create: create,
        empty: empty,
        ensureRulePayload: ensureRulePayload,
        group: group,
        groupCount: groupCount,
        getRuleSummary: getRuleSummary,
        getStageSummary: getStageSummary,
        getTraceSummary: getTraceSummary,
        groupName: groupName,
        groupRuleCount: groupRuleCount,
        groupHandle: groupHandle,
        hasStageRules: hasStageRules,
        infoSections: infoSections,
        infoSectionsForRef: infoSectionsForRef,
        materializeRuleInfo: materializeRuleInfo,
        materializeRuleTree: materializeRuleTree,
        materializeTextTile: materializeTextTile,
        materializedPayloadCacheEntries: materializedPayloadCacheEntries,
        fieldRowDetails: TraceSchema.fieldRowDetails,
        fieldRowFieldKey: TraceSchema.fieldRowFieldKey,
        fieldRowHasDetails: TraceSchema.fieldRowHasDetails,
        fieldRowKey: TraceSchema.fieldRowKey,
        fieldRowValue: TraceSchema.fieldRowValue,
        diffFieldPresets: diffFieldPresets,
        pinnedFieldPresets: pinnedFieldPresets,
        pinnedFieldValue: pinnedFieldValue,
        pinnedFields: pinnedFields,
        nodeFieldLabel: nodeFieldLabel,
        nodeFields: nodeFields,
        nodeFieldRows: nodeFieldRows,
        PAYLOAD_STATES: PAYLOAD_STATES,
        payloadDiagnostics: payloadDiagnostics,
        payloadRefForRule: tilePayloadRefForRuleHandle,
        payloadState: payloadState,
        pendingMaterializations: pendingMaterializations,
        planTree: planTree,
        planTreeForRef: planTreeForRef,
        rawRuleIndex: rawRuleIndex,
        ruleHandle: ruleHandle,
        ruleHandleForRef: ruleHandleForRef,
        ruleName: ruleName,
        ruleNameForRef: ruleNameForRef,
        ruleSummary: ruleSummary,
        ruleSummaryForRef: ruleSummaryForRef,
        ruleText: ruleText,
        ruleTextForRef: ruleTextForRef,
        ruleType: ruleType,
        ruleTypeForRef: ruleTypeForRef,
        stageCount: stageCount,
        stageGroups: stageGroups,
        stageHandle: stageHandle,
        stageName: stageName,
        stageRuleCount: stageRuleCount,
        stageSummary: stageSummary,
        setMaterializedPayloadCacheLimit: setMaterializedPayloadCacheLimit,
        setPayloadState: setPayloadState,
        traceHandle: traceHandle,
        traceSummary: traceSummary
    };
})();
