var TRACE_VIEW_BUILD_PROFILE = "release";
function createTraceWorkerProtocol() {
    var PROTOCOL_VERSION = 'optimizer-trace-worker-v1';

    function normalizedTraceGeneration(value) {
        value = Number(value);
        return Number.isInteger(value) && value >= 0 ? value : 0;
    }

    function normalizeOperation(operation) {
        operation = String(operation || '').trim();
        if (!operation) throw new Error('Trace worker operation is required');
        return operation;
    }

    function createState(traceGeneration) {
        return {
            traceGeneration: normalizedTraceGeneration(traceGeneration),
            nextRequestId: 1,
            pending: {}
        };
    }

    function requestIdKey(requestId) {
        return String(requestId);
    }

    function cloneRequest(request) {
        return {
            protocol: request.protocol,
            type: request.type,
            requestId: request.requestId,
            traceGeneration: request.traceGeneration,
            operation: request.operation,
            payload: request.payload
        };
    }

    function beginRequest(state, operation, payload) {
        if (!state) throw new Error('Trace worker protocol state is required');
        operation = normalizeOperation(operation);
        state.nextRequestId = Math.max(1, Math.floor(Number(state.nextRequestId)) || 1);
        if (!state.pending) state.pending = {};
        state.traceGeneration = normalizedTraceGeneration(state.traceGeneration);

        var request = {
            protocol: PROTOCOL_VERSION,
            type: 'request',
            requestId: state.nextRequestId++,
            traceGeneration: state.traceGeneration,
            operation: operation,
            payload: payload === undefined ? null : payload
        };
        state.pending[requestIdKey(request.requestId)] = cloneRequest(request);
        return request;
    }

    function errorPayload(error, details) {
        if (!error) {
            return {
                message: 'Trace worker operation failed',
                details: details || null
            };
        }
        return {
            name: error.name || 'Error',
            message: error.message !== undefined && error.message !== null ? String(error.message) : String(error),
            stack: error.stack || '',
            details: details || null
        };
    }

    function errorDetailsForRequest(request, details) {
        var enriched = {};
        details = details || {};
        for (var key in details) {
            if (Object.prototype.hasOwnProperty.call(details, key)) {
                enriched[key] = details[key];
            }
        }
        if (request) {
            if (enriched.requestId === undefined) enriched.requestId = request.requestId;
            if (enriched.traceGeneration === undefined) {
                enriched.traceGeneration = normalizedTraceGeneration(request.traceGeneration);
            }
            if (enriched.operation === undefined) enriched.operation = normalizeOperation(request.operation);
        }
        if (enriched.sourceBlockId === undefined) enriched.sourceBlockId = null;
        return enriched;
    }

    function resultForRequest(request, payload) {
        return {
            protocol: PROTOCOL_VERSION,
            type: 'result',
            requestId: request && request.requestId,
            traceGeneration: normalizedTraceGeneration(request && request.traceGeneration),
            operation: normalizeOperation(request && request.operation),
            payload: payload === undefined ? null : payload
        };
    }

    function errorForRequest(request, error, details) {
        var operation = normalizeOperation(request && request.operation);
        return {
            protocol: PROTOCOL_VERSION,
            type: 'error',
            requestId: request && request.requestId,
            traceGeneration: normalizedTraceGeneration(request && request.traceGeneration),
            operation: operation,
            error: errorPayload(error, errorDetailsForRequest(request, details))
        };
    }

    function pendingRequests(state) {
        var pending = state && state.pending || {};
        return Object.keys(pending)
            .sort(function(a, b) { return Number(a) - Number(b); })
            .map(function(key) { return cloneRequest(pending[key]); });
    }

    function completeResponse(state, response) {
        if (!state || !response) return { accepted: false, reason: 'missing-response' };
        if (response.protocol !== PROTOCOL_VERSION) return { accepted: false, reason: 'protocol-mismatch' };
        if (response.type !== 'result' && response.type !== 'error') return { accepted: false, reason: 'type-mismatch' };
        if (normalizedTraceGeneration(response.traceGeneration) !== normalizedTraceGeneration(state.traceGeneration)) {
            return { accepted: false, reason: 'stale-generation' };
        }

        var pending = state.pending || {};
        var key = requestIdKey(response.requestId);
        var request = pending[key];
        if (!request) return { accepted: false, reason: 'unknown-request' };
        if (request.operation !== response.operation) return { accepted: false, reason: 'operation-mismatch' };

        delete pending[key];
        return {
            accepted: true,
            request: cloneRequest(request),
            response: response
        };
    }

    return {
        beginRequest: beginRequest,
        completeResponse: completeResponse,
        createState: createState,
        errorForRequest: errorForRequest,
        pendingRequests: pendingRequests,
        PROTOCOL_VERSION: PROTOCOL_VERSION,
        resultForRequest: resultForRequest
    };
}

var TraceWorkerProtocol = createTraceWorkerProtocol();
