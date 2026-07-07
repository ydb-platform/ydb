/* brotli-dec-wasm v2.3.0, MIT OR Apache-2.0; embedded under MIT terms. See runtime/js/vendor/brotli_dec_wasm/LICENSE-MIT. */
(function(root) {
'use strict';
let wasm;

const cachedTextDecoder = (typeof TextDecoder !== 'undefined' ? new TextDecoder('utf-8', { ignoreBOM: true, fatal: true }) : { decode: () => { throw Error('TextDecoder not available') } } );

if (typeof TextDecoder !== 'undefined') { cachedTextDecoder.decode(); };

let cachedUint8Memory0 = null;

function getUint8Memory0() {
    if (cachedUint8Memory0 === null || cachedUint8Memory0.byteLength === 0) {
        cachedUint8Memory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8Memory0;
}

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return cachedTextDecoder.decode(getUint8Memory0().subarray(ptr, ptr + len));
}

const heap = new Array(128).fill(undefined);

heap.push(undefined, null, true, false);

let heap_next = heap.length;

function addHeapObject(obj) {
    if (heap_next === heap.length) heap.push(heap.length + 1);
    const idx = heap_next;
    heap_next = heap[idx];

    heap[idx] = obj;
    return idx;
}

function getObject(idx) { return heap[idx]; }

function dropObject(idx) {
    if (idx < 132) return;
    heap[idx] = heap_next;
    heap_next = idx;
}

function takeObject(idx) {
    const ret = getObject(idx);
    dropObject(idx);
    return ret;
}

let WASM_VECTOR_LEN = 0;

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1, 1) >>> 0;
    getUint8Memory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}

let cachedInt32Memory0 = null;

function getInt32Memory0() {
    if (cachedInt32Memory0 === null || cachedInt32Memory0.byteLength === 0) {
        cachedInt32Memory0 = new Int32Array(wasm.memory.buffer);
    }
    return cachedInt32Memory0;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8Memory0().subarray(ptr / 1, ptr / 1 + len);
}
/**
* No error reporting included.
* To get the detailed error code, use [`stream::BrotliDecStream`].
* @param {Uint8Array} input
* @returns {Uint8Array}
*/
function brotli_dec(input) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        wasm.brotli_dec(retptr, ptr0, len0);
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        var r3 = getInt32Memory0()[retptr / 4 + 3];
        if (r3) {
            throw takeObject(r2);
        }
        var v2 = getArrayU8FromWasm0(r0, r1).slice();
        wasm.__wbindgen_free(r0, r1 * 1, 1);
        return v2;
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
* See [`brotli_dec`].
*
* For drop-in replacement of `brotli-wasm`.
* @param {Uint8Array} buf
* @returns {Uint8Array}
*/
function decompress(buf) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        const ptr0 = passArray8ToWasm0(buf, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        wasm.brotli_dec(retptr, ptr0, len0);
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        var r3 = getInt32Memory0()[retptr / 4 + 3];
        if (r3) {
            throw takeObject(r2);
        }
        var v2 = getArrayU8FromWasm0(r0, r1).slice();
        wasm.__wbindgen_free(r0, r1 * 1, 1);
        return v2;
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

const cachedTextEncoder = (typeof TextEncoder !== 'undefined' ? new TextEncoder('utf-8') : { encode: () => { throw Error('TextEncoder not available') } } );

const encodeString = (typeof cachedTextEncoder.encodeInto === 'function'
    ? function (arg, view) {
    return cachedTextEncoder.encodeInto(arg, view);
}
    : function (arg, view) {
    const buf = cachedTextEncoder.encode(arg);
    view.set(buf);
    return {
        read: arg.length,
        written: buf.length
    };
});

function passStringToWasm0(arg, malloc, realloc) {

    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8Memory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8Memory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }

    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8Memory0().subarray(ptr + offset, ptr + len);
        const ret = encodeString(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}
/**
* Same as [`brotli_decompressor::BrotliResult`] except [`brotli_decompressor::BrotliResult::ResultFailure`].
*
* Always `> 0`.
*
* `ResultFailure` is removed
* because we will convert the failure to an actual negative error code (if available) and pass it elsewhere.
*/
const BrotliStreamResultCode = Object.freeze({ ResultSuccess:1,"1":"ResultSuccess",NeedsMoreInput:2,"2":"NeedsMoreInput",NeedsMoreOutput:3,"3":"NeedsMoreOutput", });
/**
* From [`brotli_decompressor::state::BrotliDecoderErrorCode`].
* NOTICE: All numbers are reversed to positive, required by wasm_bindgen.
*/
const BrotliDecStreamErrCode = Object.freeze({ BROTLI_DECODER_ERROR_FORMAT_EXUBERANT_NIBBLE:1,"1":"BROTLI_DECODER_ERROR_FORMAT_EXUBERANT_NIBBLE",BROTLI_DECODER_ERROR_FORMAT_RESERVED:2,"2":"BROTLI_DECODER_ERROR_FORMAT_RESERVED",BROTLI_DECODER_ERROR_FORMAT_EXUBERANT_META_NIBBLE:3,"3":"BROTLI_DECODER_ERROR_FORMAT_EXUBERANT_META_NIBBLE",BROTLI_DECODER_ERROR_FORMAT_SIMPLE_HUFFMAN_ALPHABET:4,"4":"BROTLI_DECODER_ERROR_FORMAT_SIMPLE_HUFFMAN_ALPHABET",BROTLI_DECODER_ERROR_FORMAT_SIMPLE_HUFFMAN_SAME:5,"5":"BROTLI_DECODER_ERROR_FORMAT_SIMPLE_HUFFMAN_SAME",BROTLI_DECODER_ERROR_FORMAT_CL_SPACE:6,"6":"BROTLI_DECODER_ERROR_FORMAT_CL_SPACE",BROTLI_DECODER_ERROR_FORMAT_HUFFMAN_SPACE:7,"7":"BROTLI_DECODER_ERROR_FORMAT_HUFFMAN_SPACE",BROTLI_DECODER_ERROR_FORMAT_CONTEXT_MAP_REPEAT:8,"8":"BROTLI_DECODER_ERROR_FORMAT_CONTEXT_MAP_REPEAT",BROTLI_DECODER_ERROR_FORMAT_BLOCK_LENGTH_1:9,"9":"BROTLI_DECODER_ERROR_FORMAT_BLOCK_LENGTH_1",BROTLI_DECODER_ERROR_FORMAT_BLOCK_LENGTH_2:10,"10":"BROTLI_DECODER_ERROR_FORMAT_BLOCK_LENGTH_2",BROTLI_DECODER_ERROR_FORMAT_TRANSFORM:11,"11":"BROTLI_DECODER_ERROR_FORMAT_TRANSFORM",BROTLI_DECODER_ERROR_FORMAT_DICTIONARY:12,"12":"BROTLI_DECODER_ERROR_FORMAT_DICTIONARY",BROTLI_DECODER_ERROR_FORMAT_WINDOW_BITS:13,"13":"BROTLI_DECODER_ERROR_FORMAT_WINDOW_BITS",BROTLI_DECODER_ERROR_FORMAT_PADDING_1:14,"14":"BROTLI_DECODER_ERROR_FORMAT_PADDING_1",BROTLI_DECODER_ERROR_FORMAT_PADDING_2:15,"15":"BROTLI_DECODER_ERROR_FORMAT_PADDING_2",BROTLI_DECODER_ERROR_FORMAT_DISTANCE:16,"16":"BROTLI_DECODER_ERROR_FORMAT_DISTANCE",BROTLI_DECODER_ERROR_DICTIONARY_NOT_SET:19,"19":"BROTLI_DECODER_ERROR_DICTIONARY_NOT_SET",BROTLI_DECODER_ERROR_INVALID_ARGUMENTS:20,"20":"BROTLI_DECODER_ERROR_INVALID_ARGUMENTS",BROTLI_DECODER_ERROR_ALLOC_CONTEXT_MODES:21,"21":"BROTLI_DECODER_ERROR_ALLOC_CONTEXT_MODES",BROTLI_DECODER_ERROR_ALLOC_TREE_GROUPS:22,"22":"BROTLI_DECODER_ERROR_ALLOC_TREE_GROUPS",BROTLI_DECODER_ERROR_ALLOC_CONTEXT_MAP:25,"25":"BROTLI_DECODER_ERROR_ALLOC_CONTEXT_MAP",BROTLI_DECODER_ERROR_ALLOC_RING_BUFFER_1:26,"26":"BROTLI_DECODER_ERROR_ALLOC_RING_BUFFER_1",BROTLI_DECODER_ERROR_ALLOC_RING_BUFFER_2:27,"27":"BROTLI_DECODER_ERROR_ALLOC_RING_BUFFER_2",BROTLI_DECODER_ERROR_ALLOC_BLOCK_TYPE_TREES:30,"30":"BROTLI_DECODER_ERROR_ALLOC_BLOCK_TYPE_TREES",BROTLI_DECODER_ERROR_UNREACHABLE:31,"31":"BROTLI_DECODER_ERROR_UNREACHABLE", });

const BrotliDecStreamFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_brotlidecstream_free(ptr >>> 0));
/**
*/
class BrotliDecStream {

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrotliDecStreamFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_brotlidecstream_free(ptr);
    }
    /**
    */
    constructor() {
        const ret = wasm.brotlidecstream_new();
        this.__wbg_ptr = ret >>> 0;
        return this;
    }
    /**
    * @param {Uint8Array} input
    * @param {number} output_size
    * @returns {BrotliStreamResult}
    */
    dec(input, output_size) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
            const len0 = WASM_VECTOR_LEN;
            wasm.brotlidecstream_dec(retptr, this.__wbg_ptr, ptr0, len0, output_size);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return BrotliStreamResult.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * See [`Self::dec()`].
    *
    * For drop-in replacement of `brotli-wasm`.
    * @param {Uint8Array} input
    * @param {number} output_size
    * @returns {BrotliStreamResult}
    */
    decompress(input, output_size) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
            const len0 = WASM_VECTOR_LEN;
            wasm.brotlidecstream_dec(retptr, this.__wbg_ptr, ptr0, len0, output_size);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return BrotliStreamResult.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {number}
    */
    total_out() {
        const ret = wasm.brotlidecstream_total_out(this.__wbg_ptr);
        return ret >>> 0;
    }
}

const BrotliStreamResultFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_brotlistreamresult_free(ptr >>> 0));
/**
* Returned by every successful (de)compression.
*/
class BrotliStreamResult {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BrotliStreamResult.prototype);
        obj.__wbg_ptr = ptr;
        BrotliStreamResultFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BrotliStreamResultFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_brotlistreamresult_free(ptr);
    }
    /**
    * Result code.
    *
    * See [`BrotliStreamResultCode`] for available values.
    *
    * When error, the error code is not passed here but rather goes to `Err`.
    * @returns {BrotliStreamResultCode}
    */
    get code() {
        const ret = wasm.__wbg_get_brotlistreamresult_code(this.__wbg_ptr);
        return ret;
    }
    /**
    * Result code.
    *
    * See [`BrotliStreamResultCode`] for available values.
    *
    * When error, the error code is not passed here but rather goes to `Err`.
    * @param {BrotliStreamResultCode} arg0
    */
    set code(arg0) {
        wasm.__wbg_set_brotlistreamresult_code(this.__wbg_ptr, arg0);
    }
    /**
    * Output buffer
    * @returns {Uint8Array}
    */
    get buf() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.__wbg_get_brotlistreamresult_buf(retptr, this.__wbg_ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var v1 = getArrayU8FromWasm0(r0, r1).slice();
            wasm.__wbindgen_free(r0, r1 * 1, 1);
            return v1;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * Output buffer
    * @param {Uint8Array} arg0
    */
    set buf(arg0) {
        const ptr0 = passArray8ToWasm0(arg0, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        wasm.__wbg_set_brotlistreamresult_buf(this.__wbg_ptr, ptr0, len0);
    }
    /**
    * Consumed bytes of the input buffer
    * @returns {number}
    */
    get input_offset() {
        const ret = wasm.__wbg_get_brotlistreamresult_input_offset(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
    * Consumed bytes of the input buffer
    * @param {number} arg0
    */
    set input_offset(arg0) {
        wasm.__wbg_set_brotlistreamresult_input_offset(this.__wbg_ptr, arg0);
    }
}

const DecompressStreamFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_decompressstream_free(ptr >>> 0));
/**
* See [`BrotliDecStream`].
*
* For drop-in replacement of `brotli-wasm`.
*/
class DecompressStream {

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        DecompressStreamFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_decompressstream_free(ptr);
    }
    /**
    */
    constructor() {
        const ret = wasm.decompressstream_new();
        this.__wbg_ptr = ret >>> 0;
        return this;
    }
    /**
    * @param {Uint8Array} input
    * @param {number} output_size
    * @returns {BrotliStreamResult}
    */
    decompress(input, output_size) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passArray8ToWasm0(input, wasm.__wbindgen_malloc);
            const len0 = WASM_VECTOR_LEN;
            wasm.brotlidecstream_dec(retptr, this.__wbg_ptr, ptr0, len0, output_size);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return BrotliStreamResult.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {number}
    */
    total_out() {
        const ret = wasm.decompressstream_total_out(this.__wbg_ptr);
        return ret >>> 0;
    }
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);

            } catch (e) {
                if (module.headers.get('Content-Type') != 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else {
                    throw e;
                }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);

    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };

        } else {
            return instance;
        }
    }
}

function __wbg_get_imports() {
    const imports = {};
    imports.wbg = {};
    imports.wbg.__wbindgen_error_new = function(arg0, arg1) {
        const ret = new Error(getStringFromWasm0(arg0, arg1));
        return addHeapObject(ret);
    };
    imports.wbg.__wbindgen_object_drop_ref = function(arg0) {
        takeObject(arg0);
    };
    imports.wbg.__wbg_new_abda76e883ba8a5f = function() {
        const ret = new Error();
        return addHeapObject(ret);
    };
    imports.wbg.__wbg_stack_658279fe44541cf6 = function(arg0, arg1) {
        const ret = getObject(arg1).stack;
        const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        getInt32Memory0()[arg0 / 4 + 1] = len1;
        getInt32Memory0()[arg0 / 4 + 0] = ptr1;
    };
    imports.wbg.__wbg_error_f851667af71bcfc6 = function(arg0, arg1) {
        let deferred0_0;
        let deferred0_1;
        try {
            deferred0_0 = arg0;
            deferred0_1 = arg1;
            console.error(getStringFromWasm0(arg0, arg1));
        } finally {
            wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
        }
    };
    imports.wbg.__wbindgen_throw = function(arg0, arg1) {
        throw new Error(getStringFromWasm0(arg0, arg1));
    };

    return imports;
}

function __wbg_init_memory(imports, maybe_memory) {

}

function __wbg_finalize_init(instance, module) {
    wasm = instance.exports;
    __wbg_init.__wbindgen_wasm_module = module;
    cachedInt32Memory0 = null;
    cachedUint8Memory0 = null;


    return wasm;
}

function initSync(module) {
    if (wasm !== undefined) return wasm;

    const imports = __wbg_get_imports();

    __wbg_init_memory(imports);

    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }

    const instance = new WebAssembly.Instance(module, imports);

    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(input) {
    if (wasm !== undefined) return wasm;

    if (typeof input === 'undefined') {
        throw new Error('Brotli decoder requires embedded WASM bytes');
    }
    const imports = __wbg_get_imports();

    if (typeof input === 'string' || (typeof Request === 'function' && input instanceof Request) || (typeof URL === 'function' && input instanceof URL)) {
        input = fetch(input);
    }

    __wbg_init_memory(imports);

    const { instance, module } = await __wbg_load(await input, imports);

    return __wbg_finalize_init(instance, module);
}



function base64UrlToBytes(input) {
    input = String(input || "");
    var alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    var out = [];
    var buffer = 0;
    var bits = 0;
    for (var i = 0; i < input.length; i++) {
        var value = alphabet.indexOf(input.charAt(i));
        if (value < 0) continue;
        buffer = (buffer << 6) | value;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push((buffer >> bits) & 0xFF);
        }
    }
    return new Uint8Array(out);
}
var embeddedWasmBytes = null;
function setWasmBase64(input) {
    embeddedWasmBytes = base64UrlToBytes(input);
}
function ensureInitialized() {
    if (wasm !== undefined) return;
    if (!embeddedWasmBytes) throw new Error("Embedded Brotli decoder WASM is missing");
    initSync(embeddedWasmBytes);
}
function decode(input) {
    ensureInitialized();
    return brotli_dec(input);
}
root.OptimizerTraceBrotliDecoder = {
    decode: decode,
    decompress: decode,
    setWasmBase64: setWasmBase64
};
})(typeof globalThis !== "undefined" ? globalThis : (typeof window !== "undefined" ? window : this));
