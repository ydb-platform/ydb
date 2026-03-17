import { e as experimental_async_required, a as ssr_context, c as createContext, b as getAllContexts, g as getContext, h as hasContext, s as setContext } from './context-BZS6UlnY.js';
import { b as async_mode_flag, n as noop, k as run } from './async-DWBXLqlH.js';
import { g as get_render_context, u as uneval, h as hydratable_serialization_failed, l as lifecycle_function_unavailable, a as getAbortSignal } from './uneval-ZBzcyJ66.js';

/** @import { HydratableLookupEntry } from '#server' */

/**
 * @template T
 * @param {string} key
 * @param {() => T} fn
 * @returns {T}
 */
function hydratable(key, fn) {
	if (!async_mode_flag) {
		experimental_async_required();
	}

	const { hydratable } = get_render_context();

	let entry = hydratable.lookup.get(key);

	if (entry !== undefined) {

		return /** @type {T} */ (entry.value);
	}

	const value = fn();

	entry = encode(key, value, hydratable.unresolved_promises);
	hydratable.lookup.set(key, entry);

	return value;
}

/**
 * @param {string} key
 * @param {any} value
 * @param {Map<Promise<any>, string>} [unresolved]
 */
function encode(key, value, unresolved) {
	/** @type {HydratableLookupEntry} */
	const entry = { value, serialized: '' };

	let uid = 1;

	entry.serialized = uneval(entry.value, (value, uneval) => {
		if (is_promise(value)) {
			// we serialize promises as `"${i}"`, because it's impossible for that string
			// to occur 'naturally' (since the quote marks would have to be escaped)
			// this placeholder is returned synchronously from `uneval`, which includes it in the
			// serialized string. Later (at least one microtask from now), when `p.then` runs, it'll
			// be replaced.
			const placeholder = `"${uid++}"`;
			const p = value
				.then((v) => {
					entry.serialized = entry.serialized.replace(placeholder, `r(${uneval(v)})`);
				})
				.catch((devalue_error) =>
					hydratable_serialization_failed(
						key,
						serialization_stack(entry.stack, devalue_error?.stack)
					)
				);

			unresolved?.set(p, key);
			// prevent unhandled rejections from crashing the server, track which promises are still resolving when render is complete
			p.catch(() => {}).finally(() => unresolved?.delete(p));

			(entry.promises ??= []).push(p);
			return placeholder;
		}
	});

	return entry;
}

/**
 * @param {any} value
 * @returns {value is Promise<any>}
 */
function is_promise(value) {
	// we use this check rather than `instanceof Promise`
	// because it works cross-realm
	return Object.prototype.toString.call(value) === '[object Promise]';
}

/**
 * @param {string | undefined} root_stack
 * @param {string | undefined} uneval_stack
 */
function serialization_stack(root_stack, uneval_stack) {
	let out = '';
	if (root_stack) {
		out += root_stack + '\n';
	}
	if (uneval_stack) {
		out += 'Caused by:\n' + uneval_stack + '\n';
	}
	return out || '<missing stack trace>';
}

/** @import { Snippet } from 'svelte' */
/** @import { Renderer } from '../renderer' */
/** @import { Getters } from '#shared' */

/**
 * Create a snippet programmatically
 * @template {unknown[]} Params
 * @param {(...params: Getters<Params>) => {
 *   render: () => string
 *   setup?: (element: Element) => void | (() => void)
 * }} fn
 * @returns {Snippet<Params>}
 */
function createRawSnippet(fn) {
	// @ts-expect-error the types are a lie
	return (/** @type {Renderer} */ renderer, /** @type {Params} */ ...args) => {
		var getters = /** @type {Getters<Params>} */ (args.map((value) => () => value));
		renderer.push(
			fn(...getters)
				.render()
				.trim()
		);
	};
}

/** @import { SSRContext } from '#server' */
/** @import { Renderer } from './internal/server/renderer.js' */

/** @param {() => void} fn */
function onDestroy(fn) {
	/** @type {Renderer} */ (/** @type {SSRContext} */ (ssr_context).r).on_destroy(fn);
}

function createEventDispatcher() {
	return noop;
}

function mount() {
	lifecycle_function_unavailable('mount');
}

function hydrate() {
	lifecycle_function_unavailable('hydrate');
}

function unmount() {
	lifecycle_function_unavailable('unmount');
}

function fork() {
	lifecycle_function_unavailable('fork');
}

async function tick() {}

async function settled() {}

var o = /*#__PURE__*/Object.freeze({
	__proto__: null,
	afterUpdate: noop,
	beforeUpdate: noop,
	createContext: createContext,
	createEventDispatcher: createEventDispatcher,
	createRawSnippet: createRawSnippet,
	flushSync: noop,
	fork: fork,
	getAbortSignal: getAbortSignal,
	getAllContexts: getAllContexts,
	getContext: getContext,
	hasContext: hasContext,
	hydratable: hydratable,
	hydrate: hydrate,
	mount: mount,
	onDestroy: onDestroy,
	onMount: noop,
	setContext: setContext,
	settled: settled,
	tick: tick,
	unmount: unmount,
	untrack: run
});

export { o as a, createEventDispatcher as c, onDestroy as o, tick as t };
//# sourceMappingURL=index-server-D4bj_1UO.js.map
