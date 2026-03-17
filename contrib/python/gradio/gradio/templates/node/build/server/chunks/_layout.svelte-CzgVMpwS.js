import { D as active_reaction, G as untrack, I as effect_tracking, P as PROXY_PATH_SYMBOL, U as UNINITIALIZED, J as STATE_SYMBOL, K as DERIVED, L as ASYNC, M as define_property, N as get_proxied_value, O as state_proxy_equality_mismatch, n as noop, Q as ATTACHMENT_KEY, r as render_effect, R as teardown, T as assignment_value_stale, V as sanitize_location, W as hydrating, X as DOCUMENT_FRAGMENT_NODE, Y as COMMENT_NODE, H as HYDRATION_START, t as HYDRATION_START_ELSE, q as HYDRATION_END, Z as ELEMENT_NODE, _ as dev_stack, F as FILENAME, $ as source, B as HMR, h as set, a0 as block, a1 as EFFECT_TRANSPARENT, j as get, a2 as branch, a3 as destroy_effect, a4 as hydrate_node, a5 as set_should_intro, a6 as component_context, a7 as ownership_invalid_binding, a8 as ownership_invalid_mutation, a9 as get_descriptor, aa as LEGACY_PROPS, ab as component_api_invalid_new, ac as component_api_changed, ad as validate_effect, ae as eager_effect, af as get_boundary, ag as current_batch, ah as hydrate_next, ai as skip_nodes, aj as set_hydrate_node, ak as flatten, al as set_hydrating, am as resume_effect, an as pause_effect, ao as should_defer_append, ap as create_text, aq as move_effect, ar as is_runes, A as is_promise, as as queue_micro_task, at as internal_set, au as mutable_source, av as capture, aw as Batch, ax as unset_context, ay as is_flushing_sync, az as flushSync, aA as read_hydration_instruction, aB as get_first_child, aC as derived_safe_equal, aD as EACH_IS_CONTROLLED, aE as EACH_INDEX_REACTIVE, aF as EACH_ITEM_REACTIVE, aG as EACH_ITEM_IMMUTABLE, aH as array_from, i as is_array, aI as EFFECT_OFFSCREEN, aJ as INERT, aK as get_next_sibling, aL as EACH_IS_ANIMATED, aM as clear_text_content, aN as template_effect, aO as active_effect, aP as remove_effect_dom, aQ as hydration_mismatch, aR as HYDRATION_ERROR, aS as assign_nodes, aT as create_fragment_from_html, aU as set_dev_current_component_function, aV as dev_current_component_function, aW as should_intro, aX as BLOCK_EFFECT, aY as EFFECT_RAN, aZ as effect, a_ as is_function, a$ as TRANSITION_GLOBAL, b0 as TRANSITION_IN, b1 as TRANSITION_OUT, b2 as without_reactive_context, b3 as NAMESPACE_SVG, z as is_raw_text_element, b4 as HEAD_EFFECT, b5 as deep_read_state, b6 as safe_not_equal, b7 as managed, b8 as listen_to_event_and_reset_event, b9 as select_multiple_invalid_value, ba as is, bb as previous_batch, bc as add_form_reset_listener, bd as LOADING_ATTR_SYMBOL, be as set_active_reaction, bf as set_active_effect, bg as NAMESPACE_HTML, e as get_prototype_of, bh as get_descriptors, bi as is_capture_event, bj as create_event, bk as delegate, bl as autofocus, bm as normalize_attribute, bn as can_delegate_event, bo as create_comment, bp as reset, bq as listen, br as tick, bs as user_pre_effect, bt as user_effect, bu as run_all, k as run, bv as derived, y as subscribe_to_store, g as get$1, bw as props_invalid_value, bx as PROPS_IS_UPDATED, by as proxy, bz as is_destroying_effect, bA as DESTROYED, bB as PROPS_IS_BINDABLE, bC as legacy_mode_flag, bD as PROPS_IS_RUNES, bE as PROPS_IS_IMMUTABLE, bF as update, bG as PROPS_IS_LAZY_INITIAL, bH as run_after_blockers, bI as each_key_duplicate, bJ as binding_property_non_reactive, bK as console_log_state, bL as aborted, bM as add_svelte_meta, bN as append, bO as apply, bP as async_derived, bQ as boundary, bR as child, bS as comment, bT as create_custom_element, bU as deep_read, bV as deferred_template_effect, bW as user_derived, bX as $document, bY as eager, bZ as effect_root, b_ as event, b$ as exclude_from_object, f as fallback, c0 as first_child, c1 as for_await_track_reactivity_loss, c2 as from_html, c3 as from_mathml, c4 as from_svg, c5 as from_tree, c6 as hydrate_template, c7 as invalidate_inner_signals, c8 as invoke_error_boundary, c9 as legacy_pre_effect, ca as legacy_pre_effect_reset, cb as mutate, cc as next, cd as pending$1, ce as pop, cf as props_id, cg as push, ch as remove_textarea_child, ci as replay_events, cj as run$1, ck as safe_get, cl as save, cm as set_text, cn as sibling, s as state, co as text, C as to_array, cp as track_reactivity_loss, cq as update_pre, cr as $window, cs as with_script, w as writable, d as derived$1, l as readable, ct as readonly, cu as enable_legacy_mode_flag, cv as enable_tracing_mode_flag, cw as on, cx as createSubscriber$1, cy as n } from './async-DWBXLqlH.js';
import { t as prevent_snippet_stringification, v as to_class, w as to_style, k as clsx, a as attr, x as validate_dynamic_element_tag, y as validate_store, z as validate_void_dynamic_element, r as render, i as slot$1, A as s } from './index-D1re1cuM.js';
import { a as o } from './index-server-D4bj_1UO.js';
import { c as cubicOut, T as Tween, t as tweened, r } from './tweened-Dh8Fmjhp.js';
import { s as snapshot } from './clone-CubQhOZi.js';
import { l as invalid_snippet_arguments, m as invalid_default_snippet } from './context-BZS6UlnY.js';
import { l as loop, r as raf, S as Spring, s as spring } from './spring-D6sHki8W.js';
import { f } from './legacy-server-Dv3YIwSp.js';
import './escaping-CBnpiEl5.js';
import './uneval-ZBzcyJ66.js';

/** @import { Derived, Reaction, Value } from '#client' */

/**
 * @typedef {{
 *   traces: Error[];
 * }} TraceEntry
 */

/** @type {{ reaction: Reaction | null, entries: Map<Value, TraceEntry> } | null} */
let tracing_expressions = null;

/**
 * @param {Value} signal
 * @param {TraceEntry} [entry]
 */
function log_entry(signal, entry) {
	const value = signal.v;

	if (value === UNINITIALIZED) {
		return;
	}

	const type = get_type(signal);
	const current_reaction = /** @type {Reaction} */ (active_reaction);
	const dirty = signal.wv > current_reaction.wv || current_reaction.wv === 0;
	const style = dirty
		? 'color: CornflowerBlue; font-weight: bold'
		: 'color: grey; font-weight: normal';

	// eslint-disable-next-line no-console
	console.groupCollapsed(
		signal.label ? `%c${type}%c ${signal.label}` : `%c${type}%c`,
		style,
		dirty ? 'font-weight: normal' : style,
		typeof value === 'object' && value !== null && STATE_SYMBOL in value
			? snapshot(value, true)
			: value
	);

	if (type === '$derived') {
		const deps = new Set(/** @type {Derived} */ (signal).deps);
		for (const dep of deps) {
			log_entry(dep);
		}
	}

	if (signal.created) {
		// eslint-disable-next-line no-console
		console.log(signal.created);
	}

	if (dirty && signal.updated) {
		for (const updated of signal.updated.values()) {
			if (updated.error) {
				// eslint-disable-next-line no-console
				console.log(updated.error);
			}
		}
	}

	if (entry) {
		for (var trace of entry.traces) {
			// eslint-disable-next-line no-console
			console.log(trace);
		}
	}

	// eslint-disable-next-line no-console
	console.groupEnd();
}

/**
 * @param {Value} signal
 * @returns {'$state' | '$derived' | 'store'}
 */
function get_type(signal) {
	if ((signal.f & (DERIVED | ASYNC)) !== 0) return '$derived';
	return signal.label?.startsWith('$') ? 'store' : '$state';
}

/**
 * @template T
 * @param {() => string} label
 * @param {() => T} fn
 */
function trace(label, fn) {
	var previously_tracing_expressions = tracing_expressions;

	try {
		tracing_expressions = { entries: new Map(), reaction: active_reaction };

		var start = performance.now();
		var value = fn();
		var time = (performance.now() - start).toFixed(2);

		var prefix = untrack(label);

		if (!effect_tracking()) {
			// eslint-disable-next-line no-console
			console.log(`${prefix} %cran outside of an effect (${time}ms)`, 'color: grey');
		} else if (tracing_expressions.entries.size === 0) {
			// eslint-disable-next-line no-console
			console.log(`${prefix} %cno reactive dependencies (${time}ms)`, 'color: grey');
		} else {
			// eslint-disable-next-line no-console
			console.group(`${prefix} %c(${time}ms)`, 'color: grey');

			var entries = tracing_expressions.entries;

			untrack(() => {
				for (const [signal, traces] of entries) {
					log_entry(signal, traces);
				}
			});

			tracing_expressions = null;

			// eslint-disable-next-line no-console
			console.groupEnd();
		}

		return value;
	} finally {
		tracing_expressions = previously_tracing_expressions;
	}
}

/**
 * @param {Value} source
 * @param {string} label
 */
function tag(source, label) {
	source.label = label;
	tag_proxy(source.v, label);

	return source;
}

/**
 * @param {unknown} value
 * @param {string} label
 */
function tag_proxy(value, label) {
	// @ts-expect-error
	value?.[PROXY_PATH_SYMBOL]?.(label);
	return value;
}

/**
 * @param {string} label
 * @returns {Error & { stack: string } | null}
 */
function get_error(label) {
	const error = new Error();
	const stack = get_stack();

	if (stack.length === 0) {
		return null;
	}

	stack.unshift('\n');

	define_property(error, 'stack', {
		value: stack.join('\n')
	});

	define_property(error, 'name', {
		value: label
	});

	return /** @type {Error & { stack: string }} */ (error);
}

/**
 * @returns {string[]}
 */
function get_stack() {
	// @ts-ignore - doesn't exist everywhere
	const limit = Error.stackTraceLimit;
	// @ts-ignore - doesn't exist everywhere
	Error.stackTraceLimit = Infinity;
	const stack = new Error().stack;
	// @ts-ignore - doesn't exist everywhere
	Error.stackTraceLimit = limit;

	if (!stack) return [];

	const lines = stack.split('\n');
	const new_lines = [];

	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		const posixified = line.replaceAll('\\', '/');

		if (line.trim() === 'Error') {
			continue;
		}

		if (line.includes('validate_each_keys')) {
			return [];
		}

		if (posixified.includes('svelte/src/internal') || posixified.includes('node_modules/.vite')) {
			continue;
		}

		new_lines.push(line);
	}

	return new_lines;
}

/**
 * @param {any} a
 * @param {any} b
 * @param {boolean} equal
 * @returns {boolean}
 */
function strict_equals(a, b, equal = true) {
	// try-catch needed because this tries to read properties of `a` and `b`,
	// which could be disallowed for example in a secure context
	try {
		if ((a === b) !== (get_proxied_value(a) === get_proxied_value(b))) {
			state_proxy_equality_mismatch(equal ? '===' : '!==');
		}
	} catch {}

	return (a === b) === equal;
}

/**
 * @param {any} a
 * @param {any} b
 * @param {boolean} equal
 * @returns {boolean}
 */
function equals(a, b, equal = true) {
	if ((a == b) !== (get_proxied_value(a) == get_proxied_value(b))) {
		state_proxy_equality_mismatch();
	}

	return (a == b) === equal;
}

/**
 * Substitute for the `trusted` event modifier
 * @deprecated
 * @param {(event: Event, ...args: Array<unknown>) => void} fn
 * @returns {(event: Event, ...args: unknown[]) => void}
 */
function trusted(fn) {
	return function (...args) {
		var event = /** @type {Event} */ (args[0]);
		if (event.isTrusted) {
			// @ts-ignore
			fn?.apply(this, args);
		}
	};
}

/**
 * Substitute for the `self` event modifier
 * @deprecated
 * @param {(event: Event, ...args: Array<unknown>) => void} fn
 * @returns {(event: Event, ...args: unknown[]) => void}
 */
function self(fn) {
	return function (...args) {
		var event = /** @type {Event} */ (args[0]);
		// @ts-ignore
		if (event.target === this) {
			// @ts-ignore
			fn?.apply(this, args);
		}
	};
}

/**
 * Substitute for the `stopPropagation` event modifier
 * @deprecated
 * @param {(event: Event, ...args: Array<unknown>) => void} fn
 * @returns {(event: Event, ...args: unknown[]) => void}
 */
function stopPropagation(fn) {
	return function (...args) {
		var event = /** @type {Event} */ (args[0]);
		event.stopPropagation();
		// @ts-ignore
		return fn?.apply(this, args);
	};
}

/**
 * Substitute for the `once` event modifier
 * @deprecated
 * @param {(event: Event, ...args: Array<unknown>) => void} fn
 * @returns {(event: Event, ...args: unknown[]) => void}
 */
function once(fn) {
	var ran = false;

	return function (...args) {
		if (ran) return;
		ran = true;

		// @ts-ignore
		return fn?.apply(this, args);
	};
}

/**
 * Substitute for the `stopImmediatePropagation` event modifier
 * @deprecated
 * @param {(event: Event, ...args: Array<unknown>) => void} fn
 * @returns {(event: Event, ...args: unknown[]) => void}
 */
function stopImmediatePropagation(fn) {
	return function (...args) {
		var event = /** @type {Event} */ (args[0]);
		event.stopImmediatePropagation();
		// @ts-ignore
		return fn?.apply(this, args);
	};
}

/**
 * Substitute for the `preventDefault` event modifier
 * @deprecated
 * @param {(event: Event, ...args: Array<unknown>) => void} fn
 * @returns {(event: Event, ...args: unknown[]) => void}
 */
function preventDefault(fn) {
	return function (...args) {
		var event = /** @type {Event} */ (args[0]);
		event.preventDefault();
		// @ts-ignore
		return fn?.apply(this, args);
	};
}

/** @import { Action, ActionReturn } from '../action/public' */
/** @import { Attachment } from './public' */

/**
 * Creates an object key that will be recognised as an attachment when the object is spread onto an element,
 * as a programmatic alternative to using `{@attach ...}`. This can be useful for library authors, though
 * is generally not needed when building an app.
 *
 * ```svelte
 * <script>
 * 	import { createAttachmentKey } from 'svelte/attachments';
 *
 * 	const props = {
 * 		class: 'cool',
 * 		onclick: () => alert('clicked'),
 * 		[createAttachmentKey()]: (node) => {
 * 			node.textContent = 'attached!';
 * 		}
 * 	};
 * </script>
 *
 * <button {...props}>click me</button>
 * ```
 * @since 5.29
 */
function createAttachmentKey() {
	return Symbol(ATTACHMENT_KEY);
}

/**
 * Converts an [action](https://svelte.dev/docs/svelte/use) into an [attachment](https://svelte.dev/docs/svelte/@attach) keeping the same behavior.
 * It's useful if you want to start using attachments on components but you have actions provided by a library.
 *
 * Note that the second argument, if provided, must be a function that _returns_ the argument to the
 * action function, not the argument itself.
 *
 * ```svelte
 * <!-- with an action -->
 * <div use:foo={bar}>...</div>
 *
 * <!-- with an attachment -->
 * <div {@attach fromAction(foo, () => bar)}>...</div>
 * ```
 * @template {EventTarget} E
 * @template {unknown} T
 * @overload
 * @param {Action<E, T> | ((element: E, arg: T) => void | ActionReturn<T>)} action The action function
 * @param {() => T} fn A function that returns the argument for the action
 * @returns {Attachment<E>}
 */
/**
 * Converts an [action](https://svelte.dev/docs/svelte/use) into an [attachment](https://svelte.dev/docs/svelte/@attach) keeping the same behavior.
 * It's useful if you want to start using attachments on components but you have actions provided by a library.
 *
 * Note that the second argument, if provided, must be a function that _returns_ the argument to the
 * action function, not the argument itself.
 *
 * ```svelte
 * <!-- with an action -->
 * <div use:foo={bar}>...</div>
 *
 * <!-- with an attachment -->
 * <div {@attach fromAction(foo, () => bar)}>...</div>
 * ```
 * @template {EventTarget} E
 * @overload
 * @param {Action<E, void> | ((element: E) => void | ActionReturn<void>)} action The action function
 * @returns {Attachment<E>}
 */
/**
 * Converts an [action](https://svelte.dev/docs/svelte/use) into an [attachment](https://svelte.dev/docs/svelte/@attach) keeping the same behavior.
 * It's useful if you want to start using attachments on components but you have actions provided by a library.
 *
 * Note that the second argument, if provided, must be a function that _returns_ the argument to the
 * action function, not the argument itself.
 *
 * ```svelte
 * <!-- with an action -->
 * <div use:foo={bar}>...</div>
 *
 * <!-- with an attachment -->
 * <div {@attach fromAction(foo, () => bar)}>...</div>
 * ```
 *
 * @template {EventTarget} E
 * @template {unknown} T
 * @param {Action<E, T> | ((element: E, arg: T) => void | ActionReturn<T>)} action The action function
 * @param {() => T} fn A function that returns the argument for the action
 * @returns {Attachment<E>}
 * @since 5.32
 */
function fromAction(action, fn = /** @type {() => T} */ (noop)) {
	return (element) => {
		const { update, destroy } = untrack(() => action(element, fn()) ?? {});

		if (update) {
			var ran = false;
			render_effect(() => {
				const arg = fn();
				if (ran) update(arg);
			});
			ran = true;
		}

		if (destroy) {
			teardown(destroy);
		}
	};
}

var l = /*#__PURE__*/Object.freeze({
	__proto__: null,
	createAttachmentKey: createAttachmentKey,
	fromAction: fromAction
});

/**
 *
 * @param {any} a
 * @param {any} b
 * @param {string} property
 * @param {string} location
 */
function compare(a, b, property, location) {
	if (a !== b) {
		assignment_value_stale(property, /** @type {string} */ (sanitize_location(location)));
	}

	return a;
}

/**
 * @param {any} object
 * @param {string} property
 * @param {any} value
 * @param {string} location
 */
function assign$1(object, property, value, location) {
	return compare(
		(object[property] = value),
		untrack(() => object[property]),
		property,
		location
	);
}

/**
 * @param {any} object
 * @param {string} property
 * @param {any} value
 * @param {string} location
 */
function assign_and(object, property, value, location) {
	return compare(
		(object[property] &&= value),
		untrack(() => object[property]),
		property,
		location
	);
}

/**
 * @param {any} object
 * @param {string} property
 * @param {any} value
 * @param {string} location
 */
function assign_or(object, property, value, location) {
	return compare(
		(object[property] ||= value),
		untrack(() => object[property]),
		property,
		location
	);
}

/**
 * @param {any} object
 * @param {string} property
 * @param {any} value
 * @param {string} location
 */
function assign_nullish(object, property, value, location) {
	return compare(
		(object[property] ??= value),
		untrack(() => object[property]),
		property,
		location
	);
}

/** @type {Map<String, Set<HTMLStyleElement>>} */
var all_styles = new Map();

/**
 * @param {String} hash
 */
function cleanup_styles(hash) {
	var styles = all_styles.get(hash);
	if (!styles) return;

	for (const style of styles) {
		style.remove();
	}

	all_styles.delete(hash);
}

/** @import { SourceLocation } from '#client' */

/**
 * @param {any} fn
 * @param {string} filename
 * @param {SourceLocation[]} locations
 * @returns {any}
 */
function add_locations(fn, filename, locations) {
	return (/** @type {any[]} */ ...args) => {
		const dom = fn(...args);

		var node = hydrating ? dom : dom.nodeType === DOCUMENT_FRAGMENT_NODE ? dom.firstChild : dom;
		assign_locations(node, filename, locations);

		return dom;
	};
}

/**
 * @param {Element} element
 * @param {string} filename
 * @param {SourceLocation} location
 */
function assign_location(element, filename, location) {
	// @ts-expect-error
	element.__svelte_meta = {
		parent: dev_stack,
		loc: { file: filename, line: location[0], column: location[1] }
	};

	if (location[2]) {
		assign_locations(element.firstChild, filename, location[2]);
	}
}

/**
 * @param {Node | null} node
 * @param {string} filename
 * @param {SourceLocation[]} locations
 */
function assign_locations(node, filename, locations) {
	var i = 0;
	var depth = 0;

	while (node && i < locations.length) {
		if (hydrating && node.nodeType === COMMENT_NODE) {
			var comment = /** @type {Comment} */ (node);
			if (comment.data === HYDRATION_START || comment.data === HYDRATION_START_ELSE) depth += 1;
			else if (comment.data[0] === HYDRATION_END) depth -= 1;
		}

		if (depth === 0 && node.nodeType === ELEMENT_NODE) {
			assign_location(/** @type {Element} */ (node), filename, locations[i++]);
		}

		node = node.nextSibling;
	}
}

/** @import { Effect, TemplateNode } from '#client' */

/**
 * @template {(anchor: Comment, props: any) => any} Component
 * @param {Component} fn
 */
function hmr(fn) {
	const current = source(fn);

	/**
	 * @param {TemplateNode} anchor
	 * @param {any} props
	 */
	function wrapper(anchor, props) {
		let component = {};
		let instance = {};

		/** @type {Effect} */
		let effect;

		let ran = false;

		block(() => {
			if (component === (component = get(current))) {
				return;
			}

			if (effect) {
				// @ts-ignore
				for (var k in instance) delete instance[k];
				destroy_effect(effect);
			}

			effect = branch(() => {
				// when the component is invalidated, replace it without transitions
				if (ran) set_should_intro(false);

				// preserve getters/setters
				Object.defineProperties(
					instance,
					Object.getOwnPropertyDescriptors(
						// @ts-expect-error
						new.target ? new component(anchor, props) : component(anchor, props)
					)
				);

				if (ran) set_should_intro(true);
			});
		}, EFFECT_TRANSPARENT);

		ran = true;

		if (hydrating) {
			anchor = hydrate_node;
		}

		return instance;
	}

	// @ts-expect-error
	wrapper[FILENAME] = fn[FILENAME];

	// @ts-ignore
	wrapper[HMR] = {
		fn,
		current,
		update: (/** @type {any} */ incoming) => {
			// This logic ensures that the first version of the component is the one
			// whose update function and therefore block effect is preserved across updates.
			// If we don't do this dance and instead just use `incoming` as the new component
			// and then update, we'll create an ever-growing stack of block effects.

			// Trigger the original block effect
			set(wrapper[HMR].current, incoming[HMR].fn);

			// Replace the incoming source with the original one
			incoming[HMR].current = wrapper[HMR].current;
		}
	};

	return wrapper;
}

/** @typedef {{ file: string, line: number, column: number }} Location */


/**
 * Sets up a validator that
 * - traverses the path of a prop to find out if it is allowed to be mutated
 * - checks that the binding chain is not interrupted
 * @param {Record<string, any>} props
 */
function create_ownership_validator(props) {
	const component = component_context?.function;
	const parent = component_context?.p?.function;

	return {
		/**
		 * @param {string} prop
		 * @param {any[]} path
		 * @param {any} result
		 * @param {number} line
		 * @param {number} column
		 */
		mutation: (prop, path, result, line, column) => {
			const name = path[0];
			if (is_bound_or_unset(props, name) || !parent) {
				return result;
			}

			/** @type {any} */
			let value = props;

			for (let i = 0; i < path.length - 1; i++) {
				value = value[path[i]];
				if (!value?.[STATE_SYMBOL]) {
					return result;
				}
			}

			const location = sanitize_location(`${component[FILENAME]}:${line}:${column}`);

			ownership_invalid_mutation(name, location, prop, parent[FILENAME]);

			return result;
		},
		/**
		 * @param {any} key
		 * @param {any} child_component
		 * @param {() => any} value
		 */
		binding: (key, child_component, value) => {
			if (!is_bound_or_unset(props, key) && parent && value()?.[STATE_SYMBOL]) {
				ownership_invalid_binding(
					component[FILENAME],
					key,
					child_component[FILENAME],
					parent[FILENAME]
				);
			}
		}
	};
}

/**
 * @param {Record<string, any>} props
 * @param {string} prop_name
 */
function is_bound_or_unset(props, prop_name) {
	// Can be the case when someone does `mount(Component, props)` with `let props = $state({...})`
	// or `createClassComponent(Component, props)`
	const is_entry_props = STATE_SYMBOL in props || LEGACY_PROPS in props;
	return (
		!!get_descriptor(props, prop_name)?.set ||
		(is_entry_props && prop_name in props) ||
		!(prop_name in props)
	);
}

/** @param {Function & { [FILENAME]: string }} target */
function check_target(target) {
	if (target) {
		component_api_invalid_new(target[FILENAME] ?? 'a component', target.name);
	}
}

function legacy_api() {
	const component = component_context?.function;

	/** @param {string} method */
	function error(method) {
		component_api_changed(method, component[FILENAME]);
	}

	return {
		$destroy: () => error('$destroy()'),
		$on: () => error('$on(...)'),
		$set: () => error('$set(...)')
	};
}

/**
 * @param {() => any[]} get_value
 * @param {Function} inspector
 * @param {boolean} show_stack
 */
function inspect(get_value, inspector, show_stack = false) {
	validate_effect();

	let initial = true;
	let error = /** @type {any} */ (UNINITIALIZED);

	// Inspect effects runs synchronously so that we can capture useful
	// stack traces. As a consequence, reading the value might result
	// in an error (an `$inspect(object.property)` will run before the
	// `{#if object}...{/if}` that contains it)
	eager_effect(() => {
		try {
			var value = get_value();
		} catch (e) {
			error = e;
			return;
		}

		var snap = snapshot(value, true, true);
		untrack(() => {
			if (show_stack) {
				inspector(...snap);

				if (!initial) {
					const stack = get_error('$inspect(...)');
					if (stack) {
						// eslint-disable-next-line no-console
						console.groupCollapsed('stack trace');
						// eslint-disable-next-line no-console
						console.log(stack);
						// eslint-disable-next-line no-console
						console.groupEnd();
					}
				}
			} else {
				inspector(initial ? 'init' : 'update', ...snap);
			}
		});

		initial = false;
	});

	// If an error occurs, we store it (along with its stack trace).
	// If the render effect subsequently runs, we log the error,
	// but if it doesn't run it's because the `$inspect` was
	// destroyed, meaning we don't need to bother
	render_effect(() => {
		try {
			// call `get_value` so that this runs alongside the inspect effect
			get_value();
		} catch {
			// ignore
		}

		if (error !== UNINITIALIZED) {
			// eslint-disable-next-line no-console
			console.error(error);
			error = UNINITIALIZED;
		}
	});
}

/** @import { TemplateNode, Value } from '#client' */

/**
 * @param {TemplateNode} node
 * @param {Array<Promise<void>>} blockers
 * @param {Array<() => Promise<any>>} expressions
 * @param {(anchor: TemplateNode, ...deriveds: Value[]) => void} fn
 */
function async(node, blockers = [], expressions = [], fn) {
	var boundary = get_boundary();
	var batch = /** @type {Batch} */ (current_batch);
	var blocking = boundary.is_rendered();

	boundary.update_pending_count(1);
	batch.increment(blocking);

	var was_hydrating = hydrating;

	if (was_hydrating) {
		hydrate_next();

		var previous_hydrate_node = hydrate_node;
		var end = skip_nodes(false);
		set_hydrate_node(end);
	}

	flatten(blockers, [], expressions, (values) => {
		if (was_hydrating) {
			set_hydrating(true);
			set_hydrate_node(previous_hydrate_node);
		}

		try {
			// get values eagerly to avoid creating blocks if they reject
			for (const d of values) get(d);

			fn(node, ...values);
		} finally {
			if (was_hydrating) {
				set_hydrating(false);
			}

			boundary.update_pending_count(-1);
			batch.decrement(blocking);
		}
	});
}

/**
 * @param {Node} anchor
 * @param {...(()=>any)[]} args
 */
function validate_snippet_args(anchor, ...args) {
	if (typeof anchor !== 'object' || !(anchor instanceof Node)) {
		invalid_snippet_arguments();
	}

	for (let arg of args) {
		if (typeof arg !== 'function') {
			invalid_snippet_arguments();
		}
	}
}

/** @import { Effect, TemplateNode } from '#client' */

/**
 * @typedef {{ effect: Effect, fragment: DocumentFragment }} Branch
 */

/**
 * @template Key
 */
class BranchManager {
	/** @type {TemplateNode} */
	anchor;

	/** @type {Map<Batch, Key>} */
	#batches = new Map();

	/**
	 * Map of keys to effects that are currently rendered in the DOM.
	 * These effects are visible and actively part of the document tree.
	 * Example:
	 * ```
	 * {#if condition}
	 * 	foo
	 * {:else}
	 * 	bar
	 * {/if}
	 * ```
	 * Can result in the entries `true->Effect` and `false->Effect`
	 * @type {Map<Key, Effect>}
	 */
	#onscreen = new Map();

	/**
	 * Similar to #onscreen with respect to the keys, but contains branches that are not yet
	 * in the DOM, because their insertion is deferred.
	 * @type {Map<Key, Branch>}
	 */
	#offscreen = new Map();

	/**
	 * Keys of effects that are currently outroing
	 * @type {Set<Key>}
	 */
	#outroing = new Set();

	/**
	 * Whether to pause (i.e. outro) on change, or destroy immediately.
	 * This is necessary for `<svelte:element>`
	 */
	#transition = true;

	/**
	 * @param {TemplateNode} anchor
	 * @param {boolean} transition
	 */
	constructor(anchor, transition = true) {
		this.anchor = anchor;
		this.#transition = transition;
	}

	#commit = () => {
		var batch = /** @type {Batch} */ (current_batch);

		// if this batch was made obsolete, bail
		if (!this.#batches.has(batch)) return;

		var key = /** @type {Key} */ (this.#batches.get(batch));

		var onscreen = this.#onscreen.get(key);

		if (onscreen) {
			// effect is already in the DOM — abort any current outro
			resume_effect(onscreen);
			this.#outroing.delete(key);
		} else {
			// effect is currently offscreen. put it in the DOM
			var offscreen = this.#offscreen.get(key);

			if (offscreen) {
				this.#onscreen.set(key, offscreen.effect);
				this.#offscreen.delete(key);

				// remove the anchor...
				/** @type {TemplateNode} */ (offscreen.fragment.lastChild).remove();

				// ...and append the fragment
				this.anchor.before(offscreen.fragment);
				onscreen = offscreen.effect;
			}
		}

		for (const [b, k] of this.#batches) {
			this.#batches.delete(b);

			if (b === batch) {
				// keep values for newer batches
				break;
			}

			const offscreen = this.#offscreen.get(k);

			if (offscreen) {
				// for older batches, destroy offscreen effects
				// as they will never be committed
				destroy_effect(offscreen.effect);
				this.#offscreen.delete(k);
			}
		}

		// outro/destroy all onscreen effects...
		for (const [k, effect] of this.#onscreen) {
			// ...except the one that was just committed
			//    or those that are already outroing (else the transition is aborted and the effect destroyed right away)
			if (k === key || this.#outroing.has(k)) continue;

			const on_destroy = () => {
				const keys = Array.from(this.#batches.values());

				if (keys.includes(k)) {
					// keep the effect offscreen, as another batch will need it
					var fragment = document.createDocumentFragment();
					move_effect(effect, fragment);

					fragment.append(create_text()); // TODO can we avoid this?

					this.#offscreen.set(k, { effect, fragment });
				} else {
					destroy_effect(effect);
				}

				this.#outroing.delete(k);
				this.#onscreen.delete(k);
			};

			if (this.#transition || !onscreen) {
				this.#outroing.add(k);
				pause_effect(effect, on_destroy, false);
			} else {
				on_destroy();
			}
		}
	};

	/**
	 * @param {Batch} batch
	 */
	#discard = (batch) => {
		this.#batches.delete(batch);

		const keys = Array.from(this.#batches.values());

		for (const [k, branch] of this.#offscreen) {
			if (!keys.includes(k)) {
				destroy_effect(branch.effect);
				this.#offscreen.delete(k);
			}
		}
	};

	/**
	 *
	 * @param {any} key
	 * @param {null | ((target: TemplateNode) => void)} fn
	 */
	ensure(key, fn) {
		var batch = /** @type {Batch} */ (current_batch);
		var defer = should_defer_append();

		if (fn && !this.#onscreen.has(key) && !this.#offscreen.has(key)) {
			if (defer) {
				var fragment = document.createDocumentFragment();
				var target = create_text();

				fragment.append(target);

				this.#offscreen.set(key, {
					effect: branch(() => fn(target)),
					fragment
				});
			} else {
				this.#onscreen.set(
					key,
					branch(() => fn(this.anchor))
				);
			}
		}

		this.#batches.set(batch, key);

		if (defer) {
			for (const [k, effect] of this.#onscreen) {
				if (k === key) {
					batch.skipped_effects.delete(effect);
				} else {
					batch.skipped_effects.add(effect);
				}
			}

			for (const [k, branch] of this.#offscreen) {
				if (k === key) {
					batch.skipped_effects.delete(branch.effect);
				} else {
					batch.skipped_effects.add(branch.effect);
				}
			}

			batch.oncommit(this.#commit);
			batch.ondiscard(this.#discard);
		} else {
			if (hydrating) {
				this.anchor = hydrate_node;
			}

			this.#commit();
		}
	}
}

/** @import { Source, TemplateNode } from '#client' */

const PENDING = 0;
const THEN = 1;

/** @typedef {typeof PENDING | typeof THEN | typeof CATCH} AwaitState */

/**
 * @template V
 * @param {TemplateNode} node
 * @param {(() => any)} get_input
 * @param {null | ((anchor: Node) => void)} pending_fn
 * @param {null | ((anchor: Node, value: Source<V>) => void)} then_fn
 * @param {null | ((anchor: Node, error: unknown) => void)} catch_fn
 * @returns {void}
 */
function await_block(node, get_input, pending_fn, then_fn, catch_fn) {
	if (hydrating) {
		hydrate_next();
	}

	var runes = is_runes();

	var v = /** @type {V} */ (UNINITIALIZED);
	var value = runes ? source(v) : mutable_source(v, false, false);
	var error = runes ? source(v) : mutable_source(v, false, false);

	var branches = new BranchManager(node);

	block(() => {
		var input = get_input();
		var destroyed = false;

		/** Whether or not there was a hydration mismatch. Needs to be a `let` or else it isn't treeshaken out */
		// @ts-ignore coercing `node` to a `Comment` causes TypeScript and Prettier to fight
		let mismatch = hydrating && is_promise(input) === (node.data === HYDRATION_START_ELSE);

		if (mismatch) {
			// Hydration mismatch: remove everything inside the anchor and start fresh
			set_hydrate_node(skip_nodes());
			set_hydrating(false);
		}

		if (is_promise(input)) {
			var restore = capture();
			var resolved = false;

			/**
			 * @param {() => void} fn
			 */
			const resolve = (fn) => {
				if (destroyed) return;

				resolved = true;
				// We don't want to restore the previous batch here; {#await} blocks don't follow the async logic
				// we have elsewhere, instead pending/resolve/fail states are each their own batch so to speak.
				restore(false);
				// Make sure we have a batch, since the branch manager expects one to exist
				Batch.ensure();

				if (hydrating) {
					// `restore()` could set `hydrating` to `true`, which we very much
					// don't want — we want to restore everything _except_ this
					set_hydrating(false);
				}

				try {
					fn();
				} finally {
					unset_context();

					// without this, the DOM does not update until two ticks after the promise
					// resolves, which is unexpected behaviour (and somewhat irksome to test)
					if (!is_flushing_sync) flushSync();
				}
			};

			input.then(
				(v) => {
					resolve(() => {
						internal_set(value, v);
						branches.ensure(THEN, then_fn && ((target) => then_fn(target, value)));
					});
				},
				(e) => {
					resolve(() => {
						internal_set(error, e);
						branches.ensure(THEN, catch_fn && ((target) => catch_fn(target, error)));

						if (!catch_fn) {
							// Rethrow the error if no catch block exists
							throw error.v;
						}
					});
				}
			);

			if (hydrating) {
				branches.ensure(PENDING, pending_fn);
			} else {
				// Wait a microtask before checking if we should show the pending state as
				// the promise might have resolved by then
				queue_micro_task(() => {
					if (!resolved) {
						resolve(() => {
							branches.ensure(PENDING, pending_fn);
						});
					}
				});
			}
		} else {
			internal_set(value, input);
			branches.ensure(THEN, then_fn && ((target) => then_fn(target, value)));
		}

		if (mismatch) {
			// continue in hydration mode
			set_hydrating(true);
		}

		return () => {
			destroyed = true;
		};
	});
}

/** @import { TemplateNode } from '#client' */

// TODO reinstate https://github.com/sveltejs/svelte/pull/15250

/**
 * @param {TemplateNode} node
 * @param {(branch: (fn: (anchor: Node) => void, flag?: boolean) => void) => void} fn
 * @param {boolean} [elseif] True if this is an `{:else if ...}` block rather than an `{#if ...}`, as that affects which transitions are considered 'local'
 * @returns {void}
 */
function if_block(node, fn, elseif = false) {
	if (hydrating) {
		hydrate_next();
	}

	var branches = new BranchManager(node);
	var flags = elseif ? EFFECT_TRANSPARENT : 0;

	/**
	 * @param {boolean} condition,
	 * @param {null | ((anchor: Node) => void)} fn
	 */
	function update_branch(condition, fn) {
		if (hydrating) {
			const is_else = read_hydration_instruction(node) === HYDRATION_START_ELSE;

			if (condition === is_else) {
				// Hydration mismatch: remove everything inside the anchor and start fresh.
				// This could happen with `{#if browser}...{/if}`, for example
				var anchor = skip_nodes();

				set_hydrate_node(anchor);
				branches.anchor = anchor;

				set_hydrating(false);
				branches.ensure(condition, fn);
				set_hydrating(true);

				return;
			}
		}

		branches.ensure(condition, fn);
	}

	block(() => {
		var has_branch = false;

		fn((fn, flag = true) => {
			has_branch = true;
			update_branch(flag, fn);
		});

		if (!has_branch) {
			update_branch(false, null);
		}
	}, flags);
}

/** @import { TemplateNode } from '#client' */

/**
 * @template V
 * @param {TemplateNode} node
 * @param {() => V} get_key
 * @param {(anchor: Node) => TemplateNode | void} render_fn
 * @returns {void}
 */
function key(node, get_key, render_fn) {
	if (hydrating) {
		hydrate_next();
	}

	var branches = new BranchManager(node);

	var legacy = !is_runes();

	block(() => {
		var key = get_key();

		// key blocks in Svelte <5 had stupid semantics
		if (legacy && key !== null && typeof key === 'object') {
			key = /** @type {V} */ ({});
		}

		branches.ensure(key, render_fn);
	});
}

/**
 * @param {HTMLDivElement | SVGGElement} element
 * @param {() => Record<string, string>} get_styles
 * @returns {void}
 */
function css_props(element, get_styles) {
	if (hydrating) {
		set_hydrate_node(get_first_child(element));
	}

	render_effect(() => {
		var styles = get_styles();

		for (var key in styles) {
			var value = styles[key];

			if (value) {
				element.style.setProperty(key, value);
			} else {
				element.style.removeProperty(key);
			}
		}
	});
}

/** @import { EachItem, EachOutroGroup, EachState, Effect, EffectNodes, MaybeSource, Source, TemplateNode, TransitionManager, Value } from '#client' */
/** @import { Batch } from '../../reactivity/batch.js'; */

// When making substantive changes to this file, validate them with the each block stress test:
// https://svelte.dev/playground/1972b2cf46564476ad8c8c6405b23b7b
// This test also exists in this repo, as `packages/svelte/tests/manual/each-stress-test`

/**
 * @param {any} _
 * @param {number} i
 */
function index(_, i) {
	return i;
}

/**
 * Pause multiple effects simultaneously, and coordinate their
 * subsequent destruction. Used in each blocks
 * @param {EachState} state
 * @param {Effect[]} to_destroy
 * @param {null | Node} controlled_anchor
 */
function pause_effects(state, to_destroy, controlled_anchor) {
	/** @type {TransitionManager[]} */
	var transitions = [];
	var length = to_destroy.length;

	/** @type {EachOutroGroup} */
	var group;
	var remaining = to_destroy.length;

	for (var i = 0; i < length; i++) {
		let effect = to_destroy[i];

		pause_effect(
			effect,
			() => {
				if (group) {
					group.pending.delete(effect);
					group.done.add(effect);

					if (group.pending.size === 0) {
						var groups = /** @type {Set<EachOutroGroup>} */ (state.outrogroups);

						destroy_effects(array_from(group.done));
						groups.delete(group);

						if (groups.size === 0) {
							state.outrogroups = null;
						}
					}
				} else {
					remaining -= 1;
				}
			},
			false
		);
	}

	if (remaining === 0) {
		// If we're in a controlled each block (i.e. the block is the only child of an
		// element), and we are removing all items, _and_ there are no out transitions,
		// we can use the fast path — emptying the element and replacing the anchor
		var fast_path = transitions.length === 0 && controlled_anchor !== null;

		if (fast_path) {
			var anchor = /** @type {Element} */ (controlled_anchor);
			var parent_node = /** @type {Element} */ (anchor.parentNode);

			clear_text_content(parent_node);
			parent_node.append(anchor);

			state.items.clear();
		}

		destroy_effects(to_destroy, !fast_path);
	} else {
		group = {
			pending: new Set(to_destroy),
			done: new Set()
		};

		(state.outrogroups ??= new Set()).add(group);
	}
}

/**
 * @param {Effect[]} to_destroy
 * @param {boolean} remove_dom
 */
function destroy_effects(to_destroy, remove_dom = true) {
	// TODO only destroy effects if no pending batch needs them. otherwise,
	// just re-add the `EFFECT_OFFSCREEN` flag
	for (var i = 0; i < to_destroy.length; i++) {
		destroy_effect(to_destroy[i], remove_dom);
	}
}

/** @type {TemplateNode} */
var offscreen_anchor;

/**
 * @template V
 * @param {Element | Comment} node The next sibling node, or the parent node if this is a 'controlled' block
 * @param {number} flags
 * @param {() => V[]} get_collection
 * @param {(value: V, index: number) => any} get_key
 * @param {(anchor: Node, item: MaybeSource<V>, index: MaybeSource<number>) => void} render_fn
 * @param {null | ((anchor: Node) => void)} fallback_fn
 * @returns {void}
 */
function each(node, flags, get_collection, get_key, render_fn, fallback_fn = null) {
	var anchor = node;

	/** @type {Map<any, EachItem>} */
	var items = new Map();

	var is_controlled = (flags & EACH_IS_CONTROLLED) !== 0;

	if (is_controlled) {
		var parent_node = /** @type {Element} */ (node);

		anchor = hydrating
			? set_hydrate_node(get_first_child(parent_node))
			: parent_node.appendChild(create_text());
	}

	if (hydrating) {
		hydrate_next();
	}

	/** @type {Effect | null} */
	var fallback = null;

	// TODO: ideally we could use derived for runes mode but because of the ability
	// to use a store which can be mutated, we can't do that here as mutating a store
	// will still result in the collection array being the same from the store
	var each_array = derived_safe_equal(() => {
		var collection = get_collection();

		return is_array(collection) ? collection : collection == null ? [] : array_from(collection);
	});

	/** @type {V[]} */
	var array;

	var first_run = true;

	function commit() {
		state.fallback = fallback;
		reconcile(state, array, anchor, flags, get_key);

		if (fallback !== null) {
			if (array.length === 0) {
				if ((fallback.f & EFFECT_OFFSCREEN) === 0) {
					resume_effect(fallback);
				} else {
					fallback.f ^= EFFECT_OFFSCREEN;
					move(fallback, null, anchor);
				}
			} else {
				pause_effect(fallback, () => {
					// TODO only null out if no pending batch needs it,
					// otherwise re-add `fallback.fragment` and move the
					// effect into it
					fallback = null;
				});
			}
		}
	}

	var effect = block(() => {
		array = /** @type {V[]} */ (get(each_array));
		var length = array.length;

		/** `true` if there was a hydration mismatch. Needs to be a `let` or else it isn't treeshaken out */
		let mismatch = false;

		if (hydrating) {
			var is_else = read_hydration_instruction(anchor) === HYDRATION_START_ELSE;

			if (is_else !== (length === 0)) {
				// hydration mismatch — remove the server-rendered DOM and start over
				anchor = skip_nodes();

				set_hydrate_node(anchor);
				set_hydrating(false);
				mismatch = true;
			}
		}

		var keys = new Set();
		var batch = /** @type {Batch} */ (current_batch);
		var defer = should_defer_append();

		for (var index = 0; index < length; index += 1) {
			if (
				hydrating &&
				hydrate_node.nodeType === COMMENT_NODE &&
				/** @type {Comment} */ (hydrate_node).data === HYDRATION_END
			) {
				// The server rendered fewer items than expected,
				// so break out and continue appending non-hydrated items
				anchor = /** @type {Comment} */ (hydrate_node);
				mismatch = true;
				set_hydrating(false);
			}

			var value = array[index];
			var key = get_key(value, index);

			var item = first_run ? null : items.get(key);

			if (item) {
				// update before reconciliation, to trigger any async updates
				if (item.v) internal_set(item.v, value);
				if (item.i) internal_set(item.i, index);

				if (defer) {
					batch.skipped_effects.delete(item.e);
				}
			} else {
				item = create_item(
					items,
					first_run ? anchor : (offscreen_anchor ??= create_text()),
					value,
					key,
					index,
					render_fn,
					flags,
					get_collection
				);

				if (!first_run) {
					item.e.f |= EFFECT_OFFSCREEN;
				}

				items.set(key, item);
			}

			keys.add(key);
		}

		if (length === 0 && fallback_fn && !fallback) {
			if (first_run) {
				fallback = branch(() => fallback_fn(anchor));
			} else {
				fallback = branch(() => fallback_fn((offscreen_anchor ??= create_text())));
				fallback.f |= EFFECT_OFFSCREEN;
			}
		}

		// remove excess nodes
		if (hydrating && length > 0) {
			set_hydrate_node(skip_nodes());
		}

		if (!first_run) {
			if (defer) {
				for (const [key, item] of items) {
					if (!keys.has(key)) {
						batch.skipped_effects.add(item.e);
					}
				}

				batch.oncommit(commit);
				batch.ondiscard(() => {
					// TODO presumably we need to do something here?
				});
			} else {
				commit();
			}
		}

		if (mismatch) {
			// continue in hydration mode
			set_hydrating(true);
		}

		// When we mount the each block for the first time, the collection won't be
		// connected to this effect as the effect hasn't finished running yet and its deps
		// won't be assigned. However, it's possible that when reconciling the each block
		// that a mutation occurred and it's made the collection MAYBE_DIRTY, so reading the
		// collection again can provide consistency to the reactive graph again as the deriveds
		// will now be `CLEAN`.
		get(each_array);
	});

	/** @type {EachState} */
	var state = { effect, items, outrogroups: null, fallback };

	first_run = false;

	if (hydrating) {
		anchor = hydrate_node;
	}
}

/**
 * Add, remove, or reorder items output by an each block as its input changes
 * @template V
 * @param {EachState} state
 * @param {Array<V>} array
 * @param {Element | Comment | Text} anchor
 * @param {number} flags
 * @param {(value: V, index: number) => any} get_key
 * @returns {void}
 */
function reconcile(state, array, anchor, flags, get_key) {
	var is_animated = (flags & EACH_IS_ANIMATED) !== 0;

	var length = array.length;
	var items = state.items;
	var current = state.effect.first;

	/** @type {undefined | Set<Effect>} */
	var seen;

	/** @type {Effect | null} */
	var prev = null;

	/** @type {undefined | Set<Effect>} */
	var to_animate;

	/** @type {Effect[]} */
	var matched = [];

	/** @type {Effect[]} */
	var stashed = [];

	/** @type {V} */
	var value;

	/** @type {any} */
	var key;

	/** @type {Effect | undefined} */
	var effect;

	/** @type {number} */
	var i;

	if (is_animated) {
		for (i = 0; i < length; i += 1) {
			value = array[i];
			key = get_key(value, i);
			effect = /** @type {EachItem} */ (items.get(key)).e;

			// offscreen == coming in now, no animation in that case,
			// else this would happen https://github.com/sveltejs/svelte/issues/17181
			if ((effect.f & EFFECT_OFFSCREEN) === 0) {
				effect.nodes?.a?.measure();
				(to_animate ??= new Set()).add(effect);
			}
		}
	}

	for (i = 0; i < length; i += 1) {
		value = array[i];
		key = get_key(value, i);

		effect = /** @type {EachItem} */ (items.get(key)).e;

		if (state.outrogroups !== null) {
			for (const group of state.outrogroups) {
				group.pending.delete(effect);
				group.done.delete(effect);
			}
		}

		if ((effect.f & EFFECT_OFFSCREEN) !== 0) {
			effect.f ^= EFFECT_OFFSCREEN;

			if (effect === current) {
				move(effect, null, anchor);
			} else {
				var next = prev ? prev.next : current;

				if (effect === state.effect.last) {
					state.effect.last = effect.prev;
				}

				if (effect.prev) effect.prev.next = effect.next;
				if (effect.next) effect.next.prev = effect.prev;
				link(state, prev, effect);
				link(state, effect, next);

				move(effect, next, anchor);
				prev = effect;

				matched = [];
				stashed = [];

				current = prev.next;
				continue;
			}
		}

		if ((effect.f & INERT) !== 0) {
			resume_effect(effect);
			if (is_animated) {
				effect.nodes?.a?.unfix();
				(to_animate ??= new Set()).delete(effect);
			}
		}

		if (effect !== current) {
			if (seen !== undefined && seen.has(effect)) {
				if (matched.length < stashed.length) {
					// more efficient to move later items to the front
					var start = stashed[0];
					var j;

					prev = start.prev;

					var a = matched[0];
					var b = matched[matched.length - 1];

					for (j = 0; j < matched.length; j += 1) {
						move(matched[j], start, anchor);
					}

					for (j = 0; j < stashed.length; j += 1) {
						seen.delete(stashed[j]);
					}

					link(state, a.prev, b.next);
					link(state, prev, a);
					link(state, b, start);

					current = start;
					prev = b;
					i -= 1;

					matched = [];
					stashed = [];
				} else {
					// more efficient to move earlier items to the back
					seen.delete(effect);
					move(effect, current, anchor);

					link(state, effect.prev, effect.next);
					link(state, effect, prev === null ? state.effect.first : prev.next);
					link(state, prev, effect);

					prev = effect;
				}

				continue;
			}

			matched = [];
			stashed = [];

			while (current !== null && current !== effect) {
				(seen ??= new Set()).add(current);
				stashed.push(current);
				current = current.next;
			}

			if (current === null) {
				continue;
			}
		}

		if ((effect.f & EFFECT_OFFSCREEN) === 0) {
			matched.push(effect);
		}

		prev = effect;
		current = effect.next;
	}

	if (state.outrogroups !== null) {
		for (const group of state.outrogroups) {
			if (group.pending.size === 0) {
				destroy_effects(array_from(group.done));
				state.outrogroups?.delete(group);
			}
		}

		if (state.outrogroups.size === 0) {
			state.outrogroups = null;
		}
	}

	if (current !== null || seen !== undefined) {
		/** @type {Effect[]} */
		var to_destroy = [];

		if (seen !== undefined) {
			for (effect of seen) {
				if ((effect.f & INERT) === 0) {
					to_destroy.push(effect);
				}
			}
		}

		while (current !== null) {
			// If the each block isn't inert, then inert effects are currently outroing and will be removed once the transition is finished
			if ((current.f & INERT) === 0 && current !== state.fallback) {
				to_destroy.push(current);
			}

			current = current.next;
		}

		var destroy_length = to_destroy.length;

		if (destroy_length > 0) {
			var controlled_anchor = (flags & EACH_IS_CONTROLLED) !== 0 && length === 0 ? anchor : null;

			if (is_animated) {
				for (i = 0; i < destroy_length; i += 1) {
					to_destroy[i].nodes?.a?.measure();
				}

				for (i = 0; i < destroy_length; i += 1) {
					to_destroy[i].nodes?.a?.fix();
				}
			}

			pause_effects(state, to_destroy, controlled_anchor);
		}
	}

	if (is_animated) {
		queue_micro_task(() => {
			if (to_animate === undefined) return;
			for (effect of to_animate) {
				effect.nodes?.a?.apply();
			}
		});
	}
}

/**
 * @template V
 * @param {Map<any, EachItem>} items
 * @param {Node} anchor
 * @param {V} value
 * @param {unknown} key
 * @param {number} index
 * @param {(anchor: Node, item: V | Source<V>, index: number | Value<number>, collection: () => V[]) => void} render_fn
 * @param {number} flags
 * @param {() => V[]} get_collection
 * @returns {EachItem}
 */
function create_item(items, anchor, value, key, index, render_fn, flags, get_collection) {
	var v =
		(flags & EACH_ITEM_REACTIVE) !== 0
			? (flags & EACH_ITEM_IMMUTABLE) === 0
				? mutable_source(value, false, false)
				: source(value)
			: null;

	var i = (flags & EACH_INDEX_REACTIVE) !== 0 ? source(index) : null;

	return {
		v,
		i,
		e: branch(() => {
			render_fn(anchor, v ?? value, i ?? index, get_collection);

			return () => {
				items.delete(key);
			};
		})
	};
}

/**
 * @param {Effect} effect
 * @param {Effect | null} next
 * @param {Text | Element | Comment} anchor
 */
function move(effect, next, anchor) {
	if (!effect.nodes) return;

	var node = effect.nodes.start;
	var end = effect.nodes.end;

	var dest =
		next && (next.f & EFFECT_OFFSCREEN) === 0
			? /** @type {EffectNodes} */ (next.nodes).start
			: anchor;

	while (node !== null) {
		var next_node = /** @type {TemplateNode} */ (get_next_sibling(node));
		dest.before(node);

		if (node === end) {
			return;
		}

		node = next_node;
	}
}

/**
 * @param {EachState} state
 * @param {Effect | null} prev
 * @param {Effect | null} next
 */
function link(state, prev, next) {
	if (prev === null) {
		state.effect.first = next;
	} else {
		prev.next = next;
	}

	if (next === null) {
		state.effect.last = prev;
	} else {
		next.prev = prev;
	}
}

/** @import { Effect, TemplateNode } from '#client' */

/**
 * @param {Element | Text | Comment} node
 * @param {() => string} get_value
 * @param {boolean} [svg]
 * @param {boolean} [mathml]
 * @param {boolean} [skip_warning]
 * @returns {void}
 */
function html(node, get_value, svg = false, mathml = false, skip_warning = false) {
	var anchor = node;

	var value = '';

	template_effect(() => {
		var effect = /** @type {Effect} */ (active_effect);

		if (value === (value = get_value() ?? '')) {
			if (hydrating) hydrate_next();
			return;
		}

		if (effect.nodes !== null) {
			remove_effect_dom(effect.nodes.start, /** @type {TemplateNode} */ (effect.nodes.end));
			effect.nodes = null;
		}

		if (value === '') return;

		if (hydrating) {
			// We're deliberately not trying to repair mismatches between server and client,
			// as it's costly and error-prone (and it's an edge case to have a mismatch anyway)
			/** @type {Comment} */ (hydrate_node).data;

			/** @type {TemplateNode | null} */
			var next = hydrate_next();
			var last = next;

			while (
				next !== null &&
				(next.nodeType !== COMMENT_NODE || /** @type {Comment} */ (next).data !== '')
			) {
				last = next;
				next = get_next_sibling(next);
			}

			if (next === null) {
				hydration_mismatch();
				throw HYDRATION_ERROR;
			}

			assign_nodes(hydrate_node, last);
			anchor = set_hydrate_node(next);
			return;
		}

		var html = value + '';
		if (svg) html = `<svg>${html}</svg>`;
		else if (mathml) html = `<math>${html}</math>`;

		// Don't use create_fragment_with_script_from_html here because that would mean script tags are executed.
		// @html is basically `.innerHTML = ...` and that doesn't execute scripts either due to security reasons.
		/** @type {DocumentFragment | Element} */
		var node = create_fragment_from_html(html);

		if (svg || mathml) {
			node = /** @type {Element} */ (get_first_child(node));
		}

		assign_nodes(
			/** @type {TemplateNode} */ (get_first_child(node)),
			/** @type {TemplateNode} */ (node.lastChild)
		);

		if (svg || mathml) {
			while (get_first_child(node)) {
				anchor.before(/** @type {TemplateNode} */ (get_first_child(node)));
			}
		} else {
			anchor.before(node);
		}
	});
}

/**
 * @param {Comment} anchor
 * @param {Record<string, any>} $$props
 * @param {string} name
 * @param {Record<string, unknown>} slot_props
 * @param {null | ((anchor: Comment) => void)} fallback_fn
 */
function slot(anchor, $$props, name, slot_props, fallback_fn) {
	if (hydrating) {
		hydrate_next();
	}

	var slot_fn = $$props.$$slots?.[name];
	// Interop: Can use snippets to fill slots
	var is_interop = false;
	if (slot_fn === true) {
		slot_fn = $$props[name === 'default' ? 'children' : name];
		is_interop = true;
	}

	if (slot_fn === undefined) {
		if (fallback_fn !== null) {
			fallback_fn(anchor);
		}
	} else {
		slot_fn(anchor, is_interop ? () => slot_props : slot_props);
	}
}

/**
 * @param {Record<string, any>} props
 * @returns {Record<string, boolean>}
 */
function sanitize_slots(props) {
	/** @type {Record<string, boolean>} */
	const sanitized = {};
	if (props.children) sanitized.default = true;
	for (const key in props.$$slots) {
		sanitized[key] = true;
	}
	return sanitized;
}

/** @import { Snippet } from 'svelte' */
/** @import { TemplateNode } from '#client' */
/** @import { Getters } from '#shared' */

/**
 * @template {(node: TemplateNode, ...args: any[]) => void} SnippetFn
 * @param {TemplateNode} node
 * @param {() => SnippetFn | null | undefined} get_snippet
 * @param {(() => any)[]} args
 * @returns {void}
 */
function snippet(node, get_snippet, ...args) {
	var branches = new BranchManager(node);

	block(() => {
		const snippet = get_snippet() ?? null;

		branches.ensure(snippet, snippet && ((anchor) => snippet(anchor, ...args)));
	}, EFFECT_TRANSPARENT);
}

/**
 * In development, wrap the snippet function so that it passes validation, and so that the
 * correct component context is set for ownership checks
 * @param {any} component
 * @param {(node: TemplateNode, ...args: any[]) => void} fn
 */
function wrap_snippet(component, fn) {
	const snippet = (/** @type {TemplateNode} */ node, /** @type {any[]} */ ...args) => {
		var previous_component_function = dev_current_component_function;
		set_dev_current_component_function(component);

		try {
			return fn(node, ...args);
		} finally {
			set_dev_current_component_function(previous_component_function);
		}
	};

	prevent_snippet_stringification(snippet);

	return snippet;
}

/** @import { TemplateNode, Dom } from '#client' */

/**
 * @template P
 * @template {(props: P) => void} C
 * @param {TemplateNode} node
 * @param {() => C} get_component
 * @param {(anchor: TemplateNode, component: C) => Dom | void} render_fn
 * @returns {void}
 */
function component(node, get_component, render_fn) {
	if (hydrating) {
		hydrate_next();
	}

	var branches = new BranchManager(node);

	block(() => {
		var component = get_component() ?? null;
		branches.ensure(component, component && ((target) => render_fn(target, component)));
	}, EFFECT_TRANSPARENT);
}

/** @import { AnimateFn, Animation, AnimationConfig, EachItem, Effect, EffectNodes, TransitionFn, TransitionManager } from '#client' */

/**
 * @param {Element} element
 * @param {'introstart' | 'introend' | 'outrostart' | 'outroend'} type
 * @returns {void}
 */
function dispatch_event(element, type) {
	without_reactive_context(() => {
		element.dispatchEvent(new CustomEvent(type));
	});
}

/**
 * Converts a property to the camel-case format expected by Element.animate(), KeyframeEffect(), and KeyframeEffect.setKeyframes().
 * @param {string} style
 * @returns {string}
 */
function css_property_to_camelcase(style) {
	// in compliance with spec
	if (style === 'float') return 'cssFloat';
	if (style === 'offset') return 'cssOffset';

	// do not rename custom @properties
	if (style.startsWith('--')) return style;

	const parts = style.split('-');
	if (parts.length === 1) return parts[0];
	return (
		parts[0] +
		parts
			.slice(1)
			.map(/** @param {any} word */ (word) => word[0].toUpperCase() + word.slice(1))
			.join('')
	);
}

/**
 * @param {string} css
 * @returns {Keyframe}
 */
function css_to_keyframe(css) {
	/** @type {Keyframe} */
	const keyframe = {};
	const parts = css.split(';');
	for (const part of parts) {
		const [property, value] = part.split(':');
		if (!property || value === undefined) break;

		const formatted_property = css_property_to_camelcase(property.trim());
		keyframe[formatted_property] = value.trim();
	}
	return keyframe;
}

/** @param {number} t */
const linear$1 = (t) => t;

/** @type {Effect | null} */
let animation_effect_override = null;

/** @param {Effect | null} v */
function set_animation_effect_override(v) {
	animation_effect_override = v;
}

/**
 * Called inside keyed `{#each ...}` blocks (as `$.animation(...)`). This creates an animation manager
 * and attaches it to the block, so that moves can be animated following reconciliation.
 * @template P
 * @param {Element} element
 * @param {() => AnimateFn<P | undefined>} get_fn
 * @param {(() => P) | null} get_params
 */
function animation(element, get_fn, get_params) {
	var effect = animation_effect_override ?? /** @type {Effect} */ (active_effect);
	var nodes = /** @type {EffectNodes} */ (effect.nodes);

	/** @type {DOMRect} */
	var from;

	/** @type {DOMRect} */
	var to;

	/** @type {Animation | undefined} */
	var animation;

	/** @type {null | { position: string, width: string, height: string, transform: string }} */
	var original_styles = null;

	nodes.a ??= {
		element,
		measure() {
			from = this.element.getBoundingClientRect();
		},
		apply() {
			animation?.abort();

			to = this.element.getBoundingClientRect();

			if (
				from.left !== to.left ||
				from.right !== to.right ||
				from.top !== to.top ||
				from.bottom !== to.bottom
			) {
				const options = get_fn()(this.element, { from, to }, get_params?.());

				animation = animate(this.element, options, undefined, 1, () => {
					animation?.abort();
					animation = undefined;
				});
			}
		},
		fix() {
			// If an animation is already running, transforming the element is likely to fail,
			// because the styles applied by the animation take precedence. In the case of crossfade,
			// that means the `translate(...)` of the crossfade transition overrules the `translate(...)`
			// we would apply below, leading to the element jumping somewhere to the top left.
			if (element.getAnimations().length) return;

			// It's important to destructure these to get fixed values - the object itself has getters,
			// and changing the style to 'absolute' can for example influence the width.
			var { position, width, height } = getComputedStyle(element);

			if (position !== 'absolute' && position !== 'fixed') {
				var style = /** @type {HTMLElement | SVGElement} */ (element).style;

				original_styles = {
					position: style.position,
					width: style.width,
					height: style.height,
					transform: style.transform
				};

				style.position = 'absolute';
				style.width = width;
				style.height = height;
				var to = element.getBoundingClientRect();

				if (from.left !== to.left || from.top !== to.top) {
					var transform = `translate(${from.left - to.left}px, ${from.top - to.top}px)`;
					style.transform = style.transform ? `${style.transform} ${transform}` : transform;
				}
			}
		},
		unfix() {
			if (original_styles) {
				var style = /** @type {HTMLElement | SVGElement} */ (element).style;

				style.position = original_styles.position;
				style.width = original_styles.width;
				style.height = original_styles.height;
				style.transform = original_styles.transform;
			}
		}
	};

	// in the case of a `<svelte:element>`, it's possible for `$.animation(...)` to be called
	// when an animation manager already exists, if the tag changes. in that case, we need to
	// swap out the element rather than creating a new manager, in case it happened at the same
	// moment as a reconciliation
	nodes.a.element = element;
}

/**
 * Called inside block effects as `$.transition(...)`. This creates a transition manager and
 * attaches it to the current effect — later, inside `pause_effect` and `resume_effect`, we
 * use this to create `intro` and `outro` transitions.
 * @template P
 * @param {number} flags
 * @param {HTMLElement} element
 * @param {() => TransitionFn<P | undefined>} get_fn
 * @param {(() => P) | null} get_params
 * @returns {void}
 */
function transition(flags, element, get_fn, get_params) {
	var is_intro = (flags & TRANSITION_IN) !== 0;
	var is_outro = (flags & TRANSITION_OUT) !== 0;
	var is_both = is_intro && is_outro;
	var is_global = (flags & TRANSITION_GLOBAL) !== 0;

	/** @type {'in' | 'out' | 'both'} */
	var direction = is_both ? 'both' : is_intro ? 'in' : 'out';

	/** @type {AnimationConfig | ((opts: { direction: 'in' | 'out' }) => AnimationConfig) | undefined} */
	var current_options;

	var inert = element.inert;

	/**
	 * The default overflow style, stashed so we can revert changes during the transition
	 * that are necessary to work around a Safari <18 bug
	 * TODO 6.0 remove this, if older versions of Safari have died out enough
	 */
	var overflow = element.style.overflow;

	/** @type {Animation | undefined} */
	var intro;

	/** @type {Animation | undefined} */
	var outro;

	function get_options() {
		return without_reactive_context(() => {
			// If a transition is still ongoing, we use the existing options rather than generating
			// new ones. This ensures that reversible transitions reverse smoothly, rather than
			// jumping to a new spot because (for example) a different `duration` was used
			return (current_options ??= get_fn()(element, get_params?.() ?? /** @type {P} */ ({}), {
				direction
			}));
		});
	}

	/** @type {TransitionManager} */
	var transition = {
		is_global,
		in() {
			element.inert = inert;

			if (!is_intro) {
				outro?.abort();
				outro?.reset?.();
				return;
			}

			if (!is_outro) {
				// if we intro then outro then intro again, we want to abort the first intro,
				// if it's not a bidirectional transition
				intro?.abort();
			}

			dispatch_event(element, 'introstart');

			intro = animate(element, get_options(), outro, 1, () => {
				dispatch_event(element, 'introend');

				// Ensure we cancel the animation to prevent leaking
				intro?.abort();
				intro = current_options = undefined;

				element.style.overflow = overflow;
			});
		},
		out(fn) {
			if (!is_outro) {
				fn?.();
				current_options = undefined;
				return;
			}

			element.inert = true;

			dispatch_event(element, 'outrostart');

			outro = animate(element, get_options(), intro, 0, () => {
				dispatch_event(element, 'outroend');
				fn?.();
			});
		},
		stop: () => {
			intro?.abort();
			outro?.abort();
		}
	};

	var e = /** @type {Effect & { nodes: EffectNodes }} */ (active_effect);

	(e.nodes.t ??= []).push(transition);

	// if this is a local transition, we only want to run it if the parent (branch) effect's
	// parent (block) effect is where the state change happened. we can determine that by
	// looking at whether the block effect is currently initializing
	if (is_intro && should_intro) {
		var run = is_global;

		if (!run) {
			var block = /** @type {Effect | null} */ (e.parent);

			// skip over transparent blocks (e.g. snippets, else-if blocks)
			while (block && (block.f & EFFECT_TRANSPARENT) !== 0) {
				while ((block = block.parent)) {
					if ((block.f & BLOCK_EFFECT) !== 0) break;
				}
			}

			run = !block || (block.f & EFFECT_RAN) !== 0;
		}

		if (run) {
			effect(() => {
				untrack(() => transition.in());
			});
		}
	}
}

/**
 * Animates an element, according to the provided configuration
 * @param {Element} element
 * @param {AnimationConfig | ((opts: { direction: 'in' | 'out' }) => AnimationConfig)} options
 * @param {Animation | undefined} counterpart The corresponding intro/outro to this outro/intro
 * @param {number} t2 The target `t` value — `1` for intro, `0` for outro
 * @param {(() => void)} on_finish Called after successfully completing the animation
 * @returns {Animation}
 */
function animate(element, options, counterpart, t2, on_finish) {
	var is_intro = t2 === 1;

	if (is_function(options)) {
		// In the case of a deferred transition (such as `crossfade`), `option` will be
		// a function rather than an `AnimationConfig`. We need to call this function
		// once the DOM has been updated...
		/** @type {Animation} */
		var a;
		var aborted = false;

		queue_micro_task(() => {
			if (aborted) return;
			var o = options({ direction: is_intro ? 'in' : 'out' });
			a = animate(element, o, counterpart, t2, on_finish);
		});

		// ...but we want to do so without using `async`/`await` everywhere, so
		// we return a facade that allows everything to remain synchronous
		return {
			abort: () => {
				aborted = true;
				a?.abort();
			},
			deactivate: () => a.deactivate(),
			reset: () => a.reset(),
			t: () => a.t()
		};
	}

	counterpart?.deactivate();

	if (!options?.duration) {
		on_finish();

		return {
			abort: noop,
			deactivate: noop,
			reset: noop,
			t: () => t2
		};
	}

	const { delay = 0, css, tick, easing = linear$1 } = options;

	var keyframes = [];

	if (is_intro && counterpart === undefined) {
		if (tick) {
			tick(0, 1); // TODO put in nested effect, to avoid interleaved reads/writes?
		}

		if (css) {
			var styles = css_to_keyframe(css(0, 1));
			keyframes.push(styles, styles);
		}
	}

	var get_t = () => 1 - t2;

	// create a dummy animation that lasts as long as the delay (but with whatever devtools
	// multiplier is in effect). in the common case that it is `0`, we keep it anyway so that
	// the CSS keyframes aren't created until the DOM is updated
	//
	// fill forwards to prevent the element from rendering without styles applied
	// see https://github.com/sveltejs/svelte/issues/14732
	var animation = element.animate(keyframes, { duration: delay, fill: 'forwards' });

	animation.onfinish = () => {
		// remove dummy animation from the stack to prevent conflict with main animation
		animation.cancel();

		// for bidirectional transitions, we start from the current position,
		// rather than doing a full intro/outro
		var t1 = counterpart?.t() ?? 1 - t2;
		counterpart?.abort();

		var delta = t2 - t1;
		var duration = /** @type {number} */ (options.duration) * Math.abs(delta);
		var keyframes = [];

		if (duration > 0) {
			/**
			 * Whether or not the CSS includes `overflow: hidden`, in which case we need to
			 * add it as an inline style to work around a Safari <18 bug
			 * TODO 6.0 remove this, if possible
			 */
			var needs_overflow_hidden = false;

			if (css) {
				var n = Math.ceil(duration / (1000 / 60)); // `n` must be an integer, or we risk missing the `t2` value

				for (var i = 0; i <= n; i += 1) {
					var t = t1 + delta * easing(i / n);
					var styles = css_to_keyframe(css(t, 1 - t));
					keyframes.push(styles);

					needs_overflow_hidden ||= styles.overflow === 'hidden';
				}
			}

			if (needs_overflow_hidden) {
				/** @type {HTMLElement} */ (element).style.overflow = 'hidden';
			}

			get_t = () => {
				var time = /** @type {number} */ (
					/** @type {globalThis.Animation} */ (animation).currentTime
				);

				return t1 + delta * easing(time / duration);
			};

			if (tick) {
				loop(() => {
					if (animation.playState !== 'running') return false;

					var t = get_t();
					tick(t, 1 - t);

					return true;
				});
			}
		}

		animation = element.animate(keyframes, { duration, fill: 'forwards' });

		animation.onfinish = () => {
			get_t = () => t2;
			tick?.(t2, 1 - t2);
			on_finish();
		};
	};

	return {
		abort: () => {
			if (animation) {
				animation.cancel();
				// This prevents memory leaks in Chromium
				animation.effect = null;
				// This prevents onfinish to be launched after cancel(),
				// which can happen in some rare cases
				// see https://github.com/sveltejs/svelte/issues/13681
				animation.onfinish = noop;
			}
		},
		deactivate: () => {
			on_finish = noop;
		},
		reset: () => {
			if (t2 === 0) {
				tick?.(1, 0);
			}
		},
		t: () => get_t()
	};
}

/** @import { Effect, EffectNodes, TemplateNode } from '#client' */

/**
 * @param {Comment | Element} node
 * @param {() => string} get_tag
 * @param {boolean} is_svg
 * @param {undefined | ((element: Element, anchor: Node | null) => void)} render_fn,
 * @param {undefined | (() => string)} get_namespace
 * @param {undefined | [number, number]} location
 * @returns {void}
 */
function element(node, get_tag, is_svg, render_fn, get_namespace, location) {
	let was_hydrating = hydrating;

	if (hydrating) {
		hydrate_next();
	}

	/** @type {null | Element} */
	var element = null;

	if (hydrating && hydrate_node.nodeType === ELEMENT_NODE) {
		element = /** @type {Element} */ (hydrate_node);
		hydrate_next();
	}

	var anchor = /** @type {TemplateNode} */ (hydrating ? hydrate_node : node);

	/**
	 * We track this so we can set it when changing the element, allowing any
	 * `animate:` directive to bind itself to the correct block
	 */
	var parent_effect = /** @type {Effect} */ (active_effect);

	var branches = new BranchManager(anchor, false);

	block(() => {
		const next_tag = get_tag() || null;
		var ns = get_namespace ? get_namespace() : is_svg || next_tag === 'svg' ? NAMESPACE_SVG : null;

		if (next_tag === null) {
			branches.ensure(null, null);
			set_should_intro(true);
			return;
		}

		branches.ensure(next_tag, (anchor) => {
			if (next_tag) {
				element = hydrating
					? /** @type {Element} */ (element)
					: ns
						? document.createElementNS(ns, next_tag)
						: document.createElement(next_tag);

				assign_nodes(element, element);

				if (render_fn) {
					if (hydrating && is_raw_text_element(next_tag)) {
						// prevent hydration glitches
						element.append(document.createComment(''));
					}

					// If hydrating, use the existing ssr comment as the anchor so that the
					// inner open and close methods can pick up the existing nodes correctly
					var child_anchor = hydrating
						? get_first_child(element)
						: element.appendChild(create_text());

					if (hydrating) {
						if (child_anchor === null) {
							set_hydrating(false);
						} else {
							set_hydrate_node(child_anchor);
						}
					}

					set_animation_effect_override(parent_effect);

					// `child_anchor` is undefined if this is a void element, but we still
					// need to call `render_fn` in order to run actions etc. If the element
					// contains children, it's a user error (which is warned on elsewhere)
					// and the DOM will be silently discarded
					render_fn(element, child_anchor);

					set_animation_effect_override(null);
				}

				// we do this after calling `render_fn` so that child effects don't override `nodes.end`
				/** @type {Effect & { nodes: EffectNodes }} */ (active_effect).nodes.end = element;

				anchor.before(element);
			}

			if (hydrating) {
				set_hydrate_node(anchor);
			}
		});

		// revert to the default state after the effect has been created
		set_should_intro(true);

		return () => {
			if (next_tag) {
				// if we're in this callback because we're re-running the effect,
				// disable intros (unless no element is currently displayed)
				set_should_intro(false);
			}
		};
	}, EFFECT_TRANSPARENT);

	teardown(() => {
		set_should_intro(true);
	});

	if (was_hydrating) {
		set_hydrating(true);
		set_hydrate_node(anchor);
	}
}

/** @import { TemplateNode } from '#client' */

/**
 * @param {string} hash
 * @param {(anchor: Node) => void} render_fn
 * @returns {void}
 */
function head(hash, render_fn) {
	// The head function may be called after the first hydration pass and ssr comment nodes may still be present,
	// therefore we need to skip that when we detect that we're not in hydration mode.
	let previous_hydrate_node = null;
	let was_hydrating = hydrating;

	/** @type {Comment | Text} */
	var anchor;

	if (hydrating) {
		previous_hydrate_node = hydrate_node;

		var head_anchor = get_first_child(document.head);

		// There might be multiple head blocks in our app, and they could have been
		// rendered in an arbitrary order — find one corresponding to this component
		while (
			head_anchor !== null &&
			(head_anchor.nodeType !== COMMENT_NODE || /** @type {Comment} */ (head_anchor).data !== hash)
		) {
			head_anchor = get_next_sibling(head_anchor);
		}

		// If we can't find an opening hydration marker, skip hydration (this can happen
		// if a framework rendered body but not head content)
		if (head_anchor === null) {
			set_hydrating(false);
		} else {
			var start = /** @type {TemplateNode} */ (get_next_sibling(head_anchor));
			head_anchor.remove(); // in case this component is repeated

			set_hydrate_node(start);
		}
	}

	if (!hydrating) {
		anchor = document.head.appendChild(create_text());
	}

	try {
		block(() => render_fn(anchor), HEAD_EFFECT);
	} finally {
		if (was_hydrating) {
			set_hydrating(true);
			set_hydrate_node(/** @type {TemplateNode} */ (previous_hydrate_node));
		}
	}
}

/**
 * @param {Node} anchor
 * @param {{ hash: string, code: string }} css
 */
function append_styles(anchor, css) {
	// Use `queue_micro_task` to ensure `anchor` is in the DOM, otherwise getRootNode() will yield wrong results
	effect(() => {
		var root = anchor.getRootNode();

		var target = /** @type {ShadowRoot} */ (root).host
			? /** @type {ShadowRoot} */ (root)
			: /** @type {Document} */ (root).head ?? /** @type {Document} */ (root.ownerDocument).head;

		// Always querying the DOM is roughly the same perf as additionally checking for presence in a map first assuming
		// that you'll get cache hits half of the time, so we just always query the dom for simplicity and code savings.
		if (!target.querySelector('#' + css.hash)) {
			const style = document.createElement('style');
			style.id = css.hash;
			style.textContent = css.code;

			target.appendChild(style);
		}
	});
}

/** @import { ActionPayload } from '#client' */

/**
 * @template P
 * @param {Element} dom
 * @param {(dom: Element, value?: P) => ActionPayload<P>} action
 * @param {() => P} [get_value]
 * @returns {void}
 */
function action(dom, action, get_value) {
	effect(() => {
		var payload = untrack(() => action(dom, get_value?.()) || {});

		if (get_value && payload?.update) {
			var inited = false;
			/** @type {P} */
			var prev = /** @type {any} */ ({}); // initialize with something so it's never equal on first run

			render_effect(() => {
				var value = get_value();

				// Action's update method is coarse-grained, i.e. when anything in the passed value changes, update.
				// This works in legacy mode because of mutable_source being updated as a whole, but when using $state
				// together with actions and mutation, it wouldn't notice the change without a deep read.
				deep_read_state(value);

				if (inited && safe_not_equal(prev, value)) {
					prev = value;
					/** @type {Function} */ (payload.update)(value);
				}
			});

			inited = true;
		}

		if (payload?.destroy) {
			return () => /** @type {Function} */ (payload.destroy)();
		}
	});
}

/** @import { Effect } from '#client' */

// TODO in 6.0 or 7.0, when we remove legacy mode, we can simplify this by
// getting rid of the block/branch stuff and just letting the effect rip.
// see https://github.com/sveltejs/svelte/pull/15962

/**
 * @param {Element} node
 * @param {() => (node: Element) => void} get_fn
 */
function attach(node, get_fn) {
	/** @type {false | undefined | ((node: Element) => void)} */
	var fn = undefined;

	/** @type {Effect | null} */
	var e;

	managed(() => {
		if (fn !== (fn = get_fn())) {
			if (e) {
				destroy_effect(e);
				e = null;
			}

			if (fn) {
				e = branch(() => {
					effect(() => /** @type {(node: Element) => void} */ (fn)(node));
				});
			}
		}
	});
}

/**
 * @param {Element} dom
 * @param {boolean | number} is_html
 * @param {string | null} value
 * @param {string} [hash]
 * @param {Record<string, any>} [prev_classes]
 * @param {Record<string, any>} [next_classes]
 * @returns {Record<string, boolean> | undefined}
 */
function set_class(dom, is_html, value, hash, prev_classes, next_classes) {
	// @ts-expect-error need to add __className to patched prototype
	var prev = dom.__className;

	if (
		hydrating ||
		prev !== value ||
		prev === undefined // for edge case of `class={undefined}`
	) {
		var next_class_name = to_class(value, hash, next_classes);

		if (!hydrating || next_class_name !== dom.getAttribute('class')) {
			// Removing the attribute when the value is only an empty string causes
			// performance issues vs simply making the className an empty string. So
			// we should only remove the class if the value is nullish
			// and there no hash/directives :
			if (next_class_name == null) {
				dom.removeAttribute('class');
			} else if (is_html) {
				dom.className = next_class_name;
			} else {
				dom.setAttribute('class', next_class_name);
			}
		}

		// @ts-expect-error need to add __className to patched prototype
		dom.__className = value;
	} else if (next_classes && prev_classes !== next_classes) {
		for (var key in next_classes) {
			var is_present = !!next_classes[key];

			if (prev_classes == null || is_present !== !!prev_classes[key]) {
				dom.classList.toggle(key, is_present);
			}
		}
	}

	return next_classes;
}

/**
 * @param {Element & ElementCSSInlineStyle} dom
 * @param {Record<string, any>} prev
 * @param {Record<string, any>} next
 * @param {string} [priority]
 */
function update_styles(dom, prev = {}, next, priority) {
	for (var key in next) {
		var value = next[key];

		if (prev[key] !== value) {
			if (next[key] == null) {
				dom.style.removeProperty(key);
			} else {
				dom.style.setProperty(key, value, priority);
			}
		}
	}
}

/**
 * @param {Element & ElementCSSInlineStyle} dom
 * @param {string | null} value
 * @param {Record<string, any> | [Record<string, any>, Record<string, any>]} [prev_styles]
 * @param {Record<string, any> | [Record<string, any>, Record<string, any>]} [next_styles]
 */
function set_style(dom, value, prev_styles, next_styles) {
	// @ts-expect-error
	var prev = dom.__style;

	if (hydrating || prev !== value) {
		var next_style_attr = to_style(value, next_styles);

		if (!hydrating || next_style_attr !== dom.getAttribute('style')) {
			if (next_style_attr == null) {
				dom.removeAttribute('style');
			} else {
				dom.style.cssText = next_style_attr;
			}
		}

		// @ts-expect-error
		dom.__style = value;
	} else if (next_styles) {
		if (Array.isArray(next_styles)) {
			update_styles(dom, prev_styles?.[0], next_styles[0]);
			update_styles(dom, prev_styles?.[1], next_styles[1], 'important');
		} else {
			update_styles(dom, prev_styles, next_styles);
		}
	}

	return next_styles;
}

/**
 * Selects the correct option(s) (depending on whether this is a multiple select)
 * @template V
 * @param {HTMLSelectElement} select
 * @param {V} value
 * @param {boolean} mounting
 */
function select_option(select, value, mounting = false) {
	if (select.multiple) {
		// If value is null or undefined, keep the selection as is
		if (value == undefined) {
			return;
		}

		// If not an array, warn and keep the selection as is
		if (!is_array(value)) {
			return select_multiple_invalid_value();
		}

		// Otherwise, update the selection
		for (var option of select.options) {
			option.selected = value.includes(get_option_value(option));
		}

		return;
	}

	for (option of select.options) {
		var option_value = get_option_value(option);
		if (is(option_value, value)) {
			option.selected = true;
			return;
		}
	}

	if (!mounting || value !== undefined) {
		select.selectedIndex = -1; // no option should be selected
	}
}

/**
 * Selects the correct option(s) if `value` is given,
 * and then sets up a mutation observer to sync the
 * current selection to the dom when it changes. Such
 * changes could for example occur when options are
 * inside an `#each` block.
 * @param {HTMLSelectElement} select
 */
function init_select(select) {
	var observer = new MutationObserver(() => {
		// @ts-ignore
		select_option(select, select.__value);
		// Deliberately don't update the potential binding value,
		// the model should be preserved unless explicitly changed
	});

	observer.observe(select, {
		// Listen to option element changes
		childList: true,
		subtree: true, // because of <optgroup>
		// Listen to option element value attribute changes
		// (doesn't get notified of select value changes,
		// because that property is not reflected as an attribute)
		attributes: true,
		attributeFilter: ['value']
	});

	teardown(() => {
		observer.disconnect();
	});
}

/**
 * @param {HTMLSelectElement} select
 * @param {() => unknown} get
 * @param {(value: unknown) => void} set
 * @returns {void}
 */
function bind_select_value(select, get, set = get) {
	var batches = new WeakSet();
	var mounting = true;

	listen_to_event_and_reset_event(select, 'change', (is_reset) => {
		var query = is_reset ? '[selected]' : ':checked';
		/** @type {unknown} */
		var value;

		if (select.multiple) {
			value = [].map.call(select.querySelectorAll(query), get_option_value);
		} else {
			/** @type {HTMLOptionElement | null} */
			var selected_option =
				select.querySelector(query) ??
				// will fall back to first non-disabled option if no option is selected
				select.querySelector('option:not([disabled])');
			value = selected_option && get_option_value(selected_option);
		}

		set(value);

		if (current_batch !== null) {
			batches.add(current_batch);
		}
	});

	// Needs to be an effect, not a render_effect, so that in case of each loops the logic runs after the each block has updated
	effect(() => {
		var value = get();

		if (select === document.activeElement) {
			// we need both, because in non-async mode, render effects run before previous_batch is set
			var batch = /** @type {Batch} */ (previous_batch ?? current_batch);

			// Don't update the <select> if it is focused. We can get here if, for example,
			// an update is deferred because of async work depending on the select:
			//
			// <select bind:value={selected}>...</select>
			// <p>{await find(selected)}</p>
			if (batches.has(batch)) {
				return;
			}
		}

		select_option(select, value, mounting);

		// Mounting and value undefined -> take selection from dom
		if (mounting && value === undefined) {
			/** @type {HTMLOptionElement | null} */
			var selected_option = select.querySelector(':checked');
			if (selected_option !== null) {
				value = get_option_value(selected_option);
				set(value);
			}
		}

		// @ts-ignore
		select.__value = value;
		mounting = false;
	});

	init_select(select);
}

/** @param {HTMLOptionElement} option */
function get_option_value(option) {
	// __value only exists if the <option> has a value attribute
	if ('__value' in option) {
		return option.__value;
	} else {
		return option.value;
	}
}

/** @import { Effect } from '#client' */

const CLASS = Symbol('class');
const STYLE = Symbol('style');

const IS_CUSTOM_ELEMENT = Symbol('is custom element');
const IS_HTML = Symbol('is html');

/**
 * The value/checked attribute in the template actually corresponds to the defaultValue property, so we need
 * to remove it upon hydration to avoid a bug when someone resets the form value.
 * @param {HTMLInputElement} input
 * @returns {void}
 */
function remove_input_defaults(input) {
	if (!hydrating) return;

	var already_removed = false;

	// We try and remove the default attributes later, rather than sync during hydration.
	// Doing it sync during hydration has a negative impact on performance, but deferring the
	// work in an idle task alleviates this greatly. If a form reset event comes in before
	// the idle callback, then we ensure the input defaults are cleared just before.
	var remove_defaults = () => {
		if (already_removed) return;
		already_removed = true;

		// Remove the attributes but preserve the values
		if (input.hasAttribute('value')) {
			var value = input.value;
			set_attribute(input, 'value', null);
			input.value = value;
		}

		if (input.hasAttribute('checked')) {
			var checked = input.checked;
			set_attribute(input, 'checked', null);
			input.checked = checked;
		}
	};

	// @ts-expect-error
	input.__on_r = remove_defaults;
	queue_micro_task(remove_defaults);
	add_form_reset_listener();
}

/**
 * @param {Element} element
 * @param {any} value
 */
function set_value(element, value) {
	var attributes = get_attributes(element);

	if (
		attributes.value ===
			(attributes.value =
				// treat null and undefined the same for the initial value
				value ?? undefined) ||
		// @ts-expect-error
		// `progress` elements always need their value set when it's `0`
		(element.value === value && (value !== 0 || element.nodeName !== 'PROGRESS'))
	) {
		return;
	}

	// @ts-expect-error
	element.value = value ?? '';
}

/**
 * @param {Element} element
 * @param {boolean} checked
 */
function set_checked(element, checked) {
	var attributes = get_attributes(element);

	if (
		attributes.checked ===
		(attributes.checked =
			// treat null and undefined the same for the initial value
			checked ?? undefined)
	) {
		return;
	}

	// @ts-expect-error
	element.checked = checked;
}

/**
 * Sets the `selected` attribute on an `option` element.
 * Not set through the property because that doesn't reflect to the DOM,
 * which means it wouldn't be taken into account when a form is reset.
 * @param {HTMLOptionElement} element
 * @param {boolean} selected
 */
function set_selected(element, selected) {
	if (selected) {
		// The selected option could've changed via user selection, and
		// setting the value without this check would set it back.
		if (!element.hasAttribute('selected')) {
			element.setAttribute('selected', '');
		}
	} else {
		element.removeAttribute('selected');
	}
}

/**
 * Applies the default checked property without influencing the current checked property.
 * @param {HTMLInputElement} element
 * @param {boolean} checked
 */
function set_default_checked(element, checked) {
	const existing_value = element.checked;
	element.defaultChecked = checked;
	element.checked = existing_value;
}

/**
 * Applies the default value property without influencing the current value property.
 * @param {HTMLInputElement | HTMLTextAreaElement} element
 * @param {string} value
 */
function set_default_value(element, value) {
	const existing_value = element.value;
	element.defaultValue = value;
	element.value = existing_value;
}

/**
 * @param {Element} element
 * @param {string} attribute
 * @param {string | null} value
 * @param {boolean} [skip_warning]
 */
function set_attribute(element, attribute, value, skip_warning) {
	var attributes = get_attributes(element);

	if (hydrating) {
		attributes[attribute] = element.getAttribute(attribute);

		if (
			attribute === 'src' ||
			attribute === 'srcset' ||
			(attribute === 'href' && element.nodeName === 'LINK')
		) {

			// If we reset these attributes, they would result in another network request, which we want to avoid.
			// We assume they are the same between client and server as checking if they are equal is expensive
			// (we can't just compare the strings as they can be different between client and server but result in the
			// same url, so we would need to create hidden anchor elements to compare them)
			return;
		}
	}

	if (attributes[attribute] === (attributes[attribute] = value)) return;

	if (attribute === 'loading') {
		// @ts-expect-error
		element[LOADING_ATTR_SYMBOL] = value;
	}

	if (value == null) {
		element.removeAttribute(attribute);
	} else if (typeof value !== 'string' && get_setters(element).includes(attribute)) {
		// @ts-ignore
		element[attribute] = value;
	} else {
		element.setAttribute(attribute, value);
	}
}

/**
 * @param {Element} dom
 * @param {string} attribute
 * @param {string} value
 */
function set_xlink_attribute(dom, attribute, value) {
	dom.setAttributeNS('http://www.w3.org/1999/xlink', attribute, value);
}

/**
 * @param {HTMLElement} node
 * @param {string} prop
 * @param {any} value
 */
function set_custom_element_data(node, prop, value) {
	// We need to ensure that setting custom element props, which can
	// invoke lifecycle methods on other custom elements, does not also
	// associate those lifecycle methods with the current active reaction
	// or effect
	var previous_reaction = active_reaction;
	var previous_effect = active_effect;

	// If we're hydrating but the custom element is from Svelte, and it already scaffolded,
	// then it might run block logic in hydration mode, which we have to prevent.
	let was_hydrating = hydrating;
	if (hydrating) {
		set_hydrating(false);
	}

	set_active_reaction(null);
	set_active_effect(null);

	try {
		if (
			// `style` should use `set_attribute` rather than the setter
			prop !== 'style' &&
			// Don't compute setters for custom elements while they aren't registered yet,
			// because during their upgrade/instantiation they might add more setters.
			// Instead, fall back to a simple "an object, then set as property" heuristic.
			(setters_cache.has(node.getAttribute('is') || node.nodeName) ||
			// customElements may not be available in browser extension contexts
			!customElements ||
			customElements.get(node.getAttribute('is') || node.tagName.toLowerCase())
				? get_setters(node).includes(prop)
				: value && typeof value === 'object')
		) {
			// @ts-expect-error
			node[prop] = value;
		} else {
			// We did getters etc checks already, stringify before passing to set_attribute
			// to ensure it doesn't invoke the same logic again, and potentially populating
			// the setters cache too early.
			set_attribute(node, prop, value == null ? value : String(value));
		}
	} finally {
		set_active_reaction(previous_reaction);
		set_active_effect(previous_effect);
		if (was_hydrating) {
			set_hydrating(true);
		}
	}
}

/**
 * Spreads attributes onto a DOM element, taking into account the currently set attributes
 * @param {Element & ElementCSSInlineStyle} element
 * @param {Record<string | symbol, any> | undefined} prev
 * @param {Record<string | symbol, any>} next New attributes - this function mutates this object
 * @param {string} [css_hash]
 * @param {boolean} [should_remove_defaults]
 * @param {boolean} [skip_warning]
 * @returns {Record<string, any>}
 */
function set_attributes(
	element,
	prev,
	next,
	css_hash,
	should_remove_defaults = false,
	skip_warning = false
) {
	if (hydrating && should_remove_defaults && element.tagName === 'INPUT') {
		var input = /** @type {HTMLInputElement} */ (element);
		var attribute = input.type === 'checkbox' ? 'defaultChecked' : 'defaultValue';

		if (!(attribute in next)) {
			remove_input_defaults(input);
		}
	}

	var attributes = get_attributes(element);

	var is_custom_element = attributes[IS_CUSTOM_ELEMENT];
	var preserve_attribute_case = !attributes[IS_HTML];

	// If we're hydrating but the custom element is from Svelte, and it already scaffolded,
	// then it might run block logic in hydration mode, which we have to prevent.
	let is_hydrating_custom_element = hydrating && is_custom_element;
	if (is_hydrating_custom_element) {
		set_hydrating(false);
	}

	var current = prev || {};
	var is_option_element = element.tagName === 'OPTION';

	for (var key in prev) {
		if (!(key in next)) {
			next[key] = null;
		}
	}

	if (next.class) {
		next.class = clsx(next.class);
	} else if (css_hash || next[CLASS]) {
		next.class = null; /* force call to set_class() */
	}

	if (next[STYLE]) {
		next.style ??= null; /* force call to set_style() */
	}

	var setters = get_setters(element);

	// since key is captured we use const
	for (const key in next) {
		// let instead of var because referenced in a closure
		let value = next[key];

		// Up here because we want to do this for the initial value, too, even if it's undefined,
		// and this wouldn't be reached in case of undefined because of the equality check below
		if (is_option_element && key === 'value' && value == null) {
			// The <option> element is a special case because removing the value attribute means
			// the value is set to the text content of the option element, and setting the value
			// to null or undefined means the value is set to the string "null" or "undefined".
			// To align with how we handle this case in non-spread-scenarios, this logic is needed.
			// There's a super-edge-case bug here that is left in in favor of smaller code size:
			// Because of the "set missing props to null" logic above, we can't differentiate
			// between a missing value and an explicitly set value of null or undefined. That means
			// that once set, the value attribute of an <option> element can't be removed. This is
			// a very rare edge case, and removing the attribute altogether isn't possible either
			// for the <option value={undefined}> case, so we're not losing any functionality here.
			// @ts-ignore
			element.value = element.__value = '';
			current[key] = value;
			continue;
		}

		if (key === 'class') {
			var is_html = element.namespaceURI === 'http://www.w3.org/1999/xhtml';
			set_class(element, is_html, value, css_hash, prev?.[CLASS], next[CLASS]);
			current[key] = value;
			current[CLASS] = next[CLASS];
			continue;
		}

		if (key === 'style') {
			set_style(element, value, prev?.[STYLE], next[STYLE]);
			current[key] = value;
			current[STYLE] = next[STYLE];
			continue;
		}

		var prev_value = current[key];

		// Skip if value is unchanged, unless it's `undefined` and the element still has the attribute
		if (value === prev_value && !(value === undefined && element.hasAttribute(key))) {
			continue;
		}

		current[key] = value;

		var prefix = key[0] + key[1]; // this is faster than key.slice(0, 2)
		if (prefix === '$$') continue;

		if (prefix === 'on') {
			/** @type {{ capture?: true }} */
			const opts = {};
			const event_handle_key = '$$' + key;
			let event_name = key.slice(2);
			var delegated = can_delegate_event(event_name);

			if (is_capture_event(event_name)) {
				event_name = event_name.slice(0, -7);
				opts.capture = true;
			}

			if (!delegated && prev_value) {
				// Listening to same event but different handler -> our handle function below takes care of this
				// If we were to remove and add listeners in this case, it could happen that the event is "swallowed"
				// (the browser seems to not know yet that a new one exists now) and doesn't reach the handler
				// https://github.com/sveltejs/svelte/issues/11903
				if (value != null) continue;

				element.removeEventListener(event_name, current[event_handle_key], opts);
				current[event_handle_key] = null;
			}

			if (value != null) {
				if (!delegated) {
					/**
					 * @this {any}
					 * @param {Event} evt
					 */
					function handle(evt) {
						current[key].call(this, evt);
					}

					current[event_handle_key] = create_event(event_name, element, handle, opts);
				} else {
					// @ts-ignore
					element[`__${event_name}`] = value;
					delegate([event_name]);
				}
			} else if (delegated) {
				// @ts-ignore
				element[`__${event_name}`] = undefined;
			}
		} else if (key === 'style') {
			// avoid using the setter
			set_attribute(element, key, value);
		} else if (key === 'autofocus') {
			autofocus(/** @type {HTMLElement} */ (element), Boolean(value));
		} else if (!is_custom_element && (key === '__value' || (key === 'value' && value != null))) {
			// @ts-ignore We're not running this for custom elements because __value is actually
			// how Lit stores the current value on the element, and messing with that would break things.
			element.value = element.__value = value;
		} else if (key === 'selected' && is_option_element) {
			set_selected(/** @type {HTMLOptionElement} */ (element), value);
		} else {
			var name = key;
			if (!preserve_attribute_case) {
				name = normalize_attribute(name);
			}

			var is_default = name === 'defaultValue' || name === 'defaultChecked';

			if (value == null && !is_custom_element && !is_default) {
				attributes[key] = null;

				if (name === 'value' || name === 'checked') {
					// removing value/checked also removes defaultValue/defaultChecked — preserve
					let input = /** @type {HTMLInputElement} */ (element);
					const use_default = prev === undefined;
					if (name === 'value') {
						let previous = input.defaultValue;
						input.removeAttribute(name);
						input.defaultValue = previous;
						// @ts-ignore
						input.value = input.__value = use_default ? previous : null;
					} else {
						let previous = input.defaultChecked;
						input.removeAttribute(name);
						input.defaultChecked = previous;
						input.checked = use_default ? previous : false;
					}
				} else {
					element.removeAttribute(key);
				}
			} else if (
				is_default ||
				(setters.includes(name) && (is_custom_element || typeof value !== 'string'))
			) {
				// @ts-ignore
				element[name] = value;
				// remove it from attributes's cache
				if (name in attributes) attributes[name] = UNINITIALIZED;
			} else if (typeof value !== 'function') {
				set_attribute(element, name, value);
			}
		}
	}

	if (is_hydrating_custom_element) {
		set_hydrating(true);
	}

	return current;
}

/**
 * @param {Element & ElementCSSInlineStyle} element
 * @param {(...expressions: any) => Record<string | symbol, any>} fn
 * @param {Array<() => any>} sync
 * @param {Array<() => Promise<any>>} async
 * @param {Array<Promise<void>>} blockers
 * @param {string} [css_hash]
 * @param {boolean} [should_remove_defaults]
 * @param {boolean} [skip_warning]
 */
function attribute_effect(
	element,
	fn,
	sync = [],
	async = [],
	blockers = [],
	css_hash,
	should_remove_defaults = false,
	skip_warning = false
) {
	flatten(blockers, sync, async, (values) => {
		/** @type {Record<string | symbol, any> | undefined} */
		var prev = undefined;

		/** @type {Record<symbol, Effect>} */
		var effects = {};

		var is_select = element.nodeName === 'SELECT';
		var inited = false;

		managed(() => {
			var next = fn(...values.map(get));
			/** @type {Record<string | symbol, any>} */
			var current = set_attributes(
				element,
				prev,
				next,
				css_hash,
				should_remove_defaults,
				skip_warning
			);

			if (inited && is_select && 'value' in next) {
				select_option(/** @type {HTMLSelectElement} */ (element), next.value);
			}

			for (let symbol of Object.getOwnPropertySymbols(effects)) {
				if (!next[symbol]) destroy_effect(effects[symbol]);
			}

			for (let symbol of Object.getOwnPropertySymbols(next)) {
				var n = next[symbol];

				if (symbol.description === ATTACHMENT_KEY && (!prev || n !== prev[symbol])) {
					if (effects[symbol]) destroy_effect(effects[symbol]);
					effects[symbol] = branch(() => attach(element, () => n));
				}

				current[symbol] = n;
			}

			prev = current;
		});

		if (is_select) {
			var select = /** @type {HTMLSelectElement} */ (element);

			effect(() => {
				select_option(select, /** @type {Record<string | symbol, any>} */ (prev).value, true);
				init_select(select);
			});
		}

		inited = true;
	});
}

/**
 *
 * @param {Element} element
 */
function get_attributes(element) {
	return /** @type {Record<string | symbol, unknown>} **/ (
		// @ts-expect-error
		element.__attributes ??= {
			[IS_CUSTOM_ELEMENT]: element.nodeName.includes('-'),
			[IS_HTML]: element.namespaceURI === NAMESPACE_HTML
		}
	);
}

/** @type {Map<string, string[]>} */
var setters_cache = new Map();

/** @param {Element} element */
function get_setters(element) {
	var cache_key = element.getAttribute('is') || element.nodeName;
	var setters = setters_cache.get(cache_key);
	if (setters) return setters;
	setters_cache.set(cache_key, (setters = []));

	var descriptors;
	var proto = element; // In the case of custom elements there might be setters on the instance
	var element_proto = Element.prototype;

	// Stop at Element, from there on there's only unnecessary setters we're not interested in
	// Do not use contructor.name here as that's unreliable in some browser environments
	while (element_proto !== proto) {
		descriptors = get_descriptors(proto);

		for (var key in descriptors) {
			if (descriptors[key].set) {
				setters.push(key);
			}
		}

		proto = get_prototype_of(proto);
	}

	return setters;
}

/** @type {boolean | null} */
let supported = null;

/**
 * Checks if the browser supports rich HTML content inside `<option>` elements.
 * Modern browsers preserve HTML elements inside options, while older browsers
 * strip them during parsing, leaving only text content.
 * @returns {boolean}
 */
function is_supported() {
	if (supported === null) {
		var select = document.createElement('select');
		select.innerHTML = '<option><span>t</span></option>';
		supported = /** @type {Element} */ (select.firstChild)?.firstChild?.nodeType === 1;
	}

	return supported;
}

/**
 *
 * @param {HTMLElement} element
 * @param {(new_element: HTMLElement) => void} update_element
 */
function selectedcontent(element, update_element) {
	// if it's not supported no need for special logic
	if (!is_supported()) return;

	// we use the attach function directly just to make sure is executed when is mounted to the dom
	attach(element, () => () => {
		const select = element.closest('select');
		if (!select) return;

		const observer = new MutationObserver((entries) => {
			var selected = false;

			for (const entry of entries) {
				if (entry.target === element) {
					// the `<selectedcontent>` already changed, no need to replace it
					return;
				}

				// if the changes doesn't include the selected `<option>` we don't need to do anything
				selected ||= !!entry.target.parentElement?.closest('option')?.selected;
			}

			if (selected) {
				// replace the `<selectedcontent>` with a clone
				element.replaceWith((element = /** @type {HTMLElement} */ (element.cloneNode(true))));
				update_element(element);
			}
		});

		observer.observe(select, {
			childList: true,
			characterData: true,
			subtree: true
		});

		return () => {
			observer.disconnect();
		};
	});
}

/**
 * Handles rich HTML content inside `<option>`, `<optgroup>`, or `<select>` elements with browser-specific branching.
 * Modern browsers preserve HTML inside options, while older browsers strip it to text only.
 *
 * @param {HTMLOptionElement | HTMLOptGroupElement | HTMLSelectElement} element The element to process
 * @param {() => void} rich_fn Function to process rich HTML content (modern browsers)
 */
function customizable_select(element, rich_fn) {
	var was_hydrating = hydrating;

	if (!is_supported()) {
		set_hydrating(false);
		element.textContent = '';
		element.append(create_comment(''));
	}

	try {
		rich_fn();
	} finally {
		if (was_hydrating) {
			if (hydrating) {
				reset(element);
			} else {
				set_hydrating(true);
				set_hydrate_node(element);
			}
		}
	}
}

/**
 * @param {(activeElement: Element | null) => void} update
 * @returns {void}
 */
function bind_active_element(update) {
	listen(document, ['focusin', 'focusout'], (event) => {
		if (event && event.type === 'focusout' && /** @type {FocusEvent} */ (event).relatedTarget) {
			// The tests still pass if we remove this, because of JSDOM limitations, but it is necessary
			// to avoid temporarily resetting to `document.body`
			return;
		}

		update(document.activeElement);
	});
}

/** @import { Batch } from '../../../reactivity/batch.js' */

/**
 * @param {HTMLInputElement} input
 * @param {() => unknown} get
 * @param {(value: unknown) => void} set
 * @returns {void}
 */
function bind_value(input, get, set = get) {
	var batches = new WeakSet();

	listen_to_event_and_reset_event(input, 'input', async (is_reset) => {

		/** @type {any} */
		var value = is_reset ? input.defaultValue : input.value;
		value = is_numberlike_input(input) ? to_number(value) : value;
		set(value);

		if (current_batch !== null) {
			batches.add(current_batch);
		}

		// Because `{#each ...}` blocks work by updating sources inside the flush,
		// we need to wait a tick before checking to see if we should forcibly
		// update the input and reset the selection state
		await tick();

		// Respect any validation in accessors
		if (value !== (value = get())) {
			var start = input.selectionStart;
			var end = input.selectionEnd;
			var length = input.value.length;

			// the value is coerced on assignment
			input.value = value ?? '';

			// Restore selection
			if (end !== null) {
				var new_length = input.value.length;
				// If cursor was at end and new input is longer, move cursor to new end
				if (start === end && end === length && new_length > length) {
					input.selectionStart = new_length;
					input.selectionEnd = new_length;
				} else {
					input.selectionStart = start;
					input.selectionEnd = Math.min(end, new_length);
				}
			}
		}
	});

	if (
		// If we are hydrating and the value has since changed,
		// then use the updated value from the input instead.
		(hydrating && input.defaultValue !== input.value) ||
		// If defaultValue is set, then value == defaultValue
		// TODO Svelte 6: remove input.value check and set to empty string?
		(untrack(get) == null && input.value)
	) {
		set(is_numberlike_input(input) ? to_number(input.value) : input.value);

		if (current_batch !== null) {
			batches.add(current_batch);
		}
	}

	render_effect(() => {

		var value = get();

		if (input === document.activeElement) {
			// we need both, because in non-async mode, render effects run before previous_batch is set
			var batch = /** @type {Batch} */ (previous_batch ?? current_batch);

			// Never rewrite the contents of a focused input. We can get here if, for example,
			// an update is deferred because of async work depending on the input:
			//
			// <input bind:value={query}>
			// <p>{await find(query)}</p>
			if (batches.has(batch)) {
				return;
			}
		}

		if (is_numberlike_input(input) && value === to_number(input.value)) {
			// handles 0 vs 00 case (see https://github.com/sveltejs/svelte/issues/9959)
			return;
		}

		if (input.type === 'date' && !value && !input.value) {
			// Handles the case where a temporarily invalid date is set (while typing, for example with a leading 0 for the day)
			// and prevents this state from clearing the other parts of the date input (see https://github.com/sveltejs/svelte/issues/7897)
			return;
		}

		// don't set the value of the input if it's the same to allow
		// minlength to work properly
		if (value !== input.value) {
			// @ts-expect-error the value is coerced on assignment
			input.value = value ?? '';
		}
	});
}

/** @type {Set<HTMLInputElement[]>} */
const pending = new Set();

/**
 * @param {HTMLInputElement[]} inputs
 * @param {null | [number]} group_index
 * @param {HTMLInputElement} input
 * @param {() => unknown} get
 * @param {(value: unknown) => void} set
 * @returns {void}
 */
function bind_group(inputs, group_index, input, get, set = get) {
	var is_checkbox = input.getAttribute('type') === 'checkbox';
	var binding_group = inputs;

	// needs to be let or related code isn't treeshaken out if it's always false
	let hydration_mismatch = false;

	if (group_index !== null) {
		for (var index of group_index) {
			// @ts-expect-error
			binding_group = binding_group[index] ??= [];
		}
	}

	binding_group.push(input);

	listen_to_event_and_reset_event(
		input,
		'change',
		() => {
			// @ts-ignore
			var value = input.__value;

			if (is_checkbox) {
				value = get_binding_group_value(binding_group, value, input.checked);
			}

			set(value);
		},
		// TODO better default value handling
		() => set(is_checkbox ? [] : null)
	);

	render_effect(() => {
		var value = get();

		// If we are hydrating and the value has since changed, then use the update value
		// from the input instead.
		if (hydrating && input.defaultChecked !== input.checked) {
			hydration_mismatch = true;
			return;
		}

		if (is_checkbox) {
			value = value || [];
			// @ts-ignore
			input.checked = value.includes(input.__value);
		} else {
			// @ts-ignore
			input.checked = is(input.__value, value);
		}
	});

	teardown(() => {
		var index = binding_group.indexOf(input);

		if (index !== -1) {
			binding_group.splice(index, 1);
		}
	});

	if (!pending.has(binding_group)) {
		pending.add(binding_group);

		queue_micro_task(() => {
			// necessary to maintain binding group order in all insertion scenarios
			binding_group.sort((a, b) => (a.compareDocumentPosition(b) === 4 ? -1 : 1));
			pending.delete(binding_group);
		});
	}

	queue_micro_task(() => {
		if (hydration_mismatch) {
			var value;

			if (is_checkbox) {
				value = get_binding_group_value(binding_group, value, input.checked);
			} else {
				var hydration_input = binding_group.find((input) => input.checked);
				// @ts-ignore
				value = hydration_input?.__value;
			}

			set(value);
		}
	});
}

/**
 * @param {HTMLInputElement} input
 * @param {() => unknown} get
 * @param {(value: unknown) => void} set
 * @returns {void}
 */
function bind_checked(input, get, set = get) {
	listen_to_event_and_reset_event(input, 'change', (is_reset) => {
		var value = is_reset ? input.defaultChecked : input.checked;
		set(value);
	});

	if (
		// If we are hydrating and the value has since changed,
		// then use the update value from the input instead.
		(hydrating && input.defaultChecked !== input.checked) ||
		// If defaultChecked is set, then checked == defaultChecked
		untrack(get) == null
	) {
		set(input.checked);
	}

	render_effect(() => {
		var value = get();
		input.checked = Boolean(value);
	});
}

/**
 * @template V
 * @param {Array<HTMLInputElement>} group
 * @param {V} __value
 * @param {boolean} checked
 * @returns {V[]}
 */
function get_binding_group_value(group, __value, checked) {
	/** @type {Set<V>} */
	var value = new Set();

	for (var i = 0; i < group.length; i += 1) {
		if (group[i].checked) {
			// @ts-ignore
			value.add(group[i].__value);
		}
	}

	if (!checked) {
		value.delete(__value);
	}

	return Array.from(value);
}

/**
 * @param {HTMLInputElement} input
 */
function is_numberlike_input(input) {
	var type = input.type;
	return type === 'number' || type === 'range';
}

/**
 * @param {string} value
 */
function to_number(value) {
	return value === '' ? null : +value;
}

/**
 * @param {HTMLInputElement} input
 * @param {() => FileList | null} get
 * @param {(value: FileList | null) => void} set
 */
function bind_files(input, get, set = get) {
	listen_to_event_and_reset_event(input, 'change', () => {
		set(input.files);
	});

	if (
		// If we are hydrating and the value has since changed,
		// then use the updated value from the input instead.
		hydrating &&
		input.files
	) {
		set(input.files);
	}

	render_effect(() => {
		input.files = get();
	});
}

/** @param {TimeRanges} ranges */
function time_ranges_to_array(ranges) {
	var array = [];

	for (var i = 0; i < ranges.length; i += 1) {
		array.push({ start: ranges.start(i), end: ranges.end(i) });
	}

	return array;
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {() => number | undefined} get
 * @param {(value: number) => void} set
 * @returns {void}
 */
function bind_current_time(media, get, set = get) {
	/** @type {number} */
	var raf_id;
	/** @type {number} */
	var value;

	// Ideally, listening to timeupdate would be enough, but it fires too infrequently for the currentTime
	// binding, which is why we use a raf loop, too. We additionally still listen to timeupdate because
	// the user could be scrubbing through the video using the native controls when the media is paused.
	var callback = () => {
		cancelAnimationFrame(raf_id);

		if (!media.paused) {
			raf_id = requestAnimationFrame(callback);
		}

		var next_value = media.currentTime;
		if (value !== next_value) {
			set((value = next_value));
		}
	};

	raf_id = requestAnimationFrame(callback);
	media.addEventListener('timeupdate', callback);

	render_effect(() => {
		var next_value = Number(get());

		if (value !== next_value && !isNaN(/** @type {any} */ (next_value))) {
			media.currentTime = value = next_value;
		}
	});

	teardown(() => {
		cancelAnimationFrame(raf_id);
		media.removeEventListener('timeupdate', callback);
	});
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {(array: Array<{ start: number; end: number }>) => void} set
 */
function bind_buffered(media, set) {
	/** @type {{ start: number; end: number; }[]} */
	var current;

	// `buffered` can update without emitting any event, so we check it on various events.
	// By specs, `buffered` always returns a new object, so we have to compare deeply.
	listen(media, ['loadedmetadata', 'progress', 'timeupdate', 'seeking'], () => {
		var ranges = media.buffered;

		if (
			!current ||
			current.length !== ranges.length ||
			current.some((range, i) => ranges.start(i) !== range.start || ranges.end(i) !== range.end)
		) {
			current = time_ranges_to_array(ranges);
			set(current);
		}
	});
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {(array: Array<{ start: number; end: number }>) => void} set
 */
function bind_seekable(media, set) {
	listen(media, ['loadedmetadata'], () => set(time_ranges_to_array(media.seekable)));
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {(array: Array<{ start: number; end: number }>) => void} set
 */
function bind_played(media, set) {
	listen(media, ['timeupdate'], () => set(time_ranges_to_array(media.played)));
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {(seeking: boolean) => void} set
 */
function bind_seeking(media, set) {
	listen(media, ['seeking', 'seeked'], () => set(media.seeking));
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {(seeking: boolean) => void} set
 */
function bind_ended(media, set) {
	listen(media, ['timeupdate', 'ended'], () => set(media.ended));
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {(ready_state: number) => void} set
 */
function bind_ready_state(media, set) {
	listen(
		media,
		['loadedmetadata', 'loadeddata', 'canplay', 'canplaythrough', 'playing', 'waiting', 'emptied'],
		() => set(media.readyState)
	);
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {() => number | undefined} get
 * @param {(playback_rate: number) => void} set
 */
function bind_playback_rate(media, get, set = get) {
	// Needs to happen after element is inserted into the dom (which is guaranteed by using effect),
	// else playback will be set back to 1 by the browser
	effect(() => {
		var value = Number(get());

		if (value !== media.playbackRate && !isNaN(value)) {
			media.playbackRate = value;
		}
	});

	// Start listening to ratechange events after the element is inserted into the dom,
	// else playback will be set to 1 by the browser
	effect(() => {
		listen(media, ['ratechange'], () => {
			set(media.playbackRate);
		});
	});
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {() => boolean | undefined} get
 * @param {(paused: boolean) => void} set
 */
function bind_paused(media, get, set = get) {
	var paused = get();

	var update = () => {
		if (paused !== media.paused) {
			set((paused = media.paused));
		}
	};

	// If someone switches the src while media is playing, the player will pause.
	// Listen to the canplay event to get notified of this situation.
	listen(media, ['play', 'pause', 'canplay'], update, paused == null);

	// Needs to be an effect to ensure media element is mounted: else, if paused is `false` (i.e. should play right away)
	// a "The play() request was interrupted by a new load request" error would be thrown because the resource isn't loaded yet.
	effect(() => {
		if ((paused = !!get()) !== media.paused) {
			if (paused) {
				media.pause();
			} else {
				media.play().catch(() => {
					set((paused = true));
				});
			}
		}
	});
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {() => number | undefined} get
 * @param {(volume: number) => void} set
 */
function bind_volume(media, get, set = get) {
	var callback = () => {
		set(media.volume);
	};

	if (get() == null) {
		callback();
	}

	listen(media, ['volumechange'], callback, false);

	render_effect(() => {
		var value = Number(get());

		if (value !== media.volume && !isNaN(value)) {
			media.volume = value;
		}
	});
}

/**
 * @param {HTMLVideoElement | HTMLAudioElement} media
 * @param {() => boolean | undefined} get
 * @param {(muted: boolean) => void} set
 */
function bind_muted(media, get, set = get) {
	var callback = () => {
		set(media.muted);
	};

	if (get() == null) {
		callback();
	}

	listen(media, ['volumechange'], callback, false);

	render_effect(() => {
		var value = !!get();

		if (media.muted !== value) media.muted = value;
	});
}

/**
 * @param {(online: boolean) => void} update
 * @returns {void}
 */
function bind_online(update) {
	listen(window, ['online', 'offline'], () => {
		update(navigator.onLine);
	});
}

/**
 * Makes an `export`ed (non-prop) variable available on the `$$props` object
 * so that consumers can do `bind:x` on the component.
 * @template V
 * @param {Record<string, unknown>} props
 * @param {string} prop
 * @param {V} value
 * @returns {void}
 */
function bind_prop(props, prop, value) {
	var desc = get_descriptor(props, prop);

	if (desc && desc.set) {
		props[prop] = value;
		teardown(() => {
			props[prop] = null;
		});
	}
}

/**
 * Resize observer singleton.
 * One listener per element only!
 * https://groups.google.com/a/chromium.org/g/blink-dev/c/z6ienONUb5A/m/F5-VcUZtBAAJ
 */
class ResizeObserverSingleton {
	/** */
	#listeners = new WeakMap();

	/** @type {ResizeObserver | undefined} */
	#observer;

	/** @type {ResizeObserverOptions} */
	#options;

	/** @static */
	static entries = new WeakMap();

	/** @param {ResizeObserverOptions} options */
	constructor(options) {
		this.#options = options;
	}

	/**
	 * @param {Element} element
	 * @param {(entry: ResizeObserverEntry) => any} listener
	 */
	observe(element, listener) {
		var listeners = this.#listeners.get(element) || new Set();
		listeners.add(listener);

		this.#listeners.set(element, listeners);
		this.#getObserver().observe(element, this.#options);

		return () => {
			var listeners = this.#listeners.get(element);
			listeners.delete(listener);

			if (listeners.size === 0) {
				this.#listeners.delete(element);
				/** @type {ResizeObserver} */ (this.#observer).unobserve(element);
			}
		};
	}

	#getObserver() {
		return (
			this.#observer ??
			(this.#observer = new ResizeObserver(
				/** @param {any} entries */ (entries) => {
					for (var entry of entries) {
						ResizeObserverSingleton.entries.set(entry.target, entry);
						for (var listener of this.#listeners.get(entry.target) || []) {
							listener(entry);
						}
					}
				}
			))
		);
	}
}

var resize_observer_content_box = /* @__PURE__ */ new ResizeObserverSingleton({
	box: 'content-box'
});

var resize_observer_border_box = /* @__PURE__ */ new ResizeObserverSingleton({
	box: 'border-box'
});

var resize_observer_device_pixel_content_box = /* @__PURE__ */ new ResizeObserverSingleton({
	box: 'device-pixel-content-box'
});

/**
 * @param {Element} element
 * @param {'contentRect' | 'contentBoxSize' | 'borderBoxSize' | 'devicePixelContentBoxSize'} type
 * @param {(entry: keyof ResizeObserverEntry) => void} set
 */
function bind_resize_observer(element, type, set) {
	var observer =
		type === 'contentRect' || type === 'contentBoxSize'
			? resize_observer_content_box
			: type === 'borderBoxSize'
				? resize_observer_border_box
				: resize_observer_device_pixel_content_box;

	var unsub = observer.observe(element, /** @param {any} entry */ (entry) => set(entry[type]));
	teardown(unsub);
}

/**
 * @param {HTMLElement} element
 * @param {'clientWidth' | 'clientHeight' | 'offsetWidth' | 'offsetHeight'} type
 * @param {(size: number) => void} set
 */
function bind_element_size(element, type, set) {
	var unsub = resize_observer_border_box.observe(element, () => set(element[type]));

	effect(() => {
		// The update could contain reads which should be ignored
		untrack(() => set(element[type]));
		return unsub;
	});
}

/**
 * @param {any} bound_value
 * @param {Element} element_or_component
 * @returns {boolean}
 */
function is_bound_this(bound_value, element_or_component) {
	return (
		bound_value === element_or_component || bound_value?.[STATE_SYMBOL] === element_or_component
	);
}

/**
 * @param {any} element_or_component
 * @param {(value: unknown, ...parts: unknown[]) => void} update
 * @param {(...parts: unknown[]) => unknown} get_value
 * @param {() => unknown[]} [get_parts] Set if the this binding is used inside an each block,
 * 										returns all the parts of the each block context that are used in the expression
 * @returns {void}
 */
function bind_this(element_or_component = {}, update, get_value, get_parts) {
	effect(() => {
		/** @type {unknown[]} */
		var old_parts;

		/** @type {unknown[]} */
		var parts;

		render_effect(() => {
			old_parts = parts;
			// We only track changes to the parts, not the value itself to avoid unnecessary reruns.
			parts = get_parts?.() || [];

			untrack(() => {
				if (element_or_component !== get_value(...parts)) {
					update(element_or_component, ...parts);
					// If this is an effect rerun (cause: each block context changes), then nullify the binding at
					// the previous position if it isn't already taken over by a different effect.
					if (old_parts && is_bound_this(get_value(...old_parts), element_or_component)) {
						update(null, ...old_parts);
					}
				}
			});
		});

		return () => {
			// We cannot use effects in the teardown phase, we we use a microtask instead.
			queue_micro_task(() => {
				if (parts && is_bound_this(get_value(...parts), element_or_component)) {
					update(null, ...parts);
				}
			});
		};
	});

	return element_or_component;
}

/**
 * @param {'innerHTML' | 'textContent' | 'innerText'} property
 * @param {HTMLElement} element
 * @param {() => unknown} get
 * @param {(value: unknown) => void} set
 * @returns {void}
 */
function bind_content_editable(property, element, get, set = get) {
	element.addEventListener('input', () => {
		// @ts-ignore
		set(element[property]);
	});

	render_effect(() => {
		var value = get();

		if (element[property] !== value) {
			if (value == null) {
				// @ts-ignore
				var non_null_value = element[property];
				set(non_null_value);
			} else {
				// @ts-ignore
				element[property] = value + '';
			}
		}
	});
}

/**
 * @param {string} property
 * @param {string} event_name
 * @param {Element} element
 * @param {(value: unknown) => void} set
 * @param {() => unknown} [get]
 * @returns {void}
 */
function bind_property(property, event_name, element, set, get) {
	var handler = () => {
		// @ts-ignore
		set(element[property]);
	};

	element.addEventListener(event_name, handler);

	if (get) {
		render_effect(() => {
			// @ts-ignore
			element[property] = get();
		});
	} else {
		handler();
	}

	// @ts-ignore
	if (element === document.body || element === window || element === document) {
		teardown(() => {
			element.removeEventListener(event_name, handler);
		});
	}
}

/**
 * @param {HTMLElement} element
 * @param {(value: unknown) => void} set
 * @returns {void}
 */
function bind_focused(element, set) {
	listen(element, ['focus', 'blur'], () => {
		set(element === document.activeElement);
	});
}

/**
 * @param {'x' | 'y'} type
 * @param {() => number} get
 * @param {(value: number) => void} set
 * @returns {void}
 */
function bind_window_scroll(type, get, set = get) {
	var is_scrolling_x = type === 'x';

	var target_handler = () =>
		without_reactive_context(() => {
			scrolling = true;
			clearTimeout(timeout);
			timeout = setTimeout(clear, 100); // TODO use scrollend event if supported (or when supported everywhere?)

			set(window[is_scrolling_x ? 'scrollX' : 'scrollY']);
		});

	addEventListener('scroll', target_handler, {
		passive: true
	});

	var scrolling = false;

	/** @type {ReturnType<typeof setTimeout>} */
	var timeout;
	var clear = () => {
		scrolling = false;
	};
	var first = true;

	render_effect(() => {
		var latest_value = get();
		// Don't scroll to the initial value for accessibility reasons
		if (first) {
			first = false;
		} else if (!scrolling && latest_value != null) {
			scrolling = true;
			clearTimeout(timeout);
			if (is_scrolling_x) {
				scrollTo(latest_value, window.scrollY);
			} else {
				scrollTo(window.scrollX, latest_value);
			}
			timeout = setTimeout(clear, 100);
		}
	});

	// Browsers don't fire the scroll event for the initial scroll position when scroll style isn't set to smooth
	effect(target_handler);

	teardown(() => {
		removeEventListener('scroll', target_handler);
	});
}

/**
 * @param {'innerWidth' | 'innerHeight' | 'outerWidth' | 'outerHeight'} type
 * @param {(size: number) => void} set
 */
function bind_window_size(type, set) {
	listen(window, ['resize'], () => without_reactive_context(() => set(window[type])));
}

/** @import { ComponentContextLegacy } from '#client' */

/**
 * Legacy-mode only: Call `onMount` callbacks and set up `beforeUpdate`/`afterUpdate` effects
 * @param {boolean} [immutable]
 */
function init(immutable = false) {
	const context = /** @type {ComponentContextLegacy} */ (component_context);

	const callbacks = context.l.u;
	if (!callbacks) return;

	let props = () => deep_read_state(context.s);

	if (immutable) {
		let version = 0;
		let prev = /** @type {Record<string, any>} */ ({});

		// In legacy immutable mode, before/afterUpdate only fire if the object identity of a prop changes
		const d = derived(() => {
			let changed = false;
			const props = context.s;
			for (const key in props) {
				if (props[key] !== prev[key]) {
					prev[key] = props[key];
					changed = true;
				}
			}
			if (changed) version++;
			return version;
		});

		props = () => get(d);
	}

	// beforeUpdate
	if (callbacks.b.length) {
		user_pre_effect(() => {
			observe_all(context, props);
			run_all(callbacks.b);
		});
	}

	// onMount (must run before afterUpdate)
	user_effect(() => {
		const fns = untrack(() => callbacks.m.map(run));
		return () => {
			for (const fn of fns) {
				if (typeof fn === 'function') {
					fn();
				}
			}
		};
	});

	// afterUpdate
	if (callbacks.a.length) {
		user_effect(() => {
			observe_all(context, props);
			run_all(callbacks.a);
		});
	}
}

/**
 * Invoke the getter of all signals associated with a component
 * so they can be registered to the effect this function is called in.
 * @param {ComponentContextLegacy} context
 * @param {(() => void)} props
 */
function observe_all(context, props) {
	if (context.l.s) {
		for (const signal of context.l.s) get(signal);
	}

	props();
}

/**
 * Under some circumstances, imports may be reactive in legacy mode. In that case,
 * they should be using `reactive_import` as part of the transformation
 * @param {() => any} fn
 */
function reactive_import(fn) {
	var s = source(0);

	return function () {
		if (arguments.length === 1) {
			set(s, get(s) + 1);
			return arguments[0];
		} else {
			get(s);
			return fn();
		}
	};
}

/**
 * @this {any}
 * @param {Record<string, unknown>} $$props
 * @param {Event} event
 * @returns {void}
 */
function bubble_event($$props, event) {
	var events = /** @type {Record<string, Function[] | Function>} */ ($$props.$$events)?.[
		event.type
	];

	var callbacks = is_array(events) ? events.slice() : events == null ? [] : [events];

	for (var fn of callbacks) {
		// Preserve "this" context
		fn.call(this, event);
	}
}

/**
 * Used to simulate `$on` on a component instance when `compatibility.componentApi === 4`
 * @param {Record<string, any>} $$props
 * @param {string} event_name
 * @param {Function} event_callback
 */
function add_legacy_event_listener($$props, event_name, event_callback) {
	$$props.$$events ||= {};
	$$props.$$events[event_name] ||= [];
	$$props.$$events[event_name].push(event_callback);
}

/**
 * Used to simulate `$set` on a component instance when `compatibility.componentApi === 4`.
 * Needs component accessors so that it can call the setter of the prop. Therefore doesn't
 * work for updating props in `$$props` or `$$restProps`.
 * @this {Record<string, any>}
 * @param {Record<string, any>} $$new_props
 */
function update_legacy_props($$new_props) {
	for (var key in $$new_props) {
		if (key in this) {
			this[key] = $$new_props[key];
		}
	}
}

/** @import { StoreReferencesContainer } from '#client' */
/** @import { Store } from '#shared' */

/**
 * Whether or not the prop currently being read is a store binding, as in
 * `<Child bind:x={$y} />`. If it is, we treat the prop as mutable even in
 * runes mode, and skip `binding_property_non_reactive` validation
 */
let is_store_binding = false;

let IS_UNMOUNTED = Symbol();

/**
 * Gets the current value of a store. If the store isn't subscribed to yet, it will create a proxy
 * signal that will be updated when the store is. The store references container is needed to
 * track reassignments to stores and to track the correct component context.
 * @template V
 * @param {Store<V> | null | undefined} store
 * @param {string} store_name
 * @param {StoreReferencesContainer} stores
 * @returns {V}
 */
function store_get(store, store_name, stores) {
	const entry = (stores[store_name] ??= {
		store: null,
		source: mutable_source(undefined),
		unsubscribe: noop
	});

	// if the component that setup this is already unmounted we don't want to register a subscription
	if (entry.store !== store && !(IS_UNMOUNTED in stores)) {
		entry.unsubscribe();
		entry.store = store ?? null;

		if (store == null) {
			entry.source.v = undefined; // see synchronous callback comment below
			entry.unsubscribe = noop;
		} else {
			var is_synchronous_callback = true;

			entry.unsubscribe = subscribe_to_store(store, (v) => {
				if (is_synchronous_callback) {
					// If the first updates to the store value (possibly multiple of them) are synchronously
					// inside a derived, we will hit the `state_unsafe_mutation` error if we `set` the value
					entry.source.v = v;
				} else {
					set(entry.source, v);
				}
			});

			is_synchronous_callback = false;
		}
	}

	// if the component that setup this stores is already unmounted the source will be out of sync
	// so we just use the `get` for the stores, less performant but it avoids to create a memory leak
	// and it will keep the value consistent
	if (store && IS_UNMOUNTED in stores) {
		return get$1(store);
	}

	return get(entry.source);
}

/**
 * Unsubscribe from a store if it's not the same as the one in the store references container.
 * We need this in addition to `store_get` because someone could unsubscribe from a store but
 * then never subscribe to the new one (if any), causing the subscription to stay open wrongfully.
 * @param {Store<any> | null | undefined} store
 * @param {string} store_name
 * @param {StoreReferencesContainer} stores
 */
function store_unsub(store, store_name, stores) {
	/** @type {StoreReferencesContainer[''] | undefined} */
	let entry = stores[store_name];

	if (entry && entry.store !== store) {
		// Don't reset store yet, so that store_get above can resubscribe to new store if necessary
		entry.unsubscribe();
		entry.unsubscribe = noop;
	}

	return store;
}

/**
 * Sets the new value of a store and returns that value.
 * @template V
 * @param {Store<V>} store
 * @param {V} value
 * @returns {V}
 */
function store_set(store, value) {
	store.set(value);
	return value;
}

/**
 * @param {StoreReferencesContainer} stores
 * @param {string} store_name
 */
function invalidate_store(stores, store_name) {
	var entry = stores[store_name];
	if (entry.store !== null) {
		store_set(entry.store, entry.source.v);
	}
}

/**
 * Unsubscribes from all auto-subscribed stores on destroy
 * @returns {[StoreReferencesContainer, ()=>void]}
 */
function setup_stores() {
	/** @type {StoreReferencesContainer} */
	const stores = {};

	function cleanup() {
		teardown(() => {
			for (var store_name in stores) {
				const ref = stores[store_name];
				ref.unsubscribe();
			}
			define_property(stores, IS_UNMOUNTED, {
				enumerable: false,
				value: true
			});
		});
	}

	return [stores, cleanup];
}

/**
 * Updates a store with a new value.
 * @param {Store<V>} store  the store to update
 * @param {any} expression  the expression that mutates the store
 * @param {V} new_value  the new store value
 * @template V
 */
function store_mutate(store, expression, new_value) {
	store.set(new_value);
	return expression;
}

/**
 * @param {Store<number>} store
 * @param {number} store_value
 * @param {1 | -1} [d]
 * @returns {number}
 */
function update_store(store, store_value, d = 1) {
	store.set(store_value + d);
	return store_value;
}

/**
 * @param {Store<number>} store
 * @param {number} store_value
 * @param {1 | -1} [d]
 * @returns {number}
 */
function update_pre_store(store, store_value, d = 1) {
	const value = store_value + d;
	store.set(value);
	return value;
}

/**
 * Called inside prop getters to communicate that the prop is a store binding
 */
function mark_store_binding() {
	is_store_binding = true;
}

/**
 * Returns a tuple that indicates whether `fn()` reads a prop that is a store binding.
 * Used to prevent `binding_property_non_reactive` validation false positives and
 * ensure that these props are treated as mutable even in runes mode
 * @template T
 * @param {() => T} fn
 * @returns {[T, boolean]}
 */
function capture_store_binding(fn) {
	var previous_is_store_binding = is_store_binding;

	try {
		is_store_binding = false;
		return [fn(), is_store_binding];
	} finally {
		is_store_binding = previous_is_store_binding;
	}
}

/** @import { Effect, Source } from './types.js' */

/**
 * @param {((value?: number) => number)} fn
 * @param {1 | -1} [d]
 * @returns {number}
 */
function update_prop(fn, d = 1) {
	const value = fn();
	fn(value + d);
	return value;
}

/**
 * @param {((value?: number) => number)} fn
 * @param {1 | -1} [d]
 * @returns {number}
 */
function update_pre_prop(fn, d = 1) {
	const value = fn() + d;
	fn(value);
	return value;
}

/**
 * The proxy handler for rest props (i.e. `const { x, ...rest } = $props()`).
 * Is passed the full `$$props` object and excludes the named props.
 * @type {ProxyHandler<{ props: Record<string | symbol, unknown>, exclude: Array<string | symbol>, name?: string }>}}
 */
const rest_props_handler = {
	get(target, key) {
		if (target.exclude.includes(key)) return;
		return target.props[key];
	},
	set(target, key) {

		return false;
	},
	getOwnPropertyDescriptor(target, key) {
		if (target.exclude.includes(key)) return;
		if (key in target.props) {
			return {
				enumerable: true,
				configurable: true,
				value: target.props[key]
			};
		}
	},
	has(target, key) {
		if (target.exclude.includes(key)) return false;
		return key in target.props;
	},
	ownKeys(target) {
		return Reflect.ownKeys(target.props).filter((key) => !target.exclude.includes(key));
	}
};

/**
 * @param {Record<string, unknown>} props
 * @param {string[]} exclude
 * @param {string} [name]
 * @returns {Record<string, unknown>}
 */
/*#__NO_SIDE_EFFECTS__*/
function rest_props(props, exclude, name) {
	return new Proxy(
		{ props, exclude },
		rest_props_handler
	);
}

/**
 * The proxy handler for legacy $$restProps and $$props
 * @type {ProxyHandler<{ props: Record<string | symbol, unknown>, exclude: Array<string | symbol>, special: Record<string | symbol, (v?: unknown) => unknown>, version: Source<number>, parent_effect: Effect }>}}
 */
const legacy_rest_props_handler = {
	get(target, key) {
		if (target.exclude.includes(key)) return;
		get(target.version);
		return key in target.special ? target.special[key]() : target.props[key];
	},
	set(target, key, value) {
		if (!(key in target.special)) {
			var previous_effect = active_effect;

			try {
				set_active_effect(target.parent_effect);

				// Handle props that can temporarily get out of sync with the parent
				/** @type {Record<string, (v?: unknown) => unknown>} */
				target.special[key] = prop(
					{
						get [key]() {
							return target.props[key];
						}
					},
					/** @type {string} */ (key),
					PROPS_IS_UPDATED
				);
			} finally {
				set_active_effect(previous_effect);
			}
		}

		target.special[key](value);
		update(target.version); // $$props is coarse-grained: when $$props.x is updated, usages of $$props.y etc are also rerun
		return true;
	},
	getOwnPropertyDescriptor(target, key) {
		if (target.exclude.includes(key)) return;
		if (key in target.props) {
			return {
				enumerable: true,
				configurable: true,
				value: target.props[key]
			};
		}
	},
	deleteProperty(target, key) {
		// Svelte 4 allowed for deletions on $$restProps
		if (target.exclude.includes(key)) return true;
		target.exclude.push(key);
		update(target.version);
		return true;
	},
	has(target, key) {
		if (target.exclude.includes(key)) return false;
		return key in target.props;
	},
	ownKeys(target) {
		return Reflect.ownKeys(target.props).filter((key) => !target.exclude.includes(key));
	}
};

/**
 * @param {Record<string, unknown>} props
 * @param {string[]} exclude
 * @returns {Record<string, unknown>}
 */
function legacy_rest_props(props, exclude) {
	return new Proxy(
		{
			props,
			exclude,
			special: {},
			version: source(0),
			// TODO this is only necessary because we need to track component
			// destruction inside `prop`, because of `bind:this`, but it
			// seems likely that we can simplify `bind:this` instead
			parent_effect: /** @type {Effect} */ (active_effect)
		},
		legacy_rest_props_handler
	);
}

/**
 * The proxy handler for spread props. Handles the incoming array of props
 * that looks like `() => { dynamic: props }, { static: prop }, ..` and wraps
 * them so that the whole thing is passed to the component as the `$$props` argument.
 * @type {ProxyHandler<{ props: Array<Record<string | symbol, unknown> | (() => Record<string | symbol, unknown>)> }>}}
 */
const spread_props_handler = {
	get(target, key) {
		let i = target.props.length;
		while (i--) {
			let p = target.props[i];
			if (is_function(p)) p = p();
			if (typeof p === 'object' && p !== null && key in p) return p[key];
		}
	},
	set(target, key, value) {
		let i = target.props.length;
		while (i--) {
			let p = target.props[i];
			if (is_function(p)) p = p();
			const desc = get_descriptor(p, key);
			if (desc && desc.set) {
				desc.set(value);
				return true;
			}
		}
		return false;
	},
	getOwnPropertyDescriptor(target, key) {
		let i = target.props.length;
		while (i--) {
			let p = target.props[i];
			if (is_function(p)) p = p();
			if (typeof p === 'object' && p !== null && key in p) {
				const descriptor = get_descriptor(p, key);
				if (descriptor && !descriptor.configurable) {
					// Prevent a "Non-configurability Report Error": The target is an array, it does
					// not actually contain this property. If it is now described as non-configurable,
					// the proxy throws a validation error. Setting it to true avoids that.
					descriptor.configurable = true;
				}
				return descriptor;
			}
		}
	},
	has(target, key) {
		// To prevent a false positive `is_entry_props` in the `prop` function
		if (key === STATE_SYMBOL || key === LEGACY_PROPS) return false;

		for (let p of target.props) {
			if (is_function(p)) p = p();
			if (p != null && key in p) return true;
		}

		return false;
	},
	ownKeys(target) {
		/** @type {Array<string | symbol>} */
		const keys = [];

		for (let p of target.props) {
			if (is_function(p)) p = p();
			if (!p) continue;

			for (const key in p) {
				if (!keys.includes(key)) keys.push(key);
			}

			for (const key of Object.getOwnPropertySymbols(p)) {
				if (!keys.includes(key)) keys.push(key);
			}
		}

		return keys;
	}
};

/**
 * @param {Array<Record<string, unknown> | (() => Record<string, unknown>)>} props
 * @returns {any}
 */
function spread_props(...props) {
	return new Proxy({ props }, spread_props_handler);
}

/**
 * This function is responsible for synchronizing a possibly bound prop with the inner component state.
 * It is used whenever the compiler sees that the component writes to the prop, or when it has a default prop_value.
 * @template V
 * @param {Record<string, unknown>} props
 * @param {string} key
 * @param {number} flags
 * @param {V | (() => V)} [fallback]
 * @returns {(() => V | ((arg: V) => V) | ((arg: V, mutation: boolean) => V))}
 */
function prop(props, key, flags, fallback) {
	var runes = !legacy_mode_flag || (flags & PROPS_IS_RUNES) !== 0;
	var bindable = (flags & PROPS_IS_BINDABLE) !== 0;
	var lazy = (flags & PROPS_IS_LAZY_INITIAL) !== 0;

	var fallback_value = /** @type {V} */ (fallback);
	var fallback_dirty = true;

	var get_fallback = () => {
		if (fallback_dirty) {
			fallback_dirty = false;

			fallback_value = lazy
				? untrack(/** @type {() => V} */ (fallback))
				: /** @type {V} */ (fallback);
		}

		return fallback_value;
	};

	/** @type {((v: V) => void) | undefined} */
	var setter;

	if (bindable) {
		// Can be the case when someone does `mount(Component, props)` with `let props = $state({...})`
		// or `createClassComponent(Component, props)`
		var is_entry_props = STATE_SYMBOL in props || LEGACY_PROPS in props;

		setter =
			get_descriptor(props, key)?.set ??
			(is_entry_props && key in props ? (v) => (props[key] = v) : undefined);
	}

	var initial_value;
	var is_store_sub = false;

	if (bindable) {
		[initial_value, is_store_sub] = capture_store_binding(() => /** @type {V} */ (props[key]));
	} else {
		initial_value = /** @type {V} */ (props[key]);
	}

	if (initial_value === undefined && fallback !== undefined) {
		initial_value = get_fallback();

		if (setter) {
			if (runes) props_invalid_value();
			setter(initial_value);
		}
	}

	/** @type {() => V} */
	var getter;

	if (runes) {
		getter = () => {
			var value = /** @type {V} */ (props[key]);
			if (value === undefined) return get_fallback();
			fallback_dirty = true;
			return value;
		};
	} else {
		getter = () => {
			var value = /** @type {V} */ (props[key]);

			if (value !== undefined) {
				// in legacy mode, we don't revert to the fallback value
				// if the prop goes from defined to undefined. The easiest
				// way to model this is to make the fallback undefined
				// as soon as the prop has a value
				fallback_value = /** @type {V} */ (undefined);
			}

			return value === undefined ? fallback_value : value;
		};
	}

	// prop is never written to — we only need a getter
	if (runes && (flags & PROPS_IS_UPDATED) === 0) {
		return getter;
	}

	// prop is written to, but the parent component had `bind:foo` which
	// means we can just call `$$props.foo = value` directly
	if (setter) {
		var legacy_parent = props.$$legacy;
		return /** @type {() => V} */ (
			function (/** @type {V} */ value, /** @type {boolean} */ mutation) {
				if (arguments.length > 0) {
					// We don't want to notify if the value was mutated and the parent is in runes mode.
					// In that case the state proxy (if it exists) should take care of the notification.
					// If the parent is not in runes mode, we need to notify on mutation, too, that the prop
					// has changed because the parent will not be able to detect the change otherwise.
					if (!runes || !mutation || legacy_parent || is_store_sub) {
						/** @type {Function} */ (setter)(mutation ? getter() : value);
					}

					return value;
				}

				return getter();
			}
		);
	}

	// Either prop is written to, but there's no binding, which means we
	// create a derived that we can write to locally.
	// Or we are in legacy mode where we always create a derived to replicate that
	// Svelte 4 did not trigger updates when a primitive value was updated to the same value.
	var overridden = false;

	var d = ((flags & PROPS_IS_IMMUTABLE) !== 0 ? derived : derived_safe_equal)(() => {
		overridden = false;
		return getter();
	});

	// Capture the initial value if it's bindable
	if (bindable) get(d);

	var parent_effect = /** @type {Effect} */ (active_effect);

	return /** @type {() => V} */ (
		function (/** @type {any} */ value, /** @type {boolean} */ mutation) {
			if (arguments.length > 0) {
				const new_value = mutation ? get(d) : runes && bindable ? proxy(value) : value;

				set(d, new_value);
				overridden = true;

				if (fallback_value !== undefined) {
					fallback_value = new_value;
				}

				return value;
			}

			// special case — avoid recalculating the derived if we're in a
			// teardown function and the prop was overridden locally, or the
			// component was already destroyed (this latter part is necessary
			// because `bind:this` can read props after the component has
			// been destroyed. TODO simplify `bind:this`
			if ((is_destroying_effect && overridden) || (parent_effect.f & DESTROYED) !== 0) {
				return d.v;
			}

			return get(d);
		}
	);
}

/**
 * @param {() => any} collection
 * @param {(item: any, index: number) => string} key_fn
 * @returns {void}
 */
function validate_each_keys(collection, key_fn) {
	render_effect(() => {
		const keys = new Map();
		const maybe_array = collection();
		const array = is_array(maybe_array)
			? maybe_array
			: maybe_array == null
				? []
				: Array.from(maybe_array);
		const length = array.length;
		for (let i = 0; i < length; i++) {
			const key = key_fn(array[i], i);
			if (keys.has(key)) {
				String(keys.get(key));

				/** @type {string | null} */
				let k = String(key);
				if (k.startsWith('[object ')) k = null;

				each_key_duplicate();
			}
			keys.set(key, i);
		}
	});
}

/**
 * @param {string} binding
 * @param {Array<Promise<void>>} blockers
 * @param {() => Record<string, any>} get_object
 * @param {() => string} get_property
 * @param {number} line
 * @param {number} column
 */
function validate_binding(binding, blockers, get_object, get_property, line, column) {
	run_after_blockers(blockers, () => {
		var warned = false;

		dev_current_component_function?.[FILENAME];

		render_effect(() => {
			if (warned) return;

			var [object, is_store_sub] = capture_store_binding(get_object);

			if (is_store_sub) return;

			var property = get_property();

			var ran = false;

			// by making the (possibly false, but it would be an extreme edge case) assumption
			// that a getter has a corresponding setter, we can determine if a property is
			// reactive by seeing if this effect has dependencies
			var effect = render_effect(() => {
				if (ran) return;

				// eslint-disable-next-line @typescript-eslint/no-unused-expressions
				object[property];
			});

			ran = true;

			if (effect.deps === null) {
				binding_property_non_reactive();

				warned = true;
			}
		});
	});
}

/**
 * @param {string} method
 * @param  {...any} objects
 */
function log_if_contains_state(method, ...objects) {
	untrack(() => {
		try {
			let has_state = false;
			const transformed = [];

			for (const obj of objects) {
				if (obj && typeof obj === 'object' && STATE_SYMBOL in obj) {
					transformed.push(snapshot(obj, true));
					has_state = true;
				} else {
					transformed.push(obj);
				}
			}

			if (has_state) {
				console_log_state(method);

				// eslint-disable-next-line no-console
				console.log('%c[snapshot]', 'color: grey', ...transformed);
			}
		} catch {}
	});

	return objects;
}

var _ = /*#__PURE__*/Object.freeze({
	__proto__: null,
	CLASS: CLASS,
	FILENAME: FILENAME,
	HMR: HMR,
	NAMESPACE_SVG: NAMESPACE_SVG,
	STYLE: STYLE,
	aborted: aborted,
	action: action,
	get active_effect () { return active_effect; },
	add_legacy_event_listener: add_legacy_event_listener,
	add_locations: add_locations,
	add_svelte_meta: add_svelte_meta,
	animation: animation,
	append: append,
	append_styles: append_styles,
	apply: apply,
	assign: assign$1,
	assign_and: assign_and,
	assign_nullish: assign_nullish,
	assign_or: assign_or,
	async: async,
	async_derived: async_derived,
	attach: attach,
	attachment: createAttachmentKey,
	attr: attr,
	attribute_effect: attribute_effect,
	autofocus: autofocus,
	await: await_block,
	bind_active_element: bind_active_element,
	bind_buffered: bind_buffered,
	bind_checked: bind_checked,
	bind_content_editable: bind_content_editable,
	bind_current_time: bind_current_time,
	bind_element_size: bind_element_size,
	bind_ended: bind_ended,
	bind_files: bind_files,
	bind_focused: bind_focused,
	bind_group: bind_group,
	bind_muted: bind_muted,
	bind_online: bind_online,
	bind_paused: bind_paused,
	bind_playback_rate: bind_playback_rate,
	bind_played: bind_played,
	bind_prop: bind_prop,
	bind_property: bind_property,
	bind_ready_state: bind_ready_state,
	bind_resize_observer: bind_resize_observer,
	bind_seekable: bind_seekable,
	bind_seeking: bind_seeking,
	bind_select_value: bind_select_value,
	bind_this: bind_this,
	bind_value: bind_value,
	bind_volume: bind_volume,
	bind_window_scroll: bind_window_scroll,
	bind_window_size: bind_window_size,
	boundary: boundary,
	bubble_event: bubble_event,
	check_target: check_target,
	child: child,
	cleanup_styles: cleanup_styles,
	clsx: clsx,
	comment: comment,
	component: component,
	create_custom_element: create_custom_element,
	create_ownership_validator: create_ownership_validator,
	css_props: css_props,
	customizable_select: customizable_select,
	deep_read: deep_read,
	deep_read_state: deep_read_state,
	deferred_template_effect: deferred_template_effect,
	delegate: delegate,
	derived: user_derived,
	derived_safe_equal: derived_safe_equal,
	get document () { return $document; },
	each: each,
	eager: eager,
	effect: effect,
	effect_root: effect_root,
	effect_tracking: effect_tracking,
	element: element,
	equals: equals,
	event: event,
	exclude_from_object: exclude_from_object,
	fallback: fallback,
	first_child: first_child,
	flush: flushSync,
	for_await_track_reactivity_loss: for_await_track_reactivity_loss,
	from_html: from_html,
	from_mathml: from_mathml,
	from_svg: from_svg,
	from_tree: from_tree,
	get: get,
	head: head,
	hmr: hmr,
	html: html,
	hydrate_template: hydrate_template,
	if: if_block,
	index: index,
	init: init,
	init_select: init_select,
	inspect: inspect,
	invalid_default_snippet: invalid_default_snippet,
	invalidate_inner_signals: invalidate_inner_signals,
	invalidate_store: invalidate_store,
	invoke_error_boundary: invoke_error_boundary,
	key: key,
	legacy_api: legacy_api,
	legacy_pre_effect: legacy_pre_effect,
	legacy_pre_effect_reset: legacy_pre_effect_reset,
	legacy_rest_props: legacy_rest_props,
	log_if_contains_state: log_if_contains_state,
	mark_store_binding: mark_store_binding,
	mutable_source: mutable_source,
	mutate: mutate,
	next: next,
	noop: noop,
	once: once,
	pending: pending$1,
	pop: pop,
	preventDefault: preventDefault,
	prevent_snippet_stringification: prevent_snippet_stringification,
	prop: prop,
	props_id: props_id,
	proxy: proxy,
	push: push,
	raf: raf,
	reactive_import: reactive_import,
	remove_input_defaults: remove_input_defaults,
	remove_textarea_child: remove_textarea_child,
	render_effect: render_effect,
	replay_events: replay_events,
	reset: reset,
	rest_props: rest_props,
	run: run$1,
	run_after_blockers: run_after_blockers,
	safe_get: safe_get,
	sanitize_slots: sanitize_slots,
	save: save,
	select_option: select_option,
	selectedcontent: selectedcontent,
	self: self,
	set: set,
	set_attribute: set_attribute,
	set_checked: set_checked,
	set_class: set_class,
	set_custom_element_data: set_custom_element_data,
	set_default_checked: set_default_checked,
	set_default_value: set_default_value,
	set_selected: set_selected,
	set_style: set_style,
	set_text: set_text,
	set_value: set_value,
	set_xlink_attribute: set_xlink_attribute,
	setup_stores: setup_stores,
	sibling: sibling,
	slot: slot,
	snapshot: snapshot,
	snippet: snippet,
	spread_props: spread_props,
	state: state,
	stopImmediatePropagation: stopImmediatePropagation,
	stopPropagation: stopPropagation,
	store_get: store_get,
	store_mutate: store_mutate,
	store_set: store_set,
	store_unsub: store_unsub,
	strict_equals: strict_equals,
	tag: tag,
	tag_proxy: tag_proxy,
	template_effect: template_effect,
	text: text,
	tick: tick,
	to_array: to_array,
	trace: trace,
	track_reactivity_loss: track_reactivity_loss,
	transition: transition,
	trusted: trusted,
	untrack: untrack,
	update: update,
	update_legacy_props: update_legacy_props,
	update_pre: update_pre,
	update_pre_prop: update_pre_prop,
	update_pre_store: update_pre_store,
	update_prop: update_prop,
	update_store: update_store,
	user_effect: user_effect,
	user_pre_effect: user_pre_effect,
	validate_binding: validate_binding,
	validate_dynamic_element_tag: validate_dynamic_element_tag,
	validate_each_keys: validate_each_keys,
	validate_snippet_args: validate_snippet_args,
	validate_store: validate_store,
	validate_void_dynamic_element: validate_void_dynamic_element,
	get window () { return $window; },
	with_script: with_script,
	wrap_snippet: wrap_snippet
});

/** @import { Readable, Writable } from './public.js' */

/**
 * @template V
 * @overload
 * @param {() => V} get
 * @param {(v: V) => void} set
 * @returns {Writable<V>}
 */
/**
 * @template V
 * @overload
 * @param {() => V} get
 * @returns {Readable<V>}
 */
/**
 * Create a store from a function that returns state, and (to make a writable store), an
 * optional second function that sets state.
 *
 * ```ts
 * import { toStore } from 'svelte/store';
 *
 * let count = $state(0);
 *
 * const store = toStore(() => count, (v) => (count = v));
 * ```
 * @template V
 * @param {() => V} get
 * @param {(v: V) => void} [set]
 * @returns {Writable<V> | Readable<V>}
 */
function toStore(get, set) {
	const store = writable(get());

	if (set) {
		return {
			set,
			update: (fn) => set(fn(get())),
			subscribe: store.subscribe
		};
	}

	return {
		subscribe: store.subscribe
	};
}

/**
 * @template V
 * @overload
 * @param {Writable<V>} store
 * @returns {{ current: V }}
 */
/**
 * @template V
 * @overload
 * @param {Readable<V>} store
 * @returns {{ readonly current: V }}
 */
/**
 * Convert a store to an object with a reactive `current` property. If `store`
 * is a readable store, `current` will be a readonly property.
 *
 * ```ts
 * import { fromStore, get, writable } from 'svelte/store';
 *
 * const store = writable(0);
 *
 * const count = fromStore(store);
 *
 * count.current; // 0;
 * store.set(1);
 * count.current; // 1
 *
 * count.current += 1;
 * get(store); // 2
 * ```
 * @template V
 * @param {Writable<V> | Readable<V>} store
 */
function fromStore(store) {
	if ('set' in store) {
		return {
			get current() {
				return get$1(store);
			},
			set current(v) {
				store.set(v);
			}
		};
	}

	return {
		get current() {
			return get$1(store);
		}
	};
}

var p = /*#__PURE__*/Object.freeze({
	__proto__: null,
	derived: derived$1,
	fromStore: fromStore,
	get: get$1,
	readable: readable,
	readonly: readonly,
	toStore: toStore,
	writable: writable
});

/** @import { FlipParams, AnimationConfig } from './public.js' */

/**
 * The flip function calculates the start and end position of an element and animates between them, translating the x and y values.
 * `flip` stands for [First, Last, Invert, Play](https://aerotwist.com/blog/flip-your-animations/).
 *
 * @param {Element} node
 * @param {{ from: DOMRect; to: DOMRect }} fromTo
 * @param {FlipParams} params
 * @returns {AnimationConfig}
 */
function flip(node, { from, to }, params = {}) {
	var { delay = 0, duration = (d) => Math.sqrt(d) * 120, easing = cubicOut } = params;

	var style = getComputedStyle(node);

	// find the transform origin, expressed as a pair of values between 0 and 1
	var transform = style.transform === 'none' ? '' : style.transform;
	var [ox, oy] = style.transformOrigin.split(' ').map(parseFloat);
	ox /= node.clientWidth;
	oy /= node.clientHeight;

	// calculate effect of parent transforms and zoom
	var zoom = get_zoom(node); // https://drafts.csswg.org/css-viewport/#effective-zoom
	var sx = node.clientWidth / to.width / zoom;
	var sy = node.clientHeight / to.height / zoom;

	// find the starting position of the transform origin
	var fx = from.left + from.width * ox;
	var fy = from.top + from.height * oy;

	// find the ending position of the transform origin
	var tx = to.left + to.width * ox;
	var ty = to.top + to.height * oy;

	// find the translation at the start of the transform
	var dx = (fx - tx) * sx;
	var dy = (fy - ty) * sy;

	// find the relative scale at the start of the transform
	var dsx = from.width / to.width;
	var dsy = from.height / to.height;

	return {
		delay,
		duration: typeof duration === 'function' ? duration(Math.sqrt(dx * dx + dy * dy)) : duration,
		easing,
		css: (t, u) => {
			var x = u * dx;
			var y = u * dy;
			var sx = t + u * dsx;
			var sy = t + u * dsy;

			return `transform: ${transform} translate(${x}px, ${y}px) scale(${sx}, ${sy});`;
		}
	};
}

/**
 * @param {Element} element
 */
function get_zoom(element) {
	if ('currentCSSZoom' in element) {
		return /** @type {number} */ (element.currentCSSZoom);
	}

	/** @type {Element | null} */
	var current = element;
	var zoom = 1;

	while (current !== null) {
		zoom *= +getComputedStyle(current).zoom;
		current = /** @type {Element | null} */ (current.parentElement);
	}

	return zoom;
}

var i = /*#__PURE__*/Object.freeze({
	__proto__: null,
	flip: flip
});

enable_legacy_mode_flag();

var a = /*#__PURE__*/Object.freeze({
	__proto__: null
});

enable_tracing_mode_flag();

var v = /*#__PURE__*/Object.freeze({
	__proto__: null
});

const SvelteDate = globalThis.Date;
const SvelteSet = globalThis.Set;
const SvelteMap = globalThis.Map;
const SvelteURL = globalThis.URL;
const SvelteURLSearchParams = globalThis.URLSearchParams;

class MediaQuery {
	current;
	/**
	 * @param {string} query
	 * @param {boolean} [matches]
	 */
	constructor(query, matches = false) {
		this.current = matches;
	}
}

/**
 * @param {any} _
 */
function createSubscriber(_) {
	return () => {};
}

var m = /*#__PURE__*/Object.freeze({
	__proto__: null,
	MediaQuery: MediaQuery,
	SvelteDate: SvelteDate,
	SvelteMap: SvelteMap,
	SvelteSet: SvelteSet,
	SvelteURL: SvelteURL,
	SvelteURLSearchParams: SvelteURLSearchParams,
	createSubscriber: createSubscriber
});

/**
 * A [media query](https://svelte.dev/docs/svelte/svelte-reactivity#MediaQuery) that matches if the user [prefers reduced motion](https://developer.mozilla.org/en-US/docs/Web/CSS/@media/prefers-reduced-motion).
 *
 * ```svelte
 * <script>
 * 	import { prefersReducedMotion } from 'svelte/motion';
 * 	import { fly } from 'svelte/transition';
 *
 * 	let visible = $state(false);
 * </script>
 *
 * <button onclick={() => visible = !visible}>
 * 	toggle
 * </button>
 *
 * {#if visible}
 * 	<p transition:fly={{ y: prefersReducedMotion.current ? 0 : 200 }}>
 * 		flies in, unless the user prefers reduced motion
 * 	</p>
 * {/if}
 * ```
 * @type {MediaQuery}
 * @since 5.7.0
 */
const prefersReducedMotion = /*@__PURE__*/ new MediaQuery(
	'(prefers-reduced-motion: reduce)'
);

var c = /*#__PURE__*/Object.freeze({
	__proto__: null,
	Spring: Spring,
	Tween: Tween,
	prefersReducedMotion: prefersReducedMotion,
	spring: spring,
	tweened: tweened
});

var w = /*#__PURE__*/Object.freeze({
	__proto__: null,
	on: on
});

/**
 * @template T
 */
class ReactiveValue {
	#fn;
	#subscribe;

	/**
	 *
	 * @param {() => T} fn
	 * @param {(update: () => void) => void} onsubscribe
	 */
	constructor(fn, onsubscribe) {
		this.#fn = fn;
		this.#subscribe = createSubscriber$1(onsubscribe);
	}

	get current() {
		this.#subscribe();
		return this.#fn();
	}
}

/**
 * `scrollX.current` is a reactive view of `window.scrollX`. On the server it is `undefined`.
 * @since 5.11.0
 */
const scrollX = new ReactiveValue(
	() => undefined,
	(update) => on(window, 'scroll', update)
);

/**
 * `scrollY.current` is a reactive view of `window.scrollY`. On the server it is `undefined`.
 * @since 5.11.0
 */
const scrollY = new ReactiveValue(
	() => undefined,
	(update) => on(window, 'scroll', update)
);

/**
 * `innerWidth.current` is a reactive view of `window.innerWidth`. On the server it is `undefined`.
 * @since 5.11.0
 */
const innerWidth = new ReactiveValue(
	() => undefined,
	(update) => on(window, 'resize', update)
);

/**
 * `innerHeight.current` is a reactive view of `window.innerHeight`. On the server it is `undefined`.
 * @since 5.11.0
 */
const innerHeight = new ReactiveValue(
	() => undefined,
	(update) => on(window, 'resize', update)
);

/**
 * `outerWidth.current` is a reactive view of `window.outerWidth`. On the server it is `undefined`.
 * @since 5.11.0
 */
const outerWidth = new ReactiveValue(
	() => undefined,
	(update) => on(window, 'resize', update)
);

/**
 * `outerHeight.current` is a reactive view of `window.outerHeight`. On the server it is `undefined`.
 * @since 5.11.0
 */
const outerHeight = new ReactiveValue(
	() => undefined,
	(update) => on(window, 'resize', update)
);

/**
 * `screenLeft.current` is a reactive view of `window.screenLeft`. It is updated inside a `requestAnimationFrame` callback. On the server it is `undefined`.
 * @since 5.11.0
 */
const screenLeft = new ReactiveValue(
	() => undefined,
	(update) => {
		let value = window.screenLeft;

		let frame = requestAnimationFrame(function check() {
			frame = requestAnimationFrame(check);

			if (value !== (value = window.screenLeft)) {
				update();
			}
		});

		return () => {
			cancelAnimationFrame(frame);
		};
	}
);

/**
 * `screenTop.current` is a reactive view of `window.screenTop`. It is updated inside a `requestAnimationFrame` callback. On the server it is `undefined`.
 * @since 5.11.0
 */
const screenTop = new ReactiveValue(
	() => undefined,
	(update) => {
		let value = window.screenTop;

		let frame = requestAnimationFrame(function check() {
			frame = requestAnimationFrame(check);

			if (value !== (value = window.screenTop)) {
				update();
			}
		});

		return () => {
			cancelAnimationFrame(frame);
		};
	}
);

/**
 * `online.current` is a reactive view of `navigator.onLine`. On the server it is `undefined`.
 * @since 5.11.0
 */
const online = new ReactiveValue(
	() => undefined,
	(update) => {
		const unsub_online = on(window, 'online', update);
		const unsub_offline = on(window, 'offline', update);
		return () => {
			unsub_online();
			unsub_offline();
		};
	}
);

/**
 * `devicePixelRatio.current` is a reactive view of `window.devicePixelRatio`. On the server it is `undefined`.
 * Note that behaviour differs between browsers — on Chrome it will respond to the current zoom level,
 * on Firefox and Safari it won't.
 * @type {{ get current(): number | undefined }}
 * @since 5.11.0
 */
const devicePixelRatio = /* @__PURE__ */ new (class DevicePixelRatio {
	#dpr = source(undefined);

	#update() {
		const off = on(
			window.matchMedia(`(resolution: ${window.devicePixelRatio}dppx)`),
			'change',
			() => {
				set(this.#dpr, window.devicePixelRatio);

				off();
				this.#update();
			}
		);
	}

	constructor() {
	}

	get current() {
		get(this.#dpr);
		return undefined;
	}
})();

var y = /*#__PURE__*/Object.freeze({
	__proto__: null,
	devicePixelRatio: devicePixelRatio,
	innerHeight: innerHeight,
	innerWidth: innerWidth,
	online: online,
	outerHeight: outerHeight,
	outerWidth: outerWidth,
	screenLeft: screenLeft,
	screenTop: screenTop,
	scrollX: scrollX,
	scrollY: scrollY
});

var g = /*#__PURE__*/Object.freeze({
	__proto__: null,
	render: render
});

/** @import { BlurParams, CrossfadeParams, DrawParams, FadeParams, FlyParams, ScaleParams, SlideParams, TransitionConfig } from './public' */


/** @param {number} x */
const linear = (x) => x;

/** @param {number} t */
function cubic_out(t) {
	const f = t - 1.0;
	return f * f * f + 1.0;
}

/**
 * @param {number} t
 * @returns {number}
 */
function cubic_in_out(t) {
	return t < 0.5 ? 4.0 * t * t * t : 0.5 * Math.pow(2.0 * t - 2.0, 3.0) + 1.0;
}

/** @param {number | string} value
 * @returns {[number, string]}
 */
function split_css_unit(value) {
	const split = typeof value === 'string' && value.match(/^\s*(-?[\d.]+)([^\s]*)\s*$/);
	return split ? [parseFloat(split[1]), split[2] || 'px'] : [/** @type {number} */ (value), 'px'];
}

/**
 * Animates a `blur` filter alongside an element's opacity.
 *
 * @param {Element} node
 * @param {BlurParams} [params]
 * @returns {TransitionConfig}
 */
function blur(
	node,
	{ delay = 0, duration = 400, easing = cubic_in_out, amount = 5, opacity = 0 } = {}
) {
	const style = getComputedStyle(node);
	const target_opacity = +style.opacity;
	const f = style.filter === 'none' ? '' : style.filter;
	const od = target_opacity * (1 - opacity);
	const [value, unit] = split_css_unit(amount);
	return {
		delay,
		duration,
		easing,
		css: (_t, u) => `opacity: ${target_opacity - od * u}; filter: ${f} blur(${u * value}${unit});`
	};
}

/**
 * Animates the opacity of an element from 0 to the current opacity for `in` transitions and from the current opacity to 0 for `out` transitions.
 *
 * @param {Element} node
 * @param {FadeParams} [params]
 * @returns {TransitionConfig}
 */
function fade(node, { delay = 0, duration = 400, easing = linear } = {}) {
	const o = +getComputedStyle(node).opacity;
	return {
		delay,
		duration,
		easing,
		css: (t) => `opacity: ${t * o}`
	};
}

/**
 * Animates the x and y positions and the opacity of an element. `in` transitions animate from the provided values, passed as parameters to the element's default values. `out` transitions animate from the element's default values to the provided values.
 *
 * @param {Element} node
 * @param {FlyParams} [params]
 * @returns {TransitionConfig}
 */
function fly(
	node,
	{ delay = 0, duration = 400, easing = cubic_out, x = 0, y = 0, opacity = 0 } = {}
) {
	const style = getComputedStyle(node);
	const target_opacity = +style.opacity;
	const transform = style.transform === 'none' ? '' : style.transform;
	const od = target_opacity * (1 - opacity);
	const [x_value, x_unit] = split_css_unit(x);
	const [y_value, y_unit] = split_css_unit(y);
	return {
		delay,
		duration,
		easing,
		css: (t, u) => `
			transform: ${transform} translate(${(1 - t) * x_value}${x_unit}, ${(1 - t) * y_value}${y_unit});
			opacity: ${target_opacity - od * u}`
	};
}

/**
 * Slides an element in and out.
 *
 * @param {Element} node
 * @param {SlideParams} [params]
 * @returns {TransitionConfig}
 */
function slide(node, { delay = 0, duration = 400, easing = cubic_out, axis = 'y' } = {}) {
	const style = getComputedStyle(node);

	const opacity = +style.opacity;
	const primary_property = axis === 'y' ? 'height' : 'width';
	const primary_property_value = parseFloat(style[primary_property]);
	const secondary_properties = axis === 'y' ? ['top', 'bottom'] : ['left', 'right'];
	const capitalized_secondary_properties = secondary_properties.map(
		(e) => /** @type {'Left' | 'Right' | 'Top' | 'Bottom'} */ (`${e[0].toUpperCase()}${e.slice(1)}`)
	);
	const padding_start_value = parseFloat(style[`padding${capitalized_secondary_properties[0]}`]);
	const padding_end_value = parseFloat(style[`padding${capitalized_secondary_properties[1]}`]);
	const margin_start_value = parseFloat(style[`margin${capitalized_secondary_properties[0]}`]);
	const margin_end_value = parseFloat(style[`margin${capitalized_secondary_properties[1]}`]);
	const border_width_start_value = parseFloat(
		style[`border${capitalized_secondary_properties[0]}Width`]
	);
	const border_width_end_value = parseFloat(
		style[`border${capitalized_secondary_properties[1]}Width`]
	);
	return {
		delay,
		duration,
		easing,
		css: (t) =>
			'overflow: hidden;' +
			`opacity: ${Math.min(t * 20, 1) * opacity};` +
			`${primary_property}: ${t * primary_property_value}px;` +
			`padding-${secondary_properties[0]}: ${t * padding_start_value}px;` +
			`padding-${secondary_properties[1]}: ${t * padding_end_value}px;` +
			`margin-${secondary_properties[0]}: ${t * margin_start_value}px;` +
			`margin-${secondary_properties[1]}: ${t * margin_end_value}px;` +
			`border-${secondary_properties[0]}-width: ${t * border_width_start_value}px;` +
			`border-${secondary_properties[1]}-width: ${t * border_width_end_value}px;` +
			`min-${primary_property}: 0`
	};
}

/**
 * Animates the opacity and scale of an element. `in` transitions animate from the provided values, passed as parameters, to an element's current (default) values. `out` transitions animate from an element's default values to the provided values.
 *
 * @param {Element} node
 * @param {ScaleParams} [params]
 * @returns {TransitionConfig}
 */
function scale(
	node,
	{ delay = 0, duration = 400, easing = cubic_out, start = 0, opacity = 0 } = {}
) {
	const style = getComputedStyle(node);
	const target_opacity = +style.opacity;
	const transform = style.transform === 'none' ? '' : style.transform;
	const sd = 1 - start;
	const od = target_opacity * (1 - opacity);
	return {
		delay,
		duration,
		easing,
		css: (_t, u) => `
			transform: ${transform} scale(${1 - sd * u});
			opacity: ${target_opacity - od * u}
		`
	};
}

/**
 * Animates the stroke of an SVG element, like a snake in a tube. `in` transitions begin with the path invisible and draw the path to the screen over time. `out` transitions start in a visible state and gradually erase the path. `draw` only works with elements that have a `getTotalLength` method, like `<path>` and `<polyline>`.
 *
 * @param {SVGElement & { getTotalLength(): number }} node
 * @param {DrawParams} [params]
 * @returns {TransitionConfig}
 */
function draw(node, { delay = 0, speed, duration, easing = cubic_in_out } = {}) {
	let len = node.getTotalLength();
	const style = getComputedStyle(node);
	if (style.strokeLinecap !== 'butt') {
		len += parseInt(style.strokeWidth);
	}
	if (duration === undefined) {
		if (speed === undefined) {
			duration = 800;
		} else {
			duration = len / speed;
		}
	} else if (typeof duration === 'function') {
		duration = duration(len);
	}
	return {
		delay,
		duration,
		easing,
		css: (_, u) => `
			stroke-dasharray: ${len};
			stroke-dashoffset: ${u * len};
		`
	};
}

/**
 * @template T
 * @template S
 * @param {T} tar
 * @param {S} src
 * @returns {T & S}
 */
function assign(tar, src) {
	// @ts-ignore
	for (const k in src) tar[k] = src[k];
	return /** @type {T & S} */ (tar);
}

/**
 * The `crossfade` function creates a pair of [transitions](https://svelte.dev/docs/svelte/transition) called `send` and `receive`. When an element is 'sent', it looks for a corresponding element being 'received', and generates a transition that transforms the element to its counterpart's position and fades it out. When an element is 'received', the reverse happens. If there is no counterpart, the `fallback` transition is used.
 *
 * @param {CrossfadeParams & {
 * 	fallback?: (node: Element, params: CrossfadeParams, intro: boolean) => TransitionConfig;
 * }} params
 * @returns {[(node: any, params: CrossfadeParams & { key: any; }) => () => TransitionConfig, (node: any, params: CrossfadeParams & { key: any; }) => () => TransitionConfig]}
 */
function crossfade({ fallback, ...defaults }) {
	/** @type {Map<any, Element>} */
	const to_receive = new Map();
	/** @type {Map<any, Element>} */
	const to_send = new Map();

	/**
	 * @param {Element} from_node
	 * @param {Element} node
	 * @param {CrossfadeParams} params
	 * @returns {TransitionConfig}
	 */
	function crossfade(from_node, node, params) {
		const {
			delay = 0,
			duration = /** @param {number} d */ (d) => Math.sqrt(d) * 30,
			easing = cubic_out
		} = assign(assign({}, defaults), params);
		const from = from_node.getBoundingClientRect();
		const to = node.getBoundingClientRect();
		const dx = from.left - to.left;
		const dy = from.top - to.top;
		const dw = from.width / to.width;
		const dh = from.height / to.height;
		const d = Math.sqrt(dx * dx + dy * dy);
		const style = getComputedStyle(node);
		const transform = style.transform === 'none' ? '' : style.transform;
		const opacity = +style.opacity;
		return {
			delay,
			duration: typeof duration === 'function' ? duration(d) : duration,
			easing,
			css: (t, u) => `
			   opacity: ${t * opacity};
			   transform-origin: top left;
			   transform: ${transform} translate(${u * dx}px,${u * dy}px) scale(${t + (1 - t) * dw}, ${
						t + (1 - t) * dh
					});
		   `
		};
	}

	/**
	 * @param {Map<any, Element>} items
	 * @param {Map<any, Element>} counterparts
	 * @param {boolean} intro
	 * @returns {(node: any, params: CrossfadeParams & { key: any; }) => () => TransitionConfig}
	 */
	function transition(items, counterparts, intro) {
		// @ts-expect-error TODO improve typings (are the public types wrong?)
		return (node, params) => {
			items.set(params.key, node);
			return () => {
				if (counterparts.has(params.key)) {
					const other_node = counterparts.get(params.key);
					counterparts.delete(params.key);
					return crossfade(/** @type {Element} */ (other_node), node, params);
				}
				// if the node is disappearing altogether
				// (i.e. wasn't claimed by the other list)
				// then we need to supply an outro
				items.delete(params.key);
				return fallback && fallback(node, params, intro);
			};
		};
	}
	return [transition(to_send, to_receive, false), transition(to_receive, to_send, true)];
}

var k = /*#__PURE__*/Object.freeze({
	__proto__: null,
	blur: blur,
	crossfade: crossfade,
	draw: draw,
	fade: fade,
	fly: fly,
	scale: scale,
	slide: slide
});

const d=typeof window<"u";if(d){const t={};t.svelte={};for(const e in o)t.svelte[e]=o[e];t.svelte_animate={};for(const e in i)t.svelte_animate[e]=i[e];t.svelte_attachments={};for(const e in l)t.svelte_attachments[e]=l[e];t.svelte_easing={};for(const e in r)t.svelte_easing[e]=r[e];t.svelte_internal_client={};for(const e in _)t.svelte_internal_client[e]=_[e];t.svelte_internal_flags_async={};for(const e in n)t.svelte_internal_flags_async[e]=n[e];t.svelte_internal_flags_legacy={};for(const e in a)t.svelte_internal_flags_legacy[e]=a[e];t.svelte_internal_flags_tracing={};for(const e in v)t.svelte_internal_flags_tracing[e]=v[e];t.svelte_internal_server={};for(const e in s)t.svelte_internal_server[e]=s[e];t.svelte_legacy={};for(const e in f)t.svelte_legacy[e]=f[e];t.svelte_motion={};for(const e in c)t.svelte_motion[e]=c[e];t.svelte_reactivity={};for(const e in m)t.svelte_reactivity[e]=m[e];t.svelte_reactivity_window={};for(const e in y)t.svelte_reactivity_window[e]=y[e];t.svelte_server={};for(const e in g)t.svelte_server[e]=g[e];t.svelte_store={};for(const e in p)t.svelte_store[e]=p[e];t.svelte_transition={};for(const e in k)t.svelte_transition[e]=k[e];t.svelte_events={};for(const e in w)t.svelte_events[e]=w[e];window.__gradio__svelte__=t,window.__gradio__svelte__.globals={},window.globals=window;}function u(t,e){t.push("<!--[-->"),slot$1(t,e,"default",{},null),t.push("<!--]-->");}

export { u as default };
//# sourceMappingURL=_layout.svelte-CzgVMpwS.js.map
