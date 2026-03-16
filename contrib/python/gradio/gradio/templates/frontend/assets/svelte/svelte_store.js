import { b as set_active_reaction, d as active_reaction, e as set_active_effect, f as active_effect, h as effect_root, r as render_effect, i as effect_tracking, c as createSubscriber } from './index-B0zdmtA4.js';
import { w as writable, g as get } from './index-utLXTo0e.js';
export { d as derived, r as readable, a as readonly } from './index-utLXTo0e.js';
import './index-6-3S3c88.js';
import './constants-BTL-klwN.js';
import './utils-CeSFTyv7.js';
import './custom-element-N2TeiOiC.js';
import './legacy-client-zNO_yntr.js';
import './utils-Bmu7F6be.js';

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
	var effect = active_effect;
	var reaction = active_reaction;
	var init_value = get();

	const store = writable(init_value, (set) => {
		// If the value has changed before we call subscribe, then
		// we need to treat the value as already having run
		var ran = init_value !== get();

		// TODO do we need a different implementation on the server?
		var teardown;
		// Apply the reaction and effect at the time of toStore being called
		var previous_reaction = active_reaction;
		var previous_effect = active_effect;
		set_active_reaction(reaction);
		set_active_effect(effect);

		try {
			teardown = effect_root(() => {
				render_effect(() => {
					const value = get();
					if (ran) set(value);
				});
			});
		} finally {
			set_active_reaction(previous_reaction);
			set_active_effect(previous_effect);
		}

		ran = true;

		return teardown;
	});

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
	let value = /** @type {V} */ (undefined);

	const subscribe = createSubscriber((update) => {
		let ran = false;

		const unsubscribe = store.subscribe((v) => {
			value = v;
			if (ran) update();
		});

		ran = true;

		return unsubscribe;
	});

	function current() {
		if (effect_tracking()) {
			subscribe();
			return value;
		}

		return get(store);
	}

	if ('set' in store) {
		return {
			get current() {
				return current();
			},
			set current(v) {
				store.set(v);
			}
		};
	}

	return {
		get current() {
			return current();
		}
	};
}

export { fromStore, get, toStore, writable };
