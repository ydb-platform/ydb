import { i as is_array, e as get_prototype_of, o as object_prototype } from './async-DWBXLqlH.js';

/** @import { Snapshot } from './types' */

/**
 * In dev, we keep track of which properties could not be cloned. In prod
 * we don't bother, but we keep a dummy array around so that the
 * signature stays the same
 * @type {string[]}
 */
const empty = [];

/**
 * @template T
 * @param {T} value
 * @param {boolean} [skip_warning]
 * @param {boolean} [no_tojson]
 * @returns {Snapshot<T>}
 */
function snapshot(value, skip_warning = false, no_tojson = false) {

	return clone(value, new Map(), '', empty, null, no_tojson);
}

/**
 * @template T
 * @param {T} value
 * @param {Map<T, Snapshot<T>>} cloned
 * @param {string} path
 * @param {string[]} paths
 * @param {null | T} [original] The original value, if `value` was produced from a `toJSON` call
 * @param {boolean} [no_tojson]
 * @returns {Snapshot<T>}
 */
function clone(value, cloned, path, paths, original = null, no_tojson = false) {
	if (typeof value === 'object' && value !== null) {
		var unwrapped = cloned.get(value);
		if (unwrapped !== undefined) return unwrapped;

		if (value instanceof Map) return /** @type {Snapshot<T>} */ (new Map(value));
		if (value instanceof Set) return /** @type {Snapshot<T>} */ (new Set(value));

		if (is_array(value)) {
			var copy = /** @type {Snapshot<any>} */ (Array(value.length));
			cloned.set(value, copy);

			if (original !== null) {
				cloned.set(original, copy);
			}

			for (var i = 0; i < value.length; i += 1) {
				var element = value[i];
				if (i in value) {
					copy[i] = clone(element, cloned, path, paths, null, no_tojson);
				}
			}

			return copy;
		}

		if (get_prototype_of(value) === object_prototype) {
			/** @type {Snapshot<any>} */
			copy = {};
			cloned.set(value, copy);

			if (original !== null) {
				cloned.set(original, copy);
			}

			for (var key in value) {
				copy[key] = clone(
					// @ts-expect-error
					value[key],
					cloned,
					path,
					paths,
					null,
					no_tojson
				);
			}

			return copy;
		}

		if (value instanceof Date) {
			return /** @type {Snapshot<T>} */ (structuredClone(value));
		}

		if (typeof (/** @type {T & { toJSON?: any } } */ (value).toJSON) === 'function' && !no_tojson) {
			return clone(
				/** @type {T & { toJSON(): any } } */ (value).toJSON(),
				cloned,
				path,
				paths,
				// Associate the instance with the toJSON clone
				value
			);
		}
	}

	if (value instanceof EventTarget) {
		// can't be cloned
		return /** @type {Snapshot<T>} */ (value);
	}

	try {
		return /** @type {Snapshot<T>} */ (structuredClone(value));
	} catch (e) {

		return /** @type {Snapshot<T>} */ (value);
	}
}

export { snapshot as s };
//# sourceMappingURL=clone-CubQhOZi.js.map
