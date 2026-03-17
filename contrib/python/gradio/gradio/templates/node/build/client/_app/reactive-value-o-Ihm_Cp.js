import { c as createSubscriber } from './index-B0zdmtA4.js';

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
		this.#subscribe = createSubscriber(onsubscribe);
	}

	get current() {
		this.#subscribe();
		return this.#fn();
	}
}

export { ReactiveValue as R };
