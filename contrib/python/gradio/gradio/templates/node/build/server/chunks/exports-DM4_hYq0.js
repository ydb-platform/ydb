import { s as s$1, e as e$1 } from './dev-fallback-Bc5Ork7Y.js';

// eslint-disable-next-line n/prefer-global/process
const IN_WEBCONTAINER = !!globalThis.process?.versions?.webcontainer;

/** @import { RequestEvent } from '@sveltejs/kit' */
/** @import { RequestStore } from 'types' */
/** @import { AsyncLocalStorage } from 'node:async_hooks' */


/** @type {RequestStore | null} */
let sync_store = null;

/** @type {AsyncLocalStorage<RequestStore | null> | null} */
let als;

import('node:async_hooks')
	.then((hooks) => (als = new hooks.AsyncLocalStorage()))
	.catch(() => {
		// can't use AsyncLocalStorage, but can still call getRequestEvent synchronously.
		// this isn't behind `supports` because it's basically just StackBlitz (i.e.
		// in-browser usage) that doesn't support it AFAICT
	});

function try_get_request_store() {
	return sync_store ?? als?.getStore() ?? null;
}

/**
 * @template T
 * @param {RequestStore | null} store
 * @param {() => T} fn
 */
function with_request_store(store, fn) {
	try {
		sync_store = store;
		return als ? als.run(store, fn) : fn();
	} finally {
		// Since AsyncLocalStorage is not working in webcontainers, we don't reset `sync_store`
		// and handle only one request at a time in `src/runtime/server/index.js`.
		if (!IN_WEBCONTAINER) {
			sync_store = null;
		}
	}
}

let e="",t=e;const i="_app",r=true,a={base:e,assets:t};function o(s){e=s.base,t=s.assets;}function c$2(){e=a.base,t=a.assets;}const f$1="1772203006354";

const s=new TextEncoder,f=new TextDecoder;function l$1(r,o){const t=r.split(/[/\\]/),e=o.split(/[/\\]/);for(t.pop();t[0]===e[0];)t.shift(),e.shift();let n=t.length;for(;n--;)t[n]="..";return t.concat(e).join("/")}function c$1(r){if(!s$1&&globalThis.Buffer)return globalThis.Buffer.from(r).toString("base64");let o="";for(let t=0;t<r.length;t++)o+=String.fromCharCode(r[t]);return btoa(o)}function h$1(r){if(!s$1&&globalThis.Buffer){const e=globalThis.Buffer.from(r,"base64");return new Uint8Array(e)}const o=atob(r),t=new Uint8Array(o.length);for(let e=0;e<o.length;e++)t[e]=o.charCodeAt(e);return t}

const x=/^[a-z][a-z\d+\-.]+:/i,h=new URL("sveltekit-internal://");function y(e,r){if(r[0]==="/"&&r[1]==="/")return r;let n=new URL(e,h);return n=new URL(r,n),n.protocol===h.protocol?n.pathname+n.search+n.hash:n.href}function E(e,r){return e==="/"||r==="ignore"?e:r==="never"?e.endsWith("/")?e.slice(0,-1):e:r==="always"&&!e.endsWith("/")?e+"/":e}function j(e){return e.split("%25").map(decodeURI).join("%25")}function O(e){for(const r in e)e[r]=decodeURIComponent(e[r]);return e}function R(e,r,n,o=false){const t=new URL(e);Object.defineProperty(t,"searchParams",{value:new Proxy(t.searchParams,{get(s,a){if(a==="get"||a==="getAll"||a==="has")return (p,...w)=>(n(p),s[a](p,...w));r();const i=Reflect.get(s,a);return typeof i=="function"?i.bind(s):i}}),enumerable:true,configurable:true});const u=["href","pathname","search","toString","toJSON"];o&&u.push("hash");for(const s of u)Object.defineProperty(t,s,{get(){return r(),e[s]},enumerable:true,configurable:true});return s$1||(t[Symbol.for("nodejs.util.inspect.custom")]=(s,a,i)=>i(e,a),t.searchParams[Symbol.for("nodejs.util.inspect.custom")]=(s,a,i)=>i(e.searchParams,a)),(e$1||!s$1)&&!o&&b(t),t}function b(e){_(e),Object.defineProperty(e,"hash",{get(){throw new Error("Cannot access event.url.hash. Consider using `page.url.hash` inside a component instead")}});}function U(e){_(e);for(const r of ["search","searchParams"])Object.defineProperty(e,r,{get(){throw new Error(`Cannot access url.${r} on a page with prerendering enabled`)}});}function _(e){s$1||(e[Symbol.for("nodejs.util.inspect.custom")]=(r,n,o)=>o(new URL(e),n));}function c(e){function r(n,o){if(n)for(const t in n){if(t[0]==="_"||e.has(t))continue;const u=[...e.values()],s=$(t,o?.slice(o.lastIndexOf(".")))??`valid exports are ${u.join(", ")}, or anything with a '_' prefix`;throw new Error(`Invalid export '${t}'${o?` in ${o}`:""} (${s})`)}}return r}function $(e,r=".js"){const n=[];if(l.has(e)&&n.push(`+layout${r}`),v.has(e)&&n.push(`+page${r}`),d.has(e)&&n.push(`+layout.server${r}`),g.has(e)&&n.push(`+page.server${r}`),m.has(e)&&n.push(`+server${r}`),n.length>0)return `'${e}' is a valid export in ${n.slice(0,-1).join(", ")}${n.length>1?" or ":""}${n.at(-1)}`}const l=new Set(["load","prerender","csr","ssr","trailingSlash","config"]),v=new Set([...l,"entries"]),d=new Set([...l]),g=new Set([...d,"actions","entries"]),m=new Set(["GET","POST","PATCH","PUT","DELETE","OPTIONS","HEAD","fallback","prerender","trailingSlash","config","entries"]),C=c(l),L=c(v),T=c(d),I=c(g),D=c(m);

export { C, D, E, I, L, O, R, T, U, f as a, try_get_request_store as b, c$1 as c, c$2 as d, e, f$1 as f, h$1 as h, i, j, l$1 as l, o, r, s, t, with_request_store as w, x, y };
//# sourceMappingURL=exports-DM4_hYq0.js.map
