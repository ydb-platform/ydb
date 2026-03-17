import { r as redirect } from './index-wpIsICWW.js';
import { t } from './index5-BoOEKc6P.js';
import './dev-fallback-Bc5Ork7Y.js';

function i({url:t$1}){const{pathname:e,search:a}=t$1;t&&t$1.pathname.startsWith("/theme")&&redirect(308,`http://127.0.0.1:7860${e}${a}`);}

var _layout_server_ts = /*#__PURE__*/Object.freeze({
	__proto__: null,
	load: i
});

const index = 0;
let component_cache;
const component = async () => component_cache ??= (await import('./_layout.svelte-CzgVMpwS.js')).default;
const server_id = "src/routes/+layout.server.ts";
const imports = ["_app/immutable/nodes/0.v2V8uM7n.js"];
const stylesheets = ["_app/immutable/assets/0.CV57u9zR.css"];
const fonts = [];

export { component, fonts, imports, index, _layout_server_ts as server, server_id, stylesheets };
//# sourceMappingURL=0-C8WrYb3l.js.map
