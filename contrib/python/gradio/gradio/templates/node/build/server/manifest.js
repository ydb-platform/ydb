const manifest = (() => {
function __memo(fn) {
	let value;
	return () => value ??= (value = fn());
}

return {
	appDir: "_app",
	appPath: "_app",
	assets: new Set([]),
	mimeTypes: {},
	_: {
		client: {start:"_app/immutable/entry/start.CbhyhRTH.js",app:"_app/immutable/entry/app.CHT7UvbJ.js",imports:["_app/immutable/entry/start.CbhyhRTH.js","_app/immutable/chunks/DLErdBzO.js","_app/immutable/entry/app.CHT7UvbJ.js","_app/immutable/chunks/PPVm8Dsz.js"],stylesheets:[],fonts:[],uses_env_dynamic_public:false},
		nodes: [
			__memo(() => import('./chunks/0-C8WrYb3l.js')),
			__memo(() => import('./chunks/1-CgY6k01x.js')),
			__memo(() => import('./chunks/2-BbOIMXxe.js').then(function (n) { return n.e; }))
		],
		remotes: {
			
		},
		routes: [
			{
				id: "/[...catchall]",
				pattern: /^(?:\/([^]*))?\/?$/,
				params: [{"name":"catchall","optional":false,"rest":true,"chained":true}],
				page: { layouts: [0,], errors: [1,], leaf: 2 },
				endpoint: null
			}
		],
		prerendered_routes: new Set([]),
		matchers: async () => {
			
			return {  };
		},
		server_assets: {}
	}
}
})();

const prerendered = new Set([]);

const base = "";

export { base, manifest, prerendered };
//# sourceMappingURL=manifest.js.map
