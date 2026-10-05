import {build} from 'esbuild';
import {readFile, writeFile, readdir} from 'node:fs/promises';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';
const result = await build({
    entryPoints: [fileURLToPath(new URL('./entry.js', import.meta.url))],
    outfile: fileURLToPath(new URL('../chartkit.js', import.meta.url)),
    metafile: true, bundle: true, format: 'esm', target: 'es2020', minify: true,
    legalComments: 'external',
    define: {'process.env.NODE_ENV': '"production"'},
    // ChartKit 7.1 exports this type-only module without its .js extension.
    alias: {'@gravity-ui/yagr/dist/types': '@gravity-ui/yagr/dist/types.js'},
    loader: {'.svg': 'dataurl'},
});

// Include full license notices for the packages actually included in the bundle.
const packages = new Map();
for (const input of Object.keys(result.metafile.inputs)) {
    if (!input.includes('node_modules/')) continue;
    let directory = dirname(input);
    while (directory.includes('node_modules')) {
        try {
            const pkg = JSON.parse(await readFile(join(directory, 'package.json'), 'utf8'));
            if (!pkg.name) {directory = dirname(directory); continue;}
            packages.set(pkg.name, {directory, pkg});
            break;
        } catch {directory = dirname(directory);}
    }
}
const notices = [];
for (const [name, {directory, pkg}] of [...packages].sort(([a], [b]) => a.localeCompare(b))) {
    notices.push(`${name} ${pkg.version} (${pkg.license || 'see notice'})`);
    for (const file of (await readdir(directory)).filter(file => /^(license|licence|copying|notice)(\.|$)/i.test(file)).sort()) {
        notices.push(await readFile(join(directory, file), 'utf8'));
    }
}
await writeFile(new URL('./THIRD_PARTY_LICENSES.txt', import.meta.url), notices.join('\n\n') + '\n');
