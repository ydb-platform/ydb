"""Compose version-local YQL sources and YDB fragments in a disposable docs tree."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil


def child(root, relative):
    path = PurePosixPath(relative)
    if path.is_absolute() or '..' in path.parts:
        raise ValueError(f'Unsafe relative path: {relative}')
    result = root.joinpath(*path.parts).resolve()
    if not result.is_relative_to(root.resolve()):
        raise ValueError(f'Path escapes {root}: {relative}')
    return result


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(repo_root, output, preview=False):
    repo_root, output = repo_root.resolve(), output.resolve()
    if output == repo_root or output.is_relative_to(repo_root) or repo_root.is_relative_to(output):
        raise ValueError('Output must be outside the checkout')
    if output.exists():
        raise FileExistsError(output)
    docs = repo_root / 'ydb/docs'
    config = docs / 'tools/yql_overlay'
    manifest = json.loads((config / 'manifest.json').read_text())
    if manifest['version'] != 1:
        raise ValueError('Unsupported YQL overlay manifest version')
    if not (config / 'enabled').exists() and not preview:
        raise ValueError('YQL overlay has not been enabled for this revision')
    copies, destinations = [], set()
    for page in manifest['pages']:
        lang = page['lang']
        if lang not in ('ru', 'en'):
            raise ValueError(f'Unsupported locale: {lang}')
        target_root = docs / lang / 'core/yql/reference'
        target = child(target_root, page['target'])
        if target in destinations:
            raise ValueError(f'Duplicate target: {target}')
        destinations.add(target)
        if not target.is_file():
            raise ValueError(f'Missing YDB page: {target}')
        if page['mode'] == 'keep':
            source = child(repo_root / 'yql/essentials/docs' / lang, page['source'])
            if sha256(source) != page['upstream_sha256']:
                raise ValueError(f'Upstream changed behind a full-page override; review required: {source}')
            continue
        if page['mode'] not in ('import', 'include'):
            raise ValueError(f'Unsupported mode: {page["mode"]}')
        if sha256(target) != page['target_sha256']:
            raise ValueError(f'YDB page changed since overlay extraction: {target}')
        source = child(repo_root / 'yql/essentials/docs' / lang, page['source'])
        content = source.read_text()
        actual = re.findall(r'{% include \[YDB\]\((_ydb/[^)]+)\) %}', content)
        required = [str(PurePosixPath('_ydb') / PurePosixPath(p).name)
                    for p in page.get('fragments', [])]
        if sorted(actual) != sorted(required):
            raise ValueError(f'Incompatible upstream extension points: {source}')
        if required and 'if YDB' not in content:
            raise ValueError(f'Missing consumer condition: {source}')
        copies.append((source, target.relative_to(docs)))
        for relative in page.get('fragments', []):
            fragment = child(config / 'overlays' / lang, relative)
            if not fragment.is_file():
                raise ValueError(f'Missing YDB fragment: {fragment}')
            destination = child(target_root, relative)
            if destination.exists() or destination in destinations:
                raise ValueError(f'Fragment would overwrite an existing file: {destination}')
            destinations.add(destination)
            copies.append((fragment, destination.relative_to(docs)))
    presets = (docs / 'presets.yaml').read_text()
    if presets.count('default:\n') != 1 or re.search(r'^\s*YDB:', presets, re.M):
        raise ValueError('Unexpected root presets structure or existing overlay variable')
    shutil.copytree(docs, output, ignore=shutil.ignore_patterns('__pycache__', '.DS_Store'))
    for source, relative in copies:
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    (output / 'presets.yaml').write_text(presets.replace('default:\n', 'default:\n  YDB: true\n', 1))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--preview', action='store_true', help='Validate an explicitly disabled migration locally')
    args = parser.parse_args()
    prepare(args.repo_root, args.output, args.preview)
    print(f'Prepared version-local YQL documentation in {args.output}')
