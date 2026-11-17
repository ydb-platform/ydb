#!/usr/bin/env python3
"""Synchronize a shrinking set of unmodified YQL documentation files."""
import argparse
import copy
import json
from pathlib import Path, PurePosixPath
import re
import subprocess


CONFIG = 'ydb/docs/tools/yql_sync/ownership.json'


def git(root, *args):
    result = subprocess.run(['git', '-C', str(root), *args], capture_output=True)
    if result.returncode:
        raise ValueError(result.stderr.decode().strip())
    return result.stdout


def tree(root, revision, path):
    result = {}
    for item in git(root, 'ls-tree', '-rz', revision, '--', path).split(b'\0'):
        if item:
            meta, name = item.split(b'\t', 1)
            mode, kind, oid = meta.decode().split()
            result[name.decode()] = (mode, kind, oid)
    return result


def regular(entries, path):
    item = entries.get(path)
    if item and item[:2] != ('100644', 'blob'):
        raise ValueError(f'Expected a regular documentation file: {path}')
    return item[2] if item else None


def checked_path(root, path):
    p = PurePosixPath(path)
    if p.is_absolute() or '..' in p.parts or str(p) != path or not p.parts:
        raise ValueError(f'Unsafe path: {path}')
    current = root
    for part in p.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f'Symlink not allowed: {path}')
    return current


def clean_head(root):
    if git(root, 'status', '--porcelain', '--untracked-files=all').strip():
        raise ValueError('Use a clean checkout; commit or move local changes first')
    if git(root, 'rev-parse', '--is-shallow-repository').strip() == b'true':
        raise ValueError('Full Git history is required (fetch-depth: 0)')
    return git(root, 'rev-parse', 'HEAD').decode().strip()


def write_config(root, path, state):
    dest = checked_path(root, path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)+'\n')


def initialize(root, config=CONFIG, locales=('ru', 'en')):
    """Compare current targets with the source at essentials.base_rev, not HEAD."""
    head = clean_head(root)
    if checked_path(root, config).exists():
        raise ValueError('Ownership config already exists; never reset existing ownership')
    state = {'version': 1, 'checkpoint': head, 'roots': [], 'files': {}}
    for lang in locales:
        if lang not in ('ru', 'en'):
            raise ValueError(f'Unsupported locale: {lang}')
        target = f'ydb/docs/{lang}/core/yql/reference'
        marker = checked_path(root, target+'/essentials.base_rev')
        revision, source = marker.read_text().strip().split(';')
        if source != f'yql/essentials/docs/{lang}' or not re.fullmatch('[0-9a-f]{40}', revision):
            raise ValueError(f'Invalid baseline: {marker}')
        git(root, 'merge-base', '--is-ancestor', revision, head)
        old = tree(root, revision, source)
        current = tree(root, head, target)
        state['roots'].append({'source': source, 'target': target, 'initial_revision': revision})
        names = {p[len(target)+1:] for p in current} | {p[len(source)+1:] for p in old}
        for name in sorted(names):
            if name == 'essentials.base_rev':
                continue
            src, dst = source+'/'+name, target+'/'+name
            source_blob = old.get(src)
            target_blob = current.get(dst)
            # Keep navigation, assets, local additions and deletions under YDB control.
            eligible = (name.endswith('.md') and source_blob is not None
                        and source_blob == target_blob and source_blob[:2] == ('100644', 'blob'))
            entry = {'source': src, 'owner': 'yql' if eligible else 'ydb'}
            if eligible:
                entry.update(blob=target_blob[2], revision=revision)
            else:
                entry['reason'] = ('navigation-or-asset' if not name.endswith('.md') else
                                   'target-deleted-or-renamed' if target_blob is None else
                                   'target-only' if source_blob is None else 'target-modified')
            state['files'][dst] = entry
    write_config(root, config, state)
    return state


def validate(root, state, head):
    if state.get('version') != 1 or not re.fullmatch('[0-9a-f]{40}', state.get('checkpoint', '')):
        raise ValueError('Invalid ownership state')
    git(root, 'merge-base', '--is-ancestor', state['checkpoint'], head)
    roots = state['roots']
    if not roots or len({r['target'] for r in roots}) != len(roots):
        raise ValueError('Invalid or duplicate roots')
    for r in roots:
        if (r['target'], r['source']) not in [
            (f'ydb/docs/{lang}/core/yql/reference', f'yql/essentials/docs/{lang}')
            for lang in ('ru', 'en')
        ]:
            raise ValueError('Unexpected documentation roots')
    for dst, entry in state['files'].items():
        checked_path(root, dst)
        checked_path(root, entry['source'])
        matches = [r for r in roots if dst.startswith(r['target']+'/')]
        if len(matches) != 1:
            raise ValueError(f'Target outside configured roots: {dst}')
        r = matches[0]
        if entry['source'] != r['source']+dst[len(r['target']):]:
            raise ValueError(f'Invalid source mapping: {dst}')
        if entry['owner'] not in ('ydb', 'yql'):
            raise ValueError(f'Unknown owner: {dst}')
        if entry['owner'] == 'yql':
            if not dst.endswith('.md') or not re.fullmatch('[0-9a-f]{40}', entry['blob']):
                raise ValueError(f'Invalid synchronized file: {dst}')
            if not re.fullmatch('[0-9a-f]{40}', entry['revision']):
                raise ValueError(f'Invalid source revision: {dst}')
            original = regular(tree(root, entry['revision'], entry['source']), entry['source'])
            if original != entry['blob']:
                raise ValueError(f'Baseline does not match source: {dst}')


def config_at(root, revision, config):
    # Missing config before initial adoption is expected; malformed existing state is not.
    if not tree(root, revision, config):
        return {'files': {}}
    return json.loads(git(root, 'show', revision+':'+config))


def locally_touched(root, config, state, dst, head, cache):
    commits = git(root, 'log', '--first-parent', '--format=%H',
                  state['checkpoint']+'..'+head, '--', dst).decode().splitlines()
    for commit in commits:
        if commit not in cache:
            cache[commit] = (config_at(root, commit, config), config_at(root, commit+'^', config))
        after, before = (s['files'].get(dst) for s in cache[commit])
        # A sync is recognized from its recorded source revision and actual blobs,
        # not the author or a spoofable commit-message prefix. Local edit + revert
        # still has a non-sync commit in this interval and permanently claims the file.
        if not after or after.get('owner') != 'yql' or after == before:
            return True
        actual = regular(tree(root, commit, dst), dst)
        source = regular(tree(root, after['revision'], after['source']), after['source'])
        if actual != after['blob'] or source != actual:
            return True
    return False


def synchronize(root, config=CONFIG, apply=False):
    head = clean_head(root)
    config_path = checked_path(root, config)
    state = json.loads(config_path.read_text())
    validate(root, state, head)
    result = copy.deepcopy(state)
    report = {'revision': head, 'updated': [], 'newly_owned': [], 'untracked_sources': [], 'owned': []}
    writes = {}
    cache = {}
    current = {}
    for r in state['roots']:
        current.update(tree(root, head, r['source']))
        current.update(tree(root, head, r['target']))
        for dst in sorted(p for p in current if p.startswith(r['target']+'/')):
            if dst not in result['files'] and not dst.endswith('/essentials.base_rev'):
                result['files'][dst] = {'source': r['source']+dst[len(r['target']):],
                                        'owner': 'ydb', 'reason': 'new-target-file'}
                report['newly_owned'].append(dst)
    for dst, entry in state['files'].items():
        if entry['owner'] == 'ydb':
            report['owned'].append(dst)
            continue
        actual = regular(current, dst)
        source = regular(current, entry['source'])
        reason = None
        if actual != entry['blob'] or locally_touched(root, config, state, dst, head, cache):
            reason = 'target-modified-or-deleted'
        elif source is None:
            reason = 'source-deleted-or-renamed'
        if reason:
            result['files'][dst] = {'source': entry['source'], 'owner': 'ydb', 'reason': reason}
            report['newly_owned'].append(dst)
        elif source != actual:
            writes[dst] = git(root, 'show', head+':'+entry['source'])
            result['files'][dst].update(blob=source, revision=head)
            report['updated'].append(dst)
    mapped = {entry['source'] for entry in result['files'].values()}
    report['untracked_sources'] = sorted(p for p in current
        if p.startswith('yql/essentials/docs/') and p.endswith('.md') and p not in mapped)
    # Do not create a daily metadata-only PR when nothing changed.
    if result != state:
        result['checkpoint'] = head
        if apply:
            # Validate all destinations before the first write. No reset, clean or checkout.
            paths = {dst: checked_path(root, dst) for dst in writes}
            for dst, path in paths.items():
                path.write_bytes(writes[dst])
            write_config(root, config, result)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--config', default=CONFIG)
    parser.add_argument('--initialize', action='store_true')
    parser.add_argument('--apply', action='store_true', help='Write changes; default is read-only')
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    try:
        root = Path(git(args.repo, 'rev-parse', '--show-toplevel').decode().strip())
        if args.initialize:
            if args.apply:
                raise ValueError('Initialize and apply must run separately')
            state = initialize(root, args.config)
            output = {'yql': sum(e['owner'] == 'yql' for e in state['files'].values()),
                      'ydb': sum(e['owner'] == 'ydb' for e in state['files'].values())}
        else:
            output = synchronize(root, args.config, args.apply)
        text = json.dumps(output, ensure_ascii=False, indent=2)+'\n'
        if args.report:
            args.report.write_text(text)
        print(text, end='')
    except (ValueError, KeyError, OSError) as error:
        parser.exit(1, f'YQL documentation sync failed: {error}\n')


if __name__ == '__main__':
    main()
