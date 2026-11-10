#!/usr/bin/env python3
"""Symbolize and compare snapshots from TVectorIndexUploadMemory (LLVM 18+)."""
import argparse
import collections
import json
import pathlib
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('binary', type=pathlib.Path)
parser.add_argument('profile_dir', type=pathlib.Path)
parser.add_argument('output_dir', type=pathlib.Path)
parser.add_argument('--prefix', default='', help='Use centroids- for the centroid allocation reproducer')
args = parser.parse_args()
binary = args.binary.resolve()
args.output_dir.mkdir(parents=True, exist_ok=True)
profiles = {}
metrics = {}
has_heap = {}
for stage in ('baseline', 'blocked', 'finished'):
    entries = collections.Counter()
    heap_path = args.profile_dir / f'{args.prefix}{stage}.heap.tsv'
    has_heap[stage] = heap_path.exists()
    if has_heap[stage]:
        for line in heap_path.read_text().splitlines():
            weight, count, *stack = line.split('\t')
            entries[tuple(stack)] += int(weight)
    profiles[stage] = entries
    stats = args.profile_dir / f'{args.prefix}{stage}.stats.tsv'
    if not has_heap[stage] and not stats.exists():
        raise FileNotFoundError(f'No profile or metrics for {stage} in {args.profile_dir}')
    if stats.exists():
        metrics[stage] = {key: int(value) for key, value in
                          (line.split('\t') for line in stats.read_text().splitlines())}

# Return PCs need adjustment for PIE executables and for source-line lookup.
with binary.open('rb') as executable:
    header = executable.read(18)
base = 0
if any(has_heap.values()) and int.from_bytes(header[16:18], 'little') == 3:
    maps = (args.profile_dir / f'{args.prefix}baseline.maps').read_text()
    for line in maps.splitlines():
        columns = line.split(maxsplit=5)
        if len(columns) == 6 and pathlib.Path(columns[5]).name == binary.name:
            base = int(columns[0].split('-')[0], 16) - int(columns[2], 16)
            break
    else:
        raise RuntimeError('Cannot find the PIE executable in the recorded memory maps')
pcs = sorted({pc for entries in profiles.values() for stack in entries for pc in stack})
if any(has_heap.values()) and not pcs:
    raise RuntimeError('Empty heap profiles: run with TCMalloc and vector-upload-profile=true')
cache_path = args.output_dir / 'symbols.json'
identity = [str(binary), binary.stat().st_size, binary.stat().st_mtime_ns, base]
cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
symbols = cache.get('symbols', {}) if cache.get('binary') == identity else {}
missing = [pc for pc in pcs if pc not in symbols]
if missing:
    result = subprocess.run(
        ['llvm-symbolizer', '--no-debuginfod', '--output-style=JSON', '--inlines', '--demangle', '--obj=' + str(binary)],
        input='\n'.join(hex(int(pc, 16) - base - 1) for pc in missing) + '\n',
        text=True, capture_output=True, check=True)
    decoded = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    if len(decoded) != len(missing):
        raise RuntimeError(f'Unexpected symbolizer output: {result.stderr}')
    symbols.update(zip(missing, decoded))
    cache_path.write_text(json.dumps({'binary': identity, 'symbols': symbols}, indent=2))


def frames(stack):
    return [f"{frame.get('FunctionName', '??')} ({frame.get('FileName', '')}:{frame.get('Line', 0)})"
            for pc in stack for frame in symbols[pc].get('Symbol', [])]


def category(stack):
    names = '\n'.join(frames(stack))
    if 'MakeShardRequests' in names:
        return 'upload request and retry state'
    if 'TBufferData::AddRow' in names:
        return 'scan output buffers'
    if 'SetClusters' in names and 'TClusters<' in names:
        return 'centroid state'
    if 'TSerializedCellVec::Serialize' in names and 'Feed' in names:
        return 'scan output serialization'
    if 'NTabletFlatExecutor' in names or 'NTable::' in names or 'NPageCollection' in names:
        return 'tablet storage/cache'
    return 'other'


lines = []
for stage, entries in profiles.items():
    lines.append(f'{stage}:')
    if has_heap[stage]:
        lines.append(f'  sampled live heap: {sum(entries.values()) / 2**20:.2f} MiB')
    if stage in metrics:
        lines.extend(f'  {name}: {size / 2**20:.2f} MiB ({size} bytes)'
                     for name, size in metrics[stage].items())
    by_category = collections.Counter()
    for stack, size in entries.items():
        by_category[category(stack)] += size
    lines.extend(f'  {name}: {size / 2**20:.2f} MiB' for name, size in by_category.most_common())
    if has_heap[stage]:
        (args.output_dir / f'{args.prefix}{stage}.symbolized.txt').write_text('\n\n'.join(
            f'{size} bytes\n' + '\n'.join(frames(stack)) for stack, size in entries.most_common()))
for stage in ('blocked', 'finished'):
    delta = profiles[stage].copy()
    delta.subtract(profiles['baseline'])
    lines.append(f'\n{stage} - baseline:')
    if has_heap[stage] and has_heap['baseline']:
        lines.append(f'  sampled live heap: {sum(delta.values()) / 2**20:+.2f} MiB')
    if stage in metrics and 'baseline' in metrics:
        lines.extend(f'  {name}: {(size - metrics["baseline"][name]) / 2**20:+.2f} MiB'
                     for name, size in metrics[stage].items())
    by_category = collections.Counter()
    for stack, size in delta.items():
        by_category[category(stack)] += size
    lines.extend(f'  {name}: {size / 2**20:+.2f} MiB' for name, size in by_category.most_common())
    if has_heap[stage]:
        lines.append('Largest growing stacks:')
    for stack, size in [(stack, size) for stack, size in delta.most_common() if size > 0][:12]:
        trace = frames(stack)
        relevant = [frame for frame in trace if any(name in frame for name in
                    ('TBufferData', 'Serialize', 'MakeShardRequests', 'TClusters<', 'SetClusters',
                     'TReshuffle', 'NTable::', 'NTabletFlatExecutor'))]
        lines.append(f'  {size / 2**20:+.2f} MiB\n    ' + '\n    '.join((relevant or trace)[:12]))
report = '\n'.join(lines) + '\n'
(args.output_dir / 'analysis.txt').write_text(report)
print(report)
