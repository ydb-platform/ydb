#!/usr/bin/env python3
"""Run exhaustive Spin checks in a preserved, isolated directory (Python 3.9)."""

import argparse
import hashlib
import json
import re
import shutil
import socket
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return number


def classify(log, returncode, timed_out=False, trail=False):
    """Never infer a proof from a partial search or a tool failure."""
    limits = (
        'max search depth too small',
        'depth limit reached',
        'out of memory',
        'memory exhausted',
        'search not completed',
        'interrupted',
    )
    lower = log.lower()
    if any(marker in lower for marker in ('too many processes', 'vectorsz too small')):
        return 'error'
    errors = re.search(r'errors:\s*(\d+)', log)
    violation = any(marker in lower for marker in ('assertion violated', 'invalid end state', 'acceptance cycle'))
    if trail and violation:
        return 'violated'
    if timed_out or any(marker in lower for marker in limits):
        return 'unknown'
    if returncode == 0 and errors and int(errors.group(1)) == 0:
        return 'holds'
    return 'error'


def validate_includes(model, root):
    """Support only quoted relative includes within the recorded input tree."""
    pending = [model]
    seen = set()
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        text = path.read_text()
        text = text.replace("\\\n", "")
        text = re.sub(r"/\*.*?\*/|//[^\n]*", "", text, flags=re.DOTALL)
        for directive in re.findall(r"^\s*#\s*include\s+([^\n]+)", text, re.MULTILINE):
            match = re.fullmatch(r'"([^"\n]+)"\s*', directive)
            if not match or Path(match.group(1)).is_absolute():
                raise ValueError("only quoted relative includes are supported: {}".format(path))
            include = (path.parent / match.group(1)).resolve()
            if root not in include.parents or not include.is_file():
                raise ValueError("include escapes source root or is missing: {}".format(include))
            pending.append(include)


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--model', required=True, type=Path)
    mode = result.add_mutually_exclusive_group(required=True)
    mode.add_argument('--safety', action='store_true')
    mode.add_argument('--ltl', help='exact inline LTL claim name')
    result.add_argument('--fair', action='store_true', help='weak process fairness')
    result.add_argument('--nfair', type=positive, default=3)
    result.add_argument('--depth', type=positive, default=100000)
    result.add_argument('--mem', type=positive, default=2048, help='pan MEMLIM in MB')
    result.add_argument('--timeout', type=positive, default=300, help='seconds per command')
    result.add_argument(
        '--source-root', type=Path, help='input tree containing model and relative includes; default: model directory'
    )
    result.add_argument('--output-dir', type=Path, help='new directory; must not exist')
    return result


def run(args):
    model = args.model.resolve()
    source = (args.source_root or model.parent).resolve()
    if not model.is_file() or not source.is_dir():
        raise ValueError('model file and source directory must exist')
    relative = model.relative_to(source)
    if args.fair and args.safety:
        raise ValueError('--fair requires --ltl')
    spin = shutil.which('spin')
    compiler = next((shutil.which(tool) for tool in ('cc', 'gcc', 'clang') if shutil.which(tool)), None)
    if not spin or not compiler:
        raise ValueError('Spin and a C compiler (cc/gcc/clang) are required')
    if args.output_dir:
        output = args.output_dir.absolute()
        # Refuse recursive copying when output is inside the input tree.
        if source == output or source in output.resolve().parents:
            raise ValueError('output directory must be outside source root')
        output.mkdir(parents=True, exist_ok=False)
    else:
        output = Path(tempfile.mkdtemp(prefix='spin-verify-'))
        if source in output.parents:
            output.rmdir()
            raise ValueError('temporary output is inside source root; choose --output-dir')
    work = output / 'source'
    # Symlinks could escape the snapshot and make recorded inputs misleading.
    if any(path.is_symlink() for path in source.rglob('*')):
        raise ValueError('source root must not contain symlinks')
    shutil.copytree(source, work, ignore=shutil.ignore_patterns('pan', 'pan.*', '*.trail', '.git'))
    validate_includes(work / relative, work.resolve())
    inputs = {
        str(path.relative_to(work)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(work.rglob('*'))
        if path.is_file()
    }
    cwd = work / relative.parent
    compile_flags = ['-O2', '-DMEMLIM={}'.format(args.mem)]
    if args.safety:
        compile_flags += ['-DSAFETY', '-DNOCLAIM']
    if args.fair:
        compile_flags += ['-DNFAIR={}'.format(args.nfair)]
    runtime_flags = ['-m{}'.format(args.depth)]
    if args.ltl:
        runtime_flags += ['-a', '-N', args.ltl]
    if args.fair:
        runtime_flags += ['-f']
    manifest = {
        'model': str(model),
        'source_root': str(source),
        'inputs_sha256': inputs,
        'host': socket.gethostname(),
        'started_utc': datetime.now(timezone.utc).isoformat(),
        'spin': spin,
        'compiler': compiler,
        'timeout_seconds': args.timeout,
        'compile_flags': compile_flags,
        'runtime_flags': runtime_flags,
    }
    manifest_path = output / 'manifest.json'

    def save():
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')

    def command(argv, name):
        manifest.setdefault('commands', []).append(argv)
        save()
        with (output / (name + '.log')).open('w') as log:
            try:
                proc = subprocess.run(
                    argv, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, timeout=args.timeout, check=False
                )
                return proc.returncode, False
            except subprocess.TimeoutExpired:
                return -1, True

    print('Artifacts: {}'.format(output), flush=True)
    save()
    for argv, name in (
        ([spin, '-V'], 'spin-version'),
        ([compiler, '--version'], 'compiler-version'),
        ([spin, '-a', relative.name], 'generate'),
        ([compiler] + compile_flags + ['-o', 'pan', 'pan.c'], 'compile'),
    ):
        code, timed_out = command(argv, name)
        if name == 'generate' and args.ltl and code == 0:
            generated = (output / 'generate.log').read_text(errors='replace')
            claims = re.findall(r'^ltl ([A-Za-z_][A-Za-z_0-9]*):', generated, re.MULTILINE)
            if args.ltl not in claims:
                manifest.update(status='error', failed_stage='claim-selection')
                save()
                return 2
        if code:
            manifest.update(status='unknown' if timed_out else 'error', failed_stage=name)
            save()
            return 2
    code, timed_out = command([str(cwd / 'pan')] + runtime_flags, 'pan')
    log = (output / 'pan.log').read_text(errors='replace')
    trails = list(cwd.glob('*.trail'))
    status = classify(log, code, timed_out, bool(trails))
    manifest.update(
        status=status, returncode=code, timed_out=timed_out, trails=[str(path.relative_to(output)) for path in trails]
    )
    save()
    print('Status: {}'.format(status))
    return {'holds': 0, 'violated': 1, 'unknown': 2, 'error': 2}[status]


def main():
    args = parser().parse_args()
    try:
        return run(args)
    except (OSError, ValueError) as error:
        print('Error: {}'.format(error))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
