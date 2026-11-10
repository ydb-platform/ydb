import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

STARTUP_OBJECTS = {'crtbegin.c.o', 'crtend.c.o', 'crtbegin.c.pic.o', 'crtend.c.pic.o'}


def report_stage(module_name, stage, started):
    finished = time.monotonic()
    print(f'Internalize {module_name}: {stage} {finished - started:.1f}s', file=sys.stderr)
    return finished


def prepare_dependencies(tools, inputs, temporary):
    result = []
    for index, name in enumerate(inputs):
        source = Path(name)
        if source.suffix == '.o':
            if source.name not in STARTUP_OBJECTS:
                result.extend(['--start-lib', str(source), '--end-lib'])
        elif source.suffix == '.a':
            members = subprocess.check_output([tools / 'llvm-ar', 't', source], text=True).splitlines()
            excluded = STARTUP_OBJECTS.intersection(members)
            if excluded:
                filtered = temporary / f'dependency-{index}.a'
                shutil.copyfile(source, filtered)
                subprocess.run([tools / 'llvm-ar', 'd', filtered, *sorted(excluded)], check=True)
                source = filtered
            result.append(str(source))
    return result


def read_common_symbols(tools, source):
    # Unlike nm, readelf preserves COMMON alignment as well as size.
    command = [tools / 'llvm-readelf', '--symbols', '--wide', source]
    common = []
    with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as process:
        for line in process.stdout:
            if ' COM ' not in line:
                continue
            fields = line.split(maxsplit=7)
            if len(fields) == 8 and fields[6] == 'COM':
                common.append((fields[7].rstrip('\n'), int(fields[1], 16), int(fields[2], 0)))
        if process.wait():
            raise subprocess.CalledProcessError(process.returncode, command)
    return common


def allocate_common_symbols(tools, source, temporary):
    common = read_common_symbols(tools, source)
    if not common:
        return source
    # LLD leaves COMMON unallocated in -r mode. Localizing such a symbol makes
    # the final linker interpret its alignment as an absolute address.
    alignment = max(alignment for _, alignment, _ in common)
    lines = [f'SECTIONS {{ .bss.yql_internalized_common (NOLOAD) : ALIGN({alignment}) {{']
    for name, alignment, size in common:
        lines.append(f'. = ALIGN({alignment});')
        lines.append(f'{json.dumps(name)} = .;')
        lines.append(f'. += {size};')
    lines.append('} }')
    script = temporary / 'common.ld'
    script.write_text('\n'.join(lines))
    allocated = temporary / 'allocated.o'
    subprocess.run([tools / 'ld.lld', '-r', '--unique', '-T', script, source, '-o', allocated], check=True)
    return allocated


def check_exports(tools, module, exports):
    symbols = subprocess.check_output(
        [
            tools / 'llvm-nm',
            '--extern-only',
            '--defined-only',
            '--format=posix',
            module,
        ],
        text=True,
    )
    names = {line.split()[0] for line in symbols.splitlines() if line}
    if names != set(exports):
        raise ValueError(f'Expected only {", ".join(exports)} to be public, got {sorted(names)}')


def internalize(
    toolchain,
    source,
    output,
    module_name,
    work_dir,
    exports,
    globals_archive=None,
    dependency_inputs=None,
    global_inputs=None,
):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', module_name):
        raise ValueError(f'Invalid library name: {module_name!r}')
    if not exports or any(not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', name) for name in exports):
        raise ValueError(f'Expected non-empty C export symbols, got {exports!r}')
    tools = Path(toolchain) / 'bin'
    started = time.monotonic()
    Path(work_dir).mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=work_dir) as temporary:
        temporary = Path(temporary)
        merged = temporary / 'merged.o'
        module = temporary / 'module.o'
        keep = temporary / 'exports.txt'
        keep.write_text('\n'.join(exports) + '\n')
        if dependency_inputs is not None:
            inputs = ['--whole-archive', *(global_inputs or []), '--no-whole-archive']
            inputs.extend(prepare_dependencies(tools, dependency_inputs, temporary))
        elif globals_archive:
            dependencies = temporary / 'dependencies.a'
            shutil.copyfile(source, dependencies)
            # Startup belongs to the final host. In particular, resolving
            # __dso_handle here pulls in crtbegin/crtend and their ASan-instrumented
            # .eh_frame terminator, which cannot survive the final link.
            subprocess.run(
                [
                    tools / 'llvm-ar',
                    'd',
                    dependencies,
                    'crtbegin.c.o',
                    'crtend.c.o',
                    'crtbegin.c.pic.o',
                    'crtend.c.pic.o',
                ],
                check=True,
            )
            # Debug sections are discarded from the final archive anyway. Drop
            # them before the partial links to bound peak disk and memory usage.
            subprocess.run([tools / 'llvm-objcopy', '--strip-debug', dependencies], check=True)
            source = dependencies
        # GLOBAL objects must survive even when nothing refers to their constructors.
        # Ordinary dependency members are selected on demand. Exports may belong
        # to a PEERDIR archive rather than the producer's GLOBAL objects.
        if dependency_inputs is None:
            inputs = (
                ['--whole-archive', globals_archive, '--no-whole-archive', source]
                if globals_archive
                else ['--whole-archive', source, '--no-whole-archive']
            )
        response = temporary / 'inputs.rsp'
        response.write_text('\n'.join(json.dumps(str(item), ensure_ascii=False) for item in inputs), encoding='utf-8')
        started = report_stage(module_name, 'prepare inputs', started)
        subprocess.run(
            [
                # Preserve input boundaries: an .eh_frame end marker must not terminate
                # the unwind records from a subsequent object in the final link.
                tools / 'ld.lld',
                '-r',
                '--strip-debug',
                '--force-group-allocation',
                '--unique',
                *[f'--undefined={name}' for name in exports],
                f'@{response}',
                '-o',
                merged,
            ],
            check=True,
        )
        started = report_stage(module_name, 'partial link', started)
        merged = allocate_common_symbols(tools, merged, temporary)
        started = report_stage(module_name, 'allocate common symbols', started)
        # Each localized ASan registration flag must guard only this library's
        # metadata, rather than registering the host's entire range again.
        asan_section = f'asan_globals_{module_name}'
        subprocess.run(
            [
                tools / 'llvm-objcopy',
                f'--keep-global-symbols={keep}',
                '--strip-unneeded',
                f'--rename-section=asan_globals={asan_section}',
                f'--redefine-sym=__start_asan_globals=__start_{asan_section}',
                f'--redefine-sym=__stop_asan_globals=__stop_{asan_section}',
                merged,
                module,
            ],
            check=True,
        )
        started = report_stage(module_name, 'localize symbols', started)
        check_exports(tools, module, exports)
        started = report_stage(module_name, 'check exports', started)
        # PEERDIR consumers may be shared libraries. Reject executable-only
        # relocations before publishing an archive that such hosts cannot link.
        subprocess.run([tools / 'ld.lld', '-shared', module, '-o', temporary / 'shared-check.so'], check=True)
        started = report_stage(module_name, 'check shared link', started)
        subprocess.run([tools / 'llvm-ar', 'rcs', temporary / 'cached.a', module], check=True)
        shutil.copyfile(temporary / 'cached.a', output)
        report_stage(module_name, 'write archive', started)


def resolve_paths(args):
    root = Path(args.build_root).resolve() if args.build_root else Path.cwd()
    for name in ('archiver', 'input', 'output', 'globals', 'output_alias'):
        value = getattr(args, name)
        if value is not None:
            setattr(args, name, root / value)
    for name in ('dependency_inputs', 'global_inputs'):
        values = getattr(args, name)
        if values is not None:
            setattr(args, name, [root / value for value in values])


def main():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--archiver', required=True)
    parser.add_argument('--build-root')
    parser.add_argument('--input')
    parser.add_argument('--output', required=True)
    parser.add_argument('--module-name', required=True)
    parser.add_argument('--exports', nargs='+', required=True)
    parser.add_argument('--globals')
    parser.add_argument('--dependency-inputs', nargs='*')
    parser.add_argument('--global-inputs', nargs='*')
    parser.add_argument('--output-alias')
    parser.add_argument(
        '--work-dir', required=True, help='Temporary directory, relative to the output archive directory'
    )
    args = parser.parse_args(
        arg for arg in sys.argv[1:] if arg not in ('--ya-start-command-file', '--ya-end-command-file')
    )
    if (args.input is None) == (args.dependency_inputs is None):
        parser.error('Specify either --input or --dependency-inputs')
    resolve_paths(args)
    internalize(
        Path(args.archiver).parent.parent,
        args.input,
        args.output,
        args.module_name,
        Path(args.output).resolve().parent / args.work_dir,
        args.exports,
        args.globals,
        args.dependency_inputs,
        args.global_inputs,
    )
    if args.output_alias:
        alias = Path(args.output_alias)
        alias.unlink(missing_ok=True)
        alias.hardlink_to(args.output)


if __name__ == '__main__':
    main()
