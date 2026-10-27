import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def allocate_common_symbols(tools, source, temporary):
    info = json.loads(
        subprocess.check_output([tools / 'llvm-readelf', '--symbols', '--elf-output-style=JSON', source], text=True)
    )
    common = [item['Symbol'] for item in info[0]['Symbols'] if item['Symbol']['Section']['Name'] == 'Common']
    if not common:
        return source
    # LLD leaves COMMON unallocated in -r mode. Localizing such a symbol makes
    # the final linker interpret its alignment as an absolute address.
    alignment = max(symbol['Value'] for symbol in common)
    lines = [f'SECTIONS {{ .bss.yql_udf_common (NOLOAD) : ALIGN({alignment}) {{']
    for symbol in common:
        lines.append(f'. = ALIGN({symbol["Value"]});')
        lines.append(f'{json.dumps(symbol["Name"]["Name"])} = .;')
        lines.append(f'. += {symbol["Size"]};')
    lines.append('} }')
    script = temporary / 'common.ld'
    script.write_text('\n'.join(lines))
    allocated = temporary / 'allocated.o'
    subprocess.run([tools / 'ld.lld', '-r', '--unique', '-T', script, source, '-o', allocated], check=True)
    return allocated


def check_exports(tools, module, entry_point):
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
    if names != {entry_point}:
        raise ValueError(f'Expected only {entry_point} to be public, got {sorted(names)}')


def internalize(toolchain, source, output, module_name, work_dir, globals_archive=None):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', module_name):
        raise ValueError(f'Invalid UDF name: {module_name!r}')
    tools = Path(toolchain) / 'bin'
    entry_point = f'GetStaticUdfLibrary_{module_name}'
    Path(work_dir).mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=work_dir) as temporary:
        temporary = Path(temporary)
        merged = temporary / 'merged.o'
        module = temporary / 'module.o'
        keep = temporary / 'exports.txt'
        keep.write_text(entry_point + '\n')
        if globals_archive:
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
            source = dependencies
        # GLOBAL objects must survive even when nothing refers to their constructors.
        # Ordinary dependency members are selected on demand, as in the shared UDF link.
        inputs = (
            ['--whole-archive', globals_archive, '--no-whole-archive', source]
            if globals_archive
            else ['--whole-archive', source, '--no-whole-archive']
        )
        subprocess.run(
            [
                # Preserve input boundaries: an .eh_frame end marker must not terminate
                # the unwind records from a subsequent object in the final link.
                tools / 'ld.lld',
                '-r',
                '--force-group-allocation',
                '--unique',
                *inputs,
                '-o',
                merged,
            ],
            check=True,
        )
        merged = allocate_common_symbols(tools, merged, temporary)
        # Each localized ASan registration flag must guard only this UDF's
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
        check_exports(tools, module, entry_point)
        # PEERDIR consumers may be shared libraries. Reject executable-only
        # relocations before publishing an archive that such hosts cannot link.
        subprocess.run([tools / 'ld.lld', '-shared', module, '-o', temporary / 'shared-check.so'], check=True)
        subprocess.run([tools / 'llvm-ar', 'rcs', temporary / 'cached.a', module], check=True)
        shutil.copyfile(temporary / 'cached.a', output)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--archiver', required=True)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--module-name', required=True)
    parser.add_argument('--globals')
    parser.add_argument(
        '--work-dir', required=True, help='Temporary directory, relative to the output archive directory'
    )
    args = parser.parse_args()
    internalize(
        Path(args.archiver).parent.parent,
        args.input,
        args.output,
        args.module_name,
        Path(args.output).resolve().parent / args.work_dir,
        args.globals,
    )


if __name__ == '__main__':
    main()
