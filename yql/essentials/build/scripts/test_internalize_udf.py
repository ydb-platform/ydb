import argparse
import ctypes
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from internalize_udf import internalize


class InternalizeUdfTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=self.work_dir)
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name)
        self.tools = Path(self.toolchain) / 'bin'

    def run_tool(self, name, *arguments):
        return subprocess.check_output([self.tools / name, *arguments], text=True)

    def compile(self, name, source, *flags):
        source_path = self.path / f'{name}.cpp'
        object_path = self.path / f'{name}.o'
        source_path.write_text(source)
        self.run_tool('clang++', '-c', '-g', '-fPIC', *flags, source_path, '-o', object_path)
        return object_path

    def make_archive(self, name, value, split_globals=False):
        entry = self.compile(
            name,
            f'''
            extern int Helper();
            extern "C" int Register() {{ return Helper(); }}
            extern "C" int AbiVersion() {{ return {value}; }}
            extern "C" void SetBackTraceCallback() {{}}
            int InternalGlobal = 0;
            struct Library {{ int (*Register)(); int (*AbiVersion)(); }};
            extern "C" const Library* GetStaticUdfLibrary_{name}() {{
                static const Library library{{&Register, &AbiVersion}};
                return &library;
            }}
        ''',
        )
        initializer = self.compile(
            name + '_initializer',
            f'''
            extern int InternalGlobal;
            __attribute__((constructor)) void Initialize() {{ InternalGlobal = {value}; }}
        ''',
        )
        # A valid end marker must not hide unwind records from later objects.
        terminator = self.compile(
            name + '_terminator',
            r'''
            asm(".pushsection .eh_frame,\"a\",@progbits\n.long 0\n.popsection");
        ''',
        )
        helper = self.compile(
            name + '_helper',
            '''
            extern int InternalGlobal;
            template <class T> __attribute__((noinline)) T Identity(T value) { return value; }
            int Helper() { return Identity(InternalGlobal); }
        ''',
        )
        original = self.path / f'{name}_original.a'
        globals_archive = None
        if split_globals:
            globals_archive = self.path / f'{name}.globals.a'
            self.run_tool('llvm-ar', 'rcs', globals_archive, entry, terminator, initializer)
            duplicate1 = self.compile(name + '_unused1', 'int UnusedDuplicate = 1;')
            duplicate2 = self.compile(name + '_unused2', 'int UnusedDuplicate = 2;')
            self.run_tool('llvm-ar', 'rcs', original, helper, duplicate1, duplicate2)
        else:
            self.run_tool('llvm-ar', 'rcs', original, entry, terminator, helper, initializer)
        output = self.path / f'{name}.a'
        internalize(self.toolchain, original, output, name, self.path, globals_archive)
        return output

    def test_multiple_udfs_link_and_run(self):
        first = self.make_archive('first', 17)
        second = self.make_archive('second', 29, split_globals=True)
        main = self.compile(
            'main',
            '''
            struct Library { int (*Register)(); int (*AbiVersion)(); };
            extern "C" const Library* GetStaticUdfLibrary_first();
            extern "C" const Library* GetStaticUdfLibrary_second();
            int InternalGlobal = 100;
            int Helper() { return 100; }
            extern "C" int CheckModules() {
                const auto* first = GetStaticUdfLibrary_first();
                const auto* second = GetStaticUdfLibrary_second();
                return first->Register() != 17 || second->Register() != 29
                    || first->AbiVersion() != 17 || second->AbiVersion() != 29;
            }
        ''',
        )
        library = self.path / 'test_udfs.so'
        self.run_tool('ld.lld', '-shared', main, first, second, '-o', library)
        self.assertEqual(ctypes.CDLL(str(library)).CheckModules(), 0)

    def test_only_abi_definitions_remain_global(self):
        archive = self.make_archive('symbols', 0)
        symbols = self.run_tool('llvm-nm', '--defined-only', '--extern-only', '--format=posix', archive)
        names = {line.split()[0] for line in symbols.splitlines() if line and not line.endswith(':')}
        self.assertEqual(names, {'GetStaticUdfLibrary_symbols'})
        self.assertEqual(self.run_tool('llvm-ar', 't', archive).splitlines(), ['module.o'])
        sections = self.run_tool('llvm-readelf', '--sections', archive)
        self.assertNotIn('.debug_', sections)
        self.assertNotIn('GROUP', sections)

    def test_cli_from_read_only_directory(self):
        entry = self.compile('readonly', 'extern "C" int GetStaticUdfLibrary_readonly() { return 17; }')
        output = (self.path / 'readonly.a').resolve()
        source_dir = self.path / 'source'
        source_dir.mkdir()
        source_dir.chmod(0o555)
        try:
            for work_dir in ('tmp', str((self.path / 'absolute-tmp').resolve())):
                with self.subTest(work_dir=work_dir):
                    subprocess.run(
                        [
                            sys.executable,
                            Path(__file__).with_name('internalize_udf.py').resolve(),
                            '--archiver',
                            (self.tools / 'llvm-ar').resolve(),
                            '--input',
                            entry.resolve(),
                            '--output',
                            output,
                            '--module-name',
                            'readonly',
                            '--work-dir',
                            work_dir,
                        ],
                        cwd=source_dir,
                        check=True,
                    )
                    self.assertEqual(self.run_tool('llvm-ar', 't', output).splitlines(), ['module.o'])
                    self.assertEqual(list(source_dir.iterdir()), [])
                    self.assertEqual(list((output.parent / work_dir).iterdir()), [])
        finally:
            source_dir.chmod(0o755)

    def test_missing_entry_point_rejected(self):
        source = self.compile('missing', 'int InternalGlobal = 0;')
        with self.assertRaisesRegex(ValueError, 'Expected only GetStaticUdfLibrary_missing'):
            internalize(self.toolchain, source, self.path / 'missing.a', 'missing', self.path)

    def test_non_pic_thread_local_storage_rejected(self):
        entry = self.compile(
            'tls',
            '''
            __thread int Counter;
            extern "C" int* GetStaticUdfLibrary_tls() { return &Counter; }
            ''',
            '-fno-pic',
        )
        output = self.path / 'tls.a'
        with self.assertRaises(subprocess.CalledProcessError):
            internalize(self.toolchain, entry, output, 'tls', self.path)
        self.assertFalse(output.exists())

    def test_pic_thread_local_storage_links_into_shared_library(self):
        entry = self.compile(
            'tls',
            '''
            __thread int Counter;
            extern "C" int* GetStaticUdfLibrary_tls() { return &Counter; }
            ''',
        )
        archive = self.path / 'tls.a'
        internalize(self.toolchain, entry, archive, 'tls', self.path)
        library = self.path / 'tls.so'
        self.run_tool('ld.lld', '-shared', '--whole-archive', archive, '-o', library)
        loaded = ctypes.CDLL(str(library))
        loaded.GetStaticUdfLibrary_tls.restype = ctypes.POINTER(ctypes.c_int)
        counter = loaded.GetStaticUdfLibrary_tls()
        self.assertEqual(counter[0], 0)
        counter[0] = 17
        self.assertEqual(loaded.GetStaticUdfLibrary_tls()[0], 17)

    def test_dso_handle_comes_from_host(self):
        for suffix in ('', '.pic'):
            with self.subTest(suffix=suffix):
                self.check_dso_handle_comes_from_host(suffix)

    def check_dso_handle_comes_from_host(self, suffix):
        entry = self.compile(
            'entry',
            '''
            extern "C" void* __dso_handle;
            extern "C" void OnDestruction() noexcept;
            int Constructed = 0;
            struct State {
                State() { Constructed = 17; }
                ~State() { OnDestruction(); }
            } state;
            extern "C" void* GetStaticUdfLibrary_runtime() {
                return Constructed == 17 ? &__dso_handle : nullptr;
            }
            ''',
        )
        begin = self.compile(
            f'crtbegin.c{suffix}',
            '''
            extern "C" {
                extern const int End[];
                void* __dso_handle = (void*)End;
            }
            ''',
        )
        end = self.compile(
            f'crtend.c{suffix}',
            '''
            extern "C" {
                extern const int End[] __attribute__((section(".eh_frame"))) = {0};
            }
            ''',
            '-fsanitize=address',
        )
        globals_archive = self.path / 'runtime.globals.a'
        dependencies = self.path / f'dependencies{suffix}.a'
        self.run_tool('llvm-ar', 'rcs', globals_archive, entry)
        self.run_tool('llvm-ar', 'rcs', dependencies, begin, end)
        archive = self.path / f'runtime{suffix}.a'
        internalize(self.toolchain, dependencies, archive, 'runtime', self.path, globals_archive)
        self.assertEqual(
            self.run_tool('llvm-ar', 't', dependencies).splitlines(),
            [f'crtbegin.c{suffix}.o', f'crtend.c{suffix}.o'],
        )
        host = self.compile(
            'host',
            '''
            extern "C" {
                void* __dso_handle = &__dso_handle;
                void (*Destructor)(void*);
                void* Argument;
                void* RegisteredHandle;
                int Destructions;
                int __cxa_atexit(void (*destructor)(void*), void* argument, void* handle) {
                    Destructor = destructor;
                    Argument = argument;
                    RegisteredHandle = handle;
                    return 0;
                }
                void OnDestruction() noexcept { ++Destructions; }
                void* GetStaticUdfLibrary_runtime();
                int CheckHandle() {
                    const bool correct = GetStaticUdfLibrary_runtime() == &__dso_handle
                        && RegisteredHandle == &__dso_handle && Destructor;
                    if (Destructor) Destructor(Argument);
                    return correct && Destructions == 1;
                }
            }
            ''',
        )
        library = self.path / f'runtime{suffix}.so'
        self.run_tool('ld.lld', '-shared', '-Bsymbolic', host, archive, '-o', library)
        self.assertEqual(ctypes.CDLL(str(library)).CheckHandle(), 1)

    def test_common_storage_is_allocated_before_localization(self):
        entry = self.compile(
            'common',
            '''
            int Counter __attribute__((common, aligned(64)));
            int* GetStaticUdfLibrary_common() { return &Counter; }
            ''',
            '-x',
            'c',
        )
        archive = self.path / 'common.a'
        internalize(self.toolchain, entry, archive, 'common', self.path)
        symbols = self.run_tool('llvm-nm', '--format=posix', archive)
        self.assertNotIn('Counter C ', symbols)
        library = self.path / 'common.so'
        self.run_tool('ld.lld', '-shared', '--whole-archive', archive, '-o', library)
        loaded = ctypes.CDLL(str(library))
        loaded.GetStaticUdfLibrary_common.restype = ctypes.POINTER(ctypes.c_int)
        counter = loaded.GetStaticUdfLibrary_common()
        self.assertEqual(ctypes.addressof(counter.contents) % 64, 0)
        self.assertEqual(counter[0], 0)
        counter[0] = 17
        self.assertEqual(loaded.GetStaticUdfLibrary_common()[0], 17)

    def test_asan_registration_ranges_are_per_udf(self):
        archives = []
        for name in ('first', 'second'):
            entry = self.compile(
                name,
                f'''
                int Global;
                extern "C" int* GetStaticUdfLibrary_{name}() {{ return &Global; }}
                ''',
                '-fsanitize=address',
            )
            archive = self.path / f'{name}.a'
            internalize(self.toolchain, entry, archive, name, self.path)
            archives.append(archive)
        runtime = self.compile(
            'asan_runtime',
            '''
            extern "C" {
                unsigned Registrations;
                void* Begins[2];
                void* Ends[2];
                void __asan_init() {}
                void __asan_version_mismatch_check_v8() {}
                void __asan_register_elf_globals(unsigned long* flag, void* begin, void* end) {
                    if (*flag) return;
                    if (Registrations < 2) {
                        Begins[Registrations] = begin;
                        Ends[Registrations] = end;
                    }
                    ++Registrations;
                    *flag = 1;
                }
                void __asan_unregister_elf_globals(unsigned long*, void*, void*) {}
                int CheckRanges() {
                    return Registrations == 2 && Begins[0] && Begins[1]
                        && Begins[0] != Ends[0] && Begins[1] != Ends[1]
                        && (Ends[0] <= Begins[1] || Ends[1] <= Begins[0]);
                }
            }
            ''',
        )
        library = self.path / 'asan.so'
        self.run_tool('ld.lld', '-shared', '-Bsymbolic', runtime, '--whole-archive', *archives, '-o', library)
        self.assertEqual(ctypes.CDLL(str(library)).CheckRanges(), 1)

    def test_invalid_module_name_rejected(self):
        for name in ('', 'bad-name', '0module'):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'Invalid UDF name'):
                internalize(self.toolchain, '', '', name, self.path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--toolchain', required=True)
    parser.add_argument('--work-dir', required=True)
    args, remaining = parser.parse_known_args()
    Path(args.work_dir).mkdir(parents=True, exist_ok=True)
    InternalizeUdfTest.toolchain = args.toolchain
    InternalizeUdfTest.work_dir = args.work_dir
    unittest.main(argv=[__file__, *remaining])
