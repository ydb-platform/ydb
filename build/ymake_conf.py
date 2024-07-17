#!/usr/bin/env python
# coding=utf-8

from __future__ import print_function

import base64
import itertools
import json
import logging
import optparse
import os
import posixpath
import re
import subprocess
import sys
import tempfile

import six

from functools import total_ordering

logger = logging.getLogger(__name__ if __name__ != '__main__' else 'ymake_conf.py')


class WindowsVersion(object):
    """
    Predefined values for _WIN32_WINNT macro.
    This macro specifies minimal Windows version required by the binary being build.

    A complete list of the values supported by the Windows SDK can be found at
    https://docs.microsoft.com/en-us/cpp/porting/modifying-winver-and-win32-winnt
    """
    Windows07 = '0x0601'
    Windows08 = '0x0602'
    Windows10 = '0x0A00'


# This is default Android API level unless `-DANDROID_API` is specified in cmdline
# Android api level can be resolved to Android version using
# https://android.googlesource.com/platform/bionic/+/master/docs/status.md
ANDROID_API_DEFAULT = 21

# This is default Linux SDK unless `-DOS_SDK` is specified in cmdline
LINUX_SDK_DEFAULT = "ubuntu-14"

MACOS_VERSION_MIN = "11.0"
MACOS_VERSION_MIN_AS_INT = "110000"
IOS_VERSION_MIN = "13.0"
WINDOWS_VERSION_MIN = WindowsVersion.Windows07


def init_logger(verbose):
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)


def find_conf(conf_file):
    script_dir = os.path.dirname(__file__)
    full_path = os.path.join(script_dir, conf_file)
    if os.path.exists(full_path):
        return full_path
    return None


# All paths in build graph shall be in posix format.
# This ensures that UIDs of cross and native builds won't diverge
# due to paths difference. This function is to be used to fix paths produced
# by os.path.join on Windows.
#
# FIXME: upon complete switch to Python3 os.path.join may be replaced by posixpath.join
#        and this function will become unnecessary
def win_path_fix(path):
    return path if sys.platform != 'win32' else path.replace('\\', '/')


class DebugString(object):
    def __init__(self, get_string_func):
        self.get_string_func = get_string_func

    def __str__(self):
        return self.get_string_func()


class ConfigureError(Exception):
    pass


@total_ordering
class Platform(object):
    def __init__(self, name, os, arch):
        """
        :type name: str
        :type os: str
        :type arch: str
        """
        self.name = name
        self.os = self._parse_os(os)
        self.arch = arch.lower()

        self.is_i386 = self.arch in ('i386', 'x86')
        self.is_i686 = self.arch == 'i686'
        self.is_x86 = self.is_i386 or self.is_i686
        self.is_x86_64 = self.arch in ('x86_64', 'amd64')
        self.is_intel = self.is_x86 or self.is_x86_64

        self.is_armv7 = self.arch in ('armv7', 'armv7a', 'armv7ahf', 'armv7a_neon', 'arm', 'armv7a_cortex_a9', 'armv7ahf_cortex_a35', 'armv7ahf_cortex_a53')
        self.is_armv8 = self.arch in ('armv8', 'armv8a', 'arm64', 'aarch64', 'armv8a_cortex_a35', 'armv8a_cortex_a53')
        self.is_armv8m = self.arch in ('armv8m_cortex_m33', 'armv8m_cortex_m23')
        self.is_armv7em = self.arch in ('armv7em_cortex_m4', 'armv7em_cortex_m7')
        self.is_arm64 = self.arch in ('arm64',)
        self.is_arm = self.is_armv7 or self.is_armv8 or self.is_armv8m or self.is_armv7em
        self.is_armv7_neon = self.arch in ('armv7a_neon', 'armv7ahf', 'armv7a_cortex_a9', 'armv7ahf_cortex_a35', 'armv7ahf_cortex_a53')
        self.is_armv7hf = self.arch in ('armv7ahf', 'armv7ahf_cortex_a35', 'armv7ahf_cortex_a53')
        self.is_armv5te = self.arch in ('armv5te_arm968e_s')

        self.is_rv32imc = self.arch in ('riscv32_esp',)
        self.is_riscv32 = self.is_rv32imc

        self.is_nds32 = self.arch in ('nds32le_elf_mculib_v5f',)
        self.is_tc32 = self.arch in ('tc32_elf',)

        self.is_xtensa_hifi4 = self.arch == 'xtensa_hifi4'
        self.is_xtensa_hifi5 = self.arch == 'xtensa_hifi5'
        self.is_xtensa = self.is_xtensa_hifi4 or self.is_xtensa_hifi5

        self.armv7_float_abi = None
        if self.is_armv7:
            if self.is_armv7hf:
                self.armv7_float_abi = 'hard'
            else:
                self.armv7_float_abi = 'softfp'

        self.is_cortex_a9 = self.arch in ('armv7a_cortex_a9',)
        self.is_cortex_a35 = self.arch in ('armv7ahf_cortex_a35', 'armv8a_cortex_a35')
        self.is_cortex_a53 = self.arch in ('armv7ahf_cortex_a53', 'armv8a_cortex_a53')
        self.is_cortex_m33 = self.arch in ('armv8m_cortex_m33',)
        self.is_cortex_m23 = self.arch in ('armv8m_cortex_m23',)
        self.is_cortex_m4 = self.arch in ('armv7em_cortex_m4',)
        self.is_cortex_m7 = self.arch in ('armv7em_cortex_m7')
        self.is_arm968e_s = self.arch in ('armv5te_arm968e_s')

        self.is_power8le = self.arch == 'ppc64le'
        self.is_power9le = self.arch == 'power9le'
        self.is_powerpc = self.is_power8le or self.is_power9le

        self.is_wasm64 = self.arch == 'wasm64'
        self.is_wasm = self.is_wasm64

        self.is_32_bit = self.is_x86 or self.is_armv5te or self.is_armv7 or self.is_armv8m or self.is_riscv32 or self.is_nds32 or self.is_armv7em or self.is_xtensa or self.is_tc32
        self.is_64_bit = self.is_x86_64 or self.is_armv8 or self.is_powerpc or self.is_wasm64

        assert self.is_32_bit or self.is_64_bit
        assert not (self.is_32_bit and self.is_64_bit)

        self.is_linux = self.os == 'linux' or 'yocto' in self.os
        self.is_linux_x86_64 = self.is_linux and self.is_x86_64
        self.is_linux_armv8 = self.is_linux and self.is_armv8
        self.is_linux_armv7 = self.is_linux and self.is_armv7
        self.is_linux_power8le = self.is_linux and self.is_power8le
        self.is_linux_power9le = self.is_linux and self.is_power9le
        self.is_linux_powerpc = self.is_linux_power8le or self.is_linux_power9le

        self.is_macos = self.os == 'macos'
        self.is_macos_x86_64 = self.is_macos and self.is_x86_64
        self.is_macos_arm64 = self.is_macos and self.is_arm64
        self.is_iossim = self.os == 'iossim' or (self.os == 'ios' and self.is_intel)
        self.is_ios = self.os == 'ios' or self.is_iossim
        self.is_apple = self.is_macos or self.is_ios

        self.is_windows = self.os == 'windows'
        self.is_windows_x86_64 = self.is_windows and self.is_x86_64

        self.is_android = self.os == 'android'
        if self.is_android:
            self.android_api = int(preset('ANDROID_API', ANDROID_API_DEFAULT))

        self.is_cygwin = self.os == 'cygwin'
        self.is_yocto = self.os == 'yocto'
        self.is_emscripten = self.os == 'emscripten'

        self.is_none = self.os == 'none'

        self.is_posix = self.is_linux or self.is_apple or self.is_android or self.is_cygwin or self.is_yocto

    @staticmethod
    def from_json(data):
        name = data.get('visible_name', data['toolchain'])
        return Platform(name, os=data['os'], arch=data['arch'])

    @property
    def os_variables(self):
        # 'LINUX' variable, for backward compatibility
        yield self.os.upper()

        # 'OS_LINUX' variable
        yield 'OS_{}'.format(self.os.upper())

        # yocto is linux
        if 'yocto' in self.os:
            yield 'LINUX'
            yield 'OS_LINUX'

        if self.is_macos:
            yield 'DARWIN'
            yield 'OS_DARWIN'
        if self.is_iossim:
            yield 'IOS'
            yield 'OS_IOS'
            yield 'OS_IOSSIM'

    @property
    def arch_variables(self):
        return select_multiple((
            (self.is_i386 or self.is_i686, 'ARCH_I386'),
            (self.is_i686, 'ARCH_I686'),
            (self.is_x86_64, 'ARCH_X86_64'),
            (self.is_armv7, 'ARCH_ARM7'),
            (self.is_armv7_neon, 'ARCH_ARM7_NEON'),
            (self.is_armv8, 'ARCH_ARM64'),
            (self.is_armv8m, 'ARCH_ARM8M'),
            (self.is_armv7em, 'ARCH_ARM7EM'),
            (self.is_armv5te, 'ARCH_ARM5TE'),
            (self.is_arm, 'ARCH_ARM'),
            (self.is_linux_armv8 or self.is_macos_arm64, 'ARCH_AARCH64'),
            (self.is_powerpc, 'ARCH_PPC64LE'),
            (self.is_power8le, 'ARCH_POWER8LE'),
            (self.is_power9le, 'ARCH_POWER9LE'),
            (self.is_riscv32, 'ARCH_RISCV32'),
            (self.is_xtensa_hifi4, 'ARCH_XTENSA_HIFI4'),
            (self.is_xtensa_hifi5, 'ARCH_XTENSA_HIFI5'),
            (self.is_xtensa, 'ARCH_XTENSA'),
            (self.is_nds32, 'ARCH_NDS32'),
            (self.is_tc32, 'ARCH_TC32'),
            (self.is_wasm64, 'ARCH_WASM64'),
            (self.is_32_bit, 'ARCH_TYPE_32'),
            (self.is_64_bit, 'ARCH_TYPE_64'),
        ))

    @property
    def library_path_variables(self):
        return ['LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH']

    def find_in_dict(self, dict_, default=None):
        if dict_ is None:
            return default
        for key in six.iterkeys(dict_):
            if self._parse_os(key) == self.os:
                return dict_[key]
        return default

    @property
    def os_compat(self):
        if self.is_macos:
            return 'DARWIN'
        else:
            return self.os.upper()

    @property
    def canonized_platform(self):
        os = None
        if self.os == 'macos':
            os = 'darwin'
        elif self.os == 'windows':
            os = 'win32'
        else:
            os = self.os
        if self.arch != 'x86_64':
            return '-'.join([os, self.arch])
        else:
            return os

    def exe(self, *paths):
        return posixpath.join(*paths)

    def __str__(self):
        return '{name}-{os}-{arch}'.format(name=self.name, os=self.os, arch=self.arch)

    def __eq__(self, other):
        return (self.name, self.os, self.arch) == (other.name, other.os, other.arch)

    def __lt__(self, other):
        return (self.name, self.os, self.arch) < (other.name, other.os, other.arch)

    def __hash__(self):
        return hash((self.name, self.os, self.arch))

    @staticmethod
    def _parse_os(os):
        os = os.lower()

        if os == 'darwin':
            return 'macos'
        if os in ('win', 'win32', 'win64'):
            return 'windows'
        if os.startswith('cygwin'):
            return 'cygwin'

        return os


def which(prog):
    if os.path.exists(prog) and os.access(prog, os.X_OK):
        return prog

    # Ищем в $PATH только простые команды, без путей.
    if os.path.dirname(prog) != '':
        return None

    path = os.getenv('PATH', '')

    pathext = os.environ.get('PATHEXT')
    # На Windows %PATHEXT% указывает на список расширений, которые нужно проверять
    # при поиске команды в пути. Точное совпадение без расширения имеет приоритет.
    pathext = [''] if pathext is None else [''] + pathext.lower().split(os.pathsep)

    for dir_ in path.split(os.path.pathsep):
        for ext in pathext:
            p = os.path.join(dir_, prog + ext)
            if os.path.exists(p) and os.path.isfile(p) and os.access(p, os.X_OK):
                return win_path_fix(p)

    return None


def get_stdout(command):
    stdout, code = get_stdout_and_code(command)
    return stdout if code == 0 else None


def get_stdout_and_code(command):
    # noinspection PyBroadException
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = process.communicate()
        return six.ensure_str(stdout), process.returncode
    except Exception as e:
        logger.info("While run: `%s`", e)
        return None, None


def to_strings(o):
    if isinstance(o, (list, tuple)):
        for s in o:
            for ss in to_strings(s):
                yield ss
    else:
        if o is not None:
            if isinstance(o, bool):
                yield 'yes' if o else 'no'
            elif isinstance(o, (str, int)):
                yield str(o)
            else:
                raise ConfigureError('Unexpected value {} {}'.format(type(o), o))


def emit(key, *value):
    print('{0}={1}'.format(key, ' '.join(to_strings(value))))


def emit_with_comment(comment, key, *value):
    print('# {}'.format(comment))
    emit(key, *value)


def emit_with_ignore_comment(key, *value):
    emit_with_comment('IGNORE YMAKE CONF CONTEXT', key, *value)


def append(key, *value):
    print('{0}+={1}'.format(key, ' '.join(to_strings(value))))


def emit_big(text):
    prefix = None
    first = True
    for line in text.split('\n'):
        if prefix is None:
            if not line:
                continue

            prefix = 0
            while prefix < len(line) and line[prefix] == ' ':
                prefix += 1

        if first:  # Be pretty, prepend an empty line before the output
            print()
            first = False

        print(line[prefix:])


class Variables(dict):
    def emit(self, with_ignore_comment=[]):
        with_ignore_comment_set = set(with_ignore_comment)
        for k in sorted(self.keys()):
            if k in with_ignore_comment_set:
                emit_with_ignore_comment(k, self[k])
            else:
                emit(k, self[k])

    def update_from_presets(self):
        for k in six.iterkeys(self):
            v = preset(k)
            if v is not None:
                self[k] = v

    def reset_if_any(self, value_check=None, reset_value=None):
        if value_check is None:
            def value_check(v_):
                return v_ is None

        if any(map(value_check, six.itervalues(self))):
            for k in six.iterkeys(self):
                self[k] = reset_value


def format_env(env, list_separator=':'):
    def format_value(value):
        return value if isinstance(value, str) else ('\\' + list_separator).join(value)

    def format(kv):
        return '${env:"%s=%s"}' % (kv[0], format_value(kv[1]))

    return ' '.join(map(format, sorted(six.iteritems(env))))


# TODO(somov): Проверить, используется ли это. Может быть, выпилить.
def userify_presets(presets, keys):
    for key in keys:
        user_key = 'USER_{}'.format(key)
        values = [presets.pop(key, None), presets.get(user_key)]
        presets[user_key] = ' '.join(filter(None, values))


def preset(key, default=None):
    return opts().presets.get(key, default)


def is_positive(key):
    return is_positive_str(preset(key, ''))


def is_positive_str(s):
    return s.lower() in ('yes', 'true', 'on', '1')


def is_negative(key):
    return is_negative_str(preset(key, ''))


def is_negative_str(s):
    return s.lower() in ('no', 'false', 'off', '0')


def to_bool(s, default=None):
    if isinstance(s, six.string_types):
        if is_positive_str(s):
            return True
        if is_negative_str(s):
            return False
    if default is None:
        raise ConfigureError('{} is not a bool value'.format(s))
    return default


def select(selectors, default=None, no_default=False):
    for enabled, value in selectors:
        if enabled:
            return value
    if no_default:
        raise ConfigureError()
    return default


def select_multiple(selectors):
    for enabled, value in selectors:
        if enabled:
            yield value


def unique(it):
    known = set()
    for i in it:
        if i not in known:
            known.add(i)
            yield i


class Options(object):
    def __init__(self, argv):
        def parse_presets(raw_presets):
            presets = {}
            for p in raw_presets:
                toks = p.split('=', 1)
                name = toks[0]
                value = toks[1] if len(toks) >= 2 else ''
                presets[name] = value
            return presets

        parser = optparse.OptionParser(add_help_option=False)
        opt_group = optparse.OptionGroup(parser, 'Conf script options')
        opt_group.add_option('--toolchain-params', dest='toolchain_params', action='store', help='Set toolchain params via file')
        opt_group.add_option('-D', '--preset', dest='presets', action='append', default=[], help='set or override presets')
        opt_group.add_option('-l', '--local-distbuild', dest='local_distbuild', action='store_true', default=False, help='conf for local distbuild')
        parser.add_option_group(opt_group)

        self.options, self.arguments = parser.parse_args(argv)

        argv = self.arguments
        if len(argv) < 4:
            print('Usage: ArcRoot, --BuildType--, Verbosity, [Path to local.ymake]', file=sys.stderr)
            sys.exit(1)

        self.arcadia_root = argv[1]
        init_logger(argv[3] == 'verbose')

        # Эти переменные не надо использоваться напрямую. Их значения уже разбираются в других местах.
        self.build_type = argv[2].lower()
        self.local_distbuild = self.options.local_distbuild
        self.toolchain_params = self.options.toolchain_params

        self.presets = parse_presets(self.options.presets)
        userify_presets(self.presets, ('CFLAGS', 'CXXFLAGS', 'CONLYFLAGS', 'LDFLAGS', 'GO_COMPILE_FLAGS', 'GO_LINK_FLAGS', 'STD_CXX_VERSION'))

    Instance = None


def opts():
    if Options.Instance is None:
        Options.Instance = Options(sys.argv)
    return Options.Instance


class Profiler(object):
    Generic = 'generic'
    GProf = 'gprof'


class Arcadia(object):
    def __init__(self, root):
        self.root = root


class Build(object):
    def __init__(self, arcadia, build_type, toolchain_params, force_ignore_local_files=False):
        self.arcadia = arcadia
        self.params = self._load_json_from_base64(toolchain_params)
        self.build_type = build_type

        platform = self.params['platform']
        self.host = Platform.from_json(platform['host'])
        self.target = Platform.from_json(platform['target'])

        self.tc = self._get_toolchain_options()

        # TODO(somov): Удалить, когда перестанет использоваться.
        self.build_system = 'ymake'

        self.ignore_local_files = False

        dist_prefix = 'dist-'
        if self.build_type.startswith(dist_prefix):
            self.build_system = 'distbuild'
            self.build_type = self.build_type[len(dist_prefix):]

        if force_ignore_local_files:
            self.ignore_local_files = True

        if self.is_ide_build_type(self.build_type):
            self.ignore_local_files = True

        self.pic = not is_positive('FORCE_NO_PIC')

    @property
    def host_target(self):
        return self.host, self.target

    def print_build(self):
        self._print_build_settings()

        host_os = System(self.host)
        host_os.print_host_settings()

        target_os = System(self.target)
        target_os.print_target_settings()

        if self.pic:
            emit('PIC', 'yes')

        emit('COMPILER_ID', self.tc.type.upper())

        if self.is_valgrind:
            emit('WITH_VALGRIND', 'yes')

        if self.is_coverage:
            emit('_BUILD_COVERAGE', 'yes')

        if self.is_debug:
            emit('_BUILD_DEBUG', 'yes')

        if self.is_fast_debug:
            emit('_BUILD_FAST_DEBUG', 'yes')

        if self.is_release:
            emit('_BUILD_RELEASE', 'yes')

        if self.is_sanitized:
            emit('_BUILD_SANITIZED', 'yes')

        if self.is_size_optimized:
            emit('_BUILD_SIZE_OPTIMIZED', 'yes')

        if self.with_ndebug:
            emit('_BUILD_WITH_NDEBUG', 'yes')

        toolchain_type, compiler_type, linker_type = Compilers[self.tc.type]
        toolchain = toolchain_type(self.tc, self)
        compiler = compiler_type(self.tc, self)
        linker = linker_type(self.tc, self)

        toolchain.print_toolchain()
        compiler.print_compiler()
        linker.print_linker()

        self._print_other_settings(compiler)

    def _print_build_settings(self):
        emit('BUILD_TYPE', self.build_type.upper())
        emit('BT_' + self.build_type.upper().replace('-', '_'), 'yes')

        if self.build_system == 'distbuild':
            emit('DISTBUILD', 'yes')
        elif self.build_system != 'ymake':
            raise ConfigureError()

        python_bin = preset('BUILD_PYTHON_BIN', '$(PYTHON)/python')

        emit('YMAKE_PYTHON', python_bin)
        emit('YMAKE_UNPICKLER', python_bin, '$ARCADIA_ROOT/build/plugins/_unpickler.py')

    @property
    def is_release(self):
        # TODO(somov): Проверить, бывают ли тут суффиксы на самом деле
        return self.build_type in ('release', 'relwithdebinfo', 'minsizerel', 'profile', 'gprof') or self.build_type.endswith('-release')

    @property
    def is_debug(self):
        return self.build_type in ('debug', 'debugnoasserts', 'fastdebug') or self.build_type.endswith('-debug')

    @property
    def is_fast_debug(self):
        return self.build_type == 'fastdebug'

    @property
    def is_size_optimized(self):
        return self.build_type == 'minsizerel'

    @property
    def is_coverage(self):
        return self.build_type == 'coverage'

    @property
    def is_sanitized(self):
        sanitizer = preset('SANITIZER_TYPE')
        return bool(sanitizer) and not is_negative_str(sanitizer)

    @property
    def with_ndebug(self):
        return self.build_type in ('release', 'minsizerel', 'valgrind-release', 'profile', 'gprof', 'debugnoasserts')

    @property
    def is_valgrind(self):
        return self.build_type == 'valgrind' or self.build_type == 'valgrind-release'

    @property
    def is_ide(self):
        return self.is_ide_build_type(self.build_type)

    @property
    def profiler_type(self):
        if self.build_type == 'profile':
            return Profiler.Generic
        elif self.build_type == 'gprof':
            return Profiler.GProf
        else:
            return None

    @staticmethod
    def is_ide_build_type(build_type):
        return build_type == 'nobuild'

    def _configure_runtime_versions(self):
        try:
            res = subprocess.check_output(['xcrun', 'simctl', 'list', '--json', 'runtimes'])
        except subprocess.CalledProcessError as exc:
            logger.warning(exc)
            return
        raw_object = json.loads(res)
        raw_object = raw_object['runtimes']
        for runtime in raw_object:
            if runtime['isAvailable']:
                if "iOS" in runtime['identifier']:
                    emit('DEFAULT_IOS_RUNTIME', '{}'.format(runtime['identifier']))

    def _get_toolchain_options(self):
        type_ = self.params['params']['type']

        if self.params['params'].get('local') and type_ == 'xcode':
            detector = CompilerDetector()
            detector.detect(self.params['params']['c_compiler'], self.params['params']['cxx_compiler'])
            emit('LOCAL_XCODE_TOOLS', 'yes')
            emit('XCODE', 'yes')
            emit('ACTOOL_PATH', self.params['params']['actool'])
            emit('IBTOOL_PATH', self.params['params']['ibtool'])
            self._configure_runtime_versions()
        elif type_ == 'system_cxx':
            c_compiler = self.params['params'].get('c_compiler')
            cxx_compiler = self.params['params'].get('cxx_compiler')
            if is_positive('USE_CLANG_CL'):
                c_compiler = 'clang-cl.exe' if c_compiler == 'cl.exe' else c_compiler
                cxx_compiler = 'clang-cl.exe' if cxx_compiler == 'cl.exe' else cxx_compiler
            detector = CompilerDetector()
            detector.detect(c_compiler, cxx_compiler)
            type_ = detector.type
        else:
            detector = None

        if type_ == 'msvc':
            return MSVCToolchainOptions(self, detector)
        else:
            return GnuToolchainOptions(self, detector)

    def _print_other_settings(self, compiler):
        host = self.host

        emit('USE_LOCAL_TOOLS', 'no' if self.ignore_local_files else 'yes')

        ragel = Ragel()
        ragel.configure_toolchain(self, compiler)
        ragel.print_variables()

        swiftc = SwiftCompiler(self)
        swiftc.configure()
        swiftc.print_compiler()

        if host.is_linux or host.is_macos or host.is_cygwin:
            if is_negative('USE_ARCADIA_PYTHON'):
                python = Python(self.tc)
                python.configure_posix()
                python.print_variables()

        cuda = Cuda(self)
        cuda.print_()
        CuDNN(cuda).print_()

        if self.ignore_local_files or host.is_windows or is_positive('NO_SVN_DEPENDS'):
            emit_with_ignore_comment('SVN_DEPENDS_CACHE__NO_UID__')
        else:
            emit_with_ignore_comment('SVN_DEPENDS_CACHE__NO_UID__', '${hide;kv:"disable_cache"}')

    @staticmethod
    def _load_json_from_base64(base64str):
        """
        :rtype: dict[str, Any]
        """

        def un_unicode(o):
            if isinstance(o, six.text_type):
                return six.ensure_str(o)
            if isinstance(o, list):
                return [un_unicode(oo) for oo in o]
            if isinstance(o, dict):
                return {un_unicode(k): un_unicode(v) for k, v in six.iteritems(o)}
            return o

        return un_unicode(json.loads(base64.b64decode(base64str)))


class YMake(object):
    def __init__(self, arcadia):
        self.arcadia = arcadia

    def print_presets(self):
        presets = opts().presets
        if is_positive_str(presets.get('EXPORT_GRADLE', 'no')):
            print('@import "${CONF_ROOT}/conf/export_gradle.yes.conf"')
        else:
            print('@import "${CONF_ROOT}/conf/export_gradle.no.conf"')

        if presets.get('CLANG_COVERAGE', None) is None:
            print('@import "${CONF_ROOT}/conf/coverage_full_instrumentation.conf"')
        else:
            print('@import "${CONF_ROOT}/conf/coverage_selective_instrumentation.conf"')

        if presets:
            print('# Variables set from command line by -D options')
            for key in sorted(presets):
                emit(key, opts().presets[key])

    @staticmethod
    def _print_conf_content(path):
        with open(path, 'r') as fin:
            print(fin.read())

    def print_core_conf(self):
        print('@import "${CONF_ROOT}/ymake.core.conf"')

    def print_settings(self):
        pass


class System(object):
    def __init__(self, platform):
        """
        :type platform: Platform
        """
        self.platform = platform

    def print_windows_target_const(self):
        # TODO(somov): Remove this variables, use generic OS/arch variables in makelists.
        emit('WINDOWS', 'yes')
        emit('WIN32', 'yes')
        if self.platform.is_64_bit == 64:
            emit('WIN64', 'yes')

    def print_nix_target_const(self):
        emit('JAVA_INCLUDE', '-I{0}/include -I{0}/include/{1}'.format('/usr/lib/jvm/default-java', self.platform.os_compat))

        emit('UNIX', 'yes')
        emit('REALPRJNAME')
        emit('SONAME')

    def print_target_settings(self):
        emit('TARGET_PLATFORM', self.platform.os_compat)
        emit('CANONIZED_TARGET_PLATFORM', self.platform.canonized_platform)
        emit('HARDWARE_ARCH', '32' if self.platform.is_32_bit else '64')
        emit('HARDWARE_TYPE', self.platform.arch)

        for variable in self.platform.arch_variables:
            emit(variable, 'yes')

        for variable in self.platform.os_variables:
            emit(variable, 'yes')

        if self.platform.is_armv7:
            emit('ARM7_FLOAT_ABI', self.platform.armv7_float_abi)

        if self.platform.is_android:
            emit('ANDROID_API', str(self.platform.android_api))

        if self.platform.is_posix:
            self.print_nix_target_const()
        elif self.platform.is_windows:
            self.print_windows_target_const()

    def print_host_settings(self):
        emit('HOST_PLATFORM', self.platform.os_compat)
        emit('CANONIZED_HOST_PLATFORM', self.platform.canonized_platform)

        for variable in itertools.chain(self.platform.os_variables, self.platform.arch_variables):
            emit('HOST_{var}'.format(var=variable), 'yes')


class CompilerDetector(object):
    def __init__(self):
        self.type = None
        self.c_compiler = None
        self.cxx_compiler = None
        self.version_list = None

    @staticmethod
    def preprocess_source(compiler, source):
        # noinspection PyBroadException
        try:
            fd, path = tempfile.mkstemp(suffix='.cpp')
            try:
                with os.fdopen(fd, 'w') as output:
                    output.write(source)
                stdout, code = get_stdout_and_code([compiler, '-E', path])
            except Exception as e:
                logger.info("While writing: `%s`", e)
            finally:
                os.remove(path)
            return stdout, code

        except Exception as e:
            logger.debug('Preprocessing failed: %s', e)
            return None, None

    @staticmethod
    def get_compiler_vars(compiler, names):
        prefix = '____YA_VAR_'
        source = '\n'.join(['{prefix}{name}={name}\n'.format(prefix=prefix, name=n) for n in names])

        # Некоторые препроцессоры возвращают ненулевой код возврата. Поэтому его проверять нельзя.
        # Мы можем только удостовериться после разбора stdout, что в нём
        # присутствовала хотя бы одна подставленная переменная.
        # TODO(somov): Исследовать, можно ли проверять ограниченный набор кодов возврата.
        # TODO(v-korovin): Нормально прокидывать Exception-ы, оно и так упадёт
        stdout, _ = CompilerDetector.preprocess_source(compiler, source)

        if stdout is None:
            return None

        vars_ = {}
        for line in six.ensure_str(stdout).split('\n'):
            parts = line.split('=', 1)
            if len(parts) == 2 and parts[0].startswith(prefix):
                name, value = parts[0][len(prefix):], parts[1]
                if value == name:
                    continue  # Preprocessor variable was not substituted
                vars_[name] = value

        return vars_

    def detect(self, c_compiler=None, cxx_compiler=None):
        c_compiler = c_compiler or os.environ.get('CC')
        cxx_compiler = cxx_compiler or os.environ.get('CXX') or c_compiler
        c_compiler = c_compiler or cxx_compiler

        logger.debug('e=%s', os.environ)
        if c_compiler is None:
            raise ConfigureError('Custom compiler was requested but not specified')

        c_compiler_path = which(c_compiler)

        clang_vars = ['__clang_major__', '__clang_minor__', '__clang_patchlevel__']
        gcc_vars = ['__GNUC__', '__GNUC_MINOR__', '__GNUC_PATCHLEVEL__']
        msvc_vars = ['_MSC_VER']
        apple_var = '__apple_build_version__'

        compiler_vars = self.get_compiler_vars(c_compiler_path, clang_vars + [apple_var] + gcc_vars + msvc_vars)

        if not compiler_vars:
            raise ConfigureError('Could not determine custom compiler version: {}'.format(c_compiler))

        def version(version_names):
            def iter_version():
                for name in version_names:
                    yield int(compiler_vars[name])

            # noinspection PyBroadException
            try:
                return list(iter_version())
            except Exception:
                return None

        clang_version = version(clang_vars)
        apple_build = apple_var in compiler_vars
        # TODO(somov): Учитывать номера версий сборки Apple компилятора Clang.
        _ = apple_build
        gcc_version = version(gcc_vars)
        msvc_version = version(msvc_vars)

        if msvc_version:
            logger.debug('Detected MSVC version %s', msvc_version)
            self.type = 'msvc'
        elif clang_version:
            logger.debug('Detected Clang version %s', clang_version)
            self.type = 'clang'
        elif gcc_version:
            logger.debug('Detected GCC version %s', gcc_version)
            # TODO(somov): Переименовать в gcc.
            self.type = 'gnu'
        else:
            raise ConfigureError('Could not determine custom compiler type: {}'.format(c_compiler))

        self.version_list = clang_version or gcc_version or msvc_version

        self.c_compiler = c_compiler_path
        self.cxx_compiler = cxx_compiler and which(cxx_compiler) or c_compiler_path


class ToolchainOptions(object):
    def __init__(self, build, detector):
        """
        :type build: Build
        """
        self.host = build.host
        self.target = build.target

        tc_json = build.params

        logger.debug('Toolchain host %s', self.host)
        logger.debug('Toolchain target %s', self.target)
        logger.debug('Toolchain json %s', DebugString(lambda: json.dumps(tc_json, indent=4, sort_keys=True)))

        self.params = tc_json['params']
        self._name = tc_json.get('name', 'theyknow')

        if detector:
            self.type = detector.type
            self.from_arcadia = False

            self.c_compiler = detector.c_compiler
            self.cxx_compiler = detector.cxx_compiler
            self.compiler_version_list = detector.version_list
            self.compiler_version = '.'.join(map(lambda part: six.ensure_str(str(part)), self.compiler_version_list))

        else:
            self.type = self.params['type']
            self.from_arcadia = True

            self.c_compiler = self.params['c_compiler']
            self.cxx_compiler = self.params['cxx_compiler']

            # TODO(somov): Требовать номер версии всегда.
            self.compiler_version = self.params.get('gcc_version') or self.params.get('version') or '0'
            self.compiler_version_list = list(map(int, self.compiler_version.split('.')))

        # 'match_root' at this point contains real name for references via toolchain
        self.name_marker = '$(%s)' % self.params.get('match_root', self._name.upper())

        self.arch_opt = self.params.get('arch_opt', [])
        self.triplet_opt = self.params.get('triplet_opt', {})
        self.target_opt = self.params.get('target_opt', [])

        # TODO(somov): Убрать чтение настройки из os.environ.
        self.werror_mode = preset('WERROR_MODE') or os.environ.get('WERROR_MODE') or self.params.get('werror_mode') or 'compiler_specific'

        # default C++ standard is set here, some older toolchains might need to redefine it in ya.conf.json
        self.cxx_std = self.params.get('cxx_std', 'c++20')

        self._env = tc_json.get('env', {})

        self.android_ndk_version = self.params.get('android_ndk_version', None)

        logger.debug('c_compiler=%s', self.c_compiler)
        logger.debug('cxx_compiler=%s', self.cxx_compiler)

        self.compiler_platform_projects = self.target.find_in_dict(self.params.get('platform'), [])

    def version_at_least(self, *args):
        return args <= tuple(self.compiler_version_list)

    def version_below(self, *args):
        return args > tuple(self.compiler_version_list)

    def version_exactly(self, *args):
        if not args or len(args) > len(self.compiler_version_list):
            return False
        for left, right in zip(args, list(self.compiler_version_list)[:len(args)]):
            if left != right:
                return False
        return True

    @property
    def is_gcc(self):
        return self.type == 'gnu'

    @property
    def is_clang(self):
        return self.type in ('clang', 'xcode')

    @property
    def is_xcode(self):
        return self.type == 'xcode'

    @property
    def is_from_arcadia(self):
        return self.from_arcadia

    @property
    def is_system_cxx(self):
        return self._name == "system_cxx"

    def get_env(self, convert_list=None):
        convert_list = convert_list or (lambda x: x)
        r = {}
        for k, v in six.iteritems(self._env):
            if isinstance(v, str):
                r[k] = v
            elif isinstance(v, list):
                r[k] = convert_list(v)
            else:
                logger.debug('Unexpected values in environment: %s', self._env)
                raise ConfigureError('Internal error')
        return r


class GnuToolchainOptions(ToolchainOptions):
    def __init__(self, build, detector):
        super(GnuToolchainOptions, self).__init__(build, detector)

        self.ar = self.params.get('ar')
        self.ar_plugin = self.params.get('ar_plugin')
        self.strip = self.params.get('strip')
        self.objcopy = self.params.get('objcopy')
        self.objdump = self.params.get('objdump')
        self.isystem = self.params.get('isystem')

        self.dwarf_tool = self.target.find_in_dict(self.params.get('dwarf_tool'))

        # TODO(somov): Унифицировать формат sys_lib
        self.sys_lib = self.params.get('sys_lib', {})
        if isinstance(self.sys_lib, dict):
            self.sys_lib = self.target.find_in_dict(self.sys_lib, [])

        self.os_sdk = preset('OS_SDK') or self._default_os_sdk()

        self.os_sdk_local = False

        if build.host.is_apple and build.target.is_apple and to_bool(preset('APPLE_SDK_LOCAL'), default=False):
            self.os_sdk_local = True

        if build.host.is_apple and build.target.is_ios and to_bool(preset('MAPSMOBI_BUILD_TARGET'), default=False):
            self.os_sdk_local = True

        if self.os_sdk == 'local':
            self.os_sdk_local = True

    def _default_os_sdk(self):
        if self.target.is_linux:
            if self.target.is_armv8:
                return 'ubuntu-16'

            if self.target.is_armv7 and self.target.armv7_float_abi == 'hard':
                return 'ubuntu-16'

            if self.target.is_armv7 and self.target.armv7_float_abi == 'softfp':
                return 'ubuntu-18'

            # Default OS SDK for Linux builds
            return LINUX_SDK_DEFAULT


class Toolchain(object):
    def __init__(self, tc, build):
        """
        :type tc: ToolchainOptions
        :type build: Build
        """
        self.tc = tc
        self.build = build
        self.platform_projects = self.tc.compiler_platform_projects

    def print_toolchain(self):
        if self.platform_projects:
            emit('COMPILER_PLATFORM', list(unique(self.platform_projects)))


class Compiler(object):
    def __init__(self, tc, compiler_variable):
        self.compiler_variable = compiler_variable
        self.tc = tc

    def print_compiler(self):
        # CLANG and CLANG_VER variables
        emit(self.compiler_variable, 'yes')
        cv = self.tc.compiler_version
        if '.' in cv:
            cv = cv[:cv.index('.')]
        emit('{}_VER'.format(self.compiler_variable), cv)
        if self.tc.is_xcode:
            emit('XCODE', 'yes')


class GnuToolchain(Toolchain):
    def __init__(self, tc, build):
        """
        :type tc: GnuToolchainOptions
        :type build: Build
        """

        def get_os_sdk(target):
            sdk_native_version = 10.11 if not preset('EXPERIMENTAL_MACOS_M1_SUPPORT') else '11.1'
            if target.is_macos:
                return '$MACOS_SDK_RESOURCE_GLOBAL/MacOSX{}.sdk'.format(sdk_native_version)
            elif target.is_yocto:
                return '$YOCTO_SDK_RESOURCE_GLOBAL'
            return '$OS_SDK_ROOT_RESOURCE_GLOBAL'

        super(GnuToolchain, self).__init__(tc, build)
        self.tc = tc

        host = build.host
        target = build.target

        self.c_flags_platform = list(tc.target_opt)

        self.default_os_sdk_root = get_os_sdk(target)

        self.env = self.tc.get_env()

        self.env_go = {}
        if self.tc.is_clang and not self.tc.is_system_cxx:
            self.env_go = {'PATH': ['{}/bin'.format(self.tc.name_marker)]}
        if self.tc.is_gcc:
            self.env_go = {'PATH': ['{}/gcc/bin'.format(self.tc.name_marker)]}
        if 'PATH' in self.env_go:
            if target.is_linux:
                self.env_go['PATH'].append('$OS_SDK_ROOT_RESOURCE_GLOBAL/usr/bin')
            elif target.is_macos:
                self.env_go['PATH'].extend([
                    '$MACOS_SDK_RESOURCE_GLOBAL/usr/bin',
                    '$GO_FAKE_XCRUN_RESOURCE_GLOBAL',
                ])

        self.swift_flags_platform = []
        self.swift_lib_path = None

        if self.tc.is_from_arcadia:
            for lib_path in build.host.library_path_variables:
                self.env.setdefault(lib_path, []).append('{}/lib'.format(self.tc.name_marker))

        swift_target = select(default=None, selectors=[
            (target.is_iossim and target.is_x86_64, 'x86_64-apple-ios{}-simulator'.format(IOS_VERSION_MIN)),
            (target.is_iossim and target.is_x86, 'i386-apple-ios{}-simulator'.format(IOS_VERSION_MIN)),
            (target.is_iossim and target.is_armv8, 'arm64-apple-ios{}-simulator'.format(IOS_VERSION_MIN)),
            (not target.is_iossim and target.is_ios and target.is_armv8, 'arm64-apple-ios9'),
            (not target.is_iossim and target.is_ios and target.is_armv7, 'armv7-apple-ios9'),
        ])
        if swift_target:
            self.swift_flags_platform += ['-target', swift_target]

        if self.tc.is_from_arcadia:
            self.swift_lib_path = select(default=None, selectors=[
                (host.is_macos and target.is_iossim, '$SWIFT_XCODE_TOOLCHAIN_ROOT_RESOURCE_GLOBAL/usr/lib/swift/iphonesimulator'),
                (host.is_macos and not target.is_iossim and target.is_ios and (target.is_armv8 or target.is_armv7), '$SWIFT_XCODE_TOOLCHAIN_ROOT_RESOURCE_GLOBAL/usr/lib/swift/iphoneos'),
            ])

        if self.tc.is_clang:
            target_triple = self.tc.triplet_opt.get(target.arch, None)
            if not target_triple:
                target_triple = select(default=None, selectors=[
                    (target.is_linux and target.is_x86_64, 'x86_64-linux-gnu'),
                    (target.is_linux and target.is_armv8, 'aarch64-linux-gnu'),
                    (target.is_linux and target.is_armv7 and target.armv7_float_abi == 'hard', 'armv7-linux-gnueabihf'),
                    (target.is_linux and target.is_armv7 and target.armv7_float_abi == 'softfp', 'armv7-linux-gnueabi'),
                    (target.is_linux and target.is_powerpc, 'powerpc64le-linux-gnu'),

                    (target.is_iossim and target.is_x86_64, 'x86_64-apple-ios{}-simulator'.format(IOS_VERSION_MIN)),
                    (target.is_iossim and target.is_x86, 'i386-apple-ios{}-simulator'.format(IOS_VERSION_MIN)),
                    (target.is_iossim and target.is_armv8, 'arm64-apple-ios{}-simulator'.format(IOS_VERSION_MIN)),
                    (not target.is_iossim and target.is_ios and target.is_armv8, 'arm64-apple-ios{}'.format(IOS_VERSION_MIN)),
                    (not target.is_iossim and target.is_ios and target.is_armv7, 'armv7-apple-ios{}'.format(IOS_VERSION_MIN)),

                    (target.is_apple and target.is_x86, 'i386-apple-darwin14'),
                    (target.is_apple and target.is_x86_64, 'x86_64-apple-darwin14'),
                    (target.is_apple and target.is_macos_arm64, 'arm64-apple-macos11'),
                    (target.is_apple and target.is_armv7, 'armv7-apple-darwin14'),
                    (target.is_apple and target.is_armv8, 'arm64-apple-darwin14'),

                    (target.is_yocto and target.is_armv7, 'arm-poky-linux-gnueabi'),

                    (target.is_android and target.is_x86, 'i686-linux-android'),
                    (target.is_android and target.is_x86_64, 'x86_64-linux-android'),
                    (target.is_android and target.is_armv7, 'armv7a-linux-androideabi'),
                    (target.is_android and target.is_armv8, 'aarch64-linux-android'),

                    (target.is_emscripten and target.is_wasm64, 'wasm64-unknown-emscripten'),
                ])

            if target.is_android:
                # Android NDK allows specification of API level in target triple, e.g.:
                # armv7a-linux-androideabi16, aarch64-linux-android21
                target_triple += str(target.android_api)

            if target_triple:
                self.c_flags_platform.append('--target={}'.format(target_triple))

        if self.tc.isystem:
            for root in list(self.tc.isystem):
                self.c_flags_platform.extend(['-isystem', root])

        if target.is_android:
            self.c_flags_platform.extend(['-isystem', '{}/sources/cxx-stl/llvm-libc++abi/include'.format(self.tc.name_marker)])

        if target.is_cortex_a9:
            self.c_flags_platform.append('-mcpu=cortex-a9')

        if target.is_cortex_a35:
            self.c_flags_platform.append('-mcpu=cortex-a35')

        elif target.is_cortex_a53:
            self.c_flags_platform.append('-mcpu=cortex-a53')

        elif target.is_cortex_m4:
            self.c_flags_platform.append('-mcpu=cortex-m4 -mfpu=fpv4-sp-d16')

        elif target.is_cortex_m7:
            self.c_flags_platform.append('-mcpu=cortex-m7 -mfpu=fpv5-sp-d16')

        elif target.is_cortex_m23:
            self.c_flags_platform.append('-mcpu=cortex-m23')

        elif target.is_cortex_m33:
            self.c_flags_platform.append('-mcpu=cortex-m33 -mfpu=fpv5-sp-d16')

        elif target.is_armv7_neon:
            self.c_flags_platform.append('-mfpu=neon')

        elif target.is_arm968e_s:
            self.c_flags_platform.append('-march=armv5te -mcpu=arm968e-s -mthumb-interwork -mlittle-endian')

        if (target.is_armv7 or target.is_armv8m or target.is_armv7em or target.is_armv5te) and build.is_size_optimized:
            # Enable ARM Thumb2 variable-length instruction encoding
            # to reduce code size
            self.c_flags_platform.append('-mthumb')

        if target.is_arm or target.is_powerpc:
            # On linux, ARM and PPC default to unsigned char
            # However, Arcadia requires char to be signed
            self.c_flags_platform.append('-fsigned-char')

        if target.is_rv32imc:
            self.c_flags_platform.append('-march=rv32imc')

        if self.tc.is_clang or self.tc.is_gcc and self.tc.version_at_least(8, 2):
            target_flags = select(default=[], selectors=[
                (target.is_linux and target.is_power8le, ['-mcpu=power8', '-mtune=power8', '-maltivec']),
                (target.is_linux and target.is_power9le, ['-mcpu=power9', '-mtune=power9', '-maltivec']),
                (target.is_linux and target.is_armv8, ['-march=armv8a']),
                (target.is_macos, ['-mmacosx-version-min={}'.format(MACOS_VERSION_MIN)]),
                (target.is_ios and not target.is_iossim, ['-mios-version-min={}'.format(IOS_VERSION_MIN)]),
                (target.is_iossim, ['-mios-simulator-version-min={}'.format(IOS_VERSION_MIN)]),
                (target.is_android and target.is_armv7, ['-march=armv7-a', '-mfloat-abi=softfp']),
                (target.is_android and target.is_armv8, ['-march=armv8-a']),
                (target.is_yocto and target.is_armv7, ['-march=armv7-a', '-mfpu=neon', '-mfloat-abi=hard', '-mcpu=cortex-a9', '-O1'])
            ])

            if target_flags:
                self.c_flags_platform.extend(target_flags)

            if target.is_ios:
                self.c_flags_platform.append('-D__IOS__=1')

            if target.is_apple:
                self.setup_apple_sdk(target)

            if self.tc.is_from_arcadia or self.tc.is_system_cxx:
                if target.is_linux:
                    if not tc.os_sdk_local:
                        self.setup_sdk(project='build/platform/linux_sdk', var='$OS_SDK_ROOT_RESOURCE_GLOBAL')

                    if target.is_x86_64:
                        if host.is_linux and not self.tc.is_gcc:
                            self.setup_tools(project='build/platform/linux_sdk', var='$OS_SDK_ROOT_RESOURCE_GLOBAL', bin='usr/bin', ldlibs='usr/lib/x86_64-linux-gnu')
                        elif host.is_macos:
                            self.setup_tools(project='build/platform/binutils', var='$BINUTILS_ROOT_RESOURCE_GLOBAL', bin='x86_64-linux-gnu/bin', ldlibs=None)
                    elif target.is_powerpc:
                        self.setup_tools(project='build/platform/linux_sdk', var='$OS_SDK_ROOT_RESOURCE_GLOBAL', bin='usr/bin', ldlibs='usr/x86_64-linux-gnu/powerpc64le-linux-gnu/lib')
                    elif target.is_armv8:
                        self.setup_tools(project='build/platform/linux_sdk', var='$OS_SDK_ROOT_RESOURCE_GLOBAL', bin='usr/bin', ldlibs='usr/lib/x86_64-linux-gnu')

                if target.is_yocto:
                    self.setup_sdk(project='build/platform/yocto_sdk/yocto_sdk', var='${YOCTO_SDK_ROOT_RESOURCE_GLOBAL}')

    def setup_apple_sdk(self, target):
        if not self.tc.os_sdk_local:
            self.setup_apple_arcadia_sdk(target)
        else:
            self.setup_apple_local_sdk(target)

    def setup_apple_arcadia_sdk(self, target):
        if target.is_ios:
            self.setup_xcode_sdk(project='build/internal/platform/ios_sdk', var='${IOS_SDK_ROOT_RESOURCE_GLOBAL}')
            self.platform_projects.append('build/internal/platform/macos_system_stl')
        if target.is_macos:
            self.setup_xcode_sdk(project='build/internal/platform/macos_sdk', var='${MACOS_SDK_RESOURCE_GLOBAL}')

    def setup_apple_local_sdk(self, target):
        def get_output(*args):
            return six.ensure_str(subprocess.check_output(tuple(args))).strip()

        def get_sdk_root(sdk):
            root = self.env.get('SDKROOT')
            if root not in (None, '', ['']):
                return root

            root = os.environ.get('SDKROOT')
            if root:
                return root

            return get_output('xcrun', '-sdk', sdk, '--show-sdk-path')

        if target.is_iossim:
            self.env['SDKROOT'] = get_sdk_root('iphonesimulator')
        elif target.is_ios:
            self.env['SDKROOT'] = get_sdk_root('iphoneos')
        elif target.is_macos:
            self.env['SDKROOT'] = get_sdk_root('macosx')

    def setup_sdk(self, project, var):
        self.platform_projects.append(project)
        self.c_flags_platform.append('--sysroot={}'.format(var))
        self.swift_flags_platform += ['-sdk', var]

    def setup_xcode_sdk(self, project, var):
        self.platform_projects.append(project)
        self.c_flags_platform.append('-isysroot')
        self.c_flags_platform.append(var)
        self.swift_flags_platform += ['-sdk', var]

    # noinspection PyShadowingBuiltins
    def setup_tools(self, project, var, bin, ldlibs):
        self.platform_projects.append(project)
        self.c_flags_platform.append('-B{}/{}'.format(var, bin))
        if ldlibs:
            for lib_path in self.build.host.library_path_variables:
                self.env.setdefault(lib_path, []).append('{}/{}'.format(var, ldlibs))

    def print_toolchain(self):
        super(GnuToolchain, self).print_toolchain()

        emit('TOOLCHAIN_ENV', format_env(self.env, list_separator=':'))
        emit('_GO_TOOLCHAIN_ENV_PATH', format_env(self.env_go, list_separator=':'))
        emit('C_FLAGS_PLATFORM', self.c_flags_platform)
        emit('SWIFT_FLAGS_PLATFORM', self.swift_flags_platform)
        emit('SWIFT_LD_FLAGS', '-L{}'.format(self.swift_lib_path) if self.swift_lib_path else '')

        emit('PERL_SDK', preset('OS_SDK') or self.tc.os_sdk)
        if preset('OS_SDK') is None:
            emit('OS_SDK', self.tc.os_sdk)
        emit('OS_SDK_ROOT', None if self.tc.os_sdk_local else self.default_os_sdk_root)


class GnuCompiler(Compiler):
    def __init__(self, tc, build):
        """
        :type tc: GnuToolchainOptions
        :type build: Build
        """
        compiler_variable = 'CLANG' if tc.is_clang else 'GCC'
        super(GnuCompiler, self).__init__(tc, compiler_variable)

        self.build = build
        self.host = self.build.host
        self.target = self.build.target
        self.tc = tc

        self.c_foptions = [
            # Enable C++ exceptions (and allow them to be throw through pure C code)
            '-fexceptions',
            # Enable standard-conforming behavior and generate duplicate symbol error in case of duplicated global constants.
            # See: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=85678#c0
            '-fno-common',

            # Split all functions and data into separate sections for DCE and ICF linker passes
            '-ffunction-sections',
            '-fdata-sections'
        ]

        if self.tc.is_clang and self.target.is_linux:
            # Use .init_array instead of .ctors (default for old clang versions)
            # See: https://maskray.me/blog/2021-11-07-init-ctors-init-array
            self.c_foptions.append('-fuse-init-array')

        if self.target.is_xtensa:
            self.c_foptions.append('-mlongcalls')

        if self.tc.is_clang:
            self.c_foptions += [
                # Set up output colorization
                '-fcolor-diagnostics',
                # Enable aligned allocation
                '-faligned-allocation',
                '-fdebug-default-version=4',
            ]
        elif self.tc.is_gcc:
            if self.target.is_xtensa or self.target.is_tc32:
                # Xtensa and tc32 toolchains does not support this flag
                pass
            else:
                self.c_foptions += [
                    # Set up output colorization
                    '-fdiagnostics-color=always',
                    # It looks like there is no way to enable aligned allocation in gcc
                ]

        self.c_warnings = [
            # Enable default warnings subset
            '-Wall',
            '-Wextra',
        ]
        self.cxx_warnings = [
            # Issue a warning if certain overload is hidden due to inheritance
            '-Woverloaded-virtual',
        ]

        # Disable some warnings which will fail compilation at the time
        self.c_warnings += [
            '-Wno-parentheses',
        ]

        self.c_defines = ['${hide:CPP_FAKEID}']
        if self.target.is_android:
            self.c_defines.append('${hide:ANDROID_FAKEID}')

        self.c_defines.extend([
            '-DARCADIA_ROOT=${ARCADIA_ROOT}',
            '-DARCADIA_BUILD_ROOT=${ARCADIA_BUILD_ROOT}',
        ])

        self.c_defines.extend([
            '-D_THREAD_SAFE', '-D_PTHREADS', '-D_REENTRANT',
            '-D_LARGEFILE_SOURCE', '-D__STDC_CONSTANT_MACROS', '-D__STDC_FORMAT_MACROS',
        ])

        if self.target.is_macos and self.tc.version_below(17):
            # OS-versioning macros for Darwin-based OSes were generalized in
            # https://github.com/llvm/llvm-project/commit/c8e2dd8c6f490b68e41fe663b44535a8a21dfeab
            #
            # This PR was released in llvm-17. Provide similar behaviour manually.
            self.c_defines.append("-D__ENVIRONMENT_OS_VERSION_MIN_REQUIRED__=" + MACOS_VERSION_MIN_AS_INT)

        if not self.target.is_android:
            # There is no usable _FILE_OFFSET_BITS=64 support in Androids until API 21. And it's incomplete until at least API 24.
            # https://android.googlesource.com/platform/bionic/+/master/docs/32-bit-abi.md
            # Arcadia have API 16 for 32-bit Androids.
            self.c_defines.append('-D_FILE_OFFSET_BITS=64')

        if self.target.is_linux or self.target.is_android or self.target.is_cygwin or self.target.is_none:
            self.c_defines.append('-D_GNU_SOURCE')

        if self.tc.is_clang and self.target.is_linux and self.target.is_x86_64:
            self.c_defines.append('-D_YNDX_LIBUNWIND_ENABLE_EXCEPTION_BACKTRACE')

        self.c_flags = ['$CL_DEBUG_INFO', '$CL_DEBUG_INFO_DISABLE_CACHE__NO_UID__']
        self.c_flags += self.tc.arch_opt + ['-pipe']

        self.sfdl_flags = ['-E', '-C', '-x', 'c++']

        if self.target.is_x86:
            self.c_flags.append('-m32')
        if self.target.is_x86_64:
            self.c_flags.append('-m64')

        if self.target.is_wasm64:
            # WebAssembly-specific exception handling flags
            self.c_foptions += [
                '-fwasm-exceptions',
            ]

        self.debug_info_flags = ['-g']
        if self.target.is_linux:
            self.debug_info_flags.append('-ggnu-pubnames')

        self.cross_suffix = '' if is_positive('FORCE_NO_PIC') else '.pic'

        self.optimize = None

        self.configure_build_type()

        if self.tc.is_clang:
            self.sfdl_flags.append('-Qunused-arguments')

            self.c_warnings += [
                '-Wno-implicit-const-int-float-conversion',
                # For nvcc to accept the above.
                '-Wno-unknown-warning-option',
            ]

            self.cxx_warnings += [
                '-Wimport-preprocessor-directive-pedantic',
                '-Wno-ambiguous-reversed-operator',
                '-Wno-defaulted-function-deleted',
                '-Wno-deprecated-anon-enum-enum-conversion',
                '-Wno-deprecated-enum-enum-conversion',
                '-Wno-deprecated-enum-float-conversion',
                '-Wno-deprecated-volatile',
                '-Wno-pessimizing-move',
                '-Wno-return-std-move',
                '-Wno-undefined-var-template',
            ]

        elif self.tc.is_gcc:
            if self.target.is_xtensa:
                # Xtensa toolchain does not support this flags
                pass
            else:
                self.c_foptions.append('-fno-delete-null-pointer-checks')
                self.c_foptions.append('-fabi-version=8')

    def configure_build_type(self):
        if self.build.is_valgrind:
            self.c_defines.append('-DWITH_VALGRIND=1')

        if self.build.is_debug:
            self.c_foptions.append('$FSTACK')

        if self.build.is_fast_debug:
            self.c_flags.append('-Og')

        if self.build.is_release:
            self.c_flags.append('$OPTIMIZE')
            if self.build.is_size_optimized:
                # -Oz is clang's more size-aggressive version of -Os
                # For ARM specifically, clang -Oz is on par with gcc -Os:
                # https://github.com/android/ndk/issues/133#issuecomment-365763507
                if self.tc.is_clang:
                    self.optimize = '-Oz'
                else:
                    self.optimize = '-Os'

                # Generate sections with address significance tables for ICF linker pass
                if self.tc.is_clang:
                    self.c_foptions.extend(['-faddrsig'])
            else:
                self.optimize = '-O3'

        if self.build.with_ndebug:
            self.c_defines.append('-DNDEBUG')
        else:
            self.c_defines.append('-UNDEBUG')

        if self.build.profiler_type in (Profiler.Generic, Profiler.GProf):
            self.c_foptions.append('-fno-omit-frame-pointer')

        if self.build.profiler_type == Profiler.GProf:
            self.c_flags.append('-pg')

    def print_compiler(self):
        super(GnuCompiler, self).print_compiler()

        emit('C_COMPILER', '"{}"'.format(self.tc.c_compiler))
        emit('C_COMPILER_OLD_UNQUOTED', self.tc.c_compiler)
        emit('C_COMPILER_OLD', '${quo:C_COMPILER_OLD_UNQUOTED}')
        emit('OPTIMIZE', self.optimize)
        emit('WERROR_MODE', self.tc.werror_mode)
        emit('_C_FLAGS', self.c_flags)
        emit('_C_FOPTIONS', self.c_foptions)
        emit('_STD_CXX_VERSION', preset('USER_STD_CXX_VERSION') or self.tc.cxx_std)
        append('C_DEFINES', self.c_defines)
        append('C_WARNING_OPTS', self.c_warnings)
        append('CXX_WARNING_OPTS', self.cxx_warnings)
        emit('CXX_COMPILER', '"{}"'.format(self.tc.cxx_compiler))
        emit('CXX_COMPILER_OLD_UNQUOTED', format(self.tc.cxx_compiler))
        emit('CXX_COMPILER_OLD', '${quo:CXX_COMPILER_OLD_UNQUOTED}')
        # TODO(somov): Убрать чтение настройки из os.environ
        emit('USE_ARC_PROFILE', 'yes' if preset('USE_ARC_PROFILE') or os.environ.get('USE_ARC_PROFILE') else 'no')
        emit('DEBUG_INFO_FLAGS', self.debug_info_flags)

        if self.build.is_coverage:
            emit('_IS_COVERAGE', 'yes')

        if self.tc.is_clang and self.tc.version_at_least(9):
            emit('_HAS_TIME_TRACE', 'yes')

        print('@import "${CONF_ROOT}/conf/compilers/gnu_compiler.conf"')


class SwiftCompiler(object):
    def __init__(self, build):
        self.host = build.host
        self.compiler = None

    def configure(self):
        if self.host.is_macos:
            self.compiler = '$SWIFT_XCODE_TOOLCHAIN_ROOT_RESOURCE_GLOBAL/usr/bin/swiftc'

    def print_compiler(self):
        emit('SWIFT_COMPILER', self.compiler or '')


class Linker(object):
    BFD = 'bfd'
    LLD = 'lld'
    GOLD = 'gold'

    def __init__(self, tc, build):
        """
        :type tc: ToolchainOptions
        :type build: Build
        """
        self.tc = tc
        self.build = build
        self.type = self._get_default_linker_type()

    def _get_default_linker_type(self):
        if not self.tc.is_from_arcadia or is_positive('EXPORT_CMAKE'):
            # External (e.g. system) toolchain: disable linker selection logic
            return None

        if self.build.target.is_android:
            # Android toolchain is NDK, LLD works on all supported platforms
            return Linker.LLD

        elif self.build.target.is_linux or self.build.target.is_macos or self.build.target.is_ios or self.build.target.is_wasm:
            return Linker.LLD

        # There is no linker choice on Windows (link.exe)
        return None

    def print_linker(self):
        self._print_linker_selector()

    def _print_linker_selector(self):
        # if self.type is None then _DEFAULT_LINKER is set to empty string value
        emit('_DEFAULT_LINKER_ID', self.type)


class LD(Linker):
    def __init__(self, tc, build):
        """
        :type tc: GnuToolchainOptions
        :type build: Build
        """
        super(LD, self).__init__(tc, build)

        self.build = build
        self.host = self.build.host
        self.target = self.build.target
        self.tc = tc

        target = self.target

        self.ar = preset('AR') or self.tc.ar
        self.ar_plugin = self.tc.ar_plugin
        self.strip = self.tc.strip
        self.objcopy = self.tc.objcopy
        self.objdump = self.tc.objdump

        self.musl = Setting('MUSL', convert=to_bool)

        if self.ar is None:
            if self.tc.is_from_arcadia:
                if self.tc.is_clang:
                    self.ar = '{}/bin/llvm-ar'.format(self.tc.name_marker)
                if self.tc.is_gcc:
                    self.ar = '{}/gcc/bin/gcc-ar'.format(self.tc.name_marker)
            else:
                self.ar = 'ar'

        self.ar_type = 'GNU_AR'
        self.llvm_ar_format = 'None'

        if 'libtool' in self.ar:
            self.ar_type = 'LIBTOOL'
        elif 'llvm-ar' in self.ar:
            self.ar_type = 'LLVM_AR'
            if target.is_apple:
                self.llvm_ar_format = "darwin"
            else:
                self.llvm_ar_format = "gnu"

        self.ld_flags = []

        if self.musl.value:
            self.ld_flags.extend(['-Wl,--no-as-needed'])
        elif target.is_linux:
            self.ld_flags.extend(['-ldl', '-lrt', '-Wl,--no-as-needed'])
            if self.tc.is_gcc:
                self.ld_flags.extend(('-Wl,-Bstatic', '-latomic', '-Wl,-Bdynamic'))
        elif target.is_android:
            self.ld_flags.extend(['-ldl', '-Wl,--no-as-needed'])

        self.ld_sdk = select(default=None, selectors=[
            # -platform_version <platform> <min_version> <sdk_version>
            (target.is_macos, '-Wl,-platform_version,macos,{MACOS_VERSION_MIN},11.0'.format(MACOS_VERSION_MIN=MACOS_VERSION_MIN)),
            (not target.is_iossim and target.is_ios, '-Wl,-platform_version,ios,{IOS_VERSION_MIN},13.1'.format(IOS_VERSION_MIN=IOS_VERSION_MIN)),
            (target.is_iossim, '-Wl,-platform_version,ios-simulator,{IOS_VERSION_MIN},14.5'.format(IOS_VERSION_MIN=IOS_VERSION_MIN)),
        ])

        if self.build.profiler_type == Profiler.GProf:
            self.ld_flags.append('-pg')

        if self.ld_sdk:
            self.ld_flags.append(self.ld_sdk)

        self.sys_lib = self.tc.sys_lib

        if target.is_android:
            # Use toolchain defaults to link with libunwind/clang_rt.builtins
            self.use_stdlib = '-nostdlib++'

    def print_linker(self):
        super(LD, self).print_linker()

        emit('AR_TOOL', self.ar)
        emit('AR_TYPE', self.ar_type)
        emit('_LD_LLVM_AR_FORMAT', self.llvm_ar_format)
        emit('_LD_AR_PLUGIN', self.ar_plugin or 'None')

        emit('STRIP_TOOL_VENDOR', self.strip)
        emit('OBJCOPY_TOOL_VENDOR', self.objcopy)
        emit('OBJDUMP_TOOL_VENDOR', self.objdump)

        emit('_LD_FLAGS', self.ld_flags)
        emit('_LD_SYS_LIB', self.sys_lib)
        emit('LD_SDK_VERSION', self.ld_sdk)

        dwarf_tool = self.tc.dwarf_tool
        if dwarf_tool is None and self.tc.is_clang and (self.target.is_macos or self.target.is_ios):
            dsymutil = '{}/bin/{}dsymutil'.format(self.tc.name_marker, '' if self.tc.version_at_least(7) else 'llvm-')
            dwarf_tool = '${YMAKE_PYTHON} ${input:"build/scripts/run_llvm_dsymutil.py"} ' + dsymutil
            if self.tc.version_at_least(5, 0):
                dwarf_tool += ' -flat'

        if dwarf_tool is not None:
            emit('DWARF_TOOL', dwarf_tool)

        arch_flag = '--arch={arch}'.format(arch=self.target.os_compat)
        emit('_LD_ARCH_FLAG', arch_flag)

        print('@import "${CONF_ROOT}/conf/linkers/ld.conf"')


class MSVCToolchainOptions(ToolchainOptions):
    def __init__(self, build, detector):
        super(MSVCToolchainOptions, self).__init__(build, detector)

        # C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Tools\MSVC\14.14.26428
        self.vc_root = None

        # C:\Program Files (x86)\Windows Kits\10\Include\10.0.14393.0
        self.kit_includes = None

        # C:\Program Files (x86)\Windows Kits\10\Lib\10.0.14393.0
        self.kit_libs = None

        self.under_wine_compiler = self.params.get('wine', False)
        self.under_wine_tools = not build.host.is_windows
        self.under_wine_link = self.under_wine_tools
        self.under_wine_lib = self.under_wine_tools
        self.system_msvc = self.params.get('system_msvc', False)
        self.ide_msvs = self.params.get('ide_msvs', False)
        self.use_clang = self.params.get('use_clang', False)
        self.use_msvc_linker = is_positive('USE_MSVC_LINKER')
        self.use_arcadia_toolchain = self.params.get('use_arcadia_toolchain', False)

        self.sdk_version = None

        if self.ide_msvs:
            bindir = '$(VC_ExecutablePath_x64_x64)\\'
            self.c_compiler = bindir + 'cl.exe'
            self.cxx_compiler = self.c_compiler

            self.link = bindir + 'link.exe'
            self.lib = bindir + 'lib.exe'
            self.masm_compiler = bindir + 'ml64.exe'

            self.vc_root = None

            sdk_dir = '$(WindowsSdkDir)'
            self.sdk_version = '$(WindowsTargetPlatformVersion)'
            self.kit_includes = win_path_fix(os.path.join(sdk_dir, 'Include', self.sdk_version))
            self.kit_libs = win_path_fix(os.path.join(sdk_dir, 'Lib', self.sdk_version))

        elif detector:
            self.use_clang = is_positive('USE_CLANG_CL')

            self.masm_compiler = which('ml64.exe')
            self.link = which('link.exe')
            self.lib = which('lib.exe')

            sdk_dir = os.environ.get('WindowsSdkDir')
            self.sdk_version = os.environ.get('WindowsSDKVersion').replace('\\', '')
            vc_install_dir = os.environ.get('VCToolsInstallDir')

            if any([x is None for x in (sdk_dir, self.sdk_version, vc_install_dir)]):
                raise ConfigureError('No %WindowsSdkDir%, %WindowsSDKVersion% or %VCINSTALLDIR% present. Please, run vcvars64.bat to setup preferred environment.')

            self.vc_root = os.path.normpath(vc_install_dir)
            self.kit_includes = win_path_fix(os.path.normpath(os.path.join(sdk_dir, 'Include', self.sdk_version)))
            self.kit_libs = win_path_fix(os.path.normpath(os.path.join(sdk_dir, 'Lib', self.sdk_version)))

            # TODO(somov): Определять автоматически self.version в этом случае

        else:
            if self.version_at_least(2019):
                self.sdk_version = '10.0.18362.0'
                if is_positive('MSVC20'):  # XXX: temporary flag, remove after DTCC-123 is completed
                    self.cxx_std = 'c++latest'
            else:
                self.sdk_version = '10.0.16299.0'
            sdk_dir = '$WINDOWS_KITS_RESOURCE_GLOBAL'

            self.vc_root = self.name_marker if not self.use_clang else '$MSVC_FOR_CLANG_RESOURCE_GLOBAL'
            self.kit_includes = win_path_fix(os.path.join(sdk_dir, 'Include', self.sdk_version))
            self.kit_libs = win_path_fix(os.path.join(sdk_dir, 'Lib', self.sdk_version))

            bindir = win_path_fix(os.path.join(self.vc_root, 'bin', 'Hostx64'))

            tools_name = select(selectors=[
                (build.target.is_x86, 'x86'),
                (build.target.is_x86_64, 'x64'),
                (build.target.is_armv7, 'arm'),
            ])

            asm_name = select(selectors=[
                (build.target.is_x86, 'ml.exe'),
                (build.target.is_x86_64, 'ml64.exe'),
                (build.target.is_armv7, 'armasm.exe'),
            ])

            self.masm_compiler = win_path_fix(os.path.join(bindir, tools_name, asm_name))

            if self.use_clang and not self.use_msvc_linker:
                self.link = self.host.exe(self.name_marker, "bin", "lld-link")
                self.under_wine_link = False
            else:
                self.link = win_path_fix(os.path.join(bindir, tools_name, 'link.exe'))
                self.under_wine_link = self.under_wine_tools

            if self.use_clang:
                self.lib = self.host.exe(self.name_marker, "bin", "llvm-lib")
                self.under_wine_lib = False
            else:
                self.lib = win_path_fix(os.path.join(bindir, tools_name, 'lib.exe'))
                self.under_wine_lib = self.under_wine_tools

            logger.debug("link {}; lib {}".format(self.link, self.lib))


class MSVC(object):
    def __init__(self, tc, build):
        """
        :type tc: MSVCToolchainOptions
        :type build: Build
        """
        if not isinstance(tc, MSVCToolchainOptions):
            raise TypeError('Got {} ({}) instead of an MSVCToolchainOptions'.format(tc, type(tc)))

        self.build = build
        self.tc = tc


class MSVCToolchain(MSVC, Toolchain):
    def __init__(self, tc, build):
        """
        :type tc: MSVCToolchainOptions
        :param build: Build
        """
        Toolchain.__init__(self, tc, build)
        MSVC.__init__(self, tc, build)

        if self.tc.from_arcadia and not self.tc.ide_msvs:
            self.platform_projects.append('build/internal/platform/msvc')
            self.platform_projects.append('build/platform/wine')

    def print_toolchain(self):
        super(MSVCToolchain, self).print_toolchain()

        emit('TOOLCHAIN_ENV', format_env(self.tc.get_env(), list_separator=';'))

        if self.tc.sdk_version:
            emit('WINDOWS_KITS_VERSION', self.tc.sdk_version)

        if self.tc.under_wine_tools:
            emit('_UNDER_WINE_TOOLS', 'yes')
        if self.tc.under_wine_link:
            emit('_UNDER_WINE_LINK', 'yes')
        if self.tc.under_wine_lib:
            emit('_UNDER_WINE_LIB', 'yes')
        if self.tc.under_wine_compiler:
            emit('_UNDER_WINE_COMPILER', 'yes')
        if self.tc.use_clang:
            emit('CLANG_CL', 'yes')
        if self.tc.ide_msvs:
            emit('IDE_MSVS', 'yes')
        if self.tc.use_arcadia_toolchain:
            emit('USE_ARCADIA_TOOLCHAIN', 'yes')
        emit('_MSVC_TC_KIT_LIBS', self.tc.kit_libs)
        emit('_MSVC_TC_VC_ROOT', self.tc.vc_root)

        print('@import "${CONF_ROOT}/conf/toolchains/msvc_toolchain.conf"')


class MSVCCompiler(MSVC, Compiler):
    def __init__(self, tc, build):
        Compiler.__init__(self, tc, 'MSVC')
        MSVC.__init__(self, tc, build)

    def print_compiler(self):
        super(MSVCCompiler, self).print_compiler()

        target = self.build.target

        warns_enabled = [
            4018,  # 'expression' : signed/unsigned mismatch
            4265,  # 'class' : class has virtual functions, but destructor is not virtual
            4296,  # 'operator' : expression is always false
            4431,  # missing type specifier - int assumed
        ]
        warns_as_error = [
            4013,  # 'function' undefined; assuming extern returning int
        ]
        warns_disabled = [
            # While this warning corresponds to enabled-by-default -Wmacro-redefinition,
            # it floods clog with abundant amount of log lines,
            # as yvals_core.h from Windows SDK redefines certain
            # which macros logically belong to libcxx
            4005,  # '__cpp_lib_*': macro redefinition.

            # Ne need to recheck this, but it looks like _CRT_USE_BUILTIN_OFFSETOF still makes sense
            4117,  # macro name '_CRT_USE_BUILTIN_OFFSETOF' is reserved, '#define' ignored

            4127,  # conditional expression is constant
            4200,  # nonstandard extension used : zero-sized array in struct/union
            4201,  # nonstandard extension used : nameless struct/union
            4351,  # elements of array will be default initialized
            4355,  # 'this' : used in base member initializer list
            4503,  # decorated name length exceeded, name was truncated
            4510,  # default constructor could not be generated
            4511,  # copy constructor could not be generated
            4512,  # assignment operator could not be generated
            4554,  # check operator precedence for possible error; use parentheses to clarify precedence
            4610,  # 'object' can never be instantiated - user defined constructor required
            4706,  # assignment within conditional expression
            4800,  # forcing value to bool 'true' or 'false' (performance warning)
            4996,  # The POSIX name for this item is deprecated
            4714,  # function marked as __forceinline not inlined
            4197,  # 'TAtomic' : top-level volatile in cast is ignored
            4245,  # 'initializing' : conversion from 'int' to 'ui32', signed/unsigned mismatch
            4324,  # 'ystd::function<void (uint8_t *)>': structure was padded due to alignment specifier
            5033,  # 'register' is no longer a supported storage class
        ]

        defines = [
            '${hide:CPP_FAKEID}',
            # FIXME: This is quick fix to let catboost build from MSVS IDE
            # This place is questionable overall, see YMAKE-437
            '/DARCADIA_ROOT=' + ('${ARCADIA_ROOT}' if not self.tc.ide_msvs else '.'),
            '/DARCADIA_BUILD_ROOT=' + ('${ARCADIA_BUILD_ROOT}' if not self.tc.ide_msvs else '.'),
            '/DWIN32',
            '/D_WIN32',
            '/D_WINDOWS',
            # Define _CRT_*_NO_WARNINGS macros to prevent ucrt from issuing a warning whenever
            # a POSIX-style function is used instead of the alternative Microsoft suggests as a secure / standard replacement
            # (e. g. `strncpy()` instead of `strncpy_s()`, `access()` instead of `_access()`)
            # For details see:
            # https://docs.microsoft.com/en-us/cpp/c-runtime-library/security-features-in-the-crt
            '/D_CRT_SECURE_NO_WARNINGS',
            '/D_CRT_NONSTDC_NO_WARNINGS',
            # Math constants (such as M_PI, M_E, M_SQRT2) are not defined in standard C / C++
            # In order to get them defined by Windows ucrt library,
            # you must first define _USE_MATH_DEFINES before #including <cmath> or math.h>.
            # (NB: glibc defines these macros whenever _XOPEN_SOURCE is defined)
            '/D_USE_MATH_DEFINES',
            '/D__STDC_CONSTANT_MACROS',
            '/D__STDC_FORMAT_MACROS',
            '/D_USING_V110_SDK71_',
            # Below defines are covered at
            # https://docs.microsoft.com/en-us/windows/win32/winprog/using-the-windows-headers#faster-builds-with-smaller-header-files
            # Exclude APIs such as Cryptography, DDE, RPC, Shell, and Windows Sockets (while including <windows.h>)
            '/DWIN32_LEAN_AND_MEAN',
            # Define NOMINMAX to avoid min() and max() macros definition (while including <windows.h>)
            '/DNOMINMAX',
        ]

        cxx_defines = [
            # Use builtin offsetof implementation
            # instead of a crutcy macro defined in ucrt/stddef.h.
            # The latter can not be used in constexpr statements.
            '/D_CRT_USE_BUILTIN_OFFSETOF',
        ]

        if target.is_x86_64:
            defines.extend(('/D_WIN64', '/DWIN64'))

        if target.is_armv7:
            defines.extend(('/D_ARM_WINAPI_PARTITION_DESKTOP_SDK_AVAILABLE', '/D__arm__'))

        winapi_unicode = False

        flags = [
            '/nologo', '/Zm500', '/GR', '/bigobj', '/FC', '/EHs', '/errorReport:prompt', '$MSVC_INLINE_FLAG', '/utf-8',
            # enable standard conforming mode
            '/permissive-'
        ]
        flags += self.tc.arch_opt

        c_warnings = ['/we{}'.format(code) for code in warns_as_error]
        c_warnings += ['/w1{}'.format(code) for code in warns_enabled]
        c_warnings += ['/wd{}'.format(code) for code in warns_disabled]
        cxx_warnings = []

        if self.tc.use_clang:
            if not self.tc.is_system_cxx:
                flags += [
                    # Allow <windows.h> to be included via <Windows.h> in case-sensitive file-systems.
                    '-fcase-insensitive-paths',
                ]
            flags += [
                # At the time clang-cl identifies itself as MSVC 19.11:
                # (actual value can be found in clang/lib/Driver/ToolChains/MSVC.cpp, the syntax would be like
                # ```
                # MSVT = VersionTuple(19, 11);
                # ```
                #
                # We override this value to match current value of the actual MSVC being used.
                '-fms-compatibility-version=19.21',
                # for msvc compatibility
                # https://clang.llvm.org/docs/UsersManual.html#microsoft-extensions
                '-fdelayed-template-parsing',
            ]
            if target.is_x86:
                flags.append('-m32')
            elif target.is_x86_64:
                flags.append('-m64')

            c_warnings.extend((
                '-Wno-format',
                '-Wno-parentheses',
                '-Wno-unknown-warning-option',
            ))

            cxx_warnings += [
                '-Wimport-preprocessor-directive-pedantic',
                '-Woverloaded-virtual',
                '-Wno-ambiguous-reversed-operator',
                '-Wno-defaulted-function-deleted',
                '-Wno-deprecated-anon-enum-enum-conversion',
                '-Wno-deprecated-enum-enum-conversion',
                '-Wno-deprecated-enum-float-conversion',
                '-Wno-deprecated-volatile',
                '-Wno-undefined-var-template',
            ]

            if self.tc.ide_msvs:
                cxx_warnings += [
                    '-Wno-unused-command-line-argument',
                ]

        defines.append('/D_WIN32_WINNT={0}'.format(WINDOWS_VERSION_MIN))

        if winapi_unicode:
            defines += ['/DUNICODE', '/D_UNICODE']
        else:
            defines += ['/D_MBCS']

        vc_include = win_path_fix(os.path.join(self.tc.vc_root, 'include')) if not self.tc.ide_msvs else "$(VC_VC_IncludePath.Split(';')[0].Replace('\\','/'))"

        if not self.tc.ide_msvs:
            def include_flag(path):
                return '{flag}"{path}"'.format(path=path, flag='/I ' if not self.tc.use_clang else '-imsvc')

            for name in ('shared', 'ucrt', 'um', 'winrt'):
                flags.append(include_flag(win_path_fix(os.path.join(self.tc.kit_includes, name))))
            flags.append(include_flag(vc_include))

        if self.tc.use_clang:
            emit('CLANG_CL', 'yes')
        if self.tc.ide_msvs:
            emit('IDE_MSVS', 'yes')
        if self.tc.use_arcadia_toolchain:
            emit('USE_ARCADIA_TOOLCHAIN', 'yes')

        emit('CXX_COMPILER_UNQUOTED', '"{}"'.format(self.tc.cxx_compiler))
        emit('CXX_COMPILER_OLD_UNQUOTED', self.tc.cxx_compiler)
        emit('C_COMPILER_UNQUOTED', '"{}"'.format(self.tc.c_compiler))
        emit('C_COMPILER_OLD_UNQUOTED', self.tc.c_compiler)
        emit('MASM_COMPILER_UNQUOTED', '"{}"'.format(self.tc.masm_compiler))
        emit('MASM_COMPILER_OLD_UNQUOTED', self.tc.masm_compiler)
        append('C_DEFINES', defines)
        append('C_WARNING_OPTS', c_warnings)
        emit('_CXX_DEFINES', cxx_defines)
        append('CXX_WARNING_OPTS', cxx_warnings)

        if self.build.is_release:
            emit('CFLAGS_PER_TYPE', '$CFLAGS_RELEASE')
        if self.build.is_debug:
            emit('CFLAGS_PER_TYPE', '$CFLAGS_DEBUG')
        if self.build.is_ide:
            emit('CFLAGS_PER_TYPE', '@[debug|$CFLAGS_DEBUG]@[release|$CFLAGS_RELEASE]')

        emit('_STD_CXX_VERSION', preset('USER_STD_CXX_VERSION') or self.tc.cxx_std)

        emit('_MSVC_FLAGS', flags)

        ucrt_include = win_path_fix(os.path.join(self.tc.kit_includes, 'ucrt')) if not self.tc.ide_msvs else "$(UniversalCRT_IncludePath.Split(';')[0].Replace('\\','/'))"

        # clang-cl has '#include_next', and MSVC hasn't. It needs separately specified CRT and VC include directories for libc++ to include second in order standard C and C++ headers.
        if not self.tc.use_clang:
            emit('_CFLAGS_UCRT_VC_INCLUDES', '/DY_UCRT_INCLUDE="%s"' % ucrt_include, '/DY_MSVC_INCLUDE="%s"' % vc_include)
        else:
            emit('_CFLAGS_UCRT_VC_INCLUDES')

        emit('WERROR_MODE', self.tc.werror_mode)

        print('@import "${CONF_ROOT}/conf/compilers/msvc_compiler.conf"')


class MSVCLinker(MSVC, Linker):
    def __init__(self, tc, build):
        MSVC.__init__(self, tc, build)
        Linker.__init__(self, tc, build)

    def print_linker(self):
        super(MSVCLinker, self).print_linker()

        linker = self.tc.link
        linker_lib = self.tc.lib

        emit('_MSVC_LIB', '"{}"'.format(linker_lib))
        emit('_MSVC_LIB_OLD_UNQUOTED', linker_lib)
        emit('_MSVC_LIB_OLD', '${quo:_MSVC_LIB_OLD_UNQUOTED}')
        emit('_MSVC_LINK', '"{}"'.format(linker))
        emit('_MSVC_LINK_OLD_UNQUOTED', linker)
        emit('_MSVC_LINK_OLD', '${quo:_MSVC_LINK_OLD_UNQUOTED}')

        if self.build.is_release:
            emit('LINK_EXE_FLAGS_PER_TYPE', '$LINK_EXE_FLAGS_RELEASE')
        if self.build.is_debug:
            emit('LINK_EXE_FLAGS_PER_TYPE', '$LINK_EXE_FLAGS_DEBUG')
        if self.build.is_ide and self.tc.ide_msvs:
            emit('LINK_EXE_FLAGS_PER_TYPE', '@[debug|$LINK_EXE_FLAGS_DEBUG]@[release|$LINK_EXE_FLAGS_RELEASE]')

        print('@import "${CONF_ROOT}/conf/linkers/msvc_linker.conf"')


# TODO(somov): Rename!
Compilers = {
    'gnu': (GnuToolchain, GnuCompiler, LD),
    'clang': (GnuToolchain, GnuCompiler, LD),
    'xcode': (GnuToolchain, GnuCompiler, LD),
    'msvc': (MSVCToolchain, MSVCCompiler, MSVCLinker),
}


class Ragel(object):
    def __init__(self):
        self.rlgen_flags = []
        self.ragel_flags = []
        self.ragel6_flags = []

    def configure_toolchain(self, build, compiler):
        if isinstance(compiler, MSVCCompiler):
            self.set_default_flags(optimized=False)
        elif isinstance(compiler, GnuCompiler):
            self.set_default_flags(optimized=build.is_release and not build.is_sanitized)
        else:
            raise ConfigureError('Unexpected compiler {}'.format(compiler))

    def set_default_flags(self, optimized):
        if optimized:
            self.rlgen_flags.append('-G2')
            self.ragel6_flags.append('-CG2')
        else:
            self.rlgen_flags.append('-T0')
            self.ragel6_flags.append('-CT0')

    def print_variables(self):
        emit('RLGEN_FLAGS', self.rlgen_flags)
        emit('RAGEL_FLAGS', self.ragel_flags)
        emit('RAGEL6_FLAGS', self.ragel6_flags)


class Python(object):
    def __init__(self, tc):
        self.python = None
        self.flags = None
        self.ldflags = None
        self.libraries = None
        self.includes = None
        self.tc = tc

    def configure_posix(self, python=None, python_config=None):
        self.check_configuration()

        python = python or preset('PYTHON_BIN') or which('python')
        python_config = python_config or preset('PYTHON_CONFIG') or which('python-config')

        if python is None or python_config is None:
            return

        # python-config dumps each option on one line in the specified order
        config = get_stdout([python_config, '--cflags', '--ldflags', '--includes']) or ''
        config = config.split('\n')
        if len(config) < 3:
            return

        self.python = python
        self.flags = config[0]
        self.ldflags = config[1]
        self.includes = config[2]
        # Do not split libraries from ldflags.
        # They are not used separately and get overriden together, so it is safe.
        # TODO(somov): Удалить эту переменную и PYTHON_LIBRARIES из makelist-ов.
        self.libraries = ''

    def check_configuration(self):
        if preset('USE_ARCADIA_PYTHON') == 'no':

            # Set USE_LOCAL_PYTHON to enable configuration
            # when we still using fixed OS SDK,
            # but at the same time using Python from the local system.
            #
            # This configuration is not guaranteed to work,
            # for example, if local and fixed OS SDKs differ much.
            if preset('USE_LOCAL_PYTHON') == 'yes':
                return

            if not preset('USE_SYSTEM_PYTHON') and not self.tc.os_sdk_local:
                raise Exception("Use fixed python (see https://clubs.at.yandex-team.ru/arcadia/15392) or set OS_SDK=local flag")

    def print_variables(self):
        variables = Variables({
            'PYTHON_BIN': self.python,
            'PYTHON_FLAGS': self.flags,
            'PYTHON_LDFLAGS': self.ldflags,
            'PYTHON_LIBRARIES': self.libraries,
            'PYTHON_INCLUDE': self.includes
        })

        variables.update_from_presets()
        variables.reset_if_any(reset_value='PYTHON-NOT-FOUND')
        variables.emit()


class Setting(object):
    def __init__(self, key, auto=None, convert=None, rewrite=False):
        self.key = key

        self.auto = auto
        self.convert = convert

        self.preset = preset(key)
        self.from_user = self.preset is not None
        self.rewrite = rewrite

        self._value = Setting.no_value

    @property
    def value(self):
        if self._value is Setting.no_value:
            self._value = self.calculate_value()
        return self._value

    def calculate_value(self):
        if not self.from_user:
            return self.auto if not callable(self.auto) else self.auto()
        else:
            return self.preset if not self.convert else self.convert(self.preset)

    @value.setter
    def value(self, value):
        if self.from_user:
            raise ConfigureError("Variable {key} already set by user to {old}. Can not change it's value to {new}".format(key=self.key, old=self._value, new=value))
        self._value = value

    def emit(self):
        if not self.from_user or self.rewrite:
            emit(self.key, self.value)

    no_value = object()


class Cuda(object):
    def __init__(self, build):
        """
        :type build: Build
        """
        self.build = build

        self.have_cuda = Setting('HAVE_CUDA', auto=self.auto_have_cuda, convert=to_bool)

        self.cuda_root = Setting('CUDA_ROOT')
        self.cuda_target_root = Setting('CUDA_TARGET_ROOT')
        self.cuda_version = Setting('CUDA_VERSION', auto=self.auto_cuda_version, convert=self.convert_major_version, rewrite=True)
        self.cuda_architectures = Setting('CUDA_ARCHITECTURES', auto=self.auto_cuda_architectures, rewrite=True)
        self.use_arcadia_cuda = Setting('USE_ARCADIA_CUDA', auto=self.auto_use_arcadia_cuda, convert=to_bool)
        self.use_arcadia_cuda_host_compiler = Setting('USE_ARCADIA_CUDA_HOST_COMPILER', auto=self.auto_use_arcadia_cuda_host_compiler, convert=to_bool)
        self.cuda_use_clang = Setting('CUDA_USE_CLANG', auto=False, convert=to_bool)
        self.cuda_host_compiler = Setting('CUDA_HOST_COMPILER', auto=self.auto_cuda_host_compiler)
        self.cuda_host_compiler_env = Setting('CUDA_HOST_COMPILER_ENV')
        self.cuda_host_msvc_version = Setting('CUDA_HOST_MSVC_VERSION')
        self.cuda_nvcc_flags = Setting('CUDA_NVCC_FLAGS', auto=[])

        self.peerdirs = ['build/platform/cuda']

        self.nvcc_flags = [
            # Compress fatbinary to reduce size of .nv_fatbin and prevent problems with linking
            #
            # Idea comes from many resources, one of them is https://discourse.llvm.org/t/lld-relocation-overflows-and-nv-fatbin/58889/6
            # Some sources suggest using `-Xfatbin=-compress-all`, other suggest using `-Xcuda-fatbinary --compress-all`
            # We will use the same flag as in nixpkgs
            # (https://github.com/NixOS/nixpkgs/pull/220402/files#diff-a38e6c4e8421c03dc6c2a60c9a172ceb4059048b65798e5d4a400a7a4a5720ffR167)
            "-Xfatbin=-compress-all",
            # Allow __host__, __device__ annotations in lambda declaration.
            "--expt-extended-lambda",
            # Allow host code to invoke __device__ constexpr functions and vice versa
            "--expt-relaxed-constexpr",
            # Allow to use newer compilers than CUDA Toolkit officially supports
            "--allow-unsupported-compiler",
            # Set paths explicitly
            "--dont-use-profile",
            "--libdevice-directory=$CUDA_ROOT/nvvm/libdevice",
        ]

        if not self.have_cuda.value:
            return

        if self.cuda_host_compiler.value:
            self.nvcc_flags.append('--compiler-bindir=$CUDA_HOST_COMPILER')

        if self.use_arcadia_cuda.value:
            self.cuda_root.value = '$CUDA_RESOURCE_GLOBAL'

            host, target = self.build.host_target

            if host == target:
                self.cuda_target_root.value = '$CUDA_RESOURCE_GLOBAL'
            else:
                self.cuda_target_root.value = '$CUDA_TARGET_RESOURCE_GLOBAL'

            if self.build.target.is_linux_x86_64 and self.build.tc.is_clang:
                # TODO(somov): Эта настройка должна приезжать сюда автоматически из другого места
                self.nvcc_flags.append('-I$OS_SDK_ROOT/usr/include/x86_64-linux-gnu')

    def print_(self):
        self.print_variables()
        self.print_macros()
        print('@import "${CONF_ROOT}/conf/compilers/nvcc.conf"')

    def print_variables(self):
        self.have_cuda.emit()
        if not self.have_cuda.value:
            return

        if self.use_arcadia_cuda.value and self.cuda_host_compiler.value is None:
            logger.warning('$USE_ARCADIA_CUDA is set, but no $CUDA_HOST_COMPILER')

        self.setup_vc_root()

        self.cuda_root.emit()
        self.cuda_target_root.emit()
        self.cuda_version.emit()
        self.cuda_architectures.emit()
        self.use_arcadia_cuda.emit()
        self.use_arcadia_cuda_host_compiler.emit()
        self.cuda_use_clang.emit()
        self.cuda_host_compiler.emit()
        self.cuda_host_compiler_env.emit()
        self.cuda_host_msvc_version.emit()
        self.cuda_nvcc_flags.emit()

        emit('NVCC', '"{}"'.format(self.build.host.exe('$CUDA_ROOT', 'bin', 'nvcc')))
        emit('NVCC_OLD_UNQUOTED', self.build.host.exe('$CUDA_ROOT', 'bin', 'nvcc'))
        emit('NVCC_OLD', '${quo:NVCC_OLD_UNQUOTED}')
        emit('NVCC_FLAGS', self.nvcc_flags, '$CUDA_NVCC_FLAGS')
        emit('NVCC_OBJ_EXT', '.o' if not self.build.target.is_windows else '.obj')
        emit('NVCC_ENV', format_env({'PATH': '$CUDA_ROOT/nvvm/bin:$CUDA_ROOT/bin'}))

    def print_macros(self):
        mtime = ' '
        if self.build.host_target[1].is_linux:
            mtime = ' --mtime ${tool:"tools/mtime0"} '
        if not self.cuda_use_clang.value:
            cmd = '$YMAKE_PYTHON ${input:"build/scripts/compile_cuda.py"}' + mtime + '$NVCC_OLD $NVCC_STD $NVCC_FLAGS -c ${input:SRC} -o ${output;suf=${OBJ_SUF}${NVCC_OBJ_EXT}:SRC} ${pre=-I:_C__INCLUDE} --cflags $C_FLAGS_PLATFORM $CXXFLAGS $NVCC_STD $NVCC_CFLAGS $SRCFLAGS ${input;hide:"build/platform/cuda/cuda_runtime_include.h"} $NVCC_ENV $CUDA_HOST_COMPILER_ENV ${kv;hide:"p CC"} ${kv;hide:"pc light-green"}'  # noqa E501
        else:
            cmd = '$CXX_COMPILER_OLD --cuda-path=$CUDA_ROOT $C_FLAGS_PLATFORM -c ${input:SRC} -o ${output;suf=${OBJ_SUF}${NVCC_OBJ_EXT}:SRC} ${pre=-I:_C__INCLUDE} $CXXFLAGS $SRCFLAGS $TOOLCHAIN_ENV ${kv;hide:"p CU"} ${kv;hide:"pc green"}'  # noqa E501

        emit('_SRC_CU_CMD', cmd)
        emit('_SRC_CU_PEERDIR', ' '.join(sorted(self.peerdirs)))

    # This method is only used to set HAVE_CUDA=no automatically during ymake initialization
    # (i.e. is used only in `auto_have_cuda` invocation)
    def have_cuda_in_arcadia(self):
        host, target = self.build.host_target

        if not any((host.is_linux_x86_64, host.is_windows_x86_64)):
            return False

        if host != target:
            if not (host.is_linux_x86_64 and target.is_linux_armv8):
                return False
            if not self.cuda_version.from_user:
                return False

        if self.cuda_version.value in ('11.4', '11.8', '12.1', '12.2'):
            return True
        elif self.cuda_version.value in ('10.2',) and target.is_linux_armv8:
            return True
        else:
            raise ConfigureError('CUDA version {} is not supported in Arcadia'.format(self.cuda_version.value))

    def auto_have_cuda(self):
        if is_positive('MUSL'):
            return False
        if self.build.is_sanitized:
            return False
        return self.cuda_root.from_user or self.use_arcadia_cuda.value and self.have_cuda_in_arcadia()

    def auto_cuda_version(self):
        if self.use_arcadia_cuda.value:
            return '11.4'

        if not self.have_cuda.value:
            return None

        nvcc_exe = self.build.host.exe(os.path.expanduser(self.cuda_root.value), 'bin', 'nvcc')

        def error():
            raise ConfigureError('Failed to get CUDA version from {}'.format(nvcc_exe))

        version_output = get_stdout([nvcc_exe, '--version']) or error()
        match = re.search(r'^Cuda compilation tools, release (\d+)\.\d+,', version_output, re.MULTILINE) or error()

        return match.group(1)

    def convert_major_version(self, value):
        if value == '11':
            return '11.4'
        else:
            return value

    def auto_cuda_architectures(self):
        # empty list does not mean "no architectures"
        # it means "no restriction -- any available architecture"

        host, target = self.build.host_target
        if not target.is_linux_x86_64:
            # do not impose any restrictions, when build not for "linux 64-bit"
            return ''

        # Equality to CUDA 11.4 is rather strict comparison
        # TODO: find out how we can relax check (e.g. to include more version of CUDA toolkit)
        if self.cuda_version.value == '11.4':
            # * use output of CUDA 11.4 `nvcc --help`
            # * drop support for '53', '62', '72' and '87'
            #   (these devices run only on arm64)
            # * drop support for '37'
            #   the single place it's used in Arcadia is https://a.yandex-team.ru/arcadia/sdg/sdc/third_party/cub/common.mk?rev=r13268523#L69
            return ':'.join(
                ['sm_35',
                 'sm_50', 'sm_52',
                 'sm_60', 'sm_61',
                 'sm_70', 'sm_75',
                 'sm_80', 'sm_86',
                 'compute_86'])
        else:
            return ''

    def auto_use_arcadia_cuda(self):
        return not self.cuda_root.from_user

    def auto_use_arcadia_cuda_host_compiler(self):
        return not self.cuda_host_compiler.from_user and not self.cuda_use_clang.value

    def auto_cuda_host_compiler(self):
        if not self.use_arcadia_cuda_host_compiler.value:
            return None

        host, target = self.build.host_target

        if host.is_windows_x86_64 and target.is_windows_x86_64:
            return self.cuda_windows_host_compiler()

        return select((
            (host.is_linux_x86_64 and target.is_linux_x86_64, '$CUDA_HOST_TOOLCHAIN_RESOURCE_GLOBAL/bin/clang'),
            (host.is_linux_x86_64 and target.is_linux_armv8, '$CUDA_HOST_TOOLCHAIN_RESOURCE_GLOBAL/bin/clang'),
        ))

    def cuda_windows_host_compiler(self):
        vc_version = '14.28.29910'

        env = {
            'Y_VC_Version': vc_version,
            'Y_VC_Root': '$CUDA_HOST_TOOLCHAIN_RESOURCE_GLOBAL/VC/Tools/MSVC/{}'.format(vc_version),
            'Y_SDK_Version': self.build.tc.sdk_version,
            'Y_SDK_Root': '$WINDOWS_KITS_RESOURCE_GLOBAL',
        }

        if not self.build.tc.ide_msvs:
            self.peerdirs.append('build/internal/platform/msvc')
        self.cuda_host_compiler_env.value = format_env(env)
        self.cuda_host_msvc_version.value = vc_version
        return '%(Y_VC_Root)s/bin/HostX64/x64/cl.exe' % env

    def setup_vc_root(self):
        if not self.cuda_host_compiler.from_user:
            return  # Already set in cuda_windows_host_compiler()

        if self.cuda_host_compiler_env.from_user:
            return  # We won't override user setting

        def is_root(dir):
            return all(os.path.isdir(os.path.join(dir, name)) for name in ('bin', 'include', 'lib'))

        def get_root():
            path, old_path = os.path.normpath(self.cuda_host_compiler.value), None
            while path != old_path:
                if is_root(path):
                    return path
                path, old_path = os.path.dirname(path), path

        vc_root = get_root()
        if vc_root:
            self.cuda_host_compiler_env.value = format_env({'Y_VC_Root': vc_root})


class CuDNN(object):
    def __init__(self, cuda):
        """
        :type cuda: Cuda
        """
        self.cuda = cuda

        self.cudnn_version = Setting('CUDNN_VERSION', auto=self.auto_cudnn_version)

    def have_cudnn(self):
        return self.cudnn_version.value in ('7.6.5', '8.0.5')

    def auto_cudnn_version(self):
        return '8.0.5'

    def print_(self):
        if self.cuda.have_cuda.value and self.have_cudnn():
            self.cudnn_version.emit()


def customization():
    if not is_positive('DISABLE_YMAKE_CONF_CUSTOMIZATION'):
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'internal'))
            from custom_conf import CustomConf
            logger.debug('Customization extension was successfully loaded')
            return CustomConf
        except Exception as e:
            logger.debug('Customization extension was not found; [{}]'.format(str(e)))
    else:
        logger.debug('Customization extension was disabled')

    class DummyConf:
        def __init__(self, options):
            pass

        def print_prologue(self):
            pass

        def print_epilogue(self):
            pass

    return DummyConf


def main():
    options = opts()

    CustomConfig = customization()
    custom_conf = CustomConfig(options)

    arcadia = Arcadia(options.arcadia_root)

    ymake = YMake(arcadia)

    ymake.print_core_conf()

    _INTERNAL_CONF = 'internal/conf/internal.conf'
    internal_conf_full_path = find_conf(_INTERNAL_CONF)
    if internal_conf_full_path and not is_positive('DISABLE_YMAKE_CONF_CUSTOMIZATION'):
        print('@import "${{CONF_ROOT}}/{}"'.format(_INTERNAL_CONF))

    custom_conf.print_prologue()

    ymake.print_presets()
    ymake.print_settings()

    build = Build(arcadia, options.build_type, options.toolchain_params, force_ignore_local_files=not options.local_distbuild)
    build.print_build()

    custom_conf.print_epilogue()


if __name__ == '__main__':
    main()
