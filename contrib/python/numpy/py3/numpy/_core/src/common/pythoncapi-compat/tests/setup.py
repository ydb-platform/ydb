#!/usr/bin/env python3
import os.path
import shlex
import sys
try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension
try:
    from distutils import sysconfig
except ImportError:
    import sysconfig


# C++ is only supported on Python 3.6 and newer
TEST_CPP = (sys.version_info >= (3, 6))

SRC_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))

# Windows uses MSVC compiler
MSVC = (os.name == "nt")

# C compiler flags for GCC and clang
COMMON_FLAGS = [
    # Treat warnings as error
    '-Werror',
    # Enable all warnings
    '-Wall', '-Wextra',
    # Extra warnings
    '-Wconversion',
    # /usr/lib64/pypy3.7/include/pyport.h:68:20: error: redefinition of typedef
    # 'Py_hash_t' is a C11 feature
    "-Wno-typedef-redefinition",
]
CFLAGS = COMMON_FLAGS + [
    # Use C99 for pythoncapi_compat.c which initializes PyModuleDef with a
    # mixture of designated and non-designated initializers
    '-std=c99',
]
CPPFLAGS = list(COMMON_FLAGS)
# FIXME: _Py_CAST() emits C++ compilers on Python 3.12.
# See: https://github.com/python/cpython/issues/94731
if 0:
    CPPFLAGS.extend((
        '-Wold-style-cast',
        '-Wzero-as-null-pointer-constant',
    ))


def main():
    # gh-105776: When "gcc -std=11" is used as the C++ compiler, -std=c11
    # option emits a C++ compiler warning. Remove "-std11" option from the
    # CC command.
    cmd = (sysconfig.get_config_var('CC') or '')
    if cmd:
        cmd = shlex.split(cmd)
        cmd = [arg for arg in cmd if not arg.startswith('-std=')]
        if (sys.version_info >= (3, 8)):
            cmd = shlex.join(cmd)
        elif (sys.version_info >= (3, 3)):
            cmd = ' '.join(shlex.quote(arg) for arg in cmd)
        else:
            # Python 2.7
            import pipes
            cmd = ' '.join(pipes.quote(arg) for arg in cmd)
        # CC env var overrides sysconfig CC variable in setuptools
        os.environ['CC'] = cmd

    cflags = ['-I' + SRC_DIR]
    cppflags = list(cflags)
    if not MSVC:
        cflags.extend(CFLAGS)
        cppflags.extend(CPPFLAGS)

    # C extension
    c_ext = Extension(
        'test_pythoncapi_compat_cext',
        sources=['test_pythoncapi_compat_cext.c'],
        extra_compile_args=cflags)
    extensions = [c_ext]

    if TEST_CPP:
        # C++ extension

        # MSVC has /std flag but doesn't support /std:c++11
        if not MSVC:
            versions = [
                ('test_pythoncapi_compat_cpp03ext', '-std=c++03'),
                ('test_pythoncapi_compat_cpp11ext', '-std=c++11'),
            ]
        else:
            versions = [('test_pythoncapi_compat_cppext', None)]
        for name, flag in versions:
            flags = list(cppflags)
            if flag is not None:
                flags.append(flag)
            cpp_ext = Extension(
                name,
                sources=['test_pythoncapi_compat_cppext.cpp'],
                extra_compile_args=flags,
                language='c++')
            extensions.append(cpp_ext)

    setup(name="test_pythoncapi_compat",
          ext_modules=extensions)


if __name__ == "__main__":
    main()
