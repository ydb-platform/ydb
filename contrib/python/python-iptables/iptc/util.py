import re
import os
import sys
import ctypes
import ctypes.util
from itertools import product
from subprocess import Popen, PIPE
from sys import version_info
try:
    from sysconfig import get_config_var
except ImportError:
    def get_config_var(name):
        if name == 'SO':
            return '.so'
        raise Exception('Not implemented')
try:
    from distutils.sysconfig import get_python_lib
except ModuleNotFoundError:
    import sysconfig
    def get_python_lib():
        return sysconfig.get_path("purelib")


def _insert_ko(modprobe, modname):
    p = Popen([modprobe, modname], stderr=PIPE)
    p.wait()
    return (p.returncode, p.stderr.read(1024))


def _load_ko(modname):
    # only try to load modules on kernels that support them
    if not os.path.exists("/proc/modules"):
        return (0, None)

    # this will return the full path for the modprobe binary
    modprobe = "/sbin/modprobe"
    try:
        proc = open("/proc/sys/kernel/modprobe")
        modprobe = proc.read(1024)
    except:
        pass
    if modprobe[-1] == '\n':
        modprobe = modprobe[:-1]
    return _insert_ko(modprobe, modname)


# Load a kernel module. If it is already loaded modprobe will just return 0.
def load_kernel(name, exc_if_failed=False):
    rc, err = _load_ko(name)
    if rc:
        if not err:
            err = "Failed to load the %s kernel module." % (name)
        if err[-1] == "\n":
            err = err[:-1]
        if exc_if_failed:
            raise Exception(err)


def _do_find_library(name):
    if '/' in name:
        try:
            return ctypes.CDLL(name, mode=ctypes.RTLD_GLOBAL)
        except Exception:
            return None
    p = ctypes.util.find_library(name)
    if p:
        lib = ctypes.CDLL(p, mode=ctypes.RTLD_GLOBAL)
        return lib

    # probably we have been installed in a virtualenv
    try:
        lib = ctypes.CDLL(os.path.join(get_python_lib(), name),
                          mode=ctypes.RTLD_GLOBAL)
        return lib
    except:
        pass

    for p in sys.path:
        try:
            lib = ctypes.CDLL(os.path.join(p, name), mode=ctypes.RTLD_GLOBAL)
            return lib
        except:
            pass
    return None


def _find_library(*names):
    exts = []
    if version_info >= (3, 3):
        exts.append(get_config_var("EXT_SUFFIX"))
    else:
        exts.append(get_config_var('SO'))

    if version_info >= (3, 5):
        exts.append('.so')

    for name in names:
        libnames = [name, "lib" + name]
        for ext in exts:
            libnames += [name + ext, "lib" + name + ext]
        libdir = os.environ.get('IPTABLES_LIBDIR', None)
        if libdir is not None:
            libdirs = libdir.split(':')
            libs = [os.path.join(*p) for p in product(libdirs, libnames)]
            libs.extend(libnames)
        else:
            libs = libnames
        for n in libs:
            while os.path.islink(n):
                n = os.path.realpath(n)
            lib = _do_find_library(n)
            if lib is not None:
                yield lib


def find_library(*names):
    for lib in _find_library(*names):
        major = 0
        m = re.search(r"\.so\.(\d+).?", lib._name)
        if m:
            major = int(m.group(1))
        return lib, major
    return None, None


def find_libc():
    lib = ctypes.util.find_library('c')
    if lib is not None:
        return ctypes.CDLL(lib, mode=ctypes.RTLD_GLOBAL)

    libnames = ['libc.so.6', 'libc.so.0', 'libc.so']
    for name in libnames:
        try:
            lib = ctypes.CDLL(name, mode=ctypes.RTLD_GLOBAL)
            return lib
        except:
            pass

    return None
