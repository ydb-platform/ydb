# Copyright 2014 Altera Corporation. All Rights Reserved.
# Copyright 2015-2017 John McGehee
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This module provides a base class derived from `unittest.TestClass`
for unit tests using the :py:class:`pyfakefs` module.

`fake_filesystem_unittest.TestCase` searches `sys.modules` for modules
that import the `os`, `io`, `path` `shutil`, and `pathlib` modules.

The `setUpPyfakefs()` method binds these modules to the corresponding fake
modules from `pyfakefs`.  Further, the `open()` built-in is bound to a fake
`open()`.

It is expected that `setUpPyfakefs()` be invoked at the beginning of the
derived class' `setUp()` method.  There is no need to add anything to the
derived class' `tearDown()` method.

During the test, everything uses the fake file system and modules.  This means
that even in your test fixture, familiar functions like `open()` and
`os.makedirs()` manipulate the fake file system.

Existing unit tests that use the real file system can be retrofitted to use
pyfakefs by simply changing their base class from `:py:class`unittest.TestCase`
to `:py:class`pyfakefs.fake_filesystem_unittest.TestCase`.
"""

import _io  # type:ignore[import]
import doctest
import functools
import genericpath
import glob
import inspect
import io
import linecache
import os
import shutil
import sys
import tempfile
import tokenize
import unittest
import warnings
from importlib import reload
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from importlib.util import spec_from_file_location, module_from_spec
from types import ModuleType, TracebackType, FunctionType
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Set,
    Tuple,
    Optional,
    Union,
    Type,
    Iterator,
    cast,
    ItemsView,
    Sequence,
)
from unittest import TestSuite

from pyfakefs import fake_filesystem, fake_io, fake_os, fake_open, fake_path, fake_file
from pyfakefs import fake_filesystem_shutil
from pyfakefs import fake_legacy_modules
from pyfakefs import fake_pathlib
from pyfakefs import mox3_stubout
from pyfakefs.fake_filesystem import (
    set_uid,
    set_gid,
    reset_ids,
    PatchMode,
    FakeFilesystem,
)
from pyfakefs.fake_os import use_original_os
from pyfakefs.helpers import IS_PYPY
from pyfakefs.legacy_packages import pathlib2, scandir
from pyfakefs.mox3_stubout import StubOutForTesting

OS_MODULE = "nt" if sys.platform == "win32" else "posix"
PATH_MODULE = "ntpath" if sys.platform == "win32" else "posixpath"


class TempfilePatcher:
    """Handles tempfile patching for Posix systems."""

    def __init__(self):
        self.tempfile_cleanup = None

    def start_patching(self):
        if self.tempfile_cleanup is not None:
            return
        if sys.version_info >= (3, 12):

            def cleanup(self_, windows=(os.name == "nt"), unlink=None):
                self.tempfile_cleanup(self_, windows, unlink or os.unlink)

            self.tempfile_cleanup = tempfile._TemporaryFileCloser.cleanup  # type: ignore[module-attr]
            tempfile._TemporaryFileCloser.cleanup = cleanup  # type: ignore[module-attr]
        elif sys.platform != "win32":

            def close(self_, unlink=None):
                self.tempfile_cleanup(self_, unlink or os.unlink)

            self.tempfile_cleanup = tempfile._TemporaryFileCloser.close  # type: ignore[module-attr]
            tempfile._TemporaryFileCloser.close = close  # type: ignore[module-attr]

    def stop_patching(self):
        if self.tempfile_cleanup is None:
            return
        if sys.version_info < (3, 12):
            tempfile._TemporaryFileCloser.close = self.tempfile_cleanup  # type: ignore[module-attr]
        else:
            tempfile._TemporaryFileCloser.cleanup = self.tempfile_cleanup  # type: ignore[module-attr]
        self.tempfile_cleanup = None
        # reset the cached tempdir in tempfile
        tempfile.tempdir = None


class LineCachePatcher:
    """Handles linecache patching for newer Python versions."""

    def __init__(self):
        self.linecache_updatecache = None
        self.linecache_checkcache = None

    def start_patching(self):
        if self.linecache_updatecache is not None:
            return

        def checkcache(filename=None):
            """Calls the original linecache.checkcache making sure no fake OS calls
            are used."""
            with use_original_os():
                return self.linecache_checkcache(filename)

        def updatecache(filename, module_globals=None):
            """Calls the original linecache.updatecache making sure no fake OS calls
            are used."""

            with use_original_os():
                # workaround for updatecache problem with pytest under Windows, see #1096
                if not filename.endswith(r"pytest.exe\__main__.py"):
                    return self.linecache_updatecache(filename, module_globals)
                return []

        if (
            sys.version_info >= (3, 13)
            or sys.platform == "win32"
            and sys.version_info >= (3, 12)
        ):
            # in linecache, 'os' is now imported locally, which involves the
            # dynamic patcher, therefore we patch the affected functions
            self.linecache_updatecache = linecache.updatecache
            linecache.updatecache = updatecache
            self.linecache_checkcache = linecache.checkcache
            linecache.checkcache = checkcache

    def stop_patching(self):
        if self.linecache_updatecache is None:
            return
        linecache.updatecache = self.linecache_updatecache
        linecache.checkcache = self.linecache_checkcache
        self.linecache_updatecache = None
        self.linecache_checkcache = None


def patchfs(
    _func: Optional[Callable] = None,
    *,
    additional_skip_names: Optional[List[Union[str, ModuleType]]] = None,
    modules_to_reload: Optional[List[ModuleType]] = None,
    modules_to_patch: Optional[Dict[str, ModuleType]] = None,
    allow_root_user: bool = True,
    use_known_patches: bool = True,
    patch_open_code: PatchMode = PatchMode.OFF,
    patch_default_args: bool = False,
    use_cache: bool = True,
    use_dynamic_patch: bool = True,
) -> Callable:
    """Convenience decorator to use patcher with additional parameters in a
    test function.

    Usage::

        @patchfs
        def test_my_function(fake_fs):
            fake_fs.create_file('foo')

        @patchfs(allow_root_user=False)
        def test_with_patcher_args(fs):
            os.makedirs('foo/bar')
    """

    def wrap_patchfs(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            with Patcher(
                additional_skip_names=additional_skip_names,
                modules_to_reload=modules_to_reload,
                modules_to_patch=modules_to_patch,
                allow_root_user=allow_root_user,
                use_known_patches=use_known_patches,
                patch_open_code=patch_open_code,
                patch_default_args=patch_default_args,
                use_cache=use_cache,
                use_dynamic_patch=use_dynamic_patch,
            ) as p:
                args = list(args)
                args.append(p.fs)
                return f(*args, **kwargs)

        return wrapped

    if _func:
        if not callable(_func):
            raise TypeError(
                "Decorator argument is not a function.\n"
                "Did you mean `@patchfs(additional_skip_names=...)`?"
            )
        if hasattr(_func, "patchings"):
            _func.nr_patches = len(_func.patchings)  # type: ignore
        return wrap_patchfs(_func)

    return wrap_patchfs


DOCTEST_PATCHER = None


def load_doctests(
    loader: Any,
    tests: TestSuite,
    ignore: Any,
    module: ModuleType,
    additional_skip_names: Optional[List[Union[str, ModuleType]]] = None,
    modules_to_reload: Optional[List[ModuleType]] = None,
    modules_to_patch: Optional[Dict[str, ModuleType]] = None,
    allow_root_user: bool = True,
    use_known_patches: bool = True,
    patch_open_code: PatchMode = PatchMode.OFF,
    patch_default_args: bool = False,
    use_dynamic_patch: bool = True,
) -> TestSuite:  # pylint:disable=unused-argument
    """Load the doctest tests for the specified module into unittest.
        Args:
            loader, tests, ignore : arguments passed in from `load_tests()`
            module: module under test
            remaining args: see :py:class:`TestCase` for an explanation

    File `example_test.py` in the pyfakefs release provides a usage example.
    """
    has_patcher = Patcher.DOC_PATCHER is not None
    if not has_patcher:
        Patcher.DOC_PATCHER = Patcher(
            additional_skip_names=additional_skip_names,
            modules_to_reload=modules_to_reload,
            modules_to_patch=modules_to_patch,
            allow_root_user=allow_root_user,
            use_known_patches=use_known_patches,
            patch_open_code=patch_open_code,
            patch_default_args=patch_default_args,
            use_dynamic_patch=use_dynamic_patch,
            is_doc_test=True,
        )
    assert Patcher.DOC_PATCHER is not None
    globs = Patcher.DOC_PATCHER.replace_globs(vars(module))
    tests.addTests(
        doctest.DocTestSuite(
            module,
            globs=globs,
            setUp=Patcher.DOC_PATCHER.setUp,
            tearDown=Patcher.DOC_PATCHER.tearDown,
        )
    )
    return tests


class TestCaseMixin:
    """Test case mixin that automatically replaces file-system related
    modules by fake implementations.

    Attributes:
        additional_skip_names: names of modules where no module
            replacement shall be performed, in addition to the names in
            :py:attr:`fake_filesystem_unittest.Patcher.SKIPNAMES`.
            Instead of the module names, the modules themselves may be used.
        modules_to_reload: A list of modules that need to be reloaded
            to be patched dynamically; may be needed if the module
            imports file system modules under an alias

            .. caution:: Reloading modules may have unwanted side effects.
        modules_to_patch: A dictionary of fake modules mapped to the
            fully qualified patched module names. Can be used to add patching
            of modules not provided by `pyfakefs`.

    If you specify some of these attributes here, and you have DocTests,
    consider also specifying the same arguments to :py:func:`load_doctests`.

    Example usage in derived test classes::

        from unittest import TestCase
        from fake_filesystem_unittest import TestCaseMixin

        class MyTestCase(TestCase, TestCaseMixin):
            def __init__(self, methodName='runTest'):
                super(MyTestCase, self).__init__(
                    methodName=methodName,
                    additional_skip_names=['posixpath'])

        import sut

        class AnotherTestCase(TestCase, TestCaseMixin):
            def __init__(self, methodName='runTest'):
                super(MyTestCase, self).__init__(
                    methodName=methodName, modules_to_reload=[sut])
    """

    additional_skip_names: Optional[List[Union[str, ModuleType]]] = None
    modules_to_reload: Optional[List[ModuleType]] = None
    modules_to_patch: Optional[Dict[str, ModuleType]] = None

    @property
    def patcher(self):
        if hasattr(self, "_patcher"):
            return self._patcher or Patcher.PATCHER
        return Patcher.PATCHER

    @property
    def fs(self) -> FakeFilesystem:
        return cast(FakeFilesystem, self.patcher.fs)

    def setUpPyfakefs(
        self,
        additional_skip_names: Optional[List[Union[str, ModuleType]]] = None,
        modules_to_reload: Optional[List[ModuleType]] = None,
        modules_to_patch: Optional[Dict[str, ModuleType]] = None,
        allow_root_user: bool = True,
        use_known_patches: bool = True,
        patch_open_code: PatchMode = PatchMode.OFF,
        patch_default_args: bool = False,
        use_cache: bool = True,
        use_dynamic_patch: bool = True,
    ) -> None:
        """Bind the file-related modules to the :py:class:`pyfakefs` fake file
        system instead of the real file system.  Also bind the fake `open()`
        function.

        Invoke this at the beginning of the `setUp()` method in your unit test
        class.
        For the arguments, see the `TestCaseMixin` attribute description.
        If any of the arguments is not None, it overwrites the settings for
        the current test case. Settings the arguments here may be a more
        convenient way to adapt the setting than overwriting `__init__()`.
        """
        # if the class has already a patcher setup, we use this one
        if Patcher.PATCHER is not None:
            return

        if additional_skip_names is None:
            additional_skip_names = self.additional_skip_names
        if modules_to_reload is None:
            modules_to_reload = self.modules_to_reload
        if modules_to_patch is None:
            modules_to_patch = self.modules_to_patch
        self._patcher = Patcher(
            additional_skip_names=additional_skip_names,
            modules_to_reload=modules_to_reload,
            modules_to_patch=modules_to_patch,
            allow_root_user=allow_root_user,
            use_known_patches=use_known_patches,
            patch_open_code=patch_open_code,
            patch_default_args=patch_default_args,
            use_cache=use_cache,
            use_dynamic_patch=use_dynamic_patch,
        )

        self._patcher.setUp()
        cast(TestCase, self).addCleanup(self._patcher.tearDown)

    @classmethod
    def setUpClassPyfakefs(
        cls,
        additional_skip_names: Optional[List[Union[str, ModuleType]]] = None,
        modules_to_reload: Optional[List[ModuleType]] = None,
        modules_to_patch: Optional[Dict[str, ModuleType]] = None,
        allow_root_user: bool = True,
        use_known_patches: bool = True,
        patch_open_code: PatchMode = PatchMode.OFF,
        patch_default_args: bool = False,
        use_cache: bool = True,
        use_dynamic_patch: bool = True,
    ) -> None:
        """Similar to :py:func:`setUpPyfakefs`, but as a class method that
        can be used in `setUpClass` instead of in `setUp`.
        The fake filesystem will live in all test methods in the test class
        and can be used in the usual way.
        Note that using both :py:func:`setUpClassPyfakefs` and
        :py:func:`setUpPyfakefs` in the same class will not work correctly.

        .. note:: This method is only available from Python 3.8 onwards.
        .. note:: If using `pytest` as testrunner, you need at least pytest 6.2
            for this method to work.
        """
        if sys.version_info < (3, 8):
            raise NotImplementedError(
                "setUpClassPyfakefs is only available in "
                "Python versions starting from 3.8"
            )

        # if the class has already a patcher setup, we use this one
        if Patcher.PATCHER is not None:
            return

        if additional_skip_names is None:
            additional_skip_names = cls.additional_skip_names
        if modules_to_reload is None:
            modules_to_reload = cls.modules_to_reload
        if modules_to_patch is None:
            modules_to_patch = cls.modules_to_patch
        Patcher.PATCHER = Patcher(
            additional_skip_names=additional_skip_names,
            modules_to_reload=modules_to_reload,
            modules_to_patch=modules_to_patch,
            allow_root_user=allow_root_user,
            use_known_patches=use_known_patches,
            patch_open_code=patch_open_code,
            patch_default_args=patch_default_args,
            use_cache=use_cache,
            use_dynamic_patch=use_dynamic_patch,
        )

        Patcher.PATCHER.setUp()
        cast(TestCase, cls).addClassCleanup(Patcher.PATCHER.tearDown)

    @classmethod
    def fake_fs(cls):
        """Convenience class method for accessing the fake filesystem.
        For use inside `setUpClass`, after :py:func:`setUpClassPyfakefs`
        has been called.
        """
        if Patcher.PATCHER:
            return Patcher.PATCHER.fs
        return None

    def pause(self) -> None:
        """Pause the patching of the file system modules until `resume` is
        called. After that call, all file system calls are executed in the
        real file system.
        Calling pause() twice is silently ignored.

        """
        self.patcher.pause()

    def resume(self) -> None:
        """Resume the patching of the file system modules if `pause` has
        been called before. After that call, all file system calls are
        executed in the fake file system.
        Does nothing if patching is not paused.
        """
        self.patcher.resume()


class TestCase(unittest.TestCase, TestCaseMixin):
    """Test case class that automatically replaces file-system related
    modules by fake implementations. Inherits :py:class:`TestCaseMixin`.

    The arguments are explained in :py:class:`TestCaseMixin`.
    """

    def __init__(
        self,
        methodName: str = "runTest",
        additional_skip_names: Optional[List[Union[str, ModuleType]]] = None,
        modules_to_reload: Optional[List[ModuleType]] = None,
        modules_to_patch: Optional[Dict[str, ModuleType]] = None,
    ):
        """Creates the test class instance and the patcher used to stub out
        file system related modules.

        Args:
            methodName: The name of the test method (same as in
                unittest.TestCase)
        """
        super().__init__(methodName)

        self.additional_skip_names = additional_skip_names
        self.modules_to_reload = modules_to_reload
        self.modules_to_patch = modules_to_patch

    def tearDownPyfakefs(self) -> None:
        """This method is deprecated and exists only for backward
        compatibility. It does nothing.
        """


class Patcher:
    """
    Instantiate a stub creator to bind and un-bind the file-related modules to
    the :py:mod:`pyfakefs` fake modules.

    The arguments are explained in :py:class:`TestCaseMixin`.

    :py:class:`Patcher` is used in :py:class:`TestCaseMixin`.
    :py:class:`Patcher` also works as a context manager for other tests::

        with Patcher():
            doStuff()
    """

    """Stub nothing that is imported within these modules.
    `sys` is included to prevent `sys.path` from being stubbed with the fake
    `os.path`.
    The `linecache` module is used to read the test file in case of test
    failure to get traceback information before test tear down.
    In order to make sure that reading the test file is not faked,
    we skip faking the module.
    We also have to set back the cached open function in tokenize.
    """
    SKIPMODULES = {
        None,
        fake_filesystem,
        fake_filesystem_shutil,
        fake_os,
        fake_io,
        fake_open,
        fake_path,
        fake_file,
        sys,
        linecache,
        tokenize,
        os,
        io,
        _io,
        genericpath,
        os.path,
    }
    if sys.platform == "win32":
        import nt  # type:ignore[import]
        import ntpath

        SKIPMODULES.add(nt)
        SKIPMODULES.add(ntpath)
    else:
        import posix
        import posixpath
        import fcntl

        SKIPMODULES.add(posix)
        SKIPMODULES.add(posixpath)
        SKIPMODULES.add(fcntl)

    # a list of modules detected at run-time
    # each tool defines one or more module name prefixes for modules to be skipped
    RUNTIME_SKIPMODULES = {
        "pydevd": ["_pydevd_", "pydevd", "_pydev_"],  # Python debugger (PyCharm/VSCode)
        "_jb_runner_tools": ["_jb_"],  # JetBrains tools
    }

    # caches all modules that do not have file system modules or function
    # to speed up _find_modules
    CACHED_MODULES: Set[ModuleType] = set()
    FS_MODULES: Dict[str, Set[Tuple[ModuleType, str]]] = {}
    FS_FUNCTIONS: Dict[Tuple[str, str, str], Set[ModuleType]] = {}
    FS_DEFARGS: List[Tuple[FunctionType, int, Callable[..., Any]]] = []
    SKIPPED_FS_MODULES: Dict[str, Set[Tuple[ModuleType, str]]] = {}

    assert None in SKIPMODULES, "sys.modules contains 'None' values; must skip them."

    IS_WINDOWS = sys.platform in ("win32", "cygwin")

    SKIPNAMES: Set[str] = set()

    # hold values from last call - if changed, the cache has to be invalidated
    PATCHED_MODULE_NAMES: Set[str] = set()
    ADDITIONAL_SKIP_NAMES: Set[str] = set()
    PATCH_DEFAULT_ARGS = False
    PATCHER: Optional["Patcher"] = None
    DOC_PATCHER: Optional["Patcher"] = None
    REF_COUNT = 0
    DOC_REF_COUNT = 0

    def __new__(cls, *args, **kwargs):
        if kwargs.get("is_doc_test", False):
            if cls.DOC_PATCHER is None:
                cls.DOC_PATCHER = super().__new__(cls)
            return cls.DOC_PATCHER
        if cls.PATCHER is None:
            cls.PATCHER = super().__new__(cls)
        return cls.PATCHER

    def __init__(
        self,
        additional_skip_names: Optional[List[Union[str, ModuleType]]] = None,
        modules_to_reload: Optional[List[ModuleType]] = None,
        modules_to_patch: Optional[Dict[str, ModuleType]] = None,
        allow_root_user: bool = True,
        use_known_patches: bool = True,
        patch_open_code: PatchMode = PatchMode.OFF,
        patch_default_args: bool = False,
        use_cache: bool = True,
        use_dynamic_patch: bool = True,
        is_doc_test: bool = False,
    ) -> None:
        """
        Args:
            additional_skip_names: names of modules where no module
                replacement shall be performed, in addition to the names in
                :py:attr:`fake_filesystem_unittest.Patcher.SKIPNAMES`.
                Instead of the module names, the modules themselves
                may be used.
            modules_to_reload: A list of modules that need to be reloaded
                to be patched dynamically; may be needed if the module
                imports file system modules under an alias

                .. caution:: Reloading modules may have unwanted side effects.
            modules_to_patch: A dictionary of fake modules mapped to the
                fully qualified patched module names. Can be used to add
                patching of modules not provided by `pyfakefs`.
            allow_root_user: If True (default), if the test is run as root
                user, the user in the fake file system is also considered a
                root user, otherwise it is always considered a regular user.
            use_known_patches: If True (the default), some patches for commonly
                used packages are applied which make them usable with pyfakefs.
            patch_open_code: If True, `io.open_code` is patched. The default
                is not to patch it, as it mostly is used to load compiled
                modules that are not in the fake file system.
            patch_default_args: If True, default arguments are checked for
                file system functions, which are patched. This check is
                expansive, so it is off by default.
            use_cache: If True (default), patched and non-patched modules are
                cached between tests for performance reasons. As this is a new
                feature, this argument allows to turn it off in case it
                causes any problems.
            use_dynamic_patch: If `True`, dynamic patching after setup is used
                (for example for modules loaded locally inside of functions).
                Can be switched off if it causes unwanted side effects.
        """
        self.is_doc_test = is_doc_test
        if is_doc_test:
            if self.DOC_REF_COUNT > 0:
                return
        elif self.REF_COUNT > 0:
            return
        if not allow_root_user:
            # set non-root IDs even if the real user is root
            set_uid(1)
            set_gid(1)

        self.skip_names = self.SKIPNAMES.copy()
        # save the original open function for use in pytest plugin
        self.original_open = open
        self.patch_open_code = patch_open_code
        self.tempfile_patcher = TempfilePatcher()
        self.linecache_patcher = LineCachePatcher()

        if additional_skip_names is not None:
            skip_names = [
                cast(ModuleType, m).__name__ if inspect.ismodule(m) else cast(str, m)
                for m in additional_skip_names
            ]
            self.skip_names.update(skip_names)

        self._fake_module_classes: Dict[str, Any] = {}
        self._unfaked_module_classes: Dict[str, Any] = {}
        self._class_modules: Dict[str, List[str]] = {}
        self._init_fake_module_classes()

        # reload tempfile under posix to patch default argument
        self.modules_to_reload: List[ModuleType] = []
        if modules_to_reload is not None:
            self.modules_to_reload.extend(modules_to_reload)
        self.patch_default_args = patch_default_args
        self.use_cache = use_cache
        self.use_dynamic_patch = use_dynamic_patch
        self.cleanup_handlers: Dict[str, Callable[[str], bool]] = {}

        if use_known_patches:
            from pyfakefs.patched_packages import (
                get_modules_to_patch,
                get_classes_to_patch,
                get_fake_module_classes,
                get_cleanup_handlers,
            )

            modules_to_patch = modules_to_patch or {}
            modules_to_patch.update(get_modules_to_patch())
            self._class_modules.update(get_classes_to_patch())
            self._fake_module_classes.update(get_fake_module_classes())
            self.cleanup_handlers.update(get_cleanup_handlers())

        if modules_to_patch is not None:
            for name, fake_module in modules_to_patch.items():
                self._fake_module_classes[name] = fake_module
            patched_module_names = set(modules_to_patch)
        else:
            patched_module_names = set()
        clear_cache = not use_cache
        if use_cache:
            if patched_module_names != self.PATCHED_MODULE_NAMES:
                self.__class__.PATCHED_MODULE_NAMES = patched_module_names
                clear_cache = True
            if self.skip_names != self.ADDITIONAL_SKIP_NAMES:
                self.__class__.ADDITIONAL_SKIP_NAMES = self.skip_names
                clear_cache = True
            if patch_default_args != self.PATCH_DEFAULT_ARGS:
                self.__class__.PATCH_DEFAULT_ARGS = patch_default_args
                clear_cache = True

        if clear_cache:
            self.clear_cache()
        self._fake_module_functions: Dict[str, Dict] = {}
        self._init_fake_module_functions()

        # Attributes set by _refresh()
        self._stubs: Optional[StubOutForTesting] = None
        self.fs: Optional[FakeFilesystem] = None
        self.fake_modules: Dict[str, Any] = {}
        self.unfaked_modules: Dict[str, Any] = {}

        # _isStale is set by tearDown(), reset by _refresh()
        self._isStale = True
        self._dyn_patcher: Optional[DynamicPatcher] = None
        self._patching = False
        self._paused = False
        self.has_copy_file_range = False
        self.has_copy_file = False

    @classmethod
    def clear_fs_cache(cls) -> None:
        """Clear the module cache."""
        cls.CACHED_MODULES = set()
        cls.FS_MODULES = {}
        cls.FS_FUNCTIONS = {}
        cls.FS_DEFARGS = []
        cls.SKIPPED_FS_MODULES = {}

    def clear_cache(self) -> None:
        """Clear the module cache (convenience instance method)."""
        self.__class__.clear_fs_cache()

    def register_cleanup_handler(self, name: str, handler: Callable[[str], bool]):
        """Register a handler for cleaning up a module after it had been loaded by
        the dynamic patcher. This allows to handle modules that cannot be reloaded
        without unwanted side effects.

        Args:
            name: The fully qualified module name.
            handler: A callable that may do any module cleanup, or do nothing
                and return `True` in case reloading shall be prevented.

        Returns:
            `True` if no further cleanup/reload shall occur after the handler is
                executed, `False` if the cleanup/reload shall still happen.
        """
        self.cleanup_handlers[name] = handler

    def _init_fake_module_classes(self) -> None:
        # IMPORTANT TESTING NOTE: Whenever you add a new module below, test
        # it by adding an attribute in fixtures/module_with_attributes.py
        # and a test in fake_filesystem_unittest_test.py, class
        # TestAttributesWithFakeModuleNames.
        self._fake_module_classes = {
            "os": fake_os.FakeOsModule,
            "shutil": fake_filesystem_shutil.FakeShutilModule,
            "io": fake_io.FakeIoModule,
            "pathlib": fake_pathlib.FakePathlibModule,
        }
        if sys.version_info >= (3, 13):
            # for Python 3.13, we need both pathlib (path with __init__.py) and
            # pathlib._local (has the actual implementation);
            # depending on how pathlib is imported, either may be used
            self._fake_module_classes["pathlib._local"] = fake_pathlib.FakePathlibModule
        if IS_PYPY or sys.version_info >= (3, 12):
            # in PyPy and later cpython versions, the module is referenced as _io
            self._fake_module_classes["_io"] = fake_io.FakeIoModule2
        if sys.platform == "win32":
            self._fake_module_classes["nt"] = fake_path.FakeNtModule
        else:
            self._fake_module_classes["fcntl"] = fake_filesystem.FakeFcntlModule

        # class modules maps class names against a list of modules they can
        # be contained in - this allows for alternative modules like
        # `pathlib` and `pathlib2`
        self._class_modules["Path"] = ["pathlib"]
        if sys.version_info >= (3, 13):
            self._class_modules["Path"].append("pathlib._local")
        self._unfaked_module_classes["pathlib"] = fake_pathlib.RealPathlibModule
        if sys.version_info >= (3, 13):
            self._unfaked_module_classes["pathlib._local"] = (
                fake_pathlib.RealPathlibModule
            )
        if pathlib2:
            self._fake_module_classes["pathlib2"] = (
                fake_legacy_modules.FakePathlib2Module
            )
            self._class_modules["Path"].append("pathlib2")
            self._unfaked_module_classes["pathlib2"] = fake_pathlib.RealPathlibModule
        if scandir:
            self._fake_module_classes["scandir"] = fake_legacy_modules.FakeScanDirModule
        self._fake_module_classes["Path"] = fake_pathlib.FakePathlibPathModule
        self._unfaked_module_classes["Path"] = fake_pathlib.RealPathlibPathModule

    def _init_fake_module_functions(self) -> None:
        # handle patching function imported separately like
        # `from os import stat`
        # each patched function name has to be looked up separately
        for mod_name, fake_module in self._fake_module_classes.items():
            if hasattr(fake_module, "dir"):
                module_dir = fake_module.dir
                if inspect.isfunction(module_dir):
                    for fct_name in fake_module.dir():
                        module_attr = (getattr(fake_module, fct_name), mod_name)
                        self._fake_module_functions.setdefault(fct_name, {})[
                            mod_name
                        ] = module_attr
                        if mod_name == "os":
                            self._fake_module_functions.setdefault(fct_name, {})[
                                OS_MODULE
                            ] = module_attr

        # special handling for functions in os.path
        fake_module = fake_filesystem.FakePathModule
        for fct_name in fake_module.dir():
            module_attr = (getattr(fake_module, fct_name), PATH_MODULE)
            self._fake_module_functions.setdefault(fct_name, {})["genericpath"] = (
                module_attr
            )
            self._fake_module_functions.setdefault(fct_name, {})[PATH_MODULE] = (
                module_attr
            )

    def __enter__(self) -> "Patcher":
        """Context manager for usage outside of
        fake_filesystem_unittest.TestCase.
        Ensure that all patched modules are removed in case of an
        unhandled exception.
        """
        self.setUp()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.tearDown()

    def _is_fs_module(
        self, mod: ModuleType, name: str, module_names: List[str]
    ) -> bool:
        try:
            return (
                inspect.ismodule(mod)
                and mod.__name__ in module_names
                or inspect.isclass(mod)
                and mod.__module__ in self._class_modules.get(name, [])
            )
        except Exception:
            # handle cases where the module has no __name__ or __module__
            # attribute - see #460, and any other exception triggered
            # by inspect functions
            return False

    def _is_fs_function(self, fct: FunctionType) -> bool:
        try:
            return (
                (inspect.isfunction(fct) or inspect.isbuiltin(fct))
                and fct.__name__ in self._fake_module_functions
                and fct.__module__ in self._fake_module_functions[fct.__name__]
            )
        except Exception:
            # handle cases where the function has no __name__ or __module__
            # attribute, or any other exception in inspect functions
            return False

    def _def_values(
        self, item: FunctionType
    ) -> Iterator[Tuple[FunctionType, int, Any]]:
        """Find default arguments that are file-system functions to be
        patched in top-level functions and members of top-level classes."""
        # check for module-level functions
        try:
            if item.__defaults__ and inspect.isfunction(item):
                for i, d in enumerate(item.__defaults__):
                    if self._is_fs_function(d):
                        yield item, i, d
        except Exception:
            pass
        try:
            if inspect.isclass(item):
                # check for methods in class
                # (nested classes are ignored for now)
                # inspect.getmembers is very expansive!
                for m in inspect.getmembers(item, predicate=inspect.isfunction):
                    f = cast(FunctionType, m[1])
                    if f.__defaults__:
                        for i, d in enumerate(f.__defaults__):
                            if self._is_fs_function(d):
                                yield f, i, d
        except Exception:
            # Ignore any exception, examples:
            # ImportError: No module named '_gdbm'
            # _DontDoThat() (see #523)
            pass

    def _find_def_values(self, module_items: ItemsView[str, FunctionType]) -> None:
        for _, fct in module_items:
            for f, i, d in self._def_values(fct):
                self.__class__.FS_DEFARGS.append((f, i, d))

    def _find_modules(self) -> None:
        """Find and cache all modules that import file system modules.
        Later, `setUp()` will stub these with the fake file system
        modules.
        """
        module_names = list(self._fake_module_classes.keys()) + [PATH_MODULE]
        for name, module in list(sys.modules.items()):
            try:
                if (
                    self.use_cache
                    and module in self.CACHED_MODULES
                    or not inspect.ismodule(module)
                ):
                    continue
            except Exception:
                # workaround for some py (part of pytest) versions
                # where py.error has no __name__ attribute
                # see https://github.com/pytest-dev/py/issues/73
                # and any other exception triggered by inspect.ismodule
                if self.use_cache:
                    try:
                        self.__class__.CACHED_MODULES.add(module)
                    except TypeError:
                        # unhashable module - don't cache it
                        pass
                continue
            skipped = module in self.SKIPMODULES or any(
                [sn.startswith(module.__name__) for sn in self.skip_names]
            )
            module_items = module.__dict__.copy().items()

            modules = {
                name: mod
                for name, mod in module_items
                if self._is_fs_module(mod, name, module_names)
            }

            if skipped:
                for name, mod in modules.items():
                    self.__class__.SKIPPED_FS_MODULES.setdefault(name, set()).add(
                        (module, mod.__name__)
                    )
            else:
                for name, mod in modules.items():
                    self.__class__.FS_MODULES.setdefault(name, set()).add(
                        (module, mod.__name__)
                    )
                functions = {
                    name: fct for name, fct in module_items if self._is_fs_function(fct)
                }

                for name, fct in functions.items():
                    self.__class__.FS_FUNCTIONS.setdefault(
                        (name, fct.__name__, fct.__module__), set()
                    ).add(module)

                # find default arguments that are file system functions
                if self.patch_default_args:
                    self._find_def_values(module_items)

            if self.use_cache:
                self.__class__.CACHED_MODULES.add(module)

    def _refresh(self) -> None:
        """Renew the fake file system and set the _isStale flag to `False`."""
        if self._stubs is not None:
            self._stubs.smart_unset_all()
        self._stubs = mox3_stubout.StubOutForTesting()

        self.fs = fake_filesystem.FakeFilesystem(patcher=self, create_temp_dir=True)
        self.fs.patch_open_code = self.patch_open_code
        for name in self._fake_module_classes:
            self.fake_modules[name] = self._fake_module_classes[name](self.fs)
            if hasattr(self.fake_modules[name], "skip_names"):
                self.fake_modules[name].skip_names = self.skip_names
        self.fake_modules[PATH_MODULE] = self.fake_modules["os"].path
        for name in self._unfaked_module_classes:
            self.unfaked_modules[name] = self._unfaked_module_classes[name]()

        self._isStale = False

    def setUp(self, doctester: Any = None) -> None:
        """Bind the file-related modules to the :py:mod:`pyfakefs` fake
        modules real ones.  Also bind the fake `file()` and `open()` functions.
        """
        if self.is_doc_test:
            self.__class__.DOC_REF_COUNT += 1
            if self.__class__.DOC_REF_COUNT > 1:
                return
        else:
            self.__class__.REF_COUNT += 1
            if self.__class__.REF_COUNT > 1:
                return
        self.has_fcopy_file = (
            sys.platform == "darwin"
            and hasattr(shutil, "_HAS_FCOPYFILE")
            and shutil._HAS_FCOPYFILE
        )
        if self.has_fcopy_file:
            shutil._HAS_FCOPYFILE = False  # type: ignore[attr-defined]

        self.has_copy_file_range = (
            sys.platform == "linux"
            and hasattr(shutil, "_USE_CP_COPY_FILE_RANGE")
            and shutil._USE_CP_COPY_FILE_RANGE
        )
        if self.has_copy_file_range:
            shutil._USE_CP_COPY_FILE_RANGE = False  # type: ignore[attr-defined]

        # do not use the fd functions, as they may not be available in the target OS
        if hasattr(shutil, "_use_fd_functions"):
            shutil._use_fd_functions = False  # type: ignore[module-attr]
        # in Python 3.14, _rmtree_impl is set at load time based on _use_fd_functions
        # the safe version cannot be used at the moment as it used asserts of type
        # 'assert func is os.rmtree', which do not work with the fake versions
        if hasattr(shutil, "_rmtree_impl"):
            shutil._rmtree_impl = shutil._rmtree_unsafe  # type: ignore[attr-defined]

        with warnings.catch_warnings():
            # ignore warnings, see #542 and #614
            warnings.filterwarnings("ignore")
            self._find_modules()

        self._refresh()

        if doctester is not None:
            doctester.globs = self.replace_globs(doctester.globs)

        self.start_patching()
        linecache.open = self.original_open  # type: ignore[attr-defined]
        tokenize._builtin_open = self.original_open  # type: ignore

    def start_patching(self) -> None:
        if not self._patching:
            self._patching = True
            self._paused = False

            self.linecache_patcher.start_patching()
            self.tempfile_patcher.start_patching()

            self.patch_modules()
            self.patch_functions()
            self.patch_defaults()
            self._set_glob_os_functions()

            self._dyn_patcher = DynamicPatcher(self)
            sys.meta_path.insert(0, self._dyn_patcher)
            for module in self.modules_to_reload:
                if sys.modules.get(module.__name__) is module:
                    reload(module)
            if not self.use_dynamic_patch:
                self._dyn_patcher.cleanup()
                sys.meta_path.pop(0)

    def _set_glob_os_functions(self):
        # make sure the os functions cached in glob are patched
        if sys.version_info >= (3, 13):
            globber = glob._StringGlobber  # type: ignore[module-attr]
            globber.lstat = staticmethod(os.lstat)
            if sys.version_info < (3, 14):
                globber.scandir = staticmethod(os.scandir)

    def patch_functions(self) -> None:
        assert self._stubs is not None
        for (name, ft_name, ft_mod), modules in self.FS_FUNCTIONS.items():
            method, mod_name = self._fake_module_functions[ft_name][ft_mod]
            fake_module = self.fake_modules[mod_name]
            attr = method.__get__(
                fake_module, fake_module.__class__
            )  # pytype: disable=attribute-error
            for module in modules:
                self._stubs.smart_set(module, name, attr)

    def patch_modules(self) -> None:
        skip_prefix_list = []
        for rt_skip_module, prefixes in self.RUNTIME_SKIPMODULES.items():
            if rt_skip_module in sys.modules:
                skip_prefix_list.extend(prefixes)
        skip_prefixes = tuple(skip_prefix_list)

        assert self._stubs is not None
        for name, modules in self.FS_MODULES.items():
            for module, attr in modules:
                try:
                    if not skip_prefixes or not module.__name__.startswith(
                        skip_prefixes
                    ):
                        self._stubs.smart_set(module, name, self.fake_modules[attr])
                    elif attr in self.unfaked_modules:
                        self._stubs.smart_set(module, name, self.unfaked_modules[attr])
                except Exception:
                    # handle the rare case that a module has no __name__
                    pass

        for name, modules in self.SKIPPED_FS_MODULES.items():
            for module, attr in modules:
                if attr in self.unfaked_modules:
                    self._stubs.smart_set(module, name, self.unfaked_modules[attr])

    def patch_defaults(self) -> None:
        for fct, idx, ft in self.FS_DEFARGS:
            method, mod_name = self._fake_module_functions[ft.__name__][ft.__module__]
            fake_module = self.fake_modules[mod_name]
            attr = method.__get__(
                fake_module, fake_module.__class__
            )  # pytype: disable=attribute-error
            new_defaults = []
            assert fct.__defaults__ is not None
            for i, d in enumerate(fct.__defaults__):
                if i == idx:
                    new_defaults.append(attr)
                else:
                    new_defaults.append(d)
            fct.__defaults__ = tuple(new_defaults)

    def replace_globs(self, globs_: Dict[str, Any]) -> Dict[str, Any]:
        globs = globs_.copy()
        if self._isStale:
            self._refresh()
        for name in self._fake_module_classes:
            if name in globs:
                globs[name] = self._fake_module_classes[name](self.fs)
        return globs

    def tearDown(self, doctester: Any = None):
        """Clear the fake filesystem bindings created by `setUp()`."""
        if self.is_doc_test:
            self.__class__.DOC_REF_COUNT -= 1
            if self.__class__.DOC_REF_COUNT > 0:
                return
        else:
            self.__class__.REF_COUNT -= 1
            if self.__class__.REF_COUNT > 0:
                return
        self.stop_patching()
        if self.has_fcopy_file:
            shutil._HAS_FCOPYFILE = True  # type: ignore[attr-defined]
        if self.has_copy_file_range:
            shutil._USE_CP_COPY_FILE_RANGE = True  # type: ignore[attr-defined]

        reset_ids()
        if self.is_doc_test:
            self.__class__.DOC_PATCHER = None
        else:
            self.__class__.PATCHER = None

    def stop_patching(self, temporary=False) -> None:
        if self._patching:
            self._isStale = True
            self._patching = False
            self._paused = temporary
            if self._stubs:
                self._stubs.smart_unset_all()
            self.unset_defaults()
            if self.use_dynamic_patch and self._dyn_patcher:
                self._dyn_patcher.cleanup()
                sys.meta_path.pop(0)
            self.tempfile_patcher.stop_patching()
            self.linecache_patcher.stop_patching()
            self._set_glob_os_functions()

    @property
    def is_patching(self):
        return self._patching

    def unset_defaults(self) -> None:
        for fct, idx, ft in self.FS_DEFARGS:
            new_defaults = []
            for i, d in enumerate(cast(Tuple, fct.__defaults__)):
                if i == idx:
                    new_defaults.append(ft)
                else:
                    new_defaults.append(d)
            fct.__defaults__ = tuple(new_defaults)

    def pause(self) -> None:
        """Pause the patching of the file system modules until `resume` is
        called. After that call, all file system calls are executed in the
        real file system.
        Calling pause() twice is silently ignored.

        """
        self.stop_patching(temporary=True)

    def resume(self) -> None:
        """Resume the patching of the file system modules if `pause` has
        been called before. After that call, all file system calls are
        executed in the fake file system.
        Does nothing if patching is not paused.
        """
        if self._paused:
            self.start_patching()


class Pause:
    """Simple context manager that allows to pause/resume patching the
    filesystem. Patching is paused in the context manager, and resumed after
    going out of its scope.
    """

    def __init__(self, caller: Union[Patcher, TestCaseMixin, FakeFilesystem]):
        """Initializes the context manager with the fake filesystem.

        Args:
            caller: either the FakeFilesystem instance, the Patcher instance
                or the pyfakefs test case.
        """
        if isinstance(caller, (Patcher, TestCaseMixin)):
            assert caller.fs is not None
            self._fs: FakeFilesystem = caller.fs
        elif isinstance(caller, FakeFilesystem):
            self._fs = caller
        else:
            raise ValueError(
                "Invalid argument - should be of type "
                '"fake_filesystem_unittest.Patcher", '
                '"fake_filesystem_unittest.TestCase" '
                'or "fake_filesystem.FakeFilesystem"'
            )

    def __enter__(self) -> FakeFilesystem:
        self._fs.pause()
        return self._fs

    def __exit__(self, *args: Any) -> None:
        self._fs.resume()


class DynamicPatcher(MetaPathFinder, Loader):
    """A file loader that replaces file system related modules by their
    fake implementation if they are loaded after calling `setUpPyfakefs()`.
    Implements the protocol needed for import hooks.
    """

    def __init__(self, patcher: Patcher) -> None:
        self._patcher = patcher
        self.sysmodules = {}
        self.modules = self._patcher.fake_modules
        self._loaded_module_names: Set[str] = set()
        self.cleanup_handlers = patcher.cleanup_handlers

        # remove all modules that have to be patched from `sys.modules`,
        # otherwise the find_... methods will not be called
        for name in self.modules:
            if self.needs_patch(name) and name in sys.modules:
                self.sysmodules[name] = sys.modules[name]
                del sys.modules[name]

        for name, module in self.modules.items():
            sys.modules[name] = module

    def cleanup(self) -> None:
        for module_name in self.sysmodules:
            sys.modules[module_name] = self.sysmodules[module_name]
        for module in self._patcher.modules_to_reload:
            if module.__name__ in sys.modules:
                reload(module)
        reloaded_module_names = [
            module.__name__ for module in self._patcher.modules_to_reload
        ]
        # Delete all modules loaded during the test, ensuring that
        # they are reloaded after the test.
        for name in self._loaded_module_names:
            if name in sys.modules and name not in reloaded_module_names:
                if name in self.cleanup_handlers and self.cleanup_handlers[name](name):
                    continue
                del sys.modules[name]

    def needs_patch(self, name: str) -> bool:
        """Checks if the module with the given name shall be replaced."""
        if name not in self.modules:
            self._loaded_module_names.add(name)
            return False
        if name in sys.modules and type(sys.modules[name]) is self.modules[name]:
            return False
        return True

    def fake_module_path(self, name: str) -> str:
        """Checks if the module with the given name is a module existing in the fake
        filesystem and returns its path in this case.
        """
        fs = self._patcher.fs
        # we assume that the module name is the absolute module path
        if fs is not None:
            base_path = name.replace(".", fs.path_separator)
            for path in sys.path:
                module_path = fs.joinpaths(path, base_path)
                py_module_path = module_path + ".py"
                if fs.exists(py_module_path):
                    return fs.absnormpath(py_module_path)
                init_path = fs.joinpaths(module_path, "__init__.py")
                if fs.exists(init_path):
                    return fs.absnormpath(init_path)
        return ""

    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[Union[bytes, str]]],
        target: Optional[ModuleType] = None,
    ) -> Optional[ModuleSpec]:
        """Module finder."""
        if self.needs_patch(fullname):
            return ModuleSpec(fullname, self)
        if self._patcher.patch_open_code != PatchMode.OFF:
            # handle modules created in the fake filesystem
            module_path = self.fake_module_path(fullname)
            if module_path:
                spec = spec_from_file_location(fullname, module_path)
                if spec:
                    module = module_from_spec(spec)
                    sys.modules[fullname] = module
                    return ModuleSpec(fullname, self)
        return None

    def load_module(self, fullname: str) -> ModuleType:
        """Replaces the module by its fake implementation."""
        sys.modules[fullname] = self.modules[fullname]
        return self.modules[fullname]
