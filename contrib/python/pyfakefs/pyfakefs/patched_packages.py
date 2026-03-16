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

"""
Provides patches for some commonly used modules that enable them to work
with pyfakefs.
"""

import sys
from importlib import reload

try:
    import pandas as pd

    try:
        import pandas.io.parsers as parsers
    except ImportError:
        parsers = None
except ImportError:
    pd = None
    parsers = None


try:
    import xlrd
except ImportError:
    xlrd = None


try:
    import django

    try:
        from django.core.files import locks
    except ImportError:
        locks = None
except ImportError:
    django = None
    locks = None

# From pandas v 1.2 onwards the python fs functions are used even when the engine
# selected is "c". This means that we don't explicitly have to change the engine.
patch_pandas = parsers is not None and [int(v) for v in pd.__version__.split(".")] < [
    1,
    2,
    0,
]


def get_modules_to_patch():
    modules_to_patch = {}
    if xlrd is not None:
        modules_to_patch["xlrd"] = XLRDModule
    if locks is not None:
        modules_to_patch["django.core.files.locks"] = FakeLocks
    return modules_to_patch


def get_classes_to_patch():
    classes_to_patch = {}
    if patch_pandas:
        classes_to_patch["TextFileReader"] = ["pandas.io.parsers"]
    return classes_to_patch


def reload_handler(name):
    if name in sys.modules:
        reload(sys.modules[name])
    return True


def get_cleanup_handlers():
    handlers = {}
    if pd is not None:
        handlers["pandas.core.arrays.arrow.extension_types"] = (
            handle_extension_type_cleanup
        )
    if django is not None:
        for module_name in django_view_modules():
            handlers[module_name] = lambda name=module_name: reload_handler(name)
    return handlers


def get_fake_module_classes():
    fake_module_classes = {}
    if patch_pandas:
        fake_module_classes["TextFileReader"] = FakeTextFileReader
    return fake_module_classes


if xlrd is not None:

    class XLRDModule:
        """Patches the xlrd module, which is used as the default Excel file
        reader by pandas. Disables using memory mapped files, which are
        implemented platform-specific on OS level."""

        def __init__(self, _):
            self._xlrd_module = xlrd

        def open_workbook(
            self,
            filename=None,
            logfile=sys.stdout,
            verbosity=0,
            use_mmap=False,
            file_contents=None,
            encoding_override=None,
            formatting_info=False,
            on_demand=False,
            ragged_rows=False,
        ):
            return self._xlrd_module.open_workbook(
                filename,
                logfile,
                verbosity,
                False,
                file_contents,
                encoding_override,
                formatting_info,
                on_demand,
                ragged_rows,
            )

        def __getattr__(self, name):
            """Forwards any unfaked calls to the standard xlrd module."""
            return getattr(self._xlrd_module, name)


if patch_pandas:
    # we currently need to add fake modules for both the parser module and
    # the contained text reader - maybe this can be simplified

    class FakeTextFileReader:
        fake_parsers = None

        def __init__(self, filesystem):
            if self.fake_parsers is None:
                self.__class__.fake_parsers = ParsersModule(filesystem)

        def __call__(self, *args, **kwargs):
            return self.fake_parsers.TextFileReader(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.fake_parsers.TextFileReader, name)

    class ParsersModule:
        def __init__(self, _):
            self._parsers_module = parsers

        class TextFileReader(parsers.TextFileReader):
            def __init__(self, *args, **kwargs):
                kwargs["engine"] = "python"
                super().__init__(*args, **kwargs)

        def __getattr__(self, name):
            """Forwards any unfaked calls to the standard xlrd module."""
            return getattr(self._parsers_module, name)


if pd is not None:

    def handle_extension_type_cleanup(_name):
        # the module registers two extension types on load
        # on reload it raises if the extensions have not been unregistered before
        try:
            import pyarrow

            # the code to register these types has been in the module
            # since it was created (in pandas 1.5)
            pyarrow.unregister_extension_type("pandas.interval")
            pyarrow.unregister_extension_type("pandas.period")
        except ImportError:
            pass
        return False


if locks is not None:

    class FakeLocks:
        """django.core.files.locks uses low level OS functions, fake it."""

        _locks_module = locks

        def __init__(self, _):
            pass

        @staticmethod
        def lock(f, flags):
            return True

        @staticmethod
        def unlock(f):
            return True

        def __getattr__(self, name):
            return getattr(self._locks_module, name)


if django is not None:

    def get_all_view_modules(urlpatterns, modules=None):
        if modules is None:
            modules = set()
        for pattern in urlpatterns:
            if hasattr(pattern, "url_patterns"):
                get_all_view_modules(pattern.url_patterns, modules=modules)
            else:
                if hasattr(pattern.callback, "cls"):
                    view = pattern.callback.cls
                elif hasattr(pattern.callback, "view_class"):
                    view = pattern.callback.view_class
                else:
                    view = pattern.callback
                modules.add(view.__module__)
        return modules

    def django_view_modules():
        try:
            all_urlpatterns = __import__(
                django.conf.settings.ROOT_URLCONF
            ).urls.urlpatterns
            return get_all_view_modules(all_urlpatterns)
        except Exception:
            return set()
