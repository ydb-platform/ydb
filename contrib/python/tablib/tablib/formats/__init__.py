""" Tablib - formats
"""
from functools import partialmethod
from importlib import import_module
from importlib.util import find_spec

from ..exceptions import UnsupportedFormat
from ..utils import normalize_input
from ._csv import CSVFormat
from ._json import JSONFormat
from ._tsv import TSVFormat

uninstalled_format_messages = {
    "cli": {"package_name": "tabulate package", "extras_name": "cli"},
    "df": {"package_name": "pandas package", "extras_name": "pandas"},
    "ods": {"package_name": "odfpy package", "extras_name": "ods"},
    "xls": {"package_name": "xlrd and xlwt packages", "extras_name": "xls"},
    "xlsx": {"package_name": "openpyxl package", "extras_name": "xlsx"},
    "yaml": {"package_name": "pyyaml package", "extras_name": "yaml"},
}


def load_format_class(dotted_path):
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
        return getattr(import_module(module_path), class_name)
    except (ValueError, AttributeError) as err:
        raise ImportError(f"Unable to load format class '{dotted_path}' ({err})")


class FormatDescriptorBase:
    def __init__(self, key, format_or_path):
        self.key = key
        self._format_path = None
        if isinstance(format_or_path, str):
            self._format = None
            self._format_path = format_or_path
        else:
            self._format = format_or_path

    def ensure_format_loaded(self):
        if self._format is None:
            self._format = load_format_class(self._format_path)


class ImportExportBookDescriptor(FormatDescriptorBase):
    def __get__(self, obj, cls, **kwargs):
        self.ensure_format_loaded()
        return self._format.export_book(obj, **kwargs)

    def __set__(self, obj, val):
        self.ensure_format_loaded()
        return self._format.import_book(obj, normalize_input(val))


class ImportExportSetDescriptor(FormatDescriptorBase):
    def __get__(self, obj, cls, **kwargs):
        self.ensure_format_loaded()
        return self._format.export_set(obj, **kwargs)

    def __set__(self, obj, val):
        self.ensure_format_loaded()
        return self._format.import_set(obj, normalize_input(val))


class Registry:
    _formats = {}

    def register(self, key, format_or_path):
        from ..core import Databook, Dataset

        # Create Databook.<format> read or read/write properties
        setattr(Databook, key, ImportExportBookDescriptor(key, format_or_path))

        # Create Dataset.<format> read or read/write properties,
        # and Dataset.get_<format>/set_<format> methods.
        setattr(Dataset, key, ImportExportSetDescriptor(key, format_or_path))
        try:
            setattr(Dataset, f'get_{key}', partialmethod(Dataset._get_in_format, key))
            setattr(Dataset, f'set_{key}', partialmethod(Dataset._set_in_format, key))
        except AttributeError:
            setattr(Dataset, f'get_{key}', partialmethod(Dataset._get_in_format, key))

        self._formats[key] = format_or_path

    def register_builtins(self):
        # Registration ordering matters for autodetection.
        self.register('json', JSONFormat())
        # xlsx before as xls (xlrd) can also read xlsx
        if find_spec('openpyxl'):
            self.register('xlsx', 'tablib.formats._xlsx.XLSXFormat')
        if find_spec('xlrd') and find_spec('xlwt'):
            self.register('xls', 'tablib.formats._xls.XLSFormat')
        if find_spec('yaml'):
            self.register('yaml', 'tablib.formats._yaml.YAMLFormat')
        self.register('csv', CSVFormat())
        self.register('tsv', TSVFormat())
        if find_spec('odf'):
            self.register('ods', 'tablib.formats._ods.ODSFormat')
        self.register('dbf', 'tablib.formats._dbf.DBFFormat')
        self.register('html', 'tablib.formats._html.HTMLFormat')
        self.register('jira', 'tablib.formats._jira.JIRAFormat')
        self.register('latex', 'tablib.formats._latex.LATEXFormat')
        if find_spec('pandas'):
            self.register('df', 'tablib.formats._df.DataFrameFormat')
        self.register('rst', 'tablib.formats._rst.ReSTFormat')
        self.register('sql', 'tablib.formats._sql.SQLFormat')
        if find_spec('tabulate'):
            self.register('cli', 'tablib.formats._cli.CLIFormat')

    def formats(self):
        for key, frm in self._formats.items():
            if isinstance(frm, str):
                self._formats[key] = load_format_class(frm)
            yield self._formats[key]

    def get_format(self, key):
        if key not in self._formats:
            if key in uninstalled_format_messages:
                raise UnsupportedFormat(
                    "The '{key}' format is not available. You may want to install the "
                    "{package_name} (or `pip install \"tablib[{extras_name}]\"`).".format(
                        **uninstalled_format_messages[key], key=key
                    )
                )
            raise UnsupportedFormat(f"Tablib has no format '{key}' or it is not registered.")
        if isinstance(self._formats[key], str):
            self._formats[key] = load_format_class(self._formats[key])
        return self._formats[key]


registry = Registry()
