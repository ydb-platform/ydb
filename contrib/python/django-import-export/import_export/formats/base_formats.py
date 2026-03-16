# when adding imports, ensure that they are local to the
# correct class for the file format.
# e.g. add openpyxl imports to the XLSXFormat class
# See issue 2004
import logging
import warnings

import tablib
from django.conf import settings
from django.utils.translation import gettext_lazy as _
from tablib.formats import registry

logger = logging.getLogger(__name__)


class Format:
    def get_title(self):
        return type(self)

    def create_dataset(self, in_stream):
        """
        Create dataset from given string.
        """
        raise NotImplementedError()

    def export_data(self, dataset, **kwargs):
        """
        Returns format representation for given dataset.
        """
        raise NotImplementedError()

    def is_binary(self):
        """
        Returns if this format is binary.
        """
        return True

    def get_read_mode(self):
        """
        Returns mode for opening files.
        """
        return "rb"

    def get_extension(self):
        """
        Returns extension for this format files.
        """
        return ""

    def get_content_type(self):
        # For content types see
        # https://www.iana.org/assignments/media-types/media-types.xhtml
        return "application/octet-stream"

    @classmethod
    def is_available(cls):
        return True

    def can_import(self):
        return False

    def can_export(self):
        return False


class TablibFormat(Format):
    TABLIB_MODULE = None
    CONTENT_TYPE = "application/octet-stream"

    def __init__(self, encoding=None):
        self.encoding = encoding

    def get_format(self):
        """
        Import and returns tablib module.
        """
        if not self.TABLIB_MODULE:
            raise AttributeError("TABLIB_MODULE must be defined")
        key = self.TABLIB_MODULE.split(".")[-1].replace("_", "")
        return registry.get_format(key)

    @classmethod
    def is_available(cls):
        try:
            cls().get_format()
        except (tablib.core.UnsupportedFormat, ImportError):
            return False
        return True

    def get_title(self):
        return self.get_format().title

    def create_dataset(self, in_stream, **kwargs):
        return tablib.import_set(in_stream, format=self.get_title(), **kwargs)

    def export_data(self, dataset, **kwargs):
        if getattr(settings, "IMPORT_EXPORT_ESCAPE_FORMULAE_ON_EXPORT", False) is True:
            self._escape_formulae(dataset)
        return dataset.export(self.get_title(), **kwargs)

    def get_extension(self):
        return self.get_format().extensions[0]

    def get_content_type(self):
        return self.CONTENT_TYPE

    def can_import(self):
        return hasattr(self.get_format(), "import_set")

    def can_export(self):
        return hasattr(self.get_format(), "export_set")

    def _escape_formulae(self, dataset):
        def _do_escape(s):
            return s.replace("=", "", 1) if s.startswith("=") else s

        for r in dataset:
            row = dataset.lpop()
            row = [_do_escape(str(cell)) for cell in row]
            dataset.append(row)


class TextFormat(TablibFormat):
    def create_dataset(self, in_stream, **kwargs):
        if isinstance(in_stream, bytes) and self.encoding:
            in_stream = in_stream.decode(self.encoding)
        return super().create_dataset(in_stream, **kwargs)

    def get_read_mode(self):
        return "r"

    def is_binary(self):
        return False


class CSV(TextFormat):
    TABLIB_MODULE = "tablib.formats._csv"
    CONTENT_TYPE = "text/csv"


class JSON(TextFormat):
    TABLIB_MODULE = "tablib.formats._json"
    CONTENT_TYPE = "application/json"


class YAML(TextFormat):
    TABLIB_MODULE = "tablib.formats._yaml"
    # See https://stackoverflow.com/questions/332129/yaml-mime-type
    CONTENT_TYPE = "text/yaml"


class TSV(TextFormat):
    TABLIB_MODULE = "tablib.formats._tsv"
    CONTENT_TYPE = "text/tab-separated-values"


class ODS(TextFormat):
    TABLIB_MODULE = "tablib.formats._ods"
    CONTENT_TYPE = "application/vnd.oasis.opendocument.spreadsheet"


class HTML(TextFormat):
    TABLIB_MODULE = "tablib.formats._html"
    CONTENT_TYPE = "text/html"


class XLS(TablibFormat):
    TABLIB_MODULE = "tablib.formats._xls"
    CONTENT_TYPE = "application/vnd.ms-excel"

    def create_dataset(self, in_stream):
        """
        Create dataset from first sheet.
        """
        import xlrd

        xls_book = xlrd.open_workbook(file_contents=in_stream)
        dataset = tablib.Dataset()
        sheet = xls_book.sheets()[0]

        dataset.headers = sheet.row_values(0)
        for i in range(1, sheet.nrows):
            dataset.append(sheet.row_values(i))
        return dataset


class XLSX(TablibFormat):
    TABLIB_MODULE = "tablib.formats._xlsx"
    CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    def create_dataset(self, in_stream):
        """
        Create dataset from first sheet.
        """
        from io import BytesIO

        import openpyxl

        # 'data_only' means values are read from formula cells, not the formula itself
        xlsx_book = openpyxl.load_workbook(
            BytesIO(in_stream), read_only=True, data_only=True
        )

        dataset = tablib.Dataset()
        sheet = xlsx_book.active

        # obtain generator
        rows = sheet.rows
        dataset.headers = [cell.value for cell in next(rows)]

        ignore_blanks = getattr(
            settings, "IMPORT_EXPORT_IMPORT_IGNORE_BLANK_LINES", False
        )
        for row in rows:
            row_values = [cell.value for cell in row]

            if ignore_blanks:
                # do not add empty rows to dataset
                if not all(value is None for value in row_values):
                    dataset.append(row_values)
            else:
                dataset.append(row_values)
        return dataset

    def export_data(self, dataset, **kwargs):
        from openpyxl.utils.exceptions import IllegalCharacterError

        # #1698 temporary catch for deprecation warning in openpyxl
        # this catch block must be removed when openpyxl updated
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            try:
                return super().export_data(dataset, **kwargs)
            except IllegalCharacterError as e:
                if (
                    getattr(
                        settings, "IMPORT_EXPORT_ESCAPE_ILLEGAL_CHARS_ON_EXPORT", False
                    )
                    is True
                ):
                    self._escape_illegal_chars(dataset)
                    return super().export_data(dataset, **kwargs)
                logger.exception(e)
                # not raising original error due to reflected xss risk
                raise ValueError(_("export failed due to IllegalCharacterError"))

    def _escape_illegal_chars(self, dataset):
        from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

        def _do_escape(cell):
            if type(cell) is str:
                cell = ILLEGAL_CHARACTERS_RE.sub("\N{REPLACEMENT CHARACTER}", cell)
            return cell

        for r in dataset:
            row = dataset.lpop()
            row = [_do_escape(cell) for cell in row]
            dataset.append(row)


#: These are the default formats for import and export. Whether they can be
#: used or not is depending on their implementation in the tablib library.
DEFAULT_FORMATS = [
    fmt
    for fmt in (
        CSV,
        XLS,
        XLSX,
        TSV,
        ODS,
        JSON,
        YAML,
        HTML,
    )
    if fmt.is_available()
]

#: These are the formats which support different data types (such as datetime
#: and numbers) for which `coerce_to_string` is to be set false dynamically.
BINARY_FORMATS = [
    fmt
    for fmt in (
        XLS,
        XLSX,
        ODS,
    )
    if fmt.is_available()
]
