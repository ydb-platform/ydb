#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Sequence
from typing import List

from sqlalchemy import false, true
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.roles import FromClauseRole

from .compat import string_types

NoneType = type(None)


def translate_bool(bln):
    if bln:
        return true()
    return false()


class MergeInto(UpdateBase):
    __visit_name__ = "merge_into"
    _bind = None

    def __init__(self, target, source, on):
        self.target = target
        self.source = source
        self.on = on
        self.clauses = []

    class clause(ClauseElement):
        __visit_name__ = "merge_into_clause"

        def __init__(self, command):
            self.set = {}
            self.predicate = None
            self.command = command

        def __repr__(self):
            case_predicate = (
                f" AND {str(self.predicate)}" if self.predicate is not None else ""
            )
            if self.command == "INSERT":
                sets, sets_tos = zip(*self.set.items())
                return "WHEN NOT MATCHED{} THEN {} ({}) VALUES ({})".format(
                    case_predicate,
                    self.command,
                    ", ".join(sets),
                    ", ".join(map(str, sets_tos)),
                )
            else:
                # WHEN MATCHED clause
                sets = (
                    ", ".join([f"{set[0]} = {set[1]}" for set in self.set.items()])
                    if self.set
                    else ""
                )
                return "WHEN MATCHED{} THEN {}{}".format(
                    case_predicate,
                    self.command,
                    f" SET {str(sets)}" if self.set else "",
                )

        def values(self, **kwargs):
            self.set = kwargs
            return self

        def where(self, expr):
            self.predicate = expr
            return self

    def __repr__(self):
        clauses = " ".join([repr(clause) for clause in self.clauses])
        return f"MERGE INTO {self.target} USING {self.source} ON {self.on}" + (
            f" {clauses}" if clauses else ""
        )

    def when_matched_then_update(self):
        clause = self.clause("UPDATE")
        self.clauses.append(clause)
        return clause

    def when_matched_then_delete(self):
        clause = self.clause("DELETE")
        self.clauses.append(clause)
        return clause

    def when_not_matched_then_insert(self):
        clause = self.clause("INSERT")
        self.clauses.append(clause)
        return clause


class FilesOption:
    """
    Class to represent FILES option for the snowflake COPY INTO statement
    """

    def __init__(self, file_names: List[str]):
        self.file_names = file_names

    def __str__(self):
        the_files = ["'" + f.replace("'", "\\'") + "'" for f in self.file_names]
        return f"({','.join(the_files)})"


class CopyInto(UpdateBase):
    """Copy Into Command base class, for documentation see:
    https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html"""

    __visit_name__ = "copy_into"
    _bind = None

    def __init__(self, from_, into, formatter=None):
        self.from_ = from_
        self.into = into
        self.formatter = formatter
        self.copy_options = {}

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in base.py
        """
        return f"COPY INTO {self.into} FROM {repr(self.from_)} {repr(self.formatter)} ({self.copy_options})"

    def bind(self):
        return None

    def force(self, force):
        if not isinstance(force, bool):
            raise TypeError("Parameter force should be a boolean value")
        self.copy_options.update({"FORCE": translate_bool(force)})
        return self

    def single(self, single_file):
        if not isinstance(single_file, bool):
            raise TypeError("Parameter single_file should  be a boolean value")
        self.copy_options.update({"SINGLE": translate_bool(single_file)})
        return self

    def maxfilesize(self, max_size):
        if not isinstance(max_size, int):
            raise TypeError("Parameter max_size should be an integer value")
        self.copy_options.update({"MAX_FILE_SIZE": max_size})
        return self

    def files(self, file_names):
        self.copy_options.update({"FILES": FilesOption(file_names)})
        return self

    def pattern(self, pattern):
        self.copy_options.update({"PATTERN": pattern})
        return self


class CopyFormatter(ClauseElement):
    """
    Base class for Formatter specifications inside a COPY INTO statement. May also
    be used to create a named format.
    """

    __visit_name__ = "copy_formatter"

    def __init__(self, format_name=None):
        self.options = dict()
        if format_name:
            self.options["format_name"] = format_name

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in base.py
        """
        return f"FILE_FORMAT=({self.options})"

    @staticmethod
    def value_repr(name, value):
        """
        Make a SQL-suitable representation of "value". This is called from
        the corresponding visitor function (base.py/visit_copy_formatter())
        - in case of a format name: return it without quotes
        - in case of a string: enclose in quotes: "value"
        - in case of a tuple of length 1: enclose the only element in brackets: (value)
            Standard stringification of Python would append a trailing comma: (value,)
            which is not correct in SQL
        - otherwise: just convert to str as is: value
        """
        if name == "format_name":
            return value
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, tuple) and len(value) == 1:
            return f"('{value[0]}')"
        else:
            return str(value)


class CSVFormatter(CopyFormatter):
    file_format = "csv"

    def compression(self, comp_type):
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, string_types):
            comp_type = comp_type.lower()
        _available_options = [
            "auto",
            "gzip",
            "bz2",
            "brotli",
            "zstd",
            "deflate",
            "raw_deflate",
            None,
        ]
        if comp_type not in _available_options:
            raise TypeError(f"Compression type should be one of : {_available_options}")
        self.options["COMPRESSION"] = comp_type
        return self

    def _check_delimiter(self, delimiter, delimiter_txt):
        """
        Check if a delimiter is either a string of length 1 or an integer. In case of
        a string delimiter, take into account that the actual string may be longer,
        but still evaluate to a single character (like "\\n" or r"\n"
        """
        if isinstance(delimiter, NoneType):
            return
        if isinstance(delimiter, string_types):
            delimiter_processed = delimiter.encode().decode("unicode_escape")
            if len(delimiter_processed) == 1:
                return
        if isinstance(delimiter, int):
            return
        raise TypeError(
            f"{delimiter_txt} should be a single character, that is either a string, or a number"
        )

    def record_delimiter(self, deli_type):
        """Character that separates records in an unloaded file."""
        self._check_delimiter(deli_type, "Record delimiter")
        if isinstance(deli_type, int):
            self.options["RECORD_DELIMITER"] = hex(deli_type)
        else:
            self.options["RECORD_DELIMITER"] = deli_type
        return self

    def field_delimiter(self, deli_type):
        """Character that separates fields in an unloaded file."""
        self._check_delimiter(deli_type, "Field delimiter")
        if isinstance(deli_type, int):
            self.options["FIELD_DELIMITER"] = hex(deli_type)
        else:
            self.options["FIELD_DELIMITER"] = deli_type
        return self

    def file_extension(self, ext):
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service.
        """
        if not isinstance(ext, (NoneType, string_types)):
            raise TypeError("File extension should be a string")
        self.options["FILE_EXTENSION"] = ext
        return self

    def date_format(self, dt_frmt):
        """String that defines the format of date values in the unloaded data files."""
        if not isinstance(dt_frmt, string_types):
            raise TypeError("Date format should be a string")
        self.options["DATE_FORMAT"] = dt_frmt
        return self

    def time_format(self, tm_frmt):
        """String that defines the format of time values in the unloaded data files."""
        if not isinstance(tm_frmt, string_types):
            raise TypeError("Time format should be a string")
        self.options["TIME_FORMAT"] = tm_frmt
        return self

    def timestamp_format(self, tmstmp_frmt):
        """String that defines the format of timestamp values in the unloaded data files."""
        if not isinstance(tmstmp_frmt, string_types):
            raise TypeError("Timestamp format should be a string")
        self.options["TIMESTAMP_FORMAT"] = tmstmp_frmt
        return self

    def binary_format(self, bin_fmt):
        """Character used as the escape character for any field values. The option can be used when unloading data
        from binary columns in a table."""
        if isinstance(bin_fmt, string_types):
            bin_fmt = bin_fmt.lower()
        _available_options = ["hex", "base64", "utf8"]
        if bin_fmt not in _available_options:
            raise TypeError(f"Binary format should be one of : {_available_options}")
        self.options["BINARY_FORMAT"] = bin_fmt
        return self

    def escape(self, esc):
        """Character used as the escape character for any field values."""
        self._check_delimiter(esc, "Escape")
        if isinstance(esc, int):
            self.options["ESCAPE"] = hex(esc)
        else:
            self.options["ESCAPE"] = esc
        return self

    def escape_unenclosed_field(self, esc):
        """Single character string used as the escape character for unenclosed field values only."""
        self._check_delimiter(esc, "Escape unenclosed field")
        if isinstance(esc, int):
            self.options["ESCAPE_UNENCLOSED_FIELD"] = hex(esc)
        else:
            self.options["ESCAPE_UNENCLOSED_FIELD"] = esc
        return self

    def field_optionally_enclosed_by(self, enc):
        """Character used to enclose strings. Either None, ', or \"."""
        _available_options = [None, "'", '"']
        if enc not in _available_options:
            raise TypeError(f"Enclosing string should be one of : {_available_options}")
        self.options["FIELD_OPTIONALLY_ENCLOSED_BY"] = enc
        return self

    def null_if(self, null_value):
        """Copying into a table these strings will be replaced by a NULL, while copying out of Snowflake will replace
        NULL values with the first string"""
        if not isinstance(null_value, Sequence):
            raise TypeError("Parameter null_value should be an iterable")
        self.options["NULL_IF"] = tuple(null_value)
        return self

    def skip_header(self, skip_header):
        """
        Number of header rows to be skipped at the beginning of the file
        """
        if not isinstance(skip_header, int):
            raise TypeError("skip_header  should be an int")
        self.options["SKIP_HEADER"] = skip_header
        return self

    def trim_space(self, trim_space):
        """
        Remove leading or trailing white spaces
        """
        if not isinstance(trim_space, bool):
            raise TypeError("trim_space should be a bool")
        self.options["TRIM_SPACE"] = trim_space
        return self

    def error_on_column_count_mismatch(self, error_on_col_count_mismatch):
        """
        Generate a parsing error if the number of delimited columns (i.e. fields) in
        an input data file does not match the number of columns in the corresponding table.
        """
        if not isinstance(error_on_col_count_mismatch, bool):
            raise TypeError("skip_header  should be a bool")
        self.options["ERROR_ON_COLUMN_COUNT_MISMATCH"] = error_on_col_count_mismatch
        return self


class JSONFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = "json"

    def compression(self, comp_type):
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, string_types):
            comp_type = comp_type.lower()
        _available_options = [
            "auto",
            "gzip",
            "bz2",
            "brotli",
            "zstd",
            "deflate",
            "raw_deflate",
            None,
        ]
        if comp_type not in _available_options:
            raise TypeError(f"Compression type should be one of : {_available_options}")
        self.options["COMPRESSION"] = comp_type
        return self

    def file_extension(self, ext):
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service.
        """
        if not isinstance(ext, (NoneType, string_types)):
            raise TypeError("File extension should be a string")
        self.options["FILE_EXTENSION"] = ext
        return self


class PARQUETFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = "parquet"

    def snappy_compression(self, comp):
        """Enable, or disable snappy compression"""
        if not isinstance(comp, bool):
            raise TypeError("Comp should be a Boolean value")
        self.options["SNAPPY_COMPRESSION"] = translate_bool(comp)
        return self

    def compression(self, comp):
        """
        Set compression type
        """
        if not isinstance(comp, str):
            raise TypeError("Comp should be a str value")
        self.options["COMPRESSION"] = comp
        return self

    def binary_as_text(self, value):
        """Enable, or disable binary as text"""
        if not isinstance(value, bool):
            raise TypeError("binary_as_text should be a Boolean value")
        self.options["BINARY_AS_TEXT"] = translate_bool(value)
        return self


class ExternalStage(ClauseElement, FromClauseRole):
    """External Stage descriptor"""

    __visit_name__ = "external_stage"
    _hide_froms = ()

    @staticmethod
    def prepare_namespace(namespace):
        return f"{namespace}." if not namespace.endswith(".") else namespace

    @staticmethod
    def prepare_path(path):
        return f"/{path}" if not path.startswith("/") else path

    def __init__(self, name, path=None, namespace=None, file_format=None):
        self.name = name
        self.path = self.prepare_path(path) if path else ""
        self.namespace = self.prepare_namespace(namespace) if namespace else ""
        self.file_format = file_format

    def __repr__(self):
        return f"@{self.namespace}{self.name}{self.path} ({self.file_format})"

    @classmethod
    def from_parent_stage(cls, parent_stage, path, file_format=None):
        """
        Extend an existing parent stage (with or without path) with an
        additional sub-path
        """
        return cls(
            parent_stage.name,
            f"{parent_stage.path}/{path}",
            parent_stage.namespace,
            file_format,
        )


class CreateFileFormat(DDLElement):
    """
    Encapsulates a CREATE FILE FORMAT statement; using a format description (as in
    a COPY INTO statement) and a format name.
    """

    __visit_name__ = "create_file_format"

    def __init__(self, format_name, formatter, replace_if_exists=False):
        super().__init__()
        self.format_name = format_name
        self.formatter = formatter
        self.replace_if_exists = replace_if_exists


class CreateStage(DDLElement):
    """
    Encapsulates a CREATE STAGE statement, using a container (physical base for the
    stage) and the actual ExternalStage object.
    """

    __visit_name__ = "create_stage"

    def __init__(self, container, stage, replace_if_exists=False, *, temporary=False):
        super().__init__()
        self.container = container
        self.temporary = temporary
        self.stage = stage
        self.replace_if_exists = replace_if_exists


class AWSBucket(ClauseElement):
    """AWS S3 bucket descriptor"""

    __visit_name__ = "aws_bucket"

    def __init__(self, bucket, path=None):
        self.bucket = bucket
        self.path = path
        self.encryption_used = {}
        self.credentials_used = {}

    @classmethod
    def from_uri(cls, uri):
        if uri[0:5] != "s3://":
            raise ValueError(f"Invalid AWS bucket URI: {uri}")
        b = uri[5:].split("/", 1)
        if len(b) == 1:
            bucket, path = b[0], None
        else:
            bucket, path = b
        return cls(bucket, path)

    def __repr__(self):
        credentials = "CREDENTIALS=({})".format(
            " ".join(f"{n}='{v}'" for n, v in self.credentials_used.items())
        )
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                f"{n}='{v}'" if isinstance(v, string_types) else f"{n}={v}"
                for n, v in self.encryption_used.items()
            )
        )
        uri = "'s3://{}{}'".format(self.bucket, f"/{self.path}" if self.path else "")
        return "{}{}{}".format(
            uri,
            f" {credentials}" if self.credentials_used else "",
            f" {encryption}" if self.encryption_used else "",
        )

    def credentials(
        self, aws_role=None, aws_key_id=None, aws_secret_key=None, aws_token=None
    ):
        if aws_role is None and (aws_key_id is None and aws_secret_key is None):
            raise ValueError(
                "Either 'aws_role', or aws_key_id and aws_secret_key has to be supplied"
            )
        if aws_role:
            self.credentials_used = {"AWS_ROLE": aws_role}
        else:
            self.credentials_used = {
                "AWS_SECRET_KEY": aws_secret_key,
                "AWS_KEY_ID": aws_key_id,
            }
            if aws_token:
                self.credentials_used["AWS_TOKEN"] = aws_token
        return self

    def encryption_aws_cse(self, master_key):
        self.encryption_used = {"TYPE": "AWS_CSE", "MASTER_KEY": master_key}
        return self

    def encryption_aws_sse_s3(self):
        self.encryption_used = {"TYPE": "AWS_SSE_S3"}
        return self

    def encryption_aws_sse_kms(self, kms_key_id=None):
        self.encryption_used = {"TYPE": "AWS_SSE_KMS"}
        if kms_key_id:
            self.encryption_used["KMS_KEY_ID"] = kms_key_id
        return self


class AzureContainer(ClauseElement):
    """Microsoft Azure Container descriptor"""

    __visit_name__ = "azure_container"

    def __init__(self, account, container, path=None):
        self.account = account
        self.container = container
        self.path = path
        self.encryption_used = {}
        self.credentials_used = {}

    @classmethod
    def from_uri(cls, uri):
        if uri[0:8] != "azure://":
            raise ValueError(f"Invalid Azure Container URI: {uri}")
        account, uri = uri[8:].split(".", 1)
        if uri[0:22] != "blob.core.windows.net/":
            raise ValueError(f"Invalid Azure Container URI: {uri}")
        b = uri[22:].split("/", 1)
        if len(b) == 1:
            container, path = b[0], None
        else:
            container, path = b
        return cls(account, container, path)

    def __repr__(self):
        credentials = "CREDENTIALS=({})".format(
            " ".join(f"{n}='{v}'" for n, v in self.credentials_used.items())
        )
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                f"{n}='{v}'" if isinstance(v, string_types) else f"{n}={v}"
                for n, v in self.encryption_used.items()
            )
        )
        uri = "'azure://{}.blob.core.windows.net/{}{}'".format(
            self.account, self.container, f"/{self.path}" if self.path else ""
        )
        return "{}{}{}".format(
            uri,
            f" {credentials}" if self.credentials_used else "",
            f" {encryption}" if self.encryption_used else "",
        )

    def credentials(self, azure_sas_token):
        self.credentials_used = {"AZURE_SAS_TOKEN": azure_sas_token}
        return self

    def encryption_azure_cse(self, master_key):
        self.encryption_used = {"TYPE": "AZURE_CSE", "MASTER_KEY": master_key}
        return self


CopyIntoStorage = CopyInto
