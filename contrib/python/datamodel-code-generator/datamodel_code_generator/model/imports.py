from __future__ import annotations

from datamodel_code_generator.imports import Import

IMPORT_DATACLASS = Import.from_full_path("dataclasses.dataclass")
IMPORT_FIELD = Import.from_full_path("dataclasses.field")
IMPORT_CLASSVAR = Import.from_full_path("typing.ClassVar")
IMPORT_TYPED_DICT = Import.from_full_path("typing.TypedDict")
IMPORT_TYPED_DICT_BACKPORT = Import.from_full_path("typing_extensions.TypedDict")
IMPORT_NOT_REQUIRED = Import.from_full_path("typing.NotRequired")
IMPORT_NOT_REQUIRED_BACKPORT = Import.from_full_path("typing_extensions.NotRequired")
IMPORT_MSGSPEC_STRUCT = Import.from_full_path("msgspec.Struct")
IMPORT_MSGSPEC_FIELD = Import.from_full_path("msgspec.field")
IMPORT_MSGSPEC_META = Import.from_full_path("msgspec.Meta")
IMPORT_MSGSPEC_CONVERT = Import.from_full_path("msgspec.convert")
