from __future__ import annotations

from datamodel_code_generator.imports import Import

IMPORT_CONFIG_DICT = Import.from_full_path("pydantic.ConfigDict")
IMPORT_AWARE_DATETIME = Import.from_full_path("pydantic.AwareDatetime")
IMPORT_NAIVE_DATETIME = Import.from_full_path("pydantic.NaiveDatetime")
