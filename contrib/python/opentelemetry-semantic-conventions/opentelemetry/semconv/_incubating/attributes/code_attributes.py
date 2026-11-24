# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

CODE_COLUMN: Final = "code.column"
"""
Deprecated: Replaced by `code.column.number`.
"""

CODE_COLUMN_NUMBER: Final = "code.column.number"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.code_attributes.CODE_COLUMN_NUMBER`.
"""

CODE_FILE_PATH: Final = "code.file.path"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.code_attributes.CODE_FILE_PATH`.
"""

CODE_FILEPATH: Final = "code.filepath"
"""
Deprecated: Replaced by `code.file.path`.
"""

CODE_FUNCTION: Final = "code.function"
"""
Deprecated: Value should be included in `code.function.name` which is expected to be a fully-qualified name.
"""

CODE_FUNCTION_NAME: Final = "code.function.name"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.code_attributes.CODE_FUNCTION_NAME`.
"""

CODE_LINE_NUMBER: Final = "code.line.number"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.code_attributes.CODE_LINE_NUMBER`.
"""

CODE_LINENO: Final = "code.lineno"
"""
Deprecated: Replaced by `code.line.number`.
"""

CODE_NAMESPACE: Final = "code.namespace"
"""
Deprecated: Value should be included in `code.function.name` which is expected to be a fully-qualified name.
"""

CODE_STACKTRACE: Final = "code.stacktrace"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.code_attributes.CODE_STACKTRACE`.
"""
