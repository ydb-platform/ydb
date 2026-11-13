# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


class MappingUnderflowError(Exception):
    """
    Raised when computing the lower boundary of an index that maps into a
    denormal floating point value.
    """


class MappingOverflowError(Exception):
    """
    Raised when computing the lower boundary of an index that maps into +inf.
    """
