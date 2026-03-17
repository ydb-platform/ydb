#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from .converter import SnowflakeConverter


class SnowflakeNoConverterToPython(SnowflakeConverter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_python_method(self, type_name, column):
        return None
