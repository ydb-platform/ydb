# -*- coding: UTF-8 -*-
# Copyright 2013 - 2023, jenisys
# SPDX-License-Identifier: MIT
"""
This module extends the :mod:`parse` to build and derive additional
parse-types from other, existing types.
"""

from __future__ import absolute_import
from parse_type.cardinality import Cardinality
from parse_type.builder import TypeBuilder, build_type_dict

__all__ = ["Cardinality", "TypeBuilder", "build_type_dict"]
