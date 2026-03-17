# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

"""Exceptions used in gcovr."""


class GcovrDataAssertionError(AssertionError):
    """Exception for data merge errors."""


class GcovrMergeAssertionError(AssertionError):
    """Exception for data merge errors."""


class SanityCheckError(AssertionError):
    """Raised when a sanity check fails."""
