# distutils: language = c++
# cython: embedsignature = True, language_level = 3, warn.undeclared = True, warn.unreachable = True, warn.maybe_uninitialized = True

# SPDX-License-Identifier: MIT OR Apache-2.0
# SPDX-FileCopyrightText: 2018-2025 René Kijewski <pypi.org@k6i.de>

include 'src/_imports.pyx'
include 'src/_constants.pyx'

include 'src/_exceptions.pyx'
include 'src/_exceptions_decoder.pyx'
include 'src/_exceptions_encoder.pyx'
include 'src/_raise_decoder.pyx'
include 'src/_raise_encoder.pyx'

include 'src/_unicode.pyx'

include 'src/_reader_ucs.pyx'
include 'src/_reader_callback.pyx'
include 'src/_readers.pyx'
include 'src/_decoder.pyx'

include 'src/_writers.pyx'
include 'src/_writer_reallocatable.pyx'
include 'src/_writer_callback.pyx'
include 'src/_writer_noop.pyx'
include 'src/_encoder_options.pyx'
include 'src/_encoder.pyx'

include 'src/_exports.pyx'
include 'src/_legacy.pyx'
