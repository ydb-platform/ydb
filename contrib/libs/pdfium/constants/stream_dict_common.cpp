// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "constants/stream_dict_common.h"

namespace pdfium::stream {

// PDF 1.7 spec, table 3.4.
// Entries common to all stream dictionaries.
//
// TODO(https://crbug.com/pdfium/1049): Examine all usages of "Length",
// "Filter", and "F".
const char kLength[] = "Length";
const char kFilter[] = "Filter";
const char kDecodeParms[] = "DecodeParms";
const char kF[] = "F";
const char kDL[] = "DL";

}  // namespace pdfium::stream
