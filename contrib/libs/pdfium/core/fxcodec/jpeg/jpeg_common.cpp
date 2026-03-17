// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/jpeg/jpeg_common.h"

extern "C" {

void src_do_nothing(jpeg_decompress_struct* cinfo) {}

boolean src_fill_buffer(j_decompress_ptr cinfo) {
  return FALSE;
}

boolean src_resync(j_decompress_ptr cinfo, int desired) {
  return FALSE;
}

void error_do_nothing(j_common_ptr cinfo) {}

void error_do_nothing_int(j_common_ptr cinfo, int) {}

void error_do_nothing_char(j_common_ptr cinfo, char*) {}

}  // extern "C"
