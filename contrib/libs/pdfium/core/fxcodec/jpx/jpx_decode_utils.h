// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JPX_JPX_DECODE_UTILS_H_
#define CORE_FXCODEC_JPX_JPX_DECODE_UTILS_H_

#include <stdint.h>

#include "core/fxcrt/span.h"

#include <openjpeg.h>

namespace fxcodec {

struct DecodeData {
  DecodeData() = default;
  explicit DecodeData(pdfium::span<const uint8_t> data);

  const uint8_t* src_data = nullptr;
  OPJ_SIZE_T src_size = 0;
  OPJ_SIZE_T offset = 0;
};

/* Wrappers for C-style callbacks. */
OPJ_SIZE_T opj_read_from_memory(void* p_buffer,
                                OPJ_SIZE_T nb_bytes,
                                void* p_user_data);
OPJ_OFF_T opj_skip_from_memory(OPJ_OFF_T nb_bytes, void* p_user_data);
OPJ_BOOL opj_seek_from_memory(OPJ_OFF_T nb_bytes, void* p_user_data);

}  // namespace fxcodec

#endif  // CORE_FXCODEC_JPX_JPX_DECODE_UTILS_H_
