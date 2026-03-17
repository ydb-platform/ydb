// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/jpx/jpx_decode_utils.h"

#include <algorithm>
#include <limits>
#include <type_traits>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/numerics/safe_conversions.h"

namespace fxcodec {

DecodeData::DecodeData(pdfium::span<const uint8_t> data)
    : src_data(data.data()),
      src_size(pdfium::checked_cast<OPJ_SIZE_T>(data.size())) {}

OPJ_SIZE_T opj_read_from_memory(void* p_buffer,
                                OPJ_SIZE_T nb_bytes,
                                void* p_user_data) {
  DecodeData* srcData = static_cast<DecodeData*>(p_user_data);
  if (!srcData || !srcData->src_data || srcData->src_size == 0)
    return static_cast<OPJ_SIZE_T>(-1);

  // Reads at EOF return an error code.
  if (srcData->offset >= srcData->src_size)
    return static_cast<OPJ_SIZE_T>(-1);

  UNSAFE_TODO({
    OPJ_SIZE_T bufferLength = srcData->src_size - srcData->offset;
    OPJ_SIZE_T readlength = nb_bytes < bufferLength ? nb_bytes : bufferLength;
    FXSYS_memcpy(p_buffer, &srcData->src_data[srcData->offset], readlength);
    srcData->offset += readlength;
    return readlength;
  })
}

OPJ_OFF_T opj_skip_from_memory(OPJ_OFF_T nb_bytes, void* p_user_data) {
  DecodeData* srcData = static_cast<DecodeData*>(p_user_data);
  if (!srcData || !srcData->src_data || srcData->src_size == 0)
    return static_cast<OPJ_OFF_T>(-1);

  // Offsets are signed and may indicate a negative skip. Do not support this
  // because of the strange return convention where either bytes skipped or
  // -1 is returned. Following that convention, a successful relative seek of
  // -1 bytes would be required to to give the same result as the error case.
  if (nb_bytes < 0)
    return static_cast<OPJ_OFF_T>(-1);

  auto unsigned_nb_bytes =
      static_cast<std::make_unsigned<OPJ_OFF_T>::type>(nb_bytes);
  // Additionally, the offset may take us beyond the range of a size_t (e.g.
  // 32-bit platforms). If so, just clamp at EOF.
  if (unsigned_nb_bytes >
      std::numeric_limits<OPJ_SIZE_T>::max() - srcData->offset) {
    srcData->offset = srcData->src_size;
  } else {
    OPJ_SIZE_T checked_nb_bytes = static_cast<OPJ_SIZE_T>(unsigned_nb_bytes);
    // Otherwise, mimic fseek() semantics to always succeed, even past EOF,
    // clamping at EOF.  We can get away with this since we don't actually
    // provide negative relative skips from beyond EOF back to inside the
    // data, which would be the only reason to need to know exactly how far
    // beyond EOF we are.
    srcData->offset =
        std::min(srcData->offset + checked_nb_bytes, srcData->src_size);
  }
  return nb_bytes;
}

OPJ_BOOL opj_seek_from_memory(OPJ_OFF_T nb_bytes, void* p_user_data) {
  DecodeData* srcData = static_cast<DecodeData*>(p_user_data);
  if (!srcData || !srcData->src_data || srcData->src_size == 0)
    return OPJ_FALSE;

  // Offsets are signed and may indicate a negative position, which would
  // be before the start of the file. Do not support this.
  if (nb_bytes < 0)
    return OPJ_FALSE;

  auto unsigned_nb_bytes =
      static_cast<std::make_unsigned<OPJ_OFF_T>::type>(nb_bytes);
  // Additionally, the offset may take us beyond the range of a size_t (e.g.
  // 32-bit platforms). If so, just clamp at EOF.
  if (unsigned_nb_bytes > std::numeric_limits<OPJ_SIZE_T>::max()) {
    srcData->offset = srcData->src_size;
  } else {
    OPJ_SIZE_T checked_nb_bytes = static_cast<OPJ_SIZE_T>(nb_bytes);
    // Otherwise, mimic fseek() semantics to always succeed, even past EOF,
    // again clamping at EOF.
    srcData->offset = std::min(checked_nb_bytes, srcData->src_size);
  }
  return OPJ_TRUE;
}

}  // namespace fxcodec
