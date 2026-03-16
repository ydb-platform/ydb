// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_FX_CODEC_H_
#define CORE_FXCODEC_FX_CODEC_H_

#include <stdint.h>

#include "core/fxcrt/span.h"

namespace fxcodec {

#ifdef PDF_ENABLE_XFA
class CFX_DIBAttribute {
 public:
  // Not an enum class yet because we still blindly cast integer results
  // from third-party libraries to this type.
  enum ResUnit : uint16_t {
    kResUnitNone = 0,
    kResUnitInch,
    kResUnitCentimeter,
    kResUnitMeter
  };

  CFX_DIBAttribute();
  ~CFX_DIBAttribute();

  int32_t m_nXDPI = -1;
  int32_t m_nYDPI = -1;
  ResUnit m_wDPIUnit = kResUnitNone;
};
#endif  // PDF_ENABLE_XFA

void ReverseRGB(pdfium::span<uint8_t> pDestBuf,
                pdfium::span<const uint8_t> pSrcBuf,
                int pixels);

}  // namespace fxcodec

#ifdef PDF_ENABLE_XFA
using CFX_DIBAttribute = fxcodec::CFX_DIBAttribute;
#endif

#endif  // CORE_FXCODEC_FX_CODEC_H_
