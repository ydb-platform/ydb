// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/fx_codec.h"

#include <utility>

#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span_util.h"
#include "core/fxge/dib/fx_dib.h"

namespace fxcodec {

#ifdef PDF_ENABLE_XFA
CFX_DIBAttribute::CFX_DIBAttribute() = default;

CFX_DIBAttribute::~CFX_DIBAttribute() = default;
#endif  // PDF_ENABLE_XFA

void ReverseRGB(pdfium::span<uint8_t> pDestBuf,
                pdfium::span<const uint8_t> pSrcBuf,
                int pixels) {
  const size_t count = pdfium::checked_cast<size_t>(pixels);
  auto dst_span =
      fxcrt::reinterpret_span<FX_RGB_STRUCT<uint8_t>>(pDestBuf).first(count);

  const auto src_span =
      fxcrt::reinterpret_span<const FX_RGB_STRUCT<uint8_t>>(pSrcBuf).first(
          count);

  if (dst_span.data() == src_span.data()) {
    for (auto& pix : dst_span) {
      std::swap(pix.red, pix.blue);
    }
    return;
  }

  for (const auto& src_pix : src_span) {
    auto& dst_pix = dst_span.front();
    dst_pix.red = src_pix.blue;
    dst_pix.green = src_pix.green;
    dst_pix.blue = src_pix.red;
    dst_span = dst_span.subspan(1);
  }
}

}  // namespace fxcodec
