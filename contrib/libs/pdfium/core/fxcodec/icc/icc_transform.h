// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_ICC_ICC_TRANSFORM_H_
#define CORE_FXCODEC_ICC_ICC_TRANSFORM_H_

#include <stdint.h>

#include <memory>

#include "core/fxcodec/fx_codec_def.h"
#include "core/fxcrt/span.h"

#include <lcms2.h>

namespace fxcodec {

class IccTransform {
 public:
  static std::unique_ptr<IccTransform> CreateTransformSRGB(
      pdfium::span<const uint8_t> span);

  ~IccTransform();

  void Translate(pdfium::span<const float> pSrcValues,
                 pdfium::span<float> pDestValues);
  void TranslateScanline(pdfium::span<uint8_t> pDest,
                         pdfium::span<const uint8_t> pSrc,
                         int pixels);

  int components() const { return m_nSrcComponents; }
  bool IsNormal() const { return m_bNormal; }

  static bool IsValidIccComponents(int components);

 private:
  IccTransform(cmsHTRANSFORM transform,
               int srcComponents,
               bool bIsLab,
               bool bNormal);

  const cmsHTRANSFORM m_hTransform;
  const int m_nSrcComponents;
  const bool m_bLab;
  const bool m_bNormal;
};

}  // namespace fxcodec

#endif  // CORE_FXCODEC_ICC_ICC_TRANSFORM_H_
