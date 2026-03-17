// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_iccprofile.h"

#include <utility>

#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fxcodec/icc/icc_transform.h"

namespace {

bool DetectSRGB(pdfium::span<const uint8_t> span) {
  static const char kSRGB[] = "sRGB IEC61966-2.1";
  return span.size() == 3144 && memcmp(&span[400], kSRGB, strlen(kSRGB)) == 0;
}

}  // namespace

CPDF_IccProfile::CPDF_IccProfile(RetainPtr<const CPDF_StreamAcc> stream_acc,
                                 uint32_t expected_components)
    : m_pStreamAcc(std::move(stream_acc)),
      m_bsRGB(expected_components == 3 && DetectSRGB(m_pStreamAcc->GetSpan())) {
  if (m_bsRGB) {
    m_nSrcComponents = 3;
    return;
  }

  auto transform =
      fxcodec::IccTransform::CreateTransformSRGB(m_pStreamAcc->GetSpan());
  if (!transform) {
    return;
  }

  uint32_t components = transform->components();
  if (components != expected_components) {
    return;
  }

  m_nSrcComponents = components;
  m_Transform = std::move(transform);
}

CPDF_IccProfile::~CPDF_IccProfile() = default;

bool CPDF_IccProfile::IsNormal() const {
  return m_Transform->IsNormal();
}

void CPDF_IccProfile::Translate(pdfium::span<const float> pSrcValues,
                                pdfium::span<float> pDestValues) {
  m_Transform->Translate(pSrcValues, pDestValues);
}

void CPDF_IccProfile::TranslateScanline(pdfium::span<uint8_t> pDest,
                                        pdfium::span<const uint8_t> pSrc,
                                        int pixels) {
  m_Transform->TranslateScanline(pDest, pSrc, pixels);
}
