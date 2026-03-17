// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_transferfunc.h"

#include <stdint.h>

#include <utility>

#include "core/fpdfapi/page/cpdf_transferfuncdib.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxge/dib/cfx_dibbase.h"

CPDF_TransferFunc::CPDF_TransferFunc(bool bIdentify,
                                     FixedSizeDataVector<uint8_t> samples_r,
                                     FixedSizeDataVector<uint8_t> samples_g,
                                     FixedSizeDataVector<uint8_t> samples_b)
    : m_bIdentity(bIdentify),
      m_SamplesR(std::move(samples_r)),
      m_SamplesG(std::move(samples_g)),
      m_SamplesB(std::move(samples_b)) {
  DCHECK_EQ(m_SamplesR.size(), kChannelSampleSize);
  DCHECK_EQ(m_SamplesG.size(), kChannelSampleSize);
  DCHECK_EQ(m_SamplesB.size(), kChannelSampleSize);
}

CPDF_TransferFunc::~CPDF_TransferFunc() = default;

FX_COLORREF CPDF_TransferFunc::TranslateColor(FX_COLORREF colorref) const {
  return FXSYS_BGR(m_SamplesB.span()[FXSYS_GetBValue(colorref)],
                   m_SamplesG.span()[FXSYS_GetGValue(colorref)],
                   m_SamplesR.span()[FXSYS_GetRValue(colorref)]);
}

RetainPtr<CFX_DIBBase> CPDF_TransferFunc::TranslateImage(
    RetainPtr<CFX_DIBBase> pSrc) {
  return pdfium::MakeRetain<CPDF_TransferFuncDIB>(std::move(pSrc),
                                                  pdfium::WrapRetain(this));
}

pdfium::span<const uint8_t> CPDF_TransferFunc::GetSamplesR() const {
  return m_SamplesR;
}

pdfium::span<const uint8_t> CPDF_TransferFunc::GetSamplesG() const {
  return m_SamplesG;
}

pdfium::span<const uint8_t> CPDF_TransferFunc::GetSamplesB() const {
  return m_SamplesB;
}
