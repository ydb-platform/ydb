// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_TRANSFERFUNC_H_
#define CORE_FPDFAPI_PAGE_CPDF_TRANSFERFUNC_H_

#include <stdint.h>

#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxcrt/observed_ptr.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/fx_dib.h"

class CFX_DIBBase;

class CPDF_TransferFunc final : public Retainable, public Observable {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  static constexpr size_t kChannelSampleSize = 256;

  FX_COLORREF TranslateColor(FX_COLORREF colorref) const;
  RetainPtr<CFX_DIBBase> TranslateImage(RetainPtr<CFX_DIBBase> pSrc);

  // Spans are |kChannelSampleSize| in size.
  pdfium::span<const uint8_t> GetSamplesR() const;
  pdfium::span<const uint8_t> GetSamplesG() const;
  pdfium::span<const uint8_t> GetSamplesB() const;

  bool GetIdentity() const { return m_bIdentity; }

 private:
  CPDF_TransferFunc(bool bIdentify,
                    FixedSizeDataVector<uint8_t> samples_r,
                    FixedSizeDataVector<uint8_t> samples_g,
                    FixedSizeDataVector<uint8_t> samples_b);
  ~CPDF_TransferFunc() override;

  const bool m_bIdentity;
  const FixedSizeDataVector<uint8_t> m_SamplesR;
  const FixedSizeDataVector<uint8_t> m_SamplesG;
  const FixedSizeDataVector<uint8_t> m_SamplesB;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_TRANSFERFUNC_H_
