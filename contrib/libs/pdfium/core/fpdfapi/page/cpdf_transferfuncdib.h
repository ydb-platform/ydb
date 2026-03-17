// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_TRANSFERFUNCDIB_H_
#define CORE_FPDFAPI_PAGE_CPDF_TRANSFERFUNCDIB_H_

#include <stdint.h>

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/cfx_dibbase.h"

class CPDF_TransferFunc;

class CPDF_TransferFuncDIB final : public CFX_DIBBase {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // CFX_DIBBase:
  pdfium::span<const uint8_t> GetScanline(int line) const override;

 private:
  CPDF_TransferFuncDIB(RetainPtr<const CFX_DIBBase> src,
                       RetainPtr<CPDF_TransferFunc> transfer_func);
  ~CPDF_TransferFuncDIB() override;

  void TranslateScanline(pdfium::span<const uint8_t> src_span) const;
  FXDIB_Format GetDestFormat() const;

  RetainPtr<const CFX_DIBBase> const src_;
  RetainPtr<CPDF_TransferFunc> const transfer_func_;
  const pdfium::raw_span<const uint8_t> r_samples_;
  const pdfium::raw_span<const uint8_t> g_samples_;
  const pdfium::raw_span<const uint8_t> b_samples_;
  mutable DataVector<uint8_t> scanline_;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_TRANSFERFUNCDIB_H_
