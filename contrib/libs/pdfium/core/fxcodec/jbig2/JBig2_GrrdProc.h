// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JBIG2_JBIG2_GRRDPROC_H_
#define CORE_FXCODEC_JBIG2_JBIG2_GRRDPROC_H_

#include <stdint.h>

#include <array>
#include <memory>

#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

class CJBig2_ArithDecoder;
class CJBig2_Image;
class JBig2ArithCtx;

class CJBig2_GRRDProc {
 public:
  CJBig2_GRRDProc();
  ~CJBig2_GRRDProc();

  std::unique_ptr<CJBig2_Image> Decode(CJBig2_ArithDecoder* pArithDecoder,
                                       pdfium::span<JBig2ArithCtx> grContexts);

  bool GRTEMPLATE;
  bool TPGRON;
  uint32_t GRW;
  uint32_t GRH;
  int32_t GRREFERENCEDX;
  int32_t GRREFERENCEDY;
  UnownedPtr<CJBig2_Image> GRREFERENCE;
  std::array<int8_t, 4> GRAT;

 private:
  uint32_t DecodeTemplate0UnoptCalculateContext(const CJBig2_Image& GRREG,
                                                const uint32_t* lines,
                                                uint32_t w,
                                                uint32_t h) const;
  void DecodeTemplate0UnoptSetPixel(CJBig2_Image* GRREG,
                                    uint32_t* lines,
                                    uint32_t w,
                                    uint32_t h,
                                    int bVal);

  std::unique_ptr<CJBig2_Image> DecodeTemplate0Unopt(
      CJBig2_ArithDecoder* pArithDecoder,
      pdfium::span<JBig2ArithCtx> grContexts);

  std::unique_ptr<CJBig2_Image> DecodeTemplate0Opt(
      CJBig2_ArithDecoder* pArithDecoder,
      pdfium::span<JBig2ArithCtx> grContexts);

  std::unique_ptr<CJBig2_Image> DecodeTemplate1Unopt(
      CJBig2_ArithDecoder* pArithDecoder,
      pdfium::span<JBig2ArithCtx> grContexts);

  std::unique_ptr<CJBig2_Image> DecodeTemplate1Opt(
      CJBig2_ArithDecoder* pArithDecoder,
      pdfium::span<JBig2ArithCtx> grContexts);
};

#endif  // CORE_FXCODEC_JBIG2_JBIG2_GRRDPROC_H_
