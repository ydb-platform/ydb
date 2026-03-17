// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JBIG2_JBIG2_SDDPROC_H_
#define CORE_FXCODEC_JBIG2_JBIG2_SDDPROC_H_

#include <stdint.h>

#include <array>
#include <memory>
#include <vector>

#include "core/fxcodec/jbig2/JBig2_ArithDecoder.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

class CJBig2_BitStream;
class CJBig2_HuffmanTable;
class CJBig2_Image;
class CJBig2_SymbolDict;

class CJBig2_SDDProc {
 public:
  CJBig2_SDDProc();
  ~CJBig2_SDDProc();

  std::unique_ptr<CJBig2_SymbolDict> DecodeArith(
      CJBig2_ArithDecoder* pArithDecoder,
      pdfium::span<JBig2ArithCtx> gbContexts,
      pdfium::span<JBig2ArithCtx> grContexts);

  std::unique_ptr<CJBig2_SymbolDict> DecodeHuffman(
      CJBig2_BitStream* pStream,
      pdfium::span<JBig2ArithCtx> gbContexts,
      pdfium::span<JBig2ArithCtx> grContexts);

  bool SDHUFF;
  bool SDREFAGG;
  bool SDRTEMPLATE;
  uint8_t SDTEMPLATE;
  uint32_t SDNUMINSYMS;
  uint32_t SDNUMNEWSYMS;
  uint32_t SDNUMEXSYMS;
  std::vector<UnownedPtr<CJBig2_Image>> SDINSYMS;
  UnownedPtr<const CJBig2_HuffmanTable> SDHUFFDH;
  UnownedPtr<const CJBig2_HuffmanTable> SDHUFFDW;
  UnownedPtr<const CJBig2_HuffmanTable> SDHUFFBMSIZE;
  UnownedPtr<const CJBig2_HuffmanTable> SDHUFFAGGINST;
  std::array<int8_t, 8> SDAT;
  std::array<int8_t, 4> SDRAT;

 private:
  // Reads from `SDINSYMS` if `i` is in-bounds. Otherwise, reduce `i` by
  // `SDNUMINSYMS` and read from `new_syms` at the new index.
  CJBig2_Image* GetImage(
      uint32_t i,
      pdfium::span<const std::unique_ptr<CJBig2_Image>> new_syms) const;
};

#endif  // CORE_FXCODEC_JBIG2_JBIG2_SDDPROC_H_
