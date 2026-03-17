// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/jbig2/JBig2_SddProc.h"

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "core/fxcodec/jbig2/JBig2_ArithIntDecoder.h"
#include "core/fxcodec/jbig2/JBig2_GrdProc.h"
#include "core/fxcodec/jbig2/JBig2_GrrdProc.h"
#include "core/fxcodec/jbig2/JBig2_HuffmanDecoder.h"
#include "core/fxcodec/jbig2/JBig2_HuffmanTable.h"
#include "core/fxcodec/jbig2/JBig2_SymbolDict.h"
#include "core/fxcodec/jbig2/JBig2_TrdProc.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/stl_util.h"

CJBig2_SDDProc::CJBig2_SDDProc() = default;

CJBig2_SDDProc::~CJBig2_SDDProc() = default;

std::unique_ptr<CJBig2_SymbolDict> CJBig2_SDDProc::DecodeArith(
    CJBig2_ArithDecoder* pArithDecoder,
    pdfium::span<JBig2ArithCtx> gbContexts,
    pdfium::span<JBig2ArithCtx> grContexts) {
  auto IADH = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IADW = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IAAI = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IARDX = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IARDY = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IAEX = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IADT = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IAFS = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IADS = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IAIT = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IARI = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IARDW = std::make_unique<CJBig2_ArithIntDecoder>();
  auto IARDH = std::make_unique<CJBig2_ArithIntDecoder>();

  uint32_t SBSYMCODELENA = 0;
  while ((uint32_t)(1 << SBSYMCODELENA) < (SDNUMINSYMS + SDNUMNEWSYMS)) {
    SBSYMCODELENA++;
  }
  auto IAID = std::make_unique<CJBig2_ArithIaidDecoder>((uint8_t)SBSYMCODELENA);

  std::vector<std::unique_ptr<CJBig2_Image>> SDNEWSYMS(SDNUMNEWSYMS);
  uint32_t HCHEIGHT = 0;
  uint32_t NSYMSDECODED = 0;
  while (NSYMSDECODED < SDNUMNEWSYMS) {
    std::unique_ptr<CJBig2_Image> BS;
    int32_t HCDH;
    IADH->Decode(pArithDecoder, &HCDH);
    HCHEIGHT = HCHEIGHT + HCDH;
    if ((int)HCHEIGHT < 0 || (int)HCHEIGHT > kJBig2MaxImageSize)
      return nullptr;

    uint32_t SYMWIDTH = 0;
    for (;;) {
      int32_t DW;
      if (!IADW->Decode(pArithDecoder, &DW))
        break;

      if (NSYMSDECODED >= SDNUMNEWSYMS)
        return nullptr;

      SYMWIDTH = SYMWIDTH + DW;
      if ((int)SYMWIDTH < 0 || (int)SYMWIDTH > kJBig2MaxImageSize)
        return nullptr;

      if (HCHEIGHT == 0 || SYMWIDTH == 0) {
        ++NSYMSDECODED;
        continue;
      }
      if (SDREFAGG == 0) {
        auto pGRD = std::make_unique<CJBig2_GRDProc>();
        pGRD->MMR = false;
        pGRD->GBW = SYMWIDTH;
        pGRD->GBH = HCHEIGHT;
        pGRD->GBTEMPLATE = SDTEMPLATE;
        pGRD->TPGDON = false;
        pGRD->USESKIP = false;
        pGRD->GBAT[0] = SDAT[0];
        pGRD->GBAT[1] = SDAT[1];
        pGRD->GBAT[2] = SDAT[2];
        pGRD->GBAT[3] = SDAT[3];
        pGRD->GBAT[4] = SDAT[4];
        pGRD->GBAT[5] = SDAT[5];
        pGRD->GBAT[6] = SDAT[6];
        pGRD->GBAT[7] = SDAT[7];
        BS = pGRD->DecodeArith(pArithDecoder, gbContexts);
        if (!BS)
          return nullptr;
      } else {
        uint32_t REFAGGNINST;
        IAAI->Decode(pArithDecoder, (int*)&REFAGGNINST);
        if (REFAGGNINST > 1) {
          // Huffman tables must not outlive |pDecoder|.
          auto SBHUFFFS = std::make_unique<CJBig2_HuffmanTable>(6);
          auto SBHUFFDS = std::make_unique<CJBig2_HuffmanTable>(8);
          auto SBHUFFDT = std::make_unique<CJBig2_HuffmanTable>(11);
          auto SBHUFFRDW = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRDH = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRDX = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRDY = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRSIZE = std::make_unique<CJBig2_HuffmanTable>(1);
          auto pDecoder = std::make_unique<CJBig2_TRDProc>();
          pDecoder->SBHUFF = SDHUFF;
          pDecoder->SBREFINE = true;
          pDecoder->SBW = SYMWIDTH;
          pDecoder->SBH = HCHEIGHT;
          pDecoder->SBNUMINSTANCES = REFAGGNINST;
          pDecoder->SBSTRIPS = 1;
          pDecoder->SBNUMSYMS = SDNUMINSYMS + NSYMSDECODED;
          uint32_t nTmp = 0;
          while ((uint32_t)(1 << nTmp) < pDecoder->SBNUMSYMS) {
            nTmp++;
          }
          uint8_t SBSYMCODELEN = (uint8_t)nTmp;
          pDecoder->SBSYMCODELEN = SBSYMCODELEN;
          std::vector<UnownedPtr<CJBig2_Image>> SBSYMS(pDecoder->SBNUMSYMS);
          fxcrt::Copy(pdfium::make_span(SDINSYMS).first(SDNUMINSYMS),
                      pdfium::make_span(SBSYMS));
          for (size_t i = 0; i < NSYMSDECODED; ++i) {
            SBSYMS[i + SDNUMINSYMS] = SDNEWSYMS[i].get();
          }
          pDecoder->SBSYMS = std::move(SBSYMS);
          pDecoder->SBDEFPIXEL = false;
          pDecoder->SBCOMBOP = JBIG2_COMPOSE_OR;
          pDecoder->TRANSPOSED = false;
          pDecoder->REFCORNER = JBIG2_CORNER_TOPLEFT;
          pDecoder->SBDSOFFSET = 0;
          pDecoder->SBHUFFFS = SBHUFFFS.get();
          pDecoder->SBHUFFDS = SBHUFFDS.get();
          pDecoder->SBHUFFDT = SBHUFFDT.get();
          pDecoder->SBHUFFRDW = SBHUFFRDW.get();
          pDecoder->SBHUFFRDH = SBHUFFRDH.get();
          pDecoder->SBHUFFRDX = SBHUFFRDX.get();
          pDecoder->SBHUFFRDY = SBHUFFRDY.get();
          pDecoder->SBHUFFRSIZE = SBHUFFRSIZE.get();
          pDecoder->SBRTEMPLATE = SDRTEMPLATE;
          pDecoder->SBRAT[0] = SDRAT[0];
          pDecoder->SBRAT[1] = SDRAT[1];
          pDecoder->SBRAT[2] = SDRAT[2];
          pDecoder->SBRAT[3] = SDRAT[3];
          JBig2IntDecoderState ids;
          ids.IADT = IADT.get();
          ids.IAFS = IAFS.get();
          ids.IADS = IADS.get();
          ids.IAIT = IAIT.get();
          ids.IARI = IARI.get();
          ids.IARDW = IARDW.get();
          ids.IARDH = IARDH.get();
          ids.IARDX = IARDX.get();
          ids.IARDY = IARDY.get();
          ids.IAID = IAID.get();
          BS = pDecoder->DecodeArith(pArithDecoder, grContexts, &ids);
          if (!BS)
            return nullptr;
        } else if (REFAGGNINST == 1) {
          uint32_t SBNUMSYMS = SDNUMINSYMS + NSYMSDECODED;
          uint32_t IDI;
          IAID->Decode(pArithDecoder, &IDI);
          if (IDI >= SBNUMSYMS)
            return nullptr;

          CJBig2_Image* sbsyms_idi = GetImage(IDI, SDNEWSYMS);
          if (!sbsyms_idi)
            return nullptr;

          int32_t RDXI;
          int32_t RDYI;
          IARDX->Decode(pArithDecoder, &RDXI);
          IARDY->Decode(pArithDecoder, &RDYI);

          auto pGRRD = std::make_unique<CJBig2_GRRDProc>();
          pGRRD->GRW = SYMWIDTH;
          pGRRD->GRH = HCHEIGHT;
          pGRRD->GRTEMPLATE = SDRTEMPLATE;
          pGRRD->GRREFERENCE = sbsyms_idi;
          pGRRD->GRREFERENCEDX = RDXI;
          pGRRD->GRREFERENCEDY = RDYI;
          pGRRD->TPGRON = false;
          pGRRD->GRAT[0] = SDRAT[0];
          pGRRD->GRAT[1] = SDRAT[1];
          pGRRD->GRAT[2] = SDRAT[2];
          pGRRD->GRAT[3] = SDRAT[3];
          BS = pGRRD->Decode(pArithDecoder, grContexts);
          if (!BS)
            return nullptr;
        }
      }
      SDNEWSYMS[NSYMSDECODED] = std::move(BS);
      ++NSYMSDECODED;
    }
  }

  std::vector<bool> EXFLAGS;
  EXFLAGS.resize(SDNUMINSYMS + SDNUMNEWSYMS);
  bool CUREXFLAG = false;
  uint32_t EXINDEX = 0;
  uint32_t num_ex_syms = 0;
  while (EXINDEX < SDNUMINSYMS + SDNUMNEWSYMS) {
    uint32_t EXRUNLENGTH;
    IAEX->Decode(pArithDecoder, (int*)&EXRUNLENGTH);
    FX_SAFE_UINT32 new_ex_size = EXINDEX;
    new_ex_size += EXRUNLENGTH;
    if (!new_ex_size.IsValid() ||
        new_ex_size.ValueOrDie() > SDNUMINSYMS + SDNUMNEWSYMS) {
      return nullptr;
    }

    if (CUREXFLAG)
      num_ex_syms += EXRUNLENGTH;
    std::fill_n(EXFLAGS.begin() + EXINDEX, EXRUNLENGTH, CUREXFLAG);
    EXINDEX = new_ex_size.ValueOrDie();
    CUREXFLAG = !CUREXFLAG;
  }
  if (num_ex_syms > SDNUMEXSYMS)
    return nullptr;

  std::unique_ptr<CJBig2_SymbolDict> pDict =
      std::make_unique<CJBig2_SymbolDict>();
  for (uint32_t i = 0, j = 0; i < SDNUMINSYMS + SDNUMNEWSYMS; ++i) {
    if (!EXFLAGS[i] || j >= SDNUMEXSYMS)
      continue;
    if (i < SDNUMINSYMS) {
      pDict->AddImage(
          UNSAFE_TODO(SDINSYMS[i] ? std::make_unique<CJBig2_Image>(*SDINSYMS[i])
                                  : nullptr));
    } else {
      pDict->AddImage(std::move(SDNEWSYMS[i - SDNUMINSYMS]));
    }
    ++j;
  }
  return pDict;
}

std::unique_ptr<CJBig2_SymbolDict> CJBig2_SDDProc::DecodeHuffman(
    CJBig2_BitStream* pStream,
    pdfium::span<JBig2ArithCtx> gbContexts,
    pdfium::span<JBig2ArithCtx> grContexts) {
  auto pHuffmanDecoder = std::make_unique<CJBig2_HuffmanDecoder>(pStream);
  std::vector<std::unique_ptr<CJBig2_Image>> SDNEWSYMS(SDNUMNEWSYMS);
  std::vector<uint32_t> SDNEWSYMWIDTHS;
  if (SDREFAGG == 0)
    SDNEWSYMWIDTHS.resize(SDNUMNEWSYMS);
  uint32_t HCHEIGHT = 0;
  uint32_t NSYMSDECODED = 0;
  std::unique_ptr<CJBig2_Image> BS;
  while (NSYMSDECODED < SDNUMNEWSYMS) {
    int32_t HCDH;
    if (pHuffmanDecoder->DecodeAValue(SDHUFFDH, &HCDH) != 0)
      return nullptr;

    HCHEIGHT = HCHEIGHT + HCDH;
    if ((int)HCHEIGHT < 0 || (int)HCHEIGHT > kJBig2MaxImageSize)
      return nullptr;

    uint32_t SYMWIDTH = 0;
    uint32_t TOTWIDTH = 0;
    uint32_t HCFIRSTSYM = NSYMSDECODED;
    for (;;) {
      int32_t DW;
      int32_t nVal = pHuffmanDecoder->DecodeAValue(SDHUFFDW, &DW);
      if (nVal == kJBig2OOB)
        break;
      if (nVal != 0)
        return nullptr;
      if (NSYMSDECODED >= SDNUMNEWSYMS)
        return nullptr;

      SYMWIDTH = SYMWIDTH + DW;
      if ((int)SYMWIDTH < 0 || (int)SYMWIDTH > kJBig2MaxImageSize)
        return nullptr;

      TOTWIDTH += SYMWIDTH;
      if (HCHEIGHT == 0 || SYMWIDTH == 0) {
        ++NSYMSDECODED;
        continue;
      }
      if (SDREFAGG == 1) {
        uint32_t REFAGGNINST;
        if (pHuffmanDecoder->DecodeAValue(SDHUFFAGGINST, (int*)&REFAGGNINST) !=
            0) {
          return nullptr;
        }
        BS = nullptr;
        if (REFAGGNINST > 1) {
          // Huffman tables must outlive |pDecoder|.
          auto SBHUFFFS = std::make_unique<CJBig2_HuffmanTable>(6);
          auto SBHUFFDS = std::make_unique<CJBig2_HuffmanTable>(8);
          auto SBHUFFDT = std::make_unique<CJBig2_HuffmanTable>(11);
          auto SBHUFFRDW = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRDH = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRDX = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRDY = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRSIZE = std::make_unique<CJBig2_HuffmanTable>(1);
          auto pDecoder = std::make_unique<CJBig2_TRDProc>();
          pDecoder->SBHUFF = SDHUFF;
          pDecoder->SBREFINE = true;
          pDecoder->SBW = SYMWIDTH;
          pDecoder->SBH = HCHEIGHT;
          pDecoder->SBNUMINSTANCES = REFAGGNINST;
          pDecoder->SBSTRIPS = 1;
          pDecoder->SBNUMSYMS = SDNUMINSYMS + NSYMSDECODED;
          std::vector<JBig2HuffmanCode> SBSYMCODES(pDecoder->SBNUMSYMS);
          uint32_t nTmp = 1;
          while (static_cast<uint32_t>(1 << nTmp) < pDecoder->SBNUMSYMS)
            ++nTmp;
          for (uint32_t i = 0; i < pDecoder->SBNUMSYMS; ++i) {
            SBSYMCODES[i].codelen = nTmp;
            SBSYMCODES[i].code = i;
          }
          pDecoder->SBSYMCODES = std::move(SBSYMCODES);
          std::vector<UnownedPtr<CJBig2_Image>> SBSYMS(pDecoder->SBNUMSYMS);
          fxcrt::Copy(pdfium::make_span(SDINSYMS).first(SDNUMINSYMS),
                      pdfium::make_span(SBSYMS));
          for (size_t i = 0; i < NSYMSDECODED; ++i) {
            SBSYMS[i + SDNUMINSYMS] = SDNEWSYMS[i].get();
          }
          pDecoder->SBSYMS = std::move(SBSYMS);
          pDecoder->SBDEFPIXEL = false;
          pDecoder->SBCOMBOP = JBIG2_COMPOSE_OR;
          pDecoder->TRANSPOSED = false;
          pDecoder->REFCORNER = JBIG2_CORNER_TOPLEFT;
          pDecoder->SBDSOFFSET = 0;
          pDecoder->SBHUFFFS = SBHUFFFS.get();
          pDecoder->SBHUFFDS = SBHUFFDS.get();
          pDecoder->SBHUFFDT = SBHUFFDT.get();
          pDecoder->SBHUFFRDW = SBHUFFRDW.get();
          pDecoder->SBHUFFRDH = SBHUFFRDH.get();
          pDecoder->SBHUFFRDX = SBHUFFRDX.get();
          pDecoder->SBHUFFRDY = SBHUFFRDY.get();
          pDecoder->SBHUFFRSIZE = SBHUFFRSIZE.get();
          pDecoder->SBRTEMPLATE = SDRTEMPLATE;
          pDecoder->SBRAT[0] = SDRAT[0];
          pDecoder->SBRAT[1] = SDRAT[1];
          pDecoder->SBRAT[2] = SDRAT[2];
          pDecoder->SBRAT[3] = SDRAT[3];
          BS = pDecoder->DecodeHuffman(pStream, grContexts);
          if (!BS)
            return nullptr;

        } else if (REFAGGNINST == 1) {
          uint32_t SBNUMSYMS = SDNUMINSYMS + SDNUMNEWSYMS;
          uint32_t nTmp = 1;
          while ((uint32_t)(1 << nTmp) < SBNUMSYMS) {
            nTmp++;
          }
          uint8_t SBSYMCODELEN = (uint8_t)nTmp;
          uint32_t uVal = 0;
          uint32_t IDI;
          for (;;) {
            if (pStream->read1Bit(&nTmp) != 0)
              return nullptr;

            uVal = (uVal << 1) | nTmp;
            if (uVal >= SBNUMSYMS)
              return nullptr;

            IDI = SBSYMCODELEN == 0 ? uVal : SBNUMSYMS;
            if (IDI < SBNUMSYMS)
              break;
          }

          CJBig2_Image* sbsyms_idi = GetImage(IDI, SDNEWSYMS);
          if (!sbsyms_idi)
            return nullptr;

          auto SBHUFFRDX = std::make_unique<CJBig2_HuffmanTable>(15);
          auto SBHUFFRSIZE = std::make_unique<CJBig2_HuffmanTable>(1);
          int32_t RDXI;
          int32_t RDYI;
          if ((pHuffmanDecoder->DecodeAValue(SBHUFFRDX.get(), &RDXI) != 0) ||
              (pHuffmanDecoder->DecodeAValue(SBHUFFRDX.get(), &RDYI) != 0) ||
              (pHuffmanDecoder->DecodeAValue(SBHUFFRSIZE.get(), &nVal) != 0)) {
            return nullptr;
          }

          pStream->alignByte();
          nTmp = pStream->getOffset();

          auto pGRRD = std::make_unique<CJBig2_GRRDProc>();
          pGRRD->GRW = SYMWIDTH;
          pGRRD->GRH = HCHEIGHT;
          pGRRD->GRTEMPLATE = SDRTEMPLATE;
          pGRRD->GRREFERENCE = sbsyms_idi;
          pGRRD->GRREFERENCEDX = RDXI;
          pGRRD->GRREFERENCEDY = RDYI;
          pGRRD->TPGRON = false;
          pGRRD->GRAT[0] = SDRAT[0];
          pGRRD->GRAT[1] = SDRAT[1];
          pGRRD->GRAT[2] = SDRAT[2];
          pGRRD->GRAT[3] = SDRAT[3];
          auto pArithDecoder = std::make_unique<CJBig2_ArithDecoder>(pStream);
          BS = pGRRD->Decode(pArithDecoder.get(), grContexts);
          if (!BS)
            return nullptr;

          pStream->alignByte();
          pStream->addOffset(2);
          if ((uint32_t)nVal != (pStream->getOffset() - nTmp))
            return nullptr;
        }
        SDNEWSYMS[NSYMSDECODED] = std::move(BS);
      }
      if (SDREFAGG == 0)
        SDNEWSYMWIDTHS[NSYMSDECODED] = SYMWIDTH;
      ++NSYMSDECODED;
    }
    if (SDREFAGG == 0) {
      uint32_t BMSIZE;
      if (pHuffmanDecoder->DecodeAValue(SDHUFFBMSIZE, (int32_t*)&BMSIZE) != 0) {
        return nullptr;
      }
      pStream->alignByte();
      std::unique_ptr<CJBig2_Image> BHC;
      if (BMSIZE == 0) {
        if (static_cast<int>(TOTWIDTH) > kJBig2MaxImageSize)
          return nullptr;

        // OK to not use FX_SAFE_UINT32 to calculate `stride` because
        // `kJBig2MaxImageSize` is limiting the size.
        const uint32_t stride = (TOTWIDTH + 7) / 8;
        FX_SAFE_UINT32 safe_image_size = stride;
        safe_image_size *= HCHEIGHT;
        if (!safe_image_size.IsValid() ||
            pStream->getByteLeft() < safe_image_size.ValueOrDie()) {
          return nullptr;
        }

        BHC = std::make_unique<CJBig2_Image>(TOTWIDTH, HCHEIGHT);
        for (uint32_t i = 0; i < HCHEIGHT; ++i) {
          UNSAFE_TODO(FXSYS_memcpy(BHC->data() + i * BHC->stride(),
                                   pStream->getPointer(), stride));
          pStream->addOffset(stride);
        }
      } else {
        auto pGRD = std::make_unique<CJBig2_GRDProc>();
        pGRD->MMR = true;
        pGRD->GBW = TOTWIDTH;
        pGRD->GBH = HCHEIGHT;
        pGRD->StartDecodeMMR(&BHC, pStream);
        pStream->alignByte();
      }
      if (!BHC)
        continue;

      uint32_t nTmp = 0;
      for (uint32_t i = HCFIRSTSYM; i < NSYMSDECODED; ++i) {
        SDNEWSYMS[i] = BHC->SubImage(nTmp, 0, SDNEWSYMWIDTHS[i], HCHEIGHT);
        nTmp += SDNEWSYMWIDTHS[i];
      }
    }
  }

  std::unique_ptr<CJBig2_HuffmanTable> pTable =
      std::make_unique<CJBig2_HuffmanTable>(1);
  std::vector<bool> EXFLAGS;
  EXFLAGS.resize(SDNUMINSYMS + SDNUMNEWSYMS);
  bool CUREXFLAG = false;
  uint32_t EXINDEX = 0;
  uint32_t num_ex_syms = 0;
  while (EXINDEX < SDNUMINSYMS + SDNUMNEWSYMS) {
    uint32_t EXRUNLENGTH;
    if (pHuffmanDecoder->DecodeAValue(pTable.get(), (int*)&EXRUNLENGTH) != 0)
      return nullptr;

    FX_SAFE_UINT32 new_ex_size = EXINDEX;
    new_ex_size += EXRUNLENGTH;
    if (!new_ex_size.IsValid() ||
        new_ex_size.ValueOrDie() > SDNUMINSYMS + SDNUMNEWSYMS) {
      return nullptr;
    }

    if (CUREXFLAG)
      num_ex_syms += EXRUNLENGTH;
    std::fill_n(EXFLAGS.begin() + EXINDEX, EXRUNLENGTH, CUREXFLAG);
    EXINDEX = new_ex_size.ValueOrDie();
    CUREXFLAG = !CUREXFLAG;
  }
  if (num_ex_syms > SDNUMEXSYMS)
    return nullptr;

  auto pDict = std::make_unique<CJBig2_SymbolDict>();
  for (uint32_t i = 0, j = 0; i < SDNUMINSYMS + SDNUMNEWSYMS; ++i) {
    if (!EXFLAGS[i] || j >= SDNUMEXSYMS)
      continue;
    if (i < SDNUMINSYMS) {
      pDict->AddImage(
          UNSAFE_TODO(SDINSYMS[i] ? std::make_unique<CJBig2_Image>(*SDINSYMS[i])
                                  : nullptr));
    } else {
      pDict->AddImage(std::move(SDNEWSYMS[i - SDNUMINSYMS]));
    }
    ++j;
  }
  return pDict;
}

CJBig2_Image* CJBig2_SDDProc::GetImage(
    uint32_t i,
    pdfium::span<const std::unique_ptr<CJBig2_Image>> new_syms) const {
  return i < SDNUMINSYMS ? SDINSYMS[i].get() : new_syms[i - SDNUMINSYMS].get();
}
