// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_dib.h"

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_image.h"
#include "core/fpdfapi/page/cpdf_imageobject.h"
#include "core/fpdfapi/page/cpdf_indexedcs.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fxcodec/basic/basicmodule.h"
#include "core/fxcodec/icc/icc_transform.h"
#include "core/fxcodec/jbig2/jbig2_decoder.h"
#include "core/fxcodec/jpeg/jpegmodule.h"
#include "core/fxcodec/jpx/cjpx_decoder.h"
#include "core/fxcodec/scanlinedecoder.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxcrt/zip.h"
#include "core/fxge/calculate_pitch.h"
#include "core/fxge/dib/cfx_dibitmap.h"

namespace {

bool IsValidDimension(int value) {
  constexpr int kMaxImageDimension = 0x01FFFF;
  return value > 0 && value <= kMaxImageDimension;
}

unsigned int GetBits8(pdfium::span<const uint8_t> pData,
                      uint64_t bitpos,
                      size_t nbits) {
  DCHECK(nbits == 1 || nbits == 2 || nbits == 4 || nbits == 8 || nbits == 16);
  DCHECK_EQ((bitpos & (nbits - 1)), 0u);
  unsigned int byte = pData[bitpos / 8];
  if (nbits == 8) {
    return byte;
  }
  if (nbits == 16) {
    return byte * 256 + pData[bitpos / 8 + 1];
  }
  return (byte >> (8 - nbits - (bitpos % 8))) & ((1 << nbits) - 1);
}

bool GetBitValue(pdfium::span<const uint8_t> pSrc, uint32_t pos) {
  return pSrc[pos / 8] & (1 << (7 - pos % 8));
}

// Just to sanity check and filter out obvious bad values.
bool IsMaybeValidBitsPerComponent(int bpc) {
  return bpc >= 0 && bpc <= 16;
}

bool IsAllowedBitsPerComponent(int bpc) {
  return bpc == 1 || bpc == 2 || bpc == 4 || bpc == 8 || bpc == 16;
}

bool IsColorIndexOutOfBounds(uint8_t index, const DIB_COMP_DATA& comp_datum) {
  return index < comp_datum.m_ColorKeyMin || index > comp_datum.m_ColorKeyMax;
}

bool AreColorIndicesOutOfBounds(const uint8_t* indices,
                                const DIB_COMP_DATA* comp_data,
                                size_t count) {
  UNSAFE_TODO({
    for (size_t i = 0; i < count; ++i) {
      if (IsColorIndexOutOfBounds(indices[i], comp_data[i])) {
        return true;
      }
    }
  });
  return false;
}

CJPX_Decoder::ColorSpaceOption ColorSpaceOptionFromColorSpace(
    CPDF_ColorSpace* pCS) {
  if (!pCS) {
    return CJPX_Decoder::ColorSpaceOption::kNone;
  }
  if (pCS->GetFamily() == CPDF_ColorSpace::Family::kIndexed) {
    return CJPX_Decoder::ColorSpaceOption::kIndexed;
  }
  return CJPX_Decoder::ColorSpaceOption::kNormal;
}

enum class JpxDecodeAction {
  kFail,
  kDoNothing,
  kUseGray,
  kUseRgb,
  kUseCmyk,
  kConvertArgbToRgb,
};

// ISO 32000-1:2008 section 7.4.9 says the PDF and JPX colorspaces should have
// the same number of color channels. This helper function checks the
// colorspaces match, but also tolerates unknowns.
bool IsJPXColorSpaceOrUnspecifiedOrUnknown(COLOR_SPACE actual,
                                           COLOR_SPACE expected) {
  return actual == expected || actual == OPJ_CLRSPC_UNSPECIFIED ||
         actual == OPJ_CLRSPC_UNKNOWN;
}

// Decides which JpxDecodeAction to use based on the colorspace information from
// the PDF and the JPX image. Called only when the PDF's image object contains a
// "/ColorSpace" entry.
JpxDecodeAction GetJpxDecodeActionFromColorSpaces(
    const CJPX_Decoder::JpxImageInfo& jpx_info,
    const CPDF_ColorSpace* pdf_colorspace) {
  if (pdf_colorspace ==
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray)) {
    if (!IsJPXColorSpaceOrUnspecifiedOrUnknown(/*actual=*/jpx_info.colorspace,
                                               /*expected=*/OPJ_CLRSPC_GRAY)) {
      return JpxDecodeAction::kFail;
    }
    return JpxDecodeAction::kUseGray;
  }

  if (pdf_colorspace ==
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceRGB)) {
    if (!IsJPXColorSpaceOrUnspecifiedOrUnknown(/*actual=*/jpx_info.colorspace,
                                               /*expected=*/OPJ_CLRSPC_SRGB)) {
      return JpxDecodeAction::kFail;
    }

    // The channel count of a JPX image can be different from the PDF color
    // space's component count.
    if (jpx_info.channels > 3) {
      return JpxDecodeAction::kConvertArgbToRgb;
    }
    return JpxDecodeAction::kUseRgb;
  }

  if (pdf_colorspace ==
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceCMYK)) {
    if (!IsJPXColorSpaceOrUnspecifiedOrUnknown(/*actual=*/jpx_info.colorspace,
                                               /*expected=*/OPJ_CLRSPC_CMYK)) {
      return JpxDecodeAction::kFail;
    }
    return JpxDecodeAction::kUseCmyk;
  }

  // Many PDFs generated by iOS meets this condition. Handle the discrepancy.
  // See https://crbug.com/345431077 for example.
  if (pdf_colorspace->ComponentCount() == 3 && jpx_info.channels == 4 &&
      jpx_info.colorspace == OPJ_CLRSPC_SRGB) {
    return JpxDecodeAction::kConvertArgbToRgb;
  }

  return JpxDecodeAction::kDoNothing;
}

JpxDecodeAction GetJpxDecodeActionFromImageColorSpace(
    const CJPX_Decoder::JpxImageInfo& jpx_info) {
  switch (jpx_info.colorspace) {
    case OPJ_CLRSPC_SYCC:
    case OPJ_CLRSPC_EYCC:
    case OPJ_CLRSPC_UNKNOWN:
    case OPJ_CLRSPC_UNSPECIFIED:
      return JpxDecodeAction::kDoNothing;

    case OPJ_CLRSPC_SRGB:
      if (jpx_info.channels > 3) {
        return JpxDecodeAction::kConvertArgbToRgb;
      }

      return JpxDecodeAction::kUseRgb;

    case OPJ_CLRSPC_GRAY:
      return JpxDecodeAction::kUseGray;

    case OPJ_CLRSPC_CMYK:
      return JpxDecodeAction::kUseCmyk;
  }
}

JpxDecodeAction GetJpxDecodeAction(const CJPX_Decoder::JpxImageInfo& jpx_info,
                                   const CPDF_ColorSpace* pdf_colorspace) {
  if (pdf_colorspace) {
    return GetJpxDecodeActionFromColorSpaces(jpx_info, pdf_colorspace);
  }

  // When PDF does not provide a color space, check the image color space.
  return GetJpxDecodeActionFromImageColorSpace(jpx_info);
}

int GetComponentCountFromOpjColorSpace(OPJ_COLOR_SPACE colorspace) {
  switch (colorspace) {
    case OPJ_CLRSPC_GRAY:
      return 1;

    case OPJ_CLRSPC_SRGB:
    case OPJ_CLRSPC_SYCC:
    case OPJ_CLRSPC_EYCC:
      return 3;

    case OPJ_CLRSPC_CMYK:
      return 4;

    default:
      return 0;
  }
}

}  // namespace

CPDF_DIB::CPDF_DIB(CPDF_Document* pDoc, RetainPtr<const CPDF_Stream> pStream)
    : m_pDocument(pDoc), m_pStream(std::move(pStream)) {}

CPDF_DIB::~CPDF_DIB() = default;

CPDF_DIB::JpxSMaskInlineData::JpxSMaskInlineData() = default;

CPDF_DIB::JpxSMaskInlineData::~JpxSMaskInlineData() = default;

bool CPDF_DIB::Load() {
  if (!LoadInternal(nullptr, nullptr))
    return false;

  if (CreateDecoder(0) == LoadState::kFail)
    return false;

  return ContinueInternal();
}

bool CPDF_DIB::ContinueToLoadMask() {
  if (m_pColorSpace && m_bStdCS)
    m_pColorSpace->EnableStdConversion(true);

  return ContinueInternal();
}

bool CPDF_DIB::ContinueInternal() {
  if (m_bImageMask) {
    SetMaskProperties();
  } else {
    const uint32_t bpp = m_bpc * m_nComponents;
    if (bpp == 0) {
      return false;
    }

    if (bpp == 1) {
      SetFormat(FXDIB_Format::k1bppRgb);
    } else if (bpp <= 8) {
      SetFormat(FXDIB_Format::k8bppRgb);
    } else {
      SetFormat(FXDIB_Format::kBgr);
    }
  }

  std::optional<uint32_t> pitch = fxge::CalculatePitch32(GetBPP(), GetWidth());
  if (!pitch.has_value())
    return false;

  m_LineBuf = DataVector<uint8_t>(pitch.value());
  LoadPalette();
  if (m_bColorKey) {
    // TODO(crbug.com/355676038): Consider adding support for
    // `FXDIB_Format::kBgraPremul`
    SetFormat(FXDIB_Format::kBgra);
    pitch = fxge::CalculatePitch32(GetBPP(), GetWidth());
    if (!pitch.has_value())
      return false;
    m_MaskBuf = DataVector<uint8_t>(pitch.value());
  }
  SetPitch(pitch.value());
  return true;
}

CPDF_DIB::LoadState CPDF_DIB::StartLoadDIBBase(
    bool bHasMask,
    const CPDF_Dictionary* pFormResources,
    const CPDF_Dictionary* pPageResources,
    bool bStdCS,
    CPDF_ColorSpace::Family GroupFamily,
    bool bLoadMask,
    const CFX_Size& max_size_required) {
  m_bStdCS = bStdCS;
  m_bHasMask = bHasMask;
  m_GroupFamily = GroupFamily;
  m_bLoadMask = bLoadMask;

  if (!m_pStream->IsInline())
    pFormResources = nullptr;

  if (!LoadInternal(pFormResources, pPageResources))
    return LoadState::kFail;

  uint8_t resolution_levels_to_skip = 0;
  if (max_size_required.width != 0 && max_size_required.height != 0) {
    resolution_levels_to_skip = static_cast<uint8_t>(std::log2(
        std::max(1, std::min(GetWidth() / max_size_required.width,
                             GetHeight() / max_size_required.height))));
  }

  LoadState iCreatedDecoder = CreateDecoder(resolution_levels_to_skip);
  if (iCreatedDecoder == LoadState::kFail)
    return LoadState::kFail;

  if (!ContinueToLoadMask())
    return LoadState::kFail;

  LoadState iLoadedMask = m_bHasMask ? StartLoadMask() : LoadState::kSuccess;
  if (iCreatedDecoder == LoadState::kContinue ||
      iLoadedMask == LoadState::kContinue) {
    return LoadState::kContinue;
  }

  DCHECK_EQ(iCreatedDecoder, LoadState::kSuccess);
  DCHECK_EQ(iLoadedMask, LoadState::kSuccess);
  if (m_pColorSpace && m_bStdCS)
    m_pColorSpace->EnableStdConversion(false);
  return LoadState::kSuccess;
}

CPDF_DIB::LoadState CPDF_DIB::ContinueLoadDIBBase(PauseIndicatorIface* pPause) {
  if (m_Status == LoadState::kContinue)
    return ContinueLoadMaskDIB(pPause);

  ByteString decoder = m_pStreamAcc->GetImageDecoder();
  if (decoder == "JPXDecode")
    return LoadState::kFail;

  if (decoder != "JBIG2Decode")
    return LoadState::kSuccess;

  if (m_Status == LoadState::kFail)
    return LoadState::kFail;

  FXCODEC_STATUS iDecodeStatus;
  if (!m_pJbig2Context) {
    m_pJbig2Context = std::make_unique<Jbig2Context>();
    if (m_pStreamAcc->GetImageParam()) {
      RetainPtr<const CPDF_Stream> pGlobals =
          m_pStreamAcc->GetImageParam()->GetStreamFor("JBIG2Globals");
      if (pGlobals) {
        m_pGlobalAcc = pdfium::MakeRetain<CPDF_StreamAcc>(std::move(pGlobals));
        m_pGlobalAcc->LoadAllDataFiltered();
      }
    }
    uint64_t nSrcKey = 0;
    pdfium::span<const uint8_t> pSrcSpan;
    if (m_pStreamAcc) {
      pSrcSpan = m_pStreamAcc->GetSpan();
      nSrcKey = m_pStreamAcc->KeyForCache();
    }
    uint64_t nGlobalKey = 0;
    pdfium::span<const uint8_t> pGlobalSpan;
    if (m_pGlobalAcc) {
      pGlobalSpan = m_pGlobalAcc->GetSpan();
      nGlobalKey = m_pGlobalAcc->KeyForCache();
    }
    iDecodeStatus = Jbig2Decoder::StartDecode(
        m_pJbig2Context.get(), m_pDocument->GetOrCreateCodecContext(),
        GetWidth(), GetHeight(), pSrcSpan, nSrcKey, pGlobalSpan, nGlobalKey,
        m_pCachedBitmap->GetWritableBuffer(), m_pCachedBitmap->GetPitch(),
        pPause);
  } else {
    iDecodeStatus = Jbig2Decoder::ContinueDecode(m_pJbig2Context.get(), pPause);
  }

  if (iDecodeStatus == FXCODEC_STATUS::kError) {
    m_pJbig2Context.reset();
    m_pCachedBitmap.Reset();
    m_pGlobalAcc.Reset();
    return LoadState::kFail;
  }
  if (iDecodeStatus == FXCODEC_STATUS::kDecodeToBeContinued)
    return LoadState::kContinue;

  LoadState iContinueStatus = LoadState::kSuccess;
  if (m_bHasMask) {
    if (ContinueLoadMaskDIB(pPause) == LoadState::kContinue) {
      iContinueStatus = LoadState::kContinue;
      m_Status = LoadState::kContinue;
    }
  }
  if (iContinueStatus == LoadState::kContinue)
    return LoadState::kContinue;

  if (m_pColorSpace && m_bStdCS)
    m_pColorSpace->EnableStdConversion(false);
  return iContinueStatus;
}

bool CPDF_DIB::LoadColorInfo(const CPDF_Dictionary* pFormResources,
                             const CPDF_Dictionary* pPageResources) {
  std::optional<DecoderArray> decoder_array = GetDecoderArray(m_pDict);
  if (!decoder_array.has_value())
    return false;

  m_bpc_orig = m_pDict->GetIntegerFor("BitsPerComponent");
  if (!IsMaybeValidBitsPerComponent(m_bpc_orig))
    return false;

  m_bImageMask = m_pDict->GetBooleanFor("ImageMask", /*bDefault=*/false);

  if (m_bImageMask || !m_pDict->KeyExist("ColorSpace")) {
    if (!m_bImageMask && !decoder_array.value().empty()) {
      const ByteString& filter = decoder_array.value().back().first;
      if (filter == "JPXDecode") {
        m_bDoBpcCheck = false;
        return true;
      }
    }
    m_bImageMask = true;
    m_bpc = m_nComponents = 1;
    RetainPtr<const CPDF_Array> pDecode = m_pDict->GetArrayFor("Decode");
    m_bDefaultDecode = !pDecode || !pDecode->GetIntegerAt(0);
    return true;
  }

  RetainPtr<const CPDF_Object> pCSObj =
      m_pDict->GetDirectObjectFor("ColorSpace");
  if (!pCSObj)
    return false;

  auto* pDocPageData = CPDF_DocPageData::FromDocument(m_pDocument);
  if (pFormResources)
    m_pColorSpace = pDocPageData->GetColorSpace(pCSObj.Get(), pFormResources);
  if (!m_pColorSpace)
    m_pColorSpace = pDocPageData->GetColorSpace(pCSObj.Get(), pPageResources);
  if (!m_pColorSpace)
    return false;

  // If the checks above failed to find a colorspace, and the next line to set
  // |m_nComponents| does not get reached, then a decoder can try to set
  // |m_nComponents| based on the number of channels in the image being
  // decoded.
  m_nComponents = m_pColorSpace->ComponentCount();
  m_Family = m_pColorSpace->GetFamily();
  if (m_Family == CPDF_ColorSpace::Family::kICCBased && pCSObj->IsName()) {
    ByteString cs = pCSObj->GetString();
    if (cs == "DeviceGray")
      m_nComponents = 1;
    else if (cs == "DeviceRGB")
      m_nComponents = 3;
    else if (cs == "DeviceCMYK")
      m_nComponents = 4;
  }

  ByteString filter;
  if (!decoder_array.value().empty())
    filter = decoder_array.value().back().first;

  if (!ValidateDictParam(filter))
    return false;

  return GetDecodeAndMaskArray();
}

bool CPDF_DIB::GetDecodeAndMaskArray() {
  if (!m_pColorSpace)
    return false;

  m_CompData.resize(m_nComponents);
  int max_data = (1 << m_bpc) - 1;
  RetainPtr<const CPDF_Array> pDecode = m_pDict->GetArrayFor("Decode");
  if (pDecode) {
    for (uint32_t i = 0; i < m_nComponents; i++) {
      m_CompData[i].m_DecodeMin = pDecode->GetFloatAt(i * 2);
      float max = pDecode->GetFloatAt(i * 2 + 1);
      m_CompData[i].m_DecodeStep = (max - m_CompData[i].m_DecodeMin) / max_data;
      float def_value;
      float def_min;
      float def_max;
      m_pColorSpace->GetDefaultValue(i, &def_value, &def_min, &def_max);
      if (m_Family == CPDF_ColorSpace::Family::kIndexed)
        def_max = max_data;
      if (def_min != m_CompData[i].m_DecodeMin || def_max != max)
        m_bDefaultDecode = false;
    }
  } else {
    for (uint32_t i = 0; i < m_nComponents; i++) {
      float def_value;
      m_pColorSpace->GetDefaultValue(i, &def_value, &m_CompData[i].m_DecodeMin,
                                     &m_CompData[i].m_DecodeStep);
      if (m_Family == CPDF_ColorSpace::Family::kIndexed)
        m_CompData[i].m_DecodeStep = max_data;
      m_CompData[i].m_DecodeStep =
          (m_CompData[i].m_DecodeStep - m_CompData[i].m_DecodeMin) / max_data;
    }
  }
  if (m_pDict->KeyExist("SMask"))
    return true;

  RetainPtr<const CPDF_Object> pMask = m_pDict->GetDirectObjectFor("Mask");
  if (!pMask)
    return true;

  if (const CPDF_Array* pArray = pMask->AsArray()) {
    if (pArray->size() >= m_nComponents * 2) {
      for (uint32_t i = 0; i < m_nComponents; i++) {
        int min_num = pArray->GetIntegerAt(i * 2);
        int max_num = pArray->GetIntegerAt(i * 2 + 1);
        m_CompData[i].m_ColorKeyMin = std::max(min_num, 0);
        m_CompData[i].m_ColorKeyMax = std::min(max_num, max_data);
      }
    }
    m_bColorKey = true;
  }
  return true;
}

CPDF_DIB::LoadState CPDF_DIB::CreateDecoder(uint8_t resolution_levels_to_skip) {
  ByteString decoder = m_pStreamAcc->GetImageDecoder();
  if (decoder.IsEmpty())
    return LoadState::kSuccess;

  if (m_bDoBpcCheck && m_bpc == 0)
    return LoadState::kFail;

  if (decoder == "JPXDecode") {
    m_pCachedBitmap = LoadJpxBitmap(resolution_levels_to_skip);
    return m_pCachedBitmap ? LoadState::kSuccess : LoadState::kFail;
  }

  if (decoder == "JBIG2Decode") {
    m_pCachedBitmap = pdfium::MakeRetain<CFX_DIBitmap>();
    if (!m_pCachedBitmap->Create(
            GetWidth(), GetHeight(),
            m_bImageMask ? FXDIB_Format::k1bppMask : FXDIB_Format::k1bppRgb)) {
      m_pCachedBitmap.Reset();
      return LoadState::kFail;
    }
    m_Status = LoadState::kSuccess;
    return LoadState::kContinue;
  }

  pdfium::span<const uint8_t> src_span = m_pStreamAcc->GetSpan();
  RetainPtr<const CPDF_Dictionary> pParams = m_pStreamAcc->GetImageParam();
  if (decoder == "CCITTFaxDecode") {
    m_pDecoder = CreateFaxDecoder(src_span, GetWidth(), GetHeight(), pParams);
  } else if (decoder == "FlateDecode") {
    m_pDecoder = CreateFlateDecoder(src_span, GetWidth(), GetHeight(),
                                    m_nComponents, m_bpc, pParams);
  } else if (decoder == "RunLengthDecode") {
    m_pDecoder = BasicModule::CreateRunLengthDecoder(
        src_span, GetWidth(), GetHeight(), m_nComponents, m_bpc);
  } else if (decoder == "DCTDecode") {
    if (!CreateDCTDecoder(src_span, pParams))
      return LoadState::kFail;
  }
  if (!m_pDecoder)
    return LoadState::kFail;

  const std::optional<uint32_t> requested_pitch =
      fxge::CalculatePitch8(m_bpc, m_nComponents, GetWidth());
  if (!requested_pitch.has_value())
    return LoadState::kFail;
  const std::optional<uint32_t> provided_pitch = fxge::CalculatePitch8(
      m_pDecoder->GetBPC(), m_pDecoder->CountComps(), m_pDecoder->GetWidth());
  if (!provided_pitch.has_value())
    return LoadState::kFail;
  if (provided_pitch.value() < requested_pitch.value())
    return LoadState::kFail;
  return LoadState::kSuccess;
}

bool CPDF_DIB::CreateDCTDecoder(pdfium::span<const uint8_t> src_span,
                                const CPDF_Dictionary* pParams) {
  m_pDecoder = JpegModule::CreateDecoder(
      src_span, GetWidth(), GetHeight(), m_nComponents,
      !pParams || pParams->GetIntegerFor("ColorTransform", 1));
  if (m_pDecoder)
    return true;

  std::optional<JpegModule::ImageInfo> info_opt =
      JpegModule::LoadInfo(src_span);
  if (!info_opt.has_value())
    return false;

  const JpegModule::ImageInfo& info = info_opt.value();
  SetWidth(info.width);
  SetHeight(info.height);

  if (!CPDF_Image::IsValidJpegComponent(info.num_components) ||
      !CPDF_Image::IsValidJpegBitsPerComponent(info.bits_per_components)) {
    return false;
  }

  if (m_nComponents == static_cast<uint32_t>(info.num_components)) {
    m_bpc = info.bits_per_components;
    m_pDecoder = JpegModule::CreateDecoder(src_span, GetWidth(), GetHeight(),
                                           m_nComponents, info.color_transform);
    return true;
  }

  m_nComponents = static_cast<uint32_t>(info.num_components);
  m_CompData.clear();
  if (m_pColorSpace) {
    uint32_t colorspace_comps = m_pColorSpace->ComponentCount();
    switch (m_Family) {
      case CPDF_ColorSpace::Family::kDeviceGray:
      case CPDF_ColorSpace::Family::kDeviceRGB:
      case CPDF_ColorSpace::Family::kDeviceCMYK: {
        uint32_t dwMinComps = CPDF_ColorSpace::ComponentsForFamily(m_Family);
        if (colorspace_comps < dwMinComps || m_nComponents < dwMinComps)
          return false;
        break;
      }
      case CPDF_ColorSpace::Family::kLab: {
        if (m_nComponents != 3 || colorspace_comps < 3)
          return false;
        break;
      }
      case CPDF_ColorSpace::Family::kICCBased: {
        if (!fxcodec::IccTransform::IsValidIccComponents(colorspace_comps) ||
            !fxcodec::IccTransform::IsValidIccComponents(m_nComponents) ||
            colorspace_comps < m_nComponents) {
          return false;
        }
        break;
      }
      default: {
        if (colorspace_comps != m_nComponents)
          return false;
        break;
      }
    }
  } else {
    if (m_Family == CPDF_ColorSpace::Family::kLab && m_nComponents != 3)
      return false;
  }
  if (!GetDecodeAndMaskArray())
    return false;

  m_bpc = info.bits_per_components;
  m_pDecoder = JpegModule::CreateDecoder(src_span, GetWidth(), GetHeight(),
                                         m_nComponents, info.color_transform);
  return true;
}

RetainPtr<CFX_DIBitmap> CPDF_DIB::LoadJpxBitmap(
    uint8_t resolution_levels_to_skip) {
  std::unique_ptr<CJPX_Decoder> decoder =
      CJPX_Decoder::Create(m_pStreamAcc->GetSpan(),
                           ColorSpaceOptionFromColorSpace(m_pColorSpace.Get()),
                           resolution_levels_to_skip, /*strict_mode=*/true);
  if (!decoder)
    return nullptr;

  SetWidth(GetWidth() >> resolution_levels_to_skip);
  SetHeight(GetHeight() >> resolution_levels_to_skip);

  if (!decoder->StartDecode())
    return nullptr;

  CJPX_Decoder::JpxImageInfo image_info = decoder->GetInfo();
  if (static_cast<int>(image_info.width) < GetWidth() ||
      static_cast<int>(image_info.height) < GetHeight()) {
    return nullptr;
  }

  RetainPtr<CPDF_ColorSpace> original_colorspace = m_pColorSpace;
  bool swap_rgb = false;
  bool convert_argb_to_rgb = false;
  auto action = GetJpxDecodeAction(image_info, m_pColorSpace.Get());
  switch (action) {
    case JpxDecodeAction::kFail:
      return nullptr;

    case JpxDecodeAction::kDoNothing:
      break;

    case JpxDecodeAction::kUseGray:
      m_pColorSpace =
          CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray);
      break;

    case JpxDecodeAction::kUseRgb:
      DCHECK(image_info.channels >= 3);
      swap_rgb = true;
      m_pColorSpace = nullptr;
      break;

    case JpxDecodeAction::kUseCmyk:
      m_pColorSpace =
          CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceCMYK);
      break;

    case JpxDecodeAction::kConvertArgbToRgb:
      swap_rgb = true;
      convert_argb_to_rgb = true;
      m_pColorSpace.Reset();
  }

  // If |original_colorspace| exists, then LoadColorInfo() already set
  // |m_nComponents|.
  if (original_colorspace) {
    DCHECK_NE(0u, m_nComponents);
  } else {
    DCHECK_EQ(0u, m_nComponents);
    m_nComponents = GetComponentCountFromOpjColorSpace(image_info.colorspace);
    if (m_nComponents == 0) {
      return nullptr;
    }
  }

  FXDIB_Format format;
  if (action == JpxDecodeAction::kUseGray) {
    format = FXDIB_Format::k8bppRgb;
  } else if (action == JpxDecodeAction::kUseRgb && image_info.channels == 3) {
    format = FXDIB_Format::kBgr;
  } else if (action == JpxDecodeAction::kUseRgb && image_info.channels == 4) {
    format = FXDIB_Format::kBgrx;
  } else if (action == JpxDecodeAction::kConvertArgbToRgb) {
    CHECK_GE(image_info.channels, 4);
    format = FXDIB_Format::kBgrx;
  } else {
    image_info.width = (image_info.width * image_info.channels + 2) / 3;
    format = FXDIB_Format::kBgr;
  }

  auto result_bitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!result_bitmap->Create(image_info.width, image_info.height, format))
    return nullptr;

  result_bitmap->Clear(0xFFFFFFFF);
  if (!decoder->Decode(result_bitmap->GetWritableBuffer(),
                       result_bitmap->GetPitch(), swap_rgb, m_nComponents)) {
    return nullptr;
  }

  if (convert_argb_to_rgb) {
    DCHECK_EQ(3u, m_nComponents);
    auto rgb_bitmap = pdfium::MakeRetain<CFX_DIBitmap>();
    if (!rgb_bitmap->Create(image_info.width, image_info.height,
                            FXDIB_Format::kBgr)) {
      return nullptr;
    }
    if (m_pDict->GetIntegerFor("SMaskInData") == 1) {
      // TODO(thestig): Acrobat does not support "/SMaskInData 1" combined with
      // filters. Check for that and fail early.
      DCHECK(m_JpxInlineData.data.empty());
      m_JpxInlineData.width = image_info.width;
      m_JpxInlineData.height = image_info.height;
      m_JpxInlineData.data.reserve(image_info.width * image_info.height);
      for (uint32_t row = 0; row < image_info.height; ++row) {
        auto src =
            result_bitmap->GetScanlineAs<FX_BGRA_STRUCT<uint8_t>>(row).first(
                image_info.width);
        auto dest =
            rgb_bitmap->GetWritableScanlineAs<FX_BGR_STRUCT<uint8_t>>(row);
        for (auto [input, output] : fxcrt::Zip(src, dest)) {
          m_JpxInlineData.data.push_back(input.alpha);
          const uint8_t na = 255 - input.alpha;
          output.blue = (input.blue * input.alpha + 255 * na) / 255;
          output.green = (input.green * input.alpha + 255 * na) / 255;
          output.red = (input.red * input.alpha + 255 * na) / 255;
        }
      }
    } else {
      // TODO(thestig): Is there existing code that does this already?
      for (uint32_t row = 0; row < image_info.height; ++row) {
        auto src =
            result_bitmap->GetScanlineAs<FX_BGRA_STRUCT<uint8_t>>(row).first(
                image_info.width);
        auto dest =
            rgb_bitmap->GetWritableScanlineAs<FX_BGR_STRUCT<uint8_t>>(row);
        for (auto [input, output] : fxcrt::Zip(src, dest)) {
          output.green = input.green;
          output.red = input.red;
          output.blue = input.blue;
        }
      }
    }
    result_bitmap = std::move(rgb_bitmap);
  } else if (m_pColorSpace &&
             m_pColorSpace->GetFamily() == CPDF_ColorSpace::Family::kIndexed &&
             m_bpc < 8) {
    int scale = 8 - m_bpc;
    for (uint32_t row = 0; row < image_info.height; ++row) {
      pdfium::span<uint8_t> scanline =
          result_bitmap->GetWritableScanline(row).first(image_info.width);
      for (auto& pixel : scanline) {
        pixel >>= scale;
      }
    }
  }

  // TODO(crbug.com/pdfium/1747): Handle SMaskInData entries for different
  // color space types.

  m_bpc = 8;
  return result_bitmap;
}

bool CPDF_DIB::LoadInternal(const CPDF_Dictionary* pFormResources,
                            const CPDF_Dictionary* pPageResources) {
  if (!m_pStream)
    return false;

  m_pDict = m_pStream->GetDict();
  SetWidth(m_pDict->GetIntegerFor("Width"));
  SetHeight(m_pDict->GetIntegerFor("Height"));
  if (!IsValidDimension(GetWidth()) || !IsValidDimension(GetHeight())) {
    return false;
  }

  if (!LoadColorInfo(pFormResources, pPageResources))
    return false;

  if (m_bDoBpcCheck && (m_bpc == 0 || m_nComponents == 0))
    return false;

  const std::optional<uint32_t> maybe_size =
      fxge::CalculatePitch8(m_bpc, m_nComponents, GetWidth());
  if (!maybe_size.has_value())
    return false;

  FX_SAFE_UINT32 src_size = maybe_size.value();
  src_size *= GetHeight();
  if (!src_size.IsValid())
    return false;

  m_pStreamAcc = pdfium::MakeRetain<CPDF_StreamAcc>(m_pStream);
  m_pStreamAcc->LoadAllDataImageAcc(src_size.ValueOrDie());
  return !m_pStreamAcc->GetSpan().empty();
}

CPDF_DIB::LoadState CPDF_DIB::StartLoadMask() {
  m_MatteColor = 0XFFFFFFFF;

  if (!m_JpxInlineData.data.empty()) {
    auto dict = pdfium::MakeRetain<CPDF_Dictionary>();
    dict->SetNewFor<CPDF_Name>("Type", "XObject");
    dict->SetNewFor<CPDF_Name>("Subtype", "Image");
    dict->SetNewFor<CPDF_Name>("ColorSpace", "DeviceGray");
    dict->SetNewFor<CPDF_Number>("Width", m_JpxInlineData.width);
    dict->SetNewFor<CPDF_Number>("Height", m_JpxInlineData.height);
    dict->SetNewFor<CPDF_Number>("BitsPerComponent", 8);

    return StartLoadMaskDIB(
        pdfium::MakeRetain<CPDF_Stream>(m_JpxInlineData.data, std::move(dict)));
  }

  RetainPtr<const CPDF_Stream> mask(m_pDict->GetStreamFor("SMask"));
  if (!mask) {
    mask = ToStream(m_pDict->GetDirectObjectFor("Mask"));
    return mask ? StartLoadMaskDIB(std::move(mask)) : LoadState::kSuccess;
  }

  RetainPtr<const CPDF_Array> pMatte = mask->GetDict()->GetArrayFor("Matte");
  if (pMatte && m_pColorSpace &&
      m_Family != CPDF_ColorSpace::Family::kPattern &&
      pMatte->size() == m_nComponents &&
      m_pColorSpace->ComponentCount() <= m_nComponents) {
    std::vector<float> colors =
        ReadArrayElementsToVector(pMatte.Get(), m_nComponents);

    auto rgb = m_pColorSpace->GetRGBOrZerosOnError(colors);
    m_MatteColor =
        ArgbEncode(0, FXSYS_roundf(rgb.red * 255),
                   FXSYS_roundf(rgb.green * 255), FXSYS_roundf(rgb.blue * 255));
  }
  return StartLoadMaskDIB(std::move(mask));
}

CPDF_DIB::LoadState CPDF_DIB::ContinueLoadMaskDIB(PauseIndicatorIface* pPause) {
  if (!m_pMask)
    return LoadState::kSuccess;

  LoadState ret = m_pMask->ContinueLoadDIBBase(pPause);
  if (ret == LoadState::kContinue)
    return LoadState::kContinue;

  if (m_pColorSpace && m_bStdCS)
    m_pColorSpace->EnableStdConversion(false);

  if (ret == LoadState::kFail) {
    m_pMask.Reset();
    return LoadState::kFail;
  }
  return LoadState::kSuccess;
}

RetainPtr<CPDF_DIB> CPDF_DIB::DetachMask() {
  return std::move(m_pMask);
}

bool CPDF_DIB::IsJBigImage() const {
  return m_pStreamAcc->GetImageDecoder() == "JBIG2Decode";
}

CPDF_DIB::LoadState CPDF_DIB::StartLoadMaskDIB(
    RetainPtr<const CPDF_Stream> mask_stream) {
  m_pMask = pdfium::MakeRetain<CPDF_DIB>(m_pDocument, std::move(mask_stream));
  LoadState ret = m_pMask->StartLoadDIBBase(false, nullptr, nullptr, true,
                                            CPDF_ColorSpace::Family::kUnknown,
                                            false, {0, 0});
  if (ret == LoadState::kContinue) {
    if (m_Status == LoadState::kFail)
      m_Status = LoadState::kContinue;
    return LoadState::kContinue;
  }
  if (ret == LoadState::kFail)
    m_pMask.Reset();
  return LoadState::kSuccess;
}

void CPDF_DIB::LoadPalette() {
  if (!m_pColorSpace || m_Family == CPDF_ColorSpace::Family::kPattern)
    return;

  if (m_bpc == 0)
    return;

  // Use FX_SAFE_UINT32 just to be on the safe side, in case |m_bpc| or
  // |m_nComponents| somehow gets a bad value.
  FX_SAFE_UINT32 safe_bits = m_bpc;
  safe_bits *= m_nComponents;
  uint32_t bits = safe_bits.ValueOrDefault(255);
  if (bits > 8)
    return;

  if (bits == 1) {
    if (m_bDefaultDecode && (m_Family == CPDF_ColorSpace::Family::kDeviceGray ||
                             m_Family == CPDF_ColorSpace::Family::kDeviceRGB)) {
      return;
    }
    if (m_pColorSpace->ComponentCount() > 3) {
      return;
    }
    float color_values[3];
    std::fill(std::begin(color_values), std::end(color_values),
              m_CompData[0].m_DecodeMin);

    auto rgb = m_pColorSpace->GetRGBOrZerosOnError(color_values);
    FX_ARGB argb0 =
        ArgbEncode(255, FXSYS_roundf(rgb.red * 255),
                   FXSYS_roundf(rgb.green * 255), FXSYS_roundf(rgb.blue * 255));
    FX_ARGB argb1;
    const CPDF_IndexedCS* indexed_cs = m_pColorSpace->AsIndexedCS();
    if (indexed_cs && indexed_cs->GetMaxIndex() == 0) {
      // If an indexed color space's hival value is 0, only 1 color is specified
      // in the lookup table. Another color should be set to 0xFF000000 by
      // default to set the range of the color space.
      argb1 = 0xFF000000;
    } else {
      color_values[0] += m_CompData[0].m_DecodeStep;
      color_values[1] += m_CompData[0].m_DecodeStep;
      color_values[2] += m_CompData[0].m_DecodeStep;
      auto result = m_pColorSpace->GetRGBOrZerosOnError(color_values);
      argb1 = ArgbEncode(255, FXSYS_roundf(result.red * 255),
                         FXSYS_roundf(result.green * 255),
                         FXSYS_roundf(result.blue * 255));
    }

    if (argb0 != 0xFF000000 || argb1 != 0xFFFFFFFF) {
      SetPaletteArgb(0, argb0);
      SetPaletteArgb(1, argb1);
    }
    return;
  }
  if (m_bpc == 8 && m_bDefaultDecode &&
      m_pColorSpace ==
          CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceGray)) {
    return;
  }

  int palette_count = 1 << bits;
  // Using at least 16 elements due to the call m_pColorSpace->GetRGB().
  std::vector<float> color_values(std::max(m_nComponents, 16u));
  for (int i = 0; i < palette_count; i++) {
    int color_data = i;
    for (uint32_t j = 0; j < m_nComponents; j++) {
      int encoded_component = color_data % (1 << m_bpc);
      color_data /= 1 << m_bpc;
      color_values[j] = m_CompData[j].m_DecodeMin +
                        m_CompData[j].m_DecodeStep * encoded_component;
    }
    FX_RGB_STRUCT<float> rgb;
    if (m_nComponents == 1 && m_Family == CPDF_ColorSpace::Family::kICCBased &&
        m_pColorSpace->ComponentCount() > 1) {
      const size_t nComponents = m_pColorSpace->ComponentCount();
      std::vector<float> temp_buf(nComponents, color_values[0]);
      rgb = m_pColorSpace->GetRGBOrZerosOnError(temp_buf);
    } else {
      rgb = m_pColorSpace->GetRGBOrZerosOnError(color_values);
    }
    SetPaletteArgb(i, ArgbEncode(255, FXSYS_roundf(rgb.red * 255),
                                 FXSYS_roundf(rgb.green * 255),
                                 FXSYS_roundf(rgb.blue * 255)));
  }
}

bool CPDF_DIB::ValidateDictParam(const ByteString& filter) {
  m_bpc = m_bpc_orig;

  // Per spec, |m_bpc| should always be 8 for RunLengthDecode, but too many
  // documents do not conform to it. So skip this check.

  if (filter == "JPXDecode") {
    m_bDoBpcCheck = false;
    return true;
  }

  if (filter == "CCITTFaxDecode" || filter == "JBIG2Decode") {
    m_bpc = 1;
    m_nComponents = 1;
  } else if (filter == "DCTDecode") {
    m_bpc = 8;
  }

  if (!IsAllowedBitsPerComponent(m_bpc)) {
    m_bpc = 0;
    return false;
  }
  return true;
}

void CPDF_DIB::TranslateScanline24bpp(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan) const {
  if (m_bpc == 0)
    return;

  if (TranslateScanline24bppDefaultDecode(dest_scan, src_scan))
    return;

  // Using at least 16 elements due to the call m_pColorSpace->GetRGB().
  std::vector<float> color_values(std::max(m_nComponents, 16u));
  FX_RGB_STRUCT<float> rgb = {};
  uint64_t src_bit_pos = 0;
  uint64_t src_byte_pos = 0;
  size_t dest_byte_pos = 0;
  const bool bpp8 = m_bpc == 8;
  for (int column = 0; column < GetWidth(); column++) {
    for (uint32_t color = 0; color < m_nComponents; color++) {
      if (bpp8) {
        uint8_t data = src_scan[src_byte_pos++];
        color_values[color] = m_CompData[color].m_DecodeMin +
                              m_CompData[color].m_DecodeStep * data;
      } else {
        unsigned int data = GetBits8(src_scan, src_bit_pos, m_bpc);
        color_values[color] = m_CompData[color].m_DecodeMin +
                              m_CompData[color].m_DecodeStep * data;
        src_bit_pos += m_bpc;
      }
    }
    if (TransMask()) {
      float k = 1.0f - color_values[3];
      rgb.red = (1.0f - color_values[0]) * k;
      rgb.green = (1.0f - color_values[1]) * k;
      rgb.blue = (1.0f - color_values[2]) * k;
    } else if (m_Family != CPDF_ColorSpace::Family::kPattern) {
      rgb = m_pColorSpace->GetRGBOrZerosOnError(color_values);
    }
    const float R = std::clamp(rgb.red, 0.0f, 1.0f);
    const float G = std::clamp(rgb.green, 0.0f, 1.0f);
    const float B = std::clamp(rgb.blue, 0.0f, 1.0f);
    dest_scan[dest_byte_pos] = static_cast<uint8_t>(B * 255);
    dest_scan[dest_byte_pos + 1] = static_cast<uint8_t>(G * 255);
    dest_scan[dest_byte_pos + 2] = static_cast<uint8_t>(R * 255);
    dest_byte_pos += 3;
  }
}

bool CPDF_DIB::TranslateScanline24bppDefaultDecode(
    pdfium::span<uint8_t> dest_scan,
    pdfium::span<const uint8_t> src_scan) const {
  if (!m_bDefaultDecode)
    return false;

  if (m_Family != CPDF_ColorSpace::Family::kDeviceRGB &&
      m_Family != CPDF_ColorSpace::Family::kCalRGB) {
    if (m_bpc != 8)
      return false;

    if (m_nComponents == m_pColorSpace->ComponentCount()) {
      m_pColorSpace->TranslateImageLine(dest_scan, src_scan, GetWidth(),
                                        GetWidth(), GetHeight(), TransMask());
    }
    return true;
  }

  if (m_nComponents != 3)
    return true;

  uint8_t* dest_pos = dest_scan.data();
  const uint8_t* src_pos = src_scan.data();
  switch (m_bpc) {
    case 8:
      UNSAFE_TODO({
        for (int column = 0; column < GetWidth(); column++) {
          *dest_pos++ = src_pos[2];
          *dest_pos++ = src_pos[1];
          *dest_pos++ = *src_pos;
          src_pos += 3;
        }
      });
      break;
    case 16:
      UNSAFE_TODO({
        for (int col = 0; col < GetWidth(); col++) {
          *dest_pos++ = src_pos[4];
          *dest_pos++ = src_pos[2];
          *dest_pos++ = *src_pos;
          src_pos += 6;
        }
      });
      break;
    default:
      const unsigned int max_data = (1 << m_bpc) - 1;
      uint64_t src_bit_pos = 0;
      size_t dest_byte_pos = 0;
      for (int column = 0; column < GetWidth(); column++) {
        unsigned int R = GetBits8(src_scan, src_bit_pos, m_bpc);
        src_bit_pos += m_bpc;
        unsigned int G = GetBits8(src_scan, src_bit_pos, m_bpc);
        src_bit_pos += m_bpc;
        unsigned int B = GetBits8(src_scan, src_bit_pos, m_bpc);
        src_bit_pos += m_bpc;
        R = std::min(R, max_data);
        G = std::min(G, max_data);
        B = std::min(B, max_data);
        UNSAFE_TODO({
          dest_pos[dest_byte_pos] = B * 255 / max_data;
          dest_pos[dest_byte_pos + 1] = G * 255 / max_data;
          dest_pos[dest_byte_pos + 2] = R * 255 / max_data;
          dest_byte_pos += 3;
        });
      }
      break;
  }
  return true;
}

pdfium::span<const uint8_t> CPDF_DIB::GetScanline(int line) const {
  if (m_bpc == 0)
    return pdfium::span<const uint8_t>();

  const std::optional<uint32_t> src_pitch =
      fxge::CalculatePitch8(m_bpc, m_nComponents, GetWidth());
  if (!src_pitch.has_value())
    return pdfium::span<const uint8_t>();

  uint32_t src_pitch_value = src_pitch.value();
  // This is used as the buffer of `pSrcLine` when the stream is truncated,
  // and the remaining bytes count is less than `src_pitch_value`
  DataVector<uint8_t> temp_buffer;
  pdfium::span<const uint8_t> pSrcLine;

  if (m_pCachedBitmap && src_pitch_value <= m_pCachedBitmap->GetPitch()) {
    if (line >= m_pCachedBitmap->GetHeight())
      line = m_pCachedBitmap->GetHeight() - 1;
    pSrcLine = m_pCachedBitmap->GetScanline(line);
  } else if (m_pDecoder) {
    pSrcLine = m_pDecoder->GetScanline(line);
  } else if (m_pStreamAcc->GetSize() > line * src_pitch_value) {
    pdfium::span<const uint8_t> remaining_bytes =
        m_pStreamAcc->GetSpan().subspan(line * src_pitch_value);
    if (remaining_bytes.size() >= src_pitch_value) {
      pSrcLine = remaining_bytes.first(src_pitch_value);
    } else {
      temp_buffer = DataVector<uint8_t>(src_pitch_value);
      fxcrt::Copy(remaining_bytes, temp_buffer);
      pSrcLine = temp_buffer;
    }
  }

  if (pSrcLine.empty()) {
    pdfium::span<uint8_t> result = !m_MaskBuf.empty() ? m_MaskBuf : m_LineBuf;
    fxcrt::Fill(result, 0);
    return result;
  }
  if (m_bpc * m_nComponents == 1) {
    if (m_bImageMask && m_bDefaultDecode) {
      for (uint32_t i = 0; i < src_pitch_value; i++) {
        // TODO(tsepez): Bounds check if cost is acceptable.
        UNSAFE_TODO(m_LineBuf[i] = ~pSrcLine.data()[i]);
      }
      return pdfium::make_span(m_LineBuf).first(src_pitch_value);
    }
    if (!m_bColorKey) {
      fxcrt::Copy(pSrcLine.first(src_pitch_value), m_LineBuf);
      return pdfium::make_span(m_LineBuf).first(src_pitch_value);
    }
    uint32_t reset_argb = Get1BitResetValue();
    uint32_t set_argb = Get1BitSetValue();
    auto mask32_span =
        fxcrt::reinterpret_span<uint32_t>(pdfium::make_span(m_MaskBuf));
    for (int col = 0; col < GetWidth(); col++) {
      mask32_span[col] = GetBitValue(pSrcLine, col) ? set_argb : reset_argb;
    }
    return fxcrt::reinterpret_span<uint8_t>(mask32_span.first(GetWidth()));
  }
  if (m_bpc * m_nComponents <= 8) {
    pdfium::span<uint8_t> result = m_LineBuf;
    if (m_bpc == 8) {
      fxcrt::Copy(pSrcLine.first(src_pitch_value), result);
      result = result.first(src_pitch_value);
    } else {
      uint64_t src_bit_pos = 0;
      for (int col = 0; col < GetWidth(); col++) {
        unsigned int color_index = 0;
        for (uint32_t color = 0; color < m_nComponents; color++) {
          unsigned int data = GetBits8(pSrcLine, src_bit_pos, m_bpc);
          color_index |= data << (color * m_bpc);
          src_bit_pos += m_bpc;
        }
        m_LineBuf[col] = color_index;
      }
      result = result.first(GetWidth());
    }
    if (!m_bColorKey)
      return result;

    uint8_t* pDestPixel = m_MaskBuf.data();
    const uint8_t* pSrcPixel = m_LineBuf.data();
    pdfium::span<const uint32_t> palette = GetPaletteSpan();
    UNSAFE_TODO({
      if (HasPalette()) {
        for (int col = 0; col < GetWidth(); col++) {
          uint8_t index = *pSrcPixel++;
          *pDestPixel++ = FXARGB_B(palette[index]);
          *pDestPixel++ = FXARGB_G(palette[index]);
          *pDestPixel++ = FXARGB_R(palette[index]);
          *pDestPixel++ =
              IsColorIndexOutOfBounds(index, m_CompData[0]) ? 0xFF : 0;
        }
      } else {
        for (int col = 0; col < GetWidth(); col++) {
          uint8_t index = *pSrcPixel++;
          *pDestPixel++ = index;
          *pDestPixel++ = index;
          *pDestPixel++ = index;
          *pDestPixel++ =
              IsColorIndexOutOfBounds(index, m_CompData[0]) ? 0xFF : 0;
        }
      }
    });
    return pdfium::make_span(m_MaskBuf).first(4 * GetWidth());
  }
  if (m_bColorKey) {
    if (m_nComponents == 3 && m_bpc == 8) {
      UNSAFE_TODO({
        uint8_t* alpha_channel = m_MaskBuf.data() + 3;
        for (int col = 0; col < GetWidth(); col++) {
          const uint8_t* pPixel = pSrcLine.data() + col * 3;
          alpha_channel[col * 4] =
              AreColorIndicesOutOfBounds(pPixel, m_CompData.data(), 3) ? 0xFF
                                                                       : 0;
        }
      });
    } else {
      fxcrt::Fill(m_MaskBuf, 0xFF);
    }
  }
  if (m_pColorSpace) {
    TranslateScanline24bpp(m_LineBuf, pSrcLine);
    src_pitch_value = 3 * GetWidth();
    pSrcLine = pdfium::make_span(m_LineBuf).first(src_pitch_value);
  }
  if (!m_bColorKey)
    return pSrcLine;

  // TODO(tsepez): Bounds check if cost is acceptable.
  const uint8_t* pSrcPixel = pSrcLine.data();
  uint8_t* pDestPixel = m_MaskBuf.data();
  UNSAFE_TODO({
    for (int col = 0; col < GetWidth(); col++) {
      *pDestPixel++ = *pSrcPixel++;
      *pDestPixel++ = *pSrcPixel++;
      *pDestPixel++ = *pSrcPixel++;
      pDestPixel++;
    }
  });
  return pdfium::make_span(m_MaskBuf).first(4 * GetWidth());
}

bool CPDF_DIB::SkipToScanline(int line, PauseIndicatorIface* pPause) const {
  return m_pDecoder && m_pDecoder->SkipToScanline(line, pPause);
}

size_t CPDF_DIB::GetEstimatedImageMemoryBurden() const {
  return m_pCachedBitmap ? m_pCachedBitmap->GetEstimatedImageMemoryBurden() : 0;
}

bool CPDF_DIB::TransMask() const {
  return m_bLoadMask && m_GroupFamily == CPDF_ColorSpace::Family::kDeviceCMYK &&
         m_Family == CPDF_ColorSpace::Family::kDeviceCMYK;
}

void CPDF_DIB::SetMaskProperties() {
  m_bpc = 1;
  m_nComponents = 1;
  SetFormat(FXDIB_Format::k1bppMask);
}

uint32_t CPDF_DIB::Get1BitSetValue() const {
  if (m_CompData[0].m_ColorKeyMax == 1)
    return 0x00000000;
  return HasPalette() ? GetPaletteSpan()[1] : 0xFFFFFFFF;
}

uint32_t CPDF_DIB::Get1BitResetValue() const {
  if (m_CompData[0].m_ColorKeyMin == 0)
    return 0x00000000;
  return HasPalette() ? GetPaletteSpan()[0] : 0xFF000000;
}
