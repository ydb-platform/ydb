// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "public/fpdf_sysfontinfo.h"

#include <stddef.h>

#include <memory>
#include <utility>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_codepage.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/cfx_font.h"
#include "core/fxge/cfx_fontmapper.h"
#include "core/fxge/cfx_fontmgr.h"
#include "core/fxge/cfx_gemodule.h"
#include "core/fxge/fx_font.h"
#include "core/fxge/systemfontinfo_iface.h"

#ifdef PDF_ENABLE_XFA
#include "xfa/fgas/font/cfgas_fontmgr.h"
#include "xfa/fgas/font/cfgas_gemodule.h"
#endif

static_assert(FXFONT_ANSI_CHARSET == static_cast<int>(FX_Charset::kANSI),
              "Charset must match");
static_assert(FXFONT_DEFAULT_CHARSET == static_cast<int>(FX_Charset::kDefault),
              "Charset must match");
static_assert(FXFONT_SYMBOL_CHARSET == static_cast<int>(FX_Charset::kSymbol),
              "Charset must match");
static_assert(FXFONT_SHIFTJIS_CHARSET ==
                  static_cast<int>(FX_Charset::kShiftJIS),
              "Charset must match");
static_assert(FXFONT_HANGEUL_CHARSET == static_cast<int>(FX_Charset::kHangul),
              "Charset must match");
static_assert(FXFONT_GB2312_CHARSET ==
                  static_cast<int>(FX_Charset::kChineseSimplified),
              "Charset must match");
static_assert(FXFONT_CHINESEBIG5_CHARSET ==
                  static_cast<int>(FX_Charset::kChineseTraditional),
              "Charset must match");
static_assert(FXFONT_GREEK_CHARSET ==
                  static_cast<int>(FX_Charset::kMSWin_Greek),
              "Charset must match");
static_assert(FXFONT_VIETNAMESE_CHARSET ==
                  static_cast<int>(FX_Charset::kMSWin_Vietnamese),
              "Charset must match");
static_assert(FXFONT_HEBREW_CHARSET ==
                  static_cast<int>(FX_Charset::kMSWin_Hebrew),
              "Charset must match");
static_assert(FXFONT_ARABIC_CHARSET ==
                  static_cast<int>(FX_Charset::kMSWin_Arabic),
              "Charset must match");
static_assert(FXFONT_CYRILLIC_CHARSET ==
                  static_cast<int>(FX_Charset::kMSWin_Cyrillic),
              "Charset must match");
static_assert(FXFONT_THAI_CHARSET == static_cast<int>(FX_Charset::kThai),
              "Charset must match");
static_assert(FXFONT_EASTERNEUROPEAN_CHARSET ==
                  static_cast<int>(FX_Charset::kMSWin_EasternEuropean),
              "Charset must match");
static_assert(offsetof(CFX_Font::CharsetFontMap, charset) ==
                  offsetof(FPDF_CharsetFontMap, charset),
              "CFX_Font::CharsetFontMap must be same as FPDF_CharsetFontMap");
static_assert(offsetof(CFX_Font::CharsetFontMap, fontname) ==
                  offsetof(FPDF_CharsetFontMap, fontname),
              "CFX_Font::CharsetFontMap must be same as FPDF_CharsetFontMap");
static_assert(sizeof(CFX_Font::CharsetFontMap) == sizeof(FPDF_CharsetFontMap),
              "CFX_Font::CharsetFontMap must be same as FPDF_CharsetFontMap");

class CFX_ExternalFontInfo final : public SystemFontInfoIface {
 public:
  explicit CFX_ExternalFontInfo(FPDF_SYSFONTINFO* pInfo) : m_pInfo(pInfo) {}
  ~CFX_ExternalFontInfo() override {
    if (m_pInfo->Release)
      m_pInfo->Release(m_pInfo);
  }

  bool EnumFontList(CFX_FontMapper* pMapper) override {
    if (m_pInfo->EnumFonts) {
      m_pInfo->EnumFonts(m_pInfo, pMapper);
      return true;
    }
    return false;
  }

  void* MapFont(int weight,
                bool bItalic,
                FX_Charset charset,
                int pitch_family,
                const ByteString& face) override {
    if (!m_pInfo->MapFont)
      return nullptr;

    int iExact;
    return m_pInfo->MapFont(m_pInfo, weight, bItalic, static_cast<int>(charset),
                            pitch_family, face.c_str(), &iExact);
  }

  void* GetFont(const ByteString& family) override {
    if (!m_pInfo->GetFont)
      return nullptr;
    return m_pInfo->GetFont(m_pInfo, family.c_str());
  }

  size_t GetFontData(void* hFont,
                     uint32_t table,
                     pdfium::span<uint8_t> buffer) override {
    if (!m_pInfo->GetFontData)
      return 0;
    return m_pInfo->GetFontData(m_pInfo, hFont, table, buffer.data(),
                                fxcrt::CollectionSize<unsigned long>(buffer));
  }

  bool GetFaceName(void* hFont, ByteString* name) override {
    if (!m_pInfo->GetFaceName)
      return false;
    unsigned long size = m_pInfo->GetFaceName(m_pInfo, hFont, nullptr, 0);
    if (size == 0)
      return false;
    ByteString result;
    auto result_span = result.GetBuffer(size);
    size = m_pInfo->GetFaceName(m_pInfo, hFont, result_span.data(), size);
    result.ReleaseBuffer(size);
    *name = std::move(result);
    return true;
  }

  bool GetFontCharset(void* hFont, FX_Charset* charset) override {
    if (!m_pInfo->GetFontCharset)
      return false;

    *charset = FX_GetCharsetFromInt(m_pInfo->GetFontCharset(m_pInfo, hFont));
    return true;
  }

  void DeleteFont(void* hFont) override {
    if (m_pInfo->DeleteFont)
      m_pInfo->DeleteFont(m_pInfo, hFont);
  }

 private:
  UnownedPtr<FPDF_SYSFONTINFO> const m_pInfo;
};

FPDF_EXPORT void FPDF_CALLCONV FPDF_AddInstalledFont(void* mapper,
                                                     const char* face,
                                                     int charset) {
  CFX_FontMapper* pMapper = static_cast<CFX_FontMapper*>(mapper);
  pMapper->AddInstalledFont(face, FX_GetCharsetFromInt(charset));
}

FPDF_EXPORT void FPDF_CALLCONV
FPDF_SetSystemFontInfo(FPDF_SYSFONTINFO* pFontInfoExt) {
  auto* mapper = CFX_GEModule::Get()->GetFontMgr()->GetBuiltinMapper();
  if (!pFontInfoExt) {
    std::unique_ptr<SystemFontInfoIface> info = mapper->TakeSystemFontInfo();
    // Delete `info` when it goes out of scope here.
    return;
  }

  if (pFontInfoExt->version != 1)
    return;

  mapper->SetSystemFontInfo(
      std::make_unique<CFX_ExternalFontInfo>(pFontInfoExt));

#ifdef PDF_ENABLE_XFA
  CFGAS_GEModule::Get()->GetFontMgr()->EnumFonts();
#endif
}

FPDF_EXPORT const FPDF_CharsetFontMap* FPDF_CALLCONV FPDF_GetDefaultTTFMap() {
  return reinterpret_cast<const FPDF_CharsetFontMap*>(
      CFX_Font::GetDefaultTTFMapSpan().data());
}

FPDF_EXPORT size_t FPDF_CALLCONV FPDF_GetDefaultTTFMapCount() {
  return CFX_Font::GetDefaultTTFMapSpan().size();
}

FPDF_EXPORT const FPDF_CharsetFontMap* FPDF_CALLCONV
FPDF_GetDefaultTTFMapEntry(size_t index) {
  pdfium::span<const CFX_Font::CharsetFontMap> entries =
      CFX_Font::GetDefaultTTFMapSpan();
  return index < entries.size()
             ? reinterpret_cast<const FPDF_CharsetFontMap*>(&entries[index])
             : nullptr;
}

struct FPDF_SYSFONTINFO_DEFAULT final : public FPDF_SYSFONTINFO {
  UnownedPtr<SystemFontInfoIface> m_pFontInfo;
};

static void DefaultRelease(struct _FPDF_SYSFONTINFO* pThis) {
  auto* pDefault = static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pThis);
  pDefault->m_pFontInfo.ClearAndDelete();
}

static void DefaultEnumFonts(struct _FPDF_SYSFONTINFO* pThis, void* pMapper) {
  auto* pDefault = static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pThis);
  pDefault->m_pFontInfo->EnumFontList(static_cast<CFX_FontMapper*>(pMapper));
}

static void* DefaultMapFont(struct _FPDF_SYSFONTINFO* pThis,
                            int weight,
                            FPDF_BOOL use_italic,
                            int charset,
                            int pitch_family,
                            const char* family,
                            FPDF_BOOL* /*exact*/) {
  auto* pDefault = static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pThis);
  return pDefault->m_pFontInfo->MapFont(weight, !!use_italic,
                                        FX_GetCharsetFromInt(charset),
                                        pitch_family, family);
}

void* DefaultGetFont(struct _FPDF_SYSFONTINFO* pThis, const char* family) {
  auto* pDefault = static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pThis);
  return pDefault->m_pFontInfo->GetFont(family);
}

// TODO(tsepez): should be UNSAFE_BUFFER_USAGE.
static unsigned long DefaultGetFontData(struct _FPDF_SYSFONTINFO* pThis,
                                        void* hFont,
                                        unsigned int table,
                                        unsigned char* buffer,
                                        unsigned long buf_size) {
  auto* pDefault = static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pThis);
  // SAFETY: required from caller.
  return pdfium::checked_cast<unsigned long>(pDefault->m_pFontInfo->GetFontData(
      hFont, table, UNSAFE_BUFFERS(pdfium::make_span(buffer, buf_size))));
}

// TODO(tsepez): should be UNSAFE_BUFFER_USAGE.
static unsigned long DefaultGetFaceName(struct _FPDF_SYSFONTINFO* pThis,
                                        void* hFont,
                                        char* buffer,
                                        unsigned long buf_size) {
  ByteString name;
  auto* pDefault = static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pThis);
  if (!pDefault->m_pFontInfo->GetFaceName(hFont, &name))
    return 0;

  const unsigned long copy_length =
      pdfium::checked_cast<unsigned long>(name.GetLength() + 1);
  if (copy_length <= buf_size)
    strncpy(buffer, name.c_str(), copy_length * sizeof(ByteString::CharType));

  return copy_length;
}

static int DefaultGetFontCharset(struct _FPDF_SYSFONTINFO* pThis, void* hFont) {
  FX_Charset charset;
  auto* pDefault = static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pThis);
  if (!pDefault->m_pFontInfo->GetFontCharset(hFont, &charset))
    return 0;
  return static_cast<int>(charset);
}

static void DefaultDeleteFont(struct _FPDF_SYSFONTINFO* pThis, void* hFont) {
  auto* pDefault = static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pThis);
  pDefault->m_pFontInfo->DeleteFont(hFont);
}

FPDF_EXPORT FPDF_SYSFONTINFO* FPDF_CALLCONV FPDF_GetDefaultSystemFontInfo() {
  std::unique_ptr<SystemFontInfoIface> pFontInfo =
      CFX_GEModule::Get()->GetPlatform()->CreateDefaultSystemFontInfo();
  if (!pFontInfo)
    return nullptr;

  FPDF_SYSFONTINFO_DEFAULT* pFontInfoExt =
      FX_Alloc(FPDF_SYSFONTINFO_DEFAULT, 1);
  pFontInfoExt->DeleteFont = DefaultDeleteFont;
  pFontInfoExt->EnumFonts = DefaultEnumFonts;
  pFontInfoExt->GetFaceName = DefaultGetFaceName;
  pFontInfoExt->GetFont = DefaultGetFont;
  pFontInfoExt->GetFontCharset = DefaultGetFontCharset;
  pFontInfoExt->GetFontData = DefaultGetFontData;
  pFontInfoExt->MapFont = DefaultMapFont;
  pFontInfoExt->Release = DefaultRelease;
  pFontInfoExt->version = 1;
  pFontInfoExt->m_pFontInfo = pFontInfo.release();
  return pFontInfoExt;
}

FPDF_EXPORT void FPDF_CALLCONV
FPDF_FreeDefaultSystemFontInfo(FPDF_SYSFONTINFO* pFontInfo) {
  FX_Free(static_cast<FPDF_SYSFONTINFO_DEFAULT*>(pFontInfo));
}
