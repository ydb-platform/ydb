// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_ANDROID_CFX_ANDROIDFONTINFO_H_
#define CORE_FXGE_ANDROID_CFX_ANDROIDFONTINFO_H_

#include <stdint.h>

#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/cfx_fontmapper.h"
#include "core/fxge/systemfontinfo_iface.h"

class CFPF_SkiaFontMgr;

class CFX_AndroidFontInfo final : public SystemFontInfoIface {
 public:
  CFX_AndroidFontInfo();
  ~CFX_AndroidFontInfo() override;

  bool Init(CFPF_SkiaFontMgr* pFontMgr, const char** user_paths);

  // SystemFontInfoIface:
  bool EnumFontList(CFX_FontMapper* pMapper) override;
  void* MapFont(int weight,
                bool bItalic,
                FX_Charset charset,
                int pitch_family,
                const ByteString& face) override;
  void* GetFont(const ByteString& face) override;
  size_t GetFontData(void* hFont,
                     uint32_t table,
                     pdfium::span<uint8_t> buffer) override;
  bool GetFaceName(void* hFont, ByteString* name) override;
  bool GetFontCharset(void* hFont, FX_Charset* charset) override;
  void DeleteFont(void* hFont) override;

 private:
  UnownedPtr<CFPF_SkiaFontMgr> m_pFontMgr;
};

#endif  // CORE_FXGE_ANDROID_CFX_ANDROIDFONTINFO_H_
