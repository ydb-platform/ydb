// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_SYSTEMFONTINFO_IFACE_H_
#define CORE_FXGE_SYSTEMFONTINFO_IFACE_H_

#include <stddef.h>
#include <stdint.h>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_codepage_forward.h"
#include "core/fxcrt/span.h"
#include "core/fxge/cfx_fontmapper.h"

constexpr uint32_t kTableNAME = CFX_FontMapper::MakeTag('n', 'a', 'm', 'e');
constexpr uint32_t kTableTTCF = CFX_FontMapper::MakeTag('t', 't', 'c', 'f');

class SystemFontInfoIface {
 public:
  virtual ~SystemFontInfoIface() = default;

  virtual bool EnumFontList(CFX_FontMapper* pMapper) = 0;
  virtual void* MapFont(int weight,
                        bool bItalic,
                        FX_Charset charset,
                        int pitch_family,
                        const ByteString& face) = 0;
  virtual void* GetFont(const ByteString& face) = 0;
  virtual size_t GetFontData(void* hFont,
                             uint32_t table,
                             pdfium::span<uint8_t> buffer) = 0;
  virtual bool GetFaceName(void* hFont, ByteString* name) = 0;
  virtual bool GetFontCharset(void* hFont, FX_Charset* charset) = 0;
  virtual void DeleteFont(void* hFont) = 0;
};

#endif  // CORE_FXGE_SYSTEMFONTINFO_IFACE_H_
