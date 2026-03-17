// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_ANDROID_CFPF_SKIAFONTMGR_H_
#define CORE_FXGE_ANDROID_CFPF_SKIAFONTMGR_H_

#include <map>
#include <memory>
#include <vector>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_codepage_forward.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxge/cfx_face.h"
#include "core/fxge/freetype/fx_freetype.h"

class CFPF_SkiaFont;
class CFPF_SkiaPathFont;

class CFPF_SkiaFontMgr {
 public:
  CFPF_SkiaFontMgr();
  ~CFPF_SkiaFontMgr();

  void LoadFonts(const char** user_paths);
  CFPF_SkiaFont* CreateFont(ByteStringView family_name,
                            FX_Charset charset,
                            uint32_t style);

  bool InitFTLibrary();
  RetainPtr<CFX_Face> GetFontFace(ByteStringView path, int32_t face_index);

 private:
  void ScanPath(const ByteString& path);
  void ScanFile(const ByteString& file);
  std::unique_ptr<CFPF_SkiaPathFont> ReportFace(RetainPtr<CFX_Face> face,
                                                const ByteString& file);

  bool loaded_fonts_ = false;
  ScopedFXFTLibraryRec ft_library_;
  std::vector<std::unique_ptr<CFPF_SkiaPathFont>> font_faces_;
  // Key is a hash based on CreateFont() parameters.
  std::map<uint32_t, std::unique_ptr<CFPF_SkiaFont>> family_font_map_;
};

#endif  // CORE_FXGE_ANDROID_CFPF_SKIAFONTMGR_H_
