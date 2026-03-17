// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_GEMODULE_H_
#define CORE_FXGE_CFX_GEMODULE_H_

#include <stdint.h>

#include <memory>

#include "build/build_config.h"
#include "core/fxcrt/unowned_ptr_exclusion.h"

#if BUILDFLAG(IS_APPLE)
#include "core/fxcrt/span.h"
#endif

class CFX_FontCache;
class CFX_FontMgr;
class SystemFontInfoIface;

class CFX_GEModule {
 public:
  class PlatformIface {
   public:
    static std::unique_ptr<PlatformIface> Create();
    virtual ~PlatformIface() = default;

    virtual void Init() = 0;
    virtual std::unique_ptr<SystemFontInfoIface>
    CreateDefaultSystemFontInfo() = 0;
#if BUILDFLAG(IS_APPLE)
    virtual void* CreatePlatformFont(pdfium::span<const uint8_t> font_span) = 0;
#endif
  };

  static void Create(const char** pUserFontPaths);
  static void Destroy();
  static CFX_GEModule* Get();

  CFX_FontCache* GetFontCache() const { return m_pFontCache.get(); }
  CFX_FontMgr* GetFontMgr() const { return m_pFontMgr.get(); }
  PlatformIface* GetPlatform() const { return m_pPlatform.get(); }
  const char** GetUserFontPaths() const { return m_pUserFontPaths; }

 private:
  explicit CFX_GEModule(const char** pUserFontPaths);
  ~CFX_GEModule();

  std::unique_ptr<PlatformIface> const m_pPlatform;
  std::unique_ptr<CFX_FontMgr> const m_pFontMgr;
  std::unique_ptr<CFX_FontCache> const m_pFontCache;

  // Exclude because taken from public API.
  UNOWNED_PTR_EXCLUSION const char** const m_pUserFontPaths;
};

#endif  // CORE_FXGE_CFX_GEMODULE_H_
