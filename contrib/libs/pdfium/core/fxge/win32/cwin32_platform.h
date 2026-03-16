// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_WIN32_CWIN32_PLATFORM_H_
#define CORE_FXGE_WIN32_CWIN32_PLATFORM_H_

#include <memory>

#include "core/fxge/cfx_gemodule.h"
#include "core/fxge/win32/cgdi_plus_ext.h"

class CWin32Platform final : public CFX_GEModule::PlatformIface {
 public:
  CWin32Platform();
  ~CWin32Platform() override;

  // CFX_GEModule::PlatformIface:
  void Init() override;
  std::unique_ptr<SystemFontInfoIface> CreateDefaultSystemFontInfo() override;

  CGdiplusExt m_GdiplusExt;
};

#endif  // CORE_FXGE_WIN32_CWIN32_PLATFORM_H_
