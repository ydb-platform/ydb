// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_APPLE_FX_APPLE_PLATFORM_H_
#define CORE_FXGE_APPLE_FX_APPLE_PLATFORM_H_

#include <memory>

#include "core/fxge/apple/fx_quartz_device.h"
#include "core/fxge/cfx_gemodule.h"

class CApplePlatform final : public CFX_GEModule::PlatformIface {
 public:
  CApplePlatform();
  ~CApplePlatform() override;

  // CFX_GEModule::PlatformIface:
  void Init() override;
  std::unique_ptr<SystemFontInfoIface> CreateDefaultSystemFontInfo() override;
  void* CreatePlatformFont(pdfium::span<const uint8_t> font_span) override;

  CQuartz2D m_quartz2d;
};

#endif  // CORE_FXGE_APPLE_FX_APPLE_PLATFORM_H_
