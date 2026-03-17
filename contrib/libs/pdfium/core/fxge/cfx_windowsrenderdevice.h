// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_CFX_WINDOWSRENDERDEVICE_H_
#define CORE_FXGE_CFX_WINDOWSRENDERDEVICE_H_

#include <windows.h>

#include "core/fxge/cfx_renderdevice.h"

enum class WindowsPrintMode {
  kEmf = 0,
  kTextOnly = 1,
  kPostScript2 = 2,
  kPostScript3 = 3,
  kPostScript2PassThrough = 4,
  kPostScript3PassThrough = 5,
  kEmfImageMasks = 6,
  kPostScript3Type42 = 7,
  kPostScript3Type42PassThrough = 8,
};

class CFX_PSFontTracker;
struct EncoderIface;

extern WindowsPrintMode g_pdfium_print_mode;

class CFX_WindowsRenderDevice : public CFX_RenderDevice {
 public:
  CFX_WindowsRenderDevice(HDC hDC,
                          CFX_PSFontTracker* ps_font_tracker,
                          const EncoderIface* encoder_iface);
  ~CFX_WindowsRenderDevice() override;
};

#endif  // CORE_FXGE_CFX_WINDOWSRENDERDEVICE_H_
