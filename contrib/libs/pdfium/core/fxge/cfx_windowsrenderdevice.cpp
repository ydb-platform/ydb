// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_windowsrenderdevice.h"

#include <memory>

#include "core/fxge/renderdevicedriver_iface.h"
#include "core/fxge/win32/cgdi_display_driver.h"
#include "core/fxge/win32/cgdi_printer_driver.h"
#include "core/fxge/win32/cps_printer_driver.h"
#include "core/fxge/win32/ctext_only_printer_driver.h"

namespace {

std::unique_ptr<RenderDeviceDriverIface> CreateDriver(
    HDC hDC,
    CFX_PSFontTracker* ps_font_tracker,
    const EncoderIface* encoder_iface) {
  int device_type = ::GetDeviceCaps(hDC, TECHNOLOGY);
  int obj_type = ::GetObjectType(hDC);
  bool use_printer = device_type == DT_RASPRINTER ||
                     device_type == DT_PLOTTER ||
                     device_type == DT_CHARSTREAM || obj_type == OBJ_ENHMETADC;

  if (!use_printer)
    return std::make_unique<CGdiDisplayDriver>(hDC);

  if (g_pdfium_print_mode == WindowsPrintMode::kEmf ||
      g_pdfium_print_mode == WindowsPrintMode::kEmfImageMasks) {
    return std::make_unique<CGdiPrinterDriver>(hDC);
  }

  if (g_pdfium_print_mode == WindowsPrintMode::kTextOnly)
    return std::make_unique<CTextOnlyPrinterDriver>(hDC);

  return std::make_unique<CPSPrinterDriver>(hDC, g_pdfium_print_mode,
                                            ps_font_tracker, encoder_iface);
}

}  // namespace

WindowsPrintMode g_pdfium_print_mode = WindowsPrintMode::kEmf;

CFX_WindowsRenderDevice::CFX_WindowsRenderDevice(
    HDC hDC,
    CFX_PSFontTracker* ps_font_tracker,
    const EncoderIface* encoder_iface) {
  SetDeviceDriver(CreateDriver(hDC, ps_font_tracker, encoder_iface));
}

CFX_WindowsRenderDevice::~CFX_WindowsRenderDevice() = default;
