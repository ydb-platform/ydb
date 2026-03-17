// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/render/cpdf_windowsrenderdevice.h"

#include <sstream>

#include "core/fxcodec/basic/basicmodule.h"
#include "core/fxcodec/fax/faxmodule.h"
#include "core/fxcodec/flate/flatemodule.h"
#include "core/fxcodec/jpeg/jpegmodule.h"
#include "core/fxge/win32/cfx_psrenderer.h"

namespace {

constexpr EncoderIface kEncoderIface = {
    BasicModule::A85Encode, FaxModule::FaxEncode, FlateModule::Encode,
    JpegModule::JpegEncode, BasicModule::RunLengthEncode};

}  // namespace

CPDF_WindowsRenderDevice::CPDF_WindowsRenderDevice(
    HDC hDC,
    CFX_PSFontTracker* ps_font_tracker)
    : CFX_WindowsRenderDevice(hDC, ps_font_tracker, &kEncoderIface) {}

CPDF_WindowsRenderDevice::~CPDF_WindowsRenderDevice() = default;
