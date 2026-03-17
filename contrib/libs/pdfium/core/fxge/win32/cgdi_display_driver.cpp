// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/win32/cgdi_display_driver.h"

#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxge/agg/cfx_agg_imagerenderer.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/render_defines.h"
#include "core/fxge/win32/cwin32_platform.h"

CGdiDisplayDriver::CGdiDisplayDriver(HDC hDC)
    : CGdiDeviceDriver(hDC, DeviceType::kDisplay) {
  auto* pPlatform =
      static_cast<CWin32Platform*>(CFX_GEModule::Get()->GetPlatform());
  if (pPlatform->m_GdiplusExt.IsAvailable()) {
    m_RenderCaps |= FXRC_ALPHA_PATH | FXRC_ALPHA_IMAGE;
  }
}

CGdiDisplayDriver::~CGdiDisplayDriver() = default;

int CGdiDisplayDriver::GetDeviceCaps(int caps_id) const {
  if (caps_id == FXDC_HORZ_SIZE || caps_id == FXDC_VERT_SIZE)
    return 0;
  return CGdiDeviceDriver::GetDeviceCaps(caps_id);
}

bool CGdiDisplayDriver::GetDIBits(RetainPtr<CFX_DIBitmap> bitmap,
                                  int left,
                                  int top) const {
  bool ret = false;
  int width = bitmap->GetWidth();
  int height = bitmap->GetHeight();
  HBITMAP hbmp = CreateCompatibleBitmap(m_hDC, width, height);
  HDC hDCMemory = CreateCompatibleDC(m_hDC);
  HBITMAP holdbmp = (HBITMAP)SelectObject(hDCMemory, hbmp);
  BitBlt(hDCMemory, 0, 0, width, height, m_hDC, left, top, SRCCOPY);
  SelectObject(hDCMemory, holdbmp);
  BITMAPINFO bmi = {};
  bmi.bmiHeader.biSize = sizeof bmi.bmiHeader;
  bmi.bmiHeader.biBitCount = bitmap->GetBPP();
  bmi.bmiHeader.biHeight = -height;
  bmi.bmiHeader.biPlanes = 1;
  bmi.bmiHeader.biWidth = width;
  if (bitmap->GetBPP() > 8) {
    ret = ::GetDIBits(hDCMemory, hbmp, 0, height,
                      bitmap->GetWritableBuffer().data(), &bmi,
                      DIB_RGB_COLORS) == height;
    if (ret && bitmap->IsAlphaFormat()) {
      bitmap->SetUniformOpaqueAlpha();
    }
  } else {
    auto rgb_bitmap = pdfium::MakeRetain<CFX_DIBitmap>();
    if (rgb_bitmap->Create(width, height, FXDIB_Format::kBgr)) {
      bmi.bmiHeader.biBitCount = 24;
      ::GetDIBits(hDCMemory, hbmp, 0, height,
                  rgb_bitmap->GetWritableBuffer().data(), &bmi, DIB_RGB_COLORS);
      ret = bitmap->TransferBitmap(width, height, std::move(rgb_bitmap), 0, 0);
    } else {
      ret = false;
    }
  }

  DeleteObject(hbmp);
  DeleteObject(hDCMemory);
  return ret;
}

bool CGdiDisplayDriver::SetDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                  uint32_t color,
                                  const FX_RECT& src_rect,
                                  int left,
                                  int top,
                                  BlendMode blend_type) {
  DCHECK_EQ(blend_type, BlendMode::kNormal);
  if (bitmap->IsMaskFormat()) {
    int width = bitmap->GetWidth();
    int height = bitmap->GetHeight();
    int alpha = FXARGB_A(color);
    if (bitmap->GetBPP() != 1 || alpha != 255) {
      auto background = pdfium::MakeRetain<CFX_DIBitmap>();
      if (!background->Create(width, height, FXDIB_Format::kBgrx) ||
          !GetDIBits(background, left, top) ||
          !background->CompositeMask(0, 0, width, height, std::move(bitmap),
                                     color, 0, 0, BlendMode::kNormal, nullptr,
                                     false)) {
        return false;
      }
      FX_RECT alpha_src_rect(0, 0, width, height);
      return SetDIBits(std::move(background), /*color=*/0, alpha_src_rect, left,
                       top, BlendMode::kNormal);
    }
    FX_RECT clip_rect(left, top, left + src_rect.Width(),
                      top + src_rect.Height());
    return StretchDIBits(std::move(bitmap), color, left - src_rect.left,
                         top - src_rect.top, width, height, &clip_rect,
                         FXDIB_ResampleOptions(), BlendMode::kNormal);
  }
  int width = src_rect.Width();
  int height = src_rect.Height();
  if (bitmap->IsAlphaFormat()) {
    auto rgb_bitmap = pdfium::MakeRetain<CFX_DIBitmap>();
    if (!rgb_bitmap->Create(width, height, FXDIB_Format::kBgr) ||
        !GetDIBits(rgb_bitmap, left, top) ||
        !rgb_bitmap->CompositeBitmap(0, 0, width, height, std::move(bitmap),
                                     src_rect.left, src_rect.top,
                                     BlendMode::kNormal, nullptr, false)) {
      return false;
    }
    FX_RECT alpha_src_rect(0, 0, width, height);
    return SetDIBits(std::move(rgb_bitmap), /*color=*/0, alpha_src_rect, left,
                     top, BlendMode::kNormal);
  }
  return GDI_SetDIBits(std::move(bitmap), src_rect, left, top);
}

bool CGdiDisplayDriver::UseFoxitStretchEngine(
    RetainPtr<const CFX_DIBBase> bitmap,
    uint32_t color,
    int dest_left,
    int dest_top,
    int dest_width,
    int dest_height,
    const FX_RECT* pClipRect,
    const FXDIB_ResampleOptions& options) {
  FX_RECT bitmap_clip = *pClipRect;
  if (dest_width < 0)
    dest_left += dest_width;

  if (dest_height < 0)
    dest_top += dest_height;

  bitmap_clip.Offset(-dest_left, -dest_top);
  bitmap = bitmap->StretchTo(dest_width, dest_height, options, &bitmap_clip);
  if (!bitmap) {
    return true;
  }

  FX_RECT src_rect(0, 0, bitmap->GetWidth(), bitmap->GetHeight());
  return SetDIBits(std::move(bitmap), color, src_rect, pClipRect->left,
                   pClipRect->top, BlendMode::kNormal);
}

bool CGdiDisplayDriver::StretchDIBits(RetainPtr<const CFX_DIBBase> bitmap,
                                      uint32_t color,
                                      int dest_left,
                                      int dest_top,
                                      int dest_width,
                                      int dest_height,
                                      const FX_RECT* pClipRect,
                                      const FXDIB_ResampleOptions& options,
                                      BlendMode blend_type) {
  DCHECK(bitmap);
  DCHECK(pClipRect);

  if (options.HasAnyOptions() || dest_width > 10000 || dest_width < -10000 ||
      dest_height > 10000 || dest_height < -10000) {
    return UseFoxitStretchEngine(std::move(bitmap), color, dest_left, dest_top,
                                 dest_width, dest_height, pClipRect, options);
  }
  if (bitmap->IsMaskFormat()) {
    FX_RECT image_rect;
    image_rect.left = dest_width > 0 ? dest_left : dest_left + dest_width;
    image_rect.right = dest_width > 0 ? dest_left + dest_width : dest_left;
    image_rect.top = dest_height > 0 ? dest_top : dest_top + dest_height;
    image_rect.bottom = dest_height > 0 ? dest_top + dest_height : dest_top;
    FX_RECT clip_rect = image_rect;
    clip_rect.Intersect(*pClipRect);
    clip_rect.Offset(-image_rect.left, -image_rect.top);
    int clip_width = clip_rect.Width(), clip_height = clip_rect.Height();
    bitmap = bitmap->StretchTo(dest_width, dest_height, FXDIB_ResampleOptions(),
                               &clip_rect);
    if (!bitmap) {
      return true;
    }

    auto background = pdfium::MakeRetain<CFX_DIBitmap>();
    if (!background->Create(clip_width, clip_height, FXDIB_Format::kBgrx) ||
        !GetDIBits(background, image_rect.left + clip_rect.left,
                   image_rect.top + clip_rect.top) ||
        !background->CompositeMask(0, 0, clip_width, clip_height,
                                   std::move(bitmap), color, 0, 0,
                                   BlendMode::kNormal, nullptr, false)) {
      return false;
    }

    FX_RECT src_rect(0, 0, clip_width, clip_height);
    return SetDIBits(background, 0, src_rect, image_rect.left + clip_rect.left,
                     image_rect.top + clip_rect.top, BlendMode::kNormal);
  }
  if (bitmap->IsAlphaFormat()) {
    auto* pPlatform =
        static_cast<CWin32Platform*>(CFX_GEModule::Get()->GetPlatform());
    if (pPlatform->m_GdiplusExt.IsAvailable()) {
      return pPlatform->m_GdiplusExt.StretchDIBits(m_hDC, std::move(bitmap),
                                                   dest_left, dest_top,
                                                   dest_width, dest_height);
    }
    return UseFoxitStretchEngine(std::move(bitmap), color, dest_left, dest_top,
                                 dest_width, dest_height, pClipRect,
                                 FXDIB_ResampleOptions());
  }
  return GDI_StretchDIBits(std::move(bitmap), dest_left, dest_top, dest_width,
                           dest_height, FXDIB_ResampleOptions());
}

RenderDeviceDriverIface::StartResult CGdiDisplayDriver::StartDIBits(
    RetainPtr<const CFX_DIBBase> bitmap,
    float alpha,
    uint32_t color,
    const CFX_Matrix& matrix,
    const FXDIB_ResampleOptions& options,
    BlendMode blend_type) {
  return {Result::kNotSupported, nullptr};
}
