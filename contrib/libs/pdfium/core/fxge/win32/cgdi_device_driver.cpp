// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/win32/cgdi_device_driver.h"

#include <math.h>
#include <windows.h>

#include <algorithm>
#include <array>
#include <vector>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxge/agg/cfx_agg_devicedriver.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/render_defines.h"
#include "core/fxge/win32/cwin32_platform.h"
#include "third_party/agg23/agg_clip_liang_barsky.h"

namespace {

constexpr int FillTypeToGdiFillType(CFX_FillRenderOptions::FillType fill_type) {
  return static_cast<int>(fill_type);
}

static_assert(FillTypeToGdiFillType(
                  CFX_FillRenderOptions::FillType::kEvenOdd) == ALTERNATE,
              "CFX_FillRenderOptions::FillType::kEvenOdd value mismatch");

static_assert(
    FillTypeToGdiFillType(CFX_FillRenderOptions::FillType::kWinding) == WINDING,
    "CFX_FillRenderOptions::FillType::kWinding value mismatch");

HPEN CreateExtPen(const CFX_GraphStateData* pGraphState,
                  const CFX_Matrix* pMatrix,
                  uint32_t argb) {
  DCHECK(pGraphState);

  float scale = 1.0f;
  if (pMatrix) {
    scale = fabs(pMatrix->a) > fabs(pMatrix->b) ? fabs(pMatrix->a)
                                                : fabs(pMatrix->b);
  }
  float width = std::max(scale * pGraphState->m_LineWidth, 1.0f);

  uint32_t PenStyle = PS_GEOMETRIC;
  if (!pGraphState->m_DashArray.empty())
    PenStyle |= PS_USERSTYLE;
  else
    PenStyle |= PS_SOLID;

  switch (pGraphState->m_LineCap) {
    case CFX_GraphStateData::LineCap::kButt:
      PenStyle |= PS_ENDCAP_FLAT;
      break;
    case CFX_GraphStateData::LineCap::kRound:
      PenStyle |= PS_ENDCAP_ROUND;
      break;
    case CFX_GraphStateData::LineCap::kSquare:
      PenStyle |= PS_ENDCAP_SQUARE;
      break;
  }
  switch (pGraphState->m_LineJoin) {
    case CFX_GraphStateData::LineJoin::kMiter:
      PenStyle |= PS_JOIN_MITER;
      break;
    case CFX_GraphStateData::LineJoin::kRound:
      PenStyle |= PS_JOIN_ROUND;
      break;
    case CFX_GraphStateData::LineJoin::kBevel:
      PenStyle |= PS_JOIN_BEVEL;
      break;
  }

  FX_COLORREF colorref = ArgbToColorRef(argb);
  LOGBRUSH lb;
  lb.lbColor = colorref;
  lb.lbStyle = BS_SOLID;
  lb.lbHatch = 0;
  std::vector<uint32_t> dashes;
  if (!pGraphState->m_DashArray.empty()) {
    dashes.resize(pGraphState->m_DashArray.size());
    for (size_t i = 0; i < pGraphState->m_DashArray.size(); i++) {
      dashes[i] = FXSYS_roundf(
          pMatrix ? pMatrix->TransformDistance(pGraphState->m_DashArray[i])
                  : pGraphState->m_DashArray[i]);
      dashes[i] = std::max(dashes[i], 1U);
    }
  }
  return ExtCreatePen(
      PenStyle, (DWORD)ceil(width), &lb,
      pdfium::checked_cast<DWORD>(pGraphState->m_DashArray.size()),
      reinterpret_cast<const DWORD*>(dashes.data()));
}

HBRUSH CreateBrush(uint32_t argb) {
  return CreateSolidBrush(ArgbToColorRef(argb));
}

void SetPathToDC(HDC hDC, const CFX_Path& path, const CFX_Matrix* pMatrix) {
  BeginPath(hDC);

  pdfium::span<const CFX_Path::Point> points = path.GetPoints();
  for (size_t i = 0; i < points.size(); ++i) {
    CFX_PointF pos = points[i].m_Point;
    if (pMatrix)
      pos = pMatrix->Transform(pos);

    CFX_Point screen(FXSYS_roundf(pos.x), FXSYS_roundf(pos.y));
    CFX_Path::Point::Type point_type = points[i].m_Type;
    if (point_type == CFX_Path::Point::Type::kMove) {
      MoveToEx(hDC, screen.x, screen.y, nullptr);
    } else if (point_type == CFX_Path::Point::Type::kLine) {
      if (points[i].m_Point == points[i - 1].m_Point)
        screen.x++;

      LineTo(hDC, screen.x, screen.y);
    } else if (point_type == CFX_Path::Point::Type::kBezier) {
      POINT lppt[3];
      lppt[0].x = screen.x;
      lppt[0].y = screen.y;

      pos = points[i + 1].m_Point;
      if (pMatrix)
        pos = pMatrix->Transform(pos);

      lppt[1].x = FXSYS_roundf(pos.x);
      lppt[1].y = FXSYS_roundf(pos.y);

      pos = points[i + 2].m_Point;
      if (pMatrix)
        pos = pMatrix->Transform(pos);

      lppt[2].x = FXSYS_roundf(pos.x);
      lppt[2].y = FXSYS_roundf(pos.y);
      PolyBezierTo(hDC, lppt, 3);
      i += 2;
    }
    if (points[i].m_CloseFigure)
      CloseFigure(hDC);
  }
  EndPath(hDC);
}

FixedSizeDataVector<uint8_t> GetBitmapInfoHeader(
    const RetainPtr<const CFX_DIBBase>& source) {
  size_t len = sizeof(BITMAPINFOHEADER);
  if (source->GetBPP() == 1 || source->GetBPP() == 8) {
    len += sizeof(DWORD) * (int)(1 << source->GetBPP());
  }

  auto result = FixedSizeDataVector<uint8_t>::Zeroed(len);
  auto* pbmih = reinterpret_cast<BITMAPINFOHEADER*>(result.span().data());
  pbmih->biSize = sizeof(BITMAPINFOHEADER);
  pbmih->biBitCount = source->GetBPP();
  pbmih->biCompression = BI_RGB;
  pbmih->biHeight = -(int)source->GetHeight();
  pbmih->biPlanes = 1;
  pbmih->biWidth = source->GetWidth();
  UNSAFE_TODO({
    if (source->GetBPP() == 8) {
      uint32_t* palette = (uint32_t*)(pbmih + 1);
      if (source->HasPalette()) {
        pdfium::span<const uint32_t> palette_span = source->GetPaletteSpan();
        for (int i = 0; i < 256; i++) {
          palette[i] = palette_span[i];
        }
      } else {
        for (int i = 0; i < 256; i++) {
          palette[i] = ArgbEncode(0, i, i, i);
        }
      }
    }
    if (source->GetBPP() == 1) {
      uint32_t* palette = (uint32_t*)(pbmih + 1);
      if (source->HasPalette()) {
        pdfium::span<const uint32_t> palette_span = source->GetPaletteSpan();
        palette[0] = palette_span[0];
        palette[1] = palette_span[1];
      } else {
        palette[0] = 0;
        palette[1] = 0xffffff;
      }
    }
  });
  return result;
}

#if defined(PDF_USE_SKIA)
// TODO(caryclark)  This antigrain function is duplicated here to permit
// removing the last remaining dependency. Eventually, this will be elminiated
// altogether and replace by Skia code.

struct rect_base {
  float x1;
  float y1;
  float x2;
  float y2;
};

unsigned clip_liang_barsky(float x1,
                           float y1,
                           float x2,
                           float y2,
                           const rect_base& clip_box,
                           float* x,
                           float* y) {
  const float nearzero = 1e-30f;
  float deltax = x2 - x1;
  float deltay = y2 - y1;
  unsigned np = 0;
  if (deltax == 0)
    deltax = (x1 > clip_box.x1) ? -nearzero : nearzero;
  float xin;
  float xout;
  if (deltax > 0) {
    xin = clip_box.x1;
    xout = clip_box.x2;
  } else {
    xin = clip_box.x2;
    xout = clip_box.x1;
  }
  float tinx = (xin - x1) / deltax;
  if (deltay == 0)
    deltay = (y1 > clip_box.y1) ? -nearzero : nearzero;
  float yin;
  float yout;
  if (deltay > 0) {
    yin = clip_box.y1;
    yout = clip_box.y2;
  } else {
    yin = clip_box.y2;
    yout = clip_box.y1;
  }
  float tiny = (yin - y1) / deltay;
  float tin1;
  float tin2;
  if (tinx < tiny) {
    tin1 = tinx;
    tin2 = tiny;
  } else {
    tin1 = tiny;
    tin2 = tinx;
  }
  if (tin1 <= 1.0f) {
    if (0 < tin1) {
      UNSAFE_TODO({
        *x++ = xin;
        *y++ = yin;
      });
      ++np;
    }
    if (tin2 <= 1.0f) {
      float toutx = (xout - x1) / deltax;
      float touty = (yout - y1) / deltay;
      float tout1 = (toutx < touty) ? toutx : touty;
      if (tin2 > 0 || tout1 > 0) {
        if (tin2 <= tout1) {
          if (tin2 > 0) {
            if (tinx > tiny) {
              UNSAFE_TODO({
                *x++ = xin;
                *y++ = y1 + (deltay * tinx);
              });
            } else {
              UNSAFE_TODO({
                *x++ = x1 + (deltax * tiny);
                *y++ = yin;
              });
            }
            ++np;
          }
          if (tout1 < 1.0f) {
            if (toutx < touty) {
              UNSAFE_TODO({
                *x++ = xout;
                *y++ = y1 + (deltay * toutx);
              });
            } else {
              UNSAFE_TODO({
                *x++ = x1 + (deltax * touty);
                *y++ = yout;
              });
            }
          } else {
            UNSAFE_TODO({
              *x++ = x2;
              *y++ = y2;
            });
          }
          ++np;
        } else {
          if (tinx > tiny) {
            UNSAFE_TODO({
              *x++ = xin;
              *y++ = yout;
            });
          } else {
            UNSAFE_TODO({
              *x++ = xout;
              *y++ = yin;
            });
          }
          ++np;
        }
      }
    }
  }
  return np;
}
#endif  //  defined(PDF_USE_SKIA)

unsigned LineClip(float w,
                  float h,
                  float x1,
                  float y1,
                  float x2,
                  float y2,
                  float* x,
                  float* y) {
#if defined(PDF_USE_SKIA)
  if (CFX_DefaultRenderDevice::UseSkiaRenderer()) {
    // TODO(caryclark) temporary replacement of antigrain in line function to
    // permit removing antigrain altogether
    rect_base rect = {0.0f, 0.0f, w, h};
    return clip_liang_barsky(x1, y1, x2, y2, rect, x, y);
  }
#endif
  pdfium::agg::rect_base<float> rect(0.0f, 0.0f, w, h);
  return pdfium::agg::clip_liang_barsky<float>(x1, y1, x2, y2, rect, x, y);
}

}  // namespace

CGdiDeviceDriver::CGdiDeviceDriver(HDC hDC, DeviceType device_type)
    : m_hDC(hDC), m_DeviceType(device_type) {
  SetStretchBltMode(m_hDC, HALFTONE);
  DWORD obj_type = GetObjectType(m_hDC);
  m_bMetafileDCType = obj_type == OBJ_ENHMETADC || obj_type == OBJ_ENHMETAFILE;
  if (obj_type == OBJ_MEMDC) {
    HBITMAP hBitmap = CreateBitmap(1, 1, 1, 1, nullptr);
    hBitmap = (HBITMAP)SelectObject(m_hDC, hBitmap);
    BITMAP bitmap;
    GetObject(hBitmap, sizeof(bitmap), &bitmap);
    m_nBitsPerPixel = bitmap.bmBitsPixel;
    m_Width = bitmap.bmWidth;
    m_Height = abs(bitmap.bmHeight);
    hBitmap = (HBITMAP)SelectObject(m_hDC, hBitmap);
    DeleteObject(hBitmap);
  } else {
    m_nBitsPerPixel = ::GetDeviceCaps(m_hDC, BITSPIXEL);
    m_Width = ::GetDeviceCaps(m_hDC, HORZRES);
    m_Height = ::GetDeviceCaps(m_hDC, VERTRES);
  }
  if (m_DeviceType == DeviceType::kDisplay) {
    m_RenderCaps = FXRC_GET_BITS;
  } else {
    m_RenderCaps = 0;
  }
}

CGdiDeviceDriver::~CGdiDeviceDriver() = default;

DeviceType CGdiDeviceDriver::GetDeviceType() const {
  return m_DeviceType;
}

int CGdiDeviceDriver::GetDeviceCaps(int caps_id) const {
  switch (caps_id) {
    case FXDC_PIXEL_WIDTH:
      return m_Width;
    case FXDC_PIXEL_HEIGHT:
      return m_Height;
    case FXDC_BITS_PIXEL:
      return m_nBitsPerPixel;
    case FXDC_RENDER_CAPS:
      return m_RenderCaps;
    default:
      NOTREACHED_NORETURN();
  }
}

void CGdiDeviceDriver::SaveState() {
  SaveDC(m_hDC);
}

void CGdiDeviceDriver::RestoreState(bool bKeepSaved) {
  RestoreDC(m_hDC, -1);
  if (bKeepSaved)
    SaveDC(m_hDC);
}

bool CGdiDeviceDriver::GDI_SetDIBits(RetainPtr<const CFX_DIBBase> source,
                                     const FX_RECT& src_rect,
                                     int left,
                                     int top) {
  if (m_DeviceType == DeviceType::kPrinter) {
    RetainPtr<const CFX_DIBitmap> flipped_source =
        source->FlipImage(/*bXFlip=*/false, /*bYFlip=*/true);
    if (!flipped_source) {
      return false;
    }

    CHECK(!flipped_source->GetBuffer().empty());
    FixedSizeDataVector<uint8_t> info = GetBitmapInfoHeader(flipped_source);
    auto* header = reinterpret_cast<BITMAPINFOHEADER*>(info.span().data());
    header->biHeight *= -1;
    FX_RECT dst_rect(0, 0, src_rect.Width(), src_rect.Height());
    dst_rect.Intersect(0, 0, flipped_source->GetWidth(),
                       flipped_source->GetHeight());
    int dst_width = dst_rect.Width();
    int dst_height = dst_rect.Height();
    ::StretchDIBits(m_hDC, left, top, dst_width, dst_height, 0, 0, dst_width,
                    dst_height, flipped_source->GetBuffer().data(),
                    reinterpret_cast<BITMAPINFO*>(header), DIB_RGB_COLORS,
                    SRCCOPY);
    return true;
  }

  RetainPtr<const CFX_DIBitmap> realized_source = source->RealizeIfNeeded();
  if (!realized_source) {
    return false;
  }
  FixedSizeDataVector<uint8_t> info = GetBitmapInfoHeader(realized_source);
  auto* header = reinterpret_cast<BITMAPINFOHEADER*>(info.span().data());
  ::SetDIBitsToDevice(
      m_hDC, left, top, src_rect.Width(), src_rect.Height(), src_rect.left,
      realized_source->GetHeight() - src_rect.bottom, 0,
      realized_source->GetHeight(), realized_source->GetBuffer().data(),
      reinterpret_cast<BITMAPINFO*>(header), DIB_RGB_COLORS);
  return true;
}

bool CGdiDeviceDriver::GDI_StretchDIBits(RetainPtr<const CFX_DIBBase> source,
                                         int dest_left,
                                         int dest_top,
                                         int dest_width,
                                         int dest_height,
                                         const FXDIB_ResampleOptions& options) {
  if (!source || dest_width == 0 || dest_height == 0) {
    return false;
  }

  if ((int64_t)abs(dest_width) * abs(dest_height) <
          (int64_t)source->GetWidth() * source->GetHeight() * 4 ||
      options.bInterpolateBilinear) {
    SetStretchBltMode(m_hDC, HALFTONE);
  } else {
    SetStretchBltMode(m_hDC, COLORONCOLOR);
  }

  RetainPtr<const CFX_DIBitmap> realized_source;
  if (m_DeviceType == DeviceType::kPrinter &&
      ((int64_t)source->GetWidth() * source->GetHeight() >
       (int64_t)abs(dest_width) * abs(dest_height))) {
    realized_source = source->StretchTo(dest_width, dest_height,
                                        FXDIB_ResampleOptions(), nullptr);
  } else {
    realized_source = source->RealizeIfNeeded();
  }
  if (!realized_source) {
    return false;
  }

  CHECK(!realized_source->GetBuffer().empty());
  FixedSizeDataVector<uint8_t> info = GetBitmapInfoHeader(realized_source);
  auto* header = reinterpret_cast<BITMAPINFOHEADER*>(info.span().data());
  ::StretchDIBits(m_hDC, dest_left, dest_top, dest_width, dest_height, 0, 0,
                  realized_source->GetWidth(), realized_source->GetHeight(),
                  realized_source->GetBuffer().data(),
                  reinterpret_cast<BITMAPINFO*>(header), DIB_RGB_COLORS,
                  SRCCOPY);
  return true;
}

bool CGdiDeviceDriver::GDI_StretchBitMask(RetainPtr<const CFX_DIBBase> source,
                                          int dest_left,
                                          int dest_top,
                                          int dest_width,
                                          int dest_height,
                                          uint32_t bitmap_color) {
  if (!source || dest_width == 0 || dest_height == 0) {
    return false;
  }

  RetainPtr<const CFX_DIBitmap> realized_source = source->RealizeIfNeeded();
  if (!realized_source) {
    return false;
  }

  int width = realized_source->GetWidth();
  int height = realized_source->GetHeight();
  struct {
    BITMAPINFOHEADER bmiHeader;
    std::array<uint32_t, 2> bmiColors;
  } bmi = {};
  bmi.bmiHeader.biSize = sizeof(BITMAPINFOHEADER);
  bmi.bmiHeader.biBitCount = 1;
  bmi.bmiHeader.biCompression = BI_RGB;
  bmi.bmiHeader.biHeight = -height;
  bmi.bmiHeader.biPlanes = 1;
  bmi.bmiHeader.biWidth = width;
  if (m_nBitsPerPixel != 1) {
    SetStretchBltMode(m_hDC, HALFTONE);
  }
  bmi.bmiColors[0] = 0xffffff;
  bmi.bmiColors[1] = 0;

  HBRUSH hPattern = CreateBrush(bitmap_color);
  HBRUSH hOld = (HBRUSH)SelectObject(m_hDC, hPattern);

  // In PDF, when image mask is 1, use device bitmap; when mask is 0, use brush
  // bitmap.
  // A complete list of the boolen operations is as follows:

  /* P(bitmap_color)    S(ImageMask)    D(DeviceBitmap)    Result
   *        0                 0                0              0
   *        0                 0                1              0
   *        0                 1                0              0
   *        0                 1                1              1
   *        1                 0                0              1
   *        1                 0                1              1
   *        1                 1                0              0
   *        1                 1                1              1
   */
  // The boolen codes is B8. Based on
  // http://msdn.microsoft.com/en-us/library/aa932106.aspx, the ROP3 code is
  // 0xB8074A

  ::StretchDIBits(m_hDC, dest_left, dest_top, dest_width, dest_height, 0, 0,
                  width, height, realized_source->GetBuffer().data(),
                  (BITMAPINFO*)&bmi, DIB_RGB_COLORS, 0xB8074A);

  SelectObject(m_hDC, hOld);
  DeleteObject(hPattern);

  return true;
}

FX_RECT CGdiDeviceDriver::GetClipBox() const {
  FX_RECT rect;
  if (::GetClipBox(m_hDC, reinterpret_cast<RECT*>(&rect))) {
    return rect;
  }
  return FX_RECT(0, 0, m_Width, m_Height);
}

bool CGdiDeviceDriver::MultiplyAlpha(float alpha) {
  // Not implemented. All callers are using `CFX_DIBitmap`-backed raster devices
  // anyway.
  NOTREACHED_NORETURN();
}

bool CGdiDeviceDriver::MultiplyAlphaMask(RetainPtr<const CFX_DIBitmap> mask) {
  // Not implemented. All callers are using `CFX_DIBitmap`-backed raster devices
  // anyway.
  NOTREACHED_NORETURN();
}

void CGdiDeviceDriver::DrawLine(float x1, float y1, float x2, float y2) {
  if (!m_bMetafileDCType) {  // EMF drawing is not bound to the DC.
    int startOutOfBoundsFlag = (x1 < 0) | ((x1 > m_Width) << 1) |
                               ((y1 < 0) << 2) | ((y1 > m_Height) << 3);
    int endOutOfBoundsFlag = (x2 < 0) | ((x2 > m_Width) << 1) |
                             ((y2 < 0) << 2) | ((y2 > m_Height) << 3);
    if (startOutOfBoundsFlag & endOutOfBoundsFlag)
      return;

    if (startOutOfBoundsFlag || endOutOfBoundsFlag) {
      float x[2];
      float y[2];
      unsigned np = LineClip(m_Width, m_Height, x1, y1, x2, y2, x, y);
      if (np == 0)
        return;

      if (np == 1) {
        x2 = x[0];
        y2 = y[0];
      } else {
        DCHECK_EQ(np, 2);
        x1 = x[0];
        y1 = y[0];
        x2 = x[1];
        y2 = y[1];
      }
    }
  }

  MoveToEx(m_hDC, FXSYS_roundf(x1), FXSYS_roundf(y1), nullptr);
  LineTo(m_hDC, FXSYS_roundf(x2), FXSYS_roundf(y2));
}

bool CGdiDeviceDriver::DrawPath(const CFX_Path& path,
                                const CFX_Matrix* pMatrix,
                                const CFX_GraphStateData* pGraphState,
                                uint32_t fill_color,
                                uint32_t stroke_color,
                                const CFX_FillRenderOptions& fill_options) {
  auto* pPlatform =
      static_cast<CWin32Platform*>(CFX_GEModule::Get()->GetPlatform());
  if (!(pGraphState || stroke_color == 0) &&
      !pPlatform->m_GdiplusExt.IsAvailable()) {
    CFX_FloatRect bbox_f = path.GetBoundingBox();
    if (pMatrix)
      bbox_f = pMatrix->TransformRect(bbox_f);

    FX_RECT bbox = bbox_f.GetInnerRect();
    if (bbox.Width() <= 0) {
      return DrawCosmeticLine(CFX_PointF(bbox.left, bbox.top),
                              CFX_PointF(bbox.left, bbox.bottom + 1),
                              fill_color);
    }
    if (bbox.Height() <= 0) {
      return DrawCosmeticLine(CFX_PointF(bbox.left, bbox.top),
                              CFX_PointF(bbox.right + 1, bbox.top), fill_color);
    }
  }
  int fill_alpha = FXARGB_A(fill_color);
  int stroke_alpha = FXARGB_A(stroke_color);
  bool bDrawAlpha = (fill_alpha > 0 && fill_alpha < 255) ||
                    (stroke_alpha > 0 && stroke_alpha < 255 && pGraphState);
  if (!pPlatform->m_GdiplusExt.IsAvailable() && bDrawAlpha)
    return false;

  if (pPlatform->m_GdiplusExt.IsAvailable()) {
    if (bDrawAlpha ||
        ((m_DeviceType != DeviceType::kPrinter && !fill_options.full_cover) ||
         (pGraphState && !pGraphState->m_DashArray.empty()))) {
      if (!((!pMatrix || !pMatrix->WillScale()) && pGraphState &&
            pGraphState->m_LineWidth == 1.0f && path.IsRect())) {
        if (pPlatform->m_GdiplusExt.DrawPath(m_hDC, path, pMatrix, pGraphState,
                                             fill_color, stroke_color,
                                             fill_options)) {
          return true;
        }
      }
    }
  }
  const bool fill =
      fill_options.fill_type != CFX_FillRenderOptions::FillType::kNoFill;
  HPEN hPen = nullptr;
  HBRUSH hBrush = nullptr;
  if (pGraphState && stroke_alpha) {
    SetMiterLimit(m_hDC, pGraphState->m_MiterLimit, nullptr);
    hPen = CreateExtPen(pGraphState, pMatrix, stroke_color);
    hPen = (HPEN)SelectObject(m_hDC, hPen);
  }
  if (fill && fill_alpha) {
    SetPolyFillMode(m_hDC, FillTypeToGdiFillType(fill_options.fill_type));
    hBrush = CreateBrush(fill_color);
    hBrush = (HBRUSH)SelectObject(m_hDC, hBrush);
  }
  if (path.GetPoints().size() == 2 && pGraphState &&
      !pGraphState->m_DashArray.empty()) {
    CFX_PointF pos1 = path.GetPoint(0);
    CFX_PointF pos2 = path.GetPoint(1);
    if (pMatrix) {
      pos1 = pMatrix->Transform(pos1);
      pos2 = pMatrix->Transform(pos2);
    }
    DrawLine(pos1.x, pos1.y, pos2.x, pos2.y);
  } else {
    SetPathToDC(m_hDC, path, pMatrix);
    if (pGraphState && stroke_alpha) {
      if (fill && fill_alpha) {
        if (fill_options.text_mode) {
          StrokeAndFillPath(m_hDC);
        } else {
          FillPath(m_hDC);
          SetPathToDC(m_hDC, path, pMatrix);
          StrokePath(m_hDC);
        }
      } else {
        StrokePath(m_hDC);
      }
    } else if (fill && fill_alpha) {
      FillPath(m_hDC);
    }
  }
  if (hPen) {
    hPen = (HPEN)SelectObject(m_hDC, hPen);
    DeleteObject(hPen);
  }
  if (hBrush) {
    hBrush = (HBRUSH)SelectObject(m_hDC, hBrush);
    DeleteObject(hBrush);
  }
  return true;
}

bool CGdiDeviceDriver::FillRect(const FX_RECT& rect, uint32_t fill_color) {
  auto [alpha, colorref] = ArgbToAlphaAndColorRef(fill_color);
  if (alpha == 0) {
    return true;
  }

  if (alpha < 255) {
    return false;
  }

  HBRUSH hBrush = CreateSolidBrush(colorref);
  const RECT* pRect = reinterpret_cast<const RECT*>(&rect);
  ::FillRect(m_hDC, pRect, hBrush);
  DeleteObject(hBrush);
  return true;
}

void CGdiDeviceDriver::SetBaseClip(const FX_RECT& rect) {
  m_BaseClipBox = rect;
}

bool CGdiDeviceDriver::SetClip_PathFill(
    const CFX_Path& path,
    const CFX_Matrix* pMatrix,
    const CFX_FillRenderOptions& fill_options) {
  std::optional<CFX_FloatRect> maybe_rectf = path.GetRect(pMatrix);
  if (maybe_rectf.has_value()) {
    FX_RECT rect = maybe_rectf.value().GetOuterRect();
    // Can easily apply base clip to protect against wildly large rectangular
    // clips. crbug.com/1019026
    if (m_BaseClipBox.has_value())
      rect.Intersect(m_BaseClipBox.value());
    return IntersectClipRect(m_hDC, rect.left, rect.top, rect.right,
                             rect.bottom) != ERROR;
  }
  SetPathToDC(m_hDC, path, pMatrix);
  SetPolyFillMode(m_hDC, FillTypeToGdiFillType(fill_options.fill_type));
  SelectClipPath(m_hDC, RGN_AND);
  return true;
}

bool CGdiDeviceDriver::SetClip_PathStroke(
    const CFX_Path& path,
    const CFX_Matrix* pMatrix,
    const CFX_GraphStateData* pGraphState) {
  HPEN hPen = CreateExtPen(pGraphState, pMatrix, 0xff000000);
  hPen = (HPEN)SelectObject(m_hDC, hPen);
  SetPathToDC(m_hDC, path, pMatrix);
  WidenPath(m_hDC);
  SetPolyFillMode(m_hDC, WINDING);
  bool ret = !!SelectClipPath(m_hDC, RGN_AND);
  hPen = (HPEN)SelectObject(m_hDC, hPen);
  DeleteObject(hPen);
  return ret;
}

bool CGdiDeviceDriver::DrawCosmeticLine(const CFX_PointF& ptMoveTo,
                                        const CFX_PointF& ptLineTo,
                                        uint32_t color) {
  auto [alpha, colorref] = ArgbToAlphaAndColorRef(color);
  if (alpha == 0) {
    return true;
  }

  HPEN hPen = CreatePen(PS_SOLID, 1, colorref);
  hPen = (HPEN)SelectObject(m_hDC, hPen);
  MoveToEx(m_hDC, FXSYS_roundf(ptMoveTo.x), FXSYS_roundf(ptMoveTo.y), nullptr);
  LineTo(m_hDC, FXSYS_roundf(ptLineTo.x), FXSYS_roundf(ptLineTo.y));
  hPen = (HPEN)SelectObject(m_hDC, hPen);
  DeleteObject(hPen);
  return true;
}
