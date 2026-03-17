// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/win32/cgdi_plus_ext.h"

#include <windows.h>

#include <objidl.h>

#include <algorithm>
#include <array>
#include <sstream>
#include <utility>
#include <vector>

#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_gemodule.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/win32/cwin32_platform.h"

// Has to come before gdiplus.h
namespace Gdiplus {
using std::max;
using std::min;
}  // namespace Gdiplus

#include <gdiplus.h>  // NOLINT

namespace {

enum {
  FuncId_GdipCreatePath2,
  FuncId_GdipSetPenDashArray,
  FuncId_GdipSetPenLineJoin,
  FuncId_GdipCreateFromHDC,
  FuncId_GdipSetPageUnit,
  FuncId_GdipSetSmoothingMode,
  FuncId_GdipCreateSolidFill,
  FuncId_GdipFillPath,
  FuncId_GdipDeleteBrush,
  FuncId_GdipCreatePen1,
  FuncId_GdipSetPenMiterLimit,
  FuncId_GdipDrawPath,
  FuncId_GdipDeletePen,
  FuncId_GdipDeletePath,
  FuncId_GdipDeleteGraphics,
  FuncId_GdipDisposeImage,
  FuncId_GdipCreateBitmapFromScan0,
  FuncId_GdipSetInterpolationMode,
  FuncId_GdipDrawImagePointsI,
  FuncId_GdiplusStartup,
  FuncId_GdipDrawLineI,
  FuncId_GdipCreatePath,
  FuncId_GdipSetPathFillMode,
  FuncId_GdipSetClipRegion,
  FuncId_GdipWidenPath,
  FuncId_GdipAddPathLine,
  FuncId_GdipAddPathRectangle,
  FuncId_GdipDeleteRegion,
  FuncId_GdipSetPenLineCap197819,
  FuncId_GdipSetPenDashOffset,
  FuncId_GdipCreateMatrix2,
  FuncId_GdipDeleteMatrix,
  FuncId_GdipSetWorldTransform,
  FuncId_GdipSetPixelOffsetMode,
};

LPCSTR g_GdipFuncNames[] = {
    "GdipCreatePath2",
    "GdipSetPenDashArray",
    "GdipSetPenLineJoin",
    "GdipCreateFromHDC",
    "GdipSetPageUnit",
    "GdipSetSmoothingMode",
    "GdipCreateSolidFill",
    "GdipFillPath",
    "GdipDeleteBrush",
    "GdipCreatePen1",
    "GdipSetPenMiterLimit",
    "GdipDrawPath",
    "GdipDeletePen",
    "GdipDeletePath",
    "GdipDeleteGraphics",
    "GdipDisposeImage",
    "GdipCreateBitmapFromScan0",
    "GdipSetInterpolationMode",
    "GdipDrawImagePointsI",
    "GdiplusStartup",
    "GdipDrawLineI",
    "GdipCreatePath",
    "GdipSetPathFillMode",
    "GdipSetClipRegion",
    "GdipWidenPath",
    "GdipAddPathLine",
    "GdipAddPathRectangle",
    "GdipDeleteRegion",
    "GdipSetPenLineCap197819",
    "GdipSetPenDashOffset",
    "GdipCreateMatrix2",
    "GdipDeleteMatrix",
    "GdipSetWorldTransform",
    "GdipSetPixelOffsetMode",
};
static_assert(std::size(g_GdipFuncNames) ==
                  static_cast<size_t>(FuncId_GdipSetPixelOffsetMode) + 1,
              "g_GdipFuncNames has wrong size");

using FuncType_GdipCreatePath2 =
    decltype(&Gdiplus::DllExports::GdipCreatePath2);
using FuncType_GdipSetPenDashArray =
    decltype(&Gdiplus::DllExports::GdipSetPenDashArray);
using FuncType_GdipSetPenLineJoin =
    decltype(&Gdiplus::DllExports::GdipSetPenLineJoin);
using FuncType_GdipCreateFromHDC =
    decltype(&Gdiplus::DllExports::GdipCreateFromHDC);
using FuncType_GdipSetPageUnit =
    decltype(&Gdiplus::DllExports::GdipSetPageUnit);
using FuncType_GdipSetSmoothingMode =
    decltype(&Gdiplus::DllExports::GdipSetSmoothingMode);
using FuncType_GdipCreateSolidFill =
    decltype(&Gdiplus::DllExports::GdipCreateSolidFill);
using FuncType_GdipFillPath = decltype(&Gdiplus::DllExports::GdipFillPath);
using FuncType_GdipDeleteBrush =
    decltype(&Gdiplus::DllExports::GdipDeleteBrush);
using FuncType_GdipCreatePen1 = decltype(&Gdiplus::DllExports::GdipCreatePen1);
using FuncType_GdipSetPenMiterLimit =
    decltype(&Gdiplus::DllExports::GdipSetPenMiterLimit);
using FuncType_GdipDrawPath = decltype(&Gdiplus::DllExports::GdipDrawPath);
using FuncType_GdipDeletePen = decltype(&Gdiplus::DllExports::GdipDeletePen);
using FuncType_GdipDeletePath = decltype(&Gdiplus::DllExports::GdipDeletePath);
using FuncType_GdipDeleteGraphics =
    decltype(&Gdiplus::DllExports::GdipDeleteGraphics);
using FuncType_GdipDisposeImage =
    decltype(&Gdiplus::DllExports::GdipDisposeImage);
using FuncType_GdipCreateBitmapFromScan0 =
    decltype(&Gdiplus::DllExports::GdipCreateBitmapFromScan0);
using FuncType_GdipSetInterpolationMode =
    decltype(&Gdiplus::DllExports::GdipSetInterpolationMode);
using FuncType_GdipDrawImagePointsI =
    decltype(&Gdiplus::DllExports::GdipDrawImagePointsI);
using FuncType_GdiplusStartup = decltype(&Gdiplus::GdiplusStartup);
using FuncType_GdipDrawLineI = decltype(&Gdiplus::DllExports::GdipDrawLineI);
using FuncType_GdipCreatePath = decltype(&Gdiplus::DllExports::GdipCreatePath);
using FuncType_GdipSetPathFillMode =
    decltype(&Gdiplus::DllExports::GdipSetPathFillMode);
using FuncType_GdipSetClipRegion =
    decltype(&Gdiplus::DllExports::GdipSetClipRegion);
using FuncType_GdipWidenPath = decltype(&Gdiplus::DllExports::GdipWidenPath);
using FuncType_GdipAddPathLine =
    decltype(&Gdiplus::DllExports::GdipAddPathLine);
using FuncType_GdipAddPathRectangle =
    decltype(&Gdiplus::DllExports::GdipAddPathRectangle);
using FuncType_GdipDeleteRegion =
    decltype(&Gdiplus::DllExports::GdipDeleteRegion);
using FuncType_GdipSetPenLineCap197819 =
    decltype(&Gdiplus::DllExports::GdipSetPenLineCap197819);
using FuncType_GdipSetPenDashOffset =
    decltype(&Gdiplus::DllExports::GdipSetPenDashOffset);
using FuncType_GdipCreateMatrix2 =
    decltype(&Gdiplus::DllExports::GdipCreateMatrix2);
using FuncType_GdipDeleteMatrix =
    decltype(&Gdiplus::DllExports::GdipDeleteMatrix);
using FuncType_GdipSetWorldTransform =
    decltype(&Gdiplus::DllExports::GdipSetWorldTransform);
using FuncType_GdipSetPixelOffsetMode =
    decltype(&Gdiplus::DllExports::GdipSetPixelOffsetMode);

#define CALLFUNC(gdi_plus_ext, funcname, ...) \
  reinterpret_cast<FuncType_##funcname>(      \
      (gdi_plus_ext).functions()[FuncId_##funcname])(__VA_ARGS__)

Gdiplus::GpFillMode FillType2Gdip(CFX_FillRenderOptions::FillType fill_type) {
  return fill_type == CFX_FillRenderOptions::FillType::kEvenOdd
             ? Gdiplus::FillModeAlternate
             : Gdiplus::FillModeWinding;
}

const CGdiplusExt& GetGdiplusExt() {
  auto* pData =
      static_cast<CWin32Platform*>(CFX_GEModule::Get()->GetPlatform());
  return pData->m_GdiplusExt;
}

Gdiplus::GpBrush* GdipCreateBrushImpl(DWORD argb) {
  Gdiplus::GpSolidFill* solidBrush = nullptr;
  CALLFUNC(GetGdiplusExt(), GdipCreateSolidFill,
           static_cast<Gdiplus::ARGB>(argb), &solidBrush);
  return solidBrush;
}

void OutputImage(Gdiplus::GpGraphics* graphics,
                 RetainPtr<const CFX_DIBBase> source,
                 int dest_left,
                 int dest_top,
                 int dest_width,
                 int dest_height) {
  CHECK_EQ(FXDIB_Format::kBgra, source->GetFormat());
  const CGdiplusExt& gdi_plus_ext = GetGdiplusExt();

  RetainPtr<const CFX_DIBitmap> realized_source = source->RealizeIfNeeded();
  if (!realized_source) {
    return;
  }

  // `GdipCreateBitmapFromScan0()` requires a `BYTE*` scanline buffer, but in
  // this case, the bitmap only gets read by `GdipDrawImagePointsI()`, then
  // disposed of, so it's safe to cast away `const` here.
  uint8_t* scan0 = const_cast<uint8_t*>(realized_source->GetBuffer().data());
  Gdiplus::GpBitmap* bitmap = nullptr;
  CALLFUNC(gdi_plus_ext, GdipCreateBitmapFromScan0, realized_source->GetWidth(),
           realized_source->GetHeight(), realized_source->GetPitch(),
           PixelFormat32bppARGB, scan0, &bitmap);
  if (dest_height < 0) {
    dest_height--;
  }
  if (dest_width < 0) {
    dest_width--;
  }
  const Gdiplus::Point destination_points[] = {
      Gdiplus::Point(dest_left, dest_top),
      Gdiplus::Point(dest_left + dest_width, dest_top),
      Gdiplus::Point(dest_left, dest_top + dest_height)};
  CALLFUNC(gdi_plus_ext, GdipDrawImagePointsI, graphics, bitmap,
           destination_points, std::size(destination_points));
  CALLFUNC(gdi_plus_ext, GdipDisposeImage, bitmap);
}

Gdiplus::GpPen* GdipCreatePenImpl(const CFX_GraphStateData* pGraphState,
                                  const CFX_Matrix* pMatrix,
                                  DWORD argb,
                                  bool bTextMode) {
  const CGdiplusExt& gdi_plus_ext = GetGdiplusExt();
  float width = pGraphState->m_LineWidth;
  if (!bTextMode) {
    float unit = pMatrix
                     ? 1.0f / ((pMatrix->GetXUnit() + pMatrix->GetYUnit()) / 2)
                     : 1.0f;
    width = std::max(width, unit);
  }
  Gdiplus::GpPen* pPen = nullptr;
  CALLFUNC(gdi_plus_ext, GdipCreatePen1, static_cast<Gdiplus::ARGB>(argb),
           width, Gdiplus::UnitWorld, &pPen);
  Gdiplus::LineCap lineCap = Gdiplus::LineCapFlat;
  Gdiplus::DashCap dashCap = Gdiplus::DashCapFlat;
  bool bDashExtend = false;
  switch (pGraphState->m_LineCap) {
    case CFX_GraphStateData::LineCap::kButt:
      lineCap = Gdiplus::LineCapFlat;
      break;
    case CFX_GraphStateData::LineCap::kRound:
      lineCap = Gdiplus::LineCapRound;
      dashCap = Gdiplus::DashCapRound;
      bDashExtend = true;
      break;
    case CFX_GraphStateData::LineCap::kSquare:
      lineCap = Gdiplus::LineCapSquare;
      bDashExtend = true;
      break;
  }
  CALLFUNC(gdi_plus_ext, GdipSetPenLineCap197819, pPen, lineCap, lineCap,
           dashCap);
  Gdiplus::LineJoin lineJoin = Gdiplus::LineJoinMiterClipped;
  switch (pGraphState->m_LineJoin) {
    case CFX_GraphStateData::LineJoin::kMiter:
      lineJoin = Gdiplus::LineJoinMiterClipped;
      break;
    case CFX_GraphStateData::LineJoin::kRound:
      lineJoin = Gdiplus::LineJoinRound;
      break;
    case CFX_GraphStateData::LineJoin::kBevel:
      lineJoin = Gdiplus::LineJoinBevel;
      break;
  }
  CALLFUNC(gdi_plus_ext, GdipSetPenLineJoin, pPen, lineJoin);
  if (!pGraphState->m_DashArray.empty()) {
    float* pDashArray =
        FX_Alloc(float, FxAlignToBoundary<2>(pGraphState->m_DashArray.size()));
    int nCount = 0;
    float on_leftover = 0;
    float off_leftover = 0;
    for (size_t i = 0; i < pGraphState->m_DashArray.size(); i += 2) {
      float on_phase = pGraphState->m_DashArray[i];
      float off_phase;
      if (i + 1 < pGraphState->m_DashArray.size()) {
        off_phase = pGraphState->m_DashArray[i + 1];
      } else {
        off_phase = on_phase;
      }
      on_phase /= width;
      off_phase /= width;
      if (on_phase + off_phase <= 0.00002f) {
        on_phase = 0.1f;
        off_phase = 0.1f;
      }
      if (bDashExtend) {
        if (off_phase < 1)
          off_phase = 0;
        else
          --off_phase;
        ++on_phase;
      }
      if (on_phase == 0 || off_phase == 0) {
        if (nCount == 0) {
          on_leftover += on_phase;
          off_leftover += off_phase;
        } else {
          UNSAFE_TODO({
            pDashArray[nCount - 2] += on_phase;
            pDashArray[nCount - 1] += off_phase;
          });
        }
      } else {
        UNSAFE_TODO({
          pDashArray[nCount++] = on_phase + on_leftover;
          on_leftover = 0;
          pDashArray[nCount++] = off_phase + off_leftover;
          off_leftover = 0;
        });
      }
    }
    CALLFUNC(gdi_plus_ext, GdipSetPenDashArray, pPen, pDashArray, nCount);
    float phase = pGraphState->m_DashPhase;
    if (bDashExtend) {
      if (phase < 0.5f)
        phase = 0;
      else
        phase -= 0.5f;
    }
    CALLFUNC(gdi_plus_ext, GdipSetPenDashOffset, pPen, phase);
    FX_Free(pDashArray);
    pDashArray = nullptr;
  }
  CALLFUNC(gdi_plus_ext, GdipSetPenMiterLimit, pPen, pGraphState->m_MiterLimit);
  return pPen;
}

std::optional<std::pair<size_t, size_t>> IsSmallTriangle(
    pdfium::span<const Gdiplus::PointF> points,
    const CFX_Matrix* pMatrix) {
  static constexpr std::array<std::pair<const size_t, const size_t>, 3> kPairs =
      {{{1, 2}, {0, 2}, {0, 1}}};
  for (size_t i = 0; i < std::size(kPairs); ++i) {
    size_t pair1 = kPairs[i].first;
    size_t pair2 = kPairs[i].second;
    CFX_PointF p1(points[pair1].X, points[pair1].Y);
    CFX_PointF p2(points[pair2].X, points[pair2].Y);
    if (pMatrix) {
      p1 = pMatrix->Transform(p1);
      p2 = pMatrix->Transform(p2);
    }
    CFX_PointF diff = p1 - p2;
    float distance_square = (diff.x * diff.x) + (diff.y * diff.y);
    if (distance_square < 2.25f)
      return std::make_pair(i, pair1);
  }
  return std::nullopt;
}

class GpStream final : public IStream {
 public:
  GpStream() = default;
  ~GpStream() = default;

  // IUnknown
  HRESULT STDMETHODCALLTYPE QueryInterface(REFIID iid,
                                           void** ppvObject) override {
    if (iid == __uuidof(IUnknown) || iid == __uuidof(IStream) ||
        iid == __uuidof(ISequentialStream)) {
      *ppvObject = static_cast<IStream*>(this);
      AddRef();
      return S_OK;
    }
    return E_NOINTERFACE;
  }
  ULONG STDMETHODCALLTYPE AddRef() override {
    return (ULONG)InterlockedIncrement(&m_RefCount);
  }
  ULONG STDMETHODCALLTYPE Release() override {
    ULONG res = (ULONG)InterlockedDecrement(&m_RefCount);
    if (res == 0) {
      delete this;
    }
    return res;
  }

  // ISequentialStream
  HRESULT STDMETHODCALLTYPE Read(void* output,
                                 ULONG cb,
                                 ULONG* pcbRead) override {
    if (pcbRead)
      *pcbRead = 0;

    if (m_ReadPos >= m_InterStream.tellp())
      return HRESULT_FROM_WIN32(ERROR_END_OF_MEDIA);

    size_t bytes_left = pdfium::checked_cast<size_t>(
        std::streamoff(m_InterStream.tellp()) - m_ReadPos);
    size_t bytes_out = std::min(pdfium::checked_cast<size_t>(cb), bytes_left);
    UNSAFE_TODO(FXSYS_memcpy(output, m_InterStream.str().c_str() + m_ReadPos,
                             bytes_out));
    m_ReadPos += bytes_out;
    if (pcbRead)
      *pcbRead = (ULONG)bytes_out;

    return S_OK;
  }
  HRESULT STDMETHODCALLTYPE Write(const void* input,
                                  ULONG cb,
                                  ULONG* pcbWritten) override {
    if (cb <= 0) {
      if (pcbWritten)
        *pcbWritten = 0;
      return S_OK;
    }
    m_InterStream.write(reinterpret_cast<const char*>(input), cb);
    if (pcbWritten)
      *pcbWritten = cb;
    return S_OK;
  }

  // IStream
  HRESULT STDMETHODCALLTYPE SetSize(ULARGE_INTEGER) override {
    return E_NOTIMPL;
  }
  HRESULT STDMETHODCALLTYPE CopyTo(IStream*,
                                   ULARGE_INTEGER,
                                   ULARGE_INTEGER*,
                                   ULARGE_INTEGER*) override {
    return E_NOTIMPL;
  }
  HRESULT STDMETHODCALLTYPE Commit(DWORD) override { return E_NOTIMPL; }
  HRESULT STDMETHODCALLTYPE Revert() override { return E_NOTIMPL; }
  HRESULT STDMETHODCALLTYPE LockRegion(ULARGE_INTEGER,
                                       ULARGE_INTEGER,
                                       DWORD) override {
    return E_NOTIMPL;
  }
  HRESULT STDMETHODCALLTYPE UnlockRegion(ULARGE_INTEGER,
                                         ULARGE_INTEGER,
                                         DWORD) override {
    return E_NOTIMPL;
  }
  HRESULT STDMETHODCALLTYPE Clone(IStream** stream) override {
    return E_NOTIMPL;
  }
  HRESULT STDMETHODCALLTYPE Seek(LARGE_INTEGER liDistanceToMove,
                                 DWORD dwOrigin,
                                 ULARGE_INTEGER* lpNewFilePointer) override {
    std::streamoff start;
    std::streamoff new_read_position;
    switch (dwOrigin) {
      case STREAM_SEEK_SET:
        start = 0;
        break;
      case STREAM_SEEK_CUR:
        start = m_ReadPos;
        break;
      case STREAM_SEEK_END:
        if (m_InterStream.tellp() < 0)
          return STG_E_SEEKERROR;
        start = m_InterStream.tellp();
        break;
      default:
        return STG_E_INVALIDFUNCTION;
    }
    new_read_position = start + liDistanceToMove.QuadPart;
    if (new_read_position > m_InterStream.tellp())
      return STG_E_SEEKERROR;

    m_ReadPos = new_read_position;
    if (lpNewFilePointer)
      lpNewFilePointer->QuadPart = m_ReadPos;

    return S_OK;
  }
  HRESULT STDMETHODCALLTYPE Stat(STATSTG* pStatstg,
                                 DWORD grfStatFlag) override {
    if (!pStatstg)
      return STG_E_INVALIDFUNCTION;

    ZeroMemory(pStatstg, sizeof(STATSTG));

    if (m_InterStream.tellp() < 0)
      return STG_E_SEEKERROR;

    pStatstg->cbSize.QuadPart = m_InterStream.tellp();
    return S_OK;
  }

 private:
  LONG m_RefCount = 1;
  std::streamoff m_ReadPos = 0;
  fxcrt::ostringstream m_InterStream;
};

}  // namespace

CGdiplusExt::CGdiplusExt() = default;

CGdiplusExt::~CGdiplusExt() {
  FreeLibrary(gdiplus_module_);
}

void CGdiplusExt::Load() {
  char buf[MAX_PATH];
  GetSystemDirectoryA(buf, MAX_PATH);
  ByteString dllpath = buf;
  dllpath += "\\GDIPLUS.DLL";
  gdiplus_module_ = LoadLibraryA(dllpath.c_str());
  if (!gdiplus_module_) {
    return;
  }

  gdiplus_functions_.resize(std::size(g_GdipFuncNames));
  UNSAFE_TODO({
    for (size_t i = 0; i < std::size(g_GdipFuncNames); ++i) {
      gdiplus_functions_[i] =
          GetProcAddress(gdiplus_module_, g_GdipFuncNames[i]);
      if (!gdiplus_functions_[i]) {
        FreeLibrary(gdiplus_module_);
        gdiplus_module_ = nullptr;
        return;
      }
    }
  });
  ULONG_PTR gdiplus_token;
  Gdiplus::GdiplusStartupInput gdiplus_startup_input;
  ((FuncType_GdiplusStartup)gdiplus_functions_[FuncId_GdiplusStartup])(
      &gdiplus_token, &gdiplus_startup_input, nullptr);
}

bool CGdiplusExt::StretchDIBits(HDC hDC,
                                RetainPtr<const CFX_DIBBase> source,
                                int dest_left,
                                int dest_top,
                                int dest_width,
                                int dest_height) {
  CHECK(source->IsAlphaFormat());
  Gdiplus::GpGraphics* pGraphics;
  const CGdiplusExt& gdi_plus_ext = GetGdiplusExt();
  CALLFUNC(gdi_plus_ext, GdipCreateFromHDC, hDC, &pGraphics);
  CALLFUNC(gdi_plus_ext, GdipSetPageUnit, pGraphics, Gdiplus::UnitPixel);
  if (source->GetWidth() > abs(dest_width) / 2 ||
      source->GetHeight() > abs(dest_height) / 2) {
    CALLFUNC(gdi_plus_ext, GdipSetInterpolationMode, pGraphics,
             Gdiplus::InterpolationModeHighQuality);
  } else {
    CALLFUNC(gdi_plus_ext, GdipSetInterpolationMode, pGraphics,
             Gdiplus::InterpolationModeBilinear);
  }
  OutputImage(pGraphics, std::move(source), dest_left, dest_top, dest_width,
              dest_height);
  CALLFUNC(gdi_plus_ext, GdipDeleteGraphics, pGraphics);
  return true;
}

bool CGdiplusExt::DrawPath(HDC hDC,
                           const CFX_Path& path,
                           const CFX_Matrix* pObject2Device,
                           const CFX_GraphStateData* pGraphState,
                           uint32_t fill_argb,
                           uint32_t stroke_argb,
                           const CFX_FillRenderOptions& fill_options) {
  pdfium::span<const CFX_Path::Point> points = path.GetPoints();
  if (points.empty())
    return true;

  Gdiplus::GpGraphics* pGraphics = nullptr;
  const CGdiplusExt& gdi_plus_ext = GetGdiplusExt();
  CALLFUNC(gdi_plus_ext, GdipCreateFromHDC, hDC, &pGraphics);
  CALLFUNC(gdi_plus_ext, GdipSetPageUnit, pGraphics, Gdiplus::UnitPixel);
  CALLFUNC(gdi_plus_ext, GdipSetPixelOffsetMode, pGraphics,
           Gdiplus::PixelOffsetModeHalf);
  Gdiplus::GpMatrix* pMatrix = nullptr;
  if (pObject2Device) {
    CALLFUNC(gdi_plus_ext, GdipCreateMatrix2, pObject2Device->a,
             pObject2Device->b, pObject2Device->c, pObject2Device->d,
             pObject2Device->e, pObject2Device->f, &pMatrix);
    CALLFUNC(gdi_plus_ext, GdipSetWorldTransform, pGraphics, pMatrix);
  }
  std::vector<Gdiplus::PointF> gp_points(points.size());
  std::vector<BYTE> gp_types(points.size());
  int nSubPathes = 0;
  bool bSubClose = false;
  bool bSmooth = false;
  size_t pos_subclose = 0;
  size_t startpoint = 0;
  for (size_t i = 0; i < points.size(); ++i) {
    gp_points[i].X = points[i].m_Point.x;
    gp_points[i].Y = points[i].m_Point.y;

    CFX_PointF pos = points[i].m_Point;
    if (pObject2Device)
      pos = pObject2Device->Transform(pos);

    if (pos.x > 50000.0f)
      gp_points[i].X = 50000.0f;
    if (pos.x < -50000.0f)
      gp_points[i].X = -50000.0f;
    if (pos.y > 50000.0f)
      gp_points[i].Y = 50000.0f;
    if (pos.y < -50000.0f)
      gp_points[i].Y = -50000.0f;

    CFX_Path::Point::Type point_type = points[i].m_Type;
    if (point_type == CFX_Path::Point::Type::kMove) {
      gp_types[i] = Gdiplus::PathPointTypeStart;
      nSubPathes++;
      bSubClose = false;
      startpoint = i;
    } else if (point_type == CFX_Path::Point::Type::kLine) {
      gp_types[i] = Gdiplus::PathPointTypeLine;
      if (points[i - 1].IsTypeAndOpen(CFX_Path::Point::Type::kMove) &&
          (i + 1 == points.size() ||
           points[i + 1].IsTypeAndOpen(CFX_Path::Point::Type::kMove)) &&
          gp_points[i].Y == gp_points[i - 1].Y &&
          gp_points[i].X == gp_points[i - 1].X) {
        gp_points[i].X += 0.01f;
        continue;
      }
      if (!bSmooth && gp_points[i].X != gp_points[i - 1].X &&
          gp_points[i].Y != gp_points[i - 1].Y) {
        bSmooth = true;
      }
    } else if (point_type == CFX_Path::Point::Type::kBezier) {
      gp_types[i] = Gdiplus::PathPointTypeBezier;
      bSmooth = true;
    }
    if (points[i].m_CloseFigure) {
      if (bSubClose)
        gp_types[pos_subclose] &= ~Gdiplus::PathPointTypeCloseSubpath;
      else
        bSubClose = true;
      pos_subclose = i;
      gp_types[i] |= Gdiplus::PathPointTypeCloseSubpath;
      if (!bSmooth && gp_points[i].X != gp_points[startpoint].X &&
          gp_points[i].Y != gp_points[startpoint].Y) {
        bSmooth = true;
      }
    }
  }
  const bool fill =
      fill_options.fill_type != CFX_FillRenderOptions::FillType::kNoFill;
  if (fill_options.aliased_path) {
    bSmooth = false;
    CALLFUNC(gdi_plus_ext, GdipSetSmoothingMode, pGraphics,
             Gdiplus::SmoothingModeNone);
  } else if (!fill_options.full_cover) {
    if (!bSmooth && fill)
      bSmooth = true;

    if (bSmooth || (pGraphState && pGraphState->m_LineWidth > 2)) {
      CALLFUNC(gdi_plus_ext, GdipSetSmoothingMode, pGraphics,
               Gdiplus::SmoothingModeAntiAlias);
    }
  }
  if (points.size() == 4 && !pGraphState) {
    auto indices = IsSmallTriangle(gp_points, pObject2Device);
    if (indices.has_value()) {
      auto [v1, v2] = indices.value();
      Gdiplus::GpPen* pPen = nullptr;
      CALLFUNC(gdi_plus_ext, GdipCreatePen1, fill_argb, 1.0f,
               Gdiplus::UnitPixel, &pPen);
      CALLFUNC(gdi_plus_ext, GdipDrawLineI, pGraphics, pPen,
               FXSYS_roundf(gp_points[v1].X), FXSYS_roundf(gp_points[v1].Y),
               FXSYS_roundf(gp_points[v2].X), FXSYS_roundf(gp_points[v2].Y));
      CALLFUNC(gdi_plus_ext, GdipDeletePen, pPen);
      return true;
    }
  }
  Gdiplus::GpPath* pGpPath = nullptr;
  const Gdiplus::GpFillMode gp_fill_mode =
      FillType2Gdip(fill_options.fill_type);
  CALLFUNC(gdi_plus_ext, GdipCreatePath2, gp_points.data(), gp_types.data(),
           pdfium::checked_cast<int>(points.size()), gp_fill_mode, &pGpPath);
  if (!pGpPath) {
    if (pMatrix) {
      CALLFUNC(gdi_plus_ext, GdipDeleteMatrix, pMatrix);
    }

    CALLFUNC(gdi_plus_ext, GdipDeleteGraphics, pGraphics);
    return false;
  }
  if (fill) {
    Gdiplus::GpBrush* pBrush = GdipCreateBrushImpl(fill_argb);
    CALLFUNC(gdi_plus_ext, GdipSetPathFillMode, pGpPath, gp_fill_mode);
    CALLFUNC(gdi_plus_ext, GdipFillPath, pGraphics, pBrush, pGpPath);
    CALLFUNC(gdi_plus_ext, GdipDeleteBrush, pBrush);
  }
  if (pGraphState && stroke_argb) {
    Gdiplus::GpPen* pPen =
        GdipCreatePenImpl(pGraphState, pObject2Device, stroke_argb,
                          fill_options.stroke_text_mode);
    if (nSubPathes == 1) {
      CALLFUNC(gdi_plus_ext, GdipDrawPath, pGraphics, pPen, pGpPath);
    } else {
      size_t iStart = 0;
      for (size_t i = 0; i < points.size(); ++i) {
        if (i + 1 == points.size() ||
            gp_types[i + 1] == Gdiplus::PathPointTypeStart) {
          Gdiplus::GpPath* pSubPath;
          CALLFUNC(gdi_plus_ext, GdipCreatePath2, &gp_points[iStart],
                   &gp_types[iStart], pdfium::checked_cast<int>(i - iStart + 1),
                   gp_fill_mode, &pSubPath);
          iStart = i + 1;
          CALLFUNC(gdi_plus_ext, GdipDrawPath, pGraphics, pPen, pSubPath);
          CALLFUNC(gdi_plus_ext, GdipDeletePath, pSubPath);
        }
      }
    }
    CALLFUNC(gdi_plus_ext, GdipDeletePen, pPen);
  }
  if (pMatrix) {
    CALLFUNC(gdi_plus_ext, GdipDeleteMatrix, pMatrix);
  }
  CALLFUNC(gdi_plus_ext, GdipDeletePath, pGpPath);
  CALLFUNC(gdi_plus_ext, GdipDeleteGraphics, pGraphics);
  return true;
}
