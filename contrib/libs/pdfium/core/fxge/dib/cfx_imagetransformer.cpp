// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/dib/cfx_imagetransformer.h"

#include <math.h>

#include <array>
#include <iterator>
#include <memory>
#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/dib/cfx_imagestretcher.h"
#include "core/fxge/dib/fx_dib.h"

namespace {

constexpr int kBase = 256;
constexpr float kFix16 = 0.05f;
constexpr uint8_t kOpaqueAlpha = 0xff;

uint8_t BilinearInterpolate(const uint8_t* buf,
                            const CFX_ImageTransformer::BilinearData& data,
                            int bytes_per_pixel,
                            int c_offset) {
  const int i_resx = 255 - data.res_x;
  const int col_bpp_l = data.src_col_l * bytes_per_pixel;
  const int col_bpp_r = data.src_col_r * bytes_per_pixel;
  UNSAFE_TODO({
    const uint8_t* buf_u = buf + data.row_offset_l + c_offset;
    const uint8_t* buf_d = buf + data.row_offset_r + c_offset;
    const uint8_t* src_pos0 = buf_u + col_bpp_l;
    const uint8_t* src_pos1 = buf_u + col_bpp_r;
    const uint8_t* src_pos2 = buf_d + col_bpp_l;
    const uint8_t* src_pos3 = buf_d + col_bpp_r;
    uint8_t r_pos_0 = (*src_pos0 * i_resx + *src_pos1 * data.res_x) >> 8;
    uint8_t r_pos_1 = (*src_pos2 * i_resx + *src_pos3 * data.res_x) >> 8;
    return (r_pos_0 * (255 - data.res_y) + r_pos_1 * data.res_y) >> 8;
  });
}

class CFX_BilinearMatrix {
 public:
  explicit CFX_BilinearMatrix(const CFX_Matrix& src)
      : a(FXSYS_roundf(src.a * kBase)),
        b(FXSYS_roundf(src.b * kBase)),
        c(FXSYS_roundf(src.c * kBase)),
        d(FXSYS_roundf(src.d * kBase)),
        e(FXSYS_roundf(src.e * kBase)),
        f(FXSYS_roundf(src.f * kBase)) {}

  void Transform(int x, int y, int* x1, int* y1, int* res_x, int* res_y) const {
    CFX_PointF val = TransformInternal(CFX_PointF(x, y));
    *x1 = pdfium::saturated_cast<int>(val.x / kBase);
    *y1 = pdfium::saturated_cast<int>(val.y / kBase);
    *res_x = static_cast<int>(val.x) % kBase;
    *res_y = static_cast<int>(val.y) % kBase;
    if (*res_x < 0 && *res_x > -kBase)
      *res_x = kBase + *res_x;
    if (*res_y < 0 && *res_y > -kBase)
      *res_y = kBase + *res_y;
  }

 private:
  CFX_PointF TransformInternal(CFX_PointF pt) const {
    return CFX_PointF(a * pt.x + c * pt.y + e + kBase / 2,
                      b * pt.x + d * pt.y + f + kBase / 2);
  }

  const int a;
  const int b;
  const int c;
  const int d;
  const int e;
  const int f;
};

bool InStretchBounds(const FX_RECT& clip_rect, int col, int row) {
  return col >= 0 && col <= clip_rect.Width() && row >= 0 &&
         row <= clip_rect.Height();
}

void AdjustCoords(const FX_RECT& clip_rect, int* col, int* row) {
  int& src_col = *col;
  int& src_row = *row;
  if (src_col == clip_rect.Width())
    src_col--;
  if (src_row == clip_rect.Height())
    src_row--;
}

// Let the compiler deduce the type for |func|, which cheaper than specifying it
// with std::function.
template <typename F>
void DoBilinearLoop(const CFX_ImageTransformer::CalcData& calc_data,
                    const FX_RECT& result_rect,
                    const FX_RECT& clip_rect,
                    int increment,
                    const F& func) {
  CFX_BilinearMatrix matrix_fix(calc_data.matrix);
  for (int row = 0; row < result_rect.Height(); row++) {
    uint8_t* dest = calc_data.bitmap->GetWritableScanline(row).data();
    for (int col = 0; col < result_rect.Width(); col++) {
      CFX_ImageTransformer::BilinearData d;
      d.res_x = 0;
      d.res_y = 0;
      d.src_col_l = 0;
      d.src_row_l = 0;
      matrix_fix.Transform(col, row, &d.src_col_l, &d.src_row_l, &d.res_x,
                           &d.res_y);
      if (LIKELY(InStretchBounds(clip_rect, d.src_col_l, d.src_row_l))) {
        AdjustCoords(clip_rect, &d.src_col_l, &d.src_row_l);
        d.src_col_r = d.src_col_l + 1;
        d.src_row_r = d.src_row_l + 1;
        AdjustCoords(clip_rect, &d.src_col_r, &d.src_row_r);
        d.row_offset_l = d.src_row_l * calc_data.pitch;
        d.row_offset_r = d.src_row_r * calc_data.pitch;
        func(d, dest);
      }
      UNSAFE_TODO(dest += increment);
    }
  }
}

}  // namespace

CFX_ImageTransformer::CFX_ImageTransformer(RetainPtr<const CFX_DIBBase> source,
                                           const CFX_Matrix& matrix,
                                           const FXDIB_ResampleOptions& options,
                                           const FX_RECT* pClip)
    : m_pSrc(std::move(source)), m_matrix(matrix), m_ResampleOptions(options) {
  FX_RECT result_rect = m_matrix.GetUnitRect().GetClosestRect();
  FX_RECT result_clip = result_rect;
  if (pClip)
    result_clip.Intersect(*pClip);

  if (result_clip.IsEmpty())
    return;

  m_result = result_clip;
  if (fabs(m_matrix.a) < fabs(m_matrix.b) / 20 &&
      fabs(m_matrix.d) < fabs(m_matrix.c) / 20 && fabs(m_matrix.a) < 0.5f &&
      fabs(m_matrix.d) < 0.5f) {
    int dest_width = result_rect.Width();
    int dest_height = result_rect.Height();
    result_clip.Offset(-result_rect.left, -result_rect.top);
    result_clip = result_clip.SwappedClipBox(dest_width, dest_height,
                                             m_matrix.c > 0, m_matrix.b < 0);
    m_Stretcher = std::make_unique<CFX_ImageStretcher>(
        &m_Storer, m_pSrc, dest_height, dest_width, result_clip,
        m_ResampleOptions);
    m_Stretcher->Start();
    m_type = StretchType::kRotate;
    return;
  }
  if (fabs(m_matrix.b) < kFix16 && fabs(m_matrix.c) < kFix16) {
    int dest_width =
        static_cast<int>(m_matrix.a > 0 ? ceil(m_matrix.a) : floor(m_matrix.a));
    int dest_height = static_cast<int>(m_matrix.d > 0 ? -ceil(m_matrix.d)
                                                      : -floor(m_matrix.d));
    result_clip.Offset(-result_rect.left, -result_rect.top);
    m_Stretcher = std::make_unique<CFX_ImageStretcher>(
        &m_Storer, m_pSrc, dest_width, dest_height, result_clip,
        m_ResampleOptions);
    m_Stretcher->Start();
    m_type = StretchType::kNormal;
    return;
  }

  int stretch_width = static_cast<int>(ceil(hypotf(m_matrix.a, m_matrix.b)));
  if (stretch_width == 0) {
    return;
  }

  int stretch_height = static_cast<int>(ceil(hypotf(m_matrix.c, m_matrix.d)));
  if (stretch_height == 0) {
    return;
  }

  CFX_Matrix stretch_to_dest(1.0f, 0.0f, 0.0f, -1.0f, 0.0f, stretch_height);
  stretch_to_dest.Concat(
      CFX_Matrix(m_matrix.a / stretch_width, m_matrix.b / stretch_width,
                 m_matrix.c / stretch_height, m_matrix.d / stretch_height,
                 m_matrix.e, m_matrix.f));
  CFX_Matrix dest_to_strech = stretch_to_dest.GetInverse();

  FX_RECT stretch_clip =
      dest_to_strech.TransformRect(CFX_FloatRect(result_clip)).GetOuterRect();
  if (!stretch_clip.Valid())
    return;

  stretch_clip.Intersect(0, 0, stretch_width, stretch_height);
  if (!stretch_clip.Valid())
    return;

  m_dest2stretch = dest_to_strech;
  m_StretchClip = stretch_clip;
  m_Stretcher = std::make_unique<CFX_ImageStretcher>(
      &m_Storer, m_pSrc, stretch_width, stretch_height, m_StretchClip,
      m_ResampleOptions);
  m_Stretcher->Start();
  m_type = StretchType::kOther;
}

CFX_ImageTransformer::~CFX_ImageTransformer() = default;

bool CFX_ImageTransformer::Continue(PauseIndicatorIface* pPause) {
  if (m_type == StretchType::kNone) {
    return false;
  }

  if (m_Stretcher->Continue(pPause))
    return true;

  switch (m_type) {
    case StretchType::kNone:
      // Already handled separately at the beginning of this method.
      NOTREACHED_NORETURN();
    case StretchType::kNormal:
      return false;
    case StretchType::kRotate:
      ContinueRotate(pPause);
      return false;
    case StretchType::kOther:
      ContinueOther(pPause);
      return false;
  }
}

void CFX_ImageTransformer::ContinueRotate(PauseIndicatorIface* pPause) {
  if (m_Storer.GetBitmap()) {
    m_Storer.Replace(
        m_Storer.GetBitmap()->SwapXY(m_matrix.c > 0, m_matrix.b < 0));
  }
}

void CFX_ImageTransformer::ContinueOther(PauseIndicatorIface* pPause) {
  if (!m_Storer.GetBitmap())
    return;

  auto pTransformed = pdfium::MakeRetain<CFX_DIBitmap>();
  // TODO(crbug.com/42271020): Consider adding support for
  // `FXDIB_Format::kBgraPremul`
  FXDIB_Format dest_format = m_Stretcher->source()->IsMaskFormat()
                                 ? FXDIB_Format::k8bppMask
                                 : FXDIB_Format::kBgra;
  if (!pTransformed->Create(m_result.Width(), m_result.Height(), dest_format)) {
    return;
  }

  CFX_Matrix result2stretch(1.0f, 0.0f, 0.0f, 1.0f, m_result.left,
                            m_result.top);
  result2stretch.Concat(m_dest2stretch);
  result2stretch.Translate(-m_StretchClip.left, -m_StretchClip.top);

  CalcData calc_data = {pTransformed.Get(), result2stretch,
                        m_Storer.GetBitmap()->GetBuffer().data(),
                        m_Storer.GetBitmap()->GetPitch()};
  if (m_Storer.GetBitmap()->IsMaskFormat()) {
    CalcAlpha(calc_data);
  } else {
    const int src_bytes_per_pixel = m_Storer.GetBitmap()->GetBPP() / 8;
    if (src_bytes_per_pixel == 1) {
      CalcMono(calc_data);
    } else {
      CalcColor(calc_data, dest_format, src_bytes_per_pixel);
    }
  }
  m_Storer.Replace(std::move(pTransformed));
}

RetainPtr<CFX_DIBitmap> CFX_ImageTransformer::DetachBitmap() {
  return m_Storer.Detach();
}

void CFX_ImageTransformer::CalcAlpha(const CalcData& calc_data) {
  auto func = [&calc_data](const BilinearData& data, uint8_t* dest) {
    *dest = BilinearInterpolate(calc_data.buf, data, 1, 0);
  };
  DoBilinearLoop(calc_data, m_result, m_StretchClip, 1, func);
}

void CFX_ImageTransformer::CalcMono(const CalcData& calc_data) {
  std::array<uint32_t, 256> argb;
  if (m_Storer.GetBitmap()->HasPalette()) {
    pdfium::span<const uint32_t> palette =
        m_Storer.GetBitmap()->GetPaletteSpan();
    fxcrt::Copy(palette.first(argb.size()), argb);
  } else {
    for (uint32_t i = 0; i < argb.size(); ++i) {
      argb[i] = ArgbEncode(0xff, i, i, i);
    }
  }
  const int dest_bytes_per_pixel = calc_data.bitmap->GetBPP() / 8;
  auto func = [&calc_data, &argb](const BilinearData& data, uint8_t* dest) {
    uint8_t idx = BilinearInterpolate(calc_data.buf, data, 1, 0);
    *reinterpret_cast<uint32_t*>(dest) = argb[idx];
  };
  DoBilinearLoop(calc_data, m_result, m_StretchClip, dest_bytes_per_pixel,
                 func);
}

void CFX_ImageTransformer::CalcColor(const CalcData& calc_data,
                                     FXDIB_Format dest_format,
                                     int src_bytes_per_pixel) {
  DCHECK(dest_format == FXDIB_Format::k8bppMask ||
         dest_format == FXDIB_Format::kBgra);
  const int dest_bytes_per_pixel = calc_data.bitmap->GetBPP() / 8;
  if (!m_Storer.GetBitmap()->IsAlphaFormat()) {
    auto func = [&calc_data, src_bytes_per_pixel](const BilinearData& data,
                                                  uint8_t* dest) {
      uint8_t b =
          BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 0);
      uint8_t g =
          BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 1);
      uint8_t r =
          BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 2);
      *reinterpret_cast<uint32_t*>(dest) = ArgbEncode(kOpaqueAlpha, r, g, b);
    };
    DoBilinearLoop(calc_data, m_result, m_StretchClip, dest_bytes_per_pixel,
                   func);
    return;
  }

  if (dest_format == FXDIB_Format::kBgra) {
    auto func = [&calc_data, src_bytes_per_pixel](const BilinearData& data,
                                                  uint8_t* dest) {
      uint8_t b =
          BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 0);
      uint8_t g =
          BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 1);
      uint8_t r =
          BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 2);
      uint8_t alpha =
          BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 3);
      *reinterpret_cast<uint32_t*>(dest) = ArgbEncode(alpha, r, g, b);
    };
    DoBilinearLoop(calc_data, m_result, m_StretchClip, dest_bytes_per_pixel,
                   func);
    return;
  }

  auto func = [&calc_data, src_bytes_per_pixel](const BilinearData& data,
                                                uint8_t* dest) {
    uint8_t c =
        BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 0);
    uint8_t m =
        BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 1);
    uint8_t y =
        BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 2);
    uint8_t k =
        BilinearInterpolate(calc_data.buf, data, src_bytes_per_pixel, 3);
    *reinterpret_cast<uint32_t*>(dest) = FXCMYK_TODIB(CmykEncode(c, m, y, k));
  };
  DoBilinearLoop(calc_data, m_result, m_StretchClip, dest_bytes_per_pixel,
                 func);
}
