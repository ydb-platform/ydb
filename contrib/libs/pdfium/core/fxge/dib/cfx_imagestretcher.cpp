// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/dib/cfx_imagestretcher.h"

#include <utility>

#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/dib/cstretchengine.h"
#include "core/fxge/dib/fx_dib.h"

namespace {

const int kMaxProgressiveStretchPixels = 1000000;

bool SourceSizeWithinLimit(int width, int height) {
  return !height || width < kMaxProgressiveStretchPixels / height;
}

FXDIB_Format GetStretchedFormat(const CFX_DIBBase& src) {
  FXDIB_Format format = src.GetFormat();
  if (format == FXDIB_Format::k1bppMask)
    return FXDIB_Format::k8bppMask;
  if (format == FXDIB_Format::k1bppRgb)
    return FXDIB_Format::k8bppRgb;
  if (format == FXDIB_Format::k8bppRgb && src.HasPalette())
    return FXDIB_Format::kBgr;
  return format;
}

// Builds a new palette with a size of `CFX_DIBBase::kPaletteSize` from the
// existing palette in `source`.
DataVector<uint32_t> BuildPaletteFrom1BppSource(
    const RetainPtr<const CFX_DIBBase>& source) {
  DCHECK_EQ(FXDIB_Format::k1bppRgb, source->GetFormat());
  DCHECK(source->HasPalette());

  const FX_BGRA_STRUCT<uint8_t> bgra0 =
      ArgbToBGRAStruct(source->GetPaletteArgb(0));
  const FX_BGRA_STRUCT<uint8_t> bgra1 =
      ArgbToBGRAStruct(source->GetPaletteArgb(1));
  CHECK_EQ(255, bgra0.alpha);
  CHECK_EQ(255, bgra1.alpha);

  DataVector<uint32_t> palette(CFX_DIBBase::kPaletteSize);
  for (int i = 0; i < static_cast<int>(CFX_DIBBase::kPaletteSize); ++i) {
    int r = bgra0.red + (bgra1.red - bgra0.red) * i / 255;
    int g = bgra0.green + (bgra1.green - bgra0.green) * i / 255;
    int b = bgra0.blue + (bgra1.blue - bgra0.blue) * i / 255;
    palette[i] = ArgbEncode(255, r, g, b);
  }
  return palette;
}

}  // namespace

CFX_ImageStretcher::CFX_ImageStretcher(ScanlineComposerIface* pDest,
                                       RetainPtr<const CFX_DIBBase> source,
                                       int dest_width,
                                       int dest_height,
                                       const FX_RECT& bitmap_rect,
                                       const FXDIB_ResampleOptions& options)
    : m_pDest(pDest),
      m_pSource(std::move(source)),
      m_ResampleOptions(options),
      m_DestWidth(dest_width),
      m_DestHeight(dest_height),
      m_ClipRect(bitmap_rect),
      m_DestFormat(GetStretchedFormat(*m_pSource)) {
  DCHECK(m_ClipRect.Valid());
}

CFX_ImageStretcher::~CFX_ImageStretcher() = default;

bool CFX_ImageStretcher::Start() {
  if (m_DestWidth == 0 || m_DestHeight == 0)
    return false;

  if (m_pSource->GetFormat() == FXDIB_Format::k1bppRgb &&
      m_pSource->HasPalette()) {
    if (!m_pDest->SetInfo(m_ClipRect.Width(), m_ClipRect.Height(), m_DestFormat,
                          BuildPaletteFrom1BppSource(m_pSource))) {
      return false;
    }
  } else if (!m_pDest->SetInfo(m_ClipRect.Width(), m_ClipRect.Height(),
                               m_DestFormat, {})) {
    return false;
  }
  return StartStretch();
}

bool CFX_ImageStretcher::Continue(PauseIndicatorIface* pPause) {
  return ContinueStretch(pPause);
}

RetainPtr<const CFX_DIBBase> CFX_ImageStretcher::source() {
  return m_pSource;
}

bool CFX_ImageStretcher::StartStretch() {
  m_pStretchEngine = std::make_unique<CStretchEngine>(
      m_pDest, m_DestFormat, m_DestWidth, m_DestHeight, m_ClipRect, m_pSource,
      m_ResampleOptions);
  m_pStretchEngine->StartStretchHorz();
  if (SourceSizeWithinLimit(m_pSource->GetWidth(), m_pSource->GetHeight())) {
    m_pStretchEngine->Continue(nullptr);
    return false;
  }
  return true;
}

bool CFX_ImageStretcher::ContinueStretch(PauseIndicatorIface* pPause) {
  return m_pStretchEngine && m_pStretchEngine->Continue(pPause);
}
