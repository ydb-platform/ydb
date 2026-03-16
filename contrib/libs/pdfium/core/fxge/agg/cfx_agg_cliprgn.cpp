// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/agg/cfx_agg_cliprgn.h"

#include <stdint.h>

#include <utility>

#include "core/fxcrt/check_op.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/dib/cfx_dibitmap.h"

CFX_AggClipRgn::CFX_AggClipRgn(int width, int height)
    : m_Box(0, 0, width, height) {}

CFX_AggClipRgn::CFX_AggClipRgn(const CFX_AggClipRgn& src) = default;

CFX_AggClipRgn::~CFX_AggClipRgn() = default;

void CFX_AggClipRgn::IntersectRect(const FX_RECT& rect) {
  if (m_Type == kRectI) {
    m_Box.Intersect(rect);
    return;
  }
  IntersectMaskRect(rect, m_Box, m_Mask);
}

void CFX_AggClipRgn::IntersectMaskRect(FX_RECT rect,
                                       FX_RECT mask_rect,
                                       RetainPtr<CFX_DIBitmap> pOldMask) {
  m_Type = kMaskF;
  m_Box = rect;
  m_Box.Intersect(mask_rect);
  if (m_Box.IsEmpty()) {
    m_Type = kRectI;
    return;
  }
  if (m_Box == mask_rect) {
    m_Mask = std::move(pOldMask);
    return;
  }
  m_Mask = pdfium::MakeRetain<CFX_DIBitmap>();
  CHECK(m_Mask->Create(m_Box.Width(), m_Box.Height(), FXDIB_Format::k8bppMask));
  const int offset = m_Box.left - mask_rect.left;
  for (int row = m_Box.top; row < m_Box.bottom; row++) {
    pdfium::span<uint8_t> dest_scan =
        m_Mask->GetWritableScanline(row - m_Box.top);
    pdfium::span<const uint8_t> src_scan =
        pOldMask->GetScanline(row - mask_rect.top);
    fxcrt::Copy(src_scan.subspan(offset, m_Box.Width()), dest_scan);
  }
}

void CFX_AggClipRgn::IntersectMaskF(int left,
                                    int top,
                                    RetainPtr<CFX_DIBitmap> pMask) {
  FX_RECT mask_box(left, top, left + pMask->GetWidth(),
                   top + pMask->GetHeight());
  if (!mask_box.IsEmpty()) {
    // Make sure non-empty masks have the right format. If the mask is empty,
    // then the format does not matter as it will not get used.
    CHECK_EQ(pMask->GetFormat(), FXDIB_Format::k8bppMask);
  }
  if (m_Type == kRectI) {
    IntersectMaskRect(m_Box, mask_box, std::move(pMask));
    return;
  }

  FX_RECT new_box = m_Box;
  new_box.Intersect(mask_box);
  if (new_box.IsEmpty()) {
    m_Type = kRectI;
    m_Mask = nullptr;
    m_Box = new_box;
    return;
  }
  auto new_dib = pdfium::MakeRetain<CFX_DIBitmap>();
  CHECK(new_dib->Create(new_box.Width(), new_box.Height(),
                        FXDIB_Format::k8bppMask));
  for (int row = new_box.top; row < new_box.bottom; row++) {
    pdfium::span<const uint8_t> old_scan = m_Mask->GetScanline(row - m_Box.top);
    pdfium::span<const uint8_t> mask_scan = pMask->GetScanline(row - top);
    auto new_scan = new_dib->GetWritableScanline(row - new_box.top);
    for (int col = new_box.left; col < new_box.right; col++) {
      new_scan[col - new_box.left] =
          old_scan[col - m_Box.left] * mask_scan[col - left] / 255;
    }
  }
  m_Box = new_box;
  m_Mask = std::move(new_dib);
}
