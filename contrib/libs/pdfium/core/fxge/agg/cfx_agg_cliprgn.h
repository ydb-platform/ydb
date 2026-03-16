// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_AGG_CFX_AGG_CLIPRGN_H_
#define CORE_FXGE_AGG_CFX_AGG_CLIPRGN_H_

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"

class CFX_DIBitmap;

class CFX_AggClipRgn {
 public:
  enum ClipType : bool { kRectI, kMaskF };

  CFX_AggClipRgn(int device_width, int device_height);
  CFX_AggClipRgn(const CFX_AggClipRgn& src);
  ~CFX_AggClipRgn();

  ClipType GetType() const { return m_Type; }
  const FX_RECT& GetBox() const { return m_Box; }
  RetainPtr<CFX_DIBitmap> GetMask() const { return m_Mask; }

  void IntersectRect(const FX_RECT& rect);
  void IntersectMaskF(int left, int top, RetainPtr<CFX_DIBitmap> Mask);

 private:
  void IntersectMaskRect(FX_RECT rect,
                         FX_RECT mask_rect,
                         RetainPtr<CFX_DIBitmap> pOldMask);

  ClipType m_Type = kRectI;
  FX_RECT m_Box;
  RetainPtr<CFX_DIBitmap> m_Mask;
};

#endif  // CORE_FXGE_AGG_CFX_AGG_CLIPRGN_H_
