// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_ICONFIT_H_
#define CORE_FPDFDOC_CPDF_ICONFIT_H_

#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Dictionary;

class CPDF_IconFit {
 public:
  enum class ScaleMethod { kAlways = 0, kBigger, kSmaller, kNever };

  explicit CPDF_IconFit(RetainPtr<const CPDF_Dictionary> pDict);
  CPDF_IconFit(const CPDF_IconFit& that);
  ~CPDF_IconFit();

  ScaleMethod GetScaleMethod() const;
  bool IsProportionalScale() const;
  bool GetFittingBounds() const;
  CFX_PointF GetIconBottomLeftPosition() const;
  CFX_VectorF GetScale(const CFX_SizeF& image_size,
                       const CFX_FloatRect& rcPlate) const;
  CFX_VectorF GetImageOffset(const CFX_SizeF& image_size,
                             const CFX_VectorF& scale,
                             const CFX_FloatRect& rcPlate) const;

 private:
  CFX_PointF GetIconPosition() const;

  RetainPtr<const CPDF_Dictionary> const m_pDict;
};

#endif  // CORE_FPDFDOC_CPDF_ICONFIT_H_
