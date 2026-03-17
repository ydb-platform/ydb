// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_PATH_H_
#define CORE_FPDFAPI_PAGE_CPDF_PATH_H_

#include <vector>

#include "core/fxcrt/shared_copy_on_write.h"
#include "core/fxge/cfx_path.h"

class CPDF_Path {
 public:
  CPDF_Path();
  CPDF_Path(const CPDF_Path& that);
  ~CPDF_Path();

  void Emplace() { m_Ref.Emplace(); }
  bool HasRef() const { return !!m_Ref; }

  const std::vector<CFX_Path::Point>& GetPoints() const;
  void ClosePath();

  CFX_PointF GetPoint(int index) const;
  CFX_FloatRect GetBoundingBox() const;
  CFX_FloatRect GetBoundingBoxForStrokePath(float line_width,
                                            float miter_limit) const;

  bool IsRect() const;
  void Transform(const CFX_Matrix& matrix);

  void Append(const CFX_Path& path, const CFX_Matrix* pMatrix);
  void AppendFloatRect(const CFX_FloatRect& rect);
  void AppendRect(float left, float bottom, float right, float top);
  void AppendPoint(const CFX_PointF& point, CFX_Path::Point::Type type);
  void AppendPointAndClose(const CFX_PointF& point, CFX_Path::Point::Type type);

  // TODO(tsepez): Remove when all access thru this class.
  const CFX_Path* GetObject() const { return m_Ref.GetObject(); }

 private:
  SharedCopyOnWrite<CFX_RetainablePath> m_Ref;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_PATH_H_
