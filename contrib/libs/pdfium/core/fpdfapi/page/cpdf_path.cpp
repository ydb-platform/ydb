// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_path.h"

CPDF_Path::CPDF_Path() = default;

CPDF_Path::CPDF_Path(const CPDF_Path& that) = default;

CPDF_Path::~CPDF_Path() = default;

const std::vector<CFX_Path::Point>& CPDF_Path::GetPoints() const {
  return m_Ref.GetObject()->GetPoints();
}

void CPDF_Path::ClosePath() {
  m_Ref.GetPrivateCopy()->ClosePath();
}

CFX_PointF CPDF_Path::GetPoint(int index) const {
  return m_Ref.GetObject()->GetPoint(index);
}

CFX_FloatRect CPDF_Path::GetBoundingBox() const {
  return m_Ref.GetObject()->GetBoundingBox();
}

CFX_FloatRect CPDF_Path::GetBoundingBoxForStrokePath(float line_width,
                                                     float miter_limit) const {
  return m_Ref.GetObject()->GetBoundingBoxForStrokePath(line_width,
                                                        miter_limit);
}

bool CPDF_Path::IsRect() const {
  return m_Ref.GetObject()->IsRect();
}

void CPDF_Path::Transform(const CFX_Matrix& matrix) {
  m_Ref.GetPrivateCopy()->Transform(matrix);
}

void CPDF_Path::Append(const CFX_Path& path, const CFX_Matrix* pMatrix) {
  m_Ref.GetPrivateCopy()->Append(path, pMatrix);
}

void CPDF_Path::AppendFloatRect(const CFX_FloatRect& rect) {
  m_Ref.GetPrivateCopy()->AppendFloatRect(rect);
}

void CPDF_Path::AppendRect(float left, float bottom, float right, float top) {
  m_Ref.GetPrivateCopy()->AppendRect(left, bottom, right, top);
}

void CPDF_Path::AppendPoint(const CFX_PointF& point,
                            CFX_Path::Point::Type type) {
  CFX_Path data;
  data.AppendPoint(point, type);
  Append(data, nullptr);
}

void CPDF_Path::AppendPointAndClose(const CFX_PointF& point,
                                    CFX_Path::Point::Type type) {
  CFX_Path data;
  data.AppendPointAndClose(point, type);
  Append(data, nullptr);
}
