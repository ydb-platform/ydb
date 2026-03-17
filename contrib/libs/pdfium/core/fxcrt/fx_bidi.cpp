// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_bidi.h"

#include <algorithm>

#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_unicode.h"

CFX_BidiChar::CFX_BidiChar()
    : m_CurrentSegment({0, 0, Direction::kNeutral}),
      m_LastSegment({0, 0, Direction::kNeutral}) {}

bool CFX_BidiChar::AppendChar(wchar_t wch) {
  Direction direction;
  switch (pdfium::unicode::GetBidiClass(wch)) {
    case FX_BIDICLASS::kL:
      direction = Direction::kLeft;
      break;
    case FX_BIDICLASS::kAN:
    case FX_BIDICLASS::kEN:
    case FX_BIDICLASS::kNSM:
    case FX_BIDICLASS::kCS:
    case FX_BIDICLASS::kES:
    case FX_BIDICLASS::kET:
    case FX_BIDICLASS::kBN:
      direction = Direction::kLeftWeak;
      break;
    case FX_BIDICLASS::kR:
    case FX_BIDICLASS::kAL:
      direction = Direction::kRight;
      break;
    default:
      direction = Direction::kNeutral;
      break;
  }

  bool bChangeDirection = (direction != m_CurrentSegment.direction);
  if (bChangeDirection)
    StartNewSegment(direction);

  m_CurrentSegment.count++;
  return bChangeDirection;
}

bool CFX_BidiChar::EndChar() {
  StartNewSegment(Direction::kNeutral);
  return m_LastSegment.count > 0;
}

void CFX_BidiChar::StartNewSegment(CFX_BidiChar::Direction direction) {
  m_LastSegment = m_CurrentSegment;
  m_CurrentSegment.start += m_CurrentSegment.count;
  m_CurrentSegment.count = 0;
  m_CurrentSegment.direction = direction;
}

CFX_BidiString::CFX_BidiString(const WideString& str) : m_Str(str) {
  CFX_BidiChar bidi;
  for (wchar_t c : m_Str) {
    if (bidi.AppendChar(c))
      m_Order.push_back(bidi.GetSegmentInfo());
  }
  if (bidi.EndChar())
    m_Order.push_back(bidi.GetSegmentInfo());

  size_t nR2L = std::count_if(
      m_Order.begin(), m_Order.end(), [](const CFX_BidiChar::Segment& seg) {
        return seg.direction == CFX_BidiChar::Direction::kRight;
      });

  size_t nL2R = std::count_if(
      m_Order.begin(), m_Order.end(), [](const CFX_BidiChar::Segment& seg) {
        return seg.direction == CFX_BidiChar::Direction::kLeft;
      });

  if (nR2L > 0 && nR2L >= nL2R)
    SetOverallDirectionRight();
}

CFX_BidiString::~CFX_BidiString() = default;

CFX_BidiChar::Direction CFX_BidiString::OverallDirection() const {
  DCHECK_NE(m_eOverallDirection, CFX_BidiChar::Direction::kNeutral);
  DCHECK_NE(m_eOverallDirection, CFX_BidiChar::Direction::kLeftWeak);
  return m_eOverallDirection;
}

void CFX_BidiString::SetOverallDirectionRight() {
  if (m_eOverallDirection != CFX_BidiChar::Direction::kRight) {
    std::reverse(m_Order.begin(), m_Order.end());
    m_eOverallDirection = CFX_BidiChar::Direction::kRight;
  }
}
