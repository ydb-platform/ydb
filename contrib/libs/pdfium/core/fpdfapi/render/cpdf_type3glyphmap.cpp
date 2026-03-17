// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/render/cpdf_type3glyphmap.h"

#include <math.h>

#include <algorithm>
#include <utility>

#include "core/fxcrt/fx_system.h"
#include "core/fxge/cfx_glyphbitmap.h"
#include "core/fxge/fx_font.h"

namespace {

constexpr int kType3MaxBlues = 16;

int AdjustBlueHelper(float pos, std::vector<int>* blues) {
  float min_distance = 1000000.0f;
  int closest_pos = -1;
  for (int i = 0; i < static_cast<int>(blues->size()); ++i) {
    float distance = fabs(pos - static_cast<float>(blues->at(i)));
    if (distance < std::min(0.8f, min_distance)) {
      min_distance = distance;
      closest_pos = i;
    }
  }
  if (closest_pos >= 0)
    return blues->at(closest_pos);
  int new_pos = FXSYS_roundf(pos);
  if (blues->size() < kType3MaxBlues)
    blues->push_back(new_pos);
  return new_pos;
}

}  // namespace

CPDF_Type3GlyphMap::CPDF_Type3GlyphMap() = default;

CPDF_Type3GlyphMap::~CPDF_Type3GlyphMap() = default;

std::pair<int, int> CPDF_Type3GlyphMap::AdjustBlue(float top, float bottom) {
  return std::make_pair(AdjustBlueHelper(top, &m_TopBlue),
                        AdjustBlueHelper(bottom, &m_BottomBlue));
}

const CFX_GlyphBitmap* CPDF_Type3GlyphMap::GetBitmap(uint32_t charcode) const {
  auto it = m_GlyphMap.find(charcode);
  return it != m_GlyphMap.end() ? it->second.get() : nullptr;
}

void CPDF_Type3GlyphMap::SetBitmap(uint32_t charcode,
                                   std::unique_ptr<CFX_GlyphBitmap> pMap) {
  m_GlyphMap[charcode] = std::move(pMap);
}
