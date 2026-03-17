// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/cmaps/fpdf_cmaps.h"

#include <algorithm>

#include "core/fxcrt/check.h"
#include "core/fxcrt/span.h"

namespace fxcmap {

namespace {

struct SingleCmap {
  uint16_t code;
  uint16_t cid;
};

struct RangeCmap {
  uint16_t low;
  uint16_t high;
  uint16_t cid;
};

const CMap* FindNextCMap(const CMap* pMap) {
  return pMap->m_UseOffset ? UNSAFE_TODO(pMap + pMap->m_UseOffset) : nullptr;
}

}  // namespace

uint16_t CIDFromCharCode(const CMap* pMap, uint32_t charcode) {
  DCHECK(pMap);
  const uint16_t loword = static_cast<uint16_t>(charcode);
  if (charcode >> 16) {
    while (pMap) {
      if (pMap->m_pDWordMap) {
        const DWordCIDMap* begin = pMap->m_pDWordMap;
        const auto* end = UNSAFE_TODO(begin + pMap->m_DWordCount);
        const auto* found = std::lower_bound(
            begin, end, charcode,
            [](const DWordCIDMap& element, uint32_t charcode) {
              uint16_t hiword = static_cast<uint16_t>(charcode >> 16);
              if (element.m_HiWord != hiword)
                return element.m_HiWord < hiword;
              return element.m_LoWordHigh < static_cast<uint16_t>(charcode);
            });
        if (found != end && loword >= found->m_LoWordLow &&
            loword <= found->m_LoWordHigh) {
          return found->m_CID + loword - found->m_LoWordLow;
        }
      }
      pMap = FindNextCMap(pMap);
    }
    return 0;
  }

  while (pMap && pMap->m_pWordMap) {
    switch (pMap->m_WordMapType) {
      case CMap::Type::kSingle: {
        const auto* begin =
            reinterpret_cast<const SingleCmap*>(pMap->m_pWordMap);
        const auto* end = UNSAFE_TODO(begin + pMap->m_WordCount);
        const auto* found = std::lower_bound(
            begin, end, loword, [](const SingleCmap& element, uint16_t code) {
              return element.code < code;
            });
        if (found != end && found->code == loword)
          return found->cid;
        break;
      }
      case CMap::Type::kRange: {
        const auto* begin =
            reinterpret_cast<const RangeCmap*>(pMap->m_pWordMap);
        const auto* end = UNSAFE_TODO(begin + pMap->m_WordCount);
        const auto* found = std::lower_bound(
            begin, end, loword, [](const RangeCmap& element, uint16_t code) {
              return element.high < code;
            });
        if (found != end && loword >= found->low && loword <= found->high)
          return found->cid + loword - found->low;
        break;
      }
    }
    pMap = FindNextCMap(pMap);
  }

  return 0;
}

uint32_t CharCodeFromCID(const CMap* pMap, uint16_t cid) {
  // TODO(dsinclair): This should be checking both pMap->m_WordMap and
  // pMap->m_DWordMap. There was a second while() but it was never reached as
  // the first always returns. Investigate and determine how this should
  // really be working. (https://codereview.chromium.org/2235743003 removed the
  // second while loop.)
  DCHECK(pMap);
  while (pMap) {
    switch (pMap->m_WordMapType) {
      case CMap::Type::kSingle: {
        auto single_span = UNSAFE_TODO(pdfium::make_span(
            reinterpret_cast<const SingleCmap*>(pMap->m_pWordMap),
            pMap->m_WordCount));
        for (const auto& single : single_span) {
          if (single.cid == cid) {
            return single.code;
          }
        }
        break;
      }
      case CMap::Type::kRange: {
        auto range_span = UNSAFE_TODO(pdfium::make_span(
            reinterpret_cast<const RangeCmap*>(pMap->m_pWordMap),
            pMap->m_WordCount));
        for (const auto& range : range_span) {
          if (cid >= range.cid && cid <= range.cid + range.high - range.low) {
            return range.low + cid - range.cid;
          }
        }
        break;
      }
    }
    pMap = FindNextCMap(pMap);
  }
  return 0;
}

}  // namespace fxcmap
