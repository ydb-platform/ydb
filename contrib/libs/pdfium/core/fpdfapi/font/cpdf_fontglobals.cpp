// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/font/cpdf_fontglobals.h"

#include <utility>

#include "core/fpdfapi/cmaps/CNS1/cmaps_cns1.h"
#include "core/fpdfapi/cmaps/GB1/cmaps_gb1.h"
#include "core/fpdfapi/cmaps/Japan1/cmaps_japan1.h"
#include "core/fpdfapi/cmaps/Korea1/cmaps_korea1.h"
#include "core/fpdfapi/font/cfx_stockfontarray.h"
#include "core/fpdfapi/font/cpdf_cid2unicodemap.h"
#include "core/fpdfapi/font/cpdf_cmap.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"

namespace {

CPDF_FontGlobals* g_FontGlobals = nullptr;

RetainPtr<const CPDF_CMap> LoadPredefinedCMap(ByteStringView name) {
  if (!name.IsEmpty() && name[0] == '/')
    name = name.Last(name.GetLength() - 1);
  return pdfium::MakeRetain<CPDF_CMap>(name);
}

}  // namespace

// static
void CPDF_FontGlobals::Create() {
  DCHECK(!g_FontGlobals);
  g_FontGlobals = new CPDF_FontGlobals();
}

// static
void CPDF_FontGlobals::Destroy() {
  DCHECK(g_FontGlobals);
  delete g_FontGlobals;
  g_FontGlobals = nullptr;
}

// static
CPDF_FontGlobals* CPDF_FontGlobals::GetInstance() {
  DCHECK(g_FontGlobals);
  return g_FontGlobals;
}

CPDF_FontGlobals::CPDF_FontGlobals() = default;

CPDF_FontGlobals::~CPDF_FontGlobals() = default;

void CPDF_FontGlobals::LoadEmbeddedMaps() {
  LoadEmbeddedGB1CMaps();
  LoadEmbeddedCNS1CMaps();
  LoadEmbeddedJapan1CMaps();
  LoadEmbeddedKorea1CMaps();
}

RetainPtr<CPDF_Font> CPDF_FontGlobals::Find(
    CPDF_Document* pDoc,
    CFX_FontMapper::StandardFont index) {
  auto it = m_StockMap.find(pDoc);
  if (it == m_StockMap.end() || !it->second)
    return nullptr;

  return it->second->GetFont(index);
}

void CPDF_FontGlobals::Set(CPDF_Document* pDoc,
                           CFX_FontMapper::StandardFont index,
                           RetainPtr<CPDF_Font> pFont) {
  UnownedPtr<CPDF_Document> pKey(pDoc);
  if (!pdfium::Contains(m_StockMap, pKey))
    m_StockMap[pKey] = std::make_unique<CFX_StockFontArray>();
  m_StockMap[pKey]->SetFont(index, std::move(pFont));
}

void CPDF_FontGlobals::Clear(CPDF_Document* pDoc) {
  // Avoid constructing smart-pointer key as erase() doesn't invoke
  // transparent lookup in the same way find() does.
  auto it = m_StockMap.find(pDoc);
  if (it != m_StockMap.end())
    m_StockMap.erase(it);
}

void CPDF_FontGlobals::LoadEmbeddedGB1CMaps() {
  SetEmbeddedCharset(CIDSET_GB1, fxcmap::kGB1_cmaps_span);
  SetEmbeddedToUnicode(CIDSET_GB1, fxcmap::kGB1CID2Unicode_5);
}

void CPDF_FontGlobals::LoadEmbeddedCNS1CMaps() {
  SetEmbeddedCharset(CIDSET_CNS1, fxcmap::kCNS1_cmaps_span);
  SetEmbeddedToUnicode(CIDSET_CNS1, fxcmap::kCNS1CID2Unicode_5);
}

void CPDF_FontGlobals::LoadEmbeddedJapan1CMaps() {
  SetEmbeddedCharset(CIDSET_JAPAN1, fxcmap::kJapan1_cmaps_span);
  SetEmbeddedToUnicode(CIDSET_JAPAN1, fxcmap::kJapan1CID2Unicode_4);
}

void CPDF_FontGlobals::LoadEmbeddedKorea1CMaps() {
  SetEmbeddedCharset(CIDSET_KOREA1, fxcmap::kKorea1_cmaps_span);
  SetEmbeddedToUnicode(CIDSET_KOREA1, fxcmap::kKorea1CID2Unicode_2);
}

RetainPtr<const CPDF_CMap> CPDF_FontGlobals::GetPredefinedCMap(
    const ByteString& name) {
  auto it = m_CMaps.find(name);
  if (it != m_CMaps.end())
    return it->second;

  RetainPtr<const CPDF_CMap> pCMap = LoadPredefinedCMap(name.AsStringView());
  if (!name.IsEmpty())
    m_CMaps[name] = pCMap;

  return pCMap;
}

CPDF_CID2UnicodeMap* CPDF_FontGlobals::GetCID2UnicodeMap(CIDSet charset) {
  if (!m_CID2UnicodeMaps[charset]) {
    m_CID2UnicodeMaps[charset] = std::make_unique<CPDF_CID2UnicodeMap>(charset);
  }
  return m_CID2UnicodeMaps[charset].get();
}
