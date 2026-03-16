// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_pageobjectholder.h"

#include <algorithm>
#include <utility>

#include "constants/transparency.h"
#include "core/fpdfapi/page/cpdf_allstates.h"
#include "core/fpdfapi/page/cpdf_contentparser.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/stl_util.h"

bool GraphicsData::operator<(const GraphicsData& other) const {
  if (!FXSYS_SafeEQ(fillAlpha, other.fillAlpha))
    return FXSYS_SafeLT(fillAlpha, other.fillAlpha);
  if (!FXSYS_SafeEQ(strokeAlpha, other.strokeAlpha))
    return FXSYS_SafeLT(strokeAlpha, other.strokeAlpha);
  return blendType < other.blendType;
}

bool FontData::operator<(const FontData& other) const {
  if (baseFont != other.baseFont)
    return baseFont < other.baseFont;
  return type < other.type;
}

CPDF_PageObjectHolder::CPDF_PageObjectHolder(
    CPDF_Document* pDoc,
    RetainPtr<CPDF_Dictionary> pDict,
    RetainPtr<CPDF_Dictionary> pPageResources,
    RetainPtr<CPDF_Dictionary> pResources)
    : m_pPageResources(std::move(pPageResources)),
      m_pResources(std::move(pResources)),
      m_pDict(std::move(pDict)),
      m_pDocument(pDoc) {
  DCHECK(m_pDict);
}

CPDF_PageObjectHolder::~CPDF_PageObjectHolder() = default;

bool CPDF_PageObjectHolder::IsPage() const {
  return false;
}

void CPDF_PageObjectHolder::StartParse(
    std::unique_ptr<CPDF_ContentParser> pParser) {
  DCHECK_EQ(m_ParseState, ParseState::kNotParsed);
  m_pParser = std::move(pParser);
  m_ParseState = ParseState::kParsing;
}

void CPDF_PageObjectHolder::ContinueParse(PauseIndicatorIface* pPause) {
  if (m_ParseState == ParseState::kParsed)
    return;

  DCHECK_EQ(m_ParseState, ParseState::kParsing);
  if (m_pParser->Continue(pPause))
    return;

  m_ParseState = ParseState::kParsed;
  m_pDocument->IncrementParsedPageCount();
  m_AllCTMs = m_pParser->TakeAllCTMs();

  m_pParser.reset();
}

void CPDF_PageObjectHolder::AddImageMaskBoundingBox(const CFX_FloatRect& box) {
  m_MaskBoundingBoxes.push_back(box);
}

std::set<int32_t> CPDF_PageObjectHolder::TakeDirtyStreams() {
  auto dirty_streams = std::move(m_DirtyStreams);
  m_DirtyStreams.clear();
  return dirty_streams;
}

std::optional<ByteString> CPDF_PageObjectHolder::GraphicsMapSearch(
    const GraphicsData& gd) {
  auto it = m_GraphicsMap.find(gd);
  if (it == m_GraphicsMap.end())
    return std::nullopt;

  return it->second;
}

void CPDF_PageObjectHolder::GraphicsMapInsert(const GraphicsData& gd,
                                              const ByteString& str) {
  m_GraphicsMap[gd] = str;
}

std::optional<ByteString> CPDF_PageObjectHolder::FontsMapSearch(
    const FontData& fd) {
  auto it = m_FontsMap.find(fd);
  if (it == m_FontsMap.end())
    return std::nullopt;

  return it->second;
}

void CPDF_PageObjectHolder::FontsMapInsert(const FontData& fd,
                                           const ByteString& str) {
  m_FontsMap[fd] = str;
}

CFX_Matrix CPDF_PageObjectHolder::GetCTMAtBeginningOfStream(int32_t stream) {
  CHECK(stream >= 0 || stream == CPDF_PageObject::kNoContentStream);

  if (stream == 0 || m_AllCTMs.empty()) {
    return CFX_Matrix();
  }

  if (stream == CPDF_PageObject::kNoContentStream) {
    return m_AllCTMs.rbegin()->second;
  }

  // For all other cases, CTM at beginning of `stream` is the same value as CTM
  // at the end of the previous stream.
  return GetCTMAtEndOfStream(stream - 1);
}

CFX_Matrix CPDF_PageObjectHolder::GetCTMAtEndOfStream(int32_t stream) {
  // This code should never need to calculate the CTM for the end of
  // `CPDF_PageObject::kNoContentStream`, which uses a negative sentinel value.
  // All other streams have a non-negative index.
  CHECK_GE(stream, 0);

  if (m_AllCTMs.empty()) {
    return CFX_Matrix();
  }

  const auto it = m_AllCTMs.lower_bound(stream);
  return it != m_AllCTMs.end() ? it->second : m_AllCTMs.rbegin()->second;
}

void CPDF_PageObjectHolder::LoadTransparencyInfo() {
  RetainPtr<const CPDF_Dictionary> pGroup = m_pDict->GetDictFor("Group");
  if (!pGroup)
    return;

  if (pGroup->GetByteStringFor(pdfium::transparency::kGroupSubType) !=
      pdfium::transparency::kTransparency) {
    return;
  }
  m_Transparency.SetGroup();
  if (pGroup->GetIntegerFor(pdfium::transparency::kI))
    m_Transparency.SetIsolated();
}

CPDF_PageObject* CPDF_PageObjectHolder::GetPageObjectByIndex(
    size_t index) const {
  return fxcrt::IndexInBounds(m_PageObjectList, index)
             ? m_PageObjectList[index].get()
             : nullptr;
}

void CPDF_PageObjectHolder::AppendPageObject(
    std::unique_ptr<CPDF_PageObject> pPageObj) {
  CHECK(pPageObj);
  m_PageObjectList.push_back(std::move(pPageObj));
}

std::unique_ptr<CPDF_PageObject> CPDF_PageObjectHolder::RemovePageObject(
    CPDF_PageObject* pPageObj) {
  auto it = std::find(std::begin(m_PageObjectList), std::end(m_PageObjectList),
                      fxcrt::MakeFakeUniquePtr(pPageObj));
  if (it == std::end(m_PageObjectList))
    return nullptr;

  std::unique_ptr<CPDF_PageObject> result = std::move(*it);
  m_PageObjectList.erase(it);

  int32_t content_stream = pPageObj->GetContentStream();
  if (content_stream >= 0)
    m_DirtyStreams.insert(content_stream);

  return result;
}

bool CPDF_PageObjectHolder::ErasePageObjectAtIndex(size_t index) {
  if (index >= m_PageObjectList.size())
    return false;

  m_PageObjectList.erase(m_PageObjectList.begin() + index);
  return true;
}
