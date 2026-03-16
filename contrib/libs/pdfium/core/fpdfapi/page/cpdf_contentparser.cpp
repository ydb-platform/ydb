// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_contentparser.h"

#include <utility>

#include "constants/page_object.h"
#include "core/fpdfapi/font/cpdf_type3char.h"
#include "core/fpdfapi/page/cpdf_allstates.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/page/cpdf_path.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/pauseindicator_iface.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_fillrenderoptions.h"

CPDF_ContentParser::CPDF_ContentParser(CPDF_Page* pPage)
    : m_CurrentStage(Stage::kGetContent), m_pPageObjectHolder(pPage) {
  DCHECK(pPage);
  if (!pPage->GetDocument()) {
    m_CurrentStage = Stage::kComplete;
    return;
  }

  RetainPtr<CPDF_Object> pContent =
      pPage->GetMutableDict()->GetMutableDirectObjectFor(
          pdfium::page_object::kContents);
  if (!pContent) {
    HandlePageContentFailure();
    return;
  }

  const CPDF_Stream* pStream = pContent->AsStream();
  if (pStream) {
    HandlePageContentStream(pStream);
    return;
  }

  const CPDF_Array* pArray = pContent->AsArray();
  if (pArray && HandlePageContentArray(pArray))
    return;

  HandlePageContentFailure();
}

CPDF_ContentParser::CPDF_ContentParser(
    RetainPtr<const CPDF_Stream> pStream,
    CPDF_PageObjectHolder* pPageObjectHolder,
    const CPDF_AllStates* pGraphicStates,
    const CFX_Matrix* pParentMatrix,
    CPDF_Type3Char* pType3Char,
    CPDF_Form::RecursionState* recursion_state)
    : m_CurrentStage(Stage::kParse),
      m_pPageObjectHolder(pPageObjectHolder),
      m_pType3Char(pType3Char) {
  DCHECK(m_pPageObjectHolder);
  CFX_Matrix form_matrix =
      m_pPageObjectHolder->GetDict()->GetMatrixFor("Matrix");
  if (pGraphicStates)
    form_matrix.Concat(pGraphicStates->current_transformation_matrix());

  RetainPtr<const CPDF_Array> pBBox =
      m_pPageObjectHolder->GetDict()->GetArrayFor("BBox");
  CFX_FloatRect form_bbox;
  CPDF_Path ClipPath;
  if (pBBox) {
    form_bbox = pBBox->GetRect();
    ClipPath.Emplace();
    ClipPath.AppendFloatRect(form_bbox);
    ClipPath.Transform(form_matrix);
    if (pParentMatrix)
      ClipPath.Transform(*pParentMatrix);

    form_bbox = form_matrix.TransformRect(form_bbox);
    if (pParentMatrix)
      form_bbox = pParentMatrix->TransformRect(form_bbox);
  }

  RetainPtr<CPDF_Dictionary> pResources =
      m_pPageObjectHolder->GetMutableDict()->GetMutableDictFor("Resources");
  m_pParser = std::make_unique<CPDF_StreamContentParser>(
      m_pPageObjectHolder->GetDocument(),
      m_pPageObjectHolder->GetMutablePageResources(),
      m_pPageObjectHolder->GetMutableResources(), pParentMatrix,
      m_pPageObjectHolder, std::move(pResources), form_bbox, pGraphicStates,
      recursion_state);
  m_pParser->GetCurStates()->set_current_transformation_matrix(form_matrix);
  m_pParser->GetCurStates()->set_parent_matrix(form_matrix);
  if (ClipPath.HasRef()) {
    m_pParser->GetCurStates()->mutable_clip_path().AppendPathWithAutoMerge(
        ClipPath, CFX_FillRenderOptions::FillType::kWinding);
  }
  if (m_pPageObjectHolder->GetTransparency().IsGroup()) {
    CPDF_GeneralState& state =
        m_pParser->GetCurStates()->mutable_general_state();
    state.SetBlendType(BlendMode::kNormal);
    state.SetStrokeAlpha(1.0f);
    state.SetFillAlpha(1.0f);
    state.SetSoftMask(nullptr);
  }
  m_pSingleStream = pdfium::MakeRetain<CPDF_StreamAcc>(std::move(pStream));
  m_pSingleStream->LoadAllDataFiltered();
  m_Data = m_pSingleStream->GetSpan();
}

CPDF_ContentParser::~CPDF_ContentParser() = default;

CPDF_PageObjectHolder::CTMMap CPDF_ContentParser::TakeAllCTMs() {
  return m_pParser ? m_pParser->TakeAllCTMs() : CPDF_PageObjectHolder::CTMMap();
}

// Returning |true| means that there is more content to be processed and
// Continue() should be called again. Returning |false| means that we've
// completed the parse and Continue() is complete.
bool CPDF_ContentParser::Continue(PauseIndicatorIface* pPause) {
  while (m_CurrentStage == Stage::kGetContent) {
    m_CurrentStage = GetContent();
    if (pPause && pPause->NeedToPauseNow())
      return true;
  }

  if (m_CurrentStage == Stage::kPrepareContent)
    m_CurrentStage = PrepareContent();

  while (m_CurrentStage == Stage::kParse) {
    m_CurrentStage = Parse();
    if (pPause && pPause->NeedToPauseNow())
      return true;
  }

  if (m_CurrentStage == Stage::kCheckClip)
    m_CurrentStage = CheckClip();

  DCHECK_EQ(m_CurrentStage, Stage::kComplete);
  return false;
}

CPDF_ContentParser::Stage CPDF_ContentParser::GetContent() {
  DCHECK_EQ(m_CurrentStage, Stage::kGetContent);
  DCHECK(m_pPageObjectHolder->IsPage());
  RetainPtr<const CPDF_Array> pContent =
      m_pPageObjectHolder->GetDict()->GetArrayFor(
          pdfium::page_object::kContents);
  RetainPtr<const CPDF_Stream> pStreamObj = ToStream(
      pContent ? pContent->GetDirectObjectAt(m_CurrentOffset) : nullptr);
  m_StreamArray[m_CurrentOffset] =
      pdfium::MakeRetain<CPDF_StreamAcc>(std::move(pStreamObj));
  m_StreamArray[m_CurrentOffset]->LoadAllDataFiltered();
  m_CurrentOffset++;

  return m_CurrentOffset == m_nStreams ? Stage::kPrepareContent
                                       : Stage::kGetContent;
}

CPDF_ContentParser::Stage CPDF_ContentParser::PrepareContent() {
  m_CurrentOffset = 0;

  if (m_StreamArray.empty()) {
    m_Data = m_pSingleStream->GetSpan();
    return Stage::kParse;
  }

  FX_SAFE_UINT32 safe_size = 0;
  for (const auto& stream : m_StreamArray) {
    m_StreamSegmentOffsets.push_back(safe_size.ValueOrDie());
    safe_size += stream->GetSize();
    safe_size += 1;
    if (!safe_size.IsValid())
      return Stage::kComplete;
  }

  const size_t buffer_size = safe_size.ValueOrDie();
  auto buffer = FixedSizeDataVector<uint8_t>::TryZeroed(buffer_size);
  if (buffer.empty()) {
    m_Data.emplace<pdfium::raw_span<const uint8_t>>();
    return Stage::kComplete;
  }

  auto data_span = buffer.span();
  for (const auto& stream : m_StreamArray) {
    data_span = fxcrt::spancpy(data_span, stream->GetSpan());
    data_span.front() = ' ';
    data_span = data_span.subspan(1);
  }
  m_StreamArray.clear();
  m_Data = std::move(buffer);
  return Stage::kParse;
}

CPDF_ContentParser::Stage CPDF_ContentParser::Parse() {
  if (!m_pParser) {
    m_RecursionState.parsed_set.clear();
    m_pParser = std::make_unique<CPDF_StreamContentParser>(
        m_pPageObjectHolder->GetDocument(),
        m_pPageObjectHolder->GetMutablePageResources(), nullptr, nullptr,
        m_pPageObjectHolder, m_pPageObjectHolder->GetMutableResources(),
        m_pPageObjectHolder->GetBBox(), nullptr, &m_RecursionState);
    m_pParser->GetCurStates()->mutable_color_state().SetDefault();
  }
  if (m_CurrentOffset >= GetData().size())
    return Stage::kCheckClip;

  if (m_StreamSegmentOffsets.empty())
    m_StreamSegmentOffsets.push_back(0);

  static constexpr uint32_t kParseStepLimit = 100;
  m_CurrentOffset += m_pParser->Parse(GetData(), m_CurrentOffset,
                                      kParseStepLimit, m_StreamSegmentOffsets);
  return Stage::kParse;
}

CPDF_ContentParser::Stage CPDF_ContentParser::CheckClip() {
  if (m_pType3Char) {
    m_pType3Char->InitializeFromStreamData(m_pParser->IsColored(),
                                           m_pParser->GetType3Data());
  }

  for (auto& pObj : *m_pPageObjectHolder) {
    CPDF_ClipPath& clip_path = pObj->mutable_clip_path();
    if (!clip_path.HasRef()) {
      continue;
    }
    if (clip_path.GetPathCount() != 1) {
      continue;
    }
    if (clip_path.GetTextCount() > 0) {
      continue;
    }

    CPDF_Path path = clip_path.GetPath(0);
    if (!path.IsRect() || pObj->IsShading()) {
      continue;
    }

    CFX_PointF point0 = path.GetPoint(0);
    CFX_PointF point2 = path.GetPoint(2);
    CFX_FloatRect old_rect(point0.x, point0.y, point2.x, point2.y);
    if (old_rect.Contains(pObj->GetRect()))
      clip_path.SetNull();
  }
  return Stage::kComplete;
}

void CPDF_ContentParser::HandlePageContentStream(const CPDF_Stream* pStream) {
  m_pSingleStream =
      pdfium::MakeRetain<CPDF_StreamAcc>(pdfium::WrapRetain(pStream));
  m_pSingleStream->LoadAllDataFiltered();
  m_CurrentStage = Stage::kPrepareContent;
}

bool CPDF_ContentParser::HandlePageContentArray(const CPDF_Array* pArray) {
  m_nStreams = fxcrt::CollectionSize<uint32_t>(*pArray);
  if (m_nStreams == 0)
    return false;

  m_StreamArray.resize(m_nStreams);
  return true;
}

void CPDF_ContentParser::HandlePageContentFailure() {
  m_CurrentStage = Stage::kComplete;
}

pdfium::span<const uint8_t> CPDF_ContentParser::GetData() const {
  if (is_owned()) {
    return absl::get<FixedSizeDataVector<uint8_t>>(m_Data).span();
  }
  return absl::get<pdfium::raw_span<const uint8_t>>(m_Data);
}
