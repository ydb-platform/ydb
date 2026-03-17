// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fpdfapi/page/cpdf_indexedcs.h"

#include <set>

#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

CPDF_IndexedCS::CPDF_IndexedCS() : CPDF_BasedCS(Family::kIndexed) {}

CPDF_IndexedCS::~CPDF_IndexedCS() = default;

const CPDF_IndexedCS* CPDF_IndexedCS::AsIndexedCS() const {
  return this;
}

uint32_t CPDF_IndexedCS::v_Load(CPDF_Document* pDoc,
                                const CPDF_Array* pArray,
                                std::set<const CPDF_Object*>* pVisited) {
  if (pArray->size() < 4) {
    return 0;
  }

  RetainPtr<const CPDF_Object> pBaseObj = pArray->GetDirectObjectAt(1);
  if (HasSameArray(pBaseObj.Get())) {
    return 0;
  }

  auto* pDocPageData = CPDF_DocPageData::FromDocument(pDoc);
  m_pBaseCS =
      pDocPageData->GetColorSpaceGuarded(pBaseObj.Get(), nullptr, pVisited);
  if (!m_pBaseCS) {
    return 0;
  }

  // The base color space cannot be a Pattern or Indexed space, according to ISO
  // 32000-1:2008 section 8.6.6.3.
  Family family = m_pBaseCS->GetFamily();
  if (family == Family::kIndexed || family == Family::kPattern) {
    return 0;
  }

  uint32_t base_component_count = m_pBaseCS->ComponentCount();
  DCHECK(base_component_count);
  component_min_max_ = DataVector<IndexedColorMinMax>(base_component_count);
  float defvalue;
  for (uint32_t i = 0; i < component_min_max_.size(); i++) {
    IndexedColorMinMax& comp = component_min_max_[i];
    m_pBaseCS->GetDefaultValue(i, &defvalue, &comp.min, &comp.max);
    comp.max -= comp.min;
  }

  // ISO 32000-1:2008 section 8.6.6.3 says the maximum value is 255.
  max_index_ = pArray->GetIntegerAt(2);
  if (max_index_ < 0 || max_index_ > 255) {
    return 0;
  }

  RetainPtr<const CPDF_Object> pTableObj = pArray->GetDirectObjectAt(3);
  if (!pTableObj) {
    return 0;
  }

  if (const CPDF_String* str_obj = pTableObj->AsString()) {
    ByteString str_data = str_obj->GetString();
    pdfium::span<const uint8_t> str_span = str_data.unsigned_span();
    lookup_table_ = DataVector<uint8_t>(str_span.begin(), str_span.end());
  } else if (const CPDF_Stream* stream_obj = pTableObj->AsStream()) {
    auto acc =
        pdfium::MakeRetain<CPDF_StreamAcc>(pdfium::WrapRetain(stream_obj));
    acc->LoadAllDataFiltered();
    pdfium::span<const uint8_t> str_span = acc->GetSpan();
    lookup_table_ = DataVector<uint8_t>(str_span.begin(), str_span.end());
  }
  return 1;
}

std::optional<FX_RGB_STRUCT<float>> CPDF_IndexedCS::GetRGB(
    pdfium::span<const float> pBuf) const {
  int32_t index = static_cast<int32_t>(pBuf[0]);
  if (index < 0 || index > max_index_) {
    return std::nullopt;
  }

  DCHECK(!component_min_max_.empty());
  DCHECK_EQ(component_min_max_.size(), m_pBaseCS->ComponentCount());

  FX_SAFE_SIZE_T length = index;
  length += 1;
  length *= component_min_max_.size();
  if (!length.IsValid() || length.ValueOrDie() > lookup_table_.size()) {
    return std::nullopt;
  }

  DataVector<float> comps(component_min_max_.size());
  for (uint32_t i = 0; i < component_min_max_.size(); ++i) {
    const IndexedColorMinMax& comp = component_min_max_[i];
    comps[i] =
        comp.min +
        comp.max * lookup_table_[index * component_min_max_.size() + i] / 255;
  }
  return m_pBaseCS->GetRGB(comps);
}
