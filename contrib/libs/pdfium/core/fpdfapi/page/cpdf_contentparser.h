// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PAGE_CPDF_CONTENTPARSER_H_
#define CORE_FPDFAPI_PAGE_CPDF_CONTENTPARSER_H_

#include <stdint.h>

#include <memory>
#include <vector>

#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_pageobjectholder.h"
#include "core/fpdfapi/page/cpdf_streamcontentparser.h"
#include "core/fxcrt/fixed_size_data_vector.h"
#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include <absl/types/variant.h>

class CPDF_AllStates;
class CPDF_Array;
class CPDF_Page;
class CPDF_PageObjectHolder;
class CPDF_Stream;
class CPDF_StreamAcc;
class CPDF_Type3Char;
class PauseIndicatorIface;

class CPDF_ContentParser {
 public:
  explicit CPDF_ContentParser(CPDF_Page* pPage);
  CPDF_ContentParser(RetainPtr<const CPDF_Stream> pStream,
                     CPDF_PageObjectHolder* pPageObjectHolder,
                     const CPDF_AllStates* pGraphicStates,
                     const CFX_Matrix* pParentMatrix,
                     CPDF_Type3Char* pType3Char,
                     CPDF_Form::RecursionState* recursion_state);
  ~CPDF_ContentParser();

  CPDF_PageObjectHolder::CTMMap TakeAllCTMs();

  // Returns whether to continue or not.
  bool Continue(PauseIndicatorIface* pPause);

 private:
  enum class Stage : uint8_t {
    kGetContent = 1,
    kPrepareContent,
    kParse,
    kCheckClip,
    kComplete,
  };

  Stage GetContent();
  Stage PrepareContent();
  Stage Parse();
  Stage CheckClip();

  void HandlePageContentStream(const CPDF_Stream* pStream);
  bool HandlePageContentArray(const CPDF_Array* pArray);
  void HandlePageContentFailure();

  bool is_owned() const {
    return absl::holds_alternative<FixedSizeDataVector<uint8_t>>(m_Data);
  }
  pdfium::span<const uint8_t> GetData() const;

  Stage m_CurrentStage;
  UnownedPtr<CPDF_PageObjectHolder> const m_pPageObjectHolder;
  UnownedPtr<CPDF_Type3Char> m_pType3Char;  // Only used when parsing forms.
  RetainPtr<CPDF_StreamAcc> m_pSingleStream;
  std::vector<RetainPtr<CPDF_StreamAcc>> m_StreamArray;
  std::vector<uint32_t> m_StreamSegmentOffsets;
  absl::variant<pdfium::raw_span<const uint8_t>, FixedSizeDataVector<uint8_t>>
      m_Data;
  uint32_t m_nStreams = 0;
  uint32_t m_CurrentOffset = 0;
  // Only used when parsing pages.
  CPDF_Form::RecursionState m_RecursionState;

  // Must not outlive |m_RecursionState|.
  std::unique_ptr<CPDF_StreamContentParser> m_pParser;
};

#endif  // CORE_FPDFAPI_PAGE_CPDF_CONTENTPARSER_H_
