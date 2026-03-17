// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_annotlist.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "constants/annotation_common.h"
#include "constants/annotation_flags.h"
#include "constants/form_fields.h"
#include "constants/form_flags.h"
#include "core/fpdfapi/page/cpdf_occontext.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/render/cpdf_renderoptions.h"
#include "core/fpdfdoc/cpdf_annot.h"
#include "core/fpdfdoc/cpdf_formfield.h"
#include "core/fpdfdoc/cpdf_generateap.h"
#include "core/fpdfdoc/cpdf_interactiveform.h"
#include "core/fxcrt/check.h"

namespace {

bool PopupAppearsForAnnotType(CPDF_Annot::Subtype subtype) {
  switch (subtype) {
    case CPDF_Annot::Subtype::TEXT:
    case CPDF_Annot::Subtype::LINE:
    case CPDF_Annot::Subtype::SQUARE:
    case CPDF_Annot::Subtype::CIRCLE:
    case CPDF_Annot::Subtype::POLYGON:
    case CPDF_Annot::Subtype::POLYLINE:
    case CPDF_Annot::Subtype::HIGHLIGHT:
    case CPDF_Annot::Subtype::UNDERLINE:
    case CPDF_Annot::Subtype::SQUIGGLY:
    case CPDF_Annot::Subtype::STRIKEOUT:
    case CPDF_Annot::Subtype::STAMP:
    case CPDF_Annot::Subtype::CARET:
    case CPDF_Annot::Subtype::INK:
    case CPDF_Annot::Subtype::FILEATTACHMENT:
    case CPDF_Annot::Subtype::REDACT:
      return true;
    case CPDF_Annot::Subtype::UNKNOWN:
    case CPDF_Annot::Subtype::LINK:
    case CPDF_Annot::Subtype::FREETEXT:
    case CPDF_Annot::Subtype::POPUP:
    case CPDF_Annot::Subtype::SOUND:
    case CPDF_Annot::Subtype::MOVIE:
    case CPDF_Annot::Subtype::WIDGET:
    case CPDF_Annot::Subtype::SCREEN:
    case CPDF_Annot::Subtype::PRINTERMARK:
    case CPDF_Annot::Subtype::TRAPNET:
    case CPDF_Annot::Subtype::WATERMARK:
    case CPDF_Annot::Subtype::THREED:
    case CPDF_Annot::Subtype::RICHMEDIA:
    case CPDF_Annot::Subtype::XFAWIDGET:
      return false;
  }
}

std::unique_ptr<CPDF_Annot> CreatePopupAnnot(CPDF_Document* pDocument,
                                             CPDF_Page* pPage,
                                             CPDF_Annot* pAnnot) {
  if (!PopupAppearsForAnnotType(pAnnot->GetSubtype()))
    return nullptr;

  const CPDF_Dictionary* pParentDict = pAnnot->GetAnnotDict();
  if (!pParentDict)
    return nullptr;

  // TODO(crbug.com/pdfium/1098): Determine if we really need to check if
  // /Contents is empty or not. If so, optimize decoding for empty check.
  ByteString contents =
      pParentDict->GetByteStringFor(pdfium::annotation::kContents);
  if (PDF_DecodeText(contents.unsigned_span()).IsEmpty()) {
    return nullptr;
  }

  auto pAnnotDict = pDocument->New<CPDF_Dictionary>();
  pAnnotDict->SetNewFor<CPDF_Name>(pdfium::annotation::kType, "Annot");
  pAnnotDict->SetNewFor<CPDF_Name>(pdfium::annotation::kSubtype, "Popup");
  pAnnotDict->SetNewFor<CPDF_String>(
      pdfium::form_fields::kT,
      pParentDict->GetByteStringFor(pdfium::form_fields::kT));
  pAnnotDict->SetNewFor<CPDF_String>(pdfium::annotation::kContents, contents);

  CFX_FloatRect rect = pParentDict->GetRectFor(pdfium::annotation::kRect);
  rect.Normalize();
  CFX_FloatRect popupRect(0, 0, 200, 200);
  // Note that if the popup can set its own dimensions, then we will need to
  // make sure that it isn't larger than the page size.
  if (rect.left + popupRect.Width() > pPage->GetPageWidth() &&
      rect.bottom - popupRect.Height() < 0) {
    // If the annotation is on the bottom-right corner of the page, then place
    // the popup above and to the left of the annotation.
    popupRect.Translate(rect.right - popupRect.Width(), rect.top);
  } else {
    // Place the popup below and to the right of the annotation without getting
    // clipped by page edges.
    popupRect.Translate(
        std::min(rect.left, pPage->GetPageWidth() - popupRect.Width()),
        std::max(rect.bottom - popupRect.Height(), 0.f));
  }

  pAnnotDict->SetRectFor(pdfium::annotation::kRect, popupRect);
  pAnnotDict->SetNewFor<CPDF_Number>(pdfium::annotation::kF, 0);

  auto pPopupAnnot =
      std::make_unique<CPDF_Annot>(std::move(pAnnotDict), pDocument);
  pAnnot->SetPopupAnnot(pPopupAnnot.get());
  return pPopupAnnot;
}

void GenerateAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  if (!pAnnotDict ||
      pAnnotDict->GetByteStringFor(pdfium::annotation::kSubtype) != "Widget") {
    return;
  }

  RetainPtr<const CPDF_Object> pFieldTypeObj =
      CPDF_FormField::GetFieldAttrForDict(pAnnotDict, pdfium::form_fields::kFT);
  if (!pFieldTypeObj)
    return;

  ByteString field_type = pFieldTypeObj->GetString();
  if (field_type == pdfium::form_fields::kTx) {
    CPDF_GenerateAP::GenerateFormAP(pDoc, pAnnotDict,
                                    CPDF_GenerateAP::kTextField);
    return;
  }

  RetainPtr<const CPDF_Object> pFieldFlagsObj =
      CPDF_FormField::GetFieldAttrForDict(pAnnotDict, pdfium::form_fields::kFf);
  uint32_t flags = pFieldFlagsObj ? pFieldFlagsObj->GetInteger() : 0;
  if (field_type == pdfium::form_fields::kCh) {
    auto type = (flags & pdfium::form_flags::kChoiceCombo)
                    ? CPDF_GenerateAP::kComboBox
                    : CPDF_GenerateAP::kListBox;
    CPDF_GenerateAP::GenerateFormAP(pDoc, pAnnotDict, type);
    return;
  }

  if (field_type != pdfium::form_fields::kBtn)
    return;
  if (flags & pdfium::form_flags::kButtonPushbutton)
    return;
  if (pAnnotDict->KeyExist(pdfium::annotation::kAS))
    return;

  RetainPtr<const CPDF_Dictionary> pParentDict =
      pAnnotDict->GetDictFor(pdfium::form_fields::kParent);
  if (!pParentDict || !pParentDict->KeyExist(pdfium::annotation::kAS))
    return;

  pAnnotDict->SetNewFor<CPDF_String>(
      pdfium::annotation::kAS,
      pParentDict->GetByteStringFor(pdfium::annotation::kAS));
}

}  // namespace

CPDF_AnnotList::CPDF_AnnotList(CPDF_Page* pPage)
    : m_pPage(pPage), m_pDocument(m_pPage->GetDocument()) {
  RetainPtr<CPDF_Array> pAnnots = m_pPage->GetMutableAnnotsArray();
  if (!pAnnots)
    return;

  const CPDF_Dictionary* pRoot = m_pDocument->GetRoot();
  RetainPtr<const CPDF_Dictionary> pAcroForm = pRoot->GetDictFor("AcroForm");
  bool bRegenerateAP =
      pAcroForm && pAcroForm->GetBooleanFor("NeedAppearances", false);
  for (size_t i = 0; i < pAnnots->size(); ++i) {
    RetainPtr<CPDF_Dictionary> pDict =
        ToDictionary(pAnnots->GetMutableDirectObjectAt(i));
    if (!pDict)
      continue;
    const ByteString subtype =
        pDict->GetByteStringFor(pdfium::annotation::kSubtype);
    if (subtype == "Popup") {
      // Skip creating Popup annotations in the PDF document since PDFium
      // provides its own Popup annotations.
      continue;
    }
    pAnnots->ConvertToIndirectObjectAt(i, m_pDocument);
    m_AnnotList.push_back(std::make_unique<CPDF_Annot>(pDict, m_pDocument));
    if (bRegenerateAP && subtype == "Widget" &&
        CPDF_InteractiveForm::IsUpdateAPEnabled() &&
        !pDict->GetDictFor(pdfium::annotation::kAP)) {
      GenerateAP(m_pDocument, pDict.Get());
    }
  }

  m_nAnnotCount = m_AnnotList.size();
  for (size_t i = 0; i < m_nAnnotCount; ++i) {
    std::unique_ptr<CPDF_Annot> pPopupAnnot =
        CreatePopupAnnot(m_pDocument, m_pPage, m_AnnotList[i].get());
    if (pPopupAnnot)
      m_AnnotList.push_back(std::move(pPopupAnnot));
  }
}

CPDF_AnnotList::~CPDF_AnnotList() {
  // Move the pop-up annotations out of |m_AnnotList| into |popups|. Then
  // destroy |m_AnnotList| first. This prevents dangling pointers to the pop-up
  // annotations.
  size_t nPopupCount = m_AnnotList.size() - m_nAnnotCount;
  std::vector<std::unique_ptr<CPDF_Annot>> popups(nPopupCount);
  for (size_t i = 0; i < nPopupCount; ++i)
    popups[i] = std::move(m_AnnotList[m_nAnnotCount + i]);
  m_AnnotList.clear();
}

bool CPDF_AnnotList::Contains(const CPDF_Annot* pAnnot) const {
  auto it = std::find_if(m_AnnotList.begin(), m_AnnotList.end(),
                         [pAnnot](const std::unique_ptr<CPDF_Annot>& annot) {
                           return annot.get() == pAnnot;
                         });
  return it != m_AnnotList.end();
}

void CPDF_AnnotList::DisplayPass(CPDF_RenderContext* pContext,
                                 bool bPrinting,
                                 const CFX_Matrix& mtMatrix,
                                 bool bWidgetPass) {
  CHECK(pContext);
  for (const auto& pAnnot : m_AnnotList) {
    bool bWidget = pAnnot->GetSubtype() == CPDF_Annot::Subtype::WIDGET;
    if ((bWidgetPass && !bWidget) || (!bWidgetPass && bWidget))
      continue;

    uint32_t annot_flags = pAnnot->GetFlags();
    if (annot_flags & pdfium::annotation_flags::kHidden)
      continue;

    if (bPrinting && (annot_flags & pdfium::annotation_flags::kPrint) == 0)
      continue;

    if (!bPrinting && (annot_flags & pdfium::annotation_flags::kNoView))
      continue;

    pAnnot->DrawInContext(m_pPage, pContext, mtMatrix,
                          CPDF_Annot::AppearanceMode::kNormal);
  }
}

void CPDF_AnnotList::DisplayAnnots(CPDF_RenderContext* pContext,
                                   bool bPrinting,
                                   const CFX_Matrix& mtUser2Device,
                                   bool bShowWidget) {
  CHECK(pContext);
  DisplayPass(pContext, bPrinting, mtUser2Device, false);
  if (bShowWidget)
    DisplayPass(pContext, bPrinting, mtUser2Device, true);
}
