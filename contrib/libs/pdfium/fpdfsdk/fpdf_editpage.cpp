// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "public/fpdf_edit.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "constants/page_object.h"
#include "core/fpdfapi/edit/cpdf_pagecontentgenerator.h"
#include "core/fpdfapi/page/cpdf_colorspace.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_formobject.h"
#include "core/fpdfapi/page/cpdf_imageobject.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_pageimagecache.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/page/cpdf_pathobject.h"
#include "core/fpdfapi/page/cpdf_shadingobject.h"
#include "core/fpdfapi/page/cpdf_textobject.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/render/cpdf_docrenderdata.h"
#include "core/fpdfdoc/cpdf_annot.h"
#include "core/fpdfdoc/cpdf_annotlist.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "fpdfsdk/cpdfsdk_helpers.h"
#include "public/fpdf_formfill.h"

#ifdef PDF_ENABLE_XFA
#include "fpdfsdk/fpdfxfa/cpdfxfa_context.h"
#include "fpdfsdk/fpdfxfa/cpdfxfa_page.h"
#endif  // PDF_ENABLE_XFA

namespace {

static_assert(FPDF_PAGEOBJ_TEXT ==
                  static_cast<int>(CPDF_PageObject::Type::kText),
              "FPDF_PAGEOBJ_TEXT/CPDF_PageObject::TEXT mismatch");
static_assert(FPDF_PAGEOBJ_PATH ==
                  static_cast<int>(CPDF_PageObject::Type::kPath),
              "FPDF_PAGEOBJ_PATH/CPDF_PageObject::PATH mismatch");
static_assert(FPDF_PAGEOBJ_IMAGE ==
                  static_cast<int>(CPDF_PageObject::Type::kImage),
              "FPDF_PAGEOBJ_IMAGE/CPDF_PageObject::IMAGE mismatch");
static_assert(FPDF_PAGEOBJ_SHADING ==
                  static_cast<int>(CPDF_PageObject::Type::kShading),
              "FPDF_PAGEOBJ_SHADING/CPDF_PageObject::SHADING mismatch");
static_assert(FPDF_PAGEOBJ_FORM ==
                  static_cast<int>(CPDF_PageObject::Type::kForm),
              "FPDF_PAGEOBJ_FORM/CPDF_PageObject::FORM mismatch");

bool IsPageObject(CPDF_Page* pPage) {
  if (!pPage)
    return false;

  RetainPtr<const CPDF_Dictionary> pFormDict = pPage->GetDict();
  if (!pFormDict->KeyExist(pdfium::page_object::kType))
    return false;

  RetainPtr<const CPDF_Name> pName =
      ToName(pFormDict->GetObjectFor(pdfium::page_object::kType)->GetDirect());
  return pName && pName->GetString() == "Page";
}

void CalcBoundingBox(CPDF_PageObject* pPageObj) {
  switch (pPageObj->GetType()) {
    case CPDF_PageObject::Type::kText: {
      break;
    }
    case CPDF_PageObject::Type::kPath: {
      CPDF_PathObject* pPathObj = pPageObj->AsPath();
      pPathObj->CalcBoundingBox();
      break;
    }
    case CPDF_PageObject::Type::kImage: {
      CPDF_ImageObject* pImageObj = pPageObj->AsImage();
      pImageObj->CalcBoundingBox();
      break;
    }
    case CPDF_PageObject::Type::kShading: {
      CPDF_ShadingObject* pShadingObj = pPageObj->AsShading();
      pShadingObj->CalcBoundingBox();
      break;
    }
    case CPDF_PageObject::Type::kForm: {
      CPDF_FormObject* pFormObj = pPageObj->AsForm();
      pFormObj->CalcBoundingBox();
      break;
    }
  }
}

RetainPtr<CPDF_Dictionary> GetMarkParamDict(FPDF_PAGEOBJECTMARK mark) {
  CPDF_ContentMarkItem* pMarkItem =
      CPDFContentMarkItemFromFPDFPageObjectMark(mark);
  return pMarkItem ? pMarkItem->GetParam() : nullptr;
}

RetainPtr<CPDF_Dictionary> GetOrCreateMarkParamsDict(FPDF_DOCUMENT document,
                                                     FPDF_PAGEOBJECTMARK mark) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  CPDF_ContentMarkItem* pMarkItem =
      CPDFContentMarkItemFromFPDFPageObjectMark(mark);
  if (!pMarkItem)
    return nullptr;

  RetainPtr<CPDF_Dictionary> pParams = pMarkItem->GetParam();
  if (!pParams) {
    pParams = pDoc->New<CPDF_Dictionary>();
    pMarkItem->SetDirectDict(pParams);
  }
  return pParams;
}

bool PageObjectContainsMark(CPDF_PageObject* pPageObj,
                            FPDF_PAGEOBJECTMARK mark) {
  const CPDF_ContentMarkItem* pMarkItem =
      CPDFContentMarkItemFromFPDFPageObjectMark(mark);
  return pMarkItem && pPageObj->GetContentMarks()->ContainsItem(pMarkItem);
}

CPDF_FormObject* CPDFFormObjectFromFPDFPageObject(FPDF_PAGEOBJECT page_object) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  return pPageObj ? pPageObj->AsForm() : nullptr;
}

const CPDF_PageObjectHolder* CPDFPageObjHolderFromFPDFFormObject(
    FPDF_PAGEOBJECT page_object) {
  CPDF_FormObject* pFormObject = CPDFFormObjectFromFPDFPageObject(page_object);
  return pFormObject ? pFormObject->form() : nullptr;
}

}  // namespace

FPDF_EXPORT FPDF_DOCUMENT FPDF_CALLCONV FPDF_CreateNewDocument() {
  auto pDoc =
      std::make_unique<CPDF_Document>(std::make_unique<CPDF_DocRenderData>(),
                                      std::make_unique<CPDF_DocPageData>());
  pDoc->CreateNewDoc();

  time_t currentTime;
  ByteString DateStr;
  if (IsPDFSandboxPolicyEnabled(FPDF_POLICY_MACHINETIME_ACCESS)) {
    if (FXSYS_time(&currentTime) != -1) {
      tm* pTM = FXSYS_localtime(&currentTime);
      if (pTM) {
        DateStr = ByteString::Format(
            "D:%04d%02d%02d%02d%02d%02d", pTM->tm_year + 1900, pTM->tm_mon + 1,
            pTM->tm_mday, pTM->tm_hour, pTM->tm_min, pTM->tm_sec);
      }
    }
  }

  RetainPtr<CPDF_Dictionary> pInfoDict = pDoc->GetInfo();
  if (pInfoDict) {
    if (IsPDFSandboxPolicyEnabled(FPDF_POLICY_MACHINETIME_ACCESS))
      pInfoDict->SetNewFor<CPDF_String>("CreationDate", DateStr);
    pInfoDict->SetNewFor<CPDF_String>("Creator", L"PDFium");
  }

  // Caller takes ownership of pDoc.
  return FPDFDocumentFromCPDFDocument(pDoc.release());
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_Delete(FPDF_DOCUMENT document,
                                               int page_index) {
  auto* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return;

  CPDF_Document::Extension* pExtension = pDoc->GetExtension();
  const uint32_t page_obj_num = pExtension ? pExtension->DeletePage(page_index)
                                           : pDoc->DeletePage(page_index);
  pDoc->SetPageToNullObject(page_obj_num);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDF_MovePages(FPDF_DOCUMENT document,
               const int* page_indices,
               unsigned long page_indices_len,
               int dest_page_index) {
  auto* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc) {
    return false;
  }

  // SAFETY: caller ensures `page_indices` points to at least
  // `page_indices_len` ints.
  return doc->MovePages(
      UNSAFE_BUFFERS(pdfium::make_span(page_indices, page_indices_len)),
      dest_page_index);
}

FPDF_EXPORT FPDF_PAGE FPDF_CALLCONV FPDFPage_New(FPDF_DOCUMENT document,
                                                 int page_index,
                                                 double width,
                                                 double height) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  page_index = std::clamp(page_index, 0, pDoc->GetPageCount());
  RetainPtr<CPDF_Dictionary> pPageDict(pDoc->CreateNewPage(page_index));
  if (!pPageDict)
    return nullptr;

  pPageDict->SetRectFor(pdfium::page_object::kMediaBox,
                        CFX_FloatRect(0, 0, width, height));
  pPageDict->SetNewFor<CPDF_Number>(pdfium::page_object::kRotate, 0);
  pPageDict->SetNewFor<CPDF_Dictionary>(pdfium::page_object::kResources);

#ifdef PDF_ENABLE_XFA
  if (pDoc->GetExtension()) {
    auto pXFAPage = pdfium::MakeRetain<CPDFXFA_Page>(pDoc, page_index);
    pXFAPage->LoadPDFPageFromDict(pPageDict);
    return FPDFPageFromIPDFPage(pXFAPage.Leak());  // Caller takes ownership.
  }
#endif  // PDF_ENABLE_XFA

  auto pPage = pdfium::MakeRetain<CPDF_Page>(pDoc, pPageDict);
  pPage->AddPageImageCache();
  pPage->ParseContent();

  return FPDFPageFromIPDFPage(pPage.Leak());  // Caller takes ownership.
}

FPDF_EXPORT int FPDF_CALLCONV FPDFPage_GetRotation(FPDF_PAGE page) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  return IsPageObject(pPage) ? pPage->GetPageRotation() : -1;
}

FPDF_EXPORT void FPDF_CALLCONV
FPDFPage_InsertObject(FPDF_PAGE page, FPDF_PAGEOBJECT page_object) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return;

  std::unique_ptr<CPDF_PageObject> pPageObjHolder(pPageObj);
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!IsPageObject(pPage))
    return;

  pPageObj->SetDirty(true);
  pPage->AppendPageObject(std::move(pPageObjHolder));
  CalcBoundingBox(pPageObj);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPage_RemoveObject(FPDF_PAGE page, FPDF_PAGEOBJECT page_object) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return false;

  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!IsPageObject(pPage))
    return false;

  // Caller takes ownership.
  return !!pPage->RemovePageObject(pPageObj).release();
}

FPDF_EXPORT int FPDF_CALLCONV FPDFPage_CountObjects(FPDF_PAGE page) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!IsPageObject(pPage))
    return -1;

  return pdfium::checked_cast<int>(pPage->GetPageObjectCount());
}

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV FPDFPage_GetObject(FPDF_PAGE page,
                                                             int index) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!IsPageObject(pPage))
    return nullptr;

  return FPDFPageObjectFromCPDFPageObject(pPage->GetPageObjectByIndex(index));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPage_HasTransparency(FPDF_PAGE page) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  return pPage && pPage->BackgroundAlphaNeeded();
}

FPDF_EXPORT void FPDF_CALLCONV
FPDFPageObj_Destroy(FPDF_PAGEOBJECT page_object) {
  delete CPDFPageObjectFromFPDFPageObject(page_object);
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFPageObj_GetMarkedContentID(FPDF_PAGEOBJECT page_object) {
  CPDF_PageObject* cpage_object = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!cpage_object) {
    return -1;
  }

  return cpage_object->GetContentMarks()->GetMarkedContentID();
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFPageObj_CountMarks(FPDF_PAGEOBJECT page_object) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return -1;

  return pdfium::checked_cast<int>(pPageObj->GetContentMarks()->CountItems());
}

FPDF_EXPORT FPDF_PAGEOBJECTMARK FPDF_CALLCONV
FPDFPageObj_GetMark(FPDF_PAGEOBJECT page_object, unsigned long index) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return nullptr;

  CPDF_ContentMarks* pMarks = pPageObj->GetContentMarks();
  if (index >= pMarks->CountItems())
    return nullptr;

  return FPDFPageObjectMarkFromCPDFContentMarkItem(pMarks->GetItem(index));
}

FPDF_EXPORT FPDF_PAGEOBJECTMARK FPDF_CALLCONV
FPDFPageObj_AddMark(FPDF_PAGEOBJECT page_object, FPDF_BYTESTRING name) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return nullptr;

  CPDF_ContentMarks* pMarks = pPageObj->GetContentMarks();
  pMarks->AddMark(name);
  pPageObj->SetDirty(true);

  const size_t index = pMarks->CountItems() - 1;
  return FPDFPageObjectMarkFromCPDFContentMarkItem(pMarks->GetItem(index));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_RemoveMark(FPDF_PAGEOBJECT page_object, FPDF_PAGEOBJECTMARK mark) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  CPDF_ContentMarkItem* pMarkItem =
      CPDFContentMarkItemFromFPDFPageObjectMark(mark);
  if (!pPageObj || !pMarkItem)
    return false;

  if (!pPageObj->GetContentMarks()->RemoveMark(pMarkItem))
    return false;

  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_GetName(FPDF_PAGEOBJECTMARK mark,
                        void* buffer,
                        unsigned long buflen,
                        unsigned long* out_buflen) {
  const CPDF_ContentMarkItem* pMarkItem =
      CPDFContentMarkItemFromFPDFPageObjectMark(mark);
  if (!pMarkItem || !out_buflen) {
    return false;
  }
  // SAFETY: required from caller.
  *out_buflen = Utf16EncodeMaybeCopyAndReturnLength(
      WideString::FromUTF8(pMarkItem->GetName().AsStringView()),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFPageObjMark_CountParams(FPDF_PAGEOBJECTMARK mark) {
  const CPDF_ContentMarkItem* pMarkItem =
      CPDFContentMarkItemFromFPDFPageObjectMark(mark);
  if (!pMarkItem)
    return -1;

  RetainPtr<const CPDF_Dictionary> pParams = pMarkItem->GetParam();
  return pParams ? fxcrt::CollectionSize<int>(*pParams) : 0;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_GetParamKey(FPDF_PAGEOBJECTMARK mark,
                            unsigned long index,
                            void* buffer,
                            unsigned long buflen,
                            unsigned long* out_buflen) {
  if (!out_buflen)
    return false;

  RetainPtr<const CPDF_Dictionary> pParams = GetMarkParamDict(mark);
  if (!pParams)
    return false;

  CPDF_DictionaryLocker locker(pParams);
  for (auto& it : locker) {
    if (index == 0) {
      // SAFETY: required from caller.
      *out_buflen = Utf16EncodeMaybeCopyAndReturnLength(
          WideString::FromUTF8(it.first.AsStringView()),
          UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
      return true;
    }
    --index;
  }

  return false;
}

FPDF_EXPORT FPDF_OBJECT_TYPE FPDF_CALLCONV
FPDFPageObjMark_GetParamValueType(FPDF_PAGEOBJECTMARK mark,
                                  FPDF_BYTESTRING key) {
  RetainPtr<const CPDF_Dictionary> pParams = GetMarkParamDict(mark);
  if (!pParams)
    return FPDF_OBJECT_UNKNOWN;

  RetainPtr<const CPDF_Object> pObject = pParams->GetObjectFor(key);
  return pObject ? pObject->GetType() : FPDF_OBJECT_UNKNOWN;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_GetParamIntValue(FPDF_PAGEOBJECTMARK mark,
                                 FPDF_BYTESTRING key,
                                 int* out_value) {
  if (!out_value)
    return false;

  RetainPtr<const CPDF_Dictionary> pParams = GetMarkParamDict(mark);
  if (!pParams)
    return false;

  RetainPtr<const CPDF_Object> pObj = pParams->GetObjectFor(key);
  if (!pObj || !pObj->IsNumber())
    return false;

  *out_value = pObj->GetInteger();
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_GetParamStringValue(FPDF_PAGEOBJECTMARK mark,
                                    FPDF_BYTESTRING key,
                                    void* buffer,
                                    unsigned long buflen,
                                    unsigned long* out_buflen) {
  if (!out_buflen)
    return false;

  RetainPtr<const CPDF_Dictionary> pParams = GetMarkParamDict(mark);
  if (!pParams)
    return false;

  RetainPtr<const CPDF_Object> pObj = pParams->GetObjectFor(key);
  if (!pObj || !pObj->IsString())
    return false;

  // SAFETY: required from caller.
  *out_buflen = Utf16EncodeMaybeCopyAndReturnLength(
      WideString::FromUTF8(pObj->GetString().AsStringView()),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_GetParamBlobValue(FPDF_PAGEOBJECTMARK mark,
                                  FPDF_BYTESTRING key,
                                  void* buffer,
                                  unsigned long buflen,
                                  unsigned long* out_buflen) {
  if (!out_buflen)
    return false;

  RetainPtr<const CPDF_Dictionary> pParams = GetMarkParamDict(mark);
  if (!pParams)
    return false;

  RetainPtr<const CPDF_Object> pObj = pParams->GetObjectFor(key);
  if (!pObj || !pObj->IsString())
    return false;

  // SAFETY: required from caller.
  auto result_span = UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen));
  ByteString value = pObj->GetString();
  fxcrt::try_spancpy(result_span, value.span());
  *out_buflen = pdfium::checked_cast<unsigned long>(value.span().size());
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_HasTransparency(FPDF_PAGEOBJECT page_object) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj) {
    return false;
  }
  if (pPageObj->general_state().GetBlendType() != BlendMode::kNormal) {
    return true;
  }
  if (pPageObj->general_state().GetSoftMask()) {
    return true;
  }
  if (pPageObj->general_state().GetFillAlpha() != 1.0f) {
    return true;
  }
  if (pPageObj->IsPath() &&
      pPageObj->general_state().GetStrokeAlpha() != 1.0f) {
    return true;
  }
  if (!pPageObj->IsForm()) {
    return false;
  }

  const CPDF_Form* pForm = pPageObj->AsForm()->form();
  if (!pForm) {
    return false;
  }

  const CPDF_Transparency& trans = pForm->GetTransparency();
  return trans.IsGroup() || trans.IsIsolated();
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_SetIntParam(FPDF_DOCUMENT document,
                            FPDF_PAGEOBJECT page_object,
                            FPDF_PAGEOBJECTMARK mark,
                            FPDF_BYTESTRING key,
                            int value) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !PageObjectContainsMark(pPageObj, mark))
    return false;

  RetainPtr<CPDF_Dictionary> pParams =
      GetOrCreateMarkParamsDict(document, mark);
  if (!pParams)
    return false;

  pParams->SetNewFor<CPDF_Number>(key, value);
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_SetStringParam(FPDF_DOCUMENT document,
                               FPDF_PAGEOBJECT page_object,
                               FPDF_PAGEOBJECTMARK mark,
                               FPDF_BYTESTRING key,
                               FPDF_BYTESTRING value) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !PageObjectContainsMark(pPageObj, mark))
    return false;

  RetainPtr<CPDF_Dictionary> pParams =
      GetOrCreateMarkParamsDict(document, mark);
  if (!pParams)
    return false;

  pParams->SetNewFor<CPDF_String>(key, value);
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_SetBlobParam(FPDF_DOCUMENT document,
                             FPDF_PAGEOBJECT page_object,
                             FPDF_PAGEOBJECTMARK mark,
                             FPDF_BYTESTRING key,
                             void* value,
                             unsigned long value_len) {
  if (!value && value_len > 0) {
    return false;
  }

  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !PageObjectContainsMark(pPageObj, mark)) {
    return false;
  }

  RetainPtr<CPDF_Dictionary> pParams =
      GetOrCreateMarkParamsDict(document, mark);
  if (!pParams) {
    return false;
  }

  // SAFETY: required from caller.
  pParams->SetNewFor<CPDF_String>(
      key,
      UNSAFE_BUFFERS(
          pdfium::make_span(static_cast<const uint8_t*>(value), value_len)),
      CPDF_String::DataType::kIsHex);
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObjMark_RemoveParam(FPDF_PAGEOBJECT page_object,
                            FPDF_PAGEOBJECTMARK mark,
                            FPDF_BYTESTRING key) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return false;

  RetainPtr<CPDF_Dictionary> pParams = GetMarkParamDict(mark);
  if (!pParams)
    return false;

  auto removed = pParams->RemoveFor(key);
  if (!removed)
    return false;

  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFPageObj_GetType(FPDF_PAGEOBJECT page_object) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  return pPageObj ? static_cast<int>(pPageObj->GetType())
                  : FPDF_PAGEOBJ_UNKNOWN;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPage_GenerateContent(FPDF_PAGE page) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!IsPageObject(pPage))
    return false;

  CPDF_PageContentGenerator CG(pPage);
  CG.GenerateContent();
  return true;
}

FPDF_EXPORT void FPDF_CALLCONV
FPDFPageObj_Transform(FPDF_PAGEOBJECT page_object,
                      double a,
                      double b,
                      double c,
                      double d,
                      double e,
                      double f) {
  const FS_MATRIX matrix{static_cast<float>(a), static_cast<float>(b),
                         static_cast<float>(c), static_cast<float>(d),
                         static_cast<float>(e), static_cast<float>(f)};
  FPDFPageObj_TransformF(page_object, &matrix);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_TransformF(FPDF_PAGEOBJECT page_object, const FS_MATRIX* matrix) {
  if (!matrix) {
    return false;
  }

  CPDF_PageObject* cpage_object = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!cpage_object) {
    return false;
  }

  cpage_object->Transform(CFXMatrixFromFSMatrix(*matrix));
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_GetMatrix(FPDF_PAGEOBJECT page_object, FS_MATRIX* matrix) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !matrix)
    return false;

  switch (pPageObj->GetType()) {
    case CPDF_PageObject::Type::kText:
      *matrix = FSMatrixFromCFXMatrix(pPageObj->AsText()->GetTextMatrix());
      return true;
    case CPDF_PageObject::Type::kPath:
      *matrix = FSMatrixFromCFXMatrix(pPageObj->AsPath()->matrix());
      return true;
    case CPDF_PageObject::Type::kImage:
      *matrix = FSMatrixFromCFXMatrix(pPageObj->AsImage()->matrix());
      return true;
    case CPDF_PageObject::Type::kShading:
      return false;
    case CPDF_PageObject::Type::kForm:
      *matrix = FSMatrixFromCFXMatrix(pPageObj->AsForm()->form_matrix());
      return true;
  }
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_SetMatrix(FPDF_PAGEOBJECT page_object, const FS_MATRIX* matrix) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !matrix)
    return false;

  CFX_Matrix cmatrix = CFXMatrixFromFSMatrix(*matrix);
  switch (pPageObj->GetType()) {
    case CPDF_PageObject::Type::kText:
      pPageObj->AsText()->SetTextMatrix(cmatrix);
      break;
    case CPDF_PageObject::Type::kPath:
      pPageObj->AsPath()->SetPathMatrix(cmatrix);
      break;
    case CPDF_PageObject::Type::kImage:
      pPageObj->AsImage()->SetImageMatrix(cmatrix);
      break;
    case CPDF_PageObject::Type::kShading:
      return false;
    case CPDF_PageObject::Type::kForm:
      pPageObj->AsForm()->SetFormMatrix(cmatrix);
      break;
  }
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT void FPDF_CALLCONV
FPDFPageObj_SetBlendMode(FPDF_PAGEOBJECT page_object,
                         FPDF_BYTESTRING blend_mode) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return;

  pPageObj->mutable_general_state().SetBlendMode(blend_mode);
  pPageObj->SetDirty(true);
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_TransformAnnots(FPDF_PAGE page,
                                                        double a,
                                                        double b,
                                                        double c,
                                                        double d,
                                                        double e,
                                                        double f) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage)
    return;

  CPDF_AnnotList AnnotList(pPage);
  for (size_t i = 0; i < AnnotList.Count(); ++i) {
    CPDF_Annot* pAnnot = AnnotList.GetAt(i);
    CFX_Matrix matrix((float)a, (float)b, (float)c, (float)d, (float)e,
                      (float)f);
    CFX_FloatRect rect = matrix.TransformRect(pAnnot->GetRect());

    RetainPtr<CPDF_Dictionary> pAnnotDict = pAnnot->GetMutableAnnotDict();
    RetainPtr<CPDF_Array> pRectArray = pAnnotDict->GetMutableArrayFor("Rect");
    if (pRectArray)
      pRectArray->Clear();
    else
      pRectArray = pAnnotDict->SetNewFor<CPDF_Array>("Rect");

    pRectArray->AppendNew<CPDF_Number>(rect.left);
    pRectArray->AppendNew<CPDF_Number>(rect.bottom);
    pRectArray->AppendNew<CPDF_Number>(rect.right);
    pRectArray->AppendNew<CPDF_Number>(rect.top);

    // TODO(unknown): Transform AP's rectangle
  }
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_SetRotation(FPDF_PAGE page,
                                                    int rotate) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!IsPageObject(pPage))
    return;

  rotate %= 4;
  pPage->GetMutableDict()->SetNewFor<CPDF_Number>(pdfium::page_object::kRotate,
                                                  rotate * 90);
  pPage->UpdateDimensions();
}

FPDF_BOOL FPDFPageObj_SetFillColor(FPDF_PAGEOBJECT page_object,
                                   unsigned int R,
                                   unsigned int G,
                                   unsigned int B,
                                   unsigned int A) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || R > 255 || G > 255 || B > 255 || A > 255)
    return false;

  std::vector<float> rgb = {R / 255.f, G / 255.f, B / 255.f};
  pPageObj->mutable_general_state().SetFillAlpha(A / 255.f);
  pPageObj->mutable_color_state().SetFillColor(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceRGB),
      std::move(rgb));
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_GetFillColor(FPDF_PAGEOBJECT page_object,
                         unsigned int* R,
                         unsigned int* G,
                         unsigned int* B,
                         unsigned int* A) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !R || !G || !B || !A)
    return false;

  if (!pPageObj->color_state().HasRef()) {
    return false;
  }

  FX_COLORREF fill_color = pPageObj->color_state().GetFillColorRef();
  *R = FXSYS_GetRValue(fill_color);
  *G = FXSYS_GetGValue(fill_color);
  *B = FXSYS_GetBValue(fill_color);
  *A = FXSYS_GetUnsignedAlpha(pPageObj->general_state().GetFillAlpha());
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_GetBounds(FPDF_PAGEOBJECT page_object,
                      float* left,
                      float* bottom,
                      float* right,
                      float* top) {
  CPDF_PageObject* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return false;

  const CFX_FloatRect& bbox = pPageObj->GetRect();
  *left = bbox.left;
  *bottom = bbox.bottom;
  *right = bbox.right;
  *top = bbox.top;
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_GetRotatedBounds(FPDF_PAGEOBJECT page_object,
                             FS_QUADPOINTSF* quad_points) {
  CPDF_PageObject* cpage_object = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!cpage_object || !quad_points)
    return false;

  CFX_Matrix matrix;
  switch (cpage_object->GetType()) {
    case CPDF_PageObject::Type::kText:
      matrix = cpage_object->AsText()->GetTextMatrix();
      break;
    case CPDF_PageObject::Type::kImage:
      matrix = cpage_object->AsImage()->matrix();
      break;
    default:
      // TODO(crbug.com/pdfium/1840): Support more object types.
      return false;
  }

  const CFX_FloatRect& bbox = cpage_object->GetOriginalRect();
  const CFX_PointF bottom_left = matrix.Transform({bbox.left, bbox.bottom});
  const CFX_PointF bottom_right = matrix.Transform({bbox.right, bbox.bottom});
  const CFX_PointF top_right = matrix.Transform({bbox.right, bbox.top});
  const CFX_PointF top_left = matrix.Transform({bbox.left, bbox.top});

  // See PDF 32000-1:2008, figure 64 for the QuadPoints ordering.
  quad_points->x1 = bottom_left.x;
  quad_points->y1 = bottom_left.y;
  quad_points->x2 = bottom_right.x;
  quad_points->y2 = bottom_right.y;
  quad_points->x3 = top_right.x;
  quad_points->y3 = top_right.y;
  quad_points->x4 = top_left.x;
  quad_points->y4 = top_left.y;
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_SetStrokeColor(FPDF_PAGEOBJECT page_object,
                           unsigned int R,
                           unsigned int G,
                           unsigned int B,
                           unsigned int A) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || R > 255 || G > 255 || B > 255 || A > 255)
    return false;

  std::vector<float> rgb = {R / 255.f, G / 255.f, B / 255.f};
  pPageObj->mutable_general_state().SetStrokeAlpha(A / 255.f);
  pPageObj->mutable_color_state().SetStrokeColor(
      CPDF_ColorSpace::GetStockCS(CPDF_ColorSpace::Family::kDeviceRGB),
      std::move(rgb));
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_GetStrokeColor(FPDF_PAGEOBJECT page_object,
                           unsigned int* R,
                           unsigned int* G,
                           unsigned int* B,
                           unsigned int* A) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !R || !G || !B || !A) {
    return false;
  }
  if (!pPageObj->color_state().HasRef()) {
    return false;
  }

  FX_COLORREF stroke_color = pPageObj->color_state().GetStrokeColorRef();
  *R = FXSYS_GetRValue(stroke_color);
  *G = FXSYS_GetGValue(stroke_color);
  *B = FXSYS_GetBValue(stroke_color);
  *A = FXSYS_GetUnsignedAlpha(pPageObj->general_state().GetStrokeAlpha());
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_SetStrokeWidth(FPDF_PAGEOBJECT page_object, float width) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || width < 0.0f)
    return false;

  pPageObj->mutable_graph_state().SetLineWidth(width);
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_GetStrokeWidth(FPDF_PAGEOBJECT page_object, float* width) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !width)
    return false;

  *width = pPageObj->graph_state().GetLineWidth();
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFPageObj_GetLineJoin(FPDF_PAGEOBJECT page_object) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  return pPageObj ? static_cast<int>(pPageObj->graph_state().GetLineJoin())
                  : -1;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_SetLineJoin(FPDF_PAGEOBJECT page_object, int line_join) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return false;

  if (line_join < FPDF_LINEJOIN_MITER || line_join > FPDF_LINEJOIN_BEVEL)
    return false;

  pPageObj->mutable_graph_state().SetLineJoin(
      static_cast<CFX_GraphStateData::LineJoin>(line_join));
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFPageObj_GetLineCap(FPDF_PAGEOBJECT page_object) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  return pPageObj ? static_cast<int>(pPageObj->graph_state().GetLineCap()) : -1;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_SetLineCap(FPDF_PAGEOBJECT page_object, int line_cap) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return false;

  if (line_cap < FPDF_LINECAP_BUTT ||
      line_cap > FPDF_LINECAP_PROJECTING_SQUARE) {
    return false;
  }
  pPageObj->mutable_graph_state().SetLineCap(
      static_cast<CFX_GraphStateData::LineCap>(line_cap));
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_GetDashPhase(FPDF_PAGEOBJECT page_object, float* phase) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj || !phase)
    return false;

  *phase = pPageObj->graph_state().GetLineDashPhase();
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_SetDashPhase(FPDF_PAGEOBJECT page_object, float phase) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return false;

  pPageObj->mutable_graph_state().SetLineDashPhase(phase);
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFPageObj_GetDashCount(FPDF_PAGEOBJECT page_object) {
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  return pPageObj ? pdfium::checked_cast<int>(
                        pPageObj->graph_state().GetLineDashSize())
                  : -1;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_GetDashArray(FPDF_PAGEOBJECT page_object,
                         float* dash_array,
                         size_t dash_count) {
  if (!dash_array) {
    return false;
  }
  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj) {
    return false;
  }

  // SAFETY: required from caller.
  auto result_span = UNSAFE_BUFFERS(pdfium::make_span(dash_array, dash_count));
  auto dash_vector = pPageObj->graph_state().GetLineDashArray();
  return fxcrt::try_spancpy(result_span, pdfium::make_span(dash_vector));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFPageObj_SetDashArray(FPDF_PAGEOBJECT page_object,
                         const float* dash_array,
                         size_t dash_count,
                         float phase) {
  if (dash_count > 0 && !dash_array)
    return false;

  auto* pPageObj = CPDFPageObjectFromFPDFPageObject(page_object);
  if (!pPageObj)
    return false;

  std::vector<float> dashes;
  if (dash_count > 0) {
    dashes.reserve(dash_count);
    dashes.assign(dash_array, UNSAFE_TODO(dash_array + dash_count));
  }
  pPageObj->mutable_graph_state().SetLineDash(dashes, phase, 1.0f);
  pPageObj->SetDirty(true);
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFFormObj_CountObjects(FPDF_PAGEOBJECT form_object) {
  const auto* pObjectList = CPDFPageObjHolderFromFPDFFormObject(form_object);
  return pObjectList
             ? pdfium::checked_cast<int>(pObjectList->GetPageObjectCount())
             : -1;
}

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV
FPDFFormObj_GetObject(FPDF_PAGEOBJECT form_object, unsigned long index) {
  const auto* pObjectList = CPDFPageObjHolderFromFPDFFormObject(form_object);
  if (!pObjectList)
    return nullptr;

  return FPDFPageObjectFromCPDFPageObject(
      pObjectList->GetPageObjectByIndex(index));
}
