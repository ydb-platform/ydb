// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "public/fpdf_annot.h"

#include <array>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "constants/annotation_common.h"
#include "core/fpdfapi/edit/cpdf_pagecontentgenerator.h"
#include "core/fpdfapi/page/cpdf_annotcontext.h"
#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_boolean.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fpdfdoc/cpdf_annot.h"
#include "core/fpdfdoc/cpdf_color_utils.h"
#include "core/fpdfdoc/cpdf_formfield.h"
#include "core/fpdfdoc/cpdf_generateap.h"
#include "core/fpdfdoc/cpdf_interactiveform.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/containers/contains.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/ptr_util.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_color.h"
#include "fpdfsdk/cpdfsdk_formfillenvironment.h"
#include "fpdfsdk/cpdfsdk_helpers.h"
#include "fpdfsdk/cpdfsdk_interactiveform.h"

namespace {

// These checks ensure the consistency of annotation subtype values across core/
// and public.
static_assert(static_cast<int>(CPDF_Annot::Subtype::UNKNOWN) ==
                  FPDF_ANNOT_UNKNOWN,
              "CPDF_Annot::UNKNOWN value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::TEXT) == FPDF_ANNOT_TEXT,
              "CPDF_Annot::TEXT value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::LINK) == FPDF_ANNOT_LINK,
              "CPDF_Annot::LINK value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::FREETEXT) ==
                  FPDF_ANNOT_FREETEXT,
              "CPDF_Annot::FREETEXT value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::LINE) == FPDF_ANNOT_LINE,
              "CPDF_Annot::LINE value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::SQUARE) ==
                  FPDF_ANNOT_SQUARE,
              "CPDF_Annot::SQUARE value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::CIRCLE) ==
                  FPDF_ANNOT_CIRCLE,
              "CPDF_Annot::CIRCLE value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::POLYGON) ==
                  FPDF_ANNOT_POLYGON,
              "CPDF_Annot::POLYGON value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::POLYLINE) ==
                  FPDF_ANNOT_POLYLINE,
              "CPDF_Annot::POLYLINE value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::HIGHLIGHT) ==
                  FPDF_ANNOT_HIGHLIGHT,
              "CPDF_Annot::HIGHLIGHT value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::UNDERLINE) ==
                  FPDF_ANNOT_UNDERLINE,
              "CPDF_Annot::UNDERLINE value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::SQUIGGLY) ==
                  FPDF_ANNOT_SQUIGGLY,
              "CPDF_Annot::SQUIGGLY value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::STRIKEOUT) ==
                  FPDF_ANNOT_STRIKEOUT,
              "CPDF_Annot::STRIKEOUT value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::STAMP) == FPDF_ANNOT_STAMP,
              "CPDF_Annot::STAMP value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::CARET) == FPDF_ANNOT_CARET,
              "CPDF_Annot::CARET value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::INK) == FPDF_ANNOT_INK,
              "CPDF_Annot::INK value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::POPUP) == FPDF_ANNOT_POPUP,
              "CPDF_Annot::POPUP value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::FILEATTACHMENT) ==
                  FPDF_ANNOT_FILEATTACHMENT,
              "CPDF_Annot::FILEATTACHMENT value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::SOUND) == FPDF_ANNOT_SOUND,
              "CPDF_Annot::SOUND value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::MOVIE) == FPDF_ANNOT_MOVIE,
              "CPDF_Annot::MOVIE value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::WIDGET) ==
                  FPDF_ANNOT_WIDGET,
              "CPDF_Annot::WIDGET value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::SCREEN) ==
                  FPDF_ANNOT_SCREEN,
              "CPDF_Annot::SCREEN value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::PRINTERMARK) ==
                  FPDF_ANNOT_PRINTERMARK,
              "CPDF_Annot::PRINTERMARK value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::TRAPNET) ==
                  FPDF_ANNOT_TRAPNET,
              "CPDF_Annot::TRAPNET value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::WATERMARK) ==
                  FPDF_ANNOT_WATERMARK,
              "CPDF_Annot::WATERMARK value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::THREED) ==
                  FPDF_ANNOT_THREED,
              "CPDF_Annot::THREED value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::RICHMEDIA) ==
                  FPDF_ANNOT_RICHMEDIA,
              "CPDF_Annot::RICHMEDIA value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::XFAWIDGET) ==
                  FPDF_ANNOT_XFAWIDGET,
              "CPDF_Annot::XFAWIDGET value mismatch");
static_assert(static_cast<int>(CPDF_Annot::Subtype::REDACT) ==
                  FPDF_ANNOT_REDACT,
              "CPDF_Annot::REDACT value mismatch");

// These checks ensure the consistency of annotation appearance mode values
// across core/ and public.
static_assert(static_cast<int>(CPDF_Annot::AppearanceMode::kNormal) ==
                  FPDF_ANNOT_APPEARANCEMODE_NORMAL,
              "CPDF_Annot::AppearanceMode::Normal value mismatch");
static_assert(static_cast<int>(CPDF_Annot::AppearanceMode::kRollover) ==
                  FPDF_ANNOT_APPEARANCEMODE_ROLLOVER,
              "CPDF_Annot::AppearanceMode::Rollover value mismatch");
static_assert(static_cast<int>(CPDF_Annot::AppearanceMode::kDown) ==
                  FPDF_ANNOT_APPEARANCEMODE_DOWN,
              "CPDF_Annot::AppearanceMode::Down value mismatch");

// These checks ensure the consistency of dictionary value types across core/
// and public/.
static_assert(static_cast<int>(CPDF_Object::Type::kBoolean) ==
                  FPDF_OBJECT_BOOLEAN,
              "CPDF_Object::kBoolean value mismatch");
static_assert(static_cast<int>(CPDF_Object::Type::kNumber) ==
                  FPDF_OBJECT_NUMBER,
              "CPDF_Object::kNumber value mismatch");
static_assert(static_cast<int>(CPDF_Object::Type::kString) ==
                  FPDF_OBJECT_STRING,
              "CPDF_Object::kString value mismatch");
static_assert(static_cast<int>(CPDF_Object::Type::kName) == FPDF_OBJECT_NAME,
              "CPDF_Object::kName value mismatch");
static_assert(static_cast<int>(CPDF_Object::Type::kArray) == FPDF_OBJECT_ARRAY,
              "CPDF_Object::kArray value mismatch");
static_assert(static_cast<int>(CPDF_Object::Type::kDictionary) ==
                  FPDF_OBJECT_DICTIONARY,
              "CPDF_Object::kDictionary value mismatch");
static_assert(static_cast<int>(CPDF_Object::Type::kStream) ==
                  FPDF_OBJECT_STREAM,
              "CPDF_Object::kStream value mismatch");
static_assert(static_cast<int>(CPDF_Object::Type::kNullobj) ==
                  FPDF_OBJECT_NULLOBJ,
              "CPDF_Object::kNullobj value mismatch");
static_assert(static_cast<int>(CPDF_Object::Type::kReference) ==
                  FPDF_OBJECT_REFERENCE,
              "CPDF_Object::kReference value mismatch");

// These checks ensure the consistency of annotation additional action event
// values across core/ and public.
static_assert(static_cast<int>(CPDF_AAction::kKeyStroke) ==
                  FPDF_ANNOT_AACTION_KEY_STROKE,
              "CPDF_AAction::kKeyStroke value mismatch");
static_assert(static_cast<int>(CPDF_AAction::kFormat) ==
                  FPDF_ANNOT_AACTION_FORMAT,
              "CPDF_AAction::kFormat value mismatch");
static_assert(static_cast<int>(CPDF_AAction::kValidate) ==
                  FPDF_ANNOT_AACTION_VALIDATE,
              "CPDF_AAction::kValidate value mismatch");
static_assert(static_cast<int>(CPDF_AAction::kCalculate) ==
                  FPDF_ANNOT_AACTION_CALCULATE,
              "CPDF_AAction::kCalculate value mismatch");

bool HasAPStream(CPDF_Dictionary* pAnnotDict) {
  return !!GetAnnotAP(pAnnotDict, CPDF_Annot::AppearanceMode::kNormal);
}

void UpdateContentStream(CPDF_Form* pForm, CPDF_Stream* pStream) {
  DCHECK(pForm);
  DCHECK(pStream);

  CPDF_PageContentGenerator generator(pForm);
  fxcrt::ostringstream buf;
  generator.ProcessPageObjects(&buf);
  pStream->SetDataFromStringstreamAndRemoveFilter(&buf);
}

void SetQuadPointsAtIndex(CPDF_Array* array,
                          size_t quad_index,
                          const FS_QUADPOINTSF* quad_points) {
  DCHECK(array);
  DCHECK(quad_points);
  DCHECK(IsValidQuadPointsIndex(array, quad_index));

  size_t nIndex = quad_index * 8;
  array->SetNewAt<CPDF_Number>(nIndex, quad_points->x1);
  array->SetNewAt<CPDF_Number>(++nIndex, quad_points->y1);
  array->SetNewAt<CPDF_Number>(++nIndex, quad_points->x2);
  array->SetNewAt<CPDF_Number>(++nIndex, quad_points->y2);
  array->SetNewAt<CPDF_Number>(++nIndex, quad_points->x3);
  array->SetNewAt<CPDF_Number>(++nIndex, quad_points->y3);
  array->SetNewAt<CPDF_Number>(++nIndex, quad_points->x4);
  array->SetNewAt<CPDF_Number>(++nIndex, quad_points->y4);
}

void AppendQuadPoints(CPDF_Array* array, const FS_QUADPOINTSF* quad_points) {
  DCHECK(quad_points);
  DCHECK(array);

  array->AppendNew<CPDF_Number>(quad_points->x1);
  array->AppendNew<CPDF_Number>(quad_points->y1);
  array->AppendNew<CPDF_Number>(quad_points->x2);
  array->AppendNew<CPDF_Number>(quad_points->y2);
  array->AppendNew<CPDF_Number>(quad_points->x3);
  array->AppendNew<CPDF_Number>(quad_points->y3);
  array->AppendNew<CPDF_Number>(quad_points->x4);
  array->AppendNew<CPDF_Number>(quad_points->y4);
}

void UpdateBBox(CPDF_Dictionary* annot_dict) {
  DCHECK(annot_dict);
  // Update BBox entry in appearance stream based on the bounding rectangle
  // of the annotation's quadpoints.
  RetainPtr<CPDF_Stream> pStream =
      GetAnnotAP(annot_dict, CPDF_Annot::AppearanceMode::kNormal);
  if (pStream) {
    CFX_FloatRect boundingRect =
        CPDF_Annot::BoundingRectFromQuadPoints(annot_dict);
    if (boundingRect.Contains(pStream->GetDict()->GetRectFor("BBox")))
      pStream->GetMutableDict()->SetRectFor("BBox", boundingRect);
  }
}

const CPDF_Dictionary* GetAnnotDictFromFPDFAnnotation(
    const FPDF_ANNOTATION annot) {
  CPDF_AnnotContext* context = CPDFAnnotContextFromFPDFAnnotation(annot);
  return context ? context->GetAnnotDict() : nullptr;
}

RetainPtr<CPDF_Dictionary> GetMutableAnnotDictFromFPDFAnnotation(
    FPDF_ANNOTATION annot) {
  CPDF_AnnotContext* context = CPDFAnnotContextFromFPDFAnnotation(annot);
  return context ? context->GetMutableAnnotDict() : nullptr;
}

RetainPtr<CPDF_Dictionary> SetExtGStateInResourceDict(
    CPDF_Document* pDoc,
    const CPDF_Dictionary* pAnnotDict,
    const ByteString& sBlendMode) {
  auto pGSDict =
      pdfium::MakeRetain<CPDF_Dictionary>(pAnnotDict->GetByteStringPool());

  // ExtGState represents a graphics state parameter dictionary.
  pGSDict->SetNewFor<CPDF_Name>("Type", "ExtGState");

  // CA respresents current stroking alpha specifying constant opacity
  // value that should be used in transparent imaging model.
  float fOpacity = pAnnotDict->GetFloatFor("CA");

  pGSDict->SetNewFor<CPDF_Number>("CA", fOpacity);

  // ca represents fill color alpha specifying constant opacity
  // value that should be used in transparent imaging model.
  pGSDict->SetNewFor<CPDF_Number>("ca", fOpacity);

  // AIS represents alpha source flag specifying whether current alpha
  // constant shall be interpreted as shape value (true) or opacity value
  // (false).
  pGSDict->SetNewFor<CPDF_Boolean>("AIS", false);

  // BM represents Blend Mode
  pGSDict->SetNewFor<CPDF_Name>("BM", sBlendMode);

  auto pExtGStateDict =
      pdfium::MakeRetain<CPDF_Dictionary>(pAnnotDict->GetByteStringPool());

  pExtGStateDict->SetFor("GS", pGSDict);

  auto pResourceDict = pDoc->New<CPDF_Dictionary>();
  pResourceDict->SetFor("ExtGState", pExtGStateDict);
  return pResourceDict;
}

CPDF_FormField* GetFormField(FPDF_FORMHANDLE hHandle, FPDF_ANNOTATION annot) {
  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return nullptr;

  CPDFSDK_InteractiveForm* pForm = FormHandleToInteractiveForm(hHandle);
  if (!pForm)
    return nullptr;

  CPDF_InteractiveForm* pPDFForm = pForm->GetInteractiveForm();
  return pPDFForm->GetFieldByDict(pAnnotDict);
}

// If `allowed_types` is empty, then match all types.
const CPDFSDK_Widget* GetWidgetOfTypes(
    FPDF_FORMHANDLE hHandle,
    FPDF_ANNOTATION annot,
    pdfium::span<const CPDF_FormField::Type> allowed_types) {
  const CPDF_Dictionary* annot_dict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!annot_dict) {
    return nullptr;
  }

  CPDFSDK_InteractiveForm* form = FormHandleToInteractiveForm(hHandle);
  if (!form) {
    return nullptr;
  }

  CPDF_InteractiveForm* pdf_form = form->GetInteractiveForm();
  CPDF_FormField* form_field = pdf_form->GetFieldByDict(annot_dict);
  if (!form_field) {
    return nullptr;
  }

  if (!allowed_types.empty()) {
    if (!pdfium::Contains(allowed_types, form_field->GetType())) {
      return nullptr;
    }
  }

  CPDF_FormControl* form_control = pdf_form->GetControlByDict(annot_dict);
  return form_control ? form->GetWidget(form_control) : nullptr;
}

const CPDFSDK_Widget* GetRadioButtonOrCheckBoxWidget(FPDF_FORMHANDLE handle,
                                                     FPDF_ANNOTATION annot) {
  constexpr std::array<CPDF_FormField::Type, 2> kAllowedTypes = {
      CPDF_FormField::kCheckBox, CPDF_FormField::kRadioButton};
  return GetWidgetOfTypes(handle, annot, kAllowedTypes);
}

RetainPtr<const CPDF_Array> GetInkList(FPDF_ANNOTATION annot) {
  FPDF_ANNOTATION_SUBTYPE subtype = FPDFAnnot_GetSubtype(annot);
  if (subtype != FPDF_ANNOT_INK)
    return nullptr;

  const CPDF_Dictionary* annot_dict = GetAnnotDictFromFPDFAnnotation(annot);
  return annot_dict ? annot_dict->GetArrayFor(pdfium::annotation::kInkList)
                    : nullptr;
}

}  // namespace

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_IsSupportedSubtype(FPDF_ANNOTATION_SUBTYPE subtype) {
  // The supported subtypes must also be communicated in the user doc.
  switch (subtype) {
    case FPDF_ANNOT_CIRCLE:
    case FPDF_ANNOT_FILEATTACHMENT:
    case FPDF_ANNOT_FREETEXT:
    case FPDF_ANNOT_HIGHLIGHT:
    case FPDF_ANNOT_INK:
    case FPDF_ANNOT_LINK:
    case FPDF_ANNOT_POPUP:
    case FPDF_ANNOT_SQUARE:
    case FPDF_ANNOT_SQUIGGLY:
    case FPDF_ANNOT_STAMP:
    case FPDF_ANNOT_STRIKEOUT:
    case FPDF_ANNOT_TEXT:
    case FPDF_ANNOT_UNDERLINE:
      return true;
    default:
      return false;
  }
}

FPDF_EXPORT FPDF_ANNOTATION FPDF_CALLCONV
FPDFPage_CreateAnnot(FPDF_PAGE page, FPDF_ANNOTATION_SUBTYPE subtype) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage || !FPDFAnnot_IsSupportedSubtype(subtype))
    return nullptr;

  auto pDict = pPage->GetDocument()->New<CPDF_Dictionary>();
  pDict->SetNewFor<CPDF_Name>(pdfium::annotation::kType, "Annot");
  pDict->SetNewFor<CPDF_Name>(pdfium::annotation::kSubtype,
                              CPDF_Annot::AnnotSubtypeToString(
                                  static_cast<CPDF_Annot::Subtype>(subtype)));
  auto pNewAnnot =
      std::make_unique<CPDF_AnnotContext>(pDict, IPDFPageFromFPDFPage(page));

  RetainPtr<CPDF_Array> pAnnotList = pPage->GetOrCreateAnnotsArray();
  pAnnotList->Append(pDict);

  // Caller takes ownership.
  return FPDFAnnotationFromCPDFAnnotContext(pNewAnnot.release());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFPage_GetAnnotCount(FPDF_PAGE page) {
  const CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage)
    return 0;

  RetainPtr<const CPDF_Array> pAnnots = pPage->GetAnnotsArray();
  return pAnnots ? fxcrt::CollectionSize<int>(*pAnnots) : 0;
}

FPDF_EXPORT FPDF_ANNOTATION FPDF_CALLCONV FPDFPage_GetAnnot(FPDF_PAGE page,
                                                            int index) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage || index < 0)
    return nullptr;

  RetainPtr<CPDF_Array> pAnnots = pPage->GetMutableAnnotsArray();
  if (!pAnnots || static_cast<size_t>(index) >= pAnnots->size())
    return nullptr;

  RetainPtr<CPDF_Dictionary> pDict =
      ToDictionary(pAnnots->GetMutableDirectObjectAt(index));
  if (!pDict)
    return nullptr;

  auto pNewAnnot = std::make_unique<CPDF_AnnotContext>(
      std::move(pDict), IPDFPageFromFPDFPage(page));

  // Caller takes ownership.
  return FPDFAnnotationFromCPDFAnnotContext(pNewAnnot.release());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFPage_GetAnnotIndex(FPDF_PAGE page,
                                                     FPDF_ANNOTATION annot) {
  const CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage)
    return -1;

  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return -1;

  RetainPtr<const CPDF_Array> pAnnots = pPage->GetAnnotsArray();
  if (!pAnnots)
    return -1;

  CPDF_ArrayLocker locker(pAnnots);
  auto it = std::find_if(locker.begin(), locker.end(),
                         [pAnnotDict](const RetainPtr<CPDF_Object>& candidate) {
                           return candidate->GetDirect() == pAnnotDict;
                         });

  if (it == locker.end())
    return -1;

  return pdfium::checked_cast<int>(it - locker.begin());
}

FPDF_EXPORT void FPDF_CALLCONV FPDFPage_CloseAnnot(FPDF_ANNOTATION annot) {
  delete CPDFAnnotContextFromFPDFAnnotation(annot);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFPage_RemoveAnnot(FPDF_PAGE page,
                                                         int index) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage || index < 0)
    return false;

  RetainPtr<CPDF_Array> pAnnots = pPage->GetMutableAnnotsArray();
  if (!pAnnots || static_cast<size_t>(index) >= pAnnots->size())
    return false;

  pAnnots->RemoveAt(index);
  return true;
}

FPDF_EXPORT FPDF_ANNOTATION_SUBTYPE FPDF_CALLCONV
FPDFAnnot_GetSubtype(FPDF_ANNOTATION annot) {
  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return FPDF_ANNOT_UNKNOWN;

  return static_cast<FPDF_ANNOTATION_SUBTYPE>(CPDF_Annot::StringToAnnotSubtype(
      pAnnotDict->GetNameFor(pdfium::annotation::kSubtype)));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_IsObjectSupportedSubtype(FPDF_ANNOTATION_SUBTYPE subtype) {
  // The supported subtypes must also be communicated in the user doc.
  return subtype == FPDF_ANNOT_INK || subtype == FPDF_ANNOT_STAMP;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_UpdateObject(FPDF_ANNOTATION annot, FPDF_PAGEOBJECT obj) {
  CPDF_AnnotContext* pAnnot = CPDFAnnotContextFromFPDFAnnotation(annot);
  CPDF_PageObject* pObj = CPDFPageObjectFromFPDFPageObject(obj);
  if (!pAnnot || !pAnnot->HasForm() || !pObj)
    return false;

  // Check that the annotation type is supported by this method.
  if (!FPDFAnnot_IsObjectSupportedSubtype(FPDFAnnot_GetSubtype(annot)))
    return false;

  // Check that the annotation already has an appearance stream, since an
  // existing object is to be updated.
  RetainPtr<CPDF_Dictionary> pAnnotDict = pAnnot->GetMutableAnnotDict();
  RetainPtr<CPDF_Stream> pStream =
      GetAnnotAP(pAnnotDict.Get(), CPDF_Annot::AppearanceMode::kNormal);
  if (!pStream)
    return false;

  // Check that the object is already in this annotation's object list.
  CPDF_Form* pForm = pAnnot->GetForm();
  if (!pdfium::Contains(*pForm, fxcrt::MakeFakeUniquePtr(pObj)))
    return false;

  // Update the content stream data in the annotation's AP stream.
  UpdateContentStream(pForm, pStream.Get());
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAnnot_AddInkStroke(FPDF_ANNOTATION annot,
                                                     const FS_POINTF* points,
                                                     size_t point_count) {
  if (FPDFAnnot_GetSubtype(annot) != FPDF_ANNOT_INK || !points ||
      point_count == 0 ||
      !pdfium::IsValueInRangeForNumericType<int32_t>(point_count)) {
    return -1;
  }

  RetainPtr<CPDF_Dictionary> annot_dict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  RetainPtr<CPDF_Array> inklist = annot_dict->GetOrCreateArrayFor("InkList");
  FX_SAFE_SIZE_T safe_ink_size = inklist->size();
  safe_ink_size += 1;
  if (!safe_ink_size.IsValid<int32_t>())
    return -1;

  // SAFETY: required from caller.
  auto points_span = UNSAFE_BUFFERS(pdfium::make_span(points, point_count));
  auto ink_coord_list = inklist->AppendNew<CPDF_Array>();
  for (const auto& point : points_span) {
    ink_coord_list->AppendNew<CPDF_Number>(point.x);
    ink_coord_list->AppendNew<CPDF_Number>(point.y);
  }
  return static_cast<int>(inklist->size() - 1);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_RemoveInkList(FPDF_ANNOTATION annot) {
  if (FPDFAnnot_GetSubtype(annot) != FPDF_ANNOT_INK)
    return false;

  RetainPtr<CPDF_Dictionary> annot_dict =
      CPDFAnnotContextFromFPDFAnnotation(annot)->GetMutableAnnotDict();
  annot_dict->RemoveFor("InkList");
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_AppendObject(FPDF_ANNOTATION annot, FPDF_PAGEOBJECT obj) {
  CPDF_AnnotContext* pAnnot = CPDFAnnotContextFromFPDFAnnotation(annot);
  CPDF_PageObject* pObj = CPDFPageObjectFromFPDFPageObject(obj);
  if (!pAnnot || !pObj)
    return false;

  // Check that the annotation type is supported by this method.
  if (!FPDFAnnot_IsObjectSupportedSubtype(FPDFAnnot_GetSubtype(annot)))
    return false;

  // If the annotation does not have an AP stream yet, generate and set it.
  RetainPtr<CPDF_Dictionary> pAnnotDict = pAnnot->GetMutableAnnotDict();
  RetainPtr<CPDF_Stream> pStream =
      GetAnnotAP(pAnnotDict.Get(), CPDF_Annot::AppearanceMode::kNormal);
  if (!pStream) {
    CPDF_GenerateAP::GenerateEmptyAP(pAnnot->GetPage()->GetDocument(),
                                     pAnnotDict.Get());
    pStream = GetAnnotAP(pAnnotDict.Get(), CPDF_Annot::AppearanceMode::kNormal);
    if (!pStream)
      return false;
  }

  // Get the annotation's corresponding form object for parsing its AP stream.
  if (!pAnnot->HasForm())
    pAnnot->SetForm(pStream);

  // Check that the object did not come from the same annotation. If this check
  // succeeds, then it is assumed that the object came from
  // FPDFPageObj_CreateNew{Path|Rect}() or FPDFPageObj_New{Text|Image}Obj().
  // Note that an object that came from a different annotation must not be
  // passed here, since an object cannot belong to more than one annotation.
  CPDF_Form* pForm = pAnnot->GetForm();
  if (pdfium::Contains(*pForm, fxcrt::MakeFakeUniquePtr(pObj)))
    return false;

  // Append the object to the object list.
  pForm->AppendPageObject(pdfium::WrapUnique(pObj));

  // Set the content stream data in the annotation's AP stream.
  UpdateContentStream(pForm, pStream.Get());
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAnnot_GetObjectCount(FPDF_ANNOTATION annot) {
  CPDF_AnnotContext* pAnnot = CPDFAnnotContextFromFPDFAnnotation(annot);
  if (!pAnnot)
    return 0;

  if (!pAnnot->HasForm()) {
    RetainPtr<CPDF_Dictionary> pDict = pAnnot->GetMutableAnnotDict();
    RetainPtr<CPDF_Stream> pStream =
        GetAnnotAP(pDict.Get(), CPDF_Annot::AppearanceMode::kNormal);
    if (!pStream)
      return 0;

    pAnnot->SetForm(std::move(pStream));
  }
  return pdfium::checked_cast<int>(pAnnot->GetForm()->GetPageObjectCount());
}

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV
FPDFAnnot_GetObject(FPDF_ANNOTATION annot, int index) {
  CPDF_AnnotContext* pAnnot = CPDFAnnotContextFromFPDFAnnotation(annot);
  if (!pAnnot || index < 0)
    return nullptr;

  if (!pAnnot->HasForm()) {
    RetainPtr<CPDF_Dictionary> pAnnotDict = pAnnot->GetMutableAnnotDict();
    RetainPtr<CPDF_Stream> pStream =
        GetAnnotAP(pAnnotDict.Get(), CPDF_Annot::AppearanceMode::kNormal);
    if (!pStream)
      return nullptr;

    pAnnot->SetForm(std::move(pStream));
  }

  return FPDFPageObjectFromCPDFPageObject(
      pAnnot->GetForm()->GetPageObjectByIndex(index));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_RemoveObject(FPDF_ANNOTATION annot, int index) {
  CPDF_AnnotContext* pAnnot = CPDFAnnotContextFromFPDFAnnotation(annot);
  if (!pAnnot || !pAnnot->HasForm() || index < 0)
    return false;

  // Check that the annotation type is supported by this method.
  if (!FPDFAnnot_IsObjectSupportedSubtype(FPDFAnnot_GetSubtype(annot)))
    return false;

  // Check that the annotation already has an appearance stream, since an
  // existing object is to be deleted.
  RetainPtr<CPDF_Dictionary> pAnnotDict = pAnnot->GetMutableAnnotDict();
  RetainPtr<CPDF_Stream> pStream =
      GetAnnotAP(pAnnotDict.Get(), CPDF_Annot::AppearanceMode::kNormal);
  if (!pStream)
    return false;

  if (!pAnnot->GetForm()->ErasePageObjectAtIndex(index))
    return false;

  UpdateContentStream(pAnnot->GetForm(), pStream.Get());
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_SetColor(FPDF_ANNOTATION annot,
                                                       FPDFANNOT_COLORTYPE type,
                                                       unsigned int R,
                                                       unsigned int G,
                                                       unsigned int B,
                                                       unsigned int A) {
  RetainPtr<CPDF_Dictionary> pAnnotDict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);

  if (!pAnnotDict || R > 255 || G > 255 || B > 255 || A > 255)
    return false;

  // For annotations with their appearance streams already defined, the path
  // stream's own color definitions take priority over the annotation color
  // definitions set by this method, hence this method will simply fail.
  if (HasAPStream(pAnnotDict.Get()))
    return false;

  // Set the opacity of the annotation.
  pAnnotDict->SetNewFor<CPDF_Number>("CA", A / 255.f);

  // Set the color of the annotation.
  ByteString key = type == FPDFANNOT_COLORTYPE_InteriorColor ? "IC" : "C";
  RetainPtr<CPDF_Array> pColor = pAnnotDict->GetMutableArrayFor(key);
  if (pColor)
    pColor->Clear();
  else
    pColor = pAnnotDict->SetNewFor<CPDF_Array>(key);

  pColor->AppendNew<CPDF_Number>(R / 255.f);
  pColor->AppendNew<CPDF_Number>(G / 255.f);
  pColor->AppendNew<CPDF_Number>(B / 255.f);

  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_GetColor(FPDF_ANNOTATION annot,
                                                       FPDFANNOT_COLORTYPE type,
                                                       unsigned int* R,
                                                       unsigned int* G,
                                                       unsigned int* B,
                                                       unsigned int* A) {
  RetainPtr<CPDF_Dictionary> pAnnotDict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);

  if (!pAnnotDict || !R || !G || !B || !A)
    return false;

  // For annotations with their appearance streams already defined, the path
  // stream's own color definitions take priority over the annotation color
  // definitions retrieved by this method, hence this method will simply fail.
  if (HasAPStream(pAnnotDict.Get()))
    return false;

  RetainPtr<const CPDF_Array> pColor = pAnnotDict->GetArrayFor(
      type == FPDFANNOT_COLORTYPE_InteriorColor ? "IC" : "C");
  *A = (pAnnotDict->KeyExist("CA") ? pAnnotDict->GetFloatFor("CA") : 1) * 255.f;
  if (!pColor) {
    // Use default color. The default colors must be consistent with the ones
    // used to generate AP. See calls to GetColorStringWithDefault() in
    // CPDF_GenerateAP::Generate*AP().
    if (pAnnotDict->GetNameFor(pdfium::annotation::kSubtype) == "Highlight") {
      *R = 255;
      *G = 255;
      *B = 0;
    } else {
      *R = 0;
      *G = 0;
      *B = 0;
    }
    return true;
  }

  CFX_Color color = fpdfdoc::CFXColorFromArray(*pColor);
  switch (color.nColorType) {
    case CFX_Color::Type::kRGB:
      *R = color.fColor1 * 255.f;
      *G = color.fColor2 * 255.f;
      *B = color.fColor3 * 255.f;
      break;
    case CFX_Color::Type::kGray:
      *R = 255.f * color.fColor1;
      *G = 255.f * color.fColor1;
      *B = 255.f * color.fColor1;
      break;
    case CFX_Color::Type::kCMYK:
      *R = 255.f * (1 - color.fColor1) * (1 - color.fColor4);
      *G = 255.f * (1 - color.fColor2) * (1 - color.fColor4);
      *B = 255.f * (1 - color.fColor3) * (1 - color.fColor4);
      break;
    case CFX_Color::Type::kTransparent:
      *R = 0;
      *G = 0;
      *B = 0;
      break;
  }
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_HasAttachmentPoints(FPDF_ANNOTATION annot) {
  if (!annot)
    return false;

  FPDF_ANNOTATION_SUBTYPE subtype = FPDFAnnot_GetSubtype(annot);
  return subtype == FPDF_ANNOT_LINK || subtype == FPDF_ANNOT_HIGHLIGHT ||
         subtype == FPDF_ANNOT_UNDERLINE || subtype == FPDF_ANNOT_SQUIGGLY ||
         subtype == FPDF_ANNOT_STRIKEOUT;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_SetAttachmentPoints(FPDF_ANNOTATION annot,
                              size_t quad_index,
                              const FS_QUADPOINTSF* quad_points) {
  if (!FPDFAnnot_HasAttachmentPoints(annot) || !quad_points)
    return false;

  RetainPtr<CPDF_Dictionary> pAnnotDict =
      CPDFAnnotContextFromFPDFAnnotation(annot)->GetMutableAnnotDict();
  RetainPtr<CPDF_Array> pQuadPointsArray =
      GetMutableQuadPointsArrayFromDictionary(pAnnotDict.Get());
  if (!IsValidQuadPointsIndex(pQuadPointsArray.Get(), quad_index))
    return false;

  SetQuadPointsAtIndex(pQuadPointsArray.Get(), quad_index, quad_points);
  UpdateBBox(pAnnotDict.Get());
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_AppendAttachmentPoints(FPDF_ANNOTATION annot,
                                 const FS_QUADPOINTSF* quad_points) {
  if (!FPDFAnnot_HasAttachmentPoints(annot) || !quad_points)
    return false;

  RetainPtr<CPDF_Dictionary> pAnnotDict =
      CPDFAnnotContextFromFPDFAnnotation(annot)->GetMutableAnnotDict();
  RetainPtr<CPDF_Array> pQuadPointsArray =
      GetMutableQuadPointsArrayFromDictionary(pAnnotDict.Get());
  if (!pQuadPointsArray)
    pQuadPointsArray = AddQuadPointsArrayToDictionary(pAnnotDict.Get());
  AppendQuadPoints(pQuadPointsArray.Get(), quad_points);
  UpdateBBox(pAnnotDict.Get());
  return true;
}

FPDF_EXPORT size_t FPDF_CALLCONV
FPDFAnnot_CountAttachmentPoints(FPDF_ANNOTATION annot) {
  if (!FPDFAnnot_HasAttachmentPoints(annot))
    return 0;

  const CPDF_Dictionary* pAnnotDict =
      CPDFAnnotContextFromFPDFAnnotation(annot)->GetAnnotDict();
  RetainPtr<const CPDF_Array> pArray =
      GetQuadPointsArrayFromDictionary(pAnnotDict);
  return pArray ? pArray->size() / 8 : 0;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_GetAttachmentPoints(FPDF_ANNOTATION annot,
                              size_t quad_index,
                              FS_QUADPOINTSF* quad_points) {
  if (!FPDFAnnot_HasAttachmentPoints(annot) || !quad_points)
    return false;

  const CPDF_Dictionary* pAnnotDict =
      CPDFAnnotContextFromFPDFAnnotation(annot)->GetAnnotDict();
  RetainPtr<const CPDF_Array> pArray =
      GetQuadPointsArrayFromDictionary(pAnnotDict);
  if (!pArray)
    return false;

  return GetQuadPointsAtIndex(std::move(pArray), quad_index, quad_points);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_SetRect(FPDF_ANNOTATION annot,
                                                      const FS_RECTF* rect) {
  RetainPtr<CPDF_Dictionary> pAnnotDict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict || !rect)
    return false;

  CFX_FloatRect newRect = CFXFloatRectFromFSRectF(*rect);

  // Update the "Rect" entry in the annotation dictionary.
  pAnnotDict->SetRectFor(pdfium::annotation::kRect, newRect);

  // If the annotation's appearance stream is defined, the annotation is of a
  // type that does not have quadpoints, and the new rectangle is bigger than
  // the current bounding box, then update the "BBox" entry in the AP
  // dictionary too, since its "BBox" entry comes from annotation dictionary's
  // "Rect" entry.
  if (FPDFAnnot_HasAttachmentPoints(annot))
    return true;

  RetainPtr<CPDF_Stream> pStream =
      GetAnnotAP(pAnnotDict.Get(), CPDF_Annot::AppearanceMode::kNormal);
  if (pStream && newRect.Contains(pStream->GetDict()->GetRectFor("BBox")))
    pStream->GetMutableDict()->SetRectFor("BBox", newRect);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_GetRect(FPDF_ANNOTATION annot,
                                                      FS_RECTF* rect) {
  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict || !rect)
    return false;

  *rect = FSRectFFromCFXFloatRect(
      pAnnotDict->GetRectFor(pdfium::annotation::kRect));
  return true;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetVertices(FPDF_ANNOTATION annot,
                      FS_POINTF* buffer,
                      unsigned long length) {
  FPDF_ANNOTATION_SUBTYPE subtype = FPDFAnnot_GetSubtype(annot);
  if (subtype != FPDF_ANNOT_POLYGON && subtype != FPDF_ANNOT_POLYLINE)
    return 0;

  const CPDF_Dictionary* annot_dict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!annot_dict)
    return 0;

  RetainPtr<const CPDF_Array> vertices =
      annot_dict->GetArrayFor(pdfium::annotation::kVertices);
  if (!vertices)
    return 0;

  // Truncate to an even number.
  const unsigned long points_len =
      fxcrt::CollectionSize<unsigned long>(*vertices) / 2;
  if (buffer && length >= points_len) {
    // SAFETY: required from caller.
    auto buffer_span = UNSAFE_BUFFERS(pdfium::make_span(buffer, length));
    for (unsigned long i = 0; i < points_len; ++i) {
      buffer_span[i].x = vertices->GetFloatAt(i * 2);
      buffer_span[i].y = vertices->GetFloatAt(i * 2 + 1);
    }
  }
  return points_len;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetInkListCount(FPDF_ANNOTATION annot) {
  RetainPtr<const CPDF_Array> ink_list = GetInkList(annot);
  return ink_list ? fxcrt::CollectionSize<unsigned long>(*ink_list) : 0;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetInkListPath(FPDF_ANNOTATION annot,
                         unsigned long path_index,
                         FS_POINTF* buffer,
                         unsigned long length) {
  RetainPtr<const CPDF_Array> ink_list = GetInkList(annot);
  if (!ink_list)
    return 0;

  RetainPtr<const CPDF_Array> path = ink_list->GetArrayAt(path_index);
  if (!path)
    return 0;

  // Truncate to an even number.
  const unsigned long points_len =
      fxcrt::CollectionSize<unsigned long>(*path) / 2;
  if (buffer && length >= points_len) {
    // SAFETY: required from caller.
    auto buffer_span = UNSAFE_BUFFERS(pdfium::make_span(buffer, length));
    for (unsigned long i = 0; i < points_len; ++i) {
      buffer_span[i].x = path->GetFloatAt(i * 2);
      buffer_span[i].y = path->GetFloatAt(i * 2 + 1);
    }
  }
  return points_len;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_GetLine(FPDF_ANNOTATION annot,
                                                      FS_POINTF* start,
                                                      FS_POINTF* end) {
  if (!start || !end)
    return false;

  FPDF_ANNOTATION_SUBTYPE subtype = FPDFAnnot_GetSubtype(annot);
  if (subtype != FPDF_ANNOT_LINE)
    return false;

  const CPDF_Dictionary* annot_dict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!annot_dict)
    return false;

  RetainPtr<const CPDF_Array> line =
      annot_dict->GetArrayFor(pdfium::annotation::kL);
  if (!line || line->size() < 4)
    return false;

  start->x = line->GetFloatAt(0);
  start->y = line->GetFloatAt(1);
  end->x = line->GetFloatAt(2);
  end->y = line->GetFloatAt(3);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_SetBorder(FPDF_ANNOTATION annot,
                                                        float horizontal_radius,
                                                        float vertical_radius,
                                                        float border_width) {
  RetainPtr<CPDF_Dictionary> annot_dict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  if (!annot_dict)
    return false;

  // Remove the appearance stream. Otherwise PDF viewers will render that and
  // not use the border values.
  annot_dict->RemoveFor(pdfium::annotation::kAP);

  auto border = annot_dict->SetNewFor<CPDF_Array>(pdfium::annotation::kBorder);
  border->AppendNew<CPDF_Number>(horizontal_radius);
  border->AppendNew<CPDF_Number>(vertical_radius);
  border->AppendNew<CPDF_Number>(border_width);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_GetBorder(FPDF_ANNOTATION annot,
                    float* horizontal_radius,
                    float* vertical_radius,
                    float* border_width) {
  if (!horizontal_radius || !vertical_radius || !border_width)
    return false;

  const CPDF_Dictionary* annot_dict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!annot_dict)
    return false;

  RetainPtr<const CPDF_Array> border =
      annot_dict->GetArrayFor(pdfium::annotation::kBorder);
  if (!border || border->size() < 3)
    return false;

  *horizontal_radius = border->GetFloatAt(0);
  *vertical_radius = border->GetFloatAt(1);
  *border_width = border->GetFloatAt(2);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_HasKey(FPDF_ANNOTATION annot,
                                                     FPDF_BYTESTRING key) {
  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  return pAnnotDict && pAnnotDict->KeyExist(key);
}

FPDF_EXPORT FPDF_OBJECT_TYPE FPDF_CALLCONV
FPDFAnnot_GetValueType(FPDF_ANNOTATION annot, FPDF_BYTESTRING key) {
  if (!FPDFAnnot_HasKey(annot, key))
    return FPDF_OBJECT_UNKNOWN;

  const CPDF_AnnotContext* pAnnot = CPDFAnnotContextFromFPDFAnnotation(annot);
  RetainPtr<const CPDF_Object> pObj = pAnnot->GetAnnotDict()->GetObjectFor(key);
  return pObj ? pObj->GetType() : FPDF_OBJECT_UNKNOWN;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_SetStringValue(FPDF_ANNOTATION annot,
                         FPDF_BYTESTRING key,
                         FPDF_WIDESTRING value) {
  RetainPtr<CPDF_Dictionary> pAnnotDict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return false;

  // SAFETY: required from caller.
  pAnnotDict->SetNewFor<CPDF_String>(
      key, UNSAFE_BUFFERS(WideStringFromFPDFWideString(value).AsStringView()));
  return true;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetStringValue(FPDF_ANNOTATION annot,
                         FPDF_BYTESTRING key,
                         FPDF_WCHAR* buffer,
                         unsigned long buflen) {
  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict) {
    return 0;
  }
  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      pAnnotDict->GetUnicodeTextFor(key),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_GetNumberValue(FPDF_ANNOTATION annot,
                         FPDF_BYTESTRING key,
                         float* value) {
  if (!value)
    return false;

  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return false;

  RetainPtr<const CPDF_Object> p = pAnnotDict->GetObjectFor(key);
  if (!p || p->GetType() != FPDF_OBJECT_NUMBER)
    return false;

  *value = p->GetNumber();
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_SetAP(FPDF_ANNOTATION annot,
                FPDF_ANNOT_APPEARANCEMODE appearanceMode,
                FPDF_WIDESTRING value) {
  RetainPtr<CPDF_Dictionary> pAnnotDict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return false;

  if (appearanceMode < 0 || appearanceMode >= FPDF_ANNOT_APPEARANCEMODE_COUNT)
    return false;

  static constexpr auto kModeKeyForMode =
      fxcrt::ToArray<const char*>({"N", "R", "D"});
  static_assert(kModeKeyForMode.size() == FPDF_ANNOT_APPEARANCEMODE_COUNT,
                "length of kModeKeyForMode should be equal to "
                "FPDF_ANNOT_APPEARANCEMODE_COUNT");

  const char* mode_key = kModeKeyForMode[appearanceMode];

  RetainPtr<CPDF_Dictionary> pApDict =
      pAnnotDict->GetMutableDictFor(pdfium::annotation::kAP);

  // If `value` is null, then the action is to remove.
  if (!value) {
    if (pApDict) {
      if (appearanceMode == FPDF_ANNOT_APPEARANCEMODE_NORMAL) {
        pAnnotDict->RemoveFor(pdfium::annotation::kAP);
      } else {
        pApDict->RemoveFor(mode_key);
      }
    }
    return true;
  }

  // Otherwise, add/update when `value` is non-null.
  //
  // Annotation object's non-empty bounding rect will be used as the /BBox
  // for the associated /XObject object
  CFX_FloatRect rect = pAnnotDict->GetRectFor(pdfium::annotation::kRect);
  constexpr float kMinSize = 0.000001f;
  if (rect.Width() < kMinSize || rect.Height() < kMinSize) {
    return false;
  }

  CPDF_AnnotContext* pAnnotContext = CPDFAnnotContextFromFPDFAnnotation(annot);

  CPDF_Document* pDoc = pAnnotContext->GetPage()->GetDocument();
  if (!pDoc) {
    return false;
  }

  auto stream_dict = pdfium::MakeRetain<CPDF_Dictionary>();
  stream_dict->SetNewFor<CPDF_Name>(pdfium::annotation::kType, "XObject");
  stream_dict->SetNewFor<CPDF_Name>(pdfium::annotation::kSubtype, "Form");
  stream_dict->SetRectFor("BBox", rect);
  // Transparency values are specified in range [0.0f, 1.0f]. We are strictly
  // checking for value < 1 and not <= 1 so that the output PDF size does not
  // unnecessarily bloat up by creating a new dictionary in case of solid
  // color.
  if (pAnnotDict->KeyExist("CA") && pAnnotDict->GetFloatFor("CA") < 1.0f) {
    stream_dict->SetFor("Resources", SetExtGStateInResourceDict(
                                         pDoc, pAnnotDict.Get(), "Normal"));
  }
  // SAFETY: required from caller.
  ByteString new_stream_data = PDF_EncodeText(
      UNSAFE_BUFFERS(WideStringFromFPDFWideString(value).AsStringView()));
  auto new_stream = pDoc->NewIndirect<CPDF_Stream>(std::move(stream_dict));
  new_stream->SetData(new_stream_data.unsigned_span());

  // Storing reference to indirect object in annotation's AP
  if (!pApDict) {
    pApDict = pAnnotDict->SetNewFor<CPDF_Dictionary>(pdfium::annotation::kAP);
  }
  pApDict->SetNewFor<CPDF_Reference>(mode_key, pDoc, new_stream->GetObjNum());

  return true;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetAP(FPDF_ANNOTATION annot,
                FPDF_ANNOT_APPEARANCEMODE appearanceMode,
                FPDF_WCHAR* buffer,
                unsigned long buflen) {
  RetainPtr<CPDF_Dictionary> pAnnotDict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return 0;

  if (appearanceMode < 0 || appearanceMode >= FPDF_ANNOT_APPEARANCEMODE_COUNT)
    return 0;

  CPDF_Annot::AppearanceMode mode =
      static_cast<CPDF_Annot::AppearanceMode>(appearanceMode);

  RetainPtr<CPDF_Stream> pStream = GetAnnotAPNoFallback(pAnnotDict.Get(), mode);
  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      pStream ? pStream->GetUnicodeText() : WideString(),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT FPDF_ANNOTATION FPDF_CALLCONV
FPDFAnnot_GetLinkedAnnot(FPDF_ANNOTATION annot, FPDF_BYTESTRING key) {
  CPDF_AnnotContext* pAnnot = CPDFAnnotContextFromFPDFAnnotation(annot);
  if (!pAnnot)
    return nullptr;

  RetainPtr<CPDF_Dictionary> pLinkedDict =
      pAnnot->GetMutableAnnotDict()->GetMutableDictFor(key);
  if (!pLinkedDict || pLinkedDict->GetNameFor("Type") != "Annot")
    return nullptr;

  auto pLinkedAnnot = std::make_unique<CPDF_AnnotContext>(
      std::move(pLinkedDict), pAnnot->GetPage());

  // Caller takes ownership.
  return FPDFAnnotationFromCPDFAnnotContext(pLinkedAnnot.release());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAnnot_GetFlags(FPDF_ANNOTATION annot) {
  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  return pAnnotDict ? pAnnotDict->GetIntegerFor(pdfium::annotation::kF)
                    : FPDF_ANNOT_FLAG_NONE;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_SetFlags(FPDF_ANNOTATION annot,
                                                       int flags) {
  RetainPtr<CPDF_Dictionary> pAnnotDict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return false;

  pAnnotDict->SetNewFor<CPDF_Number>(pdfium::annotation::kF, flags);
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFAnnot_GetFormFieldFlags(FPDF_FORMHANDLE hHandle, FPDF_ANNOTATION annot) {
  CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  return pFormField ? pFormField->GetFieldFlags() : FPDF_FORMFLAG_NONE;
}

FPDF_EXPORT FPDF_ANNOTATION FPDF_CALLCONV
FPDFAnnot_GetFormFieldAtPoint(FPDF_FORMHANDLE hHandle,
                              FPDF_PAGE page,
                              const FS_POINTF* point) {
  if (!point)
    return nullptr;

  const CPDFSDK_InteractiveForm* pForm = FormHandleToInteractiveForm(hHandle);
  if (!pForm)
    return nullptr;

  const CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage)
    return nullptr;

  const CPDF_InteractiveForm* pPDFForm = pForm->GetInteractiveForm();
  int annot_index = -1;
  const CPDF_FormControl* pFormCtrl = pPDFForm->GetControlAtPoint(
      pPage, CFXPointFFromFSPointF(*point), &annot_index);
  if (!pFormCtrl || annot_index == -1)
    return nullptr;
  return FPDFPage_GetAnnot(page, annot_index);
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetFormFieldName(FPDF_FORMHANDLE hHandle,
                           FPDF_ANNOTATION annot,
                           FPDF_WCHAR* buffer,
                           unsigned long buflen) {
  const CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  if (!pFormField) {
    return 0;
  }
  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      pFormField->GetFullName(),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFAnnot_GetFormFieldType(FPDF_FORMHANDLE hHandle, FPDF_ANNOTATION annot) {
  const CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  return pFormField ? static_cast<int>(pFormField->GetFieldType()) : -1;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetFormAdditionalActionJavaScript(FPDF_FORMHANDLE hHandle,
                                            FPDF_ANNOTATION annot,
                                            int event,
                                            FPDF_WCHAR* buffer,
                                            unsigned long buflen) {
  const CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  if (!pFormField)
    return 0;

  if (event < FPDF_ANNOT_AACTION_KEY_STROKE ||
      event > FPDF_ANNOT_AACTION_CALCULATE) {
    return 0;
  }

  auto type = static_cast<CPDF_AAction::AActionType>(event);
  CPDF_AAction additional_action = pFormField->GetAdditionalAction();
  CPDF_Action action = additional_action.GetAction(type);
  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      action.GetJavaScript(),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetFormFieldAlternateName(FPDF_FORMHANDLE hHandle,
                                    FPDF_ANNOTATION annot,
                                    FPDF_WCHAR* buffer,
                                    unsigned long buflen) {
  const CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  if (!pFormField) {
    return 0;
  }
  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      pFormField->GetAlternateName(),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetFormFieldValue(FPDF_FORMHANDLE hHandle,
                            FPDF_ANNOTATION annot,
                            FPDF_WCHAR* buffer,
                            unsigned long buflen) {
  const CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  if (!pFormField) {
    return 0;
  }
  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      pFormField->GetValue(),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAnnot_GetOptionCount(FPDF_FORMHANDLE hHandle,
                                                       FPDF_ANNOTATION annot) {
  const CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  return pFormField ? pFormField->CountOptions() : -1;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetOptionLabel(FPDF_FORMHANDLE hHandle,
                         FPDF_ANNOTATION annot,
                         int index,
                         FPDF_WCHAR* buffer,
                         unsigned long buflen) {
  if (index < 0)
    return 0;

  const CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  if (!pFormField || index >= pFormField->CountOptions())
    return 0;

  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      pFormField->GetOptionLabel(index),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_IsOptionSelected(FPDF_FORMHANDLE handle,
                           FPDF_ANNOTATION annot,
                           int index) {
  if (index < 0) {
    return false;
  }

  const CPDF_FormField* form_field = GetFormField(handle, annot);
  if (!form_field) {
    return false;
  }

  if (form_field->GetFieldType() != FormFieldType::kComboBox &&
      form_field->GetFieldType() != FormFieldType::kListBox) {
    return false;
  }

  return index < form_field->CountOptions() &&
         form_field->IsItemSelected(index);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_GetFontSize(FPDF_FORMHANDLE hHandle,
                      FPDF_ANNOTATION annot,
                      float* value) {
  if (!value) {
    return false;
  }

  const CPDFSDK_Widget* widget = GetWidgetOfTypes(hHandle, annot, {});
  if (!widget) {
    return false;
  }

  *value = widget->GetFontSize();
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_GetFontColor(FPDF_FORMHANDLE hHandle,
                       FPDF_ANNOTATION annot,
                       unsigned int* R,
                       unsigned int* G,
                       unsigned int* B) {
  if (!R || !G || !B) {
    return false;
  }

  const CPDFSDK_Widget* widget = GetWidgetOfTypes(hHandle, annot, {});
  if (!widget) {
    return false;
  }

  std::optional<FX_COLORREF> text_color = widget->GetTextColor();
  if (!text_color) {
    return false;
  }

  *R = FXSYS_GetRValue(*text_color);
  *G = FXSYS_GetGValue(*text_color);
  *B = FXSYS_GetBValue(*text_color);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_IsChecked(FPDF_FORMHANDLE hHandle,
                                                        FPDF_ANNOTATION annot) {
  const CPDFSDK_Widget* pWidget =
      GetRadioButtonOrCheckBoxWidget(hHandle, annot);
  return pWidget && pWidget->IsChecked();
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_SetFocusableSubtypes(FPDF_FORMHANDLE hHandle,
                               const FPDF_ANNOTATION_SUBTYPE* subtypes,
                               size_t count) {
  CPDFSDK_FormFillEnvironment* pFormFillEnv =
      CPDFSDKFormFillEnvironmentFromFPDFFormHandle(hHandle);
  if (!pFormFillEnv)
    return false;

  if (count > 0 && !subtypes)
    return false;

  // SAFETY: required from caller.
  auto subtypes_span = UNSAFE_BUFFERS(pdfium::make_span(subtypes, count));
  std::vector<CPDF_Annot::Subtype> focusable_annot_types;
  focusable_annot_types.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    focusable_annot_types.push_back(
        static_cast<CPDF_Annot::Subtype>(subtypes_span[i]));
  }

  pFormFillEnv->SetFocusableAnnotSubtypes(focusable_annot_types);
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFAnnot_GetFocusableSubtypesCount(FPDF_FORMHANDLE hHandle) {
  CPDFSDK_FormFillEnvironment* pFormFillEnv =
      CPDFSDKFormFillEnvironmentFromFPDFFormHandle(hHandle);
  if (!pFormFillEnv)
    return -1;

  return fxcrt::CollectionSize<int>(pFormFillEnv->GetFocusableAnnotSubtypes());
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAnnot_GetFocusableSubtypes(FPDF_FORMHANDLE hHandle,
                               FPDF_ANNOTATION_SUBTYPE* subtypes,
                               size_t count) {
  CPDFSDK_FormFillEnvironment* pFormFillEnv =
      CPDFSDKFormFillEnvironmentFromFPDFFormHandle(hHandle);
  if (!pFormFillEnv)
    return false;

  if (!subtypes)
    return false;

  const std::vector<CPDF_Annot::Subtype>& focusable_annot_types =
      pFormFillEnv->GetFocusableAnnotSubtypes();

  // Host should allocate enough memory to get the list of currently supported
  // focusable subtypes.
  if (count < focusable_annot_types.size())
    return false;

  // SAFETY: required from caller.
  auto subtypes_span = UNSAFE_BUFFERS(pdfium::make_span(subtypes, count));
  for (size_t i = 0; i < focusable_annot_types.size(); ++i) {
    subtypes_span[i] =
        static_cast<FPDF_ANNOTATION_SUBTYPE>(focusable_annot_types[i]);
  }
  return true;
}

FPDF_EXPORT FPDF_LINK FPDF_CALLCONV FPDFAnnot_GetLink(FPDF_ANNOTATION annot) {
  if (FPDFAnnot_GetSubtype(annot) != FPDF_ANNOT_LINK)
    return nullptr;

  // Unretained reference in public API. NOLINTNEXTLINE
  return FPDFLinkFromCPDFDictionary(
      CPDFAnnotContextFromFPDFAnnotation(annot)->GetMutableAnnotDict());
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFAnnot_GetFormControlCount(FPDF_FORMHANDLE hHandle, FPDF_ANNOTATION annot) {
  CPDF_FormField* pFormField = GetFormField(hHandle, annot);
  return pFormField ? pFormField->CountControls() : -1;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFAnnot_GetFormControlIndex(FPDF_FORMHANDLE hHandle, FPDF_ANNOTATION annot) {
  const CPDF_Dictionary* pAnnotDict = GetAnnotDictFromFPDFAnnotation(annot);
  if (!pAnnotDict)
    return -1;

  CPDFSDK_InteractiveForm* pForm = FormHandleToInteractiveForm(hHandle);
  if (!pForm)
    return -1;

  CPDF_InteractiveForm* pPDFForm = pForm->GetInteractiveForm();
  CPDF_FormField* pFormField = pPDFForm->GetFieldByDict(pAnnotDict);
  CPDF_FormControl* pFormControl = pPDFForm->GetControlByDict(pAnnotDict);
  return pFormField ? pFormField->GetControlIndex(pFormControl) : -1;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAnnot_GetFormFieldExportValue(FPDF_FORMHANDLE hHandle,
                                  FPDF_ANNOTATION annot,
                                  FPDF_WCHAR* buffer,
                                  unsigned long buflen) {
  const CPDFSDK_Widget* pWidget =
      GetRadioButtonOrCheckBoxWidget(hHandle, annot);
  if (!pWidget) {
    return 0;
  }
  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      pWidget->GetExportValue(),
      UNSAFE_TODO(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFAnnot_SetURI(FPDF_ANNOTATION annot,
                                                     const char* uri) {
  if (!uri || FPDFAnnot_GetSubtype(annot) != FPDF_ANNOT_LINK)
    return false;

  RetainPtr<CPDF_Dictionary> annot_dict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  auto action = annot_dict->SetNewFor<CPDF_Dictionary>("A");
  action->SetNewFor<CPDF_Name>("Type", "Action");
  action->SetNewFor<CPDF_Name>("S", "URI");
  action->SetNewFor<CPDF_String>("URI", uri);
  return true;
}

FPDF_EXPORT FPDF_ATTACHMENT FPDF_CALLCONV
FPDFAnnot_GetFileAttachment(FPDF_ANNOTATION annot) {
  if (FPDFAnnot_GetSubtype(annot) != FPDF_ANNOT_FILEATTACHMENT) {
    return nullptr;
  }

  RetainPtr<CPDF_Dictionary> annot_dict =
      GetMutableAnnotDictFromFPDFAnnotation(annot);
  if (!annot_dict) {
    return nullptr;
  }

  return FPDFAttachmentFromCPDFObject(
      annot_dict->GetMutableDirectObjectFor("FS"));
}

FPDF_EXPORT FPDF_ATTACHMENT FPDF_CALLCONV
FPDFAnnot_AddFileAttachment(FPDF_ANNOTATION annot, FPDF_WIDESTRING name) {
  if (FPDFAnnot_GetSubtype(annot) != FPDF_ANNOT_FILEATTACHMENT) {
    return nullptr;
  }

  CPDF_AnnotContext* context = CPDFAnnotContextFromFPDFAnnotation(annot);
  if (!context) {
    return nullptr;
  }

  RetainPtr<CPDF_Dictionary> annot_dict = context->GetMutableAnnotDict();
  if (!annot_dict) {
    return nullptr;
  }

  // SAFETY: required from caller.
  WideString ws_name = UNSAFE_BUFFERS(WideStringFromFPDFWideString(name));
  if (ws_name.IsEmpty()) {
    return nullptr;
  }

  CPDF_Document* doc = context->GetPage()->GetDocument();
  auto fs_obj = doc->NewIndirect<CPDF_Dictionary>();

  fs_obj->SetNewFor<CPDF_Name>("Type", "Filespec");
  fs_obj->SetNewFor<CPDF_String>("UF", ws_name.AsStringView());
  fs_obj->SetNewFor<CPDF_String>("F", ws_name.AsStringView());

  annot_dict->SetNewFor<CPDF_Reference>("FS", doc, fs_obj->GetObjNum());
  return FPDFAttachmentFromCPDFObject(fs_obj);
}
