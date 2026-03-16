// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_annot.h"

#include <algorithm>
#include <utility>

#include "build/build_config.h"
#include "constants/annotation_common.h"
#include "constants/annotation_flags.h"
#include "core/fpdfapi/page/cpdf_form.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_pageimagecache.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_boolean.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fpdfapi/render/cpdf_rendercontext.h"
#include "core/fpdfapi/render/cpdf_renderoptions.h"
#include "core/fpdfdoc/cpdf_generateap.h"
#include "core/fxcrt/check.h"
#include "core/fxge/cfx_fillrenderoptions.h"
#include "core/fxge/cfx_graphstatedata.h"
#include "core/fxge/cfx_path.h"
#include "core/fxge/cfx_renderdevice.h"

namespace {

const char kPDFiumKey_HasGeneratedAP[] = "PDFIUM_HasGeneratedAP";

bool IsTextMarkupAnnotation(CPDF_Annot::Subtype type) {
  return type == CPDF_Annot::Subtype::HIGHLIGHT ||
         type == CPDF_Annot::Subtype::SQUIGGLY ||
         type == CPDF_Annot::Subtype::STRIKEOUT ||
         type == CPDF_Annot::Subtype::UNDERLINE;
}

CPDF_Form* AnnotGetMatrix(CPDF_Page* pPage,
                          CPDF_Annot* pAnnot,
                          CPDF_Annot::AppearanceMode mode,
                          const CFX_Matrix& mtUser2Device,
                          CFX_Matrix* matrix) {
  CPDF_Form* pForm = pAnnot->GetAPForm(pPage, mode);
  if (!pForm)
    return nullptr;

  CFX_Matrix form_matrix = pForm->GetDict()->GetMatrixFor("Matrix");
  CFX_FloatRect form_bbox =
      form_matrix.TransformRect(pForm->GetDict()->GetRectFor("BBox"));
  matrix->MatchRect(pAnnot->GetRect(), form_bbox);

  // Compensate for page rotation.
  if ((pAnnot->GetFlags() & pdfium::annotation_flags::kNoRotate) &&
      pPage->GetPageRotation() != 0) {
    // Rotate annotation rect around top-left angle (according to the
    // specification).
    const float offset_x = pAnnot->GetRect().Left();
    const float offset_y = pAnnot->GetRect().Top();
    matrix->Concat({1, 0, 0, 1, -offset_x, -offset_y});
    // GetPageRotation returns value in fractions of pi/2.
    const float angle = FXSYS_PI / 2 * pPage->GetPageRotation();
    matrix->Rotate(angle);
    matrix->Concat({1, 0, 0, 1, offset_x, offset_y});
  }

  matrix->Concat(mtUser2Device);
  return pForm;
}

RetainPtr<CPDF_Stream> GetAnnotAPInternal(CPDF_Dictionary* pAnnotDict,
                                          CPDF_Annot::AppearanceMode eMode,
                                          bool bFallbackToNormal) {
  RetainPtr<CPDF_Dictionary> pAP =
      pAnnotDict->GetMutableDictFor(pdfium::annotation::kAP);
  if (!pAP)
    return nullptr;

  const char* ap_entry = "N";
  if (eMode == CPDF_Annot::AppearanceMode::kDown)
    ap_entry = "D";
  else if (eMode == CPDF_Annot::AppearanceMode::kRollover)
    ap_entry = "R";
  if (bFallbackToNormal && !pAP->KeyExist(ap_entry))
    ap_entry = "N";

  RetainPtr<CPDF_Object> psub = pAP->GetMutableDirectObjectFor(ap_entry);
  if (!psub)
    return nullptr;

  RetainPtr<CPDF_Stream> pStream(psub->AsMutableStream());
  if (pStream)
    return pStream;

  CPDF_Dictionary* pDict = psub->AsMutableDictionary();
  if (!pDict)
    return nullptr;

  ByteString as = pAnnotDict->GetByteStringFor(pdfium::annotation::kAS);
  if (as.IsEmpty()) {
    ByteString value = pAnnotDict->GetByteStringFor("V");
    if (value.IsEmpty()) {
      RetainPtr<const CPDF_Dictionary> pParentDict =
          pAnnotDict->GetDictFor("Parent");
      value = pParentDict ? pParentDict->GetByteStringFor("V") : ByteString();
    }
    as = (!value.IsEmpty() && pDict->KeyExist(value)) ? value : "Off";
  }
  return pDict->GetMutableStreamFor(as);
}

}  // namespace

CPDF_Annot::CPDF_Annot(RetainPtr<CPDF_Dictionary> pDict,
                       CPDF_Document* pDocument)
    : m_pAnnotDict(std::move(pDict)),
      m_pDocument(pDocument),
      m_nSubtype(StringToAnnotSubtype(
          m_pAnnotDict->GetByteStringFor(pdfium::annotation::kSubtype))),
      m_bIsTextMarkupAnnotation(IsTextMarkupAnnotation(m_nSubtype)),
      m_bHasGeneratedAP(
          m_pAnnotDict->GetBooleanFor(kPDFiumKey_HasGeneratedAP, false)) {
  GenerateAPIfNeeded();
}

CPDF_Annot::~CPDF_Annot() {
  ClearCachedAP();
}

void CPDF_Annot::GenerateAPIfNeeded() {
  if (!ShouldGenerateAP())
    return;
  if (!CPDF_GenerateAP::GenerateAnnotAP(m_pDocument, m_pAnnotDict.Get(),
                                        m_nSubtype)) {
    return;
  }

  m_pAnnotDict->SetNewFor<CPDF_Boolean>(kPDFiumKey_HasGeneratedAP, true);
  m_bHasGeneratedAP = true;
}

bool CPDF_Annot::ShouldGenerateAP() const {
  // If AP dictionary exists and defines an appearance for normal mode, we use
  // the appearance defined in the existing AP dictionary.
  RetainPtr<const CPDF_Dictionary> pAP =
      m_pAnnotDict->GetDictFor(pdfium::annotation::kAP);
  if (pAP && pAP->GetDictFor("N"))
    return false;

  return !IsHidden();
}

bool CPDF_Annot::ShouldDrawAnnotation() const {
  if (IsHidden())
    return false;
  return m_bOpenState || m_nSubtype != CPDF_Annot::Subtype::POPUP;
}

void CPDF_Annot::ClearCachedAP() {
  m_APMap.clear();
}

CPDF_Annot::Subtype CPDF_Annot::GetSubtype() const {
  return m_nSubtype;
}

CFX_FloatRect CPDF_Annot::RectForDrawing() const {
  bool bShouldUseQuadPointsCoords =
      m_bIsTextMarkupAnnotation && m_bHasGeneratedAP;
  if (bShouldUseQuadPointsCoords)
    return BoundingRectFromQuadPoints(m_pAnnotDict.Get());
  return m_pAnnotDict->GetRectFor(pdfium::annotation::kRect);
}

CFX_FloatRect CPDF_Annot::GetRect() const {
  CFX_FloatRect rect = RectForDrawing();
  rect.Normalize();
  return rect;
}

uint32_t CPDF_Annot::GetFlags() const {
  return m_pAnnotDict->GetIntegerFor(pdfium::annotation::kF);
}

bool CPDF_Annot::IsHidden() const {
  return !!(GetFlags() & pdfium::annotation_flags::kHidden);
}

RetainPtr<CPDF_Stream> GetAnnotAP(CPDF_Dictionary* pAnnotDict,
                                  CPDF_Annot::AppearanceMode eMode) {
  DCHECK(pAnnotDict);
  return GetAnnotAPInternal(pAnnotDict, eMode, true);
}

RetainPtr<CPDF_Stream> GetAnnotAPNoFallback(CPDF_Dictionary* pAnnotDict,
                                            CPDF_Annot::AppearanceMode eMode) {
  DCHECK(pAnnotDict);
  return GetAnnotAPInternal(pAnnotDict, eMode, false);
}

CPDF_Form* CPDF_Annot::GetAPForm(CPDF_Page* pPage, AppearanceMode mode) {
  RetainPtr<CPDF_Stream> pStream = GetAnnotAP(m_pAnnotDict.Get(), mode);
  if (!pStream)
    return nullptr;

  auto it = m_APMap.find(pStream);
  if (it != m_APMap.end())
    return it->second.get();

  auto pNewForm = std::make_unique<CPDF_Form>(
      m_pDocument, pPage->GetMutableResources(), pStream);
  pNewForm->ParseContent();

  CPDF_Form* pResult = pNewForm.get();
  m_APMap[pStream] = std::move(pNewForm);
  return pResult;
}

void CPDF_Annot::SetPopupAnnotOpenState(bool bOpenState) {
  if (m_pPopupAnnot)
    m_pPopupAnnot->SetOpenState(bOpenState);
}

std::optional<CFX_FloatRect> CPDF_Annot::GetPopupAnnotRect() const {
  if (!m_pPopupAnnot)
    return std::nullopt;
  return m_pPopupAnnot->GetRect();
}

// static
CFX_FloatRect CPDF_Annot::RectFromQuadPointsArray(const CPDF_Array* pArray,
                                                  size_t nIndex) {
  DCHECK(pArray);
  DCHECK(nIndex < pArray->size() / 8);

  // QuadPoints are defined with 4 pairs of numbers
  // ([ pair0, pair1, pair2, pair3 ]), where
  // pair0 = top_left
  // pair1 = top_right
  // pair2 = bottom_left
  // pair3 = bottom_right
  //
  // On the other hand, /Rect is defined as 2 pairs [pair0, pair1] where:
  // pair0 = bottom_left
  // pair1 = top_right.

  return CFX_FloatRect(
      pArray->GetFloatAt(4 + nIndex * 8), pArray->GetFloatAt(5 + nIndex * 8),
      pArray->GetFloatAt(2 + nIndex * 8), pArray->GetFloatAt(3 + nIndex * 8));
}

// static
CFX_FloatRect CPDF_Annot::BoundingRectFromQuadPoints(
    const CPDF_Dictionary* pAnnotDict) {
  CFX_FloatRect ret;
  RetainPtr<const CPDF_Array> pArray = pAnnotDict->GetArrayFor("QuadPoints");
  size_t nQuadPointCount = pArray ? QuadPointCount(pArray.Get()) : 0;
  if (nQuadPointCount == 0)
    return ret;

  ret = RectFromQuadPointsArray(pArray.Get(), 0);
  for (size_t i = 1; i < nQuadPointCount; ++i) {
    CFX_FloatRect rect = RectFromQuadPointsArray(pArray.Get(), i);
    ret.Union(rect);
  }
  return ret;
}

// static
CFX_FloatRect CPDF_Annot::RectFromQuadPoints(const CPDF_Dictionary* pAnnotDict,
                                             size_t nIndex) {
  RetainPtr<const CPDF_Array> pArray = pAnnotDict->GetArrayFor("QuadPoints");
  size_t nQuadPointCount = pArray ? QuadPointCount(pArray.Get()) : 0;
  if (nIndex >= nQuadPointCount)
    return CFX_FloatRect();
  return RectFromQuadPointsArray(pArray.Get(), nIndex);
}

// static
CPDF_Annot::Subtype CPDF_Annot::StringToAnnotSubtype(
    const ByteString& sSubtype) {
  if (sSubtype == "Text")
    return CPDF_Annot::Subtype::TEXT;
  if (sSubtype == "Link")
    return CPDF_Annot::Subtype::LINK;
  if (sSubtype == "FreeText")
    return CPDF_Annot::Subtype::FREETEXT;
  if (sSubtype == "Line")
    return CPDF_Annot::Subtype::LINE;
  if (sSubtype == "Square")
    return CPDF_Annot::Subtype::SQUARE;
  if (sSubtype == "Circle")
    return CPDF_Annot::Subtype::CIRCLE;
  if (sSubtype == "Polygon")
    return CPDF_Annot::Subtype::POLYGON;
  if (sSubtype == "PolyLine")
    return CPDF_Annot::Subtype::POLYLINE;
  if (sSubtype == "Highlight")
    return CPDF_Annot::Subtype::HIGHLIGHT;
  if (sSubtype == "Underline")
    return CPDF_Annot::Subtype::UNDERLINE;
  if (sSubtype == "Squiggly")
    return CPDF_Annot::Subtype::SQUIGGLY;
  if (sSubtype == "StrikeOut")
    return CPDF_Annot::Subtype::STRIKEOUT;
  if (sSubtype == "Stamp")
    return CPDF_Annot::Subtype::STAMP;
  if (sSubtype == "Caret")
    return CPDF_Annot::Subtype::CARET;
  if (sSubtype == "Ink")
    return CPDF_Annot::Subtype::INK;
  if (sSubtype == "Popup")
    return CPDF_Annot::Subtype::POPUP;
  if (sSubtype == "FileAttachment")
    return CPDF_Annot::Subtype::FILEATTACHMENT;
  if (sSubtype == "Sound")
    return CPDF_Annot::Subtype::SOUND;
  if (sSubtype == "Movie")
    return CPDF_Annot::Subtype::MOVIE;
  if (sSubtype == "Widget")
    return CPDF_Annot::Subtype::WIDGET;
  if (sSubtype == "Screen")
    return CPDF_Annot::Subtype::SCREEN;
  if (sSubtype == "PrinterMark")
    return CPDF_Annot::Subtype::PRINTERMARK;
  if (sSubtype == "TrapNet")
    return CPDF_Annot::Subtype::TRAPNET;
  if (sSubtype == "Watermark")
    return CPDF_Annot::Subtype::WATERMARK;
  if (sSubtype == "3D")
    return CPDF_Annot::Subtype::THREED;
  if (sSubtype == "RichMedia")
    return CPDF_Annot::Subtype::RICHMEDIA;
  if (sSubtype == "XFAWidget")
    return CPDF_Annot::Subtype::XFAWIDGET;
  if (sSubtype == "Redact")
    return CPDF_Annot::Subtype::REDACT;
  return CPDF_Annot::Subtype::UNKNOWN;
}

// static
ByteString CPDF_Annot::AnnotSubtypeToString(CPDF_Annot::Subtype nSubtype) {
  if (nSubtype == CPDF_Annot::Subtype::TEXT)
    return "Text";
  if (nSubtype == CPDF_Annot::Subtype::LINK)
    return "Link";
  if (nSubtype == CPDF_Annot::Subtype::FREETEXT)
    return "FreeText";
  if (nSubtype == CPDF_Annot::Subtype::LINE)
    return "Line";
  if (nSubtype == CPDF_Annot::Subtype::SQUARE)
    return "Square";
  if (nSubtype == CPDF_Annot::Subtype::CIRCLE)
    return "Circle";
  if (nSubtype == CPDF_Annot::Subtype::POLYGON)
    return "Polygon";
  if (nSubtype == CPDF_Annot::Subtype::POLYLINE)
    return "PolyLine";
  if (nSubtype == CPDF_Annot::Subtype::HIGHLIGHT)
    return "Highlight";
  if (nSubtype == CPDF_Annot::Subtype::UNDERLINE)
    return "Underline";
  if (nSubtype == CPDF_Annot::Subtype::SQUIGGLY)
    return "Squiggly";
  if (nSubtype == CPDF_Annot::Subtype::STRIKEOUT)
    return "StrikeOut";
  if (nSubtype == CPDF_Annot::Subtype::STAMP)
    return "Stamp";
  if (nSubtype == CPDF_Annot::Subtype::CARET)
    return "Caret";
  if (nSubtype == CPDF_Annot::Subtype::INK)
    return "Ink";
  if (nSubtype == CPDF_Annot::Subtype::POPUP)
    return "Popup";
  if (nSubtype == CPDF_Annot::Subtype::FILEATTACHMENT)
    return "FileAttachment";
  if (nSubtype == CPDF_Annot::Subtype::SOUND)
    return "Sound";
  if (nSubtype == CPDF_Annot::Subtype::MOVIE)
    return "Movie";
  if (nSubtype == CPDF_Annot::Subtype::WIDGET)
    return "Widget";
  if (nSubtype == CPDF_Annot::Subtype::SCREEN)
    return "Screen";
  if (nSubtype == CPDF_Annot::Subtype::PRINTERMARK)
    return "PrinterMark";
  if (nSubtype == CPDF_Annot::Subtype::TRAPNET)
    return "TrapNet";
  if (nSubtype == CPDF_Annot::Subtype::WATERMARK)
    return "Watermark";
  if (nSubtype == CPDF_Annot::Subtype::THREED)
    return "3D";
  if (nSubtype == CPDF_Annot::Subtype::RICHMEDIA)
    return "RichMedia";
  if (nSubtype == CPDF_Annot::Subtype::XFAWIDGET)
    return "XFAWidget";
  if (nSubtype == CPDF_Annot::Subtype::REDACT)
    return "Redact";
  return ByteString();
}

// static
size_t CPDF_Annot::QuadPointCount(const CPDF_Array* pArray) {
  return pArray->size() / 8;
}

bool CPDF_Annot::DrawAppearance(CPDF_Page* pPage,
                                CFX_RenderDevice* pDevice,
                                const CFX_Matrix& mtUser2Device,
                                AppearanceMode mode) {
  if (!ShouldDrawAnnotation())
    return false;

  // It might happen that by the time this annotation instance was created,
  // it was flagged as "hidden" (e.g. /F 2), and hence CPDF_GenerateAP decided
  // to not "generate" its AP.
  // If for a reason the object is no longer hidden, but still does not have
  // its "AP" generated, generate it now.
  GenerateAPIfNeeded();

  CFX_Matrix matrix;
  CPDF_Form* pForm = AnnotGetMatrix(pPage, this, mode, mtUser2Device, &matrix);
  if (!pForm)
    return false;

  CPDF_RenderContext context(pPage->GetDocument(),
                             pPage->GetMutablePageResources(),
                             pPage->GetPageImageCache());
  context.AppendLayer(pForm, matrix);
  context.Render(pDevice, nullptr, nullptr, nullptr);
  return true;
}

bool CPDF_Annot::DrawInContext(CPDF_Page* pPage,
                               CPDF_RenderContext* pContext,
                               const CFX_Matrix& mtUser2Device,
                               AppearanceMode mode) {
  if (!ShouldDrawAnnotation())
    return false;

  // It might happen that by the time this annotation instance was created,
  // it was flagged as "hidden" (e.g. /F 2), and hence CPDF_GenerateAP decided
  // to not "generate" its AP.
  // If for a reason the object is no longer hidden, but still does not have
  // its "AP" generated, generate it now.
  GenerateAPIfNeeded();

  CFX_Matrix matrix;
  CPDF_Form* pForm = AnnotGetMatrix(pPage, this, mode, mtUser2Device, &matrix);
  if (!pForm)
    return false;

  pContext->AppendLayer(pForm, matrix);
  return true;
}

void CPDF_Annot::DrawBorder(CFX_RenderDevice* pDevice,
                            const CFX_Matrix* pUser2Device) {
  if (GetSubtype() == CPDF_Annot::Subtype::POPUP)
    return;

  uint32_t annot_flags = GetFlags();
  if (annot_flags & pdfium::annotation_flags::kHidden)
    return;

#if BUILDFLAG(IS_WIN)
  bool is_printing = pDevice->GetDeviceType() == DeviceType::kPrinter;
  if (is_printing && (annot_flags & pdfium::annotation_flags::kPrint) == 0) {
    return;
  }
#else
  const bool is_printing = false;
#endif

  if (!is_printing && (annot_flags & pdfium::annotation_flags::kNoView)) {
    return;
  }

  RetainPtr<const CPDF_Dictionary> pBS = m_pAnnotDict->GetDictFor("BS");
  char style_char;
  float width;
  RetainPtr<const CPDF_Array> pDashArray;
  if (!pBS) {
    RetainPtr<const CPDF_Array> pBorderArray =
        m_pAnnotDict->GetArrayFor(pdfium::annotation::kBorder);
    style_char = 'S';
    if (pBorderArray) {
      width = pBorderArray->GetFloatAt(2);
      if (pBorderArray->size() == 4) {
        pDashArray = pBorderArray->GetArrayAt(3);
        if (!pDashArray) {
          return;
        }
        size_t nLen = pDashArray->size();
        size_t i = 0;
        for (; i < nLen; ++i) {
          RetainPtr<const CPDF_Object> pObj = pDashArray->GetDirectObjectAt(i);
          if (pObj && pObj->GetInteger()) {
            break;
          }
        }
        if (i == nLen) {
          return;
        }
        style_char = 'D';
      }
    } else {
      width = 1;
    }
  } else {
    ByteString style = pBS->GetByteStringFor("S");
    pDashArray = pBS->GetArrayFor("D");
    style_char = style[0];
    width = pBS->GetFloatFor("W");
  }
  if (width <= 0) {
    return;
  }
  RetainPtr<const CPDF_Array> pColor =
      m_pAnnotDict->GetArrayFor(pdfium::annotation::kC);
  uint32_t argb = 0xff000000;
  if (pColor) {
    int R = static_cast<int32_t>(pColor->GetFloatAt(0) * 255);
    int G = static_cast<int32_t>(pColor->GetFloatAt(1) * 255);
    int B = static_cast<int32_t>(pColor->GetFloatAt(2) * 255);
    argb = ArgbEncode(0xff, R, G, B);
  }
  CFX_GraphStateData graph_state;
  graph_state.m_LineWidth = width;
  if (style_char == 'U') {
    // TODO(https://crbug.com/237527): Handle the "Underline" border style
    // instead of drawing the rectangle border.
    return;
  }

  if (style_char == 'D') {
    if (pDashArray) {
      graph_state.m_DashArray =
          ReadArrayElementsToVector(pDashArray.Get(), pDashArray->size());
      if (graph_state.m_DashArray.size() % 2)
        graph_state.m_DashArray.push_back(graph_state.m_DashArray.back());
    } else {
      graph_state.m_DashArray = {3.0f, 3.0f};
    }
  }

  CFX_FloatRect rect = GetRect();
  rect.Deflate(width / 2, width / 2);

  CFX_Path path;
  path.AppendFloatRect(rect);
  pDevice->DrawPath(path, pUser2Device, &graph_state, argb, argb,
                    CFX_FillRenderOptions());
}
