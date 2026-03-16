// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_generateap.h"

#include <algorithm>
#include <sstream>
#include <utility>

#include "constants/annotation_common.h"
#include "constants/appearance.h"
#include "constants/font_encodings.h"
#include "constants/form_fields.h"
#include "core/fpdfapi/edit/cpdf_contentstream_write_utils.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_boolean.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/parser/fpdf_parser_utility.h"
#include "core/fpdfdoc/cpdf_annot.h"
#include "core/fpdfdoc/cpdf_color_utils.h"
#include "core/fpdfdoc/cpdf_defaultappearance.h"
#include "core/fpdfdoc/cpdf_formfield.h"
#include "core/fpdfdoc/cpvt_fontmap.h"
#include "core/fpdfdoc/cpvt_variabletext.h"
#include "core/fpdfdoc/cpvt_word.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxge/cfx_renderdevice.h"

namespace {

struct CPVT_Dash {
  CPVT_Dash(int32_t dash, int32_t gap, int32_t phase)
      : nDash(dash), nGap(gap), nPhase(phase) {}

  int32_t nDash;
  int32_t nGap;
  int32_t nPhase;
};

enum class PaintOperation { kStroke, kFill };

ByteString GetPDFWordString(IPVT_FontMap* pFontMap,
                            int32_t nFontIndex,
                            uint16_t Word,
                            uint16_t SubWord) {
  if (SubWord > 0)
    return ByteString::Format("%c", SubWord);

  if (!pFontMap)
    return ByteString();

  RetainPtr<CPDF_Font> pPDFFont = pFontMap->GetPDFFont(nFontIndex);
  if (!pPDFFont)
    return ByteString();

  if (pPDFFont->GetBaseFontName() == "Symbol" ||
      pPDFFont->GetBaseFontName() == "ZapfDingbats") {
    return ByteString::Format("%c", Word);
  }

  ByteString sWord;
  uint32_t dwCharCode = pPDFFont->CharCodeFromUnicode(Word);
  if (dwCharCode != CPDF_Font::kInvalidCharCode) {
    pPDFFont->AppendChar(&sWord, dwCharCode);
  }
  return sWord;
}

ByteString GetWordRenderString(ByteStringView strWords) {
  if (strWords.IsEmpty())
    return ByteString();
  return PDF_EncodeString(strWords) + " Tj\n";
}

ByteString GetFontSetString(IPVT_FontMap* pFontMap,
                            int32_t nFontIndex,
                            float fFontSize) {
  fxcrt::ostringstream sRet;
  if (pFontMap) {
    ByteString sFontAlias = pFontMap->GetPDFFontAlias(nFontIndex);
    if (sFontAlias.GetLength() > 0 && fFontSize > 0) {
      sRet << "/" << sFontAlias << " ";
      WriteFloat(sRet, fFontSize) << " Tf\n";
    }
  }
  return ByteString(sRet);
}

ByteString GenerateEditAP(IPVT_FontMap* pFontMap,
                          CPVT_VariableText::Iterator* pIterator,
                          const CFX_PointF& ptOffset,
                          bool bContinuous,
                          uint16_t SubWord) {
  fxcrt::ostringstream sEditStream;
  fxcrt::ostringstream sLineStream;
  CFX_PointF ptOld;
  CFX_PointF ptNew;
  int32_t nCurFontIndex = -1;
  CPVT_WordPlace oldplace;
  ByteString sWords;
  pIterator->SetAt(0);
  while (pIterator->NextWord()) {
    CPVT_WordPlace place = pIterator->GetWordPlace();
    if (bContinuous) {
      if (place.LineCmp(oldplace) != 0) {
        if (!sWords.IsEmpty()) {
          sLineStream << GetWordRenderString(sWords.AsStringView());
          sEditStream << sLineStream.str();
          sLineStream.str("");
          sWords.clear();
        }
        CPVT_Word word;
        if (pIterator->GetWord(word)) {
          ptNew = CFX_PointF(word.ptWord.x + ptOffset.x,
                             word.ptWord.y + ptOffset.y);
        } else {
          CPVT_Line line;
          pIterator->GetLine(line);
          ptNew = CFX_PointF(line.ptLine.x + ptOffset.x,
                             line.ptLine.y + ptOffset.y);
        }
        if (ptNew != ptOld) {
          WritePoint(sLineStream, ptNew - ptOld) << " Td\n";
          ptOld = ptNew;
        }
      }
      CPVT_Word word;
      if (pIterator->GetWord(word)) {
        if (word.nFontIndex != nCurFontIndex) {
          if (!sWords.IsEmpty()) {
            sLineStream << GetWordRenderString(sWords.AsStringView());
            sWords.clear();
          }
          sLineStream << GetFontSetString(pFontMap, word.nFontIndex,
                                          word.fFontSize);
          nCurFontIndex = word.nFontIndex;
        }
        sWords += GetPDFWordString(pFontMap, nCurFontIndex, word.Word, SubWord);
      }
      oldplace = place;
    } else {
      CPVT_Word word;
      if (pIterator->GetWord(word)) {
        ptNew =
            CFX_PointF(word.ptWord.x + ptOffset.x, word.ptWord.y + ptOffset.y);
        if (ptNew != ptOld) {
          WritePoint(sEditStream, ptNew - ptOld) << " Td\n";
          ptOld = ptNew;
        }
        if (word.nFontIndex != nCurFontIndex) {
          sEditStream << GetFontSetString(pFontMap, word.nFontIndex,
                                          word.fFontSize);
          nCurFontIndex = word.nFontIndex;
        }
        sEditStream << GetWordRenderString(
            GetPDFWordString(pFontMap, nCurFontIndex, word.Word, SubWord)
                .AsStringView());
      }
    }
  }
  if (!sWords.IsEmpty()) {
    sLineStream << GetWordRenderString(sWords.AsStringView());
    sEditStream << sLineStream.str();
  }
  return ByteString(sEditStream);
}

ByteString GenerateColorAP(const CFX_Color& color, PaintOperation nOperation) {
  fxcrt::ostringstream sColorStream;
  switch (color.nColorType) {
    case CFX_Color::Type::kRGB:
      WriteFloat(sColorStream, color.fColor1) << " ";
      WriteFloat(sColorStream, color.fColor2) << " ";
      WriteFloat(sColorStream, color.fColor3) << " ";
      sColorStream << (nOperation == PaintOperation::kStroke ? "RG" : "rg")
                   << "\n";
      break;
    case CFX_Color::Type::kGray:
      WriteFloat(sColorStream, color.fColor1) << " ";
      sColorStream << (nOperation == PaintOperation::kStroke ? "G" : "g")
                   << "\n";
      break;
    case CFX_Color::Type::kCMYK:
      WriteFloat(sColorStream, color.fColor1) << " ";
      WriteFloat(sColorStream, color.fColor2) << " ";
      WriteFloat(sColorStream, color.fColor3) << " ";
      WriteFloat(sColorStream, color.fColor4) << " ";
      sColorStream << (nOperation == PaintOperation::kStroke ? "K" : "k")
                   << "\n";
      break;
    case CFX_Color::Type::kTransparent:
      break;
  }
  return ByteString(sColorStream);
}

ByteString GenerateBorderAP(const CFX_FloatRect& rect,
                            float width,
                            const CFX_Color& color,
                            const CFX_Color& crLeftTop,
                            const CFX_Color& crRightBottom,
                            BorderStyle nStyle,
                            const CPVT_Dash& dash) {
  fxcrt::ostringstream sAppStream;
  ByteString sColor;
  const float fLeft = rect.left;
  const float fRight = rect.right;
  const float fTop = rect.top;
  const float fBottom = rect.bottom;
  if (width > 0.0f) {
    const float half_width = width / 2.0f;
    switch (nStyle) {
      case BorderStyle::kSolid:
        sColor = GenerateColorAP(color, PaintOperation::kFill);
        if (sColor.GetLength() > 0) {
          sAppStream << sColor;
          WriteRect(sAppStream, rect) << " re\n";
          CFX_FloatRect inner_rect = rect;
          inner_rect.Deflate(width, width);
          WriteRect(sAppStream, inner_rect) << " re f*\n";
        }
        break;
      case BorderStyle::kDash:
        sColor = GenerateColorAP(color, PaintOperation::kStroke);
        if (sColor.GetLength() > 0) {
          sAppStream << sColor;
          WriteFloat(sAppStream, width)
              << " w [" << dash.nDash << " " << dash.nGap << "] " << dash.nPhase
              << " d\n";
          WritePoint(sAppStream, {fLeft + half_width, fBottom + half_width})
              << " m\n";
          WritePoint(sAppStream, {fLeft + half_width, fTop - half_width})
              << " l\n";
          WritePoint(sAppStream, {fRight - half_width, fTop - half_width})
              << " l\n";
          WritePoint(sAppStream, {fRight - half_width, fBottom + half_width})
              << " l\n";
          WritePoint(sAppStream, {fLeft + half_width, fBottom + half_width})
              << " l S\n";
        }
        break;
      case BorderStyle::kBeveled:
      case BorderStyle::kInset:
        sColor = GenerateColorAP(crLeftTop, PaintOperation::kFill);
        if (sColor.GetLength() > 0) {
          sAppStream << sColor;
          WritePoint(sAppStream, {fLeft + half_width, fBottom + half_width})
              << " m\n";
          WritePoint(sAppStream, {fLeft + half_width, fTop - half_width})
              << " l\n";
          WritePoint(sAppStream, {fRight - half_width, fTop - half_width})
              << " l\n";
          WritePoint(sAppStream, {fRight - width, fTop - width}) << " l\n";
          WritePoint(sAppStream, {fLeft + width, fTop - width}) << " l\n";
          WritePoint(sAppStream, {fLeft + width, fBottom + width}) << " l f\n";
        }
        sColor = GenerateColorAP(crRightBottom, PaintOperation::kFill);
        if (sColor.GetLength() > 0) {
          sAppStream << sColor;
          WritePoint(sAppStream, {fRight - half_width, fTop - half_width})
              << " m\n";
          WritePoint(sAppStream, {fRight - half_width, fBottom + half_width})
              << " l\n";
          WritePoint(sAppStream, {fLeft + half_width, fBottom + half_width})
              << " l\n";
          WritePoint(sAppStream, {fLeft + width, fBottom + width}) << " l\n";
          WritePoint(sAppStream, {fRight - width, fBottom + width}) << " l\n";
          WritePoint(sAppStream, {fRight - width, fTop - width}) << " l f\n";
        }
        sColor = GenerateColorAP(color, PaintOperation::kFill);
        if (sColor.GetLength() > 0) {
          sAppStream << sColor;
          WriteRect(sAppStream, rect) << " re\n";
          CFX_FloatRect inner_rect = rect;
          inner_rect.Deflate(half_width, half_width);
          WriteRect(sAppStream, inner_rect) << " re f*\n";
        }
        break;
      case BorderStyle::kUnderline:
        sColor = GenerateColorAP(color, PaintOperation::kStroke);
        if (sColor.GetLength() > 0) {
          sAppStream << sColor;
          WriteFloat(sAppStream, width) << " w\n";
          WritePoint(sAppStream, {fLeft, fBottom + half_width}) << " m\n";
          WritePoint(sAppStream, {fRight, fBottom + half_width}) << " l S\n";
        }
        break;
    }
  }
  return ByteString(sAppStream);
}

ByteString GetColorStringWithDefault(const CPDF_Array* pColor,
                                     const CFX_Color& crDefaultColor,
                                     PaintOperation nOperation) {
  if (pColor) {
    CFX_Color color = fpdfdoc::CFXColorFromArray(*pColor);
    return GenerateColorAP(color, nOperation);
  }

  return GenerateColorAP(crDefaultColor, nOperation);
}

float GetBorderWidth(const CPDF_Dictionary* pDict) {
  RetainPtr<const CPDF_Dictionary> pBorderStyleDict = pDict->GetDictFor("BS");
  if (pBorderStyleDict && pBorderStyleDict->KeyExist("W"))
    return pBorderStyleDict->GetFloatFor("W");

  auto pBorderArray = pDict->GetArrayFor(pdfium::annotation::kBorder);
  if (pBorderArray && pBorderArray->size() > 2)
    return pBorderArray->GetFloatAt(2);

  return 1;
}

RetainPtr<const CPDF_Array> GetDashArray(const CPDF_Dictionary* pDict) {
  RetainPtr<const CPDF_Dictionary> pBorderStyleDict = pDict->GetDictFor("BS");
  if (pBorderStyleDict && pBorderStyleDict->GetByteStringFor("S") == "D")
    return pBorderStyleDict->GetArrayFor("D");

  RetainPtr<const CPDF_Array> pBorderArray =
      pDict->GetArrayFor(pdfium::annotation::kBorder);
  if (pBorderArray && pBorderArray->size() == 4)
    return pBorderArray->GetArrayAt(3);

  return nullptr;
}

ByteString GetDashPatternString(const CPDF_Dictionary* pDict) {
  RetainPtr<const CPDF_Array> pDashArray = GetDashArray(pDict);
  if (!pDashArray || pDashArray->IsEmpty())
    return ByteString();

  // Support maximum of ten elements in the dash array.
  size_t pDashArrayCount = std::min<size_t>(pDashArray->size(), 10);
  fxcrt::ostringstream sDashStream;

  sDashStream << "[";
  for (size_t i = 0; i < pDashArrayCount; ++i)
    WriteFloat(sDashStream, pDashArray->GetFloatAt(i)) << " ";
  sDashStream << "] 0 d\n";

  return ByteString(sDashStream);
}

ByteString GetPopupContentsString(CPDF_Document* pDoc,
                                  const CPDF_Dictionary& pAnnotDict,
                                  RetainPtr<CPDF_Font> pDefFont,
                                  const ByteString& sFontName) {
  WideString swValue(pAnnotDict.GetUnicodeTextFor(pdfium::form_fields::kT));
  swValue += L'\n';
  swValue += pAnnotDict.GetUnicodeTextFor(pdfium::annotation::kContents);

  CPVT_FontMap map(pDoc, nullptr, std::move(pDefFont), sFontName);
  CPVT_VariableText::Provider prd(&map);
  CPVT_VariableText vt(&prd);
  vt.SetPlateRect(pAnnotDict.GetRectFor(pdfium::annotation::kRect));
  vt.SetFontSize(12);
  vt.SetAutoReturn(true);
  vt.SetMultiLine(true);
  vt.Initialize();
  vt.SetText(swValue);
  vt.RearrangeAll();

  CFX_PointF ptOffset(3.0f, -3.0f);
  ByteString sContent =
      GenerateEditAP(&map, vt.GetIterator(), ptOffset, false, 0);

  if (sContent.IsEmpty())
    return ByteString();

  ByteString sColorAP = GenerateColorAP(
      CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0), PaintOperation::kFill);

  return ByteString{"BT\n", sColorAP.AsStringView(), sContent.AsStringView(),
                    "ET\n", "Q\n"};
}

RetainPtr<CPDF_Dictionary> GenerateFallbackFontDict(CPDF_Document* doc) {
  auto font_dict = doc->NewIndirect<CPDF_Dictionary>();
  font_dict->SetNewFor<CPDF_Name>("Type", "Font");
  font_dict->SetNewFor<CPDF_Name>("Subtype", "Type1");
  font_dict->SetNewFor<CPDF_Name>("BaseFont", CFX_Font::kDefaultAnsiFontName);
  font_dict->SetNewFor<CPDF_Name>("Encoding",
                                  pdfium::font_encodings::kWinAnsiEncoding);
  return font_dict;
}

RetainPtr<CPDF_Dictionary> GenerateResourceFontDict(
    CPDF_Document* doc,
    const ByteString& font_name,
    uint32_t font_dict_obj_num) {
  auto resource_font_dict = doc->New<CPDF_Dictionary>();
  resource_font_dict->SetNewFor<CPDF_Reference>(font_name, doc,
                                                font_dict_obj_num);
  return resource_font_dict;
}

ByteString GetPaintOperatorString(bool bIsStrokeRect, bool bIsFillRect) {
  if (bIsStrokeRect)
    return bIsFillRect ? "b" : "s";
  return bIsFillRect ? "f" : "n";
}

ByteString GenerateTextSymbolAP(const CFX_FloatRect& rect) {
  fxcrt::ostringstream sAppStream;
  sAppStream << GenerateColorAP(CFX_Color(CFX_Color::Type::kRGB, 1, 1, 0),
                                PaintOperation::kFill);
  sAppStream << GenerateColorAP(CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0),
                                PaintOperation::kStroke);

  constexpr int kBorderWidth = 1;
  sAppStream << kBorderWidth << " w\n";

  constexpr float kHalfWidth = kBorderWidth / 2.0f;
  constexpr int kTipDelta = 4;

  CFX_FloatRect outerRect1 = rect;
  outerRect1.Deflate(kHalfWidth, kHalfWidth);
  outerRect1.bottom += kTipDelta;

  CFX_FloatRect outerRect2 = outerRect1;
  outerRect2.left += kTipDelta;
  outerRect2.right = outerRect2.left + kTipDelta;
  outerRect2.top = outerRect2.bottom - kTipDelta;
  float outerRect2Middle = (outerRect2.left + outerRect2.right) / 2;

  // Draw outer boxes.
  WritePoint(sAppStream, {outerRect1.left, outerRect1.bottom}) << " m\n";
  WritePoint(sAppStream, {outerRect1.left, outerRect1.top}) << " l\n";
  WritePoint(sAppStream, {outerRect1.right, outerRect1.top}) << " l\n";
  WritePoint(sAppStream, {outerRect1.right, outerRect1.bottom}) << " l\n";
  WritePoint(sAppStream, {outerRect2.right, outerRect2.bottom}) << " l\n";
  WritePoint(sAppStream, {outerRect2Middle, outerRect2.top}) << " l\n";
  WritePoint(sAppStream, {outerRect2.left, outerRect2.bottom}) << " l\n";
  WritePoint(sAppStream, {outerRect1.left, outerRect1.bottom}) << " l\n";

  // Draw inner lines.
  CFX_FloatRect lineRect = outerRect1;
  const float fXDelta = 2;
  const float fYDelta = (lineRect.top - lineRect.bottom) / 4;

  lineRect.left += fXDelta;
  lineRect.right -= fXDelta;
  for (int i = 0; i < 3; ++i) {
    lineRect.top -= fYDelta;
    WritePoint(sAppStream, {lineRect.left, lineRect.top}) << " m\n";
    WritePoint(sAppStream, {lineRect.right, lineRect.top}) << " l\n";
  }
  sAppStream << "B*\n";

  return ByteString(sAppStream);
}

RetainPtr<CPDF_Dictionary> GenerateExtGStateDict(
    const CPDF_Dictionary& pAnnotDict,
    const ByteString& sExtGSDictName,
    const ByteString& sBlendMode) {
  auto pGSDict =
      pdfium::MakeRetain<CPDF_Dictionary>(pAnnotDict.GetByteStringPool());
  pGSDict->SetNewFor<CPDF_Name>("Type", "ExtGState");

  float fOpacity = pAnnotDict.KeyExist("CA") ? pAnnotDict.GetFloatFor("CA") : 1;
  pGSDict->SetNewFor<CPDF_Number>("CA", fOpacity);
  pGSDict->SetNewFor<CPDF_Number>("ca", fOpacity);
  pGSDict->SetNewFor<CPDF_Boolean>("AIS", false);
  pGSDict->SetNewFor<CPDF_Name>("BM", sBlendMode);

  auto pExtGStateDict =
      pdfium::MakeRetain<CPDF_Dictionary>(pAnnotDict.GetByteStringPool());
  pExtGStateDict->SetFor(sExtGSDictName, pGSDict);
  return pExtGStateDict;
}

RetainPtr<CPDF_Dictionary> GenerateResourceDict(
    CPDF_Document* pDoc,
    RetainPtr<CPDF_Dictionary> pExtGStateDict,
    RetainPtr<CPDF_Dictionary> pResourceFontDict) {
  auto pResourceDict = pDoc->New<CPDF_Dictionary>();
  if (pExtGStateDict)
    pResourceDict->SetFor("ExtGState", pExtGStateDict);
  if (pResourceFontDict)
    pResourceDict->SetFor("Font", pResourceFontDict);
  return pResourceDict;
}

void GenerateAndSetAPDict(CPDF_Document* doc,
                          CPDF_Dictionary* annot_dict,
                          fxcrt::ostringstream* app_stream,
                          RetainPtr<CPDF_Dictionary> resource_dict,
                          bool is_text_markup_annotation) {
  auto stream_dict = pdfium::MakeRetain<CPDF_Dictionary>();
  stream_dict->SetNewFor<CPDF_Number>("FormType", 1);
  stream_dict->SetNewFor<CPDF_Name>("Type", "XObject");
  stream_dict->SetNewFor<CPDF_Name>("Subtype", "Form");
  stream_dict->SetMatrixFor("Matrix", CFX_Matrix());

  CFX_FloatRect rect = is_text_markup_annotation
                           ? CPDF_Annot::BoundingRectFromQuadPoints(annot_dict)
                           : annot_dict->GetRectFor(pdfium::annotation::kRect);
  stream_dict->SetRectFor("BBox", rect);
  stream_dict->SetFor("Resources", std::move(resource_dict));

  auto normal_stream = doc->NewIndirect<CPDF_Stream>(std::move(stream_dict));
  normal_stream->SetDataFromStringstream(app_stream);

  RetainPtr<CPDF_Dictionary> ap_dict =
      annot_dict->GetOrCreateDictFor(pdfium::annotation::kAP);
  ap_dict->SetNewFor<CPDF_Reference>("N", doc, normal_stream->GetObjNum());
}

bool GenerateCircleAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  fxcrt::ostringstream sAppStream;
  ByteString sExtGSDictName = "GS";
  sAppStream << "/" << sExtGSDictName << " gs ";

  RetainPtr<const CPDF_Array> pInteriorColor = pAnnotDict->GetArrayFor("IC");
  sAppStream << GetColorStringWithDefault(
      pInteriorColor.Get(), CFX_Color(CFX_Color::Type::kTransparent),
      PaintOperation::kFill);

  sAppStream << GetColorStringWithDefault(
      pAnnotDict->GetArrayFor(pdfium::annotation::kC).Get(),
      CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0), PaintOperation::kStroke);

  float fBorderWidth = GetBorderWidth(pAnnotDict);
  bool bIsStrokeRect = fBorderWidth > 0;

  if (bIsStrokeRect) {
    sAppStream << fBorderWidth << " w ";
    sAppStream << GetDashPatternString(pAnnotDict);
  }

  CFX_FloatRect rect = pAnnotDict->GetRectFor(pdfium::annotation::kRect);
  rect.Normalize();

  if (bIsStrokeRect) {
    // Deflating rect because stroking a path entails painting all points
    // whose perpendicular distance from the path in user space is less than
    // or equal to half the line width.
    rect.Deflate(fBorderWidth / 2, fBorderWidth / 2);
  }

  const float fMiddleX = (rect.left + rect.right) / 2;
  const float fMiddleY = (rect.top + rect.bottom) / 2;

  // |fL| is precalculated approximate value of 4 * tan((3.14 / 2) / 4) / 3,
  // where |fL| * radius is a good approximation of control points for
  // arc with 90 degrees.
  const float fL = 0.5523f;
  const float fDeltaX = fL * rect.Width() / 2.0;
  const float fDeltaY = fL * rect.Height() / 2.0;

  // Starting point
  sAppStream << fMiddleX << " " << rect.top << " m\n";
  // First Bezier Curve
  sAppStream << fMiddleX + fDeltaX << " " << rect.top << " " << rect.right
             << " " << fMiddleY + fDeltaY << " " << rect.right << " "
             << fMiddleY << " c\n";
  // Second Bezier Curve
  sAppStream << rect.right << " " << fMiddleY - fDeltaY << " "
             << fMiddleX + fDeltaX << " " << rect.bottom << " " << fMiddleX
             << " " << rect.bottom << " c\n";
  // Third Bezier Curve
  sAppStream << fMiddleX - fDeltaX << " " << rect.bottom << " " << rect.left
             << " " << fMiddleY - fDeltaY << " " << rect.left << " " << fMiddleY
             << " c\n";
  // Fourth Bezier Curve
  sAppStream << rect.left << " " << fMiddleY + fDeltaY << " "
             << fMiddleX - fDeltaX << " " << rect.top << " " << fMiddleX << " "
             << rect.top << " c\n";

  bool bIsFillRect = pInteriorColor && !pInteriorColor->IsEmpty();
  sAppStream << GetPaintOperatorString(bIsStrokeRect, bIsFillRect) << "\n";

  auto pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Normal");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       false /*IsTextMarkupAnnotation*/);
  return true;
}

bool GenerateHighlightAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  fxcrt::ostringstream sAppStream;
  ByteString sExtGSDictName = "GS";
  sAppStream << "/" << sExtGSDictName << " gs ";

  sAppStream << GetColorStringWithDefault(
      pAnnotDict->GetArrayFor(pdfium::annotation::kC).Get(),
      CFX_Color(CFX_Color::Type::kRGB, 1, 1, 0), PaintOperation::kFill);

  RetainPtr<const CPDF_Array> pArray = pAnnotDict->GetArrayFor("QuadPoints");
  if (pArray) {
    size_t nQuadPointCount = CPDF_Annot::QuadPointCount(pArray.Get());
    for (size_t i = 0; i < nQuadPointCount; ++i) {
      CFX_FloatRect rect = CPDF_Annot::RectFromQuadPoints(pAnnotDict, i);
      rect.Normalize();

      sAppStream << rect.left << " " << rect.top << " m " << rect.right << " "
                 << rect.top << " l " << rect.right << " " << rect.bottom
                 << " l " << rect.left << " " << rect.bottom << " l h f\n";
    }
  }

  auto pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Multiply");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       true /*IsTextMarkupAnnotation*/);

  return true;
}

bool GenerateInkAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  RetainPtr<const CPDF_Array> pInkList = pAnnotDict->GetArrayFor("InkList");
  if (!pInkList || pInkList->IsEmpty())
    return false;

  float fBorderWidth = GetBorderWidth(pAnnotDict);
  const bool bIsStroke = fBorderWidth > 0;
  if (!bIsStroke)
    return false;

  ByteString sExtGSDictName = "GS";
  fxcrt::ostringstream sAppStream;
  sAppStream << "/" << sExtGSDictName << " gs ";
  sAppStream << GetColorStringWithDefault(
      pAnnotDict->GetArrayFor(pdfium::annotation::kC).Get(),
      CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0), PaintOperation::kStroke);

  sAppStream << fBorderWidth << " w ";
  sAppStream << GetDashPatternString(pAnnotDict);

  // Set inflated rect as a new rect because paths near the border with large
  // width should not be clipped to the original rect.
  CFX_FloatRect rect = pAnnotDict->GetRectFor(pdfium::annotation::kRect);
  rect.Inflate(fBorderWidth / 2, fBorderWidth / 2);
  pAnnotDict->SetRectFor(pdfium::annotation::kRect, rect);

  for (size_t i = 0; i < pInkList->size(); i++) {
    RetainPtr<const CPDF_Array> pInkCoordList = pInkList->GetArrayAt(i);
    if (!pInkCoordList || pInkCoordList->size() < 2)
      continue;

    sAppStream << pInkCoordList->GetFloatAt(0) << " "
               << pInkCoordList->GetFloatAt(1) << " m ";

    for (size_t j = 0; j < pInkCoordList->size() - 1; j += 2) {
      sAppStream << pInkCoordList->GetFloatAt(j) << " "
                 << pInkCoordList->GetFloatAt(j + 1) << " l ";
    }

    sAppStream << "S\n";
  }

  auto pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Normal");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       false /*IsTextMarkupAnnotation*/);
  return true;
}

bool GenerateTextAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  fxcrt::ostringstream sAppStream;
  ByteString sExtGSDictName = "GS";
  sAppStream << "/" << sExtGSDictName << " gs ";

  CFX_FloatRect rect = pAnnotDict->GetRectFor(pdfium::annotation::kRect);
  const float fNoteLength = 20;
  CFX_FloatRect noteRect(rect.left, rect.bottom, rect.left + fNoteLength,
                         rect.bottom + fNoteLength);
  pAnnotDict->SetRectFor(pdfium::annotation::kRect, noteRect);

  sAppStream << GenerateTextSymbolAP(noteRect);

  auto pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Normal");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       false /*IsTextMarkupAnnotation*/);
  return true;
}

bool GenerateUnderlineAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  fxcrt::ostringstream sAppStream;
  ByteString sExtGSDictName = "GS";
  sAppStream << "/" << sExtGSDictName << " gs ";

  sAppStream << GetColorStringWithDefault(
      pAnnotDict->GetArrayFor(pdfium::annotation::kC).Get(),
      CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0), PaintOperation::kStroke);

  RetainPtr<const CPDF_Array> pArray = pAnnotDict->GetArrayFor("QuadPoints");
  if (pArray) {
    static constexpr int kLineWidth = 1;
    sAppStream << kLineWidth << " w ";
    size_t nQuadPointCount = CPDF_Annot::QuadPointCount(pArray.Get());
    for (size_t i = 0; i < nQuadPointCount; ++i) {
      CFX_FloatRect rect = CPDF_Annot::RectFromQuadPoints(pAnnotDict, i);
      rect.Normalize();
      sAppStream << rect.left << " " << rect.bottom + kLineWidth << " m "
                 << rect.right << " " << rect.bottom + kLineWidth << " l S\n";
    }
  }

  auto pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Normal");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       true /*IsTextMarkupAnnotation*/);
  return true;
}

bool GeneratePopupAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  fxcrt::ostringstream sAppStream;
  ByteString sExtGSDictName = "GS";
  sAppStream << "/" << sExtGSDictName << " gs\n";

  sAppStream << GenerateColorAP(CFX_Color(CFX_Color::Type::kRGB, 1, 1, 0),
                                PaintOperation::kFill);
  sAppStream << GenerateColorAP(CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0),
                                PaintOperation::kStroke);

  const float fBorderWidth = 1;
  sAppStream << fBorderWidth << " w\n";

  CFX_FloatRect rect = pAnnotDict->GetRectFor(pdfium::annotation::kRect);
  rect.Normalize();
  rect.Deflate(fBorderWidth / 2, fBorderWidth / 2);

  sAppStream << rect.left << " " << rect.bottom << " " << rect.Width() << " "
             << rect.Height() << " re b\n";

  RetainPtr<CPDF_Dictionary> font_dict = GenerateFallbackFontDict(pDoc);
  auto* pData = CPDF_DocPageData::FromDocument(pDoc);
  RetainPtr<CPDF_Font> pDefFont = pData->GetFont(font_dict);
  if (!pDefFont)
    return false;

  const ByteString font_name = "FONT";
  RetainPtr<CPDF_Dictionary> resource_font_dict =
      GenerateResourceFontDict(pDoc, font_name, font_dict->GetObjNum());
  RetainPtr<CPDF_Dictionary> pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Normal");
  RetainPtr<CPDF_Dictionary> pResourceDict = GenerateResourceDict(
      pDoc, std::move(pExtGStateDict), std::move(resource_font_dict));

  sAppStream << GetPopupContentsString(pDoc, *pAnnotDict, std::move(pDefFont),
                                       font_name);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       false /*IsTextMarkupAnnotation*/);
  return true;
}

bool GenerateSquareAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  const ByteString sExtGSDictName = "GS";
  fxcrt::ostringstream sAppStream;
  sAppStream << "/" << sExtGSDictName << " gs ";

  RetainPtr<const CPDF_Array> pInteriorColor = pAnnotDict->GetArrayFor("IC");
  sAppStream << GetColorStringWithDefault(
      pInteriorColor.Get(), CFX_Color(CFX_Color::Type::kTransparent),
      PaintOperation::kFill);

  sAppStream << GetColorStringWithDefault(
      pAnnotDict->GetArrayFor(pdfium::annotation::kC).Get(),
      CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0), PaintOperation::kStroke);

  float fBorderWidth = GetBorderWidth(pAnnotDict);
  const bool bIsStrokeRect = fBorderWidth > 0;
  if (bIsStrokeRect) {
    sAppStream << fBorderWidth << " w ";
    sAppStream << GetDashPatternString(pAnnotDict);
  }

  CFX_FloatRect rect = pAnnotDict->GetRectFor(pdfium::annotation::kRect);
  rect.Normalize();

  if (bIsStrokeRect) {
    // Deflating rect because stroking a path entails painting all points
    // whose perpendicular distance from the path in user space is less than
    // or equal to half the line width.
    rect.Deflate(fBorderWidth / 2, fBorderWidth / 2);
  }

  const bool bIsFillRect = pInteriorColor && (pInteriorColor->size() > 0);
  sAppStream << rect.left << " " << rect.bottom << " " << rect.Width() << " "
             << rect.Height() << " re "
             << GetPaintOperatorString(bIsStrokeRect, bIsFillRect) << "\n";

  auto pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Normal");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       false /*IsTextMarkupAnnotation*/);
  return true;
}

bool GenerateSquigglyAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  fxcrt::ostringstream sAppStream;
  ByteString sExtGSDictName = "GS";
  sAppStream << "/" << sExtGSDictName << " gs ";

  sAppStream << GetColorStringWithDefault(
      pAnnotDict->GetArrayFor(pdfium::annotation::kC).Get(),
      CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0), PaintOperation::kStroke);

  RetainPtr<const CPDF_Array> pArray = pAnnotDict->GetArrayFor("QuadPoints");
  if (pArray) {
    static constexpr int kLineWidth = 1;
    static constexpr int kDelta = 2;
    sAppStream << kLineWidth << " w ";
    size_t nQuadPointCount = CPDF_Annot::QuadPointCount(pArray.Get());
    for (size_t i = 0; i < nQuadPointCount; ++i) {
      CFX_FloatRect rect = CPDF_Annot::RectFromQuadPoints(pAnnotDict, i);
      rect.Normalize();

      const float fTop = rect.bottom + kDelta;
      const float fBottom = rect.bottom;
      sAppStream << rect.left << " " << fTop << " m ";

      float fX = rect.left + kDelta;
      bool isUpwards = false;
      while (fX < rect.right) {
        sAppStream << fX << " " << (isUpwards ? fTop : fBottom) << " l ";
        fX += kDelta;
        isUpwards = !isUpwards;
      }

      float fRemainder = rect.right - (fX - kDelta);
      if (isUpwards)
        sAppStream << rect.right << " " << fBottom + fRemainder << " l ";
      else
        sAppStream << rect.right << " " << fTop - fRemainder << " l ";

      sAppStream << "S\n";
    }
  }

  auto pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Normal");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       true /*IsTextMarkupAnnotation*/);
  return true;
}

bool GenerateStrikeOutAP(CPDF_Document* pDoc, CPDF_Dictionary* pAnnotDict) {
  fxcrt::ostringstream sAppStream;
  ByteString sExtGSDictName = "GS";
  sAppStream << "/" << sExtGSDictName << " gs ";

  sAppStream << GetColorStringWithDefault(
      pAnnotDict->GetArrayFor(pdfium::annotation::kC).Get(),
      CFX_Color(CFX_Color::Type::kRGB, 0, 0, 0), PaintOperation::kStroke);

  RetainPtr<const CPDF_Array> pArray = pAnnotDict->GetArrayFor("QuadPoints");
  if (pArray) {
    size_t nQuadPointCount = CPDF_Annot::QuadPointCount(pArray.Get());
    for (size_t i = 0; i < nQuadPointCount; ++i) {
      CFX_FloatRect rect = CPDF_Annot::RectFromQuadPoints(pAnnotDict, i);
      rect.Normalize();

      float fY = (rect.top + rect.bottom) / 2;
      constexpr int kLineWidth = 1;
      sAppStream << kLineWidth << " w " << rect.left << " " << fY << " m "
                 << rect.right << " " << fY << " l S\n";
    }
  }

  auto pExtGStateDict =
      GenerateExtGStateDict(*pAnnotDict, sExtGSDictName, "Normal");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sAppStream, std::move(pResourceDict),
                       true /*IsTextMarkupAnnotation*/);
  return true;
}

}  // namespace

// static
void CPDF_GenerateAP::GenerateFormAP(CPDF_Document* pDoc,
                                     CPDF_Dictionary* pAnnotDict,
                                     FormType type) {
  RetainPtr<CPDF_Dictionary> pRootDict = pDoc->GetMutableRoot();
  if (!pRootDict)
    return;

  RetainPtr<CPDF_Dictionary> pFormDict =
      pRootDict->GetMutableDictFor("AcroForm");
  if (!pFormDict)
    return;

  ByteString DA;
  RetainPtr<const CPDF_Object> pDAObj =
      CPDF_FormField::GetFieldAttrForDict(pAnnotDict, "DA");
  if (pDAObj)
    DA = pDAObj->GetString();
  if (DA.IsEmpty())
    DA = pFormDict->GetByteStringFor("DA");
  if (DA.IsEmpty())
    return;

  CPDF_DefaultAppearance appearance(DA);

  float fFontSize = 0;
  std::optional<ByteString> font = appearance.GetFont(&fFontSize);
  if (!font.has_value())
    return;

  ByteString font_name = font.value();

  CFX_Color crText = fpdfdoc::CFXColorFromString(DA);
  RetainPtr<CPDF_Dictionary> pDRDict = pFormDict->GetMutableDictFor("DR");
  if (!pDRDict)
    return;

  RetainPtr<CPDF_Dictionary> pDRFontDict = pDRDict->GetMutableDictFor("Font");
  if (!ValidateFontResourceDict(pDRFontDict.Get()))
    return;

  RetainPtr<CPDF_Dictionary> pFontDict =
      pDRFontDict->GetMutableDictFor(font_name);
  if (!pFontDict) {
    pFontDict = GenerateFallbackFontDict(pDoc);
    pDRFontDict->SetNewFor<CPDF_Reference>(font_name, pDoc,
                                           pFontDict->GetObjNum());
  }
  auto* pData = CPDF_DocPageData::FromDocument(pDoc);
  RetainPtr<CPDF_Font> pDefFont = pData->GetFont(pFontDict);
  if (!pDefFont)
    return;

  CFX_FloatRect rcAnnot = pAnnotDict->GetRectFor(pdfium::annotation::kRect);
  RetainPtr<const CPDF_Dictionary> pMKDict = pAnnotDict->GetDictFor("MK");
  int32_t nRotate =
      pMKDict ? pMKDict->GetIntegerFor(pdfium::appearance::kR) : 0;

  CFX_FloatRect rcBBox;
  CFX_Matrix matrix;
  switch (nRotate % 360) {
    case 0:
      rcBBox = CFX_FloatRect(0, 0, rcAnnot.right - rcAnnot.left,
                             rcAnnot.top - rcAnnot.bottom);
      break;
    case 90:
      matrix = CFX_Matrix(0, 1, -1, 0, rcAnnot.right - rcAnnot.left, 0);
      rcBBox = CFX_FloatRect(0, 0, rcAnnot.top - rcAnnot.bottom,
                             rcAnnot.right - rcAnnot.left);
      break;
    case 180:
      matrix = CFX_Matrix(-1, 0, 0, -1, rcAnnot.right - rcAnnot.left,
                          rcAnnot.top - rcAnnot.bottom);
      rcBBox = CFX_FloatRect(0, 0, rcAnnot.right - rcAnnot.left,
                             rcAnnot.top - rcAnnot.bottom);
      break;
    case 270:
      matrix = CFX_Matrix(0, -1, 1, 0, 0, rcAnnot.top - rcAnnot.bottom);
      rcBBox = CFX_FloatRect(0, 0, rcAnnot.top - rcAnnot.bottom,
                             rcAnnot.right - rcAnnot.left);
      break;
  }

  BorderStyle nBorderStyle = BorderStyle::kSolid;
  float fBorderWidth = 1;
  CPVT_Dash dsBorder(3, 0, 0);
  CFX_Color crLeftTop;
  CFX_Color crRightBottom;
  if (RetainPtr<const CPDF_Dictionary> pBSDict = pAnnotDict->GetDictFor("BS")) {
    if (pBSDict->KeyExist("W"))
      fBorderWidth = pBSDict->GetFloatFor("W");

    if (RetainPtr<const CPDF_Array> pArray = pBSDict->GetArrayFor("D")) {
      dsBorder = CPVT_Dash(pArray->GetIntegerAt(0), pArray->GetIntegerAt(1),
                           pArray->GetIntegerAt(2));
    }
    if (pBSDict->GetByteStringFor("S").GetLength()) {
      switch (pBSDict->GetByteStringFor("S")[0]) {
        case 'S':
          nBorderStyle = BorderStyle::kSolid;
          break;
        case 'D':
          nBorderStyle = BorderStyle::kDash;
          break;
        case 'B':
          nBorderStyle = BorderStyle::kBeveled;
          fBorderWidth *= 2;
          crLeftTop = CFX_Color(CFX_Color::Type::kGray, 1);
          crRightBottom = CFX_Color(CFX_Color::Type::kGray, 0.5);
          break;
        case 'I':
          nBorderStyle = BorderStyle::kInset;
          fBorderWidth *= 2;
          crLeftTop = CFX_Color(CFX_Color::Type::kGray, 0.5);
          crRightBottom = CFX_Color(CFX_Color::Type::kGray, 0.75);
          break;
        case 'U':
          nBorderStyle = BorderStyle::kUnderline;
          break;
      }
    }
  }
  CFX_Color crBorder;
  CFX_Color crBG;
  if (pMKDict) {
    RetainPtr<const CPDF_Array> pArray =
        pMKDict->GetArrayFor(pdfium::appearance::kBC);
    if (pArray)
      crBorder = fpdfdoc::CFXColorFromArray(*pArray);
    pArray = pMKDict->GetArrayFor(pdfium::appearance::kBG);
    if (pArray)
      crBG = fpdfdoc::CFXColorFromArray(*pArray);
  }
  fxcrt::ostringstream sAppStream;
  ByteString sBG = GenerateColorAP(crBG, PaintOperation::kFill);
  if (sBG.GetLength() > 0) {
    sAppStream << "q\n" << sBG;
    WriteRect(sAppStream, rcBBox) << " re f\nQ\n";
  }
  ByteString sBorderStream =
      GenerateBorderAP(rcBBox, fBorderWidth, crBorder, crLeftTop, crRightBottom,
                       nBorderStyle, dsBorder);
  if (sBorderStream.GetLength() > 0)
    sAppStream << "q\n" << sBorderStream << "Q\n";

  CFX_FloatRect rcBody =
      CFX_FloatRect(rcBBox.left + fBorderWidth, rcBBox.bottom + fBorderWidth,
                    rcBBox.right - fBorderWidth, rcBBox.top - fBorderWidth);
  rcBody.Normalize();

  RetainPtr<CPDF_Dictionary> pAPDict =
      pAnnotDict->GetOrCreateDictFor(pdfium::annotation::kAP);
  RetainPtr<CPDF_Stream> pNormalStream = pAPDict->GetMutableStreamFor("N");
  RetainPtr<CPDF_Dictionary> pStreamDict;
  if (pNormalStream) {
    pStreamDict = pNormalStream->GetMutableDict();
    RetainPtr<CPDF_Dictionary> pStreamResList =
        pStreamDict->GetMutableDictFor("Resources");
    if (pStreamResList) {
      RetainPtr<CPDF_Dictionary> pStreamResFontList =
          pStreamResList->GetMutableDictFor("Font");
      if (pStreamResFontList) {
        if (!ValidateFontResourceDict(pStreamResFontList.Get()))
          return;
      } else {
        pStreamResFontList = pStreamResList->SetNewFor<CPDF_Dictionary>("Font");
      }
      if (!pStreamResFontList->KeyExist(font_name)) {
        pStreamResFontList->SetNewFor<CPDF_Reference>(font_name, pDoc,
                                                      pFontDict->GetObjNum());
      }
    } else {
      pStreamDict->SetFor("Resources", pFormDict->GetDictFor("DR")->Clone());
    }
    pStreamDict->SetMatrixFor("Matrix", matrix);
    pStreamDict->SetRectFor("BBox", rcBBox);
  } else {
    pNormalStream =
        pDoc->NewIndirect<CPDF_Stream>(pdfium::MakeRetain<CPDF_Dictionary>());
    pAPDict->SetNewFor<CPDF_Reference>("N", pDoc, pNormalStream->GetObjNum());
  }
  CPVT_FontMap map(
      pDoc, pStreamDict ? pStreamDict->GetMutableDictFor("Resources") : nullptr,
      std::move(pDefFont), font_name);
  CPVT_VariableText::Provider prd(&map);

  switch (type) {
    case CPDF_GenerateAP::kTextField: {
      RetainPtr<const CPDF_Object> pV = CPDF_FormField::GetFieldAttrForDict(
          pAnnotDict, pdfium::form_fields::kV);
      WideString swValue = pV ? pV->GetUnicodeText() : WideString();
      RetainPtr<const CPDF_Object> pQ =
          CPDF_FormField::GetFieldAttrForDict(pAnnotDict, "Q");
      int32_t nAlign = pQ ? pQ->GetInteger() : 0;
      RetainPtr<const CPDF_Object> pFf = CPDF_FormField::GetFieldAttrForDict(
          pAnnotDict, pdfium::form_fields::kFf);
      uint32_t dwFlags = pFf ? pFf->GetInteger() : 0;
      RetainPtr<const CPDF_Object> pMaxLen =
          CPDF_FormField::GetFieldAttrForDict(pAnnotDict, "MaxLen");
      uint32_t dwMaxLen = pMaxLen ? pMaxLen->GetInteger() : 0;
      CPVT_VariableText vt(&prd);
      vt.SetPlateRect(rcBody);
      vt.SetAlignment(nAlign);
      if (FXSYS_IsFloatZero(fFontSize))
        vt.SetAutoFontSize(true);
      else
        vt.SetFontSize(fFontSize);

      bool bMultiLine = (dwFlags >> 12) & 1;
      if (bMultiLine) {
        vt.SetMultiLine(true);
        vt.SetAutoReturn(true);
      }
      uint16_t subWord = 0;
      if ((dwFlags >> 13) & 1) {
        subWord = '*';
        vt.SetPasswordChar(subWord);
      }
      bool bCharArray = (dwFlags >> 24) & 1;
      if (bCharArray)
        vt.SetCharArray(dwMaxLen);
      else
        vt.SetLimitChar(dwMaxLen);

      vt.Initialize();
      vt.SetText(swValue);
      vt.RearrangeAll();
      CFX_FloatRect rcContent = vt.GetContentRect();
      CFX_PointF ptOffset;
      if (!bMultiLine) {
        ptOffset =
            CFX_PointF(0.0f, (rcContent.Height() - rcBody.Height()) / 2.0f);
      }
      ByteString sBody = GenerateEditAP(&map, vt.GetIterator(), ptOffset,
                                        !bCharArray, subWord);
      if (sBody.GetLength() > 0) {
        sAppStream << "/Tx BMC\n"
                   << "q\n";
        if (rcContent.Width() > rcBody.Width() ||
            rcContent.Height() > rcBody.Height()) {
          WriteRect(sAppStream, rcBody) << " re\nW\nn\n";
        }
        sAppStream << "BT\n"
                   << GenerateColorAP(crText, PaintOperation::kFill) << sBody
                   << "ET\n"
                   << "Q\nEMC\n";
      }
      break;
    }
    case CPDF_GenerateAP::kComboBox: {
      RetainPtr<const CPDF_Object> pV = CPDF_FormField::GetFieldAttrForDict(
          pAnnotDict, pdfium::form_fields::kV);
      WideString swValue = pV ? pV->GetUnicodeText() : WideString();
      CPVT_VariableText vt(&prd);
      CFX_FloatRect rcButton = rcBody;
      rcButton.left = rcButton.right - 13;
      rcButton.Normalize();
      CFX_FloatRect rcEdit = rcBody;
      rcEdit.right = rcButton.left;
      rcEdit.Normalize();
      vt.SetPlateRect(rcEdit);
      if (FXSYS_IsFloatZero(fFontSize))
        vt.SetAutoFontSize(true);
      else
        vt.SetFontSize(fFontSize);

      vt.Initialize();
      vt.SetText(swValue);
      vt.RearrangeAll();
      CFX_FloatRect rcContent = vt.GetContentRect();
      CFX_PointF ptOffset =
          CFX_PointF(0.0f, (rcContent.Height() - rcEdit.Height()) / 2.0f);
      ByteString sEdit =
          GenerateEditAP(&map, vt.GetIterator(), ptOffset, true, 0);
      if (sEdit.GetLength() > 0) {
        sAppStream << "/Tx BMC\nq\n";
        WriteRect(sAppStream, rcEdit) << " re\nW\nn\n";
        sAppStream << "BT\n"
                   << GenerateColorAP(crText, PaintOperation::kFill) << sEdit
                   << "ET\n"
                   << "Q\nEMC\n";
      }
      ByteString sButton =
          GenerateColorAP(CFX_Color(CFX_Color::Type::kRGB, 220.0f / 255.0f,
                                    220.0f / 255.0f, 220.0f / 255.0f),
                          PaintOperation::kFill);
      if (sButton.GetLength() > 0 && !rcButton.IsEmpty()) {
        sAppStream << "q\n" << sButton;
        WriteRect(sAppStream, rcButton) << " re f\n";
        sAppStream << "Q\n";
        ByteString sButtonBorder =
            GenerateBorderAP(rcButton, 2, CFX_Color(CFX_Color::Type::kGray, 0),
                             CFX_Color(CFX_Color::Type::kGray, 1),
                             CFX_Color(CFX_Color::Type::kGray, 0.5),
                             BorderStyle::kBeveled, CPVT_Dash(3, 0, 0));
        if (sButtonBorder.GetLength() > 0)
          sAppStream << "q\n" << sButtonBorder << "Q\n";

        CFX_PointF ptCenter = CFX_PointF((rcButton.left + rcButton.right) / 2,
                                         (rcButton.top + rcButton.bottom) / 2);
        if (FXSYS_IsFloatBigger(rcButton.Width(), 6) &&
            FXSYS_IsFloatBigger(rcButton.Height(), 6)) {
          sAppStream << "q\n"
                     << " 0 g\n";
          WritePoint(sAppStream, {ptCenter.x - 3, ptCenter.y + 1.5f}) << " m\n";
          WritePoint(sAppStream, {ptCenter.x + 3, ptCenter.y + 1.5f}) << " l\n";
          WritePoint(sAppStream, {ptCenter.x, ptCenter.y - 1.5f}) << " l\n";
          WritePoint(sAppStream, {ptCenter.x - 3, ptCenter.y + 1.5f})
              << " l f\n";
          sAppStream << sButton << "Q\n";
        }
      }
      break;
    }
    case CPDF_GenerateAP::kListBox: {
      RetainPtr<const CPDF_Array> pOpts =
          ToArray(CPDF_FormField::GetFieldAttrForDict(pAnnotDict, "Opt"));
      RetainPtr<const CPDF_Array> pSels =
          ToArray(CPDF_FormField::GetFieldAttrForDict(pAnnotDict, "I"));
      RetainPtr<const CPDF_Object> pTi =
          CPDF_FormField::GetFieldAttrForDict(pAnnotDict, "TI");
      int32_t nTop = pTi ? pTi->GetInteger() : 0;
      fxcrt::ostringstream sBody;
      if (pOpts) {
        float fy = rcBody.top;
        for (size_t i = nTop, sz = pOpts->size(); i < sz; i++) {
          if (FXSYS_IsFloatSmaller(fy, rcBody.bottom))
            break;

          if (RetainPtr<const CPDF_Object> pOpt = pOpts->GetDirectObjectAt(i)) {
            WideString swItem;
            if (pOpt->IsString()) {
              swItem = pOpt->GetUnicodeText();
            } else if (const CPDF_Array* pArray = pOpt->AsArray()) {
              RetainPtr<const CPDF_Object> pDirectObj =
                  pArray->GetDirectObjectAt(1);
              if (pDirectObj)
                swItem = pDirectObj->GetUnicodeText();
            }
            bool bSelected = false;
            if (pSels) {
              for (size_t s = 0, ssz = pSels->size(); s < ssz; s++) {
                int value = pSels->GetIntegerAt(s);
                if (value >= 0 && i == static_cast<size_t>(value)) {
                  bSelected = true;
                  break;
                }
              }
            }
            CPVT_VariableText vt(&prd);
            vt.SetPlateRect(
                CFX_FloatRect(rcBody.left, 0.0f, rcBody.right, 0.0f));
            vt.SetFontSize(FXSYS_IsFloatZero(fFontSize) ? 12.0f : fFontSize);
            vt.Initialize();
            vt.SetText(swItem);
            vt.RearrangeAll();

            float fItemHeight = vt.GetContentRect().Height();
            if (bSelected) {
              CFX_FloatRect rcItem = CFX_FloatRect(
                  rcBody.left, fy - fItemHeight, rcBody.right, fy);
              sBody << "q\n"
                    << GenerateColorAP(
                           CFX_Color(CFX_Color::Type::kRGB, 0, 51.0f / 255.0f,
                                     113.0f / 255.0f),
                           PaintOperation::kFill);
              WriteRect(sBody, rcItem) << " re f\nQ\n";
              sBody << "BT\n"
                    << GenerateColorAP(CFX_Color(CFX_Color::Type::kGray, 1),
                                       PaintOperation::kFill)
                    << GenerateEditAP(&map, vt.GetIterator(),
                                      CFX_PointF(0.0f, fy), true, 0)
                    << "ET\n";
            } else {
              sBody << "BT\n"
                    << GenerateColorAP(crText, PaintOperation::kFill)
                    << GenerateEditAP(&map, vt.GetIterator(),
                                      CFX_PointF(0.0f, fy), true, 0)
                    << "ET\n";
            }
            fy -= fItemHeight;
          }
        }
      }
      if (sBody.tellp() > 0) {
        sAppStream << "/Tx BMC\nq\n";
        WriteRect(sAppStream, rcBody) << " re\nW\nn\n"
                                      << sBody.str() << "Q\nEMC\n";
      }
      break;
    }
  }

  if (!pNormalStream)
    return;

  pNormalStream->SetDataFromStringstreamAndRemoveFilter(&sAppStream);
  pStreamDict = pNormalStream->GetMutableDict();
  pStreamDict->SetMatrixFor("Matrix", matrix);
  pStreamDict->SetRectFor("BBox", rcBBox);
  RetainPtr<CPDF_Dictionary> pStreamResList =
      pStreamDict->GetMutableDictFor("Resources");
  if (!pStreamResList) {
    pStreamDict->SetFor("Resources", pFormDict->GetDictFor("DR")->Clone());
    return;
  }

  RetainPtr<CPDF_Dictionary> pStreamResFontList =
      pStreamResList->GetMutableDictFor("Font");
  if (pStreamResFontList) {
    if (!ValidateFontResourceDict(pStreamResFontList.Get()))
      return;
  } else {
    pStreamResFontList = pStreamResList->SetNewFor<CPDF_Dictionary>("Font");
  }

  if (!pStreamResFontList->KeyExist(font_name)) {
    pStreamResFontList->SetNewFor<CPDF_Reference>(font_name, pDoc,
                                                  pFontDict->GetObjNum());
  }
}

// static
void CPDF_GenerateAP::GenerateEmptyAP(CPDF_Document* pDoc,
                                      CPDF_Dictionary* pAnnotDict) {
  auto pExtGStateDict = GenerateExtGStateDict(*pAnnotDict, "GS", "Normal");
  auto pResourceDict =
      GenerateResourceDict(pDoc, std::move(pExtGStateDict), nullptr);

  fxcrt::ostringstream sStream;
  GenerateAndSetAPDict(pDoc, pAnnotDict, &sStream, std::move(pResourceDict),
                       false);
}

// static
bool CPDF_GenerateAP::GenerateAnnotAP(CPDF_Document* pDoc,
                                      CPDF_Dictionary* pAnnotDict,
                                      CPDF_Annot::Subtype subtype) {
  switch (subtype) {
    case CPDF_Annot::Subtype::CIRCLE:
      return GenerateCircleAP(pDoc, pAnnotDict);
    case CPDF_Annot::Subtype::HIGHLIGHT:
      return GenerateHighlightAP(pDoc, pAnnotDict);
    case CPDF_Annot::Subtype::INK:
      return GenerateInkAP(pDoc, pAnnotDict);
    case CPDF_Annot::Subtype::POPUP:
      return GeneratePopupAP(pDoc, pAnnotDict);
    case CPDF_Annot::Subtype::SQUARE:
      return GenerateSquareAP(pDoc, pAnnotDict);
    case CPDF_Annot::Subtype::SQUIGGLY:
      return GenerateSquigglyAP(pDoc, pAnnotDict);
    case CPDF_Annot::Subtype::STRIKEOUT:
      return GenerateStrikeOutAP(pDoc, pAnnotDict);
    case CPDF_Annot::Subtype::TEXT:
      return GenerateTextAP(pDoc, pAnnotDict);
    case CPDF_Annot::Subtype::UNDERLINE:
      return GenerateUnderlineAP(pDoc, pAnnotDict);
    default:
      return false;
  }
}
