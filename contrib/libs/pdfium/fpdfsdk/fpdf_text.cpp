// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "public/fpdf_text.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <vector>

#include "build/build_config.h"
#include "core/fpdfapi/font/cpdf_font.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_textobject.h"
#include "core/fpdfdoc/cpdf_viewerpreferences.h"
#include "core/fpdftext/cpdf_linkextract.h"
#include "core/fpdftext/cpdf_textpage.h"
#include "core/fpdftext/cpdf_textpagefind.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "fpdfsdk/cpdfsdk_helpers.h"

namespace {

CPDF_TextPage* GetTextPageForValidIndex(FPDF_TEXTPAGE text_page, int index) {
  if (!text_page || index < 0)
    return nullptr;

  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(text_page);
  return static_cast<size_t>(index) < textpage->size() ? textpage : nullptr;
}

}  // namespace

FPDF_EXPORT FPDF_TEXTPAGE FPDF_CALLCONV FPDFText_LoadPage(FPDF_PAGE page) {
  CPDF_Page* pPDFPage = CPDFPageFromFPDFPage(page);
  if (!pPDFPage)
    return nullptr;

  CPDF_ViewerPreferences viewRef(pPDFPage->GetDocument());
  auto textpage =
      std::make_unique<CPDF_TextPage>(pPDFPage, viewRef.IsDirectionR2L());

  // Caller takes ownership.
  return FPDFTextPageFromCPDFTextPage(textpage.release());
}

FPDF_EXPORT void FPDF_CALLCONV FPDFText_ClosePage(FPDF_TEXTPAGE text_page) {
  // PDFium takes ownership.
  std::unique_ptr<CPDF_TextPage> textpage_deleter(
      CPDFTextPageFromFPDFTextPage(text_page));
}

FPDF_EXPORT int FPDF_CALLCONV FPDFText_CountChars(FPDF_TEXTPAGE text_page) {
  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(text_page);
  return textpage ? textpage->CountChars() : -1;
}

FPDF_EXPORT unsigned int FPDF_CALLCONV
FPDFText_GetUnicode(FPDF_TEXTPAGE text_page, int index) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return 0;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  return charinfo.m_Unicode;
}

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV
FPDFText_GetTextObject(FPDF_TEXTPAGE text_page, int index) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage) {
    return nullptr;
  }

  return FPDFPageObjectFromCPDFPageObject(
      textpage->GetCharInfo(index).m_pTextObj);
}

FPDF_EXPORT int FPDF_CALLCONV FPDFText_IsGenerated(FPDF_TEXTPAGE text_page,
                                                   int index) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return -1;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  return charinfo.m_CharType == CPDF_TextPage::CharType::kGenerated ? 1 : 0;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFText_IsHyphen(FPDF_TEXTPAGE text_page,
                                                int index) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage) {
    return -1;
  }

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  return charinfo.m_CharType == CPDF_TextPage::CharType::kHyphen;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFText_HasUnicodeMapError(FPDF_TEXTPAGE text_page, int index) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return -1;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  return charinfo.m_CharType == CPDF_TextPage::CharType::kNotUnicode;
}

FPDF_EXPORT double FPDF_CALLCONV FPDFText_GetFontSize(FPDF_TEXTPAGE text_page,
                                                      int index) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return 0;

  return textpage->GetCharFontSize(index);
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFText_GetFontInfo(FPDF_TEXTPAGE text_page,
                     int index,
                     void* buffer,
                     unsigned long buflen,
                     int* flags) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return 0;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  if (!charinfo.m_pTextObj)
    return 0;

  RetainPtr<CPDF_Font> font = charinfo.m_pTextObj->GetFont();
  if (flags)
    *flags = font->GetFontFlags();

  // SAFETY: required from caller.
  auto result_span = UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen));
  ByteString basefont = font->GetBaseFontName();
  auto basefont_span = basefont.span_with_terminator();
  fxcrt::try_spancpy(result_span, basefont_span);
  return pdfium::checked_cast<unsigned long>(basefont_span.size());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFText_GetFontWeight(FPDF_TEXTPAGE text_page,
                                                     int index) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return -1;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  if (!charinfo.m_pTextObj)
    return -1;

  return charinfo.m_pTextObj->GetFont()->GetFontWeight();
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFText_GetFillColor(FPDF_TEXTPAGE text_page,
                      int index,
                      unsigned int* R,
                      unsigned int* G,
                      unsigned int* B,
                      unsigned int* A) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage || !R || !G || !B || !A)
    return false;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  if (!charinfo.m_pTextObj)
    return false;

  FX_COLORREF fill_color = charinfo.m_pTextObj->color_state().GetFillColorRef();
  *R = FXSYS_GetRValue(fill_color);
  *G = FXSYS_GetGValue(fill_color);
  *B = FXSYS_GetBValue(fill_color);
  *A = FXSYS_GetUnsignedAlpha(
      charinfo.m_pTextObj->general_state().GetFillAlpha());
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFText_GetStrokeColor(FPDF_TEXTPAGE text_page,
                        int index,
                        unsigned int* R,
                        unsigned int* G,
                        unsigned int* B,
                        unsigned int* A) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage || !R || !G || !B || !A)
    return false;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  if (!charinfo.m_pTextObj)
    return false;

  FX_COLORREF stroke_color =
      charinfo.m_pTextObj->color_state().GetStrokeColorRef();
  *R = FXSYS_GetRValue(stroke_color);
  *G = FXSYS_GetGValue(stroke_color);
  *B = FXSYS_GetBValue(stroke_color);
  *A = FXSYS_GetUnsignedAlpha(
      charinfo.m_pTextObj->general_state().GetStrokeAlpha());
  return true;
}

FPDF_EXPORT float FPDF_CALLCONV FPDFText_GetCharAngle(FPDF_TEXTPAGE text_page,
                                                      int index) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return -1.0f;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  // On the left is our current Matrix and on the right a generic rotation
  // matrix for our coordinate space.
  // | a  b  0 |    | cos(t)  -sin(t)  0 |
  // | c  d  0 |    | sin(t)   cos(t)  0 |
  // | e  f  1 |    |   0        0     1 |
  // Calculate the angle of the vector
  float angle = atan2f(charinfo.m_Matrix.c, charinfo.m_Matrix.a);
  if (angle < 0)
    angle = 2 * FXSYS_PI + angle;

  return angle;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFText_GetCharBox(FPDF_TEXTPAGE text_page,
                                                        int index,
                                                        double* left,
                                                        double* right,
                                                        double* bottom,
                                                        double* top) {
  if (!left || !right || !bottom || !top)
    return false;

  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return false;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  *left = charinfo.m_CharBox.left;
  *right = charinfo.m_CharBox.right;
  *bottom = charinfo.m_CharBox.bottom;
  *top = charinfo.m_CharBox.top;
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFText_GetLooseCharBox(FPDF_TEXTPAGE text_page, int index, FS_RECTF* rect) {
  if (!rect)
    return false;

  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return false;

  *rect = FSRectFFromCFXFloatRect(textpage->GetCharLooseBounds(index));
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFText_GetMatrix(FPDF_TEXTPAGE text_page,
                                                       int index,
                                                       FS_MATRIX* matrix) {
  if (!matrix)
    return false;

  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return false;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  *matrix = FSMatrixFromCFXMatrix(charinfo.m_Matrix);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFText_GetCharOrigin(FPDF_TEXTPAGE text_page,
                       int index,
                       double* x,
                       double* y) {
  CPDF_TextPage* textpage = GetTextPageForValidIndex(text_page, index);
  if (!textpage)
    return false;

  const CPDF_TextPage::CharInfo& charinfo = textpage->GetCharInfo(index);
  *x = charinfo.m_Origin.x;
  *y = charinfo.m_Origin.y;
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFText_GetCharIndexAtPos(FPDF_TEXTPAGE text_page,
                           double x,
                           double y,
                           double xTolerance,
                           double yTolerance) {
  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(text_page);
  if (!textpage)
    return -3;

  return textpage->GetIndexAtPos(
      CFX_PointF(static_cast<float>(x), static_cast<float>(y)),
      CFX_SizeF(static_cast<float>(xTolerance),
                static_cast<float>(yTolerance)));
}

FPDF_EXPORT int FPDF_CALLCONV FPDFText_GetText(FPDF_TEXTPAGE page,
                                               int start_index,
                                               int char_count,
                                               unsigned short* result) {
  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(page);
  if (!textpage || start_index < 0 || char_count < 0 || !result) {
    return 0;
  }
  int char_available = textpage->CountChars() - start_index;
  if (char_available <= 0) {
    return 0;
  }
  char_count = std::min(char_count, char_available);
  if (char_count == 0) {
    // Writing out "", which has a character count of 1 due to the NUL.
    *result = '\0';
    return 1;
  }
  // SAFETY: Required from caller. Public API description states that
  // `result` must be able to hold `char_count` characters plus a
  // terminator.
  CHECK_LT(char_count, std::numeric_limits<int>::max());
  pdfium::span<unsigned short> result_span =
      UNSAFE_BUFFERS(pdfium::make_span(result, char_count + 1));

  // Includes two-byte terminator in string data itself.
  ByteString str = textpage->GetPageText(start_index, char_count).ToUCS2LE();
  auto str_span = fxcrt::reinterpret_span<const unsigned short>(str.span());

  // Hard CHECK() in Copy() if retrieved text is too long.
  fxcrt::Copy(str_span, result_span);
  return pdfium::checked_cast<int>(str_span.size());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFText_CountRects(FPDF_TEXTPAGE text_page,
                                                  int start,
                                                  int count) {
  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(text_page);
  return textpage ? textpage->CountRects(start, count) : 0;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFText_GetRect(FPDF_TEXTPAGE text_page,
                                                     int rect_index,
                                                     double* left,
                                                     double* top,
                                                     double* right,
                                                     double* bottom) {
  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(text_page);
  if (!textpage)
    return false;

  CFX_FloatRect rect;
  bool result = textpage->GetRect(rect_index, &rect);

  *left = rect.left;
  *top = rect.top;
  *right = rect.right;
  *bottom = rect.bottom;
  return result;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFText_GetBoundedText(FPDF_TEXTPAGE text_page,
                                                      double left,
                                                      double top,
                                                      double right,
                                                      double bottom,
                                                      unsigned short* buffer,
                                                      int buflen) {
  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(text_page);
  if (!textpage) {
    return 0;
  }
  CFX_FloatRect rect((float)left, (float)bottom, (float)right, (float)top);
  WideString wstr = textpage->GetTextByRect(rect);
  if (buflen <= 0 || !buffer) {
    return pdfium::checked_cast<int>(wstr.GetLength());
  }

  // SAFETY: Required from caller. Public API states that buflen
  // describes the number of values buffer can hold.
  const auto buffer_span = UNSAFE_BUFFERS(pdfium::make_span(buffer, buflen));

  ByteString str = wstr.ToUTF16LE();
  pdfium::span<const char> str_span = str.span();
  auto copy_span = fxcrt::reinterpret_span<const unsigned short>(str_span);
  if (copy_span.size() > buffer_span.size()) {
    copy_span = copy_span.first(buffer_span.size());
  }
  fxcrt::Copy(copy_span, buffer_span);
  return pdfium::checked_cast<int>(copy_span.size());
}

FPDF_EXPORT FPDF_SCHHANDLE FPDF_CALLCONV
FPDFText_FindStart(FPDF_TEXTPAGE text_page,
                   FPDF_WIDESTRING findwhat,
                   unsigned long flags,
                   int start_index) {
  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(text_page);
  if (!textpage)
    return nullptr;

  CPDF_TextPageFind::Options options;
  options.bMatchCase = !!(flags & FPDF_MATCHCASE);
  options.bMatchWholeWord = !!(flags & FPDF_MATCHWHOLEWORD);
  options.bConsecutive = !!(flags & FPDF_CONSECUTIVE);

  // SAFETY: required from caller.
  auto find = CPDF_TextPageFind::Create(
      textpage, UNSAFE_BUFFERS(WideStringFromFPDFWideString(findwhat)), options,
      start_index >= 0 ? std::optional<size_t>(start_index) : std::nullopt);

  // Caller takes ownership.
  return FPDFSchHandleFromCPDFTextPageFind(find.release());
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFText_FindNext(FPDF_SCHHANDLE handle) {
  if (!handle)
    return false;

  CPDF_TextPageFind* textpageFind = CPDFTextPageFindFromFPDFSchHandle(handle);
  return textpageFind->FindNext();
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFText_FindPrev(FPDF_SCHHANDLE handle) {
  if (!handle)
    return false;

  CPDF_TextPageFind* textpageFind = CPDFTextPageFindFromFPDFSchHandle(handle);
  return textpageFind->FindPrev();
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFText_GetSchResultIndex(FPDF_SCHHANDLE handle) {
  if (!handle)
    return 0;

  CPDF_TextPageFind* textpageFind = CPDFTextPageFindFromFPDFSchHandle(handle);
  return textpageFind->GetCurOrder();
}

FPDF_EXPORT int FPDF_CALLCONV FPDFText_GetSchCount(FPDF_SCHHANDLE handle) {
  if (!handle)
    return 0;

  CPDF_TextPageFind* textpageFind = CPDFTextPageFindFromFPDFSchHandle(handle);
  return textpageFind->GetMatchedCount();
}

FPDF_EXPORT void FPDF_CALLCONV FPDFText_FindClose(FPDF_SCHHANDLE handle) {
  if (!handle)
    return;

  // Take ownership back from caller and destroy.
  std::unique_ptr<CPDF_TextPageFind> textpageFind(
      CPDFTextPageFindFromFPDFSchHandle(handle));
}

// web link
FPDF_EXPORT FPDF_PAGELINK FPDF_CALLCONV
FPDFLink_LoadWebLinks(FPDF_TEXTPAGE text_page) {
  CPDF_TextPage* textpage = CPDFTextPageFromFPDFTextPage(text_page);
  if (!textpage)
    return nullptr;

  auto pagelink = std::make_unique<CPDF_LinkExtract>(textpage);
  pagelink->ExtractLinks();

  // Caller takes ownership.
  return FPDFPageLinkFromCPDFLinkExtract(pagelink.release());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFLink_CountWebLinks(FPDF_PAGELINK link_page) {
  if (!link_page)
    return 0;

  CPDF_LinkExtract* pageLink = CPDFLinkExtractFromFPDFPageLink(link_page);
  return pdfium::checked_cast<int>(pageLink->CountLinks());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFLink_GetURL(FPDF_PAGELINK link_page,
                                              int link_index,
                                              unsigned short* buffer,
                                              int buflen) {
  WideString wsUrl(L"");
  if (link_page && link_index >= 0) {
    CPDF_LinkExtract* pageLink = CPDFLinkExtractFromFPDFPageLink(link_page);
    wsUrl = pageLink->GetURL(link_index);
  }
  ByteString cbUTF16URL = wsUrl.ToUTF16LE();
  auto url_span =
      fxcrt::reinterpret_span<const unsigned short>(cbUTF16URL.span());
  if (!buffer || buflen <= 0) {
    return pdfium::checked_cast<int>(url_span.size());
  }

  // SAFETY: required from caller.
  pdfium::span<unsigned short> result_span =
      UNSAFE_BUFFERS(pdfium::make_span(buffer, buflen));

  size_t size = std::min(url_span.size(), result_span.size());
  fxcrt::Copy(url_span.first(size), result_span);
  return pdfium::checked_cast<int>(size);
}

FPDF_EXPORT int FPDF_CALLCONV FPDFLink_CountRects(FPDF_PAGELINK link_page,
                                                  int link_index) {
  if (!link_page || link_index < 0)
    return 0;

  CPDF_LinkExtract* pageLink = CPDFLinkExtractFromFPDFPageLink(link_page);
  return fxcrt::CollectionSize<int>(pageLink->GetRects(link_index));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFLink_GetRect(FPDF_PAGELINK link_page,
                                                     int link_index,
                                                     int rect_index,
                                                     double* left,
                                                     double* top,
                                                     double* right,
                                                     double* bottom) {
  if (!link_page || link_index < 0 || rect_index < 0)
    return false;

  CPDF_LinkExtract* pageLink = CPDFLinkExtractFromFPDFPageLink(link_page);
  std::vector<CFX_FloatRect> rectArray = pageLink->GetRects(link_index);
  if (rect_index >= fxcrt::CollectionSize<int>(rectArray))
    return false;

  *left = rectArray[rect_index].left;
  *right = rectArray[rect_index].right;
  *top = rectArray[rect_index].top;
  *bottom = rectArray[rect_index].bottom;
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFLink_GetTextRange(FPDF_PAGELINK link_page,
                      int link_index,
                      int* start_char_index,
                      int* char_count) {
  if (!link_page || link_index < 0)
    return false;

  CPDF_LinkExtract* page_link = CPDFLinkExtractFromFPDFPageLink(link_page);
  auto maybe_range = page_link->GetTextRange(link_index);
  if (!maybe_range.has_value())
    return false;

  *start_char_index = pdfium::checked_cast<int>(maybe_range.value().m_Start);
  *char_count = pdfium::checked_cast<int>(maybe_range.value().m_Count);
  return true;
}

FPDF_EXPORT void FPDF_CALLCONV FPDFLink_CloseWebLinks(FPDF_PAGELINK link_page) {
  delete CPDFLinkExtractFromFPDFPageLink(link_page);
}
