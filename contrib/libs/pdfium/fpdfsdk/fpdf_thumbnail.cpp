// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "public/fpdf_thumbnail.h"

#include <utility>

#include "core/fpdfapi/page/cpdf_dib.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/span.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "fpdfsdk/cpdfsdk_helpers.h"
#include "public/fpdfview.h"

namespace {

RetainPtr<const CPDF_Stream> CPDFStreamForThumbnailFromPage(FPDF_PAGE page) {
  const CPDF_Page* pdf_page = CPDFPageFromFPDFPage(page);
  if (!pdf_page)
    return nullptr;

  RetainPtr<const CPDF_Dictionary> page_dict = pdf_page->GetDict();
  if (!page_dict->KeyExist("Type"))
    return nullptr;

  return page_dict->GetStreamFor("Thumb");
}

}  // namespace

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFPage_GetDecodedThumbnailData(FPDF_PAGE page,
                                 void* buffer,
                                 unsigned long buflen) {
  RetainPtr<const CPDF_Stream> thumb_stream =
      CPDFStreamForThumbnailFromPage(page);
  if (!thumb_stream)
    return 0u;

  // SAFETY: caller ensures `buffer` points to at least `buflen` bytes.
  return DecodeStreamMaybeCopyAndReturnLength(
      std::move(thumb_stream),
      UNSAFE_BUFFERS(pdfium::make_span(static_cast<uint8_t*>(buffer),
                                       static_cast<size_t>(buflen))));
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFPage_GetRawThumbnailData(FPDF_PAGE page,
                             void* buffer,
                             unsigned long buflen) {
  RetainPtr<const CPDF_Stream> thumb_stream =
      CPDFStreamForThumbnailFromPage(page);
  if (!thumb_stream)
    return 0u;

  // SAFETY: caller ensures `buffer` points to at least `buflen` bytes.
  return GetRawStreamMaybeCopyAndReturnLength(
      std::move(thumb_stream),
      UNSAFE_BUFFERS(pdfium::make_span(static_cast<uint8_t*>(buffer),
                                       static_cast<size_t>(buflen))));
}

FPDF_EXPORT FPDF_BITMAP FPDF_CALLCONV
FPDFPage_GetThumbnailAsBitmap(FPDF_PAGE page) {
  RetainPtr<const CPDF_Stream> thumb_stream =
      CPDFStreamForThumbnailFromPage(page);
  if (!thumb_stream)
    return nullptr;

  const CPDF_Page* pdf_page = CPDFPageFromFPDFPage(page);
  auto dib_source = pdfium::MakeRetain<CPDF_DIB>(pdf_page->GetDocument(),
                                                 std::move(thumb_stream));
  const CPDF_DIB::LoadState start_status = dib_source->StartLoadDIBBase(
      false, nullptr, pdf_page->GetPageResources().Get(), false,
      CPDF_ColorSpace::Family::kUnknown, false, {0, 0});
  if (start_status == CPDF_DIB::LoadState::kFail)
    return nullptr;

  auto thumb_bitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!thumb_bitmap->Copy(std::move(dib_source))) {
    return nullptr;
  }

  CHECK(!thumb_bitmap->IsPremultiplied());
  return FPDFBitmapFromCFXDIBitmap(thumb_bitmap.Leak());
}
