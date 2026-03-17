// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "public/fpdf_edit.h"

#include <math.h>
#include <algorithm>
#include <memory>
#include <utility>

#include "core/fpdfapi/page/cpdf_dib.h"
#include "core/fpdfapi/page/cpdf_image.h"
#include "core/fpdfapi/page/cpdf_imageobject.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_pageobject.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/render/cpdf_imagerenderer.h"
#include "core/fpdfapi/render/cpdf_rendercontext.h"
#include "core/fpdfapi/render/cpdf_renderstatus.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "fpdfsdk/cpdfsdk_customaccess.h"
#include "fpdfsdk/cpdfsdk_helpers.h"

namespace {

// These checks ensure the consistency of colorspace values across core/ and
// public/.
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kDeviceGray) ==
                  FPDF_COLORSPACE_DEVICEGRAY,
              "kDeviceGray value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kDeviceRGB) ==
                  FPDF_COLORSPACE_DEVICERGB,
              "kDeviceRGB value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kDeviceCMYK) ==
                  FPDF_COLORSPACE_DEVICECMYK,
              "kDeviceCMYK value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kCalGray) ==
                  FPDF_COLORSPACE_CALGRAY,
              "kCalGray value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kCalRGB) ==
                  FPDF_COLORSPACE_CALRGB,
              "kCalRGB value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kLab) ==
                  FPDF_COLORSPACE_LAB,
              "kLab value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kICCBased) ==
                  FPDF_COLORSPACE_ICCBASED,
              "kICCBased value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kSeparation) ==
                  FPDF_COLORSPACE_SEPARATION,
              "kSeparation value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kDeviceN) ==
                  FPDF_COLORSPACE_DEVICEN,
              "kDeviceN value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kIndexed) ==
                  FPDF_COLORSPACE_INDEXED,
              "kIndexed value mismatch");
static_assert(static_cast<int>(CPDF_ColorSpace::Family::kPattern) ==
                  FPDF_COLORSPACE_PATTERN,
              "kPattern value mismatch");

RetainPtr<IFX_SeekableReadStream> MakeSeekableReadStream(
    FPDF_FILEACCESS* pFileAccess) {
  return pdfium::MakeRetain<CPDFSDK_CustomAccess>(pFileAccess);
}

CPDF_ImageObject* CPDFImageObjectFromFPDFPageObject(
    FPDF_PAGEOBJECT image_object) {
  CPDF_PageObject* pPageObject = CPDFPageObjectFromFPDFPageObject(image_object);
  return pPageObject ? pPageObject->AsImage() : nullptr;
}

bool LoadJpegHelper(FPDF_PAGE* pages,
                    int count,
                    FPDF_PAGEOBJECT image_object,
                    FPDF_FILEACCESS* file_access,
                    bool inline_jpeg) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj)
    return false;

  if (!file_access)
    return false;

  if (pages) {
    for (int index = 0; index < count; index++) {
      CPDF_Page* pPage = CPDFPageFromFPDFPage(UNSAFE_TODO(pages[index]));
      if (pPage) {
        pImgObj->GetImage()->ResetCache(pPage);
      }
    }
  }

  RetainPtr<IFX_SeekableReadStream> pFile = MakeSeekableReadStream(file_access);
  if (inline_jpeg)
    pImgObj->GetImage()->SetJpegImageInline(std::move(pFile));
  else
    pImgObj->GetImage()->SetJpegImage(std::move(pFile));

  pImgObj->SetDirty(true);
  return true;
}

}  // namespace

FPDF_EXPORT FPDF_PAGEOBJECT FPDF_CALLCONV
FPDFPageObj_NewImageObj(FPDF_DOCUMENT document) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  auto pImageObj = std::make_unique<CPDF_ImageObject>();
  pImageObj->SetImage(pdfium::MakeRetain<CPDF_Image>(pDoc));

  // Caller takes ownership.
  return FPDFPageObjectFromCPDFPageObject(pImageObj.release());
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFImageObj_LoadJpegFile(FPDF_PAGE* pages,
                          int count,
                          FPDF_PAGEOBJECT image_object,
                          FPDF_FILEACCESS* file_access) {
  return LoadJpegHelper(pages, count, image_object, file_access, false);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFImageObj_LoadJpegFileInline(FPDF_PAGE* pages,
                                int count,
                                FPDF_PAGEOBJECT image_object,
                                FPDF_FILEACCESS* file_access) {
  return LoadJpegHelper(pages, count, image_object, file_access, true);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFImageObj_SetMatrix(FPDF_PAGEOBJECT image_object,
                       double a,
                       double b,
                       double c,
                       double d,
                       double e,
                       double f) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj)
    return false;

  pImgObj->SetImageMatrix(CFX_Matrix(
      static_cast<float>(a), static_cast<float>(b), static_cast<float>(c),
      static_cast<float>(d), static_cast<float>(e), static_cast<float>(f)));
  pImgObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFImageObj_SetBitmap(FPDF_PAGE* pages,
                       int count,
                       FPDF_PAGEOBJECT image_object,
                       FPDF_BITMAP bitmap) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj) {
    return false;
  }

  RetainPtr<CFX_DIBitmap> holder(CFXDIBitmapFromFPDFBitmap(bitmap));
  if (!holder) {
    return false;
  }
  CHECK(!holder->IsPremultiplied());

  if (pages) {
    for (int index = 0; index < count; index++) {
      CPDF_Page* pPage = CPDFPageFromFPDFPage(UNSAFE_TODO(pages[index]));
      if (pPage) {
        pImgObj->GetImage()->ResetCache(pPage);
      }
    }
  }

  pImgObj->GetImage()->SetImage(holder);
  pImgObj->CalcBoundingBox();
  pImgObj->SetDirty(true);
  return true;
}

FPDF_EXPORT FPDF_BITMAP FPDF_CALLCONV
FPDFImageObj_GetBitmap(FPDF_PAGEOBJECT image_object) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj)
    return nullptr;

  RetainPtr<CPDF_Image> pImg = pImgObj->GetImage();
  if (!pImg)
    return nullptr;

  RetainPtr<CFX_DIBBase> pSource = pImg->LoadDIBBase();
  if (!pSource)
    return nullptr;

  // If the source image has a representation of 1 bit per pixel, or if the
  // source image has a color palette, convert it to a representation that does
  // not have a color palette, as there is no public API to access the palette.
  //
  // Otherwise, convert the source image to a bitmap directly,
  // retaining its color representation.
  //
  // Only return FPDF_BITMAPs in formats that FPDFBitmap_CreateEx() would
  // return.
  enum class ConversionOp {
    kRealize,
    kConvertTo8bppRgb,
    kConvertToRgb,
  };

  ConversionOp op;
  switch (pSource->GetFormat()) {
    case FXDIB_Format::k1bppMask:
    case FXDIB_Format::k8bppMask: {
      // Masks do not have palettes, so they can be safely converted to
      // `FXDIB_Format::k8bppRgb`.
      CHECK(!pSource->HasPalette());
      op = ConversionOp::kConvertTo8bppRgb;
      break;
    }
    case FXDIB_Format::k1bppRgb: {
      // If there is a palette, then convert to `FXDIB_Format::kBgr` to avoid
      // creating a bitmap with a palette.
      op = pSource->HasPalette() ? ConversionOp::kConvertToRgb
                                 : ConversionOp::kConvertTo8bppRgb;
      break;
    }
    case FXDIB_Format::k8bppRgb:
    case FXDIB_Format::kBgra:
    case FXDIB_Format::kBgr:
    case FXDIB_Format::kBgrx: {
      // If there is a palette, then convert to `FXDIB_Format::kBgr` to avoid
      // creating a bitmap with a palette.
      op = pSource->HasPalette() ? ConversionOp::kConvertToRgb
                                 : ConversionOp::kRealize;
      break;
    }
    case FXDIB_Format::kInvalid: {
      NOTREACHED_NORETURN();
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul: {
      // TODO(crbug.com/355676038): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
    }
#endif
  }

  RetainPtr<CFX_DIBitmap> pBitmap;
  switch (op) {
    case ConversionOp::kRealize:
      pBitmap = pSource->Realize();
      break;
    case ConversionOp::kConvertTo8bppRgb:
      pBitmap = pSource->ConvertTo(FXDIB_Format::k8bppRgb);
      break;
    case ConversionOp::kConvertToRgb:
      pBitmap = pSource->ConvertTo(FXDIB_Format::kBgr);
      break;
  }
  if (!pBitmap) {
    return nullptr;
  }

  CHECK(!pBitmap->HasPalette());
  CHECK(!pBitmap->IsPremultiplied());

  // Caller takes ownership.
  return FPDFBitmapFromCFXDIBitmap(pBitmap.Leak());
}

FPDF_EXPORT FPDF_BITMAP FPDF_CALLCONV
FPDFImageObj_GetRenderedBitmap(FPDF_DOCUMENT document,
                               FPDF_PAGE page,
                               FPDF_PAGEOBJECT image_object) {
  CPDF_Document* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc)
    return nullptr;

  CPDF_Page* optional_page = CPDFPageFromFPDFPage(page);
  if (optional_page && optional_page->GetDocument() != doc)
    return nullptr;

  CPDF_ImageObject* image = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!image)
    return nullptr;

  // Create |result_bitmap|.
  const CFX_Matrix& image_matrix = image->matrix();
  float output_width = std::ceil(hypotf(image_matrix.a, image_matrix.c));
  float output_height = std::ceil(hypotf(image_matrix.b, image_matrix.d));
  auto result_bitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!result_bitmap->Create(static_cast<int>(output_width),
                             static_cast<int>(output_height),
                             FXDIB_Format::kBgra)) {
    return nullptr;
  }

  // Set up all the rendering code.
  RetainPtr<CPDF_Dictionary> page_resources =
      optional_page ? optional_page->GetMutablePageResources() : nullptr;
  CPDF_RenderContext context(doc, std::move(page_resources),
                             /*pPageCache=*/nullptr);
  CFX_DefaultRenderDevice device;
  device.Attach(result_bitmap);
  CPDF_RenderStatus status(&context, &device);
  CPDF_ImageRenderer renderer(&status);

  // Need to first flip the image, as expected by |renderer|.
  CFX_Matrix render_matrix(1, 0, 0, -1, 0, output_height);

  // Then take |image_matrix|'s offset into account.
  float min_x = image_matrix.e + std::min(image_matrix.a, image_matrix.c);
  float min_y = image_matrix.f + std::min(image_matrix.b, image_matrix.d);
  render_matrix.Translate(-min_x, min_y);

  // Do the actual rendering.
  bool should_continue = renderer.Start(image, render_matrix, /*bStdCS=*/false);
  while (should_continue) {
    should_continue = renderer.Continue(/*pPause=*/nullptr);
  }

  if (!renderer.GetResult())
    return nullptr;

  CHECK(!result_bitmap->IsPremultiplied());

  // Caller takes ownership.
  return FPDFBitmapFromCFXDIBitmap(result_bitmap.Leak());
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFImageObj_GetImageDataDecoded(FPDF_PAGEOBJECT image_object,
                                 void* buffer,
                                 unsigned long buflen) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj)
    return 0;

  RetainPtr<CPDF_Image> pImg = pImgObj->GetImage();
  if (!pImg)
    return 0;

  RetainPtr<const CPDF_Stream> pImgStream = pImg->GetStream();
  if (!pImgStream)
    return 0;

  // SAFETY: caller ensures `buffer` points to at least `buflen` bytes.
  return DecodeStreamMaybeCopyAndReturnLength(
      std::move(pImgStream),
      UNSAFE_BUFFERS(pdfium::make_span(static_cast<uint8_t*>(buffer),
                                       static_cast<size_t>(buflen))));
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFImageObj_GetImageDataRaw(FPDF_PAGEOBJECT image_object,
                             void* buffer,
                             unsigned long buflen) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj)
    return 0;

  RetainPtr<CPDF_Image> pImg = pImgObj->GetImage();
  if (!pImg)
    return 0;

  RetainPtr<const CPDF_Stream> pImgStream = pImg->GetStream();
  if (!pImgStream)
    return 0;

  // SAFETY: caller ensures `buffer` points to at least `buflen` bytes.
  return GetRawStreamMaybeCopyAndReturnLength(
      std::move(pImgStream),
      UNSAFE_BUFFERS(pdfium::make_span(static_cast<uint8_t*>(buffer),
                                       static_cast<size_t>(buflen))));
}

FPDF_EXPORT int FPDF_CALLCONV
FPDFImageObj_GetImageFilterCount(FPDF_PAGEOBJECT image_object) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj)
    return 0;

  RetainPtr<CPDF_Image> pImg = pImgObj->GetImage();
  if (!pImg)
    return 0;

  RetainPtr<const CPDF_Dictionary> pDict = pImg->GetDict();
  if (!pDict)
    return 0;

  RetainPtr<const CPDF_Object> pFilter = pDict->GetDirectObjectFor("Filter");
  if (!pFilter)
    return 0;

  if (pFilter->IsArray())
    return fxcrt::CollectionSize<int>(*pFilter->AsArray());

  if (pFilter->IsName())
    return 1;

  return 0;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFImageObj_GetImageFilter(FPDF_PAGEOBJECT image_object,
                            int index,
                            void* buffer,
                            unsigned long buflen) {
  if (index < 0 || index >= FPDFImageObj_GetImageFilterCount(image_object))
    return 0;

  CPDF_PageObject* pObj = CPDFPageObjectFromFPDFPageObject(image_object);
  RetainPtr<const CPDF_Dictionary> pDict =
      pObj->AsImage()->GetImage()->GetDict();
  RetainPtr<const CPDF_Object> pFilter = pDict->GetDirectObjectFor("Filter");
  ByteString bsFilter = pFilter->IsName()
                            ? pFilter->AsName()->GetString()
                            : pFilter->AsArray()->GetByteStringAt(index);

  // SAFETY: required from caller.
  return NulTerminateMaybeCopyAndReturnLength(
      bsFilter, UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFImageObj_GetImageMetadata(FPDF_PAGEOBJECT image_object,
                              FPDF_PAGE page,
                              FPDF_IMAGEOBJ_METADATA* metadata) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj || !metadata)
    return false;

  RetainPtr<CPDF_Image> pImg = pImgObj->GetImage();
  if (!pImg)
    return false;

  metadata->marked_content_id =
      pImgObj->GetContentMarks()->GetMarkedContentID();

  const int nPixelWidth = pImg->GetPixelWidth();
  const int nPixelHeight = pImg->GetPixelHeight();
  metadata->width = nPixelWidth;
  metadata->height = nPixelHeight;

  const float nWidth = pImgObj->GetRect().Width();
  const float nHeight = pImgObj->GetRect().Height();
  constexpr int nPointsPerInch = 72;
  if (nWidth != 0 && nHeight != 0) {
    metadata->horizontal_dpi = nPixelWidth / nWidth * nPointsPerInch;
    metadata->vertical_dpi = nPixelHeight / nHeight * nPointsPerInch;
  }

  metadata->bits_per_pixel = 0;
  metadata->colorspace = FPDF_COLORSPACE_UNKNOWN;

  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage || !pPage->GetDocument() || !pImg->GetStream())
    return true;

  // A cross-document image may have come from the embedder.
  if (pPage->GetDocument() != pImg->GetDocument())
    return false;

  RetainPtr<CPDF_DIB> pSource = pImg->CreateNewDIB();
  CPDF_DIB::LoadState ret = pSource->StartLoadDIBBase(
      false, nullptr, pPage->GetPageResources().Get(), false,
      CPDF_ColorSpace::Family::kUnknown, false, {0, 0});
  if (ret == CPDF_DIB::LoadState::kFail)
    return true;

  metadata->bits_per_pixel = pSource->GetBPP();
  if (pSource->GetColorSpace()) {
    metadata->colorspace =
        static_cast<int>(pSource->GetColorSpace()->GetFamily());
  }
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFImageObj_GetImagePixelSize(FPDF_PAGEOBJECT image_object,
                               unsigned int* width,
                               unsigned int* height) {
  CPDF_ImageObject* pImgObj = CPDFImageObjectFromFPDFPageObject(image_object);
  if (!pImgObj || !width || !height) {
    return false;
  }

  RetainPtr<CPDF_Image> pImg = pImgObj->GetImage();
  if (!pImg) {
    return false;
  }

  *width = pImg->GetPixelWidth();
  *height = pImg->GetPixelHeight();
  return true;
}
