// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "public/fpdfview.h"

#include <memory>
#include <utility>
#include <vector>

#include "build/build_config.h"
#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/page/cpdf_occontext.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/page/cpdf_pageimagecache.h"
#include "core/fpdfapi/page/cpdf_pagemodule.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_parser.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfapi/render/cpdf_docrenderdata.h"
#include "core/fpdfapi/render/cpdf_pagerendercontext.h"
#include "core/fpdfapi/render/cpdf_rendercontext.h"
#include "core/fpdfapi/render/cpdf_renderoptions.h"
#include "core/fpdfdoc/cpdf_nametree.h"
#include "core/fpdfdoc/cpdf_viewerpreferences.h"
#include "core/fxcrt/cfx_read_only_span_stream.h"
#include "core/fxcrt/cfx_timer.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/ptr_util.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxge/cfx_defaultrenderdevice.h"
#include "core/fxge/cfx_gemodule.h"
#include "core/fxge/cfx_glyphcache.h"
#include "core/fxge/cfx_renderdevice.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "fpdfsdk/cpdfsdk_customaccess.h"
#include "fpdfsdk/cpdfsdk_formfillenvironment.h"
#include "fpdfsdk/cpdfsdk_helpers.h"
#include "fpdfsdk/cpdfsdk_pageview.h"
#include "fpdfsdk/cpdfsdk_renderpage.h"
#include "fxjs/ijs_runtime.h"
#include "public/fpdf_formfill.h"

#ifdef PDF_ENABLE_V8
#include "fxjs/cfx_v8_array_buffer_allocator.h"
#endif

#ifdef PDF_ENABLE_XFA
#include "fpdfsdk/fpdfxfa/cpdfxfa_context.h"
#include "fpdfsdk/fpdfxfa/cpdfxfa_page.h"
#endif  // PDF_ENABLE_XFA

#if BUILDFLAG(IS_WIN)
#include "core/fpdfapi/render/cpdf_progressiverenderer.h"
#include "core/fpdfapi/render/cpdf_windowsrenderdevice.h"
#include "public/fpdf_edit.h"

#if defined(PDF_USE_SKIA)
class SkCanvas;
#endif  // defined(PDF_USE_SKIA)

// These checks are here because core/ and public/ cannot depend on each other.
static_assert(static_cast<int>(WindowsPrintMode::kEmf) == FPDF_PRINTMODE_EMF,
              "WindowsPrintMode::kEmf value mismatch");
static_assert(static_cast<int>(WindowsPrintMode::kTextOnly) ==
                  FPDF_PRINTMODE_TEXTONLY,
              "WindowsPrintMode::kTextOnly value mismatch");
static_assert(static_cast<int>(WindowsPrintMode::kPostScript2) ==
                  FPDF_PRINTMODE_POSTSCRIPT2,
              "WindowsPrintMode::kPostScript2 value mismatch");
static_assert(static_cast<int>(WindowsPrintMode::kPostScript3) ==
                  FPDF_PRINTMODE_POSTSCRIPT3,
              "WindowsPrintMode::kPostScript3 value mismatch");
static_assert(static_cast<int>(WindowsPrintMode::kPostScript2PassThrough) ==
                  FPDF_PRINTMODE_POSTSCRIPT2_PASSTHROUGH,
              "WindowsPrintMode::kPostScript2PassThrough value mismatch");
static_assert(static_cast<int>(WindowsPrintMode::kPostScript3PassThrough) ==
                  FPDF_PRINTMODE_POSTSCRIPT3_PASSTHROUGH,
              "WindowsPrintMode::kPostScript3PassThrough value mismatch");
static_assert(static_cast<int>(WindowsPrintMode::kEmfImageMasks) ==
                  FPDF_PRINTMODE_EMF_IMAGE_MASKS,
              "WindowsPrintMode::kEmfImageMasks value mismatch");
static_assert(static_cast<int>(WindowsPrintMode::kPostScript3Type42) ==
                  FPDF_PRINTMODE_POSTSCRIPT3_TYPE42,
              "WindowsPrintMode::kPostScript3Type42 value mismatch");
static_assert(
    static_cast<int>(WindowsPrintMode::kPostScript3Type42PassThrough) ==
        FPDF_PRINTMODE_POSTSCRIPT3_TYPE42_PASSTHROUGH,
    "WindowsPrintMode::kPostScript3Type42PassThrough value mismatch");
#endif  // BUILDFLAG(IS_WIN)

#if defined(PDF_USE_SKIA)
// These checks are here because core/ and public/ cannot depend on each other.
static_assert(static_cast<int>(CFX_DefaultRenderDevice::RendererType::kAgg) ==
                  FPDF_RENDERERTYPE_AGG,
              "CFX_DefaultRenderDevice::RendererType::kAGG value mismatch");
static_assert(static_cast<int>(CFX_DefaultRenderDevice::RendererType::kSkia) ==
                  FPDF_RENDERERTYPE_SKIA,
              "CFX_DefaultRenderDevice::RendererType::kSkia value mismatch");
#endif  // defined(PDF_USE_SKIA)

namespace {

bool g_bLibraryInitialized = false;

void SetRendererType(FPDF_RENDERER_TYPE public_type) {
  // Internal definition of renderer types must stay updated with respect to
  // the public definition, such that all public definitions can be mapped to
  // an internal definition in `CFX_DefaultRenderDevice`. A public definition
  // value might not be meaningful for a particular build configuration, which
  // would mean use of that value is an error for that build.

  // AGG is always present in a build. |FPDF_RENDERERTYPE_SKIA| is valid to use
  // only if it is included in the build.
#if defined(PDF_USE_SKIA)
  // This build configuration has the option for runtime renderer selection.
  if (public_type == FPDF_RENDERERTYPE_AGG ||
      public_type == FPDF_RENDERERTYPE_SKIA) {
    CFX_DefaultRenderDevice::SetRendererType(
        static_cast<CFX_DefaultRenderDevice::RendererType>(public_type));
    return;
  }
  CHECK(false);
#else
  // `FPDF_RENDERERTYPE_AGG` is used for fully AGG builds.
  CHECK_EQ(public_type, FPDF_RENDERERTYPE_AGG);
#endif
}

void ResetRendererType() {
#if defined(PDF_USE_SKIA)
  CFX_DefaultRenderDevice::SetRendererType(
      CFX_DefaultRenderDevice::kDefaultRenderer);
#endif
}

RetainPtr<const CPDF_Object> GetXFAEntryFromDocument(const CPDF_Document* doc) {
  const CPDF_Dictionary* root = doc->GetRoot();
  if (!root)
    return nullptr;

  RetainPtr<const CPDF_Dictionary> acro_form = root->GetDictFor("AcroForm");
  return acro_form ? acro_form->GetObjectFor("XFA") : nullptr;
}

struct XFAPacket {
  ByteString name;
  RetainPtr<const CPDF_Stream> data;
};

std::vector<XFAPacket> GetXFAPackets(RetainPtr<const CPDF_Object> xfa_object) {
  std::vector<XFAPacket> packets;

  if (!xfa_object)
    return packets;

  RetainPtr<const CPDF_Stream> xfa_stream = ToStream(xfa_object->GetDirect());
  if (xfa_stream) {
    packets.push_back({"", std::move(xfa_stream)});
    return packets;
  }

  RetainPtr<const CPDF_Array> xfa_array = ToArray(xfa_object->GetDirect());
  if (!xfa_array)
    return packets;

  packets.reserve(1 + (xfa_array->size() / 2));
  for (size_t i = 0; i < xfa_array->size(); i += 2) {
    if (i + 1 == xfa_array->size())
      break;

    RetainPtr<const CPDF_String> name = xfa_array->GetStringAt(i);
    if (!name)
      continue;

    RetainPtr<const CPDF_Stream> data = xfa_array->GetStreamAt(i + 1);
    if (!data)
      continue;

    packets.push_back({name->GetString(), std::move(data)});
  }
  return packets;
}

FPDF_DOCUMENT LoadDocumentImpl(RetainPtr<IFX_SeekableReadStream> pFileAccess,
                               FPDF_BYTESTRING password) {
  if (!pFileAccess) {
    ProcessParseError(CPDF_Parser::FILE_ERROR);
    return nullptr;
  }

  auto pDocument =
      std::make_unique<CPDF_Document>(std::make_unique<CPDF_DocRenderData>(),
                                      std::make_unique<CPDF_DocPageData>());

  CPDF_Parser::Error error =
      pDocument->LoadDoc(std::move(pFileAccess), password);
  if (error != CPDF_Parser::SUCCESS) {
    ProcessParseError(error);
    return nullptr;
  }

  ReportUnsupportedFeatures(pDocument.get());
  return FPDFDocumentFromCPDFDocument(pDocument.release());
}

}  // namespace

FPDF_EXPORT void FPDF_CALLCONV FPDF_InitLibrary() {
  FPDF_InitLibraryWithConfig(nullptr);
}

FPDF_EXPORT void FPDF_CALLCONV
FPDF_InitLibraryWithConfig(const FPDF_LIBRARY_CONFIG* config) {
  if (g_bLibraryInitialized)
    return;

  FX_InitializeMemoryAllocators();
  CFX_Timer::InitializeGlobals();
  CFX_GEModule::Create(config ? config->m_pUserFontPaths : nullptr);
  CPDF_PageModule::Create();

#if defined(PDF_USE_SKIA)
  CFX_GlyphCache::InitializeGlobals();
#endif

#ifdef PDF_ENABLE_XFA
  CPDFXFA_ModuleInit();
#endif  // PDF_ENABLE_XFA

  if (config && config->version >= 2) {
    void* platform = config->version >= 3 ? config->m_pPlatform : nullptr;
    IJS_Runtime::Initialize(config->m_v8EmbedderSlot, config->m_pIsolate,
                            platform);

    if (config->version >= 4)
      SetRendererType(config->m_RendererType);
  }
  g_bLibraryInitialized = true;
}

FPDF_EXPORT void FPDF_CALLCONV FPDF_DestroyLibrary() {
  if (!g_bLibraryInitialized)
    return;

  // Note: we teardown/destroy things in reverse order.
  ResetRendererType();

  IJS_Runtime::Destroy();

#ifdef PDF_ENABLE_XFA
  CPDFXFA_ModuleDestroy();
#endif  // PDF_ENABLE_XFA

#if defined(PDF_USE_SKIA)
  CFX_GlyphCache::DestroyGlobals();
#endif

  CPDF_PageModule::Destroy();
  CFX_GEModule::Destroy();
  CFX_Timer::DestroyGlobals();
  FX_DestroyMemoryAllocators();

  g_bLibraryInitialized = false;
}

FPDF_EXPORT void FPDF_CALLCONV FPDF_SetSandBoxPolicy(FPDF_DWORD policy,
                                                     FPDF_BOOL enable) {
  return SetPDFSandboxPolicy(policy, enable);
}

#if BUILDFLAG(IS_WIN)
FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDF_SetPrintMode(int mode) {
  if (mode < FPDF_PRINTMODE_EMF ||
      mode > FPDF_PRINTMODE_POSTSCRIPT3_TYPE42_PASSTHROUGH) {
    return FALSE;
  }

  g_pdfium_print_mode = static_cast<WindowsPrintMode>(mode);
  return TRUE;
}
#endif  // BUILDFLAG(IS_WIN)

FPDF_EXPORT FPDF_DOCUMENT FPDF_CALLCONV
FPDF_LoadDocument(FPDF_STRING file_path, FPDF_BYTESTRING password) {
  // NOTE: the creation of the file needs to be by the embedder on the
  // other side of this API.
  return LoadDocumentImpl(IFX_SeekableReadStream::CreateFromFilename(file_path),
                          password);
}

FPDF_EXPORT int FPDF_CALLCONV FPDF_GetFormType(FPDF_DOCUMENT document) {
  const CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return FORMTYPE_NONE;

  const CPDF_Dictionary* pRoot = pDoc->GetRoot();
  if (!pRoot)
    return FORMTYPE_NONE;

  RetainPtr<const CPDF_Dictionary> pAcroForm = pRoot->GetDictFor("AcroForm");
  if (!pAcroForm)
    return FORMTYPE_NONE;

  RetainPtr<const CPDF_Object> pXFA = pAcroForm->GetObjectFor("XFA");
  if (!pXFA)
    return FORMTYPE_ACRO_FORM;

  bool bNeedsRendering = pRoot->GetBooleanFor("NeedsRendering", false);
  return bNeedsRendering ? FORMTYPE_XFA_FULL : FORMTYPE_XFA_FOREGROUND;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDF_LoadXFA(FPDF_DOCUMENT document) {
#ifdef PDF_ENABLE_XFA
  auto* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return false;

  auto* pContext = static_cast<CPDFXFA_Context*>(pDoc->GetExtension());
  if (pContext)
    return pContext->LoadXFADoc();
#endif  // PDF_ENABLE_XFA
  return false;
}

FPDF_EXPORT FPDF_DOCUMENT FPDF_CALLCONV
FPDF_LoadMemDocument(const void* data_buf, int size, FPDF_BYTESTRING password) {
  if (size < 0) {
    return nullptr;
  }
  // SAFETY: required from caller.
  auto data_span = UNSAFE_BUFFERS(pdfium::make_span(
      static_cast<const uint8_t*>(data_buf), static_cast<size_t>(size)));
  return LoadDocumentImpl(pdfium::MakeRetain<CFX_ReadOnlySpanStream>(data_span),
                          password);
}

FPDF_EXPORT FPDF_DOCUMENT FPDF_CALLCONV
FPDF_LoadMemDocument64(const void* data_buf,
                       size_t size,
                       FPDF_BYTESTRING password) {
  // SAFETY: required from caller.
  auto data_span = UNSAFE_BUFFERS(
      pdfium::make_span(static_cast<const uint8_t*>(data_buf), size));
  return LoadDocumentImpl(pdfium::MakeRetain<CFX_ReadOnlySpanStream>(data_span),
                          password);
}

FPDF_EXPORT FPDF_DOCUMENT FPDF_CALLCONV
FPDF_LoadCustomDocument(FPDF_FILEACCESS* pFileAccess,
                        FPDF_BYTESTRING password) {
  if (!pFileAccess)
    return nullptr;
  return LoadDocumentImpl(pdfium::MakeRetain<CPDFSDK_CustomAccess>(pFileAccess),
                          password);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDF_GetFileVersion(FPDF_DOCUMENT doc,
                                                        int* fileVersion) {
  if (!fileVersion)
    return false;

  *fileVersion = 0;
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(doc);
  if (!pDoc)
    return false;

  const CPDF_Parser* pParser = pDoc->GetParser();
  if (!pParser)
    return false;

  *fileVersion = pParser->GetFileVersion();
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDF_DocumentHasValidCrossReferenceTable(FPDF_DOCUMENT document) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  return pDoc && pDoc->has_valid_cross_reference_table();
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDF_GetDocPermissions(FPDF_DOCUMENT document) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  return pDoc ? pDoc->GetUserPermissions(/*get_owner_perms=*/true) : 0;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDF_GetDocUserPermissions(FPDF_DOCUMENT document) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  return pDoc ? pDoc->GetUserPermissions(/*get_owner_perms=*/false) : 0;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDF_GetSecurityHandlerRevision(FPDF_DOCUMENT document) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc || !pDoc->GetParser())
    return -1;

  RetainPtr<const CPDF_Dictionary> pDict = pDoc->GetParser()->GetEncryptDict();
  return pDict ? pDict->GetIntegerFor("R") : -1;
}

FPDF_EXPORT int FPDF_CALLCONV FPDF_GetPageCount(FPDF_DOCUMENT document) {
  auto* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return 0;

  auto* pExtension = pDoc->GetExtension();
  return pExtension ? pExtension->GetPageCount() : pDoc->GetPageCount();
}

FPDF_EXPORT FPDF_PAGE FPDF_CALLCONV FPDF_LoadPage(FPDF_DOCUMENT document,
                                                  int page_index) {
  auto* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  if (page_index < 0 || page_index >= FPDF_GetPageCount(document))
    return nullptr;

#ifdef PDF_ENABLE_XFA
  auto* pContext = static_cast<CPDFXFA_Context*>(pDoc->GetExtension());
  if (pContext) {
    return FPDFPageFromIPDFPage(
        pContext->GetOrCreateXFAPage(page_index).Leak());
  }
#endif  // PDF_ENABLE_XFA

  RetainPtr<CPDF_Dictionary> pDict = pDoc->GetMutablePageDictionary(page_index);
  if (!pDict)
    return nullptr;

  auto pPage = pdfium::MakeRetain<CPDF_Page>(pDoc, std::move(pDict));
  pPage->AddPageImageCache();
  pPage->ParseContent();

  return FPDFPageFromIPDFPage(pPage.Leak());
}

FPDF_EXPORT float FPDF_CALLCONV FPDF_GetPageWidthF(FPDF_PAGE page) {
  IPDF_Page* pPage = IPDFPageFromFPDFPage(page);
  return pPage ? pPage->GetPageWidth() : 0.0f;
}

FPDF_EXPORT double FPDF_CALLCONV FPDF_GetPageWidth(FPDF_PAGE page) {
  return FPDF_GetPageWidthF(page);
}

FPDF_EXPORT float FPDF_CALLCONV FPDF_GetPageHeightF(FPDF_PAGE page) {
  IPDF_Page* pPage = IPDFPageFromFPDFPage(page);
  return pPage ? pPage->GetPageHeight() : 0.0f;
}

FPDF_EXPORT double FPDF_CALLCONV FPDF_GetPageHeight(FPDF_PAGE page) {
  return FPDF_GetPageHeightF(page);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDF_GetPageBoundingBox(FPDF_PAGE page,
                                                            FS_RECTF* rect) {
  if (!rect)
    return false;

  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage)
    return false;

  *rect = FSRectFFromCFXFloatRect(pPage->GetBBox());
  return true;
}

#if BUILDFLAG(IS_WIN)
namespace {

constexpr float kEpsilonSize = 0.01f;

bool IsPageTooSmall(const CPDF_Page* page) {
  const CFX_SizeF& page_size = page->GetPageSize();
  return page_size.width < kEpsilonSize || page_size.height < kEpsilonSize;
}

bool IsScalingTooSmall(const CFX_Matrix& matrix) {
  float horizontal;
  float vertical;
  if (matrix.a == 0.0f && matrix.d == 0.0f) {
    horizontal = matrix.b;
    vertical = matrix.c;
  } else {
    horizontal = matrix.a;
    vertical = matrix.d;
  }
  return fabsf(horizontal) < kEpsilonSize || fabsf(vertical) < kEpsilonSize;
}

// Get a bitmap of just the mask section defined by |mask_box| from a full page
// bitmap |pBitmap|.
RetainPtr<CFX_DIBitmap> GetMaskBitmap(CPDF_Page* pPage,
                                      int start_x,
                                      int start_y,
                                      int size_x,
                                      int size_y,
                                      int rotate,
                                      RetainPtr<const CFX_DIBitmap> source,
                                      const CFX_FloatRect& mask_box,
                                      FX_RECT* bitmap_area) {
  if (IsPageTooSmall(pPage))
    return nullptr;

  FX_RECT page_rect(start_x, start_y, start_x + size_x, start_y + size_y);
  CFX_Matrix matrix = pPage->GetDisplayMatrix(page_rect, rotate);
  if (IsScalingTooSmall(matrix))
    return nullptr;

  *bitmap_area = matrix.TransformRect(mask_box).GetOuterRect();
  if (bitmap_area->IsEmpty())
    return nullptr;

  // Create a new bitmap to transfer part of the page bitmap to.
  RetainPtr<CFX_DIBitmap> pDst = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!pDst->Create(bitmap_area->Width(), bitmap_area->Height(),
                    FXDIB_Format::kBgra)) {
    return nullptr;
  }
  pDst->Clear(0x00ffffff);
  pDst->TransferBitmap(bitmap_area->Width(), bitmap_area->Height(),
                       std::move(source), bitmap_area->left, bitmap_area->top);
  return pDst;
}

void RenderBitmap(CFX_RenderDevice* device,
                  RetainPtr<const CFX_DIBitmap> source,
                  const FX_RECT& mask_area) {
  int size_x_bm = mask_area.Width();
  int size_y_bm = mask_area.Height();
  if (size_x_bm == 0 || size_y_bm == 0)
    return;

  // Create a new bitmap from the old one
  RetainPtr<CFX_DIBitmap> dest = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!dest->Create(size_x_bm, size_y_bm, FXDIB_Format::kBgrx)) {
    return;
  }

  dest->Clear(0xffffffff);
  dest->CompositeBitmap(0, 0, size_x_bm, size_y_bm, std::move(source), 0, 0,
                        BlendMode::kNormal, nullptr, false);

  if (device->GetDeviceType() == DeviceType::kPrinter) {
    device->StretchDIBits(std::move(dest), mask_area.left, mask_area.top,
                          size_x_bm, size_y_bm);
  } else {
    device->SetDIBits(std::move(dest), mask_area.left, mask_area.top);
  }
}

}  // namespace

FPDF_EXPORT void FPDF_CALLCONV FPDF_RenderPage(HDC dc,
                                               FPDF_PAGE page,
                                               int start_x,
                                               int start_y,
                                               int size_x,
                                               int size_y,
                                               int rotate,
                                               int flags) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage)
    return;

  auto owned_context = std::make_unique<CPDF_PageRenderContext>();
  CPDF_PageRenderContext* context = owned_context.get();
  CPDF_Page::RenderContextClearer clearer(pPage);
  pPage->SetRenderContext(std::move(owned_context));

  // Don't render the full page to bitmap for a mask unless there are a lot
  // of masks. Full page bitmaps result in large spool sizes, so they should
  // only be used when necessary. For large numbers of masks, rendering each
  // individually is inefficient and unlikely to significantly improve spool
  // size.
  const bool bEnableImageMasks =
      g_pdfium_print_mode == WindowsPrintMode::kEmfImageMasks;
  const bool bNewBitmap = pPage->BackgroundAlphaNeeded() ||
                          (pPage->HasImageMask() && !bEnableImageMasks) ||
                          pPage->GetMaskBoundingBoxes().size() > 100;
  const bool bHasMask = pPage->HasImageMask() && !bNewBitmap;
  auto* render_data = CPDF_DocRenderData::FromDocument(pPage->GetDocument());
  if (!bNewBitmap && !bHasMask) {
    context->m_pDevice = std::make_unique<CPDF_WindowsRenderDevice>(
        dc, render_data->GetPSFontTracker());
    CPDFSDK_RenderPageWithContext(context, pPage, start_x, start_y, size_x,
                                  size_y, rotate, flags,
                                  /*color_scheme=*/nullptr,
                                  /*need_to_restore=*/true, /*pause=*/nullptr);
    return;
  }

  RetainPtr<CFX_DIBitmap> pBitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  CHECK(pBitmap->Create(size_x, size_y, FXDIB_Format::kBgra));
  if (!CFX_DefaultRenderDevice::UseSkiaRenderer()) {
    // Not needed by Skia. Call it for AGG to preserve pre-existing behavior.
    pBitmap->Clear(0x00ffffff);
  }

  auto device = std::make_unique<CFX_DefaultRenderDevice>();
  device->Attach(pBitmap);
  context->m_pDevice = std::move(device);
  if (bHasMask) {
    context->m_pOptions = std::make_unique<CPDF_RenderOptions>();
    context->m_pOptions->GetOptions().bBreakForMasks = true;
  }

  CPDFSDK_RenderPageWithContext(context, pPage, start_x, start_y, size_x,
                                size_y, rotate, flags, /*color_scheme=*/nullptr,
                                /*need_to_restore=*/true,
                                /*pause=*/nullptr);

  if (!bHasMask) {
    CPDF_WindowsRenderDevice win_dc(dc, render_data->GetPSFontTracker());
    bool bitsStretched = false;
    if (win_dc.GetDeviceType() == DeviceType::kPrinter) {
      auto dest_bitmap = pdfium::MakeRetain<CFX_DIBitmap>();
      if (dest_bitmap->Create(size_x, size_y, FXDIB_Format::kBgrx)) {
        fxcrt::Fill(dest_bitmap->GetWritableBuffer().first(pBitmap->GetPitch() *
                                                           size_y),
                    -1);
        dest_bitmap->CompositeBitmap(0, 0, size_x, size_y, pBitmap, 0, 0,
                                     BlendMode::kNormal, nullptr, false);
        win_dc.StretchDIBits(std::move(dest_bitmap), 0, 0, size_x, size_y);
        bitsStretched = true;
      }
    }
    if (!bitsStretched)
      win_dc.SetDIBits(std::move(pBitmap), 0, 0);
    return;
  }

  // Finish rendering the page to bitmap and copy the correct segments
  // of the page to individual image mask bitmaps.
  const std::vector<CFX_FloatRect>& mask_boxes = pPage->GetMaskBoundingBoxes();
  std::vector<FX_RECT> bitmap_areas(mask_boxes.size());
  std::vector<RetainPtr<CFX_DIBitmap>> bitmaps(mask_boxes.size());
  for (size_t i = 0; i < mask_boxes.size(); i++) {
    bitmaps[i] = GetMaskBitmap(pPage, start_x, start_y, size_x, size_y, rotate,
                               pBitmap, mask_boxes[i], &bitmap_areas[i]);
    context->m_pRenderer->Continue(nullptr);
  }

  // Begin rendering to the printer. Add flag to indicate the renderer should
  // pause after each image mask.
  pPage->ClearRenderContext();
  owned_context = std::make_unique<CPDF_PageRenderContext>();
  context = owned_context.get();
  pPage->SetRenderContext(std::move(owned_context));
  context->m_pDevice = std::make_unique<CPDF_WindowsRenderDevice>(
      dc, render_data->GetPSFontTracker());
  context->m_pOptions = std::make_unique<CPDF_RenderOptions>();
  context->m_pOptions->GetOptions().bBreakForMasks = true;

  CPDFSDK_RenderPageWithContext(context, pPage, start_x, start_y, size_x,
                                size_y, rotate, flags, /*color_scheme=*/nullptr,
                                /*need_to_restore=*/true,
                                /*pause=*/nullptr);

  // Render masks
  for (size_t i = 0; i < mask_boxes.size(); i++) {
    // Render the bitmap for the mask and free the bitmap.
    if (bitmaps[i]) {  // will be null if mask has zero area
      RenderBitmap(context->m_pDevice.get(), std::move(bitmaps[i]),
                   bitmap_areas[i]);
    }
    // Render the next portion of page.
    context->m_pRenderer->Continue(nullptr);
  }
}
#endif  // BUILDFLAG(IS_WIN)

FPDF_EXPORT void FPDF_CALLCONV FPDF_RenderPageBitmap(FPDF_BITMAP bitmap,
                                                     FPDF_PAGE page,
                                                     int start_x,
                                                     int start_y,
                                                     int size_x,
                                                     int size_y,
                                                     int rotate,
                                                     int flags) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage) {
    return;
  }

  RetainPtr<CFX_DIBitmap> pBitmap(CFXDIBitmapFromFPDFBitmap(bitmap));
  if (!pBitmap) {
    return;
  }
  CHECK(!pBitmap->IsPremultiplied());

  auto owned_context = std::make_unique<CPDF_PageRenderContext>();
  CPDF_PageRenderContext* context = owned_context.get();
  CPDF_Page::RenderContextClearer clearer(pPage);
  pPage->SetRenderContext(std::move(owned_context));

#if defined(PDF_USE_SKIA)
  CFX_DIBitmap::ScopedPremultiplier scoped_premultiplier(
      pBitmap, CFX_DefaultRenderDevice::UseSkiaRenderer());
#endif
  auto device = std::make_unique<CFX_DefaultRenderDevice>();
  device->AttachWithRgbByteOrder(std::move(pBitmap),
                                 !!(flags & FPDF_REVERSE_BYTE_ORDER));
  context->m_pDevice = std::move(device);

  CPDFSDK_RenderPageWithContext(context, pPage, start_x, start_y, size_x,
                                size_y, rotate, flags, /*color_scheme=*/nullptr,
                                /*need_to_restore=*/true,
                                /*pause=*/nullptr);
}

FPDF_EXPORT void FPDF_CALLCONV
FPDF_RenderPageBitmapWithMatrix(FPDF_BITMAP bitmap,
                                FPDF_PAGE page,
                                const FS_MATRIX* matrix,
                                const FS_RECTF* clipping,
                                int flags) {
  CPDF_Page* pPage = CPDFPageFromFPDFPage(page);
  if (!pPage) {
    return;
  }

  RetainPtr<CFX_DIBitmap> pBitmap(CFXDIBitmapFromFPDFBitmap(bitmap));
  if (!pBitmap) {
    return;
  }
  CHECK(!pBitmap->IsPremultiplied());

  auto owned_context = std::make_unique<CPDF_PageRenderContext>();
  CPDF_PageRenderContext* context = owned_context.get();
  CPDF_Page::RenderContextClearer clearer(pPage);
  pPage->SetRenderContext(std::move(owned_context));

#if defined(PDF_USE_SKIA)
  CFX_DIBitmap::ScopedPremultiplier scoped_premultiplier(
      pBitmap, CFX_DefaultRenderDevice::UseSkiaRenderer());
#endif
  auto device = std::make_unique<CFX_DefaultRenderDevice>();
  device->AttachWithRgbByteOrder(std::move(pBitmap),
                                 !!(flags & FPDF_REVERSE_BYTE_ORDER));
  context->m_pDevice = std::move(device);

  CFX_FloatRect clipping_rect;
  if (clipping)
    clipping_rect = CFXFloatRectFromFSRectF(*clipping);
  FX_RECT clip_rect = clipping_rect.ToFxRect();

  const FX_RECT rect(0, 0, pPage->GetPageWidth(), pPage->GetPageHeight());
  CFX_Matrix transform_matrix = pPage->GetDisplayMatrix(rect, 0);
  if (matrix)
    transform_matrix *= CFXMatrixFromFSMatrix(*matrix);
  CPDFSDK_RenderPage(context, pPage, transform_matrix, clip_rect, flags,
                     /*color_scheme=*/nullptr);
}

#if defined(PDF_USE_SKIA)
FPDF_EXPORT void FPDF_CALLCONV FPDF_RenderPageSkia(FPDF_SKIA_CANVAS canvas,
                                                   FPDF_PAGE page,
                                                   int size_x,
                                                   int size_y) {
  SkCanvas* sk_canvas = SkCanvasFromFPDFSkiaCanvas(canvas);
  if (!sk_canvas) {
    return;
  }

  CPDF_Page* cpdf_page = CPDFPageFromFPDFPage(page);
  if (!cpdf_page) {
    return;
  }

  auto owned_context = std::make_unique<CPDF_PageRenderContext>();
  CPDF_PageRenderContext* context = owned_context.get();
  CPDF_Page::RenderContextClearer clearer(cpdf_page);
  cpdf_page->SetRenderContext(std::move(owned_context));

  auto device = std::make_unique<CFX_DefaultRenderDevice>();
  if (!device->AttachCanvas(*sk_canvas)) {
    return;
  }
  context->m_pDevice = std::move(device);

  CPDFSDK_RenderPageWithContext(context, cpdf_page, 0, 0, size_x, size_y, 0, 0,
                                /*color_scheme=*/nullptr,
                                /*need_to_restore=*/true, /*pause=*/nullptr);
}
#endif  // defined(PDF_USE_SKIA)

FPDF_EXPORT void FPDF_CALLCONV FPDF_ClosePage(FPDF_PAGE page) {
  if (!page)
    return;

  // Take it back across the API and hold for duration of this function.
  RetainPtr<IPDF_Page> pPage;
  pPage.Unleak(IPDFPageFromFPDFPage(page));

  if (pPage->AsXFAPage())
    return;

  // This will delete the PageView object corresponding to |pPage|. We must
  // cleanup the PageView before releasing the reference on |pPage| as it will
  // attempt to reset the PageView during destruction.
  pPage->AsPDFPage()->ClearView();
}

FPDF_EXPORT void FPDF_CALLCONV FPDF_CloseDocument(FPDF_DOCUMENT document) {
  // Take it back across the API and throw it away,
  std::unique_ptr<CPDF_Document>(CPDFDocumentFromFPDFDocument(document));
}

FPDF_EXPORT unsigned long FPDF_CALLCONV FPDF_GetLastError() {
  return FXSYS_GetLastError();
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDF_DeviceToPage(FPDF_PAGE page,
                                                      int start_x,
                                                      int start_y,
                                                      int size_x,
                                                      int size_y,
                                                      int rotate,
                                                      int device_x,
                                                      int device_y,
                                                      double* page_x,
                                                      double* page_y) {
  if (!page || !page_x || !page_y)
    return false;

  IPDF_Page* pPage = IPDFPageFromFPDFPage(page);
  const FX_RECT rect(start_x, start_y, start_x + size_x, start_y + size_y);
  std::optional<CFX_PointF> pos =
      pPage->DeviceToPage(rect, rotate, CFX_PointF(device_x, device_y));
  if (!pos.has_value())
    return false;

  *page_x = pos->x;
  *page_y = pos->y;
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDF_PageToDevice(FPDF_PAGE page,
                                                      int start_x,
                                                      int start_y,
                                                      int size_x,
                                                      int size_y,
                                                      int rotate,
                                                      double page_x,
                                                      double page_y,
                                                      int* device_x,
                                                      int* device_y) {
  if (!page || !device_x || !device_y)
    return false;

  IPDF_Page* pPage = IPDFPageFromFPDFPage(page);
  const FX_RECT rect(start_x, start_y, start_x + size_x, start_y + size_y);
  CFX_PointF page_point(static_cast<float>(page_x), static_cast<float>(page_y));
  std::optional<CFX_PointF> pos = pPage->PageToDevice(rect, rotate, page_point);
  if (!pos.has_value())
    return false;

  *device_x = FXSYS_roundf(pos->x);
  *device_y = FXSYS_roundf(pos->y);
  return true;
}

FPDF_EXPORT FPDF_BITMAP FPDF_CALLCONV FPDFBitmap_Create(int width,
                                                        int height,
                                                        int alpha) {
  auto pBitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!pBitmap->Create(width, height,
                       alpha ? FXDIB_Format::kBgra : FXDIB_Format::kBgrx)) {
    return nullptr;
  }

  CHECK(!pBitmap->IsPremultiplied());

  // Caller takes ownership.
  return FPDFBitmapFromCFXDIBitmap(pBitmap.Leak());
}

FPDF_EXPORT FPDF_BITMAP FPDF_CALLCONV FPDFBitmap_CreateEx(int width,
                                                          int height,
                                                          int format,
                                                          void* first_scan,
                                                          int stride) {
  FXDIB_Format fx_format = FXDIBFormatFromFPDFFormat(format);
  if (fx_format == FXDIB_Format::kInvalid) {
    return nullptr;
  }

  // Ensure external memory is good at least for the duration of this call.
  UnownedPtr<uint8_t> pChecker(static_cast<uint8_t*>(first_scan));
  auto pBitmap = pdfium::MakeRetain<CFX_DIBitmap>();
  if (!pBitmap->Create(width, height, fx_format, pChecker, stride)) {
    return nullptr;
  }

  CHECK(!pBitmap->IsPremultiplied());

  // Caller takes ownership.
  return FPDFBitmapFromCFXDIBitmap(pBitmap.Leak());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFBitmap_GetFormat(FPDF_BITMAP bitmap) {
  RetainPtr<CFX_DIBitmap> pBitmap(CFXDIBitmapFromFPDFBitmap(bitmap));
  if (!pBitmap) {
    return FPDFBitmap_Unknown;
  }

  switch (pBitmap->GetFormat()) {
    case FXDIB_Format::k8bppRgb:
    case FXDIB_Format::k8bppMask:
      return FPDFBitmap_Gray;
    case FXDIB_Format::kBgr:
      return FPDFBitmap_BGR;
    case FXDIB_Format::kBgrx:
      return FPDFBitmap_BGRx;
    case FXDIB_Format::kBgra:
      return FPDFBitmap_BGRA;
    default:
      return FPDFBitmap_Unknown;
  }
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV FPDFBitmap_FillRect(FPDF_BITMAP bitmap,
                                                        int left,
                                                        int top,
                                                        int width,
                                                        int height,
                                                        FPDF_DWORD color) {
  RetainPtr<CFX_DIBitmap> pBitmap(CFXDIBitmapFromFPDFBitmap(bitmap));
  if (!pBitmap) {
    return false;
  }
  CHECK(!pBitmap->IsPremultiplied());

  FX_SAFE_INT32 right = left;
  right += width;
  if (!right.IsValid()) {
    return false;
  }

  FX_SAFE_INT32 bottom = top;
  bottom += height;
  if (!bottom.IsValid()) {
    return false;
  }

  FX_RECT fill_rect(left, top, right.ValueOrDie(), bottom.ValueOrDie());

  if (!pBitmap->IsAlphaFormat()) {
    color |= 0xFF000000;
  }

  // Let CFX_DefaultRenderDevice handle the 8-bit case.
  const int bpp = pBitmap->GetBPP();
  if (bpp == 8) {
    CFX_DefaultRenderDevice device;
    device.Attach(std::move(pBitmap));
    return device.FillRect(fill_rect, static_cast<uint32_t>(color));
  }

  // Handle filling 24/32-bit bitmaps directly without CFX_DefaultRenderDevice.
  // When CFX_DefaultRenderDevice is using Skia, this avoids extra work to
  // change `pBitmap` to be premultiplied and back, or extra work to change
  // `pBitmap` to 32 BPP and back.
  fill_rect.Intersect(FX_RECT(0, 0, pBitmap->GetWidth(), pBitmap->GetHeight()));
  if (fill_rect.IsEmpty()) {
    // CFX_DefaultRenderDevice treats this as success. Match that.
    return true;
  }

  const int row_end = fill_rect.top + fill_rect.Height();
  if (bpp == 32) {
    for (int row = fill_rect.top; row < row_end; ++row) {
      auto span32 = pBitmap->GetWritableScanlineAs<uint32_t>(row).subspan(
          fill_rect.left, fill_rect.Width());
      fxcrt::Fill(span32, static_cast<uint32_t>(color));
    }
    return true;
  }

  CHECK_EQ(bpp, 24);
  const FX_BGR_STRUCT<uint8_t> bgr = {.blue = FXARGB_B(color),
                                      .green = FXARGB_G(color),
                                      .red = FXARGB_R(color)};
  for (int row = fill_rect.top; row < row_end; ++row) {
    auto bgr_span =
        pBitmap->GetWritableScanlineAs<FX_BGR_STRUCT<uint8_t>>(row).subspan(
            fill_rect.left, fill_rect.Width());
    fxcrt::Fill(bgr_span, bgr);
  }
  return true;
}

FPDF_EXPORT void* FPDF_CALLCONV FPDFBitmap_GetBuffer(FPDF_BITMAP bitmap) {
  RetainPtr<CFX_DIBitmap> pBitmap(CFXDIBitmapFromFPDFBitmap(bitmap));
  return pBitmap ? pBitmap->GetWritableBuffer().data() : nullptr;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFBitmap_GetWidth(FPDF_BITMAP bitmap) {
  RetainPtr<CFX_DIBitmap> pBitmap(CFXDIBitmapFromFPDFBitmap(bitmap));
  return pBitmap ? pBitmap->GetWidth() : 0;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFBitmap_GetHeight(FPDF_BITMAP bitmap) {
  RetainPtr<CFX_DIBitmap> pBitmap(CFXDIBitmapFromFPDFBitmap(bitmap));
  return pBitmap ? pBitmap->GetHeight() : 0;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFBitmap_GetStride(FPDF_BITMAP bitmap) {
  RetainPtr<CFX_DIBitmap> pBitmap(CFXDIBitmapFromFPDFBitmap(bitmap));
  return pBitmap ? pBitmap->GetPitch() : 0;
}

FPDF_EXPORT void FPDF_CALLCONV FPDFBitmap_Destroy(FPDF_BITMAP bitmap) {
  RetainPtr<CFX_DIBitmap> destroyer;
  destroyer.Unleak(CFXDIBitmapFromFPDFBitmap(bitmap));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDF_GetPageSizeByIndexF(FPDF_DOCUMENT document,
                         int page_index,
                         FS_SIZEF* size) {
  if (!size)
    return false;

  auto* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return false;

#ifdef PDF_ENABLE_XFA
  if (page_index < 0 || page_index >= FPDF_GetPageCount(document))
    return false;

  auto* pContext = static_cast<CPDFXFA_Context*>(pDoc->GetExtension());
  if (pContext) {
    RetainPtr<CPDFXFA_Page> pPage = pContext->GetOrCreateXFAPage(page_index);
    if (!pPage)
      return false;

    size->width = pPage->GetPageWidth();
    size->height = pPage->GetPageHeight();
    return true;
  }
#endif  // PDF_ENABLE_XFA

  RetainPtr<CPDF_Dictionary> pDict = pDoc->GetMutablePageDictionary(page_index);
  if (!pDict)
    return false;

  auto page = pdfium::MakeRetain<CPDF_Page>(pDoc, std::move(pDict));
  page->AddPageImageCache();
  size->width = page->GetPageWidth();
  size->height = page->GetPageHeight();
  return true;
}

FPDF_EXPORT int FPDF_CALLCONV FPDF_GetPageSizeByIndex(FPDF_DOCUMENT document,
                                                      int page_index,
                                                      double* width,
                                                      double* height) {
  if (!width || !height)
    return false;

  FS_SIZEF size;
  if (!FPDF_GetPageSizeByIndexF(document, page_index, &size))
    return false;

  *width = size.width;
  *height = size.height;
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDF_VIEWERREF_GetPrintScaling(FPDF_DOCUMENT document) {
  const CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return true;
  CPDF_ViewerPreferences viewRef(pDoc);
  return viewRef.PrintScaling();
}

FPDF_EXPORT int FPDF_CALLCONV
FPDF_VIEWERREF_GetNumCopies(FPDF_DOCUMENT document) {
  const CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return 1;
  CPDF_ViewerPreferences viewRef(pDoc);
  return viewRef.NumCopies();
}

FPDF_EXPORT FPDF_PAGERANGE FPDF_CALLCONV
FPDF_VIEWERREF_GetPrintPageRange(FPDF_DOCUMENT document) {
  const CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;
  CPDF_ViewerPreferences viewRef(pDoc);

  // Unretained reference in public API. NOLINTNEXTLINE
  return FPDFPageRangeFromCPDFArray(viewRef.PrintPageRange());
}

FPDF_EXPORT size_t FPDF_CALLCONV
FPDF_VIEWERREF_GetPrintPageRangeCount(FPDF_PAGERANGE pagerange) {
  const CPDF_Array* pArray = CPDFArrayFromFPDFPageRange(pagerange);
  return pArray ? pArray->size() : 0;
}

FPDF_EXPORT int FPDF_CALLCONV
FPDF_VIEWERREF_GetPrintPageRangeElement(FPDF_PAGERANGE pagerange,
                                        size_t index) {
  const CPDF_Array* pArray = CPDFArrayFromFPDFPageRange(pagerange);
  if (!pArray || index >= pArray->size())
    return -1;
  return pArray->GetIntegerAt(index);
}

FPDF_EXPORT FPDF_DUPLEXTYPE FPDF_CALLCONV
FPDF_VIEWERREF_GetDuplex(FPDF_DOCUMENT document) {
  const CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return DuplexUndefined;
  CPDF_ViewerPreferences viewRef(pDoc);
  ByteString duplex = viewRef.Duplex();
  if ("Simplex" == duplex)
    return Simplex;
  if ("DuplexFlipShortEdge" == duplex)
    return DuplexFlipShortEdge;
  if ("DuplexFlipLongEdge" == duplex)
    return DuplexFlipLongEdge;
  return DuplexUndefined;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDF_VIEWERREF_GetName(FPDF_DOCUMENT document,
                       FPDF_BYTESTRING key,
                       char* buffer,
                       unsigned long length) {
  const CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return 0;

  CPDF_ViewerPreferences viewRef(pDoc);
  std::optional<ByteString> bsVal = viewRef.GenericName(key);
  if (!bsVal.has_value()) {
    return 0;
  }
  // SAFETY: required from caller.
  return NulTerminateMaybeCopyAndReturnLength(
      bsVal.value(), UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, length)));
}

FPDF_EXPORT FPDF_DWORD FPDF_CALLCONV
FPDF_CountNamedDests(FPDF_DOCUMENT document) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return 0;

  const CPDF_Dictionary* pRoot = pDoc->GetRoot();
  if (!pRoot)
    return 0;

  auto name_tree = CPDF_NameTree::Create(pDoc, "Dests");
  FX_SAFE_UINT32 count = name_tree ? name_tree->GetCount() : 0;
  RetainPtr<const CPDF_Dictionary> pOldStyleDests = pRoot->GetDictFor("Dests");
  if (pOldStyleDests)
    count += pOldStyleDests->size();
  return count.ValueOrDefault(0);
}

FPDF_EXPORT FPDF_DEST FPDF_CALLCONV
FPDF_GetNamedDestByName(FPDF_DOCUMENT document, FPDF_BYTESTRING name) {
  if (!name || name[0] == 0)
    return nullptr;

  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  ByteString dest_name(name);

  // TODO(tsepez): murky ownership, should caller get a reference?
  // Unretained reference in public API. NOLINTNEXTLINE
  return FPDFDestFromCPDFArray(CPDF_NameTree::LookupNamedDest(pDoc, dest_name));
}

#ifdef PDF_ENABLE_V8
FPDF_EXPORT const char* FPDF_CALLCONV FPDF_GetRecommendedV8Flags() {
  // Use interpreted JS only to avoid RWX pages in our address space. Also,
  // --jitless implies --no-expose-wasm, which reduce exposure since no PDF
  // should contain web assembly.
  return "--jitless";
}

FPDF_EXPORT void* FPDF_CALLCONV FPDF_GetArrayBufferAllocatorSharedInstance() {
  // Deliberately leaked. This allocator is used outside of the library
  // initialization / destruction lifecycle, and the caller does not take
  // ownership of the object. Thus there is no existing way to delete this.
  static auto* s_allocator = new CFX_V8ArrayBufferAllocator();
  return s_allocator;
}
#endif  // PDF_ENABLE_V8

#ifdef PDF_ENABLE_XFA
FPDF_EXPORT FPDF_RESULT FPDF_CALLCONV FPDF_BStr_Init(FPDF_BSTR* bstr) {
  if (!bstr)
    return -1;

  bstr->str = nullptr;
  bstr->len = 0;
  return 0;
}

FPDF_EXPORT FPDF_RESULT FPDF_CALLCONV FPDF_BStr_Set(FPDF_BSTR* bstr,
                                                    const char* cstr,
                                                    int length) {
  if (!bstr || !cstr)
    return -1;

  if (length == -1)
    length = pdfium::checked_cast<int>(strlen(cstr));

  if (length == 0) {
    FPDF_BStr_Clear(bstr);
    return 0;
  }

  if (!bstr->str) {
    bstr->str = FX_Alloc(char, length + 1);
  } else if (bstr->len < length) {
    bstr->str = FX_Realloc(char, bstr->str, length + 1);
  }

  // SAFETY: only alloc/realloc is performed above and will ensure at least
  // length + 1 bytes are available.
  UNSAFE_BUFFERS({
    bstr->str[length] = 0;
    FXSYS_memcpy(bstr->str, cstr, length);
  });
  bstr->len = length;
  return 0;
}

FPDF_EXPORT FPDF_RESULT FPDF_CALLCONV FPDF_BStr_Clear(FPDF_BSTR* bstr) {
  if (!bstr)
    return -1;

  if (bstr->str) {
    FX_Free(bstr->str);
    bstr->str = nullptr;
  }
  bstr->len = 0;
  return 0;
}
#endif  // PDF_ENABLE_XFA

FPDF_EXPORT FPDF_DEST FPDF_CALLCONV FPDF_GetNamedDest(FPDF_DOCUMENT document,
                                                      int index,
                                                      void* buffer,
                                                      long* buflen) {
  if (!buffer)
    *buflen = 0;

  if (index < 0)
    return nullptr;

  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  const CPDF_Dictionary* pRoot = pDoc->GetRoot();
  if (!pRoot)
    return nullptr;

  auto name_tree = CPDF_NameTree::Create(pDoc, "Dests");
  size_t name_tree_count = name_tree ? name_tree->GetCount() : 0;
  RetainPtr<const CPDF_Object> pDestObj;
  WideString wsName;
  if (static_cast<size_t>(index) >= name_tree_count) {
    // If |index| is out of bounds, then try to retrieve the Nth old style named
    // destination. Where N is 0-indexed, with N = index - name_tree_count.
    RetainPtr<const CPDF_Dictionary> pDest = pRoot->GetDictFor("Dests");
    if (!pDest)
      return nullptr;

    FX_SAFE_INT32 checked_count = name_tree_count;
    checked_count += pDest->size();
    if (!checked_count.IsValid() || index >= checked_count.ValueOrDie())
      return nullptr;

    index -= name_tree_count;
    int i = 0;
    ByteStringView bsName;
    CPDF_DictionaryLocker locker(pDest);
    for (const auto& it : locker) {
      bsName = it.first.AsStringView();
      pDestObj = it.second;
      if (i == index)
        break;
      i++;
    }
    wsName = PDF_DecodeText(bsName.unsigned_span());
  } else {
    pDestObj = name_tree->LookupValueAndName(index, &wsName);
  }
  if (!pDestObj)
    return nullptr;
  if (const CPDF_Dictionary* pDict = pDestObj->AsDictionary()) {
    pDestObj = pDict->GetArrayFor("D");
    if (!pDestObj)
      return nullptr;
  }
  if (!pDestObj->IsArray())
    return nullptr;

  ByteString utf16Name = wsName.ToUTF16LE();
  int len = pdfium::checked_cast<int>(utf16Name.GetLength());
  if (!buffer) {
    *buflen = len;
  } else if (len <= *buflen) {
    // SAFETY: required from caller.
    auto buffer_span =
        UNSAFE_BUFFERS(pdfium::make_span(static_cast<char*>(buffer), *buflen));
    fxcrt::Copy(utf16Name.span(), buffer_span);
    *buflen = len;
  } else {
    *buflen = -1;
  }
  return FPDFDestFromCPDFArray(pDestObj->AsArray());
}

FPDF_EXPORT int FPDF_CALLCONV FPDF_GetXFAPacketCount(FPDF_DOCUMENT document) {
  CPDF_Document* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc)
    return -1;

  return fxcrt::CollectionSize<int>(
      GetXFAPackets(GetXFAEntryFromDocument(doc)));
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDF_GetXFAPacketName(FPDF_DOCUMENT document,
                      int index,
                      void* buffer,
                      unsigned long buflen) {
  CPDF_Document* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc || index < 0)
    return 0;

  std::vector<XFAPacket> xfa_packets =
      GetXFAPackets(GetXFAEntryFromDocument(doc));
  if (static_cast<size_t>(index) >= xfa_packets.size()) {
    return 0;
  }
  // SAFETY: required from caller.
  return NulTerminateMaybeCopyAndReturnLength(
      xfa_packets[index].name,
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDF_GetXFAPacketContent(FPDF_DOCUMENT document,
                         int index,
                         void* buffer,
                         unsigned long buflen,
                         unsigned long* out_buflen) {
  CPDF_Document* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc || index < 0 || !out_buflen)
    return false;

  std::vector<XFAPacket> xfa_packets =
      GetXFAPackets(GetXFAEntryFromDocument(doc));
  if (static_cast<size_t>(index) >= xfa_packets.size())
    return false;

  // SAFETY: caller ensures `buffer` points to at least `buflen` bytes.
  *out_buflen = DecodeStreamMaybeCopyAndReturnLength(
      xfa_packets[index].data,
      UNSAFE_BUFFERS(pdfium::make_span(static_cast<uint8_t*>(buffer),
                                       static_cast<size_t>(buflen))));
  return true;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDF_GetTrailerEnds(FPDF_DOCUMENT document,
                    unsigned int* buffer,
                    unsigned long length) {
  auto* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc)
    return 0;

  // Start recording trailer ends.
  auto* parser = doc->GetParser();
  std::vector<unsigned int> trailer_ends = parser->GetTrailerEnds();
  const unsigned long trailer_ends_len =
      fxcrt::CollectionSize<unsigned long>(trailer_ends);
  if (buffer && length >= trailer_ends_len) {
    // SAFETY: required from caller.
    fxcrt::Copy(trailer_ends,
                UNSAFE_BUFFERS(pdfium::make_span(buffer, length)));
  }

  return trailer_ends_len;
}
