// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/cpdfsdk_helpers.h"

#include <utility>

#include "build/build_config.h"
#include "constants/form_fields.h"
#include "constants/stream_dict_common.h"
#include "core/fpdfapi/page/cpdf_page.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/render/cpdf_renderoptions.h"
#include "core/fpdfdoc/cpdf_annot.h"
#include "core/fpdfdoc/cpdf_interactiveform.h"
#include "core/fpdfdoc/cpdf_metadata.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxcrt/unowned_ptr.h"
#include "fpdfsdk/cpdfsdk_formfillenvironment.h"

namespace {

constexpr char kQuadPoints[] = "QuadPoints";

// 0 bit: FPDF_POLICY_MACHINETIME_ACCESS
uint32_t g_sandbox_policy = 0xFFFFFFFF;

UNSUPPORT_INFO* g_unsupport_info = nullptr;

bool RaiseUnsupportedError(int nError) {
  if (!g_unsupport_info)
    return false;

  if (g_unsupport_info->FSDK_UnSupport_Handler)
    g_unsupport_info->FSDK_UnSupport_Handler(g_unsupport_info, nError);
  return true;
}

// Use the existence of the XFA array as a signal for XFA forms.
bool DocHasXFA(const CPDF_Document* doc) {
  const CPDF_Dictionary* root = doc->GetRoot();
  if (!root)
    return false;

  RetainPtr<const CPDF_Dictionary> form = root->GetDictFor("AcroForm");
  return form && form->GetArrayFor("XFA");
}

unsigned long GetStreamMaybeCopyAndReturnLengthImpl(
    RetainPtr<const CPDF_Stream> stream,
    pdfium::span<uint8_t> buffer,
    bool decode) {
  DCHECK(stream);
  auto stream_acc = pdfium::MakeRetain<CPDF_StreamAcc>(std::move(stream));
  if (decode)
    stream_acc->LoadAllDataFiltered();
  else
    stream_acc->LoadAllDataRaw();

  pdfium::span<const uint8_t> stream_data_span = stream_acc->GetSpan();
  if (!buffer.empty() && buffer.size() <= stream_data_span.size()) {
    fxcrt::Copy(stream_data_span, buffer);
  }
  return pdfium::checked_cast<unsigned long>(stream_data_span.size());
}

// TODO(tsepez): should be UNSAFE_BUFFER_USAGE.
size_t FPDFWideStringLength(const unsigned short* str) {
  if (!str) {
    return 0;
  }
  size_t len = 0;
  // SAFETY: NUL-termination required from caller.
  UNSAFE_BUFFERS({
    while (str[len]) {
      len++;
    }
  });
  return len;
}

#ifdef PDF_ENABLE_XFA
class FPDF_FileHandlerContext final : public IFX_SeekableStream {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // IFX_SeekableStream:
  FX_FILESIZE GetSize() override;
  FX_FILESIZE GetPosition() override;
  bool IsEOF() override;
  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override;
  bool WriteBlock(pdfium::span<const uint8_t> buffer) override;
  bool Flush() override;

 private:
  explicit FPDF_FileHandlerContext(FPDF_FILEHANDLER* pFS);
  ~FPDF_FileHandlerContext() override;

  UnownedPtr<FPDF_FILEHANDLER> const m_pFS;
  FX_FILESIZE m_nCurPos = 0;
};

FPDF_FileHandlerContext::FPDF_FileHandlerContext(FPDF_FILEHANDLER* pFS)
    : m_pFS(pFS) {
  CHECK(m_pFS);
}

FPDF_FileHandlerContext::~FPDF_FileHandlerContext() {
  if (m_pFS->Release) {
    m_pFS->Release(m_pFS->clientData);
  }
}

FX_FILESIZE FPDF_FileHandlerContext::GetSize() {
  if (m_pFS->GetSize) {
    return static_cast<FX_FILESIZE>(m_pFS->GetSize(m_pFS->clientData));
  }
  return 0;
}

bool FPDF_FileHandlerContext::IsEOF() {
  return m_nCurPos >= GetSize();
}

FX_FILESIZE FPDF_FileHandlerContext::GetPosition() {
  return m_nCurPos;
}

bool FPDF_FileHandlerContext::ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                                                FX_FILESIZE offset) {
  if (buffer.empty() || !m_pFS->ReadBlock) {
    return false;
  }

  FX_SAFE_FILESIZE new_position = offset;
  new_position += buffer.size();
  if (!new_position.IsValid()) {
    return false;
  }

  if (m_pFS->ReadBlock(m_pFS->clientData, static_cast<FPDF_DWORD>(offset),
                       buffer.data(),
                       static_cast<FPDF_DWORD>(buffer.size())) != 0) {
    return false;
  }

  m_nCurPos = new_position.ValueOrDie();
  return true;
}

bool FPDF_FileHandlerContext::WriteBlock(pdfium::span<const uint8_t> buffer) {
  if (!m_pFS->WriteBlock) {
    return false;
  }

  const FX_FILESIZE size = GetSize();
  FX_SAFE_FILESIZE new_position = size;
  new_position += buffer.size();
  if (!new_position.IsValid()) {
    return false;
  }

  if (m_pFS->WriteBlock(m_pFS->clientData, static_cast<FPDF_DWORD>(size),
                        buffer.data(),
                        static_cast<FPDF_DWORD>(buffer.size())) != 0) {
    return false;
  }

  m_nCurPos = new_position.ValueOrDie();
  return true;
}

bool FPDF_FileHandlerContext::Flush() {
  if (!m_pFS->Flush) {
    return true;
  }

  return m_pFS->Flush(m_pFS->clientData) == 0;
}
#endif  // PDF_ENABLE_XFA

}  // namespace

IPDF_Page* IPDFPageFromFPDFPage(FPDF_PAGE page) {
  return reinterpret_cast<IPDF_Page*>(page);
}

FPDF_PAGE FPDFPageFromIPDFPage(IPDF_Page* page) {
  return reinterpret_cast<FPDF_PAGE>(page);
}

CPDF_Document* CPDFDocumentFromFPDFDocument(FPDF_DOCUMENT doc) {
  return reinterpret_cast<CPDF_Document*>(doc);
}

FPDF_DOCUMENT FPDFDocumentFromCPDFDocument(CPDF_Document* doc) {
  return reinterpret_cast<FPDF_DOCUMENT>(doc);
}

CPDF_Page* CPDFPageFromFPDFPage(FPDF_PAGE page) {
  return page ? IPDFPageFromFPDFPage(page)->AsPDFPage() : nullptr;
}

FXDIB_Format FXDIBFormatFromFPDFFormat(int format) {
  switch (format) {
    case FPDFBitmap_Gray:
      return FXDIB_Format::k8bppRgb;
    case FPDFBitmap_BGR:
      return FXDIB_Format::kBgr;
    case FPDFBitmap_BGRx:
      return FXDIB_Format::kBgrx;
    case FPDFBitmap_BGRA:
      return FXDIB_Format::kBgra;
    default:
      return FXDIB_Format::kInvalid;
  }
}

CPDFSDK_InteractiveForm* FormHandleToInteractiveForm(FPDF_FORMHANDLE hHandle) {
  CPDFSDK_FormFillEnvironment* pFormFillEnv =
      CPDFSDKFormFillEnvironmentFromFPDFFormHandle(hHandle);
  return pFormFillEnv ? pFormFillEnv->GetInteractiveForm() : nullptr;
}

ByteString ByteStringFromFPDFWideString(FPDF_WIDESTRING wide_string) {
  // SAFETY: caller ensures `wide_string` is NUL-terminated and enforced
  // by UNSAFE_BUFFER_USAGE in header file.
  return UNSAFE_BUFFERS(WideStringFromFPDFWideString(wide_string).ToUTF8());
}

WideString WideStringFromFPDFWideString(FPDF_WIDESTRING wide_string) {
  // SAFETY: caller ensures `wide_string` is NUL-terminated and enforced
  // by UNSAFE_BUFFER_USAGE in header file.
  return WideString::FromUTF16LE(UNSAFE_BUFFERS(
      pdfium::make_span(reinterpret_cast<const uint8_t*>(wide_string),
                        FPDFWideStringLength(wide_string) * 2)));
}

UNSAFE_BUFFER_USAGE pdfium::span<char> SpanFromFPDFApiArgs(
    void* buffer,
    pdfium::StrictNumeric<size_t> buflen) {
  if (!buffer) {
    // API convention is to ignore `buflen` arg when `buffer` is NULL.
    return pdfium::span<char>();
  }
  // SAFETY: required from caller, enforced by UNSAFE_BUFFER_USAGE in header.
  return UNSAFE_BUFFERS(pdfium::make_span(static_cast<char*>(buffer), buflen));
}

#ifdef PDF_ENABLE_XFA
RetainPtr<IFX_SeekableStream> MakeSeekableStream(
    FPDF_FILEHANDLER* pFilehandler) {
  return pdfium::MakeRetain<FPDF_FileHandlerContext>(pFilehandler);
}
#endif  // PDF_ENABLE_XFA

RetainPtr<const CPDF_Array> GetQuadPointsArrayFromDictionary(
    const CPDF_Dictionary* dict) {
  return dict->GetArrayFor("QuadPoints");
}

RetainPtr<CPDF_Array> GetMutableQuadPointsArrayFromDictionary(
    CPDF_Dictionary* dict) {
  return pdfium::WrapRetain(
      const_cast<CPDF_Array*>(GetQuadPointsArrayFromDictionary(dict).Get()));
}

RetainPtr<CPDF_Array> AddQuadPointsArrayToDictionary(CPDF_Dictionary* dict) {
  return dict->SetNewFor<CPDF_Array>(kQuadPoints);
}

bool IsValidQuadPointsIndex(const CPDF_Array* array, size_t index) {
  return array && index < array->size() / 8;
}

bool GetQuadPointsAtIndex(RetainPtr<const CPDF_Array> array,
                          size_t quad_index,
                          FS_QUADPOINTSF* quad_points) {
  DCHECK(quad_points);
  DCHECK(array);

  if (!IsValidQuadPointsIndex(array, quad_index))
    return false;

  quad_index *= 8;
  quad_points->x1 = array->GetFloatAt(quad_index);
  quad_points->y1 = array->GetFloatAt(quad_index + 1);
  quad_points->x2 = array->GetFloatAt(quad_index + 2);
  quad_points->y2 = array->GetFloatAt(quad_index + 3);
  quad_points->x3 = array->GetFloatAt(quad_index + 4);
  quad_points->y3 = array->GetFloatAt(quad_index + 5);
  quad_points->x4 = array->GetFloatAt(quad_index + 6);
  quad_points->y4 = array->GetFloatAt(quad_index + 7);
  return true;
}

CFX_PointF CFXPointFFromFSPointF(const FS_POINTF& point) {
  return CFX_PointF(point.x, point.y);
}

CFX_FloatRect CFXFloatRectFromFSRectF(const FS_RECTF& rect) {
  return CFX_FloatRect(rect.left, rect.bottom, rect.right, rect.top);
}

FS_RECTF FSRectFFromCFXFloatRect(const CFX_FloatRect& rect) {
  return {rect.left, rect.top, rect.right, rect.bottom};
}

CFX_Matrix CFXMatrixFromFSMatrix(const FS_MATRIX& matrix) {
  return CFX_Matrix(matrix.a, matrix.b, matrix.c, matrix.d, matrix.e, matrix.f);
}

FS_MATRIX FSMatrixFromCFXMatrix(const CFX_Matrix& matrix) {
  return {matrix.a, matrix.b, matrix.c, matrix.d, matrix.e, matrix.f};
}

unsigned long NulTerminateMaybeCopyAndReturnLength(
    const ByteString& text,
    pdfium::span<char> result_span) {
  pdfium::span<const char> text_span = text.span_with_terminator();
  fxcrt::try_spancpy(result_span, text_span);
  return pdfium::checked_cast<unsigned long>(text_span.size());
}

unsigned long Utf16EncodeMaybeCopyAndReturnLength(
    const WideString& text,
    pdfium::span<char> result_span) {
  ByteString encoded_text = text.ToUTF16LE();
  pdfium::span<const char> encoded_text_span = encoded_text.span();
  fxcrt::try_spancpy(result_span, encoded_text_span);
  return pdfium::checked_cast<unsigned long>(encoded_text_span.size());
}

unsigned long GetRawStreamMaybeCopyAndReturnLength(
    RetainPtr<const CPDF_Stream> stream,
    pdfium::span<uint8_t> buffer) {
  return GetStreamMaybeCopyAndReturnLengthImpl(std::move(stream), buffer,
                                               /*decode=*/false);
}

unsigned long DecodeStreamMaybeCopyAndReturnLength(
    RetainPtr<const CPDF_Stream> stream,
    pdfium::span<uint8_t> buffer) {
  return GetStreamMaybeCopyAndReturnLengthImpl(std::move(stream), buffer,
                                               /*decode=*/true);
}

void SetPDFSandboxPolicy(FPDF_DWORD policy, FPDF_BOOL enable) {
  switch (policy) {
    case FPDF_POLICY_MACHINETIME_ACCESS: {
      uint32_t mask = 1 << policy;
      if (enable)
        g_sandbox_policy |= mask;
      else
        g_sandbox_policy &= ~mask;
    } break;
    default:
      break;
  }
}

FPDF_BOOL IsPDFSandboxPolicyEnabled(FPDF_DWORD policy) {
  switch (policy) {
    case FPDF_POLICY_MACHINETIME_ACCESS: {
      uint32_t mask = 1 << policy;
      return !!(g_sandbox_policy & mask);
    }
    default:
      return false;
  }
}

void SetPDFUnsupportInfo(UNSUPPORT_INFO* unsp_info) {
  g_unsupport_info = unsp_info;
}

void ReportUnsupportedFeatures(const CPDF_Document* pDoc) {
  const CPDF_Dictionary* pRootDict = pDoc->GetRoot();
  if (!pRootDict)
    return;

  // Portfolios and Packages
  if (pRootDict->KeyExist("Collection"))
    RaiseUnsupportedError(FPDF_UNSP_DOC_PORTABLECOLLECTION);

  RetainPtr<const CPDF_Dictionary> pNameDict = pRootDict->GetDictFor("Names");
  if (pNameDict) {
    if (pNameDict->KeyExist("EmbeddedFiles"))
      RaiseUnsupportedError(FPDF_UNSP_DOC_ATTACHMENT);

    RetainPtr<const CPDF_Dictionary> pJSDict =
        pNameDict->GetDictFor("JavaScript");
    if (pJSDict) {
      RetainPtr<const CPDF_Array> pArray = pJSDict->GetArrayFor("Names");
      if (pArray) {
        for (size_t i = 0; i < pArray->size(); i++) {
          ByteString cbStr = pArray->GetByteStringAt(i);
          if (cbStr == "com.adobe.acrobat.SharedReview.Register") {
            RaiseUnsupportedError(FPDF_UNSP_DOC_SHAREDREVIEW);
            break;
          }
        }
      }
    }
  }

  // SharedForm
  RetainPtr<const CPDF_Stream> pStream = pRootDict->GetStreamFor("Metadata");
  if (pStream) {
    CPDF_Metadata metadata(std::move(pStream));
    for (const UnsupportedFeature& feature : metadata.CheckForSharedForm())
      RaiseUnsupportedError(static_cast<int>(feature));
  }
}

void ReportUnsupportedXFA(const CPDF_Document* pDoc) {
  if (!pDoc->GetExtension() && DocHasXFA(pDoc))
    RaiseUnsupportedError(FPDF_UNSP_DOC_XFAFORM);
}

void CheckForUnsupportedAnnot(const CPDF_Annot* pAnnot) {
  switch (pAnnot->GetSubtype()) {
    case CPDF_Annot::Subtype::FILEATTACHMENT:
      RaiseUnsupportedError(FPDF_UNSP_ANNOT_ATTACHMENT);
      break;
    case CPDF_Annot::Subtype::MOVIE:
      RaiseUnsupportedError(FPDF_UNSP_ANNOT_MOVIE);
      break;
    case CPDF_Annot::Subtype::RICHMEDIA:
      RaiseUnsupportedError(FPDF_UNSP_ANNOT_SCREEN_RICHMEDIA);
      break;
    case CPDF_Annot::Subtype::SCREEN: {
      const CPDF_Dictionary* pAnnotDict = pAnnot->GetAnnotDict();
      ByteString cbString = pAnnotDict->GetByteStringFor("IT");
      if (cbString != "Img")
        RaiseUnsupportedError(FPDF_UNSP_ANNOT_SCREEN_MEDIA);
      break;
    }
    case CPDF_Annot::Subtype::SOUND:
      RaiseUnsupportedError(FPDF_UNSP_ANNOT_SOUND);
      break;
    case CPDF_Annot::Subtype::THREED:
      RaiseUnsupportedError(FPDF_UNSP_ANNOT_3DANNOT);
      break;
    case CPDF_Annot::Subtype::WIDGET: {
      const CPDF_Dictionary* pAnnotDict = pAnnot->GetAnnotDict();
      ByteString cbString =
          pAnnotDict->GetByteStringFor(pdfium::form_fields::kFT);
      if (cbString == pdfium::form_fields::kSig)
        RaiseUnsupportedError(FPDF_UNSP_ANNOT_SIG);
      break;
    }
    default:
      break;
  }
}

void ProcessParseError(CPDF_Parser::Error err) {
  uint32_t err_code = FPDF_ERR_SUCCESS;
  // Translate FPDFAPI error code to FPDFVIEW error code
  switch (err) {
    case CPDF_Parser::SUCCESS:
      err_code = FPDF_ERR_SUCCESS;
      break;
    case CPDF_Parser::FILE_ERROR:
      err_code = FPDF_ERR_FILE;
      break;
    case CPDF_Parser::FORMAT_ERROR:
      err_code = FPDF_ERR_FORMAT;
      break;
    case CPDF_Parser::PASSWORD_ERROR:
      err_code = FPDF_ERR_PASSWORD;
      break;
    case CPDF_Parser::HANDLER_ERROR:
      err_code = FPDF_ERR_SECURITY;
      break;
  }
  FXSYS_SetLastError(err_code);
}

void SetColorFromScheme(const FPDF_COLORSCHEME* pColorScheme,
                        CPDF_RenderOptions* pRenderOptions) {
  CPDF_RenderOptions::ColorScheme color_scheme;
  color_scheme.path_fill_color =
      static_cast<FX_ARGB>(pColorScheme->path_fill_color);
  color_scheme.path_stroke_color =
      static_cast<FX_ARGB>(pColorScheme->path_stroke_color);
  color_scheme.text_fill_color =
      static_cast<FX_ARGB>(pColorScheme->text_fill_color);
  color_scheme.text_stroke_color =
      static_cast<FX_ARGB>(pColorScheme->text_stroke_color);
  pRenderOptions->SetColorScheme(color_scheme);
}

std::vector<uint32_t> ParsePageRangeString(const ByteString& bsPageRange,
                                           uint32_t nCount) {
  ByteStringView alphabet(" 0123456789-,");
  for (const auto& ch : bsPageRange) {
    if (!alphabet.Contains(ch))
      return std::vector<uint32_t>();
  }

  ByteString bsStrippedPageRange = bsPageRange;
  bsStrippedPageRange.Remove(' ');

  std::vector<uint32_t> results;
  for (const auto& entry : fxcrt::Split(bsStrippedPageRange, ',')) {
    std::vector<ByteString> args = fxcrt::Split(entry, '-');
    if (args.size() == 1) {
      uint32_t page_num = pdfium::checked_cast<uint32_t>(atoi(args[0].c_str()));
      if (page_num == 0 || page_num > nCount)
        return std::vector<uint32_t>();
      results.push_back(page_num - 1);
    } else if (args.size() == 2) {
      uint32_t first_num =
          pdfium::checked_cast<uint32_t>(atoi(args[0].c_str()));
      if (first_num == 0)
        return std::vector<uint32_t>();
      uint32_t last_num = pdfium::checked_cast<uint32_t>(atoi(args[1].c_str()));
      if (last_num == 0 || first_num > last_num || last_num > nCount)
        return std::vector<uint32_t>();
      for (uint32_t i = first_num; i <= last_num; ++i)
        results.push_back(i - 1);
    } else {
      return std::vector<uint32_t>();
    }
  }
  return results;
}
