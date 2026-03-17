// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "public/fpdf_dataavail.h"

#include <memory>
#include <utility>

#include "core/fpdfapi/page/cpdf_docpagedata.h"
#include "core/fpdfapi/parser/cpdf_data_avail.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/render/cpdf_docrenderdata.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/unowned_ptr.h"
#include "core/fxcrt/unowned_ptr_exclusion.h"
#include "fpdfsdk/cpdfsdk_helpers.h"
#include "public/fpdf_formfill.h"

#ifdef PDF_ENABLE_XFA
#include "fpdfsdk/fpdfxfa/cpdfxfa_context.h"
#endif  // PDF_ENABLE_XFA

// These checks are here because core/ and public/ cannot depend on each other.
static_assert(CPDF_DataAvail::kDataError == PDF_DATA_ERROR,
              "CPDF_DataAvail::kDataError value mismatch");
static_assert(CPDF_DataAvail::kDataNotAvailable == PDF_DATA_NOTAVAIL,
              "CPDF_DataAvail::kDataNotAvailable value mismatch");
static_assert(CPDF_DataAvail::kDataAvailable == PDF_DATA_AVAIL,
              "CPDF_DataAvail::kDataAvailable value mismatch");

static_assert(CPDF_DataAvail::kLinearizationUnknown ==
                  PDF_LINEARIZATION_UNKNOWN,
              "CPDF_DataAvail::kLinearizationUnknown value mismatch");
static_assert(CPDF_DataAvail::kNotLinearized == PDF_NOT_LINEARIZED,
              "CPDF_DataAvail::kNotLinearized value mismatch");
static_assert(CPDF_DataAvail::kLinearized == PDF_LINEARIZED,
              "CPDF_DataAvail::kLinearized value mismatch");

static_assert(CPDF_DataAvail::kFormError == PDF_FORM_ERROR,
              "CPDF_DataAvail::kFormError value mismatch");
static_assert(CPDF_DataAvail::kFormNotAvailable == PDF_FORM_NOTAVAIL,
              "CPDF_DataAvail::kFormNotAvailable value mismatch");
static_assert(CPDF_DataAvail::kFormAvailable == PDF_FORM_AVAIL,
              "CPDF_DataAvail::kFormAvailable value mismatch");
static_assert(CPDF_DataAvail::kFormNotExist == PDF_FORM_NOTEXIST,
              "CPDF_DataAvail::kFormNotExist value mismatch");

namespace {

class FPDF_FileAvailContext final : public CPDF_DataAvail::FileAvail {
 public:
  explicit FPDF_FileAvailContext(FX_FILEAVAIL* avail) : avail_(avail) {}
  ~FPDF_FileAvailContext() override = default;

  // CPDF_DataAvail::FileAvail:
  bool IsDataAvail(FX_FILESIZE offset, size_t size) override {
    return !!avail_->IsDataAvail(avail_, pdfium::checked_cast<size_t>(offset),
                                 size);
  }

 private:
  // TODO(tsepez): fix murky ownership in tests.
  UNOWNED_PTR_EXCLUSION FX_FILEAVAIL* const avail_;
};

class FPDF_FileAccessContext final : public IFX_SeekableReadStream {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  // IFX_SeekableReadStream:
  FX_FILESIZE GetSize() override { return file_->m_FileLen; }

  bool ReadBlockAtOffset(pdfium::span<uint8_t> buffer,
                         FX_FILESIZE offset) override {
    if (buffer.empty() || offset < 0)
      return false;

    if (!pdfium::IsValueInRangeForNumericType<FX_FILESIZE>(buffer.size())) {
      return false;
    }

    FX_SAFE_FILESIZE new_pos = buffer.size();
    new_pos += offset;
    return new_pos.IsValid() && new_pos.ValueOrDie() <= GetSize() &&
           file_->m_GetBlock(
               file_->m_Param, pdfium::checked_cast<unsigned long>(offset),
               buffer.data(),
               pdfium::checked_cast<unsigned long>(buffer.size()));
  }

 private:
  explicit FPDF_FileAccessContext(FPDF_FILEACCESS* file) : file_(file) {}
  ~FPDF_FileAccessContext() override = default;

  // TODO(tsepez): fix murky ownership in tests.
  UNOWNED_PTR_EXCLUSION FPDF_FILEACCESS* const file_;
};

class FPDF_DownloadHintsContext final : public CPDF_DataAvail::DownloadHints {
 public:
  explicit FPDF_DownloadHintsContext(FX_DOWNLOADHINTS* pDownloadHints)
      : m_pDownloadHints(pDownloadHints) {}
  ~FPDF_DownloadHintsContext() override = default;

  // IFX_DownloadHints
  void AddSegment(FX_FILESIZE offset, size_t size) override {
    if (m_pDownloadHints) {
      m_pDownloadHints->AddSegment(m_pDownloadHints,
                                   static_cast<size_t>(offset), size);
    }
  }

 private:
  UnownedPtr<FX_DOWNLOADHINTS> m_pDownloadHints;
};

class FPDF_AvailContext {
 public:
  FPDF_AvailContext(FX_FILEAVAIL* file_avail, FPDF_FILEACCESS* file)
      : file_avail_(std::make_unique<FPDF_FileAvailContext>(file_avail)),
        file_read_(pdfium::MakeRetain<FPDF_FileAccessContext>(file)),
        data_avail_(
            std::make_unique<CPDF_DataAvail>(file_avail_.get(), file_read_)) {}
  ~FPDF_AvailContext() = default;

  CPDF_DataAvail* data_avail() { return data_avail_.get(); }

 private:
  std::unique_ptr<FPDF_FileAvailContext> const file_avail_;
  RetainPtr<FPDF_FileAccessContext> const file_read_;
  std::unique_ptr<CPDF_DataAvail> const data_avail_;
};

FPDF_AvailContext* FPDFAvailContextFromFPDFAvail(FPDF_AVAIL avail) {
  return reinterpret_cast<FPDF_AvailContext*>(avail);
}

FPDF_AVAIL FPDFAvailFromFPDFAvailContext(FPDF_AvailContext* pAvailContext) {
  return reinterpret_cast<FPDF_AVAIL>(pAvailContext);
}

}  // namespace

FPDF_EXPORT FPDF_AVAIL FPDF_CALLCONV FPDFAvail_Create(FX_FILEAVAIL* file_avail,
                                                      FPDF_FILEACCESS* file) {
  auto pAvail = std::make_unique<FPDF_AvailContext>(file_avail, file);

  // Caller takes ownership.
  return FPDFAvailFromFPDFAvailContext(pAvail.release());
}

FPDF_EXPORT void FPDF_CALLCONV FPDFAvail_Destroy(FPDF_AVAIL avail) {
  // Take ownership back from caller and destroy.
  std::unique_ptr<FPDF_AvailContext>(FPDFAvailContextFromFPDFAvail(avail));
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAvail_IsDocAvail(FPDF_AVAIL avail,
                                                   FX_DOWNLOADHINTS* hints) {
  auto* avail_context = FPDFAvailContextFromFPDFAvail(avail);
  if (!avail_context)
    return PDF_DATA_ERROR;
  FPDF_DownloadHintsContext hints_context(hints);
  return avail_context->data_avail()->IsDocAvail(&hints_context);
}

FPDF_EXPORT FPDF_DOCUMENT FPDF_CALLCONV
FPDFAvail_GetDocument(FPDF_AVAIL avail, FPDF_BYTESTRING password) {
  auto* avail_context = FPDFAvailContextFromFPDFAvail(avail);
  if (!avail_context) {
    return nullptr;
  }

  auto [error, document] = avail_context->data_avail()->ParseDocument(
      std::make_unique<CPDF_DocRenderData>(),
      std::make_unique<CPDF_DocPageData>(), password);
  if (error != CPDF_Parser::SUCCESS) {
    ProcessParseError(error);
    return nullptr;
  }

  ReportUnsupportedFeatures(document.get());
  return FPDFDocumentFromCPDFDocument(document.release());
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAvail_GetFirstPageNum(FPDF_DOCUMENT doc) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(doc);
  return pDoc ? pDoc->GetParser()->GetFirstPageNo() : 0;
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAvail_IsPageAvail(FPDF_AVAIL avail,
                                                    int page_index,
                                                    FX_DOWNLOADHINTS* hints) {
  auto* avail_context = FPDFAvailContextFromFPDFAvail(avail);
  if (!avail_context)
    return PDF_DATA_ERROR;
  if (page_index < 0)
    return PDF_DATA_NOTAVAIL;
  FPDF_DownloadHintsContext hints_context(hints);
  return avail_context->data_avail()->IsPageAvail(page_index, &hints_context);
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAvail_IsFormAvail(FPDF_AVAIL avail,
                                                    FX_DOWNLOADHINTS* hints) {
  auto* avail_context = FPDFAvailContextFromFPDFAvail(avail);
  if (!avail_context)
    return PDF_FORM_ERROR;
  FPDF_DownloadHintsContext hints_context(hints);
  return avail_context->data_avail()->IsFormAvail(&hints_context);
}

FPDF_EXPORT int FPDF_CALLCONV FPDFAvail_IsLinearized(FPDF_AVAIL avail) {
  auto* avail_context = FPDFAvailContextFromFPDFAvail(avail);
  if (!avail_context)
    return PDF_LINEARIZATION_UNKNOWN;
  return avail_context->data_avail()->IsLinearizedPDF();
}
