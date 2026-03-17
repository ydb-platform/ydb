// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "public/fpdf_signature.h"

#include <utility>
#include <vector>

#include "constants/form_fields.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "fpdfsdk/cpdfsdk_helpers.h"

namespace {

std::vector<RetainPtr<const CPDF_Dictionary>> CollectSignatures(
    CPDF_Document* doc) {
  std::vector<RetainPtr<const CPDF_Dictionary>> signatures;
  const CPDF_Dictionary* root = doc->GetRoot();
  if (!root)
    return signatures;

  RetainPtr<const CPDF_Dictionary> acro_form = root->GetDictFor("AcroForm");
  if (!acro_form)
    return signatures;

  RetainPtr<const CPDF_Array> fields = acro_form->GetArrayFor("Fields");
  if (!fields)
    return signatures;

  CPDF_ArrayLocker locker(std::move(fields));
  for (auto& field : locker) {
    RetainPtr<const CPDF_Dictionary> field_dict = field->GetDict();
    if (field_dict && field_dict->GetNameFor(pdfium::form_fields::kFT) ==
                          pdfium::form_fields::kSig) {
      signatures.push_back(std::move(field_dict));
    }
  }
  return signatures;
}

}  // namespace

FPDF_EXPORT int FPDF_CALLCONV FPDF_GetSignatureCount(FPDF_DOCUMENT document) {
  auto* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc)
    return -1;

  return fxcrt::CollectionSize<int>(CollectSignatures(doc));
}

FPDF_EXPORT FPDF_SIGNATURE FPDF_CALLCONV
FPDF_GetSignatureObject(FPDF_DOCUMENT document, int index) {
  auto* doc = CPDFDocumentFromFPDFDocument(document);
  if (!doc)
    return nullptr;

  std::vector<RetainPtr<const CPDF_Dictionary>> signatures =
      CollectSignatures(doc);
  if (!fxcrt::IndexInBounds(signatures, index))
    return nullptr;

  return FPDFSignatureFromCPDFDictionary(signatures[index].Get());
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFSignatureObj_GetContents(FPDF_SIGNATURE signature,
                             void* buffer,
                             unsigned long length) {
  const CPDF_Dictionary* signature_dict =
      CPDFDictionaryFromFPDFSignature(signature);
  if (!signature_dict) {
    return 0;
  }
  RetainPtr<const CPDF_Dictionary> value_dict =
      signature_dict->GetDictFor(pdfium::form_fields::kV);
  if (!value_dict) {
    return 0;
  }
  // SAFETY: required from caller.
  auto result_span = UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, length));
  ByteString contents = value_dict->GetByteStringFor("Contents");
  fxcrt::try_spancpy(result_span, contents.span());
  return pdfium::checked_cast<unsigned long>(contents.span().size());
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFSignatureObj_GetByteRange(FPDF_SIGNATURE signature,
                              int* buffer,
                              unsigned long length) {
  const CPDF_Dictionary* signature_dict =
      CPDFDictionaryFromFPDFSignature(signature);
  if (!signature_dict)
    return 0;

  RetainPtr<const CPDF_Dictionary> value_dict =
      signature_dict->GetDictFor(pdfium::form_fields::kV);
  if (!value_dict)
    return 0;

  RetainPtr<const CPDF_Array> byte_range = value_dict->GetArrayFor("ByteRange");
  if (!byte_range)
    return 0;

  const unsigned long byte_range_len =
      fxcrt::CollectionSize<unsigned long>(*byte_range);
  if (buffer && length >= byte_range_len) {
    // SAFETY: required from caller.
    auto buffer_span = UNSAFE_BUFFERS(pdfium::make_span(buffer, length));
    for (size_t i = 0; i < byte_range_len; ++i) {
      buffer_span[i] = byte_range->GetIntegerAt(i);
    }
  }
  return byte_range_len;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFSignatureObj_GetSubFilter(FPDF_SIGNATURE signature,
                              char* buffer,
                              unsigned long length) {
  const CPDF_Dictionary* signature_dict =
      CPDFDictionaryFromFPDFSignature(signature);
  if (!signature_dict)
    return 0;

  RetainPtr<const CPDF_Dictionary> value_dict =
      signature_dict->GetDictFor(pdfium::form_fields::kV);
  if (!value_dict || !value_dict->KeyExist("SubFilter"))
    return 0;

  ByteString sub_filter = value_dict->GetNameFor("SubFilter");

  // SAFETY: required from caller.
  return NulTerminateMaybeCopyAndReturnLength(
      sub_filter, UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, length)));
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFSignatureObj_GetReason(FPDF_SIGNATURE signature,
                           void* buffer,
                           unsigned long length) {
  const CPDF_Dictionary* signature_dict =
      CPDFDictionaryFromFPDFSignature(signature);
  if (!signature_dict)
    return 0;

  RetainPtr<const CPDF_Dictionary> value_dict =
      signature_dict->GetDictFor(pdfium::form_fields::kV);
  if (!value_dict)
    return 0;

  RetainPtr<const CPDF_Object> obj = value_dict->GetObjectFor("Reason");
  if (!obj || !obj->IsString())
    return 0;

  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      obj->GetUnicodeText(),
      UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, length)));
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFSignatureObj_GetTime(FPDF_SIGNATURE signature,
                         char* buffer,
                         unsigned long length) {
  const CPDF_Dictionary* signature_dict =
      CPDFDictionaryFromFPDFSignature(signature);
  if (!signature_dict)
    return 0;

  RetainPtr<const CPDF_Dictionary> value_dict =
      signature_dict->GetDictFor(pdfium::form_fields::kV);
  if (!value_dict)
    return 0;

  RetainPtr<const CPDF_Object> obj = value_dict->GetObjectFor("M");
  if (!obj || !obj->IsString())
    return 0;

  // SAFETY: required from caller.
  return NulTerminateMaybeCopyAndReturnLength(
      obj->GetString(), UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, length)));
}

FPDF_EXPORT unsigned int FPDF_CALLCONV
FPDFSignatureObj_GetDocMDPPermission(FPDF_SIGNATURE signature) {
  int permission = 0;
  const CPDF_Dictionary* signature_dict =
      CPDFDictionaryFromFPDFSignature(signature);
  if (!signature_dict)
    return permission;

  RetainPtr<const CPDF_Dictionary> value_dict =
      signature_dict->GetDictFor(pdfium::form_fields::kV);
  if (!value_dict)
    return permission;

  RetainPtr<const CPDF_Array> references = value_dict->GetArrayFor("Reference");
  if (!references)
    return permission;

  CPDF_ArrayLocker locker(std::move(references));
  for (auto& reference : locker) {
    RetainPtr<const CPDF_Dictionary> reference_dict = reference->GetDict();
    if (!reference_dict)
      continue;

    ByteString transform_method = reference_dict->GetNameFor("TransformMethod");
    if (transform_method != "DocMDP")
      continue;

    RetainPtr<const CPDF_Dictionary> transform_params =
        reference_dict->GetDictFor("TransformParams");
    if (!transform_params)
      continue;

    // Valid values are 1, 2 and 3; 2 is the default.
    permission = transform_params->GetIntegerFor("P", 2);
    if (permission < 1 || permission > 3)
      permission = 0;

    return permission;
  }

  return permission;
}
