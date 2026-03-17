// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "public/fpdf_attachment.h"

#include <limits.h>

#include <array>
#include <memory>
#include <utility>

#include "constants/stream_dict_common.h"
#include "core/fdrm/fx_crypt.h"
#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_reference.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fpdfdoc/cpdf_filespec.h"
#include "core/fpdfdoc/cpdf_nametree.h"
#include "core/fxcodec/data_and_bytes_consumed.h"
#include "core/fxcrt/cfx_datetime.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_extension.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "fpdfsdk/cpdfsdk_helpers.h"

namespace {

constexpr char kChecksumKey[] = "CheckSum";

}  // namespace

FPDF_EXPORT int FPDF_CALLCONV
FPDFDoc_GetAttachmentCount(FPDF_DOCUMENT document) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return 0;

  auto name_tree = CPDF_NameTree::Create(pDoc, "EmbeddedFiles");
  return name_tree ? pdfium::checked_cast<int>(name_tree->GetCount()) : 0;
}

FPDF_EXPORT FPDF_ATTACHMENT FPDF_CALLCONV
FPDFDoc_AddAttachment(FPDF_DOCUMENT document, FPDF_WIDESTRING name) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc)
    return nullptr;

  // SAFETY: required from caller.
  WideString wsName = UNSAFE_BUFFERS(WideStringFromFPDFWideString(name));
  if (wsName.IsEmpty())
    return nullptr;

  auto name_tree =
      CPDF_NameTree::CreateWithRootNameArray(pDoc, "EmbeddedFiles");
  if (!name_tree)
    return nullptr;

  // Set up the basic entries in the filespec dictionary.
  auto pFile = pDoc->NewIndirect<CPDF_Dictionary>();
  pFile->SetNewFor<CPDF_Name>("Type", "Filespec");
  pFile->SetNewFor<CPDF_String>("UF", wsName.AsStringView());
  pFile->SetNewFor<CPDF_String>(pdfium::stream::kF, wsName.AsStringView());

  // Add the new attachment name and filespec into the document's EmbeddedFiles.
  if (!name_tree->AddValueAndName(pFile->MakeReference(pDoc), wsName))
    return nullptr;

  // Unretained reference in public API. NOLINTNEXTLINE
  return FPDFAttachmentFromCPDFObject(pFile);
}

FPDF_EXPORT FPDF_ATTACHMENT FPDF_CALLCONV
FPDFDoc_GetAttachment(FPDF_DOCUMENT document, int index) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc || index < 0)
    return nullptr;

  auto name_tree = CPDF_NameTree::Create(pDoc, "EmbeddedFiles");
  if (!name_tree || static_cast<size_t>(index) >= name_tree->GetCount())
    return nullptr;

  WideString csName;

  // Unretained reference in public API. NOLINTNEXTLINE
  return FPDFAttachmentFromCPDFObject(
      name_tree->LookupValueAndName(index, &csName));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFDoc_DeleteAttachment(FPDF_DOCUMENT document, int index) {
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pDoc || index < 0)
    return false;

  auto name_tree = CPDF_NameTree::Create(pDoc, "EmbeddedFiles");
  if (!name_tree || static_cast<size_t>(index) >= name_tree->GetCount())
    return false;

  return name_tree->DeleteValueAndName(index);
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAttachment_GetName(FPDF_ATTACHMENT attachment,
                       FPDF_WCHAR* buffer,
                       unsigned long buflen) {
  CPDF_Object* pFile = CPDFObjectFromFPDFAttachment(attachment);
  if (!pFile)
    return 0;

  CPDF_FileSpec spec(pdfium::WrapRetain(pFile));
  // SAFETY: required from caller.
  return Utf16EncodeMaybeCopyAndReturnLength(
      spec.GetFileName(), UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen)));
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAttachment_HasKey(FPDF_ATTACHMENT attachment, FPDF_BYTESTRING key) {
  CPDF_Object* pFile = CPDFObjectFromFPDFAttachment(attachment);
  if (!pFile)
    return 0;

  CPDF_FileSpec spec(pdfium::WrapRetain(pFile));
  RetainPtr<const CPDF_Dictionary> pParamsDict = spec.GetParamsDict();
  return pParamsDict ? pParamsDict->KeyExist(key) : 0;
}

FPDF_EXPORT FPDF_OBJECT_TYPE FPDF_CALLCONV
FPDFAttachment_GetValueType(FPDF_ATTACHMENT attachment, FPDF_BYTESTRING key) {
  if (!FPDFAttachment_HasKey(attachment, key))
    return FPDF_OBJECT_UNKNOWN;

  CPDF_FileSpec spec(
      pdfium::WrapRetain(CPDFObjectFromFPDFAttachment(attachment)));
  RetainPtr<const CPDF_Object> pObj = spec.GetParamsDict()->GetObjectFor(key);
  return pObj ? pObj->GetType() : FPDF_OBJECT_UNKNOWN;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAttachment_SetStringValue(FPDF_ATTACHMENT attachment,
                              FPDF_BYTESTRING key,
                              FPDF_WIDESTRING value) {
  CPDF_Object* pFile = CPDFObjectFromFPDFAttachment(attachment);
  if (!pFile)
    return false;

  CPDF_FileSpec spec(pdfium::WrapRetain(pFile));
  RetainPtr<CPDF_Dictionary> pParamsDict = spec.GetMutableParamsDict();
  if (!pParamsDict)
    return false;

  // SAFETY: required from caller.
  ByteString bsValue = UNSAFE_BUFFERS(ByteStringFromFPDFWideString(value));
  ByteString bsKey = key;
  if (bsKey == kChecksumKey) {
    pParamsDict->SetNewFor<CPDF_String>(bsKey,
                                        HexDecode(bsValue.unsigned_span()).data,
                                        CPDF_String::DataType::kIsHex);
  } else {
    pParamsDict->SetNewFor<CPDF_String>(bsKey, bsValue);
  }
  return true;
}

FPDF_EXPORT unsigned long FPDF_CALLCONV
FPDFAttachment_GetStringValue(FPDF_ATTACHMENT attachment,
                              FPDF_BYTESTRING key,
                              FPDF_WCHAR* buffer,
                              unsigned long buflen) {
  CPDF_Object* file = CPDFObjectFromFPDFAttachment(attachment);
  if (!file) {
    return 0;
  }

  CPDF_FileSpec spec(pdfium::WrapRetain(file));
  RetainPtr<const CPDF_Dictionary> params = spec.GetParamsDict();
  if (!params) {
    return 0;
  }

  // SAFETY: required from caller.
  auto buffer_span = UNSAFE_BUFFERS(SpanFromFPDFApiArgs(buffer, buflen));

  ByteString key_str = key;
  RetainPtr<const CPDF_Object> object = params->GetObjectFor(key_str);
  if (!object || (!object->IsString() && !object->IsName())) {
    // Per API description, return an empty string in these cases.
    return Utf16EncodeMaybeCopyAndReturnLength(WideString(), buffer_span);
  }

  if (key_str == kChecksumKey) {
    RetainPtr<const CPDF_String> string_object = ToString(object);
    if (string_object && string_object->IsHex()) {
      ByteString encoded =
          PDF_HexEncodeString(string_object->GetString().AsStringView());
      return Utf16EncodeMaybeCopyAndReturnLength(
          PDF_DecodeText(encoded.unsigned_span()), buffer_span);
    }
  }

  return Utf16EncodeMaybeCopyAndReturnLength(object->GetUnicodeText(),
                                             buffer_span);
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAttachment_SetFile(FPDF_ATTACHMENT attachment,
                       FPDF_DOCUMENT document,
                       const void* contents,
                       unsigned long len) {
  // An empty content must have a zero length.
  if (!contents && len != 0) {
    return false;
  }

  CPDF_Object* pFile = CPDFObjectFromFPDFAttachment(attachment);
  CPDF_Document* pDoc = CPDFDocumentFromFPDFDocument(document);
  if (!pFile || !pFile->IsDictionary() || !pDoc || len > INT_MAX) {
    return false;
  }

  // Create a dictionary for the new embedded file stream.
  auto pFileStreamDict = pdfium::MakeRetain<CPDF_Dictionary>();
  auto pParamsDict = pFileStreamDict->SetNewFor<CPDF_Dictionary>("Params");

  // Set the size of the new file in the dictionary.
  pFileStreamDict->SetNewFor<CPDF_Number>(pdfium::stream::kDL,
                                          static_cast<int>(len));
  pParamsDict->SetNewFor<CPDF_Number>("Size", static_cast<int>(len));

  // Set the creation date of the new attachment in the dictionary.
  CFX_DateTime dateTime = CFX_DateTime::Now();
  pParamsDict->SetNewFor<CPDF_String>(
      "CreationDate",
      ByteString::Format("D:%d%02d%02d%02d%02d%02d", dateTime.GetYear(),
                         dateTime.GetMonth(), dateTime.GetDay(),
                         dateTime.GetHour(), dateTime.GetMinute(),
                         dateTime.GetSecond()));

  // SAFETY: required from caller.
  pdfium::span<const uint8_t> contents_span = UNSAFE_BUFFERS(
      pdfium::make_span(static_cast<const uint8_t*>(contents), len));

  std::array<uint8_t, 16> digest;
  CRYPT_MD5Generate(contents_span, digest);

  // Set the checksum of the new attachment in the dictionary.
  pParamsDict->SetNewFor<CPDF_String>(kChecksumKey, digest,
                                      CPDF_String::DataType::kIsHex);

  // Create the file stream and have the filespec dictionary link to it.
  auto pFileStream = pDoc->NewIndirect<CPDF_Stream>(
      DataVector<uint8_t>(contents_span.begin(), contents_span.end()),
      std::move(pFileStreamDict));

  auto pEFDict = pFile->AsMutableDictionary()->SetNewFor<CPDF_Dictionary>("EF");
  pEFDict->SetNewFor<CPDF_Reference>("F", pDoc, pFileStream->GetObjNum());
  return true;
}

FPDF_EXPORT FPDF_BOOL FPDF_CALLCONV
FPDFAttachment_GetFile(FPDF_ATTACHMENT attachment,
                       void* buffer,
                       unsigned long buflen,
                       unsigned long* out_buflen) {
  if (!out_buflen)
    return false;

  CPDF_Object* pFile = CPDFObjectFromFPDFAttachment(attachment);
  if (!pFile)
    return false;

  CPDF_FileSpec spec(pdfium::WrapRetain(pFile));
  RetainPtr<const CPDF_Stream> pFileStream = spec.GetFileStream();
  if (!pFileStream)
    return false;

  // SAFETY: required from caller.
  *out_buflen = DecodeStreamMaybeCopyAndReturnLength(
      std::move(pFileStream),
      UNSAFE_BUFFERS(pdfium::make_span(static_cast<uint8_t*>(buffer),
                                       static_cast<size_t>(buflen))));
  return true;
}
