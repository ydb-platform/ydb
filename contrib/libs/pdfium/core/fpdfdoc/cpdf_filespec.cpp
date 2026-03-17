// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_filespec.h"

#include <array>
#include <iterator>
#include <utility>

#include "build/build_config.h"
#include "constants/stream_dict_common.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/notreached.h"

namespace {

#if BUILDFLAG(IS_APPLE) || BUILDFLAG(IS_WIN)
WideString ChangeSlashToPlatform(WideStringView str) {
  WideString result;
  for (auto wch : str) {
    if (wch == '/') {
#if BUILDFLAG(IS_APPLE)
      result += L':';
#else
      result += L'\\';
#endif
    } else {
      result += wch;
    }
  }
  return result;
}

WideString ChangeSlashToPDF(WideStringView str) {
  WideString result;
  for (auto wch : str) {
    if (wch == '\\' || wch == ':') {
      result += L'/';
    } else {
      result += wch;
    }
  }
  return result;
}
#endif  // BUILDFLAG(IS_APPLE) || BUILDFLAG(IS_WIN)

}  // namespace

CPDF_FileSpec::CPDF_FileSpec(RetainPtr<const CPDF_Object> pObj)
    : m_pObj(std::move(pObj)) {
  DCHECK(m_pObj);
}

CPDF_FileSpec::~CPDF_FileSpec() = default;

WideString CPDF_FileSpec::DecodeFileName(const WideString& filepath) {
  if (filepath.IsEmpty()) {
    return WideString();
  }
#if BUILDFLAG(IS_APPLE)
  WideStringView view = filepath.AsStringView();
  if (view.First(sizeof("/Mac") - 1) == WideStringView(L"/Mac")) {
    return ChangeSlashToPlatform(view.Substr(1));
  }
  return ChangeSlashToPlatform(view);
#elif BUILDFLAG(IS_WIN)
  WideStringView view = filepath.AsStringView();
  if (view[0] != L'/') {
    return ChangeSlashToPlatform(view);
  }
  if (view[1] == L'/') {
    return ChangeSlashToPlatform(view.Substr(1));
  }
  if (view[2] == L'/') {
    WideString result;
    result += view[1];
    result += L':';
    result += ChangeSlashToPlatform(view.Substr(2));
    return result;
  }
  WideString result;
  result += L'\\';
  result += ChangeSlashToPlatform(view);
  return result;
#else
  return filepath;
#endif
}

WideString CPDF_FileSpec::GetFileName() const {
  WideString csFileName;
  if (const CPDF_Dictionary* pDict = m_pObj->AsDictionary()) {
    RetainPtr<const CPDF_String> pUF =
        ToString(pDict->GetDirectObjectFor("UF"));
    if (pUF)
      csFileName = pUF->GetUnicodeText();
    if (csFileName.IsEmpty()) {
      RetainPtr<const CPDF_String> pK =
          ToString(pDict->GetDirectObjectFor(pdfium::stream::kF));
      if (pK)
        csFileName = WideString::FromDefANSI(pK->GetString().AsStringView());
    }
    if (pDict->GetByteStringFor("FS") == "URL")
      return csFileName;

    if (csFileName.IsEmpty()) {
      for (const auto* key : {"DOS", "Mac", "Unix"}) {
        RetainPtr<const CPDF_String> pValue =
            ToString(pDict->GetDirectObjectFor(key));
        if (pValue) {
          csFileName =
              WideString::FromDefANSI(pValue->GetString().AsStringView());
          break;
        }
      }
    }
  } else if (const CPDF_String* pString = m_pObj->AsString()) {
    csFileName = WideString::FromDefANSI(pString->GetString().AsStringView());
  }
  return DecodeFileName(csFileName);
}

RetainPtr<const CPDF_Stream> CPDF_FileSpec::GetFileStream() const {
  const CPDF_Dictionary* pDict = m_pObj->AsDictionary();
  if (!pDict)
    return nullptr;

  // Get the embedded files dictionary.
  RetainPtr<const CPDF_Dictionary> pFiles = pDict->GetDictFor("EF");
  if (!pFiles)
    return nullptr;

  // List of keys to check for the file specification string.
  // Follows the same precedence order as GetFileName().
  static constexpr std::array<const char*, 5> kKeys = {
      {"UF", "F", "DOS", "Mac", "Unix"}};
  size_t end = pDict->GetByteStringFor("FS") == "URL" ? 2 : std::size(kKeys);
  for (size_t i = 0; i < end; ++i) {
    ByteString key = kKeys[i];
    if (!pDict->GetUnicodeTextFor(key).IsEmpty()) {
      RetainPtr<const CPDF_Stream> pStream = pFiles->GetStreamFor(key);
      if (pStream)
        return pStream;
    }
  }
  return nullptr;
}

RetainPtr<const CPDF_Dictionary> CPDF_FileSpec::GetParamsDict() const {
  RetainPtr<const CPDF_Stream> pStream = GetFileStream();
  return pStream ? pStream->GetDict()->GetDictFor("Params") : nullptr;
}

RetainPtr<CPDF_Dictionary> CPDF_FileSpec::GetMutableParamsDict() {
  return pdfium::WrapRetain(
      const_cast<CPDF_Dictionary*>(GetParamsDict().Get()));
}

WideString CPDF_FileSpec::EncodeFileName(const WideString& filepath) {
  if (filepath.IsEmpty()) {
    return WideString();
  }
#if BUILDFLAG(IS_WIN)
  WideStringView view = filepath.AsStringView();
  if (view[1] == L':') {
    WideString result(L'/');
    result += view[0];
    if (view[2] != L'\\') {
      result += L'/';
    }
    result += ChangeSlashToPDF(view.Substr(2));
    return result;
  }
  if (view[0] == L'\\' && view[1] == L'\\') {
    return ChangeSlashToPDF(view.Substr(1));
  }
  if (view[0] == L'\\') {
    return L'/' + ChangeSlashToPDF(view);
  }
  return ChangeSlashToPDF(view);
#elif BUILDFLAG(IS_APPLE)
  WideStringView view = filepath.AsStringView();
  if (view.First(sizeof("Mac") - 1).EqualsASCII("Mac")) {
    return L'/' + ChangeSlashToPDF(view);
  }
  return ChangeSlashToPDF(view);
#else
  return filepath;
#endif
}
