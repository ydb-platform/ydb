// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_FLATEENCODER_H_
#define CORE_FPDFAPI_PARSER_CPDF_FLATEENCODER_H_

#include <stdint.h>

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include <absl/types/variant.h>

class CPDF_Dictionary;
class CPDF_Encryptor;
class CPDF_Stream;
class CPDF_StreamAcc;
class IFX_ArchiveStream;

class CPDF_FlateEncoder {
 public:
  CPDF_FlateEncoder(RetainPtr<const CPDF_Stream> pStream, bool bFlateEncode);
  ~CPDF_FlateEncoder();

  void UpdateLength(size_t size);
  bool WriteDictTo(IFX_ArchiveStream* archive,
                   const CPDF_Encryptor* encryptor) const;

  pdfium::span<const uint8_t> GetSpan() const;

 private:
  bool is_owned() const {
    return absl::holds_alternative<DataVector<uint8_t>>(m_Data);
  }

  // Returns |m_pClonedDict| if it is valid. Otherwise returns |m_pDict|.
  const CPDF_Dictionary* GetDict() const;

  // Must outlive `m_Data`.
  RetainPtr<CPDF_StreamAcc> const m_pAcc;

  absl::variant<pdfium::raw_span<const uint8_t>, DataVector<uint8_t>> m_Data;

  // Only one of these two pointers is valid at any time.
  RetainPtr<const CPDF_Dictionary> m_pDict;
  RetainPtr<CPDF_Dictionary> m_pClonedDict;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_FLATEENCODER_H_
