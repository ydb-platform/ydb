// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_flateencoder.h"

#include "constants/stream_dict_common.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_name.h"
#include "core/fpdfapi/parser/cpdf_number.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fxcodec/flate/flatemodule.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/numerics/safe_conversions.h"

CPDF_FlateEncoder::CPDF_FlateEncoder(RetainPtr<const CPDF_Stream> pStream,
                                     bool bFlateEncode)
    : m_pAcc(pdfium::MakeRetain<CPDF_StreamAcc>(pStream)) {
  m_pAcc->LoadAllDataRaw();

  bool bHasFilter = pStream->HasFilter();
  if (bHasFilter && !bFlateEncode) {
    auto pDestAcc = pdfium::MakeRetain<CPDF_StreamAcc>(pStream);
    pDestAcc->LoadAllDataFiltered();

    m_Data = m_pAcc->GetSpan();
    m_pClonedDict = ToDictionary(pStream->GetDict()->Clone());
    m_pClonedDict->RemoveFor("Filter");
    DCHECK(!m_pDict);
    return;
  }
  if (bHasFilter || !bFlateEncode) {
    m_Data = m_pAcc->GetSpan();
    m_pDict = pStream->GetDict();
    DCHECK(!m_pClonedDict);
    return;
  }

  // TODO(thestig): Move to Init() and check for empty return value?
  m_Data = FlateModule::Encode(m_pAcc->GetSpan());
  m_pClonedDict = ToDictionary(pStream->GetDict()->Clone());
  m_pClonedDict->SetNewFor<CPDF_Number>(
      "Length", pdfium::checked_cast<int>(GetSpan().size()));
  m_pClonedDict->SetNewFor<CPDF_Name>("Filter", "FlateDecode");
  m_pClonedDict->RemoveFor(pdfium::stream::kDecodeParms);
  DCHECK(!m_pDict);
}

CPDF_FlateEncoder::~CPDF_FlateEncoder() = default;

void CPDF_FlateEncoder::UpdateLength(size_t size) {
  if (static_cast<size_t>(GetDict()->GetIntegerFor("Length")) == size)
    return;

  if (!m_pClonedDict) {
    m_pClonedDict = ToDictionary(m_pDict->Clone());
    m_pDict.Reset();
  }
  DCHECK(m_pClonedDict);
  DCHECK(!m_pDict);
  m_pClonedDict->SetNewFor<CPDF_Number>("Length", static_cast<int>(size));
}

bool CPDF_FlateEncoder::WriteDictTo(IFX_ArchiveStream* archive,
                                    const CPDF_Encryptor* encryptor) const {
  return GetDict()->WriteTo(archive, encryptor);
}

const CPDF_Dictionary* CPDF_FlateEncoder::GetDict() const {
  if (m_pClonedDict) {
    DCHECK(!m_pDict);
    return m_pClonedDict.Get();
  }
  return m_pDict.Get();
}

pdfium::span<const uint8_t> CPDF_FlateEncoder::GetSpan() const {
  if (is_owned())
    return absl::get<DataVector<uint8_t>>(m_Data);
  return absl::get<pdfium::raw_span<const uint8_t>>(m_Data);
}
