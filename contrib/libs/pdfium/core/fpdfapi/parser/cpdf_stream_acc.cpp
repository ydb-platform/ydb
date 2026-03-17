// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/parser/cpdf_stream_acc.h"

#include <utility>

#include "core/fdrm/fx_crypt.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_stream.h"
#include "core/fpdfapi/parser/fpdf_parser_decode.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/data_vector.h"

CPDF_StreamAcc::CPDF_StreamAcc(RetainPtr<const CPDF_Stream> pStream)
    : m_pStream(std::move(pStream)) {}

CPDF_StreamAcc::~CPDF_StreamAcc() = default;

void CPDF_StreamAcc::LoadAllData(bool bRawAccess,
                                 uint32_t estimated_size,
                                 bool bImageAcc) {
  if (bRawAccess) {
    DCHECK(!estimated_size);
    DCHECK(!bImageAcc);
  }

  if (!m_pStream)
    return;

  bool bProcessRawData = bRawAccess || !m_pStream->HasFilter();
  if (bProcessRawData)
    ProcessRawData();
  else
    ProcessFilteredData(estimated_size, bImageAcc);
}

void CPDF_StreamAcc::LoadAllDataFiltered() {
  LoadAllData(false, 0, false);
}

void CPDF_StreamAcc::LoadAllDataFilteredWithEstimatedSize(
    uint32_t estimated_size) {
  LoadAllData(false, estimated_size, false);
}

void CPDF_StreamAcc::LoadAllDataImageAcc(uint32_t estimated_size) {
  LoadAllData(false, estimated_size, true);
}

void CPDF_StreamAcc::LoadAllDataRaw() {
  LoadAllData(true, 0, false);
}

RetainPtr<const CPDF_Stream> CPDF_StreamAcc::GetStream() const {
  return m_pStream;
}

int CPDF_StreamAcc::GetLength1ForTest() const {
  return m_pStream->GetDict()->GetIntegerFor("Length1");
}

RetainPtr<const CPDF_Dictionary> CPDF_StreamAcc::GetImageParam() const {
  return m_pImageParam;
}

uint32_t CPDF_StreamAcc::GetSize() const {
  return GetSpan().size();
}

pdfium::span<const uint8_t> CPDF_StreamAcc::GetSpan() const {
  if (is_owned())
    return absl::get<DataVector<uint8_t>>(m_Data);
  if (m_pStream && m_pStream->IsMemoryBased())
    return m_pStream->GetInMemoryRawData();
  return {};
}

uint64_t CPDF_StreamAcc::KeyForCache() const {
  return m_pStream ? m_pStream->KeyForCache() : 0;
}

DataVector<uint8_t> CPDF_StreamAcc::ComputeDigest() const {
  return CRYPT_SHA1Generate(GetSpan());
}

DataVector<uint8_t> CPDF_StreamAcc::DetachData() {
  if (is_owned())
    return std::move(absl::get<DataVector<uint8_t>>(m_Data));

  auto span = absl::get<pdfium::raw_span<const uint8_t>>(m_Data);
  return DataVector<uint8_t>(span.begin(), span.end());
}

void CPDF_StreamAcc::ProcessRawData() {
  uint32_t dwSrcSize = m_pStream->GetRawSize();
  if (dwSrcSize == 0)
    return;

  if (m_pStream->IsMemoryBased()) {
    m_Data = m_pStream->GetInMemoryRawData();
    return;
  }

  DataVector<uint8_t> data = ReadRawStream();
  if (data.empty())
    return;

  m_Data = std::move(data);
}

void CPDF_StreamAcc::ProcessFilteredData(uint32_t estimated_size,
                                         bool bImageAcc) {
  uint32_t dwSrcSize = m_pStream->GetRawSize();
  if (dwSrcSize == 0)
    return;

  absl::variant<pdfium::raw_span<const uint8_t>, DataVector<uint8_t>> src_data;
  pdfium::span<const uint8_t> src_span;
  if (m_pStream->IsMemoryBased()) {
    src_span = m_pStream->GetInMemoryRawData();
    src_data = src_span;
  } else {
    DataVector<uint8_t> temp_src_data = ReadRawStream();
    if (temp_src_data.empty())
      return;

    src_span = pdfium::make_span(temp_src_data);
    src_data = std::move(temp_src_data);
  }

  std::optional<DecoderArray> decoder_array =
      GetDecoderArray(m_pStream->GetDict());
  if (!decoder_array.has_value() || decoder_array.value().empty()) {
    m_Data = std::move(src_data);
    return;
  }

  std::optional<PDFDataDecodeResult> result = PDF_DataDecode(
      src_span, estimated_size, bImageAcc, decoder_array.value());
  if (!result.has_value()) {
    m_Data = std::move(src_data);
    return;
  }

  m_ImageDecoder = std::move(result.value().image_encoding);
  m_pImageParam = std::move(result.value().image_params);

  if (result.value().data.empty()) {
    m_Data = std::move(src_data);
    return;
  }

  m_Data = std::move(result.value().data);
}

DataVector<uint8_t> CPDF_StreamAcc::ReadRawStream() const {
  DCHECK(m_pStream);
  DCHECK(m_pStream->IsFileBased());
  return m_pStream->ReadAllRawData();
}
