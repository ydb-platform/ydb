// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_STREAM_ACC_H_
#define CORE_FPDFAPI_PARSER_CPDF_STREAM_ACC_H_

#include <stdint.h>

#include <memory>

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include <absl/types/variant.h>

class CPDF_Dictionary;
class CPDF_Stream;

class CPDF_StreamAcc final : public Retainable {
 public:
  CONSTRUCT_VIA_MAKE_RETAIN;

  CPDF_StreamAcc(const CPDF_StreamAcc&) = delete;
  CPDF_StreamAcc& operator=(const CPDF_StreamAcc&) = delete;

  void LoadAllDataFiltered();
  void LoadAllDataFilteredWithEstimatedSize(uint32_t estimated_size);
  void LoadAllDataImageAcc(uint32_t estimated_size);
  void LoadAllDataRaw();

  RetainPtr<const CPDF_Stream> GetStream() const;
  RetainPtr<const CPDF_Dictionary> GetImageParam() const;

  uint32_t GetSize() const;
  pdfium::span<const uint8_t> GetSpan() const;
  uint64_t KeyForCache() const;
  DataVector<uint8_t> ComputeDigest() const;
  ByteString GetImageDecoder() const { return m_ImageDecoder; }
  DataVector<uint8_t> DetachData();

  int GetLength1ForTest() const;

 private:
  explicit CPDF_StreamAcc(RetainPtr<const CPDF_Stream> pStream);
  ~CPDF_StreamAcc() override;

  void LoadAllData(bool bRawAccess, uint32_t estimated_size, bool bImageAcc);
  void ProcessRawData();
  void ProcessFilteredData(uint32_t estimated_size, bool bImageAcc);

  // Returns the raw data from `m_pStream`, or no data on failure.
  DataVector<uint8_t> ReadRawStream() const;

  bool is_owned() const {
    return absl::holds_alternative<DataVector<uint8_t>>(m_Data);
  }

  ByteString m_ImageDecoder;
  RetainPtr<const CPDF_Dictionary> m_pImageParam;
  // Needs to outlive `m_Data` when the data is not owned.
  RetainPtr<const CPDF_Stream> const m_pStream;
  absl::variant<pdfium::raw_span<const uint8_t>, DataVector<uint8_t>> m_Data;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_STREAM_ACC_H_
