// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_STREAM_H_
#define CORE_FPDFAPI_PARSER_CPDF_STREAM_H_

#include <stdint.h>

#include <memory>
#include <set>

#include "core/fpdfapi/parser/cpdf_object.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_string_wrappers.h"
#include "core/fxcrt/retain_ptr.h"
#include <absl/types/variant.h>

class IFX_SeekableReadStream;

class CPDF_Stream final : public CPDF_Object {
 public:
  static constexpr int kFileBufSize = 512;

  CONSTRUCT_VIA_MAKE_RETAIN;

  // CPDF_Object:
  Type GetType() const override;
  RetainPtr<CPDF_Object> Clone() const override;
  WideString GetUnicodeText() const override;
  CPDF_Stream* AsMutableStream() override;
  bool WriteTo(IFX_ArchiveStream* archive,
               const CPDF_Encryptor* encryptor) const override;

  size_t GetRawSize() const;
  // Can only be called when stream is memory-based.
  // This is meant to be used by CPDF_StreamAcc only.
  // Other callers should use CPDF_StreamAcc to access data in all cases.
  pdfium::span<const uint8_t> GetInMemoryRawData() const;

  // Copies span or stream into internally-owned buffer.
  void SetData(pdfium::span<const uint8_t> pData);
  void SetDataFromStringstream(fxcrt::ostringstream* stream);

  void TakeData(DataVector<uint8_t> data);

  // Set data and remove "Filter" and "DecodeParms" fields from stream
  // dictionary. Copies span or stream into internally-owned buffer.
  void SetDataAndRemoveFilter(pdfium::span<const uint8_t> pData);
  void SetDataFromStringstreamAndRemoveFilter(fxcrt::ostringstream* stream);

  void InitStreamFromFile(RetainPtr<IFX_SeekableReadStream> file);

  // Can only be called when a stream is not memory-based.
  DataVector<uint8_t> ReadAllRawData() const;

  bool IsFileBased() const {
    return absl::holds_alternative<RetainPtr<IFX_SeekableReadStream>>(data_);
  }
  bool IsMemoryBased() const {
    return absl::holds_alternative<DataVector<uint8_t>>(data_);
  }
  bool HasFilter() const;

 private:
  friend class CPDF_Dictionary;

  // Initializes with empty data and /Length set to 0 in `dict`.
  // `dict` must be non-null and be a direct object.
  explicit CPDF_Stream(RetainPtr<CPDF_Dictionary> dict);

  // Copies `span` and `stream`, respectively. Creates a new dictionary with the
  // /Length set.
  explicit CPDF_Stream(pdfium::span<const uint8_t> span);
  explicit CPDF_Stream(fxcrt::ostringstream* stream);

  // Reads data from `file`. `dict` will have its /Length set based on `file`.
  // `dict` must be non-null and be a direct object.
  CPDF_Stream(RetainPtr<IFX_SeekableReadStream> file,
              RetainPtr<CPDF_Dictionary> dict);

  // Takes `data`.
  // `dict` must be non-null and be a direct object.
  CPDF_Stream(DataVector<uint8_t> data, RetainPtr<CPDF_Dictionary> dict);
  ~CPDF_Stream() override;

  const CPDF_Dictionary* GetDictInternal() const override;
  RetainPtr<CPDF_Object> CloneNonCyclic(
      bool bDirect,
      std::set<const CPDF_Object*>* pVisited) const override;

  void SetLengthInDict(int length);

  absl::variant<RetainPtr<IFX_SeekableReadStream>, DataVector<uint8_t>> data_;
  RetainPtr<CPDF_Dictionary> dict_;
};

inline CPDF_Stream* ToStream(CPDF_Object* obj) {
  return obj ? obj->AsMutableStream() : nullptr;
}

inline const CPDF_Stream* ToStream(const CPDF_Object* obj) {
  return obj ? obj->AsStream() : nullptr;
}

inline RetainPtr<CPDF_Stream> ToStream(RetainPtr<CPDF_Object> obj) {
  return RetainPtr<CPDF_Stream>(ToStream(obj.Get()));
}

inline RetainPtr<const CPDF_Stream> ToStream(RetainPtr<const CPDF_Object> obj) {
  return RetainPtr<const CPDF_Stream>(ToStream(obj.Get()));
}

#endif  // CORE_FPDFAPI_PARSER_CPDF_STREAM_H_
