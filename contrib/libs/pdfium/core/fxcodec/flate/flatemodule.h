// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_FLATE_FLATEMODULE_H_
#define CORE_FXCODEC_FLATE_FLATEMODULE_H_

#include <stdint.h>

#include <memory>

#include "core/fxcodec/data_and_bytes_consumed.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/span.h"

namespace fxcodec {

class ScanlineDecoder;

class FlateModule {
 public:
  static std::unique_ptr<ScanlineDecoder> CreateDecoder(
      pdfium::span<const uint8_t> src_span,
      int width,
      int height,
      int nComps,
      int bpc,
      int predictor,
      int Colors,
      int BitsPerComponent,
      int Columns);

  static DataAndBytesConsumed FlateOrLZWDecode(
      bool bLZW,
      pdfium::span<const uint8_t> src_span,
      bool bEarlyChange,
      int predictor,
      int Colors,
      int BitsPerComponent,
      int Columns,
      uint32_t estimated_size);

  static DataVector<uint8_t> Encode(pdfium::span<const uint8_t> src_span);

  FlateModule() = delete;
  FlateModule(const FlateModule&) = delete;
  FlateModule& operator=(const FlateModule&) = delete;
};

}  // namespace fxcodec

using FlateModule = fxcodec::FlateModule;

#endif  // CORE_FXCODEC_FLATE_FLATEMODULE_H_
