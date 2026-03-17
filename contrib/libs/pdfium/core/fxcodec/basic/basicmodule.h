// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_BASIC_BASICMODULE_H_
#define CORE_FXCODEC_BASIC_BASICMODULE_H_

#include <stdint.h>

#include <memory>

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/span.h"

namespace fxcodec {

class ScanlineDecoder;

class BasicModule {
 public:
  static std::unique_ptr<ScanlineDecoder> CreateRunLengthDecoder(
      pdfium::span<const uint8_t> src_buf,
      int width,
      int height,
      int nComps,
      int bpc);

  static DataVector<uint8_t> RunLengthEncode(
      pdfium::span<const uint8_t> src_span);

  static DataVector<uint8_t> A85Encode(pdfium::span<const uint8_t> src_span);

  BasicModule() = delete;
  BasicModule(const BasicModule&) = delete;
  BasicModule& operator=(const BasicModule&) = delete;
};

}  // namespace fxcodec

using BasicModule = fxcodec::BasicModule;

#endif  // CORE_FXCODEC_BASIC_BASICMODULE_H_
