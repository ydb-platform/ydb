// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_FAX_FAXMODULE_H_
#define CORE_FXCODEC_FAX_FAXMODULE_H_

#include <stdint.h>

#include <memory>

#include "build/build_config.h"
#include "core/fxcrt/span.h"

#if BUILDFLAG(IS_WIN)
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/retain_ptr.h"
#endif

class CFX_DIBBase;

namespace fxcodec {

class ScanlineDecoder;

class FaxModule {
 public:
  static std::unique_ptr<ScanlineDecoder> CreateDecoder(
      pdfium::span<const uint8_t> src_span,
      int width,
      int height,
      int K,
      bool EndOfLine,
      bool EncodedByteAlign,
      bool BlackIs1,
      int Columns,
      int Rows);

  // Return the ending bit position.
  static int FaxG4Decode(pdfium::span<const uint8_t> src_buf,
                         int starting_bitpos,
                         int width,
                         int height,
                         int pitch,
                         uint8_t* dest_buf);

#if BUILDFLAG(IS_WIN)
  // `src` must have a BPP value of 1.
  static DataVector<uint8_t> FaxEncode(RetainPtr<const CFX_DIBBase> src);
#endif  // BUILDFLAG(IS_WIN)

  FaxModule() = delete;
  FaxModule(const FaxModule&) = delete;
  FaxModule& operator=(const FaxModule&) = delete;
};

}  // namespace fxcodec

using FaxModule = fxcodec::FaxModule;

#endif  // CORE_FXCODEC_FAX_FAXMODULE_H_
