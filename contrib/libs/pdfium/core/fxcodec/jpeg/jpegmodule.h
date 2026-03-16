// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JPEG_JPEGMODULE_H_
#define CORE_FXCODEC_JPEG_JPEGMODULE_H_

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>

#include "build/build_config.h"
#include "core/fxcrt/span.h"

#if BUILDFLAG(IS_WIN)
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/retain_ptr.h"
#endif

class CFX_DIBBase;

namespace fxcodec {

class ScanlineDecoder;

class JpegModule {
 public:
  struct ImageInfo {
    uint32_t width;
    uint32_t height;
    int num_components;
    int bits_per_components;
    bool color_transform;
  };

  static std::unique_ptr<ScanlineDecoder> CreateDecoder(
      pdfium::span<const uint8_t> src_span,
      uint32_t width,
      uint32_t height,
      int nComps,
      bool ColorTransform);

  static std::optional<ImageInfo> LoadInfo(
      pdfium::span<const uint8_t> src_span);

#if BUILDFLAG(IS_WIN)
  UNSAFE_BUFFER_USAGE static bool JpegEncode(
      const RetainPtr<const CFX_DIBBase>& pSource,
      uint8_t** dest_buf,
      size_t* dest_size);
#endif  // BUILDFLAG(IS_WIN)

  JpegModule() = delete;
  JpegModule(const JpegModule&) = delete;
  JpegModule& operator=(const JpegModule&) = delete;
};

}  // namespace fxcodec

using JpegModule = fxcodec::JpegModule;

#endif  // CORE_FXCODEC_JPEG_JPEGMODULE_H_
