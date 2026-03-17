// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JPX_CJPX_DECODER_H_
#define CORE_FXCODEC_JPX_CJPX_DECODER_H_

#include <stdint.h>

#include <memory>

#include "core/fxcrt/raw_span.h"
#include "core/fxcrt/span.h"

#include <openjpeg.h>

namespace fxcodec {

struct DecodeData;

class CJPX_Decoder {
 public:
  // Calculated as log2(2^32 / 1), where 2^32 is the largest image dimension and
  // 1 is the smallest required size.
  static constexpr uint8_t kMaxResolutionsToSkip = 32;

  enum class ColorSpaceOption {
    kNone,
    kNormal,
    kIndexed,
  };

  struct JpxImageInfo {
    uint32_t width;
    uint32_t height;
    uint32_t channels;
    COLOR_SPACE colorspace;
  };

  static std::unique_ptr<CJPX_Decoder> Create(
      pdfium::span<const uint8_t> src_span,
      CJPX_Decoder::ColorSpaceOption option,
      uint8_t resolution_levels_to_skip,
      bool strict_mode);

  static void Sycc420ToRgbForTesting(opj_image_t* img);

  ~CJPX_Decoder();

  JpxImageInfo GetInfo() const;
  bool StartDecode();

  // `swap_rgb` can only be set when an image's color space type contains at
  // least 3 color components. Note that this `component_count` is not
  // equivalent to `JpxImageInfo::channels`. The JpxImageInfo channels can
  // contain extra information for rendering the image besides the color
  // component information. Therefore the `JpxImageInfo::channels` must be no
  // less than the component count.
  //
  // Example: If a JPX image's color space type is OPJ_CLRSPC_SRGB, the
  // component count for this color space is 3, and the channel count of its
  // JpxImageInfo can be 4. This is because the extra channel might contain
  // extra information, such as the transparency level of the image.
  bool Decode(pdfium::span<uint8_t> dest_buf,
              uint32_t pitch,
              bool swap_rgb,
              uint32_t component_count);

 private:
  struct CodecDeleter {
    inline void operator()(opj_codec_t* ptr) const { opj_destroy_codec(ptr); }
  };

  struct ImageDeleter {
    inline void operator()(opj_image_t* ptr) const { opj_image_destroy(ptr); }
  };

  struct StreamDeleter {
    inline void operator()(opj_stream_t* ptr) const { opj_stream_destroy(ptr); }
  };

  // Use Create() to instantiate.
  explicit CJPX_Decoder(ColorSpaceOption option);

  // TODO(crbug.com/42270564): Remove `strict_mode` once all the bugs have been
  // worked out in OpenJPEG.
  bool Init(pdfium::span<const uint8_t> src_data,
            uint8_t resolution_levels_to_skip,
            bool strict_mode);

  const ColorSpaceOption m_ColorSpaceOption;
  pdfium::raw_span<const uint8_t> m_SrcData;
  std::unique_ptr<DecodeData> m_DecodeData;
  std::unique_ptr<opj_codec_t, CodecDeleter> m_Codec;
  std::unique_ptr<opj_stream_t, StreamDeleter> m_Stream;
  std::unique_ptr<opj_image_t, ImageDeleter> m_Image;
  opj_dparameters_t m_Parameters = {};
};

}  // namespace fxcodec

using fxcodec::CJPX_Decoder;

#endif  // CORE_FXCODEC_JPX_CJPX_DECODER_H_
