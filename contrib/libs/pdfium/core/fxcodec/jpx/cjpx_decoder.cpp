// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/jpx/cjpx_decoder.h"

#include <string.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "core/fxcodec/jpx/jpx_decode_utils.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/ptr_util.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/calculate_pitch.h"


namespace fxcodec {

namespace {

// Used with std::unique_ptr to call opj_image_data_free on raw memory.
struct OpjImageDataDeleter {
  inline void operator()(void* ptr) const { opj_image_data_free(ptr); }
};

using ScopedOpjImageData = std::unique_ptr<int, OpjImageDataDeleter>;

struct OpjImageRgbData {
  ScopedOpjImageData r;
  ScopedOpjImageData g;
  ScopedOpjImageData b;
};

void fx_ignore_callback(const char* msg, void* client_data) {}

opj_stream_t* fx_opj_stream_create_memory_stream(DecodeData* data) {
  if (!data || !data->src_data || data->src_size <= 0)
    return nullptr;

  opj_stream_t* stream = opj_stream_create(OPJ_J2K_STREAM_CHUNK_SIZE,
                                           /*p_is_input=*/OPJ_TRUE);
  if (!stream)
    return nullptr;

  opj_stream_set_user_data(stream, data, nullptr);
  opj_stream_set_user_data_length(stream, data->src_size);
  opj_stream_set_read_function(stream, opj_read_from_memory);
  opj_stream_set_skip_function(stream, opj_skip_from_memory);
  opj_stream_set_seek_function(stream, opj_seek_from_memory);
  return stream;
}

std::optional<OpjImageRgbData> alloc_rgb(size_t size) {
  OpjImageRgbData data;
  data.r.reset(static_cast<int*>(opj_image_data_alloc(size)));
  if (!data.r)
    return std::nullopt;

  data.g.reset(static_cast<int*>(opj_image_data_alloc(size)));
  if (!data.g)
    return std::nullopt;

  data.b.reset(static_cast<int*>(opj_image_data_alloc(size)));
  if (!data.b)
    return std::nullopt;

  return data;
}

void sycc_to_rgb(int offset,
                 int upb,
                 int y,
                 int cb,
                 int cr,
                 int* out_r,
                 int* out_g,
                 int* out_b) {
  cb -= offset;
  cr -= offset;
  *out_r = std::clamp(y + static_cast<int>(1.402 * cr), 0, upb);
  *out_g = std::clamp(y - static_cast<int>(0.344 * cb + 0.714 * cr), 0, upb);
  *out_b = std::clamp(y + static_cast<int>(1.772 * cb), 0, upb);
}

pdfium::span<opj_image_comp_t> components_span(opj_image_t* img) {
  // SAFETY: required from third-party library.
  return UNSAFE_BUFFERS(pdfium::make_span(img->comps, img->numcomps));
}

void sycc444_to_rgb(opj_image_t* img) {
  auto components = components_span(img);
  int prec = components[0].prec;
  // If we shift 31 we're going to go negative, then things go bad.
  if (prec > 30)
    return;
  int offset = 1 << (prec - 1);
  int upb = (1 << prec) - 1;
  OPJ_UINT32 maxw =
      std::min({components[0].w, components[1].w, components[2].w});
  OPJ_UINT32 maxh =
      std::min({components[0].h, components[1].h, components[2].h});
  FX_SAFE_SIZE_T max_size = maxw;
  max_size *= maxh;
  max_size *= sizeof(int);
  if (!max_size.IsValid())
    return;

  const int* y = components[0].data;
  const int* cb = components[1].data;
  const int* cr = components[2].data;
  if (!y || !cb || !cr)
    return;

  std::optional<OpjImageRgbData> data = alloc_rgb(max_size.ValueOrDie());
  if (!data.has_value())
    return;

  int* r = data.value().r.get();
  int* g = data.value().g.get();
  int* b = data.value().b.get();
  max_size /= sizeof(int);
  for (size_t i = 0; i < max_size.ValueOrDie(); ++i) {
    UNSAFE_TODO(sycc_to_rgb(offset, upb, *y++, *cb++, *cr++, r++, g++, b++));
  }
  opj_image_data_free(components[0].data);
  opj_image_data_free(components[1].data);
  opj_image_data_free(components[2].data);
  components[0].data = data.value().r.release();
  components[1].data = data.value().g.release();
  components[2].data = data.value().b.release();
}

bool sycc420_422_size_is_valid(pdfium::span<opj_image_comp_t> components) {
  return components[0].w != std::numeric_limits<OPJ_UINT32>::max() &&
         (components[0].w + 1) / 2 == components[1].w &&
         components[1].w == components[2].w &&
         components[1].h == components[2].h;
}

bool sycc420_size_is_valid(pdfium::span<opj_image_comp_t> components) {
  return sycc420_422_size_is_valid(components) &&
         components[0].h != std::numeric_limits<OPJ_UINT32>::max() &&
         (components[0].h + 1) / 2 == components[1].h;
}

bool sycc420_must_extend_cbcr(OPJ_UINT32 y, OPJ_UINT32 cbcr) {
  return (y & 1) && (cbcr == y / 2);
}

void sycc420_to_rgb(opj_image_t* img) {
  if (!img) {
    return;
  }
  auto components = components_span(img);
  if (!sycc420_size_is_valid(components)) {
    return;
  }
  OPJ_UINT32 prec = components[0].prec;
  if (!prec)
    return;

  OPJ_UINT32 offset = 1 << (prec - 1);
  OPJ_UINT32 upb = (1 << prec) - 1;
  OPJ_UINT32 yw = components[0].w;
  OPJ_UINT32 yh = components[0].h;
  OPJ_UINT32 cbw = components[1].w;
  OPJ_UINT32 cbh = components[1].h;
  OPJ_UINT32 crw = components[2].w;
  bool extw = sycc420_must_extend_cbcr(yw, cbw);
  bool exth = sycc420_must_extend_cbcr(yh, cbh);
  FX_SAFE_UINT32 safe_size = yw;
  safe_size *= yh;
  safe_size *= sizeof(int);
  if (!safe_size.IsValid())
    return;

  const int* y = components[0].data;
  const int* cb = components[1].data;
  const int* cr = components[2].data;
  if (!y || !cb || !cr)
    return;

  std::optional<OpjImageRgbData> data = alloc_rgb(safe_size.ValueOrDie());
  if (!data.has_value())
    return;

  int* r = data.value().r.get();
  int* g = data.value().g.get();
  int* b = data.value().b.get();
  const int* ny = nullptr;
  int* nr = nullptr;
  int* ng = nullptr;
  int* nb = nullptr;
  OPJ_UINT32 i = 0;
  OPJ_UINT32 j = 0;
  UNSAFE_TODO({
    for (i = 0; i < (yh & ~(OPJ_UINT32)1); i += 2) {
      ny = y + yw;
      nr = r + yw;
      ng = g + yw;
      nb = b + yw;
      for (j = 0; j < (yw & ~(OPJ_UINT32)1); j += 2) {
        sycc_to_rgb(offset, upb, *y, *cb, *cr, r, g, b);
        ++y;
        ++r;
        ++g;
        ++b;
        sycc_to_rgb(offset, upb, *y, *cb, *cr, r, g, b);
        ++y;
        ++r;
        ++g;
        ++b;
        sycc_to_rgb(offset, upb, *ny, *cb, *cr, nr, ng, nb);
        ++ny;
        ++nr;
        ++ng;
        ++nb;
        sycc_to_rgb(offset, upb, *ny, *cb, *cr, nr, ng, nb);
        ++ny;
        ++nr;
        ++ng;
        ++nb;
        ++cb;
        ++cr;
      }
      if (j < yw) {
        if (extw) {
          --cb;
          --cr;
        }
        sycc_to_rgb(offset, upb, *y, *cb, *cr, r, g, b);
        ++y;
        ++r;
        ++g;
        ++b;
        sycc_to_rgb(offset, upb, *ny, *cb, *cr, nr, ng, nb);
        ++ny;
        ++nr;
        ++ng;
        ++nb;
        ++cb;
        ++cr;
      }
      y += yw;
      r += yw;
      g += yw;
      b += yw;
    }
    if (i < yh) {
      if (exth) {
        cb -= cbw;
        cr -= crw;
      }
      for (j = 0; j < (yw & ~(OPJ_UINT32)1); j += 2) {
        sycc_to_rgb(offset, upb, *y, *cb, *cr, r, g, b);
        ++y;
        ++r;
        ++g;
        ++b;
        sycc_to_rgb(offset, upb, *y, *cb, *cr, r, g, b);
        ++y;
        ++r;
        ++g;
        ++b;
        ++cb;
        ++cr;
      }
      if (j < yw) {
        if (extw) {
          --cb;
          --cr;
        }
        sycc_to_rgb(offset, upb, *y, *cb, *cr, r, g, b);
      }
    }
  });
  opj_image_data_free(components[0].data);
  opj_image_data_free(components[1].data);
  opj_image_data_free(components[2].data);
  components[0].data = data.value().r.release();
  components[1].data = data.value().g.release();
  components[2].data = data.value().b.release();
  components[1].w = yw;
  components[1].h = yh;
  components[2].w = yw;
  components[2].h = yh;
  components[1].dx = components[0].dx;
  components[2].dx = components[0].dx;
  components[1].dy = components[0].dy;
  components[2].dy = components[0].dy;
}

bool sycc422_size_is_valid(pdfium::span<opj_image_comp_t> components) {
  return sycc420_422_size_is_valid(components) &&
         components[0].h == components[1].h;
}

void sycc422_to_rgb(opj_image_t* img) {
  if (!img) {
    return;
  }
  auto components = components_span(img);
  if (!sycc422_size_is_valid(components)) {
    return;
  }
  int prec = components[0].prec;
  if (prec <= 0 || prec >= 32)
    return;

  int offset = 1 << (prec - 1);
  int upb = (1 << prec) - 1;
  OPJ_UINT32 maxw = components[0].w;
  OPJ_UINT32 maxh = components[0].h;
  FX_SAFE_SIZE_T max_size = maxw;
  max_size *= maxh;
  max_size *= sizeof(int);
  if (!max_size.IsValid())
    return;

  const int* y = components[0].data;
  const int* cb = components[1].data;
  const int* cr = components[2].data;
  if (!y || !cb || !cr)
    return;

  std::optional<OpjImageRgbData> data = alloc_rgb(max_size.ValueOrDie());
  if (!data.has_value())
    return;

  int* r = data.value().r.get();
  int* g = data.value().g.get();
  int* b = data.value().b.get();
  UNSAFE_TODO({
    for (uint32_t i = 0; i < maxh; ++i) {
      OPJ_UINT32 j;
      for (j = 0; j < (maxw & ~static_cast<OPJ_UINT32>(1)); j += 2) {
        sycc_to_rgb(offset, upb, *y++, *cb, *cr, r++, g++, b++);
        sycc_to_rgb(offset, upb, *y++, *cb++, *cr++, r++, g++, b++);
      }
      if (j < maxw) {
        sycc_to_rgb(offset, upb, *y++, *cb++, *cr++, r++, g++, b++);
      }
    }
  });
  opj_image_data_free(components[0].data);
  opj_image_data_free(components[1].data);
  opj_image_data_free(components[2].data);
  components[0].data = data.value().r.release();
  components[1].data = data.value().g.release();
  components[2].data = data.value().b.release();
  components[1].w = maxw;
  components[1].h = maxh;
  components[2].w = maxw;
  components[2].h = maxh;
  components[1].dx = components[0].dx;
  components[2].dx = components[0].dx;
  components[1].dy = components[0].dy;
  components[2].dy = components[0].dy;
}

bool is_sycc420(pdfium::span<opj_image_comp_t> components) {
  return components[0].dx == 1 && components[0].dy == 1 &&
         components[1].dx == 2 && components[1].dy == 2 &&
         components[2].dx == 2 && components[2].dy == 2;
}

bool is_sycc422(pdfium::span<opj_image_comp_t> components) {
  return components[0].dx == 1 && components[0].dy == 1 &&
         components[1].dx == 2 && components[1].dy == 1 &&
         components[2].dx == 2 && components[2].dy == 1;
}

bool is_sycc444(pdfium::span<opj_image_comp_t> components) {
  return components[0].dx == 1 && components[0].dy == 1 &&
         components[1].dx == 1 && components[1].dy == 1 &&
         components[2].dx == 1 && components[2].dy == 1;
}

void color_sycc_to_rgb(opj_image_t* img) {
  auto components = components_span(img);
  if (components.size() < 3) {
    img->color_space = OPJ_CLRSPC_GRAY;
    return;
  }
  if (is_sycc420(components)) {
    sycc420_to_rgb(img);
  } else if (is_sycc422(components)) {
    sycc422_to_rgb(img);
  } else if (is_sycc444(components)) {
    sycc444_to_rgb(img);
  } else {
    return;
  }
  img->color_space = OPJ_CLRSPC_SRGB;
}

}  // namespace

// static
std::unique_ptr<CJPX_Decoder> CJPX_Decoder::Create(
    pdfium::span<const uint8_t> src_span,
    CJPX_Decoder::ColorSpaceOption option,
    uint8_t resolution_levels_to_skip,
    bool strict_mode) {
  // Private ctor.
  auto decoder = pdfium::WrapUnique(new CJPX_Decoder(option));
  if (!decoder->Init(src_span, resolution_levels_to_skip, strict_mode)) {
    return nullptr;
  }
  return decoder;
}

// static
void CJPX_Decoder::Sycc420ToRgbForTesting(opj_image_t* img) {
  sycc420_to_rgb(img);
}

CJPX_Decoder::CJPX_Decoder(ColorSpaceOption option)
    : m_ColorSpaceOption(option) {}

CJPX_Decoder::~CJPX_Decoder() = default;

bool CJPX_Decoder::Init(pdfium::span<const uint8_t> src_data,
                        uint8_t resolution_levels_to_skip,
                        bool strict_mode) {
  static constexpr uint8_t kJP2Header[] = {0x00, 0x00, 0x00, 0x0c, 0x6a, 0x50,
                                           0x20, 0x20, 0x0d, 0x0a, 0x87, 0x0a};
  if (src_data.size() < sizeof(kJP2Header) ||
      resolution_levels_to_skip > kMaxResolutionsToSkip) {
    return false;
  }

  m_Image.reset();
  m_SrcData = src_data;
  m_DecodeData = std::make_unique<DecodeData>(src_data);
  m_Stream.reset(fx_opj_stream_create_memory_stream(m_DecodeData.get()));
  if (!m_Stream)
    return false;

  opj_set_default_decoder_parameters(&m_Parameters);
  m_Parameters.decod_format = 0;
  m_Parameters.cod_format = 3;
  m_Parameters.cp_reduce = resolution_levels_to_skip;
  if (memcmp(m_SrcData.data(), kJP2Header, sizeof(kJP2Header)) == 0) {
    m_Codec.reset(opj_create_decompress(OPJ_CODEC_JP2));
    m_Parameters.decod_format = 1;
  } else {
    m_Codec.reset(opj_create_decompress(OPJ_CODEC_J2K));
  }
  if (!m_Codec)
    return false;

  if (m_ColorSpaceOption == ColorSpaceOption::kIndexed) {
    m_Parameters.flags |= OPJ_DPARAMETERS_IGNORE_PCLR_CMAP_CDEF_FLAG;
  }
  opj_set_info_handler(m_Codec.get(), fx_ignore_callback, nullptr);
  opj_set_warning_handler(m_Codec.get(), fx_ignore_callback, nullptr);
  opj_set_error_handler(m_Codec.get(), fx_ignore_callback, nullptr);
  if (!opj_setup_decoder(m_Codec.get(), &m_Parameters)) {
    return false;
  }

  // For https://crbug.com/42270564
  if (!strict_mode) {
    CHECK(opj_decoder_set_strict_mode(m_Codec.get(), false));
  }

  opj_image_t* pTempImage = nullptr;
  if (!opj_read_header(m_Stream.get(), m_Codec.get(), &pTempImage)) {
    return false;
  }

  m_Image.reset(pTempImage);
  return true;
}

bool CJPX_Decoder::StartDecode() {
  if (!m_Parameters.nb_tile_to_decode) {
    if (!opj_set_decode_area(m_Codec.get(), m_Image.get(), m_Parameters.DA_x0,
                             m_Parameters.DA_y0, m_Parameters.DA_x1,
                             m_Parameters.DA_y1)) {
      m_Image.reset();
      return false;
    }
    if (!(opj_decode(m_Codec.get(), m_Stream.get(), m_Image.get()) &&
          opj_end_decompress(m_Codec.get(), m_Stream.get()))) {
      m_Image.reset();
      return false;
    }
  } else if (!opj_get_decoded_tile(m_Codec.get(), m_Stream.get(), m_Image.get(),
                                   m_Parameters.tile_index)) {
    return false;
  }

  m_Stream.reset();
  auto components = components_span(m_Image.get());
  if (m_Image->color_space != OPJ_CLRSPC_SYCC && components.size() == 3 &&
      components[0].dx == components[0].dy && components[1].dx != 1) {
    m_Image->color_space = OPJ_CLRSPC_SYCC;
  } else if (m_Image->numcomps <= 2) {
    m_Image->color_space = OPJ_CLRSPC_GRAY;
  }
  if (m_Image->color_space == OPJ_CLRSPC_SYCC)
    color_sycc_to_rgb(m_Image.get());

  // TODO(crbug.com/346606150): Investigate if it makes sense to use the data in
  // `icc_profile_buf` instead of discarding it.
  if (m_Image->icc_profile_buf) {
    free(m_Image->icc_profile_buf);
    m_Image->icc_profile_buf = nullptr;
    m_Image->icc_profile_len = 0;
  }
  return true;
}

CJPX_Decoder::JpxImageInfo CJPX_Decoder::GetInfo() const {
  const auto components = components_span(m_Image.get());
  return {components[0].w, components[0].h,
          pdfium::checked_cast<uint32_t>(components.size()),
          m_Image->color_space};
}

bool CJPX_Decoder::Decode(pdfium::span<uint8_t> dest_buf,
                          uint32_t pitch,
                          bool swap_rgb,
                          uint32_t component_count) {
  CHECK_LE(component_count, m_Image->numcomps);
  uint32_t channel_count = component_count;
  if (channel_count == 3 && m_Image->numcomps == 4) {
    // When decoding for an ARGB image, include the alpha channel in the channel
    // count.
    channel_count = 4;
  }

  std::optional<uint32_t> calculated_pitch =
      fxge::CalculatePitch32(8 * channel_count, m_Image->comps[0].w);
  if (!calculated_pitch.has_value() || pitch < calculated_pitch.value()) {
    return false;
  }

  if (swap_rgb && channel_count < 3) {
    return false;
  }

  // Initialize `channel_bufs` and `adjust_comps` to store information from all
  // the channels of the JPX image. They will contain more information besides
  // the color component data if `m_Image->numcomps` > `component_count`.
  // Currently only the color component data is used for rendering.
  // TODO(crbug.com/pdfium/1747): Make full use of the component information.
  fxcrt::Fill(dest_buf.first(m_Image->comps[0].h * pitch), 0xff);
  std::vector<uint8_t*> channel_bufs(m_Image->numcomps);
  std::vector<int> adjust_comps(m_Image->numcomps);
  const pdfium::span<opj_image_comp_t> components =
      components_span(m_Image.get());
  for (size_t i = 0; i < components.size(); i++) {
    channel_bufs[i] = dest_buf.subspan(i).data();
    adjust_comps[i] = components[i].prec - 8;
    if (i > 0) {
      if (components[i].dx != components[i - 1].dx ||
          components[i].dy != components[i - 1].dy ||
          components[i].prec != components[i - 1].prec) {
        return false;
      }
    }
  }
  if (swap_rgb)
    std::swap(channel_bufs[0], channel_bufs[2]);

  uint32_t width = components[0].w;
  uint32_t height = components[0].h;
  for (uint32_t channel = 0; channel < channel_count; ++channel) {
    uint8_t* pChannel = channel_bufs[channel];
    const int adjust = adjust_comps[channel];
    const opj_image_comp_t& comps = components[channel];
    if (!comps.data)
      continue;

    // Perfomance-sensitive code below. Combining these 3 for-loops below will
    // cause a slowdown.
    UNSAFE_TODO({
      const uint32_t src_offset = comps.sgnd ? 1 << (comps.prec - 1) : 0;
      if (adjust < 0) {
        for (uint32_t row = 0; row < height; ++row) {
          uint8_t* pScanline = pChannel + row * pitch;
          for (uint32_t col = 0; col < width; ++col) {
            uint8_t* pPixel = pScanline + col * channel_count;
            int src = comps.data[row * width + col] + src_offset;
            *pPixel = static_cast<uint8_t>(src << -adjust);
          }
        }
      } else if (adjust == 0) {
        for (uint32_t row = 0; row < height; ++row) {
          uint8_t* pScanline = pChannel + row * pitch;
          for (uint32_t col = 0; col < width; ++col) {
            uint8_t* pPixel = pScanline + col * channel_count;
            int src = comps.data[row * width + col] + src_offset;
            *pPixel = static_cast<uint8_t>(src);
          }
        }
      } else {
        for (uint32_t row = 0; row < height; ++row) {
          uint8_t* pScanline = pChannel + row * pitch;
          for (uint32_t col = 0; col < width; ++col) {
            uint8_t* pPixel = pScanline + col * channel_count;
            int src = comps.data[row * width + col] + src_offset;
            int pixel = (src >> adjust) + ((src >> (adjust - 1)) % 2);
            pixel = std::clamp(pixel, 0, 255);
            *pPixel = static_cast<uint8_t>(pixel);
          }
        }
      }
    });
  }
  return true;
}

}  // namespace fxcodec
