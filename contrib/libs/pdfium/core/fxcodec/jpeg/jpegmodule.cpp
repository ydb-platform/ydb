// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/jpeg/jpegmodule.h"

#include <setjmp.h>

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "build/build_config.h"
#include "core/fxcodec/cfx_codec_memory.h"
#include "core/fxcodec/jpeg/jpeg_common.h"
#include "core/fxcodec/scanlinedecoder.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/raw_span.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/fx_dib.h"

static pdfium::span<const uint8_t> JpegScanSOI(
    pdfium::span<const uint8_t> src_span) {
  DCHECK(!src_span.empty());

  for (size_t offset = 0; offset + 1 < src_span.size(); ++offset) {
    if (src_span[offset] == 0xff && src_span[offset + 1] == 0xd8)
      return src_span.subspan(offset);
  }
  return src_span;
}

extern "C" {

static void error_fatal(j_common_ptr cinfo) {
  longjmp(*(jmp_buf*)cinfo->client_data, -1);
}

static void src_skip_data(jpeg_decompress_struct* cinfo, long num) {
  if (num > (long)cinfo->src->bytes_in_buffer) {
    error_fatal((j_common_ptr)cinfo);
  }
  // SAFETY: required from library API as checked above.
  UNSAFE_BUFFERS(cinfo->src->next_input_byte += num);
  cinfo->src->bytes_in_buffer -= num;
}

#if BUILDFLAG(IS_WIN)
static void dest_do_nothing(j_compress_ptr cinfo) {}

static boolean dest_empty(j_compress_ptr cinfo) {
  return false;
}
#endif  // BUILDFLAG(IS_WIN)

}  // extern "C"

static bool JpegLoadInfo(pdfium::span<const uint8_t> src_span,
                         JpegModule::ImageInfo* pInfo) {
  src_span = JpegScanSOI(src_span);
  jpeg_decompress_struct cinfo;
  jpeg_error_mgr jerr;
  jerr.error_exit = error_fatal;
  jerr.emit_message = error_do_nothing_int;
  jerr.output_message = error_do_nothing;
  jerr.format_message = error_do_nothing_char;
  jerr.reset_error_mgr = error_do_nothing;
  jerr.trace_level = 0;
  cinfo.err = &jerr;
  jmp_buf mark;
  cinfo.client_data = &mark;
  if (setjmp(mark) == -1)
    return false;

  jpeg_create_decompress(&cinfo);
  jpeg_source_mgr src;
  src.init_source = src_do_nothing;
  src.term_source = src_do_nothing;
  src.skip_input_data = src_skip_data;
  src.fill_input_buffer = src_fill_buffer;
  src.resync_to_restart = src_resync;
  src.bytes_in_buffer = src_span.size();
  src.next_input_byte = src_span.data();
  cinfo.src = &src;
  if (setjmp(mark) == -1) {
    jpeg_destroy_decompress(&cinfo);
    return false;
  }
  int ret = jpeg_read_header(&cinfo, TRUE);
  if (ret != JPEG_HEADER_OK) {
    jpeg_destroy_decompress(&cinfo);
    return false;
  }
  pInfo->width = cinfo.image_width;
  pInfo->height = cinfo.image_height;
  pInfo->num_components = cinfo.num_components;
  pInfo->color_transform =
      cinfo.jpeg_color_space == JCS_YCbCr || cinfo.jpeg_color_space == JCS_YCCK;
  pInfo->bits_per_components = cinfo.data_precision;
  jpeg_destroy_decompress(&cinfo);
  return true;
}

namespace fxcodec {

namespace {

constexpr size_t kKnownBadHeaderWithInvalidHeightByteOffsetStarts[] = {94, 163};

class JpegDecoder final : public ScanlineDecoder {
 public:
  JpegDecoder();
  ~JpegDecoder() override;

  bool Create(pdfium::span<const uint8_t> src_span,
              uint32_t width,
              uint32_t height,
              int nComps,
              bool ColorTransform);

  // ScanlineDecoder:
  bool Rewind() override;
  pdfium::span<uint8_t> GetNextLine() override;
  uint32_t GetSrcOffset() override;

  bool InitDecode(bool bAcceptKnownBadHeader);

 private:
  void CalcPitch();
  void InitDecompressSrc();

  // Can only be called inside a jpeg_read_header() setjmp handler.
  bool HasKnownBadHeaderWithInvalidHeight(size_t dimension_offset) const;

  // Is a JPEG SOFn marker, which is defined as 0xff, 0xc[0-9a-f].
  bool IsSofSegment(size_t marker_offset) const;

  // Patch up the in-memory JPEG header for known bad JPEGs.
  void PatchUpKnownBadHeaderWithInvalidHeight(size_t dimension_offset);

  // Patch up the JPEG trailer, even if it is correct.
  void PatchUpTrailer();

  pdfium::span<uint8_t> GetWritableSrcData();

  // For a given invalid height byte offset in
  // |kKnownBadHeaderWithInvalidHeightByteOffsetStarts|, the SOFn marker should
  // be this many bytes before that.
  static constexpr size_t kSofMarkerByteOffset = 5;

  jmp_buf m_JmpBuf;
  jpeg_decompress_struct m_Cinfo = {};
  jpeg_error_mgr m_Jerr = {};
  jpeg_source_mgr m_Src = {};
  pdfium::raw_span<const uint8_t> m_SrcSpan;
  DataVector<uint8_t> m_ScanlineBuf;
  bool m_bInited = false;
  bool m_bStarted = false;
  bool m_bJpegTransform = false;
  uint32_t m_nDefaultScaleDenom = 1;

  static_assert(std::is_aggregate_v<decltype(m_Cinfo)>);
  static_assert(std::is_aggregate_v<decltype(m_Jerr)>);
  static_assert(std::is_aggregate_v<decltype(m_Src)>);
};

JpegDecoder::JpegDecoder() = default;

JpegDecoder::~JpegDecoder() {
  if (m_bInited)
    jpeg_destroy_decompress(&m_Cinfo);

  // Span in superclass can't outlive our buffer.
  m_pLastScanline = pdfium::span<uint8_t>();
}

bool JpegDecoder::InitDecode(bool bAcceptKnownBadHeader) {
  m_Cinfo.err = &m_Jerr;
  m_Cinfo.client_data = &m_JmpBuf;
  if (setjmp(m_JmpBuf) == -1)
    return false;

  jpeg_create_decompress(&m_Cinfo);
  InitDecompressSrc();
  m_bInited = true;

  if (setjmp(m_JmpBuf) == -1) {
    std::optional<size_t> known_bad_header_offset;
    if (bAcceptKnownBadHeader) {
      for (size_t offset : kKnownBadHeaderWithInvalidHeightByteOffsetStarts) {
        if (HasKnownBadHeaderWithInvalidHeight(offset)) {
          known_bad_header_offset = offset;
          break;
        }
      }
    }
    jpeg_destroy_decompress(&m_Cinfo);
    if (!known_bad_header_offset.has_value()) {
      m_bInited = false;
      return false;
    }

    PatchUpKnownBadHeaderWithInvalidHeight(known_bad_header_offset.value());

    jpeg_create_decompress(&m_Cinfo);
    InitDecompressSrc();
  }
  m_Cinfo.image_width = m_OrigWidth;
  m_Cinfo.image_height = m_OrigHeight;
  int ret = jpeg_read_header(&m_Cinfo, TRUE);
  if (ret != JPEG_HEADER_OK)
    return false;

  if (m_Cinfo.saw_Adobe_marker)
    m_bJpegTransform = true;

  if (m_Cinfo.num_components == 3 && !m_bJpegTransform)
    m_Cinfo.out_color_space = m_Cinfo.jpeg_color_space;

  m_OrigWidth = m_Cinfo.image_width;
  m_OrigHeight = m_Cinfo.image_height;
  m_OutputWidth = m_OrigWidth;
  m_OutputHeight = m_OrigHeight;
  m_nDefaultScaleDenom = m_Cinfo.scale_denom;
  return true;
}

bool JpegDecoder::Create(pdfium::span<const uint8_t> src_span,
                         uint32_t width,
                         uint32_t height,
                         int nComps,
                         bool ColorTransform) {
  m_SrcSpan = JpegScanSOI(src_span);
  if (m_SrcSpan.size() < 2)
    return false;

  PatchUpTrailer();

  m_Jerr.error_exit = error_fatal;
  m_Jerr.emit_message = error_do_nothing_int;
  m_Jerr.output_message = error_do_nothing;
  m_Jerr.format_message = error_do_nothing_char;
  m_Jerr.reset_error_mgr = error_do_nothing;
  m_Src.init_source = src_do_nothing;
  m_Src.term_source = src_do_nothing;
  m_Src.skip_input_data = src_skip_data;
  m_Src.fill_input_buffer = src_fill_buffer;
  m_Src.resync_to_restart = src_resync;
  m_bJpegTransform = ColorTransform;
  m_OutputWidth = m_OrigWidth = width;
  m_OutputHeight = m_OrigHeight = height;
  if (!InitDecode(/*bAcceptKnownBadHeader=*/true))
    return false;

  if (m_Cinfo.num_components < nComps)
    return false;

  if (m_Cinfo.image_width < width)
    return false;

  CalcPitch();
  m_ScanlineBuf = DataVector<uint8_t>(m_Pitch);
  m_nComps = m_Cinfo.num_components;
  m_bpc = 8;
  m_bStarted = false;
  return true;
}

bool JpegDecoder::Rewind() {
  if (m_bStarted) {
    jpeg_destroy_decompress(&m_Cinfo);
    if (!InitDecode(/*bAcceptKnownBadHeader=*/false)) {
      return false;
    }
  }
  if (setjmp(m_JmpBuf) == -1) {
    return false;
  }
  m_Cinfo.scale_denom = m_nDefaultScaleDenom;
  m_OutputWidth = m_OrigWidth;
  m_OutputHeight = m_OrigHeight;
  if (!jpeg_start_decompress(&m_Cinfo)) {
    jpeg_destroy_decompress(&m_Cinfo);
    return false;
  }
  CHECK_LE(static_cast<int>(m_Cinfo.output_width), m_OrigWidth);
  m_bStarted = true;
  return true;
}

pdfium::span<uint8_t> JpegDecoder::GetNextLine() {
  if (setjmp(m_JmpBuf) == -1)
    return pdfium::span<uint8_t>();

  uint8_t* row_array[] = {m_ScanlineBuf.data()};
  int nlines = jpeg_read_scanlines(&m_Cinfo, row_array, 1);
  if (nlines <= 0)
    return pdfium::span<uint8_t>();

  return m_ScanlineBuf;
}

uint32_t JpegDecoder::GetSrcOffset() {
  return static_cast<uint32_t>(m_SrcSpan.size() - m_Src.bytes_in_buffer);
}

void JpegDecoder::CalcPitch() {
  m_Pitch = static_cast<uint32_t>(m_Cinfo.image_width) * m_Cinfo.num_components;
  m_Pitch += 3;
  m_Pitch /= 4;
  m_Pitch *= 4;
}

void JpegDecoder::InitDecompressSrc() {
  m_Cinfo.src = &m_Src;
  m_Src.bytes_in_buffer = m_SrcSpan.size();
  m_Src.next_input_byte = m_SrcSpan.data();
}

bool JpegDecoder::HasKnownBadHeaderWithInvalidHeight(
    size_t dimension_offset) const {
  // Perform lots of possibly redundant checks to make sure this has no false
  // positives.
  bool bDimensionChecks = m_Cinfo.err->msg_code == JERR_IMAGE_TOO_BIG &&
                          m_Cinfo.image_width < JPEG_MAX_DIMENSION &&
                          m_Cinfo.image_height == 0xffff && m_OrigWidth > 0 &&
                          m_OrigWidth <= JPEG_MAX_DIMENSION &&
                          m_OrigHeight > 0 &&
                          m_OrigHeight <= JPEG_MAX_DIMENSION;
  if (!bDimensionChecks)
    return false;

  if (m_SrcSpan.size() <= dimension_offset + 3u)
    return false;

  if (!IsSofSegment(dimension_offset - kSofMarkerByteOffset))
    return false;

  const auto pHeaderDimensions = m_SrcSpan.subspan(dimension_offset);
  uint8_t nExpectedWidthByte1 = (m_OrigWidth >> 8) & 0xff;
  uint8_t nExpectedWidthByte2 = m_OrigWidth & 0xff;
  // Height high byte, height low byte, width high byte, width low byte.
  return pHeaderDimensions[0] == 0xff && pHeaderDimensions[1] == 0xff &&
         pHeaderDimensions[2] == nExpectedWidthByte1 &&
         pHeaderDimensions[3] == nExpectedWidthByte2;
}

bool JpegDecoder::IsSofSegment(size_t marker_offset) const {
  const auto pHeaderMarker = m_SrcSpan.subspan(marker_offset);
  return pHeaderMarker[0] == 0xff && pHeaderMarker[1] >= 0xc0 &&
         pHeaderMarker[1] <= 0xcf;
}

void JpegDecoder::PatchUpKnownBadHeaderWithInvalidHeight(
    size_t dimension_offset) {
  DCHECK(m_SrcSpan.size() > dimension_offset + 1u);
  auto pData = GetWritableSrcData().subspan(dimension_offset);
  pData[0] = (m_OrigHeight >> 8) & 0xff;
  pData[1] = m_OrigHeight & 0xff;
}

void JpegDecoder::PatchUpTrailer() {
  auto pData = GetWritableSrcData();
  pData[m_SrcSpan.size() - 2] = 0xff;
  pData[m_SrcSpan.size() - 1] = 0xd9;
}

pdfium::span<uint8_t> JpegDecoder::GetWritableSrcData() {
  // SAFETY: const_cast<> doesn't change size.
  return UNSAFE_BUFFERS(pdfium::make_span(
      const_cast<uint8_t*>(m_SrcSpan.data()), m_SrcSpan.size()));
}

}  // namespace

// static
std::unique_ptr<ScanlineDecoder> JpegModule::CreateDecoder(
    pdfium::span<const uint8_t> src_span,
    uint32_t width,
    uint32_t height,
    int nComps,
    bool ColorTransform) {
  DCHECK(!src_span.empty());

  auto pDecoder = std::make_unique<JpegDecoder>();
  if (!pDecoder->Create(src_span, width, height, nComps, ColorTransform))
    return nullptr;

  return pDecoder;
}

// static
std::optional<JpegModule::ImageInfo> JpegModule::LoadInfo(
    pdfium::span<const uint8_t> src_span) {
  ImageInfo info;
  if (!JpegLoadInfo(src_span, &info))
    return std::nullopt;

  return info;
}

#if BUILDFLAG(IS_WIN)
bool JpegModule::JpegEncode(const RetainPtr<const CFX_DIBBase>& pSource,
                            uint8_t** dest_buf,
                            size_t* dest_size) {
  jpeg_error_mgr jerr;
  jerr.error_exit = error_do_nothing;
  jerr.emit_message = error_do_nothing_int;
  jerr.output_message = error_do_nothing;
  jerr.format_message = error_do_nothing_char;
  jerr.reset_error_mgr = error_do_nothing;

  jpeg_compress_struct cinfo = {};  // Aggregate initialization.
  static_assert(std::is_aggregate_v<decltype(cinfo)>);
  cinfo.err = &jerr;
  jpeg_create_compress(&cinfo);
  const int bytes_per_pixel = pSource->GetBPP() / 8;
  uint32_t nComponents = bytes_per_pixel >= 3 ? 3 : 1;
  uint32_t pitch = pSource->GetPitch();
  uint32_t width = pdfium::checked_cast<uint32_t>(pSource->GetWidth());
  uint32_t height = pdfium::checked_cast<uint32_t>(pSource->GetHeight());
  FX_SAFE_UINT32 safe_buf_len = width;
  safe_buf_len *= height;
  safe_buf_len *= nComponents;
  safe_buf_len += 1024;
  if (!safe_buf_len.IsValid())
    return false;

  uint32_t dest_buf_length = safe_buf_len.ValueOrDie();
  *dest_buf = FX_TryAlloc(uint8_t, dest_buf_length);
  const int MIN_TRY_BUF_LEN = 1024;
  while (!(*dest_buf) && dest_buf_length > MIN_TRY_BUF_LEN) {
    dest_buf_length >>= 1;
    *dest_buf = FX_TryAlloc(uint8_t, dest_buf_length);
  }
  if (!(*dest_buf))
    return false;

  jpeg_destination_mgr dest;
  dest.init_destination = dest_do_nothing;
  dest.term_destination = dest_do_nothing;
  dest.empty_output_buffer = dest_empty;
  dest.next_output_byte = *dest_buf;
  dest.free_in_buffer = dest_buf_length;
  cinfo.dest = &dest;
  cinfo.image_width = width;
  cinfo.image_height = height;
  cinfo.input_components = nComponents;
  if (nComponents == 1) {
    cinfo.in_color_space = JCS_GRAYSCALE;
  } else if (nComponents == 3) {
    cinfo.in_color_space = JCS_RGB;
  } else {
    cinfo.in_color_space = JCS_CMYK;
  }
  uint8_t* line_buf = nullptr;
  if (nComponents > 1)
    line_buf = FX_Alloc2D(uint8_t, width, nComponents);

  jpeg_set_defaults(&cinfo);
  jpeg_start_compress(&cinfo, TRUE);
  JSAMPROW row_pointer[1];
  JDIMENSION row;
  while (cinfo.next_scanline < cinfo.image_height) {
    pdfium::span<const uint8_t> src_scan =
        pSource->GetScanline(cinfo.next_scanline);
    if (nComponents > 1) {
      uint8_t* dest_scan = line_buf;
      if (nComponents == 3) {
        UNSAFE_TODO({
          for (uint32_t i = 0; i < width; i++) {
            ReverseCopy3Bytes(dest_scan, src_scan.data());
            dest_scan += 3;
            src_scan = src_scan.subspan(bytes_per_pixel);
          }
        });
      } else {
        UNSAFE_TODO({
          for (uint32_t i = 0; i < pitch; i++) {
            *dest_scan++ = ~src_scan.front();
            src_scan = src_scan.subspan(1);
          }
        });
      }
      row_pointer[0] = line_buf;
    } else {
      row_pointer[0] = const_cast<uint8_t*>(src_scan.data());
    }
    row = cinfo.next_scanline;
    jpeg_write_scanlines(&cinfo, row_pointer, 1);
    UNSAFE_TODO({
      if (cinfo.next_scanline == row) {
        constexpr size_t kJpegBlockSize = 1048576;
        *dest_buf =
            FX_Realloc(uint8_t, *dest_buf, dest_buf_length + kJpegBlockSize);
        dest.next_output_byte =
            *dest_buf + dest_buf_length - dest.free_in_buffer;
        dest_buf_length += kJpegBlockSize;
        dest.free_in_buffer += kJpegBlockSize;
      }
    });
  }
  jpeg_finish_compress(&cinfo);
  jpeg_destroy_compress(&cinfo);
  FX_Free(line_buf);
  *dest_size = dest_buf_length - static_cast<size_t>(dest.free_in_buffer);

  return true;
}
#endif  // BUILDFLAG(IS_WIN)

}  // namespace fxcodec
