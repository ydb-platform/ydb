// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/progressive_decoder.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "build/build_config.h"
#include "core/fxcodec/cfx_codec_memory.h"
#include "core/fxcodec/jpeg/jpeg_progressive_decoder.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/check_op.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_2d_size.h"
#include "core/fxcrt/fx_memcpy_wrappers.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/fx_stream.h"
#include "core/fxcrt/fx_system.h"
#include "core/fxcrt/notreached.h"
#include "core/fxcrt/numerics/safe_conversions.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"
#include "core/fxge/dib/cfx_cmyk_to_srgb.h"
#include "core/fxge/dib/cfx_dibitmap.h"
#include "core/fxge/dib/fx_dib.h"

#ifdef PDF_ENABLE_XFA_BMP
#include "core/fxcodec/bmp/bmp_progressive_decoder.h"
#endif  // PDF_ENABLE_XFA_BMP

#ifdef PDF_ENABLE_XFA_GIF
#include "core/fxcodec/gif/gif_progressive_decoder.h"
#endif  // PDF_ENABLE_XFA_GIF

#ifdef PDF_ENABLE_XFA_TIFF
#include "core/fxcodec/tiff/tiff_decoder.h"
#endif  // PDF_ENABLE_XFA_TIFF

namespace fxcodec {

namespace {

constexpr size_t kBlockSize = 4096;

#ifdef PDF_ENABLE_XFA_PNG
#if BUILDFLAG(IS_APPLE)
const double kPngGamma = 1.7;
#else
const double kPngGamma = 2.2;
#endif  // BUILDFLAG(IS_APPLE)
#endif  // PDF_ENABLE_XFA_PNG

void RGB2BGR(uint8_t* buffer, int width = 1) {
  if (buffer && width > 0) {
    uint8_t temp;
    int i = 0;
    int j = 0;
    UNSAFE_TODO({
      for (; i < width; i++, j += 3) {
        temp = buffer[j];
        buffer[j] = buffer[j + 2];
        buffer[j + 2] = temp;
      }
    });
  }
}

}  // namespace

ProgressiveDecoder::ProgressiveDecoder() = default;

ProgressiveDecoder::~ProgressiveDecoder() = default;

#ifdef PDF_ENABLE_XFA_PNG
bool ProgressiveDecoder::PngReadHeader(int width,
                                       int height,
                                       int bpc,
                                       int pass,
                                       int* color_type,
                                       double* gamma) {
  if (!m_pDeviceBitmap) {
    m_SrcWidth = width;
    m_SrcHeight = height;
    m_SrcBPC = bpc;
    m_SrcPassNumber = pass;
    switch (*color_type) {
      case 0:
        m_SrcComponents = 1;
        break;
      case 4:
        m_SrcComponents = 2;
        break;
      case 2:
        m_SrcComponents = 3;
        break;
      case 3:
      case 6:
        m_SrcComponents = 4;
        break;
      default:
        m_SrcComponents = 0;
        break;
    }
    return false;
  }
  switch (m_pDeviceBitmap->GetFormat()) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppMask:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k8bppMask:
    case FXDIB_Format::k8bppRgb:
      NOTREACHED_NORETURN();
    case FXDIB_Format::kBgr:
      *color_type = 2;
      break;
    case FXDIB_Format::kBgrx:
    case FXDIB_Format::kBgra:
      *color_type = 6;
      break;
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul:
      // TODO(crbug.com/355630556): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
#endif
  }
  *gamma = kPngGamma;
  return true;
}

uint8_t* ProgressiveDecoder::PngAskScanlineBuf(int line) {
  CHECK_GE(line, 0);
  CHECK_LT(line, m_SrcHeight);
  CHECK_EQ(m_pDeviceBitmap->GetFormat(), FXDIB_Format::kBgra);
  CHECK_EQ(m_SrcFormat, FXCodec_Argb);
  pdfium::span<const uint8_t> src_span = m_pDeviceBitmap->GetScanline(line);
  pdfium::span<uint8_t> dest_span = pdfium::make_span(m_DecodeBuf);
  const size_t byte_size = Fx2DSizeOrDie(
      m_SrcWidth, GetCompsFromFormat(m_pDeviceBitmap->GetFormat()));
  fxcrt::Copy(src_span.first(byte_size), dest_span);
  return m_DecodeBuf.data();
}

void ProgressiveDecoder::PngFillScanlineBufCompleted(int pass, int line) {
  if (line < 0 || line >= m_SrcHeight) {
    return;
  }

  CHECK_EQ(m_pDeviceBitmap->GetFormat(), FXDIB_Format::kBgra);
  pdfium::span<const uint8_t> src_span = pdfium::make_span(m_DecodeBuf);
  pdfium::span<uint8_t> dest_span = m_pDeviceBitmap->GetWritableScanline(line);
  const size_t byte_size = Fx2DSizeOrDie(
      m_SrcWidth, GetCompsFromFormat(m_pDeviceBitmap->GetFormat()));
  fxcrt::Copy(src_span.first(byte_size), dest_span);
}
#endif  // PDF_ENABLE_XFA_PNG

#ifdef PDF_ENABLE_XFA_GIF
uint32_t ProgressiveDecoder::GifCurrentPosition() const {
  uint32_t remain_size = pdfium::checked_cast<uint32_t>(
      GifDecoder::GetAvailInput(m_pGifContext.get()));
  return m_offSet - remain_size;
}

bool ProgressiveDecoder::GifInputRecordPositionBuf(
    uint32_t rcd_pos,
    const FX_RECT& img_rc,
    pdfium::span<CFX_GifPalette> pal_span,
    int32_t trans_index,
    bool interlace) {
  m_offSet = rcd_pos;

  FXCODEC_STATUS error_status = FXCODEC_STATUS::kError;
  m_pCodecMemory->Seek(m_pCodecMemory->GetSize());
  if (!GifReadMoreData(&error_status))
    return false;

  if (pal_span.empty()) {
    pal_span = m_GifPalette;
  }
  if (pal_span.empty()) {
    return false;
  }
  m_SrcPalette.resize(pal_span.size());
  for (size_t i = 0; i < pal_span.size(); i++) {
    m_SrcPalette[i] =
        ArgbEncode(0xff, pal_span[i].r, pal_span[i].g, pal_span[i].b);
  }
  m_GifTransIndex = trans_index;
  m_GifFrameRect = img_rc;
  m_SrcPassNumber = interlace ? 4 : 1;
  int32_t pal_index = m_GifBgIndex;
  RetainPtr<CFX_DIBitmap> pDevice = m_pDeviceBitmap;
  if (trans_index >= static_cast<int>(pal_span.size())) {
    trans_index = -1;
  }
  if (trans_index != -1) {
    m_SrcPalette[trans_index] &= 0x00ffffff;
    if (pDevice->IsAlphaFormat()) {
      pal_index = trans_index;
    }
  }
  if (pal_index >= static_cast<int>(pal_span.size())) {
    return false;
  }
  int startX = 0;
  int startY = 0;
  int sizeX = m_SrcWidth;
  int sizeY = m_SrcHeight;
  const int bytes_per_pixel = pDevice->GetBPP() / 8;
  FX_ARGB argb = m_SrcPalette[pal_index];
  for (int row = 0; row < sizeY; row++) {
    pdfium::span<uint8_t> scan_span = pDevice->GetWritableScanline(row + startY)
                                          .subspan(startX * bytes_per_pixel);
    switch (m_TransMethod) {
      case TransformMethod::k8BppRgbToRgbNoAlpha: {
        uint8_t* pScanline = scan_span.data();
        UNSAFE_TODO({
          for (int col = 0; col < sizeX; col++) {
            *pScanline++ = FXARGB_B(argb);
            *pScanline++ = FXARGB_G(argb);
            *pScanline++ = FXARGB_R(argb);
            pScanline += bytes_per_pixel - 3;
          }
        });
        break;
      }
      case TransformMethod::k8BppRgbToArgb: {
        uint8_t* pScanline = scan_span.data();
        UNSAFE_TODO({
          for (int col = 0; col < sizeX; col++) {
            FXARGB_SetDIB(pScanline, argb);
            pScanline += 4;
          }
        });
        break;
      }
      default:
        break;
    }
  }
  return true;
}

void ProgressiveDecoder::GifReadScanline(int32_t row_num,
                                         pdfium::span<uint8_t> row_buf) {
  RetainPtr<CFX_DIBitmap> pDIBitmap = m_pDeviceBitmap;
  DCHECK(pDIBitmap);
  int32_t img_width = m_GifFrameRect.Width();
  if (!pDIBitmap->IsAlphaFormat()) {
    pdfium::span<uint8_t> byte_span = row_buf;
    for (int i = 0; i < img_width; i++) {
      if (byte_span.front() == m_GifTransIndex) {
        byte_span.front() = m_GifBgIndex;
      }
      byte_span = byte_span.subspan(1);
    }
  }
  int32_t pal_index = m_GifBgIndex;
  if (m_GifTransIndex != -1 && m_pDeviceBitmap->IsAlphaFormat()) {
    pal_index = m_GifTransIndex;
  }
  const int32_t left = m_GifFrameRect.left;
  const pdfium::span<uint8_t> decode_span = m_DecodeBuf;
  fxcrt::Fill(decode_span.first(m_SrcWidth), pal_index);
  fxcrt::Copy(row_buf.first(img_width), decode_span.subspan(left));

  int32_t line = row_num + m_GifFrameRect.top;
  if (line < 0 || line >= m_SrcHeight) {
    return;
  }

  ResampleScanline(pDIBitmap, line, decode_span, m_SrcFormat);
}
#endif  // PDF_ENABLE_XFA_GIF

#ifdef PDF_ENABLE_XFA_BMP
bool ProgressiveDecoder::BmpInputImagePositionBuf(uint32_t rcd_pos) {
  m_offSet = rcd_pos;
  FXCODEC_STATUS error_status = FXCODEC_STATUS::kError;
  return BmpReadMoreData(m_pBmpContext.get(), &error_status);
}

void ProgressiveDecoder::BmpReadScanline(uint32_t row_num,
                                         pdfium::span<const uint8_t> row_buf) {
  RetainPtr<CFX_DIBitmap> pDIBitmap = m_pDeviceBitmap;
  DCHECK(pDIBitmap);

  fxcrt::Copy(row_buf.first(m_ScanlineSize), m_DecodeBuf);

  if (row_num >= static_cast<uint32_t>(m_SrcHeight)) {
    return;
  }

  ResampleScanline(pDIBitmap, row_num, m_DecodeBuf, m_SrcFormat);
}

bool ProgressiveDecoder::BmpDetectImageTypeInBuffer(
    CFX_DIBAttribute* pAttribute) {
  std::unique_ptr<ProgressiveDecoderIface::Context> pBmpContext =
      BmpDecoder::StartDecode(this);
  BmpDecoder::Input(pBmpContext.get(), m_pCodecMemory);

  pdfium::span<const FX_ARGB> palette;
  BmpDecoder::Status read_result = BmpDecoder::ReadHeader(
      pBmpContext.get(), &m_SrcWidth, &m_SrcHeight, &m_BmpIsTopBottom,
      &m_SrcComponents, &palette, pAttribute);
  while (read_result == BmpDecoder::Status::kContinue) {
    FXCODEC_STATUS error_status = FXCODEC_STATUS::kError;
    if (!BmpReadMoreData(pBmpContext.get(), &error_status)) {
      m_status = error_status;
      return false;
    }
    read_result = BmpDecoder::ReadHeader(
        pBmpContext.get(), &m_SrcWidth, &m_SrcHeight, &m_BmpIsTopBottom,
        &m_SrcComponents, &palette, pAttribute);
  }

  if (read_result != BmpDecoder::Status::kSuccess) {
    m_status = FXCODEC_STATUS::kError;
    return false;
  }

  FXDIB_Format format = FXDIB_Format::kInvalid;
  switch (m_SrcComponents) {
    case 1:
      m_SrcFormat = FXCodec_8bppRgb;
      format = FXDIB_Format::k8bppRgb;
      break;
    case 3:
      m_SrcFormat = FXCodec_Rgb;
      format = FXDIB_Format::kBgr;
      break;
    case 4:
      m_SrcFormat = FXCodec_Rgb32;
      format = FXDIB_Format::kBgrx;
      break;
    default:
      m_status = FXCODEC_STATUS::kError;
      return false;
  }

  // Set to 0 to make CalculatePitchAndSize() calculate it.
  constexpr uint32_t kNoPitch = 0;
  std::optional<CFX_DIBitmap::PitchAndSize> needed_data =
      CFX_DIBitmap::CalculatePitchAndSize(m_SrcWidth, m_SrcHeight, format,
                                          kNoPitch);
  if (!needed_data.has_value()) {
    m_status = FXCODEC_STATUS::kError;
    return false;
  }

  uint32_t available_data = pdfium::checked_cast<uint32_t>(
      m_pFile->GetSize() - m_offSet +
      BmpDecoder::GetAvailInput(pBmpContext.get()));
  if (needed_data.value().size > available_data) {
    m_status = FXCODEC_STATUS::kError;
    return false;
  }

  m_SrcBPC = 8;
  m_pBmpContext = std::move(pBmpContext);
  if (!palette.empty()) {
    m_SrcPalette.resize(palette.size());
    fxcrt::Copy(palette, m_SrcPalette);
  } else {
    m_SrcPalette.clear();
  }
  return true;
}

bool ProgressiveDecoder::BmpReadMoreData(
    ProgressiveDecoderIface::Context* pContext,
    FXCODEC_STATUS* err_status) {
  return ReadMoreData(BmpProgressiveDecoder::GetInstance(), pContext,
                      err_status);
}

FXCODEC_STATUS ProgressiveDecoder::BmpStartDecode() {
  SetTransMethod();
  m_ScanlineSize = FxAlignToBoundary<4>(m_SrcWidth * m_SrcComponents);
  m_DecodeBuf.resize(m_ScanlineSize);
  FXDIB_ResampleOptions options;
  options.bInterpolateBilinear = true;
  m_WeightHorz.CalculateWeights(m_SrcWidth, 0, m_SrcWidth, m_SrcWidth, 0,
                                m_SrcWidth, options);
  m_status = FXCODEC_STATUS::kDecodeToBeContinued;
  return m_status;
}

FXCODEC_STATUS ProgressiveDecoder::BmpContinueDecode() {
  BmpDecoder::Status read_res = BmpDecoder::LoadImage(m_pBmpContext.get());
  while (read_res == BmpDecoder::Status::kContinue) {
    FXCODEC_STATUS error_status = FXCODEC_STATUS::kDecodeFinished;
    if (!BmpReadMoreData(m_pBmpContext.get(), &error_status)) {
      m_pDeviceBitmap = nullptr;
      m_pFile = nullptr;
      m_status = error_status;
      return m_status;
    }
    read_res = BmpDecoder::LoadImage(m_pBmpContext.get());
  }

  m_pDeviceBitmap = nullptr;
  m_pFile = nullptr;
  m_status = read_res == BmpDecoder::Status::kSuccess
                 ? FXCODEC_STATUS::kDecodeFinished
                 : FXCODEC_STATUS::kError;
  return m_status;
}
#endif  // PDF_ENABLE_XFA_BMP

#ifdef PDF_ENABLE_XFA_GIF
bool ProgressiveDecoder::GifReadMoreData(FXCODEC_STATUS* err_status) {
  return ReadMoreData(GifProgressiveDecoder::GetInstance(), m_pGifContext.get(),
                      err_status);
}

bool ProgressiveDecoder::GifDetectImageTypeInBuffer() {
  m_pGifContext = GifDecoder::StartDecode(this);
  GifDecoder::Input(m_pGifContext.get(), m_pCodecMemory);
  m_SrcComponents = 1;
  GifDecoder::Status readResult =
      GifDecoder::ReadHeader(m_pGifContext.get(), &m_SrcWidth, &m_SrcHeight,
                             &m_GifPalette, &m_GifBgIndex);
  while (readResult == GifDecoder::Status::kUnfinished) {
    FXCODEC_STATUS error_status = FXCODEC_STATUS::kError;
    if (!GifReadMoreData(&error_status)) {
      m_pGifContext = nullptr;
      m_status = error_status;
      return false;
    }
    readResult =
        GifDecoder::ReadHeader(m_pGifContext.get(), &m_SrcWidth, &m_SrcHeight,
                               &m_GifPalette, &m_GifBgIndex);
  }
  if (readResult == GifDecoder::Status::kSuccess) {
    m_SrcBPC = 8;
    return true;
  }
  m_pGifContext = nullptr;
  m_status = FXCODEC_STATUS::kError;
  return false;
}

FXCODEC_STATUS ProgressiveDecoder::GifStartDecode() {
  m_SrcFormat = FXCodec_8bppRgb;
  SetTransMethod();
  int scanline_size = FxAlignToBoundary<4>(m_SrcWidth);
  m_DecodeBuf.resize(scanline_size);
  FXDIB_ResampleOptions options;
  options.bInterpolateBilinear = true;
  m_WeightHorz.CalculateWeights(m_SrcWidth, 0, m_SrcWidth, m_SrcWidth, 0,
                                m_SrcWidth, options);
  m_FrameCur = 0;
  m_status = FXCODEC_STATUS::kDecodeToBeContinued;
  return m_status;
}

FXCODEC_STATUS ProgressiveDecoder::GifContinueDecode() {
  GifDecoder::Status readRes =
      GifDecoder::LoadFrame(m_pGifContext.get(), m_FrameCur);
  while (readRes == GifDecoder::Status::kUnfinished) {
    FXCODEC_STATUS error_status = FXCODEC_STATUS::kDecodeFinished;
    if (!GifReadMoreData(&error_status)) {
      m_pDeviceBitmap = nullptr;
      m_pFile = nullptr;
      m_status = error_status;
      return m_status;
    }
    readRes = GifDecoder::LoadFrame(m_pGifContext.get(), m_FrameCur);
  }

  if (readRes == GifDecoder::Status::kSuccess) {
    m_pDeviceBitmap = nullptr;
    m_pFile = nullptr;
    m_status = FXCODEC_STATUS::kDecodeFinished;
    return m_status;
  }

  m_pDeviceBitmap = nullptr;
  m_pFile = nullptr;
  m_status = FXCODEC_STATUS::kError;
  return m_status;
}
#endif  // PDF_ENABLE_XFA_GIF

bool ProgressiveDecoder::JpegReadMoreData(FXCODEC_STATUS* err_status) {
  return ReadMoreData(JpegProgressiveDecoder::GetInstance(),
                      m_pJpegContext.get(), err_status);
}

bool ProgressiveDecoder::JpegDetectImageTypeInBuffer(
    CFX_DIBAttribute* pAttribute) {
  m_pJpegContext = JpegProgressiveDecoder::Start();
  if (!m_pJpegContext) {
    m_status = FXCODEC_STATUS::kError;
    return false;
  }
  JpegProgressiveDecoder::GetInstance()->Input(m_pJpegContext.get(),
                                               m_pCodecMemory);

  // Setting jump marker before calling ReadHeader, since a longjmp to
  // the marker indicates a fatal error.
  if (setjmp(JpegProgressiveDecoder::GetJumpMark(m_pJpegContext.get())) == -1) {
    m_pJpegContext.reset();
    m_status = FXCODEC_STATUS::kError;
    return false;
  }

  int32_t readResult = JpegProgressiveDecoder::ReadHeader(
      m_pJpegContext.get(), &m_SrcWidth, &m_SrcHeight, &m_SrcComponents,
      pAttribute);
  while (readResult == 2) {
    FXCODEC_STATUS error_status = FXCODEC_STATUS::kError;
    if (!JpegReadMoreData(&error_status)) {
      m_status = error_status;
      return false;
    }
    readResult = JpegProgressiveDecoder::ReadHeader(
        m_pJpegContext.get(), &m_SrcWidth, &m_SrcHeight, &m_SrcComponents,
        pAttribute);
  }
  if (!readResult) {
    m_SrcBPC = 8;
    return true;
  }
  m_pJpegContext.reset();
  m_status = FXCODEC_STATUS::kError;
  return false;
}

FXCODEC_STATUS ProgressiveDecoder::JpegStartDecode() {
  // Setting jump marker before calling StartScanLine, since a longjmp to
  // the marker indicates a fatal error.
  if (setjmp(JpegProgressiveDecoder::GetJumpMark(m_pJpegContext.get())) == -1) {
    m_pJpegContext.reset();
    m_status = FXCODEC_STATUS::kError;
    return FXCODEC_STATUS::kError;
  }

  bool start_status =
      JpegProgressiveDecoder::StartScanline(m_pJpegContext.get());
  while (!start_status) {
    FXCODEC_STATUS error_status = FXCODEC_STATUS::kError;
    if (!JpegReadMoreData(&error_status)) {
      m_pDeviceBitmap = nullptr;
      m_pFile = nullptr;
      m_status = error_status;
      return m_status;
    }

    start_status = JpegProgressiveDecoder::StartScanline(m_pJpegContext.get());
  }
  m_DecodeBuf.resize(FxAlignToBoundary<4>(m_SrcWidth * m_SrcComponents));
  FXDIB_ResampleOptions options;
  options.bInterpolateBilinear = true;
  m_WeightHorz.CalculateWeights(m_SrcWidth, 0, m_SrcWidth, m_SrcWidth, 0,
                                m_SrcWidth, options);
  switch (m_SrcComponents) {
    case 1:
      m_SrcFormat = FXCodec_8bppGray;
      break;
    case 3:
      m_SrcFormat = FXCodec_Rgb;
      break;
    case 4:
      m_SrcFormat = FXCodec_Cmyk;
      break;
  }
  SetTransMethod();
  m_status = FXCODEC_STATUS::kDecodeToBeContinued;
  return m_status;
}

FXCODEC_STATUS ProgressiveDecoder::JpegContinueDecode() {
  // JpegModule* pJpegModule = m_pCodecMgr->GetJpegModule();
  // Setting jump marker before calling ReadScanLine, since a longjmp to
  // the marker indicates a fatal error.
  if (setjmp(JpegProgressiveDecoder::GetJumpMark(m_pJpegContext.get())) == -1) {
    m_pJpegContext.reset();
    m_status = FXCODEC_STATUS::kError;
    return FXCODEC_STATUS::kError;
  }

  while (true) {
    bool readRes = JpegProgressiveDecoder::ReadScanline(m_pJpegContext.get(),
                                                        m_DecodeBuf.data());
    while (!readRes) {
      FXCODEC_STATUS error_status = FXCODEC_STATUS::kDecodeFinished;
      if (!JpegReadMoreData(&error_status)) {
        m_pDeviceBitmap = nullptr;
        m_pFile = nullptr;
        m_status = error_status;
        return m_status;
      }
      readRes = JpegProgressiveDecoder::ReadScanline(m_pJpegContext.get(),
                                                     m_DecodeBuf.data());
    }
    if (m_SrcFormat == FXCodec_Rgb) {
      RGB2BGR(UNSAFE_TODO(m_DecodeBuf.data()), m_SrcWidth);
    }
    if (m_SrcRow >= m_SrcHeight) {
      m_pDeviceBitmap = nullptr;
      m_pFile = nullptr;
      m_status = FXCODEC_STATUS::kDecodeFinished;
      return m_status;
    }
    Resample(m_pDeviceBitmap, m_SrcRow, m_DecodeBuf.data(), m_SrcFormat);
    m_SrcRow++;
  }
}

#ifdef PDF_ENABLE_XFA_PNG
bool ProgressiveDecoder::PngDetectImageTypeInBuffer(
    CFX_DIBAttribute* pAttribute) {
  m_pPngContext = PngDecoder::StartDecode(this);
  if (!m_pPngContext) {
    m_status = FXCODEC_STATUS::kError;
    return false;
  }
  while (PngDecoder::ContinueDecode(m_pPngContext.get(), m_pCodecMemory,
                                    pAttribute)) {
    uint32_t remain_size = static_cast<uint32_t>(m_pFile->GetSize()) - m_offSet;
    uint32_t input_size = std::min<uint32_t>(remain_size, kBlockSize);
    if (input_size == 0) {
      m_pPngContext.reset();
      m_status = FXCODEC_STATUS::kError;
      return false;
    }
    if (m_pCodecMemory && input_size > m_pCodecMemory->GetSize())
      m_pCodecMemory = pdfium::MakeRetain<CFX_CodecMemory>(input_size);

    if (!m_pFile->ReadBlockAtOffset(
            m_pCodecMemory->GetBufferSpan().first(input_size), m_offSet)) {
      m_status = FXCODEC_STATUS::kError;
      return false;
    }
    m_offSet += input_size;
  }
  m_pPngContext.reset();
  if (m_SrcPassNumber == 0) {
    m_status = FXCODEC_STATUS::kError;
    return false;
  }
  return true;
}

FXCODEC_STATUS ProgressiveDecoder::PngStartDecode() {
  m_pPngContext = PngDecoder::StartDecode(this);
  if (!m_pPngContext) {
    m_pDeviceBitmap = nullptr;
    m_pFile = nullptr;
    m_status = FXCODEC_STATUS::kError;
    return m_status;
  }
  m_offSet = 0;
  CHECK_EQ(m_pDeviceBitmap->GetFormat(), FXDIB_Format::kBgra);
  m_SrcComponents = 4;
  m_SrcFormat = FXCodec_Argb;
  SetTransMethod();
  int scanline_size = FxAlignToBoundary<4>(m_SrcWidth * m_SrcComponents);
  m_DecodeBuf.resize(scanline_size);
  m_status = FXCODEC_STATUS::kDecodeToBeContinued;
  return m_status;
}

FXCODEC_STATUS ProgressiveDecoder::PngContinueDecode() {
  while (true) {
    uint32_t remain_size = (uint32_t)m_pFile->GetSize() - m_offSet;
    uint32_t input_size = std::min<uint32_t>(remain_size, kBlockSize);
    if (input_size == 0) {
      m_pPngContext.reset();
      m_pDeviceBitmap = nullptr;
      m_pFile = nullptr;
      m_status = FXCODEC_STATUS::kDecodeFinished;
      return m_status;
    }
    if (m_pCodecMemory && input_size > m_pCodecMemory->GetSize())
      m_pCodecMemory = pdfium::MakeRetain<CFX_CodecMemory>(input_size);

    bool bResult = m_pFile->ReadBlockAtOffset(
        m_pCodecMemory->GetBufferSpan().first(input_size), m_offSet);
    if (!bResult) {
      m_pDeviceBitmap = nullptr;
      m_pFile = nullptr;
      m_status = FXCODEC_STATUS::kError;
      return m_status;
    }
    m_offSet += input_size;
    bResult = PngDecoder::ContinueDecode(m_pPngContext.get(), m_pCodecMemory,
                                         nullptr);
    if (!bResult) {
      m_pDeviceBitmap = nullptr;
      m_pFile = nullptr;
      m_status = FXCODEC_STATUS::kError;
      return m_status;
    }
  }
}
#endif  // PDF_ENABLE_XFA_PNG

#ifdef PDF_ENABLE_XFA_TIFF
bool ProgressiveDecoder::TiffDetectImageTypeFromFile(
    CFX_DIBAttribute* pAttribute) {
  m_pTiffContext = TiffDecoder::CreateDecoder(m_pFile);
  if (!m_pTiffContext) {
    m_status = FXCODEC_STATUS::kError;
    return false;
  }
  int32_t dummy_bpc;
  bool ret = TiffDecoder::LoadFrameInfo(m_pTiffContext.get(), 0, &m_SrcWidth,
                                        &m_SrcHeight, &m_SrcComponents,
                                        &dummy_bpc, pAttribute);
  m_SrcComponents = 4;
  if (!ret) {
    m_pTiffContext.reset();
    m_status = FXCODEC_STATUS::kError;
    return false;
  }
  return true;
}

FXCODEC_STATUS ProgressiveDecoder::TiffContinueDecode() {
  // TODO(crbug.com/355630556): Consider adding support for
  // `FXDIB_Format::kBgraPremul`
  CHECK_EQ(m_pDeviceBitmap->GetFormat(), FXDIB_Format::kBgra);
  m_status =
      TiffDecoder::Decode(m_pTiffContext.get(), std::move(m_pDeviceBitmap))
          ? FXCODEC_STATUS::kDecodeFinished
          : FXCODEC_STATUS::kError;
  m_pFile = nullptr;
  return m_status;
}
#endif  // PDF_ENABLE_XFA_TIFF

bool ProgressiveDecoder::DetectImageType(FXCODEC_IMAGE_TYPE imageType,
                                         CFX_DIBAttribute* pAttribute) {
#ifdef PDF_ENABLE_XFA_TIFF
  if (imageType == FXCODEC_IMAGE_TIFF)
    return TiffDetectImageTypeFromFile(pAttribute);
#endif  // PDF_ENABLE_XFA_TIFF

  size_t size = pdfium::checked_cast<size_t>(
      std::min<FX_FILESIZE>(m_pFile->GetSize(), kBlockSize));
  m_pCodecMemory = pdfium::MakeRetain<CFX_CodecMemory>(size);
  m_offSet = 0;
  if (!m_pFile->ReadBlockAtOffset(m_pCodecMemory->GetBufferSpan().first(size),
                                  m_offSet)) {
    m_status = FXCODEC_STATUS::kError;
    return false;
  }
  m_offSet += size;

  if (imageType == FXCODEC_IMAGE_JPG)
    return JpegDetectImageTypeInBuffer(pAttribute);

#ifdef PDF_ENABLE_XFA_BMP
  if (imageType == FXCODEC_IMAGE_BMP)
    return BmpDetectImageTypeInBuffer(pAttribute);
#endif  // PDF_ENABLE_XFA_BMP

#ifdef PDF_ENABLE_XFA_GIF
  if (imageType == FXCODEC_IMAGE_GIF)
    return GifDetectImageTypeInBuffer();
#endif  // PDF_ENABLE_XFA_GIF

#ifdef PDF_ENABLE_XFA_PNG
  if (imageType == FXCODEC_IMAGE_PNG)
    return PngDetectImageTypeInBuffer(pAttribute);
#endif  // PDF_ENABLE_XFA_PNG

  m_status = FXCODEC_STATUS::kError;
  return false;
}

bool ProgressiveDecoder::ReadMoreData(
    ProgressiveDecoderIface* pModule,
    ProgressiveDecoderIface::Context* pContext,
    FXCODEC_STATUS* err_status) {
  // Check for EOF.
  if (m_offSet >= static_cast<uint32_t>(m_pFile->GetSize()))
    return false;

  // Try to get whatever remains.
  uint32_t dwBytesToFetchFromFile =
      pdfium::checked_cast<uint32_t>(m_pFile->GetSize() - m_offSet);

  // Figure out if the codec stopped processing midway through the buffer.
  size_t dwUnconsumed;
  FX_SAFE_SIZE_T avail_input = pModule->GetAvailInput(pContext);
  if (!avail_input.AssignIfValid(&dwUnconsumed))
    return false;

  if (dwUnconsumed == m_pCodecMemory->GetSize()) {
    // Codec couldn't make any progress against the bytes in the buffer.
    // Increase the buffer size so that there might be enough contiguous
    // bytes to allow whatever operation is having difficulty to succeed.
    dwBytesToFetchFromFile =
        std::min<uint32_t>(dwBytesToFetchFromFile, kBlockSize);
    size_t dwNewSize = m_pCodecMemory->GetSize() + dwBytesToFetchFromFile;
    if (!m_pCodecMemory->TryResize(dwNewSize)) {
      *err_status = FXCODEC_STATUS::kError;
      return false;
    }
  } else {
    // TODO(crbug.com/pdfium/1904): Simplify the `CFX_CodecMemory` API so we
    // don't need to do this awkward dance to free up exactly enough buffer
    // space for the next read.
    size_t dwConsumable = m_pCodecMemory->GetSize() - dwUnconsumed;
    dwBytesToFetchFromFile = pdfium::checked_cast<uint32_t>(
        std::min<size_t>(dwBytesToFetchFromFile, dwConsumable));
    m_pCodecMemory->Consume(dwBytesToFetchFromFile);
    m_pCodecMemory->Seek(dwConsumable - dwBytesToFetchFromFile);
    dwUnconsumed += m_pCodecMemory->GetPosition();
  }

  // Append new data past the bytes not yet processed by the codec.
  if (!m_pFile->ReadBlockAtOffset(m_pCodecMemory->GetBufferSpan().subspan(
                                      dwUnconsumed, dwBytesToFetchFromFile),
                                  m_offSet)) {
    *err_status = FXCODEC_STATUS::kError;
    return false;
  }
  m_offSet += dwBytesToFetchFromFile;
  return pModule->Input(pContext, m_pCodecMemory);
}

FXCODEC_STATUS ProgressiveDecoder::LoadImageInfo(
    RetainPtr<IFX_SeekableReadStream> pFile,
    FXCODEC_IMAGE_TYPE imageType,
    CFX_DIBAttribute* pAttribute,
    bool bSkipImageTypeCheck) {
  DCHECK(pAttribute);

  switch (m_status) {
    case FXCODEC_STATUS::kFrameReady:
    case FXCODEC_STATUS::kFrameToBeContinued:
    case FXCODEC_STATUS::kDecodeReady:
    case FXCODEC_STATUS::kDecodeToBeContinued:
      return FXCODEC_STATUS::kError;
    case FXCODEC_STATUS::kError:
    case FXCODEC_STATUS::kDecodeFinished:
      break;
  }
  m_pFile = std::move(pFile);
  if (!m_pFile) {
    m_status = FXCODEC_STATUS::kError;
    return m_status;
  }
  m_offSet = 0;
  m_SrcWidth = 0;
  m_SrcHeight = 0;
  m_SrcComponents = 0;
  m_SrcBPC = 0;
  m_SrcPassNumber = 0;
  if (imageType != FXCODEC_IMAGE_UNKNOWN &&
      DetectImageType(imageType, pAttribute)) {
    m_imageType = imageType;
    m_status = FXCODEC_STATUS::kFrameReady;
    return m_status;
  }
  // If we got here then the image data does not match the requested decoder.
  // If we're skipping the type check then bail out at this point and return
  // the failed status.
  if (bSkipImageTypeCheck)
    return m_status;

  for (int type = FXCODEC_IMAGE_UNKNOWN + 1; type < FXCODEC_IMAGE_MAX; type++) {
    if (DetectImageType(static_cast<FXCODEC_IMAGE_TYPE>(type), pAttribute)) {
      m_imageType = static_cast<FXCODEC_IMAGE_TYPE>(type);
      m_status = FXCODEC_STATUS::kFrameReady;
      return m_status;
    }
  }
  m_status = FXCODEC_STATUS::kError;
  m_pFile = nullptr;
  return m_status;
}

void ProgressiveDecoder::SetTransMethod() {
  switch (m_pDeviceBitmap->GetFormat()) {
    case FXDIB_Format::kInvalid:
    case FXDIB_Format::k1bppMask:
    case FXDIB_Format::k1bppRgb:
    case FXDIB_Format::k8bppMask:
    case FXDIB_Format::k8bppRgb:
      NOTREACHED_NORETURN();
    case FXDIB_Format::kBgr: {
      switch (m_SrcFormat) {
        case FXCodec_Invalid:
          m_TransMethod = TransformMethod::kInvalid;
          break;
        case FXCodec_8bppGray:
          m_TransMethod = TransformMethod::k8BppGrayToRgbMaybeAlpha;
          break;
        case FXCodec_8bppRgb:
          m_TransMethod = TransformMethod::k8BppRgbToRgbNoAlpha;
          break;
        case FXCodec_Rgb:
        case FXCodec_Rgb32:
        case FXCodec_Argb:
          m_TransMethod = TransformMethod::kRgbMaybeAlphaToRgbMaybeAlpha;
          break;
        case FXCodec_Cmyk:
          m_TransMethod = TransformMethod::kCmykToRgbMaybeAlpha;
          break;
      }
      break;
    }
    case FXDIB_Format::kBgrx:
    case FXDIB_Format::kBgra: {
      switch (m_SrcFormat) {
        case FXCodec_Invalid:
          m_TransMethod = TransformMethod::kInvalid;
          break;
        case FXCodec_8bppGray:
          m_TransMethod = TransformMethod::k8BppGrayToRgbMaybeAlpha;
          break;
        case FXCodec_8bppRgb:
          if (m_pDeviceBitmap->GetFormat() == FXDIB_Format::kBgra) {
            m_TransMethod = TransformMethod::k8BppRgbToArgb;
          } else {
            m_TransMethod = TransformMethod::k8BppRgbToRgbNoAlpha;
          }
          break;
        case FXCodec_Rgb:
        case FXCodec_Rgb32:
          m_TransMethod = TransformMethod::kRgbMaybeAlphaToRgbMaybeAlpha;
          break;
        case FXCodec_Cmyk:
          m_TransMethod = TransformMethod::kCmykToRgbMaybeAlpha;
          break;
        case FXCodec_Argb:
          m_TransMethod = TransformMethod::kArgbToArgb;
          break;
      }
      break;
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul:
      // TODO(crbug.com/355630556): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
#endif
  }
}

void ProgressiveDecoder::ResampleScanline(
    const RetainPtr<CFX_DIBitmap>& pDeviceBitmap,
    int dest_line,
    pdfium::span<uint8_t> src_span,
    FXCodec_Format src_format) {
  uint8_t* src_scan = src_span.data();
  uint8_t* dest_scan = pDeviceBitmap->GetWritableScanline(dest_line).data();
  const int src_bytes_per_pixel = (src_format & 0xff) / 8;
  const int dest_bytes_per_pixel = pDeviceBitmap->GetBPP() / 8;
  for (int dest_col = 0; dest_col < m_SrcWidth; dest_col++) {
    CStretchEngine::PixelWeight* pPixelWeights =
        m_WeightHorz.GetPixelWeight(dest_col);
    switch (m_TransMethod) {
      case TransformMethod::kInvalid:
        return;
      case TransformMethod::k8BppGrayToRgbMaybeAlpha: {
        UNSAFE_TODO({
          uint32_t dest_g = 0;
          for (int j = pPixelWeights->m_SrcStart; j <= pPixelWeights->m_SrcEnd;
               j++) {
            uint32_t pixel_weight =
                pPixelWeights->m_Weights[j - pPixelWeights->m_SrcStart];
            dest_g += pixel_weight * src_scan[j];
          }
          FXSYS_memset(dest_scan, CStretchEngine::PixelFromFixed(dest_g), 3);
          dest_scan += dest_bytes_per_pixel;
          break;
        });
      }
      case TransformMethod::k8BppRgbToRgbNoAlpha: {
        UNSAFE_TODO({
          uint32_t dest_r = 0;
          uint32_t dest_g = 0;
          uint32_t dest_b = 0;
          for (int j = pPixelWeights->m_SrcStart; j <= pPixelWeights->m_SrcEnd;
               j++) {
            uint32_t pixel_weight =
                pPixelWeights->m_Weights[j - pPixelWeights->m_SrcStart];
            uint32_t argb = m_SrcPalette[src_scan[j]];
            dest_r += pixel_weight * FXARGB_R(argb);
            dest_g += pixel_weight * FXARGB_G(argb);
            dest_b += pixel_weight * FXARGB_B(argb);
          }
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_b);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_g);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_r);
          dest_scan += dest_bytes_per_pixel - 3;
          break;
        });
      }
      case TransformMethod::k8BppRgbToArgb: {
#ifdef PDF_ENABLE_XFA_BMP
        if (m_pBmpContext) {
          UNSAFE_TODO({
            uint32_t dest_r = 0;
            uint32_t dest_g = 0;
            uint32_t dest_b = 0;
            for (int j = pPixelWeights->m_SrcStart;
                 j <= pPixelWeights->m_SrcEnd; j++) {
              uint32_t pixel_weight =
                  pPixelWeights->m_Weights[j - pPixelWeights->m_SrcStart];
              uint32_t argb = m_SrcPalette[src_scan[j]];
              dest_r += pixel_weight * FXARGB_R(argb);
              dest_g += pixel_weight * FXARGB_G(argb);
              dest_b += pixel_weight * FXARGB_B(argb);
            }
            *dest_scan++ = CStretchEngine::PixelFromFixed(dest_b);
            *dest_scan++ = CStretchEngine::PixelFromFixed(dest_g);
            *dest_scan++ = CStretchEngine::PixelFromFixed(dest_r);
            *dest_scan++ = 0xFF;
            break;
          });
        }
#endif  // PDF_ENABLE_XFA_BMP
        UNSAFE_TODO({
          uint32_t dest_a = 0;
          uint32_t dest_r = 0;
          uint32_t dest_g = 0;
          uint32_t dest_b = 0;
          for (int j = pPixelWeights->m_SrcStart; j <= pPixelWeights->m_SrcEnd;
               j++) {
            uint32_t pixel_weight =
                pPixelWeights->m_Weights[j - pPixelWeights->m_SrcStart];
            FX_ARGB argb = m_SrcPalette[src_scan[j]];
            dest_a += pixel_weight * FXARGB_A(argb);
            dest_r += pixel_weight * FXARGB_R(argb);
            dest_g += pixel_weight * FXARGB_G(argb);
            dest_b += pixel_weight * FXARGB_B(argb);
          }
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_b);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_g);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_r);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_a);
          break;
        });
      }
      case TransformMethod::kRgbMaybeAlphaToRgbMaybeAlpha: {
        UNSAFE_TODO({
          uint32_t dest_b = 0;
          uint32_t dest_g = 0;
          uint32_t dest_r = 0;
          for (int j = pPixelWeights->m_SrcStart; j <= pPixelWeights->m_SrcEnd;
               j++) {
            uint32_t pixel_weight =
                pPixelWeights->m_Weights[j - pPixelWeights->m_SrcStart];
            const uint8_t* src_pixel = src_scan + j * src_bytes_per_pixel;
            dest_b += pixel_weight * (*src_pixel++);
            dest_g += pixel_weight * (*src_pixel++);
            dest_r += pixel_weight * (*src_pixel);
          }
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_b);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_g);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_r);
          dest_scan += dest_bytes_per_pixel - 3;
          break;
        });
      }
      case TransformMethod::kCmykToRgbMaybeAlpha: {
        UNSAFE_TODO({
          uint32_t dest_b = 0;
          uint32_t dest_g = 0;
          uint32_t dest_r = 0;
          for (int j = pPixelWeights->m_SrcStart; j <= pPixelWeights->m_SrcEnd;
               j++) {
            uint32_t pixel_weight =
                pPixelWeights->m_Weights[j - pPixelWeights->m_SrcStart];
            const uint8_t* src_pixel = src_scan + j * src_bytes_per_pixel;
            FX_RGB_STRUCT<uint8_t> src_rgb =
                AdobeCMYK_to_sRGB1(255 - src_pixel[0], 255 - src_pixel[1],
                                   255 - src_pixel[2], 255 - src_pixel[3]);
            dest_b += pixel_weight * src_rgb.blue;
            dest_g += pixel_weight * src_rgb.green;
            dest_r += pixel_weight * src_rgb.red;
          }
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_b);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_g);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_r);
          dest_scan += dest_bytes_per_pixel - 3;
          break;
        });
      }
      case TransformMethod::kArgbToArgb: {
        UNSAFE_TODO({
          uint32_t dest_alpha = 0;
          uint32_t dest_r = 0;
          uint32_t dest_g = 0;
          uint32_t dest_b = 0;
          for (int j = pPixelWeights->m_SrcStart; j <= pPixelWeights->m_SrcEnd;
               j++) {
            uint32_t pixel_weight =
                pPixelWeights->m_Weights[j - pPixelWeights->m_SrcStart];
            const uint8_t* src_pixel = src_scan + j * src_bytes_per_pixel;
            pixel_weight = pixel_weight * src_pixel[3] / 255;
            dest_b += pixel_weight * (*src_pixel++);
            dest_g += pixel_weight * (*src_pixel++);
            dest_r += pixel_weight * (*src_pixel);
            dest_alpha += pixel_weight;
          }
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_b);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_g);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_r);
          *dest_scan++ = CStretchEngine::PixelFromFixed(dest_alpha * 255);
          break;
        });
      }
    }
  }
}

void ProgressiveDecoder::Resample(const RetainPtr<CFX_DIBitmap>& pDeviceBitmap,
                                  int32_t src_line,
                                  uint8_t* src_scan,
                                  FXCodec_Format src_format) {
  if (src_line < 0 || src_line >= m_SrcHeight) {
    return;
  }

  ResampleScanline(pDeviceBitmap, src_line, m_DecodeBuf, src_format);
}

FXDIB_Format ProgressiveDecoder::GetBitmapFormat() const {
  switch (m_imageType) {
    case FXCODEC_IMAGE_JPG:
#ifdef PDF_ENABLE_XFA_BMP
    case FXCODEC_IMAGE_BMP:
#endif  // PDF_ENABLE_XFA_BMP
      return GetBitsPerPixel() <= 24 ? FXDIB_Format::kBgr : FXDIB_Format::kBgrx;
#ifdef PDF_ENABLE_XFA_PNG
    case FXCODEC_IMAGE_PNG:
#endif  // PDF_ENABLE_XFA_PNG
#ifdef PDF_ENABLE_XFA_TIFF
    case FXCODEC_IMAGE_TIFF:
#endif  // PDF_ENABLE_XFA_TIFF
    default:
      // TODO(crbug.com/355630556): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      return FXDIB_Format::kBgra;
  }
}

std::pair<FXCODEC_STATUS, size_t> ProgressiveDecoder::GetFrames() {
  if (!(m_status == FXCODEC_STATUS::kFrameReady ||
        m_status == FXCODEC_STATUS::kFrameToBeContinued)) {
    return {FXCODEC_STATUS::kError, 0};
  }

  switch (m_imageType) {
#ifdef PDF_ENABLE_XFA_BMP
    case FXCODEC_IMAGE_BMP:
#endif  // PDF_ENABLE_XFA_BMP
    case FXCODEC_IMAGE_JPG:
#ifdef PDF_ENABLE_XFA_PNG
    case FXCODEC_IMAGE_PNG:
#endif  // PDF_ENABLE_XFA_PNG
#ifdef PDF_ENABLE_XFA_TIFF
    case FXCODEC_IMAGE_TIFF:
#endif  // PDF_ENABLE_XFA_TIFF
      m_FrameNumber = 1;
      m_status = FXCODEC_STATUS::kDecodeReady;
      return {m_status, 1};
#ifdef PDF_ENABLE_XFA_GIF
    case FXCODEC_IMAGE_GIF: {
      while (true) {
        GifDecoder::Status readResult;
        std::tie(readResult, m_FrameNumber) =
            GifDecoder::LoadFrameInfo(m_pGifContext.get());
        while (readResult == GifDecoder::Status::kUnfinished) {
          FXCODEC_STATUS error_status = FXCODEC_STATUS::kError;
          if (!GifReadMoreData(&error_status))
            return {error_status, 0};

          std::tie(readResult, m_FrameNumber) =
              GifDecoder::LoadFrameInfo(m_pGifContext.get());
        }
        if (readResult == GifDecoder::Status::kSuccess) {
          m_status = FXCODEC_STATUS::kDecodeReady;
          return {m_status, m_FrameNumber};
        }
        m_pGifContext = nullptr;
        m_status = FXCODEC_STATUS::kError;
        return {m_status, 0};
      }
    }
#endif  // PDF_ENABLE_XFA_GIF
    default:
      return {FXCODEC_STATUS::kError, 0};
  }
}

FXCODEC_STATUS ProgressiveDecoder::StartDecode(RetainPtr<CFX_DIBitmap> bitmap) {
  CHECK(bitmap);
  CHECK_EQ(bitmap->GetWidth(), m_SrcWidth);
  CHECK_EQ(bitmap->GetHeight(), m_SrcHeight);
  CHECK_GT(m_SrcWidth, 0);
  CHECK_GT(m_SrcHeight, 0);

  const FXDIB_Format format = bitmap->GetFormat();
  CHECK(format == FXDIB_Format::kBgra || format == FXDIB_Format::kBgr ||
        format == FXDIB_Format::kBgrx);

  if (m_status != FXCODEC_STATUS::kDecodeReady) {
    return FXCODEC_STATUS::kError;
  }

  if (m_FrameNumber == 0) {
    return FXCODEC_STATUS::kError;
  }

  if (bitmap->GetWidth() > 65535 || bitmap->GetHeight() > 65535) {
    return FXCODEC_STATUS::kError;
  }

  m_FrameCur = 0;
  m_pDeviceBitmap = std::move(bitmap);
  switch (m_imageType) {
#ifdef PDF_ENABLE_XFA_BMP
    case FXCODEC_IMAGE_BMP:
      return BmpStartDecode();
#endif  // PDF_ENABLE_XFA_BMP
#ifdef PDF_ENABLE_XFA_GIF
    case FXCODEC_IMAGE_GIF:
      return GifStartDecode();
#endif  // PDF_ENABLE_XFA_GIF
    case FXCODEC_IMAGE_JPG:
      return JpegStartDecode();
#ifdef PDF_ENABLE_XFA_PNG
    case FXCODEC_IMAGE_PNG:
      return PngStartDecode();
#endif  // PDF_ENABLE_XFA_PNG
#ifdef PDF_ENABLE_XFA_TIFF
    case FXCODEC_IMAGE_TIFF:
      m_status = FXCODEC_STATUS::kDecodeToBeContinued;
      return m_status;
#endif  // PDF_ENABLE_XFA_TIFF
    default:
      return FXCODEC_STATUS::kError;
  }
}

FXCODEC_STATUS ProgressiveDecoder::ContinueDecode() {
  if (m_status != FXCODEC_STATUS::kDecodeToBeContinued)
    return FXCODEC_STATUS::kError;

  switch (m_imageType) {
    case FXCODEC_IMAGE_JPG:
      return JpegContinueDecode();
#ifdef PDF_ENABLE_XFA_BMP
    case FXCODEC_IMAGE_BMP:
      return BmpContinueDecode();
#endif  // PDF_ENABLE_XFA_BMP
#ifdef PDF_ENABLE_XFA_GIF
    case FXCODEC_IMAGE_GIF:
      return GifContinueDecode();
#endif  // PDF_ENABLE_XFA_GIF
#ifdef PDF_ENABLE_XFA_PNG
    case FXCODEC_IMAGE_PNG:
      return PngContinueDecode();
#endif  // PDF_ENABLE_XFA_PNG
#ifdef PDF_ENABLE_XFA_TIFF
    case FXCODEC_IMAGE_TIFF:
      return TiffContinueDecode();
#endif  // PDF_ENABLE_XFA_TIFF
    default:
      return FXCODEC_STATUS::kError;
  }
}

}  // namespace fxcodec
