// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/jpeg/jpeg_progressive_decoder.h"

#include <optional>
#include <utility>

#include "core/fxcodec/cfx_codec_memory.h"
#include "core/fxcodec/fx_codec.h"
#include "core/fxcodec/jpeg/jpeg_common.h"
#include "core/fxcodec/scanlinedecoder.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_safe_types.h"
#include "core/fxcrt/ptr_util.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/fx_dib.h"

class CJpegContext final : public ProgressiveDecoderIface::Context {
 public:
  CJpegContext();
  ~CJpegContext() override;

  jmp_buf& GetJumpMark() { return m_JumpMark; }

  jmp_buf m_JumpMark;
  jpeg_decompress_struct m_Info = {};
  jpeg_error_mgr m_ErrMgr = {};
  jpeg_source_mgr m_SrcMgr = {};
  unsigned int m_SkipSize = 0;
};

extern "C" {

static void error_fatal(j_common_ptr cinfo) {
  auto* pContext = reinterpret_cast<CJpegContext*>(cinfo->client_data);
  longjmp(pContext->m_JumpMark, -1);
}

static void src_skip_data(jpeg_decompress_struct* cinfo, long num) {
  if (cinfo->src->bytes_in_buffer < static_cast<size_t>(num)) {
    auto* pContext = reinterpret_cast<CJpegContext*>(cinfo->client_data);
    pContext->m_SkipSize = (unsigned int)(num - cinfo->src->bytes_in_buffer);
    cinfo->src->bytes_in_buffer = 0;
  } else {
    // SAFETY: required from library during callback.
    UNSAFE_BUFFERS(cinfo->src->next_input_byte += num);
    cinfo->src->bytes_in_buffer -= num;
  }
}

}  // extern "C"

static void JpegLoadAttribute(const jpeg_decompress_struct& info,
                              CFX_DIBAttribute* pAttribute) {
  pAttribute->m_nXDPI = info.X_density;
  pAttribute->m_nYDPI = info.Y_density;
  pAttribute->m_wDPIUnit =
      static_cast<CFX_DIBAttribute::ResUnit>(info.density_unit);
}

CJpegContext::CJpegContext() {
  m_Info.client_data = this;
  m_Info.err = &m_ErrMgr;

  m_ErrMgr.error_exit = error_fatal;
  m_ErrMgr.emit_message = error_do_nothing_int;
  m_ErrMgr.output_message = error_do_nothing;
  m_ErrMgr.format_message = error_do_nothing_char;
  m_ErrMgr.reset_error_mgr = error_do_nothing;

  m_SrcMgr.init_source = src_do_nothing;
  m_SrcMgr.term_source = src_do_nothing;
  m_SrcMgr.skip_input_data = src_skip_data;
  m_SrcMgr.fill_input_buffer = src_fill_buffer;
  m_SrcMgr.resync_to_restart = src_resync;
}

CJpegContext::~CJpegContext() {
  jpeg_destroy_decompress(&m_Info);
}

namespace fxcodec {

namespace {

JpegProgressiveDecoder* g_jpeg_decoder = nullptr;

}  // namespace

// static
void JpegProgressiveDecoder::InitializeGlobals() {
  CHECK(!g_jpeg_decoder);
  g_jpeg_decoder = new JpegProgressiveDecoder();
}

// static
void JpegProgressiveDecoder::DestroyGlobals() {
  delete g_jpeg_decoder;
  g_jpeg_decoder = nullptr;
}

// static
JpegProgressiveDecoder* JpegProgressiveDecoder::GetInstance() {
  return g_jpeg_decoder;
}

// static
std::unique_ptr<ProgressiveDecoderIface::Context>
JpegProgressiveDecoder::Start() {
  // Use ordinary pointer until past the possibility of a longjump.
  auto* pContext = new CJpegContext();
  if (setjmp(pContext->m_JumpMark) == -1) {
    delete pContext;
    return nullptr;
  }

  jpeg_create_decompress(&pContext->m_Info);
  pContext->m_Info.src = &pContext->m_SrcMgr;
  pContext->m_SkipSize = 0;
  return pdfium::WrapUnique(pContext);
}

// static
jmp_buf& JpegProgressiveDecoder::GetJumpMark(Context* pContext) {
  return static_cast<CJpegContext*>(pContext)->GetJumpMark();
}

// static
int JpegProgressiveDecoder::ReadHeader(Context* pContext,
                                       int* width,
                                       int* height,
                                       int* nComps,
                                       CFX_DIBAttribute* pAttribute) {
  DCHECK(pAttribute);

  auto* ctx = static_cast<CJpegContext*>(pContext);
  int ret = jpeg_read_header(&ctx->m_Info, TRUE);
  if (ret == JPEG_SUSPENDED)
    return 2;
  if (ret != JPEG_HEADER_OK)
    return 1;

  *width = ctx->m_Info.image_width;
  *height = ctx->m_Info.image_height;
  *nComps = ctx->m_Info.num_components;
  JpegLoadAttribute(ctx->m_Info, pAttribute);
  return 0;
}

// static
bool JpegProgressiveDecoder::StartScanline(Context* pContext) {
  auto* ctx = static_cast<CJpegContext*>(pContext);
  ctx->m_Info.scale_denom = 1;
  return !!jpeg_start_decompress(&ctx->m_Info);
}

// static
bool JpegProgressiveDecoder::ReadScanline(Context* pContext,
                                          unsigned char* dest_buf) {
  auto* ctx = static_cast<CJpegContext*>(pContext);
  unsigned int nlines = jpeg_read_scanlines(&ctx->m_Info, &dest_buf, 1);
  return nlines == 1;
}

FX_FILESIZE JpegProgressiveDecoder::GetAvailInput(Context* pContext) const {
  auto* ctx = static_cast<CJpegContext*>(pContext);
  return static_cast<FX_FILESIZE>(ctx->m_SrcMgr.bytes_in_buffer);
}

bool JpegProgressiveDecoder::Input(Context* pContext,
                                   RetainPtr<CFX_CodecMemory> codec_memory) {
  pdfium::span<uint8_t> src_buf = codec_memory->GetUnconsumedSpan();
  auto* ctx = static_cast<CJpegContext*>(pContext);
  if (ctx->m_SkipSize) {
    if (ctx->m_SkipSize > src_buf.size()) {
      ctx->m_SrcMgr.bytes_in_buffer = 0;
      ctx->m_SkipSize -= src_buf.size();
      return true;
    }
    src_buf = src_buf.subspan(ctx->m_SkipSize);
    ctx->m_SkipSize = 0;
  }
  ctx->m_SrcMgr.next_input_byte = src_buf.data();
  ctx->m_SrcMgr.bytes_in_buffer = src_buf.size();
  return true;
}

JpegProgressiveDecoder::JpegProgressiveDecoder() = default;

JpegProgressiveDecoder::~JpegProgressiveDecoder() = default;

}  // namespace fxcodec
