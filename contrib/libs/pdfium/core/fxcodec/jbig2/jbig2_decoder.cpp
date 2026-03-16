// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcodec/jbig2/jbig2_decoder.h"

#include "core/fxcodec/jbig2/JBig2_Context.h"
#include "core/fxcodec/jbig2/JBig2_DocumentContext.h"
#include "core/fxcrt/fx_2d_size.h"
#include "core/fxcrt/span_util.h"
#include "core/fxcrt/stl_util.h"

namespace fxcodec {

namespace {

FXCODEC_STATUS Decode(Jbig2Context* pJbig2Context, bool decode_success) {
  FXCODEC_STATUS status = pJbig2Context->m_pContext->GetProcessingStatus();
  if (status != FXCODEC_STATUS::kDecodeFinished) {
    return status;
  }
  pJbig2Context->m_pContext.reset();
  if (!decode_success) {
    return FXCODEC_STATUS::kError;
  }
  uint32_t byte_size = pJbig2Context->m_height * pJbig2Context->m_dest_pitch;
  pdfium::span<uint8_t> nonraw_span(pJbig2Context->m_dest_buf.first(byte_size));
  auto dword_span = fxcrt::reinterpret_span<uint32_t>(nonraw_span);
  for (auto& pix : dword_span) {
    pix = ~pix;
  }
  return FXCODEC_STATUS::kDecodeFinished;
}

}  // namespace

Jbig2Context::Jbig2Context() = default;

Jbig2Context::~Jbig2Context() = default;

// static
FXCODEC_STATUS Jbig2Decoder::StartDecode(
    Jbig2Context* pJbig2Context,
    JBig2_DocumentContext* pJBig2DocumentContext,
    uint32_t width,
    uint32_t height,
    pdfium::span<const uint8_t> src_span,
    uint64_t src_key,
    pdfium::span<const uint8_t> global_span,
    uint64_t global_key,
    pdfium::span<uint8_t> dest_buf,
    uint32_t dest_pitch,
    PauseIndicatorIface* pPause) {
  pJbig2Context->m_width = width;
  pJbig2Context->m_height = height;
  pJbig2Context->m_pSrcSpan = src_span;
  pJbig2Context->m_nSrcKey = src_key;
  pJbig2Context->m_pGlobalSpan = global_span;
  pJbig2Context->m_nGlobalKey = global_key;
  pJbig2Context->m_dest_buf = dest_buf;
  pJbig2Context->m_dest_pitch = dest_pitch;
  fxcrt::Fill(dest_buf.first(Fx2DSizeOrDie(height, dest_pitch)), 0);
  pJbig2Context->m_pContext =
      CJBig2_Context::Create(global_span, global_key, src_span, src_key,
                             pJBig2DocumentContext->GetSymbolDictCache());
  bool succeeded = pJbig2Context->m_pContext->GetFirstPage(
      dest_buf, width, height, dest_pitch, pPause);
  return Decode(pJbig2Context, succeeded);
}

// static
FXCODEC_STATUS Jbig2Decoder::ContinueDecode(Jbig2Context* pJbig2Context,
                                            PauseIndicatorIface* pPause) {
  bool succeeded = pJbig2Context->m_pContext->Continue(pPause);
  return Decode(pJbig2Context, succeeded);
}

}  // namespace fxcodec
