// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXGE_DIB_CFX_DIBITMAP_H_
#define CORE_FXGE_DIB_CFX_DIBITMAP_H_

#include <stddef.h>
#include <stdint.h>

#include <optional>

#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/fx_memory_wrappers.h"
#include "core/fxcrt/maybe_owned.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/span_util.h"
#include "core/fxge/dib/cfx_dibbase.h"
#include "core/fxge/dib/fx_dib.h"

class CFX_DIBitmap final : public CFX_DIBBase {
 public:
  struct PitchAndSize {
    uint32_t pitch;
    uint32_t size;
  };

#if defined(PDF_USE_SKIA)
  // Scoper that conditionally pre-multiplies a bitmap in the ctor and
  // un-premultiplies in the dtor if pre-multiplication was required.
  class ScopedPremultiplier {
   public:
    // `bitmap` must start out un-premultiplied.
    // ScopedPremultiplier is a no-op if `do_premultiply` is false.
    ScopedPremultiplier(RetainPtr<CFX_DIBitmap> bitmap, bool do_premultiply);
    ~ScopedPremultiplier();

   private:
    RetainPtr<CFX_DIBitmap> const bitmap_;
    const bool do_premultiply_;
  };
#endif  // defined(PDF_USE_SKIA)

  CONSTRUCT_VIA_MAKE_RETAIN;

  [[nodiscard]] bool Create(int width, int height, FXDIB_Format format);
  [[nodiscard]] bool Create(int width,
                            int height,
                            FXDIB_Format format,
                            uint8_t* pBuffer,
                            uint32_t pitch);

  bool Copy(RetainPtr<const CFX_DIBBase> source);

  // CFX_DIBBase
  pdfium::span<const uint8_t> GetScanline(int line) const override;
  size_t GetEstimatedImageMemoryBurden() const override;
#if BUILDFLAG(IS_WIN) || defined(PDF_USE_SKIA)
  RetainPtr<const CFX_DIBitmap> RealizeIfNeeded() const override;
#endif

  pdfium::span<const uint8_t> GetBuffer() const;
  pdfium::span<uint8_t> GetWritableBuffer() {
    pdfium::span<const uint8_t> src = GetBuffer();
    // SAFETY: const_cast<>() doesn't change size.
    return UNSAFE_BUFFERS(
        pdfium::make_span(const_cast<uint8_t*>(src.data()), src.size()));
  }

  // Note that the returned scanline includes unused space at the end, if any.
  pdfium::span<uint8_t> GetWritableScanline(int line) {
    pdfium::span<const uint8_t> src = GetScanline(line);
    // SAFETY: const_cast<>() doesn't change size.
    return UNSAFE_BUFFERS(
        pdfium::make_span(const_cast<uint8_t*>(src.data()), src.size()));
  }

  // Note that the returned scanline does not include unused space at the end,
  // if any.
  template <typename T>
  pdfium::span<T> GetWritableScanlineAs(int line) {
    return fxcrt::reinterpret_span<T>(GetWritableScanline(line))
        .first(GetWidth());
  }

  void TakeOver(RetainPtr<CFX_DIBitmap>&& pSrcBitmap);
  bool ConvertFormat(FXDIB_Format format);
  void Clear(uint32_t color);

#if defined(PDF_USE_SKIA)
  uint32_t GetPixelForTesting(int x, int y) const;
#endif  // defined(PDF_USE_SKIA)

  // Requires `this` to be of format `FXDIB_Format::kBgra`.
  void SetRedFromAlpha();

  // Requires `this` to be of format `FXDIB_Format::kBgra`.
  void SetUniformOpaqueAlpha();

  // TODO(crbug.com/pdfium/2007): Migrate callers to `CFX_RenderDevice`.
  bool MultiplyAlpha(float alpha);
  bool MultiplyAlphaMask(RetainPtr<const CFX_DIBitmap> mask);

  bool TransferBitmap(int width,
                      int height,
                      RetainPtr<const CFX_DIBBase> source,
                      int src_left,
                      int src_top);

  bool CompositeBitmap(int dest_left,
                       int dest_top,
                       int width,
                       int height,
                       RetainPtr<const CFX_DIBBase> source,
                       int src_left,
                       int src_top,
                       BlendMode blend_type,
                       const CFX_AggClipRgn* pClipRgn,
                       bool bRgbByteOrder);

  bool CompositeMask(int dest_left,
                     int dest_top,
                     int width,
                     int height,
                     RetainPtr<const CFX_DIBBase> pMask,
                     uint32_t color,
                     int src_left,
                     int src_top,
                     BlendMode blend_type,
                     const CFX_AggClipRgn* pClipRgn,
                     bool bRgbByteOrder);

  void CompositeOneBPPMask(int dest_left,
                           int dest_top,
                           int width,
                           int height,
                           RetainPtr<const CFX_DIBBase> source,
                           int src_left,
                           int src_top);

  bool CompositeRect(int dest_left,
                     int dest_top,
                     int width,
                     int height,
                     uint32_t color);

  bool ConvertColorScale(uint32_t forecolor, uint32_t backcolor);

  // |width| and |height| must be greater than 0.
  // |format| must have a valid bits per pixel count.
  // If |pitch| is zero, then the actual pitch will be calculated based on
  // |width| and |format|.
  // If |pitch| is non-zero, then that be used as the actual pitch.
  // The actual pitch will be used to calculate the size.
  // Returns the calculated pitch and size on success, or nullopt on failure.
  static std::optional<PitchAndSize> CalculatePitchAndSize(int width,
                                                           int height,
                                                           FXDIB_Format format,
                                                           uint32_t pitch);

#if defined(PDF_USE_SKIA)
  // Converts from/to un-pre-multiplied alpha if necessary.
  void PreMultiply();
  void UnPreMultiply();
#endif  // defined(PDF_USE_SKIA)

 private:
  enum class Channel : uint8_t { kRed, kAlpha };

  CFX_DIBitmap();
  CFX_DIBitmap(const CFX_DIBitmap& src);
  ~CFX_DIBitmap() override;

  void ConvertBGRColorScale(uint32_t forecolor, uint32_t backcolor);
  bool TransferWithUnequalFormats(FXDIB_Format dest_format,
                                  int dest_left,
                                  int dest_top,
                                  int width,
                                  int height,
                                  RetainPtr<const CFX_DIBBase> source,
                                  int src_left,
                                  int src_top);
  void TransferWithMultipleBPP(int dest_left,
                               int dest_top,
                               int width,
                               int height,
                               RetainPtr<const CFX_DIBBase> source,
                               int src_left,
                               int src_top);
  void TransferEqualFormatsOneBPP(int dest_left,
                                  int dest_top,
                                  int width,
                                  int height,
                                  RetainPtr<const CFX_DIBBase> source,
                                  int src_left,
                                  int src_top);

  MaybeOwned<uint8_t, FxFreeDeleter> m_pBuffer;
};

#endif  // CORE_FXGE_DIB_CFX_DIBITMAP_H_
