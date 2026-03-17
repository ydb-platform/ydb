// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_transferfuncdib.h"

#include <utility>

#include "build/build_config.h"
#include "core/fpdfapi/page/cpdf_transferfunc.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/zip.h"
#include "core/fxge/calculate_pitch.h"

#if defined(PDF_USE_SKIA)
#include "core/fxcrt/notreached.h"
#endif

namespace {

CFX_DIBBase::kPlatformRGBStruct MakePlatformRGBStruct(uint8_t red,
                                                      uint8_t green,
                                                      uint8_t blue) {
  // Note that the return value may have an alpha value that will be set to 0.
  return {
      .blue = blue,
      .green = green,
      .red = red,
  };
}

}  // namespace

CPDF_TransferFuncDIB::CPDF_TransferFuncDIB(
    RetainPtr<const CFX_DIBBase> src,
    RetainPtr<CPDF_TransferFunc> transfer_func)
    : src_(std::move(src)),
      transfer_func_(std::move(transfer_func)),
      r_samples_(transfer_func_->GetSamplesR()),
      g_samples_(transfer_func_->GetSamplesG()),
      b_samples_(transfer_func_->GetSamplesB()) {
  SetWidth(src_->GetWidth());
  SetHeight(src_->GetHeight());
  SetFormat(GetDestFormat());
  SetPitch(fxge::CalculatePitch32OrDie(GetBPP(), GetWidth()));
  scanline_.resize(GetPitch());
  CHECK(!HasPalette());
}

CPDF_TransferFuncDIB::~CPDF_TransferFuncDIB() = default;

FXDIB_Format CPDF_TransferFuncDIB::GetDestFormat() const {
  if (src_->IsMaskFormat()) {
    return FXDIB_Format::k8bppMask;
  }

  if (src_->IsAlphaFormat()) {
    // TODO(crbug.com/355676038): Consider adding support for
    // `FXDIB_Format::kBgraPremul`
    return FXDIB_Format::kBgra;
  }

  return CFX_DIBBase::kPlatformRGBFormat;
}

void CPDF_TransferFuncDIB::TranslateScanline(
    pdfium::span<const uint8_t> src_span) const {
  auto scanline_span = pdfium::make_span(scanline_);
  switch (src_->GetFormat()) {
    case FXDIB_Format::kInvalid: {
      break;
    }
    case FXDIB_Format::k1bppRgb: {
      const auto color0 =
          MakePlatformRGBStruct(r_samples_[0], g_samples_[0], b_samples_[0]);
      const auto color1 = MakePlatformRGBStruct(
          r_samples_[255], g_samples_[255], b_samples_[255]);
      auto dest = fxcrt::reinterpret_span<kPlatformRGBStruct>(scanline_span);
      for (int i = 0; i < GetWidth(); i++) {
        const bool is_on = (src_span[i / 8] & (1 << (7 - i % 8)));
        dest[i] = is_on ? color1 : color0;
      }
      break;
    }
    case FXDIB_Format::k1bppMask: {
      const int m0 = r_samples_[0];
      const int m1 = r_samples_[255];
      for (int i = 0; i < GetWidth(); i++) {
        const bool is_on = (src_span[i / 8] & (1 << (7 - i % 8)));
        scanline_[i] = is_on ? m1 : m0;
      }
      break;
    }
    case FXDIB_Format::k8bppRgb: {
      pdfium::span<const uint32_t> src_palette = src_->GetPaletteSpan();
      auto dest = fxcrt::reinterpret_span<kPlatformRGBStruct>(scanline_span);
      auto zip = fxcrt::Zip(src_span.first(GetWidth()), dest);
      if (src_->HasPalette()) {
        for (auto [input, output] : zip) {
          const FX_ARGB src_argb = src_palette[input];
          output = MakePlatformRGBStruct(r_samples_[FXARGB_B(src_argb)],
                                         g_samples_[FXARGB_G(src_argb)],
                                         b_samples_[FXARGB_R(src_argb)]);
        }
      } else {
        for (auto [input, output] : zip) {
          output = MakePlatformRGBStruct(r_samples_[input], g_samples_[input],
                                         b_samples_[input]);
        }
      }
      break;
    }
    case FXDIB_Format::k8bppMask: {
      for (auto [input, output] :
           fxcrt::Zip(src_span.first(GetWidth()), scanline_span)) {
        output = r_samples_[input];
      }
      break;
    }
    case FXDIB_Format::kBgr: {
      auto src =
          fxcrt::reinterpret_span<const FX_BGR_STRUCT<uint8_t>>(src_span);
      auto dest = fxcrt::reinterpret_span<kPlatformRGBStruct>(scanline_span);
      for (auto [input, output] : fxcrt::Zip(src.first(GetWidth()), dest)) {
        output = MakePlatformRGBStruct(r_samples_[input.red],
                                       g_samples_[input.green],
                                       b_samples_[input.blue]);
      }
      break;
    }
    case FXDIB_Format::kBgrx: {
      auto src =
          fxcrt::reinterpret_span<const FX_BGRA_STRUCT<uint8_t>>(src_span);
      auto dest = fxcrt::reinterpret_span<kPlatformRGBStruct>(scanline_span);
      for (auto [input, output] : fxcrt::Zip(src.first(GetWidth()), dest)) {
        output = MakePlatformRGBStruct(r_samples_[input.red],
                                       g_samples_[input.green],
                                       b_samples_[input.blue]);
      }
      break;
    }
    case FXDIB_Format::kBgra: {
      auto src =
          fxcrt::reinterpret_span<const FX_BGRA_STRUCT<uint8_t>>(src_span);
      auto dest =
          fxcrt::reinterpret_span<FX_BGRA_STRUCT<uint8_t>>(scanline_span);
      for (auto [input, output] : fxcrt::Zip(src.first(GetWidth()), dest)) {
        output = {
            .blue = b_samples_[input.blue],
            .green = g_samples_[input.green],
            .red = r_samples_[input.red],
            .alpha = input.alpha,
        };
      }
      break;
    }
#if defined(PDF_USE_SKIA)
    case FXDIB_Format::kBgraPremul: {
      // TODO(crbug.com/355676038): Consider adding support for
      // `FXDIB_Format::kBgraPremul`
      NOTREACHED_NORETURN();
    }
#endif
  }
}

pdfium::span<const uint8_t> CPDF_TransferFuncDIB::GetScanline(int line) const {
  TranslateScanline(src_->GetScanline(line));
  return scanline_;
}
