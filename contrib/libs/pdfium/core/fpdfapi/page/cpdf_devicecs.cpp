// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/page/cpdf_devicecs.h"

#include <algorithm>

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfapi/parser/cpdf_dictionary.h"
#include "core/fpdfapi/parser/cpdf_document.h"
#include "core/fpdfapi/parser/cpdf_stream_acc.h"
#include "core/fpdfapi/parser/cpdf_string.h"
#include "core/fxcodec/fx_codec.h"
#include "core/fxcrt/check.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/notreached.h"
#include "core/fxge/dib/cfx_cmyk_to_srgb.h"

namespace {

float NormalizeChannel(float fVal) {
  return std::clamp(fVal, 0.0f, 1.0f);
}

}  // namespace

CPDF_DeviceCS::CPDF_DeviceCS(Family family) : CPDF_ColorSpace(family) {
  DCHECK(family == Family::kDeviceGray || family == Family::kDeviceRGB ||
         family == Family::kDeviceCMYK);
  SetComponentsForStockCS(ComponentsForFamily(GetFamily()));
}

CPDF_DeviceCS::~CPDF_DeviceCS() = default;

uint32_t CPDF_DeviceCS::v_Load(CPDF_Document* pDoc,
                               const CPDF_Array* pArray,
                               std::set<const CPDF_Object*>* pVisited) {
  // Unlike other classes that inherit from CPDF_ColorSpace, CPDF_DeviceCS is
  // never loaded by CPDF_ColorSpace.
  NOTREACHED_NORETURN();
}

std::optional<FX_RGB_STRUCT<float>> CPDF_DeviceCS::GetRGB(
    pdfium::span<const float> pBuf) const {
  switch (GetFamily()) {
    case Family::kDeviceGray: {
      const float pix = NormalizeChannel(pBuf.front());
      return FX_RGB_STRUCT<float>{pix, pix, pix};
    }
    case Family::kDeviceRGB: {
      const auto& rgb =
          fxcrt::reinterpret_span<const FX_RGB_STRUCT<float>>(pBuf).front();
      return FX_RGB_STRUCT<float>{
          NormalizeChannel(rgb.red),
          NormalizeChannel(rgb.green),
          NormalizeChannel(rgb.blue),
      };
    }
    case Family::kDeviceCMYK: {
      const auto& cmyk =
          fxcrt::reinterpret_span<const FX_CMYK_STRUCT<float>>(pBuf).front();
      if (IsStdConversionEnabled()) {
        return FX_RGB_STRUCT<float>{
            1.0f - std::min(1.0f, cmyk.cyan + cmyk.key),
            1.0f - std::min(1.0f, cmyk.magenta + cmyk.key),
            1.0f - std::min(1.0f, cmyk.yellow + cmyk.key),
        };
      }
      return AdobeCMYK_to_sRGB(
          NormalizeChannel(cmyk.cyan), NormalizeChannel(cmyk.magenta),
          NormalizeChannel(cmyk.yellow), NormalizeChannel(cmyk.key));
    }
    default:
      NOTREACHED_NORETURN();
  }
}

void CPDF_DeviceCS::TranslateImageLine(pdfium::span<uint8_t> dest_span,
                                       pdfium::span<const uint8_t> src_span,
                                       int pixels,
                                       int image_width,
                                       int image_height,
                                       bool bTransMask) const {
  auto rgb_out = fxcrt::reinterpret_span<FX_RGB_STRUCT<uint8_t>>(dest_span);
  switch (GetFamily()) {
    case Family::kDeviceGray:
      CHECK(!bTransMask);  // bTransMask only allowed for CMYK colorspaces.
      // Compiler can't conclude src/dest don't overlap, avoid interleaved
      // loads and stores by not using an auto& reference here.
      for (const auto pix : src_span.first(pixels)) {
        rgb_out.front().red = pix;
        rgb_out.front().green = pix;
        rgb_out.front().blue = pix;
        rgb_out = rgb_out.subspan(1);
      }
      break;
    case Family::kDeviceRGB:
      CHECK(!bTransMask);  // bTransMask only allowed for CMYK colorspaces.
      fxcodec::ReverseRGB(dest_span, src_span, pixels);
      break;
    case Family::kDeviceCMYK: {
      auto cmyk_in =
          fxcrt::reinterpret_span<const FX_CMYK_STRUCT<uint8_t>>(src_span);
      if (bTransMask) {
        // Compiler can't conclude src/dest don't overlap, avoid interleaved
        // loads and stores by not using an auto& reference here.
        for (const auto cmyk : cmyk_in.first(pixels)) {
          const int k = 255 - cmyk.key;
          rgb_out.front().red = ((255 - cmyk.cyan) * k) / 255;
          rgb_out.front().green = ((255 - cmyk.magenta) * k) / 255;
          rgb_out.front().blue = ((255 - cmyk.yellow) * k) / 255;
          rgb_out = rgb_out.subspan(1);
        }
        break;
      }
      if (IsStdConversionEnabled()) {
        // Compiler can't conclude src/dest don't overlap, avoid interleaved
        // loads and stores by not using am auto& reference here,
        for (const auto cmyk : cmyk_in.first(pixels)) {
          const uint8_t k = cmyk.key;
          rgb_out.front().blue = 255 - std::min(255, cmyk.cyan + k);
          rgb_out.front().green = 255 - std::min(255, cmyk.magenta + k);
          rgb_out.front().red = 255 - std::min(255, cmyk.yellow + k);
          rgb_out = rgb_out.subspan(1);
        }
        break;
      }
      for (const auto& cmyk : cmyk_in.first(pixels)) {
        // TODO(tsepez): maybe this is a FX_BGR_STRUCT in reality?
        FX_RGB_STRUCT<uint8_t> rgb =
            AdobeCMYK_to_sRGB1(cmyk.cyan, cmyk.magenta, cmyk.yellow, cmyk.key);
        rgb_out.front().red = rgb.blue;
        rgb_out.front().green = rgb.green;
        rgb_out.front().blue = rgb.red;
        rgb_out = rgb_out.subspan(1);
      }
      break;
    }
    default:
      NOTREACHED_NORETURN();
  }
}
