// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxge/calculate_pitch.h"

#include "core/fxcrt/fx_safe_types.h"
#include "core/fxge/dib/fx_dib.h"

namespace fxge {
namespace {

FX_SAFE_UINT32 CalculatePitch8Safely(uint32_t bpc,
                                     uint32_t components,
                                     int width) {
  FX_SAFE_UINT32 pitch = bpc;
  pitch *= components;
  pitch *= width;
  pitch += 7;
  pitch /= 8;
  return pitch;
}

FX_SAFE_UINT32 CalculatePitch32Safely(int bpp, int width) {
  FX_SAFE_UINT32 pitch = bpp;
  pitch *= width;
  pitch += 31;
  pitch /= 32;  // quantized to number of 32-bit words.
  pitch *= 4;   // and then back to bytes, (not just /8 in one step).
  return pitch;
}

}  // namespace

uint32_t CalculatePitch8OrDie(uint32_t bits_per_component,
                              uint32_t components_per_pixel,
                              int width_in_pixels) {
  return CalculatePitch8Safely(bits_per_component, components_per_pixel,
                               width_in_pixels)
      .ValueOrDie();
}

uint32_t CalculatePitch32OrDie(int bits_per_pixel, int width_in_pixels) {
  return CalculatePitch32Safely(bits_per_pixel, width_in_pixels).ValueOrDie();
}

std::optional<uint32_t> CalculatePitch8(uint32_t bits_per_component,
                                        uint32_t components,
                                        int width_in_pixels) {
  FX_SAFE_UINT32 pitch =
      CalculatePitch8Safely(bits_per_component, components, width_in_pixels);
  if (!pitch.IsValid())
    return std::nullopt;
  return pitch.ValueOrDie();
}

std::optional<uint32_t> CalculatePitch32(int bits_per_pixel,
                                         int width_in_pixels) {
  FX_SAFE_UINT32 pitch =
      CalculatePitch32Safely(bits_per_pixel, width_in_pixels);
  if (!pitch.IsValid())
    return std::nullopt;
  return pitch.ValueOrDie();
}

}  // namespace fxge
