// Copyright 2022 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXGE_CALCULATE_PITCH_H_
#define CORE_FXGE_CALCULATE_PITCH_H_

#include <stdint.h>

#include <optional>

namespace fxge {

// Returns the bytes between successive rows of an image where rows are aligned
// on a byte boundary and there is no additional padding beyond that, or nullopt
// if the result can not fit in a uint32_t.
std::optional<uint32_t> CalculatePitch8(uint32_t bits_per_component,
                                        uint32_t components_per_pixel,
                                        int width_in_pixels);

// Returns the bytes between successive rows of an image where rows are aligned
// on a 32-bit word boundary and there is no additional padding beyond that, or
// nullopt if the result can not fit in a uint32_t.
std::optional<uint32_t> CalculatePitch32(int bits_per_pixel,
                                         int width_in_pixels);

// Same as above, but terminate if the result can not fit in a uint32_t.
uint32_t CalculatePitch8OrDie(uint32_t bits_per_component,
                              uint32_t components_per_pixel,
                              int width_in_pixels);
uint32_t CalculatePitch32OrDie(int bits_per_pixel, int width_in_pixels);

}  // namespace fxge

#endif  // CORE_FXGE_CALCULATE_PITCH_H_
