// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "constants/transparency.h"

namespace pdfium::transparency {

// PDF 1.7 spec, table 7.2.
// Standard separable blend modes.
const char kNormal[] = "Normal";
const char kMultiply[] = "Multiply";
const char kScreen[] = "Screen";
const char kOverlay[] = "Overlay";
const char kDarken[] = "Darken";
const char kLighten[] = "Lighten";
const char kColorDodge[] = "ColorDodge";
const char kColorBurn[] = "ColorBurn";
const char kHardLight[] = "HardLight";
const char kSoftLight[] = "SoftLight";
const char kDifference[] = "Difference";
const char kExclusion[] = "Exclusion";

// PDF 1.7 spec, table 7.3.
// Standard nonseparable blend modes.
const char kHue[] = "Hue";
const char kSaturation[] = "Saturation";
const char kColor[] = "Color";
const char kLuminosity[] = "Luminosity";

// PDF 1.7 spec, table 7.10.
// Entries in a soft-mask dictionary.
const char kSoftMaskSubType[] = "S";
const char kAlpha[] = "Alpha";
const char kG[] = "G";
const char kBC[] = "BC";
const char kTR[] = "TR";

// PDF 1.7 spec, table 7.13.
// Additional entries specific to a transparency group attributes dictionary.
const char kGroupSubType[] = "S";
const char kTransparency[] = "Transparency";
const char kCS[] = "CS";
const char kI[] = "I";

}  // namespace pdfium::transparency
