// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CONSTANTS_TRANSPARENCY_H_
#define CONSTANTS_TRANSPARENCY_H_

namespace pdfium {
namespace transparency {

extern const char kNormal[];
extern const char kMultiply[];
extern const char kScreen[];
extern const char kOverlay[];
extern const char kDarken[];
extern const char kLighten[];
extern const char kColorDodge[];
extern const char kColorBurn[];
extern const char kHardLight[];
extern const char kSoftLight[];
extern const char kDifference[];
extern const char kExclusion[];

extern const char kHue[];
extern const char kSaturation[];
extern const char kColor[];
extern const char kLuminosity[];

extern const char kSoftMaskSubType[];
extern const char kAlpha[];
extern const char kG[];
extern const char kBC[];
extern const char kTR[];

extern const char kGroupSubType[];
extern const char kTransparency[];
extern const char kCS[];
extern const char kI[];

}  // namespace transparency
}  // namespace pdfium

#endif  // CONSTANTS_TRANSPARENCY_H_
