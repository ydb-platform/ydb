// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CONSTANTS_ANNOTATION_FLAGS_H_
#define CONSTANTS_ANNOTATION_FLAGS_H_

#include <stdint.h>

namespace pdfium {
namespace annotation_flags {

// PDF 1.7 spec, table 8.16.
constexpr uint32_t kInvisible = 1 << 0;
constexpr uint32_t kHidden = 1 << 1;
constexpr uint32_t kPrint = 1 << 2;
constexpr uint32_t kNoZoom = 1 << 3;
constexpr uint32_t kNoRotate = 1 << 4;
constexpr uint32_t kNoView = 1 << 5;
constexpr uint32_t kReadOnly = 1 << 6;
constexpr uint32_t kLocked = 1 << 7;
constexpr uint32_t kToggleNoView = 1 << 8;
constexpr uint32_t kLockedContents = 1 << 9;

}  // namespace annotation_flags
}  // namespace pdfium

#endif  // CONSTANTS_ANNOTATION_FLAGS_H_
