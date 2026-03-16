// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFTEXT_UNICODENORMALIZATIONDATA_H_
#define CORE_FPDFTEXT_UNICODENORMALIZATIONDATA_H_

#include <stdint.h>

#include <array>

extern const std::array<uint16_t, 65536> kUnicodeDataNormalization;
extern const std::array<uint16_t, 5376> kUnicodeDataNormalizationMap1;
extern const std::array<uint16_t, 1724> kUnicodeDataNormalizationMap2;
extern const std::array<uint16_t, 1164> kUnicodeDataNormalizationMap3;
extern const std::array<uint16_t, 488> kUnicodeDataNormalizationMap4;

#endif  // CORE_FPDFTEXT_UNICODENORMALIZATIONDATA_H_
