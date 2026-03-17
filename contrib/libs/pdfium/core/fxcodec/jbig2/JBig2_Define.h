// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCODEC_JBIG2_JBIG2_DEFINE_H_
#define CORE_FXCODEC_JBIG2_JBIG2_DEFINE_H_

#include <stdint.h>

struct JBig2RegionInfo {
  int32_t width;
  int32_t height;
  int32_t x;
  int32_t y;
  uint8_t flags;
};

struct JBig2HuffmanCode {
  int32_t codelen;
  int32_t code;
};

constexpr int32_t kJBig2OOB = 1;

constexpr int32_t kJBig2MaxReferredSegmentCount = 64;
constexpr uint32_t kJBig2MaxExportSymbols = 65535;
constexpr uint32_t kJBig2MaxNewSymbols = 65535;
constexpr uint32_t kJBig2MaxPatternIndex = 65535;
constexpr int32_t kJBig2MaxImageSize = 65535;

#endif  // CORE_FXCODEC_JBIG2_JBIG2_DEFINE_H_
