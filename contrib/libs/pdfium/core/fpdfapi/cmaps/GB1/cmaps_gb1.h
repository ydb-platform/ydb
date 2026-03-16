// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_CMAPS_GB1_CMAPS_GB1_H_
#define CORE_FPDFAPI_CMAPS_GB1_CMAPS_GB1_H_

#include <stdint.h>

#include "core/fpdfapi/cmaps/fpdf_cmaps.h"
#include "core/fxcrt/span.h"

namespace fxcmap {

extern const uint16_t kGB_EUC_H_0[];
extern const uint16_t kGB_EUC_V_0[];
extern const uint16_t kGBpc_EUC_H_0[];
extern const uint16_t kGBpc_EUC_V_0[];
extern const uint16_t kGBK_EUC_H_2[];
extern const uint16_t kGBK_EUC_V_2[];
extern const uint16_t kGBKp_EUC_H_2[];
extern const uint16_t kGBKp_EUC_V_2[];
extern const uint16_t kGBK2K_H_5[];
extern const DWordCIDMap kGBK2K_H_5_DWord[];
extern const uint16_t kGBK2K_V_5[];
extern const uint16_t kUniGB_UCS2_H_4[];
extern const uint16_t kUniGB_UCS2_V_4[];

extern const pdfium::span<const uint16_t> kGB1CID2Unicode_5;
extern const pdfium::span<const CMap> kGB1_cmaps_span;

}  // namespace fxcmap

#endif  // CORE_FPDFAPI_CMAPS_GB1_CMAPS_GB1_H_
