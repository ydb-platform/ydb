// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_CMAPS_JAPAN1_CMAPS_JAPAN1_H_
#define CORE_FPDFAPI_CMAPS_JAPAN1_CMAPS_JAPAN1_H_

#include <stdint.h>

#include "core/fpdfapi/cmaps/fpdf_cmaps.h"
#include "core/fxcrt/span.h"

namespace fxcmap {

extern const uint16_t k83pv_RKSJ_H_1[];
extern const uint16_t k90ms_RKSJ_H_2[];
extern const uint16_t k90ms_RKSJ_V_2[];
extern const uint16_t k90msp_RKSJ_H_2[];
extern const uint16_t k90msp_RKSJ_V_2[];
extern const uint16_t k90pv_RKSJ_H_1[];
extern const uint16_t kAdd_RKSJ_H_1[];
extern const uint16_t kAdd_RKSJ_V_1[];
extern const uint16_t kEUC_H_1[];
extern const uint16_t kEUC_V_1[];
extern const uint16_t kExt_RKSJ_H_2[];
extern const uint16_t kExt_RKSJ_V_2[];
extern const uint16_t kH_1[];
extern const uint16_t kV_1[];
extern const uint16_t kUniJIS_UCS2_H_4[];
extern const uint16_t kUniJIS_UCS2_V_4[];
extern const uint16_t kUniJIS_UCS2_HW_H_4[];
extern const uint16_t kUniJIS_UCS2_HW_V_4[];
extern const uint16_t kUniJIS_UTF16_H_0[];
extern const uint16_t kUniJIS_UTF16_H_0_DWord[];
extern const uint16_t kUniJIS_UTF16_V_0[];

extern const pdfium::span<const uint16_t> kJapan1CID2Unicode_4;
extern const pdfium::span<const CMap> kJapan1_cmaps_span;

}  // namespace fxcmap

#endif  // CORE_FPDFAPI_CMAPS_JAPAN1_CMAPS_JAPAN1_H_
