// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_CMAPS_CNS1_CMAPS_CNS1_H_
#define CORE_FPDFAPI_CMAPS_CNS1_CMAPS_CNS1_H_

#include <stdint.h>

#include "core/fpdfapi/cmaps/fpdf_cmaps.h"
#include "core/fxcrt/span.h"

namespace fxcmap {

extern const uint16_t kB5pc_H_0[];
extern const uint16_t kB5pc_V_0[];
extern const uint16_t kHKscs_B5_H_5[];
extern const uint16_t kHKscs_B5_V_5[];
extern const uint16_t kETen_B5_H_0[];
extern const uint16_t kETen_B5_V_0[];
extern const uint16_t kETenms_B5_H_0[];
extern const uint16_t kETenms_B5_V_0[];
extern const uint16_t kCNS_EUC_H_0[];
extern const DWordCIDMap kCNS_EUC_H_0_DWord[];
extern const uint16_t kCNS_EUC_V_0[];
extern const DWordCIDMap kCNS_EUC_V_0_DWord[];
extern const uint16_t kUniCNS_UCS2_H_3[];
extern const uint16_t kUniCNS_UCS2_V_3[];
extern const uint16_t kUniCNS_UTF16_H_0[];

extern const pdfium::span<const uint16_t> kCNS1CID2Unicode_5;
extern const pdfium::span<const CMap> kCNS1_cmaps_span;

}  // namespace fxcmap

#endif  // CORE_FPDFAPI_CMAPS_CNS1_CMAPS_CNS1_H_
