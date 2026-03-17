// Copyright 2015 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_CMAPS_KOREA1_CMAPS_KOREA1_H_
#define CORE_FPDFAPI_CMAPS_KOREA1_CMAPS_KOREA1_H_

#include <stdint.h>

#include "core/fpdfapi/cmaps/fpdf_cmaps.h"
#include "core/fxcrt/span.h"

namespace fxcmap {

extern const uint16_t kKSC_EUC_H_0[];
extern const uint16_t kKSC_EUC_V_0[];
extern const uint16_t kKSCms_UHC_H_1[];
extern const uint16_t kKSCms_UHC_V_1[];
extern const uint16_t kKSCms_UHC_HW_H_1[];
extern const uint16_t kKSCms_UHC_HW_V_1[];
extern const uint16_t kKSCpc_EUC_H_0[];
extern const uint16_t kUniKS_UCS2_H_1[];
extern const uint16_t kUniKS_UCS2_V_1[];
extern const uint16_t kUniKS_UTF16_H_0[];

extern const pdfium::span<const uint16_t> kKorea1CID2Unicode_2;
extern const pdfium::span<const CMap> kKorea1_cmaps_span;

}  // namespace fxcmap

#endif  // CORE_FPDFAPI_CMAPS_KOREA1_CMAPS_KOREA1_H_
