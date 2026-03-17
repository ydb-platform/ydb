// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/cmaps/CNS1/cmaps_cns1.h"

namespace fxcmap {
namespace {

const CMap kCNS1_cmaps[] = {
    {"B5pc-H", kB5pc_H_0, nullptr, 247, 0, CMap::Type::kRange, 0},
    {"B5pc-V", kB5pc_V_0, nullptr, 12, 0, CMap::Type::kRange, -1},
    {"HKscs-B5-H", kHKscs_B5_H_5, nullptr, 1210, 0, CMap::Type::kRange, 0},
    {"HKscs-B5-V", kHKscs_B5_V_5, nullptr, 13, 0, CMap::Type::kRange, -1},
    {"ETen-B5-H", kETen_B5_H_0, nullptr, 254, 0, CMap::Type::kRange, 0},
    {"ETen-B5-V", kETen_B5_V_0, nullptr, 13, 0, CMap::Type::kRange, -1},
    {"ETenms-B5-H", kETenms_B5_H_0, nullptr, 1, 0, CMap::Type::kRange, -2},
    {"ETenms-B5-V", kETenms_B5_V_0, nullptr, 18, 0, CMap::Type::kRange, -1},
    {"CNS-EUC-H", kCNS_EUC_H_0, kCNS_EUC_H_0_DWord, 157, 238,
     CMap::Type::kRange, 0},
    {"CNS-EUC-V", kCNS_EUC_V_0, kCNS_EUC_V_0_DWord, 180, 261,
     CMap::Type::kRange, 0},
    {"UniCNS-UCS2-H", kUniCNS_UCS2_H_3, nullptr, 16418, 0, CMap::Type::kRange,
     0},
    {"UniCNS-UCS2-V", kUniCNS_UCS2_V_3, nullptr, 13, 0, CMap::Type::kRange, -1},
    {"UniCNS-UTF16-H", kUniCNS_UTF16_H_0, nullptr, 14557, 0,
     CMap::Type::kSingle, 0},
    {"UniCNS-UTF16-V", kUniCNS_UCS2_V_3, nullptr, 13, 0, CMap::Type::kRange,
     -1},
};

}  // namespace

const pdfium::span<const CMap> kCNS1_cmaps_span{kCNS1_cmaps};

}  // namespace fxcmap
