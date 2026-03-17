// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/cmaps/Korea1/cmaps_korea1.h"

namespace fxcmap {
namespace {

const CMap kKorea1_cmaps[] = {
    {"KSC-EUC-H", kKSC_EUC_H_0, nullptr, 467, 0, CMap::Type::kRange, 0},
    {"KSC-EUC-V", kKSC_EUC_V_0, nullptr, 16, 0, CMap::Type::kRange, -1},
    {"KSCms-UHC-H", kKSCms_UHC_H_1, nullptr, 675, 0, CMap::Type::kRange, -2},
    {"KSCms-UHC-V", kKSCms_UHC_V_1, nullptr, 16, 0, CMap::Type::kRange, -1},
    {"KSCms-UHC-HW-H", kKSCms_UHC_HW_H_1, nullptr, 675, 0, CMap::Type::kRange,
     0},
    {"KSCms-UHC-HW-V", kKSCms_UHC_HW_V_1, nullptr, 16, 0, CMap::Type::kRange,
     -1},
    {"KSCpc-EUC-H", kKSCpc_EUC_H_0, nullptr, 509, 0, CMap::Type::kRange, -6},
    {"UniKS-UCS2-H", kUniKS_UCS2_H_1, nullptr, 8394, 0, CMap::Type::kRange, 0},
    {"UniKS-UCS2-V", kUniKS_UCS2_V_1, nullptr, 18, 0, CMap::Type::kRange, -1},
    {"UniKS-UTF16-H", kUniKS_UTF16_H_0, nullptr, 158, 0, CMap::Type::kSingle,
     -2},
    {"UniKS-UTF16-V", kUniKS_UCS2_V_1, nullptr, 18, 0, CMap::Type::kRange, -1},
};

}  // namespace

const pdfium::span<const CMap> kKorea1_cmaps_span{kKorea1_cmaps};

}  // namespace fxcmap
