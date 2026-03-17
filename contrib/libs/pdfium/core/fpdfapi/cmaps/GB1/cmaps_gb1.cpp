// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/cmaps/GB1/cmaps_gb1.h"

namespace fxcmap {
namespace {

const CMap kGB1_cmaps[] = {
    {"GB-EUC-H", kGB_EUC_H_0, nullptr, 90, 0, CMap::Type::kRange, 0},
    {"GB-EUC-V", kGB_EUC_V_0, nullptr, 20, 0, CMap::Type::kRange, -1},
    {"GBpc-EUC-H", kGBpc_EUC_H_0, nullptr, 91, 0, CMap::Type::kRange, 0},
    {"GBpc-EUC-V", kGBpc_EUC_V_0, nullptr, 20, 0, CMap::Type::kRange, -1},
    {"GBK-EUC-H", kGBK_EUC_H_2, nullptr, 4071, 0, CMap::Type::kRange, 0},
    {"GBK-EUC-V", kGBK_EUC_V_2, nullptr, 20, 0, CMap::Type::kRange, -1},
    {"GBKp-EUC-H", kGBKp_EUC_H_2, nullptr, 4070, 0, CMap::Type::kRange, -2},
    {"GBKp-EUC-V", kGBKp_EUC_V_2, nullptr, 20, 0, CMap::Type::kRange, -1},
    {"GBK2K-H", kGBK2K_H_5, kGBK2K_H_5_DWord, 4071, 1017, CMap::Type::kRange,
     -4},
    {"GBK2K-V", kGBK2K_V_5, nullptr, 41, 0, CMap::Type::kRange, -1},
    {"UniGB-UCS2-H", kUniGB_UCS2_H_4, nullptr, 13825, 0, CMap::Type::kRange, 0},
    {"UniGB-UCS2-V", kUniGB_UCS2_V_4, nullptr, 24, 0, CMap::Type::kRange, -1},
    {"UniGB-UTF16-H", kUniGB_UCS2_H_4, nullptr, 13825, 0, CMap::Type::kRange,
     0},
    {"UniGB-UTF16-V", kUniGB_UCS2_V_4, nullptr, 24, 0, CMap::Type::kRange, -1},
};

}  // namespace

const pdfium::span<const CMap> kGB1_cmaps_span{kGB1_cmaps};

}  // namespace fxcmap
