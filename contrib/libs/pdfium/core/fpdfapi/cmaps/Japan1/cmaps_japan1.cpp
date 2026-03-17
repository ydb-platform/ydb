// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfapi/cmaps/Japan1/cmaps_japan1.h"

namespace fxcmap {
namespace {

const CMap kJapan1_cmaps[] = {
    {"83pv-RKSJ-H", k83pv_RKSJ_H_1, nullptr, 222, 0, CMap::Type::kRange, 0},
    {"90ms-RKSJ-H", k90ms_RKSJ_H_2, nullptr, 171, 0, CMap::Type::kRange, 0},
    {"90ms-RKSJ-V", k90ms_RKSJ_V_2, nullptr, 78, 0, CMap::Type::kRange, -1},
    {"90msp-RKSJ-H", k90msp_RKSJ_H_2, nullptr, 170, 0, CMap::Type::kRange, -2},
    {"90msp-RKSJ-V", k90msp_RKSJ_V_2, nullptr, 78, 0, CMap::Type::kRange, -1},
    {"90pv-RKSJ-H", k90pv_RKSJ_H_1, nullptr, 263, 0, CMap::Type::kRange, 0},
    {"Add-RKSJ-H", kAdd_RKSJ_H_1, nullptr, 635, 0, CMap::Type::kRange, 0},
    {"Add-RKSJ-V", kAdd_RKSJ_V_1, nullptr, 57, 0, CMap::Type::kRange, -1},
    {"EUC-H", kEUC_H_1, nullptr, 120, 0, CMap::Type::kRange, 0},
    {"EUC-V", kEUC_V_1, nullptr, 27, 0, CMap::Type::kRange, -1},
    {"Ext-RKSJ-H", kExt_RKSJ_H_2, nullptr, 665, 0, CMap::Type::kRange, -4},
    {"Ext-RKSJ-V", kExt_RKSJ_V_2, nullptr, 39, 0, CMap::Type::kRange, -1},
    {"H", kH_1, nullptr, 118, 0, CMap::Type::kRange, 0},
    {"V", kV_1, nullptr, 27, 0, CMap::Type::kRange, -1},
    {"UniJIS-UCS2-H", kUniJIS_UCS2_H_4, nullptr, 9772, 0, CMap::Type::kSingle,
     0},
    {"UniJIS-UCS2-V", kUniJIS_UCS2_V_4, nullptr, 251, 0, CMap::Type::kSingle,
     -1},
    {"UniJIS-UCS2-HW-H", kUniJIS_UCS2_HW_H_4, nullptr, 4, 0, CMap::Type::kRange,
     -2},
    {"UniJIS-UCS2-HW-V", kUniJIS_UCS2_HW_V_4, nullptr, 199, 0,
     CMap::Type::kRange, -1},
    {"UniJIS-UTF16-H", kUniJIS_UCS2_H_4, nullptr, 9772, 0, CMap::Type::kSingle,
     0},
    {"UniJIS-UTF16-V", kUniJIS_UCS2_V_4, nullptr, 251, 0, CMap::Type::kSingle,
     -1},
};

}  // namespace

const pdfium::span<const CMap> kJapan1_cmaps_span{kJapan1_cmaps};

}  // namespace fxcmap
