// Copyright 2020 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fpdfsdk/formfiller/cffl_perwindowdata.h"

#include "core/fxcrt/ptr_util.h"
#include "fpdfsdk/cpdfsdk_widget.h"

CFFL_PerWindowData::CFFL_PerWindowData(CPDFSDK_Widget* pWidget,
                                       const CPDFSDK_PageView* pPageView,
                                       uint32_t nAppearanceAge,
                                       uint32_t nValueAge)
    : m_pWidget(pWidget),
      m_pPageView(pPageView),
      m_nAppearanceAge(nAppearanceAge),
      m_nValueAge(nValueAge) {}

CFFL_PerWindowData::CFFL_PerWindowData(const CFFL_PerWindowData& that) =
    default;

CFFL_PerWindowData::~CFFL_PerWindowData() = default;

std::unique_ptr<IPWL_FillerNotify::PerWindowData> CFFL_PerWindowData::Clone()
    const {
  // Private constructor.
  return pdfium::WrapUnique(new CFFL_PerWindowData(*this));
}
