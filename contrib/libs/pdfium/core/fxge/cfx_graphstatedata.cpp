// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxge/cfx_graphstatedata.h"

CFX_GraphStateData::CFX_GraphStateData() = default;

CFX_GraphStateData::CFX_GraphStateData(const CFX_GraphStateData& src) = default;

CFX_GraphStateData::CFX_GraphStateData(CFX_GraphStateData&& src) noexcept =
    default;

CFX_GraphStateData::~CFX_GraphStateData() = default;

CFX_GraphStateData& CFX_GraphStateData::operator=(
    const CFX_GraphStateData& that) = default;

CFX_GraphStateData& CFX_GraphStateData::operator=(
    CFX_GraphStateData&& that) noexcept = default;

CFX_RetainableGraphStateData::CFX_RetainableGraphStateData() = default;

// Note: can't default the copy constructor since Retainable has a deleted
// copy constructor (as it should). Instead, we want the default Retainable
// constructor to be invoked so as to create a copy with a ref-count of 1 as
// of the time it is created, then populate the remainder of the members from
// the |src| object.
CFX_RetainableGraphStateData::CFX_RetainableGraphStateData(
    const CFX_RetainableGraphStateData& src)
    : CFX_GraphStateData(src) {}

CFX_RetainableGraphStateData::~CFX_RetainableGraphStateData() = default;

RetainPtr<CFX_RetainableGraphStateData> CFX_RetainableGraphStateData::Clone()
    const {
  return pdfium::MakeRetain<CFX_RetainableGraphStateData>(*this);
}
