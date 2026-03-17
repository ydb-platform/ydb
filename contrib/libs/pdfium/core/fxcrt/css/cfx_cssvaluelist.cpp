// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssvaluelist.h"

#include <utility>

#include "core/fxcrt/css/cfx_css.h"

CFX_CSSValueList::CFX_CSSValueList(std::vector<RetainPtr<CFX_CSSValue>> list)
    : CFX_CSSValue(PrimitiveType::kList), list_(std::move(list)) {}

CFX_CSSValueList::~CFX_CSSValueList() = default;
