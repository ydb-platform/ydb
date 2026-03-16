// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssstringvalue.h"

CFX_CSSStringValue::CFX_CSSStringValue(WideStringView value)
    : CFX_CSSValue(PrimitiveType::kString), value_(value) {}

CFX_CSSStringValue::~CFX_CSSStringValue() = default;
