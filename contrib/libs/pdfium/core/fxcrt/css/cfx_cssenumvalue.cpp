// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/css/cfx_cssenumvalue.h"

CFX_CSSEnumValue::CFX_CSSEnumValue(CFX_CSSPropertyValue value)
    : CFX_CSSValue(PrimitiveType::kEnum), value_(value) {}

CFX_CSSEnumValue::~CFX_CSSEnumValue() = default;
