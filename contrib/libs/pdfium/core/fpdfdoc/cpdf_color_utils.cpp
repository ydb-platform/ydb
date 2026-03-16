// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fpdfdoc/cpdf_color_utils.h"

#include "core/fpdfapi/parser/cpdf_array.h"
#include "core/fpdfdoc/cpdf_defaultappearance.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/notreached.h"

namespace fpdfdoc {

CFX_Color CFXColorFromArray(const CPDF_Array& array) {
  CFX_Color rt;
  switch (array.size()) {
    case 1:
      rt = CFX_Color(CFX_Color::Type::kGray, array.GetFloatAt(0));
      break;
    case 3:
      rt = CFX_Color(CFX_Color::Type::kRGB, array.GetFloatAt(0),
                     array.GetFloatAt(1), array.GetFloatAt(2));
      break;
    case 4:
      rt = CFX_Color(CFX_Color::Type::kCMYK, array.GetFloatAt(0),
                     array.GetFloatAt(1), array.GetFloatAt(2),
                     array.GetFloatAt(3));
      break;
  }
  return rt;
}

CFX_Color CFXColorFromString(const ByteString& str) {
  CPDF_DefaultAppearance appearance(str);
  return appearance.GetColor().value_or(CFX_Color());
}

}  // namespace fpdfdoc
