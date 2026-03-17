// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_CSS_CFX_CSSVALUE_H_
#define CORE_FXCRT_CSS_CFX_CSSVALUE_H_

#include "core/fxcrt/css/cfx_css.h"
#include "core/fxcrt/retain_ptr.h"

class CFX_CSSValue : public Retainable {
 public:
  enum class PrimitiveType : uint8_t {
    kUnknown = 0,
    kNumber,
    kString,
    kRGB,
    kEnum,
    kFunction,
    kList,
  };

  PrimitiveType GetType() const { return m_value; }

 protected:
  explicit CFX_CSSValue(PrimitiveType type);
  ~CFX_CSSValue() override;

 private:
  const PrimitiveType m_value;
};

#endif  // CORE_FXCRT_CSS_CFX_CSSVALUE_H_
