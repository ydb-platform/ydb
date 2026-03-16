// Copyright 2018 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FXCRT_FX_NUMBER_H_
#define CORE_FXCRT_FX_NUMBER_H_

#include <stdint.h>

#include "core/fxcrt/bytestring.h"
#include <absl/types/variant.h>

class FX_Number {
 public:
  FX_Number();
  explicit FX_Number(uint32_t value) = delete;  // catch misuse.
  explicit FX_Number(int32_t value);
  explicit FX_Number(float value);
  explicit FX_Number(ByteStringView str);

  bool IsInteger() const;
  bool IsSigned() const;

  int32_t GetSigned() const;  // Underflow/Overflow possible.
  float GetFloat() const;

 private:
  absl::variant<uint32_t, int32_t, float> value_ = 0u;
};

#endif  // CORE_FXCRT_FX_NUMBER_H_
