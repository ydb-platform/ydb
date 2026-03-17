// Copyright 2019 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFDOC_CPDF_ICON_H_
#define CORE_FPDFDOC_CPDF_ICON_H_

#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/fx_coordinates.h"
#include "core/fxcrt/retain_ptr.h"

class CPDF_Stream;

class CPDF_Icon final {
 public:
  explicit CPDF_Icon(RetainPtr<const CPDF_Stream> pStream);
  ~CPDF_Icon();

  CFX_SizeF GetImageSize() const;
  CFX_Matrix GetImageMatrix() const;
  ByteString GetImageAlias() const;

 private:
  RetainPtr<const CPDF_Stream> const m_pStream;
};

#endif  // CORE_FPDFDOC_CPDF_ICON_H_
