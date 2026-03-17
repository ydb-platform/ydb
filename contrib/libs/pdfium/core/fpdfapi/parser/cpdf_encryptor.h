// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_ENCRYPTOR_H_
#define CORE_FPDFAPI_PARSER_CPDF_ENCRYPTOR_H_

#include <stdint.h>

#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/span.h"
#include "core/fxcrt/unowned_ptr.h"

class CPDF_CryptoHandler;

class CPDF_Encryptor {
 public:
  CPDF_Encryptor(const CPDF_CryptoHandler* pHandler, int objnum);
  ~CPDF_Encryptor();

  DataVector<uint8_t> Encrypt(pdfium::span<const uint8_t> src_data) const;

 private:
  UnownedPtr<const CPDF_CryptoHandler> const m_pHandler;
  const int m_ObjNum;
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_ENCRYPTOR_H_
