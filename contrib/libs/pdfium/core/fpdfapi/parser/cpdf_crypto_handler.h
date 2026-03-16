// Copyright 2016 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FPDFAPI_PARSER_CPDF_CRYPTO_HANDLER_H_
#define CORE_FPDFAPI_PARSER_CPDF_CRYPTO_HANDLER_H_

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <memory>

#include "core/fdrm/fx_crypt.h"
#include "core/fxcrt/binary_buffer.h"
#include "core/fxcrt/bytestring.h"
#include "core/fxcrt/data_vector.h"
#include "core/fxcrt/fx_memory_wrappers.h"
#include "core/fxcrt/retain_ptr.h"
#include "core/fxcrt/span.h"

class CPDF_Dictionary;
class CPDF_Object;

class CPDF_CryptoHandler {
 public:
  enum class Cipher {
    kNone = 0,
    kRC4 = 1,
    kAES = 2,
    kAES2 = 3,
  };

  static bool IsSignatureDictionary(const CPDF_Dictionary* dictionary);

  CPDF_CryptoHandler(Cipher cipher, pdfium::span<const uint8_t> key);
  ~CPDF_CryptoHandler();

  bool DecryptObjectTree(RetainPtr<CPDF_Object> object);

  DataVector<uint8_t> EncryptContent(uint32_t objnum,
                                     uint32_t gennum,
                                     pdfium::span<const uint8_t> source) const;

  bool IsCipherAES() const;

 private:
  size_t DecryptGetSize(size_t src_size);
  void* DecryptStart(uint32_t objnum, uint32_t gennum);
  ByteString Decrypt(uint32_t objnum, uint32_t gennum, const ByteString& str);
  bool DecryptStream(void* context,
                     pdfium::span<const uint8_t> source,
                     BinaryBuffer& dest_buf);
  bool DecryptFinish(void* context, BinaryBuffer& dest_buf);
  void PopulateKey(uint32_t objnum, uint32_t gennum, uint8_t* key) const;

  const size_t m_KeyLen;
  const Cipher m_Cipher;
  std::unique_ptr<CRYPT_aes_context, FxFreeDeleter> m_pAESContext;
  std::array<uint8_t, 32> m_EncryptKey = {};
};

#endif  // CORE_FPDFAPI_PARSER_CPDF_CRYPTO_HANDLER_H_
