// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#ifndef CORE_FDRM_FX_CRYPT_AES_H_
#define CORE_FDRM_FX_CRYPT_AES_H_

#include <stdint.h>

#include <array>

#include "core/fxcrt/span.h"

struct CRYPT_aes_context {
  static constexpr int kMaxNb = 8;
  static constexpr int kMaxNr = 14;
  static constexpr int kSchedSize = (kMaxNr + 1) * kMaxNb;

  int Nb;
  int Nr;
  std::array<uint32_t, kSchedSize> keysched;
  std::array<uint32_t, kSchedSize> invkeysched;
  std::array<uint32_t, kMaxNb> iv;
};

void CRYPT_AESSetKey(CRYPT_aes_context* ctx,
                     const uint8_t* key,
                     uint32_t keylen);
void CRYPT_AESSetIV(CRYPT_aes_context* ctx, const uint8_t* iv);
void CRYPT_AESDecrypt(CRYPT_aes_context* ctx,
                      uint8_t* dest,
                      const uint8_t* src,
                      uint32_t size);
void CRYPT_AESEncrypt(CRYPT_aes_context* ctx,
                      pdfium::span<uint8_t> dest,
                      pdfium::span<const uint8_t> src);

#endif  // CORE_FDRM_FX_CRYPT_AES_H_
