// Copyright 2014 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fdrm/fx_crypt.h"

#include <utility>

#include "core/fxcrt/byteorder.h"
#include "core/fxcrt/compiler_specific.h"
#include "core/fxcrt/stl_util.h"

namespace {

const uint8_t md5_padding[64] = {
    0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

void md5_process(CRYPT_md5_context* ctx, pdfium::span<const uint8_t, 64> data) {
  uint32_t X[16] = {
      fxcrt::GetUInt32LSBFirst(data.subspan<0, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<4, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<8, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<12, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<16, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<20, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<24, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<28, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<32, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<36, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<40, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<44, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<48, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<52, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<56, 4>()),
      fxcrt::GetUInt32LSBFirst(data.subspan<60, 4>()),
  };
  uint32_t A = ctx->state[0];
  uint32_t B = ctx->state[1];
  uint32_t C = ctx->state[2];
  uint32_t D = ctx->state[3];
#define S(x, n) ((x << n) | ((x & 0xFFFFFFFF) >> (32 - n)))
#define P(a, b, c, d, k, s, t)  \
  {                             \
    a += F(b, c, d) + X[k] + t; \
    a = S(a, s) + b;            \
  }
#define F(x, y, z) (z ^ (x & (y ^ z)))
  P(A, B, C, D, 0, 7, 0xD76AA478);
  P(D, A, B, C, 1, 12, 0xE8C7B756);
  P(C, D, A, B, 2, 17, 0x242070DB);
  P(B, C, D, A, 3, 22, 0xC1BDCEEE);
  P(A, B, C, D, 4, 7, 0xF57C0FAF);
  P(D, A, B, C, 5, 12, 0x4787C62A);
  P(C, D, A, B, 6, 17, 0xA8304613);
  P(B, C, D, A, 7, 22, 0xFD469501);
  P(A, B, C, D, 8, 7, 0x698098D8);
  P(D, A, B, C, 9, 12, 0x8B44F7AF);
  P(C, D, A, B, 10, 17, 0xFFFF5BB1);
  P(B, C, D, A, 11, 22, 0x895CD7BE);
  P(A, B, C, D, 12, 7, 0x6B901122);
  P(D, A, B, C, 13, 12, 0xFD987193);
  P(C, D, A, B, 14, 17, 0xA679438E);
  P(B, C, D, A, 15, 22, 0x49B40821);
#undef F
#define F(x, y, z) (y ^ (z & (x ^ y)))
  P(A, B, C, D, 1, 5, 0xF61E2562);
  P(D, A, B, C, 6, 9, 0xC040B340);
  P(C, D, A, B, 11, 14, 0x265E5A51);
  P(B, C, D, A, 0, 20, 0xE9B6C7AA);
  P(A, B, C, D, 5, 5, 0xD62F105D);
  P(D, A, B, C, 10, 9, 0x02441453);
  P(C, D, A, B, 15, 14, 0xD8A1E681);
  P(B, C, D, A, 4, 20, 0xE7D3FBC8);
  P(A, B, C, D, 9, 5, 0x21E1CDE6);
  P(D, A, B, C, 14, 9, 0xC33707D6);
  P(C, D, A, B, 3, 14, 0xF4D50D87);
  P(B, C, D, A, 8, 20, 0x455A14ED);
  P(A, B, C, D, 13, 5, 0xA9E3E905);
  P(D, A, B, C, 2, 9, 0xFCEFA3F8);
  P(C, D, A, B, 7, 14, 0x676F02D9);
  P(B, C, D, A, 12, 20, 0x8D2A4C8A);
#undef F
#define F(x, y, z) (x ^ y ^ z)
  P(A, B, C, D, 5, 4, 0xFFFA3942);
  P(D, A, B, C, 8, 11, 0x8771F681);
  P(C, D, A, B, 11, 16, 0x6D9D6122);
  P(B, C, D, A, 14, 23, 0xFDE5380C);
  P(A, B, C, D, 1, 4, 0xA4BEEA44);
  P(D, A, B, C, 4, 11, 0x4BDECFA9);
  P(C, D, A, B, 7, 16, 0xF6BB4B60);
  P(B, C, D, A, 10, 23, 0xBEBFBC70);
  P(A, B, C, D, 13, 4, 0x289B7EC6);
  P(D, A, B, C, 0, 11, 0xEAA127FA);
  P(C, D, A, B, 3, 16, 0xD4EF3085);
  P(B, C, D, A, 6, 23, 0x04881D05);
  P(A, B, C, D, 9, 4, 0xD9D4D039);
  P(D, A, B, C, 12, 11, 0xE6DB99E5);
  P(C, D, A, B, 15, 16, 0x1FA27CF8);
  P(B, C, D, A, 2, 23, 0xC4AC5665);
#undef F
#define F(x, y, z) (y ^ (x | ~z))
  P(A, B, C, D, 0, 6, 0xF4292244);
  P(D, A, B, C, 7, 10, 0x432AFF97);
  P(C, D, A, B, 14, 15, 0xAB9423A7);
  P(B, C, D, A, 5, 21, 0xFC93A039);
  P(A, B, C, D, 12, 6, 0x655B59C3);
  P(D, A, B, C, 3, 10, 0x8F0CCC92);
  P(C, D, A, B, 10, 15, 0xFFEFF47D);
  P(B, C, D, A, 1, 21, 0x85845DD1);
  P(A, B, C, D, 8, 6, 0x6FA87E4F);
  P(D, A, B, C, 15, 10, 0xFE2CE6E0);
  P(C, D, A, B, 6, 15, 0xA3014314);
  P(B, C, D, A, 13, 21, 0x4E0811A1);
  P(A, B, C, D, 4, 6, 0xF7537E82);
  P(D, A, B, C, 11, 10, 0xBD3AF235);
  P(C, D, A, B, 2, 15, 0x2AD7D2BB);
  P(B, C, D, A, 9, 21, 0xEB86D391);
#undef F
  ctx->state[0] += A;
  ctx->state[1] += B;
  ctx->state[2] += C;
  ctx->state[3] += D;
}

}  // namespace

void CRYPT_ArcFourSetup(CRYPT_rc4_context* context,
                        pdfium::span<const uint8_t> key) {
  context->x = 0;
  context->y = 0;
  for (int i = 0; i < CRYPT_rc4_context::kPermutationLength; ++i)
    context->m[i] = i;

  int j = 0;
  for (int i = 0; i < CRYPT_rc4_context::kPermutationLength; ++i) {
    size_t size = key.size();
    j = (j + context->m[i] + (size ? key[i % size] : 0)) & 0xFF;
    std::swap(context->m[i], context->m[j]);
  }
}

void CRYPT_ArcFourCrypt(CRYPT_rc4_context* context,
                        pdfium::span<uint8_t> data) {
  for (auto& datum : data) {
    context->x = (context->x + 1) & 0xFF;
    context->y = (context->y + context->m[context->x]) & 0xFF;
    std::swap(context->m[context->x], context->m[context->y]);
    datum ^=
        context->m[(context->m[context->x] + context->m[context->y]) & 0xFF];
  }
}

void CRYPT_ArcFourCryptBlock(pdfium::span<uint8_t> data,
                             pdfium::span<const uint8_t> key) {
  CRYPT_rc4_context s;
  CRYPT_ArcFourSetup(&s, key);
  CRYPT_ArcFourCrypt(&s, data);
}

CRYPT_md5_context CRYPT_MD5Start() {
  CRYPT_md5_context context;
  context.total[0] = 0;
  context.total[1] = 0;
  context.state[0] = 0x67452301;
  context.state[1] = 0xEFCDAB89;
  context.state[2] = 0x98BADCFE;
  context.state[3] = 0x10325476;
  return context;
}

void CRYPT_MD5Update(CRYPT_md5_context* context,
                     pdfium::span<const uint8_t> data) {
  if (data.empty())
    return;

  uint32_t left = (context->total[0] >> 3) & 0x3F;
  uint32_t fill = 64 - left;
  context->total[0] += data.size() << 3;
  context->total[1] += data.size() >> 29;
  context->total[0] &= 0xFFFFFFFF;
  context->total[1] += context->total[0] < data.size() << 3;

  const pdfium::span<uint8_t> buffer_span = pdfium::make_span(context->buffer);
  if (left && data.size() >= fill) {
    fxcrt::Copy(data.first(fill), buffer_span.subspan(left));
    md5_process(context, context->buffer);
    data = data.subspan(fill);
    left = 0;
  }
  while (data.size() >= 64) {
    md5_process(context, data.first(64));
    data = data.subspan(64);
  }
  if (!data.empty()) {
    fxcrt::Copy(data, buffer_span.subspan(left));
  }
}

void CRYPT_MD5Finish(CRYPT_md5_context* context,
                     pdfium::span<uint8_t, 16> digest) {
  uint8_t msglen[8];
  auto msglen_span = pdfium::make_span(msglen);
  fxcrt::PutUInt32LSBFirst(context->total[0], msglen_span.subspan<0, 4>());
  fxcrt::PutUInt32LSBFirst(context->total[1], msglen_span.subspan<4, 4>());
  uint32_t last = (context->total[0] >> 3) & 0x3F;
  uint32_t padn = (last < 56) ? (56 - last) : (120 - last);
  CRYPT_MD5Update(context, pdfium::make_span(md5_padding).first(padn));
  CRYPT_MD5Update(context, msglen);
  fxcrt::PutUInt32LSBFirst(context->state[0], digest.subspan<0, 4>());
  fxcrt::PutUInt32LSBFirst(context->state[1], digest.subspan<4, 4>());
  fxcrt::PutUInt32LSBFirst(context->state[2], digest.subspan<8, 4>());
  fxcrt::PutUInt32LSBFirst(context->state[3], digest.subspan<12, 4>());
}

void CRYPT_MD5Generate(pdfium::span<const uint8_t> data,
                       pdfium::span<uint8_t, 16> digest) {
  CRYPT_md5_context ctx = CRYPT_MD5Start();
  CRYPT_MD5Update(&ctx, data);
  CRYPT_MD5Finish(&ctx, digest);
}
