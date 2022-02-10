/*
chacha-merged.c version 20080118
D. J. Bernstein
Public domain.
*/

#include "chacha.h"
#include "secured_block.h"

#include <util/system/yassert.h>

#define U32C(v) (v##U)
#define U32V(v) ((ui32)(v) & U32C(0xFFFFFFFF))

#define ROTL32(v, n) \
  (U32V((v) << (n)) | ((v) >> (32 - (n))))

#define U8TO32_LITTLE(p) (*(ui32*)(p))
#define U32TO8_LITTLE(p, v) (*(ui32*)(p) = (v))

#define ROTATE(v,c) (ROTL32(v,c))
#define XOR(v,w) ((v) ^ (w))
#define PLUS(v,w) (U32V((v) + (w)))
#define PLUSONE(v) (PLUS((v),1))

#define QUARTERROUND(a,b,c,d) \
  a = PLUS(a,b); d = ROTATE(XOR(d,a),16); \
  c = PLUS(c,d); b = ROTATE(XOR(b,c),12); \
  a = PLUS(a,b); d = ROTATE(XOR(d,a), 8); \
  c = PLUS(c,d); b = ROTATE(XOR(b,c), 7);


static const char sigma[] = "expand 32-byte k";
static const char tau[] = "expand 16-byte k";

#if (!defined(_win_) && !defined(_arm64_))
constexpr size_t ChaCha::KEY_SIZE;
constexpr size_t ChaCha::BLOCK_SIZE;
#endif

void ChaCha::SetKey(const ui8* key, size_t size)
{
  const char *constants;
  Y_ASSERT((size == 16 || size == 32) && "key must be 16 or 32 bytes long");

  state_[4] = U8TO32_LITTLE(key + 0);
  state_[5] = U8TO32_LITTLE(key + 4);
  state_[6] = U8TO32_LITTLE(key + 8);
  state_[7] = U8TO32_LITTLE(key + 12);
  if (size == 32) {    /* recommended */
    key += 16;
    constants = sigma;
  } else {              /* size == 128 */
    constants = tau;
  }
  state_[8] = U8TO32_LITTLE(key + 0);
  state_[9] = U8TO32_LITTLE(key + 4);
  state_[10] = U8TO32_LITTLE(key + 8);
  state_[11] = U8TO32_LITTLE(key + 12);
  state_[0] = U8TO32_LITTLE(constants + 0);
  state_[1] = U8TO32_LITTLE(constants + 4);
  state_[2] = U8TO32_LITTLE(constants + 8);
  state_[3] = U8TO32_LITTLE(constants + 12);
}

void ChaCha::SetIV(const ui8* iv, const ui8* blockIdx)
{
  state_[12] = U8TO32_LITTLE(blockIdx + 0);
  state_[13] = U8TO32_LITTLE(blockIdx + 4);
  state_[14] = U8TO32_LITTLE(iv + 0);
  state_[15] = U8TO32_LITTLE(iv + 4);
}

void ChaCha::SetIV(const ui8* iv)
{
  state_[12] = 0;
  state_[13] = 0;
  state_[14] = U8TO32_LITTLE(iv + 0);
  state_[15] = U8TO32_LITTLE(iv + 4);
}

void ChaCha::Encipher(const ui8* plaintext, ui8* ciphertext, size_t size)
{
  ui32 x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15;
  ui32 j0, j1, j2, j3, j4, j5, j6, j7, j8, j9, j10, j11, j12, j13, j14, j15;
  ui8* ctarget = nullptr;
  ui8 tmp[64];
  size_t i;

  if (!size) return;

  j0 = state_[0];
  j1 = state_[1];
  j2 = state_[2];
  j3 = state_[3];
  j4 = state_[4];
  j5 = state_[5];
  j6 = state_[6];
  j7 = state_[7];
  j8 = state_[8];
  j9 = state_[9];
  j10 = state_[10];
  j11 = state_[11];
  j12 = state_[12];
  j13 = state_[13];
  j14 = state_[14];
  j15 = state_[15];

  for (;;) {
    if (size < 64) {
      for (i = 0; i < size; ++i) tmp[i] = plaintext[i];
      plaintext = tmp;
      ctarget = ciphertext;
      ciphertext = tmp;
    }
    x0 = j0;
    x1 = j1;
    x2 = j2;
    x3 = j3;
    x4 = j4;
    x5 = j5;
    x6 = j6;
    x7 = j7;
    x8 = j8;
    x9 = j9;
    x10 = j10;
    x11 = j11;
    x12 = j12;
    x13 = j13;
    x14 = j14;
    x15 = j15;
    for (i = rounds_; i > 0; i -= 2) {
      QUARTERROUND( x0, x4, x8,x12)
      QUARTERROUND( x1, x5, x9,x13)
      QUARTERROUND( x2, x6,x10,x14)
      QUARTERROUND( x3, x7,x11,x15)
      QUARTERROUND( x0, x5,x10,x15)
      QUARTERROUND( x1, x6,x11,x12)
      QUARTERROUND( x2, x7, x8,x13)
      QUARTERROUND( x3, x4, x9,x14)
    }
    x0 = PLUS(x0,j0);
    x1 = PLUS(x1,j1);
    x2 = PLUS(x2,j2);
    x3 = PLUS(x3,j3);
    x4 = PLUS(x4,j4);
    x5 = PLUS(x5,j5);
    x6 = PLUS(x6,j6);
    x7 = PLUS(x7,j7);
    x8 = PLUS(x8,j8);
    x9 = PLUS(x9,j9);
    x10 = PLUS(x10,j10);
    x11 = PLUS(x11,j11);
    x12 = PLUS(x12,j12);
    x13 = PLUS(x13,j13);
    x14 = PLUS(x14,j14);
    x15 = PLUS(x15,j15);

    x0 = XOR(x0,U8TO32_LITTLE(plaintext + 0));
    x1 = XOR(x1,U8TO32_LITTLE(plaintext + 4));
    x2 = XOR(x2,U8TO32_LITTLE(plaintext + 8));
    x3 = XOR(x3,U8TO32_LITTLE(plaintext + 12));
    x4 = XOR(x4,U8TO32_LITTLE(plaintext + 16));
    x5 = XOR(x5,U8TO32_LITTLE(plaintext + 20));
    x6 = XOR(x6,U8TO32_LITTLE(plaintext + 24));
    x7 = XOR(x7,U8TO32_LITTLE(plaintext + 28));
    x8 = XOR(x8,U8TO32_LITTLE(plaintext + 32));
    x9 = XOR(x9,U8TO32_LITTLE(plaintext + 36));
    x10 = XOR(x10,U8TO32_LITTLE(plaintext + 40));
    x11 = XOR(x11,U8TO32_LITTLE(plaintext + 44));
    x12 = XOR(x12,U8TO32_LITTLE(plaintext + 48));
    x13 = XOR(x13,U8TO32_LITTLE(plaintext + 52));
    x14 = XOR(x14,U8TO32_LITTLE(plaintext + 56));
    x15 = XOR(x15,U8TO32_LITTLE(plaintext + 60));

    j12 = PLUSONE(j12);
    if (!j12) {
      j13 = PLUSONE(j13);
      /* stopping at 2^70 bytes per nonce is user's responsibility */
    }

    U32TO8_LITTLE(ciphertext + 0,x0);
    U32TO8_LITTLE(ciphertext + 4,x1);
    U32TO8_LITTLE(ciphertext + 8,x2);
    U32TO8_LITTLE(ciphertext + 12,x3);
    U32TO8_LITTLE(ciphertext + 16,x4);
    U32TO8_LITTLE(ciphertext + 20,x5);
    U32TO8_LITTLE(ciphertext + 24,x6);
    U32TO8_LITTLE(ciphertext + 28,x7);
    U32TO8_LITTLE(ciphertext + 32,x8);
    U32TO8_LITTLE(ciphertext + 36,x9);
    U32TO8_LITTLE(ciphertext + 40,x10);
    U32TO8_LITTLE(ciphertext + 44,x11);
    U32TO8_LITTLE(ciphertext + 48,x12);
    U32TO8_LITTLE(ciphertext + 52,x13);
    U32TO8_LITTLE(ciphertext + 56,x14);
    U32TO8_LITTLE(ciphertext + 60,x15);

    if (size <= 64) {
      if (size < 64) {
        for (i = 0; i < size; ++i) ctarget[i] = ciphertext[i];
      }
      state_[12] = j12;
      state_[13] = j13;
      return;
    }
    size -= 64;
    ciphertext += 64;
    plaintext += 64;
  }
}

void ChaCha::Decipher(const ui8* ciphertext, ui8* plaintext, size_t size)
{
    Encipher(ciphertext, plaintext, size);
}

ChaCha::~ChaCha() {
    SecureWipeBuffer((ui8*)&state_[4], 32);
}

