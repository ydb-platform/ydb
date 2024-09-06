/* ChaCha implementation using 512-bit vectorization by the authors of [1].
* This is a public domain implementation, which improves the slightly modified implementations
* of Ted Krovetz in the Chromium Project by using the Advanced Vector Extensions AVX512
* to widen the vectorization. Further details and measurement results are provided in:
* [1] Goll, M., and Gueron,S.: Vectorization of ChaCha Stream Cipher. Cryptology ePrint Archive,
* Report 2013/759, November, 2013, http://eprint.iacr.org/2013/759.pdf
*/
#include "chacha_512.h"
#include "secured_block.h"

#include <util/system/align.h>
#include <util/system/yassert.h>


constexpr size_t ChaCha512::KEY_SIZE;
constexpr size_t ChaCha512::BLOCK_SIZE;

void ChaCha512::SetKey(const ui8* key, size_t size)
{
    Y_ASSERT((size == KEY_SIZE) && "key must be 32 bytes long");

#ifdef __AVX512F__
    q0_ = (vec512)_mm512_broadcast_i32x4(*(__m128i*)chacha_const);
    q1_ = (vec512)_mm512_broadcast_i32x4(((__m128i*)key)[0]);
    q2_ = (vec512)_mm512_broadcast_i32x4(((__m128i*)key)[1]);
#else
    Y_UNUSED(key);
    return;
#endif
}

void ChaCha512::SetIV(const ui8* iv, const ui8* blockIdx)
{
#ifdef __AVX512F__
    ui64 counter = *((ui64*)blockIdx);
    ui32 *nonce = ((ui32*)iv);
    vec128 s3 = (vec128) {
        static_cast<unsigned int>(counter & 0xffffffff), static_cast<unsigned int>(counter >> 32),
        ((unsigned int *)nonce)[0], ((unsigned int *)nonce)[1]
    };
    q3_ = ADD512_64(_mm512_broadcast_i32x4((__m128i)s3), _mm512_set_epi64(0,3,0,2,0,1,0,0));
#else
    Y_UNUSED(iv);
    Y_UNUSED(blockIdx);
#endif
}


void ChaCha512::SetIV(const ui8* iv) {
    const ui8 zero[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    SetIV(iv, zero);
}

void ChaCha512::EncipherImpl(const ui8* plaintext, ui8* ciphertext, size_t len)
{
    ui32 *op=(ui32 *)ciphertext, *ip=(ui32 *)plaintext;
#ifdef __AVX512F__
    for (ui32 j=0; j < len/768; j++) {
        vec512 v0=q0_, v1=q1_, v2=q2_, v3=q3_;
        vec512 v4=q0_, v5=q1_, v6=q2_, v7=ADD512_64(q3_, FOUR);
        vec512 v8=q0_, v9=q1_, v10=q2_, v11=ADD512_64(v7, FOUR);

        for (ui32 i = rounds_/2; i; i--) {
            DQROUND_VECTORS_512(v0,v1,v2,v3)
            DQROUND_VECTORS_512(v4,v5,v6,v7)
            DQROUND_VECTORS_512(v8,v9,v10,v11)
        }

        WRITE_XOR_512(ip, op,  0, ADD512_32(v0,q0_), ADD512_32(v1,q1_), ADD512_32(v2, q2_), ADD512_32(v3, q3_))
        q3_ = ADD512_64(q3_, FOUR);
        WRITE_XOR_512(ip, op, 64, ADD512_32(v4, q0_), ADD512_32(v5, q1_), ADD512_32(v6, q2_), ADD512_32(v7, q3_))
        q3_ = ADD512_64( q3_, FOUR);
        WRITE_XOR_512(ip, op,128, ADD512_32(v8, q0_), ADD512_32(v9, q1_), ADD512_32(v10, q2_), ADD512_32(v11, q3_))
        q3_ = ADD512_64(q3_, FOUR);

        ip += 192;
        op += 192;
    }
    len = len % 768;

    if (len >= 512) {
        vec512 v0=q0_, v1=q1_, v2=q2_, v3=q3_;
        vec512 v4=q0_, v5=q1_, v6=q2_, v7=ADD512_64(q3_, FOUR);

        for (ui32 i = rounds_/2; i; i--) {
            DQROUND_VECTORS_512(v0,v1,v2,v3)
            DQROUND_VECTORS_512(v4,v5,v6,v7)
        }

        WRITE_XOR_512(ip, op, 0, ADD512_32(v0,q0_), ADD512_32(v1,q1_), ADD512_32(v2,q2_), ADD512_32(v3,q3_))
        q3_ = ADD512_64(q3_, FOUR);
        WRITE_XOR_512(ip, op, 64, ADD512_32(v4,q0_), ADD512_32(v5,q1_), ADD512_32(v6,q2_), ADD512_32(v7,q3_))
        q3_ = ADD512_64(q3_, FOUR);

        ip += 128;
        op += 128;

        len = len % 512;
    }

    if (len >= 256) {
        vec512 v0=q0_, v1=q1_, v2=q2_, v3=q3_;

        for (ui32 i = rounds_/2; i; i--) {
            DQROUND_VECTORS_512(v0,v1,v2,v3)
        }

        WRITE_XOR_512(ip, op, 0, ADD512_32(v0,q0_), ADD512_32(v1,q1_), ADD512_32(v2,q2_), ADD512_32(v3,q3_))
        q3_ = ADD512_64(q3_, FOUR);

        ip += 64;
        op += 64;

        len = len % 256;
    }

    if (len) {
        vec512 v0=q0_, v1=q1_, v2=q2_, v3=q3_;
        __attribute__ ((aligned (16))) vec128 buf[4];

        for (ui32 i = rounds_/2; i; i--) {
            DQROUND_VECTORS_512(v0,v1,v2,v3)
        }
        v0 = ADD512_32(v0,q0_); v1 = ADD512_32(v1,q1_);
        v2 = ADD512_32(v2,q2_); v3 = ADD512_32(v3,q3_);

        ui32 j = 0;

        if (len >= 192) {
            WRITE_XOR_256(ip, op, 0, LOW256(v0), LOW256(v1), LOW256(v2), LOW256(v3));
            WRITE_XOR_128(ip, op, 32, LOW128(HIGH256(v0)), LOW128(HIGH256(v1)), LOW128(HIGH256(v2)), LOW128(HIGH256(v3)));
            buf[0] = HIGH128(HIGH256(v0)); j = 192;
            q3_ = ADD512_64(q3_, THREE_);
            if (len >= 208) {
                STORE128(op + 48, XOR128(LOAD128(ip + 48), buf[0]));
                buf[1] = HIGH128(HIGH256(v1));
                if (len >= 224) {
                    STORE128(op + 52, XOR128(LOAD128(ip + 52), buf[1]));
                    buf[2] = HIGH128(HIGH256(v2));
                    if (len >= 240) {
                        STORE128(op + 56, XOR128(LOAD128(ip + 56), buf[2]));
                        buf[3] = HIGH128(HIGH256(v3));
                    }
                }
            }
        } else if (len >= 128) {
            WRITE_XOR_256(ip, op, 0, LOW256(v0), LOW256(v1), LOW256(v2), LOW256(v3));
            buf[0] = LOW128(HIGH256(v0)); j = 128;
            q3_ = ADD512_64(q3_, TWO_);
            if (len >= 144) {
                STORE128(op + 32, XOR128(LOAD128(ip + 32), buf[0]));
                buf[1] = LOW128(HIGH256(v1));
                if (len >= 160) {
                    STORE128(op + 36, XOR128(LOAD128(ip + 36), buf[1]));
                    buf[2] = LOW128(HIGH256(v2));
                    if (len >= 176) {
                        STORE128(op + 40, XOR128(LOAD128(ip + 40), buf[2]));
                        buf[3] = LOW128(HIGH256(v3));
                    }
                }
            }
        } else if (len >= 64) {
            WRITE_XOR_128(ip, op, 0, LOW128(LOW256(v0)), LOW128(LOW256(v1)), LOW128(LOW256(v2)), LOW128(LOW256(v3)));
            buf[0] = HIGH128(LOW256(v0)); j = 64;
            q3_ = ADD512_64(q3_, ONE_);
            if (len >= 80) {
                STORE128(op + 16, XOR128(LOAD128(ip + 16), buf[0]));
                buf[1] = HIGH128(LOW256(v1));
                if (len >= 96) {
                    STORE128(op + 20, XOR128(LOAD128(ip + 20), buf[1]));
                    buf[2] = HIGH128(LOW256(v2));
                    if (len >= 112) {
                        STORE128(op + 24, XOR128(LOAD128(ip + 24), buf[2]));
                        buf[3] = HIGH128(LOW256(v3));
                    }
                }
            }
        } else {
            buf[0] = LOW128(LOW256(v0));  j = 0;
            if (len >= 16) {
                STORE128(op + 0, XOR128(LOAD128(ip + 0), buf[0]));
                buf[1] = LOW128(LOW256(v1));
                if (len >= 32) {
                    STORE128(op + 4, XOR128(LOAD128(ip + 4), buf[1]));
                    buf[2] = LOW128(LOW256(v2));
                    if (len >= 48) {
                        STORE128(op + 8, XOR128(LOAD128(ip + 8), buf[2]));
                        buf[3] = LOW128(LOW256(v3));
                    }
                }
            }
        }

        for (ui32 i=(len & ~15); i<len; i++) {
            ((unsigned char *)op)[i] = ((unsigned char *)ip)[i] ^ ((unsigned char *)buf)[i-j];
        }

        SecureWipeBuffer((ui8*)buf, sizeof(buf));
    }

    return;
#else
    Y_UNUSED(ip);
    Y_UNUSED(op);
    Y_UNUSED(len);
    return;
#endif
}

void ChaCha512::Encipher(const ui8* plaintext, ui8* ciphertext, size_t len)
{
    EncipherImpl(plaintext, ciphertext, len);
}

ChaCha512::~ChaCha512() {
#ifdef __AVX512F__
    SecureWipeBuffer((ui8*)&q1_, 64);
    SecureWipeBuffer((ui8*)&q2_, 64);
#endif
}
