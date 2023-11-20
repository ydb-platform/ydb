/*
chacha-merged.c version 20080118
D. J. Bernstein
Public domain.
*/

#include "chacha_vec.h"
#include "secured_block.h"

#include <util/system/align.h>
#include <util/system/yassert.h>


/* Chacha implementation for 16-byte vectors by Ted Krovetz (ted@krovetz.net).
 * Assumes 32-bit int, 64-bit long long. Public domain. Modified: 2013.06.19.
 * Chacha is an improvement on the stream cipher Salsa, described at
 * http://cr.yp.to/papers.html#chacha
 */
#include <string.h>

/* This implementation is designed for Neon, SSE and AltiVec machines. The
 * following specify how to do certain vector operations efficiently on
 * each architecture, using intrinsics.
 * This implementation supports parallel processing of multiple blocks,
 * including potentially using general-purpose registers.
 */
#if (__ARM_NEON__ || defined(_arm64_))
#   include <arm_neon.h>
#   define ONE       (vec)vsetq_lane_u32(1,vdupq_n_u32(0),0)
#   define NONCE(p, bp)  (vec)vcombine_u32(vcreate_u32(*(uint64_t *)(bp)), vcreate_u32(*(uint64_t *)(p)))
#   define ROTV1(x)  (vec)vextq_u32((uint32x4_t)x,(uint32x4_t)x,1)
#   define ROTV2(x)  (vec)vextq_u32((uint32x4_t)x,(uint32x4_t)x,2)
#   define ROTV3(x)  (vec)vextq_u32((uint32x4_t)x,(uint32x4_t)x,3)
#   define ROTW16(x) (vec)vrev32q_u16((uint16x8_t)x)
#   if __clang__
#      define ROTW7(x)  (x << ((vec){ 7, 7, 7, 7})) ^ (x >> ((vec){25,25,25,25}))
#      define ROTW8(x)  (x << ((vec){ 8, 8, 8, 8})) ^ (x >> ((vec){24,24,24,24}))
#      define ROTW12(x) (x << ((vec){12,12,12,12})) ^ (x >> ((vec){20,20,20,20}))
#   else
#      define ROTW7(x)  (vec)vsriq_n_u32(vshlq_n_u32((uint32x4_t)x,7),(uint32x4_t)x,25)
#      define ROTW8(x)  (vec)vsriq_n_u32(vshlq_n_u32((uint32x4_t)x,8),(uint32x4_t)x,24)
#      define ROTW12(x) (vec)vsriq_n_u32(vshlq_n_u32((uint32x4_t)x,12),(uint32x4_t)x,20)
#   endif
#elif __SSE2__
#   include <emmintrin.h>
#   define ONE       (vec)_mm_set_epi32(0,0,0,1)
#   define NONCE(p, bp)  (vec)(_mm_slli_si128(_mm_loadl_epi64((__m128i *)(p)),8) ^ _mm_loadl_epi64((__m128i *)(bp)))
#   define ROTV1(x)  (vec)_mm_shuffle_epi32((__m128i)x,_MM_SHUFFLE(0,3,2,1))
#   define ROTV2(x)  (vec)_mm_shuffle_epi32((__m128i)x,_MM_SHUFFLE(1,0,3,2))
#   define ROTV3(x)  (vec)_mm_shuffle_epi32((__m128i)x,_MM_SHUFFLE(2,1,0,3))
#   define ROTW7(x)  (vec)(_mm_slli_epi32((__m128i)x, 7) ^ _mm_srli_epi32((__m128i)x,25))
#   define ROTW12(x) (vec)(_mm_slli_epi32((__m128i)x,12) ^ _mm_srli_epi32((__m128i)x,20))
#   define SHIFT_LEFT(x) (vec)_mm_bslli_si128((__m128i)(x), 8)
#   define SHIFT_RIGHT(x) (vec)_mm_bsrli_si128((__m128i)(x), 8)
#   define SET(x0, x1) (vec)_mm_set_epi64x((x0), (x1))

#   if __SSSE3__
#       include <tmmintrin.h>
#       define ROTW8(x)  (vec)_mm_shuffle_epi8((__m128i)x,_mm_set_epi8(14,13,12,15,10,9,8,11,6,5,4,7,2,1,0,3))
#       define ROTW16(x) (vec)_mm_shuffle_epi8((__m128i)x,_mm_set_epi8(13,12,15,14,9,8,11,10,5,4,7,6,1,0,3,2))
#   else
#       define ROTW8(x)  (vec)(_mm_slli_epi32((__m128i)x, 8) ^ _mm_srli_epi32((__m128i)x,24))
#       define ROTW16(x) (vec)(_mm_slli_epi32((__m128i)x,16) ^ _mm_srli_epi32((__m128i)x,16))
#   endif
#else
#   error -- Implementation supports only machines with neon or SSE2
#endif

#ifndef REVV_BE
#   define REVV_BE(x)  (x)
#endif

#ifndef REVW_BE
#   define REVW_BE(x)  (x)
#endif

#define DQROUND_VECTORS(a,b,c,d)                \
    a += b; d ^= a; d = ROTW16(d);              \
    c += d; b ^= c; b = ROTW12(b);              \
    a += b; d ^= a; d = ROTW8(d);               \
    c += d; b ^= c; b = ROTW7(b);               \
    b = ROTV1(b); c = ROTV2(c);  d = ROTV3(d);  \
    a += b; d ^= a; d = ROTW16(d);              \
    c += d; b ^= c; b = ROTW12(b);              \
    a += b; d ^= a; d = ROTW8(d);               \
    c += d; b ^= c; b = ROTW7(b);               \
    b = ROTV3(b); c = ROTV2(c); d = ROTV1(d);

#define QROUND_WORDS(a,b,c,d) \
    a = a+b; d ^= a; d = d<<16 | d>>16; \
    c = c+d; b ^= c; b = b<<12 | b>>20; \
    a = a+b; d ^= a; d = d<< 8 | d>>24; \
    c = c+d; b ^= c; b = b<< 7 | b>>25;

#define WRITE_XOR(in, op, d, v0, v1, v2, v3)                        \
    *(vec *)(op + d +  0) = *(vec *)(in + d +  0) ^ v0;             \
    *(vec *)(op + d +  4) = *(vec *)(in + d +  4) ^ v1;             \
    *(vec *)(op + d +  8) = *(vec *)(in + d +  8) ^ v2;             \
    *(vec *)(op + d + 12) = *(vec *)(in + d + 12) ^ v3;

template<bool Aligned>
Y_FORCE_INLINE void WriteXor(ui32 *op, ui32 *ip,
        vec v0, vec v1, vec v2, vec v3,
        const vec i_v[4]) {
    if constexpr (Aligned) {
        *(vec *)(op +  0) = *(vec *)(ip +  0) ^ v0;
        *(vec *)(op +  4) = *(vec *)(ip +  4) ^ v1;
        *(vec *)(op +  8) = *(vec *)(ip +  8) ^ v2;
        *(vec *)(op + 12) = *(vec *)(ip + 12) ^ v3;
    } else {
        *(vec *)(op +  0) = i_v[0] ^ v0;
        *(vec *)(op +  4) = i_v[1] ^ v1;
        *(vec *)(op +  8) = i_v[2] ^ v2;
        *(vec *)(op + 12) = i_v[3] ^ v3;
    }
}

template<bool Aligned, bool IsFirst, bool IsLast>
Y_FORCE_INLINE void ReadW(ui32 *ip, vec i_v[4], vec& next_i_v) {
    if constexpr (Aligned) {
        return;
    }

    vec tmp;

    if constexpr (IsFirst) {
        next_i_v = SET(0, *(ui64*)(ip - 2));
    }

    tmp = *(vec*)(ip + 0);
    i_v[1] = SHIFT_RIGHT(tmp);
    i_v[0] = next_i_v | SHIFT_LEFT(tmp);

    tmp = *(vec*)(ip + 4);
    i_v[2] = SHIFT_RIGHT(tmp);
    i_v[1] = i_v[1] | SHIFT_LEFT(tmp);

    tmp = *(vec*)(ip + 8);
    i_v[3] = SHIFT_RIGHT(tmp);
    i_v[2] = i_v[2] | SHIFT_LEFT(tmp);

    if constexpr (IsLast) {
        i_v[3] = i_v[3] | SET(*(ui64*)(ip + 12), 0);
    } else {
        tmp = *(vec*)(ip + 12);
        next_i_v = SHIFT_RIGHT(tmp);
        i_v[3] = i_v[3] | SHIFT_LEFT(tmp);
    }
}


constexpr size_t ChaChaVec::KEY_SIZE;
constexpr size_t ChaChaVec::BLOCK_SIZE;

alignas(16) ui32 chacha_const[] = {
    0x61707865, 0x3320646e, 0x79622d32, 0x6b206574
};

void ChaChaVec::SetKey(const ui8* key, size_t size)
{
    Y_ASSERT((size == KEY_SIZE) && "key must be 32 bytes long");

    alignas(16) ui8 aligned_key[KEY_SIZE];
    memcpy(aligned_key, key, size);

    ui32 *kp;
    #if ( __ARM_NEON__ || __SSE2__ || defined(_arm64_))
        kp = (ui32*)aligned_key;
    #else
        alignas(16) ui32 k[4];
        ((vec *)k)[0] = ((vec *)aligned_key)[0];
        ((vec *)k)[1] = ((vec *)aligned_key)[1];
        kp = (ui32*)k;
    #endif
    s0_ = *(vec *)chacha_const;
    s1_ = ((vec *)kp)[0];
    s2_ = ((vec *)kp)[1];

    SecureWipeBuffer(aligned_key, KEY_SIZE);
}

void ChaChaVec::SetIV(const ui8* iv, const ui8* blockIdx)
{
    ui32 *np;
    ui32 *bp;
    #if ( __ARM_NEON__ || __SSE2__ || defined(_arm64_))
        np = (ui32*)iv;
        bp = (ui32*)blockIdx;
    #else
        alignas(16) ui32 nonce[2];
        nonce[0] = REVW_BE(((ui32*)iv)[0]);
        nonce[1] = REVW_BE(((ui32*)iv)[1]);
        np = (ui32*)nonce;
        alignas(16) ui32 idx[2];
        idx[0] = REVW_BE(((ui32*)blockIdx)[0]);
        idx[1] = REVW_BE(((ui32*)blockIdx)[1]);
        bp = (ui32*)idx;
    #endif
    s3_ = NONCE(np, bp);
}


void ChaChaVec::SetIV(const ui8* iv) {
    const ui8 zero[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    SetIV(iv, zero);
}

template<bool Aligned>
void ChaChaVec::EncipherImpl(const ui8* plaintext, ui8* ciphertext, size_t len)
{
    size_t iters, i;
    ui32* ip = (ui32*)AlignUp<intptr_t>(intptr_t(plaintext), 16);
    ui32 *op = (ui32*)ciphertext;

    const ui32 unalignment = intptr_t(plaintext) % 16;
    if constexpr (!Aligned) {
        Y_ABORT_UNLESS(unalignment == 8, "Unalignment# %d", (int)unalignment);
    }

    Y_ABORT_UNLESS(intptr_t(ip) % 16 == 0);
    Y_ABORT_UNLESS(intptr_t(op) % 16 == 0);

    // Unused if Aligned
    vec i_v[4];
    vec next_i_v;

    for (iters = 0; iters < len/(CHACHA_BPI * BLOCK_SIZE); iters++) {
        vec v0,v1,v2,v3,v4,v5,v6,v7;
        v4 = v0 = s0_; v5 = v1 = s1_; v6 = v2 = s2_; v3 = s3_;
        v7 = v3 + ONE;
        #if CHACHA_VBPI > 2
            vec v8,v9,v10,v11;
            v8 = v4; v9 = v5; v10 = v6;
            v11 =  v7 + ONE;
        #endif
        #if CHACHA_VBPI > 3
            vec v12,v13,v14,v15;
            v12 = v8; v13 = v9; v14 = v10;
            v15 = v11 + ONE;
        #endif
        #if CHACHA_GPR_TOO
            ui32* kp = (ui32*)&s1_;
            ui32* np = (ui32*)&s3_ + 2;

            ui32 x0, x1, x2, x3, x4, x5, x6, x7, x8,
                          x9, x10, x11, x12, x13, x14, x15;
            x0 = chacha_const[0]; x1 = chacha_const[1];
            x2 = chacha_const[2]; x3 = chacha_const[3];
            x4 = kp[0]; x5 = kp[1]; x6  = kp[2]; x7  = kp[3];
            x8 = kp[4]; x9 = kp[5]; x10 = kp[6]; x11 = kp[7];
            x12 = CHACHA_BPI*iters+(CHACHA_BPI-1); x13 = 0; x14 = np[0]; x15 = np[1];
        #endif
        for (i = rounds_/2; i; i--) {
            DQROUND_VECTORS(v0,v1,v2,v3)
            DQROUND_VECTORS(v4,v5,v6,v7)
            #if CHACHA_VBPI > 2
                DQROUND_VECTORS(v8,v9,v10,v11)
            #endif
            #if CHACHA_VBPI > 3
                DQROUND_VECTORS(v12,v13,v14,v15)
            #endif
            #if CHACHA_GPR_TOO
                QROUND_WORDS(x0, x4,  x8, x12)
                QROUND_WORDS(x1, x5,  x9, x13)
                QROUND_WORDS(x2, x6, x10, x14)
                QROUND_WORDS(x3, x7, x11, x15)
                QROUND_WORDS(x0, x5, x10, x15)
                QROUND_WORDS(x1, x6, x11, x12)
                QROUND_WORDS(x2, x7,  x8, x13)
                QROUND_WORDS(x3, x4,  x9, x14)
            #endif
        }
        ReadW<Aligned, true, false>(ip + 0, i_v, next_i_v);
        WriteXor<Aligned>(op + 0, ip + 0, v0+s0_, v1+s1_, v2+s2_, v3+s3_, i_v);
        s3_ += ONE;
        ReadW<Aligned, false, CHACHA_VBPI == 2>(ip + 16, i_v, next_i_v);
        WriteXor<Aligned>(op + 16, ip + 16, v4+s0_, v5+s1_, v6+s2_, v7+s3_, i_v);
        s3_ += ONE;
        #if CHACHA_VBPI > 2
            ReadW<Aligned, false, CHACHA_VBPI == 3>(ip + 32, i_v, next_i_v);
            WriteXor<Aligned>(op + 32, ip + 32, v8+s0_, v9+s1_, v10+s2_, v11+s3_, i_v);
            s3_ += ONE;
        #endif
        #if CHACHA_VBPI > 3
            ReadW<Aligned, false, CHACHA_VBPI == 4>(ip + 48, i_v, next_i_v);
            WriteXor<Aligned>(op + 48, ip + 48, v12+s0_, v13+s1_, v14+s2_, v15+s3_, i_v);
            s3_ += ONE;
        #endif
        ip += CHACHA_VBPI*16;
        Y_ASSERT(intptr_t(ip) % 16 == 0);
        op += CHACHA_VBPI*16;
        Y_ASSERT(intptr_t(op) % 16 == 0);
        #if CHACHA_GPR_TOO
            op[0]  = REVW_BE(REVW_BE(ip[0])  ^ (x0  + chacha_const[0]));
            op[1]  = REVW_BE(REVW_BE(ip[1])  ^ (x1  + chacha_const[1]));
            op[2]  = REVW_BE(REVW_BE(ip[2])  ^ (x2  + chacha_const[2]));
            op[3]  = REVW_BE(REVW_BE(ip[3])  ^ (x3  + chacha_const[3]));
            op[4]  = REVW_BE(REVW_BE(ip[4])  ^ (x4  + kp[0]));
            op[5]  = REVW_BE(REVW_BE(ip[5])  ^ (x5  + kp[1]));
            op[6]  = REVW_BE(REVW_BE(ip[6])  ^ (x6  + kp[2]));
            op[7]  = REVW_BE(REVW_BE(ip[7])  ^ (x7  + kp[3]));
            op[8]  = REVW_BE(REVW_BE(ip[8])  ^ (x8  + kp[4]));
            op[9]  = REVW_BE(REVW_BE(ip[9])  ^ (x9  + kp[5]));
            op[10] = REVW_BE(REVW_BE(ip[10]) ^ (x10 + kp[6]));
            op[11] = REVW_BE(REVW_BE(ip[11]) ^ (x11 + kp[7]));
            op[12] = REVW_BE(REVW_BE(ip[12]) ^ (x12 + CHACHA_BPI*iters+(CHACHA_BPI-1)));
            op[13] = REVW_BE(REVW_BE(ip[13]) ^ (x13));
            op[14] = REVW_BE(REVW_BE(ip[14]) ^ (x14 + np[0]));
            op[15] = REVW_BE(REVW_BE(ip[15]) ^ (x15 + np[1]));
            s3_ += ONE;
            ip += 16;
            op += 16;
        #endif
    }

    for (iters = len % (CHACHA_BPI*BLOCK_SIZE)/BLOCK_SIZE; iters != 0; iters--) {
        vec v0 = s0_, v1 = s1_, v2 = s2_, v3 = s3_;
        for (i = rounds_/2; i; i--) {
            DQROUND_VECTORS(v0,v1,v2,v3)
        }
        ReadW<Aligned, true, true>(ip, i_v, next_i_v);
        WriteXor<Aligned>(op, ip, v0+s0_, v1+s1_, v2+s2_, v3+s3_, i_v);
        s3_ += ONE;
        ip += 16;
        op += 16;
    }

    len = len % BLOCK_SIZE;
    if (len) {
        if (!Aligned) {
            // Unaligned version can work only on full blocks
            alignas(16) ui8 buf[BLOCK_SIZE];
            memcpy(buf, ip - 2, len);
            EncipherImpl<true>(buf, buf, len);
            memcpy(op, buf, len);
            SecureWipeBuffer(buf, BLOCK_SIZE);
        } else {
            alignas(16) char buf[16];
            vec tail;
            vec v0, v1, v2, v3;
            v0 = s0_; v1 = s1_; v2 = s2_; v3 = s3_;
            for (i = rounds_/2; i; i--) {
                DQROUND_VECTORS(v0,v1,v2,v3)
            }
            if (len >= 32) {
                *(vec *)(op + 0) = *(vec *)(ip + 0) ^ (v0 + s0_);
                *(vec *)(op + 4) = *(vec *)(ip + 4) ^ (v1 + s1_);
                if (len >= 48) {
                    *(vec *)(op + 8) = *(vec *)(ip + 8) ^ (v2 + s2_);
                    tail = v3 + s3_;
                    op += 12;
                    ip += 12;
                    len -= 48;
                } else {
                    tail = v2 + s2_;
                    op += 8;
                    ip += 8;
                    len -= 32;
                }
            } else if (len >= 16) {
                *(vec *)(op + 0) = *(vec *)(ip + 0) ^ (v0 + s0_);
                tail = v1 + s1_;
                op += 4;
                ip += 4;
                len -= 16;
            } else {
                tail = v0 + s0_;
            }
            memcpy(buf, ip, len);
            void *bp = buf;
            *(vec *)bp = tail ^ *(vec *)bp;
            memcpy(op, buf, len);
            SecureWipeBuffer(buf, 16);
        }
    }
}

// Old version, used only for compatibility tests
void ChaChaVec::EncipherOld(const ui8* plaintext, ui8* ciphertext, size_t len)
{
    size_t iters, i;
    ui32 *op=(ui32*)ciphertext, *ip=(ui32*)plaintext;

    for (iters = 0; iters < len/(CHACHA_BPI * BLOCK_SIZE); iters++) {
        vec v0,v1,v2,v3,v4,v5,v6,v7;
        v4 = v0 = s0_; v5 = v1 = s1_; v6 = v2 = s2_; v3 = s3_;
        v7 = v3 + ONE;
        #if CHACHA_VBPI > 2
            vec v8,v9,v10,v11;
            v8 = v4; v9 = v5; v10 = v6;
            v11 =  v7 + ONE;
        #endif
        #if CHACHA_VBPI > 3
            vec v12,v13,v14,v15;
            v12 = v8; v13 = v9; v14 = v10;
            v15 = v11 + ONE;
        #endif
        #if CHACHA_GPR_TOO
            ui32* kp = (ui32*)&s1_;
            ui32* np = (ui32*)&s3_ + 2;

            ui32 x0, x1, x2, x3, x4, x5, x6, x7, x8,
                          x9, x10, x11, x12, x13, x14, x15;
            x0 = chacha_const[0]; x1 = chacha_const[1];
            x2 = chacha_const[2]; x3 = chacha_const[3];
            x4 = kp[0]; x5 = kp[1]; x6  = kp[2]; x7  = kp[3];
            x8 = kp[4]; x9 = kp[5]; x10 = kp[6]; x11 = kp[7];
            x12 = CHACHA_BPI*iters+(CHACHA_BPI-1); x13 = 0; x14 = np[0]; x15 = np[1];
        #endif
        for (i = rounds_/2; i; i--) {
            DQROUND_VECTORS(v0,v1,v2,v3)
            DQROUND_VECTORS(v4,v5,v6,v7)
            #if CHACHA_VBPI > 2
                DQROUND_VECTORS(v8,v9,v10,v11)
            #endif
            #if CHACHA_VBPI > 3
                DQROUND_VECTORS(v12,v13,v14,v15)
            #endif
            #if CHACHA_GPR_TOO
                QROUND_WORDS(x0, x4,  x8, x12)
                QROUND_WORDS(x1, x5,  x9, x13)
                QROUND_WORDS(x2, x6, x10, x14)
                QROUND_WORDS(x3, x7, x11, x15)
                QROUND_WORDS(x0, x5, x10, x15)
                QROUND_WORDS(x1, x6, x11, x12)
                QROUND_WORDS(x2, x7,  x8, x13)
                QROUND_WORDS(x3, x4,  x9, x14)
            #endif
        }
        WRITE_XOR(ip, op, 0, v0+s0_, v1+s1_, v2+s2_, v3+s3_)
        s3_ += ONE;
        WRITE_XOR(ip, op, 16, v4+s0_, v5+s1_, v6+s2_, v7+s3_)
        s3_ += ONE;
        #if CHACHA_VBPI > 2
            WRITE_XOR(ip, op, 32, v8+s0_, v9+s1_, v10+s2_, v11+s3_)
            s3_ += ONE;
        #endif
        #if CHACHA_VBPI > 3
            WRITE_XOR(ip, op, 48, v12+s0_, v13+s1_, v14+s2_, v15+s3_)
            s3_ += ONE;
        #endif
        ip += CHACHA_VBPI*16;
        op += CHACHA_VBPI*16;
        #if CHACHA_GPR_TOO
            op[0]  = REVW_BE(REVW_BE(ip[0])  ^ (x0  + chacha_const[0]));
            op[1]  = REVW_BE(REVW_BE(ip[1])  ^ (x1  + chacha_const[1]));
            op[2]  = REVW_BE(REVW_BE(ip[2])  ^ (x2  + chacha_const[2]));
            op[3]  = REVW_BE(REVW_BE(ip[3])  ^ (x3  + chacha_const[3]));
            op[4]  = REVW_BE(REVW_BE(ip[4])  ^ (x4  + kp[0]));
            op[5]  = REVW_BE(REVW_BE(ip[5])  ^ (x5  + kp[1]));
            op[6]  = REVW_BE(REVW_BE(ip[6])  ^ (x6  + kp[2]));
            op[7]  = REVW_BE(REVW_BE(ip[7])  ^ (x7  + kp[3]));
            op[8]  = REVW_BE(REVW_BE(ip[8])  ^ (x8  + kp[4]));
            op[9]  = REVW_BE(REVW_BE(ip[9])  ^ (x9  + kp[5]));
            op[10] = REVW_BE(REVW_BE(ip[10]) ^ (x10 + kp[6]));
            op[11] = REVW_BE(REVW_BE(ip[11]) ^ (x11 + kp[7]));
            op[12] = REVW_BE(REVW_BE(ip[12]) ^ (x12 + CHACHA_BPI*iters+(CHACHA_BPI-1)));
            op[13] = REVW_BE(REVW_BE(ip[13]) ^ (x13));
            op[14] = REVW_BE(REVW_BE(ip[14]) ^ (x14 + np[0]));
            op[15] = REVW_BE(REVW_BE(ip[15]) ^ (x15 + np[1]));
            s3_ += ONE;
            ip += 16;
            op += 16;
        #endif
    }
    for (iters = len % (CHACHA_BPI*BLOCK_SIZE)/BLOCK_SIZE; iters != 0; iters--) {
        vec v0 = s0_, v1 = s1_, v2 = s2_, v3 = s3_;
        for (i = rounds_/2; i; i--) {
            DQROUND_VECTORS(v0,v1,v2,v3)
        }
        WRITE_XOR(ip, op, 0, v0+s0_, v1+s1_, v2+s2_, v3+s3_)
        s3_ += ONE;
        ip += 16;
        op += 16;
    }
    len = len % BLOCK_SIZE;
    if (len) {
        alignas(16) char buf[16];
        vec tail;
        vec v0, v1, v2, v3;
        v0 = s0_; v1 = s1_; v2 = s2_; v3 = s3_;
        for (i = rounds_/2; i; i--) {
            DQROUND_VECTORS(v0,v1,v2,v3)
        }
        if (len >= 32) {
            *(vec *)(op + 0) = *(vec *)(ip + 0) ^ REVV_BE(v0 + s0_);
            *(vec *)(op + 4) = *(vec *)(ip + 4) ^ REVV_BE(v1 + s1_);
            if (len >= 48) {
                *(vec *)(op + 8) = *(vec *)(ip + 8) ^ REVV_BE(v2 + s2_);
                tail = REVV_BE(v3 + s3_);
                op += 12;
                ip += 12;
                len -= 48;
            } else {
                tail = REVV_BE(v2 + s2_);
                op += 8;
                ip += 8;
                len -= 32;
            }
        } else if (len >= 16) {
            *(vec *)(op + 0) = *(vec *)(ip + 0) ^ REVV_BE(v0 + s0_);
            tail = REVV_BE(v1 + s1_);
            op += 4;
            ip += 4;
            len -= 16;
        } else tail = REVV_BE(v0 + s0_);
        memcpy(buf, ip, len);
        void *bp = buf;
        *(vec *)bp = tail ^ *(vec *)bp;
        memcpy(op, buf, len);
        SecureWipeBuffer(buf, 16);
    }
}

void ChaChaVec::Encipher(const ui8* plaintext, ui8* ciphertext, size_t len)
{
    const ui32 input_unalignment = intptr_t(plaintext) % 16;
    if (input_unalignment == 0) {
        EncipherImpl<true>(plaintext, ciphertext, len);
    } else if (input_unalignment == 8) {
        EncipherImpl<false>(plaintext, ciphertext, len);
    } else {
        Y_ABORT("ChaChaVec can work only with input aligned on 8, 16 or more bytes");
    }
}

void ChaChaVec::Decipher(const ui8* ciphertext, ui8* plaintext, size_t len)
{
    Encipher(ciphertext, plaintext, len);
}

ChaChaVec::~ChaChaVec() {
    SecureWipeBuffer((ui8*)&s1_, 16);
    SecureWipeBuffer((ui8*)&s2_, 16);
}
