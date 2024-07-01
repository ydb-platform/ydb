#pragma once

#include <immintrin.h>
#include <util/system/types.h>

// round selector, specified values:
//  8:  low security - high speed
// 12:  mid security -  mid speed
// 20: high security -  low speed
#ifndef CHACHA_ROUNDS
#define CHACHA_ROUNDS 8
#endif

#ifdef __AVX512F__
typedef unsigned vec128 __attribute__ ((vector_size (16)));
#define XOR128(a,b)	(vec128)_mm_xor_si128((__m128i)a, (__m128i)b)
#define LOAD128(m)	(vec128)_mm_loadu_si128((__m128i*)(m))
#define STORE128(m,r)	_mm_storeu_si128((__m128i*)(m), (__m128i) (r))
#define WRITE_XOR_128(ip, op, d, v0, v1, v2, v3)	\
    STORE128(op + d + 0, XOR128(LOAD128(ip + d + 0), v0));	\
    STORE128(op + d + 4, XOR128(LOAD128(ip + d + 4), v1));	\
    STORE128(op + d + 8, XOR128(LOAD128(ip + d + 8), v2));	\
    STORE128(op + d +12, XOR128(LOAD128(ip + d +12), v3));
typedef unsigned vec256 __attribute__ ((vector_size (32)));
#define TWO	_mm256_set_epi64x(0,2,0,2)
#define LOAD256(m)		(vec256)_mm256_loadu_si256((__m256i*)(m))
#define STORE256(m,r)	_mm256_storeu_si256((__m256i*)(m), (__m256i) (r))
#define LOW128(x)	(vec128)_mm256_castsi256_si128((__m256i) (x))
#define HIGH128(x)	(vec128)_mm256_extractf128_si256((__m256i) (x), 1)
#define ADD256_32(a,b)	(vec256)_mm256_add_epi32((__m256i)a, (__m256i)b)
#define ADD256_64(a,b)	(vec256)_mm256_add_epi64((__m256i)a, (__m256i)b)
#define XOR256(a,b)	(vec256)_mm256_xor_si256((__m256i)a, (__m256i)b)
#define ROR256_V1(x)	(vec256)_mm256_shuffle_epi32((__m256i)x,_MM_SHUFFLE(0,3,2,1))
#define ROR256_V2(x)	(vec256)_mm256_shuffle_epi32((__m256i)x,_MM_SHUFFLE(1,0,3,2))
#define ROR256_V3(x)	(vec256)_mm256_shuffle_epi32((__m256i)x,_MM_SHUFFLE(2,1,0,3))
#define ROL256_7(x)		XOR256(_mm256_slli_epi32((__m256i)x, 7), _mm256_srli_epi32((__m256i)x,25))
#define ROL256_12(x)	XOR256(_mm256_slli_epi32((__m256i)x,12), _mm256_srli_epi32((__m256i)x,20))
#define ROL256_8(x)		(vec256)_mm256_shuffle_epi8((__m256i)x,_mm256_set_epi8(14,13,12,15,	\
                                                                               10, 9, 8,11,	\
                                                                                6, 5, 4, 7,	\
                                                                                2, 1, 0, 3,	\
                                                                               14,13,12,15,	\
                                                                               10, 9, 8,11,	\
                                                                                6, 5, 4, 7,	\
                                                                                2, 1, 0, 3))
#define ROL256_16(x)	(vec256)_mm256_shuffle_epi8((__m256i)x,_mm256_set_epi8(13,12,15,14,	\
                                                                                9, 8,11,10,	\
                                                                                5, 4, 7, 6,	\
                                                                                1, 0, 3, 2,	\
                                                                               13,12,15,14,	\
                                                                                9, 8,11,10,	\
                                                                                5, 4, 7, 6,	\
                                                                                1, 0, 3, 2))
#define DQROUND_VECTORS_256(a,b,c,d)						\
    a = ADD256_32(a,b); d = XOR256(d,a); d = ROL256_16(d);	\
    c = ADD256_32(c,d); b = XOR256(b,c); b = ROL256_12(b);	\
    a = ADD256_32(a,b); d = XOR256(d,a); d = ROL256_8(d);	\
    c = ADD256_32(c,d); b = XOR256(b,c); b = ROL256_7(b);	\
    b = ROR256_V1(b); c = ROR256_V2(c); d = ROR256_V3(d);	\
    a = ADD256_32(a,b); d = XOR256(d,a); d = ROL256_16(d);	\
    c = ADD256_32(c,d); b = XOR256(b,c); b = ROL256_12(b);	\
    a = ADD256_32(a,b); d = XOR256(d,a); d = ROL256_8(d);	\
    c = ADD256_32(c,d); b = XOR256(b,c); b = ROL256_7(b);	\
    b = ROR256_V3(b); c = ROR256_V2(c); d = ROR256_V1(d);
#define WRITE_XOR_256(ip, op, d, v0, v1, v2, v3)							\
    STORE256(op + d + 0, XOR256(LOAD256(ip + d + 0), _mm256_permute2x128_si256((__m256i)v0, (__m256i)v1, 0x20)));	\
    STORE256(op + d + 8, XOR256(LOAD256(ip + d + 8), _mm256_permute2x128_si256((__m256i)v2, (__m256i)v3, 0x20)));	\
    STORE256(op + d +16, XOR256(LOAD256(ip + d +16), _mm256_permute2x128_si256((__m256i)v0, (__m256i)v1, 0x31)));	\
    STORE256(op + d +24, XOR256(LOAD256(ip + d +24), _mm256_permute2x128_si256((__m256i)v2, (__m256i)v3, 0x31)));
typedef unsigned vec512 __attribute__ ((vector_size (64)));
typedef long long __m512i __attribute__ ((__vector_size__ (64), __may_alias__));
#define ONE_	_mm512_set_epi64(0,1,0,1,0,1,0,1)
#define TWO_	_mm512_set_epi64(0,2,0,2,0,2,0,2)
#define THREE_	_mm512_set_epi64(0,3,0,3,0,3,0,3)
#define FOUR	_mm512_set_epi64(0,4,0,4,0,4,0,4)
#define LOAD512(m)		(vec512)_mm512_loadu_si512((__m512i*)(m))
#define STORE512(m,r)	_mm512_storeu_si512((__m512i*)(m), (__m512i) (r))
#define LOW256(x)	(vec256)_mm512_extracti64x4_epi64((__m512i) (x), 0)
#define HIGH256(x)	(vec256)_mm512_extracti64x4_epi64((__m512i) (x), 1)
#define ADD512_32(a,b)	(vec512)_mm512_add_epi32((__m512i)a, (__m512i)b)
#define ADD512_64(a,b)	(vec512)_mm512_add_epi64((__m512i)a, (__m512i)b)
#define XOR512(a,b)	(vec512)_mm512_xor_si512((__m512i)a, (__m512i)b)
#define ROR512_V1(x)	(vec512)_mm512_shuffle_epi32((__m512i)x,_MM_SHUFFLE(0,3,2,1))
#define ROR512_V2(x)	(vec512)_mm512_shuffle_epi32((__m512i)x,_MM_SHUFFLE(1,0,3,2))
#define ROR512_V3(x)	(vec512)_mm512_shuffle_epi32((__m512i)x,_MM_SHUFFLE(2,1,0,3))
#define ROL512(x,b)		(vec512)_mm512_rol_epi32((__m512i)x, b)
#define DQROUND_VECTORS_512(a,b,c,d)						\
    a = ADD512_32(a,b); d = XOR512(d,a); d = ROL512(d,16);	\
    c = ADD512_32(c,d); b = XOR512(b,c); b = ROL512(b,12);	\
    a = ADD512_32(a,b); d = XOR512(d,a); d = ROL512(d, 8);	\
    c = ADD512_32(c,d); b = XOR512(b,c); b = ROL512(b, 7);	\
    b = ROR512_V1(b); c = ROR512_V2(c); d = ROR512_V3(d);	\
    a = ADD512_32(a,b); d = XOR512(d,a); d = ROL512(d,16);	\
    c = ADD512_32(c,d); b = XOR512(b,c); b = ROL512(b,12);	\
    a = ADD512_32(a,b); d = XOR512(d,a); d = ROL512(d, 8);	\
    c = ADD512_32(c,d); b = XOR512(b,c); b = ROL512(b, 7);	\
    b = ROR512_V3(b); c = ROR512_V2(c); d = ROR512_V1(d);
#define WRITE_XOR_512(ip, op, d, v0, v1, v2, v3)																		\
    STORE512(op + d + 0, XOR512(LOAD512(ip + d + 0),																	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(1,0,7,6,5,4,3,2), (__m512i)(v3)), 0x0fff,	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(3,2,1,0,7,6,5,4), (__m512i)(v2)), 0xf0ff,	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(5,4,3,2,1,0,7,6), (__m512i)(v1)), 0xff0f,	\
            (__m512i)(v0))))));																							\
    STORE512(op + d +16, XOR512(LOAD512(ip + d +16),																	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(3,2,1,0,7,6,5,4), (__m512i)(v3)), 0x0fff,	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(5,4,3,2,1,0,7,6), (__m512i)(v2)), 0xf0ff,	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(1,0,7,6,5,4,3,2), (__m512i)(v0)), 0xfff0,	\
            (__m512i)(v1))))));																							\
    STORE512(op + d +32, XOR512(LOAD512(ip + d +32),																	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(5,4,3,2,1,0,7,6), (__m512i)(v3)), 0x0fff,	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(1,0,7,6,5,4,3,2), (__m512i)(v1)), 0xff0f,	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(3,2,1,0,7,6,5,4), (__m512i)(v0)), 0xfff0,	\
            (__m512i)(v2))))));																							\
    STORE512(op + d +48, XOR512(LOAD512(ip + d +48),																	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(1,0,7,6,5,4,3,2), (__m512i)(v2)), 0xf0ff,	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(3,2,1,0,7,6,5,4), (__m512i)(v1)), 0xff0f,	\
            _mm512_mask_mov_epi32(_mm512_permutexvar_epi64(_mm512_set_epi64(5,4,3,2,1,0,7,6), (__m512i)(v0)), 0xfff0,	\
            (__m512i)(v3))))));
#endif

class ChaCha512
{
public:
    using NonceType = ui64;
    static constexpr size_t KEY_SIZE = 32;
    static constexpr size_t BLOCK_SIZE = 64;
    
    alignas(16) static constexpr ui32 chacha_const[] = {
        0x61707865, 0x3320646e, 0x79622d32, 0x6b206574
    };

public:
    ChaCha512(ui8 rounds = CHACHA_ROUNDS): rounds_(rounds) {}

    void SetKey(const ui8* key, size_t size);
    void SetIV(const ui8* iv, const ui8* blockIdx);
    void SetIV(const ui8* iv);
    void Encipher(const ui8* plaintext, ui8* ciphertext, size_t size);

    ~ChaCha512();
private:
    void EncipherImpl(const ui8* plaintext, ui8* ciphertext, size_t len);

#ifdef __AVX512F__
    vec512 q0_, q1_, q2_, q3_;
#endif
    ui8 rounds_;
};
