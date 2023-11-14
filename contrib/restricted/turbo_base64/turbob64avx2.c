/**
Copyright (c) 2016-2019, Powturbo
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    - homepage : https://sites.google.com/site/powturbo/
    - github   : https://github.com/powturbo
    - twitter  : https://twitter.com/powturbo
    - email    : powturbo [_AT_] gmail [_DOT_] com
**/
// SSE + AVX2 Based on:
// http://0x80.pl/articles/index.html#base64-algorithm-update
// https://arxiv.org/abs/1704.00605
// https://gist.github.com/aqrit/a2ccea48d7cac7e9d4d99f19d4759666 (decode)

#include <immintrin.h>

#define UA_MEMCPY
#include "conf.h"
#include "turbob64.h"
#include "turbob64_.h"

//--------------------- Decode ----------------------------------------------------------------------
#define MM256_PACK8TO6(v,cpv) {\
  const __m256i merge_ab_and_bc = _mm256_maddubs_epi16(v,            _mm256_set1_epi32(0x01400140));\
                              v = _mm256_madd_epi16(merge_ab_and_bc, _mm256_set1_epi32(0x00011000));\
                              v = _mm256_shuffle_epi8(v, cpv);\
}

#define MM256_MAP8TO6(iv, shifted, delta_asso, delta_values, ov) { /*map 8-bits ascii to 6-bits bin*/\
                shifted    = _mm256_srli_epi32(iv, 3);\
  const __m256i delta_hash = _mm256_avg_epu8(_mm256_shuffle_epi8(delta_asso, iv), shifted);\
                        ov = _mm256_add_epi8(_mm256_shuffle_epi8(delta_values, delta_hash), iv);\
}

#define MM256_B64CHK(iv, shifted, check_asso, check_values, vx) {\
  const __m256i check_hash = _mm256_avg_epu8( _mm256_shuffle_epi8(check_asso, iv), shifted);\
  const __m256i        chk = _mm256_adds_epi8(_mm256_shuffle_epi8(check_values, check_hash), iv);\
                        vx = _mm256_or_si256(vx, chk);\
}

#define OVD 8
size_t tb64avx2dec(const unsigned char *in, size_t inlen, unsigned char *out) {
  if(inlen >= 32+OVD) { 
    const unsigned char *ip;
          unsigned char *op; 
    __m256i vx = _mm256_setzero_si256();
    const __m256i delta_asso   = _mm256_setr_epi8(0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,   0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x0f,
                                                  0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,   0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x0f);
    const __m256i delta_values = _mm256_setr_epi8(0x00, 0x00, 0x00, 0x13, 0x04, 0xbf, 0xbf, 0xb9,   0xb9, 0x00, 0x10, 0xc3, 0xbf, 0xbf, 0xb9, 0xb9,
                                                  0x00, 0x00, 0x00, 0x13, 0x04, 0xbf, 0xbf, 0xb9,   0xb9, 0x00, 0x10, 0xc3, 0xbf, 0xbf, 0xb9, 0xb9);
      #ifndef NB64CHECK
    const __m256i check_asso   = _mm256_setr_epi8(0x0d, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,   0x01, 0x01, 0x03, 0x07, 0x0b, 0x0b, 0x0b, 0x0f,
                                                  0x0d, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,   0x01, 0x01, 0x03, 0x07, 0x0b, 0x0b, 0x0b, 0x0f);
    const __m256i check_values = _mm256_setr_epi8(0x80, 0x80, 0x80, 0x80, 0xcf, 0xbf, 0xd5, 0xa6,   0xb5, 0x86, 0xd1, 0x80, 0xb1, 0x80, 0x91, 0x80,
                                                  0x80, 0x80, 0x80, 0x80, 0xcf, 0xbf, 0xd5, 0xa6,   0xb5, 0x86, 0xd1, 0x80, 0xb1, 0x80, 0x91, 0x80);
      #endif
          __m256i          cpv = _mm256_set_epi8( -1, -1, -1, -1, 12, 13, 14,  8,    9, 10,  4,  5,  6,  0,  1,  2,
                                                  -1, -1, -1, -1, 12, 13, 14,  8,    9, 10,  4,  5,  6,  0,  1,  2);

    for(ip = in, op = out; ip < (in+inlen)-(64+OVD); ip += 64, op += (64/4)*3) {           
      __m256i          iv0 = _mm256_loadu_si256((__m256i *)ip);    
      __m256i          iv1 = _mm256_loadu_si256((__m256i *)(ip+32)); 
   
      __m256i ov0,shifted0; MM256_MAP8TO6(iv0, shifted0, delta_asso, delta_values, ov0); MM256_PACK8TO6(ov0, cpv);
      __m256i ov1,shifted1; MM256_MAP8TO6(iv1, shifted1, delta_asso, delta_values, ov1); MM256_PACK8TO6(ov1, cpv); PREFETCH(ip,1024,0); 
      
      _mm_storeu_si128((__m128i*) op,       _mm256_castsi256_si128(ov0));
      _mm_storeu_si128((__m128i*)(op + 12), _mm256_extracti128_si256(ov0, 1));                          
      _mm_storeu_si128((__m128i*)(op + 24), _mm256_castsi256_si128(ov1));
      _mm_storeu_si128((__m128i*)(op + 36), _mm256_extracti128_si256(ov1, 1));                         
 
      CHECK0(MM256_B64CHK(iv0, shifted0, check_asso, check_values, vx));
      CHECK1(MM256_B64CHK(iv1, shifted1, delta_asso, delta_values, vx));
    }

    if(ip < (in+inlen)-(32+OVD)) {
      __m256i          iv0 = _mm256_loadu_si256((__m256i *)ip);      
      __m256i ov0,shifted0; MM256_MAP8TO6(iv0, shifted0, delta_asso, delta_values, ov0); MM256_PACK8TO6(ov0, cpv);
      
      _mm_storeu_si128((__m128i*) op,       _mm256_castsi256_si128(ov0));
      _mm_storeu_si128((__m128i*)(op + 12), _mm256_extracti128_si256(ov0, 1));                          
      ip +=  32;
      op += (32/4)*3;
      CHECK1(MM256_B64CHK(iv0, shifted0, check_asso, check_values, vx));
    }
    size_t rc;
    if(!(rc = _tb64xdec(ip, inlen-(ip-in), op)) || _mm256_movemask_epi8(vx)) return 0;
    return (op-out)+rc; 
  }
  return _tb64xdec(in, inlen, out);
}

//-------------------- Encode ----------------------------------------------------------------------
static ALWAYS_INLINE __m256i mm256_map6to8(const __m256i v) { /*map 6-bits bin to 8-bits ascii (https://arxiv.org/abs/1704.00605) */
  __m256i vidx = _mm256_subs_epu8(v,   _mm256_set1_epi8(51));
          vidx = _mm256_sub_epi8(vidx, _mm256_cmpgt_epi8(v, _mm256_set1_epi8(25)));

  const __m256i offsets = _mm256_set_epi8(0, 0, -16, -19, -4, -4, -4, -4,   -4, -4, -4, -4, -4, -4, 71, 65,
                                          0, 0, -16, -19, -4, -4, -4, -4,   -4, -4, -4, -4, -4, -4, 71, 65);
  return _mm256_add_epi8(v, _mm256_shuffle_epi8(offsets, vidx));
}

static ALWAYS_INLINE __m256i mm256_unpack6to8(__m256i v) { /* https://arxiv.org/abs/1704.00605 p.12*/
  __m256i va = _mm256_mulhi_epu16(_mm256_and_si256(v, _mm256_set1_epi32(0x0fc0fc00)), _mm256_set1_epi32(0x04000040));
  __m256i vb = _mm256_mullo_epi16(_mm256_and_si256(v, _mm256_set1_epi32(0x003f03f0)), _mm256_set1_epi32(0x01000010));
  return _mm256_or_si256(va, vb);
}

size_t tb64avx2enc(const unsigned char* in, size_t inlen, unsigned char *out) {
  const unsigned char *ip = in; 
        unsigned char *op = out;
        size_t   outlen = TB64ENCLEN(inlen);
  
  if(outlen >= 32+OVD) {
    const __m256i    shuf = _mm256_set_epi8(10,11, 9,10, 7, 8, 6, 7, 4,   5, 3, 4, 1, 2, 0, 1,
                                            10,11, 9,10, 7, 8, 6, 7, 4,   5, 3, 4, 1, 2, 0, 1);

    for(; op <= (out+outlen)-(64+OVD); op += 64, ip += (64/4)*3) {     								         
      __m256i v0 = _mm256_castsi128_si256(    _mm_loadu_si128((__m128i *) ip));      
              v0 = _mm256_inserti128_si256(v0,_mm_loadu_si128((__m128i *)(ip+12)),1);   
      __m256i v1 = _mm256_castsi128_si256(    _mm_loadu_si128((__m128i *)(ip+24)));      
              v1 = _mm256_inserti128_si256(v1,_mm_loadu_si128((__m128i *)(ip+36)),1);   

      v0 = _mm256_shuffle_epi8(v0, shuf); v0 = mm256_unpack6to8(v0); v0 = mm256_map6to8(v0);                                                                                                           
      v1 = _mm256_shuffle_epi8(v1, shuf); v1 = mm256_unpack6to8(v1); v1 = mm256_map6to8(v1);  PREFETCH(ip, 1024, 0);   

      _mm256_storeu_si256((__m256i*) op,     v0);                                            
      _mm256_storeu_si256((__m256i*)(op+32), v1);    
    }
    if(op <= (out+outlen)-(32+OVD)) {
      __m256i v0 = _mm256_castsi128_si256(    _mm_loadu_si128((__m128i *) ip));      
              v0 = _mm256_inserti128_si256(v0,_mm_loadu_si128((__m128i *)(ip+12)),1);   
      v0 = _mm256_shuffle_epi8(v0, shuf); v0 = mm256_unpack6to8(v0); v0 = mm256_map6to8(v0);                                                                                                           
      _mm256_storeu_si256((__m256i*) op, v0);                                            
      op +=  32; 
      ip += (32/4)*3;
    }
  }
  EXTAIL();
  return outlen;
}

//------- optimized functions for short strings only --------------------------
// can read beyond the input buffer end, 
// therefore input buffer size must be 32 bytes larger than input length

size_t _tb64avx2dec(const unsigned char *in, size_t inlen, unsigned char *out) {
  if(inlen >= 16) { 
    const unsigned char *ip;
          unsigned char *op; 
    const __m256i delta_asso   = _mm256_setr_epi8(0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,   0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x0f,
                                                  0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,   0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x0f);
    const __m256i delta_values = _mm256_setr_epi8(0x00, 0x00, 0x00, 0x13, 0x04, 0xbf, 0xbf, 0xb9,   0xb9, 0x00, 0x10, 0xc3, 0xbf, 0xbf, 0xb9, 0xb9,
                                                  0x00, 0x00, 0x00, 0x13, 0x04, 0xbf, 0xbf, 0xb9,   0xb9, 0x00, 0x10, 0xc3, 0xbf, 0xbf, 0xb9, 0xb9);
      #ifndef NB64CHECK
    const __m256i check_asso   = _mm256_setr_epi8(0x0d, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,   0x01, 0x01, 0x03, 0x07, 0x0b, 0x0b, 0x0b, 0x0f,
                                                  0x0d, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,   0x01, 0x01, 0x03, 0x07, 0x0b, 0x0b, 0x0b, 0x0f);
    const __m256i check_values = _mm256_setr_epi8(0x80, 0x80, 0x80, 0x80, 0xcf, 0xbf, 0xd5, 0xa6,   0xb5, 0x86, 0xd1, 0x80, 0xb1, 0x80, 0x91, 0x80,
                                                  0x80, 0x80, 0x80, 0x80, 0xcf, 0xbf, 0xd5, 0xa6,   0xb5, 0x86, 0xd1, 0x80, 0xb1, 0x80, 0x91, 0x80);
      #endif
    const __m256i          cpv = _mm256_set_epi8( -1, -1, -1, -1, 12, 13, 14,  8,    9, 10,  4,  5,  6,  0,  1,  2,
                                                  -1, -1, -1, -1, 12, 13, 14,  8,    9, 10,  4,  5,  6,  0,  1,  2);
    __m256i vx = _mm256_setzero_si256();
    for(ip = in, op = out; ip < (in+inlen)-32; ip += 32, op += (32/4)*3) {
      __m256i          iv0 = _mm256_loadu_si256((__m256i *)ip);
      __m256i ov0,shifted0; MM256_MAP8TO6(iv0, shifted0, delta_asso, delta_values, ov0); MM256_PACK8TO6(ov0, cpv);
      
      _mm_storeu_si128((__m128i*) op,       _mm256_castsi256_si128(ov0));
      _mm_storeu_si128((__m128i*)(op + 12), _mm256_extracti128_si256(ov0, 1));                          
      CHECK0(MM256_B64CHK(iv0, shifted0, check_asso, check_values, vx));
    }

    unsigned cx;
    if(ip < (in+inlen)-16) {
      __m128i iv0 = _mm_loadu_si128((__m128i *) ip);
      __m128i _vx = _mm_or_si128(_mm256_extracti128_si256(vx, 1), _mm256_castsi256_si128(vx));
      __m128i ov0, shifted0; MM_MAP8TO6( iv0, shifted0,_mm256_castsi256_si128(delta_asso),_mm256_castsi256_si128(delta_values), ov0); 
	                         MM_PACK8TO6(ov0, _mm256_castsi256_si128(cpv));
      _mm_storeu_si128((__m128i*) op, ov0);  
      ip += 16; op += (16/4)*3;
      CHECK0(MM_B64CHK(iv0, shifted0, _mm256_castsi256_si128(check_asso), _mm256_castsi256_si128(check_values), _vx));
      cx = _mm_movemask_epi8(_vx);
    } else
      cx = _mm256_movemask_epi8(vx);

    size_t rc = _tb64xd(ip, inlen-(ip-in), op);
    if(!rc || cx) return 0;
    return (op-out)+rc;
  }
  return _tb64xd(in, inlen, out);
}

size_t _tb64avx2enc(const unsigned char* in, size_t inlen, unsigned char *out) {
  const unsigned char *ip = in; 
        unsigned char *op = out;
        size_t   outlen = TB64ENCLEN(inlen);
  if(outlen >= 32+4) { 
    const __m256i    shuf = _mm256_set_epi8(10,11, 9,10, 7, 8, 6, 7, 4,   5, 3, 4, 1, 2, 0, 1,
                                            10,11, 9,10, 7, 8, 6, 7, 4,   5, 3, 4, 1, 2, 0, 1);

    for(; op <= (out+outlen)-32; op += 32, ip += (32/4)*3) {
      __m256i v0 = _mm256_castsi128_si256(    _mm_loadu_si128((__m128i *) ip));      
              v0 = _mm256_inserti128_si256(v0,_mm_loadu_si128((__m128i *)(ip+12)),1);   
      v0 = _mm256_shuffle_epi8(v0, shuf); v0 = mm256_unpack6to8(v0); v0 = mm256_map6to8(v0);                                                                                                           
      _mm256_storeu_si256((__m256i*) op,     v0);                                            
    }
  }
  if(op <= (out+outlen)-(16+4)) {
    const __m128i    shuf = _mm_set_epi8(10,11, 9,10, 7, 8, 6, 7, 4,   5, 3, 4, 1, 2, 0, 1);

    __m128i v0 = _mm_loadu_si128((__m128i*)ip);
            v0 = _mm_shuffle_epi8(v0, shuf);
            v0 = mm_unpack6to8(v0);
            v0 = mm_map6to8(v0);
    _mm_storeu_si128((__m128i*) op, v0);                                          
    op += 16; ip += (16/4)*3;
  }
  EXTAIL();
  return outlen;
}

