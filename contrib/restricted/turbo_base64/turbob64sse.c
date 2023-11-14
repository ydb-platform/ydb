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
// TubeBase64: ssse3 + arm neon functions (see also turbob64avx2)

#include <string.h>

  #if defined(__AVX__)
#include <immintrin.h>
#define FUNPREF tb64avx
  #elif defined(__SSE4_1__)
#include <smmintrin.h>
#define FUNPREF tb64sse
  #elif defined(__SSSE3__)
    #ifdef __powerpc64__
#define __SSE__   1
#define __SSE2__  1
#define __SSE3__  1
#define NO_WARN_X86_INTRINSICS 1
    #endif
#define FUNPREF tb64sse
#include <tmmintrin.h>
  #elif defined(__SSE2__)
#include <emmintrin.h>
  #elif defined(__ARM_NEON)
#include <arm_neon.h>
  #endif
  
#define UA_MEMCPY
#include "conf.h"
#include "turbob64.h"
#include "turbob64_.h"

#ifdef __ARM_NEON  //----------------------------------- arm neon --------------------------------

#define _ 0xff // invald entry
static const unsigned char lut[] = {
 _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
 _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
 _, _, _, _, _, _, _, _, _, _, _,62, _, _, _,63,
52,53,54,55,56,57,58,59,60,61, _, _, _, _, _, _,
 _, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
15,16,17,18,19,20,21,22,23,24,25, _, _, _, _, _,
 _,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
41,42,43,44,45,46,47,48,49,50,51, _, _, _, _, _,
};
#undef _

  #ifndef vld1q_u8_x4
static inline uint8x16x4_t vld1q_u8_x4(const uint8_t *lut) {
  uint8x16x4_t v;
  v.val[0] = vld1q_u8(lut);
  v.val[1] = vld1q_u8(lut+16);
  v.val[2] = vld1q_u8(lut+32);
  v.val[3] = vld1q_u8(lut+48);
  return v;
}
  #endif

#define B64D(iv, ov) {\
    iv.val[0] = vqtbx4q_u8(vqtbl4q_u8(vlut1, veorq_u8(iv.val[0], cv40)), vlut0, iv.val[0]);\
    iv.val[1] = vqtbx4q_u8(vqtbl4q_u8(vlut1, veorq_u8(iv.val[1], cv40)), vlut0, iv.val[1]);\
    iv.val[2] = vqtbx4q_u8(vqtbl4q_u8(vlut1, veorq_u8(iv.val[2], cv40)), vlut0, iv.val[2]);\
    iv.val[3] = vqtbx4q_u8(vqtbl4q_u8(vlut1, veorq_u8(iv.val[3], cv40)), vlut0, iv.val[3]);\
\
	ov.val[0] = vorrq_u8(vshlq_n_u8(iv.val[0], 2), vshrq_n_u8(iv.val[1], 4));\
	ov.val[1] = vorrq_u8(vshlq_n_u8(iv.val[1], 4), vshrq_n_u8(iv.val[2], 2));\
	ov.val[2] = vorrq_u8(vshlq_n_u8(iv.val[2], 6),            iv.val[3]    );\
}

#define _MM_B64CHK(iv, xv) xv = vorrq_u8(xv, vorrq_u8(vorrq_u8(iv.val[0], iv.val[1]), vorrq_u8(iv.val[2], iv.val[3])))

size_t tb64ssedec(const unsigned char *in, size_t inlen, unsigned char *out) {
  const unsigned char *ip;
        unsigned char *op; 
  const uint8x16x4_t vlut0 = vld1q_u8_x4( lut),
                     vlut1 = vld1q_u8_x4(&lut[64]);
  const uint8x16_t    cv40 = vdupq_n_u8(0x40);
        uint8x16_t      xv = vdupq_n_u8(0);
  #define ND 256
  for(ip = in, op = out; ip != in+(inlen&~(ND-1)); ip += ND, op += (ND/4)*3) { PREFETCH(ip,256,0);	
    uint8x16x4_t iv0 = vld4q_u8(ip),
                 iv1 = vld4q_u8(ip+64);                                                    
	uint8x16x3_t ov0,ov1; 
    B64D(iv0, ov0);
      #if ND > 128
	CHECK1(_MM_B64CHK(iv0,xv));
      #else
	CHECK0(_MM_B64CHK(iv0,xv));
      #endif
	B64D(iv1, ov1); CHECK1(_MM_B64CHK(iv1,xv));
      #if ND > 128
    iv0 = vld4q_u8(ip+128);
    iv1 = vld4q_u8(ip+192);              
      #endif
	vst3q_u8(op,    ov0);       
	vst3q_u8(op+48, ov1);                                                                                                                                                                       
      #if ND > 128
	B64D(iv0,ov0);	CHECK1(_MM_B64CHK(iv0,xv));
	B64D(iv1,ov1); 
	vst3q_u8(op+ 96, ov0);       
	vst3q_u8(op+144, ov1);                                                                                                                                                                       
	CHECK0(_MM_B64CHK(iv1,xv));
      #endif
  }
  for(                 ; ip != in+(inlen&~(64-1)); ip += 64, op += (64/4)*3) { 	
    uint8x16x4_t iv = vld4q_u8(ip);
	uint8x16x3_t ov; B64D(iv,ov);
	vst3q_u8(op, ov);                                                                                                                          
	CHECK0(xv = vorrq_u8(xv, vorrq_u8(vorrq_u8(iv.val[0], iv.val[1]), vorrq_u8(iv.val[2], iv.val[3]))));
  }
  size_t rc;
  if(!(rc=tb64xdec(ip, inlen&(64-1), op)) || vaddvq_u8(vshrq_n_u8(xv,7))) return 0; //decode all
  return (op-out)+rc; 
}

//--------------------------------------------------------------------------------------------------
#define B64E(iv, ov) {\
  ov.val[0] =                                             vshrq_n_u8(iv.val[0], 2);\
  ov.val[1] = vandq_u8(vorrq_u8(vshlq_n_u8(iv.val[0], 4), vshrq_n_u8(iv.val[1], 4)), cv3f);\
  ov.val[2] = vandq_u8(vorrq_u8(vshlq_n_u8(iv.val[1], 2), vshrq_n_u8(iv.val[2], 6)), cv3f);\
  ov.val[3] = vandq_u8(                    iv.val[2],                                cv3f);\
\
  ov.val[0] = vqtbl4q_u8(vlut, ov.val[0]);\
  ov.val[1] = vqtbl4q_u8(vlut, ov.val[1]);\
  ov.val[2] = vqtbl4q_u8(vlut, ov.val[2]);\
  ov.val[3] = vqtbl4q_u8(vlut, ov.val[3]);\
}

size_t tb64sseenc(const unsigned char* in, size_t inlen, unsigned char *out) {
  static unsigned char lut[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const unsigned char *ip; 
        unsigned char *op;
  const size_t      outlen = TB64ENCLEN(inlen);
  const uint8x16x4_t vlut = vld1q_u8_x4(lut);
  const uint8x16_t   cv3f = vdupq_n_u8(0x3f);

  #define NE 128 // 256//
  for(ip = in, op = out; op != out+(outlen&~(NE-1)); op += NE, ip += (NE/4)*3) { 	 							
          uint8x16x3_t iv0 = vld3q_u8(ip),
                       iv1 = vld3q_u8(ip+48);                   

    uint8x16x4_t ov0,ov1; B64E(iv0, ov0); B64E(iv1, ov1);                                       
      #if NE > 128 
    iv0 = vld3q_u8(ip+ 96);
    iv1 = vld3q_u8(ip+144);                   
      #endif
	vst4q_u8(op,    ov0);                                                       
	vst4q_u8(op+64, ov1);                          	//PREFETCH(ip,256,0);                                                  
      #if NE > 128 
                         B64E(iv0, ov0); B64E(iv1, ov1);                                             
 	vst4q_u8(op+128, ov0);                                                       
	vst4q_u8(op+192, ov1);                          	                                            
          #endif
  }
  for(                 ; op != out+(outlen&~(64-1)); op += 64, ip += (64/4)*3) { 								
    const uint8x16x3_t iv = vld3q_u8(ip);
    uint8x16x4_t       ov; 
    B64E(iv, ov); 
	vst4q_u8(op,ov);                                                       
  } 
  EXTAIL();
  return outlen;
}

#elif defined(__SSSE3__) //----------------- SSSE3 / SSE4.1 / AVX (derived from the AVX2 functions ) -----------------------------------------------------------------

#define OVD 4
size_t TEMPLATE2(FUNPREF, dec)(const unsigned char *in, size_t inlen, unsigned char *out) {
  if(inlen >= 16+OVD) {
    const unsigned char *ip;
          unsigned char *op; 
      #ifdef __AVX__
    #define ND 64
      #else
    #define ND 32
      #endif
    __m128i vx = _mm_setzero_si128();
    const __m128i delta_asso   = _mm_setr_epi8(0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,  0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x0f);
    const __m128i delta_values = _mm_setr_epi8(0x00, 0x00, 0x00, 0x13, 0x04, 0xbf, 0xbf, 0xb9,  0xb9, 0x00, 0x10, 0xc3, 0xbf, 0xbf, 0xb9, 0xb9);
      #ifndef NB64CHECK
    const __m128i check_asso   = _mm_setr_epi8(0x0d, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,  0x01, 0x01, 0x03, 0x07, 0x0b, 0x0b, 0x0b, 0x0f);
    const __m128i check_values = _mm_setr_epi8(0x80, 0x80, 0x80, 0x80, 0xcf, 0xbf, 0xd5, 0xa6,  0xb5, 0x86, 0xd1, 0x80, 0xb1, 0x80, 0x91, 0x80);    
      #endif
    const __m128i          cpv = _mm_set_epi8( -1, -1, -1, -1, 12, 13, 14,  8,    9, 10,  4,  5,  6,  0,  1,  2);

    for(ip = in, op = out; ip < (in+inlen)-(ND+OVD); ip += ND, op += (ND/4)*3) {
      __m128i iv0 = _mm_loadu_si128((__m128i *) ip),
              iv1 = _mm_loadu_si128((__m128i *)(ip+16));
                                                                                
      __m128i ov0,shifted0; MM_MAP8TO6(iv0, shifted0,delta_asso, delta_values, ov0); MM_PACK8TO6(ov0, cpv);
      __m128i ov1,shifted1; MM_MAP8TO6(iv1, shifted1,delta_asso, delta_values, ov1); MM_PACK8TO6(ov1, cpv);

      _mm_storeu_si128((__m128i*) op,     ov0);                                                  
      _mm_storeu_si128((__m128i*)(op+12), ov1);                                             PREFETCH(ip,1024,0);                                        

        #if ND > 32
      __m128i iv2 = _mm_loadu_si128((__m128i *)(ip+32)),
              iv3 = _mm_loadu_si128((__m128i *)(ip+48));
        #endif
      
      CHECK0(MM_B64CHK(iv0, shifted0, check_asso, check_values, vx));
      CHECK1(MM_B64CHK(iv1, shifted1, check_asso, check_values, vx));

        #if ND > 32
      __m128i ov2,shifted2; MM_MAP8TO6(iv2, shifted2,delta_asso, delta_values, ov2); MM_PACK8TO6(ov2, cpv);
      __m128i ov3,shifted3; MM_MAP8TO6(iv3, shifted3,delta_asso, delta_values, ov3); MM_PACK8TO6(ov3, cpv);
    
      _mm_storeu_si128((__m128i*)(op+24), ov2);                                                  
      _mm_storeu_si128((__m128i*)(op+36), ov3);                                                                                 
      CHECK1(MM_B64CHK(iv2, shifted2, check_asso, check_values, vx));
      CHECK1(MM_B64CHK(iv3, shifted3, check_asso, check_values, vx));
        #endif
    }
      #ifdef __AVX__
    for(; ip < (in+inlen)-(16+OVD); ip += 16, op += (16/4)*3) {
      #else
    if(ip < (in+inlen)-(16+OVD)) {
      #endif
      __m128i iv0 = _mm_loadu_si128((__m128i *) ip);
      __m128i ov0, shifted0; MM_MAP8TO6(iv0, shifted0,delta_asso, delta_values, ov0); MM_PACK8TO6(ov0, cpv);
      _mm_storeu_si128((__m128i*) op, ov0);                                                  
      CHECK1(MM_B64CHK(iv0, shifted0, check_asso, check_values, vx));
        #ifndef __AVX__
      ip += 16; op += (16/4)*3;
        #endif
    }
    size_t rc;
    if(!(rc = _tb64xdec(ip, inlen-(ip-in), op)) || _mm_movemask_epi8(vx)) return 0;
    return (op-out)+rc;
  }
  return _tb64xdec(in, inlen, out);
}

#define OVE 8
size_t TEMPLATE2(FUNPREF, enc)(const unsigned char* in, size_t inlen, unsigned char *out) { 
  const unsigned char *ip = in; 
        unsigned char *op = out;
  size_t   outlen = TB64ENCLEN(inlen); 
    #ifdef __AVX__
  #define NE 64
    #else
  #define NE 32
    #endif
  if(outlen >= 16+OVE) {
    const __m128i shuf    = _mm_set_epi8(10,11,  9, 10,  7,  8,  6,  7,    4,  5,  3,  4,  1,  2,  0,  1);

    for(; op <= (out+outlen)-(NE+OVE); op += NE, ip += (NE/4)*3) {                       PREFETCH(ip,1024,0);            
      __m128i v0 = _mm_loadu_si128((__m128i*)ip),   
              v1 = _mm_loadu_si128((__m128i*)(ip+12)); 
        #if NE > 32
      __m128i v2 = _mm_loadu_si128((__m128i*)(ip+24)),
              v3 = _mm_loadu_si128((__m128i*)(ip+36)); 
        #endif
              v0 = _mm_shuffle_epi8(v0, shuf);
              v1 = _mm_shuffle_epi8(v1, shuf);
              v0 = mm_unpack6to8(v0);
              v1 = mm_unpack6to8(v1);
              v0 = mm_map6to8(v0);
              v1 = mm_map6to8(v1);
      _mm_storeu_si128((__m128i*) op,     v0);                                          
      _mm_storeu_si128((__m128i*)(op+16), v1);                                          
        #if NE > 32
              v2 = _mm_shuffle_epi8(v2, shuf);
              v3 = _mm_shuffle_epi8(v3, shuf);
              v2 = mm_unpack6to8(v2);
              v3 = mm_unpack6to8(v3);
              v2 = mm_map6to8(v2);
              v3 = mm_map6to8(v3);
      _mm_storeu_si128((__m128i*)(op+32), v2);                                          
      _mm_storeu_si128((__m128i*)(op+48), v3);                                          
        #endif            
    }

    for(; op <= (out+outlen)-(16+OVE); op += 16, ip += (16/4)*3) {
      __m128i v0 = _mm_loadu_si128((__m128i*)ip);
              v0 = _mm_shuffle_epi8(v0, shuf);
              v0 = mm_unpack6to8(v0);
              v0 = mm_map6to8(v0);
      _mm_storeu_si128((__m128i*) op, v0);                                          
    }
  }
  EXTAIL();
  return outlen;
}
#endif

//-------------------------------------------------------------------------------------------------------------------
#if !defined(__AVX__) //include only 1 time
size_t tb64memcpy(const unsigned char* in, size_t inlen, unsigned char *out) {
  memcpy(out, in, inlen);
  return inlen;
}
 
static unsigned _cpuisa;
//--------------------- CPU detection -------------------------------------------
    #if defined(__i386__) || defined(__x86_64__)
      #if _MSC_VER >=1300
#include <intrin.h>
      #elif defined (__INTEL_COMPILER)
#include <x86intrin.h>
      #endif

static inline void cpuid(int reg[4], int id) {
      #if defined (_MSC_VER) //|| defined (__INTEL_COMPILER)
  __cpuidex(reg, id, 0);
      #elif defined(__i386__) || defined(__x86_64__)
  __asm("cpuid" : "=a"(reg[0]),"=b"(reg[1]),"=c"(reg[2]),"=d"(reg[3]) : "a"(id),"c"(0) : );
      #endif
}

static inline uint64_t xgetbv (int ctr) {
      #if(defined _MSC_VER && (_MSC_FULL_VER >= 160040219) || defined __INTEL_COMPILER)
  return _xgetbv(ctr);
      #elif defined(__i386__) || defined(__x86_64__)
  unsigned a, d;
  __asm("xgetbv" : "=a"(a),"=d"(d) : "c"(ctr) : );
  return (uint64_t)d << 32 | a;
      #else
  unsigned a=0, d=0;
  return (uint64_t)d << 32 | a;
      #endif
}
    #endif

#define AVX512F     0x001
#define AVX512DQ    0x002
#define AVX512IFMA  0x004
#define AVX512PF    0x008
#define AVX512ER    0x010
#define AVX512CD    0x020
#define AVX512BW    0x040
#define AVX512VL    0x080
#define AVX512VNNI  0x100
#define AVX512VBMI  0x200
#define AVX512VBMI2 0x400

#define IS_SSE       0x10
#define IS_SSE2      0x20
#define IS_SSE3      0x30
#define IS_SSSE3     0x32
#define IS_POWER9    0x34 // powerpc
#define IS_NEON      0x38 // arm neon
#define IS_SSE41     0x40
#define IS_SSE41x    0x41 //+popcount
#define IS_SSE42     0x42
#define IS_AVX       0x50
#define IS_AVX2      0x60
#define IS_AVX512    0x800

unsigned cpuisa(void) {
  int c[4] = {0};
  if(_cpuisa) return _cpuisa;
  _cpuisa++;
    #if defined(__i386__) || defined(__x86_64__)
  cpuid(c, 0);
  if(c[0]) {
    cpuid(c, 1);
    //family = ((c >> 8) & 0xf) + ((c >> 20) & 0xff)
    //model  = ((c >> 4) & 0xf) + ((c >> 12) & 0xf0)
    if( c[3] & (1 << 25)) {         _cpuisa  = IS_SSE;
    if( c[3] & (1 << 26)) {         _cpuisa  = IS_SSE2;
    if( c[2] & (1 <<  0)) {         _cpuisa  = IS_SSE3;
      //                            _cpuisa  = IS_SSE3SLOW; // Atom SSSE3 slow
    if( c[2] & (1 <<  9)) {         _cpuisa  = IS_SSSE3;
    if( c[2] & (1 << 19)) {         _cpuisa  = IS_SSE41;
    if( c[2] & (1 << 23)) {         _cpuisa  = IS_SSE41x; // +popcount
    if( c[2] & (1 << 20)) {         _cpuisa  = IS_SSE42;  // SSE4.2
    if((c[2] & (1 << 28)) &&
       (c[2] & (1 << 27)) &&                           // OSXSAVE
       (c[2] & (1 << 26)) &&                           // XSAVE
       (xgetbv(0) & 6)==6) {        _cpuisa  = IS_AVX; // AVX
      if(c[2]& (1 <<  3))           _cpuisa |= 1;      // +FMA3
      if(c[2]& (1 << 16))           _cpuisa |= 2;      // +FMA4
      if(c[2]& (1 << 25))           _cpuisa |= 4;      // +AES
      cpuid(c, 7);
      if(c[1] & (1 << 5)) {         _cpuisa = IS_AVX2;
        if(c[1] & (1 << 16)) {
          cpuid(c, 0xd);
          if((c[0] & 0x60)==0x60) { _cpuisa = IS_AVX512;
            cpuid(c, 7);
            if(c[1] & (1<<16))      _cpuisa |= AVX512F;
            if(c[1] & (1<<17))      _cpuisa |= AVX512DQ;
            if(c[1] & (1<<21))      _cpuisa |= AVX512IFMA;
            if(c[1] & (1<<26))      _cpuisa |= AVX512PF;
            if(c[1] & (1<<27))      _cpuisa |= AVX512ER;
            if(c[1] & (1<<28))      _cpuisa |= AVX512CD;
            if(c[1] & (1<<30))      _cpuisa |= AVX512BW;
            if(c[1] & (1u<<31))     _cpuisa |= AVX512VL;
            if(c[2] & (1<< 1))      _cpuisa |= AVX512VBMI;
            if(c[2] & (1<<11))      _cpuisa |= AVX512VNNI;
            if(c[2] & (1<< 6))      _cpuisa |= AVX512VBMI2;
      }}}
    }}}}}}}}}
    #elif defined(__powerpc64__)
  _cpuisa = IS_POWER9; // power9
    #elif defined(__ARM_NEON)
  _cpuisa = IS_NEON; // ARM_NEON
    #endif
  return _cpuisa;
}

unsigned cpuini(unsigned cpuisa) { if(cpuisa) _cpuisa = cpuisa; return _cpuisa; }

char *cpustr(unsigned cpuisa) {
  if(!cpuisa) cpuisa = _cpuisa;
    #if defined(__i386__) || defined(__x86_64__)
  if(cpuisa >= IS_AVX512) {
    if(cpuisa & AVX512VBMI2) return "avx512vbmi2";
    if(cpuisa & AVX512VBMI)  return "avx512vbmi";
    if(cpuisa & AVX512VNNI)  return "avx512vnni";
    if(cpuisa & AVX512VL)    return "avx512vl";
    if(cpuisa & AVX512BW)    return "avx512bw";
    if(cpuisa & AVX512CD)    return "avx512cd";
    if(cpuisa & AVX512ER)    return "avx512er";
    if(cpuisa & AVX512PF)    return "avx512pf";
    if(cpuisa & AVX512IFMA)  return "avx512ifma";
    if(cpuisa & AVX512DQ)    return "avx512dq";
    if(cpuisa & AVX512F)     return "avx512f";
    return "avx512";
  }
  else if(cpuisa >= IS_AVX2)    return "avx2";
  else if(cpuisa >= IS_AVX)
    switch(cpuisa&0xf) {
      case 1: return "avx+fma3";
      case 2: return "avx+fma4";
      case 4: return "avx+aes";
      case 5: return "avx+fma3+aes";
      default:return "avx";
    }
  else if(cpuisa >= IS_SSE42)   return "sse4.2";
  else if(cpuisa >= IS_SSE41x)  return "sse4.1+popcnt";
  else if(cpuisa >= IS_SSE41)   return "sse4.1";
  else if(cpuisa >= IS_SSSE3)   return "ssse3";
  else if(cpuisa >= IS_SSE3)    return "sse3";
  else if(cpuisa >= IS_SSE2)    return "sse2";
  else if(cpuisa >= IS_SSE)     return "sse";
     #elif defined(__powerpc64__)
  if(cpuisa >= IS_POWER9)       return "power9";
    #elif defined(__ARM_NEON)
  if(cpuisa >= IS_NEON)         return "arm_neon";
    #endif
  return "none";
}

//---------------------------------------------------------------------------------
TB64FUNC _tb64e = tb64xenc;
TB64FUNC _tb64d = tb64xdec;

static int tb64set;
 
void tb64ini(unsigned id, unsigned isshort) { 
  int i; 
  if(tb64set) return; 
  tb64set++;   
  i = id?id:cpuisa();
    #if defined(__i386__) || defined(__x86_64__)
      #ifdef USE_AVX512
  if(i >= IS_AVX512) {  
    _tb64e = i >= (IS_AVX512|AVX512VL)?tb64avx2enc:tb64avx2enc; 
    _tb64d = tb64avx512dec;
  } else 
      #endif
      #ifndef NO_AVX2
  if(i >= IS_AVX2) {  
    _tb64e = isshort?_tb64avx2enc:tb64avx2enc; 
    _tb64d = isshort?_tb64avx2dec:tb64avx2dec;
  } else 
      #endif
      #ifndef NO_AVX
    if(i >= IS_AVX) {  
    _tb64e = tb64avxenc; 
    _tb64d = tb64avxdec;
  } else 
      #endif
    #endif
    #if defined(__i386__) || defined(__x86_64__) || defined(__ARM_NEON) || defined(__powerpc64__)
      #ifndef NO_SSE
  if(i >= IS_SSSE3) {  
    _tb64e = tb64sseenc; 
    _tb64d = tb64ssedec;
  }
      #endif
    #endif
}

size_t tb64enc(const unsigned char *in, size_t inlen, unsigned char *out) {
  if(!tb64set) tb64ini(0,0);
  return _tb64e(in,inlen,out);
}
size_t tb64dec(const unsigned char *in, size_t inlen, unsigned char *out) {
  if(!tb64set) tb64ini(0,0);
  return _tb64d(in,inlen,out);
}
#endif

