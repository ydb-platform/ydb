size_t _tb64xdec( const unsigned char *in, size_t inlen, unsigned char *out);
size_t tb64memcpy(const unsigned char *in, size_t inlen, unsigned char *out);  // testing only

#define PREFETCH(_ip_,_i_,_rw_) __builtin_prefetch(_ip_+(_i_),_rw_)

  #if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#define BSWAP32(a) a 
#define BSWAP64(a) a 
  #else
#define BSWAP32(a) bswap32(a)
#define BSWAP64(a) bswap64(a)
  #endif  

  #ifdef NB64CHECK
#define CHECK0(a)
#define CHECK1(a)
  #else
#define CHECK0(a) a
    #ifdef B64CHECK
#define CHECK1(a) a
    #else
#define CHECK1(a)
    #endif
  #endif

//--------------------- Encoding ----------------------------------------------------------
extern unsigned char tb64lutse[];

#define SU32(_u_) (tb64lutse[(_u_>> 8) & 0x3f] << 24 |\
                   tb64lutse[(_u_>>14) & 0x3f] << 16 |\
                   tb64lutse[(_u_>>20) & 0x3f] <<  8 |\
                   tb64lutse[(_u_>>26) & 0x3f])

#define ETAIL()\
  unsigned _l = (in+inlen) - ip;\
  if(_l == 3) { unsigned _u = ip[0]<<24 | ip[1]<<16 | ip[2]<<8; stou32(op, SU32(_u)); op+=4; ip+=3; }\
  else if(_l) { *op++ = tb64lutse[(ip[0]>>2)&0x3f];\
    if(_l == 2) *op++ = tb64lutse[(ip[0] & 0x3) << 4 | (ip[1] & 0xf0) >> 4],\
                *op++ = tb64lutse[(ip[1] & 0xf) << 2];\
    else        *op++ = tb64lutse[(ip[0] & 0x3) << 4], *op++ = '=';\
    *op++ = '=';\
  }

  
extern const unsigned short tb64lutxe[];
#define XU32(_u_) (tb64lutxe[(_u_ >>  8) & 0xfff] << 16 |\
                   tb64lutxe[ _u_ >> 20])

#define EXTAIL() for(; op < (out+outlen)-4; op += 4, ip += 3) { unsigned _u = BSWAP32(ctou32(ip)); stou32(op, XU32(_u)); } ETAIL()
//--------------------- Decoding ----------------------------------------------------------  
extern const unsigned tb64lutxd0[];
extern const unsigned tb64lutxd1[];
extern const unsigned tb64lutxd2[];
extern const unsigned tb64lutxd3[];

#define DU32(_u_) (tb64lutxd0[(unsigned char)(_u_     )] |\
                   tb64lutxd1[(unsigned char)(_u_>>  8)] |\
                   tb64lutxd2[(unsigned char)(_u_>> 16)] |\
                   tb64lutxd3[                _u_>> 24 ] )

  #if 0
static ALWAYS_INLINE size_t _tb64xd(const unsigned char *in, size_t inlen, unsigned char *out) {
  const unsigned char *ip    = in;
        unsigned char *op    = out;  
  for(; ip < (in+inlen)-4; ip += 4, op += 3) { unsigned u = ctou32(ip); u = DU32(u); stou32(op, u); }

  unsigned u = 0, l = (in+inlen) - ip; 
  if(l == 4) 																	// last 4 bytes
    if(    ip[3]=='=') { l = 3; 
      if(  ip[2]=='=') { l = 2; 
        if(ip[1]=='=')   l = 1; 
	  }
	}
  unsigned char *up = (unsigned char *)&u;
  switch(l) {
    case 4: u = ctou32(ip); u = DU32(u);                                   *op++ = up[0]; *op++ = up[1]; *op++ = up[2]; break; // 4->3 bytes
    case 3: u = tb64lutxd0[ip[0]] | tb64lutxd1[ip[1]] | tb64lutxd2[ip[2]]; *op++ = up[0]; *op++ = up[1];                break; // 3->2 bytes
    case 2: u = tb64lutxd0[ip[0]] | tb64lutxd1[ip[1]];                     *op++ = up[0];                               break; // 2->1 byte
    case 1: u = tb64lutxd0[ip[0]];                                         *op++ = up[0];                               break; // 1->1 byte
  }
  return op-out;
}
  #else
static ALWAYS_INLINE size_t _tb64xd(const unsigned char *in, size_t inlen, unsigned char *out) { 
  const unsigned char *ip    = in;
        unsigned char *op    = out;  
        unsigned      cu     = 0;
  for(; ip < (in+inlen)-4; ip += 4, op += 3) { unsigned u = ctou32(ip); u = DU32(u); stou32(op, u); cu |= u; }

  unsigned u = 0, l = (in+inlen) - ip; 
  if(l == 4) 																	// last 4 bytes
    if(    ip[3]=='=') { l = 3; 
      if(  ip[2]=='=') { l = 2; 
        if(ip[1]=='=')   l = 1; 
	  }
	}
  unsigned char *up = (unsigned char *)&u;
  switch(l) {
    case 4: u = ctou32(ip); u = DU32(u);                                   *op++ = up[0]; *op++ = up[1]; *op++ = up[2]; cu |= u; break; // 4->3 bytes
    case 3: u = tb64lutxd0[ip[0]] | tb64lutxd1[ip[1]] | tb64lutxd2[ip[2]]; *op++ = up[0]; *op++ = up[1];                cu |= u; break; // 3->2 bytes
    case 2: u = tb64lutxd0[ip[0]] | tb64lutxd1[ip[1]];                     *op++ = up[0];                               cu |= u; break; // 2->1 byte
    case 1: u = tb64lutxd0[ip[0]];                                         *op++ = up[0];                               cu |= u; break; // 1->1 byte
  }
  return (cu == -1)?0:(op-out);
}
  #endif
//--------------------------- sse -----------------------------------------------------------------

#if defined(__SSSE3__)
#include <tmmintrin.h>
#define MM_PACK8TO6(v, cpv) {\
  const __m128i merge_ab_and_bc = _mm_maddubs_epi16(v,            _mm_set1_epi32(0x01400140));  /*/dec_reshuffle: https://arxiv.org/abs/1704.00605 P.17*/\
                              v = _mm_madd_epi16(merge_ab_and_bc, _mm_set1_epi32(0x00011000));\
                              v = _mm_shuffle_epi8(v, cpv);\
}

#define MM_MAP8TO6(iv, shifted, delta_asso, delta_values, ov) { /*map 8-bits ascii to 6-bits bin*/\
                shifted    = _mm_srli_epi32(iv, 3);\
  const __m128i delta_hash = _mm_avg_epu8(_mm_shuffle_epi8(delta_asso, iv), shifted);\
                        ov = _mm_add_epi8(_mm_shuffle_epi8(delta_values, delta_hash), iv);\
}

#define MM_B64CHK(iv, shifted, check_asso, check_values, vx) {\
  const __m128i check_hash = _mm_avg_epu8( _mm_shuffle_epi8(check_asso, iv), shifted);\
  const __m128i        chk = _mm_adds_epi8(_mm_shuffle_epi8(check_values, check_hash), iv);\
                        vx = _mm_or_si128(vx, chk);\
}

static ALWAYS_INLINE __m128i mm_map6to8(const __m128i v) {
  const __m128i offsets = _mm_set_epi8( 0, 0,-16,-19, -4, -4, -4, -4,   -4, -4, -4, -4, -4, -4, 71, 65);

  __m128i vidx = _mm_subs_epu8(v,   _mm_set1_epi8(51));
          vidx = _mm_sub_epi8(vidx, _mm_cmpgt_epi8(v, _mm_set1_epi8(25)));
  return _mm_add_epi8(v, _mm_shuffle_epi8(offsets, vidx));
}

static ALWAYS_INLINE __m128i mm_unpack6to8(__m128i v) {
  __m128i va = _mm_mulhi_epu16(_mm_and_si128(v, _mm_set1_epi32(0x0fc0fc00)), _mm_set1_epi32(0x04000040));
  __m128i vb = _mm_mullo_epi16(_mm_and_si128(v, _mm_set1_epi32(0x003f03f0)), _mm_set1_epi32(0x01000010));
  return       _mm_or_si128(va, vb);                        
}
#endif

