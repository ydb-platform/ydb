/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Mandar Gurav <mandar@multicorewareinc.com>
 *          Mahesh Pittala <mahesh@multicorewareinc.com>
 *          Min Chen <min.chen@multicorewareinc.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com.
 *****************************************************************************/

#include "common.h"
#include "primitives.h"
#include "x265.h"
#include "ppccommon.h"

#include <cstdlib> // abs()

//using namespace X265_NS;

namespace X265_NS {
// place functions in anonymous namespace (file static)

 /* Null vector */
#define LOAD_ZERO const vec_u8_t zerov = vec_splat_u8( 0 )

#define zero_u8v  (vec_u8_t)  zerov
#define zero_s8v  (vec_s8_t)  zerov
#define zero_u16v (vec_u16_t) zerov
#define zero_s16v (vec_s16_t) zerov
#define zero_u32v (vec_u32_t) zerov
#define zero_s32v (vec_s32_t) zerov

 /* 8 <-> 16 bits conversions */
#ifdef WORDS_BIGENDIAN
#define vec_u8_to_u16_h(v) (vec_u16_t) vec_mergeh( zero_u8v, (vec_u8_t) v )
#define vec_u8_to_u16_l(v) (vec_u16_t) vec_mergel( zero_u8v, (vec_u8_t) v )
#define vec_u8_to_s16_h(v) (vec_s16_t) vec_mergeh( zero_u8v, (vec_u8_t) v )
#define vec_u8_to_s16_l(v) (vec_s16_t) vec_mergel( zero_u8v, (vec_u8_t) v )
#else
#define vec_u8_to_u16_h(v) (vec_u16_t) vec_mergeh( (vec_u8_t) v, zero_u8v )
#define vec_u8_to_u16_l(v) (vec_u16_t) vec_mergel( (vec_u8_t) v, zero_u8v )
#define vec_u8_to_s16_h(v) (vec_s16_t) vec_mergeh( (vec_u8_t) v, zero_u8v )
#define vec_u8_to_s16_l(v) (vec_s16_t) vec_mergel( (vec_u8_t) v, zero_u8v )
#endif

#define vec_u8_to_u16(v) vec_u8_to_u16_h(v)
#define vec_u8_to_s16(v) vec_u8_to_s16_h(v)

#if defined(__GNUC__)
#define ALIGN_VAR_8(T, var)  T var __attribute__((aligned(8)))
#define ALIGN_VAR_16(T, var) T var __attribute__((aligned(16)))
#define ALIGN_VAR_32(T, var) T var __attribute__((aligned(32)))
#elif defined(_MSC_VER)
#define ALIGN_VAR_8(T, var)  __declspec(align(8)) T var
#define ALIGN_VAR_16(T, var) __declspec(align(16)) T var
#define ALIGN_VAR_32(T, var) __declspec(align(32)) T var
#endif // if defined(__GNUC__)

typedef uint8_t  pixel;
typedef uint32_t sum2_t ;
typedef uint16_t sum_t ;
#define BITS_PER_SUM (8 * sizeof(sum_t))

/***********************************************************************
 * SAD routines - altivec implementation
 **********************************************************************/
template<int lx, int ly>
void inline sum_columns_altivec(vec_s32_t sumv, int* sum){}

template<int lx, int ly>
int inline sad16_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    assert(lx <=16);
    LOAD_ZERO;
    vec_u8_t  pix1v, pix2v;
    vec_u8_t  absv = zero_u8v;
    vec_s32_t sumv = zero_s32v;
    ALIGN_VAR_16(int, sum );

    for( int y = 0; y < ly; y++ )
    {
        pix1v = /*vec_vsx_ld*/vec_xl( 0, pix1);
        pix2v = /*vec_vsx_ld*/vec_xl( 0, pix2);
        //print_vec_u8("pix1v", &pix1v);
        //print_vec_u8("pix2v", &pix2v);

        absv = (vector unsigned char)vec_sub(vec_max(pix1v, pix2v), vec_min(pix1v, pix2v)); 
        //print_vec_u8("abs sub", &absv);

        sumv = (vec_s32_t) vec_sum4s( absv, (vec_u32_t) sumv);
        //print_vec_i("vec_sum4s 0", &sumv);

        pix1 += stride_pix1;
        pix2 += stride_pix2;
    }

    sum_columns_altivec<lx, ly>(sumv, &sum);
    //printf("<%d %d>%d\n", lx, ly, sum);
    return sum;
}

template<int lx, int ly> //to be implemented later
int sad16_altivec(const int16_t* pix1, intptr_t stride_pix1, const int16_t* pix2, intptr_t stride_pix2)
{
    int sum = 0;
    return sum;
}

template<int lx, int ly>//to be implemented later
int sad_altivec(const int16_t* pix1, intptr_t stride_pix1, const int16_t* pix2, intptr_t stride_pix2)
{
    int sum = 0;
    return sum;
}

template<>
void inline sum_columns_altivec<16, 4>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 3", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<16, 8>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 3", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<16, 12>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 3", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<16, 16>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 3", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<16, 24>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 3", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<16, 32>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 3", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<16, 48>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 3", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<16, 64>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 3", &sumv);
    vec_ste( sumv, 0, sum );
}


template<>
void inline sum_columns_altivec<8, 4>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sum2s( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 1 );
    //print_vec_i("vec_splat 1", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<8, 8>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sum2s( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 1 );
    //print_vec_i("vec_splat 1", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<8, 16>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sum2s( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 1 );
    //print_vec_i("vec_splat 1", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<8, 32>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_sum2s( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 1 );
    //print_vec_i("vec_splat 1", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<4, 4>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_splat( sumv, 0 );
    //print_vec_i("vec_splat 0", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<4, 8>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_splat( sumv, 0 );
    //print_vec_i("vec_splat 0", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<4, 16>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    sumv = vec_splat( sumv, 0 );
    //print_vec_i("vec_splat 0", &sumv);
    vec_ste( sumv, 0, sum );
}

template<>
void inline sum_columns_altivec<12, 16>(vec_s32_t sumv, int* sum)
{
    LOAD_ZERO;
    vec_s32_t sum1v= vec_splat( sumv, 3);
    sumv = vec_sums( sumv, zero_s32v );
    //print_vec_i("vec_sums", &sumv);
    sumv = vec_splat( sumv, 3 );
    //print_vec_i("vec_splat 1", &sumv);
    sumv = vec_sub(sumv, sum1v);
    vec_ste( sumv, 0, sum );
}

template<int lx, int ly>
int inline sad_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2){ return 0; }

template<>
int inline sad_altivec<24, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 32>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<8, 32>(pix1+16, stride_pix1, pix2+16, stride_pix2);
    //printf("<24 32>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<32, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 8>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 8>(pix1+16, stride_pix1, pix2+16, stride_pix2);
   //printf("<32 8>%d\n", sum);
   return sum;
}

template<>
int inline sad_altivec<32, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 16>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 16>(pix1+16, stride_pix1, pix2+16, stride_pix2);
    //printf("<32 16>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<32, 24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 24>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 24>(pix1+16, stride_pix1, pix2+16, stride_pix2);
    //printf("<32 24>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<32, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 32>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 32>(pix1+16, stride_pix1, pix2+16, stride_pix2);
    //printf("<32 32>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<32, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 64>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 64>(pix1+16, stride_pix1, pix2+16, stride_pix2);
    //printf("<32 64>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<48, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 64>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 64>(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + sad16_altivec<16, 64>(pix1+32, stride_pix1, pix2+32, stride_pix2);
    //printf("<48 64>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<64, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 16>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 16>(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + sad16_altivec<16, 16>(pix1+32, stride_pix1, pix2+32, stride_pix2)
              + sad16_altivec<16, 16>(pix1+48, stride_pix1, pix2+48, stride_pix2);
    //printf("<64 16>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<64, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 32>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 32>(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + sad16_altivec<16, 32>(pix1+32, stride_pix1, pix2+32, stride_pix2)
              + sad16_altivec<16, 32>(pix1+48, stride_pix1, pix2+48, stride_pix2);
    //printf("<64 32>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<64, 48>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 48>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 48>(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + sad16_altivec<16, 48>(pix1+32, stride_pix1, pix2+32, stride_pix2)
              + sad16_altivec<16, 48>(pix1+48, stride_pix1, pix2+48, stride_pix2);
    //printf("<64 48>%d\n", sum);
    return sum;
}

template<>
int inline sad_altivec<64, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16(int, sum );
    sum = sad16_altivec<16, 64>(pix1, stride_pix1, pix2, stride_pix2)
              + sad16_altivec<16, 64>(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + sad16_altivec<16, 64>(pix1+32, stride_pix1, pix2+32, stride_pix2)
              + sad16_altivec<16, 64>(pix1+48, stride_pix1, pix2+48, stride_pix2);
    //printf("<64 64>%d\n", sum);
    return sum;
}

/***********************************************************************
 * SAD_X3 routines - altivec implementation
 **********************************************************************/
template<int lx, int ly>
void inline sad16_x3_altivec(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    res[0] = 0;
    res[1] = 0;
    res[2] = 0;
    assert(lx <=16);
    LOAD_ZERO;                                         
    vec_u8_t  pix1v, pix2v, pix3v, pix4v;    
    vec_u8_t  absv1_2 = zero_u8v;
    vec_u8_t  absv1_3 = zero_u8v;
    vec_u8_t  absv1_4 = zero_u8v;
    vec_s32_t sumv0 = zero_s32v;                        
    vec_s32_t sumv1 = zero_s32v;                        
    vec_s32_t sumv2 = zero_s32v;                        

    for( int y = 0; y < ly; y++ )                      
    {                                                  
        pix1v = vec_xl( 0, pix1); //@@RM vec_vsx_ld( 0, pix1);       
        pix2v = vec_xl( 0, pix2); //@@RM vec_vsx_ld( 0, pix2);
        pix3v = vec_xl( 0, pix3); //@@RM vec_vsx_ld( 0, pix3);       
        pix4v = vec_xl( 0, pix4); //@@RM vec_vsx_ld( 0, pix4);

        //@@RM : using vec_abs has 2 drawbacks here:
        //@@RM first, it produces the incorrect result (unpack should be used first)
        //@@RM second, it is slower than sub(max,min), as noted in freescale's documentation
        //@@RM absv = (vector unsigned char)vec_abs((vector signed char)vec_sub(pix1v, pix2v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix2v));
        absv1_2 = (vector unsigned char)vec_sub(vec_max(pix1v, pix2v), vec_min(pix1v, pix2v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix2v));
        sumv0 = (vec_s32_t) vec_sum4s( absv1_2, (vec_u32_t) sumv0);  

        absv1_3 = (vector unsigned char)vec_sub(vec_max(pix1v, pix3v), vec_min(pix1v, pix3v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix3v));
        sumv1 = (vec_s32_t) vec_sum4s( absv1_3, (vec_u32_t) sumv1);  

        absv1_4 = (vector unsigned char)vec_sub(vec_max(pix1v, pix4v), vec_min(pix1v, pix4v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix3v));
        sumv2 = (vec_s32_t) vec_sum4s( absv1_4, (vec_u32_t) sumv2);  

        pix1 += FENC_STRIDE;                                
        pix2 += frefstride;                                
        pix3 += frefstride;                                
        pix4 += frefstride;                                
    }  

    sum_columns_altivec<lx, ly>(sumv0, res+0);
    sum_columns_altivec<lx, ly>(sumv1, res+1);
    sum_columns_altivec<lx, ly>(sumv2, res+2);
    //printf("<%d %d>%d %d %d\n", lx, ly, res[0], res[1], res[2]);
}

template<int lx, int ly>
void inline sad_x3_altivec(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res){}

template<>
void inline sad_x3_altivec<24, 32>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[3];	
    sad16_x3_altivec<16, 32>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<8, 32>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    //printf("<24 32>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void inline sad_x3_altivec<32, 8>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[3];
    sad16_x3_altivec<16, 8>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 8>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    //printf("<32 8>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void inline sad_x3_altivec<32, 16>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[3];
    sad16_x3_altivec<16, 16>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 16>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    //printf("<32 16>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void inline sad_x3_altivec<32, 24>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[3];
    sad16_x3_altivec<16, 24>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 24>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    //printf("<32 24>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void sad_x3_altivec<32, 32>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
   
    const int lx = 32 ;
    const int ly = 32 ;

    vector unsigned int v_zeros = {0, 0, 0, 0} ;

    vector signed short v_results_0 = {0, 0, 0, 0, 0, 0, 0, 0} ;
    vector signed short v_results_1 = {0, 0, 0, 0, 0, 0, 0, 0} ;
    vector signed short v_results_2 = {0, 0, 0, 0, 0, 0, 0, 0} ;


    vector signed int v_results_int_0 ;
    vector signed int v_results_int_1 ;
    vector signed int v_results_int_2 ;

    vector unsigned char v_pix1 ;
    vector unsigned char v_pix2 ;
    vector unsigned char v_pix3 ;
    vector unsigned char v_pix4 ;

    vector unsigned char v_abs_diff_0 ;
    vector unsigned char v_abs_diff_1 ;
    vector unsigned char v_abs_diff_2 ;

    vector signed short v_unpack_mask = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ;

    vector signed short v_short_0_0 , v_short_0_1 ;
    vector signed short v_short_1_0 , v_short_1_1 ;
    vector signed short v_short_2_0 , v_short_2_1 ;

    vector signed short v_sum_0 ;
    vector signed short v_sum_1 ;
    vector signed short v_sum_2 ;



    res[0] = 0;
    res[1] = 0;
    res[2] = 0;
    for (int y = 0; y < ly; y++)
    {
        for (int x = 0; x < lx; x+=16)
        {
            v_pix1 = vec_xl(x, pix1) ;

            // for(int ii=0; ii<16; ii++) { res[0] += abs(pix1[x + ii] - pix2[x + ii]); }
            v_pix2 = vec_xl(x, pix2) ;
            v_abs_diff_0 = vec_sub(vec_max(v_pix1, v_pix2), vec_min(v_pix1, v_pix2)) ;
            v_short_0_0 = vec_unpackh((vector signed char)v_abs_diff_0) ;
            v_short_0_0 = vec_and(v_short_0_0, v_unpack_mask) ;
            v_short_0_1 = vec_unpackl((vector signed char)v_abs_diff_0) ;
            v_short_0_1 = vec_and(v_short_0_1, v_unpack_mask) ;
            v_sum_0 = vec_add(v_short_0_0, v_short_0_1) ;
            v_results_0 = vec_add(v_results_0, v_sum_0) ;

            // for(int ii=0; ii<16; ii++) { res[1] += abs(pix1[x + ii] - pix3[x + ii]); }
            v_pix3 = vec_xl(x, pix3) ;
            v_abs_diff_1 = vec_sub(vec_max(v_pix1, v_pix3), vec_min(v_pix1, v_pix3)) ;
            v_short_1_0 = vec_unpackh((vector signed char)v_abs_diff_1) ;
            v_short_1_0 = vec_and(v_short_1_0, v_unpack_mask) ;
            v_short_1_1 = vec_unpackl((vector signed char)v_abs_diff_1) ;
            v_short_1_1 = vec_and(v_short_1_1, v_unpack_mask) ;
            v_sum_1 = vec_add(v_short_1_0, v_short_1_1) ;
            v_results_1 = vec_add(v_results_1, v_sum_1) ;


            // for(int ii=0; ii<16; ii++) { res[2] += abs(pix1[x + ii] - pix4[x + ii]); }
            v_pix4 = vec_xl(x, pix4) ;
            v_abs_diff_2 = vec_sub(vec_max(v_pix1, v_pix4), vec_min(v_pix1, v_pix4)) ;
            v_short_2_0 = vec_unpackh((vector signed char)v_abs_diff_2) ;
            v_short_2_0 = vec_and(v_short_2_0, v_unpack_mask) ;
            v_short_2_1 = vec_unpackl((vector signed char)v_abs_diff_2) ;
            v_short_2_1 = vec_and(v_short_2_1, v_unpack_mask) ;
            v_sum_2 = vec_add(v_short_2_0, v_short_2_1) ;
            v_results_2 = vec_add(v_results_2, v_sum_2) ;

        }

        pix1 += FENC_STRIDE;
        pix2 += frefstride;
        pix3 += frefstride;
        pix4 += frefstride;
    }

    
    v_results_int_0 = vec_sum4s((vector signed short)v_results_0, (vector signed int)v_zeros) ;
    v_results_int_0 = vec_sums(v_results_int_0, (vector signed int)v_zeros) ;
    res[0] = v_results_int_0[3] ;


    v_results_int_1 = vec_sum4s((vector signed short)v_results_1, (vector signed int)v_zeros) ;
    v_results_int_1 = vec_sums(v_results_int_1, (vector signed int)v_zeros) ;
    res[1] = v_results_int_1[3] ;


    v_results_int_2 = vec_sum4s((vector signed short)v_results_2, (vector signed int)v_zeros) ;
    v_results_int_2 = vec_sums(v_results_int_2, (vector signed int)v_zeros) ;
    res[2] = v_results_int_2[3] ;

    //printf("<32 32>%d %d %d\n", res[0], res[1], res[2]);

} // end sad_x3_altivec

template<>
void inline sad_x3_altivec<32, 64>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[3];
    sad16_x3_altivec<16, 64>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 64>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    //printf("<32 64>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void inline sad_x3_altivec<48, 64>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[6];
    sad16_x3_altivec<16, 64>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 64>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, sum+3);
    sad16_x3_altivec<16, 64>(pix1+32, pix2+32, pix3+32, pix4+32, frefstride, res);
    res[0] = sum[0]+sum[3]+res[0];	
    res[1] = sum[1]+sum[4]+res[1];	
    res[2] = sum[2]+sum[5]+res[2];	
    //printf("<48 64>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void inline sad_x3_altivec<64, 16>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[9];
    sad16_x3_altivec<16, 16>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 16>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, sum+3);
    sad16_x3_altivec<16, 16>(pix1+32, pix2+32, pix3+32, pix4+32, frefstride, sum+6);
    sad16_x3_altivec<16, 16>(pix1+48, pix2+48, pix3+48, pix4+48, frefstride, res);              
    res[0] = sum[0]+sum[3]+sum[6]+res[0];	
    res[1] = sum[1]+sum[4]+sum[7]+res[1];	
    res[2] = sum[2]+sum[5]+sum[8]+res[2];	
    //printf("<64 16>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void inline sad_x3_altivec<64, 32>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[9];
    sad16_x3_altivec<16, 32>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 32>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, sum+3);
    sad16_x3_altivec<16, 32>(pix1+32, pix2+32, pix3+32, pix4+32, frefstride, sum+6);
    sad16_x3_altivec<16, 32>(pix1+48, pix2+48, pix3+48, pix4+48, frefstride, res);              
    res[0] = sum[0]+sum[3]+sum[6]+res[0];	
    res[1] = sum[1]+sum[4]+sum[7]+res[1];	
    res[2] = sum[2]+sum[5]+sum[8]+res[2];	
    //printf("<64 32>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void inline sad_x3_altivec<64, 48>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[9];
    sad16_x3_altivec<16, 48>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 48>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, sum+3);
    sad16_x3_altivec<16, 48>(pix1+32, pix2+32, pix3+32, pix4+32, frefstride, sum+6);
    sad16_x3_altivec<16, 48>(pix1+48, pix2+48, pix3+48, pix4+48, frefstride, res);              
    res[0] = sum[0]+sum[3]+sum[6]+res[0];	
    res[1] = sum[1]+sum[4]+sum[7]+res[1];	
    res[2] = sum[2]+sum[5]+sum[8]+res[2];	
    //printf("<64 48>%d %d %d\n", res[0], res[1], res[2]);
}

template<>
void inline sad_x3_altivec<64, 64>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, intptr_t frefstride, int32_t* res)
{
    int32_t sum[9];
    sad16_x3_altivec<16, 64>(pix1, pix2, pix3, pix4, frefstride, sum);
    sad16_x3_altivec<16, 64>(pix1+16, pix2+16, pix3+16, pix4+16, frefstride, sum+3);
    sad16_x3_altivec<16, 64>(pix1+32, pix2+32, pix3+32, pix4+32, frefstride, sum+6);
    sad16_x3_altivec<16, 64>(pix1+48, pix2+48, pix3+48, pix4+48, frefstride, res);              
    res[0] = sum[0]+sum[3]+sum[6]+res[0];	
    res[1] = sum[1]+sum[4]+sum[7]+res[1];	
    res[2] = sum[2]+sum[5]+sum[8]+res[2];	
    //printf("<64 64>%d %d %d\n", res[0], res[1], res[2]);
}

/***********************************************************************
 * SAD_X4 routines - altivec implementation
 **********************************************************************/
template<int lx, int ly>
void inline sad16_x4_altivec(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    res[0] = 0;
    res[1] = 0;
    res[2] = 0;
    assert(lx <=16);
    LOAD_ZERO;                                         
    vec_u8_t  pix1v, pix2v, pix3v, pix4v, pix5v;    
    vec_u8_t  absv1_2 = zero_u8v;
    vec_u8_t  absv1_3 = zero_u8v;
    vec_u8_t  absv1_4 = zero_u8v;
    vec_u8_t  absv1_5 = zero_u8v;
    vec_s32_t sumv0 = zero_s32v;                        
    vec_s32_t sumv1 = zero_s32v;                        
    vec_s32_t sumv2 = zero_s32v;                        
    vec_s32_t sumv3 = zero_s32v;                        

    for( int y = 0; y < ly; y++ )                      
    {                                                  
        pix1v = vec_xl( 0, pix1); //@@RM vec_vsx_ld( 0, pix1);       
        pix2v = vec_xl( 0, pix2); //@@RM vec_vsx_ld( 0, pix2);
        pix3v = vec_xl( 0, pix3); //@@RM vec_vsx_ld( 0, pix3);       
        pix4v = vec_xl( 0, pix4); //@@RM vec_vsx_ld( 0, pix4);
        pix5v = vec_xl( 0, pix5); //@@RM vec_vsx_ld( 0, pix4);

        //@@RM : using vec_abs has 2 drawbacks here:
        //@@RM first, it produces the incorrect result (unpack should be used first)
        //@@RM second, it is slower than sub(max,min), as noted in freescale's documentation
        //@@RM absv = (vector unsigned char)vec_abs((vector signed char)vec_sub(pix1v, pix2v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix2v));
        absv1_2 = (vector unsigned char)vec_sub(vec_max(pix1v, pix2v), vec_min(pix1v, pix2v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix2v));
        sumv0 = (vec_s32_t) vec_sum4s( absv1_2, (vec_u32_t) sumv0);  

        absv1_3 = (vector unsigned char)vec_sub(vec_max(pix1v, pix3v), vec_min(pix1v, pix3v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix3v));
        sumv1 = (vec_s32_t) vec_sum4s( absv1_3, (vec_u32_t) sumv1);  

        absv1_4 = (vector unsigned char)vec_sub(vec_max(pix1v, pix4v), vec_min(pix1v, pix4v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix3v));
        sumv2 = (vec_s32_t) vec_sum4s( absv1_4, (vec_u32_t) sumv2);  

        absv1_5 = (vector unsigned char)vec_sub(vec_max(pix1v, pix5v), vec_min(pix1v, pix5v)); //@@RM vec_abs((vec_s8_t)vec_sub(pix1v, pix3v));
        sumv3 = (vec_s32_t) vec_sum4s( absv1_5, (vec_u32_t) sumv3);  

        pix1 += FENC_STRIDE;                                
        pix2 += frefstride;                                
        pix3 += frefstride;                                
        pix4 += frefstride;                                
        pix5 += frefstride;                                
    }  

    sum_columns_altivec<lx, ly>(sumv0, res+0);
    sum_columns_altivec<lx, ly>(sumv1, res+1);
    sum_columns_altivec<lx, ly>(sumv2, res+2);
    sum_columns_altivec<lx, ly>(sumv3, res+3);
    //printf("<%d %d>%d %d %d %d\n", lx, ly, res[0], res[1], res[2], res[3]);
}

template<int lx, int ly>
void inline sad_x4_altivec(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res){}


template<>
void inline sad_x4_altivec<24, 32>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[4];	
    sad16_x4_altivec<16, 32>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<8, 32>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    res[3] += sum[3];	
    //printf("<24 32>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}

template<>
void inline sad_x4_altivec<32, 8>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[4];
    sad16_x4_altivec<16, 8>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<16, 8>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    res[3] += sum[3];	
    //printf("<32 8>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}

template<>
void sad_x4_altivec<32,16>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
   
    const int lx = 32 ;
    const int ly = 16 ;

    vector unsigned int v_zeros = {0, 0, 0, 0} ;

    vector signed short v_results_0 = {0, 0, 0, 0, 0, 0, 0, 0} ;
    vector signed short v_results_1 = {0, 0, 0, 0, 0, 0, 0, 0} ;
    vector signed short v_results_2 = {0, 0, 0, 0, 0, 0, 0, 0} ;
    vector signed short v_results_3 = {0, 0, 0, 0, 0, 0, 0, 0} ;


    vector signed int v_results_int_0 ;
    vector signed int v_results_int_1 ;
    vector signed int v_results_int_2 ;
    vector signed int v_results_int_3 ;

    vector unsigned char v_pix1 ;
    vector unsigned char v_pix2 ;
    vector unsigned char v_pix3 ;
    vector unsigned char v_pix4 ;
    vector unsigned char v_pix5 ;

    vector unsigned char v_abs_diff_0 ;
    vector unsigned char v_abs_diff_1 ;
    vector unsigned char v_abs_diff_2 ;
    vector unsigned char v_abs_diff_3 ;

    vector signed short v_unpack_mask = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ;

    vector signed short v_short_0_0 , v_short_0_1 ;
    vector signed short v_short_1_0 , v_short_1_1 ;
    vector signed short v_short_2_0 , v_short_2_1 ;
    vector signed short v_short_3_0 , v_short_3_1 ;

    vector signed short v_sum_0 ;
    vector signed short v_sum_1 ;
    vector signed short v_sum_2 ;
    vector signed short v_sum_3 ;


    res[0] = 0;
    res[1] = 0;
    res[2] = 0;
    res[3] = 0;
    for (int y = 0; y < ly; y++)
    {
        for (int x = 0; x < lx; x+=16)
        {
            v_pix1 = vec_xl(x, pix1) ;

            // for(int ii=0; ii<16; ii++) { res[0] += abs(pix1[x + ii] - pix2[x + ii]); }
            v_pix2 = vec_xl(x, pix2) ;
            v_abs_diff_0 = vec_sub(vec_max(v_pix1, v_pix2), vec_min(v_pix1, v_pix2)) ;
            v_short_0_0 = vec_unpackh((vector signed char)v_abs_diff_0) ;
            v_short_0_0 = vec_and(v_short_0_0, v_unpack_mask) ;
            v_short_0_1 = vec_unpackl((vector signed char)v_abs_diff_0) ;
            v_short_0_1 = vec_and(v_short_0_1, v_unpack_mask) ;
            v_sum_0 = vec_add(v_short_0_0, v_short_0_1) ;
            v_results_0 = vec_add(v_results_0, v_sum_0) ;

            // for(int ii=0; ii<16; ii++) { res[1] += abs(pix1[x + ii] - pix3[x + ii]); }
            v_pix3 = vec_xl(x, pix3) ;
            v_abs_diff_1 = vec_sub(vec_max(v_pix1, v_pix3), vec_min(v_pix1, v_pix3)) ;
            v_short_1_0 = vec_unpackh((vector signed char)v_abs_diff_1) ;
            v_short_1_0 = vec_and(v_short_1_0, v_unpack_mask) ;
            v_short_1_1 = vec_unpackl((vector signed char)v_abs_diff_1) ;
            v_short_1_1 = vec_and(v_short_1_1, v_unpack_mask) ;
            v_sum_1 = vec_add(v_short_1_0, v_short_1_1) ;
            v_results_1 = vec_add(v_results_1, v_sum_1) ;


            // for(int ii=0; ii<16; ii++) { res[2] += abs(pix1[x + ii] - pix4[x + ii]); }
            v_pix4 = vec_xl(x, pix4) ;
            v_abs_diff_2 = vec_sub(vec_max(v_pix1, v_pix4), vec_min(v_pix1, v_pix4)) ;
            v_short_2_0 = vec_unpackh((vector signed char)v_abs_diff_2) ;
            v_short_2_0 = vec_and(v_short_2_0, v_unpack_mask) ;
            v_short_2_1 = vec_unpackl((vector signed char)v_abs_diff_2) ;
            v_short_2_1 = vec_and(v_short_2_1, v_unpack_mask) ;
            v_sum_2 = vec_add(v_short_2_0, v_short_2_1) ;
            v_results_2 = vec_add(v_results_2, v_sum_2) ;


            // for(int ii=0; ii<16; ii++) { res[3] += abs(pix1[x + ii] - pix5[x + ii]); }
            v_pix5 = vec_xl(x, pix5) ;
            v_abs_diff_3 = vec_sub(vec_max(v_pix1, v_pix5), vec_min(v_pix1, v_pix5)) ;
            v_short_3_0 = vec_unpackh((vector signed char)v_abs_diff_3) ;
            v_short_3_0 = vec_and(v_short_3_0, v_unpack_mask) ;
            v_short_3_1 = vec_unpackl((vector signed char)v_abs_diff_3) ;
            v_short_3_1 = vec_and(v_short_3_1, v_unpack_mask) ;
            v_sum_3 = vec_add(v_short_3_0, v_short_3_1) ;
            v_results_3 = vec_add(v_results_3, v_sum_3) ;
        }

        pix1 += FENC_STRIDE;
        pix2 += frefstride;
        pix3 += frefstride;
        pix4 += frefstride;
        pix5 += frefstride;
    }

    
    v_results_int_0 = vec_sum4s((vector signed short)v_results_0, (vector signed int)v_zeros) ;
    v_results_int_0 = vec_sums(v_results_int_0, (vector signed int)v_zeros) ;
    res[0] = v_results_int_0[3] ;


    v_results_int_1 = vec_sum4s((vector signed short)v_results_1, (vector signed int)v_zeros) ;
    v_results_int_1 = vec_sums(v_results_int_1, (vector signed int)v_zeros) ;
    res[1] = v_results_int_1[3] ;


    v_results_int_2 = vec_sum4s((vector signed short)v_results_2, (vector signed int)v_zeros) ;
    v_results_int_2 = vec_sums(v_results_int_2, (vector signed int)v_zeros) ;
    res[2] = v_results_int_2[3] ;


    v_results_int_3 = vec_sum4s((vector signed short)v_results_3, (vector signed int)v_zeros) ;
    v_results_int_3 = vec_sums(v_results_int_3, (vector signed int)v_zeros) ;
    res[3] = v_results_int_3[3] ;
    //printf("<32 16>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
} // end sad_x4_altivec

template<>
void inline sad_x4_altivec<32, 24>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[4];
    sad16_x4_altivec<16, 24>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<16, 24>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    res[3] += sum[3];	
    //printf("<32 24>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}

template<>
void sad_x4_altivec<32,32>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
   
    const int lx = 32 ;
    const int ly = 32 ;

    vector unsigned int v_zeros = {0, 0, 0, 0} ;

    vector signed short v_results_0 = {0, 0, 0, 0, 0, 0, 0, 0} ;
    vector signed short v_results_1 = {0, 0, 0, 0, 0, 0, 0, 0} ;
    vector signed short v_results_2 = {0, 0, 0, 0, 0, 0, 0, 0} ;
    vector signed short v_results_3 = {0, 0, 0, 0, 0, 0, 0, 0} ;


    vector signed int v_results_int_0 ;
    vector signed int v_results_int_1 ;
    vector signed int v_results_int_2 ;
    vector signed int v_results_int_3 ;

    vector unsigned char v_pix1 ;
    vector unsigned char v_pix2 ;
    vector unsigned char v_pix3 ;
    vector unsigned char v_pix4 ;
    vector unsigned char v_pix5 ;

    vector unsigned char v_abs_diff_0 ;
    vector unsigned char v_abs_diff_1 ;
    vector unsigned char v_abs_diff_2 ;
    vector unsigned char v_abs_diff_3 ;

    vector signed short v_unpack_mask = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ;

    vector signed short v_short_0_0 , v_short_0_1 ;
    vector signed short v_short_1_0 , v_short_1_1 ;
    vector signed short v_short_2_0 , v_short_2_1 ;
    vector signed short v_short_3_0 , v_short_3_1 ;

    vector signed short v_sum_0 ;
    vector signed short v_sum_1 ;
    vector signed short v_sum_2 ;
    vector signed short v_sum_3 ;


    res[0] = 0;
    res[1] = 0;
    res[2] = 0;
    res[3] = 0;
    for (int y = 0; y < ly; y++)
    {
        for (int x = 0; x < lx; x+=16)
        {
            v_pix1 = vec_xl(x, pix1) ;

            // for(int ii=0; ii<16; ii++) { res[0] += abs(pix1[x + ii] - pix2[x + ii]); }
            v_pix2 = vec_xl(x, pix2) ;
            v_abs_diff_0 = vec_sub(vec_max(v_pix1, v_pix2), vec_min(v_pix1, v_pix2)) ;
            v_short_0_0 = vec_unpackh((vector signed char)v_abs_diff_0) ;
            v_short_0_0 = vec_and(v_short_0_0, v_unpack_mask) ;
            v_short_0_1 = vec_unpackl((vector signed char)v_abs_diff_0) ;
            v_short_0_1 = vec_and(v_short_0_1, v_unpack_mask) ;
            v_sum_0 = vec_add(v_short_0_0, v_short_0_1) ;
            v_results_0 = vec_add(v_results_0, v_sum_0) ;

            // for(int ii=0; ii<16; ii++) { res[1] += abs(pix1[x + ii] - pix3[x + ii]); }
            v_pix3 = vec_xl(x, pix3) ;
            v_abs_diff_1 = vec_sub(vec_max(v_pix1, v_pix3), vec_min(v_pix1, v_pix3)) ;
            v_short_1_0 = vec_unpackh((vector signed char)v_abs_diff_1) ;
            v_short_1_0 = vec_and(v_short_1_0, v_unpack_mask) ;
            v_short_1_1 = vec_unpackl((vector signed char)v_abs_diff_1) ;
            v_short_1_1 = vec_and(v_short_1_1, v_unpack_mask) ;
            v_sum_1 = vec_add(v_short_1_0, v_short_1_1) ;
            v_results_1 = vec_add(v_results_1, v_sum_1) ;


            // for(int ii=0; ii<16; ii++) { res[2] += abs(pix1[x + ii] - pix4[x + ii]); }
            v_pix4 = vec_xl(x, pix4) ;
            v_abs_diff_2 = vec_sub(vec_max(v_pix1, v_pix4), vec_min(v_pix1, v_pix4)) ;
            v_short_2_0 = vec_unpackh((vector signed char)v_abs_diff_2) ;
            v_short_2_0 = vec_and(v_short_2_0, v_unpack_mask) ;
            v_short_2_1 = vec_unpackl((vector signed char)v_abs_diff_2) ;
            v_short_2_1 = vec_and(v_short_2_1, v_unpack_mask) ;
            v_sum_2 = vec_add(v_short_2_0, v_short_2_1) ;
            v_results_2 = vec_add(v_results_2, v_sum_2) ;


            // for(int ii=0; ii<16; ii++) { res[3] += abs(pix1[x + ii] - pix5[x + ii]); }
            v_pix5 = vec_xl(x, pix5) ;
            v_abs_diff_3 = vec_sub(vec_max(v_pix1, v_pix5), vec_min(v_pix1, v_pix5)) ;
            v_short_3_0 = vec_unpackh((vector signed char)v_abs_diff_3) ;
            v_short_3_0 = vec_and(v_short_3_0, v_unpack_mask) ;
            v_short_3_1 = vec_unpackl((vector signed char)v_abs_diff_3) ;
            v_short_3_1 = vec_and(v_short_3_1, v_unpack_mask) ;
            v_sum_3 = vec_add(v_short_3_0, v_short_3_1) ;
            v_results_3 = vec_add(v_results_3, v_sum_3) ;
        }

        pix1 += FENC_STRIDE;
        pix2 += frefstride;
        pix3 += frefstride;
        pix4 += frefstride;
        pix5 += frefstride;
    }

    
    v_results_int_0 = vec_sum4s((vector signed short)v_results_0, (vector signed int)v_zeros) ;
    v_results_int_0 = vec_sums(v_results_int_0, (vector signed int)v_zeros) ;
    res[0] = v_results_int_0[3] ;


    v_results_int_1 = vec_sum4s((vector signed short)v_results_1, (vector signed int)v_zeros) ;
    v_results_int_1 = vec_sums(v_results_int_1, (vector signed int)v_zeros) ;
    res[1] = v_results_int_1[3] ;


    v_results_int_2 = vec_sum4s((vector signed short)v_results_2, (vector signed int)v_zeros) ;
    v_results_int_2 = vec_sums(v_results_int_2, (vector signed int)v_zeros) ;
    res[2] = v_results_int_2[3] ;


    v_results_int_3 = vec_sum4s((vector signed short)v_results_3, (vector signed int)v_zeros) ;
    v_results_int_3 = vec_sums(v_results_int_3, (vector signed int)v_zeros) ;
    res[3] = v_results_int_3[3] ;

    //printf("<32 32>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
} // end sad_x4_altivec

template<>
void inline sad_x4_altivec<32, 64>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[4];
    sad16_x4_altivec<16, 64>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<16, 64>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, res);
    res[0] += sum[0];	
    res[1] += sum[1];	
    res[2] += sum[2];	
    res[3] += sum[3];	
    //printf("<32 64>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}

template<>
void inline sad_x4_altivec<48, 64>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[8];
    sad16_x4_altivec<16, 64>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<16, 64>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, sum+4);
    sad16_x4_altivec<16, 64>(pix1+32, pix2+32, pix3+32, pix4+32, pix5+32, frefstride, res);
    res[0] = sum[0]+sum[4]+res[0];	
    res[1] = sum[1]+sum[5]+res[1];	
    res[2] = sum[2]+sum[6]+res[2];	
    res[3] = sum[3]+sum[7]+res[3];	
    //printf("<48 64>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}

template<>
void inline sad_x4_altivec<64, 16>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[12];
    sad16_x4_altivec<16, 16>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<16, 16>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, sum+4);
    sad16_x4_altivec<16, 16>(pix1+32, pix2+32, pix3+32, pix4+32, pix5+32, frefstride, sum+8);
    sad16_x4_altivec<16, 16>(pix1+48, pix2+48, pix3+48, pix4+48, pix5+48, frefstride, res);              
    res[0] = sum[0]+sum[4]+sum[8]+res[0];	
    res[1] = sum[1]+sum[5]+sum[9]+res[1];	
    res[2] = sum[2]+sum[6]+sum[10]+res[2];	
    res[3] = sum[3]+sum[7]+sum[11]+res[3];	
    //printf("<64 16>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}

template<>
void inline sad_x4_altivec<64, 32>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[12];
    sad16_x4_altivec<16, 32>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<16, 32>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, sum+4);
    sad16_x4_altivec<16, 32>(pix1+32, pix2+32, pix3+32, pix4+32, pix5+32, frefstride, sum+8);
    sad16_x4_altivec<16, 32>(pix1+48, pix2+48, pix3+48, pix4+48, pix5+48, frefstride, res);              
    res[0] = sum[0]+sum[4]+sum[8]+res[0];	
    res[1] = sum[1]+sum[5]+sum[9]+res[1];	
    res[2] = sum[2]+sum[6]+sum[10]+res[2];	
    res[3] = sum[3]+sum[7]+sum[11]+res[3];	
    //printf("<64 32>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}

template<>
void inline sad_x4_altivec<64, 48>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[12];
    sad16_x4_altivec<16, 48>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<16, 48>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, sum+4);
    sad16_x4_altivec<16, 48>(pix1+32, pix2+32, pix3+32, pix4+32, pix5+32, frefstride, sum+8);
    sad16_x4_altivec<16, 48>(pix1+48, pix2+48, pix3+48, pix4+48, pix5+48, frefstride, res);              
    res[0] = sum[0]+sum[4]+sum[8]+res[0];	
    res[1] = sum[1]+sum[5]+sum[9]+res[1];	
    res[2] = sum[2]+sum[6]+sum[10]+res[2];	
    res[3] = sum[3]+sum[7]+sum[11]+res[3];	
    //printf("<64 48>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}

template<>
void inline sad_x4_altivec<64, 64>(const pixel* pix1, const pixel* pix2, const pixel* pix3, const pixel* pix4, const pixel* pix5, intptr_t frefstride, int32_t* res)
{
    int32_t sum[12];
    sad16_x4_altivec<16, 64>(pix1, pix2, pix3, pix4, pix5, frefstride, sum);
    sad16_x4_altivec<16, 64>(pix1+16, pix2+16, pix3+16, pix4+16, pix5+16, frefstride, sum+4);
    sad16_x4_altivec<16, 64>(pix1+32, pix2+32, pix3+32, pix4+32, pix5+32, frefstride, sum+8);
    sad16_x4_altivec<16, 64>(pix1+48, pix2+48, pix3+48, pix4+48, pix5+48, frefstride, res);              
    res[0] = sum[0]+sum[4]+sum[8]+res[0];	
    res[1] = sum[1]+sum[5]+sum[9]+res[1];	
    res[2] = sum[2]+sum[6]+sum[10]+res[2];	
    res[3] = sum[3]+sum[7]+sum[11]+res[3];	
    //printf("<64 64>%d %d %d %d\n", res[0], res[1], res[2], res[3]);
}


/***********************************************************************
 * SATD routines - altivec implementation
 **********************************************************************/
#define HADAMARD4_VEC(s0, s1, s2, s3, d0, d1, d2, d3) \
{\
    vec_s16_t t0, t1, t2, t3;\
    t0 = vec_add(s0, s1);\
    t1 = vec_sub(s0, s1);\
    t2 = vec_add(s2, s3);\
    t3 = vec_sub(s2, s3);\
    d0 = vec_add(t0, t2);\
    d2 = vec_sub(t0, t2);\
    d1 = vec_add(t1, t3);\
    d3 = vec_sub(t1, t3);\
}

#define VEC_TRANSPOSE_4(a0,a1,a2,a3,b0,b1,b2,b3) \
    b0 = vec_mergeh( a0, a0 ); \
    b1 = vec_mergeh( a1, a0 ); \
    b2 = vec_mergeh( a2, a0 ); \
    b3 = vec_mergeh( a3, a0 ); \
    a0 = vec_mergeh( b0, b2 ); \
    a1 = vec_mergel( b0, b2 ); \
    a2 = vec_mergeh( b1, b3 ); \
    a3 = vec_mergel( b1, b3 ); \
    b0 = vec_mergeh( a0, a2 ); \
    b1 = vec_mergel( a0, a2 ); \
    b2 = vec_mergeh( a1, a3 ); \
    b3 = vec_mergel( a1, a3 )

#define VEC_TRANSPOSE_8(a0,a1,a2,a3,a4,a5,a6,a7,b0,b1,b2,b3,b4,b5,b6,b7) \
    b0 = vec_mergeh( a0, a4 ); \
    b1 = vec_mergel( a0, a4 ); \
    b2 = vec_mergeh( a1, a5 ); \
    b3 = vec_mergel( a1, a5 ); \
    b4 = vec_mergeh( a2, a6 ); \
    b5 = vec_mergel( a2, a6 ); \
    b6 = vec_mergeh( a3, a7 ); \
    b7 = vec_mergel( a3, a7 ); \
    a0 = vec_mergeh( b0, b4 ); \
    a1 = vec_mergel( b0, b4 ); \
    a2 = vec_mergeh( b1, b5 ); \
    a3 = vec_mergel( b1, b5 ); \
    a4 = vec_mergeh( b2, b6 ); \
    a5 = vec_mergel( b2, b6 ); \
    a6 = vec_mergeh( b3, b7 ); \
    a7 = vec_mergel( b3, b7 ); \
    b0 = vec_mergeh( a0, a4 ); \
    b1 = vec_mergel( a0, a4 ); \
    b2 = vec_mergeh( a1, a5 ); \
    b3 = vec_mergel( a1, a5 ); \
    b4 = vec_mergeh( a2, a6 ); \
    b5 = vec_mergel( a2, a6 ); \
    b6 = vec_mergeh( a3, a7 ); \
    b7 = vec_mergel( a3, a7 )

int satd_4x4_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16( int, sum );

    LOAD_ZERO;              
    vec_s16_t pix1v, pix2v;
    vec_s16_t diff0v, diff1v, diff2v, diff3v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v;
    vec_s32_t satdv, satdv1, satdv2, satdv3;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );       
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;
	
    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    /* Hadamar H */
    HADAMARD4_VEC(diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v);
    VEC_TRANSPOSE_4( temp0v, temp1v, temp2v, temp3v, diff0v, diff1v, diff2v, diff3v );
    /* Hadamar V */
    HADAMARD4_VEC(diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v);

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1 = vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2 = vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3 = vec_sum4s( temp3v, zero_s32v );

    satdv += satdv1;
    satdv2 += satdv3;
    satdv += satdv2;

    satdv = vec_sum2s( satdv, zero_s32v );
    //satdv = vec_splat( satdv, 1 );
    //vec_ste( satdv, 0, &sum );
    sum = vec_extract(satdv, 1);
    //print(sum);
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    satdv = vec_sum2s( satdv, zero_s32v );
    //satdv = vec_splat( satdv, 1 );
    //vec_ste( satdv, 0, &sum );
    sum = vec_extract(satdv, 1);
    //print(sum);
#endif
    return sum >> 1;
}

#define HADAMARD4_x2vec(v_out0, v_out1, v_in0, v_in1, v_perm_l0_0, v_perm_l0_1) \
{ \
    \
    vector unsigned int v_l0_input_0, v_l0_input_1 ;  \
    v_l0_input_0 = vec_perm((vector unsigned int)v_in0, (vector unsigned int)v_in1, v_perm_l0_0) ;    \
    v_l0_input_1 = vec_perm((vector unsigned int)v_in0, (vector unsigned int)v_in1, v_perm_l0_1) ;    \
    \
    vector unsigned int v_l0_add_result, v_l0_sub_result ;    \
    v_l0_add_result = vec_add(v_l0_input_0, v_l0_input_1) ; \
    v_l0_sub_result = vec_sub(v_l0_input_0, v_l0_input_1) ; \
    \
    vector unsigned char v_perm_l1_0 = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17} ; \
    vector unsigned char v_perm_l1_1 = {0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0xF,  0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F} ; \
    \
    vector unsigned int v_l1_input_0, v_l1_input_1 ;  \
    v_l1_input_0 = vec_perm(v_l0_add_result, v_l0_sub_result, v_perm_l1_0) ;    \
    v_l1_input_1 = vec_perm(v_l0_add_result, v_l0_sub_result, v_perm_l1_1) ;    \
    \
    vector unsigned int v_l1_add_result, v_l1_sub_result ;    \
    v_l1_add_result = vec_add(v_l1_input_0, v_l1_input_1) ; \
    v_l1_sub_result = vec_sub(v_l1_input_0, v_l1_input_1) ; \
    \
    \
    v_out0 = v_l1_add_result ;    \
    v_out1 = v_l1_sub_result ;    \
\
\
}

int satd_4x8_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16( int, sum );

    LOAD_ZERO;              
    vec_s16_t pix1v, pix2v;
    vec_s16_t diff0v, diff1v, diff2v, diff3v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v;
    vec_s32_t satdv, satdv1, satdv2, satdv3;;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;
	
    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    /* Hadamar H */
    HADAMARD4_VEC(diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v);
    VEC_TRANSPOSE_4( temp0v, temp1v, temp2v, temp3v, diff0v, diff1v, diff2v, diff3v );
    /* Hadamar V */
    HADAMARD4_VEC(diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v);

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    satdv += satdv1;
    satdv2 += satdv3;
    satdv += satdv2;
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );
#endif

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;
	
    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                                    
    pix2   += stride_pix2;

    /* Hadamar H */
    HADAMARD4_VEC(diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v);
    VEC_TRANSPOSE_4( temp0v, temp1v, temp2v, temp3v, diff0v, diff1v, diff2v, diff3v );
    /* Hadamar V */
    HADAMARD4_VEC(diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v);

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv += vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1 = vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2 = vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3 = vec_sum4s( temp3v, zero_s32v );

    satdv += satdv1;
    satdv2 += satdv3;
    satdv += satdv2;

    satdv = vec_sum2s( satdv, zero_s32v );
    sum = vec_extract(satdv, 1);	
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    satdv = vec_sum2s( satdv, zero_s32v );
    satdv = vec_splat( satdv, 1 );
    vec_ste( satdv, 0, &sum );
#endif
    return sum >> 1;
}

#if 1
static int satd_8x4_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    const vector signed short v_unsigned_short_mask = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ;
    vector unsigned char v_pix1_ub, v_pix2_ub ;
    vector signed short v_pix1_ss, v_pix2_ss ;
    vector signed short v_sub ;
    vector signed int v_sub_sw_0, v_sub_sw_1 ;
    vector signed int v_packed_sub_0, v_packed_sub_1 ;
    vector unsigned int v_hadamard_result_0, v_hadamard_result_1, v_hadamard_result_2, v_hadamard_result_3 ;

   // for (int i = 0; i < 4; i+=2, pix1 += 2*stride_pix1, pix2 += 2*stride_pix2)
   // {
        //a0 = (pix1[0] - pix2[0]) + ((sum2_t)(pix1[4] - pix2[4]) << BITS_PER_SUM);
        //a1 = (pix1[1] - pix2[1]) + ((sum2_t)(pix1[5] - pix2[5]) << BITS_PER_SUM);
        //a2 = (pix1[2] - pix2[2]) + ((sum2_t)(pix1[6] - pix2[6]) << BITS_PER_SUM);
        //a3 = (pix1[3] - pix2[3]) + ((sum2_t)(pix1[7] - pix2[7]) << BITS_PER_SUM);

    // Load 16 elements from each pix array
    v_pix1_ub = vec_xl(0, pix1) ;
    v_pix2_ub = vec_xl(0, pix2) ;

    // We only care about the top 8, and in short format
    v_pix1_ss = vec_unpackh((vector signed char)v_pix1_ub) ;
    v_pix2_ss = vec_unpackh((vector signed char)v_pix2_ub) ;

    // Undo the sign extend of the unpacks
    v_pix1_ss = vec_and(v_pix1_ss, v_unsigned_short_mask) ;
    v_pix2_ss = vec_and(v_pix2_ss, v_unsigned_short_mask) ;

    // Peform the subtraction
    v_sub = vec_sub(v_pix1_ss, v_pix2_ss) ;

    // Unpack the sub results into ints
    v_sub_sw_0 = vec_unpackh(v_sub) ;
    v_sub_sw_1 = vec_unpackl(v_sub) ;
    v_sub_sw_1 = vec_sl(v_sub_sw_1, (vector unsigned int){16,16,16,16}) ;

    // Add the int sub results (compatibility with the original code)
    v_packed_sub_0 = vec_add(v_sub_sw_0, v_sub_sw_1) ;

    //a0 = (pix1[0] - pix2[0]) + ((sum2_t)(pix1[4] - pix2[4]) << BITS_PER_SUM);
    //a1 = (pix1[1] - pix2[1]) + ((sum2_t)(pix1[5] - pix2[5]) << BITS_PER_SUM);
    //a2 = (pix1[2] - pix2[2]) + ((sum2_t)(pix1[6] - pix2[6]) << BITS_PER_SUM);
    //a3 = (pix1[3] - pix2[3]) + ((sum2_t)(pix1[7] - pix2[7]) << BITS_PER_SUM);

    // Load 16 elements from each pix array
    v_pix1_ub = vec_xl(stride_pix1, pix1) ;
    v_pix2_ub = vec_xl(stride_pix2, pix2) ;

    // We only care about the top 8, and in short format
    v_pix1_ss = vec_unpackh((vector signed char)v_pix1_ub) ;
    v_pix2_ss = vec_unpackh((vector signed char)v_pix2_ub) ;

    // Undo the sign extend of the unpacks
    v_pix1_ss = vec_and(v_pix1_ss, v_unsigned_short_mask) ;
    v_pix2_ss = vec_and(v_pix2_ss, v_unsigned_short_mask) ;

    // Peform the subtraction
    v_sub = vec_sub(v_pix1_ss, v_pix2_ss) ;

    // Unpack the sub results into ints
    v_sub_sw_0 = vec_unpackh(v_sub) ;
    v_sub_sw_1 = vec_unpackl(v_sub) ;
    v_sub_sw_1 = vec_sl(v_sub_sw_1, (vector unsigned int){16,16,16,16}) ;

    // Add the int sub results (compatibility with the original code)
    v_packed_sub_1 = vec_add(v_sub_sw_0, v_sub_sw_1) ;

    // original: HADAMARD4(tmp[i][0], tmp[i][1], tmp[i][2], tmp[i][3], a0, a1, a2, a3);
    // modified while vectorizing: HADAMARD4(tmp[i][0], tmp[i][1], tmp[i][2], tmp[i][3], v_packed_sub_0[0], v_packed_sub_0[1], v_packed_sub_0[2], v_packed_sub_0[3]);

    // original: HADAMARD4(tmp[i+1][0], tmp[i+1][1], tmp[i+1][2], tmp[i+1][3], a0, a1, a2, a3);
    // modified while vectorizing: HADAMARD4(tmp[i+1][0], tmp[i+1][1], tmp[i+1][2], tmp[i+1][3], v_packed_sub_1[0], v_packed_sub_1[1], v_packed_sub_1[2], v_packed_sub_1[3]);

    // Go after two hadamard4(int) at once, fully utilizing the vector width
    // Note that the hadamard4(int) provided by x264/x265 is actually two hadamard4(short) simultaneously
    const vector unsigned char v_perm_l0_0 = {0x00, 0x01, 0x02, 0x03, 0x10, 0x11, 0x12, 0x13, 0x08, 0x09, 0x0A, 0x0B, 0x18, 0x19, 0x1A, 0x1B} ;
    const vector unsigned char v_perm_l0_1 = {0x04, 0x05, 0x06, 0x07, 0x14, 0x15, 0x16, 0x17, 0x0C, 0x0D, 0x0E, 0x0F, 0x1C, 0x1D, 0x1E, 0x1F} ;
    HADAMARD4_x2vec(v_hadamard_result_0, v_hadamard_result_1, v_packed_sub_0, v_packed_sub_1, v_perm_l0_0, v_perm_l0_1) ;

    //##
    // tmp[0][0] = v_hadamard_result_0[0] ;
    // tmp[0][1] = v_hadamard_result_0[2] ;
    // tmp[0][2] = v_hadamard_result_1[0] ;
    // tmp[0][3] = v_hadamard_result_1[2] ;

    // tmp[1][0] = v_hadamard_result_0[1] ;
    // tmp[1][1] = v_hadamard_result_0[3] ;
    // tmp[1][2] = v_hadamard_result_1[1] ;
    // tmp[1][3] = v_hadamard_result_1[3] ;
    //## 

    //a0 = (pix1[0] - pix2[0]) + ((sum2_t)(pix1[4] - pix2[4]) << BITS_PER_SUM);
    //a1 = (pix1[1] - pix2[1]) + ((sum2_t)(pix1[5] - pix2[5]) << BITS_PER_SUM);
    //a2 = (pix1[2] - pix2[2]) + ((sum2_t)(pix1[6] - pix2[6]) << BITS_PER_SUM);
    //a3 = (pix1[3] - pix2[3]) + ((sum2_t)(pix1[7] - pix2[7]) << BITS_PER_SUM);

    // Load 16 elements from each pix array
    v_pix1_ub = vec_xl(2*stride_pix1, pix1) ;
    v_pix2_ub = vec_xl(2*stride_pix1, pix2) ;

    // We only care about the top 8, and in short format
    v_pix1_ss = vec_unpackh((vector signed char)v_pix1_ub) ;
    v_pix2_ss = vec_unpackh((vector signed char)v_pix2_ub) ;

    // Undo the sign extend of the unpacks
    v_pix1_ss = vec_and(v_pix1_ss, v_unsigned_short_mask) ;
    v_pix2_ss = vec_and(v_pix2_ss, v_unsigned_short_mask) ;

    // Peform the subtraction
    v_sub = vec_sub(v_pix1_ss, v_pix2_ss) ;

    // Unpack the sub results into ints
    v_sub_sw_0 = vec_unpackh(v_sub) ;
    v_sub_sw_1 = vec_unpackl(v_sub) ;
    v_sub_sw_1 = vec_sl(v_sub_sw_1, (vector unsigned int){16,16,16,16}) ;

    // Add the int sub results (compatibility with the original code)
    v_packed_sub_0 = vec_add(v_sub_sw_0, v_sub_sw_1) ;

    //a0 = (pix1[0] - pix2[0]) + ((sum2_t)(pix1[4] - pix2[4]) << BITS_PER_SUM);
    //a1 = (pix1[1] - pix2[1]) + ((sum2_t)(pix1[5] - pix2[5]) << BITS_PER_SUM);
    //a2 = (pix1[2] - pix2[2]) + ((sum2_t)(pix1[6] - pix2[6]) << BITS_PER_SUM);
    //a3 = (pix1[3] - pix2[3]) + ((sum2_t)(pix1[7] - pix2[7]) << BITS_PER_SUM);

    // Load 16 elements from each pix array
    v_pix1_ub = vec_xl(3*stride_pix1, pix1) ;
    v_pix2_ub = vec_xl(3*stride_pix2, pix2) ;

    // We only care about the top 8, and in short format
    v_pix1_ss = vec_unpackh((vector signed char)v_pix1_ub) ;
    v_pix2_ss = vec_unpackh((vector signed char)v_pix2_ub) ;

    // Undo the sign extend of the unpacks
    v_pix1_ss = vec_and(v_pix1_ss, v_unsigned_short_mask) ;
    v_pix2_ss = vec_and(v_pix2_ss, v_unsigned_short_mask) ;

    // Peform the subtraction
    v_sub = vec_sub(v_pix1_ss, v_pix2_ss) ;

    // Unpack the sub results into ints
    v_sub_sw_0 = vec_unpackh(v_sub) ;
    v_sub_sw_1 = vec_unpackl(v_sub) ;
    v_sub_sw_1 = vec_sl(v_sub_sw_1, (vector unsigned int){16,16,16,16}) ;

    // Add the int sub results (compatibility with the original code)
    v_packed_sub_1 = vec_add(v_sub_sw_0, v_sub_sw_1) ;


    // original: HADAMARD4(tmp[i][0], tmp[i][1], tmp[i][2], tmp[i][3], a0, a1, a2, a3);
    // modified while vectorizing: HADAMARD4(tmp[i][0], tmp[i][1], tmp[i][2], tmp[i][3], v_packed_sub_0[0], v_packed_sub_0[1], v_packed_sub_0[2], v_packed_sub_0[3]);

    // original: HADAMARD4(tmp[i+1][0], tmp[i+1][1], tmp[i+1][2], tmp[i+1][3], a0, a1, a2, a3);
    // modified while vectorizing: HADAMARD4(tmp[i+1][0], tmp[i+1][1], tmp[i+1][2], tmp[i+1][3], v_packed_sub_1[0], v_packed_sub_1[1], v_packed_sub_1[2], v_packed_sub_1[3]);

    // Go after two hadamard4(int) at once, fully utilizing the vector width
    // Note that the hadamard4(int) provided by x264/x265 is actually two hadamard4(short) simultaneously
    HADAMARD4_x2vec(v_hadamard_result_2, v_hadamard_result_3, v_packed_sub_0, v_packed_sub_1, v_perm_l0_0, v_perm_l0_1) ;

    //##
    //## tmp[2][0] = v_hadamard_result_2[0] ;
    //## tmp[2][1] = v_hadamard_result_2[2] ;
    //## tmp[2][2] = v_hadamard_result_3[0] ;
    //## tmp[2][3] = v_hadamard_result_3[2] ;
    //##
    //##        tmp[3][0] = v_hadamard_result_2[1] ;
    //##        tmp[3][1] = v_hadamard_result_2[3] ;
    //##        tmp[3][2] = v_hadamard_result_3[1] ;
    //##        tmp[3][3] = v_hadamard_result_3[3] ;
    //## 
   // }
   //    for (int i = 0; i < 4; i++)
   //    {
       // HADAMARD4(a0, a1, a2, a3, tmp[0][0], tmp[1][0], tmp[2][0], tmp[3][0]);
       // sum += abs2(a0) + abs2(a1) + abs2(a2) + abs2(a3);

       // HADAMARD4(a0, a1, a2, a3, tmp[0][1], tmp[1][1], tmp[2][1], tmp[3][1]);
       // sum += abs2(a0) + abs2(a1) + abs2(a2) + abs2(a3);
    const vector unsigned char v_lowerloop_perm_l0_0 = {0x00, 0x01, 0x02, 0x03, 0x08, 0x09, 0x0A, 0x0B, 0x10, 0x11, 0x12, 0x13, 0x18, 0x19, 0x1A, 0x1B} ;
    const vector unsigned char v_lowerloop_perm_l0_1 = {0x04, 0x05, 0x06, 0x07, 0x0C, 0x0D, 0x0E, 0x0F, 0x14, 0x15, 0x16, 0x17, 0x1C, 0x1D, 0x1E, 0x1F} ;
    HADAMARD4_x2vec(v_hadamard_result_0, v_hadamard_result_2, v_hadamard_result_0, v_hadamard_result_2, v_lowerloop_perm_l0_0, v_lowerloop_perm_l0_1) ;
            
    const vector unsigned int v_15      = {15, 15, 15, 15} ; 
    const vector unsigned int v_0x10001 = (vector unsigned int){ 0x10001, 0x10001, 0x10001, 0x10001 };
    const vector unsigned int v_0xffff  = (vector unsigned int){ 0xffff, 0xffff, 0xffff, 0xffff };
      

    vector unsigned int v_hadamard_result_s_0 ;
    v_hadamard_result_s_0 = vec_sra(v_hadamard_result_0, v_15) ;
    v_hadamard_result_s_0 = vec_and(v_hadamard_result_s_0, v_0x10001) ;
    asm ("vmuluwm %0,%1,%2"
              : "=v" (v_hadamard_result_s_0)
              : "v"  (v_hadamard_result_s_0) , "v" (v_0xffff)
            ) ;
    v_hadamard_result_0 = vec_add(v_hadamard_result_0, v_hadamard_result_s_0) ;
    v_hadamard_result_0 = vec_xor(v_hadamard_result_0, v_hadamard_result_s_0) ;

    vector unsigned int v_hadamard_result_s_2 ;
    v_hadamard_result_s_2 = vec_sra(v_hadamard_result_2, v_15) ;
    v_hadamard_result_s_2 = vec_and(v_hadamard_result_s_2, v_0x10001) ;
    asm ("vmuluwm %0,%1,%2"
              : "=v" (v_hadamard_result_s_2)
              : "v"  (v_hadamard_result_s_2) , "v" (v_0xffff)
            ) ;
    v_hadamard_result_2 = vec_add(v_hadamard_result_2, v_hadamard_result_s_2) ;
    v_hadamard_result_2 = vec_xor(v_hadamard_result_2, v_hadamard_result_s_2) ;

    // HADAMARD4(a0, a1, a2, a3, tmp[0][2], tmp[1][2], tmp[2][2], tmp[3][2]);
    // sum += abs2(a0) + abs2(a1) + abs2(a2) + abs2(a3);

    // HADAMARD4(a0, a1, a2, a3, tmp[0][3], tmp[1][3], tmp[2][3], tmp[3][3]);
    // sum += abs2(a0) + abs2(a1) + abs2(a2) + abs2(a3);

    HADAMARD4_x2vec(v_hadamard_result_1, v_hadamard_result_3, v_hadamard_result_1, v_hadamard_result_3, v_lowerloop_perm_l0_0, v_lowerloop_perm_l0_1) ;

    vector unsigned int v_hadamard_result_s_1 ;
    v_hadamard_result_s_1 = vec_sra(v_hadamard_result_1, v_15) ;
    v_hadamard_result_s_1 = vec_and(v_hadamard_result_s_1, v_0x10001) ;
    asm ("vmuluwm %0,%1,%2"
              : "=v" (v_hadamard_result_s_1)
              : "v"  (v_hadamard_result_s_1) , "v" (v_0xffff)
            ) ;
    v_hadamard_result_1 = vec_add(v_hadamard_result_1, v_hadamard_result_s_1) ;
    v_hadamard_result_1 = vec_xor(v_hadamard_result_1, v_hadamard_result_s_1) ;

    vector unsigned int v_hadamard_result_s_3 ;
    v_hadamard_result_s_3 = vec_sra(v_hadamard_result_3, v_15) ;
    v_hadamard_result_s_3 = vec_and(v_hadamard_result_s_3, v_0x10001) ;
    asm ("vmuluwm %0,%1,%2"
              : "=v" (v_hadamard_result_s_3)
              : "v"  (v_hadamard_result_s_3) , "v" (v_0xffff)
            ) ;
    v_hadamard_result_3 = vec_add(v_hadamard_result_3, v_hadamard_result_s_3) ;
    v_hadamard_result_3 = vec_xor(v_hadamard_result_3, v_hadamard_result_s_3) ;

//   }


    vector unsigned int v_sum_0, v_sum_1 ;
    vector signed int v_sum ;

    v_sum_0 = vec_add(v_hadamard_result_0, v_hadamard_result_2) ;
    v_sum_1 = vec_add(v_hadamard_result_1, v_hadamard_result_3) ;

    v_sum_0 = vec_add(v_sum_0, v_sum_1) ;

    vector signed int v_zero = {0, 0, 0, 0} ;
    v_sum = vec_sums((vector signed int)v_sum_0, v_zero) ;

    // return (((sum_t)sum) + (sum >> BITS_PER_SUM)) >> 1;
    return (((sum_t)v_sum[3]) + (v_sum[3] >> BITS_PER_SUM)) >> 1;
}
#else
int satd_8x4_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16( int, sum );
    LOAD_ZERO;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diff0v, diff1v, diff2v, diff3v, diff4v, diff5v, diff6v, diff7v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v, temp4v, temp5v, temp6v, temp7v;
    vec_s32_t satdv;


    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff4v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff5v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff6v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff7v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;
	
    HADAMARD4_VEC( diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v );
    //HADAMARD4_VEC( diff4v, diff5v, diff6v, diff7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diff0v, diff1v, diff2v, diff3v,
                     diff4v, diff5v, diff6v, diff7v );

    HADAMARD4_VEC( diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diff4v, diff5v, diff6v, diff7v, temp4v, temp5v, temp6v, temp7v );

    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );

    satdv = vec_sums( satdv, zero_s32v );
    satdv = vec_splat( satdv, 3 );
    vec_ste( satdv, 0, &sum );

    //print(sum);
    return sum>>1;
}
#endif

int satd_8x8_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16( int, sum );
    LOAD_ZERO;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diff0v, diff1v, diff2v, diff3v, diff4v, diff5v, diff6v, diff7v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v, temp4v, temp5v, temp6v, temp7v;
    vec_s32_t satdv, satdv1, satdv2, satdv3, satdv4, satdv5, satdv6, satdv7;
    //vec_s32_t satdv=(vec_s32_t){0,0,0,0};


    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff4v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff5v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff6v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff7v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;
	
    HADAMARD4_VEC( diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diff4v, diff5v, diff6v, diff7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diff0v, diff1v, diff2v, diff3v,
                     diff4v, diff5v, diff6v, diff7v );

    HADAMARD4_VEC( diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diff4v, diff5v, diff6v, diff7v, temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;

    satdv = vec_sums( satdv, zero_s32v );
    sum = vec_extract(satdv, 3);
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );

    satdv = vec_sums( satdv, zero_s32v );
    satdv = vec_splat( satdv, 3 );
    vec_ste( satdv, 0, &sum );
#endif
    return sum>>1;
}

int satd_8x16_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16( int, sum );

    LOAD_ZERO;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diff0v, diff1v, diff2v, diff3v, diff4v, diff5v, diff6v, diff7v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v, temp4v, temp5v, temp6v, temp7v;
    //vec_s32_t satdv=(vec_s32_t){0,0,0,0};
    vec_s32_t satdv, satdv1, satdv2, satdv3, satdv4, satdv5, satdv6, satdv7;


    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff4v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff5v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff6v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff7v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;
	
    HADAMARD4_VEC( diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diff4v, diff5v, diff6v, diff7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diff0v, diff1v, diff2v, diff3v,
                     diff4v, diff5v, diff6v, diff7v );

    HADAMARD4_VEC( diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diff4v, diff5v, diff6v, diff7v, temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );
#endif

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff4v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff5v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff6v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16(vec_xl(0, pix2) );               
    diff7v = vec_sub( pix1v, pix2v );               
    pix1   += stride_pix1;                               
    pix2   += stride_pix2;
	
    HADAMARD4_VEC( diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diff4v, diff5v, diff6v, diff7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diff0v, diff1v, diff2v, diff3v,
                     diff4v, diff5v, diff6v, diff7v );

    HADAMARD4_VEC( diff0v, diff1v, diff2v, diff3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diff4v, diff5v, diff6v, diff7v, temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv += vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;

    satdv = vec_sums( satdv, zero_s32v );
    sum = vec_extract(satdv, 3);
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );

    satdv = vec_sums( satdv, zero_s32v );
    satdv = vec_splat( satdv, 3 );
    vec_ste( satdv, 0, &sum );
#endif
    return sum >> 1;
}

#define VEC_DIFF_S16(p1,i1,p2,i2,dh,dl)\
{\
    pix1v = (vec_s16_t)vec_xl(0, p1);\
    temp0v = vec_u8_to_s16_h( pix1v );\
    temp1v = vec_u8_to_s16_l( pix1v );\
    pix2v = (vec_s16_t)vec_xl(0, p2);\
    temp2v = vec_u8_to_s16_h( pix2v );\
    temp3v = vec_u8_to_s16_l( pix2v );\
    dh = vec_sub( temp0v, temp2v );\
    dl  = vec_sub( temp1v, temp3v );\
    p1 += i1;\
    p2 += i2;\
}


int satd_16x4_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16( int, sum );
    LOAD_ZERO;
    //vec_s32_t satdv=(vec_s32_t){0,0,0,0};
    vec_s32_t satdv, satdv1, satdv2, satdv3, satdv4, satdv5, satdv6, satdv7;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diffh0v, diffh1v, diffh2v, diffh3v;
    vec_s16_t diffl0v, diffl1v, diffl2v, diffl3v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v,
              temp4v, temp5v, temp6v, temp7v;

    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh0v,diffl0v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh1v, diffl1v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh2v, diffl2v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh3v, diffl3v);
    

    HADAMARD4_VEC( diffh0v, diffh1v, diffh2v, diffh3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffl0v, diffl1v, diffl2v, diffl3v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diffh0v, diffh1v, diffh2v, diffh3v,
                     diffl0v, diffl1v, diffl2v, diffl3v);

    HADAMARD4_VEC( diffh0v, diffh1v, diffh2v, diffh3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffl0v, diffl1v, diffl2v, diffl3v, temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;

    satdv = vec_sums( satdv, zero_s32v );
    sum = vec_extract(satdv, 3);
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );

    satdv = vec_sums( satdv, zero_s32v );
    satdv = vec_splat( satdv, 3 );
    vec_ste( satdv, 0, &sum );
#endif
    return sum >> 1;
}

int satd_16x8_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16( int, sum );
    LOAD_ZERO;
    //vec_s32_t satdv=(vec_s32_t){0,0,0,0};
    vec_s32_t satdv, satdv1, satdv2, satdv3, satdv4, satdv5, satdv6, satdv7;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diffh0v, diffh1v, diffh2v, diffh3v,
              diffh4v, diffh5v, diffh6v, diffh7v;
    vec_s16_t diffl0v, diffl1v, diffl2v, diffl3v,
              diffl4v, diffl5v, diffl6v, diffl7v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v,
              temp4v, temp5v, temp6v, temp7v;

    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh0v,diffl0v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh1v, diffl1v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh2v, diffl2v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh3v, diffl3v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh4v, diffl4v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh5v, diffl5v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh6v, diffl6v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh7v, diffl7v);
    
    HADAMARD4_VEC( diffh0v, diffh1v, diffh2v, diffh3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffh4v, diffh5v, diffh6v, diffh7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diffh0v, diffh1v, diffh2v, diffh3v,
                     diffh4v, diffh5v, diffh6v, diffh7v );

    HADAMARD4_VEC( diffh0v, diffh1v, diffh2v, diffh3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffh4v, diffh5v, diffh6v, diffh7v, temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );
#endif

    HADAMARD4_VEC( diffl0v, diffl1v, diffl2v, diffl3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffl4v, diffl5v, diffl6v, diffl7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diffl0v, diffl1v, diffl2v, diffl3v,
                     diffl4v, diffl5v, diffl6v, diffl7v );

    HADAMARD4_VEC( diffl0v, diffl1v, diffl2v, diffl3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffl4v, diffl5v, diffl6v, diffl7v,  temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv += vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;

    satdv = vec_sums( satdv, zero_s32v );
    sum = vec_extract(satdv, 3);
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );

    satdv = vec_sums( satdv, zero_s32v );
    satdv = vec_splat( satdv, 3 );
    vec_ste( satdv, 0, &sum );
#endif
    return sum >> 1;
}

int satd_16x16_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    ALIGN_VAR_16( int, sum );
    LOAD_ZERO;
    //vec_s32_t satdv=(vec_s32_t){0,0,0,0};
    vec_s32_t satdv, satdv1, satdv2, satdv3, satdv4, satdv5, satdv6, satdv7;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diffh0v, diffh1v, diffh2v, diffh3v,
              diffh4v, diffh5v, diffh6v, diffh7v;
    vec_s16_t diffl0v, diffl1v, diffl2v, diffl3v,
              diffl4v, diffl5v, diffl6v, diffl7v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v,
              temp4v, temp5v, temp6v, temp7v;

    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh0v,diffl0v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh1v, diffl1v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh2v, diffl2v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh3v, diffl3v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh4v, diffl4v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh5v, diffl5v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh6v, diffl6v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh7v, diffl7v);
    
    HADAMARD4_VEC( diffh0v, diffh1v, diffh2v, diffh3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffh4v, diffh5v, diffh6v, diffh7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diffh0v, diffh1v, diffh2v, diffh3v,
                     diffh4v, diffh5v, diffh6v, diffh7v );

    HADAMARD4_VEC( diffh0v, diffh1v, diffh2v, diffh3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffh4v, diffh5v, diffh6v, diffh7v, temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );
#endif

    HADAMARD4_VEC( diffl0v, diffl1v, diffl2v, diffl3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffl4v, diffl5v, diffl6v, diffl7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diffl0v, diffl1v, diffl2v, diffl3v,
                     diffl4v, diffl5v, diffl6v, diffl7v );

    HADAMARD4_VEC( diffl0v, diffl1v, diffl2v, diffl3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffl4v, diffl5v, diffl6v, diffl7v,  temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv += vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;

#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );
#endif
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh0v,diffl0v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh1v, diffl1v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh2v, diffl2v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh3v, diffl3v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh4v, diffl4v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh5v, diffl5v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh6v, diffl6v);
    VEC_DIFF_S16(pix1,stride_pix1,pix2,stride_pix2,diffh7v, diffl7v);
    
    HADAMARD4_VEC( diffh0v, diffh1v, diffh2v, diffh3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffh4v, diffh5v, diffh6v, diffh7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diffh0v, diffh1v, diffh2v, diffh3v,
                     diffh4v, diffh5v, diffh6v, diffh7v );

    HADAMARD4_VEC( diffh0v, diffh1v, diffh2v, diffh3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffh4v, diffh5v, diffh6v, diffh7v, temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv += vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );
#endif
    HADAMARD4_VEC( diffl0v, diffl1v, diffl2v, diffl3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffl4v, diffl5v, diffl6v, diffl7v, temp4v, temp5v, temp6v, temp7v );

    VEC_TRANSPOSE_8( temp0v, temp1v, temp2v, temp3v,
                     temp4v, temp5v, temp6v, temp7v,
                     diffl0v, diffl1v, diffl2v, diffl3v,
                     diffl4v, diffl5v, diffl6v, diffl7v );

    HADAMARD4_VEC( diffl0v, diffl1v, diffl2v, diffl3v, temp0v, temp1v, temp2v, temp3v );
    HADAMARD4_VEC( diffl4v, diffl5v, diffl6v, diffl7v,  temp4v, temp5v, temp6v, temp7v );

#if 1
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv += vec_sum4s( temp0v, zero_s32v);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv1= vec_sum4s( temp1v, zero_s32v );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv2= vec_sum4s( temp2v, zero_s32v );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv3= vec_sum4s( temp3v, zero_s32v );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv4 = vec_sum4s( temp4v, zero_s32v);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv5= vec_sum4s( temp5v, zero_s32v );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv6= vec_sum4s( temp6v, zero_s32v );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv7= vec_sum4s( temp7v, zero_s32v );

    satdv  += satdv1;
    satdv2 += satdv3;
    satdv4 += satdv5;
    satdv6 += satdv7;

    satdv  += satdv2;
    satdv4 += satdv6;
    satdv  += satdv4;

    satdv = vec_sums( satdv, zero_s32v );
    sum = vec_extract(satdv, 3);
#else
    temp0v = vec_max( temp0v, vec_sub( zero_s16v, temp0v ) );
    satdv = vec_sum4s( temp0v, satdv);

    temp1v = vec_max( temp1v, vec_sub( zero_s16v, temp1v ) );
    satdv= vec_sum4s( temp1v, satdv );

    temp2v = vec_max( temp2v, vec_sub( zero_s16v, temp2v ) );
    satdv= vec_sum4s( temp2v, satdv );

    temp3v = vec_max( temp3v, vec_sub( zero_s16v, temp3v ) );
    satdv= vec_sum4s( temp3v, satdv );

    temp4v = vec_max( temp4v, vec_sub( zero_s16v, temp4v ) );
    satdv = vec_sum4s( temp4v, satdv);

    temp5v = vec_max( temp5v, vec_sub( zero_s16v, temp5v ) );
    satdv= vec_sum4s( temp5v, satdv );

    temp6v = vec_max( temp6v, vec_sub( zero_s16v, temp6v ) );
    satdv= vec_sum4s( temp6v, satdv );

    temp7v = vec_max( temp7v, vec_sub( zero_s16v, temp7v ) );
    satdv= vec_sum4s( temp7v, satdv );

    satdv = vec_sums( satdv, zero_s32v );
    satdv = vec_splat( satdv, 3 );
    vec_ste( satdv, 0, &sum );
#endif
    return sum >> 1;
}


template<int w, int h>
int satd_altivec(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2);

template<>
int satd_altivec<4, 4>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    return satd_4x4_altivec(pix1, stride_pix1, pix2, stride_pix2);
}

template<>
int satd_altivec<4, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    return  satd_4x8_altivec(pix1, stride_pix1, pix2, stride_pix2);
}

template<>
int satd_altivec<4, 12>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_4x4_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+4*stride_pix1, stride_pix1, pix2+4*stride_pix2, stride_pix2);

    return satd;
}

template<>
int satd_altivec<4, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_4x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+8*stride_pix1, stride_pix1, pix2+8*stride_pix2, stride_pix2);

    return satd;
}

template<>
int satd_altivec<4, 24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_4x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+8*stride_pix1, stride_pix1, pix2+8*stride_pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+16*stride_pix1, stride_pix1, pix2+16*stride_pix2, stride_pix2);

    return satd;
}

template<>
int satd_altivec<4, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_4x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+8*stride_pix1, stride_pix1, pix2+8*stride_pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+16*stride_pix1, stride_pix1, pix2+16*stride_pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+24*stride_pix1, stride_pix1, pix2+24*stride_pix2, stride_pix2);

    return satd;
}

template<>
int satd_altivec<4, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_altivec<4, 32>(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_altivec<4, 32>(pix1+32*stride_pix1, stride_pix1, pix2+32*stride_pix2, stride_pix2);

    return satd;
}

template<>
int satd_altivec<8, 4>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    return satd_8x4_altivec(pix1, stride_pix1, pix2, stride_pix2);
}

template<>
int satd_altivec<8, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    return satd_8x8_altivec(pix1, stride_pix1, pix2, stride_pix2);
}

template<>
int satd_altivec<8, 12>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_8x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x4_altivec(pix1+8*stride_pix1, stride_pix1, pix2+8*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<8,16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    return satd_8x16_altivec(pix1, stride_pix1, pix2, stride_pix2);
}

template<>
int satd_altivec<8,24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_8x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x16_altivec(pix1+8*stride_pix1, stride_pix1, pix2+8*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<8,32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_8x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x16_altivec(pix1+16*stride_pix1, stride_pix1, pix2+16*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<8,64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_8x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x16_altivec(pix1+16*stride_pix1, stride_pix1, pix2+16*stride_pix2, stride_pix2)
		+ satd_8x16_altivec(pix1+32*stride_pix1, stride_pix1, pix2+32*stride_pix2, stride_pix2)
		+ satd_8x16_altivec(pix1+48*stride_pix1, stride_pix1, pix2+48*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<12, 4>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_8x4_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x4_altivec(pix1+8, stride_pix1, pix2+8, stride_pix2);
    return satd;
}

template<>
int satd_altivec<12, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_8x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+8, stride_pix1, pix2+8, stride_pix2);
    return satd;
}

template<>
int satd_altivec<12, 12>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 8*stride_pix1;
    const pixel *pix4 = pix2 + 8*stride_pix2;
    satd = satd_8x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+8, stride_pix1, pix2+8, stride_pix2);
              + satd_8x4_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_4x4_altivec(pix3+8, stride_pix1, pix4+8, stride_pix2);
    return satd;
}

template<>
int satd_altivec<12, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 8*stride_pix1;
    const pixel *pix4 = pix2 + 8*stride_pix2;
    satd = satd_8x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+8, stride_pix1, pix2+8, stride_pix2)
		+ satd_8x8_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_4x8_altivec(pix3+8, stride_pix1, pix4+8, stride_pix2);
    return satd;
}

template<>
int satd_altivec<12, 24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 8*stride_pix1;
    const pixel *pix4 = pix2 + 8*stride_pix2;
    satd = satd_8x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_4x8_altivec(pix1+8, stride_pix1, pix2+8, stride_pix2)
		+ satd_8x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_altivec<4, 16>(pix3+8, stride_pix1, pix4+8, stride_pix2);
    return satd;
}

template<>
int satd_altivec<12, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    satd = satd_8x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_altivec<4, 16>(pix1+8, stride_pix1, pix2+8, stride_pix2)
		+ satd_8x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_altivec<4, 16>(pix3+8, stride_pix1, pix4+8, stride_pix2);
    return satd;
}

template<>
int satd_altivec<12, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    const pixel *pix5 = pix1 + 32*stride_pix1;
    const pixel *pix6 = pix2 + 32*stride_pix2;
    const pixel *pix7 = pix1 + 48*stride_pix1;
    const pixel *pix8 = pix2 + 48*stride_pix2;
    satd = satd_8x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_altivec<4, 16>(pix1+8, stride_pix1, pix2+8, stride_pix2)
		+ satd_8x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_altivec<4, 16>(pix3+8, stride_pix1, pix4+8, stride_pix2)
		+ satd_8x16_altivec(pix5, stride_pix1, pix6, stride_pix2)
		+ satd_altivec<4, 16>(pix5+8, stride_pix1, pix6+8, stride_pix2)
		+ satd_8x16_altivec(pix7, stride_pix1, pix8, stride_pix2)
		+ satd_altivec<4, 16>(pix7+8, stride_pix1, pix8+8, stride_pix2);
    return satd;
}
	
template<>
int satd_altivec<16, 4>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    return satd_16x4_altivec(pix1, stride_pix1, pix2, stride_pix2);
}

template<>
int satd_altivec<16, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    return satd_16x8_altivec(pix1, stride_pix1, pix2, stride_pix2);
}

template<>
int satd_altivec<16, 12>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x4_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x8_altivec(pix1+4*stride_pix1, stride_pix1, pix2+4*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<16, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    return satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2);
}

template<>
int satd_altivec<16, 24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x8_altivec(pix1+16*stride_pix1, stride_pix1, pix2+16*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<16, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x16_altivec(pix1+16*stride_pix1, stride_pix1, pix2+16*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<16, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x16_altivec(pix1+16*stride_pix1, stride_pix1, pix2+16*stride_pix2, stride_pix2)
		+ satd_16x16_altivec(pix1+32*stride_pix1, stride_pix1, pix2+32*stride_pix2, stride_pix2)
		+ satd_16x16_altivec(pix1+48*stride_pix1, stride_pix1, pix2+48*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<24, 4>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x4_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x4_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<24, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x8_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<24, 12>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 8*stride_pix1;
    const pixel *pix4 = pix2 + 8*stride_pix2;
    satd = satd_16x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x8_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x4_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_8x4_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<24, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<24, 24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_altivec<24, 16>(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_altivec<24, 8>(pix1+16*stride_pix1, stride_pix1, pix2+16*stride_pix2, stride_pix2);
    return satd;
}

template<>
int satd_altivec<24, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_8x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_8x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<24, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    const pixel *pix5 = pix1 + 32*stride_pix1;
    const pixel *pix6 = pix2 + 32*stride_pix2;
    const pixel *pix7 = pix1 + 48*stride_pix1;
    const pixel *pix8 = pix2 + 48*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_8x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
		+ satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_8x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
		+ satd_16x16_altivec(pix5, stride_pix1, pix6, stride_pix2)
		+ satd_8x16_altivec(pix5+16, stride_pix1, pix6+16, stride_pix2)
		+ satd_16x16_altivec(pix7, stride_pix1, pix8, stride_pix2)
		+ satd_8x16_altivec(pix7+16, stride_pix1, pix8+16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<32, 4>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x4_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x4_altivec(pix1 + 16, stride_pix1, pix2 + 16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<32, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x8_altivec(pix1 + 16, stride_pix1, pix2 + 16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<32, 12>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 8*stride_pix1;
    const pixel *pix4 = pix2 + 8*stride_pix2;
    satd = satd_16x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x8_altivec(pix1 + 16, stride_pix1, pix2 + 16, stride_pix2)
		+ satd_16x4_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_16x4_altivec(pix3 + 16, stride_pix1, pix4 + 16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<32, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x16_altivec(pix1 + 16, stride_pix1, pix2 + 16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<32, 24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x16_altivec(pix1 + 16, stride_pix1, pix2 + 16, stride_pix2)
		+ satd_16x8_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_16x8_altivec(pix3 + 16, stride_pix1, pix4 + 16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<32, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x16_altivec(pix1 + 16, stride_pix1, pix2 + 16, stride_pix2)
		+ satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_16x16_altivec(pix3 + 16, stride_pix1, pix4 + 16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<32, 48>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    const pixel *pix5 = pix1 + 32*stride_pix1;
    const pixel *pix6 = pix2 + 32*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
		+ satd_16x16_altivec(pix1 + 16, stride_pix1, pix2 + 16, stride_pix2)
		+ satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_16x16_altivec(pix3 + 16, stride_pix1, pix4 + 16, stride_pix2)
		+ satd_16x16_altivec(pix5, stride_pix1, pix6, stride_pix2)
		+ satd_16x16_altivec(pix5+16, stride_pix1, pix6+16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<32, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    const pixel *pix5 = pix1 + 32*stride_pix1;
    const pixel *pix6 = pix2 + 32*stride_pix2;
    const pixel *pix7 = pix1 + 48*stride_pix1;
    const pixel *pix8 = pix2 + 48*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
		+ satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
		+ satd_16x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
		+ satd_16x16_altivec(pix5, stride_pix1, pix6, stride_pix2)
		+ satd_16x16_altivec(pix5+16, stride_pix1, pix6+16, stride_pix2)
		+ satd_16x16_altivec(pix7, stride_pix1, pix8, stride_pix2)
		+ satd_16x16_altivec(pix7+16, stride_pix1, pix8+16, stride_pix2);
    return satd;
}

template<>
int satd_altivec<48, 4>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x4_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x4_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x4_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<48, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x8_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x8_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<48, 12>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 8*stride_pix1;
    const pixel *pix4 = pix2 + 8*stride_pix2;
    satd = satd_16x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x8_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x8_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2)
              +satd_16x4_altivec(pix3, stride_pix1, pix4, stride_pix2)
              + satd_16x4_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
              + satd_16x4_altivec(pix3+32, stride_pix1, pix4+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<48, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x16_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<48, 24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 8*stride_pix1;
    const pixel *pix4 = pix2 + 8*stride_pix2;
    satd = satd_16x8_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x8_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x8_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2)
              +satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
              + satd_16x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
              + satd_16x16_altivec(pix3+32, stride_pix1, pix4+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<48, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x16_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2)
              +satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
              + satd_16x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
              + satd_16x16_altivec(pix3+32, stride_pix1, pix4+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<48, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    const pixel *pix5 = pix1 + 32*stride_pix1;
    const pixel *pix6 = pix2 + 32*stride_pix2;
    const pixel *pix7 = pix1 + 48*stride_pix1;
    const pixel *pix8 = pix2 + 48*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x16_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2)
              +satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
              + satd_16x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
              + satd_16x16_altivec(pix3+32, stride_pix1, pix4+32, stride_pix2)
              +satd_16x16_altivec(pix5, stride_pix1, pix6, stride_pix2)
              + satd_16x16_altivec(pix5+16, stride_pix1, pix6+16, stride_pix2)
              + satd_16x16_altivec(pix5+32, stride_pix1, pix6+32, stride_pix2)
              +satd_16x16_altivec(pix7, stride_pix1, pix8, stride_pix2)
              + satd_16x16_altivec(pix7+16, stride_pix1,pix8+16, stride_pix2)
              + satd_16x16_altivec(pix7+32, stride_pix1, pix8+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<64, 4>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_altivec<32, 4>(pix1, stride_pix1, pix2, stride_pix2)
              + satd_altivec<32, 4>(pix1+32, stride_pix1, pix2+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<64, 8>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_altivec<32, 8>(pix1, stride_pix1, pix2, stride_pix2)
              + satd_altivec<32, 8>(pix1+32, stride_pix1, pix2+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<64, 12>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_altivec<32, 12>(pix1, stride_pix1, pix2, stride_pix2)
              + satd_altivec<32, 12>(pix1+32, stride_pix1, pix2+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<64, 16>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x16_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2)
              + satd_16x16_altivec(pix1+48, stride_pix1, pix2+48, stride_pix2);
    return satd;
}

template<>
int satd_altivec<64, 24>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    satd = satd_altivec<32, 24>(pix1, stride_pix1, pix2, stride_pix2)
              + satd_altivec<32, 24>(pix1+32, stride_pix1, pix2+32, stride_pix2);
    return satd;
}

template<>
int satd_altivec<64, 32>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x16_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2)
              + satd_16x16_altivec(pix1+48, stride_pix1, pix2+48, stride_pix2)
              + satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
              + satd_16x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
              + satd_16x16_altivec(pix3+32, stride_pix1, pix4+32, stride_pix2)
              + satd_16x16_altivec(pix3+48, stride_pix1, pix4+48, stride_pix2);
    return satd;
}

template<>
int satd_altivec<64, 48>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    const pixel *pix5 = pix1 + 32*stride_pix1;
    const pixel *pix6 = pix2 + 32*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x16_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2)
              + satd_16x16_altivec(pix1+48, stride_pix1, pix2+48, stride_pix2)
              + satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
              + satd_16x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
              + satd_16x16_altivec(pix3+32, stride_pix1, pix4+32, stride_pix2)
              + satd_16x16_altivec(pix3+48, stride_pix1, pix4+48, stride_pix2)
              + satd_16x16_altivec(pix5, stride_pix1, pix6, stride_pix2)
              + satd_16x16_altivec(pix5+16, stride_pix1, pix6+16, stride_pix2)
              + satd_16x16_altivec(pix5+32, stride_pix1, pix6+32, stride_pix2)
              + satd_16x16_altivec(pix5+48, stride_pix1, pix6+48, stride_pix2);
    return satd;
}

template<>
int satd_altivec<64, 64>(const pixel* pix1, intptr_t stride_pix1, const pixel* pix2, intptr_t stride_pix2)
{
    int satd = 0;
    const pixel *pix3 = pix1 + 16*stride_pix1;
    const pixel *pix4 = pix2 + 16*stride_pix2;
    const pixel *pix5 = pix1 + 32*stride_pix1;
    const pixel *pix6 = pix2 + 32*stride_pix2;
    const pixel *pix7 = pix1 + 48*stride_pix1;
    const pixel *pix8 = pix2 + 48*stride_pix2;
    satd = satd_16x16_altivec(pix1, stride_pix1, pix2, stride_pix2)
              + satd_16x16_altivec(pix1+16, stride_pix1, pix2+16, stride_pix2)
              + satd_16x16_altivec(pix1+32, stride_pix1, pix2+32, stride_pix2)
              + satd_16x16_altivec(pix1+48, stride_pix1, pix2+48, stride_pix2)
              + satd_16x16_altivec(pix3, stride_pix1, pix4, stride_pix2)
              + satd_16x16_altivec(pix3+16, stride_pix1, pix4+16, stride_pix2)
              + satd_16x16_altivec(pix3+32, stride_pix1, pix4+32, stride_pix2)
              + satd_16x16_altivec(pix3+48, stride_pix1, pix4+48, stride_pix2)
              + satd_16x16_altivec(pix5, stride_pix1, pix6, stride_pix2)
              + satd_16x16_altivec(pix5+16, stride_pix1, pix6+16, stride_pix2)
              + satd_16x16_altivec(pix5+32, stride_pix1, pix6+32, stride_pix2)
              + satd_16x16_altivec(pix5+48, stride_pix1, pix6+48, stride_pix2)
              + satd_16x16_altivec(pix7, stride_pix1, pix8, stride_pix2)
              + satd_16x16_altivec(pix7+16, stride_pix1, pix8+16, stride_pix2)
              + satd_16x16_altivec(pix7+32, stride_pix1, pix8+32, stride_pix2)
              + satd_16x16_altivec(pix7+48, stride_pix1, pix8+48, stride_pix2);
    return satd;
}


/***********************************************************************
 * SA8D routines - altivec implementation
 **********************************************************************/
#define SA8D_1D_ALTIVEC( sa8d0v, sa8d1v, sa8d2v, sa8d3v,  \
                         sa8d4v, sa8d5v, sa8d6v, sa8d7v ) \
{                                                         \
    /* int    a0  =        SRC(0) + SRC(4) */             \
    vec_s16_t a0v = vec_add(sa8d0v, sa8d4v);              \
    /* int    a4  =        SRC(0) - SRC(4) */             \
    vec_s16_t a4v = vec_sub(sa8d0v, sa8d4v);              \
    /* int    a1  =        SRC(1) + SRC(5) */             \
    vec_s16_t a1v = vec_add(sa8d1v, sa8d5v);              \
    /* int    a5  =        SRC(1) - SRC(5) */             \
    vec_s16_t a5v = vec_sub(sa8d1v, sa8d5v);              \
    /* int    a2  =        SRC(2) + SRC(6) */             \
    vec_s16_t a2v = vec_add(sa8d2v, sa8d6v);              \
    /* int    a6  =        SRC(2) - SRC(6) */             \
    vec_s16_t a6v = vec_sub(sa8d2v, sa8d6v);              \
    /* int    a3  =        SRC(3) + SRC(7) */             \
    vec_s16_t a3v = vec_add(sa8d3v, sa8d7v);              \
    /* int    a7  =        SRC(3) - SRC(7) */             \
    vec_s16_t a7v = vec_sub(sa8d3v, sa8d7v);              \
                                                          \
    /* int    b0  =         a0 + a2  */                   \
    vec_s16_t b0v = vec_add(a0v, a2v);                    \
    /* int    b2  =         a0 - a2; */                   \
    vec_s16_t  b2v = vec_sub(a0v, a2v);                   \
    /* int    b1  =         a1 + a3; */                   \
    vec_s16_t b1v = vec_add(a1v, a3v);                    \
    /* int    b3  =         a1 - a3; */                   \
    vec_s16_t b3v = vec_sub(a1v, a3v);                    \
    /* int    b4  =         a4 + a6; */                   \
    vec_s16_t b4v = vec_add(a4v, a6v);                    \
    /* int    b6  =         a4 - a6; */                   \
    vec_s16_t b6v = vec_sub(a4v, a6v);                    \
    /* int    b5  =         a5 + a7; */                   \
    vec_s16_t b5v = vec_add(a5v, a7v);                    \
    /* int    b7  =         a5 - a7; */                   \
    vec_s16_t b7v = vec_sub(a5v, a7v);                    \
                                                          \
    /* DST(0,        b0 + b1) */                          \
    sa8d0v = vec_add(b0v, b1v);                           \
    /* DST(1,        b0 - b1) */                          \
    sa8d1v = vec_sub(b0v, b1v);                           \
    /* DST(2,        b2 + b3) */                          \
    sa8d2v = vec_add(b2v, b3v);                           \
    /* DST(3,        b2 - b3) */                          \
    sa8d3v = vec_sub(b2v, b3v);                           \
    /* DST(4,        b4 + b5) */                          \
    sa8d4v = vec_add(b4v, b5v);                           \
    /* DST(5,        b4 - b5) */                          \
    sa8d5v = vec_sub(b4v, b5v);                           \
    /* DST(6,        b6 + b7) */                          \
    sa8d6v = vec_add(b6v, b7v);                           \
    /* DST(7,        b6 - b7) */                          \
    sa8d7v = vec_sub(b6v, b7v);                           \
}

inline int sa8d_8x8_altivec(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2)
{
    ALIGN_VAR_16(int, sum);

    LOAD_ZERO;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diff0v, diff1v, diff2v, diff3v, diff4v, diff5v, diff6v, diff7v;
    vec_s16_t sa8d0v, sa8d1v, sa8d2v, sa8d3v, sa8d4v, sa8d5v, sa8d6v, sa8d7v;
	
    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff4v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff5v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff6v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff7v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;


    SA8D_1D_ALTIVEC(diff0v, diff1v, diff2v, diff3v,
                    diff4v, diff5v, diff6v, diff7v);
    VEC_TRANSPOSE_8(diff0v, diff1v, diff2v, diff3v,
                    diff4v, diff5v, diff6v, diff7v,
                    sa8d0v, sa8d1v, sa8d2v, sa8d3v,
                    sa8d4v, sa8d5v, sa8d6v, sa8d7v );
    SA8D_1D_ALTIVEC(sa8d0v, sa8d1v, sa8d2v, sa8d3v,
                    sa8d4v, sa8d5v, sa8d6v, sa8d7v );

    /* accumulation of the absolute value of all elements of the resulting bloc */
    vec_s16_t abs0v = vec_max( sa8d0v, vec_sub( zero_s16v, sa8d0v ) );
    vec_s16_t abs1v = vec_max( sa8d1v, vec_sub( zero_s16v, sa8d1v ) );
    vec_s16_t sum01v = vec_add(abs0v, abs1v);

    vec_s16_t abs2v = vec_max( sa8d2v, vec_sub( zero_s16v, sa8d2v ) );
    vec_s16_t abs3v = vec_max( sa8d3v, vec_sub( zero_s16v, sa8d3v ) );
    vec_s16_t sum23v = vec_add(abs2v, abs3v);

    vec_s16_t abs4v = vec_max( sa8d4v, vec_sub( zero_s16v, sa8d4v ) );
    vec_s16_t abs5v = vec_max( sa8d5v, vec_sub( zero_s16v, sa8d5v ) );
    vec_s16_t sum45v = vec_add(abs4v, abs5v);

    vec_s16_t abs6v = vec_max( sa8d6v, vec_sub( zero_s16v, sa8d6v ) );
    vec_s16_t abs7v = vec_max( sa8d7v, vec_sub( zero_s16v, sa8d7v ) );
    vec_s16_t sum67v = vec_add(abs6v, abs7v);

    vec_s16_t sum0123v = vec_add(sum01v, sum23v);
    vec_s16_t sum4567v = vec_add(sum45v, sum67v);

    vec_s32_t sumblocv;

    sumblocv = vec_sum4s(sum0123v, (vec_s32_t)zerov );
    //print_vec_s("sum0123v", &sum0123v);	
    //print_vec_i("sumblocv = vec_sum4s(sum0123v, 0 )", &sumblocv);	
    sumblocv = vec_sum4s(sum4567v, sumblocv );
    //print_vec_s("sum4567v", &sum4567v);	
    //print_vec_i("sumblocv = vec_sum4s(sum4567v, sumblocv )", &sumblocv);	
    sumblocv = vec_sums(sumblocv, (vec_s32_t)zerov );
    //print_vec_i("sumblocv=vec_sums(sumblocv,0 )", &sumblocv);	
    sumblocv = vec_splat(sumblocv, 3);
    //print_vec_i("sumblocv = vec_splat(sumblocv, 3)", &sumblocv);	
    vec_ste(sumblocv, 0, &sum);

    return (sum + 2) >> 2;
}


int sa8d_8x8_altivec(const int16_t* pix1, intptr_t i_pix1)
{
    int sum = 0;
    return ((sum+2)>>2);
}

inline int sa8d_8x16_altivec(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2)
{
    ALIGN_VAR_16(int, sum);
    ALIGN_VAR_16(int, sum1);

    LOAD_ZERO;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diff0v, diff1v, diff2v, diff3v, diff4v, diff5v, diff6v, diff7v;
    vec_s16_t sa8d0v, sa8d1v, sa8d2v, sa8d3v, sa8d4v, sa8d5v, sa8d6v, sa8d7v;
	
    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff4v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff5v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff6v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff7v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;


    SA8D_1D_ALTIVEC(diff0v, diff1v, diff2v, diff3v,
                    diff4v, diff5v, diff6v, diff7v);
    VEC_TRANSPOSE_8(diff0v, diff1v, diff2v, diff3v,
                    diff4v, diff5v, diff6v, diff7v,
                    sa8d0v, sa8d1v, sa8d2v, sa8d3v,
                    sa8d4v, sa8d5v, sa8d6v, sa8d7v );
    SA8D_1D_ALTIVEC(sa8d0v, sa8d1v, sa8d2v, sa8d3v,
                    sa8d4v, sa8d5v, sa8d6v, sa8d7v );

    /* accumulation of the absolute value of all elements of the resulting bloc */
    vec_s16_t abs0v = vec_max( sa8d0v, vec_sub( zero_s16v, sa8d0v ) );
    vec_s16_t abs1v = vec_max( sa8d1v, vec_sub( zero_s16v, sa8d1v ) );
    vec_s16_t sum01v = vec_add(abs0v, abs1v);

    vec_s16_t abs2v = vec_max( sa8d2v, vec_sub( zero_s16v, sa8d2v ) );
    vec_s16_t abs3v = vec_max( sa8d3v, vec_sub( zero_s16v, sa8d3v ) );
    vec_s16_t sum23v = vec_add(abs2v, abs3v);

    vec_s16_t abs4v = vec_max( sa8d4v, vec_sub( zero_s16v, sa8d4v ) );
    vec_s16_t abs5v = vec_max( sa8d5v, vec_sub( zero_s16v, sa8d5v ) );
    vec_s16_t sum45v = vec_add(abs4v, abs5v);

    vec_s16_t abs6v = vec_max( sa8d6v, vec_sub( zero_s16v, sa8d6v ) );
    vec_s16_t abs7v = vec_max( sa8d7v, vec_sub( zero_s16v, sa8d7v ) );
    vec_s16_t sum67v = vec_add(abs6v, abs7v);

    vec_s16_t sum0123v = vec_add(sum01v, sum23v);
    vec_s16_t sum4567v = vec_add(sum45v, sum67v);

    vec_s32_t sumblocv, sumblocv1;

    sumblocv = vec_sum4s(sum0123v, (vec_s32_t)zerov );
    sumblocv = vec_sum4s(sum4567v, sumblocv );
    sumblocv = vec_sums(sumblocv, (vec_s32_t)zerov );
    sumblocv = vec_splat(sumblocv, 3);
    vec_ste(sumblocv, 0, &sum);

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff0v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff1v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff2v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff3v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff4v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff5v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff6v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;

    pix1v = vec_u8_to_s16(vec_xl(0, pix1));
    pix2v = vec_u8_to_s16( vec_xl(0, pix2) );               
    diff7v = vec_sub( pix1v, pix2v );               
    pix1   += i_pix1;                               
    pix2   += i_pix2;


    SA8D_1D_ALTIVEC(diff0v, diff1v, diff2v, diff3v,
                    diff4v, diff5v, diff6v, diff7v);
    VEC_TRANSPOSE_8(diff0v, diff1v, diff2v, diff3v,
                    diff4v, diff5v, diff6v, diff7v,
                    sa8d0v, sa8d1v, sa8d2v, sa8d3v,
                    sa8d4v, sa8d5v, sa8d6v, sa8d7v );
    SA8D_1D_ALTIVEC(sa8d0v, sa8d1v, sa8d2v, sa8d3v,
                    sa8d4v, sa8d5v, sa8d6v, sa8d7v );

    /* accumulation of the absolute value of all elements of the resulting bloc */
    abs0v = vec_max( sa8d0v, vec_sub( zero_s16v, sa8d0v ) );
    abs1v = vec_max( sa8d1v, vec_sub( zero_s16v, sa8d1v ) );
    sum01v = vec_add(abs0v, abs1v);

    abs2v = vec_max( sa8d2v, vec_sub( zero_s16v, sa8d2v ) );
    abs3v = vec_max( sa8d3v, vec_sub( zero_s16v, sa8d3v ) );
    sum23v = vec_add(abs2v, abs3v);

    abs4v = vec_max( sa8d4v, vec_sub( zero_s16v, sa8d4v ) );
    abs5v = vec_max( sa8d5v, vec_sub( zero_s16v, sa8d5v ) );
    sum45v = vec_add(abs4v, abs5v);

    abs6v = vec_max( sa8d6v, vec_sub( zero_s16v, sa8d6v ) );
    abs7v = vec_max( sa8d7v, vec_sub( zero_s16v, sa8d7v ) );
    sum67v = vec_add(abs6v, abs7v);

    sum0123v = vec_add(sum01v, sum23v);
    sum4567v = vec_add(sum45v, sum67v);

    sumblocv1 = vec_sum4s(sum0123v, (vec_s32_t)zerov );
    sumblocv1 = vec_sum4s(sum4567v, sumblocv1 );
    sumblocv1 = vec_sums(sumblocv1, (vec_s32_t)zerov );
    sumblocv1 = vec_splat(sumblocv1, 3);
    vec_ste(sumblocv1, 0, &sum1);

    sum = (sum + 2) >> 2;
    sum1 = (sum1 + 2) >> 2;
    sum += sum1;
    return (sum);
}

inline int sa8d_16x8_altivec(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2)
{
    ALIGN_VAR_16(int, sumh);
    ALIGN_VAR_16(int, suml);

    LOAD_ZERO;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diffh0v, diffh1v, diffh2v, diffh3v,
              diffh4v, diffh5v, diffh6v, diffh7v;
    vec_s16_t diffl0v, diffl1v, diffl2v, diffl3v,
              diffl4v, diffl5v, diffl6v, diffl7v;
    vec_s16_t sa8dh0v, sa8dh1v, sa8dh2v, sa8dh3v, sa8dh4v, sa8dh5v, sa8dh6v, sa8dh7v;
    vec_s16_t sa8dl0v, sa8dl1v, sa8dl2v, sa8dl3v, sa8dl4v, sa8dl5v, sa8dl6v, sa8dl7v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v;
	
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh0v,diffl0v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh1v, diffl1v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh2v, diffl2v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh3v, diffl3v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh4v, diffl4v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh5v, diffl5v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh6v, diffl6v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh7v, diffl7v);

    SA8D_1D_ALTIVEC(diffh0v, diffh1v, diffh2v, diffh3v, diffh4v, diffh5v, diffh6v, diffh7v);
    VEC_TRANSPOSE_8(diffh0v, diffh1v, diffh2v, diffh3v, diffh4v, diffh5v, diffh6v, diffh7v,
                    sa8dh0v, sa8dh1v, sa8dh2v, sa8dh3v, sa8dh4v, sa8dh5v, sa8dh6v, sa8dh7v );
    SA8D_1D_ALTIVEC(sa8dh0v, sa8dh1v, sa8dh2v, sa8dh3v, sa8dh4v, sa8dh5v, sa8dh6v, sa8dh7v);

    SA8D_1D_ALTIVEC(diffl0v, diffl1v, diffl2v, diffl3v, diffl4v, diffl5v, diffl6v, diffl7v);
    VEC_TRANSPOSE_8(diffl0v, diffl1v, diffl2v, diffl3v, diffl4v, diffl5v, diffl6v, diffl7v,
                    sa8dl0v, sa8dl1v, sa8dl2v, sa8dl3v, sa8dl4v, sa8dl5v, sa8dl6v, sa8dl7v );
    SA8D_1D_ALTIVEC(sa8dl0v, sa8dl1v, sa8dl2v, sa8dl3v, sa8dl4v, sa8dl5v, sa8dl6v, sa8dl7v);

    /* accumulation of the absolute value of all elements of the resulting bloc */
    sa8dh0v = vec_max( sa8dh0v, vec_sub( zero_s16v, sa8dh0v ) );
    sa8dh1v = vec_max( sa8dh1v, vec_sub( zero_s16v, sa8dh1v ) );
    vec_s16_t sumh01v = vec_add(sa8dh0v, sa8dh1v);

    sa8dh2v = vec_max( sa8dh2v, vec_sub( zero_s16v, sa8dh2v ) );
    sa8dh3v = vec_max( sa8dh3v, vec_sub( zero_s16v, sa8dh3v ) );
    vec_s16_t sumh23v = vec_add(sa8dh2v, sa8dh3v);

    sa8dh4v = vec_max( sa8dh4v, vec_sub( zero_s16v, sa8dh4v ) );
    sa8dh5v = vec_max( sa8dh5v, vec_sub( zero_s16v, sa8dh5v ) );
    vec_s16_t sumh45v = vec_add(sa8dh4v, sa8dh5v);

    sa8dh6v = vec_max( sa8dh6v, vec_sub( zero_s16v, sa8dh6v ) );
    sa8dh7v = vec_max( sa8dh7v, vec_sub( zero_s16v, sa8dh7v ) );
    vec_s16_t sumh67v = vec_add(sa8dh6v, sa8dh7v);

    vec_s16_t sumh0123v = vec_add(sumh01v, sumh23v);
    vec_s16_t sumh4567v = vec_add(sumh45v, sumh67v);

    vec_s32_t sumblocv_h;

    sumblocv_h = vec_sum4s(sumh0123v, (vec_s32_t)zerov );
    //print_vec_s("sum0123v", &sum0123v);	
    //print_vec_i("sumblocv = vec_sum4s(sum0123v, 0 )", &sumblocv);	
    sumblocv_h = vec_sum4s(sumh4567v, sumblocv_h );
    //print_vec_s("sum4567v", &sum4567v);	
    //print_vec_i("sumblocv = vec_sum4s(sum4567v, sumblocv )", &sumblocv);	
    sumblocv_h = vec_sums(sumblocv_h, (vec_s32_t)zerov );
    //print_vec_i("sumblocv=vec_sums(sumblocv,0 )", &sumblocv);	
    sumblocv_h = vec_splat(sumblocv_h, 3);
    //print_vec_i("sumblocv = vec_splat(sumblocv, 3)", &sumblocv);	
    vec_ste(sumblocv_h, 0, &sumh);

    sa8dl0v = vec_max( sa8dl0v, vec_sub( zero_s16v, sa8dl0v ) );
    sa8dl1v = vec_max( sa8dl1v, vec_sub( zero_s16v, sa8dl1v ) );
    vec_s16_t suml01v = vec_add(sa8dl0v, sa8dl1v);

    sa8dl2v = vec_max( sa8dl2v, vec_sub( zero_s16v, sa8dl2v ) );
    sa8dl3v = vec_max( sa8dl3v, vec_sub( zero_s16v, sa8dl3v ) );
    vec_s16_t suml23v = vec_add(sa8dl2v, sa8dl3v);

    sa8dl4v = vec_max( sa8dl4v, vec_sub( zero_s16v, sa8dl4v ) );
    sa8dl5v = vec_max( sa8dl5v, vec_sub( zero_s16v, sa8dl5v ) );
    vec_s16_t suml45v = vec_add(sa8dl4v, sa8dl5v);

    sa8dl6v = vec_max( sa8dl6v, vec_sub( zero_s16v, sa8dl6v ) );
    sa8dl7v = vec_max( sa8dl7v, vec_sub( zero_s16v, sa8dl7v ) );
    vec_s16_t suml67v = vec_add(sa8dl6v, sa8dl7v);

    vec_s16_t suml0123v = vec_add(suml01v, suml23v);
    vec_s16_t suml4567v = vec_add(suml45v, suml67v);

    vec_s32_t sumblocv_l;

    sumblocv_l = vec_sum4s(suml0123v, (vec_s32_t)zerov );
    //print_vec_s("sum0123v", &sum0123v);	
    //print_vec_i("sumblocv = vec_sum4s(sum0123v, 0 )", &sumblocv);	
    sumblocv_l = vec_sum4s(suml4567v, sumblocv_l );
    //print_vec_s("sum4567v", &sum4567v);	
    //print_vec_i("sumblocv = vec_sum4s(sum4567v, sumblocv )", &sumblocv);	
    sumblocv_l = vec_sums(sumblocv_l, (vec_s32_t)zerov );
    //print_vec_i("sumblocv=vec_sums(sumblocv,0 )", &sumblocv);	
    sumblocv_l = vec_splat(sumblocv_l, 3);
    //print_vec_i("sumblocv = vec_splat(sumblocv, 3)", &sumblocv);	
    vec_ste(sumblocv_l, 0, &suml);

    sumh = (sumh + 2) >> 2;
    suml= (suml + 2) >> 2;
    return (sumh+suml);
}

inline int sa8d_16x16_altivec(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2)
{
    ALIGN_VAR_16(int, sumh0);
    ALIGN_VAR_16(int, suml0);

    ALIGN_VAR_16(int, sumh1);
    ALIGN_VAR_16(int, suml1);

    ALIGN_VAR_16(int, sum);
	
    LOAD_ZERO;
    vec_s16_t pix1v, pix2v;
    vec_s16_t diffh0v, diffh1v, diffh2v, diffh3v,
              diffh4v, diffh5v, diffh6v, diffh7v;
    vec_s16_t diffl0v, diffl1v, diffl2v, diffl3v,
              diffl4v, diffl5v, diffl6v, diffl7v;
    vec_s16_t sa8dh0v, sa8dh1v, sa8dh2v, sa8dh3v, sa8dh4v, sa8dh5v, sa8dh6v, sa8dh7v;
    vec_s16_t sa8dl0v, sa8dl1v, sa8dl2v, sa8dl3v, sa8dl4v, sa8dl5v, sa8dl6v, sa8dl7v;
    vec_s16_t temp0v, temp1v, temp2v, temp3v;
	
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh0v,diffl0v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh1v, diffl1v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh2v, diffl2v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh3v, diffl3v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh4v, diffl4v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh5v, diffl5v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh6v, diffl6v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh7v, diffl7v);

    SA8D_1D_ALTIVEC(diffh0v, diffh1v, diffh2v, diffh3v, diffh4v, diffh5v, diffh6v, diffh7v);
    VEC_TRANSPOSE_8(diffh0v, diffh1v, diffh2v, diffh3v, diffh4v, diffh5v, diffh6v, diffh7v,
                    sa8dh0v, sa8dh1v, sa8dh2v, sa8dh3v, sa8dh4v, sa8dh5v, sa8dh6v, sa8dh7v );
    SA8D_1D_ALTIVEC(sa8dh0v, sa8dh1v, sa8dh2v, sa8dh3v, sa8dh4v, sa8dh5v, sa8dh6v, sa8dh7v);

    SA8D_1D_ALTIVEC(diffl0v, diffl1v, diffl2v, diffl3v, diffl4v, diffl5v, diffl6v, diffl7v);
    VEC_TRANSPOSE_8(diffl0v, diffl1v, diffl2v, diffl3v, diffl4v, diffl5v, diffl6v, diffl7v,
                    sa8dl0v, sa8dl1v, sa8dl2v, sa8dl3v, sa8dl4v, sa8dl5v, sa8dl6v, sa8dl7v );
    SA8D_1D_ALTIVEC(sa8dl0v, sa8dl1v, sa8dl2v, sa8dl3v, sa8dl4v, sa8dl5v, sa8dl6v, sa8dl7v);

    /* accumulation of the absolute value of all elements of the resulting bloc */
    sa8dh0v = vec_max( sa8dh0v, vec_sub( zero_s16v, sa8dh0v ) );
    sa8dh1v = vec_max( sa8dh1v, vec_sub( zero_s16v, sa8dh1v ) );
    vec_s16_t sumh01v = vec_add(sa8dh0v, sa8dh1v);

    sa8dh2v = vec_max( sa8dh2v, vec_sub( zero_s16v, sa8dh2v ) );
    sa8dh3v = vec_max( sa8dh3v, vec_sub( zero_s16v, sa8dh3v ) );
    vec_s16_t sumh23v = vec_add(sa8dh2v, sa8dh3v);

    sa8dh4v = vec_max( sa8dh4v, vec_sub( zero_s16v, sa8dh4v ) );
    sa8dh5v = vec_max( sa8dh5v, vec_sub( zero_s16v, sa8dh5v ) );
    vec_s16_t sumh45v = vec_add(sa8dh4v, sa8dh5v);

    sa8dh6v = vec_max( sa8dh6v, vec_sub( zero_s16v, sa8dh6v ) );
    sa8dh7v = vec_max( sa8dh7v, vec_sub( zero_s16v, sa8dh7v ) );
    vec_s16_t sumh67v = vec_add(sa8dh6v, sa8dh7v);

    vec_s16_t sumh0123v = vec_add(sumh01v, sumh23v);
    vec_s16_t sumh4567v = vec_add(sumh45v, sumh67v);

    vec_s32_t sumblocv_h0;

    sumblocv_h0 = vec_sum4s(sumh0123v, (vec_s32_t)zerov );
    sumblocv_h0 = vec_sum4s(sumh4567v, sumblocv_h0 );
    sumblocv_h0 = vec_sums(sumblocv_h0, (vec_s32_t)zerov );
    sumblocv_h0 = vec_splat(sumblocv_h0, 3);
    vec_ste(sumblocv_h0, 0, &sumh0);

    sa8dl0v = vec_max( sa8dl0v, vec_sub( zero_s16v, sa8dl0v ) );
    sa8dl1v = vec_max( sa8dl1v, vec_sub( zero_s16v, sa8dl1v ) );
    vec_s16_t suml01v = vec_add(sa8dl0v, sa8dl1v);

    sa8dl2v = vec_max( sa8dl2v, vec_sub( zero_s16v, sa8dl2v ) );
    sa8dl3v = vec_max( sa8dl3v, vec_sub( zero_s16v, sa8dl3v ) );
    vec_s16_t suml23v = vec_add(sa8dl2v, sa8dl3v);

    sa8dl4v = vec_max( sa8dl4v, vec_sub( zero_s16v, sa8dl4v ) );
    sa8dl5v = vec_max( sa8dl5v, vec_sub( zero_s16v, sa8dl5v ) );
    vec_s16_t suml45v = vec_add(sa8dl4v, sa8dl5v);

    sa8dl6v = vec_max( sa8dl6v, vec_sub( zero_s16v, sa8dl6v ) );
    sa8dl7v = vec_max( sa8dl7v, vec_sub( zero_s16v, sa8dl7v ) );
    vec_s16_t suml67v = vec_add(sa8dl6v, sa8dl7v);

    vec_s16_t suml0123v = vec_add(suml01v, suml23v);
    vec_s16_t suml4567v = vec_add(suml45v, suml67v);

    vec_s32_t sumblocv_l0;

    sumblocv_l0 = vec_sum4s(suml0123v, (vec_s32_t)zerov );
    sumblocv_l0 = vec_sum4s(suml4567v, sumblocv_l0 );
    sumblocv_l0 = vec_sums(sumblocv_l0, (vec_s32_t)zerov );
    sumblocv_l0 = vec_splat(sumblocv_l0, 3);
    vec_ste(sumblocv_l0, 0, &suml0);

    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh0v,diffl0v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh1v, diffl1v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh2v, diffl2v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh3v, diffl3v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh4v, diffl4v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh5v, diffl5v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh6v, diffl6v);
    VEC_DIFF_S16(pix1,i_pix1,pix2,i_pix2,diffh7v, diffl7v);

    SA8D_1D_ALTIVEC(diffh0v, diffh1v, diffh2v, diffh3v, diffh4v, diffh5v, diffh6v, diffh7v);
    VEC_TRANSPOSE_8(diffh0v, diffh1v, diffh2v, diffh3v, diffh4v, diffh5v, diffh6v, diffh7v,
                    sa8dh0v, sa8dh1v, sa8dh2v, sa8dh3v, sa8dh4v, sa8dh5v, sa8dh6v, sa8dh7v );
    SA8D_1D_ALTIVEC(sa8dh0v, sa8dh1v, sa8dh2v, sa8dh3v, sa8dh4v, sa8dh5v, sa8dh6v, sa8dh7v);

    SA8D_1D_ALTIVEC(diffl0v, diffl1v, diffl2v, diffl3v, diffl4v, diffl5v, diffl6v, diffl7v);
    VEC_TRANSPOSE_8(diffl0v, diffl1v, diffl2v, diffl3v, diffl4v, diffl5v, diffl6v, diffl7v,
                    sa8dl0v, sa8dl1v, sa8dl2v, sa8dl3v, sa8dl4v, sa8dl5v, sa8dl6v, sa8dl7v );
    SA8D_1D_ALTIVEC(sa8dl0v, sa8dl1v, sa8dl2v, sa8dl3v, sa8dl4v, sa8dl5v, sa8dl6v, sa8dl7v);

    /* accumulation of the absolute value of all elements of the resulting bloc */
    sa8dh0v = vec_max( sa8dh0v, vec_sub( zero_s16v, sa8dh0v ) );
    sa8dh1v = vec_max( sa8dh1v, vec_sub( zero_s16v, sa8dh1v ) );
    sumh01v = vec_add(sa8dh0v, sa8dh1v);

    sa8dh2v = vec_max( sa8dh2v, vec_sub( zero_s16v, sa8dh2v ) );
    sa8dh3v = vec_max( sa8dh3v, vec_sub( zero_s16v, sa8dh3v ) );
    sumh23v = vec_add(sa8dh2v, sa8dh3v);

    sa8dh4v = vec_max( sa8dh4v, vec_sub( zero_s16v, sa8dh4v ) );
    sa8dh5v = vec_max( sa8dh5v, vec_sub( zero_s16v, sa8dh5v ) );
    sumh45v = vec_add(sa8dh4v, sa8dh5v);

    sa8dh6v = vec_max( sa8dh6v, vec_sub( zero_s16v, sa8dh6v ) );
    sa8dh7v = vec_max( sa8dh7v, vec_sub( zero_s16v, sa8dh7v ) );
    sumh67v = vec_add(sa8dh6v, sa8dh7v);

    sumh0123v = vec_add(sumh01v, sumh23v);
    sumh4567v = vec_add(sumh45v, sumh67v);

    vec_s32_t sumblocv_h1;

    sumblocv_h1 = vec_sum4s(sumh0123v, (vec_s32_t)zerov );
    sumblocv_h1 = vec_sum4s(sumh4567v, sumblocv_h1 );
    sumblocv_h1 = vec_sums(sumblocv_h1, (vec_s32_t)zerov );
    sumblocv_h1 = vec_splat(sumblocv_h1, 3);
    vec_ste(sumblocv_h1, 0, &sumh1);

    sa8dl0v = vec_max( sa8dl0v, vec_sub( zero_s16v, sa8dl0v ) );
    sa8dl1v = vec_max( sa8dl1v, vec_sub( zero_s16v, sa8dl1v ) );
    suml01v = vec_add(sa8dl0v, sa8dl1v);

    sa8dl2v = vec_max( sa8dl2v, vec_sub( zero_s16v, sa8dl2v ) );
    sa8dl3v = vec_max( sa8dl3v, vec_sub( zero_s16v, sa8dl3v ) );
    suml23v = vec_add(sa8dl2v, sa8dl3v);

    sa8dl4v = vec_max( sa8dl4v, vec_sub( zero_s16v, sa8dl4v ) );
    sa8dl5v = vec_max( sa8dl5v, vec_sub( zero_s16v, sa8dl5v ) );
    suml45v = vec_add(sa8dl4v, sa8dl5v);

    sa8dl6v = vec_max( sa8dl6v, vec_sub( zero_s16v, sa8dl6v ) );
    sa8dl7v = vec_max( sa8dl7v, vec_sub( zero_s16v, sa8dl7v ) );
    suml67v = vec_add(sa8dl6v, sa8dl7v);

    suml0123v = vec_add(suml01v, suml23v);
    suml4567v = vec_add(suml45v, suml67v);

    vec_s32_t sumblocv_l1;

    sumblocv_l1 = vec_sum4s(suml0123v, (vec_s32_t)zerov );
    sumblocv_l1 = vec_sum4s(suml4567v, sumblocv_l1 );
    sumblocv_l1 = vec_sums(sumblocv_l1, (vec_s32_t)zerov );
    sumblocv_l1 = vec_splat(sumblocv_l1, 3);
    vec_ste(sumblocv_l1, 0, &suml1);

    sum = (sumh0+suml0+sumh1+suml1 + 2) >>2;
    return (sum );
}

int sa8d_16x32_altivec(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2)
{
    ALIGN_VAR_16(int, sum);
    sum =  sa8d_16x16_altivec(pix1, i_pix1, pix2, i_pix2) 
		+ sa8d_16x16_altivec(pix1+16*i_pix1, i_pix1, pix2+16*i_pix2, i_pix2);
    return sum;	
}

int sa8d_32x32_altivec(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2)
{
    ALIGN_VAR_16(int, sum);
    int offset1, offset2;
    offset1 = 16*i_pix1;
    offset2 = 16*i_pix2;	
    sum =  sa8d_16x16_altivec(pix1, i_pix1, pix2, i_pix2) 
		+ sa8d_16x16_altivec(pix1+16, i_pix1, pix2+16, i_pix2)
		+ sa8d_16x16_altivec(pix1+offset1, i_pix1, pix2+offset2, i_pix2)
		+ sa8d_16x16_altivec(pix1+16+offset1, i_pix1, pix2+16+offset2, i_pix2);
    return sum;	
}

int sa8d_32x64_altivec(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2)
{
    ALIGN_VAR_16(int, sum);
    int offset1, offset2;
    offset1 = 16*i_pix1;
    offset2 = 16*i_pix2;	
    sum =  sa8d_16x16_altivec(pix1, i_pix1, pix2, i_pix2) 
		+ sa8d_16x16_altivec(pix1+16, i_pix1, pix2+16, i_pix2)
		+ sa8d_16x16_altivec(pix1+offset1, i_pix1, pix2+offset2, i_pix2)
		+ sa8d_16x16_altivec(pix1+16+offset1, i_pix1, pix2+16+offset2, i_pix2)
		+ sa8d_16x16_altivec(pix1+32*i_pix1, i_pix1, pix2+32*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+16+32*i_pix1, i_pix1, pix2+16+32*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+48*i_pix1, i_pix1, pix2+48*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+16+48*i_pix1, i_pix1, pix2+16+48*i_pix2, i_pix2);
    return sum;	
}

int sa8d_64x64_altivec(const pixel* pix1, intptr_t i_pix1, const pixel* pix2, intptr_t i_pix2)
{
    ALIGN_VAR_16(int, sum);
    int offset1, offset2;
    offset1 = 16*i_pix1;
    offset2 = 16*i_pix2;	
    sum =  sa8d_16x16_altivec(pix1, i_pix1, pix2, i_pix2) 
		+ sa8d_16x16_altivec(pix1+16, i_pix1, pix2+16, i_pix2)
		+ sa8d_16x16_altivec(pix1+32, i_pix1, pix2+32, i_pix2)
		+ sa8d_16x16_altivec(pix1+48, i_pix1, pix2+48, i_pix2)
		+ sa8d_16x16_altivec(pix1+offset1, i_pix1, pix2+offset2, i_pix2)
		+ sa8d_16x16_altivec(pix1+16+offset1, i_pix1, pix2+16+offset2, i_pix2)
		+ sa8d_16x16_altivec(pix1+32+offset1, i_pix1, pix2+32+offset2, i_pix2)
		+ sa8d_16x16_altivec(pix1+48+offset1, i_pix1, pix2+48+offset2, i_pix2)
		+ sa8d_16x16_altivec(pix1+32*i_pix1, i_pix1, pix2+32*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+16+32*i_pix1, i_pix1, pix2+16+32*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+32+32*i_pix1, i_pix1, pix2+32+32*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+48+32*i_pix1, i_pix1, pix2+48+32*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+48*i_pix1, i_pix1, pix2+48*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+16+48*i_pix1, i_pix1, pix2+16+48*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+32+48*i_pix1, i_pix1, pix2+32+48*i_pix2, i_pix2)
		+ sa8d_16x16_altivec(pix1+48+48*i_pix1, i_pix1, pix2+48+48*i_pix2, i_pix2);
    return sum;	
}

/* Initialize entries for pixel functions defined in this file */
void setupPixelPrimitives_altivec(EncoderPrimitives &p)
{
#define LUMA_PU(W, H) \
    if (W<=16) { \
        p.pu[LUMA_ ## W ## x ## H].sad = sad16_altivec<W, H>; \
        p.pu[LUMA_ ## W ## x ## H].sad_x3 = sad16_x3_altivec<W, H>; \
        p.pu[LUMA_ ## W ## x ## H].sad_x4 = sad16_x4_altivec<W, H>; \
    } \
    else { \
       p.pu[LUMA_ ## W ## x ## H].sad = sad_altivec<W, H>; \
       p.pu[LUMA_ ## W ## x ## H].sad_x3 = sad_x3_altivec<W, H>; \
       p.pu[LUMA_ ## W ## x ## H].sad_x4 = sad_x4_altivec<W, H>; \
    }

    LUMA_PU(4, 4);
    LUMA_PU(8, 8);
    LUMA_PU(16, 16);
    LUMA_PU(32, 32);
    LUMA_PU(64, 64);
    LUMA_PU(4, 8);
    LUMA_PU(8, 4);
    LUMA_PU(16,  8);
    LUMA_PU(8, 16);
    LUMA_PU(16, 12);
    LUMA_PU(12, 16);
    LUMA_PU(16,  4);
    LUMA_PU(4, 16);
    LUMA_PU(32, 16);
    LUMA_PU(16, 32);
    LUMA_PU(32, 24);
    LUMA_PU(24, 32);
    LUMA_PU(32,  8);
    LUMA_PU(8, 32);
    LUMA_PU(64, 32);
    LUMA_PU(32, 64);
    LUMA_PU(64, 48);
    LUMA_PU(48, 64);
    LUMA_PU(64, 16);
    LUMA_PU(16, 64);

    p.pu[LUMA_4x4].satd   = satd_4x4_altivec;//satd_4x4;
    p.pu[LUMA_8x8].satd   = satd_8x8_altivec;//satd8<8, 8>;
    p.pu[LUMA_8x4].satd   = satd_8x4_altivec;//satd_8x4;
    p.pu[LUMA_4x8].satd   = satd_4x8_altivec;//satd4<4, 8>;
    p.pu[LUMA_16x16].satd = satd_16x16_altivec;//satd8<16, 16>;
    p.pu[LUMA_16x8].satd  = satd_16x8_altivec;//satd8<16, 8>;
    p.pu[LUMA_8x16].satd  = satd_8x16_altivec;//satd8<8, 16>;
    p.pu[LUMA_16x12].satd = satd_altivec<16, 12>;//satd8<16, 12>;
    p.pu[LUMA_12x16].satd = satd_altivec<12, 16>;//satd4<12, 16>;
    p.pu[LUMA_16x4].satd  = satd_altivec<16, 4>;//satd8<16, 4>;
    p.pu[LUMA_4x16].satd  = satd_altivec<4, 16>;//satd4<4, 16>;
    p.pu[LUMA_32x32].satd = satd_altivec<32, 32>;//satd8<32, 32>;
    p.pu[LUMA_32x16].satd = satd_altivec<32, 16>;//satd8<32, 16>;
    p.pu[LUMA_16x32].satd = satd_altivec<16, 32>;//satd8<16, 32>;
    p.pu[LUMA_32x24].satd = satd_altivec<32, 24>;//satd8<32, 24>;
    p.pu[LUMA_24x32].satd = satd_altivec<24, 32>;//satd8<24, 32>;
    p.pu[LUMA_32x8].satd  = satd_altivec<32, 8>;//satd8<32, 8>;
    p.pu[LUMA_8x32].satd  = satd_altivec<8,32>;//satd8<8, 32>;
    p.pu[LUMA_64x64].satd = satd_altivec<64, 64>;//satd8<64, 64>;
    p.pu[LUMA_64x32].satd = satd_altivec<64, 32>;//satd8<64, 32>;
    p.pu[LUMA_32x64].satd = satd_altivec<32, 64>;//satd8<32, 64>;
    p.pu[LUMA_64x48].satd = satd_altivec<64, 48>;//satd8<64, 48>;
    p.pu[LUMA_48x64].satd = satd_altivec<48, 64>;//satd8<48, 64>;
    p.pu[LUMA_64x16].satd = satd_altivec<64, 16>;//satd8<64, 16>;
    p.pu[LUMA_16x64].satd = satd_altivec<16, 64>;//satd8<16, 64>;

    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].satd   = satd_4x4_altivec;//satd_4x4;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].satd   = satd_8x8_altivec;//satd8<8, 8>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].satd = satd_16x16_altivec;//satd8<16, 16>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].satd = satd_altivec<32, 32>;//satd8<32, 32>;

    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].satd   = satd_8x4_altivec;//satd_8x4;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].satd   = satd_4x8_altivec;//satd4<4, 8>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].satd  = satd_16x8_altivec;//satd8<16, 8>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].satd  = satd_8x16_altivec;//satd8<8, 16>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].satd = satd_altivec<32, 16>;//satd8<32, 16>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].satd = satd_altivec<16, 32>;//satd8<16, 32>;

    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].satd = satd_altivec<16, 12>;//satd4<16, 12>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].satd = satd_altivec<12, 16>;//satd4<12, 16>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].satd  = satd_altivec<16, 4>;//satd4<16, 4>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].satd  = satd_altivec<4, 16>;//satd4<4, 16>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].satd = satd_altivec<32, 24>;//satd8<32, 24>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].satd = satd_altivec<24, 32>;//satd8<24, 32>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].satd  = satd_altivec<32, 8>;//satd8<32, 8>;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].satd  = satd_altivec<8,32>;//satd8<8, 32>;

    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].satd   = satd_4x8_altivec;//satd4<4, 8>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].satd  = satd_8x16_altivec;//satd8<8, 16>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].satd = satd_altivec<16, 32>;//satd8<16, 32>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].satd = satd_altivec<32, 64>;//satd8<32, 64>;

    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].satd   = satd_4x4_altivec;//satd_4x4;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].satd   = satd_8x8_altivec;//satd8<8, 8>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].satd  = satd_altivec<4, 16>;//satd4<4, 16>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].satd = satd_16x16_altivec;//satd8<16, 16>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].satd  = satd_altivec<8,32>;//satd8<8, 32>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].satd = satd_altivec<32, 32>;//satd8<32, 32>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].satd = satd_altivec<16, 64>;//satd8<16, 64>;

    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].satd  = satd_altivec<8, 12>;//satd4<8, 12>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].satd   = satd_8x4_altivec;//satd4<8, 4>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].satd = satd_altivec<16, 24>;//satd8<16, 24>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].satd = satd_altivec<12, 32>;//satd4<12, 32>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].satd  = satd_16x8_altivec;//satd8<16, 8>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].satd  = satd_altivec<4, 32>;//satd4<4, 32>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].satd = satd_altivec<32, 48>;//satd8<32, 48>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].satd = satd_altivec<24, 64>;//satd8<24, 64>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].satd = satd_altivec<32, 16>;//satd8<32, 16>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].satd  = satd_altivec<8,64>;//satd8<8, 64>;

    p.cu[BLOCK_4x4].sa8d   = satd_4x4_altivec;//satd_4x4;
    p.cu[BLOCK_8x8].sa8d   = sa8d_8x8_altivec;//sa8d_8x8;
    p.cu[BLOCK_16x16].sa8d = sa8d_16x16_altivec;//sa8d_16x16;
    p.cu[BLOCK_32x32].sa8d = sa8d_32x32_altivec;//sa8d16<32, 32>;
    p.cu[BLOCK_64x64].sa8d = sa8d_64x64_altivec;//sa8d16<64, 64>;

    p.chroma[X265_CSP_I420].cu[BLOCK_16x16].sa8d = sa8d_8x8_altivec;//sa8d8<8, 8>;
    p.chroma[X265_CSP_I420].cu[BLOCK_32x32].sa8d = sa8d_16x16_altivec;//sa8d16<16, 16>;
    p.chroma[X265_CSP_I420].cu[BLOCK_64x64].sa8d = sa8d_32x32_altivec;//sa8d16<32, 32>;

    p.chroma[X265_CSP_I422].cu[BLOCK_16x16].sa8d = sa8d_8x16_altivec;//sa8d8<8, 16>;
    p.chroma[X265_CSP_I422].cu[BLOCK_32x32].sa8d = sa8d_16x32_altivec;//sa8d16<16, 32>;
    p.chroma[X265_CSP_I422].cu[BLOCK_64x64].sa8d = sa8d_32x64_altivec;//sa8d16<32, 64>;

}
}
