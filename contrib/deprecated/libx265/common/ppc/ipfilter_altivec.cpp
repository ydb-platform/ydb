/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Roger Moussalli <rmoussal@us.ibm.com>
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

#include <iostream>
#include "common.h"
#include "primitives.h"
#include "ppccommon.h"

using namespace X265_NS;

// ORIGINAL : for(col=0; col<16; col++) {sum[col]  = src[ocol+col + 0 * srcStride] * c[0];}
#define multiply_pixel_coeff(/*vector int*/ v_sum_0, /*vector int*/ v_sum_1, /*vector int*/ v_sum_2, /*vector int*/ v_sum_3, /*const pixel * */ src, /*int*/ src_offset, /*vector signed short*/ v_coeff) \
{ \
    vector unsigned char v_pixel ; \
    vector signed short v_pixel_16_h, v_pixel_16_l ; \
    const vector signed short v_mask_unisgned_8_to_16 = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ; \
\
    /* load the pixels */ \
    v_pixel = vec_xl(src_offset, src) ; \
\
    /* unpack the 8-bit pixels to 16-bit values (and undo the sign extension) */ \
    v_pixel_16_h = vec_unpackh((vector signed char)v_pixel) ; \
    v_pixel_16_l = vec_unpackl((vector signed char)v_pixel) ; \
    v_pixel_16_h = vec_and(v_pixel_16_h, v_mask_unisgned_8_to_16) ; \
    v_pixel_16_l = vec_and(v_pixel_16_l, v_mask_unisgned_8_to_16) ; \
\
    /* multiply the pixels by the coefficient */ \
    v_sum_0 = vec_mule(v_pixel_16_h, v_coeff) ; \
    v_sum_1 = vec_mulo(v_pixel_16_h, v_coeff) ; \
    v_sum_2 = vec_mule(v_pixel_16_l, v_coeff) ; \
    v_sum_3 = vec_mulo(v_pixel_16_l, v_coeff) ; \
} // end multiply_pixel_coeff()


// ORIGINAL : for(col=0; col<16; col++) {sum[col] += src[ocol+col + 1 * srcStride] * c[1];}
#define multiply_accumulate_pixel_coeff(/*vector int*/ v_sum_0, /*vector int*/ v_sum_1, /*vector int*/ v_sum_2, /*vector int*/ v_sum_3, /*const pixel * */ src, /*int*/ src_offset, /*vector signed short*/ v_coeff) \
{ \
    vector unsigned char v_pixel ; \
    vector signed short v_pixel_16_h, v_pixel_16_l ; \
    vector int v_product_int_0, v_product_int_1, v_product_int_2, v_product_int_3 ; \
    const vector signed short v_mask_unisgned_8_to_16 = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ; \
\
    /* ORIGINAL : for(col=0; col<16; col++) {sum[col]  = src[ocol+col + 0 * srcStride] * c[0];} */ \
    /* load the pixels */ \
    v_pixel = vec_xl(src_offset, src) ; \
\
    /* unpack the 8-bit pixels to 16-bit values (and undo the sign extension) */ \
    v_pixel_16_h = vec_unpackh((vector signed char)v_pixel) ; \
    v_pixel_16_l = vec_unpackl((vector signed char)v_pixel) ; \
    v_pixel_16_h = vec_and(v_pixel_16_h, v_mask_unisgned_8_to_16) ; \
    v_pixel_16_l = vec_and(v_pixel_16_l, v_mask_unisgned_8_to_16) ; \
\
    /* multiply the pixels by the coefficient */ \
    v_product_int_0 = vec_mule(v_pixel_16_h, v_coeff) ; \
    v_product_int_1 = vec_mulo(v_pixel_16_h, v_coeff) ; \
    v_product_int_2 = vec_mule(v_pixel_16_l, v_coeff) ; \
    v_product_int_3 = vec_mulo(v_pixel_16_l, v_coeff) ; \
\
    /* accumulate the results with the sum vectors */ \
    v_sum_0 = vec_add(v_sum_0, v_product_int_0) ; \
    v_sum_1 = vec_add(v_sum_1, v_product_int_1) ; \
    v_sum_2 = vec_add(v_sum_2, v_product_int_2) ; \
    v_sum_3 = vec_add(v_sum_3, v_product_int_3) ; \
} // end multiply_accumulate_pixel_coeff()



#if 0
//ORIGINAL
// Works with the following values:
// N = 8
// width >= 16 (multiple of 16)
// any height
template<int N, int width, int height>
void interp_vert_pp_altivec(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx)
{


    const int16_t* c = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    const int shift = IF_FILTER_PREC;
    const int offset = 1 << (shift - 1);
    const uint16_t maxVal = (1 << X265_DEPTH) - 1;

    src -= (N / 2 - 1) * srcStride;


    // Vector to hold replicated shift amount
    const vector unsigned int v_shift = {shift, shift, shift, shift} ;

    // Vector to hold replicated offset
    const vector int v_offset = {offset, offset, offset, offset} ;

    // Vector to hold replicated maxVal
    const vector signed short v_maxVal = {maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal} ;


    // Vector to hold replicated coefficients (one coefficient replicated per vector)
    vector signed short v_coeff_0, v_coeff_1, v_coeff_2, v_coeff_3, v_coeff_4, v_coeff_5, v_coeff_6, v_coeff_7 ;
    vector signed short v_coefficients = vec_xl(0, c) ; // load all coefficients into one vector
    
    // Replicate the coefficients into respective vectors
    v_coeff_0 = vec_splat(v_coefficients, 0) ;
    v_coeff_1 = vec_splat(v_coefficients, 1) ;
    v_coeff_2 = vec_splat(v_coefficients, 2) ;
    v_coeff_3 = vec_splat(v_coefficients, 3) ;
    v_coeff_4 = vec_splat(v_coefficients, 4) ;
    v_coeff_5 = vec_splat(v_coefficients, 5) ;
    v_coeff_6 = vec_splat(v_coefficients, 6) ;
    v_coeff_7 = vec_splat(v_coefficients, 7) ;

    

    int row, ocol, col;
    for (row = 0; row < height; row++)
    {
        for (ocol = 0; ocol < width; ocol+=16)
        {


           // int sum[16] ;
           // int16_t val[16] ;

           // --> for(col=0; col<16; col++) {sum[col]  = src[ocol+col + 1 * srcStride] * c[0];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 1 * srcStride] * c[1];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 2 * srcStride] * c[2];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 3 * srcStride] * c[3];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 4 * srcStride] * c[4];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 5 * srcStride] * c[5];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 6 * srcStride] * c[6];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 7 * srcStride] * c[7];}


	        vector signed int v_sum_0, v_sum_1, v_sum_2, v_sum_3 ;
            vector signed short v_val_0, v_val_1 ;



            multiply_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol, v_coeff_0) ;
            multiply_accumulate_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol + 1 * srcStride, v_coeff_1) ;
            multiply_accumulate_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol + 2 * srcStride, v_coeff_2) ;
            multiply_accumulate_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol + 3 * srcStride, v_coeff_3) ;
            multiply_accumulate_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol + 4 * srcStride, v_coeff_4) ;
            multiply_accumulate_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol + 5 * srcStride, v_coeff_5) ;
            multiply_accumulate_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol + 6 * srcStride, v_coeff_6) ;
            multiply_accumulate_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol + 7 * srcStride, v_coeff_7) ;





            // --> for(col=0; col<16; col++) {val[col] = (int16_t)((sum[col] + offset) >> shift);}
            // Add offset
            v_sum_0 = vec_add(v_sum_0, v_offset) ;
            v_sum_1 = vec_add(v_sum_1, v_offset) ;
            v_sum_2 = vec_add(v_sum_2, v_offset) ;
            v_sum_3 = vec_add(v_sum_3, v_offset) ;
            // Shift right by "shift"
            v_sum_0 = vec_sra(v_sum_0, v_shift) ;
            v_sum_1 = vec_sra(v_sum_1, v_shift) ;
            v_sum_2 = vec_sra(v_sum_2, v_shift) ;
            v_sum_3 = vec_sra(v_sum_3, v_shift) ;

            // Pack into 16-bit numbers
            v_val_0 = vec_pack(v_sum_0, v_sum_2) ;
            v_val_1 = vec_pack(v_sum_1, v_sum_3) ;


            
            // --> for(col=0; col<16; col++) {val[col] = (val[col] < 0) ? 0 : val[col];}
            vector bool short v_comp_zero_0, v_comp_zero_1 ;
            vector signed short v_max_masked_0, v_max_masked_1 ;
            vector signed short zeros16 = {0,0,0,0,0,0,0,0} ;
            // Compute less than 0
            v_comp_zero_0 = vec_cmplt(v_val_0, zeros16) ;
            v_comp_zero_1 = vec_cmplt(v_val_1, zeros16) ;
            // Keep values that are greater or equal to 0
            v_val_0 = vec_andc(v_val_0, v_comp_zero_0) ;
            v_val_1 = vec_andc(v_val_1, v_comp_zero_1) ;



            // --> for(col=0; col<16; col++) {val[col] = (val[col] > maxVal) ? maxVal : val[col];}
            vector bool short v_comp_max_0, v_comp_max_1 ;
            // Compute greater than max
            v_comp_max_0 = vec_cmpgt(v_val_0, v_maxVal) ;
            v_comp_max_1 = vec_cmpgt(v_val_1, v_maxVal) ;
            // Replace values greater than maxVal with maxVal
            v_val_0 = vec_sel(v_val_0, v_maxVal, v_comp_max_0) ;
            v_val_1 = vec_sel(v_val_1, v_maxVal, v_comp_max_1) ;



            // --> for(col=0; col<16; col++) {dst[ocol+col] = (pixel)val[col];}
            // Pack the vals into 8-bit numbers
            // but also re-ordering them - side effect of mule and mulo
            vector unsigned char v_result ;
            vector unsigned char v_perm_index = {0x00, 0x10, 0x02, 0x12, 0x04, 0x14, 0x06, 0x16, 0x08 ,0x18, 0x0A, 0x1A, 0x0C, 0x1C, 0x0E, 0x1E} ;
            v_result = (vector unsigned char)vec_perm(v_val_0, v_val_1, v_perm_index) ;
            // Store the results back to dst[]
            vec_xst(v_result, ocol, (unsigned char *)dst) ;
        }

        src += srcStride;
        dst += dstStride;
    }
} // end interp_vert_pp_altivec()
#else 
// Works with the following values:
// N = 8
// width >= 16 (multiple of 16)
// any height
template<int N, int width, int height>
void interp_vert_pp_altivec(const pixel* __restrict__ src, intptr_t srcStride, pixel* __restrict__ dst, intptr_t dstStride, int coeffIdx)
{
    const int16_t* __restrict__ c = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    int shift = IF_FILTER_PREC;
    int offset = 1 << (shift - 1);
    uint16_t maxVal = (1 << X265_DEPTH) - 1;

    src -= (N / 2 - 1) * srcStride;

    vector signed short vcoeff0     = vec_splats(c[0]);
    vector signed short vcoeff1     = vec_splats(c[1]);
    vector signed short vcoeff2     = vec_splats(c[2]);
    vector signed short vcoeff3     = vec_splats(c[3]);
    vector signed short vcoeff4     = vec_splats(c[4]);
    vector signed short vcoeff5     = vec_splats(c[5]);
    vector signed short vcoeff6     = vec_splats(c[6]);
    vector signed short vcoeff7     = vec_splats(c[7]);
    vector signed short voffset     = vec_splats((short)offset);
    vector signed short vshift      = vec_splats((short)shift);
    vector signed short vmaxVal     = vec_splats((short)maxVal);
    vector signed short  vzero_s16  = vec_splats( (signed short)0u);;
    vector signed int    vzero_s32  = vec_splats( (signed int)0u);
    vector unsigned char vzero_u8   = vec_splats( (unsigned char)0u );
    vector unsigned char vchar_to_short_maskH = {24, 0, 25, 0, 26, 0, 27, 0, 28, 0, 29, 0, 30, 0, 31, 0}; 
    vector unsigned char vchar_to_short_maskL = {16, 0, 17, 0 ,18, 0, 19, 0, 20, 0, 21, 0, 22, 0, 23, 0}; 

    vector signed short vsrcH, vsrcL, vsumH, vsumL;
    vector unsigned char vsrc;

    vector signed short vsrc2H, vsrc2L, vsum2H, vsum2L;
    vector unsigned char vsrc2;

    const pixel* __restrict__ src2 = src+srcStride;
    pixel* __restrict__ dst2       = dst+dstStride;

    int row, col;
    for (row = 0; row < height; row+=2)
    {
        for (col = 0; col < width; col+=16)
        {
            vsrc   = vec_xl(0, (unsigned char*)&src[col + 0*srcStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH  = vsrcH * vcoeff0;
            vsumL  = vsrcL * vcoeff0;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 1*srcStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH += vsrcH * vcoeff1;
            vsumL += vsrcL * vcoeff1;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 2*srcStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH += vsrcH * vcoeff2;
            vsumL += vsrcL * vcoeff2;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 3*srcStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH += vsrcH * vcoeff3;
            vsumL += vsrcL * vcoeff3;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 4*srcStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH += vsrcH * vcoeff4;
            vsumL += vsrcL * vcoeff4;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 5*srcStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH += vsrcH * vcoeff5;
            vsumL += vsrcL * vcoeff5;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 6*srcStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH += vsrcH * vcoeff6;
            vsumL += vsrcL * vcoeff6;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 7*srcStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH += vsrcH * vcoeff7;
            vsumL += vsrcL * vcoeff7;

            vector short vvalH = (vsumH + voffset) >> vshift;
            vvalH = vec_max( vvalH, vzero_s16 );
            vvalH = vec_min( vvalH, vmaxVal   );

            vector short vvalL = (vsumL + voffset) >> vshift;
            vvalL = vec_max( vvalL, vzero_s16 );
            vvalL = vec_min( vvalL, vmaxVal   );

            vector signed char vdst = vec_pack( vvalL, vvalH );
            vec_xst( vdst, 0, (signed char*)&dst[col] );

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 0*srcStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H  = vsrc2H * vcoeff0;
            vsum2L  = vsrc2L * vcoeff0;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 1*srcStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H += vsrc2H * vcoeff1;
            vsum2L += vsrc2L * vcoeff1;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 2*srcStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H += vsrc2H * vcoeff2;
            vsum2L += vsrc2L * vcoeff2;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 3*srcStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H += vsrc2H * vcoeff3;
            vsum2L += vsrc2L * vcoeff3;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 4*srcStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H += vsrc2H * vcoeff4;
            vsum2L += vsrc2L * vcoeff4;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 5*srcStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H += vsrc2H * vcoeff5;
            vsum2L += vsrc2L * vcoeff5;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 6*srcStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H += vsrc2H * vcoeff6;
            vsum2L += vsrc2L * vcoeff6;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 7*srcStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H += vsrc2H * vcoeff7;
            vsum2L += vsrc2L * vcoeff7;

            vector short vval2H = (vsum2H + voffset) >> vshift;
            vval2H = vec_max( vval2H, vzero_s16 );
            vval2H = vec_min( vval2H, vmaxVal   );

            vector short vval2L = (vsum2L + voffset) >> vshift;
            vval2L = vec_max( vval2L, vzero_s16 );
            vval2L = vec_min( vval2L, vmaxVal   );

            vector signed char vdst2 = vec_pack( vval2L, vval2H );
            vec_xst( vdst2, 0, (signed char*)&dst2[col] );
        }

        src  += 2*srcStride;
        dst  += 2*dstStride;
        src2 += 2*srcStride;
        dst2 += 2*dstStride;
    }
}
#endif


// ORIGINAL : for(col=0; col<16; col++) {sum[col]  = src[ocol+col + 0 * srcStride] * c[0];}
#define multiply_sp_pixel_coeff(/*vector int*/ v_sum_0, /*vector int*/ v_sum_1, /*vector int*/ v_sum_2, /*vector int*/ v_sum_3, /*const int16_t * */ src, /*int*/ src_offset, /*vector signed short*/ v_coeff) \
{ \
    vector signed short v_pixel_16_h, v_pixel_16_l ; \
\
    /* load the pixels */ \
    v_pixel_16_h = vec_xl(src_offset, src) ; \
    v_pixel_16_l = vec_xl(src_offset + 16, src) ; \
\
    /* multiply the pixels by the coefficient */ \
    v_sum_0 = vec_mule(v_pixel_16_h, v_coeff) ; \
    v_sum_1 = vec_mulo(v_pixel_16_h, v_coeff) ; \
    v_sum_2 = vec_mule(v_pixel_16_l, v_coeff) ; \
    v_sum_3 = vec_mulo(v_pixel_16_l, v_coeff) ; \
\
} // end multiply_pixel_coeff()


// ORIGINAL : for(col=0; col<16; col++) {sum[col] += src[ocol+col + 1 * srcStride] * c[1];}
#define multiply_accumulate_sp_pixel_coeff(/*vector int*/ v_sum_0, /*vector int*/ v_sum_1, /*vector int*/ v_sum_2, /*vector int*/ v_sum_3, /*const pixel * */ src, /*int*/ src_offset, /*vector signed short*/ v_coeff) \
{ \
    vector signed short v_pixel_16_h, v_pixel_16_l ; \
    vector int v_product_int_0, v_product_int_1, v_product_int_2, v_product_int_3 ; \
\
    /* ORIGINAL : for(col=0; col<16; col++) {sum[col]  = src[ocol+col + 0 * srcStride] * c[0];} */ \
\
    /* load the pixels */ \
    v_pixel_16_h = vec_xl(src_offset, src) ; \
    v_pixel_16_l = vec_xl(src_offset + 16, src) ; \
\
    /* multiply the pixels by the coefficient */ \
    v_product_int_0 = vec_mule(v_pixel_16_h, v_coeff) ; \
    v_product_int_1 = vec_mulo(v_pixel_16_h, v_coeff) ; \
    v_product_int_2 = vec_mule(v_pixel_16_l, v_coeff) ; \
    v_product_int_3 = vec_mulo(v_pixel_16_l, v_coeff) ; \
\
    /* accumulate the results with the sum vectors */ \
    v_sum_0 = vec_add(v_sum_0, v_product_int_0) ; \
    v_sum_1 = vec_add(v_sum_1, v_product_int_1) ; \
    v_sum_2 = vec_add(v_sum_2, v_product_int_2) ; \
    v_sum_3 = vec_add(v_sum_3, v_product_int_3) ; \
\
} // end multiply_accumulate_pixel_coeff()


// Works with the following values:
// N = 8
// width >= 16 (multiple of 16)
// any height
template <int N, int width, int height>
void filterVertical_sp_altivec(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx)
{
    int headRoom = IF_INTERNAL_PREC - X265_DEPTH;
    unsigned int shift = IF_FILTER_PREC + headRoom;
    int offset = (1 << (shift - 1)) + (IF_INTERNAL_OFFS << IF_FILTER_PREC);
    const uint16_t maxVal = (1 << X265_DEPTH) - 1;
    const int16_t* coeff = (N == 8 ? g_lumaFilter[coeffIdx] : g_chromaFilter[coeffIdx]);

    src -= (N / 2 - 1) * srcStride;


    // Vector to hold replicated shift amount
    const vector unsigned int v_shift = {shift, shift, shift, shift} ;

    // Vector to hold replicated offset
    const vector int v_offset = {offset, offset, offset, offset} ;

    // Vector to hold replicated maxVal
    const vector signed short v_maxVal = {maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal} ;


    // Vector to hold replicated coefficients (one coefficient replicated per vector)
    vector signed short v_coeff_0, v_coeff_1, v_coeff_2, v_coeff_3, v_coeff_4, v_coeff_5, v_coeff_6, v_coeff_7 ;
    vector signed short v_coefficients = vec_xl(0, coeff) ; // load all coefficients into one vector
    
    // Replicate the coefficients into respective vectors
    v_coeff_0 = vec_splat(v_coefficients, 0) ;
    v_coeff_1 = vec_splat(v_coefficients, 1) ;
    v_coeff_2 = vec_splat(v_coefficients, 2) ;
    v_coeff_3 = vec_splat(v_coefficients, 3) ;
    v_coeff_4 = vec_splat(v_coefficients, 4) ;
    v_coeff_5 = vec_splat(v_coefficients, 5) ;
    v_coeff_6 = vec_splat(v_coefficients, 6) ;
    v_coeff_7 = vec_splat(v_coefficients, 7) ;

    

    int row, ocol, col;
    for (row = 0; row < height; row++)
    {
        for (ocol = 0; ocol < width; ocol+= 16 )
        {

           // int sum[16] ;
           // int16_t val[16] ;

           // --> for(col=0; col<16; col++) {sum[col]  = src[ocol+col + 1 * srcStride] * c[0];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 1 * srcStride] * c[1];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 2 * srcStride] * c[2];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 3 * srcStride] * c[3];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 4 * srcStride] * c[4];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 5 * srcStride] * c[5];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 6 * srcStride] * c[6];}
           // --> for(col=0; col<16; col++) {sum[col] += src[ocol+col + 7 * srcStride] * c[7];}


	        vector signed int v_sum_0, v_sum_1, v_sum_2, v_sum_3 ;
            vector signed short v_val_0, v_val_1 ;


            // Added a factor of 2 to the offset since this is a BYTE offset, and each input pixel is of size 2Bytes
            multiply_sp_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, ocol * 2, v_coeff_0) ;
            multiply_accumulate_sp_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, (ocol + 1 * srcStride) * 2, v_coeff_1) ;
            multiply_accumulate_sp_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, (ocol + 2 * srcStride) * 2, v_coeff_2) ;
            multiply_accumulate_sp_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, (ocol + 3 * srcStride) * 2, v_coeff_3) ;
            multiply_accumulate_sp_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, (ocol + 4 * srcStride) * 2, v_coeff_4) ;
            multiply_accumulate_sp_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, (ocol + 5 * srcStride) * 2, v_coeff_5) ;
            multiply_accumulate_sp_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, (ocol + 6 * srcStride) * 2, v_coeff_6) ;
            multiply_accumulate_sp_pixel_coeff(v_sum_0, v_sum_1, v_sum_2, v_sum_3, src, (ocol + 7 * srcStride) * 2, v_coeff_7) ;





            // --> for(col=0; col<16; col++) {val[col] = (int16_t)((sum[col] + offset) >> shift);}
            // Add offset
            v_sum_0 = vec_add(v_sum_0, v_offset) ;
            v_sum_1 = vec_add(v_sum_1, v_offset) ;
            v_sum_2 = vec_add(v_sum_2, v_offset) ;
            v_sum_3 = vec_add(v_sum_3, v_offset) ;
            // Shift right by "shift"
            v_sum_0 = vec_sra(v_sum_0, v_shift) ;
            v_sum_1 = vec_sra(v_sum_1, v_shift) ;
            v_sum_2 = vec_sra(v_sum_2, v_shift) ;
            v_sum_3 = vec_sra(v_sum_3, v_shift) ;

            // Pack into 16-bit numbers
            v_val_0 = vec_pack(v_sum_0, v_sum_2) ;
            v_val_1 = vec_pack(v_sum_1, v_sum_3) ;


            
            // --> for(col=0; col<16; col++) {val[col] = (val[col] < 0) ? 0 : val[col];}
            vector bool short v_comp_zero_0, v_comp_zero_1 ;
            vector signed short v_max_masked_0, v_max_masked_1 ;
            vector signed short zeros16 = {0,0,0,0,0,0,0,0} ;
            // Compute less than 0
            v_comp_zero_0 = vec_cmplt(v_val_0, zeros16) ;
            v_comp_zero_1 = vec_cmplt(v_val_1, zeros16) ;
            // Keep values that are greater or equal to 0
            v_val_0 = vec_andc(v_val_0, v_comp_zero_0) ;
            v_val_1 = vec_andc(v_val_1, v_comp_zero_1) ;



            // --> for(col=0; col<16; col++) {val[col] = (val[col] > maxVal) ? maxVal : val[col];}
            vector bool short v_comp_max_0, v_comp_max_1 ;
            // Compute greater than max
            v_comp_max_0 = vec_cmpgt(v_val_0, v_maxVal) ;
            v_comp_max_1 = vec_cmpgt(v_val_1, v_maxVal) ;
            // Replace values greater than maxVal with maxVal
            v_val_0 = vec_sel(v_val_0, v_maxVal, v_comp_max_0) ;
            v_val_1 = vec_sel(v_val_1, v_maxVal, v_comp_max_1) ;



            // --> for(col=0; col<16; col++) {dst[ocol+col] = (pixel)val[col];}
            // Pack the vals into 8-bit numbers
            // but also re-ordering them - side effect of mule and mulo
            vector unsigned char v_result ;
            vector unsigned char v_perm_index = {0x00, 0x10, 0x02, 0x12, 0x04, 0x14, 0x06, 0x16, 0x08 ,0x18, 0x0A, 0x1A, 0x0C, 0x1C, 0x0E, 0x1E} ;
            v_result = (vector unsigned char)vec_perm(v_val_0, v_val_1, v_perm_index) ;
            // Store the results back to dst[]
            vec_xst(v_result, ocol, (unsigned char *)dst) ;
        }

        src += srcStride;
        dst += dstStride;
    }
} // end filterVertical_sp_altivec()





// Works with the following values:
// N = 8
// width >= 32 (multiple of 32)
// any height
template <int N, int width, int height>
void interp_horiz_ps_altivec(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt)
{

    const int16_t* coeff = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    int headRoom = IF_INTERNAL_PREC - X265_DEPTH;
    unsigned int shift = IF_FILTER_PREC - headRoom;
    int offset = -IF_INTERNAL_OFFS << shift;
    int blkheight = height;

    src -= N / 2 - 1;

    if (isRowExt)
    {
        src -= (N / 2 - 1) * srcStride;
        blkheight += N - 1;
    }


    vector signed short v_coeff ;
    v_coeff = vec_xl(0, coeff) ;


    vector unsigned char v_pixel_char_0, v_pixel_char_1, v_pixel_char_2 ;
    vector signed short v_pixel_short_0, v_pixel_short_1, v_pixel_short_2, v_pixel_short_3, v_pixel_short_4 ;
    const vector signed short v_mask_unisgned_char_to_short = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ; \
    const vector signed int v_zeros_int = {0, 0, 0, 0} ;
    const vector signed short v_zeros_short = {0, 0, 0, 0, 0, 0, 0, 0} ;

    vector signed int v_product_0_0, v_product_0_1 ;
    vector signed int v_product_1_0, v_product_1_1 ;
    vector signed int v_product_2_0, v_product_2_1 ;
    vector signed int v_product_3_0, v_product_3_1 ;

    vector signed int v_sum_0, v_sum_1, v_sum_2, v_sum_3 ;
    
    vector signed int v_sums_temp_col0, v_sums_temp_col1, v_sums_temp_col2, v_sums_temp_col3 ;
    vector signed int v_sums_col0_0, v_sums_col0_1 ;
    vector signed int v_sums_col1_0, v_sums_col1_1 ;
    vector signed int v_sums_col2_0, v_sums_col2_1 ;
    vector signed int v_sums_col3_0, v_sums_col3_1 ;


    const vector signed int v_offset = {offset, offset, offset, offset};
    const vector unsigned int v_shift = {shift, shift, shift, shift} ;


    vector unsigned char v_sums_shamt = {0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} ;



    pixel *next_src ; 
    int16_t *next_dst ;

    int row, col;
    for (row = 0; row < blkheight; row++)
    {
        next_src = (pixel *)src + srcStride ;
        next_dst = (int16_t *)dst + dstStride ;

        for(int col_iter=0; col_iter<width; col_iter+=32)
        {
            // Load a full row of pixels (32 + 7)
            v_pixel_char_0 = vec_xl(0, src) ;
            v_pixel_char_1 = vec_xl(16, src) ;
            v_pixel_char_2 = vec_xl(32, src) ;


            v_sums_temp_col0 = v_zeros_int ;
            v_sums_temp_col1 = v_zeros_int ;
            v_sums_temp_col2 = v_zeros_int ;
            v_sums_temp_col3 = v_zeros_int ;
            

            // Expand the loaded pixels into shorts
            v_pixel_short_0 = vec_unpackh((vector signed char)v_pixel_char_0) ;
            v_pixel_short_1 = vec_unpackl((vector signed char)v_pixel_char_0) ;
            v_pixel_short_2 = vec_unpackh((vector signed char)v_pixel_char_1) ;
            v_pixel_short_3 = vec_unpackl((vector signed char)v_pixel_char_1) ;
            v_pixel_short_4 = vec_unpackh((vector signed char)v_pixel_char_2) ;

            v_pixel_short_0 = vec_and(v_pixel_short_0, v_mask_unisgned_char_to_short) ;
            v_pixel_short_1 = vec_and(v_pixel_short_1, v_mask_unisgned_char_to_short) ;
            v_pixel_short_2 = vec_and(v_pixel_short_2, v_mask_unisgned_char_to_short) ;
            v_pixel_short_3 = vec_and(v_pixel_short_3, v_mask_unisgned_char_to_short) ;
            v_pixel_short_4 = vec_and(v_pixel_short_4, v_mask_unisgned_char_to_short) ;


            
            // Four colum sets are processed below
            // One colum per set per iteration
            for(col=0; col < 8; col++)
            {

                // Multiply the pixels by the coefficients
                v_product_0_0 = vec_mule(v_pixel_short_0, v_coeff) ;
                v_product_0_1 = vec_mulo(v_pixel_short_0, v_coeff) ;
                
                v_product_1_0 = vec_mule(v_pixel_short_1, v_coeff) ;
                v_product_1_1 = vec_mulo(v_pixel_short_1, v_coeff) ;
                
                v_product_2_0 = vec_mule(v_pixel_short_2, v_coeff) ;
                v_product_2_1 = vec_mulo(v_pixel_short_2, v_coeff) ;
                
                v_product_3_0 = vec_mule(v_pixel_short_3, v_coeff) ;
                v_product_3_1 = vec_mulo(v_pixel_short_3, v_coeff) ;


                // Sum up the multiplication results
                v_sum_0 = vec_add(v_product_0_0, v_product_0_1) ;
                v_sum_0 = vec_sums(v_sum_0, v_zeros_int) ;

                v_sum_1 = vec_add(v_product_1_0, v_product_1_1) ;
                v_sum_1 = vec_sums(v_sum_1, v_zeros_int) ;

                v_sum_2 = vec_add(v_product_2_0, v_product_2_1) ;
                v_sum_2 = vec_sums(v_sum_2, v_zeros_int) ;

                v_sum_3 = vec_add(v_product_3_0, v_product_3_1) ;
                v_sum_3 = vec_sums(v_sum_3, v_zeros_int) ;


                // Insert the sum results into respective vectors
                v_sums_temp_col0 = vec_sro(v_sums_temp_col0, v_sums_shamt) ;
                v_sums_temp_col0 = vec_or(v_sum_0, v_sums_temp_col0) ;

                v_sums_temp_col1 = vec_sro(v_sums_temp_col1, v_sums_shamt) ;
                v_sums_temp_col1 = vec_or(v_sum_1, v_sums_temp_col1) ;

                v_sums_temp_col2 = vec_sro(v_sums_temp_col2, v_sums_shamt) ;
                v_sums_temp_col2 = vec_or(v_sum_2, v_sums_temp_col2) ;

                v_sums_temp_col3 = vec_sro(v_sums_temp_col3, v_sums_shamt) ;
                v_sums_temp_col3 = vec_or(v_sum_3, v_sums_temp_col3) ;


                if(col == 3)
                {
                    v_sums_col0_0 = v_sums_temp_col0 ;
                    v_sums_col1_0 = v_sums_temp_col1 ;
                    v_sums_col2_0 = v_sums_temp_col2 ;
                    v_sums_col3_0 = v_sums_temp_col3 ;

                    v_sums_temp_col0 = v_zeros_int ;
                    v_sums_temp_col1 = v_zeros_int ;
                    v_sums_temp_col2 = v_zeros_int ;
                    v_sums_temp_col3 = v_zeros_int ;
                }


                // Shift the pixels by 1 (short pixel)
                v_pixel_short_0 = vec_sld(v_pixel_short_1, v_pixel_short_0, 14) ;
                v_pixel_short_1 = vec_sld(v_pixel_short_2, v_pixel_short_1, 14) ;
                v_pixel_short_2 = vec_sld(v_pixel_short_3, v_pixel_short_2, 14) ;
                v_pixel_short_3 = vec_sld(v_pixel_short_4, v_pixel_short_3, 14) ;
                const vector unsigned char v_shift_right_two_bytes_shamt = {0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} ;
                v_pixel_short_4 = vec_sro(v_pixel_short_4, v_shift_right_two_bytes_shamt) ;
            }

            // Copy the sums result to the second vector (per colum)
            v_sums_col0_1 = v_sums_temp_col0 ;
            v_sums_col1_1 = v_sums_temp_col1 ;
            v_sums_col2_1 = v_sums_temp_col2 ;
            v_sums_col3_1 = v_sums_temp_col3 ;
            


            // Post processing and eventually 2 stores
            // Original code:
            // int16_t val = (int16_t)((sum + offset) >> shift);
            // dst[col] = val;


            v_sums_col0_0 = vec_sra(vec_add(v_sums_col0_0, v_offset), v_shift) ;
            v_sums_col0_1 = vec_sra(vec_add(v_sums_col0_1, v_offset), v_shift) ;
            v_sums_col1_0 = vec_sra(vec_add(v_sums_col1_0, v_offset), v_shift) ;
            v_sums_col1_1 = vec_sra(vec_add(v_sums_col1_1, v_offset), v_shift) ;
            v_sums_col2_0 = vec_sra(vec_add(v_sums_col2_0, v_offset), v_shift) ;
            v_sums_col2_1 = vec_sra(vec_add(v_sums_col2_1, v_offset), v_shift) ;
            v_sums_col3_0 = vec_sra(vec_add(v_sums_col3_0, v_offset), v_shift) ;
            v_sums_col3_1 = vec_sra(vec_add(v_sums_col3_1, v_offset), v_shift) ;

            
            vector signed short v_val_col0, v_val_col1, v_val_col2, v_val_col3 ;
            v_val_col0 = vec_pack(v_sums_col0_0, v_sums_col0_1) ;
            v_val_col1 = vec_pack(v_sums_col1_0, v_sums_col1_1) ;
            v_val_col2 = vec_pack(v_sums_col2_0, v_sums_col2_1) ;
            v_val_col3 = vec_pack(v_sums_col3_0, v_sums_col3_1) ;


            
            // Store results
            vec_xst(v_val_col0, 0, dst) ;
            vec_xst(v_val_col1, 16, dst) ;
            vec_xst(v_val_col2, 32, dst) ;
            vec_xst(v_val_col3, 48, dst) ;

            src += 32 ;
            dst += 32 ;

        } // end for col_iter

        src = next_src ;
        dst = next_dst ;
    }
} // interp_horiz_ps_altivec () 



// Works with the following values:
// N = 8
// width >= 32 (multiple of 32)
// any height
template <int N, int width, int height>
void interp_hv_pp_altivec(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int idxX, int idxY)
{

    short immedVals[(64 + 8) * (64 + 8)];

    interp_horiz_ps_altivec<N, width, height>(src, srcStride, immedVals, width, idxX, 1);

    //!!filterVertical_sp_c<N>(immedVals + 3 * width, width, dst, dstStride, width, height, idxY);
    filterVertical_sp_altivec<N,width,height>(immedVals + 3 * width, width, dst, dstStride, idxY);
}

//ORIGINAL
#if 0
// Works with the following values:
// N = 8
// width >= 32 (multiple of 32)
// any height
template <int N, int width, int height>
void interp_horiz_pp_altivec(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx)
{

    const int16_t* coeff = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    int headRoom = IF_FILTER_PREC;
    int offset =  (1 << (headRoom - 1));
    uint16_t maxVal = (1 << X265_DEPTH) - 1;
    int cStride = 1;

    src -= (N / 2 - 1) * cStride;


    vector signed short v_coeff ;
    v_coeff = vec_xl(0, coeff) ;


    vector unsigned char v_pixel_char_0, v_pixel_char_1, v_pixel_char_2 ;
    vector signed short v_pixel_short_0, v_pixel_short_1, v_pixel_short_2, v_pixel_short_3, v_pixel_short_4 ;
    const vector signed short v_mask_unisgned_char_to_short = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ; \
    const vector signed int v_zeros_int = {0, 0, 0, 0} ;
    const vector signed short v_zeros_short = {0, 0, 0, 0, 0, 0, 0, 0} ;

    vector signed int v_product_0_0, v_product_0_1 ;
    vector signed int v_product_1_0, v_product_1_1 ;
    vector signed int v_product_2_0, v_product_2_1 ;
    vector signed int v_product_3_0, v_product_3_1 ;

    vector signed int v_sum_0, v_sum_1, v_sum_2, v_sum_3 ;
    
    vector signed int v_sums_temp_col0, v_sums_temp_col1, v_sums_temp_col2, v_sums_temp_col3 ;
    vector signed int v_sums_col0_0, v_sums_col0_1 ;
    vector signed int v_sums_col1_0, v_sums_col1_1 ;
    vector signed int v_sums_col2_0, v_sums_col2_1 ;
    vector signed int v_sums_col3_0, v_sums_col3_1 ;


    const vector signed int v_offset = {offset, offset, offset, offset};
    const vector unsigned int v_headRoom = {headRoom, headRoom, headRoom, headRoom} ;


    vector unsigned char v_sums_shamt = {0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} ;


    pixel *next_src ; 
    pixel *next_dst ;

    int row, col;
    for (row = 0; row < height; row++)
    {
        next_src = (pixel *)src + srcStride ;
        next_dst = (pixel *)dst + dstStride ;

        for(int col_iter=0; col_iter<width; col_iter+=32)
        {

            // Load a full row of pixels (32 + 7)
            v_pixel_char_0 = vec_xl(0, src) ;
            v_pixel_char_1 = vec_xl(16, src) ;
            v_pixel_char_2 = vec_xl(32, src) ;


            v_sums_temp_col0 = v_zeros_int ;
            v_sums_temp_col1 = v_zeros_int ;
            v_sums_temp_col2 = v_zeros_int ;
            v_sums_temp_col3 = v_zeros_int ;
            

            // Expand the loaded pixels into shorts
            v_pixel_short_0 = vec_unpackh((vector signed char)v_pixel_char_0) ;
            v_pixel_short_1 = vec_unpackl((vector signed char)v_pixel_char_0) ;
            v_pixel_short_2 = vec_unpackh((vector signed char)v_pixel_char_1) ;
            v_pixel_short_3 = vec_unpackl((vector signed char)v_pixel_char_1) ;
            v_pixel_short_4 = vec_unpackh((vector signed char)v_pixel_char_2) ;

            v_pixel_short_0 = vec_and(v_pixel_short_0, v_mask_unisgned_char_to_short) ;
            v_pixel_short_1 = vec_and(v_pixel_short_1, v_mask_unisgned_char_to_short) ;
            v_pixel_short_2 = vec_and(v_pixel_short_2, v_mask_unisgned_char_to_short) ;
            v_pixel_short_3 = vec_and(v_pixel_short_3, v_mask_unisgned_char_to_short) ;
            v_pixel_short_4 = vec_and(v_pixel_short_4, v_mask_unisgned_char_to_short) ;


            
            // Four colum sets are processed below
            // One colum per set per iteration
            for(col=0; col < 8; col++)
            {

                // Multiply the pixels by the coefficients
                v_product_0_0 = vec_mule(v_pixel_short_0, v_coeff) ;
                v_product_0_1 = vec_mulo(v_pixel_short_0, v_coeff) ;
                
                v_product_1_0 = vec_mule(v_pixel_short_1, v_coeff) ;
                v_product_1_1 = vec_mulo(v_pixel_short_1, v_coeff) ;
                
                v_product_2_0 = vec_mule(v_pixel_short_2, v_coeff) ;
                v_product_2_1 = vec_mulo(v_pixel_short_2, v_coeff) ;
                
                v_product_3_0 = vec_mule(v_pixel_short_3, v_coeff) ;
                v_product_3_1 = vec_mulo(v_pixel_short_3, v_coeff) ;


                // Sum up the multiplication results
                v_sum_0 = vec_add(v_product_0_0, v_product_0_1) ;
                v_sum_0 = vec_sums(v_sum_0, v_zeros_int) ;

                v_sum_1 = vec_add(v_product_1_0, v_product_1_1) ;
                v_sum_1 = vec_sums(v_sum_1, v_zeros_int) ;

                v_sum_2 = vec_add(v_product_2_0, v_product_2_1) ;
                v_sum_2 = vec_sums(v_sum_2, v_zeros_int) ;

                v_sum_3 = vec_add(v_product_3_0, v_product_3_1) ;
                v_sum_3 = vec_sums(v_sum_3, v_zeros_int) ;


                // Insert the sum results into respective vectors
                v_sums_temp_col0 = vec_sro(v_sums_temp_col0, v_sums_shamt) ;
                v_sums_temp_col0 = vec_or(v_sum_0, v_sums_temp_col0) ;

                v_sums_temp_col1 = vec_sro(v_sums_temp_col1, v_sums_shamt) ;
                v_sums_temp_col1 = vec_or(v_sum_1, v_sums_temp_col1) ;

                v_sums_temp_col2 = vec_sro(v_sums_temp_col2, v_sums_shamt) ;
                v_sums_temp_col2 = vec_or(v_sum_2, v_sums_temp_col2) ;

                v_sums_temp_col3 = vec_sro(v_sums_temp_col3, v_sums_shamt) ;
                v_sums_temp_col3 = vec_or(v_sum_3, v_sums_temp_col3) ;


                if(col == 3)
                {
                    v_sums_col0_0 = v_sums_temp_col0 ;
                    v_sums_col1_0 = v_sums_temp_col1 ;
                    v_sums_col2_0 = v_sums_temp_col2 ;
                    v_sums_col3_0 = v_sums_temp_col3 ;

                    v_sums_temp_col0 = v_zeros_int ;
                    v_sums_temp_col1 = v_zeros_int ;
                    v_sums_temp_col2 = v_zeros_int ;
                    v_sums_temp_col3 = v_zeros_int ;
                }


                // Shift the pixels by 1 (short pixel)
                v_pixel_short_0 = vec_sld(v_pixel_short_1, v_pixel_short_0, 14) ;
                v_pixel_short_1 = vec_sld(v_pixel_short_2, v_pixel_short_1, 14) ;
                v_pixel_short_2 = vec_sld(v_pixel_short_3, v_pixel_short_2, 14) ;
                v_pixel_short_3 = vec_sld(v_pixel_short_4, v_pixel_short_3, 14) ;
                const vector unsigned char v_shift_right_two_bytes_shamt = {0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} ;
                v_pixel_short_4 = vec_sro(v_pixel_short_4, v_shift_right_two_bytes_shamt) ;
            }

            // Copy the sums result to the second vector (per colum)
            v_sums_col0_1 = v_sums_temp_col0 ;
            v_sums_col1_1 = v_sums_temp_col1 ;
            v_sums_col2_1 = v_sums_temp_col2 ;
            v_sums_col3_1 = v_sums_temp_col3 ;
            


            // Post processing and eventually 2 stores
            // Original code:
            // int16_t val = (int16_t)((sum + offset) >> headRoom);
            // if (val < 0) val = 0;
            // if (val > maxVal) val = maxVal;
            // dst[col] = (pixel)val;


            v_sums_col0_0 = vec_sra(vec_add(v_sums_col0_0, v_offset), v_headRoom) ;
            v_sums_col0_1 = vec_sra(vec_add(v_sums_col0_1, v_offset), v_headRoom) ;
            v_sums_col1_0 = vec_sra(vec_add(v_sums_col1_0, v_offset), v_headRoom) ;
            v_sums_col1_1 = vec_sra(vec_add(v_sums_col1_1, v_offset), v_headRoom) ;
            v_sums_col2_0 = vec_sra(vec_add(v_sums_col2_0, v_offset), v_headRoom) ;
            v_sums_col2_1 = vec_sra(vec_add(v_sums_col2_1, v_offset), v_headRoom) ;
            v_sums_col3_0 = vec_sra(vec_add(v_sums_col3_0, v_offset), v_headRoom) ;
            v_sums_col3_1 = vec_sra(vec_add(v_sums_col3_1, v_offset), v_headRoom) ;

            
            vector signed short v_val_col0, v_val_col1, v_val_col2, v_val_col3 ;
            v_val_col0 = vec_pack(v_sums_col0_0, v_sums_col0_1) ;
            v_val_col1 = vec_pack(v_sums_col1_0, v_sums_col1_1) ;
            v_val_col2 = vec_pack(v_sums_col2_0, v_sums_col2_1) ;
            v_val_col3 = vec_pack(v_sums_col3_0, v_sums_col3_1) ;


            // if (val < 0) val = 0;
            vector bool short v_comp_zero_col0, v_comp_zero_col1, v_comp_zero_col2, v_comp_zero_col3 ;
            // Compute less than 0
            v_comp_zero_col0 = vec_cmplt(v_val_col0, v_zeros_short) ;
            v_comp_zero_col1 = vec_cmplt(v_val_col1, v_zeros_short) ;
            v_comp_zero_col2 = vec_cmplt(v_val_col2, v_zeros_short) ;
            v_comp_zero_col3 = vec_cmplt(v_val_col3, v_zeros_short) ;
            // Keep values that are greater or equal to 0
            v_val_col0 = vec_andc(v_val_col0, v_comp_zero_col0) ;
            v_val_col1 = vec_andc(v_val_col1, v_comp_zero_col1) ;
            v_val_col2 = vec_andc(v_val_col2, v_comp_zero_col2) ;
            v_val_col3 = vec_andc(v_val_col3, v_comp_zero_col3) ;


            // if (val > maxVal) val = maxVal;
            vector bool short v_comp_max_col0, v_comp_max_col1, v_comp_max_col2, v_comp_max_col3 ;
            const vector signed short v_maxVal = {maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal} ;
            // Compute greater than max
            v_comp_max_col0 = vec_cmpgt(v_val_col0, v_maxVal) ;
            v_comp_max_col1 = vec_cmpgt(v_val_col1, v_maxVal) ;
            v_comp_max_col2 = vec_cmpgt(v_val_col2, v_maxVal) ;
            v_comp_max_col3 = vec_cmpgt(v_val_col3, v_maxVal) ;
            // Replace values greater than maxVal with maxVal
            v_val_col0 = vec_sel(v_val_col0, v_maxVal, v_comp_max_col0) ;
            v_val_col1 = vec_sel(v_val_col1, v_maxVal, v_comp_max_col1) ;
            v_val_col2 = vec_sel(v_val_col2, v_maxVal, v_comp_max_col2) ;
            v_val_col3 = vec_sel(v_val_col3, v_maxVal, v_comp_max_col3) ;

            // (pixel)val
            vector unsigned char v_final_result_0, v_final_result_1 ;
            v_final_result_0 = vec_pack((vector unsigned short)v_val_col0, (vector unsigned short)v_val_col1) ;
            v_final_result_1 = vec_pack((vector unsigned short)v_val_col2, (vector unsigned short)v_val_col3) ;
            


            // Store results
            vec_xst(v_final_result_0, 0, dst) ;
            vec_xst(v_final_result_1, 16, dst) ;


            src += 32 ;
            dst += 32 ;

        } // end for col_iter


        src = next_src ;
        dst = next_dst ;
    }
} // interp_horiz_pp_altivec()
#else
template<int N, int width, int height>
void interp_horiz_pp_altivec(const pixel* __restrict__ src, intptr_t srcStride, pixel* __restrict__ dst, intptr_t dstStride, int coeffIdx)
{
    const int16_t* __restrict__ coeff = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
    int headRoom = IF_FILTER_PREC;
    int offset =  (1 << (headRoom - 1));
    uint16_t maxVal = (1 << X265_DEPTH) - 1;
    int cStride = 1;

    src -= (N / 2 - 1) * cStride;

    vector signed short vcoeff0     = vec_splats(coeff[0]);
    vector signed short vcoeff1     = vec_splats(coeff[1]);
    vector signed short vcoeff2     = vec_splats(coeff[2]);
    vector signed short vcoeff3     = vec_splats(coeff[3]);
    vector signed short vcoeff4     = vec_splats(coeff[4]);
    vector signed short vcoeff5     = vec_splats(coeff[5]);
    vector signed short vcoeff6     = vec_splats(coeff[6]);
    vector signed short vcoeff7     = vec_splats(coeff[7]);
    vector signed short voffset     = vec_splats((short)offset);
    vector signed short vheadRoom   = vec_splats((short)headRoom);
    vector signed short vmaxVal     = vec_splats((short)maxVal);
    vector signed short  vzero_s16  = vec_splats( (signed short)0u);;
    vector signed int    vzero_s32  = vec_splats( (signed int)0u);
    vector unsigned char vzero_u8   = vec_splats( (unsigned char)0u );

    vector signed short vsrcH, vsrcL, vsumH, vsumL;
    vector unsigned char vsrc;

    vector signed short vsrc2H, vsrc2L, vsum2H, vsum2L;
    vector unsigned char vsrc2;

    vector unsigned char vchar_to_short_maskH = {24, 0, 25, 0, 26, 0, 27, 0, 28, 0, 29, 0, 30, 0, 31, 0}; 
    vector unsigned char vchar_to_short_maskL = {16, 0, 17, 0 ,18, 0, 19, 0, 20, 0, 21, 0, 22, 0, 23, 0}; 

    const pixel* __restrict__ src2 = src+srcStride;
    pixel* __restrict__ dst2 = dst+dstStride;

    int row, col;
    for (row = 0; row < height; row+=2)
    {
        for (col = 0; col < width; col+=16)
        {
            vsrc   = vec_xl(0, (unsigned char*)&src[col + 0*cStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );

            vsumH  = vsrcH * vcoeff0;
            vsumL  = vsrcL * vcoeff0;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 1*cStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH  += vsrcH * vcoeff1;
            vsumL  += vsrcL * vcoeff1;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 2*cStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH  += vsrcH * vcoeff2;
            vsumL  += vsrcL * vcoeff2;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 3*cStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH  += vsrcH * vcoeff3;
            vsumL  += vsrcL * vcoeff3;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 4*cStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH  += vsrcH * vcoeff4;
            vsumL  += vsrcL * vcoeff4;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 5*cStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH  += vsrcH * vcoeff5;
            vsumL  += vsrcL * vcoeff5;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 6*cStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH  += vsrcH * vcoeff6;
            vsumL  += vsrcL * vcoeff6;

            vsrc   = vec_xl(0, (unsigned char*)&src[col + 7*cStride]);
            vsrcH  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskH );
            vsrcL  = (vector signed short)vec_perm( vzero_u8, vsrc, vchar_to_short_maskL );
            vsumH  += vsrcH * vcoeff7;
            vsumL  += vsrcL * vcoeff7;

            vector short vvalH = (vsumH + voffset) >> vheadRoom;
            vvalH = vec_max( vvalH, vzero_s16 );
            vvalH = vec_min( vvalH, vmaxVal   );

            vector short vvalL = (vsumL + voffset) >> vheadRoom;
            vvalL = vec_max( vvalL, vzero_s16 );
            vvalL = vec_min( vvalL, vmaxVal   );

            vector signed char vdst = vec_pack( vvalL, vvalH );
            vec_xst( vdst, 0, (signed char*)&dst[col] );



            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 0*cStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );

            vsum2H  = vsrc2H * vcoeff0;
            vsum2L  = vsrc2L * vcoeff0;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 1*cStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H  += vsrc2H * vcoeff1;
            vsum2L  += vsrc2L * vcoeff1;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 2*cStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H  += vsrc2H * vcoeff2;
            vsum2L  += vsrc2L * vcoeff2;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 3*cStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H  += vsrc2H * vcoeff3;
            vsum2L  += vsrc2L * vcoeff3;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 4*cStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H  += vsrc2H * vcoeff4;
            vsum2L  += vsrc2L * vcoeff4;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 5*cStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H  += vsrc2H * vcoeff5;
            vsum2L  += vsrc2L * vcoeff5;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 6*cStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H  += vsrc2H * vcoeff6;
            vsum2L  += vsrc2L * vcoeff6;

            vsrc2   = vec_xl(0, (unsigned char*)&src2[col + 7*cStride]);
            vsrc2H  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskH );
            vsrc2L  = (vector signed short)vec_perm( vzero_u8, vsrc2, vchar_to_short_maskL );
            vsum2H  += vsrc2H * vcoeff7;
            vsum2L  += vsrc2L * vcoeff7;

            vector short vval2H = (vsum2H + voffset) >> vheadRoom;
            vval2H = vec_max( vval2H, vzero_s16 );
            vval2H = vec_min( vval2H, vmaxVal   );

            vector short vval2L = (vsum2L + voffset) >> vheadRoom;
            vval2L = vec_max( vval2L, vzero_s16 );
            vval2L = vec_min( vval2L, vmaxVal   );

            vector signed char vdst2 = vec_pack( vval2L, vval2H );
            vec_xst( vdst2, 0, (signed char*)&dst2[col] );
        }

        src  += 2*srcStride;
        dst  += 2*dstStride;

        src2 += 2*srcStride;
        dst2 += 2*dstStride;
    }
}
#endif


// Works with the following values:
// N = 8
// width >= 32 (multiple of 32)
// any height
//template <int N, int width, int height>
//void interp_horiz_pp_altivec(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx)
//{
//
//    const int16_t* coeff = (N == 4) ? g_chromaFilter[coeffIdx] : g_lumaFilter[coeffIdx];
//    int headRoom = IF_FILTER_PREC;
//    int offset =  (1 << (headRoom - 1));
//    uint16_t maxVal = (1 << X265_DEPTH) - 1;
//    int cStride = 1;
//
//    src -= (N / 2 - 1) * cStride;
//
//
//    vector signed short v_coeff ;
//    v_coeff = vec_xl(0, coeff) ;
//
//
//    vector unsigned char v_pixel_char_0, v_pixel_char_1, v_pixel_char_2 ;
//    vector signed short v_pixel_short_0, v_pixel_short_1, v_pixel_short_2, v_pixel_short_3, v_pixel_short_4 ;
//    const vector signed short v_mask_unisgned_char_to_short = {0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF, 0x00FF} ;
//    const vector signed int v_zeros_int = {0, 0, 0, 0} ;
//    const vector signed short v_zeros_short = {0, 0, 0, 0, 0, 0, 0, 0} ;
//
//    vector signed int v_product_0_0, v_product_0_1 ;
//    vector signed int v_product_1_0, v_product_1_1 ;
//    vector signed int v_product_2_0, v_product_2_1 ;
//    vector signed int v_product_3_0, v_product_3_1 ;
//
//    vector signed int v_sum_0, v_sum_1, v_sum_2, v_sum_3 ;
//    
//    vector signed int v_sums_temp_col0, v_sums_temp_col1, v_sums_temp_col2, v_sums_temp_col3 ;
//    vector signed int v_sums_col0_0, v_sums_col0_1 ;
//    vector signed int v_sums_col1_0, v_sums_col1_1 ;
//    vector signed int v_sums_col2_0, v_sums_col2_1 ;
//    vector signed int v_sums_col3_0, v_sums_col3_1 ;
//
//
//    const vector signed int v_offset = {offset, offset, offset, offset};
//    const vector unsigned int v_headRoom = {headRoom, headRoom, headRoom, headRoom} ;
//
//
//    vector unsigned char v_sums_shamt = {0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} ;
//
//
//    pixel *next_src ; 
//    pixel *next_dst ;
//
//    int row, col;
//    for (row = 0; row < height; row++)
//    {
//        next_src = (pixel *)src + srcStride ;
//        next_dst = (pixel *)dst + dstStride ;
//
//        for(int col_iter=0; col_iter<width; col_iter+=32)
//        {
//
//            // Load a full row of pixels (32 + 7)
//            v_pixel_char_0 = vec_xl(0, src) ;
//            v_pixel_char_1 = vec_xl(16, src) ;
//            v_pixel_char_2 = vec_xl(32, src) ;
//
//
//            v_sums_temp_col0 = v_zeros_int ;
//            v_sums_temp_col1 = v_zeros_int ;
//            v_sums_temp_col2 = v_zeros_int ;
//            v_sums_temp_col3 = v_zeros_int ;
//            
//
//            // Expand the loaded pixels into shorts
//            v_pixel_short_0 = vec_unpackh((vector signed char)v_pixel_char_0) ;
//            v_pixel_short_1 = vec_unpackl((vector signed char)v_pixel_char_0) ;
//            v_pixel_short_2 = vec_unpackh((vector signed char)v_pixel_char_1) ;
//            v_pixel_short_3 = vec_unpackl((vector signed char)v_pixel_char_1) ;
//            v_pixel_short_4 = vec_unpackh((vector signed char)v_pixel_char_2) ;
//
//            v_pixel_short_0 = vec_and(v_pixel_short_0, v_mask_unisgned_char_to_short) ;
//            v_pixel_short_1 = vec_and(v_pixel_short_1, v_mask_unisgned_char_to_short) ;
//            v_pixel_short_2 = vec_and(v_pixel_short_2, v_mask_unisgned_char_to_short) ;
//            v_pixel_short_3 = vec_and(v_pixel_short_3, v_mask_unisgned_char_to_short) ;
//            v_pixel_short_4 = vec_and(v_pixel_short_4, v_mask_unisgned_char_to_short) ;
//
//
//            
//            // Four colum sets are processed below
//            // One colum per set per iteration
//            for(col=0; col < 8; col++)
//            {
//
//                // Multiply the pixels by the coefficients
//                v_product_0_0 = vec_mule(v_pixel_short_0, v_coeff) ;
//                v_product_0_1 = vec_mulo(v_pixel_short_0, v_coeff) ;
//                
//                v_product_1_0 = vec_mule(v_pixel_short_1, v_coeff) ;
//                v_product_1_1 = vec_mulo(v_pixel_short_1, v_coeff) ;
//                
//                v_product_2_0 = vec_mule(v_pixel_short_2, v_coeff) ;
//                v_product_2_1 = vec_mulo(v_pixel_short_2, v_coeff) ;
//                
//                v_product_3_0 = vec_mule(v_pixel_short_3, v_coeff) ;
//                v_product_3_1 = vec_mulo(v_pixel_short_3, v_coeff) ;
//
//
//                // Sum up the multiplication results
//                v_sum_0 = vec_add(v_product_0_0, v_product_0_1) ;
//                v_sum_0 = vec_sums(v_sum_0, v_zeros_int) ;
//
//                v_sum_1 = vec_add(v_product_1_0, v_product_1_1) ;
//                v_sum_1 = vec_sums(v_sum_1, v_zeros_int) ;
//
//                v_sum_2 = vec_add(v_product_2_0, v_product_2_1) ;
//                v_sum_2 = vec_sums(v_sum_2, v_zeros_int) ;
//
//                v_sum_3 = vec_add(v_product_3_0, v_product_3_1) ;
//                v_sum_3 = vec_sums(v_sum_3, v_zeros_int) ;
//
//
//                // Insert the sum results into respective vectors
//                v_sums_temp_col0 = vec_sro(v_sums_temp_col0, v_sums_shamt) ;
//                v_sums_temp_col0 = vec_or(v_sum_0, v_sums_temp_col0) ;
//
//                v_sums_temp_col1 = vec_sro(v_sums_temp_col1, v_sums_shamt) ;
//                v_sums_temp_col1 = vec_or(v_sum_1, v_sums_temp_col1) ;
//
//                v_sums_temp_col2 = vec_sro(v_sums_temp_col2, v_sums_shamt) ;
//                v_sums_temp_col2 = vec_or(v_sum_2, v_sums_temp_col2) ;
//
//                v_sums_temp_col3 = vec_sro(v_sums_temp_col3, v_sums_shamt) ;
//                v_sums_temp_col3 = vec_or(v_sum_3, v_sums_temp_col3) ;
//
//
//                if(col == 3)
//                {
//                    v_sums_col0_0 = v_sums_temp_col0 ;
//                    v_sums_col1_0 = v_sums_temp_col1 ;
//                    v_sums_col2_0 = v_sums_temp_col2 ;
//                    v_sums_col3_0 = v_sums_temp_col3 ;
//
//                    v_sums_temp_col0 = v_zeros_int ;
//                    v_sums_temp_col1 = v_zeros_int ;
//                    v_sums_temp_col2 = v_zeros_int ;
//                    v_sums_temp_col3 = v_zeros_int ;
//                }
//
//
//                // Shift the pixels by 1 (short pixel)
//                v_pixel_short_0 = vec_sld(v_pixel_short_1, v_pixel_short_0, 14) ;
//                v_pixel_short_1 = vec_sld(v_pixel_short_2, v_pixel_short_1, 14) ;
//                v_pixel_short_2 = vec_sld(v_pixel_short_3, v_pixel_short_2, 14) ;
//                v_pixel_short_3 = vec_sld(v_pixel_short_4, v_pixel_short_3, 14) ;
//                const vector unsigned char v_shift_right_two_bytes_shamt = {0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} ;
//                v_pixel_short_4 = vec_sro(v_pixel_short_4, v_shift_right_two_bytes_shamt) ;
//            }
//
//            // Copy the sums result to the second vector (per colum)
//            v_sums_col0_1 = v_sums_temp_col0 ;
//            v_sums_col1_1 = v_sums_temp_col1 ;
//            v_sums_col2_1 = v_sums_temp_col2 ;
//            v_sums_col3_1 = v_sums_temp_col3 ;
//            
//
//
//            // Post processing and eventually 2 stores
//            // Original code:
//            // int16_t val = (int16_t)((sum + offset) >> headRoom);
//            // if (val < 0) val = 0;
//            // if (val > maxVal) val = maxVal;
//            // dst[col] = (pixel)val;
//
//
//            v_sums_col0_0 = vec_sra(vec_add(v_sums_col0_0, v_offset), v_headRoom) ;
//            v_sums_col0_1 = vec_sra(vec_add(v_sums_col0_1, v_offset), v_headRoom) ;
//            v_sums_col1_0 = vec_sra(vec_add(v_sums_col1_0, v_offset), v_headRoom) ;
//            v_sums_col1_1 = vec_sra(vec_add(v_sums_col1_1, v_offset), v_headRoom) ;
//            v_sums_col2_0 = vec_sra(vec_add(v_sums_col2_0, v_offset), v_headRoom) ;
//            v_sums_col2_1 = vec_sra(vec_add(v_sums_col2_1, v_offset), v_headRoom) ;
//            v_sums_col3_0 = vec_sra(vec_add(v_sums_col3_0, v_offset), v_headRoom) ;
//            v_sums_col3_1 = vec_sra(vec_add(v_sums_col3_1, v_offset), v_headRoom) ;
//
//            
//            vector signed short v_val_col0, v_val_col1, v_val_col2, v_val_col3 ;
//            v_val_col0 = vec_pack(v_sums_col0_0, v_sums_col0_1) ;
//            v_val_col1 = vec_pack(v_sums_col1_0, v_sums_col1_1) ;
//            v_val_col2 = vec_pack(v_sums_col2_0, v_sums_col2_1) ;
//            v_val_col3 = vec_pack(v_sums_col3_0, v_sums_col3_1) ;
//
//
//            // if (val < 0) val = 0;
//            vector bool short v_comp_zero_col0, v_comp_zero_col1, v_comp_zero_col2, v_comp_zero_col3 ;
//            // Compute less than 0
//            v_comp_zero_col0 = vec_cmplt(v_val_col0, v_zeros_short) ;
//            v_comp_zero_col1 = vec_cmplt(v_val_col1, v_zeros_short) ;
//            v_comp_zero_col2 = vec_cmplt(v_val_col2, v_zeros_short) ;
//            v_comp_zero_col3 = vec_cmplt(v_val_col3, v_zeros_short) ;
//            // Keep values that are greater or equal to 0
//            v_val_col0 = vec_andc(v_val_col0, v_comp_zero_col0) ;
//            v_val_col1 = vec_andc(v_val_col1, v_comp_zero_col1) ;
//            v_val_col2 = vec_andc(v_val_col2, v_comp_zero_col2) ;
//            v_val_col3 = vec_andc(v_val_col3, v_comp_zero_col3) ;
//
//
//            // if (val > maxVal) val = maxVal;
//            vector bool short v_comp_max_col0, v_comp_max_col1, v_comp_max_col2, v_comp_max_col3 ;
//            const vector signed short v_maxVal = {maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal} ;
//            // Compute greater than max
//            v_comp_max_col0 = vec_cmpgt(v_val_col0, v_maxVal) ;
//            v_comp_max_col1 = vec_cmpgt(v_val_col1, v_maxVal) ;
//            v_comp_max_col2 = vec_cmpgt(v_val_col2, v_maxVal) ;
//            v_comp_max_col3 = vec_cmpgt(v_val_col3, v_maxVal) ;
//            // Replace values greater than maxVal with maxVal
//            v_val_col0 = vec_sel(v_val_col0, v_maxVal, v_comp_max_col0) ;
//            v_val_col1 = vec_sel(v_val_col1, v_maxVal, v_comp_max_col1) ;
//            v_val_col2 = vec_sel(v_val_col2, v_maxVal, v_comp_max_col2) ;
//            v_val_col3 = vec_sel(v_val_col3, v_maxVal, v_comp_max_col3) ;
//
//            // (pixel)val
//            vector unsigned char v_final_result_0, v_final_result_1 ;
//            v_final_result_0 = vec_pack((vector unsigned short)v_val_col0, (vector unsigned short)v_val_col1) ;
//            v_final_result_1 = vec_pack((vector unsigned short)v_val_col2, (vector unsigned short)v_val_col3) ;
//            
//
//
//            // Store results
//            vec_xst(v_final_result_0, 0, dst) ;
//            vec_xst(v_final_result_1, 16, dst) ;
//
//
//            src += 32 ;
//            dst += 32 ;
//
//        } // end for col_iter
//
//
//        src = next_src ;
//        dst = next_dst ;
//    }
//} // interp_horiz_pp_altivec()


namespace X265_NS {

void setupFilterPrimitives_altivec(EncoderPrimitives& p)
{
    // interp_vert_pp_c
    p.pu[LUMA_16x16].luma_vpp   = interp_vert_pp_altivec<8, 16, 16> ;
    p.pu[LUMA_32x8].luma_vpp    = interp_vert_pp_altivec<8, 32, 8> ;
    p.pu[LUMA_16x12].luma_vpp   = interp_vert_pp_altivec<8, 16, 12> ;
    p.pu[LUMA_16x4].luma_vpp    = interp_vert_pp_altivec<8, 16, 4> ;
    p.pu[LUMA_32x32].luma_vpp   = interp_vert_pp_altivec<8, 32, 32> ;
    p.pu[LUMA_32x16].luma_vpp   = interp_vert_pp_altivec<8, 32, 16> ;
    p.pu[LUMA_16x32].luma_vpp   = interp_vert_pp_altivec<8, 16, 32> ;
    p.pu[LUMA_32x24].luma_vpp   = interp_vert_pp_altivec<8, 32, 24> ;
    p.pu[LUMA_32x8].luma_vpp    = interp_vert_pp_altivec<8, 32, 8> ;
    p.pu[LUMA_64x64].luma_vpp   = interp_vert_pp_altivec<8, 64, 64> ;
    p.pu[LUMA_64x32].luma_vpp   = interp_vert_pp_altivec<8, 64, 32> ;
    p.pu[LUMA_32x64].luma_vpp   = interp_vert_pp_altivec<8, 32, 64> ;
    p.pu[LUMA_64x48].luma_vpp   = interp_vert_pp_altivec<8, 64, 48> ;
    p.pu[LUMA_48x64].luma_vpp   = interp_vert_pp_altivec<8, 48, 64> ;
    p.pu[LUMA_64x16].luma_vpp   = interp_vert_pp_altivec<8, 64, 16> ;
    p.pu[LUMA_16x64].luma_vpp   = interp_vert_pp_altivec<8, 16, 64> ;

    // interp_hv_pp_c
    p.pu[LUMA_32x32].luma_hvpp   = interp_hv_pp_altivec<8, 32, 32> ;
    p.pu[LUMA_32x16].luma_hvpp   = interp_hv_pp_altivec<8, 32, 16> ;
    p.pu[LUMA_32x24].luma_hvpp   = interp_hv_pp_altivec<8, 32, 24> ;
    p.pu[LUMA_32x8].luma_hvpp    = interp_hv_pp_altivec<8, 32, 8> ;
    p.pu[LUMA_64x64].luma_hvpp   = interp_hv_pp_altivec<8, 64, 64> ;
    p.pu[LUMA_64x32].luma_hvpp   = interp_hv_pp_altivec<8, 64, 32> ;
    p.pu[LUMA_32x64].luma_hvpp   = interp_hv_pp_altivec<8, 32, 64> ;
    p.pu[LUMA_64x48].luma_hvpp   = interp_hv_pp_altivec<8, 64, 48> ;
    p.pu[LUMA_64x16].luma_hvpp   = interp_hv_pp_altivec<8, 64, 16> ;

    // interp_horiz_pp_c
    p.pu[LUMA_32x32].luma_hpp   = interp_horiz_pp_altivec<8, 32, 32> ;
    p.pu[LUMA_32x16].luma_hpp   = interp_horiz_pp_altivec<8, 32, 16> ;
    p.pu[LUMA_32x24].luma_hpp   = interp_horiz_pp_altivec<8, 32, 24> ;
    p.pu[LUMA_32x8].luma_hpp    = interp_horiz_pp_altivec<8, 32, 8> ;
    p.pu[LUMA_64x64].luma_hpp   = interp_horiz_pp_altivec<8, 64, 64> ;
    p.pu[LUMA_64x32].luma_hpp   = interp_horiz_pp_altivec<8, 64, 32> ;
    p.pu[LUMA_32x64].luma_hpp   = interp_horiz_pp_altivec<8, 32, 64> ;
    p.pu[LUMA_64x48].luma_hpp   = interp_horiz_pp_altivec<8, 64, 48> ;
    p.pu[LUMA_64x16].luma_hpp   = interp_horiz_pp_altivec<8, 64, 16> ;
}

} // end namespace X265_NS
