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

#include "common.h"
#include "primitives.h"
#include "contexts.h"   // costCoeffNxN_c
#include "threading.h"  // CLZ
#include "ppccommon.h"

using namespace X265_NS;

static uint32_t quant_altivec(const int16_t* coef, const int32_t* quantCoeff, int32_t* deltaU, int16_t* qCoef, int qBits, int add, int numCoeff)
{

    X265_CHECK(qBits >= 8, "qBits less than 8\n");

    X265_CHECK((numCoeff % 16) == 0, "numCoeff must be multiple of 16\n");

    int qBits8 = qBits - 8;
    uint32_t numSig = 0;


    int level[8] ;
    int sign[8] ;
    int tmplevel[8] ;

    const vector signed short v_zeros = {0, 0, 0, 0, 0, 0, 0, 0} ;
    const vector signed short v_neg1 = {-1, -1, -1, -1, -1, -1, -1, -1} ;
    const vector signed short v_pos1_ss = {1, 1, 1, 1, 1, 1, 1, 1} ;
    const vector signed int v_pos1_sw = {1, 1, 1, 1} ;

    const vector signed int v_clip_high = {32767, 32767, 32767, 32767} ;
    const vector signed int v_clip_low = {-32768, -32768, -32768, -32768} ;


    vector signed short v_level_ss ;
    vector signed int v_level_0, v_level_1 ;
    vector signed int v_tmplevel_0, v_tmplevel_1 ;
    vector signed short v_sign_ss ;
    vector signed int v_sign_0, v_sign_1 ;
    vector signed int v_quantCoeff_0, v_quantCoeff_1 ;

    vector signed int v_numSig = {0, 0, 0, 0} ;

    vector signed int v_add ;
    v_add[0] = add ;
    v_add = vec_splat(v_add, 0) ;

    vector unsigned int v_qBits ;
    v_qBits[0] = qBits ;
    v_qBits = vec_splat(v_qBits, 0) ;

    vector unsigned int v_qBits8 ;
    v_qBits8[0] = qBits8 ;
    v_qBits8 = vec_splat(v_qBits8, 0) ;


    for (int blockpos_outer = 0; blockpos_outer < numCoeff; blockpos_outer+=16)
    {
        int blockpos = blockpos_outer ;

        // for(int ii=0; ii<8; ii++) { level[ii] = coef[blockpos+ii] ;}
        v_level_ss = vec_xl(0, &coef[blockpos]) ;
        v_level_0 = vec_unpackh(v_level_ss) ;
        v_level_1 = vec_unpackl(v_level_ss) ;


        // for(int ii=0; ii<8; ii++) { sign[ii] = (level[ii] < 0 ? -1 : 1) ;}
        vector bool short v_level_cmplt0 ;
        v_level_cmplt0 = vec_cmplt(v_level_ss, v_zeros) ;
        v_sign_ss = vec_sel(v_pos1_ss, v_neg1, v_level_cmplt0) ;
        v_sign_0 = vec_unpackh(v_sign_ss) ;
        v_sign_1 = vec_unpackl(v_sign_ss) ;
        
        

        // for(int ii=0; ii<8; ii++) { tmplevel[ii] = abs(level[ii]) * quantCoeff[blockpos+ii] ;}
        v_level_0 = vec_abs(v_level_0) ;
        v_level_1 = vec_abs(v_level_1) ;
        v_quantCoeff_0 = vec_xl(0, &quantCoeff[blockpos]) ;
        v_quantCoeff_1 = vec_xl(16, &quantCoeff[blockpos]) ;
        
        asm ("vmuluwm %0,%1,%2"
              : "=v" (v_tmplevel_0)
              : "v"  (v_level_0) , "v" (v_quantCoeff_0)
            ) ;

        asm ("vmuluwm %0,%1,%2"
              : "=v" (v_tmplevel_1)
              : "v"  (v_level_1) , "v" (v_quantCoeff_1)
            ) ;



        // for(int ii=0; ii<8; ii++) { level[ii] = ((tmplevel[ii] + add) >> qBits) ;}
        v_level_0 = vec_sra(vec_add(v_tmplevel_0, v_add), v_qBits) ;
        v_level_1 = vec_sra(vec_add(v_tmplevel_1, v_add), v_qBits) ;

        // for(int ii=0; ii<8; ii++) { deltaU[blockpos+ii] = ((tmplevel[ii] - (level[ii] << qBits)) >> qBits8) ;} 
        vector signed int v_temp_0_sw, v_temp_1_sw ;
        v_temp_0_sw = vec_sl(v_level_0, v_qBits) ;
        v_temp_1_sw = vec_sl(v_level_1, v_qBits) ;

        v_temp_0_sw = vec_sub(v_tmplevel_0, v_temp_0_sw) ;
        v_temp_1_sw = vec_sub(v_tmplevel_1, v_temp_1_sw) ;

        v_temp_0_sw = vec_sra(v_temp_0_sw, v_qBits8) ;
        v_temp_1_sw = vec_sra(v_temp_1_sw, v_qBits8) ;

        vec_xst(v_temp_0_sw, 0, &deltaU[blockpos]) ;
        vec_xst(v_temp_1_sw, 16, &deltaU[blockpos]) ;


        // for(int ii=0; ii<8; ii++) { if(level[ii]) ++numSig ; }
        vector bool int v_level_cmpeq0 ;
        vector signed int v_level_inc ;
        v_level_cmpeq0 = vec_cmpeq(v_level_0, (vector signed int)v_zeros) ;
        v_level_inc = vec_sel(v_pos1_sw, (vector signed int)v_zeros, v_level_cmpeq0) ;
        v_numSig = vec_add(v_numSig, v_level_inc) ;

        v_level_cmpeq0 = vec_cmpeq(v_level_1, (vector signed int)v_zeros) ;
        v_level_inc = vec_sel(v_pos1_sw, (vector signed int)v_zeros, v_level_cmpeq0) ;
        v_numSig = vec_add(v_numSig, v_level_inc) ;


        // for(int ii=0; ii<8; ii++) { level[ii] *= sign[ii]; }
        asm ("vmuluwm %0,%1,%2"
              : "=v" (v_level_0)
              : "v"  (v_level_0) , "v" (v_sign_0)
            ) ;

        asm ("vmuluwm %0,%1,%2"
              : "=v" (v_level_1)
              : "v"  (v_level_1) , "v" (v_sign_1)
            ) ;



        // for(int ii=0; ii<8; ii++) {qCoef[blockpos+ii] = (int16_t)x265_clip3(-32768, 32767, level[ii]);}
        vector bool int v_level_cmp_clip_high, v_level_cmp_clip_low ;

        v_level_cmp_clip_high = vec_cmpgt(v_level_0, v_clip_high) ;
        v_level_0 = vec_sel(v_level_0, v_clip_high, v_level_cmp_clip_high) ;
        v_level_cmp_clip_low = vec_cmplt(v_level_0, v_clip_low) ;
        v_level_0 = vec_sel(v_level_0, v_clip_low, v_level_cmp_clip_low) ;


        v_level_cmp_clip_high = vec_cmpgt(v_level_1, v_clip_high) ;
        v_level_1 = vec_sel(v_level_1, v_clip_high, v_level_cmp_clip_high) ;
        v_level_cmp_clip_low = vec_cmplt(v_level_1, v_clip_low) ;
        v_level_1 = vec_sel(v_level_1, v_clip_low, v_level_cmp_clip_low) ;

        v_level_ss = vec_pack(v_level_0, v_level_1) ;

        vec_xst(v_level_ss, 0, &qCoef[blockpos]) ;




        // UNROLL ONCE MORE (which is ok since loops for multiple of 16 times, though that is NOT obvious to the compiler)
        blockpos += 8 ;

        // for(int ii=0; ii<8; ii++) { level[ii] = coef[blockpos+ii] ;}
        v_level_ss = vec_xl(0, &coef[blockpos]) ;
        v_level_0 = vec_unpackh(v_level_ss) ;
        v_level_1 = vec_unpackl(v_level_ss) ;


        // for(int ii=0; ii<8; ii++) { sign[ii] = (level[ii] < 0 ? -1 : 1) ;}
        v_level_cmplt0 = vec_cmplt(v_level_ss, v_zeros) ;
        v_sign_ss = vec_sel(v_pos1_ss, v_neg1, v_level_cmplt0) ;
        v_sign_0 = vec_unpackh(v_sign_ss) ;
        v_sign_1 = vec_unpackl(v_sign_ss) ;
        
        

        // for(int ii=0; ii<8; ii++) { tmplevel[ii] = abs(level[ii]) * quantCoeff[blockpos+ii] ;}
        v_level_0 = vec_abs(v_level_0) ;
        v_level_1 = vec_abs(v_level_1) ;
        v_quantCoeff_0 = vec_xl(0, &quantCoeff[blockpos]) ;
        v_quantCoeff_1 = vec_xl(16, &quantCoeff[blockpos]) ;
        
        asm ("vmuluwm %0,%1,%2"
              : "=v" (v_tmplevel_0)
              : "v"  (v_level_0) , "v" (v_quantCoeff_0)
            ) ;

        asm ("vmuluwm %0,%1,%2"
              : "=v" (v_tmplevel_1)
              : "v"  (v_level_1) , "v" (v_quantCoeff_1)
            ) ;



        // for(int ii=0; ii<8; ii++) { level[ii] = ((tmplevel[ii] + add) >> qBits) ;}
        v_level_0 = vec_sra(vec_add(v_tmplevel_0, v_add), v_qBits) ;
        v_level_1 = vec_sra(vec_add(v_tmplevel_1, v_add), v_qBits) ;

        // for(int ii=0; ii<8; ii++) { deltaU[blockpos+ii] = ((tmplevel[ii] - (level[ii] << qBits)) >> qBits8) ;} 
        v_temp_0_sw = vec_sl(v_level_0, v_qBits) ;
        v_temp_1_sw = vec_sl(v_level_1, v_qBits) ;

        v_temp_0_sw = vec_sub(v_tmplevel_0, v_temp_0_sw) ;
        v_temp_1_sw = vec_sub(v_tmplevel_1, v_temp_1_sw) ;

        v_temp_0_sw = vec_sra(v_temp_0_sw, v_qBits8) ;
        v_temp_1_sw = vec_sra(v_temp_1_sw, v_qBits8) ;

        vec_xst(v_temp_0_sw, 0, &deltaU[blockpos]) ;
        vec_xst(v_temp_1_sw, 16, &deltaU[blockpos]) ;


        // for(int ii=0; ii<8; ii++) { if(level[ii]) ++numSig ; }
        v_level_cmpeq0 = vec_cmpeq(v_level_0, (vector signed int)v_zeros) ;
        v_level_inc = vec_sel(v_pos1_sw, (vector signed int)v_zeros, v_level_cmpeq0) ;
        v_numSig = vec_add(v_numSig, v_level_inc) ;

        v_level_cmpeq0 = vec_cmpeq(v_level_1, (vector signed int)v_zeros) ;
        v_level_inc = vec_sel(v_pos1_sw, (vector signed int)v_zeros, v_level_cmpeq0) ;
        v_numSig = vec_add(v_numSig, v_level_inc) ;


        // for(int ii=0; ii<8; ii++) { level[ii] *= sign[ii]; }
        asm ("vmuluwm %0,%1,%2"
              : "=v" (v_level_0)
              : "v"  (v_level_0) , "v" (v_sign_0)
            ) ;

        asm ("vmuluwm %0,%1,%2"
              : "=v" (v_level_1)
              : "v"  (v_level_1) , "v" (v_sign_1)
            ) ;



        // for(int ii=0; ii<8; ii++) {qCoef[blockpos+ii] = (int16_t)x265_clip3(-32768, 32767, level[ii]);}
        v_level_cmp_clip_high = vec_cmpgt(v_level_0, v_clip_high) ;
        v_level_0 = vec_sel(v_level_0, v_clip_high, v_level_cmp_clip_high) ;
        v_level_cmp_clip_low = vec_cmplt(v_level_0, v_clip_low) ;
        v_level_0 = vec_sel(v_level_0, v_clip_low, v_level_cmp_clip_low) ;


        v_level_cmp_clip_high = vec_cmpgt(v_level_1, v_clip_high) ;
        v_level_1 = vec_sel(v_level_1, v_clip_high, v_level_cmp_clip_high) ;
        v_level_cmp_clip_low = vec_cmplt(v_level_1, v_clip_low) ;
        v_level_1 = vec_sel(v_level_1, v_clip_low, v_level_cmp_clip_low) ;

        v_level_ss = vec_pack(v_level_0, v_level_1) ;

        vec_xst(v_level_ss, 0, &qCoef[blockpos]) ;


    }

    v_numSig = vec_sums(v_numSig, (vector signed int)v_zeros) ;

    // return numSig;
    return v_numSig[3] ;
} // end quant_altivec()


inline void denoiseDct_unroll8_altivec(int16_t* dctCoef, uint32_t* resSum, const uint16_t* offset, int numCoeff, int index_offset)
{
    vector short v_level_ss, v_sign_ss ;
    vector int v_level_h_sw, v_level_l_sw ;
    vector int v_level_h_processed_sw, v_level_l_processed_sw ;
    vector int v_sign_h_sw, v_sign_l_sw ;
    vector unsigned int v_resSum_h_uw, v_resSum_l_uw ;
    vector unsigned short v_offset_us ;
    vector unsigned int v_offset_h_uw, v_offset_l_uw ;
    const vector unsigned short v_shamt_us = {15,15,15,15,15,15,15,15} ;
    const vector unsigned int v_unpack_mask = {0x0FFFF, 0x0FFFF, 0x0FFFF, 0x0FFFF} ;
    vector bool int vec_less_than_zero_h_bw, vec_less_than_zero_l_bw ;
    LOAD_ZERO;

    // for(int jj=0; jj<8; jj++) v_level[jj]=dctCoef[ii*8 + jj] ;
    v_level_ss = vec_xl(0, &dctCoef[index_offset]) ;
    v_level_h_sw = vec_unpackh(v_level_ss) ;
    v_level_l_sw = vec_unpackl(v_level_ss) ;
    
    // for(int jj=0; jj<8; jj++) v_sign[jj] = v_level[jj] >> 31 ;
    v_sign_ss = vec_sra(v_level_ss, v_shamt_us) ;
    v_sign_h_sw = vec_unpackh(v_sign_ss) ;
    v_sign_l_sw = vec_unpackl(v_sign_ss) ;



    // for(int jj=0; jj<8; jj++) v_level[jj] = (v_level[jj] + v_sign[jj]) ^ v_sign[jj] ;
    v_level_h_sw = vec_add(v_level_h_sw, v_sign_h_sw) ;
    v_level_l_sw = vec_add(v_level_l_sw, v_sign_l_sw) ;
    
    v_level_h_sw = vec_xor(v_level_h_sw, v_sign_h_sw) ;
    v_level_l_sw = vec_xor(v_level_l_sw, v_sign_l_sw) ;



    // for(int jj=0; jj<8; jj++) resSum[ii*8 + jj] += v_level[jj] ;
    v_resSum_h_uw = vec_xl(0, &resSum[index_offset]) ;
    v_resSum_l_uw = vec_xl(0, &resSum[index_offset + 4]) ;

    v_resSum_h_uw = vec_add(v_resSum_h_uw, (vector unsigned int)v_level_h_sw) ;
    v_resSum_l_uw = vec_add(v_resSum_l_uw, (vector unsigned int)v_level_l_sw) ;

    vec_xst(v_resSum_h_uw, 0, &resSum[index_offset]) ;
    vec_xst(v_resSum_l_uw, 0, &resSum[index_offset + 4]) ;


    // for(int jj=0; jj<8; jj++) v_level[jj] -= offset[ii*8 + jj] ;
    v_offset_us = vec_xl(0, &offset[index_offset]) ;
    v_offset_h_uw = (vector unsigned int)vec_unpackh((vector signed short)v_offset_us) ;
    v_offset_l_uw = (vector unsigned int)vec_unpackl((vector signed short)v_offset_us) ;
    v_offset_h_uw = vec_and(v_offset_h_uw, v_unpack_mask) ;
    v_offset_l_uw = vec_and(v_offset_l_uw, v_unpack_mask) ;
    v_level_h_sw = vec_sub(v_level_h_sw, (vector signed int) v_offset_h_uw) ;
    v_level_l_sw = vec_sub(v_level_l_sw, (vector signed int) v_offset_l_uw) ;


    // for (int jj = 0; jj < 8; jj++)  dctCoef[ii*8 + jj] = (int16_t)(v_level[jj] < 0 ? 0 : (v_level[jj] ^ v_sign[jj]) - v_sign[jj]);
    // (level ^ sign) - sign
    v_level_h_processed_sw = vec_xor(v_level_h_sw, v_sign_h_sw) ;
    v_level_l_processed_sw = vec_xor(v_level_l_sw, v_sign_l_sw) ;
    v_level_h_processed_sw = vec_sub(v_level_h_processed_sw, v_sign_h_sw) ;
    v_level_l_processed_sw = vec_sub(v_level_l_processed_sw, v_sign_l_sw) ;

    //vec_less_than_zero_h_bw = vec_cmplt(v_level_h_sw, (vector signed int){0, 0, 0, 0}) ;
    //vec_less_than_zero_l_bw = vec_cmplt(v_level_l_sw, (vector signed int){0, 0, 0, 0}) ;
    vec_less_than_zero_h_bw = vec_cmplt(v_level_h_sw, zero_s32v) ;
    vec_less_than_zero_l_bw = vec_cmplt(v_level_l_sw, zero_s32v) ;

    v_level_h_sw = vec_sel(v_level_h_processed_sw, (vector signed int){0, 0, 0, 0}, vec_less_than_zero_h_bw) ;
    v_level_l_sw = vec_sel(v_level_l_processed_sw, (vector signed int){0, 0, 0, 0}, vec_less_than_zero_l_bw) ;

    v_level_ss = vec_pack(v_level_h_sw, v_level_l_sw) ;

    vec_xst(v_level_ss, 0, &dctCoef[index_offset]) ;
}


void denoiseDct_altivec(int16_t* dctCoef, uint32_t* resSum, const uint16_t* offset, int numCoeff)
{
    int ii_offset ;
    
    // For each set of 256
    for(int ii=0; ii<(numCoeff/256); ii++)
    {
        #pragma unroll
        for(int jj=0; jj<32; jj++)
        {
            denoiseDct_unroll8_altivec(dctCoef, resSum, offset, numCoeff, ii*256 + jj*8) ;
        }
    }

    ii_offset = ((numCoeff >> 8) << 8) ;

    // For each set of 64
    for(int ii=0; ii<((numCoeff%256) /64); ii++)
    {
        #pragma unroll
        for(int jj=0; jj<8; jj++)
        {
            denoiseDct_unroll8_altivec(dctCoef, resSum, offset, numCoeff, ii_offset + ii*64 + jj*8) ;
        }
    }


    ii_offset = ((numCoeff >> 6) << 6) ;

    // For each set of 8
    for(int ii=0; ii < ((numCoeff%64) /8); ii++)
    {
        denoiseDct_unroll8_altivec(dctCoef, resSum, offset, numCoeff, ii_offset + (ii*8)) ;
    }

    
    ii_offset = ((numCoeff >> 3) << 3) ;

    for (int ii = 0; ii < (numCoeff % 8); ii++)
    {
        int level = dctCoef[ii + ii_offset];
        int sign = level >> 31;
        level = (level + sign) ^ sign;
        resSum[ii+ii_offset] += level;
        level -= offset[ii+ii_offset] ;
        dctCoef[ii+ii_offset] = (int16_t)(level < 0 ? 0 : (level ^ sign) - sign);
    }

} // end denoiseDct_altivec()




inline void transpose_matrix_8_altivec(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
{
    vector signed short v_src_0 ;
    vector signed short v_src_1 ;
    vector signed short v_src_2 ;
    vector signed short v_src_3 ;
    vector signed short v_src_4 ;
    vector signed short v_src_5 ;
    vector signed short v_src_6 ;
    vector signed short v_src_7 ;

    vector signed short v_dst_32s_0 ;
    vector signed short v_dst_32s_1 ;
    vector signed short v_dst_32s_2 ;
    vector signed short v_dst_32s_3 ;
    vector signed short v_dst_32s_4 ;
    vector signed short v_dst_32s_5 ;
    vector signed short v_dst_32s_6 ;
    vector signed short v_dst_32s_7 ;

    vector signed short v_dst_64s_0 ;
    vector signed short v_dst_64s_1 ;
    vector signed short v_dst_64s_2 ;
    vector signed short v_dst_64s_3 ;
    vector signed short v_dst_64s_4 ;
    vector signed short v_dst_64s_5 ;
    vector signed short v_dst_64s_6 ;
    vector signed short v_dst_64s_7 ;
    
    vector signed short v_dst_128s_0 ;
    vector signed short v_dst_128s_1 ;
    vector signed short v_dst_128s_2 ;
    vector signed short v_dst_128s_3 ;
    vector signed short v_dst_128s_4 ;
    vector signed short v_dst_128s_5 ;
    vector signed short v_dst_128s_6 ;
    vector signed short v_dst_128s_7 ;

    v_src_0 = vec_xl(0, src) ;
    v_src_1 = vec_xl( (srcStride*2) , src) ;
    v_src_2 = vec_xl( (srcStride*2) * 2, src) ;
    v_src_3 = vec_xl( (srcStride*2) * 3, src) ;
    v_src_4 = vec_xl( (srcStride*2) * 4, src) ;
    v_src_5 = vec_xl( (srcStride*2) * 5, src) ;
    v_src_6 = vec_xl( (srcStride*2) * 6, src) ;
    v_src_7 = vec_xl( (srcStride*2) * 7, src) ;
    
    vector unsigned char v_permute_32s_high = {0x00, 0x01, 0x10, 0x11, 0x02, 0x03, 0x12, 0x13, 0x04, 0x05, 0x14, 0x15, 0x06, 0x07, 0x16, 0x17} ;
    vector unsigned char v_permute_32s_low = {0x08, 0x09, 0x18, 0x19, 0x0A, 0x0B, 0x1A, 0x1B, 0x0C, 0x0D, 0x1C, 0x1D, 0x0E, 0x0F, 0x1E, 0x1F} ;
    vector unsigned char v_permute_64s_high = {0x00, 0x01, 0x02, 0x03, 0x10, 0x11, 0x12, 0x13, 0x04, 0x05, 0x06, 0x07, 0x14, 0x015, 0x16, 0x17} ;
    vector unsigned char v_permute_64s_low = {0x08, 0x09, 0x0A, 0x0B, 0x18, 0x19, 0x1A, 0x1B, 0x0C, 0x0D, 0x0E, 0x0F, 0x1C, 0x1D, 0x1E, 0x1F} ;
    vector unsigned char v_permute_128s_high = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x10, 0x11, 0x12, 0x13, 0x14, 0x015, 0x16, 0x17} ;
    vector unsigned char v_permute_128s_low = {0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F} ;

    v_dst_32s_0 = vec_perm(v_src_0, v_src_1, v_permute_32s_high) ;
    v_dst_32s_1 = vec_perm(v_src_2, v_src_3, v_permute_32s_high) ;
    v_dst_32s_2 = vec_perm(v_src_4, v_src_5, v_permute_32s_high) ;
    v_dst_32s_3 = vec_perm(v_src_6, v_src_7, v_permute_32s_high) ;
    v_dst_32s_4 = vec_perm(v_src_0, v_src_1, v_permute_32s_low) ;
    v_dst_32s_5 = vec_perm(v_src_2, v_src_3, v_permute_32s_low) ;
    v_dst_32s_6 = vec_perm(v_src_4, v_src_5, v_permute_32s_low) ;
    v_dst_32s_7 = vec_perm(v_src_6, v_src_7, v_permute_32s_low) ;

    v_dst_64s_0 = vec_perm(v_dst_32s_0, v_dst_32s_1, v_permute_64s_high) ;
    v_dst_64s_1 = vec_perm(v_dst_32s_2, v_dst_32s_3, v_permute_64s_high) ;
    v_dst_64s_2 = vec_perm(v_dst_32s_0, v_dst_32s_1, v_permute_64s_low) ;
    v_dst_64s_3 = vec_perm(v_dst_32s_2, v_dst_32s_3, v_permute_64s_low) ;
    v_dst_64s_4 = vec_perm(v_dst_32s_4, v_dst_32s_5, v_permute_64s_high) ;
    v_dst_64s_5 = vec_perm(v_dst_32s_6, v_dst_32s_7, v_permute_64s_high) ;
    v_dst_64s_6 = vec_perm(v_dst_32s_4, v_dst_32s_5, v_permute_64s_low) ;
    v_dst_64s_7 = vec_perm(v_dst_32s_6, v_dst_32s_7, v_permute_64s_low) ;

    v_dst_128s_0 = vec_perm(v_dst_64s_0, v_dst_64s_1, v_permute_128s_high) ;
    v_dst_128s_1 = vec_perm(v_dst_64s_0, v_dst_64s_1, v_permute_128s_low) ;
    v_dst_128s_2 = vec_perm(v_dst_64s_2, v_dst_64s_3, v_permute_128s_high) ;
    v_dst_128s_3 = vec_perm(v_dst_64s_2, v_dst_64s_3, v_permute_128s_low) ;
    v_dst_128s_4 = vec_perm(v_dst_64s_4, v_dst_64s_5, v_permute_128s_high) ;
    v_dst_128s_5 = vec_perm(v_dst_64s_4, v_dst_64s_5, v_permute_128s_low) ;
    v_dst_128s_6 = vec_perm(v_dst_64s_6, v_dst_64s_7, v_permute_128s_high) ;
    v_dst_128s_7 = vec_perm(v_dst_64s_6, v_dst_64s_7, v_permute_128s_low) ;


    vec_xst(v_dst_128s_0,                  0, dst) ;
    vec_xst(v_dst_128s_1,  (dstStride*2)    , dst) ;
    vec_xst(v_dst_128s_2,  (dstStride*2) * 2, dst) ;
    vec_xst(v_dst_128s_3,  (dstStride*2) * 3, dst) ;
    vec_xst(v_dst_128s_4,  (dstStride*2) * 4, dst) ;
    vec_xst(v_dst_128s_5,  (dstStride*2) * 5, dst) ;
    vec_xst(v_dst_128s_6,  (dstStride*2) * 6, dst) ;
    vec_xst(v_dst_128s_7,  (dstStride*2) * 7, dst) ;

} // end transpose_matrix_8_altivec()


inline void transpose_matrix_16_altivec(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
{
    transpose_matrix_8_altivec((int16_t *)src, srcStride, (int16_t *)dst, dstStride) ;
    transpose_matrix_8_altivec((int16_t *)&src[8] , srcStride, (int16_t *)&dst[dstStride*8], dstStride) ;
    transpose_matrix_8_altivec((int16_t *)&src[srcStride*8], srcStride, (int16_t *)&dst[8], dstStride) ;
    transpose_matrix_8_altivec((int16_t *)&src[srcStride*8 + 8], srcStride, (int16_t *)&dst[dstStride*8 + 8], dstStride) ;
} // end transpose_matrix_16_altivec()


inline void transpose_matrix_32_altivec(int16_t *src, intptr_t srcStride, int16_t *dst, intptr_t dstStride)
{
    transpose_matrix_16_altivec((int16_t *)src, srcStride, (int16_t *)dst, dstStride) ;
    transpose_matrix_16_altivec((int16_t *)&src[16] , srcStride, (int16_t *)&dst[dstStride*16], dstStride) ;
    transpose_matrix_16_altivec((int16_t *)&src[srcStride*16], srcStride, (int16_t *)&dst[16], dstStride) ;
    transpose_matrix_16_altivec((int16_t *)&src[srcStride*16 + 16], srcStride, (int16_t *)&dst[dstStride*16 + 16], dstStride) ;
} // end transpose_matrix_32_altivec()


inline static void partialButterfly32_transposedSrc_altivec(const int16_t* __restrict__ src, int16_t* __restrict__ dst, int shift)
{
    const int line = 32 ;
    
    int j, k;
    int E[16][8], O[16][8];
    int EE[8][8], EO[8][8];
    int EEE[4][8], EEO[4][8];
    int EEEE[2][8], EEEO[2][8];
    int add = 1 << (shift - 1);

    for (j = 0; j < line/8; j++)
    {
        /* E and O*/
        for(int ii=0; ii<8; ii++) { E[0][ii] = src[(0*line) + ii] + src[((31 - 0)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[0][ii] = src[(0*line) + ii] - src[((31 - 0)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[1][ii] = src[(1*line) + ii] + src[((31 - 1)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[1][ii] = src[(1*line) + ii] - src[((31 - 1)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[2][ii] = src[(2*line) + ii] + src[((31 - 2)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[2][ii] = src[(2*line) + ii] - src[((31 - 2)*line) + ii] ; }
        
        for(int ii=0; ii<8; ii++) { E[3][ii] = src[(3*line) + ii] + src[((31 - 3)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[3][ii] = src[(3*line) + ii] - src[((31 - 3)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[4][ii] = src[(4*line) + ii] + src[((31 - 4)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[4][ii] = src[(4*line) + ii] - src[((31 - 4)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[5][ii] = src[(5*line) + ii] + src[((31 - 5)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[5][ii] = src[(5*line) + ii] - src[((31 - 5)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[6][ii] = src[(6*line) + ii] + src[((31 - 6)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[6][ii] = src[(6*line) + ii] - src[((31 - 6)*line) + ii] ; }
        
        for(int ii=0; ii<8; ii++) { E[7][ii] = src[(7*line) + ii] + src[((31 - 7)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[7][ii] = src[(7*line) + ii] - src[((31 - 7)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[8][ii] = src[(8*line) + ii] + src[((31 - 8)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[8][ii] = src[(8*line) + ii] - src[((31 - 8)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[9][ii] = src[(9*line) + ii] + src[((31 - 9)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[9][ii] = src[(9*line) + ii] - src[((31 - 9)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[10][ii] = src[(10*line) + ii] + src[((31 - 10)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[10][ii] = src[(10*line) + ii] - src[((31 - 10)*line) + ii] ; }
        
        for(int ii=0; ii<8; ii++) { E[11][ii] = src[(11*line) + ii] + src[((31 - 11)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[11][ii] = src[(11*line) + ii] - src[((31 - 11)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[12][ii] = src[(12*line) + ii] + src[((31 - 12)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[12][ii] = src[(12*line) + ii] - src[((31 - 12)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[13][ii] = src[(13*line) + ii] + src[((31 - 13)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[13][ii] = src[(13*line) + ii] - src[((31 - 13)*line) + ii] ; }

        for(int ii=0; ii<8; ii++) { E[14][ii] = src[(14*line) + ii] + src[((31 - 14)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[14][ii] = src[(14*line) + ii] - src[((31 - 14)*line) + ii] ; }
        
        for(int ii=0; ii<8; ii++) { E[15][ii] = src[(15*line) + ii] + src[((31 - 15)*line) + ii] ; }
        for(int ii=0; ii<8; ii++) { O[15][ii] = src[(15*line) + ii] - src[((31 - 15)*line) + ii] ; }


        /* EE and EO */
        for(int ii=0; ii<8; ii++) {EE[0][ii] = E[0][ii] + E[15 - 0][ii];}
        for(int ii=0; ii<8; ii++) {EO[0][ii] = E[0][ii] - E[15 - 0][ii];}

        for(int ii=0; ii<8; ii++) {EE[1][ii] = E[1][ii] + E[15 - 1][ii];}
        for(int ii=0; ii<8; ii++) {EO[1][ii] = E[1][ii] - E[15 - 1][ii];}

        for(int ii=0; ii<8; ii++) {EE[2][ii] = E[2][ii] + E[15 - 2][ii];}
        for(int ii=0; ii<8; ii++) {EO[2][ii] = E[2][ii] - E[15 - 2][ii];}

        for(int ii=0; ii<8; ii++) {EE[3][ii] = E[3][ii] + E[15 - 3][ii];}
        for(int ii=0; ii<8; ii++) {EO[3][ii] = E[3][ii] - E[15 - 3][ii];}

        for(int ii=0; ii<8; ii++) {EE[4][ii] = E[4][ii] + E[15 - 4][ii];}
        for(int ii=0; ii<8; ii++) {EO[4][ii] = E[4][ii] - E[15 - 4][ii];}

        for(int ii=0; ii<8; ii++) {EE[5][ii] = E[5][ii] + E[15 - 5][ii];}
        for(int ii=0; ii<8; ii++) {EO[5][ii] = E[5][ii] - E[15 - 5][ii];}

        for(int ii=0; ii<8; ii++) {EE[6][ii] = E[6][ii] + E[15 - 6][ii];}
        for(int ii=0; ii<8; ii++) {EO[6][ii] = E[6][ii] - E[15 - 6][ii];}

        for(int ii=0; ii<8; ii++) {EE[7][ii] = E[7][ii] + E[15 - 7][ii];}
        for(int ii=0; ii<8; ii++) {EO[7][ii] = E[7][ii] - E[15 - 7][ii];}


        /* EEE and EEO */
        for(int ii=0; ii<8; ii++) {EEE[0][ii] = EE[0][ii] + EE[7 - 0][ii];}
        for(int ii=0; ii<8; ii++) {EEO[0][ii] = EE[0][ii] - EE[7 - 0][ii];}

        for(int ii=0; ii<8; ii++) {EEE[1][ii] = EE[1][ii] + EE[7 - 1][ii];}
        for(int ii=0; ii<8; ii++) {EEO[1][ii] = EE[1][ii] - EE[7 - 1][ii];}

        for(int ii=0; ii<8; ii++) {EEE[2][ii] = EE[2][ii] + EE[7 - 2][ii];}
        for(int ii=0; ii<8; ii++) {EEO[2][ii] = EE[2][ii] - EE[7 - 2][ii];}

        for(int ii=0; ii<8; ii++) {EEE[3][ii] = EE[3][ii] + EE[7 - 3][ii];}
        for(int ii=0; ii<8; ii++) {EEO[3][ii] = EE[3][ii] - EE[7 - 3][ii];}



        /* EEEE and EEEO */
        for(int ii=0; ii<8; ii++) {EEEE[0][ii] = EEE[0][ii] + EEE[3][ii];}
        for(int ii=0; ii<8; ii++) {EEEO[0][ii] = EEE[0][ii] - EEE[3][ii];}

        for(int ii=0; ii<8; ii++) {EEEE[1][ii] = EEE[1][ii] + EEE[2][ii];}
        for(int ii=0; ii<8; ii++) {EEEO[1][ii] = EEE[1][ii] - EEE[2][ii];}


        /* writing to dst */
        for(int ii=0; ii<8; ii++) {dst[0 + ii] = (int16_t)((g_t32[0][0] * EEEE[0][ii] + g_t32[0][1] * EEEE[1][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(16 * line) + ii] = (int16_t)((g_t32[16][0] * EEEE[0][ii] + g_t32[16][1] * EEEE[1][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(8 * line ) + ii] = (int16_t)((g_t32[8][0] * EEEO[0][ii] + g_t32[8][1] * EEEO[1][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(24 * line) + ii] = (int16_t)((g_t32[24][0] * EEEO[0][ii] + g_t32[24][1] * EEEO[1][ii] + add) >> shift);}

        for(int ii=0; ii<8; ii++) {dst[(4 * line) + ii] = (int16_t)((g_t32[4][0] * EEO[0][ii] + g_t32[4][1] * EEO[1][ii] + g_t32[4][2] * EEO[2][ii] + g_t32[4][3] * EEO[3][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(12 * line) + ii] = (int16_t)((g_t32[12][0] * EEO[0][ii] + g_t32[12][1] * EEO[1][ii] + g_t32[12][2] * EEO[2][ii] + g_t32[12][3] * EEO[3][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(20 * line) + ii] = (int16_t)((g_t32[20][0] * EEO[0][ii] + g_t32[20][1] * EEO[1][ii] + g_t32[20][2] * EEO[2][ii] + g_t32[20][3] * EEO[3][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(28 * line) + ii] = (int16_t)((g_t32[28][0] * EEO[0][ii] + g_t32[28][1] * EEO[1][ii] + g_t32[28][2] * EEO[2][ii] + g_t32[28][3] * EEO[3][ii] + add) >> shift);}

        for(int ii=0; ii<8; ii++) {dst[(2 * line) + ii] = (int16_t)((g_t32[2][0] * EO[0][ii] + g_t32[2][1] * EO[1][ii] + g_t32[2][2] * EO[2][ii] + g_t32[2][3] * EO[3][ii] + g_t32[2][4] * EO[4][ii] + g_t32[2][5] * EO[5][ii] + g_t32[2][6] * EO[6][ii] + g_t32[2][7] * EO[7][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(6 * line) + ii] = (int16_t)((g_t32[6][0] * EO[0][ii] + g_t32[6][1] * EO[1][ii] + g_t32[6][2] * EO[2][ii] + g_t32[6][3] * EO[3][ii] + g_t32[6][4] * EO[4][ii] + g_t32[6][5] * EO[5][ii] + g_t32[6][6] * EO[6][ii] + g_t32[6][7] * EO[7][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(10 * line) + ii] = (int16_t)((g_t32[10][0] * EO[0][ii] + g_t32[10][1] * EO[1][ii] + g_t32[10][2] * EO[2][ii] + g_t32[10][3] * EO[3][ii] + g_t32[10][4] * EO[4][ii] + g_t32[10][5] * EO[5][ii] + g_t32[10][6] * EO[6][ii] + g_t32[10][7] * EO[7][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(14 * line) + ii] = (int16_t)((g_t32[14][0] * EO[0][ii] + g_t32[14][1] * EO[1][ii] + g_t32[14][2] * EO[2][ii] + g_t32[14][3] * EO[3][ii] + g_t32[14][4] * EO[4][ii] + g_t32[14][5] * EO[5][ii] + g_t32[14][6] * EO[6][ii] + g_t32[14][7] * EO[7][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(18 * line) + ii] = (int16_t)((g_t32[18][0] * EO[0][ii] + g_t32[18][1] * EO[1][ii] + g_t32[18][2] * EO[2][ii] + g_t32[18][3] * EO[3][ii] + g_t32[18][4] * EO[4][ii] + g_t32[18][5] * EO[5][ii] + g_t32[18][6] * EO[6][ii] + g_t32[18][7] * EO[7][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(22 * line) + ii] = (int16_t)((g_t32[22][0] * EO[0][ii] + g_t32[22][1] * EO[1][ii] + g_t32[22][2] * EO[2][ii] + g_t32[22][3] * EO[3][ii] + g_t32[22][4] * EO[4][ii] + g_t32[22][5] * EO[5][ii] + g_t32[22][6] * EO[6][ii] + g_t32[22][7] * EO[7][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(26 * line) + ii] = (int16_t)((g_t32[26][0] * EO[0][ii] + g_t32[26][1] * EO[1][ii] + g_t32[26][2] * EO[2][ii] + g_t32[26][3] * EO[3][ii] + g_t32[26][4] * EO[4][ii] + g_t32[26][5] * EO[5][ii] + g_t32[26][6] * EO[6][ii] + g_t32[26][7] * EO[7][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) {dst[(30 * line) + ii] = (int16_t)((g_t32[30][0] * EO[0][ii] + g_t32[30][1] * EO[1][ii] + g_t32[30][2] * EO[2][ii] + g_t32[30][3] * EO[3][ii] + g_t32[30][4] * EO[4][ii] + g_t32[30][5] * EO[5][ii] + g_t32[30][6] * EO[6][ii] + g_t32[30][7] * EO[7][ii] + add) >> shift);}


        for(int ii=0; ii<8; ii++) { dst[(1 * line) + ii] = (int16_t)((g_t32[1][0] * O[0][ii] + g_t32[1][1] * O[1][ii] + g_t32[1][2]   * O[2][ii] + g_t32[1][3] * O[3][ii] + g_t32[1][4] * O[4][ii] + g_t32[1][5] * O[5][ii] + g_t32[1][6]   * O[6][ii] + g_t32[1][7] * O[7][ii] + g_t32[1][8] * O[8][ii] + g_t32[1][9] * O[9][ii] + g_t32[1][10] * O[10][ii] + g_t32[1][11] * O[11][ii] + g_t32[1][12] * O[12][ii] + g_t32[1][13] * O[13][ii] + g_t32[1][14] * O[14][ii] + g_t32[1][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(3 * line) + ii] = (int16_t)((g_t32[3][0] * O[0][ii] + g_t32[3][1] * O[1][ii] + g_t32[3][2]   * O[2][ii] + g_t32[3][3] * O[3][ii] + g_t32[3][4] * O[4][ii] + g_t32[3][5] * O[5][ii] + g_t32[3][6]   * O[6][ii] + g_t32[3][7] * O[7][ii] + g_t32[3][8] * O[8][ii] + g_t32[3][9] * O[9][ii] + g_t32[3][10] * O[10][ii] + g_t32[3][11] * O[11][ii] + g_t32[3][12] * O[12][ii] + g_t32[3][13] * O[13][ii] + g_t32[3][14] * O[14][ii] + g_t32[3][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(5 * line) + ii] = (int16_t)((g_t32[5][0] * O[0][ii] + g_t32[5][1] * O[1][ii] + g_t32[5][2]   * O[2][ii] + g_t32[5][3] * O[3][ii] + g_t32[5][4] * O[4][ii] + g_t32[5][5] * O[5][ii] + g_t32[5][6]   * O[6][ii] + g_t32[5][7] * O[7][ii] + g_t32[5][8] * O[8][ii] + g_t32[5][9] * O[9][ii] + g_t32[5][10] * O[10][ii] + g_t32[5][11] * O[11][ii] + g_t32[5][12] * O[12][ii] + g_t32[5][13] * O[13][ii] + g_t32[5][14] * O[14][ii] + g_t32[5][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(7 * line) + ii] = (int16_t)((g_t32[7][0] * O[0][ii] + g_t32[7][1] * O[1][ii] + g_t32[7][2]   * O[2][ii] + g_t32[7][3] * O[3][ii] + g_t32[7][4] * O[4][ii] + g_t32[7][5] * O[5][ii] + g_t32[7][6]   * O[6][ii] + g_t32[7][7] * O[7][ii] + g_t32[7][8] * O[8][ii] + g_t32[7][9] * O[9][ii] + g_t32[7][10] * O[10][ii] + g_t32[7][11] * O[11][ii] + g_t32[7][12] * O[12][ii] + g_t32[7][13] * O[13][ii] + g_t32[7][14] * O[14][ii] + g_t32[7][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(9 * line) + ii] = (int16_t)((g_t32[9][0] * O[0][ii] + g_t32[9][1] * O[1][ii] + g_t32[9][2]   * O[2][ii] + g_t32[9][3] * O[3][ii] + g_t32[9][4] * O[4][ii] + g_t32[9][5] * O[5][ii] + g_t32[9][6]   * O[6][ii] + g_t32[9][7] * O[7][ii] + g_t32[9][8] * O[8][ii] + g_t32[9][9] * O[9][ii] + g_t32[9][10] * O[10][ii] + g_t32[9][11] * O[11][ii] + g_t32[9][12] * O[12][ii] + g_t32[9][13] * O[13][ii] + g_t32[9][14] * O[14][ii] + g_t32[9][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(11 * line) + ii] = (int16_t)((g_t32[11][0] * O[0][ii] + g_t32[11][1] * O[1][ii] + g_t32[11][2]   * O[2][ii] + g_t32[11][3] * O[3][ii] + g_t32[11][4] * O[4][ii] + g_t32[11][5] * O[5][ii] + g_t32[11][6]   * O[6][ii] + g_t32[11][7] * O[7][ii] + g_t32[11][8] * O[8][ii] + g_t32[11][9] * O[9][ii] + g_t32[11][10] * O[10][ii] + g_t32[11][11] * O[11][ii] + g_t32[11][12] * O[12][ii] + g_t32[11][13] * O[13][ii] + g_t32[11][14] * O[14][ii] + g_t32[11][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(13 * line) + ii] = (int16_t)((g_t32[13][0] * O[0][ii] + g_t32[13][1] * O[1][ii] + g_t32[13][2]   * O[2][ii] + g_t32[13][3] * O[3][ii] + g_t32[13][4] * O[4][ii] + g_t32[13][5] * O[5][ii] + g_t32[13][6]   * O[6][ii] + g_t32[13][7] * O[7][ii] + g_t32[13][8] * O[8][ii] + g_t32[13][9] * O[9][ii] + g_t32[13][10] * O[10][ii] + g_t32[13][11] * O[11][ii] + g_t32[13][12] * O[12][ii] + g_t32[13][13] * O[13][ii] + g_t32[13][14] * O[14][ii] + g_t32[13][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(15 * line) + ii] = (int16_t)((g_t32[15][0] * O[0][ii] + g_t32[15][1] * O[1][ii] + g_t32[15][2]   * O[2][ii] + g_t32[15][3] * O[3][ii] + g_t32[15][4] * O[4][ii] + g_t32[15][5] * O[5][ii] + g_t32[15][6]   * O[6][ii] + g_t32[15][7] * O[7][ii] + g_t32[15][8] * O[8][ii] + g_t32[15][9] * O[9][ii] + g_t32[15][10] * O[10][ii] + g_t32[15][11] * O[11][ii] + g_t32[15][12] * O[12][ii] + g_t32[15][13] * O[13][ii] + g_t32[15][14] * O[14][ii] + g_t32[15][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(17 * line) + ii] = (int16_t)((g_t32[17][0] * O[0][ii] + g_t32[17][1] * O[1][ii] + g_t32[17][2]   * O[2][ii] + g_t32[17][3] * O[3][ii] + g_t32[17][4] * O[4][ii] + g_t32[17][5] * O[5][ii] + g_t32[17][6]   * O[6][ii] + g_t32[17][7] * O[7][ii] + g_t32[17][8] * O[8][ii] + g_t32[17][9] * O[9][ii] + g_t32[17][10] * O[10][ii] + g_t32[17][11] * O[11][ii] + g_t32[17][12] * O[12][ii] + g_t32[17][13] * O[13][ii] + g_t32[17][14] * O[14][ii] + g_t32[17][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(19 * line) + ii] = (int16_t)((g_t32[19][0] * O[0][ii] + g_t32[19][1] * O[1][ii] + g_t32[19][2]   * O[2][ii] + g_t32[19][3] * O[3][ii] + g_t32[19][4] * O[4][ii] + g_t32[19][5] * O[5][ii] + g_t32[19][6]   * O[6][ii] + g_t32[19][7] * O[7][ii] + g_t32[19][8] * O[8][ii] + g_t32[19][9] * O[9][ii] + g_t32[19][10] * O[10][ii] + g_t32[19][11] * O[11][ii] + g_t32[19][12] * O[12][ii] + g_t32[19][13] * O[13][ii] + g_t32[19][14] * O[14][ii] + g_t32[19][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(21 * line) + ii] = (int16_t)((g_t32[21][0] * O[0][ii] + g_t32[21][1] * O[1][ii] + g_t32[21][2]   * O[2][ii] + g_t32[21][3] * O[3][ii] + g_t32[21][4] * O[4][ii] + g_t32[21][5] * O[5][ii] + g_t32[21][6]   * O[6][ii] + g_t32[21][7] * O[7][ii] + g_t32[21][8] * O[8][ii] + g_t32[21][9] * O[9][ii] + g_t32[21][10] * O[10][ii] + g_t32[21][11] * O[11][ii] + g_t32[21][12] * O[12][ii] + g_t32[21][13] * O[13][ii] + g_t32[21][14] * O[14][ii] + g_t32[21][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(23 * line) + ii] = (int16_t)((g_t32[23][0] * O[0][ii] + g_t32[23][1] * O[1][ii] + g_t32[23][2]   * O[2][ii] + g_t32[23][3] * O[3][ii] + g_t32[23][4] * O[4][ii] + g_t32[23][5] * O[5][ii] + g_t32[23][6]   * O[6][ii] + g_t32[23][7] * O[7][ii] + g_t32[23][8] * O[8][ii] + g_t32[23][9] * O[9][ii] + g_t32[23][10] * O[10][ii] + g_t32[23][11] * O[11][ii] + g_t32[23][12] * O[12][ii] + g_t32[23][13] * O[13][ii] + g_t32[23][14] * O[14][ii] + g_t32[23][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(25 * line) + ii] = (int16_t)((g_t32[25][0] * O[0][ii] + g_t32[25][1] * O[1][ii] + g_t32[25][2]   * O[2][ii] + g_t32[25][3] * O[3][ii] + g_t32[25][4] * O[4][ii] + g_t32[25][5] * O[5][ii] + g_t32[25][6]   * O[6][ii] + g_t32[25][7] * O[7][ii] + g_t32[25][8] * O[8][ii] + g_t32[25][9] * O[9][ii] + g_t32[25][10] * O[10][ii] + g_t32[25][11] * O[11][ii] + g_t32[25][12] * O[12][ii] + g_t32[25][13] * O[13][ii] + g_t32[25][14] * O[14][ii] + g_t32[25][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(27 * line) + ii] = (int16_t)((g_t32[27][0] * O[0][ii] + g_t32[27][1] * O[1][ii] + g_t32[27][2]   * O[2][ii] + g_t32[27][3] * O[3][ii] + g_t32[27][4] * O[4][ii] + g_t32[27][5] * O[5][ii] + g_t32[27][6]   * O[6][ii] + g_t32[27][7] * O[7][ii] + g_t32[27][8] * O[8][ii] + g_t32[27][9] * O[9][ii] + g_t32[27][10] * O[10][ii] + g_t32[27][11] * O[11][ii] + g_t32[27][12] * O[12][ii] + g_t32[27][13] * O[13][ii] + g_t32[27][14] * O[14][ii] + g_t32[27][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(29 * line) + ii] = (int16_t)((g_t32[29][0] * O[0][ii] + g_t32[29][1] * O[1][ii] + g_t32[29][2]   * O[2][ii] + g_t32[29][3] * O[3][ii] + g_t32[29][4] * O[4][ii] + g_t32[29][5] * O[5][ii] + g_t32[29][6]   * O[6][ii] + g_t32[29][7] * O[7][ii] + g_t32[29][8] * O[8][ii] + g_t32[29][9] * O[9][ii] + g_t32[29][10] * O[10][ii] + g_t32[29][11] * O[11][ii] + g_t32[29][12] * O[12][ii] + g_t32[29][13] * O[13][ii] + g_t32[29][14] * O[14][ii] + g_t32[29][15] * O[15][ii] + add) >> shift);}
        for(int ii=0; ii<8; ii++) { dst[(31 * line) + ii] = (int16_t)((g_t32[31][0] * O[0][ii] + g_t32[31][1] * O[1][ii] + g_t32[31][2]   * O[2][ii] + g_t32[31][3] * O[3][ii] + g_t32[31][4] * O[4][ii] + g_t32[31][5] * O[5][ii] + g_t32[31][6]   * O[6][ii] + g_t32[31][7] * O[7][ii] + g_t32[31][8] * O[8][ii] + g_t32[31][9] * O[9][ii] + g_t32[31][10] * O[10][ii] + g_t32[31][11] * O[11][ii] + g_t32[31][12] * O[12][ii] + g_t32[31][13] * O[13][ii] + g_t32[31][14] * O[14][ii] + g_t32[31][15] * O[15][ii] + add) >> shift);}

        src += 8 ;
        dst += 8 ;
    }
} // end partialButterfly32_transposedSrc_altivec()


inline static void partialButterfly16_transposedSrc_altivec(const int16_t* __restrict__ src, int16_t* __restrict__ dst, int shift)
{
    const int line = 16 ;

    int j, k;
    int add = 1 << (shift - 1);

    int E[8][8], O[8][8] ;
    int EE[4][8], EO[4][8] ;
    int EEE[2][8], EEO[2][8] ;


    for (j = 0; j < line/8; j++)
    {
        /* E and O */
        for(int ii=0; ii<8; ii++) { E[0][ii] = src[(0*line) + ii] + src[ ((15 - 0) * line) + ii] ;}
        for(int ii=0; ii<8; ii++) { O[0][ii] = src[(0*line) + ii] - src[ ((15 - 0) * line) + ii] ;}

        for(int ii=0; ii<8; ii++) { E[1][ii] = src[(1*line) + ii] + src[ ((15 - 1) * line) + ii] ;}
        for(int ii=0; ii<8; ii++) { O[1][ii] = src[(1*line) + ii] - src[ ((15 - 1) * line) + ii] ;}

        for(int ii=0; ii<8; ii++) { E[2][ii] = src[(2*line) + ii] + src[ ((15 - 2) * line) + ii] ;}
        for(int ii=0; ii<8; ii++) { O[2][ii] = src[(2*line) + ii] - src[ ((15 - 2) * line) + ii] ;}

        for(int ii=0; ii<8; ii++) { E[3][ii] = src[(3*line) + ii] + src[ ((15 - 3) * line) + ii] ;}
        for(int ii=0; ii<8; ii++) { O[3][ii] = src[(3*line) + ii] - src[ ((15 - 3) * line) + ii] ;}

        for(int ii=0; ii<8; ii++) { E[4][ii] = src[(4*line) + ii] + src[ ((15 - 4) * line) + ii] ;}
        for(int ii=0; ii<8; ii++) { O[4][ii] = src[(4*line) + ii] - src[ ((15 - 4) * line) + ii] ;}

        for(int ii=0; ii<8; ii++) { E[5][ii] = src[(5*line) + ii] + src[ ((15 - 5) * line) + ii] ;}
        for(int ii=0; ii<8; ii++) { O[5][ii] = src[(5*line) + ii] - src[ ((15 - 5) * line) + ii] ;}

        for(int ii=0; ii<8; ii++) { E[6][ii] = src[(6*line) + ii] + src[ ((15 - 6) * line) + ii] ;}
        for(int ii=0; ii<8; ii++) { O[6][ii] = src[(6*line) + ii] - src[ ((15 - 6) * line) + ii] ;}

        for(int ii=0; ii<8; ii++) { E[7][ii] = src[(7*line) + ii] + src[ ((15 - 7) * line) + ii] ;}
        for(int ii=0; ii<8; ii++) { O[7][ii] = src[(7*line) + ii] - src[ ((15 - 7) * line) + ii] ;}


        /* EE and EO */
        for(int ii=0; ii<8; ii++) { EE[0][ii] = E[0][ii] + E[7-0][ii] ;} 
        for(int ii=0; ii<8; ii++) { EO[0][ii] = E[0][ii] - E[7-0][ii] ;}

        for(int ii=0; ii<8; ii++) { EE[1][ii] = E[1][ii] + E[7-1][ii] ;}
        for(int ii=0; ii<8; ii++) { EO[1][ii] = E[1][ii] - E[7-1][ii] ;}

        for(int ii=0; ii<8; ii++) { EE[2][ii] = E[2][ii] + E[7-2][ii] ;}
        for(int ii=0; ii<8; ii++) { EO[2][ii] = E[2][ii] - E[7-2][ii] ;}

        for(int ii=0; ii<8; ii++) { EE[3][ii] = E[3][ii] + E[7-3][ii] ;}
        for(int ii=0; ii<8; ii++) { EO[3][ii] = E[3][ii] - E[7-3][ii] ;}


        /* EEE and EEO */
        for(int ii=0; ii<8; ii++) { EEE[0][ii] = EE[0][ii] + EE[3][ii] ;}
        for(int ii=0; ii<8; ii++) { EEO[0][ii] = EE[0][ii] - EE[3][ii] ;}

        for(int ii=0; ii<8; ii++) { EEE[1][ii] = EE[1][ii] + EE[2][ii] ;}
        for(int ii=0; ii<8; ii++) { EEO[1][ii] = EE[1][ii] - EE[2][ii] ;}


        /* Writing to dst */
        for(int ii=0; ii<8; ii++) { dst[    0       + ii] = (int16_t)((g_t16[0][0] * EEE[0][ii] + g_t16[0][1] * EEE[1][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(8 * line)  + ii] = (int16_t)((g_t16[8][0] * EEE[0][ii] + g_t16[8][1] * EEE[1][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(4 * line)  + ii] = (int16_t)((g_t16[4][0] * EEO[0][ii] + g_t16[4][1] * EEO[1][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(12 * line) + ii] = (int16_t)((g_t16[12][0] * EEO[0][ii] + g_t16[12][1] * EEO[1][ii] + add) >> shift) ; }

        for(int ii=0; ii<8; ii++) { dst[(2 * line) + ii] = (int16_t)((g_t16[2][0] * EO[0][ii] + g_t16[2][1] * EO[1][ii] + g_t16[2][2] * EO[2][ii] + g_t16[2][3] * EO[3][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(6 * line) + ii] = (int16_t)((g_t16[6][0] * EO[0][ii] + g_t16[6][1] * EO[1][ii] + g_t16[6][2] * EO[2][ii] + g_t16[6][3] * EO[3][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(10 * line) + ii] = (int16_t)((g_t16[10][0] * EO[0][ii] + g_t16[10][1] * EO[1][ii] + g_t16[10][2] * EO[2][ii] + g_t16[10][3] * EO[3][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(14 * line) + ii] = (int16_t)((g_t16[14][0] * EO[0][ii] + g_t16[14][1] * EO[1][ii] + g_t16[14][2] * EO[2][ii] + g_t16[14][3] * EO[3][ii] + add) >> shift) ;}
    
        for(int ii=0; ii<8; ii++) { dst[(1 * line) + ii] =  (int16_t)((g_t16[1][0] * O[0][ii] + g_t16[1][1] * O[1][ii] + g_t16[1][2] * O[2][ii] + g_t16[1][3] * O[3][ii] + g_t16[1][4] * O[4][ii] + g_t16[1][5] * O[5][ii] + g_t16[1][6] * O[6][ii] + g_t16[1][7] * O[7][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(3 * line) + ii] =  (int16_t)((g_t16[3][0] * O[0][ii] + g_t16[3][1] * O[1][ii] + g_t16[3][2] * O[2][ii] + g_t16[3][3] * O[3][ii] + g_t16[3][4] * O[4][ii] + g_t16[3][5] * O[5][ii] + g_t16[3][6] * O[6][ii] + g_t16[3][7] * O[7][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(5 * line) + ii] =  (int16_t)((g_t16[5][0] * O[0][ii] + g_t16[5][1] * O[1][ii] + g_t16[5][2] * O[2][ii] + g_t16[5][3] * O[3][ii] + g_t16[5][4] * O[4][ii] + g_t16[5][5] * O[5][ii] + g_t16[5][6] * O[6][ii] + g_t16[5][7] * O[7][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(7 * line) + ii] =  (int16_t)((g_t16[7][0] * O[0][ii] + g_t16[7][1] * O[1][ii] + g_t16[7][2] * O[2][ii] + g_t16[7][3] * O[3][ii] + g_t16[7][4] * O[4][ii] + g_t16[7][5] * O[5][ii] + g_t16[7][6] * O[6][ii] + g_t16[7][7] * O[7][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(9 * line) + ii] =  (int16_t)((g_t16[9][0] * O[0][ii] + g_t16[9][1] * O[1][ii] + g_t16[9][2] * O[2][ii] + g_t16[9][3] * O[3][ii] + g_t16[9][4] * O[4][ii] + g_t16[9][5] * O[5][ii] + g_t16[9][6] * O[6][ii] + g_t16[9][7] * O[7][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(11 * line) + ii] =  (int16_t)((g_t16[11][0] * O[0][ii] + g_t16[11][1] * O[1][ii] + g_t16[11][2] * O[2][ii] + g_t16[11][3] * O[3][ii] + g_t16[11][4] * O[4][ii] + g_t16[11][5] * O[5][ii] + g_t16[11][6] * O[6][ii] + g_t16[11][7] * O[7][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(13 * line) + ii] =  (int16_t)((g_t16[13][0] * O[0][ii] + g_t16[13][1] * O[1][ii] + g_t16[13][2] * O[2][ii] + g_t16[13][3] * O[3][ii] + g_t16[13][4] * O[4][ii] + g_t16[13][5] * O[5][ii] + g_t16[13][6] * O[6][ii] + g_t16[13][7] * O[7][ii] + add) >> shift) ;}
        for(int ii=0; ii<8; ii++) { dst[(15 * line) + ii] =  (int16_t)((g_t16[15][0] * O[0][ii] + g_t16[15][1] * O[1][ii] + g_t16[15][2] * O[2][ii] + g_t16[15][3] * O[3][ii] + g_t16[15][4] * O[4][ii] + g_t16[15][5] * O[5][ii] + g_t16[15][6] * O[6][ii] + g_t16[15][7] * O[7][ii] + add) >> shift) ;}


        src += 8; 
        dst += 8 ;

    }
} // end partialButterfly16_transposedSrc_altivec()


static void dct16_altivec(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    const int shift_1st = 3 + X265_DEPTH - 8;
    const int shift_2nd = 10;

    ALIGN_VAR_32(int16_t, coef[16 * 16]);
    ALIGN_VAR_32(int16_t, block_transposed[16 * 16]);
    ALIGN_VAR_32(int16_t, coef_transposed[16 * 16]);

    transpose_matrix_16_altivec((int16_t *)src, srcStride, (int16_t *)block_transposed, 16) ;
    partialButterfly16_transposedSrc_altivec(block_transposed, coef, shift_1st) ;

    transpose_matrix_16_altivec((int16_t *)coef, 16, (int16_t *)coef_transposed, 16) ;
    partialButterfly16_transposedSrc_altivec(coef_transposed, dst, shift_2nd);
} // end dct16_altivec()




static void dct32_altivec(const int16_t* src, int16_t* dst, intptr_t srcStride)
{
    const int shift_1st = 4 + X265_DEPTH - 8;
    const int shift_2nd = 11;

    ALIGN_VAR_32(int16_t, coef[32 * 32]);
    ALIGN_VAR_32(int16_t, block_transposed[32 * 32]);
    ALIGN_VAR_32(int16_t, coef_transposed[32 * 32]);

    transpose_matrix_32_altivec((int16_t *)src, srcStride, (int16_t *)block_transposed, 32) ;
    partialButterfly32_transposedSrc_altivec(block_transposed, coef, shift_1st) ;

    transpose_matrix_32_altivec((int16_t *)coef, 32, (int16_t *)coef_transposed, 32) ;
    partialButterfly32_transposedSrc_altivec(coef_transposed, dst, shift_2nd);
} // end dct32_altivec()


namespace X265_NS {
// x265 private namespace

void setupDCTPrimitives_altivec(EncoderPrimitives& p)
{
    p.quant = quant_altivec ;

    p.cu[BLOCK_16x16].dct = dct16_altivec ;
    p.cu[BLOCK_32x32].dct = dct32_altivec ;

    p.denoiseDct = denoiseDct_altivec ;
}
}
