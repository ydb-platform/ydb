/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Min Chen <chenm003@163.com>
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

#ifndef X265_CONSTANTS_H
#define X265_CONSTANTS_H

#include "common.h"

namespace X265_NS {
// private namespace

extern double x265_lambda_tab[QP_MAX_MAX + 1];
extern double x265_lambda2_tab[QP_MAX_MAX + 1];
extern const uint16_t x265_chroma_lambda2_offset_tab[MAX_CHROMA_LAMBDA_OFFSET + 1];

enum { ChromaQPMappingTableSize = 70 };
enum { AngleMapping422TableSize = 36 };

extern const uint8_t g_chromaScale[ChromaQPMappingTableSize];
extern const uint8_t g_chroma422IntraAngleMappingTable[AngleMapping422TableSize];

// flexible conversion from relative to absolute index
extern const uint32_t g_zscanToRaster[MAX_NUM_PARTITIONS];
extern const uint32_t g_rasterToZscan[MAX_NUM_PARTITIONS];

// conversion of partition index to picture pel position
extern const uint8_t g_zscanToPelX[MAX_NUM_PARTITIONS];
extern const uint8_t g_zscanToPelY[MAX_NUM_PARTITIONS];
extern const uint8_t g_log2Size[MAX_CU_SIZE + 1]; // from size to log2(size)

// global variable (CTU width/height, max. CU depth)
extern uint32_t g_maxLog2CUSize;
extern uint32_t g_maxCUSize;
extern uint32_t g_maxCUDepth;
extern uint32_t g_unitSizeDepth; // Depth at which 4x4 unit occurs from max CU size
extern uint32_t g_maxSlices; // number of Slices

extern const int16_t g_t4[4][4];
extern const int16_t g_t8[8][8];
extern const int16_t g_t16[16][16];
extern const int16_t g_t32[32][32];

// Subpel interpolation defines and constants

#define NTAPS_LUMA        8                            // Number of taps for luma
#define NTAPS_CHROMA      4                            // Number of taps for chroma
#define IF_INTERNAL_PREC 14                            // Number of bits for internal precision
#define IF_FILTER_PREC    6                            // Log2 of sum of filter taps
#define IF_INTERNAL_OFFS (1 << (IF_INTERNAL_PREC - 1)) // Offset used internally
#define SLFASE_CONSTANT  0x5f4e4a53

extern const int16_t g_lumaFilter[4][NTAPS_LUMA];      // Luma filter taps
extern const int16_t g_chromaFilter[8][NTAPS_CHROMA];  // Chroma filter taps

// Scanning order & context mapping table

#define NUM_SCAN_SIZE 4

extern const uint16_t* const g_scanOrder[NUM_SCAN_TYPE][NUM_SCAN_SIZE];
extern const uint16_t* const g_scanOrderCG[NUM_SCAN_TYPE][NUM_SCAN_SIZE];
extern const uint16_t g_scan8x8diag[8 * 8];
ALIGN_VAR_16(extern const uint16_t, g_scan4x4[NUM_SCAN_TYPE + 1][4 * 4]);  // +1 for safe buffer area for codeCoeffNxN assembly optimize, there have up to 15 bytes beyond bound read
extern const uint8_t g_lastCoeffTable[32];
extern const uint8_t g_goRiceRange[5]; // maximum value coded with Rice codes

// CABAC tables
extern const uint8_t g_lpsTable[64][4];
extern const uint8_t x265_exp2_lut[64];

// Intra tables
extern const uint8_t g_intraFilterFlags[NUM_INTRA_MODE];

extern const uint32_t g_depthScanIdx[8][8];

extern const double g_YUVtoRGB_BT2020[3][3];

#define MIN_HDR_LEGAL_RANGE 64
#define MAX_HDR_LEGAL_RANGE 940
#define CBCR_OFFSET 512
extern const double g_ST2084_PQTable[MAX_HDR_LEGAL_RANGE - MIN_HDR_LEGAL_RANGE + 1];

}

#endif
