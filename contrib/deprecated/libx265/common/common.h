/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Deepthi Nandakumar <deepthi@multicorewareinc.com>
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

#ifndef X265_COMMON_H
#define X265_COMMON_H

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <ctime>

#include <stdint.h>
#include <memory.h>
#include <assert.h>

#include "x265.h"

#if ENABLE_PPA && ENABLE_VTUNE
#error "PPA and VTUNE cannot both be enabled. Disable one of them."
#endif
#if ENABLE_PPA
#include "profile/PPA/ppa.h" // Y_IGNORE
#define ProfileScopeEvent(x) PPAScopeEvent(x)
#define THREAD_NAME(n,i)
#define PROFILE_INIT()       PPA_INIT()
#define PROFILE_PAUSE()
#define PROFILE_RESUME()
#elif ENABLE_VTUNE
#include "profile/vtune/vtune.h" // Y_IGNORE
#define ProfileScopeEvent(x) VTuneScopeEvent _vtuneTask(x)
#define THREAD_NAME(n,i)     vtuneSetThreadName(n, i)
#define PROFILE_INIT()       vtuneInit()
#define PROFILE_PAUSE()      __itt_pause()
#define PROFILE_RESUME()     __itt_resume()
#else
#define ProfileScopeEvent(x)
#define THREAD_NAME(n,i)
#define PROFILE_INIT()
#define PROFILE_PAUSE()
#define PROFILE_RESUME()
#endif

#define FENC_STRIDE 64
#define NUM_INTRA_MODE 35

#if defined(__GNUC__)
#define ALIGN_VAR_4(T, var)  T var __attribute__((aligned(4)))
#define ALIGN_VAR_8(T, var)  T var __attribute__((aligned(8)))
#define ALIGN_VAR_16(T, var) T var __attribute__((aligned(16)))
#define ALIGN_VAR_32(T, var) T var __attribute__((aligned(32)))

#if defined(__MINGW32__)
#define fseeko fseeko64
#endif

#elif defined(_MSC_VER)

#define ALIGN_VAR_4(T, var)  __declspec(align(4)) T var
#define ALIGN_VAR_8(T, var)  __declspec(align(8)) T var
#define ALIGN_VAR_16(T, var) __declspec(align(16)) T var
#define ALIGN_VAR_32(T, var) __declspec(align(32)) T var
#define fseeko _fseeki64

#endif // if defined(__GNUC__)

#if HAVE_INT_TYPES_H
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#define X265_LL "%" PRIu64
#else
#define X265_LL "%lld"
#endif

#if _DEBUG && defined(_MSC_VER)
#define DEBUG_BREAK() __debugbreak()
#elif __APPLE_CC__
#define DEBUG_BREAK() __builtin_trap()
#else
#define DEBUG_BREAK() abort()
#endif

/* If compiled with CHECKED_BUILD perform run-time checks and log any that
 * fail, both to stderr and to a file */
#if CHECKED_BUILD || _DEBUG
namespace X265_NS { extern int g_checkFailures; }
#define X265_CHECK(expr, ...) if (!(expr)) { \
    x265_log(NULL, X265_LOG_ERROR, __VA_ARGS__); \
    FILE *fp = fopen("x265_check_failures.txt", "a"); \
    if (fp) { fprintf(fp, "%s:%d\n", __FILE__, __LINE__); fprintf(fp, __VA_ARGS__); fclose(fp); } \
    g_checkFailures++; DEBUG_BREAK(); \
}
#if _MSC_VER
#pragma warning(disable: 4127) // some checks have constant conditions
#endif
#else
#define X265_CHECK(expr, ...)
#endif

#if HIGH_BIT_DEPTH
typedef uint16_t pixel;
typedef uint32_t sum_t;
typedef uint64_t sum2_t;
typedef uint64_t pixel4;
typedef int64_t  ssum2_t;
#else
typedef uint8_t  pixel;
typedef uint16_t sum_t;
typedef uint32_t sum2_t;
typedef uint32_t pixel4;
typedef int32_t  ssum2_t; // Signed sum
#endif // if HIGH_BIT_DEPTH

#if X265_DEPTH < 10
typedef uint32_t sse_t;
#else
typedef uint64_t sse_t;
#endif

#ifndef NULL
#define NULL 0
#endif

#define MAX_UINT        0xFFFFFFFFU // max. value of unsigned 32-bit integer
#define MAX_INT         2147483647  // max. value of signed 32-bit integer
#define MAX_INT64       0x7FFFFFFFFFFFFFFFLL  // max. value of signed 64-bit integer
#define MAX_DOUBLE      1.7e+308    // max. value of double-type value

#define QP_MIN          0
#define QP_MAX_SPEC     51 /* max allowed signaled QP in HEVC */
#define QP_MAX_MAX      69 /* max allowed QP to be output by rate control */

#define MIN_QPSCALE     0.21249999999999999
#define MAX_MAX_QPSCALE 615.46574234477100


template<typename T>
inline T x265_min(T a, T b) { return a < b ? a : b; }

template<typename T>
inline T x265_max(T a, T b) { return a > b ? a : b; }

template<typename T>
inline T x265_clip3(T minVal, T maxVal, T a) { return x265_min(x265_max(minVal, a), maxVal); }

template<typename T> /* clip to pixel range, 0..255 or 0..1023 */
inline pixel x265_clip(T x) { return (pixel)x265_min<T>(T((1 << X265_DEPTH) - 1), x265_max<T>(T(0), x)); }

typedef int16_t  coeff_t;      // transform coefficient

#define X265_MIN(a, b) ((a) < (b) ? (a) : (b))
#define X265_MAX(a, b) ((a) > (b) ? (a) : (b))
#define COPY1_IF_LT(x, y) {if ((y) < (x)) (x) = (y);}
#define COPY2_IF_LT(x, y, a, b) \
    if ((y) < (x)) \
    { \
        (x) = (y); \
        (a) = (b); \
    }
#define COPY3_IF_LT(x, y, a, b, c, d) \
    if ((y) < (x)) \
    { \
        (x) = (y); \
        (a) = (b); \
        (c) = (d); \
    }
#define COPY4_IF_LT(x, y, a, b, c, d, e, f) \
    if ((y) < (x)) \
    { \
        (x) = (y); \
        (a) = (b); \
        (c) = (d); \
        (e) = (f); \
    }
#define X265_MIN3(a, b, c) X265_MIN((a), X265_MIN((b), (c)))
#define X265_MAX3(a, b, c) X265_MAX((a), X265_MAX((b), (c)))
#define X265_MIN4(a, b, c, d) X265_MIN((a), X265_MIN3((b), (c), (d)))
#define X265_MAX4(a, b, c, d) X265_MAX((a), X265_MAX3((b), (c), (d)))
#define QP_BD_OFFSET (6 * (X265_DEPTH - 8))
#define MAX_CHROMA_LAMBDA_OFFSET 36

// arbitrary, but low because SATD scores are 1/4 normal
#define X265_LOOKAHEAD_QP (12 + QP_BD_OFFSET)

// Use the same size blocks as x264.  Using larger blocks seems to give artificially
// high cost estimates (intra and inter both suffer)
#define X265_LOWRES_CU_SIZE   8
#define X265_LOWRES_CU_BITS   3

#define X265_MALLOC(type, count)    (type*)x265_malloc(sizeof(type) * (count))
#define X265_FREE(ptr)              x265_free(ptr)
#define X265_FREE_ZERO(ptr)         x265_free(ptr); (ptr) = NULL
#define CHECKED_MALLOC(var, type, count) \
    { \
        var = (type*)x265_malloc(sizeof(type) * (count)); \
        if (!var) \
        { \
            x265_log(NULL, X265_LOG_ERROR, "malloc of size %d failed\n", sizeof(type) * (count)); \
            goto fail; \
        } \
    }
#define CHECKED_MALLOC_ZERO(var, type, count) \
    { \
        var = (type*)x265_malloc(sizeof(type) * (count)); \
        if (var) \
            memset((void*)var, 0, sizeof(type) * (count)); \
        else \
        { \
            x265_log(NULL, X265_LOG_ERROR, "malloc of size %d failed\n", sizeof(type) * (count)); \
            goto fail; \
        } \
    }

#if defined(_MSC_VER)
#define X265_LOG2F(x) (logf((float)(x)) * 1.44269504088896405f)
#define X265_LOG2(x) (log((double)(x)) * 1.4426950408889640513713538072172)
#else
#define X265_LOG2F(x) log2f(x)
#define X265_LOG2(x)  log2(x)
#endif

#define NUM_CU_DEPTH            4                           // maximum number of CU depths
#define NUM_FULL_DEPTH          5                           // maximum number of full depths
#define MIN_LOG2_CU_SIZE        3                           // log2(minCUSize)
#define MAX_LOG2_CU_SIZE        6                           // log2(maxCUSize)
#define MIN_CU_SIZE             (1 << MIN_LOG2_CU_SIZE)     // minimum allowable size of CU
#define MAX_CU_SIZE             (1 << MAX_LOG2_CU_SIZE)     // maximum allowable size of CU

#define LOG2_UNIT_SIZE          2                           // log2(unitSize)
#define UNIT_SIZE               (1 << LOG2_UNIT_SIZE)       // unit size of CU partition

#define LOG2_RASTER_SIZE        (MAX_LOG2_CU_SIZE - LOG2_UNIT_SIZE)
#define RASTER_SIZE             (1 << LOG2_RASTER_SIZE)
#define MAX_NUM_PARTITIONS      (RASTER_SIZE * RASTER_SIZE)

#define MIN_PU_SIZE             4
#define MIN_TU_SIZE             4
#define MAX_NUM_SPU_W           (MAX_CU_SIZE / MIN_PU_SIZE) // maximum number of SPU in horizontal line

#define MAX_LOG2_TR_SIZE 5
#define MAX_LOG2_TS_SIZE 2 // TODO: RExt
#define MAX_TR_SIZE (1 << MAX_LOG2_TR_SIZE)
#define MAX_TS_SIZE (1 << MAX_LOG2_TS_SIZE)

#define COEF_REMAIN_BIN_REDUCTION   3 // indicates the level at which the VLC
                                      // transitions from Golomb-Rice to TU+EG(k)

#define SBH_THRESHOLD               4 // fixed sign bit hiding controlling threshold

#define C1FLAG_NUMBER               8 // maximum number of largerThan1 flag coded in one chunk:  16 in HM5
#define C2FLAG_NUMBER               1 // maximum number of largerThan2 flag coded in one chunk:  16 in HM5

#define SAO_ENCODING_RATE           0.75
#define SAO_ENCODING_RATE_CHROMA    0.5

#define MLS_GRP_NUM                 64 // Max number of coefficient groups, max(16, 64)
#define MLS_CG_SIZE                 4  // Coefficient group size of 4x4
#define MLS_CG_BLK_SIZE             (MLS_CG_SIZE * MLS_CG_SIZE)
#define MLS_CG_LOG2_SIZE            2

#define QUANT_IQUANT_SHIFT          20 // Q(QP%6) * IQ(QP%6) = 2^20
#define QUANT_SHIFT                 14 // Q(4) = 2^14
#define SCALE_BITS                  15 // Inherited from TMuC, presumably for fractional bit estimates in RDOQ
#define MAX_TR_DYNAMIC_RANGE        15 // Maximum transform dynamic range (excluding sign bit)

#define SHIFT_INV_1ST               7  // Shift after first inverse transform stage
#define SHIFT_INV_2ND               12 // Shift after second inverse transform stage

#define AMVP_DECIMATION_FACTOR      4

#define SCAN_SET_SIZE               16
#define LOG2_SCAN_SET_SIZE          4

#define ALL_IDX                     -1
#define PLANAR_IDX                  0
#define VER_IDX                     26 // index for intra VERTICAL   mode
#define HOR_IDX                     10 // index for intra HORIZONTAL mode
#define DC_IDX                      1  // index for intra DC mode
#define NUM_CHROMA_MODE             5  // total number of chroma modes
#define DM_CHROMA_IDX               36 // chroma mode index for derived from luma intra mode

#define MDCS_ANGLE_LIMIT            4 // distance from true angle that horiz or vertical scan is allowed
#define MDCS_LOG2_MAX_SIZE          3 // TUs with log2 of size greater than this can only use diagonal scan

#define MAX_NUM_REF_PICS            16 // max. number of pictures used for reference
#define MAX_NUM_REF                 16 // max. number of entries in picture reference list
#define MAX_NUM_SHORT_TERM_RPS      64 // max. number of short term reference picture set in SPS

#define REF_NOT_VALID               -1

#define AMVP_NUM_CANDS              2 // number of AMVP candidates
#define MRG_MAX_NUM_CANDS           5 // max number of final merge candidates

#define CHROMA_H_SHIFT(x) (x == X265_CSP_I420 || x == X265_CSP_I422)
#define CHROMA_V_SHIFT(x) (x == X265_CSP_I420)
#define X265_MAX_PRED_MODE_PER_CTU 85 * 2 * 8

#define MAX_NUM_TR_COEFFS           MAX_TR_SIZE * MAX_TR_SIZE // Maximum number of transform coefficients, for a 32x32 transform
#define MAX_NUM_TR_CATEGORIES       16                        // 32, 16, 8, 4 transform categories each for luma and chroma

#define PIXEL_MAX ((1 << X265_DEPTH) - 1)

#define INTEGRAL_PLANE_NUM          12 // 12 integral planes for 32x32, 32x24, 32x8, 24x32, 16x16, 16x12, 16x4, 12x16, 8x32, 8x8, 4x16 and 4x4.

#define NAL_TYPE_OVERHEAD 2
#define START_CODE_OVERHEAD 3 
#define FILLER_OVERHEAD (NAL_TYPE_OVERHEAD + START_CODE_OVERHEAD + 1)

namespace X265_NS {

enum { SAO_NUM_OFFSET = 4 };

enum SaoMergeMode
{
    SAO_MERGE_NONE,
    SAO_MERGE_LEFT,
    SAO_MERGE_UP
};

struct SaoCtuParam
{
    SaoMergeMode mergeMode;
    int  typeIdx;
    uint32_t bandPos;    // BO band position
    int  offset[SAO_NUM_OFFSET];

    void reset()
    {
        mergeMode = SAO_MERGE_NONE;
        typeIdx = -1;
        bandPos = 0;
        offset[0] = 0;
        offset[1] = 0;
        offset[2] = 0;
        offset[3] = 0;
    }
};

struct SAOParam
{
    SaoCtuParam* ctuParam[3];
    bool         bSaoFlag[2];
    int          numCuInWidth;

    SAOParam()
    {
        for (int i = 0; i < 3; i++)
            ctuParam[i] = NULL;
    }

    ~SAOParam()
    {
        delete[] ctuParam[0];
        delete[] ctuParam[1];
        delete[] ctuParam[2];
    }
};
enum TextType
{
    TEXT_LUMA     = 0,  // luma
    TEXT_CHROMA_U = 1,  // chroma U
    TEXT_CHROMA_V = 2,  // chroma V
    MAX_NUM_COMPONENT = 3
};

// coefficient scanning type used in ACS
enum ScanType
{
    SCAN_DIAG = 0,     // up-right diagonal scan
    SCAN_HOR = 1,      // horizontal first scan
    SCAN_VER = 2,      // vertical first scan
    NUM_SCAN_TYPE = 3
};

enum SignificanceMapContextType
{
    CONTEXT_TYPE_4x4 = 0,
    CONTEXT_TYPE_8x8 = 1,
    CONTEXT_TYPE_NxN = 2,
    CONTEXT_NUMBER_OF_TYPES = 3
};

/* located in pixel.cpp */
void extendPicBorder(pixel* recon, intptr_t stride, int width, int height, int marginX, int marginY);

/* located in common.cpp */
int64_t  x265_mdate(void);
#define  x265_log(param, ...) general_log(param, "x265", __VA_ARGS__)
#define  x265_log_file(param, ...) general_log_file(param, "x265", __VA_ARGS__)
void     general_log(const x265_param* param, const char* caller, int level, const char* fmt, ...);
#if _WIN32
void     general_log_file(const x265_param* param, const char* caller, int level, const char* fmt, ...);
FILE*    x265_fopen(const char* fileName, const char* mode);
int      x265_unlink(const char* fileName);
int      x265_rename(const char* oldName, const char* newName);
#else
#define  general_log_file(param, caller, level, fmt, ...) general_log(param, caller, level, fmt, __VA_ARGS__)
#define  x265_fopen(fileName, mode) fopen(fileName, mode)
#define  x265_unlink(fileName) unlink(fileName)
#define  x265_rename(oldName, newName) rename(oldName, newName)
#endif
int      x265_exp2fix8(double x);

double   x265_ssim2dB(double ssim);
double   x265_qScale2qp(double qScale);
double   x265_qp2qScale(double qp);
uint32_t x265_picturePlaneSize(int csp, int width, int height, int plane);

void*    x265_malloc(size_t size);
void     x265_free(void *ptr);
char*    x265_slurp_file(const char *filename);

/* located in primitives.cpp */
void     x265_setup_primitives(x265_param* param);
void     x265_report_simd(x265_param* param);
}

#include "constants.h"

#endif // ifndef X265_COMMON_H
