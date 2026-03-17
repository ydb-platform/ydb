/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * This file is part of libde265.
 *
 * libde265 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * libde265 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with libde265.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "fallback-dct.h"

#if defined(_MSC_VER) || defined(__MINGW32__)
# include <malloc.h>
#elif defined(HAVE_ALLOCA_H)
# include <alloca.h>
#endif

#include <assert.h>
#include <algorithm>


static void printMatrix(const char* name, const int16_t* v, int n)
{
  printf("--- %s ---\n",name);
  for (int r=0;r<n;r++) {
    for (int c=0;c<n;c++) {
      printf("%4d ",v[c+r*n]);
    }
    printf("\n");
  }
}



void transform_skip_8_fallback(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride)
{
  int nT = 4;
  int bdShift2 = 20-8;

  assert(0); // DEPRECATED, should not be used anymore because of fixed 4x4 size

  for (int y=0;y<nT;y++)
    for (int x=0;x<nT;x++) {
      int32_t c = coeffs[x+y*nT] << 7;
      c = (c+(1<<(bdShift2-1)))>>bdShift2;

      dst[y*stride+x] = Clip1_8bit(dst[y*stride+x] + c);
    }
}


void transform_skip_16_fallback(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth)
{
  int nT = 4;
  int bdShift2 = 20-bit_depth;

  assert(0); // DEPRECATED, should not be used anymore because of fixed 4x4 size

  for (int y=0;y<nT;y++)
    for (int x=0;x<nT;x++) {
      int32_t c = coeffs[x+y*nT] << 7;
      c = (c+(1<<(bdShift2-1)))>>bdShift2;

      dst[y*stride+x] = Clip_BitDepth(dst[y*stride+x] + c, bit_depth);
    }
}


void transform_skip_residual_fallback(int32_t *residual, const int16_t *coeffs, int nT,
                                      int tsShift,int bdShift)
{
  const int rnd = 1<<(bdShift-1);

  for (int y=0;y<nT;y++)
    for (int x=0;x<nT;x++) {
      int32_t c = coeffs[x+y*nT] << tsShift;
      residual[x+y*nT] = (c + rnd) >> bdShift;
    }
}


void transform_skip_rdpcm_v_8_fallback(uint8_t *dst, const int16_t *coeffs, int log2nT, ptrdiff_t stride)
{
  int bitDepth = 8;
  int bdShift2 = 20-bitDepth;
  int offset = (1<<(bdShift2-1));
  int tsShift = 5 + log2nT; // TODO: extended_precision
  int nT = 1<<log2nT;

  for (int x=0;x<nT;x++) {
    int32_t sum = 0;

    for (int y=0;y<nT;y++) {
      int c = coeffs[x+y*nT] << tsShift;
      sum += (c+offset)>>bdShift2;

      dst[y*stride+x] = Clip1_8bit(dst[y*stride+x] + sum);
    }
  }
}

void transform_skip_rdpcm_h_8_fallback(uint8_t *dst, const int16_t *coeffs, int log2nT, ptrdiff_t stride)
{
  int bitDepth = 8;
  int bdShift2 = 20-bitDepth;
  int offset = (1<<(bdShift2-1));
  int tsShift = 5 + log2nT; // TODO: extended_precision
  int nT = 1<<log2nT;

  for (int y=0;y<nT;y++) {
    int32_t sum = 0;

    for (int x=0;x<nT;x++) {
      int c = coeffs[x+y*nT] << tsShift;
      sum += (c+offset)>>bdShift2;

      dst[y*stride+x] = Clip1_8bit(dst[y*stride+x] + sum);
    }
  }
}


void transform_bypass_rdpcm_v_8_fallback(uint8_t *dst, const int16_t *coeffs,int nT,ptrdiff_t stride)
{
  for (int x=0;x<nT;x++) {
    int32_t sum=0;
    for (int y=0;y<nT;y++) {
      sum += coeffs[x+y*nT];

      dst[y*stride+x] = Clip1_8bit(dst[y*stride+x] + sum);
    }
  }
}


void transform_bypass_rdpcm_h_8_fallback(uint8_t *dst, const int16_t *coeffs,int nT,ptrdiff_t stride)
{
  for (int y=0;y<nT;y++) {
    int32_t sum=0;
    for (int x=0;x<nT;x++) {
      sum += coeffs[x+y*nT];

      dst[y*stride+x] = Clip1_8bit(dst[y*stride+x] + sum);
    }
  }
}


void transform_bypass_rdpcm_v_fallback(int32_t *dst, const int16_t *coeffs,int nT)
{
  for (int x=0;x<nT;x++) {
    int32_t sum=0;
    for (int y=0;y<nT;y++) {
      sum += coeffs[x+y*nT];

      dst[y*nT+x] = sum;
    }
  }
}


void transform_bypass_rdpcm_h_fallback(int32_t *dst, const int16_t *coeffs,int nT)
{
  for (int y=0;y<nT;y++) {
    int32_t sum=0;
    for (int x=0;x<nT;x++) {
      sum += coeffs[x+y*nT];

      dst[y*nT+x] = sum;
    }
  }
}


void rdpcm_v_fallback(int32_t* residual, const int16_t* coeffs, int nT,int tsShift,int bdShift)
{
  int rnd = (1<<(bdShift-1));

  for (int x=0;x<nT;x++) {
    int sum=0;
    for (int y=0;y<nT;y++) {
      int c = coeffs[x+y*nT] << tsShift;
      sum += (c+rnd)>>bdShift;
      residual[y*nT+x] = sum;
    }
  }
}


void rdpcm_h_fallback(int32_t* residual, const int16_t* coeffs, int nT,int tsShift,int bdShift)
{
  int rnd = (1<<(bdShift-1));

  for (int y=0;y<nT;y++) {
    int sum=0;
    for (int x=0;x<nT;x++) {
      int c = coeffs[x+y*nT] << tsShift;
      sum += (c+rnd)>>bdShift;
      residual[y*nT+x] = sum;
    }
  }
}


void transform_bypass_fallback(int32_t *dst, const int16_t *coeffs, int nT)
{
  for (int y=0;y<nT;y++)
    for (int x=0;x<nT;x++) {
      int32_t c = coeffs[x+y*nT];

      dst[y*nT+x] = c;
    }
}


void transform_bypass_8_fallback(uint8_t *dst, const int16_t *coeffs, int nT, ptrdiff_t stride)
{
  for (int y=0;y<nT;y++)
    for (int x=0;x<nT;x++) {
      int32_t c = coeffs[x+y*nT];

      dst[y*stride+x] = Clip1_8bit(dst[y*stride+x] + c);
    }
}


void transform_bypass_16_fallback(uint16_t *dst, const int16_t *coeffs, int nT, ptrdiff_t stride, int bit_depth)
{
  int bdShift2 = 20-bit_depth;

  for (int y=0;y<nT;y++)
    for (int x=0;x<nT;x++) {
      int32_t c = coeffs[x+y*nT];

      dst[y*stride+x] = Clip_BitDepth(dst[y*stride+x] + c, bit_depth);
    }
}


void rotate_coefficients_fallback(int16_t *coeff, int nT)
{
  for (int y=0;y<nT/2;y++)
    for (int x=0;x<nT;x++) {
      std::swap(coeff[y*nT+x], coeff[(nT-1-y)*nT + nT-1-x]);
    }
}



static int8_t mat_8_357[4][4] = {
  { 29, 55, 74, 84 },
  { 74, 74,  0,-74 },
  { 84,-29,-74, 55 },
  { 55,-84, 74,-29 }
};



void transform_4x4_luma_add_8_fallback(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride)
{
  int16_t g[4][4];

  int postShift = 20-8; // 8 bit
  int rndV = 1<<(7-1);
  int rndH = 1<<(postShift-1);


  // --- V ---

  for (int c=0;c<4;c++) {
    /*
    logtrace(LogTransform,"DST-V: ");
    for (int r=0;r<4;r++) {
      logtrace(LogTransform,"%d ",coeffs[c+r*4]);
    }
    logtrace(LogTransform,"* -> ");
    */

    for (int i=0;i<4;i++) {
      int sum=0;

      for (int j=0;j<4;j++) {
        sum += mat_8_357[j][i] * coeffs[c+j*4];
      }

      g[i][c] = Clip3(-32768,32767, (sum+rndV)>>7);
    }

    /*
    for (int y=0;y<4;y++) {
      logtrace(LogTransform,"*%d ",g[y][c]);
    }
    logtrace(LogTransform,"*\n");
    */
  }


  // --- H ---

  for (int y=0;y<4;y++) {

    /*
    logtrace(LogTransform,"DST-H: ");
    for (int c=0;c<4;c++) {
      logtrace(LogTransform,"%d ",g[y][c]);
    }
    logtrace(LogTransform,"* -> ");
    */

    for (int i=0;i<4;i++) {
      int sum=0;

      for (int j=0;j<4;j++) {
        sum += mat_8_357[j][i] * g[y][j];
      }

      int out = Clip3(-32768,32767, (sum+rndH)>>postShift);

      dst[y*stride+i] = Clip1_8bit(dst[y*stride+i] + out);

      logtrace(LogTransform,"*%d ",out);
    }

    logtrace(LogTransform,"*\n");
  }
}


void transform_4x4_luma_add_16_fallback(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride,
                                        int bit_depth)
{
  int16_t g[4][4];

  int postShift = 20-bit_depth;
  int rndV = 1<<(7-1);
  int rndH = 1<<(postShift-1);


  // --- V ---

  for (int c=0;c<4;c++) {
    /*
    logtrace(LogTransform,"DST-V: ");
    for (int r=0;r<4;r++) {
      logtrace(LogTransform,"%d ",coeffs[c+r*4]);
    }
    logtrace(LogTransform,"* -> ");
    */

    for (int i=0;i<4;i++) {
      int sum=0;

      for (int j=0;j<4;j++) {
        sum += mat_8_357[j][i] * coeffs[c+j*4];
      }

      g[i][c] = Clip3(-32768,32767, (sum+rndV)>>7);
    }

    /*
    for (int y=0;y<4;y++) {
      logtrace(LogTransform,"*%d ",g[y][c]);
    }
    logtrace(LogTransform,"*\n");
    */
  }


  // --- H ---

  for (int y=0;y<4;y++) {

    /*
    logtrace(LogTransform,"DST-H: ");
    for (int c=0;c<4;c++) {
      logtrace(LogTransform,"%d ",g[y][c]);
    }
    logtrace(LogTransform,"* -> ");
    */

    for (int i=0;i<4;i++) {
      int sum=0;

      for (int j=0;j<4;j++) {
        sum += mat_8_357[j][i] * g[y][j];
      }

      int out = Clip3(-32768,32767, (sum+rndH)>>postShift);

      dst[y*stride+i] = Clip_BitDepth(dst[y*stride+i] + out, bit_depth);

      logtrace(LogTransform,"*%d ",out);
    }

    logtrace(LogTransform,"*\n");
  }
}


void fdst_4x4_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  int16_t g[4*4];

  int BD = 8;
  int shift1 = Log2(4) + BD -9;
  int shift2 = Log2(4) + 6;

  int rnd1 = 1<<(shift1-1);
  int rnd2 = 1<<(shift2-1);


  // --- V ---

  for (int c=0;c<4;c++) {

    /*
    logtrace(LogTransform,"DST-V: ");
    for (int r=0;r<4;r++) {
      logtrace(LogTransform,"%d ",coeffs[c+r*4]);
    }
    logtrace(LogTransform,"* -> ");
    */

    for (int i=0;i<4;i++) {
      int sum=0;

      for (int j=0;j<4;j++) {
        sum += mat_8_357[i][j] * input[c+j*stride];
      }

      g[c+4*i] = Clip3(-32768,32767, (sum+rnd1)>>shift1);
    }
  }


  // --- H ---

  for (int y=0;y<4;y++) {
    for (int i=0;i<4;i++) {
      int sum=0;

      for (int j=0;j<4;j++) {
        sum += mat_8_357[i][j] * g[y*4+j];
      }

      // TODO: do we need clipping ?
      int out = (sum+rnd2)>>shift2; // Clip3(-32768,32767, (sum+rndH)>>postShift);

      coeffs[y*4+i] = out;

      logtrace(LogTransform,"*%d ",out);
    }

    logtrace(LogTransform,"*\n");
  }
}


void transform_idst_4x4_fallback(int32_t *dst, const int16_t *coeffs, int bdShift, int max_coeff_bits)
{
  int16_t g[4][4];

  int rndV = 1<<(7-1);
  int rndH = 1<<(bdShift-1);

  int CoeffMax = (1<<max_coeff_bits)-1;
  int CoeffMin = -(1<<max_coeff_bits);


  // --- V ---

  for (int c=0;c<4;c++) {
    for (int i=0;i<4;i++) {
      int sum=0;

      for (int j=0;j<4;j++) {
        sum += mat_8_357[j][i] * coeffs[c+j*4];
      }

      g[i][c] = Clip3(CoeffMin,CoeffMax, (sum+rndV)>>7);
    }
  }


  // --- H ---

  for (int y=0;y<4;y++) {
    for (int i=0;i<4;i++) {
      int sum=0;

      for (int j=0;j<4;j++) {
        sum += mat_8_357[j][i] * g[y][j];
      }

      dst[y*4+i] = (sum + rndH)>>bdShift;
    }
  }
}



static int8_t mat_dct[32][32] = {
  { 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64},
  { 90, 90, 88, 85, 82, 78, 73, 67, 61, 54, 46, 38, 31, 22, 13,  4,      -4,-13,-22,-31,-38,-46,-54,-61,-67,-73,-78,-82,-85,-88,-90,-90},
  { 90, 87, 80, 70, 57, 43, 25,  9, -9,-25,-43,-57,-70,-80,-87,-90,     -90,-87,-80,-70,-57,-43,-25, -9,  9, 25, 43, 57, 70, 80, 87, 90},
  { 90, 82, 67, 46, 22, -4,-31,-54,-73,-85,-90,-88,-78,-61,-38,-13,      13, 38, 61, 78, 88, 90, 85, 73, 54, 31,  4,-22,-46,-67,-82,-90},
  { 89, 75, 50, 18,-18,-50,-75,-89,-89,-75,-50,-18, 18, 50, 75, 89,      89, 75, 50, 18,-18,-50,-75,-89,-89,-75,-50,-18, 18, 50, 75, 89},
  { 88, 67, 31,-13,-54,-82,-90,-78,-46, -4, 38, 73, 90, 85, 61, 22,     -22,-61,-85,-90,-73,-38,  4, 46, 78, 90, 82, 54, 13,-31,-67,-88},
  { 87, 57,  9,-43,-80,-90,-70,-25, 25, 70, 90, 80, 43, -9,-57,-87,     -87,-57, -9, 43, 80, 90, 70, 25,-25,-70,-90,-80,-43,  9, 57, 87},
  { 85, 46,-13,-67,-90,-73,-22, 38, 82, 88, 54, -4,-61,-90,-78,-31,      31, 78, 90, 61,  4,-54,-88,-82,-38, 22, 73, 90, 67, 13,-46,-85},
  { 83, 36,-36,-83,-83,-36, 36, 83, 83, 36,-36,-83,-83,-36, 36, 83,      83, 36,-36,-83,-83,-36, 36, 83, 83, 36,-36,-83,-83,-36, 36, 83},
  { 82, 22,-54,-90,-61, 13, 78, 85, 31,-46,-90,-67,  4, 73, 88, 38,     -38,-88,-73, -4, 67, 90, 46,-31,-85,-78,-13, 61, 90, 54,-22,-82},
  { 80,  9,-70,-87,-25, 57, 90, 43,-43,-90,-57, 25, 87, 70, -9,-80,     -80, -9, 70, 87, 25,-57,-90,-43, 43, 90, 57,-25,-87,-70,  9, 80},
  { 78, -4,-82,-73, 13, 85, 67,-22,-88,-61, 31, 90, 54,-38,-90,-46,      46, 90, 38,-54,-90,-31, 61, 88, 22,-67,-85,-13, 73, 82,  4,-78},
  { 75,-18,-89,-50, 50, 89, 18,-75,-75, 18, 89, 50,-50,-89,-18, 75,      75,-18,-89,-50, 50, 89, 18,-75,-75, 18, 89, 50,-50,-89,-18, 75},
  { 73,-31,-90,-22, 78, 67,-38,-90,-13, 82, 61,-46,-88, -4, 85, 54,     -54,-85,  4, 88, 46,-61,-82, 13, 90, 38,-67,-78, 22, 90, 31,-73},
  { 70,-43,-87,  9, 90, 25,-80,-57, 57, 80,-25,-90, -9, 87, 43,-70,     -70, 43, 87, -9,-90,-25, 80, 57,-57,-80, 25, 90,  9,-87,-43, 70},
  { 67,-54,-78, 38, 85,-22,-90,  4, 90, 13,-88,-31, 82, 46,-73,-61,      61, 73,-46,-82, 31, 88,-13,-90, -4, 90, 22,-85,-38, 78, 54,-67},
  { 64,-64,-64, 64, 64,-64,-64, 64, 64,-64,-64, 64, 64,-64,-64, 64,      64,-64,-64, 64, 64,-64,-64, 64, 64,-64,-64, 64, 64,-64,-64, 64},
  { 61,-73,-46, 82, 31,-88,-13, 90, -4,-90, 22, 85,-38,-78, 54, 67,     -67,-54, 78, 38,-85,-22, 90,  4,-90, 13, 88,-31,-82, 46, 73,-61},
  { 57,-80,-25, 90, -9,-87, 43, 70,-70,-43, 87,  9,-90, 25, 80,-57,     -57, 80, 25,-90,  9, 87,-43,-70, 70, 43,-87, -9, 90,-25,-80, 57},
  { 54,-85, -4, 88,-46,-61, 82, 13,-90, 38, 67,-78,-22, 90,-31,-73,      73, 31,-90, 22, 78,-67,-38, 90,-13,-82, 61, 46,-88,  4, 85,-54},
  { 50,-89, 18, 75,-75,-18, 89,-50,-50, 89,-18,-75, 75, 18,-89, 50,      50,-89, 18, 75,-75,-18, 89,-50,-50, 89,-18,-75, 75, 18,-89, 50},
  { 46,-90, 38, 54,-90, 31, 61,-88, 22, 67,-85, 13, 73,-82,  4, 78,     -78, -4, 82,-73,-13, 85,-67,-22, 88,-61,-31, 90,-54,-38, 90,-46},
  { 43,-90, 57, 25,-87, 70,  9,-80, 80, -9,-70, 87,-25,-57, 90,-43,     -43, 90,-57,-25, 87,-70, -9, 80,-80,  9, 70,-87, 25, 57,-90, 43},
  { 38,-88, 73, -4,-67, 90,-46,-31, 85,-78, 13, 61,-90, 54, 22,-82,      82,-22,-54, 90,-61,-13, 78,-85, 31, 46,-90, 67,  4,-73, 88,-38},
  { 36,-83, 83,-36,-36, 83,-83, 36, 36,-83, 83,-36,-36, 83,-83, 36,      36,-83, 83,-36,-36, 83,-83, 36, 36,-83, 83,-36,-36, 83,-83, 36},
  { 31,-78, 90,-61,  4, 54,-88, 82,-38,-22, 73,-90, 67,-13,-46, 85,     -85, 46, 13,-67, 90,-73, 22, 38,-82, 88,-54, -4, 61,-90, 78,-31},
  { 25,-70, 90,-80, 43,  9,-57, 87,-87, 57, -9,-43, 80,-90, 70,-25,     -25, 70,-90, 80,-43, -9, 57,-87, 87,-57,  9, 43,-80, 90,-70, 25},
  { 22,-61, 85,-90, 73,-38, -4, 46,-78, 90,-82, 54,-13,-31, 67,-88,      88,-67, 31, 13,-54, 82,-90, 78,-46,  4, 38,-73, 90,-85, 61,-22},
  { 18,-50, 75,-89, 89,-75, 50,-18,-18, 50,-75, 89,-89, 75,-50, 18,      18,-50, 75,-89, 89,-75, 50,-18,-18, 50,-75, 89,-89, 75,-50, 18},
  { 13,-38, 61,-78, 88,-90, 85,-73, 54,-31,  4, 22,-46, 67,-82, 90,     -90, 82,-67, 46,-22, -4, 31,-54, 73,-85, 90,-88, 78,-61, 38,-13},
  {  9,-25, 43,-57, 70,-80, 87,-90, 90,-87, 80,-70, 57,-43, 25, -9,      -9, 25,-43, 57,-70, 80,-87, 90,-90, 87,-80, 70,-57, 43,-25,  9},
  {  4,-13, 22,-31, 38,-46, 54,-61, 67,-73, 78,-82, 85,-88, 90,-90,      90,-90, 88,-85, 82,-78, 73,-67, 61,-54, 46,-38, 31,-22, 13, -4}
};




template <class pixel_t>
void transform_idct_add(pixel_t *dst, ptrdiff_t stride,
                        int nT, const int16_t *coeffs, int bit_depth)
{
  /*
    The effective shift is
    7 bits right for bit-depth 8,
    6 bits right for bit-depth 9,
    5 bits right for bit-depth 10.

    Computation is independent of the block size.
    Each multiplication with the table includes a left shift of 6 bits.
    Hence, we have 2* 6 bits = 12 bits left shift.
    V-pass has fixed 7 bit right shift.
    H-pass has 20-BitDepth bit right shift;

    Effective shift 's' means: residual value 1 gives DC-coeff (1<<s).
   */


  int postShift = 20-bit_depth;
  int rnd1 = 1<<(7-1);
  int rnd2 = 1<<(postShift-1);
  int fact = (1<<(5-Log2(nT)));

  int16_t g[32*32];  // actually, only [nT*nT] used

  // TODO: valgrind reports that dst[] contains uninitialized data.
  // Probably from intra-prediction.

  /*
  for (int i=0;i<nT*nT;i++) {
    printf("%d\n",coeffs[i]);
  }

  for (int y=0;y<nT;y++) {
    for (int i=0;i<nT;i++) {
      printf("%d ",dst[y*stride+i]);
    }
  }
  printf("\n");
  */

  /*
  printf("--- input\n");
  for (int r=0;r<nT;r++, printf("\n"))
    for (int c=0;c<nT;c++) {
      printf("%3d ",coeffs[c+r*nT]);
    }
  */

  for (int c=0;c<nT;c++) {

    /*
    logtrace(LogTransform,"DCT-V: ");
    for (int i=0;i<nT;i++) {
      logtrace(LogTransform,"*%d ",coeffs[c+i*nT]);
    }
    logtrace(LogTransform,"* -> ");
    */


    // find last non-zero coefficient to reduce computations carried out in DCT

    int lastCol = nT-1;
    for (;lastCol>=0;lastCol--) {
      if (coeffs[c+lastCol*nT]) { break; }
    }

    for (int i=0;i<nT;i++) {
      int sum=0;

      /*
      printf("input: ");
      for (int j=0;j<nT;j++) {
        printf("%3d ",coeffs[c+j*nT]);
      }
      printf("\n");

      printf("mat: ");
      for (int j=0;j<nT;j++) {
        printf("%3d ",mat_dct[fact*j][i]);
      }
      printf("\n");
      */

      for (int j=0;j<=lastCol /*nT*/;j++) {
        sum += mat_dct[fact*j][i] * coeffs[c+j*nT];
      }

      g[c+i*nT] = Clip3(-32768,32767, (sum+rnd1)>>7);

      logtrace(LogTransform,"*%d ",g[c+i*nT]);
    }
    logtrace(LogTransform,"*\n");
  }

  /*
  printf("--- temp\n");
  for (int r=0;r<nT;r++, printf("\n"))
    for (int c=0;c<nT;c++) {
      printf("%3d ",g[c+r*nT]);
    }
  */

  for (int y=0;y<nT;y++) {
    /*
    logtrace(LogTransform,"DCT-H: ");
    for (int i=0;i<nT;i++) {
      logtrace(LogTransform,"*%d ",g[i+y*nT]);
    }
    logtrace(LogTransform,"* -> ");
    */


    // find last non-zero coefficient to reduce computations carried out in DCT

    int lastCol = nT-1;
    for (;lastCol>=0;lastCol--) {
      if (g[y*nT+lastCol]) { break; }
    }


    for (int i=0;i<nT;i++) {
      int sum=0;

      for (int j=0;j<=lastCol /*nT*/;j++) {
        sum += mat_dct[fact*j][i] * g[y*nT+j];
      }

      //int out = Clip3(-32768,32767, (sum+rnd2)>>postShift);
      int out = (sum+rnd2)>>postShift;

      //fprintf(stderr,"%d*%d+%d = %d\n",y,stride,i,y*stride+i);
      //fprintf(stderr,"[%p]=%d\n",&dst[y*stride+i], Clip1_8bit(dst[y*stride+i]));
      dst[y*stride+i] = Clip_BitDepth(dst[y*stride+i] + out, bit_depth);

      logtrace(LogTransform,"*%d ",out);
    }
    logtrace(LogTransform,"*\n");
  }
}



void transform_idct_fallback(int32_t *dst, int nT, const int16_t *coeffs, int bdShift, int max_coeff_bits)
{
  /*
    The effective shift is
    7 bits right for bit-depth 8,
    6 bits right for bit-depth 9,
    5 bits right for bit-depth 10.

    One transformation with raw transform filter values increases range be 2048 (=32*64).
    This equals 11 bits.

    Computation is independent of the block size.
    Each multiplication with the table includes a left shift of 6 bits.
    Hence, we have 2* 6 bits = 12 bits left shift.
    V-pass has fixed 7 bit right shift.
    H-pass has 20-BitDepth bit right shift;

    Effective shift 's' means: residual value 1 gives DC-coeff (1<<s).
   */


  int rnd1 = 1<<(7-1);
  int fact = (1<<(5-Log2(nT)));

  //int bdShift = 20-bit_depth;
  int rnd2 = 1<<(bdShift-1);

  int16_t g[32*32];  // actually, only [nT*nT] used

  int CoeffMax = (1<<max_coeff_bits)-1;
  int CoeffMin = -(1<<max_coeff_bits);

  // TODO: valgrind reports that dst[] contains uninitialized data.
  // Probably from intra-prediction.

  /*
  for (int i=0;i<nT*nT;i++) {
    printf("%d\n",coeffs[i]);
  }

  for (int y=0;y<nT;y++) {
    for (int i=0;i<nT;i++) {
      printf("%d ",dst[y*stride+i]);
    }
  }
  printf("\n");
  */

  /*
  printf("--- input\n");
  for (int r=0;r<nT;r++, printf("\n"))
    for (int c=0;c<nT;c++) {
      printf("%3d ",coeffs[c+r*nT]);
    }
  */

  for (int c=0;c<nT;c++) {

    /*
    logtrace(LogTransform,"DCT-V: ");
    for (int i=0;i<nT;i++) {
      logtrace(LogTransform,"*%d ",coeffs[c+i*nT]);
    }
    logtrace(LogTransform,"* -> ");
    */


    // find last non-zero coefficient to reduce computations carried out in DCT

    int lastCol = nT-1;
    for (;lastCol>=0;lastCol--) {
      if (coeffs[c+lastCol*nT]) { break; }
    }

    for (int i=0;i<nT;i++) {
      int sum=0;

      /*
      printf("input: ");
      for (int j=0;j<nT;j++) {
        printf("%3d ",coeffs[c+j*nT]);
      }
      printf("\n");

      printf("mat: ");
      for (int j=0;j<nT;j++) {
        printf("%3d ",mat_dct[fact*j][i]);
      }
      printf("\n");
      */

      for (int j=0;j<=lastCol /*nT*/;j++) {
        sum += mat_dct[fact*j][i] * coeffs[c+j*nT];
      }

      g[c+i*nT] = Clip3(CoeffMin,CoeffMax, (sum+rnd1)>>7);

      logtrace(LogTransform,"*%d ",g[c+i*nT]);
    }
    logtrace(LogTransform,"*\n");
  }

  /*
  printf("--- temp\n");
  for (int r=0;r<nT;r++, printf("\n"))
    for (int c=0;c<nT;c++) {
      printf("%3d ",g[c+r*nT]);
    }
  */

  for (int y=0;y<nT;y++) {
    /*
    logtrace(LogTransform,"DCT-H: ");
    for (int i=0;i<nT;i++) {
      logtrace(LogTransform,"*%d ",g[i+y*nT]);
    }
    logtrace(LogTransform,"* -> ");
    */


    // find last non-zero coefficient to reduce computations carried out in DCT

    int lastCol = nT-1;
    for (;lastCol>=0;lastCol--) {
      if (g[y*nT+lastCol]) { break; }
    }


    for (int i=0;i<nT;i++) {
      int sum=0;

      for (int j=0;j<=lastCol /*nT*/;j++) {
        sum += mat_dct[fact*j][i] * g[y*nT+j];
      }

      dst[y*nT+i] = (sum + rnd2)>>bdShift;

      logtrace(LogTransform,"*%d ",sum);
    }
    logtrace(LogTransform,"*\n");
  }
}


void transform_idct_4x4_fallback(int32_t *dst, const int16_t *coeffs, int bdShift, int max_coeff_bits)
{
  transform_idct_fallback(dst,4,coeffs,bdShift,max_coeff_bits);
}

void transform_idct_8x8_fallback(int32_t *dst, const int16_t *coeffs, int bdShift, int max_coeff_bits)
{
  transform_idct_fallback(dst,8,coeffs,bdShift,max_coeff_bits);
}

void transform_idct_16x16_fallback(int32_t *dst, const int16_t *coeffs,
                                   int bdShift, int max_coeff_bits)
{
  transform_idct_fallback(dst,16,coeffs,bdShift,max_coeff_bits);
}

void transform_idct_32x32_fallback(int32_t *dst, const int16_t *coeffs,
                                   int bdShift, int max_coeff_bits)
{
  transform_idct_fallback(dst,32,coeffs,bdShift,max_coeff_bits);
}




void transform_4x4_add_8_fallback(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride)
{
  transform_idct_add<uint8_t>(dst,stride,  4, coeffs, 8);
}

void transform_8x8_add_8_fallback(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride)
{
  transform_idct_add<uint8_t>(dst,stride,  8, coeffs, 8);
}

void transform_16x16_add_8_fallback(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride)
{
  transform_idct_add<uint8_t>(dst,stride,  16, coeffs, 8);
}

void transform_32x32_add_8_fallback(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride)
{
  transform_idct_add<uint8_t>(dst,stride,  32, coeffs, 8);
}


void transform_4x4_add_16_fallback(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth)
{
  transform_idct_add<uint16_t>(dst,stride,  4, coeffs, bit_depth);
}

void transform_8x8_add_16_fallback(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth)
{
  transform_idct_add<uint16_t>(dst,stride,  8, coeffs, bit_depth);
}

void transform_16x16_add_16_fallback(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth)
{
  transform_idct_add<uint16_t>(dst,stride,  16, coeffs, bit_depth);
}

void transform_32x32_add_16_fallback(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth)
{
  transform_idct_add<uint16_t>(dst,stride,  32, coeffs, bit_depth);
}


static void transform_fdct_8(int16_t* coeffs, int nT,
                             const int16_t *input, ptrdiff_t stride)
{
  /*
    Each sum over a basis vector sums nT elements, which is compensated by
    shifting right by Log2(nT), effectively dividing by 2^Log2(nT) = nT.
    Do this in each of the H/V passes.

    Each multiplication with the table includes a left shift of 6 bits.
    Hence, we have in total 2* 6 bits = 12 bits left shift because of the
    multiplications.

    We carry out shifts after each pass:
    First (V) pass has BitDepth-9 bit right shift,
    Second (H) pass has fixed 6 bit right shift.

    For bit-depth 8, the total shift is 7 bits left.
    For bit-depth 9, the total shift is 6 bits left.
    For bit-depth 10, the total shift is 5 bits left.

    I.e.: a constant residual value 1 gives DC-coeff (1<<s).

    For 8-bit images in a 32x32 block, the input are 8 bits + 1 sign bit.
    After the first pass, we need 9+5+6=20 bits for the intermediate sum
    (9 bit input, 5 bit because we sum 2^5 elements, 6 bit because of multiplication with 64).
    The first pass shift is Log2(32) - 1 -> 4 bits and we are down to 16 bits again.
    After the second pass, we need 16+5+6=27 bits for the intermediate sum
    (16 bit input, 5 bit because we sum 2^5 elements, 6 bit because of coefficient multiplication).
    The second pass shift is Log2(32)+6 = 11 and we are down again to 16 bits.

    For larger input bit-depths, the intermediate result after the first pass
    will be wider accordingly, but the widths after the shifts are the same.
  */

  int BitDepth = 8;

  //          / compensate everything | / effective word length |
  int shift1 = Log2(nT) + 6 + BitDepth  - 15;
  int shift2 = Log2(nT) + 6;

  int rnd1 = 1<<(shift1-1);
  int rnd2 = 1<<(shift2-1);
  int fact = (1<<(5-Log2(nT)));

  int16_t g[32*32];  // actually, only [nT*nT] used

  for (int c=0;c<nT;c++) {

    for (int i=0;i<nT;i++) {
      int sum=0;

      for (int j=0;j<nT;j++) {
        sum += mat_dct[fact*i][j] * input[c+j*stride];
      }

      g[c+i*nT] = (sum+rnd1)>>shift1; // clipping to -32768;32767 unnecessary
    }
  }


  for (int y=0;y<nT;y++) {
    for (int i=0;i<nT;i++) {
      int sum=0;

      for (int j=0;j<nT;j++) {
        sum += mat_dct[fact*i][j] * g[y*nT+j];
      }

      // no clipping to -32768;32767 required
      int out = (sum+rnd2)>>shift2;

      coeffs[y*nT+i] = out;
    }
  }
}


void fdct_4x4_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  transform_fdct_8(coeffs, 4, input,stride);
}

void fdct_8x8_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  transform_fdct_8(coeffs, 8, input,stride);
}

void fdct_16x16_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  transform_fdct_8(coeffs, 16, input,stride);
}

void fdct_32x32_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  transform_fdct_8(coeffs, 32, input,stride);
}




void hadamard_transform_8(int16_t *coeffs, int n, const int16_t *input, ptrdiff_t stride)
{
  int16_t tmp[32*32];

  // row transforms

  //printMatrix("input",input,n);

  int16_t am[32],bm[32];
  int16_t *a = am, *b = bm;
  for (int row=0;row<n;row++) {
    int rs = row*stride;
    for (int i=0;i<(n>>1);i++) {
      a[       i] = input[i+rs] + input[i+(n>>1)+rs];
      a[(n>>1)+i] = input[i+rs] - input[i+(n>>1)+rs];
    }

    int iOuter=(n>>1);
    int nInner=(n>>2);

    while (nInner>=2) {
      std::swap(a,b);

      for (int k=0;k<n;k+=iOuter) {
        for (int i=0;i<nInner;i++) {
          a[k+i       ] = b[k+i] + b[k+i+nInner];
          a[k+i+nInner] = b[k+i] - b[k+i+nInner];
        }
      }

      iOuter>>=1;
      nInner>>=1;
    }

    for (int k=0;k<n;k+=2) {
      tmp[k  +n*row] = a[k] + a[k+1];
      tmp[k+1+n*row] = a[k] - a[k+1];
    }
  }

  //printMatrix("tmp",tmp,n);

  // column transforms

  for (int col=0;col<n;col++) {
    for (int i=0;i<(n>>1);i++) {
      a[       i] = tmp[i*n+col] + tmp[(i+(n>>1))*n+col];
      a[(n>>1)+i] = tmp[i*n+col] - tmp[(i+(n>>1))*n+col];
    }

    int iOuter=(n>>1);
    int nInner=(n>>2);

    while (nInner>=2) {
      std::swap(a,b);

      for (int k=0;k<n;k+=iOuter) {
        for (int i=0;i<nInner;i++) {
          a[k+i       ] = b[k+i] + b[k+i+nInner];
          a[k+i+nInner] = b[k+i] - b[k+i+nInner];
        }
      }

      iOuter>>=1;
      nInner>>=1;
    }

    for (int k=0;k<n;k+=2) {
      coeffs[col+(k  )*n] = a[k] + a[k+1];
      coeffs[col+(k+1)*n] = a[k] - a[k+1];
    }
  }

  //printMatrix("coeffs",coeffs,n);
}


void hadamard_4x4_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  int16_t tmp[4*4];

  // row transforms

  //printMatrix("input",input,4);

  int16_t a[4];
  for (int row=0;row<4;row++) {
    int rs = row*stride;
    a[0] = input[0+rs] + input[2+rs];
    a[1] = input[1+rs] + input[3+rs];
    a[2] = input[0+rs] - input[2+rs];
    a[3] = input[1+rs] - input[3+rs];

    tmp[0+4*row] = a[0]+a[1];
    tmp[1+4*row] = a[0]-a[1];
    tmp[2+4*row] = a[2]+a[3];
    tmp[3+4*row] = a[2]-a[3];
  }

  //printMatrix("tmp",tmp,4);

  // column transforms

  for (int col=0;col<4;col++) {
    a[0] = tmp[col+0*4] + tmp[col+2*4];
    a[1] = tmp[col+1*4] + tmp[col+3*4];
    a[2] = tmp[col+0*4] - tmp[col+2*4];
    a[3] = tmp[col+1*4] - tmp[col+3*4];

    coeffs[col+0*4] = a[0]+a[1];
    coeffs[col+1*4] = a[0]-a[1];
    coeffs[col+2*4] = a[2]+a[3];
    coeffs[col+3*4] = a[2]-a[3];
  }

  //printMatrix("coeffs",coeffs,4);
}


void hadamard_8x8_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  int16_t tmp[8*8];

  // row transforms

  //printMatrix("input",input,8);

  int16_t a[8],b[8];
  for (int row=0;row<8;row++) {
    int rs = row*stride;
    a[0] = input[0+rs] + input[4+rs];
    a[1] = input[1+rs] + input[5+rs];
    a[2] = input[2+rs] + input[6+rs];
    a[3] = input[3+rs] + input[7+rs];
    a[4] = input[0+rs] - input[4+rs];
    a[5] = input[1+rs] - input[5+rs];
    a[6] = input[2+rs] - input[6+rs];
    a[7] = input[3+rs] - input[7+rs];

    b[0] = a[0]+a[2];
    b[1] = a[1]+a[3];
    b[2] = a[0]-a[2];
    b[3] = a[1]-a[3];
    b[4] = a[4]+a[6];
    b[5] = a[5]+a[7];
    b[6] = a[4]-a[6];
    b[7] = a[5]-a[7];

    tmp[0+8*row] = b[0]+b[1];
    tmp[1+8*row] = b[0]-b[1];
    tmp[2+8*row] = b[2]+b[3];
    tmp[3+8*row] = b[2]-b[3];
    tmp[4+8*row] = b[4]+b[5];
    tmp[5+8*row] = b[4]-b[5];
    tmp[6+8*row] = b[6]+b[7];
    tmp[7+8*row] = b[6]-b[7];
  }

  //printMatrix("tmp",tmp,8);

  // column transforms

  for (int col=0;col<8;col++) {
    a[0] = tmp[col+0*8] + tmp[col+4*8];
    a[1] = tmp[col+1*8] + tmp[col+5*8];
    a[2] = tmp[col+2*8] + tmp[col+6*8];
    a[3] = tmp[col+3*8] + tmp[col+7*8];
    a[4] = tmp[col+0*8] - tmp[col+4*8];
    a[5] = tmp[col+1*8] - tmp[col+5*8];
    a[6] = tmp[col+2*8] - tmp[col+6*8];
    a[7] = tmp[col+3*8] - tmp[col+7*8];

    b[0] = a[0]+a[2];
    b[1] = a[1]+a[3];
    b[2] = a[0]-a[2];
    b[3] = a[1]-a[3];
    b[4] = a[4]+a[6];
    b[5] = a[5]+a[7];
    b[6] = a[4]-a[6];
    b[7] = a[5]-a[7];

    coeffs[col+0*8] = b[0]+b[1];
    coeffs[col+1*8] = b[0]-b[1];
    coeffs[col+2*8] = b[2]+b[3];
    coeffs[col+3*8] = b[2]-b[3];
    coeffs[col+4*8] = b[4]+b[5];
    coeffs[col+5*8] = b[4]-b[5];
    coeffs[col+6*8] = b[6]+b[7];
    coeffs[col+7*8] = b[6]-b[7];
  }

  //printMatrix("coeffs",coeffs,8);
}


void hadamard_16x16_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  hadamard_transform_8(coeffs,16, input,stride);
}

void hadamard_32x32_8_fallback(int16_t *coeffs, const int16_t *input, ptrdiff_t stride)
{
  hadamard_transform_8(coeffs,32, input,stride);
}
