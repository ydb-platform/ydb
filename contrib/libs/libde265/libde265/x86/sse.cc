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

#ifdef _MSC_VER
#include <intrin.h>
#endif

#include "x86/sse.h"
#include "x86/sse-motion.h"
#include "x86/sse-dct.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef __GNUC__
#include <cpuid.h>
#endif

void init_acceleration_functions_sse(struct acceleration_functions* accel)
{
  uint32_t ecx=0,edx=0;

#ifdef _MSC_VER
  uint32_t regs[4];
  int a = 1;

  __cpuid((int *)regs, (int)a);

  ecx = regs[2];
  edx = regs[3];
#else
  uint32_t eax,ebx;
  __get_cpuid(1, &eax,&ebx,&ecx,&edx);
#endif
  
  // printf("CPUID EAX=1 -> ECX=%x EDX=%x\n", regs[2], regs[3]);

  //int have_MMX    = !!(edx & (1<<23));
  int have_SSE    = !!(edx & (1<<25));
  int have_SSE4_1 = !!(ecx & (1<<19));

  // printf("MMX:%d SSE:%d SSE4_1:%d\n",have_MMX,have_SSE,have_SSE4_1);

  if (have_SSE) {
  }

#if HAVE_SSE4_1
  if (have_SSE4_1) {
    accel->put_unweighted_pred_8   = ff_hevc_put_unweighted_pred_8_sse;
    accel->put_weighted_pred_avg_8 = ff_hevc_put_weighted_pred_avg_8_sse;

    accel->put_hevc_epel_8    = ff_hevc_put_hevc_epel_pixels_8_sse;
    accel->put_hevc_epel_h_8  = ff_hevc_put_hevc_epel_h_8_sse;
    accel->put_hevc_epel_v_8  = ff_hevc_put_hevc_epel_v_8_sse;
    accel->put_hevc_epel_hv_8 = ff_hevc_put_hevc_epel_hv_8_sse;

    accel->put_hevc_qpel_8[0][0] = ff_hevc_put_hevc_qpel_pixels_8_sse;
    accel->put_hevc_qpel_8[0][1] = ff_hevc_put_hevc_qpel_v_1_8_sse;
    accel->put_hevc_qpel_8[0][2] = ff_hevc_put_hevc_qpel_v_2_8_sse;
    accel->put_hevc_qpel_8[0][3] = ff_hevc_put_hevc_qpel_v_3_8_sse;
    accel->put_hevc_qpel_8[1][0] = ff_hevc_put_hevc_qpel_h_1_8_sse;
    accel->put_hevc_qpel_8[1][1] = ff_hevc_put_hevc_qpel_h_1_v_1_sse;
    accel->put_hevc_qpel_8[1][2] = ff_hevc_put_hevc_qpel_h_1_v_2_sse;
    accel->put_hevc_qpel_8[1][3] = ff_hevc_put_hevc_qpel_h_1_v_3_sse;
    accel->put_hevc_qpel_8[2][0] = ff_hevc_put_hevc_qpel_h_2_8_sse;
    accel->put_hevc_qpel_8[2][1] = ff_hevc_put_hevc_qpel_h_2_v_1_sse;
    accel->put_hevc_qpel_8[2][2] = ff_hevc_put_hevc_qpel_h_2_v_2_sse;
    accel->put_hevc_qpel_8[2][3] = ff_hevc_put_hevc_qpel_h_2_v_3_sse;
    accel->put_hevc_qpel_8[3][0] = ff_hevc_put_hevc_qpel_h_3_8_sse;
    accel->put_hevc_qpel_8[3][1] = ff_hevc_put_hevc_qpel_h_3_v_1_sse;
    accel->put_hevc_qpel_8[3][2] = ff_hevc_put_hevc_qpel_h_3_v_2_sse;
    accel->put_hevc_qpel_8[3][3] = ff_hevc_put_hevc_qpel_h_3_v_3_sse;

    accel->transform_skip_8 = ff_hevc_transform_skip_8_sse;

    // actually, for these two functions, the scalar fallback seems to be faster than the SSE code
    //accel->transform_4x4_luma_add_8 = ff_hevc_transform_4x4_luma_add_8_sse4; // SSE-4 only TODO
    //accel->transform_4x4_add_8   = ff_hevc_transform_4x4_add_8_sse4;

    accel->transform_add_8[1] = ff_hevc_transform_8x8_add_8_sse4;
    accel->transform_add_8[2] = ff_hevc_transform_16x16_add_8_sse4;
    accel->transform_add_8[3] = ff_hevc_transform_32x32_add_8_sse4;
  }
#endif
}

