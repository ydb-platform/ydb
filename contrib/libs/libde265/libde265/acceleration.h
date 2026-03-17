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

#ifndef DE265_ACCELERATION_H
#define DE265_ACCELERATION_H

#include <stddef.h>
#include <stdint.h>
#include <assert.h>


struct acceleration_functions
{
  void (*put_weighted_pred_avg_8)(uint8_t *_dst, ptrdiff_t dststride,
                                  const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                  int width, int height);

  void (*put_unweighted_pred_8)(uint8_t *_dst, ptrdiff_t dststride,
                                const int16_t *src, ptrdiff_t srcstride,
                                int width, int height);

  void (*put_weighted_pred_8)(uint8_t *_dst, ptrdiff_t dststride,
                              const int16_t *src, ptrdiff_t srcstride,
                              int width, int height,
                              int w,int o,int log2WD);
  void (*put_weighted_bipred_8)(uint8_t *_dst, ptrdiff_t dststride,
                                const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                int width, int height,
                                int w1,int o1, int w2,int o2, int log2WD);


  void (*put_weighted_pred_avg_16)(uint16_t *_dst, ptrdiff_t dststride,
                                  const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                   int width, int height, int bit_depth);

  void (*put_unweighted_pred_16)(uint16_t *_dst, ptrdiff_t dststride,
                                const int16_t *src, ptrdiff_t srcstride,
                                int width, int height, int bit_depth);

  void (*put_weighted_pred_16)(uint16_t *_dst, ptrdiff_t dststride,
                              const int16_t *src, ptrdiff_t srcstride,
                              int width, int height,
                              int w,int o,int log2WD, int bit_depth);
  void (*put_weighted_bipred_16)(uint16_t *_dst, ptrdiff_t dststride,
                                const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                int width, int height,
                                int w1,int o1, int w2,int o2, int log2WD, int bit_depth);


  void put_weighted_pred_avg(void *_dst, ptrdiff_t dststride,
                             const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                             int width, int height, int bit_depth) const;

  void put_unweighted_pred(void *_dst, ptrdiff_t dststride,
                           const int16_t *src, ptrdiff_t srcstride,
                           int width, int height, int bit_depth) const;

  void put_weighted_pred(void *_dst, ptrdiff_t dststride,
                         const int16_t *src, ptrdiff_t srcstride,
                         int width, int height,
                         int w,int o,int log2WD, int bit_depth) const;
  void put_weighted_bipred(void *_dst, ptrdiff_t dststride,
                           const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                           int width, int height,
                           int w1,int o1, int w2,int o2, int log2WD, int bit_depth) const;




  void (*put_hevc_epel_8)(int16_t *dst, ptrdiff_t dststride,
                          const uint8_t *src, ptrdiff_t srcstride, int width, int height,
                          int mx, int my, int16_t* mcbuffer);
  void (*put_hevc_epel_h_8)(int16_t *dst, ptrdiff_t dststride,
                            const uint8_t *src, ptrdiff_t srcstride, int width, int height,
                            int mx, int my, int16_t* mcbuffer, int bit_depth);
  void (*put_hevc_epel_v_8)(int16_t *dst, ptrdiff_t dststride,
                            const uint8_t *src, ptrdiff_t srcstride, int width, int height,
                            int mx, int my, int16_t* mcbuffer, int bit_depth);
  void (*put_hevc_epel_hv_8)(int16_t *dst, ptrdiff_t dststride,
                             const uint8_t *src, ptrdiff_t srcstride, int width, int height,
                             int mx, int my, int16_t* mcbuffer, int bit_depth);

  void (*put_hevc_qpel_8[4][4])(int16_t *dst, ptrdiff_t dststride,
                                const uint8_t *src, ptrdiff_t srcstride, int width, int height,
                                int16_t* mcbuffer);


  void (*put_hevc_epel_16)(int16_t *dst, ptrdiff_t dststride,
                           const uint16_t *src, ptrdiff_t srcstride, int width, int height,
                           int mx, int my, int16_t* mcbuffer, int bit_depth);
  void (*put_hevc_epel_h_16)(int16_t *dst, ptrdiff_t dststride,
                             const uint16_t *src, ptrdiff_t srcstride, int width, int height,
                            int mx, int my, int16_t* mcbuffer, int bit_depth);
  void (*put_hevc_epel_v_16)(int16_t *dst, ptrdiff_t dststride,
                             const uint16_t *src, ptrdiff_t srcstride, int width, int height,
                             int mx, int my, int16_t* mcbuffer, int bit_depth);
  void (*put_hevc_epel_hv_16)(int16_t *dst, ptrdiff_t dststride,
                              const uint16_t *src, ptrdiff_t srcstride, int width, int height,
                              int mx, int my, int16_t* mcbuffer, int bit_depth);

  void (*put_hevc_qpel_16[4][4])(int16_t *dst, ptrdiff_t dststride,
                                 const uint16_t *src, ptrdiff_t srcstride, int width, int height,
                                 int16_t* mcbuffer, int bit_depth);


  void put_hevc_epel(int16_t *dst, ptrdiff_t dststride,
                     const void *src, ptrdiff_t srcstride, int width, int height,
                     int mx, int my, int16_t* mcbuffer, int bit_depth) const;
  void put_hevc_epel_h(int16_t *dst, ptrdiff_t dststride,
                       const void *src, ptrdiff_t srcstride, int width, int height,
                       int mx, int my, int16_t* mcbuffer, int bit_depth) const;
  void put_hevc_epel_v(int16_t *dst, ptrdiff_t dststride,
                       const void *src, ptrdiff_t srcstride, int width, int height,
                       int mx, int my, int16_t* mcbuffer, int bit_depth) const;
  void put_hevc_epel_hv(int16_t *dst, ptrdiff_t dststride,
                        const void *src, ptrdiff_t srcstride, int width, int height,
                        int mx, int my, int16_t* mcbuffer, int bit_depth) const;

  void put_hevc_qpel(int16_t *dst, ptrdiff_t dststride,
                     const void *src, ptrdiff_t srcstride, int width, int height,
                     int16_t* mcbuffer, int dX,int dY, int bit_depth) const;


  // --- inverse transforms ---

  void (*transform_bypass)(int32_t *residual, const int16_t *coeffs, int nT);
  void (*transform_bypass_rdpcm_v)(int32_t *r, const int16_t *coeffs, int nT);
  void (*transform_bypass_rdpcm_h)(int32_t *r, const int16_t *coeffs, int nT);

  // 8 bit

  void (*transform_skip_8)(uint8_t *_dst, const int16_t *coeffs, ptrdiff_t _stride); // no transform
  void (*transform_skip_rdpcm_v_8)(uint8_t *_dst, const int16_t *coeffs, int nT, ptrdiff_t _stride);
  void (*transform_skip_rdpcm_h_8)(uint8_t *_dst, const int16_t *coeffs, int nT, ptrdiff_t _stride);
  void (*transform_4x4_dst_add_8)(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride); // iDST
  void (*transform_add_8[4])(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride); // iDCT

  // 9-16 bit

  void (*transform_skip_16)(uint16_t *_dst, const int16_t *coeffs, ptrdiff_t _stride, int bit_depth); // no transform
  void (*transform_4x4_dst_add_16)(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth); // iDST
  void (*transform_add_16[4])(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth); // iDCT


  void (*rotate_coefficients)(int16_t *coeff, int nT);

  void (*transform_idst_4x4)(int32_t *dst, const int16_t *coeffs, int bdShift, int max_coeff_bits);
  void (*transform_idct_4x4)(int32_t *dst, const int16_t *coeffs, int bdShift, int max_coeff_bits);
  void (*transform_idct_8x8)(int32_t *dst, const int16_t *coeffs, int bdShift, int max_coeff_bits);
  void (*transform_idct_16x16)(int32_t *dst,const int16_t *coeffs,int bdShift, int max_coeff_bits);
  void (*transform_idct_32x32)(int32_t *dst,const int16_t *coeffs,int bdShift, int max_coeff_bits);
  void (*add_residual_8)(uint8_t *dst, ptrdiff_t stride, const int32_t* r, int nT, int bit_depth);
  void (*add_residual_16)(uint16_t *dst,ptrdiff_t stride,const int32_t* r, int nT, int bit_depth);

  template <class pixel_t>
  void add_residual(pixel_t *dst, ptrdiff_t stride, const int32_t* r, int nT, int bit_depth) const;

  void (*rdpcm_v)(int32_t* residual, const int16_t* coeffs, int nT,int tsShift,int bdShift);
  void (*rdpcm_h)(int32_t* residual, const int16_t* coeffs, int nT,int tsShift,int bdShift);

  void (*transform_skip_residual)(int32_t *residual, const int16_t *coeffs, int nT,
                                  int tsShift,int bdShift);


  template <class pixel_t> void transform_skip(pixel_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth) const;
  template <class pixel_t> void transform_skip_rdpcm_v(pixel_t *dst, const int16_t *coeffs, int nT, ptrdiff_t stride, int bit_depth) const;
  template <class pixel_t> void transform_skip_rdpcm_h(pixel_t *dst, const int16_t *coeffs, int nT, ptrdiff_t stride, int bit_depth) const;
  template <class pixel_t> void transform_4x4_dst_add(pixel_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth) const;
  template <class pixel_t> void transform_add(int sizeIdx, pixel_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth) const;



  // --- forward transforms ---

  void (*fwd_transform_4x4_dst_8)(int16_t *coeffs, const int16_t* src, ptrdiff_t stride); // fDST

  // indexed with (log2TbSize-2)
  void (*fwd_transform_8[4])     (int16_t *coeffs, const int16_t *src, ptrdiff_t stride); // fDCT


  // forward Hadamard transform (without scaling factor)
  // (4x4,8x8,16x16,32x32) indexed with (log2TbSize-2)
  void (*hadamard_transform_8[4])     (int16_t *coeffs, const int16_t *src, ptrdiff_t stride);
};


/*
template <> inline void acceleration_functions::put_weighted_pred_avg<uint8_t>(uint8_t *_dst, ptrdiff_t dststride,
                                                                               const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                                                               int width, int height, int bit_depth) { put_weighted_pred_avg_8(_dst,dststride,src1,src2,srcstride,width,height); }
template <> inline void acceleration_functions::put_weighted_pred_avg<uint16_t>(uint16_t *_dst, ptrdiff_t dststride,
                                                                                const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                                                                int width, int height, int bit_depth) { put_weighted_pred_avg_16(_dst,dststride,src1,src2,
                                                                                                                                                 srcstride,width,height,bit_depth); }

template <> inline void acceleration_functions::put_unweighted_pred<uint8_t>(uint8_t *_dst, ptrdiff_t dststride,
                                                                             const int16_t *src, ptrdiff_t srcstride,
                                                                             int width, int height, int bit_depth) { put_unweighted_pred_8(_dst,dststride,src,srcstride,width,height); }
template <> inline void acceleration_functions::put_unweighted_pred<uint16_t>(uint16_t *_dst, ptrdiff_t dststride,
                                                                              const int16_t *src, ptrdiff_t srcstride,
                                                                              int width, int height, int bit_depth) { put_unweighted_pred_16(_dst,dststride,src,srcstride,width,height,bit_depth); }

template <> inline void acceleration_functions::put_weighted_pred<uint8_t>(uint8_t *_dst, ptrdiff_t dststride,
                                                                           const int16_t *src, ptrdiff_t srcstride,
                                                                           int width, int height,
                                                                           int w,int o,int log2WD, int bit_depth) { put_weighted_pred_8(_dst,dststride,src,srcstride,width,height,w,o,log2WD); }
template <> inline void acceleration_functions::put_weighted_pred<uint16_t>(uint16_t *_dst, ptrdiff_t dststride,
                                                                            const int16_t *src, ptrdiff_t srcstride,
                                                                            int width, int height,
                                                                            int w,int o,int log2WD, int bit_depth) { put_weighted_pred_16(_dst,dststride,src,srcstride,width,height,w,o,log2WD,bit_depth); }

template <> inline void acceleration_functions::put_weighted_bipred<uint8_t>(uint8_t *_dst, ptrdiff_t dststride,
                                                                             const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                                                             int width, int height,
                                                                             int w1,int o1, int w2,int o2, int log2WD, int bit_depth) { put_weighted_bipred_8(_dst,dststride,src1,src2,srcstride,
                                                                                                                                                              width,height,
                                                                                                                                                              w1,o1,w2,o2,log2WD); }
template <> inline void acceleration_functions::put_weighted_bipred<uint16_t>(uint16_t *_dst, ptrdiff_t dststride,
                                                                              const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                                                              int width, int height,
                                                                              int w1,int o1, int w2,int o2, int log2WD, int bit_depth) { put_weighted_bipred_16(_dst,dststride,src1,src2,srcstride,
                                                                                                                                                                width,height,
                                                                                                                                                                w1,o1,w2,o2,log2WD,bit_depth); }
*/


inline void acceleration_functions::put_weighted_pred_avg(void* _dst, ptrdiff_t dststride,
                                                          const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                                          int width, int height, int bit_depth) const
{
  if (bit_depth <= 8)
    put_weighted_pred_avg_8((uint8_t*)_dst,dststride,src1,src2,srcstride,width,height);
  else
    put_weighted_pred_avg_16((uint16_t*)_dst,dststride,src1,src2,srcstride,width,height,bit_depth);
}


inline void acceleration_functions::put_unweighted_pred(void* _dst, ptrdiff_t dststride,
                                                        const int16_t *src, ptrdiff_t srcstride,
                                                        int width, int height, int bit_depth) const
{
  if (bit_depth <= 8)
    put_unweighted_pred_8((uint8_t*)_dst,dststride,src,srcstride,width,height);
  else
    put_unweighted_pred_16((uint16_t*)_dst,dststride,src,srcstride,width,height,bit_depth);
}


inline void acceleration_functions::put_weighted_pred(void* _dst, ptrdiff_t dststride,
                                                      const int16_t *src, ptrdiff_t srcstride,
                                                      int width, int height,
                                                      int w,int o,int log2WD, int bit_depth) const
{
  if (bit_depth <= 8)
    put_weighted_pred_8((uint8_t*)_dst,dststride,src,srcstride,width,height,w,o,log2WD);
  else
    put_weighted_pred_16((uint16_t*)_dst,dststride,src,srcstride,width,height,w,o,log2WD,bit_depth);
}


inline void acceleration_functions::put_weighted_bipred(void* _dst, ptrdiff_t dststride,
                                                        const int16_t *src1, const int16_t *src2, ptrdiff_t srcstride,
                                                        int width, int height,
                                                        int w1,int o1, int w2,int o2, int log2WD, int bit_depth) const
{
  if (bit_depth <= 8)
    put_weighted_bipred_8((uint8_t*)_dst,dststride,src1,src2,srcstride, width,height, w1,o1,w2,o2,log2WD);
  else
    put_weighted_bipred_16((uint16_t*)_dst,dststride,src1,src2,srcstride, width,height, w1,o1,w2,o2,log2WD,bit_depth);
}



inline void acceleration_functions::put_hevc_epel(int16_t *dst, ptrdiff_t dststride,
                                                  const void *src, ptrdiff_t srcstride, int width, int height,
                                                  int mx, int my, int16_t* mcbuffer, int bit_depth) const
{
  if (bit_depth <= 8)
    put_hevc_epel_8(dst,dststride,(const uint8_t*)src,srcstride,width,height,mx,my,mcbuffer);
  else
    put_hevc_epel_16(dst,dststride,(const uint16_t*)src,srcstride,width,height,mx,my,mcbuffer, bit_depth);
}

inline void acceleration_functions::put_hevc_epel_h(int16_t *dst, ptrdiff_t dststride,
                                                    const void *src, ptrdiff_t srcstride, int width, int height,
                                                    int mx, int my, int16_t* mcbuffer, int bit_depth) const
{
  if (bit_depth <= 8)
    put_hevc_epel_h_8(dst,dststride,(const uint8_t*)src,srcstride,width,height,mx,my,mcbuffer,bit_depth);
  else
    put_hevc_epel_h_16(dst,dststride,(const uint16_t*)src,srcstride,width,height,mx,my,mcbuffer,bit_depth);
}

inline void acceleration_functions::put_hevc_epel_v(int16_t *dst, ptrdiff_t dststride,
                                                    const void *src, ptrdiff_t srcstride, int width, int height,
                                                    int mx, int my, int16_t* mcbuffer, int bit_depth) const
{
  if (bit_depth <= 8)
    put_hevc_epel_v_8(dst,dststride,(const uint8_t*)src,srcstride,width,height,mx,my,mcbuffer,bit_depth);
  else
    put_hevc_epel_v_16(dst,dststride,(const uint16_t*)src,srcstride,width,height,mx,my,mcbuffer, bit_depth);
}

inline void acceleration_functions::put_hevc_epel_hv(int16_t *dst, ptrdiff_t dststride,
                                                     const void *src, ptrdiff_t srcstride, int width, int height,
                                                     int mx, int my, int16_t* mcbuffer, int bit_depth) const
{
  if (bit_depth <= 8)
    put_hevc_epel_hv_8(dst,dststride,(const uint8_t*)src,srcstride,width,height,mx,my,mcbuffer,bit_depth);
  else
    put_hevc_epel_hv_16(dst,dststride,(const uint16_t*)src,srcstride,width,height,mx,my,mcbuffer, bit_depth);
}

inline void acceleration_functions::put_hevc_qpel(int16_t *dst, ptrdiff_t dststride,
                                                  const void *src, ptrdiff_t srcstride, int width, int height,
                                                  int16_t* mcbuffer, int dX,int dY, int bit_depth) const
{
  if (bit_depth <= 8)
    put_hevc_qpel_8[dX][dY](dst,dststride,(const uint8_t*)src,srcstride,width,height,mcbuffer);
  else
    put_hevc_qpel_16[dX][dY](dst,dststride,(const uint16_t*)src,srcstride,width,height,mcbuffer, bit_depth);
}

template <> inline void acceleration_functions::transform_skip<uint8_t>(uint8_t *dst, const int16_t *coeffs,ptrdiff_t stride, int bit_depth) const { transform_skip_8(dst,coeffs,stride); }
template <> inline void acceleration_functions::transform_skip<uint16_t>(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth) const { transform_skip_16(dst,coeffs,stride, bit_depth); }

template <> inline void acceleration_functions::transform_skip_rdpcm_v<uint8_t>(uint8_t *dst, const int16_t *coeffs, int nT, ptrdiff_t stride, int bit_depth) const { assert(bit_depth==8); transform_skip_rdpcm_v_8(dst,coeffs,nT,stride); }
template <> inline void acceleration_functions::transform_skip_rdpcm_h<uint8_t>(uint8_t *dst, const int16_t *coeffs, int nT, ptrdiff_t stride, int bit_depth) const { assert(bit_depth==8); transform_skip_rdpcm_h_8(dst,coeffs,nT,stride); }
template <> inline void acceleration_functions::transform_skip_rdpcm_v<uint16_t>(uint16_t *dst, const int16_t *coeffs, int nT, ptrdiff_t stride, int bit_depth) const { assert(false); /*transform_skip_rdpcm_v_8(dst,coeffs,nT,stride);*/ }
template <> inline void acceleration_functions::transform_skip_rdpcm_h<uint16_t>(uint16_t *dst, const int16_t *coeffs, int nT, ptrdiff_t stride, int bit_depth) const { assert(false); /*transform_skip_rdpcm_h_8(dst,coeffs,nT,stride);*/ }


template <> inline void acceleration_functions::transform_4x4_dst_add<uint8_t>(uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride,int bit_depth) const { transform_4x4_dst_add_8(dst,coeffs,stride); }
template <> inline void acceleration_functions::transform_4x4_dst_add<uint16_t>(uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride,int bit_depth) const { transform_4x4_dst_add_16(dst,coeffs,stride,bit_depth); }

template <> inline void acceleration_functions::transform_add<uint8_t>(int sizeIdx, uint8_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth) const { transform_add_8[sizeIdx](dst,coeffs,stride); }
template <> inline void acceleration_functions::transform_add<uint16_t>(int sizeIdx, uint16_t *dst, const int16_t *coeffs, ptrdiff_t stride, int bit_depth) const { transform_add_16[sizeIdx](dst,coeffs,stride,bit_depth); }

template <> inline void acceleration_functions::add_residual(uint8_t *dst,  ptrdiff_t stride, const int32_t* r, int nT, int bit_depth) const { add_residual_8(dst,stride,r,nT,bit_depth); }
template <> inline void acceleration_functions::add_residual(uint16_t *dst, ptrdiff_t stride, const int32_t* r, int nT, int bit_depth) const { add_residual_16(dst,stride,r,nT,bit_depth); }

#endif
