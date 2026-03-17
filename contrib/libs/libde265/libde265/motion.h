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

#ifndef DE265_MOTION_H
#define DE265_MOTION_H

#include <stdint.h>
#include "slice.h"

class base_context;
class slice_segment_header;

class MotionVector
{
 public:
  int16_t x,y;
};


class PBMotion
{
 public:
  uint8_t predFlag[2];  // which of the two vectors is actually used
  int8_t  refIdx[2];    // index into RefPicList
  MotionVector  mv[2];  // the absolute motion vectors

  bool operator==(const PBMotion&) const;
};


class PBMotionCoding
{
 public:
  // index into RefPicList
  int8_t  refIdx[2];

  // motion vector difference
  int16_t mvd[2][2]; // [L0/L1][x/y]  (only in top left position - ???)

  // enum InterPredIdc, whether this is prediction from L0,L1, or BI
  uint8_t inter_pred_idc : 2;

  // which of the two MVPs is used
  uint8_t mvp_l0_flag : 1;
  uint8_t mvp_l1_flag : 1;

  // whether merge mode is used
  uint8_t merge_flag : 1;
  uint8_t merge_idx  : 3;
};


void get_merge_candidate_list(base_context* ctx,
                              const slice_segment_header* shdr,
                              struct de265_image* img,
                              int xC,int yC, int xP,int yP,
                              int nCS, int nPbW,int nPbH, int partIdx,
                              PBMotion* mergeCandList);

/*
int derive_spatial_merging_candidates(const struct de265_image* img,
                                      int xC, int yC, int nCS, int xP, int yP,
                                      uint8_t singleMCLFlag,
                                      int nPbW, int nPbH,
                                      int partIdx,
                                      MotionVectorSpec* out_cand,
                                      int maxCandidates);
*/

void generate_inter_prediction_samples(base_context* ctx,
                                       const slice_segment_header* shdr,
                                       struct de265_image* img,
                                       int xC,int yC,
                                       int xB,int yB,
                                       int nCS, int nPbW,int nPbH,
                                       const PBMotion* vi);


/* Fill list (two entries) of motion-vector predictors for MVD coding.
 */
void fill_luma_motion_vector_predictors(base_context* ctx,
                                        const slice_segment_header* shdr,
                                        de265_image* img,
                                        int xC,int yC,int nCS,int xP,int yP,
                                        int nPbW,int nPbH, int l,
                                        int refIdx, int partIdx,
                                        MotionVector out_mvpList[2]);


void decode_prediction_unit(base_context* ctx,const slice_segment_header* shdr,
                            de265_image* img, const PBMotionCoding& motion,
                            int xC,int yC, int xB,int yB, int nCS, int nPbW,int nPbH, int partIdx);




class MotionVectorAccess
{
public:
  virtual enum PartMode get_PartMode(int x,int y) const = 0;
  virtual const PBMotion& get_mv_info(int x,int y) const = 0;
};


void get_merge_candidate_list_without_step_9(base_context* ctx,
                                             const slice_segment_header* shdr,
                                             const MotionVectorAccess& mvaccess,
                                             de265_image* img,
                                             int xC,int yC, int xP,int yP,
                                             int nCS, int nPbW,int nPbH, int partIdx,
                                             int max_merge_idx,
                                             PBMotion* mergeCandList);

#endif
