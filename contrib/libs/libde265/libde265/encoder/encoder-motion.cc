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

#include "encoder/encoder-motion.h"
#include "encoder/encoder-context.h"
#include "decctx.h"
#include "util.h"
#include "dpb.h"
#include "motion.h"

#include <assert.h>


#include <sys/types.h>
#include <signal.h>
#include <string.h>

#if defined(_MSC_VER) || defined(__MINGW32__)
# include <malloc.h>
#elif defined(HAVE_ALLOCA_H)
# include <alloca.h>
#endif


class MotionVectorAccess_encoder_context : public MotionVectorAccess
{
public:
  MotionVectorAccess_encoder_context(const encoder_context* e) : ectx(e) { }

  enum PartMode get_PartMode(int x,int y) const override { return ectx->ctbs.getCB(x,y)->PartMode; }
  const PBMotion& get_mv_info(int x,int y) const override { return ectx->ctbs.getPB(x,y)->motion; }

private:
  const encoder_context* ectx;
};



void get_merge_candidate_list_from_tree(encoder_context* ectx,
                                        const slice_segment_header* shdr,
                                        int xC,int yC, int xP,int yP,
                                        int nCS, int nPbW,int nPbH, int partIdx,
                                        PBMotion* mergeCandList)
{
  int max_merge_idx = 5-shdr->five_minus_max_num_merge_cand -1;

  get_merge_candidate_list_without_step_9(ectx, shdr,
                                          MotionVectorAccess_encoder_context(ectx), ectx->img,
                                          xC,yC,xP,yP,nCS,nPbW,nPbH, partIdx,
                                          max_merge_idx, mergeCandList);

  // 9. for encoder: modify all merge candidates

  for (int i=0;i<=max_merge_idx;i++) {
    if (mergeCandList[i].predFlag[0] &&
        mergeCandList[i].predFlag[1] &&
        nPbW+nPbH==12)
      {
        mergeCandList[i].refIdx[1]   = -1;
        mergeCandList[i].predFlag[1] = 0;
      }
  }
}
