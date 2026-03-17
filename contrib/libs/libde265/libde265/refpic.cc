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

#include "refpic.h"
#include "decctx.h"
#include "util.h"

#include <assert.h>
#include <stdlib.h>
#if defined(_MSC_VER) || defined(__MINGW32__)
# include <malloc.h>
#elif defined(HAVE_ALLOCA_H)
# include <alloca.h>
#endif


void ref_pic_set::reset()
{
  NumNegativePics = 0;
  NumPositivePics = 0;
  NumDeltaPocs = 0;
  NumPocTotalCurr_shortterm_only = 0;

  for (int i=0;i<MAX_NUM_REF_PICS;i++) {
    DeltaPocS0[i] = 0;
    DeltaPocS1[i] = 0;

    UsedByCurrPicS0[i] = 0;
    UsedByCurrPicS1[i] = 0;
  }
}


void ref_pic_set::compute_derived_values()
{
  NumPocTotalCurr_shortterm_only = 0;

  for (int i=0; i<NumNegativePics; i++)
    if (UsedByCurrPicS0[i])
      NumPocTotalCurr_shortterm_only++;

  for (int i=0; i<NumPositivePics; i++)
    if (UsedByCurrPicS1[i])
      NumPocTotalCurr_shortterm_only++;

  NumDeltaPocs = NumNegativePics + NumPositivePics;


  /*
    NOTE: this is done when reading the slice header.
    The value numPocTotalCurr is then stored in the slice header.

  for (int i = 0; i < num_long_term_sps + num_long_term_pics; i++ )
            if( UsedByCurrPicLt[i] )
              NumPocTotalCurr++
                }
  */
}


/* A ref-pic-set is coded either coded
   - as a list of the relative POC deltas themselves, or
   - by shifting an existing ref-pic-set by some number of frames
   When shifting an existing set, the frame 0 is also shifted as an additional reference frame.
   When coding the ref-pic-sets in the SPS, prediction is always from the previous set.
   In the slice header, the ref-pic-set can use any previous set as reference.
 */
bool read_short_term_ref_pic_set(error_queue* errqueue,
                                 const seq_parameter_set* sps,
                                 bitreader* br,
                                 ref_pic_set* out_set, // where to store the read set
                                 int idxRps,  // index of the set to be read
                                 const std::vector<ref_pic_set>& sets, // previously read sets
                                 bool sliceRefPicSet) // is this in the slice header?
{
  // --- is this set coded in prediction mode (not possible for the first set)

  char inter_ref_pic_set_prediction_flag;

  if (idxRps != 0) {
    inter_ref_pic_set_prediction_flag = get_bits(br,1);
  }
  else {
    inter_ref_pic_set_prediction_flag = 0;
  }



  if (inter_ref_pic_set_prediction_flag) {
    int vlc;

    /* Only for the last ref_pic_set (that's the one coded in the slice header),
       we can specify relative to which reference set we code the set. */

    int delta_idx;
    if (sliceRefPicSet) { // idxRps == num_short_term_ref_pic_sets) {
      delta_idx = vlc = get_uvlc(br);
      if (delta_idx==UVLC_ERROR) {
        return false;
      }

      if (delta_idx>=idxRps) {
        return false;
      }

      delta_idx++;
    } else {
      delta_idx = 1;
    }

    int RIdx = idxRps - delta_idx; // this is our source set, which we will modify
    assert(RIdx>=0);

    int delta_rps_sign = get_bits(br,1);
    int abs_delta_rps  = vlc = get_uvlc(br);
    if (vlc==UVLC_ERROR) { return false; }
    abs_delta_rps++;
    int DeltaRPS = (delta_rps_sign ? -abs_delta_rps : abs_delta_rps);

    // bits are stored in this order:
    // - all bits for negative Pocs (forward),
    // - then all bits for positive Pocs (forward),
    // - then bits for '0', shifting of the current picture
    // in total, these are 'nDeltaPocsRIdx'+1 bits

    logtrace(LogHeaders,"predicted from %d with delta %d\n",RIdx,DeltaRPS);

    int nDeltaPocsRIdx= sets[RIdx].NumDeltaPocs; // size of source set
    char *const used_by_curr_pic_flag = (char *)alloca((nDeltaPocsRIdx+1) * sizeof(char));
    char *const use_delta_flag = (char *)alloca((nDeltaPocsRIdx+1) * sizeof(char));

    for (int j=0;j<=nDeltaPocsRIdx;j++) {
      used_by_curr_pic_flag[j] = get_bits(br,1);
      if (used_by_curr_pic_flag[j]) {
        use_delta_flag[j] = 1;  // if this frame is used, we also have to apply the delta
      } else {
        use_delta_flag[j] = get_bits(br,1);  // otherwise, it is only optionally included
      }
    }

    logtrace(LogHeaders,"flags: ");
    for (int j=0;j<=nDeltaPocsRIdx;j++) {
      logtrace(LogHeaders,"%d ", use_delta_flag[j]);
    }
    logtrace(LogHeaders,"\n");

    int nNegativeRIdx = sets[RIdx].NumNegativePics;
    int nPositiveRIdx = sets[RIdx].NumPositivePics;

    // --- update list 0 (negative Poc) ---
    // Iterate through all Pocs in decreasing value order (positive reverse, 0, negative forward).

    int i=0; // target index

    // positive list
    for (int j=nPositiveRIdx-1;j>=0;j--) {
      assert(RIdx >= 0 && RIdx < sets.size());
      assert(j>=0 && j < MAX_NUM_REF_PICS);

      int dPoc = sets[RIdx].DeltaPocS1[j] + DeltaRPS; // new delta
      if (dPoc<0 && use_delta_flag[nNegativeRIdx+j]) {
        if (i>= MAX_NUM_REF_PICS) { return false; }

        out_set->DeltaPocS0[i] = dPoc;
        out_set->UsedByCurrPicS0[i] = used_by_curr_pic_flag[nNegativeRIdx+j];
        i++;
      }
    }

    // frame 0
    if (DeltaRPS<0 && use_delta_flag[nDeltaPocsRIdx]) {
      if (i>= MAX_NUM_REF_PICS) { return false; }

      out_set->DeltaPocS0[i] = DeltaRPS;
      out_set->UsedByCurrPicS0[i] = used_by_curr_pic_flag[nDeltaPocsRIdx];
      i++;
    }

    // negative list
    for (int j=0;j<nNegativeRIdx;j++) {
      int dPoc = sets[RIdx].DeltaPocS0[j] + DeltaRPS;
      if (dPoc<0 && use_delta_flag[j]) {
        if (i>= MAX_NUM_REF_PICS) { return false; }

        out_set->DeltaPocS0[i] = dPoc;
        out_set->UsedByCurrPicS0[i] = used_by_curr_pic_flag[j];
        i++;
      }
    }

    out_set->NumNegativePics = i;


    // --- update list 1 (positive Poc) ---
    // Iterate through all Pocs in increasing value order (negative reverse, 0, positive forward)

    i=0; // target index

    // negative list
    for (int j=nNegativeRIdx-1;j>=0;j--) {
      int dPoc = sets[RIdx].DeltaPocS0[j] + DeltaRPS;
      if (dPoc>0 && use_delta_flag[j]) {
        if (i>= MAX_NUM_REF_PICS) { return false; }

        out_set->DeltaPocS1[i] = dPoc;
        out_set->UsedByCurrPicS1[i] = used_by_curr_pic_flag[j];
        i++;
      }
    }

    // frame 0
    if (DeltaRPS>0 && use_delta_flag[nDeltaPocsRIdx]) {
      if (i>= MAX_NUM_REF_PICS) { return false; }

      out_set->DeltaPocS1[i] = DeltaRPS;
      out_set->UsedByCurrPicS1[i] = used_by_curr_pic_flag[nDeltaPocsRIdx];
      i++;
    }

    // positive list
    for (int j=0;j<nPositiveRIdx;j++) {
      int dPoc = sets[RIdx].DeltaPocS1[j] + DeltaRPS;
      if (dPoc>0 && use_delta_flag[nNegativeRIdx+j]) {
        if (i>= MAX_NUM_REF_PICS) { return false; }

        out_set->DeltaPocS1[i] = dPoc;
        out_set->UsedByCurrPicS1[i] = used_by_curr_pic_flag[nNegativeRIdx+j];
        i++;
      }
    }

    out_set->NumPositivePics = i;

  } else {

    // --- first, read the number of past and future frames in this set ---

    int num_negative_pics = get_uvlc(br);
    int num_positive_pics = get_uvlc(br);

    if (num_negative_pics == UVLC_ERROR ||
        num_positive_pics == UVLC_ERROR) {
      // invalid num-ref-pics value
      errqueue->add_warning(DE265_WARNING_MAX_NUM_REF_PICS_EXCEEDED, false);
      return false;
    }

    // total number of reference pictures may not exceed buffer capacity
    if (num_negative_pics + num_positive_pics >
        sps->sps_max_dec_pic_buffering[ sps->sps_max_sub_layers-1 ]) {

      out_set->NumNegativePics = 0;
      out_set->NumPositivePics = 0;
      out_set->NumDeltaPocs = 0;
      out_set->NumPocTotalCurr_shortterm_only = 0;

      errqueue->add_warning(DE265_WARNING_MAX_NUM_REF_PICS_EXCEEDED, false);
      return false;
    }

    if (num_negative_pics > MAX_NUM_REF_PICS ||
        num_positive_pics > MAX_NUM_REF_PICS) {
      errqueue->add_warning(DE265_WARNING_MAX_NUM_REF_PICS_EXCEEDED, false);
      return false;
    }

    out_set->NumNegativePics = num_negative_pics;
    out_set->NumPositivePics = num_positive_pics;

    // --- now, read the deltas between the reference frames to fill the lists ---

    // past frames

    int lastPocS=0;
    for (int i=0;i<num_negative_pics;i++) {
      int  delta_poc_s0 = get_uvlc(br);
      if (delta_poc_s0==UVLC_ERROR) { return false; }
      delta_poc_s0++;
      char used_by_curr_pic_s0_flag = get_bits(br,1);

      out_set->DeltaPocS0[i]      = lastPocS - delta_poc_s0;
      out_set->UsedByCurrPicS0[i] = used_by_curr_pic_s0_flag;
      lastPocS = out_set->DeltaPocS0[i];
    }

    // future frames

    lastPocS=0;
    for (int i=0;i<num_positive_pics;i++) {
      int  delta_poc_s1 = get_uvlc(br);
      if (delta_poc_s1==UVLC_ERROR) { return false; }
      delta_poc_s1++;
      char used_by_curr_pic_s1_flag = get_bits(br,1);

      out_set->DeltaPocS1[i]      = lastPocS + delta_poc_s1;
      out_set->UsedByCurrPicS1[i] = used_by_curr_pic_s1_flag;
      lastPocS = out_set->DeltaPocS1[i];
    }
  }


  out_set->compute_derived_values();

  return true;
}


bool write_short_term_ref_pic_set_nopred(error_queue* errqueue,
                                         const seq_parameter_set* sps,
                                         CABAC_encoder& out,
                                         const ref_pic_set* in_set, // which set to write
                                         int idxRps,  // index of the set to be written
                                         const std::vector<ref_pic_set>& sets, // previously read sets
                                         bool sliceRefPicSet) // is this in the slice header?
{
  if (idxRps != 0) {
    // inter_ref_pic_set_prediction_flag
    out.write_bit(0);
  }


  // --- first, write the number of past and future frames in this set ---

  out.write_uvlc(in_set->NumNegativePics);
  out.write_uvlc(in_set->NumPositivePics);

  // --- now, write the deltas between the reference frames to fill the lists ---

  // past frames

  int lastPocS=0;
  for (int i=0;i<in_set->NumNegativePics;i++) {
    int  delta_poc_s0 = lastPocS - in_set->DeltaPocS0[i];
    char used_by_curr_pic_s0_flag = in_set->UsedByCurrPicS0[i];

    assert(delta_poc_s0 >= 1);
    out.write_uvlc(delta_poc_s0-1);
    out.write_bit(used_by_curr_pic_s0_flag);
    lastPocS = in_set->DeltaPocS0[i];
  }

  // future frames

  lastPocS=0;
  for (int i=0;i<in_set->NumPositivePics;i++) {
    int  delta_poc_s1 = in_set->DeltaPocS1[i] - lastPocS;
    char used_by_curr_pic_s1_flag = in_set->UsedByCurrPicS1[i];

    assert(delta_poc_s1 >= 1);
    out.write_uvlc(delta_poc_s1-1);
    out.write_bit(used_by_curr_pic_s1_flag);
    lastPocS = in_set->DeltaPocS1[i];
  }

  return true;
}


bool write_short_term_ref_pic_set(error_queue* errqueue,
                                  const seq_parameter_set* sps,
                                  CABAC_encoder& out,
                                  const ref_pic_set* in_set, // which set to write
                                  int idxRps,  // index of the set to be read
                                  const std::vector<ref_pic_set>& sets, // previously read sets
                                  bool sliceRefPicSet) // is this in the slice header?
{
  return write_short_term_ref_pic_set_nopred(errqueue, sps, out, in_set, idxRps, sets,
                                             sliceRefPicSet);
}


void dump_short_term_ref_pic_set(const ref_pic_set* set, FILE* fh)
{
  log2fh(fh,"NumDeltaPocs: %d [-:%d +:%d]\n", set->NumDeltaPocs,
         set->NumNegativePics, set->NumPositivePics);

  log2fh(fh,"DeltaPocS0:");
  for (int i=0;i<set->NumNegativePics;i++) {
    if (i) { log2fh(fh,","); }
    log2fh(fh," %d/%d",set->DeltaPocS0[i],set->UsedByCurrPicS0[i]);
  }
  log2fh(fh,"\n");

  log2fh(fh,"DeltaPocS1:");
  for (int i=0;i<set->NumPositivePics;i++) {
    if (i) { log2fh(fh,","); }
    log2fh(fh," %d/%d",set->DeltaPocS1[i],set->UsedByCurrPicS1[i]);
  }
  log2fh(fh,"\n");
}


void dump_compact_short_term_ref_pic_set(const ref_pic_set* set, int range, FILE* fh)
{
  char *const log = (char *)alloca((range+1+range+1) * sizeof(char));
  log[2*range+1] = 0;
  for (int i=0;i<2*range+1;i++) log[i]='.';
  log[range]='|';

  for (int i=set->NumNegativePics-1;i>=0;i--) {
    int n = set->DeltaPocS0[i];
    if (n>=-range && n<=range) {
      if (set->UsedByCurrPicS0[i]) log[n+range] = 'X';
      else log[n+range] = 'o';
    } else { log2fh(fh,"*%d%c ",n, set->UsedByCurrPicS0[i] ? 'X':'o'); }
  }

  for (int i=set->NumPositivePics-1;i>=0;i--) {
    int n = set->DeltaPocS1[i];
    if (n>=-range && n<=range) {
      if (set->UsedByCurrPicS1[i]) log[n+range] = 'X';
      else log[n+range] = 'o';
    } else { log2fh(fh,"*%d%c ",n, set->UsedByCurrPicS1[i] ? 'X':'o'); }
  }

  log2fh(fh,"*%s\n",log);
}
