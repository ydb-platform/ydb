/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * Authors: Dirk Farin <farin@struktur.de>
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

#ifndef ALGO_H
#define ALGO_H

#include "libde265/encoder/encoder-types.h"


/* When entering the next recursion level, it is assumed that
   a valid CB structure is passed down. Ownership is transferred to
   the new algorithm. That algorithm passes back a (possibly different)
   CB structure that the first algorithm should use. The receiving
   algorithm will be the owner of the passed back algorithm.
   The original CB structure might have been deleted in the called algorithm.

   When using CodingOptions, it is important to set the passed back
   enc_node in the CodingOption (set_node()), so that the CodingOption
   can correctly handle ownership and delete nodes as needed.

   The context_model_table passed down is at the current state.
   When the algorithm returns, the state should represent the state
   after running this algorithm.
 */

class Algo
{
 public:
  virtual ~Algo() { }

  virtual const char* name() const { return "noname"; }

#ifdef DE265_LOG_DEBUG
  void enter();
  void descend(const enc_node* node,const char* option_description, ...);
  void ascend(const enc_node* resultNode=NULL, const char* fmt=NULL, ...);
  void leaf(const enc_node* node,const char* option_description, ...);
#else
  inline void enter() { }
  inline void descend(const enc_node*,const char*, ...) { }
  inline void ascend(const enc_node* resultNode=NULL,const char* fmt=NULL, ...) { }
  inline void leaf(const enc_node*,const char*, ...) { }
#endif
};


class Algo_CB : public Algo
{
 public:
  virtual ~Algo_CB() { }

  /* The context_model_table that is provided can be modified and
     even released in the function. On exit, it should be filled with
     a (optionally new) context_model_table that represents the state
     after encoding the syntax element. However, to speed up computation,
     it is also allowed to not modify the context_model_table at all.
   */
  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb) = 0;
};


class Algo_PB : public Algo
{
 public:
  virtual ~Algo_PB() { }

  virtual enc_cb* analyze(encoder_context*,
                          context_model_table&,
                          enc_cb* cb,
                          int PBidx, int x,int y,int w,int h) = 0;
};


#endif
