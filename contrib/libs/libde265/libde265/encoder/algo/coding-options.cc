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

#include "libde265/encoder/algo/coding-options.h"
#include "libde265/encoder/encoder-context.h"


template <class node>
CodingOptions<node>::CodingOptions(encoder_context* ectx, node* _node, context_model_table& tab)
{
  //mCBMode = true;
  mInputNode = _node;
  mContextModelInput = &tab;

  mBestRDO=-1;

  mECtx = ectx;
}

template <class node>
CodingOptions<node>::~CodingOptions()
{
}

template <class node>
CodingOption<node> CodingOptions<node>::new_option(bool active)
{
  if (!active) {
    return CodingOption<node>();
  }


  CodingOptionData opt;

  bool firstOption = mOptions.empty();
  if (firstOption) {
    opt.mNode = mInputNode;
  }
  else {
    opt.mNode = new node(*mInputNode);
  }

  opt.context  = *mContextModelInput;
  opt.computed = false;

  CodingOption<node> option(this, mOptions.size());

  mOptions.push_back( std::move(opt) );

  return option;
}


template <class node>
void CodingOptions<node>::start(enum RateEstimationMethod rateMethod)
{
  /* We don't need the input context model anymore.
     Releasing it now may save a copy during a later decouple().
  */
  mContextModelInput->release();

  bool adaptiveContext;
  switch (rateMethod) {
  case Rate_Default:
    adaptiveContext = mECtx->use_adaptive_context;
    break;
  case Rate_FixedContext:
    adaptiveContext = false;
    break;
  case Rate_AdaptiveContext:
    adaptiveContext = true;
    break;
  }

  if (adaptiveContext) {
    /* If we modify the context models in this algorithm,
       we need separate models for each option.
    */
    for (auto& option : mOptions) {
      option.context.decouple();
    }

    cabac = &cabac_adaptive;
  }
  else {
    cabac = &cabac_constant;
  }
}


template <class node>
void CodingOptions<node>::compute_rdo_costs()
{
  for (size_t i=0;i<mOptions.size();i++) {
    if (mOptions[i].computed) {
      //printf("compute_rdo_costs %d: %f\n",i, mOptions[i].mNode->rate);
      mOptions[i].rdoCost = mOptions[i].mNode->distortion + mECtx->lambda * mOptions[i].mNode->rate;
    }
  }
}


template <class node>
int CodingOptions<node>::find_best_rdo_index()
{
  assert(mOptions.size()>0);


  float bestRDOCost = 0;
  bool  first=true;
  int   bestRDO=-1;

  for (size_t i=0;i<mOptions.size();i++) {
    if (mOptions[i].computed) {
      float cost = mOptions[i].rdoCost;

      //printf("option %d cost: %f\n",i,cost);

      if (first || cost < bestRDOCost) {
        bestRDOCost = cost;
        first = false;
        bestRDO = i;
      }
    }
  }

  return bestRDO;
}


template <class node>
node* CodingOptions<node>::return_best_rdo_node()
{
  int bestRDO = find_best_rdo_index();

  assert(bestRDO>=0);

  *mContextModelInput = mOptions[bestRDO].context;


  // delete all CBs except the best one

  for (size_t i=0;i<mOptions.size();i++) {
    if (i != bestRDO)
      {
        delete mOptions[i].mNode;
        mOptions[i].mNode = NULL;
      }
  }

  return mOptions[bestRDO].mNode;
}


template <class node>
void CodingOption<node>::begin()
{
  assert(mParent);
  assert(mParent->cabac); // did you call CodingOptions.start() ?

  mParent->cabac->reset();
  mParent->cabac->set_context_models( &get_context() );

  mParent->mOptions[mOptionIdx].computed = true;

  // link this node into the coding tree

  node* n = get_node();
  *(n->downPtr) = n;
}


template <class node>
void CodingOption<node>::end()
{
}


template class CodingOptions<enc_tb>;
template class CodingOptions<enc_cb>;

template class CodingOption<enc_tb>;
template class CodingOption<enc_cb>;
