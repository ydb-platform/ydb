/*
 * H.265 video codec.
 * Copyright (c) 2014 struktur AG, Dirk Farin <farin@struktur.de>
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

#include "libde265/alloc_pool.h"
#include "libde265/util.h"
#include <assert.h>
#include <stdio.h>

#define DEBUG_MEMORY 1


alloc_pool::alloc_pool(size_t objSize, int poolSize, bool grow)
  : mObjSize(objSize),
    mPoolSize(poolSize),
    mGrow(grow)
{
  m_freeList.reserve(poolSize);
  m_memBlocks.reserve(8);

  add_memory_block();
}


void alloc_pool::add_memory_block()
{
  uint8_t* p = new uint8_t[mObjSize * mPoolSize];
  m_memBlocks.push_back(p);

  for (int i=0;i<mPoolSize;i++)
    {
      m_freeList.push_back(p + (mPoolSize-1-i) * mObjSize);
    }
}

alloc_pool::~alloc_pool()
{
  FOR_LOOP(uint8_t*, p, m_memBlocks) {
    delete[] p;
  }
}


void* alloc_pool::new_obj(const size_t size)
{
  if (size != mObjSize) {
    return ::operator new(size);
  }

  if (m_freeList.size()==0) {
    if (mGrow) {
      add_memory_block();
      if (DEBUG_MEMORY) { fprintf(stderr,"additional block allocated in memory pool\n"); }
    }
    else {
      return NULL;
    }
  }

  assert(!m_freeList.empty());

  void* p = m_freeList.back();
  m_freeList.pop_back();

  return p;
}


void  alloc_pool::delete_obj(void* obj)
{
  int memBlockSize = mObjSize * mPoolSize;

  FOR_LOOP(uint8_t*, memBlk, m_memBlocks) {
    if (memBlk <= obj && obj < memBlk + memBlockSize) {
      m_freeList.push_back(obj);
      return;
    }
  }

  ::operator delete(obj);
}
