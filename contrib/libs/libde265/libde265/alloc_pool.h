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

#ifndef ALLOC_POOL_H
#define ALLOC_POOL_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <vector>
#include <cstddef>
#include <cstdint>


class alloc_pool
{
 public:
  alloc_pool(size_t objSize, int poolSize=1000, bool grow=true);
  ~alloc_pool();

  void* new_obj(const size_t size);
  void  delete_obj(void*);
  void  purge();

 private:
  size_t mObjSize;
  int    mPoolSize;
  bool   mGrow;

  std::vector<uint8_t*> m_memBlocks;
  std::vector<void*>    m_freeList;

  void add_memory_block();
};

#endif
