/*******************************************************************************
 * tlx/container.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_HEADER
#define TLX_CONTAINER_HEADER

//! \defgroup tlx_container Containers and Data Structures
//! Containers and Data Structures

/*[[[perl
print "#include <$_>\n" foreach sort glob("tlx/container/"."*.hpp");
]]]*/
#include <tlx/container/btree.hpp>
#include <tlx/container/btree_map.hpp>
#include <tlx/container/btree_multimap.hpp>
#include <tlx/container/btree_multiset.hpp>
#include <tlx/container/btree_set.hpp>
#include <tlx/container/d_ary_addressable_int_heap.hpp>
#include <tlx/container/d_ary_heap.hpp>
#include <tlx/container/loser_tree.hpp>
#include <tlx/container/lru_cache.hpp>
#include <tlx/container/radix_heap.hpp>
#include <tlx/container/ring_buffer.hpp>
#include <tlx/container/simple_vector.hpp>
#include <tlx/container/splay_tree.hpp>
#include <tlx/container/string_view.hpp>
// [[[end]]]

#endif // !TLX_CONTAINER_HEADER

/******************************************************************************/
