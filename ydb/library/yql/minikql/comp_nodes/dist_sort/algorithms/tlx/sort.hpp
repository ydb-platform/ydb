/*******************************************************************************
 * tlx/sort.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_HEADER
#define TLX_SORT_HEADER

//! \defgroup tlx_sort Sorting Algorithms
//! Specialized Sorting Algorithms

/*[[[perl
print "#include <$_>\n" foreach sort grep(!/_impl/, glob("tlx/sort/"."*.hpp"));
]]]*/
#include <tlx/sort/parallel_mergesort.hpp>
#include <tlx/sort/strings.hpp>
#include <tlx/sort/strings_parallel.hpp>
// [[[end]]]

#endif // !TLX_SORT_HEADER

/******************************************************************************/
