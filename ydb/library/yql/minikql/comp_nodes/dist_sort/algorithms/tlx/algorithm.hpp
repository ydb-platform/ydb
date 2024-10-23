/*******************************************************************************
 * tlx/algorithm.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_ALGORITHM_HEADER
#define TLX_ALGORITHM_HEADER

//! \defgroup tlx_algorithm Algorithms
//! Algorithms for iterators and ranges

/*[[[perl
print "#include <$_>\n" foreach sort glob("tlx/algorithm/"."*.hpp");
]]]*/
#include "algorithm/exclusive_scan.hpp"
#include "algorithm/is_sorted_cmp.hpp"
#include "algorithm/merge_advance.hpp"
#include "algorithm/merge_combine.hpp"
#include "algorithm/multisequence_partition.hpp"
#include "algorithm/multisequence_selection.hpp"
#include "algorithm/multiway_merge.hpp"
#include "algorithm/multiway_merge_splitting.hpp"
#include "algorithm/parallel_multiway_merge.hpp"
#include "algorithm/random_bipartition_shuffle.hpp"
// [[[end]]]

#endif // !TLX_ALGORITHM_HEADER

/******************************************************************************/
