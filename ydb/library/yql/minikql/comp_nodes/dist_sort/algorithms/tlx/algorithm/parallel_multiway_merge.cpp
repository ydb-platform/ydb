/*******************************************************************************
 * tlx/algorithm/parallel_multiway_merge.cpp
 *
 * Parallel multiway merge settings
 *
 * Copied and modified from STXXL, see http://stxxl.org, which itself extracted
 * it from MCSTL http://algo2.iti.uni-karlsruhe.de/singler/mcstl/. Both are
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2014-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <parallel_multiway_merge.hpp>

namespace tlx {

//! \addtogroup tlx_algorithm
//! \{

// parallel multiway merge settings

bool parallel_multiway_merge_force_sequential = false;

bool parallel_multiway_merge_force_parallel = false;

size_t parallel_multiway_merge_minimal_k = 2;

size_t parallel_multiway_merge_minimal_n = 1000;

size_t parallel_multiway_merge_oversampling = 10;

//! \}

} // namespace tlx

/******************************************************************************/
