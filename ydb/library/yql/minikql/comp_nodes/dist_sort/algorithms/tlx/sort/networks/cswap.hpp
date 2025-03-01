/*******************************************************************************
 * tlx/sort/networks/cswap.hpp
 *
 * Conditional swap implementation used for sorting networks.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018-2020 Jasper Marianczuk <jasper.marianczuk@gmail.com>
 * Copyright (C) 2020 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_NETWORKS_CSWAP_HEADER
#define TLX_SORT_NETWORKS_CSWAP_HEADER

#include <algorithm>

namespace tlx {

//! \addtogroup tlx_sort
//! \{
//! \name Implementations of Sorting Networks
//! \{

//! Implementations of sorting networks for up to sixteen elements.
namespace sort_networks {

//! Conditional swap implementation used for sorting networks: trivial portable
//! C++ implementation with custom comparison method and std::swap().
template <typename Comparator>
class CS_IfSwap
{
public:
    CS_IfSwap(Comparator cmp) : cmp_(cmp) { }

    template <typename Type>
    inline void operator () (Type& left, Type& right) {
        if (cmp_(right, left)) { std::swap(left, right); }
    }

protected:
    Comparator cmp_;
};

/******************************************************************************/

//! \}
//! \}

} // namespace sort_networks
} // namespace tlx

#endif // !TLX_SORT_NETWORKS_CSWAP_HEADER

/******************************************************************************/
