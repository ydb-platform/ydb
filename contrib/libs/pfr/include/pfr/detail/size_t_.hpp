// Copyright (c) 2016-2023 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_SIZE_T_HPP
#define PFR_DETAIL_SIZE_T_HPP
#pragma once

namespace pfr { namespace detail {

///////////////////// General utility stuff
template <std::size_t Index>
using size_t_ = std::integral_constant<std::size_t, Index >;

}} // namespace pfr::detail

#endif // PFR_DETAIL_SIZE_T_HPP
