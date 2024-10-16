// Copyright (c) 2016-2023 Antony Polukhin
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_CORE_HPP
#define PFR_DETAIL_CORE_HPP
#pragma once

#include <pfr/detail/config.hpp>

// Each core provides `pfr::detail::tie_as_tuple` and
// `pfr::detail::for_each_field_dispatcher` functions.
//
// The whole PFR library is build on top of those two functions.
#if PFR_USE_CPP17
#   include <pfr/detail/core17.hpp>
#elif PFR_USE_LOOPHOLE
#   include <pfr/detail/core14_loophole.hpp>
#else
#   include <pfr/detail/core14_classic.hpp>
#endif

#endif // PFR_DETAIL_CORE_HPP
