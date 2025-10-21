// Copyright (c) 2022 Denis Mikhailov
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_TRAITS_FWD_HPP
#define PFR_DETAIL_TRAITS_FWD_HPP
#pragma once

#include <pfr/detail/config.hpp>

#if !defined(PFR_USE_MODULES) || defined(PFR_INTERFACE_UNIT)

namespace pfr {

PFR_BEGIN_MODULE_EXPORT

template<class T, class WhatFor>
struct is_reflectable;

PFR_END_MODULE_EXPORT

} // namespace pfr

#endif  // #if !defined(PFR_USE_MODULES) || defined(PFR_INTERFACE_UNIT)

#endif // PFR_DETAIL_TRAITS_FWD_HPP

