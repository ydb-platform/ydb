// Copyright (c) 2016-2023 Antony Polukhin
// Copyright (c) 2022 Denis Mikhailov
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef PFR_DETAIL_CONFIG_HPP
#define PFR_DETAIL_CONFIG_HPP
#pragma once

#include <pfr/config.hpp>

#if !PFR_ENABLED

#error Boost.PFR library is not supported in your environment.             \
       Try one of the possible solutions:                                  \
       1. try to take away an '-DPFR_ENABLED=0', if it exists        \
       2. enable C++14;                                                    \
       3. enable C++17;                                                    \
       4. update your compiler;                                            \
       or disable this error by '-DPFR_ENABLED=1' if you really know what are you doing.

#endif // !PFR_ENABLED

#endif // PFR_DETAIL_CONFIG_HPP

