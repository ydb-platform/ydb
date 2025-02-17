// Copyright (c) 2023 Bela Schaum, X-Ryl669, Denis Mikhailov.
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)


// Initial implementation by Bela Schaum, https://github.com/schaumb
// The way to make it union and UB free by X-Ryl669, https://github.com/X-Ryl669
//

#ifndef PFR_DETAIL_FAKE_OBJECT_HPP
#define PFR_DETAIL_FAKE_OBJECT_HPP
#pragma once

#include <pfr/detail/config.hpp>

namespace pfr { namespace detail {

template <class T>
extern const T fake_object;

}} // namespace pfr::detail

#endif // PFR_DETAIL_FAKE_OBJECT_HPP

