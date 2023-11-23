#ifndef BOOST_QVM_ERROR_HPP_INCLUDED
#define BOOST_QVM_ERROR_HPP_INCLUDED

// Copyright 2008-2022 Emil Dotchevski and Reverge Studios, Inc.

// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <exception>

#include <boost/qvm/config.hpp>

namespace boost { namespace qvm {

struct
error:
    std::exception
    {
    char const *
    what() const BOOST_QVM_NOEXCEPT
        {
        return "Boost QVM error";
        }

    ~error() BOOST_QVM_NOEXCEPT
        {
        }
    };

struct zero_determinant_error: error { };
struct zero_magnitude_error: error { };

} }

#endif
