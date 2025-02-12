/*******************************************************************************
 * tlx/die.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_DIE_HEADER
#define TLX_DIE_HEADER

#include <tlx/die/core.hpp>

namespace tlx {

/******************************************************************************/
// die macros

//! Instead of std::terminate(), throw the output the message via an exception.
#define die(msg) \
    tlx_die(msg)

//! Check condition X and die miserably if false. Same as assert() except this
//! is also active in Release mode.
#define die_unless(X) \
    tlx_die_unless(X)

//! Check condition X and die miserably if true. Opposite of assert() except
//! this is also active in Release mode.
#define die_if(X) \
    tlx_die_if(X)

//! Check condition X and die miserably if false. Same as die_unless() except
//! the user additionally passes a message.
#define die_verbose_unless(X, msg) \
    tlx_die_verbose_unless(X, msg)

//! Check condition X and die miserably if false. Same as die_if() except the
//! the user additionally passes a message.
#define die_verbose_if(X, msg) \
    tlx_die_verbose_if(X, msg)

/******************************************************************************/
// die_unequal()

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging.
#define die_unequal(X, Y) \
    tlx_die_unequal(X, Y)

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging. Only active if NDEBUG is not defined.
#define assert_equal(X, Y) \
    tlx_assert_equal(X, Y)

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging. Same as die_unequal() except the user additionally passes
//! a message.
#define die_verbose_unequal(X, Y, msg) \
    tlx_die_verbose_unequal(X, Y, msg)

/******************************************************************************/
// die_unequal_eps()

//! Check that ABS(X - Y) <= eps or die miserably, but output the values of X
//! and Y for better debugging.
#define die_unequal_eps(X, Y, eps) \
    tlx_die_unequal_eps(X, Y, eps)

//! Check that ABS(X - Y) <= eps or die miserably, but output the values of X
//! and Y for better debugging. Same as die_unequal_eps() except the user
//! additionally passes a message.
#define die_verbose_unequal_eps(X, Y, eps, msg) \
    tlx_die_verbose_unequal_eps(X, Y, eps, msg)

//! Check that ABS(X - Y) <= 0.000001 or die miserably, but output the values of
//! X and Y for better debugging.
#define die_unequal_eps6(X, Y) \
    tlx_die_unequal_eps6(X, Y)

//! Check that ABS(X - Y) <= 0.000001 or die miserably, but output the values of
//! X and Y for better debugging. Same as die_unequal_eps6() except the user
//! additionally passes a message.
#define die_verbose_unequal_eps6(X, Y, msg) \
    tlx_die_verbose_unequal_eps6(X, Y, msg)

/******************************************************************************/
// die_equal()

//! Die miserably if X == Y, but first output the values of X and Y for better
//! debugging.
#define die_equal(X, Y) \
    tlx_die_equal(X, Y)

//! Die miserably if X == Y, but first output the values of X and Y for better
//! debugging. Only active if NDEBUG is not defined.
#define assert_unequal(X, Y) \
    tlx_assert_unequal(X, Y)

//! Die miserably if X == Y, but first output the values of X and Y for better
//! debugging. Same as die_equal() except the user additionally passes a
//! message.
#define die_verbose_equal(X, Y, msg) \
    tlx_die_verbose_equal(X, Y, msg)

/******************************************************************************/
// die_unless_throws()

//! Define to check that [code] throws and exception of given type
#define die_unless_throws(code, exception_type) \
    tlx_die_unless_throws(code, exception_type)

} // namespace tlx

#endif // !TLX_DIE_HEADER

/******************************************************************************/
