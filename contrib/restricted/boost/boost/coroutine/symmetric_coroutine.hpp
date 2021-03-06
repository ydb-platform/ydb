
//          Copyright Oliver Kowalke 2009.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_COROUTINES_SYMMETRIC_COROUTINE_H
#define BOOST_COROUTINES_SYMMETRIC_COROUTINE_H

#ifndef BOOST_COROUTINES_NO_DEPRECATION_WARNING
# if defined(_MSC_VER) || defined(__BORLANDC__) || defined(__DMC__)
//#  pragma message ("Warning: Boost.Coroutine is now deprecated. Please switch to Boost.Coroutine2. To disable this warning message, define BOOST_COROUTINES_NO_DEPRECATION_WARNING.")
# elif defined(__GNUC__) || defined(__HP_aCC) || defined(__SUNPRO_CC) || defined(__IBMCPP__)
//#  warning "Boost.Coroutine is now deprecated. Please switch to Boost.Coroutine2. To disable this warning message, define BOOST_COROUTINES_NO_DEPRECATION_WARNING."
# endif
#endif

#include <boost/config.hpp>

#include <boost/coroutine/detail/symmetric_coroutine_call.hpp>
#include <boost/coroutine/detail/symmetric_coroutine_yield.hpp>

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace coroutines {

template< typename T >
struct symmetric_coroutine
{
    typedef detail::symmetric_coroutine_call< T >   call_type;
    typedef detail::symmetric_coroutine_yield< T >  yield_type;
};

}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif

#endif // BOOST_COROUTINES_SYMMETRIC_COROUTINE_H
