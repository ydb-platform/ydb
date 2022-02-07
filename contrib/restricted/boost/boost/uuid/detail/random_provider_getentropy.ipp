//
// Copyright (c) 2017 James E. King III
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
//   http://www.boost.org/LICENCE_1_0.txt)
//
// getentropy() capable platforms
//

#include <boost/throw_exception.hpp>
#include <cerrno>
#include <unistd.h>

namespace boost {
namespace uuids {
namespace detail {

class random_provider_base
{
  public:
    //! Obtain entropy and place it into a memory location
    //! \param[in]  buf  the location to write entropy
    //! \param[in]  siz  the number of bytes to acquire
    void get_random_bytes(void *buf, size_t siz)
    {
        if (-1 == getentropy(buf, siz))
        {
            int err = errno;
            BOOST_THROW_EXCEPTION(entropy_error(err, "getentropy"));
        }
    }
};

} // detail
} // uuids
} // boost
