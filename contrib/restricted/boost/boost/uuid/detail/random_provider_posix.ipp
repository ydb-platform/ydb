/* boost uuid/detail/random_provider_posix implementation
*
* Copyright Jens Maurer 2000
* Copyright 2007 Andy Tompkins.
* Copyright Steven Watanabe 2010-2011
* Copyright 2017 James E. King III
*
* Distributed under the Boost Software License, Version 1.0. (See
* accompanying file LICENSE_1_0.txt or copy at
* http://www.boost.org/LICENSE_1_0.txt)
*
* $Id$
*/

#include <boost/core/ignore_unused.hpp>
#include <boost/throw_exception.hpp>
#include <boost/uuid/entropy_error.hpp>
#include <cerrno>
#include <fcntl.h>    // open
#include <sys/stat.h>
#include <sys/types.h>
#if defined(BOOST_HAS_UNISTD_H)
#include <unistd.h>
#endif

#ifndef BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_CLOSE
#define BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_CLOSE ::close
#endif
#ifndef BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_OPEN
#define BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_OPEN ::open
#endif
#ifndef BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_READ
#define BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_READ ::read
#endif

namespace boost {
namespace uuids {
namespace detail {

class random_provider_base
{
  public:
    random_provider_base()
      : fd_(0)
    {
        int flags = O_RDONLY;
#if defined(O_CLOEXEC)
        flags |= O_CLOEXEC;
#endif
        fd_ = BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_OPEN("/dev/urandom", flags);

        if (-1 == fd_)
        {
            int err = errno;
            BOOST_THROW_EXCEPTION(entropy_error(err, "open /dev/urandom"));
        }
    }

    ~random_provider_base() BOOST_NOEXCEPT
    {
        if (fd_)
        {
            ignore_unused(BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_CLOSE(fd_));
        }
    }

    //! Obtain entropy and place it into a memory location
    //! \param[in]  buf  the location to write entropy
    //! \param[in]  siz  the number of bytes to acquire
    void get_random_bytes(void *buf, size_t siz)
    {
        size_t offset = 0;
        do
        {
            ssize_t sz = BOOST_UUID_RANDOM_PROVIDER_POSIX_IMPL_READ(
                fd_, static_cast<char *>(buf) + offset, siz - offset);

            if (sz < 1)
            {
                int err = errno;
                BOOST_THROW_EXCEPTION(entropy_error(err, "read"));
            }

            offset += sz;

        } while (offset < siz);
    }

  private:
    int fd_;
};

} // detail
} // uuids
} // boost
