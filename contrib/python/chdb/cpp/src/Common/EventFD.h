#pragma once

#if defined(OS_LINUX)

#include <cstddef>
#include <cstdint>


namespace DB_CHDB
{

struct EventFD
{
    EventFD();
    ~EventFD();

    /// Both read() and write() are blocking.
    /// TODO: add non-blocking flag to ctor.
    uint64_t read() const;
    bool write(uint64_t increase = 1) const;

    int fd = -1;
};

}

#else

namespace DB_CHDB
{

struct EventFD
{
};

}

#endif
