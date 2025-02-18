#pragma once

#include <library/cpp/yt/misc/enum.h>

#include <errno.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! NYT::TError::FromSystem adds this value to a system errno. The enum
//! below lists several errno's that are used in our code.
constexpr int LinuxErrorCodeBase = 4200;
constexpr int LinuxErrorCodeCount = 2000;

DEFINE_ENUM(ELinuxErrorCode,
    ((NOENT)              ((LinuxErrorCodeBase + ENOENT)))
    ((IO)                 ((LinuxErrorCodeBase + EIO)))
    ((ACCESS)             ((LinuxErrorCodeBase + EACCES)))
    ((NFILE)              ((LinuxErrorCodeBase + ENFILE)))
    ((MFILE)              ((LinuxErrorCodeBase + EMFILE)))
    ((NOSPC)              ((LinuxErrorCodeBase + ENOSPC)))
    ((PIPE)               ((LinuxErrorCodeBase + EPIPE)))
    ((CONNRESET)          ((LinuxErrorCodeBase + ECONNRESET)))
    ((TIMEDOUT)           ((LinuxErrorCodeBase + ETIMEDOUT)))
    ((CONNREFUSED)        ((LinuxErrorCodeBase + ECONNREFUSED)))
    ((DQUOT)              ((LinuxErrorCodeBase + EDQUOT)))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
