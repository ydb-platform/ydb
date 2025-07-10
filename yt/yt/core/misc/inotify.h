#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NFS {

////////////////////////////////////////////////////////////////////////////////
// Wrappers for Linux Inofity API

// See inotify.h ABI
DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EInotifyWatchEvents, ui32,
    ((Access)       (0x00000001))
    ((Modify)       (0x00000002))
    ((Attrib)       (0x00000004))
    ((CloseWrite)   (0x00000008))
    ((CloseNowrite) (0x00000010))
    ((Close)        (EInotifyWatchEvents::CloseWrite | EInotifyWatchEvents::CloseNowrite))
    ((Open)         (0x00000020))
    ((MovedFrom)    (0x00000040))
    ((MovedTo)      (0x00000080))
    ((Move)         (EInotifyWatchEvents::MovedFrom | EInotifyWatchEvents::MovedTo))
    ((Create)       (0x00000100))
    ((Delete)       (0x00000200))
    ((DeleteSelf)   (0x00000400))
    ((MoveSelf)     (0x00000800))
);

////////////////////////////////////////////////////////////////////////////////

class TInotifyHandle
    : private TNonCopyable
{
public:
    TInotifyHandle();
    ~TInotifyHandle();

    struct TPollResult
    {
        int WD = -1;
        EInotifyWatchEvents Events;
    };

    std::optional<TPollResult> Poll();

    DEFINE_BYVAL_RO_PROPERTY(int, FD, -1);
};

////////////////////////////////////////////////////////////////////////////////

class TInotifyWatch
    : private TNonCopyable
{
public:
    TInotifyWatch(
        TInotifyHandle* handle,
        std::string path,
        EInotifyWatchEvents mask);
    ~TInotifyWatch();

    DEFINE_BYVAL_RO_PROPERTY(int, FD, -1);
    DEFINE_BYVAL_RO_PROPERTY(int, WD, -1);

private:
    const std::string Path_;
    const EInotifyWatchEvents Mask_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFS
