#pragma once

#include <library/cpp/yt/error/error.h>

#include <util/system/platform.h>

namespace NYT::NProcFS {

////////////////////////////////////////////////////////////////////////////////

int GetThreadCount();

YT_DEFINE_ERROR_ENUM(
    ((FailedToParseProcFS)     (30))
    ((NoSuchInfoInProcFS)      (31))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProcFS
