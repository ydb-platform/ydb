#pragma once

#include <library/cpp/yt/memory/serialize.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NYTree {

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnrecognizedStrategy,
    (Drop)
    (Keep)
    (KeepRecursive)
    (Throw)
    (ThrowRecursive)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
