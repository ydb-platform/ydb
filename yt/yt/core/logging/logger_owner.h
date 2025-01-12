#pragma once

#include "serializable_logger.h"

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NLogging {

using NPhoenix::TLoadContext;
using NPhoenix::TSaveContext;
using NPhoenix::TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

//! Typically serves as a virtual base for classes that need a member logger.
class TLoggerOwner
{
protected:
    TSerializableLogger Logger;

    TLoggerOwner() = default;

    PHOENIX_DECLARE_TYPE(TLoggerOwner, 0x9feb0be1);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
