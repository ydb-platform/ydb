#include "logger_owner.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

void TLoggerOwner::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, Logger);
}

void TLoggerOwner::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, Logger);
}

void TLoggerOwner::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
