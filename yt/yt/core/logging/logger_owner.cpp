#include "logger_owner.h"

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

void TLoggerOwner::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Logger);
}

PHOENIX_DEFINE_TYPE(TLoggerOwner);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
