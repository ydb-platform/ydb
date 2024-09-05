#include "logger_owner.h"

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

void TLoggerOwner::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::Logger>("logger")();
}

PHOENIX_DEFINE_TYPE(TLoggerOwner);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
