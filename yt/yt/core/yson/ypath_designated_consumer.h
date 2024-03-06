#include "public.h"
#include "consumer.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMissingPathMode,
    (ThrowError)
    (Ignore)
);

std::unique_ptr<NYson::IYsonConsumer> CreateYPathDesignatedConsumer(
    NYPath::TYPath path,
    EMissingPathMode missingPathMode,
    NYson::IYsonConsumer* underlyingConsumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
