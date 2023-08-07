#include "public.h"
#include "consumer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Depth limit must be non-negative.
std::unique_ptr<IYsonConsumer> CreateDepthLimitingYsonConsumer(
    NYson::IYsonConsumer* underlyingConsumer,
    int depthLimit,
    EYsonType ysonType = EYsonType::Node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
