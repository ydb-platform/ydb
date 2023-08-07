#include "statistics_producer.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NYTAlloc {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateStatisticsProducer()
{
    // COMPAT(babenko): this will die soon anyway.
    return BIND([] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
