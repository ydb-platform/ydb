#include "producer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonProducer& value, IYsonConsumer* consumer)
{
    value.Run(consumer);
}

void Serialize(const TYsonCallback& value, IYsonConsumer* consumer)
{
    Serialize(TYsonProducer(value), consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
