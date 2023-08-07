#include "producer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TYsonProducer::TYsonProducer(TYsonCallback callback, EYsonType type)
    : Type_(type)
    , Callback_(std::move(callback))
{
    YT_ASSERT(Callback_);
}

void TYsonProducer::Run(IYsonConsumer* consumer) const
{
    Callback_(consumer);
}

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
