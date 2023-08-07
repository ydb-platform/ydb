#ifndef PRODUCER_INL_H_
#error "Direct inclusion of this file is not allowed, include producer.h"
// For the sake of sane code completion.
#include "producer.h"
#endif

#include "public.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

template <class... TAdditionalArgs>
TExtendedYsonProducer<TAdditionalArgs...>::TExtendedYsonProducer(
    TExtendedYsonProducer::TUnderlyingCallback callback,
    EYsonType type)
    : Type_(type)
    , Callback_(std::move(callback))
{
    YT_VERIFY(Callback_);
}

template <class... TAdditionalArgs>
void TExtendedYsonProducer<TAdditionalArgs...>::Run(IYsonConsumer* consumer, TAdditionalArgs... args) const
{
    Callback_(consumer, std::forward<TAdditionalArgs>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson