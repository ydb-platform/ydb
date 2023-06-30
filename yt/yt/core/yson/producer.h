#pragma once

#include "public.h"
#include "string.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! A callback capable of generating YSON by calling appropriate
//! methods for its IYsonConsumer argument.
using TYsonCallback = TCallback<void(IYsonConsumer*)>;

//! A callback capable of generating YSON by calling appropriate
//! methods for its IYsonConsumer and some additional arguments.
template <class... TAdditionalArgs>
using TExtendedYsonCallback = TCallback<void(IYsonConsumer*, TAdditionalArgs...)>;

////////////////////////////////////////////////////////////////////////////////

class TYsonProducer
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

public:
    TYsonProducer() = default;
    TYsonProducer(
        TYsonCallback callback,
        EYsonType type = NYson::EYsonType::Node);

    void Run(IYsonConsumer* consumer) const;

private:
    TYsonCallback Callback_;
};

////////////////////////////////////////////////////////////////////////////////

template <class... TAdditionalArgs>
class TExtendedYsonProducer
{
    using TUnderlyingCallback = TExtendedYsonCallback<TAdditionalArgs...>;
public:
    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

public:
    TExtendedYsonProducer() = default;
    TExtendedYsonProducer(
        TUnderlyingCallback callback,
        EYsonType type = NYson::EYsonType::Node);

    void Run(IYsonConsumer* consumer, TAdditionalArgs... args) const;

private:
    TUnderlyingCallback Callback_;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonProducer& value, NYson::IYsonConsumer* consumer);
void Serialize(const TYsonCallback& value, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PRODUCER_INL_H_
#include "producer-inl.h"
#undef PRODUCER_INL_H_
