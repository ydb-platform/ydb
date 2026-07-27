#pragma once

#include "public.h"
#include "string.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! A callback capable of generating YSON by calling appropriate
//! methods for its IYsonConsumer and some additional arguments.
template <class... TAdditionalArgs>
using TParametricYsonCallback = TCallback<void(IYsonConsumer*, TAdditionalArgs...)>;

//! A callback capable of generating YSON by calling appropriate
//! methods for its IYsonConsumer argument.
using TYsonCallback = TParametricYsonCallback<>;

////////////////////////////////////////////////////////////////////////////////

template <class... TAdditionalArgs>
class TParametricYsonProducer
{
    using TUnderlyingCallback = TParametricYsonCallback<TAdditionalArgs...>;
public:
    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

public:
    TParametricYsonProducer() = default;
    TParametricYsonProducer(
        TUnderlyingCallback callback,
        EYsonType type = NYson::EYsonType::Node);

    void Run(IYsonConsumer* consumer, TAdditionalArgs... args) const;

private:
    TUnderlyingCallback Callback_;
};

//! A producer generating a plain YSON with no additional arguments.
using TYsonProducer = TParametricYsonProducer<>;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonProducer& value, NYson::IYsonConsumer* consumer);
void Serialize(const TYsonCallback& value, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PRODUCER_INL_H_
#include "producer-inl.h"
#undef PRODUCER_INL_H_
