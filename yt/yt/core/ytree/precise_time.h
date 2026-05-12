#pragma once

#include "yson_struct.h"
#include "yson_schema.h"

#include <util/datetime/base.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TPreciseInstant
    : public TInstant
{
public:
    using TInstant::TInstant;

    constexpr TPreciseInstant(const TInstant& instant)
        : TInstant(instant)
    { }

    constexpr TPreciseInstant(TInstant&& instant)
        : TInstant(std::move(instant))
    { }
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TPreciseInstant& value, NYson::IYsonConsumer* consumer);
void Deserialize(TPreciseInstant& value, INodePtr node);
void Deserialize(TPreciseInstant& value, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

class TPreciseDuration
    : public TDuration
{
public:
    using TDuration::TDuration;

    constexpr TPreciseDuration(const TDuration& duration)
        : TDuration(duration)
    { }

    constexpr TPreciseDuration(TDuration&& duration)
        : TDuration(std::move(duration))
    { }
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TPreciseDuration& value, NYson::IYsonConsumer* consumer);
void Deserialize(TPreciseDuration& value, INodePtr node);
void Deserialize(TPreciseDuration& value, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

Y_DECLARE_PODTYPE(NYT::NYTree::TPreciseInstant);
Y_DECLARE_PODTYPE(NYT::NYTree::TPreciseDuration);
