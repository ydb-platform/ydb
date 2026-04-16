#include "precise_time.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <>
void WriteSchema<TPreciseInstant>(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& /*options*/)
{
    BuildYsonFluently(consumer)
        .Value("uint64");
}

void Serialize(const TPreciseInstant& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnUint64Scalar(value.GetValue());
}

void Deserialize(TPreciseInstant& value, INodePtr node)
{
     if (node->GetType() == ENodeType::Uint64) {
        value = TInstant::FromValue(node->AsUint64()->GetValue());
    } else {
        THROW_ERROR_EXCEPTION("Cannot parse TPreciseInstant value from %Qlv",
            node->GetType());
    }
}

void Deserialize(TPreciseInstant& value, NYson::TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    if ((*cursor)->GetType() == NYson::EYsonItemType::Uint64Value) {
        value = TInstant::FromValue((*cursor)->UncheckedAsUint64());
        cursor->Next();
    } else {
        THROW_ERROR_EXCEPTION("Cannot parse TPreciseInstant value from %Qlv",
            (*cursor)->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <>
void WriteSchema<TPreciseDuration>(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& /*options*/)
{
    BuildYsonFluently(consumer)
        .Value("uint64");
}

void Serialize(const TPreciseDuration& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnUint64Scalar(value.GetValue());
}

void Deserialize(TPreciseDuration& value, INodePtr node)
{
     if (node->GetType() == ENodeType::Uint64) {
        value = TDuration::FromValue(node->AsUint64()->GetValue());
    } else {
        THROW_ERROR_EXCEPTION("Cannot parse TPreciseDuration value from %Qlv",
            node->GetType());
    }
}

void Deserialize(TPreciseDuration& value, NYson::TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    if ((*cursor)->GetType() == NYson::EYsonItemType::Uint64Value) {
        value = TDuration::FromValue((*cursor)->UncheckedAsUint64());
        cursor->Next();
    } else {
        THROW_ERROR_EXCEPTION("Cannot parse TPreciseDuration value from %Qlv",
            (*cursor)->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
