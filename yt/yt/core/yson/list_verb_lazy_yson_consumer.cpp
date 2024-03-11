#include "list_verb_lazy_yson_consumer.h"

#include "yt/yt/core/misc/error.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TListVerbLazyYsonConsumer::TListVerbLazyYsonConsumer(
    IYsonConsumer* underlyingConsumer,
    std::optional<int> limit)
    : UnderlyingConsumer_(underlyingConsumer)
    , Limit_(limit)
{ }

void TListVerbLazyYsonConsumer::OnMyBeginMap()
{
    UnderlyingConsumer_->OnBeginList();
}

void TListVerbLazyYsonConsumer::OnMyKeyedItem(TStringBuf key)
{
    if (!Limit_ || Limit_.value() > Index_) {
        UnderlyingConsumer_->OnListItem();
        UnderlyingConsumer_->OnStringScalar(key);
        ++Index_;
    }

    Forward(GetNullYsonConsumer());
}

void TListVerbLazyYsonConsumer::OnMyEndMap()
{
    UnderlyingConsumer_->OnEndList();
}

void TListVerbLazyYsonConsumer::OnMyBeginAttributes()
{
    Forward(GetNullYsonConsumer(), /*onFinished*/ nullptr, EYsonType::MapFragment);
}

void TListVerbLazyYsonConsumer::OnMyEndAttributes()
{ }

void TListVerbLazyYsonConsumer::OnMyBeginList()
{
    ThrowUnexpectedToken("BeginList");
}

void TListVerbLazyYsonConsumer::OnMyListItem()
{
    ThrowUnexpectedToken("ListItem");
}

void TListVerbLazyYsonConsumer::OnMyEndList()
{
    ThrowUnexpectedToken("EndList");
}

void TListVerbLazyYsonConsumer::OnMyStringScalar(TStringBuf value)
{
    ThrowUnexpectedToken(Format("StringScalar: %qv", value));
}

void TListVerbLazyYsonConsumer::OnMyInt64Scalar(i64 value)
{
    ThrowUnexpectedToken(Format("Int64Scalar: %qv", value));
}

void TListVerbLazyYsonConsumer::OnMyUint64Scalar(ui64 value)
{
    ThrowUnexpectedToken(Format("Uint64Scalar: %qv", value));
}

void TListVerbLazyYsonConsumer::OnMyDoubleScalar(double value)
{
    ThrowUnexpectedToken(Format("DoubleScalar: %qv", value));
}

void TListVerbLazyYsonConsumer::OnMyBooleanScalar(bool value)
{
    ThrowUnexpectedToken(Format("BooleanScalar: %qv", value));
}

void TListVerbLazyYsonConsumer::OnMyEntity()
{
    ThrowUnexpectedToken("Entity");
}

void TListVerbLazyYsonConsumer::ThrowUnexpectedToken(TStringBuf unexpectedToken)
{
    THROW_ERROR_EXCEPTION("Unexpected yson element; expected MapNode but was %v", unexpectedToken);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
