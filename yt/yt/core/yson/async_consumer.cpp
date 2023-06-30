#include "async_consumer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TAsyncYsonConsumerAdapter::TAsyncYsonConsumerAdapter(IYsonConsumer* underlyingConsumer)
    : UnderlyingConsumer_(underlyingConsumer)
{ }

void TAsyncYsonConsumerAdapter::OnStringScalar(TStringBuf value)
{
    UnderlyingConsumer_->OnStringScalar(value);
}

void TAsyncYsonConsumerAdapter::OnInt64Scalar(i64 value)
{
    UnderlyingConsumer_->OnInt64Scalar(value);
}

void TAsyncYsonConsumerAdapter::OnUint64Scalar(ui64 value)
{
    UnderlyingConsumer_->OnUint64Scalar(value);
}

void TAsyncYsonConsumerAdapter::OnDoubleScalar(double value)
{
    UnderlyingConsumer_->OnDoubleScalar(value);
}

void TAsyncYsonConsumerAdapter::OnBooleanScalar(bool value)
{
    UnderlyingConsumer_->OnBooleanScalar(value);
}

void TAsyncYsonConsumerAdapter::OnEntity()
{
    UnderlyingConsumer_->OnEntity();
}

void TAsyncYsonConsumerAdapter::OnBeginList()
{
    UnderlyingConsumer_->OnBeginList();
}

void TAsyncYsonConsumerAdapter::OnListItem()
{
    UnderlyingConsumer_->OnListItem();
}

void TAsyncYsonConsumerAdapter::OnEndList()
{
    UnderlyingConsumer_->OnEndList();
}

void TAsyncYsonConsumerAdapter::OnBeginMap()
{
    UnderlyingConsumer_->OnBeginMap();
}

void TAsyncYsonConsumerAdapter::OnKeyedItem(TStringBuf key)
{
    UnderlyingConsumer_->OnKeyedItem(key);
}

void TAsyncYsonConsumerAdapter::OnEndMap()
{
    UnderlyingConsumer_->OnEndMap();
}

void TAsyncYsonConsumerAdapter::OnBeginAttributes()
{
    UnderlyingConsumer_->OnBeginAttributes();
}

void TAsyncYsonConsumerAdapter::OnEndAttributes()
{
    UnderlyingConsumer_->OnEndAttributes();
}

void TAsyncYsonConsumerAdapter::OnRaw(TStringBuf yson, EYsonType type)
{
    UnderlyingConsumer_->OnRaw(yson, type);
}

void TAsyncYsonConsumerAdapter::OnRaw(TFuture<TYsonString> /*asyncStr*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
