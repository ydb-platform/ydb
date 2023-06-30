#pragma once

#include "public.h"
#include "string.h"
#include "consumer.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Extends IYsonConsumer by enabling asynchronously constructed
//! segments to be injected into the stream.
struct IAsyncYsonConsumer
    : public IYsonConsumer
{
    using IYsonConsumer::OnRaw;
    virtual void OnRaw(TFuture<TYsonString> asyncStr) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Turns IYsonConsumer into IAsyncConsumer. No asynchronous calls are allowed.
class TAsyncYsonConsumerAdapter
    : public IAsyncYsonConsumer
{
public:
    explicit TAsyncYsonConsumerAdapter(IYsonConsumer* underlyingConsumer);

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;
    using IYsonConsumer::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type) override;
    void OnRaw(TFuture<TYsonString> asyncStr) override;

private:
    IYsonConsumer* const UnderlyingConsumer_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

