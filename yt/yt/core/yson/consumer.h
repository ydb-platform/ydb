#pragma once

#include <library/cpp/yt/yson_string/public.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Some consumers act like buffered streams and thus must be flushed at the end.
struct IFlushableYsonConsumer
    : public virtual IYsonConsumer
{
    virtual void Flush() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! A base class for most YSON consumers.
class TYsonConsumerBase
    : public virtual IYsonConsumer
{
public:
    //! Parses #str and converts it into a sequence of elementary calls.
    void OnRaw(TStringBuf str, EYsonType type) override;
    using IYsonConsumer::OnRaw;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
