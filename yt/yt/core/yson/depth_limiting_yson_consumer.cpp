#include "depth_limiting_yson_consumer.h"
#include "forwarding_consumer.h"
#include "null_consumer.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

static const TString TruncatedStubMessage("<truncated>");

////////////////////////////////////////////////////////////////////////////////

class TDepthLimitingYsonConsumer
    : public TForwardingYsonConsumer
{
public:
    TDepthLimitingYsonConsumer(
        IYsonConsumer* underlyingConsumer,
        int depthLimit,
        EYsonType ysonType)
        : UnderlyingConsumer_(underlyingConsumer)
        , DepthLimit_(depthLimit)
    {
        THROW_ERROR_EXCEPTION_IF(DepthLimit_ < 0,
            "YSON depth limit must be non-negative");

        if (DepthLimit_ == 0) {
            Forward(&NullConsumer_, /*onFinished*/ {}, ysonType);
        }
    }

    void OnMyStringScalar(TStringBuf value) override
    {
        UnderlyingConsumer_->OnStringScalar(value);
    }

    void OnMyInt64Scalar(i64 value) override
    {
        UnderlyingConsumer_->OnInt64Scalar(value);
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        UnderlyingConsumer_->OnUint64Scalar(value);
    }

    void OnMyDoubleScalar(double value) override
    {
        UnderlyingConsumer_->OnDoubleScalar(value);
    }

    void OnMyBooleanScalar(bool value) override
    {
        UnderlyingConsumer_->OnBooleanScalar(value);
    }

    void OnMyEntity() override
    {
        UnderlyingConsumer_->OnEntity();
    }

    void OnMyBeginList() override
    {
        ++Depth_;

        if (Depth_ < DepthLimit_) {
            UnderlyingConsumer_->OnBeginList();
        } else {
            UnderlyingConsumer_->OnStringScalar(TruncatedStubMessage);
            Forward(
                &NullConsumer_,
                /*onFinished*/ {},
                EYsonType::ListFragment);
        }
    }

    void OnMyListItem() override
    {
        UnderlyingConsumer_->OnListItem();
    }

    void OnMyEndList() override
    {
        if (Depth_ < DepthLimit_) {
            UnderlyingConsumer_->OnEndList();
        }

        --Depth_;
    }

    void OnMyBeginMap() override
    {
        ++Depth_;

        if (Depth_ < DepthLimit_) {
            UnderlyingConsumer_->OnBeginMap();
        } else {
            UnderlyingConsumer_->OnStringScalar(TruncatedStubMessage);
            Forward(
                &NullConsumer_,
                /*onFinished*/ {},
                EYsonType::MapFragment);
        }
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        UnderlyingConsumer_->OnKeyedItem(key);
    }

    void OnMyEndMap() override
    {
        if (Depth_ < DepthLimit_) {
            UnderlyingConsumer_->OnEndMap();
        }

        --Depth_;
    }

    void OnMyBeginAttributes() override
    {
        ++Depth_;

        if (Depth_ < DepthLimit_) {
            UnderlyingConsumer_->OnBeginAttributes();
        } else {
            Forward(
                &NullConsumer_,
                /*onFinished*/ {},
                EYsonType::MapFragment);
        }
    }

    void OnMyEndAttributes() override
    {
        if (Depth_ < DepthLimit_) {
            UnderlyingConsumer_->OnEndAttributes();
        }

        --Depth_;
    }

private:
    IYsonConsumer* const UnderlyingConsumer_;
    const int DepthLimit_;

    int Depth_ = 0;

    TNullYsonConsumer NullConsumer_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYsonConsumer> CreateDepthLimitingYsonConsumer(
    NYson::IYsonConsumer* underlyingConsumer,
    int depthLimit,
    EYsonType ysonType)
{
    return std::make_unique<TDepthLimitingYsonConsumer>(
        underlyingConsumer,
        depthLimit,
        ysonType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
