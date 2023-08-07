#pragma once

#include "string.h"

#include <yt/yt/core/yson/consumer.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TForwardingYsonConsumer
    : public virtual TYsonConsumerBase
{
public:
    // IYsonConsumer methods
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

    void OnRaw(TStringBuf yson, EYsonType type) override;

protected:
    void Forward(
        IYsonConsumer* consumer,
        std::function<void()> onFinished = nullptr,
        EYsonType type = EYsonType::Node);

    void Forward(
        std::vector<IYsonConsumer*> consumers,
        std::function<void()> onFinished = nullptr,
        EYsonType type = EYsonType::Node);

    virtual void OnMyStringScalar(TStringBuf value);
    virtual void OnMyInt64Scalar(i64 value);
    virtual void OnMyUint64Scalar(ui64 value);
    virtual void OnMyDoubleScalar(double value);
    virtual void OnMyBooleanScalar(bool value);
    virtual void OnMyEntity();

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList();

    virtual void OnMyBeginMap();
    virtual void OnMyKeyedItem(TStringBuf key);
    virtual void OnMyEndMap();

    virtual void OnMyBeginAttributes();
    virtual void OnMyEndAttributes();

    virtual void OnMyRaw(TStringBuf yson, EYsonType type);

private:
    std::vector<IYsonConsumer*> ForwardingConsumers_;
    int ForwardingDepth_ = 0;
    EYsonType ForwardingType_;
    std::function<void()> OnFinished_;

    bool CheckForwarding(int depthDelta = 0);
    void UpdateDepth(int depthDelta, bool checkFinish = true);
    void FinishForwarding();
};

////////////////////////////////////////////////////////////////////////////////

//! This is the most basic forwarding consumer which forwards all the events to given consumers.
class TTeeYsonConsumer
    : public TForwardingYsonConsumer
{
public:
    explicit TTeeYsonConsumer(
        std::vector<IYsonConsumer*> consumers,
        std::vector<std::unique_ptr<IYsonConsumer>> ownedConsumers = {},
        EYsonType type = EYsonType::Node);

private:
    std::vector<std::unique_ptr<IYsonConsumer>> OwnedConsumers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
