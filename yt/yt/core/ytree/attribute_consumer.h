#pragma once

#include "public.h"

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/forwarding_consumer.h>
#include <yt/yt/core/yson/stream.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TAttributeConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    explicit TAttributeConsumer(IAttributeDictionary* attributes, std::optional<THashSet<TString>> keyWhitelist = {});
    IAttributeDictionary* GetAttributes() const;

protected:
    void OnMyStringScalar(TStringBuf value) override;
    void OnMyInt64Scalar(i64 value) override;
    void OnMyUint64Scalar(ui64 value) override;
    void OnMyDoubleScalar(double value) override;
    void OnMyBooleanScalar(bool value) override;
    void OnMyEntity() override;
    void OnMyBeginList() override;

    void OnMyKeyedItem(TStringBuf key) override;
    void OnMyBeginMap() override;
    void OnMyEndMap() override;
    void OnMyBeginAttributes() override;
    void OnMyEndAttributes()override;

private:
    IAttributeDictionary* const Attributes_;

    const std::optional<THashSet<TString>> KeyWhitelist_;

    TStringStream Output_;
    std::unique_ptr<NYson::TBufferedBinaryYsonWriter> Writer_;

    void ThrowMapExpected();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
