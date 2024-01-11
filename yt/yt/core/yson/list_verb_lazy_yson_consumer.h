#include "public.h"

#include <yt/yt/core/yson/forwarding_consumer.h>
#include <yt/yt/core/yson/null_consumer.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TListVerbLazyYsonConsumer
    : public TForwardingYsonConsumer
{
public:
    explicit TListVerbLazyYsonConsumer(
        IYsonConsumer* underlyingConsumer,
        std::optional<int> limit = std::nullopt);

    void OnMyBeginMap() override;
    void OnMyKeyedItem(TStringBuf key) override;
    void OnMyEndMap() override;

    void OnMyBeginAttributes() override;
    void OnMyEndAttributes() override;

    void OnMyBeginList() override;
    void OnMyListItem() override;
    void OnMyEndList() override;

    void OnMyStringScalar(TStringBuf value) override;
    void OnMyInt64Scalar(i64 value) override;
    void OnMyUint64Scalar(ui64 value) override;
    void OnMyDoubleScalar(double value) override;
    void OnMyBooleanScalar(bool value) override;
    void OnMyEntity() override;

private:
    IYsonConsumer* UnderlyingConsumer_;
    const std::optional<int> Limit_;
    int Index_ = 0;

    void ThrowUnexpectedToken(TStringBuf unexpectedToken);
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
