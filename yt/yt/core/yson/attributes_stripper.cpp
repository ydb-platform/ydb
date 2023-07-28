#include "attributes_stripper.h"
#include "string.h"
#include "writer.h"
#include "parser.h"

#include <library/cpp/yson/consumer.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TYsonAttributesStripper
    : public IYsonConsumer
{
public:
    explicit TYsonAttributesStripper(IYsonConsumer* output)
        : Output_(output)
    { }

    void OnStringScalar(TStringBuf value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnStringScalar(value);
        }
    }

    void OnInt64Scalar(i64 value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnInt64Scalar(value);
        }
    }

    void OnUint64Scalar(ui64 value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnInt64Scalar(value);
        }
    }

    void OnDoubleScalar(double value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnDoubleScalar(value);
        }
    }

    void OnBooleanScalar(bool value) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBooleanScalar(value);
        }
    }

    void OnEntity() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEntity();
        }
    }

    void OnBeginList() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBeginList();
        }
    }

    void OnListItem() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnListItem();
        }
    }

    void OnEndList() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEndList();
        }
    }

    void OnBeginMap() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnBeginMap();
        }
    }

    void OnKeyedItem(TStringBuf key) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnKeyedItem(key);
        }
    }

    void OnEndMap() override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnEndMap();
        }
    }

    void OnBeginAttributes() override
    {
        ++AttributesDepth_;
    }

    void OnEndAttributes() override
    {
        --AttributesDepth_;
    }

    void OnRaw(TStringBuf yson, EYsonType type) override
    {
        if (AttributesDepth_ == 0) {
            Output_->OnRaw(yson, type);
        }
    }

private:
    IYsonConsumer* const Output_;
    int AttributesDepth_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TYsonString StripAttributes(const TYsonString& yson)
{
    TStringStream outputStream;
    TYsonWriter writer(&outputStream);
    TYsonAttributesStripper stripper(&writer);
    ParseYsonStringBuffer(yson.AsStringBuf(), yson.GetType(), &stripper);
    return TYsonString(outputStream.Str(), yson.GetType());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
