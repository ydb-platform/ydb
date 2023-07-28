#pragma once

#include "forwarding_consumer.h"
#include "protobuf_interop_options.h"
#include "writer.h"

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/ypath/stack.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TForwardingUnknownYsonFieldValueWriter
    : public TForwardingYsonConsumer
{
public:
    TForwardingUnknownYsonFieldValueWriter(
        TBufferedBinaryYsonWriter& ysonWriter,
        const TProtobufWriterOptions::TUnknownYsonFieldModeResolver& modeResolver);

    DEFINE_BYREF_RW_PROPERTY(NYPath::TYPathStack, YPathStack);

    void Flush();
    void ResetMode();

private:
    const TProtobufWriterOptions::TUnknownYsonFieldModeResolver& ModeResolver_;

    TBufferedBinaryYsonWriter& YsonWriter_;

    EUnknownYsonFieldsMode LastMode_ = EUnknownYsonFieldsMode::Forward;

    void OnMyStringScalar(TStringBuf value) override;
    void OnMyInt64Scalar(i64 value) override;
    void OnMyUint64Scalar(ui64 value) override;
    void OnMyDoubleScalar(double value) override;
    void OnMyBooleanScalar(bool value) override;
    void OnMyEntity() override;
    void OnMyBeginList() override;
    void OnMyListItem() override;
    void OnMyEndList() override;
    void OnMyBeginMap() override;
    void OnMyKeyedItem(TStringBuf key) override;
    void OnMyEndMap() override;
    void OnMyBeginAttributes() override;
    void OnMyEndAttributes() override;

    void ValidateLeaf();
    void ValidateNode();

    void ThrowUnknownField();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
