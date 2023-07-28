#include "protobuf_interop_unknown_fields.h"
#include "null_consumer.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TForwardingUnknownYsonFieldValueWriter::TForwardingUnknownYsonFieldValueWriter(
    TBufferedBinaryYsonWriter& ysonWriter,
    const TProtobufWriterOptions::TUnknownYsonFieldModeResolver& modeResolver)
    : ModeResolver_(modeResolver)
    , YsonWriter_(ysonWriter)
{
    YT_VERIFY(ModeResolver_);
}

void TForwardingUnknownYsonFieldValueWriter::OnMyStringScalar(TStringBuf value)
{
    ValidateLeaf();
    YsonWriter_.OnStringScalar(value);
}

void TForwardingUnknownYsonFieldValueWriter::OnMyInt64Scalar(i64 value)
{
    ValidateLeaf();
    YsonWriter_.OnInt64Scalar(value);
}

void TForwardingUnknownYsonFieldValueWriter::OnMyUint64Scalar(ui64 value)
{
    ValidateLeaf();
    YsonWriter_.OnUint64Scalar(value);
}

void TForwardingUnknownYsonFieldValueWriter::OnMyDoubleScalar(double value)
{
    ValidateLeaf();
    YsonWriter_.OnDoubleScalar(value);
}

void TForwardingUnknownYsonFieldValueWriter::OnMyBooleanScalar(bool value)
{
    ValidateLeaf();
    YsonWriter_.OnBooleanScalar(value);
}

void TForwardingUnknownYsonFieldValueWriter::OnMyEntity()
{
    ValidateLeaf();
    YsonWriter_.OnEntity();
}

void TForwardingUnknownYsonFieldValueWriter::OnMyBeginList()
{
    YsonWriter_.OnBeginList();
    YPathStack_.Push(-1);
}

void TForwardingUnknownYsonFieldValueWriter::OnMyListItem()
{
    YPathStack_.IncreaseLastIndex();
    LastMode_ = ModeResolver_(YPathStack_.GetPath());
    switch (LastMode_) {
        case EUnknownYsonFieldsMode::Skip: {
            Forward(GetNullYsonConsumer(), [] {});
            return;
        }
        case EUnknownYsonFieldsMode::Keep: {
            YsonWriter_.OnListItem();
            Forward(&YsonWriter_, [] {});
            return;
        }
        case EUnknownYsonFieldsMode::Forward: {
            YsonWriter_.OnListItem();
            break;
        }
        default: {
            ValidateNode();
        }
    }
}

void TForwardingUnknownYsonFieldValueWriter::OnMyEndList()
{
    YPathStack_.Pop();
    YsonWriter_.OnEndList();
}

void TForwardingUnknownYsonFieldValueWriter::OnMyBeginMap()
{
    YsonWriter_.OnBeginMap();
    // Needed for invariant that previous map key is popped in OnMyKeyedItem.
    YPathStack_.Push("");
}

void TForwardingUnknownYsonFieldValueWriter::OnMyKeyedItem(TStringBuf key)
{
    YPathStack_.Pop();
    YPathStack_.Push(TString{key});
    LastMode_ = ModeResolver_(YPathStack_.GetPath());
    switch (LastMode_) {
        case EUnknownYsonFieldsMode::Skip: {
            Forward(GetNullYsonConsumer(), [] {});
            return;
        }
        case EUnknownYsonFieldsMode::Keep: {
            YsonWriter_.OnKeyedItem(key);
            Forward(&YsonWriter_, [] {});
            return;
        }
        case EUnknownYsonFieldsMode::Forward: {
            YsonWriter_.OnKeyedItem(key);
            break;
        }
        case EUnknownYsonFieldsMode::Fail: {
            ThrowUnknownField();
            break;
        }
        default: {
            YT_ABORT();
        }
    }
}

void TForwardingUnknownYsonFieldValueWriter::OnMyEndMap()
{
    YPathStack_.Pop();
    YsonWriter_.OnEndMap();
}

void TForwardingUnknownYsonFieldValueWriter::OnMyBeginAttributes()
{
    YsonWriter_.OnBeginAttributes();
    Forward(&YsonWriter_, [] {});
}

void TForwardingUnknownYsonFieldValueWriter::OnMyEndAttributes()
{
    YsonWriter_.OnEndAttributes();
}

void TForwardingUnknownYsonFieldValueWriter::ValidateLeaf()
{
    if (LastMode_ != EUnknownYsonFieldsMode::Keep) {
        ThrowUnknownField();
    }
}

void TForwardingUnknownYsonFieldValueWriter::ValidateNode()
{
    if (LastMode_ == EUnknownYsonFieldsMode::Fail) {
        ThrowUnknownField();
    }
}

void TForwardingUnknownYsonFieldValueWriter::Flush()
{
    YsonWriter_.Flush();
}

void TForwardingUnknownYsonFieldValueWriter::ResetMode()
{
    LastMode_ = EUnknownYsonFieldsMode::Forward;
}

void TForwardingUnknownYsonFieldValueWriter::ThrowUnknownField()
{
    auto lastElement = YPathStack_.TryGetStringifiedLastPathToken().value_or(TString{});
    auto path = YPathStack_.GetPath();
    YPathStack_.Pop();
    THROW_ERROR_EXCEPTION("Unknown field %Qv at %v",
        lastElement,
        YPathStack_.GetHumanReadablePath())
        << TErrorAttribute("ypath", path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
