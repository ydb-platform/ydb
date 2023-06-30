#include "token_writer.h"

#include <cmath>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

namespace {

size_t FloatToStringWithNanInf(double value, char* buf, size_t size)
{
    if (std::isfinite(value)) {
        return FloatToString(value, buf, size);
    }

    static const TStringBuf nanLiteral = "%nan";
    static const TStringBuf infLiteral = "%inf";
    static const TStringBuf negativeInfLiteral = "%-inf";

    TStringBuf str;
    if (std::isnan(value)) {
        str = nanLiteral;
    } else if (std::isinf(value) && value > 0) {
        str = infLiteral;
    } else {
        str = negativeInfLiteral;
    }
    YT_VERIFY(str.size() + 1 <= size);
    ::memcpy(buf, str.data(), str.size() + 1);
    return str.size();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TUncheckedYsonTokenWriter::TUncheckedYsonTokenWriter(
    IZeroCopyOutput* output,
    EYsonType /*type*/,
    int /*nestingLevelLimit*/)
    : WriterHolder_(output)
    , Writer_(&*WriterHolder_)
{ }

TUncheckedYsonTokenWriter::TUncheckedYsonTokenWriter(
    TZeroCopyOutputStreamWriter* writer,
    EYsonType /*type*/,
    int /*nestingLevelLimit*/)
    : Writer_(writer)
{ }

void TUncheckedYsonTokenWriter::WriteTextBoolean(bool value)
{
    auto res = value ? TStringBuf("%true") : TStringBuf("%false");
    Writer_->Write(res.data(), res.size());
}

void TUncheckedYsonTokenWriter::WriteTextInt64(i64 value)
{
    auto res = ::ToString(value);
    Writer_->Write(res.data(), res.size());
}

void TUncheckedYsonTokenWriter::WriteTextUint64(ui64 value)
{
    auto res = ::ToString(value);
    Writer_->Write(res.data(), res.size());
    WriteSimple('u');
}

void TUncheckedYsonTokenWriter::WriteTextDouble(double value)
{
    char buf[256];
    auto str = TStringBuf(buf, FloatToStringWithNanInf(value, buf, sizeof(buf)));
    Writer_->Write(str.data(), str.size());
    if (str.find('.') == TString::npos && str.find('e') == TString::npos && std::isfinite(value)) {
        WriteSimple('.');
    }
}

void TUncheckedYsonTokenWriter::WriteTextString(TStringBuf value)
{
    WriteSimple('"');
    auto res = EscapeC(value.data(), value.length());
    Writer_->Write(res.data(), res.length());
    WriteSimple('"');
}

void TUncheckedYsonTokenWriter::WriteRawNodeUnchecked(TStringBuf value)
{
    Writer_->Write(value.data(), value.size());
}

ui64 TUncheckedYsonTokenWriter::GetTotalWrittenSize() const
{
    return Writer_->GetTotalWrittenSize();
}

////////////////////////////////////////////////////////////////////////////////

TCheckedYsonTokenWriter::TCheckedYsonTokenWriter(IZeroCopyOutput* writer, EYsonType type, int nestingLevelLimit)
    : Checker_(type, nestingLevelLimit)
    , UncheckedWriter_(writer, type)
{ }

TCheckedYsonTokenWriter::TCheckedYsonTokenWriter(TZeroCopyOutputStreamWriter* writer, EYsonType type, int nestingLevelLimit)
    : Checker_(type, nestingLevelLimit)
    , UncheckedWriter_(writer, type)
{ }

void TCheckedYsonTokenWriter::Flush()
{
    UncheckedWriter_.Flush();
}

void TCheckedYsonTokenWriter::WriteTextBoolean(bool value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
    UncheckedWriter_.WriteTextBoolean(value);
}

void TCheckedYsonTokenWriter::WriteBinaryBoolean(bool value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
    UncheckedWriter_.WriteBinaryBoolean(value);
}

void TCheckedYsonTokenWriter::WriteTextInt64(i64 value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::Int64Value);
    UncheckedWriter_.WriteTextInt64(value);
}

void TCheckedYsonTokenWriter::WriteBinaryInt64(i64 value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::Int64Value);
    UncheckedWriter_.WriteBinaryInt64(value);
}

void TCheckedYsonTokenWriter::WriteTextUint64(ui64 value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::Uint64Value);
    UncheckedWriter_.WriteTextUint64(value);
}

void TCheckedYsonTokenWriter::WriteBinaryUint64(ui64 value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::Uint64Value);
    UncheckedWriter_.WriteBinaryUint64(value);
}

void TCheckedYsonTokenWriter::WriteTextDouble(double value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
    UncheckedWriter_.WriteTextDouble(value);
}

void TCheckedYsonTokenWriter::WriteBinaryDouble(double value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
    UncheckedWriter_.WriteBinaryDouble(value);
}

void TCheckedYsonTokenWriter::WriteTextString(TStringBuf value)
{
    Checker_.OnString();
    UncheckedWriter_.WriteTextString(value);
}

void TCheckedYsonTokenWriter::WriteBinaryString(TStringBuf value)
{
    Checker_.OnString();
    UncheckedWriter_.WriteBinaryString(value);
}

void TCheckedYsonTokenWriter::WriteEntity()
{
    Checker_.OnSimpleNonstring(EYsonItemType::EntityValue);
    UncheckedWriter_.WriteEntity();
}

void TCheckedYsonTokenWriter::WriteBeginMap()
{
    Checker_.OnBeginMap();
    UncheckedWriter_.WriteBeginMap();
}

void TCheckedYsonTokenWriter::WriteEndMap()
{
    Checker_.OnEndMap();
    UncheckedWriter_.WriteEndMap();
}

void TCheckedYsonTokenWriter::WriteBeginAttributes()
{
    Checker_.OnAttributesBegin();
    UncheckedWriter_.WriteBeginAttributes();
}

void TCheckedYsonTokenWriter::WriteEndAttributes()
{
    Checker_.OnAttributesEnd();
    UncheckedWriter_.WriteEndAttributes();
}

void TCheckedYsonTokenWriter::WriteBeginList()
{
    Checker_.OnBeginList();
    UncheckedWriter_.WriteBeginList();
}

void TCheckedYsonTokenWriter::WriteEndList()
{
    Checker_.OnEndList();
    UncheckedWriter_.WriteEndList();
}

void TCheckedYsonTokenWriter::WriteItemSeparator()
{
    Checker_.OnSeparator();
    UncheckedWriter_.WriteItemSeparator();
}

void TCheckedYsonTokenWriter::WriteKeyValueSeparator()
{
    Checker_.OnEquality();
    UncheckedWriter_.WriteKeyValueSeparator();
}

void TCheckedYsonTokenWriter::WriteSpace(char value)
{
    UncheckedWriter_.WriteSpace(value);
}

void TCheckedYsonTokenWriter::Finish()
{
    Checker_.OnFinish();
    UncheckedWriter_.Finish();
}

void TCheckedYsonTokenWriter::WriteRawNodeUnchecked(TStringBuf value)
{
    Checker_.OnSimpleNonstring(EYsonItemType::EntityValue);
    UncheckedWriter_.WriteRawNodeUnchecked(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
