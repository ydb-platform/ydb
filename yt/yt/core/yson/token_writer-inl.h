#ifndef TOKEN_WRITER_INL_H_
#error "Direct inclusion of this file is not allowed, include token_writer.h"
// For the sake of sane code completion.
#include "token_writer.h"
#endif

#include "detail.h"

#include <library/cpp/yt/coding/varint.h>

#include <util/generic/typetraits.h>
#include <util/string/escape.h>

#include <cctype>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void TUncheckedYsonTokenWriter::Flush()
{
    if (Y_LIKELY(Writer_->RemainingBytes() > 0)) {
        Writer_->UndoRemaining();
    }
}

template <typename T>
void TUncheckedYsonTokenWriter::WriteSimple(T value)
{
    Writer_->Write(&value, sizeof(value));
}

template <typename T>
void TUncheckedYsonTokenWriter::WriteVarInt(T value)
{
    NYT::WriteVarInt(Writer_, value);
}

void TUncheckedYsonTokenWriter::WriteBinaryBoolean(bool value)
{
    WriteSimple(value ? NDetail::TrueMarker : NDetail::FalseMarker);
}

void TUncheckedYsonTokenWriter::WriteBinaryInt64(i64 value)
{
    WriteSimple(NDetail::Int64Marker);
    WriteVarInt<i64>(value);
}

void TUncheckedYsonTokenWriter::WriteBinaryUint64(ui64 value)
{
    WriteSimple(NDetail::Uint64Marker);
    WriteVarInt<ui64>(value);
}

void TUncheckedYsonTokenWriter::WriteBinaryDouble(double value)
{
    WriteSimple(NDetail::DoubleMarker);
    WriteSimple(value);
}

void TUncheckedYsonTokenWriter::WriteBinaryString(TStringBuf value)
{
    WriteSimple(NDetail::StringMarker);
    WriteVarInt<i32>(value.length());
    Writer_->Write(value.begin(), value.size());
}

void TUncheckedYsonTokenWriter::WriteEntity()
{
    WriteSimple(NDetail::EntitySymbol);
}

void TUncheckedYsonTokenWriter::WriteBeginMap()
{
    WriteSimple(NDetail::BeginMapSymbol);
}

void TUncheckedYsonTokenWriter::WriteEndMap()
{
    WriteSimple(NDetail::EndMapSymbol);
}

void TUncheckedYsonTokenWriter::WriteBeginAttributes()
{
    WriteSimple(NDetail::BeginAttributesSymbol);
}

void TUncheckedYsonTokenWriter::WriteEndAttributes()
{
    WriteSimple(NDetail::EndAttributesSymbol);
}

void TUncheckedYsonTokenWriter::WriteBeginList()
{
    WriteSimple(NDetail::BeginListSymbol);
}

void TUncheckedYsonTokenWriter::WriteEndList()
{
    WriteSimple(NDetail::EndListSymbol);
}

void TUncheckedYsonTokenWriter::WriteItemSeparator()
{
    WriteSimple(NDetail::ItemSeparatorSymbol);
}

void TUncheckedYsonTokenWriter::WriteKeyValueSeparator()
{
    WriteSimple(NDetail::KeyValueSeparatorSymbol);
}

void TUncheckedYsonTokenWriter::WriteSpace(char value)
{
    YT_ASSERT(std::isspace(value));
    WriteSimple(value);
}

void TUncheckedYsonTokenWriter::Finish()
{
    Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
