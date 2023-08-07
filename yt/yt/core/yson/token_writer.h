#pragma once

#include "public.h"

#include "syntax_checker.h"

#include <yt/yt/core/misc/zerocopy_output_writer.h>

#include <util/stream/zerocopy_output.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TUncheckedYsonTokenWriter
{
public:
    explicit TUncheckedYsonTokenWriter(
        IZeroCopyOutput* output,
        EYsonType type = EYsonType::Node,
        int nestingLevelLimit = DefaultYsonParserNestingLevelLimit);
    explicit TUncheckedYsonTokenWriter(
        TZeroCopyOutputStreamWriter* writer,
        EYsonType type = EYsonType::Node,
        int nestingLevelLimit = DefaultYsonParserNestingLevelLimit);

    void WriteTextBoolean(bool value);
    void WriteTextInt64(i64 value);
    void WriteTextUint64(ui64 value);
    void WriteTextDouble(double value);
    void WriteTextString(TStringBuf value);

    Y_FORCE_INLINE void WriteBinaryBoolean(bool value);
    Y_FORCE_INLINE void WriteBinaryInt64(i64 value);
    Y_FORCE_INLINE void WriteBinaryUint64(ui64 value);
    Y_FORCE_INLINE void WriteBinaryDouble(double value);
    Y_FORCE_INLINE void WriteBinaryString(TStringBuf value);
    Y_FORCE_INLINE void WriteEntity();

    Y_FORCE_INLINE void WriteBeginMap();
    Y_FORCE_INLINE void WriteEndMap();
    Y_FORCE_INLINE void WriteBeginAttributes();
    Y_FORCE_INLINE void WriteEndAttributes();
    Y_FORCE_INLINE void WriteBeginList();
    Y_FORCE_INLINE void WriteEndList();

    Y_FORCE_INLINE void WriteItemSeparator();
    Y_FORCE_INLINE void WriteKeyValueSeparator();

    Y_FORCE_INLINE void WriteSpace(char value);

    void WriteRawNodeUnchecked(TStringBuf value);

    Y_FORCE_INLINE void Flush();
    Y_FORCE_INLINE void Finish();

    ui64 GetTotalWrittenSize() const;

private:
    template <typename T>
    Y_FORCE_INLINE void WriteSimple(T value);

    template <typename T>
    Y_FORCE_INLINE void WriteVarInt(T value);

private:
    std::optional<TZeroCopyOutputStreamWriter> WriterHolder_;
    TZeroCopyOutputStreamWriter* Writer_;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckedYsonTokenWriter
{
public:
    explicit TCheckedYsonTokenWriter(
        IZeroCopyOutput* writer,
        EYsonType type = EYsonType::Node,
        int nestingLevelLimit = DefaultYsonParserNestingLevelLimit);
    explicit TCheckedYsonTokenWriter(
        TZeroCopyOutputStreamWriter* writer,
        EYsonType type = EYsonType::Node,
        int nestingLevelLimit = DefaultYsonParserNestingLevelLimit);

    void WriteTextBoolean(bool value);
    void WriteBinaryBoolean(bool value);
    void WriteTextInt64(i64 value);
    void WriteBinaryInt64(i64 value);
    void WriteTextUint64(ui64 value);
    void WriteBinaryUint64(ui64 value);
    void WriteTextDouble(double value);
    void WriteBinaryDouble(double value);
    void WriteTextString(TStringBuf value);
    void WriteBinaryString(TStringBuf value);
    void WriteEntity();

    void WriteBeginMap();
    void WriteEndMap();
    void WriteBeginAttributes();
    void WriteEndAttributes();
    void WriteBeginList();
    void WriteEndList();

    void WriteItemSeparator();
    void WriteKeyValueSeparator();

    void WriteSpace(char value);

    void WriteRawNodeUnchecked(TStringBuf value);

    void Flush();
    void Finish();

private:
    NDetail::TYsonSyntaxChecker Checker_;
    TUncheckedYsonTokenWriter UncheckedWriter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define TOKEN_WRITER_INL_H_
#include "token_writer-inl.h"
#undef TOKEN_WRITER_INL_H_
