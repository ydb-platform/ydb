#pragma once

///
/// @file yt/cpp/mapreduce/interface/skiff_row.h
/// Header containing interfaces that you need to define for using TSkiffRowTableReader
/// What you need to do for your struct type TMyType:
/// 1. Write `true` specialization TIsSkiffRow<TMyType>;
/// 2. Write specialization GetSkiffSchema<TMyType>();
/// 3. Write your own parser derived from ISkiffRowParser and write specialization GetSkiffParser<TMyType>() which returns this parser.

#include "fwd.h"

#include <yt/cpp/mapreduce/skiff/skiff_schema.h>

#include <yt/cpp/mapreduce/interface/format.h>

#include <library/cpp/skiff/skiff.h>

#include <util/generic/maybe.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Need to write `true_type` specialization for your row type `T`.
/// And implement two functions: `GetSkiffSchema` and `CreateSkiffParser`.
///
/// Example:
///
/// template <>
/// struct TIsSkiffRow<T>
///     : std::true_type
/// { };
///
template<class T>
struct TIsSkiffRow
    : std::false_type
{ };

////////////////////////////////////////////////////////////////////////////////

//! Return skiff schema for row type `T`.
/// Need to write its specialization.
template <typename T>
NSkiff::TSkiffSchemaPtr GetSkiffSchema(const TMaybe<TSkiffRowHints>& /*hints*/)
{
    static_assert(TDependentFalse<T>, "Unimplemented `GetSkiffSchema` method");
}

////////////////////////////////////////////////////////////////////////////////

//! Allow to parse rows as user's structs from stream (TCheckedInDebugSkiffParser).
/// Need to write derived class for your own row type.
///
/// Example:
///
/// class TMySkiffRowParser : public ISkiffRowParser
/// {
/// public:
///    TMySkiffRowParser(TMySkiffRow* row)
///        : Row_(row)
///    {}
///
///    void Parse(NSkiff::TCheckedInDebugSkiffParser* parser)
/// .  {
///        Row_->SomeInt64Field = parser->ParseInt64();
///    }
///
/// private:
///    TMySkiffRow* Row_;
/// }
///
class ISkiffRowParser
    : public TThrRefBase
{
public:
    //! Read one row from parser
    virtual void Parse(NSkiff::TCheckedInDebugSkiffParser* /*parser*/) = 0;
};

//! Creates a parser for row type `T`.
template <typename T>
ISkiffRowParserPtr CreateSkiffParser(T* /*row*/, const TMaybe<TSkiffRowHints>& /*hints*/)
{
    static_assert(TDependentFalse<T>, "Unimplemented `CreateSkiffParser` function");
}

////////////////////////////////////////////////////////////////////////////////

//! Allow to skip row content without getting row.
/// By default row will be parsed using your parser derived from ISkiffRowParser.
/// If you want, you can write more optimal skipper, but it isn't required.
class ISkiffRowSkipper
    : public TThrRefBase
{
public:
    virtual void SkipRow(NSkiff::TCheckedInDebugSkiffParser* /*parser*/) = 0;
};

//! Default ISkiffRowSkipper implementation.
template <typename T>
class TSkiffRowSkipper : public ISkiffRowSkipper {
public:
    explicit TSkiffRowSkipper(const TMaybe<TSkiffRowHints>& hints)
        : Parser_(CreateSkiffParser<T>(&Row_, hints))
    { }

    void SkipRow(NSkiff::TCheckedInDebugSkiffParser* parser) {
        Parser_->Parse(parser);
    }

private:
    T Row_;
    ISkiffRowParserPtr Parser_;
};

//! Creates a skipper for row type 'T'.
/// You don't need to write its specialization.
template <typename T>
ISkiffRowSkipperPtr CreateSkiffSkipper(const TMaybe<TSkiffRowHints>& hints)
{
    return ::MakeIntrusive<TSkiffRowSkipper<T>>(hints);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
