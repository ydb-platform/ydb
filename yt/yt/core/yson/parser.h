#pragma once

#include "public.h"
#include "detail.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! @brief Configuration of yson parser.
struct TYsonParserConfig
{
    //! @brief Enable info about line in error messages
    //!
    //! Makes error messages friendlier but slows down parsing.
    bool EnableLinePositionInfo = false;

    i64 MemoryLimit = std::numeric_limits<i64>::max();

    //! @brief Enable context dumping in error messages
    //!
    //! Makes error messages friendlier but slightly slows down parsing.
    bool EnableContext = true;

    //! @brief Maximum level of nesting of lists and maps.
    int NestingLevelLimit = DefaultYsonParserNestingLevelLimit;
};

class TYsonParser
{
public:
    TYsonParser(IYsonConsumer* consumer, EYsonType type = EYsonType::Node, TYsonParserConfig config = {});
    ~TYsonParser();

    void Read(const char* begin, const char* end, bool finish = false);
    void Read(TStringBuf data);
    void Finish();

    const char* GetCurrentPositionInBlock();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

class TStatelessYsonParser
{
public:
    TStatelessYsonParser(IYsonConsumer* consumer, TYsonParserConfig config = {});
    ~TStatelessYsonParser();

    void Parse(TStringBuf data, EYsonType type = EYsonType::Node);
    void Stop();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

void ParseYsonStringBuffer(TStringBuf buffer, EYsonType type, IYsonConsumer* consumer, TYsonParserConfig config = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
