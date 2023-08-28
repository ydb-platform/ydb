#include "parser.h"
#include "consumer.h"
#include "format.h"
#include "parser_detail.h"

#include <yt/yt/core/actions/bind.h>

namespace NYT::NYson {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TYsonParser::TImpl
{
private:
    using TParserCoroutine = TCoroutine<int(const char* begin, const char* end, bool finish)>;

    TParserCoroutine ParserCoroutine_;
    TParserYsonStreamImpl<IYsonConsumer, TBlockReader<TParserCoroutine>> Parser_;

public:
    TImpl(IYsonConsumer* consumer, EYsonType parsingMode, TYsonParserConfig config)
        : ParserCoroutine_(BIND(
            [=, this, config = std::move(config)] (TParserCoroutine& self, const char* begin, const char* end, bool finish) {
                Parser_.DoParse(
                    TBlockReader<TParserCoroutine>(self, begin, end, finish),
                    consumer,
                    parsingMode,
                    config);
        }))
    { }

    void Read(const char* begin, const char* end, bool finish = false)
    {
        if (!ParserCoroutine_.IsCompleted()) {
            ParserCoroutine_.Run(begin, end, finish);
        } else {
            THROW_ERROR_EXCEPTION("Input is already parsed");
        }
    }

    void Read(TStringBuf data, bool finish = false)
    {
        Read(data.begin(), data.end(), finish);
    }

    void Finish()
    {
        Read(0, 0, true);
    }

    const char* GetCurrentPositionInBlock()
    {
        return Parser_.GetCurrentPositionInBlock();
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonParser::TYsonParser(IYsonConsumer* consumer, EYsonType type, TYsonParserConfig config)
    : Impl(std::make_unique<TImpl>(consumer, type, std::move(config)))
{ }

TYsonParser::~TYsonParser()
{ }

void TYsonParser::Read(const char* begin, const char* end, bool finish)
{
    Impl->Read(begin, end, finish);
}

void TYsonParser::Read(TStringBuf data)
{
    Impl->Read(data);
}

void TYsonParser::Finish()
{
    Impl->Finish();
}

const char* TYsonParser::GetCurrentPositionInBlock()
{
    return Impl->GetCurrentPositionInBlock();
}

////////////////////////////////////////////////////////////////////////////////

class TStatelessYsonParser::TImpl
{
private:
    const std::unique_ptr<TStatelessYsonParserImplBase> Impl;

public:
    TImpl(IYsonConsumer* consumer, TYsonParserConfig config)
        : Impl([&] () -> TStatelessYsonParserImplBase* {
            if (config.EnableContext && config.EnableLinePositionInfo) {
                return new TStatelessYsonParserImpl<IYsonConsumer, 64, true>(consumer, config.MemoryLimit, config.NestingLevelLimit);
            } else if (config.EnableContext && !config.EnableLinePositionInfo) {
                return new TStatelessYsonParserImpl<IYsonConsumer, 64, false>(consumer, config.MemoryLimit, config.NestingLevelLimit);
            } else if (!config.EnableContext && config.EnableLinePositionInfo) {
                return new TStatelessYsonParserImpl<IYsonConsumer, 0, true>(consumer, config.MemoryLimit, config.NestingLevelLimit);
            } else {
                return new TStatelessYsonParserImpl<IYsonConsumer, 0, false>(consumer, config.MemoryLimit, config.NestingLevelLimit);
            }
        }())
    { }

    void Parse(TStringBuf data, EYsonType type = EYsonType::Node)
    {
        Impl->Parse(data, type);
    }

    void Stop()
    {
        Impl->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

TStatelessYsonParser::TStatelessYsonParser(IYsonConsumer* consumer, TYsonParserConfig config)
    : Impl(std::make_unique<TImpl>(consumer, config))
{ }

TStatelessYsonParser::~TStatelessYsonParser()
{ }

void TStatelessYsonParser::Parse(TStringBuf data, EYsonType type)
{
    Impl->Parse(data, type);
}

void TStatelessYsonParser::Stop()
{
    Impl->Stop();
}

////////////////////////////////////////////////////////////////////////////////

void ParseYsonStringBuffer(TStringBuf buffer, EYsonType type, IYsonConsumer* consumer, TYsonParserConfig config)
{
    TParserYsonStreamImpl<IYsonConsumer, TStringReader> parser;
    TStringReader reader(buffer.begin(), buffer.end());
    parser.DoParse(reader, consumer, type, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
