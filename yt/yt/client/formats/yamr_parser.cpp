#include "yamr_parser.h"
#include "yamr_parser_base.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TYamrParserConsumer
    : public TYamrConsumerBase
{
public:
    TYamrParserConsumer(NYson::IYsonConsumer* consumer, TYamrFormatConfigPtr config)
        : TYamrConsumerBase(consumer)
        , Config(config)
    { }

    void ConsumeKey(TStringBuf key) override
    {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        Consumer->OnKeyedItem(Config->Key);
        Consumer->OnStringScalar(key);
    }

    void ConsumeSubkey(TStringBuf subkey) override
    {
        Consumer->OnKeyedItem(Config->Subkey);
        Consumer->OnStringScalar(subkey);
    }

    void ConsumeValue(TStringBuf value) override
    {
        Consumer->OnKeyedItem(Config->Value);
        Consumer->OnStringScalar(value);
        Consumer->OnEndMap();
    }

private:
    TYamrFormatConfigPtr Config;

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamr(
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    if (!config) {
        config = New<TYamrFormatConfig>();
    }

    auto parserConsumer = New<TYamrParserConsumer>(consumer, config);

    return config->Lenval
        ? std::unique_ptr<IParser>(
            new TYamrLenvalBaseParser(
                parserConsumer,
                config->HasSubkey,
                config->EnableEom))
        : std::unique_ptr<IParser>(
            new TYamrDelimitedBaseParser(
                parserConsumer,
                config,
                config->EnableEscaping /* enableKeyEscaping */,
                config->EnableEscaping /* enableValueEscaping */));
}

////////////////////////////////////////////////////////////////////////////////

void ParseYamr(
    IInputStream* input,
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    auto parser = CreateParserForYamr(consumer, config);
    Parse(input, parser.get());
}

void ParseYamr(
    TStringBuf data,
    NYson::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{
    auto parser = CreateParserForYamr(consumer, config);
    parser->Read(data);
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
