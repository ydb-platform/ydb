#include "yamred_dsv_parser.h"
#include "dsv_parser.h"
#include "yamr_parser_base.h"

namespace NYT::NFormats {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TYamredDsvParserConsumer
    : public TYamrConsumerBase
{
public:
    TYamredDsvParserConsumer(IYsonConsumer* consumer, TYamredDsvFormatConfigPtr config)
        : TYamrConsumerBase(consumer)
        , Config(config)
        , DsvParser(CreateParserForDsv(consumer, ConvertTo<TDsvFormatConfigPtr>(Config), /*wrapWithMap*/ false))
    { }

    void ConsumeKey(TStringBuf key) override
    {
        Consumer->OnListItem();
        Consumer->OnBeginMap();
        ConsumeFields(Config->KeyColumnNames, key);
    }

    void ConsumeSubkey(TStringBuf subkey) override
    {
        ConsumeFields(Config->SubkeyColumnNames, subkey);
    }

    void ConsumeValue(TStringBuf value) override
    {
        DsvParser->Read(value);
        DsvParser->Finish();
        Consumer->OnEndMap();
    }

private:
    TYamredDsvFormatConfigPtr Config;
    std::unique_ptr<IParser> DsvParser;

    void ConsumeFields(
        const std::vector<TString>& fieldNames,
        TStringBuf wholeField)
    {
        static const char* emptyString = "";
        char delimiter = Config->YamrKeysSeparator;

        std::vector<TStringBuf> fields;
        if (wholeField.length() == 0) {
            fields = std::vector<TStringBuf>(1, TStringBuf(emptyString));
        } else {
            size_t position = 0;
            while (position != TStringBuf::npos) {
                size_t newPosition = (fields.size() + 1 == fieldNames.size())
                    ? TStringBuf::npos
                    : wholeField.find(delimiter, position);
                fields.push_back(wholeField.substr(position, newPosition));
                position = (newPosition == TStringBuf::npos) ? TStringBuf::npos : newPosition + 1;
            }
        }

        if (fields.size() != fieldNames.size()) {
            THROW_ERROR_EXCEPTION("Invalid number of key fields in YAMRed DSV: expected %v, actual %v",
                fieldNames.size(),
                fields.size());
        }

        for (int i = 0; i < static_cast<int>(fields.size()); ++i) {
            Consumer->OnKeyedItem(fieldNames[i]);
            Consumer->OnStringScalar(fields[i]);
        }
    }

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYamredDsv(
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parserConsumer = New<TYamredDsvParserConsumer>(consumer, config);

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
                false /* enableValueEscaping */));
}

////////////////////////////////////////////////////////////////////////////////

void ParseYamredDsv(
    IInputStream* input,
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parser = CreateParserForYamredDsv(consumer, config);
    Parse(input, parser.get());
}

void ParseYamredDsv(
    TStringBuf data,
    IYsonConsumer* consumer,
    TYamredDsvFormatConfigPtr config)
{
    auto parser = CreateParserForYamredDsv(consumer, config);
    parser->Read(data);
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

