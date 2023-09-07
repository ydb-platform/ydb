#include "yson_parser.h"

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/yson/parser.h>

namespace NYT::NFormats {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Wrapper around YSON parser that implements IParser interface.
class TYsonParserAdapter
    : public IParser
{
public:
    TYsonParserAdapter(
        IYsonConsumer* consumer,
        EYsonType type,
        bool enableLinePositionInfo)
        : Parser(consumer, type, {
            .EnableLinePositionInfo=enableLinePositionInfo,
            .MemoryLimit=NTableClient::MaxRowWeightLimit
        })
    { }

    void Read(TStringBuf data) override
    {
        Parser.Read(data);
    }

    void Finish() override
    {
        Parser.Finish();
    }

private:
    TYsonParser Parser;

};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYson(
    IYsonConsumer* consumer,
    EYsonType type,
    bool enableLinePositionInfo)
{
    return std::unique_ptr<IParser>(new TYsonParserAdapter(consumer, type, enableLinePositionInfo));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
