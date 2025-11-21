#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NJson {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TJsonWriterTest, Basic)
{
    auto getWrittenJson = [] (bool pretty) {
        TStringStream outputStream;
        auto writer = CreateJsonWriter(&outputStream, pretty);

        BuildYsonFluently(writer.get())
            .BeginList()
                .Item().Value(123)
                .Item().Value(-56)
                .Item()
                .BeginMap()
                    .Item("key").Value(true)
                    .Item("entity").Entity()
                    .Item("value").Value(4.25)
                .EndMap()
                .Item().Value(std::numeric_limits<double>::infinity())
            .EndList();
        writer->Flush();
        return outputStream.Str();
    };

    EXPECT_EQ(getWrittenJson(false), R"([123,-56,{"key":true,"entity":null,"value":4.25},inf])");
    EXPECT_EQ(Strip(getWrittenJson(true)),
        "[\n"
        "    123,\n"
        "    -56,\n"
        "    {\n"
        "        \"key\": true,\n"
        "        \"entity\": null,\n"
        "        \"value\": 4.25\n"
        "    },\n"
        "    inf\n"
        "]");
}

TEST(TJsonWriterTest, StartNextValue)
{
    TStringStream outputStream;
    {
        auto writer = CreateJsonWriter(&outputStream);

        BuildYsonFluently(writer.get())
            .BeginList()
                .Item().Value(123)
                .Item().Value("hello")
            .EndList();

        writer->StartNextValue();

        BuildYsonFluently(writer.get())
            .BeginMap()
                .Item("abc").Value(true)
                .Item("def").Value(-1.5)
            .EndMap();

        writer->Flush();
    }

    EXPECT_EQ(outputStream.Str(), "[123,\"hello\"]\n{\"abc\":true,\"def\":-1.5}");
}

TEST(TJsonWriterTest, Errors)
{
    TStringStream outputStream;

    {
        auto writer = CreateJsonWriter(&outputStream);
        // Non-UTF-8.
        EXPECT_THROW(writer->OnStringScalar("\xFF\xFE\xFC"), TErrorException);
    }
    {
        auto writer = CreateJsonWriter(&outputStream);
        EXPECT_THROW(writer->OnBeginAttributes(), TErrorException);
    }
    {
        auto writer = CreateJsonWriter(&outputStream);
        EXPECT_THROW(writer->OnRaw("{x=3}", EYsonType::Node), TErrorException);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJson
