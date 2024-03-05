#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemaSerialization, ParseUsingNodeAndSerialize)
{
    const char* schemaString = "<strict=%true;unique_keys=%false>"
        "[{name=a;required=%false;type=int64;};{deleted=%true;stable_name=b}]";
    TTableSchema schema;
    Deserialize(schema, NYTree::ConvertToNode(NYson::TYsonString(TString(schemaString))));

    EXPECT_EQ(1, std::ssize(schema.Columns()));
    EXPECT_EQ("a", schema.Columns()[0].Name());
    EXPECT_EQ(1, std::ssize(schema.DeletedColumns()));
    EXPECT_EQ("b", schema.DeletedColumns()[0].StableName().Underlying());

    NYT::NFormats::TFormat format(NFormats::EFormatType::Json);

    TBlobOutput buffer;
    auto consumer = CreateConsumerForFormat(format, NFormats::EDataType::Structured, &buffer);

    Serialize(schema, consumer.get());
    consumer->Flush();
    auto ref = buffer.Flush();
    auto buf = ref.ToStringBuf();

    EXPECT_EQ(TString(buf.data(), buf.size()),
R"RR({"$attributes":{"strict":true,"unique_keys":false},"$value":[{"name":"a","required":false,"type":"int64","type_v3":{"type_name":"optional","item":"int64"}},{"stable_name":"b","deleted":true}]})RR");
}

TEST(TSchemaSerialization, Cursor)
{
    const char* schemaString = "<strict=%true;unique_keys=%false>"
        "[{name=a;required=%false;type=int64;};{deleted=%true;stable_name=b}]";

    TMemoryInput input(schemaString);
    NYson::TYsonPullParser parser(&input, NYson::EYsonType::Node);
    NYson::TYsonPullParserCursor cursor(&parser);

    TTableSchema schema;
    Deserialize(schema, &cursor);

    EXPECT_EQ(1, std::ssize(schema.Columns()));
    EXPECT_EQ("a", schema.Columns()[0].Name());
    EXPECT_EQ(1, std::ssize(schema.DeletedColumns()));
    EXPECT_EQ("b", schema.DeletedColumns()[0].StableName().Underlying());
}

TEST(TSchemaSerialization, Deleted)
{
    const char* schemaString = "<strict=%true;unique_keys=%false>"
        "[{name=a;required=%false;type=int64;};{deleted=%true;name=b}]";

    TTableSchema schema;
    EXPECT_THROW_WITH_SUBSTRING(
        Deserialize(schema, NYTree::ConvertToNode(NYson::TYsonString(TString(schemaString)))),
        "Stable name should be set for a deleted column");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
