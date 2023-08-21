#include <yt/yt/client/table_client/column_sort_schema.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TColumnSortSchemaTest, Serialize)
{
    auto test = [] (TString name, ESortOrder sortOrder, TString expected) {
        TColumnSortSchema schema{
            .Name = name,
            .SortOrder = sortOrder
        };
        auto serialized = ConvertToYsonString(schema, NYson::EYsonFormat::Text).ToString();
        EXPECT_EQ(serialized, expected);
    };

    test("foo", ESortOrder::Ascending, "\"foo\"");
    test("bar", ESortOrder::Descending, "{\"name\"=\"bar\";\"sort_order\"=\"descending\";}");
}

TEST(TColumnSortSchemaTest, Deserialize)
{
    auto test = [] (TString serialized, TString name, ESortOrder sortOrder) {
        auto schema = ConvertTo<TColumnSortSchema>(TYsonString(serialized));
        EXPECT_EQ(schema.Name, name);
        EXPECT_EQ(schema.SortOrder, sortOrder);
    };

    test("{\"name\"=\"foo\";\"sort_order\"=\"ascending\"}", "foo", ESortOrder::Ascending);
    test("{\"name\"=\"bar\";\"sort_order\"=\"descending\"}", "bar", ESortOrder::Descending);
    test("\"baz\"", "baz", ESortOrder::Ascending);
    EXPECT_THROW_WITH_SUBSTRING(test("42", "foo", ESortOrder::Ascending), "Unexpected type of column sort schema node");
}

TEST(TColumnSortSchemaTest, ProtobufConversion)
{
    TSortColumns sortColumns;
    sortColumns.push_back(TColumnSortSchema{
        .Name = "foo",
        .SortOrder = ESortOrder::Ascending
    });
    sortColumns.push_back(TColumnSortSchema{
        .Name = "bar",
        .SortOrder = ESortOrder::Descending
    });

    NProto::TSortColumnsExt sortColumnsExt;
    ToProto(&sortColumnsExt, sortColumns);

    TSortColumns parsedSortColumns;
    FromProto(&parsedSortColumns, sortColumnsExt);

    EXPECT_EQ(sortColumns, parsedSortColumns);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
