#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yaml/writer.h>

#include <yt/yt/core/yaml/config.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/string/stream.h>

namespace NYT::NYaml {
namespace {

using namespace NYson;
using namespace NYTree;

//////////////////////////////////////////////////////////////////////////////

std::string YsonToYaml(const TYsonString& yson, const TYsonString& formatAttributes = TYsonString(TStringBuf("{}")))
{
    TStdStringStream outputStream;
    auto config = ConvertTo<TYamlFormatConfigPtr>(formatAttributes);
    auto writer = CreateYamlWriter(&outputStream, yson.GetType(), config);
    Serialize(yson, writer.get());
    writer->Flush();
    return outputStream.Str();
}

//////////////////////////////////////////////////////////////////////////////

TEST(TYamlWriterTest, Simple)
{
    std::string yson = "hello";
    // Here and in the rest of the tests we introduce an extra leading \n for the better readabilty, which we later
    // strip off in the comparison.
    std::string expectedYaml = R"(
hello
)";

    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, IntegersWithoutUintTag)
{
    std::string yson = "{a=1; b=1u; c=-1; d=9223372036854775808u; e=-9223372036854775808; f=18446744073709551615u}";
    std::string expectedYaml = R"(
a: 1
b: 1
c: -1
d: 9223372036854775808
e: -9223372036854775808
f: 18446744073709551615
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, IntegersWithUintTag)
{
    std::string formatAttributes = "{write_uint_tag=%true}";
    std::string yson = "{a=1; b=1u; c=-1; d=9223372036854775808u; e=-9223372036854775808; f=18446744073709551615u}";
    std::string expectedYaml = R"(
a: 1
b: !yt/uint64 1
c: -1
d: !yt/uint64 9223372036854775808
e: -9223372036854775808
f: !yt/uint64 18446744073709551615
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson), TYsonString(formatAttributes)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, Doubles)
{
    std::string yson = "{a=2.7; b=-3.14; c=0.0; d=4.; e=1e30; f=%nan; g=%inf; h=%-inf}";
    std::string expectedYaml = R"(
a: 2.7
b: -3.14
c: 0.
d: 4.
e: 1e+30
f: .nan
g: .inf
h: -.inf
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, Entity)
{
    std::string yson = "{a=#}";
    std::string expectedYaml = R"(
a: null
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, Booleans)
{
    std::string yson = "{a=%true; b=%false}";
    std::string expectedYaml = R"(
a: true
b: false
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, Strings)
{
    // a and b may be represented as plain scalars.
    // c-e must be quoted on syntactical level, so libyaml chooses a single-quoted style.
    // f-i must be quoted because they meet regexps for non-string types, so we force a double-quoted style.
    std::string yson = R"({a=hello; b="23asd"; c=" "; d="foo\nbar"; e=""; f="42"; g="TRUE"; h="1e4000"; i="~";})";
    std::string expectedYaml = R"(
a: hello
b: 23asd
c: ' '
d: 'foo

  bar'
e: ""
f: "42"
g: "TRUE"
h: "1e4000"
i: "~"
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, Mappings)
{
    std::string yson("{a={x=1;y={foo=bar;bar=foo}};b={z=3};c={};}");
    std::string expectedYaml = R"(
a:
  x: 1
  y:
    foo: bar
    bar: foo
b:
  z: 3
c: {}
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, Sequences)
{
    std::string yson = "[foo; [1; 2; 3]; bar; []; [[[#]]]]";
    std::string expectedYaml = R"(
- foo
- - 1
  - 2
  - 3
- bar
- []
- - - - null
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, MultiDocument)
{
    std::string yson = "foo;{a=1;b=2};[x;y];{};#;bar;[]";
    std::string expectedYaml = R"(
foo
---
a: 1
b: 2
---
- x
- y
--- {}
--- null
--- bar
--- []
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson, EYsonType::ListFragment)), expectedYaml.substr(1));
}

TEST(TYamlWriterTest, Attributes)
{
    std::string yson = "<x=1;y=2>{a=<>42; b=<x=#>[1;2;3]; c=<foo=1>#;}";
    std::string expectedYaml = R"(
!yt/attrnode
- x: 1
  y: 2
- a: !yt/attrnode
  - {}
  - 42
  b: !yt/attrnode
  - x: null
  - - 1
    - 2
    - 3
  c: !yt/attrnode
  - foo: 1
  - null
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson)), expectedYaml.substr(1));
};

//////////////////////////////////////////////////////////////////////////////

TEST(TYamlWriterTest, EmptyStream)
{
    std::string yson = "";
    std::string expectedYaml = "";
    EXPECT_EQ(YsonToYaml(TYsonString(yson, EYsonType::ListFragment)), expectedYaml);
}

//////////////////////////////////////////////////////////////////////////////

//! There is a reverse test in yaml_reader_ut.cpp.
TEST(TYamlWriterTest, RealExample)
{
    std::string formatAttributes = "{write_uint_tag=%true}";
    std::string yson = R"(
{
    "mount_config" = {};
    "schema" = <
        "strict" = %true;
        "unique_keys" = %false;
    > [
        {
            "name" = "lat";
            "required" = %false;
            "type" = "double";
            "type_v3" = {
                "type_name" = "optional";
                "item" = "double";
            };
        };
        {
            "name" = "lon";
            "required" = %false;
            "type" = "double";
            "type_v3" = {
                "type_name" = "optional";
                "item" = "double";
            };
        };
    ];
    "native_cell_tag" = 9991u;
    "creation_time" = "2024-08-15T11:17:59.314773Z";
    "inherit_acl" = %true;
    "revision" = 8233452423020u;
    "resource_usage" = {
        "node_count" = 1;
        "chunk_count" = 1;
        "disk_space_per_medium" = {
            "default" = 562182;
        };
        "disk_space" = 562182;
        "chunk_host_cell_master_memory" = 0;
        "master_memory" = 0;
        "tablet_count" = 0;
        "tablet_static_memory" = 0;
    };
    "acl" = [];
    "id" = "77d-1c53a-27070191-e4d8f5ac";
    "parent_id" = "77d-1c0d3-2707012f-ddf40dd7";
    "foreign" = %false;
    "type" = "table";
    "sequoia" = %false;
    "ref_counter" = 1;
    "builtin" = %false;
    "owner" = "max";
    "compression_ratio" = 0.3679379456925491;
}
    )";

    std::string expectedYaml = R"(
mount_config: {}
schema: !yt/attrnode
- strict: true
  unique_keys: false
- - name: lat
    required: false
    type: double
    type_v3:
      type_name: optional
      item: double
  - name: lon
    required: false
    type: double
    type_v3:
      type_name: optional
      item: double
native_cell_tag: !yt/uint64 9991
creation_time: 2024-08-15T11:17:59.314773Z
inherit_acl: true
revision: !yt/uint64 8233452423020
resource_usage:
  node_count: 1
  chunk_count: 1
  disk_space_per_medium:
    default: 562182
  disk_space: 562182
  chunk_host_cell_master_memory: 0
  master_memory: 0
  tablet_count: 0
  tablet_static_memory: 0
acl: []
id: 77d-1c53a-27070191-e4d8f5ac
parent_id: 77d-1c0d3-2707012f-ddf40dd7
foreign: false
type: table
sequoia: false
ref_counter: 1
builtin: false
owner: max
compression_ratio: 0.3679379456925491
)";
    EXPECT_EQ(YsonToYaml(TYsonString(yson), TYsonString(formatAttributes)), expectedYaml.substr(1));
}

//////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYaml
