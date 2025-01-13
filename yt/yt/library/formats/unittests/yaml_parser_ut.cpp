#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/library/formats/yaml_parser.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT::NFormats {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////

TString ParseYaml(const TString& yaml, EYsonType ysonType)
{
    TStringStream inputStream(yaml);
    TStringStream outputStream;
    TYsonWriter writer(&outputStream, EYsonFormat::Pretty, ysonType);
    auto config = New<TYamlFormatConfig>();
    ParseYaml(&inputStream, &writer, config, ysonType);
    return outputStream.Str();
}

//////////////////////////////////////////////////////////////////////////////

TEST(TYamlParserTest, Simple)
{
    TString yaml = R"(
hello)";
    // Here and in the rest of the tests we introduce an extra leading \n for the better readabilty, which we later
    // strip off in the comparison.
    TString expectedYson = R"(
"hello")";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));
}

TEST(TYamlParserTest, Integers)
{
    TString yaml = R"(
a: 1
b: -1
# Hex and oct
c: 0xDeAdBeEf
d: 0o42
# Various non-normalized forms of numbers
e: -000
f: +0
g: +42
# Would be oct in YAML 1.1, but not in YAML 1.2!
h: 0042
i: -018
# 2^63, should be unsigned
j: 9223372036854775808
# 2^64 - 1, should be unsigned
k: 18446744073709551615
l: -9223372036854775808
m: !yt/uint64 1234
n: !!int 23
o: !!int -15)";
    TString expectedYson = R"(
{
    "a" = 1;
    "b" = -1;
    "c" = 3735928559u;
    "d" = 34u;
    "e" = 0;
    "f" = 0;
    "g" = 42;
    "h" = 42;
    "i" = -18;
    "j" = 9223372036854775808u;
    "k" = 18446744073709551615u;
    "l" = -9223372036854775808;
    "m" = 1234u;
    "n" = 23;
    "o" = -15;
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));

    std::vector<TString> invalidYamls = {
        "!!int -0x42",
        "!!int -0o23",
        "!!int deadbeef",
        "!!int 18446744073709551616",
        "!!int -9223372036854775809"
        "!yt/uint64 -1",
        "!yt/uint64 18446744073709551616",
        "!!int 0x",
        // Examples below were integers in YAML 1.1, but not in YAML 1.2.
        "!!int 123_456",
        "!!int 190:20:30",
        "!!int 0b1001",
        "!!int \"\"",
    };
    for (const auto& yaml : invalidYamls) {
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(ParseYaml(yaml, EYsonType::Node), std::exception, "is not an integer or does not fit")
            << "For YAML: " << yaml << std::endl;
    }
}

TEST(TYamlParserTest, Floats)
{
    TString yaml = R"(
a: 1.
b: .2
c: +3.14
d: -2.17
e: .inf
f: -.Inf
g: +.INF
h: .nan
i: .NaN
j: .NAN
k: !!float 42
l: 1e2
m: 1e+2
n: 1e-2
)";
    TString expectedYson = R"(
{
    "a" = 1.;
    "b" = 0.2;
    "c" = 3.14;
    "d" = -2.17;
    "e" = %inf;
    "f" = %-inf;
    "g" = %inf;
    "h" = %nan;
    "i" = %nan;
    "j" = %nan;
    "k" = 42.;
    "l" = 100.;
    "m" = 100.;
    "n" = 0.01;
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));

    std::vector<TString> invalidYamls = {
        "!!float 0o23",
        "!!float 1e",
        "!!float 1e+",
        "!!float 1e-",
        "!!float 1e-2.3",
        "!!float 1e2.3",
        // Examples below were integers in YAML 1.1, but not in YAML 1.2.
        "!!float 123_456",
        "!!float 190:20:30.15",
        "!!float inf",
        "!!float .InF",
        "!!float -+42.0",
        "!!float .",
        // For some reason arcadian FloatToString parses this, but it feels excessive to ban that
        // despite not satisfying the regexp from the spec.
        // "!!float 0x42",
    };
    for (const auto& yaml : invalidYamls) {
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(ParseYaml(yaml, EYsonType::Node), std::exception, "is not a floating point")
            << "For YAML: " << yaml << std::endl;
    }
}

TEST(TYamlParserTest, Booleans)
{
    TString yaml = R"(
a: true
b: false
c: True
d: False
e: TRUE
f: FALSE
g: !!bool true
)";
    TString expectedYson = R"(
{
    "a" = %true;
    "b" = %false;
    "c" = %true;
    "d" = %false;
    "e" = %true;
    "f" = %false;
    "g" = %true;
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));

    std::vector<TString> invalidYamls = {
        "!!bool 1",
        "!!bool 0",
        // Examples below were booleans in YAML 1.1, but not in YAML 1.2.
        "!!bool yes",
        "!!bool no",
        "!!bool on",
        "!!bool off",
        "!!bool y",
        "!!bool n",
        "!!bool \"\"",
    };
    for (const auto& yaml : invalidYamls) {
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(ParseYaml(yaml, EYsonType::Node), std::exception, "is not a boolean")
            << "For YAML: " << yaml << std::endl;
    }
}

TEST(TYamlParserTest, Nulls)
{
    TString yaml = R"(
a: null
b: Null
c: NULL
d: ~
e:
f: !!null null
# This is not allowed by a regexp in a spec, but feels excessive to ban.
g: !!null foo
)";
    TString expectedYson = R"(
{
    "a" = #;
    "b" = #;
    "c" = #;
    "d" = #;
    "e" = #;
    "f" = #;
    "g" = #;
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));
}

TEST(TYamlParserTest, Strings)
{
    TString yaml = R"(
a: "hello"
b: 'world'
c: of
d: !!str warcraft
e: !!str 42
f: !!str ~
g: ! hello
)";
    TString expectedYson = R"(
{
    "a" = "hello";
    "b" = "world";
    "c" = "of";
    "d" = "warcraft";
    "e" = "42";
    "f" = "~";
    "g" = "hello";
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));
}

TEST(TYamlParserTest, Mappings)
{
    TString yaml = R"(
a:
  x: 1
  y:
    foo: bar
    bar: foo
42:
  z: 3
c: {}
)";
    TString expectedYson = R"(
{
    "a" = {
        "x" = 1;
        "y" = {
            "foo" = "bar";
            "bar" = "foo";
        };
    };
    "42" = {
        "z" = 3;
    };
    "c" = {};
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));
}

TEST(TYamlParserTest, Sequences)
{
    TString yaml = R"(
- foo
- - 1
  - 2
  - 3
- bar
- []
- - - - null
)";
    TString expectedYson = R"(
[
    "foo";
    [
        1;
        2;
        3;
    ];
    "bar";
    [];
    [
        [
            [
                #;
            ];
        ];
    ];
])";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));
}

TEST(TYamlParserTest, Attributes)
{
    TString yaml = R"(
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
    // <x=1;y=2>{a=<>42; b=<x=#>[1;2;3]; c=<foo=1>#;}
    TString expectedYson = R"(
<
    "x" = 1;
    "y" = 2;
> {
    "a" = <> 42;
    "b" = <
        "x" = #;
    > [
        1;
        2;
        3;
    ];
    "c" = <
        "foo" = 1;
    > #;
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));

    std::vector<std::pair<TString, TString>> invalidYamlsAndErrors = {
        {R"(
!yt/attrnode
- x: 1
)", "Unexpected event type \"sequence_end\""},
        {R"(
!yt/attrnode
- foo
- bar
)", "Unexpected event type \"scalar\""},
        {R"(
!yt/attrnode
- x: 1
- y: 2
- z: 3
)", "Unexpected event type \"mapping_start\""},
};
    for (const auto& [yaml, error] : invalidYamlsAndErrors) {
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(ParseYaml(yaml, EYsonType::Node), std::exception, error)
            << "For YAML: " << yaml << std::endl;
    }
};

TEST(TYamlParserTest, MultiDocument)
{
    TString yaml = R"(
a: 1
---
foo
---
~
---
)";
    TString expectedYson = R"(
{
    "a" = 1;
};
"foo";
#;
#;
)";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::ListFragment), expectedYson.substr(1));
}

TEST(TYamlParserTest, Anchors)
{
    TString yaml = R"(
a: &foo 1
b: *foo
c: &bar
  x: &baz
  - False
  - &qux True
  y: 2
  z: *baz
  t: *foo
  w: *qux
d: *bar
e: *baz
f: *foo
g: *qux
)";
    TString expectedYson = R"(
{
    "a" = 1;
    "b" = 1;
    "c" = {
        "x" = [
            %false;
            %true;
        ];
        "y" = 2;
        "z" = [
            %false;
            %true;
        ];
        "t" = 1;
        "w" = %true;
    };
    "d" = {
        "x" = [
            %false;
            %true;
        ];
        "y" = 2;
        "z" = [
            %false;
            %true;
        ];
        "t" = 1;
        "w" = %true;
    };
    "e" = [
        %false;
        %true;
    ];
    "f" = 1;
    "g" = %true;
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));

    std::vector<std::pair<TString, TString>> invalidYamlsAndErrors = {
        {R"(
a: *foo
)", "undefined or unfinished anchor"},
        {R"(
- &foo a
- &foo b
)", "already defined"},
        {R"(
a: &foo
- b: &foo
  - c
)", "already defined"},
        {R"(
a: &foo
  bar: *foo
)", "undefined or unfinished anchor"},
        {R"(
a: &foo bar
*foo: baz
)", "alias as a map key is not supported"},
        {R"(
&foo a: b
)", "anchors on map keys is not supported"},
    };
    for (const auto& [yaml, error] : invalidYamlsAndErrors) {
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(ParseYaml(yaml, EYsonType::Node), std::exception, error)
            << "For YAML: " << yaml << std::endl;
    }
}

TEST(TYamlParserTest, Empty)
{
    TString yaml = "";
    TString expectedYson = "";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::ListFragment), expectedYson);
}

//! There is a reverse test in yaml_writer_ut.cpp.
TEST(TYamlParserTest, RealExample)
{
    TString yaml = R"(
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
    TString expectedYson = R"(
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
})";
    EXPECT_EQ(ParseYaml(yaml, EYsonType::Node), expectedYson.substr(1));
}

////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
