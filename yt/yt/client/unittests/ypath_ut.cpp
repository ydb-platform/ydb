#include "key_helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/key_bound.h>

#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/tree_visitor.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <util/string/vector.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;
using namespace NYPath;
using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TYPathTest
    : public ::testing::Test
{
public:
    IYPathServicePtr RootService;

    void SetUp() override
    {
        RootService = GetEphemeralNodeFactory()->CreateMap();
    }

    static TYsonString TextifyYson(const TYsonString& data)
    {
        return ConvertToYsonString(data, NYson::EYsonFormat::Text);
    }

    void Set(const TYPath& path, const TString& value)
    {
        SyncYPathSet(RootService, path, TYsonString(value));
    }

    void Remove(const TYPath& path)
    {
        SyncYPathRemove(RootService, path);
    }

    TYsonString Get(const TYPath& path)
    {
        return TextifyYson(SyncYPathGet(RootService, path));
    }

    std::vector<TString> List(const TYPath& path)
    {
        return SyncYPathList(RootService, path);
    }

    void Check(const TYPath& path, const TString& expected)
    {
        TYsonString output = Get(path);
        EXPECT_TRUE(
            AreNodesEqual(
                ConvertToNode(TYsonString(expected)),
                ConvertToNode(output)));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYPathTest, MapModification)
{
    Set("/map", "{hello=world; list=[0;a;{}]; n=1}");

    Set("/map/hello", "not_world");
    Check("", "{map={hello=not_world;list=[0;a;{}];n=1}}");

    Set("/map/list/2/some", "value");
    Check("", "{map={hello=not_world;list=[0;a;{some=value}];n=1}}");

    Remove("/map/n");
    Check("", "{map={hello=not_world;list=[0;a;{some=value}]}}");

    Set("/map/list", "[]");
    Check("", "{map={hello=not_world;list=[]}}");

    Remove("/map/hello");
    Check("", "{map={list=[]}}");

    Remove("/map");
    Check("", "{}");
}

TEST_F(TYPathTest, ListModification)
{
    Set("/list", "[1;2;3]");
    Check("", "{list=[1;2;3]}");
    Check("/list", "[1;2;3]");
    Check("/list/0", "1");
    Check("/list/1", "2");
    Check("/list/2", "3");
    Check("/list/-1", "3");
    Check("/list/-2", "2");
    Check("/list/-3", "1");

    Set("/list/end", "4");
    Check("/list", "[1;2;3;4]");

    Set("/list/end", "5");
    Check("/list", "[1;2;3;4;5]");

    Set("/list/2", "100");
    Check("/list", "[1;2;100;4;5]");

    Set("/list/-2", "3");
    Check("/list", "[1;2;100;3;5]");

    Remove("/list/4");
    Check("/list", "[1;2;100;3]");

    Remove("/list/2");
    Check("/list", "[1;2;3]");

    Remove("/list/-1");
    Check("/list", "[1;2]");

    Set("/list/before:0", "0");
    Check("/list", "[0;1;2]");

    Set("/list/after:1", "3");
    Check("/list", "[0;1;3;2]");

    Set("/list/after:-1", "4");
    Check("/list", "[0;1;3;2;4]");

    Set("/list/before:-1", "5");
    Check("/list", "[0;1;3;2;5;4]");

    Set("/list/begin", "6");
    Check("/list", "[6;0;1;3;2;5;4]");
}

TEST_F(TYPathTest, ListReassignment)
{
    Set("/list", "[a;b;c]");
    Set("/list", "[1;2;3]");

    Check("", "{list=[1;2;3]}");
}

TEST_F(TYPathTest, Clear)
{
    Set("/my", "{list=<type=list>[1;2];map=<type=map>{a=1;b=2}}");

    Remove("/my/list/*");
    Check("/my/list", "<type=list>[]");
    Check("/my/list/@", "{type=list}");

    Remove("/my/map/*");
    Check("/my/map", "<type=map>{}");
    Check("/my/map/@", "{type=map}");
}

TEST_F(TYPathTest, Ls)
{
    Set("", "{a={x1={y1=1}};b={x2={y2=2}};c={x3={y3=3}};d={x4={y4=4}}}");

    Remove("/b");
    Set("/e", "5");

    auto result = List("");
    std::sort(result.begin(), result.end());

    std::vector<TString> expected;
    expected.push_back("a");
    expected.push_back("c");
    expected.push_back("d");
    expected.push_back("e");

    EXPECT_EQ(expected, result);
}

TEST_F(TYPathTest, LsOnUnsupportedNodes)
{
    EXPECT_ANY_THROW({
        Set("list", "[1; 2; 3; 4]");
        List("list");
    });

    EXPECT_ANY_THROW({
        Set("str", "aaa");
        List("str");
    });

    EXPECT_ANY_THROW({
        Set("int", "42");
        List("int");
    });

    EXPECT_ANY_THROW({
        Set("double", "3.14");
        List("double");
    });

    EXPECT_ANY_THROW({
        Set("entity", "#");
        List("entity");
    });
}

TEST_F(TYPathTest, Attributes)
{
    Set("/root", "<attr=100;mode=rw> {nodes=[1; 2]}");
    Check("/root/@", "{attr=100;mode=rw}");
    Check("/root/@attr", "100");

    Set("/root/value", "<>500");
    Check("/root/value", "500");

    Remove("/root/@*");
    Check("/root/@", "{}");

    Remove("/root/nodes");
    Remove("/root/value");
    Check("", "{root={}}");

    Set("/root/2", "<author=ignat> #");
    Check("", "{root={\"2\"=<author=ignat>#}}");
    Check("/root/2/@", "{author=ignat}");
    Check("/root/2/@author", "ignat");

    // note: empty attributes are shown when nested
    Set("/root/3", "<dir=<file=<>-100>#>#");
    Check("/root/3/@", "{dir=<file=<>-100>#}");
    Check("/root/3/@dir/@", "{file=<>-100}");
    Check("/root/3/@dir/@file", "<>-100");
    Check("/root/3/@dir/@file/@", "{}");
}

TEST_F(TYPathTest, RemoveAll)
{
    // from map
    Set("/map", "{foo=bar;key=value}");
    Remove("/map/*");
    Check("/map", "{}");

    // from list
    Set("/list", "[10;20;30]");
    Remove("/list/*");
    Check("/list", "[]");

    // from attributes
    Set("/attr", "<foo=bar;key=value>42");
    Remove("/attr/@*");
    Check("/attr/@", "{}");
}

TEST_F(TYPathTest, InvalidCases)
{
    Set("/root", "{}");

    // exception when setting attributes
    EXPECT_ANY_THROW(Set("/root/some", "[10; {key=value;foo=<attr=42a>bar}]"));
    Check("/root", "{}");

    EXPECT_ANY_THROW(Set("/a/b", "1")); // /a must exist
    EXPECT_ANY_THROW(Set("a", "{}")); // must start with '/'
    EXPECT_ANY_THROW(Set("/root/", "{}")); // cannot end with '/'
    EXPECT_ANY_THROW(Set("", "[]")); // change the type of root
    EXPECT_ANY_THROW(Remove("")); // remove the root
    EXPECT_ANY_THROW(Get("/b")); // get non-existent path

    // get non-existent attribute from non-existent node
    EXPECT_ANY_THROW(Get("/b/@some"));

    // get non-existent attribute from existent node
    EXPECT_ANY_THROW({
        Set("/c", "{}");
        Get("/c/@some");
    });

    // remove non-existing child
    EXPECT_ANY_THROW(Remove("/a"));
}

TEST_F(TYPathTest, ParseRichYPath1)
{
    auto path = NYPath::TRichYPath::Parse("<a=b>//home/ignat{a,b}[1:2]");
    EXPECT_EQ(path.GetPath(), "//home/ignat");
    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(path.Attributes()),
            ConvertToNode(TYsonString(TStringBuf("{a=b;columns=[a;b]; ranges=[{upper_limit={key=[2]};lower_limit={key=[1]}}]}")))));
}

TEST_F(TYPathTest, ParseRichYPath2)
{
    auto path = NYPath::TRichYPath::Parse("<a=b>//home");
    EXPECT_EQ(path.GetPath(), "//home");
    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(path.Attributes()),
            ConvertToNode(TYsonString(TStringBuf("{a=b}")))));
}

TEST_F(TYPathTest, ParseRichYPath3)
{
    auto path = NYPath::TRichYPath::Parse("//home");
    EXPECT_EQ(path.GetPath(), "//home");
    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(path.Attributes()),
            ConvertToNode(EmptyAttributes())));
}

TEST_F(TYPathTest, ParseRichYPath4)
{
    auto path = NYPath::TRichYPath::Parse("//home[:]");
    EXPECT_EQ(path.GetPath(), "//home");
    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(path.Attributes()),
            ConvertToNode(TYsonString(TStringBuf("{ranges=[{}]}")))));
}

TEST_F(TYPathTest, ParseRichYPath5)
{
    auto path = NYPath::TRichYPath::Parse("//home[(x, y):(a, b)]");
    EXPECT_EQ(path.GetPath(), "//home");
    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(path.Attributes()),
            ConvertToNode(TYsonString(TStringBuf(
                "{ranges=[{lower_limit={key=[x;y]};upper_limit={key=[a;b]}}]}")))));
}

TEST_F(TYPathTest, ParseRichYPath6)
{
    auto path = NYPath::TRichYPath::Parse("//home[#1:#2,x:y]");
    EXPECT_EQ(path.GetPath(), "//home");
    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(path.Attributes()),
            ConvertToNode(TYsonString(TStringBuf(
                "{ranges=["
                    "{lower_limit={row_index=1};upper_limit={row_index=2}};"
                    "{lower_limit={key=[x]};upper_limit={key=[y]}}"
                "]}")))));
}

TEST_F(TYPathTest, ParseRichYPath7)
{
    auto path = NYPath::TRichYPath::Parse("//home[x:#1000]");
    EXPECT_EQ(path.GetPath(), "//home");
    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(path.Attributes()),
            ConvertToNode(TYsonString(TStringBuf(
                "{ranges=["
                    "{lower_limit={key=[x]};upper_limit={row_index=1000}};"
                "]}")))));
}

TEST_F(TYPathTest, ParseRichYPath8)
{
    auto path = NYPath::TRichYPath::Parse(" <a=b> //home");
    EXPECT_EQ(path.GetPath(), "//home");
    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(path.Attributes()),
            ConvertToNode(TYsonString(TStringBuf("{a=b}")))));
}

TEST_F(TYPathTest, ParseRichYPath9)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TRichYPath::Parse("@home"),
        std::exception,
        "does not start with a valid root-designator");
}

TEST_F(TYPathTest, ParseRichYPath10)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TRichYPath::Parse(" \n <a=b>\n//home"),
        std::exception,
        "does not start with a valid root-designator");
}

TEST_F(TYPathTest, ParseRichYPath11)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TRichYPath::Parse(" \n//home"),
        std::exception,
        "does not start with a valid root-designator");
}

TEST_F(TYPathTest, IgnoreAmpersand1)
{
    Set("&/a", "b");
    Check("/a", "b");
    Check("&/a", "b");
}

TEST_F(TYPathTest, IgnoreAmpersand2)
{
    Set("/list", "[]");
    Set("/list&/end", "0");
    Check("/list", "[0]");
}

TEST_F(TYPathTest, IgnoreAmpersand3)
{
    Set("/map", "{}");
    Set("/map/@attr", "value");
    Check("/map&/@attr", "value");
}

TEST_F(TYPathTest, Cluster)
{
    {
        auto path = TRichYPath::Parse("mycluster://home/mytable");
        EXPECT_EQ("//home/mytable", path.GetPath());
        EXPECT_EQ("mycluster", path.GetCluster());
    }

    {
        auto path = TRichYPath::Parse("<cluster=first_cluster> second_cluster://home/mytable");
        EXPECT_EQ("//home/mytable", path.GetPath());
        EXPECT_EQ("second_cluster", path.GetCluster());
    }

    {
        auto path = TRichYPath::Parse(" <> mycluster://home/mytable");
        EXPECT_EQ("//home/mytable", path.GetPath());
        EXPECT_EQ("mycluster", path.GetCluster());
    }

    {
        auto path = TRichYPath::Parse("long-cluster-name_with_underscores://home/long-table-name");
        EXPECT_EQ("//home/long-table-name", path.GetPath());
        EXPECT_EQ("long-cluster-name_with_underscores", path.GetCluster());
    }

    {
        auto path = TRichYPath::Parse("//home/mytable");
        EXPECT_EQ("//home/mytable", path.GetPath());
        EXPECT_FALSE(path.GetCluster().has_value());
    }

    {
        auto path = TRichYPath::Parse("//path:with:colons/my:table");
        EXPECT_EQ("//path:with:colons/my:table", path.GetPath());
        EXPECT_FALSE(path.GetCluster().has_value());
    }

    {
        auto path = TRichYPath::Parse("//path-with-dashes/my-table");
        EXPECT_EQ("//path-with-dashes/my-table", path.GetPath());
        EXPECT_FALSE(path.GetCluster().has_value());
    }

    {
        // NB: There doesn't seem to be a feasible way to check this without interpreting the actual tokens.
        auto path = TRichYPath::Parse("replica://primary://tmp/queue");
        EXPECT_EQ("//primary://tmp/queue", path.GetPath());
        EXPECT_EQ("replica", path.GetCluster());
    }

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TRichYPath::Parse("bad+cluster!name://home/mytable"),
        std::exception,
        "illegal symbol");

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(TRichYPath::Parse("://home/mytable"),
        std::exception,
        "cannot be empty");

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TRichYPath::Parse("localhost:1234://queue"),
        std::exception,
        "does not start with a valid root-designator");

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TRichYPath::Parse("pythia:markov://queue"),
        std::exception,
        "does not start with a valid root-designator");
}

TEST_F(TYPathTest, NewReadRanges)
{
    TComparator comparatorAsc1({ESortOrder::Ascending});
    TComparator comparatorAsc2({ESortOrder::Ascending, ESortOrder::Ascending});
    TComparator comparatorDesc1({ESortOrder::Descending});
    TComparator comparatorDesc2({ESortOrder::Descending, ESortOrder::Descending});

    auto makeRow = [&] (const std::vector<int> values) {
        std::vector<TUnversionedValue> unversionedValues;
        unversionedValues.reserve(values.size());
        for (int value : values) {
            unversionedValues.emplace_back(MakeUnversionedInt64Value(value));
        }
        return MakeRow(std::move(unversionedValues));
    };

    {
        std::vector<TReadRange> ranges(1);
        // No ranges.
        EXPECT_EQ(
            ranges,
            TRichYPath::Parse("//t").GetNewRanges());
    }
    {
        // Some row index ranges in short form.
        std::vector<TReadRange> ranges(2);
        ranges[0].LowerLimit().SetRowIndex(1);
        ranges[0].UpperLimit().SetRowIndex(2);

        ranges[1].LowerLimit().SetRowIndex(3);

        EXPECT_EQ(
            ranges,
            TRichYPath::Parse("//t[#1, #3:]").GetNewRanges());
    }

    {
        // Key range without provided comparator is not allowed.
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            TRichYPath::Parse("//t[foo]").GetNewRanges(),
            std::exception,
            "for an unsorted object");
    }

    {
        // Some key ranges in short form.
        std::vector<TReadRange> ranges(2);
        // (123,456):789
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({123, 456});
        ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() < makeRow({789});
        // (424,242)
        ranges[1].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({424, 242});
        ranges[1].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeRow({424, 242});

        auto ypath = TRichYPath::Parse("//t[(123,456):789, (424,242)]");

        EXPECT_EQ(
            ranges,
            ypath.GetNewRanges(comparatorAsc2));
        EXPECT_EQ(
            ranges,
            ypath.GetNewRanges(comparatorDesc2));
    }

    {
        // Same key ranges in short form, but in context of shorter key prefix.
        // Inclusiveness of some key bounds is toggled.
        std::vector<TReadRange> ranges(2);
        // (123,456):789
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() > makeRow({123});
        ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() < makeRow({789});
        // (424,242); in ascending case it transforms into empty range.
        ranges[1].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() > makeRow({});
        ranges[1].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() < makeRow({});

        auto ypath = TRichYPath::Parse("//t[(123,456):789, (424,242)]");

        EXPECT_EQ(
            ranges,
            ypath.GetNewRanges(comparatorAsc1));
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            ypath.GetNewRanges(comparatorDesc1),
            std::exception,
            "Read limit key cannot be longer");
    }

    {
        // Short key in context of a longer key prefix.
        std::vector<TReadRange> ranges(2);
        // (123):456
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({123});
        ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() < makeRow({456});
        // (789)
        ranges[1].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({789});
        ranges[1].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeRow({789});

        auto ypath = TRichYPath::Parse("//t[(123):456, (789)]");

        EXPECT_EQ(
            ranges,
            ypath.GetNewRanges(comparatorAsc2));
        EXPECT_EQ(
            ranges,
            ypath.GetNewRanges(comparatorDesc2));
    }

    {
        // Some row index ranges in full form.
        std::vector<TReadRange> ranges(2);
        // #42:#57
        ranges[0].LowerLimit().SetRowIndex(42);
        ranges[0].UpperLimit().SetRowIndex(57);
        // #123
        ranges[1].LowerLimit().SetRowIndex(123);
        ranges[1].UpperLimit().SetRowIndex(124);

        auto ypath = TRichYPath::Parse(
            "<ranges=["
            "{lower_limit={row_index=42};upper_limit={row_index=57}};"
            "{exact={row_index=123}};"
            "]>//t");

        EXPECT_EQ(ranges, ypath.GetNewRanges());
    }

    {
        // Lower limit simultaneously with exact.
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            TRichYPath::Parse("<ranges=[{lower_limit={};exact={}}]>//t").GetNewRanges(),
            std::exception,
            "Exact limit cannot be specified simultaneously");
    }

    {
        // Key bound in exact limit.
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            TRichYPath::Parse(R"(<ranges=[{exact={key_bound=["<="; []]}}]>//t)").GetNewRanges(),
            std::exception,
            "Key bound cannot be specified in exact");
    }

    {
        // Key and key bound together, first case is regular for backward-compatible serialization,
        // second contains incorrect key which should be ignored.
        std::vector<TReadRange> ranges(2);
        // (42):
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({42});

        // (57):(23)

        ranges[1].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({57});
        ranges[1].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() < makeRow({23});

        auto ypath = TRichYPath::Parse(
            "<ranges=["
            R"({lower_limit={key_bound=[">=";[42]]; key=[42]}};)"
            R"({lower_limit={key_bound=[">=";[57]]; key=[123;asd]}; upper_limit={key_bound=["<"; [23]]; key=[]}};)"
            "]>//t");

        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorAsc1));
        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorDesc1));
        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorAsc2));
        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorDesc2));
    }

    {
        // Key with sentinels (ok for ascending and not ok for descending).
        std::vector<TReadRange> ranges(1);
        // (42):(57,<type=max>#)
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({42});
        ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeRow({57});

        auto ypath = TRichYPath::Parse("<ranges=[{lower_limit={key=[42]};upper_limit={key=[57;<type=max>#]}}]>//tmp/t");

        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorAsc2));

        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            ypath.GetNewRanges(comparatorDesc2),
            std::exception,
            "Sentinel values are not allowed");
    }

    {
        // Key with sentinels (ok for ascending and not ok for descending).
        std::vector<TReadRange> ranges(1);
        // (42):(57,<type=max>#)
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({42});
        ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeRow({57});

        auto ypath = TRichYPath::Parse("<ranges=[{lower_limit={key=[42]};upper_limit={key=[57;<type=max>#]}}]>//tmp/t");

        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorAsc2));

        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            ypath.GetNewRanges(comparatorDesc2),
            std::exception,
            "Sentinel values are not allowed");
    }

    {
        // Correct usage of key bounds for previous case.
        std::vector<TReadRange> ranges(1);
        // >=(42):<=(57)
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({42});
        ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeRow({57});

        auto ypath = TRichYPath::Parse(
            R"(<ranges=[{lower_limit={key_bound=[">=";[42]]};upper_limit={key_bound=["<=";[57]];}}]>//tmp/t)");

        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorAsc2));
        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorDesc2));
        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorAsc1));
        EXPECT_EQ(ranges, ypath.GetNewRanges(comparatorDesc1));
    }

    {
        std::vector<TReadRange> ranges(1);
        // >=(42,57) with shorter comparator, i.e not ok both for ascending and descending sort order.
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({42, 57});

        auto ypath = TRichYPath::Parse(R"(<ranges=[{lower_limit={key_bound=[">="; [42;57]]};}]>//tmp/t)");

        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            ypath.GetNewRanges(comparatorAsc1),
            std::exception,
            "Key bound length must not exceed");
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            ypath.GetNewRanges(comparatorDesc1),
            std::exception,
            "Key bound length must not exceed");
    }

    {
        // Exact consisting of multiple independent selectors.
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            TRichYPath::Parse("<ranges=[{exact={row_index=42;chunk_index=23}}]>//tmp/t").GetNewRanges(),
            std::exception,
            "Exact read limit must have exactly one independent selector");
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            TRichYPath::Parse("<ranges=[{exact={tablet_index=42;chunk_index=23}}]>//tmp/t").GetNewRanges(),
            std::exception,
            "Exact read limit must have exactly one independent selector");
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(
            TRichYPath::Parse("<ranges=[{exact={tablet_index=42;row_index=12;chunk_index=23}}]>//tmp/t").GetNewRanges(),
            std::exception,
            "Exact read limit must have exactly one independent selector");
    }
    {
        // Exact consisting of tablet_index or tablet_index + row_index (which is OK).
        std::vector<TReadRange> ranges(2);
        // tablet_index = 2
        ranges[0].LowerLimit().SetTabletIndex(2);
        ranges[0].UpperLimit().SetTabletIndex(3);
        // tablet_index = 2; row_index = 42;
        ranges[1].LowerLimit().SetTabletIndex(2);
        ranges[1].LowerLimit().SetRowIndex(42);
        ranges[1].UpperLimit().SetTabletIndex(2);
        ranges[1].UpperLimit().SetRowIndex(43);

        auto ypath = TRichYPath::Parse(
            "<ranges=["
            "{exact={tablet_index=2}};"
            "{exact={tablet_index=2;row_index=42}};"
            "]>//tmp/t");
    }

    {
        // Short key in context of a longer key prefix, long key in context of a shorter key prefix
        // and a key of proper length but with sentinels; long form. With ascending comparator
        // we should replace latter two ranges with an empty range.
        std::vector<TReadRange> ranges(3);
        // [(42)]
        ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeRow({42});
        ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeRow({42});
        // [(42, 43, 44)]
        ranges[1].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() > makeRow({});
        ranges[1].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() < makeRow({});
        // [(42, <type=max>#)]
        ranges[2].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() > makeRow({});
        ranges[2].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() < makeRow({});

        auto ypath = TRichYPath::Parse(
            "<ranges=["
            "{exact={key=[42];}};"
            "{exact={key=[42;43;44];}};"
            "{exact={key=[42;<type=max>#];}};"
            "]>//tmp/t");

        EXPECT_EQ(
            ranges,
            ypath.GetNewRanges(comparatorAsc2));
    }
}

TEST_F(TYPathTest, RangesTypeHintsInt64)
{
    // Verify that int64 key in YPath can be implicitly converted to uint64 and double.

    TComparator comparator({ESortOrder::Ascending, ESortOrder::Ascending});

    auto makeMixedRow = [&] (ui64 first, double second) {
        std::vector<TUnversionedValue> unversionedValues;
        unversionedValues.emplace_back(MakeUnversionedUint64Value(first));
        unversionedValues.emplace_back(MakeUnversionedDoubleValue(second));
        return MakeRow(std::move(unversionedValues));
    };

    std::vector<TReadRange> ranges(2);
    // [(42, 43)]
    ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeMixedRow(42u, 43.0);
    ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeMixedRow(42u, 43.0);
    // [(39, 45)]
    ranges[1].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeMixedRow(39u, 45.0);
    ranges[1].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeMixedRow(39u, 45.0);

    std::vector<TReadRange> smallRanges{ranges[0]};

    auto ypath = TRichYPath::Parse(
        "<ranges=["
        "{exact={key=[42;43];}};"
        "{exact={key=[39;45];}};"
        "]>//tmp/t");

    EXPECT_EQ(
        ranges,
        ypath.GetNewRanges(comparator, {NTableClient::EValueType::Uint64, NTableClient::EValueType::Double}));

    auto smallYpath = TRichYPath::Parse("//tmp/t[(42, 43)]");
    EXPECT_EQ(
        smallRanges,
        smallYpath.GetNewRanges(comparator, {NTableClient::EValueType::Uint64, NTableClient::EValueType::Double}));
}

TEST_F(TYPathTest, RangesTypeHintsUint64)
{
    // Verify that uint64 key in YPath can be implicitly converted to int64 and double.

    TComparator comparator({ESortOrder::Ascending, ESortOrder::Ascending});

    auto makeMixedRow = [&] (i64 first, double second) {
        std::vector<TUnversionedValue> unversionedValues;
        unversionedValues.emplace_back(MakeUnversionedInt64Value(first));
        unversionedValues.emplace_back(MakeUnversionedDoubleValue(second));
        return MakeRow(std::move(unversionedValues));
    };

    std::vector<TReadRange> ranges(2);
    // [(42, 43)]
    ranges[0].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeMixedRow(42, 43.0);
    ranges[0].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeMixedRow(42, 43.0);
    // [(39, 45)]
    ranges[1].LowerLimit().KeyBound() = TOwningKeyBound::FromRow() >= makeMixedRow(39, 45.0);
    ranges[1].UpperLimit().KeyBound() = TOwningKeyBound::FromRow() <= makeMixedRow(39, 45.0);

    std::vector<TReadRange> smallRanges{ranges[0]};

    auto ypath = TRichYPath::Parse(
        "<ranges=["
        "{exact={key=[42u;43u];}};"
        "{exact={key=[39u;45u];}};"
        "]>//tmp/t");

    EXPECT_EQ(
        ranges,
        ypath.GetNewRanges(comparator, {NTableClient::EValueType::Int64, NTableClient::EValueType::Double}));

    auto smallYpath = TRichYPath::Parse("//tmp/t[(42u, 43u)]");
    EXPECT_EQ(
        smallRanges,
        smallYpath.GetNewRanges(comparator, {NTableClient::EValueType::Int64, NTableClient::EValueType::Double}));
}

////////////////////////////////////////////////////////////////////////////////

class TRichYPathToStringTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TString>
{ };

TEST_P(TRichYPathToStringTest, TestRichYPathToString)
{
    auto path = NYPath::TRichYPath::Parse(GetParam());
    auto newPathString = ToString(path);
    auto parsedPath = NYPath::TRichYPath::Parse(newPathString);
    EXPECT_EQ(path.GetPath(), parsedPath.GetPath());
    EXPECT_TRUE(path.Attributes() == parsedPath.Attributes());
};

INSTANTIATE_TEST_SUITE_P(
    TRichYPathToStringTest,
    TRichYPathToStringTest,
    ::testing::Values(
        "//home/ignat",
        "<a=b>//home",
        "<a=b>//home/ignat{a,b}[1:2]",
        "//home[#1:#2,x:y]",
        "<a=b;c=d>//home{a,b}[(x, y):(a, b),#1:#2]",
        "<ranges={lower_limit={chunk_index=10}}>//home/ignat/my_table{}",
        "<a=b; columns=[key1;key2;key3]>//tmp/[0, (3, abc, true), :12u]"
));

////////////////////////////////////////////////////////////////////////////////

class TEmbeddedYPathOpsTest
    : public ::testing::Test
{
public:
    static INodePtr ParseNode(const TString& data)
    {
        return ConvertToNode(TYsonString(data));
    }

    static void ExpectEqual(INodePtr node, const TString& ysonString)
    {
        EXPECT_EQ(ConvertToYsonString(node, EYsonFormat::Text).AsStringBuf(), ysonString);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TEmbeddedYPathOpsTest, SimpleMap)
{
    const auto node = ParseNode(R""({foo={bar="baz"}})"");
    ExpectEqual(GetNodeByYPath(node, "/foo/bar"), R""("baz")"");

    EXPECT_THROW(GetNodeByYPath(node, "/foo/qux"), std::exception);
    EXPECT_THROW(GetNodeByYPath(node, "/foo/bar/qux"), std::exception);
}

TEST_F(TEmbeddedYPathOpsTest, SimpleList)
{
    const auto node = ParseNode(R""({home={roizner={list=[100; 500; {foo=bar}; 42]}}})"");
    ExpectEqual(GetNodeByYPath(node, "/home/roizner/list/1"), "500");
    ExpectEqual(GetNodeByYPath(node, "/home/roizner/list/-1"), "42");
    ExpectEqual(GetNodeByYPath(node, "/home/roizner/list/2/foo"), R""("bar")"");

    EXPECT_THROW(GetNodeByYPath(node, "/home/roizner/list/4"), std::exception);
    EXPECT_THROW(GetNodeByYPath(node, "/home/roizner/list/-5"), std::exception);
}

TEST_F(TEmbeddedYPathOpsTest, attributes)
{
    const auto node = ParseNode(R""({home=<account=sys>{dir1=<account=root;user_attr=<omg="embedded attributes">{foo=bar}>{};dir2={}}})"");
    ExpectEqual(GetNodeByYPath(node, "/home/dir1/@account"), R""("root")"");
    ExpectEqual(GetNodeByYPath(node, "/home/dir1/@user_attr/foo"), R""("bar")"");
    ExpectEqual(GetNodeByYPath(node, "/home/dir1/@user_attr/@omg"), R""("embedded attributes")"");
    ExpectEqual(GetNodeByYPath(node, "/home/dir1/@"), R""({"account"="root";"user_attr"=<"omg"="embedded attributes";>{"foo"="bar";};})"");

    EXPECT_THROW(GetNodeByYPath(node, "/home/dir1/@user_attr/bar"), std::exception);
    EXPECT_THROW(GetNodeByYPath(node, "/home/dir2/@account"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
