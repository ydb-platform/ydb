#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void SyncYPathMultisetAttributes(
    const IYPathServicePtr& service,
    const TYPath& path,
    const std::vector<std::pair<TString, TYsonString>>& requests)
{
    auto multisetAttributesRequest = TYPathProxy::MultisetAttributes(path);
    for (const auto& request : requests) {
        auto req = multisetAttributesRequest->add_subrequests();
        req->set_attribute(request.first);
        req->set_value(request.second.ToString());
    }
    ExecuteVerb(service, multisetAttributesRequest)
        .Get()
        .ThrowOnError();
}

TEST(TYTreeTest, TestMultisetAttributes)
{
    std::vector<std::pair<TString, TYsonString>> attributes1 = {
        {"k1", TYsonString(TStringBuf("v1"))},
        {"k2", TYsonString(TStringBuf("v2"))},
        {"k3", TYsonString(TStringBuf("v3"))}
    };

    std::vector<std::pair<TString, TYsonString>> attributes2 = {
        {"k2", TYsonString(TStringBuf("v4"))},
        {"k3", TYsonString(TStringBuf("v5"))},
        {"k4", TYsonString(TStringBuf("v6"))}
    };

    auto node = ConvertToNode(TYsonString(TStringBuf("{}")));

    SyncYPathMultisetAttributes(node, "/@", attributes1);

    EXPECT_EQ(node->Attributes().ListKeys().size(), 3u);
    for (const auto& [attribute, value] : attributes1) {
        EXPECT_EQ(node->Attributes().GetYson(attribute).ToString(), value.ToString());
    }

    SyncYPathMultisetAttributes(node, "/@", attributes2);
    EXPECT_EQ(node->Attributes().ListKeys().size(), 4u);
    for (const auto& [attribute, value] : attributes2) {
        EXPECT_EQ(node->Attributes().GetYson(attribute).ToString(), value.ToString());
    }
    EXPECT_EQ(node->Attributes().GetYson("k1").ToString(), "v1");

}

TEST(TYTreeTest, TestMultisetInvalidAttributes)
{
    std::vector<std::pair<TString, TYsonString>> validAttributes = {
        {"k1", TYsonString(TStringBuf("v1"))},
        {"k2", TYsonString(TStringBuf("v2"))},
        {"k3", TYsonString(TStringBuf("v3"))}
    };
    std::vector<std::pair<TString, TYsonString>> invalidAttributes = {
        {"k1", TYsonString(TStringBuf("v1"))},
        {"",   TYsonString(TStringBuf("v2"))}, // Empty attributes are not allowed.
        {"k3", TYsonString(TStringBuf("v3"))}
    };

    auto node = ConvertToNode(TYsonString(TStringBuf("{}")));

    EXPECT_THROW(SyncYPathMultisetAttributes(node, "", validAttributes), std::exception);
    EXPECT_THROW(SyncYPathMultisetAttributes(node, "/", validAttributes), std::exception);
    EXPECT_THROW(SyncYPathMultisetAttributes(node, "/@/", validAttributes), std::exception);
    EXPECT_THROW(SyncYPathMultisetAttributes(node, "/@", invalidAttributes), std::exception);
}

TEST(TYTreeTest, TestMultisetAttributesByPath)
{
    std::vector<std::pair<TString, TYsonString>> attributes1 = {
        {"a", TYsonString(TStringBuf("{}"))},
    };
    std::vector<std::pair<TString, TYsonString>> attributes2 = {
        {"a/b", TYsonString(TStringBuf("v1"))},
        {"a/c", TYsonString(TStringBuf("v2"))},
    };
    std::vector<std::pair<TString, TYsonString>> attributes3 = {
        {"b", TYsonString(TStringBuf("v3"))},
        {"c", TYsonString(TStringBuf("v4"))},
    };

    auto node = ConvertToNode(TYsonString(TStringBuf("{}")));

    SyncYPathMultisetAttributes(node, "/@", attributes1);
    SyncYPathMultisetAttributes(node, "/@a", attributes3);

    auto attribute = ConvertToNode(node->Attributes().GetYson("a"))->AsMap();
    EXPECT_EQ(attribute->GetKeys(), std::vector<std::string>({"b", "c"}));
}

TEST(TYTreeTest, TestGetWithAttributes)
{
    auto yson = TYsonStringBuf(
        "{"
        "    foo = <"
        "        a = 1;"
        "        b = {"
        "            x = 2;"
        "            y = 3;"
        "        };"
        "        c = [i1; i2; i3];"
        "        d = 4;"
        "    > {"
        "        bar = <"
        "            a = {"
        "                x = 5;"
        "                y = 6;"
        "            };"
        "            b = 7;"
        "            c = [j1; j2; j3; j4];"
        "            e = 8;"
        "        > #;"
        "    };"
        "}");
    auto node = ConvertToNode(yson);

    auto compareYsons = [] (TYsonStringBuf expectedYson, INodePtr node, const TAttributeFilter& attributeFilter) {
        // NB: serialization of ephemeral nodes is always stable.
        TYsonString actualYson = SyncYPathGet(node, "", attributeFilter);
        auto stableOriginal = ConvertToYsonString(node, EYsonFormat::Pretty).ToString();
        auto stableExpected = ConvertToYsonString(ConvertToNode(expectedYson), EYsonFormat::Pretty).ToString();
        auto stableActual = ConvertToYsonString(ConvertToNode(actualYson), EYsonFormat::Pretty).ToString();
        EXPECT_EQ(stableExpected, stableActual)
            << "Original:" << std::endl << stableOriginal << std::endl
            << "Filtered by: " << Format("%v", attributeFilter) << std::endl
            << "Expected: " << std::endl << stableExpected << std::endl
            << "Actual: " << std::endl << stableActual << std::endl;
    };

    compareYsons(
        yson,
        node,
        TAttributeFilter());

    auto expectedYson1 = TYsonStringBuf(
        "{"
        "    foo = <"
        "        a = 1;"
        "        b = {"
        "            x = 2;"
        "            y = 3;"
        "        };"
        "    > {"
        "        bar = <"
        "            a = {"
        "                x = 5;"
        "                y = 6;"
        "            };"
        "            b = 7;"
        "            e = 8;"
        "        > #;"
        "    };"
        "}");

    compareYsons(
        expectedYson1,
        node,
        TAttributeFilter({"a", "b", "e"}));

    compareYsons(
        expectedYson1,
        node,
        TAttributeFilter({}, {"/a", "/b", "/e"}));

    auto expectedYson2 = TYsonStringBuf(
        "{"
        "    foo = <"
        "        a = 1;"
        "    > {"
        "        bar = <"
        "            a = {"
        "                x = 5;"
        "                y = 6;"
        "            };"
        "        > #;"
        "    };"
        "}");

    compareYsons(
        expectedYson2,
        node,
        TAttributeFilter({}, {"/a"}));

    auto expectedYson3 = TYsonStringBuf(
        "{"
        "    foo = <"
        "        b = {"
        "            y = 3;"
        "        };"
        "    > {"
        "        bar = <"
        "            a = {"
        "                x = 5;"
        "            };"
        "        > #;"
        "    };"
        "}");

    compareYsons(
        expectedYson3,
        node,
        TAttributeFilter({}, {"/a/x", "/b/y"}));

    auto expectedYson4 = TYsonStringBuf(
        "{"
        "    foo = <"
        "        c = [#; i2];"
        "    > {"
        "        bar = <"
        "            c = [#; j2; #; j4];"
        "        > #;"
        "    };"
        "}");

    compareYsons(
        expectedYson4,
        node,
        TAttributeFilter({}, {"/c/1", "/c/3"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree

