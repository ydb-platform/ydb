#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/map.h>

#include "xml-document.h"

Y_UNIT_TEST_SUITE(TestXmlDocument) {
    Y_UNIT_TEST(Iteration) {
        NXml::TDocument xml(
            "<?xml version=\"1.0\"?>\n"
            "<root>qq<a><b></b></a>ww<c></c></root>",
            NXml::TDocument::String);

        NXml::TConstNode root = xml.Root();
        UNIT_ASSERT_EQUAL(root.Name(), "root");
        NXml::TConstNode n = root.FirstChild().NextSibling();
        UNIT_ASSERT_EQUAL(n.Name(), "a");
        n = n.NextSibling().NextSibling();
        UNIT_ASSERT_EQUAL(n.Name(), "c");
    }

    Y_UNIT_TEST(ParseString) {
        NXml::TDocument xml(
            "<?xml version=\"1.0\"?>\n"
            "<root>\n"
            "<a><b len=\"15\" correct=\"1\">hello world</b></a>\n"
            "<text>Некоторый текст</text>\n"
            "</root>",
            NXml::TDocument::String);

        NXml::TConstNode root = xml.Root();
        NXml::TConstNode b = root.Node("a/b");
        UNIT_ASSERT_EQUAL(b.Attr<int>("len"), 15);
        UNIT_ASSERT_EQUAL(b.Attr<bool>("correct"), true);

        NXml::TConstNode text = root.Node("text");
        UNIT_ASSERT_EQUAL(text.Value<TString>(), "Некоторый текст");
    }
    Y_UNIT_TEST(SerializeString) {
        NXml::TDocument xml("frob", NXml::TDocument::RootName);
        xml.Root().SetAttr("xyzzy", "Frobozz");
        xml.Root().SetAttr("kulness", 0.3);
        xml.Root().SetAttr("timelimit", 3);

        NXml::TNode authors = xml.Root().AddChild("authors");
        authors.AddChild("graham").SetAttr("name", "Nelson");
        authors.AddChild("zarf").SetValue("Andrew Plotkin");
        authors.AddChild("emshort", "Emily Short");

        TString data = xml.ToString("utf-8");
        UNIT_ASSERT_EQUAL(data, "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                                "<frob xyzzy=\"Frobozz\" kulness=\"0.3\" timelimit=\"3\">\n"
                                "  <authors>\n"
                                "    <graham name=\"Nelson\"/>\n"
                                "    <zarf>Andrew Plotkin</zarf>\n"
                                "    <emshort>Emily Short</emshort>\n"
                                "  </authors>\n"
                                "</frob>\n");
        // check default utf8 output with ru
        {
            NXml::TDocument xml2("frob", NXml::TDocument::RootName);
            xml2.Root().SetAttr("xyzzy", "привет =)");
            UNIT_ASSERT_VALUES_EQUAL(xml2.ToString(), "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                                                      "<frob xyzzy=\"привет =)\"/>\n");
        }
    }
    Y_UNIT_TEST(XPathNs) {
        using namespace NXml;
        TDocument xml(
            "<?xml version=\"1.0\"?>\n"
            "<root xmlns='http://hello.com/hello'>\n"
            "<a><b len=\"15\" correct=\"1\">hello world</b></a>\n"
            "<text>Некоторый текст</text>\n"
            "</root>",
            TDocument::String);

        TNamespacesForXPath nss;
        TNamespaceForXPath ns = {"h", "http://hello.com/hello"};
        nss.push_back(ns);

        TConstNode root = xml.Root();
        TConstNode b = root.Node("h:a/h:b", false, nss);
        UNIT_ASSERT_EQUAL(b.Attr<int>("len"), 15);
        UNIT_ASSERT_EQUAL(b.Attr<bool>("correct"), true);

        TConstNode text = root.Node("h:text", false, nss);
        UNIT_ASSERT_EQUAL(text.Value<TString>(), "Некоторый текст");

        // For performance you can create xpath context once using nss and pass it.
        TXPathContextPtr ctxt = root.CreateXPathContext(nss);
        UNIT_ASSERT(root.Node("text", true, *ctxt).IsNull());
        UNIT_ASSERT_EXCEPTION(root.Node("text", false, *ctxt), yexception);
        UNIT_ASSERT_EQUAL(root.Node("h:text", false, *ctxt).Value<TString>(), "Некоторый текст");
    }
    Y_UNIT_TEST(XmlNodes) {
        using namespace NXml;
        TDocument xml("<?xml version=\"1.0\"?>\n"
                      "<root>qq<a><b>asdfg</b></a>ww<c></c></root>",
                      NXml::TDocument::String);
        TNode root = xml.Root();
        UNIT_ASSERT_EQUAL(root.Value<TString>(), "qqasdfgww");
        TConstNode node = root.FirstChild();
        UNIT_ASSERT_EQUAL(node.IsText(), true);
        UNIT_ASSERT_EQUAL(node.Value<TString>(), "qq");
        node = node.NextSibling();
        UNIT_ASSERT_EQUAL(node.IsText(), false);
        UNIT_ASSERT_EQUAL(node.Name(), "a");
        UNIT_ASSERT_EQUAL(node.Value<TString>(), "asdfg");
        node = node.NextSibling();
        UNIT_ASSERT_EQUAL(node.IsText(), true);
        UNIT_ASSERT_EQUAL(node.Value<TString>(), "ww");
        node = node.NextSibling();
        UNIT_ASSERT_EQUAL(node.IsText(), false);
        UNIT_ASSERT_EQUAL(node.Name(), "c");
        UNIT_ASSERT_EQUAL(node.Value<TString>(), "");
        node = node.NextSibling();
        UNIT_ASSERT_EQUAL(node.IsNull(), true);
        TStringStream iterLog;
        for (const auto& node2 : root.Nodes("/root/*")) {
            iterLog << node2.Name() << ';';
        }
        UNIT_ASSERT_STRINGS_EQUAL(iterLog.Str(), "a;c;");

        // get only element nodes, ignore text nodes with empty "name" param
        node = root.FirstChild(TString());
        UNIT_ASSERT_EQUAL(node.IsText(), false);
        UNIT_ASSERT_EQUAL(node.Name(), "a");
        node = node.NextSibling(TString());
        UNIT_ASSERT_EQUAL(node.IsText(), false);
        UNIT_ASSERT_EQUAL(node.Name(), "c");

        // use exact "name" to retrieve children and siblings
        node = root.FirstChild("a");
        UNIT_ASSERT_EQUAL(node.IsNull(), false);
        UNIT_ASSERT_EQUAL(node.Name(), "a");
        node = node.NextSibling("c");
        UNIT_ASSERT_EQUAL(node.IsNull(), false);
        UNIT_ASSERT_EQUAL(node.Name(), "c");
        node = root.FirstChild("c"); // skip "a"
        UNIT_ASSERT_EQUAL(node.IsNull(), false);
        UNIT_ASSERT_EQUAL(node.Name(), "c");

        // node not found: no exceptions, null nodes are returned
        node = root.FirstChild("b"); // b is not direct child of root
        UNIT_ASSERT_EQUAL(node.IsNull(), true);
        node = root.FirstChild("nosuchnode");
        UNIT_ASSERT_EQUAL(node.IsNull(), true);
        node = root.FirstChild();
        node = root.NextSibling("unknownnode");
        UNIT_ASSERT_EQUAL(node.IsNull(), true);
        UNIT_ASSERT_EXCEPTION(node.Name(), yexception);
        UNIT_ASSERT_EXCEPTION(node.Value<TString>(), yexception);
        UNIT_ASSERT_EXCEPTION(node.IsText(), yexception);
    }
    Y_UNIT_TEST(DefVal) {
        using namespace NXml;
        TDocument xml("<?xml version=\"1.0\"?>\n"
                      "<root><a></a></root>",
                      NXml::TDocument::String);
        UNIT_ASSERT_EQUAL(xml.Root().Node("a", true).Node("b", true).Value<int>(3), 3);
    }
    Y_UNIT_TEST(NodesVsXPath) {
        using namespace NXml;
        TDocument xml("<?xml version=\"1.0\"?>\n"
                      "<root><a x=\"y\"></a></root>",
                      NXml::TDocument::String);
        UNIT_ASSERT_EXCEPTION(xml.Root().Nodes("/root/a/@x"), yexception);
        UNIT_ASSERT_VALUES_EQUAL(xml.Root().XPath("/root/a/@x").Size(), 1);
    }
    Y_UNIT_TEST(NodeIsFirst) {
        using namespace NXml;
        TDocument xml("<?xml version=\"1.0\"?>\n"
                      "<root><a x=\"y\">first</a>"
                      "<a>second</a></root>",
                      NXml::TDocument::String);
        UNIT_ASSERT_EXCEPTION(xml.Root().Node("/root/a/@x"), yexception);
        UNIT_ASSERT_STRINGS_EQUAL(xml.Root().Node("/root/a").Value<TString>(), "first");
    }
    Y_UNIT_TEST(CopyNode) {
        using namespace NXml;
        // default-construct empty node
        TNode empty;
        // put to container
        TMap<int, TNode> nmap;
        nmap[2];

        // do copy
        TDocument xml("<?xml version=\"1.0\"?>\n"
                      "<root><a></a></root>",
                      TDocument::String);

        TDocument xml2("<?xml version=\"1.0\"?>\n"
                       "<root><node><b>bold</b><i>ita</i></node></root>",
                       TDocument::String);

        TNode node = xml2.Root().Node("//node");
        TNode place = xml.Root().Node("//a");

        place.AddChild(node);

        TStringStream s;
        xml.Save(s, "", false);
        UNIT_ASSERT_VALUES_EQUAL(s.Str(),
                                 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                 "<root><a><node><b>bold</b><i>ita</i></node></a></root>\n");
    }

    Y_UNIT_TEST(RenderNode) {
        using namespace NXml;
        {
            // no namespaces
            TDocument xml(
                "<?xml version=\"1.0\"?>\n"
                "<root>\n"
                "<a><b len=\"15\" correct=\"1\">hello world</b></a>\n"
                "<text>Некоторый текст</text>\n"
                "</root>",
                TDocument::String);
            TNode n = xml.Root().Node("//a");
            UNIT_ASSERT_VALUES_EQUAL(n.ToString(), "<a><b len=\"15\" correct=\"1\">hello world</b></a>");
        }
        {
            // namespaces
            TDocument xml(
                "<?xml version=\"1.0\"?>\n"
                "<root xmlns='http://hello.com/hello'>\n"
                "<a><b len=\"15\" correct=\"1\">hello world</b></a>\n"
                "<text>Некоторый текст</text>\n"
                "</root>",
                TDocument::String);
            TNamespacesForXPath nss;
            TNamespaceForXPath ns = {"h", "http://hello.com/hello"};
            nss.push_back(ns);

            TNode n = xml.Root().Node("//h:a", false, nss);
            UNIT_ASSERT_VALUES_EQUAL(n.ToString(), "<a><b len=\"15\" correct=\"1\">hello world</b></a>");
        }
    }

    Y_UNIT_TEST(ReuseXPathContext) {
        using namespace NXml;

        TDocument xml(
            "<?xml version=\"1.0\"?>\n"
            "<root>\n"
            "<a><b><c>Hello, world!</c></b></a>\n"
            "<text x=\"10\">First</text>\n"
            "<text y=\"20\">Second</text>\n"
            "</root>",
            TDocument::String);

        TXPathContextPtr rootCtxt = xml.Root().CreateXPathContext();

        // Check Node()
        TConstNode b = xml.Root().Node("a/b", false, *rootCtxt);

        // We can use root node context for xpath evaluation in any node
        TConstNode c1 = b.Node("c", false, *rootCtxt);
        UNIT_ASSERT_EQUAL(c1.Value<TString>(), "Hello, world!");

        TXPathContextPtr bCtxt = b.CreateXPathContext();
        TConstNode c2 = b.Node("c", false, *bCtxt);
        UNIT_ASSERT_EQUAL(c2.Value<TString>(), "Hello, world!");

        // Mixing contexts from different documents is forbidden
        TDocument otherXml("<root></root>", TDocument::String);
        TXPathContextPtr otherCtxt = otherXml.Root().CreateXPathContext();
        UNIT_ASSERT_EXCEPTION(b.Node("c", false, *otherCtxt), yexception);

        // Check Nodes()
        TConstNodes texts = xml.Root().Nodes("text", true, *rootCtxt);
        UNIT_ASSERT_EQUAL(texts.Size(), 2);

        // Nodes() does't work for non-element nodes
        UNIT_ASSERT_EXCEPTION(xml.Root().Nodes("text/@x", true, *rootCtxt), yexception);

        // Check XPath()
        TConstNodes ys = xml.Root().XPath("text/@y", true, *rootCtxt);
        UNIT_ASSERT_EQUAL(ys.Size(), 1);
        UNIT_ASSERT_EQUAL(ys[0].Value<int>(), 20);
    }

    Y_UNIT_TEST(Html) {
        using namespace NXml;

        TDocument htmlChunk("video", TDocument::RootName);
        TNode videoNode = htmlChunk.Root();

        videoNode.SetAttr("controls");

        TStringStream ss;
        videoNode.SaveAsHtml(ss);
        UNIT_ASSERT_EQUAL(ss.Str(), "<video controls></video>");
    }

    Y_UNIT_TEST(Move) {
        using namespace NXml;

        TDocument xml1("foo", TDocument::RootName);
        xml1.Root().AddChild("bar");

        UNIT_ASSERT_VALUES_EQUAL(xml1.Root().ToString(), "<foo><bar/></foo>");

        TDocument xml2 = std::move(xml1);
        UNIT_ASSERT_EXCEPTION(xml1.Root(), yexception);
        UNIT_ASSERT_VALUES_EQUAL(xml2.Root().ToString(), "<foo><bar/></foo>");
    }

    Y_UNIT_TEST(StringConversion) {
        using namespace NXml;
        TDocument xml("foo", TDocument::RootName);
        auto root = xml.Root();
        const TStringBuf stringBuf = "bar";
        root.SetAttr("bar", stringBuf);
        const TString tString = "baz";
        root.SetAttr("baz", tString);
        root.SetAttr("quux", "literal");
        root.SetAttr("frob", 500);
    }
}
