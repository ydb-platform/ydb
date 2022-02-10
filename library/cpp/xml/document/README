A wrapper around the DOM interface of libxml2.

The standard way to use it is as follows:

    #include <library/cpp/xml/document/xml-document.h>
    ...

    // open a document
    NXml::TDocument xml("filename.xml");

    // get a nodeset from an XPath query
    NXml::TConstNodes nodes = xml.Root().Nodes("xpath/expression/here");

    // iterate over the nodeset
    for (size_t i = 0; i < nodes.size(); ++i) {
        using namespace NXml;
        TConstNode& node = nodes[i];
        // query node
        TString name = node.Name();
        TString lang = node.Attr<TString>("lang");
        TString text = node.Value<TString>();
        TConstNode child = node.GetFirstChild("");
        // edit node
        TNode node = child.ConstCast();
        node.DelAttr("id");
        node.SetAttr("x", 2);
        node.SetValue(5);
        node.AddText(" apples");
    }

    // edit documents with copy-paste
    NXml::TDocument xml2("<xpath><node/></xpath>", NXml::TDocument::String);
    NXml::TNode place = xml2.Root().Node("xpath/node");
    // copy node's subtree from one document to another
    place.AddChild(xml.Root());
    // save (render) single element
    TString modifiedNode = place.ToString();
    // save whole document with optional encoding
    TString modifiedDoc  = xml2.ToString("ISO-8559-1");


See xml-document_ut.cpp for more examples.
