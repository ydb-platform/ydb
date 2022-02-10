#include "xml-document.h"

#include <libxml/xinclude.h>
#include <libxml/xpathInternals.h>

#include <library/cpp/xml/init/init.h>

#include <util/generic/yexception.h>
#include <util/folder/dirut.h>

namespace {
    struct TInit {
        inline TInit() {
            NXml::InitEngine();
        }
    } initer;
}

namespace NXml {
    TDocument::TDocument(const TString& xml, Source type) {
        switch (type) {
            case File:
                ParseFile(xml);
                break;
            case String:
                ParseString(xml);
                break;
            case RootName: {
                TDocHolder doc(xmlNewDoc(XMLCHAR("1.0")));
                if (!doc)
                    THROW(XmlException, "Can't create xml document.");
                doc->encoding = xmlStrdup(XMLCHAR("utf-8"));

                TNodePtr node(xmlNewNode(nullptr, XMLCHAR(xml.c_str())));
                if (!node)
                    THROW(XmlException, "Can't create root node.");
                xmlDocSetRootElement(doc.Get(), node.Get());
                Y_UNUSED(node.Release());
                Doc = std::move(doc);
            } break;
            default:
                THROW(InvalidArgument, "Wrong source type");
        }
    }

    TDocument::TDocument(TDocument&& doc)
        : Doc(std::move(doc.Doc))
    {
    }

    TDocument& TDocument::operator=(TDocument&& doc) {
        if (this != &doc)
            doc.Swap(*this);

        return *this;
    }

    void TDocument::ParseFile(const TString& file) {
        if (!NFs::Exists(file))
            THROW(XmlException, "File " << file << " doesn't exist");

        TParserCtxtPtr pctx(xmlNewParserCtxt());
        if (!pctx)
            THROW(XmlException, "Can't create parser context");

        TDocHolder doc(xmlCtxtReadFile(pctx.Get(), file.c_str(), nullptr, XML_PARSE_NOCDATA));
        if (!doc)
            THROW(XmlException, "Can't parse file " << file);

        int res = xmlXIncludeProcessFlags(doc.Get(), XML_PARSE_XINCLUDE | XML_PARSE_NOCDATA | XML_PARSE_NOXINCNODE);

        if (res == -1)
            THROW(XmlException, "XIncludes processing failed");

        Doc = std::move(doc);
    }

    void TDocument::ParseString(TZtStringBuf xml) {
        TParserCtxtPtr pctx(xmlNewParserCtxt());
        if (pctx.Get() == nullptr)
            THROW(XmlException, "Can't create parser context");

        TDocHolder doc(xmlCtxtReadMemory(pctx.Get(), xml.c_str(), (int)xml.size(), nullptr, nullptr, XML_PARSE_NOCDATA));

        if (!doc)
            THROW(XmlException, "Can't parse string");

        Doc = std::move(doc);
    }

    TNode TDocument::Root() {
        xmlNode* r = xmlDocGetRootElement(Doc.Get());
        if (r == nullptr)
            THROW(XmlException, "TDocument hasn't root element");

        return TNode(Doc.Get(), r);
    }

    TConstNode TDocument::Root() const {
        xmlNode* r = xmlDocGetRootElement(Doc.Get());
        if (r == nullptr)
            THROW(XmlException, "TDocument hasn't root element");

        return TConstNode(TNode(Doc.Get(), r));
    }

    bool TNode::IsNull() const {
        return NodePointer == nullptr;
    }

    bool TNode::IsElementNode() const {
        return !IsNull() && (NodePointer->type == XML_ELEMENT_NODE);
    }

    TXPathContextPtr TNode::CreateXPathContext(const TNamespacesForXPath& nss) const {
        TXPathContextPtr ctx = xmlXPathNewContext(DocPointer);
        if (!ctx)
            THROW(XmlException, "Can't create empty xpath context");

        for (const auto& ns : nss) {
            const int r = xmlXPathRegisterNs(ctx.Get(), XMLCHAR(ns.Prefix.c_str()), XMLCHAR(ns.Url.c_str()));
            if (r != 0)
                THROW(XmlException, "Can't register namespace " << ns.Url << " with prefix " << ns.Prefix);
        }

        return ctx;
    }

    TConstNodes TNode::XPath(TZtStringBuf xpath, bool quiet, const TNamespacesForXPath& ns) const {
        TXPathContextPtr ctxt = CreateXPathContext(ns);
        return XPath(xpath, quiet, *ctxt);
    }

    TConstNodes TNode::XPath(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const {
        if (xmlXPathSetContextNode(NodePointer, &ctxt) != 0)
            THROW(XmlException, "Can't set xpath context node, probably the context is associated with another document");

        TXPathObjectPtr obj = xmlXPathEvalExpression(XMLCHAR(xpath.c_str()), &ctxt);
        if (!obj)
            THROW(XmlException, "Can't evaluate xpath expression " << xpath);

        TConstNodes nodes(DocPointer, obj);

        if (nodes.Size() == 0 && !quiet)
            THROW(NodeNotFound, xpath);

        return nodes;
    }

    TConstNodes TNode::Nodes(TZtStringBuf xpath, bool quiet, const TNamespacesForXPath& ns) const {
        TXPathContextPtr ctxt = CreateXPathContext(ns);
        return Nodes(xpath, quiet, *ctxt);
    }

    TConstNodes TNode::Nodes(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const {
        TConstNodes nodes = XPath(xpath, quiet, ctxt);
        if (nodes.Size() != 0 && !nodes[0].IsElementNode())
            THROW(XmlException, "xpath points to non-element nodes: " << xpath);
        return nodes;
    }

    TNode TNode::Node(TZtStringBuf xpath, bool quiet, const TNamespacesForXPath& ns) {
        TXPathContextPtr ctxt = CreateXPathContext(ns);
        return Node(xpath, quiet, *ctxt);
    }

    TConstNode TNode::Node(TZtStringBuf xpath, bool quiet, const TNamespacesForXPath& ns) const {
        TXPathContextPtr ctxt = CreateXPathContext(ns);
        return Node(xpath, quiet, *ctxt);
    }

    TNode TNode::Node(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) {
        TConstNodes n = Nodes(xpath, quiet, ctxt);

        if (n.Size() == 0 && !quiet)
            THROW(NodeNotFound, xpath);

        if (n.Size() == 0)
            return TNode();
        else
            return n[0].ConstCast();
    }

    TConstNode TNode::Node(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const {
        return const_cast<TNode*>(this)->Node(xpath, quiet, ctxt);
    }

    TNode TNode::FirstChild(TZtStringBuf name) {
        if (IsNull())
            THROW(XmlException, "Node is null");

        return Find(NodePointer->children, name);
    }

    TConstNode TNode::FirstChild(TZtStringBuf name) const {
        return const_cast<TNode*>(this)->FirstChild(name);
    }

    TNode TNode::FirstChild() {
        if (IsNull())
            THROW(XmlException, "Node is null");

        return TNode(DocPointer, NodePointer->children);
    }

    TConstNode TNode::FirstChild() const {
        return const_cast<TNode*>(this)->FirstChild();
    }

    TNode TNode::Parent() {
        if (nullptr == NodePointer->parent)
            THROW(XmlException, "Parent node not exists");

        return TNode(DocPointer, NodePointer->parent);
    }

    TConstNode TNode::Parent() const {
        return const_cast<TNode*>(this)->Parent();
    }

    TNode TNode::NextSibling(TZtStringBuf name) {
        if (IsNull())
            THROW(XmlException, "Node is null");

        return Find(NodePointer->next, name);
    }

    TConstNode TNode::NextSibling(TZtStringBuf name) const {
        return const_cast<TNode*>(this)->NextSibling(name);
    }

    TNode TNode::NextSibling() {
        if (IsNull())
            THROW(XmlException, "Node is null");

        return TNode(DocPointer, NodePointer->next);
    }

    TConstNode TNode::NextSibling() const {
        return const_cast<TNode*>(this)->NextSibling();
    }

    /* NOTE: by default child will inherit it's parent ns */

    TNode TNode::AddChild(TZtStringBuf name) {
        return AddChild(name, "");
    }

    /* NOTE: source node will be copied, as otherwise it will be double-freed from this and its own document */

    TNode TNode::AddChild(const TConstNode& node) {
        xmlNodePtr copy = xmlDocCopyNode(node.ConstCast().NodePointer, DocPointer, 1 /* recursive */);
        copy = xmlAddChild(NodePointer, copy);
        return TNode(DocPointer, copy);
    }

    void TNode::SetPrivate(void* priv) {
        NodePointer->_private = priv;
    }

    void* TNode::GetPrivate() const {
        return NodePointer->_private;
    }

    TNode TNode::Find(xmlNode* start, TZtStringBuf name) {
        for (; start; start = start->next)
            if (start->type == XML_ELEMENT_NODE && (name.empty() || !xmlStrcmp(start->name, XMLCHAR(name.c_str()))))
                return TNode(DocPointer, start);

        return TNode();
    }

    TString TNode::Name() const {
        if (IsNull())
            THROW(XmlException, "Node is null");

        return CAST2CHAR(NodePointer->name);
    }

    TString TNode::Path() const {
        TCharPtr path(xmlGetNodePath(NodePointer));
        if (!!path)
            return CAST2CHAR(path.Get());
        else
            return "";
    }

    xmlNode* TNode::GetPtr() {
        return NodePointer;
    }

    const xmlNode* TNode::GetPtr() const {
        return NodePointer;
    }

    bool TNode::IsText() const {
        if (IsNull())
            THROW(XmlException, "Node is null");

        return NodePointer->type == XML_TEXT_NODE;
    }

    void TNode::Remove() {
        xmlNode* nodePtr = GetPtr();
        xmlUnlinkNode(nodePtr);
        xmlFreeNode(nodePtr);
    }

    static int XmlWriteToOstream(void* context, const char* buffer, int len) {
        // possibly use to save doc as well
        IOutputStream* out = (IOutputStream*)context;
        out->Write(buffer, len);
        return len;
    }

    void TNode::SaveInternal(IOutputStream& stream, TZtStringBuf enc, int options) const {
        const char* encoding = enc.size() ? enc.data() : "utf-8";
        TSaveCtxtPtr ctx(xmlSaveToIO(XmlWriteToOstream, /* close */ nullptr, &stream,
                                     encoding, options));
        if (xmlSaveTree(ctx.Get(), (xmlNode*)GetPtr()) < 0)
            THROW(XmlException, "Failed saving node to stream");
    }

    void TNode::Save(IOutputStream& stream, TZtStringBuf enc, bool shouldFormat) const {
        SaveInternal(stream, enc, shouldFormat ? XML_SAVE_FORMAT : 0);
    }

    void TNode::SaveAsHtml(IOutputStream& stream, TZtStringBuf enc, bool shouldFormat) const {
        int options = XML_SAVE_AS_HTML;
        options |= shouldFormat ? XML_SAVE_FORMAT : 0;
        SaveInternal(stream, enc, options);
    }

    TConstNodes::TConstNodes(const TConstNodes& nodes)
        : SizeValue(nodes.Size())
        , Doc(nodes.Doc)
        , Obj(nodes.Obj)
    {
    }

    TConstNodes& TConstNodes::operator=(const TConstNodes& nodes) {
        if (this != &nodes) {
            SizeValue = nodes.Size();
            Doc = nodes.Doc;
            Obj = nodes.Obj;
        }

        return *this;
    }

    TConstNodes::TConstNodes(TConstNodesRef ref)
        : SizeValue(ref.r_.Size())
        , Doc(ref.r_.Doc)
        , Obj(ref.r_.Obj)
    {
    }

    TConstNodes& TConstNodes::operator=(TConstNodesRef ref) {
        if (this != &ref.r_) {
            SizeValue = ref.r_.Size();
            Doc = ref.r_.Doc;
            Obj = ref.r_.Obj;
        }
        return *this;
    }

    TConstNodes::operator TConstNodesRef() {
        return TConstNodesRef(*this);
    }

    TConstNodes::TConstNodes(xmlDoc* doc, TXPathObjectPtr obj)
        : SizeValue(obj && obj->nodesetval ? obj->nodesetval->nodeNr : 0)
        , Doc(doc)
        , Obj(obj)
    {
    }

    TConstNode TConstNodes::operator[](size_t number) const {
        if (number + 1 > Size())
            THROW(XmlException, "index out of range " << number);

        if (!Obj || !Obj->nodesetval)
            THROW(XmlException, "Broken TConstNodes object, Obj is null");

        xmlNode* node = Obj->nodesetval->nodeTab[number];
        return TNode(Doc, node);
    }

    TConstNode TConstNodes::TNodeIter::operator*() const {
        return Nodes[Index];
    }

}
