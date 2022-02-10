#pragma once

#include <library/cpp/string_utils/ztstrbuf/ztstrbuf.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <algorithm>
#include "libxml-guards.h"

namespace NXml {
    class TNode;

    class TConstNodes;
    class TConstNode;

    using TXPathContext = xmlXPathContext;

    class TDocument {
    public:
        enum Source {
            File,
            String,
            RootName,
        };

    public:
        /**
        * create TDocument
        * @param source: filename, XML string, or name for the root element (depends on @src)
        * @param src: source type: File | String | RootName
        * throws if file not found or cannot be parsed
        */
        TDocument(const TString& source, Source type = File);

    public:
        TDocument(const TDocument& that) = delete;
        TDocument& operator=(const TDocument& that) = delete;

        TDocument(TDocument&& that);
        TDocument& operator=(TDocument&& that);

        /**
        * get root element
        */
        TNode Root();
        TConstNode Root() const;

        void Save(IOutputStream& stream, TZtStringBuf enc = "", bool shouldFormat = true) const {
            int bufferSize = 0;
            xmlChar* xmlBuff = nullptr;
            const char* encoding = enc.size() ? enc.data() : Doc->encoding ? nullptr : "UTF-8";
            xmlDocDumpFormatMemoryEnc(Doc.Get(), &xmlBuff, &bufferSize, encoding, shouldFormat);
            TCharPtr xmlCharBuffPtr(xmlBuff);
            stream.Write(xmlBuff, bufferSize);
        }

        TString ToString(TZtStringBuf enc = "", bool shouldFormat = true) const {
            TStringStream s;
            Save(s, enc, shouldFormat);
            return s.Str();
        }

        void Swap(TDocument& that) {
            std::swap(this->Doc, that.Doc);
        }

        xmlDocPtr GetImpl() {
            return Doc.Get();
        }

    private:
        void ParseFile(const TString& file);
        void ParseString(TZtStringBuf xml);

        TDocument(TDocHolder doc)
            : Doc(std::move(doc))
        {
        }

        TDocHolder Doc;
    };

    struct TNamespaceForXPath {
        TString Prefix;
        TString Url;
    };
    typedef TVector<TNamespaceForXPath> TNamespacesForXPath;

    class TConstNodes {
    private:
        struct TConstNodesRef {
            explicit TConstNodesRef(TConstNodes& n)
                : r_(n)
            {
            }
            TConstNodes& r_;
        };

    public:
        TConstNodes(const TConstNodes& nodes);
        TConstNodes& operator=(const TConstNodes& nodes);

        TConstNodes(TConstNodesRef ref);
        TConstNodes& operator=(TConstNodesRef ref);

        operator TConstNodesRef();

        /**
        * get node by id
        * @param number: node id
        */
        TConstNode operator[](size_t number) const;

        /**
        * get number of nodes
        */
        size_t Size() const {
            return SizeValue;
        }
        size_t size() const {
            return SizeValue;
        }

        struct TNodeIter {
            const TConstNodes& Nodes;
            size_t Index;
            TConstNode operator*() const;
            bool operator==(const TNodeIter& other) const {
                return Index == other.Index;
            }
            bool operator!=(const TNodeIter& other) const {
                return !(*this == other);
            }
            TNodeIter operator++() {
                Index++;
                return *this;
            }
        };
        TNodeIter begin() const {
            return TNodeIter{*this, 0};
        }
        TNodeIter end() const {
            return TNodeIter{*this, size()};
        }

    private:
        friend class TDocument;
        friend class TConstNode;
        friend class TNode;

        TConstNodes(xmlDoc* doc, TXPathObjectPtr obj);

        size_t SizeValue;
        xmlDoc* Doc;
        TXPathObjectPtr Obj;
    };

    class TNode {
    public:
        friend class TDocument;
        friend class TConstNode;
        friend class TTextReader;

        /**
        * check if node is null
        */
        bool IsNull() const;

        /**
        * check if node is element node
        */
        bool IsElementNode() const;

        /**
        * Create xpath context to be used later for fast xpath evaluation.
        * @param nss: explicitly specify XML namespaces to use and their prefixes
        *
        * For better performance, when you need to evaluate several xpath expressions,
        * it makes sense to create a context, load namespace prefixes once
        * and use the context several times in Node(), Nodes(), XPath() function calls for several nodes.
        * The context may be used with any node of the current document, but
        * cannot be shared between different XML documents.
        */
        TXPathContextPtr CreateXPathContext(const TNamespacesForXPath& nss = TNamespacesForXPath()) const;

        /**
        * get all element nodes matching given xpath expression
        * @param xpath: xpath expression
        * @param quiet: don't throw exception if zero nodes found
        * @param ns: explicitly specify XML namespaces to use and their prefixes
        *
        * For historical reasons, this only works for *element* nodes.
        * Use the XPath function if you need other kinds of nodes.
        */
        TConstNodes Nodes(TZtStringBuf xpath, bool quiet = false, const TNamespacesForXPath& ns = TNamespacesForXPath()) const;

        /**
        * get all element nodes matching given xpath expression
        * @param xpath: xpath expression
        * @param quiet: don't throw exception if zero nodes found
        * @param ctxt: reusable xpath context
        *
        * For historical reasons, this only works for *element* nodes.
        * Use the XPath function if you need other kinds of nodes.
        */
        TConstNodes Nodes(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const;

        /**
        * get all nodes matching given xpath expression
        * @param xpath: xpath expression
        * @param quiet: don't throw exception if zero nodes found
        * @param ns: explicitly specify XML namespaces to use and their prefixes
        */
        TConstNodes XPath(TZtStringBuf xpath, bool quiet = false, const TNamespacesForXPath& ns = TNamespacesForXPath()) const;

        /**
        * get all nodes matching given xpath expression
        * @param xpath: xpath expression
        * @param quiet: don't throw exception if zero nodes found
        * @param ctxt: reusable xpath context
        */
        TConstNodes XPath(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const;

        /**
        * get the first element node matching given xpath expression
        * @param xpath: path to node (from current node)
        * @param quiet: don't throw exception if node not found,
        *               return null node (@see IsNull())
        * @param ns: explicitly specify XML namespaces to use and their prefixes
        *
        * For historical reasons, this only works for *element* nodes.
        * Use the XPath function if you need other kinds of nodes.
        */
        /// @todo: quiet should be default, empty nodeset is not an error
        TNode Node(TZtStringBuf xpath, bool quiet = false, const TNamespacesForXPath& ns = TNamespacesForXPath());
        TConstNode Node(TZtStringBuf xpath, bool quiet = false, const TNamespacesForXPath& ns = TNamespacesForXPath()) const;

        /**
        * get the first element node matching given xpath expression
        * @param xpath: path to node (from current node)
        * @param quiet: don't throw exception if node not found,
        *               return null node (@see IsNull())
        * @param ctxt: reusable xpath context
        *
        * For historical reasons, this only works for *element* nodes.
        * Use the XPath function if you need other kinds of nodes.
        */
        TNode Node(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt);
        TConstNode Node(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const;

        /**
        * get node first child
        * @param name: child name
        * @note if name is empty, returns the first child node of type "element"
        * @note returns null node if no child found
        */
        TNode FirstChild(TZtStringBuf name);
        TConstNode FirstChild(TZtStringBuf name) const;

        TNode FirstChild();
        TConstNode FirstChild() const;

        /**
        * get parent node
        * throws exception if has no parent
        */
        TNode Parent();
        TConstNode Parent() const;

        /**
        * get node neighbour
        * @param name: neighbour name
        * @note if name is empty, returns the next sibling node of type "element"
        * @node returns null node if no neighbour found
        */
        TNode NextSibling(TZtStringBuf name);
        TConstNode NextSibling(TZtStringBuf name) const;

        TNode NextSibling();
        TConstNode NextSibling() const;

        /**
        * create child node
        * @param name: child name
        * returns new empty node
        */
        TNode AddChild(TZtStringBuf name);

        /**
        * create child node with given value
        * @param name: child name
        * @param value: node value
        */
        template <class T>
        typename std::enable_if<!std::is_convertible_v<T, TZtStringBuf>, TNode>::type
        AddChild(TZtStringBuf name, const T& value);

        TNode AddChild(TZtStringBuf name, TZtStringBuf value);

        /**
        * add child node, making recursive copy of original
        * @param node: node to copy from
        * returns added node
        */
        TNode AddChild(const TConstNode& node);

        /**
        * create text child node
        * @param name: child name
        * @param value: node value
        */
        template <class T>
        typename std::enable_if<!std::is_convertible_v<T, TStringBuf>, TNode>::type
        AddText(const T& value);

        TNode AddText(TStringBuf value);

        /**
        * get node attribute
        * @param name: attribute name
        * throws exception if attribute not found
        */
        template <class T>
        T Attr(TZtStringBuf name) const;

        /**
        * get node attribute
        * @param name: attribute name
        * returns default value if attribute not found
        */
        template <class T>
        T Attr(TZtStringBuf name, const T& defvalue) const;

        /**
        * get node attribute
        * @param name: attribute name
        * @param value: return-value
        * throws exception if attribute not found
        */
        template <class T>
        void Attr(TZtStringBuf name, T& value) const;

        /**
        * get node attribute
        * @param name: attribute name
        * @param defvalue: default value
        * @param value: return-value
        * returns default value if attribute not found, attr value otherwise
        */
        template <class T>
        void Attr(TZtStringBuf name, T& value, const T& defvalue) const;

        /**
        * get node value (text)
        * @throws exception if node is blank
        */
        template <class T>
        T Value() const;

        /**
        * get node value
        * @param defvalue: default value
        * returns default value if node is blank
        */
        template <class T>
        T Value(const T& defvalue) const;

        /**
        * set node value
        * @param value: new text value
        */
        template <class T>
        typename std::enable_if<!std::is_convertible_v<T, TStringBuf>, void>::type
        SetValue(const T& value);

        void SetValue(TStringBuf value);

        /**
        * set/reset node attribute value,
        * if attribute does not exist, it'll be created
        * @param name: attribute name
        * @param value: attribute value
        */
        template<class T>
        typename std::enable_if<!std::is_convertible_v<T, TZtStringBuf>, void>::type
        SetAttr(TZtStringBuf name, const T& value);

        void SetAttr(TZtStringBuf name, TZtStringBuf value);

        void SetAttr(TZtStringBuf name);

        /**
        * delete node attribute
        * @param name: attribute name
        */
        void DelAttr(TZtStringBuf name);

        /**
        * set node application data
        * @param priv: new application data pointer
        */
        void SetPrivate(void* priv);

        /**
        * @return application data pointer, passed by SetPrivate
        */
        void* GetPrivate() const;

        /**
        * get node name
        */
        TString Name() const;

        /**
        * get node xpath
        */
        TString Path() const;

        /**
        * get node xml representation
        */
        TString ToString(TZtStringBuf enc = "") const {
            TStringStream s;
            Save(s, enc);
            return s.Str();
        }
        void Save(IOutputStream& stream, TZtStringBuf enc = "", bool shouldFormat = false) const;
        void SaveAsHtml(IOutputStream& stream, TZtStringBuf enc = "", bool shouldFormat = false) const;

        /**
        * get pointer to internal node
        */
        xmlNode* GetPtr();
        const xmlNode* GetPtr() const;

        /**
        * check if node is text-only node
        */
        bool IsText() const;

        /**
        * unlink node from parent and free
        */
        void Remove();

        /**
        * constructs null node
        */
        TNode()
            : NodePointer(nullptr)
            , DocPointer(nullptr)
        {
        }

    private:
        friend class TConstNodes;

        TNode(xmlDoc* doc, xmlNode* node)
            : NodePointer(node)
            , DocPointer(doc)
        {
        }

        TNode Find(xmlNode* start, TZtStringBuf name);

        template <class T>
        void AttrInternal(TCharPtr& value, T& res, TStringBuf errContext) const;

        void SaveInternal(IOutputStream& stream, TZtStringBuf enc, int options) const;

        xmlNode* NodePointer;
        xmlDoc* DocPointer;
    };

    class TConstNode {
    public:
        friend class TDocument;
        friend class TConstNodes;
        friend class TNode;
        /**
        * check if node is null
        */
        bool IsNull() const {
            return ActualNode.IsNull();
        }

        bool IsElementNode() const {
            return ActualNode.IsElementNode();
        }

        TConstNode Parent() const {
            return ActualNode.Parent();
        }

        /**
        * Create xpath context to be used later for fast xpath evaluation.
        * @param nss: explicitly specify XML namespaces to use and their prefixes
        */
        TXPathContextPtr CreateXPathContext(const TNamespacesForXPath& nss = TNamespacesForXPath()) const {
            return ActualNode.CreateXPathContext(nss);
        }

        /**
        * get all element nodes matching given xpath expression
        * @param xpath: xpath expression
        * @param quiet: don't throw exception if zero nodes found
        * @param ns: explicitly specify XML namespaces to use and their prefixes
        *
        * For historical reasons, this only works for *element* nodes.
        * Use the XPath function if you need other kinds of nodes.
        */
        TConstNodes Nodes(TZtStringBuf xpath, bool quiet = false, const TNamespacesForXPath& ns = TNamespacesForXPath()) const {
            return ActualNode.Nodes(xpath, quiet, ns);
        }

        /**
        * get all element nodes matching given xpath expression
        * @param xpath: xpath expression
        * @param quiet: don't throw exception if zero nodes found
        * @param ctxt: reusable xpath context
        *
        * For historical reasons, this only works for *element* nodes.
        * Use the XPath function if you need other kinds of nodes.
        */
        TConstNodes Nodes(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const {
            return ActualNode.Nodes(xpath, quiet, ctxt);
        }

        /**
        * get all nodes matching given xpath expression
        * @param xpath: xpath expression
        * @param quiet: don't throw exception if zero nodes found
        * @param ns: explicitly specify XML namespaces to use and their prefixes
        */
        TConstNodes XPath(TZtStringBuf xpath, bool quiet = false, const TNamespacesForXPath& ns = TNamespacesForXPath()) const {
            return ActualNode.XPath(xpath, quiet, ns);
        }

        /**
        * get all nodes matching given xpath expression
        * @param xpath: xpath expression
        * @param quiet: don't throw exception if zero nodes found
        * @param ctxt: reusable xpath context
        */
        TConstNodes XPath(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const {
            return ActualNode.XPath(xpath, quiet, ctxt);
        }

        /**
        * get the first element node matching given xpath expression
        * @param xpath: path to node (from current node)
        * @param quiet: don't throw exception if node not found,
        *               return null node (@see IsNull())
        * @param ns: explicitly specify XML namespaces to use and their prefixes
        *
        * For historical reasons, this only works for *element* nodes.
        * Use the XPath function if you need other kinds of nodes.
        */
        TConstNode Node(TZtStringBuf xpath, bool quiet = false, const TNamespacesForXPath& ns = TNamespacesForXPath()) const {
            return ActualNode.Node(xpath, quiet, ns);
        }

        /**
        * get the first element node matching given xpath expression
        * @param xpath: path to node (from current node)
        * @param quiet: don't throw exception if node not found,
        *               return null node (@see IsNull())
        * @param ctxt: reusable xpath context
        *
        * For historical reasons, this only works for *element* nodes.
        * Use the XPath function if you need other kinds of nodes.
        */
        TConstNode Node(TZtStringBuf xpath, bool quiet, TXPathContext& ctxt) const {
            return ActualNode.Node(xpath, quiet, ctxt);
        }

        TConstNode FirstChild(TZtStringBuf name) const {
            return ActualNode.FirstChild(name);
        }

        TConstNode FirstChild() const {
            return ActualNode.FirstChild();
        }

        /**
        * get node neighbour
        * @param name: neighbour name
        * throws exception if no neighbour found
        */
        TConstNode NextSibling(TZtStringBuf name) const {
            return ActualNode.NextSibling(name);
        }

        TConstNode NextSibling() const {
            return ActualNode.NextSibling();
        }

        /**
        * get node attribute
        * @param name: attribute name
        * throws exception if attribute not found
        */
        template <class T>
        T Attr(TZtStringBuf name) const {
            return ActualNode.Attr<T>(name);
        }

        /**
        * get node attribute
        * @param name: attribute name
        * returns default value if attribute not found
        */
        template <class T>
        T Attr(TZtStringBuf name, const T& defvalue) const {
            return ActualNode.Attr(name, defvalue);
        }

        /**
        * get node attribute
        * @param name: attribute name
        * @param value: return-value
        * throws exception if attribute not found
        */
        template <class T>
        void Attr(TZtStringBuf name, T& value) const {
            return ActualNode.Attr(name, value);
        }

        /**
        * get node attribute
        * @param name: attribute name
        * @param defvalue: default value
        * @param value: return-value
        * returns default value if attribute not found, attr value otherwise
        */
        template <class T>
        void Attr(TZtStringBuf name, T& value, const T& defvalue) const {
            return ActualNode.Attr(name, value, defvalue);
        }

        /**
        * get node value (text)
        * @throws exception if node is blank
        */
        template <class T>
        T Value() const {
            return ActualNode.Value<T>();
        }

        /**
        * get node value
        * @param defvalue: default value
        * returns default value if node is blank
        */
        template <class T>
        T Value(const T& defvalue) const {
            return ActualNode.Value(defvalue);
        }

        /**
        * get node name
        */
        TString Name() const {
            return ActualNode.Name();
        }

        /**
        * @return application data pointer, passed by SetPrivate
        */
        void* GetPrivate() const {
            return ActualNode.GetPrivate();
        }

        /**
        * get pointer to internal node
        */
        const xmlNode* GetPtr() const {
            return ActualNode.GetPtr();
        }

        /**
        * check if node is text-only node
        */
        bool IsText() const {
            return ActualNode.IsText();
        }

        /**
        * get node xpath
        */
        TString Path() const {
            return ActualNode.Path();
        }

        /**
        * get node xml representation
        */
        TString ToString(TZtStringBuf enc = "") const {
            return ActualNode.ToString(enc);
        }

        TConstNode() = default;
        TConstNode(TNode node)
            : ActualNode(node)
        {
        }

        TNode ConstCast() const {
            return ActualNode;
        }

    private:
        TNode ActualNode;
    };

}
