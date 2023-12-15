#pragma once

#include <util/generic/yexception.h>
#include <util/system/compiler.h>
#include <util/system/yassert.h>
#include <util/stream/str.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <memory>
#include <optional>

#define ENSURE_NODE_NOT_EMPTY(NODE) Y_ENSURE_EX(NODE, TFyamlEx() << "Expected non-empty Node")
#define ENSURE_DOCUMENT_NOT_EMPTY(NODE) Y_ENSURE_EX(NODE, TFyamlEx() << "Expected non-empty Document")

struct fy_parser;
struct fy_node;
struct fy_document;
struct fy_diag;
struct fy_document_iterator;
struct fy_node_pair;
extern "C" struct fy_node *fy_node_buildf(struct fy_document *fyd, const char *fmt, ...);

namespace NKikimr::NFyaml {
    namespace NDetail {
        template <class T>
        class TNodeOps;
    }
    class TNodeRef;
    class TNode;
}

bool operator==(const fy_node* node1, const NKikimr::NFyaml::NDetail::TNodeOps<NKikimr::NFyaml::TNodeRef>& node2);
bool operator==(const fy_node* node1, const NKikimr::NFyaml::NDetail::TNodeOps<NKikimr::NFyaml::TNode>& node2);

namespace NKikimr::NFyaml {

struct TStringPtrHashT {
    size_t operator()(const TSimpleSharedPtr<TString>& str) const {
        return (size_t)str.Get();
    }
};

// do TFyaml(str) instead of TFyaml() << str;
class TFyamlEx : public yexception {
public:
    TFyamlEx() {}

    TFyamlEx(TString error) : Errors_({error}) {}

    TFyamlEx(std::initializer_list<TString> errors) : Errors_(errors) {}

    const TVector<TString>& Errors() {
        return Errors_;
    }

    const char* what() const noexcept override {
        What_ = TString(yexception::what());
        for (auto& err : Errors_) {
            What_.push_back('\n');
            What_.append(err);
        }

        return What_.c_str();
    }

    TFyamlEx& AddError(TString error) {
        Errors_.push_back(error);
        return *this;
    }

    TStringBuf AsStrBuf() const {
        return what();
    }

private:
    TVector<TString> Errors_;
    mutable TString What_;
};

enum class ENodeType {
    Scalar,
    Sequence,
    Mapping,
};

enum class ENodeStyle {
    Any = -1,
    Flow,
    Block,
    Plain,
    SingleQuoted,
    DoubleQuoted,
    Literal,
    Folded,
    Alias,
};

class TNodeRef;
class TDocumentIterator;
class TDocument;
class TNode;
class TMappingIterator;
class TReverseMappingIterator;
class TMapping;
class TSequenceIterator;
class TReverseSequenceIterator;
class TSequence;
class TJsonEmitter;
class TParser;
struct TMark;

namespace NDetail {

class IBasicUserData {
public:
    virtual ~IBasicUserData() = default;
};

template <class T>
class TUserDataHolder : public IBasicUserData {
public:
    TUserDataHolder(IBasicUserData* next, T* data)
        : Next_(next)
        , Data_(data)
    {}

private:
    std::unique_ptr<IBasicUserData> Next_ = nullptr;
    std::unique_ptr<T> Data_ = nullptr;
};

void ThrowAllExceptionsIfAny(fy_diag* diag);

void RethrowError(fy_diag* diag);

void RethrowOnError(bool isError, fy_node* node);

void RethrowOnError(bool isError, fy_node_pair* pair);

void RethrowOnError(bool isError, fy_diag* diag);

void FreeChar(char* mem);

bool IsComplexType(ENodeType type);

class TNodeOpsBase {
protected:
    TString Path(fy_node* node) const;

    ENodeType Type(fy_node* node) const;

    fy_node* Copy(fy_node* node) const;

    fy_node* Copy(fy_node* node, fy_document* to) const;

    bool IsAlias(fy_node* node) const;

    fy_node* ResolveAlias(fy_node* node) const;

    fy_node* CreateReference(fy_node* node) const;

    fy_node* Sequence(fy_node* node) const;

    fy_node* Map(fy_node* node) const;

    TString Scalar(fy_node* node) const;

    TMark BeginMark(fy_node* node) const;

    TMark EndMark(fy_node* node) const;

    void Insert(fy_node* thisNode, fy_node* node);

    std::optional<TString> Tag(fy_node* node) const;

    void SetTag(fy_node* node, const TString& tag);

    bool RemoveTag(fy_node* node);

    bool HasAnchor(fy_node* node) const;

    void SetAnchor(fy_node* node, const TString& anchor);

    bool DeepEqual(fy_node* thisNode, fy_node* other);

    std::unique_ptr<char, void(*)(char*)> EmitToCharArray(fy_node* node) const;

    void SetStyle(fy_node* node, ENodeStyle style);

    ENodeStyle Style(fy_node* node) const;

protected:
    void SetUserData(fy_node* node, NDetail::IBasicUserData* data);

    NDetail::IBasicUserData* UserData(fy_node* node) const;

    void ClearUserData(fy_node* node);
};

template <class T>
class TNodeOps : public TNodeOpsBase {
friend class ::NKikimr::NFyaml::TNodeRef;

public:
    template <class OtherT>
    bool operator==(const TNodeOps<OtherT>& other) const { return Node() == other.Node(); }

    bool operator==(const fy_node* node) const { return Node() == node; }

    friend bool ::operator==(const fy_node* node1, const TNodeOps<NKikimr::NFyaml::TNodeRef>& node2);
    friend bool ::operator==(const fy_node* node1, const TNodeOps<NKikimr::NFyaml::TNode>& node2);

    explicit operator bool() const { return Node() != nullptr; }

    TString Path() const;

    ENodeType Type() const;

    TNode Copy() const;

    TNode Copy(TDocument& to) const;

    bool IsAlias() const;

    TNodeRef ResolveAlias() const;

    TNode CreateReference() const;

    TSequence Sequence() const;

    TMapping Map() const;

    TString Scalar() const;

    TMark BeginMark() const;

    TMark EndMark() const;

    void Insert(const TNodeRef& node);

    bool Empty() const { return Node() == nullptr; }

    std::optional<TString> Tag() const;

    void SetTag(const TString& tag);

    bool RemoveTag();

    bool HasAnchor() const;

    void SetAnchor(const TString& anchor);

    bool DeepEqual(const TNodeRef& other);

    std::unique_ptr<char, void(*)(char*)> EmitToCharArray() const;

    void SetStyle(ENodeStyle style);

    ENodeStyle Style() const;

protected:
    const T& AsDerived() const;

    fy_node* Node() const;

    fy_node* Node();

    void SetUserData(NDetail::IBasicUserData* data);

    NDetail::IBasicUserData* UserData() const;

    void ClearUserData();
};

} // namespace NDetail

class TDocumentIterator {
    friend class TDocument;
public:
    explicit TDocumentIterator(fy_document_iterator* iterator = nullptr);

protected:
    std::unique_ptr<fy_document_iterator, void(*)(fy_document_iterator*)> Iterator_;
};

class TNodeRef : public NDetail::TNodeOps<TNodeRef> {
    friend class TNodeOps<TNodeRef>;
    friend class TNodeOpsBase;
    friend class TDocument;
    friend class TDocumentNodeIterator;
    friend class TMapping;
    friend class TMappingIterator;
    friend class TReverseMappingIterator;
    friend class TNodePairRef;
    friend class TSequence;
    friend class TSequenceIterator;
    friend class TReverseSequenceIterator;
    friend class TJsonEmitter;

public:
    TNodeRef() = default;

    template <class T>
    TNodeRef(const TNodeOps<T>& other) : Node_(other.Node()) {}

    explicit TNodeRef(const TNodeRef& other) : Node_(other.Node_) {}

    TNodeRef(fy_node* node);

    TNodeRef& operator=(const TNodeRef& other) { Node_ = other.Node_; return *this; }

    TNodeRef& operator=(fy_node* node) { Node_ = node; return *this; }

protected:
    fy_node* Node_ = nullptr;

private:
    fy_node* NodeRawPointer() const;
};

class TNode : public NDetail::TNodeOps<TNode> {
    friend class TNodeOps<TNode>;

public:
    TNode(fy_node* node = nullptr);

    template <class T>
    explicit TNode(const TNodeOps<T>& other) : Node_(other.Node_) {}

    TNodeRef Ref() { return TNodeRef(*this); }

private:
    std::shared_ptr<fy_node> Node_;

    TNode& operator=(fy_node* node);

    fy_node* NodeRawPointer() const {
        return Node_.get();
    }
};

class TNodePairRef {
friend class TMappingIterator;
friend class TReverseMappingIterator;
friend class TMapping;

public:
    TNodePairRef(fy_node_pair* pair = nullptr) : Pair_(pair) {}

    bool operator==(const TNodePairRef& other) const { return Pair_ == other.Pair_; }

    explicit operator bool() const { return Pair_ != nullptr; }

    TNodeRef Key() const;

    int Index(const TNodeRef& node) const;

    void SetKey(const TNodeRef& node);

    TNodeRef Value() const;

    void SetValue(const TNodeRef& node);

private:
    fy_node_pair* Pair_ = nullptr;
};

class TMappingIterator {
    friend class TMapping;
public:
    explicit TMappingIterator(const TNodeRef& node, bool end = false);

    TMappingIterator(const TMappingIterator& other) {
        Node_ = other.Node_;
        NodePair_ = other.NodePair_;
    }

    TMappingIterator& operator=(const TMappingIterator& other) {
        Node_ = other.Node_;
        NodePair_ = other.NodePair_;
        return *this;
    }

    TMappingIterator& operator++();

    const TNodePairRef* operator->() const { return &NodePair_; }

    TMappingIterator operator++(int) {
        TMappingIterator retval = *this;
        ++(*this);
        return retval;
    }

    bool operator==(TMappingIterator other) const { return Node_ == other.Node_ && NodePair_ == other.NodePair_; }

    const TNodePairRef& operator*() const { return NodePair_; }

private:
    TNodeRef Node_;
    TNodePairRef NodePair_;
};

class TReverseMappingIterator {
    friend class TMapping;
public:
    explicit TReverseMappingIterator(const TNodeRef& node, bool end = false);

    TReverseMappingIterator(const TReverseMappingIterator& other) {
        Node_ = other.Node_;
        NodePair_ = other.NodePair_;
    }

    TReverseMappingIterator& operator=(const TReverseMappingIterator& other) {
        Node_ = other.Node_;
        NodePair_ = other.NodePair_;
        return *this;
    }

    TReverseMappingIterator& operator++();

    const TNodePairRef* operator->() const { return &NodePair_; }

    TReverseMappingIterator operator++(int) {
        TReverseMappingIterator retval = *this;
        ++(*this);
        return retval;
    }

    bool operator==(TReverseMappingIterator other) const { return Node_ == other.Node_ && NodePair_ == other.NodePair_; }

    bool operator!=(TReverseMappingIterator other) const { return !(*this == other); }

    const TNodePairRef& operator*() const { return NodePair_; }

private:
    TNodeRef Node_;
    TNodePairRef NodePair_;
};

class TMapping : public TNodeRef {
public:
    template <class T>
    explicit TMapping(const TNodeOps<T>& node)
        : TNodeRef(node)
    {
        Y_DEBUG_ABORT_UNLESS(Type() == ENodeType::Mapping);
    }

    TMappingIterator begin() const {
        return TMappingIterator(*this);
    }

    TMappingIterator end() const {
        return TMappingIterator(*this, true);
    }

    TReverseMappingIterator rbegin() const {
        return TReverseMappingIterator(*this);
    }

    TReverseMappingIterator rend() const {
        return TReverseMappingIterator(*this, true);
    }

    size_t size() const;

    size_t empty() const;

    TNodePairRef at(int index) const;

    TNodePairRef operator[](int index) const;

    TNodeRef at(const TString& index) const;

    TNodePairRef pair_at(const TString& index) const;

    TNodePairRef pair_at_opt(const TString& index) const;

    TNodeRef operator[](const TString& index) const;

    TNodeRef operator[](const char* str) const;

    void Append(const TNodeRef& key, const TNodeRef& value);

    void Prepend(const TNodeRef& key, const TNodeRef& value);

    void Remove(const TNodePairRef& toRemove);

    bool Has(TString key) const;

    TMappingIterator Remove(const TMappingIterator& toRemove);

    void Remove(const TNodeRef& key);
};

class TSequenceIterator {
    friend class TSequence;
public:
    explicit TSequenceIterator(const TNodeRef& node, bool end = false);

    TSequenceIterator(const TSequenceIterator& other) {
        Node_ = other.Node_;
        IterNode_ = other.IterNode_;
        Iter_ = other.Iter_;
    }

    TSequenceIterator& operator=(const TSequenceIterator& other) {
        Node_ = other.Node_;
        IterNode_ = other.IterNode_;
        Iter_ = other.Iter_;
        return *this;
    }

    TSequenceIterator& operator++();

    const TNodeRef* operator->() const {
        return &IterNode_;
    }

    TSequenceIterator operator++(int) {
        TSequenceIterator retval = *this;
        ++(*this);
        return retval;
    }

    bool operator==(TSequenceIterator other) const { return Node_ == other.Node_ && Iter_ == other.Iter_; }

    bool operator!=(TSequenceIterator other) const { return !(*this == other); }

    const TNodeRef& operator*() const { return IterNode_; }

    void InsertBefore(const TNodeRef& node);

    void InsertAfter(const TNodeRef& node);

private:
    TNodeRef Node_;
    TNodeRef IterNode_;
    void* Iter_ = nullptr;
};

class TReverseSequenceIterator {
    friend class TSequence;
public:
    explicit TReverseSequenceIterator(const TNodeRef& node, bool end = false);

    TReverseSequenceIterator(const TReverseSequenceIterator& other) {
        Node_ = other.Node_;
        IterNode_ = other.IterNode_;
        Iter_ = other.Iter_;
    }

    TReverseSequenceIterator& operator=(const TReverseSequenceIterator& other) {
        Node_ = other.Node_;
        IterNode_ = other.IterNode_;
        Iter_ = other.Iter_;
        return *this;
    }

    TReverseSequenceIterator& operator++();

    const TNodeRef* operator->() const {
        return &IterNode_;
    }

    TReverseSequenceIterator operator++(int) {
        TReverseSequenceIterator retval = *this;
        ++(*this);
        return retval;
    }

    bool operator==(TReverseSequenceIterator other) const { return Node_ == other.Node_ && Iter_ == other.Iter_; }

    bool operator!=(TReverseSequenceIterator other) const { return !(*this == other); }

    const TNodeRef& operator*() const { return IterNode_; }

    void InsertBefore(const TNodeRef& node);

    void InsertAfter(const TNodeRef& node);

private:
    TNodeRef Node_;
    TNodeRef IterNode_;
    void* Iter_ = nullptr;
};

class TSequence : public TNodeRef {
public:
    explicit TSequence(const TNodeRef& node)
        : TNodeRef(node)
    {
        Y_DEBUG_ABORT_UNLESS(Type() == ENodeType::Sequence);
    }

    TSequenceIterator begin() const {
        return TSequenceIterator(*this);
    }

    TSequenceIterator end() const {
        return TSequenceIterator(*this, true);
    }

    TReverseSequenceIterator rbegin() const {
        return TReverseSequenceIterator(*this);
    }

    TReverseSequenceIterator rend() const {
        return TReverseSequenceIterator(*this, true);
    }

    size_t size() const;

    size_t empty() const;

    TNodeRef at(int index) const;

    TNodeRef operator[](int index) const;

    void Append(const TNodeRef& node);

    void Prepend(const TNodeRef& node);

    void InsertBefore(const TNodeRef& mark, const TNodeRef& node);

    void InsertAfter(const TNodeRef& mark, const TNodeRef& node);

    TNode Remove(const TNodeRef& toRemove);

    TSequenceIterator Remove(const TSequenceIterator& toRemove);

    TReverseSequenceIterator Remove(const TReverseSequenceIterator& toRemove);
};

class TDocumentNodeIterator
    : public TDocumentIterator
{
public:
    explicit TDocumentNodeIterator(TNodeRef&& node);

    TDocumentNodeIterator(const TDocumentNodeIterator& other)
        : TDocumentIterator(other.Iterator_.get())
        , Node_(other.Node_)
    {}

    TDocumentNodeIterator& operator=(const TDocumentNodeIterator& other) {
        Iterator_.reset(other.Iterator_.get());
        Node_ = other.Node_;
        return *this;
    }

    TDocumentNodeIterator& operator++();

    TNodeRef* operator->() {
        return &Node_;
    }

    TDocumentNodeIterator operator++(int) {
        TDocumentNodeIterator retval = *this;
        ++(*this);
        return retval;
    }

    bool operator==(TDocumentNodeIterator other) const { return Node_ == other.Node_; }

    bool operator!=(TDocumentNodeIterator other) const { return !(*this == other); }

    TNodeRef& operator*() { return Node_; }

private:
    TNodeRef Node_;
};

class TDocument {
    friend class NDetail::TNodeOps<TNodeRef>;
    friend class NDetail::TNodeOps<TNode>;
    friend class TParser;

    explicit TDocument(TString str, fy_document* doc = nullptr, fy_diag* diag = nullptr);
    explicit TDocument(fy_document* doc = nullptr, fy_diag* diag = nullptr);

public:
    TDocument(TDocument&& other)
        : Document_(std::move(other.Document_))
        , Diag_(std::move(other.Diag_))
    {}

    static TDocument Parse(TString cstr);

    TDocument Clone() const;

    template <class... Args>
    size_t Scanf(const char* fmt, Args&& ...args) {
        Y_DEBUG_ABORT_UNLESS(Document_);
        return fy_document_scanf(Document_.get(), fmt, std::forward<Args>(args)...);
    }

    void InsertAt(const char* path, const TNodeRef& node);

    template <class... Args>
    TNodeRef Buildf(const char* fmt, Args&& ...args) {
        Y_DEBUG_ABORT_UNLESS(Document_);
        return fy_node_buildf(Document_.get(), fmt, std::forward<Args>(args)...);
    }

    TNodeRef Buildf(const char* content);

    void Resolve();

    bool HasDirectives();

    bool HasExplicitDocumentStart();

    bool HasExplicitDocumentEnd();

    void SetParent(const TDocument& doc);

    TNodeRef Root();

    void SetRoot(const TNodeRef& node);

    TDocumentNodeIterator begin() {
        auto it = TDocumentNodeIterator(Root());
        ++it;
        return it;
    }

    TDocumentNodeIterator end() {
        return TDocumentNodeIterator(TNodeRef(nullptr));
    }

    TNodeRef CreateAlias(const TString& name);

    std::unique_ptr<char, void(*)(char*)> EmitToCharArray() const;

    TMark BeginMark() const;

    TMark EndMark() const;

private:
    std::unique_ptr<fy_document, void(*)(fy_document*)> Document_;
    std::unique_ptr<fy_diag, void(*)(fy_diag*)> Diag_;

    static void DestroyUserData(fy_node *fyn, void *meta, void *user) {
        Y_UNUSED(fyn);
        Y_UNUSED(user);
        if (meta) {
            auto* data = reinterpret_cast<NDetail::IBasicUserData*>(meta);
            delete data;
        }
    }

    static void DestroyDocumentStrings(fy_document *fyd, void *user) {
        Y_UNUSED(fyd);
        if (user) {
            auto* data = reinterpret_cast<THashSet<TSimpleSharedPtr<TString>, TStringPtrHashT>*>(user);
            delete data;
        }
    }

    bool RegisterUserDataCleanup();
    void UnregisterUserDataCleanup();
};

class TJsonEmitter {
public:
    explicit TJsonEmitter(TDocument& doc) : Node_(doc.Root()) {}
    explicit TJsonEmitter(const TNodeRef& node) : Node_(node) {}

    std::unique_ptr<char, void(*)(char*)> EmitToCharArray() const;

private:
    const TNodeRef Node_;
};

class TParser {
    TParser(TString rawStream, fy_parser* doc, fy_diag* diag);
public:
    static TParser Create(TString str);

    std::optional<TDocument> NextDocument();
private:
    TString RawDocumentStream_;
    std::unique_ptr<fy_parser, void(*)(fy_parser*)> Parser_;
    std::unique_ptr<fy_diag, void(*)(fy_diag*)> Diag_;
};

struct TMark {
    size_t InputPos;
    int Line;
    int Column;
};

namespace NDetail {

template <class T>
TNode TNodeOps<T>::CreateReference() const {
    return TNode(TNodeOpsBase::CreateReference(Node()));
}

template <class T>
TNode TNodeOps<T>::Copy() const {
    return TNode(TNodeOpsBase::Copy(Node()));
}

template <class T>
TNode TNodeOps<T>::Copy(TDocument& to) const {
    return TNode(TNodeOpsBase::Copy(Node(), to.Document_.get()));
}

template <class T>
TString TNodeOps<T>::Path() const {
    return TNodeOpsBase::Path(Node());
}

template <class T>
ENodeType TNodeOps<T>::Type() const {
    return TNodeOpsBase::Type(Node());
}

template <class T>
bool TNodeOps<T>::IsAlias() const {
    return TNodeOpsBase::IsAlias(Node());
}

template <class T>
TNodeRef TNodeOps<T>::ResolveAlias() const {
    return TNodeRef(TNodeOpsBase::ResolveAlias(Node()));
}

template <class T>
TString TNodeOps<T>::Scalar() const {
    return TNodeOpsBase::Scalar(Node());
}

template <class T>
TMark TNodeOps<T>::BeginMark() const {
    return TNodeOpsBase::BeginMark(Node());
}

template <class T>
TMark TNodeOps<T>::EndMark() const {
    return TNodeOpsBase::EndMark(Node());
}

template <class T>
TMapping TNodeOps<T>::Map() const {
    return TMapping(TNodeRef(TNodeOpsBase::Map(Node())));
}

template <class T>
TSequence TNodeOps<T>::Sequence() const {
    return TSequence(TNodeRef(TNodeOpsBase::Sequence(Node())));
}

template <class T>
void TNodeOps<T>::Insert(const TNodeRef& node) {
    return TNodeOpsBase::Insert(Node(), node.Node_);
}

template <class T>
std::optional<TString> TNodeOps<T>::Tag() const {
    return TNodeOpsBase::Tag(Node());
}

template <class T>
void TNodeOps<T>::SetTag(const TString& tag) {
    return TNodeOpsBase::SetTag(Node(), tag);
}

template <class T>
bool TNodeOps<T>::RemoveTag() {
    return TNodeOpsBase::RemoveTag(Node());
}

template <class T>
bool TNodeOps<T>::HasAnchor() const {
    return TNodeOpsBase::HasAnchor(Node());
}

template <class T>
void TNodeOps<T>::SetAnchor(const TString& anchor) {
    return TNodeOpsBase::SetAnchor(Node(), anchor);
}

template <class T>
bool TNodeOps<T>::DeepEqual(const TNodeRef& other) {
    return TNodeOpsBase::DeepEqual(Node(), other.Node_);
}

template <class T>
std::unique_ptr<char, void(*)(char*)> TNodeOps<T>::EmitToCharArray() const {
    return TNodeOpsBase::EmitToCharArray(Node());
}

template <class T>
void TNodeOps<T>::SetStyle(ENodeStyle style) {
    return TNodeOpsBase::SetStyle(Node(), style);
}

template <class T>
ENodeStyle TNodeOps<T>::Style() const {
    return TNodeOpsBase::Style(Node());
}

template <class T>
const T& TNodeOps<T>::AsDerived() const {
    return static_cast<const T&>(*this);
}

template <class T>
fy_node* TNodeOps<T>::Node() const {
    return AsDerived().NodeRawPointer();
}

template <class T>
fy_node* TNodeOps<T>::Node() {
    return AsDerived().NodeRawPointer();
}

template <class T>
void TNodeOps<T>::SetUserData(IBasicUserData* data) {
    return TNodeOpsBase::SetUserData(Node(), data);
}

template <class T>
IBasicUserData* TNodeOps<T>::UserData() const {
    return TNodeOpsBase::UserData(Node());
}

template <class T>
void TNodeOps<T>::ClearUserData() {
    return TNodeOpsBase::ClearUserData(Node());
}

} // namespace NDetail

} // namesapce NKikimr::NFyaml
