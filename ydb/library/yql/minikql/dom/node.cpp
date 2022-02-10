#include "node.h"

#include <util/generic/algorithm.h>

namespace NYql::NDom {

namespace {

inline bool StringLess(const TPair& x, const TPair& y) {
    return x.first.AsStringRef() < y.first.AsStringRef();
}

inline bool StringRefLess(const TPair& x, const TStringRef& y) {
    return x.first.AsStringRef() < y;
}

inline bool StringEquals(const TPair& x, const TPair& y) {
    return x.first.AsStringRef() == y.first.AsStringRef();
}

}

template <bool NoSwap>
TMapNode::TIterator<NoSwap>::TIterator(const TMapNode* parent)
    : Parent(const_cast<TMapNode*>(parent))
    , Index(-1)
{}

template <bool NoSwap>
bool TMapNode::TIterator<NoSwap>::Skip() {
    if (Index + 1 == Parent->UniqueCount_) {
        return false;
    }

    ++Index;
    return true;
}

template <bool NoSwap>
bool TMapNode::TIterator<NoSwap>::Next(TUnboxedValue& key) {
    if (!Skip())
        return false;
    if constexpr (NoSwap) {
        key = Parent->Items_[Index].first;
    } else {
        key = Parent->Items_[Index].second;
    }
    return true;
}

template <bool NoSwap>
bool TMapNode::TIterator<NoSwap>::NextPair(TUnboxedValue& key, TUnboxedValue& payload) {
    if (!Next(key))
        return false;
    if constexpr (NoSwap) {
        payload = Parent->Items_[Index].second;
    } else {
        payload = Parent->Items_[Index].first;
    }
    return true;
}

TMapNode::TMapNode(TMapNode&& src)
    : Count_(src.Count_), UniqueCount_(src.UniqueCount_), Items_(src.Items_)
{
    src.Count_ = src.UniqueCount_ = 0U;
    src.Items_ = nullptr;
}

TMapNode::TMapNode(const TPair* items, ui32 count)
    : Count_(count)
    , Items_((TPair*)UdfAllocateWithSize(sizeof(TPair) * count))
{
    std::memset(Items_, 0, sizeof(TPair) * count);
    for (ui32 i = 0; i < count; ++i) {
        Items_[i] = std::move(items[i]);
    }

    StableSort(Items_, Items_ + count, StringLess);
    UniqueCount_ = Unique(Items_, Items_ + count, StringEquals) - Items_;
    for (ui32 i = UniqueCount_; i < count; ++i) {
        Items_[i].first.Clear();
        Items_[i].second.Clear();
    }
}

TMapNode::~TMapNode() {
    for (ui32 i = 0; i < UniqueCount_; ++i) {
        Items_[i].first.Clear();
        Items_[i].second.Clear();
    }

    UdfFreeWithSize(Items_, sizeof(TPair) * Count_);
}

ui64 TMapNode::GetDictLength() const {
    return UniqueCount_;
}

TUnboxedValue TMapNode::GetDictIterator() const {
    return TUnboxedValuePod(new TIterator<true>(this));
}

TUnboxedValue TMapNode::GetKeysIterator() const {
    return TUnboxedValuePod(new TIterator<true>(this));
}

TUnboxedValue TMapNode::GetPayloadsIterator() const {
    return TUnboxedValuePod(new TIterator<false>(this));
}

bool TMapNode::Contains(const TUnboxedValuePod& key) const {
    return BinarySearch(Items_, Items_ + UniqueCount_, std::make_pair(key, TUnboxedValuePod()), StringLess);
}

TUnboxedValue TMapNode::Lookup(const TUnboxedValuePod& key) const {
    return Lookup(key.AsStringRef());
}

TUnboxedValue TMapNode::Lookup(const TStringRef& key) const {
    const auto it = LowerBound(Items_, Items_ + UniqueCount_, key, StringRefLess);
    if (it == Items_ + UniqueCount_ || static_cast<TStringBuf>(it->first.AsStringRef()) != static_cast<TStringBuf>(key))
        return {};

    return it->second;
}

bool TMapNode::HasDictItems() const {
    return UniqueCount_ > 0ULL;
}

bool TMapNode::IsSortedDict() const {
    return true;
}

void* TMapNode::GetResource() {
    return Items_;
}

TAttrNode::TAttrNode(const TUnboxedValue& map, TUnboxedValue&& value)
    : TMapNode(std::move(*static_cast<TMapNode*>(map.AsBoxed().Get()))), Value_(std::move(value))
{}

TAttrNode::TAttrNode(TUnboxedValue&& value, const TPair* items, ui32 count)
    : TMapNode(items, count), Value_(std::move(value))
{}

TUnboxedValue TAttrNode::GetVariantItem() const {
    return Value_;
}

TDebugPrinter::TDebugPrinter(const TUnboxedValuePod& node)
    : Node(node)
{}

IOutputStream& TDebugPrinter::Out(IOutputStream &o) const {
    switch (GetNodeType(Node)) {
        case ENodeType::Entity:
            o << "entity (#)";
            break;
        case ENodeType::Bool:
            o << "boolean (" << (Node.Get<bool>() ? "true" : "false") << ") value";
            break;
        case ENodeType::Int64:
            o << "integer (" << Node.Get<i64>() << ") value";
            break;
        case ENodeType::Uint64:
            o << "unsigned integer (" << Node.Get<ui64>() << ") value";
            break;
        case ENodeType::Double:
            o << "floating point (" << Node.Get<double>() << ") value";
            break;
        case ENodeType::String:
            if (const std::string_view str(Node.AsStringRef()); str.empty())
                o << "empty string";
            else if(Node.IsEmbedded() && str.cend() == std::find_if(str.cbegin(), str.cend(), [](char c){ return !std::isprint(c); }))
                o << "string '" << str << "' value";
            else
                o << "string value of size " << str.size();
            break;
        case ENodeType::List:
            if (Node.IsBoxed())
                o << "list of size " << Node.GetListLength();
            else
                o << "empty list";
            break;
        case ENodeType::Dict:
            if (Node.IsBoxed())
                o << "dict of size " << Node.GetDictLength();
            else
                o << "empty dict";
            break;
        case ENodeType::Attr:
            return TDebugPrinter(Node.GetVariantItem()).Out(o);
        default:
            o << "invalid node";
            break;
    }
    return o;
}

}
