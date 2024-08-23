#include "yql_constraint.h"
#include "yql_expr.h"

#include <util/digest/murmur.h>
#include <util/generic/utility.h>
#include <util/generic/algorithm.h>
#include <util/string/join.h>

#include <algorithm>
#include <iterator>

namespace NYql {

TConstraintNode::TConstraintNode(TExprContext& ctx, std::string_view name)
    : Hash_(MurmurHash<ui64>(name.data(), name.size()))
    , Name_(ctx.AppendString(name))
{
}

TConstraintNode::TConstraintNode(TConstraintNode&& constr)
    : Hash_(constr.Hash_)
    , Name_(constr.Name_)
{
    constr.Hash_ = 0;
    constr.Name_ = {};
}

void TConstraintNode::Out(IOutputStream& out) const {
    out.Write(Name_);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TPartOfConstraintBase::TPartOfConstraintBase(TExprContext& ctx, std::string_view name)
    : TConstraintNode(ctx, name)
{}

TConstraintWithFieldsNode::TConstraintWithFieldsNode(TExprContext& ctx, std::string_view name)
    : TPartOfConstraintBase(ctx, name)
{}

const TTypeAnnotationNode* TPartOfConstraintBase::GetSubTypeByPath(const TPathType& path, const TTypeAnnotationNode& type) {
    if (path.empty() && ETypeAnnotationKind::Optional != type.GetKind())
        return &type;

    const auto tail = [](const TPathType& path) {
        auto p(path);
        p.pop_front();
        return p;
    };
    switch (type.GetKind()) {
        case ETypeAnnotationKind::Optional:
            return GetSubTypeByPath(path, *type.Cast<TOptionalExprType>()->GetItemType());
        case ETypeAnnotationKind::List: // TODO: Remove later: temporary stub for single AsList in FlatMap and same cases.
            return GetSubTypeByPath(path, *type.Cast<TListExprType>()->GetItemType());
        case ETypeAnnotationKind::Struct:
            if (const auto itemType = type.Cast<TStructExprType>()->FindItemType(path.front()))
                return GetSubTypeByPath(tail(path), *itemType);
            break;
        case ETypeAnnotationKind::Tuple:
            if (const auto index = TryFromString<ui64>(TStringBuf(path.front())))
                if (const auto typleType = type.Cast<TTupleExprType>(); typleType->GetSize() > *index)
                    return GetSubTypeByPath(tail(path), *typleType->GetItems()[*index]);
            break;
        case ETypeAnnotationKind::Multi:
            if (const auto index = TryFromString<ui64>(TStringBuf(path.front())))
                if (const auto multiType = type.Cast<TMultiExprType>(); multiType->GetSize() > *index)
                    return GetSubTypeByPath(tail(path), *multiType->GetItems()[*index]);
            break;
        case ETypeAnnotationKind::Variant:
            return GetSubTypeByPath(path, *type.Cast<TVariantExprType>()->GetUnderlyingType());
        case ETypeAnnotationKind::Dict:
            if (const auto index = TryFromString<ui8>(TStringBuf(path.front())))
                switch (*index) {
                    case 0U: return GetSubTypeByPath(tail(path), *type.Cast<TDictExprType>()->GetKeyType());
                    case 1U: return GetSubTypeByPath(tail(path), *type.Cast<TDictExprType>()->GetPayloadType());
                    default: break;
                }
            break;
        default:
            break;
    }
    return nullptr;
}

bool TPartOfConstraintBase::HasDuplicates(const TSetOfSetsType& sets) {
    for (auto ot = sets.cbegin(); sets.cend() != ot; ++ot) {
        for (auto it = sets.cbegin(); sets.cend() != it; ++it) {
            if (ot->size() < it->size() && std::all_of(ot->cbegin(), ot->cend(), [it](const TPathType& path) { return it->contains(path); }))
                return true;
        }
    }
    return false;
}

NYT::TNode TPartOfConstraintBase::PathToNode(const TPartOfConstraintBase::TPathType& path) {
    if (1U == path.size())
        return TStringBuf(path.front());

    return std::accumulate(path.cbegin(), path.cend(),
        NYT::TNode::CreateList(),
        [](NYT::TNode node, std::string_view p) -> NYT::TNode { return std::move(node).Add(TStringBuf(p)); }
    );
};

NYT::TNode TPartOfConstraintBase::SetToNode(const TPartOfConstraintBase::TSetType& set, bool withShortcut) {
    if (withShortcut && 1U == set.size() && 1U == set.front().size())
        return TStringBuf(set.front().front());

    return std::accumulate(set.cbegin(), set.cend(),
        NYT::TNode::CreateList(),
        [](NYT::TNode node, const TPathType& path) -> NYT::TNode { return std::move(node).Add(PathToNode(path)); }
    );
};

NYT::TNode TPartOfConstraintBase::SetOfSetsToNode(const TPartOfConstraintBase::TSetOfSetsType& sets) {
    return std::accumulate(sets.cbegin(), sets.cend(),
        NYT::TNode::CreateList(),
        [](NYT::TNode node, const TSetType& s) {
            return std::move(node).Add(TPartOfConstraintBase::SetToNode(s, true));
        });
}

TPartOfConstraintBase::TPathType TPartOfConstraintBase::NodeToPath(TExprContext& ctx, const NYT::TNode& node) {
    if (node.IsString())
        return TPartOfConstraintBase::TPathType{ctx.AppendString(node.AsString())};

    TPartOfConstraintBase::TPathType path;
    for (const auto& col : node.AsList()) {
        path.emplace_back(ctx.AppendString(col.AsString()));
    }
    return path;
};

TPartOfConstraintBase::TSetType TPartOfConstraintBase::NodeToSet(TExprContext& ctx, const NYT::TNode& node) {
    if (node.IsString())
        return TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType(1U, ctx.AppendString(node.AsString()))};

    TPartOfConstraintBase::TSetType set;
    for (const auto& col : node.AsList()) {
        set.insert_unique(NodeToPath(ctx, col));
    }
    return set;
};

TPartOfConstraintBase::TSetOfSetsType TPartOfConstraintBase::NodeToSetOfSets(TExprContext& ctx, const NYT::TNode& node) {
    TSetOfSetsType sets;
    for (const auto& s : node.AsList()) {
        sets.insert_unique(NodeToSet(ctx, s));
    }
    return sets;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TConstraintNode* TConstraintSet::GetConstraint(std::string_view name) const {
    const auto it = std::lower_bound(Constraints_.cbegin(), Constraints_.cend(), name, TConstraintNode::TCompare());
    if (it != Constraints_.cend() && (*it)->GetName() == name) {
        return *it;
    }
    return nullptr;
}

void TConstraintSet::AddConstraint(const TConstraintNode* node) {
    if (!node) {
        return;
    }
    const auto it = std::lower_bound(Constraints_.begin(), Constraints_.end(), node, TConstraintNode::TCompare());
    if (it == Constraints_.end() || (*it)->GetName() != node->GetName()) {
        Constraints_.insert(it, node);
    } else {
        Y_ENSURE(node->Equals(**it), "Adding unequal constraint: " << *node << " != " << **it);
    }
}

const TConstraintNode* TConstraintSet::RemoveConstraint(std::string_view name) {
    const TConstraintNode* res = nullptr;
    const auto it = std::lower_bound(Constraints_.begin(), Constraints_.end(), name, TConstraintNode::TCompare());
    if (it != Constraints_.end() && (*it)->GetName() == name) {
        res = *it;
        Constraints_.erase(it);
    }
    return res;
}

void TConstraintSet::Out(IOutputStream& out) const {
    out.Write('{');
    bool first = true;
    for (const auto& c: Constraints_) {
        if (!first)
            out.Write(',');
        out << *c;
        first = false;
    }
    out.Write('}');
}

void TConstraintSet::ToJson(NJson::TJsonWriter& writer) const {
    writer.OpenMap();
    for (const auto& node : Constraints_) {
        writer.WriteKey(node->GetName());
        node->ToJson(writer);
    }
    writer.CloseMap();
}

NYT::TNode TConstraintSet::ToYson() const {
    auto res = NYT::TNode::CreateMap();
    for (const auto& node : Constraints_) {
        auto serialized = node->ToYson();
        YQL_ENSURE(!serialized.IsUndefined(), "Cannot serialize " << node->GetName() << " constraint");
        res[node->GetName()] = std::move(serialized);
    }
    return res;
}

bool TConstraintSet::FilterConstraints(const TPredicate& predicate) {
    const auto size = Constraints_.size();
    for (auto it = Constraints_.begin(); Constraints_.end() != it;)
        if (predicate((*it)->GetName()))
            ++it;
        else
            it = Constraints_.erase(it);
    return Constraints_.size() != size;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

size_t GetElementsCount(const TTypeAnnotationNode* type) {
    if (type) {
        switch (type->GetKind()) {
            case ETypeAnnotationKind::Tuple:  return type->Cast<TTupleExprType>()->GetSize();
            case ETypeAnnotationKind::Multi:  return type->Cast<TMultiExprType>()->GetSize();
            case ETypeAnnotationKind::Struct: return type->Cast<TStructExprType>()->GetSize();
            default: break;
        }
    }
    return 0U;
}

std::deque<std::string_view> GetAllItemTypeFields(const TTypeAnnotationNode* type, TExprContext& ctx) {
    std::deque<std::string_view> fields;
    if (type) {
        switch (type->GetKind()) {
            case ETypeAnnotationKind::Struct:
                if (const auto structType = type->Cast<TStructExprType>()) {
                    fields.resize(structType->GetSize());
                    std::transform(structType->GetItems().cbegin(), structType->GetItems().cend(), fields.begin(), std::bind(&TItemExprType::GetName, std::placeholders::_1));
                }
                break;
            case ETypeAnnotationKind::Tuple:
                if (const auto size = type->Cast<TTupleExprType>()->GetSize()) {
                    fields.resize(size);
                    ui32 i = 0U;
                    std::generate(fields.begin(), fields.end(), [&]() { return ctx.GetIndexAsString(i++); });
                }
                break;
            case ETypeAnnotationKind::Multi:
                if (const auto size = type->Cast<TMultiExprType>()->GetSize()) {
                    fields.resize(size);
                    ui32 i = 0U;
                    std::generate(fields.begin(), fields.end(), [&]() { return ctx.GetIndexAsString(i++); });
                }
                break;
            default:
                break;
        }
    }
    return fields;
}

TPartOfConstraintBase::TSetOfSetsType MakeFullSet(const TPartOfConstraintBase::TSetType& keys) {
    TPartOfConstraintBase::TSetOfSetsType sets;
    sets.reserve(sets.size());
    for (const auto& key : keys)
        sets.insert_unique(TPartOfConstraintBase::TSetType{key});
    return sets;
}

}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TSortedConstraintNode::TSortedConstraintNode(TExprContext& ctx, TContainerType&& content)
    : TConstraintWithFieldsT(ctx, Name())
    , Content_(std::move(content))
{
    YQL_ENSURE(!Content_.empty());
    for (const auto& c : Content_) {
        YQL_ENSURE(!c.first.empty());
        for (const auto& path : c.first)
            Hash_ = std::accumulate(path.cbegin(), path.cend(), c.second ? Hash_ : ~Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
    }
}

TSortedConstraintNode::TSortedConstraintNode(TExprContext& ctx, const NYT::TNode& serialized)
    : TSortedConstraintNode(ctx, NodeToContainer(ctx, serialized))
{
}

TSortedConstraintNode::TContainerType TSortedConstraintNode::NodeToContainer(TExprContext& ctx, const NYT::TNode& serialized) {
    TSortedConstraintNode::TContainerType sorted;
    try {
        for (const auto& pair : serialized.AsList()) {
            TPartOfConstraintBase::TSetType set = TPartOfConstraintBase::NodeToSet(ctx, pair.AsList().front());
            sorted.emplace_back(std::move(set), pair.AsList().back().AsBool());
        }
    } catch (...) {
        YQL_ENSURE(false, "Cannot deserialize " << Name() << " constraint: " << CurrentExceptionMessage());
    }
    return sorted;
}

TSortedConstraintNode::TSortedConstraintNode(TSortedConstraintNode&&) = default;

bool TSortedConstraintNode::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }

    if (const auto c = dynamic_cast<const TSortedConstraintNode*>(&node)) {
        return GetContent() == c->GetContent();
    }
    return false;
}

bool TSortedConstraintNode::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetName() != node.GetName()) {
        return false;
    }

    const auto& content = static_cast<const TSortedConstraintNode&>(node).GetContent();
    if (content.size() > Content_.size())
        return false;
    for (TContainerType::size_type i = 0U; i < content.size(); ++i) {
        if (Content_[i].second != content[i].second ||
            !(std::includes(Content_[i].first.cbegin(), Content_[i].first.cend(), content[i].first.cbegin(), content[i].first.cend())  || std::includes(content[i].first.cbegin(), content[i].first.cend(), Content_[i].first.cbegin(), Content_[i].first.cend())))
            return false;
    }

    return true;
}

void TSortedConstraintNode::Out(IOutputStream& out) const {
    TConstraintNode::Out(out);
    out.Write('(');
    bool first = true;
    for (const auto& c : Content_) {
        if (first)
            first = false;
        else
            out.Write(';');

        out.Write(JoinSeq(',', c.first));
        out.Write('[');
        out.Write(c.second ? "asc" : "desc");
        out.Write(']');
    }
    out.Write(')');
}

void TSortedConstraintNode::ToJson(NJson::TJsonWriter& out) const {
    out.OpenArray();
    for (const auto& c : Content_) {
        out.OpenArray();
        out.Write(JoinSeq(';', c.first));
        out.Write(c.second);
        out.CloseArray();
    }
    out.CloseArray();
}

NYT::TNode TSortedConstraintNode::ToYson() const {
    return std::accumulate(Content_.cbegin(), Content_.cend(),
        NYT::TNode::CreateList(),
        [](NYT::TNode node, const std::pair<TSetType, bool>& pair) {
            return std::move(node).Add(NYT::TNode::CreateList().Add(TPartOfConstraintBase::SetToNode(pair.first, false)).Add(pair.second));
        });
}

bool TSortedConstraintNode::IsPrefixOf(const TSortedConstraintNode& node) const {
    return node.Includes(*this);
}

bool TSortedConstraintNode::StartsWith(const TSetType& prefix) const {
    auto set = prefix;
    for (const auto& key : Content_) {
        bool found = false;
        std::for_each(key.first.cbegin(), key.first.cend(), [&set, &found] (const TPathType& path) {
            if (const auto it = set.find(path); set.cend() != it) {
                set.erase(it);
                found = true;
            }
        });

        if (!found)
            break;
    }

    return set.empty();
}

TPartOfConstraintBase::TSetType TSortedConstraintNode::GetFullSet() const {
    TSetType set;
    set.reserve(Content_.size());
    for (const auto& key : Content_)
        set.insert_unique(key.first.cbegin(), key.first.cend());
    return set;
}

void TSortedConstraintNode::FilterUncompleteReferences(TSetType& references) const {
    TSetType complete;
    complete.reserve(references.size());

    for (const auto& item : Content_) {
        bool found = false;
        for (const auto& path : item.first) {
            if (references.contains(path)) {
                found = true;
                complete.insert_unique(path);
            }
        }

        if (!found)
            break;
    }

    references = std::move(complete);
}

const TSortedConstraintNode* TSortedConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }

    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TSortedConstraintNode>();
    }

    std::optional<TContainerType> content;
    for (size_t i = 0U; i < constraints.size(); ++i) {
        if (const auto sort = constraints[i]->GetConstraint<TSortedConstraintNode>()) {
            const auto& nextContent = sort->GetContent();
            if (content) {
                const auto size = std::min(content->size(), nextContent.size());
                content->resize(size);
                for (auto j = 0U; j < size; ++j) {
                    auto& one = (*content)[j];
                    auto& two = nextContent[j];
                    TSetType common;
                    common.reserve(std::min(one.first.size(), two.first.size()));
                    std::set_intersection(one.first.cbegin(), one.first.cend(), two.first.cbegin(), two.first.cend(), std::back_inserter(common));
                    if (common.empty() || one.second != two.second) {
                        content->resize(j);
                        break;
                    } else
                        one.first = std::move(common);
                }
                if (content->empty())
                    break;
            } else {
                content = nextContent;
            }
        } else if (!constraints[i]->GetConstraint<TEmptyConstraintNode>()) {
            content.reset();
            break;
        }
    }

    return !content || content->empty() ? nullptr : ctx.MakeConstraint<TSortedConstraintNode>(std::move(*content));
}

const TSortedConstraintNode* TSortedConstraintNode::MakeCommon(const TSortedConstraintNode* other, TExprContext& ctx) const {
    if (!other) {
        return nullptr;
    } else if (this == other) {
        return this;
    }

    auto content = other->GetContent();
    const auto size = std::min(content.size(), Content_.size());
    content.resize(size);
    for (auto j = 0U; j < size; ++j) {
        auto& one = content[j];
        auto& two = Content_[j];
        TSetType common;
        common.reserve(std::min(one.first.size(), two.first.size()));
        std::set_intersection(one.first.cbegin(), one.first.cend(), two.first.cbegin(), two.first.cend(), std::back_inserter(common));
        if (common.empty() || one.second != two.second) {
            content.resize(j);
            break;
        } else
            one.first = std::move(common);
    }

    return content.empty() ? nullptr : ctx.MakeConstraint<TSortedConstraintNode>(std::move(content));
}

const TSortedConstraintNode* TSortedConstraintNode::CutPrefix(size_t newPrefixLength, TExprContext& ctx) const {
    if (!newPrefixLength)
        return nullptr;

    if (newPrefixLength >= Content_.size())
        return this;

    auto content = Content_;
    content.resize(newPrefixLength);
    return ctx.MakeConstraint<TSortedConstraintNode>(std::move(content));
}

const TConstraintWithFieldsNode* TSortedConstraintNode::DoFilterFields(TExprContext& ctx, const TPathFilter& filter) const {
    if (!filter)
        return this;

    TContainerType sorted;
    sorted.reserve(Content_.size());
    for (const auto& item : Content_) {
        TSetType newSet;
        newSet.reserve(item.first.size());
        for (const auto& path : item.first) {
            if (filter(path))
                newSet.insert_unique(path);
        }

        if (newSet.empty())
            break;
        else
            sorted.emplace_back(std::move(newSet), item.second);
    }
    return sorted.empty() ? nullptr : ctx.MakeConstraint<TSortedConstraintNode>(std::move(sorted));
}

const TConstraintWithFieldsNode* TSortedConstraintNode::DoRenameFields(TExprContext& ctx, const TPathReduce& reduce) const {
    if (!reduce)
        return this;

    TContainerType sorted;
    sorted.reserve(Content_.size());
    for (const auto& item : Content_) {
        TSetType newSet;
        newSet.reserve(item.first.size());
        for (const auto& path : item.first) {
            if (const auto& newPaths = reduce(path); !newPaths.empty())
                newSet.insert_unique(newPaths.cbegin(), newPaths.cend());
        }

        if (newSet.empty())
            break;
        else
            sorted.emplace_back(std::move(newSet), item.second);
    }
    return sorted.empty() ? nullptr : ctx.MakeConstraint<TSortedConstraintNode>(std::move(sorted));
}

bool TSortedConstraintNode::IsApplicableToType(const TTypeAnnotationNode& type) const {
    const auto& itemType = GetSeqItemType(type);
    return std::all_of(Content_.cbegin(), Content_.cend(), [&itemType](const std::pair<TSetType, bool>& pair) {
        return std::all_of(pair.first.cbegin(), pair.first.cend(), std::bind(&GetSubTypeByPath, std::placeholders::_1, std::cref(itemType)));
    });
}


const TConstraintWithFieldsNode*
TSortedConstraintNode::DoGetComplicatedForType(const TTypeAnnotationNode& type, TExprContext& ctx) const {
    const auto& rowType = GetSeqItemType(type);
    bool changed = false;
    auto content = Content_;
    for (auto it = content.begin(); content.end() != it;) {
        const auto subType = GetSubTypeByPath(it->first.front(), rowType);
        auto fields = GetAllItemTypeFields(subType, ctx);
        for (auto j = it->first.cbegin(); it->first.cend() != ++j;) {
            if (!IsSameAnnotation(*GetSubTypeByPath(*j, rowType), *subType)) {
                fields.clear();
                break;
            }
        }

        if (fields.empty() || ETypeAnnotationKind::Struct == subType->GetKind())
            ++it;
        else {
            changed = true;
            const bool dir = it->second;
            auto set = it->first;
            for (auto& path : set)
                path.emplace_back();
            for (it = content.erase(it); !fields.empty(); fields.pop_front()) {
                auto paths = set;
                for (auto& path : paths)
                    path.back() = fields.front();
                it = content.emplace(it, std::move(paths), dir);
                ++it;
            }
        }
    }

    return changed ? ctx.MakeConstraint<TSortedConstraintNode>(std::move(content)) : this;
}

const TConstraintWithFieldsNode*
TSortedConstraintNode::DoGetSimplifiedForType(const TTypeAnnotationNode& type, TExprContext& ctx) const {
    if (Content_.size() == 1U && Content_.front().first.size() == 1U && Content_.front().first.front().empty())
        return DoGetComplicatedForType(type, ctx);

    const auto& rowType = GetSeqItemType(type);
    const auto getPrefix = [](TPartOfConstraintBase::TPathType path) {
        path.pop_back();
        return path;
    };

    bool changed = false;
    auto content = Content_;
    for (bool setChanged = true; setChanged;) {
        setChanged = false;
        for (auto it = content.begin(); content.end() != it;) {
            if (it->first.size() > 1U) {
                for (const auto& path : it->first) {
                    if (path.size() > 1U && path.back() == ctx.GetIndexAsString(0U)) {
                        const auto prefix = getPrefix(path);
                        if (const auto subType = GetSubTypeByPath(prefix, rowType); ETypeAnnotationKind::Struct != subType->GetKind() && 1 == GetElementsCount(subType)) {
                            it->first.erase(path);
                            it->first.insert(prefix);
                            changed = setChanged = true;
                        }
                    }
                }
                ++it;
            } else if (it->first.size() == 1U && it->first.front().size() > 1U) {
                const auto prefix = getPrefix(it->first.front());
                if (const auto subType = GetSubTypeByPath(prefix, rowType); it->first.front().back() == ctx.GetIndexAsString(0U) && ETypeAnnotationKind::Struct != subType->GetKind()) {
                    auto from = it++;
                    for (auto i = 1U; content.cend() != it && it->first.size() == 1U && it->first.front().size() > 1U && ctx.GetIndexAsString(i) == it->first.front().back() && prefix == getPrefix(it->first.front()) && from->second == it->second; ++i)
                        ++it;

                    if (ssize_t(GetElementsCount(subType)) == std::distance(from, it)) {
                        *from = std::make_pair(TPartOfConstraintBase::TSetType{std::move(prefix)}, from->second);
                        ++from;
                        it = content.erase(from, it);
                        changed = setChanged = true;
                    }
                } else
                    ++it;
            } else
                ++it;
        }
    }

    return changed ? ctx.MakeConstraint<TSortedConstraintNode>(std::move(content)) : this;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TChoppedConstraintNode::TChoppedConstraintNode(TExprContext& ctx, TSetOfSetsType&& sets)
    : TConstraintWithFieldsT(ctx, Name())
    , Sets_(std::move(sets))
{
    YQL_ENSURE(!Sets_.empty());
    YQL_ENSURE(!HasDuplicates(Sets_));
    const auto size = Sets_.size();
    Hash_ = MurmurHash<ui64>(&size, sizeof(size), Hash_);
    for (const auto& set : Sets_) {
        YQL_ENSURE(!set.empty());
        for (const auto& path : set)
            Hash_ = std::accumulate(path.cbegin(), path.cend(), Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
    }
}

TChoppedConstraintNode::TChoppedConstraintNode(TExprContext& ctx, const TSetType& keys)
    : TChoppedConstraintNode(ctx, MakeFullSet(keys))
{}

TChoppedConstraintNode::TChoppedConstraintNode(TExprContext& ctx, const NYT::TNode& serialized)
    : TChoppedConstraintNode(ctx, NodeToSets(ctx, serialized))
{
}

TChoppedConstraintNode::TSetOfSetsType TChoppedConstraintNode::NodeToSets(TExprContext& ctx, const NYT::TNode& serialized) {
    try {
        return TPartOfConstraintBase::NodeToSetOfSets(ctx, serialized);
    } catch (...) {
        YQL_ENSURE(false, "Cannot deserialize " << Name() << " constraint: " << CurrentExceptionMessage());
    }
    Y_UNREACHABLE();
}

TChoppedConstraintNode::TChoppedConstraintNode(TChoppedConstraintNode&& constr) = default;

TPartOfConstraintBase::TSetType TChoppedConstraintNode::GetFullSet() const {
    TSetType set;
    set.reserve(Sets_.size());
    for (const auto& key : Sets_)
        set.insert_unique(key.cbegin(), key.cend());
    return set;
}

bool TChoppedConstraintNode::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    if (const auto c = dynamic_cast<const TChoppedConstraintNode*>(&node)) {
        return Sets_ == c->Sets_;
    }
    return false;
}

bool TChoppedConstraintNode::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (const auto c = dynamic_cast<const TChoppedConstraintNode*>(&node)) {
        return std::includes(Sets_.cbegin(), Sets_.cend(), c->Sets_.cbegin(), c->Sets_.cend());
    }
    return false;
}

void TChoppedConstraintNode::Out(IOutputStream& out) const {
    TConstraintNode::Out(out);
    out.Write('(');

    for (const auto& set : Sets_) {
        out.Write('(');
        bool first = true;
        for (const auto& path : set) {
            if (first)
                first = false;
            else
                out.Write(',');
            out << path;
        }
        out.Write(')');
    }
    out.Write(')');
}

void TChoppedConstraintNode::ToJson(NJson::TJsonWriter& out) const {
    out.OpenArray();
    for (const auto& set : Sets_) {
        out.OpenArray();
        for (const auto& path : set) {
            out.Write(JoinSeq(';', path));
        }
        out.CloseArray();
    }
    out.CloseArray();
}

NYT::TNode TChoppedConstraintNode::ToYson() const {
    return TPartOfConstraintBase::SetOfSetsToNode(Sets_);
}

bool TChoppedConstraintNode::Equals(const TSetType& prefix) const {
    auto set = prefix;
    for (const auto& key : Sets_) {
        bool found = false;
        std::for_each(key.cbegin(), key.cend(), [&set, &found] (const TPathType& path) {
            if (const auto it = set.find(path); set.cend() != it) {
                set.erase(it);
                found = true;
            }
        });

        if (!found)
            return false;
    }

    return set.empty();
}


void TChoppedConstraintNode::FilterUncompleteReferences(TSetType& references) const {
    TSetType complete;
    complete.reserve(references.size());

    for (const auto& item : Sets_) {
        bool found = false;
        for (const auto& path : item) {
            if (references.contains(path)) {
                found = true;
                complete.insert_unique(path);
            }
        }

        if (!found)
            break;
    }

    references = std::move(complete);
}

const TChoppedConstraintNode* TChoppedConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }
    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TChoppedConstraintNode>();
    }

    TSetOfSetsType sets;
    for (auto c: constraints) {
        if (const auto uniq = c->GetConstraint<TChoppedConstraintNode>()) {
            if (sets.empty())
                sets = uniq->GetContent();
            else {
                TSetOfSetsType both;
                both.reserve(std::min(sets.size(), uniq->GetContent().size()));
                std::set_intersection(sets.cbegin(), sets.cend(), uniq->GetContent().cbegin(), uniq->GetContent().cend(), std::back_inserter(both));
                if (both.empty()) {
                    if (!c->GetConstraint<TEmptyConstraintNode>())
                        return nullptr;
                } else
                    sets = std::move(both);
            }
        } else if (!c->GetConstraint<TEmptyConstraintNode>()) {
            return nullptr;
        }
    }

    return sets.empty() ? nullptr : ctx.MakeConstraint<TChoppedConstraintNode>(std::move(sets));
}

const TConstraintWithFieldsNode*
TChoppedConstraintNode::DoFilterFields(TExprContext& ctx, const TPathFilter& predicate) const {
    if (!predicate)
        return this;

    TSetOfSetsType chopped;
    chopped.reserve(Sets_.size());
    for (const auto& set : Sets_) {
        auto newSet = set;
        for (auto it = newSet.cbegin(); newSet.cend() != it;) {
            if (predicate(*it))
                ++it;
            else
                it = newSet.erase(it);
        }

        if (newSet.empty())
            return nullptr;;

        chopped.insert_unique(std::move(newSet));
    }
    return ctx.MakeConstraint<TChoppedConstraintNode>(std::move(chopped));
}

const TConstraintWithFieldsNode*
TChoppedConstraintNode::DoRenameFields(TExprContext& ctx, const TPathReduce& reduce) const {
    if (!reduce)
        return this;

    TSetOfSetsType chopped;
    chopped.reserve(Sets_.size());
    for (const auto& set : Sets_) {
        TSetType newSet;
        newSet.reserve(set.size());
        for (const auto& path : set) {
            if (const auto& newPaths = reduce(path); !newPaths.empty())
                newSet.insert_unique(newPaths.cbegin(), newPaths.cend());
        }

        if (newSet.empty())
            return nullptr;

        chopped.insert_unique(std::move(newSet));
    }

    return ctx.MakeConstraint<TChoppedConstraintNode>(std::move(chopped));
}

const TChoppedConstraintNode*
TChoppedConstraintNode::MakeCommon(const TChoppedConstraintNode* other, TExprContext& ctx) const {
    if (!other) {
        return nullptr;
    }
    if (this == other) {
        return this;
    }

    TSetOfSetsType both;
    both.reserve(std::min(Sets_.size(), other->Sets_.size()));
    std::set_intersection(Sets_.cbegin(), Sets_.cend(), other->Sets_.cbegin(), other->Sets_.cend(), std::back_inserter(both));
    return both.empty() ? nullptr : ctx.MakeConstraint<TChoppedConstraintNode>(std::move(both));
}

bool TChoppedConstraintNode::IsApplicableToType(const TTypeAnnotationNode& type) const {
    const auto& itemType = GetSeqItemType(type);
    return std::all_of(Sets_.cbegin(), Sets_.cend(), [&itemType](const TSetType& set) {
        return std::all_of(set.cbegin(), set.cend(), std::bind(&GetSubTypeByPath, std::placeholders::_1, std::cref(itemType)));
    });
}

const TConstraintWithFieldsNode*
TChoppedConstraintNode::DoGetComplicatedForType(const TTypeAnnotationNode& type, TExprContext& ctx) const {
    const auto& rowType = GetSeqItemType(type);
    bool changed = false;
    auto sets = Sets_;
    for (auto it = sets.begin(); sets.end() != it;) {
        auto fields = GetAllItemTypeFields(GetSubTypeByPath(it->front(), rowType), ctx);
        for (auto j = it->cbegin(); it->cend() != ++j;) {
            if (const auto& copy = GetAllItemTypeFields(GetSubTypeByPath(*j, rowType), ctx); copy != fields) {
                fields.clear();
                break;
            }
        }

        if (fields.empty())
            ++it;
        else {
            changed = true;
            auto set = *it;
            for (auto& path : set)
                path.emplace_back();
            for (it = sets.erase(it); !fields.empty(); fields.pop_front()) {
                auto paths = set;
                for (auto& path : paths)
                    path.back() = fields.front();
                it = sets.insert_unique(std::move(paths)).first;
            }
        }
    }

    return changed ? ctx.MakeConstraint<TChoppedConstraintNode>(std::move(sets)) : this;
}

const TConstraintWithFieldsNode*
TChoppedConstraintNode::DoGetSimplifiedForType(const TTypeAnnotationNode& type, TExprContext& ctx) const {
    if (Sets_.size() == 1U && Sets_.front().size() == 1U && Sets_.front().front().empty())
        return DoGetComplicatedForType(type, ctx);

    const auto& rowType = GetSeqItemType(type);
    const auto getPrefix = [](TPartOfConstraintBase::TPathType path) {
        path.pop_back();
        return path;
    };

    bool changed = false;
    auto sets = Sets_;
    for (bool setChanged = true; setChanged;) {
        setChanged = false;
        for (auto it = sets.begin(); sets.end() != it;) {
            if (it->size() != 1U || it->front().size() <= 1U)
                ++it;
            else {
                auto from = it++;
                const auto prefix = getPrefix(from->front());
                while (sets.cend() != it && it->size() == 1U && it->front().size() > 1U && prefix == getPrefix(it->front()))
                    ++it;

                if (ssize_t(GetElementsCount(GetSubTypeByPath(prefix, rowType))) == std::distance(from, it)) {
                    *from++ = TPartOfConstraintBase::TSetType{std::move(prefix)};
                    it = sets.erase(from, it);
                    changed = setChanged = true;
                }
            }
        }
    }

    return changed ? ctx.MakeConstraint<TChoppedConstraintNode>(std::move(sets)) : this;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<bool Distinct>
TConstraintWithFieldsNode::TSetOfSetsType
TUniqueConstraintNodeBase<Distinct>::ColumnsListToSets(const std::vector<std::string_view>& columns) {
    YQL_ENSURE(!columns.empty());
    TConstraintWithFieldsNode::TSetOfSetsType sets;
    sets.reserve(columns.size());
    std::for_each(columns.cbegin(), columns.cend(), [&sets](const std::string_view& column) { sets.insert_unique(TConstraintWithFieldsNode::TSetType{column.empty() ? TConstraintWithFieldsNode::TPathType() : TConstraintWithFieldsNode::TPathType(1U, column)}); });
    return sets;
}

template<bool Distinct>
typename TUniqueConstraintNodeBase<Distinct>::TContentType
TUniqueConstraintNodeBase<Distinct>::DedupSets(TContentType&& sets) {
    for (bool found = true; found && sets.size() > 1U;) {
        found = false;
        for (auto ot = sets.cbegin(); !found && sets.cend() != ot; ++ot) {
            for (auto it = sets.cbegin(); sets.cend() != it;) {
                if (ot->size() < it->size() && std::all_of(ot->cbegin(), ot->cend(), [it](const TConstraintWithFieldsNode::TSetType& set) { return it->contains(set); })) {
                    it = sets.erase(it);
                    found = true;
                } else
                    ++it;
            }
        }
    }

    return std::move(sets);
}

template<bool Distinct>
typename TUniqueConstraintNodeBase<Distinct>::TContentType
TUniqueConstraintNodeBase<Distinct>::MakeCommonContent(const TContentType& one, const TContentType& two) {
    TContentType both;
    both.reserve(std::min(one.size(), two.size()));
    for (const auto& setsOne : one) {
        for (const auto& setsTwo : two) {
            if (setsOne.size() == setsTwo.size()) {
                TConstraintWithFieldsNode::TSetOfSetsType sets;
                sets.reserve(setsTwo.size());
                for (const auto& setOne : setsOne) {
                    for (const auto& setTwo : setsTwo) {
                        TConstraintWithFieldsNode::TSetType set;
                        set.reserve(std::min(setOne.size(), setTwo.size()));
                        std::set_intersection(setOne.cbegin(), setOne.cend(), setTwo.cbegin(), setTwo.cend(), std::back_inserter(set));
                        if (!set.empty())
                            sets.insert_unique(std::move(set));
                    }
                }
                if (sets.size() == setsOne.size())
                    both.insert_unique(std::move(sets));
            }
        }
    }
    return both;
}

template<bool Distinct>
TUniqueConstraintNodeBase<Distinct>::TUniqueConstraintNodeBase(TExprContext& ctx, TContentType&& sets)
    : TBase(ctx, Name())
    , Content_(DedupSets(std::move(sets)))
{
    YQL_ENSURE(!Content_.empty());
    const auto size = Content_.size();
    TBase::Hash_ = MurmurHash<ui64>(&size, sizeof(size), TBase::Hash_);
    for (const auto& sets : Content_) {
        YQL_ENSURE(!sets.empty());
        YQL_ENSURE(!TConstraintWithFieldsNode::HasDuplicates(sets));
        for (const auto& set : sets) {
            YQL_ENSURE(!set.empty());
            for (const auto& path : set)
                TBase::Hash_ = std::accumulate(path.cbegin(), path.cend(), TBase::Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
        }
    }
}

template<bool Distinct>
TUniqueConstraintNodeBase<Distinct>::TUniqueConstraintNodeBase(TExprContext& ctx, const std::vector<std::string_view>& columns)
    : TUniqueConstraintNodeBase(ctx, TContentType{TPartOfConstraintBase::TSetOfSetsType{ColumnsListToSets(columns)}})
{}

template<bool Distinct>
TUniqueConstraintNodeBase<Distinct>::TUniqueConstraintNodeBase(TExprContext& ctx, const NYT::TNode& serialized)
    : TUniqueConstraintNodeBase(ctx, NodeToContent(ctx, serialized))
{
}

template<bool Distinct>
TUniqueConstraintNodeBase<Distinct>::TContentType TUniqueConstraintNodeBase<Distinct>::NodeToContent(TExprContext& ctx, const NYT::TNode& serialized) {
    TUniqueConstraintNode::TContentType content;
    try {
        for (const auto& item : serialized.AsList()) {
            content.insert_unique(TPartOfConstraintBase::NodeToSetOfSets(ctx, item));
        }
    } catch (...) {
        YQL_ENSURE(false, "Cannot deserialize " << Name() << " constraint: " << CurrentExceptionMessage());
    }
    return content;
}

template<bool Distinct>
TUniqueConstraintNodeBase<Distinct>::TUniqueConstraintNodeBase(TUniqueConstraintNodeBase&& constr) = default;

template<bool Distinct>
TPartOfConstraintBase::TSetType
TUniqueConstraintNodeBase<Distinct>::GetFullSet() const {
    TPartOfConstraintBase::TSetType set;
    set.reserve(Content_.size());
    for (const auto& sets : Content_)
        for (const auto& key : sets)
            set.insert_unique(key.cbegin(), key.cend());
    return set;
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (TBase::GetHash() != node.GetHash()) {
        return false;
    }
    if (const auto c = dynamic_cast<const TUniqueConstraintNodeBase*>(&node)) {
        return Content_ == c->Content_;
    }
    return false;
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::Includes(const TConstraintNode& node) const {
    if (this == &node)
        return true;

    if (const auto c = dynamic_cast<const TUniqueConstraintNodeBase*>(&node)) {
        return std::all_of(c->Content_.cbegin(), c->Content_.cend(), [&] (const TConstraintWithFieldsNode::TSetOfSetsType& oldSets) {
            return std::any_of(Content_.cbegin(), Content_.cend(), [&] (const TConstraintWithFieldsNode::TSetOfSetsType& newSets) {
                return oldSets.size() == newSets.size() && std::all_of(oldSets.cbegin(), oldSets.cend(), [&] (const TConstraintWithFieldsNode::TSetType& oldSet) {
                    return std::any_of(newSets.cbegin(), newSets.cend(), [&] (const TConstraintWithFieldsNode::TSetType& newSet) {
                        return std::includes(newSet.cbegin(), newSet.cend(), oldSet.cbegin(), oldSet.cend());
                    });
                });
            });
        });
    }
    return false;
}

template<bool Distinct>
void TUniqueConstraintNodeBase<Distinct>::Out(IOutputStream& out) const {
    TConstraintNode::Out(out);
    out.Write('(');
    for (const auto& sets : Content_) {
        out.Write('(');
        bool first = true;
        for (const auto& set : sets) {
            if (first)
                first = false;
            else
                out << ',';
            if (1U == set.size())
                out << set.front();
            else
                out << set;
        }
        out.Write(')');
    }
    out.Write(')');
}

template<bool Distinct>
void TUniqueConstraintNodeBase<Distinct>::ToJson(NJson::TJsonWriter& out) const {
    out.OpenArray();
    for (const auto& sets : Content_) {
        out.OpenArray();
        for (const auto& set : sets) {
            out.OpenArray();
            for (const auto& path : set) {
                out.Write(JoinSeq(';', path));
            }
            out.CloseArray();
        }
        out.CloseArray();
    }
    out.CloseArray();
}

template<bool Distinct>
NYT::TNode TUniqueConstraintNodeBase<Distinct>::ToYson() const {
    return std::accumulate(Content_.cbegin(), Content_.cend(),
        NYT::TNode::CreateList(),
        [](NYT::TNode node, const TConstraintWithFieldsNode::TSetOfSetsType& sets) {
            return std::move(node).Add(TConstraintWithFieldsNode::SetOfSetsToNode(sets));
        });
}

template<bool Distinct>
const TUniqueConstraintNodeBase<Distinct>* TUniqueConstraintNodeBase<Distinct>::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }
    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TUniqueConstraintNodeBase>();
    }

    TContentType content;
    for (auto c: constraints) {
        if (const auto uniq = c->GetConstraint<TUniqueConstraintNodeBase>()) {
            if (content.empty())
                content = uniq->GetContent();
            else {
                if (auto both = MakeCommonContent(content, uniq->Content_); both.empty()) {
                    if (!c->GetConstraint<TEmptyConstraintNode>())
                        return nullptr;
                } else
                    content = std::move(both);
            }
        } else if (!c->GetConstraint<TEmptyConstraintNode>()) {
            return nullptr;
        }
    }

    return content.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNodeBase>(std::move(content));
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::IsOrderBy(const TSortedConstraintNode& sorted) const {
    TConstraintWithFieldsNode::TSetType ordered;
    TConstraintWithFieldsNode::TSetOfSetsType columns;
    for (const auto& key : sorted.GetContent()) {
        ordered.insert_unique(key.first.cbegin(), key.first.cend());
        columns.insert_unique(key.first);
    }

    for (const auto& sets : Content_) {
        if (std::all_of(sets.cbegin(), sets.cend(), [&ordered](const TConstraintWithFieldsNode::TSetType& set) {
            return std::any_of(set.cbegin(), set.cend(), [&ordered](const TConstraintWithFieldsNode::TPathType& path) { return ordered.contains(path); });
        })) {
            std::for_each(sets.cbegin(), sets.cend(), [&columns](const TConstraintWithFieldsNode::TSetType& set) {
                std::for_each(set.cbegin(), set.cend(), [&columns](const TConstraintWithFieldsNode::TPathType& path) {
                    if (const auto it = std::find_if(columns.cbegin(), columns.cend(), [&path](const TConstraintWithFieldsNode::TSetType& s) { return s.contains(path); }); columns.cend() != it)
                        columns.erase(it);
                });
            });
            if (columns.empty())
                return true;
        }
    }

    return false;
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::ContainsCompleteSet(const std::vector<std::string_view>& columns) const {
    if (columns.empty())
        return false;

    const std::unordered_set<std::string_view> ordered(columns.cbegin(), columns.cend());
    for (const auto& sets : Content_) {
        if (std::all_of(sets.cbegin(), sets.cend(), [&ordered](const TConstraintWithFieldsNode::TSetType& set) {
            return std::any_of(set.cbegin(), set.cend(), [&ordered](const TConstraintWithFieldsNode::TPathType& path) { return !path.empty() && ordered.contains(path.front()); });
        }))
            return true;
    }
    return false;
}

template<bool Distinct>
void TUniqueConstraintNodeBase<Distinct>::FilterUncompleteReferences(TPartOfConstraintBase::TSetType& references) const {
    TPartOfConstraintBase::TSetType input(std::move(references));
    references.clear();
    references.reserve(input.size());
    for (const auto& sets : Content_) {
        if (std::all_of(sets.cbegin(), sets.cend(), [&input] (const TPartOfConstraintBase::TSetType& set) { return std::any_of(set.cbegin(), set.cend(), std::bind(&TPartOfConstraintBase::TSetType::contains<TPartOfConstraintBase::TPathType>, std::cref(input), std::placeholders::_1)); }))
            std::for_each(sets.cbegin(), sets.cend(), [&] (const TPartOfConstraintBase::TSetType& set) { std::for_each(set.cbegin(), set.cend(), [&] (const TPartOfConstraintBase::TPathType& path) {
                if (input.contains(path))
                    references.insert_unique(path);
            }); });
    }
}

template<bool Distinct>
const TConstraintWithFieldsNode*
TUniqueConstraintNodeBase<Distinct>::DoFilterFields(TExprContext& ctx, const TPartOfConstraintBase::TPathFilter& predicate) const {
    if (!predicate)
        return this;

    TContentType content;
    content.reserve(Content_.size());
    for (const auto& sets : Content_) {
        if (std::all_of(sets.cbegin(), sets.cend(), [&predicate](const TPartOfConstraintBase::TSetType& set) { return std::any_of(set.cbegin(), set.cend(), predicate); })) {
            TPartOfConstraintBase::TSetOfSetsType newSets;
            newSets.reserve(sets.size());
            std::for_each(sets.cbegin(), sets.cend(), [&](const TPartOfConstraintBase::TSetType& set) {
                TPartOfConstraintBase::TSetType newSet;
                newSet.reserve(set.size());
                std::copy_if(set.cbegin(), set.cend(), std::back_inserter(newSet), predicate);
                newSets.insert_unique(std::move(newSet));
            });
            content.insert_unique(std::move(newSets));
        }
    }
    return content.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNodeBase>(std::move(content));
}

template<bool Distinct>
const TConstraintWithFieldsNode*
TUniqueConstraintNodeBase<Distinct>::DoRenameFields(TExprContext& ctx, const TPartOfConstraintBase::TPathReduce& reduce) const {
    if (!reduce)
        return this;

    TContentType content;
    content.reserve(Content_.size());
    for (const auto& sets : Content_) {
        TPartOfConstraintBase::TSetOfSetsType newSets;
        newSets.reserve(sets.size());
        for (const auto& set : sets) {
            TPartOfConstraintBase::TSetType newSet;
            newSet.reserve(set.size());
            for (const auto& path : set) {
                const auto newPaths = reduce(path);
                newSet.insert_unique(newPaths.cbegin(), newPaths.cend());
            }
            if (!newSet.empty())
               newSets.insert_unique(std::move(newSet));
        }
        if (sets.size() == newSets.size())
            content.insert_unique(std::move(newSets));
    }
    return content.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNodeBase>(std::move(content));
}

template<bool Distinct>
const TUniqueConstraintNodeBase<Distinct>*
TUniqueConstraintNodeBase<Distinct>::MakeCommon(const TUniqueConstraintNodeBase* other, TExprContext& ctx) const {
    if (!other)
        return nullptr;

    if (this == other)
        return this;

    auto both = MakeCommonContent(Content_, other->Content_);
    return both.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNodeBase>(std::move(both));
}

template<bool Distinct>
const TUniqueConstraintNodeBase<Distinct>* TUniqueConstraintNodeBase<Distinct>::Merge(const TUniqueConstraintNodeBase* one, const TUniqueConstraintNodeBase* two, TExprContext& ctx) {
    if (!one)
        return two;
    if (!two)
        return one;

    auto content = one->Content_;
    content.insert_unique(two->Content_.cbegin(), two->Content_.cend());
    return ctx.MakeConstraint<TUniqueConstraintNodeBase<Distinct>>(std::move(content));
}

template<bool Distinct>
const TConstraintWithFieldsNode*
TUniqueConstraintNodeBase<Distinct>::DoGetComplicatedForType(const TTypeAnnotationNode& type, TExprContext& ctx) const {
    const auto& rowType = GetSeqItemType(type);
    bool changed = false;
    auto content = Content_;
    for (auto& sets : content) {
        for (auto it = sets.begin(); sets.end() != it;) {
            auto fields = GetAllItemTypeFields(TBase::GetSubTypeByPath(it->front(), rowType), ctx);
            for (auto j = it->cbegin(); it->cend() != ++j;) {
                if (const auto& copy = GetAllItemTypeFields(TBase::GetSubTypeByPath(*j, rowType), ctx); copy != fields) {
                    fields.clear();
                    break;
                }
            }

            if (fields.empty())
                ++it;
            else {
                changed = true;
                auto set = *it;
                for (auto& path : set)
                    path.emplace_back();
                for (it = sets.erase(it); !fields.empty(); fields.pop_front()) {
                    auto paths = set;
                    for (auto& path : paths)
                        path.back() = fields.front();
                    it = sets.insert_unique(std::move(paths)).first;
                }
            }
        }
    }

    return changed ? ctx.MakeConstraint<TUniqueConstraintNodeBase<Distinct>>(std::move(content)) : this;
}

template<bool Distinct>
const TConstraintWithFieldsNode*
TUniqueConstraintNodeBase<Distinct>::DoGetSimplifiedForType(const TTypeAnnotationNode& type, TExprContext& ctx) const {
    if (Content_.size() == 1U && Content_.front().size() == 1U && Content_.front().front().size() == 1U && Content_.front().front().front().empty())
        return DoGetComplicatedForType(type, ctx);

    const auto& rowType = GetSeqItemType(type);
    const auto getPrefix = [](TPartOfConstraintBase::TPathType path) {
        path.pop_back();
        return path;
    };

    bool changed = false;
    auto content = Content_;
    for (auto& sets : content) {
        for (bool setChanged = true; setChanged;) {
            setChanged = false;
            for (auto it = sets.begin(); sets.end() != it;) {
                if (!it->empty() && it->front().size() > 1U) {
                    TPartOfConstraintBase::TSetType prefixes;
                    prefixes.reserve(it->size());
                    for (const auto& path : *it) {
                        if (path.size() > 1U) {
                            prefixes.emplace_back(getPrefix(path));
                        }
                    }

                    auto from = it++;
                    if (prefixes.size() < from->size())
                        continue;

                    while (sets.cend() != it && it->size() == prefixes.size() &&
                        std::all_of(it->cbegin(), it->cend(), [&](const TPartOfConstraintBase::TPathType& path) { return path.size() > 1U && prefixes.contains(getPrefix(path)); })) {
                            ++it;
                    }

                    if (std::all_of(prefixes.cbegin(), prefixes.cend(),
                        [width = std::distance(from, it), &rowType] (const TPartOfConstraintBase::TPathType& path) { return width == ssize_t(GetElementsCount(TBase::GetSubTypeByPath(path, rowType))); })) {
                        *from++ =std::move(prefixes);
                        it = sets.erase(from, it);
                        changed = setChanged = true;
                    }
                } else
                    ++it;
            }
        }
    }

    return changed ? ctx.MakeConstraint<TUniqueConstraintNodeBase<Distinct>>(std::move(content)) : this;
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::IsApplicableToType(const TTypeAnnotationNode& type) const {
    if (ETypeAnnotationKind::Dict == type.GetKind())
        return true; // TODO: check for dict.
    const auto& itemType = GetSeqItemType(type);
    return std::all_of(Content_.cbegin(), Content_.cend(), [&itemType](const TConstraintWithFieldsNode::TSetOfSetsType& sets) {
        return std::all_of(sets.cbegin(), sets.cend(), [&itemType](const TConstraintWithFieldsNode::TSetType& set) {
            return std::all_of(set.cbegin(), set.cend(), std::bind(&TConstraintWithFieldsNode::GetSubTypeByPath, std::placeholders::_1, std::cref(itemType)));
        });
    });
}

template class TUniqueConstraintNodeBase<false>;
template class TUniqueConstraintNodeBase<true>;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<class TOriginalConstraintNode>
TPartOfConstraintNode<TOriginalConstraintNode>::TPartOfConstraintNode(TExprContext& ctx, TMapType&& mapping)
    : TBase(ctx, Name())
    , Mapping_(std::move(mapping))
{
    YQL_ENSURE(!Mapping_.empty());
    for (const auto& part : Mapping_) {
        YQL_ENSURE(!part.second.empty());
        const auto hash = part.first->GetHash();
        TBase::Hash_ = MurmurHash<ui64>(&hash, sizeof(hash), TBase::Hash_);
        for (const auto& item: part.second) {
            TBase::Hash_ = std::accumulate(item.first.cbegin(), item.first.cend(), TBase::Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
            TBase::Hash_ = std::accumulate(item.second.cbegin(), item.second.cend(), TBase::Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
        }
    }
}

template<class TOriginalConstraintNode>
TPartOfConstraintNode<TOriginalConstraintNode>::TPartOfConstraintNode(TExprContext& ctx, const NYT::TNode&)
    : TBase(ctx, Name())
{
    YQL_ENSURE(false, "TPartOfConstraintNode cannot be deserialized");
}

template<class TOriginalConstraintNode>
TPartOfConstraintNode<TOriginalConstraintNode>::TPartOfConstraintNode(TPartOfConstraintNode&& constr) = default;

template<class TOriginalConstraintNode>
bool TPartOfConstraintNode<TOriginalConstraintNode>::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (TBase::GetHash() != node.GetHash()) {
        return false;
    }
    if (const auto c = dynamic_cast<const TPartOfConstraintNode*>(&node)) {
        return Mapping_ == c->Mapping_;
    }
    return false;
}

template<class TOriginalConstraintNode>
bool TPartOfConstraintNode<TOriginalConstraintNode>::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (const auto c = dynamic_cast<const TPartOfConstraintNode*>(&node)) {
        for (const auto& part : c->Mapping_) {
            if (const auto it = Mapping_.find(part.first); Mapping_.cend() != it) {
                for (const auto& pair : part.second) {
                    if (const auto p = it->second.find(pair.first); it->second.cend() == p || p->second != pair.second) {
                        return false;
                    }
                }
            } else
                return false;
        }
        return true;
    }
    return false;
}

template<class TOriginalConstraintNode>
void TPartOfConstraintNode<TOriginalConstraintNode>::Out(IOutputStream& out) const {
    TConstraintNode::Out(out);
    out.Write('(');
    bool first = true;
    for (const auto& part : Mapping_) {
        for (const auto& item : part.second) {
            if (first)
                first = false;
            else
                out.Write(',');

            out << item.first;
            out.Write(':');
            out << item.second;
        }
    }
    out.Write(')');
}

template<class TOriginalConstraintNode>
void TPartOfConstraintNode<TOriginalConstraintNode>::ToJson(NJson::TJsonWriter& out) const {
    out.OpenMap();
    for (const auto& part : Mapping_) {
        for (const auto& [resultColumn, originalColumn] : part.second) {
            out.Write(JoinSeq(';', resultColumn), JoinSeq(';', originalColumn));
        }
    }
    out.CloseMap();
}

template<class TOriginalConstraintNode>
NYT::TNode TPartOfConstraintNode<TOriginalConstraintNode>::ToYson() const {
    return {}; // cannot be serialized
}

template<class TOriginalConstraintNode>
const TPartOfConstraintNode<TOriginalConstraintNode>*
TPartOfConstraintNode<TOriginalConstraintNode>::ExtractField(TExprContext& ctx, const std::string_view& field) const {
    TMapType passtrought;
    for (const auto& part : Mapping_) {
        auto it = part.second.lower_bound(TPartOfConstraintBase::TPathType(1U, field));
        if (part.second.cend() == it || it->first.front() != field)
            continue;

        TPartType mapping;
        mapping.reserve(part.second.size());
        while (it < part.second.cend() && !it->first.empty() && field == it->first.front()) {
            auto item = *it++;
            item.first.pop_front();
            mapping.emplace_back(std::move(item));
        }

        if (!mapping.empty()) {
            passtrought.emplace(part.first, std::move(mapping));
        }
    }
    return passtrought.empty() ? nullptr : ctx.MakeConstraint<TPartOfConstraintNode>(std::move(passtrought));
}

template<class TOriginalConstraintNode>
const TPartOfConstraintBase*
TPartOfConstraintNode<TOriginalConstraintNode>::DoFilterFields(TExprContext& ctx, const TPartOfConstraintBase::TPathFilter& predicate) const {
    if (!predicate)
        return this;

    auto mapping = Mapping_;
    for (auto part = mapping.begin(); mapping.end() != part;) {
        for (auto it = part->second.cbegin(); part->second.cend() != it;) {
            if (predicate(it->first))
                ++it;
            else
                it = part->second.erase(it);
        }

        if (part->second.empty())
            part = mapping.erase(part);
        else
            ++part;
    }
    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPartOfConstraintNode>(std::move(mapping));
}

template<class TOriginalConstraintNode>
const TPartOfConstraintBase*
TPartOfConstraintNode<TOriginalConstraintNode>::DoRenameFields(TExprContext& ctx, const TPartOfConstraintBase::TPathReduce& rename) const {
    if (!rename)
        return this;

    TMapType mapping(Mapping_.size());
    for (const auto& part : Mapping_) {
        TPartType map;
        map.reserve(part.second.size());

        for (const auto& item : part.second) {
            for (auto& path : rename(item.first)) {
                map.insert_unique(std::make_pair(std::move(path), item.second));
            }
        }

        if (!map.empty())
            mapping.emplace(part.first, std::move(map));
    }
    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPartOfConstraintNode>(std::move(mapping));
}

template<class TOriginalConstraintNode>
const TPartOfConstraintNode<TOriginalConstraintNode>*
TPartOfConstraintNode<TOriginalConstraintNode>::CompleteOnly(TExprContext& ctx) const {
    TMapType mapping(Mapping_);

    for (auto it = mapping.begin(); mapping.end() != it;) {
        TPartOfConstraintBase::TSetType set;
        set.reserve(it->second.size());
        std::for_each(it->second.cbegin(), it->second.cend(), [&](const typename TPartType::value_type& pair) { set.insert_unique(pair.second); });

        it->first->FilterUncompleteReferences(set);

        for (auto jt = it->second.cbegin(); it->second.cend() != jt;) {
            if (set.contains(jt->second))
                ++jt;
            else
                jt = it->second.erase(jt);
        }

        if (it->second.empty())
            it = mapping.erase(it);
        else
            ++it;
    }

    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPartOfConstraintNode>(std::move(mapping));
}

template<class TOriginalConstraintNode>
const TPartOfConstraintNode<TOriginalConstraintNode>*
TPartOfConstraintNode<TOriginalConstraintNode>:: RemoveOriginal(TExprContext& ctx, const TMainConstraint* original) const {
    TMapType mapping(Mapping_);
    mapping.erase(original);
    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPartOfConstraintNode>(std::move(mapping));
}

template<class TOriginalConstraintNode>
typename TPartOfConstraintNode<TOriginalConstraintNode>::TMapType
TPartOfConstraintNode<TOriginalConstraintNode>::GetColumnMapping(const std::string_view& asField) const {
    auto mapping = Mapping_;
    if (!asField.empty()) {
        for (auto& item : mapping) {
            for (auto& part : item.second) {
                part.first.emplace_front(asField);
            }
        }
    }
    return mapping;
}

template<class TOriginalConstraintNode>
typename TPartOfConstraintNode<TOriginalConstraintNode>::TMapType
TPartOfConstraintNode<TOriginalConstraintNode>::GetColumnMapping(TExprContext& ctx, const std::string_view& prefix) const {
    auto mapping = Mapping_;
    if (!prefix.empty()) {
        const TString str(prefix);
        for (auto& item : mapping) {
            for (auto& part : item.second) {
                if (part.first.empty())
                    part.first.emplace_front(prefix);
                else
                    part.first.front() = ctx.AppendString(str + part.first.front());
            }
        }
    }
    return mapping;
}

template<class TOriginalConstraintNode>
const TPartOfConstraintNode<TOriginalConstraintNode>*
TPartOfConstraintNode<TOriginalConstraintNode>::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }

    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TPartOfConstraintNode>();
    }

    bool first = true;
    TMapType mapping;
    for (size_t i = 0; i < constraints.size(); ++i) {
        const auto part = constraints[i]->GetConstraint<TPartOfConstraintNode>();
        if (!part)
            return nullptr;
        if (first) {
            mapping = part->GetColumnMapping();
            first = false;
        } else {
            for (const auto& nextMapping : part->GetColumnMapping()) {
                if (const auto it = mapping.find(nextMapping.first); mapping.cend() != it) {
                    TPartType result;
                    std::set_intersection(
                        it->second.cbegin(), it->second.cend(),
                        nextMapping.second.cbegin(), nextMapping.second.cend(),
                        std::back_inserter(result),
                        [] (const typename TPartType::value_type& c1, const typename TPartType::value_type& c2) {
                            return c1 < c2;
                        }
                    );
                    if (result.empty())
                        mapping.erase(it);
                    else
                        it->second = std::move(result);
                }
            }
        }
        if (mapping.empty()) {
            break;
        }
    }

    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPartOfConstraintNode>(std::move(mapping));
}

template<class TOriginalConstraintNode>
const typename TPartOfConstraintNode<TOriginalConstraintNode>::TMapType&
TPartOfConstraintNode<TOriginalConstraintNode>::GetColumnMapping() const {
    return Mapping_;
}

template<class TOriginalConstraintNode>
typename TPartOfConstraintNode<TOriginalConstraintNode>::TMapType
TPartOfConstraintNode<TOriginalConstraintNode>::GetCommonMapping(const TOriginalConstraintNode* complete, const TPartOfConstraintNode* incomplete, const std::string_view& field) {
    TMapType mapping;
    if (incomplete) {
        mapping = incomplete->GetColumnMapping();
        mapping.erase(complete);
        if (!field.empty()) {
            for (auto& part : mapping) {
                std::for_each(part.second.begin(), part.second.end(), [&field](typename TPartType::value_type& map) { map.first.push_front(field); });
            }
        }
    }

    if (complete) {
        auto& part = mapping[complete];
        for (const auto& path : complete->GetFullSet()) {
            auto key = path;
            if (!field.empty())
                key.emplace_front(field);
            part.insert_unique(std::make_pair(key, path));
        }
    }

    return mapping;
}

template<class TOriginalConstraintNode>
void TPartOfConstraintNode<TOriginalConstraintNode>::UniqueMerge(TMapType& output, TMapType&& input) {
    output.merge(input);
    while (!input.empty()) {
        const auto exists = input.extract(input.cbegin());
        auto& target = output[exists.key()];
        target.reserve(target.size() + exists.mapped().size());
        for (auto& item : exists.mapped())
            target.insert_unique(std::move(item));
    }
}

template<class TOriginalConstraintNode>
typename TPartOfConstraintNode<TOriginalConstraintNode>::TMapType
TPartOfConstraintNode<TOriginalConstraintNode>::ExtractField(const TMapType& mapping, const std::string_view& field) {
    TMapType parts;
    for (const auto& part : mapping) {
        auto it = part.second.lower_bound(TPartOfConstraintBase::TPathType(1U, field));
        if (part.second.cend() == it || it->first.empty() || it->first.front() != field)
            continue;

        TPartType mapping;
        mapping.reserve(part.second.size());
        while (it < part.second.cend() && !it->first.empty() && field == it->first.front()) {
            auto item = *it++;
            item.first.pop_front();
            mapping.emplace_back(std::move(item));
        }

        if (!mapping.empty()) {
            parts.emplace(part.first, std::move(mapping));
        }
    }
    return parts;
}

template<class TOriginalConstraintNode>
const TOriginalConstraintNode*
TPartOfConstraintNode<TOriginalConstraintNode>::MakeComplete(TExprContext& ctx, const TMapType& mapping, const TOriginalConstraintNode* original, const std::string_view& field) {
    if (const auto it = mapping.find(original); mapping.cend() != it) {
        TReversePartType reverseMap;
        reverseMap.reserve(it->second.size());
        for (const auto& map : it->second)
            reverseMap[map.second].insert_unique(map.first);

        const auto rename = [&](const TPartOfConstraintBase::TPathType& path) {
            const auto& set = reverseMap[path];
            std::vector<TPartOfConstraintBase::TPathType> out(set.cbegin(), set.cend());
            if (!field.empty())
                std::for_each(out.begin(), out.end(), [&field](TPartOfConstraintBase::TPathType& path) { path.emplace_front(field); });
            return out;
        };

        return it->first->RenameFields(ctx, rename);
    }

    return nullptr;
}

template<class TOriginalConstraintNode>
const TOriginalConstraintNode*
TPartOfConstraintNode<TOriginalConstraintNode>::MakeComplete(TExprContext& ctx, const TPartOfConstraintNode* partial, const TOriginalConstraintNode* original, const std::string_view& field) {
    if (!partial)
        return nullptr;

    return MakeComplete(ctx, partial->GetColumnMapping(), original, field);
}

template<class TOriginalConstraintNode>
bool TPartOfConstraintNode<TOriginalConstraintNode>::IsApplicableToType(const TTypeAnnotationNode& type) const {
    if (ETypeAnnotationKind::Dict == type.GetKind())
        return true; // TODO: check for dict.

    const auto itemType = GetSeqItemType(&type);
    const auto& actualType = itemType ? *itemType : type;
    return std::all_of(Mapping_.cbegin(), Mapping_.cend(), [&actualType](const typename TMapType::value_type& pair) {
        return std::all_of(pair.second.cbegin(), pair.second.cend(), [&actualType](const typename TPartType::value_type& part) { return bool(TPartOfConstraintBase::GetSubTypeByPath(part.first, actualType)); });
    });
}

template class TPartOfConstraintNode<TSortedConstraintNode>;
template class TPartOfConstraintNode<TChoppedConstraintNode>;
template class TPartOfConstraintNode<TUniqueConstraintNode>;
template class TPartOfConstraintNode<TDistinctConstraintNode>;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TEmptyConstraintNode::TEmptyConstraintNode(TExprContext& ctx)
    : TConstraintNode(ctx, Name())
{
}

TEmptyConstraintNode::TEmptyConstraintNode(TEmptyConstraintNode&& constr)
    : TConstraintNode(std::move(static_cast<TConstraintNode&>(constr)))
{
}

TEmptyConstraintNode::TEmptyConstraintNode(TExprContext& ctx, const NYT::TNode& serialized)
    : TConstraintNode(ctx, Name())
{
    YQL_ENSURE(serialized.IsEntity(), "Unexpected serialized content of " << Name() << " constraint");
}

bool TEmptyConstraintNode::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    return GetName() == node.GetName();
}

void TEmptyConstraintNode::ToJson(NJson::TJsonWriter& out) const {
    out.Write(true);
}

NYT::TNode TEmptyConstraintNode::ToYson() const {
    return NYT::TNode::CreateEntity();
}

const TEmptyConstraintNode* TEmptyConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& /*ctx*/) {
    if (constraints.empty()) {
        return nullptr;
    }

    auto empty = constraints.front()->GetConstraint<TEmptyConstraintNode>();
    if (AllOf(constraints.cbegin() + 1, constraints.cend(), [empty](const TConstraintSet* c) { return c->GetConstraint<TEmptyConstraintNode>() == empty; })) {
        return empty;
    }
    return nullptr;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TVarIndexConstraintNode::TVarIndexConstraintNode(TExprContext& ctx, const TMapType& mapping)
    : TConstraintNode(ctx, Name())
    , Mapping_(mapping)
{
    Hash_ = MurmurHash<ui64>(Mapping_.data(), Mapping_.size() * sizeof(TMapType::value_type), Hash_);
    YQL_ENSURE(!Mapping_.empty());
}

TVarIndexConstraintNode::TVarIndexConstraintNode(TExprContext& ctx, const TVariantExprType& itemType)
    : TVarIndexConstraintNode(ctx, itemType.GetUnderlyingType()->Cast<TTupleExprType>()->GetSize())
{
}

TVarIndexConstraintNode::TVarIndexConstraintNode(TExprContext& ctx, size_t mapItemsCount)
    : TConstraintNode(ctx, Name())
{
    YQL_ENSURE(mapItemsCount > 0);
    for (size_t i = 0; i < mapItemsCount; ++i) {
        Mapping_.push_back(std::make_pair(i, i));
    }
    Hash_ = MurmurHash<ui64>(Mapping_.data(), Mapping_.size() * sizeof(TMapType::value_type), Hash_);
    YQL_ENSURE(!Mapping_.empty());
}

TVarIndexConstraintNode::TVarIndexConstraintNode(TExprContext& ctx, const NYT::TNode& serialized)
    : TVarIndexConstraintNode(ctx, NodeToMapping(serialized))
{
}

TVarIndexConstraintNode::TVarIndexConstraintNode(TVarIndexConstraintNode&& constr)
    : TConstraintNode(std::move(static_cast<TConstraintNode&>(constr)))
    , Mapping_(std::move(constr.Mapping_))
{
}

TVarIndexConstraintNode::TMapType TVarIndexConstraintNode::NodeToMapping(const NYT::TNode& serialized) {
    TMapType mapping;
    try {
        for (const auto& pair: serialized.AsList()) {
            mapping.insert(std::make_pair<ui32, ui32>(pair.AsList().front().AsUint64(), pair.AsList().back().AsUint64()));
        }
    } catch (...) {
        YQL_ENSURE(false, "Cannot deserialize " << Name() << " constraint: " << CurrentExceptionMessage());
    }
    return mapping;
}

TVarIndexConstraintNode::TMapType TVarIndexConstraintNode::GetReverseMapping() const {
    TMapType reverseMapping;
    std::transform(Mapping_.cbegin(), Mapping_.cend(),
        std::back_inserter(reverseMapping),
        [] (const std::pair<size_t, size_t>& p) { return std::make_pair(p.second, p.first); }
    );
    ::Sort(reverseMapping);
    return reverseMapping;
}

bool TVarIndexConstraintNode::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    if (GetName() != node.GetName()) {
        return false;
    }
    if (auto c = dynamic_cast<const TVarIndexConstraintNode*>(&node)) {
        return GetIndexMapping() == c->GetIndexMapping();
    }
    return false;
}

bool TVarIndexConstraintNode::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetName() != node.GetName()) {
        return false;
    }
    if (auto c = dynamic_cast<const TVarIndexConstraintNode*>(&node)) {
        for (auto& pair: c->Mapping_) {
            if (auto p = Mapping_.FindPtr(pair.first)) {
                if (*p != pair.second) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }
    return false;
}

void TVarIndexConstraintNode::Out(IOutputStream& out) const {
    TConstraintNode::Out(out);
    out.Write('(');

    bool first = true;
    for (auto& item: Mapping_) {
        if (!first) {
            out.Write(',');
        }
        out << item.first << ':' << item.second;
        first = false;
    }
    out.Write(')');
}

void TVarIndexConstraintNode::ToJson(NJson::TJsonWriter& out) const {
    out.OpenArray();
    for (const auto& [resultIndex, originalIndex]: Mapping_) {
        out.OpenArray();
        out.Write(resultIndex);
        out.Write(originalIndex);
        out.CloseArray();
    }
    out.CloseArray();
}

NYT::TNode TVarIndexConstraintNode::ToYson() const {
    return std::accumulate(Mapping_.cbegin(), Mapping_.cend(),
        NYT::TNode::CreateList(),
        [](NYT::TNode node, const TMapType::value_type& p) {
            return std::move(node).Add(NYT::TNode::CreateList().Add(p.first).Add(p.second));
        });
}

const TVarIndexConstraintNode* TVarIndexConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }

    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TVarIndexConstraintNode>();
    }

    TVarIndexConstraintNode::TMapType mapping;
    for (size_t i = 0; i < constraints.size(); ++i) {
        if (auto varIndex = constraints[i]->GetConstraint<TVarIndexConstraintNode>()) {
            mapping.insert(varIndex->GetIndexMapping().begin(), varIndex->GetIndexMapping().end());
        }
    }
    if (mapping.empty()) {
        return nullptr;
    }
    ::SortUnique(mapping);
    return ctx.MakeConstraint<TVarIndexConstraintNode>(std::move(mapping));
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TMultiConstraintNode::TMultiConstraintNode(TExprContext& ctx, TMapType&& items)
    : TConstraintNode(ctx, Name())
    , Items_(std::move(items))
{
    YQL_ENSURE(Items_.size());
    for (auto& item: Items_) {
        Hash_ = MurmurHash<ui64>(&item.first, sizeof(item.first), Hash_);
        for (auto c: item.second.GetAllConstraints()) {
            const auto itemHash = c->GetHash();
            Hash_ = MurmurHash<ui64>(&itemHash, sizeof(itemHash), Hash_);
        }
    }
}

TMultiConstraintNode::TMultiConstraintNode(TExprContext& ctx, ui32 index, const TConstraintSet& constraints)
    : TMultiConstraintNode(ctx, TMapType{{index, constraints}})
{
}

TMultiConstraintNode::TMultiConstraintNode(TExprContext& ctx, const NYT::TNode& serialized)
    : TMultiConstraintNode(ctx, NodeToMapping(ctx, serialized))
{
}

TMultiConstraintNode::TMultiConstraintNode(TMultiConstraintNode&& constr)
    : TConstraintNode(std::move(static_cast<TConstraintNode&>(constr)))
    , Items_(std::move(constr.Items_))
{
}

TMultiConstraintNode::TMapType TMultiConstraintNode::NodeToMapping(TExprContext& ctx, const NYT::TNode& serialized) {
    TMapType mapping;
    try {
        for (const auto& pair: serialized.AsList()) {
            mapping.insert(std::make_pair((ui32)pair.AsList().front().AsUint64(), ctx.MakeConstraintSet(pair.AsList().back())));
        }
    } catch (...) {
        YQL_ENSURE(false, "Cannot deserialize " << Name() << " constraint: " << CurrentExceptionMessage());
    }
    return mapping;
}

bool TMultiConstraintNode::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    if (GetName() != node.GetName()) {
        return false;
    }
    if (auto c = dynamic_cast<const TMultiConstraintNode*>(&node)) {
        return GetItems() == c->GetItems();
    }
    return false;
}

bool TMultiConstraintNode::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetName() != node.GetName()) {
        return false;
    }

    if (auto m = dynamic_cast<const TMultiConstraintNode*>(&node)) {
        for (auto& item: Items_) {
            const auto it = m->Items_.find(item.first);
            if (it == m->Items_.end()) {
                if (!item.second.GetConstraint<TEmptyConstraintNode>()) {
                    return false;
                }
                continue;
            }

            for (auto c: it->second.GetAllConstraints()) {
                auto cit = item.second.GetConstraint(c->GetName());
                if (!cit) {
                    return false;
                }
                if (!cit->Includes(*c)) {
                    return false;
                }
            }
        }
        return true;
    }
    return false;
}

bool TMultiConstraintNode::FilteredIncludes(const TConstraintNode& node, const THashSet<TString>& blacklist) const {
    if (this == &node) {
        return true;
    }
    if (GetName() != node.GetName()) {
        return false;
    }

    if (auto m = dynamic_cast<const TMultiConstraintNode*>(&node)) {
        for (auto& item: Items_) {
            const auto it = m->Items_.find(item.first);
            if (it == m->Items_.end()) {
                if (!item.second.GetConstraint<TEmptyConstraintNode>()) {
                    return false;
                }
                continue;
            }

            for (auto c: it->second.GetAllConstraints()) {
                if (!blacklist.contains(c->GetName())) {
                    const auto cit = item.second.GetConstraint(c->GetName());
                    if (!cit) {
                        return false;
                    }
                    if (!cit->Includes(*c)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
    return false;
}

void TMultiConstraintNode::Out(IOutputStream& out) const {
    TConstraintNode::Out(out);
    out.Write('(');
    bool first = true;
    for (auto& item: Items_) {
        if (!first) {
            out.Write(',');
        }
        out << item.first << ':' << '{';
        bool firstConstr = true;
        for (auto c: item.second.GetAllConstraints()) {
            if (!firstConstr) {
                out.Write(',');
            }
            out << *c;
            firstConstr = false;
        }
        out << '}';
        first = false;
    }
    out.Write(')');
}

void TMultiConstraintNode::ToJson(NJson::TJsonWriter& out) const {
    out.OpenMap();
    for (const auto& [index, constraintSet] : Items_) {
        out.WriteKey(ToString(index));
        constraintSet.ToJson(out);
    }
    out.CloseMap();
}

NYT::TNode TMultiConstraintNode::ToYson() const {
    return std::accumulate(Items_.cbegin(), Items_.cend(),
        NYT::TNode::CreateList(),
        [](NYT::TNode node, const TMapType::value_type& p) {
            return std::move(node).Add(NYT::TNode::CreateList().Add(p.first).Add(p.second.ToYson()));
        });
}

const TMultiConstraintNode* TMultiConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    } else if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TMultiConstraintNode>();
    }

    TMapType multiItems;
    for (auto c: constraints) {
        if (auto m = c->GetConstraint<TMultiConstraintNode>()) {
            multiItems.insert(m->GetItems().begin(), m->GetItems().end());
        } else if (!c->GetConstraint<TEmptyConstraintNode>()) {
            return nullptr;
        }
    }
    if (multiItems.empty()) {
        return nullptr;
    }

    multiItems.sort();
    // Remove duplicates
    // For duplicated items keep only Empty constraint
    auto cur = multiItems.begin();
    while (cur != multiItems.end()) {
        auto start = cur;
        do {
            ++cur;
        } while (cur != multiItems.end() && start->first == cur->first);

        switch (std::distance(start, cur)) {
        case 0:
            break;
        case 1:
            if (start->second.GetConstraint<TEmptyConstraintNode>()) {
                cur = multiItems.erase(start, cur);
            }
            break;
        default:
            {
                std::vector<TMapType::value_type> nonEmpty;
                std::copy_if(start, cur, std::back_inserter(nonEmpty),
                    [] (const TMapType::value_type& v) {
                        return !v.second.GetConstraint<TEmptyConstraintNode>();
                    }
                );
                start->second.Clear();
                if (nonEmpty.empty()) {
                    start->second.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                } else if (nonEmpty.size() == 1) {
                    start->second = nonEmpty.front().second;
                }
                cur = multiItems.erase(start + 1, cur);
            }
        }
    }
    if (!multiItems.empty()) {
        return ctx.MakeConstraint<TMultiConstraintNode>(std::move(multiItems));
    }

    return nullptr;
}

const TMultiConstraintNode* TMultiConstraintNode::FilterConstraints(TExprContext& ctx, const TConstraintSet::TPredicate& predicate) const {
    auto items = Items_;
    bool hasContent = false, hasChanges = false;
    for (auto& item : items) {
        hasChanges = hasChanges || item.second.FilterConstraints(predicate);
        hasContent = hasContent || item.second;
    }

    return hasContent ? hasChanges ? ctx.MakeConstraint<TMultiConstraintNode>(std::move(items)) : this : nullptr;
}

} // namespace NYql

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<>
void Out<NYql::TPartOfConstraintBase::TPathType>(IOutputStream& out, const NYql::TPartOfConstraintBase::TPathType& path) {
    if (path.empty())
        out.Write('/');
    else {
        bool first = true;
        for (const auto& c : path) {
            if (first)
                first = false;
            else
                out.Write('/');
            out.Write(c);
        }
    }
}

template<>
void Out<NYql::TPartOfConstraintBase::TSetType>(IOutputStream& out, const NYql::TPartOfConstraintBase::TSetType& c) {
    out.Write('{');
    bool first = true;
    for (const auto& path : c) {
        if (first)
            first = false;
        else
            out.Write(',');
        out << path;
    }
    out.Write('}');
}

template<>
void Out<NYql::TPartOfConstraintBase::TSetOfSetsType>(IOutputStream& out, const NYql::TPartOfConstraintBase::TSetOfSetsType& c) {
    out.Write('{');
    bool first = true;
    for (const auto& path : c) {
        if (first)
            first = false;
        else
            out.Write(',');
        out << path;
    }
    out.Write('}');
}

template<>
void Out<NYql::TConstraintNode>(IOutputStream& out, const NYql::TConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TConstraintSet>(IOutputStream& out, const NYql::TConstraintSet& s) {
    s.Out(out);
}

template<>
void Out<NYql::TSortedConstraintNode>(IOutputStream& out, const NYql::TSortedConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TChoppedConstraintNode>(IOutputStream& out, const NYql::TChoppedConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TUniqueConstraintNode>(IOutputStream& out, const NYql::TUniqueConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TDistinctConstraintNode>(IOutputStream& out, const NYql::TDistinctConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TPartOfSortedConstraintNode>(IOutputStream& out, const NYql::TPartOfSortedConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TPartOfChoppedConstraintNode>(IOutputStream& out, const NYql::TPartOfChoppedConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TPartOfUniqueConstraintNode>(IOutputStream& out, const NYql::TPartOfUniqueConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TPartOfDistinctConstraintNode>(IOutputStream& out, const NYql::TPartOfDistinctConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TEmptyConstraintNode>(IOutputStream& out, const NYql::TEmptyConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TVarIndexConstraintNode>(IOutputStream& out, const NYql::TVarIndexConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TMultiConstraintNode>(IOutputStream& out, const NYql::TMultiConstraintNode& c) {
    c.Out(out);
}
