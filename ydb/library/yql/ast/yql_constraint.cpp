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

const TTypeAnnotationNode* TConstraintNode::GetSubTypeByPath(const TPathType& path, const TTypeAnnotationNode& type) {
    if (path.empty())
        return &type;

    const auto tail = [](const TPathType& path) {
        auto p(path);
        p.pop_front();
        return p;
    };
    switch (type.GetKind()) {
        case ETypeAnnotationKind::Optional:
            return GetSubTypeByPath(path, *type.Cast<TOptionalExprType>()->GetItemType());
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
        default:
            break;
    }
    return nullptr;
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

void TConstraintSet::ToJson(NJson::TJsonWriter& writer) const {
    writer.OpenMap();
    for (const auto& node : Constraints_) {
        writer.WriteKey(node->GetName());
        node->ToJson(writer);
    }
    writer.CloseMap();
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TColumnSetConstraintNodeBase::TColumnSetConstraintNodeBase(TExprContext& ctx, TStringBuf name, const TSetType& columns)
    : TConstraintNode(ctx, name)
{
    YQL_ENSURE(!columns.empty());
    for (auto& c: columns) {
        Hash_ = MurmurHash<ui64>(c.data(), c.size(), Hash_);
        Columns_.push_back(ctx.AppendString(c));
    }
}

TColumnSetConstraintNodeBase::TColumnSetConstraintNodeBase(TExprContext& ctx, TStringBuf name, const std::vector<TStringBuf>& columns)
    : TConstraintNode(ctx, name)
{
    YQL_ENSURE(!columns.empty());
    for (auto& c: columns) {
        YQL_ENSURE(Columns_.insert_unique(ctx.AppendString(c)).second, "Duplicate Unique constraint column: " << c);
    }
    for (auto& c: Columns_) {
        Hash_ = MurmurHash<ui64>(c.data(), c.size(), Hash_);
    }
}

TColumnSetConstraintNodeBase::TColumnSetConstraintNodeBase(TExprContext& ctx, TStringBuf name, const std::vector<TString>& columns)
    : TConstraintNode(ctx, name)
{
    YQL_ENSURE(!columns.empty());
    for (auto& c: columns) {
        YQL_ENSURE(Columns_.insert_unique(ctx.AppendString(c)).second, "Duplicate Unique constraint column: " << c);
    }
    for (auto& c: Columns_) {
        Hash_ = MurmurHash<ui64>(c.data(), c.size(), Hash_);
    }
}

TColumnSetConstraintNodeBase::TColumnSetConstraintNodeBase(TColumnSetConstraintNodeBase&& constr)
    : TConstraintNode(std::move(static_cast<TConstraintNode&>(constr)))
    , Columns_(std::move(constr.Columns_))
{
}

bool TColumnSetConstraintNodeBase::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    if (GetName() != node.GetName()) {
        return false;
    }
    if (auto c = dynamic_cast<const TColumnSetConstraintNodeBase*>(&node)) {
        return Columns_ == c->Columns_;
    }
    return false;
}

bool TColumnSetConstraintNodeBase::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetName() != node.GetName()) {
        return false;
    }
    if (auto c = dynamic_cast<const TColumnSetConstraintNodeBase*>(&node)) {
        for (auto& col: c->Columns_) {
            if (!Columns_.has(col)) {
                return false;
            }
        }
        return true;
    }
    return false;
}

void TColumnSetConstraintNodeBase::Out(IOutputStream& out) const {
    TConstraintNode::Out(out);
    out.Write('(');

    bool first = true;
    for (auto& col: Columns_) {
        if (!first) {
            out.Write(',');
        }
        out.Write(col);
        first = false;
    }
    out.Write(')');
}

void TColumnSetConstraintNodeBase::ToJson(NJson::TJsonWriter& out) const {
    out.OpenArray();
    for (const auto& column : Columns_) {
        out.Write(column);
    }
    out.CloseArray();
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TSortedConstraintNode::TSortedConstraintNode(TExprContext& ctx, TContainerType&& content)
    : TConstraintNode(ctx, Name()), Content_(std::move(content))
{
    YQL_ENSURE(!Content_.empty());
    for (const auto& c : Content_) {
        YQL_ENSURE(!c.first.empty());
        for (const auto& path : c.first)
            Hash_ = std::accumulate(path.cbegin(), path.cend(), c.second ? Hash_ : ~Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
    }
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

bool TSortedConstraintNode::IsPrefixOf(const TSortedConstraintNode& node) const {
    return node.Includes(*this);
}

bool TSortedConstraintNode::IsOrderBy(const TUniqueConstraintNode& unique) const {
    NSorted::TSimpleSet<TColumnsSet> columns;
    for (const auto& key : Content_)
        columns.insert_unique(key.first);

    const auto ordered = columns;
    for (const auto& set : unique.GetAllSets()) {
        if (std::all_of(set.cbegin(), set.cend(), [&ordered](const TPathType& path) {
            return !path.empty() && std::any_of(ordered.cbegin(), ordered.cend(), [&path](const TColumnsSet& s) { return s.contains(path); });
        })) {
            std::for_each(set.cbegin(), set.cend(), [&columns](const TPathType& path) {
                if (const auto it = std::find_if(columns.cbegin(), columns.cend(), [&path](const TColumnsSet& s) { return s.contains(path); }); columns.cend() != it)
                    columns.erase(it);
            });
            if (columns.empty())
                return true;
        }
    }

    return false;
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
        if (!constraints[i]->GetConstraint<TEmptyConstraintNode>()) {
            if (const auto sort = constraints[i]->GetConstraint<TSortedConstraintNode>()) {
                const auto& nextContent = sort->GetContent();
                if (content) {
                    const auto size = std::min(content->size(), nextContent.size());
                    content->resize(size);
                    for (auto j = 0U; j < size; ++j) {
                        auto& one = (*content)[j];
                        auto& two = nextContent[j];
                        TColumnsSet common;
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
            } else {
                content.reset();
                break;
            }
        }
    }

    return !content || content->empty() ? nullptr : ctx.MakeConstraint<TSortedConstraintNode>(std::move(*content));
}

const TSortedConstraintNode* TSortedConstraintNode::MakeCommon(const TSortedConstraintNode* other, TExprContext& ctx) const {
    if (!other) {
        return nullptr;
    }

    auto content = other->GetContent();
    const auto size = std::min(content.size(), Content_.size());
    content.resize(size);
    for (auto j = 0U; j < size; ++j) {
        auto& one = content[j];
        auto& two = Content_[j];
        TColumnsSet common;
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

const TSortedConstraintNode* TSortedConstraintNode::FilterByType(const TSortedConstraintNode* sorted, const TStructExprType* outItemType, TExprContext& ctx) {
    if (sorted) {
        auto content = sorted->GetContent();
        for (size_t i = 0; i < content.size(); ++i) {
            for (auto it = content[i].first.cbegin(); content[i].first.cend() != it;) {
                if (GetSubTypeByPath(*it, *outItemType))
                    ++it;
                else
                    it = content[i].first.erase(it);
            }

            if (content[i].first.empty()) {
                content.resize(i);
                break;
            }
        }

        if (!content.empty()) {
            return ctx.MakeConstraint<TSortedConstraintNode>(std::move(content));
        }
    }
    return nullptr;
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

bool TSortedConstraintNode::IsApplicableToType(const TTypeAnnotationNode& type) const {
    const auto& itemType = GetSeqItemType(type);
    return std::all_of(Content_.cbegin(), Content_.cend(), [&itemType](const std::pair<TColumnsSet, bool>& pair) {
        return std::all_of(pair.first.cbegin(), pair.first.cend(), std::bind(&GetSubTypeByPath, std::placeholders::_1, std::cref(itemType)));
    });
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TGroupByConstraintNode::TGroupByConstraintNode(TExprContext& ctx, const std::vector<TStringBuf>& columns)
    : TColumnSetConstraintNodeBase(ctx, Name(), columns)
{
    YQL_ENSURE(!Columns_.empty());
}

TGroupByConstraintNode::TGroupByConstraintNode(TExprContext& ctx, const std::vector<TString>& columns)
    : TColumnSetConstraintNodeBase(ctx, Name(), columns)
{
    YQL_ENSURE(!Columns_.empty());
}

TGroupByConstraintNode::TGroupByConstraintNode(TExprContext& ctx, const TGroupByConstraintNode& constr, size_t prefixLength)
    : TColumnSetConstraintNodeBase(ctx, Name(), std::vector<TStringBuf>(constr.GetColumns().begin(), constr.GetColumns().begin() + Min<size_t>(prefixLength, constr.GetColumns().size())))
{
    YQL_ENSURE(!Columns_.empty());
    YQL_ENSURE(Columns_.size() == prefixLength);
}

TGroupByConstraintNode::TGroupByConstraintNode(TGroupByConstraintNode&& constr)
    : TColumnSetConstraintNodeBase(std::move(static_cast<TColumnSetConstraintNodeBase&>(constr)))
{
}

size_t TGroupByConstraintNode::GetCommonPrefixLength(const TGroupByConstraintNode& node) const {
    const size_t minSize = Min(Columns_.size(), node.Columns_.size());
    for (size_t i = 0; i < minSize; ++i) {
        if (*(Columns_.begin() + i) != *(node.Columns_.begin() +i)) {
            return i;
        }
    }
    return minSize;
}

const TGroupByConstraintNode* TGroupByConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }

    auto groupBy = constraints.front()->GetConstraint<TGroupByConstraintNode>();
    if (constraints.size() == 1 || !groupBy) {
        return groupBy;
    }

    size_t commonPrefixLength = groupBy->GetColumns().size();
    for (size_t i = 1; i < constraints.size() && commonPrefixLength > 0; ++i) {
        if (auto nextGroupBy = constraints[i]->GetConstraint<TGroupByConstraintNode>()) {
            commonPrefixLength = Min(commonPrefixLength, nextGroupBy->GetCommonPrefixLength(*groupBy));
        } else {
            commonPrefixLength = 0;
        }
    }
    if (commonPrefixLength) {
        return ctx.MakeConstraint<TGroupByConstraintNode>(*groupBy, commonPrefixLength);
    }

    return nullptr;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

TUniqueConstraintNode::TSetType ColumnsListToSet(const std::vector<std::string_view>& columns) {
    YQL_ENSURE(!columns.empty());
    TUniqueConstraintNode::TSetType set;
    set.reserve(columns.size());
    std::transform(columns.cbegin(), columns.cend(), std::back_inserter(set), [](const std::string_view& column) { return TConstraintNode::TPathType(1U, column); });
    std::sort(set.begin(), set.end());
    return set;
}

TUniqueConstraintNode::TFullSetType DedupSets(TUniqueConstraintNode::TFullSetType&& sets) {
    for (bool found = true; found && sets.size() > 1U;) {
        found = false;
        for (auto ot = sets.cbegin(); !found && sets.cend() != ot; ++ot) {
            for (auto it = sets.cbegin(); sets.cend() != it;) {
                if (ot->size() < it->size() && std::all_of(ot->cbegin(), ot->cend(), [it](const TConstraintNode::TPathType& path) { return it->contains(path); })) {
                    it = sets.erase(it);
                    found = true;
                } else
                    ++it;
            }
        }
    }

    return std::move(sets);
}

}

TUniqueConstraintNode::TUniqueConstraintNode(TExprContext& ctx, TFullSetType&& sets)
    : TConstraintNode(ctx, Name()), Sets_(DedupSets(std::move(sets)))
{
    YQL_ENSURE(!Sets_.empty());
    const auto size = Sets_.size();
    Hash_ = MurmurHash<ui64>(&size, sizeof(size), Hash_);
    for (const auto& set : Sets_) {
        YQL_ENSURE(!set.empty());
        for (const auto& path : set)
            Hash_ = std::accumulate(path.cbegin(), path.cend(), Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
    }
}

TUniqueConstraintNode::TUniqueConstraintNode(TExprContext& ctx, const std::vector<std::string_view>& columns)
    : TUniqueConstraintNode(ctx, TFullSetType{ColumnsListToSet(columns)})
{}

TUniqueConstraintNode::TUniqueConstraintNode(TUniqueConstraintNode&& constr) = default;

bool TUniqueConstraintNode::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    if (const auto c = dynamic_cast<const TUniqueConstraintNode*>(&node)) {
        return Sets_ == c->Sets_;
    }
    return false;
}

bool TUniqueConstraintNode::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (const auto c = dynamic_cast<const TUniqueConstraintNode*>(&node)) {
        return std::includes(Sets_.cbegin(), Sets_.cend(), c->Sets_.cbegin(), c->Sets_.cend());
    }
    return false;
}

void TUniqueConstraintNode::Out(IOutputStream& out) const {
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

void TUniqueConstraintNode::ToJson(NJson::TJsonWriter& out) const {
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

const TUniqueConstraintNode* TUniqueConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }
    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TUniqueConstraintNode>();
    }

    TFullSetType sets;
    for (auto c: constraints) {
        if (const auto uniq = c->GetConstraint<TUniqueConstraintNode>()) {
            if (sets.empty())
                sets = uniq->GetAllSets();
            else {
                TFullSetType both;
                std::set_intersection(sets.cbegin(), sets.cend(), uniq->GetAllSets().cbegin(), uniq->GetAllSets().cend(), both.end());
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

    return sets.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNode>(std::move(sets));
}

bool TUniqueConstraintNode::HasEqualColumns(const std::vector<std::string_view>& columns) const {
    if (columns.empty())
        return false;

    const std::unordered_set<std::string_view> ordered(columns.cbegin(), columns.cend());
    std::unordered_set<std::string_view> uniques(columns.size());
    for (const auto& set : Sets_) {
        if (std::all_of(set.cbegin(), set.cend(), [&ordered](const TPathType& path) { return !path.empty() && ordered.contains(path.front()); })) {
            for (const auto& path : set) {
                if (!path.empty()) {
                    uniques.emplace(path.front());
                }
            }
            if (uniques.size() == ordered.size())
                return true;
        }
    }
    return false;
}

const TUniqueConstraintNode* TUniqueConstraintNode::FilterFields(TExprContext& ctx, const std::function<bool(const TPathType&)>& predicate) const {
    auto sets = Sets_;
    for (auto it = sets.cbegin(); sets.cend() != it;) {
        if (std::all_of(it->cbegin(), it->cend(), predicate))
            ++it;
        else
            it = sets.erase(it);
    }
    return sets.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNode>(std::move(sets));
}

const TUniqueConstraintNode* TUniqueConstraintNode::RenameFields(TExprContext& ctx, const std::function<std::vector<std::string_view>(const std::string_view&)>& reduce) const {
    TFullSetType sets;
    sets.reserve(Sets_.size());
    for (const auto& set : Sets_) {
        std::vector<TSetType> newSets(1U);
        newSets.front().reserve(set.size());
        for (const auto& path : set) {
            auto newNames = reduce(path.front());
            if (newNames.empty())
                break;
            auto tmpSets(std::move(newSets));
            for (const auto& newName : newNames) {
                for (const auto& oldSet : tmpSets) {
                    newSets.emplace_back(oldSet);
                    auto newPath = path;
                    newPath.front() = newName;
                    newSets.back().insert_unique(std::move(newPath));
                }
            }
        }
        if (set.size() == newSets.front().size())
            sets.insert_unique(newSets.cbegin(), newSets.cend());
    }
    return sets.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNode>(std::move(sets));
}

bool TUniqueConstraintNode::IsApplicableToType(const TTypeAnnotationNode& type) const {
    const auto& itemType = GetSeqItemType(type);
    return std::all_of(Sets_.cbegin(), Sets_.cend(), [&itemType](const TSetType& set) {
        return std::all_of(set.cbegin(), set.cend(), std::bind(&GetSubTypeByPath, std::placeholders::_1, std::cref(itemType)));
    });
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TPartOfUniqueConstraintNode::TPartOfUniqueConstraintNode(TExprContext& ctx, TMapType&& mapping)
    : TConstraintNode(ctx, Name()), Mapping_(std::move(mapping))
{
    YQL_ENSURE(!Mapping_.empty());
    for (const auto& part : Mapping_) {
        YQL_ENSURE(!part.second.empty());
        const auto hash = part.first->GetHash();
        Hash_ = MurmurHash<ui64>(&hash, sizeof(hash), Hash_);
        for (const auto& item: part.second) {
            Hash_ = std::accumulate(item.first.cbegin(), item.first.cend(), Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
            Hash_ = std::accumulate(item.second.cbegin(), item.second.cend(), Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
        }
    }
}

TPartOfUniqueConstraintNode::TPartOfUniqueConstraintNode(TPartOfUniqueConstraintNode&& constr) = default;

bool TPartOfUniqueConstraintNode::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    if (const auto c = dynamic_cast<const TPartOfUniqueConstraintNode*>(&node)) {
        return Mapping_ == c->Mapping_;
    }
    return false;
}

bool TPartOfUniqueConstraintNode::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (const auto c = dynamic_cast<const TPartOfUniqueConstraintNode*>(&node)) {
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

void TPartOfUniqueConstraintNode::Out(IOutputStream& out) const {
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

void TPartOfUniqueConstraintNode::ToJson(NJson::TJsonWriter& out) const {
    out.OpenMap();
    for (const auto& part : Mapping_) {
        for (const auto& [resultColumn, originalColumn] : part.second) {
            out.Write(JoinSeq(';', resultColumn), JoinSeq(';', originalColumn));
        }
    }
    out.CloseMap();
}

const TPartOfUniqueConstraintNode* TPartOfUniqueConstraintNode::ExtractField(TExprContext& ctx, const std::string_view& field) const {
    TMapType passtrought;
    for (const auto& part : Mapping_) {
        auto it = part.second.lower_bound(TPathType(1U, field));
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
    return passtrought.empty() ? nullptr : ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(passtrought));
}

const TPartOfUniqueConstraintNode* TPartOfUniqueConstraintNode::FilterFields(TExprContext& ctx, const std::function<bool(const TPathType& front)>& predicate) const {
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
    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(mapping));
}

TPartOfUniqueConstraintNode::TMapType TPartOfUniqueConstraintNode::GetColumnMapping(const std::string_view& asField) const {
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

const TPartOfUniqueConstraintNode* TPartOfUniqueConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }

    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TPartOfUniqueConstraintNode>();
    }

    bool first = true;
    TPartOfUniqueConstraintNode::TMapType mapping;
    for (size_t i = 0; i < constraints.size(); ++i) {
        auto part = constraints[i]->GetConstraint<TPartOfUniqueConstraintNode>();
        if (!part) {
            if (constraints[i]->GetConstraint<TEmptyConstraintNode>()) {
                continue;
            }
            return nullptr;
        }
        if (first) {
            mapping = part->GetColumnMapping();
            first = false;
        } else {
            for (const auto& nextMapping : part->GetColumnMapping()) {
                if (const auto it = mapping.find(nextMapping.first); mapping.cend() != it) {
                    TPartOfUniqueConstraintNode::TPartType result;
                    std::set_intersection(
                        it->second.cbegin(), it->second.cend(),
                        nextMapping.second.cbegin(), nextMapping.second.cend(),
                        std::back_inserter(result),
                        [] (const TPartOfUniqueConstraintNode::TPartType::value_type& c1, const TPartOfUniqueConstraintNode::TPartType::value_type& c2) {
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

    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(mapping));
}

const TPartOfUniqueConstraintNode::TMapType& TPartOfUniqueConstraintNode::GetColumnMapping() const {
    return Mapping_;
}

TPartOfUniqueConstraintNode::TMapType
TPartOfUniqueConstraintNode::GetCommonMapping(const TUniqueConstraintNode* complete, const TPartOfUniqueConstraintNode* incomplete, const std::string_view& field) {
    TMapType mapping;
    if (incomplete) {
        mapping = incomplete->GetColumnMapping();
        mapping.erase(complete);
        if (!field.empty()) {
            for (auto& part : mapping) {
                std::for_each(part.second.begin(), part.second.end(), [&field](TPartType::value_type& map) { map.first.push_front(field); });
            }
        }
    }

    if (complete) {
        auto& part = mapping[complete];
        for (const auto& set : complete->GetAllSets()) {
            for (const auto& path : set) {
                auto key = path;
                if (!field.empty())
                    key.emplace_front(field);
                part.insert_unique(std::make_pair(key, path));
            }
        }
    }

    return mapping;
}

void TPartOfUniqueConstraintNode::UniqueMerge(TMapType& output, TMapType&& input) {
    output.merge(std::move(input));
    while (!input.empty()) {
        const auto exists = input.extract(input.cbegin());
        auto& target = output[exists.key()];
        target.reserve(target.size() + exists.mapped().size());
        for (auto& item : exists.mapped())
            target.insert_unique(std::move(item));
    }
}

void TPartOfUniqueConstraintNode::FilterFields(TMapType& mapping, const std::function<bool(const std::string_view& front)>& predicate) {
    for (auto part = mapping.begin(); mapping.end() != part;) {
        for (auto it = part->second.cbegin(); part->second.cend() != it;) {
            if (it->first.empty() || !predicate(it->first.front()))
                it = part->second.erase(it);
            else
                ++it;
        }

        if (part->second.empty())
            part = mapping.erase(part);
        else
            ++part;
    }
}

TPartOfUniqueConstraintNode::TMapType
TPartOfUniqueConstraintNode::ExtractField(const TMapType& mapping, const std::string_view& field) {
    TMapType parts;
    for (const auto& part : mapping) {
        auto it = part.second.lower_bound(TPathType(1U, field));
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

const TUniqueConstraintNode* TPartOfUniqueConstraintNode::MakeComplete(TExprContext& ctx, const TMapType& mapping, const TUniqueConstraintNode* original) {
    if (const auto it = mapping.find(original); mapping.cend() != it) {
        TUniqueConstraintNode::TFullSetType newSets;
        for (const auto& set : it->first->GetAllSets()) {
            TReversePartType reverseMap;
            reverseMap.reserve(it->second.size());
            for (const auto& map : it->second)
                reverseMap[map.second].insert_unique(map.first);

            if (std::all_of(set.cbegin(), set.cend(), [reverseMap] (const TPathType& path) { return reverseMap.contains(path); })) {
                TUniqueConstraintNode::TFullSetType addSets = {TUniqueConstraintNode::TSetType()};
                for (const auto& old : set) {
                    const auto& renamed = reverseMap[old];
                    TUniqueConstraintNode::TFullSetType incSets;
                    for (auto s = addSets.begin(); addSets.end() != s; s = addSets.erase(s)) {
                        for (const auto& item : renamed) {
                            auto newSet = std::move(*s);
                            newSet.insert_unique(item);
                            incSets.insert_unique(std::move(newSet));
                        }
                    }
                    incSets.swap(addSets);
                }
                newSets.insert(addSets.cbegin(), addSets.cend());
            }
        }

        if (!newSets.empty())
            return ctx.MakeConstraint<TUniqueConstraintNode>(std::move(newSets));
    }

    return nullptr;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TPassthroughConstraintNode::TPassthroughConstraintNode(TExprContext& ctx, TMapType&& mapping)
    : TConstraintNode(ctx, Name()), Mapping_(std::move(mapping))
{
    YQL_ENSURE(!Mapping_.empty());
    if (const auto original = Mapping_.cbegin()->first;
        original && 1U == Mapping_.size() && 1U == original->Mapping_.size() && Mapping_.cbegin()->second == original->Mapping_.cbegin()->second) {
        // Comeback original constraint.
        Hash_ = original->Hash_;
        Mapping_ = original->Mapping_;
    } else {
        for (const auto& part : Mapping_) {
            YQL_ENSURE(!part.second.empty());
            if (part.first)
                Hash_ = MurmurHash<ui64>(&part.first->Hash_, sizeof(part.first->Hash_), Hash_);
            for (const auto& item: part.second) {
                Hash_ = std::accumulate(item.first.cbegin(), item.first.cend(), Hash_, [](ui64 hash, const std::string_view& field) { return MurmurHash<ui64>(field.data(), field.size(), hash); });
                Hash_ = MurmurHash<ui64>(item.second.data(), item.second.size(), Hash_);
            }
        }
    }
}

TPassthroughConstraintNode::TPassthroughConstraintNode(TExprContext& ctx, const TStructExprType& itemType)
    : TConstraintNode(ctx, Name())
{
    auto& mapping = Mapping_[nullptr];
    for (const auto& item: itemType.GetItems()) {
        const auto name = ctx.AppendString(item->GetName());
        Hash_ = MurmurHash<ui64>(name.data(), name.size(), Hash_); // hash as name
        Hash_ = MurmurHash<ui64>(name.data(), name.size(), Hash_); // hash as value
        mapping.push_back(std::make_pair(TPathType(1U, name), name)); // Struct items are sorted
    }

    YQL_ENSURE(!mapping.empty());
}

TPassthroughConstraintNode::TPassthroughConstraintNode(TExprContext& ctx, const TTupleExprType& itemType)
    : TConstraintNode(ctx, Name())
{
    auto& mapping = Mapping_[nullptr];
    for (ui32 i = 0U; i < itemType.GetSize(); ++i) {
        const auto name = ctx.GetIndexAsString(i);
        Hash_ = MurmurHash<ui64>(name.data(), name.size(), Hash_); // hash as name
        Hash_ = MurmurHash<ui64>(name.data(), name.size(), Hash_); // hash as value
        mapping.push_back(std::make_pair(TPathType(1U, name), name)); // Struct items are sorted
    }

    YQL_ENSURE(!mapping.empty());
}

TPassthroughConstraintNode::TPassthroughConstraintNode(TExprContext& ctx, const TMultiExprType& itemType)
    : TConstraintNode(ctx, Name())
{
    auto& mapping = Mapping_[nullptr];
    for (ui32 i = 0U; i < itemType.GetSize(); ++i) {
        const auto name = ctx.GetIndexAsString(i);
        Hash_ = MurmurHash<ui64>(name.data(), name.size(), Hash_); // hash as name
        Hash_ = MurmurHash<ui64>(name.data(), name.size(), Hash_); // hash as value
        mapping.push_back(std::make_pair(TPathType(1U, name), name)); // Struct items are sorted
    }

    YQL_ENSURE(!mapping.empty());
}

TPassthroughConstraintNode::TPassthroughConstraintNode(TPassthroughConstraintNode&& constr) = default;

bool TPassthroughConstraintNode::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    if (const auto c = dynamic_cast<const TPassthroughConstraintNode*>(&node)) {
        return Mapping_ == c->Mapping_;
    }
    return false;
}

bool TPassthroughConstraintNode::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (const auto c = dynamic_cast<const TPassthroughConstraintNode*>(&node)) {
        for (const auto& part : c->Mapping_) {
            if (const auto it = Mapping_.find(part.first == this ? nullptr : part.first); Mapping_.cend() != it) {
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

void TPassthroughConstraintNode::Out(IOutputStream& out) const {
    TConstraintNode::Out(out);
    out.Write('(');

    bool first = true;
    for (const auto& part : Mapping_) {
        for (const auto& item : part.second) {
            if (!first) {
                out.Write(',');
            }
            if (!item.first.empty()) {
                auto it = item.first.cbegin();
                out.Write(*it);
                while (item.first.cend() > ++it) {
                    out.Write('#');
                    out.Write(*it);
                }
            }
            out.Write(':');
            out.Write(item.second);

            first = false;
        }
    }
    out.Write(')');
}

void TPassthroughConstraintNode::ToJson(NJson::TJsonWriter& out) const {
    out.OpenMap();
    for (const auto& part : Mapping_) {
        for (const auto& [resultColumn, originalColumn] : part.second) {
            out.Write(JoinSeq(';', resultColumn), originalColumn);
        }
    }
    out.CloseMap();
}

const TPassthroughConstraintNode* TPassthroughConstraintNode::ExtractField(TExprContext& ctx, const std::string_view& field) const {
    TMapType passtrought;
    for (const auto& part : Mapping_) {
        auto it = part.second.lower_bound(TPathType(1U, field));
        if (part.second.cend() == it || it->first.front() != field)
            continue;

        if (1U == it->first.size()) {
            return ctx.MakeConstraint<TPassthroughConstraintNode>(TMapType{{part.first ? part.first : this , TPartType{{TPathType(), it->second}}}});
        }

        TPartType mapping;
        mapping.reserve(part.second.size());
        while (it < part.second.cend() && it->first.size() > 1U && field == it->first.front()) {
            auto item = *it++;
            item.first.pop_front();
            mapping.emplace_back(std::move(item));
        }

        if (!mapping.empty()) {
            passtrought.emplace(part.first ? part.first : this, std::move(mapping));
        }
    }
    return passtrought.empty() ? nullptr : ctx.MakeConstraint<TPassthroughConstraintNode>(std::move(passtrought));
}

const TPassthroughConstraintNode* TPassthroughConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }

    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TPassthroughConstraintNode>();
    }

    bool first = true;
    TPassthroughConstraintNode::TMapType mapping;
    for (size_t i = 0; i < constraints.size(); ++i) {
        auto part = constraints[i]->GetConstraint<TPassthroughConstraintNode>();
        if (!part) {
            if (constraints[i]->GetConstraint<TEmptyConstraintNode>()) {
                continue;
            }
            return nullptr;
        }
        if (first) {
            mapping = part->GetColumnMapping();
            if (const auto self = mapping.find(nullptr); mapping.cend() != self) {
                mapping.emplace(part, std::move(mapping.extract(self).mapped()));
            }
            first = false;
        } else {
            for (const auto& nextMapping : part->GetColumnMapping()) {
                if (const auto it = mapping.find(nextMapping.first ? nextMapping.first : part); mapping.cend() != it) {
                    TPassthroughConstraintNode::TPartType result;
                    std::set_intersection(
                        it->second.cbegin(), it->second.cend(),
                        nextMapping.second.cbegin(), nextMapping.second.cend(),
                        std::back_inserter(result),
                        [] (const TPassthroughConstraintNode::TPartType::value_type& c1, const TPassthroughConstraintNode::TPartType::value_type& c2) {
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

    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPassthroughConstraintNode>(std::move(mapping));
}

const TPassthroughConstraintNode::TMapType& TPassthroughConstraintNode::GetColumnMapping() const {
    return Mapping_;
}

TPassthroughConstraintNode::TReverseMapType TPassthroughConstraintNode::GetReverseMapping() const {
    if (1U == Mapping_.size() && 1U == Mapping_.cbegin()->second.size() && Mapping_.cbegin()->second.cbegin()->first.empty())
        return {{Mapping_.cbegin()->second.cbegin()->second, Mapping_.cbegin()->second.cbegin()->second}};

    TReverseMapType reverseMapping;
    for (const auto& part : Mapping_) {
        for (const auto& item : part.second) {
            if (1U == item.first.size()) {
                reverseMapping.emplace_back(item.second, item.first.front());
            }
        }
    }
    ::Sort(reverseMapping);
    return reverseMapping;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

TEmptyConstraintNode::TEmptyConstraintNode(TExprContext& ctx)
    : TConstraintNode(ctx, Name())
{
}

TEmptyConstraintNode::TEmptyConstraintNode(TEmptyConstraintNode&& constr)
    : TConstraintNode(std::move(static_cast<TConstraintNode&>(constr)))
{
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

TVarIndexConstraintNode::TVarIndexConstraintNode(TVarIndexConstraintNode&& constr)
    : TConstraintNode(std::move(static_cast<TConstraintNode&>(constr)))
    , Mapping_(std::move(constr.Mapping_))
{
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

TMultiConstraintNode::TMultiConstraintNode(TExprContext& ctx, const TMapType& items)
    : TConstraintNode(ctx, Name())
{
    YQL_ENSURE(items.size());
    for (auto& item: items) {
        Hash_ = MurmurHash<ui64>(&item.first, sizeof(item.first), Hash_);
        for (auto c: item.second.GetAllConstraints()) {
            const auto itemHash = c->GetHash();
            Hash_ = MurmurHash<ui64>(&itemHash, sizeof(itemHash), Hash_);
        }
    }
    Items_ = items;
}

TMultiConstraintNode::TMultiConstraintNode(TExprContext& ctx, ui32 index, const TConstraintSet& constraints)
    : TMultiConstraintNode(ctx, TMapType{{index, constraints}})
{
}

TMultiConstraintNode::TMultiConstraintNode(TMultiConstraintNode&& constr)
    : TConstraintNode(std::move(static_cast<TConstraintNode&>(constr)))
    , Items_(std::move(constr.Items_))
{
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

const TMultiConstraintNode* TMultiConstraintNode::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    } else if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TMultiConstraintNode>();
    }

    TMultiConstraintNode::TMapType multiItems;
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
                std::vector<TMultiConstraintNode::TMapType::value_type> nonEmpty;
                std::copy_if(start, cur, std::back_inserter(nonEmpty),
                    [] (const TMultiConstraintNode::TMapType::value_type& v) {
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

} // namespace NYql

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<>
void Out<NYql::TConstraintNode::TPathType>(IOutputStream& out, const NYql::TConstraintNode::TPathType& path) {
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
void Out<NYql::TConstraintNode>(IOutputStream& out, const NYql::TConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TSortedConstraintNode>(IOutputStream& out, const NYql::TSortedConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TGroupByConstraintNode>(IOutputStream& out, const NYql::TGroupByConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TUniqueConstraintNode>(IOutputStream& out, const NYql::TUniqueConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TPartOfUniqueConstraintNode>(IOutputStream& out, const NYql::TPartOfUniqueConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TPassthroughConstraintNode>(IOutputStream& out, const NYql::TPassthroughConstraintNode& c) {
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
