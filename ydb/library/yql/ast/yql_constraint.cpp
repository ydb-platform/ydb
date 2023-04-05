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

TSortedConstraintNode::TFullSetType TSortedConstraintNode::GetAllSets() const {
    TFullSetType sets;
    for (const auto& key : Content_)
        sets.insert_unique(key.first);
    return sets;
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

const TSortedConstraintNode* TSortedConstraintNode::FilterFields(TExprContext& ctx, const TPathFilter& filter) const {
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

const TSortedConstraintNode* TSortedConstraintNode::RenameFields(TExprContext& ctx, const TPathReduce& reduce) const {
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

const TConstraintNode* TSortedConstraintNode::OnlySimpleColumns(TExprContext& ctx) const {
    return FilterFields(ctx, std::bind(std::equal_to<TPathType::size_type>(), std::bind(&TPathType::size, std::placeholders::_1), 1ULL));
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

template<bool Distinct>
typename TUniqueConstraintNodeBase<Distinct>::TSetType
TUniqueConstraintNodeBase<Distinct>::ColumnsListToSet(const std::vector<std::string_view>& columns) {
    YQL_ENSURE(!columns.empty());
    TSetType set;
    set.reserve(columns.size());
    std::transform(columns.cbegin(), columns.cend(), std::back_inserter(set), [](const std::string_view& column) { return TPathType(1U, column); });
    std::sort(set.begin(), set.end());
    return set;
}

template<bool Distinct>
typename TUniqueConstraintNodeBase<Distinct>::TFullSetType
TUniqueConstraintNodeBase<Distinct>::DedupSets(TFullSetType&& sets) {
    for (bool found = true; found && sets.size() > 1U;) {
        found = false;
        for (auto ot = sets.cbegin(); !found && sets.cend() != ot; ++ot) {
            for (auto it = sets.cbegin(); sets.cend() != it;) {
                if (ot->size() < it->size() && std::all_of(ot->cbegin(), ot->cend(), [it](const TPathType& path) { return it->contains(path); })) {
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
TUniqueConstraintNodeBase<Distinct>::TUniqueConstraintNodeBase(TExprContext& ctx, TFullSetType&& sets)
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

template<bool Distinct>
TUniqueConstraintNodeBase<Distinct>::TUniqueConstraintNodeBase(TExprContext& ctx, const std::vector<std::string_view>& columns)
    : TUniqueConstraintNodeBase(ctx, TFullSetType{ColumnsListToSet(columns)})
{}

template<bool Distinct>
TUniqueConstraintNodeBase<Distinct>::TUniqueConstraintNodeBase(TUniqueConstraintNodeBase&& constr) = default;

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
        return false;
    }
    if (const auto c = dynamic_cast<const TUniqueConstraintNodeBase*>(&node)) {
        return Sets_ == c->Sets_;
    }
    return false;
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::Includes(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (const auto c = dynamic_cast<const TUniqueConstraintNodeBase*>(&node)) {
        return std::includes(Sets_.cbegin(), Sets_.cend(), c->Sets_.cbegin(), c->Sets_.cend());
    }
    return false;
}

template<bool Distinct>
void TUniqueConstraintNodeBase<Distinct>::Out(IOutputStream& out) const {
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

template<bool Distinct>
void TUniqueConstraintNodeBase<Distinct>::ToJson(NJson::TJsonWriter& out) const {
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

template<bool Distinct>
const TUniqueConstraintNodeBase<Distinct>* TUniqueConstraintNodeBase<Distinct>::MakeCommon(const std::vector<const TConstraintSet*>& constraints, TExprContext& ctx) {
    if (constraints.empty()) {
        return nullptr;
    }
    if (constraints.size() == 1) {
        return constraints.front()->GetConstraint<TUniqueConstraintNodeBase>();
    }

    TFullSetType sets;
    for (auto c: constraints) {
        if (const auto uniq = c->GetConstraint<TUniqueConstraintNodeBase>()) {
            if (sets.empty())
                sets = uniq->GetAllSets();
            else {
                TFullSetType both;
                both.reserve(std::min(sets.size(), uniq->GetAllSets().size()));
                std::set_intersection(sets.cbegin(), sets.cend(), uniq->GetAllSets().cbegin(), uniq->GetAllSets().cend(), std::back_inserter(both));
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

    return sets.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNodeBase>(std::move(sets));
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::IsOrderBy(const TSortedConstraintNode& sorted) const {
    auto columns = sorted.GetAllSets();
    const auto ordered = columns;
    for (const auto& set : GetAllSets()) {
        if (std::all_of(set.cbegin(), set.cend(), [&ordered](const TPathType& path) {
            return !path.empty() && std::any_of(ordered.cbegin(), ordered.cend(), [&path](const TSetType& s) { return s.contains(path); });
        })) {
            std::for_each(set.cbegin(), set.cend(), [&columns](const TPathType& path) {
                if (const auto it = std::find_if(columns.cbegin(), columns.cend(), [&path](const TSetType& s) { return s.contains(path); }); columns.cend() != it)
                    columns.erase(it);
            });
            if (columns.empty())
                return true;
        }
    }

    return false;
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::HasEqualColumns(const std::vector<std::string_view>& columns) const {
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

template<bool Distinct>
void TUniqueConstraintNodeBase<Distinct>::FilterUncompleteReferences(TSetType& references) const {
    for (const auto& set : Sets_) {
        if (!std::all_of(set.cbegin(), set.cend(), std::bind(&TSetType::contains<TPathType>, std::cref(references), std::placeholders::_1)))
            std::for_each(set.cbegin(), set.cend(), [&references] (const TPathType& path) { references.erase(path); });
    }
}

template<bool Distinct>
const TUniqueConstraintNodeBase<Distinct>*
TUniqueConstraintNodeBase<Distinct>::FilterFields(TExprContext& ctx, const TPathFilter& predicate) const {
    auto sets = Sets_;
    for (auto it = sets.cbegin(); sets.cend() != it;) {
        if (std::all_of(it->cbegin(), it->cend(), predicate))
            ++it;
        else
            it = sets.erase(it);
    }
    return sets.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNodeBase>(std::move(sets));
}

template<bool Distinct>
const TUniqueConstraintNodeBase<Distinct>*
TUniqueConstraintNodeBase<Distinct>::RenameFields(TExprContext& ctx, const TPathReduce& reduce) const {
    TFullSetType sets;
    sets.reserve(Sets_.size());
    for (const auto& set : Sets_) {
        std::vector<TSetType> newSets(1U);
        newSets.front().reserve(set.size());
        for (const auto& path : set) {
            auto newPaths = reduce(path);
            if (newPaths.empty())
                break;
            auto tmpSets(std::move(newSets));
            for (const auto& newPath : newPaths) {
                for (const auto& oldSet : tmpSets) {
                    newSets.emplace_back(oldSet);
                    newSets.back().insert_unique(newPath);
                }
            }
        }
        if (set.size() == newSets.front().size())
            sets.insert_unique(newSets.cbegin(), newSets.cend());
    }
    return sets.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNodeBase>(std::move(sets));
}

template<bool Distinct>
const TUniqueConstraintNodeBase<Distinct>*
TUniqueConstraintNodeBase<Distinct>::MakeCommon(const TUniqueConstraintNodeBase* other, TExprContext& ctx) const {
    if (!other) {
        return nullptr;
    }
    if (this == other) {
        return this;
    }

    TFullSetType both;
    both.reserve(std::min(Sets_.size(), other->Sets_.size()));
    std::set_intersection(Sets_.cbegin(), Sets_.cend(), other->Sets_.cbegin(), other->Sets_.cend(), std::back_inserter(both));
    return both.empty() ? nullptr : ctx.MakeConstraint<TUniqueConstraintNodeBase>(std::move(both));
}

template<bool Distinct>
const TUniqueConstraintNodeBase<Distinct>* TUniqueConstraintNodeBase<Distinct>::Merge(const TUniqueConstraintNodeBase* one, const TUniqueConstraintNodeBase* two, TExprContext& ctx) {
    if (!one)
        return two;
    if (!two)
        return one;

    auto sets = one->GetAllSets();
    sets.insert_unique(two->GetAllSets().cbegin(), two->GetAllSets().cend());
    return ctx.MakeConstraint<TUniqueConstraintNodeBase<Distinct>>(std::move(sets));
}

template<bool Distinct>
bool TUniqueConstraintNodeBase<Distinct>::IsApplicableToType(const TTypeAnnotationNode& type) const {
    if (ETypeAnnotationKind::Dict == type.GetKind())
        return true; // TODO: check for dict.
    const auto& itemType = GetSeqItemType(type);
    return std::all_of(Sets_.cbegin(), Sets_.cend(), [&itemType](const TSetType& set) {
        return std::all_of(set.cbegin(), set.cend(), std::bind(&GetSubTypeByPath, std::placeholders::_1, std::cref(itemType)));
    });
}

template<bool Distinct>
const TConstraintNode* TUniqueConstraintNodeBase<Distinct>::OnlySimpleColumns(TExprContext& ctx) const {
    return FilterFields(ctx, std::bind(std::equal_to<typename TPathType::size_type>(), std::bind(&TPathType::size, std::placeholders::_1), 1ULL));
}

template class TUniqueConstraintNodeBase<false>;
template class TUniqueConstraintNodeBase<true>;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<class TOriginalConstraintNode>
TPartOfConstraintNode<TOriginalConstraintNode>::TPartOfConstraintNode(TExprContext& ctx, TMapType&& mapping)
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

template<class TOriginalConstraintNode>
TPartOfConstraintNode<TOriginalConstraintNode>::TPartOfConstraintNode(TPartOfConstraintNode&& constr) = default;

template<class TOriginalConstraintNode>
bool TPartOfConstraintNode<TOriginalConstraintNode>::Equals(const TConstraintNode& node) const {
    if (this == &node) {
        return true;
    }
    if (GetHash() != node.GetHash()) {
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
const TPartOfConstraintNode<TOriginalConstraintNode>*
TPartOfConstraintNode<TOriginalConstraintNode>::ExtractField(TExprContext& ctx, const std::string_view& field) const {
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
    return passtrought.empty() ? nullptr : ctx.MakeConstraint<TPartOfConstraintNode>(std::move(passtrought));
}

template<class TOriginalConstraintNode>
const TPartOfConstraintNode<TOriginalConstraintNode>*
TPartOfConstraintNode<TOriginalConstraintNode>::FilterFields(TExprContext& ctx, const TPathFilter& predicate) const {
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
const TPartOfConstraintNode<TOriginalConstraintNode>*
TPartOfConstraintNode<TOriginalConstraintNode>::RenameFields(TExprContext& ctx, const TPathReduce& rename) const {
    auto mapping = Mapping_;
    for (auto part = mapping.begin(); mapping.end() != part;) {
        TPartType old;
        part->second.swap(old);
        for (auto& item : std::move(old)) {
            for (auto& path : rename(item.first)) {
                part->second.insert_unique(std::make_pair(std::move(path), std::move(item.second)));
            }
        }

        if (part->second.empty())
            part = mapping.erase(part);
        else
            ++part;
    }
    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPartOfConstraintNode>(std::move(mapping));
}

template<class TOriginalConstraintNode>
const TPartOfConstraintNode<TOriginalConstraintNode>*
TPartOfConstraintNode<TOriginalConstraintNode>::CompleteOnly(TExprContext& ctx) const {
    TMapType mapping(Mapping_);

    for (auto it = mapping.begin(); mapping.end() != it;) {
        TSetType set;
        set.reserve(it->second.size());
        std::for_each(it->second.cbegin(), it->second.cend(), [&](const TPartType::value_type& pair) { set.insert_unique(pair.second); });

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
                        [] (const TPartOfConstraintNode<TOriginalConstraintNode>::TPartType::value_type& c1, const TPartOfConstraintNode<TOriginalConstraintNode>::TPartType::value_type& c2) {
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

template<class TOriginalConstraintNode>
void TPartOfConstraintNode<TOriginalConstraintNode>::UniqueMerge(TMapType& output, TMapType&& input) {
    output.merge(std::move(input));
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

template<class TOriginalConstraintNode>
const TOriginalConstraintNode*
TPartOfConstraintNode<TOriginalConstraintNode>::MakeComplete(TExprContext& ctx, const TMapType& mapping, const TOriginalConstraintNode* original, const std::string_view& field) {
    if (const auto it = mapping.find(original); mapping.cend() != it) {
        TReversePartType reverseMap;
        reverseMap.reserve(it->second.size());
        for (const auto& map : it->second)
            reverseMap[map.second].insert_unique(map.first);

        const auto rename = [&](const TPathType& path) {
            const auto& set = reverseMap[path];
            std::vector<TPathType> out(set.cbegin(), set.cend());
            if (!field.empty())
                std::for_each(out.begin(), out.end(), [&field](TPathType& path) { path.emplace_front(field); });
            return out;
        };

        return it->first->RenameFields(ctx, rename);
    }

    return nullptr;
}

template<class TOriginalConstraintNode>
bool TPartOfConstraintNode<TOriginalConstraintNode>::IsApplicableToType(const TTypeAnnotationNode& type) const {
    if (ETypeAnnotationKind::Dict == type.GetKind())
        return true; // TODO: check for dict.

    const auto itemType = GetSeqItemType(&type);
    const auto& actualType = itemType ? *itemType : type;
    return std::all_of(Mapping_.cbegin(), Mapping_.cend(), [&actualType](const typename TMapType::value_type& pair) {
        return std::all_of(pair.second.cbegin(), pair.second.cend(), [&actualType](const typename TPartType::value_type& part) { return bool(GetSubTypeByPath(part.first, actualType)); });
    });
}

template class TPartOfConstraintNode<TSortedConstraintNode>;
template class TPartOfConstraintNode<TUniqueConstraintNode>;
template class TPartOfConstraintNode<TDistinctConstraintNode>;

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

TPassthroughConstraintNode::TPassthroughConstraintNode(TExprContext& ctx, const ui32 width)
    : TConstraintNode(ctx, Name())
{
    auto& mapping = Mapping_[nullptr];
    for (ui32 i = 0U; i < width; ++i) {
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

void TPassthroughConstraintNode::UniqueMerge(TMapType& output, TMapType&& input) {
    output.merge(std::move(input));
    while (!input.empty()) {
        const auto exists = input.extract(input.cbegin());
        auto& target = output[exists.key()];
        target.reserve(target.size() + exists.mapped().size());
        for (auto& item : exists.mapped())
            target.insert_unique(std::move(item));
    }
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

const TPassthroughConstraintNode* TPassthroughConstraintNode::MakeCommon(const TPassthroughConstraintNode* other, TExprContext& ctx) const {
    if (!other) {
        return nullptr;
    } else if (this == other) {
        return this;
    }

    auto mapping = GetColumnMapping();
    if (const auto self = mapping.find(nullptr); mapping.cend() != self)
        mapping.emplace(this, std::move(mapping.extract(self).mapped()));

    for (const auto& nextMapping : other->GetColumnMapping()) {
        if (const auto it = mapping.find(nextMapping.first ? nextMapping.first : other); mapping.cend() != it) {
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

    return mapping.empty() ? nullptr : ctx.MakeConstraint<TPassthroughConstraintNode>(std::move(mapping));
}

const TPassthroughConstraintNode::TMapType& TPassthroughConstraintNode::GetColumnMapping() const {
    return Mapping_;
}

TPassthroughConstraintNode::TMapType TPassthroughConstraintNode::GetColumnMapping(const std::string_view& field) const {
    TMapType mapping(Mapping_.size());
    for (const auto& map : Mapping_) {
        TPartType part;
        part.reserve(map.second.size());
        std::transform(map.second.cbegin(), map.second.cend(), std::back_inserter(part), [&field](TPartType::value_type item) {
            item.first.emplace_front(field);
            return item;
        });
        mapping.emplace(map.first ? map.first : this, std::move(part));
    }
    return mapping;
}

TPassthroughConstraintNode::TMapType TPassthroughConstraintNode::GetColumnMapping(TExprContext& ctx, const std::string_view& prefix) const {
    TMapType mapping(Mapping_.size());
    for (const auto& map : Mapping_) {
        TPartType part;
        part.reserve(map.second.size());
        const TString str(prefix);
        std::transform(map.second.cbegin(), map.second.cend(), std::back_inserter(part), [&](TPartType::value_type item) {
            if (item.first.empty())
                item.first.emplace_front(prefix);
            else
                item.first.front() = ctx.AppendString(str + item.first.front());
            return item;
        });
        mapping.emplace(map.first ? map.first : this, std::move(part));
    }
    return mapping;
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

const TConstraintNode* TMultiConstraintNode::OnlySimpleColumns(TExprContext& ctx) const {
    auto items = Items_;
    for (auto& item : items) {
        TConstraintSet newSet;
        for (const auto& constraint : item.second.GetAllConstraints())
            if (const auto filtered = constraint->OnlySimpleColumns(ctx))
                newSet.AddConstraint(filtered);
        item.second = std::move(newSet);
    }
    return ctx.MakeConstraint<TMultiConstraintNode>(std::move(items));
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
void Out<NYql::TConstraintSet>(IOutputStream& out, const NYql::TConstraintSet& s) {
    s.Out(out);
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
void Out<NYql::TDistinctConstraintNode>(IOutputStream& out, const NYql::TDistinctConstraintNode& c) {
    c.Out(out);
}

template<>
void Out<NYql::TPartOfSortedConstraintNode>(IOutputStream& out, const NYql::TPartOfSortedConstraintNode& c) {
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
