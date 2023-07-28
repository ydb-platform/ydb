#include "dnf.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

#include <util/generic/hash.h>

namespace NYT {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TConjunctiveClause::TConjunctiveClause(const std::vector<TString>& include, const std::vector<TString>& exclude)
    : Include_(include)
    , Exclude_(exclude)
{
    std::sort(Include_.begin(), Include_.end());
    std::sort(Exclude_.begin(), Exclude_.end());
    // Exception can be thrown here.
    Validate();
}

void TConjunctiveClause::Validate() const
{
    for (const auto& includeItem : Include_) {
        if (std::binary_search(Exclude_.begin(), Exclude_.end(), includeItem)) {
            THROW_ERROR_EXCEPTION("Include and exclude sets must be disjoint, but item %Qv is present in both",
                includeItem);
        }
    }
}

template <class TContainer>
bool TConjunctiveClause::IsSatisfiedByImpl(const TContainer& value) const
{
    int includeCount = 0;
    for (const auto& item : value) {
        if (std::binary_search(Exclude_.begin(), Exclude_.end(), item)) {
            return false;
        }
        if (std::binary_search(Include_.begin(), Include_.end(), item)) {
            ++includeCount;
        }
    }
    return includeCount == std::ssize(Include_);
}

bool TConjunctiveClause::IsSatisfiedBy(const std::vector<TString>& value) const
{
    return IsSatisfiedByImpl(value);
}

bool TConjunctiveClause::IsSatisfiedBy(const THashSet<TString>& value) const
{
    return IsSatisfiedByImpl(value);
}

size_t TConjunctiveClause::GetHash() const
{
    const size_t multiplier = 1000003;

    auto hashOfSet = [] (const std::vector<TString>& container) {
        const size_t multiplier = 67;

        size_t result = 0;
        for (const auto& str : container) {
            result = result * multiplier + ComputeHash(str);
        }
        return result;
    };

    return multiplier * hashOfSet(Include_) + hashOfSet(Exclude_);
}

void Serialize(const TConjunctiveClause& clause, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("include").Value(clause.Include())
            .Item("exclude").Value(clause.Exclude())
        .EndMap();
}

void Deserialize(TConjunctiveClause& clause, INodePtr node)
{
    if (node->GetType() == ENodeType::String) {
        clause.Include() = std::vector<TString>({node->AsString()->GetValue()});
    } else if (node->GetType() == ENodeType::Map) {
        auto mapNode = node->AsMap();
        auto includeNode = mapNode->FindChild("include");
        if (includeNode) {
            if (includeNode->GetType() != ENodeType::List) {
                THROW_ERROR_EXCEPTION("Conjunction include item must be \"list\"");
            }
            clause.Include() = ConvertTo<std::vector<TString>>(includeNode);
        }

        auto excludeNode = mapNode->FindChild("exclude");
        if (excludeNode) {
            if (excludeNode->GetType() != ENodeType::List) {
                THROW_ERROR_EXCEPTION("Conjunction exclude item must be \"list\"");
            }
            clause.Exclude() = ConvertTo<std::vector<TString>>(excludeNode);
        }
    } else {
        THROW_ERROR_EXCEPTION("Conjunction clause can only be parsed from \"string\" or \"map\"");
    }
}

bool operator==(const TConjunctiveClause& lhs, const TConjunctiveClause& rhs)
{
    return lhs.Include() == rhs.Include() &&
        lhs.Exclude() == rhs.Exclude();
}

bool operator<(const TConjunctiveClause& lhs, const TConjunctiveClause& rhs)
{
    if (lhs.Include() != rhs.Include()) {
        return lhs.Include() < rhs.Include();
    }
    return lhs.Exclude() < rhs.Exclude();
}

////////////////////////////////////////////////////////////////////////////////

TDnfFormula::TDnfFormula(const std::vector<TConjunctiveClause>& clauses)
    : Clauses_(clauses)
{
    std::sort(Clauses_.begin(), Clauses_.end());
}

template <class TContainer>
bool TDnfFormula::IsSatisfiedByImpl(const TContainer& value) const
{
    for (const auto& clause : Clauses_) {
        if (clause.IsSatisfiedBy(value)) {
            return true;
        }
    }
    return false;
}

bool TDnfFormula::IsSatisfiedBy(const std::vector<TString>& value) const
{
    return IsSatisfiedByImpl(value);
}

bool TDnfFormula::IsSatisfiedBy(const THashSet<TString>& value) const
{
    return IsSatisfiedByImpl(value);
}

size_t TDnfFormula::GetHash() const
{
    const size_t multiplier = 424243;

    size_t result = 0;
    for (const auto& clause : Clauses_) {
        result = result * multiplier + clause.GetHash();
    }
    return result;
}

bool operator<(const TDnfFormula& lhs, const TDnfFormula& rhs)
{
    return lhs.Clauses() < rhs.Clauses();
}

bool operator==(const TDnfFormula& lhs, const TDnfFormula& rhs)
{
    return lhs.Clauses() == rhs.Clauses();
}

void Serialize(const TDnfFormula& dnf, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoListFor(dnf.Clauses(), [=] (TFluentList fluent, const TConjunctiveClause& clause) {
            fluent
                .Item().Value(clause);
        });
}

void Deserialize(TDnfFormula& dnf, NYTree::INodePtr node)
{
    auto clauses = ConvertTo<std::vector<TConjunctiveClause>>(node);
    dnf = TDnfFormula(clauses);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::TConjunctiveClause>::operator()(const NYT::TConjunctiveClause& clause) const
{
    return clause.GetHash();
}

size_t THash<NYT::TDnfFormula>::operator()(const NYT::TDnfFormula& dnf) const
{
    return dnf.GetHash();
}

////////////////////////////////////////////////////////////////////////////////

