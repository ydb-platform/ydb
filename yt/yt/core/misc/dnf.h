#pragma once

#include "property.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TConjunctiveClause
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TString>, Include);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TString>, Exclude);

public:
    TConjunctiveClause() = default;
    TConjunctiveClause(const std::vector<TString>& include, const std::vector<TString>& exclude);

    bool IsSatisfiedBy(const std::vector<TString>& value) const;
    bool IsSatisfiedBy(const THashSet<TString>& value) const;

    size_t GetHash() const;

private:
    void Validate() const;

    template<class TContainer>
    bool IsSatisfiedByImpl(const TContainer& value) const;
};

bool operator<(const TConjunctiveClause& lhs, const TConjunctiveClause& rhs);
bool operator==(const TConjunctiveClause& lhs, const TConjunctiveClause& rhs);

void Serialize(const TConjunctiveClause& rule, NYson::IYsonConsumer* consumer);
void Deserialize(TConjunctiveClause& rule, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TDnfFormula
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TConjunctiveClause>, Clauses);

public:
    explicit TDnfFormula(const std::vector<TConjunctiveClause>& clauses = {});

    bool IsSatisfiedBy(const std::vector<TString>& value) const;
    bool IsSatisfiedBy(const THashSet<TString>& value) const;

    size_t GetHash() const;

private:
    template<class TContainer>
    bool IsSatisfiedByImpl(const TContainer& value) const;
};

bool operator<(const TDnfFormula& lhs, const TDnfFormula& rhs);
bool operator==(const TDnfFormula& lhs, const TDnfFormula& rhs);

void Serialize(const TDnfFormula& rule, NYson::IYsonConsumer* consumer);
void Deserialize(TDnfFormula& rule, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::TConjunctiveClause>
{
    size_t operator()(const NYT::TConjunctiveClause& clause) const;
};

template <>
struct THash<NYT::TDnfFormula>
{
    size_t operator()(const NYT::TDnfFormula& dnf) const;
};

////////////////////////////////////////////////////////////////////////////////
