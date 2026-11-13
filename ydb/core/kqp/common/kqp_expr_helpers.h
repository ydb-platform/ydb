#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <optional>
#include <string_view>
#include <unordered_set>

namespace NKikimr::NKqp::NOpt {

const std::unordered_set<std::string_view>& GetNonDeterministicFunctions();
// These calls are replaced with cached query parameters by the peephole optimizer.
bool IsQueryBoundNonDeterministic(const NYql::TExprNode& node);
bool IsFalseBool(const NYql::TExprNode& node);

struct TLikeExpression {
    NYql::TExprNode::TPtr Column;
    NYql::TExprNode::TPtr Pattern;
    std::optional<char> Escape;
    bool CaseSensitive = true;
};

// Accepts a LIKE/ILIKE apply, Coalesce(match, false), or Map/FlatMap for an optional column,
// only when the match is necessary for the predicate to be true. Patterns may be dynamic:
// callers validate/evaluate them, or use ExtractLiteral when a literal pattern is required.
// AND is split by callers so each conjunct can supply its own index constraint.
// NOT and OR are not traversed and supply no hint.
std::optional<TLikeExpression> TryParseLikeExpression(const NYql::TExprNode::TPtr& predicate);

bool ExtractLiteral(const NYql::TExprNode::TPtr& node, TString& value);
// Accept only identity/string conversions of a table member, including UDF AutoMap.
// SafeCast/ToString accept only String/Utf8 inputs; other types must not receive string index constraints.
NYql::TExprNode::TPtr MemberColumn(NYql::TExprNode::TPtr node);

struct TFulltextPatternQuery {
    NYql::TExprNode::TPtr Pattern;
    std::optional<char> Escape;
    TStringBuf Mode;
};

std::optional<TFulltextPatternQuery> TryParseFulltextPatternQuery(const NYql::TExprNode::TPtr& node);

}   // namespace NKikimr::NKqp::NOpt
