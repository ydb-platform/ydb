#pragma once

#include "ast_nodes.h"

#include <ydb/library/yql/parser/proto_ast/gen/jsonpath/JsonPathParser.pb.h>

namespace NYql::NJsonPath {

class TAstBuilder {
public:
    TAstBuilder(TIssues& issues);

    TAstNodePtr Build(const NJsonPathGenerated::TJsonPathParserAST& ast);

private:
    TArrayAccessNode::TSubscript BuildArraySubscript(const NJsonPathGenerated::TRule_array_subscript& node);
    TAstNodePtr BuildArrayAccessor(const NJsonPathGenerated::TRule_array_accessor& node, TAstNodePtr input);
    TAstNodePtr BuildWildcardArrayAccessor(const NJsonPathGenerated::TRule_wildcard_array_accessor& node, TAstNodePtr input);

    TString BuildIdentifier(const NJsonPathGenerated::TRule_identifier& node);
    TAstNodePtr BuildMemberAccessor(const NJsonPathGenerated::TRule_member_accessor& node, TAstNodePtr input);
    TAstNodePtr BuildWildcardMemberAccessor(const NJsonPathGenerated::TRule_wildcard_member_accessor& node, TAstNodePtr input);

    TAstNodePtr BuildFilter(const NJsonPathGenerated::TRule_filter& node, TAstNodePtr input);

    TAstNodePtr BuildMethod(const NJsonPathGenerated::TRule_method& node, TAstNodePtr input);

    TAstNodePtr BuildAccessorOp(const NJsonPathGenerated::TRule_accessor_op& node, TAstNodePtr input);
    TAstNodePtr BuildAccessorExpr(const NJsonPathGenerated::TRule_accessor_expr& node);

    TAstNodePtr BuildPrimary(const NJsonPathGenerated::TRule_primary& node);

    TAstNodePtr BuildPlainExpr(const NJsonPathGenerated::TRule_plain_expr& node);
    TAstNodePtr BuildLikeRegexExpr(const NJsonPathGenerated::TRule_like_regex_expr& node, TAstNodePtr input);
    TAstNodePtr BuildPredicateExpr(const NJsonPathGenerated::TRule_predicate_expr& node);
    TAstNodePtr BuildUnaryExpr(const NJsonPathGenerated::TRule_unary_expr& node);
    TAstNodePtr BuildMulExpr(const NJsonPathGenerated::TRule_mul_expr& node);
    TAstNodePtr BuildAddExpr(const NJsonPathGenerated::TRule_add_expr& node);
    TAstNodePtr BuildCompareExpr(const NJsonPathGenerated::TRule_compare_expr& node);
    TAstNodePtr BuildEqualExpr(const NJsonPathGenerated::TRule_equal_expr& node);
    TAstNodePtr BuildAndExpr(const NJsonPathGenerated::TRule_and_expr& node);
    TAstNodePtr BuildOrExpr(const NJsonPathGenerated::TRule_or_expr& node);

    TAstNodePtr BuildExpr(const NJsonPathGenerated::TRule_expr& node);
    TAstNodePtr BuildJsonPath(const NJsonPathGenerated::TRule_jsonpath& node);

    void Error(TPosition pos, const TStringBuf message);

    TIssues& Issues;
};

}
