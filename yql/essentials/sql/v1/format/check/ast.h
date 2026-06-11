#pragma once

#include <yql/essentials/ast/yql_ast.h>

namespace NSQLFormat {

// true - equal, false - not equal, nullopt - unknown
TMaybe<bool> AreAstEqual(const NYql::TAstNode* lhs, const NYql::TAstNode* rhs);

} // namespace NSQLFormat
