#pragma once

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

const TRule_tuple_or_expr* GetTupleOrExpr(const TRule_smart_parenthesis& msg);

bool IsSelect(const TRule_smart_parenthesis& msg);

bool IsSelect(const TRule_expr& msg);

bool IsOnlySubExpr(const TRule_select_subexpr& msg);

} // namespace NSQLTranslationV1
