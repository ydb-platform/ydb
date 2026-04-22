#pragma once

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

TVector<const TRule_sql_stmt_core*> Statements(const TRule_sql_query& rule Y_LIFETIME_BOUND);

bool IsEmptyQuery(google::protobuf::Message* message);

const TRule_tuple_or_expr* GetTupleOrExpr(const TRule_smart_parenthesis& msg Y_LIFETIME_BOUND);

bool IsSelect(const TRule_smart_parenthesis& msg);

bool IsSelect(const TRule_expr& msg);

bool IsOnlySubExpr(const TRule_select_subexpr& msg);

bool IsOnlySelect(const TRule_select_stmt& rule);

const TRule_select_kind_partial& Unpack(const TRule_select_kind_parenthesis& rule Y_LIFETIME_BOUND);

} // namespace NSQLTranslationV1
