#include "parse_tree.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/vector.h>

namespace NSQLTranslationV1 {

TVector<const TRule_sql_stmt_core*> Statements(const TRule_sql_stmt_list& rule) {
    if (!rule.HasBlock2()) {
        return {};
    }

    const auto& block = rule.GetBlock2();

    TVector<const TRule_sql_stmt_core*> statements(Reserve(1 + block.GetBlock2().size()));
    statements.emplace_back(&block.GetRule_sql_stmt1().GetRule_sql_stmt_core2());
    for (const auto& block : block.GetBlock2()) {
        statements.emplace_back(&block.GetRule_sql_stmt2().GetRule_sql_stmt_core2());
    }
    return statements;
}

TVector<const TRule_sql_stmt_core*> Statements(const TRule_sql_query& rule) {
    switch (rule.GetAltCase()) {
        case NSQLv1Generated::TRule_sql_query::kAltSqlQuery1:
            return Statements(rule.GetAlt_sql_query1().GetRule_sql_stmt_list1());
        case NSQLv1Generated::TRule_sql_query::kAltSqlQuery2:
            return {};
        case NSQLv1Generated::TRule_sql_query::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

bool IsEmptyQuery(google::protobuf::Message* message) {
    YQL_ENSURE(message);

    auto ast = static_cast<const TSQLv1ParserAST&>(*message);
    const auto& sqlQuery = ast.GetRule_sql_query();

    if (sqlQuery.GetAltCase() == NSQLv1Generated::TRule_sql_query::kAltSqlQuery1 &&
        !sqlQuery.GetAlt_sql_query1().GetRule_sql_stmt_list1().HasBlock2())
    {
        return true;
    }

    return false;
}

const TRule_select_or_expr* GetSelectOrExpr(const TRule_smart_parenthesis& msg) {
    if (!msg.GetBlock2().HasAlt1()) {
        return nullptr;
    }

    return &msg.GetBlock2()
                .GetAlt1()
                .GetRule_select_subexpr1()
                .GetRule_select_subexpr_intersect1()
                .GetRule_select_or_expr1();
}

const TRule_tuple_or_expr* GetTupleOrExpr(const TRule_smart_parenthesis& msg) {
    const auto* select_or_expr = GetSelectOrExpr(msg);
    if (!select_or_expr) {
        return nullptr;
    }

    if (!select_or_expr->HasAlt_select_or_expr2()) {
        return nullptr;
    }

    return &select_or_expr
                ->GetAlt_select_or_expr2()
                .GetRule_tuple_or_expr1();
}

const TRule_smart_parenthesis* GetParenthesis(const TRule_expr& msg) {
    if (!msg.HasAlt_expr1()) {
        return nullptr;
    }

    const auto& con = msg.GetAlt_expr1()
                          .GetRule_or_subexpr1()
                          .GetRule_and_subexpr1()
                          .GetRule_xor_subexpr1()
                          .GetRule_eq_subexpr1()
                          .GetRule_neq_subexpr1()
                          .GetRule_bit_subexpr1()
                          .GetRule_add_subexpr1()
                          .GetRule_mul_subexpr1()
                          .GetRule_con_subexpr1();

    if (!con.HasAlt_con_subexpr1()) {
        return nullptr;
    }

    const auto& unary_subexpr = con.GetAlt_con_subexpr1()
                                    .GetRule_unary_subexpr1();

    if (!unary_subexpr.HasAlt_unary_subexpr1()) {
        return nullptr;
    }

    const auto& block = unary_subexpr.GetAlt_unary_subexpr1()
                            .GetRule_unary_casual_subexpr1()
                            .GetBlock1();

    if (!block.HasAlt2()) {
        return nullptr;
    }

    const auto& atom = block.GetAlt2()
                           .GetRule_atom_expr1();

    if (!atom.HasAlt_atom_expr3()) {
        return nullptr;
    }

    return &atom.GetAlt_atom_expr3()
                .GetRule_lambda1()
                .GetRule_smart_parenthesis1();
}

bool IsSelect(const TRule_smart_parenthesis& msg) {
    const auto* select_or_expr = GetSelectOrExpr(msg);
    if (!select_or_expr) {
        return false;
    }

    if (select_or_expr->HasAlt_select_or_expr1()) {
        return true;
    }

    return IsSelect(
        select_or_expr
            ->GetAlt_select_or_expr2()
            .GetRule_tuple_or_expr1()
            .GetRule_expr1());
}

bool IsSelect(const TRule_expr& msg) {
    const auto* parenthesis = GetParenthesis(msg);
    if (!parenthesis) {
        return false;
    }

    return IsSelect(*parenthesis);
}

bool IsOnlySubExpr(const TRule_select_subexpr& msg) {
    return msg.GetBlock2().empty() &&
           msg.GetRule_select_subexpr_intersect1().GetBlock2().empty();
}

bool IsOnlySelect(const TRule_select_stmt& rule) {
    return rule.GetBlock2().empty() &&
           rule.GetRule_select_stmt_intersect1().GetBlock2().empty();
}

const TRule_select_kind_partial& Unpack(const TRule_select_kind_parenthesis& rule) {
    switch (rule.GetAltCase()) {
        case NSQLv1Generated::TRule_select_kind_parenthesis::kAltSelectKindParenthesis1:
            return rule.GetAlt_select_kind_parenthesis1().GetRule_select_kind_partial1();
        case NSQLv1Generated::TRule_select_kind_parenthesis::kAltSelectKindParenthesis2:
            return rule.GetAlt_select_kind_parenthesis2().GetRule_select_kind_partial2();
        case NSQLv1Generated::TRule_select_kind_parenthesis::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
}

} // namespace NSQLTranslationV1
