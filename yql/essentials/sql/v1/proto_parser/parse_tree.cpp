#include "parse_tree.h"

namespace NSQLTranslationV1 {

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

bool IsOnlySubExpr(const TRule_select_subexpr& node) {
    return node.GetBlock2().size() == 0 &&
           node.GetRule_select_subexpr_intersect1().GetBlock2().size() == 0;
}

} // namespace NSQLTranslationV1
