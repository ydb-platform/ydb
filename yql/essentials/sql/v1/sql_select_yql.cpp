#include "sql_select_yql.h"

#include "antlr_token.h"
#include "sql_expression.h"
#include "sql_group_by.h"
#include "select_yql.h"

#include <util/generic/overloaded.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TYqlSelect final: public TSqlTranslation {
public:
    TYqlSelect(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodeResult Build(const TRule_select_stmt& rule) {
        const auto& intersect = rule.GetRule_select_stmt_intersect1();
        if (!rule.GetBlock2().empty()) {
            return Unsupported("(union_op select_stmt_intersect)*");
        }

        const auto& partial = Unpack(intersect.GetRule_select_kind_parenthesis1());
        if (!intersect.GetBlock2().empty()) {
            return Unsupported("(intersect_op select_kind_parenthesis)*");
        }

        return Build(partial);
    }

    TNodeResult Build(const TRule_values_stmt& rule) {
        TYqlValuesArgs values;

        Token(rule.GetToken1());
        const auto& rows = rule.GetRule_values_source_row_list2();

        if (auto result = Build(rows.GetRule_values_source_row1())) {
            values.Rows.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& row : rows.GetBlock2()) {
            if (auto result = Build(row.GetRule_values_source_row2())) {
                values.Rows.emplace_back(std::move(*result));
            } else {
                return std::unexpected(result.error());
            }
        }

        return TNonNull(BuildYqlValues(Ctx_.Pos(), std::move(values)));
    }

    TNodeResult Build(const TRule_select_subexpr& rule, EColumnRefState state) {
        const auto& intersect = rule.GetRule_select_subexpr_intersect1();
        if (!rule.GetBlock2().empty()) {
            return Unsupported("(union_op select_subexpr_intersect)*");
        }

        const auto& select_or_expr = intersect.GetRule_select_or_expr1();
        if (!intersect.GetBlock2().empty()) {
            return Unsupported("(intersect_op select_or_expr)*");
        }

        return Build(select_or_expr, state);
    }

    TNodeResult Build(const TRule_exists_expr& rule) {
        return Build(rule.GetBlock3()).transform([](auto x) {
            return TNonNull(BuildYqlExistsSubquery(std::move(x)));
        });
    }

private:
    TNodeResult Build(const TRule_exists_expr::TBlock3& block) {
        switch (block.GetAltCase()) {
            case TRule_exists_expr_TBlock3::kAlt1:
                return Build(block.GetAlt1().GetRule_select_stmt1());
            case TRule_exists_expr_TBlock3::kAlt2:
                return Build(block.GetAlt2().GetRule_values_stmt1());
            case TRule_exists_expr_TBlock3::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TNodeResult Build(const TRule_select_or_expr& rule, EColumnRefState state) {
        switch (rule.GetAltCase()) {
            case TRule_select_or_expr::kAltSelectOrExpr1:
                return Build(rule.GetAlt_select_or_expr1().GetRule_select_kind_partial1());
            case TRule_select_or_expr::kAltSelectOrExpr2:
                return Build(rule.GetAlt_select_or_expr2().GetRule_tuple_or_expr1(), state);
            case TRule_select_or_expr::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TNodeResult Build(const TRule_select_kind_partial& rule) {
        TYqlSelectArgs select;

        if (rule.HasBlock2()) {
            const auto& block = rule.GetBlock2();

            Token(block.GetToken1());
            if (auto result = Build(block.GetRule_expr2(), EColumnRefState::Deny)) {
                select.Limit = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock2() && rule.GetBlock2().HasBlock3()) {
            const auto& block = rule.GetBlock2().GetBlock3();

            Token(block.GetToken1());
            if (auto result = Build(block.GetRule_expr2(), EColumnRefState::Deny)) {
                select.Offset = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        return Build(rule.GetRule_select_kind1(), std::move(select));
    }

    TNodeResult Build(const TRule_select_kind& rule, TYqlSelectArgs&& select) {
        if (rule.HasBlock1()) {
            return Unsupported("DISCARD");
        }

        if (rule.HasBlock3()) {
            return Unsupported("INTO RESULT pure_column_or_named");
        }

        switch (const auto& block = rule.GetBlock2(); block.GetAltCase()) {
            case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt1:
                return Unsupported("process_core");
            case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt2:
                return Unsupported("reduce_core");
            case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt3:
                return Build(block.GetAlt3().GetRule_select_core1(), std::move(select));
            case NSQLv1Generated::TRule_select_kind_TBlock2::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TNodeResult Build(const TRule_select_core& rule, TYqlSelectArgs&& select) {
        if (rule.HasBlock1()) {
            Token(rule.GetBlock1().GetToken1());

            if (auto result = Build(rule.GetBlock1().GetRule_join_source2())) {
                select.Source = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        Token(rule.GetToken2());

        if (Mode_ != NSQLTranslation::ESqlMode::QUERY) {
            return Unsupported("ESqlMode != QUERY");
        }

        if (!Ctx_.SimpleColumns) {
            return Unsupported("PRAGMA DisableSimpleColumns");
        }

        if (rule.HasBlock3()) {
            Token(rule.GetBlock3().GetToken1());
            return Unsupported("STREAM");
        }

        if (rule.GetRule_opt_set_quantifier4().HasBlock1()) {
            return Unsupported("opt_set_quantifier");
        }

        if (auto result = BuildProjection(rule)) {
            select.Projection = std::move(*result);
        } else {
            return std::unexpected(result.error());
        }

        if (rule.HasBlock8()) {
            Token(rule.GetBlock8().GetToken1());
            return Unsupported("WITHOUT (IF EXISTS)? without_column_list");
        }

        if (rule.HasBlock9()) {
            Token(rule.GetBlock9().GetToken1());

            if (rule.HasBlock1()) {
                Ctx_.IncrementMonCounter("sql_errors", "DoubleFrom");
                Ctx_.Error() << "Only one FROM clause is allowed";
                return std::unexpected(ESQLError::Basic);
            }

            if (auto result = Build(rule.GetBlock9().GetRule_join_source2())) {
                select.Source = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock10()) {
            Token(rule.GetBlock10().GetToken1());
            if (auto result = Build(rule.GetBlock10().GetRule_expr2(), EColumnRefState::Allow)) {
                select.Where = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock11()) {
            if (auto result = Build(rule.GetBlock11().GetRule_group_by_clause1())) {
                select.GroupBy = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock12()) {
            Token(rule.GetBlock12().GetToken1());
            if (auto result = Build(rule.GetBlock12().GetRule_expr2(), EColumnRefState::Allow)) {
                select.Having = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock13()) {
            return Unsupported("window_clause");
        }

        if (rule.HasBlock14()) {
            if (auto result = Build(rule.GetBlock14().GetRule_ext_order_by_clause1())) {
                select.OrderBy = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (select.Source &&
            1 < select.Source->Sources.size() &&
            std::holds_alternative<TPlainAsterisk>(select.Projection))
        {
            return Unsupported("JOIN with an asterisk projection");
        }

        if (auto node = BuildYqlSelect(Ctx_.Pos(), std::move(select))) {
            return TNonNull(node);
        } else {
            return std::unexpected(ESQLError::Basic);
        }
    }

    TSQLResult<TProjection> BuildProjection(const TRule_select_core& rule) {
        TVector<TNodePtr> terms;

        if (auto result = Build(rule.GetRule_result_column5())) {
            if (std::holds_alternative<TPlainAsterisk>(*result)) {
                if (!rule.GetBlock6().empty()) {
                    Error() << "Unable to use plain '*' with other projection items";
                    return std::unexpected(ESQLError::Basic);
                }

                return TPlainAsterisk();
            }

            terms.emplace_back(std::move(std::get<TNodePtr>(*result)));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& block : rule.GetBlock6()) {
            if (auto result = Build(block.GetRule_result_column2())) {
                if (std::holds_alternative<TPlainAsterisk>(*result)) {
                    Error() << "Unable to use plain '*' with other projection items";
                    return std::unexpected(ESQLError::Basic);
                }

                terms.emplace_back(std::move(std::get<TNodePtr>(*result)));
            } else {
                return std::unexpected(result.error());
            }
        }

        return terms;
    }

    TSQLResult<std::variant<TNodePtr, TPlainAsterisk>> Build(const TRule_result_column& rule) {
        switch (rule.GetAltCase()) {
            case NSQLv1Generated::TRule_result_column::kAltResultColumn1:
                return Build(rule.GetAlt_result_column1());
            case NSQLv1Generated::TRule_result_column::kAltResultColumn2:
                return Build(rule.GetAlt_result_column2());
            case NSQLv1Generated::TRule_result_column::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TSQLResult<std::variant<TNodePtr, TPlainAsterisk>> Build(const TRule_result_column::TAlt1& alt) {
        if (alt.GetRule_opt_id_prefix1().HasBlock1()) {
            return Unsupported("an_id DOT ASTERISK");
        }

        return TPlainAsterisk();
    }

    TNodeResult Build(const TRule_result_column::TAlt2& alt) {
        TNodeResult expr = Build(alt.GetRule_expr1(), EColumnRefState::Allow);
        if (!expr) {
            return std::unexpected(expr.error());
        }

        if (auto result = Label(alt)) {
            if (*result) {
                (*expr)->SetLabel(*(*result));
            }
        } else {
            return std::unexpected(result.error());
        }

        return expr;
    }

    TSQLResult<TYqlJoin> Build(const TRule_join_source& rule) {
        if (rule.HasBlock1()) {
            Token(rule.GetBlock1().GetToken1());
            return Unsupported("ANY");
        }

        TYqlJoin join = {
            .Sources = TVector<TYqlSource>(Reserve(1 + rule.GetBlock3().size())),
            .Constraints = TVector<TYqlJoinConstraint>(Reserve(rule.GetBlock3().size())),
        };

        if (auto result = Build(rule.GetRule_flatten_source2())) {
            join.Sources.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& block : rule.GetBlock3()) {
            TSQLResult<EYqlJoinKind> kind = Build(block.GetRule_join_op1());
            if (!kind) {
                return std::unexpected(kind.error());
            }

            if (block.HasBlock2()) {
                Token(block.GetBlock2().GetToken1());
                return Unsupported("ANY");
            }

            TSQLResult<TYqlSource> source = Build(block.GetRule_flatten_source3());
            if (!source) {
                return std::unexpected(source.error());
            }

            if (!block.HasBlock4() && kind != EYqlJoinKind::Cross) {
                Error() << "Expected ON or USING expression";
                return std::unexpected(ESQLError::Basic);
            }

            if (block.HasBlock4() && kind == EYqlJoinKind::Cross) {
                Error() << "Cross join should not have ON or USING expression";
                return std::unexpected(ESQLError::Basic);
            }

            auto constraint = [&]() -> TSQLResult<TYqlJoinConstraint> {
                if (!block.HasBlock4()) {
                    YQL_ENSURE(kind == EYqlJoinKind::Cross);
                    return TYqlJoinConstraint{
                        .Kind = EYqlJoinKind::Cross,
                        .Condition = TNodePtr(),
                    };
                }

                const auto& join_constraint = block.GetBlock4().GetRule_join_constraint1();
                return Build(join_constraint, *kind);
            }();

            if (!constraint) {
                return std::unexpected(constraint.error());
            }

            join.Sources.emplace_back(std::move(*source));
            join.Constraints.emplace_back(std::move(*constraint));
        }

        return join;
    }

    TSQLResult<EYqlJoinKind> Build(const TRule_join_op& rule) {
        switch (rule.GetAltCase()) {
            case TRule_join_op::kAltJoinOp1: {
                if (!Ctx_.AnsiImplicitCrossJoin) {
                    Token(rule.GetAlt_join_op1().GetToken1());
                    Error() << "Cartesian product of tables is disabled. "
                            << "Please use an explicit CROSS JOIN or "
                            << "enable it via PRAGMA AnsiImplicitCrossJoin";
                    return std::unexpected(ESQLError::Basic);
                }

                return EYqlJoinKind::Cross;
            }
            case TRule_join_op::kAltJoinOp2:
                return Build(rule.GetAlt_join_op2());
            case TRule_join_op::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TSQLResult<EYqlJoinKind> Build(const TRule_join_op::TAlt2& alt) {
        Token(alt.GetToken3());

        if (alt.HasBlock1()) {
            return Unsupported("NATURAL");
        }

        const auto& block = alt.GetBlock2();
        switch (block.GetAltCase()) {
            case TRule_join_op_TAlt2_TBlock2::kAlt1:
                break;
            case TRule_join_op_TAlt2_TBlock2::kAlt2:
                YQL_ENSURE(IS_TOKEN(block.GetAlt2().GetToken1().GetId(), INNER));
                return EYqlJoinKind::Inner;
            case TRule_join_op_TAlt2_TBlock2::kAlt3:
                YQL_ENSURE(IS_TOKEN(block.GetAlt3().GetToken1().GetId(), CROSS));
                return EYqlJoinKind::Cross;
            case TRule_join_op_TAlt2_TBlock2::ALT_NOT_SET:
                Y_UNREACHABLE();
        }

        const auto& alt1 = block.GetAlt1();
        if (alt1.HasBlock1()) {
            const auto& block = alt1.GetBlock1();
            switch (block.GetAltCase()) {
                case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt1: {
                    const auto& alt = block.GetAlt1();

                    if (alt.HasBlock2()) {
                        return Unsupported("(ONLY | SEMI)");
                    }

                    YQL_ENSURE(IS_TOKEN(block.GetAlt1().GetToken1().GetId(), LEFT));
                    return EYqlJoinKind::Left;
                }
                case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt2: {
                    const auto& alt = block.GetAlt2();

                    if (alt.HasBlock2()) {
                        return Unsupported("(ONLY | SEMI)");
                    }

                    YQL_ENSURE(IS_TOKEN(block.GetAlt2().GetToken1().GetId(), RIGHT));
                    return EYqlJoinKind::Right;
                }
                case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt3:
                    YQL_ENSURE(IS_TOKEN(block.GetAlt3().GetToken1().GetId(), EXCLUSION));
                    return Unsupported("EXCLUSION");
                case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt4:
                    YQL_ENSURE(IS_TOKEN(block.GetAlt4().GetToken1().GetId(), FULL));
                    return Unsupported("FULL");
                case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::ALT_NOT_SET:
                    Y_UNREACHABLE();
            }
        }

        if (alt1.HasBlock2()) {
            YQL_ENSURE(IS_TOKEN(alt1.GetBlock2().GetToken1().GetId(), OUTER));
            return Unsupported("OUTER");
        }

        return EYqlJoinKind::Inner;
    }

    TSQLResult<TYqlJoinConstraint> Build(const TRule_join_constraint& rule, EYqlJoinKind kind) {
        switch (rule.GetAltCase()) {
            case TRule_join_constraint::kAltJoinConstraint1:
                return Build(rule.GetAlt_join_constraint1(), kind);
            case TRule_join_constraint::kAltJoinConstraint2:
                return Unsupported("USING pure_column_or_named_list");
            case TRule_join_constraint::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TSQLResult<TYqlJoinConstraint> Build(const TRule_join_constraint::TAlt1& alt, EYqlJoinKind kind) {
        Token(alt.GetToken1());
        if (auto result = Build(alt.GetRule_expr2(), EColumnRefState::Allow)) {
            return TYqlJoinConstraint{
                .Kind = kind,
                .Condition = std::move(*result),
            };
        } else {
            return std::unexpected(result.error());
        }
    }

    TSQLResult<TYqlSource> Build(const TRule_flatten_source& rule) {
        if (rule.HasBlock2()) {
            Token(rule.GetBlock2().GetToken1());
            return Unsupported("FLATTEN ((OPTIONAL|LIST|DICT)? BY flatten_by_arg | COLUMNS)");
        }

        return Build(rule.GetRule_named_single_source1());
    }

    TSQLResult<TYqlSource> Build(const TRule_named_single_source& rule) {
        TYqlSource source;

        if (auto result = Build(rule.GetRule_single_source1())) {
            source.Node = std::move(*result);
        } else {
            return std::unexpected(result.error());
        }

        if (rule.HasBlock2()) {
            return Unsupported("row_pattern_recognition_clause");
        }

        if (rule.HasBlock3()) {
            const auto& block = rule.GetBlock3();

            source.Alias.ConstructInPlace();

            if (auto name = TableAlias(block.GetBlock1())) {
                source.Alias->Name = std::move(*name);
            } else {
                return std::unexpected(ESQLError::Basic);
            }

            if (block.HasBlock2()) {
                source.Alias->Columns = TableColumns(block.GetBlock2().GetRule_pure_column_list1());
            }
        }

        if (rule.HasBlock4()) {
            return Unsupported("sample_clause | tablesample_clause");
        }

        return source;
    }

    TNodeResult Build(const TRule_single_source& rule) {
        switch (rule.GetAltCase()) {
            case NSQLv1Generated::TRule_single_source::kAltSingleSource1:
                return Build(rule.GetAlt_single_source1().GetRule_table_ref1());
            case NSQLv1Generated::TRule_single_source::kAltSingleSource2:
                return Build(rule.GetAlt_single_source2().GetRule_select_stmt2());
            case NSQLv1Generated::TRule_single_source::kAltSingleSource3:
                return Build(rule.GetAlt_single_source3().GetRule_values_stmt2());
            case NSQLv1Generated::TRule_single_source::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TNodeResult Build(const TRule_table_ref& rule) {
        TString service = Ctx_.Scoped->CurrService;
        TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;

        if (rule.HasBlock1()) {
            const auto& expr = rule.GetBlock1().GetRule_cluster_expr1();
            if (!ClusterExpr(expr, /* allowWildcard = */ false, service, cluster)) {
                return std::unexpected(ESQLError::Basic);
            }
        }

        if (rule.HasBlock2()) {
            Token(rule.GetBlock2().GetToken1());
            return Unsupported("COMMAT");
        }

        if (rule.HasBlock4()) {
            return Unsupported("table_hints");
        }

        return Build(rule.GetBlock3(), std::move(service), std::move(cluster));
    }

    TNodeResult Build(const TRule_table_ref::TBlock3& block, TString service, TDeferredAtom cluster) {
        switch (block.GetAltCase()) {
            case TRule_table_ref_TBlock3::kAlt1:
                return Build(block.GetAlt1().GetRule_table_key1(), std::move(service), std::move(cluster));
            case TRule_table_ref_TBlock3::kAlt2:
                return Unsupported("an_id_expr LPAREN (table_arg (COMMA table_arg)* COMMA?)? RPAREN");
            case TRule_table_ref_TBlock3::kAlt3:
                return Unsupported("bind_parameter (LPAREN expr_list? RPAREN)? (VIEW view_name)?");
            case TRule_table_ref_TBlock3::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TNodeResult Build(const TRule_table_key& rule, TString service, TDeferredAtom cluster) {
        if (cluster.Empty()) {
            Ctx_.Error() << "No cluster name given and no default cluster is selected";
            return std::unexpected(ESQLError::Basic);
        }

        if (cluster.GetLiteral() == nullptr) {
            return Unsupported("not literal cluster");
        }

        if (rule.HasBlock2()) {
            return Unsupported("(VIEW view_name)");
        }

        TString key = Id(rule.GetRule_id_table_or_type1(), *this);

        TYqlTableRefArgs args = {
            .Service = std::move(service),
            .Cluster = *cluster.GetLiteral(),
            .Key = std::move(key),
        };

        return TNonNull(BuildYqlTableRef(Ctx_.Pos(), std::move(args)));
    }

    TSQLResult<TVector<TNodePtr>> Build(const TRule_values_source_row& rule) {
        TVector<TNodePtr> columns;

        TSqlExpression sqlExpr(Ctx_, Mode_);
        if (!Unwrap(ExprList(sqlExpr, columns, rule.GetRule_expr_list2()))) {
            return std::unexpected(ESQLError::Basic);
        }

        return columns;
    }

    TSQLResult<TGroupBy> Build(const TRule_group_by_clause& rule) {
        TGroupByClause legacy(Ctx_, Mode_);
        legacy.SetYqlSelectProduced(true);
        if (!legacy.Build(rule)) {
            return std::unexpected(ESQLError::Basic);
        }

        if (!legacy.Aliases().empty()) {
            return Unsupported("GROUP BY aliases");
        }
        if (legacy.GetLegacyHoppingWindow() != nullptr) {
            return Unsupported("GROUP BY HOP");
        }
        if (legacy.IsCompactGroupBy()) {
            return Unsupported("GROUP COMPACT BY");
        }
        if (!legacy.GetSuffix().empty()) {
            return Unsupported("GROUP BY ... WITH an_id");
        }

        return TGroupBy{
            .Keys = std::move(legacy.Content()),
        };
    }

    TSQLResult<TOrderBy> Build(const TRule_ext_order_by_clause& rule) {
        TOrderBy orderBy;

        if (rule.HasBlock1()) {
            Token(rule.GetBlock1().GetToken1());
            return Unsupported("ASSUME ORDER BY");
        }

        const auto& clause = rule.GetRule_order_by_clause2();
        const auto& sort = clause.GetRule_sort_specification_list3();

        Token(clause.GetToken1());

        if (auto result = Build(sort.GetRule_sort_specification1())) {
            orderBy.Keys.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& block : sort.GetBlock2()) {
            if (auto result = Build(block.GetRule_sort_specification2())) {
                orderBy.Keys.emplace_back(std::move(*result));
            } else {
                return std::unexpected(result.error());
            }
        }

        return orderBy;
    }

    TSQLResult<TSortSpecificationPtr> Build(const TRule_sort_specification& rule) {
        TNodeResult expr = Build(rule.GetRule_expr1(), EColumnRefState::Allow);
        if (!expr) {
            return std::unexpected(expr.error());
        }

        TString direction;
        if (rule.HasBlock2()) {
            direction = ToLowerUTF8(Token(rule.GetBlock2().GetToken1()));
        }

        bool isAscending;
        if (direction == "") {
            isAscending = true;
        } else if (direction == "asc") {
            isAscending = true;
        } else if (direction == "desc") {
            isAscending = false;
        } else {
            YQL_ENSURE(false, "Unexpected ORDER BY direction " << direction);
        }

        return new TSortSpecification(std::move(*expr), isAscending);
    }

    template <class TRule>
        requires std::same_as<TRule, TRule_expr> ||
                 std::same_as<TRule, TRule_tuple_or_expr>
    TNodeResult Build(const TRule& rule, EColumnRefState state) {
        TColumnRefScope scope(Ctx_, state);
        TSqlExpression sqlExpr(Ctx_, Mode_);
        sqlExpr.SetYqlSelectProduced(true);

        return sqlExpr.Build(rule);
    }

    TSQLResult<TMaybe<TString>> Label(const TRule_result_column::TAlt2& rule) {
        if (!rule.HasBlock2()) {
            return Nothing();
        }

        const auto& block = rule.GetBlock2();
        switch (block.Alt_case()) {
            case TRule_result_column_TAlt2_TBlock2::kAlt1:
                return Id(block.GetAlt1().GetRule_an_id_or_type2(), *this);
            case TRule_result_column_TAlt2_TBlock2::kAlt2: {
                if (!Ctx_.AnsiOptionalAs) {
                    Error() << "Expecting mandatory AS here. "
                            << "Did you miss comma? "
                            << "Please add PRAGMA AnsiOptionalAs; "
                            << "for ANSI compatibility";
                    return std::unexpected(ESQLError::Basic);
                }

                return Id(block.GetAlt2().GetRule_an_id_as_compat1(), *this);
            }
            case TRule_result_column_TAlt2_TBlock2::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    const TRule_select_kind_partial& Unpack(const TRule_select_kind_parenthesis& parenthesis) {
        switch (parenthesis.GetAltCase()) {
            case NSQLv1Generated::TRule_select_kind_parenthesis::kAltSelectKindParenthesis1:
                return parenthesis.GetAlt_select_kind_parenthesis1().GetRule_select_kind_partial1();
            case NSQLv1Generated::TRule_select_kind_parenthesis::kAltSelectKindParenthesis2:
                return parenthesis.GetAlt_select_kind_parenthesis2().GetRule_select_kind_partial2();
            case NSQLv1Generated::TRule_select_kind_parenthesis::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TMaybe<TString> TableAlias(const TRule_named_single_source::TBlock3::TBlock1& block) {
        switch (block.GetAltCase()) {
            case TRule_named_single_source_TBlock3_TBlock1::kAlt1:
                return Id(block.GetAlt1().GetRule_an_id2(), *this);
            case TRule_named_single_source_TBlock3_TBlock1::kAlt2: {
                if (!Ctx_.AnsiOptionalAs) {
                    Error() << "Expecting mandatory AS here. "
                            << "Did you miss comma? "
                            << "Please add PRAGMA AnsiOptionalAs; "
                            << "for ANSI compatibility";
                    return Nothing();
                }

                return Id(block.GetAlt2().GetRule_an_id_as_compat1(), *this);
            }
            case TRule_named_single_source_TBlock3_TBlock1::ALT_NOT_SET:
                Y_UNREACHABLE();
        }
    }

    TVector<TString> TableColumns(const TRule_pure_column_list& rule) {
        TVector<TString> columns;
        columns.emplace_back(Id(rule.GetRule_an_id2(), *this));
        for (const auto& id : rule.GetBlock3()) {
            columns.emplace_back(Id(id.GetRule_an_id2(), *this));
        }
        return columns;
    }

    std::unexpected<ESQLError> Unsupported(TStringBuf message) {
        return UnsupportedYqlSelect(Ctx_, message);
    }
};

NYql::TLangVersion YqlSelectLangVersion() {
    return NYql::GetMaxLangVersion();
}

std::unexpected<ESQLError> YqlSelectUnsupported(TContext& ctx, TStringBuf message) {
    if (ctx.GetYqlSelectMode() == EYqlSelectMode::Force) {
        ctx.Error() << "YqlSelect unsupported: " << message;
    }

    return std::unexpected(ESQLError::UnsupportedYqlSelect);
}

TNodeResult BuildYqlSelect(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_select_stmt& rule)
{
    return TYqlSelect(ctx, mode)
        .Build(rule)
        .transform([](auto x) {
            return TNonNull(BuildYqlStatement(std::move(x)));
        });
}

TNodeResult BuildYqlSelectSubExpr(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_select_subexpr& rule,
    EColumnRefState state)
{
    return TYqlSelect(ctx, mode)
        .Build(rule, state)
        .transform([](auto x) {
            return TNonNull(WrapYqlSelectSubExpr(std::move(x)));
        });
}

TNodeResult BuildYqlExists(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_exists_expr& rule)
{
    return TYqlSelect(ctx, mode).Build(rule);
}

TNodeResult BuildYqlSelect(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_values_stmt& rule)
{
    return TYqlSelect(ctx, mode)
        .Build(rule)
        .transform([](auto x) {
            return TNonNull(BuildYqlStatement(std::move(x)));
        });
}

} // namespace NSQLTranslationV1
