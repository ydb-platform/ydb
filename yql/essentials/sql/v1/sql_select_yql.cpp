#include "sql_select_yql.h"

#include "sql_expression.h"
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

private:
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
            return Unsupported("group_by_clause");
        }

        if (rule.HasBlock12()) {
            Token(rule.GetBlock12().GetToken1());
            return Unsupported("HAVING expr");
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
            return std::unexpected(ESQLError::Basic);
        }

        if (const auto label = Label(alt)) {
            (*expr)->SetLabel(*label);
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

            if (!block.HasBlock4()) {
                return Unsupported("absent join_constraint");
            }

            const auto& join_constraint = block.GetBlock4().GetRule_join_constraint1();
            TSQLResult<TYqlJoinConstraint> constraint = Build(join_constraint, *kind);
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
            case TRule_join_op::kAltJoinOp1:
                return Unsupported("COMMA");
            case TRule_join_op::kAltJoinOp2:
                break;
            case TRule_join_op::ALT_NOT_SET:
                Y_UNREACHABLE();
        };

        const auto& alt = rule.GetAlt_join_op2();

        Token(alt.GetToken3());

        if (alt.HasBlock1()) {
            return Unsupported("NATURAL");
        }

        const auto& block = alt.GetBlock2();
        if (!block.HasAlt1()) {
            return Unsupported("INNER | CROSS");
        }

        const auto& alt1 = block.GetAlt1();
        if (alt1.HasBlock1()) {
            return Unsupported("(LEFT | RIGHT | EXCLUSION | FULL)");
        }
        if (alt1.HasBlock2()) {
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
            source.Alias->Name = TableAlias(block.GetBlock1());
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

    TNodeResult Build(const TRule_expr& rule, EColumnRefState state) {
        TColumnRefScope scope(Ctx_, state);
        TSqlExpression sqlExpr(Ctx_, Mode_);
        sqlExpr.ProduceYqlColumnRef();

        TNodeResult expr = sqlExpr.BuildSourceOrNode(rule);
        if (!expr) {
            return std::unexpected(expr.error());
        }

        if (TSourcePtr source = MoveOutIfSource(*expr)) {
            return Unsupported("select_subexpr");
        }

        return expr;
    }

    TMaybe<TString> Label(const TRule_result_column::TAlt2& rule) {
        if (!rule.HasBlock2()) {
            return Nothing();
        }

        const auto& block = rule.GetBlock2();
        switch (block.Alt_case()) {
            case TRule_result_column_TAlt2_TBlock2::kAlt1:
                return Id(block.GetAlt1().GetRule_an_id_or_type2(), *this);
            case TRule_result_column_TAlt2_TBlock2::kAlt2:
                return Id(block.GetAlt2().GetRule_an_id_as_compat1(), *this);
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

    TString TableAlias(const TRule_named_single_source::TBlock3::TBlock1& block) {
        switch (block.GetAltCase()) {
            case TRule_named_single_source_TBlock3_TBlock1::kAlt1:
                return Id(block.GetAlt1().GetRule_an_id2(), *this);
            case TRule_named_single_source_TBlock3_TBlock1::kAlt2:
                return Id(block.GetAlt2().GetRule_an_id_as_compat1(), *this);
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
        if (Ctx_.GetYqlSelectMode() == EYqlSelectMode::Force) {
            Error() << "YqlSelect unsupported: " << message;
        }

        return std::unexpected(ESQLError::Basic);
    }
};

TNodeResult BuildYqlSelect(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_select_stmt& rule)
{
    if (auto result = TYqlSelect(ctx, mode).Build(rule)) {
        return TNonNull(BuildYqlStatement(std::move(*result)));
    } else {
        return std::unexpected(result.error());
    }
}

TNodeResult BuildYqlSelect(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_values_stmt& rule)
{
    if (auto result = TYqlSelect(ctx, mode).Build(rule)) {
        return TNonNull(BuildYqlStatement(std::move(*result)));
    } else {
        return std::unexpected(result.error());
    }
}

} // namespace NSQLTranslationV1
