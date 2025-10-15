#include "sql_select_yql.h"

#include "sql_expression.h"
#include "select_yql.h"

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TYqlSelect final: public TSqlTranslation {
public:
    template <class T>
    using TResult = std::expected<T, EYqlSelectError>;

    TYqlSelect(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TResult<TNodePtr> Build(const TRule_select_stmt& rule) {
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

private:
    TResult<TNodePtr> Build(const TRule_select_kind_partial& rule) {
        if (rule.HasBlock2()) {
            return Unsupported("(LIMIT expr ((OFFSET | COMMA) expr)?)");
        }

        return Build(rule.GetRule_select_kind1());
    }

    TResult<TNodePtr> Build(const TRule_select_kind& rule) {
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
                return Build(block.GetAlt3().GetRule_select_core1());
            case NSQLv1Generated::TRule_select_kind_TBlock2::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    TResult<TNodePtr> Build(const TRule_select_core& rule) {
        TYqlSelectArgs select;

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

        if (rule.HasBlock3()) {
            Token(rule.GetBlock3().GetToken1());
            return Unsupported("STREAM");
        }

        if (rule.GetRule_opt_set_quantifier4().HasBlock1()) {
            return Unsupported("opt_set_quantifier");
        }

        if (auto result = BuildTerms(rule)) {
            select.Terms = std::move(*result);
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
                return std::unexpected(EYqlSelectError::Error);
            }

            if (auto result = Build(rule.GetBlock9().GetRule_join_source2())) {
                select.Source = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock10()) {
            Token(rule.GetBlock10().GetToken1());
            return Unsupported("WHERE expr");
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
            return Unsupported("ext_order_by_clause");
        }

        if (auto node = BuildYqlSelect(Ctx_.Pos(), std::move(select))) {
            return node;
        } else {
            return std::unexpected(EYqlSelectError::Error);
        }
    }

    TResult<TVector<TNodePtr>> BuildTerms(const TRule_select_core& rule) {
        TVector<TNodePtr> terms;

        if (auto result = Build(rule.GetRule_result_column5())) {
            terms.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& block : rule.GetBlock6()) {
            if (auto result = Build(block.GetRule_result_column2())) {
                terms.emplace_back(std::move(*result));
            } else {
                return std::unexpected(result.error());
            }
        }

        return terms;
    }

    TResult<TNodePtr> Build(const TRule_result_column& rule) {
        switch (rule.GetAltCase()) {
            case NSQLv1Generated::TRule_result_column::kAltResultColumn1:
                return Unsupported("opt_id_prefix ASTERISK");
            case NSQLv1Generated::TRule_result_column::kAltResultColumn2: {
                const auto& alt = rule.GetAlt_result_column2();

                TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
                TSqlExpression sqlExpr(Ctx_, Mode_);
                sqlExpr.ProduceYqlColumnRef();

                TNodePtr expr = sqlExpr.Build(alt.GetRule_expr1());
                if (!expr) {
                    return std::unexpected(EYqlSelectError::Error);
                }

                if (const auto label = Label(alt)) {
                    expr->SetLabel(*label);
                }

                return expr;
            } break;
            case NSQLv1Generated::TRule_result_column::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    TResult<TYqlSource> Build(const TRule_join_source& rule) {
        if (rule.HasBlock1()) {
            Token(rule.GetBlock1().GetToken1());
            return Unsupported<TYqlSource>("ANY");
        }

        if (!rule.GetBlock3().empty()) {
            return Unsupported<TYqlSource>("join_op ANY? flatten_source join_constraint?");
        }

        return Build(rule.GetRule_flatten_source2());
    }

    TResult<TYqlSource> Build(const TRule_flatten_source& rule) {
        if (rule.HasBlock2()) {
            Token(rule.GetBlock2().GetToken1());
            return Unsupported<TYqlSource>("FLATTEN ((OPTIONAL|LIST|DICT)? BY flatten_by_arg | COLUMNS)");
        }

        return Build(rule.GetRule_named_single_source1());
    }

    TResult<TYqlSource> Build(const TRule_named_single_source& rule) {
        TYqlSource source;

        if (auto result = Build(rule.GetRule_single_source1())) {
            source.Node = std::move(*result);
        } else {
            return std::unexpected(result.error());
        }

        if (rule.HasBlock2()) {
            return Unsupported<TYqlSource>("row_pattern_recognition_clause");
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
            return Unsupported<TYqlSource>("sample_clause | tablesample_clause");
        }

        return source;
    }

    TResult<TNodePtr> Build(const TRule_single_source& rule) {
        switch (rule.GetAltCase()) {
            case NSQLv1Generated::TRule_single_source::kAltSingleSource1:
                return Unsupported("table_ref");
            case NSQLv1Generated::TRule_single_source::kAltSingleSource2:
                return Unsupported("select_stmt");
            case NSQLv1Generated::TRule_single_source::kAltSingleSource3:
                return Build(rule.GetAlt_single_source3().GetRule_values_stmt2());
            case NSQLv1Generated::TRule_single_source::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    TResult<TNodePtr> Build(const TRule_values_stmt& rule) {
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

        return BuildYqlValues(Ctx_.Pos(), std::move(values));
    }

    TResult<TVector<TNodePtr>> Build(const TRule_values_source_row& rule) {
        TVector<TNodePtr> columns;

        TSqlExpression sqlExpr(Ctx_, Mode_);
        if (!ExprList(sqlExpr, columns, rule.GetRule_expr_list2())) {
            return std::unexpected(EYqlSelectError::Error);
        }

        return columns;
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
                Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    const TRule_select_kind_partial& Unpack(const TRule_select_kind_parenthesis& parenthesis) {
        switch (parenthesis.GetAltCase()) {
            case NSQLv1Generated::TRule_select_kind_parenthesis::kAltSelectKindParenthesis1:
                return parenthesis.GetAlt_select_kind_parenthesis1().GetRule_select_kind_partial1();
            case NSQLv1Generated::TRule_select_kind_parenthesis::kAltSelectKindParenthesis2:
                return parenthesis.GetAlt_select_kind_parenthesis2().GetRule_select_kind_partial2();
            case NSQLv1Generated::TRule_select_kind_parenthesis::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }
    }

    TString TableAlias(const TRule_named_single_source::TBlock3::TBlock1& block) {
        switch (block.GetAltCase()) {
            case TRule_named_single_source_TBlock3_TBlock1::kAlt1:
                return Id(block.GetAlt1().GetRule_an_id2(), *this);
            case TRule_named_single_source_TBlock3_TBlock1::kAlt2:
                return Id(block.GetAlt2().GetRule_an_id_as_compat1(), *this);
            case TRule_named_single_source_TBlock3_TBlock1::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
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

    template <class T = TNodePtr>
    TResult<T> Unsupported(TStringBuf message) {
        if (Ctx_.YqlSelectMode == EYqlSelectMode::Force) {
            Error() << "YqlSelect unsupported: " << message;
        }

        return std::unexpected(EYqlSelectError::Error);
    }
};

TYqlSelectResult BuildYqlSelect(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_select_stmt& rule)
{
    return TYqlSelect(ctx, mode).Build(rule);
}

} // namespace NSQLTranslationV1
