#include "sql_select_yql.h"

#include "antlr_token.h"
#include "select_yql.h"
#include "sql_expression.h"
#include "sql_select_window.h"
#include "sql_select.h"

#include <yql/essentials/sql/v1/proto_parser/parse_tree.h>

#include <util/generic/overloaded.h>
#include <util/generic/scope.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

struct TRawCTE {
    TYqlSourceAlias Alias;
    const TRule_cte_value* ParseTree = nullptr;
    bool IsRecursive = false;
};

class TYqlSelect final: public TSqlTranslation {
public:
    explicit TYqlSelect(const TSqlTranslation& that)
        : TSqlTranslation(that)
    {
        SetYqlSelectProduced(true);
    }

    TYqlSelect(const TYqlSelect& that) = default;

    TNodeResult Build(const TRule_select_stmt& rule) {
        return WithForkedNamespace([&](TYqlSelect& that) {
            return that.BuildForked(rule);
        });
    }

    TNodeResult Build(const TRule_select_unparenthesized_stmt& rule) {
        return WithForkedNamespace([&](TYqlSelect& that) {
            return that.BuildForked(rule);
        });
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

    TNodeResult Build(
        const TRule_select_subexpr& rule,
        EColumnRefState state,
        ESmartParenthesis smartParenthesis)
    {
        return WithForkedNamespace([&](TYqlSelect& that) {
            return that.BuildForked(rule, state, smartParenthesis);
        });
    }

    TNodeResult Build(const TRule_exists_expr& rule) {
        TYqlSelect that(*this);
        that.CurrentCTE_ = Nothing();
        return that.Build(rule.GetBlock3()).transform([](auto x) {
            return TNonNull(BuildYqlExistsSubquery(std::move(x)));
        });
    }

private:
    TSQLStatus Build(const TRule_cte_with_clause& rule) {
        auto bindings = RawCTEs(rule);
        if (!bindings) {
            return std::unexpected(bindings.error());
        }

        for (auto& binding : *bindings) {
            auto result = Build(binding);
            if (!result) {
                return std::unexpected(result.error());
            }
        }

        return std::monostate();
    }

    TSQLResult<TVector<TRawCTE>> RawCTEs(const TRule_cte_with_clause& rule) {
        TVector<TRawCTE> bindings(Reserve(rule.GetBlock3().size() + 1));

        if (auto result = Build(rule.GetRule_cte_binding2())) {
            bindings.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& block : rule.GetBlock3()) {
            if (auto result = Build(block.GetRule_cte_binding2())) {
                bindings.emplace_back(std::move(*result));
            } else {
                return std::unexpected(result.error());
            }
        }

        return bindings;
    }

    TSQLResult<TRawCTE> Build(const TRule_cte_binding& rule) {
        bool isRecursive = rule.HasBlock1();

        auto alias = Build(rule.GetRule_cte_key2());
        if (!alias) {
            return std::unexpected(alias.error());
        }

        const auto& tree = rule.GetRule_cte_value5();
        return TRawCTE{
            .Alias = std::move(*alias),
            .ParseTree = &tree,
            .IsRecursive = isRecursive,
        };
    }

    TSQLResult<TYqlSourceAlias> Build(const TRule_cte_key& rule) {
        TString id = Id(rule.GetRule_id_table_or_type1(), *this);
        if (id.empty()) {
            return std::unexpected(ESQLError::Basic);
        }

        TVector<TYqlColumnRef> columns;
        if (rule.HasBlock2()) {
            columns = TableColumns(rule.GetBlock2().GetRule_pure_column_list1());
        }

        return TYqlSourceAlias{
            .Position = Ctx_.Pos(),
            .Name = std::move(id),
            .Columns = std::move(columns),
            .Kind = TYqlSourceAlias::EKind::CTE,
        };
    }

    TSQLStatus Build(const TRawCTE& binding) {
        TString name = binding.Alias.Name;
        TPosition position = binding.Alias.Position;

        const auto define = [&](TReadyCTE cte) -> bool {
            auto old = CTEs_->Exchange(name, std::move(cte));
            if (old && !old->IsRecursiveNotReady()) {
                Ctx_.Error(position)
                    << "Bad CTE: "
                    << "Redefinition is forbidden: "
                    << name;
                return false;
            }

            return true;
        };

        if (binding.IsRecursive) {
            TReadyCTE cte = {
                .Alias = binding.Alias,
                .Node = nullptr,
            };

            if (!define(std::move(cte))) {
                return std::unexpected(ESQLError::Basic);
            }
        }

        TNodeResult result = [&] {
            TYqlSelect that(*this);
            that.CurrentCTE_ = name;
            return that.Build(*binding.ParseTree);
        }();
        if (!result) {
            return std::unexpected(result.error());
        }

        TReadyCTE cte = {
            .Alias = binding.Alias,
            .Node = std::move(*result),
        };

        if (!define(std::move(cte))) {
            return std::unexpected(ESQLError::Basic);
        }

        return std::monostate();
    }

    TNodeResult Build(const TRule_cte_value& rule) {
        switch (rule.GetAltCase()) {
            case TRule_cte_value::kAltCteValue1:
                return Build(rule.GetAlt_cte_value1().GetRule_select_stmt1());
            case TRule_cte_value::kAltCteValue2:
                return Build(rule.GetAlt_cte_value2().GetRule_values_stmt1());
            case TRule_cte_value::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TNodeResult BuildForked(const TRule_select_stmt& rule) {
        if (auto status = BuildCTE(rule); !status) {
            return std::unexpected(status.error());
        }

        const auto& core = rule.GetRule_select_stmt_core2();
        return Finalize(BuildUnion(core, TYqlSelectArgs()));
    }

    TNodeResult BuildForked(const TRule_select_unparenthesized_stmt& rule) {
        if (auto status = BuildCTE(rule); !status) {
            return std::unexpected(status.error());
        }

        const auto& core = rule.GetRule_select_unparenthesized_stmt_core2();
        return Finalize(BuildUnion(core, TYqlSelectArgs()));
    }

    TNodeResult BuildForked(
        const TRule_select_subexpr& rule,
        EColumnRefState state,
        ESmartParenthesis smartParenthesis)
    {
        if (auto status = BuildCTE(rule); !status) {
            return std::unexpected(status.error());
        }

        const auto& core = rule.GetRule_select_subexpr_core2();

        const bool isSelect = IsSelect(
            core
                .GetRule_select_subexpr_intersect1()
                .GetRule_select_or_expr1());

        if (rule.HasBlock1() && !isSelect) {
            Token(rule.GetBlock1().GetRule_cte_with_clause1().GetToken1());
            Ctx_.Error() << "A WITH clause can only be used before a SELECT statement, "
                            "but the expression that follows it is not a SELECT";
            return std::unexpected(ESQLError::Basic);
        }

        if (!IsOnlySubExpr(rule)) {
            return Finalize(BuildUnion(core, TYqlSelectArgs()));
        }

        const auto& intersect = core.GetRule_select_subexpr_intersect1();
        YQL_ENSURE(core.GetBlock2().empty(), "Unexpected (union_op select_subexpr_intersect)*");

        const auto& select_or_expr = intersect.GetRule_select_or_expr1();
        YQL_ENSURE(intersect.GetBlock2().empty(), "Unexpected (intersect_op select_or_expr)*");

        return Build(select_or_expr, state, smartParenthesis);
    }

    const auto& GetFirstArgument(const auto& rule) {
        using T = std::decay_t<decltype(rule)>;

        if constexpr (std::is_same_v<T, TRule_select_stmt_core>) {
            return rule.GetRule_select_stmt_intersect1();
        } else if constexpr (std::is_same_v<T, TRule_select_stmt_intersect>) {
            return rule.GetRule_select_kind_parenthesis1();
        } else if constexpr (std::is_same_v<T, TRule_select_unparenthesized_stmt_core>) {
            return rule.GetRule_select_unparenthesized_stmt_intersect1();
        } else if constexpr (std::is_same_v<T, TRule_select_unparenthesized_stmt_intersect>) {
            return rule.GetRule_select_kind_partial1();
        } else if constexpr (std::is_same_v<T, TRule_select_subexpr_core>) {
            return rule.GetRule_select_subexpr_intersect1();
        } else if constexpr (std::is_same_v<T, TRule_select_subexpr_intersect>) {
            return rule.GetRule_select_or_expr1();
        } else {
            static_assert(false);
        }
    }

    const auto& GetNextArgument(const auto& block) {
        using T = std::decay_t<decltype(block)>;

        if constexpr (std::is_same_v<T, TRule_select_stmt_core::TBlock2>) {
            return block.GetRule_select_stmt_intersect2();
        } else if constexpr (std::is_same_v<T, TRule_select_stmt_intersect::TBlock2>) {
            return block.GetRule_select_kind_parenthesis2();
        } else if constexpr (std::is_same_v<T, TRule_select_unparenthesized_stmt_core::TBlock2>) {
            return block.GetRule_select_stmt_intersect2();
        } else if constexpr (std::is_same_v<T, TRule_select_unparenthesized_stmt_intersect::TBlock2>) {
            return block.GetRule_select_kind_parenthesis2();
        } else if constexpr (std::is_same_v<T, TRule_select_subexpr_core::TBlock2>) {
            return block.GetRule_select_subexpr_intersect2();
        } else if constexpr (std::is_same_v<T, TRule_select_subexpr_intersect::TBlock2>) {
            return block.GetRule_select_or_expr2();
        } else {
            static_assert(false);
        }
    }

    template <class TRule>
        requires std::is_same_v<TRule, TRule_select_stmt> ||
                 std::is_same_v<TRule, TRule_select_unparenthesized_stmt> ||
                 std::is_same_v<TRule, TRule_select_subexpr>
    TSQLStatus BuildCTE(const TRule& rule) {
        if (rule.HasBlock1()) {
            auto status = Build(rule.GetBlock1().GetRule_cte_with_clause1());
            if (!status) {
                return std::unexpected(status.error());
            }
        }

        return std::monostate();
    }

    template <class TRule>
        requires std::is_same_v<TRule, TRule_select_stmt_core> ||
                 std::is_same_v<TRule, TRule_select_unparenthesized_stmt_core> ||
                 std::is_same_v<TRule, TRule_select_subexpr_core>
    TSQLResult<TYqlSelectArgs>
    BuildUnion(const TRule& rule, TYqlSelectArgs&& select) {
        {
            const auto& first = GetFirstArgument(rule);
            if (auto result = BuildIntersection(first, std::move(select))) {
                select = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        for (const auto& block : rule.GetBlock2()) {
            const auto& next = GetNextArgument(block);
            if (auto result = BuildIntersection(next, std::move(select))) {
                select = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }

            const auto& op = block.GetRule_union_op1();
            select.SetOps.emplace_back(ToOp(op));
        }

        return select;
    }

    template <class TRule>
        requires std::is_same_v<TRule, TRule_select_stmt_intersect> ||
                 std::is_same_v<TRule, TRule_select_unparenthesized_stmt_intersect> ||
                 std::is_same_v<TRule, TRule_select_subexpr_intersect>
    TSQLResult<TYqlSelectArgs>
    BuildIntersection(const TRule& rule, TYqlSelectArgs&& select) {
        {
            const auto& first = GetFirstArgument(rule);
            if (auto result = Build(first, std::move(select))) {
                select = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        for (const auto& block : rule.GetBlock2()) {
            const auto& next = GetNextArgument(block);
            if (auto result = Build(next, std::move(select))) {
                select = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }

            const auto& op = block.GetRule_intersect_op1();
            select.SetOps.emplace_back(ToOp(op));
        }

        return select;
    }

    TSQLResult<TYqlSelectArgs>
    Build(const TRule_select_kind_parenthesis& rule, TYqlSelectArgs&& select) {
        return Build(Unpack(rule), std::move(select), IsParenthesised(rule));
    }

    bool IsParenthesised(const TRule_select_kind_parenthesis& rule) {
        switch (rule.GetAltCase()) {
            case TRule_select_kind_parenthesis::kAltSelectKindParenthesis1:
                return false;
            case TRule_select_kind_parenthesis::kAltSelectKindParenthesis2:
                return true;
            case TRule_select_kind_parenthesis::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TSQLResult<TYqlSelectArgs> Build(
        const TRule_select_kind_partial& rule,
        TYqlSelectArgs&& select,
        bool isParenthesised = false)
    {
        auto result = Build(rule);
        if (!result) {
            return std::unexpected(result.error());
        }

        if (!isParenthesised && select.OrderBy) {
            YQL_ENSURE(!select.OrderBy->Keys.empty());
            Ctx_.Error(select.OrderBy->Keys.at(0)->OrderExpr->GetPos())
                << "ORDER BY within UNION/EXCEPT/INTERSECT "
                << "is only allowed after last subquery";
            return std::unexpected(ESQLError::Basic);
        }

        if (!isParenthesised && select.Limit) {
            Ctx_.Error((*select.Limit)->GetPos())
                << "LIMIT within UNION/EXCEPT/INTERSECT "
                << "is only allowed after last subquery";
            return std::unexpected(ESQLError::Basic);
        }

        if (!isParenthesised && select.Offset) {
            Ctx_.Error((*select.Offset)->GetPos())
                << "OFFSET within UNION/EXCEPT/INTERSECT "
                << "is only allowed after last subquery";
            return std::unexpected(ESQLError::Basic);
        }

        TYqlSelectArgs next = DestructYqlSelect(*result);

        YQL_ENSURE(next.SetItems.size() == 1);
        auto& only = next.SetItems[0];

        YQL_ENSURE(!next.OrderBy);
        next.OrderBy = std::exchange(only.OrderBy, Nothing());
        // ORDER BY is linked to `SqlSetItem`, because only this
        // allows to reference an external column.

        if (isParenthesised) {
            only.OrderBy = std::exchange(next.OrderBy, Nothing());
            only.Limit = std::exchange(next.Limit, Nothing());
            only.Offset = std::exchange(next.Offset, Nothing());
        }

        select.SetItems.emplace_back(std::move(only));
        select.SetOps.emplace_back(EYqlSetOp::Push);
        select.OrderBy = std::move(next.OrderBy);
        select.Limit = std::move(next.Limit);
        select.Offset = std::move(next.Offset);
        return select;
    }

    TSQLResult<TYqlSelectArgs> Build(
        const TRule_select_or_expr& rule,
        TYqlSelectArgs&& select)
    {
        switch (rule.GetAltCase()) {
            case TRule_select_or_expr::kAltSelectOrExpr1: {
                const auto& alt = rule.GetAlt_select_or_expr1();
                return Build(alt.GetRule_select_kind_partial1(), std::move(select));
            }
            case TRule_select_or_expr::kAltSelectOrExpr2:
                return Unsupported("tuple_or_expr at UNION/EXCEPT/INTERSECT context");
            case TRule_select_or_expr::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TNodeResult Build(const TRule_exists_expr::TBlock3& block) {
        switch (block.GetAltCase()) {
            case TRule_exists_expr_TBlock3::kAlt1:
                return Build(block.GetAlt1().GetRule_select_stmt1());
            case TRule_exists_expr_TBlock3::kAlt2:
                return Build(block.GetAlt2().GetRule_values_stmt1());
            case TRule_exists_expr_TBlock3::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TNodeResult Build(
        const TRule_select_or_expr& rule,
        EColumnRefState state,
        ESmartParenthesis smartParenthesis)
    {
        switch (rule.GetAltCase()) {
            case TRule_select_or_expr::kAltSelectOrExpr1: {
                const auto& alt = rule.GetAlt_select_or_expr1().GetRule_select_kind_partial1();
                return TYqlSelect(*this).Build(alt);
            }
            case TRule_select_or_expr::kAltSelectOrExpr2: {
                const auto& alt = rule.GetAlt_select_or_expr2().GetRule_tuple_or_expr1();
                return Build(alt, state, smartParenthesis);
            }
            case TRule_select_or_expr::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
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
            case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt4:
                return Unsupported("combine_core");
            case NSQLv1Generated::TRule_select_kind_TBlock2::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TNodeResult Build(const TRule_select_core& rule, TYqlSelectArgs&& select) {
        TYqlSetItemArgs setItem;

        if (rule.HasBlock1()) {
            Token(rule.GetBlock1().GetToken1());

            if (auto result = Build(rule.GetBlock1().GetRule_join_source2())) {
                setItem.Source = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        Token(rule.GetToken2());
        setItem.Position = Ctx_.Pos();

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

        if (auto q = rule.GetRule_opt_set_quantifier4(); q.HasBlock1()) {
            const auto& token = q.GetBlock1().GetToken1();

            if (IS_TOKEN(token.GetId(), ALL)) {
                setItem.Distinct = false;
            } else if (IS_TOKEN(token.GetId(), DISTINCT)) {
                setItem.Distinct = true;
            } else {
                Error() << "Expected ALL or DISTINCT, got " << Token(token);
                return std::unexpected(ESQLError::Basic);
            }
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
                setItem.Source = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock10()) {
            Token(rule.GetBlock10().GetToken1());
            if (auto result = Build(rule.GetBlock10().GetRule_expr2(), EColumnRefState::Allow)) {
                setItem.Where = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock11()) {
            if (auto result = Build(rule.GetBlock11().GetRule_group_by_clause1())) {
                setItem.GroupBy = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock12()) {
            Token(rule.GetBlock12().GetToken1());
            if (auto result = Build(rule.GetBlock12().GetRule_expr2(), EColumnRefState::Allow)) {
                setItem.Having = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        if (rule.HasBlock14()) {
            if (auto result = Build(rule.GetBlock14().GetRule_ext_order_by_clause1())) {
                setItem.OrderBy = std::move(*result);
            } else {
                return std::unexpected(result.error());
            }
        }

        TWinSpecs windows;
        Ctx_.WinSpecsScopes.push_back(std::ref(windows));
        Y_DEFER {
            Ctx_.WinSpecsScopes.pop_back();
        };

        if (rule.HasBlock13()) {
            const auto& w = rule.GetBlock13().GetRule_window_clause1();
            if (!TSqlWindow(*this).Build(w, windows)) {
                return std::unexpected(ESQLError::Basic);
            }
        }

        if (auto result = BuildProjection(rule)) {
            setItem.Projection = std::move(*result);
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& [name, w] : windows) {
            auto window = Build(*w);
            if (!window) {
                return std::unexpected(window.error());
            }

            setItem.Windows[name] = std::move(*window);
        }

        select.SetItems = {std::move(setItem)};
        select.SetOps = {EYqlSetOp::Push};

        auto node = BuildYqlSelect(Ctx_.Pos(), std::move(select));
        if (!node) {
            return std::unexpected(ESQLError::Basic);
        }

        return TNonNull(node);
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
                YQL_ENSURE(false, "Unreachable");
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

    TSQLResult<TWindow> Build(const TWindowSpecification& legacy) {
        TWindow window;

        window.Name = *legacy.ExistingWindowName.OrElse("");

        window.PartitionBy = legacy.Partitions;

        if (legacy.IsCompact) {
            return Unsupported("window_compact");
        }

        if (!legacy.OrderBy.empty()) {
            window.OrderBy = TOrderBy{.Keys = legacy.OrderBy};
        }

        if (legacy.Session) {
            return Unsupported("window_session");
        }

        window.Frame = legacy.Frame;

        return window;
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
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TSQLResult<EYqlJoinKind> Build(const TRule_join_op::TAlt2& alt) {
        Token(alt.GetToken3());

        if (alt.HasBlock1()) {
            return Unsupported("NATURAL");
        }

        const auto& block2 = alt.GetBlock2();
        switch (block2.GetAltCase()) {
            case TRule_join_op_TAlt2_TBlock2::kAlt1:
                break;
            case TRule_join_op_TAlt2_TBlock2::kAlt2:
                YQL_ENSURE(IS_TOKEN(block2.GetAlt2().GetToken1().GetId(), INNER));
                return EYqlJoinKind::Inner;
            case TRule_join_op_TAlt2_TBlock2::kAlt3:
                YQL_ENSURE(IS_TOKEN(block2.GetAlt3().GetToken1().GetId(), CROSS));
                return EYqlJoinKind::Cross;
            case TRule_join_op_TAlt2_TBlock2::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }

        const auto& alt1 = block2.GetAlt1();
        if (!alt1.HasBlock1()) {
            return EYqlJoinKind::Inner;
        }

        const auto& block1 = alt1.GetBlock1();
        switch (block1.GetAltCase()) {
            case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt1: {
                const auto& alt = block1.GetAlt1();

                if (alt.HasBlock2()) {
                    return Unsupported("(ONLY | SEMI)");
                }

                YQL_ENSURE(IS_TOKEN(block1.GetAlt1().GetToken1().GetId(), LEFT));
                return EYqlJoinKind::Left;
            }
            case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt2: {
                const auto& alt = block1.GetAlt2();

                if (alt.HasBlock2()) {
                    return Unsupported("(ONLY | SEMI)");
                }

                YQL_ENSURE(IS_TOKEN(block1.GetAlt2().GetToken1().GetId(), RIGHT));
                return EYqlJoinKind::Right;
            }
            case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt3:
                if (alt1.HasBlock2()) {
                    Token(alt1.GetBlock2().GetToken1());
                    Error() << "Invalid join type: EXCLUSION OUTER JOIN. "
                            << "OUTER keyword is optional and can only "
                            << "be used after LEFT, RIGHT or FULL";
                    return std::unexpected(ESQLError::Basic);
                }

                YQL_ENSURE(IS_TOKEN(block1.GetAlt3().GetToken1().GetId(), EXCLUSION));
                return Unsupported("EXCLUSION");
            case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt4:
                YQL_ENSURE(IS_TOKEN(block1.GetAlt4().GetToken1().GetId(), FULL));
                return EYqlJoinKind::Full;
            case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TSQLResult<TYqlJoinConstraint> Build(const TRule_join_constraint& rule, EYqlJoinKind kind) {
        switch (rule.GetAltCase()) {
            case TRule_join_constraint::kAltJoinConstraint1:
                return Build(rule.GetAlt_join_constraint1(), kind);
            case TRule_join_constraint::kAltJoinConstraint2:
                return Unsupported("USING pure_column_or_named_list");
            case TRule_join_constraint::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
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
        TSQLResult<TYqlSource> source = Build(rule.GetRule_hinted_single_source1());
        if (!source) {
            return std::unexpected(source.error());
        }

        if (rule.HasBlock2()) {
            return Unsupported("row_pattern_recognition_clause");
        }

        if (rule.HasBlock3()) {
            const auto& block = rule.GetBlock3();

            if (!source->Alias) {
                source->Alias.ConstructInPlace();
            }

            if (auto name = TableAlias(block.GetBlock1())) {
                source->Alias->Name = std::move(*name);
            } else {
                return std::unexpected(ESQLError::Basic);
            }

            if (block.HasBlock2()) {
                source->Alias->Columns = TableColumns(block.GetBlock2().GetRule_pure_column_list1());
            }
        }

        if (rule.HasBlock4()) {
            return Unsupported("sample_clause | tablesample_clause");
        }

        return source;
    }

    TSQLResult<TYqlSource> Build(const TRule_hinted_single_source& rule) {
        const auto result = Build(rule.GetRule_single_source1());

        if (rule.HasBlock2()) {
            return Unsupported("table_hints");
        }

        return result;
    }

    TSQLResult<TYqlSource> Build(const TRule_single_source& rule) {
        switch (rule.GetAltCase()) {
            case NSQLv1Generated::TRule_single_source::kAltSingleSource1:
                return Build(rule.GetAlt_single_source1().GetRule_table_ref1());
            case NSQLv1Generated::TRule_single_source::kAltSingleSource2: {
                TYqlSelect that(*this);
                that.CurrentCTE_ = Nothing();
                return that.Build(rule.GetAlt_single_source2().GetRule_select_stmt2())
                    .transform([](auto x) { return TYqlSource{.Node = std::move(x)}; });
            }
            case NSQLv1Generated::TRule_single_source::kAltSingleSource3:
                return Build(rule.GetAlt_single_source3().GetRule_values_stmt2())
                    .transform([](auto x) { return TYqlSource{.Node = std::move(x)}; });
            case NSQLv1Generated::TRule_single_source::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TSQLResult<TYqlSource> Build(const TRule_table_ref& rule) {
        if (auto maybe = TryBuildCTE(rule)) {
            return std::move(*maybe);
        }

        const bool isClusterExplicit = rule.HasBlock1();

        TString service = Ctx_.Scoped->CurrService;
        TDeferredAtom cluster = Ctx_.Scoped->CurrCluster;

        if (isClusterExplicit) {
            const auto& expr = rule.GetBlock1().GetRule_cluster_expr1();
            if (!ClusterExpr(expr, /* allowWildcard = */ false, service, cluster)) {
                return std::unexpected(ESQLError::Basic);
            }
        }

        const bool isAnonymous = rule.HasBlock2();

        return Build(
            rule.GetBlock3(),
            std::move(service),
            std::move(cluster),
            isAnonymous,
            isClusterExplicit);
    }

    TMaybe<TSQLResult<TYqlSource>> TryBuildCTE(const TRule_table_ref& rule) {
        const auto* name = GetCTERef(rule);
        if (!name) {
            return Nothing();
        }

        TString id = Id(*name, *this);
        if (id.empty()) {
            return std::unexpected(ESQLError::Basic);
        }

        const TReadyCTE* cte = CTEs_->Get(id);
        if (!cte) {
            return Nothing();
        }

        if (cte->IsRecursiveNotReady() && cte->Alias.Name != CurrentCTE_) {
            TString current =
                CurrentCTE_
                    .Transform([](auto&& x) { return "'" + x + "'"; })
                    .GetOrElse("undefined");

            Ctx_.Error() << "Can't reference outer RECURSIVE CTE '" << cte->Alias.Name << "'. "
                         << "Recursion only with a current CTE is allowed, "
                         << "which is " << current << " here";
            return std::unexpected(ESQLError::Basic);
        }

        TNodePtr node;
        if (cte->IsRecursiveNotReady()) {
            node = BuildYqlSelf(cte->Alias.Position);
        } else {
            node = cte->Node;
        }

        return TYqlSource{
            .Node = std::move(node),
            .Alias = cte->Alias,
        };
    }

    TSQLResult<TYqlSource>
    Build(
        const TRule_table_ref::TBlock3& block,
        TString service,
        TDeferredAtom cluster,
        bool isAnonymous,
        bool isClusterExplicit)
    {
        switch (block.GetAltCase()) {
            case TRule_table_ref_TBlock3::kAlt1:
                return Build(
                    block.GetAlt1().GetRule_table_key1(),
                    std::move(service),
                    std::move(cluster),
                    isAnonymous);
            case TRule_table_ref_TBlock3::kAlt2:
                return Unsupported("an_id_expr LPAREN (table_arg (COMMA table_arg)* COMMA?)? RPAREN");
            case TRule_table_ref_TBlock3::kAlt3:
                return Build(
                           block.GetAlt3(),
                           std::move(service),
                           std::move(cluster),
                           isAnonymous,
                           isClusterExplicit)
                    .transform([](auto x) { return TYqlSource{.Node = std::move(x)}; });
            case TRule_table_ref_TBlock3::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TSQLResult<TYqlSource> Build(
        const TRule_table_key& rule,
        TString service,
        TDeferredAtom cluster,
        bool isAnonymous)
    {
        if (cluster.Empty()) {
            const TString key = Id(rule.GetRule_id_table_or_type1(), *this);
            Ctx_.Error() << "No cluster name given and no default cluster is selected. "
                         << "If '" << key << "' is meant to be a CTE, "
                         << "check that it is defined and visible in the current scope";
            return std::unexpected(ESQLError::Basic);
        }

        if (rule.HasBlock2()) {
            return Unsupported("(VIEW view_name)");
        }

        TString key = Id(rule.GetRule_id_table_or_type1(), *this);
        if (key.empty()) {
            return std::unexpected(ESQLError::Basic);
        }

        TYqlTableRefArgs args = {
            .Service = std::move(service),
            .Cluster = std::move(cluster),
            .Key = TDeferredAtom(Ctx_.Pos(), key),
            .IsAnonymous = isAnonymous,
        };

        TYqlSource source = {
            .Node = BuildYqlTableRef(Ctx_.Pos(), std::move(args)),
            .Alias = TYqlSourceAlias{
                .Position = Ctx_.Pos(),
                .Name = std::move(key)},
        };

        return std::move(source);
    }

    TNodeResult Build(
        const TRule_table_ref::TBlock3::TAlt3& alt,
        TString service,
        TDeferredAtom cluster,
        bool isAnonymous,
        bool isClusterExplicit)
    {
        const auto& bindParameter = alt.GetRule_bind_parameter1();
        Token(bindParameter.GetToken1());

        TString name;
        if (!NamedNodeImpl(bindParameter, name, *this)) {
            return std::unexpected(ESQLError::Basic);
        }

        TNodePtr node = GetNamedNode(name);
        if (!node) {
            return std::unexpected(ESQLError::Basic);
        }

        if (auto source = GetYqlSource(node)) {
            if (isAnonymous) {
                return Unsupported("COMMAT bind_parameter");
            }

            if (isClusterExplicit && !Ctx_.Warning(Ctx_.Pos(), TIssuesIds::WARNING, [](auto& out) {
                    out << "Explicit cluster is ignored";
                }))
            {
                return std::unexpected(ESQLError::Basic);
            }

            return TNonNull(node);
        }

        TYqlTableRefArgs args = {
            .Service = std::move(service),
            .Cluster = std::move(cluster),
            .Key = TDeferredAtom(node, Ctx_),
            .IsAnonymous = isAnonymous,
        };

        return TNonNull(BuildYqlTableRef(Ctx_.Pos(), std::move(args)));
    }

    TSQLResult<TVector<TNodePtr>> Build(const TRule_values_source_row& rule) {
        TVector<TNodePtr> columns;

        TSqlExpression sqlExpr(*this);
        if (!Unwrap(ExprList(sqlExpr, columns, rule.GetRule_expr_list2()))) {
            return std::unexpected(ESQLError::Basic);
        }

        return columns;
    }

    TSQLResult<TGroupBy> Build(const TRule_group_by_clause& rule) {
        TPosition position = Ctx_.TokenPosition(rule.GetToken1());
        Token(rule.GetToken1());

        if (rule.HasBlock2()) {
            return Unsupported("GROUP COMPACT BY");
        }

        if (Ctx_.IsAnyUnusedHintForToken(position, [](const auto& hint) { return to_lower(hint.Name) == "compact"; })) {
            return Unsupported("GROUP /*+ compact */ BY");
        }

        if (TPosition position; IsDistinctOptSet(rule.GetRule_opt_set_quantifier4(), position)) {
            Ctx_.Error(position) << "DISTINCT is not supported in GROUP BY clause yet!";
            return std::unexpected(ESQLError::Basic);
        }

        if (rule.HasBlock6()) {
            return Unsupported("GROUP BY ... WITH an_id");
        }

        return Build(rule.GetRule_grouping_element_list5());
    }

    TSQLResult<TGroupBy> Build(const TRule_grouping_element_list& rule) {
        TVector<TGroupBy::TElement> elements(Reserve(1 + rule.GetBlock2().size()));

        if (auto result = Build(rule.GetRule_grouping_element1())) {
            elements.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& block : rule.GetBlock2()) {
            if (auto result = Build(block.GetRule_grouping_element2())) {
                elements.emplace_back(std::move(*result));
            } else {
                return std::unexpected(result.error());
            }
        }

        return TGroupBy{
            .Elements = std::move(elements),
        };
    }

    TSQLResult<TGroupBy::TElement> Build(const TRule_grouping_element& rule) {
        switch (rule.GetAltCase()) {
            case TRule_grouping_element::kAltGroupingElement1:
                return Build(rule.GetAlt_grouping_element1().GetRule_ordinary_grouping_set1());
            case TRule_grouping_element::kAltGroupingElement2:
                return Build(rule.GetAlt_grouping_element2().GetRule_rollup_list1());
            case TRule_grouping_element::kAltGroupingElement3:
                return Build(rule.GetAlt_grouping_element3().GetRule_cube_list1());
            case TRule_grouping_element::kAltGroupingElement4:
                return Build(rule.GetAlt_grouping_element4().GetRule_grouping_sets_specification1());
            case TRule_grouping_element::kAltGroupingElement5:
                return Unsupported("hopping_window_specification");
            case TRule_grouping_element::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TNodeResult Build(const TRule_ordinary_grouping_set& rule) {
        const auto& namedExpr = rule.GetRule_named_expr1();
        if (namedExpr.HasBlock2()) {
            return Unsupported("GROUP BY aliases");
        }

        TNodeResult result = Build(
            namedExpr.GetRule_expr1(),
            EColumnRefState::Allow,
            ESmartParenthesis::GroupBy);

        if (!result) {
            return std::unexpected(result.error());
        }

        TNodePtr expr = std::move(*result);

        const TString label = expr->GetLabel();
        const bool isColumn = expr->GetColumnName();
        const bool isGroupingSet = expr->ContentListPtr();

        if (isGroupingSet) {
            return TNonNull(std::move(expr));
        }

        if (!label && !isColumn && !Ctx_.YqlSelectAllowUnnamedGroupByExpr) {
            Ctx_.Error()
                << "Unnamed expressions are not supported here. "
                << "Please use '<expr> AS <name>' or PRAGMA YqlSelectAllowUnnamedGroupByExpr";
            return std::unexpected(ESQLError::Basic);
        }

        return TNonNull(std::move(expr));
    }

    TSQLResult<TGroupBy::TElement> Build(const TRule_rollup_list& rule) {
        Token(rule.GetToken1());
        return Build(rule.GetRule_ordinary_grouping_set_list3())
            .transform([](TVector<TNodePtr> exprs) {
                return TGroupingSets::TRollup{.Expressions = std::move(exprs)};
            });
    }

    TSQLResult<TGroupBy::TElement> Build(const TRule_cube_list& rule) {
        Token(rule.GetToken1());
        return Build(rule.GetRule_ordinary_grouping_set_list3())
            .transform([](TVector<TNodePtr> exprs) {
                return TGroupingSets::TCube{.Expressions = std::move(exprs)};
            });
    }

    TSQLResult<TGroupBy::TElement> Build(const TRule_grouping_sets_specification& rule) {
        const auto build = [&](const TRule_grouping_element& rule) -> TSQLResult<TVector<TNodePtr>> {
            if (rule.GetAltCase() != TRule_grouping_element::kAltGroupingElement1) {
                return Unsupported("GROUPING SETS with nested ROLLUP/CUBE/GROUPING SETS");
            }

            const auto& set = rule.GetAlt_grouping_element1().GetRule_ordinary_grouping_set1();
            return Build(set).transform([](TNonNull<TNodePtr> node) -> TVector<TNodePtr> {
                if (auto* set = dynamic_cast<TListOfNamedNodes*>(node.Get())) {
                    return *set->ContentListPtr();
                }

                return TVector<TNodePtr>{std::move(node)};
            });
        };

        Token(rule.GetToken1());
        const auto& list = rule.GetRule_grouping_element_list4();

        TVector<TVector<TNodePtr>> sets(Reserve(1 + list.GetBlock2().size()));

        if (auto result = build(list.GetRule_grouping_element1())) {
            sets.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& block : list.GetBlock2()) {
            if (auto result = build(block.GetRule_grouping_element2())) {
                sets.emplace_back(std::move(*result));
            } else {
                return std::unexpected(result.error());
            }
        }

        return TGroupingSets{.Sets = std::move(sets)};
    }

    TSQLResult<TVector<TNodePtr>> Build(const TRule_ordinary_grouping_set_list& list) {
        const auto build = [&](const TRule_ordinary_grouping_set& rule) -> TNodeResult {
            auto result = Build(rule);
            if (!result) {
                return std::unexpected(result.error());
            }

            if (dynamic_cast<const TListOfNamedNodes*>((*result).Get())) {
                return Unsupported("ROLLUP/CUBE sublists of elements in parenthesis");
            }

            return result;
        };

        TVector<TNodePtr> exprs(Reserve(1 + list.GetBlock2().size()));

        if (auto result = build(list.GetRule_ordinary_grouping_set1())) {
            exprs.emplace_back(std::move(*result));
        } else {
            return std::unexpected(result.error());
        }

        for (const auto& block : list.GetBlock2()) {
            if (auto result = build(block.GetRule_ordinary_grouping_set2())) {
                exprs.emplace_back(std::move(*result));
            } else {
                return std::unexpected(result.error());
            }
        }

        return exprs;
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
        if (direction.empty()) {
            isAscending = true;
        } else if (direction == "asc") {
            isAscending = true;
        } else if (direction == "desc") {
            isAscending = false;
        } else {
            YQL_ENSURE(false, "Unexpected ORDER BY direction " << direction);
        }

        return new TSortSpecification(*expr, isAscending);
    }

    template <class TRule>
        requires std::same_as<TRule, TRule_expr> ||
                 std::same_as<TRule, TRule_tuple_or_expr>
    TNodeResult Build(
        const TRule& rule,
        EColumnRefState state,
        ESmartParenthesis smartParenthesis = ESmartParenthesis::Default)
    {
        TColumnRefScope scope(Ctx_, state);
        TSqlExpression sqlExpr(*this);
        sqlExpr.SetSmartParenthesisMode(smartParenthesis);
        return sqlExpr.Build(rule);
    }

    TSQLResult<TMaybe<TString>> Label(const TRule_result_column::TAlt2& rule) {
        if (!rule.HasBlock2()) {
            return Nothing();
        }

        const auto& block = rule.GetBlock2();
        switch (block.Alt_case()) {
            case TRule_result_column_TAlt2_TBlock2::kAlt1: {
                TString id = Id(block.GetAlt1().GetRule_an_id_or_type2(), *this);
                if (id.empty()) {
                    return std::unexpected(ESQLError::Basic);
                }

                return id;
            }
            case TRule_result_column_TAlt2_TBlock2::kAlt2: {
                if (!Ctx_.AnsiOptionalAs) {
                    Error() << "Expecting mandatory AS here. "
                            << "Did you miss comma? "
                            << "Please add PRAGMA AnsiOptionalAs; "
                            << "for ANSI compatibility";
                    return std::unexpected(ESQLError::Basic);
                }

                TString id = Id(block.GetAlt2().GetRule_an_id_as_compat1(), *this);
                if (id.empty()) {
                    return std::unexpected(ESQLError::Basic);
                }

                return id;
            }
            case TRule_result_column_TAlt2_TBlock2::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TMaybe<TString> TableAlias(const TRule_named_single_source::TBlock3::TBlock1& block) {
        switch (block.GetAltCase()) {
            case TRule_named_single_source_TBlock3_TBlock1::kAlt1: {
                TString id = Id(block.GetAlt1().GetRule_an_id2(), *this);
                if (id.empty()) {
                    return Nothing();
                }

                return id;
            }
            case TRule_named_single_source_TBlock3_TBlock1::kAlt2: {
                if (!Ctx_.AnsiOptionalAs) {
                    Error() << "Expecting mandatory AS here. "
                            << "Did you miss comma? "
                            << "Please add PRAGMA AnsiOptionalAs; "
                            << "for ANSI compatibility";
                    return Nothing();
                }

                TString id = Id(block.GetAlt2().GetRule_an_id_as_compat1(), *this);
                if (id.empty()) {
                    return Nothing();
                }

                return id;
            }
            case TRule_named_single_source_TBlock3_TBlock1::ALT_NOT_SET:
                YQL_ENSURE(false, "Unreachable");
        }
    }

    TVector<TYqlColumnRef> TableColumns(const TRule_pure_column_list& rule) {
        TVector<TYqlColumnRef> columns(Reserve(1 + rule.GetBlock3().size()));
        columns.emplace_back(TableColumn(rule.GetRule_an_id2()));
        for (const auto& id : rule.GetBlock3()) {
            columns.emplace_back(TableColumn(id.GetRule_an_id2()));
        }
        return columns;
    }

    TYqlColumnRef TableColumn(const TRule_an_id& rule) {
        TString id = Id(rule, *this);
        return {
            .Position = Ctx_.Pos(),
            .Name = std::move(id),
        };
    }

    EYqlSetOp ToOp(const TRule_union_op& node) {
        const TString token = ToLowerUTF8(node.GetToken1().GetValue());

        EYqlSetOp op;
        if (token == "union") {
            op = EYqlSetOp::Union;
        } else if (token == "except") {
            op = EYqlSetOp::Except;
        } else {
            Y_ABORT(
                "You should change implementation according to grammar changes. "
                "Invalid token: %s", token.c_str());
        }

        if (IsAllQualifiedOp(node)) {
            op = AllQualified(op);
        }

        return op;
    }

    EYqlSetOp ToOp(const TRule_intersect_op& node) {
        const TString token = ToLowerUTF8(node.GetToken1().GetValue());

        EYqlSetOp op;
        if (token == "intersect") {
            op = EYqlSetOp::Intersect;
        } else {
            Y_ABORT(
                "You should change implementation according to grammar changes. "
                "Invalid token: %s", token.c_str());
        }

        if (IsAllQualifiedOp(node)) {
            op = AllQualified(op);
        }

        return op;
    }

    TNodeResult Finalize(TSQLResult<TYqlSelectArgs> select) {
        if (!select) {
            return std::unexpected(select.error());
        }

        YQL_ENSURE(!select->SetItems.empty());
        TPosition position = select->SetItems.front().Position;

        if (select->SetItems.size() == 1) {
            select = FinalizeOnly(std::move(*select));
        } else {
            select = FinalizeMulti(std::move(*select));
        }

        if (!select) {
            return std::unexpected(select.error());
        }

        return TNonNull(NSQLTranslationV1::BuildYqlSelect(
            std::move(position), std::move(*select)));
    }

    TYqlSelectArgs FinalizeOnly(TYqlSelectArgs&& select) {
        YQL_ENSURE(select.SetItems.size() == 1);
        auto& only = select.SetItems[0];

        const bool isItemAugmented =
            (only.OrderBy || only.Limit || only.Offset);

        const bool isSelectAugmented =
            (select.OrderBy || select.Limit || select.Offset);

        YQL_ENSURE(!(isItemAugmented && isSelectAugmented));

        if (!only.OrderBy) {
            // Pass `ORDER BY` to a `SqlSetItem`, because only this
            // allows to reference an external column
            only.OrderBy = std::exchange(select.OrderBy, Nothing());
        }

        if (only.Limit) {
            // Option `limit` at a `SqlSetItem` is not yet supported.
            select.Limit = std::exchange(only.Limit, Nothing());
        }

        if (only.Offset) {
            // Option `limit` at a `SqlSetItem` is not yet supported.
            select.Offset = std::exchange(only.Offset, Nothing());
        }

        return select;
    }

    TSQLResult<TYqlSelectArgs> FinalizeMulti(TYqlSelectArgs&& select) {
        YQL_ENSURE(1 < select.SetItems.size());

        for (const auto& item : select.SetItems) {
            if (item.OrderBy) {
                return Unsupported("ORDER BY within UNION/EXCEPT/INTERSECT component");
            }

            if (item.Limit) {
                return Unsupported("LIMIT within UNION/EXCEPT/INTERSECT component");
            }

            if (item.Offset) {
                return Unsupported("OFFSET within UNION/EXCEPT/INTERSECT component");
            }
        }

        return select;
    }

    std::unexpected<ESQLError> Unsupported(TStringBuf message) {
        return UnsupportedYqlSelect(Ctx_, message);
    }

    TNodeResult WithForkedNamespace(std::invocable<TYqlSelect&> auto&& f) {
        TYqlSelect that(*this);
        that.ForkNamespace();

        TNodeResult result = f(that);
        if (result && !that.WarnUnusedCTEs()) {
            return std::unexpected(ESQLError::Basic);
        }

        return result;
    }

    TMaybe<TString> CurrentCTE_ = Nothing();
};

NYql::TLangVersion YqlSelectLangVersion() {
    return NYql::GetMaxLangVersion();
}

std::unexpected<ESQLError> YqlSelectUnsupported(TContext& ctx, TStringBuf message) {
    if (ctx.GetYqlSelectMode() == EYqlSelect::Force) {
        ctx.Error() << "YqlSelect unsupported: " << message;
    }

    return std::unexpected(ESQLError::UnsupportedYqlSelect);
}

TNodeResult BuildYqlSelectStatement(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_select_stmt& rule)
{
    return TYqlSelect(that)
        .Build(rule)
        .transform([](auto x) {
            return TNonNull(BuildYqlStatement(std::move(x)));
        });
}

TNodeResult BuildYqlSelectStatement(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_values_stmt& rule)
{
    return TYqlSelect(that)
        .Build(rule)
        .transform([](auto x) {
            return TNonNull(BuildYqlStatement(std::move(x)));
        });
}

TNodeResult BuildYqlSelectSubExpr(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_select_subexpr& rule,
    EColumnRefState state,
    ESmartParenthesis smartParenthesis)
{
    return TYqlSelect(that)
        .Build(rule, state, smartParenthesis)
        .transform([](auto x) {
            return TNonNull(WrapYqlSelectSubExpr(std::move(x)));
        });
}

TNodeResult BuildYqlSelectSubExpr(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_select_unparenthesized_stmt& rule)
{
    return TYqlSelect(that)
        .Build(rule)
        .transform([](auto x) {
            return TNonNull(WrapYqlSelectSubExpr(std::move(x)));
        });
}

TNodeResult BuildYqlExists(
    TSqlTranslation& that,
    const NSQLv1Generated::TRule_exists_expr& rule)
{
    return TYqlSelect(that).Build(rule);
}

} // namespace NSQLTranslationV1
