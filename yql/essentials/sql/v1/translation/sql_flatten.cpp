#include "sql_flatten.h"

#include <util/string/ascii.h>

namespace NSQLTranslationV1 {

TNodeResult TSqlFlatten::NamedColumn(const TRule_named_column& node) {
    const auto& column = node.GetRule_column_name1();
    const TString sourceName = OptIdPrefixAsStr(column.GetRule_opt_id_prefix1(), *this);
    const TString columnName = Id(column.GetRule_an_id2(), *this);
    if (columnName.empty()) {
        if (!Ctx_.HasPendingErrors) {
            Ctx_.Error() << "Empty column name is not allowed";
        }
        return std::unexpected(ESQLError::Basic);
    }

    TNodePtr result = BuildColumn(Ctx_.Pos(), columnName, sourceName);
    if (node.HasBlock2()) {
        const TString label = Id(node.GetBlock2().GetRule_an_id2(), *this);
        if (label.empty()) {
            if (!Ctx_.HasPendingErrors) {
                Ctx_.Error() << "Empty column name is not allowed";
            }
            return std::unexpected(ESQLError::Basic);
        }
        result->SetLabel(label);
    }
    return TNonNull(std::move(result));
}

TSQLStatus TSqlFlatten::AddExpression(
    TFlattenByArgs& args,
    TNodePtr expression,
    TStringBuf sourceLabel)
{
    const TString* sourceName = expression->GetSourceName();
    const TString* columnName = expression->GetColumnName();
    if (columnName && (!sourceName || sourceName->empty() || *sourceName == sourceLabel)) {
        expression->AssumeColumn();
        args.Arguments.push_back({
            .Column = std::move(expression),
            .Expression = {},
        });
        return std::monostate();
    }

    const TString label = expression->GetLabel();
    if (label.empty()) {
        Ctx_.Error(expression->GetPos()) << "Unnamed expression after FLATTEN BY is not allowed";
        return std::unexpected(ESQLError::Basic);
    }
    auto targetColumn = BuildColumn(expression->GetPos(), label);
    if (columnName) {
        TVector<INode::TIdPart> ids = {BuildYqlColumnRef(expression->GetPos(), /*maybeType=*/false), *sourceName};
        auto column = BuildAccess(expression->GetPos(), ids, /*isLookup=*/false);
        ids = {std::move(column), *columnName};
        expression = BuildAccess(expression->GetPos(), ids, /*isLookup=*/false);
        expression->SetLabel(label);
    }
    args.Arguments.push_back({
        .Column = std::move(targetColumn),
        .Expression = std::move(expression),
    });
    return std::monostate();
}

TSQLResult<TFlattenByArgs> TSqlFlatten::NamedExpressions(
    const TRule_named_expr_list& node,
    TStringBuf sourceLabel)
{
    TColumnRefScope scope(Ctx_, EColumnRefState::Allow);
    TVector<TNodePtr> expressions;
    if (auto status = NamedExprList(node, expressions); !status || Ctx_.HasPendingErrors) {
        return std::unexpected(status ? ESQLError::Basic : status.error());
    }

    TFlattenByArgs args;
    for (TNodePtr& expression : expressions) {
        YQL_ENSURE(!expression->ContentListPtr());
        if (auto status = AddExpression(args, std::move(expression), sourceLabel); !status) {
            return std::unexpected(status.error());
        }
    }
    return args;
}

TSQLResult<TFlattenByArgs> TSqlFlatten::ParseArgs(
    TStringBuf sourceLabel,
    const TRule_flatten_by_arg& node)
{
    switch (node.GetAltCase()) {
        case TRule_flatten_by_arg::kAltFlattenByArg1: {
            const auto& alt = node.GetAlt_flatten_by_arg1();
            auto expression = NamedColumn(alt.GetRule_named_column1());
            if (!expression) {
                return std::unexpected(expression.error());
            }
            TFlattenByArgs args;
            if (auto status = AddExpression(args, std::move(*expression), sourceLabel); !status) {
                return std::unexpected(status.error());
            }
            return args;
        }
        case TRule_flatten_by_arg::kAltFlattenByArg2: {
            const auto& alt = node.GetAlt_flatten_by_arg2();
            return NamedExpressions(alt.GetRule_named_expr_list2(), sourceLabel);
        }
        case TRule_flatten_by_arg::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
}

TString TSqlFlatten::ParseMode(
    const TRule_flatten_source::TBlock2::TBlock2::TAlt1& alt)
{
    if (!alt.HasBlock1()) {
        return "auto";
    }
    return to_lower(Token(alt.GetBlock1().GetToken1()));
}

} // namespace NSQLTranslationV1
