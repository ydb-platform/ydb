#pragma once

#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TSqlFlatten final: public TSqlTranslation {
public:
    explicit TSqlFlatten(const TSqlTranslation& that)
        : TSqlTranslation(that)
    {
    }

    TSQLResult<TFlattenByArgs> ParseArgs(TStringBuf sourceLabel, const TRule_flatten_by_arg& node);
    TString ParseMode(const TRule_flatten_source::TBlock2::TBlock2::TAlt1& alt);

private:
    TNodeResult NamedColumn(const TRule_named_column& node);
    TSQLResult<TFlattenByArgs> NamedExpressions(const TRule_named_expr_list& node, TStringBuf sourceLabel);
    TSQLStatus AddExpression(TFlattenByArgs& args, TNodePtr expression, TStringBuf sourceLabel);
};

} // namespace NSQLTranslationV1
