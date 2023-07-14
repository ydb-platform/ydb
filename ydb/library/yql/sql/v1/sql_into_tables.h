#pragma once

#include "sql_translation.h"
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlIntoTable: public TSqlTranslation {
public:
    TSqlIntoTable(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_into_table_stmt& node);

private:
    //bool BuildValuesRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow);
    //TSourcePtr ValuesSource(const TRule_values_source& node, TVector<TString>& columnsHint);
    //TSourcePtr IntoValuesSource(const TRule_into_values_source& node);

    bool ValidateServiceName(const TRule_into_table_stmt& node, const TTableRef& table, ESQLWriteColumnMode mode,
        const TPosition& pos);
    TString SqlIntoModeStr;
    TString SqlIntoUserModeStr;
};

} // namespace NSQLTranslationV1
