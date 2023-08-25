#pragma once

#include "sql_translation.h"
#include <ydb/library/yql/parser/proto_ast/gen/v1_proto/SQLv1Parser.pb.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlValues: public TSqlTranslation {
public:
    TSqlValues(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSourcePtr Build(const TRule_values_stmt& node, TPosition& valuesPos, const TVector<TString>& derivedColumns = {}, TPosition derivedColumnsPos = TPosition());
protected:
    bool BuildRows(const TRule_values_source_row_list& node, TVector<TVector<TNodePtr>>& rows);

private:
    bool BuildRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow);
};

class TSqlIntoValues: public TSqlValues {
public:
    TSqlIntoValues(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlValues(ctx, mode)
    {
    }

    TSourcePtr Build(const TRule_into_values_source& node, const TString& operationName);

private:
    TSourcePtr ValuesSource(const TRule_values_source& node, TVector<TString>& columnsHint,
        const TString& operationName);
};

} // namespace NSQLTranslationV1
