#pragma once

#include "sql_translation.h"
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlValues: public TSqlTranslation {
public:
    explicit TSqlValues(const TSqlTranslation& that)
        : TSqlTranslation(that)
    {
    }

    TSourcePtr Build(const TRule_values_stmt& node, TPosition& valuesPos, const TVector<TString>& derivedColumns = {}, TPosition derivedColumnsPos = TPosition());

protected:
    bool BuildRows(const TRule_values_source_row_list& node, TVector<TVector<TNodePtr>>& rows);

    TSourcePtr ValuesSource(const TRule_values_source& node, const TVector<TString>& columnsHint,
                            const TString& operationName);

private:
    bool BuildRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow);
};

class TSqlIntoValues: public TSqlValues {
public:
    explicit TSqlIntoValues(const TSqlTranslation& that)
        : TSqlValues(that)
    {
    }

    TSourcePtr Build(const TRule_into_values_source& node, const TString& operationName);
};

class TSqlAsValues: public TSqlValues {
public:
    explicit TSqlAsValues(const TSqlTranslation& that)
        : TSqlValues(that)
    {
    }

    TSourcePtr Build(const TRule_values_source& node, const TString& operationName);
};

} // namespace NSQLTranslationV1
