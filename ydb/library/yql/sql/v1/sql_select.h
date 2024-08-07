#pragma once

#include "sql_translation.h"
#include <ydb/library/yql/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlSelect: public TSqlTranslation {
public:
    TSqlSelect(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSourcePtr Build(const TRule_select_stmt& node, TPosition& selectPos);
    TSourcePtr Build(const TRule_select_unparenthesized_stmt& node, TPosition& selectPos);

private:
    bool SelectTerm(TVector<TNodePtr>& terms, const TRule_result_column& node);
    bool ValidateSelectColumns(const TVector<TNodePtr>& terms);
    bool ColumnName(TVector<TNodePtr>& keys, const TRule_column_name& node);
    bool ColumnName(TVector<TNodePtr>& keys, const TRule_without_column_name& node);
    template<typename TRule>
    bool ColumnList(TVector<TNodePtr>& keys, const TRule& node);
    bool NamedColumn(TVector<TNodePtr>& columnList, const TRule_named_column& node);
    TSourcePtr SingleSource(const TRule_single_source& node, const TVector<TString>& derivedColumns, TPosition derivedColumnsPos, bool unorderedSubquery);
    TSourcePtr NamedSingleSource(const TRule_named_single_source& node, bool unorderedSubquery);
    bool FlattenByArg(const TString& sourceLabel, TVector<TNodePtr>& flattenByColumns, TVector<TNodePtr>& flattenByExprs, const TRule_flatten_by_arg& node);
    TSourcePtr FlattenSource(const TRule_flatten_source& node);
    TSourcePtr JoinSource(const TRule_join_source& node);
    bool JoinOp(ISource* join, const TRule_join_source::TBlock3& block, TMaybe<TPosition> anyPos);
    TNodePtr JoinExpr(ISource*, const TRule_join_constraint& node);
    TSourcePtr ProcessCore(const TRule_process_core& node, const TWriteSettings& settings, TPosition& selectPos);
    TSourcePtr ReduceCore(const TRule_reduce_core& node, const TWriteSettings& settings, TPosition& selectPos);

    struct TSelectKindPlacement {
        bool IsFirstInSelectOp = false;
        bool IsLastInSelectOp = false;
    };

    TSourcePtr SelectCore(const TRule_select_core& node, const TWriteSettings& settings, TPosition& selectPos,
        TMaybe<TSelectKindPlacement> placement, TVector<TSortSpecificationPtr>& selectOpOrederBy, bool& selectOpAssumeOrderBy);

    bool WindowDefinition(const TRule_window_definition& node, TWinSpecs& winSpecs);
    bool WindowClause(const TRule_window_clause& node, TWinSpecs& winSpecs);

    struct TSelectKindResult {
        TSourcePtr Source;
        TWriteSettings Settings;

        TVector<TSortSpecificationPtr> SelectOpOrderBy;
        bool SelectOpAssumeOrderBy = false;
        TNodePtr SelectOpSkipTake;

        inline explicit operator bool() const {
            return static_cast<bool>(Source);
        }
    };

    bool ValidateLimitOrderByWithSelectOp(TMaybe<TSelectKindPlacement> placement, TStringBuf what);
    bool NeedPassLimitOrderByToUnderlyingSelect(TMaybe<TSelectKindPlacement> placement);

    template<typename TRule>
    TSourcePtr Build(const TRule& node, TPosition pos, TSelectKindResult&& first);


    TSelectKindResult SelectKind(const TRule_select_kind& node, TPosition& selectPos, TMaybe<TSelectKindPlacement> placement);
    TSelectKindResult SelectKind(const TRule_select_kind_partial& node, TPosition& selectPos, TMaybe<TSelectKindPlacement> placement);
    TSelectKindResult SelectKind(const TRule_select_kind_parenthesis& node, TPosition& selectPos, TMaybe<TSelectKindPlacement> placement);
};

} //namespace NSQLTranslationV1
