#pragma once

#include "ddl_streaming_query.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TStreamingQueryTranslation final: public TSqlTranslation {
public:
    TStreamingQueryTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_create_streaming_query_stmt& node);
    TNodePtr Build(const TRule_alter_streaming_query_stmt& node);
    TNodePtr Build(const TRule_drop_streaming_query_stmt& node);

private:
    bool BuildStreamingQueryNestedSetting(TNodePtr value, TObjectFeatureNodePtr& settings);
    bool BuildStreamingQuerySettingValue(TStringBuf name, TNodePtr value, TPosition pos, TObjectFeatureNode& features);
    bool ParseStreamingQuerySetting(const TRule_streaming_query_setting& node, TStreamingQuerySettings& settings);
    bool ParseStreamingQuerySettings(const TRule_streaming_query_settings& node, TStreamingQuerySettings& settings);
    bool ParseStreamingQueryDefinition(const TRule_streaming_query_definition& node, TStreamingQuerySettings& settings);
    bool ParseAlterStreamingQueryAction(const TRule_alter_streaming_query_action& node, TStreamingQuerySettings& settings);
};

} // namespace NSQLTranslationV1
