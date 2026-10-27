#pragma once

#include "ddl_topic.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TTopicTranslation final: public TSqlTranslation {
public:
    TTopicTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_create_topic_stmt& rule);
    TNodePtr Build(const TRule_alter_topic_stmt& rule);
    TNodePtr Build(const TRule_drop_topic_stmt& rule);

private:
    TIdentifier GetTopicConsumerId(const TRule_topic_consumer_ref& node);
    bool CreateConsumerSettings(const TRule_topic_consumer_settings& settingsNode, TTopicConsumerSettings& settings);
    bool CreateTopicSettings(const TRule_topic_settings& node, TTopicSettings& params);
    bool CreateTopicConsumer(const TRule_topic_create_consumer_entry& node,
                             TVector<TTopicConsumerDescription>& consumers);
    bool CreateTopicEntry(const TRule_create_topic_entry& node, TCreateTopicParameters& params);

    bool AlterTopicConsumer(const TRule_alter_topic_alter_consumer& node,
                            THashMap<TString, TTopicConsumerDescription>& alterConsumers);

    bool AlterTopicConsumerEntry(const TRule_alter_topic_alter_consumer_entry& node,
                                 TTopicConsumerDescription& alterConsumer);

    bool AlterTopicAction(const TRule_alter_topic_action& node, TAlterTopicParameters& params);

    bool TopicRefImpl(const TRule_topic_ref& node, TTopicRef& result);
};

} // namespace NSQLTranslationV1
