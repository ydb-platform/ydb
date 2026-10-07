#include "sql_ddl_topic.h"

#include "sql_expression.h"

namespace NSQLTranslationV1 {

namespace {

bool StoreConsumerIntervalSetting(
    TNodePtr& setting, TSqlExpression& ctx, TStringBuf statement, const TIdentifier& id,
    const TNodePtr& valueExprNode, bool reset) {
    if (setting) {
        ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
        return false;
    }
    if (reset) {
        ctx.Error() << to_upper(id.Name) << " reset is not supported";
        return false;
    }
    if (valueExprNode->GetOpName() != "Interval") {
        ctx.Error() << "Literal of Interval type is expected for " << to_upper(id.Name) << " setting";
        return false;
    }
    setting = valueExprNode;
    return true;
}

bool StoreConsumerSettingsEntry(
    const TIdentifier& id, const TRule_topic_consumer_setting_value* value, TSqlExpression& ctx,
    TTopicConsumerSettings& settings,
    bool reset, bool alter) {
    YQL_ENSURE(value || reset);
    const TStringBuf statement = alter ? "ALTER CONSUMER"sv : "CONSUMER"sv;
    TNodePtr valueExprNode;
    if (value) {
        valueExprNode = Unwrap(ctx.Build(value->GetRule_expr1()));
        if (!valueExprNode) {
            ctx.Error() << "invalid value for setting: " << id.Name;
            return false;
        }
    }
    auto name = to_lower(id.Name);
    if (name == "important") {
        if (settings.Important) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "Bool") {
            ctx.Error() << to_upper(id.Name) << " value should be boolean";
            return false;
        }
        settings.Important = valueExprNode;
    } else if (name == "availability_period") {
        if (settings.AvailabilityPeriod) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            settings.AvailabilityPeriod.Reset();
        } else {
            if (valueExprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for " << to_upper(id.Name) << " setting";
                return false;
            }
            settings.AvailabilityPeriod.Set(valueExprNode);
        }
    } else if (name == "read_from") {
        if (settings.ReadFromTs) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            settings.ReadFromTs.Reset();
        } else {
            settings.ReadFromTs.Set(valueExprNode);
        }
    } else if (name == "supported_codecs") {
        if (settings.SupportedCodecs) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            settings.SupportedCodecs.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            settings.SupportedCodecs.Set(valueExprNode);
        }
    } else if (name == "type") {
        if (settings.Type) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (alter) {
            ctx.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || (valueExprNode->GetLiteralType() != "String" && valueExprNode->GetLiteralType() != "Enum")) {
            ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
        TString value = to_upper(valueExprNode->GetLiteralValue());
        if (value != "STREAMING" && value != "SHARED") {
            ctx.Error() << to_upper(id.Name) << " value should be 'STREAMING' or 'SHARED', got: " << valueExprNode->GetLiteralValue();
            return false;
        }
        settings.Type = valueExprNode;
    } else if (name == "keep_messages_order") {
        if (settings.KeepMessagesOrder) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (alter) {
            ctx.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "Bool") {
            ctx.Error() << to_upper(id.Name) << " value should be boolean";
            return false;
        }
        settings.KeepMessagesOrder = valueExprNode;
    } else if (name == "default_processing_timeout") {
        if (!StoreConsumerIntervalSetting(settings.DefaultProcessingTimeout, ctx, statement, id, valueExprNode, reset)) {
            return false;
        }
    } else if (name == "max_processing_attempts") {
        if (settings.MaxProcessingAttempts) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsIntegerLiteral()) {
            ctx.Error() << to_upper(id.Name) << " value should be a integer";
            return false;
        }
        settings.MaxProcessingAttempts = valueExprNode;
    } else if (name == "dead_letter_policy") {
        if (settings.DeadLetterPolicy) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || (valueExprNode->GetLiteralType() != "String" && valueExprNode->GetLiteralType() != "Enum")) {
            ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
        TString value = to_upper(valueExprNode->GetLiteralValue());
        if (value != "MOVE" && value != "DELETE" && value != "NONE") {
            ctx.Error() << to_upper(id.Name) << " value should be 'MOVE', 'DELETE' or 'NONE', got: " << valueExprNode->GetLiteralValue();
            return false;
        }
        settings.DeadLetterPolicy = valueExprNode;
    } else if (name == "dead_letter_queue") {
        if (settings.DeadLetterQueue) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in " << statement << " statement for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
            ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
        settings.DeadLetterQueue = valueExprNode;
    } else if (name == "receive_message_wait_time") {
        if (!StoreConsumerIntervalSetting(settings.ReceiveMessageWaitTime, ctx, statement, id, valueExprNode, reset)) {
            return false;
        }
    } else if (name == "receive_message_delay") {
        if (!StoreConsumerIntervalSetting(settings.ReceiveMessageDelay, ctx, statement, id, valueExprNode, reset)) {
            return false;
        }
    } else {
        ctx.Error() << to_upper(id.Name) << ": unknown option for consumer";
        return false;
    }
    return true;
}

} // namespace

TIdentifier TTopicTranslation::GetTopicConsumerId(const TRule_topic_consumer_ref& node) {
    return IdEx(node.GetRule_an_id_pure1(), *this);
}

bool TTopicTranslation::CreateConsumerSettings(
    const TRule_topic_consumer_settings& node, TTopicConsumerSettings& settings) {
    const auto& firstEntry = node.GetRule_topic_consumer_settings_entry1();
    TSqlExpression expr(*this);
    if (!StoreConsumerSettingsEntry(
            IdEx(firstEntry.GetRule_an_id1(), *this),
            &firstEntry.GetRule_topic_consumer_setting_value3(),
            expr, settings, /*reset=*/false,
            /* alter = */ false)) {
        return false;
    }
    for (auto& block : node.GetBlock2()) {
        const auto& entry = block.GetRule_topic_consumer_settings_entry2();
        if (!StoreConsumerSettingsEntry(
                IdEx(entry.GetRule_an_id1(), *this),
                &entry.GetRule_topic_consumer_setting_value3(),
                expr, settings, /*reset=*/false,
                /* alter = */ false)) {
            return false;
        }
    }
    return true;
}

bool TTopicTranslation::CreateTopicConsumer(
    const TRule_topic_create_consumer_entry& node,
    TVector<TTopicConsumerDescription>& consumers) {
    consumers.emplace_back(IdEx(node.GetRule_an_id2(), *this));

    if (node.HasBlock3()) {
        auto& settings = node.GetBlock3().GetRule_topic_consumer_with_settings1().GetRule_topic_consumer_settings3();
        if (!CreateConsumerSettings(settings, consumers.back().Settings)) {
            return false;
        }
    }

    return true;
}

bool TTopicTranslation::AlterTopicConsumerEntry(
    const TRule_alter_topic_alter_consumer_entry& node, TTopicConsumerDescription& alterConsumer) {
    switch (node.Alt_case()) {
        case TRule_alter_topic_alter_consumer_entry::kAltAlterTopicAlterConsumerEntry1:
            return CreateConsumerSettings(
                node.GetAlt_alter_topic_alter_consumer_entry1().GetRule_topic_alter_consumer_set1().GetRule_topic_consumer_settings3(),
                alterConsumer.Settings);
        // case TRule_alter_topic_alter_consumer_entry::ALT_NOT_SET:
        case TRule_alter_topic_alter_consumer_entry::kAltAlterTopicAlterConsumerEntry2: {
            auto& resetNode = node.GetAlt_alter_topic_alter_consumer_entry2().GetRule_topic_alter_consumer_reset1();
            TSqlExpression expr(*this);
            if (!StoreConsumerSettingsEntry(
                    IdEx(resetNode.GetRule_an_id3(), *this),
                    /*value=*/nullptr,
                    expr, alterConsumer.Settings, /*reset=*/true,
                    /* alter = */ true)) {
                return false;
            }

            for (auto& resetItem : resetNode.GetBlock4()) {
                if (!StoreConsumerSettingsEntry(
                        IdEx(resetItem.GetRule_an_id2(), *this),
                        /*value=*/nullptr,
                        expr, alterConsumer.Settings, /*reset=*/true,
                        /* alter = */ true)) {
                    return false;
                }
            }
            return true;
        }
        case NSQLv1Generated::TRule_alter_topic_alter_consumer_entry::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
    return true;
}

bool TTopicTranslation::AlterTopicConsumer(
    const TRule_alter_topic_alter_consumer& node,
    THashMap<TString, TTopicConsumerDescription>& alterConsumers) {
    auto consumerId = GetTopicConsumerId(node.GetRule_topic_consumer_ref3());
    TString name = to_lower(consumerId.Name);
    auto iter = alterConsumers.insert(std::make_pair(
                                          name, TTopicConsumerDescription(std::move(consumerId))))
                    .first;
    return AlterTopicConsumerEntry(node.GetRule_alter_topic_alter_consumer_entry4(), iter->second);
}

bool TTopicTranslation::CreateTopicEntry(const TRule_create_topic_entry& node, TCreateTopicParameters& params) {
    // Will need a switch() here if (ever) create_topic_entry gets more than 1 type of statement
    auto& consumer = node.GetRule_topic_create_consumer_entry1();
    return CreateTopicConsumer(consumer, params.Consumers);
}

namespace {

bool StoreTopicSettingsEntry(
    const TIdentifier& id, const TRule_topic_setting_value* value, TSqlExpression& ctx,
    TTopicSettings& settings, bool reset) {
    YQL_ENSURE(value || reset);
    TNodePtr valueExprNode;
    if (value) {
        valueExprNode = Unwrap(ctx.Build(value->GetRule_expr1()));
        if (!valueExprNode) {
            ctx.Error() << "invalid value for setting: " << id.Name;
            return false;
        }
    }

    if (to_lower(id.Name) == "min_active_partitions") {
        if (reset) {
            settings.MinPartitions.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.MinPartitions.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_count_limit" || to_lower(id.Name) == "max_active_partitions") {
        if (reset) {
            settings.MaxPartitions.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.MaxPartitions.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "retention_period") {
        if (reset) {
            settings.RetentionPeriod.Reset();
        } else {
            if (valueExprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for retention";
                return false;
            }
            settings.RetentionPeriod.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "retention_storage_mb") {
        if (reset) {
            settings.RetentionStorage.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.RetentionStorage.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_write_speed_bytes_per_second") {
        if (reset) {
            settings.PartitionWriteSpeed.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.PartitionWriteSpeed.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_write_burst_bytes") {
        if (reset) {
            settings.PartitionWriteBurstSpeed.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.PartitionWriteBurstSpeed.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "metering_mode") {
        if (reset) {
            settings.MeteringMode.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be string";
                return false;
            }
            settings.MeteringMode.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "supported_codecs") {
        if (reset) {
            settings.SupportedCodecs.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be string";
                return false;
            }
            settings.SupportedCodecs.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "auto_partitioning_stabilization_window") {
        if (reset) {
            settings.AutoPartitioningStabilizationWindow.Reset();
        } else {
            if (valueExprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for retention";
                return false;
            }
            settings.AutoPartitioningStabilizationWindow.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "auto_partitioning_up_utilization_percent") {
        if (reset) {
            settings.AutoPartitioningUpUtilizationPercent.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.AutoPartitioningUpUtilizationPercent.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "auto_partitioning_down_utilization_percent") {
        if (reset) {
            settings.AutoPartitioningDownUtilizationPercent.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.AutoPartitioningDownUtilizationPercent.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "auto_partitioning_strategy") {
        if (reset) {
            settings.AutoPartitioningStrategy.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be string";
                return false;
            }
            settings.AutoPartitioningStrategy.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "metrics_level") {
        if (reset) {
            settings.MetricsLevel.Reset();
        } else if (!StoreStringOrInt(valueExprNode, settings.MetricsLevel)) {
            ctx.Error() << to_upper(id.Name) << " value should be an integer or a string";
            return false;
        }
    } else if (to_lower(id.Name) == "content_based_deduplication") {
        if (reset) {
            settings.ContentBasedDeduplication.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "Bool") {
                ctx.Error() << to_upper(id.Name) << " value should be bool";
                return false;
            }
            settings.ContentBasedDeduplication.Set(valueExprNode);
        }
    } else {
        ctx.Error() << "unknown topic setting: " << id.Name;
        return false;
    }
    return true;
}

} // namespace

bool TTopicTranslation::AlterTopicAction(const TRule_alter_topic_action& node, TAlterTopicParameters& params) {
    // alter_topic_action:
    // alter_topic_add_consumer
    // | alter_topic_alter_consumer
    // | alter_topic_drop_consumer
    // | alter_topic_set_settings
    // | alter_topic_reset_settings

    switch (node.Alt_case()) {
        case TRule_alter_topic_action::kAltAlterTopicAction1: // alter_topic_add_consumer
            return CreateTopicConsumer(
                node.GetAlt_alter_topic_action1().GetRule_alter_topic_add_consumer1().GetRule_topic_create_consumer_entry2(),
                params.AddConsumers);

        case TRule_alter_topic_action::kAltAlterTopicAction2: // alter_topic_alter_consumer
            return AlterTopicConsumer(
                node.GetAlt_alter_topic_action2().GetRule_alter_topic_alter_consumer1(),
                params.AlterConsumers);

        case TRule_alter_topic_action::kAltAlterTopicAction3: // drop_consumer
            params.DropConsumers.emplace_back(GetTopicConsumerId(
                node.GetAlt_alter_topic_action3().GetRule_alter_topic_drop_consumer1().GetRule_topic_consumer_ref3()));
            return true;

        case TRule_alter_topic_action::kAltAlterTopicAction4: // set_settings
            return CreateTopicSettings(
                node.GetAlt_alter_topic_action4().GetRule_alter_topic_set_settings1().GetRule_topic_settings3(),
                params.TopicSettings);

        case TRule_alter_topic_action::kAltAlterTopicAction5: { // reset_settings
            auto& resetNode = node.GetAlt_alter_topic_action5().GetRule_alter_topic_reset_settings1();
            TSqlExpression expr(*this);
            if (!StoreTopicSettingsEntry(
                    IdEx(resetNode.GetRule_an_id3(), *this),
                    /*value=*/nullptr, expr,
                    params.TopicSettings, /*reset=*/true)) {
                return false;
            }

            for (auto& resetItem : resetNode.GetBlock4()) {
                if (!StoreTopicSettingsEntry(
                        IdEx(resetItem.GetRule_an_id_pure2(), *this),
                        /*value=*/nullptr, expr,
                        params.TopicSettings, /*reset=*/true)) {
                    return false;
                }
            }
            return true;
        }

        case NSQLv1Generated::TRule_alter_topic_action::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
    return true;
}

bool TTopicTranslation::CreateTopicSettings(const TRule_topic_settings& node, TTopicSettings& params) {
    const auto& firstEntry = node.GetRule_topic_settings_entry1();
    TSqlExpression expr(*this);

    if (!StoreTopicSettingsEntry(
            IdEx(firstEntry.GetRule_an_id1(), *this),
            &firstEntry.GetRule_topic_setting_value3(),
            expr, params, /*reset=*/false)) {
        return false;
    }
    for (auto& block : node.GetBlock2()) {
        const auto& entry = block.GetRule_topic_settings_entry2();
        if (!StoreTopicSettingsEntry(
                IdEx(entry.GetRule_an_id1(), *this),
                &entry.GetRule_topic_setting_value3(),
                expr, params, /*reset=*/false)) {
            return false;
        }
    }
    return true;
}

bool TTopicTranslation::TopicRefImpl(const TRule_topic_ref& node, TTopicRef& result) {
    TString service = Context().Scoped->CurrService;
    TDeferredAtom cluster = Context().Scoped->CurrCluster;
    if (node.HasBlock1()) {
        if (Mode_ == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            Error() << "Cluster should not be used in limited view";
            return false;
        }

        if (!ClusterExpr(node.GetBlock1().GetRule_cluster_expr1(), /*allowWildcard=*/false, service, cluster)) {
            return false;
        }
    }

    if (cluster.Empty()) {
        Error() << "No cluster name given and no default cluster is selected";
        return false;
    }

    result = TTopicRef(Context().MakeName("topic"), cluster, nullptr);
    auto topic = Id(node.GetRule_an_id2(), *this);
    result.Keys = BuildTopicKey(Context().Pos(), result.Cluster, TDeferredAtom(Context().Pos(), topic));

    return true;
}

TNodePtr TTopicTranslation::Build(const TRule_create_topic_stmt& rule) {
    Ctx_.BodyPart();
    // create_topic_stmt: CREATE TOPIC (IF NOT EXISTS)? topic1 (CONSUMER ...)? [WITH (opt1 = val1, ...]?
    TTopicRef tr;
    if (!TopicRefImpl(rule.GetRule_topic_ref4(), tr)) {
        return nullptr;
    }
    bool existingOk = false;
    if (rule.HasBlock3()) { // if not exists
        existingOk = true;
    }

    TCreateTopicParameters params;
    params.ExistingOk = existingOk;
    if (rule.HasBlock5()) { // create_topic_entry (consumers)
        auto& entries = rule.GetBlock5().GetRule_create_topic_entries1();
        auto& firstEntry = entries.GetRule_create_topic_entry2();
        if (!CreateTopicEntry(firstEntry, params)) {
            return nullptr;
        }
        const auto& list = entries.GetBlock3();
        for (auto& node : list) {
            if (!CreateTopicEntry(node.GetRule_create_topic_entry2(), params)) {
                return nullptr;
            }
        }
    }
    if (rule.HasBlock6()) { // with_topic_settings
        auto& topic_settings_node = rule.GetBlock6().GetRule_with_topic_settings1().GetRule_topic_settings3();
        CreateTopicSettings(topic_settings_node, params.TopicSettings);
    }

    return BuildCreateTopic(Ctx_.Pos(), tr, params, Ctx_.Scoped);
}

TNodePtr TTopicTranslation::Build(const TRule_alter_topic_stmt& rule) {
    // alter_topic_stmt: ALTER TOPIC topic_ref alter_topic_action (COMMA alter_topic_action)*;
    // alter_topic_stmt: ALTER TOPIC IF EXISTS topic_ref alter_topic_action (COMMA alter_topic_action)*;

    Ctx_.BodyPart();
    TTopicRef tr;
    bool missingOk = false;
    if (rule.HasBlock3()) { // IF EXISTS
        missingOk = true;
    }
    if (!TopicRefImpl(rule.GetRule_topic_ref4(), tr)) {
        return nullptr;
    }

    TAlterTopicParameters params;
    params.MissingOk = missingOk;
    auto& firstEntry = rule.GetRule_alter_topic_action5();
    if (!AlterTopicAction(firstEntry, params)) {
        return nullptr;
    }
    const auto& list = rule.GetBlock6();
    for (auto& node : list) {
        if (!AlterTopicAction(node.GetRule_alter_topic_action2(), params)) {
            return nullptr;
        }
    }

    return BuildAlterTopic(Ctx_.Pos(), tr, params, Ctx_.Scoped);
}

TNodePtr TTopicTranslation::Build(const TRule_drop_topic_stmt& rule) {
    // drop_topic_stmt: DROP TOPIC (IF EXISTS)? topic_ref;
    Ctx_.BodyPart();

    TDropTopicParameters params;
    params.MissingOk = rule.HasBlock3(); // IF EXISTS

    TTopicRef tr;
    if (!TopicRefImpl(rule.GetRule_topic_ref4(), tr)) {
        return nullptr;
    }
    return BuildDropTopic(Ctx_.Pos(), tr, params, Ctx_.Scoped);
}

} // namespace NSQLTranslationV1
