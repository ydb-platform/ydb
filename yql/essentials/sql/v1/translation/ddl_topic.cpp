#include "ddl_topic.h"

#include "context.h"
#include "source.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

using namespace NYql;

namespace NSQLTranslationV1 {

TTopicRef::TTopicRef(TString refName, TDeferredAtom cluster, TNodePtr keys)
    : RefName(std::move(refName))
    , Cluster(std::move(cluster))
    , Keys(std::move(keys))
{
}

class TTopicKey: public ITableKeys {
public:
    TTopicKey(TPosition pos, TDeferredAtom cluster, const TDeferredAtom& name)
        : ITableKeys(pos)
        , Cluster_(std::move(cluster))
        , Name_(name)
        , Full_(name.GetRepr())
    {
    }

    const TString* GetTableName() const override {
        return Name_.GetLiteral() ? &Full_ : nullptr;
    }

    TNodePtr BuildKeys(TContext& ctx, ITableKeys::EBuildKeysMode) override {
        const auto path = ctx.GetPrefixedPath(Service_, Cluster_, Name_);
        if (!path) {
            return nullptr;
        }
        auto key = Y("Key", Q(Y(Q("topic"), Y("String", path))));
        return key;
    }

private:
    TString Service_;
    TDeferredAtom Cluster_;
    TDeferredAtom Name_;
    TString View_;
    TString Full_;
};

TNodePtr BuildTopicKey(TPosition pos, const TDeferredAtom& cluster, const TDeferredAtom& name) {
    return new TTopicKey(pos, cluster, name);
}

namespace {

TNullable<TNodePtr> CreateConsumerDesc(TContext& ctx, const TTopicConsumerDescription& desc, const INode& node, bool alter) {
    auto setValue = [&](const TNodePtr& settings, const TNodePtr& value, const auto& setter) {
        if (value) {
            return node.L(settings, node.Q(node.Y(node.Q(setter), value)));
        }
        return settings;
    };

    auto setValueWithReset = [&](const TNodePtr& settings, const NYql::TResetableSetting<TNodePtr, void>& value, const auto& setter, const auto& resetter) {
        if (!value) {
            return settings;
        }
        if (value.IsSet()) {
            return node.L(settings, node.Q(node.Y(node.Q(setter), value.GetValueSet())));
        } else {
            YQL_ENSURE(alter, "Cannot reset on create");
            return node.L(settings, node.Q(node.Y(node.Q(resetter), node.Q(node.Y()))));
        }
    };

    if (alter) {
        if (desc.Settings.Type) {
            ctx.Error() << "type alter is not supported";
            return {nullptr};
        }
        if (desc.Settings.KeepMessagesOrder) {
            ctx.Error() << "keep_messages_order alter is not supported";
            return {nullptr};
        }
    }

    auto settings = node.Y();
    settings = setValue(settings, desc.Settings.Important, "important");
    settings = setValueWithReset(settings, desc.Settings.AvailabilityPeriod, "setAvailabilityPeriod", "resetAvailabilityPeriod");
    settings = setValueWithReset(settings, desc.Settings.ReadFromTs, "setReadFromTs", "resetReadFromTs");
    settings = setValueWithReset(settings, desc.Settings.SupportedCodecs, "setSupportedCodecs", "resetSupportedCodecs");
    settings = setValue(settings, desc.Settings.Type, "type");
    settings = setValue(settings, desc.Settings.KeepMessagesOrder, "keep_messages_order");
    settings = setValue(settings, desc.Settings.DefaultProcessingTimeout, "default_processing_timeout");
    settings = setValue(settings, desc.Settings.MaxProcessingAttempts, "max_processing_attempts");
    settings = setValue(settings, desc.Settings.DeadLetterPolicy, "dead_letter_policy");
    settings = setValue(settings, desc.Settings.DeadLetterQueue, "dead_letter_queue");
    settings = setValue(settings, desc.Settings.ReceiveMessageWaitTime, "receive_message_wait_time");
    settings = setValue(settings, desc.Settings.ReceiveMessageDelay, "receive_message_delay");

    return node.Y(
        node.Q(node.Y(node.Q("name"), BuildQuotedAtom(desc.Name.Pos, desc.Name.Name))),
        node.Q(node.Y(node.Q("settings"), node.Q(settings))));
}

} // namespace

class TCreateTopicNode final: public TAstListNode {
public:
    TCreateTopicNode(TPosition pos, const TTopicRef& tr, TCreateTopicParameters params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic_(tr)
        , Params_(std::move(params))
        , Scoped_(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Topic_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        if (!Params_.Consumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params_.Consumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " defined more than once";
                    return false;
                }
            }
        }

        auto opts = Y();
        TString mode = Params_.ExistingOk ? "create_if_not_exists" : "create";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));

        for (const auto& consumer : Params_.Consumers) {
            const auto desc = CreateConsumerDesc(ctx, consumer, *this, /*alter=*/false);
            if (!desc) {
                return false;
            }
            opts = L(opts, Q(Y(Q("consumer"), Q(desc))));
        }

        if (Params_.TopicSettings.IsSet()) {
            auto settings = Y();

#define INSERT_TOPIC_SETTING(NAME)                                                            \
    if (const auto& NAME##Val = Params_.TopicSettings.NAME) {                                 \
        if (NAME##Val.IsSet()) {                                                              \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(set##NAME)), NAME##Val.GetValueSet()))); \
        } else {                                                                              \
            YQL_ENSURE(false, "Can't reset on create");                                       \
        }                                                                                     \
    }

            INSERT_TOPIC_SETTING(MaxPartitions)
            INSERT_TOPIC_SETTING(MinPartitions)
            INSERT_TOPIC_SETTING(RetentionPeriod)
            INSERT_TOPIC_SETTING(RetentionStorage)
            INSERT_TOPIC_SETTING(SupportedCodecs)
            INSERT_TOPIC_SETTING(PartitionWriteSpeed)
            INSERT_TOPIC_SETTING(PartitionWriteBurstSpeed)
            INSERT_TOPIC_SETTING(MeteringMode)
            INSERT_TOPIC_SETTING(AutoPartitioningStabilizationWindow)
            INSERT_TOPIC_SETTING(AutoPartitioningUpUtilizationPercent)
            INSERT_TOPIC_SETTING(AutoPartitioningDownUtilizationPercent)
            INSERT_TOPIC_SETTING(AutoPartitioningStrategy)
            INSERT_TOPIC_SETTING(MetricsLevel)
            INSERT_TOPIC_SETTING(ContentBasedDeduplication)

#undef INSERT_TOPIC_SETTING

            opts = L(opts, Q(Y(Q("topicSettings"), Q(settings))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, TString(KikimrProviderName)),
                                            Scoped_->WrapCluster(Topic_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TTopicRef Topic_;
    const TCreateTopicParameters Params_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildCreateTopic(
    TPosition pos, const TTopicRef& tr, const TCreateTopicParameters& params, TScopedStatePtr scoped) {
    return new TCreateTopicNode(pos, tr, params, scoped);
}

class TAlterTopicNode final: public TAstListNode {
public:
    TAlterTopicNode(TPosition pos, const TTopicRef& tr, TAlterTopicParameters params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic_(tr)
        , Params_(std::move(params))
        , Scoped_(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        auto keys = Topic_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::CREATE);
        if (!keys || !keys->Init(ctx, src)) {
            return false;
        }

        if (!Params_.AddConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params_.AddConsumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " defined more than once";
                    return false;
                }
            }
        }
        if (!Params_.AlterConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& [_, consumer] : Params_.AlterConsumers) {
                if (!consumerNames.insert(consumer.Name.Name).second) {
                    ctx.Error(consumer.Name.Pos) << "Consumer " << consumer.Name.Name << " altered more than once";
                    return false;
                }
            }
        }
        if (!Params_.DropConsumers.empty())
        {
            THashSet<TString> consumerNames;
            for (const auto& consumer : Params_.DropConsumers) {
                if (!consumerNames.insert(consumer.Name).second) {
                    ctx.Error(consumer.Pos) << "Consumer " << consumer.Name << " dropped more than once";
                    return false;
                }
            }
        }

        auto opts = Y();
        TString mode = Params_.MissingOk ? "alter_if_exists" : "alter";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));

        for (const auto& consumer : Params_.AddConsumers) {
            const auto desc = CreateConsumerDesc(ctx, consumer, *this, /*alter=*/false);
            if (!desc) {
                return false;
            }
            opts = L(opts, Q(Y(Q("addConsumer"), Q(desc))));
        }

        for (const auto& [_, consumer] : Params_.AlterConsumers) {
            const auto desc = CreateConsumerDesc(ctx, consumer, *this, /*alter=*/true);
            if (!desc) {
                return false;
            }
            opts = L(opts, Q(Y(Q("alterConsumer"), Q(desc))));
        }

        for (const auto& consumer : Params_.DropConsumers) {
            const auto name = BuildQuotedAtom(consumer.Pos, consumer.Name);
            opts = L(opts, Q(Y(Q("dropConsumer"), name)));
        }

        if (Params_.TopicSettings.IsSet()) {
            auto settings = Y();

#define INSERT_TOPIC_SETTING(NAME)                                                            \
    if (const auto& NAME##Val = Params_.TopicSettings.NAME) {                                 \
        if (NAME##Val.IsSet()) {                                                              \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(set##NAME)), NAME##Val.GetValueSet()))); \
        } else {                                                                              \
            settings = L(settings, Q(Y(Q(Y_STRINGIZE(reset##NAME)), Q(Y()))));                \
        }                                                                                     \
    }

            INSERT_TOPIC_SETTING(MaxPartitions)
            INSERT_TOPIC_SETTING(MinPartitions)
            INSERT_TOPIC_SETTING(RetentionPeriod)
            INSERT_TOPIC_SETTING(RetentionStorage)
            INSERT_TOPIC_SETTING(SupportedCodecs)
            INSERT_TOPIC_SETTING(PartitionWriteSpeed)
            INSERT_TOPIC_SETTING(PartitionWriteBurstSpeed)
            INSERT_TOPIC_SETTING(MeteringMode)
            INSERT_TOPIC_SETTING(AutoPartitioningStabilizationWindow)
            INSERT_TOPIC_SETTING(AutoPartitioningUpUtilizationPercent)
            INSERT_TOPIC_SETTING(AutoPartitioningDownUtilizationPercent)
            INSERT_TOPIC_SETTING(AutoPartitioningStrategy)
            INSERT_TOPIC_SETTING(MetricsLevel)
            INSERT_TOPIC_SETTING(ContentBasedDeduplication)

#undef INSERT_TOPIC_SETTING

            opts = L(opts, Q(Y(Q("topicSettings"), Q(settings))));
        }

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, TString(KikimrProviderName)),
                                            Scoped_->WrapCluster(Topic_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, src);
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    const TTopicRef Topic_;
    const TAlterTopicParameters Params_;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildAlterTopic(
    TPosition pos, const TTopicRef& tr, const TAlterTopicParameters& params, TScopedStatePtr scoped) {
    return new TAlterTopicNode(pos, tr, params, scoped);
}

class TDropTopicNode final: public TAstListNode {
public:
    TDropTopicNode(TPosition pos, const TTopicRef& tr, const TDropTopicParameters& params, TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Topic_(tr)
        , Params_(params)
        , Scoped_(scoped)
    {
        scoped->UseCluster(TString(KikimrProviderName), Topic_.Cluster);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        auto keys = Topic_.Keys->GetTableKeys()->BuildKeys(ctx, ITableKeys::EBuildKeysMode::DROP);
        if (!keys || !keys->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        auto opts = Y();

        TString mode = Params_.MissingOk ? "drop_if_exists" : "drop";
        opts = L(opts, Q(Y(Q("mode"), Q(mode))));

        Add("block", Q(Y(
                         Y("let", "sink", Y("DataSink", BuildQuotedAtom(Pos_, TString(KikimrProviderName)),
                                            Scoped_->WrapCluster(Topic_.Cluster, ctx))),
                         Y("let", "world", Y(TString(WriteName), "world", "sink", keys, Y("Void"), Q(opts))),
                         Y("return", ctx.PragmaAutoCommit ? Y(TString(CommitName), "world", "sink") : AstNode("world")))));

        return TAstListNode::DoInit(ctx, FakeSource_.Get());
    }

    TPtr DoClone() const final {
        return {};
    }

private:
    TTopicRef Topic_;
    TDropTopicParameters Params_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildDropTopic(TPosition pos, const TTopicRef& topic, const TDropTopicParameters& params, TScopedStatePtr scoped) {
    return new TDropTopicNode(pos, topic, params, scoped);
}

} // namespace NSQLTranslationV1
