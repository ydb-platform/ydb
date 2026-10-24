#include "logging.h"
#include "private_events.h"
#include "stream_creator.h"
#include "target_with_stream.h"
#include "util.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/json/json_writer.h>

#include <util/string/cast.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TStreamCreator: public TActorBootstrapped<TStreamCreator> {
    static constexpr TDuration RetryDelay = TDuration::Seconds(10);

    static NYdb::NTable::TChangefeedDescription MakeChangefeed(
            const TString& name,
            const TDuration& retentionPeriod,
            const std::optional<TDuration>& resolvedTimestamps,
            const NJson::TJsonMap& attrs,
            bool schemaChanges,
            bool initialScan)
    {
        using namespace NYdb::NTable;

        auto desc = TChangefeedDescription(name, EChangefeedMode::Updates, EChangefeedFormat::Json)
            .WithRetentionPeriod(retentionPeriod)
            .AddAttribute("__async_replication", NJson::WriteJson(attrs, false));

        if (initialScan) {
            desc.WithInitialScan();
        }

        if (schemaChanges) {
            desc.WithSchemaChanges();
        }

        if (resolvedTimestamps) {
            desc
                .WithVirtualTimestamps()
                .WithResolvedTimestamps(*resolvedTimestamps);
        }

        return desc;
    }

    void RequestPermission() {
        Send(Parent, new TEvPrivate::TEvRequestCreateStream());
        Become(&TThis::StateRequestPermission);
    }

    STATEFN(StateRequestPermission) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateRequestPermission"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvAllowCreateStream, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvAllowCreateStream::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        CreateStream();
    }

    void CreateStream() {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
        case TReplication::ETargetKind::IndexTable:
            Send(YdbProxy, new TEvYdbProxy::TEvAlterTableRequest(SrcPath, NYdb::NTable::TAlterTableSettings()
                .AppendAddChangefeeds(Changefeed)),
                0,
                ProxyGeneration);
            break;
        case TReplication::ETargetKind::Transfer:
            return CreateConsumer();
        }

        Become(&TThis::StateCreateStream);
    }

    STATEFN(StateCreateStream) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateCreateStream"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvAlterTableResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvYdbProxy::TEvAlterTableResponse::TPtr& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        auto& result = ev->Get()->Result;

        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                YDB_LOG_DEBUG("Retry CreateStream");
                return Retry();
            }

            YDB_LOG_ERROR("Error",
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()});
            return Reply(std::move(result));
        } else {
            YDB_LOG_INFO("Success",
                {"issues", result.GetIssues().ToOneLineString()});
            return CreateConsumer();
        }
    }

    TString BuildStreamPath() const {
        switch (Kind) {
        case TReplication::ETargetKind::Table:
            return CanonizePath(ChildPath(SplitPath(SrcPath), TString{Changefeed.GetName()}));
        case TReplication::ETargetKind::IndexTable:
            return CanonizePath(ChildPath(SplitPath(SrcPath), {"indexImplTable", TString{Changefeed.GetName()}}));
        case TReplication::ETargetKind::Transfer:
            return CanonizePath(SplitPath(SrcPath));
        }
    }

    void CreateConsumer() {
        const auto streamPath = BuildStreamPath();
        const auto settings = NYdb::NTopic::TAlterTopicSettings()
            .BeginAddConsumer()
                .ConsumerName(SrcConsumerName)
            .EndAddConsumer();

        Send(YdbProxy, new TEvYdbProxy::TEvAlterTopicRequest(streamPath, settings), 0, ProxyGeneration);
        Become(&TThis::StateCreateConsumer);
    }

    STATEFN(StateCreateConsumer) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateCreateConsumer"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvAlterTopicResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvYdbProxy::TEvAlterTopicResponse::TPtr& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});
        auto& result = ev->Get()->Result;

        if (result.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
            return Reply(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()));
        }

        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                YDB_LOG_DEBUG("Retry CreateConsumer");
                return Retry();
            }

            YDB_LOG_ERROR("Error",
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()});
        } else {
            YDB_LOG_INFO("Success",
                {"issues", result.GetIssues().ToOneLineString()});
        }

        Reply(std::move(result));
    }

    void CheckConsumerExists() {
        const auto streamPath = BuildStreamPath();
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTopicRequest(streamPath, {}), 0, ProxyGeneration);
        Become(&TThis::StateCheckConsumerExists);
    }

    STATEFN(StateCheckConsumerExists) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateCheckConsumerExists"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                YDB_LOG_WARN("Error of resolving topic",
                    {"streamPath", BuildStreamPath()},
                    {"ev", ev->Get()->ToString()},
                    {"outcome", "retry"});
                return Retry();
            }

            YDB_LOG_ERROR("Error of resolving topic",
                {"streamPath", BuildStreamPath()},
                {"ev", ev->Get()->ToString()},
                {"outcome", "stop"});
            NYdb::NIssue::TIssues issues = result.GetIssues();
            return Reply(NYdb::TStatus(result.GetStatus(), std::move(issues)));
        }

        bool exists = AnyOf(result.GetTopicDescription().GetConsumers(), [&](const auto& consumer) {
            return consumer.GetConsumerName() == SrcConsumerName;
        });

        if (exists) {
            Reply(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()));
        } else {
            NYdb::NIssue::TIssues issues;
            issues.AddIssue(TStringBuilder() << "consumer '" << SrcConsumerName << "' does not exist");
            Reply(NYdb::TStatus(NYdb::EStatus::SCHEME_ERROR, std::move(issues)));
        }
    }

    void Reply(NYdb::TStatus&& status) {
        if (status.IsSuccess() && Kind == TReplication::ETargetKind::Table) {
            return DescribeStreamTable();
        }

        Finish(std::move(status));
    }

    void DescribeStreamTable() {
        // Creation can replay an existing stream after the feature flag has
        // changed. Record its actual capability, not the requested setting.
        Send(
            YdbProxy,
            new TEvYdbProxy::TEvDescribeTableRequest(SrcPath, NYdb::NTable::TDescribeTableSettings()),
            0,
            ProxyGeneration);
        Become(&TThis::StateDescribeStreamTable);
    }

    STATEFN(StateDescribeStreamTable) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        if (ev->Cookie != ProxyGeneration) {
            return;
        }
        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                return Retry();
            }

            auto issues = result.GetIssues();
            return Finish(NYdb::TStatus(result.GetStatus(), std::move(issues)));
        }

        for (const auto& stream : result.GetTableDescription().GetChangefeedDescriptions()) {
            if (stream.GetName() == Changefeed.GetName()) {
                return Finish(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()), stream.GetSchemaChanges());
            }
        }
        NYdb::NIssue::TIssues issues;
        issues.AddIssue(TStringBuilder() << "changefeed '" << Changefeed.GetName() << "' does not exist");
        Finish(NYdb::TStatus(NYdb::EStatus::SCHEME_ERROR, std::move(issues)));
    }

    void Retry() {
        Schedule(RetryDelay, new TEvents::TEvWakeup(ProxyGeneration));
    }

    void RetryCurrentOperation() {
        const auto state = CurrentStateFunc();
        if (state == &TThis::StateCreateStream) {
            CreateStream();
        } else if (state == &TThis::StateCreateConsumer) {
            CreateConsumer();
        } else if (state == &TThis::StateCheckConsumerExists) {
            CheckConsumerExists();
        } else if (state == &TThis::StateDescribeStreamTable) {
            DescribeStreamTable();
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == ProxyGeneration) {
            RetryCurrentOperation();
        }
    }

    void Handle(TEvPrivate::TEvUpdateYdbProxy::TPtr& ev) {
        if (YdbProxy != ev->Get()->YdbProxy) {
            YdbProxy = ev->Get()->YdbProxy;
            ++ProxyGeneration;
            // Keep the controller's existing scheme-operation permission. Only
            // replay the pending source operation, which may have committed.
            RetryCurrentOperation();
        }
    }

    void Finish(NYdb::TStatus&& status, bool schemaChanges = false) {
        auto result = MakeHolder<TEvPrivate::TEvCreateStreamResult>(ReplicationId, TargetId, std::move(status));
        result->SchemaChanges = schemaChanges;
        Send(Parent, result.Release());
        PassAway();
    }

    void Cancel() {
        // Cancellation only releases the limiter; it must not change the
        // persisted stream state as a successful or failed creation would.
        Send(Parent, new TEvPrivate::TEvCancelCreateStream());
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_STREAM_CREATOR;
    }

    explicit TStreamCreator(
            const TActorId& parent,
            const TActorId& proxy,
            ui64 rid,
            ui64 tid,
            const TReplication::ITarget::IConfig::TPtr& config,
            const TString& streamName,
            const TString& consumerName,
            const TDuration& retentionPeriod,
            const std::optional<TDuration>& resolvedTimestamps,
            bool supportsTopicAutopartitioning,
            bool schemaChanges,
            bool needCreate,
            bool initialScan,
            bool describeOnly)
        : Parent(parent)
        , YdbProxy(proxy)
        , ReplicationId(rid)
        , TargetId(tid)
        , Kind(config->GetKind())
        , SrcPath(config->GetSrcPath())
        , SrcConsumerName(consumerName)
        , Changefeed(MakeChangefeed(streamName, retentionPeriod, resolvedTimestamps, NJson::TJsonMap{
            {"path", config->GetDstPath()},
            {"id", ToString(rid)},
            // A schema barrier snapshots its partition membership, so topic
            // repartitioning is unsupported for schema-aware streams.
            {"supports_topic_autopartitioning", supportsTopicAutopartitioning && !schemaChanges},
        }, schemaChanges, initialScan))
        , NeedCreate(needCreate)
        , DescribeOnly(describeOnly)
        , LogPrefix(CreateActorLogPrefix("StreamCreator", ReplicationId, TargetId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        if (DescribeOnly) {
            DescribeStreamTable();
        } else if (NeedCreate) {
            RequestPermission();
        } else {
            CheckConsumerExists();
        }
    }

    STATEFN(StateBase) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateBase"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvUpdateYdbProxy, Handle);
            sFunc(TEvents::TEvPoison, Cancel);
        }
    }

private:
    const TActorId Parent;
    TActorId YdbProxy;
    ui64 ProxyGeneration = 0;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TReplication::ETargetKind Kind;
    const TString SrcPath;
    const TString SrcConsumerName;
    const NYdb::NTable::TChangefeedDescription Changefeed;
    const bool NeedCreate;
    const bool DescribeOnly;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

}; // TStreamCreator

IActor* CreateStreamCreator(TReplication* replication, ui64 targetId, const TActorContext& ctx, bool describeOnly) {
    const auto* target = replication->FindTarget(targetId);
    Y_ABORT_UNLESS(target);

    const auto& config = replication->GetConfig();
    const auto& consistency = config.GetConsistencySettings();
    const auto resolvedTimestamps = consistency.HasGlobal()
        ? std::make_optional(TDuration::MilliSeconds(consistency.GetGlobal().GetCommitIntervalMilliSeconds()))
        : std::nullopt;
    const bool needCreate = !config.HasTransferSpecific() || !config.GetTransferSpecific().GetTarget().HasConsumerName();
    const bool supportsTopicAutopartitioning = !consistency.HasGlobal() && AppData()->FeatureFlags.GetEnableTopicAutopartitioningForReplication();
    const bool schemaChanges = AppData()->FeatureFlags.GetEnableAsyncReplicationSchemaChanges();
    const bool initialScan = config.GetInitialScan() || target->IsIndexBuild();

    return CreateStreamCreator(ctx.SelfID, replication->GetYdbProxy(),
        replication->GetId(), target->GetId(),
        target->GetConfig(), target->GetStreamName(), target->GetStreamConsumerName(),
        TDuration::Seconds(AppData()->ReplicationConfig.GetRetentionPeriodSeconds()), resolvedTimestamps,
        supportsTopicAutopartitioning, needCreate, schemaChanges, initialScan, describeOnly);
}

IActor* CreateStreamCreator(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
        const TReplication::ITarget::IConfig::TPtr& config,
        const TString& streamName, const TString& consumerName, const TDuration& retentionPeriod,
        const std::optional<TDuration>& resolvedTimestamps,
        bool supportsTopicAutopartitioning, bool needCreate, bool schemaChanges, bool initialScan, bool describeOnly)
{
    return new TStreamCreator(parent, proxy, rid, tid, config,
        streamName, consumerName, retentionPeriod, resolvedTimestamps, supportsTopicAutopartitioning,
        schemaChanges, needCreate, initialScan, describeOnly);
}

}
