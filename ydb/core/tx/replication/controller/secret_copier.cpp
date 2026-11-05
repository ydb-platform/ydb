#include "secret_copier.h"
#include "private_events.h"
#include "logging.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/replication/controller/protos/secret_copy.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/tx_proxy_status.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

namespace NSchemeEvents = NSchemeShard::TEvSchemeShard;

namespace {

TString ResolveSecretPath(const TString& path, const TString& database) {
    return CanonizePath(path.StartsWith("/") ? path : database + "/" + path);
}

}

bool DecodeSecretRecord(const TTopicMessage& message, NKikimrReplication::TSecretRecord& record) {
    return message.GetCodec() == NYdb::NTopic::ECodec::RAW
        && record.ParseFromString(message.GetData())
        && record.HasSourcePath() && !record.GetSourcePath().empty() && record.HasValue();
}

class TSecretCopier: public TActorBootstrapped<TSecretCopier> {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

public:
    TSecretCopier(const TActorId& parent, TReplication::TPtr replication)
        : Parent(parent)
        , Replication(std::move(replication))
        , Proxy(Replication->GetYdbProxy())
    {
        auto targets = Replication->GetTargets();
        SortBy(targets, [](const auto* target) { return target->GetId(); });
        const auto& sourceDatabase = Replication->GetConfig().GetSrcConnectionParams().GetDatabase();
        const auto& database = sourceDatabase.empty() ? Replication->GetDatabase() : sourceDatabase;
        for (const auto* target : targets) {
            if (target->GetKind() != TReplication::ETargetKind::Secret) {
                continue;
            }
            const auto sourcePath = ResolveSecretPath(target->GetSrcPath(), database);
            auto [it, inserted] = Targets.try_emplace(sourcePath);
            it->second.push_back(target->GetId());
            ++TargetCount;
            if (inserted) {
                Paths.push_back(sourcePath);
            }
            TopicPath = CanonizePath(target->GetStreamName());
            if (target->GetDstState() == TReplication::EDstState::Done) {
                Completed.insert(target->GetId());
            }
        }
    }

    void Bootstrap() {
        Become(&TThis::StateCopy);
        if (Targets.empty()) {
            return Finish(NYdb::EStatus::SUCCESS);
        }
        if (Completed.size() == TargetCount) {
            return DropTopic();
        }
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = Replication->GetDatabase();
        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = Replication->GetPathId();
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = TNavigate::OpPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (StopRequested) {
            return DropTopic();
        }
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != 1 || entries.front().Status != TNavigate::EStatus::Ok
            || !entries.front().SecurityObject)
        {
            return Finish(NYdb::EStatus::SCHEME_ERROR, "Cannot resolve secret replication owner");
        }
        Owner = entries.front().SecurityObject->GetOwnerSID();
        ExportInFlight = true;
        Send(Proxy, new TEvYdbProxy::TEvExportSecretsRequest(TopicPath, Paths,
            NYdb::NScheme::TDescribePathSettings()));
    }

    void Handle(TEvYdbProxy::TEvExportSecretsResponse::TPtr& ev) {
        ExportInFlight = false;
        if (StopRequested) {
            return DropTopic();
        }
        const auto& status = ev->Get()->Result;
        YDB_LOG_INFO("Source secret export completed", {"status", status.GetStatus()},
            {"issues", status.GetIssues().ToOneLineString()});
        if (!status.IsSuccess()) {
            return Finish(status.GetStatus(), TStringBuilder() << "Cannot export source secrets: "
                << status.GetIssues().ToOneLineString());
        }
        auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("replicationConsumer")
            .AutoCommit(false)
            .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path(TopicPath).AppendPartitionIds(0));
        ReaderCreating = true;
        Send(Proxy, new TEvYdbProxy::TEvCreateTopicReaderRequest(settings));
    }

    void Handle(TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
        ReaderCreating = false;
        Reader = ev->Get()->Result;
        if (StopRequested) {
            DropTopic();
        } else {
            ReadNext();
        }
    }

    void ReadNext() {
        Send(Reader, new TEvYdbProxy::TEvReadTopicRequest(TEvYdbProxy::TReadTopicSettings()));
    }

    void Handle(TEvYdbProxy::TEvReadTopicResponse::TPtr& ev) {
        if (StopRequested) {
            return DropTopic();
        }
        Records = std::move(ev->Get()->Result.Messages);
        YDB_LOG_INFO("Read initial secret records", {"count", Records.size()});
        NextRecord = 0;
        ApplyNext();
    }

    void ApplyNext() {
        while (true) {
            while (NextTarget < CurrentTargets.size()) {
                const auto targetId = CurrentTargets[NextTarget++];
                if (Completed.contains(targetId)) {
                    continue;
                }
                CurrentTarget = targetId;
                CreateInFlight = true;
                if (PipeCache) {
                    CreateSecret();
                } else {
                    Send(MakeTxProxyID(), new TEvTxUserProxy::TEvGetProxyServicesRequest());
                }
                return;
            }
            if (NextRecord == Records.size()) {
                break;
            }
            NKikimrReplication::TSecretRecord record;
            if (!DecodeSecretRecord(Records[NextRecord++], record)) {
                return Finish(NYdb::EStatus::BAD_REQUEST, "Malformed secret replication record");
            }
            const auto target = Targets.find(record.GetSourcePath());
            if (target == Targets.end()) {
                return Finish(NYdb::EStatus::BAD_REQUEST, "Unexpected secret replication source path");
            }
            CurrentTargets = target->second;
            NextTarget = 0;
            CurrentValue = record.GetValue();
        }
        if (Completed.size() == TargetCount) {
            DropTopic();
        } else {
            ReadNext();
        }
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev) {
        PipeCache = ev->Get()->Services.LeaderPipeCache;
        CreateSecret();
    }

    void CreateSecret() {
        if (StopRequested) {
            CreateInFlight = false;
            return DropTopic();
        }
        const auto* target = Replication->FindTarget(CurrentTarget);
        if (!target) {
            return Finish(NYdb::EStatus::CANCELLED, "Secret target removed during initial copy");
        }
        const auto prefix = Replication->GetDatabase() + "/";
        const auto destination = ResolveSecretPath(target->GetDstPath(), Replication->GetDatabase());
        if (!destination.StartsWith(prefix)) {
            return Finish(NYdb::EStatus::BAD_REQUEST, "Secret target must belong to the destination database");
        }
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName(Replication->GetDatabase());
        request->Record.SetUserToken(NACLib::TUserToken(Owner, TVector<NACLib::TSID>()).SerializeAsString());
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSecret);
        tx.SetWorkingDir(Replication->GetDatabase());
        tx.SetFailOnExist(true);
        tx.SetFailedOnAlreadyExists(true);
        auto& secret = *tx.MutableCreateSecret();
        secret.SetName(destination.substr(prefix.size()));
        secret.SetValue(CurrentValue);
        secret.SetInheritPermissions(false);
        Send(MakeTxProxyID(), request.Release());
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto code = YdbStatusFromProxyStatus(ev->Get());
        YDB_LOG_INFO("Destination secret creation status", {"status", code}, {"targetId", CurrentTarget});
        if (!IsTxProxyInProgress(code) && code != Ydb::StatusIds::SUCCESS) {
            return Finish(static_cast<NYdb::EStatus>(code), "Cannot create destination secret");
        }
        const auto& record = ev->Get()->Record;
        CurrentPathId = TPathId(record.GetSchemeShardTabletId(), record.GetPathId());
        if (code == Ydb::StatusIds::SUCCESS) {
            return SecretCreated();
        }
        Send(PipeCache, new TEvPipeCache::TEvForward(
            new NSchemeEvents::TEvNotifyTxCompletion(record.GetTxId()), Replication->GetSchemeShardId(), true));
    }

    void Handle(NSchemeEvents::TEvNotifyTxCompletionResult::TPtr&) {
        SecretCreated();
    }

    void SecretCreated() {
        CreateInFlight = false;
        Completed.insert(CurrentTarget);
        Created.emplace(CurrentTarget, CurrentPathId);
        if (StopRequested) {
            DropTopic();
        } else {
            ApplyNext();
        }
    }

    void DropTopic() {
        if (DroppingTopic) {
            return;
        }
        DroppingTopic = true;
        if (Reader) {
            Send(std::exchange(Reader, {}), new TEvents::TEvPoison());
        }
        Send(Proxy, new TEvYdbProxy::TEvDropTopicRequest(TopicPath, NYdb::NTopic::TDropTopicSettings()));
    }

    void Handle(TEvYdbProxy::TEvDropTopicResponse::TPtr& ev) {
        const auto& status = ev->Get()->Result;
        if (!status.IsSuccess() && status.GetStatus() != NYdb::EStatus::SCHEME_ERROR) {
            return Finish(status.GetStatus(), "Cannot remove secret replication topic");
        }
        Finish(NYdb::EStatus::SUCCESS);
    }

    void Handle(TEvYdbProxy::TEvTopicReaderGone::TPtr&) {
        if (!DroppingTopic) {
            Finish(NYdb::EStatus::UNAVAILABLE, "Secret replication reader stopped");
        }
    }

    void Handle(TEvPrivate::TEvCancelSecretCopy::TPtr&) {
        StopRequested = true;
        if (!ExportInFlight && !CreateInFlight && !ReaderCreating) {
            DropTopic();
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        Finish(NYdb::EStatus::UNAVAILABLE, "Destination SchemeShard is unavailable");
    }

    void Finish(NYdb::EStatus code, const TString& issue = {}) {
        YDB_LOG_INFO("Initial secret copy completed", {"status", code}, {"issue", issue},
            {"created", Created.size()});
        NYdb::NIssue::TIssues issues;
        if (issue) {
            issues.AddIssue(issue);
        }
        Send(Parent, new TEvPrivate::TEvSecretsCopied(Replication->GetId(),
            NYdb::TStatus(code, std::move(issues)), std::move(Created)));
        PassAway();
    }

    void PassAway() override {
        if (Reader) {
            Send(Reader, new TEvents::TEvPoison());
        }
        TActorBootstrapped::PassAway();
    }

    STATEFN(StateCopy) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvPrivate::TEvCancelSecretCopy, Handle);
            hFunc(TEvYdbProxy::TEvExportSecretsResponse, Handle);
            hFunc(TEvYdbProxy::TEvCreateTopicReaderResponse, Handle);
            hFunc(TEvYdbProxy::TEvReadTopicResponse, Handle);
            hFunc(TEvYdbProxy::TEvDropTopicResponse, Handle);
            hFunc(TEvYdbProxy::TEvTopicReaderGone, Handle);
            hFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, Handle);
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(NSchemeEvents::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(NSchemeEvents::TEvNotifyTxCompletionRegistered);
            IgnoreFunc(TEvYdbProxy::TEvStartTopicReadingSession);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    const TActorId Parent;
    const TReplication::TPtr Replication;
    const TActorId Proxy;
    TString TopicPath;
    TString Owner;
    TString CurrentValue;
    std::vector<std::string> Paths;
    THashMap<TString, TVector<ui64>> Targets;
    THashSet<ui64> Completed;
    THashMap<ui64, TPathId> Created;
    size_t TargetCount = 0;
    TVector<ui64> CurrentTargets;
    size_t NextTarget = 0;
    TVector<TTopicMessage> Records;
    size_t NextRecord = 0;
    ui64 CurrentTarget = 0;
    TPathId CurrentPathId;
    TActorId Reader;
    TActorId PipeCache;
    bool StopRequested = false;
    bool ExportInFlight = false;
    bool CreateInFlight = false;
    bool ReaderCreating = false;
    bool DroppingTopic = false;
};

IActor* CreateSecretCopier(const TActorId& parent, TReplication::TPtr replication) {
    return new TSecretCopier(parent, std::move(replication));
}

}
