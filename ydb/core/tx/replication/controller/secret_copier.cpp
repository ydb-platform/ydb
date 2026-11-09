#include "secret_copier.h"
#include "private_events.h"
#include "logging.h"
#include "target_secret.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/algorithm.h>

#include <optional>
#include <utility>

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

    enum class EPhase {
        ResolvingOwner,
        CreatingReader,
        Reading,
        ResolvingServices,
        AllocatingTxId,
        PreparingSecret,
        ProposingSecret,
        WaitingSecret,
        DescribingSecret,
        SavingProgress,
        CommittingOffset,
        RetryingTransport,
    };

    struct TDestination {
        TString Path;
        ui64 CreateTxId;
    };

public:
    TSecretCopier(const TActorId& parent, const TReplication& replication, bool recoverOnly)
        : Parent(parent)
        , ReplicationId(replication.GetId())
        , ReplicationPathId(replication.GetPathId())
        , Database(replication.GetDatabase())
        , SchemeShardId(replication.GetSchemeShardId())
        , Proxy(replication.GetYdbProxy())
        , RecoverOnly(recoverOnly)
    {
        auto targets = replication.GetTargets();
        SortBy(targets, [](const auto* target) { return target->GetId(); });
        const auto& sourceDatabase = replication.GetConfig().GetSrcConnectionParams().GetDatabase();
        const auto& database = sourceDatabase.empty() ? Database : sourceDatabase;
        for (const auto* target : targets) {
            if (target->GetKind() != TReplication::ETargetKind::Secret) {
                continue;
            }
            const auto txId = static_cast<const TTargetSecret*>(target)->GetCreateTxId();
            if (RecoverOnly && (!txId || target->GetDstPathId())) {
                continue;
            }
            Destinations.emplace(target->GetId(), TDestination{
                ResolveSecretPath(target->GetDstPath(), Database), txId});
            const auto sourcePath = ResolveSecretPath(target->GetSrcPath(), database);
            Targets[sourcePath].push_back(target->GetId());
            ++TargetCount;
            if (target->GetDstState() == TReplication::EDstState::Done || target->GetDstPathId()) {
                Completed.insert(target->GetId());
            } else {
                const auto topicPath = target->GetStreamPath();
                if (TopicPath.empty()) {
                    TopicPath = topicPath;
                } else if (TopicPath != topicPath) {
                    InconsistentTopicPaths = true;
                }
            }
        }
    }

    void Bootstrap() {
        Become(&TThis::StateCopy);
        if (Targets.empty()) {
            return Finish(NYdb::EStatus::SUCCESS);
        }
        if (Completed.size() == TargetCount) {
            return Finish(NYdb::EStatus::SUCCESS);
        }
        if (InconsistentTopicPaths) {
            return Finish(NYdb::EStatus::BAD_REQUEST, "Secret targets must use a shared replication topic");
        }
        ResolveOwner();
    }

private:
    void ResolveOwner() {
        Phase = EPhase::ResolvingOwner;
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = Database;
        auto& entry = request->ResultSet.emplace_back();
        entry.TableId = ReplicationPathId;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = TNavigate::OpPath;
        entry.SyncVersion = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (Phase != EPhase::ResolvingOwner) {
            return;
        }
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != 1 || entries.front().Status != TNavigate::EStatus::Ok
            || !entries.front().SecurityObject)
        {
            return Finish(NYdb::EStatus::SCHEME_ERROR, "Cannot resolve secret replication owner");
        }
        Owner = entries.front().SecurityObject->GetOwnerSID();
        CreateReader();
    }

    void CreateReader() {
        auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("replicationConsumer")
            .AutoCommit(false)
            .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path(TopicPath).AppendPartitionIds(0));
        Phase = EPhase::CreatingReader;
        Send(Proxy, new TEvYdbProxy::TEvCreateTopicReaderRequest(settings));
    }

    void Handle(TEvYdbProxy::TEvCreateTopicReaderResponse::TPtr& ev) {
        if (Phase != EPhase::CreatingReader) {
            return;
        }
        Reader = ev->Get()->Result;
        Phase = EPhase::Reading;
        if (StopStatus) {
            Finish(StopStatus->GetStatus(), StopStatus->GetIssues().ToOneLineString());
        } else {
            ReadNext();
        }
    }

    void ReadNext() {
        Phase = EPhase::Reading;
        Send(Reader, new TEvYdbProxy::TEvReadTopicRequest(TEvYdbProxy::TReadTopicSettings()));
    }

    void Handle(TEvYdbProxy::TEvStartTopicReadingSession::TPtr& ev) {
        ReadSessionId = ev->Get()->Result.ReadSessionId;
    }

    void Handle(TEvYdbProxy::TEvReadTopicResponse::TPtr& ev) {
        if (Phase != EPhase::Reading) {
            return;
        }
        PartitionId = ev->Get()->Result.PartitionId;
        Records = std::move(ev->Get()->Result.Messages);
        if (Records.empty()) {
            return ReadNext();
        }
        NextOffset = Records.back().GetOffset() + 1;
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
                CurrentTxId = Destinations.at(targetId).CreateTxId;
                CreateMayExist = CurrentTxId != 0;
                CreateSent = false;
                if (!CurrentTxId) {
                    Phase = EPhase::AllocatingTxId;
                    Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
                } else if (PipeCache) {
                    CreateSecret();
                } else {
                    Phase = EPhase::ResolvingServices;
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
                if (RecoverOnly) {
                    continue;
                }
                return Finish(NYdb::EStatus::BAD_REQUEST, "Unexpected secret replication source path");
            }
            CurrentTargets = target->second;
            NextTarget = 0;
            CurrentValue = record.GetValue();
        }
        if (RecoverOnly) {
            // Recovery may skip records for targets whose CREATE never began.
            // Leave their source offsets untouched for a later normal resume.
            if (Completed.size() == TargetCount) {
                return Finish(NYdb::EStatus::SUCCESS);
            }
            return ReadNext();
        }
        // The next source offset must never hide destination identities that
        // have only been recorded in this actor's memory.
        Phase = EPhase::SavingProgress;
        Send(Parent, new TEvPrivate::TEvSecretsCopied(ReplicationId,
            NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), Created, false, Rejected), 0, NextOffset);
    }

    void Handle(TEvPrivate::TEvSecretsCopiedSaved::TPtr& ev) {
        if (ev->Sender != Parent || Phase != EPhase::SavingProgress || ev->Cookie != NextOffset) {
            return;
        }
        Phase = EPhase::CommittingOffset;
        Send(Proxy, new TEvYdbProxy::TEvCommitOffsetRequest(TopicPath, PartitionId,
            "replicationConsumer", NextOffset, NYdb::NTopic::TCommitOffsetSettings().ReadSessionId(ReadSessionId)));
    }

    void Handle(TEvYdbProxy::TEvCommitOffsetResponse::TPtr& ev) {
        if (Phase != EPhase::CommittingOffset) {
            return;
        }
        const auto& status = ev->Get()->Result;
        if (!status.IsSuccess()) {
            return Finish(status.GetStatus(), "Cannot commit copied secret records");
        }
        Send(Reader, new TEvYdbProxy::TEvCommitOffsetRequest(TopicPath, PartitionId,
            "replicationConsumer", NextOffset, NYdb::NTopic::TCommitOffsetSettings().ReadSessionId(ReadSessionId)));
        if (Completed.size() == TargetCount) {
            return Finish(NYdb::EStatus::SUCCESS);
        }
        ReadNext();
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev) {
        if (Phase != EPhase::ResolvingServices) {
            return;
        }
        PipeCache = ev->Get()->Services.LeaderPipeCache;
        CreateSecret();
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        if (Phase != EPhase::AllocatingTxId) {
            return;
        }
        CurrentTxId = ev->Get()->TxId;
        PipeCache = ev->Get()->Services.LeaderPipeCache;
        Phase = EPhase::PreparingSecret;
        Send(Parent, new TEvPrivate::TEvPrepareSecretCreate(ReplicationId, CurrentTarget, CurrentTxId));
    }

    void Handle(TEvPrivate::TEvSecretCreatePrepared::TPtr& ev) {
        if (ev->Sender != Parent || Phase != EPhase::PreparingSecret || ev->Cookie != CurrentTarget) {
            return;
        }
        CreateMayExist |= ev->Get()->TxId != CurrentTxId;
        CurrentTxId = ev->Get()->TxId;
        Destinations.at(CurrentTarget).CreateTxId = CurrentTxId;
        // Once the intent is durable, settle it even if PAUSE/DROP arrived.
        CreateSecret();
    }

    void CreateSecret() {
        const auto prefix = Database + "/";
        const auto& destination = Destinations.at(CurrentTarget).Path;
        if (!destination.StartsWith(prefix)) {
            return RejectSecret(NYdb::EStatus::BAD_REQUEST, "Secret target must belong to the destination database");
        }
        auto request = MakeHolder<NSchemeEvents::TEvModifySchemeTransaction>(CurrentTxId, SchemeShardId);
        request->Record.SetOwner(Owner);
        auto& tx = *request->Record.AddTransaction();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSecret);
        tx.SetWorkingDir(Database);
        // Return the existing object's creation identity without replacing it.
        tx.SetFailOnExist(false);
        auto& secret = *tx.MutableCreateSecret();
        secret.SetName(destination.substr(prefix.size()));
        secret.SetValue(CurrentValue);
        secret.SetInheritPermissions(false);
        CreateMayExist |= CreateSent;
        CreateSent = true;
        CreateRejected = false;
        Phase = EPhase::ProposingSecret;
        Send(PipeCache, new TEvPipeCache::TEvForward(request.Release(), SchemeShardId, true, CurrentTxId));
    }

    void Handle(NSchemeEvents::TEvModifySchemeTransactionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (Phase != EPhase::ProposingSecret || record.GetTxId() != CurrentTxId) {
            return;
        }
        YDB_LOG_INFO("Destination secret creation status", {"status", record.GetStatus()}, {"targetId", CurrentTarget});
        switch (record.GetStatus()) {
        case NKikimrScheme::StatusAccepted:
            Phase = EPhase::WaitingSecret;
            return SubscribeTx();
        case NKikimrScheme::StatusAlreadyExists:
        case NKikimrScheme::StatusMultipleModifications:
            if (record.GetPathCreateTxId() != CurrentTxId) {
                CreateRejected = true;
                return DescribeSecret();
            }
            Phase = EPhase::WaitingSecret;
            return SubscribeTx();
        default:
            // Quotas and other checks can reject a retry before SchemeShard
            // examines the existing path. Reconcile the original request first.
            CreateRejected = true;
            return DescribeSecret();
        }
    }

    void SubscribeTx() {
        Send(PipeCache, new TEvPipeCache::TEvForward(
            new NSchemeEvents::TEvNotifyTxCompletion(CurrentTxId), SchemeShardId, true, CurrentTxId));
    }

    void Handle(NSchemeEvents::TEvNotifyTxCompletionResult::TPtr& ev) {
        if (Phase != EPhase::WaitingSecret || ev->Get()->Record.GetTxId() != CurrentTxId) {
            return;
        }
        DescribeSecret();
    }

    void DescribeSecret() {
        Phase = EPhase::DescribingSecret;
        // Metadata only: values are read exclusively from the source topic.
        Send(PipeCache, new TEvPipeCache::TEvForward(
            new NSchemeEvents::TEvDescribeScheme(Destinations.at(CurrentTarget).Path),
            SchemeShardId, true, CurrentTxId), 0, CurrentTxId);
    }

    void Handle(NSchemeEvents::TEvDescribeSchemeResult::TPtr& ev) {
        if (Phase != EPhase::DescribingSecret || ev->Cookie != CurrentTxId) {
            return;
        }
        const auto& record = ev->Get()->GetRecord();
        if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
            if (CreateRejected && !CreateMayExist && record.GetStatus() == NKikimrScheme::StatusPathDoesNotExist) {
                // The first, unambiguously rejected proposal cannot own an
                // object. Unlike a restored/retried intent it may be discarded.
                return RejectSecret(NYdb::EStatus::SCHEME_ERROR, "Cannot create destination secret");
            }
            if (CreateRejected && (!RecoverOnly || ReconnectRequested)) {
                Phase = EPhase::Reading;
                return Finish(NYdb::EStatus::SCHEME_ERROR, "Cannot reconcile destination secret creation");
            }
            // A notification alone does not prove that CREATE was applied.
            // Keep the intent and replay the same request after a transient loss.
            return Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup(CurrentTxId));
        }
        const auto& self = record.GetPathDescription().GetSelf();
        if (self.GetPathType() != NKikimrSchemeOp::EPathTypeSecret || self.GetCreateTxId() != CurrentTxId) {
            return RejectSecret(NYdb::EStatus::SCHEME_ERROR, "Destination secret belongs to another creation operation");
        }
        if (!self.GetCreateFinished()) {
            Phase = EPhase::WaitingSecret;
            return SubscribeTx();
        }
        CurrentPathId = TPathId(record.GetPathOwnerId(), record.GetPathId());
        SecretCreated();
    }

    void RejectSecret(NYdb::EStatus status, const TString& issue) {
        Rejected.insert(CurrentTarget);
        Phase = EPhase::Reading;
        if (PipeCache && CurrentTxId) {
            Send(PipeCache, new TEvPipeCache::TEvUnlink(SchemeShardId));
        }
        CurrentTxId = 0;
        if (RecoverOnly) {
            // A definite rejection owns no destination. Cleanup may continue.
            Completed.insert(CurrentTarget);
            ApplyNext();
        } else {
            Finish(status, issue);
        }
    }

    void SecretCreated() {
        if (CurrentTxId) {
            Send(PipeCache, new TEvPipeCache::TEvUnlink(SchemeShardId));
        }
        Phase = EPhase::Reading;
        CurrentTxId = 0;
        Completed.insert(CurrentTarget);
        Created.emplace(CurrentTarget, CurrentPathId);
        if (StopStatus) {
            Finish(StopStatus->GetStatus(), StopStatus->GetIssues().ToOneLineString());
        } else {
            ApplyNext();
        }
    }

    void Handle(TEvYdbProxy::TEvTopicReaderGone::TPtr&) {
        Finish(NYdb::EStatus::UNAVAILABLE, "Secret replication reader stopped");
    }

    void Handle(TEvPrivate::TEvCancelSecretCopy::TPtr& ev) {
        if (!RecoverOnly || ev->Get()->Reconnect) {
            ReconnectRequested = ev->Get()->Reconnect;
            Finish(NYdb::EStatus::SUCCESS);
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if ((Phase != EPhase::ProposingSecret && Phase != EPhase::WaitingSecret && Phase != EPhase::DescribingSecret)
            || ev->Get()->TabletId != SchemeShardId
            || ev->Cookie != CurrentTxId)
        {
            return;
        }
        // A prepared request may have been accepted. Keep its identity across
        // delivery failures instead of abandoning an unowned destination.
        CreateMayExist = true;
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup(CurrentTxId));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (Phase == EPhase::RetryingTransport && ev->Get()->Tag == 0) {
            return Owner.empty() ? ResolveOwner() : CreateReader();
        }
        if (ev->Get()->Tag != CurrentTxId) {
            return;
        }
        if (Phase == EPhase::WaitingSecret) {
            SubscribeTx();
        } else if (Phase == EPhase::ProposingSecret || Phase == EPhase::DescribingSecret) {
            CreateSecret();
        }
    }

    void Finish(NYdb::EStatus code, const TString& issue = {}) {
        if (!StopStatus || StopStatus->IsSuccess()) {
            NYdb::NIssue::TIssues issues;
            if (issue) {
                issues.AddIssue(issue);
            }
            StopStatus.emplace(code, std::move(issues));
        }
        if (Phase == EPhase::CreatingReader || Phase == EPhase::PreparingSecret
            || Phase == EPhase::ProposingSecret || Phase == EPhase::WaitingSecret || Phase == EPhase::DescribingSecret)
        {
            return;
        }
        code = StopStatus->GetStatus();
        if (RecoverOnly && !StopStatus->IsSuccess()) {
            if (!ReconnectRequested && Completed.size() != TargetCount) {
                // Keep unresolved intents, but avoid a controller restart loop
                // when the source connection or owner lookup is unavailable.
                if (Reader) {
                    Send(std::exchange(Reader, {}), new TEvents::TEvPoison());
                }
                ReadSessionId.clear();
                Records.clear();
                CurrentTargets.clear();
                CurrentValue.clear();
                NextRecord = NextTarget = 0;
                StopStatus.reset();
                Phase = EPhase::RetryingTransport;
                return Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
            }
            StopStatus.emplace(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues{});
            code = NYdb::EStatus::SUCCESS;
        }
        YDB_LOG_INFO("Initial secret copy completed", {"status", code}, {"issue", issue},
            {"created", Created.size()});
        Send(Parent, new TEvPrivate::TEvSecretsCopied(ReplicationId,
            std::move(*StopStatus), std::move(Created), true, std::move(Rejected)));
        PassAway();
    }

    void PassAway() override {
        if (Reader) {
            Send(Reader, new TEvents::TEvPoison());
        }
        if (PipeCache && CurrentTxId) {
            Send(PipeCache, new TEvPipeCache::TEvUnlink(SchemeShardId));
        }
        TActorBootstrapped::PassAway();
    }

    STATEFN(StateCopy) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvPrivate::TEvCancelSecretCopy, Handle);
            hFunc(TEvPrivate::TEvSecretsCopiedSaved, Handle);
            hFunc(TEvPrivate::TEvSecretCreatePrepared, Handle);
            hFunc(TEvYdbProxy::TEvCreateTopicReaderResponse, Handle);
            hFunc(TEvYdbProxy::TEvStartTopicReadingSession, Handle);
            hFunc(TEvYdbProxy::TEvReadTopicResponse, Handle);
            hFunc(TEvYdbProxy::TEvCommitOffsetResponse, Handle);
            hFunc(TEvYdbProxy::TEvTopicReaderGone, Handle);
            hFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, Handle);
            hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
            hFunc(NSchemeEvents::TEvModifySchemeTransactionResult, Handle);
            hFunc(NSchemeEvents::TEvDescribeSchemeResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            hFunc(NSchemeEvents::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(NSchemeEvents::TEvNotifyTxCompletionRegistered);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    const TActorId Parent;
    const ui64 ReplicationId;
    const TPathId ReplicationPathId;
    const TString Database;
    const ui64 SchemeShardId;
    const TActorId Proxy;
    const bool RecoverOnly;
    TString TopicPath;
    TString ReadSessionId;
    TString Owner;
    TString CurrentValue;
    THashMap<TString, TVector<ui64>> Targets;
    THashMap<ui64, TDestination> Destinations;
    THashSet<ui64> Completed;
    THashMap<ui64, TPathId> Created;
    THashSet<ui64> Rejected;
    size_t TargetCount = 0;
    TVector<ui64> CurrentTargets;
    size_t NextTarget = 0;
    TVector<TTopicMessage> Records;
    size_t NextRecord = 0;
    ui64 PartitionId = 0;
    ui64 NextOffset = 0;
    ui64 CurrentTarget = 0;
    ui64 CurrentTxId = 0;
    TPathId CurrentPathId;
    TActorId Reader;
    TActorId PipeCache;
    EPhase Phase = EPhase::ResolvingOwner;
    std::optional<NYdb::TStatus> StopStatus;
    bool InconsistentTopicPaths = false;
    bool CreateRejected = false;
    bool CreateMayExist = false;
    bool CreateSent = false;
    bool ReconnectRequested = false;
};

IActor* CreateSecretCopier(const TActorId& parent, const TReplication& replication, bool recoverOnly) {
    return new TSecretCopier(parent, replication, recoverOnly);
}

}
