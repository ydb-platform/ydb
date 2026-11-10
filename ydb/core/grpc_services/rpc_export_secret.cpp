#include "service_secret.h"
#include "rpc_scheme_base.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/core/persqueue/public/schema/schema_operation.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/retry/retry_policy.h>

namespace NKikimr::NGRpcService {

namespace {

TString CanonicalizePath(const TString& path, const TString& database) {
    return CanonizePath(path.StartsWith('/') ? path : database + "/" + path);
}

}

using TEvExportSecretRequest = TGrpcRequestOperationCall<Ydb::Secret::ExportSecretRequest,
    Ydb::Secret::ExportSecretResponse>;

class TExportSecretRPC: public TRpcSchemeRequestActor<TExportSecretRPC, TEvExportSecretRequest> {
    using TBase = TRpcSchemeRequestActor<TExportSecretRPC, TEvExportSecretRequest>;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        const auto token = Request_->GetInternalToken();
        if (!token || token->GetUserSID().empty() || !IsAdministrator(AppData(), token.Get())) {
            return Fail(Ydb::StatusIds::UNAUTHORIZED, "Secret export requires a cluster administrator");
        }

        const auto* request = GetProtoRequest();
        Database = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));
        TopicPath = CanonicalizePath(Request_->NormalizePath(request->topic_path()), Database);
        SecretPath = CanonicalizePath(Request_->NormalizePath(request->secret_path()), Database);
        if (Database.empty() || !TopicPath.StartsWith(Database + "/") || !SecretPath.StartsWith(Database + "/")) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Secret and topic paths must belong to the source database");
        }

        if (!AppData()->FeatureFlags.GetEnableSchemaSecrets()) {
            return Fail(Ydb::StatusIds::UNSUPPORTED, "Schema secrets are disabled");
        }

        Become(&TThis::StateDescribeSecret);
        DescribeSecret();
    }

private:
    void Fail(Ydb::StatusIds::StatusCode status, const TString& reason) {
        Request_->RaiseIssue(NYql::TIssue(reason));
        Reply(status, ActorContext());
    }

    void DescribeSecret() {
        auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
        SetDatabase(request.Get(), *Request_);
        auto* describe = request->Record.MutableDescribePath();
        // The caller is a cluster administrator; secret ACLs are not required.
        describe->SetPath(SecretPath);
        describe->MutableOptions()->SetReturnSecretValue(true);
        Send(MakeTxProxyID(), request.Release());
    }

    void Retry(IRetryPolicy<>::IRetryState::TPtr& state, ui64 tag, const TString& errorMessage) {
        if (!state) {
            static const auto policy = IRetryPolicy<>::GetExponentialBackoffPolicy(
                [] { return ERetryErrorClass::ShortRetry; },
                TDuration::MilliSeconds(100), TDuration::MilliSeconds(500),
                TDuration::Seconds(5), 10, TDuration::Seconds(10));
            state = policy->CreateRetryState();
        }
        if (const auto delay = state->GetNextRetryDelay()) {
            Schedule(*delay, new TEvents::TEvWakeup(tag));
        } else {
            Fail(Ydb::StatusIds::UNAVAILABLE, errorMessage);
        }
    }

    void RetryTopic() {
        Retry(TopicRetryState, WakeupTagRetryTopic, "Secret export topic is temporarily unavailable");
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();
        switch (record.GetStatus()) {
        case NKikimrScheme::StatusSuccess:
            break;
        case NKikimrScheme::StatusNotAvailable:
            return Retry(SecretRetryState, WakeupTagRetrySecret, "Secret is temporarily unavailable");
        case NKikimrScheme::StatusAccessDenied:
            return Fail(Ydb::StatusIds::UNAUTHORIZED, "Cannot read secret");
        default:
            return Fail(Ydb::StatusIds::SCHEME_ERROR, "Cannot describe secret");
        }
        const auto& description = record.GetPathDescription();
        if (description.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeSecret
            || !description.HasSecretDescription() || !description.GetSecretDescription().HasValue())
        {
            return Fail(Ydb::StatusIds::SCHEME_ERROR, "Secret value is missing");
        }
        Value = description.GetSecretDescription().GetValue();
        Become(&TThis::StateExport);
        NavigateTopic();
    }

    void HandleRetry(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Tag == WakeupTagRetrySecret) {
            DescribeSecret();
        } else if (ev->Get()->Tag == WakeupTagRetryTopic) {
            NavigateTopic();
        } else {
            TBase::HandleWakeup(ev, ctx);
        }
    }

    void NavigateTopic() {
        auto navigate = MakeHolder<TNavigate>();
        navigate->DatabaseName = Database;
        navigate->UserToken = Request_->GetInternalToken();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.Path = NKikimr::SplitPath(TopicPath);
        entry.Operation = TNavigate::OpTopic;
        entry.Access = NACLib::UpdateRow;
        entry.SyncVersion = true;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != 1) {
            return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected secret export topic description");
        }
        const auto& entry = entries.front();
        if (entry.Status == TNavigate::EStatus::PathErrorUnknown && !TopicCreationAttempted) {
            TopicCreationAttempted = true;
            return PrepareTopic();
        }
        if (entry.Status != TNavigate::EStatus::Ok) {
            switch (entry.Status) {
            case TNavigate::EStatus::AccessDenied:
                return Fail(Ydb::StatusIds::UNAUTHORIZED, "Cannot access secret export topic for writing");
            case TNavigate::EStatus::PathErrorUnknown:
            case TNavigate::EStatus::Unknown:
            case TNavigate::EStatus::TableCreationNotComplete:
            case TNavigate::EStatus::LookupError:
            case TNavigate::EStatus::RedirectLookupError:
                // Wait for concurrent creation and SchemeCache publication.
                return RetryTopic();
            default:
                return Fail(Ydb::StatusIds::SCHEME_ERROR, "Cannot resolve secret export topic");
            }
        }
        if (entry.Kind != TNavigate::KindTopic || !entry.PQGroupInfo) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Secret export destination is not a topic");
        }
        if (!entry.SecurityObject || entry.SecurityObject->GetOwnerSID().empty()) {
            return Fail(Ydb::StatusIds::UNAUTHORIZED, "Cannot determine secret export topic owner");
        }
        const auto& owner = entry.SecurityObject->GetOwnerSID();
        const auto token = Request_->GetInternalToken();
        const NACLib::TUserToken ownerToken(owner, TVector<NACLib::TSID>());
        // Use verified groups only when they belong to the actual topic owner.
        const auto* ownerIdentity = (owner == token->GetUserSID()) ? token.Get() : &ownerToken;
        if (!IsAdministrator(AppData(), ownerIdentity)) {
            return Fail(Ydb::StatusIds::UNAUTHORIZED, "Secret export topic must be owned by a cluster administrator");
        }
        const auto& description = entry.PQGroupInfo->Description;
        const auto& config = description.GetPQTabletConfig();
        if (description.PartitionsSize() != 1
            || description.GetPartitions(0).GetPartitionId() != 0
            || description.GetPartitions(0).GetStatus() != NKikimrPQ::Active
            || config.GetPartitionStrategy().GetPartitionStrategyType() != NKikimrPQ::TPQTabletConfig::DISABLED)
        {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED,
                "Secret export requires a single active partition 0 with auto-partitioning disabled");
        }
        const auto consumerName = NPersQueue::ConvertNewConsumerName("replicationConsumer", AppData()->PQConfig);
        bool compatibleConsumer = false;
        for (const auto& consumer : config.GetConsumers()) {
            if (consumer.GetName() == consumerName && consumer.GetImportant()
                && consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING
                && consumer.GetReadFromTimestampsMs() == 0)
            {
                compatibleConsumer = true;
                break;
            }
        }
        if (!compatibleConsumer) {
            return Fail(Ydb::StatusIds::PRECONDITION_FAILED,
                "Secret export requires an important streaming replication consumer without a read timestamp filter");
        }
        auto opts = NPQ::TPartitionWriterOpts()
            .WithSourceId(SecretPath).WithDatabase(Database).WithTopicPath(TopicPath)
            .WithToken(Request_->GetSerializedToken());
        const auto& partition = description.GetPartitions(0);
        Writer = Register(NPQ::CreatePartitionWriter(SelfId(), partition.GetTabletId(), partition.GetPartitionId(), opts));
    }

    void PrepareTopic() {
        Ydb::Topic::CreateTopicRequest topic;
        topic.set_path(TopicPath);
        topic.mutable_partitioning_settings()->set_min_active_partitions(1);
        topic.mutable_partitioning_settings()->set_max_active_partitions(1);
        topic.mutable_retention_period()->set_seconds(AppData()->ReplicationConfig.GetRetentionPeriodSeconds());
        auto* consumer = topic.add_consumers();
        consumer->set_name("replicationConsumer");
        consumer->set_important(true);
        TopicPreparer = Register(NPQ::NSchema::CreateCreateTopicActor(SelfId(), {
            .Database = Database,
            .PeerName = Request_->GetPeerName(),
            .Request = std::move(topic),
            .UserToken = Request_->GetInternalToken(),
            .IfNotExists = true,
            .PrepareOnly = true,
        }));
    }

    void Handle(NPQ::NSchema::TEvSchemaResponse::TPtr& ev) {
        if (ev->Sender != TopicPreparer) {
            return;
        }
        TopicPreparer = {};
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return Fail(ev->Get()->Status, "Cannot prepare secret export topic");
        }
        if (ev->Get()->ModifyScheme.GetOperationType() != NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup) {
            return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected secret export topic operation");
        }
        auto request = CreateProposeTransaction();
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx = std::move(ev->Get()->ModifyScheme);
        tx.SetFailOnExist(false);
        // Protect the new topic before its first reader can attach.
        NACLib::TDiffACL acl;
        acl.SetInterruptInheritance(true);
        acl.AddAccess(
            NACLib::EAccessType::Allow, NACLib::GenericFull,
            Request_->GetInternalToken()->GetUserSID(), NACLib::EInheritanceType::InheritNone
        );
        tx.MutableModifyACL()->SetDiffACL(acl.SerializeAsString());
        SetupActor = Register(NPQ::NSchema::CreateSchemaOperation(SelfId(), TopicPath, std::move(request)));
    }

    void Handle(NPQ::NSchema::TEvSchemaOperationResponse::TPtr& ev) {
        if (ev->Sender != SetupActor) {
            return;
        }
        SetupActor = {};
        const auto status = ev->Get()->Status;
        if (status == Ydb::StatusIds::OVERLOADED || status == Ydb::StatusIds::UNAVAILABLE) {
            // Resolve again before retrying an idempotent create: another exporter
            // may have created the topic, or our previous proposal may have succeeded.
            TopicCreationAttempted = false;
            return RetryTopic();
        }
        if (status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::ALREADY_EXISTS) {
            return Fail(status, TStringBuilder() << "Cannot prepare secret export transport: " << ev->Get()->ErrorMessage);
        }
        // An existing topic keeps its ACL. Validate its actual owner and
        // write permissions after the source-side scheme operation.
        NavigateTopic();
    }

    void Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev) {
        if (ev->Sender != Writer) {
            return;
        }
        if (!ev->Get()->IsSuccess()) {
            return Fail(Ydb::StatusIds::UNAVAILABLE, TStringBuilder()
                << "Cannot initialize secret export writer: " << ev->Get()->GetError().Reason);
        }
        NKikimrReplication::TSecretRecord record;
        record.SetSourcePath(SecretPath);
        record.SetValue(Value);
        NKikimrPQClient::TDataChunk chunk;
        chunk.SetCodec(0);
        chunk.SetData(record.SerializeAsString());
        auto request = MakeHolder<NPQ::TEvPartitionWriter::TEvWriteRequest>(1);
        auto& write = *request->Record.MutablePartitionRequest()->AddCmdWrite();
        write.SetSourceId(NPQ::NSourceIdEncoding::EncodeSimple(SecretPath));
        // Each canonical secret path has its own SourceId. SeqNo=1 deduplicates
        // retries of that secret while other secrets write independently.
        write.SetSeqNo(1);
        write.SetCreateTimeMS(Now().MilliSeconds());
        write.SetData(chunk.SerializeAsString());
        Send(Writer, request.Release());
    }

    void Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev) {
        if (ev->Sender != Writer) {
            return;
        }
        if (!ev->Get()->IsSuccess()) {
            return Fail(Ydb::StatusIds::UNAVAILABLE, "Cannot write secret export record");
        }
        Reply(Ydb::StatusIds::SUCCESS, ActorContext());
    }

    void Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr& ev) {
        if (ev->Sender == Writer) {
            Fail(Ydb::StatusIds::UNAVAILABLE, "Secret export writer disconnected");
        }
    }

    void PassAway() override {
        for (const auto& actor : {SetupActor, TopicPreparer}) {
            if (actor) {
                Send(actor, new TEvents::TEvPoison());
            }
        }
        if (Writer) {
            Send(Writer, new TEvents::TEvPoisonPill());
        }
        TBase::PassAway();
    }

    STATEFN(StateDescribeSecret) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(TEvents::TEvWakeup, HandleRetry);
        default:
            TBase::StateWork(ev);
        }
    }

    STATEFN(StateExport) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NPQ::NSchema::TEvSchemaResponse, Handle);
            hFunc(NPQ::NSchema::TEvSchemaOperationResponse, Handle);
            hFunc(NPQ::TEvPartitionWriter::TEvInitResult, Handle);
            hFunc(NPQ::TEvPartitionWriter::TEvWriteResponse, Handle);
            hFunc(NPQ::TEvPartitionWriter::TEvDisconnected, Handle);
            IgnoreFunc(NPQ::TEvPartitionWriter::TEvWriteAccepted);
            HFunc(TEvents::TEvWakeup, HandleRetry);
        default:
            TBase::StateWork(ev);
        }
    }

    static constexpr ui64 WakeupTagRetrySecret = 100;
    static constexpr ui64 WakeupTagRetryTopic = 101;
    IRetryPolicy<>::IRetryState::TPtr SecretRetryState;
    IRetryPolicy<>::IRetryState::TPtr TopicRetryState;
    TString Database;
    TString TopicPath;
    TString SecretPath;
    TString Value;
    TActorId Writer;
    bool TopicCreationAttempted = false;
    TActorId SetupActor;
    TActorId TopicPreparer;
};

void DoExportSecretRequest(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TExportSecretRPC(request.release()));
}

} // namespace NKikimr::NGRpcService
