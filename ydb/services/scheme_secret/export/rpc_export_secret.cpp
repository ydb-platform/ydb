#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/service_secret.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/core/persqueue/public/schema/schema_operation.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>
#include <ydb/services/scheme_secret/resolver.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

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

    enum class ESetupStage {
        None,
        Directory,
        Topic,
    };

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        const auto* request = GetProtoRequest();
        Database = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));
        TopicPath = CanonicalizePath(Request_->NormalizePath(request->topic_path()), Database);
        SecretPath = CanonicalizePath(Request_->NormalizePath(request->secret_path()), Database);
        if (Database.empty() || !TopicPath.StartsWith(Database + "/") || !SecretPath.StartsWith(Database + "/")) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Secret and topic paths must belong to the source database");
        }
        const auto token = Request_->GetInternalToken();
        if (!token || token->GetUserSID().empty()) {
            return Fail(Ydb::StatusIds::UNAUTHORIZED, "Secret export requires an authenticated source user");
        }
        Become(&TThis::StateExport);
        const auto actorSystem = ActorContext().ActorSystem();
        const auto selfId = SelfId();
        NSecret::DescribeSecret({SecretPath}, token, Database, actorSystem,
            {.RetryPolicy = NSecret::MakeShortRetryPolicy()})
            .Subscribe([actorSystem, selfId](const auto& result) {
                actorSystem->Send(selfId, new NKqp::TEvDescribeSecretsResponse(result.GetValue()));
            });
    }

private:
    void Fail(Ydb::StatusIds::StatusCode status, const TString& reason) {
        Request_->RaiseIssue(NYql::TIssue(reason));
        Reply(status, ActorContext());
    }

    void Handle(NKqp::TEvDescribeSecretsResponse::TPtr& ev) {
        const auto& description = ev->Get()->Description;
        if (description.Status != Ydb::StatusIds::SUCCESS) {
            Request_->RaiseIssues(description.Issues);
            return Reply(description.Status, ActorContext());
        }
        if (description.SecretValues.size() != 1) {
            return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected number of resolved secrets");
        }
        Value = description.SecretValues.front();
        NavigateTopic();
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
            if (!IsAdministrator(AppData(), Request_->GetInternalToken().Get())) {
                return Fail(Ydb::StatusIds::UNAUTHORIZED, "Creating a secret export topic requires a cluster administrator");
            }
            TopicCreationAttempted = true;
            return PrepareTopicDirectory();
        }
        if (entry.Status != TNavigate::EStatus::Ok) {
            switch (entry.Status) {
            case TNavigate::EStatus::AccessDenied:
                return Fail(Ydb::StatusIds::UNAUTHORIZED, "Cannot access secret export topic for writing");
            case TNavigate::EStatus::Unknown:
            case TNavigate::EStatus::TableCreationNotComplete:
            case TNavigate::EStatus::LookupError:
            case TNavigate::EStatus::RedirectLookupError:
                // Other secret targets can be preparing the same topic.
                return Fail(Ydb::StatusIds::UNAVAILABLE, "Secret export topic is not ready");
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
        const auto* ownerIdentity = owner == token->GetUserSID() ? token.Get() : &ownerToken;
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

    void PrepareTopicDirectory() {
        const TString directory{ExtractParent(TopicPath)};
        if (directory == Database) {
            return PrepareTopic();
        }
        auto request = CreateProposeTransaction();
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetWorkingDir(TString{ExtractParent(directory)});
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
        tx.SetFailOnExist(false);
        tx.MutableMkDir()->SetName(TString{ExtractBase(directory)});
        SetupStage = ESetupStage::Directory;
        SetupActor = Register(NPQ::NSchema::CreateSchemaOperation(SelfId(), directory,
            std::move(request), ++SetupCookie));
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
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull,
            Request_->GetInternalToken()->GetUserSID(), NACLib::EInheritanceType::InheritNone);
        tx.MutableModifyACL()->SetDiffACL(acl.SerializeAsString());
        SetupStage = ESetupStage::Topic;
        SetupActor = Register(NPQ::NSchema::CreateSchemaOperation(SelfId(), TopicPath,
            std::move(request), ++SetupCookie));
    }

    void Handle(NPQ::NSchema::TEvSchemaOperationResponse::TPtr& ev) {
        if (ev->Sender != SetupActor || ev->Cookie != SetupCookie) {
            return;
        }
        SetupActor = {};
        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::ALREADY_EXISTS) {
            return Fail(status, TStringBuilder() << "Cannot prepare secret export transport: " << ev->Get()->ErrorMessage);
        }
        const auto stage = SetupStage;
        SetupStage = ESetupStage::None;
        switch (stage) {
        case ESetupStage::Directory:
            return PrepareTopic();
        case ESetupStage::Topic:
            // An existing topic keeps its ACL. Validate its actual owner and
            // write permissions after the source-side scheme operation.
            return NavigateTopic();
        case ESetupStage::None:
            return;
        }
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
        // A retained topic represents one export: retries of this source are deduplicated.
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

    STATEFN(StateExport) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvDescribeSecretsResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NPQ::NSchema::TEvSchemaResponse, Handle);
            hFunc(NPQ::NSchema::TEvSchemaOperationResponse, Handle);
            hFunc(NPQ::TEvPartitionWriter::TEvInitResult, Handle);
            hFunc(NPQ::TEvPartitionWriter::TEvWriteResponse, Handle);
            hFunc(NPQ::TEvPartitionWriter::TEvDisconnected, Handle);
            IgnoreFunc(NPQ::TEvPartitionWriter::TEvWriteAccepted);
        default:
            TBase::StateWork(ev);
        }
    }

    TString Database;
    TString TopicPath;
    TString SecretPath;
    TString Value;
    TActorId Writer;
    bool TopicCreationAttempted = false;
    ESetupStage SetupStage = ESetupStage::None;
    TActorId SetupActor;
    TActorId TopicPreparer;
    ui64 SetupCookie = 0;
};

void DoExportSecretRequest(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TExportSecretRPC(request.release()));
}

} // namespace NKikimr::NGRpcService
