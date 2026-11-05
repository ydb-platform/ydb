#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/service_secret.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/tx/replication/controller/protos/secret_copy.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>
#include <ydb/services/scheme_secret/resolver.h>

namespace NKikimr::NGRpcService {

namespace NSchemeEvents = NSchemeShard::TEvSchemeShard;

using TEvExportSecretsRequest = TGrpcRequestOperationCall<Ydb::Secret::ExportSecretsRequest,
    Ydb::Secret::ExportSecretsResponse>;

class TExportSecretsRPC: public TRpcSchemeRequestActor<TExportSecretsRPC, TEvExportSecretsRequest> {
    using TBase = TRpcSchemeRequestActor<TExportSecretsRPC, TEvExportSecretsRequest>;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

public:
    using TBase::TBase;

    void Bootstrap() {
        const auto* request = GetProtoRequest();
        Database = CanonizePath(Request_->GetDatabaseName().GetOrElse(""));
        TopicPath = Request_->NormalizePath(request->topic_path());
        if (request->secret_paths().empty() || !TopicPath.StartsWith(Database + "/")) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Secret paths and a topic in the source database are required");
        }
        for (const auto& path : request->secret_paths()) {
            const auto normalized = Request_->NormalizePath(path);
            if (!normalized.StartsWith(Database + "/")) {
                return Fail(Ydb::StatusIds::BAD_REQUEST, "Secret must belong to the source database");
            }
            Paths.push_back(normalized);
        }
        const auto token = Request_->GetInternalToken();
        if (!token || token->GetUserSID().empty()) {
            return Fail(Ydb::StatusIds::UNAUTHORIZED, "Secret export requires an authenticated source user");
        }
        Become(&TThis::StateExport);
        const auto actorSystem = ActorContext().ActorSystem();
        const auto selfId = SelfId();
        // Read once. The replication does not subscribe to subsequent changes.
        NSecret::DescribeSecret(Paths, token, Database, actorSystem,
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
        if (description.SecretValues.size() != Paths.size()) {
            return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected number of resolved secrets");
        }
        Values = description.SecretValues;
        Ydb::Topic::CreateTopicRequest topic;
        topic.set_path(TopicPath);
        topic.mutable_partitioning_settings()->set_min_active_partitions(1);
        topic.mutable_partitioning_settings()->set_max_active_partitions(1);
        topic.mutable_retention_period()->set_seconds(86400);
        topic.add_consumers()->set_name("replicationConsumer");
        Register(NPQ::NSchema::CreateCreateTopicActor(SelfId(), {
            .Database = Database,
            .PeerName = Request_->GetPeerName(),
            .Request = std::move(topic),
            .UserToken = Request_->GetInternalToken(),
            .IfNotExists = false,
            .PrepareOnly = true,
        }));
    }

    void Handle(NPQ::NSchema::TEvSchemaResponse::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return Fail(ev->Get()->Status, "Cannot prepare secret replication topic");
        }
        auto request = CreateProposeTransaction();
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx = std::move(ev->Get()->ModifyScheme);
        tx.SetFailOnExist(true);
        // Apply privacy atomically: an inherited reader must never attach first.
        NACLib::TDiffACL acl;
        acl.SetInterruptInheritance(true);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull,
            Request_->GetInternalToken()->GetUserSID(), NACLib::EInheritanceType::InheritNone);
        tx.MutableModifyACL()->SetDiffACL(acl.SerializeAsString());
        Send(MakeTxProxyID(), request.release());
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto code = YdbStatusFromProxyStatus(ev->Get());
        if (!IsTxProxyInProgress(code)) {
            if (code == Ydb::StatusIds::SUCCESS) {
                return NavigateTopic();
            }
            return Fail(code, "Cannot create private secret replication topic");
        }
        SetSchemeShardId(ev->Get()->Record.GetSchemeShardTabletId());
        ForwardToSchemeShard(ActorContext(), std::make_unique<NSchemeEvents::TEvNotifyTxCompletion>(
            ev->Get()->Record.GetTxId()));
    }

    void OnNotifyTxCompletionResult(NSchemeEvents::TEvNotifyTxCompletionResult::TPtr&,
        const TActorContext&) override
    {
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
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& entries = ev->Get()->Request->ResultSet;
        if (entries.size() != 1 || entries.front().Status != TNavigate::EStatus::Ok) {
            return Fail(Ydb::StatusIds::UNAUTHORIZED, "Cannot access secret export topic for writing");
        }
        const auto& entry = entries.front();
        if (entry.Kind != TNavigate::KindTopic || !entry.PQGroupInfo
            || entry.PQGroupInfo->Description.PartitionsSize() != 1)
        {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Secret export requires a single-partition topic");
        }
        const auto& partition = entry.PQGroupInfo->Description.GetPartitions(0);
        auto opts = NPQ::TPartitionWriterOpts()
            .WithSourceId(TopicPath).WithDatabase(Database).WithTopicPath(TopicPath)
            .WithToken(Request_->GetSerializedToken());
        Writer = Register(NPQ::CreatePartitionWriter(SelfId(), partition.GetTabletId(), partition.GetPartitionId(), opts));
    }

    void Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev) {
        if (!ev->Get()->IsSuccess()) {
            return Fail(Ydb::StatusIds::UNAVAILABLE, TStringBuilder()
                << "Cannot initialize secret export writer: " << ev->Get()->GetError().Reason);
        }
        WriteNext();
    }

    void WriteNext() {
        if (Next == Paths.size()) {
            return Reply(Ydb::StatusIds::SUCCESS, ActorContext());
        }
        NKikimrReplication::TSecretRecord record;
        record.SetSourcePath(Paths[Next]);
        record.SetValue(Values[Next]);
        NKikimrPQClient::TDataChunk chunk;
        chunk.SetCodec(0);
        chunk.SetData(record.SerializeAsString());
        auto request = MakeHolder<NPQ::TEvPartitionWriter::TEvWriteRequest>(Next + 1);
        auto& write = *request->Record.MutablePartitionRequest()->AddCmdWrite();
        write.SetSourceId(NPQ::NSourceIdEncoding::EncodeSimple(TopicPath));
        write.SetSeqNo(Next + 1);
        write.SetCreateTimeMS(Now().MilliSeconds());
        write.SetData(chunk.SerializeAsString());
        Send(Writer, request.Release());
    }

    void Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev) {
        if (!ev->Get()->IsSuccess()) {
            return Fail(Ydb::StatusIds::UNAVAILABLE, "Cannot write secret export record");
        }
        ++Next;
        WriteNext();
    }

    void Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr&) {
        Fail(Ydb::StatusIds::UNAVAILABLE, "Secret export writer disconnected");
    }

    void PassAway() override {
        if (Writer) {
            Send(Writer, new TEvents::TEvPoisonPill());
        }
        TBase::PassAway();
    }

    STATEFN(StateExport) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvDescribeSecretsResponse, Handle);
            hFunc(NPQ::NSchema::TEvSchemaResponse, Handle);
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
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
    TVector<TString> Paths;
    std::vector<TString> Values;
    TActorId Writer;
    size_t Next = 0;
};

void DoExportSecretsRequest(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TExportSecretsRPC(request.release()));
}

}
