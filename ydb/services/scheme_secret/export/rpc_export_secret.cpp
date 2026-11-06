#include <ydb/core/base/appdata.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/service_secret.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/persqueue/writer/partition_chooser.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/tx/replication/controller/protos/secret_copy.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_secret.pb.h>
#include <ydb/services/scheme_secret/resolver.h>

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

    void Bootstrap() {
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
        if (entry.Status != TNavigate::EStatus::Ok) {
            if (entry.Status == TNavigate::EStatus::AccessDenied) {
                return Fail(Ydb::StatusIds::UNAUTHORIZED, "Cannot access secret export topic for writing");
            }
            return Fail(Ydb::StatusIds::SCHEME_ERROR, "Cannot resolve secret export topic");
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
        const auto chooser = NPQ::CreatePartitionChooser(entry.PQGroupInfo->Description);
        const auto* partition = chooser->GetPartition(ui32{0});
        if (!partition) {
            return Fail(Ydb::StatusIds::BAD_REQUEST, "Secret export topic has no writable partition 0");
        }
        auto opts = NPQ::TPartitionWriterOpts()
            .WithSourceId(SecretPath).WithDatabase(Database).WithTopicPath(TopicPath)
            .WithToken(Request_->GetSerializedToken());
        Writer = Register(NPQ::CreatePartitionWriter(SelfId(), partition->TabletId, partition->PartitionId, opts));
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
        if (Writer) {
            Send(Writer, new TEvents::TEvPoisonPill());
        }
        TBase::PassAway();
    }

    STATEFN(StateExport) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvDescribeSecretsResponse, Handle);
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
    TString SecretPath;
    TString Value;
    TActorId Writer;
};

void DoExportSecretRequest(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TExportSecretRPC(request.release()));
}

} // namespace NKikimr::NGRpcService
