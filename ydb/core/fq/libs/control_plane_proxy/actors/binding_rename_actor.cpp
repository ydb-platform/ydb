#include "base_actor.h"
#include "binding_rename_actor.h"

#include <ydb/core/base/path.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/private_client/private_client.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/generic/algorithm.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

#include <algorithm>

namespace NFq::NPrivate {

using TProxyRequest = TEvControlPlaneProxy::TEvModifyBindingRequest;
using TProxyResponse = TEvControlPlaneProxy::TEvModifyBindingResponse;
using TStorageRequest = TEvControlPlaneStorage::TEvModifyBindingRequest;
using TStorageResponse = TEvControlPlaneStorage::TEvModifyBindingResponse;
using TRename = FederatedQuery::Internal::BindingRename;

class TRenameBindingActor;

template <>
struct TBaseActorTypeTag<TRenameBindingActor> {
    using TRequest = TProxyRequest;
    using TResponse = TProxyResponse;
};

namespace {

constexpr TStringBuf AbsentSource = "-";

struct TPrivateSchemaResponse: TEventLocal<TPrivateSchemaResponse, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    explicit TPrivateSchemaResponse(TMutateExternalTableResult result)
        : Mutate(std::move(result))
    {
    }

    explicit TPrivateSchemaResponse(TDescribeExternalTableResult result)
        : Describe(std::move(result))
    {
    }

    explicit TPrivateSchemaResponse(TString error)
        : Error(std::move(error))
    {
    }

    std::optional<TMutateExternalTableResult> Mutate;
    std::optional<TDescribeExternalTableResult> Describe;
    TString Error;
};

TString FormatColumnType(const Ydb::Type& type) {
    const auto& item = type.has_optional_type() ? type.optional_type().item() : type;
    if (item.has_pg_type()) {
        const auto& pg = item.pg_type();
        const NKikimr::NPg::ITypeDesc* descriptor;
        if (pg.oid()) {
            descriptor = NKikimr::NPg::TypeDescFromPgTypeId(pg.oid());
        } else {
            descriptor = NKikimr::NPg::TypeDescFromPgTypeName(pg.type_name());
            if (!descriptor) {
                descriptor = NKikimr::NPg::TypeDescFromPgTypeId(NYql::NPg::LookupType(pg.type_name()).TypeId);
            }
        }
        Y_ENSURE(descriptor, "Unsupported PostgreSQL external table column type");
        return NKikimr::NPg::PgTypeNameFromTypeDesc(descriptor, pg.type_modifier());
    }
    Y_ENSURE(item.type_case() == Ydb::Type::kTypeId || item.has_decimal_type(), "Unsupported external table column type");
    NYql::TExprContext context;
    NYdb::TTypeParser parser(item);
    const auto* node = MakeType(parser, context);
    Y_ENSURE(node, "Unsupported external table column type");
    return NYql::FormatType(node);
}

Fq::Private::MutateExternalTableRequest MakeCreateRequest(const TRename& rename) {
    const auto& subset = rename.destination().setting().object_storage().subset(0);
    Fq::Private::MutateExternalTableRequest request;
    request.set_relative_path(rename.destination().name());
    auto& create = *request.mutable_create();
    create.set_key(rename.operation_id());
    create.set_relative_data_source_path(rename.connection_name());
    create.set_location(subset.path_pattern());
    for (const auto& column : subset.schema().column()) {
        auto& target = *create.add_columns();
        target.set_name(column.name());
        target.set_type(FormatColumnType(column.type()));
        target.set_not_null(!column.type().has_optional_type());
    }

    auto& attributes = *create.mutable_attributes();
    if (!subset.format().empty()) {
        attributes["format"] = subset.format();
    }
    if (!subset.compression().empty()) {
        attributes["compression"] = subset.compression();
    }
    for (const auto& [key, value] : subset.format_setting()) {
        attributes[to_lower(key)] = value;
    }
    for (const auto& [key, value] : subset.projection()) {
        attributes[key] = value;
    }
    if (!subset.partitioned_by().empty()) {
        NJson::TJsonValue names(NJson::JSON_ARRAY);
        for (const auto& name : subset.partitioned_by()) {
            names.AppendValue(name);
        }
        attributes["partitioned_by"] = NJson::WriteJson(names, false);
    }
    return request;
}

bool MatchesSource(
    const TRename& rename, const FederatedQuery::BindingContent& binding, const Ydb::Table::DescribeExternalTableResult& description) {
    const auto& objectStorage = binding.setting().object_storage();
    if (objectStorage.subset_size() != 1) {
        return false;
    }
    const auto& subset = objectStorage.subset(0);
    if (description.self().name() != rename.source_name() || description.source_type() != "ObjectStorage" ||
        description.data_source_path() !=
            NKikimr::CanonizePath(TStringBuilder() << rename.compute_database() << '/' << rename.connection_name()) ||
        description.location() != subset.path_pattern() || description.columns_size() != subset.schema().column_size()) {
        return false;
    }

    for (const auto& column : subset.schema().column()) {
        const auto found = std::find_if(description.columns().begin(), description.columns().end(), [&column](const auto& existing) {
            return existing.name() == column.name();
        });
        if (found == description.columns().end() || FormatColumnType(found->type()) != FormatColumnType(column.type()) ||
            (found->type().has_pg_type() ? found->not_null() : !found->type().has_optional_type()) == column.type().has_optional_type()) {
            return false;
        }
    }

    THashMap<TString, NJson::TJsonValue> expected;
    if (!subset.format().empty()) {
        expected["FORMAT"].SetType(NJson::JSON_ARRAY).AppendValue(subset.format());
    }
    if (!subset.compression().empty()) {
        expected["COMPRESSION"].SetType(NJson::JSON_ARRAY).AppendValue(subset.compression());
    }
    for (const auto& [key, value] : subset.format_setting()) {
        expected[to_upper(key)].SetType(NJson::JSON_ARRAY).AppendValue(value);
    }
    if (!subset.partitioned_by().empty()) {
        auto& names = expected["PARTITIONED_BY"].SetType(NJson::JSON_ARRAY);
        for (const auto& name : subset.partitioned_by()) {
            names.AppendValue(name);
        }
    }
    if (!subset.projection().empty()) {
        const auto found = description.content().find("PROJECTION");
        NJson::TJsonValue outer;
        NJson::TJsonValue actual;
        NJson::TJsonValue projection(NJson::JSON_MAP);
        for (const auto& [key, value] : subset.projection()) {
            projection[key] = value;
        }
        if (found == description.content().end() || !NJson::ReadJsonTree(found->second, &outer) || !outer.IsArray() ||
            outer.GetArray().size() != 1 || !outer.GetArray().front().IsString() ||
            !NJson::ReadJsonTree(outer.GetArray().front().GetString(), &actual) || !(projection == actual)) {
            return false;
        }
    }

    if (description.content().size() != expected.size() + !subset.projection().empty()) {
        return false;
    }
    for (const auto& [key, value] : expected) {
        const auto found = description.content().find(key);
        NJson::TJsonValue actual;
        if (found == description.content().end() || !NJson::ReadJsonTree(found->second, &actual) || !(value == actual)) {
            return false;
        }
    }
    return true;
}

bool HasValidIdentity(const Fq::Private::ExternalTableIdentity& identity) {
    return identity.owner_id() && identity.local_path_id() && identity.version();
}

}   // namespace

class TRenameBindingActor final: public TBaseActor<TRenameBindingActor> {
public:
    static constexpr char ActorName[] = "YQ_RENAME_BINDING";

    TRenameBindingActor(const TActorId& proxy, TProxyRequest::TPtr request, TDuration timeout, const TRequestCommonCountersPtr& counters,
        TPermissions permissions, std::unique_ptr<TPrivateClient> compute)
        : TBaseActor(proxy, std::move(request), timeout, counters)
        , Permissions(permissions)
        , Compute(std::move(compute))
    {
    }

    void BootstrapImpl() override {
        if (!Compute) {
            SendStorage(EBindingRenameAction::Probe);
        } else if (Request->Get()->Rename) {
            Drive();
        } else {
            if (!ValidateDestination(Request->Get()->Request.content())) {
                return Reply(Failure);
            }
            SendStorage(EBindingRenameAction::Prepare);
        }
    }

    STRICT_STFUNC(StateFunc,
        cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(TStorageResponse, Handle);
        hFunc(TPrivateSchemaResponse, Handle);
    )

private:
    enum class EComputeAction {
        Describe,
        Create,
        Commit,
        DropSource,
        Abort,
    };

    void SendStorage(EBindingRenameAction action, const TString& identity = {}) {
        StorageAction = action;
        const auto& request = *Request->Get();
        auto event = std::make_unique<TStorageRequest>(request.Scope, request.Request, request.User, request.Token, request.CloudId, Permissions,
            request.Quotas, request.TenantInfo, request.ComputeDatabase.GetOrElse({}));
        event->RenameAction = action;
        if (action == EBindingRenameAction::Prepare) {
            event->ObservedRevision = request.OldBindingRevision;
            event->RenameConnectionName = request.ConnectionContent->name();
        }
        if (request.Rename) {
            event->RenameOperationId = request.Rename->operation_id();
        }
        event->RenameIdentity = identity;
        Send(ControlPlaneStorageServiceActorId(), event.release());
    }

    void Handle(TStorageResponse::TPtr& ev) {
        auto& request = *Request->Get();
        const auto& response = *ev->Get();
        if (response.Issues) {
            if (StorageAction == EBindingRenameAction::Commit && AllOf(response.Issues, [](const auto& issue) {
                    return issue.GetCode() == TIssuesIds::BAD_REQUEST || issue.GetCode() == TIssuesIds::ACCESS_DENIED;
                })) {
                Failure = response.Issues;
                SendStorage(EBindingRenameAction::Abort);
            } else {
                Reply(response.Issues);
            }
            return;
        }

        const auto& audit = response.AuditDetails;
        if (StorageAction == EBindingRenameAction::Probe) {
            if (!audit.Before) {
                return Fail("Couldn't read binding state");
            }
            request.OldBindingContent = audit.Before->content();
            request.OldBindingRevision = audit.Before->meta().revision();
            request.Rename = audit.Rename;
            SendRequestToSender();
            return;
        }
        if (audit.IdempotencyResult) {
            return ReplySuccess(audit);
        }
        if (!audit.Rename) {
            return Fail("Couldn't determine binding rename state");
        }
        request.Rename = audit.Rename;
        if (audit.After) {
            CommitAudit = audit;
        }
        if (StorageAction == EBindingRenameAction::Finish) {
            if (request.Rename->state() == TRename::ABORTED) {
                if (Failure.Empty()) {
                    Failure.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Binding rename was aborted. Retry to start a new operation"));
                }
                return Reply(Failure);
            }
            return ReplySuccess(CommitAudit);
        }
        Drive();
    }

    bool ValidateDestination(const FederatedQuery::BindingContent& content) {
        if (content.setting().object_storage().subset_size() == 1) {
            return true;
        }
        Failure.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "A binding rename requires exactly one Object Storage subset"));
        return false;
    }

    void Drive() {
        const auto& request = *Request->Get();
        const auto& rename = *request.Rename;
        const auto& connection = request.ComputeDatabase->connection();
        if (rename.compute_endpoint() != connection.endpoint() || rename.compute_database() != connection.database()) {
            return Fail("Compute database changed while the binding rename was pending");
        }
        switch (rename.state()) {
            case TRename::PREPARED: {
                if (!ValidateDestination(rename.destination())) {
                    return SendStorage(EBindingRenameAction::Abort);
                }
                if (rename.source_identity().empty()) {
                    Fq::Private::DescribeExternalTableRequest describe;
                    describe.set_relative_path(rename.source_name());
                    ComputeAction = EComputeAction::Describe;
                    return WaitFor(Compute->DescribeExternalTable(
                        std::move(describe), TDescribeExternalTableSettings().ClientTimeout(RequestTimeout).OperationTimeout(RequestTimeout)));
                }
                Fq::Private::MutateExternalTableRequest create;
                try {
                    create = MakeCreateRequest(rename);
                } catch (...) {
                    Failure.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, CurrentExceptionMessage()));
                    return SendStorage(EBindingRenameAction::Abort);
                }
                try {
                    return SendCompute(EComputeAction::Create, std::move(create));
                } catch (...) {
                    return Fail(CurrentExceptionMessage());
                }
            }
            case TRename::COMMITTED: {
                if (!CommitAudit.After) {
                    CommitAudit.IdempotencyResult = true;
                }
                Fq::Private::MutateExternalTableRequest commit;
                commit.set_relative_path(rename.destination().name());
                commit.mutable_commit()->set_key(rename.operation_id());
                return SendCompute(EComputeAction::Commit, std::move(commit));
            }
            case TRename::ABORTED: {
                Fq::Private::MutateExternalTableRequest abort;
                abort.set_relative_path(rename.destination().name());
                abort.mutable_abort()->set_key(rename.operation_id());
                return SendCompute(EComputeAction::Abort, std::move(abort));
            }
            case TRename::FINISHED:
                CommitAudit.IdempotencyResult = true;
                return ReplySuccess(CommitAudit);
            default:
                return Fail("Unknown binding rename state");
        }
    }

    void SendCompute(EComputeAction action, Fq::Private::MutateExternalTableRequest request) {
        ComputeAction = action;
        WaitFor(Compute->MutateExternalTable(
            std::move(request), TMutateExternalTableSettings().ClientTimeout(RequestTimeout).OperationTimeout(RequestTimeout)));
    }

    template <class T>
    void WaitFor(NThreading::TFuture<T> future) {
        future.Subscribe([as = TActivationContext::ActorSystem(), self = SelfId()](auto response) {
            try {
                as->Send(self, new TPrivateSchemaResponse(response.ExtractValueSync()));
            } catch (...) {
                as->Send(self, new TPrivateSchemaResponse(CurrentExceptionMessage()));
            }
        });
    }

    void Handle(TPrivateSchemaResponse::TPtr& ev) {
        auto& request = *Request->Get();
        const auto& rename = *request.Rename;
        const auto& response = *ev->Get();
        if (!response.Error.empty()) {
            return Fail(response.Error);
        }
        if (ComputeAction == EComputeAction::Describe) {
            const auto& result = *response.Describe;
            if (result.GetStatus() == NYdb::EStatus::NOT_FOUND) {
                return SendStorage(EBindingRenameAction::RecordSource, TString(AbsentSource));
            }
            if (!result.IsSuccess()) {
                if (result.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED) {
                    Failure.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, result.GetIssues().ToOneLineString()));
                    return SendStorage(EBindingRenameAction::Abort);
                }
                return Fail(result.GetIssues().ToOneLineString());
            }
            bool matchesSource = false;
            try {
                matchesSource = result.IsResultSet() && HasValidIdentity(result.GetResult().identity()) &&
                                MatchesSource(rename, *request.OldBindingContent, result.GetResult().description());
            } catch (...) {
                Failure.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, CurrentExceptionMessage()));
                return SendStorage(EBindingRenameAction::Abort);
            }
            if (!matchesSource) {
                Failure.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "The source table no longer matches the binding"));
                return SendStorage(EBindingRenameAction::Abort);
            }
            return SendStorage(EBindingRenameAction::RecordSource, Base64Encode(result.GetResult().identity().SerializeAsString()));
        }

        const auto& result = *response.Mutate;
        if (!result.IsSuccess() && !(ComputeAction == EComputeAction::DropSource && result.GetStatus() == NYdb::EStatus::NOT_FOUND)) {
            const auto status = result.GetStatus();
            if (ComputeAction == EComputeAction::Create &&
                (status == NYdb::EStatus::BAD_REQUEST || status == NYdb::EStatus::ALREADY_EXISTS || status == NYdb::EStatus::SCHEME_ERROR ||
                    status == NYdb::EStatus::PRECONDITION_FAILED)) {
                Failure.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, result.GetIssues().ToOneLineString()));
                return SendStorage(EBindingRenameAction::Abort);
            }
            return Fail(result.GetIssues().ToOneLineString());
        }

        switch (ComputeAction) {
            case EComputeAction::Create:
                if (!result.IsResultSet() || !HasValidIdentity(result.GetResult().created_table())) {
                    return Fail("Compute did not return the new table identity");
                }
                return SendStorage(EBindingRenameAction::Commit, Base64Encode(result.GetResult().created_table().SerializeAsString()));
            case EComputeAction::Commit: {
                if (rename.source_identity() == AbsentSource) {
                    return SendStorage(EBindingRenameAction::Finish);
                }
                Fq::Private::MutateExternalTableRequest drop;
                drop.set_relative_path(rename.source_name());
                auto& identity = *drop.mutable_guarded_drop();
                try {
                    if (!identity.ParseFromString(Base64Decode(rename.source_identity())) || !HasValidIdentity(identity)) {
                        return Fail("Invalid saved source table identity");
                    }
                } catch (...) {
                    return Fail("Invalid saved source table identity");
                }
                return SendCompute(EComputeAction::DropSource, std::move(drop));
            }
            case EComputeAction::DropSource:
            case EComputeAction::Abort:
                return SendStorage(EBindingRenameAction::Finish);
            case EComputeAction::Describe:
                Y_ABORT("Unexpected compute response");
        }
    }

    void HandleTimeout() {
        Counters->Timeout->Inc();
        Reply({ MakeErrorIssue(TIssuesIds::TIMEOUT, "Timeout occurred. Try repeating the request later") });
    }

    void Reply(const NYql::TIssues& issues) {
        TBase::SendErrorMessageToSender(new TProxyResponse(issues, Request->Get()->SubjectType), 0, Request->Cookie);
    }

    void ReplySuccess(const TAuditDetails<FederatedQuery::Binding>& audit) {
        Counters->Ok->Inc();
        Send(Request->Sender, new TProxyResponse(FederatedQuery::ModifyBindingResult{}, audit, Request->Get()->SubjectType), 0, Request->Cookie);
        PassAway();
    }

    void Fail(const TString& detail) {
        NYql::TIssues issues;
        issues.AddIssue(MakeErrorIssue(
            TIssuesIds::INTERNAL_ERROR, TStringBuilder() << "Couldn't resolve binding rename. Retry the same request. " << detail));
        Reply(issues);
    }

    TPermissions Permissions;
    std::unique_ptr<TPrivateClient> Compute;
    EBindingRenameAction StorageAction = EBindingRenameAction::None;
    EComputeAction ComputeAction = EComputeAction::Describe;
    TAuditDetails<FederatedQuery::Binding> CommitAudit;
    NYql::TIssues Failure;
};

NActors::IActor* MakeProbeBindingRenameActor(
    const TActorId& proxy, TProxyRequest::TPtr request, TCounters& counters, TDuration timeout, TPermissions permissions) {
    return new TRenameBindingActor(proxy, std::move(request), timeout, counters.GetCommonCounters(RTC_MODIFY_BINDING), permissions, {});
}

NActors::IActor* MakeRenameBindingActor(const TActorId& proxy, TProxyRequest::TPtr request, TCounters& counters, TDuration timeout,
    TPermissions permissions, const TComputeConfig& compute, const TYqSharedResources::TPtr& resources,
    const NKikimr::TYdbCredentialsProviderFactory& credentials) {
    auto settings = compute.GetSchemeConnection(request->Get()->Scope);
    const auto& connection = request->Get()->ComputeDatabase->connection();
    settings.set_endpoint(connection.endpoint());
    settings.set_database(connection.database());
    settings.set_usessl(connection.usessl());
    auto client =
        std::make_unique<TPrivateClient>(resources->UserSpaceYdbDriver, GetClientSettings<NYdb::TCommonClientSettings>(settings, credentials));
    return new TRenameBindingActor(
        proxy, std::move(request), timeout, counters.GetCommonCounters(RTC_MODIFY_BINDING), permissions, std::move(client));
}

}   // namespace NFq::NPrivate
