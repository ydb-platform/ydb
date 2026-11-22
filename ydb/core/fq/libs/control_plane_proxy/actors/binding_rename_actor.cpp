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
    auto& create = *(rename.lease_deadline_ms() ? request.mutable_leased_create() : request.mutable_create());
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

Fq::Private::ExternalTableMutationScope MutationScope(const TRename& rename) {
    Fq::Private::ExternalTableMutationScope scope;
    scope.set_principal(rename.compute_principal());
    scope.set_scheme_shard_id(rename.compute_scheme_shard_id());
    scope.set_domain_owner_id(rename.compute_domain_owner_id());
    scope.set_domain_path_id(rename.compute_domain_path_id());
    return scope;
}

bool HasValidIdentity(const Fq::Private::ExternalTableIdentity& identity) {
    return identity.owner_id() && identity.local_path_id() && identity.version();
}

}   // namespace

class TRenameBindingActor final: public TBaseActor<TRenameBindingActor> {
public:
    static constexpr char ActorName[] = "YQ_RENAME_BINDING";

    TRenameBindingActor(const TActorId& proxy, TProxyRequest::TPtr request, TDuration timeout, const TRequestCommonCountersPtr& counters,
        TPermissions permissions, std::unique_ptr<TPrivateClient> compute, bool recovery = false)
        : TBaseActor(proxy, std::move(request), timeout, counters)
        , Permissions(permissions)
        , Compute(std::move(compute))
        , Recovery(recovery)
    {
    }

    void BootstrapImpl() override {
        if (!Compute) {
            SendStorage(EBindingRenameAction::Probe);
        } else if (Request->Get()->Rename) {
            if (Recovery && Request->Get()->Rename->state() == TRename::PREPARED) {
                SendStorage(EBindingRenameAction::Expire);
            } else {
                Drive();
            }
        } else {
            if (!ValidateDestination(Request->Get()->Request.content())) {
                return Reply(Failure);
            }
            Fq::Private::DescribeExternalTableRequest request;
            request.set_resolve_mutation_scope(true);
            ComputeAction = EComputeAction::ResolveScope;
            SendDescribe(std::move(request));
        }
    }

    STRICT_STFUNC(StateFunc,
        cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(TStorageResponse, Handle);
        hFunc(TPrivateSchemaResponse, Handle);
    )

private:
    enum class EComputeAction {
        ResolveScope,
        Describe,
        Create,
        Commit,
        DropSource,
        Abort,
        Release,
    };

    void SendStorage(EBindingRenameAction action, const TString& identity = {}) {
        StorageAction = action;
        const auto& request = *Request->Get();
        auto event = std::make_unique<TStorageRequest>(request.Scope, request.Request, request.User, request.Token, request.CloudId, Permissions,
            request.Quotas, request.TenantInfo, request.ComputeDatabase.GetOrElse({}));
        event->RenameAction = action;
        event->RecoverRename = Recovery;
        if (action == EBindingRenameAction::Prepare) {
            event->ObservedRevision = request.OldBindingRevision;
            event->RenameConnectionName = request.ConnectionContent->name();
            event->RenameComputeScope = ComputeScope;
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
            if (audit.After) {
                CommitAudit = audit;
                CommitAudit.IdempotencyResult = true;
                return ReplySuccess(CommitAudit);
            }
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
        if (audit.After && !CommitAudit.After) {
            CommitAudit = audit;
            CommitAudit.IdempotencyResult = !audit.Before;
        }
        if (StorageAction == EBindingRenameAction::Finish) {
            if (!Recovery && request.Rename->state() == TRename::ABORTED) {
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
        if (rename.compute_endpoint() != connection.endpoint() || rename.compute_database() != connection.database() ||
            (rename.lease_deadline_ms() && rename.compute_ssl() != connection.usessl())) {
            return Fail("Compute database changed while the binding rename was pending");
        }
        switch (rename.state()) {
            case TRename::PREPARED: {
                if (!ValidateDestination(rename.destination())) {
                    return SendStorage(EBindingRenameAction::Abort);
                }
                if (rename.source_identity().empty()) {
                    Fq::Private::DescribeExternalTableRequest describe;
                    ComputeAction = EComputeAction::Describe;
                    if (rename.lease_deadline_ms()) {
                        describe.set_scoped_relative_path(rename.source_name());
                        *describe.mutable_expected_scope() = MutationScope(rename);
                    } else {
                        describe.set_relative_path(rename.source_name());
                    }
                    return SendDescribe(std::move(describe));
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
                auto* ticket = rename.lease_deadline_ms() ? commit.mutable_leased_commit() : commit.mutable_commit();
                ticket->set_key(rename.operation_id());
                return SendCompute(EComputeAction::Commit, std::move(commit));
            }
            case TRename::ABORTED: {
                Fq::Private::MutateExternalTableRequest abort;
                abort.set_relative_path(rename.destination().name());
                auto* ticket = rename.lease_deadline_ms() ? abort.mutable_leased_abort() : abort.mutable_abort();
                ticket->set_key(rename.operation_id());
                return SendCompute(EComputeAction::Abort, std::move(abort));
            }
            case TRename::FINISHED:
                if (Recovery) {
                    Fq::Private::MutateExternalTableRequest release;
                    release.set_relative_path(rename.destination().name());
                    release.mutable_release()->set_key(rename.operation_id());
                    return SendCompute(EComputeAction::Release, std::move(release));
                }
                CommitAudit.IdempotencyResult = true;
                return ReplySuccess(CommitAudit);
            default:
                return Fail("Unknown binding rename state");
        }
    }

    void SendDescribe(Fq::Private::DescribeExternalTableRequest request) {
        WaitFor(Compute->DescribeExternalTable(
            std::move(request), TDescribeExternalTableSettings().ClientTimeout(RequestTimeout).OperationTimeout(RequestTimeout)));
    }

    void SendCompute(EComputeAction action, Fq::Private::MutateExternalTableRequest request) {
        ComputeAction = action;
        const auto& rename = *Request->Get()->Rename;
        if (rename.lease_deadline_ms()) {
            *request.mutable_expected_scope() = MutationScope(rename);
        }
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
        const auto& response = *ev->Get();
        if (!response.Error.empty()) {
            return Fail(response.Error);
        }
        if (ComputeAction == EComputeAction::ResolveScope) {
            const auto& result = *response.Describe;
            if (!result.IsSuccess() || !result.IsResultSet() || !result.GetResult().has_mutation_scope()) {
                return Fail(result.GetIssues().ToOneLineString());
            }
            ComputeScope = result.GetResult().mutation_scope();
            if (!ComputeScope.scheme_shard_id() || ComputeScope.principal().empty() || !ComputeScope.domain_owner_id() ||
                !ComputeScope.domain_path_id()) {
                return Fail("Compute did not return a mutation scope");
            }
            return SendStorage(EBindingRenameAction::Prepare);
        }

        const auto& rename = *request.Rename;
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
                auto& identity = *(rename.lease_deadline_ms() ? drop.mutable_leased_guarded_drop() : drop.mutable_guarded_drop());
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
            case EComputeAction::Release:
                return ReplySuccess(CommitAudit);
            case EComputeAction::ResolveScope:
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
    const bool Recovery;
    Fq::Private::ExternalTableMutationScope ComputeScope;
    EBindingRenameAction StorageAction = EBindingRenameAction::None;
    EComputeAction ComputeAction = EComputeAction::Describe;
    TAuditDetails<FederatedQuery::Binding> CommitAudit;
    NYql::TIssues Failure;
};

NActors::IActor* MakeProbeBindingRenameActor(
    const TActorId& proxy, TProxyRequest::TPtr request, TCounters& counters, TDuration timeout, TPermissions permissions) {
    return new TRenameBindingActor(proxy, std::move(request), timeout, counters.GetCommonCounters(RTC_MODIFY_BINDING), permissions, {});
}

namespace {

std::unique_ptr<TPrivateClient> MakeRenameComputeClient(const TString& scope, const NFq::NConfig::TYdbStorageConfig& connection,
    const TComputeConfig& compute, const TYqSharedResources::TPtr& resources, const NKikimr::TYdbCredentialsProviderFactory& credentials) {
    auto settings = compute.GetSchemeConnection(scope);
    settings.set_endpoint(connection.endpoint());
    settings.set_database(connection.database());
    settings.set_usessl(connection.usessl());
    return std::make_unique<TPrivateClient>(
        resources->UserSpaceYdbDriver, GetClientSettings<NYdb::TCommonClientSettings>(settings, credentials));
}

}   // namespace

NActors::IActor* MakeRenameBindingActor(const TActorId& proxy, TProxyRequest::TPtr request, TCounters& counters, TDuration timeout,
    TPermissions permissions, const TComputeConfig& compute, const TYqSharedResources::TPtr& resources,
    const NKikimr::TYdbCredentialsProviderFactory& credentials) {
    auto client = MakeRenameComputeClient(request->Get()->Scope, request->Get()->ComputeDatabase->connection(), compute, resources, credentials);
    return new TRenameBindingActor(
        proxy, std::move(request), timeout, counters.GetCommonCounters(RTC_MODIFY_BINDING), permissions, std::move(client));
}

namespace {

class TBindingRenameRecoveryActor final: public TActorBootstrapped<TBindingRenameRecoveryActor> {
    using TWorkRequest = TEvControlPlaneStorage::TEvBindingRenameWorkRequest;
    using TWorkResponse = TEvControlPlaneStorage::TEvBindingRenameWorkResponse;
    using TDatabaseResponse = TEvControlPlaneStorage::TEvDescribeDatabaseResponse;

public:
    TBindingRenameRecoveryActor(TRequestCommonCountersPtr counters, TDuration timeout, const TComputeConfig& compute,
        const TYqSharedResources::TPtr& resources, const NKikimr::TYdbCredentialsProviderFactory& credentials)
        : Counters(std::move(counters))
        , Timeout(timeout)
        , Compute(compute)
        , Resources(resources)
        , Credentials(credentials)
    {
    }

    void Bootstrap() {
        Become(&TBindingRenameRecoveryActor::StateWork);
        ScheduleNext();
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TSystem::Poison, PassAway);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TWorkResponse, Handle);
        hFunc(TDatabaseResponse, Handle);
        hFunc(TProxyResponse, Handle);
    )

private:
    void ScheduleNext() {
        Work.Clear();
        ++Generation;
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    ui64 StartWait() {
        ++Generation;
        Schedule(Timeout, new TEvents::TEvWakeup(Generation));
        return Generation;
    }

    void SendStorage(IEventBase* event) {
        Send(ControlPlaneStorageServiceActorId(), event, IEventHandle::FlagTrackDelivery, StartWait());
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (!ev->Get()->Tag) {
            SendStorage(new TWorkRequest());
        } else if (ev->Get()->Tag == Generation) {
            ScheduleNext();
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Cookie == Generation) {
            ScheduleNext();
        }
    }

    void Handle(TWorkResponse::TPtr& ev) {
        if (ev->Cookie != Generation) {
            return;
        }
        const auto& response = *ev->Get();
        if (response.Issues || !response.Result.has_rename()) {
            return ScheduleNext();
        }
        Work = response.Result;
        const auto& controlPlane = Compute.GetProto().GetYdb().GetControlPlane();
        if (controlPlane.HasSingle()) {
            FederatedQuery::Internal::ComputeDatabaseInternal database;
            *database.mutable_connection() = controlPlane.GetSingle().GetConnection();
            return Recover(database);
        }

        SendStorage(new TEvControlPlaneStorage::TEvDescribeDatabaseRequest({}, Work.scope()));
    }

    void Handle(TDatabaseResponse::TPtr& ev) {
        if (ev->Cookie != Generation) {
            return;
        }
        const auto& response = *ev->Get();
        if (response.Issues) {
            return ScheduleNext();
        }
        Recover(response.Record);
    }

    void Recover(const FederatedQuery::Internal::ComputeDatabaseInternal& database) {
        const auto& rename = Work.rename();
        const auto& connection = database.connection();
        if (!rename.lease_deadline_ms() || rename.compute_principal().empty() || !rename.compute_scheme_shard_id() ||
            !rename.compute_domain_owner_id() || !rename.compute_domain_path_id() || rename.compute_endpoint() != connection.endpoint() ||
            rename.compute_database() != connection.database() || rename.compute_ssl() != connection.usessl()) {
            YDB_LOG_WARN_COMP(::NKikimrServices::YQ_CONTROL_PLANE_PROXY, "Unable to verify binding rename recovery route",
                {"operation_id", rename.operation_id()});
            return ScheduleNext();
        }

        std::unique_ptr<TPrivateClient> client;
        try {
            client = MakeRenameComputeClient(Work.scope(), connection, Compute, Resources, Credentials);
        } catch (...) {
            YDB_LOG_WARN_COMP(::NKikimrServices::YQ_CONTROL_PLANE_PROXY, "Unable to create binding rename recovery client",
                {"operation_id", rename.operation_id()});
            return ScheduleNext();
        }

        FederatedQuery::ModifyBindingRequest content;
        content.set_binding_id(Work.binding_id());
        content.set_idempotency_key(rename.idempotency_key());
        content.set_previous_revision(rename.source_revision());
        *content.mutable_content() = rename.destination();
        auto request = std::make_unique<TProxyRequest>(Work.scope(), content, rename.user(), "", TVector<TString>{});
        request->ComputeDatabase = database;
        request->Rename = rename;
        auto event = IEventHandle::Downcast<TProxyRequest>(
            TAutoPtr<IEventHandle>(new IEventHandle(SelfId(), SelfId(), request.release(), 0, StartWait())));
        Register(new TRenameBindingActor(SelfId(), std::move(event), Timeout, Counters, {}, std::move(client), true));
    }

    void Handle(TProxyResponse::TPtr& ev) {
        if (ev->Cookie != Generation) {
            return;
        }
        if (ev->Get()->Issues) {
            YDB_LOG_WARN_COMP(::NKikimrServices::YQ_CONTROL_PLANE_PROXY, "Binding rename recovery failed",
                {"operation_id", Work.rename().operation_id()}, {"issues", ev->Get()->Issues.ToOneLineString()});
            return ScheduleNext();
        }
        if (Work.rename().state() == TRename::FINISHED) {
            auto request = std::make_unique<TWorkRequest>();
            request->Request.mutable_rename()->set_operation_id(Work.rename().operation_id());
            SendStorage(request.release());
        } else {
            ScheduleNext();
        }
    }

    const TRequestCommonCountersPtr Counters;
    const TDuration Timeout;
    const TComputeConfig Compute;
    const TYqSharedResources::TPtr Resources;
    const NKikimr::TYdbCredentialsProviderFactory Credentials;
    FederatedQuery::Internal::BindingRenameWork Work;
    ui64 Generation = 0;
};

}   // namespace

NActors::IActor* MakeBindingRenameRecoveryActor(TCounters& counters, TDuration timeout, const TComputeConfig& compute,
    const TYqSharedResources::TPtr& resources, const NKikimr::TYdbCredentialsProviderFactory& credentials) {
    return new TBindingRenameRecoveryActor(counters.GetCommonCounters(RTC_MODIFY_BINDING), timeout, compute, resources, credentials);
}

}   // namespace NFq::NPrivate
