#include "binding_rename.h"
#include "ydb_control_plane_storage_impl.h"

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/validators.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT ::NKikimrServices::YQ_CONTROL_PLANE_STORAGE

namespace NFq {

namespace {

void EnqueueBindingRename(
    TSqlQueryBuilder& builder, const TString& scope, const TString& bindingId, const TBindingRename& rename, bool releasable) {
    if (!rename.lease_deadline_ms()) {
        return;
    }
    FederatedQuery::Internal::BindingRenameWork work;
    work.set_scope(scope);
    work.set_binding_id(bindingId);
    *work.mutable_rename() = rename;
    if (releasable) {
        work.mutable_rename()->set_state(TBindingRename::FINISHED);
    }
    const auto nextAttempt = rename.state() == TBindingRename::PREPARED ? TInstant::MilliSeconds(rename.lease_deadline_ms()) : TInstant::Now();
    builder.AddString("rename_operation_id", rename.operation_id());
    builder.AddString("rename_scope", scope);
    builder.AddString("rename_work", work.SerializeAsString());
    builder.AddTimestamp("rename_next_attempt", nextAttempt);
    builder.AddBool("rename_releasable", releasable);
    builder.AddText("UPSERT INTO `" BINDING_RENAME_WORK_TABLE_NAME "` (`" OPERATION_ID_COLUMN_NAME "`, `" SCOPE_COLUMN_NAME
                    "`, `" NEXT_ATTEMPT_COLUMN_NAME "`, `" RELEASABLE_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME
                    "`) "
                    "VALUES ($rename_operation_id, $rename_scope, $rename_next_attempt, $rename_releasable, $rename_work);");
}

}   // namespace

NYql::TIssues TControlPlaneStorageBase::ValidateRequest(TEvControlPlaneStorage::TEvCreateBindingRequest::TPtr& ev) const {
    NYql::TIssues issues = ValidateBinding(ev);

    const auto& event = *ev->Get();
    const auto& permissions = GetCreateBindingPerimssions(event);
    if (event.Request.content().acl().visibility() == FederatedQuery::Acl::SCOPE && !permissions.Check(TPermissions::MANAGE_PUBLIC)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED,
            "Permission denied to create a binding with these parameters. Please receive a permission yq.resources.managePublic"));
    }

    return issues;
}

TPermissions TControlPlaneStorageBase::GetCreateBindingPerimssions(const TEvControlPlaneStorage::TEvCreateBindingRequest& event) const {
    TPermissions permissions = Config->Proto.GetEnablePermissions() ? event.Permissions : TPermissions{ TPermissions::MANAGE_PUBLIC };
    if (IsSuperUser(event.User)) {
        permissions.SetAll();
    }
    return permissions;
}

std::pair<FederatedQuery::Binding, FederatedQuery::Internal::BindingInternal> TControlPlaneStorageBase::GetCreateBindingProtos(
    const FederatedQuery::CreateBindingRequest& request, const TString& cloudId, const TString& user, TInstant startTime) const {
    const TString& bindingId = GetEntityIdAsString(Config->IdsPrefix, EEntityType::BINDING);

    FederatedQuery::Binding binding;
    FederatedQuery::BindingContent& content = *binding.mutable_content();
    content = request.content();
    *binding.mutable_meta() = CreateCommonMeta(bindingId, user, startTime, InitialRevision);

    FederatedQuery::Internal::BindingInternal bindingInternal;
    bindingInternal.set_cloud_id(cloudId);

    return { binding, bindingInternal };
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvCreateBindingRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvCreateBindingRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_BINDING, RTC_CREATE_BINDING);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    const FederatedQuery::CreateBindingRequest& request = event.Request;
    int byteSize = request.ByteSize();
    const TString connectionId = request.content().connection_id();
    const TString idempotencyKey = request.idempotency_key();

    const auto [binding, bindingInternal] = GetCreateBindingProtos(request, cloudId, user, startTime);
    const auto& content = binding.content();
    const TString& bindingId = binding.meta().id();

    YDB_LOG_TRACE("Dump logPrefix, createBindingRequest, request",
        {"logPrefix", MakeLogPrefix(scope, user, bindingId)},
        {"createBindingRequest", NKikimr::MaskTicket(token)},
        {"request", request.DebugString()});

    if (const auto& issues = ValidateRequest(ev)) {
        YDB_LOG_DEBUG("CreateBindingRequest, validation",
            {"logPrefix", MakeLogPrefix(scope, user, bindingId)},
            {"failed", NKikimr::MaskTicket(token)},
            {"request", request.DebugString()},
            {"error", issues});
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvCreateBindingResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(CreateBindingRequest, scope, user, delta, byteSize, false);
        return;
    }

    std::shared_ptr<std::pair<FederatedQuery::CreateBindingResult, TAuditDetails<FederatedQuery::Binding>>> response =
        std::make_shared<std::pair<FederatedQuery::CreateBindingResult, TAuditDetails<FederatedQuery::Binding>>>();
    response->first.set_binding_id(bindingId);
    response->second.After.ConstructInPlace().CopyFrom(binding);
    response->second.CloudId = cloudId;

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "CreateBinding");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("binding_id", bindingId);
    queryBuilder.AddString("connection_id", connectionId);
    queryBuilder.AddString("user", user);
    queryBuilder.AddInt64("visibility", content.acl().visibility());
    queryBuilder.AddString("name", content.name());
    queryBuilder.AddString("binding", binding.SerializeAsString());
    queryBuilder.AddInt64("revision", InitialRevision);
    queryBuilder.AddString("internal", bindingInternal.SerializeAsString());

    InsertIdempotencyKey(queryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), startTime + Config->IdempotencyKeyTtl);
    queryBuilder.AddText("INSERT INTO `" BINDINGS_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" BINDING_ID_COLUMN_NAME
                         "`, `" CONNECTION_ID_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`, `" NAME_COLUMN_NAME
                         "`, `" BINDING_COLUMN_NAME "`, `" REVISION_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME
                         "`) VALUES\n"
                         "    ($scope, $binding_id, $connection_id, $user, $visibility, $name, $binding, $revision, $internal);");

    auto connectionNameUniqueValidator = CreateUniqueNameValidator(CONNECTIONS_TABLE_NAME, content.acl().visibility(), scope, content.name(),
        user, "Connection with the same name already exists. Please choose another name", YdbConnection->TablePathPrefix);

    auto bindingNameUniqueValidator = CreateUniqueNameValidator(BINDINGS_TABLE_NAME, content.acl().visibility(), scope, content.name(), user,
        "Binding with the same name already exists. Please choose another name", YdbConnection->TablePathPrefix);

    auto validatorCountBindings = CreateCountEntitiesValidator(scope, BINDINGS_TABLE_NAME, Config->Proto.GetMaxCountBindings(),
        "Too many bindings in folder: " + ToString(Config->Proto.GetMaxCountBindings()) + ". Please remove unused bindings",
        YdbConnection->TablePathPrefix);

    auto validatorConnectionExists = CreateConnectionExistsValidator(scope, connectionId,
        "Connection " + connectionId + " does not exist or permission denied. Please check the id connection or your access rights",
        GetCreateBindingPerimssions(event), user, content.acl().visibility(), YdbConnection->TablePathPrefix);

    auto connectionValidator = CreateBindingConnectionValidator(scope, connectionId, user, YdbConnection->TablePathPrefix);

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(
            scope, idempotencyKey, response, YdbConnection->TablePathPrefix, requestCounters.Common->ParseProtobufError));
    }

    validators.push_back(connectionNameUniqueValidator);
    validators.push_back(bindingNameUniqueValidator);
    validators.push_back(validatorCountBindings);
    validators.push_back(validatorConnectionExists);
    validators.push_back(connectionValidator);

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    TAsyncStatus result = Write(query.Sql, query.Params, requestCounters, debugInfo, validators);
    auto prepare = [response] {
        return *response;
    };
    auto success = SendResponse<TEvControlPlaneStorage::TEvCreateBindingResponse, FederatedQuery::CreateBindingResult>(
        MakeLogPrefix(scope, user, bindingId) + "CreateBindingRequest", NActors::TActivationContext::ActorSystem(), result, SelfId(),
        std::move(ev), startTime, requestCounters, prepare, debugInfo);

    success.Apply([=](const auto& future) {
        TDuration delta = TInstant::Now() - startTime;
        LWPROBE(CreateBindingRequest, scope, user, delta, byteSize, future.GetValue());
    });
}

NYql::TIssues TControlPlaneStorageBase::ValidateRequest(TEvControlPlaneStorage::TEvListBindingsRequest::TPtr& ev) const {
    return ValidateEvent(ev);
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvListBindingsRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvListBindingsRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_BINDINGS, RTC_LIST_BINDINGS);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::ListBindingsRequest& request = event.Request;
    const TString user = event.User;
    const TString pageToken = request.page_token();
    const int byteSize = event.Request.ByteSize();
    const int64_t limit = request.limit();
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions() ? event.Permissions : TPermissions{ TPermissions::VIEW_PUBLIC };
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }

    YDB_LOG_TRACE("Dump logPrefix, listBindingsRequest, request",
        {"logPrefix", MakeLogPrefix(scope, user)},
        {"listBindingsRequest", NKikimr::MaskTicket(token)},
        {"request", request.DebugString()});

    if (const auto& issues = ValidateRequest(ev)) {
        YDB_LOG_DEBUG("ListBindingsRequest, validation",
            {"logPrefix", MakeLogPrefix(scope, user)},
            {"failed", NKikimr::MaskTicket(token)},
            {"request", request.DebugString()},
            {"error", issues});
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvListBindingsResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ListBindingsRequest, scope, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "ListBindings");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("last_binding", pageToken);
    queryBuilder.AddUint64("limit", limit + 1);

    queryBuilder.AddText("SELECT `" SCOPE_COLUMN_NAME "`, `" BINDING_ID_COLUMN_NAME "`, `" BINDING_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME
                         "`\n"
                         "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` >= $last_binding\n");

    TString filter;
    if (request.has_filter()) {
        TVector<TString> filters;
        if (request.filter().connection_id()) {
            queryBuilder.AddString("connection_id", request.filter().connection_id());
            filters.push_back("`" CONNECTION_ID_COLUMN_NAME "` == $connection_id");
        }

        if (request.filter().name()) {
            queryBuilder.AddString("filter_name", request.filter().name());
            if (event.IsExactNameMatch) {
                filters.push_back("`" NAME_COLUMN_NAME "` = $filter_name");
            } else {
                filters.push_back("FIND(`" NAME_COLUMN_NAME "`, $filter_name) IS NOT NULL");
            }
        }

        if (request.filter().created_by_me()) {
            queryBuilder.AddString("user", user);
            filters.push_back("`" USER_COLUMN_NAME "` = $user");
        }

        if (request.filter().visibility() != FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
            queryBuilder.AddInt64("visibility", request.filter().visibility());
            filters.push_back("`" VISIBILITY_COLUMN_NAME "` = $visibility");
        }

        filter = JoinSeq(" AND ", filters);
    }

    PrepareViewAccessCondition(queryBuilder, permissions, user);

    if (filter) {
        queryBuilder.AddText(" AND (" + filter + ")");
    }

    queryBuilder.AddText("ORDER BY `" SCOPE_COLUMN_NAME "`, `" BINDING_ID_COLUMN_NAME
                         "`\n"
                         "LIMIT $limit;");

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets = resultSets, limit, commonCounters = requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR)
                << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        FederatedQuery::ListBindingsResult result;
        TResultSetParser parser(resultSets->front());
        while (parser.TryNextRow()) {
            FederatedQuery::Binding binding;
            if (!binding.ParseFromString(*parser.ColumnParser(BINDING_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR)
                    << "Error parsing proto message for binding. Please contact internal support";
            }
            FederatedQuery::BriefBinding& briefBinding = *result.add_binding();
            briefBinding.set_name(binding.content().name());
            briefBinding.set_connection_id(binding.content().connection_id());
            *briefBinding.mutable_meta() = binding.meta();
            switch (binding.content().setting().binding_case()) {
                case FederatedQuery::BindingSetting::kDataStreams: {
                    briefBinding.set_type(FederatedQuery::BindingSetting::DATA_STREAMS);
                    break;
                }
                case FederatedQuery::BindingSetting::kObjectStorage: {
                    briefBinding.set_type(FederatedQuery::BindingSetting::OBJECT_STORAGE);
                    break;
                }
                // Do not replace with default. Adding a new binding should cause a compilation error
                case FederatedQuery::BindingSetting::BINDING_NOT_SET:
                    break;
            }
            briefBinding.set_visibility(binding.content().acl().visibility());
        }

        if (result.binding_size() == limit + 1) {
            result.set_next_page_token(result.binding(result.binding_size() - 1).meta().id());
            result.mutable_binding()->RemoveLast();
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvListBindingsResponse, FederatedQuery::ListBindingsResult>(
        MakeLogPrefix(scope, user) + "ListBindingsRequest", NActors::TActivationContext::ActorSystem(), result, SelfId(), std::move(ev),
        startTime, requestCounters, prepare, debugInfo);

    success.Apply([=](const auto& future) {
        TDuration delta = TInstant::Now() - startTime;
        LWPROBE(ListBindingsRequest, scope, user, delta, byteSize, future.GetValue());
    });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDescribeBindingRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDescribeBindingRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_BINDING, RTC_DESCRIBE_BINDING);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::DescribeBindingRequest& request = event.Request;
    const TString bindingId = request.binding_id();
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions() ? event.Permissions : TPermissions{ TPermissions::VIEW_PUBLIC };
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int byteSize = request.ByteSize();
    YDB_LOG_TRACE("Dump logPrefix, describeBindingRequest, request",
        {"logPrefix", MakeLogPrefix(scope, user, bindingId)},
        {"describeBindingRequest", NKikimr::MaskTicket(token)},
        {"request", request.DebugString()});
    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        YDB_LOG_DEBUG("DescribeBindingRequest, validation",
            {"logPrefix", MakeLogPrefix(scope, user, bindingId)},
            {"failed", NKikimr::MaskTicket(token)},
            {"request", request.DebugString()},
            {"error", issues});
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDescribeBindingResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DescribeBindingRequest, scope, bindingId, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DescribeBinding");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("binding_id", bindingId);
    queryBuilder.AddText("SELECT `" BINDING_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME
                         "`\n"
                         "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;");

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [permissions, user, resultSets = resultSets, commonCounters = requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR)
                << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::ACCESS_DENIED)
                << "Binding does not exist or permission denied. Please check the id binding or your access rights";
        }

        FederatedQuery::DescribeBindingResult result;
        if (!result.mutable_binding()->ParseFromString(*parser.ColumnParser(BINDING_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR)
                << "Error parsing proto message for binding. Please contact internal support";
        }

        bool hasViewAccess =
            HasViewAccess(permissions, result.binding().content().acl().visibility(), result.binding().meta().created_by(), user);
        if (!hasViewAccess) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::ACCESS_DENIED)
                << "Binding does not exist or permission denied. Please check the id binding or your access rights";
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvDescribeBindingResponse, FederatedQuery::DescribeBindingResult>(
        MakeLogPrefix(scope, user, bindingId) + "DescribeBindingRequest", NActors::TActivationContext::ActorSystem(), result, SelfId(),
        std::move(ev), startTime, requestCounters, prepare, debugInfo);

    success.Apply([=](const auto& future) {
        TDuration delta = TInstant::Now() - startTime;
        LWPROBE(DescribeBindingRequest, scope, user, bindingId, delta, byteSize, future.GetValue());
    });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvModifyBindingRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvModifyBindingRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_BINDING, RTC_MODIFY_BINDING);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::ModifyBindingRequest& request = event.Request;
    const TString bindingId = request.binding_id();
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions() ? event.Permissions : TPermissions{ TPermissions::MANAGE_PUBLIC };
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const TString idempotencyKey = request.idempotency_key();
    const auto renameAction = event.RenameAction;
    const bool clientRequest = renameAction == EBindingRenameAction::None || renameAction == EBindingRenameAction::Prepare;
    const bool requiresAccess = clientRequest || renameAction == EBindingRenameAction::Commit;
    auto viewPermissions = Config->Proto.GetEnablePermissions() ? event.Permissions : TPermissions{ TPermissions::VIEW_PUBLIC };
    if (IsSuperUser(user)) {
        viewPermissions.SetAll();
    }
    const bool checkCommitKey = renameAction == EBindingRenameAction::Commit && !idempotencyKey.empty();
    const auto leaseDeadlineMs = renameAction == EBindingRenameAction::Prepare && event.RenameComputeScope.scheme_shard_id()
                                     ? (startTime + TDuration::Minutes(30)).MilliSeconds()
                                     : 0;
    TString operationId = event.RenameOperationId;
    if (renameAction == EBindingRenameAction::Prepare) {
        operationId = CreateGuidAsString();
        if (leaseDeadlineMs) {
            operationId = TStringBuilder() << "fqv1:" << leaseDeadlineMs << ':' << operationId;
        }
    }
    const int byteSize = request.ByteSize();
    YDB_LOG_TRACE("Dump logPrefix, modifyBindingRequest, request",
        {"logPrefix", MakeLogPrefix(scope, user, bindingId)},
        {"modifyBindingRequest", NKikimr::MaskTicket(token)},
        {"request", request.DebugString()});

    NYql::TIssues issues;
    if (event.RecoverRename) {
        if (renameAction != EBindingRenameAction::Expire && renameAction != EBindingRenameAction::Finish) {
            issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Invalid binding rename recovery action"));
        }
    } else {
        issues = requiresAccess ? ValidateBinding(ev) : ValidateEvent(ev);
    }
    const auto& computeScope = event.RenameComputeScope;
    if (renameAction == EBindingRenameAction::Prepare &&
        (!computeScope.principal().empty() || computeScope.scheme_shard_id() || computeScope.domain_owner_id() ||
            computeScope.domain_path_id()) &&
        (computeScope.principal().empty() || !computeScope.scheme_shard_id() || !computeScope.domain_owner_id() ||
            !computeScope.domain_path_id())) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, "Incomplete Compute mutation scope"));
    }
    if (issues) {
        YDB_LOG_DEBUG("ModifyBindingRequest, validation",
            {"logPrefix", MakeLogPrefix(scope, user, bindingId)},
            {"failed", NKikimr::MaskTicket(token)},
            {"request", request.DebugString()},
            {"error", issues});
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvModifyBindingResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ModifyBindingRequest, scope, bindingId, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "ModifyBinding(read)");
    readQueryBuilder.AddString("scope", scope);
    readQueryBuilder.AddString("binding_id", bindingId);
    readQueryBuilder.AddText(
        "$selected = SELECT `" BINDING_COLUMN_NAME "`, `" CONNECTION_ID_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME
        "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME
        "` = $binding_id;\n"
        "$connection_id = SELECT `" CONNECTION_ID_COLUMN_NAME
        "` FROM $selected;\n"
        "SELECT `" BINDING_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME
        "` FROM $selected;\n"
        "SELECT `" VISIBILITY_COLUMN_NAME "`, `" CONNECTION_COLUMN_NAME "` FROM `" CONNECTIONS_TABLE_NAME
        "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" CONNECTION_ID_COLUMN_NAME "` = $connection_id;");

    if (checkCommitKey) {
        ReadIdempotencyKeyQuery(readQueryBuilder, scope, idempotencyKey);
    }

    using TResponse = std::pair<FederatedQuery::ModifyBindingResult, TBindingModifyAuditDetails>;
    auto response = std::make_shared<TResponse>();
    auto prepareParams = [response, permissions, viewPermissions, bindingId, idempotencyKey, user, scope, request = request,
                             observedRevision = event.ObservedRevision, renameAction, clientRequest, requiresAccess, checkCommitKey, operationId,
                             identity = event.RenameIdentity, connectionName = event.RenameConnectionName,
                             computeEndpoint = event.ComputeDatabase.connection().endpoint(),
                             computeDatabase = event.ComputeDatabase.connection().database(), tablePathPrefix = YdbConnection->TablePathPrefix,
                             idempotencyKeyTtl = Config->IdempotencyKeyTtl, leaseDeadlineMs, computeScope,
                             computeSsl = event.ComputeDatabase.connection().usessl(),
                             commonCounters = requestCounters.Common](const std::vector<TResultSet>& resultSets) {
        response->second = {};
        if (resultSets.size() != (checkCommitKey ? 3 : 2)) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Unexpected number of result sets for binding update";
        }

        FederatedQuery::Binding binding;
        FederatedQuery::Internal::BindingInternal bindingInternal;
        {
            TResultSetParser parser(resultSets.front());
            if (!parser.TryNextRow()) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::ACCESS_DENIED)
                    << "Binding does not exist or permission denied. Please check the binding id or your access rights";
            }

            if (!binding.ParseFromString(*parser.ColumnParser(BINDING_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR)
                    << "Error parsing proto message for binding. Please contact internal support";
            }
            if (!bindingInternal.ParseFromString(parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString().value_or(""))) {
                commonCounters->ParseProtobufError->Inc();
                ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing binding internal metadata";
            }
        }

        const auto& rename = bindingInternal.rename();
        const auto renameState = rename.state();
        const bool matches = bindingInternal.has_rename() && MatchesBindingRename(rename, request, user);
        auto setCommittedAudit = [&] {
            response->second.After = binding;
            response->second.CloudId = bindingInternal.cloud_id();
        };
        if (renameAction == EBindingRenameAction::Probe) {
            if (!matches && !HasViewAccess(viewPermissions, binding.content().acl().visibility(), binding.meta().created_by(), user)) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::ACCESS_DENIED)
                    << "Binding does not exist or permission denied. Please check the binding id or your access rights";
            }
            if (matches) {
                response->second.Rename = rename;
            }
            if (matches && renameState == TBindingRename::FINISHED) {
                setCommittedAudit();
            } else {
                response->second.Before = binding;
            }
            return std::make_pair(TString{}, NYdb::TParamsBuilder{}.Build());
        }

        const auto requestBindingVisibility = request.content().acl().visibility();
        if (requiresAccess) {
            if (!HasManageAccess(permissions, binding.content().acl().visibility(), binding.meta().created_by(), user)) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::ACCESS_DENIED)
                    << "Binding does not exist or permission denied. Please check the id binding or your access rights";
            }
            TResultSetParser parser(resultSets[1]);
            if (!parser.TryNextRow()) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::ACCESS_DENIED)
                    << "Connection does not exist or permission denied. Please check the connection id or your access rights";
            }
            const auto visibility = static_cast<FederatedQuery::Acl::Visibility>(
                parser.ColumnParser(VISIBILITY_COLUMN_NAME).GetOptionalInt64().value_or(FederatedQuery::Acl::VISIBILITY_UNSPECIFIED));
            if (requestBindingVisibility == FederatedQuery::Acl::SCOPE && visibility == FederatedQuery::Acl::PRIVATE) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST)
                    << "Binding with SCOPE visibility cannot refer to connection with PRIVATE visibility";
            }
            if (renameAction == EBindingRenameAction::Prepare || renameAction == EBindingRenameAction::Commit) {
                FederatedQuery::Connection connection;
                if (!connection.ParseFromString(parser.ColumnParser(CONNECTION_COLUMN_NAME).GetOptionalString().value_or(""))) {
                    commonCounters->ParseProtobufError->Inc();
                    ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing connection metadata";
                }
                const auto& expected = renameAction == EBindingRenameAction::Commit ? rename.connection_name() : connectionName;
                if (expected.empty() || expected != connection.content().name()) {
                    ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "Binding rename connection has changed";
                }
            }
        }

        auto noWrite = [] {
            return std::make_pair(TString{}, NYdb::TParamsBuilder{}.Build());
        };
        auto writeInternal = [&](const TBindingRename& work, bool releasable) {
            TSqlQueryBuilder builder(tablePathPrefix, "ModifyBinding(rename)");
            builder.AddString("scope", scope);
            builder.AddString("binding_id", bindingId);
            builder.AddString("internal", bindingInternal.SerializeAsString());
            builder.AddText("UPDATE `" BINDINGS_TABLE_NAME "` SET `" INTERNAL_COLUMN_NAME
                            "` = $internal"
                            " WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;");
            EnqueueBindingRename(builder, scope, bindingId, work, releasable);
            auto query = builder.Build();
            return std::make_pair(query.Sql, query.Params);
        };
        auto returnRename = [&] {
            if (renameState == TBindingRename::COMMITTED || renameState == TBindingRename::FINISHED) {
                setCommittedAudit();
            } else {
                response->second.Before = binding;
            }
            response->second.Rename = bindingInternal.rename();
        };

        if (renameAction == EBindingRenameAction::Prepare && matches) {
            returnRename();
            return noWrite();
        }
        if (clientRequest && HasPendingBindingRename(bindingInternal)) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "A different rename is pending for this binding";
        }
        if (!clientRequest) {
            if (!matches) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "Binding rename request has changed";
            }
            const auto effect = AdvanceBindingRename(*bindingInternal.mutable_rename(), renameAction, operationId, identity, binding);
            returnRename();
            switch (effect) {
                case EBindingRenameWrite::None:
                    return noWrite();
                case EBindingRenameWrite::Forget:
                    bindingInternal.clear_rename();
                    return writeInternal(*response->second.Rename, true);
                case EBindingRenameWrite::Journal:
                    return writeInternal(*response->second.Rename, renameAction == EBindingRenameAction::Finish);
                case EBindingRenameWrite::Binding:
                    if (checkCommitKey && TResultSetParser(resultSets[2]).TryNextRow()) {
                        ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST)
                            << "The idempotency key has been used by a different operation";
                    }
                    break;
            }
        } else if ((observedRevision && binding.meta().revision() != *observedRevision) ||
                   (request.previous_revision() > 0 && binding.meta().revision() != request.previous_revision())) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST)
                << "Revision of the binding has been changed already. Please restart the request with a new revision";
        }

        auto& content = *binding.mutable_content();

        bool validateType = content.setting().binding_case() == request.content().setting().binding_case();

        if (!validateType) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST)
                << "Binding type cannot be changed. Please specify the same binding type";
        }

        if (binding.content().acl().visibility() == FederatedQuery::Acl::SCOPE && requestBindingVisibility == FederatedQuery::Acl::PRIVATE) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST)
                << "Changing visibility from SCOPE to PRIVATE is forbidden. Please create a new binding with visibility PRIVATE";
        }

        if (content.connection_id() != request.content().connection_id()) {
            ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST)
                << "Connection id cannot be changed. Please specify the same connection id";
        }

        if (renameAction == EBindingRenameAction::Prepare) {
            if (content.name() == request.content().name()) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "A binding rename must change the name";
            }
            auto& intent = *bindingInternal.mutable_rename();
            intent.Clear();
            intent.set_operation_id(operationId);
            intent.set_source_name(content.name());
            intent.set_source_revision(binding.meta().revision());
            *intent.mutable_destination() = request.content();
            intent.set_idempotency_key(idempotencyKey);
            intent.set_user(user);
            intent.set_connection_name(connectionName);
            intent.set_compute_endpoint(computeEndpoint);
            intent.set_compute_database(computeDatabase);
            intent.set_compute_ssl(computeSsl);
            intent.set_lease_deadline_ms(leaseDeadlineMs);
            intent.set_compute_principal(computeScope.principal());
            intent.set_compute_scheme_shard_id(computeScope.scheme_shard_id());
            intent.set_compute_domain_owner_id(computeScope.domain_owner_id());
            intent.set_compute_domain_path_id(computeScope.domain_path_id());
            returnRename();
            return writeInternal(intent, false);
        }

        if (renameAction != EBindingRenameAction::Commit) {
            bindingInternal.clear_rename();
        }

        auto& meta = *binding.mutable_meta();
        meta.set_revision(meta.revision() + 1);
        meta.set_modified_by(user);
        *meta.mutable_modified_at() = NProtoInterop::CastToProto(TInstant::Now());
        content = request.content();

        response->second.After.ConstructInPlace().CopyFrom(binding);
        response->second.CloudId = bindingInternal.cloud_id();

        TSqlQueryBuilder writeQueryBuilder(tablePathPrefix, "ModifyBinding(write)");
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddString("binding_id", bindingId);
        writeQueryBuilder.AddInt64("visibility", binding.content().acl().visibility());
        writeQueryBuilder.AddString("name", binding.content().name());
        writeQueryBuilder.AddInt64("revision", meta.revision());
        writeQueryBuilder.AddString("internal", bindingInternal.SerializeAsString());
        writeQueryBuilder.AddString("binding", binding.SerializeAsString());
        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), TInstant::Now() + idempotencyKeyTtl);
        writeQueryBuilder.AddText(
            "UPDATE `" BINDINGS_TABLE_NAME "` SET `" VISIBILITY_COLUMN_NAME "` = $visibility, `" NAME_COLUMN_NAME
            "` = $name, `" REVISION_COLUMN_NAME "` = $revision, `" INTERNAL_COLUMN_NAME "` = $internal, `" BINDING_COLUMN_NAME
            "` = $binding\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;\n");
        if (renameAction == EBindingRenameAction::Commit) {
            EnqueueBindingRename(writeQueryBuilder, scope, bindingId, bindingInternal.rename(), false);
        }
        const auto writeQuery = writeQueryBuilder.Build();
        return make_pair(writeQuery.Sql, writeQuery.Params);
    };

    TVector<TValidationQuery> validators;
    if (clientRequest && idempotencyKey) {
        TSqlQueryBuilder builder(YdbConnection->TablePathPrefix, "ModifyBinding(idempotency)");
        ReadIdempotencyKeyQuery(builder, scope, idempotencyKey);
        builder.AddString("binding_id", bindingId);
        builder.AddText("SELECT `" INTERNAL_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME
                        "`"
                        " WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;");
        const auto query = builder.Build();
        validators.push_back({ query.Sql, query.Params,
            [response, renameAction, idempotencyKey, parseProtobufError = requestCounters.Common->ParseProtobufError](
                NYdb::NTable::TDataQueryResult result) {
                const auto& resultSets = result.GetResultSets();
                if (resultSets.size() != 2) {
                    ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Unexpected binding idempotency result";
                }
                TResultSetParser idempotency(resultSets.front());
                if (!idempotency.TryNextRow()) {
                    return false;
                }
                TResultSetParser binding(resultSets.back());
                if (binding.TryNextRow()) {
                    FederatedQuery::Internal::BindingInternal internal;
                    if (!internal.ParseFromString(binding.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString().value_or(""))) {
                        parseProtobufError->Inc();
                        ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing binding internal metadata";
                    }
                    if (HasPendingBindingRename(internal) &&
                        (renameAction == EBindingRenameAction::None || internal.rename().idempotency_key() == idempotencyKey)) {
                        return false;
                    }
                }
                if (!response->first.ParseFromString(*idempotency.ColumnParser(RESPONSE_COLUMN_NAME).GetOptionalString())) {
                    parseProtobufError->Inc();
                    ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing binding idempotency result";
                }
                response->second = {};
                response->second.IdempotencyResult = true;
                return true;
            } });
    }

    if (requiresAccess) {
        auto accessValidator = CreateManageAccessValidator(BINDINGS_TABLE_NAME, BINDING_ID_COLUMN_NAME, scope, bindingId, user,
            "Binding does not exist or permission denied. Please check the id binding or your access rights", permissions,
            YdbConnection->TablePathPrefix);
        validators.push_back(accessValidator);

        auto connectionNameUniqueValidator =
            CreateUniqueNameValidator(CONNECTIONS_TABLE_NAME, request.content().acl().visibility(), scope, request.content().name(), user,
                "Connection with the same name already exists. Please choose another name", YdbConnection->TablePathPrefix);
        validators.push_back(connectionNameUniqueValidator);
        auto bindingNameUniqueValidator = CreateModifyUniqueNameValidator(BINDINGS_TABLE_NAME, BINDING_ID_COLUMN_NAME,
            request.content().acl().visibility(), scope, request.content().name(), user, bindingId,
            "Binding with the same name already exists. Please choose another name", YdbConnection->TablePathPrefix);
        validators.push_back(bindingNameUniqueValidator);
    }

    const auto readQuery = readQueryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(readQuery.Sql, readQuery.Params, prepareParams, requestCounters, debugInfo, validators);
    auto prepare = [response] {
        return *response;
    };
    auto success = SendResponse<TEvControlPlaneStorage::TEvModifyBindingResponse, FederatedQuery::ModifyBindingResult>(
        MakeLogPrefix(scope, user, bindingId) + "ModifyBindingRequest", NActors::TActivationContext::ActorSystem(), result, SelfId(),
        std::move(ev), startTime, requestCounters, prepare, debugInfo);

    success.Apply([=](const auto& future) {
        TDuration delta = TInstant::Now() - startTime;
        LWPROBE(ModifyBindingRequest, scope, user, bindingId, delta, byteSize, future.GetValue());
    });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvBindingRenameWorkRequest::TPtr& ev) {
    const auto now = TInstant::Now();
    const auto& operationId = ev->Get()->Request.rename().operation_id();
    auto counters = Counters.GetCounters("", "", RTS_MAX, RTC_BINDING_RENAME_WORK);
    counters.IncInFly();
    auto result = std::make_shared<FederatedQuery::Internal::BindingRenameWork>();
    TAsyncStatus status;
    if (operationId) {
        TSqlQueryBuilder builder(TablePathPrefix, "AcknowledgeBindingRenameRelease");
        builder.AddString("operation_id", operationId);
        builder.AddText("DELETE FROM `" BINDING_RENAME_WORK_TABLE_NAME "` WHERE `" OPERATION_ID_COLUMN_NAME
                        "` = $operation_id AND `" RELEASABLE_COLUMN_NAME "`;");
        const auto query = builder.Build();
        status = Write(query.Sql, query.Params, counters, {});
    } else {
        TSqlQueryBuilder builder(TablePathPrefix, "ClaimBindingRename");
        builder.AddTimestamp("now", now);
        builder.AddText("SELECT `" OPERATION_ID_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "` FROM `" BINDING_RENAME_WORK_TABLE_NAME
                        "` VIEW by_next_attempt WHERE `" NEXT_ATTEMPT_COLUMN_NAME "` <= $now ORDER BY `" NEXT_ATTEMPT_COLUMN_NAME
                        "`, `" OPERATION_ID_COLUMN_NAME "` LIMIT 1;");
        const auto query = builder.Build();
        status = ReadModifyWrite(query.Sql, query.Params, [result, now, tablePathPrefix = TablePathPrefix](const auto& sets) {
            result->Clear();
            TResultSetParser parser(sets.at(0));
            if (!parser.TryNextRow()) {
                return std::make_pair(TString{}, NYdb::TParamsBuilder{}.Build());
            }
            const auto operationId = parser.ColumnParser(OPERATION_ID_COLUMN_NAME).GetOptionalString().value_or("");
            if (!result->ParseFromString(parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString().value_or("")) || operationId.empty() ||
                result->rename().operation_id() != operationId) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Invalid binding rename work";
            }
            TSqlQueryBuilder write(tablePathPrefix, "ClaimBindingRename");
            write.AddString("operation_id", TString(operationId));
            write.AddTimestamp("next_attempt", now + TDuration::Minutes(1));
            write.AddText("UPDATE `" BINDING_RENAME_WORK_TABLE_NAME "` SET `" NEXT_ATTEMPT_COLUMN_NAME
                          "` = $next_attempt WHERE `" OPERATION_ID_COLUMN_NAME "` = $operation_id;");
            const auto query = write.Build();
            return std::make_pair(query.Sql, query.Params);
        }, counters);
    }

    SendResponse<TEvControlPlaneStorage::TEvBindingRenameWorkResponse, FederatedQuery::Internal::BindingRenameWork>("BindingRenameWork",
        TActivationContext::ActorSystem(), status, SelfId(), std::move(ev), now, counters, [result] {
            return *result;
        },
        {});
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDeleteBindingRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDeleteBindingRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_BINDING, RTC_DELETE_BINDING);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::DeleteBindingRequest& request = event.Request;
    const TString user = event.User;
    const TString token = event.Token;
    const TString bindingId = request.binding_id();
    const TString idempotencyKey = request.idempotency_key();
    const int byteSize = event.Request.ByteSize();
    TPermissions permissions = Config->Proto.GetEnablePermissions() ? event.Permissions : TPermissions{ TPermissions::MANAGE_PUBLIC };
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int previousRevision = request.previous_revision();

    YDB_LOG_TRACE("Dump logPrefix, deleteBindingRequest, request",
        {"logPrefix", MakeLogPrefix(scope, user, bindingId)},
        {"deleteBindingRequest", NKikimr::MaskTicket(token)},
        {"request", request.DebugString()});

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        YDB_LOG_DEBUG("DeleteBindingRequest, validation",
            {"logPrefix", MakeLogPrefix(scope, user, bindingId)},
            {"failed", NKikimr::MaskTicket(token)},
            {"request", request.DebugString()},
            {"error", issues});
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDeleteBindingResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DeleteBindingRequest, scope, bindingId, user, delta, byteSize, false);
        return;
    }

    std::shared_ptr<std::pair<FederatedQuery::DeleteBindingResult, TAuditDetails<FederatedQuery::Binding>>> response =
        std::make_shared<std::pair<FederatedQuery::DeleteBindingResult, TAuditDetails<FederatedQuery::Binding>>>();

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DeleteBinding");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("binding_id", bindingId);

    InsertIdempotencyKey(queryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), TInstant::Now() + Config->IdempotencyKeyTtl);
    queryBuilder.AddText("DELETE FROM `" BINDINGS_TABLE_NAME
                         "`\n"
                         "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;");

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(
            scope, idempotencyKey, response, YdbConnection->TablePathPrefix, requestCounters.Common->ParseProtobufError));
    }

    auto accessValidator = CreateManageAccessValidator(BINDINGS_TABLE_NAME, BINDING_ID_COLUMN_NAME, scope, bindingId, user,
        "Binding does not exist or permission denied. Please check the id binding or your access rights", permissions,
        YdbConnection->TablePathPrefix);
    validators.push_back(accessValidator);

    if (previousRevision > 0) {
        auto revisionValidator = CreateRevisionValidator(BINDINGS_TABLE_NAME, BINDING_ID_COLUMN_NAME, scope, bindingId, previousRevision,
            "Revision of the binding has been changed already. Please restart the request with a new revision", YdbConnection->TablePathPrefix);
        validators.push_back(revisionValidator);
    }

    TSqlQueryBuilder checkBindingBuilder(YdbConnection->TablePathPrefix, "DeleteBinding(binding)");
    checkBindingBuilder.AddString("scope", scope);
    checkBindingBuilder.AddString("binding_id", bindingId);
    checkBindingBuilder.AddText("SELECT `" BINDING_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME
                                "`"
                                " WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;");
    const auto checkBinding = checkBindingBuilder.Build();
    validators.push_back({ checkBinding.Sql, checkBinding.Params,
        [response, parseProtobufError = requestCounters.Common->ParseProtobufError](NYdb::NTable::TDataQueryResult result) {
            const auto& resultSets = result.GetResultSets();
            if (resultSets.size() != 1) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Unexpected binding result";
            }
            TResultSetParser parser(resultSets.front());
            if (!parser.TryNextRow()) {
                return false;
            }
            FederatedQuery::Internal::BindingInternal internal;
            if (!internal.ParseFromString(parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString().value_or("")) ||
                !response->second.Before.ConstructInPlace().ParseFromString(*parser.ColumnParser(BINDING_COLUMN_NAME).GetOptionalString())) {
                parseProtobufError->Inc();
                ythrow NKikimr::TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing binding metadata";
            }
            if (HasPendingBindingRename(internal)) {
                ythrow NKikimr::TCodeLineException(TIssuesIds::BAD_REQUEST) << "A rename is pending for this binding";
            }
            return false;
        } });

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = Write(query.Sql, query.Params, requestCounters, debugInfo, validators);
    auto prepare = [response] {
        return *response;
    };
    auto success = SendResponse<TEvControlPlaneStorage::TEvDeleteBindingResponse, FederatedQuery::DeleteBindingResult>(
        MakeLogPrefix(scope, user, bindingId) + "DeleteBindingRequest", NActors::TActivationContext::ActorSystem(), result, SelfId(),
        std::move(ev), startTime, requestCounters, prepare, debugInfo);

    success.Apply([=](const auto& future) {
        TDuration delta = TInstant::Now() - startTime;
        LWPROBE(DeleteBindingRequest, scope, user, bindingId, delta, byteSize, future.GetValue());
    });
}

}   // namespace NFq
