#include "ydb_control_plane_storage_impl.h"

#include <util/string/join.h>

#include <ydb/core/yq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/yq/libs/control_plane_storage/validators.h>
#include <ydb/core/yq/libs/db_schema/db_schema.h>

namespace NYq {

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvCreateBindingRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvCreateBindingRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_CREATE_BINDING);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                        ? event.Permissions
                        : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const YandexQuery::CreateBindingRequest& request = event.Request;
    const TString bindingId = GetEntityIdAsString(Config.IdsPrefix, EEntityType::BINDING);
    int byteSize = request.ByteSize();
    const TString connectionId = request.content().connection_id();
    const TString idempotencyKey = request.idempotency_key();

    CPS_LOG_T(MakeLogPrefix(scope, user, bindingId)
        << "CreateBindingRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());

    NYql::TIssues issues = ValidateBinding(ev);
    if (request.content().acl().visibility() == YandexQuery::Acl::SCOPE && !permissions.Check(TPermissions::MANAGE_PUBLIC)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a binding with these parameters. Please receive a permission yq.resources.managePublic"));
    }
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user, bindingId)
            << "CreateBindingRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvCreateBindingResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(CreateBindingRequest, scope, user, delta, byteSize, false);
        return;
    }

    YandexQuery::Binding binding;
    YandexQuery::BindingContent& content = *binding.mutable_content();
    content = request.content();
    *binding.mutable_meta() = CreateCommonMeta(bindingId, user, startTime, InitialRevision);

    YandexQuery::Internal::BindingInternal bindingInternal;
    bindingInternal.set_cloud_id(cloudId);

    std::shared_ptr<std::pair<YandexQuery::CreateBindingResult, TAuditDetails<YandexQuery::Binding>>> response = std::make_shared<std::pair<YandexQuery::CreateBindingResult, TAuditDetails<YandexQuery::Binding>>>();
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

    InsertIdempotencyKey(queryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), startTime + Config.IdempotencyKeyTtl);
    queryBuilder.AddText(
        "INSERT INTO `" BINDINGS_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" BINDING_ID_COLUMN_NAME "`, `" CONNECTION_ID_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`, `" NAME_COLUMN_NAME "`, `" BINDING_COLUMN_NAME "`, `" REVISION_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "`) VALUES\n"
        "    ($scope, $binding_id, $connection_id, $user, $visibility, $name, $binding, $revision, $internal);"
    );

    auto validatorName = CreateUniqueNameValidator(
        BINDINGS_TABLE_NAME,
        content.acl().visibility(),
        scope,
        content.name(),
        user,
        "Binding with the same name already exists. Please choose another name",
        YdbConnection->TablePathPrefix);

    auto validatorCountBindings = CreateCountEntitiesValidator(
        scope,
        BINDINGS_TABLE_NAME,
        Config.Proto.GetMaxCountBindings(),
        "Too many bindings in folder: " + ToString(Config.Proto.GetMaxCountBindings()) + ". Please remove unused bindings",
        YdbConnection->TablePathPrefix);

    auto validatorConnectionExists = CreateConnectionExistsValidator(
        scope,
        connectionId,
        "Connection " + connectionId + " does not exist or permission denied. Please check the id connection or your access rights",
        permissions,
        user,
        content.acl().visibility(),
        YdbConnection->TablePathPrefix);

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix));
    }

    validators.push_back(validatorName);
    validators.push_back(validatorCountBindings);
    validators.push_back(validatorConnectionExists);

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    TAsyncStatus result = Write(NActors::TActivationContext::ActorSystem(), query.Sql, query.Params, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendAuditResponse<TEvControlPlaneStorage::TEvCreateBindingResponse, YandexQuery::CreateBindingResult, TAuditDetails<YandexQuery::Binding>>(
        MakeLogPrefix(scope, user, bindingId) + "CreateBindingRequest",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(CreateBindingRequest, scope, user, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvListBindingsRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvListBindingsRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_LIST_BINDINGS);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const YandexQuery::ListBindingsRequest& request = event.Request;
    const TString user = event.User;
    const TString pageToken = request.page_token();
    const int byteSize = event.Request.ByteSize();
    const int64_t limit = request.limit();
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                            ? event.Permissions
                            : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }

    CPS_LOG_T(MakeLogPrefix(scope, user) << "ListBindingsRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user)
            << "ListBindingsRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvListBindingsResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ListBindingsRequest, scope, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "ListBindings");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("last_binding", pageToken);
    queryBuilder.AddUint64("limit", limit + 1);

    queryBuilder.AddText(
        "SELECT `" BINDING_ID_COLUMN_NAME "`, `" BINDING_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` >= $last_binding\n"
    );

    TString filter;
    if (request.has_filter()) {
        TVector<TString> filters;
        if (request.filter().connection_id()) {
            queryBuilder.AddString("connection_id", request.filter().connection_id());
            filters.push_back("`" CONNECTION_ID_COLUMN_NAME "` == $connection_id");
        }

        if (request.filter().name()) {
            queryBuilder.AddString("filter_name", request.filter().name());
            filters.push_back("`" NAME_COLUMN_NAME "` LIKE '%' || $filter_name || '%'");
        }

        if (request.filter().created_by_me()) {
            queryBuilder.AddString("user", user);
            filters.push_back("`" USER_COLUMN_NAME "` = $user");
        }

        filter = JoinSeq(" AND ", filters);
    }

    PrepareViewAccessCondition(queryBuilder, permissions, user);

    if (filter) {
        queryBuilder.AddText(" AND (" + filter + ")");
    }

    queryBuilder.AddText(
        "ORDER BY `" BINDING_ID_COLUMN_NAME "`\n"
        "LIMIT $limit;"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, limit] {
        if (resultSets->size() != 1) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        YandexQuery::ListBindingsResult result;
        TResultSetParser parser(resultSets->front());
        while (parser.TryNextRow()) {
            YandexQuery::Binding binding;
            if (!binding.ParseFromString(*parser.ColumnParser(BINDING_COLUMN_NAME).GetOptionalString())) {
                ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for binding. Please contact internal support";
            }
            YandexQuery::BriefBinding& briefBinding = *result.add_binding();
            briefBinding.set_name(binding.content().name());
            briefBinding.set_connection_id(binding.content().connection_id());
            *briefBinding.mutable_meta() = binding.meta();
            switch (binding.content().setting().binding_case()) {
            case YandexQuery::BindingSetting::kDataStreams: {
                briefBinding.set_type(YandexQuery::BindingSetting::DATA_STREAMS);
                break;
            }
            case YandexQuery::BindingSetting::kObjectStorage: {
                briefBinding.set_type(YandexQuery::BindingSetting::OBJECT_STORAGE);
                break;
            }
            // Do not replace with default. Adding a new binding should cause a compilation error
            case YandexQuery::BindingSetting::BINDING_NOT_SET:
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

    auto success = SendResponse<TEvControlPlaneStorage::TEvListBindingsResponse, YandexQuery::ListBindingsResult>(
        MakeLogPrefix(scope, user) + "ListBindingsRequest",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

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
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_DESCRIBE_BINDING);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const YandexQuery::DescribeBindingRequest& request = event.Request;
    const TString bindingId = request.binding_id();
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                        ? event.Permissions
                        : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int byteSize = request.ByteSize();
    CPS_LOG_T(MakeLogPrefix(scope, user, bindingId)
        << "DescribeBindingRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());
    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user, bindingId)
            << "DescribeBindingRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDescribeBindingResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DescribeBindingRequest, scope, bindingId, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DescribeBinding");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("binding_id", bindingId);
    queryBuilder.AddText(
        "SELECT `" BINDING_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [=, resultSets=resultSets] {
        if (resultSets->size() != 1) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Binding does not exist or permission denied. Please check the id binding or your access rights";
        }

        YandexQuery::DescribeBindingResult result;
        if (!result.mutable_binding()->ParseFromString(*parser.ColumnParser(BINDING_COLUMN_NAME).GetOptionalString())) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for binding. Please contact internal support";
        }

        bool hasViewAccess = HasViewAccess(permissions, result.binding().content().acl().visibility(), result.binding().meta().created_by(), user);
        if (!hasViewAccess) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Binding does not exist or permission denied. Please check the id binding or your access rights";
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvDescribeBindingResponse, YandexQuery::DescribeBindingResult>(
        MakeLogPrefix(scope, user, bindingId) + "DescribeBindingRequest",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

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
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_MODIFY_BINDING);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const YandexQuery::ModifyBindingRequest& request = event.Request;
    const TString bindingId = request.binding_id();
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                        ? event.Permissions
                        : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int64_t previousRevision = request.previous_revision();
    const TString idempotencyKey = request.idempotency_key();
    const int byteSize = request.ByteSize();
    CPS_LOG_T(MakeLogPrefix(scope, user, bindingId)
        << "ModifyBindingRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());

    NYql::TIssues issues = ValidateBinding(ev);
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user, bindingId)
            << "ModifyBindingRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvModifyBindingResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ModifyBindingRequest, scope, bindingId, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "ModifyBinding(read)");
    readQueryBuilder.AddString("scope", scope);
    readQueryBuilder.AddString("binding_id", bindingId);
    readQueryBuilder.AddText(
        "SELECT `" BINDING_COLUMN_NAME "` FROM `" BINDINGS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;"
    );

    std::shared_ptr<std::pair<YandexQuery::ModifyBindingResult, TAuditDetails<YandexQuery::Binding>>> response = std::make_shared<std::pair<YandexQuery::ModifyBindingResult, TAuditDetails<YandexQuery::Binding>>>();
    auto prepareParams = [=, config=Config](const TVector<TResultSet>& resultSets) {
        if (resultSets.size() != 1) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets.size() << ". Please contact internal support";
        }

        YandexQuery::Binding binding;
        TResultSetParser parser(resultSets.front());
        if (!parser.TryNextRow()) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Binding does not exist or permission denied. Please check the id binding or your access rights";
        }

        if (!binding.ParseFromString(*parser.ColumnParser(BINDING_COLUMN_NAME).GetOptionalString())) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for binding. Please contact internal support";
        }

        bool hasManageAccess = HasManageAccess(permissions, binding.content().acl().visibility(), binding.meta().created_by(), user);
        if (!hasManageAccess) {
            ythrow TControlPlaneStorageException(TIssuesIds::ACCESS_DENIED) << "Binding does not exist or permission denied. Please check the id binding or your access rights";
        }

        auto& meta = *binding.mutable_meta();
        meta.set_revision(meta.revision() + 1);
        meta.set_modified_by(user);
        *meta.mutable_modified_at() = NProtoInterop::CastToProto(TInstant::Now());

        auto& content = *binding.mutable_content();

        bool validateType = content.setting().binding_case() == request.content().setting().binding_case();

        if (!validateType) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Binding type cannot be changed. Please specify the same binding type";
        }

        if (content.acl().visibility() == YandexQuery::Acl::SCOPE && request.content().acl().visibility() == YandexQuery::Acl::PRIVATE) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Changing visibility from SCOPE to PRIVATE is forbidden. Please create a new binding with visibility PRIVATE";
        }

        if (content.connection_id() != request.content().connection_id()) {
            ythrow TControlPlaneStorageException(TIssuesIds::BAD_REQUEST) << "Connection id cannot be changed. Please specify the same connection id";
        }

        content = request.content();

        YandexQuery::Internal::BindingInternal bindingInternal;
        response->second.After.ConstructInPlace().CopyFrom(binding);
        response->second.CloudId = bindingInternal.cloud_id();

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "ModifyBinding(write)");
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddString("binding_id", bindingId);
        writeQueryBuilder.AddInt64("visibility", binding.content().acl().visibility());
        writeQueryBuilder.AddString("name", binding.content().name());
        writeQueryBuilder.AddInt64("revision", meta.revision());
        writeQueryBuilder.AddString("internal", bindingInternal.SerializeAsString());
        writeQueryBuilder.AddString("binding", binding.SerializeAsString());
        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), TInstant::Now() + Config.IdempotencyKeyTtl);
        writeQueryBuilder.AddText(
            "UPDATE `" BINDINGS_TABLE_NAME "` SET `" VISIBILITY_COLUMN_NAME "` = $visibility, `" NAME_COLUMN_NAME "` = $name, `" REVISION_COLUMN_NAME "` = $revision, `" INTERNAL_COLUMN_NAME "` = $internal, `" BINDING_COLUMN_NAME "` = $binding\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;\n"
        );
        const auto writeQuery = writeQueryBuilder.Build();
        return make_pair(writeQuery.Sql, writeQuery.Params);
    };

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix));
    }

    auto accessValidator = CreateManageAccessValidator(
        BINDINGS_TABLE_NAME,
        BINDING_ID_COLUMN_NAME,
        scope,
        bindingId,
        user,
        "Binding does not exist or permission denied. Please check the id binding or your access rights",
        permissions,
        YdbConnection->TablePathPrefix);
    validators.push_back(accessValidator);

    if (previousRevision > 0) {
        auto revisionValidator = CreateRevisionValidator(
            BINDINGS_TABLE_NAME,
            BINDING_ID_COLUMN_NAME,
            scope,
            bindingId,
            previousRevision,
            "Revision of the binding has been changed already. Please restart the request with a new revision",
            YdbConnection->TablePathPrefix);
        validators.push_back(revisionValidator);
    }

    {
        auto modifyUniqueNameValidator = CreateModifyUniqueNameValidator(
            BINDINGS_TABLE_NAME,
            BINDING_ID_COLUMN_NAME,
            request.content().acl().visibility(),
            scope,
            request.content().name(),
            user,
            bindingId,
            "Binding with the same name already exists. Please choose another name",
            YdbConnection->TablePathPrefix);
        validators.push_back(modifyUniqueNameValidator);
    }

    const auto readQuery = readQueryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(NActors::TActivationContext::ActorSystem(), readQuery.Sql, readQuery.Params, prepareParams, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendAuditResponse<TEvControlPlaneStorage::TEvModifyBindingResponse, YandexQuery::ModifyBindingResult, TAuditDetails<YandexQuery::Binding>>(
        MakeLogPrefix(scope, user, bindingId) + "ModifyBindingRequest",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(ModifyBindingRequest, scope, user, bindingId, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDeleteBindingRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDeleteBindingRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCountersPtr requestCounters = Counters.GetScopeCounters(cloudId, scope, RTS_DELETE_BINDING);
    requestCounters->InFly->Inc();
    requestCounters->RequestBytes->Add(event.GetByteSize());
    const YandexQuery::DeleteBindingRequest& request = event.Request;
    const TString user = event.User;
    const TString token = event.Token;
    const TString bindingId = request.binding_id();
    const TString idempotencyKey = request.idempotency_key();
    const int byteSize = event.Request.ByteSize();
    TPermissions permissions = Config.Proto.GetEnablePermissions()
                        ? event.Permissions
                        : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int previousRevision = request.previous_revision();

    CPS_LOG_T(MakeLogPrefix(scope, user, bindingId)
        << "DeleteBindingRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user, bindingId)
            << "DeleteBindingRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDeleteBindingResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DeleteBindingRequest, scope, bindingId, user, delta, byteSize, false);
        return;
    }

    std::shared_ptr<std::pair<YandexQuery::DeleteBindingResult, TAuditDetails<YandexQuery::Binding>>> response = std::make_shared<std::pair<YandexQuery::DeleteBindingResult, TAuditDetails<YandexQuery::Binding>>>();

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DeleteBinding");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("binding_id", bindingId);

    InsertIdempotencyKey(queryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), TInstant::Now() + Config.IdempotencyKeyTtl);
    queryBuilder.AddText(
        "DELETE FROM `" BINDINGS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" BINDING_ID_COLUMN_NAME "` = $binding_id;"
    );

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix));
    }

    auto accessValidator = CreateManageAccessValidator(
        BINDINGS_TABLE_NAME,
        BINDING_ID_COLUMN_NAME,
        scope,
        bindingId,
        user,
        "Binding does not exist or permission denied. Please check the id binding or your access rights",
        permissions,
        YdbConnection->TablePathPrefix);
    validators.push_back(accessValidator);

    if (previousRevision > 0) {
        auto revisionValidator = CreateRevisionValidator(
            BINDINGS_TABLE_NAME,
            BINDING_ID_COLUMN_NAME,
            scope,
            bindingId,
            previousRevision,
            "Revision of the binding has been changed already. Please restart the request with a new revision",
            YdbConnection->TablePathPrefix);
        validators.push_back(revisionValidator);
    }

    validators.push_back(CreateEntityExtractor(
        scope,
        bindingId,
        BINDING_COLUMN_NAME,
        BINDING_ID_COLUMN_NAME,
        BINDINGS_TABLE_NAME,
        response,
        YdbConnection->TablePathPrefix));

    const auto query = queryBuilder.Build();
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = Write(NActors::TActivationContext::ActorSystem(), query.Sql, query.Params, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendAuditResponse<TEvControlPlaneStorage::TEvDeleteBindingResponse, YandexQuery::DeleteBindingResult, TAuditDetails<YandexQuery::Binding>>(
        MakeLogPrefix(scope, user, bindingId) + "DeleteBindingRequest",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(DeleteBindingRequest, scope, user, bindingId, delta, byteSize, future.GetValue());
        });
}

} // NYq
