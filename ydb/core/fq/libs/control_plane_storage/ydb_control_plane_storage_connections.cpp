#include "validators.h"
#include "ydb_control_plane_storage_impl.h"

#include <util/string/join.h>

#include <ydb/public/api/protos/draft/fq.pb.h>

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>

namespace NFq {

namespace {

void PrepareSensitiveFields(::FederatedQuery::Connection& connection, bool extractSensitiveFields) {
    if (extractSensitiveFields) {
        return;
    }

    auto& setting = *connection.mutable_content()->mutable_setting();
    if (setting.has_clickhouse_cluster()) {
        auto& ch = *setting.mutable_clickhouse_cluster();
        ch.set_password("");
    }
    if (setting.has_postgresql_cluster()) {
        auto& pg = *setting.mutable_postgresql_cluster();
        pg.set_password("");
    }
}

}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvCreateConnectionRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvCreateConnectionRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_CONNECTION, RTC_CREATE_CONNECTION);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::CreateConnectionRequest& request = event.Request;
    const TString user = event.User;
    const TString token = event.Token;
    const int byteSize = request.ByteSize();
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                            ? event.Permissions
                            : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const TString idempotencyKey = request.idempotency_key();
    const TString connectionId = GetEntityIdAsString(Config->IdsPrefix, EEntityType::CONNECTION);

    CPS_LOG_T(MakeLogPrefix(scope, user, connectionId)
        << "CreateConnectionRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());

    NYql::TIssues issues = ValidateConnection(ev);
    if (request.content().acl().visibility() == FederatedQuery::Acl::SCOPE && !permissions.Check(TPermissions::MANAGE_PUBLIC)) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "Permission denied to create a connection with these parameters. Please receive a permission yq.resources.managePublic"));
    }
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user, connectionId)
            << "CreateConnectionRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvCreateConnectionResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(CreateConnectionRequest, scope, user, delta, byteSize, false);
        return;
    }

    FederatedQuery::Connection connection;
    FederatedQuery::ConnectionContent& content = *connection.mutable_content();
    content = request.content();
    *connection.mutable_meta() = CreateCommonMeta(connectionId, user, startTime, InitialRevision);

    FederatedQuery::Internal::ConnectionInternal connectionInternal;
    connectionInternal.set_cloud_id(cloudId);

    std::shared_ptr<std::pair<FederatedQuery::CreateConnectionResult, TAuditDetails<FederatedQuery::Connection>>> response = std::make_shared<std::pair<FederatedQuery::CreateConnectionResult, TAuditDetails<FederatedQuery::Connection>>>();
    response->first.set_connection_id(connectionId);
    response->second.After.ConstructInPlace().CopyFrom(connection);
    response->second.CloudId = cloudId;

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "CreateConnection");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("connection_id", connectionId);
    queryBuilder.AddString("user", user);
    queryBuilder.AddInt64("visibility", content.acl().visibility());
    queryBuilder.AddString("name", content.name());
    queryBuilder.AddInt64("connection_type", content.setting().connection_case());
    queryBuilder.AddString("connection", connection.SerializeAsString());
    queryBuilder.AddInt64("revision", InitialRevision);
    queryBuilder.AddString("internal", connectionInternal.SerializeAsString());

    InsertIdempotencyKey(queryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), startTime + Config->IdempotencyKeyTtl);

    queryBuilder.AddText(
        "INSERT INTO `" CONNECTIONS_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" CONNECTION_ID_COLUMN_NAME "`, `" USER_COLUMN_NAME "`, `" VISIBILITY_COLUMN_NAME "`, `" NAME_COLUMN_NAME "`, `" CONNECTION_TYPE_COLUMN_NAME "`, `" CONNECTION_COLUMN_NAME "`, `" REVISION_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "`) VALUES\n"
        "    ($scope, $connection_id, $user, $visibility, $name, $connection_type, $connection, $revision, $internal);"
    );

    auto connectionNameUniqueValidator = CreateUniqueNameValidator(
        CONNECTIONS_TABLE_NAME,
        content.acl().visibility(),
        scope,
        content.name(),
        user,
        "Connection with the same name already exists. Please choose another name",
        YdbConnection->TablePathPrefix);

    auto bindingNameUniqueValidator = CreateUniqueNameValidator(
        BINDINGS_TABLE_NAME,
        content.acl().visibility(),
        scope,
        content.name(),
        user,
        "Binding with the same name already exists. Please choose another name",
        YdbConnection->TablePathPrefix);

    auto validatorCountConnections = CreateCountEntitiesValidator(
        scope,
        CONNECTIONS_TABLE_NAME,
        Config->Proto.GetMaxCountConnections(),
        "Too many connections in folder: " + ToString(Config->Proto.GetMaxCountConnections()) + ". Please remove unused connections",
        YdbConnection->TablePathPrefix);

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix, requestCounters.Common->ParseProtobufError));
    }
    validators.push_back(connectionNameUniqueValidator);
    validators.push_back(bindingNameUniqueValidator);
    validators.push_back(validatorCountConnections);

    if (content.acl().visibility() == FederatedQuery::Acl::PRIVATE) {
        auto overridBindingValidator = CreateConnectionOverrideBindingValidator(
            scope,
            content.name(),
            permissions,
            user,
            YdbConnection->TablePathPrefix);
        validators.push_back(overridBindingValidator);
    }

    const auto query = queryBuilder.Build();

    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    TAsyncStatus result = Write(query.Sql, query.Params, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvCreateConnectionResponse, FederatedQuery::CreateConnectionResult>(
        MakeLogPrefix(scope, user, connectionId) + "CreateConnectionRequest",
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
            LWPROBE(CreateConnectionRequest, scope, user, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvListConnectionsRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvListConnectionsRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_CONNECTIONS, RTC_LIST_CONNECTIONS);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::ListConnectionsRequest& request = event.Request;
    bool extractSensitiveFields = event.ExtractSensitiveFields;

    const TString user = event.User;
    const TString pageToken = request.page_token();
    const int byteSize = request.ByteSize();
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                        ? event.Permissions
                        : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int64_t limit = request.limit();
    CPS_LOG_T(MakeLogPrefix(scope, user)
        << "ListConnectionsRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user)
            << "ListConnectionsRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvListConnectionsResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ListConnectionsRequest, scope, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "ListConnections");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("last_connection", pageToken);
    queryBuilder.AddUint64("limit", limit + 1);

    queryBuilder.AddText(
        "SELECT `" SCOPE_COLUMN_NAME "`, `" CONNECTION_ID_COLUMN_NAME "`, `" CONNECTION_COLUMN_NAME "` FROM `" CONNECTIONS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" CONNECTION_ID_COLUMN_NAME "` >= $last_connection\n"
    );

    TString filter;
    if (request.has_filter()) {
        TVector<TString> filters;
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

        if (request.filter().connection_type() != FederatedQuery::ConnectionSetting::CONNECTION_TYPE_UNSPECIFIED) {
            queryBuilder.AddInt64("connection_type", request.filter().connection_type());
            filters.push_back("`" CONNECTION_TYPE_COLUMN_NAME "` = $connection_type");
        }

        if (request.filter().visibility() != FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
            queryBuilder.AddInt64("visibility", request.filter().visibility());
            filters.push_back("`" VISIBILITY_COLUMN_NAME "` = $visibility");
        }

        filter = JoinSeq(" AND ", filters);
    }

    PrepareViewAccessCondition(queryBuilder, permissions, user);

    if (filter) {
        queryBuilder.AddText(" AND (" + filter + ")\n");
    }

    queryBuilder.AddText(
        "ORDER BY `" SCOPE_COLUMN_NAME "`, `" CONNECTION_ID_COLUMN_NAME "`\n"
        "LIMIT $limit;"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [resultSets=resultSets, limit, extractSensitiveFields, commonCounters=requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        FederatedQuery::ListConnectionsResult result;
        TResultSetParser parser(resultSets->front());
        while (parser.TryNextRow()) {
            auto& connection = *result.add_connection();
            if (!connection.ParseFromString(*parser.ColumnParser(CONNECTION_COLUMN_NAME).GetOptionalString())) {
                commonCounters->ParseProtobufError->Inc();
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for connection. Please contact internal support";
            }
            PrepareSensitiveFields(connection, extractSensitiveFields);
        }

        if (result.connection_size() == limit + 1) {
            result.set_next_page_token(result.connection(result.connection_size() - 1).meta().id());
            result.mutable_connection()->RemoveLast();
        }
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvListConnectionsResponse, FederatedQuery::ListConnectionsResult>(
        MakeLogPrefix(scope, user) + "ListConnectionsRequest",
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
            LWPROBE(ListConnectionsRequest, scope, user, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDescribeConnectionRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDescribeConnectionRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_CONNECTION, RTC_DESCRIBE_CONNECTION);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::DescribeConnectionRequest& request = event.Request;
    const TString user = event.User;
    const TString connectionId = request.connection_id();
    const TString token = event.Token;
    const bool extractSensitiveFields = event.ExtractSensitiveFields;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                    ? event.Permissions
                    : TPermissions{TPermissions::VIEW_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const int byteSize = request.ByteSize();

    CPS_LOG_T(MakeLogPrefix(scope, user, connectionId)
        << "DescribeConnectionRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user, connectionId)
            << "DescribeConnectionRequest, validation failed: "
            << NKikimr::MaskTicket(token)<< " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDescribeConnectionResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DescribeConnectionRequest, scope, connectionId, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DescribeConnection");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("connection_id", connectionId);

    queryBuilder.AddText(
        "SELECT `" CONNECTION_COLUMN_NAME "` FROM `" CONNECTIONS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" CONNECTION_ID_COLUMN_NAME "` = $connection_id;"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [=, resultSets=resultSets, commonCounters=requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        FederatedQuery::DescribeConnectionResult result;
        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Connection does not exist or permission denied. Please check the id connection or your access rights";
        }

        if (!result.mutable_connection()->ParseFromString(*parser.ColumnParser(CONNECTION_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for connection. Please contact internal support";
        }

        bool hasViewAccess = HasViewAccess(permissions, result.connection().content().acl().visibility(), result.connection().meta().created_by(), user);
        if (!hasViewAccess) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Connection does not exist or permission denied. Please check the id connection or your access rights";
        }

        PrepareSensitiveFields(*result.mutable_connection(), extractSensitiveFields);
        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvDescribeConnectionResponse, FederatedQuery::DescribeConnectionResult>(
        MakeLogPrefix(scope, user, connectionId) + "DescribeConnectionRequest",
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
            LWPROBE(DescribeConnectionRequest, scope, connectionId, user, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvModifyConnectionRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvModifyConnectionRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_CONNECTION, RTC_MODIFY_CONNECTION);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                    ? event.Permissions
                    : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const FederatedQuery::ModifyConnectionRequest& request = event.Request;
    const TString connectionId = request.connection_id();
    const int64_t previousRevision = request.previous_revision();
    const TString idempotencyKey = request.idempotency_key();
    const int byteSize = request.ByteSize();
    CPS_LOG_T(MakeLogPrefix(scope, user, connectionId)
        << "ModifyConnectionRequest: "
        << NKikimr::MaskTicket(token)
        << " " << request.DebugString());

    NYql::TIssues issues = ValidateConnection(ev, false);
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user, connectionId)
            << "ModifyConnectionRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvModifyConnectionResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(ModifyConnectionRequest, scope, connectionId, user, delta, byteSize, false);
        return;
    }

    TSqlQueryBuilder readQueryBuilder(YdbConnection->TablePathPrefix, "ModifyConnection(read)");
    readQueryBuilder.AddString("scope", scope);
    readQueryBuilder.AddString("connection_id", connectionId);
    readQueryBuilder.AddText(
        "SELECT `" CONNECTION_COLUMN_NAME "` FROM `" CONNECTIONS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" CONNECTION_ID_COLUMN_NAME "` = $connection_id;"
    );

    std::shared_ptr<std::pair<FederatedQuery::ModifyConnectionResult, TAuditDetails<FederatedQuery::Connection>>> response = std::make_shared<std::pair<FederatedQuery::ModifyConnectionResult, TAuditDetails<FederatedQuery::Connection>>>();
    auto prepareParams = [=, config=Config, commonCounters=requestCounters.Common](const TVector<TResultSet>& resultSets) {
        if (resultSets.size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets.size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets.front());
        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Connection does not exist or permission denied. Please check the id connection or your access rights";
        }

        FederatedQuery::Connection connection;
        if (!connection.ParseFromString(*parser.ColumnParser(CONNECTION_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for connection. Please contact internal support";
        }

        auto& meta = *connection.mutable_meta();
        meta.set_revision(meta.revision() + 1);
        meta.set_modified_by(user);
        *meta.mutable_modified_at() = NProtoInterop::CastToProto(TInstant::Now());

        auto& content = *connection.mutable_content();

        bool validateType = content.setting().connection_case() == request.content().setting().connection_case();

        if (!validateType) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Connection type cannot be changed. Please specify the same connection type";
        }

        if (content.acl().visibility() == FederatedQuery::Acl::SCOPE && request.content().acl().visibility() == FederatedQuery::Acl::PRIVATE) {
            ythrow TCodeLineException(TIssuesIds::BAD_REQUEST) << "Changing visibility from SCOPE to PRIVATE is forbidden. Please create a new connection with visibility PRIVATE";
        }

        // FIXME: this code needs better generalization
        if (request.content().setting().has_clickhouse_cluster()) {
            auto clickHousePassword = request.content().setting().clickhouse_cluster().password();
            if (!clickHousePassword) {
                clickHousePassword = content.setting().clickhouse_cluster().password();
            }
            content = request.content();
            content.mutable_setting()->mutable_clickhouse_cluster()->set_password(clickHousePassword);
        } else if (request.content().setting().has_postgresql_cluster()) {
            auto postgreSQLPassword = request.content().setting().postgresql_cluster().password();
            if (!postgreSQLPassword) {
                postgreSQLPassword = content.setting().postgresql_cluster().password();
            }
            content = request.content();
            content.mutable_setting()->mutable_postgresql_cluster()->set_password(postgreSQLPassword);
        } else {
            content = request.content();
        }

        FederatedQuery::Internal::ConnectionInternal connectionInternal;
        response->second.After.ConstructInPlace().CopyFrom(connection);
        response->second.CloudId = connectionInternal.cloud_id();

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "ModifyConnection(write)");
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddString("connection_id", connectionId);
        writeQueryBuilder.AddInt64("visibility", connection.content().acl().visibility());
        writeQueryBuilder.AddString("name", connection.content().name());
        writeQueryBuilder.AddInt64("revision", meta.revision());
        writeQueryBuilder.AddString("internal", connectionInternal.SerializeAsString());
        writeQueryBuilder.AddString("connection", connection.SerializeAsString());
        InsertIdempotencyKey(writeQueryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), TInstant::Now() + Config->IdempotencyKeyTtl);
        writeQueryBuilder.AddText(
            "UPDATE `" CONNECTIONS_TABLE_NAME "` SET `" VISIBILITY_COLUMN_NAME "` = $visibility, `" NAME_COLUMN_NAME "` = $name, `" REVISION_COLUMN_NAME "` = $revision, `" INTERNAL_COLUMN_NAME "` = $internal, `" CONNECTION_COLUMN_NAME "` = $connection\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" CONNECTION_ID_COLUMN_NAME "` = $connection_id;"
        );
        const auto writeQuery = writeQueryBuilder.Build();
        return make_pair(writeQuery.Sql, writeQuery.Params);
    };

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix, requestCounters.Common->ParseProtobufError));
    }

    auto accessValidator = CreateManageAccessValidator(
        CONNECTIONS_TABLE_NAME,
        CONNECTION_ID_COLUMN_NAME,
        scope,
        connectionId,
        user,
        "Connection does not exist or permission denied. Please check the id connection or your access rights",
        permissions,
        YdbConnection->TablePathPrefix);
    validators.push_back(accessValidator);

    if (previousRevision > 0) {
        auto revisionValidator = CreateRevisionValidator(
            CONNECTIONS_TABLE_NAME,
            CONNECTION_ID_COLUMN_NAME,
            scope,
            connectionId,
            previousRevision,
            "Revision of the connection has been changed already. Please restart the request with a new revision",
            YdbConnection->TablePathPrefix);
        validators.push_back(revisionValidator);
    }

    {
        auto connectionNameUniqueValidator = CreateModifyUniqueNameValidator(
            CONNECTIONS_TABLE_NAME,
            CONNECTION_ID_COLUMN_NAME,
            request.content().acl().visibility(),
            scope,
            request.content().name(),
            user,
            connectionId,
            "Connection with the same name already exists. Please choose another name",
            YdbConnection->TablePathPrefix);
        validators.push_back(connectionNameUniqueValidator);
    }
    {
        auto bindingNameUniqueValidator = CreateUniqueNameValidator(
            BINDINGS_TABLE_NAME,
            request.content().acl().visibility(),
            scope,
            request.content().name(),
            user,
            "Binding with the same name already exists. Please choose another name",
            YdbConnection->TablePathPrefix);
        validators.push_back(bindingNameUniqueValidator);
    }

    const auto readQuery = readQueryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(readQuery.Sql, readQuery.Params, prepareParams, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvModifyConnectionResponse, FederatedQuery::ModifyConnectionResult>(
        MakeLogPrefix(scope, user, connectionId) + "ModifyConnectionRequest",
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
            LWPROBE(ModifyConnectionRequest, scope, user, connectionId, delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDeleteConnectionRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDeleteConnectionRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_CONNECTION, RTC_DELETE_CONNECTION);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::DeleteConnectionRequest& request = event.Request;

    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                    ? event.Permissions
                    : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const TString connectionId = request.connection_id();
    const TString idempotencyKey = request.idempotency_key();
    const int byteSize = request.ByteSize();
    const int previousRevision = request.previous_revision();
    CPS_LOG_T(MakeLogPrefix(scope, user, connectionId)
        << "DeleteConnectionRequest: "
        << NKikimr::MaskTicket(token) << " "
        << request.DebugString());

    NYql::TIssues issues = ValidateEvent(ev);
    if (issues) {
        CPS_LOG_D(MakeLogPrefix(scope, user, connectionId)
            << "DeleteConnectionRequest, validation failed: "
            << NKikimr::MaskTicket(token) << " "
            << request.DebugString()
            << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvDeleteConnectionResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(DeleteConnectionRequest, scope, connectionId, user, delta, byteSize, false);
        return;
    }

    std::shared_ptr<std::pair<FederatedQuery::DeleteConnectionResult, TAuditDetails<FederatedQuery::Connection>>> response = std::make_shared<std::pair<FederatedQuery::DeleteConnectionResult, TAuditDetails<FederatedQuery::Connection>>>();

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DeleteConnection");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("connection_id", connectionId);

    InsertIdempotencyKey(queryBuilder, scope, idempotencyKey, response->first.SerializeAsString(), TInstant::Now() + Config->IdempotencyKeyTtl);
    queryBuilder.AddText(
        "DELETE FROM `" CONNECTIONS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" CONNECTION_ID_COLUMN_NAME "` = $connection_id;"
    );

    TVector<TValidationQuery> validators;
    if (idempotencyKey) {
        validators.push_back(CreateIdempotencyKeyValidator(scope, idempotencyKey, response, YdbConnection->TablePathPrefix, requestCounters.Common->ParseProtobufError));
    }

    auto accessValidator = CreateManageAccessValidator(
        CONNECTIONS_TABLE_NAME,
        CONNECTION_ID_COLUMN_NAME,
        scope,
        connectionId,
        user,
        "Connection does not exist or permission denied. Please check the id connection or your access rights",
        permissions,
        YdbConnection->TablePathPrefix);
    validators.push_back(accessValidator);

    if (previousRevision > 0) {
        auto revisionValidator = CreateRevisionValidator(
            CONNECTIONS_TABLE_NAME,
            CONNECTION_ID_COLUMN_NAME,
            scope,
            connectionId,
            previousRevision,
            "Revision of the connection has been changed already. Please restart the request with a new revision",
            YdbConnection->TablePathPrefix);
        validators.push_back(revisionValidator);
    }

    {
        auto relatedBindingsValidator = CreateRelatedBindingsValidator(scope,
            connectionId,
            "There are bindings related with connection. Please remove them at the beginning",
            YdbConnection->TablePathPrefix);
        validators.push_back(relatedBindingsValidator);
    }

    validators.push_back(CreateEntityExtractor(
        scope,
        connectionId,
        CONNECTION_COLUMN_NAME,
        CONNECTION_ID_COLUMN_NAME,
        CONNECTIONS_TABLE_NAME,
        response,
        YdbConnection->TablePathPrefix,
        requestCounters.Common->ParseProtobufError));

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = Write(query.Sql, query.Params, requestCounters, debugInfo, validators);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvDeleteConnectionResponse, FederatedQuery::DeleteConnectionResult>(
        MakeLogPrefix(scope, user, connectionId) + "DeleteConnectionRequest",
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
            LWPROBE(DeleteConnectionRequest, scope, user, connectionId, delta, byteSize, future.GetValue());
        });
}

} // NFq
