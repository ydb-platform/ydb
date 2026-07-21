#include "kafka_metadata_service.h"

#include <ydb/core/kafka_proxy/actors/kafka_balancer_actor.h>
#include <ydb/core/kafka_proxy/kafka_constants.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/public/api/protos/ydb_scripting.pb.h>

#include <util/string/builder.h>

namespace NKafka {

using namespace NActors;
static constexpr i64 PRODUCER_ID_SEQUENCE_GAP = 1000;

void TKafkaMetadataService::Bootstrap(const NActors::TActorContext&) {
    Become(&TKafkaMetadataService::StateWork);
    switch (TablesType) {
        case ETables::ConsumerGroupsAndMembers:
            TablesToCreate = 2;
            InitializeConsumerMembersTable();
            InitializeConsumerGroupsTable();
            break;
        case ETables::TransactionalProducers:
            TablesToCreate = 1;
            if (!SourceDatabasePath.empty() && SourceDatabasePath != DatabasePath) {
                SendProducerSequenceStartQuery();
            } else {
                InitializeTransactionalProducersTable(/*producerIdSequenceMinValue*/ 1);
            }
            break;
    }
}

TString TKafkaMetadataService::BuildTablePath(const TString& databasePath, const TString& tableName) {
    return "/" + databasePath + "/.metadata/" + tableName;
}

TString TKafkaMetadataService::GetTablePath(const TString& tableName) const {
    return BuildTablePath(DatabasePath, tableName);
}

void TKafkaMetadataService::InitializeConsumerMembersTable() {
    const TString tablePath = GetTablePath("kafka_consumer_members");
    KAFKA_LOG_D("Creating table " << tablePath);
    Ydb::Table::CreateTableRequest request;
    request.set_session_id("");
    request.set_path(tablePath);
    request.add_primary_key("database");
    request.add_primary_key("consumer_group");
    request.add_primary_key("generation");
    request.add_primary_key("member_id");
    {
        auto& column = *request.add_columns();
        column.set_name("database");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("consumer_group");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("generation");
        column.mutable_type()->set_type_id(Ydb::Type::UINT64);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("member_id");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("instance_id");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("leaved");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::BOOL);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("assignment");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("worker_state_proto");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("heartbeat_deadline");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::DATETIME);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("session_timeout_ms");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("rebalance_timeout_ms");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
    }
    {
        auto* ttlSettings = request.mutable_ttl_settings();
        auto* columnTtl = ttlSettings->mutable_date_type_column();
        columnTtl->set_column_name("heartbeat_deadline");
        columnTtl->set_expire_after_seconds(NKafka::MAX_SESSION_TIMEOUT_MS * 5 / 1000);
    }
    {
        auto& index = *request.add_indexes();
        index.set_name("idx_group_generation_db_hb");
        *index.mutable_global_index() = Ydb::Table::GlobalIndex();
        index.add_index_columns("database");
        index.add_index_columns("consumer_group");
        index.add_index_columns("generation");
        index.add_index_columns("heartbeat_deadline");
    }

    SendCreateTableRequest(std::move(request), "kafka_consumer_members");
}

void TKafkaMetadataService::InitializeConsumerGroupsTable() {
    const TString tablePath = GetTablePath("kafka_consumer_groups");
    KAFKA_LOG_D("Creating table " << tablePath);
    Ydb::Table::CreateTableRequest request;
    request.set_session_id("");
    request.set_path(tablePath);
    request.add_primary_key("database");
    request.add_primary_key("consumer_group");
    {
        auto& column = *request.add_columns();
        column.set_name("database");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("consumer_group");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("generation");
        column.mutable_type()->set_type_id(Ydb::Type::UINT64);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("state");
        column.mutable_type()->set_type_id(Ydb::Type::UINT64);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("last_heartbeat_time");
        column.mutable_type()->set_type_id(Ydb::Type::DATETIME);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("master");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("protocol_type");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("protocol");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("last_success_generation");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
    }
    {
        auto* ttlSettings = request.mutable_ttl_settings();
        auto* columnTtl = ttlSettings->mutable_date_type_column();
        columnTtl->set_column_name("last_heartbeat_time");
        columnTtl->set_expire_after_seconds(NKafka::MAX_SESSION_TIMEOUT_MS * 10 / 1000);
    }
    {
        auto& index = *request.add_indexes();
        index.set_name("idx_last_hb");
        *index.mutable_global_index() = Ydb::Table::GlobalIndex();
        index.add_index_columns("last_heartbeat_time");
    }

    SendCreateTableRequest(std::move(request), "kafka_consumer_groups");
}

void TKafkaMetadataService::InitializeTransactionalProducersTable(i64 producerIdSequenceMinValue) {
    const TString tablePath = GetTablePath("kafka_transactional_producers");
    KAFKA_LOG_D("Creating table " << tablePath << " with producer_id sequence starting at " << producerIdSequenceMinValue);
    Ydb::Table::CreateTableRequest request;
    request.set_session_id("");
    request.set_path(tablePath);
    request.add_primary_key("database");
    request.add_primary_key("transactional_id");
    request.add_primary_key("producer_id");
    {
        auto& column = *request.add_columns();
        column.set_name("database");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("transactional_id");
        column.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("producer_id");
        // we need to use signed int, cause Kafka protocol uses signed int and we can't overflow it on client
        column.mutable_type()->set_type_id(Ydb::Type::INT64);
        column.mutable_from_sequence()->set_name("producer_id");
        column.mutable_from_sequence()->set_min_value(producerIdSequenceMinValue);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("producer_epoch");
        // we need to use signed int, cause Kafka protocol uses signed int and we can't overflow it on client
        column.mutable_type()->set_type_id(Ydb::Type::INT16);
    }
    // updated_at column is only used for ttl purposes. No other business logic relys on it
    {
        auto& column = *request.add_columns();
        column.set_name("updated_at");
        column.mutable_type()->set_type_id(Ydb::Type::DATETIME);
    }
    {
        auto* ttlSettings = request.mutable_ttl_settings();
        auto* columnTtl = ttlSettings->mutable_date_type_column();
        columnTtl->set_column_name("updated_at");
        columnTtl->set_expire_after_seconds(NKafka::TRANSACTIONAL_ID_EXPIRATION_MS);
    }

    SendCreateTableRequest(std::move(request), "kafka_transactional_producers");
}

void TKafkaMetadataService::SendProducerSequenceStartQuery() {
    const TString sourceTablePath = BuildTablePath(SourceDatabasePath, "kafka_transactional_producers");
    KAFKA_LOG_D("Reading max producer_id for database " << DatabasePath << " from " << sourceTablePath);

    Ydb::Scripting::ExecuteYqlRequest request;
    request.set_script(TStringBuilder()
        << "--!syntax_v1\n"
        << "DECLARE $database AS Utf8;\n"
        << "SELECT MAX(producer_id) AS max_producer_id FROM `" << sourceTablePath << "`\n"
        << "WHERE database = $database;");
    {
        Ydb::TypedValue database;
        database.mutable_type()->set_type_id(Ydb::Type::UTF8);
        database.mutable_value()->set_text_value(DatabasePath);
        (*request.mutable_parameters())["$database"] = std::move(database);
    }

    using TExecuteYqlRpc =
        NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Scripting::ExecuteYqlRequest, Ydb::Scripting::ExecuteYqlResponse>;

    auto* actorSystem = TActivationContext::ActorSystem();
    const TActorId selfId = SelfId();

    auto future = NKikimr::NRpcService::DoLocalRpc<TExecuteYqlRpc>(
        std::move(request),
        SourceDatabasePath,
        NACLib::TSystemUsers::Metadata().SerializeAsString(),
        actorSystem,
        /*internalCall*/ true);

    future.Subscribe([actorSystem, selfId](const NThreading::TFuture<Ydb::Scripting::ExecuteYqlResponse>& f) {
        const auto& operation = f.GetValue().operation();
        TString error;
        // On any failure or an empty source table, start from 1 (same as a brand-new dedicated database).
        i64 minValue = 1;
        if (operation.status() != Ydb::StatusIds::SUCCESS) {
            error = operation.DebugString();
        } else {
            Ydb::Scripting::ExecuteYqlResult result;
            operation.result().UnpackTo(&result);
            if (result.result_sets_size() > 0
                    && result.result_sets(0).rows_size() > 0
                    && result.result_sets(0).rows(0).items_size() > 0) {
                const auto& item = result.result_sets(0).rows(0).items(0);
                // MAX over an empty set returns NULL; only offset when there is an actual value.
                if (item.value_case() == Ydb::Value::kInt64Value) {
                    minValue = item.int64_value() + PRODUCER_ID_SEQUENCE_GAP;
                }
            }
        }
        actorSystem->Send(selfId, new TEvPrivate::TEvProducerSequenceStart(operation.status(), error, minValue));
    });
}

void TKafkaMetadataService::Handle(TEvPrivate::TEvProducerSequenceStart::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    if (msg->Status != Ydb::StatusIds::SUCCESS) {
        LOG_WARN_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Failed to read max producer_id from the shared table for database '" << DatabasePath
                << "': " << msg->Error << ". Starting the producer_id sequence from " << msg->MinValue << ".");
    } else {
        LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Starting the producer_id sequence for database '" << DatabasePath << "' at " << msg->MinValue);
    }

    InitializeTransactionalProducersTable(msg->MinValue);
}

void TKafkaMetadataService::SendCreateTableRequest(Ydb::Table::CreateTableRequest&& request, const TString& tableName) {
    using TCreateTableRpc =
        NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::CreateTableRequest, Ydb::Table::CreateTableResponse>;

    auto* actorSystem = TActivationContext::ActorSystem();
    const TActorId selfId = SelfId();

    auto future = NKikimr::NRpcService::DoLocalRpc<TCreateTableRpc>(
        std::move(request),
        DatabasePath,
        NACLib::TSystemUsers::Metadata().SerializeAsString(),
        actorSystem,
        /*internalCall*/ true);

    future.Subscribe([actorSystem, selfId, tableName](const NThreading::TFuture<Ydb::Table::CreateTableResponse>& f) {
        const auto& operation = f.GetValue().operation();
        const TString error = operation.status() == Ydb::StatusIds::SUCCESS ? TString{} : operation.DebugString();
        actorSystem->Send(selfId, new TEvPrivate::TEvTableCreated(tableName, operation.status(), error));
    });
}

void TKafkaMetadataService::SendEnableAutopartitioningRequest(const TString& tableName) {
    const TString tablePath = GetTablePath(tableName);
    KAFKA_LOG_D("Enabling autopartitioning for table " << tablePath);
    Ydb::Table::AlterTableRequest request;
    request.set_session_id("");
    request.set_path(tablePath);
    auto* partitioning = request.mutable_alter_partitioning_settings();
    partitioning->set_min_partitions_count(1);
    partitioning->set_max_partitions_count(1000);
    partitioning->set_partitioning_by_load(::Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED);
    partitioning->set_partitioning_by_size(::Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED);

    SendAlterTableRequest(std::move(request), tableName);
}

void TKafkaMetadataService::SendAlterTableRequest(Ydb::Table::AlterTableRequest&& request, const TString& tableName) {
    using TAlterTableRpc =
        NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Table::AlterTableRequest, Ydb::Table::AlterTableResponse>;

    auto* actorSystem = TActivationContext::ActorSystem();
    const TActorId selfId = SelfId();

    auto future = NKikimr::NRpcService::DoLocalRpc<TAlterTableRpc>(
        std::move(request),
        DatabasePath,
        NACLib::TSystemUsers::Metadata().SerializeAsString(),
        actorSystem,
        /*internalCall*/ true);

    future.Subscribe([actorSystem, selfId, tableName](const NThreading::TFuture<Ydb::Table::AlterTableResponse>& f) {
        const auto& operation = f.GetValue().operation();
        const TString error = operation.status() == Ydb::StatusIds::SUCCESS ? TString{} : operation.DebugString();
        actorSystem->Send(selfId, new TEvPrivate::TEvTableAltered(tableName, operation.status(), error));
    });
}

void TKafkaMetadataService::SendReadOnlyAclRequest(const TString& tableName) {
    const TString tablePath = GetTablePath(tableName);
    KAFKA_LOG_D("Setting read-only ACL for table " << tablePath);

    Ydb::Scheme::ModifyPermissionsRequest request;
    request.set_path(tablePath);
    request.set_clear_permissions(true);
    request.set_interrupt_inheritance(true);
    auto* grant = request.add_actions()->mutable_grant();
    const auto& allAuthenticatedUsers = NKikimr::AppData()->AllAuthenticatedUsers;
    grant->set_subject(allAuthenticatedUsers ? allAuthenticatedUsers : "USERS");
    grant->add_permission_names("ydb.tables.read");
    grant->add_permission_names("ydb.deprecated.describe_schema");

    SendModifyPermissionsRequest(std::move(request), tableName);
}

void TKafkaMetadataService::SendModifyPermissionsRequest(Ydb::Scheme::ModifyPermissionsRequest&& request, const TString& tableName) {
    using TModifyPermissionsRpc =
        NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Scheme::ModifyPermissionsRequest, Ydb::Scheme::ModifyPermissionsResponse>;

    auto* actorSystem = TActivationContext::ActorSystem();
    const TActorId selfId = SelfId();

    auto future = NKikimr::NRpcService::DoLocalRpc<TModifyPermissionsRpc>(
        std::move(request),
        DatabasePath,
        NACLib::TSystemUsers::Metadata().SerializeAsString(),
        actorSystem,
        true);

    future.Subscribe([actorSystem, selfId, tableName](const NThreading::TFuture<Ydb::Scheme::ModifyPermissionsResponse>& f) {
        const auto& operation = f.GetValue().operation();
        const TString error = operation.status() == Ydb::StatusIds::SUCCESS ? TString{} : operation.DebugString();
        actorSystem->Send(selfId, new TEvPrivate::TEvAclModified(tableName, operation.status(), error));
    });
}

void TKafkaMetadataService::Handle(TEvPrivate::TEvTableCreated::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    const auto status = msg->Status;

    if (status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::ALREADY_EXISTS) {
        LOG_ERROR_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Failed to create kafka metadata table '" << msg->TableName << "': " << msg->Error);
        ++ProcessedRequests;
        ReplyIfRequired(ctx);
        return;
    }

    if (status == Ydb::StatusIds::ALREADY_EXISTS) {
        LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Kafka metadata table '" << msg->TableName << "' already exists, skipping modifications and migration");
        ++ProcessedRequests;
        ReplyIfRequired(ctx);
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
        "Kafka metadata table '" << msg->TableName << "' is created (status "
            << Ydb::StatusIds::StatusCode_Name(status) << "), enabling autopartitioning");

    SendEnableAutopartitioningRequest(msg->TableName);
}

void TKafkaMetadataService::Handle(TEvPrivate::TEvTableAltered::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    const auto status = msg->Status;

    if (status != Ydb::StatusIds::SUCCESS) {
        LOG_ERROR_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Failed to enable autopartitioning for kafka metadata table '" << msg->TableName << "': " << msg->Error);
        ++ProcessedRequests;
        ReplyIfRequired(ctx);
        return;
    }

    if ((msg->TableName == "kafka_consumer_groups" || msg->TableName == "kafka_consumer_members") && ShouldMigrate()) {
        LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Kafka metadata table '" << msg->TableName << "' autopartitioning enabled (status "
                << Ydb::StatusIds::StatusCode_Name(status) << "), migrating rows from '"
                << BuildTablePath(SourceDatabasePath, msg->TableName) << "'");
        SendMigrationReadRequest(msg->TableName);
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
        "Kafka metadata table '" << msg->TableName << "' autopartitioning enabled (status "
            << Ydb::StatusIds::StatusCode_Name(status) << "), applying read-only ACL");

    SendReadOnlyAclRequest(msg->TableName);
}

bool TKafkaMetadataService::ShouldMigrate() const {
    return !SourceDatabasePath.empty() && SourceDatabasePath != DatabasePath;
}

TString TKafkaMetadataService::YqlType(const Ydb::Type& type) {
    if (type.has_optional_type()) {
        return YqlType(type.optional_type().item()) + "?";
    }
    switch (type.type_id()) {
        case Ydb::Type::BOOL: return "Bool";
        case Ydb::Type::INT16: return "Int16";
        case Ydb::Type::UINT32: return "Uint32";
        case Ydb::Type::INT64: return "Int64";
        case Ydb::Type::UINT64: return "Uint64";
        case Ydb::Type::DATETIME: return "Datetime";
        case Ydb::Type::STRING: return "String";
        case Ydb::Type::UTF8: return "Utf8";
        default: return {};
    }
}

void TKafkaMetadataService::SendMigrationReadRequest(const TString& tableName) {
    const TString sourceTablePath = BuildTablePath(SourceDatabasePath, tableName);
    KAFKA_LOG_D("Reading rows to migrate from " << sourceTablePath);

    Ydb::Scripting::ExecuteYqlRequest request;
    request.set_script(TStringBuilder()
        << "--!syntax_v1\n"
        << "DECLARE $database AS Utf8;\n"
        << "SELECT * FROM `" << sourceTablePath << "`\n"
        << "WHERE database = $database;");
    {
        Ydb::TypedValue database;
        database.mutable_type()->set_type_id(Ydb::Type::UTF8);
        database.mutable_value()->set_text_value(DatabasePath);
        (*request.mutable_parameters())["$database"] = std::move(database);
    }

    using TExecuteYqlRpc =
        NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Scripting::ExecuteYqlRequest, Ydb::Scripting::ExecuteYqlResponse>;

    auto* actorSystem = TActivationContext::ActorSystem();
    const TActorId selfId = SelfId();

    auto future = NKikimr::NRpcService::DoLocalRpc<TExecuteYqlRpc>(
        std::move(request),
        SourceDatabasePath,
        NACLib::TSystemUsers::Metadata().SerializeAsString(),
        actorSystem,
        /*internalCall*/ true);

    future.Subscribe([actorSystem, selfId, tableName](const NThreading::TFuture<Ydb::Scripting::ExecuteYqlResponse>& f) {
        const auto& operation = f.GetValue().operation();
        Ydb::ResultSet resultSet;
        TString error;
        if (operation.status() != Ydb::StatusIds::SUCCESS) {
            error = operation.DebugString();
        } else {
            Ydb::Scripting::ExecuteYqlResult result;
            operation.result().UnpackTo(&result);
            if (result.result_sets_size() > 0) {
                resultSet = std::move(*result.mutable_result_sets(0));
            }
        }
        actorSystem->Send(selfId, new TEvPrivate::TEvMigrationRead(tableName, operation.status(), error, std::move(resultSet)));
    });
}

void TKafkaMetadataService::SendMigrationUpsertRequest(const TString& tableName, const Ydb::ResultSet& resultSet) {
    const TString targetTablePath = GetTablePath(tableName);
    const ui64 rowCount = resultSet.rows_size();
    KAFKA_LOG_D("Upserting " << rowCount << " migrated rows into " << targetTablePath);

    TStringBuilder structFields;
    TStringBuilder columnNames;
    Ydb::TypedValue rows;
    auto* structType = rows.mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type();
    for (int i = 0; i < resultSet.columns_size(); ++i) {
        const auto& column = resultSet.columns(i);
        auto* member = structType->add_members();
        member->set_name(column.name());
        *member->mutable_type() = column.type();
        if (i > 0) {
            structFields << ", ";
            columnNames << ", ";
        }
        structFields << column.name() << ": " << YqlType(column.type());
        columnNames << column.name();
    }
    for (const auto& row : resultSet.rows()) {
        *rows.mutable_value()->add_items() = row;
    }

    Ydb::Scripting::ExecuteYqlRequest request;
    request.set_script(TStringBuilder()
        << "--!syntax_v1\n"
        << "DECLARE $rows AS List<Struct<" << structFields << ">>;\n"
        << "UPSERT INTO `" << targetTablePath << "`\n"
        << "SELECT " << columnNames << "\n"
        << "FROM AS_TABLE($rows);");
    (*request.mutable_parameters())["$rows"] = std::move(rows);

    using TExecuteYqlRpc =
        NKikimr::NGRpcService::TGrpcRequestOperationCall<Ydb::Scripting::ExecuteYqlRequest, Ydb::Scripting::ExecuteYqlResponse>;

    auto* actorSystem = TActivationContext::ActorSystem();
    const TActorId selfId = SelfId();

    auto future = NKikimr::NRpcService::DoLocalRpc<TExecuteYqlRpc>(
        std::move(request),
        DatabasePath,
        NACLib::TSystemUsers::Metadata().SerializeAsString(),
        actorSystem,
        /*internalCall*/ true);

    future.Subscribe([actorSystem, selfId, tableName, rowCount](const NThreading::TFuture<Ydb::Scripting::ExecuteYqlResponse>& f) {
        const auto& operation = f.GetValue().operation();
        const TString error = operation.status() == Ydb::StatusIds::SUCCESS ? TString{} : operation.DebugString();
        actorSystem->Send(selfId, new TEvPrivate::TEvMigrationWritten(tableName, operation.status(), error, rowCount));
    });
}

void TKafkaMetadataService::Handle(TEvPrivate::TEvMigrationRead::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    if (msg->Status != Ydb::StatusIds::SUCCESS) {
        LOG_ERROR_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Failed to read rows of table '" << msg->TableName << "' to migrate for database '" << DatabasePath
                << "': " << msg->Error << ". Proceeding without migration.");
        SendReadOnlyAclRequest(msg->TableName);
        return;
    }

    if (msg->ResultSet.rows_size() == 0) {
        LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
            "No rows of table '" << msg->TableName << "' to migrate into database '" << DatabasePath
                << "', applying read-only ACL");
        SendReadOnlyAclRequest(msg->TableName);
        return;
    }

    SendMigrationUpsertRequest(msg->TableName, msg->ResultSet);
}

void TKafkaMetadataService::Handle(TEvPrivate::TEvMigrationWritten::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    if (msg->Status != Ydb::StatusIds::SUCCESS) {
        LOG_ERROR_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Failed to migrate " << msg->RowCount << " rows of table '" << msg->TableName << "' into database '"
                << DatabasePath << "': " << msg->Error << ". Proceeding without migration.");
    } else {
        LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Migrated " << msg->RowCount << " rows of table '" << msg->TableName << "' into database '"
                << DatabasePath << "', applying read-only ACL");
    }

    SendReadOnlyAclRequest(msg->TableName);
}

void TKafkaMetadataService::Handle(TEvPrivate::TEvAclModified::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    const auto status = msg->Status;

    if (status != Ydb::StatusIds::SUCCESS) {
        LOG_ERROR_S(ctx, NKikimrServices::KAFKA_PROXY,
            "Failed to set read-only ACL for kafka metadata table '" << msg->TableName << "': " << msg->Error);
        ++ProcessedRequests;
        ReplyIfRequired(ctx);
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
        "Kafka metadata table '" << msg->TableName << "' is ready (status "
            << Ydb::StatusIds::StatusCode_Name(status) << ")");
    ++ProcessedRequests;
    ReplyIfRequired(ctx);
}

void TKafkaMetadataService::ReplyIfRequired(const TActorContext& ctx) {
    if (ProcessedRequests == TablesToCreate) {
        KAFKA_LOG_I("All metadata tables are created for database " << DatabasePath);
        Die(ctx);
    }
}

static bool TryRequestMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath,
                                             const TString& sourceDatabasePath, TKafkaMetadataService::ETables tables,
                                             const TActorContext& ctx) {
    if (!NKikimr::AppData()->FeatureFlags.GetEnableServerlessTransactions() || status != Ydb::StatusIds::SCHEME_ERROR) {
        return false;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::KAFKA_PROXY, TStringBuilder() << "Kafka metadata tables are missing for database '" << databasePath
            << "'. Requesting their creation and asking the client to retry.");
    ctx.Register(new TKafkaMetadataService(databasePath, tables, sourceDatabasePath));
    return true;
}

bool TryRequestConsumerMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath, const TString& sourceDatabasePath, const TActorContext& ctx) {
    return TryRequestMetadataTablesCreation(status, databasePath, sourceDatabasePath, TKafkaMetadataService::ETables::ConsumerGroupsAndMembers, ctx);
}

bool TryRequestProducerMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath, const TString& sourceDatabasePath, const TActorContext& ctx) {
    // sourceDatabasePath is used only to read the max producer_id for picking the sequence start value; the producers
    // table itself is still not migrated (its table name is absent from the TEvTableAltered migration trigger).
    return TryRequestMetadataTablesCreation(status, databasePath, sourceDatabasePath, TKafkaMetadataService::ETables::TransactionalProducers, ctx);
}
} // namespace NKafka
