#include "kafka_metadata_service.h"

#include <ydb/core/kafka_proxy/actors/kafka_balancer_actor.h>
#include <ydb/core/kafka_proxy/kafka_constants.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

namespace NKafka {

using namespace NActors;

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
            InitializeTransactionalProducersTable();
            break;
    }
}

TString TKafkaMetadataService::GetTablePath(const TString& tableName) const {
    return "/" + DatabasePath + "/.metadata/" + tableName;
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

void TKafkaMetadataService::InitializeTransactionalProducersTable() {
    const TString tablePath = GetTablePath("kafka_transactional_producers");
    KAFKA_LOG_D("Creating table " << tablePath);
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
        column.mutable_from_sequence()->set_min_value(1);
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

    LOG_INFO_S(ctx, NKikimrServices::KAFKA_PROXY,
        "Kafka metadata table '" << msg->TableName << "' autopartitioning enabled (status "
            << Ydb::StatusIds::StatusCode_Name(status) << "), applying read-only ACL");

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
                                             TKafkaMetadataService::ETables tables, const TActorContext& ctx) {
    if (!NKikimr::AppData()->FeatureFlags.GetEnableServerlessTransactions() || status != Ydb::StatusIds::SCHEME_ERROR) {
        return false;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::KAFKA_PROXY, TStringBuilder() << "Kafka metadata tables are missing for database '" << databasePath
            << "'. Requesting their creation and asking the client to retry.");
    ctx.Register(new TKafkaMetadataService(databasePath, tables));
    return true;
}

bool TryRequestConsumerMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath, const TActorContext& ctx) {
    return TryRequestMetadataTablesCreation(status, databasePath, TKafkaMetadataService::ETables::ConsumerGroupsAndMembers, ctx);
}

bool TryRequestProducerMetadataTablesCreation(Ydb::StatusIds::StatusCode status, const TString& databasePath, const TActorContext& ctx) {
    return TryRequestMetadataTablesCreation(status, databasePath, TKafkaMetadataService::ETables::TransactionalProducers, ctx);
}
} // namespace NKafka
