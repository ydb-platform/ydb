#include "kafka_consumer_groups_metadata_initializers.h"
#include "actors/kafka_balancer_actor.h"

namespace NKikimr::NGRpcProxy::V1 {

using namespace NMetadata;

void TKafkaConsumerGroupsMetaInitializer::DoPrepare(NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NInitializer::ITableModifier::TPtr> result;
    auto tablePath = TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath();
    {
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

        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(request, "create"));

        {
            Ydb::Table::AlterTableRequest request;
            request.set_session_id("");
            request.set_path(tablePath);
            request.mutable_alter_partitioning_settings()->set_min_partitions_count(1);
            request.mutable_alter_partitioning_settings()->set_max_partitions_count(1000);
            request.mutable_alter_partitioning_settings()->set_partitioning_by_load(::Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED);
            request.mutable_alter_partitioning_settings()->set_partitioning_by_size(::Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED);

            result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogAlterTable>(request, "enable_autopartitioning_by_load"));
        }
    }
    result.emplace_back(NInitializer::TACLModifierConstructor::GetReadOnlyModifier(tablePath, "acl"));
    controller->OnPreparationFinished(result);
}

}
