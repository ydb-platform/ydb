#include "kafka_consumer_members_metadata_initializers.h"
#include "actors/kafka_balancer_actor.h"

namespace NKikimr::NGRpcProxy::V1 {

using namespace NMetadata;

void TKafkaConsumerMembersMetaInitializer::DoPrepare(NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NInitializer::ITableModifier::TPtr> result;
    auto tablePath = TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath();
    {
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
