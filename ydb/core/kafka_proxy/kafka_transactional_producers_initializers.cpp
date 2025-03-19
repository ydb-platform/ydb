#include "kafka_transactional_producers_initializers.h"
#include "kafka_constants.h"

namespace NKikimr::NGRpcProxy::V1 {

using namespace NMetadata;

void TTransactionalProducersInitializer::DoPrepare(NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NInitializer::ITableModifier::TPtr> result;
    auto tablePath = TTransactionalProducersInitManager::GetInstant()->GetStorageTablePath();
    {
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
        // set ttl on updated_at column
        {
            auto* ttlSettings = request.mutable_ttl_settings();
            auto* columnTtl = ttlSettings->mutable_date_type_column();
            columnTtl->set_column_name("updated_at");
            columnTtl->set_expire_after_seconds(NKafka::TRANSACTIONAL_ID_EXPIRATION_MS);
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
