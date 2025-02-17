#include "kafka_consumer_groups_metadata_initializers.h"

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
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("consumer_group");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("generation");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("state");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("last_heartbeat_time");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::DATETIME);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("master");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("protocol_type");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("protocol");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto* ttlSettings = request.mutable_ttl_settings();
            auto* columnTtl = ttlSettings->mutable_date_type_column();
            columnTtl->set_column_name("last_heartbeat_time");
            columnTtl->set_expire_after_seconds(300);
        }
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(request, "create"));
    }
    result.emplace_back(NInitializer::TACLModifierConstructor::GetReadOnlyModifier(tablePath, "acl"));
    controller->OnPreparationFinished(result);
}

}
