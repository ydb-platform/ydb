#include "kafka_consumer_members_metadata_initializers.h"

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
            column.set_name("member_id");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
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
            column.set_name("last_heartbeat_time");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::DATETIME);
        }
        {
            auto* ttlSettings = request.mutable_ttl_settings();
            auto* columnTtl = ttlSettings->mutable_date_type_column();
            columnTtl->set_column_name("last_heartbeat_time");
            columnTtl->set_expire_after_seconds(60);
        }
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(request, "create"));
    }

    result.emplace_back(NInitializer::TACLModifierConstructor::GetReadOnlyModifier(tablePath, "acl"));
    controller->OnPreparationFinished(result);
}

}
