#include "metadata_initializers.h"

namespace NKikimr::NGRpcProxy::V1 {

using namespace NMetadata;

void TSrcIdMetaInitializer::DoPrepare(NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NInitializer::ITableModifier::TPtr> result;
    auto tablePath = TSrcIdMetaInitManager::GetInstant()->GetStorageTablePath();
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(tablePath);
        request.add_primary_key("Hash");
        request.add_primary_key("Topic");
        request.add_primary_key("ProducerId");
        {
            auto& column = *request.add_columns();
            column.set_name("Hash");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("Topic");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("ProducerId");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("AccessTime");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("CreateTime");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("Partition");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        {
            auto* partSettings = request.mutable_partitioning_settings();
            partSettings->add_partition_by("Hash");
            partSettings->set_partitioning_by_size(Ydb::FeatureFlag::ENABLED);
            partSettings->set_partitioning_by_load(Ydb::FeatureFlag::DISABLED);
            partSettings->set_max_partitions_count(1000);
        }
        {
            auto* ttlSettings = request.mutable_ttl_settings();
            auto* columnTtl = ttlSettings->mutable_value_since_unix_epoch();
            columnTtl->set_column_name("AccessTime");
            columnTtl->set_expire_after_seconds(1382400);
            columnTtl->set_column_unit(Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MILLISECONDS);
        }

        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(request, "create"));

        {
            Ydb::Table::AlterTableRequest request;
            request.set_session_id("");
            request.set_path(tablePath);

            {
                auto& column = *request.add_add_columns();
                column.set_name("SeqNo");
                column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
            }

            result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogAlterTable>(request, "add_column_SeqNo"));
        }

        {
            Ydb::Table::AlterTableRequest request;
            request.set_session_id("");
            request.set_path(tablePath);
            request.mutable_alter_partitioning_settings()->set_min_partitions_count(50);
            request.mutable_alter_partitioning_settings()->set_partitioning_by_load(::Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED);

            result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogAlterTable>(request, "enable_autopartitioning_by_load"));
        }
    }
    result.emplace_back(NInitializer::TACLModifierConstructor::GetReadOnlyModifier(tablePath, "acl"));
    controller->OnPreparationFinished(result);
}

} // namespace NKikimr::NGRpcProxy::V1
